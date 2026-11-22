// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 公共 broker 契约：同一组断言跑 native（默认）和当前 Node host（KITESURF_BROKER_KIND=node）。
// 只覆盖两端都必须满足的协议；Rust 内部时序由 native:broker_test 证明。

import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { once } from "node:events";
import { after, before, test } from "node:test";
import WebSocket from "ws";
import { ProfileStore } from "../src/profile-store.mjs";
import { startBrowser } from "../src/server.mjs";
import { required, startNativeHost, waitFor } from "./host-adapter.mjs";

const kind = process.env.KITESURF_BROKER_KIND || "native";
const expectedVersion = kind === "node" ? "0.5.0" : "0.5.4";
const engine = resolve(import.meta.dirname, "../../engine");
const dist = process.env.KITESURF_TEST_DIST || join(engine, "local_dist");
const assets = process.env.KITESURF_TEST_ASSETS || join(engine, "public");
let host;
let directory;
let done;

async function startNodeHost() {
  directory = mkdtempSync(join(tmpdir(), "ks-broker-node-"));
  const profile = new ProfileStore(join(directory, "profile.sqlite"), { create: true });
  const browser = await startBrowser({ profile, dist, assets, port: 0, allowPrivateNetwork: true });
  return {
    port: browser.port,
    done: browser.done.then((reason) => ({ reason })),
    stop: () => browser.close(),
  };
}

async function startNative() {
  const configDir = required("KITESURF_CONFIG_DIR");
  directory = mkdtempSync(join(tmpdir(), "ks-broker-native-"));
  const profile = join(directory, "profile.sqlite");
  // 公共契约读取真实的磁盘 profile status；T2 内存探针字段不进入该契约。
  new ProfileStore(profile, { create: true }).close();
  const native = await startNativeHost({
    workerd: required("KITESURF_WORKERD"),
    config: `${configDir}/local-private.capnp`,
    assets: required("KITESURF_ENGINE_ASSETS"),
    assetsManifest: `${configDir}/assets-manifest.json`,
    profile,
  });
  return {
    port: native.port,
    done: native.exit.then(({ code, signal }) => ({ code, signal })),
    stop: () => native.stop("SIGTERM", 30_000),
  };
}

async function request(port, route, options = {}) {
  const response = await fetch(`http://127.0.0.1:${port}${route}`, {
    method: options.method || "GET",
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const text = await response.text();
  let body;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  return { status: response.status, body };
}

function rawRequest(port, path, headers) {
  return new Promise((resolve, reject) => {
    const call = httpRequest({ host: "127.0.0.1", port, path, headers }, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk) => {
        body += chunk;
      });
      response.on("end", () => resolve({ status: response.statusCode, body }));
    });
    call.on("error", reject);
    call.end();
  });
}

async function client(port, query = "", path = "/devtools/browser") {
  const ws = new WebSocket(`ws://127.0.0.1:${port}${path}${query}`);
  const frames = [];
  let cursor = 0;
  ws.on("message", (data) => frames.push(JSON.parse(data.toString())));
  const closed = new Promise((resolve) => {
    ws.on("close", (code, reason) => resolve({ code, reason: reason.toString() }));
  });
  try {
    await new Promise((resolve, reject) => {
      ws.once("open", resolve);
      ws.once("error", reject);
      ws.once("unexpected-response", (_request, response) => {
        reject(new Error(`upgrade rejected with ${response.statusCode}`));
      });
    });
  } catch (error) {
    ws.terminate();
    throw error;
  }
  ws.on("error", () => {});
  const channel = {
    ws,
    frames,
    send(value) {
      ws.send(JSON.stringify(value));
    },
    async frame(predicate, description) {
      return waitFor(() => {
        for (let index = cursor; index < frames.length; index++) {
          if (predicate(frames[index])) {
            cursor = index + 1;
            return frames[index];
          }
        }
        return null;
      }, description);
    },
    reply(id, description) {
      return channel.frame((frame) => frame.id === id && frame.method === undefined, description);
    },
    async close() {
      if (ws.readyState === WebSocket.CLOSED) return;
      const exiting = once(ws, "close");
      ws.close();
      await exiting;
    },
    closed,
  };
  return channel;
}

async function pageClient(port, targetId) {
  return waitFor(async () => {
    try {
      return await client(port, "?owner=alice", `/devtools/page/${targetId}`);
    } catch {
      return null;
    }
  }, "page endpoint");
}

async function rejection(url) {
  const ws = new WebSocket(url);
  try {
    return await new Promise((resolve, reject) => {
      ws.on("unexpected-response", (_request, response) => resolve(response.statusCode));
      ws.on("open", () => reject(new Error(`unexpected upgrade: ${url}`)));
      ws.on("error", () => resolve(0));
    });
  } finally {
    ws.terminate();
  }
}

before(async () => {
  host = kind === "node" ? await startNodeHost() : await startNative();
});

after(async () => {
  try {
    await host?.stop();
  } catch {}
  if (directory) rmSync(directory, { recursive: true, force: true });
});

test("broker public contract", { timeout: 300_000 }, async (t) => {
  const base = () => `http://127.0.0.1:${host.port}`;

  await t.test("health, session and discovery endpoints", async () => {
    const health = await request(host.port, "/health");
    assert.equal(health.status, 200);
    assert.deepEqual(health.body, { alive: true, version: expectedVersion });
    const session = await request(host.port, "/session");
    assert.equal(session.status, 200);
    assert.equal(session.body.version, expectedVersion);
    assert.ok(typeof session.body.sessionId === "string" && session.body.sessionId.length > 0);
    assert.ok(typeof session.body.generation === "string" && session.body.generation.length > 0);
    assert.equal(session.body.alive, true);
    assert.equal(session.body.controller, null);
    assert.equal(session.body.controlEpoch, 1);
    assert.ok(["new-profile", "profile-restored"].includes(session.body.recovery));
    assert.ok(typeof session.body.profile.path === "string");
    assert.ok(Array.isArray(session.body.tabs) && session.body.tabs.length >= 1);
    for (const route of ["/json/version", "/json/version/"]) {
      const version = await request(host.port, route);
      assert.equal(version.status, 200);
      assert.equal(version.body.Browser, `Kitesurf/${expectedVersion}`);
      assert.equal(version.body["Protocol-Version"], "1.3");
      assert.equal(
        version.body.webSocketDebuggerUrl,
        `ws://127.0.0.1:${host.port}/devtools/browser`,
      );
    }
    for (const route of ["/json", "/json/list"]) {
      const list = await request(host.port, route);
      assert.equal(list.status, 200);
      assert.ok(Array.isArray(list.body) && list.body.length >= 1);
      for (const entry of list.body) {
        assert.equal(entry.type, "page");
        assert.ok(typeof entry.id === "string" && entry.id.length > 0);
        assert.ok("title" in entry && "url" in entry);
        assert.equal(
          entry.webSocketDebuggerUrl,
          `ws://127.0.0.1:${host.port}/devtools/page/${entry.id}`,
        );
        assert.ok(entry.devtoolsFrontendUrl.includes(`/devtools/page/${entry.id}`));
      }
    }
    const missing = await request(host.port, "/not-a-route");
    assert.equal(missing.status, 404);
    assert.deepEqual(missing.body, { error: "Not found" });
    const root = await request(host.port, "/");
    assert.equal(root.status, 200);
    assert.equal(root.body.session, "/session");
    assert.equal(root.body.cdp, `ws://127.0.0.1:${host.port}/devtools/browser`);
  });

  await t.test("origin, host, body and session rejection", async () => {
    const wrongHost = await rawRequest(host.port, "/health", {
      host: `evil.example:${host.port}`,
    });
    assert.equal(wrongHost.status, 403);
    const wrongOrigin = await rawRequest(host.port, "/health", {
      host: `127.0.0.1:${host.port}`,
      origin: "https://evil.example",
    });
    assert.equal(wrongOrigin.status, 403);
    const oversized = await request(host.port, "/control", {
      method: "POST",
      body: { owner: "alice", padding: "x".repeat(70_000) },
    });
    assert.equal(oversized.status, 409);
    assert.equal(
      await rejection(`ws://127.0.0.1:${host.port}/devtools/browser?session=other`),
      409,
    );
    if (kind === "native") {
      const duplicate = await rejection(
        `ws://127.0.0.1:${host.port}/devtools/browser?role=viewer&role=controller`,
      );
      assert.equal(duplicate, 409);
    }
  });

  let viewer;
  let controller;
  let fenced;

  await t.test("viewer whitelist, same external ids and control epoch", async () => {
    controller = await client(host.port, "?owner=alice");
    viewer = await client(host.port, "?owner=viewer&role=viewer");
    controller.send({ id: 1, method: "Browser.getVersion" });
    const version = await controller.reply(1, "controller version");
    assert.equal(version.result.revision, "@kitesurf");
    viewer.send({ id: 1, method: "Runtime.evaluate", params: { expression: "1" } });
    const denied = await viewer.reply(1, "viewer evaluate denial");
    assert.equal(denied.error.message, "Viewer connection cannot control the browser");
    controller.send({ id: 2, method: "Target.getTargets" });
    viewer.send({ id: 2, method: "Target.getTargets" });
    const fromController = await controller.reply(2, "controller targets");
    const fromViewer = await viewer.reply(2, "viewer targets");
    assert.ok(Array.isArray(fromController.result.targetInfos));
    assert.ok(Array.isArray(fromViewer.result.targetInfos));
    const conflict = await request(host.port, "/control", {
      method: "POST",
      body: { owner: "bob" },
    });
    assert.equal(conflict.status, 409);
    assert.equal(conflict.body.error, "Browser has another controller");
    assert.equal(conflict.body.controller, "alice");
    const same = await request(host.port, "/control", {
      method: "POST",
      body: { owner: "alice" },
    });
    assert.equal(same.status, 200);
    assert.deepEqual(same.body, { controller: "alice", controlEpoch: 2 });
    controller.send({ id: 3, method: "Browser.getVersion" });
    const changed = await controller.reply(3, "fenced controller");
    assert.equal(changed.error.message, "Control has changed; reconnect and observe before acting");
    viewer.send({ id: 3, method: "Target.getTargets" });
    const survived = await viewer.reply(3, "viewer after takeover");
    assert.ok(Array.isArray(survived.result.targetInfos));
    fenced = await client(host.port, "?owner=alice");
    fenced.send({ id: 1, method: "Target.createTarget", params: { url: "about:blank" } });
    const created = await fenced.reply(1, "create target");
    assert.ok(typeof created.result.targetId === "string" && created.result.targetId.length > 0);
    fenced.targetId = created.result.targetId;
  });

  await t.test("page endpoint injects its primary session and expires cleanly", async () => {
    await fenced.close();
    const page = await pageClient(host.port, fenced.targetId);
    page.send({
      id: 1,
      method: "Runtime.evaluate",
      params: { expression: "6 * 7", returnByValue: true },
    });
    const evaluated = await page.reply(1, "page evaluation");
    assert.equal(evaluated.result.result.value, 42);
    assert.equal(evaluated.sessionId, undefined);
    assert.equal(
      page.frames.some((frame) => frame.method === "Target.attachedToTarget"),
      false,
      "the page endpoint must not announce itself",
    );
    await page.close();
    const expired = await pageClient(host.port, "nope");
    const close = await expired.closed;
    assert.equal(close.code, 1008);
    assert.equal(close.reason, "Target expired");
    await controller.close();
    await viewer.close();
  });

  await t.test(
    "online backup preserves the destination and keeps the session running",
    async () => {
      const path = join(directory, "online-backup.sqlite");
      const options = { method: "POST", body: { path } };
      const backup = await request(host.port, "/backup", options);
      assert.equal(backup.status, 200);
      assert.equal(realpathSync(backup.body.path), realpathSync(path));
      const bytes = readFileSync(path);
      const conflict = await request(host.port, "/backup", options);
      assert.equal(conflict.status, 409);
      assert.deepEqual(readFileSync(path), bytes);
      const session = await request(host.port, "/session");
      assert.equal(session.status, 200);
      assert.equal(session.body.alive, true);
    },
  );

  await t.test("close flushes before success and stops the host", async () => {
    const closed = await request(host.port, "/close", { method: "POST" });
    assert.equal(closed.status, 200);
    assert.deepEqual(closed.body, { closed: true });
    done = await Promise.race([
      host.done,
      new Promise((resolve) => setTimeout(resolve, 30_000, { timeout: true })),
    ]);
    if (kind === "node") assert.deepEqual(done, { reason: "closed" });
    else assert.equal(done.code, 0, JSON.stringify(done));
    await assert.rejects(fetch(`${base()}/health`));
  });
});
