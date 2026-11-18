// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T4 真实 workerd：多 client 的同号隔离、viewer 权限、controller 重连后的 session/tabs、
// page endpoint 的隐式 session 与 target 过期。断言只读取 CDP 响应和 /session。

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket from "ws";

import { required, startNativeHost, waitFor } from "./host-adapter.mjs";

const nativeHost = required("KITESURF_NATIVE_HOST");
const workerd = required("KITESURF_WORKERD");
const configDir = required("KITESURF_CONFIG_DIR");
const engineAssets = required("KITESURF_ENGINE_ASSETS");
const assetsManifest = join(configDir, "assets-manifest.json");
process.env.KITESURF_NATIVE_HOST = nativeHost;

function fixture(t) {
  const server = createServer((request, response) => {
    const route = (request.url || "/").split("?")[0];
    if (route === "/favicon.ico") {
      response.writeHead(204);
      response.end();
      return;
    }
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(
      "<!doctype html><html><head><title>ks-broker-engine</title></head><body>" +
        '<h1 id="route">engine</h1><script>window.boot=true;</script></body></html>',
    );
  });
  server.listen(0, "127.0.0.1");
  t.after(() => server.close());
  return once(server, "listening").then(() => ({
    url: `http://127.0.0.1:${server.address().port}`,
  }));
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

async function connectRaw(port, query = "", path = "/devtools/browser") {
  const ws = new WebSocket(`ws://127.0.0.1:${port}${path}${query}`);
  const frames = [];
  const consumed = new Set();
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
  const client = {
    ws,
    frames,
    closed,
    send(value) {
      ws.send(JSON.stringify(value));
    },
    async frame(predicate, description) {
      return waitFor(() => {
        for (const frame of frames) {
          if (!consumed.has(frame) && predicate(frame)) {
            consumed.add(frame);
            return frame;
          }
        }
        return null;
      }, description);
    },
    reply(id, description) {
      return client.frame(
        (frame) => frame.id === id && frame.method === undefined,
        description,
      );
    },
    async close() {
      if (ws.readyState === WebSocket.CLOSED) return;
      const exiting = once(ws, "close");
      ws.close();
      await exiting;
    },
  };
  return client;
}

let externalId = 1000;

async function call(client, method, params = {}, sessionId) {
  const id = externalId++;
  const frame = { id, method, params };
  if (sessionId !== undefined) frame.sessionId = sessionId;
  client.send(frame);
  return client.reply(id, `${method} reply`);
}

async function evaluateOn(client, sessionId, expression) {
  const reply = await call(
    client,
    "Runtime.evaluate",
    { expression, returnByValue: true, awaitPromise: true },
    sessionId,
  );
  if (reply.error) throw new Error(reply.error.message);
  if (reply.result.exceptionDetails) throw new Error(JSON.stringify(reply.result.exceptionDetails));
  return reply.result.result.value;
}

test("broker engine", { timeout: 300_000 }, async (t) => {
  const site = await fixture(t);
  const pageUrl = `${site.url}/`;
  const directory = mkdtempSync(join(tmpdir(), "ks-broker-engine-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const profile = join(directory, "profile.sqlite");
  const created = spawnSync(nativeHost, ["profile", "create", profile], { encoding: "utf8" });
  assert.equal(created.status, 0, created.stderr);
  const host = await startNativeHost({
    workerd,
    config: join(configDir, "local-private.capnp"),
    assets: engineAssets,
    assetsManifest,
    profile,
    observe: join(directory, "observe.jsonl"),
  });
  t.after(async () => {
    try {
      await host.stop("SIGTERM", 30_000);
    } catch {}
  });

  const baseline = (await request(host.port, "/session")).body;
  assert.equal(baseline.controller, null);
  assert.equal(baseline.controlEpoch, 1);
  assert.ok(baseline.tabs.length >= 1);

  let controller = await connectRaw(host.port, "?owner=alice");
  const viewer = await connectRaw(host.port, "?owner=viewer&role=viewer");
  t.after(async () => {
    await controller?.close();
    await viewer.close();
  });
  let targetId;

  await t.test("same external id stays isolated per client", async () => {
    controller.send({ id: 100, method: "Browser.getVersion" });
    viewer.send({ id: 100, method: "Target.getTargets" });
    const version = await controller.reply(100, "controller version");
    const targets = await viewer.reply(100, "viewer targets");
    assert.equal(version.result.revision, "@kitesurf");
    assert.ok(Array.isArray(targets.result.targetInfos));
    assert.equal(version.id, 100);
    assert.equal(targets.id, 100);
  });

  await t.test("a duplicate in-flight id is rejected on the same client", async () => {
    controller.send({ id: 77, method: "Browser.getVersion" });
    controller.send({ id: 77, method: "Browser.getVersion" });
    const first = await controller.reply(77, "first duplicate id frame");
    const second = await controller.reply(77, "second duplicate id frame");
    const replies = [first, second];
    assert.equal(replies.filter((reply) => reply.result !== undefined).length, 1);
    const denied = replies.find((reply) => reply.error !== undefined);
    assert.equal(denied.error.message, "Duplicate in-flight request id");
  });

  await t.test("viewer cannot control while read-only methods work", async () => {
    const cases = [
      ["Runtime.evaluate", { expression: "1" }],
      ["Page.navigate", { url: pageUrl }],
      ["Target.createTarget", { url: "about:blank" }],
      ["Target.setAutoAttach", { autoAttach: true, waitForDebuggerOnStart: false }],
    ];
    for (const [method, params] of cases) {
      const reply = await call(viewer, method, params);
      assert.equal(reply.error.message, "Viewer connection cannot control the browser", method);
    }
    const allowed = await call(viewer, "Target.getTargets");
    assert.ok(Array.isArray(allowed.result.targetInfos));
  });

  await t.test("controller cannot close with a viewer-owned session", async () => {
    const targets = await call(viewer, "Target.getTargets");
    const targetId = targets.result.targetInfos[0].targetId;
    const attached = await call(viewer, "Target.attachToTarget", { targetId, flatten: true });
    const denied = await call(controller, "Browser.close", {}, attached.result.sessionId);
    assert.match(denied.error.message, /belongs to another connection/);
    assert.equal((await host.request("/health")).body.alive, true);
    assert.ok((await call(controller, "Browser.getVersion")).result);
    assert.ok((await call(viewer, "Target.getTargets")).result);
  });

  await t.test("controller creates a tab and receives discovery", async () => {
    const discover = await call(controller, "Target.setDiscoverTargets", { discover: true });
    assert.equal(discover.error, undefined);
    const known = new Set((await request(host.port, "/json/list")).body.map((entry) => entry.id));
    const id = externalId++;
    controller.send({ id, method: "Target.createTarget", params: { url: "about:blank" } });
    const createdEvent = await controller.frame(
      (frame) =>
        frame.method === "Target.targetCreated" &&
        !known.has(frame.params?.targetInfo?.targetId),
      "targetCreated for the new tab",
    );
    const result = await controller.reply(id, "createTarget reply");
    targetId = result.result.targetId;
    assert.equal(createdEvent.params.targetInfo.targetId, targetId);
    assert.equal(createdEvent.params.targetInfo.type, "page");
    const listed = (await request(host.port, "/json/list")).body;
    assert.ok(listed.some((entry) => entry.id === targetId));
  });

  let sessionId;
  await t.test("an attached page survives a controller reconnect", async () => {
    const attached = await call(controller, "Target.attachToTarget", { targetId, flatten: true });
    sessionId = attached.result.sessionId;
    assert.ok(typeof sessionId === "string" && sessionId.length > 0);
    await call(controller, "Page.enable", {}, sessionId);
    await call(controller, "Page.navigate", { url: pageUrl }, sessionId);
    await waitFor(
      () => evaluateOn(controller, sessionId, "window.boot === true"),
      "the fixture page to boot",
    );
    const before = await request(host.port, "/session");
    assert.equal(before.body.controller, "alice");
    assert.ok(before.body.tabs.some((tab) => tab.targetId === targetId));

    await controller.close();
    controller = await connectRaw(
      host.port,
      `?owner=alice&session=${encodeURIComponent(baseline.sessionId)}`,
    );
    const after = await request(host.port, "/session");
    assert.equal(after.body.generation, baseline.generation);
    assert.equal(after.body.sessionId, baseline.sessionId);
    assert.ok(after.body.tabs.some((tab) => tab.targetId === targetId));
    const viewerTargets = await call(viewer, "Target.getTargets");
    assert.ok(viewerTargets.result.targetInfos.some((target) => target.targetId === targetId));
    const targets = await call(controller, "Target.getTargets");
    assert.ok(targets.result.targetInfos.some((target) => target.targetId === targetId));

    const reattached = await call(controller, "Target.attachToTarget", {
      targetId,
      flatten: true,
    });
    const reattachedSession = reattached.result.sessionId;
    await call(controller, "Page.enable", {}, reattachedSession);
    assert.equal(
      await evaluateOn(controller, reattachedSession, "document.title"),
      "ks-broker-engine",
    );
    // engine/page console 输出只能以固定类别计数；原始秘密不得进入宿主输出或 observer。
    const secret = `ks-console-secret-${Date.now()}`;
    await evaluateOn(controller, reattachedSession, `console.log(${JSON.stringify(secret)})`);
    await new Promise((resolve) => setTimeout(resolve, 300));
    const observed = readFileSync(join(directory, "observe.jsonl"), "utf8");
    assert.ok(!host.stdout.includes(secret), "a page console secret reached host stdout");
    assert.ok(!host.stderr.includes(secret), "a page console secret reached host stderr");
    assert.ok(!observed.includes(secret), "a page console secret reached the observer");
  });

  await t.test("page endpoint injects its primary session without announcement", async () => {
    await controller.close();
    const page = await connectRaw(host.port, "?owner=alice", `/devtools/page/${targetId}`);
    t.after(() => page.close());
    const reply = await call(
      page,
      "Runtime.evaluate",
      { expression: "6 * 7", returnByValue: true },
    );
    assert.equal(reply.result.result.value, 42);
    assert.equal(reply.sessionId, undefined);
    assert.equal(
      page.frames.some((frame) => frame.method === "Target.attachedToTarget"),
      false,
      "the page endpoint must not announce its target",
    );
    const listed = (await request(host.port, "/json/list")).body;
    const entry = listed.find((target) => target.id === targetId);
    assert.ok(entry);
    assert.equal(entry.webSocketDebuggerUrl, `ws://127.0.0.1:${host.port}/devtools/page/${targetId}`);
    await page.close();
  });

  await t.test("closing a target expires its page endpoint", async () => {
    controller = await connectRaw(host.port, "?owner=alice");
    const closed = await call(controller, "Target.closeTarget", { targetId });
    assert.deepEqual(closed.result, { success: true });
    await waitFor(async () => {
      const session = (await request(host.port, "/session")).body;
      return !session.tabs.some((tab) => tab.targetId === targetId);
    }, "the closed tab to leave /session");
    await controller.close();
    const expired = await connectRaw(host.port, "?owner=alice", `/devtools/page/${targetId}`);
    const close = await expired.closed;
    assert.equal(close.code, 1008);
    assert.equal(close.reason, "Target expired");
    assert.ok(!(await request(host.port, "/json/list")).body.some((entry) => entry.id === targetId));
  });
});
