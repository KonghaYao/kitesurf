// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T0 契约分三层：两种宿主都必须满足的公共 HTTP/CDP 协议、native 宿主的 T2 内存探针，
// 以及 035a014 Node 宿主的持久化 profile 与 CLI 基线。KS_HOST_KIND=legacy 选择后者。

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdtempSync, realpathSync, rmSync } from "node:fs";
import { request as httpRequest } from "node:http";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { after, before, test } from "node:test";

import { connect, evaluate } from "./helpers.mjs";
import {
  required,
  startLegacyHost,
  startNativeHost,
  waitFor,
  withTimeout,
} from "./host-adapter.mjs";

const kind = process.env.KS_HOST_KIND || "native";
const hostVersion = kind === "legacy" ? "0.5.0" : "0.5.4";
const legacyOnly = { skip: kind !== "legacy" && "035a014 Node host baseline only" };
const nativeOnly = { skip: kind !== "native" && "native T2 test-profile probe only" };
let host;
let cdp;

const LEGACY_HELP = `Kitesurf 0.5.0 — a local browser with persistent profiles

kitesurf serve [--profile FILE] [--create-profile] [--port 8787]
               [--allow-private-network]
kitesurf profile create FILE
kitesurf status|flush|stop [--port 8787]
kitesurf backup --output FILE [--port 8787]
kitesurf control --owner NAME [--takeover] [--port 8787]

The default profile is created once under your user data directory.
An explicit --profile must already exist unless --create-profile is used.
Disconnecting CDP keeps tabs alive. Browser.close saves and exits.
`;

/** @param {string[]} args @returns {import("node:child_process").SpawnSyncReturns<string>} */
function legacyCli(args) {
  return spawnSync(process.execPath, [join(host.root, "src/cli.mjs"), ...args], {
    encoding: "utf8",
    timeout: 30_000,
  });
}

/** @param {string} path @returns {string} */
function profileId(path) {
  return createHash("sha256").update(path).digest("hex").slice(0, 16);
}

/** @returns {Promise<boolean>} */
async function portIsFree(port) {
  const server = createServer();
  try {
    await new Promise((resolve, reject) => {
      server.once("error", reject);
      server.listen(port, "127.0.0.1", resolve);
    });
    return true;
  } catch {
    return false;
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

before(async () => {
  if (kind === "legacy") {
    host = await startLegacyHost({
      root: required("KS_LEGACY_HOST"),
      dist: required("KS_LEGACY_DIST"),
      assets: required("KS_LEGACY_ASSETS"),
    });
  } else {
    const configDir = required("KITESURF_CONFIG_DIR");
    host = await startNativeHost({
      workerd: required("KITESURF_WORKERD"),
      config: `${configDir}/local-private.capnp`,
      assets: required("KITESURF_ENGINE_ASSETS"),
      assetsManifest: `${configDir}/assets-manifest.json`,
    });
  }
});

after(async () => {
  try {
    if (cdp?.ws?.readyState === 1) await cdp.disconnect();
  } catch {}
  if (host) {
    try {
      await host.stop("SIGTERM", 30_000);
    } catch {}
  }
});

test("health reports the version and a live engine", async () => {
  const { status, body } = await host.request("/health");
  assert.equal(status, 200);
  assert.equal(body.alive, true);
  assert.equal(body.version, hostVersion);
});

test("json/version points at the browser endpoint", async () => {
  const { status, body } = await host.request("/json/version");
  assert.equal(status, 200);
  assert.equal(body.Browser, `Kitesurf/${hostVersion}`);
  assert.equal(body["Protocol-Version"], "1.3");
  assert.equal(body.webSocketDebuggerUrl, `ws://127.0.0.1:${host.port}/devtools/browser`);
});

test("json/list describes each page with both debugger URLs", async () => {
  const { status, body } = await host.request("/json/list");
  assert.equal(status, 200);
  assert.ok(Array.isArray(body));
  assert.ok(body.length >= 1);
  for (const entry of body) {
    assert.equal(entry.type, "page");
    assert.equal(
      entry.webSocketDebuggerUrl,
      `ws://127.0.0.1:${host.port}/devtools/page/${entry.id}`,
    );
    assert.match(
      entry.devtoolsFrontendUrl,
      /^devtools:\/\/devtools\/bundled\/inspector\.html\?ws=/,
    );
  }
});

test("the ready line uses the shared startup fields", () => {
  assert.deepEqual(Object.keys(host.ready).sort(), ["generation", "port", "profile", "ready"]);
  assert.equal(host.ready.ready, true);
  assert.equal(typeof host.ready.port, "number");
  assert.equal(host.ready.port, host.port);
  assert.equal(host.ready.generation, host.generation);
  assert.equal(typeof host.ready.profile, "string");
  assert.ok(host.ready.profile.length > 0);
});

test("session exposes the shared discovery fields", async () => {
  const { status, body } = await host.request("/session");
  assert.equal(status, 200);
  assert.equal(body.version, hostVersion);
  assert.equal(body.generation, host.generation);
  assert.equal(body.alive, true);
  assert.equal(typeof body.sessionId, "string");
  assert.ok(body.sessionId.length > 0);
  assert.match(body.recovery, /^(new-profile|profile-restored)$/);
  assert.ok(Array.isArray(body.tabs));
  assert.equal(typeof body.profile.revision, "number");
});

test("the root document advertises the CDP address", async () => {
  const { status, body } = await host.request("/");
  assert.equal(status, 200);
  assert.equal(body.cdp, `ws://127.0.0.1:${host.port}/devtools/browser`);
  assert.equal(typeof body.inspect, "string");
  assert.equal(typeof body.profile, "string");
});

test("flush completes through the engine profile bridge", async () => {
  const { status, body } = await host.request("/flush", { method: "POST" });
  assert.equal(status, 200);
  assert.equal(typeof body.revision, "number");
});

test("native profile is an in-memory T2 test double", nativeOnly, async () => {
  // 共用契约宿主仍显式启用内存替身，保持 T0/T2 断言不变；正式 SQLite 由本包的
  // profile_compat_test 与 profile_engine_smoke_test 单独证明，不伪造磁盘元数据。
  assert.equal(host.profile, "memory");
  assert.equal(host.ready.profile, "memory");
  const session = await host.request("/session");
  assert.deepEqual(Object.keys(session.body.profile).sort(), [
    "bytes",
    "loads",
    "revision",
    "saves",
  ]);
  for (const field of ["bytes", "loads", "revision", "saves"])
    assert.equal(typeof session.body.profile[field], "number");
  assert.ok(session.body.profile.bytes > 0);
  const flush = await host.request("/flush", { method: "POST" });
  assert.deepEqual(Object.keys(flush.body).sort(), ["bytes", "loads", "revision", "saves"]);
  const root = await host.request("/");
  assert.equal(root.body.profile, "memory");
});

test("legacy session and flush expose the persisted profile identity", legacyOnly, async () => {
  const session = await host.request("/session");
  const flush = await host.request("/flush", { method: "POST" });
  for (const profile of [session.body.profile, flush.body]) {
    assert.deepEqual(Object.keys(profile).sort(), ["format", "id", "path", "revision"]);
    assert.equal(profile.path, host.profile);
    assert.ok(isAbsolute(profile.path));
    assert.match(profile.path, /\.sqlite$/);
    assert.equal(profile.format, 1);
    assert.equal(typeof profile.revision, "number");
    assert.match(profile.id, /^[0-9a-f]{16}$/);
    assert.equal(profile.id, profileId(profile.path));
  }
});

test("legacy root advertises the persisted profile path", legacyOnly, async () => {
  const root = await host.request("/");
  const session = await host.request("/session");
  assert.equal(root.status, 200);
  assert.equal(root.body.profile, host.profile);
  assert.ok(isAbsolute(root.body.profile));
  assert.equal(session.body.profile.path, root.body.profile);
  assert.equal(session.body.profile.id, profileId(root.body.profile));
});

test("legacy CLI help, version and errors keep their exact samples", legacyOnly, () => {
  const version = legacyCli(["--version"]);
  assert.equal(version.status, 0);
  assert.equal(version.stdout, "kitesurf 0.5.0\n");
  assert.equal(version.stderr, "");

  const help = legacyCli(["--help"]);
  assert.equal(help.status, 0);
  assert.equal(help.stdout, `${LEGACY_HELP}\n`);
  assert.equal(help.stderr, "");

  const errors = [
    [["bogus"], "kitesurf: Unknown command: bogus\n"],
    [["serve", "--dist"], "kitesurf: Unknown or incomplete argument: --dist\n"],
    [["serve", "--port", "99999"], "kitesurf: Invalid port\n"],
    [["backup"], "kitesurf: backup requires --output FILE\n"],
    [["control"], "kitesurf: control requires --owner NAME\n"],
  ];
  for (const [args, message] of errors) {
    const result = legacyCli(args);
    assert.equal(result.status, 1, args.join(" "));
    assert.equal(result.stdout, "");
    assert.equal(result.stderr, message);
  }

  const missing = legacyCli(["serve", "--profile", join(tmpdir(), `ks-absent-${host.pid}.sqlite`)]);
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /^kitesurf: Cannot open profile .+: ENOENT/);
});

test("legacy CLI profile create reports the baseline file identity", legacyOnly, (t) => {
  const directory = mkdtempSync(join(tmpdir(), "ks-legacy-profile-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const result = legacyCli(["profile", "create", join(directory, "created.sqlite")]);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stderr, "");
  const created = JSON.parse(result.stdout);
  assert.deepEqual(Object.keys(created).sort(), ["format", "id", "path", "revision"]);
  assert.equal(created.path, realpathSync(join(directory, "created.sqlite")));
  assert.equal(created.format, 1);
  assert.equal(created.revision, 0);
  assert.equal(created.id, profileId(created.path));
  assert.ok(existsSync(created.path));
});

test("legacy CLI creates the default profile under XDG_DATA_HOME", legacyOnly, async (t) => {
  const dataHome = mkdtempSync(join(tmpdir(), "ks-legacy-data-"));
  const other = await startLegacyHost({
    root: host.root,
    dist: host.dist,
    assets: host.assets,
    port: 0,
    profilePath: null,
    createProfile: false,
    env: { XDG_DATA_HOME: dataHome },
  });
  t.after(async () => {
    try {
      await other.stop("SIGTERM", 30_000);
    } catch {}
    rmSync(dataHome, { recursive: true, force: true });
  });
  const expected = join(dataHome, "kitesurf", "default.sqlite");
  assert.equal(other.profile, realpathSync(expected));
  assert.ok(existsSync(expected));
  const session = await other.request("/session");
  assert.equal(session.body.profile.path, other.profile);
  assert.equal(session.body.profile.id, profileId(other.profile));
});

test("legacy CLI defaults to port 8787", legacyOnly, async (t) => {
  if (!(await portIsFree(8787))) {
    t.skip("port 8787 is already in use");
    return;
  }
  const other = await startLegacyHost({
    root: host.root,
    dist: host.dist,
    assets: host.assets,
    port: null,
  });
  t.after(async () => {
    try {
      await other.stop("SIGTERM", 30_000);
    } catch {}
  });
  assert.equal(other.port, 8787);
  assert.equal(other.ready.port, 8787);
});

test("the CDP bootstrap creates, attaches and evaluates", async () => {
  cdp = await connect(host.port);
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Page.enable", {}, sessionId);
  const tree = await cdp.send("Page.getFrameTree", {}, sessionId);
  await cdp.send(
    "Page.setDocumentContent",
    {
      frameId: tree.frameTree.frame.id,
      html: "<!doctype html><title>contract</title><body><p id='value'>kitesurf</p></body>",
    },
    sessionId,
  );
  assert.equal(await evaluate(cdp, sessionId, "document.title"), "contract");
  assert.equal(
    await evaluate(cdp, sessionId, "document.getElementById('value').textContent"),
    "kitesurf",
  );

  const info = await cdp.send("Kitesurf.getSessionInfo", {});
  const session = await host.request("/session");
  assert.equal(info.sessionId, session.body.sessionId);
  assert.equal(info.connectionMode, "browser");
  assert.equal(info.environment, "local");
  assert.equal(info.buildVersion, hostVersion);
});

test("the Host header gate rejects a foreign origin", async () => {
  const status = await new Promise((resolve, reject) => {
    const request = httpRequest(
      {
        host: "127.0.0.1",
        port: host.port,
        path: "/health",
        headers: { host: "evil.example" },
      },
      (response) => {
        response.resume();
        resolve(response.statusCode);
      },
    );
    request.on("error", reject);
    request.end();
  });
  assert.equal(status, 403);
});

test("Browser.close flushes and exits the host cleanly", async () => {
  const result = await cdp.send("Browser.close", {});
  assert.deepEqual(result, {});
  await waitFor(() => cdp.ws.readyState === 3, "the CDP socket closes");
  const statusAfter = await withTimeout(host.exit, 30_000);
  assert.ok(!statusAfter.timeout, "host exits after close");
  assert.equal(statusAfter.code, 0);
  if (kind === "native") {
    const shutdown = host
      .events()
      .filter((event) => event.kind === "shutdown")
      .at(-1);
    assert.deepEqual(
      { clean: shutdown.clean, dirRemoved: shutdown.dirRemoved },
      { clean: true, dirRemoved: true },
    );
    const exit = host
      .events()
      .filter((event) => event.kind === "engine-exit")
      .at(-1);
    assert.equal(exit.reaped, true);
    assert.ok(!existsSync(host.runDir()), "the private run directory is gone");
  }
});
