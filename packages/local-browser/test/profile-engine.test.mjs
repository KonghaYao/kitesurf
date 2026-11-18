// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T3 的真实 engine + SQLite profile 冒烟：Node 播种、workerd 加载、CDP 修改、flush 落盘、
// 退出后读库、重新打开恢复，以及 CLI 备份后用备份启动。断言读取磁盘和页面状态，不读日志。

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { existsSync, mkdtempSync, readFileSync, realpathSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";

import { ProfileStore } from "../src/profile-store.mjs";
import { connect, evaluate, navigate } from "./helpers.mjs";
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
      `<!doctype html><html><head><title>${route}</title></head><body>` +
        `<h1 id="route">${route}</h1>` +
        `<script>window.boot=true;</script></body></html>`,
    );
  });
  server.listen(0, "127.0.0.1");
  t.after(() => server.close());
  return once(server, "listening").then(() => ({
    url: `http://127.0.0.1:${server.address().port}`,
  }));
}

async function startHost(t, profile) {
  const host = await startNativeHost({
    workerd,
    config: join(configDir, "local-private.capnp"),
    assets: engineAssets,
    assetsManifest,
    profile,
  });
  t.after(async () => {
    try {
      await host.stop("SIGTERM", 20_000);
    } catch {}
  });
  return host;
}

async function openPage(t, host, url) {
  const cdp = await connect(host.port);
  t.after(() => cdp.disconnect());
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Page.enable", {}, sessionId);
  await navigate(cdp, sessionId, url);
  return { cdp, sessionId };
}

function readProfile(path) {
  const database = new DatabaseSync(path);
  try {
    const row = database.prepare("SELECT revision, state FROM profile WHERE id=1").get();
    return { revision: row.revision, state: JSON.parse(row.state), text: row.state };
  } finally {
    database.close();
  }
}

function seedProfile(path, origin) {
  const store = new ProfileStore(path, { create: !existsSync(path) });
  store.acquire("seed");
  const state = {
    cookies: [
      {
        name: "seed",
        value: "disk",
        host: "127.0.0.1",
        path: "/",
        secure: false,
        httpOnly: false,
        sameSite: "Lax",
        hostOnly: true,
        expiresAt: null,
        createdAt: 1,
      },
      {
        name: "sid",
        value: "http-only-disk",
        host: "127.0.0.1",
        path: "/",
        secure: false,
        httpOnly: true,
        sameSite: "Lax",
        hostOnly: true,
        expiresAt: null,
        createdAt: 2,
      },
    ],
    localStorage: [[origin, [["seed", "disk-value"]]]],
  };
  store.save("seed", 1, state);
  store.close();
  return state;
}

function cookie(state, name) {
  return state.cookies.find((entry) => entry.name === name);
}

function storage(state, origin) {
  const entry = state.localStorage.find(([found]) => found === origin);
  return entry ? Object.fromEntries(entry[1]) : {};
}

test(
  "CDP changes reach SQLite and survive a host restart",
  { timeout: 300_000 },
  async (t) => {
    const site = await fixture(t);
    const directory = mkdtempSync(join(tmpdir(), "ks-profile-engine-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const path = join(directory, "account.sqlite");
    seedProfile(path, site.url);
    assert.equal(readProfile(path).revision, 1);

    const first = await startHost(t, path);
    assert.equal(first.profile, realpathSync(path));
    const session = await first.request("/session");
    assert.equal(session.body.recovery, "profile-restored");
    assert.equal(session.body.profile.revision, 1);
    assert.equal(session.body.profile.format, 1);
    assert.match(session.body.profile.id, /^[0-9a-f]{16}$/);

    const page = await openPage(t, first, `${site.url}/a`);
    assert.equal(
      await waitFor(
        () => evaluate(page.cdp, page.sessionId, "localStorage.getItem('seed')"),
        "the seeded localStorage value",
      ),
      "disk-value",
    );
    assert.match(await evaluate(page.cdp, page.sessionId, "document.cookie"), /seed=disk/);
    assert.ok(!(await evaluate(page.cdp, page.sessionId, "document.cookie")).includes("sid="));
    const cookies = await evaluate(
      page.cdp,
      page.sessionId,
      "(() => { document.cookie='saved=disk; path=/'; localStorage.setItem('k','v'); return document.cookie; })()",
    );
    assert.match(cookies, /saved=disk/);
    const flushed = await first.request("/flush", { method: "POST" });
    assert.equal(flushed.status, 200);
    assert.ok(flushed.body.revision >= 2);
    await first.stop("SIGTERM", 20_000);

    const persisted = readProfile(path);
    assert.ok(persisted.revision >= 2);
    assert.equal(cookie(persisted.state, "seed").value, "disk");
    assert.equal(cookie(persisted.state, "saved").value, "disk");
    assert.equal(cookie(persisted.state, "sid").value, "http-only-disk");
    assert.equal(cookie(persisted.state, "sid").httpOnly, true);
    assert.equal(storage(persisted.state, site.url).seed, "disk-value");
    assert.equal(storage(persisted.state, site.url).k, "v");

    const second = await startHost(t, path);
    const restored = await second.request("/session");
    assert.equal(restored.body.recovery, "profile-restored");
    assert.equal(restored.body.profile.revision, persisted.revision);
    const reopened = await openPage(t, second, `${site.url}/a`);
    assert.equal(
      await waitFor(
        () => evaluate(reopened.cdp, reopened.sessionId, "localStorage.getItem('k')"),
        "the restored localStorage value",
      ),
      "v",
    );
    assert.match(
      await evaluate(reopened.cdp, reopened.sessionId, "document.cookie"),
      /saved=disk/,
    );
    await second.stop("SIGTERM", 20_000);
  },
);

test(
  "the CLI backup boots the engine with the saved cookies and storage",
  { timeout: 300_000 },
  async (t) => {
    const site = await fixture(t);
    const directory = mkdtempSync(join(tmpdir(), "ks-profile-cli-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const path = join(directory, "account.sqlite");
    const backup = join(directory, "backup.sqlite");

    const created = spawnSync(nativeHost, ["profile", "create", path], { encoding: "utf8" });
    assert.equal(created.status, 0, created.stderr);
    const createdStatus = JSON.parse(created.stdout);
    assert.equal(createdStatus.revision, 0);
    assert.equal(createdStatus.format, 1);
    assert.equal(createdStatus.path, realpathSync(path));
    const original = readFileSync(path);
    const again = spawnSync(nativeHost, ["profile", "create", path], { encoding: "utf8" });
    assert.notEqual(again.status, 0);
    assert.deepEqual(readFileSync(path), original, "create never overwrites");
    const status = spawnSync(nativeHost, ["profile", "status", path], { encoding: "utf8" });
    assert.equal(status.status, 0, status.stderr);
    assert.equal(JSON.parse(status.stdout).revision, 0);

    seedProfile(path, site.url);
    const seeded = spawnSync(nativeHost, ["profile", "status", path], { encoding: "utf8" });
    assert.equal(JSON.parse(seeded.stdout).revision, 1);

    const first = await startHost(t, path);
    const page = await openPage(t, first, `${site.url}/a`);
    await waitFor(
      () => evaluate(page.cdp, page.sessionId, "localStorage.getItem('seed')"),
      "the seeded localStorage value",
    );
    await evaluate(
      page.cdp,
      page.sessionId,
      "(() => { document.cookie='backup=yes; path=/'; localStorage.setItem('backup-key','backup-value'); })()",
    );
    const flushed = await first.request("/flush", { method: "POST" });
    assert.equal(flushed.status, 200);
    await first.stop("SIGTERM", 20_000);

    const backedUp = spawnSync(nativeHost, ["profile", "backup", path, "--output", backup], {
      encoding: "utf8",
    });
    assert.equal(backedUp.status, 0, backedUp.stderr);
    assert.equal(JSON.parse(backedUp.stdout).path, realpathSync(backup));
    assert.equal(statSync(backup).mode & 0o777, 0o600);
    const backupBytes = readFileSync(backup);
    const refused = spawnSync(nativeHost, ["profile", "backup", path, "--output", backup], {
      encoding: "utf8",
    });
    assert.notEqual(refused.status, 0);
    assert.deepEqual(readFileSync(backup), backupBytes, "backup never overwrites");

    const restored = await startHost(t, backup);
    const session = await restored.request("/session");
    assert.equal(session.body.recovery, "profile-restored");
    assert.ok(session.body.profile.revision >= 2);
    const reopened = await openPage(t, restored, `${site.url}/a`);
    assert.equal(
      await waitFor(
        () =>
          evaluate(reopened.cdp, reopened.sessionId, "localStorage.getItem('backup-key')"),
        "the backup localStorage value",
      ),
      "backup-value",
    );
    assert.match(
      await evaluate(reopened.cdp, reopened.sessionId, "document.cookie"),
      /backup=yes/,
    );
    await restored.stop("SIGTERM", 20_000);
  },
);
