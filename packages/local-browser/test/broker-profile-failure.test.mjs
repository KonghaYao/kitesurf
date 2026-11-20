// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// T4 私有故障注入：真实 workerd + SQLite。sqlite commit 后把一次回复改成 409，下一次
// 提交必须先用同一 revision/state 幂等重试，再提交新 snapshot；另一次 422 证明被拒绝的
// state 可以纠正后重新保存。控制文件只存在于测试临时目录。

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { test } from "node:test";

import { connect, evaluate, openPage } from "./helpers.mjs";
import { required, startNativeHost, waitFor } from "./host-adapter.mjs";

const nativeHost = required("KITESURF_NATIVE_HOST");
const workerd = required("KITESURF_WORKERD");
const configDir = required("KITESURF_CONFIG_DIR");
const engineAssets = required("KITESURF_ENGINE_ASSETS");
const assetsManifest = join(configDir, "assets-manifest.json");
process.env.KITESURF_NATIVE_HOST = nativeHost;

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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
      "<!doctype html><html><head><title>ks-profile-failure</title></head><body>" +
        "<script>window.boot=true;</script></body></html>",
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

function setFaults(path, verbs) {
  const staging = `${path}.next`;
  writeFileSync(staging, verbs.map((verb) => `${verb}\n`).join(""));
  renameSync(staging, path);
}

function profileEvents(path) {
  let text;
  try {
    text = readFileSync(path, "utf8");
  } catch {
    return [];
  }
  const events = [];
  for (const line of text.split("\n")) {
    if (!line.trim()) continue;
    try {
      const event = JSON.parse(line);
      if (event.kind === "profile") events.push(event);
    } catch {}
  }
  return events;
}

function waitForSave(path, predicate, description) {
  return waitFor(
    () => profileEvents(path).find((event) => event.operation === "save" && predicate(event)),
    description,
    30_000,
  );
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

function storage(state, origin) {
  const entry = state.localStorage.find(([found]) => found === origin);
  return entry ? Object.fromEntries(entry[1]) : {};
}

test("profile failure", { timeout: 300_000 }, async (t) => {
  const site = await fixture(t);
  const pageUrl = `${site.url}/`;
  const directory = mkdtempSync(join(tmpdir(), "ks-broker-failure-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const profile = join(directory, "profile.sqlite");
  const faults = join(directory, "faults.txt");
  const observer = join(directory, "observe.jsonl");
  setFaults(faults, []);
  const created = spawnSync(nativeHost, ["profile", "create", profile], { encoding: "utf8" });
  assert.equal(created.status, 0, created.stderr);
  const host = await startNativeHost({
    workerd,
    config: join(configDir, "local-private.capnp"),
    assets: engineAssets,
    assetsManifest,
    profile,
    observe: observer,
    env: { KITESURF_TEST_FAULTS: faults },
  });
  let stopped = false;
  t.after(async () => {
    if (stopped) return;
    try {
      await host.stop("SIGTERM", 30_000);
    } catch {}
  });

  const cdp = await connect(host.port, "?owner=alice");
  t.after(() => cdp.disconnect());
  const { sessionId } = await openPage(cdp, pageUrl);
  let previousRevision = -1;
  await waitFor(
    async () => {
      const revision = (await request(host.port, "/session")).body.profile.revision;
      const stable = revision === previousRevision;
      previousRevision = revision;
      return stable;
    },
    "the profile revision to settle",
    15_000,
  );

  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const secrets = {
    first: `ks-synthetic-first-${suffix}`,
    second: `ks-synthetic-second-${suffix}`,
    third: `ks-synthetic-third-${suffix}`,
    fourth: `ks-synthetic-fourth-${suffix}`,
  };
  const writePhase = (value) =>
    evaluate(cdp, sessionId, `localStorage.setItem("phase", ${JSON.stringify(value)})`);

  setFaults(faults, ["pause-save"]);
  await delay(300);
  const baseRevision = (await request(host.port, "/session")).body.profile.revision;
  assert.ok(Number.isSafeInteger(baseRevision));
  const pausedCount = profileEvents(observer).length;
  await writePhase(secrets.first);
  await delay(500);
  assert.equal(
    profileEvents(observer).length,
    pausedCount,
    "pause-save must hold the save before the observer records it",
  );

  setFaults(faults, ["drop-save-reply"]);
  const lost = await waitForSave(observer, (event) => event.status === 409, "the lost save");
  const failedRevision = lost.revision;
  assert.equal(failedRevision, baseRevision + 1);

  await writePhase(secrets.second);
  await delay(300);
  const flushed = await request(host.port, "/flush", { method: "POST" });
  assert.equal(flushed.status, 200);
  await waitForSave(
    observer,
    (event) => event.revision === failedRevision && event.status === 200,
    "the idempotent retry at the failed revision",
  );
  await waitForSave(
    observer,
    (event) => event.revision === failedRevision + 1 && event.status === 200,
    "the new snapshot after the retry",
  );

  setFaults(faults, ["pause-save"]);
  await delay(300);
  const rejectedPause = profileEvents(observer).length;
  await writePhase(secrets.third);
  await delay(500);
  assert.equal(
    profileEvents(observer).length,
    rejectedPause,
    "pause-save must hold the rejected save too",
  );
  setFaults(faults, ["reject-save"]);
  const rejected = await waitForSave(
    observer,
    (event) => event.status === 422,
    "the rejected save",
  );
  const rejectedRevision = rejected.revision;
  assert.equal(rejectedRevision, failedRevision + 2);

  await writePhase(secrets.fourth);
  await delay(300);
  const corrected = await request(host.port, "/flush", { method: "POST" });
  assert.equal(corrected.status, 200);
  await waitForSave(
    observer,
    (event) => event.revision === rejectedRevision && event.status === 200,
    "the corrected save after the rejection",
  );
  const sequence = profileEvents(observer)
    .filter((event) => event.operation === "save" && event.revision >= baseRevision + 1)
    .map((event) => [event.revision, event.status]);
  t.diagnostic(`profile save sequence ${JSON.stringify(sequence)}`);
  assert.deepEqual(sequence, [
    [baseRevision + 1, 409],
    [baseRevision + 1, 200],
    [baseRevision + 2, 200],
    [baseRevision + 3, 422],
    [baseRevision + 3, 200],
  ]);

  await host.stop("SIGTERM", 30_000);
  stopped = true;
  const persisted = readProfile(profile);
  assert.equal(persisted.revision, rejectedRevision);
  assert.equal(storage(persisted.state, site.url).phase, secrets.fourth);
  assert.ok(!persisted.text.includes(secrets.third), "a rejected state was never written");

  const observed = readFileSync(observer, "utf8");
  for (const value of Object.values(secrets)) {
    assert.ok(!host.stdout.includes(value), "a synthetic secret reached stdout");
    assert.ok(!host.stderr.includes(value), "a synthetic secret reached stderr");
    assert.ok(!observed.includes(value), "a synthetic secret reached the observer");
    assert.ok(!JSON.stringify(flushed.body).includes(value));
    assert.ok(!JSON.stringify(corrected.body).includes(value));
  }
});
