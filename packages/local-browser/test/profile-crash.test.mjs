// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 跨语言崩溃契约：Rust 真实 COMMIT 后 SIGKILL、Node 重开并精确重试；Node COMMIT 后
// kill Rust 写者再重开；丢失回复在两种方向都只能以同一 revision 与同一 TEXT 重试。
// 只 kill 本文件 spawn 的 PID，并等待 exit；断言读取 SQLite 的实际 TEXT。

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";

import { ProfileStore, validateState } from "../src/profile-store.mjs";
import { required } from "./host-adapter.mjs";

const harnessBinary = required("KITESURF_PROFILE_HARNESS");
const SAMPLE = 667082108456853.2;

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), "ks-profile-crash-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return { directory, path: join(directory, "account.sqlite") };
}

function storedText(path) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT revision, state FROM profile WHERE id=1").get();
  } finally {
    database.close();
  }
}

function alive(child) {
  return child.exitCode === null && child.signalCode === null;
}

function liveHarness(t) {
  const child = spawn(harnessBinary, [], { stdio: ["pipe", "pipe", "inherit"] });
  const exited = once(child, "exit");
  const waiters = [];
  let buffer = "";
  let closed = false;
  let failure;
  child.stdout.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    buffer += chunk;
    for (;;) {
      const index = buffer.indexOf("\n");
      if (index < 0) break;
      const line = buffer.slice(0, index);
      buffer = buffer.slice(index + 1);
      if (line.trim() === "") continue;
      const resolve = waiters.shift();
      if (resolve) resolve(JSON.parse(line));
    }
  });
  exited.then(() => {
    failure = new Error("profile harness exited");
    while (waiters.length) waiters.shift()(failure);
  });
  t.after(async () => {
    if (closed) return;
    closed = true;
    if (alive(child)) {
      child.kill("SIGKILL");
      await exited;
    }
  });
  return {
    child,
    async send(command) {
      if (failure) throw failure;
      return new Promise((resolve) => {
        waiters.push(resolve);
        child.stdin.write(JSON.stringify(command) + "\n");
      });
    },
    async kill() {
      if (closed) return;
      closed = true;
      if (alive(child)) child.kill("SIGKILL");
      await exited;
      assert.equal(child.signalCode, "SIGKILL");
    },
    async close() {
      if (closed) return;
      closed = true;
      child.stdin.end();
      await exited;
    },
  };
}

async function runSilentHarness(t, commands) {
  const child = spawn(harnessBinary, [], { stdio: ["pipe", "ignore", "inherit"] });
  const exited = once(child, "exit");
  t.after(async () => {
    if (!alive(child)) return;
    child.kill("SIGKILL");
    await exited;
  });
  child.stdin.end(commands.map((command) => JSON.stringify(command)).join("\n") + "\n");
  const [code, signal] = await exited;
  assert.equal(code, 0, `silent harness failed: ${signal}`);
  assert.equal(signal, null);
}

test("a Rust COMMIT survives SIGKILL and Node retries it byte for byte", async (t) => {
  const { directory, path } = fixture(t);
  const state = { cookies: [], localStorage: [], x: SAMPLE };
  const oracle = validateState(state);
  const live = liveHarness(t);
  assert.equal((await live.send({ command: "open", path, create: true })).ok, true);
  assert.equal((await live.send({ command: "acquire", owner: "owner" })).ok, true);
  assert.equal(
    (await live.send({ command: "save", owner: "owner", revision: "1", state: oracle })).ok,
    true,
  );
  await live.kill();
  assert.equal(storedText(path).revision, 1);
  assert.equal(storedText(path).state, oracle);

  const nodeStore = new ProfileStore(path);
  assert.equal(nodeStore.acquire("owner").revision, 1);
  nodeStore.save("owner", 1, state);
  assert.throws(() => nodeStore.save("owner", 1, { ...state, x: 1 }), /Stale/);
  const backup = join(directory, "backup.sqlite");
  assert.equal(nodeStore.backup(backup), backup);
  nodeStore.close();
  assert.equal(storedText(path).revision, 1);
  assert.equal(storedText(path).state, oracle, "exact retry after the crash");

  const reader = liveHarness(t);
  assert.equal((await reader.send({ command: "open", path: backup })).ok, true);
  const read = await reader.send({ command: "acquire", owner: "owner" });
  assert.equal(read.revision, 1);
  assert.equal(read.state, oracle);
  await reader.close();
});

test("Node COMMIT then a killed Rust writer reopens and retries", async (t) => {
  const { path } = fixture(t);
  const first = { cookies: [], localStorage: [], x: SAMPLE };
  validateState(first);
  const second = { cookies: [], localStorage: [], x: -SAMPLE };
  const secondOracle = validateState(second);

  const nodeStore = new ProfileStore(path, { create: true });
  nodeStore.acquire("owner");
  nodeStore.save("owner", 1, first);
  nodeStore.close();

  const live = liveHarness(t);
  assert.equal((await live.send({ command: "open", path })).ok, true);
  assert.equal((await live.send({ command: "acquire", owner: "owner" })).ok, true);
  assert.equal(
    (await live.send({ command: "save", owner: "owner", revision: "2", state: secondOracle })).ok,
    true,
  );
  await live.kill();
  assert.equal(storedText(path).revision, 2);
  assert.equal(storedText(path).state, secondOracle);

  const reopened = new ProfileStore(path);
  assert.equal(reopened.acquire("owner").revision, 2);
  assert.equal(reopened.read().state.x, -SAMPLE);
  reopened.save("owner", 2, second);
  assert.throws(() => reopened.save("owner", 2, first), /Stale/);
  reopened.close();
  assert.equal(storedText(path).revision, 2);
  assert.equal(storedText(path).state, secondOracle);
});

test("lost replies retry exactly after real commits in both directions", async (t) => {
  const state = { cookies: [], localStorage: [], x: SAMPLE };
  const oracle = validateState(state);
  const changed = validateState({ ...state, x: -SAMPLE });

  const fromRust = fixture(t);
  await runSilentHarness(t, [
    { command: "open", path: fromRust.path, create: true },
    { command: "acquire", owner: "owner" },
    { command: "save", owner: "owner", revision: "1", state: oracle },
  ]);
  assert.equal(storedText(fromRust.path).revision, 1);
  assert.equal(storedText(fromRust.path).state, oracle);

  const nodeStore = new ProfileStore(fromRust.path);
  assert.equal(nodeStore.acquire("owner").revision, 1);
  nodeStore.save("owner", 1, state);
  assert.throws(() => nodeStore.save("owner", 1, { ...state, x: -SAMPLE }), /Stale/);
  nodeStore.close();
  assert.equal(storedText(fromRust.path).state, oracle);
  assert.equal(storedText(fromRust.path).revision, 1);

  const fromNode = fixture(t);
  const writer = new ProfileStore(fromNode.path, { create: true });
  writer.acquire("owner");
  writer.save("owner", 1, state);
  writer.close();

  const live = liveHarness(t);
  assert.equal((await live.send({ command: "open", path: fromNode.path })).ok, true);
  assert.equal((await live.send({ command: "acquire", owner: "owner" })).ok, true);
  assert.equal(
    (await live.send({ command: "save", owner: "owner", revision: "1", state: oracle })).ok,
    true,
  );
  const rejected = await live.send({
    command: "save",
    owner: "owner",
    revision: "1",
    state: changed,
  });
  assert.equal(rejected.kind, "conflict");
  assert.equal((await live.send({ command: "status" })).revision, 1);
  await live.close();
  assert.equal(storedText(fromNode.path).state, oracle);
});
