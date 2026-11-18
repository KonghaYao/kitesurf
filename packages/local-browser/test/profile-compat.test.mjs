// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Node ProfileStore 与 Rust profile 的双向兼容：原始 state TEXT、JSON.stringify 语义、
// revision 重试/拒绝、跨进程锁、备份互读和配额。断言比较磁盘 TEXT，不做结构 deepEqual。

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { DatabaseSync } from "node:sqlite";

import { ProfileStore, validateState } from "../src/profile-store.mjs";
import { required } from "./host-adapter.mjs";

const harnessBinary = required("KITESURF_PROFILE_HARNESS");

function fixture(t) {
  const directory = mkdtempSync(join(tmpdir(), "ks-profile-compat-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return { directory, path: join(directory, "account.sqlite") };
}

function run(commands) {
  const result = spawnSync(harnessBinary, [], {
    input: commands.map((command) => JSON.stringify(command)).join("\n") + "\n",
    encoding: "utf8",
    maxBuffer: 16 * 1024 * 1024,
    timeout: 120_000,
  });
  assert.equal(result.status, 0, `harness failed: ${result.stderr}`);
  const replies = result.stdout
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => JSON.parse(line));
  assert.equal(replies.length, commands.length, result.stdout);
  return replies;
}

async function liveHarness(t) {
  const child = spawn(harnessBinary, [], { stdio: ["pipe", "pipe", "inherit"] });
  t.after(() => child.kill("SIGKILL"));
  child.stdout.setEncoding("utf8");
  const waiters = [];
  let buffer = "";
  let failure;
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
  child.on("exit", () => {
    failure = new Error("profile harness exited");
    while (waiters.length) waiters.shift()(failure);
  });
  let closed = false;
  return {
    async send(command) {
      if (failure) throw failure;
      return new Promise((resolve) => {
        waiters.push(resolve);
        child.stdin.write(JSON.stringify(command) + "\n");
      });
    },
    async close() {
      if (closed) return;
      closed = true;
      child.stdin.end();
      await once(child, "exit");
    },
  };
}

function storedText(path) {
  const database = new DatabaseSync(path);
  try {
    return database.prepare("SELECT revision, state FROM profile WHERE id=1").get();
  } finally {
    database.close();
  }
}

const floatBits = new DataView(new ArrayBuffer(8));

function fromBits(value) {
  floatBits.setBigUint64(0, BigInt.asUintN(64, value), false);
  return floatBits.getFloat64(0, false);
}

function nextUp(value) {
  if (Number.isNaN(value) || value === Infinity) return value;
  if (value === 0) return Number.MIN_VALUE;
  floatBits.setFloat64(0, value, false);
  const step = value > 0 ? 1n : -1n;
  return fromBits(floatBits.getBigUint64(0, false) + step);
}

function nextDown(value) {
  return -nextUp(-value);
}

/** 固定 xorshift 序列；跳过非有限值（JSON 文本无法表示它们）。 */
function randomDoubles(count) {
  let state = 0x243f6a8885a308d3n;
  const values = [];
  while (values.length < count) {
    state = BigInt.asUintN(64, state ^ (state << 13n));
    state = BigInt.asUintN(64, state ^ (state >> 7n));
    state = BigInt.asUintN(64, state ^ (state << 17n));
    const value = fromBits(state);
    if (Number.isFinite(value)) values.push(value);
  }
  return values;
}

function numberSamples() {
  const samples = [
    667082108456853.2,
    667082108456853.3,
    -667082108456853.2,
    -667082108456853.3,
    0,
    -0,
    0.1,
    -0.1,
    1,
    -1,
    0.30000000000000004,
    Number.MIN_VALUE,
    -Number.MIN_VALUE,
    Number.MAX_VALUE,
    -Number.MAX_VALUE,
    Number.MIN_VALUE * 2,
    2.2250738585072014e-308,
    1e-7,
    -1e-7,
    1e-6,
    1e20,
    -1e20,
    1e21,
    -1e21,
    Number.MAX_SAFE_INTEGER,
    Number.MAX_SAFE_INTEGER + 1,
    -Number.MAX_SAFE_INTEGER,
    -Number.MAX_SAFE_INTEGER - 1,
    9007199254740993,
  ];
  for (const base of [
    0.1,
    667082108456853.2,
    1e21,
    Number.MIN_VALUE,
    2.2250738585072014e-308,
    Number.MAX_VALUE,
  ]) {
    for (const value of [nextDown(base), nextUp(base)]) {
      if (Number.isFinite(value)) samples.push(value);
    }
  }
  return samples.concat(randomDoubles(32));
}

function fullState(token) {
  return {
    cookies: [
      {
        name: "sid",
        value: token,
        host: "example.com",
        path: "/private",
        secure: true,
        httpOnly: true,
        sameSite: "Strict",
        hostOnly: true,
        expiresAt: 1.75e12,
        createdAt: 100.5,
      },
    ],
    localStorage: [
      ["https://example.com", [["token", token], ["__proto__", "safe"]]],
      ["http://127.0.0.1:8080", [["empty", ""]]],
    ],
    extra: { keep: true },
  };
}

test("a Node-written profile loads byte for byte in Rust", (t) => {
  const { path } = fixture(t);
  const nodeStore = new ProfileStore(path, { create: true });
  nodeStore.acquire("owner-node");
  nodeStore.save("owner-node", 1, fullState("T3"));
  const nodeStatus = nodeStore.status();
  nodeStore.close();
  const persisted = storedText(path);
  assert.equal(persisted.revision, 1);

  const [opened, acquired] = run([
    { command: "open", path },
    { command: "acquire", owner: "owner-node" },
  ]);
  assert.equal(opened.ok, true);
  assert.equal(opened.revision, 1);
  assert.equal(opened.format, 1);
  assert.equal(opened.id, nodeStatus.id);
  assert.equal(acquired.revision, 1);
  assert.equal(acquired.state, persisted.state);
});

test("a Rust-written profile reads back in Node with the same disk text", (t) => {
  const { path } = fixture(t);
  const state = fullState("rust");
  const oracle = validateState(state);
  const replies = run([
    { command: "open", path, create: true },
    { command: "acquire", owner: "owner-rust" },
    { command: "save", owner: "owner-rust", revision: "1", state: oracle },
    { command: "close" },
  ]);
  assert.deepEqual(
    replies.map((reply) => reply.ok),
    [true, true, true, true],
  );
  const persisted = storedText(path);
  assert.equal(persisted.revision, 1);
  assert.equal(persisted.state, oracle);

  const nodeStore = new ProfileStore(path);
  assert.deepEqual(nodeStore.read(), { revision: 1, state });
  assert.equal(nodeStore.status().id.length, 16);
  nodeStore.close();
});

test("JSON.stringify differential covers order, numbers and surrogate strings", (t) => {
  const cases = [
    `{"localStorage":[],"cookies":[],"1":4,"2":2,"b":1,"a":3}`,
    `{"cookies":[],"localStorage":[],"x":1.0}`,
    `{"cookies":[],"localStorage":[],"x":1e21}`,
    `{"cookies":[],"localStorage":[],"x":1e-7}`,
    `{"cookies":[],"localStorage":[],"x":0.000001}`,
    `{"cookies":[],"localStorage":[],"x":-0}`,
    `{"cookies":[],"localStorage":[],"x":9007199254740993}`,
    `{"cookies":[],"localStorage":[],"x":1e400}`,
    `{"cookies":[],"localStorage":[],"x":"\\ud800"}`,
    `{"cookies":[],"localStorage":[],"x":"\\ud83d\\ude00"}`,
    `{"cookies":[],"localStorage":[],"x":"\u{1f600}"}`,
    `{"cookies":[],"localStorage":[],"x":"\\u0000"}`,
    `{"cookies":[],"localStorage":[],"a":1,"a":2}`,
    `{"cookies":[],"localStorage":[],"__proto__":{"p":1}}`,
    ` { "cookies" : [], "localStorage" : [] } `,
    `{"cookies":[{"name":"a","value":"b","host":"h","path":"/","secure":false,"httpOnly":true,"hostOnly":true,"sameSite":null,"createdAt":0.5,"expiresAt":1e300}],"localStorage":[]}`,
  ];
  const directory = mkdtempSync(join(tmpdir(), "ks-profile-json-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const commands = [];
  const files = [];
  for (const [index, input] of cases.entries()) {
    const path = join(directory, `case-${index}.sqlite`);
    const oracle = validateState(JSON.parse(input));
    files.push({ path, oracle });
    commands.push(
      { command: "open", path, create: true },
      { command: "acquire", owner: "owner" },
      { command: "save", owner: "owner", revision: "1", state: input },
      { command: "close" },
    );
  }
  const replies = run(commands);
  for (const [index, reply] of replies.entries()) {
    assert.equal(reply.ok, true, `command ${index} failed: ${reply.message}`);
  }
  for (const { path, oracle } of files) {
    assert.equal(storedText(path).state, oracle, path);
    const nodeStore = new ProfileStore(path);
    assert.deepEqual(nodeStore.read().state, JSON.parse(oracle));
    nodeStore.close();
  }
  const surrogate = storedText(files[8].path).state;
  assert.equal(surrogate, '{"cookies":[],"localStorage":[],"x":"\\ud800"}');
  assert.equal(files[8].oracle, surrogate);
});

test("ECMAScript number formatting differential covers ties, subnormals and boundaries", (t) => {
  const samples = numberSamples();
  const directory = mkdtempSync(join(tmpdir(), "ks-profile-numbers-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const commands = [];
  const files = [];
  for (const [index, value] of samples.entries()) {
    const path = join(directory, `number-${index}.sqlite`);
    const state = { cookies: [], localStorage: [], x: value };
    const text = JSON.stringify(state);
    files.push({ path, value, oracle: validateState(state) });
    commands.push(
      { command: "open", path, create: true },
      { command: "acquire", owner: "owner" },
      { command: "save", owner: "owner", revision: "1", state: text },
      { command: "close" },
    );
  }
  const replies = run(commands);
  for (const [index, reply] of replies.entries()) {
    assert.equal(reply.ok, true, `command ${index} failed: ${reply.message}`);
  }
  for (const { path, value, oracle } of files) {
    const stored = storedText(path);
    assert.equal(stored.revision, 1, path);
    assert.equal(stored.state, oracle, path);
    const nodeStore = new ProfileStore(path);
    assert.equal(nodeStore.read().revision, 1, path);
    assert.equal(nodeStore.read().state.x, Object.is(value, -0) ? 0 : value, path);
    nodeStore.close();
  }
});

test("the shortest-round-trip sample keeps exact retries in both directions", (t) => {
  const value = 667082108456853.2;
  const state = { cookies: [], localStorage: [], x: value };
  const oracle = validateState(state);
  const changed = validateState({ ...state, x: nextUp(value) });
  assert.notEqual(changed, oracle);

  const { path } = fixture(t);
  const nodeStore = new ProfileStore(path, { create: true });
  nodeStore.acquire("owner");
  nodeStore.save("owner", 1, state);
  nodeStore.close();
  assert.equal(storedText(path).state, oracle);

  const rustReplies = run([
    { command: "open", path },
    { command: "acquire", owner: "owner" },
    { command: "save", owner: "owner", revision: "1", state: oracle },
    { command: "save", owner: "owner", revision: "1", state: changed },
    { command: "status" },
    { command: "close" },
  ]);
  assert.equal(rustReplies[2].ok, true, "Rust exact retry after Node commit");
  assert.equal(rustReplies[3].kind, "conflict");
  assert.equal(rustReplies[4].revision, 1, "rejected revision must not be stored");
  assert.equal(storedText(path).state, oracle);

  const { path: rustPath } = fixture(t);
  const rustWrite = run([
    { command: "open", path: rustPath, create: true },
    { command: "acquire", owner: "owner" },
    { command: "save", owner: "owner", revision: "1", state: oracle },
    { command: "close" },
  ]);
  assert.equal(rustWrite[2].ok, true);
  assert.equal(storedText(rustPath).state, oracle);

  const fromRust = new ProfileStore(rustPath);
  assert.equal(fromRust.acquire("owner").revision, 1);
  fromRust.save("owner", 1, state);
  assert.throws(() => fromRust.save("owner", 1, { ...state, x: nextUp(value) }), /Stale/);
  fromRust.close();
  assert.equal(storedText(rustPath).revision, 1);
  assert.equal(storedText(rustPath).state, oracle, "Node retry must keep the SQL text");

  const advance = new ProfileStore(rustPath);
  advance.acquire("owner");
  advance.save("owner", 2, { ...state, x: nextUp(value) });
  advance.close();
  assert.equal(storedText(rustPath).revision, 2);
  assert.equal(storedText(rustPath).state, changed);
});

test("state limits match Node at the cookie and per-origin quota boundaries", (t) => {
  const { path } = fixture(t);
  const cookies = Array.from({ length: 3001 }, (_, index) => ({
    name: `c${index}`,
    value: "v",
    host: "example.com",
    path: "/",
    secure: false,
    httpOnly: false,
    sameSite: null,
    hostOnly: true,
    expiresAt: null,
    createdAt: index,
  }));
  assert.throws(() => validateState({ cookies, localStorage: [] }), /Too many cookies/);
  const oversized = {
    cookies: [],
    localStorage: [["http://example.com", [["k", "x".repeat(2.5 * 1024 * 1024 + 1)]]]],
  };
  assert.throws(() => validateState(oversized), /Storage quota exceeded/);

  const replies = run([
    { command: "open", path, create: true },
    { command: "acquire", owner: "owner" },
    { command: "save", owner: "owner", revision: "1", state: JSON.stringify({ cookies, localStorage: [] }) },
    { command: "save", owner: "owner", revision: "1", state: JSON.stringify(oversized) },
    { command: "save", owner: "owner", revision: "1", state: JSON.stringify(fullState("ok")) },
    { command: "close" },
  ]);
  assert.equal(replies[2].kind, "invalid_state");
  assert.equal(replies[3].kind, "invalid_state");
  assert.equal(replies[4].ok, true);
  const nodeStore = new ProfileStore(path);
  t.after(() => nodeStore.close());
  assert.equal(nodeStore.read().revision, 1);
});

test("revision retry and rejection are identical across the two stores", (t) => {
  const { path } = fixture(t);
  const base = `{"cookies":[],"localStorage":[]}`;
  const changed = `{"cookies":[],"localStorage":[],"a":1}`;
  const replies = run([
    { command: "open", path, create: true },
    { command: "acquire", owner: "owner" },
    { command: "save", owner: "owner", revision: "1", state: base },
    { command: "save", owner: "owner", revision: "1", state: base },
    { command: "save", owner: "owner", revision: "1", state: changed },
    { command: "save", owner: "owner", revision: "0", state: base },
    { command: "save", owner: "owner", revision: "3", state: base },
    { command: "save", owner: "owner", revision: "1.5", state: base },
    { command: "save", owner: "owner", revision: "-1", state: base },
    { command: "save", owner: "owner", revision: "2", state: base },
    { command: "close" },
  ]);
  assert.equal(replies[2].ok, true);
  assert.equal(replies[3].ok, true, "the exact lost-reply retry succeeds");
  for (const index of [4, 5, 6, 7, 8]) assert.equal(replies[index].kind, "conflict");
  assert.equal(replies[9].ok, true);
  const nodeStore = new ProfileStore(path);
  assert.deepEqual(nodeStore.read(), { revision: 2, state: JSON.parse(base) });
  nodeStore.close();
});

test("both stores compete for one database lock, including a symlink alias", async (t) => {
  const { directory, path } = fixture(t);
  const alias = join(directory, "alias.sqlite");
  symlinkSync(path, alias);

  const live = await liveHarness(t);
  t.after(() => live.close().catch(() => {}));
  assert.equal((await live.send({ command: "open", path, create: true })).ok, true);
  assert.equal((await live.send({ command: "acquire", owner: "owner" })).ok, true);
  assert.throws(() => new ProfileStore(path), /locked/);
  assert.throws(() => new ProfileStore(alias), /locked/);
  await live.close();

  const nodeStore = new ProfileStore(path);
  nodeStore.acquire("owner");
  const blocked = await liveHarness(t);
  t.after(() => blocked.close().catch(() => {}));
  assert.equal((await blocked.send({ command: "open", path })).ok, false);
  assert.equal((await blocked.send({ command: "open", path: alias })).ok, false);
  nodeStore.close();
  assert.equal((await blocked.send({ command: "open", path })).ok, true);
  await blocked.close();
});

test("backups are readable by the other implementation", async (t) => {
  const { directory, path } = fixture(t);
  const state = fullState("backup");
  const oracle = validateState(state);
  const rustBackup = join(directory, "rust-backup.sqlite");
  const nodeBackup = join(directory, "node-backup.sqlite");
  const replies = run([
    { command: "open", path, create: true },
    { command: "acquire", owner: "rust" },
    { command: "save", owner: "rust", revision: "1", state: oracle },
    { command: "backup", destination: rustBackup },
    { command: "close" },
  ]);
  assert.equal(replies[3].ok, true);

  const fromRust = new ProfileStore(rustBackup);
  assert.deepEqual(fromRust.read(), { revision: 1, state });
  fromRust.close();

  const nodeStore = new ProfileStore(path);
  nodeStore.acquire("node");
  const nodeText = validateState(state);
  nodeStore.save("node", 2, state);
  assert.equal(nodeStore.backup(nodeBackup), nodeBackup);
  nodeStore.close();
  const nodeStored = storedText(path).state;
  assert.equal(nodeStored, nodeText);

  const fromNode = run([
    { command: "open", path: nodeBackup },
    { command: "acquire", owner: "node" },
  ]);
  assert.equal(fromNode[0].ok, true);
  assert.equal(fromNode[1].revision, 2);
  assert.equal(fromNode[1].state, nodeStored);
});
