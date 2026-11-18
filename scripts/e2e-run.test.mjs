// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import test from "node:test";

import { createUncaughtWatcher, exitCodeFor, runObserved, uncaughtLinesIn } from "./e2e-run.mjs";

function collect(stream) {
  let text = "";
  stream.setEncoding("utf8");
  stream.on("data", (chunk) => {
    text += chunk;
  });
  return () => text;
}

test("the scanner joins chunks, handles CRLF and flushes an unterminated line", () => {
  const watcher = createUncaughtWatcher();
  watcher.push("uncaught exc");
  watcher.push("eption; source = Uncaught (async)\r\n");
  watcher.push("uncaught exception; duplicate\r\nuncaught exception; duplicate\r\n");
  watcher.push("uncaught exception; no trailing newline");
  watcher.end();
  assert.deepEqual(watcher.lines, [
    "uncaught exception; source = Uncaught (async)",
    "uncaught exception; duplicate",
    "uncaught exception; duplicate",
    "uncaught exception; no trailing newline",
  ]);
});

test("only a column-zero prefix counts, after stripping terminal colors", () => {
  const lines = uncaughtLinesIn(
    " uncaught exception; indented\n" +
      "not uncaught exception; embedded\n" +
      "\u001b[31muncaught exception; colored\u001b[0m\n",
  );
  assert.deepEqual(lines, ["uncaught exception; colored"]);
});

function node(code) {
  return [process.execPath, ["-e", code]];
}

test("runObserved tees both streams and detects an uncaught line split across chunks", async () => {
  const stdout = new PassThrough();
  const stderr = new PassThrough();
  const stdoutText = collect(stdout);
  const stderrText = collect(stderr);
  const [command, args] = node(`
    process.stdout.write("Test Files 1 passed\\n");
    process.stderr.write("uncaught exc");
    setTimeout(() => {
      process.stderr.write("eption; source = Uncaught (async)\\n");
      process.exit(0);
    }, 10);
  `);
  const result = await runObserved(command, args, { stdout, stderr });
  assert.equal(result.code, 0);
  assert.deepEqual(result.uncaught, ["uncaught exception; source = Uncaught (async)"]);
  assert.equal(exitCodeFor(result, result.uncaught.length), 1);
  assert.match(stdoutText(), /Test Files 1 passed/);
  assert.match(stderrText(), /uncaught exception; source = Uncaught \(async\)/);
});

test("runObserved detects stdout uncaught output without a final newline", async () => {
  const [command, args] = node(`process.stdout.write("before\\nuncaught exception; stdout only");`);
  const result = await runObserved(command, args, {
    stdout: new PassThrough(),
    stderr: new PassThrough(),
  });
  assert.equal(result.code, 0);
  assert.deepEqual(result.uncaught, ["uncaught exception; stdout only"]);
  assert.equal(exitCodeFor(result, result.uncaught.length), 1);
});

test("clean output keeps exit 0 and preserves an original failure", async () => {
  const [cleanCommand, cleanArgs] = node(`process.stdout.write("Test Files 1 passed\\n");`);
  const cleanResult = await runObserved(cleanCommand, cleanArgs, {
    stdout: new PassThrough(),
    stderr: new PassThrough(),
  });
  assert.equal(cleanResult.code, 0);
  assert.deepEqual(cleanResult.uncaught, []);
  assert.equal(exitCodeFor(cleanResult, 0), 0);

  const [failing, failingArgs] = node(
    `process.stdout.write("uncaught exception; still fails\\n"); process.exit(7);`,
  );
  const failed = await runObserved(failing, failingArgs, {
    stdout: new PassThrough(),
    stderr: new PassThrough(),
  });
  assert.equal(failed.code, 7);
  assert.equal(failed.uncaught.length, 1);
  assert.equal(exitCodeFor(failed, failed.uncaught.length), 7);
});
