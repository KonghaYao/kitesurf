// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";

const RUNNER = resolve(import.meta.dirname, "e2e-shard.mjs");
const TEST_FILE = "test/e2e/example.e2e.test.ts";
const REPORT = `import { writeFileSync } from "node:fs";
const output = process.argv.find((arg) => arg.startsWith("--outputFile.json="));
writeFileSync(output.slice("--outputFile.json=".length), JSON.stringify({
  testResults: [{ name: process.cwd() + "/${TEST_FILE}" }],
}));
`;

/** A fake external engine tree whose Vitest CLI runs the supplied source. */
function fakeEngine(t, source) {
  const root = mkdtempSync(join(tmpdir(), "kitesurf-external-e2e-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const engine = join(root, "external/kitesurf+/packages/engine");
  const vitest = join(engine, "node_modules/vitest");
  mkdirSync(vitest, { recursive: true });
  writeFileSync(join(engine, "package.json"), '{"type":"module"}');
  writeFileSync(join(vitest, "package.json"), '{"bin":{"vitest":"cli.mjs"}}');
  writeFileSync(join(vitest, "cli.mjs"), source);
  return { root, engine, out: join(root, "results") };
}

function runShard({ root, engine, out }) {
  return spawnSync(
    process.execPath,
    [
      RUNNER,
      "--engine-dir",
      engine,
      "--engine-dist",
      "dist",
      "--fixtures-dist",
      "fixtures",
      "--out-dir",
      out,
      TEST_FILE,
    ],
    { cwd: root, encoding: "utf8", env: { ...process.env, BAZEL_BINDIR: "bazel-out/bin" } },
  );
}

test("e2e shards resolve Vitest and tests from the external engine directory", (t) => {
  const engine = fakeEngine(t, REPORT);
  const result = runShard(engine);
  assert.equal(result.status, 0, result.stderr);
  assert.deepEqual(JSON.parse(readFileSync(join(engine.out, "shard.json"), "utf8")).files, [
    TEST_FILE,
  ]);
});

test("e2e shards fail on workerd uncaught lines even when Vitest exits 0", (t) => {
  const engine = fakeEngine(
    t,
    `${REPORT}process.stdout.write("uncaught exception; source = Uncaught (async)\\n");`,
  );
  const result = runShard(engine);
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /workerd reported 1 uncaught exception line\(s\)/);
  assert.match(result.stderr, /uncaught exception; source = Uncaught \(async\)/);
});
