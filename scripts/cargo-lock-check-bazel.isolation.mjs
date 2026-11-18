// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 在祖先目录放置 rsproxy 污染 config 的 fixture 里运行真正的 cargo lock 检查：
// 脚本必须仍使用 vendored 源；同环境下去掉命令行 --config 的 cargo 必须失败，
// 证明该 fixture 确实会覆盖 CARGO_HOME，而不是空跑。

import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { test } from "node:test";

function located(value) {
  if (!value || isAbsolute(value)) return value;
  const base = process.env.RUNFILES_DIR ?? join(import.meta.dirname, "..");
  return join(base, process.env.JS_BINARY__WORKSPACE || "_main", value);
}

const cargo = located(process.env.CARGO);
const rustc = located(process.env.RUSTC);
const vendorIndex = located(process.env.CARGO_VENDOR_INDEX);
const script = join(import.meta.dirname, "cargo-lock-check-bazel.mjs");

test("cargo lock check ignores a polluted ancestor cargo config", (t) => {
  assert.ok(cargo && rustc && vendorIndex);
  const directory = mkdtempSync(join(tmpdir(), "ks-cargo-lock-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const poison = join(directory, "poison");
  mkdirSync(join(poison, ".cargo"), { recursive: true });
  writeFileSync(
    join(poison, ".cargo", "config.toml"),
    `[source.crates-io]\nreplace-with = "rsproxy-sparse"\n` +
      `[source.rsproxy]\nregistry = "https://rsproxy.cn/crates.io-index"\n` +
      `[source.rsproxy-sparse]\nregistry = "sparse+https://rsproxy.cn/index/"\n`,
  );
  const work = join(poison, "work");
  mkdirSync(work, { recursive: true });

  const isolated = spawnSync(process.execPath, [script], {
    encoding: "utf8",
    timeout: 120_000,
    env: {
      ...process.env,
      TEST_TMPDIR: work,
      CARGO: cargo,
      RUSTC: rustc,
      CARGO_VENDOR_INDEX: vendorIndex,
    },
  });
  assert.equal(isolated.status, 0, isolated.stderr);
  assert.match(isolated.stdout, /Cargo\.lock matches the public workspace manifests/);

  const workspace = join(work, "workspace");
  const home = join(workspace, ".cargo-home");
  const unisolated = spawnSync(
    cargo,
    ["metadata", "--locked", "--offline", "--format-version", "1"],
    {
      cwd: workspace,
      encoding: "utf8",
      timeout: 120_000,
      env: { PATH: process.env.PATH, HOME: home, CARGO_HOME: home, RUSTC: rustc },
    },
  );
  assert.notEqual(unisolated.status, 0, "the polluted fixture must override CARGO_HOME");
  assert.match(unisolated.stderr, /rsproxy/);
});
