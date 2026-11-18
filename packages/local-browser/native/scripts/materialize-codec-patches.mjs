#!/usr/bin/env node
// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const native = process.env.BUILD_WORKSPACE_DIRECTORY
  ? join(process.env.BUILD_WORKSPACE_DIRECTORY, "packages/local-browser/native")
  : resolve(dirname(fileURLToPath(import.meta.url)), "..");
const patches = join(native, "patches");
const manifest = JSON.parse(readFileSync(join(patches, "manifest.json"), "utf8"));
const out = join(native, "target/vendor-patched");
const cache = join(process.env.CARGO_HOME || join(homedir(), ".cargo"), "registry/cache");
const check = process.argv.includes("--check");
mkdirSync(out, { recursive: true });

// 只消费已有 registry archive；缺失时由 Cargo/Bazel 获取固定版本，不访问其他源码。
for (const crate of manifest.crates) {
  const key = `${crate.name}-${crate.version}`;
  const archive = readdirSync(cache).map((registry) => join(cache, registry, `${key}.crate`))
    .find((candidate) => existsSync(candidate));
  if (!archive) throw new Error(`missing registry archive: ${key}`);
  if (createHash("sha256").update(readFileSync(archive)).digest("hex") !== crate.sha256) {
    throw new Error(`registry checksum mismatch: ${key}`);
  }
  const digest = createHash("sha256").update(JSON.stringify(crate));
  for (const patch of crate.series) digest.update(readFileSync(join(patches, key, patch)));
  const stamp = digest.digest("hex");
  const destination = join(out, key);
  const marker = join(destination, ".codec-patch-stamp");
  if (existsSync(marker) && readFileSync(marker, "utf8") === stamp) continue;
  if (check) throw new Error(`stale codec materialization: ${key}`);
  const staging = mkdtempSync(join(out, ".codec-"));
  try {
    execFileSync("tar", ["-xzf", archive, "-C", staging, "--strip-components=1"]);
    for (const patch of crate.series) {
      const path = join(patches, key, patch);
      const options = { cwd: staging, env: { ...process.env, GIT_CEILING_DIRECTORIES: out } };
      execFileSync("git", ["apply", "--check", path], options);
      execFileSync("git", ["apply", path], options);
    }
    writeFileSync(join(staging, ".codec-patch-stamp"), stamp);
    // 只替换本脚本拥有、位于 native target 下的生成目录。
    rmSync(destination, { recursive: true, force: true });
    renameSync(staging, destination);
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
}
