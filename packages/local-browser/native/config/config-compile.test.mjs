// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 把构建期配置固定到运行时 workerd：每个生成的配置都必须编译为自包含二进制，并且该
// 二进制必须能在没有模块文件的目录里开始监听。

import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import { test } from "node:test";

// Bazel 的环境变量保存 bin 相对路径；测试进程在 runfiles 树内运行。
function located(value) {
  if (!value || isAbsolute(value)) return value;
  const runfiles = process.env.RUNFILES_DIR;
  if (!runfiles) return value;
  return join(runfiles, process.env.JS_BINARY__WORKSPACE || "_main", value);
}

const workerd = located(process.env.KITESURF_WORKERD);
const configDir = located(process.env.KITESURF_CONFIG_DIR);
const engineAssets = located(process.env.KITESURF_ENGINE_ASSETS);
assert.ok(workerd, "KITESURF_WORKERD must point at the pinned workerd");
assert.ok(configDir, "KITESURF_CONFIG_DIR must point at the generated config");
assert.ok(engineAssets, "KITESURF_ENGINE_ASSETS must point at the native asset tree");
const CONFIGS = ["local-public.capnp", "local-private.capnp", "local-gc.capnp"];

function compile(t, name) {
  const root = mkdtempSync(join(tmpdir(), "ks-native-compile-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  assert.ok(existsSync(join(configDir, name)), `${name} was generated`);
  const result = spawnSync(workerd, ["compile", "--config-only", join(configDir, name)], {
    maxBuffer: 256 * 1024 * 1024,
    timeout: 120_000,
  });
  assert.equal(result.status, 0, `${name}: ${result.stderr}`);
  assert.ok(
    result.stdout.length > 1024 * 1024,
    `${name} compiled config is ${result.stdout.length} bytes`,
  );
  const binary = join(root, "config.bin");
  writeFileSync(binary, result.stdout);
  return { root, binary };
}

async function serve(t, root, binary) {
  const engineSocket = join(root, "engine.sock");
  const child = spawn(
    workerd,
    [
      "serve",
      "--experimental",
      "--binary",
      binary,
      "--socket-addr",
      `engine=unix:${engineSocket}`,
      "--external-addr",
      `profile=unix:${join(root, "profile.sock")}`,
      "--external-addr",
      `assets=unix:${join(root, "assets.sock")}`,
      "--external-addr",
      `fonts=unix:${join(root, "fonts.sock")}`,
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  t.after(() => {
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  });
  const exited = new Promise((resolve) => child.once("exit", (...args) => resolve(args)));
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (existsSync(engineSocket)) {
      child.kill("SIGTERM");
      const [code] = await Promise.race([
        exited,
        new Promise((resolve) => setTimeout(resolve, 10_000, ["no exit"])),
      ]);
      assert.equal(code, 0, `binary config server exited ${code}: ${stderr.slice(0, 2000)}`);
      return;
    }
    if (child.exitCode !== null) break;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  assert.fail(`binary config did not listen: ${stderr.slice(0, 4000)}`);
}

for (const name of CONFIGS) {
  test(
    `${name} compiles and serves without its module directory`,
    { timeout: 180_000 },
    async (t) => {
      const { root, binary } = compile(t, name);
      await serve(t, root, binary);
    },
  );
}

test("the native asset tree and manifest carry no bundled fonts", () => {
  const files = [];
  const walk = (directory, prefix) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const name = `${prefix}${entry.name}`;
      if (entry.isDirectory()) {
        walk(join(directory, entry.name), `${name}/`);
      } else if (entry.isFile()) {
        files.push(name);
      }
    }
  };
  walk(engineAssets, "");
  files.sort();
  const bundled = files.filter(
    (name) => name.startsWith("fonts/") || /\.(ttf|otf|woff2?)$/i.test(name),
  );
  assert.deepEqual(bundled, []);
  for (const name of ["rbi/scene_host.json", "rbi/scene_host.wasm", "fixtures/pixel.png"]) {
    assert.ok(files.includes(name), `${name} stays in the native asset tree`);
  }
  const manifest = JSON.parse(readFileSync(join(configDir, "assets-manifest.json"), "utf8"));
  assert.deepEqual(
    manifest.files.map((entry) => entry.path.slice(1)).sort(),
    files,
    "the manifest lists exactly the native asset tree",
  );
});
