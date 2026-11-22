// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import {
  classifyModule,
  collectAssets,
  collectModules,
  emitConfig,
  emitModules,
  generate,
} from "./generate.mjs";

function temp(t) {
  const root = mkdtempSync(join(tmpdir(), "ks-native-generate-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

function writeTree(root, files) {
  for (const [name, contents] of Object.entries(files)) {
    const path = join(root, name);
    mkdirSync(join(path, ".."), { recursive: true });
    writeFileSync(path, contents);
  }
}

test("classifyModule maps each bundled suffix and rejects unknown files", () => {
  assert.equal(classifyModule("index.js"), "esModule");
  assert.equal(classifyModule("core_bg.wasm"), "wasm");
  assert.equal(classifyModule("kitesurf_core.glue.js"), "text");
  assert.equal(classifyModule("terminal.css"), "text");
  assert.equal(classifyModule("data.dat"), "text");
  assert.equal(classifyModule("noto.woff2"), "data");
  assert.equal(classifyModule("blob.bin"), "data");
  assert.equal(classifyModule("notes.txt"), null);
  assert.equal(classifyModule("index.js.map"), null);
});

test("collectModules keeps index.js first and ignores the Wrangler manifest and maps", (t) => {
  const root = temp(t);
  writeTree(root, {
    "engine-mf.json": "{}",
    "index.js": "export default {};",
    "index.js.map": "{}",
    "renderer.js": "export {};",
    "core.glue.js": "wasm;",
    "terminal.css": "body{}",
    "data.dat": "x",
    "core_bg.wasm": "wasm",
    "noto.woff2": "font",
    "blob.bin": "bin",
  });
  const modules = collectModules(root);
  assert.deepEqual(
    modules.map((module) => [module.name, module.type]),
    [
      ["index.js", "esModule"],
      ["blob.bin", "data"],
      ["core_bg.wasm", "wasm"],
      ["core.glue.js", "text"],
      ["data.dat", "text"],
      ["noto.woff2", "data"],
      ["renderer.js", "esModule"],
      ["terminal.css", "text"],
    ],
  );
});

test("collectModules rejects a missing entry, unknown types and unsafe names", (t) => {
  const root = temp(t);
  writeTree(root, { "other.js": "export {};" });
  assert.throws(() => collectModules(root), /no index\.js entry/);
  rmSync(root, { recursive: true, force: true });
  mkdirSync(root);
  writeTree(root, { "index.js": "export {};", "readme.txt": "x" });
  assert.throws(() => collectModules(root), /unknown module type: readme\.txt/);
  rmSync(join(root, "readme.txt"));
  writeTree(root, { "bad name.js": "export {};" });
  assert.throws(() => collectModules(root), /unsafe module name: bad name\.js/);
});

test("emitModules writes the gc wrapper first only when asked", () => {
  const modules = [
    { type: "esModule", name: "index.js", bytes: 3 },
    { type: "wasm", name: "core_bg.wasm", bytes: 4 },
  ];
  const plain = emitModules(modules, false);
  assert.ok(plain.startsWith('using Workerd = import "/workerd/workerd.capnp";'));
  assert.ok(!plain.includes("__ks_gc_wrapper.js"));
  assert.match(plain, /\(name = "index\.js", esModule = embed "modules\/index\.js"\)/);
  assert.match(plain, /\(name = "core_bg\.wasm", wasm = embed "modules\/core_bg\.wasm"\)/);
  const wrapped = emitModules(modules, true);
  const entries = wrapped.split("\n").filter((line) => line.includes("embed"));
  assert.match(entries[0], /__ks_gc_wrapper\.js/);
  assert.match(entries[1], /index\.js/);
});

test("collectAssets hashes nested files and records their served paths", (t) => {
  const root = temp(t);
  writeTree(root, {
    "fonts/NotoSans.ttf": "font-bytes",
    "rbi/scene_host.json": "{}",
    "top.txt": "hello",
  });
  const assets = collectAssets(root);
  assert.deepEqual(
    assets.map((entry) => entry.path),
    ["/fonts/NotoSans.ttf", "/rbi/scene_host.json", "/top.txt"],
  );
  const top = assets.find((entry) => entry.path === "/top.txt");
  assert.equal(top.size, 5);
  assert.equal(top.sha256, createHash("sha256").update("hello").digest("hex"));
});

test("emitConfig selects the network scope, system roots and explicit bindings", () => {
  const options = {
    compatibilityDate: "2026-09-11",
    compatibilityFlags: ["nodejs_compat"],
    vars: { KITESURF_ENV: "local" },
    loaderBinding: "LOADER",
    profileBinding: "LOCAL_PROFILE",
    assetsBinding: "FONT_ASSETS",
  };
  const publicConfig = emitConfig({
    ...options,
    modulesConst: "modules.capnp",
    privateNetwork: false,
  });
  assert.ok(publicConfig.includes('using M = import "modules.capnp";'));
  assert.ok(
    publicConfig.includes(
      '(name = "internet", network = (allow = ["public"], tlsOptions = (trustBrowserCas = true)))',
    ),
  );
  assert.ok(!publicConfig.includes("KITESURF_ALLOW_PRIVATE_NETWORK"));
  assert.ok(publicConfig.includes('(name = "LOADER", workerLoader = ())'));
  assert.ok(publicConfig.includes('(name = "LOCAL_PROFILE", service = "profile")'));
  assert.ok(publicConfig.includes('(name = "FONT_ASSETS", service = "assets")'));
  assert.ok(publicConfig.includes('(name = "LOCAL_FONTS", service = "fonts")'));
  assert.ok(publicConfig.includes('(name = "fonts", external = (http = ()))'));
  assert.ok(publicConfig.includes('v8Flags = ["--expose-gc"]'));
  const privateConfig = emitConfig({
    ...options,
    modulesConst: "modules-gc.capnp",
    privateNetwork: true,
  });
  assert.ok(
    privateConfig.includes(
      '(name = "internet", network = (allow = ["public", "private", "local"], tlsOptions = (trustBrowserCas = true)))',
    ),
  );
  assert.ok(privateConfig.includes('(name = "KITESURF_ALLOW_PRIVATE_NETWORK", text = "1")'));
  assert.ok(privateConfig.includes('using M = import "modules-gc.capnp";'));
});

test("generate writes modules, manifests and three configs, replacing stale output", (t) => {
  const root = temp(t);
  const dist = join(root, "dist");
  const assets = join(root, "assets");
  const out = join(root, "out");
  writeTree(dist, {
    "engine-mf.json": JSON.stringify({
      compatibilityDate: "2026-09-11",
      compatibilityFlags: ["nodejs_compat"],
      bindings: { KITESURF_ENV: "local", KITESURF_VERSION: "0.5.3" },
      workerLoaders: { LOADER: {} },
      assets: { binding: "FONT_ASSETS" },
    }),
    "index.js": "export default {};",
    "core_bg.wasm": "wasm",
    "index.js.map": "{}",
  });
  writeTree(assets, { "fixtures/pixel.png": "png", "rbi/scene_host.json": "{}" });
  mkdirSync(out, { recursive: true });
  writeFileSync(join(out, "stale.txt"), "stale");
  const result = generate({ dist, assets, out });
  assert.deepEqual(result, { modules: 2, assets: 2, version: "0.5.3" });
  assert.ok(!existsSync(join(out, "stale.txt")));
  const manifest = JSON.parse(readFileSync(join(out, "modules.json"), "utf8"));
  assert.equal(manifest.entry, "index.js");
  assert.deepEqual(
    manifest.modules.map((module) => module.name),
    ["index.js", "core_bg.wasm"],
  );
  const assetManifest = JSON.parse(readFileSync(join(out, "assets-manifest.json"), "utf8"));
  assert.equal(assetManifest.version, 1);
  assert.deepEqual(
    assetManifest.files.map((entry) => entry.path),
    ["/fixtures/pixel.png", "/rbi/scene_host.json"],
  );
  for (const name of [
    "modules.capnp",
    "modules-gc.capnp",
    "local-public.capnp",
    "local-private.capnp",
    "local-gc.capnp",
  ]) {
    assert.ok(existsSync(join(out, name)), name);
  }
  assert.match(readFileSync(join(out, "modules-gc.capnp"), "utf8"), /__ks_gc_wrapper\.js/);
  assert.match(readFileSync(join(out, "modules", "__ks_gc_wrapper.js"), "utf8"), /\[ks-gc\]/);
  assert.match(
    readFileSync(join(out, "local-public.capnp"), "utf8"),
    /KITESURF_ENV", text = "local"/,
  );
  assert.match(readFileSync(join(out, "local-private.capnp"), "utf8"), /"private", "local"/);
  assert.match(readFileSync(join(out, "local-gc.capnp"), "utf8"), /"private", "local"/);
  for (const name of ["local-public.capnp", "local-private.capnp", "local-gc.capnp"]) {
    const config = readFileSync(join(out, name), "utf8");
    assert.match(config, /KITESURF_SYSTEM_FONTS", text = "1"/, `${name} selects system fonts`);
    assert.match(config, /tlsOptions = \(trustBrowserCas = true\)/, `${name} trusts system roots`);
    assert.match(config, /\(name = "LOCAL_FONTS", service = "fonts"\)/, name);
    assert.match(config, /\(name = "fonts", external = \(http = \(\)\)\)/, name);
  }
  // 版本取自 Cargo 环境，必须覆盖 manifest 里的值；用一个不同的值证明覆盖生效。
  assert.equal(generate({ dist, assets, out, releaseVersion: "0.5.4" }).version, "0.5.4");
  for (const name of [
    "modules.json",
    "modules-gc.capnp",
    "local-gc.capnp",
    "modules/__ks_gc_wrapper.js",
  ]) {
    assert.ok(!existsSync(join(out, name)), `${name} is excluded from release`);
  }
  for (const name of ["local-public.capnp", "local-private.capnp"]) {
    assert.match(readFileSync(join(out, name), "utf8"), /KITESURF_VERSION", text = "0\.5\.4"/);
  }
});

test("generate rejects an engine manifest without the expected bindings", (t) => {
  const root = temp(t);
  const dist = join(root, "dist");
  writeTree(dist, {
    "engine-mf.json": JSON.stringify({ compatibilityDate: "2026-09-11" }),
    "index.js": "export default {};",
  });
  assert.throws(() => generate({ dist, assets: dist, out: join(root, "out") }), /workerLoader/);
});
