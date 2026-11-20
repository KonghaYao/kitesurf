// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 原生 workerd 配置的构建期生成器。Node 只在这里运行：产出的配置、模块字节和资源清单
// 是运行时输入，原生宿主绝不读取 engine-mf.json，也绝不调用 Node。

import { createHash } from "node:crypto";
import {
  copyFileSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { join, relative, resolve } from "node:path";
import { pathToFileURL } from "node:url";

const MODULE_NAME = /^[A-Za-z0-9._/-]+$/;
const VERSION = "0.5.0";
const GC_WRAPPER = "__ks_gc_wrapper.js";
const IGNORED = new Set(["engine-mf.json"]);
const GC_WRAPPER_SOURCE = `import worker from "./index.js";
export * from "./index.js";
const original = globalThis.gc;
if (typeof original === "function") {
  globalThis.gc = function (...args) {
    const at = Date.now();
    const result = original.apply(this, args);
    console.log("[ks-gc] " + JSON.stringify({ at, done: Date.now() }));
    return result;
  };
}
export default worker;
`;

/** @typedef {{ type: "esModule" | "text" | "wasm" | "data", name: string, bytes: number }} Module */

/** @param {string} name @returns {"esModule" | "text" | "wasm" | "data" | null} */
export function classifyModule(name) {
  if (name.endsWith(".glue.js") || name.endsWith(".css") || name.endsWith(".dat")) return "text";
  if (name.endsWith(".wasm")) return "wasm";
  if (name.endsWith(".woff2") || name.endsWith(".bin")) return "data";
  if (name.endsWith(".js")) return "esModule";
  return null;
}

/** @param {string} root @returns {Module[]} */
export function collectModules(root) {
  const modules = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {
      const full = join(directory, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      if (!entry.isFile()) throw new Error(`module input is not a regular file: ${full}`);
      const name = relative(root, full).replaceAll("\\", "/");
      if (IGNORED.has(name) || name.endsWith(".map")) continue;
      if (!MODULE_NAME.test(name)) throw new Error(`unsafe module name: ${name}`);
      const type = classifyModule(name);
      if (type === null) throw new Error(`unknown module type: ${name}`);
      modules.push({ type, name, bytes: statSync(full).size });
    }
  };
  walk(root);
  const entry = modules.find((module) => module.name === "index.js");
  if (!entry) throw new Error(`dist has no index.js entry: ${root}`);
  if (entry.type !== "esModule") throw new Error("index.js must be an ES module");
  modules.sort((a, b) => {
    if (a.name === "index.js") return -1;
    if (b.name === "index.js") return 1;
    return a.name.localeCompare(b.name);
  });
  return modules;
}

/** @param {string} root @returns {{ path: string, size: number, sha256: string }[]} */
export function collectAssets(root) {
  const files = [];
  const walk = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true }).sort((a, b) =>
      a.name.localeCompare(b.name),
    )) {
      const full = join(directory, entry.name);
      if (entry.isDirectory()) {
        walk(full);
        continue;
      }
      if (!entry.isFile()) throw new Error(`asset is not a regular file: ${full}`);
      const name = relative(root, full).replaceAll("\\", "/");
      if (name.split("/").includes("..")) throw new Error(`asset escapes its root: ${name}`);
      const bytes = readFileSync(full);
      files.push({
        path: `/${name}`,
        size: bytes.length,
        sha256: createHash("sha256").update(bytes).digest("hex"),
      });
    }
  };
  walk(root);
  files.sort((a, b) => a.path.localeCompare(b.path));
  return files;
}

/** @param {Module[]} modules @param {boolean} gc @returns {string} */
export function emitModules(modules, gc = false) {
  const entries = [];
  if (gc) entries.push(`  (name = "${GC_WRAPPER}", esModule = embed "modules/${GC_WRAPPER}"),`);
  for (const module of modules) {
    const field =
      module.type === "esModule"
        ? "esModule"
        : module.type === "text"
          ? "text"
          : module.type === "wasm"
            ? "wasm"
            : "data";
    entries.push(`  (name = "${module.name}", ${field} = embed "modules/${module.name}"),`);
  }
  return [
    'using Workerd = import "/workerd/workerd.capnp";',
    "",
    "const modules :List(Workerd.Worker.Module) = [",
    ...entries,
    "];",
    "",
  ].join("\n");
}

function capnpString(value) {
  if (!/^[\x20-\x7e]*$/.test(value)) throw new Error(`unsupported config string: ${value}`);
  return JSON.stringify(value);
}

/** @param {object} options @returns {string} */
export function emitConfig(options) {
  const {
    modulesConst = "modules.capnp",
    privateNetwork = false,
    compatibilityDate,
    compatibilityFlags,
    vars,
    loaderBinding,
    profileBinding,
    assetsBinding,
    fontsBinding = "LOCAL_FONTS",
  } = options;
  const network = privateNetwork ? '["public", "private", "local"]' : '["public"]';
  const bindings = [
    ...Object.entries(vars).map(
      ([name, value]) => `      (name = "${name}", text = ${capnpString(value)}),`,
    ),
    `      (name = "${loaderBinding}", workerLoader = ()),`,
    `      (name = "${profileBinding}", service = "profile"),`,
    `      (name = "${assetsBinding}", service = "assets"),`,
    `      (name = "${fontsBinding}", service = "fonts"),`,
  ];
  if (privateNetwork) bindings.push(`      (name = "KITESURF_ALLOW_PRIVATE_NETWORK", text = "1"),`);
  const flags = compatibilityFlags.map((flag) => capnpString(flag)).join(", ");
  // A network service without tlsOptions has no TLS client, so every HTTPS fetch fails with a
  // workerd internal error. Trust the system roots, as a browser does.
  return [
    'using Workerd = import "/workerd/workerd.capnp";',
    `using M = import "${modulesConst}";`,
    "",
    "const config :Workerd.Config = (",
    "  services = [",
    '    (name = "main", worker = (',
    "      modules = M.modules,",
    `      compatibilityDate = ${capnpString(compatibilityDate)},`,
    `      compatibilityFlags = [${flags}],`,
    "      bindings = [",
    ...bindings,
    "      ],",
    '      globalOutbound = "internet",',
    "    )),",
    '    (name = "profile", external = (http = ())),',
    '    (name = "assets", external = (http = ())),',
    '    (name = "fonts", external = (http = ())),',
    `    (name = "internet", network = (allow = ${network}, tlsOptions = (trustBrowserCas = true))),`,
    "  ],",
    '  sockets = [(name = "engine", http = (), service = "main")],',
    '  v8Flags = ["--expose-gc"],',
    ");",
    "",
  ].join("\n");
}

function readOptions(dist) {
  const file = join(dist, "engine-mf.json");
  const options = JSON.parse(readFileSync(file, "utf8"));
  if (typeof options.compatibilityDate !== "string")
    throw new Error("engine-mf.json has no compatibilityDate");
  const compatibilityFlags = Array.isArray(options.compatibilityFlags)
    ? options.compatibilityFlags
    : [];
  const vars = {};
  for (const [name, value] of Object.entries(options.bindings ?? {})) {
    if (typeof value !== "string") throw new Error(`binding ${name} is not a text var`);
    vars[name] = value;
  }
  // 原生宿主只运行在系统字体模式；打包字体不会进入 native 资源树。
  vars.KITESURF_SYSTEM_FONTS = "1";
  const loaders = Object.keys(options.workerLoaders ?? {});
  if (loaders.length !== 1) throw new Error("expected exactly one workerLoader binding");
  const assetsBinding = options.assets?.binding;
  if (typeof assetsBinding !== "string") throw new Error("engine-mf.json has no assets binding");
  return {
    compatibilityDate: options.compatibilityDate,
    compatibilityFlags,
    vars,
    loaderBinding: loaders[0],
    profileBinding: "LOCAL_PROFILE",
    assetsBinding,
    fontsBinding: "LOCAL_FONTS",
  };
}

/** @param {{ dist: string, assets: string, out: string, releaseVersion?: string }} paths
 * @returns {{ modules: number, assets: number, version: string }}
 */
export function generate({ dist, assets, out, releaseVersion }) {
  const distRoot = resolve(dist);
  const assetsRoot = resolve(assets);
  const outRoot = resolve(out);
  const options = readOptions(distRoot);
  if (releaseVersion) options.vars.KITESURF_VERSION = releaseVersion;
  const modules = collectModules(distRoot);
  const assetFiles = collectAssets(assetsRoot);
  rmSync(outRoot, { recursive: true, force: true });
  mkdirSync(join(outRoot, "modules"), { recursive: true });
  for (const module of modules)
    copyFileSync(join(distRoot, module.name), join(outRoot, "modules", module.name));
  writeFileSync(join(outRoot, "modules.capnp"), emitModules(modules, false));
  writeFileSync(
    join(outRoot, "assets-manifest.json"),
    JSON.stringify({ version: 1, files: assetFiles }, null, 2) + "\n",
  );
  writeFileSync(
    join(outRoot, "local-public.capnp"),
    emitConfig({ ...options, modulesConst: "modules.capnp", privateNetwork: false }),
  );
  writeFileSync(
    join(outRoot, "local-private.capnp"),
    emitConfig({ ...options, modulesConst: "modules.capnp", privateNetwork: true }),
  );
  if (!releaseVersion) {
    writeFileSync(join(outRoot, "modules", GC_WRAPPER), GC_WRAPPER_SOURCE);
    writeFileSync(join(outRoot, "modules-gc.capnp"), emitModules(modules, true));
    writeFileSync(
      join(outRoot, "modules.json"),
      JSON.stringify({ entry: "index.js", modules }, null, 2) + "\n",
    );
    // GC 探针驱动 loopback fixture，因此其包装变体需要显式选择私有网络。
    writeFileSync(
      join(outRoot, "local-gc.capnp"),
      emitConfig({ ...options, modulesConst: "modules-gc.capnp", privateNetwork: true }),
    );
  }
  return {
    modules: modules.length,
    assets: assetFiles.length,
    version: releaseVersion ?? VERSION,
  };
}

function readReleaseVersion(path) {
  const version = /^CARGO_PKG_VERSION=([^\r\n]+)$/m.exec(readFileSync(path, "utf8"))?.[1];
  if (!version || !/^\d+\.\d+\.\d+(?:-[A-Za-z0-9.-]+)?$/.test(version))
    throw new Error("Cargo environment has no usable CARGO_PKG_VERSION");
  return version;
}

function parseArgs(argv) {
  const options = {};
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index];
    const value = () => {
      const next = argv[++index];
      if (!next) throw new Error(`${argument} requires a value`);
      return next;
    };
    if (argument === "--dist") options.dist = value();
    else if (argument === "--assets") options.assets = value();
    else if (argument === "--out") options.out = value();
    else if (argument === "--release-version-env")
      options.releaseVersion = readReleaseVersion(value());
    else throw new Error(`unknown option: ${argument}`);
  }
  if (!options.dist || !options.assets || !options.out)
    throw new Error("--dist, --assets and --out are required");
  return options;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const result = generate(parseArgs(process.argv.slice(2)));
    console.log(
      `native config: ${result.modules} modules, ${result.assets} assets (${result.version})`,
    );
  } catch (error) {
    console.error(`native config: ${error.message}`);
    process.exit(1);
  }
}
