// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  realpathSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { inject } from "postject";

const here = import.meta.dirname;
const repository = resolve(here, "../..");
const workspace = process.env.BUILD_WORKSPACE_DIRECTORY || repository;
const nodeBinary = realpathSync(process.env.JS_BINARY__NODE_BINARY || process.execPath);
const nodeLicensePath = resolve(nodeBinary, "../../LICENSE");
const options = {};
for (let i = 2; i < process.argv.length; i += 2) {
  const flag = process.argv[i];
  if (!["--out", "--dist", "--assets", "--portable"].includes(flag) || !process.argv[i + 1])
    throw new Error(`Unknown or incomplete argument: ${flag}`);
  options[flag.slice(2)] = resolve(process.argv[i + 1]);
}
if (Number(process.versions.node.split(".")[0]) !== 24)
  throw new Error("Build with the pinned Node 24 toolchain");
if (!["linux", "darwin"].includes(process.platform))
  throw new Error("v0.5 packages Linux and macOS");
const output = options.out || resolve(workspace, "dist/local-browser");
const dist = options.dist || resolve(repository, "packages/engine/local_dist");
const assets = options.assets || resolve(repository, "packages/engine/public");
function copyReleaseFile(source, destination) {
  if (existsSync(destination) && realpathSync(destination) === realpathSync(source))
    throw new Error(`Release output aliases its source: ${destination}`);
  mkdirSync(dirname(destination), { recursive: true });
  rmSync(destination, { force: true });
  writeFileSync(destination, readFileSync(source), { mode: 0o644 });
}
function copyReleaseTree(source, destination) {
  mkdirSync(destination, { recursive: true });
  for (const entry of readdirSync(source)) {
    const from = join(source, entry);
    const to = join(destination, entry);
    if (statSync(from).isDirectory()) copyReleaseTree(from, to);
    else copyReleaseFile(from, to);
  }
}
if (options.portable) {
  mkdirSync(options.portable, { recursive: true });
  copyReleaseTree(dist, join(options.portable, "engine"));
  copyReleaseTree(assets, join(options.portable, "public"));
  process.exit(0);
}
if (!existsSync(nodeLicensePath))
  throw new Error("Node distribution LICENSE is missing; package with the pinned Bazel toolchain");
const temporary = mkdtempSync(join(tmpdir(), "kitesurf-package-"));
const files = [];
const links = [];
const packages = new Map();
const notices = [];
const blobs = new Map();

function addFile(source, destination) {
  const content = readFileSync(source);
  const digest = createHash("sha256").update(content).digest("hex");
  const mode = statSync(source).mode & 0o111 ? 0o700 : 0o600;
  const prior = blobs.get(`${digest}/${mode}`);
  if (prior) {
    links.push({ path: destination, target: prior });
    return;
  }
  blobs.set(`${digest}/${mode}`, destination);
  files.push({ path: destination, mode, content: content.toString("base64") });
}

function addTree(source, destination) {
  for (const item of readdirSync(source, { withFileTypes: true }).sort((a, b) =>
    a.name.localeCompare(b.name),
  )) {
    if (item.name === "node_modules" || item.name.endsWith(".map")) continue;
    const from = join(source, item.name);
    const to = `${destination}/${item.name}`;
    if (statSync(from).isDirectory()) addTree(from, to);
    else addFile(from, to);
  }
}

function packageRoot(name, from) {
  const require = createRequire(join(from, "package.json"));
  for (const modules of require.resolve.paths(name) || []) {
    const path = join(modules, name);
    if (existsSync(join(path, "package.json"))) return realpathSync(path);
  }
  throw new Error(`Missing runtime dependency: ${name} from ${from}`);
}

function addPackage(name, from) {
  const source = packageRoot(name, from);
  if (packages.has(source)) return packages.get(source);
  const metadata = JSON.parse(readFileSync(join(source, "package.json"), "utf8"));
  const matches = (list, value) =>
    !list ||
    (!list.includes(`!${value}`) &&
      (!list.some((v) => !v.startsWith("!")) || list.includes(value)));
  if (!matches(metadata.os, process.platform) || !matches(metadata.cpu, process.arch)) return null;
  const modules = `deps/${packages.size}/node_modules`;
  const destination = `${modules}/${name}`;
  packages.set(source, destination);
  notices.push({
    name: metadata.name,
    version: metadata.version,
    license: metadata.license,
    source: metadata.repository,
    path: destination,
  });
  addTree(source, destination);
  const dependencies = { ...metadata.dependencies, ...metadata.optionalDependencies };
  for (const child of Object.keys(dependencies).sort()) {
    let childSource;
    try {
      childSource = packageRoot(child, source);
    } catch (error) {
      if (metadata.optionalDependencies?.[child]) continue;
      throw error;
    }
    const childDestination = addPackage(child, source);
    if (childDestination && childSource)
      links.push({ path: `${modules}/${child}`, target: childDestination });
  }
  return destination;
}

try {
  mkdirSync(output, { recursive: true });
  for (const name of ["LICENSE", "NOTICE"])
    copyReleaseFile(join(repository, name), join(output, name));
  copyReleaseTree(join(repository, "licenses"), join(output, "licenses"));
  copyReleaseFile(join(here, "README.md"), join(output, "README.md"));
  copyReleaseFile(nodeLicensePath, join(output, "NODE-LICENSE.txt"));
  addTree(join(here, "src"), "src");
  addFile(join(here, "package.json"), "package.json");
  addTree(dist, "engine");
  addTree(assets, "public");
  const metadata = JSON.parse(readFileSync(join(here, "package.json"), "utf8"));
  for (const name of Object.keys(metadata.dependencies)) {
    links.push({ path: `node_modules/${name}`, target: addPackage(name, here) });
  }
  const manifest = join(temporary, "runtime.gz");
  writeFileSync(manifest, gzipSync(Buffer.from(JSON.stringify({ files, links })), { level: 9 }));
  const cli = join(temporary, "cli.gz");
  const cliPaths = new Set(["src/cli.mjs", "src/profile-store.mjs", "package.json"]);
  writeFileSync(
    cli,
    gzipSync(
      Buffer.from(
        JSON.stringify({ files: files.filter((file) => cliPaths.has(file.path)), links: [] }),
      ),
    ),
  );
  files.length = 0;
  links.length = 0;
  blobs.clear();
  globalThis.gc?.();
  const blob = join(temporary, "sea.blob");
  const config = join(temporary, "sea.json");
  writeFileSync(
    config,
    JSON.stringify({
      main: join(here, "src/bootstrap.cjs"),
      output: blob,
      disableExperimentalSEAWarning: true,
      useSnapshot: false,
      useCodeCache: false,
      assets: { runtime: manifest, cli },
    }),
  );
  execFileSync(nodeBinary, ["--experimental-sea-config", config], { stdio: "inherit" });
  const executable = join(output, "kitesurf.partial");
  copyFileSync(nodeBinary, executable);
  chmodSync(executable, 0o755);
  if (process.platform === "darwin") execFileSync("codesign", ["--remove-signature", executable]);
  await inject(executable, "NODE_SEA_BLOB", readFileSync(blob), {
    sentinelFuse: "NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2",
    ...(process.platform === "darwin" ? { machoSegmentName: "NODE_SEA" } : {}),
  });
  if (process.platform === "darwin") execFileSync("codesign", ["--sign", "-", executable]);
  const version = execFileSync(executable, ["--version"], { encoding: "utf8", timeout: 30_000 });
  if (!version.includes(`kitesurf ${metadata.version}`))
    throw new Error("Packaged executable version check failed");
  const inventory = {
    version: metadata.version,
    node: process.version,
    platform: process.platform,
    arch: process.arch,
    packages: notices,
  };
  writeFileSync(join(output, "DEPENDENCIES.json"), JSON.stringify(inventory, null, 2) + "\n");
  const checksum = createHash("sha256").update(readFileSync(executable)).digest("hex");
  const finalExecutable = join(output, "kitesurf");
  renameSync(executable, finalExecutable);
  writeFileSync(join(output, "SHA256SUMS"), `${checksum}  kitesurf\n`);
  console.log(
    JSON.stringify({
      executable: finalExecutable,
      bytes: statSync(finalExecutable).size,
      sha256: checksum,
      packages: notices.length,
    }),
  );
} finally {
  rmSync(join(output, "kitesurf.partial"), { force: true });
  rmSync(temporary, { recursive: true, force: true });
}
