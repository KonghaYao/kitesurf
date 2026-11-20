// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmodSync,
  closeSync,
  copyFileSync,
  cpSync,
  createReadStream,
  mkdirSync,
  mkdtempSync,
  openSync,
  readFileSync,
  readSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { parseArgs } from "node:util";

const here = import.meta.dirname;
const repository = resolve(here, "../..");
const workspace = process.env.BUILD_WORKSPACE_DIRECTORY || repository;
const [host, workerd, config, assets, bundle, ...args] = process.argv.slice(2);
const { values } = parseArgs({
  args,
  options: { out: { type: "string" }, archive: { type: "string" }, tag: { type: "string" } },
});
const { version } = JSON.parse(readFileSync(join(here, "package.json"), "utf8"));
const platform = `${process.platform}-${process.arch}`;
if (!["linux-x64", "linux-arm64", "darwin-arm64"].includes(platform))
  throw new Error(`Unsupported local browser platform: ${platform}`);
if (!/^\d+\.\d+\.\d+$/.test(version) || (values.tag && values.tag !== `v${version}`))
  throw new Error(`Release tag ${values.tag ?? "(unset)"} does not match version ${version}`);

function verifyExecutable(path) {
  const header = Buffer.alloc(20);
  const fd = openSync(path, "r");
  try {
    if (readSync(fd, header, 0, header.length, 0) !== header.length)
      throw new Error(`Incomplete executable header: ${path}`);
  } finally {
    closeSync(fd);
  }
  const native =
    process.platform === "linux"
      ? header.subarray(0, 6).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46, 2, 1])) &&
        [2, 3].includes(header.readUInt16LE(16)) &&
        header.readUInt16LE(18) === (process.arch === "x64" ? 62 : 183)
      : header.readUInt32LE(0) === 0xfeedfacf &&
        header.readUInt32LE(4) === 0x0100000c &&
        header.readUInt32LE(12) === 2;
  if (!native) throw new Error(`Wrong executable format or architecture for ${platform}: ${path}`);
}

function executableVersion(path) {
  return execFileSync(resolve(path), ["--version"], {
    encoding: "utf8",
    env: { PATH: "" },
    timeout: 30_000,
  }).trim();
}

async function fileInfo(path) {
  const hash = createHash("sha256");
  for await (const bytes of createReadStream(path)) hash.update(bytes);
  return { bytes: statSync(path).size, sha256: hash.digest("hex") };
}

async function treeInfo(path, prefix) {
  const files = [];
  for (const name of readdirSync(path).sort()) {
    const child = join(path, name);
    const relative = `${prefix}/${name}`;
    if (statSync(child).isDirectory()) files.push(...(await treeInfo(child, relative)));
    else files.push({ path: relative, ...(await fileInfo(child)) });
  }
  return files;
}

function copyReleaseEntry(source, destination) {
  // Runfiles links are transparent when reading each file into the staging directory.
  if (!statSync(source).isDirectory()) {
    copyFileSync(source, destination);
    return;
  }
  mkdirSync(destination);
  for (const name of readdirSync(source))
    copyReleaseEntry(join(source, name), join(destination, name));
}

async function archivePackage(directory, destination) {
  mkdirSync(destination, { recursive: true });
  const temporary = mkdtempSync(join(destination, ".browser-archive-"));
  const name = `kitesurf-${version}-${platform}.tar.gz`;
  const archive = join(temporary, name);
  try {
    execFileSync("tar", ["-czf", archive, "-C", directory, ...readdirSync(directory).sort()], {
      stdio: "inherit",
      env: { ...process.env, COPYFILE_DISABLE: "1" },
    });
    const { sha256 } = await fileInfo(archive);
    writeFileSync(join(temporary, `${name}.sha256`), `${sha256}  ${name}\n`);
    for (const file of [name, `${name}.sha256`])
      renameSync(join(temporary, file), join(destination, file));
  } finally {
    rmSync(temporary, { recursive: true, force: true });
  }
}

const staged = mkdtempSync(join(tmpdir(), "kitesurf-release-"));
try {
  const executable = join(staged, "kitesurf");
  copyFileSync(host, executable);
  chmodSync(executable, 0o755);
  verifyExecutable(executable);
  verifyExecutable(workerd);
  if (executableVersion(executable) !== `kitesurf ${version}`)
    throw new Error("The native executable version does not match package metadata");
  const workerdVersion = executableVersion(workerd).match(/^workerd (\S+)$/)?.[1];
  if (!workerdVersion) throw new Error("Could not identify the bundled workerd version");

  const binary = await fileInfo(executable);
  const inventory = {
    version,
    platform,
    systemFonts: process.platform === "linux" ? "Fontconfig" : "Core Text",
    host: { path: "kitesurf", implementation: "Rust", license: "Apache-2.0", ...binary },
    runtime: {
      ...(await fileInfo(bundle)),
      workerd: {
        version: workerdVersion,
        license: "Apache-2.0",
        source: "https://github.com/cloudflare/workerd",
        ...(await fileInfo(workerd)),
      },
      engine: await treeInfo(config, "config"),
      assets: await treeInfo(assets, "assets"),
    },
  };
  for (const name of ["LICENSE", "NOTICE", "licenses"])
    copyReleaseEntry(join(repository, name), join(staged, name));
  copyFileSync(join(here, "README.md"), join(staged, "README.md"));
  writeFileSync(join(staged, "DEPENDENCIES.json"), `${JSON.stringify(inventory, null, 2)}\n`);
  writeFileSync(join(staged, "SHA256SUMS"), `${binary.sha256}  kitesurf\n`);

  const output = resolve(workspace, values.out || "dist/local-browser");
  mkdirSync(output, { recursive: true });
  cpSync(staged, output, { recursive: true });
  if (values.archive) await archivePackage(staged, resolve(workspace, values.archive));
  console.log(join(output, "kitesurf"));
} finally {
  rmSync(staged, { recursive: true, force: true });
}
