// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

const { getAsset } = require("node:sea");
const { gunzipSync } = require("node:zlib");
const { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { dirname, join, relative, resolve, sep } = require("node:path");
const { pathToFileURL } = require("node:url");

const root = mkdtempSync(join(tmpdir(), "kitesurf-runtime-"));
process.on("exit", () => {
  rmSync(root, { recursive: true, force: true });
});
const inside = (name) => {
  const path = resolve(root, name);
  if (!path.startsWith(root + sep)) throw new Error("Invalid bundled asset path");
  return path;
};
try {
  const argv = process.argv.slice(2);
  const serve =
    (!argv[0] || argv[0] === "serve" || argv[0].startsWith("-")) &&
    !argv.some((arg) => ["--version", "--help", "-h"].includes(arg));
  const payload = JSON.parse(
    gunzipSync(Buffer.from(getAsset(serve ? "runtime" : "cli"))).toString("utf8"),
  );
  for (const file of payload.files) {
    const path = inside(file.path);
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    writeFileSync(path, Buffer.from(file.content, "base64"), { mode: file.mode, flag: "wx" });
  }
  for (const link of payload.links) {
    const path = inside(link.path);
    mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
    symlinkSync(relative(dirname(path), inside(link.target)), path);
  }
  import(pathToFileURL(join(root, "src/cli.mjs")).href)
    .then(({ main }) => main(process.argv.slice(2), root))
    .catch((error) => {
      console.error(`kitesurf: ${error.message}`);
      process.exitCode = 1;
    });
} catch (error) {
  console.error(`kitesurf: ${error.message}`);
  process.exitCode = 1;
}
