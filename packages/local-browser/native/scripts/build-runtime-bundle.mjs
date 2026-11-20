// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { createHash } from "node:crypto";
import { closeSync, openSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// Keep the format and limits in sync with native/src/bundle.rs.
const MAGIC = Buffer.from("KSRTv001");
const MAX_BYTES = 1024 * 1024 * 1024;
const MAX_FILE_BYTES = 512 * 1024 * 1024;
const MAX_FILES = 32768;
const MAX_PATH_BYTES = 1024;
const FILE_HEADER_BYTES = 2 + 4 + 32;

function tree(root, prefix) {
  const entries = [];
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    const source = join(root, entry.name);
    const path = `${prefix}/${entry.name}`;
    if (entry.isDirectory()) entries.push(...tree(source, path));
    else if (entry.isFile()) entries.push({ source, path });
    else throw new Error(`Runtime input is not a regular file or directory: ${path}`);
  }
  return entries;
}

function writeEntry(output, { path, source }, remaining) {
  const name = Buffer.from(path, "utf8");
  if (
    name.length > MAX_PATH_BYTES ||
    !path
      .split("/")
      .every((part) => /^[A-Za-z0-9._-]{1,255}$/.test(part) && part !== "." && part !== "..")
  )
    throw new Error(`Invalid runtime path: ${path}`);

  const stat = statSync(source);
  const size = FILE_HEADER_BYTES + name.length + stat.size;
  if (!stat.isFile() || stat.size > MAX_FILE_BYTES || size > remaining)
    throw new Error(`Runtime input exceeds its size limit: ${path}`);
  const bytes = readFileSync(source);
  if (bytes.length !== stat.size) throw new Error(`Runtime input changed size: ${path}`);
  const header = Buffer.alloc(FILE_HEADER_BYTES);
  header.writeUInt16BE(name.length, 0);
  header.writeUInt32BE(bytes.length, 2);
  createHash("sha256").update(bytes).digest().copy(header, 6);
  writeFileSync(output, header);
  writeFileSync(output, name);
  writeFileSync(output, bytes);
  return size;
}

const [workerd, config, assets, destination] = process.argv.slice(2);
if (process.argv.length !== 6)
  throw new Error("Usage: build-runtime-bundle WORKERD CONFIG-DIR ASSETS-DIR OUTPUT");
const entries = [
  { path: "workerd", source: workerd },
  ...tree(config, "config"),
  ...tree(assets, "assets"),
];
entries.sort((a, b) => Number(a.path > b.path) - Number(a.path < b.path));
if (entries.length > MAX_FILES) throw new Error("Too many files in the embedded runtime");
const output = openSync(destination, "w");
try {
  const header = Buffer.alloc(MAGIC.length + 4);
  MAGIC.copy(header);
  header.writeUInt32BE(entries.length, MAGIC.length);
  writeFileSync(output, header);
  let remaining = MAX_BYTES - header.length;
  for (const entry of entries) remaining -= writeEntry(output, entry, remaining);
} finally {
  closeSync(output);
}
