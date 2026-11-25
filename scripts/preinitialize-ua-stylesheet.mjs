// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Each page has its own Wasm instance. Initialize the fixed UA stylesheet during the build.
// Only linear memory may change. The function must run without a host, and the module must
// have no start function or passive segments whose state we could fail to capture.
import { deepStrictEqual, ok, strictEqual } from "node:assert";

const PAGE_BYTES = 65536;
const SECTIONS = {
  table: 4,
  memory: 5,
  global: 6,
  export: 7,
  start: 8,
  element: 9,
  dataCount: 12,
  data: 11,
};
const HEADER = Buffer.from([0, 97, 115, 109, 1, 0, 0, 0]);

class Reader {
  constructor(bytes) {
    this.bytes = bytes;
    this.pos = 0;
  }
  /** 读取下一个字节。
   * @returns {number} */
  byte() {
    ok(this.pos < this.bytes.length, "truncated Wasm section");
    return this.bytes[this.pos++];
  }
  /** 读取一个 LEB128 整数。
   * @returns {number} */
  integer() {
    let value = 0;
    for (let shift = 0; shift <= 28; shift += 7) {
      const byte = this.byte();
      value += (byte & 127) * 2 ** shift;
      if (byte < 128) return value;
    }
    throw new Error("unsupported Wasm integer");
  }
  /** 截取指定长度的字节。
   * @returns {Buffer} */
  take(length) {
    const start = this.pos;
    this.pos += length;
    ok(this.pos <= this.bytes.length, "truncated Wasm payload");
    return this.bytes.subarray(start, this.pos);
  }
  /** 读取剩余字节。
   * @returns {Buffer} */
  rest() {
    return this.take(this.bytes.length - this.pos);
  }
}

function leb(value, signed = false) {
  ok(Number.isSafeInteger(value) && value >= 0 && value <= 0xffffffff);
  const bytes = [];
  for (;;) {
    const low = value % 128;
    value = Math.floor(value / 128);
    const more = value !== 0 || (signed && low >= 64);
    bytes.push(low + (more ? 128 : 0));
    if (!more) break;
  }
  return Buffer.from(bytes);
}

function vector(count, ...payload) {
  return Buffer.concat([leb(count), ...payload]);
}

function appendEntries(body, entries) {
  const r = new Reader(body);
  return vector(r.integer() + entries.length, r.rest(), ...entries);
}

function emit(sections, overrides = new Map()) {
  const parts = [HEADER];
  for (const { id, body } of sections) {
    const bytes = overrides.get(id) ?? body;
    parts.push(Buffer.from([id]), leb(bytes.length), bytes);
  }
  return Buffer.concat(parts);
}

function checkActiveSegments(body, elements) {
  const r = new Reader(body);
  const count = r.integer();
  for (let i = 0; i < count; i++) {
    strictEqual(r.integer(), 0, "expected active segment for table or memory zero");
    strictEqual(r.byte(), 0x41, "expected i32.const offset");
    r.integer();
    strictEqual(r.byte(), 0x0b, "expected end of offset");
    const length = r.integer();
    if (elements) {
      for (let j = 0; j < length; j++) r.integer();
    } else {
      r.take(length);
    }
  }
  strictEqual(r.pos, body.length);
  return count;
}

function hostImports(module, calls) {
  const imports = Object.create(null);
  for (const { module: namespace, name, kind } of WebAssembly.Module.imports(module)) {
    const host = (imports[namespace] ??= Object.create(null));
    if (kind === "function") {
      host[name] = () => {
        calls.add(name);
        throw new Error(`UA initialization called host import ${name}`);
      };
    } else if (
      kind === "tag" &&
      namespace === "./kitesurf_core_bg.js" &&
      name === "__wbindgen_jstag"
    ) {
      host[name] = WebAssembly.JSTag;
    } else if (
      kind === "tag" &&
      namespace === "./kitesurf_core_bg.js" &&
      name === "__wbindgen_wrapped_jstag"
    ) {
      host[name] = new WebAssembly.Tag({ parameters: ["externref"] });
    } else {
      throw new Error(`unsupported UA initialization import ${namespace}.${name} (${kind})`);
    }
  }
  return imports;
}

// Export the private globals and tables in the build-time instance only. In particular,
// an externref allocated in the UA constructor cannot silently become a page-local handle.
function inspectionExports(section, tables, globals) {
  const entries = [];
  for (const [kind, count] of [
    [1, tables],
    [3, globals],
  ]) {
    for (let index = 0; index < count; index++) {
      const name = Buffer.from(`__ua_inspect_${kind}_${index}`);
      entries.push(Buffer.concat([leb(name.length), name, Buffer.from([kind]), leb(index)]));
    }
  }
  return appendEntries(section, entries);
}

function nonMemoryState(exports, tables, globals) {
  const result = [];
  for (let i = 0; i < globals; i++) result.push(exports[`__ua_inspect_3_${i}`].value);
  for (let i = 0; i < tables; i++) {
    const table = exports[`__ua_inspect_1_${i}`];
    result.push(Array.from({ length: table.length }, (_, j) => table.get(j)));
  }
  return result;
}

function changedSegments(before, after) {
  // Coalesce nearby writes to limit data-segment metadata. Never materialize untouched pages.
  const mergeGap = 32;
  const entries = [];
  let start = -1,
    last = -1;
  function finish() {
    const data = after.subarray(start, last + 1);
    entries.push(
      Buffer.concat([
        Buffer.from([0, 0x41]),
        leb(start, true),
        Buffer.from([0x0b]),
        leb(data.length),
        data,
      ]),
    );
  }
  for (let i = 0; i < after.length; i++) {
    if ((before[i] ?? 0) === after[i]) continue;
    if (start < 0 || i - last > mergeGap) {
      if (start >= 0) finish();
      start = i;
    }
    last = i;
  }
  if (start >= 0) finish();
  return entries;
}

/** 将默认 UA 样式表初始化结果写入返回的 Wasm 字节。
 * @returns {Buffer} */
export function preinitializeUaStylesheet(binary) {
  const r = new Reader(binary);
  deepStrictEqual(r.take(HEADER.length), HEADER);
  const sections = [];
  while (r.pos < binary.length) {
    const id = r.byte();
    sections.push({ id, body: r.take(r.integer()) });
  }
  const section = (id) => sections.find((s) => s.id === id)?.body;
  ok(!section(SECTIONS.start), "UA snapshot requires a module without a start section");
  checkActiveSegments(section(SECTIONS.element), true);
  const count = checkActiveSegments(section(SECTIONS.data), false);
  const countSection = section(SECTIONS.dataCount);
  if (countSection) strictEqual(new Reader(countSection).integer(), count);
  const tables = new Reader(section(SECTIONS.table)).integer();
  const globals = new Reader(section(SECTIONS.global)).integer();
  const memory = new Reader(section(SECTIONS.memory));
  strictEqual(memory.integer(), 1);
  const flags = memory.integer();
  ok(flags === 0 || flags === 1, "UA snapshot requires unshared 32-bit memory");
  const oldPages = memory.integer();
  const limits = memory.rest();
  const inspect = inspectionExports(section(SECTIONS.export), tables, globals);
  const module = new WebAssembly.Module(emit(sections, new Map([[SECTIONS.export, inspect]])));
  const calls = new Set();
  const imports = hostImports(module, calls);
  const instance = new WebAssembly.Instance(module, imports).exports;
  const before = Buffer.from(new Uint8Array(instance.memory.buffer));
  strictEqual(before.length, oldPages * PAGE_BYTES);
  const beforeState = nonMemoryState(instance, tables, globals);
  instance.ks_prepare_default_stylesheet();
  deepStrictEqual(
    nonMemoryState(instance, tables, globals),
    beforeState,
    "UA initialization changed a global or table",
  );
  const after = Buffer.from(instance.memory.buffer);
  const pages = after.length / PAGE_BYTES;
  const segments = changedSegments(before, after);
  const changes = new Map([
    [SECTIONS.data, appendEntries(section(SECTIONS.data), segments)],
    [SECTIONS.memory, vector(1, leb(flags), leb(pages), limits)],
  ]);
  if (countSection) changes.set(SECTIONS.dataCount, leb(count + segments.length));
  const result = emit(sections, changes);
  // Check the emitted bytes at the same initialization boundary as the original constructor.
  // Creating the original instance twice also catches nondeterministic initialization.
  const reference = new WebAssembly.Instance(module, imports).exports;
  reference.ks_prepare_default_stylesheet();
  ok(after.equals(Buffer.from(reference.memory.buffer)), "nondeterministic UA initialization");
  strictEqual(calls.size, 0, "UA initialization called a host import");
  const restored = new WebAssembly.Instance(new WebAssembly.Module(result), imports).exports;
  ok(
    after.equals(Buffer.from(restored.memory.buffer)),
    "UA snapshot did not reproduce Wasm memory",
  );
  return result;
}
