// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Owns the local system-font child process and exposes the private LOCAL_FONTS binding.
// Requests are constrained to generic/script selection and opaque ids; no path crosses the IPC.

import { spawn } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { join } from "node:path";

const MAX_REQUEST_BYTES = 64 * 1024;
const MAX_HEADER_BYTES = 64 * 1024;
const MAX_FACE_BYTES = 24 * 1024 * 1024;
const MAX_QUEUE = 4;
const REQUEST_TIMEOUT_MS = 20_000;
const MAX_STDERR_BYTES = 8 * 1024;
const FAMILIES = new Set([
  "sans",
  "sans-serif",
  "ui-sans",
  "serif",
  "ui-serif",
  "mono",
  "monospace",
  "ui-mono",
  "system-ui",
  "emoji",
  "script",
]);

class FontHostError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
  }
}

function json(status, value) {
  return new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json", "cache-control": "no-store" },
  });
}

function safeText(value, max) {
  return typeof value === "string" && value.length <= max ? value : null;
}

function validateId(value) {
  return typeof value === "string" && /^[0-9a-f]{32}$/.test(value) ? value : null;
}

function validateMatch(body) {
  if (!body || typeof body !== "object") throw new Error("invalid match body");
  const family = body.family === undefined ? undefined : safeText(body.family, 32);
  const script = body.script === undefined ? undefined : safeText(body.script, 4);
  const locale = body.locale === undefined ? undefined : safeText(body.locale, 32);
  const style = body.style === undefined ? undefined : safeText(body.style, 16);
  const text = body.text === undefined ? undefined : safeText(body.text, 4096);
  const weight = body.weight === undefined ? undefined : body.weight;
  if (body.family !== undefined && family === null) throw new Error("invalid family");
  if (family !== undefined && !FAMILIES.has(family)) throw new Error("unknown family");
  if (body.script !== undefined && (script === null || !/^[A-Za-z]{4}$/.test(script)))
    throw new Error("invalid script");
  if (body.locale !== undefined && (locale === null || !/^[A-Za-z0-9_-]{1,32}$/.test(locale)))
    throw new Error("invalid locale");
  if (
    body.style !== undefined &&
    (style === null || !["normal", "italic", "oblique"].includes(style))
  )
    throw new Error("invalid style");
  if (weight !== undefined && (!Number.isFinite(weight) || weight < 1 || weight > 1000))
    throw new Error("invalid weight");
  if (text !== null && text !== undefined && [...text].length > 256)
    throw new Error("text sample too large");
  return { family, script, locale, style, weight, text };
}

/** @returns {string} */
export function resolveFontHost(root) {
  const runfiles = process.env.RUNFILES_DIR;
  const workspace = process.env.TEST_WORKSPACE;
  const candidates = [
    process.env.KITESURF_FONT_HOST,
    join(root, "font-host"),
    join(root, "font-host-bin", "kitesurf-font-host"),
    join(root, "font-host-bin"),
    runfiles && workspace
      ? join(runfiles, workspace, "packages/local-browser/font-host/kitesurf-font-host")
      : null,
  ].filter(Boolean);
  for (const candidate of candidates) {
    try {
      if (existsSync(candidate) && statSync(candidate).isFile()) return candidate;
    } catch {}
  }
  throw new Error(
    "The system font helper is missing. Build it with Bazel or reinstall the local browser.",
  );
}

/** @returns {Promise<{
 * probe: object,
 * close: () => Promise<void>,
 * stats: () => Promise<object>,
 * fetch: (request: Request) => Promise<Response>
 * }>} */
export async function startFontHost({ binary }) {
  const child = spawn(binary, [], {
    stdio: ["pipe", "pipe", "pipe"],
    env: process.env,
  });
  let buffer = Buffer.alloc(0);
  let active = null;
  let closed = false;
  let stderr = "";
  let spawnError = null;
  const queue = [];
  const failAll = (error) => {
    if (active) {
      clearTimeout(active.timer);
      active.reject(error);
      active = null;
    }
    for (const item of queue.splice(0)) {
      clearTimeout(item.timer);
      item.reject(error);
    }
  };
  const stop = () => {
    closed = true;
    if (!child.killed) child.kill("SIGKILL");
  };
  child.on("error", (error) => {
    spawnError = error;
    failAll(new FontHostError("unavailable", `System font helper failed: ${error.message}`));
  });
  child.on("exit", () => {
    closed = true;
    failAll(new FontHostError("unavailable", "System font helper exited"));
  });
  child.stderr.on("data", (chunk) => {
    stderr = (stderr + chunk.toString()).slice(-MAX_STDERR_BYTES);
  });

  const writeFrame = (value) => {
    const header = Buffer.from(JSON.stringify(value));
    if (header.length > MAX_HEADER_BYTES)
      throw new FontHostError("bad_request", "Font helper request is too large");
    const frame = Buffer.allocUnsafe(4 + header.length + 4);
    frame.writeUInt32BE(header.length, 0);
    header.copy(frame, 4);
    frame.writeUInt32BE(0, 4 + header.length);
    child.stdin.write(frame);
  };

  const finish = (header, body) => {
    if (!active) return;
    const item = active;
    active = null;
    clearTimeout(item.timer);
    if (header.ok === true) item.resolve({ header, body });
    else
      item.reject(
        new FontHostError(
          typeof header.code === "string" ? header.code : "internal",
          typeof header.message === "string" ? header.message : "Font helper rejected the request",
        ),
      );
    pump();
  };

  const onData = (chunk) => {
    buffer = Buffer.concat([buffer, chunk]);
    for (;;) {
      if (buffer.length < 4) return;
      const headerLength = buffer.readUInt32BE(0);
      if (headerLength > MAX_HEADER_BYTES) {
        stop();
        failAll(new FontHostError("bad_response", "Font helper sent an oversized header"));
        return;
      }
      if (buffer.length < 4 + headerLength + 4) return;
      const bodyLength = buffer.readUInt32BE(4 + headerLength);
      if (bodyLength > MAX_FACE_BYTES) {
        stop();
        failAll(new FontHostError("bad_response", "Font helper sent an oversized body"));
        return;
      }
      const end = 4 + headerLength + 4 + bodyLength;
      if (buffer.length < end) return;
      let header;
      try {
        header = JSON.parse(buffer.subarray(4, 4 + headerLength).toString("utf8"));
      } catch {
        stop();
        failAll(new FontHostError("bad_response", "Font helper sent invalid JSON"));
        return;
      }
      const body = Buffer.from(buffer.subarray(4 + headerLength + 4, end));
      buffer = buffer.subarray(end);
      finish(header, body);
    }
  };
  child.stdout.on("data", onData);

  const pump = () => {
    if (active || closed || queue.length === 0) return;
    const item = queue.shift();
    active = item;
    item.timer = setTimeout(() => {
      stop();
      failAll(new FontHostError("timeout", "System font helper timed out"));
    }, REQUEST_TIMEOUT_MS);
    try {
      writeFrame(item.value);
    } catch (error) {
      active = null;
      clearTimeout(item.timer);
      item.reject(error);
      pump();
    }
  };

  const request = (value) => {
    if (closed || spawnError)
      return Promise.reject(
        spawnError
          ? new FontHostError("unavailable", `System font helper failed: ${spawnError.message}`)
          : new FontHostError("unavailable", "System font helper is not running"),
      );
    if (active && queue.length >= MAX_QUEUE)
      return Promise.reject(new FontHostError("busy", "System font helper is busy"));
    return new Promise((resolve, reject) => {
      queue.push({ value, resolve, reject, timer: null });
      pump();
    });
  };

  const probe = await request({ op: "probe" });
  const status = probe.header?.probe;
  if (!status || status.sans !== true) {
    await close();
    throw new Error(
      "No usable system sans-serif font is installed; the local browser cannot start without a base font.",
    );
  }

  async function close() {
    if (!closed) {
      try {
        await Promise.race([
          request({ op: "shutdown" }),
          new Promise((resolve) => setTimeout(resolve, 500)),
        ]);
      } catch {}
    }
    closed = true;
    if (!child.killed) child.kill("SIGTERM");
    await new Promise((resolve) => {
      if (child.exitCode !== null || child.signalCode !== null) return resolve();
      const timer = setTimeout(() => {
        if (!child.killed) child.kill("SIGKILL");
        resolve();
      }, 500);
      child.once("exit", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  return {
    probe: status,
    close,
    stats() {
      return request({ op: "stats" }).then((result) => result.header.stats);
    },
    async fetch(req) {
      const url = new URL(req.url);
      if (req.method !== "POST") return json(405, { error: "method not allowed" });
      let body;
      try {
        const text = await req.text();
        if (text.length > MAX_REQUEST_BYTES) throw new Error("request too large");
        body = text ? JSON.parse(text) : {};
      } catch {
        return json(400, { error: "invalid request" });
      }
      try {
        if (url.pathname === "/match") {
          const match = validateMatch(body);
          const result = await request({ op: "match", ...match });
          return json(200, result.header.match);
        }
        if (url.pathname === "/face") {
          const id = validateId(body.id);
          if (!id) return json(400, { error: "invalid font id" });
          const result = await request({ op: "face", id });
          if (!Number.isInteger(result.header.bytes) || result.header.bytes !== result.body.length)
            return json(502, { error: "font response is inconsistent" });
          return new Response(result.body, {
            status: 200,
            headers: {
              "content-type": "font/otf",
              "content-length": String(result.body.length),
              "cache-control": "no-store",
            },
          });
        }
        if (url.pathname === "/stats") {
          const result = await request({ op: "stats" });
          return json(200, result.header.stats);
        }
        return json(404, { error: "not found" });
      } catch (error) {
        if (!(error instanceof FontHostError))
          console.error("[font-host] request failed:", error?.message ?? error);
        const code = error instanceof FontHostError ? error.code : "internal";
        const status =
          code === "bad_request" || code === "unknown_id" || code === "expired_id"
            ? 404
            : code === "busy"
              ? 429
              : 502;
        return json(status, { error: code });
      }
    },
  };
}
