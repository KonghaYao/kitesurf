// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 一套契约测试，两种宿主实现：原生 supervisor（T2）和 035a014 的 Node 宿主，后者用作
// T0 基线。legacy 树由调用方解压；Bazel 只运行原生实现。

import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

/** Bazel 环境变量保存 bin 相对路径；测试在 runfiles 树内运行。
 * @param {string | undefined} value
 * @returns {string | undefined}
 */
export function located(value) {
  if (!value || isAbsolute(value)) return value;
  const runfiles = process.env.RUNFILES_DIR;
  if (!runfiles) return value;
  return join(runfiles, process.env.JS_BINARY__WORKSPACE || "_main", value);
}

/** @param {string} name @returns {string} */
export function required(name) {
  const value = located(process.env[name]);
  if (!value) throw new Error(`${name} must be set`);
  return value;
}

/** @param {string} path @returns {Array<object>} */
export function readEvents(path) {
  try {
    return readFileSync(path, "utf8")
      .split("\n")
      .filter((line) => line.trim() !== "")
      .map((line) => JSON.parse(line));
  } catch {
    return [];
  }
}

/** @returns {Promise<*>} */
export async function waitFor(check, description, timeout = 30_000) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  throw new Error(`Timed out waiting for ${description}: ${last?.message || "condition false"}`);
}

/** 与退出 promise 竞速，且不留下持有测试进程的定时器。
 * @returns {Promise<*>}
 */
export async function withTimeout(promise, timeout, fallback = { timeout: true }) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((resolve) => {
        timer = setTimeout(resolve, timeout, fallback);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function request(route, options = {}) {
  const response = await fetch(`http://127.0.0.1:${this.port}${route}`, {
    method: options.method || "GET",
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const text = await response.text();
  let body;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  return { status: response.status, body };
}

function commandLine(pid) {
  try {
    return execFileSync("ps", ["-p", String(pid), "-o", "command="], { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

function engineSocketFrom(command) {
  const match = command.match(/--socket-addr engine=unix:([^ ]+)/);
  return match ? match[1] : null;
}

const HOST_METHODS = Object.freeze({
  request,
  events() {
    return readEvents(this.observerPath);
  },
  stderrText() {
    return this.stderr;
  },
  stdoutText() {
    return this.stdout;
  },
  runDir() {
    const listen = readEvents(this.observerPath)
      .filter((event) => event.kind === "engine-listen" && typeof event.path === "string")
      .at(-1);
    if (listen) return join(listen.path, "..");
    const socket = engineSocketFrom(commandLine(this.pid));
    return socket ? join(socket, "..") : null;
  },
});

/**
 * 主机开始监听并打印 ready JSON 行。
 * @returns {Promise<object>}
 */
export async function startNativeHost({
  workerd,
  config,
  assets,
  assetsManifest,
  port = 0,
  observe,
  startupTimeoutMs = 30_000,
  profileOrigin,
  profile,
  createProfile = false,
  env,
}) {
  const directory = mkdtempSync(join(tmpdir(), "ks-native-host-"));
  const observerPath = observe || join(directory, "observe.jsonl");
  const binary = located(process.env.KITESURF_NATIVE_HOST);
  if (!binary) throw new Error("KITESURF_NATIVE_HOST must point at the native host binary");
  // Bazel 沙箱的 TMPDIR 对 Darwin Unix socket（104 字节）来说太长。宿主仍自行选择私有
  // 运行目录；这里只提供一个较短的父目录。
  const runRoot = mkdtempSync("/tmp/ks-native-run-");
  const args = [
    "serve",
    "--workerd",
    workerd,
    "--config",
    config,
    "--assets-root",
    assets,
    "--assets-manifest",
    assetsManifest,
    "--port",
    String(port),
    "--startup-timeout-ms",
    String(startupTimeoutMs),
    "--test-observe",
    observerPath,
  ];
  if (profileOrigin) args.push("--profile-origin", profileOrigin);
  // 默认宿主使用显式的内存测试替身；正式 SQLite 只由测试显式传入 profile。
  if (profile) {
    args.push("--profile", profile);
    if (createProfile) args.push("--create-profile");
  } else {
    args.push("--test-memory-profile");
  }
  const child = spawn(binary, args, {
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...env, TMPDIR: runRoot },
  });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const exit = once(child, "exit").then(([code, signal]) => ({ code, signal }));
  const ready = await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`no ready line; stderr: ${stderr.slice(-4000)}`)),
      startupTimeoutMs + 5_000,
    );
    const scan = () => {
      for (const line of stdout.split("\n")) {
        if (!line.startsWith("{")) continue;
        try {
          const value = JSON.parse(line);
          if (value.ready) {
            clearTimeout(timer);
            resolve(value);
            return;
          }
        } catch {}
      }
    };
    child.stdout.on("data", scan);
    exit.then((status) => {
      clearTimeout(timer);
      reject(
        new Error(
          `host exited ${JSON.stringify(status)} before ready; stderr: ${stderr.slice(-4000)}`,
        ),
      );
    });
  }).catch((error) => {
    child.kill("SIGKILL");
    rmSync(runRoot, { recursive: true, force: true });
    throw error;
  });
  const host = {
    ...HOST_METHODS,
    kind: "native",
    pid: child.pid,
    port: ready.port,
    generation: ready.generation,
    profile: ready.profile,
    ready,
    observerPath,
    directory,
    exit,
    get stdout() {
      return stdout;
    },
    get stderr() {
      return stderr;
    },
    async stop(signal = "SIGTERM", timeout = 20_000) {
      if (child.exitCode === null && child.signalCode === null) child.kill(signal);
      try {
        const status = await withTimeout(exit, timeout);
        if (status.timeout) {
          child.kill("SIGKILL");
          throw new Error(`host did not exit after ${signal}; stderr: ${stderr.slice(-4000)}`);
        }
        return status;
      } finally {
        rmSync(runRoot, { recursive: true, force: true });
      }
    },
  };
  return host;
}

/** 035a014 宿主：Node、Miniflare 和旧 CLI，从解压后的源码树运行。
 * @returns {Promise<object>}
 */
export async function startLegacyHost({
  root,
  dist,
  assets,
  allowPrivateNetwork = true,
  port = 0,
  profilePath,
  createProfile = true,
  env = {},
}) {
  const directory = mkdtempSync(join(tmpdir(), "ks-legacy-host-"));
  const explicitProfile =
    profilePath === undefined ? join(directory, "profile.sqlite") : profilePath;
  const args = [join(root, "src/cli.mjs"), "serve", "--dist", dist, "--assets", assets];
  if (port !== null) args.push("--port", String(port));
  if (explicitProfile !== null) args.push("--profile", explicitProfile);
  if (createProfile && explicitProfile !== null) args.push("--create-profile");
  if (allowPrivateNetwork) args.push("--allow-private-network");
  const child = spawn(process.execPath, args, {
    stdio: ["ignore", "pipe", "pipe"],
    env: { ...process.env, ...env },
  });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const exit = once(child, "exit").then(([code, signal]) => ({ code, signal }));
  const ready = await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`no ready line; stderr: ${stderr.slice(-4000)}`)),
      90_000,
    );
    const scan = () => {
      for (const line of stdout.split("\n")) {
        if (!line.startsWith("{")) continue;
        try {
          const value = JSON.parse(line);
          if (value.ready) {
            clearTimeout(timer);
            resolve(value);
            return;
          }
        } catch {}
      }
    };
    child.stdout.on("data", scan);
    exit.then((status) => {
      clearTimeout(timer);
      reject(
        new Error(
          `legacy host exited ${JSON.stringify(status)} before ready; stderr: ${stderr.slice(-4000)}`,
        ),
      );
    });
  }).catch((error) => {
    child.kill("SIGKILL");
    throw error;
  });
  const host = {
    ...HOST_METHODS,
    kind: "legacy",
    pid: child.pid,
    port: ready.port,
    generation: ready.generation,
    profile: ready.profile,
    ready,
    root,
    dist,
    assets,
    observerPath: null,
    directory,
    exit,
    get stdout() {
      return stdout;
    },
    get stderr() {
      return stderr;
    },
    async stop(signal = "SIGTERM", timeout = 60_000) {
      if (child.exitCode === null && child.signalCode === null) child.kill(signal);
      const status = await withTimeout(exit, timeout);
      if (status.timeout) {
        child.kill("SIGKILL");
        throw new Error(`legacy host did not exit after ${signal}; stderr: ${stderr.slice(-4000)}`);
      }
      rmSync(directory, { recursive: true, force: true });
      return status;
    },
  };
  return host;
}

/** @returns {Array<object>} */
export function eventsOfKind(events, kind) {
  return events.filter((event) => event.kind === kind);
}
