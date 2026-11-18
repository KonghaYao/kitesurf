// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// `bazel run //packages/engine:e2e -- [vitest filters and flags]`: run the e2e suite, or the files
// matching the filters, against the engine and fixtures dists Bazel built. The output observer is
// shared with scripts/e2e-shard.mjs: a real workerd line starting with "uncaught exception;" fails
// the run even when Vitest exits 0, and chunk boundaries must not hide it.

import { spawn } from "node:child_process";
import { readFileSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const UNCAUGHT_PREFIX = /^uncaught exception;/;
const ANSI = new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g");

/** Strip terminal color so the prefix is checked at column zero.
 * @param {string} line @returns {string}
 */
function normalized(line) {
  return line.replace(ANSI, "").replace(/\r$/, "");
}

/** Incremental UTF-8 line splitter for child output and log files.
 * @param {(line: string) => void} onLine @returns {{ push(chunk: string): void, end(): void }}
 */
export function createLineSplitter(onLine) {
  let pending = "";
  return {
    push(chunk) {
      pending += chunk;
      let newline;
      while ((newline = pending.indexOf("\n")) !== -1) {
        onLine(pending.slice(0, newline));
        pending = pending.slice(newline + 1);
      }
    },
    end() {
      if (pending !== "") onLine(pending);
      pending = "";
    },
  };
}

/** @returns {{ push(chunk: string): void, end(): void, lines: string[] }} */
export function createUncaughtWatcher() {
  const lines = [];
  const splitter = createLineSplitter((line) => {
    if (UNCAUGHT_PREFIX.test(normalized(line))) lines.push(normalized(line));
  });
  return {
    push: splitter.push,
    end: splitter.end,
    lines,
  };
}

/** @param {string} text @returns {string[]} */
export function uncaughtLinesIn(text) {
  const watcher = createUncaughtWatcher();
  watcher.push(text);
  watcher.end();
  return watcher.lines;
}

// Run a child with its piped output teed to the terminal while scanning both streams.
/** @param {string} command @param {string[]} args
 * @param {object} [options] @returns {Promise<object>}
 */
export async function runObserved(command, args, options = {}) {
  const child = spawn(command, args, {
    cwd: options.cwd,
    env: options.env,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const watcher = createUncaughtWatcher();
  const tee = (target) => (chunk) => {
    target.write(chunk);
    watcher.push(chunk);
  };
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", tee(options.stdout ?? process.stdout));
  child.stderr.on("data", tee(options.stderr ?? process.stderr));
  const result = await new Promise((resolve) => {
    child.once("error", (error) => resolve({ code: null, signal: null, error }));
    child.once("close", (code, signal) => resolve({ code, signal }));
  });
  watcher.end();
  return { ...result, uncaught: watcher.lines };
}

/** Preserve a failing child status; otherwise turn observed uncaught lines into exit 1.
 * @param {{ code: number | null, signal: NodeJS.Signals | null }} result
 * @param {number} uncaughtCount @returns {number}
 */
export function exitCodeFor(result, uncaughtCount) {
  if (typeof result.code === "number" && result.code !== 0) return result.code;
  if (uncaughtCount > 0) return 1;
  return result.code === 0 ? 0 : 1;
}

async function main() {
  const engineDir = resolve(import.meta.dirname, "../packages/engine");
  const require = createRequire(resolve(engineDir, "package.json"));
  const manifest = require.resolve("vitest/package.json");
  const vitest = resolve(dirname(manifest), JSON.parse(readFileSync(manifest, "utf8")).bin.vitest);
  const result = await runObserved(
    process.execPath,
    [vitest, "run", "--config", "vitest.e2e.config.mts", ...process.argv.slice(2)],
    {
      cwd: engineDir,
      env: {
        ...process.env,
        KS_E2E_ENGINE_DIST: resolve(engineDir, "dist"),
        KS_E2E_FIXTURES_DIST: resolve(engineDir, "e2e_fixtures_dist"),
      },
    },
  );
  if (result.error) {
    console.error(`[e2e] could not start vitest: ${result.error.message}`);
    process.exit(1);
  }
  if (result.uncaught.length > 0) {
    console.error(
      `[e2e] workerd reported ${result.uncaught.length} uncaught exception line(s); failing the run`,
    );
  }
  process.exit(exitCodeFor(result, result.uncaught.length));
}

const invokedDirectly =
  process.argv[1] !== undefined &&
  realpathSync(process.argv[1]) === realpathSync(fileURLToPath(import.meta.url));
if (invokedDirectly) await main();
