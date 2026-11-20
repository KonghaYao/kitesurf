// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { chmodSync, copyFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { connect, evaluate, fixtureServer, openPage } from "./helpers.mjs";
import { located, withTimeout } from "./host-adapter.mjs";

const executable = located(process.env.KITESURF_TEST_BINARY);
const runtime = executable ? "native-binary" : "node-host";
const timeout = 3_900_000;
const exec = promisify(execFile);

async function startNativeBrowser(directory) {
  const binary = join(directory, "kitesurf");
  copyFileSync(executable, binary);
  chmodSync(binary, 0o755);
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      ([key]) => !/^(KITESURF_|KS_|RUNFILES_|JS_BINARY_|BAZEL_)/.test(key),
    ),
  );
  Object.assign(env, {
    PATH: "",
    NODE_OPTIONS: "",
    NODE_PATH: "",
    TMPDIR: directory,
    HOME: directory,
    XDG_DATA_HOME: join(directory, "data"),
  });
  const options = { cwd: directory, env };
  const cli = async (...args) => (await exec(binary, args, { ...options, timeout: 30_000 })).stdout;
  assert.equal(await cli("--version"), "kitesurf 0.5.2\n", "use the Rust release executable");
  const child = spawn(binary, ["serve", "--port", "0", "--allow-private-network"], {
    ...options,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const exit = new Promise((finish) =>
    child.once("close", (code, signal) => finish({ code, signal })),
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr = (stderr + chunk).slice(-8_192);
  });
  const output = createInterface({ input: child.stdout });
  const ready = new Promise((finish, fail) => {
    child.once("error", fail);
    child.once("close", (code, signal) =>
      fail(new Error(`Host exited before ready (${code ?? signal}): ${stderr}`)),
    );
    output.on("line", (line) => {
      try {
        const value = JSON.parse(line);
        if (value.ready === true) finish(value);
      } catch {}
    });
  });
  const reap = async () => {
    if (child.exitCode === null && child.signalCode === null && child.kill("SIGTERM")) {
      if ((await withTimeout(exit, 15_000)).timeout) process.kill(-child.pid, "SIGKILL");
    }
    await exit;
  };
  try {
    const { port } = await withTimeout(ready, 60_000);
    assert.ok(Number.isInteger(port) && port > 0, `Native host was not ready: ${stderr}`);
    return {
      port,
      status: async () => JSON.parse(await cli("status", "--port", String(port))),
      async close() {
        try {
          assert.deepEqual(JSON.parse(await cli("stop", "--port", String(port))), { closed: true });
          const stopped = await withTimeout(exit, 15_000);
          assert.deepEqual(stopped, { code: 0, signal: null }, stderr);
          assert.doesNotMatch(stderr, /uncaught|internal[\s_-]+error/i);
        } finally {
          await reap();
        }
      },
    };
  } catch (error) {
    await reap();
    throw error;
  }
}

async function startLegacyBrowser(directory) {
  const { ProfileStore } = await import("../src/profile-store.mjs");
  const { startBrowser } = await import("../src/server.mjs");
  const engine = resolve(import.meta.dirname, "../../engine");
  const profile = new ProfileStore(join(directory, "profile.sqlite"), { create: true });
  return startBrowser({
    profile,
    port: 0,
    allowPrivateNetwork: true,
    dist: process.env.KITESURF_TEST_DIST || join(engine, "local_dist"),
    assets: process.env.KITESURF_TEST_ASSETS || join(engine, "public"),
  });
}

test(
  `an unattended ${runtime} keeps its actual page beyond upstream lifetime limits`,
  { timeout },
  async (t) => {
    const requestedIdleMilliseconds = Math.ceil(
      Number(process.env.KITESURF_SOAK_MINUTES || 61) * 60_000,
    );
    assert.ok(
      requestedIdleMilliseconds > 0 && requestedIdleMilliseconds < timeout,
      "invalid soak duration",
    );
    const directory = mkdtempSync(
      executable ? "/tmp/kitesurf-soak-" : join(tmpdir(), "kitesurf-soak-"),
    );
    const fixture = await fixtureServer();
    let browser;
    let cdp;
    t.after(async () => {
      try {
        await cdp?.disconnect();
      } finally {
        try {
          await browser?.close();
        } finally {
          await fixture.close();
          rmSync(directory, { recursive: true, force: true });
        }
      }
    });
    browser = await (executable ? startNativeBrowser(directory) : startLegacyBrowser(directory));
    cdp = await connect(browser.port);
    const page = await openPage(cdp, `${fixture.url}/long-task`);
    await evaluate(
      cdp,
      page.sessionId,
      "window.liveMarker=61317; document.getElementById('draft').value='waiting for user'; sessionStorage.setItem('step','confirm')",
    );
    const before = await browser.status();
    await cdp.disconnect();
    const startedAt = Date.now();
    const idleStarted = performance.now();
    console.log(
      JSON.stringify({
        event: "idle-start",
        runtime,
        startedAt: new Date(startedAt).toISOString(),
        port: browser.port,
        generation: before.generation,
        requestedIdleMilliseconds,
      }),
    );
    // No polling or CDP heartbeat during this interval: it must be real idle time.
    let idleMilliseconds = 0;
    while (idleMilliseconds < requestedIdleMilliseconds) {
      await delay(requestedIdleMilliseconds - idleMilliseconds);
      idleMilliseconds = Math.floor(performance.now() - idleStarted);
    }
    cdp = await connect(browser.port);
    const attached = await cdp.send("Target.attachToTarget", {
      targetId: page.targetId,
      flatten: true,
    });
    const state = await evaluate(
      cdp,
      attached.sessionId,
      "[window.liveMarker,document.getElementById('draft').value,sessionStorage.getItem('step')]",
    );
    assert.deepEqual(state, [61317, "waiting for user", "confirm"]);
    const after = await browser.status();
    assert.equal(after.sessionId, before.sessionId);
    assert.equal(after.generation, before.generation);
    const image = await cdp.send("Page.captureScreenshot", { format: "png" }, attached.sessionId);
    assert.ok(Buffer.from(image.data, "base64").length > 1000);
    const report = {
      runtime,
      startedAt,
      finishedAt: Date.now(),
      requestedIdleMilliseconds,
      idleMilliseconds,
      sameSession: true,
      sameLivePage: true,
      screenshotBytes: Buffer.from(image.data, "base64").length,
    };
    console.log(JSON.stringify(report));
    if (process.env.KITESURF_SOAK_REPORT)
      writeFileSync(process.env.KITESURF_SOAK_REPORT, JSON.stringify(report, null, 2) + "\n");
  },
);
