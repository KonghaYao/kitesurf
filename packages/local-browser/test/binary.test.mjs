// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PNG } from "pngjs";
import { connect, evaluate, fixtureServer, openPage, until } from "./helpers.mjs";

const executable = process.env.KITESURF_TEST_BINARY;

test(
  "standalone executable: empty PATH, real rendering, crash and credential recovery",
  { timeout: 300_000, skip: !executable },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-binary-test-"));
    const fixture = await fixtureServer();
    const path = join(directory, "account.sqlite");
    const env = { ...process.env, PATH: "", NODE_OPTIONS: "", TMPDIR: directory };
    let child;
    let cdp;
    let output = "";
    let port;
    const start = async (create) => {
      output = "";
      child = spawn(
        executable,
        [
          "serve",
          "--profile",
          path,
          "--port",
          "0",
          "--allow-private-network",
          ...(create ? ["--create-profile"] : []),
        ],
        { env, detached: true, stdio: ["ignore", "pipe", "pipe"] },
      );
      child.stdout.on("data", (data) => {
        output += data;
      });
      child.stderr.on("data", (data) => {
        output += data;
      });
      const ready = await until(
        () => {
          if (child.exitCode !== null) throw new Error(output.slice(-4000));
          for (const line of output.split("\n")) {
            try {
              const value = JSON.parse(line);
              if (value.ready) return value;
            } catch {}
          }
          return false;
        },
        "standalone browser readiness",
        60_000,
      );
      port = ready.port;
      cdp = await connect(port);
      return ready;
    };
    t.after(async () => {
      await cdp?.disconnect();
      if (child && child.exitCode === null && child.signalCode === null) {
        const ended = once(child, "exit");
        process.kill(-child.pid, "SIGKILL");
        await ended;
      }
      await fixture.close();
      rmSync(directory, { recursive: true, force: true });
    });
    const version = spawnSync(executable, ["--version"], {
      env,
      encoding: "utf8",
      timeout: 30_000,
    });
    assert.equal(version.status, 0, version.stderr);
    assert.match(version.stdout, /0\.5\.0/);
    const first = await start(true);
    let page = await openPage(cdp, `${fixture.url}/before-crash`);
    await evaluate(cdp, page.sessionId, "document.getElementById('login').click()");
    await until(
      () => evaluate(cdp, page.sessionId, "window.loggedIn === true"),
      "standalone login",
    );
    const screenshot = await cdp.send("Page.captureScreenshot", { format: "png" }, page.sessionId);
    assert.ok(PNG.sync.read(Buffer.from(screenshot.data, "base64")).width > 0);
    const flush = spawnSync(executable, ["flush", "--port", String(port)], {
      env,
      encoding: "utf8",
      timeout: 30_000,
    });
    assert.equal(flush.status, 0, flush.stderr);
    await cdp.disconnect();
    cdp = undefined;
    const ended = once(child, "exit");
    process.kill(-child.pid, "SIGKILL");
    await ended;
    const second = await start(false);
    assert.notEqual(second.generation, first.generation);
    page = await openPage(cdp, `${fixture.url}/after-crash`);
    assert.deepEqual(
      await evaluate(cdp, page.sessionId, "[boot.token,boot.authenticated,boot.step]"),
      ["T1", true, null],
    );
    const stopped = once(child, "exit");
    await cdp.send("Browser.close");
    await stopped;
    assert.equal(child.exitCode, 0, output.slice(-4000));
    cdp = undefined;
  },
);
