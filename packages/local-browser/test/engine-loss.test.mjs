// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import childProcess from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { ProfileStore } from "../src/profile-store.mjs";
import { startBrowser } from "../src/server.mjs";
import { connect, evaluate, fixtureServer, openPage } from "./helpers.mjs";

test(
  "losing workerd during close releases the profile and never reports a clean save",
  {
    timeout: 60_000,
  },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-engine-loss-"));
    const path = join(directory, "profile.sqlite");
    const fixture = await fixtureServer();
    const engine = resolve(import.meta.dirname, "../../engine");
    const profile = new ProfileStore(path, { create: true });
    let engineProcess;
    const spawn = childProcess.spawn;
    t.mock.method(childProcess, "spawn", (...args) => {
      const child = spawn(...args);
      if (String(args[0]).endsWith("/workerd") && args[1]?.includes("serve")) engineProcess = child;
      return child;
    });
    const browser = await startBrowser({
      profile,
      port: 0,
      allowPrivateNetwork: true,
      dist: process.env.KITESURF_TEST_DIST || join(engine, "local_dist"),
      assets: process.env.KITESURF_TEST_ASSETS || join(engine, "public"),
    });
    const cdp = await connect(browser.port);
    let ended = false;
    let closing;
    const originalFetch = profile.fetch.bind(profile);
    const blocked = Promise.withResolvers();
    const release = Promise.withResolvers();
    t.after(async () => {
      release.resolve();
      profile.fetch = originalFetch;
      await closing?.catch(() => {});
      await cdp.disconnect();
      if (!ended) await browser.close().catch(() => {});
      await fixture.close();
      rmSync(directory, { recursive: true, force: true });
    });
    const page = await openPage(cdp, `${fixture.url}/engine-loss`);
    await evaluate(cdp, page.sessionId, "localStorage.setItem('durable','saved')");
    await cdp.send("Kitesurf.flushProfile");
    profile.fetch = async (request) => {
      if (new URL(request.url).pathname !== "/save") return originalFetch(request);
      blocked.resolve();
      await release.promise;
      return Response.json({ message: "Injected engine termination" }, { status: 409 });
    };
    await evaluate(cdp, page.sessionId, "localStorage.setItem('uncommitted','lost')");
    closing = assert.rejects(browser.close(), /session ended|session is lost/);
    await blocked.promise;
    assert.ok(engineProcess, "must identify this test's own workerd child");
    engineProcess.kill("SIGKILL");
    release.resolve();
    await closing;
    assert.equal(await browser.done, "engine-lost");
    ended = true;
    await assert.rejects(fetch(`http://127.0.0.1:${browser.port}/health`));
    const restored = new ProfileStore(path);
    try {
      const entries = new Map(
        restored.read().state.localStorage.find(([origin]) => origin === fixture.url)[1],
      );
      assert.equal(entries.get("durable"), "saved");
      assert.equal(entries.has("uncommitted"), false);
    } finally {
      restored.close();
    }
  },
);
