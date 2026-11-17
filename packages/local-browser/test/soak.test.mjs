// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as delay } from "node:timers/promises";
import { ProfileStore } from "../src/profile-store.mjs";
import { startBrowser } from "../src/server.mjs";
import { connect, evaluate, fixtureServer, openPage } from "./helpers.mjs";

test(
  "an unattended browser keeps its actual page beyond upstream lifetime limits",
  { timeout: 3_900_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-soak-"));
    const fixture = await fixtureServer();
    const engine = resolve(import.meta.dirname, "../../engine");
    const profile = new ProfileStore(join(directory, "profile.sqlite"), { create: true });
    const browser = await startBrowser({
      profile,
      port: 0,
      allowPrivateNetwork: true,
      dist: process.env.KITESURF_TEST_DIST || join(engine, "local_dist"),
      assets: process.env.KITESURF_TEST_ASSETS || join(engine, "public"),
    });
    let cdp = await connect(browser.port);
    t.after(async () => {
      await cdp.disconnect();
      await browser.close();
      await fixture.close();
      rmSync(directory, { recursive: true, force: true });
    });
    const page = await openPage(cdp, `${fixture.url}/long-task`);
    await evaluate(
      cdp,
      page.sessionId,
      "window.liveMarker=61317; document.getElementById('draft').value='waiting for user'; sessionStorage.setItem('step','confirm')",
    );
    const before = await browser.status();
    await cdp.disconnect();
    const startedAt = Date.now();
    console.log(
      JSON.stringify({
        event: "idle-start",
        startedAt: new Date(startedAt).toISOString(),
        port: browser.port,
        generation: before.generation,
      }),
    );
    // No polling or CDP heartbeat during this interval: it must be real idle time.
    await delay(Number(process.env.KITESURF_SOAK_MINUTES || 61) * 60_000);
    const idleMilliseconds = Date.now() - startedAt;
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
      startedAt,
      finishedAt: Date.now(),
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
