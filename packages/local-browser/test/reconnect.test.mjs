// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import puppeteer from "puppeteer-core";
import { ProfileStore } from "../src/profile-store.mjs";
import { startBrowser } from "../src/server.mjs";
import { connect as connectCdp, fixtureServer } from "./helpers.mjs";

test(
  "Puppeteer reconnect discovers the same live page repeatedly",
  { timeout: 120_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-reconnect-"));
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
    let client;
    let viewer;
    t.after(async () => {
      await client?.disconnect();
      await viewer?.disconnect();
      await browser.close();
      await fixture.close();
      rmSync(directory, { recursive: true, force: true });
    });
    const connect = () =>
      puppeteer.connect({
        browserURL: `http://127.0.0.1:${browser.port}`,
        defaultViewport: null,
        protocolTimeout: 15_000,
      });
    client = await connect();
    const page = await client.newPage();
    await page.goto(`${fixture.url}/reconnect`, { waitUntil: "domcontentloaded" });
    await page.evaluate(
      "window.reconnectMarker=917;document.getElementById('draft').value='retained'",
    );
    viewer = await connectCdp(browser.port, "?role=viewer");
    const { targetInfos } = await viewer.send("Target.getTargets");
    const target = targetInfos.find((target) => target.url === `${fixture.url}/reconnect`);
    const { sessionId: viewerSession } = await viewer.send("Target.attachToTarget", {
      targetId: target.targetId,
      flatten: true,
    });
    for (let attempt = 0; attempt < 3; attempt++) {
      await client.disconnect();
      client = await connect();
      const pages = await client.pages();
      const restored = pages.find((page) => page.url() === `${fixture.url}/reconnect`);
      assert.ok(restored, `missing page on reconnect ${attempt + 1}`);
      assert.deepEqual(
        await restored.evaluate("[window.reconnectMarker,document.getElementById('draft').value]"),
        [917, "retained"],
      );
      const image = await viewer.send("Page.captureScreenshot", { format: "png" }, viewerSession);
      assert.ok(Buffer.from(image.data, "base64").length > 1000);
    }
  },
);
