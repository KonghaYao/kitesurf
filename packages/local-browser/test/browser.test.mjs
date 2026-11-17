// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { PNG } from "pngjs";
import { ProfileStore } from "../src/profile-store.mjs";
import { startBrowser } from "../src/server.mjs";
import { connect, evaluate, fixtureServer, navigate, openPage, until } from "./helpers.mjs";

const engine = resolve(import.meta.dirname, "../../engine");
const dist = process.env.KITESURF_TEST_DIST || join(engine, "local_dist");
const assets = process.env.KITESURF_TEST_ASSETS || join(engine, "public");

test(
  "real workerd: login, rendering, live reconnect, origin/tab storage and cold restore",
  { timeout: 480_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-browser-test-"));
    const fixture = await fixtureServer();
    const other = await fixtureServer();
    let browser;
    let cdp;
    t.after(async () => {
      await cdp?.disconnect();
      await browser?.close();
      await fixture.close();
      await other.close();
      rmSync(directory, { recursive: true, force: true });
    });
    const path = join(directory, "account.sqlite");
    const start = async (create) => {
      const profile = new ProfileStore(path, { create });
      browser = await startBrowser({ profile, dist, assets, port: 0, allowPrivateNetwork: true });
      cdp = await connect(browser.port);
      return profile;
    };
    let profile = await start(true);
    const first = await browser.status();
    let page = await openPage(cdp, `${fixture.url}/account`);
    const read = (expression) => evaluate(cdp, page.sessionId, expression);
    const api = async (route, body) => {
      const response = await fetch(`http://127.0.0.1:${browser.port}${route}`, {
        method: "POST",
        body: body && JSON.stringify(body),
      });
      const result = await response.json();
      return { status: response.status, result };
    };

    await t.test("input and screenshot use the actual browser renderer", async () => {
      await read("document.getElementById('username').focus()");
      await cdp.send("Input.insertText", { text: "Alice" }, page.sessionId);
      assert.equal(await read("document.getElementById('username').value"), "Alice");
      const button = await read(
        "JSON.stringify(document.getElementById('login').getBoundingClientRect())",
      );
      const box = JSON.parse(button);
      const click = {
        x: box.x + box.width / 2,
        y: box.y + box.height / 2,
        button: "left",
        clickCount: 1,
      };
      await cdp.send(
        "Input.dispatchMouseEvent",
        { ...click, type: "mousePressed" },
        page.sessionId,
      );
      await cdp.send(
        "Input.dispatchMouseEvent",
        { ...click, type: "mouseReleased" },
        page.sessionId,
      );
      await until(() => read("window.loggedIn === true"), "login");
      const screenshot = await cdp.send(
        "Page.captureScreenshot",
        { format: "png" },
        page.sessionId,
      );
      const bytes = Buffer.from(screenshot.data, "base64");
      const png = PNG.sync.read(bytes);
      assert.ok(png.width >= 400 && png.height >= 300);
      let blue = 0;
      for (let i = 0; i < png.data.length; i += 4)
        if (png.data[i] < 80 && png.data[i + 2] > 150 && png.data[i + 3] === 255) blue++;
      assert.ok(blue > 5000, "header must be rendered, not an empty PNG");
      if (process.env.KITESURF_SCREENSHOT) writeFileSync(process.env.KITESURF_SCREENSHOT, bytes);
      assert.equal((await api("/flush")).status, 200);
      const cookies = profile.read().state.cookies;
      assert.equal(cookies.find((c) => c.name === "sid")?.httpOnly, true);
      assert.equal(cookies.find((c) => c.name === "displayName")?.value, "Alice");
    });

    await t.test("disconnect retains the same page, draft and sessionStorage", async () => {
      await read(
        "document.getElementById('draft').value='unfinished order'; window.liveMarker=731",
      );
      const expiredSession = page.sessionId;
      await cdp.disconnect();
      const status = await browser.status();
      assert.equal(status.sessionId, first.sessionId);
      assert.equal(status.generation, first.generation);
      cdp = await connect(browser.port);
      await assert.rejects(
        evaluate(cdp, expiredSession, "window.liveMarker=0"),
        /attachment expired/,
      );
      page.sessionId = (
        await cdp.send("Target.attachToTarget", { targetId: page.targetId, flatten: true })
      ).sessionId;
      assert.deepEqual(
        await read(
          "[window.liveMarker,document.getElementById('draft').value,sessionStorage.getItem('step')]",
        ),
        [731, "unfinished order", "checkout"],
      );
    });

    await t.test(
      "navigation restores storage before scripts, with origin and tab separation",
      async () => {
        await navigate(cdp, page.sessionId, `${fixture.url}/next`);
        assert.deepEqual(await read("[boot.token,boot.step,boot.authenticated]"), [
          "T1",
          "checkout",
          true,
        ]);
        assert.ok(!(await read("document.cookie")).includes("sid="));
        await cdp.send("Page.reload", {}, page.sessionId);
        await until(() => read("window.boot && boot.step === 'checkout'"), "reload");
        await navigate(cdp, page.sessionId, `${other.url}/other`);
        assert.deepEqual(await read("[boot.token,boot.step]"), [null, null]);
        await navigate(cdp, page.sessionId, `${fixture.url}/return`);
        assert.deepEqual(await read("[boot.token,boot.step]"), ["T1", "checkout"]);
        const second = await openPage(cdp, `${fixture.url}/second`);
        assert.deepEqual(await evaluate(cdp, second.sessionId, "[boot.token,boot.step]"), [
          "T1",
          null,
        ]);
        await Promise.all([
          read("localStorage.setItem('shared','first')"),
          evaluate(cdp, second.sessionId, "localStorage.setItem('shared','second')"),
        ]);
        assert.equal((await api("/flush")).status, 200);
        assert.equal(
          await read("localStorage.getItem('shared')"),
          await evaluate(cdp, second.sessionId, "localStorage.getItem('shared')"),
        );
        assert.ok((await evaluate(cdp, second.sessionId, "events.length")) > 0);
        const context = await cdp.send("Target.createBrowserContext");
        const privatePage = await openPage(cdp, `${fixture.url}/private`, context.browserContextId);
        assert.deepEqual(
          await evaluate(cdp, privatePage.sessionId, "[boot.token,boot.authenticated]"),
          [null, false],
        );
        await evaluate(cdp, privatePage.sessionId, "localStorage.setItem('private','secret')");
        await cdp.send("Target.disposeBrowserContext", context);
        assert.equal((await api("/flush")).status, 200);
        assert.ok(!JSON.stringify(profile.read().state.localStorage).includes("secret"));
      },
    );

    await t.test(
      "rotation and consistent backup survive a clean process-runtime restart",
      async () => {
        await read(
          "fetch('/rotate',{method:'POST'}).then(r=>r.json()).then(d=>localStorage.setItem('token',d.token))",
        );
        const backup = join(directory, "backup.sqlite");
        assert.equal((await api("/backup", { path: backup })).status, 200);
        const restored = new ProfileStore(backup);
        assert.equal(restored.read().state.cookies.find((c) => c.name === "sid").value, "T2");
        restored.close();
        const oldTarget = page.targetId;
        await cdp.send("Browser.close");
        await browser.done;
        browser = undefined;
        cdp = undefined;
        profile = await start(false);
        const status = await browser.status();
        assert.notEqual(status.sessionId, first.sessionId);
        assert.notEqual(status.generation, first.generation);
        assert.equal(status.recovery, "profile-restored");
        assert.ok(!status.tabs.some((tab) => tab.targetId === oldTarget));
        page = await openPage(cdp, `${fixture.url}/restored`);
        assert.deepEqual(
          await read("[boot.token,boot.step,boot.authenticated,typeof window.liveMarker]"),
          ["T2", null, true, "undefined"],
        );
      },
    );

    await t.test("takeover fences old commands and viewer access cannot mutate", async () => {
      assert.equal((await api("/control", { owner: "human", takeover: true })).status, 200);
      await assert.rejects(read("localStorage.setItem('stale','bad')"), /Control has changed/);
      const viewer = await connect(browser.port, "?role=viewer");
      await assert.rejects(
        viewer.send("Runtime.evaluate", { expression: "1" }, page.sessionId),
        /Viewer/,
      );
      const attached = await viewer.send("Target.attachToTarget", {
        targetId: page.targetId,
        flatten: true,
      });
      await viewer.send("Page.captureScreenshot", { format: "png" }, attached.sessionId);
      await viewer.disconnect();
      await cdp.disconnect();
      cdp = await connect(browser.port, "?owner=human");
      page.sessionId = (
        await cdp.send("Target.attachToTarget", { targetId: page.targetId, flatten: true })
      ).sessionId;
      assert.equal(await read("localStorage.getItem('stale')"), null);
    });

    await t.test("closing a tab harvests its latest cookie and storage writes", async () => {
      const closing = await openPage(cdp, `${fixture.url}/closing-tab`);
      await evaluate(
        cdp,
        closing.sessionId,
        "document.cookie='lastWrite=saved; Path=/; Max-Age=3600';localStorage.setItem('last-tab','saved')",
      );
      await cdp.send("Target.closeTarget", { targetId: closing.targetId });
      assert.equal(profile.read().state.cookies.find((c) => c.name === "lastWrite").value, "saved");
      assert.ok(JSON.stringify(profile.read().state.localStorage).includes("last-tab"));
      await read("document.cookie='lastWrite=; Path=/; Max-Age=0'");
    });

    await t.test("save failure refuses close and leaves the live page recoverable", async () => {
      const save = profile.save.bind(profile);
      profile.save = () => {
        throw new Error("Injected disk write failure");
      };
      await read("localStorage.setItem('after-failure','keep-me'); window.liveMarker=919");
      assert.equal((await api("/flush")).status, 409);
      await assert.rejects(cdp.send("Browser.close"), /Injected disk write failure/);
      assert.equal((await browser.status()).alive, true);
      assert.equal(await read("window.liveMarker"), 919);
      profile.save = save;
      assert.equal((await api("/flush")).status, 200);
    });

    await t.test(
      "a committed save with a lost reply can be retried before newer changes",
      async () => {
        const save = profile.save.bind(profile);
        profile.save = (...args) => {
          save(...args);
          throw new Error("Injected reply loss after commit");
        };
        await read("localStorage.setItem('reply-loss','first')");
        assert.equal((await api("/flush")).status, 409);
        profile.save = save;
        await read("localStorage.setItem('reply-loss','second')");
        assert.equal((await api("/flush")).status, 200);
        const stored = profile
          .read()
          .state.localStorage.find(([origin]) => origin === fixture.url)[1];
        assert.equal(new Map(stored).get("reply-loss"), "second");
      },
    );

    await t.test("a rejected oversized snapshot can be corrected without restarting", async () => {
      const save = profile.save.bind(profile);
      profile.save = () => {
        throw Object.assign(new Error("Storage quota exceeded"), { profileCode: "INVALID_STATE" });
      };
      await read("localStorage.setItem('rejected','oversized')");
      assert.equal((await api("/flush")).status, 409);
      profile.save = save;
      await read("localStorage.removeItem('rejected')");
      assert.equal((await api("/flush")).status, 200);
    });

    await t.test("logout does not resurrect an old credential after restart", async () => {
      await read(
        "fetch('/logout',{method:'POST'}).then(()=>{localStorage.clear();document.cookie='displayName=; Path=/; Max-Age=0';})",
      );
      await cdp.send("Browser.close");
      await browser.done;
      browser = undefined;
      cdp = undefined;
      profile = await start(false);
      page = await openPage(cdp, `${fixture.url}/signed-out`);
      assert.deepEqual(await read("[boot.token,boot.authenticated,document.cookie]"), [
        null,
        false,
        "",
      ]);
      assert.equal(profile.read().state.cookies.length, 0);
    });
  },
);
