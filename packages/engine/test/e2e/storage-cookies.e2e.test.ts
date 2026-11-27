// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, test } from "./harness";

const FIXTURE = "https://ks.invalid/__fixtures/cookies.html";

interface CdpCookie {
  name: string;
  value: string;
  domain: string;
}

test("Storage cookie round-trip: get, set, live visibility, clear", async ({
  page,
  cdpSession: cdp,
}) => {
  await page.goto(FIXTURE, { waitUntil: "domcontentloaded" });
  let res = (await cdp.send("Storage.getCookies")) as { cookies: CdpCookie[] };
  const server = res.cookies.find((c) => c.name === "server_cookie");
  expect(server?.value).toBe("hello");

  await cdp.send("Storage.setCookies", {
    cookies: [{ name: "ks_e2e", value: "wave3", domain: "ks.invalid", path: "/" }],
  });
  res = (await cdp.send("Storage.getCookies")) as { cookies: CdpCookie[] };
  expect(res.cookies.some((c) => c.name === "ks_e2e" && c.value === "wave3")).toBe(true);

  expect(await page.evaluate("document.cookie")).toContain("ks_e2e=wave3");

  await cdp.send("Storage.clearCookies");
  res = (await cdp.send("Storage.getCookies")) as { cookies: CdpCookie[] };
  expect(res.cookies.length).toBe(0);
  expect(await page.evaluate("document.cookie")).toBe("");
}, 120_000);

test("Network cookie writes update existing tabs according to document URL and HttpOnly", async ({
  browser,
  page,
  cdpSession,
}) => {
  const origin = "https://ks.invalid";
  const path = "/__fixtures/lifecycle.html";
  const url = origin + path;
  const peer = await browser.newPage();
  const otherPath = await browser.newPage();
  const otherHost = await browser.newPage();
  await Promise.all([
    page.goto(url),
    peer.goto(url + "?peer"),
    otherPath.goto(origin + "/__fixtures/big-doc.html?rows=1&seed=1"),
    otherHost.goto("https://ks-other.invalid" + path),
  ]);

  // agent-browser cookies set uses Network.setCookies and supplies a URL.
  await cdpSession.send("Network.setCookies", {
    cookies: [
      { name: "sample", value: "verified", url, path },
      { name: "private_sample", value: "secret", url, path, httpOnly: true },
    ],
  });
  const visible = "sample=verified";
  const jar = await cdpSession.send("Network.getCookies", { urls: [url] });
  expect(jar.cookies).toEqual(
    expect.arrayContaining([expect.objectContaining({ name: "private_sample", httpOnly: true })]),
  );
  expect(await page.evaluate("document.cookie")).toBe(visible);
  expect(await peer.evaluate("document.cookie")).toBe(visible);
  expect(await otherPath.evaluate("document.cookie")).not.toContain("sample=");
  expect(await otherHost.evaluate("document.cookie")).toBe("");

  await page.evaluate((cookiePath) => {
    document.cookie = "private_sample=spoof; Path=" + cookiePath;
  }, path);
  expect(await page.evaluate("document.cookie")).toBe(visible);
  expect(await peer.evaluate("document.cookie")).toBe(visible);
  expect((await cdpSession.send("Network.getCookies", { urls: [url] })).cookies).toEqual(
    expect.arrayContaining([
      expect.objectContaining({ name: "private_sample", value: "secret", httpOnly: true }),
    ]),
  );

  await cdpSession.send("Network.deleteCookies", { name: "sample", url, path });
  expect(await page.evaluate("document.cookie")).toBe("");
  expect(await peer.evaluate("document.cookie")).toBe("");
}, 120_000);

test("document.cookie writes are visible in another live tab without navigation", async ({
  browser,
  page,
}) => {
  const url = "https://ks.invalid/__fixtures/lifecycle.html";
  const peer = await browser.newPage();
  const context = await browser.createBrowserContext();
  const isolated = await context.newPage();
  await Promise.all([page.goto(url), peer.goto(url), isolated.goto(url)]);
  expect(await peer.evaluate(() => document.cookie)).toBe("");

  await page.evaluate(() => {
    document.cookie = "shared=page; Path=/";
  });
  expect(await peer.evaluate(() => document.cookie)).toBe("shared=page");
  expect(await isolated.evaluate(() => document.cookie)).toBe("");

  await peer.evaluate(() => {
    document.cookie = "shared=; Path=/; Max-Age=0";
  });
  expect(await page.evaluate(() => document.cookie)).toBe("");
  await context.close();
}, 120_000);

test("cookie writes after a navigation's startup reach its newly published document", async ({
  browser,
  page,
  cdpSession,
}) => {
  const url = "https://ks.invalid/during-cookie-navigation.html";
  const gate = "https://ks.invalid/cookie-navigation-gate";
  const peer = await browser.newPage();
  await peer.goto("https://ks.invalid/__fixtures/lifecycle.html");
  const peerCdp = await peer.createCDPSession();
  await cdpSession.send("Fetch.enable", {
    patterns: [{ urlPattern: url }, { urlPattern: gate }],
  });
  const pendingGate = new Promise<string>((resolve) => {
    cdpSession.on("Fetch.requestPaused", ({ request, requestId }) => {
      if (request.url === gate) {
        resolve(requestId);
      } else if (request.url === url) {
        const html = `<title>cookie startup</title><script type="module">
          globalThis.startupCookies = document.cookie;
          await fetch(${JSON.stringify(gate)});
        </script>`;
        void cdpSession.send("Fetch.fulfillRequest", {
          requestId,
          responseCode: 200,
          responseHeaders: [{ name: "Content-Type", value: "text/html" }],
          body: btoa(html),
        });
      }
    });
  });

  const navigation = page.goto(url, { waitUntil: "domcontentloaded" });
  const requestId = await pendingGate;
  try {
    expect(
      await peerCdp.send("Network.setCookie", { name: "concurrent", value: "updated", url }),
    ).toEqual({ success: true });
  } finally {
    await cdpSession.send("Fetch.fulfillRequest", { requestId, responseCode: 204 });
  }
  await navigation;
  expect(await page.evaluate("globalThis.startupCookies")).toBe("");
  expect(await page.evaluate(() => document.cookie)).toBe("concurrent=updated");
}, 120_000);

test("a remote cookie update preserves an unreported document.cookie write", async ({
  browser,
  page,
  cdpSession,
}) => {
  const url = "https://ks.invalid/__fixtures/lifecycle.html";
  const gate = "https://ks.invalid/pending-cookie-gate";
  const peer = await browser.newPage();
  await Promise.all([page.goto(url), peer.goto(url)]);
  const peerCdp = await peer.createCDPSession();
  await cdpSession.send("Fetch.enable", { patterns: [{ urlPattern: gate }] });
  const paused = new Promise<string>((resolve) => {
    cdpSession.once("Fetch.requestPaused", ({ requestId }) => resolve(requestId));
  });
  const written = page.evaluate(async (gateUrl) => {
    document.cookie = "pending=local; Path=/";
    await fetch(gateUrl);
    return document.cookie.split("; ").sort();
  }, gate);
  const requestId = await paused;
  try {
    expect(await peerCdp.send("Network.setCookie", { name: "remote", value: "cdp", url })).toEqual({
      success: true,
    });
  } finally {
    await cdpSession.send("Fetch.fulfillRequest", { requestId, responseCode: 204 });
  }
  expect(await written).toEqual(["pending=local", "remote=cdp"]);
  expect(await peer.evaluate(() => document.cookie.split("; ").sort())).toEqual([
    "pending=local",
    "remote=cdp",
  ]);
}, 120_000);

test("an HTTP response's cookies are visible to another already open tab", async ({
  browser,
  page,
}) => {
  const url = "https://ks.invalid/__fixtures/lifecycle.html";
  const peer = await browser.newPage();
  await Promise.all([page.goto(url), peer.goto(url)]);
  expect(await page.evaluate(() => document.cookie)).toBe("");
  expect(await peer.evaluate(async (fixture) => (await fetch(fixture)).status, FIXTURE)).toBe(200);
  expect(await page.evaluate(() => document.cookie)).toContain("server_cookie=hello");
}, 120_000);
