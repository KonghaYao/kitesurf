// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, test } from "./harness";

const A = "https://ks.invalid/__fixtures/events.html";
const B = "https://ks.invalid/__fixtures/keyboard.html";

test("script navigation updates the page session history", async ({ page, cdpSession }) => {
  await page.goto(A, { waitUntil: "domcontentloaded" });
  expect(await page.evaluate(() => history.length)).toBe(1);

  // Chrome returns undefined and navigates after the script task; tolerate the engine's
  // stricter isolate retirement if the reply loses that race.
  await page.evaluate((to: string) => location.assign(to), B).catch(() => {});
  await page.waitForFunction(() => !!document.querySelector("#i"), { timeout: 15_000 });

  expect(await page.evaluate(() => ({ href: location.href, length: history.length }))).toEqual({
    href: B,
    length: 2,
  });
  const afterAssign = await cdpSession.send("Page.getNavigationHistory");
  expect(afterAssign.currentIndex).toBe(1);
  expect(afterAssign.entries.map((entry) => entry.url)).toEqual([A, B]);

  await page.evaluate(() => history.back()).catch(() => {});
  await page
    .waitForFunction(() => document.querySelector("#hdr")?.textContent === "events", {
      timeout: 15_000,
    })
    .catch(() => {});
  expect(await page.evaluate(() => ({ href: location.href, length: history.length }))).toEqual({
    href: A,
    length: 2,
  });
  const afterBack = await cdpSession.send("Page.getNavigationHistory");
  expect(afterBack.currentIndex).toBe(0);

  await page.evaluate(() => history.forward()).catch(() => {});
  await page.waitForFunction(() => !!document.querySelector("#i"), { timeout: 15_000 });
  expect(await page.evaluate(() => ({ href: location.href, length: history.length }))).toEqual({
    href: B,
    length: 2,
  });
  const afterForward = await cdpSession.send("Page.getNavigationHistory");
  expect(afterForward.currentIndex).toBe(1);
}, 120_000);
