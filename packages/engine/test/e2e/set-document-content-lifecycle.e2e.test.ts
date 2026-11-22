// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, test } from "./harness";

const A = "https://ks.invalid/__fixtures/events.html";

test("Page.setDocumentContent announces its frame commit", async ({ page, cdpSession }) => {
  await page.goto(A, { waitUntil: "domcontentloaded" });
  await cdpSession.send("Page.enable");
  await cdpSession.send("Runtime.enable");
  const { frameTree } = await cdpSession.send("Page.getFrameTree");
  const frameId = frameTree.frame.id;

  const events: string[] = [];
  cdpSession.on("Page.frameStartedLoading", ({ frameId: id }) => {
    if (id === frameId) events.push("frameStartedLoading");
  });
  cdpSession.on("Page.frameNavigated", ({ frame }) => {
    if (frame.id === frameId) events.push(`frameNavigated:${frame.url}`);
  });

  await cdpSession.send("Page.setDocumentContent", {
    frameId,
    html: "<!doctype html><title>Set</title><p id=s>set</p>",
  });
  // The engine emits both events while handling the command, before it builds the reply; this
  // second round-trip only confirms the commit happened.
  expect(await page.evaluate(() => document.title)).toBe("Set");
  expect(events).toEqual(["frameStartedLoading", `frameNavigated:${A}`]);
}, 120_000);
