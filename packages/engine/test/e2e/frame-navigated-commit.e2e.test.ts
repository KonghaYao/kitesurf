// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import type { CDPSession } from "@cloudflare/puppeteer";

import { expect, test } from "./harness";

const BASE = "https://ks.invalid/__fixtures";
const DATA_URL = "data:text/html,%3Ctitle%3ESecond%3C/title%3E%3Cp%3Esecond%3C/p%3E";
// The report measured 6-15s between the commit and Page.frameNavigated. A commit-time emit is
// three orders of magnitude faster, so this budget leaves room for a slow CI host and still
// fails the regression.
const COMMIT_BUDGET_MS = 3_000;
const LIFECYCLE_TIMEOUT_MS = 15_000;

interface Timing {
  frames: number[];
  lifecycle: { at: number; seq: number }[];
  commitSeq: number;
}

async function measureScriptNavigation(cdpSession: CDPSession, trigger: string): Promise<Timing> {
  const start = Date.now();
  const timing: Timing = { frames: [], lifecycle: [], commitSeq: -1 };
  let seq = 0;
  const onFrame = ({ frame }: { frame: { url: string } }) => {
    if (frame.url !== DATA_URL) return;
    timing.frames.push(Date.now() - start);
    // Two same-millisecond events still have a defined order in the protocol; the sequence
    // number records it, and the timestamps only feed the budget.
    if (timing.commitSeq < 0) timing.commitSeq = ++seq;
  };
  const onLifecycle = () => timing.lifecycle.push({ at: Date.now() - start, seq: ++seq });
  cdpSession.on("Page.frameNavigated", onFrame);
  cdpSession.on("Page.domContentEventFired", onLifecycle);
  cdpSession.on("Page.loadEventFired", onLifecycle);
  try {
    // A script navigation retires the context that issued it, so the reply may be an error.
    await cdpSession.send("Runtime.evaluate", { expression: trigger }).catch(() => undefined);
    await expect
      .poll(() => timing.frames.length, { timeout: LIFECYCLE_TIMEOUT_MS })
      .toBeGreaterThan(0);
    // The commit is only the start of the new document's lifecycle; wait for its own events
    // before comparing the order. Stopping at frameNavigated would drop them.
    await expect
      .poll(() => timing.lifecycle.filter(({ at }) => at >= timing.frames[0]!).length, {
        timeout: LIFECYCLE_TIMEOUT_MS,
      })
      .toBeGreaterThan(0);
  } finally {
    cdpSession.off("Page.frameNavigated", onFrame);
    cdpSession.off("Page.domContentEventFired", onLifecycle);
    cdpSession.off("Page.loadEventFired", onLifecycle);
  }
  return timing;
}

test("a script navigation commits its frame event without trailing the document", async ({
  page,
  cdpSession,
}) => {
  await page.goto(`${BASE}/lifecycle.html`, { waitUntil: "load" });
  await cdpSession.send("Page.enable");

  const clicked = await measureScriptNavigation(
    cdpSession,
    `(() => {
      const a = document.createElement("a");
      a.href = ${JSON.stringify(DATA_URL)};
      a.textContent = "go";
      document.body.append(a);
      a.click();
      return "clicked";
    })()`,
  );
  const title = await cdpSession.send("Runtime.evaluate", {
    expression: "document.title",
    returnByValue: true,
  });
  expect(title.result.value).toBe("Second");

  await page.goto(`${BASE}/lifecycle.html`, { waitUntil: "load" });
  const assigned = await measureScriptNavigation(
    cdpSession,
    `location.assign(${JSON.stringify(DATA_URL)}); "assigned"`,
  );

  for (const timing of [clicked, assigned]) {
    const committedAt = timing.frames[0]!;
    expect(committedAt).toBeLessThanOrEqual(COMMIT_BUDGET_MS);
    // The commit precedes the new document's own lifecycle events.
    const afterCommit = timing.lifecycle.filter(({ at }) => at >= committedAt);
    expect(afterCommit.length).toBeGreaterThan(0);
    expect(timing.commitSeq).toBeLessThan(afterCommit[0]!.seq);
  }
}, 120_000);
