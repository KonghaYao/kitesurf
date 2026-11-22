// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, test } from "./harness";

const BASE = "https://ks.invalid/__fixtures";
const DOCUMENTS = [`${BASE}/lifecycle.html`, `${BASE}/events.html`, `${BASE}/keyboard.html`];
const ROUNDS = 6;
const COMMAND_DEADLINE_MS = 30_000;

type Outcome<T> =
  | { label: string; value: T; timedOut: false }
  | { label: string; error: unknown; timedOut: boolean };

// A superseded navigation or a command racing a commit may fail, but it must answer or fail:
// a promise that never settles loses the client's session.
async function settled<T>(label: string, promise: Promise<T>): Promise<Outcome<T>> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} did not settle`)), COMMAND_DEADLINE_MS);
  });
  try {
    return { label, value: await Promise.race([promise, deadline]), timedOut: false };
  } catch (error) {
    return {
      label,
      error,
      timedOut: (error as Error).message === `${label} did not settle`,
    };
  } finally {
    clearTimeout(timer);
  }
}

test("repeated navigation answers every command and keeps the session", async ({
  page,
  cdpSession,
}) => {
  await page.goto(DOCUMENTS[0]!, { waitUntil: "domcontentloaded" });
  await cdpSession.send("Page.enable");
  await cdpSession.send("Runtime.enable");

  const outcomes: Array<Outcome<unknown>> = [];
  let lastNavigate: Promise<unknown> = Promise.resolve();

  for (let round = 0; round < ROUNDS; round++) {
    const url = DOCUMENTS[(round + 1) % DOCUMENTS.length]!;
    // Deliberately not awaited: the evaluate and the history read are issued while the
    // navigation is still running, which is the window the report says loses replies.
    const navigating = cdpSession.send("Page.navigate", { url });
    navigating.catch(() => {});
    lastNavigate = navigating;
    outcomes.push(
      await settled(`navigate#${round}`, navigating),
      await settled(
        `evaluate#${round}`,
        cdpSession.send("Runtime.evaluate", { expression: "location.href", returnByValue: true }),
      ),
      await settled(`history#${round}`, cdpSession.send("Page.getNavigationHistory")),
    );
  }

  for (const outcome of outcomes) {
    if (outcome.timedOut) expect.unreachable(`${outcome.label} never answered`);
  }

  const finalNavigate = await settled("final navigate", lastNavigate);
  if ("error" in finalNavigate) throw finalNavigate.error;
  expect(typeof (finalNavigate.value as { frameId?: string }).frameId).toBe("string");

  const evaluated = await cdpSession.send("Runtime.evaluate", {
    expression: "location.href",
    returnByValue: true,
  });
  expect(evaluated.result.value).toBe(DOCUMENTS[ROUNDS % DOCUMENTS.length]);

  const history = (await cdpSession.send("Page.getNavigationHistory")) as {
    currentIndex: number;
    entries: Array<{ url: string }>;
  };
  expect(history.entries[history.currentIndex]!.url).toBe(DOCUMENTS[ROUNDS % DOCUMENTS.length]);
}, 120_000);
