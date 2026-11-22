// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, test } from "./harness";

const A = "https://ks.invalid/__fixtures/events.html";
const B = "https://ks.invalid/__fixtures/keyboard.html";

test("the committed context is published before the Page.navigate reply", async ({
  page,
  cdpSession,
}) => {
  await page.goto(A, { waitUntil: "domcontentloaded" });
  await cdpSession.send("Page.enable");
  await cdpSession.send("Runtime.enable");

  const seen: string[] = [];
  cdpSession.on("Page.frameNavigated", ({ frame }) => {
    if (frame.url === B) seen.push("frameNavigated");
  });
  cdpSession.on("Runtime.executionContextCreated", ({ context }) => {
    if (context.auxData?.isDefault === true) seen.push("context");
  });

  await cdpSession.send("Page.navigate", { url: B });

  expect(seen).toContain("frameNavigated");
  expect(seen).toContain("context");
  expect(seen.indexOf("frameNavigated")).toBeLessThan(seen.indexOf("context"));
}, 120_000);

test("an evaluate issued after frameNavigated runs in the new document", async ({
  page,
  cdpSession,
}) => {
  await page.goto(A, { waitUntil: "domcontentloaded" });
  await cdpSession.send("Page.enable");

  const committed = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      cdpSession.off("Page.frameNavigated", onFrame);
      reject(new Error("Page.frameNavigated for the new document did not arrive"));
    }, 15_000);
    const onFrame = ({ frame }: { frame: { url: string } }) => {
      if (frame.url !== B) return;
      clearTimeout(timer);
      cdpSession.off("Page.frameNavigated", onFrame);
      resolve();
    };
    cdpSession.on("Page.frameNavigated", onFrame);
  });

  // Deliberately not awaited: the evaluate is sent while the navigation is between commit and
  // the publication of the replacement isolate.
  const navigation = cdpSession.send("Page.navigate", { url: B });
  navigation.catch(() => {});
  await committed;

  const evaluated = await cdpSession.send("Runtime.evaluate", {
    expression: "location.href",
    returnByValue: true,
  });
  expect(evaluated.result.value).toBe(B);

  const reply = (await navigation) as { errorText?: string };
  expect(reply.errorText).toBeUndefined();
}, 120_000);
