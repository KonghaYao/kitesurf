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

test("an installed document keeps its frames and serves source to a late debugger", async ({
  page,
  cdpSession,
}) => {
  await page.goto(A, { waitUntil: "domcontentloaded" });
  const { frameTree } = await cdpSession.send("Page.getFrameTree");
  await cdpSession.send("Page.setDocumentContent", {
    frameId: frameTree.frame.id,
    html: `<!doctype html><iframe name="scannedChild" srcdoc="<p>child</p>"></iframe>
      <script>globalThis.scanSourceMarker = 4803;</script>`,
  });

  const installed = await cdpSession.send("Page.getFrameTree");
  expect(installed.frameTree.childFrames?.map(({ frame }) => frame.name)).toEqual(["scannedChild"]);
  const scriptIds: string[] = [];
  cdpSession.on("Debugger.scriptParsed", (script) => {
    if (script.url === "/__ks_user_classic_regular.js") scriptIds.push(script.scriptId);
  });
  await cdpSession.send("Debugger.enable");
  const [scriptId] = scriptIds;
  if (scriptId === undefined) throw new Error("Debugger did not announce the classic bundle");
  const source = await cdpSession.send("Debugger.getScriptSource", { scriptId });
  expect(source.scriptSource).toContain("globalThis.scanSourceMarker = 4803;");
  const evaluated = await cdpSession.send("Runtime.evaluate", {
    expression: "globalThis.scanSourceMarker",
    returnByValue: true,
  });
  expect(evaluated.result.value).toBe(4803);
}, 120_000);
