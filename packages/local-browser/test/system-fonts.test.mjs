// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Local system-font smoke tests. Assets here never contain the bundled font directory.

import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { chmodSync, cpSync, existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative, resolve, sep } from "node:path";
import { PNG } from "pngjs";
import { ProfileStore } from "../src/profile-store.mjs";
import { startBrowser } from "../src/server.mjs";
import { connect, until } from "./helpers.mjs";

const engine = resolve(import.meta.dirname, "../../engine");
const dist = process.env.KITESURF_TEST_DIST || join(engine, "local_dist");
const assets = process.env.KITESURF_TEST_ASSETS || join(engine, "public");
const notoSans = join(engine, "public/fonts/NotoSans.ttf");

function withoutFonts(source, destination) {
  cpSync(source, destination, {
    recursive: true,
    filter: (path) => {
      const pathRelative = relative(source, path);
      return pathRelative !== "fonts" && !pathRelative.startsWith(`fonts${sep}`);
    },
  });
  assert.equal(existsSync(join(destination, "fonts")), false);
}

async function fixture(html) {
  const server = createServer((request, response) => {
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    });
    response.end(html);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    close: () =>
      new Promise((done) => {
        server.close(done);
        server.closeAllConnections();
      }),
  };
}

function ink(png, rect) {
  const left = Math.max(0, Math.floor(rect.x));
  const top = Math.max(0, Math.floor(rect.y));
  const right = Math.min(png.width, Math.ceil(rect.x + rect.width));
  const bottom = Math.min(png.height, Math.ceil(rect.y + rect.height));
  let dark = 0;
  let colorful = 0;
  for (let y = top; y < bottom; y++) {
    for (let x = left; x < right; x++) {
      const index = (y * png.width + x) * 4;
      const r = png.data[index];
      const g = png.data[index + 1];
      const b = png.data[index + 2];
      if (r < 230 || g < 230 || b < 230) dark++;
      if (Math.max(r, g, b) - Math.min(r, g, b) > 48) colorful++;
    }
  }
  return { dark, colorful };
}

async function familyNames(cdp, sessionId) {
  const dump = await cdp.send("Kitesurf.getFontFaces", {}, sessionId);
  const entries = dump && "byTarget" in dump ? Object.values(dump.byTarget) : [dump];
  return entries.flatMap((entry) => {
    if (typeof entry?.json !== "string") return [];
    try {
      const faces = JSON.parse(entry.json);
      return Array.isArray(faces)
        ? faces.flatMap((face) => (typeof face?.family === "string" ? [face.family] : []))
        : [];
    } catch {
      return [];
    }
  });
}

async function diagnostics(cdp, targetId) {
  const state = await cdp.send("Kitesurf.getPageState", { targetId });
  return Array.isArray(state.fontDiagnostics) ? state.fontDiagnostics : [];
}

async function openPage(cdp, url) {
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Page.enable", {}, sessionId);
  await cdp.send("Runtime.enable", {}, sessionId);
  await cdp.send("Page.navigate", { url }, sessionId);
  await until(
    () =>
      cdp
        .send("Runtime.evaluate", { expression: "!!window.boot", returnByValue: true }, sessionId)
        .then((result) => result.result.value === true),
    "fixture navigation",
  );
  return { targetId, sessionId };
}

async function screenshot(cdp, sessionId) {
  const shot = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
  return PNG.sync.read(Buffer.from(shot.data, "base64"));
}

async function boxes(cdp, sessionId) {
  const raw = await cdp.send(
    "Runtime.evaluate",
    {
      expression:
        "JSON.stringify(Object.fromEntries([...document.querySelectorAll('[id]')].map(e => [e.id, e.getBoundingClientRect()])))",
      returnByValue: true,
    },
    sessionId,
  );
  return JSON.parse(raw.result.value);
}

test(
  "local browser renders with system fonts and no bundled font assets",
  { timeout: 480_000 },
  async (t) => {
    if (!["darwin", "linux"].includes(process.platform)) return t.skip("platform unsupported");
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-system-fonts-"));
    const localAssets = join(directory, "public");
    const server =
      await fixture(`<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><style>
      body { margin: 0; background: #fff; color: #111; }
      p { margin: 8px 0; font-size: 32px; }
      #cjk, #emoji { font-size: 44px; }
      </style><script>window.boot = true;</script></head><body>
      <p id="latin" style="font-family: sans-serif">Latin sans 123</p>
      <p id="serif" style="font-family: serif">Serif 123</p>
      <p id="mono" style="font-family: monospace">mono 123</p>
      <p id="cjk" style="font-family: sans-serif">中文 世界</p>
      <p id="emoji" style="font-family: sans-serif">😀🎉</p>
      <p id="script" style="font-family: sans-serif">مرحبا</p>
      <script>
      setTimeout(() => {
        const p = document.createElement('p');
        p.id = 'dynamic';
        p.style.fontFamily = 'sans-serif';
        p.textContent = 'שלום';
        document.body.append(p);
      }, 250);
      </script>
      </body></html>`);
    let browser;
    let cdp;
    t.after(async () => {
      await cdp?.disconnect();
      await browser?.close();
      await server.close();
      rmSync(directory, { recursive: true, force: true });
    });
    withoutFonts(assets, localAssets);
    const profile = new ProfileStore(join(directory, "profile.sqlite"), { create: true });
    browser = await startBrowser({ profile, dist, assets: localAssets, port: 0 });
    cdp = await connect(browser.port);
    const page = await openPage(cdp, `${server.url}/system-fonts`);
    const evaluate = (expression) =>
      cdp
        .send("Runtime.evaluate", { expression, returnByValue: true }, page.sessionId)
        .then((result) => result.result.value);
    await until(
      () =>
        diagnostics(cdp, page.targetId).then((lines) =>
          lines.some((line) => line.startsWith("system core")),
        ),
      "system core",
    );
    await until(
      () =>
        diagnostics(cdp, page.targetId).then(
          (lines) =>
            lines.some((line) => line.startsWith("system CJK")) ||
            lines.some((line) => line.includes("missing coverage: emoji")) ||
            lines.some((line) => line.includes("missing coverage: script=Hani")),
        ),
      "system CJK or emoji diagnostic",
    );
    const found = await diagnostics(cdp, page.targetId);
    const names = await familyNames(cdp, page.sessionId);
    assert.ok(names.length >= 3, `expected several system families, got ${names}`);

    const first = await screenshot(cdp, page.sessionId);
    const firstBoxes = await boxes(cdp, page.sessionId);
    for (const id of ["latin", "serif", "mono"]) {
      const pixels = ink(first, firstBoxes[id]);
      assert.ok(pixels.dark > 40, `${id} must render glyph ink, got ${JSON.stringify(pixels)}`);
    }
    const cjkLine = found.find((line) => line.startsWith("system CJK"));
    if (cjkLine) assert.ok(ink(first, firstBoxes.cjk).dark > 40, "CJK glyphs must render");
    const emojiLine = found.find((line) => line.startsWith("system emoji"));
    if (emojiLine && process.platform === "darwin")
      assert.ok(
        ink(first, firstBoxes.emoji).colorful > 20,
        "emoji must render with color, not only monochrome ink",
      );
    if (emojiLine && process.platform === "linux")
      t.diagnostic("Linux emoji bitmaps are BGRA; glifo 0.4 supports PNG bitmaps only");
    const arabicLine = found.find((line) => line.startsWith("system script=Arab"));
    if (arabicLine) assert.ok(ink(first, firstBoxes.script).dark > 20, "Arabic glyphs must render");

    await until(
      () =>
        evaluate(
          "!!document.getElementById('dynamic') && document.getElementById('dynamic').getBoundingClientRect().width > 20",
        ),
      "dynamic script text",
    );
    await until(
      () =>
        diagnostics(cdp, page.targetId).then(
          (lines) =>
            lines.some((line) => line.startsWith("system script=Hebr")) ||
            lines.some((line) => line.includes("missing coverage: script=Hebr")),
        ),
      "dynamic Hebrew demand",
    );
    const afterDynamic = await diagnostics(cdp, page.targetId);
    const dynamicBoxes = await boxes(cdp, page.sessionId);
    const hebrewLine = afterDynamic.find((line) => line.startsWith("system script=Hebr"));
    if (hebrewLine) {
      const dynamicShot = await screenshot(cdp, page.sessionId);
      assert.ok(
        ink(dynamicShot, dynamicBoxes.dynamic).dark > 10,
        "dynamically inserted script text must render after demand",
      );
    }

    const blank = await cdp.send("Target.createTarget", { url: "about:blank" });
    const blankSession = (
      await cdp.send("Target.attachToTarget", { targetId: blank.targetId, flatten: true })
    ).sessionId;
    await cdp.send("Runtime.enable", {}, blankSession);
    await cdp.send(
      "Runtime.evaluate",
      {
        expression:
          "document.body.style.cssText='margin:0;background:#fff;color:#111;font:32px sans-serif';document.body.textContent='about blank sans'",
        returnByValue: true,
      },
      blankSession,
    );
    await new Promise((resolve) => setTimeout(resolve, 500));
    const blankInk = ink(await screenshot(cdp, blankSession), {
      x: 0,
      y: 0,
      width: 1280,
      height: 200,
    });
    assert.ok(blankInk.dark > 100, "about:blank must render text after lazy font setup");

    await cdp.send("Kitesurf.disposeRenderer");
    await new Promise((resolve) => setTimeout(resolve, 300));
    const restarted = await screenshot(cdp, page.sessionId);
    assert.ok(
      ink(restarted, firstBoxes.latin).dark > 40,
      "renderer restart must re-request system font data",
    );
    if (emojiLine && process.platform === "darwin")
      assert.ok(
        ink(restarted, firstBoxes.emoji).colorful > 20,
        "renderer restart must preserve color emoji",
      );
  },
);

test(
  "local font helper diagnostics: missing script coverage and missing base font",
  { timeout: 240_000 },
  async (t) => {
    if (!["darwin", "linux"].includes(process.platform)) return t.skip("platform unsupported");
    const directory = mkdtempSync(join(tmpdir(), "kitesurf-font-stub-"));
    const localAssets = join(directory, "public");
    const stub = join(directory, "font-host-stub.cjs");
    withoutFonts(assets, localAssets);
    writeFileSync(
      stub,
      `#!/usr/bin/env node
const { readFileSync, readSync, writeSync } = require("node:fs");
const font = readFileSync(process.env.KS_TEST_FONT);
const sans = { id: "a".repeat(32), family: "Stub Sans", weight: 400, style: "normal", width: 1, covered: true, missing: 0, bytes: font.length };
const read = () => {
  const length = Buffer.alloc(4);
  if (readSync(0, length) !== 4) return null;
  const header = Buffer.alloc(length.readUInt32BE(0));
  readSync(0, header);
  const bodyLength = Buffer.alloc(4);
  readSync(0, bodyLength);
  return JSON.parse(header.toString("utf8"));
};
const write = (header, body = Buffer.alloc(0)) => {
  const h = Buffer.from(JSON.stringify(header));
  const out = Buffer.alloc(8 + h.length + body.length);
  out.writeUInt32BE(h.length, 0);
  h.copy(out, 4);
  out.writeUInt32BE(body.length, 4 + h.length);
  body.copy(out, 8 + h.length);
  writeSync(1, out);
};
for (;;) {
  const request = read();
  if (!request) break;
  if (request.op === "probe") {
    write({ ok: true, probe: { sans: process.env.KS_STUB_NO_SANS !== "1", serif: true, mono: true, system_ui: true, emoji: false, families: 1 } });
  } else if (request.op === "match" && request.family === "sans") {
    write({ ok: true, match: { faces: [sans], covered: true, missing: 0 } });
  } else if (request.op === "match") {
    write({ ok: true, match: { faces: [], covered: false, missing: 1, reason: "missing_coverage" } });
  } else if (request.op === "face") {
    write({ ok: true, id: request.id, bytes: font.length }, font);
  } else if (request.op === "stats") {
    write({ ok: true, stats: { faces: 1, cached_faces: 1, cached_bytes: font.length, matches: 1, served: 1, cache_hits: 1, cache_misses: 0, limited_emoji: 0, rejected: 0 } });
  } else {
    write({ ok: true });
  }
}
`,
    );
    chmodSync(stub, 0o755);
    const server = await fixture(`<!doctype html><html lang="zh-CN"><body>
      <script>window.boot = true;</script>
      <p style="font-family:sans-serif">中文 مرحبا 😀</p>
    </body></html>`);
    process.env.KS_TEST_FONT = notoSans;
    let browser;
    let cdp;
    t.after(async () => {
      await cdp?.disconnect();
      await browser?.close();
      await server.close();
      delete process.env.KS_TEST_FONT;
      delete process.env.KS_STUB_NO_SANS;
      rmSync(directory, { recursive: true, force: true });
    });

    browser = await startBrowser({
      profile: new ProfileStore(join(directory, "profile.sqlite"), { create: true }),
      dist,
      assets: localAssets,
      port: 0,
      fontHost: stub,
    });
    cdp = await connect(browser.port);
    const page = await openPage(cdp, `${server.url}/stub`);
    await until(
      () =>
        diagnostics(cdp, page.targetId).then(
          (lines) =>
            lines.some((line) => line.includes("missing coverage: script=Hani")) &&
            lines.some((line) => line.includes("missing coverage: script=Arab")) &&
            lines.some((line) => line.includes("missing coverage: emoji")),
        ),
      "missing coverage diagnostics",
    );
    const names = await familyNames(cdp, page.sessionId);
    assert.ok(names.includes("Stub Sans"), `stub sans face must register, got ${names}`);
    await cdp.send("Browser.close");
    await browser.done;
    cdp = undefined;
    browser = undefined;

    const noSansProfile = new ProfileStore(join(directory, "no-sans.sqlite"), { create: true });
    process.env.KS_STUB_NO_SANS = "1";
    await assert.rejects(
      startBrowser({
        profile: noSansProfile,
        dist,
        assets: localAssets,
        port: 0,
        fontHost: stub,
      }),
      /No usable system sans-serif font/,
    );
    noSansProfile.close();
  },
);
