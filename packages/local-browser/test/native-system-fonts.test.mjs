// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 原生宿主的系统字体链路：每个进程一个进程内字体服务，资源树没有打包字体，页面按
// target 注册系统字体，且作者 @font-face 仍从远端页面来源获取并绘制。

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { PNG } from "pngjs";

import { connect, navigate, until } from "./helpers.mjs";
import {
  eventsOfKind,
  readEvents,
  required,
  startNativeHost,
  waitFor,
} from "./host-adapter.mjs";

const nativeHost = required("KITESURF_NATIVE_HOST");
const workerd = required("KITESURF_WORKERD");
const configDir = required("KITESURF_CONFIG_DIR");
const engineAssets = required("KITESURF_ENGINE_ASSETS");
const assetsManifest = join(configDir, "assets-manifest.json");
const authorFont = join(import.meta.dirname, "fixtures/author-mono.woff2");

process.env.KITESURF_NATIVE_HOST = nativeHost;

const WORKER_CYCLE = `(async () => {
  const source = 'self.onmessage = (e) => postMessage({echo: e.data, isScope: self instanceof DedicatedWorkerGlobalScope});';
  const url = URL.createObjectURL(new Blob([source], { type: 'text/javascript' }));
  const worker = new Worker(url);
  const reply = await new Promise((resolve) => {
    worker.onmessage = (event) => resolve(event.data);
    worker.postMessage('ping');
  });
  worker.terminate();
  return reply;
})()`;

function events(host, kind) {
  return eventsOfKind(readEvents(host.observerPath), kind);
}

function filesUnder(root) {
  const found = [];
  const walk = (directory, prefix) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const name = `${prefix}${entry.name}`;
      if (entry.isDirectory()) walk(join(directory, entry.name), `${name}/`);
      else if (entry.isFile()) found.push(name);
    }
  };
  walk(root, "");
  return found.sort();
}

function processTable() {
  return execFileSync("ps", ["-axo", "pid=,ppid=,command="], { encoding: "utf8" })
    .split("\n")
    .map((line) => {
      const match = line.match(/^\s*(\d+)\s+(\d+)\s+(.*)$/);
      return match ? { pid: Number(match[1]), parent: Number(match[2]), command: match[3] } : null;
    })
    .filter(Boolean);
}

function descendants(pid) {
  const table = processTable();
  const found = [];
  const queue = [pid];
  while (queue.length > 0) {
    const parent = queue.shift();
    for (const entry of table.filter((candidate) => candidate.parent === parent)) {
      found.push(entry);
      queue.push(entry.pid);
    }
  }
  return found;
}

async function startHost(t, { sqlite = false, env } = {}) {
  const directory = mkdtempSync(join(tmpdir(), "ks-native-system-fonts-"));
  const profile = sqlite ? join(directory, "profile.sqlite") : undefined;
  const host = await startNativeHost({
    workerd,
    config: join(configDir, "local-private.capnp"),
    assets: engineAssets,
    assetsManifest,
    observe: join(directory, "observe.jsonl"),
    profile,
    createProfile: sqlite,
    env,
  });
  t.after(async () => {
    try {
      await host.stop("SIGTERM", 20_000);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  });
  host.stateDirectory = directory;
  return host;
}

async function open(cdp, url) {
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Page.enable", {}, sessionId);
  if (url) {
    await navigate(cdp, sessionId, url);
  }
  return { targetId, sessionId };
}

async function evaluateIn(cdp, sessionId, expression) {
  const result = await cdp.send(
    "Runtime.evaluate",
    { expression, returnByValue: true, awaitPromise: true },
    sessionId,
  );
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}

async function setContent(cdp, sessionId, html) {
  const tree = await cdp.send("Page.getFrameTree", {}, sessionId);
  await cdp.send("Page.setDocumentContent", { frameId: tree.frameTree.frame.id, html }, sessionId);
  await until(() => evaluateIn(cdp, sessionId, "document.body !== null"), "a body");
}

async function diagnostics(cdp, targetId) {
  const state = await cdp.send("Kitesurf.getPageState", { targetId });
  return Array.isArray(state.fontDiagnostics) ? state.fontDiagnostics : [];
}

async function hasDiagnostic(cdp, targetId, prefix) {
  return (await diagnostics(cdp, targetId)).some((line) => line.startsWith(prefix));
}

function faceFamilies(dump) {
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

async function screenshot(cdp, sessionId) {
  const shot = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
  return PNG.sync.read(Buffer.from(shot.data, "base64"));
}

function ink(png, rect) {
  const left = Math.max(0, Math.floor(rect.x));
  const top = Math.max(0, Math.floor(rect.y));
  const right = Math.min(png.width, Math.ceil(rect.x + rect.width));
  const bottom = Math.min(png.height, Math.ceil(rect.y + rect.height));
  let dark = 0;
  for (let y = top; y < bottom; y++) {
    for (let x = left; x < right; x++) {
      const index = (y * png.width + x) * 4;
      if (png.data[index] < 200 || png.data[index + 1] < 200 || png.data[index + 2] < 200) dark++;
    }
  }
  return dark;
}

async function rect(cdp, sessionId, id) {
  return evaluateIn(
    cdp,
    sessionId,
    `(() => { const r = document.getElementById(${JSON.stringify(id)}).getBoundingClientRect(); return { x: r.x, y: r.y, width: r.width, height: r.height }; })()`,
  );
}

function noNodePath(t) {
  const directory = mkdtempSync(join(tmpdir(), "ks-native-no-node-"));
  const bin = join(directory, "bin");
  const marker = join(directory, "runtime-tool-ran");
  mkdirSync(bin);
  for (const name of ["node", "npx", "miniflare", "wrangler", "font-host", "kitesurf-font-host"]) {
    writeFileSync(
      join(bin, name),
      `#!/bin/sh\necho "${name} $@" >> ${JSON.stringify(marker)}\nexit 97\n`,
      { mode: 0o755 },
    );
  }
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return { bin, marker };
}

test(
  "native starts in system-font mode with no bundled fonts and renders lazily",
  { timeout: 180_000 },
  async (t) => {
    const bundled = filesUnder(engineAssets).filter(
      (name) => name.startsWith("fonts/") || /\.(ttf|otf|woff2?)$/i.test(name),
    );
    assert.deepEqual(bundled, []);
    const trap = noNodePath(t);
    const host = await startHost(t, { sqlite: true, env: { PATH: trap.bin } });
    const runDir = host.runDir();
    assert.ok(runDir, "the host exposes its run directory");
    for (const name of ["fonts.sock", "profile.sock", "assets.sock"]) {
      assert.ok(existsSync(join(runDir, name)), `${name} exists while running`);
    }
    assert.equal(statSync(join(runDir, "fonts.sock")).mode & 0o777, 0o600);
    assert.equal((await host.request("/fonts/NotoSans.ttf")).status, 404);
    assert.equal(existsSync(trap.marker), false, "the host executed a runtime helper");
    const children = descendants(host.pid);
    assert.ok(children.length > 0, "the host owns a runtime child");
    for (const entry of children) {
      assert.match(entry.command, /workerd/, `unexpected runtime process: ${entry.command}`);
      assert.ok(
        !/(^|\/)(node|npx|miniflare|wrangler|font-host|kitesurf-font-host)(\s|$)/.test(
          entry.command,
        ),
        `unexpected runtime helper: ${entry.command}`,
      );
    }

    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const page = await open(cdp);
    await setContent(
      cdp,
      page.sessionId,
      "<style>html,body{margin:0;background:#fff;color:#111}p{font:36px sans-serif}</style>" +
        "<p id='text'>Hamburgefontsiv 123</p>",
    );
    await waitFor(
      () =>
        evaluateIn(
          cdp,
          page.sessionId,
          "document.getElementById('text').getBoundingClientRect().width > 100",
        ),
      "system sans layout",
    );
    const lines = await waitFor(
      () =>
        diagnostics(cdp, page.targetId).then((found) =>
          found.some((line) => /^system core sans=(?!\?$).+/.test(line)) ? found : null,
        ),
      "the system sans core",
    );
    assert.ok(!lines.some((line) => line.startsWith("system serif")));
    const box = await rect(cdp, page.sessionId, "text");
    assert.ok(ink(await screenshot(cdp, page.sessionId), box) > 100, "system glyph ink");
    const faces = faceFamilies(await cdp.send("Kitesurf.getFontFaces", {}, page.sessionId));
    assert.ok(faces.length >= 1, `registered system faces: ${faces}`);
    assert.deepEqual(
      events(host, "asset").filter((event) => event.path.startsWith("/fonts/")),
      [],
      "no bundled font asset was requested",
    );
    assert.ok(!/registered \d+ Latin UA fonts/.test(host.stderrText()));

    const status = await host.stop("SIGTERM", 20_000);
    assert.equal(status.code, 0);
    const shutdown = events(host, "shutdown").at(-1);
    assert.equal(shutdown.clean, true);
    assert.equal(shutdown.fonts, true);
    assert.equal(shutdown.dirRemoved, true);
    assert.ok(!existsSync(runDir));
    assert.ok(existsSync(join(host.stateDirectory, "profile.sqlite")));
    assert.deepEqual(
      readdirSync(host.stateDirectory).filter((name) => /font/i.test(name)),
      [],
      "font state is not persisted beside the profile",
    );
  },
);

function fixtureSite(t) {
  const server = createServer((request, response) => {
    const route = (request.url || "/").split("?")[0];
    const spec = {
      "/a": { family: "sans-serif", text: "Sans route A", marker: "a" },
      "/b": { family: "serif", text: "Serif route B", marker: "b" },
      "/c": { family: "monospace", text: "Mono route C", marker: "c" },
    }[route] ?? { family: "sans-serif", text: "Default route", marker: "d" };
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    });
    response.end(
      `<!doctype html><html><head><style>html,body{margin:0;background:#fff;color:#111}` +
        `#text{font:36px ${spec.family}}</style><script>window.boot=true;` +
        `window.__marker=${JSON.stringify(spec.marker)};</script></head>` +
        `<body><div><span id="text">${spec.text}</span></div></body></html>`,
    );
  });
  server.listen(0, "127.0.0.1");
  t.after(() => server.close());
  return once(server, "listening").then(() => {
    const address = server.address();
    return { url: `http://127.0.0.1:${address.port}` };
  });
}

async function faceFamiliesFor(cdp, targetId) {
  const dump = await cdp.send("Kitesurf.getFontFaces", { targetId });
  return faceFamilies(dump);
}

test(
  "each target keeps its own system font registration",
  { timeout: 180_000 },
  async (t) => {
    const host = await startHost(t);
    const site = await fixtureSite(t);
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const first = await open(cdp, `${site.url}/a`);
    const second = await open(cdp, `${site.url}/b`);
    await until(async () => hasDiagnostic(cdp, first.targetId, "system core"), "first core");
    await until(async () => hasDiagnostic(cdp, second.targetId, "system core"), "second core");
    const initialFirst = new Set(await faceFamiliesFor(cdp, first.targetId));
    const initialSecond = new Set(await faceFamiliesFor(cdp, second.targetId));
    const secondOnly = [...initialSecond].filter((family) => !initialFirst.has(family));
    assert.ok(
      secondOnly.length > 0,
      `the serif target's extra families=${JSON.stringify(secondOnly)}; first=${JSON.stringify([...initialFirst])}; second=${JSON.stringify([...initialSecond])}`,
    );
  },
);

test(
  "navigation and a renderer rebuild re-register only their own system fonts",
  { timeout: 180_000 },
  async (t) => {
    const host = await startHost(t);
    const site = await fixtureSite(t);
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const first = await open(cdp, `${site.url}/a`);
    await until(async () => hasDiagnostic(cdp, first.targetId, "system core"), "first core");
    const initialFirst = new Set(await faceFamiliesFor(cdp, first.targetId));

    await navigate(cdp, first.sessionId, `${site.url}/c`);
    await until(
      async () => {
        const current = await faceFamiliesFor(cdp, first.targetId);
        return current.some((family) => !initialFirst.has(family));
      },
      "the navigated page's mono family",
    );
    const navigatedFirst = new Set(await faceFamiliesFor(cdp, first.targetId));
    const navigatedOnly = [...navigatedFirst].filter((family) => !initialFirst.has(family));
    assert.ok(navigatedOnly.length > 0);
    assert.ok(
      [...initialFirst].every((family) => navigatedFirst.has(family)),
      "navigation re-registered the system core",
    );

    await cdp.send("Kitesurf.disposeRenderer");
    await new Promise((resolve) => setTimeout(resolve, 300));
    const box = await rect(cdp, first.sessionId, "text");
    assert.ok(ink(await screenshot(cdp, first.sessionId), box) > 50, "rebuilt renderer ink");

    assert.deepEqual(await evaluateIn(cdp, first.sessionId, WORKER_CYCLE), {
      echo: "ping",
      isScope: true,
    });
  },
);

test(
  "author @font-face fetches from its page origin while system fonts stay the base",
  { timeout: 180_000 },
  async (t) => {
    const host = await startHost(t);
    const fontBytes = readFileSync(authorFont);
    const seen = [];
    const server = createServer((request, response) => {
      const route = (request.url || "/").split("?")[0];
      seen.push(route);
      if (route === "/author.woff2") {
        response.writeHead(200, {
          "content-type": "font/woff2",
          "cache-control": "no-store",
          "content-length": String(fontBytes.length),
        });
        response.end(fontBytes);
        return;
      }
      response.writeHead(200, {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      });
      response.end(
        `<!doctype html><html><head><style>html,body{margin:0;background:#fff;color:#111}
        @font-face{font-family:AuthorMono;src:url(/author.woff2);}
        #author{font-family:AuthorMono;font-size:36px}
        #fallback{font-family:sans-serif;font-size:36px}</style>
        <script>window.boot=true;</script></head><body>
        <div><span id="author">Author 123</span></div>
        <div><span id="fallback">Author 123</span></div></body></html>`,
      );
    });
    server.listen(0, "127.0.0.1");
    t.after(() => server.close());
    await once(server, "listening");
    const url = `http://127.0.0.1:${server.address().port}/author`;
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
    const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
    await cdp.send("Page.enable", {}, sessionId);
    await navigate(cdp, sessionId, url);
    const page = { targetId, sessionId };
    const measure = () =>
      evaluateIn(
        cdp,
        page.sessionId,
        `(() => {
          const author = document.getElementById('author').getBoundingClientRect().width;
          const fallback = document.getElementById('fallback').getBoundingClientRect().width;
          return JSON.stringify({ author, fallback, status: document.fonts.status });
        })()`,
      ).then((text) => JSON.parse(text));
    await until(async () => {
      const widths = await measure();
      return widths.status === "loaded" &&
        widths.author > 0 &&
        widths.fallback > 0 &&
        widths.author !== widths.fallback
        ? widths
        : null;
    }, "the author font swapped into layout");
    assert.ok(seen.includes("/author.woff2"), "the author origin served the font");
    const box = await rect(cdp, page.sessionId, "author");
    assert.ok(ink(await screenshot(cdp, page.sessionId), box) > 100, "author glyph ink");
    assert.ok(await hasDiagnostic(cdp, page.targetId, "system core"));
    assert.equal((await host.request("/fonts/NotoSans.ttf")).status, 404);
  },
);
