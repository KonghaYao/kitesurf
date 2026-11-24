// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 针对真实固定版本 workerd 和引擎 bundle 的 T2 行为：target 隔离、导航失效、像素、worker、
// profile 桥、渲染资源、GC 回收、传输和关闭。每条断言都读取引擎响应、截图或宿主观察记录。

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { once } from "node:events";
import {
  copyFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { createServer, request as httpRequest } from "node:http";
import { createServer as createTcpServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { test } from "node:test";
import { PNG } from "pngjs";

import { connect, evaluate, navigate } from "./helpers.mjs";
import {
  eventsOfKind,
  readEvents,
  required,
  startNativeHost,
  waitFor,
  withTimeout,
} from "./host-adapter.mjs";

const nativeHost = required("KITESURF_NATIVE_HOST");
const workerd = required("KITESURF_WORKERD");
const configDir = required("KITESURF_CONFIG_DIR");
const engineAssets = required("KITESURF_ENGINE_ASSETS");
const assetsManifest = join(configDir, "assets-manifest.json");

process.env.KITESURF_NATIVE_HOST = nativeHost;

function config(name) {
  return resolve(configDir, name);
}

async function hostFor(
  t,
  { capnp = "local-private.capnp", observe = true, profileOrigin, env } = {},
) {
  const directory = mkdtempSync(join(tmpdir(), "ks-smoke-"));
  const observerPath = observe ? join(directory, "observe.jsonl") : undefined;
  const host = await startNativeHost({
    workerd,
    config: config(capnp),
    assets: engineAssets,
    assetsManifest,
    observe: observerPath,
    profileOrigin,
    env,
  });
  t.after(async () => {
    try {
      await host.stop("SIGTERM", 20_000);
    } catch {}
    rmSync(directory, { recursive: true, force: true });
  });
  return host;
}

function events(host, kind) {
  return eventsOfKind(readEvents(host.observerPath), kind);
}

async function open(cdp, url) {
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Page.enable", {}, sessionId);
  if (url) await navigate(cdp, sessionId, url);
  return { targetId, sessionId };
}

async function setContent(cdp, sessionId, html) {
  const tree = await cdp.send("Page.getFrameTree", {}, sessionId);
  await cdp.send("Page.setDocumentContent", { frameId: tree.frameTree.frame.id, html }, sessionId);
  await waitFor(() => evaluate(cdp, sessionId, "document.body !== null"), "a body");
}

async function evaluateIn(cdp, sessionId, expression, contextId) {
  const result = await cdp.send(
    "Runtime.evaluate",
    { expression, returnByValue: true, contextId },
    sessionId,
  );
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}

function fixture(t) {
  const server = createServer((request, response) => {
    const route = (request.url || "/").split("?")[0];
    const letter = route.replace("/", "") || "a";
    if (route === "/favicon.ico") {
      response.writeHead(204);
      response.end();
      return;
    }
    response.writeHead(200, { "content-type": "text/html; charset=utf-8" });
    response.end(
      `<!doctype html><html><head><title>${letter}</title></head><body style="margin:0">` +
        `<h1 id="route">${letter}</h1>` +
        `<script>window.boot=true;window.__marker=${JSON.stringify(letter)};</script>` +
        `</body></html>`,
    );
  });
  server.listen(0, "127.0.0.1");
  t.after(() => server.close());
  return once(server, "listening").then(() => {
    const address = server.address();
    return { url: `http://127.0.0.1:${address.port}` };
  });
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function processCommand(pid) {
  try {
    return execFileSync("ps", ["-p", String(pid), "-o", "command="], { encoding: "utf8" }).trim();
  } catch {
    return "";
  }
}

function unixRequest(socketPath, { method = "GET", path = "/", body, headers = {} } = {}) {
  return new Promise((resolve, reject) => {
    const request = httpRequest({ socketPath, method, path, headers }, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () =>
        resolve({
          status: response.statusCode,
          headers: response.headers,
          body: Buffer.concat(chunks).toString("utf8"),
        }),
      );
    });
    request.on("error", reject);
    if (body !== undefined) request.write(JSON.stringify(body));
    request.end();
  });
}

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

test(
  "two page targets keep their own globals and DOM over one upstream",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t);
    const site = await fixture(t);
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const first = await open(cdp, `${site.url}/a`);
    const second = await open(cdp, `${site.url}/b`);
    assert.equal(await evaluate(cdp, first.sessionId, "window.__marker"), "a");
    assert.equal(await evaluate(cdp, second.sessionId, "window.__marker"), "b");
    assert.equal(
      await evaluate(cdp, first.sessionId, "document.getElementById('route').textContent"),
      "a",
    );
    assert.equal(
      await evaluate(cdp, second.sessionId, "document.getElementById('route').textContent"),
      "b",
    );
    await evaluate(cdp, second.sessionId, "window.__onlySecond = 7");
    assert.equal(await evaluate(cdp, first.sessionId, "typeof window.__onlySecond"), "undefined");
    const targets = await cdp.send("Target.getTargets");
    assert.ok(targets.targetInfos.filter((info) => info.type === "page").length >= 2);
    const opens = events(host, "upstream-open");
    assert.equal(opens.length, 1);
    assert.equal(opens[0].connections, 1);
  },
);

test("navigation destroys the old context and its globals", { timeout: 120_000 }, async (t) => {
  const host = await hostFor(t);
  const site = await fixture(t);
  const cdp = await connect(host.port);
  t.after(() => cdp.disconnect());
  const page = await open(cdp, `${site.url}/a`);
  const seen = [];
  cdp.ws.on("message", (data) => {
    const message = JSON.parse(data.toString());
    if (message.method) seen.push(message);
  });
  await cdp.send("Runtime.enable", {}, page.sessionId);
  const tree = await cdp.send("Page.getFrameTree", {}, page.sessionId);
  const frameId = tree.frameTree.frame.id;
  // 默认 context id 恒为 1 且能跨导航存活，因此隔离 world 的 context id 可供后续 evaluate
  // 证明已失效。
  const { executionContextId } = await cdp.send(
    "Page.createIsolatedWorld",
    { frameId, worldName: "smoke" },
    page.sessionId,
  );
  assert.equal(
    await evaluateIn(cdp, page.sessionId, "location.pathname", executionContextId),
    "/a",
  );
  await evaluate(cdp, page.sessionId, "window.__oldMarker = 1");
  await navigate(cdp, page.sessionId, `${site.url}/b`);
  assert.equal(await evaluate(cdp, page.sessionId, "window.__marker"), "b");
  assert.equal(await evaluate(cdp, page.sessionId, "typeof window.__oldMarker"), "undefined");
  const stale = await cdp
    .send("Runtime.evaluate", { expression: "1", contextId: executionContextId }, page.sessionId)
    .then(
      () => null,
      (error) => error.message,
    );
  assert.match(String(stale), /context|destroyed/i, "the old context no longer evaluates");
  assert.ok(
    seen.some((event) => event.method === "Runtime.executionContextsCleared"),
    "the navigation cleared the old contexts",
  );
  assert.ok(events(host, "engine-listen").length >= 1);
});

test(
  "the screenshot has the real viewport, image pixels and color blocks",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t);
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const page = await open(cdp);
    await setContent(
      cdp,
      page.sessionId,
      "<style>html,body{margin:0;background:#2255ff}div{position:absolute;left:300px;top:0;width:120px;height:80px;background:#ff0000}img{position:absolute;left:0;top:0;width:200px;height:160px}</style>" +
        "<img id='pixel' src='http://kitesurf-fonts/fixtures/pixel.png'><div></div>",
    );
    const width = await waitFor(
      () =>
        evaluate(
          cdp,
          page.sessionId,
          "document.querySelector('div').getBoundingClientRect().width",
        ),
      "the red box layout",
    );
    assert.equal(width, 120);
    assert.equal(
      await waitFor(
        () =>
          evaluate(
            cdp,
            page.sessionId,
            "document.getElementById('pixel').complete && document.getElementById('pixel').naturalWidth",
          ),
        "the fixture image",
      ),
      8,
    );
    const asset = await waitFor(
      () => events(host, "asset").find((event) => event.path === "/fixtures/pixel.png"),
      "the fixture image asset",
    );
    assert.equal(asset.status, 200);
    assert.equal(asset.bytes, statSync(join(engineAssets, "fixtures/pixel.png")).size);
    const shot = await cdp.send("Page.captureScreenshot", { format: "png" }, page.sessionId);
    const bytes = Buffer.from(shot.data, "base64");
    assert.equal(bytes.subarray(0, 8).toString("hex"), "89504e470d0a1a0a");
    const png = PNG.sync.read(bytes);
    assert.equal(png.width, 1280);
    assert.equal(png.height, 720);
    const pixel = (x, y) => {
      const at = (y * png.width + x) * 4;
      return [png.data[at], png.data[at + 1], png.data[at + 2]];
    };
    assert.deepEqual(pixel(50, 10), [255, 0, 0], "the image's red half");
    assert.deepEqual(pixel(150, 10), [0, 255, 0], "the image's green half");
    assert.deepEqual(pixel(310, 10), [255, 0, 0], "the CSS color block");
    assert.deepEqual(pixel(600, 300), [34, 85, 255], "the page background");
    let red = 0;
    let green = 0;
    let blue = 0;
    for (let index = 0; index < png.data.length; index += 4) {
      const [r, g, b] = [png.data[index], png.data[index + 1], png.data[index + 2]];
      if (r === 255 && g === 0 && b === 0) red++;
      if (r === 0 && g === 255 && b === 0) green++;
      if (r === 34 && g === 85 && b === 255) blue++;
    }
    // 图像缩放会在线条边缘平滑采样，因此只要求大块实色区域存在。
    assert.ok(red >= 20_000, `the red CSS block and the image's red half: ${red}`);
    assert.ok(green >= 10_000, `the image's green half: ${green}`);
    assert.ok(blue > 500_000, `background pixels ${blue}`);
  },
);

test("a classic dedicated worker answers and terminates", { timeout: 120_000 }, async (t) => {
  const host = await hostFor(t);
  const cdp = await connect(host.port);
  t.after(() => cdp.disconnect());
  const page = await open(cdp);
  await setContent(cdp, page.sessionId, "<p>workers</p>");
  const reply = await evaluate(cdp, page.sessionId, WORKER_CYCLE);
  assert.deepEqual(reply, { echo: "ping", isScope: true });
});

// 默认契约宿主显式使用内存 profile 测试替身，只验证引擎与 binding 的加载与计数；
// 正式 SQLite 的内容和磁盘证据由 profile_engine_smoke_test 单独覆盖。
test(
  "the profile bridge loads the seed state and records redacted save operations",
  { timeout: 120_000 },
  async (t) => {
    const site = await fixture(t);
    const host = await hostFor(t, { profileOrigin: site.url });
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const page = await open(cdp, `${site.url}/a`);
    const loaded = await waitFor(
      () => events(host, "profile").find((event) => event.operation === "load"),
      "the profile load",
    );
    assert.equal(loaded.revision, 0);
    assert.equal(loaded.operation, "load");
    assert.equal(loaded.status, 200);
    assert.deepEqual(Object.keys(loaded).sort(), ["kind", "operation", "revision", "status"]);
    assert.equal(
      await waitFor(
        () => evaluate(cdp, page.sessionId, "localStorage.getItem('seed')"),
        "the seeded localStorage value",
      ),
      "initial",
    );
    assert.match(await evaluate(cdp, page.sessionId, "document.cookie"), /seed=initial/);
    assert.ok(!(await evaluate(cdp, page.sessionId, "document.cookie")).includes("sid="));
    const saved = await evaluate(
      cdp,
      page.sessionId,
      "(() => { document.cookie = 'saved=1; path=/'; localStorage.setItem('k', 'v'); return document.cookie + '|' + localStorage.getItem('k'); })()",
    );
    assert.match(saved, /saved=1/);
    assert.match(saved, /\|v$/);
    const flushed = await host.request("/flush", { method: "POST" });
    assert.equal(flushed.status, 200);
    assert.ok(flushed.body.saves >= 1);
    const save = await waitFor(
      () =>
        events(host, "profile").find((event) => event.operation === "save" && event.status === 200),
      "the profile save",
    );
    assert.equal(save.operation, "save");
    assert.equal(typeof save.revision, "number");
    assert.deepEqual(Object.keys(save).sort(), ["kind", "operation", "revision", "status"]);
    const session = await host.request("/session");
    assert.ok(session.body.profile.loads >= 1);
    assert.ok(session.body.profile.saves >= 1);
  },
);

function gcFixtureConfig(t) {
  const directory = mkdtempSync(join(tmpdir(), "ks-native-gc-"));
  const modules = join(directory, "modules");
  mkdirSync(modules);
  for (const file of readdirSync(join(configDir, "modules"))) {
    if (file === "__ks_gc_wrapper.js") continue;
    copyFileSync(join(configDir, "modules", file), join(modules, file));
  }
  for (const file of ["local-gc.capnp", "modules-gc.capnp"]) {
    copyFileSync(config(file), join(directory, file));
  }
  // Log every real-clock gc call; retirements and sweeps share this one path. A call without the
  // global receiver throws, which the engine's runGc must swallow.
  writeFileSync(
    join(modules, "__ks_gc_wrapper.js"),
    `import worker from "./index.js";
export * from "./index.js";
const get = Reflect.get;
const now = Date.now;
Reflect.get = function (target, key, ...args) {
  const gc = get(target, key, ...args);
  if (target !== globalThis || key !== "gc" || typeof gc !== "function") return gc;
  return function (...args) {
    if (this !== globalThis) throw new Error("gc lost its global receiver");
    const start = now();
    const result = Reflect.apply(gc, this, args);
    console.log("[ks-gc] " + JSON.stringify({ at: start, done: start + Math.max(0, now() - start) }));
    return result;
  };
};
export default worker;
`,
  );
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  return join(directory, "local-gc.capnp");
}

test(
  "retirements collect at most once a second and trailing sweeps continue",
  { timeout: 180_000 },
  async (t) => {
    const host = await hostFor(t, { capnp: gcFixtureConfig(t) });
    const site = await fixture(t);
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const page = await open(cdp, `${site.url}/a`);
    assert.equal(await evaluate(cdp, page.sessionId, "typeof globalThis.gc"), "undefined");
    const collected = () => events(host, "gc");
    assert.equal(collected().length, 0);

    // Two closes in one throttle window: only the first retirement collects. The next two
    // collections are the 150 ms and 350 ms sweeps, never the second retirement.
    const targets = [];
    for (let index = 0; index < 2; index++) {
      const target = await open(cdp);
      await setContent(cdp, target.sessionId, `<p>${index}</p>`);
      targets.push(target.targetId);
    }
    assert.equal(collected().length, 0, "opening targets does not retire an isolate");
    const before = await cdp.send("Kitesurf.getMemoryStats");
    const closeStarted = Date.now();
    const closes = await Promise.all(
      targets.map((targetId) => cdp.send("Target.closeTarget", { targetId })),
    );
    for (const closed of closes) assert.deepEqual(closed, { success: true });
    const after = await cdp.send("Kitesurf.getMemoryStats");
    assert.equal(after.isolatesKilled - before.isolatesKilled, targets.length);

    await waitFor(() => collected().length >= 3, "one retirement and two sweeps", 5_000);
    await delay(300);
    const burst = collected();
    assert.equal(burst.length, 3, "only one retirement collects inside the throttle window");
    assert.ok(burst[0].at >= closeStarted, "the close starts the first collection");
    assert.ok(burst[1].at - burst[0].at >= 100, `first sweep gap ${burst[1].at - burst[0].at}`);
    assert.ok(burst[2].at - burst[0].at >= 250, `second sweep gap ${burst[2].at - burst[0].at}`);
    for (const event of burst) assert.ok(event.done >= event.at);

    // The 5 s and 15.2 s tails finish; no retirement or navigation runs in this window.
    await delay(16_000);
    const quiet = collected().length;
    await delay(500);
    assert.equal(collected().length, quiet, "trailing sweeps stop after the last pass");

    // A navigation after the window collects again and schedules its own first two sweeps.
    const lastAt = collected().at(-1).at;
    await delay(Math.max(0, lastAt + 1_100 - Date.now()));
    const beforeNavigation = collected().length;
    await navigate(cdp, page.sessionId, `${site.url}/b`);
    const retirement = await waitFor(
      () => collected()[beforeNavigation],
      "a navigation retirement collection",
    );
    assert.ok(retirement.at - lastAt >= 1_000, `navigation gap ${retirement.at - lastAt}`);
    await waitFor(
      () => collected().length >= beforeNavigation + 3,
      "trailing sweeps after a navigation",
      5_000,
    );
    await delay(300);
    const followed = collected();
    assert.equal(followed.length, beforeNavigation + 3, "navigation kept the early sweep count");
    assert.ok(followed[beforeNavigation + 1].at - retirement.at >= 100);
    for (const event of followed.slice(beforeNavigation)) assert.ok(event.done >= event.at);

    const stats = await cdp.send("Kitesurf.getMemoryStats");
    assert.ok(stats.isolatesKilled >= 3, "retirements come from the closed targets and navigation");
  },
);

test(
  "a normal config launches without the gc wrapper and still retires isolates",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t, { capnp: "local-public.capnp" });
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const page = await open(cdp);
    await setContent(cdp, page.sessionId, "<p>plain</p>");
    assert.deepEqual(await evaluate(cdp, page.sessionId, WORKER_CYCLE), {
      echo: "ping",
      isScope: true,
    });
    await delay(1_500);
    assert.equal(events(host, "gc").length, 0);
    assert.ok(!host.stderrText().includes("[ks-gc]"));
    const stats = await cdp.send("Kitesurf.getMemoryStats", {}, page.sessionId);
    assert.ok(stats.isolatesKilled >= 1, `isolatesKilled ${stats.isolatesKilled}`);
  },
);

test(
  "the transport is one UDS chain with port zero and a control fd",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t);
    assert.notEqual(host.port, 0);
    const workerdPid = events(host, "workerd-start")[0].pid;
    const command = processCommand(workerdPid);
    assert.match(command, /--experimental \S+local-(?:public|private)\.capnp/);
    assert.match(command, /--socket-addr engine=unix:(\S+)\/engine\.sock/);
    assert.match(command, /--external-addr profile=unix:\S+\/profile\.sock/);
    assert.match(command, /--external-addr assets=unix:\S+\/assets\.sock/);
    assert.match(command, /--external-addr fonts=unix:\S+\/fonts\.sock/);
    assert.match(command, /--control-fd 3/);
    assert.doesNotMatch(command, /--socket-fd/);
    const socket = command.match(/--socket-addr engine=unix:(\S+)/)[1];
    assert.ok(statSync(socket).isSocket());
    assert.ok(statSync(join(host.runDir(), "profile.sock")).isSocket());
    assert.ok(statSync(join(host.runDir(), "assets.sock")).isSocket());
    assert.ok(statSync(join(host.runDir(), "fonts.sock")).isSocket());
    assert.ok(socket.length < 104, `the Darwin socket path fits: ${socket.length}`);
    const runDir = events(host, "run-dir").at(-1);
    assert.equal(runDir.mode, 0o700);
    assert.equal(runDir.path, host.runDir());
    const kinds = readEvents(host.observerPath).map((event) => event.kind);
    assert.ok(kinds.indexOf("engine-listen") < kinds.indexOf("upstream-open"));
    assert.ok(kinds.indexOf("upstream-open") < kinds.indexOf("ready"));
    const starts = events(host, "workerd-start");
    assert.equal(starts.length, 1);
    assert.equal(events(host, "upstream-open").length, 1);
    const cdp = await connect(host.port);
    const page = await open(cdp);
    await setContent(cdp, page.sessionId, "<p>transport</p>");
    assert.equal(await evaluate(cdp, page.sessionId, "1 + 1"), 2);
    await cdp.disconnect();
    assert.equal(events(host, "upstream-open").length, 1);
  },
);

test(
  "a bad config fails without leftovers, and a restart works",
  { timeout: 120_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "ks-bad-config-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const badConfig = join(directory, "bad.capnp");
    const fs = await import("node:fs");
    fs.writeFileSync(badConfig, "this is not a capnp config\n");
    const observerPath = join(directory, "observe.jsonl");
    await assert.rejects(
      startNativeHost({
        workerd,
        config: badConfig,
        assets: engineAssets,
        assetsManifest,
        observe: observerPath,
        startupTimeoutMs: 20_000,
      }),
      /exited|ready/,
    );
    const commandLines = (() => {
      try {
        return execFileSync("pgrep", ["-f", badConfig], { encoding: "utf8" }).trim();
      } catch {
        return "";
      }
    })();
    assert.equal(commandLines, "");
    const exited = readEvents(observerPath).filter((event) => event.kind === "engine-exit");
    assert.ok(exited.length === 0 || exited.every((event) => event.reaped));
    const host = await hostFor(t, { capnp: "local-private.capnp" });
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const page = await open(cdp);
    await setContent(cdp, page.sessionId, "<p>restart</p>");
    assert.equal(await evaluate(cdp, page.sessionId, "6 * 7"), 42);
  },
);

test(
  "SIGTERM reaps workerd, removes the run directory and releases the port",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t);
    const cdp = await connect(host.port);
    await cdp.disconnect();
    const runDir = host.runDir();
    assert.ok(existsSync(runDir));
    const status = await host.stop("SIGTERM");
    assert.equal(status.code, 0);
    const shutdown = events(host, "shutdown").at(-1);
    assert.deepEqual(
      { clean: shutdown.clean, dirRemoved: shutdown.dirRemoved },
      { clean: true, dirRemoved: true },
    );
    assert.ok(!existsSync(runDir));
    const engineExit = events(host, "engine-exit").at(-1);
    assert.equal(engineExit.reaped, true);
    const server = createTcpServer();
    server.listen(host.port, "127.0.0.1");
    await once(server, "listening");
    server.close();
  },
);

test(
  "killing workerd is reported as engine loss and still reaps the directory",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t);
    const runDir = host.runDir();
    const workerdPid = events(host, "workerd-start")[0].pid;
    process.kill(workerdPid, "SIGKILL");
    const status = await withTimeout(host.exit, 30_000);
    assert.ok(!status.timeout, "host exits after engine loss");
    assert.equal(status.code, 1);
    assert.ok(!existsSync(runDir));
    const shutdown = events(host, "shutdown").at(-1);
    assert.equal(shutdown.clean, false);
    assert.equal(shutdown.dirRemoved, true);
    assert.ok(host.stderrText().includes("engine was lost"));
    const server = createTcpServer();
    server.listen(host.port, "127.0.0.1");
    await once(server, "listening");
    server.close();
  },
);

test(
  "the native runtime needs no Node on PATH and no engine-mf.json",
  { timeout: 120_000 },
  async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "ks-no-node-"));
    const bin = join(directory, "bin");
    const marker = join(directory, "node-ran");
    mkdirSync(bin);
    for (const name of ["node", "npx", "miniflare", "wrangler"]) {
      writeFileSync(
        join(bin, name),
        `#!/bin/sh\necho "${name} $@" >> ${JSON.stringify(marker)}\nexit 97\n`,
        { mode: 0o755 },
      );
    }
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    const host = await hostFor(t, { env: { PATH: bin } });
    const cdp = await connect(host.port);
    t.after(() => cdp.disconnect());
    const workerdPid = events(host, "workerd-start")[0].pid;
    const page = await open(cdp);
    await setContent(cdp, page.sessionId, "<p id='runtime'>native</p>");
    assert.equal(
      await evaluate(cdp, page.sessionId, "document.getElementById('runtime').textContent"),
      "native",
    );
    assert.equal(existsSync(marker), false, "the host did not execute a Node tool");
    assert.ok(!processCommand(workerdPid).includes("engine-mf.json"));
  },
);

test(
  "private services are manifest-limited and absent from the public routes",
  { timeout: 120_000 },
  async (t) => {
    const host = await hostFor(t);
    const assets = join(host.runDir(), "assets.sock");
    const profile = join(host.runDir(), "profile.sock");
    const fixture = await unixRequest(assets, { path: "/fixtures/pixel.png" });
    assert.equal(fixture.status, 200);
    assert.equal(fixture.headers["content-type"], "image/png");
    assert.equal(fixture.body.length, statSync(join(engineAssets, "fixtures/pixel.png")).size);
    assert.equal((await unixRequest(assets, { path: "/etc/passwd" })).status, 404);
    assert.equal((await unixRequest(assets, { path: "/fonts/../fixtures/pixel.png" })).status, 404);
    assert.equal((await unixRequest(assets, { path: "/fonts/NotoSans.ttf" })).status, 404);
    const ranged = await unixRequest(assets, {
      path: "/rbi/scene_host.json",
      headers: { range: "bytes=0-3" },
    });
    const rangedBytes = statSync(join(engineAssets, "rbi", "scene_host.json")).size;
    assert.equal(ranged.status, 206);
    assert.equal(ranged.headers["content-range"], `bytes 0-3/${rangedBytes}`);
    assert.equal(ranged.body.length, 4);
    assert.equal(
      (await unixRequest(profile, { method: "POST", path: "/load", body: { owner: "probe" } }))
        .status,
      409,
    );
    for (const body of [{}, { owner: {} }, { owner: "" }])
      assert.equal(
        (await unixRequest(profile, { method: "POST", path: "/load", body })).status,
        409,
      );
    assert.equal((await host.request("/fixtures/pixel.png")).status, 404);
    assert.equal((await host.request("/load", { method: "POST" })).status, 404);
    const pages = await host.request("/json/list");
    assert.ok(pages.body.every((entry) => entry.type === "page"));
    assert.ok(!JSON.stringify(pages.body).includes("profile.sock"));
    const logs = host.stdoutText() + host.stderrText();
    const observerText = readFileSync(host.observerPath, "utf8");
    assert.ok(!logs.includes("http-only-seed"), "profile state stays out of logs");
    assert.ok(!observerText.includes("http-only-seed"), "profile state stays out of the observer");
    assert.ok(!observerText.includes('"state"'), "the observer has no state field");
    assert.ok(!observerText.includes('"owner"'), "the observer has no owner field");
    for (const event of events(host, "profile"))
      assert.deepEqual(Object.keys(event).sort(), ["kind", "operation", "revision", "status"]);
  },
);

test(
  "the public config blocks loopback while the private config allows it",
  { timeout: 120_000 },
  async (t) => {
    const site = await fixture(t);
    const publicHost = await hostFor(t, { capnp: "local-public.capnp" });
    const publicCdp = await connect(publicHost.port);
    t.after(() => publicCdp.disconnect());
    const publicPage = await open(publicCdp);
    const navigated = await publicCdp.send(
      "Page.navigate",
      { url: `${site.url}/a` },
      publicPage.sessionId,
    );
    const href = await evaluate(publicCdp, publicPage.sessionId, "location.href");
    assert.ok(navigated.errorText || !href.endsWith("/a"), JSON.stringify({ navigated, href }));
    const publicCookies = String(
      await evaluate(publicCdp, publicPage.sessionId, "document.cookie"),
    );
    assert.ok(!publicCookies.includes("seed"));
    assert.ok(!publicCookies.includes("sid"));
    assert.equal(
      await evaluate(publicCdp, publicPage.sessionId, "localStorage.getItem('seed')"),
      null,
    );
    assert.equal((await publicHost.request("/load", { method: "POST" })).status, 404);
    const privateHost = await hostFor(t, { capnp: "local-private.capnp" });
    const privateCdp = await connect(privateHost.port);
    t.after(() => privateCdp.disconnect());
    const privatePage = await open(privateCdp, `${site.url}/a`);
    assert.equal(await evaluate(privateCdp, privatePage.sessionId, "window.__marker"), "a");
    const privateCookies = String(
      await evaluate(privateCdp, privatePage.sessionId, "document.cookie"),
    );
    assert.ok(!privateCookies.includes("seed"));
    assert.ok(!privateCookies.includes("sid"));
    assert.equal(
      await evaluate(privateCdp, privatePage.sessionId, "localStorage.getItem('seed')"),
      null,
    );
    assert.equal((await privateHost.request("/load", { method: "POST" })).status, 404);
  },
);
