// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Release acceptance drives only a copy of the shipped executable, never the Node host or
// separately staged engine assets. Node is the external test driver, outside the runtime tree.
import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync, spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import {
  chmodSync,
  copyFileSync,
  existsSync,
  lstatSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  realpathSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { createServer as createTcpServer } from "node:net";
import { PNG } from "pngjs";
import { connect, evaluate, fixtureServer, openPage, until } from "./helpers.mjs";

const suppliedBinary = process.env.KITESURF_TEST_BINARY;
const executable =
  suppliedBinary &&
  (isAbsolute(suppliedBinary)
    ? suppliedBinary
    : process.env.RUNFILES_DIR
      ? join(process.env.RUNFILES_DIR, process.env.JS_BINARY__WORKSPACE || "_main", suppliedBinary)
      : resolve(suppliedBinary));

function descendants(pid) {
  const table = execFileSync("ps", ["-axo", "pid=,ppid=,command="], { encoding: "utf8" })
    .trim()
    .split("\n")
    .map((line) => {
      const match = /^\s*(\d+)\s+(\d+)\s+(.+)$/.exec(line);
      return match && { pid: Number(match[1]), parent: Number(match[2]), command: match[3] };
    })
    .filter(Boolean);
  const found = [];
  const queue = [pid];
  while (queue.length) {
    const parent = queue.shift();
    for (const entry of table.filter((candidate) => candidate.parent === parent)) {
      found.push(entry);
      queue.push(entry.pid);
    }
  }
  return found;
}

function assertNativeTree(pid, env) {
  const children = descendants(pid);
  assert.equal(
    children.length,
    1,
    `only workerd may be a runtime subprocess: ${JSON.stringify(children)}`,
  );
  assert.equal(children[0].parent, pid);
  const version = spawnSync(children[0].command.split(/\s+/)[0], ["--version"], {
    env,
    encoding: "utf8",
    timeout: 30_000,
  });
  assert.ifError(version.error);
  assert.equal(version.status, 0, version.stderr);
  assert.equal(version.stdout, "workerd 2026-09-11\n");
  assert.match(basename(children[0].command.split(/\s+/)[0]), /^workerd(?:\.exe)?$/);
  assert.doesNotMatch(
    children[0].command,
    /(?:^|\/)(?:node|npm|npx|miniflare|wrangler|kitesurf-font-host)(?:\s|$)/,
  );
  const socket = /--socket-addr\s+engine=unix:([^ ]+)/.exec(children[0].command)?.[1];
  assert.ok(socket, "workerd must use the owned private engine socket");
  return { pid: children[0].pid, directory: dirname(socket) };
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

async function assertPortReleased(port) {
  const listener = createTcpServer();
  try {
    await new Promise((resolve, reject) => {
      listener.once("error", reject);
      listener.listen(port, "127.0.0.1", resolve);
    });
  } finally {
    if (listener.listening) await new Promise((resolve) => listener.close(resolve));
  }
}

function glyphMask(png, cell) {
  const pixels = [];
  let left = png.width;
  let top = png.height;
  for (
    let y = Math.max(0, Math.floor(cell.y));
    y < Math.min(png.height, cell.y + cell.height);
    y++
  ) {
    for (
      let x = Math.max(0, Math.floor(cell.x));
      x < Math.min(png.width, cell.x + cell.width);
      x++
    ) {
      const index = (y * png.width + x) * 4;
      if (png.data[index] > 160 || png.data[index + 1] > 160 || png.data[index + 2] > 160) continue;
      left = Math.min(left, x);
      top = Math.min(top, y);
      pixels.push([x, y]);
    }
  }
  return { ink: pixels.length, shape: pixels.map(([x, y]) => `${x - left},${y - top}`).join(" ") };
}

async function assertGlyphRendering(cdp) {
  const { targetId } = await cdp.send("Target.createTarget", { url: "about:blank" });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  const tree = await cdp.send("Page.getFrameTree", {}, sessionId);
  await cdp.send(
    "Page.setDocumentContent",
    {
      frameId: tree.frameTree.frame.id,
      html:
        "<!doctype html><style>html,body{margin:0;background:#fff;color:#111}" +
        "span{display:inline-block;width:80px;height:80px;font:48px/80px sans-serif;vertical-align:top}</style>" +
        "<body><span id='a'>I</span><span id='b'>W</span><span id='c'>I</span></body>",
    },
    sessionId,
  );
  await until(
    async () => {
      const result = await cdp.send("Page.captureScreenshot", { format: "png" }, sessionId);
      const png = PNG.sync.read(Buffer.from(result.data, "base64"));
      assert.ok(png.width >= 240 && png.height >= 80, "a real viewport");
      const cells = await evaluate(
        cdp,
        sessionId,
        "['a','b','c'].map(function(id){var r=document.getElementById(id).getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height};})",
      );
      const masks = cells.map((cell) => glyphMask(png, cell));
      if (!masks.every(({ ink }) => ink > 40)) return false;
      assert.equal(
        masks[0].shape,
        masks[2].shape,
        "repeated glyphs have the same rasterized shape",
      );
      assert.notEqual(
        masks[0].shape,
        masks[1].shape,
        "I and W render distinct glyphs, not missing-glyph boxes",
      );
      return true;
    },
    "visible system-font glyphs",
    30_000,
  );
  await until(
    async () => {
      const state = await cdp.send("Kitesurf.getPageState", { targetId });
      return state.fontDiagnostics?.some((line) => /^system core sans=(?!\?$).+/.test(line));
    },
    "system sans font registration",
    30_000,
  );
  await cdp.send("Target.closeTarget", { targetId });
}

test(
  "native release: copied executable, empty PATH, real glyphs and persistent account recovery",
  { timeout: 480_000 },
  async (t) => {
    assert.ok(executable, "KITESURF_TEST_BINARY must point at the shipped native executable");
    const fileType = lstatSync(executable);
    assert.ok(
      fileType.isFile() && !fileType.isSymbolicLink(),
      "the supplied release executable must be a real file, not a host-dependent symlink",
    );
    // Keep Unix socket paths below Darwin's 104-byte limit even under Bazel's long TMPDIR.
    const directory = mkdtempSync("/tmp/ks-release-");
    const copied = join(directory, "kitesurf");
    copyFileSync(executable, copied);
    chmodSync(copied, 0o755);
    assert.equal(
      readFileSync(copied).includes(Buffer.from("NODE_SEA_FUSE_fce680ab2cc467b6e072b8b5df1996b2")),
      false,
      "the shipped executable must not be a Node SEA runtime",
    );
    assert.deepEqual(readdirSync(directory), ["kitesurf"], "copy only the executable");
    const fixture = await fixtureServer();
    const profile = join(directory, "account.sqlite");
    const backup = join(directory, "backup.sqlite");
    const isolated = join(directory, "isolated.sqlite");
    const env = {
      ...process.env,
      PATH: "",
      NODE_OPTIONS: "",
      NODE_PATH: "",
      TMPDIR: directory,
      HOME: directory,
      XDG_DATA_HOME: directory,
    };
    for (const key of Object.keys(env)) {
      if (/^(KITESURF_|KS_|RUNFILES_|JS_BINARY_|BAZEL_)/.test(key)) delete env[key];
    }
    let child;
    let cdp;
    let output = "";
    let stdout = "";
    let stderr = "";
    let port;
    let workerdPid;
    let workerdDirectory;
    const cli = (args) => {
      const result = spawnSync(copied, args, {
        env,
        cwd: directory,
        encoding: "utf8",
        timeout: 30_000,
      });
      assert.ifError(result.error);
      assert.equal(result.status, 0, `${args.join(" ")}: ${result.stderr}`);
      assert.equal(result.signal, null);
      return result.stdout;
    };
    const start = async (path, create = false) => {
      output = "";
      stdout = "";
      stderr = "";
      child = spawn(
        copied,
        [
          "serve",
          "--profile",
          path,
          "--port",
          "0",
          "--allow-private-network",
          ...(create ? ["--create-profile"] : []),
        ],
        { env, cwd: directory, detached: true, stdio: ["ignore", "pipe", "pipe"] },
      );
      child.stdout.on("data", (data) => {
        stdout += data;
        output += data;
      });
      child.stderr.on("data", (data) => {
        stderr += data;
        output += data;
      });
      child.on("error", (error) => {
        stderr += error.message;
        output += error.message;
      });
      try {
        const ready = await until(
          () => {
            if (child.exitCode !== null || child.signalCode !== null)
              throw new Error(output.slice(-4000));
            for (const line of output.split("\n")) {
              let value;
              try {
                value = JSON.parse(line);
              } catch {
                continue;
              }
              if (value.ready) return value;
            }
            return false;
          },
          "copied native release readiness",
          60_000,
        );
        port = ready.port;
        const engine = assertNativeTree(child.pid, env);
        workerdPid = engine.pid;
        workerdDirectory = engine.directory;
        cdp = await connect(port);
        return ready;
      } catch (error) {
        throw new Error(
          `${error.message}\n` +
            `native child pid=${child.pid} exitCode=${child.exitCode} signalCode=${child.signalCode}\n` +
            `recent stdout:\n${stdout.slice(-8000)}\nrecent stderr:\n${stderr.slice(-8000)}`,
          { cause: error },
        );
      }
    };
    const close = async (method = "cdp") => {
      const ended = once(child, "exit");
      if (method === "cli") cli(["stop", "--port", String(port)]);
      else if (method === "sigterm") assert.equal(child.kill("SIGTERM"), true);
      else await cdp.send("Browser.close");
      await ended;
      assert.equal(child.exitCode, 0, output.slice(-4000));
      assert.doesNotMatch(output, /uncaught|internal[\s_-]+error/i);
      if (method === "sigterm") {
        assert.equal(child.signalCode, null);
        assert.equal(processExists(workerdPid), false, "SIGTERM must reap the owned workerd");
        assert.equal(
          existsSync(workerdDirectory),
          false,
          "SIGTERM must remove the private run directory",
        );
        await assertPortReleased(port);
        t.diagnostic(
          "SIGTERM exited cleanly, reaped workerd, removed its directory and released the port",
        );
      }
      await cdp.disconnect();
      cdp = undefined;
    };
    const assertRestored = async (route) => {
      const page = await openPage(cdp, `${fixture.url}/${route}`);
      assert.deepEqual(
        await evaluate(cdp, page.sessionId, "[boot.token,boot.authenticated,boot.step]"),
        ["T1", true, null],
      );
      const visible = await evaluate(cdp, page.sessionId, "document.cookie");
      assert.match(visible, /displayName=Alice/);
      assert.doesNotMatch(
        visible,
        /(?:^|;\s*)sid=/,
        "HttpOnly credentials stay hidden after recovery",
      );
      assert.ok(
        fixture.seen.some(
          (request) => request.path === `/${route}` && /sid=T1/.test(request.cookie),
        ),
        "the restored HttpOnly cookie is sent to the origin",
      );
    };
    t.after(async () => {
      await cdp?.disconnect();
      if (child && child.exitCode === null && child.signalCode === null) {
        const ended = once(child, "exit");
        process.kill(-child.pid, "SIGKILL");
        await ended;
      }
      await fixture.close();
      rmSync(directory, { recursive: true, force: true });
    });

    assert.equal(cli(["--version"]), "kitesurf 0.5.2\n");
    assert.match(cli(["--help"]), /kitesurf.*serve/);
    const first = await start(profile, true);
    const status = JSON.parse(cli(["status", "--port", String(port)]));
    assert.equal(status.generation, first.generation);
    let page = await openPage(cdp, `${fixture.url}/before-crash`);
    assert.equal(await evaluate(cdp, page.sessionId, "navigator.userAgent"), "Kitesurf/0.5.2");
    await evaluate(cdp, page.sessionId, "document.getElementById('username').focus()");
    await cdp.send("Input.insertText", { text: "Alice" }, page.sessionId);
    assert.equal(
      await evaluate(cdp, page.sessionId, "document.getElementById('username').value"),
      "Alice",
    );
    const box = await evaluate(
      cdp,
      page.sessionId,
      "(()=>{var r=document.getElementById('login').getBoundingClientRect();return {x:r.x,y:r.y,width:r.width,height:r.height};})()",
    );
    const click = {
      x: box.x + box.width / 2,
      y: box.y + box.height / 2,
      button: "left",
      clickCount: 1,
    };
    for (const type of ["mousePressed", "mouseReleased"]) {
      await cdp.send("Input.dispatchMouseEvent", { ...click, type }, page.sessionId);
    }
    await until(
      () => evaluate(cdp, page.sessionId, "window.loggedIn === true"),
      "synthetic account login",
    );
    assert.equal(await evaluate(cdp, page.sessionId, "localStorage.getItem('token')"), "T1");
    assert.doesNotMatch(await evaluate(cdp, page.sessionId, "document.cookie"), /(?:^|;\s*)sid=/);
    const screenshotPath = process.env.KITESURF_TEST_SCREENSHOT;
    if (screenshotPath) {
      await evaluate(
        cdp,
        page.sessionId,
        "document.getElementById('result').textContent='登录成功：Alice · 中文持久账号';document.querySelector('header').textContent='Kitesurf · 本地浏览器登录验收';",
      );
    }
    await assertGlyphRendering(cdp);
    if (screenshotPath) {
      try {
        await until(
          async () => {
            await cdp.send("Page.captureScreenshot", { format: "png" }, page.sessionId);
            const state = await cdp.send("Kitesurf.getPageState", { targetId: page.targetId });
            return state.fontDiagnostics?.some((line) =>
              /^(system CJK|missing coverage:) script=Hani /.test(line),
            );
          },
          "Chinese screenshot font selection",
          30_000,
        );
      } catch (error) {
        t.diagnostic(`Screenshot font readiness: ${error.message}`);
      }
      const shot = await cdp.send("Page.captureScreenshot", { format: "png" }, page.sessionId);
      writeFileSync(screenshotPath, Buffer.from(shot.data, "base64"));
      t.diagnostic(`Synthetic account screenshot: ${screenshotPath}`);
    }
    assertNativeTree(child.pid, env);
    cli(["flush", "--port", String(port)]);
    const backedUp = JSON.parse(cli(["backup", "--output", backup, "--port", String(port)]));
    assert.equal(backedUp.path, realpathSync(backup));
    assert.equal(statSync(backup).mode & 0o777, 0o600);
    assert.equal(
      JSON.parse(cli(["status", "--port", String(port)])).generation,
      first.generation,
      "online backup keeps the host alive",
    );
    await cdp.disconnect();
    cdp = undefined;
    const ended = once(child, "exit");
    process.kill(-child.pid, "SIGKILL");
    await ended;

    const second = await start(profile);
    assert.notEqual(second.generation, first.generation);
    await assertRestored("after-crash");
    await close();
    await start(backup);
    await assertRestored("from-online-backup");
    await close("cli");
    cli(["profile", "create", isolated]);
    await start(isolated);
    page = await openPage(cdp, `${fixture.url}/independent-profile`);
    assert.deepEqual(
      await evaluate(cdp, page.sessionId, "[boot.token,boot.authenticated,boot.step]"),
      [null, false, null],
    );
    const claimed = JSON.parse(
      cli(["control", "--owner", "release-acceptance", "--takeover", "--port", String(port)]),
    );
    assert.equal(claimed.controller, "release-acceptance");
    await cdp.disconnect();
    cdp = await connect(port, "?owner=release-acceptance");
    assert.equal((await cdp.send("Browser.getVersion")).revision, "@kitesurf");
    await close();
    await start(isolated);
    await openPage(cdp, `${fixture.url}/before-sigterm`);
    await close("sigterm");
  },
);
