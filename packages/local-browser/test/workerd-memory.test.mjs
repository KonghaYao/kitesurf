// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 原生发布的内存验收：只驱动归档的可执行文件，在 linux-x64 上按 v0.5.3 的基准重放 4 个页面、
// 96 次导航（默认 1107 MiB，带 --no-flush-liftoff-code 为 545 MiB）。指标是宿主与其 workerd
// 子进程的 PSS 之和；页面全部关闭后的残留只报告，不在这里归因。
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { once } from "node:events";
import {
  chmodSync,
  copyFileSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { createServer } from "node:http";
import { dirname, join } from "node:path";
import { createInterface } from "node:readline";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import { connect, navigate, openPage } from "./helpers.mjs";
import { located, withTimeout } from "./host-adapter.mjs";

const PAGES = 4;
const NAVIGATIONS_PER_PAGE = 24;
// 导航后先等 3 秒，让退休的子 isolate 走完清理，再读采样。
const SETTLE_AFTER_NAVIGATION_MS = 3_000;
const DEFAULT_LIMIT_MIB = 800;
// 与基准一致：关闭全部页面后等 30 秒再读，便于对照 1082/510 MiB 的残留数字。
const DEFAULT_SETTLE_AFTER_CLOSE_MS = 30_000;
const exec = promisify(execFile);

const executable = located(process.env.KITESURF_TEST_BINARY);
const releaseConfig = located(process.env.KITESURF_RELEASE_CONFIG);
const limitMib = Number(process.env.KITESURF_MEMORY_LIMIT_MIB || DEFAULT_LIMIT_MIB);
const settleAfterCloseMs = Number(
  process.env.KITESURF_MEMORY_SETTLE_MS || DEFAULT_SETTLE_AFTER_CLOSE_MS,
);

function kibToMib(kib) {
  return Math.round((kib / 1024) * 10) / 10;
}

// smaps_rollup 的 Pss 把共享页按映射比例分摊，宿主与 workerd 相加不会重复计算同一份代码页。
function pssKib(pid) {
  const rollup = readFileSync(`/proc/${pid}/smaps_rollup`, "utf8");
  const matched = /^Pss:\s+(\d+) kB$/m.exec(rollup);
  if (!matched) throw new Error(`/proc/${pid}/smaps_rollup has no Pss line`);
  return Number(matched[1]);
}

function processName(pid) {
  try {
    return readFileSync(`/proc/${pid}/comm`, "utf8").trim();
  } catch {
    return "";
  }
}

function parentPid(pid) {
  const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
  // comm 可以包含空格和括号，字段从最后一个 ")" 之后开始：state、ppid、...
  return Number(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[1]);
}

function descendants(pid) {
  const table = new Map();
  for (const entry of readdirSync("/proc")) {
    if (!/^\d+$/.test(entry)) continue;
    let parent;
    try {
      parent = parentPid(Number(entry));
    } catch {
      continue;
    }
    if (!table.has(parent)) table.set(parent, []);
    table.get(parent).push(Number(entry));
  }
  const found = [];
  const queue = [pid];
  while (queue.length) {
    for (const child of table.get(queue.shift()) || []) {
      found.push(child);
      queue.push(child);
    }
  }
  return found;
}

function measure(hostPid) {
  const engines = descendants(hostPid).filter((pid) => processName(pid).startsWith("workerd"));
  const hostKib = pssKib(hostPid);
  const engineKib = engines.reduce((sum, pid) => sum + pssKib(pid), 0);
  return {
    engines,
    hostMib: kibToMib(hostKib),
    workerdMib: kibToMib(engineKib),
    totalMib: kibToMib(hostKib + engineKib),
  };
}

function engineConfig(pid) {
  const args = readFileSync(`/proc/${pid}/cmdline`, "utf8").split("\0").filter(Boolean);
  const config = args.find((argument) => argument.endsWith(".capnp"));
  assert.ok(config, `workerd command has no config: ${args.join(" ")}`);
  return config;
}

function assertLiftoffFlag(path, label) {
  const config = readFileSync(path, "utf8");
  assert.match(
    config,
    /v8Flags = \[[^\]]*"--no-flush-liftoff-code"/,
    `${label} must keep Liftoff code for the memory acceptance`,
  );
  assert.match(config, /"--expose-gc"/, `${label} must expose gc to the engine`);
}

function fixtureServer() {
  const server = createServer((request, response) => {
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    });
    response.end(
      "<!doctype html><html><head><title>memory</title></head>" +
        '<body style="margin:0"><h1 id="route">memory</h1>' +
        "<script>window.boot=true;</script></body></html>",
    );
  });
  server.listen(0, "127.0.0.1");
  return once(server, "listening").then(() => ({
    url: `http://127.0.0.1:${server.address().port}`,
    close: () =>
      new Promise((resolve) => {
        server.close(resolve);
        server.closeAllConnections();
      }),
  }));
}

async function startNativeBrowser(directory) {
  const binary = join(directory, "kitesurf");
  copyFileSync(executable, binary);
  chmodSync(binary, 0o755);
  const env = Object.fromEntries(
    Object.entries(process.env).filter(
      ([key]) => !/^(KITESURF_|KS_|RUNFILES_|JS_BINARY_|BAZEL_)/.test(key),
    ),
  );
  Object.assign(env, {
    PATH: "",
    NODE_OPTIONS: "",
    NODE_PATH: "",
    TMPDIR: directory,
    HOME: directory,
    XDG_DATA_HOME: join(directory, "data"),
  });
  const options = { cwd: directory, env };
  const cli = async (...args) => (await exec(binary, args, { ...options, timeout: 30_000 })).stdout;
  assert.match(
    await cli("--version"),
    /^kitesurf \d+\.\d+\.\d+/,
    "use the Rust release executable",
  );
  const child = spawn(binary, ["serve", "--port", "0", "--allow-private-network"], {
    ...options,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const exit = new Promise((finish) =>
    child.once("close", (code, signal) => finish({ code, signal })),
  );
  let stderr = "";
  child.stderr.setEncoding("utf8");
  child.stderr.on("data", (chunk) => {
    stderr = (stderr + chunk).slice(-8_192);
  });
  const output = createInterface({ input: child.stdout });
  const ready = new Promise((finish, fail) => {
    child.once("error", fail);
    child.once("close", (code, signal) =>
      fail(new Error(`Host exited before ready (${code ?? signal}): ${stderr}`)),
    );
    output.on("line", (line) => {
      try {
        const value = JSON.parse(line);
        if (value.ready === true) finish(value);
      } catch {}
    });
  });
  const reap = async () => {
    if (child.exitCode === null && child.signalCode === null && child.kill("SIGTERM")) {
      if ((await withTimeout(exit, 15_000)).timeout) process.kill(-child.pid, "SIGKILL");
    }
    await exit;
  };
  try {
    const { port } = await withTimeout(ready, 60_000);
    assert.ok(Number.isInteger(port) && port > 0, `Native host was not ready: ${stderr}`);
    return {
      port,
      pid: child.pid,
      async close() {
        try {
          assert.deepEqual(JSON.parse(await cli("stop", "--port", String(port))), { closed: true });
          assert.deepEqual(await withTimeout(exit, 15_000), { code: 0, signal: null }, stderr);
          assert.doesNotMatch(stderr, /uncaught|internal[\s_-]+error/i);
        } finally {
          await reap();
        }
      },
    };
  } catch (error) {
    await reap();
    throw error;
  }
}

test(
  "native release: the workerd config keeps Liftoff code and stays inside the memory budget",
  { timeout: 900_000 },
  async (t) => {
    if (process.platform !== "linux") {
      t.skip(
        `/proc PSS accounting is Linux-only (${process.platform}); run the acceptance on a Linux release runner`,
      );
      return;
    }
    if (process.arch !== "x64") {
      t.skip(`the ${DEFAULT_LIMIT_MIB} MiB budget is calibrated on linux-x64, not ${process.arch}`);
      return;
    }
    assert.ok(
      executable,
      "KITESURF_TEST_BINARY must point at the archived native release executable",
    );
    assert.ok(Number.isFinite(limitMib) && limitMib > 0, "KITESURF_MEMORY_LIMIT_MIB is invalid");
    // Unix socket 路径要低于 108 字节，Bazel 的 TMPDIR 可能很长。
    const directory = mkdtempSync("/tmp/ks-memory-");
    const fixture = await fixtureServer();
    let browser;
    let cdp;
    t.after(async () => {
      try {
        await cdp?.disconnect();
      } finally {
        try {
          await browser?.close();
        } finally {
          await fixture.close();
          rmSync(directory, { recursive: true, force: true });
        }
      }
    });
    browser = await startNativeBrowser(directory);
    const startup = measure(browser.pid);
    assert.equal(
      startup.engines.length,
      1,
      `one workerd child: ${JSON.stringify(startup.engines)}`,
    );
    const config = engineConfig(startup.engines[0]);
    assertLiftoffFlag(config, "the extracted config");
    for (const name of ["local-public.capnp", "local-private.capnp"])
      assertLiftoffFlag(join(dirname(config), name), `the extracted ${name}`);
    if (releaseConfig)
      for (const name of ["local-public.capnp", "local-private.capnp"])
        assertLiftoffFlag(join(releaseConfig, name), `the packaged ${name}`);
    cdp = await connect(browser.port);
    const before = measure(browser.pid);
    const pages = [];
    for (let page = 0; page < PAGES; page++)
      pages.push(await openPage(cdp, `${fixture.url}/page?page=${page}&n=0`));
    let navigations = PAGES;
    for (let index = 1; index < NAVIGATIONS_PER_PAGE; index++) {
      for (let page = 0; page < PAGES; page++) {
        await navigate(cdp, pages[page].sessionId, `${fixture.url}/page?page=${page}&n=${index}`);
        navigations++;
      }
    }
    await delay(SETTLE_AFTER_NAVIGATION_MS);
    const afterNavigation = measure(browser.pid);
    const engineStats = await cdp.send("Kitesurf.getMemoryStats");
    for (const page of pages)
      assert.deepEqual(await cdp.send("Target.closeTarget", { targetId: page.targetId }), {
        success: true,
      });
    await delay(settleAfterCloseMs);
    const afterClose = measure(browser.pid);
    const report = {
      event: "memory-acceptance",
      platform: `${process.platform}-${process.arch}`,
      binary: executable,
      config,
      pages: PAGES,
      navigations,
      limitMib,
      settleAfterNavigationMilliseconds: SETTLE_AFTER_NAVIGATION_MS,
      settleAfterCloseMilliseconds: settleAfterCloseMs,
      startupMib: startup.totalMib,
      beforeNavigationMib: before.totalMib,
      afterNavigationMib: afterNavigation.totalMib,
      afterCloseMib: afterClose.totalMib,
      hostMib: {
        startup: startup.hostMib,
        afterNavigation: afterNavigation.hostMib,
        afterClose: afterClose.hostMib,
      },
      workerdMib: {
        startup: startup.workerdMib,
        afterNavigation: afterNavigation.workerdMib,
        afterClose: afterClose.workerdMib,
      },
      engine: {
        accountedBytes: engineStats.total?.bytes,
        isolatesKilled: engineStats.isolatesKilled,
      },
    };
    console.log(JSON.stringify(report));
    if (process.env.KITESURF_MEMORY_REPORT)
      writeFileSync(process.env.KITESURF_MEMORY_REPORT, JSON.stringify(report, null, 2) + "\n");
    // 800 MiB 让带 flag 的 545 MiB 有约 45% 余量，同时远低于无 flag 的 1107 MiB。
    assert.ok(
      afterNavigation.totalMib < limitMib,
      `post-navigation PSS ${afterNavigation.totalMib} MiB must stay below ${limitMib} MiB ` +
        `(host ${afterNavigation.hostMib} + workerd ${afterNavigation.workerdMib})`,
    );
  },
);
