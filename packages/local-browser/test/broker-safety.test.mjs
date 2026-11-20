// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// 真实 workerd 的诊断、关闭排空和有限并发回归；故障子宿主明确要求非零退出。
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import WebSocket from "ws";
import { required, startNativeHost, waitFor, withTimeout } from "./host-adapter.mjs";

const workerd = required("KITESURF_WORKERD");
const configDir = required("KITESURF_CONFIG_DIR");
const options = {
  workerd,
  config: join(configDir, "local-private.capnp"),
  assets: required("KITESURF_ENGINE_ASSETS"),
  assetsManifest: join(configDir, "assets-manifest.json"),
};
const sensitive = "synthetic-diagnostic-private-payload";

async function client(host) {
  const ws = new WebSocket(`ws://127.0.0.1:${host.port}/devtools/browser?owner=safety`);
  const pending = new Map();
  let id = 0;
  ws.on("message", (data) => {
    const frame = JSON.parse(data.toString());
    if (frame.id !== undefined) pending.get(frame.id)?.(frame);
  });
  ws.on("error", () => {});
  await new Promise((resolve, reject) => {
    ws.once("open", resolve);
    ws.once("error", reject);
  });
  return {
    ws,
    call(method, params = {}, sessionId) {
      const requestId = ++id;
      const result = new Promise((resolve) => pending.set(requestId, resolve));
      ws.send(JSON.stringify({ id: requestId, method, params, sessionId }));
      return withTimeout(result, 15_000).then((reply) => {
        pending.delete(requestId);
        assert.equal(reply.timeout, undefined, `${method} timed out`);
        return reply;
      });
    },
  };
}

function completed(host, expectedCode, fatal) {
  const events = host.events();
  const exit = events.findLast((event) => event.kind === "engine-exit");
  const shutdown = events.findLast((event) => event.kind === "shutdown");
  assert.ok(exit?.reaped, "workerd was not reaped");
  assert.equal(exit.fatal, fatal);
  if (!fatal && expectedCode === 0) assert.equal(exit.code, 0);
  assert.equal(shutdown?.clean, expectedCode === 0);
  assert.equal(shutdown?.dirRemoved, true);
  const runDir = events.find((event) => event.kind === "run-dir")?.path;
  assert.ok(runDir && !existsSync(runDir));
  if (!fatal && expectedCode === 0) {
    assert.equal(
      events.some((event) => event.kind === "engine-lost"),
      false,
    );
  }
  if (!fatal) {
    assert.equal(
      events.some((event) => event.kind === "engine-diagnostic"),
      false,
    );
    assert.doesNotMatch(host.stderr, /uncaught|internal[\s_-]+error/i);
  }
  assert.ok(!host.stdout.includes(sensitive));
  assert.ok(!host.stderr.includes(sensitive));
  assert.ok(!readFileSync(host.observerPath, "utf8").includes(sensitive));
  return { exit, shutdown };
}

function wrapper(directory, phase) {
  const path = join(directory, "workerd-wrapper");
  const script = `${path}.mjs`;
  writeFileSync(path, `#!/bin/sh\nexec '${process.execPath}' '${script}' "$@"\n`);
  const trigger = join(directory, "trigger");
  const observe = join(directory, "observe.jsonl");
  writeFileSync(
    script,
    `
import { spawn } from 'node:child_process';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
const phase = ${JSON.stringify(phase)};
const payload = ${JSON.stringify(sensitive)};
let injected = false;
function inject() {
  if (injected) return;
  injected = true;
  if (phase === 'gc') {
    process.stdout.write('[ks-gc] ' + JSON.stringify({at:1,done:2,extra:{payload}}) + '\\n');
    return;
  }
  const error = phase === 'close' ? 'Uncaught Error: Network connection lost.' : 'UnCaUgHt Error';
  process.stderr.write('[ks-gc] {"at":1,"done":2,"extra":"' + payload + '"} ' + error + ' ' + payload + '\\n');
  process.stdout.write('workerd INTERNAL   ERROR ' + payload + '\\n');
}
function drain() {
  const target = phase.endsWith('-stdout') ? process.stdout : process.stderr;
  const bytes = Buffer.from('workerd INTERNAL\\x0bERROR ' + payload + ' ' + 'x'.repeat(16384));
  for (let offset = 0; offset < bytes.length; offset += 7) target.write(bytes.subarray(offset, offset + 7));
}
const child = spawn(${JSON.stringify(workerd)}, process.argv.slice(2), {stdio:['ignore','pipe','pipe',3]});
writeFileSync(${JSON.stringify(join(directory, "workerd-pid"))}, String(child.pid));
child.stdout.pipe(process.stdout);
child.stderr.pipe(process.stderr);
if (phase === 'before') setTimeout(inject, 20);
const timer = setInterval(() => {
  if (existsSync(${JSON.stringify(trigger)})) inject();
  if (phase === 'close') {
    try { if (readFileSync(${JSON.stringify(observe)}, 'utf8').includes('"kind":"close-succeeded"')) inject(); } catch {}
  }
}, 2);
let killTimer;
for (const signal of ['SIGTERM','SIGINT']) process.on(signal, () => {
  if (phase === 'shutdown' || phase === 'close') inject();
  child.kill(signal);
  killTimer ??= setTimeout(() => child.kill('SIGKILL'), 2000);
});
child.on('close', (code, signal) => {
  clearTimeout(killTimer);
  clearInterval(timer);
  writeFileSync(${JSON.stringify(join(directory, "workerd-reaped"))}, JSON.stringify({code,signal}));
  if (phase === 'exit') inject();
  if (phase.startsWith('drain-')) drain();
  process.exitCode = code ?? (signal === 'SIGTERM' || signal === 'SIGINT' ? 0 : 1);
});
`,
  );
  // 测试包装器持有真实 workerd 并转发信号；不改变引擎模块或公开 fault API。
  return { path, trigger, observe };
}

async function wrapped(t, phase) {
  const directory = mkdtempSync(join(tmpdir(), "ks-diagnostic-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const files = wrapper(directory, phase);
  const { chmodSync } = await import("node:fs");
  chmodSync(files.path, 0o700);
  const host = await startNativeHost({
    ...options,
    workerd: files.path,
    observe: files.observe,
    expectedFailure: phase !== "gc",
  });
  if (phase !== "before") assert.ok(host.port, host.stderr);
  t.after(() => host.stop());
  return {
    host,
    ...files,
    childReaped: join(directory, "workerd-reaped"),
    childPid: Number(readFileSync(join(directory, "workerd-pid"), "utf8")),
  };
}

for (const phase of ["before", "after", "close", "shutdown", "exit"]) {
  test(
    `fatal diagnostic ${phase}: expected child-host failure, redacted and reaped`,
    { timeout: 60_000 },
    async (t) => {
      const { host, trigger, childPid, childReaped } = await wrapped(t, phase);
      if (phase === "before") {
        assert.equal(host.ready.failed, true);
      } else if (phase === "after") {
        assert.equal((await host.request("/health")).body.alive, true);
        writeFileSync(trigger, "inject");
        try {
          const health = await host.request("/health");
          assert.ok(!JSON.stringify(health).includes(sensitive));
        } catch (error) {
          assert.match(error.message, /fetch failed|ECONNREFUSED|socket|terminated/);
        }
      } else if (phase === "close" || phase === "exit") {
        const cdp = await client(host);
        t.after(() => cdp.ws.terminate());
        assert.deepEqual((await cdp.call("Browser.close")).result, {});
      } else {
        await host.stop("SIGTERM");
      }
      const status = await withTimeout(host.exit, 20_000);
      assert.equal(status.timeout, undefined);
      assert.equal(status.code, 1);
      assert.ok(existsSync(childReaped), "wrapper must reap the real workerd before exiting");
      assert.throws(() => process.kill(childPid, 0), { code: "ESRCH" });
      const events = host.events();
      assert.ok(events.some((event) => event.kind === "engine-diagnostic"));
      const exit = events.findLast((event) => event.kind === "engine-exit");
      assert.equal(exit?.reaped, true);
      assert.equal(exit?.fatal, true);
      if (phase === "close") assert.equal(exit.uncaught, 1);
      const runDir = events.find((event) => event.kind === "run-dir")?.path;
      assert.ok(runDir && !existsSync(runDir));
      completed(host, 1, true);
      if (phase === "close" || phase === "exit") {
        assert.ok(events.some((event) => event.kind === "close-succeeded"));
      }
      for (const text of [host.stdout, host.stderr, readFileSync(host.observerPath, "utf8")]) {
        assert.ok(!text.includes(sensitive));
      }
      t.diagnostic(JSON.stringify({ phase, expectedFailure: true, hostCode: status.code, exit }));
    },
  );
}

for (const stream of ["stdout", "stderr"]) {
  test(
    `final drain vertical tab on ${stream}: fatal after workerd exit 0`,
    { timeout: 60_000 },
    async (t) => {
      const { host, childReaped, childPid } = await wrapped(t, `drain-${stream}`);
      const closed = await withTimeout(host.request("/close", { method: "POST" }), 15_000);
      assert.equal(closed.timeout, undefined);
      assert.equal(closed.status, 200);
      const status = await withTimeout(host.exit, 20_000);
      assert.equal(status.timeout, undefined);
      assert.equal(status.code, 1);
      assert.ok(existsSync(childReaped), "wrapper must reap the real workerd before exiting");
      assert.throws(() => process.kill(childPid, 0), { code: "ESRCH" });
      const events = host.events();
      const categories = events
        .filter((event) => event.kind === "engine-diagnostic")
        .map((event) => event.category);
      assert.deepEqual(categories, ["internal-error"]);
      const exit = events.findLast((event) => event.kind === "engine-exit");
      assert.equal(exit?.reaped, true);
      assert.equal(exit?.fatal, true);
      completed(host, 1, true);
      t.diagnostic(JSON.stringify({ stream, hostCode: status.code, fatal: exit?.fatal }));
    },
  );
}

test(
  "finite parallel close: 12 CDP trials with offsets, HTTP and signal controls",
  { timeout: 180_000 },
  async (t) => {
    let passed = 0;
    for (let batch = 0; batch < 3; batch++) {
      const trials = [];
      try {
        // Prepare each live page first so all four close requests race the same lifecycle stage.
        // Cold page compilation is independent of closing and can stagger those requests.
        for (let index = 0; index < 4; index++) {
          const host = await startNativeHost(options);
          const trial = { host, cdp: undefined };
          trials.push(trial);
          const cdp = await client(host);
          trial.cdp = cdp;
          assert.equal((await host.request("/health")).body.alive, true);
          assert.equal((await host.request("/flush", { method: "POST" })).status, 200);
          const created = await cdp.call("Target.createTarget", { url: "about:blank" });
          const attached = await cdp.call("Target.attachToTarget", {
            targetId: created.result.targetId,
            flatten: true,
          });
          const sessionId = attached.result.sessionId;
          const content = await cdp.call(
            "Page.setDocumentContent",
            { html: "<body><p>close race</p></body>" },
            sessionId,
          );
          assert.equal(content.error, undefined);
          const evaluated = await cdp.call(
            "Runtime.evaluate",
            { expression: "6 * 7", returnByValue: true },
            sessionId,
          );
          assert.equal(evaluated.result.result.value, 42);
        }
        const results = await Promise.allSettled(
          trials.map(async ({ host, cdp }, offset) => {
            await new Promise((resolve) => setTimeout(resolve, offset));
            assert.deepEqual((await cdp.call("Browser.close")).result, {});
            const status = await withTimeout(host.exit, 20_000);
            assert.equal(status.code, 0);
            completed(host, 0, false);
          }),
        );
        for (const result of results) {
          if (result.status === "rejected") throw result.reason;
          passed++;
        }
      } finally {
        await Promise.all(
          trials.map(async ({ host, cdp }) => {
            cdp?.ws.terminate();
            await host.stop();
          }),
        );
      }
    }
    for (const mode of ["http", "signal"]) {
      const host = await startNativeHost(options);
      try {
        if (mode === "http")
          assert.equal((await host.request("/close", { method: "POST" })).status, 200);
        else
          assert.equal(
            (await host.stop("SIGTERM")).code,
            0,
            JSON.stringify({ stderr: host.stderr, events: host.events() }),
          );
        assert.equal((await host.exit).code, 0);
        completed(host, 0, false);
      } finally {
        await host.stop();
      }
    }
    t.diagnostic(`CDP_CLOSE_TRIALS=${passed} FAILED=0 offsets=0,1,2,3 HTTP=1 SIGNAL=1`);
  },
);

test("real engine crash remains a nonzero reaped host failure", { timeout: 60_000 }, async (t) => {
  const host = await startNativeHost({ ...options, expectedFailure: true });
  t.after(() => host.stop());
  const pid = host.events().find((event) => event.kind === "workerd-start").pid;
  process.kill(pid, "SIGKILL");
  const status = await withTimeout(host.exit, 20_000);
  assert.equal(status.code, 1);
  const { exit } = completed(host, 1, false);
  assert.equal(exit.signal, 9);
  assert.ok(host.events().some((event) => event.kind === "engine-lost"));
});

test("GC extended payload only emits numeric allowlist", { timeout: 60_000 }, async (t) => {
  const { host, trigger } = await wrapped(t, "gc");
  writeFileSync(trigger, "inject");
  const gc = await waitFor(
    () => host.events().find((event) => event.kind === "gc"),
    "GC allowlist",
  );
  assert.deepEqual(gc, { kind: "gc", at: 1, done: 2 });
  assert.equal((await host.request("/close", { method: "POST" })).status, 200);
  assert.equal((await host.exit).code, 0);
  completed(host, 0, false);
});
