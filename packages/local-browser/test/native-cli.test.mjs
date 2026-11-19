// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// control 命令的独立 CLI 合同：KITESURF_CLI_SCENARIO=hermetic 只驱动本进程 HTTP fixture，
// =engine 才启动真实 workerd；两个 Bazel target 各选一个场景，不扩大共享 host 合同。

import assert from "node:assert/strict";
import { execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { test } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { readEvents, required, startNativeHost } from "./host-adapter.mjs";

const nativeHost = required("KITESURF_NATIVE_HOST");
process.env.KITESURF_NATIVE_HOST = nativeHost;
const scenario = process.env.KITESURF_CLI_SCENARIO;

function spawnCli(args, timeout = 60_000) {
  const child = spawn(nativeHost, args, { stdio: ["ignore", "pipe", "pipe"] });
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  let killed = false;
  const watchdog = setTimeout(() => {
    killed = true;
    child.kill("SIGKILL");
  }, timeout);
  const exit = new Promise((resolve) => {
    child.once("close", (code, signal) => {
      clearTimeout(watchdog);
      resolve({ code, signal, stdout, stderr, killed });
    });
  });
  return { child, exit };
}

async function runCli(args, timeout = 60_000) {
  const { exit } = spawnCli(args, timeout);
  return exit;
}

async function listen(t, respond) {
  const seen = [];
  const server = createServer((request, response) => {
    const chunks = [];
    request.on("data", (chunk) => chunks.push(chunk));
    request.on("end", () => {
      const record = {
        method: request.method,
        url: request.url,
        headers: request.headers,
        body: Buffer.concat(chunks).toString("utf8"),
        socket: request.socket,
        response,
      };
      seen.push(record);
      respond(record);
    });
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  t.after(
    () =>
      new Promise((resolve) => {
        server.close(resolve);
        server.closeAllConnections();
      }),
  );
  return { port: server.address().port, seen };
}

function trackClose(record) {
  record.closed = new Promise((resolve) => {
    if (record.socket.destroyed) resolve();
    else record.socket.once("close", resolve);
  });
  return record.closed;
}

async function within(promise, milliseconds, message) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(message)), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function freePort() {
  const server = createServer();
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const { port } = server.address();
  await new Promise((resolve) => server.close(resolve));
  return port;
}

async function portAvailable(port) {
  const server = createServer();
  return new Promise((resolve) => {
    server.once("error", () => resolve(false));
    server.listen(port, "127.0.0.1", () => server.close(() => resolve(true)));
  });
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

function assertCommandFailure(result, stderr) {
  assert.equal(result.killed, false, "the CLI watchdog killed the process");
  assert.equal(result.code, 1);
  assert.equal(result.stdout, "");
  assert.equal(result.stderr, stderr);
}

function assertSingleControl(record, port) {
  assert.equal(record.method, "POST");
  assert.equal(record.url, "/control");
  assert.equal(record.headers.host, `127.0.0.1:${port}`);
  assert.match(record.headers["content-type"], /^application\/json/);
}

function hermeticTests() {
  test("help and version stay offline and document control", { timeout: 30_000 }, async () => {
    const help = await runCli(["--help"]);
    assert.equal(help.code, 0);
    assert.equal(help.stderr, "");
    assert.match(
      help.stdout,
      /kitesurf-native control --owner NAME \[--takeover\] \[--port 8787\]/,
    );

    const version = await runCli(["--version"]);
    assert.equal(version.code, 0);
    assert.equal(version.stdout, "kitesurf 0.5.0\n");
    assert.equal(version.stderr, "");

    const controlHelp = await runCli(["control", "--help"]);
    assert.equal(controlHelp.code, 0);
    assert.match(controlHelp.stdout, /control --owner NAME/);
    assert.equal(controlHelp.stderr, "");

    const controlVersion = await runCli(["control", "--version"]);
    assert.equal(controlVersion.code, 0);
    assert.equal(controlVersion.stdout, "kitesurf 0.5.0\n");
    assert.equal(controlVersion.stderr, "");
  });

  test(
    "help and version are recognized before any control value",
    { timeout: 30_000 },
    async (t) => {
      const fixture = await listen(t, (record) => {
        record.response.writeHead(200, { "content-type": "application/json" });
        record.response.end(JSON.stringify({ controller: "fixture", controlEpoch: 2 }));
      });
      const port = String(fixture.port);
      const topLevelHelp = await runCli(["--help"]);
      assert.equal(topLevelHelp.code, 0);
      const help = topLevelHelp.stdout;
      assert.match(help, /^Kitesurf native local host 0\.5\.0\n/);
      assert.match(help, /kitesurf-native control --owner NAME \[--takeover\] \[--port 8787\]\n/);
      const version = "kitesurf 0.5.0\n";
      const cases = [
        ["--owner", "--help", "--port", port],
        ["--owner", "--version", "--port", port],
        ["--port", "--help"],
        ["--bogus", "--help"],
        ["-h"],
        ["--help", "--version"],
        ["--owner", "alice", "--version", "--help"],
      ];
      for (const args of cases) {
        const expected = args.includes("--version") ? version : help;
        const result = await runCli(["control", ...args]);
        const observed = fixture.seen.map(({ method, url, body }) => ({ method, url, body }));
        assert.equal(result.killed, false);
        assert.equal(result.code, 0, `${args.join(" ")}: ${result.stderr}`);
        assert.equal(result.stderr, "");
        assert.equal(result.stdout, expected, `${args.join(" ")} sent ${JSON.stringify(observed)}`);
      }
      assert.equal(fixture.seen.length, 0, "help/version must not reach the host");
    },
  );

  test("control sends one POST with takeover false by default", { timeout: 30_000 }, async (t) => {
    const response = { controller: "alice", controlEpoch: 2 };
    const fixture = await listen(t, (record) => {
      record.response.writeHead(200, { "content-type": "application/json" });
      record.response.end(JSON.stringify(response));
    });
    const result = await runCli(["control", "--owner", "alice", "--port", String(fixture.port)]);
    assert.equal(result.killed, false);
    assert.equal(result.code, 0);
    assert.equal(result.stderr, "");
    assert.equal(result.stdout, `${JSON.stringify(response)}\n`);
    assert.equal(fixture.seen.length, 1);
    assertSingleControl(fixture.seen[0], fixture.port);
    assert.equal(fixture.seen[0].body, '{"owner":"alice","takeover":false}');
  });

  test("--takeover sends true and forwards a Unicode owner", { timeout: 30_000 }, async (t) => {
    const owner = "控制者-α";
    const fixture = await listen(t, (record) => {
      record.response.writeHead(200, { "content-type": "application/json" });
      record.response.end(JSON.stringify({ controller: owner, controlEpoch: 3 }));
    });
    const result = await runCli([
      "control",
      "--owner",
      owner,
      "--takeover",
      "--port",
      String(fixture.port),
    ]);
    assert.equal(result.code, 0);
    assert.equal(result.stderr, "");
    assert.equal(JSON.parse(result.stdout).controller, owner);
    assert.equal(fixture.seen.length, 1);
    assertSingleControl(fixture.seen[0], fixture.port);
    assert.deepEqual(JSON.parse(fixture.seen[0].body), { owner, takeover: true });
  });

  test(
    "a 409 fails with empty stdout after exactly one request",
    { timeout: 30_000 },
    async (t) => {
      const body = '{"error":"Browser has another controller","controller":"alice"}';
      const fixture = await listen(t, (record) => {
        record.response.writeHead(409, { "content-type": "application/json" });
        record.response.end(body);
      });
      const result = await runCli(["control", "--owner", "bob", "--port", String(fixture.port)]);
      assertCommandFailure(result, `kitesurf: ${body}\n`);
      assert.equal(fixture.seen.length, 1);
      assertSingleControl(fixture.seen[0], fixture.port);
    },
  );

  test("a non-2xx response without a body reports its status", { timeout: 30_000 }, async (t) => {
    const fixture = await listen(t, (record) => {
      record.response.writeHead(503);
      record.response.end();
    });
    const result = await runCli(["control", "--owner", "alice", "--port", String(fixture.port)]);
    assertCommandFailure(result, "kitesurf: control request failed with status 503\n");
    assert.equal(fixture.seen.length, 1);
  });

  test("invalid arguments fail before any request", { timeout: 30_000 }, async (t) => {
    const fixture = await listen(t, () => {
      throw new Error("invalid arguments must not reach the host");
    });
    const cases = [
      [["control"], "kitesurf: control requires --owner NAME\n"],
      [["control", "--port", String(fixture.port)], "kitesurf: control requires --owner NAME\n"],
      [["control", "--owner"], "kitesurf: --owner requires a value\n"],
      [
        ["control", "--owner", "alice", "--port", String(fixture.port), "--bogus"],
        "kitesurf: unknown or incomplete argument: --bogus\n",
      ],
      [
        ["control", "--owner", "alice", "--port", "65536"],
        "kitesurf: --port must be an integer from 0 to 65535\n",
      ],
      [
        ["control", "--owner", "alice", "--port", "abc"],
        "kitesurf: --port must be an integer from 0 to 65535\n",
      ],
    ];
    for (const [args, stderr] of cases) {
      const result = await runCli(args);
      assertCommandFailure(result, stderr);
    }
    assert.equal(fixture.seen.length, 0);
  });

  test("a refused connection fails without stdout", { timeout: 30_000 }, async () => {
    const port = await freePort();
    const result = await runCli(["control", "--owner", "alice", "--port", String(port)]);
    assert.equal(result.killed, false);
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.match(
      result.stderr,
      new RegExp(`^kitesurf: cannot connect to 127\\.0\\.0\\.1:${port}:`),
    );
  });

  test("the default port is 8787", { timeout: 30_000 }, async (t) => {
    if (!(await portAvailable(8787))) {
      t.skip("127.0.0.1:8787 is in use; not probing an unrelated service");
      return;
    }
    const result = await runCli(["control", "--owner", "default-port-probe"]);
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.match(result.stderr, /^kitesurf: cannot connect to 127\.0\.0\.1:8787:/);
  });

  test("a truncated response fails with empty stdout and drops the socket", async (t) => {
    const fixture = await listen(t, (record) => {
      trackClose(record);
      record.response.writeHead(200, { "content-length": "64" });
      record.response.write("partial");
      record.response.socket.destroy();
    });
    const result = await runCli(["control", "--owner", "alice", "--port", String(fixture.port)]);
    assert.equal(result.killed, false);
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.match(result.stderr, /^kitesurf: /);
    assert.equal(fixture.seen.length, 1);
    await within(fixture.seen[0].closed, 5_000, "the client did not drop the truncated connection");
  });

  test(
    "a declared oversized response fails before reading its body",
    { timeout: 30_000 },
    async (t) => {
      const fixture = await listen(t, (record) => {
        trackClose(record);
        record.response.writeHead(200, { "content-length": String(8 * 1024 * 1024) });
        record.response.write("x");
      });
      const started = Date.now();
      const result = await runCli(["control", "--owner", "alice", "--port", String(fixture.port)]);
      assert.equal(result.code, 1);
      assert.equal(result.stdout, "");
      assert.equal(result.stderr, "kitesurf: control response exceeds 65536 bytes\n");
      assert.ok(Date.now() - started < 5_000);
      assert.equal(fixture.seen.length, 1);
      await within(fixture.seen[0].closed, 5_000, "the client did not drop the oversized response");
    },
  );

  test("a streamed oversized response fails at the bound", { timeout: 30_000 }, async (t) => {
    const fixture = await listen(t, (record) => {
      trackClose(record);
      record.response.writeHead(200, { "content-type": "text/plain" });
      record.response.write(Buffer.alloc(70 * 1024, 0x61));
    });
    const result = await runCli(["control", "--owner", "alice", "--port", String(fixture.port)]);
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(result.stderr, "kitesurf: control response exceeds 65536 bytes\n");
    assert.equal(fixture.seen.length, 1);
    await within(fixture.seen[0].closed, 5_000, "the client did not drop the oversized response");
  });

  test(
    "a stalled host hits the fixed deadline once and the client drops",
    { timeout: 60_000 },
    async (t) => {
      let observed;
      const requested = new Promise((resolve) => {
        observed = resolve;
      });
      const fixture = await listen(t, (record) => {
        trackClose(record);
        observed();
      });
      const started = Date.now();
      const { child, exit } = spawnCli([
        "control",
        "--owner",
        "stalled",
        "--port",
        String(fixture.port),
      ]);
      await within(requested, 10_000, "the control request was not observed");
      const command = execFileSync("ps", ["-p", String(child.pid), "-o", "command="], {
        encoding: "utf8",
      }).trim();
      assert.equal(basename(command.split(/\s+/)[0]), "kitesurf-native");
      assert.ok(!/miniflare/i.test(command));
      const result = await exit;
      const elapsed = Date.now() - started;
      assert.equal(result.killed, false);
      assert.equal(result.code, 1);
      assert.equal(result.stdout, "");
      assert.equal(
        result.stderr,
        `kitesurf: control request to 127.0.0.1:${fixture.port} timed out\n`,
      );
      assert.ok(elapsed >= 14_000, `the CLI exited after ${elapsed}ms, before the fixed deadline`);
      assert.ok(elapsed < 45_000, `the CLI exited after ${elapsed}ms`);
      assert.equal(fixture.seen.length, 1);
      await within(fixture.seen[0].closed, 5_000, "the client did not drop the stalled connection");
    },
  );

  test("SIGTERM cancels an in-flight request without retrying", { timeout: 30_000 }, async (t) => {
    let observed;
    const requested = new Promise((resolve) => {
      observed = resolve;
    });
    const fixture = await listen(t, (record) => {
      trackClose(record);
      observed();
    });
    const { child, exit } = spawnCli([
      "control",
      "--owner",
      "cancel",
      "--port",
      String(fixture.port),
    ]);
    await within(requested, 10_000, "the control request was not observed");
    child.kill("SIGTERM");
    const result = await exit;
    assert.equal(result.code, null);
    assert.equal(result.signal, "SIGTERM");
    assert.equal(result.stdout, "");
    await within(fixture.seen[0].closed, 5_000, "the killed client left the connection open");
    await delay(300);
    assert.equal(fixture.seen.length, 1);
  });
}

function engineTests() {
  test(
    "CLI control fences the previous CDP controller on a real engine",
    { timeout: 300_000 },
    async (t) => {
      const workerd = required("KITESURF_WORKERD");
      const configDir = required("KITESURF_CONFIG_DIR");
      const engineAssets = required("KITESURF_ENGINE_ASSETS");
      const assetsManifest = join(configDir, "assets-manifest.json");
      const directory = mkdtempSync(join(tmpdir(), "ks-native-cli-"));
      const profilePath = join(directory, "profile.sqlite");
      const seedState = {
        cookies: [
          {
            name: "seed",
            value: "disk",
            host: "127.0.0.1",
            path: "/",
            secure: false,
            httpOnly: false,
            sameSite: "Lax",
            hostOnly: true,
            expiresAt: null,
            createdAt: 1,
          },
        ],
        localStorage: [["http://seed.local", [["seed", "disk-value"]]]],
      };
      const { ProfileStore } = await import("../src/profile-store.mjs");
      const seeded = new ProfileStore(profilePath, { create: true });
      seeded.acquire("seed");
      seeded.save("seed", 1, seedState);
      seeded.close();

      const host = await startNativeHost({
        workerd,
        config: join(configDir, "local-private.capnp"),
        assets: engineAssets,
        assetsManifest,
        profile: profilePath,
      });
      let stopped = false;
      t.after(async () => {
        try {
          if (!stopped) await host.stop("SIGTERM", 20_000);
        } finally {
          rmSync(directory, { recursive: true, force: true });
        }
      });

      const initial = await host.request("/session");
      assert.equal(initial.status, 200);
      assert.equal(initial.body.controller, null);
      assert.equal(initial.body.controlEpoch, 1);
      assert.equal(initial.body.recovery, "profile-restored");
      assert.ok(initial.body.profile.revision >= 1);

      const alpha = await runCli(["control", "--owner", "cli-alpha", "--port", String(host.port)]);
      assert.equal(alpha.code, 0, alpha.stderr);
      assert.equal(alpha.stderr, "");
      assert.ok(alpha.stdout.endsWith("\n"));
      const alphaBody = JSON.parse(alpha.stdout);
      assert.equal(alphaBody.controller, "cli-alpha");
      assert.equal(alphaBody.controlEpoch, 2);

      const { connect } = await import("./helpers.mjs");
      const old = await connect(host.port, "?owner=cli-alpha");
      t.after(() => old.disconnect());
      const oldVersion = await old.send("Browser.getVersion");
      assert.equal(oldVersion.revision, "@kitesurf");

      const conflict = await runCli([
        "control",
        "--owner",
        "cli-beta",
        "--port",
        String(host.port),
      ]);
      assert.equal(conflict.code, 1);
      assert.equal(conflict.stdout, "");
      assert.match(conflict.stderr, /Browser has another controller/);
      assert.match(conflict.stderr, /cli-alpha/);
      const unchanged = await host.request("/session");
      assert.equal(unchanged.body.controller, "cli-alpha");
      assert.equal(unchanged.body.controlEpoch, 2);

      const takeover = await runCli([
        "control",
        "--owner",
        "cli-beta",
        "--takeover",
        "--port",
        String(host.port),
      ]);
      assert.equal(takeover.code, 0, takeover.stderr);
      const takeoverBody = JSON.parse(takeover.stdout);
      assert.equal(takeoverBody.controller, "cli-beta");
      assert.equal(takeoverBody.controlEpoch, 3);
      const after = await host.request("/session");
      assert.equal(after.body.controller, "cli-beta");
      assert.equal(after.body.controlEpoch, 3);

      await assert.rejects(
        old.send("Browser.getVersion"),
        /Control has changed; reconnect and observe before acting/,
      );
      const next = await connect(host.port, "?owner=cli-beta");
      t.after(() => next.disconnect());
      const nextVersion = await next.send("Browser.getVersion");
      assert.equal(nextVersion.revision, "@kitesurf");

      await old.disconnect();
      await next.disconnect();

      const flushed = await host.request("/flush", { method: "POST" });
      assert.equal(flushed.status, 200);
      assert.ok(flushed.body.revision >= 1);

      const runDir = host.runDir();
      assert.ok(runDir, "the host run directory was not observed");
      const command = execFileSync("ps", ["-p", String(host.pid), "-o", "command="], {
        encoding: "utf8",
      }).trim();
      assert.equal(basename(command.split(/\s+/)[0]), "kitesurf-native");
      assert.ok(!/miniflare/i.test(command));
      const engineStart = readEvents(host.observerPath).find(
        (event) => event.kind === "workerd-start",
      );
      assert.ok(engineStart && Number.isInteger(engineStart.pid), "workerd pid was not observed");

      const status = await host.stop("SIGTERM", 20_000);
      stopped = true;
      assert.equal(status.code, 0);
      assert.equal(status.signal, null);
      assert.doesNotMatch(host.stderr, /uncaught|internal[\s_-]+error/i);

      const events = readEvents(host.observerPath);
      assert.equal(
        events.some((event) => event.kind === "engine-diagnostic"),
        false,
      );
      const engineExit = events.find((event) => event.kind === "engine-exit");
      assert.ok(engineExit, "the engine exit event was not recorded");
      assert.equal(engineExit.reaped, true);
      assert.equal(engineExit.fatal, false);
      assert.equal(engineExit.uncaught, 0);
      assert.equal(engineExit.internalErrors, 0);
      const shutdown = events.find((event) => event.kind === "shutdown");
      assert.ok(shutdown, "the host shutdown event was not recorded");
      assert.equal(shutdown.clean, true);
      assert.equal(shutdown.fonts, true);
      assert.equal(shutdown.dirRemoved, true);
      assert.equal(shutdown.exit, "code 0");

      assert.equal(existsSync(runDir), false, "the host run directory survived shutdown");
      assert.equal(processExists(engineStart.pid), false, "workerd was not reaped");
      assert.equal(await portAvailable(host.port), true, "the host port was not released");

      const reopened = new ProfileStore(profilePath);
      const saved = reopened.read();
      reopened.close();
      assert.ok(saved.revision >= 1);
      assert.equal(saved.state.cookies.find((cookie) => cookie.name === "seed").value, "disk");
      const storage = saved.state.localStorage.find(([origin]) => origin === "http://seed.local");
      assert.equal(Object.fromEntries(storage[1]).seed, "disk-value");
      const statusResult = await runCli(["profile", "status", profilePath]);
      assert.equal(statusResult.code, 0, statusResult.stderr);
      assert.equal(JSON.parse(statusResult.stdout).revision, saved.revision);
    },
  );
}

if (scenario === "hermetic") hermeticTests();
else if (scenario === "engine") engineTests();
else throw new Error("KITESURF_CLI_SCENARIO must be hermetic or engine");
