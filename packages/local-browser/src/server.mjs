// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { createServer } from "node:http";
import { readFileSync, readdirSync } from "node:fs";
import { resolve, relative } from "node:path";
import { randomUUID } from "node:crypto";
import { once } from "node:events";
import WebSocket, { WebSocketServer } from "ws";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { Attachments } from "./attachments.mjs";

const VIEW_METHODS = new Set([
  "Browser.getVersion",
  "Target.getTargets",
  "Target.getTargetInfo",
  "Target.attachToTarget",
  "Target.detachFromTarget",
  "Target.setDiscoverTargets",
  "Page.enable",
  "Page.disable",
  "Page.captureScreenshot",
  "Page.startScreencast",
  "Page.stopScreencast",
  "Page.screencastFrameAck",
  "Page.getFrameTree",
  "Page.getLayoutMetrics",
  "DOM.getDocument",
  "DOM.getOuterHTML",
  "DOM.querySelector",
  "DOM.querySelectorAll",
  "Accessibility.getFullAXTree",
  "Kitesurf.getSessionInfo",
]);

function modulesAt(root) {
  const result = [];
  function visit(dir) {
    for (const entry of readdirSync(dir, { withFileTypes: true })) {
      const path = resolve(dir, entry.name);
      if (entry.isDirectory()) {
        visit(path);
        continue;
      }
      const name = relative(root, path);
      const type = name.endsWith(".wasm")
        ? "CompiledWasm"
        : /\.(glue\.js|css|dat)$/.test(name)
          ? "Text"
          : /\.(woff2|bin)$/.test(name)
            ? "Data"
            : /\.js$/.test(name)
              ? "ESModule"
              : null;
      if (type) result.push({ type, path });
    }
  }
  visit(root);
  return result.sort((a, b) =>
    a.path.endsWith("/index.js")
      ? -1
      : b.path.endsWith("/index.js")
        ? 1
        : a.path.localeCompare(b.path),
  );
}

/** @returns {Promise<{
 * port: number, generation: string, done: Promise<string>,
 * status: () => Promise<object>, close: () => Promise<void>
 * }>} */
export async function startBrowser({
  profile,
  dist,
  assets,
  port = 8787,
  allowPrivateNetwork = false,
}) {
  const restored = profile.status().revision > 0;
  process.env.MINIFLARE_WORKERD_V8_FLAGS ??= "--expose-gc";
  const base = JSON.parse(readFileSync(resolve(dist, "engine-mf.json"), "utf8"));
  const mf = new Miniflare(
    convertV4MiniflareOptions({
      host: "127.0.0.1",
      port: 0,
      cf: false,
      workers: [
        {
          ...base,
          name: "kitesurf-local",
          modulesRoot: dist,
          modules: modulesAt(dist),
          assets: { ...base.assets, directory: assets },
          bindings: {
            ...base.bindings,
            KITESURF_ENV: "local",
            KITESURF_VERSION: "0.5.0",
            KITESURF_RESOURCE_DRAIN_QUIET_TURNS: "5",
            KITESURF_ALLOW_PRIVATE_NETWORK: allowPrivateNetwork ? "1" : "",
          },
          serviceBindings: { LOCAL_PROFILE: (request) => profile.fetch(request) },
        },
      ],
    }),
  );
  let upstream;
  let http;
  try {
    const address = await mf.ready;
    const engineUrl = new URL("/devtools/browser", address);
    engineUrl.protocol = "ws:";
    upstream = new WebSocket(engineUrl);
    await once(upstream, "open");
    upstream.on("error", (error) => console.error(`Browser transport failed: ${error.message}`));
    let nextId = 1;
    const pending = new Map();
    const clients = new Set();
    const pageSessions = new Map();
    const inflight = new Set();
    const generation = randomUUID();
    let controller = null;
    let controlEpoch = 1;
    let activeController = null;
    let closing = false;
    let alive = true;
    let dispose;
    let disposal;
    let finished;
    const done = new Promise((resolve) => {
      finished = resolve;
    });
    const rejectPending = () => {
      for (const waiting of pending.values()) {
        clearTimeout(waiting.timer);
        waiting.reject(new Error("Browser session ended; outcome may be unknown"));
      }
      pending.clear();
    };
    upstream.on("close", () => {
      const wasAlive = alive;
      alive = false;
      rejectPending();
      if (!wasAlive || closing) return;
      console.error(
        "Browser engine stopped unexpectedly. Live pages are lost; saved profile remains.",
      );
      void dispose?.("engine-lost").catch((error) => console.error(error.message));
    });
    const send = (method, params, sessionId) =>
      new Promise((resolve, reject) => {
        if (!alive || upstream.readyState !== WebSocket.OPEN) {
          reject(new Error("Browser session is lost; restart explicitly"));
          return;
        }
        const id = nextId++;
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`CDP timeout: ${method}; outcome may be unknown`));
        }, 120_000);
        pending.set(id, { resolve, reject, timer });
        upstream.send(JSON.stringify({ id, method, params, sessionId }));
      });
    const attachments = new Attachments(send);
    upstream.on("message", (data) => {
      const message = JSON.parse(data.toString());
      if (message.id !== undefined) {
        const waiting = pending.get(message.id);
        if (!waiting) return;
        pending.delete(message.id);
        clearTimeout(waiting.timer);
        if (message.error)
          waiting.reject(
            Object.assign(new Error(message.error.message), { code: message.error.code }),
          );
        else waiting.resolve(message.result);
      } else {
        attachments.event(message);
        for (const client of clients) {
          if (client.readyState !== WebSocket.OPEN) continue;
          if (!attachments.visible(client, message)) continue;
          if (!pageSessions.has(client)) {
            client.send(JSON.stringify(message));
            continue;
          }
          if (message.sessionId !== pageSessions.get(client)) continue;
          const { sessionId: _sessionId, ...event } = message;
          client.send(JSON.stringify(event));
        }
      }
    });
    const session = await send("Kitesurf.getSessionInfo");
    const sessionInfo = async () => ({
      version: "0.5.0",
      sessionId: session.sessionId,
      generation,
      alive,
      recovery: restored ? "profile-restored" : "new-profile",
      profile: profile.status(),
      controller,
      controlEpoch,
      tabs: (await send("Target.getTargets")).targetInfos.filter((t) => t.type === "page"),
    });
    const shutdown = async (beforeDispose) => {
      if (closing) throw new Error("Browser is already closing");
      closing = true;
      try {
        await Promise.all([...inflight]);
        await attachments.barrier;
        await send("Kitesurf.flushProfile");
        await send("Browser.close");
      } catch (error) {
        if (!alive || upstream.readyState !== WebSocket.OPEN) await dispose("engine-lost");
        else closing = false;
        throw error;
      }
      try {
        beforeDispose?.();
      } finally {
        await dispose();
      }
    };
    dispose = (reason = "closed") => {
      disposal ??= (async () => {
        alive = false;
        rejectPending();
        for (const client of clients) client.close(1000, "Browser closed");
        upstream.close();
        http?.close();
        http?.closeIdleConnections();
        try {
          await mf.dispose();
        } finally {
          try {
            profile.close();
          } finally {
            finished(reason);
          }
        }
      })();
      return disposal;
    };
    const allowed = (request) => {
      const host = request.headers.host;
      if (host !== `127.0.0.1:${port}` && host !== `localhost:${port}`) return false;
      const origin = request.headers.origin;
      return (
        !origin ||
        origin === `http://${host}` ||
        origin === "devtools://devtools" ||
        origin === "https://chrome-devtools-frontend.appspot.com"
      );
    };
    const jsonBody = async (request) => {
      let body = "";
      for await (const chunk of request) {
        body += chunk;
        if (body.length > 65536) throw new Error("Request too large");
      }
      return body ? JSON.parse(body) : {};
    };
    http = createServer(async (request, response) => {
      const reply = (status, value) => {
        response.writeHead(status, {
          "content-type": "application/json",
          "cache-control": "no-store",
        });
        response.end(JSON.stringify(value));
      };
      if (!allowed(request)) {
        reply(403, { error: "Local browser origin rejected" });
        return;
      }
      const url = new URL(request.url, `http://127.0.0.1:${port}`);
      try {
        if (request.method === "GET" && url.pathname === "/health")
          return reply(200, { alive, version: "0.5.0" });
        if (request.method === "GET" && url.pathname === "/session")
          return reply(200, await sessionInfo());
        if (request.method === "GET" && url.pathname.replace(/\/$/, "") === "/json/version")
          return reply(200, {
            Browser: "Kitesurf/0.5.0",
            "Protocol-Version": "1.3",
            webSocketDebuggerUrl: `ws://127.0.0.1:${port}/devtools/browser`,
          });
        if (
          request.method === "GET" &&
          ["/json", "/json/list"].includes(url.pathname.replace(/\/$/, ""))
        ) {
          const info = await sessionInfo();
          return reply(
            200,
            info.tabs.map((t) => ({
              id: t.targetId,
              type: "page",
              title: t.title,
              url: t.url,
              webSocketDebuggerUrl: `ws://127.0.0.1:${port}/devtools/page/${t.targetId}`,
              devtoolsFrontendUrl: `devtools://devtools/bundled/inspector.html?ws=127.0.0.1:${port}/devtools/page/${t.targetId}`,
            })),
          );
        }
        if (request.method === "POST" && url.pathname === "/flush") {
          await send("Kitesurf.flushProfile");
          return reply(200, profile.status());
        }
        if (request.method === "POST" && url.pathname === "/backup") {
          const body = await jsonBody(request);
          await send("Kitesurf.flushProfile");
          return reply(200, { path: profile.backup(body.path) });
        }
        if (request.method === "POST" && url.pathname === "/control") {
          if (closing) throw new Error("Browser is closing");
          const body = await jsonBody(request);
          if (typeof body.owner !== "string" || !body.owner || body.owner.length > 128)
            throw new Error("A short owner name is required");
          if (controller && controller !== body.owner && !body.takeover)
            return reply(409, { error: "Browser has another controller", controller });
          controller = body.owner;
          controlEpoch++;
          const previous = activeController;
          activeController = null;
          if (previous) await attachments.retire(previous);
          return reply(200, { controller, controlEpoch });
        }
        if (request.method === "POST" && url.pathname === "/close") {
          await shutdown(() => reply(200, { closed: true }));
          return;
        }
        if (request.method === "GET" && url.pathname === "/")
          return reply(200, {
            browser: "Kitesurf 0.5.0",
            session: "/session",
            cdp: `ws://127.0.0.1:${port}/devtools/browser`,
            inspect: "chrome://inspect — configure this address",
            profile: profile.path,
          });
        reply(404, { error: "Not found" });
      } catch (error) {
        reply(409, { error: error.message });
      }
    });
    const wss = new WebSocketServer({ noServer: true, maxPayload: 16 * 1024 * 1024 });
    http.on("upgrade", (request, socket, head) => {
      const url = new URL(request.url, `http://127.0.0.1:${port}`);
      const reject = (message) => {
        socket.end(`HTTP/1.1 409 Conflict\r\nConnection: close\r\n\r\n${message}`);
      };
      if (
        !allowed(request) ||
        closing ||
        !alive ||
        !/^\/devtools\/(browser|page\/[^/]+)$/.test(url.pathname)
      )
        return reject("Browser unavailable");
      if (url.searchParams.has("session") && url.searchParams.get("session") !== session.sessionId)
        return reject("Session expired");
      const viewer = url.searchParams.get("role") === "viewer";
      const owner = url.searchParams.get("owner") || "default";
      if (owner.length > 128) return reject("Owner name is too long");
      if (!viewer) {
        if (controller && controller !== owner)
          return reject("Another controller owns the browser; explicitly take control");
        if (activeController?.readyState === WebSocket.OPEN)
          return reject("Controller is already connected");
        controller = owner;
      }
      const epoch = controlEpoch;
      wss.handleUpgrade(request, socket, head, (client) => {
        clients.add(client);
        attachments.add(client);
        client.on("error", () => client.terminate());
        if (!viewer) activeController = client;
        client.on("close", () => {
          clients.delete(client);
          pageSessions.delete(client);
          if (alive && !closing) void attachments.retire(client);
          if (activeController === client) activeController = null;
        });
        let pageReady = Promise.resolve(undefined);
        if (url.pathname.startsWith("/devtools/page/")) {
          pageSessions.set(client, null);
          pageReady = attachments
            .call(client, "Target.attachToTarget", {
              targetId: url.pathname.split("/").at(-1),
              flatten: true,
            })
            .then((result) => {
              if (clients.has(client)) pageSessions.set(client, result.sessionId);
              return result.sessionId;
            });
          void pageReady.catch(() => client.close(1008, "Target expired"));
        }
        client.on("message", async (data) => {
          let request;
          try {
            request = JSON.parse(data.toString());
            const pageSession = await pageReady;
            if (!Number.isSafeInteger(request.id) || typeof request.method !== "string")
              throw new Error("Invalid CDP request");
            if (closing) throw new Error("Browser is closing");
            if (viewer && !VIEW_METHODS.has(request.method))
              throw new Error("Viewer connection cannot control the browser");
            if (
              !viewer &&
              (controller !== owner || epoch !== controlEpoch || activeController !== client)
            )
              throw new Error("Control has changed; reconnect and observe before acting");
            if (request.method === "Browser.close") {
              await shutdown(() => client.send(JSON.stringify({ id: request.id, result: {} })));
              return;
            }
            const promise = attachments.call(
              client,
              request.method,
              request.params,
              request.sessionId ?? pageSession,
            );
            inflight.add(promise);
            let result;
            try {
              result = await promise;
            } finally {
              inflight.delete(promise);
            }
            if (client.readyState === WebSocket.OPEN)
              client.send(JSON.stringify({ id: request.id, result, sessionId: request.sessionId }));
          } catch (error) {
            if (client.readyState === WebSocket.OPEN)
              client.send(
                JSON.stringify({
                  id: request?.id ?? 0,
                  error: { code: error.code ?? -32000, message: error.message },
                  sessionId: request?.sessionId,
                }),
              );
          }
        });
      });
    });
    http.listen(port, "127.0.0.1");
    await once(http, "listening");
    port = http.address().port;
    return { port, generation, done, status: sessionInfo, close: shutdown };
  } catch (error) {
    upstream?.close();
    http?.close();
    await mf.dispose();
    throw error;
  }
}
