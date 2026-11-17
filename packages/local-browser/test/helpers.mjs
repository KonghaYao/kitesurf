// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { createServer } from "node:http";
import { once } from "node:events";
import { setTimeout as delay } from "node:timers/promises";
import WebSocket from "ws";

/** @returns {Promise<{
 * ws: WebSocket, disconnect: () => Promise<void>,
 * send: (method: string, params?: object, sessionId?: string) => Promise<object>
 * }>} */
export async function connect(port, query = "", path = "/devtools/browser") {
  const ws = new WebSocket(`ws://127.0.0.1:${port}${path}${query}`);
  const pending = new Map();
  let next = 1;
  ws.on("message", (data) => {
    const result = JSON.parse(data.toString());
    const waiting = pending.get(result.id);
    if (!waiting) return;
    pending.delete(result.id);
    clearTimeout(waiting.timer);
    if (result.error) waiting.reject(new Error(result.error.message));
    else waiting.resolve(result.result);
  });
  ws.on("close", () => {
    for (const waiting of pending.values()) {
      clearTimeout(waiting.timer);
      waiting.reject(new Error("CDP connection closed"));
    }
    pending.clear();
  });
  await once(ws, "open");
  return {
    ws,
    send(method, params = {}, sessionId) {
      return new Promise((resolve, reject) => {
        const id = next++;
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`Timeout: ${method}`));
        }, 45_000);
        pending.set(id, { resolve, reject, timer });
        ws.send(JSON.stringify({ id, method, params, sessionId }));
      });
    },
    async disconnect() {
      if (ws.readyState === WebSocket.CLOSED) return;
      const closed = once(ws, "close");
      ws.close();
      await closed;
    },
  };
}

/** @returns {Promise<unknown>} */
export async function evaluate(cdp, sessionId, expression) {
  const result = await cdp.send(
    "Runtime.evaluate",
    { expression, returnByValue: true, awaitPromise: true },
    sessionId,
  );
  if (result.exceptionDetails) throw new Error(JSON.stringify(result.exceptionDetails));
  return result.result.value;
}

/** @template T @param {() => Promise<T>} check @returns {Promise<T>} */
export async function until(check, description, timeout = 20_000) {
  const start = Date.now();
  let last;
  while (Date.now() - start < timeout) {
    try {
      const value = await check();
      if (value) return value;
    } catch (error) {
      last = error;
    }
    await delay(100);
  }
  throw new Error(`Timed out waiting for ${description}: ${last?.message || "condition false"}`);
}

/** @returns {Promise<{ targetId: string, sessionId: string }>} */
export async function openPage(cdp, url, browserContextId) {
  const { targetId } = await cdp.send("Target.createTarget", {
    url: "about:blank",
    browserContextId,
  });
  const { sessionId } = await cdp.send("Target.attachToTarget", { targetId, flatten: true });
  await cdp.send("Page.enable", {}, sessionId);
  await navigate(cdp, sessionId, url);
  return { targetId, sessionId };
}

/** @returns {Promise<void>} */
export async function navigate(cdp, sessionId, url) {
  const result = await cdp.send("Page.navigate", { url }, sessionId);
  if (result.errorText) throw new Error(result.errorText);
  await until(
    () => evaluate(cdp, sessionId, `globalThis.boot && location.href === ${JSON.stringify(url)}`),
    `navigation to ${url}`,
  );
}

/** @returns {Promise<{ url: string, seen: object[], close: () => Promise<void> }>} */
export async function fixtureServer() {
  const seen = [];
  const server = createServer((request, response) => {
    seen.push({ path: request.url, cookie: request.headers.cookie || "", method: request.method });
    if (request.url === "/login" || request.url === "/rotate") {
      const token = request.url === "/login" ? "T1" : "T2";
      response.writeHead(200, {
        "content-type": "application/json",
        "set-cookie": `sid=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=3600`,
      });
      response.end(JSON.stringify({ token }));
      return;
    }
    if (request.url === "/logout") {
      response.writeHead(200, { "set-cookie": "sid=; Path=/; HttpOnly; Max-Age=0" });
      response.end("ok");
      return;
    }
    response.writeHead(200, {
      "content-type": "text/html; charset=utf-8",
      "cache-control": "no-store",
    });
    response.end(`<!doctype html><html><head><style>
      body { margin: 0; background: #eef3f8; font: 20px sans-serif; color: #182b42; }
      header { background: #245fd3; color: white; padding: 32px; }
      main { padding: 32px; } input, button { display: block; padding: 12px; margin: 12px 0; }
      button { background: #ffbb55; border: 0; } #result { background: white; padding: 20px; }
      </style><script>
      window.boot = { token: localStorage.getItem('token'), step: sessionStorage.getItem('step'), cookie: document.cookie,
        authenticated: ${JSON.stringify(request.headers.cookie?.includes("sid=") || false)} };
      window.events = [];
      addEventListener('storage', function(e) { events.push({key:e.key,oldValue:e.oldValue,newValue:e.newValue}); });
      </script></head><body><header>Kitesurf persistent account</header><main>
      <label>Username<input id="username"></label><label>Password<input id="password" type="password"></label>
      <button id="login">Log in</button><input id="draft" placeholder="Unsaved draft"><div id="result">Signed out</div>
      <script>document.getElementById('login').onclick = async function() {
        const response = await fetch('/login', {method:'POST'}); const data = await response.json();
        localStorage.setItem('token', data.token); sessionStorage.setItem('step', 'checkout');
        document.cookie = 'displayName=Alice; Path=/; SameSite=Lax; Max-Age=3600';
        document.getElementById('result').textContent = 'Welcome Alice'; window.loggedIn = true;
      };</script></main></body></html>`);
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return {
    url: `http://127.0.0.1:${server.address().port}`,
    seen,
    close: () =>
      new Promise((resolve) => {
        server.close(resolve);
        server.closeAllConnections();
      }),
  };
}
