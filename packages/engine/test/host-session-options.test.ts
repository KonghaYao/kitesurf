// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { describe, expect, it } from "vitest";

import { resolveConnectionOptions } from "../src/host.ts";

const deadlines = { idleTimeoutMs: 600_000, maxSessionMs: 3_600_000 };

describe("resolveConnectionOptions", () => {
  it("uses generic defaults based on the WebSocket path", () => {
    expect(
      resolveConnectionOptions(
        new Request("https://kitesurf.invalid/devtools/page/kitesurf"),
        "connection-1",
      ),
    ).toEqual({ ...deadlines, entrypoint: "devtools", fontDefer: false });
    expect(
      resolveConnectionOptions(
        new Request("https://kitesurf.invalid/devtools/browser"),
        "connection-2",
      ),
    ).toEqual({ ...deadlines, entrypoint: "puppeteer", fontDefer: false });
  });

  it("ignores internal session headers on the public default host", () => {
    const request = new Request("https://kitesurf.invalid/devtools/browser", {
      headers: {
        "x-kitesurf-entrypoint": "html",
        "x-kitesurf-font-defer": "1",
      },
    });

    expect(resolveConnectionOptions(request, "connection-3")).toEqual({
      ...deadlines,
      entrypoint: "puppeteer",
      fontDefer: false,
    });
  });

  it("uses host-owned entrypoint and font options", () => {
    const request = new Request("https://kitesurf.invalid/devtools/page/kitesurf");

    let receivedRequest: Request | undefined;
    let receivedConnectionId: string | undefined;
    expect(
      resolveConnectionOptions(request, "connection-4", {
        connectionOptions: (received, connectionId) => {
          receivedRequest = received;
          receivedConnectionId = connectionId;
          return { entrypoint: "host-action", fontDefer: true };
        },
      }),
    ).toEqual({ ...deadlines, entrypoint: "host-action", fontDefer: true });
    expect(receivedRequest).toBe(request);
    expect(receivedConnectionId).toBe("connection-4");
  });

  it("keeps the generic entrypoint when the host only supplies font options", () => {
    const request = new Request("https://kitesurf.invalid/devtools/page/kitesurf");

    expect(
      resolveConnectionOptions(request, "connection-5", {
        connectionOptions: () => ({ fontDefer: true }),
      }),
    ).toEqual({ ...deadlines, entrypoint: "devtools", fontDefer: true });
  });

  it("allows a trusted local host to remove session deadlines", () => {
    const options = resolveConnectionOptions(
      new Request("http://localhost/devtools/browser"),
      "local",
      {
        connectionOptions: () => ({ idleTimeoutMs: 0, maxSessionMs: 0 }),
      },
    );
    expect(options.idleTimeoutMs).toBe(0);
    expect(options.maxSessionMs).toBe(0);
  });
});
