// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { describe, it, expect, vi } from "vitest";
import {
  directExternalFetch,
  fetchParserImage,
  type ExternalOutbound,
} from "../src/eval/external-outbound.ts";

function outbound(reply: Response) {
  const seen: Request[] = [];
  const stub: ExternalOutbound & { seen: Request[] } = {
    seen,
    async fetch(req) {
      seen.push(req);
      return reply.clone();
    },
  };
  return stub;
}

describe("directExternalFetch", () => {
  it("uses the host egress hook exclusively when present", async () => {
    const stub = outbound(new Response("h1{}", { status: 200 }));
    const resp = await directExternalFetch(
      stub,
      new Request("https://127.0.0.1:1/app.css", {
        headers: { accept: "text/css" },
      }),
    );
    expect(await resp.text()).toBe("h1{}");
    expect(stub.seen[0]!.url).toBe("https://127.0.0.1:1/app.css");
    expect(stub.seen[0]!.headers.get("accept")).toBe("text/css");
  });

  it("passes the host's error response through untouched", async () => {
    const stub = outbound(
      new Response("unavailable", {
        status: 503,
        headers: { "x-host-reason": "unavailable" },
      }),
    );
    const resp = await directExternalFetch(stub, new Request("https://unreachable.example/"));
    expect(resp.status).toBe(503);
    expect(resp.headers.get("x-host-reason")).toBe("unavailable");
  });

  it("uses plain fetch when no host egress hook exists", async () => {
    const request = new Request("https://page.example/app.css");
    const reply = new Response("h1{}");
    const native = vi.fn().mockResolvedValue(reply);
    vi.stubGlobal("fetch", native);
    try {
      expect(await directExternalFetch(undefined, request)).toBe(reply);
      expect(native.mock.calls).toEqual([[request]]);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});

describe("fetchParserImage", () => {
  const requestUrl = "https://page.example/cover.png";

  it("retries a native disconnect under the same signal", async () => {
    const request = new Request(requestUrl);
    const native = vi
      .fn()
      .mockRejectedValueOnce(new Error("Network connection lost."))
      .mockResolvedValueOnce(new Response("ok"));
    vi.stubGlobal("fetch", native);
    try {
      expect(await (await fetchParserImage(request)).text()).toBe("ok");
      expect(native.mock.calls).toEqual([[request], [request]]);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("keeps a second internal error from restarting the page retry", async () => {
    const error = new Error("internal error; fixture");
    const native = vi
      .fn()
      .mockRejectedValueOnce(new Error("Network connection lost."))
      .mockRejectedValueOnce(error);
    vi.stubGlobal("fetch", native);
    try {
      await expect(fetchParserImage(new Request(requestUrl))).rejects.toMatchObject({
        message: `Parser image retry failed: ${error.message}`,
        cause: error,
      });
      expect(native).toHaveBeenCalledTimes(2);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("lets the page handle an internal error on the first attempt", async () => {
    const error = new Error("internal error; fixture");
    const native = vi.fn().mockRejectedValue(error);
    vi.stubGlobal("fetch", native);
    try {
      await expect(fetchParserImage(new Request(requestUrl))).rejects.toBe(error);
      expect(native).toHaveBeenCalledTimes(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it("does not retry a disconnect after the page lifetime ends", async () => {
    const lifetime = new AbortController();
    const native = vi.fn(async () => {
      lifetime.abort();
      throw new Error("Network connection lost.");
    });
    vi.stubGlobal("fetch", native);
    try {
      const request = new Request(requestUrl, { signal: lifetime.signal });
      await expect(fetchParserImage(request)).rejects.toThrow("Network connection lost.");
      expect(native).toHaveBeenCalledTimes(1);
    } finally {
      vi.unstubAllGlobals();
    }
  });
});
