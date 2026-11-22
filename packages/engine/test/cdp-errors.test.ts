// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { describe, expect, it } from "vitest";

import { CdpCommandError, retryableContextDestroyed } from "../src/cdp/errors.ts";

describe("CDP command errors", () => {
  it("marks a replaced isolate's evaluate as retryable", () => {
    const error = retryableContextDestroyed(1);
    expect(error).toBeInstanceOf(CdpCommandError);
    expect(error.code).toBe(-32000);
    expect(error.message).toBe("Execution context was destroyed.");
    expect(error.data).toEqual({
      retryable: true,
      reason: "execution-context-destroyed",
      executionContextId: 1,
    });
  });

  it("omits an unpublished execution context id", () => {
    expect(retryableContextDestroyed(0).data).toEqual({
      retryable: true,
      reason: "execution-context-destroyed",
    });
  });
});
