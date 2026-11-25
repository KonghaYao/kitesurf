// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, it } from "vitest";
import { buildDocument, callModelContextTool } from "../src/index.js";
import { acquireModelContext } from "../src/model-context.js";

// wasm DomHandle 有 165 个成员；本测试只使用这两个。替身在工厂边界收窄，避免链式断言。
type BuildDocumentHost = Parameters<typeof buildDocument>[0];

function fakeHost(): BuildDocumentHost {
  const host: Partial<BuildDocumentHost> = {
    documentNodeId: () => 0,
    querySelectorAll: () => new Int32Array(0),
  };
  return host as BuildDocumentHost;
}

const host = fakeHost();

it("keeps prototype enumerability observable to a prior document's abort callback", async () => {
  const globalPrototype = Object.getPrototypeOf(globalThis);
  const options = { pageUrl: "https://a.test/", initialCookieString: "" };
  try {
    const first = buildDocument(host, options);
    const proto = first.Node.prototype;
    const descriptor = (key: string) => Object.getOwnPropertyDescriptor(proto, key);
    expect(descriptor("__insertAdjacentHTML")?.enumerable).toBe(true);
    expect(descriptor("constructor")?.enumerable).toBe(false);
    expect(
      Object.getOwnPropertyDescriptor(first.Text.prototype, "_refreshTextInfo")?.enumerable,
    ).toBe(false);

    const observed: unknown[] = [];
    const getter = () => observed.push("accessor invoked");
    const method = function () {};
    Object.defineProperty(method, "name", {
      configurable: true,
      get() {
        observed.push("method wrapped");
        return "patchedMethod";
      },
    });
    Object.defineProperties(proto, {
      patchedMethod: { value: method, configurable: true },
      _accessor: { get: getter, configurable: true },
      _fixed: { value: 1 },
    });
    await acquireModelContext(first.document).registerTool({
      name: "wait-for-navigation",
      description: "Observe teardown",
      execute: (_args, options?: WebMCP.ToolExecuteCallbackOptions) => {
        const signal = options?.signal;
        if (!signal) throw new Error("Expected a signal for the active tool invocation");
        return new Promise<string>((resolve) => {
          signal.addEventListener("abort", () => {
            observed.push([
              descriptor("patchedMethod")?.enumerable,
              descriptor("_accessor")?.enumerable,
              descriptor("_fixed")?.enumerable,
              descriptor("constructor")?.enumerable,
            ]);
            resolve("aborted");
          });
        });
      },
    });
    const pending = callModelContextTool("wait-for-navigation", {}, "navigation");
    buildDocument(host, options);
    expect(await pending).toMatchObject({ isError: false });
    expect(observed).toEqual([[true, true, false, false], "method wrapped"]);
    expect(descriptor("_accessor")).toMatchObject({
      configurable: true,
      enumerable: true,
      get: getter,
    });
    expect(descriptor("patchedMethod")?.value).not.toBe(method);
  } finally {
    Object.setPrototypeOf(globalThis, globalPrototype);
  }
});
