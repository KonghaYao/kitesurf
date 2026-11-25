// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { expect, it } from "vitest";
import { finalizeInterface, idlInterfaceMeta, markBrandWrapped } from "../src/webidl.js";

it("sets the arity of a function on a nonconfigurable property", () => {
  class Example {
    fixed(value: number) {
      return value;
    }
  }
  const fixed = Example.prototype.fixed;
  Object.defineProperty(Example.prototype, "fixed", { configurable: false });
  finalizeInterface(Example, { name: "Example", brandOps: true, methodLengths: { fixed: 0 } });

  expect(Example.prototype.fixed).toBe(fixed);
  expect(Object.getOwnPropertyDescriptor(Example.prototype, "fixed")).toEqual({
    value: fixed,
    writable: true,
    enumerable: false,
    configurable: false,
  });
  expect(Object.getOwnPropertyDescriptor(fixed, "length")).toEqual({
    value: 0,
    writable: false,
    enumerable: false,
    configurable: true,
  });
});

it("keeps the arity and identity of branded wrappers on repeat finalization", async () => {
  class Example {
    async operation(value = 0) {
      return value;
    }
    static from(value = 0) {
      return value;
    }
  }
  const meta = {
    name: "Example",
    brandOps: true,
    methodLengths: { operation: 1 },
    staticMethodLengths: { from: 1 },
    promiseOps: ["operation"],
  };
  finalizeInterface(Example, meta);
  const operation = Example.prototype.operation;
  const from = Example.from;
  expect(operation.length).toBe(1);
  expect(from.length).toBe(1);
  await expect(operation.call(new Example(), 3)).resolves.toBe(3);
  await expect(Reflect.apply(operation, {}, [3])).rejects.toThrow("Illegal invocation");
  await expect(operation.call(new Example())).rejects.toThrow("1 argument required");
  expect(() => from()).toThrow("1 argument required");

  finalizeInterface(Example, meta);
  expect(Example.prototype.operation).toBe(operation);
  expect(Example.from).toBe(from);
  expect(Object.getOwnPropertyDescriptor(Example.prototype, "operation")?.enumerable).toBe(true);
});

it("uses inherited arity for an own method and retains metadata order for aliases", () => {
  class Example {
    appendChild(value = 0) {
      return value;
    }
    first() {}
    second() {}
  }
  const shared = Example.prototype.first;
  markBrandWrapped(shared);
  Example.prototype.second = shared;
  finalizeInterface(
    Example,
    idlInterfaceMeta("HTMLButtonElement", {
      brandOps: true,
      methodLengths: { second: 2, first: 1 },
    }),
  );

  const element = new Example();
  expect(element.appendChild.length).toBe(1);
  expect(() => element.appendChild()).toThrow("1 argument required");
  expect(element.appendChild(5)).toBe(5);
  expect(element.first).toBe(shared);
  expect(element.second).toBe(shared);
  expect(shared.length).toBe(1);
});
