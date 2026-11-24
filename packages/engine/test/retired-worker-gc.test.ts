// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { collectRetiredWorkers, sweepRetiredWorkers } from "../src/eval/retired-worker-gc.ts";

// The throttle is module state, so each test starts a minute after the previous one.
let epoch = Date.UTC(2026, 0, 1);

beforeEach(() => {
  vi.useFakeTimers();
  epoch += 60_000;
  vi.setSystemTime(epoch);
});

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("collectRetiredWorkers", () => {
  it("does nothing without gc()", () => {
    expect(Reflect.get(globalThis, "gc")).toBeUndefined();
    expect(() => collectRetiredWorkers()).not.toThrow();
  });

  it("ignores a gc global that is not a function", () => {
    vi.stubGlobal("gc", 1);
    expect(() => collectRetiredWorkers()).not.toThrow();
  });

  it("calls gc() once when it is available", () => {
    const contexts: unknown[] = [];
    const gc = vi.fn(function (this: unknown) {
      contexts.push(this);
    });
    vi.stubGlobal("gc", gc);
    collectRetiredWorkers();
    expect(gc).toHaveBeenCalledTimes(1);
    expect(contexts).toEqual([globalThis]);
  });

  it("collects at most once a second", () => {
    const gc = vi.fn();
    vi.stubGlobal("gc", gc);
    collectRetiredWorkers();
    vi.setSystemTime(epoch + 999);
    collectRetiredWorkers();
    expect(gc, "a second retirement within 1 s is throttled").toHaveBeenCalledTimes(1);
    vi.setSystemTime(epoch + 1_000);
    collectRetiredWorkers();
    expect(gc).toHaveBeenCalledTimes(2);
  });

  it("keeps collecting after the clock steps back", () => {
    const gc = vi.fn();
    vi.stubGlobal("gc", gc);
    collectRetiredWorkers();
    vi.setSystemTime(epoch - 30_000);
    collectRetiredWorkers();
    expect(gc, "a backwards step does not throttle").toHaveBeenCalledTimes(2);
    vi.setSystemTime(epoch - 29_500);
    collectRetiredWorkers();
    expect(gc, "the throttle restarts from the stepped-back time").toHaveBeenCalledTimes(2);
  });

  it("collects the last retirement after a burst and stops after the quiet period", () => {
    const gc = vi.fn();
    vi.stubGlobal("gc", gc);
    collectRetiredWorkers();
    vi.setSystemTime(epoch + 100);
    collectRetiredWorkers();
    for (const elapsed of [100, 149]) {
      vi.setSystemTime(epoch + 100 + elapsed);
      sweepRetiredWorkers();
    }
    expect(gc).toHaveBeenCalledTimes(1);
    for (const elapsed of [200, 400, 5_000, 5_200, 15_000, 15_200, 60_000]) {
      vi.setSystemTime(epoch + 100 + elapsed);
      sweepRetiredWorkers();
      sweepRetiredWorkers();
    }
    expect(gc).toHaveBeenCalledTimes(7);
  });

  it("coalesces overdue passes and handles a clock step without retaining new work", () => {
    const gc = vi.fn();
    vi.stubGlobal("gc", gc);
    collectRetiredWorkers();
    vi.setSystemTime(epoch - 10_000);
    sweepRetiredWorkers();
    vi.setSystemTime(epoch - 9_800);
    sweepRetiredWorkers();
    vi.setSystemTime(epoch + 20_000);
    sweepRetiredWorkers();
    sweepRetiredWorkers();
    expect(gc).toHaveBeenCalledTimes(3);
  });

  it("calls gc with the global receiver for retirements and sweeps", () => {
    const contexts: unknown[] = [];
    const gc = vi.fn(function (this: unknown) {
      contexts.push(this);
    });
    vi.stubGlobal("gc", gc);
    collectRetiredWorkers();
    vi.setSystemTime(epoch + 200);
    sweepRetiredWorkers();
    expect(contexts).toEqual([globalThis, globalThis]);
  });

  it("swallows a throwing gc()", () => {
    const gc = vi.fn(() => {
      throw new Error("gc failed");
    });
    vi.stubGlobal("gc", gc);
    expect(() => collectRetiredWorkers()).not.toThrow();
    expect(gc).toHaveBeenCalledTimes(1);
    vi.setSystemTime(epoch + 200);
    expect(() => sweepRetiredWorkers()).not.toThrow();
    expect(gc).toHaveBeenCalledTimes(2);
  });
});
