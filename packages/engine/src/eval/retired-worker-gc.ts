// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// A retired loaded worker stays alive until this isolate's GC collects the last stub to it, and
// V8 does not count that worker's memory, so dozens can pile up. A local workerd exposes gc()
// through --expose-gc. Retirements collect at most once a second; because a navigation can bury
// the last retirement inside that window, the local session's tick also runs tail sweeps.

const RETIRED_WORKER_GC_INTERVAL_MS = 1_000;

// The local session ticks every 200 ms. Give teardown promises a second turn at each stage.
const SWEEP_DELAYS_MS = [150, 350, 5_000, 5_200, 15_000, 15_200] as const;
let lastCollectedAt = 0;
let retiredAt = 0;
let nextSweep: number = SWEEP_DELAYS_MS.length;

function runGc(gc: unknown): void {
  if (typeof gc !== "function") return;
  try {
    Reflect.apply(gc, globalThis, []);
  } catch {}
}

/** Collects this isolate's garbage after a loaded worker retires; a no-op without gc(). */
export function collectRetiredWorkers(): void {
  const gc: unknown = Reflect.get(globalThis, "gc");
  if (typeof gc !== "function") return;
  const now = Date.now();
  retiredAt = now;
  nextSweep = 0;
  // A clock that stepped back does not throttle, or collection would stop until it caught up.
  if (now >= lastCollectedAt && now - lastCollectedAt < RETIRED_WORKER_GC_INTERVAL_MS) return;
  lastCollectedAt = now;
  runGc(gc);
}

/** The local broker holds this session open across page closes and client handoffs. */
export function sweepRetiredWorkers(): void {
  const delay = SWEEP_DELAYS_MS[nextSweep];
  if (delay === undefined) return;
  const now = Date.now();
  if (now < retiredAt) retiredAt = now;
  const elapsed = now - retiredAt;
  if (elapsed < delay) return;
  // A suspended process or two sessions ticking together must not replay missed GC passes.
  do {
    nextSweep++;
  } while ((SWEEP_DELAYS_MS[nextSweep] ?? Infinity) <= elapsed);
  lastCollectedAt = now;
  runGc(Reflect.get(globalThis, "gc"));
}
