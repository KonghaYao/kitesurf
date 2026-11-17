// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import type {
  StorageArea,
  WebStorageMutation,
  WebStorageDelivery,
} from "@kitesurf/jsrpc-types/web-storage";
import { webStorage as seed } from "./__ks_startup_probe_input.js";
import { sendUpstreamEventConfirmed } from "./internal-state.js";
import { state, ksGlobal } from "./state.ts";
import { StorageReplica } from "./storage-replica.ts";

const LIMIT_BYTES = 5 * 1024 * 1024;
interface StorageAPI {
  readonly length: number;
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem(key: string): void;
  clear(): void;
  key(index: number): string | null;
}
const replica = new StorageReplica(seed);
const pending = new Set<Promise<void>>();
let failed: unknown;

function publish(change: WebStorageMutation): void {
  const promise = sendUpstreamEventConfirmed({
    event: "kitesurf.webStorageChanged",
    params: replica.write(change),
  });
  pending.add(promise);
  void promise.then(
    () => pending.delete(promise),
    (error) => {
      failed = error;
      pending.delete(promise);
    },
  );
}

export async function flushWebStorage(): Promise<void> {
  while (pending.size) await Promise.all(pending);
  if (failed) throw failed;
}

export function applyWebStorageChange(change: WebStorageDelivery): void {
  replica.apply(change);
  if (change.notify && state.window && ksGlobal.StorageEvent) {
    const event = new ksGlobal.StorageEvent("storage", {
      key: change.key,
      oldValue: change.oldValue,
      newValue: change.value,
      url: change.url,
      storageArea: ksGlobal[change.area],
    });
    state.window.dispatchEvent(event);
  }
}

export function makeWebStorage(area: StorageArea): StorageAPI {
  const values = replica.bucket(area);
  const api = {
    getItem(key: string) {
      return values.get(String(key)) ?? null;
    },
    setItem(key: string, value: string) {
      key = String(key);
      value = String(value);
      let bytes = 2 * (key.length + value.length);
      for (const [k, v] of values) if (k !== key) bytes += 2 * (k.length + v.length);
      if (bytes > LIMIT_BYTES)
        throw new DOMException("Storage quota exceeded", "QuotaExceededError");
      if (values.get(key) === value) return;
      publish({ area, key, value });
    },
    removeItem(key: string) {
      key = String(key);
      if (values.has(key)) publish({ area, key, value: null });
    },
    clear() {
      if (!values.size) return;
      publish({ area, key: null, value: null });
    },
    key(index: number) {
      return [...values.keys()][Number(index) >>> 0] ?? null;
    },
    get length() {
      return values.size;
    },
  };
  return new Proxy(api, {
    get(target, key, receiver) {
      if (Reflect.has(target, key) || typeof key !== "string")
        return Reflect.get(target, key, receiver);
      return values.get(key);
    },
    set(_target, key, value) {
      if (typeof key !== "string") return false;
      api.setItem(key, value);
      return true;
    },
    deleteProperty(_target, key) {
      if (typeof key === "string") api.removeItem(key);
      return true;
    },
    ownKeys() {
      return [...values.keys()];
    },
    getOwnPropertyDescriptor(_target, key) {
      if (typeof key !== "string" || !values.has(key)) return undefined;
      return { configurable: true, enumerable: true, writable: true, value: values.get(key) };
    },
  });
}
