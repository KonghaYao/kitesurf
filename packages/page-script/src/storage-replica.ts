// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import type {
  StorageArea,
  WebStorageChange,
  WebStorageDelivery,
  WebStorageMutation,
  WebStorageSeed,
} from "@kitesurf/jsrpc-types/web-storage";

function mutate(values: Map<string, string>, change: WebStorageMutation): void {
  if (change.key === null) values.clear();
  else if (change.value === null) values.delete(change.key);
  else values.set(change.key, change.value);
}

export class StorageReplica {
  private sequence = 0;
  private pending = new Map<number, WebStorageChange>();
  private base: Record<StorageArea, Map<string, string>>;
  private visible: Record<StorageArea, Map<string, string>>;
  constructor(seed: WebStorageSeed) {
    this.base = {
      localStorage: new Map(seed.localStorage),
      sessionStorage: new Map(seed.sessionStorage),
    };
    this.visible = {
      localStorage: new Map(seed.localStorage),
      sessionStorage: new Map(seed.sessionStorage),
    };
  }
  bucket(area: StorageArea): Map<string, string> {
    return this.visible[area];
  }
  write(mutation: WebStorageMutation): WebStorageChange {
    const change = { ...mutation, sequence: ++this.sequence };
    this.pending.set(change.sequence, change);
    mutate(this.visible[change.area], change);
    return change;
  }
  apply(change: WebStorageDelivery): void {
    mutate(this.base[change.area], change);
    if (change.ack !== null) this.pending.delete(change.ack);
    const visible = this.visible[change.area];
    visible.clear();
    for (const [key, value] of this.base[change.area]) visible.set(key, value);
    // Unacknowledged local writes stay visible even when an older echo arrives.
    for (const pending of this.pending.values()) {
      if (pending.area === change.area) mutate(visible, pending);
    }
  }
}
