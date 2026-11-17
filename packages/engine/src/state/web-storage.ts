// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import type {
  WebStorageChange,
  WebStorageDelivery,
  WebStorageSeed,
  StorageEntries,
} from "@kitesurf/jsrpc-types/web-storage";

export type LocalStorageSnapshot = Array<[string, StorageEntries]>;
interface Receiver {
  context: string;
  tab: string;
  origin: string;
  url: string;
  apply(change: WebStorageDelivery): Promise<unknown>;
  flush(): Promise<unknown>;
}

export class SessionWebStorage {
  private local = new Map<string, Map<string, Map<string, string>>>();
  private session = new Map<string, Map<string, Map<string, string>>>();
  private receivers = new Set<Receiver>();
  private deliveries = new Map<Receiver, Promise<unknown>>();
  private failures = new Map<Receiver, unknown>();
  constructor(
    private defaultContext: string,
    snapshot: LocalStorageSnapshot,
    private changed: () => void,
  ) {
    this.local.set(
      defaultContext,
      new Map(snapshot.map(([origin, entries]) => [origin, new Map(entries)])),
    );
  }
  private bucket(
    area: "localStorage" | "sessionStorage",
    context: string,
    tab: string,
    origin: string,
  ) {
    const map = area === "localStorage" ? this.local : this.session;
    const scope = area === "localStorage" ? context : tab;
    let origins = map.get(scope);
    if (!origins) map.set(scope, (origins = new Map()));
    let entries = origins.get(origin);
    if (!entries) origins.set(origin, (entries = new Map()));
    return entries;
  }
  seed(context: string, tab: string, origin: string): WebStorageSeed {
    if (!origin) return { localStorage: [], sessionStorage: [] };
    return {
      localStorage: [...this.bucket("localStorage", context, tab, origin)],
      sessionStorage: [...this.bucket("sessionStorage", context, tab, origin)],
    };
  }
  register(receiver: Receiver): () => void {
    this.receivers.add(receiver);
    return () => {
      this.receivers.delete(receiver);
      this.deliveries.delete(receiver);
      this.failures.delete(receiver);
    };
  }
  change(sender: Receiver, change: WebStorageChange): void {
    if (!this.receivers.has(sender) || !sender.origin) return;
    const { area, key, value } = change;
    const data = this.bucket(area, sender.context, sender.tab, sender.origin);
    const oldValue = key === null ? null : (data.get(key) ?? null);
    const changed = key === null ? data.size > 0 : oldValue !== value;
    if (key === null) data.clear();
    else if (value === null) data.delete(key);
    else data.set(key, value);
    if (changed && area === "localStorage" && sender.context === this.defaultContext)
      this.changed();
    for (const peer of this.receivers) {
      if (peer.context !== sender.context || peer.origin !== sender.origin) continue;
      if (area === "sessionStorage" && peer.tab !== sender.tab) continue;
      // Echo the committed order to the sender too, so concurrent writers converge.
      const delivery = (this.deliveries.get(peer) ?? Promise.resolve())
        .then(() => {
          if (!this.receivers.has(peer)) return;
          return peer.apply({
            ...change,
            ack: peer === sender ? change.sequence : null,
            oldValue,
            url: sender.url,
            notify: peer !== sender && changed,
          });
        })
        .catch((error) => {
          if (this.receivers.has(peer)) this.failures.set(peer, error);
        });
      this.deliveries.set(peer, delivery);
    }
  }
  async flush(tab?: string): Promise<void> {
    const peers = [...this.receivers].filter((peer) => tab === undefined || peer.tab === tab);
    await Promise.all(
      peers.map(async (peer) => {
        try {
          await peer.flush();
        } catch (error) {
          if (this.receivers.has(peer)) throw error;
        }
      }),
    );
    await Promise.all(peers.map((peer) => this.deliveries.get(peer)));
    if (peers.some((peer) => this.failures.has(peer)))
      throw new Error("A live page failed to receive a storage update");
  }
  snapshot(): LocalStorageSnapshot {
    return [...(this.local.get(this.defaultContext) ?? [])].map(([origin, values]) => [
      origin,
      [...values],
    ]);
  }
  closeTab(tab: string): void {
    this.session.delete(tab);
  }
  closeContext(context: string): void {
    this.local.delete(context);
  }
}

const sessions = new Map<string, SessionWebStorage>();
export const registerWebStorage = (id: string, storage: SessionWebStorage): void => {
  sessions.set(id, storage);
};
export const webStorageFor = (id: string): SessionWebStorage | undefined => sessions.get(id);
export const unregisterWebStorage = (id: string): void => {
  sessions.delete(id);
};
