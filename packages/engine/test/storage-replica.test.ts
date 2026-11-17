// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { describe, expect, it } from "vitest";
import { StorageReplica } from "../../page-script/src/storage-replica.ts";
import type { WebStorageChange, WebStorageDelivery } from "@kitesurf/jsrpc-types/web-storage";

const ack = (change: WebStorageChange): WebStorageDelivery => ({
  ...change,
  ack: change.sequence,
  oldValue: null,
  url: "https://example.com",
  notify: false,
});

describe("pending browser storage writes", () => {
  it("a late delete echo cannot hide a newer token and swallow logout", () => {
    const replica = new StorageReplica({ localStorage: [["token", "old"]], sessionStorage: [] });
    const first = replica.write({ area: "localStorage", key: "token", value: null });
    const second = replica.write({ area: "localStorage", key: "token", value: "new" });
    replica.apply(ack(first));
    expect(replica.bucket("localStorage").get("token")).toBe("new");
    const logout = replica.write({ area: "localStorage", key: "token", value: null });
    replica.apply(ack(second));
    expect(replica.bucket("localStorage").has("token")).toBe(false);
    replica.apply(ack(logout));
    expect(replica.bucket("localStorage").has("token")).toBe(false);
  });

  it("a remote write does not overwrite a local operation before its acknowledgement", () => {
    const replica = new StorageReplica({ localStorage: [], sessionStorage: [] });
    const own = replica.write({ area: "localStorage", key: "token", value: "mine" });
    replica.apply({ ...ack(own), value: "peer", ack: null, notify: true });
    expect(replica.bucket("localStorage").get("token")).toBe("mine");
    replica.apply(ack(own));
    expect(replica.bucket("localStorage").get("token")).toBe("mine");
    replica.apply({ ...ack(own), value: "later-peer", ack: null, notify: true });
    expect(replica.bucket("localStorage").get("token")).toBe("later-peer");
  });

  it("clear preserves later local writes while acknowledgements are delayed", () => {
    const replica = new StorageReplica({
      localStorage: [["old", "old"]],
      sessionStorage: [["step", "1"]],
    });
    const clear = replica.write({ area: "localStorage", key: null, value: null });
    const set = replica.write({ area: "localStorage", key: "new", value: "new" });
    replica.apply(ack(clear));
    expect([...replica.bucket("localStorage")]).toEqual([["new", "new"]]);
    replica.apply(ack(set));
    expect([...replica.bucket("localStorage")]).toEqual([["new", "new"]]);
    expect(replica.bucket("sessionStorage").get("step")).toBe("1");
  });
});
