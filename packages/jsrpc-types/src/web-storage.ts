// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

export type StorageArea = "localStorage" | "sessionStorage";
export type StorageEntries = Array<[string, string]>;
export interface WebStorageSeed {
  localStorage: StorageEntries;
  sessionStorage: StorageEntries;
}
export interface WebStorageMutation {
  area: StorageArea;
  key: string | null;
  value: string | null;
}
export interface WebStorageChange extends WebStorageMutation {
  sequence: number;
}
export interface WebStorageDelivery extends WebStorageChange {
  ack: number | null;
  oldValue: string | null;
  url: string;
  notify: boolean;
}
