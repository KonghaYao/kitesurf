// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// One product token for CDP discovery and page globals; a build reports one version.

const FALLBACK_VERSION = "0.0.1";

/** The Kitesurf product token, e.g. `Kitesurf/0.5.2`. */
export function kitesurfProduct(version: string | undefined): string {
  return `Kitesurf/${version?.trim() || FALLBACK_VERSION}`;
}
