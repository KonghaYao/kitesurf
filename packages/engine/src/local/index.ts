// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { KitesurfEngine } from "../index.ts";
import { sweepRetiredWorkers } from "../eval/retired-worker-gc.ts";
import type { EngineHost } from "../host.ts";
import type { ProfileSnapshot } from "../state/profile.ts";
export { SandboxOutbound, DeniedOutbound, PageScriptTail, SpanSink } from "../index.ts";

export default class LocalBrowserEngine extends KitesurfEngine {
  protected override host(): EngineHost {
    return {
      onSessionTick: sweepRetiredWorkers,
      connectionOptions: () => ({ idleTimeoutMs: 0, maxSessionMs: 0 }),
      openProfile: async (owner, env) => {
        const bridge = (env as Env & { LOCAL_PROFILE: Fetcher }).LOCAL_PROFILE;
        const request = async (path: string, body: unknown) => {
          const response = await bridge.fetch(
            new Request(`http://profile/${path}`, {
              method: "POST",
              body: JSON.stringify(body),
            }),
          );
          if (!response.ok)
            throw Object.assign(new Error(`Profile ${path}: ${await response.text()}`), {
              profileRejected: response.status === 422,
            });
          return response;
        };
        const loaded = await (
          await request("load", { owner })
        ).json<{ state: ProfileSnapshot; revision: number }>();
        let revision = loaded.revision;
        let pending: { revision: number; state: ProfileSnapshot } | undefined;
        const commit = async () => {
          if (!pending) return;
          try {
            await request("save", { owner, ...pending });
          } catch (error) {
            if (error instanceof Error && "profileRejected" in error && error.profileRejected)
              pending = undefined;
            throw error;
          }
          revision = pending.revision;
          pending = undefined;
        };
        return {
          initial: loaded.state,
          save: async (state) => {
            // A lost reply retries the exact request before accepting a newer snapshot.
            await commit();
            pending = { revision: revision + 1, state: structuredClone(state) };
            await commit();
          },
        };
      },
      createCdpCommandExtension: () => async (ctx, req) => {
        if (req.method !== "Kitesurf.flushProfile") return { handled: false };
        await ctx.browser.flushProfile?.();
        return { handled: true, result: { saved: true } };
      },
    };
  }
}
