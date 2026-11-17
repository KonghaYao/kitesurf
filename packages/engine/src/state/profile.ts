// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import type { CookieEntry } from "../cookies.ts";
import type { DispatchCtx } from "../cdp/dispatch.ts";
import { jarForTarget } from "../cdp/target.ts";
import type { LocalStorageSnapshot, SessionWebStorage } from "./web-storage.ts";

export interface ProfileSnapshot {
  cookies: CookieEntry[];
  localStorage: LocalStorageSnapshot;
}
export interface ProfilePersistence {
  initial: ProfileSnapshot;
  save(snapshot: ProfileSnapshot): Promise<void>;
}

export class ProfileCommitter {
  private dirty = 0;
  private saved = 0;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private tail = Promise.resolve();
  constructor(
    private persistence: ProfilePersistence,
    private snapshot: () => ProfileSnapshot,
  ) {}
  changed = (): void => {
    this.dirty++;
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.flush().catch((error) => console.error("Profile save failed:", String(error)));
    }, 100);
  };
  flush(): Promise<void> {
    const save = async () => {
      const version = this.dirty;
      if (version === this.saved) return;
      await this.persistence.save(this.snapshot());
      this.saved = version;
    };
    const next = this.tail.then(save, save);
    this.tail = next;
    return next;
  }
  stop(): void {
    clearTimeout(this.timer);
  }
}

export async function flushProfile(
  ctx: DispatchCtx,
  storage: SessionWebStorage,
  committer?: ProfileCommitter,
  targetId?: string,
): Promise<void> {
  await storage.flush(targetId);
  for (const target of ctx.browser.targets.values()) {
    if (targetId !== undefined && target.targetId !== targetId) continue;
    const holders = [target, ...target.frames.values()];
    for (const holder of holders) {
      const stub = holder.pageScript;
      const url = holder.pageScriptUrl;
      if (!stub || !url || stub.isAlive?.() === false) continue;
      let drained;
      try {
        drained = await stub.drainConsole();
      } catch (error) {
        if (stub.isAlive?.() === false) continue;
        throw error;
      }
      if (holder.pageScript !== stub) continue;
      for (const raw of drained.cookieWrites ?? [])
        jarForTarget(ctx, target).setFromDocumentCookie(url, raw);
    }
  }
  await committer?.flush();
}
