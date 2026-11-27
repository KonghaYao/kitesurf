// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import type { CookieJar } from "../cookies.ts";
import type { PageScriptStub } from "../eval/page-script.ts";
import { jarInBrowser } from "./target.ts";
import type { BrowserState, TargetState } from "./types.ts";

const syncedViews = new WeakMap<CookieJar, { revision: number; pending?: Promise<void> }>();

export function syncBrowserCookieViews(browser: BrowserState): Promise<unknown> | undefined {
  const pending = [syncDocumentCookies(browser, browser.cookies)];
  for (const { cookieJar } of browser.contexts.values()) {
    pending.push(syncDocumentCookies(browser, cookieJar));
  }
  if (pending.some(Boolean)) return Promise.all(pending);
}

export function syncDocumentCookies(
  browser: BrowserState,
  jar: CookieJar,
): Promise<void> | undefined {
  const revision = jar.revision;
  const previous = syncedViews.get(jar);
  if (!revision || previous?.revision === revision) return previous?.pending;
  const record = { revision, pending: undefined as Promise<void> | undefined };
  syncedViews.set(jar, record);
  record.pending = updateViews(browser, jar).finally(() => {
    record.pending = undefined;
  });
  return record.pending;
}

// Every live document in the browser context has its own synchronous cookie view.
// Use the URL sealed to its isolate, since a pending navigation can change target.url.
async function updateViews(browser: BrowserState, jar: CookieJar): Promise<void> {
  const pending: Promise<unknown>[] = [];
  for (const target of browser.targets.values()) {
    if (jarInBrowser(browser, target) !== jar) continue;
    if (target.pageScript && target.pageScriptUrl) {
      pending.push(target.pageScript.syncCookies(jar.documentCookieString(target.pageScriptUrl)));
    }
    for (const frame of target.frames.values()) {
      if (
        !frame.detached &&
        frame.pageScript &&
        frame.pageScriptUrl &&
        frame.pageScriptSandbox?.sameOrigin
      ) {
        pending.push(frame.pageScript.syncCookies(jar.documentCookieString(frame.pageScriptUrl)));
      }
    }
  }
  // A page may retire its isolate while the mutation is in flight.
  await Promise.allSettled(pending);
}

export function harvestDocumentCookies(
  browser: BrowserState,
  target: TargetState,
  url: string | null | undefined,
  writes: string[] | undefined,
  source?: PageScriptStub,
): Promise<unknown> | undefined {
  if (!url || !writes?.length) return;
  const jar = jarInBrowser(browser, target);
  const revision = jar.revision;
  for (const raw of writes) jar.setFromDocumentCookie(url, raw);
  if (jar.revision !== revision) return syncDocumentCookies(browser, jar);
  // A rejected write may still be present in the source isolate's optimistic cookie view.
  if (source) return source.syncCookies(jar.documentCookieString(url)).catch(() => {});
}
