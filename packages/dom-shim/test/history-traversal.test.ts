// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { afterEach, expect, it, vi } from "vitest";
import { DocumentImpl } from "../src/document.js";
import { installWindow, location } from "../src/window.js";
import type { HistoryChangeDetail, HistorySeed } from "../src/state.js";

const origin = "https://example.test";
const current = vi.hoisted(() => ({
  url: "",
  documentUrl: "",
  calls: [] as Array<{ url: string; kind: string; entryId?: number; traversal?: boolean }>,
}));
vi.mock("../src/state.js", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../src/state.js")>()),
  getCurrentUrl: () => current.url,
  getDocumentUrl: () => current.documentUrl || current.url,
  setCurrentUrl: (url: string, kind: string, detail?: HistoryChangeDetail) => {
    current.calls.push({
      url,
      kind,
      ...(typeof detail?.entryId === "number" ? { entryId: detail.entryId } : {}),
      ...(detail?.traversal === true ? { traversal: true } : {}),
    });
    // Page-script commits a same-document URL immediately; a cross-document one waits.
    if (kind !== "navigate" && kind !== "historyTraverse") {
      current.url = url;
      current.documentUrl = url;
    }
  },
}));

function windowWith(seed: HistorySeed | undefined, url = `${origin}/b`) {
  current.url = url;
  current.documentUrl = url;
  current.calls = [];
  return installWindow(new DocumentImpl(), seed).windowObj;
}

afterEach(() => {
  vi.restoreAllMocks();
});

it("seeds history.length from the engine's committed session history", () => {
  const window = windowWith({
    entries: [
      { url: `${origin}/a`, entryId: 1 },
      { url: `${origin}/b`, entryId: 2 },
    ],
    index: 1,
  });
  expect(window.history.length).toBe(2);
  expect(window.history.state).toBeNull();
});

it("back() across a document boundary asks the engine to navigate to that entry", () => {
  const window = windowWith({
    entries: [
      { url: `${origin}/a`, entryId: 1 },
      { url: `${origin}/b`, entryId: 2 },
    ],
    index: 1,
  });
  window.history.back();
  expect(current.calls).toEqual([{ url: `${origin}/a`, kind: "historyTraverse", entryId: 1 }]);
});

it("forward() across a document boundary asks the engine to navigate to that entry", () => {
  const window = windowWith({
    entries: [
      { url: `${origin}/a`, entryId: 1 },
      { url: `${origin}/b`, entryId: 2 },
    ],
    index: 0,
  });
  window.history.forward();
  expect(current.calls).toEqual([{ url: `${origin}/b`, kind: "historyTraverse", entryId: 2 }]);
});

it("traversal within this document's own entries stays a history API change", () => {
  const window = windowWith({
    entries: [
      { url: `${origin}/a`, entryId: 1 },
      { url: `${origin}/b`, entryId: 2 },
    ],
    index: 1,
  });
  window.history.pushState({ page: 1 }, "", "/c");
  window.history.go(-1);
  expect(current.calls).toEqual([
    { url: `${origin}/c`, kind: "pushState" },
    { url: `${origin}/b`, kind: "historyApi", entryId: 2, traversal: true },
  ]);
});

it("a fragment navigation pushes an entry and back() traverses it", () => {
  const window = windowWith(undefined, `${origin}/b`);
  location.assign("#two");
  expect(window.history.length).toBe(2);
  expect(current.calls).toEqual([{ url: `${origin}/b#two`, kind: "hash" }]);

  window.history.back();
  expect(window.history.length).toBe(2);
  expect(current.calls[1]).toEqual({
    url: `${origin}/b`,
    kind: "hash",
    traversal: true,
  });
  expect(current.url).toBe(`${origin}/b`);

  window.history.forward();
  expect(current.calls[2]).toEqual({
    url: `${origin}/b#two`,
    kind: "hash",
    traversal: true,
  });
});

it("a fragment navigation truncates the forward entries the engine drops", () => {
  const window = windowWith(
    {
      entries: [
        { url: `${origin}/a`, entryId: 1 },
        { url: `${origin}/b`, entryId: 2 },
        { url: `${origin}/c`, entryId: 3 },
      ],
      index: 0,
    },
    `${origin}/a`,
  );
  location.assign("#two");
  expect(window.history.length).toBe(2);
  window.history.forward();
  expect(current.calls).toEqual([{ url: `${origin}/a#two`, kind: "hash" }]);
});

it("go() leaves history alone beyond either end", () => {
  const window = windowWith({
    entries: [{ url: `${origin}/a`, entryId: 1 }],
    index: 0,
  });
  window.history.go(-1);
  window.history.go(1);
  expect(current.calls).toEqual([]);
});
