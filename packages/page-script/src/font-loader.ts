// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Registers and loads fonts for core layout and text rendering.
// Fonts come from engine Static Assets through the `kitesurf-fonts` sentinel host and must be
// registered before the first DomHandle, or text renders as tofu.

import {
  register_ua_fonts,
  register_emoji_font,
  register_symbol_fonts,
  register_cjk_font,
  register_cjk_region_font,
  register_script_font,
  register_system_face,
  map_system_generic,
  append_system_fallback,
  append_system_emoji,
  finish_system_fonts,
  setFontShapingLazy,
  pendingLazyFontPaths,
  upgradeFontFace,
  finishFontUpgrade,
  uaFontProfile,
} from "./wasm-glue.js";
import { FetchSlots } from "./fetch-slots.ts";
import { NativePromise } from "./native-scheduling.ts";
import {
  startupFontDefer,
  startupFontProfile,
  startupPixelFree,
  startupSystemFonts,
} from "./startup-probe.ts";
import { CJK_REGION_SCANS, CJK_SCAN, EMOJI_BMP_SCAN, SCRIPT_RANGES } from "./source-scan.ts";
import type { SourceFontScan } from "@kitesurf/jsrpc-types/page-script";

let pending: Promise<void> | null = null;

let emojiPending: Promise<void> | null = null;

const ASSET_HOST = "http://kitesurf-fonts";

// The profile's UA faces, from core so the fetched files are the assets registration records.
// `faces` follows register_ua_fonts order: sans pair, system pair, serif trio, Georgia pair.
interface UaFontProfile {
  name: string;
  faces: string[];
  serifOnDemand: boolean;
  latinFallback: boolean;
}

const UA_FACE_COUNT = 9;
const REQUIRED_UA_FACES = 2;
const SERIF_FACES = { start: 4, end: 7 };

let uaProfile: UaFontProfile | null = null;

function readUaFontProfile(name: string): UaFontProfile | null {
  const json = uaFontProfile(name);
  if (!json) return null;
  const raw: unknown = JSON.parse(json);
  if (typeof raw !== "object" || raw === null) return null;
  if (!("faces" in raw && "serifOnDemand" in raw && "latinFallback" in raw)) return null;
  const { faces, serifOnDemand, latinFallback } = raw;
  if (!Array.isArray(faces) || faces.length !== UA_FACE_COUNT) return null;
  if (!faces.every((face): face is string => typeof face === "string")) return null;
  return {
    name,
    faces,
    serifOnDemand: serifOnDemand === true,
    latinFallback: latinFallback === true,
  };
}

function activeUaFontProfile(): UaFontProfile {
  if (uaProfile) return uaProfile;
  uaProfile = readUaFontProfile(startupFontProfile);
  if (!uaProfile) {
    console.warn(`[kitesurf-fonts] unknown font profile ${JSON.stringify(startupFontProfile)}`);
    uaProfile = readUaFontProfile("noto");
  }
  if (!uaProfile) throw new Error("[kitesurf-fonts] core has no noto font profile");
  return uaProfile;
}

// The profile's face paths for this page; "" skips a face, and serif may wait for a serif family.
function uaFacePaths(wantSerif: boolean): string[] {
  const { faces, serifOnDemand } = activeUaFontProfile();
  return faces.map((path, i) =>
    serifOnDemand && !wantSerif && i >= SERIF_FACES.start && i < SERIF_FACES.end ? "" : path,
  );
}

const MONO_FONT_PATH = "/fonts/NotoSansMono.ttf";

const EMPTY_FACE = new Uint8Array(0);

function registerUaFaces(faces: Uint8Array[], mono: Uint8Array): boolean {
  const face = (i: number): Uint8Array => faces[i] ?? EMPTY_FACE;
  return register_ua_fonts(
    face(0),
    face(1),
    face(2),
    face(3),
    face(4),
    face(5),
    face(6),
    face(7),
    face(8),
    mono,
    activeUaFontProfile().name,
  );
}

const SYMBOL_FONT_PATHS = ["/fonts/NotoSansSymbols.ttf", "/fonts/NotoSansSymbols2.ttf"];

const EMOJI_FONT_PATH = "/fonts/noto-color-emoji.woff2";

const CJK_FONT_PATH = "/fonts/NotoSansCJKsc-Regular.woff2";

const EMOJI_SHAPING_PATH = "/fonts/noto-color-emoji.shaping.otf";

const CJK_SHAPING_PATH = "/fonts/NotoSansCJKsc-Regular.shaping.otf";

let cjkPending: Promise<void> | null = null;

interface CjkRegionFont {
  region: string;
  path: string;
  shapingPath: string;
  range?: RegExp;
}
const CJK_REGION_FILES: Record<string, string> = {
  JP: "NotoSansCJKjp-Regular",
  TC: "NotoSansCJKtc-Regular",
};
const CJK_REGION_FONTS: CjkRegionFont[] = CJK_REGION_SCANS.map(({ region, range }) => {
  const file = CJK_REGION_FILES[region];
  if (!file) throw new Error(`[kitesurf-fonts] no face file for CJK region ${region}`);
  return { region, path: `/fonts/${file}.woff2`, shapingPath: `/fonts/${file}.shaping.otf`, range };
});

let symbolPending: Promise<void> | null = null;

interface ScriptFont {
  tag: string;
  path: string;
  range: RegExp;
}
const SCRIPT_FONT_FILES: Record<string, string> = {
  // Noto Sans for the Latin codepoints a profile's UA faces lack; only when the profile asks.
  Latn: "NotoSans.ttf",
  Arab: "NotoSansArabic.ttf",
  Hebr: "NotoSansHebrew.ttf",
  Deva: "NotoSansDevanagari.ttf",
  Thai: "NotoSansThai.ttf",
  Beng: "NotoSansBengali.ttf",
  Taml: "NotoSansTamil.ttf",
  Ethi: "NotoSansEthiopic.ttf",
  Guru: "NotoSansGurmukhi.ttf",
  Gujr: "NotoSansGujarati.ttf",
  Knda: "NotoSansKannada.ttf",
  Mlym: "NotoSansMalayalam.ttf",
  Telu: "NotoSansTelugu.ttf",
  Sinh: "NotoSansSinhala.ttf",
  Mymr: "NotoSansMyanmar.ttf",
  Khmr: "NotoSansKhmer.ttf",
  Laoo: "NotoSansLao.ttf",
  Geor: "NotoSansGeorgian.ttf",
  Armn: "NotoSansArmenian.ttf",
  Hang: "NotoSansKR.ttf",
};
const SCRIPT_FONTS: ScriptFont[] = SCRIPT_RANGES.map(({ tag, range }) => {
  const file = SCRIPT_FONT_FILES[tag];
  if (!file) throw new Error(`[kitesurf-fonts] no face file for script ${tag}`);
  return { tag, path: `/fonts/${file}`, range };
});

const scriptPending = new Map<string, Promise<void>>();

function scriptFontActive(scriptFont: ScriptFont): boolean {
  return scriptFont.tag !== "Latn" || activeUaFontProfile().latinFallback;
}

function registerScriptIfNeeded(scriptFont: ScriptFont): Promise<void> {
  const existing = scriptPending.get(scriptFont.tag);
  if (existing) return existing;
  const registrationPromise = (async () => {
    try {
      const bytes = await fetchFaceBytes(scriptFont.path);
      register_script_font(bytes, scriptFont.tag);
      console.log(`[kitesurf-fonts] registered ${scriptFont.tag} (${bytes.byteLength} bytes)`);
    } catch (e) {
      console.warn(`[kitesurf-fonts] ${scriptFont.tag} registration failed:`, String(e));
      scriptPending.delete(scriptFont.tag);
      throw e;
    }
  })();
  scriptPending.set(scriptFont.tag, registrationPromise);
  return registrationPromise;
}

async function fetchBytes(path: string): Promise<Uint8Array> {
  const response = await fetch(ASSET_HOST + path);
  if (!response.ok) {
    throw new Error(`[kitesurf-fonts] ${path}: HTTP ${response.status} ${response.statusText}`);
  }
  return new Uint8Array(await response.arrayBuffer());
}

function shapingVariant(path: string): string {
  return path.replace(/\.ttf$/, ".shaping.otf");
}

// The CJK faces ship as a WOFF2 full file and a separate outline-free shaping file. A full-mode
// page (canvas or SVG text) needs outlines, so it skips the shaping file.
function fetchCjkBytes(path: string, shapingPath: string): Promise<Uint8Array> {
  if (fontLoaderMode() === "full") return fetchBytes(path);
  return fetchBytes(shapingPath).catch(() => fetchBytes(path));
}

// An upgrade that finished while a lazy fetch was in flight never swaps that face, so a shaping
// file that lands after the switch to full is replaced by the full file before it registers.
async function fetchCjkFace(path: string, shapingPath: string): Promise<Uint8Array> {
  const startedLazy = fontLoaderMode() === "lazy";
  const bytes = await fetchCjkBytes(path, shapingPath);
  return startedLazy && fontLoaderMode() === "full" ? fetchBytes(path) : bytes;
}

let fontMode: "full" | "pixelFree" | "lazy" | null = null;

export function fontLoaderMode(): "full" | "pixelFree" | "lazy" {
  return fontMode ?? "full";
}

function decideFontMode(scan: SourceFontScan | undefined): void {
  if (fontMode !== null) return;
  if (startupSystemFonts) {
    fontMode = "full";
    return;
  }
  if (startupPixelFree) {
    fontMode = "pixelFree";
    return;
  }
  if (scan?.outline) {
    fontMode = "full";
    return;
  }
  fontMode = "lazy";
  try {
    setFontShapingLazy(true);
  } catch {}
}

async function fetchFaceBytes(path: string): Promise<Uint8Array> {
  if (fontLoaderMode() === "full") return fetchBytes(path);
  return fetchBytes(shapingVariant(path)).catch(() => fetchBytes(path));
}

let upgradePending: Promise<void> | null = null;
let onUpgraded: (() => void) | null = null;

export function setFontUpgradeCallback(cb: () => void): void {
  onUpgraded = cb;
}

let authorUpgradeHook: (() => Promise<void>) | null = null;

export function setAuthorFontUpgradeHook(hook: () => Promise<void>): void {
  authorUpgradeHook = hook;
}

export function upgradePendingPromise(): Promise<void> | null {
  return upgradePending;
}

export function upgradeToFullFonts(): Promise<void> | null {
  if (startupSystemFonts) return null;
  if (fontLoaderMode() !== "lazy") return upgradePending;
  if (upgradePending) return upgradePending;
  upgradePending = (async () => {
    const startedAt =
      typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
    let fontPaths: string[] = [];
    try {
      fontPaths = pendingLazyFontPaths();
    } catch {}
    const fetchedFonts = await Promise.all(
      fontPaths.map(async (fontPath) => {
        try {
          return [fontPath, await fetchBytes(fontPath)] as const;
        } catch (e) {
          console.warn(`[kitesurf-fonts] upgrade fetch ${fontPath} failed:`, String(e));
          return [fontPath, null] as const;
        }
      }),
    );
    let swappedCount = 0;
    for (const [fontPath, fontBytes] of fetchedFonts) {
      if (!fontBytes) continue;
      try {
        if (upgradeFontFace(fontPath, fontBytes)) swappedCount++;
      } catch (e) {
        console.warn(`[kitesurf-fonts] upgrade swap ${fontPath} failed:`, String(e));
      }
    }
    try {
      await authorUpgradeHook?.();
    } catch {}
    let finished = false;
    try {
      finished = finishFontUpgrade();
    } catch {}
    if (finished) {
      fontMode = "full";
    } else {
      upgradePending = null;
    }
    const finishedAt =
      typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
    console.log(
      `[kitesurf-fonts] upgraded ${swappedCount}/${fontPaths.length} faces to full in ${(finishedAt - startedAt).toFixed(1)}ms`,
    );
    if (swappedCount > 0) {
      try {
        onUpgraded?.();
      } catch {}
    }
  })();
  return upgradePending;
}

function registerSymbolsIfNeeded(wantSymbols2: boolean): Promise<void> {
  if (symbolPending) return symbolPending;
  symbolPending = (async () => {
    try {
      const fontFetches = [fetchFaceBytes(SYMBOL_FONT_PATHS[0])];
      if (wantSymbols2) fontFetches.push(fetchFaceBytes(SYMBOL_FONT_PATHS[1]));
      const fetchedFonts = await Promise.all(fontFetches);
      const symbols = fetchedFonts[0];
      const symbols2 = wantSymbols2 ? fetchedFonts[1] : new Uint8Array(0);
      register_symbol_fonts(symbols, symbols2);
      console.log(
        `[kitesurf-fonts] registered ${wantSymbols2 ? 2 : 1} symbol font(s) (${symbols.byteLength + symbols2.byteLength} bytes)`,
      );
    } catch (e) {
      console.warn("[kitesurf-fonts] symbol registration failed:", String(e));
      symbolPending = null;
      throw e;
    }
  })();
  return symbolPending;
}

function registerCjkIfNeeded(): Promise<void> {
  if (cjkPending) return cjkPending;
  cjkPending = (async () => {
    try {
      const startedAt =
        typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
      const bytes = await fetchCjkFace(CJK_FONT_PATH, CJK_SHAPING_PATH);
      // Core declines while the UA faces, and with them the context, are still deferred.
      if (!register_cjk_font(bytes) && fontsDeferredUnregistered()) {
        cjkPending = null;
        return;
      }
      const finishedAt =
        typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
      console.log(
        `[kitesurf-fonts] registered CJK font (${bytes.byteLength} bytes) in ${(finishedAt - startedAt).toFixed(1)}ms`,
      );
    } catch (e) {
      console.warn("[kitesurf-fonts] CJK registration failed:", String(e));
      cjkPending = null;
      throw e;
    }
  })();
  return cjkPending;
}

const cjkRegionPending = new Map<string, Promise<void>>();

function registerCjkRegionIfNeeded(font: CjkRegionFont): Promise<void> {
  const existing = cjkRegionPending.get(font.region);
  if (existing) return existing;
  const registrationPromise = (async () => {
    try {
      const bytes = await fetchCjkFace(font.path, font.shapingPath);
      if (!register_cjk_region_font(bytes, font.region) && fontsDeferredUnregistered()) {
        cjkRegionPending.delete(font.region);
        return;
      }
      console.log(
        `[kitesurf-fonts] registered CJK ${font.region} font (${bytes.byteLength} bytes)`,
      );
    } catch (e) {
      console.warn(`[kitesurf-fonts] CJK ${font.region} registration failed:`, String(e));
      cjkRegionPending.delete(font.region);
      throw e;
    }
  })();
  cjkRegionPending.set(font.region, registrationPromise);
  return registrationPromise;
}

function registerEmojiIfNeeded(): Promise<void> {
  if (emojiPending) return emojiPending;
  emojiPending = (async () => {
    try {
      const emoji = await fetchBytes(EMOJI_SHAPING_PATH).catch(() => fetchBytes(EMOJI_FONT_PATH));
      register_emoji_font(emoji);
      console.log(`[kitesurf-fonts] registered color emoji (${emoji.byteLength} bytes)`);
    } catch (e) {
      console.warn("[kitesurf-fonts] emoji registration failed:", String(e));
      emojiPending = null;
      throw e;
    }
  })();
  return emojiPending;
}

export function emojiPendingPromise(): Promise<void> | null {
  return emojiPending;
}

export function cjkPendingPromise(): Promise<void> | null {
  return cjkPending;
}

export function scriptPendingPromises(): Promise<void>[] {
  return [...scriptPending.values(), ...cjkRegionPending.values()];
}

interface ScriptTextProbe {
  nonLatinTextWrites?(): number;
  textUsesCodepoints?(ranges: Uint32Array): boolean;
}

let nonLatinWritesSeen = 0;

function classRanges(unicodeRange: RegExp): Uint32Array {
  const out: number[] = [];
  for (const match of unicodeRange.source.matchAll(/\\u([0-9A-F]{4})(?:-\\u([0-9A-F]{4}))?/gi)) {
    const start = Number.parseInt(match[1], 16);
    out.push(start, match[2] ? Number.parseInt(match[2], 16) : start);
  }
  return Uint32Array.from(out);
}
const CJK_RANGES = classRanges(CJK_SCAN);
const CJK_REGION_RANGES = CJK_REGION_FONTS.flatMap((font) =>
  font.range ? [{ font, ranges: classRanges(font.range) }] : [],
);
const EMOJI_RANGES = Uint32Array.from([...classRanges(EMOJI_BMP_SCAN), 0x1f000, 0x1ffff]);
const SCRIPT_FACE_RANGES = SCRIPT_FONTS.map((scriptFont) => ({
  scriptFont,
  ranges: classRanges(scriptFont.range),
}));

export function noteNavigateText(dom: ScriptTextProbe): void {
  nonLatinWritesSeen = dom.nonLatinTextWrites?.() ?? 0;
}

export function demandFacesForScriptText(dom: ScriptTextProbe): void {
  if (!dom.nonLatinTextWrites || !dom.textUsesCodepoints) return;
  const writes = dom.nonLatinTextWrites();
  if (writes === nonLatinWritesSeen) return;
  nonLatinWritesSeen = writes;
  if (startupSystemFonts) {
    const systemTasks: Promise<void>[] = [];
    if (dom.textUsesCodepoints(EMOJI_RANGES)) systemTasks.push(ensureSystemEmoji());
    if (dom.textUsesCodepoints(CJK_RANGES)) systemTasks.push(ensureSystemCjk());
    for (const { scriptFont, ranges } of SCRIPT_FACE_RANGES) {
      if (scriptFont.tag === "Latn") continue;
      if (dom.textUsesCodepoints(ranges)) systemTasks.push(ensureSystemScript(scriptFont));
    }
    if (systemTasks.length === 0) return;
    void NativePromise.all(systemTasks).then(() => onUpgraded?.());
    return;
  }
  const tasks: Promise<void>[] = [];
  if (!cjkPending && dom.textUsesCodepoints(CJK_RANGES)) {
    tasks.push(registerCjkIfNeeded());
  }
  for (const { font, ranges } of CJK_REGION_RANGES) {
    if (!cjkRegionPending.has(font.region) && dom.textUsesCodepoints(ranges)) {
      tasks.push(registerCjkRegionIfNeeded(font));
    }
  }
  if (!emojiPending && dom.textUsesCodepoints(EMOJI_RANGES)) {
    tasks.push(registerEmojiIfNeeded());
  }
  for (const { scriptFont, ranges } of SCRIPT_FACE_RANGES) {
    if (
      !scriptPending.has(scriptFont.tag) &&
      scriptFontActive(scriptFont) &&
      dom.textUsesCodepoints(ranges)
    ) {
      tasks.push(registerScriptIfNeeded(scriptFont));
    }
  }
  if (tasks.length === 0) return;
  void NativePromise.all(tasks.map((task) => task.catch(() => {}))).then(() => onUpgraded?.());
}

let forceMono = false;

export function forceMonoFace(): void {
  forceMono = true;
}

function computeFontWants(scan?: SourceFontScan): {
  symbols: boolean;
  symbols2: boolean;
  emoji: boolean;
  cjk: boolean;
  cjkRegions: CjkRegionFont[];
  mono: boolean;
  serif: boolean;
  scripts: ScriptFont[];
} {
  return {
    symbols: !!scan?.symbols,
    symbols2: !!scan?.symbols2,
    emoji: !!scan?.emoji,
    cjk: !!scan?.cjk,
    cjkRegions: scan
      ? CJK_REGION_FONTS.filter(({ region }) => scan.cjkRegions.includes(region))
      : [],
    mono: forceMono || !!scan?.mono,
    serif: !!scan?.serif,
    scripts: scan
      ? SCRIPT_FONTS.filter((font) => scan.scripts.includes(font.tag) && scriptFontActive(font))
      : [],
  };
}

const SYSTEM_FONT_ENDPOINT = "http://kitesurf-fonts/system";
// The local helper processes one request at a time, and large faces share a read budget.
const systemFontSlots = new FetchSlots(1, 1);
const SYSTEM_FACE_CACHE_MAX = 64 * 1024 * 1024;
const SYSTEM_FACE_MAX = 24 * 1024 * 1024;
const systemFaceCache = new Map<string, Uint8Array>();
const systemFacePending = new Map<string, Promise<Uint8Array>>();
let systemFaceCacheBytes = 0;
let systemLocale = "";
let systemKana = false;
let systemHang = false;
let systemCorePromise: Promise<void> | null = null;
const systemCjkPending = new Map<string, Promise<void>>();
let systemEmojiPromise: Promise<void> | null = null;
const systemScriptPending = new Map<string, Promise<void>>();
const systemFamilies: { sans?: string; serif?: string; mono?: string; emoji?: string } = {};
const systemDiagnostics: string[] = [];

function noteSystemDiagnostic(message: string): void {
  if (!systemDiagnostics.includes(message) && systemDiagnostics.length < 32)
    systemDiagnostics.push(message);
}

export function systemFontDiagnostics(): string[] {
  return systemDiagnostics.slice();
}

interface SystemFace {
  id: string;
  family: string;
  weight: number;
  style: string;
  width: number;
  covered: boolean;
  missing: number;
  bytes: number;
}

interface SystemMatch {
  faces?: SystemFace[];
  covered?: boolean;
  missing?: number;
  reason?: string;
}

function safeFamilyName(value: string): string {
  return value.replace(/[\u0000-\u001f\u007f]/g, "?").slice(0, 200);
}

// The CJK locale the host asks the system for: the region the source scan matched. Kana reads as
// Japanese and a `zh-TW`/`zh-HK` lang as Traditional, so a Han-only page picks a fitting face.
function scanCjkLocale(scan?: SourceFontScan): string {
  if (!scan) return "";
  if (scan.cjkRegions.includes("JP")) return "ja";
  if (scan.cjkRegions.includes("TC")) return "zh-TW";
  return "";
}

async function fetchSystemFont<T>(
  path: string,
  label: string,
  read: (response: Response) => Promise<T>,
): Promise<T> {
  await systemFontSlots.acquire(SYSTEM_FONT_ENDPOINT);
  try {
    let response: Response;
    try {
      response = await fetch(SYSTEM_FONT_ENDPOINT + path);
    } catch (error) {
      throw new Error(`[kitesurf-fonts] ${label} failed: ${String(error)}`);
    }
    if (!response.ok) {
      await response.body?.cancel().catch(() => {});
      throw new Error(`[kitesurf-fonts] ${label}: HTTP ${response.status}`);
    }
    return await read(response);
  } finally {
    systemFontSlots.release(SYSTEM_FONT_ENDPOINT);
  }
}

async function systemMatch(
  family: string,
  options: {
    script?: string;
    locale?: string;
    weight?: number;
    style?: string;
    text?: string;
  } = {},
): Promise<SystemMatch> {
  const params = new URLSearchParams({
    family,
    style: options.style ?? "normal",
    weight: String(options.weight ?? 400),
  });
  if (options.script) params.set("script", options.script);
  if (options.locale) params.set("locale", options.locale);
  if (options.text) params.set("text", options.text.slice(0, 4096));
  const label = `system match ${family}/${options.script ?? ""}/${options.locale ?? ""}`;
  const body = await fetchSystemFont(
    `/match?${params}`,
    label,
    (response) => response.json() as Promise<SystemMatch>,
  );
  return body && typeof body === "object" ? body : {};
}

async function systemFetchFace(id: string): Promise<Uint8Array> {
  if (!/^[0-9a-f]{32}$/.test(id)) throw new Error("[kitesurf-fonts] invalid system font id");
  const cached = systemFaceCache.get(id);
  if (cached) {
    systemFaceCache.delete(id);
    systemFaceCache.set(id, cached);
    return cached;
  }
  const existing = systemFacePending.get(id);
  if (existing) return existing;
  const pending = (async () => {
    try {
      const buffer = await fetchSystemFont(`/face?id=${id}`, "system face", (response) =>
        response.arrayBuffer(),
      );
      const bytes = new Uint8Array(buffer);
      if (bytes.byteLength === 0 || bytes.byteLength > SYSTEM_FACE_MAX)
        throw new Error("[kitesurf-fonts] system face size is not allowed");
      if (bytes.byteLength <= SYSTEM_FACE_CACHE_MAX) {
        while (
          systemFaceCacheBytes + bytes.byteLength > SYSTEM_FACE_CACHE_MAX &&
          systemFaceCache.size > 0
        ) {
          const oldest = systemFaceCache.keys().next().value as string | undefined;
          if (oldest === undefined) break;
          const evicted = systemFaceCache.get(oldest);
          systemFaceCache.delete(oldest);
          systemFaceCacheBytes -= evicted ? evicted.byteLength : 0;
        }
        systemFaceCache.set(id, bytes);
        systemFaceCacheBytes += bytes.byteLength;
      }
      return bytes;
    } finally {
      systemFacePending.delete(id);
    }
  })();
  systemFacePending.set(id, pending);
  return pending;
}

async function registerSystemFace(
  face: SystemFace,
  options: {
    generics?: string[];
    scripts?: string[];
    emoji?: boolean;
    mono?: boolean;
  } = {},
): Promise<boolean> {
  const bytes = await systemFetchFace(face.id);
  let registered = false;
  try {
    registered = register_system_face(bytes, face.family, options.mono === true);
  } catch (error) {
    console.warn("[kitesurf-fonts] system face registration failed:", String(error));
  }
  for (const generic of options.generics ?? []) {
    try {
      map_system_generic(generic, face.family);
    } catch {}
  }
  for (const script of options.scripts ?? []) {
    try {
      append_system_fallback(script, face.family);
    } catch {}
  }
  if (options.emoji) {
    try {
      append_system_emoji(face.family);
    } catch {}
  }
  return registered;
}

function firstFace(match: SystemMatch): SystemFace | null {
  return Array.isArray(match.faces) && match.faces.length > 0 ? match.faces[0] : null;
}

function warnSystemMissing(kind: string, match: SystemMatch): void {
  const reason = typeof match.reason === "string" ? match.reason : "missing_coverage";
  const missing =
    typeof match.missing === "number" && Number.isFinite(match.missing) ? match.missing : 0;
  const message = `missing coverage: ${kind} reason=${reason} missing=${missing}`;
  noteSystemDiagnostic(message);
  console.warn(`[kitesurf-fonts] ${message}`);
}

function systemFallbackMatch(kind: string, match: SystemMatch) {
  // A face that misses the representative sample can still render other characters.
  const face = firstFace(match);
  const covered = face !== null && match.covered === true;
  if (!covered) warnSystemMissing(kind, match);
  return { face, covered };
}

async function ensureSystemRole(
  role: "sans" | "sans-italic" | "serif" | "mono" | "system-ui",
  family: string,
  options: {
    weight?: number;
    style?: string;
    text?: string;
    generics?: string[];
    mono?: boolean;
  },
): Promise<void> {
  const match = await systemMatch(family, options);
  const face = firstFace(match);
  if (!face) {
    if (role === "sans") {
      throw new Error(
        "[kitesurf-fonts] no system sans-serif font is available; install a base font or use the bundled build",
      );
    }
    warnSystemMissing(role, match);
    return;
  }
  if (role === "sans") systemFamilies.sans = face.family;
  if (role === "serif") systemFamilies.serif = face.family;
  if (role === "mono") systemFamilies.mono = face.family;
  await registerSystemFace(face, {
    generics: options.generics,
    mono: options.mono,
  });
}

async function ensureSystemCore(): Promise<void> {
  if (systemCorePromise) return systemCorePromise;
  systemCorePromise = (async () => {
    const startedAt =
      typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
    await ensureSystemRole("sans", "sans", {
      weight: 400,
      style: "normal",
      text: "Aa",
      generics: ["sans-serif", "ui-sans-serif"],
    });
    await ensureSystemRole("sans-italic", "sans", {
      weight: 400,
      style: "italic",
      text: "Aa",
      generics: ["sans-serif", "ui-sans-serif"],
    }).catch(() => {});
    let systemUi: SystemFace | null = null;
    try {
      systemUi = firstFace(
        await systemMatch("system-ui", { weight: 400, style: "normal", text: "Aa" }),
      );
    } catch {}
    if (systemUi) {
      await registerSystemFace(systemUi, { generics: ["system-ui"] });
    } else if (systemFamilies.sans) {
      try {
        map_system_generic("system-ui", systemFamilies.sans);
      } catch {}
    }
    try {
      finish_system_fonts();
    } catch {}
    const finishedAt =
      typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
    const coreMessage = `system core sans=${safeFamilyName(systemFamilies.sans ?? "?")}`;
    noteSystemDiagnostic(coreMessage);
    console.log(`[kitesurf-fonts] ${coreMessage} in ${(finishedAt - startedAt).toFixed(1)}ms`);
  })();
  return systemCorePromise;
}

async function ensureSystemSerif(): Promise<void> {
  try {
    await ensureSystemRole("serif", "serif", {
      weight: 400,
      style: "normal",
      text: "Aa",
      generics: ["serif", "ui-serif"],
    });
  } catch (error) {
    console.warn("[kitesurf-fonts] system serif unavailable:", String(error));
  }
}

async function ensureSystemMono(): Promise<void> {
  try {
    await ensureSystemRole("mono", "mono", {
      weight: 400,
      style: "normal",
      text: "Mm0",
      generics: ["monospace", "ui-monospace"],
      mono: true,
    });
  } catch (error) {
    console.warn("[kitesurf-fonts] system monospace unavailable:", String(error));
  }
}

async function ensureSystemEmoji(): Promise<void> {
  if (systemEmojiPromise) return systemEmojiPromise;
  systemEmojiPromise = (async () => {
    const match = await systemMatch("emoji", { weight: 400, style: "normal", text: "\u{1F600}" });
    const { face, covered } = systemFallbackMatch("emoji", match);
    if (!face) return;
    systemFamilies.emoji = face.family;
    await registerSystemFace(face, { emoji: true });
    if (!covered) return;
    noteSystemDiagnostic(`system emoji family=${safeFamilyName(face.family)} bytes=${face.bytes}`);
    console.log(
      `[kitesurf-fonts] registered system emoji ${safeFamilyName(face.family)} (${face.bytes} bytes)`,
    );
  })().catch((error) => {
    systemEmojiPromise = null;
    console.warn("[kitesurf-fonts] system emoji unavailable:", String(error));
  });
  return systemEmojiPromise;
}

interface CjkRequest {
  script: string;
  locale: string;
  text: string;
}

function cjkRequests(): CjkRequest[] {
  const locale = systemLocale.toLowerCase();
  const hasKana = systemKana || locale.startsWith("ja");
  const hasHang = systemHang || locale.startsWith("ko");
  const requests: CjkRequest[] = [];
  if (hasKana) requests.push({ script: "Hani", locale: "ja", text: "\u4e16" });
  if (hasHang) requests.push({ script: "Hang", locale: "ko", text: "\ud55c" });
  if (!hasKana && !hasHang) {
    const selected = locale.startsWith("ja")
      ? "ja"
      : locale.startsWith("ko")
        ? "ko"
        : locale.startsWith("zh")
          ? systemLocale
          : "";
    requests.push({
      script: selected === "ko" ? "Hang" : "Hani",
      locale: selected,
      text: selected === "ko" ? "\ud55c" : "\u4e16",
    });
  }
  if (locale.startsWith("ja") && !requests.some((request) => request.script === "Hani")) {
    requests.push({ script: "Hani", locale: "ja", text: "\u4e16" });
  }
  return requests;
}

async function ensureSystemCjk(): Promise<void> {
  const requests = cjkRequests();
  await Promise.all(requests.map((request) => ensureSystemCjkRequest(request)));
  try {
    finish_system_fonts();
  } catch {}
}

function ensureSystemCjkRequest(request: CjkRequest): Promise<void> {
  const key = `${request.script}:${request.locale}`;
  const existing = systemCjkPending.get(key);
  if (existing) return existing;
  const pending = (async () => {
    try {
      const match = await systemMatch("script", {
        script: request.script,
        locale: request.locale || undefined,
        weight: 400,
        style: "normal",
        text: request.text,
      });
      const { face, covered } = systemFallbackMatch(
        `script=${request.script} locale=${request.locale || "-"}`,
        match,
      );
      if (!face) return;
      await registerSystemFace(face, { scripts: ["Hani", "Hira", "Kana", "Hang"] });
      if (!covered) return;
      noteSystemDiagnostic(
        `system CJK script=${request.script} locale=${request.locale || "-"} family=${safeFamilyName(face.family)} bytes=${face.bytes}`,
      );
      console.log(
        `[kitesurf-fonts] registered system CJK ${safeFamilyName(face.family)} for ${request.script}/${request.locale || "-"} (${face.bytes} bytes)`,
      );
    } catch (error) {
      console.warn(
        `[kitesurf-fonts] system CJK ${request.script}/${request.locale || "-"} unavailable:`,
        String(error),
      );
    }
  })();
  systemCjkPending.set(key, pending);
  return pending;
}

const SCRIPT_SAMPLES: Record<string, string> = {
  Arab: "\u0627",
  Hebr: "\u05d0",
  Deva: "\u0905",
  Thai: "\u0e01",
  Beng: "\u0985",
  Taml: "\u0b85",
  Ethi: "\u1200",
  Guru: "\u0a05",
  Gujr: "\u0a85",
  Knda: "\u0c85",
  Mlym: "\u0d05",
  Telu: "\u0c05",
  Sinh: "\u0d85",
  Mymr: "\u1000",
  Khmr: "\u1780",
  Laoo: "\u0e81",
  Geor: "\u10d0",
  Armn: "\u0531",
};

async function ensureSystemScript(scriptFont: ScriptFont): Promise<void> {
  const existing = systemScriptPending.get(scriptFont.tag);
  if (existing) return existing;
  const pending = (async () => {
    const match = await systemMatch("script", {
      script: scriptFont.tag,
      weight: 400,
      style: "normal",
      text: SCRIPT_SAMPLES[scriptFont.tag] ?? "",
    });
    const { face, covered } = systemFallbackMatch(`script=${scriptFont.tag}`, match);
    if (!face) return;
    await registerSystemFace(face, { scripts: [scriptFont.tag] });
    if (!covered) return;
    noteSystemDiagnostic(
      `system script=${scriptFont.tag} family=${safeFamilyName(face.family)} bytes=${face.bytes}`,
    );
    console.log(
      `[kitesurf-fonts] registered system script ${scriptFont.tag} ${safeFamilyName(face.family)} (${face.bytes} bytes)`,
    );
  })().catch((error) => {
    systemScriptPending.delete(scriptFont.tag);
    console.warn(`[kitesurf-fonts] system script ${scriptFont.tag} unavailable:`, String(error));
  });
  systemScriptPending.set(scriptFont.tag, pending);
  return pending;
}

async function ensureSystemFonts(
  wants: ReturnType<typeof computeFontWants>,
  scan?: SourceFontScan,
): Promise<void> {
  systemLocale = scanCjkLocale(scan) || systemLocale;
  systemKana = scan ? scan.cjkRegions.includes("JP") : false;
  systemHang = scan ? scan.scripts.includes("Hang") : false;
  await ensureSystemCore();
  const tasks: Promise<void>[] = [];
  if (wants.serif) tasks.push(ensureSystemSerif());
  if (wants.mono) tasks.push(ensureSystemMono());
  if (wants.emoji) tasks.push(ensureSystemEmoji());
  if (wants.cjk) tasks.push(ensureSystemCjk());
  // Latn only names glyphs a UA profile lacks; the system sans face already covers Latin.
  for (const scriptFont of wants.scripts) {
    if (scriptFont.tag !== "Latn") tasks.push(ensureSystemScript(scriptFont));
  }
  if (tasks.length) await Promise.all(tasks);
  try {
    finish_system_fonts();
  } catch {}
}

export async function ensureFontsRegistered(scan?: SourceFontScan): Promise<void> {
  if (startupSystemFonts) {
    await ensureSystemFonts(computeFontWants(scan), scan);
    return;
  }
  decideFontMode(scan);
  const wants = computeFontWants(scan);
  const wantSymbols = wants.symbols;
  const wantSymbols2 = wants.symbols2;
  const wantEmoji = wants.emoji;
  const wantCjk = wants.cjk;
  const wantMono = wants.mono;
  const wantSerif = wants.serif;

  if (!pending) {
    pending = (async () => {
      const startedAt =
        typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
      try {
        // Only the sans pair is required; any other face settles empty and its generics use sans.
        const optional = (path: string): Promise<Uint8Array> =>
          fetchFaceBytes(path).catch((e) => {
            console.warn(`[kitesurf-fonts] ${path} fetch failed:`, String(e));
            return EMPTY_FACE;
          });
        const [core, mono] = await Promise.all([
          Promise.all(
            uaFacePaths(wantSerif).map((path, i) => {
              if (!path) return EMPTY_FACE;
              return i < REQUIRED_UA_FACES ? fetchFaceBytes(path) : optional(path);
            }),
          ),
          wantMono ? optional(MONO_FONT_PATH) : EMPTY_FACE,
        ]);

        registerUaFaces(core, mono);

        const completedAt =
          typeof performance !== "undefined" && performance.now ? performance.now() : Date.now();
        const faces = [...core, mono].filter((bytes) => bytes.byteLength > 0);
        const totalBytes = faces.reduce((sum, bytes) => sum + bytes.byteLength, 0);
        console.log(
          `[kitesurf-fonts] registered ${faces.length} Latin UA fonts (${activeUaFontProfile().name}, ${totalBytes} bytes) in ${(completedAt - startedAt).toFixed(1)}ms`,
        );
      } catch (e) {
        console.warn("[kitesurf-fonts] registration failed:", String(e));
        pending = null;
        throw e;
      }
    })();
  }

  try {
    await pending;
  } catch {
    return;
  }

  const tasks: Promise<void>[] = [];
  if (wantSymbols) tasks.push(registerSymbolsIfNeeded(wantSymbols2).catch(() => {}));
  if (wantEmoji) tasks.push(registerEmojiIfNeeded().catch(() => {}));
  if (wantCjk) tasks.push(registerCjkIfNeeded().catch(() => {}));
  for (const font of wants.cjkRegions) {
    tasks.push(registerCjkRegionIfNeeded(font).catch(() => {}));
  }
  for (const scriptFont of wants.scripts) {
    tasks.push(registerScriptIfNeeded(scriptFont).catch(() => {}));
  }
  if (tasks.length) await Promise.all(tasks);
}

let deferStarted = false;
let deferDemandArmed = false;
let deferCoreRegistered = false;
let deferCoreFailed = false;
let deferDegradedReads = 0;
let deferFetchSettled: Promise<void> | null = null;
let deferWants: ReturnType<typeof computeFontWants> | null = null;
const deferRegistered = new Set<string>();
let deferCorePaths: string[] = [];
const deferStash: {
  core: (Uint8Array | null)[];
  mono: Uint8Array | null;
  symbols: Uint8Array | null;
  symbols2: Uint8Array | null;
  emoji: Uint8Array | null;
  cjk: Uint8Array | null;
  cjkRegions: Map<CjkRegionFont["region"], Uint8Array>;
  scripts: Map<string, Uint8Array>;
} = {
  core: [],
  mono: null,
  symbols: null,
  symbols2: null,
  emoji: null,
  cjk: null,
  cjkRegions: new Map(),
  scripts: new Map(),
};

export function fontsDeferredUnregistered(): boolean {
  return deferStarted && !deferCoreRegistered;
}

export function deferredDegradedReads(): number {
  return deferDegradedReads;
}

const deferSettledEmpty = new Set<string>();

function deferCoreBytesReady(): boolean {
  if (!deferWants) return false;
  const coreReady = deferCorePaths.every(
    (path, i) => !path || deferStash.core[i] || deferSettledEmpty.has(path),
  );
  if (!coreReady) return false;
  if (deferWants.mono && !deferStash.mono && !deferSettledEmpty.has("mono")) return false;
  return true;
}

function tryRegisterDeferred(): boolean {
  const wants = deferWants;
  if (!wants || deferCoreFailed) return false;
  if (!deferCoreRegistered) {
    if (!deferCoreBytesReady()) return false;
    try {
      const registered = registerUaFaces(
        deferStash.core.map((bytes) => bytes ?? EMPTY_FACE),
        deferStash.mono ?? EMPTY_FACE,
      );
      deferCoreRegistered = true;
      // Completed handoffs no longer need their JS staging buffers.
      if (registered) {
        deferStash.core.fill(null);
        deferStash.mono = null;
      }
      console.log("[kitesurf-fonts] deferred: registered Latin UA core on demand");
    } catch (e) {
      console.warn("[kitesurf-fonts] deferred core registration failed:", String(e));
      deferCoreFailed = true;
      return false;
    }
  }
  const group = (name: string, run: () => void): void => {
    if (deferRegistered.has(name)) return;
    deferRegistered.add(name);
    try {
      run();
    } catch (e) {
      console.warn(`[kitesurf-fonts] deferred ${name} registration failed:`, String(e));
    }
  };
  const { symbols, symbols2, emoji, cjk } = deferStash;
  if (wants.symbols && symbols && (!wants.symbols2 || symbols2)) {
    group("symbols", () => {
      if (register_symbol_fonts(symbols, symbols2 ?? new Uint8Array(0))) {
        deferStash.symbols = null;
        deferStash.symbols2 = null;
      }
    });
  }
  if (wants.emoji && emoji) {
    group("emoji", () => {
      if (register_emoji_font(emoji)) deferStash.emoji = null;
    });
  }
  if (wants.cjk && cjk) {
    group("cjk", () => {
      if (register_cjk_font(cjk)) deferStash.cjk = null;
    });
  }
  for (const [region, bytes] of deferStash.cjkRegions) {
    group(`cjk:${region}`, () => {
      if (register_cjk_region_font(bytes, region)) deferStash.cjkRegions.delete(region);
    });
  }
  for (const [tag, bytes] of deferStash.scripts) {
    group(`script:${tag}`, () => {
      if (register_script_font(bytes, tag)) deferStash.scripts.delete(tag);
    });
  }
  return true;
}

export function startFontPrefetch(scan?: SourceFontScan): void {
  if (startupSystemFonts) {
    void ensureFontsRegistered(scan);
    return;
  }
  if (deferStarted) return;
  deferStarted = true;
  decideFontMode(scan);
  const wants = computeFontWants(scan);
  deferWants = wants;
  const all: Promise<void>[] = [];
  const kick = (
    fetchPromise: Promise<Uint8Array>,
    storeBytes: (bytes: Uint8Array) => void,
    tier: "core" | "optional" | "extra",
    label: string,
  ): void => {
    all.push(
      fetchPromise
        .then((bytes) => {
          storeBytes(bytes);
          if (deferDemandArmed) tryRegisterDeferred();
        })
        .catch((error) => {
          if (tier === "core") deferCoreFailed = true;
          if (tier === "optional") {
            deferSettledEmpty.add(label);
            if (deferDemandArmed) tryRegisterDeferred();
          }
          console.warn(`[kitesurf-fonts] deferred ${label} fetch failed:`, String(error));
        }),
    );
  };
  deferCorePaths = uaFacePaths(wants.serif);
  deferStash.core = deferCorePaths.map(() => null);
  deferCorePaths.forEach((path, i) => {
    if (!path) return;
    kick(
      fetchFaceBytes(path),
      (bytes) => (deferStash.core[i] = bytes),
      i < REQUIRED_UA_FACES ? "core" : "optional",
      path,
    );
  });
  if (wants.mono) {
    kick(fetchFaceBytes(MONO_FONT_PATH), (bytes) => (deferStash.mono = bytes), "optional", "mono");
  }
  if (wants.symbols) {
    kick(
      fetchFaceBytes(SYMBOL_FONT_PATHS[0]),
      (bytes) => (deferStash.symbols = bytes),
      "extra",
      "symbols",
    );
  }
  if (wants.symbols2) {
    kick(
      fetchFaceBytes(SYMBOL_FONT_PATHS[1]),
      (bytes) => (deferStash.symbols2 = bytes),
      "extra",
      "symbols2",
    );
  }
  if (wants.emoji) {
    kick(
      fetchBytes(EMOJI_SHAPING_PATH).catch(() => fetchBytes(EMOJI_FONT_PATH)),
      (bytes) => (deferStash.emoji = bytes),
      "extra",
      "emoji",
    );
  }
  if (wants.cjk) {
    kick(
      fetchCjkFace(CJK_FONT_PATH, CJK_SHAPING_PATH),
      (bytes) => (deferStash.cjk = bytes),
      "extra",
      "cjk",
    );
  }
  for (const font of wants.cjkRegions) {
    kick(
      fetchCjkFace(font.path, font.shapingPath),
      (bytes) => deferStash.cjkRegions.set(font.region, bytes),
      "extra",
      `cjk:${font.region}`,
    );
  }
  for (const scriptFont of wants.scripts) {
    kick(
      fetchFaceBytes(scriptFont.path),
      (bytes) => deferStash.scripts.set(scriptFont.tag, bytes),
      "extra",
      scriptFont.tag,
    );
  }
  deferFetchSettled = Promise.all(all).then(() => undefined);
  console.log(
    `[kitesurf-fonts] deferred: prefetching ${all.length} face file(s), registration on demand`,
  );
}

let parkedFontReleaseHook: (() => void) | null = null;

export function setParkedFontReleaseHook(cb: () => void): void {
  parkedFontReleaseHook = cb;
}

export function fontDemandFired(): boolean {
  return deferDemandArmed;
}

function releaseParkedFonts(): void {
  try {
    parkedFontReleaseHook?.();
  } catch (e) {
    console.warn("[kitesurf-fonts] parked font release failed:", String(e));
  }
}

export function demandDeferredFonts(): boolean {
  if (!deferStarted) {
    releaseParkedFonts();
    return true;
  }
  deferDemandArmed = true;
  releaseParkedFonts();
  if (deferCoreRegistered) {
    tryRegisterDeferred();
    return true;
  }
  if (tryRegisterDeferred()) return true;
  deferDegradedReads++;
  if (deferDegradedReads === 1) {
    console.warn(
      "[kitesurf-fonts] layout demand before deferred font bytes arrived — degraded read",
    );
  }
  return false;
}

export async function awaitDeferredFontBytes(): Promise<void> {
  if (!deferStarted || deferFetchSettled === null) return;
  await deferFetchSettled;
}

export async function awaitDeferredFonts(): Promise<void> {
  if (!deferStarted) return;
  deferDemandArmed = true;
  releaseParkedFonts();
  if (deferFetchSettled) await deferFetchSettled;
  tryRegisterDeferred();
}

export function fontDeferActive(): boolean {
  return !startupSystemFonts && startupFontDefer === true && startupPixelFree === true;
}
