// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { DatabaseSync } from "node:sqlite";
import { closeSync, openSync, realpathSync, statSync, unlinkSync } from "node:fs";
import { resolve } from "node:path";
import { createHash } from "node:crypto";

const APPLICATION_ID = 0x4b535046;
const FORMAT = 1;
const MAX_STATE_BYTES = 64 * 1024 * 1024;
const emptyState = () => ({ cookies: [], localStorage: [] });

/** @returns {string} */
export function validateState(state) {
  if (!state || !Array.isArray(state.cookies) || !Array.isArray(state.localStorage))
    throw new Error("Invalid profile state");
  if (state.cookies.length > 3000) throw new Error("Too many cookies");
  for (const c of state.cookies) {
    if (
      !c ||
      !["name", "value", "host", "path"].every((k) => typeof c[k] === "string") ||
      !["secure", "httpOnly", "hostOnly"].every((k) => typeof c[k] === "boolean") ||
      ![null, "Strict", "Lax", "None"].includes(c.sameSite) ||
      !Number.isFinite(c.createdAt) ||
      !(c.expiresAt === null || Number.isFinite(c.expiresAt))
    )
      throw new Error("Invalid profile cookie");
  }
  const origins = new Set();
  for (const entry of state.localStorage) {
    if (!Array.isArray(entry) || entry.length !== 2) throw new Error("Invalid storage origin");
    const [origin, pairs] = entry;
    const url = new URL(origin);
    if (
      !["http:", "https:"].includes(url.protocol) ||
      url.origin !== origin ||
      origins.has(origin) ||
      !Array.isArray(pairs)
    )
      throw new Error("Invalid storage origin");
    origins.add(origin);
    const keys = new Set();
    let bytes = 0;
    for (const pair of pairs) {
      if (
        !Array.isArray(pair) ||
        pair.length !== 2 ||
        !pair.every((v) => typeof v === "string") ||
        keys.has(pair[0])
      )
        throw new Error("Invalid storage entry");
      keys.add(pair[0]);
      bytes += 2 * (pair[0].length + pair[1].length);
    }
    if (bytes > 5 * 1024 * 1024) throw new Error("Storage quota exceeded");
  }
  const json = JSON.stringify(state);
  if (Buffer.byteLength(json) > MAX_STATE_BYTES) throw new Error("Profile exceeds 64 MiB");
  return json;
}

export class ProfileStore {
  #db;
  #owner;
  constructor(path, { create = false } = {}) {
    path = resolve(path);
    let created = false;
    try {
      if (create) {
        closeSync(openSync(path, "wx", 0o600));
        created = true;
      }
      this.path = realpathSync(path);
      const stat = statSync(this.path);
      if (!stat.isFile() || stat.nlink !== 1)
        throw new Error("Profile must be a regular file without hard links");
      this.#db = new DatabaseSync(this.path, { timeout: 0 });
      this.#db.exec("PRAGMA locking_mode=EXCLUSIVE; PRAGMA synchronous=FULL; BEGIN EXCLUSIVE");
      if (created) {
        this.#db.exec(
          `PRAGMA application_id=${APPLICATION_ID}; PRAGMA user_version=${FORMAT}; CREATE TABLE profile (id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL, state TEXT NOT NULL)`,
        );
        this.#db.prepare("INSERT INTO profile VALUES (1, 0, ?)").run(JSON.stringify(emptyState()));
      } else {
        const app = this.#db.prepare("PRAGMA application_id").get().application_id;
        const version = this.#db.prepare("PRAGMA user_version").get().user_version;
        if (app !== APPLICATION_ID || version !== FORMAT)
          throw new Error(`Unsupported profile format (${app}/${version})`);
        if (this.#db.prepare("PRAGMA quick_check").get().quick_check !== "ok")
          throw new Error("Profile integrity check failed");
      }
      this.#db.exec("COMMIT");
      this.read();
    } catch (error) {
      try {
        this.#db?.close();
      } catch {}
      if (created)
        try {
          unlinkSync(path);
        } catch {}
      throw new Error(`Cannot open profile ${path}: ${error.message}`, { cause: error });
    }
  }
  /** @returns {{ revision: number, state: object }} */
  read() {
    const row = this.#db.prepare("SELECT revision, state FROM profile WHERE id=1").get();
    if (!row || !Number.isSafeInteger(row.revision) || row.revision < 0)
      throw new Error("Invalid profile revision");
    const state = JSON.parse(row.state);
    validateState(state);
    return { revision: row.revision, state };
  }
  /** @returns {{ revision: number, state: object }} */
  acquire(owner) {
    if (typeof owner !== "string" || !owner) throw new Error("Missing session owner");
    if (this.#owner && this.#owner !== owner)
      throw new Error("Profile is already attached to a live session");
    this.#owner = owner;
    return this.read();
  }
  /** @returns {void} */
  save(owner, revision, state) {
    if (!this.#owner || owner !== this.#owner) throw new Error("Stale profile owner");
    if (!Number.isSafeInteger(revision)) throw new Error("Invalid revision");
    let json;
    try {
      json = validateState(state);
    } catch (error) {
      throw Object.assign(error, { profileCode: "INVALID_STATE" });
    }
    this.#db.exec("BEGIN EXCLUSIVE");
    try {
      const previous = this.#db.prepare("SELECT revision, state FROM profile WHERE id=1").get();
      if (previous.revision === revision && previous.state === json) {
        this.#db.exec("COMMIT");
        return;
      }
      if (revision !== previous.revision + 1) throw new Error("Stale or skipped profile revision");
      this.#db.prepare("UPDATE profile SET revision=?, state=? WHERE id=1").run(revision, json);
      this.#db.exec("COMMIT");
    } catch (error) {
      this.#db.exec("ROLLBACK");
      throw error;
    }
  }
  /** @returns {string} */
  backup(path) {
    const destination = resolve(path);
    // VACUUM INTO produces one consistent database; it refuses a nonempty destination.
    closeSync(openSync(destination, "wx", 0o600));
    try {
      this.#db.prepare("VACUUM INTO ?").run(destination);
    } catch (error) {
      unlinkSync(destination);
      throw error;
    }
    return destination;
  }
  /** @returns {Promise<Response>} */
  async fetch(request) {
    try {
      const body = await request.json();
      const route = new URL(request.url).pathname;
      if (route === "/load") return Response.json(this.acquire(body.owner));
      if (route === "/save") {
        this.save(body.owner, body.revision, body.state);
        return Response.json({ saved: true });
      }
      return new Response("Not found", { status: 404 });
    } catch (error) {
      return Response.json(
        { code: error.profileCode || "COMMIT_UNKNOWN", message: error.message },
        {
          status: error.profileCode === "INVALID_STATE" ? 422 : 409,
        },
      );
    }
  }
  /** @returns {{ path: string, revision: number, format: number, id: string }} */
  status() {
    const { revision } = this.#db.prepare("SELECT revision FROM profile WHERE id=1").get();
    return {
      path: this.path,
      revision,
      format: FORMAT,
      id: createHash("sha256").update(this.path).digest("hex").slice(0, 16),
    };
  }
  /** @returns {void} */
  close() {
    this.#owner = undefined;
    this.#db.close();
  }
}
