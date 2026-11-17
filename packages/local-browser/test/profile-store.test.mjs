// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, writeFileSync, rmSync, symlinkSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { ProfileStore } from "../src/profile-store.mjs";

function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), "kitesurf-profile-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return { dir, path: join(dir, "account.sqlite") };
}
const state = (token) => ({
  cookies: [
    {
      name: "sid",
      value: token,
      host: "example.com",
      path: "/private",
      secure: true,
      httpOnly: true,
      sameSite: "Strict",
      hostOnly: true,
      expiresAt: Date.now() + 3600000,
      createdAt: 100,
    },
  ],
  localStorage: [
    [
      "https://example.com",
      [
        ["token", token],
        ["__proto__", "safe"],
      ],
    ],
  ],
});

test("full cookie metadata and origin data survive reopen and backup", (t) => {
  const { dir, path } = fixture(t);
  let db = new ProfileStore(path, { create: true });
  db.acquire("first");
  const saved = state("T2");
  db.save("first", 1, saved);
  const backup = db.backup(join(dir, "backup.sqlite"));
  db.close();
  for (const file of [path, backup]) {
    db = new ProfileStore(file);
    assert.deepEqual(db.read(), { revision: 1, state: saved });
    db.close();
  }
});

test("missing, damaged, unknown-format and existing-create files never fall back or overwrite", (t) => {
  const { path } = fixture(t);
  assert.throws(() => new ProfileStore(path), /Cannot open/);
  writeFileSync(path, "preserve these bytes");
  const before = readFileSync(path);
  assert.throws(() => new ProfileStore(path));
  assert.throws(() => new ProfileStore(path, { create: true }));
  assert.deepEqual(readFileSync(path), before);
  rmSync(path);
  const db = new DatabaseSync(path);
  db.exec(
    "PRAGMA application_id=1263751238; PRAGMA user_version=99; CREATE TABLE future (id INTEGER)",
  );
  db.close();
  const future = readFileSync(path);
  assert.throws(() => new ProfileStore(path), /Unsupported profile format/);
  assert.deepEqual(readFileSync(path), future);
});

test("a profile has one owner, including another process and a symlink alias", (t) => {
  const { dir, path } = fixture(t);
  const db = new ProfileStore(path, { create: true });
  try {
    assert.throws(() => new ProfileStore(path), /locked/);
    if (process.platform !== "win32") {
      const alias = join(dir, "alias.sqlite");
      symlinkSync(path, alias);
      assert.throws(() => new ProfileStore(alias), /locked/);
    }
    const child = spawnSync(
      process.execPath,
      [
        "--input-type=module",
        "-e",
        `import { ProfileStore } from ${JSON.stringify(new URL("../src/profile-store.mjs", import.meta.url).href)}; new ProfileStore(${JSON.stringify(path)});`,
      ],
      { encoding: "utf8" },
    );
    assert.notEqual(child.status, 0);
    assert.match(child.stderr, /locked/);
  } finally {
    db.close();
  }
});

test("stale owner, stale revision and deletion cannot resurrect older credentials", (t) => {
  const { path } = fixture(t);
  const db = new ProfileStore(path, { create: true });
  try {
    db.acquire("first");
    const first = state("T1");
    db.save("first", 1, first);
    assert.throws(() => db.acquire("second"), /live session/);
    assert.throws(() => db.save("old", 2, first), /Stale/);
    db.save("first", 2, { cookies: [], localStorage: [] });
    assert.throws(() => db.save("first", 1, first), /Stale/);
    assert.deepEqual(db.read().state, { cookies: [], localStorage: [] });
  } finally {
    db.close();
  }
});

test("process termination releases ownership and preserves committed revisions", (t) => {
  const { path } = fixture(t);
  const child = spawnSync(process.execPath, [
    "--input-type=module",
    "-e",
    `
    import { ProfileStore } from ${JSON.stringify(new URL("../src/profile-store.mjs", import.meta.url).href)};
    const db = new ProfileStore(${JSON.stringify(path)}, {create:true}); db.acquire('crash');
    db.save('crash', 1, ${JSON.stringify(state("committed"))}); process.kill(process.pid, 'SIGKILL');
  `,
  ]);
  assert.notEqual(child.status, 0);
  const db = new ProfileStore(path);
  try {
    assert.equal(db.read().revision, 1);
    assert.equal(db.read().state.cookies[0].value, "committed");
  } finally {
    db.close();
  }
});

test("a quota rejection leaves the revision reusable for a corrected snapshot", async (t) => {
  const { path } = fixture(t);
  const db = new ProfileStore(path, { create: true });
  db.acquire("writer");
  try {
    const save = (state) =>
      db.fetch(
        new Request("http://profile/save", {
          method: "POST",
          body: JSON.stringify({ owner: "writer", revision: 1, state }),
        }),
      );
    const rejected = await save({
      cookies: [],
      localStorage: [["https://example.com", [["large", "x".repeat(3_000_000)]]]],
    });
    assert.equal(rejected.status, 422);
    assert.equal((await rejected.json()).code, "INVALID_STATE");
    assert.equal(db.read().revision, 0);
    assert.equal((await save(state("fixed"))).status, 200);
    assert.equal(db.read().state.cookies[0].value, "fixed");
  } finally {
    db.close();
  }
});
