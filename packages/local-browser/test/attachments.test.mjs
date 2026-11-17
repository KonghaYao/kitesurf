// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

import { test } from "node:test";
import assert from "node:assert/strict";
import { Attachments } from "../src/attachments.mjs";

test("disconnect during attach releases the late session before a new connection acts", async () => {
  let reply;
  let entered;
  const started = new Promise((resolve) => {
    entered = resolve;
  });
  const calls = [];
  const old = {};
  const next = {};
  const attachments = new Attachments(async (method, params) => {
    calls.push([method, params]);
    if (method === "Target.attachToTarget") {
      attachments.event({
        method: "Target.attachedToTarget",
        params: { sessionId: "late", targetInfo: { targetId: "page" } },
      });
      entered();
      return new Promise((resolve) => {
        reply = resolve;
      });
    }
    return {};
  });
  attachments.add(old);
  const attaching = attachments.call(old, "Target.attachToTarget", { targetId: "page" });
  await started;
  const retired = attachments.retire(old);
  attachments.add(next);
  const reading = attachments.call(next, "Browser.getVersion");
  assert.equal(calls.length, 1);
  reply({ sessionId: "late" });
  await Promise.all([attaching, retired, reading]);
  assert.deepEqual(
    calls.map(([method]) => method),
    ["Target.attachToTarget", "Target.detachFromTarget", "Browser.getVersion"],
  );
  assert.equal(attachments.owners.size, 0);
  await assert.rejects(
    attachments.call(next, "Runtime.evaluate", { expression: "dangerous()" }, "late"),
    /attachment expired/,
  );
  assert.equal(calls.length, 3, "expired commands must never reach the browser");
});

test("a viewer attachment belongs only to that viewer while auto-attachment is enabled", async () => {
  const controller = {};
  const viewer = {};
  let event;
  const attachments = new Attachments(async (method, params) => {
    if (method === "Target.getTargets") return { targetInfos: [] };
    if (method === "Target.attachToTarget") {
      event = {
        method: "Target.attachedToTarget",
        params: { sessionId: "viewer", targetInfo: { targetId: params.targetId } },
      };
      attachments.event(event);
      return { sessionId: "viewer" };
    }
    return {};
  });
  attachments.add(controller);
  attachments.add(viewer);
  await attachments.call(controller, "Target.setAutoAttach", { autoAttach: true });
  await attachments.call(viewer, "Target.attachToTarget", { targetId: "page" });
  assert.equal(attachments.visible(controller, event), false);
  assert.equal(attachments.visible(viewer, event), true);
  await attachments.retire(controller);
  assert.equal(attachments.owners.get("viewer"), viewer);
});

test("page-mode auto-attachments are released without closing their targets", async () => {
  const controller = {};
  const calls = [];
  const attachments = new Attachments(async (method, params) => {
    calls.push([method, params]);
    return {};
  });
  attachments.add(controller);
  attachments.own(controller, "page-session", "page");
  await attachments.call(controller, "Target.setAutoAttach", { autoAttach: true }, "page-session");
  attachments.event({
    method: "Target.attachedToTarget",
    params: { sessionId: "automatic", targetInfo: { targetId: "another-page" } },
  });
  await attachments.retire(controller);
  assert.deepEqual(
    calls
      .filter(([method]) => method === "Target.detachFromTarget")
      .map(([, params]) => params.sessionId),
    ["page-session", "automatic"],
  );
  assert.equal(
    calls.some(([method]) => method === "Target.closeTarget"),
    false,
  );
  assert.equal(attachments.owners.size, 0);
});
