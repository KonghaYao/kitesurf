// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

export class Attachments {
  records = new Map();
  owners = new Map();
  explicit = new Map();
  autoClient = null;
  barrier = Promise.resolve();
  management = Promise.resolve();
  constructor(send) {
    this.send = send;
  }
  /** @returns {void} */
  add(client) {
    this.records.set(client, { closed: false, sessions: new Map(), pending: new Set() });
  }
  /** @returns {void} */
  own(client, sessionId, targetId) {
    const record = this.records.get(client);
    if (!record) return;
    this.owners.set(sessionId, client);
    record.sessions.set(sessionId, targetId);
  }
  /** @returns {void} */
  forget(sessionId) {
    this.records.get(this.owners.get(sessionId))?.sessions.delete(sessionId);
    this.owners.delete(sessionId);
  }
  /** @returns {void} */
  event(message) {
    if (message.method === "Target.attachedToTarget") {
      const { sessionId, targetInfo } = message.params;
      const owner =
        this.explicit.get(targetInfo.targetId) ||
        this.owners.get(message.sessionId) ||
        this.autoClient;
      if (owner) this.own(owner, sessionId, targetInfo.targetId);
    }
    if (message.method === "Target.detachedFromTarget") this.forget(message.params.sessionId);
  }
  /** @returns {boolean} */
  visible(client, message) {
    if (message.method === "Target.attachedToTarget")
      return this.owners.get(message.params.sessionId) === client;
    return !message.sessionId || this.owners.get(message.sessionId) === client;
  }
  /** @returns {Promise<object>} */
  async attach(client, params, parent) {
    this.explicit.set(params.targetId, client);
    try {
      const result = await this.send("Target.attachToTarget", params, parent);
      this.own(client, result.sessionId, params.targetId);
      return result;
    } finally {
      this.explicit.delete(params.targetId);
    }
  }
  /** @returns {Promise<object>} */
  async call(client, method, params = {}, sessionId) {
    const record = this.records.get(client);
    if (!record || record.closed) throw new Error("CDP connection ended");
    const barrier = this.barrier;
    const operation = async () => {
      await barrier;
      if (record.closed) throw new Error("CDP connection ended");
      if (sessionId && this.owners.get(sessionId) !== client)
        throw new Error("CDP attachment expired or belongs to another connection; attach again");
      if (method === "Target.attachToTarget") return this.attach(client, params, sessionId);
      if (method === "Target.detachFromTarget" && this.owners.get(params.sessionId) !== client)
        throw new Error("This connection does not own that attachment");
      // The engine's auto-attach flag is global, including requests from a page endpoint.
      if (method === "Target.setAutoAttach") this.autoClient = params.autoAttach ? client : null;
      const result = await this.send(method, params, sessionId);
      if (method === "Target.attachToBrowserTarget") this.own(client, result.sessionId, "browser");
      if (method === "Target.detachFromTarget") this.forget(params.sessionId);
      if (method === "Target.setAutoAttach" && !sessionId && params.autoAttach && !record.closed) {
        const { targetInfos } = await this.send("Target.getTargets");
        for (const target of targetInfos) {
          if (target.type !== "page" || [...record.sessions.values()].includes(target.targetId))
            continue;
          await this.attach(client, { targetId: target.targetId, flatten: true });
        }
      }
      return result;
    };
    // Serialize attachment changes so an emitted event precedes its exact request's reply.
    const managed = [
      "Target.attachToTarget",
      "Target.setAutoAttach",
      "Target.detachFromTarget",
      "Target.attachToBrowserTarget",
    ].includes(method);
    const promise = managed ? this.management.then(operation, operation) : operation();
    if (managed) this.management = promise.catch(() => {});
    record.pending.add(promise);
    try {
      return await promise;
    } finally {
      record.pending.delete(promise);
    }
  }
  /** @returns {Promise<void>} */
  retire(client) {
    const record = this.records.get(client);
    if (!record || record.closed) return this.barrier;
    record.closed = true;
    const wasAuto = this.autoClient === client;
    const pending = [...record.pending];
    const cleanup = async () => {
      await Promise.allSettled(pending);
      if (wasAuto) {
        await this.send("Target.setAutoAttach", { autoAttach: false, flatten: true });
        if (this.autoClient === client) this.autoClient = null;
      }
      for (const sessionId of record.sessions.keys()) {
        await this.send("Target.detachFromTarget", { sessionId });
        this.forget(sessionId);
      }
      this.records.delete(client);
    };
    this.barrier = this.barrier.then(cleanup, cleanup).catch((error) => {
      console.error(`CDP attachment cleanup failed: ${error.message}`);
    });
    return this.barrier;
  }
}
