// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// CDP command errors that carry a protocol `data` payload.

export class CdpCommandError extends Error {
  readonly code: number;
  readonly data?: Record<string, unknown>;

  constructor(init: { message: string; code?: number; data?: Record<string, unknown> }) {
    super(init.message);
    this.name = "CdpCommandError";
    this.code = init.code ?? -32000;
    this.data = init.data;
  }
}

// A destroyed isolate is a navigation race, not a failed expression. Clients retry the command
// once the new document's context is published.
export function retryableContextDestroyed(executionContextId?: number): CdpCommandError {
  return new CdpCommandError({
    message: "Execution context was destroyed.",
    data: {
      retryable: true,
      reason: "execution-context-destroyed",
      ...(executionContextId && executionContextId > 0 ? { executionContextId } : {}),
    },
  });
}
