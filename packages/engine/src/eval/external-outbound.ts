// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
// https://opensource.org/licenses/Apache-2.0

// Selects the host egress hook or Worker global fetch for forwarded requests.

export interface ExternalOutbound {
  /** Performs the host's external fetch; request failures propagate to the caller. */
  fetch(request: Request): Promise<Response>;
}

/** Uses the host fetch when configured, otherwise the Worker global `fetch`. */
export function directExternalFetch(
  externalOutbound: ExternalOutbound | undefined,
  request: Request,
): Promise<Response> {
  return externalOutbound ? externalOutbound.fetch(request) : fetch(request);
}

/** Reissue a parser image GET once when native egress loses the connection before headers. */
export async function fetchParserImage(request: Request): Promise<Response> {
  try {
    return await fetch(request);
  } catch (error) {
    if (
      request.signal.aborted ||
      !(error instanceof Error) ||
      error.message !== "Network connection lost." ||
      ("retryable" in error && error.retryable === false)
    ) {
      throw error;
    }
  }
  try {
    return await fetch(request);
  } catch (error) {
    const message = String(error instanceof Error ? error.message : error);
    if (message.startsWith("internal error")) {
      // The page shim retries an internal error. Keep this failure within the egress budget.
      throw new Error(`Parser image retry failed: ${message}`, { cause: error });
    }
    throw error;
  }
}
