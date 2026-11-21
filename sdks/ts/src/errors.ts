// SDK error types and their mapping to and from the WIT `failure` variant.
//
// Mirrors the Python SDK set (09-09): NonRetryable, Retryable, Timeout, Cancelled,
// ProtocolError/Deadlock (kind `sdk_protocol`), InvalidString (kind `invalid_string`);
// Timeout, Cancelled, ProtocolError and InvalidString are NonRetryable subclasses.
// `kind` is matched against a step retry policy's non-retryable kinds by the host.

import type { ErrorInfo, Failure, Payload } from "./host.ts";

/** Reserved kind of a host `failure::timeout`. */
export const TIMEOUT = "timeout";
/** Reserved kind of a host `failure::cancelled`. */
export const CANCELLED = "cancelled";
/** SDK protocol violations (deadlock, journaled call in a step, call after the terminal). */
export const SDK_PROTOCOL = "sdk_protocol";
/** A string with a lone surrogate was about to cross the WIT boundary (PF-W4). */
export const INVALID_STRING = "invalid_string";
/** A value could not be encoded or decoded as JSON. */
export const INVALID_JSON = "invalid_json";
/** An uncaught JS exception (D-P9-20). */
export const JS_EXCEPTION = "js_exception";
/** `run` was called with a workflow name the module does not export. */
export const UNKNOWN_WORKFLOW = "unknown_workflow";
/** A child run was terminated (PF-W7). */
export const CHILD_TERMINATED = "child_terminated";

/** Base class of every SDK error that maps to a WIT failure. */
export class OstroError extends Error {
  kind: string;
  retryable: boolean;
  /** JSON-able details, sent as an `application/json` payload. */
  details: unknown;

  constructor(kind: string, message: string, retryable: boolean, details?: unknown) {
    super(message);
    this.name = "OstroError";
    this.kind = kind;
    this.retryable = retryable;
    this.details = details;
  }

  /** The WIT failure this error records as. */
  toFailure(): Failure {
    return failureOf(this.kind, this.message, this.retryable, this.details);
  }
}

/** A failure the host must not retry. */
export class NonRetryableError extends OstroError {
  constructor(kind: string, message: string, details?: unknown) {
    super(kind, message, false, details);
    this.name = "NonRetryableError";
  }
}

/** A failure the host may retry under the step's retry policy. */
export class RetryableError extends OstroError {
  constructor(kind: string, message: string, details?: unknown) {
    super(kind, message, true, details);
    this.name = "RetryableError";
  }
}

/** The host timed the operation out (`failure::timeout`). */
export class TimeoutError extends NonRetryableError {
  constructor(message = "timed out") {
    super(TIMEOUT, message);
    this.name = "TimeoutError";
  }
}

/** The run was cancelled (`failure::cancelled`, D11), or a cancelled op was awaited. */
export class CancelledError extends NonRetryableError {
  constructor(message = "cancelled") {
    super(CANCELLED, message);
    this.name = "CancelledError";
  }
}

/** An SDK protocol violation (non-retryable `sdk_protocol`). */
export class ProtocolError extends NonRetryableError {
  constructor(message: string) {
    super(SDK_PROTOCOL, message);
    this.name = "ProtocolError";
  }
}

/** The workflow awaits something no Ostro op can complete. */
export class DeadlockError extends ProtocolError {
  constructor(message = "deadlock: the workflow awaits a promise no Ostro operation can settle") {
    super(message);
    this.name = "DeadlockError";
  }
}

/** A string with a lone surrogate (non-retryable `invalid_string`, PF-W4). */
export class InvalidStringError extends NonRetryableError {
  constructor(message: string) {
    super(INVALID_STRING, message);
    this.name = "InvalidStringError";
  }
}

/** A feature the SDK deliberately does not support (for example `setInterval`). */
export class NotSupportedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NotSupportedError";
  }
}

// Details are JSON-encoded by the caller-provided encoder to avoid an import cycle with
// json.ts (json.ts depends on errors.ts).
let encodeDetails: (v: unknown) => Payload | undefined = () => undefined;
let decodeDetails: (p: Payload) => unknown = () => undefined;

/** Wire the JSON codec (called once by json.ts). */
export function setDetailsCodec(enc: (v: unknown) => Payload | undefined, dec: (p: Payload) => unknown): void {
  encodeDetails = enc;
  decodeDetails = dec;
}

function failureOf(kind: string, message: string, retryable: boolean, details: unknown): Failure {
  if (!retryable && kind === TIMEOUT) return { tag: "timeout" };
  if (!retryable && kind === CANCELLED) return { tag: "cancelled" };
  const info: ErrorInfo = { kind, message, details: details === undefined ? undefined : encodeDetails(details) };
  return retryable ? { tag: "retryable", val: info } : { tag: "non-retryable", val: info };
}

function describe(e: unknown): string {
  if (e instanceof Error) return e.name && e.name !== "Error" ? `${e.name}: ${e.message}` : `Error: ${e.message}`;
  try {
    return `thrown value: ${String(e)}`;
  } catch {
    return "thrown value";
  }
}

/**
 * The WIT failure for anything thrown.
 *
 * `OstroError`s map to their own failure; anything else becomes `js_exception`, retryable
 * when `uncaughtRetryable` (a step body) and non-retryable otherwise (a workflow body,
 * like the Python SDK's `python_exception`).
 */
export function toFailure(e: unknown, uncaughtRetryable: boolean): Failure {
  if (e instanceof OstroError) return e.toFailure();
  return failureOf(JS_EXCEPTION, describe(e), uncaughtRetryable, undefined);
}

/** The SDK error for a WIT failure (round-trips `toFailure`). */
export function fromFailure(f: Failure): OstroError {
  switch (f.tag) {
    case "timeout":
      return new TimeoutError();
    case "cancelled":
      return new CancelledError();
    case "retryable": {
      return new RetryableError(f.val.kind, f.val.message, details(f.val));
    }
    case "non-retryable": {
      const { kind, message } = f.val;
      if (kind === SDK_PROTOCOL) return new ProtocolError(message);
      if (kind === INVALID_STRING) return new InvalidStringError(message);
      // A non-retryable error-info with a reserved kind (SDKs never send it).
      if (kind === TIMEOUT) return new TimeoutError(message);
      if (kind === CANCELLED) return new CancelledError(message);
      return new NonRetryableError(kind, message, details(f.val));
    }
  }
}

function details(info: ErrorInfo): unknown {
  if (info.details === undefined) return undefined;
  try {
    return decodeDetails(info.details);
  } catch {
    return undefined;
  }
}
