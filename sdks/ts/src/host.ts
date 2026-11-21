// Host contract of the Ostro TypeScript SDK.
//
// `Host` mirrors the frozen WIT world `ostro:runtime@0.1.0` (`wit/runtime.wit`) 1:1 with
// TypeScript types. The SDK core talks to the host only through this interface, so
// host-side tests (`node --test`) install a fake host and never load guest bindings.
//
// Type mapping:
// - op ids and every other WIT u64/s64 are `bigint`, never `number` (PF-W3): the
//   cancellation op is 2^64-1, which `Number` cannot represent;
// - a payload is `{ contentType, data: Uint8Array }`;
// - WIT records are interfaces with camelCase fields; WIT variants are `{ tag, val? }`
//   with the WIT case name (`"non-retryable"`, `"retry-now"`), as jco delivers them;
// - `result<T, failure>` is `{ tag: "ok", val: T } | { tag: "err", val: Failure }`;
// - WIT `datetime` is `{ seconds: bigint; nanoseconds: number }`.
//
// No `ostro:runtime/*` import specifiers here: those live in `src/bindings.ts` (09-09),
// which only the guest entry imports. Erasable TypeScript only (no enums, namespaces or
// parameter properties), so Node 24 type stripping runs these sources directly.

/** Op id of `effects.cancellation` (2^64-1); `u64::MAX-15..=u64::MAX` is host-reserved. */
export const CANCELLATION_OP: bigint = 2n ** 64n - 1n;

// --- types --------------------------------------------------------------------------

/** WIT `types.payload`. */
export interface Payload {
  contentType: string;
  data: Uint8Array;
}

/** WIT `types.datetime`. */
export interface Datetime {
  seconds: bigint;
  nanoseconds: number;
}

/** WIT `types.error-info`. */
export interface ErrorInfo {
  kind: string;
  message: string;
  details?: Payload;
}

/** WIT `types.failure`. */
export type Failure =
  | { tag: "retryable"; val: ErrorInfo }
  | { tag: "non-retryable"; val: ErrorInfo }
  | { tag: "timeout" }
  | { tag: "cancelled" };

/** WIT `result<T, failure>`. */
export type Outcome<T> = { tag: "ok"; val: T } | { tag: "err"; val: Failure };

/** WIT `types.retry-policy`. */
export interface RetryPolicy {
  maxAttempts: number;
  initialIntervalMs: bigint;
  maxIntervalMs: bigint;
  backoffCoefficient: number;
  nonRetryableKinds: string[];
}

/** WIT `types.step-options`. */
export interface StepOptions {
  retry?: RetryPolicy;
  timeoutMs?: bigint;
  fuel?: bigint;
}

/** WIT `types.run-info`. */
export interface RunInfo {
  runId: string;
  workflow: string;
  workflowVersion: number;
  attempt: number;
  moduleHash: string;
  tenant?: string;
  parentRunId?: string;
  deadline?: Datetime;
  continuedFrom?: string;
}

// --- journal ------------------------------------------------------------------------

/** Opaque handle of an executing step (WIT `journal.step`); only its host interprets it. */
export interface StepHandle {
  readonly __ostroStep?: never;
}

/** WIT `journal.step-start`. */
export type StepStart =
  | { tag: "replayed"; val: Outcome<Payload> }
  | { tag: "execute"; val: StepHandle };

/** WIT `journal.completion-ack`. */
export type CompletionAck = "recorded" | "retry-now";

// --- effects ------------------------------------------------------------------------

/** WIT `effects.header`. */
export interface Header {
  name: string;
  value: string;
}

/** WIT `effects.http-request`. */
export interface HttpRequest {
  method: string;
  url: string;
  headers: Header[];
  body?: Uint8Array;
  timeoutMs?: bigint;
  idempotencyKey?: string;
  retry?: RetryPolicy;
  redact: string[];
}

/** WIT `effects.http-response`. */
export interface HttpResponse {
  status: number;
  headers: Header[];
  body: Uint8Array;
}

/** WIT `effects.sql-value`. */
export type SqlValue =
  | { tag: "null" }
  | { tag: "boolean"; val: boolean }
  | { tag: "int8"; val: bigint }
  | { tag: "float8"; val: number }
  | { tag: "numeric"; val: string }
  | { tag: "text"; val: string }
  | { tag: "bytea"; val: Uint8Array }
  | { tag: "json"; val: string }
  | { tag: "timestamptz"; val: Datetime };

/** WIT `effects.sql-request`. */
export interface SqlRequest {
  connection: string;
  statement: string;
  params: SqlValue[];
  exactlyOnce: boolean;
}

/** WIT `effects.sql-result`. */
export interface SqlResult {
  columns: string[];
  rows: SqlValue[][];
  rowsAffected: bigint;
}

/** WIT `effects.object-request`. */
export interface ObjectRequest {
  connection: string;
  op: string;
  key: string;
  body?: Uint8Array;
}

/** WIT `effects.child-options`. */
export interface ChildOptions {
  queue?: string;
  version?: number;
  detached: boolean;
  runTimeoutMs?: bigint;
}

/** WIT `effects.await-token`. */
export interface AwaitToken {
  op: bigint;
  token: string;
}

/** WIT `effects.op-output`. */
export type OpOutput =
  | { tag: "http"; val: HttpResponse }
  | { tag: "sql"; val: SqlResult }
  | { tag: "object"; val: Uint8Array }
  | { tag: "timer" }
  | { tag: "signal"; val: Payload }
  | { tag: "child"; val: Payload }
  | { tag: "token"; val: Payload }
  | { tag: "cancel-requested" };

/** WIT `effects.completion`. */
export interface Completion {
  op: bigint;
  outcome: Outcome<OpOutput>;
}

/** WIT `effects.wait-mode`. */
export type WaitMode = "all" | "any";

/** WIT `log.level`. */
export type LogLevel = "trace" | "debug" | "info" | "warn" | "error";

// --- the contract -------------------------------------------------------------------

/** The WIT world's imports, 1:1. */
export interface Host {
  // context
  info(): RunInfo;
  now(): Datetime;
  randomU64(): bigint;
  randomBytes(len: number): Uint8Array;
  uuidV7(): string;
  isReplaying(): boolean;
  patched(changeId: string): boolean;
  setStatus(json: string): void;
  continueAsNewSuggested(): boolean;

  // journal
  beginStep(name: string, opts: StepOptions): StepStart;
  stepSeq(step: StepHandle): bigint;
  stepComplete(step: StepHandle, outcome: Outcome<Payload>): CompletionAck;

  // effects
  httpFetch(req: HttpRequest): bigint;
  sqlQuery(req: SqlRequest): bigint;
  objectStore(req: ObjectRequest): bigint;
  sleep(durationMs: bigint): bigint;
  sleepUntil(at: Datetime): bigint;
  waitSignal(name: string, timeoutMs: bigint | undefined): bigint;
  startChild(workflow: string, input: Payload, opts: ChildOptions): bigint;
  createAwaitToken(timeoutMs: bigint | undefined): AwaitToken;
  cancellation(): bigint;
  cancelOp(op: bigint): void;
  wait(ops: bigint[], mode: WaitMode): Completion[];

  // log
  logEmit(level: LogLevel, message: string, fields: [string, string][]): void;
}

let current: Host | undefined;

/** Install `h` as the SDK's host (`undefined` uninstalls); returns the previous host. */
export function setHost(h: Host | undefined): Host | undefined {
  const previous = current;
  current = h;
  return previous;
}

/** The installed host. Throws if none is installed (the guest entry installs the bindings). */
export function host(): Host {
  if (current === undefined) {
    throw new Error("ostro: no host installed (the guest entry installs the bindings host, 09-09)");
  }
  return current;
}
