// Durable steps: `step(name, fn, opts?)`, retry policies, the step lock and step
// exclusivity (PF-W1, D-P9-16).
//
// - Steps are serialised by a FIFO lock, so step order = call order (Rust parity) and no
//   `begin-step` is issued while another step is open.
// - Exclusivity: `step()` sets the already queued jobs aside and issues `begin-step` from
//   a job at the front of the queue (so the caller's synchronous continuation never runs
//   with a step open; jobs it queues meanwhile are set aside too). While the body runs
//   only jobs enqueued after the step opened are drained. When the step closes, the
//   set-aside jobs go back to the front of the queue in their original order. A step that
//   waited for the lock opens from its own job and sets aside the jobs queued behind it.
// - One resolution mechanism for both paths: the caller awaits one promise `done` that is
//   never resolved synchronously. Replayed: a job resolving `done` is enqueued right
//   after `begin-step` returns. Live: once the body settled, its jobs are drained and the
//   set-aside jobs are back at the front, the same job is enqueued. In both paths the
//   queue then holds exactly the sibling jobs that were queued when the step began, so a
//   multi-hop sibling interleaves identically on a live run and on its replay.
// - Quiescence with a step open (the body awaits something only the drive loop could
//   settle, e.g. a durable op created outside the step) completes the step
//   `non-retryable{kind:"sdk_protocol"}`. Journaled calls inside a body throw
//   `ProtocolError` at once (see `Activation.checkJournaled`).
// - The value returned is always decoded from the journaled JSON, so live and replay
//   return identical values. `retry-now` re-runs the body on the same handle.

import { SDK_PROTOCOL, fromFailure, toFailure } from "./errors.ts";
import type {
  Outcome,
  Payload,
  StepHandle,
  StepOptions as WitStepOptions,
  RetryPolicy as WitRetryPolicy,
} from "./host.ts";
import { fromPayload, toPayload } from "./json.ts";
import { activation, currentActivation } from "./loop.ts";
import type { Activation } from "./loop.ts";
import { SdkPromise, enqueueJob, restoreJobsAtFront, setIdleHook, takeQueuedJobs } from "./promise.ts";
import type { Job } from "./promise.ts";
import { checkString } from "./utf8.ts";

/** Fields of a retry policy; any omitted field takes the SDK default. */
export interface RetryPolicyInit {
  /** Attempts including the first (default 3). */
  maxAttempts?: number;
  /** Delay before the first retry in ms (default 1000). */
  initialIntervalMs?: number;
  /** Upper bound of the delay in ms (default 60000). */
  maxIntervalMs?: number;
  /** Delay multiplier per attempt (default 2.0). */
  backoffCoefficient?: number;
  /** Error kinds that are never retried (default none). */
  nonRetryableKinds?: string[];
}

/**
 * Exponential-backoff retry policy. Defaults apply only to the fields left out of a
 * partially specified policy (3 attempts, 1 s, 60 s, 2.0: the Rust macro's defaults,
 * PF-W6); a step without `retry` gets no host retries.
 */
export class RetryPolicy {
  maxAttempts: number;
  initialIntervalMs: number;
  maxIntervalMs: number;
  backoffCoefficient: number;
  nonRetryableKinds: string[];

  constructor(init: RetryPolicyInit = {}) {
    this.maxAttempts = init.maxAttempts ?? 3;
    this.initialIntervalMs = init.initialIntervalMs ?? 1000;
    this.maxIntervalMs = init.maxIntervalMs ?? 60000;
    this.backoffCoefficient = init.backoffCoefficient ?? 2.0;
    this.nonRetryableKinds = (init.nonRetryableKinds ?? []).slice();
  }

  /** The WIT `retry-policy` record. */
  toWit(): WitRetryPolicy {
    return {
      maxAttempts: u32(this.maxAttempts, "retry.maxAttempts"),
      initialIntervalMs: u64(this.initialIntervalMs, "retry.initialIntervalMs"),
      maxIntervalMs: u64(this.maxIntervalMs, "retry.maxIntervalMs"),
      backoffCoefficient: Number(this.backoffCoefficient),
      nonRetryableKinds: this.nonRetryableKinds.map((k, i) => checkString(k, `retry.nonRetryableKinds[${i}]`)),
    };
  }
}

/** Options of a durable step. Omitted fields use the host defaults. */
export interface StepOpts {
  /** Retry policy: a `RetryPolicy` or a partial init (defaults fill the rest). */
  retry?: RetryPolicy | RetryPolicyInit | null;
  /** Step timeout in ms. */
  timeoutMs?: number | bigint;
  /** Fuel budget of the body (guest-runtime instructions; not portable across languages). */
  fuel?: number | bigint;
}

const U64_MAX = BigInt("18446744073709551615");

/** A non-negative integer as a WIT u64 (bigint). */
export function u64(v: number | bigint, what: string): bigint {
  if (typeof v === "bigint") {
    if (v < BigInt(0) || v > U64_MAX) throw new RangeError(`${what} must fit in a u64`);
    return v;
  }
  if (typeof v !== "number" || !Number.isFinite(v) || v < 0 || Math.floor(v) !== v) {
    throw new RangeError(`${what} must be a non-negative integer`);
  }
  return BigInt(v);
}

/** A non-negative integer as a WIT u32. */
export function u32(v: number, what: string): number {
  if (typeof v !== "number" || !Number.isInteger(v) || v < 0 || v > 0xffffffff) {
    throw new RangeError(`${what} must be a u32`);
  }
  return v;
}

/** The WIT `step-options` record for `opts`. */
export function toWitStepOptions(opts: StepOpts | undefined): WitStepOptions {
  const o = opts ?? {};
  let retry: WitRetryPolicy | undefined;
  if (o.retry !== undefined && o.retry !== null) {
    retry = (o.retry instanceof RetryPolicy ? o.retry : new RetryPolicy(o.retry)).toWit();
  }
  return {
    retry,
    timeoutMs: o.timeoutMs === undefined ? undefined : u64(o.timeoutMs, "timeoutMs"),
    fuel: o.fuel === undefined ? undefined : u64(o.fuel, "fuel"),
  };
}

type BodyOutcome = { ok: true; value: unknown } | { ok: false; error: unknown };

interface OpenStep {
  act: Activation;
  name: string;
  handle: StepHandle;
  setAside: Job[];
  body: () => unknown;
  attempt: number;
  outcome: BodyOutcome | undefined;
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
}

/** True while a step body is running (timers raise `sdk_protocol` then, D-P9-16). */
export function stepOpen(): boolean {
  const act = currentActivation();
  return act !== undefined && act.openStep !== undefined;
}

/** Message of the failure recorded when a step body awaits a durable operation. */
export const STEP_AWAITS_DURABLE = "step bodies cannot await durable operations";

function settleDone(outcome: Outcome<Payload>, resolve: (v: unknown) => void, reject: (e: unknown) => void): void {
  if (outcome.tag === "err") {
    reject(fromFailure(outcome.val));
    return;
  }
  let v: unknown;
  try {
    v = fromPayload(outcome.val);
  } catch (e) {
    reject(e);
    return;
  }
  resolve(v);
}

function encodeOutcome(o: BodyOutcome): Outcome<Payload> {
  if (!o.ok) return { tag: "err", val: toFailure(o.error, true) };
  try {
    return { tag: "ok", val: toPayload(o.value) };
  } catch (e) {
    return { tag: "err", val: toFailure(e, false) };
  }
}

function release(act: Activation): void {
  const next = act.stepWaiters.shift();
  if (next !== undefined) enqueueJob(next);
  else act.stepHeld = false;
}

function runBody(open: OpenStep): void {
  open.attempt += 1;
  const attempt = open.attempt;
  open.outcome = undefined;
  let r: unknown;
  try {
    r = open.body();
  } catch (e) {
    open.outcome = { ok: false, error: e };
    return;
  }
  SdkPromise.resolve(r).then(
    (value) => {
      if (open.attempt === attempt && open.act.openStep === open) open.outcome = { ok: true, value };
    },
    (error) => {
      if (open.attempt === attempt && open.act.openStep === open) open.outcome = { ok: false, error };
    },
  );
}

// Runs as a job at the front of the queue. `earlier` holds the jobs that were queued when
// `step()` was called (empty for a step that waited for the lock); together with the
// jobs queued since (by the caller's synchronous continuation) they are set aside while
// the body runs, and restored in order before `done` is resolved.
function begin(
  act: Activation,
  name: string,
  opts: WitStepOptions,
  body: () => unknown,
  resolve: (v: unknown) => void,
  reject: (e: unknown) => void,
  earlier: Job[],
): void {
  const aside = earlier.concat(takeQueuedJobs());
  let start;
  try {
    start = act.host.beginStep(name, opts);
  } catch (e) {
    restoreJobsAtFront(aside);
    release(act);
    throw e;
  }
  if (start.tag === "replayed") {
    const recorded = start.val;
    restoreJobsAtFront(aside);
    enqueueJob(() => settleDone(recorded, resolve, reject));
    release(act);
    return;
  }
  const open: OpenStep = {
    act,
    name,
    handle: start.val,
    setAside: aside,
    body,
    attempt: 0,
    outcome: undefined,
    resolve,
    reject,
  };
  act.openStep = open;
  runBody(open);
}

// Called by `drain` whenever the queue is empty: completes the open step, if any.
function onIdle(): boolean {
  const act = currentActivation();
  if (act === undefined || act.openStep === undefined) return false;
  const open = act.openStep as OpenStep;
  const outcome: Outcome<Payload> =
    open.outcome === undefined
      ? {
          tag: "err",
          val: { tag: "non-retryable", val: { kind: SDK_PROTOCOL, message: STEP_AWAITS_DURABLE, details: undefined } },
        }
      : encodeOutcome(open.outcome);
  const ack = act.host.stepComplete(open.handle, outcome);
  if (ack === "retry-now") {
    runBody(open);
    return true;
  }
  act.openStep = undefined;
  restoreJobsAtFront(open.setAside);
  enqueueJob(() => settleDone(outcome, open.resolve, open.reject));
  release(act);
  return true;
}

setIdleHook(onIdle);

/**
 * Run (or replay) the durable step `name` and return a promise of its value.
 *
 * - Replay: the recorded outcome is decoded; `fn` is not called.
 * - Execute: `fn` runs (it may return a promise); its value is journaled as JSON. A
 *   thrown `NonRetryableError` records a non-retryable failure, a `RetryableError` a
 *   retryable one with its kind, any other exception a retryable `js_exception`.
 *
 * Inside the body only context reads (`ctx.now()`, `ctx.randomU64()`, ...) and logging
 * are allowed; journaled calls (effects, `step`, `patched`, `setStatus`, timers) throw
 * `ProtocolError`.
 */
export function step<T>(name: string, fn: () => T | PromiseLike<T>, opts?: StepOpts): SdkPromise<T> {
  const act = activation();
  act.checkJournaled(`step(${JSON.stringify(name)})`);
  checkString(name, "step name");
  if (typeof fn !== "function") throw new TypeError("step body must be a function");
  const wopts = toWitStepOptions(opts);
  const { promise, resolve, reject } = SdkPromise.withResolvers<T>();
  const res = resolve as (v: unknown) => void;
  if (!act.stepHeld) {
    // Set the queued sibling jobs aside now and begin in a job at the front, so the
    // caller's synchronous continuation never runs while the step is open.
    act.stepHeld = true;
    const earlier = takeQueuedJobs();
    enqueueJob(() => begin(act, name, wopts, fn, res, reject, earlier));
  } else {
    act.stepWaiters.push(() => begin(act, name, wopts, fn, res, reject, []));
  }
  return promise;
}
