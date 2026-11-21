// The synchronous drive loop over `effects.wait` (09-RESEARCH Pattern 2 / "TS export").
//
//   loop { drain(); if root settled: break; ops = interested ops ascending;
//          none → Deadlock; completions = wait(ops [+ CANCELLATION], any);
//          resolve completions in returned order }
//
// One `Activation` per `run` call holds the op table, the interested set, the step lock
// state and the closed flag. After the root settles the activation is closed: every
// journaled entry point throws `ProtocolError` without calling the host (Pitfall 1), and
// leftover jobs never run inside `run`.

import { CancelledError, DeadlockError, ProtocolError, fromFailure } from "./errors.ts";
import type { Completion, Host, OpOutput } from "./host.ts";
import { CANCELLATION_OP } from "./host.ts";
import { SdkPromise, drain, resetJobs } from "./promise.ts";

/** Decoder from a successful op output to the op's value. Throws on a mismatched output. */
export type Decode<T> = (out: OpOutput) => T;

/** Bookkeeping for one op id. */
export interface OpEntry {
  id: bigint;
  promise: SdkPromise<unknown>;
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
  decode: Decode<unknown>;
  shielded: boolean;
}

function compareBigint(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** State of one `run` activation. */
export class Activation {
  host: Host;
  /** Op entries, created when an op is first awaited (`then`). */
  entries: Map<bigint, OpEntry> = new Map();
  /** Ops whose completion the workflow is waiting for. */
  interested: Set<bigint> = new Set();
  /** The run's cancellation was observed; later unshielded waits reject at once (D11). */
  cancelled = false;
  /** The root settled: journaled calls throw `ProtocolError`. */
  closed = false;
  /** Step lock (D-P9-16): held from begin-step until the step's result is delivered. */
  stepHeld = false;
  stepWaiters: (() => void)[] = [];
  /** The open step (set by step.ts while a body runs). */
  openStep: unknown = undefined;
  /** Number of `wait` calls issued (tests and diagnostics). */
  waits = 0;

  constructor(h: Host) {
    this.host = h;
  }

  /** Throw unless a journaled host call is allowed now (`what` names the call). */
  checkJournaled(what: string): void {
    if (this.closed) throw new ProtocolError(`${what} after the workflow settled (journaled calls after the terminal are not allowed)`);
    if (this.openStep !== undefined) {
      throw new ProtocolError(`${what} inside an open step (step bodies cannot call journaled operations)`);
    }
  }

  /** The entry of op `id`, created on first use. */
  entry(id: bigint, decode: Decode<unknown>, shielded: boolean): OpEntry {
    let e = this.entries.get(id);
    if (e === undefined) {
      const { promise, resolve, reject } = SdkPromise.withResolvers<unknown>();
      e = { id, promise, resolve, reject, decode, shielded };
      this.entries.set(id, e);
    }
    return e;
  }

  /** Register interest in op `id` and return its promise (lazy wait). */
  awaitOp(id: bigint, decode: Decode<unknown>, shielded: boolean): SdkPromise<unknown> {
    const e = this.entry(id, decode, shielded);
    if (!e.promise.settled) {
      if (this.cancelled && !e.shielded && id !== CANCELLATION_OP) {
        // Cancellation is remembered: an unshielded wait rejects without a host call.
        e.reject(new CancelledError());
      } else {
        this.interested.add(id);
      }
    }
    return e.promise;
  }

  /** The op list of the next `wait`: pending interested ops ascending, + CANCELLATION. */
  waitList(): bigint[] {
    const ops: bigint[] = [];
    let unshielded = false;
    for (const id of this.interested) {
      const e = this.entries.get(id);
      if (e === undefined || e.promise.settled) continue;
      ops.push(id);
      if (!e.shielded && id !== CANCELLATION_OP) unshielded = true;
    }
    ops.sort(compareBigint);
    if (ops.length > 0 && unshielded && !this.cancelled && ops[ops.length - 1] !== CANCELLATION_OP) {
      ops.push(CANCELLATION_OP);
    }
    return ops;
  }

  /** Apply one completion returned by `wait`. */
  complete(c: Completion): void {
    if (c.op === CANCELLATION_OP) {
      this.cancelled = true;
      for (const e of this.entries.values()) {
        if (e.promise.settled) continue;
        if (e.id === CANCELLATION_OP) {
          this.settleEntry(e, c);
        } else if (!e.shielded) {
          this.interested.delete(e.id);
          e.reject(new CancelledError());
        }
      }
      this.interested.delete(CANCELLATION_OP);
      return;
    }
    const e = this.entries.get(c.op);
    this.interested.delete(c.op);
    if (e === undefined || e.promise.settled) return;
    this.settleEntry(e, c);
  }

  settleEntry(e: OpEntry, c: Completion): void {
    this.interested.delete(e.id);
    if (c.outcome.tag === "err") {
      e.reject(fromFailure(c.outcome.val));
      return;
    }
    let v: unknown;
    try {
      v = e.decode(c.outcome.val);
    } catch (err) {
      e.reject(err);
      return;
    }
    e.resolve(v);
  }
}

let current: Activation | undefined;

/** Start a fresh activation on `h`: a new op table and an empty job queue. */
export function beginActivation(h: Host): Activation {
  resetJobs();
  current = new Activation(h);
  return current;
}

/** The current (or most recent) activation, if any. */
export function currentActivation(): Activation | undefined {
  return current;
}

/** The current activation; throws `ProtocolError` outside a workflow run. */
export function activation(): Activation {
  if (current === undefined) {
    throw new ProtocolError("no workflow activation (Ostro APIs are only usable inside a workflow run)");
  }
  return current;
}

/** Forget the current activation (host tests). */
export function clearActivation(): void {
  current = undefined;
  resetJobs();
}

/**
 * Drive `root` to settlement inside one synchronous call: drain the job queue to
 * quiescence, then wait on the interested ops, until the root settles. Closes the
 * activation on return (also when it throws).
 *
 * Throws `DeadlockError` when the root is pending, nothing is runnable and no op is
 * interested (it awaits something no Ostro op can settle); `wait(any, [])` is never called.
 */
export function driveToCompletion(act: Activation, root: SdkPromise<unknown>): void {
  try {
    const stop = (): boolean => root.settled && act.openStep === undefined;
    for (;;) {
      drain(stop);
      if (root.settled) return;
      const ops = act.waitList();
      if (ops.length === 0) throw new DeadlockError();
      act.waits += 1;
      const completions = act.host.wait(ops, "any");
      if (completions.length === 0) throw new ProtocolError("wait(any) returned no completion");
      for (const c of completions) act.complete(c);
    }
  } finally {
    act.closed = true;
  }
}
