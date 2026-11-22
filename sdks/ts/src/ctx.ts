// The workflow context passed to every workflow function.
//
// Context reads (`info`, `now`, `randomU64`, `randomBytes`, `uuid7`, `isReplaying`,
// `continueAsNewSuggested`) and `log` are allowed anywhere, including step bodies (where
// randomness comes from the host's step-local stream). Journaled calls (effects,
// `patched`, `setStatus`, `cancel`, `cancellation`) throw `ProtocolError` inside an open
// step and after the workflow settled, before any host call.
//
// D11 shielding uses an explicit shielded ctx (`ctx.shielded(async (s) => ...)`): neither
// QuickJS nor StarlingMonkey ships AsyncContext, so a depth counter would be unsound
// across `await` (SDK-2). Ops created through `s` are not rejected by a cancellation.

import type { Datetime, LogLevel, Payload, RunInfo } from "./host.ts";
import { toPayload, stringifyJson } from "./json.ts";
import type { Activation } from "./loop.ts";
import {
  ChildRun,
  Op,
  decodeChild,
  decodeChildRun,
  decodeSignal,
  decodeToken,
  decodeUnit,
} from "./ops.ts";
import { HttpClient, ObjectClient, SqlBuilder, idempotencyKeyFor } from "./effects.ts";
import { Saga } from "./saga.ts";
import { step, u32, u64 } from "./step.ts";
import { checkString } from "./utf8.ts";

const TWO_POW_MINUS_53 = Math.pow(2, -53);
const ELEVEN = BigInt(11);

/** `random_f64` from a u64 draw: `(u >> 11) * 2^-53` (conformance-pinned, D-P9-15). */
export function f64FromU64(u: bigint): number {
  return Number(u >> ELEVEN) * TWO_POW_MINUS_53;
}

/** Milliseconds since the epoch of a WIT datetime. */
export function datetimeToMs(d: Datetime): number {
  return Number(d.seconds) * 1000 + Math.floor(d.nanoseconds / 1e6);
}

/** The WIT datetime of `ms` milliseconds since the epoch (ms ≥ 0). */
export function msToDatetime(ms: number): Datetime {
  if (!Number.isFinite(ms) || ms < 0) throw new RangeError("time must be a non-negative finite number of ms");
  const whole = Math.floor(ms);
  const seconds = Math.floor(whole / 1000);
  return { seconds: BigInt(seconds), nanoseconds: (whole - seconds * 1000) * 1e6 };
}

/** Thrown by `ctx.continueAsNew`; `run` maps it to the `continue-as-new` run result. */
export class ContinueAsNew extends Error {
  readonly input: Payload;
  readonly workflow: string | undefined;
  readonly version: number | undefined;

  constructor(input: Payload, workflow: string | undefined, version: number | undefined) {
    super("continue-as-new");
    this.name = "ContinueAsNew";
    this.input = input;
    this.workflow = workflow;
    this.version = version;
  }
}

/** Options of `ctx.continueAsNew`. */
export interface ContinueAsNewOpts {
  /** Continue as another workflow (default: the same one). */
  workflow?: string;
  /** Pin the successor's version (default: the active version). */
  version?: number;
}

/** Options of `ctx.startChild`. */
export interface ChildOpts {
  queue?: string;
  version?: number;
  /** Detached children resolve to a `ChildRun` as soon as the start is accepted. */
  detached?: boolean;
  runTimeoutMs?: number | bigint;
}

/** Options with an optional timeout. */
export interface TimeoutOpts {
  timeoutMs?: number | bigint;
}

/** An await token: hand `token` to an external system; `op` completes with its payload. */
export interface AwaitTokenHandle<T> {
  token: string;
  op: Op<T>;
}

/** A workflow name or anything with a `name` (the value `workflow()` returns). */
export type WorkflowTarget = string | { name: string };

const LEVELS: LogLevel[] = ["trace", "debug", "info", "warn", "error"];

/** The workflow context. */
export class Ctx {
  _act: Activation;
  /** True for the ctx passed to a `shielded` callback. */
  readonly isShielded: boolean;

  constructor(act: Activation, shielded: boolean) {
    this._act = act;
    this.isShielded = shielded;
  }

  // --- context reads ------------------------------------------------------------------

  /** Facts about this run. */
  info(): RunInfo {
    return this._act.host.info();
  }

  /** Logical workflow time (deterministic on replay). */
  now(): Date {
    return new Date(this.nowMs());
  }

  /** Logical workflow time in ms since the epoch. */
  nowMs(): number {
    return datetimeToMs(this._act.host.now());
  }

  /** A host-drawn u64 (D7: the step-local stream inside a step body). */
  randomU64(): bigint {
    return this._act.host.randomU64();
  }

  /** A float in [0, 1) from one `randomU64` draw. */
  randomF64(): number {
    return f64FromU64(this.randomU64());
  }

  /** `n` host-drawn bytes (n ≤ 65536). */
  randomBytes(n: number): Uint8Array {
    if (!Number.isInteger(n) || n < 0 || n > 65536) throw new RangeError("randomBytes: n must be an integer in 0..=65536");
    return this._act.host.randomBytes(n);
  }

  /** A UUIDv7 from the host's uuid stream (lowercase canonical form). */
  uuid7(): string {
    return this._act.host.uuidV7();
  }

  /** Alias of `randomU64` (cross-SDK naming). */
  random_u64(): bigint {
    return this.randomU64();
  }

  /** Alias of `randomF64` (cross-SDK naming). */
  random_f64(): number {
    return this.randomF64();
  }

  /** Alias of `randomBytes` (cross-SDK naming). */
  random_bytes(n: number): Uint8Array {
    return this.randomBytes(n);
  }

  /** True while the activation replays journaled history. */
  isReplaying(): boolean {
    return this._act.host.isReplaying();
  }

  /** True when the journal is large enough that the workflow should continue-as-new. */
  continueAsNewSuggested(): boolean {
    return this._act.host.continueAsNewSuggested();
  }

  /** Emit a log line (not journaled). */
  log(level: LogLevel, message: string, fields?: Record<string, string> | [string, string][]): void {
    if (!LEVELS.includes(level)) throw new RangeError(`unknown log level ${String(level)}`);
    const list: [string, string][] = [];
    const entries: [string, string][] = Array.isArray(fields) ? fields : Object.entries(fields ?? {});
    for (const [k, v] of entries) list.push([checkString(String(k), "log field name"), checkString(String(v), "log field value")]);
    this._act.host.logEmit(level, checkString(String(message), "log message"), list);
  }

  // --- journaled ----------------------------------------------------------------------

  /** Versioning gate: true if `changeId` is active for this run. */
  patched(changeId: string): boolean {
    this._act.checkJournaled("patched");
    return this._act.host.patched(checkString(changeId, "change id"));
  }

  /** Publish a JSON status document (journaled). */
  setStatus(status: unknown): void {
    this._act.checkJournaled("setStatus");
    this._act.host.setStatus(stringifyJson(status));
  }

  /** End this run with continue-as-new: a fresh run starts with `input`. Never returns. */
  continueAsNew(input: unknown, opts?: ContinueAsNewOpts): never {
    const o = opts ?? {};
    const workflow = o.workflow === undefined ? undefined : checkString(o.workflow, "continueAsNew workflow");
    const version = o.version === undefined ? undefined : u32(o.version, "continueAsNew version");
    throw new ContinueAsNew(toPayload(input), workflow, version);
  }

  /** A durable timer of `ms` milliseconds. */
  sleep(ms: number | bigint): Op<void> {
    this._act.checkJournaled("sleep");
    const d = u64(ms, "sleep duration");
    return this._op(this._act.host.sleep(d), decodeUnit) as Op<void>;
  }

  /** A durable timer until `at` (a Date or ms since the epoch). */
  sleepUntil(at: Date | number): Op<void> {
    this._act.checkJournaled("sleepUntil");
    const ms = typeof at === "number" ? at : at.getTime();
    return this._op(this._act.host.sleepUntil(msToDatetime(ms)), decodeUnit) as Op<void>;
  }

  /** Wait for the next signal `name`; its JSON payload is the value. Timeout → TimeoutError. */
  waitSignal<T = unknown>(name: string, opts?: TimeoutOpts): Op<T> {
    this._act.checkJournaled("waitSignal");
    const timeout = opts?.timeoutMs === undefined ? undefined : u64(opts.timeoutMs, "timeoutMs");
    return this._op(this._act.host.waitSignal(checkString(name, "signal name"), timeout), decodeSignal) as Op<T>;
  }

  /**
   * Start a child workflow (PF-W7). Attached: the op resolves to the child's output.
   * Detached: it resolves to a `ChildRun` once the start is accepted.
   */
  startChild<T = unknown>(workflow: WorkflowTarget, input: unknown, opts?: ChildOpts): Op<T> {
    this._act.checkJournaled("startChild");
    const name = checkString(typeof workflow === "string" ? workflow : workflow.name, "child workflow name");
    const o = opts ?? {};
    const payload = toPayload(input);
    const detached = o.detached === true;
    const id = this._act.host.startChild(name, payload, {
      queue: o.queue === undefined ? undefined : checkString(o.queue, "child queue"),
      version: o.version === undefined ? undefined : u32(o.version, "child version"),
      detached,
      runTimeoutMs: o.runTimeoutMs === undefined ? undefined : u64(o.runTimeoutMs, "runTimeoutMs"),
    });
    return (detached ? this._op(id, decodeChildRun) : this._op(id, decodeChild)) as Op<T>;
  }

  /** Create an await token; its op completes with the JSON payload sent for the token. */
  createAwaitToken<T = unknown>(opts?: TimeoutOpts): AwaitTokenHandle<T> {
    this._act.checkJournaled("createAwaitToken");
    const timeout = opts?.timeoutMs === undefined ? undefined : u64(opts.timeoutMs, "timeoutMs");
    const t = this._act.host.createAwaitToken(timeout);
    return { token: t.token, op: this._op(t.op, decodeToken) as Op<T> };
  }

  /**
   * Cancel a pending op (D5): waiting on it then rejects with `CancelledError` unless it
   * completed earlier in journal order. On a child op it also cancels the child run.
   * Losers of `Promise.race` are never cancelled implicitly.
   */
  cancel(op: Op<unknown> | bigint): void {
    this._act.checkJournaled("cancel");
    this._act.host.cancelOp(typeof op === "bigint" ? op : op.id);
  }

  /** The op that completes once a graceful cancel of this run is journaled. */
  cancellation(): Op<void> {
    this._act.checkJournaled("cancellation");
    return this._op(this._act.host.cancellation(), decodeUnit) as Op<void>;
  }

  /**
   * Run `fn` with a shielded ctx (D11): ops created through it are not rejected when the
   * run is cancelled. Use it for cleanup and compensation.
   */
  shielded<R>(fn: (s: Ctx) => R): R {
    return fn(this.isShielded ? this : new Ctx(this._act, true));
  }

  // --- effect builders, idempotency key, saga (09-12) -----------------------------------

  /** HTTP effect builders: `await ctx.http.post(url).json(body).send()`. */
  get http(): HttpClient {
    return new HttpClient(this);
  }

  /** SQL statements against `connection`: `await ctx.sql("db").query(stmt, params)`. */
  sql(connection: string): SqlBuilder {
    return new SqlBuilder(connection, this);
  }

  /** Object-store operations against `connection`: `await ctx.object("s3").get(key)`. */
  object(connection: string): ObjectClient {
    return new ObjectClient(connection, this);
  }

  /** The semantic idempotency key for `name` in this run (P-11; equal across SDKs). */
  idempotencyKey(name: string): string {
    return idempotencyKeyFor(this.info().runId, name);
  }

  /** A new, empty `Saga` for this run (D-P9-18). */
  saga(): Saga {
    return new Saga(this);
  }

  /** `step(...)` as a ctx method. */
  step<T>(name: string, fn: () => T | PromiseLike<T>, opts?: Parameters<typeof step>[2]): PromiseLike<T> {
    return step(name, fn, opts);
  }

  _op<T>(id: bigint, decode: (out: never) => T): Op<T> {
    return new Op<T>(this._act, id, decode as never, this.isShielded);
  }
}

export { ChildRun };
