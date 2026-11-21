// Shared test helpers: a recording, scripted fake `Host` for host-side SDK tests.

import type {
  AwaitToken,
  ChildOptions,
  Completion,
  CompletionAck,
  Datetime,
  Host,
  HttpRequest,
  LogLevel,
  ObjectRequest,
  Outcome,
  Payload,
  RunInfo,
  SqlRequest,
  StepHandle,
  StepOptions,
  StepStart,
  WaitMode,
} from "../src/host.ts";
import { CANCELLATION_OP } from "../src/host.ts";

export { CANCELLATION_OP };

/** Op ids handed out by the fake host start here, one per allocating call, in order. */
export const FIRST_OP = 16n;

/** The fake host's `journal.step` handle. */
export interface FakeStep extends StepHandle {
  name: string;
  seq: bigint;
}

export type Call = [method: string, args: unknown[]];

/**
 * A recording, scripted implementation of `Host`.
 *
 * - Every call is appended to `calls` as `[method, args]`.
 * - Op-allocating calls return fresh `bigint` ids from `FIRST_OP` upward;
 *   `cancellation()` returns `CANCELLATION_OP` (2^64-1).
 * - `beginStep` pops the next scripted start (`scriptBeginStep`), by default `execute`.
 * - `wait` pops the next scripted completion list (`scriptWait`); an unscripted wait
 *   throws, because it is a test bug.
 */
export class FakeHost implements Host {
  calls: Call[] = [];
  runInfo: RunInfo = {
    runId: "00000000-0000-8000-8000-000000000001",
    workflow: "test",
    workflowVersion: 1,
    attempt: 1,
    moduleHash: "0".repeat(64),
  };
  clock: Datetime = { seconds: 1_700_000_000n, nanoseconds: 0 };
  replaying = false;
  patches = new Map<string, boolean>();
  statuses: string[] = [];
  logs: [LogLevel, string, [string, string][]][] = [];
  cancelledOps: bigint[] = [];
  nextOp: bigint = FIRST_OP;
  nextSeq = 1n;
  #u64 = 0n;
  #uuids = 0;
  #beginSteps: StepStart[] = [];
  #stepAcks: CompletionAck[] = [];
  #waits: Completion[][] = [];

  // --- scripting --------------------------------------------------------------------

  scriptBeginStep(...starts: StepStart[]): void {
    this.#beginSteps.push(...starts);
  }

  scriptStepAck(...acks: CompletionAck[]): void {
    this.#stepAcks.push(...acks);
  }

  /** Queue `wait` results; each argument is the list one `wait` call returns. */
  scriptWait(...completionLists: Completion[][]): void {
    this.#waits.push(...completionLists);
  }

  opsIssued(): bigint[] {
    const out: bigint[] = [];
    for (let op = FIRST_OP; op < this.nextOp; op++) out.push(op);
    return out;
  }

  #record(method: string, ...args: unknown[]): void {
    this.calls.push([method, args]);
  }

  #op(): bigint {
    const op = this.nextOp;
    this.nextOp += 1n;
    return op;
  }

  // --- context ----------------------------------------------------------------------

  info(): RunInfo {
    this.#record("info");
    return this.runInfo;
  }

  now(): Datetime {
    this.#record("now");
    return this.clock;
  }

  randomU64(): bigint {
    this.#record("randomU64");
    // splitmix64 over a counter: deterministic and distinct.
    const mask = 2n ** 64n - 1n;
    this.#u64 = (this.#u64 + 0x9e3779b97f4a7c15n) & mask;
    let z = this.#u64;
    z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & mask;
    z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & mask;
    return z ^ (z >> 31n);
  }

  randomBytes(len: number): Uint8Array {
    this.#record("randomBytes", len);
    return Uint8Array.from({ length: len }, (_, i) => (i * 31 + 7) % 256);
  }

  uuidV7(): string {
    this.#record("uuidV7");
    this.#uuids += 1;
    return `018f0000-0000-7000-8000-${this.#uuids.toString(16).padStart(12, "0")}`;
  }

  isReplaying(): boolean {
    this.#record("isReplaying");
    return this.replaying;
  }

  patched(changeId: string): boolean {
    this.#record("patched", changeId);
    if (!this.patches.has(changeId)) this.patches.set(changeId, true);
    return this.patches.get(changeId) === true;
  }

  setStatus(json: string): void {
    this.#record("setStatus", json);
    this.statuses.push(json);
  }

  continueAsNewSuggested(): boolean {
    this.#record("continueAsNewSuggested");
    return false;
  }

  // --- journal ----------------------------------------------------------------------

  beginStep(name: string, opts: StepOptions): StepStart {
    this.#record("beginStep", name, opts);
    const scripted = this.#beginSteps.shift();
    if (scripted !== undefined) return scripted;
    const step: FakeStep = { name, seq: this.nextSeq };
    this.nextSeq += 1n;
    return { tag: "execute", val: step };
  }

  stepSeq(step: StepHandle): bigint {
    this.#record("stepSeq", step);
    return (step as FakeStep).seq;
  }

  stepComplete(step: StepHandle, outcome: Outcome<Payload>): CompletionAck {
    this.#record("stepComplete", step, outcome);
    return this.#stepAcks.shift() ?? "recorded";
  }

  // --- effects ----------------------------------------------------------------------

  httpFetch(req: HttpRequest): bigint {
    this.#record("httpFetch", req);
    return this.#op();
  }

  sqlQuery(req: SqlRequest): bigint {
    this.#record("sqlQuery", req);
    return this.#op();
  }

  objectStore(req: ObjectRequest): bigint {
    this.#record("objectStore", req);
    return this.#op();
  }

  sleep(durationMs: bigint): bigint {
    this.#record("sleep", durationMs);
    return this.#op();
  }

  sleepUntil(at: Datetime): bigint {
    this.#record("sleepUntil", at);
    return this.#op();
  }

  waitSignal(name: string, timeoutMs: bigint | undefined): bigint {
    this.#record("waitSignal", name, timeoutMs);
    return this.#op();
  }

  startChild(workflow: string, input: Payload, opts: ChildOptions): bigint {
    this.#record("startChild", workflow, input, opts);
    return this.#op();
  }

  createAwaitToken(timeoutMs: bigint | undefined): AwaitToken {
    this.#record("createAwaitToken", timeoutMs);
    const op = this.#op();
    return { op, token: `tok-${op}` };
  }

  cancellation(): bigint {
    this.#record("cancellation");
    return CANCELLATION_OP;
  }

  cancelOp(op: bigint): void {
    this.#record("cancelOp", op);
    this.cancelledOps.push(op);
  }

  wait(ops: bigint[], mode: WaitMode): Completion[] {
    this.#record("wait", [...ops], mode);
    const scripted = this.#waits.shift();
    if (scripted === undefined) {
      throw new Error(`FakeHost: unscripted wait(ops=[${ops.join(", ")}], mode=${mode})`);
    }
    return scripted;
  }

  // --- log --------------------------------------------------------------------------

  logEmit(level: LogLevel, message: string, fields: [string, string][]): void {
    this.#record("logEmit", level, message, [...fields]);
    this.logs.push([level, message, [...fields]]);
  }
}

// --- helpers for the SDK core tests (09-10) ------------------------------------------
//
// Kept as free functions: the scaffold test pins FakeHost's method set to the WIT.

import { SdkPromise } from "../src/promise.ts";

/** `application/json` payload of `v` (test-side encoder; the SDK has its own). */
export function json(v: unknown): Payload {
  return { contentType: "application/json", data: new TextEncoder().encode(JSON.stringify(v)) };
}

/** JSON value of a payload (empty data = null). */
export function unjson(p: Payload): unknown {
  return p.data.length === 0 ? null : JSON.parse(new TextDecoder().decode(p.data));
}

/** A successful completion of `op`. */
export function done(op: bigint, val: import("../src/host.ts").OpOutput): Completion {
  return { op, outcome: { tag: "ok", val } };
}

/** A timer completion. */
export function timerFired(op: bigint): Completion {
  return { op, outcome: { tag: "ok", val: { tag: "timer" } } };
}

/** A signal completion carrying JSON `v`. */
export function signalReceived(op: bigint, v: unknown): Completion {
  return { op, outcome: { tag: "ok", val: { tag: "signal", val: json(v) } } };
}

/** The cancellation completion. */
export function cancelRequested(): Completion {
  return { op: CANCELLATION_OP, outcome: { tag: "ok", val: { tag: "cancel-requested" } } };
}

/** Method names of the recorded calls, optionally without some methods. */
export function methods(fake: FakeHost, ...without: string[]): string[] {
  return fake.calls.map(([m]) => m).filter((m) => !without.includes(m));
}

/** The recorded `stepComplete` outcomes, in order. */
export function stepOutcomes(fake: FakeHost): Outcome<Payload>[] {
  return fake.calls.filter(([m]) => m === "stepComplete").map(([, a]) => a[1] as Outcome<Payload>);
}

/**
 * A FakeHost that replays `live`: every step `live` completed with `recorded` replays with
 * that outcome; wait results are scripted by the caller.
 */
export function replayOf(live: FakeHost): FakeHost {
  const replay = new FakeHost();
  replay.replaying = true;
  replay.clock = live.clock;
  for (const outcome of stepOutcomes(live)) replay.scriptBeginStep({ tag: "replayed", val: outcome });
  return replay;
}

/**
 * The async-to-generator lowering Rolldown emits for `target: "es2016"` (oxc runtime
 * helper `asyncToGenerator`), bound to SdkPromise. Host tests write workflow bodies as
 * `lowered(function* () { const x = yield step(...); ... })`, which runs exactly like the
 * bundled `async function` would inside the guest (native async functions in Node use the
 * engine's own promise and microtask queue, never the SDK's).
 */
export function lowered<A extends unknown[], R>(
  gen: (...args: A) => Generator<unknown, R, any>,
): (...args: A) => SdkPromise<R> {
  return function (this: unknown, ...args: A): SdkPromise<R> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    return new SdkPromise<R>((resolve, reject) => {
      const it = gen.apply(self, args);
      const advance = (key: "next" | "throw", arg: unknown): void => {
        let info: IteratorResult<unknown, R>;
        try {
          info = it[key](arg as never);
        } catch (e) {
          reject(e);
          return;
        }
        if (info.done) resolve(info.value);
        else SdkPromise.resolve(info.value).then(next, thrw);
      };
      const next = (v: unknown): void => advance("next", v);
      const thrw = (e: unknown): void => advance("throw", e);
      next(undefined);
    });
  };
}
