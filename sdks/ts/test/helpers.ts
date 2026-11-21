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
