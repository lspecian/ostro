// Ops: thenables over durable operations.
//
// Effect calls are eager (the op id is allocated by the host when the call is made) and
// waiting is lazy: `then` registers interest, so only awaited ops enter the next
// `effects.wait`. Op ids are `bigint` end to end (PF-W3); 2^64-1 cannot be a Number.
// `Promise.all`/`race` over ops call `then` from a job, which is deterministic.

import { NonRetryableError } from "./errors.ts";
import type { HttpResponse, OpOutput, SqlResult } from "./host.ts";
import { fromPayload } from "./json.ts";
import type { Activation, Decode } from "./loop.ts";
import type { SdkPromise } from "./promise.ts";

/** A pending durable operation that resolves to `T`. */
export class Op<T = unknown> implements PromiseLike<T> {
  /** The op id (deterministic per run). */
  readonly id: bigint;
  /** True if the op was created through a shielded ctx (D11). */
  readonly shielded: boolean;
  _act: Activation;
  _decode: Decode<T>;

  constructor(act: Activation, id: bigint, decode: Decode<T>, shielded: boolean) {
    if (typeof id !== "bigint") throw new TypeError("op ids are bigint");
    this._act = act;
    this.id = id;
    this._decode = decode;
    this.shielded = shielded;
  }

  /** The op's promise (registers interest on first use). */
  promise(): SdkPromise<T> {
    return this._act.awaitOp(this.id, this._decode as Decode<unknown>, this.shielded) as SdkPromise<T>;
  }

  then<R1 = T, R2 = never>(
    onFulfilled?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onRejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): SdkPromise<R1 | R2> {
    return this.promise().then(onFulfilled, onRejected);
  }

  catch<R = never>(onRejected?: ((reason: unknown) => R | PromiseLike<R>) | null): SdkPromise<T | R> {
    return this.promise().catch(onRejected);
  }

  finally(onFinally?: (() => unknown) | null): SdkPromise<T> {
    return this.promise().finally(onFinally);
  }
}

/** The result of a detached child start (PF-W7: `{"run_id": "<id>"}`). */
export class ChildRun {
  readonly runId: string;
  constructor(runId: string) {
    this.runId = runId;
  }
  toJSON(): { run_id: string } {
    return { run_id: this.runId };
  }
}

function unexpected(what: string, out: OpOutput): never {
  throw new NonRetryableError("protocol", `expected ${what} output, got ${out.tag}`);
}

/** Timer and cancellation ops resolve to `undefined`. */
export function decodeUnit(out: OpOutput): undefined {
  if (out.tag === "timer" || out.tag === "cancel-requested") return undefined;
  return unexpected("timer", out);
}

/** Signal ops resolve to the JSON-decoded signal payload. */
export function decodeSignal(out: OpOutput): unknown {
  if (out.tag === "signal") return fromPayload(out.val);
  return unexpected("signal", out);
}

/** Await-token ops resolve to the JSON-decoded completion payload. */
export function decodeToken(out: OpOutput): unknown {
  if (out.tag === "token") return fromPayload(out.val);
  return unexpected("token", out);
}

/** Attached child ops resolve to the JSON-decoded child output (empty output = null). */
export function decodeChild(out: OpOutput): unknown {
  if (out.tag === "child") return fromPayload(out.val);
  return unexpected("child", out);
}

/** Detached child ops resolve to a `ChildRun`. */
export function decodeChildRun(out: OpOutput): ChildRun {
  if (out.tag !== "child") return unexpected("child", out);
  const v = fromPayload(out.val) as { run_id?: unknown } | null;
  if (v === null || typeof v !== "object" || typeof v.run_id !== "string") {
    throw new NonRetryableError("protocol", "detached child output has no run_id");
  }
  return new ChildRun(v.run_id);
}

/** HTTP ops resolve to the raw response (builders land in 09-12). */
export function decodeHttp(out: OpOutput): HttpResponse {
  if (out.tag === "http") return out.val;
  return unexpected("http", out);
}

/** SQL ops resolve to the raw result. */
export function decodeSql(out: OpOutput): SqlResult {
  if (out.tag === "sql") return out.val;
  return unexpected("sql", out);
}

/** Object-store ops resolve to the raw bytes. */
export function decodeObject(out: OpOutput): Uint8Array {
  if (out.tag === "object") return out.val;
  return unexpected("object", out);
}
