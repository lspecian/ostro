// Spike S9 TS guest: SdkPromise + SDK-owned job loop prototype (09-RESEARCH Pattern 2)
// against the real `ostro:runtime@0.1.0` world. Throwaway; built for QuickJS
// (`--backend quickjs --backend-qjs-disable-async --backend-qjs-stub-wasi`) and
// StarlingMonkey (`--disable all`) with `transform.target = "es2016"` so every async
// function is lowered to generators that call `new Promise(..)` = SdkPromise.
//
// Workflows (describe order):
// - hello:          one step, completes (fuel / instantiate reference).
// - s9-probe:       Promise.all over two locked steps, then Promise.race(sleep 1h,
//                   wait-signal "go"), cancel the loser, one more step.
// - res-*:          `result<run-result, failure>` export mapping probes (question 7).
// - surrogate-*:    lone-surrogate probes (question 8).
// - env:            which web globals the backend provides.
// No bigint literals: the es2016 transform cannot lower them.

import { beginStep } from "ostro:runtime/journal@0.1.0";
import { cancelOp, cancellation, sleep, wait, waitSignal } from "ostro:runtime/effects@0.1.0";

// ---------------------------------------------------------------------------
// SDK-owned job queue and promise
// ---------------------------------------------------------------------------

type Job = () => void;
const jobs: Job[] = [];
export const stats = { jobsRun: 0 };

function enqueue(j: Job): void {
  jobs.push(j);
}

export function drain(): void {
  let i = 0;
  while (i < jobs.length) {
    const j = jobs[i++];
    stats.jobsRun++;
    j();
  }
  jobs.length = 0;
}

const PENDING = 0;
const FULFILLED = 1;
const REJECTED = 2;

type Cb = [unknown, unknown, (v: unknown) => void, (e: unknown) => void];

export class SdkPromise<T = unknown> {
  _s = PENDING;
  _v: unknown = undefined;
  _cbs: Cb[] | null = [];

  constructor(executor: (res: (v?: unknown) => void, rej: (e?: unknown) => void) => void) {
    const [res, rej] = this._resolvers();
    try {
      executor(res, rej);
    } catch (e) {
      rej(e);
    }
  }

  _resolvers(): [(v?: unknown) => void, (e?: unknown) => void] {
    let done = false;
    return [
      (v?: unknown) => {
        if (done) return;
        done = true;
        this._resolve(v);
      },
      (e?: unknown) => {
        if (done) return;
        done = true;
        this._settle(REJECTED, e);
      },
    ];
  }

  _resolve(v: unknown): void {
    if (v === this) {
      this._settle(REJECTED, new TypeError("promise resolved with itself"));
      return;
    }
    if (v !== null && (typeof v === "object" || typeof v === "function")) {
      let then: unknown;
      try {
        then = (v as { then?: unknown }).then;
      } catch (e) {
        this._settle(REJECTED, e);
        return;
      }
      if (typeof then === "function") {
        const [res, rej] = this._resolvers();
        enqueue(() => {
          try {
            (then as Function).call(v, res, rej);
          } catch (e) {
            rej(e);
          }
        });
        return;
      }
    }
    this._settle(FULFILLED, v);
  }

  _settle(s: number, v: unknown): void {
    if (this._s !== PENDING) return;
    this._s = s;
    this._v = v;
    const cbs = this._cbs as Cb[];
    this._cbs = null;
    for (const c of cbs) this._schedule(c);
  }

  _schedule(c: Cb): void {
    enqueue(() => {
      const [onF, onR, res, rej] = c;
      const h = this._s === FULFILLED ? onF : onR;
      if (typeof h !== "function") {
        (this._s === FULFILLED ? res : rej)(this._v);
        return;
      }
      try {
        res((h as Function)(this._v));
      } catch (e) {
        rej(e);
      }
    });
  }

  then(onF?: unknown, onR?: unknown): SdkPromise {
    return new SdkPromise((res, rej) => {
      const c: Cb = [onF, onR, res, rej];
      if (this._s !== PENDING) this._schedule(c);
      else (this._cbs as Cb[]).push(c);
    });
  }

  catch(onR?: unknown): SdkPromise {
    return this.then(undefined, onR);
  }

  finally(f: () => unknown): SdkPromise {
    return this.then(
      (v: unknown) => SdkPromise.resolve(f()).then(() => v),
      (e: unknown) =>
        SdkPromise.resolve(f()).then(() => {
          throw e;
        }),
    );
  }

  static resolve(v?: unknown): SdkPromise {
    if (v instanceof SdkPromise) return v;
    return new SdkPromise((r) => r(v));
  }

  static reject(e?: unknown): SdkPromise {
    return new SdkPromise((_r, j) => j(e));
  }

  static all(it: Iterable<unknown>): SdkPromise {
    return new SdkPromise((res, rej) => {
      const arr = Array.from(it);
      const out: unknown[] = new Array(arr.length);
      let n = arr.length;
      if (n === 0) return res(out);
      arr.forEach((p, i) =>
        SdkPromise.resolve(p).then((v: unknown) => {
          out[i] = v;
          if (--n === 0) res(out);
        }, rej),
      );
    });
  }

  static race(it: Iterable<unknown>): SdkPromise {
    return new SdkPromise((res, rej) => {
      for (const p of Array.from(it)) SdkPromise.resolve(p).then(res, rej);
    });
  }

  static allSettled(it: Iterable<unknown>): SdkPromise {
    return SdkPromise.all(
      Array.from(it).map((p) =>
        SdkPromise.resolve(p).then(
          (value: unknown) => ({ status: "fulfilled", value }),
          (reason: unknown) => ({ status: "rejected", reason }),
        ),
      ),
    );
  }

  static any(it: Iterable<unknown>): SdkPromise {
    return new SdkPromise((res, rej) => {
      const arr = Array.from(it);
      const errs: unknown[] = new Array(arr.length);
      let n = arr.length;
      if (n === 0) return rej(new Error("All promises were rejected"));
      arr.forEach((p, i) =>
        SdkPromise.resolve(p).then(res, (e: unknown) => {
          errs[i] = e;
          if (--n === 0) {
            const err = new Error("All promises were rejected") as Error & { errors: unknown[] };
            err.errors = errs;
            rej(err);
          }
        }),
      );
    });
  }
}

// Installed at module evaluation = pre-init, before any user code runs.
const nativePromise = globalThis.Promise;
(globalThis as Record<string, unknown>).Promise = SdkPromise;
(globalThis as Record<string, unknown>).queueMicrotask = (f: () => void) => enqueue(f);

// ---------------------------------------------------------------------------
// Ops (thenables: `then` registers interest = lazy wait)
// ---------------------------------------------------------------------------

const CANCELLATION = BigInt("18446744073709551615");
type Entry = { p: SdkPromise; res: (v?: unknown) => void; rej: (e?: unknown) => void };
const entries = new Map<bigint, Entry>();
const interested = new Set<bigint>();

function entry(id: bigint): Entry {
  let e = entries.get(id);
  if (!e) {
    let res!: (v?: unknown) => void;
    let rej!: (e?: unknown) => void;
    const p = new SdkPromise((a, b) => {
      res = a;
      rej = b;
    });
    e = { p, res, rej };
    entries.set(id, e);
  }
  return e;
}

export class OpFailed extends Error {
  failure: unknown;
  constructor(failure: unknown) {
    super("op failed: " + JSON.stringify(failure, (_k, v) => (typeof v === "bigint" ? v.toString() : v)));
    this.failure = failure;
  }
}

export class Op {
  id: bigint;
  constructor(id: bigint) {
    this.id = id;
  }
  then(onF?: unknown, onR?: unknown): SdkPromise {
    interested.add(this.id);
    return entry(this.id).p.then(onF, onR);
  }
}

type Completion = { op: bigint; outcome: { tag: string; val?: unknown } };

function complete(c: Completion): void {
  interested.delete(c.op);
  const e = entry(c.op);
  if (c.outcome.tag === "ok") e.res(c.outcome.val);
  else e.rej(new OpFailed(c.outcome.val));
}

let cancelOpId: bigint | null = null;
let closed = false;

/** Drive `root` to settlement inside one synchronous export call. */
export function driveLoop(root: SdkPromise): void {
  for (;;) {
    drain();
    if (root._s !== PENDING) break;
    const ops = Array.from(interested).filter((id) => {
      const e = entries.get(id);
      return !e || e.p._s === PENDING;
    });
    ops.sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    if (ops.length === 0) {
      throw new Error("sdk_protocol: deadlock, the workflow awaits a non-Ostro promise");
    }
    if (cancelOpId === null) cancelOpId = cancellation();
    ops.push(cancelOpId);
    const done = wait(ops, "any") as Completion[];
    for (const c of done) {
      if (c.op === CANCELLATION) {
        for (const id of Array.from(interested)) complete({ op: id, outcome: { tag: "err", val: { tag: "cancelled" } } });
      } else {
        complete(c);
      }
    }
  }
  closed = true;
}

// ---------------------------------------------------------------------------
// UTF-8 + JSON payloads (QuickJS has no TextEncoder)
// ---------------------------------------------------------------------------

export function utf8Encode(s: string): Uint8Array {
  const out: number[] = [];
  for (let i = 0; i < s.length; i++) {
    let c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff && i + 1 < s.length) {
      const d = s.charCodeAt(i + 1);
      if (d >= 0xdc00 && d <= 0xdfff) {
        c = 0x10000 + ((c - 0xd800) << 10) + (d - 0xdc00);
        i++;
      }
    }
    if (c < 0x80) out.push(c);
    else if (c < 0x800) out.push(0xc0 | (c >> 6), 0x80 | (c & 63));
    else if (c < 0x10000) out.push(0xe0 | (c >> 12), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
    else out.push(0xf0 | (c >> 18), 0x80 | ((c >> 12) & 63), 0x80 | ((c >> 6) & 63), 0x80 | (c & 63));
  }
  return new Uint8Array(out);
}

export function utf8Decode(b: Uint8Array): string {
  let s = "";
  for (let i = 0; i < b.length; ) {
    const x = b[i++];
    let c: number;
    if (x < 0x80) c = x;
    else if (x < 0xe0) c = ((x & 31) << 6) | (b[i++] & 63);
    else if (x < 0xf0) c = ((x & 15) << 12) | ((b[i++] & 63) << 6) | (b[i++] & 63);
    else c = ((x & 7) << 18) | ((b[i++] & 63) << 12) | ((b[i++] & 63) << 6) | (b[i++] & 63);
    s += String.fromCodePoint(c);
  }
  return s;
}

type Payload = { contentType: string; data: Uint8Array };

export function jsonPayload(v: unknown): Payload {
  return { contentType: "application/json", data: utf8Encode(JSON.stringify(v === undefined ? null : v)) };
}

export function fromPayload(p: Payload): unknown {
  return p.data.length === 0 ? null : JSON.parse(utf8Decode(p.data));
}

// ---------------------------------------------------------------------------
// Steps (FIFO lock: step order = call order, PF-W1)
// ---------------------------------------------------------------------------

let lockTail: SdkPromise = SdkPromise.resolve();

export function step<T>(name: string, body: () => T): SdkPromise {
  const run = lockTail.then(() => {
    if (closed) throw new Error("sdk_protocol: journaled call after the terminal");
    const s = beginStep(name, { retry: undefined, timeoutMs: undefined, fuel: undefined }) as {
      tag: string;
      val: { tag?: string; val?: unknown; complete?: (o: unknown) => unknown };
    };
    if (s.tag === "replayed") {
      const r = s.val as { tag: string; val: unknown };
      if (r.tag === "ok") return fromPayload(r.val as Payload);
      throw new OpFailed(r.val);
    }
    const out = body();
    (s.val as { complete: (o: unknown) => unknown }).complete({ tag: "ok", val: jsonPayload(out) });
    return out;
  });
  lockTail = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

// ---------------------------------------------------------------------------
// Workflows
// ---------------------------------------------------------------------------

async function hello(_input: unknown): Promise<unknown> {
  return await step("greet", () => "hello, ostro");
}

async function s9Probe(input: unknown): Promise<unknown> {
  const x = ((input as { x?: number }) || {}).x ?? 2;
  const [a, b] = (await Promise.all([step("a", () => x * 2), step("b", () => x * 3)])) as number[];
  const timer = new Op(sleep(BigInt(3600000)));
  const sig = new Op(waitSignal("go", undefined));
  const winner = await Promise.race([
    timer.then(() => "timer"),
    sig.then((v: unknown) => ({ signal: fromPayload((v as { val: Payload }).val) })),
  ]);
  if (winner !== "timer") cancelOp(timer.id);
  const c = await step("c", () => a + b);
  return { a, b, c, winner, promiseIsSdk: Promise === (SdkPromise as unknown), nativePromiseKept: typeof nativePromise };
}

function failure(kind: string, message: string): unknown {
  return { tag: "non-retryable", val: { kind, message, details: undefined } };
}

type Wf = (input: unknown) => unknown;
const registry: Record<string, Wf> = { hello, "s9-probe": s9Probe };
export function register(name: string, wf: Wf): void {
  registry[name] = wf;
}

const extra: Record<string, () => unknown> = {
  // question 7: what does the host receive for each return/throw shape?
  "res-ret-plain": () => ({ tag: "completed", val: jsonPayload("plain") }),
  "res-ret-ok-tagged": () => ({ tag: "ok", val: { tag: "completed", val: jsonPayload("tagged") } }),
  "res-ret-err-tagged": () => ({ tag: "err", val: failure("probe", "returned tagged err") }),
  "res-throw-failure": () => {
    throw failure("probe", "thrown failure variant");
  },
  "res-throw-err-tagged": () => {
    throw { tag: "err", val: failure("probe", "thrown tagged err") };
  },
  "res-throw-error": () => {
    throw new Error("plain Error thrown");
  },
  // question 8
  "surrogate-check": () => {
    const proto = String.prototype as unknown as { isWellFormed?: () => boolean; toWellFormed?: () => string };
    const lone = "a\uD800b";
    return {
      tag: "completed",
      val: jsonPayload({
        isWellFormed: typeof proto.isWellFormed,
        toWellFormed: typeof proto.toWellFormed,
        loneWellFormed: typeof proto.isWellFormed === "function" ? lone.isWellFormed() : null,
        pairWellFormed: typeof proto.isWellFormed === "function" ? "a😀b".isWellFormed() : null,
      }),
    };
  },
  "surrogate-step": () => {
    beginStep("bad\uD800", { retry: undefined, timeoutMs: undefined, fuel: undefined });
    return { tag: "completed", val: jsonPayload("no trap") };
  },
  "surrogate-output": () => ({ tag: "completed", val: { contentType: "text/x\uD800", data: new Uint8Array(0) } }),
  env: () => {
    const g = globalThis as Record<string, unknown>;
    const names = [
      "TextEncoder",
      "TextDecoder",
      "structuredClone",
      "crypto",
      "setTimeout",
      "console",
      "WeakRef",
      "FinalizationRegistry",
      "AggregateError",
      "performance",
    ];
    const out: Record<string, string> = {};
    for (const n of names) out[n] = typeof g[n];
    return { tag: "completed", val: jsonPayload(out) };
  },
};

export const workflows = {
  describe(): unknown[] {
    return [...Object.keys(registry), ...Object.keys(extra)].map((name) => ({
      name,
      version: 1,
      defaultQueue: undefined,
      runTimeoutMs: undefined,
    }));
  },
  run(name: string, input: Payload): unknown {
    const ex = extra[name];
    if (ex) return ex();
    const wf = registry[name];
    if (!wf) throw failure("unknown_workflow", name);
    const root = SdkPromise.resolve().then(() => wf(fromPayload(input)));
    driveLoop(root);
    if (root._s === FULFILLED) return { tag: "completed", val: jsonPayload(root._v) };
    const e = root._v;
    throw failure("workflow_error", e instanceof Error ? e.message : String(e));
  },
};
