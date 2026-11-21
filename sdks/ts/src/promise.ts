// SDK-owned promise and job queue (09-RESEARCH Pattern 2, verified on QuickJS and
// StarlingMonkey in Spike S9).
//
// The `run` export is synchronous, so neither the engine's microtask queue nor native
// promises can drive a workflow. At module evaluation (pre-init) the SDK installs
// `SdkPromise` as `globalThis.Promise` and overrides `queueMicrotask`; the bundle config
// (`rolldown.ostro.mjs`, `target: "es2016"`) lowers every async function to a generator
// driven by `new Promise(..)` / `Promise.resolve(..).then(..)`, which then run on this
// queue. `drain()` runs the queue synchronously to quiescence.
//
// Reaction ordering follows ECMA-262 (PromiseReactionJob, PromiseResolveThenableJob):
// resolving with a thenable costs one job to call its `then` plus the reaction job, so
// ordering matches native promises for the same program.
//
// Erasable TypeScript only, no `#private` fields (they lower to WeakMaps), no bigint
// literals (es2016 does not lower them).

/** A queued job. */
export type Job = () => void;

let queue: Job[] = [];
let head = 0;
let idleHook: (() => boolean) | undefined;
let jobErrors: unknown[] = [];

/** Append a job to the SDK job queue. */
export function enqueueJob(job: Job): void {
  queue.push(job);
}

/** Number of queued, not yet run jobs. */
export function pendingJobs(): number {
  return queue.length - head;
}

/**
 * Remove every queued job and return them in order (step exclusivity, D-P9-16: the jobs
 * queued when a step opens are set aside until the step closes).
 */
export function takeQueuedJobs(): Job[] {
  const rest = queue.slice(head);
  queue = [];
  head = 0;
  return rest;
}

/** Put `jobs` back at the front of the queue, ahead of anything queued now, in order. */
export function restoreJobsAtFront(jobs: Job[]): void {
  if (jobs.length === 0) return;
  const rest = queue.slice(head);
  queue = jobs.concat(rest);
  head = 0;
}

/** Drop every queued job and recorded job error (a fresh queue per activation). */
export function resetJobs(): void {
  queue = [];
  head = 0;
  jobErrors = [];
}

/**
 * Install the hook `drain` calls whenever the queue is empty. It returns true when it
 * did something (for example enqueued jobs), and `drain` then continues.
 */
export function setIdleHook(hook: (() => boolean) | undefined): void {
  idleHook = hook;
}

/** Exceptions thrown by plain jobs (`queueMicrotask` callbacks) since the last reset. */
export function takeJobErrors(): unknown[] {
  const out = jobErrors;
  jobErrors = [];
  return out;
}

/**
 * Run queued jobs in FIFO order until the queue is empty and the idle hook has nothing
 * more to do. Jobs enqueued while draining run in the same drain. `stop` is checked
 * before every job; when it returns true the drain returns and the remaining jobs stay
 * queued.
 */
export function drain(stop?: () => boolean): void {
  for (;;) {
    while (head < queue.length) {
      if (stop !== undefined && stop()) return;
      const job = queue[head];
      head += 1;
      try {
        job();
      } catch (e) {
        // A plain job threw (a promise job never throws: it settles its promise).
        jobErrors.push(e);
      }
    }
    queue = [];
    head = 0;
    if (stop !== undefined && stop()) return;
    if (idleHook === undefined || !idleHook()) return;
  }
}

const PENDING = 0;
const FULFILLED = 1;
const REJECTED = 2;

type Reaction = {
  onFulfilled: unknown;
  onRejected: unknown;
  resolve: (v: unknown) => void;
  reject: (e: unknown) => void;
};

type Executor<T> = (resolve: (value?: T | PromiseLike<T>) => void, reject: (reason?: unknown) => void) => void;

function isObjectLike(v: unknown): v is object {
  return v !== null && (typeof v === "object" || typeof v === "function");
}

function aggregateError(errors: unknown[]): Error {
  const Agg = (globalThis as { AggregateError?: new (errors: unknown[], message: string) => Error }).AggregateError;
  if (typeof Agg === "function") return new Agg(errors, "All promises were rejected");
  const err = new Error("All promises were rejected") as Error & { errors: unknown[] };
  err.name = "AggregateError";
  err.errors = errors;
  return err;
}

/** A Promises/A+ promise whose jobs run on the SDK job queue. */
export class SdkPromise<T = unknown> {
  /** 0 pending, 1 fulfilled, 2 rejected. */
  _state: number = PENDING;
  _value: unknown = undefined;
  _reactions: Reaction[] | null = [];

  constructor(executor: Executor<T>) {
    if (typeof executor !== "function") throw new TypeError("Promise resolver is not a function");
    const [resolve, reject] = this._resolvingFunctions();
    try {
      executor(resolve as (value?: T | PromiseLike<T>) => void, reject);
    } catch (e) {
      reject(e);
    }
  }

  /** True once fulfilled or rejected. */
  get settled(): boolean {
    return this._state !== PENDING;
  }

  get fulfilled(): boolean {
    return this._state === FULFILLED;
  }

  get rejected(): boolean {
    return this._state === REJECTED;
  }

  /** The fulfilment value or rejection reason (undefined while pending). */
  get result(): unknown {
    return this._value;
  }

  _resolvingFunctions(): [(v?: unknown) => void, (e?: unknown) => void] {
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
      this._settle(REJECTED, new TypeError("Chaining cycle detected for promise"));
      return;
    }
    if (!isObjectLike(v)) {
      this._settle(FULFILLED, v);
      return;
    }
    let then: unknown;
    try {
      then = (v as { then?: unknown }).then;
    } catch (e) {
      this._settle(REJECTED, e);
      return;
    }
    if (typeof then !== "function") {
      this._settle(FULFILLED, v);
      return;
    }
    // PromiseResolveThenableJob.
    const [resolve, reject] = this._resolvingFunctions();
    enqueueJob(() => {
      try {
        (then as (f: unknown, r: unknown) => unknown).call(v, resolve, reject);
      } catch (e) {
        reject(e);
      }
    });
  }

  _settle(state: number, value: unknown): void {
    if (this._state !== PENDING) return;
    this._state = state;
    this._value = value;
    const reactions = this._reactions as Reaction[];
    this._reactions = null;
    for (const r of reactions) this._schedule(r);
  }

  _schedule(r: Reaction): void {
    enqueueJob(() => {
      const handler = this._state === FULFILLED ? r.onFulfilled : r.onRejected;
      if (typeof handler !== "function") {
        if (this._state === FULFILLED) r.resolve(this._value);
        else r.reject(this._value);
        return;
      }
      let out: unknown;
      try {
        out = (handler as (v: unknown) => unknown)(this._value);
      } catch (e) {
        r.reject(e);
        return;
      }
      r.resolve(out);
    });
  }

  then<R1 = T, R2 = never>(
    onFulfilled?: ((value: T) => R1 | PromiseLike<R1>) | null,
    onRejected?: ((reason: unknown) => R2 | PromiseLike<R2>) | null,
  ): SdkPromise<R1 | R2> {
    return new SdkPromise<R1 | R2>((resolve, reject) => {
      const r: Reaction = {
        onFulfilled,
        onRejected,
        resolve: resolve as (v: unknown) => void,
        reject,
      };
      if (this._state === PENDING) (this._reactions as Reaction[]).push(r);
      else this._schedule(r);
    });
  }

  catch<R = never>(onRejected?: ((reason: unknown) => R | PromiseLike<R>) | null): SdkPromise<T | R> {
    return this.then(undefined, onRejected);
  }

  finally(onFinally?: (() => unknown) | null): SdkPromise<T> {
    if (typeof onFinally !== "function") return this.then(onFinally, onFinally) as SdkPromise<T>;
    return this.then(
      (v: T) => SdkPromise.resolve(onFinally()).then(() => v),
      (e: unknown) =>
        SdkPromise.resolve(onFinally()).then(() => {
          throw e;
        }),
    ) as SdkPromise<T>;
  }

  get [Symbol.toStringTag](): string {
    return "Promise";
  }

  static resolve<U>(value?: U | PromiseLike<U>): SdkPromise<U> {
    if (value instanceof SdkPromise) return value as SdkPromise<U>;
    return new SdkPromise<U>((resolve) => resolve(value));
  }

  static reject<U = never>(reason?: unknown): SdkPromise<U> {
    return new SdkPromise<U>((_resolve, reject) => reject(reason));
  }

  static withResolvers<U>(): {
    promise: SdkPromise<U>;
    resolve: (value?: U | PromiseLike<U>) => void;
    reject: (reason?: unknown) => void;
  } {
    let resolve!: (value?: U | PromiseLike<U>) => void;
    let reject!: (reason?: unknown) => void;
    const promise = new SdkPromise<U>((a, b) => {
      resolve = a;
      reject = b;
    });
    return { promise, resolve, reject };
  }

  static all(values: Iterable<unknown>): SdkPromise<unknown[]> {
    return new SdkPromise<unknown[]>((resolve, reject) => {
      const items = Array.from(values);
      const out: unknown[] = new Array(items.length);
      let remaining = items.length + 1;
      items.forEach((item, i) => {
        SdkPromise.resolve(item).then((v) => {
          out[i] = v;
          remaining -= 1;
          if (remaining === 0) resolve(out);
        }, reject);
      });
      remaining -= 1;
      if (remaining === 0) resolve(out);
    });
  }

  static allSettled(values: Iterable<unknown>): SdkPromise<unknown[]> {
    return new SdkPromise<unknown[]>((resolve) => {
      const items = Array.from(values);
      const out: unknown[] = new Array(items.length);
      let remaining = items.length + 1;
      const done = (): void => {
        remaining -= 1;
        if (remaining === 0) resolve(out);
      };
      items.forEach((item, i) => {
        SdkPromise.resolve(item).then(
          (value) => {
            out[i] = { status: "fulfilled", value };
            done();
          },
          (reason) => {
            out[i] = { status: "rejected", reason };
            done();
          },
        );
      });
      done();
    });
  }

  static race(values: Iterable<unknown>): SdkPromise<unknown> {
    return new SdkPromise<unknown>((resolve, reject) => {
      for (const item of Array.from(values)) SdkPromise.resolve(item).then(resolve, reject);
    });
  }

  static any(values: Iterable<unknown>): SdkPromise<unknown> {
    return new SdkPromise<unknown>((resolve, reject) => {
      const items = Array.from(values);
      const errors: unknown[] = new Array(items.length);
      let remaining = items.length + 1;
      const failed = (): void => {
        remaining -= 1;
        if (remaining === 0) reject(aggregateError(errors));
      };
      items.forEach((item, i) => {
        SdkPromise.resolve(item).then(resolve, (e) => {
          errors[i] = e;
          failed();
        });
      });
      failed();
    });
  }
}

/** `queueMicrotask` on the SDK job queue (also the bundle's injected `queueMicrotask`). */
export function sdkQueueMicrotask(callback: () => void): void {
  if (typeof callback !== "function") throw new TypeError("queueMicrotask: callback is not a function");
  enqueueJob(callback);
}

let nativePromise: unknown;
let nativeQueueMicrotask: unknown;
let globalsInstalled = false;

/**
 * Install `SdkPromise` as `globalThis.Promise` and route `queueMicrotask` to the SDK job
 * queue. Called once at module evaluation of the guest (pre-init) by `export.ts`.
 */
export function installGlobals(): void {
  if (globalsInstalled) return;
  const g = globalThis as Record<string, unknown>;
  nativePromise = g.Promise;
  nativeQueueMicrotask = g.queueMicrotask;
  g.Promise = SdkPromise;
  g.queueMicrotask = sdkQueueMicrotask;
  globalsInstalled = true;
}

/** Undo `installGlobals` (host tests only). */
export function uninstallGlobals(): void {
  if (!globalsInstalled) return;
  const g = globalThis as Record<string, unknown>;
  g.Promise = nativePromise;
  g.queueMicrotask = nativeQueueMicrotask;
  globalsInstalled = false;
}
