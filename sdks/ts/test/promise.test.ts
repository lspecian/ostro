// SdkPromise and the job queue: ordering parity with native promises, combinators,
// drain semantics, global installation.

import assert from "node:assert/strict";
import { test } from "node:test";

import {
  SdkPromise,
  drain,
  enqueueJob,
  installGlobals,
  pendingJobs,
  resetJobs,
  restoreJobsAtFront,
  takeJobErrors,
  takeQueuedJobs,
  uninstallGlobals,
} from "../src/promise.ts";

type P = typeof Promise;
type Scenario = (P: P, log: (s: unknown) => void) => void;

function runNative(s: Scenario): Promise<unknown[]> {
  const out: unknown[] = [];
  s(Promise, (x) => out.push(x));
  return new Promise((r) => setImmediate(() => r(out)));
}

function runSdk(s: Scenario): unknown[] {
  resetJobs();
  const out: unknown[] = [];
  s(SdkPromise as unknown as P, (x) => out.push(x));
  drain();
  return out;
}

async function sameOrder(s: Scenario): Promise<unknown[]> {
  const native = await runNative(s);
  const sdk = runSdk(s);
  assert.deepEqual(sdk, native);
  assert.ok(native.length > 0);
  return sdk;
}

test("then chains interleave like native promises", async () => {
  await sameOrder((P, log) => {
    const a = P.resolve(1);
    const b = P.resolve(2);
    a.then(() => log("a1"))
      .then(() => log("a2"))
      .then(() => log("a3"));
    b.then(() => log("b1")).then(() => log("b2"));
    log("sync");
  });
});

test("resolving with a promise takes two extra jobs (adoption)", async () => {
  const order = await sameOrder((P, log) => {
    const inner = P.resolve("x");
    new P((r) => r(inner)).then((v) => log(`adopt:${v}`));
    P.resolve()
      .then(() => log("t1"))
      .then(() => log("t2"))
      .then(() => log("t3"))
      .then(() => log("t4"));
  });
  assert.deepEqual(order, ["t1", "t2", "adopt:x", "t3", "t4"]);
});

test("returning a promise from a handler and a plain thenable", async () => {
  await sameOrder((P, log) => {
    P.resolve()
      .then(() => P.resolve("from-handler"))
      .then((v) => log(v));
    P.resolve({ then: (r: (v: unknown) => void) => r("thenable") }).then((v) => log(v));
    P.resolve()
      .then(() => log("u1"))
      .then(() => log("u2"))
      .then(() => log("u3"))
      .then(() => log("u4"));
  });
});

test("rejection propagation and catch", async () => {
  await sameOrder((P, log) => {
    P.reject("e1")
      .then(() => log("never"))
      .then(() => log("never2"))
      .catch((e) => log(`caught:${e}`));
    P.resolve()
      .then(() => {
        throw "e2";
      })
      .then(
        () => log("never3"),
        (e) => log(`second:${e}`),
      );
    new P(() => {
      throw "executor";
    }).catch((e) => log(`exec:${e}`));
    P.resolve()
      .then(() => log("v1"))
      .then(() => log("v2"))
      .then(() => log("v3"));
  });
});

test("finally passes values and reasons through", async () => {
  await sameOrder((P, log) => {
    P.resolve(3)
      .finally(() => log("f1"))
      .then((v) => log(`v:${v}`));
    P.reject("r")
      .finally(() => log("f2"))
      .catch((e) => log(`e:${e}`));
    P.resolve(4)
      .finally(() => {
        throw "override";
      })
      .catch((e) => log(`o:${e}`));
    P.resolve()
      .then(() => log("w1"))
      .then(() => log("w2"))
      .then(() => log("w3"))
      .then(() => log("w4"));
  });
});

test("combinators settle in the same order and with the same results as native", async () => {
  await sameOrder((P, log) => {
    const thenable = { then: (r: (v: unknown) => void) => r("t") };
    P.all([1, P.resolve(2), thenable]).then((v) => log(["all", v]));
    P.all([]).then((v) => log(["all-empty", v]));
    P.all([P.resolve(1), P.reject("no")]).catch((e) => log(["all-rej", e]));
    P.race([new P(() => {}), P.resolve("r1"), "r2"]).then((v) => log(["race", v]));
    P.allSettled([1, P.reject("x"), thenable]).then((v) => log(["settled", v]));
    P.any([P.reject("a"), P.resolve("b")]).then((v) => log(["any", v]));
    P.any([P.reject("a"), P.reject("b")]).catch((e: { errors: unknown[] }) => log(["any-rej", e.errors]));
    P.any([]).catch((e: { errors: unknown[] }) => log(["any-empty", e.errors]));
    P.resolve()
      .then(() => log("z1"))
      .then(() => log("z2"))
      .then(() => log("z3"))
      .then(() => log("z4"))
      .then(() => log("z5"));
  });
});

test("resolving a promise with itself rejects with TypeError", () => {
  resetJobs();
  let resolveSelf!: (v: unknown) => void;
  const p = new SdkPromise((r) => {
    resolveSelf = r as (v: unknown) => void;
  });
  resolveSelf(p);
  drain();
  assert.ok(p.rejected);
  assert.ok(p.result instanceof TypeError);
});

test("withResolvers, resolve identity and state accessors", () => {
  resetJobs();
  const { promise, resolve } = SdkPromise.withResolvers<number>();
  assert.equal(SdkPromise.resolve(promise), promise);
  assert.equal(promise.settled, false);
  resolve(7);
  assert.equal(promise.fulfilled, true);
  assert.equal(promise.result, 7);
  assert.equal(Object.prototype.toString.call(promise), "[object Promise]");
});

test("an unhandled rejection does not crash the drain", () => {
  resetJobs();
  const log: string[] = [];
  SdkPromise.reject(new Error("nobody listens"));
  SdkPromise.resolve()
    .then(() => {
      throw new Error("also unhandled");
    })
    .then(() => log.push("skipped"));
  SdkPromise.resolve().then(() => log.push("after"));
  drain();
  assert.deepEqual(log, ["after"]);
  assert.equal(pendingJobs(), 0);
});

test("drain runs jobs FIFO, including jobs enqueued while draining", () => {
  resetJobs();
  const log: number[] = [];
  enqueueJob(() => {
    log.push(1);
    enqueueJob(() => {
      log.push(3);
      enqueueJob(() => log.push(5));
    });
  });
  enqueueJob(() => {
    log.push(2);
    enqueueJob(() => log.push(4));
  });
  drain();
  assert.deepEqual(log, [1, 2, 3, 4, 5]);
  assert.equal(pendingJobs(), 0);
});

test("a throwing plain job is recorded and the drain continues", () => {
  resetJobs();
  const log: string[] = [];
  enqueueJob(() => {
    throw new Error("boom");
  });
  enqueueJob(() => log.push("next"));
  drain();
  assert.deepEqual(log, ["next"]);
  const errors = takeJobErrors();
  assert.equal(errors.length, 1);
  assert.match(String(errors[0]), /boom/);
});

test("drain stops when asked and keeps the remaining jobs", () => {
  resetJobs();
  const log: number[] = [];
  for (let i = 0; i < 4; i++) enqueueJob(() => log.push(i));
  drain(() => log.length >= 2);
  assert.deepEqual(log, [0, 1]);
  assert.equal(pendingJobs(), 2);
  drain();
  assert.deepEqual(log, [0, 1, 2, 3]);
});

test("set-aside jobs are restored at the front in original order", () => {
  resetJobs();
  const log: string[] = [];
  enqueueJob(() => log.push("a"));
  enqueueJob(() => log.push("b"));
  const aside = takeQueuedJobs();
  assert.equal(pendingJobs(), 0);
  enqueueJob(() => log.push("new"));
  restoreJobsAtFront(aside);
  drain();
  assert.deepEqual(log, ["a", "b", "new"]);
});

test("installGlobals swaps Promise and queueMicrotask, uninstallGlobals restores them", () => {
  resetJobs();
  const NativePromise = globalThis.Promise;
  const nativeQm = globalThis.queueMicrotask;
  installGlobals();
  try {
    assert.equal(globalThis.Promise, SdkPromise as unknown);
    const log: string[] = [];
    globalThis.queueMicrotask(() => log.push("job"));
    assert.equal(pendingJobs(), 1);
    // `new Promise` in lowered async code now builds an SdkPromise.
    const p = new globalThis.Promise((r) => r(1));
    assert.ok(p instanceof SdkPromise);
    drain();
    assert.deepEqual(log, ["job"]);
  } finally {
    uninstallGlobals();
  }
  assert.equal(globalThis.Promise, NativePromise);
  assert.equal(globalThis.queueMicrotask, nativeQm);
});
