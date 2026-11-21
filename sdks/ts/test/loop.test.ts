// The synchronous drive loop over `effects.wait`: lazy interest, ascending wait lists with
// CANCELLATION, completion order, deadlock detection, closing, cancellation (D11).

import assert from "node:assert/strict";
import { test } from "node:test";

import { CancelledError, DeadlockError, NonRetryableError, ProtocolError, TimeoutError } from "../src/errors.ts";
import { Activation, beginActivation, driveToCompletion } from "../src/loop.ts";
import { Op, decodeSignal, decodeUnit } from "../src/ops.ts";
import { SdkPromise, drain, enqueueJob, pendingJobs } from "../src/promise.ts";
import { CANCELLATION_OP, FakeHost, FIRST_OP, cancelRequested, json, methods, signalReceived, timerFired } from "./helpers.ts";

const A = FIRST_OP;
const B = FIRST_OP + 1n;
const C = FIRST_OP + 2n;

function sleepOp(act: Activation, fake: FakeHost, ms: bigint, shielded = false): Op<void> {
  return new Op(act, fake.sleep(ms), decodeUnit, shielded) as Op<void>;
}

function signalOp(act: Activation, fake: FakeHost, name: string, shielded = false): Op<unknown> {
  return new Op(act, fake.waitSignal(name, undefined), decodeSignal, shielded);
}

function waits(fake: FakeHost): bigint[][] {
  return fake.calls.filter(([m]) => m === "wait").map(([, a]) => a[0] as bigint[]);
}

test("Promise.all over two ops: one result in input order, ascending wait lists + CANCELLATION", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const a = sleepOp(act, fake, 1000n);
  const b = signalOp(act, fake, "go");
  fake.scriptWait([signalReceived(B, { v: 1 })], [timerFired(A)]);
  const root = SdkPromise.all([a, b]);
  driveToCompletion(act, root);
  assert.ok(root.fulfilled);
  assert.deepEqual(root.result, [undefined, { v: 1 }]);
  assert.deepEqual(waits(fake), [
    [A, B, CANCELLATION_OP],
    [A, CANCELLATION_OP],
  ]);
  for (const call of fake.calls.filter(([m]) => m === "wait")) assert.equal(call[1][1], "any");
  assert.equal(act.closed, true);
});

test("completions are resolved in the order wait returns them", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const a = sleepOp(act, fake, 1n);
  const b = sleepOp(act, fake, 2n);
  const order: string[] = [];
  fake.scriptWait([timerFired(B), timerFired(A)]);
  const root = SdkPromise.all([a.then(() => order.push("a")), b.then(() => order.push("b"))]);
  driveToCompletion(act, root);
  assert.deepEqual(order, ["b", "a"]);
  assert.equal(act.waits, 1);
});

test("Promise.race(sleep, signal) resolves with the signal and leaves the sleep pending (no implicit cancel)", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const timer = sleepOp(act, fake, 3_600_000n);
  const sig = signalOp(act, fake, "go");
  fake.scriptWait([signalReceived(B, "payload")]);
  const root = SdkPromise.race([timer.then(() => "timer"), sig]);
  driveToCompletion(act, root);
  assert.equal(root.result, "payload");
  assert.equal(act.entries.get(A)?.promise.settled, false);
  assert.ok(!methods(fake).includes("cancelOp"));
  assert.deepEqual(fake.cancelledOps, []);
});

test("ops are lazy: an op nobody awaits is never waited on", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  sleepOp(act, fake, 10n);
  const b = sleepOp(act, fake, 20n);
  fake.scriptWait([timerFired(B)]);
  const root = b.then(() => "ok");
  driveToCompletion(act, root);
  assert.deepEqual(waits(fake), [[B, CANCELLATION_OP]]);
});

test("a root awaiting a never-settling promise is a deadlock, and wait(any, []) is never called", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const never = new SdkPromise(() => {});
  const root = SdkPromise.resolve().then(() => never);
  assert.throws(
    () => driveToCompletion(act, root),
    (e: unknown) => e instanceof DeadlockError && e instanceof ProtocolError && e.kind === "sdk_protocol" && !e.retryable,
  );
  assert.ok(!methods(fake).includes("wait"));
  assert.equal(act.closed, true);
});

test("after the root settles leftover jobs do not run, and a journaled call from one throws without a host call", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  let leftoverError: unknown;
  const leftover = (): void => {
    try {
      act.checkJournaled("sleep");
      fake.sleep(1n);
    } catch (e) {
      leftoverError = e;
    }
  };
  const root = SdkPromise.resolve().then(() => {
    enqueueJob(leftover);
    return 1;
  });
  driveToCompletion(act, root);
  assert.equal(root.result, 1);
  assert.equal(pendingJobs(), 1, "the leftover job did not run inside the drive loop");
  drain();
  assert.ok(leftoverError instanceof ProtocolError);
  assert.match(String((leftoverError as Error).message), /after the workflow settled/);
  assert.deepEqual(methods(fake), []);
});

test("op ids never pass through Number: 2^64-2 round-trips exactly", () => {
  const fake = new FakeHost();
  const big = 2n ** 64n - 2n;
  fake.nextOp = big;
  const act = beginActivation(fake);
  const op = sleepOp(act, fake, 5n);
  assert.equal(op.id, big);
  assert.notEqual(BigInt(Number(big)), big);
  fake.scriptWait([timerFired(big)]);
  const root = op.then(() => "fired");
  driveToCompletion(act, root);
  assert.equal(root.result, "fired");
  assert.deepEqual(waits(fake), [[big, CANCELLATION_OP]]);
  assert.equal(typeof waits(fake)[0][0], "bigint");
  assert.throws(() => new Op(act, Number(5) as unknown as bigint, decodeUnit, false), /bigint/);
});

test("op failures reject with the mapped SDK error; mismatched outputs are protocol errors", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const sig = signalOp(act, fake, "never");
  const bad = signalOp(act, fake, "odd");
  const failed = new Op(act, fake.sleep(1n), decodeUnit, false);
  fake.scriptWait([
    { op: A, outcome: { tag: "err", val: { tag: "timeout" } } },
    timerFired(B),
    { op: C, outcome: { tag: "err", val: { tag: "non-retryable", val: { kind: "boom", message: "m" } } } },
  ]);
  const root = SdkPromise.allSettled([sig, bad, failed]);
  driveToCompletion(act, root);
  const [r1, r2, r3] = root.result as { status: string; reason: unknown }[];
  assert.ok(r1.reason instanceof TimeoutError);
  assert.ok(r2.reason instanceof NonRetryableError && r2.reason.kind === "protocol");
  assert.ok(r3.reason instanceof NonRetryableError && r3.reason.kind === "boom");
});

test("CANCELLATION rejects pending unshielded ops; shielded ops keep waiting; cancellation is remembered", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const unshielded = sleepOp(act, fake, 3_600_000n);
  const shielded = sleepOp(act, fake, 1000n, true);
  fake.scriptWait([cancelRequested()], [timerFired(B)]);
  const log: string[] = [];
  let later: Op<void> | undefined;
  const root = SdkPromise.all([
    unshielded.then(
      () => log.push("unshielded fired"),
      (e) => {
        log.push(e instanceof CancelledError ? "unshielded cancelled" : "other");
        // A new unshielded wait after the cancel rejects at once, with no host wait.
        later = sleepOp(act, fake, 5n);
        return later.then(
          () => log.push("later fired"),
          () => log.push("later cancelled"),
        );
      },
    ),
    shielded.then(() => log.push("shielded fired")),
  ]);
  driveToCompletion(act, root);
  assert.ok(root.fulfilled);
  assert.deepEqual(log, ["unshielded cancelled", "later cancelled", "shielded fired"]);
  // The second wait lists only the shielded op: no CANCELLATION once it was observed.
  assert.deepEqual(waits(fake), [
    [A, B, CANCELLATION_OP],
    [B],
  ]);
  assert.equal(act.cancelled, true);
});

test("no CANCELLATION in the wait list when every interested op is shielded", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const s = sleepOp(act, fake, 1n, true);
  fake.scriptWait([timerFired(A)]);
  driveToCompletion(act, s.then(() => 1));
  assert.deepEqual(waits(fake), [[A]]);
});

test("an explicitly awaited cancellation op resolves on cancel and is not duplicated in the list", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const cancel = new Op(act, fake.cancellation(), decodeUnit, false);
  const timer = sleepOp(act, fake, 10n);
  fake.scriptWait([cancelRequested()]);
  const root = SdkPromise.race([cancel.then(() => "cancel"), timer.then(() => "timer")]);
  driveToCompletion(act, root);
  assert.equal(root.result, "cancel");
  assert.deepEqual(waits(fake), [[A, CANCELLATION_OP]]);
});

test("payloads decode through the SDK codec (empty data is null)", () => {
  const fake = new FakeHost();
  const act = beginActivation(fake);
  const a = signalOp(act, fake, "x");
  const b = signalOp(act, fake, "y");
  fake.scriptWait([
    signalReceived(A, { k: [1, "ü"] }),
    { op: B, outcome: { tag: "ok", val: { tag: "signal", val: { contentType: "application/json", data: new Uint8Array() } } } },
  ]);
  const root = SdkPromise.all([a, b]);
  driveToCompletion(act, root);
  assert.deepEqual(root.result, [{ k: [1, "ü"] }, null]);
  assert.deepEqual(json(1).contentType, "application/json");
});

test("errors: toFailure/fromFailure round-trip every WIT failure variant", async () => {
  const { RetryableError, InvalidStringError, toFailure, fromFailure } = await import("../src/errors.ts");
  await import("../src/json.ts");
  const cases: [Error, unknown][] = [
    [new NonRetryableError("boom", "m", { a: 1 }), "non-retryable"],
    [new RetryableError("flaky", "m"), "retryable"],
    [new TimeoutError(), "timeout"],
    [new CancelledError(), "cancelled"],
    [new ProtocolError("p"), "non-retryable"],
    [new DeadlockError(), "non-retryable"],
    [new InvalidStringError("s"), "non-retryable"],
  ];
  for (const [e, tag] of cases) {
    const f = toFailure(e, true);
    assert.equal(f.tag, tag, e.name);
    const back = fromFailure(f);
    assert.equal(back.constructor, e instanceof DeadlockError ? ProtocolError : e.constructor, e.name);
    assert.deepEqual(toFailure(back, true), f, e.name);
  }
  const nr = toFailure(new NonRetryableError("boom", "m", { a: 1 }), true);
  assert.ok(nr.tag === "non-retryable" && nr.val.details !== undefined);
  assert.deepEqual((fromFailure(nr) as NonRetryableError).details, { a: 1 });
  for (const e of [new TimeoutError(), new CancelledError(), new ProtocolError("p")]) assert.ok(e instanceof NonRetryableError);
  // Uncaught JS errors: retryable js_exception in step bodies, non-retryable in workflows.
  assert.deepEqual(toFailure(new TypeError("bad"), true), {
    tag: "retryable",
    val: { kind: "js_exception", message: "TypeError: bad", details: undefined },
  });
  assert.equal(toFailure("str", false).tag, "non-retryable");
});
