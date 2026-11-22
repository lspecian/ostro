// Saga helper (09-12, D-P9-18): LIFO, sequential, shielded, continues past failures and
// attaches every failure to the rethrown error's details (Rust `Saga` parity).

import assert from "node:assert/strict";
import { test } from "node:test";

import { Ctx } from "../src/ctx.ts";
import { CancelledError, NonRetryableError, toFailure } from "../src/errors.ts";
import { beginActivation, driveToCompletion } from "../src/loop.ts";
import { SdkPromise } from "../src/promise.ts";
import { Saga, SagaReport } from "../src/saga.ts";
import { step } from "../src/step.ts";
import { CANCELLATION_OP, FakeHost, FIRST_OP, cancelRequested, lowered, timerFired } from "./helpers.ts";

interface Run {
  value: unknown;
  error: unknown;
}

function runWorkflow(fake: FakeHost, body: (ctx: Ctx) => unknown): Run {
  const act = beginActivation(fake);
  const ctx = new Ctx(act, false);
  const root = SdkPromise.resolve().then(() => body(ctx));
  const out: Run = { value: undefined, error: undefined };
  try {
    driveToCompletion(act, root);
  } catch (e) {
    out.error = e;
  }
  if (root.fulfilled) out.value = root.result;
  else if (root.rejected) out.error = root.result;
  return out;
}

function stepNames(fake: FakeHost): string[] {
  return fake.calls.filter(([m]) => m === "beginStep").map(([, a]) => a[0] as string);
}

const bookA = (): SdkPromise<string> => step("book_a", () => "a");
const bookB = (): SdkPromise<string> => step("book_b", () => "b");
const bookC = (fail: boolean): SdkPromise<string> =>
  step("book_c", () => {
    if (fail) throw new NonRetryableError("book_c_failed", "no rooms left");
    return "c";
  });
const cancelA = (): SdkPromise<null> => step("cancel_a", () => null);
const cancelB = (fail = false): SdkPromise<null> =>
  step("cancel_b", () => {
    if (fail) throw new NonRetryableError("cancel_b_failed", "refund rejected");
    return null;
  });

function three(cancelBFails: boolean) {
  return lowered(function* (ctx: Ctx) {
    const saga = ctx.saga();
    yield bookA();
    saga.add("cancel_a", () => cancelA());
    yield bookB();
    saga.add("cancel_b", () => cancelB(cancelBFails));
    try {
      yield bookC(true);
    } catch (e) {
      yield saga.failWith(e);
    }
    throw new Error("unreachable");
  });
}

test("saga: three compensates in reverse order with details", () => {
  const fake = new FakeHost();
  const run = runWorkflow(fake, three(false));
  assert.deepEqual(stepNames(fake), ["book_a", "book_b", "book_c", "cancel_b", "cancel_a"]);
  const err = run.error as NonRetryableError;
  assert.equal(err.kind, "book_c_failed");
  assert.deepEqual(err.details, { compensated: ["cancel_b", "cancel_a"], compensation_failures: [] });
});

test("saga: a failing compensation does not stop the rest", () => {
  const fake = new FakeHost();
  const run = runWorkflow(fake, three(true));
  assert.deepEqual(stepNames(fake), ["book_a", "book_b", "book_c", "cancel_b", "cancel_a"]);
  const err = run.error as NonRetryableError;
  assert.equal(err.kind, "book_c_failed");
  assert.deepEqual(err.details, {
    compensated: ["cancel_a"],
    compensation_failures: [
      { compensation: "cancel_b", error: { kind: "cancel_b_failed", message: "refund rejected", retryable: false } },
    ],
  });
});

test("saga: no failure, no compensation", () => {
  const fake = new FakeHost();
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const saga = new Saga(ctx);
      yield bookA();
      saga.add("cancel_a", () => cancelA());
      yield bookB();
      saga.add("cancel_b", () => cancelB());
      yield bookC(false);
      return saga.length;
    }),
  );
  assert.equal(run.value, 2);
  assert.deepEqual(stepNames(fake), ["book_a", "book_b", "book_c"]);
});

test("saga: compensations run shielded after the run was cancelled", () => {
  const fake = new FakeHost();
  fake.scriptWait([cancelRequested()], [timerFired(FIRST_OP + 1n)], [timerFired(FIRST_OP + 2n)]);
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const saga = ctx.saga();
      yield bookA();
      saga.add("cancel_a", (s) => s.sleep(1000).then(() => cancelA()));
      yield bookB();
      saga.add("cancel_b", (s) => s.sleep(1000).then(() => cancelB()));
      try {
        yield ctx.sleep(3_600_000);
      } catch (e) {
        yield saga.failWith(e);
      }
    }),
  );
  assert.ok(run.error instanceof CancelledError, String(run.error));
  assert.deepEqual(stepNames(fake), ["book_a", "book_b", "cancel_b", "cancel_a"]);
  assert.deepEqual((run.error as CancelledError).details, { compensated: ["cancel_b", "cancel_a"], compensation_failures: [] });
  const waits = fake.calls.filter(([m]) => m === "wait").map(([, a]) => a[0]);
  assert.deepEqual(waits, [[FIRST_OP, CANCELLATION_OP], [FIRST_OP + 1n], [FIRST_OP + 2n]]);
});

test("saga: a cancellation arriving during compensation does not interrupt it", () => {
  const fake = new FakeHost();
  fake.scriptWait([cancelRequested()], [timerFired(FIRST_OP + 1n)]);
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const saga = ctx.saga();
      const sibling = ctx.sleep(3_600_000);
      sibling.then(
        () => undefined,
        () => undefined,
      );
      saga.add("cancel_a", () => cancelA());
      saga.add("cancel_b", (s) => s.sleep(1000).then(() => cancelB()));
      const report = (yield saga.compensate()) as SagaReport;
      let siblingCancelled = false;
      try {
        yield sibling;
      } catch (e) {
        siblingCancelled = e instanceof CancelledError;
      }
      return { report: report.toJSON(), siblingCancelled };
    }),
  );
  assert.equal(run.error, undefined);
  assert.deepEqual(run.value, { report: { compensated: ["cancel_b", "cancel_a"], compensation_failures: [] }, siblingCancelled: true });
  assert.deepEqual(stepNames(fake), ["cancel_b", "cancel_a"]);
});

test("saga: failWith keeps earlier details and wraps plain errors as js_exception", () => {
  const fake = new FakeHost();
  const run = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const out: unknown[] = [];
      const saga = ctx.saga();
      saga.add("cancel_a", () => cancelA());
      try {
        yield saga.failWith(new NonRetryableError("k", "m", { order: 7 }));
      } catch (e) {
        out.push((e as NonRetryableError).details);
      }
      const saga2 = ctx.saga();
      saga2.add("boom", () => {
        throw new TypeError("bad");
      });
      try {
        yield saga2.failWith(new RangeError("x"));
      } catch (e) {
        const n = e as NonRetryableError;
        out.push([n.kind, n.message, n.details]);
      }
      return out;
    }),
  );
  assert.equal(run.error, undefined);
  assert.deepEqual(run.value, [
    { compensated: ["cancel_a"], compensation_failures: [], details: { order: 7 } },
    [
      "js_exception",
      "RangeError: x",
      {
        compensated: [],
        compensation_failures: [{ compensation: "boom", error: { kind: "js_exception", message: "TypeError: bad", retryable: false } }],
      },
    ],
  ]);
});

test("saga details reach the WIT failure", () => {
  const f = toFailure(new NonRetryableError("book_c_failed", "no rooms", { compensated: ["cancel_a"], compensation_failures: [] }), false);
  assert.equal(f.tag, "non-retryable");
  const info = (f as { val: { details?: { contentType: string; data: Uint8Array } } }).val;
  assert.equal(new TextDecoder().decode(info.details?.data), '{"compensated":["cancel_a"],"compensation_failures":[]}');
});
