// Steps (lock, exclusivity, resolution rule), ctx, shielding, timers and the determinism
// surface, on the FakeHost.

import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { ContinueAsNew, Ctx, f64FromU64 } from "../src/ctx.ts";
import { install, uninstall } from "../src/determinism.ts";
import {
  CancelledError,
  InvalidStringError,
  NonRetryableError,
  ProtocolError,
  RetryableError,
} from "../src/errors.ts";
import type { Outcome, Payload } from "../src/host.ts";
import { beginActivation, driveToCompletion } from "../src/loop.ts";
import type { Activation } from "../src/loop.ts";
import { ChildRun } from "../src/ops.ts";
import { SdkPromise, drain } from "../src/promise.ts";
import { RetryPolicy, STEP_AWAITS_DURABLE, step, stepOpen, toWitStepOptions } from "../src/step.ts";
import {
  CANCELLATION_OP,
  FakeHost,
  FIRST_OP,
  cancelRequested,
  done,
  json,
  lowered,
  methods,
  replayOf,
  signalReceived,
  stepOutcomes,
  timerFired,
  unjson,
} from "./helpers.ts";

const vectorsDir = fileURLToPath(new URL("../../../conformance/vectors/", import.meta.url));

interface Run {
  act: Activation;
  root: SdkPromise<unknown>;
  error: unknown;
}

/** Run `body(ctx)` as a workflow on `fake` (the loop `run` uses, minus the export). */
function runWorkflow(fake: FakeHost, body: (ctx: Ctx) => unknown): Run {
  const act = beginActivation(fake);
  const ctx = new Ctx(act, false);
  const root = SdkPromise.resolve().then(() => body(ctx));
  let error: unknown;
  try {
    driveToCompletion(act, root);
  } catch (e) {
    error = e;
  }
  return { act, root, error };
}

function withDeterminism<T>(f: () => T): T {
  install();
  try {
    return f();
  } finally {
    uninstall();
  }
}

function ok(v: unknown): Outcome<Payload> {
  return { tag: "ok", val: json(v) };
}

// --- steps ----------------------------------------------------------------------------

test("a replayed step returns the journaled value without running the body", () => {
  const fake = new FakeHost();
  fake.scriptBeginStep({ tag: "replayed", val: ok({ n: 42 }) });
  let ran = false;
  const r = runWorkflow(fake, () =>
    step("charge", () => {
      ran = true;
      return { n: 0 };
    }),
  );
  assert.equal(r.error, undefined);
  assert.deepEqual(r.root.result, { n: 42 });
  assert.equal(ran, false);
  assert.deepEqual(methods(fake), ["beginStep"]);
});

test("an executed step journals JSON and returns the value decoded from it", () => {
  const fake = new FakeHost();
  const r = runWorkflow(fake, () => step("s", () => ({ a: 1, gone: undefined, when: new Date(0) })));
  assert.deepEqual(r.root.result, { a: 1, when: "1970-01-01T00:00:00.000Z" });
  assert.deepEqual(methods(fake), ["beginStep", "stepComplete"]);
  assert.deepEqual(unjson((stepOutcomes(fake)[0] as { val: Payload }).val), { a: 1, when: "1970-01-01T00:00:00.000Z" });
  assert.deepEqual(fake.calls[0][1], ["s", { retry: undefined, timeoutMs: undefined, fuel: undefined }]);
});

test("step failures: NonRetryable, Retryable, other exceptions (js_exception) and invalid outputs", () => {
  const fake = new FakeHost();
  const errors: unknown[] = [];
  const r = runWorkflow(
    fake,
    lowered(function* () {
      for (const body of [
        () => {
          throw new NonRetryableError("bad_input", "nope");
        },
        () => {
          throw new RetryableError("http_5xx", "later");
        },
        () => {
          throw new TypeError("oops");
        },
        () => SdkPromise.reject(new Error("async oops")),
        () => ({ big: BigInt(1) }),
      ]) {
        try {
          yield step("s", body);
        } catch (e) {
          errors.push(e);
        }
      }
      return "done";
    }),
  );
  assert.equal(r.root.result, "done");
  const recorded = stepOutcomes(fake).map((o) => (o.tag === "err" ? o.val : o));
  assert.deepEqual(recorded, [
    { tag: "non-retryable", val: { kind: "bad_input", message: "nope", details: undefined } },
    { tag: "retryable", val: { kind: "http_5xx", message: "later", details: undefined } },
    { tag: "retryable", val: { kind: "js_exception", message: "TypeError: oops", details: undefined } },
    { tag: "retryable", val: { kind: "js_exception", message: "Error: async oops", details: undefined } },
    {
      tag: "non-retryable",
      val: { kind: "invalid_json", message: "value is not JSON-encodable: Do not know how to serialize a BigInt", details: undefined },
    },
  ]);
  assert.ok(errors[0] instanceof NonRetryableError && (errors[0] as NonRetryableError).kind === "bad_input");
  assert.ok(errors[1] instanceof RetryableError);
  assert.equal((errors[2] as RetryableError).kind, "js_exception");
  assert.equal(errors.length, 5);
});

test("retry-now re-runs the body on the same handle", () => {
  const fake = new FakeHost();
  fake.scriptStepAck("retry-now", "retry-now");
  let attempts = 0;
  const r = runWorkflow(fake, () =>
    step("flaky", () => {
      attempts += 1;
      if (attempts < 3) throw new RetryableError("transient", `attempt ${attempts}`);
      return attempts;
    }),
  );
  assert.equal(r.root.result, 3);
  assert.equal(attempts, 3);
  assert.deepEqual(methods(fake), ["beginStep", "stepComplete", "stepComplete", "stepComplete"]);
});

test("retry policy: defaults only fill a partially specified policy; none without retry", () => {
  assert.deepEqual(toWitStepOptions(undefined), { retry: undefined, timeoutMs: undefined, fuel: undefined });
  assert.deepEqual(toWitStepOptions({ retry: { maxAttempts: 5 }, timeoutMs: 2000, fuel: 10 }), {
    retry: { maxAttempts: 5, initialIntervalMs: 1000n, maxIntervalMs: 60000n, backoffCoefficient: 2, nonRetryableKinds: [] },
    timeoutMs: 2000n,
    fuel: 10n,
  });
  assert.deepEqual(new RetryPolicy({ nonRetryableKinds: ["x"] }).toWit().nonRetryableKinds, ["x"]);
  assert.throws(() => toWitStepOptions({ timeoutMs: -1 }), RangeError);
});

test("Promise.all over two steps: begin_step(b) only after a completed; results in call order", () => {
  const fake = new FakeHost();
  const r = runWorkflow(
    fake,
    lowered(function* () {
      return yield SdkPromise.all([
        step(
          "a",
          lowered(function* () {
            yield SdkPromise.resolve();
            return "A";
          }),
        ),
        step("b", () => "B"),
      ]);
    }),
  );
  assert.deepEqual(r.root.result, ["A", "B"]);
  assert.deepEqual(
    fake.calls.filter(([m]) => m === "beginStep" || m === "stepComplete").map(([m, a]) => `${m}:${m === "beginStep" ? a[0] : (a[0] as { name: string }).name}`),
    ["beginStep:a", "stepComplete:a", "beginStep:b", "stepComplete:b"],
  );
});

test("a step body awaiting ctx.sleep completes non-retryable sdk_protocol", () => {
  const fake = new FakeHost();
  const r = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      try {
        yield step(
          "bad",
          lowered(function* () {
            yield ctx.sleep(1000);
          }),
        );
      } catch (e) {
        return (e as ProtocolError).kind;
      }
      return "no error";
    }),
  );
  assert.equal(r.root.result, "sdk_protocol");
  const [outcome] = stepOutcomes(fake);
  assert.ok(outcome.tag === "err" && outcome.val.tag === "non-retryable" && outcome.val.val.kind === "sdk_protocol");
  assert.ok(!methods(fake).includes("sleep"), "no durable op is issued from inside a step");
});

test("a step body awaiting an op created outside the step completes sdk_protocol (quiescence)", () => {
  const fake = new FakeHost();
  const r = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const timer = ctx.sleep(1000);
      try {
        yield step("waits", () => timer.then(() => "late"));
      } catch (e) {
        return [(e as ProtocolError).kind, (e as Error).message];
      }
      return "no error";
    }),
  );
  assert.deepEqual(r.root.result, ["sdk_protocol", STEP_AWAITS_DURABLE]);
  assert.ok(!methods(fake).includes("wait"), "the loop never waits while a step is open");
});

test("B1: a sibling queued before the step calls the host only after step_complete", () => {
  const fake = new FakeHost();
  fake.scriptWait([timerFired(FIRST_OP)]);
  const r = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const sibling = lowered(function* () {
        yield SdkPromise.resolve();
        const n = ctx.randomU64();
        yield ctx.sleep(1000);
        return n.toString();
      })();
      const s = yield step(
        "draw",
        lowered(function* () {
          yield SdkPromise.resolve();
          return ctx.randomU64().toString();
        }),
      );
      return { step: s, sibling: yield sibling };
    }),
  );
  assert.equal(r.error, undefined);
  assert.deepEqual(methods(fake), ["beginStep", "randomU64", "stepComplete", "randomU64", "sleep", "wait"]);
});

test("B1: siblings A, B queued before a step run in order after it closes, then the caller; replay is identical", () => {
  const program = (log: string[]) =>
    lowered(function* (ctx: Ctx) {
      lowered(function* () {
        yield SdkPromise.resolve();
        log.push("A");
        ctx.info();
      })();
      lowered(function* () {
        yield SdkPromise.resolve();
        log.push("B");
        ctx.isReplaying();
      })();
      const v = yield step(
        "s",
        lowered(function* () {
          yield SdkPromise.resolve();
          log.push("body");
          return 1;
        }),
      );
      log.push("caller");
      ctx.uuid7();
      return v;
    });
  const liveLog: string[] = [];
  const live = new FakeHost();
  runWorkflow(live, program(liveLog));
  assert.deepEqual(liveLog, ["body", "A", "B", "caller"]);
  const replayLog: string[] = [];
  const replay = replayOf(live);
  const r = runWorkflow(replay, program(replayLog));
  assert.equal(r.root.result, 1);
  assert.deepEqual(replayLog, ["A", "B", "caller"]);
  assert.deepEqual(methods(replay), methods(live, "stepComplete"));
});

test("B1: a multi-hop sibling interleaves identically on a live run and its replay", () => {
  const program = (log: string[]) =>
    lowered(function* (ctx: Ctx) {
      const sibling = lowered(function* () {
        for (let hop = 1; hop <= 3; hop++) {
          yield SdkPromise.resolve();
          log.push(`sib${hop}:${ctx.randomU64() % 1000n}`);
        }
      })();
      const v = yield step(
        "s",
        lowered(function* () {
          yield SdkPromise.resolve();
          yield SdkPromise.resolve();
          return "v";
        }),
      );
      log.push(`caller1:${v}`);
      yield SdkPromise.resolve();
      log.push(`caller2:${ctx.randomU64() % 1000n}`);
      yield sibling;
      return log.length;
    });
  const liveLog: string[] = [];
  const live = new FakeHost();
  runWorkflow(live, program(liveLog));
  const replayLog: string[] = [];
  const replay = replayOf(live);
  runWorkflow(replay, program(replayLog));
  assert.ok(liveLog.length >= 5);
  assert.deepEqual(replayLog, liveLog);
  assert.deepEqual(methods(replay), methods(live, "stepComplete"));
});

test("the caller's synchronous continuation never runs with a step open; jobs it queues wait for the step", () => {
  const fake = new FakeHost();
  fake.scriptWait([timerFired(FIRST_OP)]);
  const log: string[] = [];
  const r = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const s = step("a", () => {
        log.push("body");
        return "A";
      });
      const t = ctx.sleep(10);
      SdkPromise.resolve().then(() => log.push("caller job"));
      log.push("caller sync");
      const [sv] = (yield SdkPromise.all([s, t])) as unknown[];
      return sv;
    }),
  );
  assert.equal(r.error, undefined);
  assert.equal(r.root.result, "A");
  assert.deepEqual(log, ["caller sync", "body", "caller job"]);
  assert.deepEqual(methods(fake), ["sleep", "beginStep", "stepComplete", "wait"]);
});

test("journaled calls inside a step throw ProtocolError without a host call; context reads are allowed", () => {
  const fake = new FakeHost();
  const seen: string[] = [];
  runWorkflow(fake, (ctx) =>
    step("inside", () => {
      ctx.now();
      ctx.randomU64();
      ctx.log("info", "hi");
      for (const f of [() => ctx.patched("p"), () => ctx.setStatus({}), () => ctx.waitSignal("x"), () => step("nested", () => 1)]) {
        try {
          f();
        } catch (e) {
          seen.push((e as ProtocolError).kind);
        }
      }
      assert.equal(stepOpen(), true);
      return 1;
    }),
  );
  assert.deepEqual(seen, ["sdk_protocol", "sdk_protocol", "sdk_protocol", "sdk_protocol"]);
  assert.deepEqual(methods(fake), ["beginStep", "now", "randomU64", "logEmit", "stepComplete"]);
  assert.equal(stepOpen(), false);
});

test("a step name with a lone surrogate is invalid_string before any host call", () => {
  const fake = new FakeHost();
  const r = runWorkflow(fake, () => step("bad\uD800", () => 1));
  assert.ok(r.root.result instanceof InvalidStringError);
  assert.deepEqual(methods(fake), []);
});

// --- timers ---------------------------------------------------------------------------

test("B1 timers: setTimeout inside an open step throws sdk_protocol and the step records it", () => {
  const fake = new FakeHost();
  withDeterminism(() =>
    runWorkflow(fake, () =>
      step("timer-in-step", () => {
        setTimeout(() => {}, 1000);
        return 1;
      }).catch((e: ProtocolError) => e.kind),
    ),
  );
  const [outcome] = stepOutcomes(fake);
  assert.ok(outcome.tag === "err" && outcome.val.tag === "non-retryable" && outcome.val.val.kind === "sdk_protocol");
  assert.ok(!methods(fake).includes("sleep"));
});

test("timers: setTimeout outside a step is one durable sleep; clearTimeout issues cancel-op", () => {
  const fake = new FakeHost();
  fake.scriptWait([timerFired(FIRST_OP)]);
  const fired: string[] = [];
  const r = withDeterminism(() =>
    runWorkflow(
      fake,
      () =>
        new SdkPromise((resolve) => {
          const keep = setTimeout((tag: string) => {
            fired.push(tag);
            resolve(fired);
          }, 1000, "f");
          const drop = setTimeout(() => fired.push("never"), 5000);
          clearTimeout(drop);
          clearTimeout(drop);
          assert.equal(typeof keep, "number");
        }),
    ),
  );
  assert.equal(r.error, undefined);
  assert.deepEqual(fired, ["f"]);
  assert.deepEqual(fake.calls.filter(([m]) => m === "sleep").map(([, a]) => a[0]), [1000n, 5000n]);
  assert.deepEqual(fake.cancelledOps, [FIRST_OP + 1n]);
  assert.deepEqual(fake.calls.find(([m]) => m === "wait")?.[1][0], [FIRST_OP, CANCELLATION_OP]);
});

test("timers: setInterval is not supported; zero-delay setTimeout is a job, not a timer", () => {
  const fake = new FakeHost();
  const r = withDeterminism(() =>
    runWorkflow(
      fake,
      () =>
        new SdkPromise((resolve) => {
          assert.throws(() => setInterval(() => {}, 10), /not supported/);
          setTimeout(() => resolve("yielded"), 0);
        }),
    ),
  );
  assert.equal(r.root.result, "yielded");
  assert.deepEqual(methods(fake), []);
});

// --- determinism (WF-07) --------------------------------------------------------------

test("WF-07: Date.now(), new Date() and performance.now() are logical, inside and outside steps", () => {
  const fake = new FakeHost();
  fake.clock = { seconds: 1_700_000_000n, nanoseconds: 123_000_000 };
  const r = withDeterminism(() =>
    runWorkflow(fake, () =>
      step("t", () => [Date.now(), new Date().getTime(), performance.now()]).then((inside) => ({
        inside,
        outside: [Date.now(), new Date().getTime(), performance.now(), typeof Date(), new Date(5).getTime()],
      })),
    ),
  );
  const t = 1_700_000_000_123;
  assert.deepEqual(r.root.result, { inside: [t, t, t], outside: [t, t, t, "string", 5] });
  assert.ok(new Date() instanceof Date);
});

test("WF-07: Math.random, crypto.getRandomValues and crypto.randomUUID call the host", () => {
  const fake = new FakeHost();
  const probe = new FakeHost();
  const u = probe.randomU64();
  const r = withDeterminism(() =>
    runWorkflow(fake, () => {
      const m = Math.random();
      const bytes = crypto.getRandomValues(new Uint8Array(4));
      const words = crypto.getRandomValues(new Uint32Array(2));
      const id = crypto.randomUUID();
      return { m, bytes: Array.from(bytes), words: words.length, id };
    }),
  );
  const out = r.root.result as { m: number; bytes: number[]; words: number; id: string };
  assert.equal(out.m, f64FromU64(u));
  assert.deepEqual(out.bytes, [7, 38, 69, 100]);
  assert.match(out.id, /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
  assert.deepEqual(
    fake.calls.map(([m, a]) => [m, ...a]),
    [["randomU64"], ["randomBytes", 4], ["randomBytes", 8], ["randomBytes", 16]],
  );
});

test("WF-07: a Map built from host data iterates identically on a live run and its replay", () => {
  const program = (ctx: Ctx) =>
    step("load", () => {
      const o: Record<string, number> = {};
      for (let i = 0; i < 16; i++) o[`k${(ctx.randomU64() % 97n).toString()}`] = i;
      return o;
    }).then((o) => {
      const m = new Map(Object.entries(o as Record<string, number>));
      m.set("late", 1);
      const set = new Set(Object.keys(o as object).reverse());
      return [Array.from(m.keys()), Array.from(set)];
    });
  const live = new FakeHost();
  const a = runWorkflow(live, program);
  const b = runWorkflow(replayOf(live), program);
  assert.ok(((a.root.result as string[][])[0]).length > 2);
  assert.deepEqual(b.root.result, a.root.result);
});

test("console.* goes to log.emit; WeakRef is strong; FinalizationRegistry is a no-op", () => {
  const fake = new FakeHost();
  withDeterminism(() =>
    runWorkflow(fake, () => {
      console.log("x");
      console.warn("w", 1, { a: [1] });
      console.error(new Error("e"));
      const target = {};
      const ref = new WeakRef(target);
      assert.equal(ref.deref(), target);
      const reg = new FinalizationRegistry(() => {
        throw new Error("never");
      });
      reg.register(target, "held");
      return 1;
    }),
  );
  assert.deepEqual(fake.logs[0], ["info", "x", []]);
  assert.deepEqual(fake.logs[1], ["warn", 'w 1 {"a":[1]}', []]);
  assert.equal(fake.logs[2][0], "error");
  assert.match(fake.logs[2][1], /^Error: e/);
});

test("ctx.random_f64() = Number(u >> 11n) * 2**-53, and random_f64.json vectors pass", (t) => {
  const fake = new FakeHost();
  const probe = new FakeHost();
  const u = probe.randomU64();
  const r = runWorkflow(fake, (ctx) => [ctx.random_f64(), ctx.randomF64(), ctx.random_u64(), ctx.uuid7(), ctx.random_bytes(2).length]);
  const out = r.root.result as unknown[];
  assert.equal(out[0], Number(u >> 11n) * 2 ** -53);
  assert.equal(typeof out[2], "bigint");
  const file = `${vectorsDir}random_f64.json`;
  if (!existsSync(file)) {
    t.skip("conformance/vectors/random_f64.json is written by 09-06 and is not present");
    return;
  }
  const doc = JSON.parse(readFileSync(file, "utf8")) as { cases: { input: { u64: string }; expected: { f64_bits_hex: string } }[] };
  for (const c of doc.cases) {
    const view = new DataView(new ArrayBuffer(8));
    view.setFloat64(0, f64FromU64(BigInt(c.input.u64)), false);
    assert.equal(Buffer.from(view.buffer).toString("hex"), c.expected.f64_bits_hex, c.input.u64);
  }
});

// --- ctx --------------------------------------------------------------------------------

test("ctx: patched, setStatus, signals with timeout, children (attached and detached), await tokens, cancel", () => {
  const fake = new FakeHost();
  fake.patches.set("old", false);
  const sig = FIRST_OP;
  const attached = FIRST_OP + 1n;
  const detached = FIRST_OP + 2n;
  const tok = FIRST_OP + 3n;
  fake.scriptWait([
    signalReceived(sig, { v: 1 }),
    done(attached, { tag: "child", val: json({ echo: 1 }) }),
    done(detached, { tag: "child", val: json({ run_id: "r-9" }) }),
    done(tok, { tag: "token", val: json("approved") }),
  ]);
  const r = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      const flags = [ctx.patched("new"), ctx.patched("old")];
      ctx.setStatus({ stage: "one" });
      const s = ctx.waitSignal("go", { timeoutMs: 10_000 });
      const c = ctx.startChild("leaf", { n: 1 }, { queue: "q", version: 2, runTimeoutMs: 5000 });
      const d = ctx.startChild({ name: "audit" }, null, { detached: true });
      const token = ctx.createAwaitToken({ timeoutMs: 60_000 });
      const [sv, cv, dv, tv] = (yield SdkPromise.all([s, c, d, token.op])) as unknown[];
      ctx.cancel(c);
      return { flags, sv, cv, run: (dv as ChildRun).runId, isRun: dv instanceof ChildRun, tv, token: token.token };
    }),
  );
  assert.equal(r.error, undefined);
  assert.deepEqual(r.root.result, {
    flags: [true, false],
    sv: { v: 1 },
    cv: { echo: 1 },
    run: "r-9",
    isRun: true,
    tv: "approved",
    token: `tok-${tok}`,
  });
  assert.deepEqual(fake.statuses, ['{"stage":"one"}']);
  const child = fake.calls.find(([m]) => m === "startChild") as [string, unknown[]];
  assert.equal(child[1][0], "leaf");
  assert.deepEqual(unjson(child[1][1] as Payload), { n: 1 });
  assert.deepEqual(child[1][2], { queue: "q", version: 2, detached: false, runTimeoutMs: 5000n });
  assert.deepEqual(fake.calls.find(([m]) => m === "waitSignal")?.[1], ["go", 10_000n]);
  assert.deepEqual(fake.cancelledOps, [attached]);
});

test("ctx: continueAsNew throws a ContinueAsNew carrying the encoded input", () => {
  const fake = new FakeHost();
  const r = runWorkflow(fake, (ctx) => ctx.continueAsNew({ n: 2 }, { workflow: "other", version: 3 }));
  const e = r.root.result as ContinueAsNew;
  assert.ok(e instanceof ContinueAsNew);
  assert.deepEqual(unjson(e.input), { n: 2 });
  assert.equal(e.workflow, "other");
  assert.equal(e.version, 3);
});

test("after the workflow settles, a leftover job calling ctx.sleep throws ProtocolError without a host call", () => {
  const fake = new FakeHost();
  let leftover: unknown;
  let saved: Ctx | undefined;
  const r = runWorkflow(fake, (ctx) => {
    saved = ctx;
    SdkPromise.resolve()
      .then(() => undefined)
      .then(() => {
        try {
          ctx.sleep(10);
        } catch (e) {
          leftover = e;
        }
      });
    return "done";
  });
  assert.equal(r.root.result, "done");
  assert.equal(leftover, undefined, "the leftover job did not run inside the loop");
  drain();
  assert.ok(leftover instanceof ProtocolError);
  assert.throws(() => (saved as Ctx).waitSignal("x"), ProtocolError);
  assert.deepEqual(methods(fake), []);
});

// --- cancellation and shielding (D11) --------------------------------------------------

test("CANCELLATION rejects pending unshielded ops; ops from a shielded ctx keep waiting; cancellation is remembered", () => {
  const fake = new FakeHost();
  const work = FIRST_OP;
  const cleanup = FIRST_OP + 1n;
  fake.scriptWait([cancelRequested()], [timerFired(cleanup)]);
  const log: string[] = [];
  const r = runWorkflow(
    fake,
    lowered(function* (ctx: Ctx) {
      try {
        yield ctx.sleep(3_600_000);
        log.push("slept");
      } catch (e) {
        log.push(e instanceof CancelledError ? "cancelled" : "other");
        yield ctx.shielded(
          lowered(function* (s: Ctx) {
            assert.equal(s.isShielded, true);
            yield s.sleep(1000);
            log.push("cleanup done");
            yield step("release", () => "released");
          }),
        );
        try {
          yield ctx.sleep(5);
        } catch (e2) {
          log.push(e2 instanceof CancelledError ? "remembered" : "other");
        }
        throw e;
      }
    }),
  );
  assert.ok(r.root.result instanceof CancelledError);
  assert.deepEqual(log, ["cancelled", "cleanup done", "remembered"]);
  const waits = fake.calls.filter(([m]) => m === "wait").map(([, a]) => a[0]);
  assert.deepEqual(waits, [[work, CANCELLATION_OP], [cleanup]]);
});
