// TypeScript conformance workflows, v1 (plan 09-15, SDK-05 / WF-07).
//
// Mirrors guests/conf-rust exactly: same base names, inputs, step names and outputs (the
// contract tables are in guests/conf-rust/src/lib.rs), exported as `ts_<base>` (D-P9-23).
// The engine-leg specs (conformance/spec/engine) and the server-leg specs
// (conformance/spec/server) name the base; the harness adds the prefix.
//
// Written as idiomatic TypeScript on purpose, so the suite tests the SDK as users write
// it: `Promise.all` for joins, `Promise.race` + explicit `ctx.cancel` for selects,
// `ctx.shielded(async (s) => ...)` with the explicit shielded ctx, `ctx.saga()`, bigint
// op ids, native `Date.now()` / `Math.random()` / `crypto.getRandomValues` / `Map`.
//
// conformance/guests/ts-v2 exports the v2 bodies of `conf_patch` and `conf_ndet` (the only
// workflows the engine leg runs on the v2 module).

import {
  CancelledError,
  NonRetryableError,
  OstroError,
  RetryPolicy,
  RetryableError,
  TimeoutError,
  decodeUtf8,
  step,
  workflow,
} from "ostro";
import type { ChildOpts, ChildRun, Ctx, Op } from "ostro";

export { workflows } from "ostro";

const HOUR_MS = 3_600_000;

type Json = unknown;
type Obj = Record<string, unknown>;

/** Registers `ts_<base>`. */
function wf<I = Json>(base: string, fn: (ctx: Ctx, input: I) => Promise<Json>): void {
  workflow<I, Json>({ name: `ts_${base}` }, fn);
}

/** A step that returns `value` (default options). */
function named(name: string, value: string): PromiseLike<string> {
  return step(name, () => value);
}

function unexpected(what: string, detail: unknown): NonRetryableError {
  let d: string;
  try {
    d = JSON.stringify(detail) ?? String(detail);
  } catch {
    d = String(detail);
  }
  return new NonRetryableError("unexpected", `${what}: ${d}`);
}

function asObj(v: unknown): Obj {
  return v !== null && typeof v === "object" && !Array.isArray(v) ? (v as Obj) : {};
}

function isCancelled(e: unknown): boolean {
  return e instanceof CancelledError;
}

// --- steps -----------------------------------------------------------------------

wf("conf_steps", async () => {
  const a = await named("a", "a");
  const b = await named("b", "b");
  const c = await named("c", "c");
  return { a, b, c };
});

wf("conf_step_fail", async () => {
  try {
    const v = await step("boom", (): string => {
      throw new NonRetryableError("boom", "always fails");
    });
    throw unexpected("boom succeeded", v);
  } catch (e) {
    if (e instanceof OstroError && e.kind !== "unexpected") return { caught: e.kind };
    throw e;
  }
});

wf("conf_step_rng", async (ctx) => {
  // Drawn inside the body: the host's step-local stream (D7).
  const value = await step("draw", () => ctx.randomU64().toString());
  return { value };
});

wf("conf_step_sibling", async (ctx) => {
  // B1 / C-STEP-04: the async step body yields (`await Promise.resolve()`) while a sibling
  // task is runnable. Step exclusivity sets the sibling aside until the step closed, so
  // its ctx draw comes from the ctx stream (not the step-local one) and its timer lands
  // after the step in the journal, exactly like the Rust reference's sequential code.
  const sibling = async (): Promise<string> => {
    await Promise.resolve();
    const r = ctx.randomU64().toString();
    await ctx.sleep(1000);
    return r;
  };
  const [s, siblingRand] = await Promise.all([
    step("s", async () => {
      await Promise.resolve();
      return "s";
    }),
    sibling(),
  ]);
  return { step: s, sibling_rand: siblingRand };
});

/** Native time, randomness and Map iteration (WF-07) through the SDK's determinism surface. */
function observeNative(): Obj {
  const m = new Map<string, number>();
  for (let i = 0; i < 16; i++) m.set(`k${i}`, i);
  const mapOrder: string[] = [];
  for (const k of m.keys()) mapOrder.push(k);
  // A Map keyed by native random draws: its iteration order follows the draws.
  const drawn = new Map<number, number>();
  for (let i = 0; i < 4; i++) drawn.set(Math.random(), i);
  const drawnOrder: number[] = [];
  for (const v of drawn.values()) drawnOrder.push(v);
  return {
    time_ms: Date.now(),
    date_iso: new Date().toISOString(),
    map_order: mapOrder,
    random: Math.random(),
    drawn_order: drawnOrder,
  };
}

wf("conf_native", async () => {
  const outside = observeNative();
  const inside = await step("observe", () => observeNative());
  return { outside, inside };
});

// --- retries ---------------------------------------------------------------------

wf<{ succeed_after_ms: number }>("conf_retry", async (ctx, input) => {
  // Run start in logical time; each attempt observes max(last ts, retry_at) (D22).
  const t0 = ctx.nowMs();
  const n = input.succeed_after_ms;
  const elapsed = await step(
    "flaky",
    () => {
      const e = Math.max(0, ctx.nowMs() - t0);
      if (e < n) throw new RetryableError("transient", `${e} ms < ${n} ms`);
      return e;
    },
    {
      retry: new RetryPolicy({ maxAttempts: 3, initialIntervalMs: 1000, maxIntervalMs: 60_000, backoffCoefficient: 2.0 }),
    },
  );
  return { elapsed_ms: elapsed };
});

wf("conf_retry_exhaust", async () => {
  await step(
    "always",
    (): string => {
      throw new RetryableError("transient", "always fails");
    },
    { retry: { maxAttempts: 2, initialIntervalMs: 1000, maxIntervalMs: 60_000, backoffCoefficient: 2.0 } },
  );
  throw unexpected("always", "succeeded");
});

// --- timers and signals ----------------------------------------------------------

wf("conf_timer", async (ctx) => {
  await ctx.sleep(HOUR_MS);
  return { slept: true };
});

wf("conf_timer_vs_signal", async (ctx) => {
  const timer = ctx.sleep(HOUR_MS);
  const go = ctx.waitSignal("go");
  const w = await Promise.race([
    timer.then(() => ({ winner: "timer" as const, payload: undefined as unknown })),
    go.then((payload) => ({ winner: "signal" as const, payload })),
  ]);
  return w.winner === "timer" ? { winner: "timer" } : { winner: "signal", payload: w.payload };
});

wf("conf_signals", async (ctx) => {
  const first = await ctx.waitSignal("go");
  const second = await ctx.waitSignal("go");
  return { got: [first, second] };
});

wf("conf_signal_timeout", async (ctx) => {
  try {
    const payload = await ctx.waitSignal("never", { timeoutMs: 10_000 });
    return { timeout: false, payload };
  } catch (e) {
    if (e instanceof TimeoutError) return { timeout: true };
    throw e;
  }
});

wf("conf_gather", async (ctx) => {
  const [, , signal] = await Promise.all([ctx.sleep(1000), ctx.sleep(2000), ctx.waitSignal("go")]);
  return { signal };
});

/** Awaits a cancelled loser; true if it rejected with CancelledError. */
async function loserCancelled(op: Op<unknown>): Promise<boolean> {
  try {
    await op;
    return false;
  } catch (e) {
    if (isCancelled(e)) return true;
    throw e;
  }
}

wf("conf_race_cancel", async (ctx) => {
  const timer = ctx.sleep(HOUR_MS);
  const go = ctx.waitSignal("go");
  const winner = await Promise.race([timer.then(() => "timer" as const), go.then(() => "signal" as const)]);
  const loser: Op<unknown> = winner === "timer" ? go : timer;
  ctx.cancel(loser);
  if (!(await loserCancelled(loser))) throw unexpected("loser wait", "completed");
  return { winner, loser_cancelled: true };
});

// --- cancellation ----------------------------------------------------------------

wf("conf_cancel_wait", async (ctx) => {
  await ctx.sleep(HOUR_MS);
  return { slept: true };
});

wf("conf_cancel_shielded", async (ctx) => {
  try {
    await ctx.sleep(HOUR_MS);
    return { slept: true };
  } catch (e) {
    if (isCancelled(e)) await ctx.shielded(() => named("cleanup", "cleaned"));
    throw e;
  }
});

wf("conf_cancel_timer", async (ctx) => {
  const timer = ctx.sleep(HOUR_MS);
  ctx.cancel(timer);
  if (await loserCancelled(timer)) return { cancelled_op: true };
  throw unexpected("cancelled timer wait", "completed");
});

// --- versioning (v1 bodies; v2 in conformance/guests/ts-v2) -----------------------

wf("conf_patch", async (ctx) => {
  const a = await named("a", "a");
  await ctx.sleep(1000);
  const b = await named("b", "b");
  return [a, b];
});

wf("conf_ndet", async (ctx) => {
  const a = await named("a", "a");
  await ctx.sleep(1000);
  const b = await named("b", "b");
  return [a, b];
});

// --- randomness and values -------------------------------------------------------

wf("conf_rand", async (ctx) => {
  const u64: string[] = [];
  for (let i = 0; i < 3; i++) u64.push(ctx.randomU64().toString());
  return { u64, uuid: ctx.uuid7() };
});

function hex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += (b < 16 ? "0" : "") + b.toString(16);
  return s;
}

wf("conf_rand_native", async () => {
  // The native APIs: crypto.getRandomValues is host random-bytes and Math.random is one
  // random-u64 draw, (u >> 11) * 2^-53, so the values equal Rust's random_bytes/random_f64.
  const bytes = hex(crypto.getRandomValues(new Uint8Array(8)));
  const f64 = Math.random();
  return { bytes, f64 };
});

wf("conf_values", async (ctx) => {
  // The order is part of the contract (PF-W9).
  const random_u64 = ctx.randomU64().toString();
  const random_f64 = ctx.randomF64();
  const uuid_v7 = ctx.uuid7();
  const idempotency_key = ctx.idempotencyKey("x");
  return { random_u64, random_f64, uuid_v7, idempotency_key };
});

// --- continue-as-new, tokens, status ---------------------------------------------

interface CanInput {
  n: number;
  generation?: number;
}

wf<CanInput>("conf_can", async (ctx, input) => {
  const generation = input.generation ?? 0;
  if (input.n === 0) return { generation };
  return ctx.continueAsNew({ n: input.n - 1, generation: generation + 1 });
});

wf("conf_token", async (ctx) => {
  const t = ctx.createAwaitToken();
  if (t.token === "") throw unexpected("token", "empty");
  const token_payload = await t.op;
  return { token_payload };
});

wf("conf_status", async (ctx) => {
  ctx.setStatus({ stage: "one" });
  ctx.setStatus({ stage: "two", n: 2 });
  return { done: true };
});

// --- sagas (same shapes as guests/sdk-saga) --------------------------------------

async function bookTwo(ctx: Ctx): Promise<ReturnType<Ctx["saga"]>> {
  const saga = ctx.saga();
  await named("book_a", "a");
  saga.add("cancel_a", () => named("cancel_a", "a"));
  await named("book_b", "b");
  saga.add("cancel_b", () => named("cancel_b", "b"));
  return saga;
}

wf("conf_saga_three", async (ctx) => {
  const saga = await bookTwo(ctx);
  let v: string;
  try {
    v = await step("book_c", (): string => {
      throw new NonRetryableError("book_c_failed", "no rooms left");
    });
  } catch (e) {
    return saga.failWith(e);
  }
  throw unexpected("book_c succeeded", v);
});

wf("conf_saga_cancel", async (ctx) => {
  const saga = await bookTwo(ctx);
  try {
    await ctx.sleep(HOUR_MS);
  } catch (e) {
    return saga.failWith(e);
  }
  return { slept: true };
});

// --- server leg (plan 09-13 contract) --------------------------------------------

/** Input of the `conf_parent_*` workflows: the child's full (prefixed) workflow name. */
interface ParentInput {
  child: string;
  input?: unknown;
  run_timeout_ms?: number;
  delay_ms?: number;
}

function childOpts(input: ParentInput, extra?: ChildOpts): ChildOpts {
  const o: ChildOpts = { ...extra };
  if (typeof input.run_timeout_ms === "number") o.runTimeoutMs = input.run_timeout_ms;
  return o;
}

function startChild(ctx: Ctx, input: ParentInput): Op<unknown> {
  return ctx.startChild(input.child, input.input ?? null, childOpts(input));
}

async function childOrCaught(op: PromiseLike<unknown>): Promise<Obj> {
  try {
    return { child: await op };
  } catch (e) {
    if (e instanceof OstroError) return { caught: e.kind };
    throw e;
  }
}

wf<ParentInput>("conf_parent_attached", async (ctx, input) => childOrCaught(startChild(ctx, input)));

wf<ParentInput>("conf_parent_fail_child", async (ctx, input) => childOrCaught(startChild(ctx, input)));

wf<ParentInput>("conf_parent_detached", async (ctx, input) => {
  const run = (await ctx.startChild(input.child, input.input ?? null, childOpts(input, { detached: true }))) as ChildRun;
  return { run_id: run.runId };
});

wf<ParentInput>("conf_parent_cancel_child", async (ctx, input) => {
  const op = startChild(ctx, input);
  await ctx.sleep(input.delay_ms ?? 1000);
  ctx.cancel(op);
  return childOrCaught(op);
});

wf<ParentInput>("conf_parent_close", async (ctx, input) => {
  startChild(ctx, input);
  await ctx.sleep(input.delay_ms ?? 1000);
  return { started: true };
});

wf<{ next?: string[] }>("conf_parent_chain", async (ctx, input) => {
  const next = input?.next ?? [];
  if (next.length === 0) return { leaf: true };
  const child = await ctx.startChild(next[0], { next: next.slice(1) });
  return { child };
});

wf<ParentInput>("conf_parent_shielded_child", async (ctx, input) => {
  // W2 / C-CHILD-11: the await is shielded (the child op is created through `s`), so the
  // run's cancellation does not reject it; the cancel cascade still cancels the child,
  // whose cancelled outcome then rejects the shielded await with CancelledError.
  return ctx.shielded(async (s) => {
    const op = s.startChild(input.child, input.input ?? null, childOpts(input));
    try {
      return { first: await op };
    } catch (e) {
      if (!isCancelled(e)) throw e;
    }
    s.startChild(input.child, {}, childOpts(input));
    await s.sleep(1000);
    return { first: "cancelled", cleanup_child: true };
  });
});

wf("conf_leaf_echo", async (ctx, input) => {
  const ms = asObj(input).delay_ms;
  if (typeof ms === "number") await ctx.shielded((s) => s.sleep(ms));
  return { echo: input };
});

wf("conf_leaf_fail", async () => {
  throw new NonRetryableError("boom", "conf_leaf_fail always fails");
});

wf("conf_leaf_sleep", async (ctx, input) => {
  const ms = asObj(input).ms;
  try {
    await ctx.sleep(typeof ms === "number" ? ms : HOUR_MS);
    return { slept: true };
  } catch (e) {
    if (isCancelled(e)) await ctx.shielded(() => named("cleanup", "cleaned"));
    throw e;
  }
});

wf<CanInput>("conf_leaf_can", async (ctx, input) => {
  const generation = input.generation ?? 0;
  if (input.n === 0) return { done: true, generation };
  return ctx.continueAsNew({ n: input.n - 1, generation: generation + 1 });
});

wf("conf_sig_api", async (ctx) => {
  const approved = await ctx.waitSignal("approve");
  return { approved };
});

wf("conf_token_api", async (ctx) => {
  const t = ctx.createAwaitToken();
  ctx.setStatus({ token: t.token });
  const token_payload = await t.op;
  return { token_payload };
});

/** `POST url` with JSON body `{}` and idempotency key `ctx.idempotencyKey(keyName)`. */
async function pay(ctx: Ctx, url: string, keyName: string): Promise<Obj> {
  const resp = await ctx.http.post(url).idempotencyKey(ctx.idempotencyKey(keyName)).json({}).send();
  let body: unknown = null;
  try {
    body = JSON.parse(decodeUtf8(resp.body));
  } catch {
    body = null;
  }
  return { status: resp.status, body };
}

wf<{ url: string }>("conf_fx_http", async (ctx, input) => pay(ctx, input.url, "pay"));

wf<{ url: string }>("conf_saga_http", async (ctx, input) => {
  const url = `${input.url}/pay`;
  const saga = ctx.saga();
  await pay(ctx, url, "a");
  saga.add("refund_a", (s) => pay(s, url, "refund-a"));
  await pay(ctx, url, "b");
  saga.add("refund_b", (s) => pay(s, url, "refund-b"));
  let v: string;
  try {
    v = await step("book_c", (): string => {
      throw new NonRetryableError("book_c_failed", "no rooms left");
    });
  } catch (e) {
    return saga.failWith(e);
  }
  throw unexpected("book_c succeeded", v);
});
