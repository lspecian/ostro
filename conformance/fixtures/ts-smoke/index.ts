// SDK smoke guest (plan 09-12): the first real-engine runs of an SDK-built TypeScript
// module, on QuickJS (`ts`) and StarlingMonkey (`ts-sm`).
//
// Same workflow names and outputs as conformance/fixtures/py-smoke (see its docstring);
// crates/ostro-conformance/tests/sdk_engine_smoke.rs checks both.

import { CancelledError, OstroError, NonRetryableError, RetryPolicy, step, workflow } from "ostro";
import type { Ctx } from "ostro";

export { workflows } from "ostro";

workflow({ name: "smoke_steps" }, async (_ctx: Ctx) => {
  const first = await step("a", () => "a");
  const second = await step("b", () => "b");
  const [third, fourth] = await Promise.all([step("c", () => "c"), step("d", () => "d")]);
  return [first, second, third, fourth];
});

workflow({ name: "smoke_race" }, async (ctx: Ctx) => {
  const timer = ctx.sleep(3_600_000);
  const signal = ctx.waitSignal("go");
  const winner = await Promise.race([timer.then(() => ({ w: "timer" as const })), signal.then((p) => ({ w: "signal" as const, p }))]);
  if (winner.w !== "signal") return { winner: "timer" };
  ctx.cancel(timer);
  let loserCancelled = false;
  try {
    await timer;
  } catch (e) {
    loserCancelled = e instanceof CancelledError;
  }
  return { winner: "signal", payload: winner.p, loser_cancelled: loserCancelled };
});

workflow({ name: "smoke_sleep" }, async (ctx: Ctx) => {
  await ctx.sleep(3_600_000);
  return { slept: true };
});

workflow({ name: "smoke_child" }, async (ctx: Ctx) => {
  const out = await ctx.startChild("leaf_echo", { x: 1 });
  return { child: out };
});

workflow({ name: "smoke_saga" }, async (ctx: Ctx) => {
  const saga = ctx.saga();
  await step("book_a", () => "a");
  saga.add("cancel_a", () => step("cancel_a", () => null));
  await step("book_b", () => "b");
  saga.add("cancel_b", () => step("cancel_b", () => null));
  try {
    await step("book_c", () => {
      throw new NonRetryableError("book_c_failed", "no rooms left");
    });
  } catch (e) {
    if (e instanceof OstroError) await saga.failWith(e);
    throw e;
  }
  return { unexpected: true };
});

workflow({ name: "smoke_saga_cancel" }, async (ctx: Ctx) => {
  const saga = ctx.saga();
  await step("book_a", () => "a");
  saga.add("cancel_a", () => step("cancel_a", () => null));
  await step("book_b", () => "b");
  saga.add("cancel_b", () => step("cancel_b", () => null));
  try {
    await ctx.sleep(3_600_000);
  } catch (e) {
    if (e instanceof CancelledError) await saga.failWith(e);
    throw e;
  }
  return { unexpected: true };
});

workflow({ name: "smoke_rand" }, async (ctx: Ctx) => {
  return {
    f64: ctx.random_f64(),
    u64: ctx.randomU64().toString(),
    native: Math.random(),
    uuid: ctx.uuid7(),
    key: ctx.idempotencyKey("x"),
    time_ms: Date.now(),
  };
});

workflow({ name: "smoke_print" }, async (_ctx: Ctx) => {
  console.log("hello from the ts smoke guest");
  console.error("to stderr");
  return { printed: true };
});

workflow({ name: "smoke_effects" }, async (ctx: Ctx) => {
  const resp = await ctx.http
    .post("https://api.example.com/v1/charges")
    .secretHeader("Authorization", "Bearer {{secret:stripe}}")
    .json({ amount: 100 })
    .idempotencyKey(ctx.idempotencyKey("charge"))
    .timeout(10_000)
    .retry(new RetryPolicy({ maxAttempts: 2 }))
    .redact("Authorization")
    .send();
  const rows = await ctx.sql("db").exactlyOnce().query("UPDATE c SET n = n + $1 RETURNING n", [1, "t", null, 1.5]);
  const put = await ctx.object("files").put("k", new Uint8Array([98, 111, 100, 121]));
  let body = "";
  for (const x of resp.body) body += String.fromCharCode(x);
  return {
    status: resp.status,
    body,
    rows_affected: Number(rows.rowsAffected),
    columns: rows.columns,
    e_tag: put.eTag,
  };
});
