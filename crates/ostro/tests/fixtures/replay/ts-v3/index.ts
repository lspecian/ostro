// `ostro replay` fixture v3 (plan 10-11, CLI-07 SC3), mirroring guests/p10-replay-v3.
//
// v3: the swap behind `ctx.patched("reorder")`; v1 journals take the old order.

import { step, workflow } from "ostro";
import type { Ctx } from "ostro";

export { workflows } from "ostro";

// `input.sleep_s` (default 1 s), in ms.
function sleepMs(input: unknown): number {
  const s = (input as { sleep_s?: unknown } | null)?.sleep_s;
  return (typeof s === "number" ? s : 1) * 1000;
}

workflow({ name: "ts_replay_demo", version: 3 }, async (ctx: Ctx, input: unknown) => {
  const steps = ctx.patched("reorder")
    ? [await step("b", () => "b"), await step("a", () => "a")]
    : [await step("a", () => "a"), await step("b", () => "b")];
  await ctx.sleep(sleepMs(input));
  return steps;
});
