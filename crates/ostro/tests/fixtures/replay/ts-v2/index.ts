// `ostro replay` fixture v2 (plan 10-11, CLI-07 SC3), mirroring guests/p10-replay-v2.
//
// v2: steps swapped with no patch, deliberately non-deterministic on a v1 journal.

import { step, workflow } from "ostro";
import type { Ctx } from "ostro";

export { workflows } from "ostro";

// `input.sleep_s` (default 1 s), in ms.
function sleepMs(input: unknown): number {
  const s = (input as { sleep_s?: unknown } | null)?.sleep_s;
  return (typeof s === "number" ? s : 1) * 1000;
}

workflow({ name: "ts_replay_demo", version: 2 }, async (ctx: Ctx, input: unknown) => {
  const steps = [await step("b", () => "b"), await step("a", () => "a")];
  await ctx.sleep(sleepMs(input));
  return steps;
});
