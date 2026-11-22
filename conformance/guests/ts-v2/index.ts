// TypeScript conformance workflows, v2 (plan 09-15).
//
// The v2 bodies of `conf_patch` and `conf_ndet` (contract: guests/conf-rust-v2). These are
// the only workflows the engine leg runs on the v2 module (C-PATCH-01 swaps a parked v1
// run onto it, C-PATCH-02 starts fresh on it, C-NDET-01 replays a v1 journal on it); a
// guest dir is built on its own, so the v1 bodies are not repeated here.
//
// - `conf_patch` v2: `ctx.patched("x") ? step a2 : step a`, sleep 1 s, step `b`. A v1
//   journal takes the old branch (no marker); a fresh run journals PatchMarker("x").
// - `conf_ndet` v2: steps reordered (`b`, sleep 1 s, `a`): a v1 journal is non-determinism.

import { step, workflow } from "ostro";
import type { Ctx } from "ostro";

export { workflows } from "ostro";

workflow({ name: "ts_conf_patch" }, async (ctx: Ctx) => {
  const first = ctx.patched("x") ? await step("a2", () => "a2") : await step("a", () => "a");
  await ctx.sleep(1000);
  const b = await step("b", () => "b");
  return [first, b];
});

workflow({ name: "ts_conf_ndet" }, async (ctx: Ctx) => {
  const b = await step("b", () => "b");
  await ctx.sleep(1000);
  const a = await step("a", () => "a");
  return [b, a];
});
