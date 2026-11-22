// Hello, Ostro: the smallest TypeScript workflow module built with the SDK.
//
// Build it with `sdks/scripts/build-guest.sh ts sdks/ts/examples/hello hello.wasm`
// (QuickJS, the default backend) or `... ts-sm ...` (StarlingMonkey). The QuickJS
// component imports only `ostro:runtime/*@0.1.0` and exports only
// `ostro:runtime/workflows@0.1.0`.
//
// jco takes the component's exports from the entry module, so the entry re-exports the
// SDK's `workflows` (the registry built by the `workflow(...)` calls below).

import { workflow, step } from "ostro";
import type { Ctx } from "ostro";

export { workflows } from "ostro";

interface HelloInput {
  name: string;
}

workflow({ name: "hello" }, async (ctx: Ctx, input: HelloInput) => {
  const greeting = await step("greet", () => `hello, ${input.name}`);
  // Two durable timers in parallel: one wait, both complete after 2 s.
  await Promise.all([ctx.sleep(1000), ctx.sleep(2000)]);
  return { greeting, lucky: ctx.random_f64() };
});
