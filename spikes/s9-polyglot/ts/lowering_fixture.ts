// Spike S9 lowering fixture (question 9). Throwaway.
//
// Exercises every async construct an SDK user or an npm dependency may write, bundled
// with jco's Rolldown and `transform.target = "es2016"`. run.sh stages this file next to
// probe.ts (the SdkPromise prototype) and a generated `./dep.ts`, in two variants:
// `dep.ts` with a top-level await (expected to fail: TLA cannot be lowered to a sync
// module evaluation) and without it. The bundle is dumped and grepped for leftover
// `async`/`await` tokens; the no-TLA build also runs on the real engine (`lowering`).

import { depValue } from "./dep.ts";
import { register, step } from "./probe.ts";

// async function
async function double(x: number): Promise<number> {
  return (await step("double", () => x * 2)) as number;
}

// async arrow
const triple = async (x: number): Promise<number> => (await step("triple", () => x * 3)) as number;

// async generator
async function* counter(n: number): AsyncGenerator<number> {
  for (let i = 0; i < n; i++) {
    yield (await step("gen-" + i, () => i)) as number;
  }
}

// async iterable consumed with for await
async function sumAll(n: number): Promise<number> {
  let s = 0;
  for await (const v of counter(n)) s += v;
  return s;
}

// class with async method
class Worker {
  base = 10;
  async run(): Promise<number> {
    return this.base + (await double(1));
  }
}

async function lowering(_input: unknown): Promise<unknown> {
  const d = await double(5);
  const t = await triple(5);
  const s = await sumAll(3);
  const w = await new Worker().run();
  const settled = await Promise.allSettled([Promise.resolve(1), Promise.reject(new Error("x"))]);
  const any = await Promise.any([Promise.reject(new Error("y")), Promise.resolve("any-ok")]);
  let caught = "";
  try {
    await (async () => {
      throw new Error("async throw");
    })();
  } catch (e) {
    caught = (e as Error).message;
  }
  return {
    d,
    t,
    s,
    w,
    settled: (settled as { status: string }[]).map((r) => r.status),
    any,
    caught,
    dep: depValue,
  };
}

register("lowering", lowering);
export { workflows } from "./probe.ts";
