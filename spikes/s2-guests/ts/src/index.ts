// Spike S2 TypeScript guest (jco 1.42.0 / componentize-js 0.23.0, --disable all). Throwaway.
import { beginStep, beginStepU64, completeU64 } from 'ostro:spike/journal@0.1.0';
import { randomU64 } from 'ostro:spike/context@0.1.0';

export function run(input: string): string {
  const start = beginStep("greet");
  if (start.tag === "replayed") return start.val;
  const step = start.val;
  const out = "hello " + input;
  const resourceSeq = step.seq();
  step.complete(out);
  const h = beginStepU64("greet-u64");
  completeU64(h, out);
  return JSON.stringify({ out, resource_seq: resourceSeq.toString(), u64_seq: h.toString() });
}

export function probeRandom(): string {
  return JSON.stringify([Math.random(), Math.random(), Math.random()]);
}

export function probeTime(): string {
  return JSON.stringify(Date.now());
}

export function probeHash(): string {
  // Mixed numeric and string keys: integer-like keys are ordered first.
  const o: Record<string, number> = { b: 1, "10": 2, a: 3, "2": 4, c: 5 };
  return JSON.stringify(Object.keys(o));
}

export function probeUuid(): string {
  const a = new Uint8Array(8);
  crypto.getRandomValues(a);
  return JSON.stringify(Array.from(a));
}

// splitmix64 over BigInt, reduced to a [0,1) double.
function splitmix(seed: bigint): () => number {
  let s = BigInt.asUintN(64, seed);
  return () => {
    s = BigInt.asUintN(64, s + 0x9e3779b97f4a7c15n);
    let z = s;
    z = BigInt.asUintN(64, (z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n);
    z = BigInt.asUintN(64, (z ^ (z >> 27n)) * 0x94d049bb133111ebn);
    z = z ^ (z >> 31n);
    return Number(z >> 11n) / 2 ** 53;
  };
}

export function runReseeded(seedFromHost: boolean): string {
  let seed: string | null = null;
  if (seedFromHost) {
    const n = randomU64();
    seed = n.toString();
    Math.random = splitmix(n);
  }
  return JSON.stringify({ seed, draws: [Math.random(), Math.random(), Math.random()] });
}

export function runDroppedStep(): string {
  const start = beginStep("dropped");
  if (start.tag === "replayed") return JSON.stringify({ replayed: start.val });
  const step = start.val;
  const seq = step.seq();
  // Deliberately no complete(). Release the handle explicitly if the
  // binding exposes Symbol.dispose; otherwise it is left to the GC.
  const disp = (step as any)[(Symbol as any).dispose ?? Symbol.for("dispose")];
  let disposed = false;
  if (typeof disp === "function") { disp.call(step); disposed = true; }
  return JSON.stringify({ dropped_seq: seq.toString(), explicit_dispose: disposed });
}
