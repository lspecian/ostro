// Determinism surface (D-P9-15, SDK-1, SDK-4, WF-07), installed at module evaluation of
// the guest (pre-init) by export.ts. Nothing here calls the host at evaluation time
// (PF-W8): every host call happens inside a workflow activation.
//
// | Global                         | Behaviour                                                |
// |--------------------------------|----------------------------------------------------------|
// | Math.random                    | `(host random-u64 >> 11) * 2^-53` (step-local in steps)  |
// | crypto.getRandomValues         | host `random-bytes` (≤ 65536 bytes)                      |
// | crypto.randomUUID              | UUIDv4 from 16 host random bytes (like Python uuid4)     |
// | Date.now / new Date()          | logical time (`context.now`)                             |
// | performance.now                | logical time in ms (timeOrigin 0)                        |
// | setTimeout / clearTimeout      | durable sleep op / cancel-op; `sdk_protocol` in a step   |
// | setInterval                    | NotSupportedError                                        |
// | console.*                      | `log.emit`                                               |
// | WeakRef                        | strong reference (GC timing differs live vs replay)      |
// | FinalizationRegistry           | no-op                                                    |
//
// Outside an activation (module evaluation at build time) Date/performance fall back to
// the native clock and console output is dropped; randomness and timers throw.
//
// jco's bundler injects its own shims for the free globals `console`, `setTimeout`,
// `clearTimeout`, `setInterval`, `clearInterval` and `queueMicrotask`, bound at module
// evaluation (before the SDK installs anything; on QuickJS `console` even adds a
// `jco:node/console` WIT import). `rolldown.ostro.mjs` maps those injections to the SDK
// exports `__ostroConsole`, `__ostroSetTimeout`, ... (index.ts), which are the same
// objects installed here.

import { NotSupportedError, ProtocolError } from "./errors.ts";
import type { LogLevel } from "./host.ts";
import { Ctx, datetimeToMs, f64FromU64 } from "./ctx.ts";
import { currentActivation } from "./loop.ts";
import type { Activation } from "./loop.ts";
import type { Op } from "./ops.ts";
import { enqueueJob } from "./promise.ts";
import { stepOpen } from "./step.ts";
import { isWellFormed } from "./utf8.ts";

const NativeDate = Date;
const nativeDateNow = Date.now.bind(Date);

function liveActivation(what: string): Activation {
  const act = currentActivation();
  if (act === undefined) {
    throw new ProtocolError(`${what} outside a workflow run (module evaluation runs at build time, PF-W8)`);
  }
  return act;
}

/** Logical now in ms (native clock outside an activation). */
export function logicalNowMs(): number {
  const act = currentActivation();
  if (act === undefined) return nativeDateNow();
  return datetimeToMs(act.host.now());
}

// --- randomness -----------------------------------------------------------------------

function mathRandom(): number {
  return f64FromU64(liveActivation("Math.random()").host.randomU64());
}

type IntegerArray =
  | Int8Array
  | Uint8Array
  | Uint8ClampedArray
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | BigInt64Array
  | BigUint64Array;

function getRandomValues<T extends IntegerArray>(array: T): T {
  if (!ArrayBuffer.isView(array) || array instanceof DataView || array instanceof Float32Array || array instanceof Float64Array) {
    throw new TypeError("crypto.getRandomValues: argument must be an integer typed array");
  }
  if (array.byteLength > 65536) {
    throw new RangeError("crypto.getRandomValues: at most 65536 bytes per call");
  }
  const bytes = liveActivation("crypto.getRandomValues()").host.randomBytes(array.byteLength);
  new Uint8Array(array.buffer, array.byteOffset, array.byteLength).set(bytes);
  return array;
}

function hex2(b: number): string {
  return (b < 16 ? "0" : "") + b.toString(16);
}

function randomUUID(): string {
  const b = liveActivation("crypto.randomUUID()").host.randomBytes(16);
  b[6] = (b[6] & 0x0f) | 0x40;
  b[8] = (b[8] & 0x3f) | 0x80;
  let s = "";
  for (let i = 0; i < 16; i++) {
    if (i === 4 || i === 6 || i === 8 || i === 10) s += "-";
    s += hex2(b[i]);
  }
  return s;
}

// --- time -----------------------------------------------------------------------------

type DateCtor = DateConstructor & { new (...args: unknown[]): Date };

// A function constructor (not a class) so `Date()` without `new` keeps working. It shares
// `Date.prototype`, so `instanceof Date` and every Date method are unchanged; only the
// no-argument form reads the logical clock.
function makeDate(): DateCtor {
  function OstroDate(this: unknown, ...args: unknown[]): unknown {
    if (new.target === undefined) return new NativeDate(logicalNowMs()).toString();
    const ctorArgs = args.length === 0 ? [logicalNowMs()] : args;
    return Reflect.construct(NativeDate, ctorArgs, new.target);
  }
  OstroDate.prototype = NativeDate.prototype;
  const D = OstroDate as unknown as DateCtor & Record<string, unknown>;
  D.now = (): number => logicalNowMs();
  D.parse = NativeDate.parse;
  D.UTC = NativeDate.UTC;
  return D;
}

const ostroPerformance = {
  timeOrigin: 0,
  now(): number {
    return logicalNowMs();
  },
  toJSON(): { timeOrigin: number } {
    return { timeOrigin: 0 };
  },
};

// --- timers ---------------------------------------------------------------------------

interface Timer {
  op: Op<void> | undefined;
  ctx: Ctx | undefined;
  fired: boolean;
  cancelled: boolean;
}

let timerAct: Activation | undefined;
let timerSeq = 0;
let timers = new Map<number, Timer>();

function timerTable(act: Activation): Map<number, Timer> {
  if (timerAct !== act) {
    // Ids restart per activation, so live and replay hand out the same ids.
    timerAct = act;
    timerSeq = 0;
    timers = new Map();
  }
  return timers;
}

export function sdkSetTimeout(callback: unknown, delay?: unknown, ...args: unknown[]): number {
  if (typeof callback !== "function") throw new TypeError("setTimeout: the callback must be a function");
  const act = liveActivation("setTimeout");
  const table = timerTable(act);
  const n = Number(delay);
  const ms = Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
  timerSeq += 1;
  const id = timerSeq;
  const t: Timer = { op: undefined, ctx: undefined, fired: false, cancelled: false };
  const fire = (): void => {
    if (t.cancelled) return;
    t.fired = true;
    table.delete(id);
    (callback as (...a: unknown[]) => void)(...args);
  };
  if (ms === 0) {
    // A zero delay is a yield, not a durable timer (like asyncio.sleep(0)).
    table.set(id, t);
    enqueueJob(fire);
    return id;
  }
  if (stepOpen()) {
    throw new ProtocolError("setTimeout inside an open step (step bodies cannot start durable timers)");
  }
  t.ctx = new Ctx(act, false);
  t.op = t.ctx.sleep(ms);
  table.set(id, t);
  t.op.then(fire, () => {
    table.delete(id);
  });
  return id;
}

export function sdkClearTimeout(id: unknown): void {
  const act = currentActivation();
  if (act === undefined || act !== timerAct || typeof id !== "number") return;
  const t = timers.get(id);
  if (t === undefined || t.fired || t.cancelled) return;
  t.cancelled = true;
  timers.delete(id);
  if (t.op !== undefined && t.ctx !== undefined) {
    t.ctx.cancel(t.op);
    // Nobody awaits a cleared timer: keep it out of later wait lists.
    act.interested.delete(t.op.id);
  }
}

export function sdkSetInterval(): never {
  throw new NotSupportedError("setInterval is not supported in Ostro workflows (use a loop over ctx.sleep)");
}

// --- console --------------------------------------------------------------------------

function wellFormed(s: string): string {
  if (isWellFormed(s)) return s;
  const fix = (String.prototype as { toWellFormed?: () => string }).toWellFormed;
  if (typeof fix === "function") return fix.call(s);
  return s.replace(/[\uD800-\uDFFF]/g, "�");
}

function formatArg(v: unknown): string {
  if (typeof v === "string") return v;
  if (v instanceof Error) return v.stack !== undefined && v.stack !== "" ? `${v.name}: ${v.message}\n${v.stack}` : `${v.name}: ${v.message}`;
  if (typeof v === "bigint") return `${v.toString()}n`;
  if (v === undefined || typeof v === "function" || typeof v === "symbol") return String(v);
  try {
    const s = JSON.stringify(v, (_k, x: unknown) => (typeof x === "bigint" ? x.toString() : x));
    return s === undefined ? String(v) : s;
  } catch {
    return String(v);
  }
}

function emitter(level: LogLevel): (...args: unknown[]) => void {
  return (...args: unknown[]): void => {
    const act = currentActivation();
    if (act === undefined) return;
    act.host.logEmit(level, wellFormed(args.map(formatArg).join(" ")), []);
  };
}

function makeConsole(): Record<string, unknown> {
  const info = emitter("info");
  return {
    log: info,
    info,
    debug: emitter("debug"),
    trace: emitter("trace"),
    warn: emitter("warn"),
    error: emitter("error"),
    dir: info,
    assert: (cond: unknown, ...args: unknown[]): void => {
      if (!cond) emitter("error")("Assertion failed", ...args);
    },
  };
}

/** The SDK console (`log.emit`); also the bundle's injected `console`. */
export const ostroConsole: Record<string, unknown> = makeConsole();

// --- finalizers -----------------------------------------------------------------------

/** `WeakRef` replacement: a strong reference, so `deref()` never depends on GC timing. */
class StrongRef<T extends object> {
  _target: T;
  constructor(target: T) {
    if (target === null || (typeof target !== "object" && typeof target !== "function")) {
      throw new TypeError("WeakRef: target must be an object");
    }
    this._target = target;
  }
  deref(): T {
    return this._target;
  }
}

/** `FinalizationRegistry` replacement: callbacks never run (GC timing differs on replay). */
class NoopFinalizationRegistry {
  constructor(callback: unknown) {
    if (typeof callback !== "function") throw new TypeError("FinalizationRegistry: callback must be a function");
  }
  register(): void {}
  unregister(): boolean {
    return false;
  }
}

// --- install --------------------------------------------------------------------------

const PATCHED = [
  "Date",
  "performance",
  "crypto",
  "console",
  "setTimeout",
  "clearTimeout",
  "setInterval",
  "clearInterval",
  "WeakRef",
  "FinalizationRegistry",
] as const;

let saved: Map<string, PropertyDescriptor | undefined> | undefined;
let savedMathRandom: (() => number) | undefined;

function define(name: string, value: unknown): void {
  Object.defineProperty(globalThis, name, { value, writable: true, configurable: true, enumerable: false });
}

/** Install the determinism surface (idempotent). */
export function install(): void {
  if (saved !== undefined) return;
  saved = new Map();
  for (const name of PATCHED) saved.set(name, Object.getOwnPropertyDescriptor(globalThis, name));
  savedMathRandom = Math.random;
  Math.random = mathRandom;
  define("Date", makeDate());
  define("performance", ostroPerformance);
  define("crypto", { getRandomValues, randomUUID });
  define("console", ostroConsole);
  define("setTimeout", sdkSetTimeout);
  define("clearTimeout", sdkClearTimeout);
  define("setInterval", sdkSetInterval);
  define("clearInterval", sdkClearTimeout);
  define("WeakRef", StrongRef);
  define("FinalizationRegistry", NoopFinalizationRegistry);
}

/** Restore the native globals (host tests only). */
export function uninstall(): void {
  if (saved === undefined) return;
  for (const [name, desc] of saved) {
    if (desc === undefined) delete (globalThis as Record<string, unknown>)[name];
    else Object.defineProperty(globalThis, name, desc);
  }
  if (savedMathRandom !== undefined) Math.random = savedMathRandom;
  saved = undefined;
  savedMathRandom = undefined;
}
