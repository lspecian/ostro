// Workflow registry and the jco export of `ostro:runtime/workflows@0.1.0`.
//
// Guest entry pattern (one entry module, everything through the package's single export):
//
//   import { workflow, step } from "ostro";
//   export { workflows } from "ostro";
//   workflow({ name: "hello" }, async (ctx, input) => { ... });
//
// At module evaluation (pre-init) this module installs SdkPromise as the global Promise
// and the determinism surface, so user modules evaluated afterwards already see them.
// Under Node (host tests) nothing is installed: tests drive the loop explicitly and
// install/uninstall the globals around the cases that need them.
//
// `run` result mapping (S9 question 7, measured on QuickJS and StarlingMonkey): return the
// plain `run-result` value on success; on failure throw exactly the WIT `failure` variant
// value. Anything else escaping the export traps the activation (AttemptFailed), so
// everything is caught here and converted.

import { ContinueAsNew, Ctx } from "./ctx.ts";
import { install } from "./determinism.ts";
import { UNKNOWN_WORKFLOW, toFailure } from "./errors.ts";
import type { Failure, Host, Payload } from "./host.ts";
import { hasHost, host, setHost } from "./host.ts";
import { fromPayload, toPayload } from "./json.ts";
import { beginActivation, driveToCompletion } from "./loop.ts";
import { SdkPromise, installGlobals } from "./promise.ts";
import { u32, u64 } from "./step.ts";
import { checkString } from "./utf8.ts";

/** A workflow function: `(ctx, input) => output` (usually async). */
export type WorkflowFn<I = unknown, O = unknown> = (ctx: Ctx, input: I) => O | PromiseLike<O>;

/** Registration options of a workflow. */
export interface WorkflowOpts {
  name: string;
  /** Workflow version (u32, default 1). */
  version?: number;
  /** Default task queue. */
  queue?: string;
  /** Default run timeout in ms. */
  runTimeoutMs?: number | bigint;
}

/** A registered workflow (pass it to `ctx.startChild` or use its `name`). */
export interface WorkflowDefinition<I = unknown, O = unknown> {
  readonly name: string;
  readonly version: number;
  readonly queue: string | undefined;
  readonly runTimeoutMs: bigint | undefined;
  readonly fn: WorkflowFn<I, O>;
}

/** WIT `workflows.workflow-meta`. */
export interface WorkflowMeta {
  name: string;
  version: number;
  defaultQueue: string | undefined;
  runTimeoutMs: bigint | undefined;
}

/** WIT `workflows.run-result`. */
export type RunResult =
  | { tag: "completed"; val: Payload }
  | { tag: "continue-as-new"; val: { input: Payload; workflow: string | undefined; version: number | undefined } };

const registry = new Map<string, WorkflowDefinition>();

/**
 * Register a workflow. Names are unique per module: a duplicate throws at module
 * evaluation (which fails the build).
 */
export function workflow<I = unknown, O = unknown>(opts: WorkflowOpts | string, fn: WorkflowFn<I, O>): WorkflowDefinition<I, O> {
  const o: WorkflowOpts = typeof opts === "string" ? { name: opts } : opts;
  if (o === null || typeof o !== "object") throw new TypeError("workflow: options must be an object or a name");
  const name = checkString(o.name, "workflow name");
  if (name === "") throw new TypeError("workflow: name must not be empty");
  if (typeof fn !== "function") throw new TypeError(`workflow ${JSON.stringify(name)}: fn must be a function`);
  if (registry.has(name)) throw new Error(`workflow ${JSON.stringify(name)} is registered twice (names must be unique)`);
  const def: WorkflowDefinition<I, O> = Object.freeze({
    name,
    version: o.version === undefined ? 1 : u32(o.version, "workflow version"),
    queue: o.queue === undefined ? undefined : checkString(o.queue, "workflow queue"),
    runTimeoutMs: o.runTimeoutMs === undefined ? undefined : u64(o.runTimeoutMs, "runTimeoutMs"),
    fn,
  });
  registry.set(name, def as WorkflowDefinition);
  return def;
}

/** Remove every registered workflow (host tests only). */
export function _clearRegistry(): void {
  registry.clear();
}

let guestHost: Host | undefined;

/** The host `run` installs when none is installed (index.ts wires the jco bindings). */
export function setGuestHost(h: Host | undefined): void {
  guestHost = h;
}

function describe(): WorkflowMeta[] {
  const names = Array.from(registry.keys()).sort();
  return names.map((n) => {
    const d = registry.get(n) as WorkflowDefinition;
    return { name: d.name, version: d.version, defaultQueue: d.queue, runTimeoutMs: d.runTimeoutMs };
  });
}

function unknownWorkflow(name: string): Failure {
  return {
    tag: "non-retryable",
    val: { kind: UNKNOWN_WORKFLOW, message: `this component does not export workflow ${JSON.stringify(name)}`, details: undefined },
  };
}

function activate(def: WorkflowDefinition, input: Payload): RunResult {
  if (!hasHost()) {
    if (guestHost === undefined) throw new Error("ostro: no host installed and no guest bindings");
    setHost(guestHost);
  }
  const act = beginActivation(host());
  try {
    const value = fromPayload(input);
    const ctx = new Ctx(act, false);
    const root = SdkPromise.resolve().then(() => def.fn(ctx, value));
    driveToCompletion(act, root);
    if (root.fulfilled) return { tag: "completed", val: toPayload(root.result) };
    const e = root.result;
    if (e instanceof ContinueAsNew) {
      return { tag: "continue-as-new", val: { input: e.input, workflow: e.workflow, version: e.version } };
    }
    throw e;
  } finally {
    act.closed = true;
  }
}

function run(name: string, input: Payload): RunResult {
  const def = registry.get(name);
  if (def === undefined) throw unknownWorkflow(name);
  try {
    return activate(def, input);
  } catch (e) {
    // Throw exactly the WIT failure variant (S9 result-mapping rule).
    throw toFailure(e, false);
  }
}

/** The `ostro:runtime/workflows@0.1.0` export, shaped for jco. */
export const workflows = { describe, run };

function isNodeHost(): boolean {
  const p = (globalThis as { process?: { versions?: { node?: unknown } } }).process;
  return p !== undefined && p.versions !== undefined && typeof p.versions.node === "string";
}

if (!isNodeHost()) {
  installGlobals();
  install();
}
