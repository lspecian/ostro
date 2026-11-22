// The saga helper (WF-10, D-P9-18), identical in behaviour to the Rust SDK's `Saga`.
//
//   const saga = ctx.saga();               // or new Saga(ctx)
//   await step("book_a", bookA);
//   saga.add("cancel_a", (s) => step("cancel_a", cancelA));
//   await step("book_b", bookB);
//   saga.add("cancel_b", (s) => step("cancel_b", cancelB));
//   try {
//     await step("book_c", bookC);
//   } catch (e) {
//     await saga.failWith(e);              // runs cancel_b, then cancel_a; rethrows e
//   }
//
// `compensate()` runs the compensations sequentially, last registered first (LIFO), inside
// `ctx.shielded(...)`: each compensation receives the explicit shielded ctx `s` (no
// AsyncContext on QuickJS/StarlingMonkey), and ops created through `s` are not rejected by
// the run's cancellation, so a cancelled workflow still compensates. A failing
// compensation does not stop the others; every failure is reported. `failWith(e)`
// compensates and rethrows `e` with the report in its `details`:
//
//   {"compensated": ["cancel_b", "cancel_a"],
//    "compensation_failures": [{"compensation": "cancel_x",
//                               "error": {"kind": "...", "message": "...", "retryable": false}}]}
//
// Earlier details of the error are kept under "details". No `async` functions (see
// effects.ts). Running compensations in parallel is out of scope.

import { ContinueAsNew } from "./ctx.ts";
import type { Ctx } from "./ctx.ts";
import { JS_EXCEPTION, NonRetryableError, OstroError } from "./errors.ts";
import { SdkPromise } from "./promise.ts";
import { checkString } from "./utf8.ts";

/** A compensation: called with the shielded ctx; may return a value or a promise. */
export type Compensation = (s: Ctx) => unknown;

/** One compensation that failed during `compensate()`. */
export interface CompensationFailure {
  compensation: string;
  error: unknown;
}

function describe(e: unknown): string {
  if (e instanceof Error) return e.name && e.name !== "Error" ? `${e.name}: ${e.message}` : `Error: ${e.message}`;
  try {
    return `thrown value: ${String(e)}`;
  } catch {
    return "thrown value";
  }
}

function errorJson(e: unknown): { kind: string; message: string; retryable: boolean } {
  if (e instanceof OstroError) return { kind: e.kind, message: e.message, retryable: e.retryable };
  return { kind: JS_EXCEPTION, message: describe(e), retryable: false };
}

/** What `compensate()` did. */
export class SagaReport {
  /** Compensations that succeeded, in the order they ran (reverse registration order). */
  compensated: string[];
  /** Compensations that failed, in the order they ran. */
  failures: CompensationFailure[];

  constructor() {
    this.compensated = [];
    this.failures = [];
  }

  /** True if every compensation succeeded. */
  isClean(): boolean {
    return this.failures.length === 0;
  }

  /** `{"compensated": [..], "compensation_failures": [..]}`. */
  toJSON(): { compensated: string[]; compensation_failures: { compensation: string; error: ReturnType<typeof errorJson> }[] } {
    return {
      compensated: this.compensated.slice(),
      compensation_failures: this.failures.map((f) => ({ compensation: f.compensation, error: errorJson(f.error) })),
    };
  }
}

/** Compensations registered while a workflow makes progress. */
export class Saga {
  _ctx: Ctx;
  _comps: [string, Compensation][];

  constructor(ctx: Ctx) {
    this._ctx = ctx;
    this._comps = [];
  }

  /** Register compensation `name`; it runs before every compensation registered earlier. */
  add(name: string, compensation: Compensation): void {
    checkString(name, "compensation name");
    if (typeof compensation !== "function") throw new TypeError("compensation must be a function");
    this._comps.push([name, compensation]);
  }

  /** Number of registered compensations. */
  get length(): number {
    return this._comps.length;
  }

  /**
   * Run every compensation, last registered first, with the shielded ctx; a failure is
   * recorded and the remaining compensations still run. The saga is empty afterwards.
   */
  compensate(): SdkPromise<SagaReport> {
    const comps = this._comps.splice(0, this._comps.length).reverse();
    const report = new SagaReport();
    return this._ctx.shielded((s) => {
      const next = (i: number): SdkPromise<SagaReport> | SagaReport => {
        if (i >= comps.length) return report;
        const [name, fn] = comps[i];
        let r: SdkPromise<unknown>;
        try {
          r = SdkPromise.resolve(fn(s));
        } catch (e) {
          r = SdkPromise.reject(e);
        }
        return r
          .then(
            () => {
              report.compensated.push(name);
            },
            (e: unknown) => {
              report.failures.push({ compensation: name, error: e });
            },
          )
          .then(() => next(i + 1));
      };
      return SdkPromise.resolve().then(() => next(0));
    });
  }

  /**
   * Compensate, then reject with `error` carrying the report in its `details`. An
   * `OstroError` is rethrown itself (earlier details move under "details"); anything else
   * becomes a non-retryable `js_exception`. A continue-as-new is rethrown unchanged.
   */
  failWith(error: unknown): SdkPromise<never> {
    return this.compensate().then((report) => {
      if (error instanceof ContinueAsNew) throw error;
      const details: Record<string, unknown> = report.toJSON();
      if (error instanceof OstroError) {
        if (error.details !== undefined) details.details = error.details;
        error.details = details;
        throw error;
      }
      throw new NonRetryableError(JS_EXCEPTION, describe(error), details);
    });
  }
}
