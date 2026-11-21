// Ostro TypeScript SDK (WIT ostro:runtime@0.1.0).
//
// Guest entry (jco needs the `workflows` export on the entry module):
//
//   import { workflow, step } from "ostro";
//   export { workflows } from "ostro";
//
//   workflow({ name: "hello" }, async (ctx, input) => {
//     const greeting = await step("greet", () => `hello, ${input.name}`);
//     await ctx.sleep(1000);
//     return { greeting };
//   });

import { bindingsHost } from "./bindings.ts";
import { setGuestHost } from "./export.ts";

export const VERSION = "0.1.0";

export { workflow, workflows } from "./export.ts";
export type { RunResult, WorkflowDefinition, WorkflowFn, WorkflowMeta, WorkflowOpts } from "./export.ts";
export { RetryPolicy, step, stepOpen } from "./step.ts";
export type { RetryPolicyInit, StepOpts } from "./step.ts";
export { ContinueAsNew, Ctx, f64FromU64 } from "./ctx.ts";
export type { AwaitTokenHandle, ChildOpts, ContinueAsNewOpts, TimeoutOpts, WorkflowTarget } from "./ctx.ts";
export { ChildRun, Op } from "./ops.ts";
export {
  CANCELLED,
  CHILD_TERMINATED,
  CancelledError,
  DeadlockError,
  INVALID_JSON,
  INVALID_STRING,
  InvalidStringError,
  JS_EXCEPTION,
  NonRetryableError,
  NotSupportedError,
  OstroError,
  ProtocolError,
  RetryableError,
  SDK_PROTOCOL,
  TIMEOUT,
  TimeoutError,
  UNKNOWN_WORKFLOW,
} from "./errors.ts";
export { SdkPromise } from "./promise.ts";
export { CANCELLATION_OP } from "./host.ts";
export type { Host, LogLevel, Payload, RunInfo } from "./host.ts";
export { decodeUtf8, encodeUtf8 } from "./utf8.ts";
export { sha256, sha256Hex } from "./sha256.ts";
// Targets of the bundle config's `transform.inject` (rolldown.ostro.mjs); not user API.
export {
  ostroConsole as __ostroConsole,
  sdkClearTimeout as __ostroClearTimeout,
  sdkClearTimeout as __ostroClearInterval,
  sdkSetInterval as __ostroSetInterval,
  sdkSetTimeout as __ostroSetTimeout,
} from "./determinism.ts";
export { sdkQueueMicrotask as __ostroQueueMicrotask } from "./promise.ts";

setGuestHost(bindingsHost);
