// The only SDK module that imports the jco bindings of `ostro:runtime@0.1.0`, adapting
// them to the `Host` interface. index.ts wires it as the guest host, which `run`
// installs when no other host is set. Under Node the host tests map these specifiers to
// throwing stubs (test/index.js) and always install a FakeHost instead.
//
// jco shapes: WIT u64 → bigint, list<u8> → Uint8Array, records → camelCase objects,
// variants → { tag, val }, option → undefined, `journal.step` → a resource object with
// `seq()` and `complete(outcome)`.

import {
  continueAsNewSuggested,
  info,
  isReplaying,
  now,
  patched,
  randomBytes,
  randomU64,
  setStatus,
  uuidV7,
} from "ostro:runtime/context@0.1.0";
import {
  cancelOp,
  cancellation,
  createAwaitToken,
  httpFetch,
  objectStore,
  sleep,
  sleepUntil,
  sqlQuery,
  startChild,
  wait,
  waitSignal,
} from "ostro:runtime/effects@0.1.0";
import { beginStep } from "ostro:runtime/journal@0.1.0";
import { emit } from "ostro:runtime/log@0.1.0";

import type { CompletionAck, Host, Outcome, Payload, StepHandle } from "./host.ts";

type JcoStep = StepHandle & { seq(): bigint; complete(outcome: Outcome<Payload>): CompletionAck };

/** `Host` over the jco-generated imports. */
export const bindingsHost: Host = {
  info: () => info(),
  now: () => now(),
  randomU64: () => randomU64(),
  randomBytes: (len) => randomBytes(len),
  uuidV7: () => uuidV7(),
  isReplaying: () => isReplaying(),
  patched: (changeId) => patched(changeId),
  setStatus: (json) => setStatus(json),
  continueAsNewSuggested: () => continueAsNewSuggested(),

  beginStep: (name, opts) => beginStep(name, opts),
  stepSeq: (step) => (step as JcoStep).seq(),
  stepComplete: (step, outcome) => (step as JcoStep).complete(outcome),

  httpFetch: (req) => httpFetch(req),
  sqlQuery: (req) => sqlQuery(req),
  objectStore: (req) => objectStore(req),
  sleep: (durationMs) => sleep(durationMs),
  sleepUntil: (at) => sleepUntil(at),
  waitSignal: (name, timeoutMs) => waitSignal(name, timeoutMs),
  startChild: (workflow, input, opts) => startChild(workflow, input, opts),
  createAwaitToken: (timeoutMs) => createAwaitToken(timeoutMs),
  cancellation: () => cancellation(),
  cancelOp: (op) => cancelOp(op),
  wait: (ops, mode) => wait(ops, mode),

  logEmit: (level, message, fields) => emit(level, message, fields),
};
