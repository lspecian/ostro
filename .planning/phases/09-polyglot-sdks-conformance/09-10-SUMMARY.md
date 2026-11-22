---
phase: 09-polyglot-sdks-conformance
plan: 10
subsystem: ts-sdk
tags: [typescript, sdk, sdkpromise, jco, quickjs, starlingmonkey, determinism, steps, cancellation, wf-07]
requires:
  - phase: 09-polyglot-sdks-conformance
    provides: "09-02 build-guest.sh ts|ts-sm, pinned npm toolchain, ostro-conformance inspect/polyglot, SDK WIT copy"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-03 TS Host interface (bigint op ids), setHost/host, FakeHost, node --test scaffold"
  - phase: 09-polyglot-sdks-conformance
    provides: "09-04 S9: result-mapping rule, es2016 lowering coverage, SdkPromise prototype on QJS+SM, isWellFormed on QJS"
provides:
  - "sdks/ts/src: SdkPromise + job queue, synchronous drive loop over effects.wait, Op/ChildRun, errors, JSON/UTF-8/sha256, Ctx, step/RetryPolicy, determinism surface, jco workflows export, bindings (ostro:runtime/* adapter)"
  - "76 host tests under Node 24 (FakeHost): A+/native ordering parity, B1 exclusivity live == replay, D11 shielding, closed-after-settle, timers, WF-07"
  - "sdks/ts/examples/hello + crates/ostro-conformance/tests/ts_sdk_build.rs (QuickJS + StarlingMonkey build/inspect + AST lowering check)"
affects: [09-12, 09-14, 09-16, 09-17, 09-20]
tech-stack:
  added: []
  patterns:
    - "SDK-owned promise installed as globalThis.Promise at module evaluation; sync `run` drains the job queue, then one effects.wait(sorted bigint ops [+ CANCELLATION], any)"
    - "step exclusivity by swapping the job queue: jobs queued before the step are set aside and restored at the front in order; `done` always resolved by an enqueued job"
    - "bundle config `transform.inject` swaps jco's console/timer/queueMicrotask shims for the SDK's deterministic ones"
key-files:
  created:
    - sdks/ts/src/promise.ts
    - sdks/ts/src/loop.ts
    - sdks/ts/src/ops.ts
    - sdks/ts/src/errors.ts
    - sdks/ts/src/json.ts
    - sdks/ts/src/utf8.ts
    - sdks/ts/src/sha256.ts
    - sdks/ts/src/ctx.ts
    - sdks/ts/src/step.ts
    - sdks/ts/src/determinism.ts
    - sdks/ts/src/export.ts
    - sdks/ts/src/bindings.ts
    - sdks/ts/test/promise.test.ts
    - sdks/ts/test/loop.test.ts
    - sdks/ts/test/encoding.test.ts
    - sdks/ts/test/step_ctx.test.ts
    - sdks/ts/test/export.test.ts
    - sdks/ts/examples/hello/index.ts
    - crates/ostro-conformance/tests/ts_sdk_build.rs
  modified:
    - sdks/ts/src/index.ts
    - sdks/ts/src/host.ts
    - sdks/ts/test/helpers.ts
    - sdks/ts/test/index.js
    - sdks/ts/rolldown.ostro.mjs
    - sdks/scripts/toolchain/rolldown.ostro.mjs
key-decisions:
  - "Guest entry pattern: `import { workflow, step } from \"ostro\"; export { workflows } from \"ostro\";` (jco takes exports from the entry module)"
  - "run result mapping (S9): return the plain run-result; throw exactly the WIT failure variant; everything is caught in `run` so nothing else escapes (which would trap)"
  - "Globals (SdkPromise, determinism surface) are installed at module evaluation only when not under Node; host tests install/uninstall explicitly"
  - "rolldown.ostro.mjs injects the SDK's console/setTimeout/clearTimeout/setInterval/clearInterval/queueMicrotask for free globals, replacing jco's shims (otherwise QJS gains a jco:node/console import)"
  - "Ctx exposes camelCase methods plus snake_case aliases (random_u64, random_f64, random_bytes) for cross-SDK naming"
requirements-completed: []
duration: ~2 sessions (interrupted by a reboot after Task 2)
completed: 2026-11-22
---

# Phase 9 Plan 10: TypeScript SDK core Summary

**The TypeScript workflow SDK: an SDK-owned `SdkPromise` (installed as `globalThis.Promise`) over a synchronously drained job queue, a sync `run` export that drives `effects.wait` with bigint op ids, `workflow()`/`step()`/`Ctx` with serialised exclusive steps that interleave identically live and on replay, D11 shielding, the WF-07 determinism surface and the jco `workflows` export. 76 host tests pass on Node 24. The SDK-built hello guest builds on QuickJS (1.998 MB, imports only `ostro:runtime/*@0.1.0`, exports only `workflows`) and StarlingMonkey (12.72 MB, also `ostro:runtime/*` only), and the bundle has zero unlowered async code.**

## Commits
- `b53cf5e` feat(09-10): TS SdkPromise, job queue, drive loop, ops, errors, encodings (11 files)
- `da3d2c5` feat(09-10): TS ctx, steps, shielding, determinism, export and bindings (13 files)
- `cdf425f` feat(09-10): TS SDK hello example guest and QuickJS/StarlingMonkey build/inspect test (2 files)

## Public API (`import ... from "ostro"`)
| Name | What |
|------|------|
| `workflow(opts \| name, fn)` | `opts = {name, version?, queue?, runTimeoutMs?}`; `fn(ctx, input)` (usually async). Duplicate names throw at module evaluation. Returns a frozen `WorkflowDefinition`. |
| `step(name, fn, opts?)` | Durable step; `opts = {retry?, timeoutMs?, fuel?}`. Also `ctx.step(...)`. Returns an SDK promise. |
| `RetryPolicy({maxAttempts, initialIntervalMs, maxIntervalMs, backoffCoefficient, nonRetryableKinds})` | Unset fields default to 3 / 1000 / 60000 / 2.0 (Rust partial defaults). `retry: null`/absent = no host retries. |
| `Ctx` | Reads: `info()`, `now()` (Date), `nowMs()`, `randomU64()` (bigint), `randomF64()`, `randomBytes(n)`, `uuid7()`, `isReplaying()`, `continueAsNewSuggested()`, `log(level, msg, fields?)`; snake aliases `random_u64/random_f64/random_bytes`.<br>Journaled: `patched(id)`, `setStatus(json)`, `continueAsNew(input, {workflow?, version?})`.<br>Effects returning `Op`: `sleep(ms)`, `sleepUntil(Date \| epochMs)`, `waitSignal(name, {timeoutMs?})`, `startChild(workflow, input, {queue, version, detached, runTimeoutMs})` (detached → `ChildRun`), `createAwaitToken({timeoutMs?}) → {op, token}`, `cancel(op)`, `cancellation()`, `shielded(async (s) => ...)` (explicit shielded ctx; no AsyncContext on QJS/SM). |
| `Op`, `ChildRun` | Thenable with `id: bigint` (never Number). Ids allocated eagerly; `then` registers interest. |
| Errors | `OstroError` → `NonRetryableError`, `RetryableError`, `TimeoutError`, `CancelledError`, `ProtocolError`/`DeadlockError` (`sdk_protocol`), `InvalidStringError` (`invalid_string`), `NotSupportedError`; kind constants exported. `toFailure`/`fromFailure` round-trip every WIT failure variant. |
| Misc | `SdkPromise`, `CANCELLATION_OP`, `encodeUtf8/decodeUtf8`, `sha256/sha256Hex`, `f64FromU64`, `VERSION`, types `Host`, `Payload`, `RunInfo`, `LogLevel`. `__ostro*` exports are bundle-inject targets, not user API. |

**Guest entry pattern for jco** (recorded in `index.ts`/`export.ts`):
```ts
import { workflow, step } from "ostro";
export { workflows } from "ostro";
workflow({ name: "hello" }, async (ctx, input) => { ... });
```
jco takes the component's exports from the entry module, so the entry re-exports `workflows`. `index.ts` wires `bindings.ts` (the only module importing `ostro:runtime/{context,journal,effects,log}@0.1.0`) as the guest host.

**`run` result mapping (S9 rule, measured on QJS and SM):**

| Outcome | WIT |
|---------|-----|
| returned value | return `{tag: "completed", val: application/json}` |
| `ctx.continueAsNew` | return `{tag: "continue-as-new", ...}` |
| `CancelledError` / `TimeoutError` / other `OstroError` | throw the WIT `failure` variant (`cancelled` / `timeout` / own kind) |
| any other exception | throw non-retryable `js_exception` |
| unknown workflow | throw non-retryable `unknown_workflow` |

Everything is caught in `run`; anything else escaping the export would trap the activation. Inside step bodies a non-`OstroError` becomes **retryable** `js_exception`; `retry-now` re-runs the body; the returned value is decoded from the journaled JSON.

## Step exclusivity (B1) as built
- Steps take an SDK FIFO lock; `begin_step` is issued from a front job so the caller's synchronous continuation never runs with a step open.
- **Replayed:** `enqueueJob(() => resolveDone(v))` right after `begin_step`.
- **Execute:** the currently queued jobs are set aside; the drain runs only jobs enqueued after the step opened until the body settles; the set-aside jobs are restored at the front in original order, then `enqueueJob(() => resolveDone(v))`.
- Quiescence with a step open → step completes non-retryable `sdk_protocol`.
- `setTimeout` inside an open step throws `ProtocolError`; outside, it is one durable sleep op and `clearTimeout` is `cancel-op`; `setInterval` throws `NotSupportedError`.
- Host tests prove: sibling RNG/sleep only after `step_complete`; siblings A, B run in order before the caller with live == replay call lists (apart from `step_complete`); a ≥2-hop sibling gives equal marker/value traces live and on replay.

## Example guest (Task 3)
`ts_sdk_build` (`--features polyglot`), `sdks/ts/examples/hello` (step `greet`, `Promise.all` over two sleeps, `ctx.random_f64()`):

| Backend | Bytes | Core instances | Core modules | Imports | Exports |
|---------|-------|----------------|--------------|---------|---------|
| QuickJS (`ts`) | 1,998,372 | 53 | 11 | `ostro:runtime/{types,context,journal,effects,log}@0.1.0` | `ostro:runtime/workflows@0.1.0` only |
| StarlingMonkey (`ts-sm`) | 12,724,324 | 8 | 3 | same five | `ostro:runtime/workflows@0.1.0` |

Lowering check: Rolldown 1.3.3 from `sdks/scripts/toolchain` with `sdks/ts/rolldown.ostro.mjs` merged (`ostro:*` external, staging identical to `build-guest.sh`), output `target/ts-bundle-check/bundle.js` (55,402 bytes), parsed with the toolchain's acorn: **0 async functions, 0 await expressions, 0 for-await, 0 `async function`/`async (` tokens**, no Rolldown diagnostics. (An AST check instead of a raw grep, because SDK comments and strings legitimately contain the word "await".)

**The first real-engine runs of SDK-built TS guests are 09-12 Task 3.** No engine run was done here, so the engine bug "step begun then guest returns → replay NonDeterminism" (being fixed under 09-07) was not exercised by any 09-10 test.

## Test results
- `node --test sdks/ts/test/`: **# pass 76, # fail 0** (Node 24.13.0). Files: promise 14, loop 13, encoding 7, step_ctx 26, export 7, scaffold (09-03) 9. `conformance/vectors/json.json` loaded and passing.
- `cargo test -p ostro-conformance --features polyglot --locked --test ts_sdk_build -- --nocapture --test-threads=1`: `test result: ok. 3 passed`.
- `cmp wit/ostro-runtime-0.1.0/runtime.wit sdks/ts/wit/runtime.wit`: identical.
- `cargo clippy -p ostro-conformance --features polyglot --locked --tests -- -D warnings`: clean; `rustfmt --check` on the new test: clean.

## Deviations from Plan
1. **[Rule 3] Files outside `files_modified` (Task 2, `da3d2c5`):** `sdks/ts/rolldown.ostro.mjs` and its byte-identical copy `sdks/scripts/toolchain/rolldown.ostro.mjs` gained `transform.inject` for `console`, `setTimeout`, `clearTimeout`, `setInterval`, `clearInterval`, `queueMicrotask`. Without it jco binds its own shims at module evaluation (before the SDK installs globals), and on QuickJS `console` adds a `jco:node/console` WIT import, breaking SDK-03's zero-non-ostro-import rule. `sdks/ts/test/index.js` gained a Node resolve hook mapping `ostro:runtime/*@0.1.0` to throwing stubs generated from the frozen WIT so `index.ts` (which imports `bindings.ts`) loads under Node. 09-17 owns packaging/build-config changes and should review.
2. **[Plan defect] `ostro:runtime/types@0.1.0` import.** As in 09-09, jco imports the type-only `types` interface; the test allows any `ostro:runtime/*@0.1.0` and requires context/journal/effects/log.
3. **Cosmetic build noise:** `build-guest.sh ts` prints Rolldown `UNRESOLVED_IMPORT` warnings for `ostro:runtime/*` (the shipped bundle config has no `external: [/^ostro:/]`, S9's had). Harmless (treated as external); left for 09-17 rather than editing the bundle config again.
4. **Differences from Rust semantics, with reasons:**
   - Ctx method names are camelCase (JS convention) with snake_case aliases for the random reads.
   - Uncaught step-body exceptions are retryable `js_exception`; workflow-level uncaught exceptions are non-retryable `js_exception` (Rust has no uncaught-exception case).
   - `shielded(fn)` takes an explicit shielded ctx `s`; ops must be created through `s` to be shielded (no AsyncContext on QJS/SM).
   - Cancellation is remembered once delivered (same as Python); the 09-11 projection normalises the missing re-issued `wait([op, CANCEL])`.
5. **Execution note:** the run was interrupted by a machine reboot after Task 2; Task 3 and this SUMMARY were completed by a resumed executor without redoing Tasks 1–2.

## Notes for later plans
- **09-12:** effect builders (http/sql/object), `idempotencyKey` (vectors in `conformance/vectors/idempotency.json`), saga; first real-engine runs of the hello guest on QJS and SM. `ctx.shielded` is the D-P9-18 hook.
- **09-14:** TS conformance guests use the entry pattern above.
- **09-17:** review deviation 1 (inject config) and 3 (`external`), sizes: QJS 2.0 MB, SM 12.7 MB.

## Self-Check: PASSED
- FOUND at main: every key-files path above.
- FOUND: b53cf5e, da3d2c5 and cdf425f in `git log main`.
