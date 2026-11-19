---
phase: 05-workflow-semantics-core-hardening
plan: 06
subsystem: sdk
tags: [rust-sdk, d11, cancellation, shielded, await-tokens, continue-as-new, journal-shape, readme]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-01 contract (journal kinds, statuses); 05-03 cancel-op -> Err(cancelled), first-completion-wins, signal/token waits never hot-park, patched memo"
provides:
  - "Rust SDK WIT 0.1 workflow surface complete: Ctx::create_await_token -> AwaitToken<T>, continue-as-new (WorkflowError::continue_as_new / Ctx::continue_as_new / ContinueAsNew builder), Ctx::shielded"
  - "D11 cancellation surfacing: Op::wait = wait([op, CANCELLATION], any); join! = any-rounds over pending + CANCELLATION; select! adds CANCELLATION and returns index N on cancel; no double wrap when the cancellation op is already waited; shielded waits use the plain list"
  - "ostro_sdk::{CANCELLATION, is_cancellation, ops::is_shielded}"
  - "SDK README: frozen Phase 5 semantics (D6, D7, FIFO signals, evicting signal/token waits, D11 + shielded, one-shot tokens, continue-as-new, PF-W1/W5/W9, attempt F4)"
  - "guests/sdk-cancel and crates/ostro-engine/tests/sdk_cancellation.rs (8 tests)"
affects: [05-09, 05-10, 05-12, 05-13, 05-15, phase-9]

tech-stack:
  added: []
  patterns:
    - "Continue-as-new travels through Err(WorkflowError) carrying a private ContinueAsNew; __run maps it to run-result::continue-as-new (Workflow trait unchanged, `?` short-circuits)"
    - "Shield depth is a thread-local counter restored by a drop guard (unwind-safe on host tests)"
    - "Engine proofs that depend on input order commit the inputs by hand (both orders in one batch) instead of using testkit scripting"

key-files:
  created:
    - guests/sdk-cancel/Cargo.toml
    - guests/sdk-cancel/src/lib.rs
    - crates/ostro-engine/tests/sdk_cancellation.rs
  modified:
    - sdks/rust/ostro-sdk/src/ctx.rs
    - sdks/rust/ostro-sdk/src/ops.rs
    - sdks/rust/ostro-sdk/src/registry.rs
    - sdks/rust/ostro-sdk/src/error.rs
    - sdks/rust/ostro-sdk/src/lib.rs
    - sdks/rust/README.md
    - crates/ostro-engine/tests/sdk_guests.rs
    - .planning/ROADMAP.md

key-decisions:
  - "Continue-as-new shape: `return Err(ctx.continue_as_new(&input))` (or `Err(ContinueAsNew::new(&i)?.workflow(w).version(v).into())`). WorkflowError gains a private `continuation` field (kind `continue_as_new`, never sent to the host as a failure). An unencodable input becomes a non-retryable invalid_json failure"
  - "The SDK uses the WIT-frozen constant CANCELLATION = u64::MAX instead of calling effects.cancellation() per wait (the host call is unjournaled and returns that value)"
  - "join! with D11 waits `any` in rounds over the still-pending ops + CANCELLATION; ops completed before the cancel keep their result, the rest resolve to Err(cancelled). A join can now journal more than one WaitResolved when completions arrive separately"
  - "select! keeps its `(usize, Completion)` signature: on cancel it returns index == argument count with the cancellation completion; Op::decode maps that completion to Err(cancelled) for any op"
  - "When cancellation wins, the SDK does not auto-cancel the still-open op (the guest can ctx.cancel it); this keeps the journal minimal"
  - "AwaitToken::op consumes the token handle (Op is not Clone); token() borrows; Debug never prints the token"
  - "REQUIREMENTS.md unchanged: WF-03/WF-05/API-07 still need server-side work (05-04/05-08/05-10/05-13)"

requirements-completed: []

duration: ~60min
completed: 2026-11-19
---

# Phase 5 Plan 06: Rust SDK Tokens, Continue-as-New and D11 Cancellation Summary

**The Rust SDK now covers the whole WIT 0.1 workflow surface. It adds typed await tokens and continue-as-new returned through `Err`. D11 cancellation surfacing is in: every SDK wait includes the run's cancellation op unless it runs inside `ctx.shielded`. An `sdk-cancel` guest proves first-completion-wins on the engine in both input orders. The journal-shape change needed no edits to existing assertions, and no snapshot was touched.**

## Performance

- **Duration:** about 60 min, including the final `ci-local --no-musl` run.
- **Completed:** 2026-11-19.
- **Tasks:** 2/2.
- **Files:** 3 created, 7 code/doc files modified, plus ROADMAP.md.

## Accomplishments

- **Await tokens.** `ctx.create_await_token::<T>(timeout) -> AwaitToken<T>`. `.token()` borrows the token string; `.op()` / `into_parts()` hand over the `Op<T>`. The completion payload is JSON-decoded (`OpOutput::Token`).
- **Continue-as-new.** `Err(ctx.continue_as_new(&input))`, or the `ContinueAsNew` builder for workflow and version. `registry::__run` maps it to `RunResult::ContinueAsNew(Continuation)`.
- **D11.**
  - `Op::wait` issues `wait([op, CANCELLATION], any)` and decodes the first completion. A cancellation completion gives `Err(StepError::cancelled())`.
  - `join!` waits `any` in rounds; ops still pending at the cancel resolve to `Err(cancelled)`.
  - `select!` returns index N (the argument count) on cancel.
  - A wait whose list already contains the cancellation op is not wrapped again. `ctx.shielded(|| …)` disables the wrapping, and sections nest.
- **README.** Rewritten Semantics section covering every item the plan lists.

## Task Commits

1. **Task 1: SDK await tokens, continue-as-new, D11 + shielded, README**: `b6cbdd9` (feat). Includes host unit tests: shield nesting and unwind, wait-list construction, take-first ordering, join rounds and partial cancel, select index N, token decode, continuation mapping.
2. **Task 2: sdk-cancel guest and engine-level D11 proof**: `764bda4` (test).

## D11 journal-shape change (applied deliberately)

What changed in SDK journals:
- Every unshielded SDK wait's **command hash** now covers `[ops…, CANCELLATION]` with mode `any`. Before, it covered `[op]` with mode `all` for `Op::wait` and `join!`, and `[ops…]` with `any` for `select!`.
- `WaitResolved.completed` lists `CANCELLATION` (u64::MAX) when the cancel had been journaled by the time the wait resolved.
- `join!` can journal several `WaitResolved` events, one per round, when completions arrive separately (live or hot-parked). With the testkit's fire-on-eviction scripting it is still one.
- Shielded waits keep the old shape: plain list, `all` for `Op::wait`/`join!`.

**Assertions updated: none were needed.** I audited every suite that runs SDK guests: engine `sdk_guests`, `patching` and `step_retry` (which uses retry-probe, not an SDK guest); ostro-server `sdk_e2e`, `executor`, `modules_api`, `runs_api` and `start_run_api`; ostro `e2e_dev`. None asserts on Wait command hashes or `WaitResolved` contents, and none cancels an SDK run. Counts of steps, timers and events per activation are unchanged.

05-03's `tests/patching.rs` assertion that a resumed v1 activation appends `[WaitResolved, StepCompleted, RunCompleted]` **still holds unchanged**. `Op::wait()` still journals exactly one `WaitResolved`, and only the hash and op list inside it differ. `sdk_guests.rs` gained a module-doc note recording this, plus `sdk-cancel` in the describe list. The det-basic golden and every file under `crates/ostro-engine/tests/snapshots` are untouched (`git diff --name-only` is empty).

Journals recorded by an SDK guest built **before** this change will diverge (NonDeterminism) on the new SDK at their first wait. This is acceptable pre-freeze: no replay-corpus entry exists yet (05-12 records after this plan).

## Verification

- `cargo test --manifest-path sdks/rust/Cargo.toml`: ostro-sdk has 27 unit tests and the macros crate 7 unit tests plus trybuild, all green.
- `cargo clippy --manifest-path sdks/rust/Cargo.toml --target wasm32-wasip2 -- -D warnings` is clean, the `--all-targets` host clippy is clean, and the wasm build is clean.
- `cargo test -p ostro-engine --test sdk_cancellation` (8 tests):
  - `cancel_before_timer_fired_is_cancelled`: `[CancelRequested, TimerFired]` in one batch gives `"cancelled"`, with `WaitResolved [CANCELLATION, timer]`.
  - `timer_fired_before_cancel_is_slept`: the reverse order gives `"slept"`.
  - `timer_alone_is_slept`.
  - `shielded_cleanup_runs_after_cancel`:
    - the work wait resolves on the cancel;
    - the unshielded 100 ms probe resolves at once with `[CANCELLATION]`;
    - the cleanup TimerStarted is journaled after CancelRequested;
    - the shielded wait resolves on `[cleanup]` only, and the run returns `"cleaned"`.
  - `select_loser_cancel_completes_with_cancelled`: the loser is completed by the guest's `OpCancelled`, and the waits are `[[short], [long]]`.
  - `await_token_completes_with_json_payload`.
  - `await_token_wait_observes_cancellation`: the run fails with `Failure::Cancelled`.
  - `continue_as_new_returns_the_continuation`: covers both the plain path and the builder path (workflow and version).

  Every test replays its journal with **0 appends**.
- **Mutation check.** Forcing `with_cancellation` to return `None` (no D11 wrapping) failed 4 of the 8 tests: both orderings, shielded and token-cancel. `select_loser` does not depend on the wrapping (cancel-op completion is 05-03's). The change was reverted.
- `sdk_guests` (7), `patching` (4) and `step_retry` (6) are green.
- ostro-server `sdk_e2e` (9), `executor` (8), `modules_api` (3), `runs_api` (2), `start_run_api` (6) and ostro `e2e_dev` (3) are green.
- **sdk_e2e after both 05-06 and 05-07:** ran green at `764bda4`, which contains 05-07's `a82df07`/`31b992f` and both of this plan's commits. The two 05-07 `ActiveVersion` literals were not touched; this plan made no edit to sdk_e2e.rs.

## Final gate

`CARGO_TARGET_DIR=target/wt-05-06 bash scripts/ci-local.sh --no-musl` ran in a detached scratch worktree at **`764bda4`**, so other agents' uncommitted edits were excluded. Other plans' gates were running on the machine at the same time (load average about 13).

```
toolchain 1.100.0                    PASS  1s
cargo fmt --check                    PASS  0s
cargo clippy -D warnings             PASS  31s
cargo build (warning-free)           PASS  67s
sdk workspace fmt + clippy           PASS  3s
sdk workspace test + wasm build      PASS  5s
guests build (wasm32-wasip2)         PASS  11s
cargo test                           FAIL  127s   (load timing flake, see below)
failpoint suites                     PASS  852s
cargo deny check                     PASS  1s
check-vendored.sh                    PASS  0s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  0s
zig/musl legs                        SKIP  (--no-musl)
```

- **`cargo test` failure: timing flake under load, not this plan.** `ostro --test dev_startup` failed two startup-budget tests. `warm_restart_under_1s` measured 1.22 s against the 1 s budget, and `default_data_dir_is_dot_ostro_and_persists` hit "/readyz not 200 within 30s". The cold first run took 10.1 s, against about 1.5 s normally. This plan touched no server, CLI or startup code.
- **Rerun alone in the same worktree:** `cargo test -p ostro --test dev_startup -- --test-threads=1` passed 4/4 (1 measurement-only test ignored).
- **Full workspace:** cargo stops at the first failing test binary, so `cargo test --workspace --locked --no-fail-fast` was re-run in the same worktree. It exited 0 with **962 passed, 0 failed, 10 ignored** over 124 result blocks.
- **Known flake not seen:** the failpoint step (including `ostro-partition` `rebuild_digest`) passed in the full run, and the known `node open timed out` flake did not occur.
- No new dependencies: no chrono, bincode or wasmtime `cache`. The root Cargo.lock is unchanged.
- `FAILPOINT_CRATES`, ci.yml, WIT and RESERVATIONS are untouched. No ordinal was added.

## Deviations from Plan

### Auto-fixed Issues

None. Two additions beyond the plan text:
- The `sdk-cancel` guest has three extra modes (`token`, `continue`, `continue_as`), and the suite has three tests for them. They give the new await-token and continue-as-new SDK API an engine-level proof, not just host unit tests.
- The `select_loser` case is **not** `#[ignore]`d, because 05-03 has landed.

### Not changed (listed in files_modified)

- `crates/ostro-engine/tests/step_retry.rs`, `crates/ostro-server/tests/sdk_e2e.rs` and `crates/ostro/tests/e2e_dev.rs`: green as they are. The D11 shape change touched no assertion in them.

## Issues Encountered

None. All commits went through a private index (`GIT_INDEX_FILE`, `read-tree HEAD`, add own paths). After each commit, `git reset -q -- <paths>` ran on the shared index and `git show --stat HEAD` was checked. Each commit contains only this plan's files.

## Hand-off

- **05-09 (continue-as-new apply, guests):**
  - A guest continues with `return Err(ctx.continue_as_new(&input))`, or with `Err(ContinueAsNew::new(&i)?.workflow("w").version(n).into())`.
  - The engine reports `ActivationOutcome::ContinuedAsNew { continuation }`. `continuation.input` is `application/json`, and `workflow`/`version` are `None` unless set.
  - `guests/sdk-cancel` modes `continue` and `continue_as` are ready-made fixtures. `continue` continues into `{"mode":"wait_sleep"}`; `continue_as` continues into `sdk-cancel` v1 `{"mode":"token"}`.
- **05-10 (phase-5 guests, race-fuzz) and order-saga:**
  - Write cleanup and compensation inside `ctx.shielded(|| …)`. Unshielded waits after a cancel return `Err(cancelled)` at once.
  - Propagating `Err(cancelled)` with `?` ends the run Cancelled (`Failure::Cancelled`).
  - `select!` returns index == number of arguments on cancel. Use `ostro_sdk::is_cancellation(&c)`, or decode with any op to get `Err(cancelled)`. Avoid `_ => unreachable!()` arms.
  - When cancellation wins, the SDK leaves the op open. Call `ctx.cancel(&op)` if it must not complete later (for example, a pending token).
  - `join!` may journal several `WaitResolved` events. Race-fuzz traces should compare live and replay, not assume one wait per `join!`.
  - The `select_loser` engine case is already enabled; there is nothing to re-enable.
- **05-12 (corpus):** record SDK journals only from this SDK version onward (D11 shape). Earlier SDK journals diverge at the first wait.
- **05-13 (token API):** the SDK side is done. The guest hands `t.token()` to the external system, which presents it to `POST /v1/tokens/complete`. The SDK never logs the token, and `AwaitToken`'s Debug omits it.
- **Phase 9 (Python/TS SDKs):**
  - Mirror D11 so the semantics match. Waits are `wait([ops…, CANCELLATION], any)` with the cancellation op **appended last**. `join` runs `any` rounds over the pending ops plus CANCELLATION. `select` reports cancel distinctly. `shielded` (a context manager or callback) uses the plain list and `all`.
  - Cross-SDK journals never need to match: each module replays its own. But the documented semantics (first completion in journal order wins; shielded cleanup) should be identical.
  - Use the constant 2^64-1 as a `bigint` in JS (PF-W3). Never pass it through `Number`.
  - Continue-as-new can be a return value or an exception, whichever is idiomatic. It maps to `run-result::continue-as-new`.
  - Re-read the README "Semantics" section for the PF-W1/W5/W9, D7 and F4 conventions mirrored there.

## Next Phase Readiness

The SDK surface is final for WIT 0.1, and the D11 shape lands before any corpus entry. 05-09, 05-10, 05-12, 05-15 and order-saga can build on it.

## Self-Check: PASSED

**PASSED.**

- All created files were FOUND: `guests/sdk-cancel/{Cargo.toml,src/lib.rs}` and `crates/ostro-engine/tests/sdk_cancellation.rs`.
- Both commits were FOUND: `b6cbdd9` and `764bda4`.
- Each artifact contains its check string: `fn create_await_token` in ctx.rs, `shield` in ops.rs.
- `crates/ostro-engine/tests/snapshots` has no changes.
