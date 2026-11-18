---
phase: 04-single-node-server-rust-mvp
plan: 10
subsystem: engine
tags: [retry, timeout, fuel, wf-02, wasmtime, epoch]
requires:
  - phase: 04-single-node-server-rust-mvp
    provides: "04-07 host step retry (StepRep.timeout_ms/fuel_budget, retry::decide wiring, StepBackoff eviction, retry-probe guest)"
  - phase: 03-deterministic-wasm-engine-wit-0-1
    provides: "03-03 epoch ticker + configure_store, 03-07 per-step fuel shadowing, 03-08 activation driver"
provides:
  - "Step timeout-ms enforced by the epoch-deadline callback (trap payload error::StepTimedOut{seq})"
  - "HasLimits::step_deadline() (default None); HostState::open_step (OpenStep { rep, deadline })"
  - "HostState::fail_interrupted_step: records the interrupted attempt (Timeout / NonRetryable fuel_exhausted) via retry::decide"
  - "EvictReason::StepInterrupted (code step_interrupted): step failure recorded, re-activate immediately"
  - "retry-probe modes hang:T, quick, burn, spin"
affects: [04 executor/dispatcher (re-activate on StepInterrupted, requeue on StepBackoff), 04-20 e2e WF-02 proof]
tech-stack:
  added: []
  patterns:
    - "Per-step deadline lives in store data T; Wasmtime 50.0.2 epoch callback gets StoreHookState<T> with data()"
    - "Interrupted step failures are recorded by the driver after unwind (no in-place retry possible)"
key-files:
  created:
    - crates/ostro-engine/tests/step_timeout.rs
  modified:
    - crates/ostro-engine/src/engine.rs
    - crates/ostro-engine/src/host/journal.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-engine/src/activation.rs
    - crates/ostro-engine/src/error.rs
    - crates/ostro-engine/src/limits.rs
    - guests/retry-probe/src/lib.rs
    - guests/sandbox/src/lib.rs
    - crates/ostro-engine/tests/sandbox.rs
key-decisions:
  - "Open Question 5 resolved: Wasmtime 50.0.2 epoch_deadline_callback receives StoreHookState<T> (data()/data_mut()), and an Err from the callback propagates as the trap, so the step deadline is a HostState field read via HasLimits::step_deadline; no fallback needed"
  - "Effective deadline = min(activation wall deadline, step deadline); when the step deadline is <= the wall deadline it traps StepTimedOut{seq}, else Trap::Interrupt (attempt Timeout). Checked per epoch tick (10 ms)"
  - "Step timeout is per attempt (start-to-close): armed at begin-step, re-armed after the in-activation retry sleep, cleared on recorded completion/drop/requeue"
  - "Interrupted step: retry::decide with a zero window; Retry -> SAF with backoff forced to Requeue + Evicted{StepBackoff{retry_at}}; Record -> StepCompleted(Err) + Evicted{StepInterrupted} (new reason, executor re-activates at once)"
  - "OutOfFuel is attributed to the step only if its budget was applied and strictly below the outer remaining fuel; otherwise it is the activation's FuelExhausted. fuel_used of a fuel breach = the whole step budget"
  - "Unattributable StepTimedOut (no matching open step) classifies as AttemptFailure::Timeout"
requirements-completed: [WF-02]
duration: 25min
completed: 2026-11-18
---

# Phase 4 Plan 10: Step Timeout + Per-Step Fuel Breach Attribution Summary

**A step's `timeout-ms` is now enforced. The store's epoch-deadline callback reads the open step's deadline from `HostState` and traps `StepTimedOut{seq}`. A step body that runs out of its own fuel budget no longer fails the attempt. In both cases the driver records the failure for that step only, through the 04-07 retry path. While attempts remain it writes `StepAttemptFailed(Requeue)` and evicts with `StepBackoff`. When none remain it writes `StepCompleted(Err(..))` and evicts with the new `StepInterrupted`.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-11-18T21:00:00Z
- **Completed:** 2026-11-18T21:25:00Z
- **Tasks:** 1 (TDD: RED + GREEN commits)
- **Files modified:** 10 (1 created)

## Accomplishments
- **Research Open Question 5 (resolved):** I read the Wasmtime 50.0.2 source.
  - `Store::epoch_deadline_callback` takes `FnMut(StoreHookState<T>) -> Result<UpdateDeadline>`.
  - `StoreHookState` exposes `data()` and `data_mut()`.
  - The callback's `Err` is propagated with `?` from the `new_epoch` libcall, so it becomes the guest trap.
  - So a per-step deadline in the store data is visible to the callback, and the plan's fallback was not needed.
- **Callback logic (`configure_store`):** each epoch tick compares now against `HasLimits::step_deadline()` and `wall_deadline()`.
  - The step deadline passed and is no later than the wall deadline: it traps `StepTimedOut{seq}`.
  - The wall deadline passed: `Interrupt`.
  - Otherwise: `Yield(1)`.
- **`HostState.open_step`:** holds `OpenStep { rep: StepRep, deadline }`.
  - begin-step sets it.
  - After the in-activation retry sleep it moves to the next seq and attempt with a fresh deadline.
  - Recorded completion, drop and a requeue clear it.
- **Driver (`activation.rs`):** on `StepTimedOut`, or on `Trap::OutOfFuel` while the step budget was the binding limit, it calls `HostState::fail_interrupted_step(cause, store fuel)`, flushes, and ends with an `Evicted` outcome. A `ReplayError` here becomes `AttemptFailed`. Anything not attributable goes to `classify` as before.
- **`tests/step_timeout.rs`:** 5 tests, all green.
  - `hang:200`: attempt 1 ends `SAF{attempt 1, Timeout, Requeue, retry_at = ts+10ms}` with `StepBackoff`. Attempt 2 at retry_at gives `StepCompleted(Err(Timeout))` with `StepInterrupted`. Re-activation returns `err:timeout`. Total under 1 s. Replay appends 0 events and does not wait.
  - `quick`: unaffected.
  - `burn`: `StepCompleted(Err(NonRetryable{fuel_exhausted}))` with fuel_used 1,000,000 and no SAF, even though the policy allows 3 attempts. The run finishes `err:non-retryable:fuel_exhausted`, not `AttemptFailed(FuelExhausted)`.
  - `spin` outside a step: `AttemptFailed(Timeout)` at a 300 ms activation_timeout.
  - `hang:5000` under a 300 ms activation_timeout: the activation deadline wins with `AttemptFailed(Timeout)` and nothing is journaled.
- `cargo test -p ostro-engine --locked` passes in full, including step_retry, wit_surface, determinism, stack_depth, hot_wait, sandbox and sdk_guests. `cargo clippy -p ostro-engine --all-targets -D warnings` is clean. `git diff --stat crates/ostro-engine/tests/snapshots` is empty, so the det-basic golden snapshot is untouched.

## Task Commits

1. **Task 1 RED: retry-probe modes, failing step_timeout suite, EvictReason::StepInterrupted** - `3697797` (test)
2. **Task 1 GREEN: epoch-callback step timeout + fuel-breach attribution** - `8128cc6` (feat)

## Files Created/Modified
- `crates/ostro-engine/tests/step_timeout.rs` - timeout/retry interplay, in-time step, fuel breach, activation deadline
- `crates/ostro-engine/src/engine.rs` - epoch callback aware of the step deadline
- `crates/ostro-engine/src/host/journal.rs` - OpenStep lifecycle, `StepInterrupt`, `step_fuel_breach`, `fail_interrupted_step`
- `crates/ostro-engine/src/host/state.rs` - `OpenStep`, `open_step` field, `HasLimits::step_deadline` impl
- `crates/ostro-engine/src/activation.rs` - post-unwind step failure recording, `classify_outcome`, StepTimedOut fallback
- `crates/ostro-engine/src/error.rs` - `StepTimedOut`, `EvictReason::StepInterrupted`
- `crates/ostro-engine/src/limits.rs` - `HasLimits::step_deadline` (default `None`)
- `guests/retry-probe/src/lib.rs` - hang:T / quick / burn / spin modes
- `guests/sandbox/src/lib.rs`, `crates/ostro-engine/tests/sandbox.rs` - step-fuel expectations moved to step-level semantics

## Decisions Made
See `key-decisions` in the frontmatter.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Intended semantic change] Phase 3 sandbox step-fuel test expected AttemptFailed(FuelExhausted)**
- **Found during:** Task 1 GREEN (full suite)
- **Issue:** `sandbox_step_fuel` and `sandbox_isolation` (03-10, ENG-05) asserted that a per-step fuel breach fails the attempt. This plan changes that behaviour on purpose. The sandbox guest also panicked when a step it had replayed contained a recorded failure.
- **Fix:** The `step-fuel` workflow now returns the hot step's recorded failure as the run failure. The tests assert the first activation is `Evicted{StepInterrupted}`, the run ends `Failed{NonRetryable fuel_exhausted}` after 2 activations, and the steps are `["ok", "hot"]`.
- **Files modified:** guests/sandbox/src/lib.rs, crates/ostro-engine/tests/sandbox.rs
- **Commit:** 8128cc6

**2. [Rule 3 - Blocking] Edits outside files_modified (error.rs, limits.rs)**
- `StepTimedOut` and `EvictReason::StepInterrupted` belong in `error.rs`. The callback reads the step deadline through the `HasLimits` trait in `limits.rs`, as a default method. Both changes are additive.

**3. [Process] Running the plan's verify command without `--target-dir` created `guests/target`**
- That directory then became a `members = ["*"]` workspace member, which broke `tests/common::guest_wasm`. I removed it and used `--target-dir target/guests` from then on.

## Issues Encountered
None beyond the above. The concurrent 04-06 and 04-08 work did not affect `-p ostro-engine`.

## Next Phase Readiness
- The executor or dispatcher must handle two eviction reasons:
  - `Evicted{StepInterrupted}`: re-activate immediately. The journal already holds the step's recorded failure.
  - `Evicted{StepBackoff{retry_at}}`: requeue at `retry_at` (as in 04-07).
- WF-02 is delivered at the engine level: retries, non-retryable kinds, timeout and fuel budget. The end-to-end proof through the server and SDK is in 04-20.
- Timeout granularity is `engine.epoch_tick` (10 ms default). A step blocked in a host call is not interrupted until wasm runs again. Inside an open step only non-blocking imports are allowed, so this does not matter today.

## Self-Check: PASSED
- FOUND: crates/ostro-engine/tests/step_timeout.rs, guests/retry-probe/src/lib.rs
- FOUND commits: 3697797, 8128cc6

---
*Phase: 04-single-node-server-rust-mvp*
*Completed: 2026-11-18*
