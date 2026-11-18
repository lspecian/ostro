---
phase: 04-single-node-server-rust-mvp
plan: 07
subsystem: engine
tags: [retry, wf-02, host-adapter, eviction, wasmtime]
requires:
  - phase: 04-single-node-server-rust-mvp
    provides: "04-04 StepAttemptFailed kind 29, retry::decide, StepBegin::Live { attempt, last_retry_at_nanos }, record_attempt_failed"
  - phase: 03-deterministic-wasm-engine-wit-0-1
    provides: "03-07 host adapter + per-step fuel shadowing, 03-08 activation driver flush rules, 03-11 hot wait + L2 InstancePre LRU"
provides:
  - "Host-evaluated step retry: StepAttemptFailed(InActivation) + flush + in-host sleep + retry-now; StepAttemptFailed(Requeue) + Evicted{StepBackoff}"
  - "EvictReason::StepBackoff { retry_at_nanos } and EvictReason::code() metrics label"
  - "Re-activation before retry_at re-evicts with StepBackoff without running the body"
  - "Runtime::prepared(&[u8;32]) -> Option<Arc<Prepared>> (pure L2 lookup)"
  - "HostState::set_step_retry(window, cancel); ReplayState::live_now_nanos()"
  - "guests/retry-probe test guest"
affects: [04-10 step timeouts, 04 executor/dispatcher (requeue at retry_at), SDK retry loops]
tech-stack:
  added: []
  patterns:
    - "[method]step.complete shadowed with LinkerInstance::func_wrap_async (StoreContextMut kept for fuel get/set)"
    - "Retry decisions read the clock only for steps with a policy; their result is journaled, so replay never re-decides or sleeps"
key-files:
  created:
    - guests/retry-probe/Cargo.toml
    - guests/retry-probe/src/lib.rs
    - crates/ostro-engine/tests/step_retry.rs
  modified:
    - crates/ostro-engine/src/host/journal.rs
    - crates/ostro-engine/src/host/state.rs
    - crates/ostro-engine/src/host/bindings.rs
    - crates/ostro-engine/src/host/effects.rs
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/activation.rs
    - crates/ostro-engine/src/error.rs
    - crates/ostro-engine/src/runtime.rs
key-decisions:
  - "Retry window = engine.hot_wait, wired by the activation driver for every activation (with or without a hot-wait input channel); HostState built directly defaults to the hot-wait window or zero (every retry evicts)"
  - "retry_at = live now (max(clock, last command ts, max journal ts)) + delay at decision time; the SAF ts is >= that (equal under a frozen clock)"
  - "Requeue SAF is flushed by the driver after the StepBackoff trap unwinds (same path as WaitUnsatisfied); InActivation SAF is flushed inside the host call before sleeping"
  - "A resumed step whose last SAF retry_at is in the future always evicts StepBackoff (no in-host sleep at begin-step, which stays a sync func_wrap)"
  - "Per-step fuel is re-armed per attempt: budget = min(step fuel budget, outer remaining after the failed attempt); SAF.fuel_used is the failed attempt's usage"
requirements-completed: []
duration: 10min
completed: 2026-11-18
---

# Phase 4 Plan 07: Host Step Retry (retry-now, StepBackoff, Runtime::prepared) Summary

**`[method]step.complete` now evaluates the step's retry policy: short backoffs journal and flush `StepAttemptFailed(InActivation)`, sleep in the host and return `retry-now`; long ones journal `StepAttemptFailed(Requeue)` and evict with the new `EvictReason::StepBackoff { retry_at_nanos }`. Attempt counting lives in the journal, so it carries over across re-activations and runtime restarts.**

## Performance

- **Duration:** ~10 min
- **Started:** 2026-11-18T20:48:00Z
- **Completed:** 2026-11-18T20:58:00Z
- **Tasks:** 2
- **Files modified:** 11 (3 created)

## Accomplishments
- `complete_step_impl` is async. On `Err(f)` for a step with a policy it calls `retry::decide(policy, attempt, f, live_now, retry_window)`.
  - **Record:** the existing `StepCompleted` path runs and the guest gets `Recorded`.
  - **Retry{InActivation}:** `record_attempt_failed` journals the SAF, the `StepRep` moves to the next seq with `attempt + 1`, per-step fuel is re-armed, the SAF is flushed, the host sleeps the delay (cancel wins and gives `Evict{External}`), and the guest gets `RetryNow`.
  - **Retry{Requeue}:** the SAF is journaled and the call traps `Evict{StepBackoff{retry_at_nanos}}`.
- `begin_step_impl` uses `StepBegin::Live { seq, attempt, last_retry_at_nanos }`. If `retry_at` is still after live now, it abandons the step and evicts with `StepBackoff` without running the body. Nothing is appended.
- `[method]step.complete` is registered with `LinkerInstance::func_wrap_async`. I checked the Wasmtime 50.0.2 source: the closure receives `StoreContextMut<'_, T>` and returns `Box<dyn Future + Send + '_>`, so `get_fuel`/`set_fuel` still work.
- `EvictReason::StepBackoff { retry_at_nanos }` added. `EvictReason` stays `Copy + Eq`. New `EvictReason::code()` feeds a new `ostro_activation_evictions_total{reason}` counter. Also new: `ostro_step_retries_total{backoff}`.
- `Runtime::prepared(sha)` looks up the L2 LRU only and bumps recency. It reads no bytes and does no hashing, and leaves the AOT cache stats unchanged (tested).
- `guests/retry-probe` (raw wit-bindgen, no SDK) has modes exhaust, recover:N, fatal, nonretryable, requeue and nopolicy.
- `tests/step_retry.rs` has 6 tests, all green:
  - exhaust: SAF 1/2/3 InActivation with retry_at = ts + 10/20/40 ms, then StepCompleted(Err). One activation, 4 append batches (each SAF flushed before its sleep). Replaying the journal appends 0 events and does not sleep.
  - recover:2: 2 SAFs, then StepCompleted(Ok).
  - fatal / nonretryable / nopolicy: no SAFs; the completion comes right after RunStarted.
  - requeue: StepBackoff(T0+5s). Re-activating before retry_at evicts again with no new events. Attempt 2 at T0+5s, then the recorded attempt 3.
  - Runtime restart between attempts: numbering continues at 2, total attempts = max_attempts.
  - `prepared(sha)` lookup.
- The full `cargo test -p ostro-engine --locked` passes, including determinism, hot_wait, wit_surface, host_adapter, sandbox, stack_depth (ENG-06) and the concurrent 04-06 sdk_guests. `cargo clippy -p ostro-engine --all-targets -D warnings` is clean. `git diff --stat crates/ostro-engine/tests/snapshots` is empty: the det-basic golden snapshot was not re-blessed.

## Task Commits

1. **Task 1: retry-probe guest, step_retry tests (RED), StepBackoff, Runtime::prepared** - `5f4b5e9` (test)
2. **Task 2: Host retry evaluation, async complete, StepBackoff eviction (GREEN)** - `774f7a4` (feat)

## Files Created/Modified
- `guests/retry-probe/{Cargo.toml,src/lib.rs}` - retry probe workflow
- `crates/ostro-engine/tests/step_retry.rs` - retry suite driven by FakeSink with a settable TestClock
- `crates/ostro-engine/src/host/journal.rs` - retry evaluation, retry-now, StepBackoff, begin-step backoff check, fuel re-arm
- `crates/ostro-engine/src/host/state.rs` - `retry_window`/`retry_cancel`, `set_step_retry`, `func_wrap_async` complete, docs
- `crates/ostro-engine/src/host/bindings.rs` - `StepRep { attempt, retry, timeout_ms, fuel_budget }` (Eq dropped because of f64)
- `crates/ostro-engine/src/host/effects.rs` - `flush_trap` made `pub(crate)`
- `crates/ostro-engine/src/replay/cursor.rs` - `live_now_nanos()`
- `crates/ostro-engine/src/activation.rs` - sets the retry window/cancel; eviction-reason metric
- `crates/ostro-engine/src/error.rs` - `StepBackoff`, `code()`
- `crates/ostro-engine/src/runtime.rs` - `prepared(sha)`

## Decisions Made
See `key-decisions` in the frontmatter. In short:
- The retry window is `engine.hot_wait`.
- `retry_at` is computed from live now when the decision is made.
- A Requeue SAF is flushed by the driver after the trap unwinds.
- `begin-step` evicts and never sleeps.
- Fuel is re-armed per attempt.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Edits outside files_modified (bindings.rs, effects.rs, cursor.rs)**
- **Found during:** Task 2
- **Issue:** `StepRep` is defined in `host/bindings.rs`. The flush error mapping `flush_trap` was private in `effects.rs`. An exact live "now" (`max(clock, last cmd ts, max journal ts)`) needed `max_ts`, which is private to the replay cursor.
- **Fix:** Added fields to `StepRep`, made `flush_trap` `pub(crate)`, and added `ReplayState::live_now_nanos()`. All three changes are additive.
- **Files modified:** crates/ostro-engine/src/host/bindings.rs, host/effects.rs, replay/cursor.rs
- **Commit:** 774f7a4

**2. [Design] Retry window set by a setter, not a `HostInit` field**
- Adding a field to `HostInit` would break the struct literals in `tests/host_adapter.rs` and `tests/wit_surface.rs`. Instead, `HostState::set_step_retry(window, cancel)` is called by the activation driver. A HostState built directly defaults to the hot-wait window (or zero).

**3. [Design] Requeue flush happens in the driver after unwind**
- The plan said "record SAF, flush, then trap". The driver already flushes on every `Evict` (03-08). The flush errors map the same way and the result is the same, so there is no second flush path in the host.

**Total deviations:** 1 auto-fixed (Rule 3) and 2 minor design simplifications. No scope change.

## Issues Encountered
None. 04-06 added `guests/sdk-*` while this plan was running; the guests workspace still built and their `sdk_guests` suite passed.

## User Setup Required
None.

## Next Phase Readiness
- The executor or dispatcher must handle `Evicted { StepBackoff { retry_at_nanos } }` by requeueing the run at `retry_at`. It can use `Runtime::prepared(sha)` before falling back to `prepare_cached` over fetched bytes.
- 04-10 (step timeouts) can use `StepRep.timeout_ms`. WF-02 stays open until 04-10 lands.
- Not done: an in-activation retry sleep is not registered with `ParkedRegistry`, so it does not take part in budget-pressure LRU eviction. The sleep is bounded by `engine.hot_wait`.

## Self-Check: PASSED
- FOUND: guests/retry-probe/src/lib.rs, crates/ostro-engine/tests/step_retry.rs
- FOUND commits: 5f4b5e9, 774f7a4

---
*Phase: 04-single-node-server-rust-mvp*
*Completed: 2026-11-18*
