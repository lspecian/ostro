---
phase: 04-single-node-server-rust-mvp
plan: 04
subsystem: engine
tags: [journal, retry, replay, wf-02, determinism]
requires:
  - phase: 03-deterministic-wasm-engine-wit-0-1
    provides: journal codec v1 + golden vectors (03-02), replay cursor (03-05), step-local RNG (03-11)
provides:
  - "EventKind::StepAttemptFailed = 29 (additive, no EVENT_CODEC_VER bump) + StepAttemptFailed / AttemptBackoff"
  - "ostro_engine::retry::{RetryPolicy, RetryDecision, decide, delay_ms, is_retryable}"
  - "ReplayState::begin_step attempt-chain consumption; StepBegin::Live { seq, attempt, last_retry_at_nanos }"
  - "ReplayState::record_attempt_failed(seq, body) -> next seq"
affects: [04-07 host step retry, 04-10 step timeouts, replay corpus]
tech-stack:
  added: []
  patterns:
    - "Additive journal kinds: new code + payload struct + EventBody variant appended at the end + golden line appended after all Phase 3 vectors"
    - "Retry backoff via repeated IEEE multiplication with early cap (platform-exact, no powf)"
key-files:
  created:
    - crates/ostro-engine/src/retry.rs
  modified:
    - crates/ostro-types/src/journal.rs
    - crates/ostro-types/tests/journal_golden.rs
    - crates/ostro-types/tests/golden/journal_v1.txt
    - crates/ostro-engine/src/lib.rs
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/tests.rs
    - crates/ostro-engine/src/host/journal.rs
key-decisions:
  - "StepAttemptFailed is a guest-command event (code 29, family-matched with CommandKind::Step); attempt numbers in a chain must be 1,2,..,n (gap = BadJournal)"
  - "max_attempts counts total attempts incl. the first; 0 and 1 mean no retry (differs from Temporal); Timeout is retryable; no jitter in v1"
  - "record_attempt_failed re-opens the step at the next seq with a step-local RNG seeded from the NEW seq; run-level streams saved once and restored on close"
patterns-established:
  - "Additive event kinds never bump EVENT_CODEC_VER; documented in journal.rs module docs"
requirements-completed: []
duration: 9min
completed: 2026-11-18
---

# Phase 4 Plan 04: StepAttemptFailed + Retry Decision + Replay Attempt Chains Summary

**Additive journal kind 29 `StepAttemptFailed` (golden line appended, zero existing lines changed), a pure saturating exponential-backoff `retry::decide`, and replay-cursor support that consumes attempt chains and resumes live at attempt k+1.**

## Performance

- **Duration:** ~9 min
- **Started:** 2026-11-18T20:39:00Z
- **Completed:** 2026-11-18T20:48:00Z
- **Tasks:** 2
- **Files modified:** 8 (1 created)

## Accomplishments
- `EventKind::StepAttemptFailed = 29` with `StepAttemptFailed { meta, name, attempt, failure, retry_at_nanos, backoff, fuel_used }` and `AttemptBackoff { InActivation, Requeue }`; `EventBody` variant appended at the end; encode/decode, summary, `command_meta`, `from_code` extended. Module docs record the "additive kind, no EVENT_CODEC_VER bump" rule.
- Golden file: exactly one line added (`event 29 step_attempt_failed ...`); `git diff | grep '^-[^-]'` is empty. Proptest `arb_body` covers SAF round-trips.
- `ostro_engine::retry`: `RetryPolicy` (plain mirror of WIT `retry-policy`), `RetryDecision::{Record, Retry { retry_at_nanos, delay, backoff }}`, `decide(policy, attempt, failure, now_nanos, hot_wait)`; delays 200/400/800/1000/1000 ms for initial 200, coeff 2, max 1 s; NaN/inf/<1 coeff -> 1; max < initial -> initial; overflow saturates; `initial == 0` early exit so huge attempt numbers never spin.
- Replay cursor: `begin_step` consumes a `StepAttemptFailed` chain with the step's hash and returns the recorded `StepCompleted`, or `Live { seq, attempt: n+1, last_retry_at_nanos }`. `record_attempt_failed` appends the SAF at the open seq and re-opens at the next seq with fresh step-local RNG. `complete_step` stays StepCompleted-only.
- Determinism suite green without re-blessing det-basic; full `cargo test -p ostro-engine` green.

## Task Commits

1. **Task 1: StepAttemptFailed kind + retry decision** - `6a76bd3` (feat)
2. **Task 2: Replay cursor attempt chains** - `660c1d4` (test, RED) + `807e812` (feat, GREEN)

## Files Created/Modified
- `crates/ostro-types/src/journal.rs` - kind 29, struct, enum, codec arms, docs, unit tests (21 kinds)
- `crates/ostro-types/tests/journal_golden.rs` - SAF golden fixture (appended last, seq 22) + proptest arm
- `crates/ostro-types/tests/golden/journal_v1.txt` - one appended line
- `crates/ostro-engine/src/retry.rs` - retry policy evaluator + tests
- `crates/ostro-engine/src/lib.rs` - `pub mod retry;`
- `crates/ostro-engine/src/replay/cursor.rs` - chain loop, `open_live_step`, `record_attempt_failed`, `StepBegin::Live` fields
- `crates/ostro-engine/src/replay/tests.rs` - 6 attempt-chain tests
- `crates/ostro-engine/src/host/journal.rs` - `StepBegin::Live { seq, .. }` pass-through

## Decisions Made
- SAF is family-matched with `CommandKind::Step`, so a non-step command where the journal has a SAF is a NonDeterminism at the SAF's seq (existing divergence path).
- Attempt numbers are validated against the chain position (BadJournal on mismatch) rather than trusted blindly.
- The golden SAF fixture is placed after all Phase 3 events in `golden_events()` so blessing only adds a line (the event line sits after `event 69`, not after `event 28`).

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Unbounded backoff loop for zero initial interval**
- **Found during:** Task 1
- **Issue:** with `initial_interval_ms = 0` and coeff > 1 the delay never reaches the cap, so the multiplication loop would run `attempt` times (up to u32::MAX).
- **Fix:** early return when `initial == 0`; test added.
- **Files modified:** crates/ostro-engine/src/retry.rs
- **Commit:** 6a76bd3

### Process notes
- Task 1 RED/GREEN were not split into separate commits: the journal type addition and its golden test are coupled, and leaving `ostro-types` uncompilable in the shared working tree would have broken concurrently running plans. RED was still observed (golden test failed on the missing kind-29 line before blessing). Task 2 has a separate RED commit (lib test target only; the library kept compiling).

**Total deviations:** 1 auto-fixed (Rule 1). **Impact:** none on scope.

## Issues Encountered
None.

## User Setup Required
None.

## Next Phase Readiness
- 04-07 can wire `retry-now` / StepBackoff eviction using `retry::decide`, `StepBegin::Live { attempt, last_retry_at_nanos }` and `record_attempt_failed`. WF-02 remains open until 04-07 (host wiring) and 04-10 (step timeouts) land.

## Self-Check: PASSED
- FOUND: crates/ostro-engine/src/retry.rs
- FOUND commits: 6a76bd3, 660c1d4, 807e812

---
*Phase: 04-single-node-server-rust-mvp*
*Completed: 2026-11-18*
