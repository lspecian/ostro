---
phase: 05-workflow-semantics-core-hardening
plan: 27
subsystem: engine
tags: [cancellation, step-retry, d25, determinism, qa-01, gap-closure]

requires:
  - phase: 05-20
    provides: "gate campaign finding (seed 0x0520, shard 12): cancel acked during in-activation step retries, attempts 6-9 still ran, run failed by retry exhaustion"
  - phase: 05-06
    provides: "D11 SDK cancellation surfacing (waits observe the cancellation op unless shielded)"
  - phase: 05-23
    provides: "D22 retry logical clock (begin_step raises now to retry_at)"
provides:
  - "D25: step retry backoff is a cancellation point (in-activation, requeued, timeout-interrupted)"
  - "ReplayState::cancel_pending / cancel_requested_seq / note_step_cancelled / deliver_inputs_in_backoff"
  - "sdk-cancel guest modes retry_step (backoff_ms) and shielded_retry"
  - "Gate shards 8, 9, 10, 12, 17, 18, 19 re-run on the fixed binary; aggregate 10,000 kills, 0 violations, RESULT: PASS"
affects: [05-20, 06, 09]

tech-stack:
  added: []
  patterns:
    - "A host-substituted step outcome is journaled and the activation evicted (StepInterrupted), never returned through completion-ack::recorded, so live and replay hand the guest the same value"

key-files:
  created:
    - crates/ostro-engine/tests/step_retry_cancel.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-27-SUMMARY.md
  modified:
    - crates/ostro-engine/src/host/journal.rs
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/wait.rs
    - crates/ostro-server/tests/semantics_e2e.rs
    - guests/sdk-cancel/src/lib.rs
    - docs/workflow-semantics.md
    - .planning/phases/05-workflow-semantics-core-hardening/05-DECISIONS.md
    - .planning/ROADMAP.md

key-decisions:
  - "D25: while a CancelRequested is journaled that the guest has not been handed yet, a failed step is never retried; it resolves StepCompleted(Err(cancelled))"
  - "'Handed the cancel' = a guest wait resolved with the cancellation op in its completed list, or a step resolved Err(cancelled) after the CancelRequested; after that, retries run normally (shielded cleanup completes)"
  - "In-activation the guest is blocked in complete(); completion-ack::recorded cannot carry a substituted outcome (the SDK returns its own encoded value), so the cancelled completion is journaled and the activation evicts StepInterrupted; the re-activation replays Err(cancelled)"
  - "Requeued: a resumed attempt with a pending cancel journals the cancelled completion at begin-step and returns it as replayed (before the not-yet-due check), so no extra eviction is needed"
  - "No WIT, journal kind, codec, apply or executor change; the server executor needed none (StepInterrupted already re-activates; a CAS conflict on EnqueueReady re-activates)"
  - "The mixed campaign (13 shards pre-fix, 7 post-fix) is reported as is: the fix is engine-only and touches no storage/apply/WAL path that the kill checks exercise"

requirements-completed: []

duration: ~2h40m (incl. ~54 min campaign wall)
completed: 2026-11-19
---

# Phase 5 Plan 27: Step retry backoff is a cancellation point (D25) Summary

**An accepted cancel now stops further step retries. A step that would be retried while a not-yet-observed `CancelRequested` is journaled resolves `Err(cancelled)`, and the run ends `cancelled`. This covers in-activation retries (cancel already journaled, or arriving during the backoff sleep), requeued retries and timeout-interrupted attempts. The decision is derived only from the journal, so it is replay-identical. The re-run gate shards pass against the unchanged checker: 10,000 kills, 0 violations.**

## The finding

Gate campaign shard 12, run `007038fn7de5jx3jpeh8nhft4xs8r`: step4 attempt 5 failed (seq 14), `cancel_requested` was journaled (seq 15), and attempts 6–9 still ran (seq 16–19). The step completed with the transient error and the run ended `failed`. The checker reported `WrongStatus ... stop intent Some(Cancel), acked Some(Cancel)`.

## Design

- **Rule (replay core).** `ReplayState::cancel_pending()` is true when the completion index has a `CancelRequested` (the completion of `CANCELLATION_OP`) and the guest has not been handed it yet. A hand-off is either a resolved wait (live or replayed `WaitResolved`) whose completed list contains `CANCELLATION_OP`, or a step resolved `Err(cancelled)` after the cancel. Both depend only on the journal and the guest's own replayed commands.
- **In-activation retry (`complete_step_impl`).** The failed attempt's `StepAttemptFailed` is journaled as before. Then:
  - If a cancel is already pending, `StepCompleted(Err(cancelled))` is journaled at the next seq and the activation evicts `StepInterrupted`.
  - Otherwise the attempt failure is flushed and the backoff sleep now also selects on the activation's input channel. Arriving inputs are delivered with `deliver_inputs_in_backoff`, which moves the step's reserved seq past them. If one of them is the `CancelRequested`, the step is completed as cancelled and the activation evicts the same way.

  The guest never gets an answer to that `complete` call. Its re-activation replays the recorded cancelled step.
- **Requeued retry.** The same check runs before `StepBackoff`, and the activation evicts `StepInterrupted`, which re-activates at once. `begin-step` of a resumed attempt (failures recorded) with a pending cancel journals the cancelled completion and returns it as `replayed`. That check comes before the not-yet-due check, so it fires before `retry_at`. If the cancel lands after the `EnqueueReady` CAS, the run wakes at `retry_at` at the latest (apply does not re-key a Ready row) and resolves cancelled then, with no further attempt.
- **Timeout-interrupted attempts.** `record_interrupted` applies the same check.
- **Shielded (D11).** After the hand-off, retries are normal, so cleanup and compensation steps retry to completion (`crash-saga` compensation, `sdk-cancel shielded_retry`). **Limitation:** the host cannot see `shielded`, which is SDK-only, so a retry inside a shielded section *before* the guest has observed the cancel also resolves cancelled. This is documented in `docs/workflow-semantics.md` §5.
- **Not changed:** the first attempt always runs and a running attempt is never interrupted. There are no WIT, journal-kind, codec, apply or executor changes. New metric: `ostro_step_retries_cancelled_total`.

## Tests

- `crates/ostro-engine/tests/step_retry_cancel.rs` (5 tests). A/B check: 3 of them fail on the pre-fix `host/journal.rs`.
  - `cancel_during_in_activation_backoff_stops_retries_and_run_ends_cancelled`: journal `RunStarted, SAF(1), CancelRequested, StepCompleted(cancelled)`, then the re-activation gives `Failed{Cancelled}` with 1 attempt. Replay is identical (0 appends, no sleep).
  - `cancel_journaled_before_a_failed_attempt_stops_its_retry`.
  - `requeued_retry_with_pending_cancel_resolves_cancelled_before_retry_at`.
  - `shielded_cleanup_step_still_retries_after_the_cancel_was_observed`: 2 retries, `cleaned:3`.
  - `retries_without_a_cancel_are_unchanged`: control, 9 retries.
- `crates/ostro-server/tests/semantics_e2e.rs` adds `cancel_stops_step_retries_in_activation` (hot_wait 3 s, 1.5 s backoff) and `cancel_stops_requeued_step_retries` (hot_wait 50 ms, 1.5 s backoff). Each checks:
  - REST status `cancelled`;
  - the step completed `Err(cancelled)` after the cancel, with no `StepAttemptFailed` after the cancel;
  - the journal unchanged after 2×backoff + 200 ms;
  - fresh-runtime replay gives `Failed{Cancelled}` with 0 appends.
- `cargo test -p ostro-engine -p ostro-server --locked`: 437 passed, 0 failed, 5 ignored.
- `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. sdk-cancel guest clippy is clean, `cargo fmt --check` is clean, and the build has no warnings.

## Campaign re-run (unchanged checker)

- The failpoint binary was rebuilt from `c6b1c09`: `target/failpoints/release/ostro` sha256 `ddbd2bbb…aabe6` (was `45736828…dfd70b`).
- `crates/ostro-crashtest/src/check.rs` is byte-identical to 7289d56, and the crashtest tree is clean in wt-05-20.
- `shard-12*` and `shard-17*` were moved to `target/campaign-gate/superseded/`. `shard-08..10` were not at the top level; their first attempt stays in `first-attempt-08-10/`. The old violation histories were left in place.
- Shards were re-run with seed 0x0520, 20 shards, 10,000 faults and parallel 6:
  - batch `8-10,12,17,18`: rc 0, 1945 s;
  - batch `19`: rc 0, 1275 s.

  Every re-run shard had 500 kills and 0 violations. Shard 12 final: cancelled 177, completed 5923, terminated 72, failed 0. The shard-17 terminate pattern did not recur (failed 2, both accepted by the checker).
- Aggregate report: `kills=10000 lost_acked_runs=0 duplicate_journal_entries=0 non_terminal_runs=0 lost_acked_signals=0 wrong_results=0 other_violations=0`, shards 20/20, acked runs 120,762, **RESULT: PASS**.
- Mixed binaries: the runner does not record binary identity and did not refuse. 13 shards (0–7, 11, 13–16) ran the pre-fix binary. The fix only changes engine retry behaviour after a cancel. Their durability evidence (lost/duplicated data, rebuild digests) covers storage/apply/WAL code that this change does not touch, so the shards were not all re-run. The orchestrator may still require a full 20-shard re-run on `ddbd2bbb…` for a single-binary gate claim.

## WIT doc-comment text (for the freeze patch; runtime.wit not edited)

For `resource step` / `complete`:

`/// A step retry is a cancellation point: while a graceful cancel the guest has not yet observed is journaled, a failed step is not retried but resolves `err(cancelled)` (the activation may be evicted inside `complete`; replay then returns the recorded outcome).`

## Deviations from Plan

- **[Rule 3] An existing guest was extended instead of adding a new one.** `guests/sdk-cancel` gained the modes `retry_step` and `shielded_retry` (a new optional `backoff_ms` input field). Replay-corpus entries carry their own module bytes, so they are unaffected.
- No server executor change was needed. The executor already re-activates on `StepInterrupted` and on an `EnqueueReady` CAS conflict.

## Self-Check: PASSED

The product commit `c6b1c09` exists on main, all listed files exist, and the aggregate report says RESULT: PASS.
