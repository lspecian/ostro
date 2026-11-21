---
phase: 07-workers-placement-flow-control
plan: 04
subsystem: partition-apply
tags: [pause, resume, apply, rebuild-digest, ops]
requires:
  - phase: 07-workers-placement-flow-control
    provides: "07-03 format contract: Cmd 20/21, RejectReason::RunPaused (15, code 31), Action::RunPaused/RunResumed, apply/pause.rs stubs"
provides:
  - "apply_pause_run / apply_resume_run (1 read each)"
  - "pause gate inside run::enqueue_ready (paused => Ready, no ready row, no EnqueueReady)"
  - "RunPaused rejects of EventsAppended / AttemptGranted after the fencing checks"
  - "terminal::on_terminal clears RunRecord.pause (B7)"
  - "PartitionSink: RunPaused -> AppendError::Fenced"
  - "rebuild_digest gen_pause generator + pause invariants + fixed_scenario_pause"
affects: [07-05, 07-08, 07-14, 07-19]
tech-stack:
  added: []
  patterns: ["single choke point gate in enqueue_ready instead of per-caller checks"]
key-files:
  created:
    - crates/ostro-partition/tests/pause.rs
    - crates/ostro-partition/tests/common/gen_pause.rs
  modified:
    - crates/ostro-partition/src/apply/pause.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/terminal.rs
    - crates/ostro-server/src/sink.rs
    - crates/ostro-partition/tests/p7_format.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-partition/tests/rebuild_digest.rs
    - docs/storage-format.md
key-decisions:
  - "PauseRun with an unknown reason code (not 0/1) is rejected Invalid(Decode), like an unknown ConflictPolicy; no new InvalidCode"
  - "Auto-pause fencing mirrors EventsAppended exactly: StaleEpoch for a foreign epoch, Fenced unless the token equals the current attempt; checked after RunTerminal, before idempotence"
  - "A paused run that is still Running keeps getting the in-memory Action::InputAppended on inputs (inputs.rs is outside this plan); the dispatcher ignores it after revoking the run (07-05)"
  - "Resume ready time = ResumeRun.requested_at_ms"
requirements-completed: []
duration: ~3h20m wall (of which ~57 min for the 256-case rebuild_digest run, ~20 min for the failpoint run and ~70 min for the full ostro-partition suite, at load 17-24)
completed: 2026-11-21
---

# Phase 7 Plan 04: Run pause/resume apply Summary

**`PauseRun`/`ResumeRun` implemented in `apply/pause.rs` (1 read each), with the pause check in `enqueue_ready` as the single gate. Paused runs reject executor writes with `RunPaused` after fencing. Every terminal transition clears the mark, so a run that times out while paused can be retried. `PartitionSink` maps `RunPaused` to `Fenced`. The rebuild_digest pause generator passes 256 cases.**

## Task commits

1. Task 1: `9928aaf` feat(07-04): run pause/resume apply with enqueue_ready gate (7 files, +896/-40)
2. Task 2: `0c9840c` test(07-04): rebuild_digest pause generator (3 files, +496/-1)

## Behaviour

| Situation | Result |
|---|---|
| `PauseRun` on a Scheduled / Ready / Running run | `pause = Some(PauseMark{reason, at_ms: requested_at_ms, detail})`, ready row removed via `rec.ready`, status unchanged (Running stays Running, its `running` row too), `Action::RunPaused`, Accepted |
| `PauseRun` on an already paused run | Accepted, no writes, no action |
| `PauseRun` on a terminal / unknown run | `RunTerminal` / `RunNotFound` |
| `PauseRun` with a bad run id / foreign partition / detail > 4 KiB / reason not 0 or 1 | `Invalid(BadRunId)` / `WrongPartition` / `Invalid(FieldTooLong)` / `Invalid(Decode)`, all checked before the read |
| Auto-pause (`token` set) with a foreign epoch / a token that is not the current attempt | `StaleEpoch` / `Fenced{current}`, same as `EventsAppended` |
| `enqueue_ready` while paused (every caller: input wake, start, retry, `EnqueueReady` intent) | `leave_running` + `remove_ready`, status Ready, `rec.ready = None`, **no** ready row, **no** `EnqueueReady` |
| `EventsAppended` while paused (plain, `EnqueueReady`, `Suspend`, `CompleteRun`, CAN) | `RunPaused`, nothing written |
| `AttemptGranted` while paused | `RunPaused`, nothing written |
| Inputs while paused (`TimerFired`, `CompleteEffect`, `SignalRun`, `CompleteToken`, `RequestCancel`) | journaled (`next_seq` moves), no ready row, no `EnqueueReady` |
| Run timeout / `TerminateRun` while paused | terminal as usual. `terminal::on_terminal` clears `pause` (B7), and `TimedOut` stays resumable |
| `RetryRun` after TimedOut-while-paused | ready row + `EnqueueReady`, and the run can be granted and completed again |
| `ResumeRun` on a paused Ready / Running run | mark cleared, then `enqueue_ready` at `requested_at_ms` (leave_running + ready row + `EnqueueReady`; `first_activation = current_attempt.is_none()`, so `false` for a started run), `Action::RunResumed` |
| `ResumeRun` on a paused Scheduled run | mark cleared only, `Action::RunResumed` |
| `ResumeRun` on a run that is not paused / terminal / unknown | Accepted with no change / `RunTerminal` / `RunNotFound` |
| `PartitionSink` sees `Rejected(RunPaused)` | `AppendError::Fenced` (one appended match arm) |

## Read counts

`PauseRun` 1 and `ResumeRun` 1 (the run record; the resume's `enqueue_ready` is blind). Both are asserted with the `ApplyBatch` read counter in `tests/pause.rs` and `tests/p7_format.rs`. A `PauseRun` rejected during validation reads 0.

## Where the RunPaused checks sit (`apply/run.rs`)

- `apply_events_appended`: MissingToken → run id → RunNotFound → RunTerminal → StaleEpoch → Fenced → **RunPaused** → SeqMismatch → contiguity → CAN routing → intents. A stale zombie still sees `Fenced`, and a paused run never reaches `SeqMismatch` or the CAN path.
- `apply_attempt_granted`: MissingToken → run id → RunNotFound → RunTerminal → StaleEpoch → Fenced (token not newer) → **RunPaused**.
- The gate is inside `enqueue_ready`, after `leave_running`/`remove_ready` and before the row insert.

## rebuild_digest

- `gen_pause` is registered as `Op::Pause` (weight 2) in `op_strategy`, after the 06-03 effects hook. `pause_invariant` runs after `effects_invariant` in `check` and fails if a paused run has a ready slot or a ready row, or if a terminal run is paused.
- New `fixed_scenario_pause`: 24 own runs plus driver ops, with a restart. One instrumented run (instrumentation reverted) saw 29 accepted PauseRun, 10 ResumeRun, 15 RetryRun, 13 `EventsAppended` and 15 `AttemptGranted` rejected `RunPaused`, plus Fenced/StaleEpoch auto-pauses.
- `PROPTEST_CASES=256 cargo test -p ostro-partition --test rebuild_digest --locked`: `test result: ok. 7 passed`, 3414.5 s at load ~23.
- `cargo test -p ostro-partition --features failpoints --test rebuild_digest --locked -- --test-threads=1`: `test result: ok. 7 passed`, 1182 s.

## Verification

- Task 1 verify (`pause`, `apply_core`, `apply_phase4`, `timers`, `apply_timer_input`, plus `p7_format`, `apply_lifecycle`, `apply_waits`, `apply_effects`, `apply_seams`): 15 / 19 / 19 / 7 / 8 / 4 / 18 / 26 / 14 / 9 passed; `cargo build -p ostro-server --locked` Finished.
- Full `cargo test -p ostro-partition --locked` on 0c9840c: 40 binaries, 366 passed, 0 failed (4168 s).
- `cargo build --workspace --all-targets --locked`: Finished, 0 warnings. `cargo clippy --workspace --all-targets --locked -- -D warnings`: clean. `cargo fmt --all --check`: clean.

## Deviations from Plan

1. **[Rule 3 - Blocking] `tests/p7_format.rs` edited (not in files_modified).** 07-03's `pause_and_resume_route_to_not_implemented_stubs` asserted the stubs that this plan replaces, so it would have failed. It is now `pause_and_resume_route_to_pause_rs`: Accepted, 1 read, the RunPaused/RunResumed actions, the real outcome code folded into the hash, and RunNotFound for unknown runs. The other three p7_format tests are unchanged. Commit 9928aaf.
2. **Inputs to a paused run that is still `Running` emit `Action::InputAppended`.** `inputs.rs` is outside files_modified, and the action is in memory only (no ready row, no `EnqueueReady`, no state difference). The dispatcher has revoked the paused run by then (07-05). This is documented in storage-format.md §17.
3. **`fixed_scenario_pause` added** to rebuild_digest.rs, alongside the generator registration, so pause coverage does not depend on random draws. The plan only asked for the registration.
4. **storage-format.md:** 07-03 had already written most of the pause semantics in §17. This plan appended one "Implemented by 07-04" block covering reject order, read budget, inputs while paused, TimedOut-while-paused vs D1/D2 and the sink mapping. Existing text was not edited, so the 07-03 sentence "rejects them with `Invalid(NotImplemented)` ... until 07-04" stays as written, and the new block says the stubs are gone.
5. **Target dir / worktree:** `target/wt-07-04` and a scratchpad worktree, per the orchestrator brief. `bash scripts/ci-local.sh` was not run (shared-load policy, as in 07-03). The gate here is the plan's verify blocks plus the full ostro-partition suite, workspace clippy and fmt.

## Issues / follow-ups

- 07-05: consume `Action::RunPaused` (revoke) / `Action::RunResumed`, and ignore `InputAppended` for revoked runs.
- 07-08: auto-pause submission (`PauseRun` with token, reason 1). Apply already fences it.
- OPS-01 is not marked complete here: the control-plane (07-14) and auto-pause (07-08) parts remain.

## Self-Check: PASSED
- FOUND: crates/ostro-partition/src/apply/pause.rs, crates/ostro-partition/tests/pause.rs (9928aaf); crates/ostro-partition/tests/common/gen_pause.rs (0c9840c)
- FOUND: commits 9928aaf, 0c9840c on main
