---
phase: 05-workflow-semantics-core-hardening
plan: 23
subsystem: engine-replay
tags: [replay, logical-time, step-retry, d22, wf-03, wf-02]

requires:
  - phase: 05-workflow-semantics-core-hardening
    provides: "05-03 replay core (completions index, patched memo, skip_unqueried_markers); 05-06 SDK test-file ownership in wave 2"
  - phase: 04-single-node-server-rust-mvp
    provides: "04-04 StepAttemptFailed kind 29 with retry_at_nanos + attempt chains; 04-07 host step retry (retry-now / StepBackoff); 04-20 finding 1 (long_backoff failed twice)"
provides:
  - "D22 retry logical clock: a step attempt that re-runs after a StepAttemptFailed observes now = max(last journaled ts, retry_at_nanos); monotonic, journal-derived, identical on replay"
  - "crates/ostro-engine/tests/step_retry_clock.rs: engine-level sdk-retry long_backoff / recover_hot under D22 + replay-core identity tests"
  - "sdk-retry long_backoff fails exactly once (1 StepAttemptFailed, success on attempt 2)"
affects: [05-12, 05-17, phase-6-sdks]

tech-stack:
  added: []
  patterns:
    - "Raise, never set: logical time is raised to retry_at with max(), so it cannot go backwards"
    - "Raise only when due: begin_step raises the clock for a resumed requeued attempt only if retry_at <= live now, so the host's StepBackoff re-eviction check (retry_at > live_now) is unchanged"

key-files:
  created:
    - crates/ostro-engine/tests/step_retry_clock.rs
    - .planning/phases/05-workflow-semantics-core-hardening/05-23-SUMMARY.md
  modified:
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/mod.rs
    - crates/ostro-engine/src/host/context.rs
    - crates/ostro-engine/tests/sdk_guests.rs
    - crates/ostro/tests/e2e_dev.rs
    - .planning/ROADMAP.md

key-decisions:
  - "D22 lives entirely in ReplayState: record_attempt_failed raises last_cmd_ts to the failure's retry_at_nanos right after appending it (in-activation retry: the host then sleeps until retry_at); begin_step raises it after consuming a requeued chain that is not closed by a completion"
  - "begin_step raises only when the attempt is due (retry_at <= live_now_nanos()). A not-yet-due attempt is handed back by the host without running its body, so the raise would only have hidden the backoff. When due, retry_at <= live_now already, so live_now (and therefore the host check and every live timestamp) is unchanged by the raise"
  - "The raise is folded into last_cmd_ts, so logical_now_nanos, live_now_nanos (retry decisions), append_live stamps and uuid_v7 all see it with no new field. A fully recorded chain is consumed through its StepCompleted, whose recorded ts then defines now exactly as before, so pre-D22 journals replay byte-identically"
  - "host/journal.rs untouched (not in files_modified): the due-check trick made a host change unnecessary"

patterns-established:
  - "Retry logical clock (D22)"

requirements-completed: [WF-03]

duration: 110min
completed: 2026-11-19
---

# Phase 5 Plan 23: D22 retry logical clock Summary

**A step attempt that re-runs after a `StepAttemptFailed` now sees logical time `max(last journaled ts, retry_at_nanos)`. The clock is raised inside `ReplayState` from kind 29's existing `retry_at_nanos` (no field, no `EVENT_CODEC_VER` bump, no golden touched). It is monotonic and identical on replay, so sdk-retry `long_backoff` fails exactly once and succeeds on attempt 2.**

## Performance

- **Duration:** about 110 min, most of it full-gate runs on a machine at load average 17-21
- **Started:** 2026-11-19T07:55:13Z
- **Completed:** 2026-11-19T09:45Z
- **Tasks:** 2 (TDD: RED commit, GREEN commit, expectation fixes)
- **Files modified:** 6 source/test files, plus SUMMARY and ROADMAP

## Accomplishments

- **Engine rule (cursor.rs).**
  - `record_attempt_failed` appends the SAF, then raises `last_cmd_ts` to `max(ts, retry_at)`. The in-activation next attempt runs after the host's sleep, sees `retry_at` and is stamped at least that.
  - `begin_step`, after consuming a requeued chain with no completion, raises the clock to `last_retry_at_nanos` only if it is due.
  - New private helper `raise_to_retry_at`.
  - Module, struct and `logical_now_nanos` docs describe D22, and so do the `replay/mod.rs` and `host/context.rs::now` docs.
- **`step_retry_clock.rs` (6 tests).**
  - `long_backoff_fails_once_and_sees_retry_at`: TestClock. Attempt 1 at T0 is requeued with retry_at T0+3s. At T0+1s the attempt re-evicts with zero appends, proving the raise does not hide the backoff. At T0+3s attempt 2 succeeds with step output `3000` (ms elapsed), giving exactly 1 SAF, a completion at or after retry_at and monotone journal ts. Replays (clock +1 h, and a fresh runtime) append 0 and give the identical result.
  - `hot_retries_are_stamped_at_retry_at`: recover_hot with fail_times 2 on a frozen clock. SAF2 is stamped at retry_at1 (T0+50ms), retry_at2 is T0+150ms, and the completion is stamped at T0+150ms. The replay appends 0.
  - Replay core:
    - `requeued_attempt_now_is_retry_at_and_replay_matches` gives identical `now()` before and after the step, live and on replay, with different node clocks.
    - `retry_at_before_last_ts_keeps_last_ts`.
    - `attempt_not_due_does_not_raise_the_clock`.
    - `hot_retry_now_is_retry_at` covers a lagging node clock.
- **Expectation fixes.**
  - `crates/ostro/tests/e2e_dev.rs`: in `kill9_mid_run_restart_completes_and_metadata_persists`, `step_attempt_failed` count for `slow` 2 -> **1**. Reason: under D22, requeued attempt 2 sees elapsed >= 3 s. The comment is rewritten.
  - `crates/ostro-engine/tests/sdk_guests.rs`: doc pointer only. No long_backoff assertion existed there; the engine-level long_backoff/recover_hot coverage lives in `step_retry_clock.rs`.
  - `crates/ostro-server/tests/sdk_e2e.rs`: **no change needed**. No test there runs `long_backoff`. `retry_exhaust` (exact 50/100/200 ms `retry_at - ts`, each retry at or after retry_at), `retry_recover_hot` and `retry_restart_between_attempts` stay green under D22, because the retry decision reads `live_now_nanos`, which includes the raise, so `retry_at - ts` stays exactly the backoff. 05-07's two ActiveVersion literals were not touched.

## Task Commits

1. **Task 1 RED: failing D22 tests**: `177eee1` (test)
2. **Task 1 GREEN: engine rule**: `e7997a7` (feat). Also includes the rustfmt of the new test file.
3. **Task 2: long_backoff expectations**: `d3d8dc5` (test)

**Plan metadata:** the docs commit with this SUMMARY and ROADMAP.

## Verification

- At `d3d8dc5`, in a detached scratch worktree:
  - `cargo test -p ostro-engine --locked`: every suite green, including step_retry_clock 6, step_retry 6, determinism 10, sdk_guests 7, patching 4, sdk_cancellation 8 and lib unit 83.
  - `git diff --name-only crates/ostro-engine/tests/snapshots` is empty.
  - ostro-server `sdk_e2e` 9/9 and ostro `e2e_dev` 3/3 green.
  - `cargo clippy -p ostro-engine --all-targets -D warnings` is clean. `cargo fmt --check` is clean.
- `bash scripts/ci-local.sh --no-musl` at `d3d8dc5` (load average 17-21, with three other executors building):
  - PASS: toolchain, fmt, clippy -D warnings, warning-free build, sdk workspace fmt/clippy/test/wasm, guests build, cargo deny, check-vendored, deny fixtures, no chronik/tokio-uring/prometheus, workflow validation.
  - FAIL `cargo test`: only `ostro --test dev_startup warm_restart_under_1s` (warm restart took 6.6 s under load). Rerun with `cargo test --workspace --no-fail-fast`: 124 suites ok, the only failure was again dev_startup. Rerun alone twice: 1151 ms and 1075 ms against the 1 s budget, at load average about 17. This is the known load flake. Its path, a warm `ostro dev` restart with no runs, never executes replay code.
  - FAIL `failpoint suites`: `ostro-log fatal_io` (2 tests: role still `Leader` when health became `Failed`, a timing race under load). ostro-wal and ostro-blob passed before it. Rerun alone with `cargo test -p ostro-log --features failpoints -- --test-threads=1`: all green, including fatal_io. The step stopped at ostro-log, so ostro-partition and ostro-crashtest failpoints were run separately: all green. ostro-log does not depend on ostro-engine.
- Re-verified at the later HEAD `1a6775c`, after the 05-10 guests/engine guest suite and the 05-24 commits landed: `cargo test -p ostro-engine` has 24 suites ok and 0 failed, ostro-server `sdk_e2e` is 9/9 and ostro `e2e_dev` is 3/3.

## Decisions Made

See `key-decisions` in the frontmatter.

## Deviations from Plan

### Auto-fixed Issues

None. The plan's key_link put the raise in `host/context.rs`. It is in `ReplayState` instead, which `context.rs::now` reads; `context.rs` gets the D22 doc only. That is the "Find where logical_now is computed ... apply the max rule" action taken literally. `host/journal.rs` (outside files_modified) needed no change.

### Scope notes

- sdk_guests.rs and sdk_e2e.rs contained no long_backoff expectation, so the "fails exactly once in all three files" truth is enforced by e2e_dev.rs (binary e2e) and step_retry_clock.rs (engine). sdk_e2e.rs is unchanged.

## Issues Encountered

- The session scratchpad is shared with parallel executors. My first worktree, `scratchpad/wt`, picked up untracked 05-10 guest directories and a modified copy of my test file from another agent. I moved to `scratchpad/wt-05-23` and left `scratchpad/wt` in place, since another agent may be using it.
- zsh does not word-split `$F`; commits were run under `bash -c`.

## STATE notes (for the orchestrator; no gsd-tools mutators run)

- Phase 5: 05-23 complete (wave 3). D22 is implemented. WF-03 (retry logical clock part) is done; the Phase 4 WF-02 long_backoff expectation is corrected.
- Decision: D22 lives in ReplayState (raise last_cmd_ts to retry_at on record_attempt_failed, and on begin_step only when due); no host, codec or golden change.
- Metric: Phase 05 P23, about 110 min, 2 tasks, 6 files.

## Hand-off

- **05-12 (corpus, records sdk-retry entries).**
  - Record after `e7997a7`. A `long_backoff` journal now has **one** `StepAttemptFailed(slow, attempt 1, Requeue, retry_at = ts + 3 s)` followed by `StepCompleted(slow, Ok("3000"-ish elapsed ms))`, with the completion ts at or after retry_at.
  - A `recover_hot` journal stamps each later SAF/completion at least the previous retry_at, so with a frozen clock the ts are exactly the retry_at values.
  - Replay of such journals is identical with any node clock, because nothing in the rule reads the clock except the "due" gate, which only decides whether the body runs live at all.
  - Pre-D22 journals (two SAFs for long_backoff) still replay byte-identically: a closed chain is consumed through its recorded StepCompleted ts. Do not mix them into a D22 corpus as "expected live behaviour", though.
- **Timestamps.** A live run's completion after a requeued retry can carry a ts slightly ahead of the node clock only if the node clock lags retry_at. That cannot happen on the requeue path, because of the due gate. On the hot path the host sleeps the full delay first.
- `is_replaying()`, `next_command_hash_ver()` and `patched()` are untouched (05-03 hand-off honoured).
