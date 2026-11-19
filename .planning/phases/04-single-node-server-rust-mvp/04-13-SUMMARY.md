---
phase: 04-single-node-server-rust-mvp
plan: 13
subsystem: partition-apply
tags: [apply, timers, journal, wake-up, suspend, cas, input-appended]
requires:
  - phase: 04-12
    provides: Action::InputAppended, Suspend CAS, Running keyspace, query::journal_rows
  - phase: 02-14
    provides: timer service + TimerFired apply (idempotent via the timer row)
provides:
  - TimerFired for a run's timer op journaled as a TimerFired event at next_seq (ts = fired_at_ms * 1e6), next_seq bumped, no inbox row
  - Wake by status (Scheduled -> Ready/EnqueueReady, Running -> Action::InputAppended{run, seq}, Ready -> event only)
  - pub(crate) apply::run::put_event (one journal-row writer, shared with EventsAppended)
  - docs/storage-format.md §12 TimerFired semantics bullet
affects: [04-14, 04-15, 04-16, 04-20, 05-01]
tech-stack:
  added: []
  patterns:
    - "Inputs are journal events written by apply at next_seq; the from_seq == next_seq check on every EventsAppended is the CAS against lost wake-ups"
key-files:
  created:
    - crates/ostro-partition/tests/apply_timer_input.rs
  modified:
    - crates/ostro-partition/src/apply/timer.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-partition/tests/timers.rs
    - crates/ostro-partition/tests/apply_cross.rs
    - crates/ostro-partition/tests/rebuild_basic.rs
    - docs/storage-format.md
key-decisions:
  - "Timer journal event stored as prost JournalEvent{seq, kind 64, payload: inline(encode_event bytes)}, exactly the row format EventsAppended writes and query::journal_rows decodes"
  - "Ready-at for a woken Scheduled run stays fired_at_ms (unchanged from Phase 2)"
  - "next_seq bump uses saturating_add (u64 seq exhaustion is unreachable; apply stays total)"
  - "InboxMsg::TimerFired kept (postcard variant index stability), documented as no longer written"
  - "Cancel-while-Running interim fix NOT applied: Phase 4 has no cancel path (no API/command emits CancelRequest in 04-12/16/18); left to Phase 5, see Known gaps"
requirements-completed: []
duration: 35min
completed: 2026-11-19
---

# Phase 4 Plan 13: TimerFired journaled as input + wake Summary

**When a run's timer fires, apply now writes a `TimerFired { op }` journal event at `rec.next_seq`, with `ts_nanos = fired_at_ms * 1e6`, and bumps `next_seq`. It no longer writes an inbox row. It then wakes the run: a Scheduled run becomes Ready (`EnqueueReady`), a Running run gets `Action::InputAppended{run, seq}`, and a Ready run only gets the event. Because `next_seq` moves, a Suspend built before the input was seen fails its `from_seq` CAS with `SeqMismatch`, so the wake-up is not lost.**

## Performance

- **Duration:** about 35 min, plus the ci-local run
- **Tasks:** 1 (TDD)
- **Files:** 1 created, 7 modified

## Accomplishments

- `apply/timer.rs`: in the branch for timer ops other than the start-delay op, apply now encodes `Event{seq: next_seq, ts_nanos, body: TimerFired{op}}` with `ostro_types::journal::encode_event`. It stores the result through `put_event` as `JournalEvent{seq, kind: 64, payload: inline}`, bumps `next_seq`, and then matches on status:
  - Scheduled → `make_ready`, which emits `EnqueueReady` with ready_at = fired_at_ms.
  - Running → `Action::InputAppended{run, seq}`. The `running` row is untouched.
  - Ready → no action.
- The start-delay op (`u64::MAX`), terminal runs, absent runs and the Duplicate re-fire behave as before.
- The read budget is unchanged: at most 2 reads (the timer row, then the run record). No clock is read, since `fired_at_ms` comes from the command.
- `apply/run.rs`: added `pub(crate) fn put_event(b, run, &JournalEvent)`. `apply_events_appended` uses it. This is the only edit to run.rs.
- `store/values.rs`: added a doc comment on `InboxMsg::TimerFired` saying it is no longer written. The variant is kept.
- `docs/storage-format.md`: one new §12 apply-rules bullet covering TimerFired journaling, the wake paths and SeqMismatch. The §6 inbox row now notes that the variant is no longer written.

## Tests

- **New** `tests/apply_timer_input.rs`, 8 tests:
  - Scheduled → Ready, with the journal row decoded to `TimerFired{op: 7}` and ts 1_000_000_000.
  - Running → `InputAppended{seq: 1}`, running row unchanged, then a continuation from seq 2.
  - Ready → event only, ready row kept.
  - Terminal run and duplicate keep the Phase 2 semantics.
  - A duplicate fire on a live run journals once.
  - A start-delay fire writes no journal event.
  - **Lost wake-up race:** flush to N=2, the timer journals seq 2, then a Suspend at from_seq 2 is rejected `SeqMismatch{expected: 3}` and the tables are unchanged. A Suspend at 3 is accepted.
  - Determinism: the same commands on two stores give the same digest and journal.
- **Updated, only what the behavior change required:**
  - `tests/timers.rs`: the `inbox_fired` helper became `journal_fired`. It reads `TimerFired` events through `query::journal_rows`, decodes them, and asserts that the inbox holds no timer. `duplicate_timer_fired_has_no_effect` now asserts `next_seq == 2` and `inbox_next == 0` instead of `inbox_next == 1`.
  - `tests/apply_cross.rs::timer_fire_once`: now expects `[InputAppended{seq: 0}]` instead of no actions, an empty inbox, the decoded journal event, and `next_seq == 1`.
  - This file is not listed in the plan's `files_modified`, but it is the Phase 2 apply-level timer test and asserted the old inbox behavior.
- **`tests/common/mod.rs` was not changed.** Its `inbox`, `signal_names` and `cancel_count` helpers only serve signals and cancels, and no timer case used them.
- **`rebuild_digest` stays green**, including the proptest and both fixed scenarios. Its driver model still sees more `SeqMismatch` rejections, as 04-12 predicted.

## Verification

- `cargo test -p ostro-partition --locked`: all suites green.
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`: clean, with and without `--features failpoints`. `cargo fmt --all --check`: clean.
- `bash scripts/ci-local.sh --no-musl` at `177b787`, in a detached scratch worktree with a private target dir:
  - PASS: toolchain, fmt, clippy, warning-free build, `cargo test` (577 s), deny, check-vendored, deny-fixtures, forbidden-deps and workflow validation. The 3 musl legs SKIP (`--no-musl`).
  - The failpoint step failed once on `shuffler_exactly_once::exactly_once_under_flaky_acks_and_restarts`: `node open timed out` (10 s `Node::open` timeout) with load average 12, while three executors were building in parallel. That test involves no timers.
  - Re-run alone with `--features failpoints -- --test-threads=1`: 3/3 green.
  - The failpoint step was then re-run for the crates the abort skipped:
    - ostro-partition: `cargo clippy --features failpoints --all-targets -D warnings` clean, and `cargo test --features failpoints -- --test-threads=1` exit 0 (21 suites ok).
    - ostro-crashtest: clippy clean, tests exit 0.
    - ostro-wal, ostro-blob and ostro-log had passed inside the ci-local run.
  - **Net: all steps green; one load-induced flake re-ran clean.**

## Task Commits

1. **Task 1 RED: failing tests**: `c49df7a` (test)
2. **Task 1 GREEN: journal TimerFired inputs and wake the run**: `6fa5167` (feat)
3. **Rule 1 fix: rebuild_basic follows SeqMismatch**: `177b787` (fix)

## Deviations from Plan

1. **[Rule 3 - Blocking] `tests/apply_cross.rs::timer_fire_once` updated.** The plan did not list this file, but it pinned the old inbox behavior. Only that test changed.
2. **`tests/common/mod.rs` unchanged.** The plan listed it "for timer cases", but there were none.
3. **`store/values.rs` doc comment** on the now-unwritten `InboxMsg::TimerFired`. Documentation only.
4. **[Rule 1 - Bug] `tests/rebuild_basic.rs` became timing-dependent.** The ci-local failpoint step caught it.
   - Its workload schedules timers due at 5_000 ms. On the real clock those are overdue, so the timer service can journal `TimerFired` at seq 1 before the test's completing `EventsAppended{from_seq: 1}`, which is then rejected `SeqMismatch{expected: 2}`.
   - Fix: the completing append follows `SeqMismatch` to `expected`. The workload's shape is unchanged.
   - Commit `177b787`.
5. **Verification target dir.** A first ci-local run with the shared `CARGO_TARGET_DIR=/home/lspecian/Development/ostro/target` showed `apply_timer_input` failing with RED-state results while the same tests passed in isolation.
   - The likely cause: ostro-partition artifacts are shared between worktrees with the same metadata hash, and a parallel executor building an older checkout (pre-04-13 `timer.rs`) overwrote them.
   - The final verification used a private target dir in the scratchpad.

## Known gaps (not fixed, out of scope)

- **Cancel while Running vs. Suspend:** the interim fix (in the Suspend arm, if `rec.cancel_requested` then enqueue ready) was **not applied**. Phase 4 has no cancel path: none of 04-12, 04-16 or 04-18 adds a cancel API or command, and `CancelRequest` only arrives through cross-partition `InboundBatch`, which no Phase 4 component emits. Phase 5 (05-01 `RequestCancel`, Cmd 9) must close it, either with that cheap fix or by journaling cancel requests.
- Signals and child results still go to `inbox` and do not bump `next_seq`. That is Phase 5's job.
- `ostro-crashtest`'s `crash-target` schedules timers that are due now or overdue, and marks a run done on any rejected append. After 04-13 a fired timer moves `next_seq`, so some runs stop early with `SeqMismatch`. Coverage drops a little but correctness is unaffected, since the crash loops compare rebuilt state. The crash loops are `#[ignore]` and were not run here. Cheap follow-up: on `SeqMismatch{expected}`, set `next_seq = expected` and keep the run alive.

## Notes for later plans

- **04-16 (executor):**
  - **On a Suspend `SeqMismatch`, never re-submit the Suspend at the updated `sink.next_seq()`.** That would accept a Suspend over an unconsumed input and lose the wake-up. Re-activate instead (`ExecOutcome::Reactivate`), or feed the input to the hot activation first. The same applies to any `EventsAppended` from an activation that has not seen the input.
  - Input bridge: forward `job.inputs` seqs by reading and decoding the Events row (`query::journal_rows(r, run, seq, 1)` then `decode_event`). **Skip seqs below the end of the loaded journal.** An input journaled after `AttemptGranted` but before the journal load is both in the loaded journal and possibly forwarded.
  - An input journaled before the dispatcher registered the lease is dropped by the dispatcher, which ignores the forward when no lease exists. The journal and the from_seq CAS still cover it: the activation's next append gets `SeqMismatch`, so it must re-activate.
  - Timer journal events are prost `JournalEvent{kind 64, payload: Inline(encode_event bytes)}`. `ts_nanos` is a whole number of ms × 1e6.
- **04-14 (dispatcher):** apply now emits `InputAppended` from `TimerFired` only, and only for Running runs. It is never emitted together with `EnqueueReady` for the same command.
- **04-20 (e2e):** the det-basic and SDK sleeps now resolve through the journal. The start delay still goes through `START_DELAY_OP` with no journal event.
- **05-01:** no ordinals allocated. The free tags are unchanged (`Derived` 7, `StartRun` 19, `Cmd` 8). Journal kind 64 (TimerFired) already existed.

## Self-Check: PASSED

- FOUND: crates/ostro-partition/tests/apply_timer_input.rs
- FOUND: crates/ostro-partition/src/apply/timer.rs (`encode_event`, `InputAppended`)
- FOUND: crates/ostro-partition/src/apply/run.rs (`pub(crate) fn put_event`)
- FOUND: docs/storage-format.md §12 TimerFired bullet
- FOUND: commits c49df7a, 6fa5167, 177b787
