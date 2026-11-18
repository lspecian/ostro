---
phase: 02-durable-partition-log-state-machine
plan: 10
subsystem: storage
tags: [apply, outbox, inbound, exactly-once, timers, idempotency, cancel-cascade, cron]
requires:
  - phase: 02-09
    provides: "apply core, start_run/enqueue_ready/load_run/store_run, outbox::emit, Cursor::set_inbound_seen, frozen outcome codes, stub signatures"
provides:
  - "apply_inbound_batch: (src, seq) exactly-once against inbound_seen, whole-batch SeqGap rejection, StartChild/ChildCompleted/Signal/CancelRun/CronStart handlers"
  - "apply_outbox_truncated: blind (zero-read) idempotent outbox truncation"
  - "apply_timer_fired: idempotent TimerFired incl. START_DELAY_OP delayed starts"
  - "pub(crate) inbound::make_ready (Scheduled -> Ready only)"
  - "InvalidCode::BadSource, InvalidCode::TruncateBeyondEnd (appended)"
affects: [02-11, 02-13, 02-14, 02-15]
tech-stack:
  added: []
  patterns:
    - "Structural problems reject the whole InboundBatch before any write; state-dependent misses are per-message drops that still consume the seq"
    - "Timer row is the idempotency token: absent -> Duplicate"
key-files:
  created:
    - crates/ostro-partition/tests/apply_cross.rs
  modified:
    - crates/ostro-partition/src/apply/inbound.rs
    - crates/ostro-partition/src/apply/timer.rs
    - crates/ostro-partition/src/apply/outbox.rs
    - crates/ostro-partition/src/apply/outcome.rs
    - crates/ostro-partition/src/apply/mod.rs
    - crates/ostro-partition/src/apply/cursor.rs
decisions:
  - "InboundBatch whole-command rejections (no writes): src outside count -> Invalid(BadSource); msg/kind absent -> Invalid(MissingField); target partition != this partition -> WrongPartition; unparsable target -> Invalid(BadRunId); oversize inline payload -> PayloadTooLarge; ChildCompleted with bad child id / non-terminal status -> Invalid(BadRunId/BadIntent); seq gap -> SeqGap{expected, found}. Empty or all-seen batch -> Duplicate"
  - "Per-message drops (seq consumed): target run unknown/terminal, cancel already requested, StartChild/CronStart outcomes other than Accepted (start_run validates before writing, so a rejected start writes nothing)"
  - "make_ready ready_at: inbound messages use the run's created_at_ms; TimerFired START_DELAY_OP uses due_at_ms (= requested ready_at); other timers use fired_at_ms"
  - "A signal/child completion/cancel to a Scheduled (delayed-start) run wakes it early; the later START_DELAY_OP fire then only removes its timer row"
  - "OutboxTruncated: upto <= truncated -> Duplicate; upto >= next_seq (incl. targets never emitted or > u16) -> Invalid(TruncateBeyondEnd); else blind remove truncated+1..=upto"
  - "TimerFired on a missing (orphan) run or a terminal run: Accepted, timer row removed, nothing else"
  - "Cron dedupe row: dedupe_key(\"__cron/<schedule_id>\", fire_time_ms BE) -> run id; the run's own conflict_policy decides Duplicate vs IdempotencyConflict, both are drops"
metrics:
  duration: 20min
  completed: 2026-11-18
---

# Phase 2 Plan 10: Cross-Partition and Timer Apply Summary

This plan fills in the three stub bodies left by 02-09, completing the Phase 2 apply command set.

- **Inbound messages are applied exactly once.** Each `InboundBatch` is deduplicated per `(src, seq)` against `inbound_seen`, and a gap rejects the whole batch.
- **All five message kinds are handled.** These are child start, child completion, signal, cancel (which cascades to every child through the outbox) and cron start. A cron start runs only once per `(schedule, fire_time)`.
- **Outbox truncation** deletes rows without reading them and can be repeated safely.
- **`TimerFired` is idempotent**, including the `START_DELAY_OP` timer used for delayed starts.

## What was built

- **`apply/inbound.rs`:**
  - `apply_inbound_batch` checks the whole batch first: the source is inside the partition count, each message has a body, each message is routed to this partition, no inline payload is oversize, and each `ChildCompleted` is well formed.
  - It then skips the leading messages with `seq <= seen` and requires the rest to be exactly `seen+1, seen+2, …`.
  - It applies each new message and calls `cur.set_inbound_seen(src, last)`.
  - Handlers:
    - `StartChild` reuses `start_run(dedupe=None)`.
    - `CronStart` reuses `start_run(dedupe=("__cron/<id>", fire_time BE))`.
    - `ChildCompleted` writes an inbox entry, removes the children row, decrements `children` and makes the parent ready.
    - `Signal` writes an inbox entry and makes the run ready.
    - `CancelRun` sets `cancel_requested`, writes an inbox entry, makes the run ready, pushes `Action::CancelRequested`, and emits a `CancelRun` for each child found by `range_prefix(Children, children_prefix(run))`.
  - `make_ready` is `pub(crate)` and moves only a Scheduled run to Ready.
- **`apply/outbox.rs`:** `apply_outbox_truncated` keeps the per-target watermark in `CursorMeta.outbox_truncated` and deletes rows blindly.
- **`apply/timer.rs`:** `apply_timer_fired` reads the timer row once. If the row is absent the result is Duplicate. If it is present, the row is removed and the run is read. A terminal run gets no further effect. `START_DELAY_OP` moves the run to Ready at `due_at_ms`. Any other op writes an inbox `TimerFired{op, fired_at_ms}` and makes the run ready.
- **`apply/outcome.rs`:** appends `InvalidCode::BadSource` and `InvalidCode::TruncateBeyondEnd` at the end of the enum. Both still hash as code 26, and the frozen-table doc comment is extended.
- **`apply/mod.rs`:** the doc now states the read budgets for `TimerFired`, `OutboxTruncated` and `InboundBatch`.

## Verification

- `cargo test -p ostro-partition --test apply_cross`: 16 tests pass.
  - inbound: `inbound_outbox_exactly_once`, `inbound_gap` (includes a gap in the middle of a batch), `inbound_validation`, `inbound_signal_unknown_run`, `inbound_signal_wakes_scheduled_run`, `inbound_child_roundtrip`, `inbound_cancel_cascade`, `inbound_cron_once`, `inbound_seen_persists`
  - outbox: `outbox_truncate`, which asserts 0 reads
  - timer: `timer_fire_once`, `timer_cancelled`, `timer_delayed_start`, `timer_delayed_start_after_early_wake`, `timer_terminal_run`, `timer_reads_and_validation`
- `cargo test -p ostro-partition`: all suites pass, including `apply_core` (19).
- `cargo clippy -p ostro-partition --all-targets -- -D warnings` is clean, both with and without `--features failpoints`.
- `cargo fmt --all --check` is clean.
- `grep -n NotImplemented crates/ostro-partition/src/apply` finds only the enum definition, so no command can reach it.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed `#[allow(dead_code)]` from `Cursor::set_inbound_seen`**
- `cursor.rs` is not in the plan's `files_modified`, but the 02-09 summary asked for this, and the function is now used.
- **Files modified:** crates/ostro-partition/src/apply/cursor.rs
- **Commit:** 5a9be9c

**2. [Rule 2 - Correctness] Extra whole-batch structural checks on `InboundBatch`**
- The plan specified only the src and body checks. I also added a routing check (WrongPartition), an oversize-payload check and a `ChildCompleted` well-formedness check. Without them, a misrouted or malformed message would be silently consumed or would reach a handler's internal `Codec` error path. All of these use existing outcome codes.
- **Commit:** 5a9be9c

## Notes for later plans

- **02-13 (shuffler):** read the source's `outbox` rows for a target in seq order, then apply `InboundBatch{src, msgs}` on the target. After the target has applied it, apply `OutboxTruncated{target, upto}` on the source. Overlapping resends are safe. A `SeqGap` means rows were skipped, which is a shuffler bug.
- **02-14 (timer service):** a re-fire after leader change is safe because it returns Duplicate. `fired_at_ms` must come from the service, since apply never reads the clock. `CompleteRun` leaves timer rows in place, and firing them only removes the rows.
- **`values.rs` doc:** `CursorMeta.outbox_truncated` says "rows below this seq". Apply treats it as "rows `<=` this seq have been truncated". The doc comment was not changed because the file is outside this plan's scope.

## Requirements

None are marked complete. STOR-12's apply half (exactly-once inbound plus truncation) is done, and the shuffler is 02-13. STOR-13's apply half (idempotent `TimerFired`) is done, and the lazy windowed timer service is 02-14.

## Commits

- f9c010a test(02-10): add failing inbound/outbox cross-partition apply tests
- 5a9be9c feat(02-10): exactly-once InboundBatch apply, cancel cascade, cron dedupe, outbox truncation
- e2edd6a test(02-10): add failing idempotent TimerFired tests
- 2d7cad6 feat(02-10): idempotent TimerFired apply incl. delayed starts

## Self-Check: PASSED

Created files exist (apply_cross.rs, 02-10-SUMMARY.md); commits f9c010a, 5a9be9c, e2edd6a, 2d7cad6 present.
