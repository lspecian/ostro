---
phase: 02-durable-partition-log-state-machine
plan: 14
subsystem: storage
tags: [timers, lazy-window, timer-fired, retention, trim, readiness-barrier, checkpoint, leader-services]
requires:
  - phase: 02-08
    provides: "ReadyBarrier::mint / TrimPermit, Rf1Log::trim_prefix (whole segments below floor, clamped to HW)"
  - phase: 02-10
    provides: "Idempotent TimerFired apply (row absent = Duplicate), delayed start via START_DELAY_OP"
  - phase: 02-11
    provides: "MachineHandle::{subscribe_actions, force_checkpoint, durable_checkpoint, check_healthy}"
  - phase: 02-12
    provides: "Node, LeaderCtx, leader::{timers,retention}::spawn stubs with fixed signatures, ManualClock via NodeConfig.clock"
provides:
  - "ostro_partition::leader::timers::{spawn, TimerConfig (+stats), TimerStats}: lazy windowed timer service"
  - "ostro_partition::leader::retention::{spawn, RetentionConfig (+stats), RetentionStats, trim_floor}: barrier-gated retention"
  - "tests/timers.rs (7 incl. hundred_thousand_timers_bounded_window_large), tests/trim_safety.rs (4)"
affects: [02-15, 02-16, 04-runtime, 08-snapshots]
tech-stack:
  added: []
  patterns:
    - "Timer window = BTreeSet<(due, RunId, op)> + horizon; invariant: every timer row with due < horizon is loaded; truncated scan -> horizon = last due (rows at the horizon may be partial)"
    - "Refill = raw due-prefix range scan [0u64 BE, (now+span) BE) over Table::Timers, limit window_max; only when horizon == 0 or (now + span/2 >= horizon and window has room)"
    - "Action-driven window maintenance; broadcast Lagged -> horizon = 0 (forced refill); stale re-inserts are harmless because apply answers Duplicate"
    - "Retention pass = force_checkpoint -> floor = min(ckpt, snapshot_floor, retention_floor) -> barrier.mint -> trim_prefix, info log every pass"
key-files:
  created:
    - crates/ostro-partition/tests/timers.rs
    - crates/ostro-partition/tests/trim_safety.rs
  modified:
    - crates/ostro-partition/src/leader/timers.rs
    - crates/ostro-partition/src/leader/retention.rs
decisions:
  - "Horizon boundary rule: on a truncated refill keep all loaded rows and set horizon = last row's due (the invariant only covers due < horizon), so a window full of one shared due still makes progress; ScheduleTimer actions with due >= horizon are left to the next refill"
  - "Refill is skipped while the window is full: rows below the horizon are all loaded, so a rescan could not add anything (avoids a 50 ms rescan loop when window_max rows are pending but not due)"
  - "TimerStats.fired counts Accepted only; Duplicate is counted separately. Rejected TimerFired is logged and dropped from the window (a later refill retries the row)"
  - "Submit/refill errors stop the timer service only if the partition is unhealthy; otherwise warn and retry after 50 ms (e.g. WAL backpressure)"
  - "Retention: NotReady from mint skips the pass (counted in RetentionStats.not_ready); checkpoint failure stops the service; trim failure stops it only if the partition is unhealthy"
metrics:
  duration: 25min
  completed: 2026-11-18
---

# Phase 2 Plan 14: Lazy Windowed Timers and Barrier-Gated Retention Summary

The leader now runs two more services. The **timer service** keeps a bounded, lazily refilled window of the `timers` table and fires due timers as batches of `TimerFired` (fired_at from the leader clock). Because apply removes the row on the first fire and answers `Duplicate` afterwards, a timer takes effect once even if fired twice. The **retention service** forces a durable checkpoint, mints a `TrimPermit` from the readiness barrier, and trims only whole segments below `min(checkpoint, snapshot_floor, retention_floor)`. With the Phase 2 default `snapshot_floor = 0` it deletes nothing.

## What was built

- **leader/timers.rs**
  - `Window { set: BTreeSet<(due, RunId, op)>, horizon, max }`. A refill scans `Table::Timers` from due 0 up to `now + window_span_ms` (limit `window_max`), so overdue timers are always included.
  - `ScheduleTimer`/`CancelTimer` actions keep the window current. If the window overflows, its largest entry is popped and `horizon` drops to that entry's due. A lagged action stream forces a refill.
  - Fire loop: up to `fire_batch` due entries go into one `submit`. Each entry is removed once its outcome arrives. When nothing is due, the loop sleeps `min(earliest - now, 50 ms)` or wakes early on an action or shutdown.
  - Metrics: `ostro_timer_window_entries` (gauge) and `ostro_timers_fired_total`. Optional `TimerStats`: `window_len`, `max_window_len`, `fired`, `duplicates`, `refills`.
  - Two unit tests cover the horizon/bound rule and the truncated reset.
- **leader/retention.rs**
  - One pass every `interval` (the first after one interval); each pass is logged with checkpoint, floor and new_start, including "nothing to trim".
  - `trim_floor(ckpt, cfg)` is public and unit-tested.
  - Optional `RetentionStats`: `passes`, `not_ready`, `last_checkpoint`, `last_floor`, `last_log_start`.
- The spawn signatures and config defaults from 02-12 are unchanged. Only the optional `stats` field was added to each config, and both configs keep `Default`.

## Verification

- `cargo test --release -p ostro-partition --test timers --test trim_safety`: 7 + 4 green.
  - The 100k test takes about 11 s in release, and the timers suite takes about 52 s in debug.
- **timers.rs**
  - fire once, after which the row is gone
  - a cancelled timer never fires
  - a delayed start goes Scheduled → Ready, with a ready row
  - an overdue timer fires exactly once after a restart, with `fired_at = now`
  - a manual duplicate `TimerFired` returns `Duplicate` and the inbox still holds 1 entry
  - 25 timers sharing the cut-off due with `window_max` 10 all fire
  - 100 000 timers over 2 h with `window_max` 1 000: `max_window_len` stayed ≤ 1 000, `fired == 100 000`, the Timers table ended empty, and the inbox holds 100 000 distinct ops
- **trim_safety.rs**
  - Inert default: 3 passes, `log_start` stays 0, all 5 segments kept.
  - Injected floor in the middle of segment 3: segments 1–2 are deleted and `log_start = ends[1]`. Every offset from `log_start` up to HW still reads back, the digest is unchanged, and checkpoint ≥ floor.
  - `snapshot_floor = MAX`: floor == checkpoint, and only the active segment remains.
  - Deleting the state dir and reopening then gives `Failed("snapshot required…")`.
  - Inspect mode: no pass runs, `mint` returns NotReady, and no segment is deleted.
- `cargo clippy -p ostro-partition --all-targets -- -D warnings`: no warnings from this plan's files. The only error comes from 02-13's in-progress `tests/shuffler_exactly_once.rs`, which has a `let_unit_value` issue at line 223.
- rustfmt `--check` is clean on all four files.
- Whole crate: every suite passes except `rebuild_basic` (see Deferred Issues).

## Deviations from Plan

### Auto-fixed Issues

None — no bugs needed fixing. Design choices within the plan's latitude:

- **Refill range bound:** the scan uses 8-byte due prefixes (`0u64.to_be_bytes()` .. `reach.to_be_bytes()`) instead of `timer_key(x, MIN_RUN, 0)`. This avoids building a sentinel `RunId`, and the key order is the same.
- **Horizon boundary rule:** the plan offered two options; this plan keeps the rows at the horizon and sets `horizon = last_due`. The `timers_sharing_the_cutoff_due_all_fire` test covers it.

### TDD note

For both tasks the tests and the implementation were written in the same step and committed together, so there is no separate RED commit. The stubs could not have passed any behaviour, because they never fire or trim.

## Deferred Issues (out of scope, caused by concurrent 02-13)

- `tests/rebuild_basic.rs` now fails at line 146 with `router.deliver(..)` returning `Duplicate`, where the test expects `Accepted`.
  - Cause: the workload's manual outbox shuffle now races 02-13's live shuffler, which delivers the same batches first.
  - This is not caused by the timer service (TimerFired creates no outbox rows); the suite passed with this plan's timers alone before 02-13 landed.
  - The test needs to stop shuffling by hand or tolerate `Duplicate`. That belongs to 02-13.
- `tests/shuffler_exactly_once.rs:223` fails clippy with `let_unit_value`; this is 02-13's work in progress.

## Requirements

- **STOR-13** complete: timers load lazily in a bounded time window, and `TimerFired` is idempotent end to end (roadmap criterion 4: a duplicate TimerFired has no effect).
- **STOR-08** complete: segments are deleted only below the forced durable checkpoint, and only with a permit from the opened readiness barrier. Retention is inert by default (roadmap criterion 5). This builds on 02-08 (trim rule/permit) and 02-12 (startup barrier).

## Notes for later plans

- **02-16 / docs:** document that `snapshot_floor` stays 0 until Phase 8. `trim_safety::floor_never_exceeds_the_durable_checkpoint` shows a rebuild from a trimmed log fails with "snapshot required".
- **Phase 8:** set `RetentionConfig.snapshot_floor` from the latest snapshot to activate trimming.
- **Phase 4:** timer `fired_at_ms` comes from `LeaderCtx.clock`; tests drive it with `ManualClock`.

## Commits

- 9c1d28b feat(02-14): lazy windowed timer service firing idempotent TimerFired (STOR-13)
- 9561c89 feat(02-14): barrier-gated, checkpoint-first, inert-by-default retention (STOR-08)

## Self-Check: PASSED

All 4 files exist; commits 9c1d28b, 9561c89 present.
