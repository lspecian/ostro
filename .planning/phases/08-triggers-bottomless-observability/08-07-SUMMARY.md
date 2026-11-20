---
phase: 08-triggers-bottomless-observability
plan: 07
subsystem: partition-apply
tags: [cron, schedule, watermark, overlap, apply, rebuild-digest]
requires:
  - "08-01 contract: Cmd::ScheduleFire 27, keyspace 16 schedule_state (schedule_state_key / schedule_run_key), ScheduleStateRow / BufferedFire / ScheduleRunRow, InvalidCode 22/23, routing::schedule_owner, cron_idempotency_key / cron_manual_idempotency_key"
  - "Phase 5 internals: run::start_run, waits::request_cancel, lifecycle::terminate_run, terminal::on_terminal; 06-03 terminal P-8 cleanup"
provides:
  - "apply/schedule.rs: apply_schedule_fire (validation, W4 incarnation reset, watermark, 5 overlap policies, manual, advance) and on_run_terminal (buffered start, reverse-row cleanup, continue-as-new clears last_run)"
  - "tests/common/gen_schedule.rs: ScheduleFire command builders + proptest workload generator + Model (exactly-once expectations)"
affects: [08-14, 08-22]
tech-stack:
  added: []
  patterns:
    - "terminal hook gated on the run's own record (idempotency key prefix `__cron/`) so non-schedule terminals pay 0 extra reads"
    - "start first, store the schedule row, then cancel/terminate the previous run: the old run's terminal hook sees the new last_run and does nothing"
key-files:
  created:
    - crates/ostro-partition/tests/apply_schedule.rs
    - crates/ostro-partition/tests/common/gen_schedule.rs
  modified:
    - crates/ostro-partition/src/apply/schedule.rs
    - crates/ostro-partition/src/apply/mod.rs
    - crates/ostro-partition/src/apply/terminal.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/lifecycle.rs
    - crates/ostro-partition/src/apply/retention.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-partition/tests/rebuild_digest.rs
key-decisions:
  - "terminal::on_terminal gains a `cur: &mut Cursor` parameter (needed to start a buffered fire through run::start_run); its 3 call sites pass it"
  - "The schedule terminal hook runs only for runs whose idempotency key starts with `__cron/` (every schedule-started run has one, validated at fire time): non-schedule terminals read nothing extra (plan said 1)"
  - "start.conflict_policy must be ReturnExisting (else BadScheduleFire), instead of overriding it"
  - "Any started fire drops a pending buffered fire (skipped += 1); a TerminateOther/CancelOther/AllowAll start supersedes it"
  - "A fire whose start is rejected is rejected as a whole with nothing written (the W4 reset happens on the next valid fire)"
  - "Without a buffered fire, last_run stays set (now terminal) at its terminal transition, except ContinuedAsNew, which clears it"
metrics:
  duration: "~2 h"
  completed: 2026-11-20
---

# Phase 8 Plan 07: ScheduleFire apply Summary

**The owner partition now applies cron fires exactly once per `(schedule, fire_time)`. A durable watermark in `schedule_state` moves in the same batch as the run start. All five overlap policies run locally against `last_run`, manual fires and catch-up advances are handled, buffered fires start from the terminal hook, and schedule incarnations (W4) reset the row. Incremental apply equals log replay under generated schedule workloads.**

## Outcomes

`ApplyOutcome::Duplicate { existing: Option<RunId> }` can carry `None`, so no substitute form was needed and no new outcome code was added.

| Case | Outcome | Row effect |
|---|---|---|
| malformed (kind, name empty / > 256 B, fire_time 0, start presence, manual request_id empty / > 64 B, overlap > 4, wrong idempotency key, conflict_policy != ReturnExisting) | `Rejected(Invalid(BadScheduleFire))` | none (0 reads) |
| `schedule_owner(name, P) != partition` | `Rejected(Invalid(ScheduleOwnerMismatch))` | none |
| start run id malformed / foreign | `Invalid(BadRunId)` / `WrongPartition` | none |
| row incarnation newer than the command (stale fire) | `Duplicate { existing: None }` | none |
| row incarnation older | row reset, then the rules below | same batch |
| Advance, `ft <= W` | `Duplicate { existing: None }` | duplicates += 1 |
| Advance, `ft > W` | `Accepted` | W = ft, skipped += cmd.skipped |
| Fire, `ft <= W` | `Duplicate { existing: last_run, or the dedupe row's run, or None }` | duplicates += 1 |
| Fire/Manual, last run absent/terminal, or AllowAll | `Accepted` (run started) | last_run, fired += 1, reverse row, W = ft (Fire), buffered dropped (skipped += 1) |
| live last run, Skip | `Accepted` | skipped += 1, W = ft (Fire) |
| live last run, BufferOne | `Accepted` | buffered = fire (replaced one: skipped += 1), W = ft (Fire) |
| live last run, CancelOther / TerminateOther | `Accepted` (start, then internal RequestCancel / TerminateRun, reason `schedule_overlap`, at `start.requested_at_ms`) | as for a start |
| the start is answered Duplicate by its dedupe row (repeated manual request_id, or a key used by an earlier incarnation) | `Duplicate { existing }` | duplicates += 1, W = ft (Fire) |
| the start is rejected | that rejection | none |

Outcome-detail strings: the cancel/terminate reason is `"schedule_overlap"` (`schedule::OVERLAP_REASON`), stored in `RunRecord.cancel_reason` / `terminal_reason`, and the `RunTerminated` journal reason.

**Terminal hook** (`schedule::on_run_terminal`, called from `terminal::on_terminal` after the lifecycle hook, for `__cron/` runs only):
1. Read the reverse row. If it is absent, stop.
2. Delete the reverse row and read the schedule row.
3. If the incarnation differs or `last_run != run`, stop.
4. If a fire is buffered, decode its prost `StartRun` and start it via `run::start_run`. On success, set `last_run` to the new run, add its reverse row and set `fired += 1`. If the start returns Duplicate, set `duplicates += 1`; if it is rejected, set `skipped += 1`. Then clear `buffered`.
5. If nothing was buffered and the status is `ContinuedAsNew`, clear `last_run`. This is the known limitation: continue-as-new chains are not tracked, and it is documented in the module docs. docs/cron.md is 08-14's file.

**Retention expiry:** `retention::delete_run` blind-removes the run's reverse row, for `__cron/` runs only.

## Phase 5 internals reused

- `run::start_run` (the dedupe/idempotency path with `ConflictPolicy::ReturnExisting`) and `run::local_run` / `run::load_run`
- `waits::request_cancel` (graceful, D3) and `lifecycle::terminate_run` (hard stop), called with constructed `RequestCancel` / `TerminateRun` structs. They were already `pub(crate)`, and no command is re-submitted.
- `terminal::on_terminal`: its signature gained `cur: &mut Cursor` (see Deviations).

## Read counts (asserted in tests/apply_schedule.rs)

| Path | Reads |
|---|---|
| Rejected by validation | 0 |
| First fire (no last_run): schedule row + StartRun dedupe | 2 |
| Skip / BufferOne with a live last run: schedule row + last-run record | 2 |
| Start with last_run set | 3 (+ cancel/terminate reads for CancelOther/TerminateOther) |
| Non-schedule run terminal (non-resumable, no waits): record + effects scan | 2 (no extra read for the schedule hook) |
| Schedule run terminal, no buffered fire: + reverse row + schedule row | 4 |
| Schedule run terminal starting a buffered fire: + its dedupe read | 5 |
| Schedule-keyed run terminal whose reverse row is already gone | 3 |

## Verification

- `cargo test -p ostro-partition --test apply_schedule --locked`: 20 passed.
- `cargo test -p ostro-partition --test rebuild_digest --locked -- schedule`: 2 passed (`schedule_workloads` proptest, 128 cases, plus `fixed_schedule_workload`), 488 s. The time goes to two fresh fjall store opens per case.
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`, with and without `--features failpoints`: clean. `cargo fmt --all --check`: clean.
- `cargo test -p ostro-partition --features failpoints --locked --no-fail-fast -- --test-threads=1`: 36 binaries ok, 337 tests passed. One failure: `timers::hundred_thousand_timers_bounded_window_large` panicked with "timed out: step fired" at load average ~21. It passed when re-run alone (57 s). This is a load flake, and that path never touches schedule code.
- After rebasing onto main 45cd563: `cargo build --workspace --locked` has 0 warnings; apply_schedule (20), apply_effects (14), apply_lifecycle (18) and apply_retention (5) are green.

## Deviations from Plan

**1. [Rule 3 - Blocking] `terminal::on_terminal` signature + call sites in `run.rs` and `lifecycle.rs` (not in files_modified).** Starting a buffered fire inside the terminal hook needs `run::start_run`, which takes the cursor. One-line changes at the 3 call sites (run.rs CompleteRun, lifecycle.rs hard_stop and continue-as-new).

**2. [Rule 2] `retention.rs` edited (not in files_modified)** for the plan-mandated "retention expiry deletes its reverse row if still present". It is a guarded blind remove, so other expiries keep their exact write set.

**3. Read count for non-schedule terminals: 0 extra instead of the plan's "exactly 1".** The hook is gated on the run record's `__cron/` idempotency key, which is already in memory. This is cheaper, and the existing read-count assertions in apply_core/apply_lifecycle/apply_phase4/apply_seams/apply_effects stay unchanged. Schedule runs pay 1–3.

**4. Rebuild-digest case shape.** `schedule_workloads` is a direct `apply_command` property (incremental per-command batches vs a one-batch replay, digest + rolling hash, plus the exactly-once property). It is not a new op in the node-level proptest: schedule-started runs are not in that driver's model, and 128 node-level cases would take hours.

**5. Coordinator request: `style(08-07): rustfmt 06-03 partition files`.** A separate formatting-only commit over the five files 06-03 left unformatted (terminal.rs, apply_effects.rs, apply_lifecycle.rs, rebuild_digest.rs, common/gen_effects.rs).

**6. Commit split.** The terminal hook and `tests/common/gen_schedule.rs` landed with Task 1: the BufferOne/W4 tests need the hook, and apply_schedule.rs uses the generator's builders. Task 2's commit carries the rebuild-digest property.

format_corpus is not re-blessed and docs/storage-format.md is unchanged.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| (coordinator) | ba5e72d | style(08-07): rustfmt 06-03 partition files |
| 1 (+ hook) | bba9269 | feat(08-07): ScheduleFire apply: watermark, overlap policies, manual, buffered fires, W4 incarnations |
| 2 | a7c420b | test(08-07): rebuild_digest schedule_workloads property and fixed schedule workload |

## Self-Check: PASSED

- FOUND: crates/ostro-partition/src/apply/schedule.rs, tests/apply_schedule.rs, tests/common/gen_schedule.rs
- FOUND: commits ba5e72d, bba9269, a7c420b on main
