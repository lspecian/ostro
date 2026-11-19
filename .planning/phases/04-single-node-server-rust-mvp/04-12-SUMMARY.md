---
phase: 04-single-node-server-rust-mvp
plan: 12
subsystem: partition-apply
tags: [apply, fjall, keyspace, suspend, cas, dispatcher-prereq, query, storage-format]
requires:
  - phase: 04-09
    provides: StartRun fields 14..18, Derived::Suspend (tag 6), ostro-types limit constants
  - phase: 02-09
    provides: apply core (validation before writes, one point read on hot paths, rolling hash)
  - phase: 02-15
    provides: rebuild-digest property (live == rebuilt-from-log)
provides:
  - RunRecord trailing fields module_sha, tags, tenant, traceparent, run_timeout_ms, completed_at_ms
  - Table::Running (14th keyspace, "running"), with a row exactly while status == Running
  - Suspend apply (status -> Scheduled; from_seq check is the CAS); InvalidCode::BadLength, InvalidCode::NotRunning
  - Action::InputAppended { run, seq } (no emitter yet)
  - ostro_partition::query::{running_runs, ready_entries, ReadyEntryRow, journal_rows, run_record, run_payload, runs_page, PAYLOAD_INPUT, PAYLOAD_RESULT}
  - docs/storage-format.md §12 "Phase 4 additions (pre-freeze)"
affects: [04-13, 04-14, 04-15, 04-16, 04-18, 05-01]
tech-stack:
  added: []
  patterns:
    - "Index-by-status keyspace: blind insert on entry (AttemptGranted), blind remove in leave_running() before every status change out of Running; no reads"
    - "StartRun extras validated before the point read (check_start_extras), so rejections cost 0 reads"
key-files:
  created:
    - crates/ostro-partition/src/query.rs
    - crates/ostro-partition/tests/apply_phase4.rs
  modified:
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/outcome.rs
    - crates/ostro-partition/src/apply/mod.rs
    - crates/ostro-partition/src/store/mod.rs
    - crates/ostro-partition/src/store/keys.rs
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/tests/store.rs
    - crates/ostro-partition/tests/rebuild_digest.rs
    - docs/storage-format.md
key-decisions:
  - "Suspend on a run whose status is not Running is rejected Invalid(NotRunning), a new InvalidCode appended at the end (still outcome code 26). Without this, a stale holder of a still-current token could suspend a run that had yielded (Ready via EnqueueReady) and drop its ready row"
  - "module_sha of any length other than 0 or 32 is rejected Invalid(BadLength), a new InvalidCode. Tags, tenant and traceparent over their limits reuse FieldTooLong"
  - "Empty/zero StartRun extras and CompleteRun.completed_at_ms = 0 are stored as None (proto3 'unset')"
  - "RunRecord changed positionally without a VALUE_FORMAT bump, per the plan: pre-freeze, no migration, delete pre-Phase-4 dev data dirs (documented in storage-format §12)"
  - "Running row value is values::encode(&(epoch, n)), i.e. VALUE_FORMAT byte + postcard, like every other postcard value"
  - "journal_rows returns (seq, kind, PayloadRef); an event stored without a payload yields an empty inline ref"
requirements-completed: []
duration: 30min
completed: 2026-11-19
---

# Phase 4 Plan 12: Apply gaps (RunRecord fields, Running keyspace, Suspend CAS, InputAppended, query helpers) Summary

**Apply now stores StartRun 14..18 and CompleteRun.completed_at_ms in RunRecord. A new 14th keyspace `running` indexes every Running run with its attempt token. `Suspend` moves a Running run to Scheduled, with the existing `from_seq == next_seq` check acting as the compare-and-set. Everything is blind writes, with no new point reads. A `query` module gives the dispatcher and API typed reads.**

## Performance

- **Duration:** about 30 min, including the ci-local run
- **Tasks:** 2 (Task 1 TDD)
- **Files:** 2 created, 10 modified

## Accomplishments

- **RunRecord** gained trailing fields: `module_sha: Option<[u8;32]>`, `tags: Vec<String>`, `tenant: Option<String>`, `traceparent: Option<String>`, `run_timeout_ms: Option<u64>` and `completed_at_ms: Option<u64>`.
- **StartRun validation** (`check_start_extras`) uses `ostro_types::command::{MODULE_SHA_LEN, MAX_RUN_TAGS, MAX_RUN_TAG_LEN, MAX_TENANT_LEN, MAX_TRACEPARENT_LEN}`. It runs before the point read, so a rejected start costs 0 reads.
- **`Table::Running`**, named "running", is appended to `Table`/`TABLES`. A new `TABLE_COUNT = 14` replaces the hard-coded `13` in the keyspace arrays, and `keys::running_key` was added.
  - `PartitionStore::open` and `digest()` iterate `TABLES`, so they pick up the new keyspace automatically.
  - `AttemptGranted` blind-inserts or overwrites the row.
  - `leave_running()` blind-removes it before every status change out of Running: `enqueue_ready`, `Suspend` and `CompleteRun`.
- **Suspend**, in `check_intents` and the write loop, replaces both temporary arms from 04-09.
  - It must be the last intent. It cannot be combined with `EnqueueReady` or `CompleteRun` in either order, or repeated; any of these is `BadIntent`.
  - It needs status Running, else `NotRunning`.
  - Effect: status becomes Scheduled; the ready and running rows are removed; `current_attempt` is kept; no action is emitted.
- **`Action::InputAppended { run, seq }`** was added.
- **`query.rs`** provides `running_runs`, `ready_entries` → `ReadyEntryRow`, `journal_rows`, `run_record`, `run_payload` (with `PAYLOAD_INPUT`/`PAYLOAD_RESULT` re-exported) and `runs_page` (strictly after a cursor, in key order).
- **rebuild_digest:**
  - Two new op variants, `StartV4 { p, bad }` and `Suspend { p, i, mode, event, timer }`, are added as new `prop_oneof!` arms (weights 3 and 4). The existing arms and `fixed_scenario_with_restarts` are unchanged.
  - New test `fixed_scenario_phase4`: 40 rounds of rich and invalid starts, grant, suspend (timers due in the past, so the timer service wakes the runs), re-grant, events/complete, and a restart. It is rebuilt with chunk 1 and chunk 1024.
  - A temporary instrumented run showed 31 accepted Suspends and 9 rejected ones (Fenced, SeqMismatch, RunTerminal). The instrumentation was not committed.
- **docs/storage-format.md:**
  - §6 now lists 14 keyspaces, with row 13 `running` and the RunRecord trailing fields.
  - §8 lists the new InvalidCodes.
  - New §12 "Phase 4 additions (pre-freeze)" covers StartRun 14..18, Derived 6 = Suspend, the RunRecord fields with the no-migration note, the running keyspace, the Suspend apply rules and CAS, `InputAppended`, journal kind 29 and the free tags.

## Read budget (apply hot path)

| Command / arm | Point reads | Change |
|---|---|---|
| StartRun | 1 (dedupe or run) | none; extra checks run before the read |
| AttemptGranted | 1 (run) | +1 blind insert (`running`) |
| EventsAppended + Suspend | 1 (run) | +1 blind remove (`running`); remove_ready is blind |
| EventsAppended + EnqueueReady / CompleteRun | 1 (run) | +1 blind remove when the run was Running |

The tests assert these counts: `reads == 1` for grant, complete and suspend, and `reads == 0` for rejected StartRun extras.

## Task Commits

1. **Task 1 RED: failing tests**: `65c5465` (test)
2. **Task 1 GREEN: RunRecord fields, Running keyspace, Suspend, InputAppended**: `c5f300c` (feat)
3. **Task 2: query helpers, rebuild-digest coverage, storage-format §12**: `e9a3831` (feat)

## Verification

- `cargo test -p ostro-partition --locked`: all suites green (apply_phase4 19 tests; rebuild_digest 3 tests, including the proptest and both fixed scenarios, in 99 s).
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`: clean, with and without `--features failpoints`.
- `cargo build --workspace --all-targets --locked`: 0 warnings. `cargo fmt --all --check`: clean.
- `bash scripts/ci-local.sh --no-musl`: **PASS** (exit 0). Steps: toolchain, fmt 1 s, clippy 5 s, build 4 s, cargo test 250 s, failpoint suites 283 s (including `ostro-partition --features failpoints -- --test-threads=1`), deny, check-vendored, deny-fixtures, forbidden-deps, workflow validation all PASS; the 3 musl legs are SKIP (--no-musl)

## Digest / golden impact

No pinned digest golden exists. `store_digest.rs` and `rebuild_digest.rs` compare digests between stores, and `apply_core.rs`'s `FOLD_GOLDEN` is the rolling hash, which depends only on outcome codes and command bytes. **No golden was re-blessed.** The logical digest value of every store changes anyway, because the digest hashes each keyspace's name and row count and there is now a 14th keyspace. Existing rows are unchanged. The only test that pinned the keyspace set was `tests/store.rs::defaults_and_tables`, which now expects `"running"`. `value_round_trips` now sets the new RunRecord fields.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Correctness] Suspend requires status Running (`InvalidCode::NotRunning`)**
- **Found during:** Task 1 design
- **Issue:** `EventsAppended` accepts the current token whatever the status. After a yield (`EnqueueReady`, so the run is Ready and keeps its token), a late Suspend from the same holder would move the run to Scheduled and delete its ready row, so the run would never be dispatched.
- **Fix:** `check_intents` gets the status. Suspend on a non-Running run is rejected with the new `InvalidCode::NotRunning`, appended at the end; it still folds as code 26.
- **Commit:** c5f300c

**2. [Rule 3 - Blocking] `apply::run` made `pub(crate)`** so `query.rs` can re-export `PAYLOAD_INPUT`/`PAYLOAD_RESULT` without duplicating them. Commit e9a3831.

**3. Test expectation fix (no code change):** `ready_entries` key order sorts queues by their `u16 len ‖ utf8` encoding, so shorter names come first, not alphabetical order. The test and the doc comment now say so.

**4. Plan vs. doc policy:** storage-format.md's freeze policy says incompatible changes before Phase 5 bump a version. This plan explicitly says not to bump `VALUE_FORMAT`. I followed the plan and documented the exception in §6 and §12.

## Known gaps (not fixed, out of scope)

- **Inbox inputs vs. Suspend:** signals, child results and cancel requests still go to `inbox` and do not bump `next_seq`. One that arrives while a run is Running does not fail a later Suspend, so the suspended run is not woken by it. `cancel_requested` is set on the record, but nothing wakes the run. 04-13 closes this for timers only. Phase 5, which moves signals into the journal, has to close it for the others. A cheap interim mitigation, needing no read: in the Suspend arm, if `rec.cancel_requested` then enqueue_ready instead of Scheduled.
- No apply-path throughput re-measurement (80.8k cmds/s RMW baseline). The only hot-path additions are one blind insert per grant and one blind remove per Running exit.

## Notes for later plans

- **04-13 (timer inputs):**
  - There is no shared "write an event row" helper. `apply_events_appended` inlines `b.insert(Table::Events, event_key(&run, ev.seq), ev.encode_to_vec())` with a prost `JournalEvent`. Add a small `pub(crate) fn put_event` in run.rs, or inline the same line. `query::journal_rows` decodes exactly that row format.
  - A timer firing on a Running run must not touch the `running` row (status stays Running).
  - `make_ready` → `enqueue_ready` already calls `leave_running`. It is a no-op for Scheduled runs.
  - `rebuild_digest`'s driver model (`RunModel.next_seq`) does not know that TimerFired will bump `next_seq`. After 04-13, more generated Events/Suspend ops will be rejected SeqMismatch. That is still deterministic and the property stays valid, but the accept rate drops. `fixed_scenario_phase4` schedules timers due in the past in its Suspend ops, which exercises exactly that path.
- **04-14 (dispatcher):**
  - Rebuild from `query::running_runs` (`Vec<(RunId, AttemptToken)>`) and `query::ready_entries(r, limit)` (`Vec<ReadyEntryRow{queue, priority, ready_at_ms, run, concurrency_key}>`) inside `spawn_blocking`.
  - A run in `running` has no ready row, and vice versa.
  - Suspend emits **no action**. The dispatcher learns a run left Running only from its own job outcome. A later wake-up arrives as `EnqueueReady`.
- **04-15 (server sink):**
  - Read through `query::{journal_rows, run_record, run_payload}`.
  - `RunRecord.module_sha` is `Option<[u8;32]>`; `tenant`, `traceparent` and `run_timeout_ms` are `Option`s.
  - Submit Suspend as `EventsAppended{from_seq: sink.next_seq(), events: [], derived: [Derived::suspend()]}`. `SeqMismatch{expected}` means re-activate. `Invalid(NotRunning)` means the run is no longer Running (it yielded or was re-dispatched); treat it like Fenced.
- **04-18 (API):** `runs_page(r, after, limit)` is strictly after `after`. StartRun rejections map as follows: `Invalid(BadLength)` is a bad module sha, and `Invalid(FieldTooLong)` is tags, tenant or traceparent over their limits.
- **05-01:** free tags are unchanged by this plan: Derived 7, StartRun 19, Cmd 8 (Phase 5 reserves StartRun 19..23 and Cmd 8..12). The next `Table` must be appended after `Running`, and `TABLE_COUNT` bumped.

## Self-Check: PASSED

- FOUND: crates/ostro-partition/src/query.rs (`pub fn running_runs`)
- FOUND: crates/ostro-partition/tests/apply_phase4.rs
- FOUND: crates/ostro-partition/src/store/mod.rs (`Table::Running`)
- FOUND: docs/storage-format.md (`running` keyspace, §12)
- FOUND: commits 65c5465, c5f300c, e9a3831
