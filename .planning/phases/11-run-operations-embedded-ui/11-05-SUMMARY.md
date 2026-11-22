---
phase: 11-run-operations-embedded-ui
plan: 05
subsystem: partition store / apply / query
tags: [derived-keyspace, x_run_index, OPS-05, rebuild, proptest]
requires:
  - 11-02 (apply/index.rs stubs, x_run_index reservation)
provides:
  - store::derived (x_run_index keys, marker, run_index_rows, IndexKind, IndexState)
  - ApplyBatch::insert_derived/remove_derived/derived_len
  - PartitionStore::{run_index_state, start_run_index_rebuild, join_run_index_rebuild}
  - StoreReader::{run_index_state, run_index_rows}
  - query::{index_page, index_state, IndexPage, IndexError}
affects: [11-11, 11-12, 11-13]
tech-stack:
  added: []
  patterns: ["derived keyspace outside TABLES, rows atomic with the state change, offset-stamped marker, clear-and-rebuild under the apply lock"]
key-files:
  created:
    - crates/ostro-partition/src/store/derived.rs
    - crates/ostro-partition/tests/run_index.rs
  modified:
    - crates/ostro-partition/src/store/{mod,batch,snapshot}.rs
    - crates/ostro-partition/src/apply/{index,run,terminal,lifecycle,retention}.rs
    - crates/ostro-partition/src/query.rs
    - crates/ostro-partition/src/node.rs
decisions:
  - "The marker is rewritten by EVERY apply commit while the index is ready (one 12-byte insert per chunk, not per command), so `built_at_applied_offset == applied_offset` holds after any crash; a binary without the hooks leaves it lagging and the next open rebuilds"
  - "A fresh store writes the marker at creation (empty index is complete); a staged snapshot install never does, so an installed store opens Rebuilding"
  - "Apply lock = a Mutex in the store's shared index state, taken by ApplyBatch::commit around the gate commit and by each rebuild batch (lock order apply lock -> commit gate everywhere)"
  - "Rebuild runs on its own std thread (not the apply thread, not a leader service), started from PartitionStart::catch_up_and_announce after the Inspect return; dropping the store stops it after the current batch and joins it"
  - "store::derived is `pub mod` (not pub(crate)) so the integration test and 11-11/12/13 can build prefixes"
metrics:
  duration: "~3h (mostly the full ostro-partition suite under shared I/O load: 2484 s)"
  completed: 2026-11-22
---

# Phase 11 Plan 05: Derived run index x_run_index Summary

`x_run_index` is a fjall keyspace in each partition's Database that is not a `Table` and not in `TABLES`. Apply writes its rows in the same fjall batch as the state change. If the marker is missing or stale, a background thread rebuilds the index from `runs`. `query::index_page` serves newest-first, bounded, budgeted pages with a continuation token. Digest, OSNP and blob GC never see the keyspace, and the v0.1 format corpus and the rebuild_digest proptests are unchanged.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| 1+2 | bd15cb5 | `feat(11-05)`: derived keyspace, batch writes, hooks, rebuild, install handling, page query, tests |

## Hook call sites (one line each)

- `crates/ostro-partition/src/apply/run.rs:403`: `super::index::on_start(b, &run, &rec);` in the shared `start_run`. Every creation path goes through it: API StartRun (`apply/mod.rs:140`), inbound StartChild (`inbound.rs:172`), CronStart (`inbound.rs:244`), ScheduleFire (`schedule.rs:289`, `:376`), the continue-as-new successor (inbound StartRun -> `lifecycle::inbound_start_run` -> `start_run`) and restart-mode RetryRun (a StartRun with `retry_of`). No path bypasses it.
- `crates/ostro-partition/src/apply/terminal.rs:70`: `super::index::on_terminal(b, run, rec);` in `terminal::on_terminal`. Every terminal transition goes through it: CompleteRun (`run.rs:709`), hard stops (`lifecycle.rs:190`) and continue-as-new (`lifecycle.rs:607`).
- `crates/ostro-partition/src/apply/lifecycle.rs:291`: `super::index::on_resume(b, &run, &rec);` in `retry_run`, before `enqueue_ready`, while `rec.status` is still the terminal status.
- `crates/ostro-partition/src/apply/retention.rs:165`: `super::index::on_expire(b, run, rec);` in `delete_run`, the real delete point. It is reached for a normal expiry, for the deferred chain link at its bound, and from `delete_deferred` (chain_done). The first expiry of a deferred chain link takes the `defer` branch, so the record and its rows stay.
- `crates/ostro-partition/src/node.rs:900`: `self.store.start_run_index_rebuild();` after the `StartMode::Inspect` early return (outside `files_modified`, see Deviations).

The hooks only write; they never read. Apply read counts are unchanged; `apply_retention`'s `reads == 10` assertion still passes. Ready/Running transitions write no row (`ready_and_running_transitions_write_no_index_row` checks `derived_len() == 0` for AttemptGranted, non-terminal EventsAppended and a re-grant).

## Keys and marker

`kind ‖ value ‖ 0x00 ‖ sfx` (Tag 0x01, Workflow 0x02, Tenant 0x04), `0x03 ‖ status ‖ sfx` (terminal statuses only: Completed 3, Failed 4, Cancelled 5, Terminated 6, TimedOut 7, ContinuedAsNew 8), `0x05 ‖ sfx`. `sfx` = `!created_at_ms` (u64 BE) ‖ `!uuid` (16 B). Values are empty. Marker `0x00 'v'` -> `RUN_INDEX_VERSION u32 BE ‖ built_at_applied_offset u64 BE`. Tags, workflows and tenants containing 0x00 get no row: apply accepts such tags, and the proptest includes the tag `"nul\0tag"`. An empty tag is indexed under its own prefix `0x01 0x00`.

## Rebuild mechanism

`PartitionStore::start_run_index_rebuild` spawns a `run-index-rebuild` std thread with cloned handles. The thread:
1. Under the apply lock, removes the marker.
2. Clears the keyspace in cursor batches of 1,000, each under the apply lock.
3. Walks `runs` in batches of 1,000. Each batch reads the records and writes their rows with no apply commit in between.
4. Under the lock, writes the marker at the current `applied_offset` and flips the state to Ready.

The thread yields between batches. Hook writes during the rebuild are idempotent deltas: runs it has already visited stay current, and runs it has not visited yet are written at their current state. `rebuild_racing_live_apply_converges` checks this with 3,000 runs plus resumes, expiries, completions and starts applied during the rebuild. `start_run_index_rebuild` is called only on Leader starts; `inspect_mode_never_rebuilds_and_leader_mode_does` uses a real `Node`. Dropping `PartitionStore` stops and joins the worker. Snapshot install, which needs the store closed, inherits that.

## Install finding

The staging store cannot contain `x_run_index` rows. It is built from OSNP frames, which only carry `TABLES`. `open_raw` now goes through `PartitionStore::assemble`, which opens the derived keyspace empty and writes no marker. So no drop hunk was needed: the installed store opens `Rebuilding`, and the Leader start rebuilds it. `installed_snapshot_opens_rebuilding_and_rebuilds` covers this, including digest equality and rows equal to the source after the rebuild.

## Tests

- `cargo test -p ostro-partition --test run_index`: 14 passed (13 unit/integration + `index_rows_equal_f_of_runs` proptest). The proptest mixes StartRun, inbound StartChild (uuid v8), CronStart, grants, completions (resumable or not), continue-as-new with the successor delivered as inbound StartRun, terminate, resume, restart, expiry at the first due time and at the deferral bound, and crashes. For each case it checks (a) live, (b) after drop and reopen, (c) after marker deletion, reopen and rebuild.
- Proptest timings: 64 cases (default) took 101 s in the full-suite run and ~95 s standalone under shared load (~1.5 s/case, dominated by fjall open/fsync: at least 3 opens per case). 16 cases took 34 s. A 1,000-case gate run is estimated at ~25 min under this load. It was not run here.
- Mutation check: with `on_resume` removed, the proptest fails (index != scan) within 32 cases.
- `cargo test -p ostro-partition --locked --no-fail-fast`: exit 0, 47 binaries, 447 passed, 0 failed. This includes `format_corpus` (2, the v0.1 corpus rebuilds unchanged), `rebuild_digest`, `snapshot`, `snapshot_tail`, `trim_safety`, `p11_format` and `apply_retention`.
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`: clean, also with `--features failpoints`. `cargo fmt --check -p ostro-partition`: clean.
- After rebasing onto 4802f15: `cargo check --workspace --all-targets --locked` is clean, clippy is clean again, and `run_index` passes (PROPTEST_CASES=16).
- `grep x_run_index store/digest.rs` finds nothing. `TABLES` and `TABLE_COUNT` are unchanged.
- Not run: the full `bash scripts/ci-local.sh`, because the machine is shared and the partition suite alone took 41 min of wall time.

## Deviations from Plan

**1. [Rule 3 - Blocking] `crates/ostro-partition/src/node.rs` edited (outside `files_modified`).** The plan says the rebuild must not start in `StartMode::Inspect`, and the store does not know the start mode. One call line plus a comment went in `PartitionStart::catch_up_and_announce` right after the Inspect return. Every alternative also touched a file outside the list: machine.rs, leader services, or StoreConfig in config.rs.

**2. One commit for Tasks 1 and 2.** The hooks, the marker and the rebuild share `derived.rs` and the commit-path marker logic, and the tests exercise both tasks. The brief's worktree + CAS workflow replaced the plan's `/tmp/ostro-wt-11-05` + private-index procedure.

**3. Marker on every commit, not only on batches that write rows.** If only batches with derived rows rewrote the marker, its offset would lag after any AttemptGranted, and every reopen would rebuild. Writing it on every commit keeps "offset == applied" exact. The only change to the activation hot path is one marker insert per apply chunk; no index rows are added.

**4. Extra pub test helpers:** `ApplyBatch::derived_len`, `PartitionStore::set_run_index_hooks` (`#[doc(hidden)]`, simulates a binary without the hooks) and `delete_run_index_marker` (`#[doc(hidden)]`).

## Open items

- `docs/storage-format.md` has the frozen PF-11-2 rule but not the concrete `x_run_index` key layout. The file is not in this plan's `files_modified`. 11-08 owns the storage-format "Phase 11 additions" section and could add the layout above.
- The 1,000-case proptest gate run is still pending (see the timing estimate above).

## Self-Check: PASSED
- `crates/ostro-partition/src/store/derived.rs` and `crates/ostro-partition/tests/run_index.rs` exist; feat commit present on main.
