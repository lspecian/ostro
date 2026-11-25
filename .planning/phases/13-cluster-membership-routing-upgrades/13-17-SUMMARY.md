---
phase: 13-cluster-membership-routing-upgrades
plan: 17
subsystem: RepinRun + migrate-runs + executor module_gone (CLUS-07 second half)
tags: [repin, migrate-runs, replay-check, module-gone, compat-gate, mutant, tier-b]
requires:
  - phase: 13-01
    provides: "Cmd::RepinRun (43), RejectReason ModuleMismatch (23) / RunBusy (24), repin.rs + control/migrate.rs stubs"
  - phase: 13-04
    provides: "compat::require(view, Feature::RepinRun, level_max)"
  - phase: 13-12
    provides: "routed fan-out of migrate_runs (one leg per leader, ostro-partitions), route::table::listed_partitions"
  - phase: 13-13
    provides: "route::consistency::before_meta_read / before_partition_read"
  - phase: 13-15
    provides: "module_retire::check_not_retired / parse_sha"
  - phase: 10
    provides: "ostro_engine::replay_check (unchanged), guests p10-replay-v1/v2/v3"
provides:
  - "ostro-partition apply/repin.rs: RepinRun apply"
  - "ostro_server::control::migrate::{migrate_runs, sort_report, limit_share, MUTANT_SKIP_SEQ_CHECK, METRIC_RUNS, REASON_FEATURE, BeforeSubmitHook, set_before_submit_hook}"
  - "ostro_server::executor::{REASON_MODULE_GONE, MODULE_GONE_CONFIRM}; EngineExecutor::module_gone (crate); derive::ModuleResolver::{module_known, changes}"
  - "mutant migrate::mutant::skip_seq_check placed and biting (server test + tier-B)"
  - "tier-B group migrate (4 registry scenarios + migrate_refused_before_finalize)"
affects: [13-16, 13-18, 13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "repin = compare-and-set on (module_sha, next_seq, no lease), so 'only after a passing replay' holds by construction"
    - "deterministic global limit split per partition (same shares at every node)"
    - "missing meta record re-read on meta changes for a bounded window before a fatal verdict"
key-files:
  created:
    - crates/ostro-partition/tests/apply_repin.rs
    - crates/ostro-server/tests/migrate_runs.rs
    - crates/ostro-server/tests/p13_module_gone.rs
    - crates/ostro-repl-conformance/tests/migrate.rs
  modified:
    - crates/ostro-partition/src/apply/repin.rs
    - crates/ostro-partition/tests/p13_format.rs
    - crates/ostro-server/src/control/migrate.rs
    - crates/ostro-server/src/executor.rs
    - crates/ostro-server/src/derive.rs
    - crates/ostro-server/src/workers/remote_executor.rs
    - crates/ostro-server/src/services/run_spans.rs
    - crates/ostro-server/src/route/fanout.rs
    - crates/ostro-repl-conformance/src/scenarios/migrate.rs
key-decisions:
  - "Lease check = status Running (the running row exists exactly while Running, run.rs invariant): no second read; a hot-parked activation keeps its run Running, so it is RunBusy (W15 confirmed, no eviction needed)"
  - "Check order: validation (run id, shas) -> RunNotFound -> RunTerminal -> ModuleMismatch -> SeqMismatch{expected: next_seq} -> RunBusy -> accept"
  - "Limit split: partition p gets L/P + (p < L%P) of limit L over P partitions; no limit = all matching runs"
  - "filter = comma-separated tags, all required"
  - "Gate refusal is 409 conflict reason feature_not_enabled (plan + 13-14 upload gate), not 13-15's 412"
  - "module_gone = the module's Module record is missing in meta (re-read on meta changes for MODULE_GONE_CONFIRM = 2 s); a missing blob with the record present stays module_not_found"
  - "The router's merged report is sorted by run id (fanout.rs), so every node answers the same report"
requirements-completed: []
duration: "about 4.5 h including the cluster runs"
completed: 2026-11-25
---

# Phase 13 Plan 17: RepinRun + migrate-runs Summary

**`ostro migrate-runs` moves live runs to another workflow version only after the run's own journal replays cleanly against it: `Cmd::RepinRun` applies as a compare-and-set on `(module_sha, next_seq, no lease)`, each partition leader replays with `ostro_engine::replay_check` and submits with `expected_next_seq` = the replayed journal length, gated by `compat::require(RepinRun)`. A run whose module record was deleted by module GC now fails loudly with the non-retryable `module_gone`.**

## RepinRun rules (Cmd 43, `ostro-partition/src/apply/repin.rs`)

| condition (in this order) | outcome |
|---|---|
| run id not 18 bytes / another partition | `Invalid(BadRunId)` / `WrongPartition` |
| `from_sha` or `to_sha` empty / not 32 bytes | `Invalid(MissingField)` / `Invalid(BadLength)` |
| no run record | `RunNotFound` |
| terminal | `RunTerminal` |
| `module_sha != from_sha` (incl. unpinned) | `ModuleMismatch` (index 23, code 39) |
| `next_seq != expected_next_seq` | `SeqMismatch{expected: next_seq}` |
| status `Running` (leased; includes a hot-parked activation) | `RunBusy` (index 24, code 40) |
| otherwise | `Accepted`: `module_sha = to_sha`, nothing else |

One read (the run record), one write on success, nothing written on any reject, no action, no clock. A paused run that is not Running can be repinned (the mark stays). Outcome codes go into the rolling hash like every outcome. Hot-parked (W15): the Phase 3 hot wait flushes intents without a `Suspend`, so the run stays `Running` with its `running` row until eviction. `hot_parked_run_is_busy` exists in both `apply_repin.rs` and `migrate_runs.rs`.

Digest coverage: `rebuild_digest.rs`'s generator lives outside this plan's files, so `apply_repin.rs` carries its own proptest. It applies a random mix of start / grant / step / suspend / complete / repin (matching or wrong sha, seq ±1). For every op it checks that an accepted repin met every precondition and changed only the pin, and that a rejected one wrote nothing. It then rebuilds the log on a fresh store and requires the same digest and rolling hash.

## migrate-runs (`ostro-server/src/control/migrate.rs`)

Entry: any node. The routed layer (13-12) fans out one leg per leader with `ostro-partitions`. Each leg works as follows.
1. Validates the workflow name, runs `before_meta_read`, and resolves `from` (version → sha, or sha) and `to_version`. An unknown version is 404; a malformed sha or `from == to` is 400.
2. `check_not_retired(to)` → 409 `module_retired`.
3. Unless `dry_run`: `compat::require(Feature::RepinRun)` before any work → 409 `feature_not_enabled`, with details `{reason, feature: "repin_run", needed, finalized}`. A dry run works at any level.
4. For each `listed_partitions` partition with a non-zero share: `before_partition_read`, then an oldest-first scan for non-terminal runs of `workflow` on `from_sha` carrying every `filter` tag, up to the share.
5. Prepares the target once (runtime cache, else blob store). Reads each run's journal and runs `replay_check`: `Ok` / `OkFrontier` → passed; `Diverged{seq, detail}` → failed with that seq; `Error` or an unreadable journal → failed with seq 0.
6. Unless `dry_run`: calls the test-only before-submit hook, then submits `RepinRun{expected_next_seq: replayed journal length, requested_at_ms: ctx.clock}` per passed run. Outcomes: Accepted → repinned; SeqMismatch → seq_mismatch; RunBusy → busy; ModuleMismatch → failed "module changed concurrently"; RunTerminal / RunNotFound → failed; StaleEpoch / WrongPartition → 503 `not_leader`, so the router re-routes.

**Report format** (`MigrateRunsReport`, 13-01 shape): `passed[]`, `repinned[]`, `failed[{run_id, seq, detail}]`, `busy[]`, `seq_mismatch[]`. Every list is sorted by run id, both in each leg and in the router's merge (`fanout.rs`), so the same request gives the same report at every node. This is tested in tier B with two dry runs at different nodes.

**Limit split:** partition `p` gets `L / P + (p < L % P)` of limit L over P partitions. The total is exactly L, and every node computes the same shares, so the selection does not depend on the receiving node. No limit means every matching run.

**Mutant** `migrate::mutant::skip_seq_check` (failpoints build): `expected_next_seq` = the run's `next_seq` read at submit time. It bites in two places: `migrate_runs.rs::failpoints::skip_seq_check_mutant_bites` (the grown journal is repinned), and tier-B `concurrent_growth_mutant_bites` (the scenario fails with a `seq_mismatch` violation).

Metric: `ostro_migrate_runs_total{outcome}`. One info line per leg.

## module_gone (`executor.rs`)

When metadata is wired (`MetaModules`), a pinned module whose `Module` record is missing fails the run with `CompleteRun{Failed, resumable: false}`. The failure JSON is `{"reason_code":"module_gone","kind":"module_gone","message":"module <sha> is gone: its metadata record no longer exists (module GC deleted it after it was retired); the run cannot continue on it"}`. This holds even when the blob is still on disk, which was the 13-15 retry-restart finding. Before failing, the record is re-read on every meta change for `MODULE_GONE_CONFIRM` = 2 s, so a node whose meta view trails does not fail a run whose module was just uploaded (tested). The check covers both the embedded path and the remote runner (`workers/remote_executor.rs`). The WF-08 `module_override` test hook skips it. `run_spans` counts `module_gone` as its own DLQ reason label. Harnesses without meta (`NoModules`) skip the check.

## Task commits

1. `029e2e0` feat(13-17): RepinRun apply
2. `e2115fe` feat(13-17): migrate-runs + module_gone
3. `f2aaf0c` test(13-17): tier-B migrate-runs scenarios
4. `c57a4c5` fix(13-17): migrate_runs test imports the mutant name only with failpoints (found by the plan-level lint group)

## Verification

- `cargo test -p ostro-partition --test apply_repin`: 10 passed (incl. `hot_parked_run_is_busy`, the 32-case proptest); `--test p13_format`: 3 passed. clippy `-D warnings` and fmt clean.
- `cargo test -p ostro-server --features failpoints --test migrate_runs --test p13_module_gone -- --test-threads=1` (local, slot-5): 8 + 2 passed (incl. `failpoints::skip_seq_check_mutant_bites`). clippy `-D warnings` clean with and without failpoints.
- `cargo test -p ostro-repl-conformance --features failpoints --locked --test migrate -- --test-threads=1` (cluster slot 1, `ostro-k8s-ci --cmd`): 6 passed, twice in a row (167 s, 303 s): `passing_runs_repinned_cluster_wide`, `diverging_runs_never_repinned`, `concurrent_growth_seq_mismatch`, `busy_runs_reported`, `migrate_refused_before_finalize`, `concurrent_growth_mutant_bites`. Default build: 5 passed after the diverging runs' sleep went from 20 s to 45 s; `diverging` alone then passed 3/3. clippy `-D warnings --all-targets --features failpoints`: clean.
- `PROPTEST_CASES=128 cargo test -p ostro-partition --test rebuild_digest --test apply_repin` (cluster slot 1): 8 + 10 passed (rebuild_digest 384 s).
- Plan-level `ostro-k8s-ci --parallel f2aaf0c` (full run, 77 min wall on a shared cluster):

  | step | result |
  |---|---|
  | fmt, build (warning-free), sdk, guests, order-saga, semantics sleepers, failpoint list sync, phase 7 legs, deny, vendored, deny fixtures, no chronik, one log path + P12 grep gates (clock gate), workflow validation | PASS |
  | failpoint suites (fp1+fp2+fp3) | PASS. Includes tier-B `migrate` 6/6 with the mutant, server `migrate_runs` failpoints, `apply_repin` |
  | replication mutant matrix, server campaign smoke (20 kills), ostro-build polyglot | PASS |
  | cargo clippy -D warnings | FAIL: unused import in the default build of `tests/migrate_runs.rs`. Fixed in `c57a4c5`; the lint group re-run at `c57a4c5` (slot 1, `CI_LOCAL_SKIP` = the lint group's) passed every step |
  | polyglot group | NORESULT (pod create "database is locked"). Re-run at `c57a4c5`: all 8 polyglot steps PASS |
  | cargo test | FAIL, 1 test: `ostro-partition` `trim_safety::sc4_random_snapshot_trim_kill_sequences` ("node open timed out", a load timeout; 13-09 saw `trim_safety` flake the same way). Re-run alone at `c57a4c5` (slot 1): 6/6 passed (124 s). test2 stopped at that failure, so its remaining units were re-run with `--no-fail-fast` at `c57a4c5` (slot 1, 55 min): all 352 test targets passed, including `trim_safety`, `migrate_runs` (default build, 7), `p13_module_gone` (2), `executor`, `worker_remote` and every ostro-server / ostro / repl-conformance target of the group. test1 passed in the parallel run |
  | musl binary size < 60 MB | FAIL (known, being fixed by another agent) |
  | musl zigbuild test | FAIL: `ostro` `worker_failover::{oscillation, sustained_overload}` (both on the known flaky list; timing assertions, no `module_gone` in the log) |

## Deviations from Plan

1. **[Rule 1] `p13_format.rs` (13-01 contract test)** asserted the 13-01 stub's `Invalid(NotImplemented)` with 0 reads. It now expects the real apply (`RunNotFound` after one read, nothing written). This is a stub assertion, not an oracle.
2. **[Rule 3] Files outside `files_modified`:**
   - `derive.rs`: two defaulted `ModuleResolver` methods, so the executor can see the meta record without touching 13-02-only `server.rs`.
   - `workers/remote_executor.rs` (13-14's file): the same `module_gone` check for worker queues.
   - `services/run_spans.rs`: the DLQ label.
   - `route/fanout.rs` (13-12's file): one call that sorts the merged report.
3. **[Adaptation] 3-node coverage lives in tier B** (`tests/migrate.rs`), not in `migrate_runs.rs`, which is single node (the tier-B harness is the in-process 3-node cluster).
4. **[Adaptation] Replay runtime:** `replay_check` runs on the node's engine runtime, as the 10-11 replay endpoint and the 11-10 rerun check do, not on a separate frozen-FakeClock runtime. The check's sink refuses every append past the journal end and performs no effect I/O, so nothing live runs.
5. **[Adaptation] busy scenario:** a hot wait parks only waits shorter than `hot_wait`. Those runs finish their sleep in process and complete, so "a later migrate repins them" cannot happen for a hot-parked run. The scenario asserts `busy`, pin kept, completion on v1, and that a later migrate finds nothing to move. Repinning after `busy` is shown in `concurrent_growth_seq_mismatch`'s retry loop.
6. **[Decision] module_gone scope:** a missing record is module_gone. A missing local blob while the record exists stays `module_not_found` (also non-retryable): the executor has no cross-member blob fetch, and "gone" would mislabel a replication defect.
7. **[Decision] Gate error code:** 409 `feature_not_enabled`, as the plan and the 13-14 upload gate use. 13-15's retire uses 412; 13-20 should unify.
8. **[Finding, scenario precondition] The gate races finalization per leg.** A submitting migrate checks `compat::require` on every leg after that leg's own meta barrier. If the finalization commits during the fan-out, a leg whose barrier came first answers 409 while another leg has already repinned. The router then fails the whole call with 409, and a rerun reports only the runs that were left (seen once in tier B before the scenarios waited for level 2 everywhere). Nothing unsafe happens: every repin is after the finalizing entry, and the upgrade oracle holds. The scenarios now start once every node sees level 2. A fix would decide the gate once at the entry node before the fan-out; the router has no P13Ctx today (13-12's file), so this is left to 13-18.
9. **Worktree / target / CI:** scratchpad worktree, `target/slot-5`, cluster slot 1 for targeted runs, `--parallel` for the plan check.

## Handover

- **13-18:**
  - Retired-module refusal is still missing on retry-restart, Phase 11 rerun, DLQ restart and bulk restart; they bypass `check_not_retired`. A restart pinned to a GC'd module now fails `module_gone` at its first activation, but one pinned to a retired module that is not yet deleted still runs.
  - Those restarts are built on local partition handles and are not routed (503 `not_leader`).
  - Start (`runs.rs`, key_partition class) and activation (`modules.rs`, meta_write) still read meta without `before_meta_read` before `check_not_retired`.
  - 13-03's `FakeModuleLifecycle` answers 409 on a second retire, but retire is idempotent.
  - Not touched by 13-17: `RunResult::from_json` matches `module_gone` as a substring of `error`, while runs report failures in `failure`. The oracle was not changed; 13-15's scenario folds `failure` in, and 13-18 owns the fix.
  - `concurrent_growth_mutant_bites` is the tier-B matrix entry for `migrate::mutant::skip_seq_check`.
  - The migrate gate races finalization per leg (deviation 8): decide the gate once at the entry node before the fan-out.
- **13-20:** add `module_gone` to `docs/metrics.md`'s `ostro_dlq_total` reason list; document the report format, the limit split and P13-F3 (run-info module hash may change after a repin); unify the 409 / 412 gate code.
- **ci.yml:** no hunk needed. `ostro-server` and `ostro-repl-conformance` failpoint suites already run whole crates.
- The separate single-node `partition offline` hunt on DLQ requeue/restart: 13-17 did not touch the DLQ / rerun / restart paths. The migrate fan-out does use the same router plan (`plan_partitions` → 503 `partition_offline` when a partition has no leader), so a migrate issued during such a window answers that 503.
