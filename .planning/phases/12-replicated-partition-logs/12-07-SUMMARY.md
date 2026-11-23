---
phase: 12-replicated-partition-logs
plan: 07
subsystem: conformance-scenarios
tags: [conformance, clus-01, clus-02, clus-04, scenarios, mutants, red-run, standby, bootstrap, fetch, blobs, zombie, objstore]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 Scenario registry, pending lists and ostro_log::mutants names; 12-03 InProcCluster, FaultLayer, oracles, runner; 12-06 scenarios::placement::support helpers and the eight core scenario names the matrix maps to"
provides:
  - "Scenario groups standby, bootstrap, fetch_path, blobs, one_path, zombie, object_storage (12 scenarios, all pending)"
  - "mutants::MutantMap / MAP covering ostro_log::mutants::ALL (11 tier-B pairs, count_nondurable_leo tier C only); MATRIX kept as the 12-02 view"
  - "tests/mutants.rs: the permanent mutant matrix (baseline pass, mutant must give a ScenarioFailure, SKIP (pending) / OSTRO_REPL_REQUIRE_ALL=1)"
  - "scenarios::standby::derived: measured x_run_index comparison (the standby oracle's table digests cover only the 17 OSNP tables)"
  - "pending/rest.rs listing all 12 scenarios"
affects: [12-12, 12-14, 12-15, 12-16, 12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "Snapshot + trim driven through the node's own services (Node::snapshot_now, Node::retention_pass) instead of waiting for the hourly cadence; the trim itself is then measured from the WAL scan"
    - "A max-size envelope is built by one PartitionHandle::submit of N inline StartRuns (one submit = one envelope), since the API never batches that much"
    - "Batching proof without self-report: with each fetch held 400 ms, a >= 2-record jump in the follower's verified log between two samples < 350 ms apart came from one fetch response"
    - "remote_chain_never_above_hw bounds chunks by the minimum measured durable end over the live ISR (the HW can never exceed it), not by the leader's reported HW"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/{standby,bootstrap,fetch_path,blobs,one_path,zombie,object_storage,mutants}.rs
  modified:
    - crates/ostro-repl-conformance/src/scenarios/{standby,bootstrap,fetch_path,blobs,one_path,zombie,object_storage}.rs
    - crates/ostro-repl-conformance/src/pending/rest.rs
    - crates/ostro-repl-conformance/src/mutants.rs
key-decisions:
  - "RP-12 in tier B is the transport-side half: every Fetch of one follower is held 400 ms (4x fetch_max_wait) by the fault layer; a leader-side stalled disk read needs a failpoint, and failpoints are only allowed in tests/mutants.rs"
  - "MATRIX (12-02) kept alongside MAP, both built from the same slices: tests/skeleton.rs (12-02's file) iterates MATRIX"
  - "Derived x_* keyspaces are compared by a scenario-local measured read (copy + open, compare only where the index is Ready) because the standby oracle (12-03, not editable here) hashes only TABLES"
  - "one_path asserts K <= delta(hw_publishes) <= delta(verified records) over K sequential acked appends: the measurable form of 'equals the HW advance count'"
requirements-completed: []
duration: "~2 h (scenarios + matrix) + group red run 7 min + ci-local gate 1 h 45 min + full red run 14 min"
completed: 2026-11-23
---

# Phase 12 Plan 07: remaining conformance groups and mutant matrix Summary

**The rest of the CLUS-01 suite is written before integration. It covers hot standby, snapshot bootstrap, the fetch path (separate timeouts, a maximum-size envelope, batching), blob-before-commit, one path, the zombie leader and object storage: 12 scenarios, all pending. The permanent mutant matrix maps all ten `repl::mutant::*` failpoints to their scenarios. On the unintegrated build every one of the 12 scenarios is red with a measured `ScenarioFailure` and no harness error. The 11 RF>1 rows show the uniform `cluster_formed` reason. The RF=1 one-path row fails on its own oracle, `status`, because there is no `hw_publishes` yet. The matrix prints `SKIP (pending)` for all 11 tier-B pairs and `TIER-C` for `count_nondurable_leo`.**

## Performance

- Tasks: 2 of 2
- Commits: 4ef0c25 (Task 1), 32b3d72 (Task 2), plus this docs commit
- Group tests with pending skipped: 0.00 s each. Group red run: 30-91 s per group (30 s formation deadline per RF>1 scenario). Full `red_run`: 819 s for 30 scenarios.

## Scenario table

| Scenario | Group | Pending | Measured oracles (besides `cluster_formed` first and the 12-06 final bundle: replica identity below HW, acked_on_disk, la_rule, election audit) |
|----------|-------|---------|------|
| followers_are_hot_standbys | standby | yes | `standby::equal_state` and derived `x_run_index` on every ISR member within 1 s of the HW settling. Followers' `DispatcherStats.dispatched == 0`, leader's > 0 (1 partition, so two nodes lead nothing) |
| empty_node_bootstraps_from_snapshot | bootstrap | yes | Node 3's partition data is wiped (meta and node.toml kept). Every node runs `snapshot_now` + `retention_pass`. The leader's measured log start must be > 0. Node 3 rejoins the ISR, its log start must be > 0 (anchored at S, so it installed), and its state is equal |
| node_behind_log_start_reinstalls | bootstrap | yes | Same checks, with "held" being node 3's measured end when it stopped: the leader's trimmed start must be above it, and so must node 3's new log start |
| install_stream_cut_resumes | bootstrap | yes | When `snapshots/pNNNN/.incoming-*` has bytes on node 3, the link to its leader is cut (`partition`) for 500 ms and then healed. Node 3 still installs and ends identical. If every install finishes before staging is seen, that is a harness panic, never red evidence |
| slow_read_longer_than_max_wait_completes | fetch_path | yes | Every `Fetch` of node 3 is held 400 ms (4x `fetch_max_wait`). Node 3 never leaves the published ISR during 6 s of acked appends and reaches the leader's measured end |
| max_size_envelope_replicates | fetch_path | yes | Node 3's Fetch is blackholed. One `PartitionHandle::submit` of 16 x 240 KiB inline StartRuns (~3.75 MiB, measured >= 3 MiB in the leader scan) commits with the other follower. After the heal, node 3's measured end passes it |
| batching_on_user_partitions | fetch_path | yes | Under 40-way API load with fetches held 400 ms, node 3's verified record count grows by >= 2 between samples < 350 ms apart. The reported `max_envelopes_per_fetch` must be >= 2 when present |
| blob_before_commit_survives_failover | blobs | yes | A 1 MiB input is acked and the leader is killed at once. The new leader completes the run, and `/v1/runs/{id}/result` equals the input (smoke echoes it) |
| one_path_rf1_hw_through_tracker | one_path | yes | Per partition, over K=5 sequential acked appends: K <= delta `hw_publishes` <= delta verified records, and `hw_publishes` > 0. HW settles at the measured durable end |
| isolated_leader_cannot_commit | zombie | yes | The isolated leader acks nothing. A probe started within `session_timeout` + 1 s gets 503 with "leader" in the body. After the heal, none of its old-epoch keys is below the HW on any node |
| remote_chain_equals_committed_prefix | object_storage | yes | Load, a failover, more load, then every partition has a chain head. `objstore::chain_equals_committed` holds against the leader's scan below HW |
| remote_chain_never_above_hw | object_storage | yes | Followers' Fetch and the leader's Propose are blackholed. The leader's measured end must be above the stalled ISR-min bound. After 2 x `seal_interval` + 0.5 s, before healing, `objstore::nothing_above(bucket, q, bound)` holds for every partition |

## Mutant map (`mutants::MAP`, unit test: key set == `ostro_log::mutants::ALL`, 11 tier-B pairs)

| Mutant | Tier-B scenarios | Tier C |
|--------|------------------|--------|
| isr_from_assignment | isr_empty_follower_not_in_isr | - |
| hw_leader_leo | leader_dies_with_unreplicated_writes, hw_is_min_over_isr | - |
| count_nondurable_leo | - | follower_power_loss (12-17 process_mutants) |
| skip_epoch_check | fetch_with_stale_epoch_fenced | - |
| no_truncate | leader_dies_with_unreplicated_writes | - |
| unclean_election | failover_elects_isr_member, whole_isr_dead_goes_offline | - |
| ignore_min_insync | writes_stop_below_min_insync | - |
| shrink_before_commit | isr_slow_follower_shrinks | - |
| upload_above_hw | remote_chain_never_above_hw | - |
| rf1_bypass | one_path_rf1_hw_through_tracker | - |

Mutant run (`cargo test -p ostro-repl-conformance --features failpoints --test mutants -- --test-threads=1 --nocapture`, at 32b3d72):

```
MUTANT repl::mutant::isr_from_assignment: isr_empty_follower_not_in_isr SKIP (pending)
MUTANT repl::mutant::hw_leader_leo: leader_dies_with_unreplicated_writes SKIP (pending)
MUTANT repl::mutant::hw_leader_leo: hw_is_min_over_isr SKIP (pending)
MUTANT repl::mutant::count_nondurable_leo: TIER-C (process_mutants) follower_power_loss
MUTANT repl::mutant::skip_epoch_check: fetch_with_stale_epoch_fenced SKIP (pending)
MUTANT repl::mutant::no_truncate: leader_dies_with_unreplicated_writes SKIP (pending)
MUTANT repl::mutant::unclean_election: failover_elects_isr_member SKIP (pending)
MUTANT repl::mutant::unclean_election: whole_isr_dead_goes_offline SKIP (pending)
MUTANT repl::mutant::ignore_min_insync: writes_stop_below_min_insync SKIP (pending)
MUTANT repl::mutant::shrink_before_commit: isr_slow_follower_shrinks SKIP (pending)
MUTANT repl::mutant::upload_above_hw: remote_chain_never_above_hw SKIP (pending)
MUTANT repl::mutant::rf1_bypass: one_path_rf1_hw_through_tracker SKIP (pending)
mutant matrix: 0 (mutant, scenario) pair(s) bitten
```

With `OSTRO_REPL_REQUIRE_ALL=1` the same run fails and names each pending pair (checked). Without the feature, `tests/mutants.rs` prints "failpoints feature off".

## CLUS-01 red run (before integration), remaining groups

Build commit **32b3d72**: `OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1`. The test passed in 819 s with 0 HARNESS rows across all 30 scenarios. The 12-06 rows are unchanged: 16 FAIL `cluster_formed`, and placement_rf1 passes. Rows for this plan's groups:

```
scenario | pending | result | oracle: detail
followers_are_hot_standbys | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; node 1 partition 0: 1 replica(s) [1], expected rf 3; ...
empty_node_bootstraps_from_snapshot | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
node_behind_log_start_reinstalls | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
install_stream_cut_resumes | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
slow_read_longer_than_max_wait_completes | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
max_size_envelope_replicates | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
batching_on_user_partitions | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
blob_before_commit_survives_failover | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
one_path_rf1_hw_through_tracker | yes | FAIL | status: node 1 partition 0: /v1/admin/replication reports no hw_publishes (got null)
isolated_leader_cannot_commit | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
remote_chain_equals_committed_prefix | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
remote_chain_never_above_hw | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; ...
```

The uniform `cluster_formed` reason is what the unintegrated build is expected to give (12-03), and it is recorded as such. The oracle-specific evidence comes from the mutant matrix once 12-14 removes the pending entries. one_path is RF=1, so `cluster_formed` already holds. Its red row is the scenario's own `status` oracle: the replication route returns 404, so no `hw_publishes` is reported.

## Deviations from Plan

**1. [Interpretation] RP-12 stall is on the transport side.** A stalled read inside the leader's disk path needs a failpoint, and the phase rules allow failpoints only in `tests/mutants.rs`. Instead, the fault layer holds every Fetch of one follower for 4x `fetch_max_wait`. This catches a follower that bounds its fetch RPC by the idle wait. The leader-side half (`fetch_read_timeout` not cut by `max_wait`) stays with 12-11's unit tests.

**2. [Rule 2 - Missing coverage] Derived keyspaces are compared in the scenario.** The `standby` oracle hashes only `TABLES`, so it does not include `x_run_index`. The plan requires derived `x_*` keyspaces "when present", and oracles are not editable here. `scenarios::standby::derived` copies and opens each store the way the oracle does, then compares `x_run_index` rows between nodes whose index is `Ready`.

**3. [Rule 3 - Blocking] `mutants::MATRIX` kept.** `tests/skeleton.rs` (12-02, not in this plan's files) iterates `MATRIX`. It is now built from the same slices as `MAP`, and a unit test pins that the two agree.

**4. [Interpretation] Snapshot and trim are driven through `Node::snapshot_now` / `Node::retention_pass`.** The harness config keeps the 60 s `min_interval` and 1 h interval, and the harness may not be edited. The trim itself is then checked from the WAL scan.

**5. [Interpretation] Max-size envelope through `PartitionHandle::submit`.** The API writes one command per envelope, and an inline payload is at most 256 KiB, so no API call can produce a ~4 MiB envelope.

No harness, oracle, runner, `scenarios/mod.rs`, server or other crate file was touched. No dependency was added. FAILPOINT_CRATES and ci.yml are unchanged.

## Open items for later plans

- **12-14:** the scenarios read `partitions[].hw`, `partitions[].hw_publishes` and `partitions[].max_envelopes_per_fetch` from `GET /v1/admin/replication`, following the `ReplicaStatus` field names. zombie expects a write to an isolated (demoted) node to get 503 with a body that mentions "leader".
- **12-14/12-16:** timing bounds have not been tried against a real cluster: 1 s standby tracking, `session_timeout` + 1 s demotion, the 400 ms fetch hold, 2 x `seal_interval` + 0.5 s. `install_stream_cut_resumes` depends on seeing the `.incoming-*` file before an in-memory install finishes, so make the snapshot larger if it turns into a harness panic. `node_behind_log_start_reinstalls` and `empty_node_*` require retention to trim past an out-of-ISR replica's end.
- **12-15:** `remote_chain_equals_committed_prefix` waits for a chain head on every partition (`objstore::find_head`), so `decode_head` must match the 12-15 head encoding.
- **12-17:** `follower_power_loss` is the tier-C name `count_nondurable_leo` maps to.

## Plan gate

I ran `ci-local --no-musl` in the detached worktree at **32b3d72**, on slot-5 with load around 10 from parallel agents:

```
fmt / clippy -D warnings / build warning-free     PASS
sdk workspace, guests, order-saga                 PASS
cargo test                                        FAIL 252s (fail-fast stop: ostro --test backup_cli
                                                    bucket_online_and_offline_round_trips, PUT module TimedOut)
polyglot conformance / clients python, ts, go      PASS
ostro-build polyglot                              FAIL (ts_offline_after_warm: npmrc parent dir missing
                                                    under the worktree's target/ostro-toolchains-test)
semantics sleepers / failpoint list sync          PASS
failpoint suites (incl. tests/mutants.rs)         PASS 4527s
server campaign smoke (20 kills)                  PASS
phase 7 legs / deny / vendored / no-chronik / one log path / workflow validation  PASS
```

Neither failure touches this plan's code, and both passed on rerun at the same commit. `cargo test -p ostro-build --features polyglot -- --test-threads=2` was green. `cargo test --workspace --locked --no-fail-fast` was green with **3197 passed, 0 failed**. Before the CAS onto the moved main (c7e0c37), `cargo check --workspace --all-targets` was clean.

**Gate verdict: green at 32b3d72. Two flaky failures outside this plan's crate passed on rerun.**

## Self-Check: PASSED

- Files: the seven scenario modules, `pending/rest.rs`, `src/mutants.rs` and the eight test files are present at main.
- Commits: 4ef0c25 and 32b3d72 are on main.
