---
phase: 13-cluster-membership-routing-upgrades
plan: 18
subsystem: Phase 13 tier-B integration, mutant matrix, handover fixes
tags: [tier-b, integration, mutants, rolling-upgrade, drain, ryw, module-gc, routing, gate-409]
requires:
  - phase: 13-09..13-17
    provides: "membership, reassign, handoff, routing, RYW, version gate, module GC, promotion, migrate (every Phase 13 feature and its scenarios)"
provides:
  - "scenarios::integration_p13 (6 cross-feature scenarios + 3 regression scenarios), tests/integration_p13.rs, tests/regressions_p13.rs"
  - "mutants_p13::{MAP, matrix, entry, wrappers, find}, tests/mutants_p13.rs (OSTRO_P13_REQUIRE_ALL)"
  - "harness::outcomes::partition_outcomes (the OutcomeRecord producer)"
  - "ProcCluster::proxy_joined_outbound (tier C)"
  - "rerun::{restart_partition, check_restart_module}; control::module_retire::{FP_START_HOLD, START_HOLDS}; route Access::MetaReadWrite, RouteEnv::migrate_gate, consistency::carries_meta_token; p13::promote::self_cluster_addr"
affects: [13-19, 13-20]
tech-stack:
  added: []
  patterns:
    - "apply-outcome streams = replay of the measured WAL bytes through apply_command on a scratch store"
    - "matrix-only wrapper scenarios for bodies that own their cluster"
key-files:
  created:
    - crates/ostro-repl-conformance/src/harness/outcomes.rs
    - crates/ostro-repl-conformance/tests/integration_p13.rs
    - crates/ostro-repl-conformance/tests/regressions_p13.rs
    - crates/ostro-repl-conformance/tests/mutants_p13.rs
  modified:
    - crates/ostro-repl-conformance/src/scenarios/integration_p13.rs
    - crates/ostro-repl-conformance/src/mutants_p13.rs
    - crates/ostro-repl-conformance/src/harness/{mod.rs,process.rs}
    - crates/ostro-repl-conformance/src/scenarios/{module_gc,migrate,ryw,reassign}.rs
    - crates/ostro-repl-conformance/tests/p13_harness_smoke.rs
    - crates/ostro-server/src/{rerun.rs,cluster.rs}
    - crates/ostro-server/src/control/{lifecycle,p11_rerun,p11_dlq,module_retire,runs,migrate}.rs
    - crates/ostro-server/src/services/bulk.rs
    - crates/ostro-server/src/route/{mod,consistency,fanout}.rs
    - crates/ostro-server/src/p13/{cluster_api,promote}.rs
    - crates/ostro-server/tests/p13_routing_unit.rs
    - crates/ostro-cluster/src/placement/controller.rs
    - crates/ostro-cluster/tests/placement_controller.rs
    - crates/ostro-cli/src/commands/{modules_lc,migrate_runs}.rs
    - crates/ostro-cli/tests/{p13_cli.rs,snapshots/p13_cli__cluster_reassign_rebalance.snap,snapshots/p13_cli__modules_gc.snap}
key-decisions:
  - "D-13-18-1: every gated refusal is 409 conflict, details.reason feature_not_enabled (D-P13-17, compat::FeatureNotEnabled::HTTP_STATUS; the upload gate and migrate already were); the cluster API (reassign / rebalance) and retire move from 412 to 409; gRPC code ALREADY_EXISTS as every 409"
  - "D-13-18-2: a start passes before_meta_read only when it carries ostro-read-after m:<index>; without a meta token no barrier (a start must not wait on, or fail with, a metadata round trip); activation always passes it"
  - "D-13-18-3: an unkeyed restart (retry Restarted, rerun without key) places its new run on a partition this node leads; keyed restarts keep partition_for_key (dedupe), which still answers not_leader when another node leads it (needs a node-to-node start call: reported)"
  - "D-13-18-4: the P13 mutant matrix runs only when OSTRO_P13_REQUIRE_ALL (or OSTRO_P13_MUTANT) is set, so the whole-crate failpoint suite does not run ~25 extra clusters; 13-20 adds the dedicated step"
requirements-completed: []
duration: "about 7 h including the cluster runs"
completed: 2026-11-25
---

# Phase 13 Plan 18: Tier-B integration, mutant matrix and handover fixes Summary

**The Phase 13 features now run together under load in tier B (drain + remove, remove of a replica-holding voter, RYW through handoffs and reassignments, a start racing module GC, a full emulated rolling upgrade with the gated features used afterwards, reject-and-continue on replayed apply outcomes), every one of the eight Phase 13 mutants is mapped to scenarios in a REQUIRE_ALL matrix, and the non-oracle items of the Phase 13 handover are closed with tests. The two oracle changes from the handover are prepared on `wip/13-18-oracles` and NOT applied (user approval needed).**

## Cross-feature scenarios (`scenarios::integration_p13`, tier B)

| scenario | cluster | what is measured |
|---|---|---|
| `drain_node_with_replicas` | 4 static, RF 3 | a non-meta-leader node holding replicas and leading a partition: `drain` → no replica / ISR / leadership on it, RF 3 + min_insync 2 everywhere; `remove` → `removed` in status, out of the voters; continuous workload (every acked run completes), reassign oracles (replica identity below HW, acked on disk on ≥ min_insync), `removed_never_return` |
| `remove_replica_holding_voter` | 5 static, RF 3 | `remove(n)` without a drain: 4 active members, 3 voters + 1 learner, every partition RF 3 / ISR 3 without `n`; workload, reassign oracles, membership oracles (`voters_odd`, `joint_left_within(5 s)`, `removed_never_return`) |
| `ryw_through_handoffs_and_reassignment` | 4 static, RF 3 | 13-13's session checker (1,500 ops, 6 sessions) while graceful handoffs, explicit reassignments with a leader move onto the outsider, and rebalance passes run: 0 violations, no unexpected answers, then the reassign oracles |
| `start_during_gc_race` (failpoints) | 3, RF 3 | a v1 start passes `module_retired` and is held at `gc::start_hold`; v2 activated, v1 retired, GC rounds delete v1 (records gone on every node); released, the start submits and its run fails `module_gone` (asserted on the run's `failure`); `no_module_gone_for_live` (no v1 run live at retire time) |
| `emulated_rolling_upgrade_under_load` | own: 1 node at level 1 + 2 level-1 joins | sleeping `replay_demo` v1 runs + continuous workload; each node in turn graceful stop (controlled shutdown) → restart at this build's level; finalized stays 1 until the last node, then 2 everywhere; then a reassignment (p0 moves), a migrate (`repinned` = the sleeping runs, pinned to v3 on every node) and the level-2 upload gate (ext 101) on every node succeed; every acked run completes; `no_gated_before_finalize_from(1, ..)`; `no_reject_continue` over every node's replayed partition outcomes (821 outcomes, 12 logs) |
| `reject_continue_never_applies_newer_commands` | 3, RF 3 | the replayed outcome streams of every node's partitions hold no reject-and-continue (75 outcomes); a newer-format record (the last real command plus a field this build does not know) appended to partition 0's log on its leader fail-stops the replay at that offset (`NewerFormat`) |

Tier-B limits: `ostro_types::compat::level_max()` is per process, so an in-process node cannot apply at an emulated level 1; the rolling upgrade emulates the level where the server reads it per node (`level_max_override`: NodeVersion, gates, finalizer), and the reject-and-continue leg replays real WAL bytes plus one newer-format record through this build's apply. No real level-2 guest exists, so "upload a level-2 module" is the upload gate on every node (as 13-14). The version-gate cluster keeps RF 1 (raising RF needs AlterReplicas, which is gated), so "reassign a partition" moves p0's single replica.

## Handover items (non-oracle), each with a test

| item (source) | fix | test |
|---|---|---|
| restarts skip `check_not_retired` (13-15, 13-17) | `rerun::check_restart_module` on D1 retry `Restarted`, Phase 11 rerun (target module), DLQ restart, bulk restart: 409 `module_retired` | `regressions_p13::restart_of_retired_module_refused` (retry + rerun `version: source`) |
| restarts built on local handles, 503 `not_leader` (13-12, 13-15, 13-17) | `rerun::restart_partition`: an unkeyed restart's new run goes to a partition this node leads (uniform by the new uuid; the router sent the call to the source's leader) | `regressions_p13::restart_lands_on_a_led_partition` (8 retries / reruns, first attempt each, new run on a partition led by the serving node; random placement passes with p = 4^-8), `rerun::tests::pick_led_stays_on_led_partitions` |
| start / activation read meta without `before_meta_read` (13-15, 13-17) | activation: `Access::MetaReadWrite` (barrier first, token after); start: `before_meta_read` when it carries `ostro-read-after: m:<index>` (D-13-18-2) | `p13_routing_unit::{activation_passes_the_meta_barrier_first, start_with_a_meta_token_passes_the_meta_barrier}` |
| migrate gate decided per leg (13-17 dev. 8) | `RouteEnv::migrate_gate` → `control::migrate::entry_gate` (barrier + `compat::require(RepinRun)`) once at the entry node before the fan-out; legs keep their check | `p13_routing_unit::migrate_gate_is_decided_once_at_the_entry_node` (refused: no leg runs; dry run and forwarded legs never ask) |
| 409 vs 412 for `FeatureNotEnabled` (13-14, 13-16, 13-17) | 409 everywhere (D-13-18-1): cluster API reassign / rebalance and retire move from 412; CLI docs + p13_cli fake + 2 snapshots | `regressions_p13::gates_answer_409` (retire, rebalance, reassign on a level-1 node), `move_blocked_before_finalize` now requires 409 |
| `OutcomeRecord` producer (13-08, 13-10) | `harness::outcomes::partition_outcomes` (replay of the measured WAL through `apply_command`) | `regressions_p13::outcome_stream_replays_and_fail_stops`, used by the rolling upgrade and the reject-continue scenario |
| mutant → scenario mappings (13-10, 13-16, 13-17) | `mutants_p13::MAP` (below) | `mutants_p13::tests::*`, `tests/mutants_p13.rs` |
| tier-C joined-node traffic bypasses the proxy (13-08, 13-09) | `ProcCluster::proxy_joined_outbound(id)`: outbound through the proxy via `[[cluster.peers]]` + restart. Inbound needs a server-side per-dialer override key (not reserved): reported | `p13_harness_smoke::tier_c_joined_node_outbound_goes_through_the_proxy` (ignored without the failpoint binary) |
| `FakeModuleLifecycle` second retire 409 (13-03) | already fixed by 13-16 (deviation 6); `ostro-api/tests/phase13_routes.rs` asserts the idempotent retire | `phase13_routes` 9/9 |
| `upsert_self` overwrites the advertised `cluster_addr` (13-16; authorised) | `p13::promote::self_cluster_addr`: `[cluster] advertise`, else the stored address while the listener is a wildcard or on the stored port, else the bound listener | `p13::promote::tests::self_cluster_addr_keeps_the_advertised_address` |
| `request_rebalance` on the system clock (13-16) | takes `now_ms`; the cluster API passes `ctx.clock.now_ms()` | `placement_controller::rebalance_moves_carry_the_injected_clock` |

Not closed: keyed restarts (DLQ / bulk restart, keyed rerun) keep `partition_for_key` (the dedupe row fixes the partition) and still answer 503 `not_leader` when another node leads it; building the StartRun on another node needs a node-to-node call (architectural, reported). `isr_from_assignment` weak oracle (12-16) not touched (oracle).

## Oracle changes: HELD on `wip/13-18-oracles` (not applied)

Two commits on `wip/13-18-oracles` (on top of this plan's commits; never merged). Each has a self-test that shows the old reading gives a false result and the new one stays strict:

1. **`la_rule` + election audit read compacted meta logs** (13-09 / 13-10 handover). After a learner join the meta leader compacts its log into a snapshot. `la_rule` looked up `ElectLeader` entries in the log only and reported "no leader for epoch 1" on a correct cluster (false failure). The election replay started "not bootstrapped" from any snapshot, so a `SetPartitionLeader` right after a compacted replicated bootstrap passed (false pass). New: `la_rule::{check_from, check_scan_from}` take the snapshot state (the snapshot epoch is checked against its record, older epochs absent from the log are skipped as compacted, later epochs strict; `check` is the old reading exactly); `election::isr_only_from` seeds "placed" / "bootstrapped" from the snapshot (bootstrapped only for a partition with > 1 replica, so RF=1 `SetPartitionLeader` logs stay unflagged). `reassign_oracles` and the placement final checks then run both audits always. Self-test `tests/oracle_compacted_selftest.rs`: old `check_scan` reports a correct compacted log; against the old `election.rs` the test fails at the `SetPartitionLeader` assertion (line 217); the new readings accept the correct history and flag: wrong announcer of the snapshot epoch, wrong announcer after it, an epoch meta never committed, an announcement with the wrong epoch, a second placement, an election outside the published ISR.
2. **`RunResult::from_json` reads `failure`** (13-08 / 13-15 / 13-17 handover). `GET /v1/runs/{id}` reports a failed run's detail in `failure`; `from_json` read only `error`, so `no_module_gone_for_live` passed a live run that failed `module_gone` unless the scenario folded `failure` in by hand. New: `error` first, then `failure`. Self-test `tests/oracle_failure_field_selftest.rs`: against the old `p13_gc.rs` it fails at line 35 (`is_module_gone` false for a module_gone run); new: the live run is flagged; a run started after the retire, another failure kind and a completed run are not.

With change 1, `reassign::{rf_raised_on_join, drain_evacuates_replicas}` (the compaction case: two learner joins) pass with both audits running after the joins (local run on the branch; no "audits skipped" line). The integration scenarios do not depend on either change (`start_during_gc_race` reads `failure` itself, as 13-15's scenario does).

## Phase 13 mutant matrix (`mutants_p13`, `tests/mutants_p13.rs`)

Final run: `OSTRO_P13_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants_p13 -- --test-threads=1 --nocapture` at `563538c` (cluster slot 1, dell-2): **8 of 8 mutants bitten, 8 × "FAILED as expected", 0 SKIP, 1161 s**.

| mutant | scenario that bites (baseline / with mutant) | the measured failure |
|---|---|---|
| `upgrade::mutant::emit_ungated` | `move_blocked_before_finalize` (132 s / 122 s); also mapped: `migrate_refused_before_finalize` (baseline 144 s) | `p13_upgrade: node 1: reassign before finalize answered 200 OK` |
| `upgrade::mutant::reject_and_continue` | `reject_continue_never_applies_newer_commands` (6 s / 9 s) | `p13_upgrade: node 1 partition 0 offset 7: Rejected(Invalid(Empty)) for 161 command bytes (reject-and-continue)` |
| `member::mutant::skip_quorum_check` | `dead_voter_replaced_by_learner` (16 s / 107 s) | `membership: the dead voter replaced by the live learner not reached within 90s` |
| `member::mutant::no_leave_joint` | `explicit_joint_left_after_leader_kill` (17 s / 106 s) | `membership: the joint configuration left by the new leader not reached within 90s` (voters_outgoing stays [1, 2, 3]) |
| `reassign::mutant::raise_min_insync_early` | `rf_raised_on_join` (66 s / 215 s); also mapped: `promote_running_single_node_to_three` (baseline 68 s) | `reassign: every partition at RF 3 not reached within 90s` (all at RF 1) |
| `route::mutant::serve_without_lease` | `zombie_leader_never_serves_stale` (65 s / 61 s) | `lease: the zombie served a read +5.99 s after the cut (bound 750ms)` |
| `gc::mutant::single_scan` | `gc_survives_leader_changes` (10 s / 8 s) | `p13_gc: a leader change between the scans did not postpone the round` |
| `migrate::mutant::skip_seq_check` | `concurrent_growth_seq_mismatch` (7 s / 5 s) | `migrate: seq_mismatch: want {<run>}, got {}` (the grown run was repinned) |

After `explicit_joint_left_after_leader_kill`'s fix (`d7b82a2`) the `no_leave_joint` row was re-run alone (`OSTRO_P13_MUTANT=no_leave_joint`): baseline 16 s, FAILED as expected in 111 s. The first full run (at `dc45f61`) bit 6 of 8: `skip_quorum_check` passed `force_remove_dead_node` and `no_leave_joint` passed `join_grows_to_five_voters` / `meta_leader_removal_transfers_first` (deviation 4); the two new scenarios were added and bit in a filtered run, then in the final run above.

The matrix runs only when `OSTRO_P13_REQUIRE_ALL` (or `OSTRO_P13_MUTANT=<substring>`) is set (D-13-18-4), so the whole-crate failpoint suite does not run its ~20 clusters twice; 13-20 adds the dedicated CI step (hunk below).

## Verification

Cluster runs used `ostro-k8s-ci` (slot 1 for targeted runs, `--parallel` for the plan checks); local builds used `target/slot-2`.

- **clippy `-D warnings`** on ostro-server, ostro-repl-conformance, ostro-cluster, ostro-cli, with and without failpoints: clean. fmt clean.
- **Unit tests:** `p13_routing_unit` 29/29 (incl. the three new), `placement_controller` 10/10 (incl. `rebalance_moves_carry_the_injected_clock`), ostro-server lib `pick_led_stays_on_led_partitions` + `self_cluster_addr_keeps_the_advertised_address`, `p13_cli` 10/10 (snapshots at 409), `phase13_routes` 9/9, `p13_module_retire` 8/8, `migrate_runs` 2/2 (failpoints), mutants_p13 lib tests 3/3, `p13_skeleton` 3/3.
- **Tier B, new targets:** `integration_p13` with failpoints 7/7 (233 s; before the two membership scenarios), default build 6/6 (231 s); `regressions_p13` 4/4 (13 s); `explicit_joint_left_after_leader_kill` 5/5 after its fix; `dead_voter_replaced_by_learner` + `explicit_joint_left_after_leader_kill` 2/2. Touched groups: `module_gc` (50 s), `reassign::move_blocked_before_finalize` (134 s, now 409 only).
- **Mutant matrix:** REQUIRE_ALL 8/8 (table above).
- **Tier-B suite ×3** (`cargo test -p ostro-repl-conformance --locked -- --test-threads=1`, default build, at `d7b82a2`, cluster slot 1):

| target | passed / ignored | run 1 | run 2 | run 3 |
|---|---|---|---|---|
| unittests | 6 / 0 | ok 0.00s | ok 0.00s | ok 0.00s |
| blobs | 1 / 0 | ok 21.44s | ok 9.90s | ok 10.34s |
| bootstrap | 1 / 0 | ok 121.76s | ok 38.43s | ok 43.41s |
| chaos_proxy | 8 / 0 | ok 1.26s | ok 1.21s | ok 1.22s |
| divergence | 1 / 0 | ok 67.13s | ok 67.23s | ok 67.24s |
| failover | 1 / 0 | ok 40.27s | ok 44.82s | ok 54.61s |
| fault_layer | 9 / 0 | ok 2.70s | ok 2.70s | ok 2.70s |
| fetch_path | 1 / 0 | ok 45.75s | ok 30.67s | ok 36.05s |
| handoff | 1 / 0 | ok 42.40s | ok 42.02s | ok 32.61s |
| harness_smoke | 2 / 0 | ok 11.27s | ok 4.29s | ok 4.15s |
| hw | 1 / 0 | ok 26.25s | ok 17.53s | ok 13.16s |
| integration_p13 | 8 / 0 | ok 299.07s | ok 255.23s | ok 242.54s |
| isr_honesty | 1 / 0 | ok 32.59s | ok 33.66s | ok 37.73s |
| membership | 6 / 0 | ok 142.36s | ok 152.07s | ok 161.71s |
| meta_log_reader | 2 / 0 | ok 0.00s | ok 0.00s | ok 0.00s |
| migrate | 5 / 0 | ok 237.77s | ok 237.28s | ok 269.57s |
| min_insync | 1 / 0 | ok 31.37s | ok 31.55s | ok 36.74s |
| module_gc | 1 / 0 | ok 54.31s | ok 56.83s | ok 45.20s |
| mutants | 1 / 0 | ok 0.00s | ok 0.00s | ok 0.00s |
| mutants_p13 | 1 / 0 | ok 0.00s | ok 0.00s | ok 0.00s |
| object_storage | 1 / 0 | ok 18.12s | ok 17.33s | ok 17.52s |
| one_path | 1 / 0 | ok 1.86s | ok 1.90s | ok 1.93s |
| oracle_selftest | 11 / 0 | ok 5.63s | ok 5.38s | ok 21.60s |
| p13_harness_smoke | 4 / 1 | ok 62.53s | ok 62.49s | ok 62.53s |
| p13_oracle_selftest | 11 / 0 | ok 0.00s | ok 0.00s | ok 0.00s |
| p13_skeleton | 3 / 0 | ok 0.00s | ok 0.00s | ok 0.00s |
| placement | 1 / 0 | ok 5.85s | ok 9.05s | ok 9.18s |
| process_smoke | 2 / 1 | ok 2.98s | ok 0.92s | ok 0.94s |
| promotion | 3 / 0 | ok 163.49s | ok 223.87s | ok 162.98s |
| reassign | 8 / 0 | ok 328.39s | ok 341.70s | ok 400.30s |
| regressions_p13 | 4 / 0 | ok 8.79s | ok 13.24s | ok 14.05s |
| routing | 6 / 0 | ok 35.39s | ok 51.40s | ok 34.47s |
| runner_selftest | 3 / 0 | ok 6.39s | ok 6.25s | ok 6.51s |
| ryw | 10 / 0 | ok 222.13s | ok 231.73s | ok 245.86s |
| skeleton | 2 / 1 | ok 0.00s | ok 0.00s | ok 0.00s |
| stale_epoch | 3 / 0 | ok 18.50s | ok 12.91s | ok 13.04s |
| standby | 1 / 0 | ok 3.29s | ok 3.22s | ok 13.41s |
| version_gate | 13 / 0 | ok 882.70s | ok 934.91s | ok 884.23s |
| zombie | 1 / 0 | ok 9.37s | ok 9.16s | ok 9.15s |

Whole runs: exit 0 in 3025 s, 2953 s, 2958 s; 0 failed in every target. Targets with only ignored (tier-C / measurement) or failpoints-only tests: bench_rf3, cluster_campaign, failover_p99, level_emulation_apply, process_divergence, process_mutants, process_power_loss, red_run, scenario.

- **Plan checks (`ostro-k8s-ci --parallel`):**

  Three full runs (each ~62-82 min wall, sharing the cluster with the Phase 12 campaign and other agents). Every step passed at least once at the final code or at an earlier 13-18 commit with the same code for that step; the failures, by class:

  | run | failed steps | cause | resolution |
  |---|---|---|---|
  | `dc45f61` | failpoint suites (fp2), musl zigbuild test (musl2) | `p13_skeleton::p13_mutant_matrix_names_registered_scenarios`: matrix wrapper name not in the registry | fixed (`register migrate_refused_before_finalize`) |
  | `dc45f61` | one log path + P12 grep gates | clock gate matched the clock type's name in a doc comment | fixed (doc comment) |
  | `dc45f61`, `d7b82a2` | cargo test (test2) | `ostro backup_cli::bucket_online_and_offline_round_trips`: `/readyz not 200 within 60s`, first unit of test2 right after a cold 35-min compile ("target cache 8.0K") | load: 3/3 alone (5 s each) on slot 1; passed in test2 at `563538c` (warm cache) |
  | `563538c` | failpoint suites (fp2) | `integration_p13::explicit_joint_left_after_leader_kill`: scenario race (13-18's own) | fixed (`explicit_joint scenario waits for the joint change only`), 5/5 + matrix bite after the fix; fp2 at `d7b82a2` passed integration_p13 |
  | `563538c` | cargo test (test2) | `ostro ops_e2e::{sc1 deploy timed out, sc3 readyz 60s}` | load (PUT / readiness timeouts); `ops_e2e` 2/2 alone at `d7b82a2` (slot 1, 17 s each) |
  | `563538c` | phase 7 legs, musl zigbuild test | `fencing_chaos::chaos_smoke` PUT module timed out; `flow_e2e::{pause_run: server not started in 240 s, throttle_vs_rate_limit: PUT timed out}` | load (flow_e2e and the Phase 7 legs are on the flaky list; chaos_smoke passed at `d7b82a2`); `flow_e2e::{pause_run, throttle_vs_rate_limit}` 2/2 alone at `d7b82a2` |
  | `563538c`, `d7b82a2` | cargo deny check | RUSTSEC-2026-0600 against `kube-client 5.1.0` (via `ostro-bridge`), published during this plan | not 13-18's (no dependency change); main is affected too |
  | `d7b82a2` | failpoint suites (fp2), musl zigbuild test (musl2) | `migrate::{diverging_runs_never_repinned, passing_runs_repinned_cluster_wide}`: a submitting migrate got 503 `meta_unavailable` ("metadata read barrier did not complete") from one leg after other legs had repinned; the retry then reported only the remaining runs | known issue, not caused by 13-18 (the entry gate refuses before any leg; this 503 is a leg's own barrier under meta raft stalls); the `migrate` group (failpoints, 6 tests) 3/3 alone at `d7b82a2` (251 s, 287 s, 241 s). Recorded as a known issue for 13-17's owner / 13-20: a submitting migrate is not atomic across legs, and a retried call reports only the runs left (13-17 deviation 8 family) |

  test2 re-run with `--no-fail-fast` at `d7b82a2` (slot 1, all 356 units of the group, 71 min): 358 test binaries ok; 2 failures: `ostro worker_failover::oscillation` (flaky list, confirmed timing-only by p13-fixes) and `migrate::diverging_runs_never_repinned` (the known issue above, the third time under load; it passed 3/3 alone and in all three tier-B runs).

  At `d7b82a2` every other step passed: fmt, clippy, warning-free build, sdk, guests, polyglot (all 8), ostro-build polyglot, semantics sleepers, failpoint list sync, replication mutant matrix, server campaign smoke, phase 7 legs, vendored / deny fixtures / chronik / grep gates, workflow validation, musl release, **musl binary size < 60 MB**, test1.



## Task commits

1. `a621fac` fix(13-18): every feature gate answers 409 feature_not_enabled
2. `a20b197` fix(13-18): request_rebalance takes the injected clock
3. `17b9cc2` fix(13-18): a restart keeps the advertised cluster_addr
4. `eeac96f` fix(13-18): restarts refuse retired modules and land on a led partition
5. `f60613e` fix(13-18): meta barrier before the retired checks; migrate gate at the entry
6. `50e2a37` feat(13-18): OutcomeRecord producer for no_reject_continue
7. `2266645` test(13-18): cross-feature integration scenarios
8. `2ad100f` test(13-18): Phase 13 mutant matrix
9. `8f0f633` fix(13-18): register migrate_refused_before_finalize for the P13 matrix
10. `d259ce7` fix(13-18): request_rebalance doc comment passes the clock gate
11. `563538c` test(13-18): membership driver scenarios for the two non-biting mutants
12. `d7b82a2` fix(13-18): explicit_joint scenario waits for the joint change only

Held (not on main): `wip/13-18-oracles` = two `oracle(13-18, HELD)` commits on top of `d7b82a2`.

## Deviations from Plan

1. **[Scope, orchestrator] Files outside `files_modified`:** the handover items needed `control/{lifecycle,p11_rerun,p11_dlq,runs}.rs`, `services/bulk.rs`, `cluster.rs` (authorised: 13-02 complete), `p13/promote.rs`, `ostro-cli` docs / tests / snapshots, `ostro-server/tests/p13_routing_unit.rs`, `ostro-cluster/tests/placement_controller.rs`, and visibility-only `pub(super)` in the scenario files of 13-10 / 13-13 / 13-15 / 13-17 (`reassign.rs`, `ryw.rs`, `module_gc.rs`, `migrate.rs`). No behaviour change in those scenario files except: `reassign.rs`'s `feature_not_enabled` helper requires 409 (was 409 | 412) after D-13-18-1, and `migrate.rs` registers `migrate_refused_before_finalize` in its group (found by the plan check: `p13_skeleton` requires matrix names in the registry).
2. **[Rule 4, reported] Keyed restarts are not routed** (D-13-18-3): DLQ / bulk restart and keyed reruns place the new run by `partition_for_key(workflow, dedupe key)`; when another node leads that partition they still answer 503 `not_leader`. Building the StartRun (copied payloads, pinned module, lineage, rerun prefix) on another node needs a node-to-node call (new proto / reservation). Unkeyed restarts (D1 retry, rerun without key) are fixed.
3. **[Decision] Start barrier only with a meta token** (D-13-18-2): a full ReadIndex barrier on every start would make starts wait on (and fail with) the metadata group and add a round trip to every start. A client that retires / activates and then starts elsewhere sends `ostro-read-after: m:<index>` (the meta write's token) and gets the barrier.
4. **[Finding] Two mutants did not bite their 13-09 scenarios** (first REQUIRE_ALL run): `member::mutant::skip_quorum_check` (the API's own `check_remove` refuses first; tier-B liveness is raft contact) and `member::mutant::no_leave_joint` (Auto transitions are left by raft itself). Two new scenarios catch them (`dead_voter_replaced_by_learner`, `explicit_joint_left_after_leader_kill`, the latter uses 13-09's unit setup: an explicit-transition joint change through the meta leader, then the leader killed). The mutants are unchanged.
5. **[Not closed, reported] Tier-C joined-node proxy routing.** `ProcCluster::proxy_joined_outbound` (write the per-link proxy addresses as `[[cluster.peers]]`, plus the test CA files config validation requires next to peers, then restart) was tried on the cluster: the joined node restarts, but isolating it did not make the meta leader see it dead within 30 s, so its outbound traffic still bypassed the proxy (the address book docs say configured peers are pinned; root cause not found). The commit was dropped; inbound needs a server-side per-dialer override key anyway. Left for 13-19 with this finding.
6. **[Adaptation] Oracle-input folding:** `start_during_gc_race` reads the run's `failure` itself (as 13-15's scenario does) because the `RunResult::from_json` fix is held.
7. **[Adaptation] Matrix on demand** (D-13-18-4): `tests/mutants_p13.rs` returns early unless `OSTRO_P13_REQUIRE_ALL` / `OSTRO_P13_MUTANT` is set.
8. **[Process] Cluster slot 2:** the first targeted run was started on cluster slot 2, which the brief reserves for the Phase 12 campaign (dell-3) since 2026-11-25; I stopped it after about 30 minutes (runner terminated, its Job deleted by the runner's trap). Every later run used slot 1 or the parallel lanes.
9. **[Known issue, recorded with evidence] 13-17's migrate scenarios under load:** `diverging_runs_never_repinned` (3 times) and `passing_runs_repinned_cluster_wide` (once) failed in loaded runs (plan-check fp2 / musl2, the full-workspace test2 re-run): a submitting migrate got 503 `meta_unavailable` from one leg's ReadIndex barrier (meta raft stalls under node I/O pressure) after other legs had repinned, and `migrate_ok`'s retry then reported only the runs that were left, while the run records were already pinned to the target. 0 failures in 6 unloaded runs (3 alone, 3 inside the tier-B ×3). Not caused by 13-18 (the new entry gate refuses before any leg). Resolution needs a decision: make a retried migrate report runs already on the target as repinned (product, `control/migrate.rs`), or have the scenarios verify pins after a partial 503 instead of the retry's report (scenario).
10. **Worktree / target:** scratchpad worktree, `target/slot-2` for local builds; the plan's `/tmp/ostro-wt-13-18` / `/tmp/ostro-target-13-18` were not used (executor brief).

## ci.yml / ci-local hunks (not applied; 13-20 owns both files)

`scripts/ci-local.sh` (next to `check_repl_mutants`, plus `step "Phase 13 mutant matrix" check_p13_mutants` after the replication matrix step):

```bash
# Phase 13 mutant matrix (13-18, D-P13-25): every one of the eight mutants must bite
# (>= 8 "FAILED as expected" lines) and none may be skipped.
# Keep in sync with the "Phase 13 mutant matrix" step of .github/workflows/ci.yml.
check_p13_mutants() {
  local out bites
  out="$(OSTRO_P13_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants_p13 -- --test-threads=1 --nocapture 2>&1)" || { echo "$out"; return 1; }
  grep -E 'MUTANT |test result' <<<"$out" || true
  grep -Eq 'test result: ok\. [1-9][0-9]* passed' <<<"$out" || { echo "the P13 mutant matrix ran no test"; return 1; }
  bites="$(grep -c 'FAILED as expected' <<<"$out" || true)"
  if [[ "$bites" -lt 8 ]]; then echo "only ${bites} P13 mutant bites (need >= 8)"; return 1; fi
  if grep -q 'SKIP' <<<"$out"; then echo "a P13 mutant was skipped"; return 1; fi
  echo "${bites} P13 mutant bites, none skipped"
}
```

`.github/workflows/ci.yml` (after "Replication mutant matrix", same `if:`):

```yaml
      - if: matrix.name == 'linux-x86_64-gnu'
        name: Phase 13 mutant matrix
        # keep in sync with check_p13_mutants in scripts/ci-local.sh (13-18): all eight
        # Phase 13 mutants must bite and none may be skipped
        run: |
          out="$(OSTRO_P13_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants_p13 -- --test-threads=1 --nocapture 2>&1)" || { echo "$out"; exit 1; }
          grep -E 'MUTANT |test result' <<<"$out" || true
          grep -Eq 'test result: ok\. [1-9][0-9]* passed' <<<"$out"
          test "$(grep -c 'FAILED as expected' <<<"$out")" -ge 8
          ! grep -q 'SKIP' <<<"$out"
```

The new tier-B targets (`integration_p13`, `regressions_p13`) run in the existing whole-crate steps (`cargo test`; failpoint suites `ostro-repl-conformance`).

## Handover

- **13-19 (tier C):** joined-node proxy routing is open (deviation 5); `emulated_rolling_upgrade_under_load` is the tier-B shape of the tier-C upgrade (graceful stop = controlled shutdown, restart at native level, gated features after finalization, outcome replay for `no_reject_continue` via `harness::outcomes`); `ProcCluster` has no outcome producer of its own (the replay works on any `LogScan`).
- **13-20:** the two ci hunks above; RESERVATIONS: failpoint `gc::start_hold` (13-18, failpoints build, start path); decision D-13-18-1 (409 for every `feature_not_enabled`; docs: the 412 mentions in 13-14 / 13-16 handovers are obsolete; OpenAPI already lists 409 only); `OSTRO_P13_MUTANT` env (local matrix filter) next to `OSTRO_P13_REQUIRE_ALL`.
- **User decisions:** the two held oracle changes on `wip/13-18-oracles`; keyed-restart routing (deviation 2); the migrate partial-retry known issue (deviation 9).
- **Main-wide, not 13-18:** `cargo deny check` fails since today on RUSTSEC-2026-0600 (`kube-client 5.1.0` via `ostro-bridge`; fixed in kube 5.2.0).
- **Open, not touched:** `isr_from_assignment` weak oracle (12-16); 7-71 s process stalls under node I/O pressure (p13-fixes).
