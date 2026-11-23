---
phase: 12-replicated-partition-logs
plan: 16
subsystem: replication-conformance
tags: [replication, conformance, tier-b, mutants, isr, fresh-term, self-demotion, snapshot-install, handles, clus-01, clus-02, clus-03, clus-04]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-14 integrated server (ClusterServices, MetaAssignments, NodeLeaderHints, red/green table and 9 pending entries); 12-06/12-07 scenarios, oracles and mutant matrix; 12-10 controller/request_fresh_term; 12-11 fetch/publisher; 12-12 install; 12-13 role drivers"
provides:
  - "controller::read_barrier (linearizable read point via a no-op Delete(Member(0), v0) proposal); request_fresh_term decides proposals from a fresh view; a new meta leader's controller takes the barrier before planning"
  - "IsrPublisherConfig::propose_timeout (server: session_timeout); publisher never re-adds an ISR-dropped replica before it fetched again"
  - "PartitionLog::note_fetch_envelopes / max_envelopes_per_fetch (measured batching in ReplicaStatus)"
  - "roles::step_down refuses submits before the leader services stop; NodeLeaderHints treats a Leader outside the led set as stepping down; failpoint repl::before_services_stop"
  - "ServerControlPlane / bridge PartitionTable / P7 NodeRefs / RunWatchers / run_spans / dlq_hook never cache PartitionHandles (control::watch::next_actions re-subscribes per machine); dlq_hook catch-up for partitions led after the start"
  - "ServerOptions::wal_rotation_bytes test hook; ClusterSpec::wal_rotation_bytes"
  - "FaultLayer::response_lost (blackhole/cut swallow in-flight answers)"
  - "tests/scenario.rs (OSTRO_REPL_SCENARIO / OSTRO_REPL_REPEAT)"
affects: [12-17, 12-18]
tech-stack:
  added: []
  patterns:
    - "decide-then-propose from a view at least as new as the call (read barrier) wherever a proposal is planned from the local meta view"
    - "never hold a PartitionHandle beyond one use: look it up in the node; hold only action receivers and re-subscribe when the machine is replaced"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/scenario.rs
  modified:
    - crates/ostro-cluster/src/{controller,isr_publisher,fetch,config}.rs
    - crates/ostro-log/src/{partition_log,replicated}.rs, crates/ostro-log/src/replica/tracker.rs
    - crates/ostro-partition/src/roles.rs
    - crates/ostro-server/src/{cluster,server,effects,p7,devtime}.rs, control/{mod,watch,...}.rs, services/{dlq_hook,run_spans}.rs
    - crates/ostro-repl-conformance/src/harness/{fault,inproc,mod}.rs, scenarios/{bootstrap,divergence,isr_honesty}.rs, pending/{core,rest}.rs
key-decisions:
  - "Read barrier = a proposal that can never change state (Delete of Member(0) at expected version 0): MetaHandle::propose returns only after this node applied it, so no new RPC or proto change is needed"
  - "The divergence failures were a harness fidelity bug, not a product bug: an in-flight long-poll Fetch delivered the 'unreplicated' tail after the blackhole was installed; the record was legitimately committed by the next leader (KIP-101). Blackhole/cut now also swallow answers of calls in flight"
  - "Bootstrap needs small WAL segments in the test cluster (retention deletes whole segments; default rotation 250 MB): ServerOptions test hook, not a user config key"
  - "The three bootstrap scenarios stay pending: installs now work end to end, but the disk oracle demands every acked StartRun in >= min_insync WALs, which retention legitimately removed. Changing that oracle is a decision for the user (see Open items); it was not changed here"
requirements-completed: []
duration: ~6 h
completed: 2026-11-23
---

# Phase 12 Plan 16: Tier-B conformance green + mutant matrix Summary

**27 of 30 tier-B scenarios are green with no pending entry (the 3 bootstrap scenarios install correctly now but stay pending on a disk-oracle decision), and all 11 tier-B mutant pairs bite under `OSTRO_REPL_REQUIRE_ALL=1`, each on its target oracle. Eight production bugs were fixed with regression tests (stale-view fresh terms, unbounded `AlterIsr`, demotion holding requests, cached handles pinning follower stores, publisher undoing controller shrinks, missing batching metric, dishonest `in_sync` rows, no trim without small segments), plus one harness fidelity bug that was the whole "divergent command below HW" class.**

## Commits

| Commit | Message |
| --- | --- |
| cfd8296 | fix(12-16): batching_on_user_partitions: max_envelopes_per_fetch was never recorded |
| 9cc975c | fix(12-16): isr_slow_follower_shrinks: a lost AlterIsr held the shrink for 10 s |
| dcbf662 | fix(12-16): isolated_leader_rejoin_truncates: fresh terms were decided from a stale meta view |
| 0598a1c | fix(12-16): two_quick_leader_changes_truncate: a blackholed link still delivered in-flight answers |
| ebe7525 | fix(12-16): isolated_leader_cannot_commit: a demoted leader held requests until its services stopped |
| d82a7e3 | fix(12-16): bootstrap: no trim with 250 MB segments; cached handles pinned follower stores |
| f7cd300 | fix(12-16): ISR publisher re-added a controller-shrunk node before it fetched again |
| 8c2b10a | fix(12-16): replication status reported never-fetched replicas as in sync |
| 3519eb6 | test(12-16): mutants bite on their target oracles; pending lists down to bootstrap |
| 264a676 | fix(12-16): node_behind_log_start_reinstalls: trim check read the killed node's log |
| (docs) | docs(12-16): summary, STATE line, reservations |

## Bugs found (scenario, root cause, fix, regression test)

| # | Scenario | Root cause | Fix | Regression test |
| --- | --- | --- | --- | --- |
| 1 | batching_on_user_partitions | `ReplicaStatus.max_envelopes_per_fetch` was hard-coded 0 | the fetch handler records each data-carrying response's envelope count (`PartitionLog::note_fetch_envelopes`, atomic max) | `fetch_service::validation_order_and_every_error_code` (0 before, 4 after a 4-batch response) |
| 2 | isr_slow_follower_shrinks | `IsrPublisher` awaited `AlterIsr` without its own bound; a request lost on the healed link waited for the forwarder's 10 s deadline before the unknown-outcome path re-proposed | `IsrPublisherConfig::propose_timeout` (server: `session_timeout`): unanswered = unknown outcome, same CAS re-proposed, pending change stays counted | `isr_publisher::hung_alter_isr_is_retried_after_the_propose_timeout` |
| 3 | isolated_leader_rejoin_truncates | after self-demotion, contact back made the role driver call `request_fresh_term`, which read the not-yet-caught-up local view (still "leader at epoch 1, ISR [1,2,3]") and proposed `ElectLeader` from that stale ISR (apply said StaleVersion; the election oracle audits proposals) | `controller::read_barrier` (no-op `Delete(Member(0), v0)`: propose returns after local apply) before the first proposal of `request_fresh_term`; a new meta leader's controller takes it before planning (bounded 2 s) | `controller::fresh_term_after_a_cut_decides_from_the_current_view` (fails without: stale `ElectLeader{isr [1,2,3], v1}` forwarded) |
| 4 | two_quick_leader_changes_truncate (3/3), leader_dies_with_unreplicated_writes (flaky) | **not a product bug.** Disk dump: the "divergent" kip2 start sat at offset 14 (epoch 2) on all three replicas. `hold_tail` blackholes followers' Fetch, but the fault proxy decided only at request arrival, so the follower's long-poll Fetch already parked at the leader returned the first "unreplicated" append; the follower made it durable, its next progress report was blackholed (never acked), and the next leader, elected from the ISR with the record, committed it: correct KIP-101/279 behaviour for a record that did replicate | harness fidelity: a response that becomes ready while a Blackhole (or Cut) rule matches its link is lost (`FaultLayer::response_lost`, no RNG draw) | `fault_layer::blackhole_swallows_the_answer_of_a_call_in_flight` (returned Ok before) |
| 5 | isolated_leader_cannot_commit | self-demotion fired in time, but `step_down` stopped the leader services first; the dispatcher's shutdown waits up to `min(lease_ttl, 5 s)` for executor jobs that can never commit, and only then refused submits and changed the role, so the API gate still saw Leader and requests hung | refuse pending/new submits and drop the handle epoch right after leaving the led set, before stopping services; `NodeLeaderHints` treats Leader-but-not-led as stepping down (503 + hint) | `role_lifecycle::failpoints::demotion_refuses_submits_before_the_services_stop` (failpoint `repl::before_services_stop` holds the step-down 2 s; fails with the old order) |
| 6a | bootstrap ×3 ("leader log starts at 0") | not a retention-floor bug: every pass logged floor = snapshot offset, "nothing to trim". Retention deletes whole WAL segments; segments rotate at 250 MB, so a test-sized log never trims | `ServerOptions::wal_rotation_bytes` test hook (binary never sets it) + `ClusterSpec::wal_rotation_bytes`; bootstrap specs use 2 KiB, like the ostro-partition trim suites | scenario-level (leaders now trim; installs happen) |
| 6b | bootstrap ×3 (then: "state store still in use; users=1", 12-14 open item) | control plane, bridge partition table, P7 NodeRefs, RunWatchers, run_spans and dlq_hook held `PartitionHandle` clones for the node's lifetime (each pins the store and goes stale on restart), so every follower install was refused after `quiesce_timeout` | handles looked up in the node per use everywhere; receivers re-subscribe when the machine is replaced (`control::watch::next_actions`); dlq_hook catch-up for partitions led after the start (12-14 open item) | `cluster_api`: a running server's follower partition quiesces for an install within 5 s and follows again after reopen (fails on main: quiesce times out) |
| 7 | seen in zombie / failover logs | after liveness declared a node dead, the controller's `AlterIsr` dropped it and the leader's publisher re-added it at once (tracker row still "caught up" for `replica_lag_max`): `AlterIsr` ping-pong v2/v4/v6..., HW stalling on the dead node each time | the publisher re-adds a replica an adopted ISR dropped only after it fetched again since then | `isr_publisher::controller_shrink_is_not_undone_without_a_new_fetch` (fails without the gate) |
| 8 | status (12-14 open item) | `ReplicaTracker::status` set `in_sync` from the join grace alone | `in_sync` requires a fetch | `replica_unit::tracker_status_never_fetched_is_not_in_sync` |
| 9 | node_behind_log_start_reinstalls | scenario bug: `trim_above` measured the leader recorded before node 3's kill (node 3 itself for a partition it led) | measure the partition's current leader | scenario-level |

## Scenario / harness changes (with justification)

No oracle was changed. Every change below adds checking or fixes a demonstrated scenario/harness bug.

- **Fault layer** (bug 4): blackhole/cut now also swallow answers of calls in flight. A real dead link loses response packets; the old proxy delivered data appended after the hold was installed, which made the "unreplicated tail" premise false.
- **Bootstrap specs** (6a): 2 KiB WAL rotation (test hook). **`trim_above`** (9): current leader instead of a stale one.
- **`isr_honesty::honest_sample`**: durable ends are measured after the status instead of before-and-after with a discard on any change. Durable ends only grow, so "still below the reported HW afterwards" is a sound violation; no sample is discarded for a moving end. Before, with runs still executing, no checkable sample existed and `isr_from_assignment` bit on "no stable status sample" instead of the status oracle.
- **`divergence::truncated`**: when the old leader never rejoins, `none_committed` (below the current HW) and `acked_on_disk` run before the rejoin timeout is reported, so `no_truncate` / `hw_leader_leo` are named by the replica / disk oracles instead of "not rejoined within 22s". The success path is unchanged.
- **`tests/scenario.rs`**: on-demand runner (`OSTRO_REPL_SCENARIO=a,b OSTRO_REPL_REPEAT=n`, `RUST_LOG` for server logs). Debug aid only.

## Final red run (all tier-B scenarios)

`OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1` at 3519eb6, 249.6 s, `test result: ok`:

| scenario | pending | result |
| --- | --- | --- |
| placement_rf3_three_replicas | no | pass |
| placement_rf1_single_node | no | pass |
| isr_empty_follower_not_in_isr | no | pass |
| isr_lagging_follower_rejoins | no | pass |
| isr_slow_follower_shrinks | no | pass |
| hw_is_min_over_isr | no | pass |
| hw_rf3_commits_within_rounds | no | pass |
| fetch_with_stale_epoch_fenced | no | pass |
| fetch_with_future_epoch_unknown | no | pass |
| leader_dies_with_unreplicated_writes | no | pass |
| isolated_leader_rejoin_truncates | no | pass |
| two_quick_leader_changes_truncate | no | pass |
| failover_elects_isr_member | no | pass |
| whole_isr_dead_goes_offline | no | pass |
| resigned_leader_moves | no | pass |
| writes_stop_below_min_insync | no | pass |
| writes_resume_when_follower_rejoins | no | pass |
| followers_are_hot_standbys | no | pass |
| empty_node_bootstraps_from_snapshot | yes | FAIL disk: acked runs below the trimmed log start on 0 nodes' WAL |
| node_behind_log_start_reinstalls | yes | FAIL (at 3519eb6: stale-leader trim check, fixed in 264a676; since then FAIL disk as above) |
| install_stream_cut_resumes | yes | FAIL disk: as above |
| slow_read_longer_than_max_wait_completes | no | pass |
| max_size_envelope_replicates | no | pass |
| batching_on_user_partitions | no | pass |
| blob_before_commit_survives_failover | no | pass |
| one_path_rf1_hw_through_tracker | no | pass |
| isolated_leader_cannot_commit | no | pass |
| remote_chain_equals_committed_prefix | no | pass |
| remote_chain_never_above_hw | no | pass |

**27/30 pass, 3 pending (bootstrap), 0 harness errors.** The bootstrap rows now get through trim, `SNAPSHOT_REQUIRED`, staging, quiesce, install, reopen, rejoin and identical state, and fail only in `final_checks_with`'s disk oracle (see Open items).

Consecutive full green runs of every non-pending scenario: (1) the group run (`--test placement --test hw --test isr_honesty --test stale_epoch --test min_insync --test failover --test divergence --test standby --test fetch_path --test blobs --test bootstrap --test zombie --test object_storage --test one_path --test-threads=1`), (2) the red run above, (3) the full crate suite `cargo test -p ostro-repl-conformance --locked --no-fail-fast -- --test-threads=1` at 264a676 (26 test binaries incl. every group and the harness/oracle/runner self-tests: 52 passed, 0 failed). The mutant matrix's baselines re-ran the 11 mapped scenarios green in between. Individually (`tests/scenario.rs`): isolated_leader_rejoin_truncates, two_quick_leader_changes_truncate and leader_dies_with_unreplicated_writes 3/3 each after their fixes (two_quick was 0/3 before), isolated_leader_cannot_commit 2/2.

## Mutant matrix

`OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture` at 3519eb6 (235 s, `test result: ok`, **11 pairs bitten, no SKIP**), every bite on its target oracle:

| mutant | scenario | oracle that fired |
| --- | --- | --- |
| isr_from_assignment | isr_empty_follower_not_in_isr | status: node 3 is reported in the ISR but its measured durable LEO 1 < HW 7 |
| hw_leader_leo | leader_dies_with_unreplicated_writes | disk: acked run ... on disk on 1 node(s) [2] < min_insync 2 |
| hw_leader_leo | hw_is_min_over_isr | hw: leader 1 HW 4 above ISR member 3's measured durable end 2 |
| count_nondurable_leo | (tier C: follower_power_loss, 12-17) | TIER-C |
| skip_epoch_check | fetch_with_stale_epoch_fenced | fence: answered None (leader epoch 2), expected FencedLeaderEpoch |
| no_truncate | leader_dies_with_unreplicated_writes | replica: unacknowledged divergent command div-conf-605-542 is below HW 22 |
| unclean_election | failover_elects_isr_member | election: ElectLeader elects node 2 outside the published ISR [1, 3] |
| unclean_election | whole_isr_dead_goes_offline | election: ElectLeader elects node 2 outside the published ISR [1] |
| ignore_min_insync | writes_stop_below_min_insync | min_insync: run acknowledged with ISR [1] below min_insync |
| shrink_before_commit | isr_slow_follower_shrinks | hw: leader HW 64 above slow follower 3's durable end 2 while its shrink is uncommitted |
| upload_above_hw | remote_chain_never_above_hw | objstore: chunk ... ends at 16 above HW 10 |
| rf1_bypass | one_path_rf1_hw_through_tracker | one_path: the tracker published the HW 0 time(s) |

The 12-14 weak bites are gone: `isr_from_assignment` now fails on the status oracle (was "no stable status sample"), `hw_leader_leo` / `no_truncate` on the disk / replica oracles (were "not rejoined within 22s"). No mutant site was moved.

## Tests

| Suite | Result |
| --- | --- |
| `ostro-server` full (`--no-fail-fast`, after the handle refactor) | 601 passed, 0 failed |
| `ostro-log`, `ostro-cluster`, `ostro-partition` full (`--no-fail-fast`) | 708 passed, 0 failed |
| `ostro-partition --features failpoints --test role_lifecycle` | 9 passed |
| `ostro-cluster` controller 9, isr_publisher 7, fetch_service 7 | green |
| `ostro-repl-conformance` full crate (`--no-fail-fast --test-threads=1`) | 52 passed, 0 failed (fault_layer 9 incl. the new regression) |
| clippy `-D warnings` ostro-log/cluster/partition/server/repl-conformance, all targets, with and without `failpoints`; `cargo fmt --all --check` | clean |

Not run: the full `scripts/ci-local.sh --no-musl` (2-3 h; other executors building). RF=1 runs the same code path (`placement_rf1_single_node`, `one_path_rf1_hw_through_tracker`, `cluster_boot` green); frozen storage v0.1 formats unchanged (no format, proto or ordinal change; the read barrier is an ordinary `Delete`).

## Deviations from Plan

### Auto-fixed issues

1. **[Rule 1] bugs 1-3, 5-8** above, each in its owning file with a regression test.
2. **[Rule 3] Harness fidelity (bug 4)** and **scenario bug (9)**: harness/scenario fixes, justified above.
3. **Files outside `files_modified`** (minimal hunks): `ostro-log/src/replica/tracker.rs` (bug 8), `ostro-server/src/{server,control/*,effects,p7,devtime,services/{dlq_hook,run_spans}}.rs` (bug 6b: every handle cache had to go; `server.rs` also carries the `wal_rotation_bytes` hook), `ostro-cluster/src/config.rs` (propose_timeout from session_timeout), test files. No proto, format or ordinal change.

### Not done / handed on

- **Bootstrap scenarios stay pending: needs a user decision.** `oracle::disk::acked_on_disk` requires every acked run's `StartRun` in >= `min_insync` nodes' verified WAL. Once retention trims (which these scenarios require), the early acked runs exist only in the state stores / snapshots, so the check cannot pass by design. Options: (a) let the oracle accept, for a node whose WAL starts above 0, the run's record in that node's state store (retention only deletes below a durable checkpoint and the oldest snapshot, so the state store is then the durable copy); (b) keep the oracle WAL-only and pass only post-trim acks to `final_checks_with` in the bootstrap scenarios (pre-trim runs are covered by `wait_completed` and the `standby` equal-state check); (c) leave them pending for 12-18. An attempt at (a) was blocked as an oracle change; nothing was changed. Recommended: (a).
- `catch_up_partition` holds a `StoreReader` for the duration of one catch-up pass (up to the 60 s rebuild fallback); transient, but an install landing during it waits for it.
- Full ci-local gate: 12-18.

