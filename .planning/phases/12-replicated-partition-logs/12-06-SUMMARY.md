---
phase: 12-replicated-partition-logs
plan: 06
subsystem: conformance-scenarios
tags: [conformance, clus-01, scenarios, red-run, isr, hw, divergence, failover, min-insync]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-02 Scenario registry + pending lists; 12-03 InProcCluster, FaultLayer, oracles, runner"
provides:
  - "CLUS-01 core scenario groups placement, isr_honesty, hw, stale_epoch, divergence, failover, min_insync (17 scenarios, 16 pending)"
  - "scenarios::placement::support: measured helpers shared by the core groups (published assignment from the meta log on disk, measured durable ends, stable HW-vs-durable sampling, keyed workloads, final oracle bundle)"
  - "pending/core.rs listing every RF>1 core scenario"
  - "seven thin group test files (tests/{placement,isr_honesty,hw,stale_epoch,divergence,failover,min_insync}.rs)"
affects: [12-07, 12-14, 12-16, 12-18]
tech-stack:
  added: []
  patterns:
    - "HW checks use stable samples only: a node's measured durable end is read before and after the leader's reported HW, and the sample counts only when both reads agree"
    - "Unreplicated tails are held by blackholing the followers' Fetch AND the leader's Propose to the meta leader, so the ISR cannot shrink and the failover stays clean"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/placement.rs
    - crates/ostro-repl-conformance/tests/isr_honesty.rs
    - crates/ostro-repl-conformance/tests/hw.rs
    - crates/ostro-repl-conformance/tests/stale_epoch.rs
    - crates/ostro-repl-conformance/tests/divergence.rs
    - crates/ostro-repl-conformance/tests/failover.rs
    - crates/ostro-repl-conformance/tests/min_insync.rs
  modified:
    - crates/ostro-repl-conformance/src/scenarios/{placement,isr_honesty,hw,stale_epoch,divergence,failover,min_insync}.rs
    - crates/ostro-repl-conformance/src/pending/core.rs
key-decisions:
  - "Shared helpers live in placement.rs as `pub(crate) mod support` (scenarios/mod.rs belongs to 12-02 and the plan allows no new file)"
  - "The leader HW is the one self-report the scenarios read (`partitions[].hw` of GET /v1/admin/replication, the 12-14 shape); it is only ever compared against measured durable ends. Published ISR/leader/epoch come from the authoritative node's committed meta log on disk"
  - "two_quick_leader_changes_truncate uses 5 nodes (RF 3): with 3 voters, two dead leaders in a row leave no meta quorum"
  - "resigned_leader_moves triggers the move with a graceful stop: ResignLeader is only proposed on LogHealth::Failed, which needs a failpoint, and scenarios may not use failpoints (Pitfall 9)"
requirements-completed: []
duration: "~2 h (scenarios) + red run 8 min + ci-local gate"
completed: 2026-11-23
---

# Phase 12 Plan 06: CLUS-01 core conformance groups Summary

**The CLUS-01 core groups are written before replication is integrated: placement, ISR honesty, HW, stale epoch, divergence, failover/offline and min_insync, 17 scenarios in all. Every scenario asserts only through measured oracles and opens with the `cluster_formed` precondition under a hard deadline. On the unintegrated build all 16 RF>1 scenarios are red with a `ScenarioFailure` (`cluster_formed`, never a harness error), and the RF=1 placement scenario passes.**

## Performance

- Tasks: 2 of 2
- Commits: 6b69f20 (Task 1), cf9e801 (Task 2), plus the docs commit
- Group tests (pending skipped): `placement` 39 s (runs the RF=1 scenario), the other six 0.00 s. Red run: 487 s for 17 scenarios.

## Scenario table

| Scenario | Group | Pending | Oracle(s) asserted (besides `cluster_formed` first and the final bundle) |
|----------|-------|---------|------------------------------------------------------------------------|
| placement_rf3_three_replicas | placement | yes | `placement` (every node's committed meta state: rf distinct replicas, epoch >= 1, leader = replicas[0], leaders within ±1) |
| placement_rf1_single_node | placement | **no** | `placement` (replicas [1], ISR [1], leader 1, epoch >= 1) |
| isr_empty_follower_not_in_isr | isr_honesty | yes | `status::honest` on stable samples throughout, `election` (node 3 leaves the published ISR) |
| isr_lagging_follower_rejoins | isr_honesty | yes | `isr` (re-added only with measured durable end >= the static HW; re-added within lag + 20 s) |
| isr_slow_follower_shrinks | isr_honesty | yes | `hw` (leader HW <= slow follower's measured durable end while the shrink is held uncommitted, under continuous appends), `election` (removed within lag + 2 s after heal) |
| hw_is_min_over_isr | hw | yes | `hw` (HW <= throttled ISR member's measured durable end; >= 3 stable samples) |
| hw_rf3_commits_within_rounds | hw | yes | `availability` (every sequential ack within 5 s, after a warm-up ack per partition) |
| fetch_with_stale_epoch_fenced | stale_epoch | yes | `fence` (raw Fetch over `channel_as(follower, new leader)` at the old epoch → FENCED_LEADER_EPOCH, no envelopes; current epoch → NONE) |
| fetch_with_future_epoch_unknown | stale_epoch | yes | `fence` (epoch + 5 → UNKNOWN_LEADER_EPOCH) |
| leader_dies_with_unreplicated_writes | divergence | yes | `disk` (K appends reach the leader's disk), `replica` (no divergent key below HW on any node after rejoin) |
| isolated_leader_rejoin_truncates | divergence | yes | `replica` (same, the leader is isolated instead of killed) |
| two_quick_leader_changes_truncate | divergence | yes | `replica` (two divergent tails from epochs e1 and e2 are truncated against e3; 5 nodes) |
| failover_elects_isr_member | failover | yes | `election` audit first (catches the unclean proposal even when apply rejects it), then `election` (the new leader is F3), `availability` (runs ack and complete within 10 s) |
| whole_isr_dead_goes_offline | failover | yes | `election` audit, `election` (leader 0, ISR kept [L], no proposal for another node for 4 sessions + 1 s, re-elected after restart), `availability` (acked runs complete) |
| resigned_leader_moves | failover | yes | `election` audit, `election` (new leader from the old ISR, the epoch never goes back after the old leader rejoins) |
| writes_stop_below_min_insync | min_insync | yes | `min_insync` (503 with ISR [L], zero acks for the pre-shrink appends, HW frozen), `hw` (HW <= the min_insync-th highest measured durable end) |
| writes_resume_when_follower_rejoins | min_insync | yes | `election` (the healed follower rejoins), `availability` (acks within 10 s), `disk` via the bundle |

**Final bundle** (`support::final_checks_with`, every scenario): wait for a meta leader; settle each partition (the live ISR members' measured durable ends equal the leader's and its HW, in two consecutive samples); `replica::identical_below_hw` over the live ISR; `disk::acked_on_disk` over every node with `min_insync`; `la_rule::check` on every node and partition; `election::isr_only_from(snapshot_state, entries)`.

**B4 shapes, for the 12-07 mutant matrix:**
- `failover_elects_isr_member`: replicas `[L, F2, F3]`, F2's Fetch is blackholed until the ISR is `[L, F3]`, and F2 stays live with meta traffic flowing. When L is killed, the first live replica in `replicas` order (F2) is outside the ISR, so `unclean_election` proposes it and the election audit, which runs before the leader check, names the proposal.
- `isr_slow_follower_shrinks`: the partition leader is not the meta leader. Its `Propose` to the meta leader is blackholed, so the shrink `AlterIsr` stays uncommitted, while the slow follower's Fetch is delayed 2 × `replica_lag_max`. Appends fire every 100 ms during the hold, and stable samples assert HW <= the slow follower's on-disk durable end. That is the sample where `shrink_before_commit` bites.

## CLUS-01 red run (before integration), core groups

Build: **cf9e801** (main after Task 2). Command: `OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run -- --ignored --nocapture --test-threads=1` → `test result: ok` (no red-run problems), 487 s.

| scenario | pending | result | oracle: detail |
|----------|---------|--------|----------------|
| placement_rf3_three_replicas | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; node 1 partition 0: 1 replica(s) [1], expected rf 3; … |
| placement_rf1_single_node | no | pass | |
| isr_empty_follower_not_in_isr | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| isr_lagging_follower_rejoins | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| isr_slow_follower_shrinks | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| hw_is_min_over_isr | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| hw_rf3_commits_within_rounds | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| fetch_with_stale_epoch_fenced | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| fetch_with_future_epoch_unknown | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| leader_dies_with_unreplicated_writes | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| isolated_leader_rejoin_truncates | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| two_quick_leader_changes_truncate | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3, 4, 5]; … |
| failover_elects_isr_member | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| whole_isr_dead_goes_offline | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| resigned_leader_moves | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| writes_stop_below_min_insync | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |
| writes_resume_when_follower_rejoins | yes | FAIL | cluster_formed: node 1: meta voters [1] != configured [1, 2, 3]; … |

Each elided detail goes on to list every node: `node N: meta voters [N] != configured [...]` and `node N partition P: 1 replica(s) [N], expected rf 3` for all four partitions. The run had no `HARNESS` rows. All 16 pending rows are red with a `ScenarioFailure`, and the uniform `cluster_formed` reason is the expected one on the unintegrated build (re-check W7): until 12-14 every node is its own single-voter, RF=1 cluster. The proof that each scenario fails on a broken build for an oracle-specific reason is the mutant matrix (12-07, made mandatory by 12-18).

## Deviations from Plan

**1. [Interpretation] The shared helpers live in `placement.rs`.** The core groups need the same measured reads: published assignment from the meta log on disk, durable ends, stable HW samples, keyed workloads and the final bundle. `scenarios/mod.rs` belongs to 12-02 and `files_modified` allows no new file, so the helpers are `pub(crate) mod support` inside the first core group. 12-07 can use them as `super::placement::support::*`.

**2. [Interpretation] `two_quick_leader_changes_truncate` uses 5 nodes.** The plan's default is 3 nodes. With 3 meta voters, killing two partition leaders in a row leaves one live voter, so no election can commit. Five voters with RF 3 keep a quorum while A (epoch e1) and X (epoch e2) are both down. Both then restart and truncate against Y (epoch e3).

**3. [Interpretation] `resigned_leader_moves` uses a graceful stop.** `ResignLeader` is proposed only when a led partition's log turns `LogHealth::Failed` (D-P12-9). Reaching that state needs a failpoint, and scenarios may not use failpoints (Pitfall 9). The scenario stops the leader gracefully, which in tier B means isolate + stop. It then asserts that leadership moves inside the old ISR at a higher epoch, that writes resume, and that the old leader rejoins without the epoch going back. A resign driven by a failure belongs in the 12-07 matrix or a tier-C test.

**4. [Interpretation] Holding an unreplicated tail.** In the divergence scenarios the followers' Fetch to the leader **and** the leader's `Propose` to the meta leader are both blackholed. Without the second blackhole, the leader's shrink to ISR [L] would commit after `replica_lag_max`, and killing it would make the partition go offline instead of failing over. So these scenarios pick a partition the meta leader does not lead.

No harness, oracle, runner, `scenarios/mod.rs`, server or other crate file was touched. No dependency was added, and FAILPOINT_CRATES ↔ ci.yml is unchanged.

## Open items for later plans

- **12-14:** the scenarios read the leader HW from `GET /v1/admin/replication` → `partitions[].hw` (the 12-14 plan's shape). `oracle::status::reported_isr` (12-03) reads `partitions[].isr`, but the 12-14 plan names that field `published_isr`. One of the two must change when the route lands, or `status::honest` reports "the status reports no ISR".
- **12-14/12-16:** the scenario bodies have never run against a replicated cluster. Expect to tune their timing bounds (`ACK_T` 15 s, `CHANGE_T` 20 s, `SETTLE_T` 30 s, `RESUME_T` 10 s, `COMMIT_T` 5 s, lag + 2 s for the held shrink) when they go green. Change a bound only to fit measured behaviour, never to hide a violation.
- `la_rule::check` uses the authoritative meta log's entries. If the meta log was compacted, epochs from before the snapshot have no committed leader in `entries`, and `la_rule` reports them. The short scenarios are not expected to compact. If one does, add a snapshot-aware leader map to `la_rule` (12-03 owns the oracle).
- The divergent commands are found in the WAL by substring match of their unique idempotency key in the command bytes. This works whether `StartRun.idempotency_key` stores the key verbatim or the input carries it.

## Plan gate

I ran `bash scripts/ci-local.sh --no-musl` in a detached worktree at **cf9e801** (the Task 2 commit, on main), on slot-3 with load 10-17 from parallel agents: **all steps passed**.

```
fmt / clippy -D warnings / build warning-free         PASS
sdk workspace, guests, order-saga                     PASS
cargo test                                            PASS 5336s  (3097 passed, 0 failed)
polyglot conformance / clippy polyglot / ostro-build  PASS
clients python / ts / go                              PASS
semantics sleepers / failpoint list sync              PASS
failpoint suites                                      PASS 5303s
server campaign smoke (20 kills)                      PASS
phase 7 legs / deny / vendored / no-chronik / one partition log path / workflow validation  PASS
external PG/MySQL/S3, musl                            SKIP
```

Inside the gate, every ostro-repl-conformance binary was green. The seven new group tests reported PENDING for the RF>1 scenarios and passed `placement_rf1_single_node`; skeleton, harness_smoke, fault_layer, oracle_selftest and runner_selftest were green too. Before the gate, `cargo clippy -p ostro-repl-conformance --tests --locked -- -D warnings` was clean on the rebased tree, and each task's automated verify passed (nonzero `test result: ok`, the `PENDING isr_empty_follower_not_in_isr` and `PENDING failover_elects_isr_member` lines present).

## Self-Check: PASSED

- Files: the seven scenario modules, pending/core.rs and the seven tests/*.rs files are present at main.
- Commits: 6b69f20 and cf9e801 are on main.
- Red run: 16/16 pending core scenarios FAIL with a `ScenarioFailure` (cluster_formed), the RF=1 scenario passes, and there are 0 HARNESS rows.
