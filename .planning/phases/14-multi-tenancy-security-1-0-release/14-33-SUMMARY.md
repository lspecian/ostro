---
phase: 14-multi-tenancy-security-1-0-release
plan: 33
subsystem: replication (ISR protocol) + controlled shutdown (CLUS-06)
tags: [p14, isr, alter-isr, controlled-shutdown, self-shrink, mutant, tier-b, tier-c, clus-06]
requires:
  - phase: 14-01b
    provides: "gate part 2 (no seam needed by this plan)"
  - phase: 12-04 / 12-11 / 12-16
    provides: "two-phase ISR protocol (IsrState, maximal ISR, next_change), ISR publisher with its fetched-again gate, MetaCommand 5 AlterIsr"
  - phase: 13-11
    provides: "controlled_shutdown (handoffs, quorum probe, meta transfer), ShutdownMeta, p13::before_shutdown"
provides:
  - "ReplicaTracker::on_isr_published / removed_at_version: no ISR re-add from progress measured before a removal"
  - "repl::mutant::readd_stale_progress (ostro-log mutants.rs; P13 matrix entry 9)"
  - "controlled-shutdown ISR self-shrink: ShutdownReport.self_shrink, SelfShrink, FollowedPartition, ShutdownMeta::{followed, alter_isr, stop_following}"
  - "ClusterServices::stop_following (LateFollower stop set) + the p13 ShutdownView adapter"
  - "InProcCluster::take_handle (tier-B graceful stop under traffic)"
  - "tier-B scenarios shutdown_followed_partitions_keep_committing, shutdown_self_shrink_respects_min_insync, restart_rejoins_isr_after_fresh_catchup; tier-C controlled_shutdown_followed_partition_stall"
affects: [14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "a removal's evidence reset lives where every publication is adopted (PartitionLog::set_published_isr), not in one proposer"
    - "stop fetching before proposing the self-shrink: no fetch of the leaving node can land after the leader adopted the removal"
key-files:
  created: []
  modified:
    - crates/ostro-log/src/replica/{tracker,isr}.rs
    - crates/ostro-log/src/mutants.rs
    - crates/ostro-log/src/partition_log.rs (deviation: the tracker hook)
    - crates/ostro-log/tests/replica_unit.rs
    - crates/ostro-cluster/src/shutdown.rs
    - crates/ostro-cluster/src/isr_publisher.rs (deviation: mutant also skips the fetched-again gate)
    - crates/ostro-cluster/tests/handoff_executor.rs
    - crates/ostro-server/src/p13/mod.rs
    - crates/ostro-server/src/cluster.rs (deviation: stop_following on the follower driver)
    - crates/ostro-repl-conformance/src/scenarios/handoff.rs
    - crates/ostro-repl-conformance/src/mutants_p13.rs
    - crates/ostro-repl-conformance/src/harness/inproc.rs (deviation: take_handle)
    - crates/ostro-repl-conformance/tests/{handoff,mutants_p13,process_shutdown}.rs
    - crates/ostro-repl-conformance/tests/p13_skeleton.rs (deviation: registry self-test accepts P14_MUTANTS)
key-decisions:
  - "Order inverted vs. the plan text: stop_following first, then the AlterIsr proposals. Under the new rule a fetch that lands after the leader adopted the removal IS a fresh caught-up observation; with propose-then-stop such a fetch races the stop and re-adds the leaving node"
  - "The re-add rule is a tracker reset applied to every adopted publication that drops a replica (lag shrink, controller shrink, self-shrink): last_caught_up_ms = 0, previous-response target cleared, removed_at_version set until the next caught-up fetch; the durable LEO is kept"
  - "readd_stale_progress also bypasses the 12-16 fetched-again gate in isr_publisher.rs (a second, racy stale-progress guard: the role driver often adopts the publication before the publisher records removed_at), so the mutant represents 'no stale-progress rule at all' and bites deterministically"
  - "The mutant is kept out of ostro_log::mutants::ALL (the Phase 12 matrix asserts MAP == ALL) and registered in the Phase 13 matrix after the eight compat::MUTANTS (P14_MUTANTS)"
  - "Tier-C stall measured with the shipped [replication] timings (session_timeout 3 s, replica_lag_max 10 s): with the 600 ms test timings the before/after difference would hide below the 1 s bound"
  - "A lost self-shrink CAS waits (bounded, VIEW_CATCH_UP 1 s) for this node's metadata view to move past the proposed epoch / ISR version before its single retry, so a lagging local view does not end in version_race with the fetchers already stopped"
  - "The STATE follow-up 'removed node that misses the final conf-change commit keeps running' is NOT handled here: it lives in the membership code (ostro-cluster membership / server membership hooks), shares nothing with the ISR protocol, and is harmless (pre-vote / check-quorum); left for its own fix"
requirements-completed: []
duration: ~3h
completed: 2026-11-27
---

# Phase 14 Plan 33: Controlled-shutdown ISR self-shrink Summary

**Phase 13 bug 4 is fixed. A controlled shutdown no longer stalls the partitions the leaving node only follows. After the handoffs the node stops its fetchers for those partitions and leaves their published ISR itself (`AlterIsr{published - self}`, a CAS through meta). The 12-04 ISR protocol gains one rule: a replica a publication removed is re-added only after a caught-up fetch observed after the removal. Tier C with the shipped timings, 27 followed-partition samples per run: the write stall went from p50 4.19 s / p99 4.29 s (self-shrink disabled, every stall hit the 4 s window bound) to p50 68 ms / p99 111 ms. The new mutant `repl::mutant::readd_stale_progress` bites in the P13 matrix. Phase 12/13 oracles and checkers are unchanged.**

CLUS-06 is refined, not re-ticked (D-P14-52; 14-27 owns the requirement bookkeeping).

## Commits

| Task | Commit | What |
|------|--------|------|
| 1 | 398130a | tracker reset + `should_expand` rule, mutant, self-shrink step, `stop_following` seam + production impl, unit tests |
| 2 | 981fc35 | tier-B scenarios, `InProcCluster::take_handle`, P13 matrix entry, tier-C stall test |
| fix | aece208 | self-shrink retry waits (bounded) for a lagging local metadata view after a lost CAS |
| fix | 3262944 | `tests/p13_skeleton.rs` registry self-test accepts `P14_MUTANTS` (found by the plan gate) |

(Rebased onto 5e9166f before the CAS; the pre-rebase ids were 7a67976 / 656c7e3.)

## The rule as landed (ostro-log)

- `ReplicaTracker::on_isr_published(prev, next, version)` is called from `PartitionLog::set_published_isr`. Every adopted publication goes through it, whoever proposed it: this leader's lag shrink, the controller's liveness shrink, a stopping node's self-shrink. For every replica in `prev` and not in `next`:
  - `last_caught_up_ms` is set to 0 and the previous-response target (`prev_response_end`) is cleared;
  - `removed_at_version = Some(version)` is set.
  
  Replicas in `next` drop a stale mark. The durable LEO is kept: it describes the replica's disk, and the HW no longer counts the replica anyway.
- `on_fetch` clears the mark on the first caught-up fetch. Because the previous-response target was cleared, that first fetch after the removal must reach the leader's current durable end.
- `isr::should_expand` returns `false` while `removed_at_version(replica)` is set. `maximal`, the HW code and the `AlterIsr` apply validation are untouched.
- A lag-based shrink and its re-add behave as before: the replica was already past `replica_lag_max`, and a caught-up fetch re-adds it (unit test).
- Mutant `READD_STALE_PROGRESS = "repl::mutant::readd_stale_progress"` skips the reset. It also skips the 12-16 fetched-again gate in `isr_publisher.rs`, so it means "no stale-progress rule at all". It is not in `mutants::ALL` (the Phase 12 matrix asserts `MAP == ALL`).

## The shutdown step (ostro-cluster `shutdown.rs`, `p13/mod.rs`, `cluster.rs`)

The order is: members ≤ 1 → skip; quorum probe; handoffs; **self-shrink**; meta leadership transfer. Everything shares the one `shutdown_handoff_timeout` budget.

1. `ShutdownMeta::followed(node)` lists the partitions where meta names the node a replica and published-ISR member but not the leader. This includes partitions handed off a moment earlier, if the local view already shows that.
2. `FollowedPartition::shrunk_isr(me)` gives `published - me`, but only if it still holds the leader and has at least `min_insync` members. Otherwise the line is `kept_min_insync` (Phase 13 behaviour).
3. `ShutdownMeta::stop_following(&eligible)` runs first. The production impl is `ShutdownView` in `p13/mod.rs`, which calls `ClusterServices::stop_following`, which uses the `LateFollower` stop set: running fetchers of those partitions stop, and later starts (a role-driver restart on a leader change) idle until exit. It resolves once no fetcher of those partitions is running.
4. Then `AlterIsr{partition, epoch, expected_isr_version, isr: published - me}`, `SELF_SHRINK_PARALLELISM = 4` at a time. On `StaleVersion` it waits up to `VIEW_CATCH_UP` (1 s, polled every 20 ms) for the local view to move past the proposed epoch / ISR version, then retries once.
5. `ShutdownReport.self_shrink: Vec<(u16, SelfShrink)>`, values `shrunk | kept_min_insync | no_quorum | version_race | timeout | rejected` (`rejected` also covers an unknown outcome). The p13 adapter logs it with the existing `controlled shutdown` line. No quorum: every followed partition is `no_quorum` and nothing is attempted. Budget exhausted: `timeout`, and no meta transfer.

`ShutdownMeta`'s new methods have defaults (no followed partitions, refused `alter_isr`, no-op `stop_following`), so `controlled_shutdown(MetaHandle)` and the test fakes keep the Phase 13 behaviour.

**Why stop first, then propose:** under the new rule, a fetch that reaches the leader after it adopted the removal is a legitimate fresh caught-up observation. With propose-then-stop, the node's own fetcher (long-polling every 100 ms on a busy partition) can race the stop and get the node re-added just before it exits, which brings back the stall. The cost of stopping first: if the proposal then fails (`version_race` / `rejected` / `timeout`), the partition stalls from the stop instead of from the exit, until the controller shrinks the gone node. That is never longer than before by more than the rest of the shutdown.

## Evidence

### Unit (local, slot-1 target)

- `ostro-log --test replica_unit`: 50 passed. New: removed replica not re-added from stale progress; fresh caught-up fetch re-adds; the first fetch after a removal must reach the current end; lag shrink and its re-add unchanged; re-added replica drops its mark; a pending `Add` of a replica a version removed is dropped by its failed CAS and recomputed. With `--features failpoints`: `readd_stale_progress_mutant_bites`. Full `cargo test -p ostro-log`: green.
- `ostro-cluster --test handoff_executor`: 25 passed (10 new self-shrink tests):
  - proposals only for followed partitions, never for led ones;
  - `stop_following` first, with exactly the eligible partitions, then the proposals, then the meta transfer;
  - the `min_insync` / leader guard;
  - one re-read on `StaleVersion`;
  - a lagging view is waited for, and that wait is bounded;
  - rejected and unknown outcomes reported;
  - bounded by the budget;
  - no quorum skips everything;
  - the Phase 13 fake (defaults) yields no lines.
- clippy `-D warnings`: ostro-log + ostro-cluster with failpoints; ostro-server, ostro-cluster and ostro-repl-conformance with default features; ostro-server and ostro-repl-conformance with failpoints. `scripts/p12-grep-gates.sh` and `scripts/p13-grep-gates.sh` green.

### Tier B (cluster slot 1, dell-2; group `--test handoff`, failpoints build)

All 7 scenarios of the group pass: 92.8 s at f4a1c50 and again 58.0 s at the final aece208 (run D). Lines from the new scenarios (f4a1c50):

```
shutdown_followed_partitions_keep_committing: node 3 (led Some(1)) stopped in 508.885974ms; left the ISR of [(0, 394.961625ms), (1, 382.464275ms), (3, 394.961625ms)] (quiet: 3); busy gaps (partition, longest, acks) [(0, 36.650832ms, 345), (1, 41.82173ms, 342)]
shutdown_self_shrink_respects_min_insync: node 2 stayed in the ISR of [0, 3] until it exited (360.992089ms)
restart_rejoins_isr_after_fresh_catchup: node 1 (led Some(2), meta leader: false) stopped in 294.413975ms, down 4.011162902s; restart + rejoin of every ISR 1.702856021s
```

The "never re-added until restart" check covers the whole committed meta log from just before the stop to the end of the hold (read while the node is still down). "Left the ISR before it exited" uses 10 ms samples of a surviving node's metadata view.

### Mutant (P13 matrix, `OSTRO_P13_MUTANT=readd_stale_progress`)

```
MUTANT repl::mutant::readd_stale_progress: shutdown_followed_partitions_keep_committing baseline passed in 11.54426566s
MUTANT repl::mutant::readd_stale_progress: shutdown_followed_partitions_keep_committing FAILED as expected in 5.232564469s: self_shrink: partition 3: node 2 left the published ISR at index 41 but was re-added at index 42 (ISR [1, 3, 2]) while shutting down or down, before its restart; history [(41, [1, 3]), (42, [1, 3, 2]), (43, [1, 3]), (45, [1, 3, 2]), (46, [1, 3]), (47, [1, 3, 2]), (48, [1, 3]) ...
```

The quiet followed partition's leader re-adds the stopped node from its pre-removal progress and then ping-pongs with the controller's liveness shrink: exactly the 12-16 pattern the rule prevents. It also bit in both local runs.

### Tier C stall table (cluster slot 1, dell-2; `controlled_shutdown_followed_partition_stall`)

Setup: 3 static processes, RF 3 / `min_insync` 2, shipped `[replication]` timings (`session_timeout` 3 s, `replica_lag_max` 10 s), the continuous mixed workload plus one probe per partition every 50 ms. 20 SIGTERMs round-robin, each starting from a healthy cluster. Window: SIGTERM until the end of a `session_timeout + 1 s` hold while the node is down. Stall = the longest gap between probe acks on each partition the node only followed at the SIGTERM.

| run | commit | followed-partition samples | p50 | p99 | max | self-shrink lines | runs acked / completed |
|-----|--------|----------------------------|-----|-----|-----|-------------------|------------------------|
| before (self-shrink step disabled; throwaway commit fe6ecf0, never merged) | fe6ecf0 | 27 | 4.188 s | 4.295 s | 4.295 s | none | 5,167 / 5,167 |
| after | 656c7e3 (= 981fc35 before the rebase) | 27 | 68.4 ms | 111.0 ms | 111.0 ms | every one `shrunk` | 18,440 / 18,440 |
| after, 30 SIGTERMs (final code) | aece208 | 41 | 67.2 ms | 122.7 ms | 122.7 ms | 120 of 120 partition lines `shrunk` (each stop also shrank the partitions it had just handed off) | 35,112 / 35,112 |

- "Before": 25 of 27 stalls lasted the whole window (≥ 4.16 s, so the real stall is longer than measured). Two samples were about 57 ms; their node was still in the published ISR after the hold, so they look like a stale `published()` read in that run. The before-run is evidence only and was never merged: it is the tree of 656c7e3 with `self_shrink_all` returning at once.
- An earlier "after" run on f4a1c50 (same code, but the test did not wait for a healthy cluster before each SIGTERM and did not record logs) had 26 of 27 samples at 65 to 71 ms and one 4.19 s outlier in repeat 1 that the test could not explain. Two changes followed: (1) the test now waits for health before every SIGTERM and records each partition's ISR at the SIGTERM plus the stopped node's self-shrink log lines; (2) aece208 hardened the most plausible self-shrink failure (lost CAS against a lagging local view: `version_race` with the fetchers already stopped).

## Mutant registration

- `ostro_log::mutants::READD_STALE_PROGRESS` (site: `replica/tracker.rs` `on_isr_published`; also `isr_publisher.rs`'s fetched-again gate).
- `ostro-repl-conformance/src/mutants_p13.rs`: `P14_MUTANTS = [READD_STALE_PROGRESS]`, MAP entry 9 → `shutdown_followed_partitions_keep_committing`. The self-test (`map_covers_exactly_the_eight_mutants_in_order`, name unchanged) now asserts `MUTANTS ++ P14_MUTANTS` and 9 entries, and `mutant_names_match_their_sites` checks `MAP[8]`.

## ci-local / ci.yml hunks for 14-27 (not applied here)

The new tier-C test runs automatically in the existing "Phase 13 tier C (multi-process)" step: `--test process_shutdown -- --ignored` runs every ignored test of the file, about 3.5 min more with 20 repeats. Only the P13 mutant count changes:

```diff
--- a/scripts/ci-local.sh
-# Phase 13 mutant matrix (13-18, D-P13-25): every one of the eight mutants must bite
-# (>= 8 "FAILED as expected" lines) and none may be skipped.
+# Phase 13 mutant matrix (13-18, D-P13-25; + 14-33 readd_stale_progress): every one of the
+# nine mutants must bite (>= 9 "FAILED as expected" lines) and none may be skipped.
@@ check_p13_mutants
-  if [[ "$bites" -lt 8 ]]; then echo "only ${bites} P13 mutant bites (need >= 8)"; return 1; fi
+  if [[ "$bites" -lt 9 ]]; then echo "only ${bites} P13 mutant bites (need >= 9)"; return 1; fi
--- a/.github/workflows/ci.yml
       - if: matrix.name == 'linux-x86_64-gnu'
         name: Phase 13 mutant matrix
-        # keep in sync with check_p13_mutants in scripts/ci-local.sh (13-18): all eight
-        # Phase 13 mutants must bite and none may be skipped
+        # keep in sync with check_p13_mutants in scripts/ci-local.sh (13-18, 14-33): all nine
+        # Phase 13 mutants must bite and none may be skipped
@@
-          test "$(grep -c 'FAILED as expected' <<<"$out")" -ge 8
+          test "$(grep -c 'FAILED as expected' <<<"$out")" -ge 9
```

Optional, for 14-27: the "Phase 13 tier C" step comment can mention the followed-partition stall test (`OSTRO_SHUTDOWN_STALL_REPEATS`, default 20).

## Deviations from Plan

### Auto-fixed / necessary

1. **[Rule 1 - Bug] Stop following before proposing (order inverted).** Found while designing Task 1. Under the new rule, a fetch landing after the leader adopted the removal counts as fresh, so propose-then-stop races the node's own fetcher and can re-add it. The must-have still holds: after the self-shrink the node is not following and stays out of the ISR until exit. The `handoff_executor` tests assert the order used here. Commit 398130a.
2. **[Rule 3 - Blocking] `crates/ostro-log/src/partition_log.rs` (6 lines).** Only the partition log holds both the tracker and the adopted publication, so the reset hook must live there (every adoption path calls `set_published_isr`). 398130a.
3. **[Rule 3 - Blocking] `crates/ostro-server/src/cluster.rs` (`ClusterServices::stop_following`, the `LateFollower` stop set).** The fetchers are only reachable through the follower driver that `cluster.rs` owns. `p13/mod.rs` cannot stop them otherwise without touching the role driver (ostro-partition) or `fetcher.rs`. 398130a.
4. **[Rule 2 - Correctness of the evidence] `crates/ostro-cluster/src/isr_publisher.rs` (one mutant condition).** The publisher's 12-16 fetched-again gate is a second, racy stale-progress guard. Without bypassing it under the mutant, the mutant would bite only when the role driver happens to adopt the publication first. Behaviour without failpoints is unchanged. 398130a.
5. **[Rule 3 - Blocking] `crates/ostro-repl-conformance/src/harness/inproc.rs` (`take_handle`, harness only, no oracle).** The tier-B check "stays out of the ISR until exit", plus traffic during the stop, needs the graceful stop to run while the scenario still reads the cluster. `upgrade()` holds `&mut` for the whole stop and restart. 981fc35.
6. **[Rule 1 - Robustness] Lost-CAS retry waits for a lagging local view.** Found during tier-C evidence. Commit aece208.
7. The P13 matrix self-test in `src/mutants_p13.rs` (not an oracle) now expects `MUTANTS ++ P14_MUTANTS` (9 entries).
8. **[Rule 1 - Bug caused by this plan] `crates/ostro-repl-conformance/tests/p13_skeleton.rs` (registry self-test, not an oracle).** `p13_mutant_matrix_names_registered_scenarios` required every matrix mutant to be in `compat::MUTANTS`; it now also accepts `mutants_p13::P14_MUTANTS`. The first gate run found it ("failpoint suites", fp2 group). Main carried the failing self-test between aece208 and 3262944, a few minutes. Commit 3262944.

No oracle or crash checker was changed. TDD note: the unit tests and the implementation were written in the same pass, not as a separate failing commit. The behaviour each test pins was checked against the pre-change code by reasoning (for example, `self_shrink_waits_for_a_lagging_view_after_a_lost_cas` proposes the stale version twice without aece208).

### Not done / left

- **Removed node that misses the final conf-change commit (STATE follow-up from p13-memb):** not handled. It is membership code (ostro-cluster membership / the server's Removed handler), unrelated to the ISR protocol, and harmless (pre-vote / check-quorum). It needs its own small plan or fix.
- Mixed-version clusters: an old-version leader may still re-add a stopping node from stale progress. That is no worse than before (D-P14-50).

## Gate

The plan-level gate was cut short by cluster saturation and then the orchestrator's pause on cluster use. Results:

- **Gate 1, `--parallel` at 981fc35** (before aece208 and 3262944), 70 min. Every dell-2 group ran:
  - `cargo test` (test1) PASS 689 s;
  - server campaign smoke PASS 1147 s;
  - replication mutant matrix PASS 272 s;
  - musl release / size limit / zigbuild test PASS;
  - "failpoint suites" FAIL, only on `p13_skeleton::p13_mutant_matrix_names_registered_scenarios` (caused by this plan, fixed in 3262944).
  
  Every dell-1 group (lint, poly, obuild, test2, musl2) never started: API unreachable, container never started, pods Pending on CPU. That is infrastructure. I stopped the runner's two still-Pending group runners (test2, obuild, lane 1); their runners reported "the Job is deleted".
- **Gate 2, `--parallel` at 3262944** (= main after this plan's code). At the time of the pause:
  - fp1 PASS (failpoint suites part 533 s, campaign smoke 1095 s);
  - musl1 PASS;
  - test1 PASS;
  - fp2: "failpoint suites" FAIL on `routing::internal_consumers_route` ("trigger through node 1: 404 schedule rt-cron-2 not found"). It passed in gate 1's fp2 at 981fc35, and the scenario stops no node and does not involve the ISR. An alone-rerun x3 was queued on slot 1 but had not run at the pause;
  - musl2 infrastructure (container never started);
  - lint, poly, obuild, test2 and fp3 still running at the pause (results not collected).
- **Lint-group steps as a slot-1 job at 3262944** (dell-1 was saturated):
  - `cargo fmt --check` FAIL, only on 14-29's files (ostro-backup `src/cluster_cut.rs`, `src/uploader.rs`, `tests/cluster_cut.rs`, `tests/meta_head.rs`, ostro-server `tests/cluster_meta_head.rs`); every 14-33 file is rustfmt-clean;
  - clippy `-D warnings` and the warning-free build clean;
  - **Phase 13 mutant matrix with `OSTRO_P13_REQUIRE_ALL=1`: 9 P13 mutant bites, none skipped** (incl. `readd_stale_progress`);
  - Phase 13 tier C was still running at the pause.
- Targeted at the final code: tier B `--test handoff` 7/7, mutant bite, tier C 30 SIGTERMs (see Evidence).


## Self-Check: PASSED

Commits 398130a, 981fc35, aece208 and 3262944 are ancestors of main. `shutdown.rs` contains `expected_isr_version`, `p13/mod.rs` contains `stop_following`, `tracker.rs` contains `last_caught_up_ms`, and `process_shutdown.rs` contains `controlled_shutdown_followed_partition_stall`.

## Post-handback finding (2026-11-27, slot-1 lint-steps job at 3262944): RESOLVED (75f3406)

The ci-local "Phase 13 tier C (multi-process)" step **FAILS at 3262944** on `process_shutdown::controlled_shutdown_followed_partition_stall`. That step runs every ignored test of `process_shutdown`, so main's ci-local now has this failing step.

- p99 was 4.31 s from a single outlier: repeat 1 (the first SIGTERM after cluster start), node 1, partition 1 (leader 2, ISR [2, 1, 3]).
- The other 26 samples were 66 to 110 ms.
- `controlled_shutdown_timing` in the same file passed.
- The other Phase 13 tier C tests (promotion, rolling upgrade x3, RYW, downgrade) passed. `upgrade_from_prev` and `cluster_campaign_p13` did not run after the failure.

The stopped node's log for that repeat shows it stopped following partition 1 and the self-shrink of partition 1 committed (`result="shrunk"`), yet partition 1 had no probe ack for the whole 4 s hold. So the stall is **not** a failed self-shrink, and aece208 (the lagging-view retry) does not address it.

The same "repeat 1, node 1, partition 1" outlier appeared in the first run (f4a1c50). In the passing runs, repeat 1 / partition 1 was still the worst sample (111 to 123 ms). Something specific to the first leader term after bootstrap, on the remaining ISR or on the leader's HW, still holds the partition after the removal. Root cause unknown: it needs the leader's (node 2's) log for that repeat.

The plan's p99 < 1 s claim above holds for the runs listed there but **not reliably**. This needs a fixer (14-33 follow-up) before the 14-27 gate. Options:
- investigate the first-term HW/ISR behaviour (preferred);
- or, if it is a test artefact of the fresh cluster, warm up with one throwaway SIGTERM cycle and record it. That is a change to a 14-33 measurement, not to a Phase 12/13 oracle.

Also measured in the same job: P13 mutant matrix REQUIRE_ALL 9/9; clippy, build, deny, grep gates, phase 7 legs and wit-next-fixture PASS; `cargo fmt --check` FAIL only on 14-29's files.

### Resolution (2026-11-27, 14-33 follow-up fixer): 75f3406

The root cause is a **lost metadata lease round**: two nodes' `ReadIndex` request contexts collided in the vendored meta raft driver. It is not first-term HW/ISR state and not a measurement artefact.

- The driver built a `ReadIndex` context from a kind byte plus a per-driver sequence only. raft-rs keys a pending read by its context on the leader and silently ignores a second request whose context is already pending. When two nodes issued the same sequence at the same moment, one read was lost until `read_timeout`, which is two election timeouts (2 s with the shipped meta timings).
- On a fresh cluster, the followers' first lease rounds all carry sequence 1, and their counters advance in step after that. The losing follower then had no metadata lease for 2 s.
- During those 2 s, every partition write it led waited the 500 ms anchor wait and was refused (`not_leader`, "no metadata lease"). The router then held each refused write for up to 2 x `session_timeout` (6 s), waiting for a view change that never came.
- The client gave up at 5 s, so the partition had no probe ack from the SIGTERM to the end of the hold. Measured that way, the stall is `exit + hold`, which matches 4.31 s = 0.29 s + 4 s.
- Repeat 1 is not special to the self-shrink. The shrink committed and the leader adopted it in a few ms. The write stall comes from the lease, on whichever node lost the collision. A meta leadership transfer can trigger the same collision later in a run (seen locally at repeat 2: node 3's lease lapsed after the transfer from node 2).

Evidence (local, slot-6, failpoint debug build, shipped timings):
- **Reproduced at 3262944 + b3657fa:** 5 failures in 29 short runs, at repeat 1 or 2, with stalls of 4.18 to 4.78 s.
- **Probe logging:** the stalled partition's single probe request timed out client-side after 5.00 s, and the retry with the same key answered `created: true`, so the first attempt was never written. The leader node logged "no metadata lease" refusals after a 500 ms wait each.
- **Driver logging on the losing follower:** `issue round seq=1 term=2 leader=3`, never resolved, then `round timed out` 2.0 s later; `seq=2` resolved in 3 ms. The other follower's `seq=1`, sent 0.5 ms apart, resolved in 9 ms.
- **Before the fix:** 4 of 8 instrumented runs had a lost round with 5 to 7 refusals.
- **After the fix:**
  - 0 of 14 runs (42 SIGTERMs, including meta transfers) had a refusal; the longest anchor wait was 105 ms.
  - All 14 runs passed, with a worst stall of 158 ms.
  - The full ci-local `--test process_shutdown` (both ignored tests, 20 repeats) passed: followed-partition stall p50 61 ms / p99 166 ms over 27 samples, and `controlled_shutdown_timing` passed.

The fix (VENDORED.md P6) adds this node's id to every context: kind byte, node id, sequence, 17 bytes in all. Contexts are opaque to raft-rs and echoed only to the requester, so mixed-version clusters are unaffected.

The regression test is `ostro-raft-driver` `concurrent_follower_reads_do_not_collide`. Two followers read at the same moment under a 50 ms link delay, 5 rounds. Before the fix it fails deterministically (`Err(Dropped)` after 1.0 s); after it, it passes 10 of 10 runs. `cargo test -p ostro-raft-driver` (43) and `-p ostro-meta` pass, clippy on the crate is clean, and check-vendored passes.

No test bound or oracle was changed, and there is no warm-up cycle.

Not changed, for the orchestrator to decide: the router turns a lease refusal on the partition's own leader into a re-route wait for a view change of up to 2 x `session_timeout`. That amplified a 2 s lease gap into a > 5 s client stall. A genuine meta-contact loss still behaves this way (p13-fixes / 13-12 design). One option is to end the wait once the local lease holds again.


Final `--parallel` at 3262944, finished after the pause:

| group | node | result |
|---|---|---|
| test1 | dell-2 | 0 (PASS) |
| fp1 | dell-2 | 0 (PASS) |
| fp3 | dell-2 | 0 (PASS) |
| musl1 | dell-2 | 0 (PASS; size limit PASS) |
| replication mutant matrix | dell-2 | PASS |
| server campaign smoke | dell-2 | PASS |
| test2 | dell-1 | FAIL, only on the known load-only `backup_cli::bucket_online_and_offline_round_trips` |
| obuild | dell-1 | FAIL, `ostro-build toolchain_cache` timeouts under the dell-1 saturation; unrelated to 14-33 |
| fp2 | dell-2 | FAIL, `routing::internal_consumers_route` (above); never rerun alone |
| lint, poly | dell-1 | exit 137 (killed, infrastructure) |
| musl2 | — | container never started |
