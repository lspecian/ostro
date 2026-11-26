---
phase: 13-cluster-membership-routing-upgrades
verified: 2026-11-26T10:43:16Z
status: human_needed
score: 4/4 success criteria verified (5/5 requirements satisfied); 3 by-design manual items open
re_verification: false
evidence_commit: 7942813 (main 3794b5f has no non-.planning changes since 7942813)
human_verification:
  - test: "Phase 13 >= 10,000-fault campaign (cluster_campaign_p13, OSTRO_CAMPAIGN_FAULTS=10000) at a commit that includes d33fce7, with child runs on and a disk-backed OSTRO_PROC_ROOT (or a larger /dev/shm)"
    expected: "Campaign reaches the fault target; membership, upgrade, RYW, GC and outcome oracles pass; 0 lost runs"
    why_human: "Manual gate run by design (D-13-20-3: per-push CI runs 50 faults). The 2026-11-26 run (b47721e, child runs off) stopped at 1,110 faults when the pod's /dev/shm filled (fjall ENOSPC). This was a harness storage limit, not an oracle failure (target/k8s-ci-logs/20261126-065118-s0-b47721e0-cmd.log)"
  - test: "Real previous-binary rolling upgrade: run upgrade_from_prev with OSTRO_PREV_BIN pointing at the first release-tagged binary"
    expected: "Rolling upgrade from N-1 to N with runs in flight, 0 lost, finalization after the last node"
    why_human: "No release tag exists yet. The ROADMAP marks this human_needed. Older levels are emulated on every commit (OSTRO_TEST_LEVEL_MAX), and tier C process_rolling_upgrade passes x3"
  - test: "Re-measure the 13-20 timings (handoff unavailability, controlled shutdown, restart->ready, first activation after restart, RYW latencies, time-to-RF-3) on quiet, dedicated nodes"
    expected: "Numbers at or below the 13-20 table, which was measured in a 4-CPU CI pod on a shared, loaded node"
    why_human: "Native timings are human_needed in the ROADMAP. Only shared CI nodes were available"
---

# Phase 13: Cluster Membership, Routing & Upgrades Verification Report

**Phase Goal:** Operators can form, grow and upgrade a 3- or 5-node cluster, or promote an existing single node, without draining runs. Any node serves any request.
**Verified:** 2026-11-26T10:43:16Z
**Status:** human_needed (all automated must-haves verified; only by-design manual items remain)
**Re-verification:** No (initial verification)

Method: code read from `main` (git show / git grep), not from the stale working tree. Evidence is the existing CI logs. No suites were re-run. Between the evidence commit 7942813 and main 3794b5f only `.planning/` changed (`git diff --stat 7942813 main -- . ':!.planning'` is empty), so the evidence applies to current main.

## Goal Achievement

### Observable Truths (ROADMAP success criteria)

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `ostro cluster init` promotes a running single node to a cluster without an export. `ostro cluster join/remove` changes membership of the multi-voter meta Raft (3/5 nodes) safely | VERIFIED | `crates/ostro-cli/src/commands/cluster.rs` (ClusterCmd Init/Join/Remove/Drain/Token/Upgrade, snapshot tests `p13_cli__cluster_*`). `crates/ostro-server/src/p13/{join,joiner,membership,cluster_api}.rs`. Reconciler with joint changes 1->3->5 / 5->3->1 (`ostro-cluster/tests/reconcile_model.rs`: `six_members_stay_at_five_voters`, `removing_two_of_five_is_one_joint_change`; `membership_driver.rs`: `removal_of_a_node_without_replicas_shrinks_five_to_three`). Tier C `process_promotion` at 7942813: init, 2 joins, RF 3 in 2.99 s, kill -9 node 1, 362 runs completed, 0 lost (lint log l.551-567). Mutants `skip_quorum_check` and `no_leave_joint` bite (lint log l.459-461). Required failpoint suites p13_boot_modes and p13_promote PASS |
| 2 | A request sent to any node is forwarded to the owning partition leader. A client always reads its own prior writes | VERIFIED | `RoutedControlPlane` in `crates/ostro-server/src/route/mod.rs` wraps every API trait (single construction site in server.rs per 13-02). Fan-out in `route/fanout.rs`. Lease reads, meta ReadIndex barrier, consistency tokens and `strict_reads` (13-13). Tier C `process_ryw`: 9000 ops, 0 RYW violations through moves, failover and a zombie (lint log l.647-654). Mutant `route::mutant::serve_without_lease` bites (l.466) |
| 3 | A rolling upgrade across one minor version completes with runs in flight and none drained. The host links WIT 0.1.x and 0.2.x side by side. Workers receive only activations compatible with their advertised WIT/engine versions | VERIFIED (emulated levels; real N-1 is human) | Gate, finalizer, hold and fences in `ostro-cluster/src/{compat,upgrade}.rs`. Partition fail-stop on newer commands in `ostro-partition/src/apply/mod.rs` (db09626). Tier C `process_rolling_upgrade` with OSTRO_UPGRADE_ITERS=3 and child runs on: 7907 acknowledged runs, 0 lost, 0 duplicate journal entries, finalized 2 on every node ~550 ms after the last restart (l.577-637). `process_downgrade` fences PASS (l.666-677). `LinkerSet` keyed by WIT line in `ostro-engine/src/host/linker_set.rs` (`WitLine::V0_1`/`V0_2`). The 0.2 line is a test-only fixture behind `wit-next-fixture` by design (WIT 0.2 does not exist yet; `ostro:runtime@0.1.0` is frozen). The `engine wit-next-fixture suite` CI step PASS. Worker placement predicate `workers/placement.rs::compatible` (used at l.141, including `session_current`). Mutants `emit_ungated` and `reject_and_continue` bite (l.455-457) |
| 4 | Module GC never deletes a module referenced by a non-terminal run. `ostro migrate-runs` re-pins runs only after a passing dry-run replay | VERIFIED | Two-scan, reference-checked GC (13-15; required failpoint suite p13_module_gc). Mutant `gc::mutant::single_scan` bites (l.468). `control/migrate.rs::migrate_runs` submits `RepinRun` only for runs whose replay `check()` passed (`passed` vector). Dry-run never submits. Seq check, mutant `migrate::mutant::skip_seq_check` bites (l.470). Tier B `diverging_runs_never_repinned`, `start_during_gc_race` -> `module_gone` |

**Score:** 4/4 truths verified

### Required Artifacts (spot-checked at Levels 1-3)

| Artifact | Status | Details |
|---|---|---|
| `crates/ostro-cli/src/commands/cluster.rs`, `migrate_runs.rs`, `modules_lc.rs` | VERIFIED | CLI verbs exist, snapshot-tested, and call ostro-client |
| `crates/ostro-server/src/p13/*` (join, joiner, membership, keyed_start, inbound, module_lifecycle, cluster_api) | VERIFIED | Wired from the server seam (13-02). Exercised by tier B/C |
| `crates/ostro-server/src/route/{mod,fanout}.rs` | VERIFIED | RoutedControlPlane over every trait. Exercised by tier C RYW |
| `crates/ostro-cluster/src/{compat,upgrade}.rs` | VERIFIED | require/fences/finalizer. Mutant emit_ungated bites |
| `crates/ostro-partition/src/apply/mod.rs` (NewerFormat fail-stop) | VERIFIED | db09626. `apply_newer_format` 5/5 and `level_emulation_apply` 2/2 green in CI |
| `crates/ostro-engine/src/host/linker_set.rs` | VERIFIED | Per-line linkers. Fixture line feature-gated |
| `crates/ostro-server/src/workers/placement.rs` | VERIFIED | `compatible()` used by the placement predicate |
| `crates/ostro-server/src/control/migrate.rs` | VERIFIED | Replay check before RepinRun |
| `crates/ostro-repl-conformance/{src/oracle/p13_*,src/scenarios/*p13*,tests/process_*,tests/cluster_campaign_p13.rs,tests/mutants_p13.rs}` | VERIFIED | Required in ci-local and ci.yml (13-20) |
| `scripts/p13-grep-gates.sh`, docs (`cluster-operations.md`, `compat.md`, `upgrade-runbook.md`) | VERIFIED | Gate step PASS |

### Key Link Verification

| From | To | Via | Status |
|---|---|---|---|
| CLI `cluster init/join/remove` | server Cluster/Membership API | ostro-client gRPC/REST | WIRED (p13_cli snapshots, p13_promote, tier C promotion) |
| Any node API | owning partition leader | RoutedControlPlane forward, one hop, re-route on not_leader | WIRED (tier B routing parity, tier C RYW) |
| Gated emitters | `compat::require` | finalized level check -> 409 feature_not_enabled | WIRED (emit_ungated mutant bites; version_gate scenarios) |
| Partition apply | `StoreError::NewerFormat` | re-encode identity / unknown tag / level emulation | WIRED (reject_and_continue bites; level_emulation_apply green) |
| Worker dispatch | `placement::compatible` | advertised WIT lines + session level | WIRED (worker_compat, version_gate) |
| migrate-runs | RepinRun | only after `check()` replay passes | WIRED (skip_seq_check bites; diverging_runs_never_repinned) |
| Module GC | run references | two scans with a partition/epoch coverage check | WIRED (single_scan bites; p13_module_gc) |

### CI evidence (at 7942813)

- `target/k8s-ci-logs/20261126-091656-par-7942813b-summary.txt`: every step PASS except `cargo test` (test2). The only failure there is `ostro-partition cron_driver::catch_up_skip_advances_the_watermark_without_runs` ("timed out after 10s", test2 pod at 27 GiB peak). The re-run `20261126-104011-s0-7942813b-cmd.log` passed (whole test2 shard, 373 test binaries ok, RC_TEST2=0). This is a known load-only flake. It is not a Phase 13 defect.
- Phase 13 mutant matrix 8/8 "FAILED as expected", 0 SKIP. The engine wit-next-fixture suite, tier C 7/7 (`upgrade_from_prev` is a SKIP line by design), P13 grep gates and failpoint suites (including p13_boot_modes, p13_module_gc and p13_promote) all PASS. The musl size gate (57.7 MB) PASS.

### Requirements Coverage

| Requirement | Source Plans | Status | Evidence |
|---|---|---|---|
| CLUS-05 (membership, init/join/remove, promotion) | 13-05, 06, 09, 10, 16 | SATISFIED | SC1 evidence above |
| CLUS-06 (rolling upgrade without draining, version gates) | 13-04, 11, 14 | SATISFIED (real N-1 human) | SC3 evidence. 13-04 fully landed (see below) |
| CLUS-07 (module GC, migrate-runs) | 13-15, 17 | SATISFIED | SC4 evidence |
| ENG-10 (WIT lines side by side) | 13-07, 14 | SATISFIED | linker_set + the wit-next-fixture CI step |
| API-08 (any-node routing, RYW) | 13-12, 13 | SATISFIED | SC2 evidence |

No orphaned requirements: REQUIREMENTS.md maps exactly CLUS-05, CLUS-06, CLUS-07, ENG-10 and API-08 to Phase 13. All five are marked Complete with evidence.

### Plan 13-04 status (coordinator question)

Nothing from 13-04 is still open in code. Task 2 (the partition fail-stop) landed on main as **db09626**: `StoreError::NewerFormat` on an unknown Cmd tag, a re-encode length mismatch, or a feature above level_max under emulation, plus the `reject_and_continue` mutant and the `apply_newer_format` / `level_emulation_apply` tests. It came with **a649ebd**, the user-approved `rebuild_digest` Op::Raw generator change. Checked against the 13-04 must_haves:
- fail-stop with `Rejected(Invalid(Empty))` kept for empty bytes: `apply_newer_format` 5/5 green (re-run log l.2054-2063);
- level-emulation leg: `level_emulation_apply` (`emulated_level_1_binary_fail_stops_on_repin_run`, `child_level_1`) green in the failpoint suite (fp2 log l.224-228);
- both mutants bite in the required matrix.

The `wip/13-04-task2`, `wip/13-09-seam` and `wip/13-18-oracles` branches are superseded by equivalent commits on main (db09626/a649ebd, 774e59d, 5459b70/d7e6b91).

Three tracking files are stale (documentation only, not a code gap). The orchestrator should fix them when it marks the phase complete:
- ROADMAP still shows `- [ ] 13-04-PLAN.md ... PARTIAL`;
- STATE.md line 61 still says `Phase 13 13-04: PARTIAL ... HELD`;
- the 13-04-SUMMARY headline still says Task 2 is HELD, and its frontmatter has `requirements-completed: []`.

### Anti-Patterns Found

| File | Pattern | Severity | Impact |
|---|---|---|---|
| (none in Phase 13 production code) | TODO(p13-children) was removed in 7942813. The P13 grep gates (clock, journal literal, Chronik, InitCluster in joiner) pass | - | - |
| `crates/ostro-engine/src/host/linker_set.rs` | 0.2 line only under `wit-next-fixture` | Info | By design: WIT 0.2 does not exist yet and 0.1.0 is frozen. The side-by-side mechanism is real and CI-tested |

### Known gaps (orchestrator list): judgement

1. **10k-fault campaign stopped at 1,110 faults** (/dev/shm full; b47721e, child runs off, before d33fce7). **Does not block the goal; human_needed (by-design manual gate).** No oracle failed. Membership and upgrade oracles also passed in the 500-fault (13-19) and 50-fault (per-commit CI, child runs on) campaigns. Concern worth tracking: partition logs showed `local_floor=0 floor=0` for 3.6 h. The local floor is the oldest local snapshot (`leader/retention.rs`), so no snapshot was taken in that run. The defaults are `interval` 1 h and `bytes_threshold` 256 MiB, and the leader-only SnapshotService cadence likely restarts with every leadership change, so heavy leader churn (a fault every ~12 s) may postpone snapshots and trimming indefinitely. That is a Phase 8 retention liveness issue that shows up under Phase 13 churn, not a Phase 13 success criterion. Suggested: diagnose it when re-running the campaign (Phase 14 todo).
2. **Controlled-shutdown follower-ISR stall** deferred to 14-33 (user decision). **Does not block.** SC3 requires "none drained": 0 lost runs, and no drain is involved. The stall is availability-only (followed partitions p99 ~1.1-1.4 s in process_shutdown). 14-33-PLAN.md exists and targets p99 < 1 s.
3. **Joined-node proxy routing in tier C** open. **Does not block.** This is a harness fault-injection gap: the chaos proxy does not cut traffic to joined nodes (STATE 13-18 note). Product routing through every node is covered by tier B routing parity and tier C RYW / promotion (joined nodes serve). Tracked in STATE Phase 14 follow-ups.
4. **Real previous-binary upgrade** needs a release tag. **Does not block; human_needed by design** (ROADMAP Phase 13 plans line). Emulated levels run on every commit.
5. **Timings on shared loaded nodes.** **Does not block; human_needed by design** (ROADMAP: native timings human_needed). No success criterion sets a numeric bound.

### Human Verification Required

1. **10k campaign.** Test: run `cluster_campaign_p13` with `OSTRO_CAMPAIGN_FAULTS=10000` at a commit containing d33fce7, child runs on, disk-backed `OSTRO_PROC_ROOT`. Expected: target reached, all oracles pass, 0 lost. While it runs, check whether `floor` rises above 0 (snapshots/trim under churn). Why human: manual gate run (D-13-20-3), hours long and storage-bound.
2. **Real N-1 upgrade.** Test: `OSTRO_PREV_BIN=<first tagged binary>` and run `upgrade_from_prev`. Expected: rolling upgrade, 0 lost, finalize. Why human: needs a release tag.
3. **Quiet-node timings.** Test: re-run the tier C measurement tests on dedicated nodes. Expected: at or below the 13-20 table. Why human: needs dedicated hardware.

### Gaps Summary

No automated must-have failed. All four success criteria and all five requirements have code that exists, is substantive and is wired, plus CI evidence at a commit code-identical to main. Each criterion has mutants that bite. What remains is the three by-design manual items above, the stale 13-04 tracking text (ROADMAP checkbox, STATE line 61, 13-04-SUMMARY headline), and two follow-ups already tracked for Phase 14: the follower-ISR stall (14-33) and tier-C joined-node proxy injection. One new concern should be recorded: no snapshot/trim (`floor=0`) under long campaign churn.

---

_Verified: 2026-11-26T10:43:16Z_
_Verifier: Claude (gsd-verifier)_
