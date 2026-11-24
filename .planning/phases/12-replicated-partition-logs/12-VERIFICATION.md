---
phase: 12-replicated-partition-logs
verified: 2026-11-24T11:05:00Z
status: human_needed
score: 4/4 success criteria verified; 4/4 requirements satisfied in code; 104/105 plan must-have truths verified, 1 pending the approved campaign re-run (QA-01 10k)
human_verification:
  - test: "Re-run the 10,000-fault tier-C cluster campaign with the user-approved (2026-11-24) D-P12-27 trimmed-log rule wired into tests/cluster_campaign.rs disk_check and scenarios::process::tools::final_checks (OSTRO_CAMPAIGN_FAULTS=10000 ... --test cluster_campaign -- --ignored)"
    expected: "0 lost acked runs, 0 duplicate journal entries over >= 10,000 faults"
    why_human: "PENDING the campaign re-run (another agent runs it now). The 12-18 run stopped at fault 1,030 only because the campaign checker was WAL-only; the post-mortem shows all 133,443 flagged runs on 3/3 nodes. Not a gap."
  - test: "Failover p99 and RF=3 benchmark on native Linux NVMe, 8 vCPU (failover_p99 with OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1; bench_rf3)"
    expected: "p99 < 10 s (WSL2 advisory: 5.44 s over 200 iterations)"
    why_human: "WSL2 numbers are advisory (D15)"
  - test: "SeaweedFS/S3 legs of the cluster bucket tests (ostro-backup cluster_head / cluster_repair against S3)"
    expected: "green in CI service containers"
    why_human: "No Docker locally (P8-D18); SeaweedFS only, no MinIO"
  - test: "Push and confirm remote CI: the new 'Phase 12 grep gates', 'Failpoint suites' with OSTRO_REPL_REQUIRE_ALL=1 and 'Replication mutant matrix' steps (ci.yml hunk applied in 151a6cf), plus aarch64 legs"
    expected: "all green"
    why_human: "No git remote yet (FOUND-05)"
  - test: "macOS CI legs"
    expected: "green (the only known macOS-only failure, stale_epoch, was fixed in the harness in aaa5eab)"
    why_human: "Deferred by the user's Linux-first decision"
  - test: "User review of D-P12-2 (static multi-voter meta pulled forward), D-P12-3 (cluster-wide restore deferred), D-P12-4 (parsed-base rule for a torn first record of an anchored log)"
    expected: "user accepts or overrides the adopted defaults (12-DECISIONS.md, STATE Pending Todos)"
    why_human: "Adopted defaults awaiting the user's review"
---

# Phase 12: Replicated Partition Logs Verification Report

**Phase Goal:** Partition logs replicate across nodes with no acknowledged-write loss. Chronik's pure algorithms are ported behind the existing `ReplicatedLog`, and the conformance suite gates everything.
**Verified:** 2026-11-24 (main at 151a6cf, detached worktree, build slot target/slot-2)
**Status:** human_needed
**Re-verification:** No, initial verification

## Independent runs by the verifier (at 151a6cf)

| Command | Result |
| --- | --- |
| `OSTRO_REPL_REQUIRE_ALL=1 cargo test -p ostro-repl-conformance --features failpoints --locked --test mutants -- --test-threads=1 --nocapture` | ok, 274 s; **11** `FAILED as expected` (isr_from_assignment, hw_leader_leo x2, skip_epoch_check, no_truncate, unclean_election x2, ignore_min_insync, shrink_before_commit, upload_above_hw, rf1_bypass), 0 `SKIP (pending)`; count_nondurable_leo listed as TIER-C |
| `OSTRO_REPL_RED_RUN=1 cargo test -p ostro-repl-conformance --locked --test red_run --test skeleton -- --ignored --test-threads=1` | ok, 249 s; **29/29** scenarios `pending no / pass`; `no_pending_left` ok |
| `cargo test -p ostro-cluster --features failpoints --locked -- --test-threads=1` | all green: lib 22, controller 10, fetch_mutants 2, fetch_service 7, fetcher 9, install 6, install_crash 4 (192 s), isr_publisher 7, liveness 10, meta_peer 3, snapshot_stream 9 |
| `cargo test -p ostro-log --features failpoints --test replica_mutants --test partition_log_mutants --test replica_model --test replica_unit --test partition_log_leader --test partition_log_follower --test torn_tail` | all green |
| `cargo test -p ostro-meta --locked` | all green (incl. multi_voter 3, meta_golden, elect/alter/resign apply tables) |
| `bash scripts/p12-grep-gates.sh` | rc 0 |
| ci-local `check_failpoint_sync` against the applied ci.yml | in sync; ci.yml parses as YAML |

Note: `install_crash` fails 2/4 when run with parallel test threads (global `fail` registry, no serialization). The project runs every failpoint suite with `--test-threads=1` (ci-local FAILPOINT_CRATES, ci.yml), where it passes. Not a gap; listed as Info below.

Prior evidence used (not re-run, per instructions): full ci-local with musl at 754f271 (`target/p12-gate/ci-local.log`, the single `cargo test` red is the missing-`CACHEDIR.TAG` environment issue, re-run green in `12-18-triggers-rerun.log`; the musl leg ran 388 test binaries, 3241 passed, 0 failed); tier-C `process_divergence` / `process_power_loss` 10/10 and `process_mutants` bite (`target/p12-gate/12-18-process_*.log`); failover p99 report (`target/failover-p99/report.txt`, 200 iterations, p99 5438 ms, 67 controller targets, 66 blackholes, 0 not resumed).

## Goal Achievement: Success Criteria

| # | Success criterion | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Conformance suite exists before integration and fails on a broken build; covers placement, ISR honesty from measured LEOs, divergence/truncation, failover, min-insync | VERIFIED | Suite landed in 12-06/12-07 (wave 4), integration in 12-14 (wave 6); red-run tables in 12-06/12-07 SUMMARYs. Ten `repl::mutant::*` sites in production code (isr.rs, watermark.rs, tracker.rs, fetcher.rs, fetch.rs, failover.rs, partition_log.rs, uploader.rs, isr_publisher.rs); verifier's mutant run: 11 bites, 0 pending; tier-C count_nondurable_leo bites (12-18 gate log). Scenario groups in `crates/ostro-repl-conformance/src/scenarios/` cover all listed areas |
| 2 | RF=3: HW = min(LEO over ISR); stale-epoch fetches rejected; dead leader's divergent tail truncated on rejoin incl. object storage; suite 100% green | VERIFIED | `crates/ostro-log/src/replica/watermark.rs:27-61` (min over maximal ISR, frozen below min_insync, monotone); `crates/ostro-cluster/src/fetch.rs:237-299` (NOT_REPLICA -> FENCED/UNKNOWN epoch -> NOT_LEADER -> divergence -> progress); `crates/ostro-log/src/replica/divergence.rs` (UNDEFINED -> SnapshotRequired, KIP-279 iteration); `crates/ostro-backup/src/cluster.rs` takeover repair + epoch-fenced `p/<pppp>/head`. Red run 29/29 incl. hw_is_min_over_isr, fetch_with_stale_epoch_fenced, leader_dies_with_unreplicated_writes, isolated_leader_rejoin_truncates, two_quick_leader_changes_truncate, remote_chain_equals_committed_prefix, remote_chain_never_above_hw; tier-C process_divergence 10/10 with real kill -9 |
| 3 | Leaders elected only from published ISR; writes stop when ISR < min_insync | VERIFIED | Meta apply `crates/ostro-meta/src/state.rs:353-424` rejects any leader/ISR member outside the published ISR (also leader 0) and stale epoch/isr_version; planner `crates/ostro-log/src/replica/failover.rs` elects only live ISR members, ISR = isr ∩ live, offline keeps one member. `append_allowed` / NotEnoughReplicas gate. Scenarios failover_elects_isr_member, whole_isr_dead_goes_offline, writes_stop_below_min_insync pass with the election::isr_only audit; unclean_election and ignore_min_insync mutants bite |
| 4 | Followers are hot standbys and bootstrap from a snapshot; failover p99 < 10 s; runs resume; RF=1 same code path | VERIFIED (native-Linux timing human_needed) | followers_are_hot_standbys, empty_node_bootstraps_from_snapshot, node_behind_log_start_reinstalls, install_stream_cut_resumes pass; install order with failpoints in `crates/ostro-cluster/src/install.rs:377-408`, install_crash 4/4. Failover p99 5.44 s over 200 iterations, 0 not resumed (WSL2, advisory). One path: `Rf1Log` gone from crates/, grep gates (no `rf == 1` / `replicas.len() == 1` in ostro-log/ostro-cluster), one_path_rf1_hw_through_tracker pass, rf1_bypass bites |

**Score:** 4/4 success criteria verified.

## Plan must-haves (105 truths across 12-01..12-18)

| Plan | Truths | Status | Key evidence |
| --- | --- | --- | --- |
| 12-01 gate | 7/7 | VERIFIED | 08-VERIFICATION passed, 07-VERIFICATION human_needed (by-design); RESERVATIONS P12 rows + env/metric names (lines 388-409); D-P12-1..26 and [USER REVIEW] D-P12-2/3/4 in STATE (564-566); Cargo files touched only by 6f75fb3 (12-01) and 4c165f7 is proto-only; FAILPOINT_CRATES includes ostro-cluster and ostro-repl-conformance and equals ci.yml |
| 12-02 contracts | 7/7 | VERIFIED | `crates/ostro-proto/proto/ostro/cluster/v1/cluster.proto` (Replication 4 RPCs, MetaPeer 3 RPCs), only commit 4c165f7 (never edited later); MetaCommand 0-6, ext 80; ten mutant names in ostro_log::mutants; pending lists exist and are empty |
| 12-03 tier-B harness | 4/4 | VERIFIED | harness/{certs,fault,inproc}.rs, oracle/*.rs with oracle_selftest.rs, runner_selftest.rs, fault_layer.rs, harness_smoke.rs |
| 12-04 pure algorithms | 7/7 | VERIFIED | replica/{tracker,isr,watermark,divergence,failover}.rs; replica_model (proptest state machine) and replica_mutants green in verifier run |
| 12-05 meta commands + MetaPeer | 7/7 | VERIFIED | state.rs elect_leader/alter_isr/resign_leader checks; multi_voter 3/3 green; meta_peer 3/3 green; authz.rs rejects worker/other-cluster certs |
| 12-06 core suite first | 4/4 | VERIFIED | scenarios placement/isr_honesty/hw/stale_epoch/divergence/failover/min_insync; red-run tables in 12-06-SUMMARY; all now pass |
| 12-07 remaining groups + matrix | 4/4 | VERIFIED | standby/bootstrap/fetch_path/blobs/one_path/zombie/object_storage scenarios; tests/mutants.rs with REQUIRE_ALL (verified run) |
| 12-08 one log implementation | 9/9 | VERIFIED | `crates/ostro-log/src/partition_log.rs`; no Rf1Log in crates/; partition_log_leader/follower/mutants and torn_tail green |
| 12-09 tier-C harness | 4/4 | VERIFIED | harness/chaos_proxy.rs, harness/process.rs (127.r.r.i loopback, kill -9); chaos_proxy and process_smoke tests; 12-18 tier-C runs |
| 12-10 controller | 6/6 | VERIFIED | `vendor/ostro-raft-driver/src/driver.rs:1111` sample_live_includes_learners; VENDORED.md wiped-voter text; controller.rs (no SetPartitionLeader per grep gate); liveness.rs; members.rs upsert_self; controller 10/10, liveness 10/10 |
| 12-11 follower-pull Fetch | 6/6 | VERIFIED | fetch.rs fixed order and separate max_wait/read_timeout; fetch decode limit = max_envelope_bytes + 64 KiB (fetch.rs:55-57, fetcher.rs:317); fetcher.rs truncation/blobs-before-append; isr_publisher.rs; fetch_mutants 2/2 |
| 12-12 snapshot bootstrap | 4/4 | VERIFIED | snapshot.rs (OSNP v1 frames, resume), install.rs fixed order + three failpoints; snapshot_stream 9, install 6, install_crash 4 |
| 12-13 role lifecycle | 7/7 | VERIFIED | ostro-partition roles; next_term_epoch only used by `TermMode::Inspect` (`crates/ostro-partition/src/node.rs:1272-1282`); bind_node_id / bind_cluster_id (`node.rs:1473,1495`) |
| 12-14 server integration | 8/8 | VERIFIED | `crates/ostro-server/src/cluster.rs` boot order (doc lines 1-41 match the plan), self-demotion counter (cluster.rs:909), `/v1/admin/replication` route (cluster.rs:670-675), not_leader 503 in control/; legacy path gone (grep gate); conformance red run green |
| 12-15 bottomless cluster mode | 6/6 | VERIFIED | `crates/ostro-backup/src/cluster.rs` (head, fencing metric), uploader.rs:937/956 gauge, restore.rs:403-416 refusal; upload_above_hw bites; remote_chain_* scenarios pass |
| 12-16 suite 100% green | 3/3 | VERIFIED | 29/29 red run, 11 bites, no SKIP (verifier runs) |
| 12-17 multi-process | 6/6 | VERIFIED (timings advisory) | 12-17-SUMMARY: SC2 kill path 10/10, power-loss 10/10 + mutant 5/5, failover p99 200 iterations, sweep kept defaults (0 term changes in 10 loaded min), bench_rf3 90,830/90,830 verified, 500-fault campaign 0 lost / 0 duplicates |
| 12-18 phase gate | 5/6 + 1 pending | VERIFIED except campaign | ci-local green at 754f271 (env-caused red re-run green); ci.yml now carries REQUIRE_ALL + mutant matrix + grep gates (151a6cf, sync check passes); pending lists empty; grep gates rc 0; docs (replication.md, storage-format §10.1, snapshot-format §9.1, bottomless "Cluster mode"); RESERVATIONS flipped; REQUIREMENTS CLUS-01..04 ticked with evidence. The "10,000-fault campaign" half of truth 4 is PENDING the approved re-run (recorded as human_needed per the truth's own "reached count + human_needed" clause) |

## Requirements Coverage

| Requirement | Source plans | Status | Evidence |
| --- | --- | --- | --- |
| CLUS-01 | 12-01..03, 12-06, 12-07, 12-16, 12-18 | SATISFIED | Suite before integration, red runs recorded, mutant matrix mandatory in ci-local and ci.yml, verifier run 11 bites / 0 pending |
| CLUS-02 | 12-04, 12-08, 12-11, 12-14, 12-15, 12-17 | SATISFIED | Follower-pull Fetch over the cluster listener, HW = min over ISR, epoch fencing, divergence truncation incl. object storage (scenarios + tier C) |
| CLUS-03 | 12-04, 12-05, 12-10, 12-11, 12-13 | SATISFIED | ISR-only election at meta apply and in the planner; min_insync append gate and HW freeze |
| CLUS-04 | 12-08, 12-09, 12-10, 12-12, 12-13, 12-17 | SATISFIED (native timing human_needed) | Hot standby, snapshot bootstrap, crash-safe install, failover p99 5.44 s (WSL2), RF=1 same path |

No orphaned requirements: REQUIREMENTS.md maps only CLUS-01..04 to Phase 12.

## Anti-Patterns / Info

| File | Line | Pattern | Severity | Impact |
| --- | --- | --- | --- | --- |
| `crates/ostro-partition/src/leadership.rs` | 10-13 | Doc comment still mentions "the temporary legacy self-leader path ... (removed by 12-14)" | Info | Stale comment only; the path is gone (next_term_epoch is called only from `TermMode::Inspect`, node.rs:1275) |
| `crates/ostro-partition/src/node.rs` | 1256 | Field still named `rf1` on `PartitionStart` | Info | Cosmetic naming; type is `PartitionLog` |
| `crates/ostro-cluster/tests/install_crash.rs` | 150, 243 | Global `fail::cfg` without serialization | Info | Fails under parallel threads; CI runs it with `--test-threads=1` (passes) |
| `.planning/RESERVATIONS.md` | 403, 405, 407-409 | §11 name rows (REST path, install::/fetch::/isr::/elect:: failpoints, `p/<pppp>/head`, `ostro_repl_*` metrics, env vars) still read "planned P12" although all exist in code | Info | Tracking-file staleness; the 12-18 truth only requires the ordinal rows to flip, which they did |
| `.planning/phases/12-replicated-partition-logs/12-VALIDATION.md` | 151, 184-185 | Campaign row says "user decision needed" and the ci.yml row says "apply the hunk" | Info | Both are now resolved (user approved D-P12-27 for the campaign 2026-11-24; ci.yml hunk applied in 151a6cf); remaining action is the campaign re-run and a remote CI run |
| `Cargo.lock` | 615 | `chrono` present | Info | Dev-only (s3s test server, since 08-03); `cargo tree -i chrono -e normal` is empty and cargo deny bans it for normal deps. Not a Phase 12 regression |
| `target/p12-gate/mutant-matrix.log` | 1 | Shows 10 lines starting `MUTANT` | Info | The isr_from_assignment bite shares the `test mutant_matrix ...` line; 11 bites in fact (also in ci-local.log and the verifier's run) |

No TODO/placeholder stubs and no blocker anti-patterns found in Phase 12 production files.

Known design limits (not gaps, recorded for Phase 13): stale reads from a cut-off controller for about 1.5-2 s until self-demotion (13-03 ReadIndex / lease); a new leader can answer SNAPSHOT_REQUIRED before its LeaderAnnounced is logged (safe, costly; 12-18 side observation); `isr_from_assignment` sometimes bites through the status oracle's weaker form (12-16 open item; it bit in every gate run seen).

## Human Verification Required

### 1. 10,000-fault cluster campaign (QA-01): PENDING the campaign re-run
**Test:** Re-run `OSTRO_CAMPAIGN_FAULTS=10000 ... --test cluster_campaign -- --ignored` with the approved D-P12-27 trimmed-log rule in the campaign's checks.
**Expected:** 0 lost acked runs, 0 duplicate journal entries.
**Why human:** In progress in the background (another agent). 12-18's stop at fault 1,030 was the WAL-only checker, not data loss (`target/campaign-p12-gate/HUMAN_NEEDED`, post-mortem: every flagged run on 3/3 nodes).

### 2. Native-Linux failover p99 and RF=3 benchmark
**Test:** failover_p99 (200 iterations, ASSERT) and bench_rf3 on native NVMe, 8 vCPU.
**Expected:** p99 < 10 s.
**Why human:** WSL2 numbers are advisory (D15).

### 3. SeaweedFS/S3 legs of the cluster bucket tests
**Why human:** No Docker locally; CI service containers after the first push.

### 4. Remote CI (Linux x86_64/aarch64): new grep-gate, REQUIRE_ALL failpoint and mutant-matrix steps
**Why human:** No git remote yet.

### 5. macOS legs
**Why human:** Deferred by the user's Linux-first decision.

### 6. User review of D-P12-2, D-P12-3, D-P12-4
**Why human:** Adopted defaults; see 12-DECISIONS.md.

## Gaps Summary

No gaps. Every success criterion, requirement and plan must-have is backed by code and tests that pass when run independently at 151a6cf (mutant matrix 11 bites / 0 pending, red run 29/29, ostro-cluster / ostro-log / ostro-meta suites, grep gates, failpoint sync). The only unmet measurement is the 10,000-fault campaign. It waits on the approved re-run with the D-P12-27 rule and is not a gap. Everything else outstanding is the by-design human_needed set above.

---

_Verified: 2026-11-24_
_Verifier: Claude (gsd-verifier)_
