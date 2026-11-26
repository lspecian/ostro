---
phase: 13-cluster-membership-routing-upgrades
plan: 19
subsystem: Phase 13 tier-C evidence (multi-process promotion, rolling upgrade, RYW, fences, shutdown timing, campaign)
tags: [tier-c, promotion, rolling-upgrade, ryw, downgrade, controlled-shutdown, prewarm, campaign, measurements]
requires:
  - phase: 13-08..13-18
    provides: "P13Harness for ProcCluster, every Phase 13 feature, the Phase 12 + 13 oracles"
provides:
  - "scenarios::process_p13 (tier-C tools: mixed workload with child runs + per-partition probes, run-outcome / exactly-once oracle, final checks, standby identity, upgrade oracles, reports under $OSTRO_P13_REPORT_DIR)"
  - "tests/{process_promotion,process_rolling_upgrade,process_ryw,process_downgrade,process_shutdown,upgrade_from_prev,cluster_campaign_p13}.rs (all #[ignore], tier C)"
  - "ProcOptions::{level_max,bin}, ProcCluster::{stop_gracefully (pub),set_bin,set_level_max}; scenarios::ryw::tier_c"
affects: [13-20]
tech-stack:
  added: []
  patterns:
    - "static three-node cluster at an emulated level (ProcOptions::level_max) so chaos-proxy faults reach every link during a rolling upgrade"
    - "campaign in two phases: finalized 1 under a hold (emulated-level rolling restarts), then level 2 (drains move replicas)"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/process_promotion.rs
    - crates/ostro-repl-conformance/tests/process_rolling_upgrade.rs
    - crates/ostro-repl-conformance/tests/process_ryw.rs
    - crates/ostro-repl-conformance/tests/process_downgrade.rs
    - crates/ostro-repl-conformance/tests/process_shutdown.rs
    - crates/ostro-repl-conformance/tests/upgrade_from_prev.rs
    - crates/ostro-repl-conformance/tests/cluster_campaign_p13.rs
  modified:
    - crates/ostro-repl-conformance/src/scenarios/process_p13.rs
    - crates/ostro-repl-conformance/src/harness/process.rs
    - crates/ostro-repl-conformance/src/scenarios/ryw.rs
key-decisions:
  - "D-13-19-1: the tier-C rolling upgrade, RYW, downgrade, shutdown and campaign clusters are static ([[cluster.peers]] through the chaos proxy) started at OSTRO_TEST_LEVEL_MAX=1; promotion uses the real init + join flow. Joined nodes dial peers directly (13-18 handover), so proxy faults only reach static links"
  - "D-13-19-2: the campaign runs phase A (first fifth, >= 10 faults) at finalized 1 under a hold, where rolling restarts flip OSTRO_TEST_LEVEL_MAX 1 <-> unset, then finalizes and runs phase B at level 2 (at level 1 AlterReplicas is gated, so drains cannot move replicas)"
  - "D-13-19-3: OSTRO_P13_CHILD_RUNS (default on) puts parent_attached runs with a cross-partition child into the workload; =0 leaves them out so the other oracles can be measured while the child-run loss below is open; every report names the mode"
requirements-completed: []
duration: "about 2 days wall clock (two machine crashes, two fix rounds by p13-bugs2); cluster time about 5 h"
completed: 2026-11-26
---

# Phase 13 Plan 19: Tier-C evidence Summary

**Phase 13 success criteria 1–3 now have multi-process evidence on native Linux. With real `ostro` failpoint processes, real SIGTERM / `kill -9` and chaos-proxy faults, the following hold: a running single node is promoted to RF 3 under load and survives `kill -9` of the original node; three emulated rolling upgrades from level 1 complete under load and proxy faults with 0 lost or duplicated runs; RYW holds across processes through leader moves, failover and a proxy-isolated zombie; the downgrade fences hold before and after finalization; controlled-shutdown timing is measured; and the 500-fault Phase 13 campaign passes every oracle. One product gap stays open: child runs are lost under restarts (finding 1; reproduced, not fixed here).**

All numbers were measured on the microk8s cluster (`ostro-k8s-ci`, namespace `ostro-ci`) on **native Linux**: Dell nodes (32 cores, 256 GB, shared with other workloads), pods with 16 CPUs, kernel 6.8.0-149. The clusters ran on `/dev/shm`. dell-1 hosted slot 0, dell-2 slot 1; dell-3 (the Phase 12 campaign) was never used. The nodes were not quiet: CI groups and other executors ran alongside, so the p99s include that load. A quiet-node re-measurement would need an exclusive slot (not taken). The `host:` line of every report names the node.

## Tests (all `#[ignore]`, tier C, `--release --features failpoints --test-threads=1`, binary `cargo build -p ostro --features failpoints`)

| test | what | final run (commit, node) | result |
|---|---|---|---|
| `process_promotion` | single node + workload → `cluster init` → 2 × `ostro cluster join` → RF 3 / ISR 3 / min_insync 2 + 3 voters → `kill -9` node 1 | 478e54d, dell-2 | PASS |
| `process_rolling_upgrade` (`OSTRO_UPGRADE_ITERS=3`) | 3 static processes at level 1, workload + 20 ms delay on every link + one 1.5 s partition per iteration; each node SIGTERM → restart without the variable; finalized 1 until the last, then 2; gated features 409 before / work after; oracles | 478e54d, dell-2 | PASS 3/3 |
| `process_ryw` | 13-13 session workload (6 sessions, 9000 ops) through a SIGTERM leader move, `kill -9` failover, proxy-isolated zombie still in the load balancer | 478e54d tree (c checkout), dell-2 | PASS, 0 violations |
| `process_downgrade` | hold → level-1 restart serves; release → finalized 2 → level-1 start refused before any partition file changes | 478e54d, dell-2 | PASS |
| `process_shutdown` | 10 SIGTERMs of the busiest leader under load | 478e54d tree, dell-2 | PASS |
| `upgrade_from_prev` | without `OSTRO_PREV_BIN`: SKIP line; with it, a rolling upgrade from the previous binary | 0c971db, dell-2 | PASS (SKIP) / PASS (hook check, see below) |
| `cluster_campaign_p13` (`OSTRO_CAMPAIGN_FAULTS=500`) | Phase 12 menu + join, drain + remove, handoff storm, meta leader transfer, rolling restarts at an emulated level | 478e54d, dell-1, `OSTRO_P13_CHILD_RUNS=0` | PASS |

With `OSTRO_P13_CHILD_RUNS` on (the default), `process_ryw` and `process_shutdown` pass. The rolling upgrade, downgrade, promotion and campaign fail only on lost child runs (finding 1).

## Reports (pasted from `$OSTRO_P13_REPORT_DIR/<test>/report.txt`)

### process_promotion (478e54d, dell-2)
```
init: 2.045951ms; policy {"max_voters":5,"min_insync":2,"rf":3}; members 1
join: node 2 ready after 443.141674ms
join: node 3 ready after 531.83544ms
time-to-RF-3: 3.758309397s from the first join (two joins 975.011155ms, then RF 3 / ISR 3 / min_insync 2 + 3 voters after 2.783295288s)
leaders before the kill: [(0, 1), (1, 1), (2, 1), (3, 1)]
kill -9 node 1 (Killed): meta leader now 3; every partition led by a survivor after 1.365763931s
workload: 529 acknowledged runs {Steps: 63, Hot: 63, Signal: 61, Probe: 342}; 0 keys given up after 60s; 8 non-2xx start answers (retried); exactly-once key violations 0
outcomes: 529 acknowledged runs checked: statuses {"completed": 529}; lost 0; not completed 0; duplicate journal entries 0
final checks: replica identity, acked on disk (529 acks over 3 nodes), LA rule, ISR-only elections: pass (meta snapshot index 56, 25 log entries)
```
Before p13-bugs2 fix 2 (abd0fa7), every join took 60.5 s and time-to-RF-3 was 121 s (finding 3). An earlier run at 0f8ed72 acknowledged and completed 12.5k runs with 0 lost.

### process_rolling_upgrade ×3 (478e54d, dell-2; per-iteration oracle lines identical in shape)
```
iteration N: finalized 1 on [1, 2, 3]; reassign + migrate refused 409 feature_not_enabled
  finalized 2 on every node 0.54-1.2 s after the last restart (finalized stayed 1 after each earlier restart)
  reassign p0 [1, 2, 3] -> [1, 2] -> [1, 2, 3] after finalization (110-435 ms each)
  migrate replay_demo v1 -> v3 after finalization: 2 runs repinned, module v3, completed
  outcomes: about 2.5-2.9k acknowledged runs per iteration, all completed; lost 0; duplicate journal entries 0; exactly-once key violations 0
  standby identity: pass
  upgrade oracles: no_gated_before_finalize_from(1): pass (85-93 meta entries, 12 partition logs); no_reject_continue: pass (about 35k apply outcomes)
  final checks: replica identity, acked on disk, LA rule, ISR-only elections: pass
controlled shutdown (SIGTERM -> exit): p50 702.41664ms / p99 873.301629ms / max 873.301629ms (n=9)
restart (spawn -> ready line): p50 710.760619ms / p99 841.210224ms / max 841.210224ms (n=9)
handoff unavailability per partition per restart (longest probe-ack gap over the restart window): p50 276.918393ms / p99 1.720740051s / max 1.720740051s (n=36)
first activation after an upgraded restart (start -> completed through the restarted node, one run per partition): p50 454.452403ms / p99 602.854689ms / max 602.854689ms (n=36)
the second run per partition through the restarted node, right after: p50 459.035696ms / p99 529.442846ms / max 529.442846ms (n=36)
the same timing before any restart under the same link delay (baseline, every node): p50 425.452296ms / p99 523.1835ms / max 523.1835ms (n=36)
finalization after the last restart: p50 540.903527ms / p99 564.768998ms / max 564.768998ms (n=3)
acknowledged runs (all iterations): 8613
```
**AOT pre-warm (first-activation p99).** After every restart, `ostro_cluster_prewarm_modules_total{result="compiled"}` on the restarted node was 2–4, so the workload's modules were compiled before their first activation. The first run through a restarted node is p50 +29 ms / p99 +80 ms over the steady-state baseline taken under the same 20 ms link delay; the second run equals the baseline. Absolute values are dominated by the injected delay (five replicated appends at 2 × 20 ms each). A one-off outlier of 1.96 s (one partition, one restart, n=12) appeared in two earlier runs, never in the final ×3. An earlier baseline taken without the link delay (18 ms) was not comparable and was replaced (55c053d).

### process_ryw (dell-2)
```
leader moves: SIGTERM node 1 (led [0, 3]) exited in 169-181 ms (graceful true); leaders after [(0, 2), (3, 2)]; restarted
failover: kill -9 node 2|3 (led p0|p2); successor after 624-629 ms; restarted
zombie: node 1 (led p0|p2) isolated by the chaos proxy, still in the load balancer; successor after 627-734 ms; healed after +4 s
workload: 9000 ops, 4518-4525 writes acked, 3960-3964 reads recorded, 23-37 unavailable, 0 retried terminates, 0 unexpected; read p50 4.8ms p99 22-28ms, write p50 10.3ms p99 16ms
result: PASS (0 RYW violations)
```

### process_downgrade (478e54d, dell-2)
```
before finalize (hold set): all nodes restarted at level 2, finalized stayed 1 (5 s sampled); node 3 restarted at level 1 serves (run completed in 70.88066ms); finalized still 1
release -> finalized 2 on every node after 314.642924ms
after finalize: node 3 started at level 1 -> refused, process exit status: 1, log: ERROR ostro_cluster::compat: boot fence refused: data finalized by a newer Ostro (level 2, this binary supports up to 1); upgrade this binary finalized=2 level_max=1
  partition files unchanged (mtime + length) from the test process: 196 files (172 under state/, 16 under wal/)
  node 3 restarted at level 2 serves again (run in 136.727831ms); finalized {1: 2, 2: 2, 3: 2}
outcomes: 279 acknowledged runs checked: statuses {"completed": 279}; lost 0
```

### process_shutdown (10 repeats, dell-2)
```
SIGTERM -> process exit: p50 179.708469ms / p99 207.091608ms / max 207.091608ms (n=10)
restart -> ready line: p50 442.202145ms / p99 554.85283ms / max 554.85283ms (n=10)
unavailability of the partitions the stopped node led: p50 564.860102ms / p99 676.688242ms / max 676.688242ms (n=23)
unavailability of the other partitions over the same windows: p50 568.367109ms / p99 669.773603ms / max 669.773603ms (n=17)
handed off before the exit (election reason RESIGN): 23; not: []
outcomes: 345 acknowledged runs checked: statuses {"completed": 345}; lost 0
```
In the latest run (child runs on, 478e54d tree), the numbers were the same within noise: 0 failed runs, all handoffs RESIGN. The SIGTERM path runs 13-11's `before_shutdown`: every partition the stopped node led had a successor elected with reason RESIGN before the process exited. The partitions it did not lead stall just as long (finding 4).

### upgrade_from_prev
```
SKIP: OSTRO_PREV_BIN not set (human_needed until the first release tag)
```
Hook check with `OSTRO_PREV_BIN` set to a copy of the same failpoint binary (0c971db, dell-2): previous → this build, 585–650 ms per node; finalized 2 18 ms after the last upgrade; 185 runs completed; final checks pass. **human_needed:** a real N−1 run, once the first release tag exists.

### cluster_campaign_p13, 500 faults (478e54d, dell-1, OSTRO_P13_CHILD_RUNS=0)
```
phase A -> B: every member at this build's level, hold released, finalized 2 after 745.799163ms
faults injected: 500 of 500 (phase A: 100); by kind: {LeaderKill: 61, FollowerKill: 50, ControllerKill: 47, ProxyPartition: 52, WalFailKill: 62, Join: 47, DrainRemove: 46, HandoffStorm: 45, MetaTransfer: 38, RollingRestart: 52}
batch checks: 50 (every 10 faults); final disk check over 125801 acks on members [1, 2, 3, 50]: pass
removed nodes: {4..49}; joined members at the end: [50]
workload: 125801 acknowledged runs {Steps: 41934, Hot: 41933, Signal: 41934}; 0 keys given up after 60s; 18683 non-2xx start answers (retried); exactly-once key violations 0
lost acked runs: 0; duplicate journal entries: 0
final checks: replica identity, acked on disk (125801 acks over 4 nodes), LA rule, ISR-only elections: pass (meta snapshot index 6834, 1 log entries)
membership oracles (voters_odd, joint_left_within 5 s, removed_never_return over 50 meta logs): pass over 1181 status samples
upgrade oracles: no_gated_before_finalize_from(1): pass (1 meta entries, 16 partition logs); no_reject_continue: pass (2723799 apply outcomes, 0 trimmed logs skipped)
result: PASS; elapsed 4703.970272446s
```
The campaign stayed far inside the 4 h time box, so there is no HUMAN_NEEDED marker. **Weak spot:** by the end, the meta log on disk was compacted to one entry after the snapshot. `no_gated_before_finalize_from` therefore checked the partition logs, but almost none of the meta history. The rolling-upgrade runs above cover the meta side with their uncompacted logs (85–93 entries). The same campaign with child runs on (478e54d tree, dell-1) also injected 500 / 500 faults with every batch disk check green, then failed on 143 lost child runs out of 36,920 (finding 1).

## Findings (product, reported; fixes landed by p13-bugs2 where noted)

1. **OPEN: child runs lost under restarts.** Cross-node child delivery (767d3ee) works in steady state; `process_shutdown` (level 2, 10 SIGTERMs) lost 0 of 33. Under restarts while finalized at level 1, `parent_attached` runs never reach a terminal status within 180 s:

   | run | lost |
   |---|---|
   | rolling upgrade ×1 | 77 of 263 |
   | `process_downgrade` | 41 of 87 |
   | campaign | 143 of 36,920 (phase A was at level 1) |
   | `process_promotion` (level 2, after `kill -9` of node 1, all on p3) | 3 of 32 |

   Every other run kind completed. Logs: `target/k8s-ci-logs/20261126-024331-s1-034a5f05-cmd.log`, `20261126-024346-s0-bf6d60f3-cmd.log`. Root cause not diagnosed: 767d3ee has no level gate, so the level-1 correlation is unexplained. Needs an owner; the tests keep child runs on by default, so they stay red until then.
2. **FIXED (1769996): a restarted node fail-stopped at boot** ("metadata proposal failed: not the raft leader (no leader known)") after a join had made a joined node the meta leader. Found by the campaign. The address book of a restarted static node held only `[[cluster.peers]]`.
3. **FIXED (abd0fa7): every join into a one-voter cluster cost 60 s** (`wait_first_terms` treated `voters().len() == 1` as single). Found by `process_promotion`; time-to-RF-3 went from 121 s to 3.8 s.
4. **DEFERRED (documented by p13-bugs2): ISR stall on controlled shutdown.** The stopping node is not removed from follower ISRs before it exits, so partitions it did not lead stall about one session timeout (`process_shutdown`: p50 568 ms, the same as the partitions it handed off).
5. **Pre-existing, not 13-19:** the plan check's only red was `ostro::backup_cli::bucket_online_and_offline_round_trips` (`/readyz` not 200 within 60 s; test2 and musl2), bisected below.

## Deviations from Plan

1. **[Rule 3] Harness additions outside `files_modified`.**
   - `harness/process.rs`: `ProcOptions::level_max` / `bin`, `ProcCluster::set_bin` / `set_level_max`, and `stop_gracefully` made public (it returns whether the process exited on its own).
   - `scenarios/ryw.rs`: `load_cfg_with` plus a public `tier_c` wrapper over the 13-13 session workload and checker (its fields are module-private).
2. **[Design, D-13-19-1] Static clusters for the proxy-fault tests.** Joined-node proxy routing is still open (13-18 handover); the tests document it.
3. **[Design, D-13-19-2] The campaign runs in two phases.** At finalized level 1, replica moves are gated.
4. **[Scope] No level-2 module upload in tier C.** The shipped binary has no level-2 WIT line; tier B covers the upload gate (13-14 / 13-18). Reassign and migrate before and after finalization are covered.
5. **[Scope] Leader moves in `process_ryw` are SIGTERM restarts (controlled-shutdown handoffs), not `NodeOps.HandOff` calls.** The test process holds no node identity.
6. **[Test fix] First-activation baseline under the same link delay** (55c053d); second-run timing added.
7. **[D-13-19-3] `OSTRO_P13_CHILD_RUNS` switch** for finding 1 (default on).
8. **[Process] Paths.** Worktree under `~/.local/share/ostro-orchestrator/wt/13-19` and `target/slot-2` (executor brief), not the plan's /tmp paths. A WSL crash wiped the first worktree; the work was redone from main. Reports were written under `/tmp/ostro-p13-reports` inside the CI pods and are pasted above.

## Verification

- clippy `-D warnings` and fmt on ostro-repl-conformance (lib + the seven targets): clean.
- Plan check `ostro-k8s-ci --parallel 478e54d` (35 min): every step PASS — lint, clippy, build, sdk, guests, polyglot, failpoint suites, replication mutant matrix, server campaign smoke, grep gates, deny, musl size — except `cargo test` and `musl zigbuild test`. Both failed only on `ostro::backup_cli::bucket_online_and_offline_round_trips` (finding 5). Its re-run alone at 478e54d and at 55c053d (before p13-bugs2) is recorded below.

### Bisect of finding 5
`cargo test -p ostro --test backup_cli -- --test-threads=1`, twice each, cluster slot 1 (dell-2):
- 478e54d: 2/2 passed (6.2 s, 5.4 s)
- 55c053d (before p13-bugs2): 2/2 passed (5.5 s, 5.4 s)

Load-sensitive, not a regression: the server boot exceeded the test's 60 s `/readyz` bound inside the loaded test2 / musl2 pods. It joins the flaky list next to `flow_e2e` boot under load. Logs: `target/k8s-ci-logs/20261126-060929-s1-478e54da-cmd.log`, `20261126-061115-s1-55c053d9-cmd.log`.

## ci.yml / ci-local hunk for 13-20 (not applied)

A tier-C Phase 13 step: release mode, the failpoint binary, `--ignored --test-threads=1`. Keep it in sync with ci-local:

```yaml
      - name: Phase 13 tier C (multi-process)
        env:
          OSTRO_P13_REPORT_DIR: ${{ runner.temp }}/ostro-p13-reports
          OSTRO_UPGRADE_ITERS: "3"
          OSTRO_CAMPAIGN_FAULTS: "50"
        run: |
          cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
          for t in process_promotion process_rolling_upgrade process_ryw process_downgrade process_shutdown upgrade_from_prev cluster_campaign_p13; do
            cargo test --release -p ostro-repl-conformance --features failpoints --locked --test "$t" -- --ignored --test-threads=1
          done
```

Until finding 1 is fixed, either set `OSTRO_P13_CHILD_RUNS: "0"` there (and track the finding) or expect red. The binary path the harness looks for is `target/failpoints/debug/ostro`; `OSTRO_FAILPOINT_BIN` overrides it.

## Handover for 13-20

- **CI:** the step above (and the same in ci-local). The ≥500-fault campaign (about 80 min) belongs in a nightly or manual job, not per-push.
- **Docs, test environment variables:**

  | variable | default |
  |---|---|
  | `OSTRO_P13_REPORT_DIR` | absolute, `/tmp/ostro-p13-reports` |
  | `OSTRO_UPGRADE_ITERS` | 1 (gate 3) |
  | `OSTRO_CAMPAIGN_FAULTS` | 50 (smoke ≥ 500) |
  | `OSTRO_CAMPAIGN_TIMEBOX_S` | 4 h |
  | `OSTRO_SHUTDOWN_REPEATS` | 10 |
  | `OSTRO_PREV_BIN` | real N−1 run; human_needed |
  | `OSTRO_P13_CHILD_RUNS` | on |
  | `OSTRO_FAILPOINT_BIN`, `OSTRO_PROC_ROOT`, `OSTRO_PROC_KEEP` | existing harness variables |

- **Docs, behaviour:**
  - the controlled-shutdown ISR stall (finding 4) and its measured cost;
  - the pre-warm effect (first activation within about 30 ms p50 of steady state).
- **Open:**
  - finding 1 (child runs under restarts);
  - joined-node proxy routing (tier C);
  - `upgrade_from_prev` human_needed;
  - quiet-node re-measurement if exclusive numbers are wanted.

## Self-Check: PASSED
- Files: the seven tests under `crates/ostro-repl-conformance/tests/` and `src/scenarios/process_p13.rs` exist on main.
- Commits on main: 0f8ed72, 0c971db, 55c053d, 478e54d.
