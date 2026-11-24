---
phase: 12-replicated-partition-logs
plan: 17
subsystem: replication-conformance
tags: [replication, conformance, tier-c, kill-9, failover, p99, sweep, benchmark, campaign, clus-02, clus-04]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-09 ProcCluster + ChaosProxy (tier C); 12-14 integrated server (503 leader hints, /v1/admin/replication, self-demotion); 12-16 tier-B 29/29 green, 11/11 tier-B mutants, D-P12-27 disk-oracle trimmed-log rule"
provides:
  - "Scenario<C = InProcCluster>: the scenario layer is generic over ClusterHarness; runner::run_on_cluster and cluster_formed::check(_within) run on either tier; scenarios::all_process()"
  - "scenarios::process: process_leader_dies_with_unreplicated_writes, follower_power_loss (tier C), run_proc/skip_reason, tools:: tier-C helpers (meta-log assignments, admin meta leader, HintClient following 503 leader hints, settle, final checks)"
  - "ProcCluster: [cluster] advertise = node IP, ProcOptions::replication, failpoints(n), restart_many, restart heals before spawn, API port pinned across restarts"
  - "tests: process_divergence, process_power_loss, process_mutants, failover_p99 (+ sweep), bench_rf3, cluster_campaign (all #[ignore])"
  - "oracle::disk::start_run_ids; acked_on_scans no longer quadratic"
affects: [12-18]
tech-stack:
  added: []
  patterns:
    - "Tier-C power loss = a WAL write that fails before writing a byte (wal::write EIO) then kill -9: a blocking pause failpoint starves the process's tokio workers and its metadata Raft"
    - "Leader hints are followed unless they name the node that just refused (a cut-off old leader's stale self-hint)"
key-files:
  created:
    - crates/ostro-repl-conformance/tests/process_divergence.rs
    - crates/ostro-repl-conformance/tests/process_power_loss.rs
    - crates/ostro-repl-conformance/tests/process_mutants.rs
    - crates/ostro-repl-conformance/tests/failover_p99.rs
    - crates/ostro-repl-conformance/tests/bench_rf3.rs
    - crates/ostro-repl-conformance/tests/cluster_campaign.rs
  modified:
    - crates/ostro-repl-conformance/src/scenarios/process.rs
    - crates/ostro-repl-conformance/src/scenarios/{mod,runner}.rs
    - crates/ostro-repl-conformance/src/oracle/{cluster_formed,disk}.rs
    - crates/ostro-repl-conformance/src/harness/process.rs
    - crates/ostro-repl-conformance/tests/process_smoke.rs
key-decisions:
  - "[replication] defaults kept (meta_election_tick 10, session_timeout 3s): the sweep's most conservative setting already has failover p99 5.6 s < 8 s and 0 meta term changes in 10 loaded minutes; config.rs unchanged"
  - "follower_power_loss uses wal::write=return (EIO before the write) instead of a pause failpoint, and a dead second follower instead of a paused one: fail's pause blocks tokio worker threads (and LIFO-slot tasks), which starved the paused process's metadata Raft so elections never completed"
  - "Failover is measured per partition the target led as max(pre-fault durable-sleep run completes, new start acked); controller faults are every third iteration and no-RST blackholes a third, independently"
requirements-completed: []
duration: ~3 h of implementation and measurement plus the ci-local gate
completed: 2026-11-24
---

# Phase 12 Plan 17: Multi-process kill paths and measurements Summary

**SC2 and SC4 hold with real process death. A leader holding unreplicated writes is `kill -9`'d and truncates its tail on restart (10/10, then 3/3 after the last harness change). A follower whose WAL writes fail is `kill -9`'d and never causes acked loss (10/10 + 3/3), and `count_nondurable_leo` turns that red on the disk oracle (5/5 runs). Leader failover over 200 iterations on the shipped defaults: p50 2.47 s, p99 5.42 s, max 5.70 s, runs resume in every iteration. That covers 67 controller faults and 66 no-RST blackholes. The tuning sweep keeps the defaults (`meta_election_tick 10`, `session_timeout 3s`): p99 5.6 s with 0 term changes in a 10-minute loaded steady state. RF=3 runs 15.1k verified steps/s (debug build, advisory), with every acknowledged step found on disk on 2 or more replicas. A 500-fault crash campaign ends with 0 lost acked runs and 0 duplicate journal entries out of 54,470 acks.**

All WSL2 numbers are advisory (D15). The server under test is the debug failpoint build (`cargo build -p ostro --features failpoints`) and the test harness is a debug build. The native-Linux run is human_needed (commands below).

## Commits

| Commit | Message |
| --- | --- |
| f5c281b | test(12-17): tier-C SC2 kill path, follower power-loss, count_nondurable_leo mutant |
| de6622e | test(12-17): failover p99 harness and meta election tick tuning |
| dfa8ddb | test(12-17): RF=3 bytes-on-disk benchmark and cluster crash campaign |
| a8fa64d | fix(12-17): tier-C nodes keep their API address across restarts |
| 587714e | fix(12-17): bench_rf3 checks completion concurrently with a per-run bound |
| 30247b8 | style(12-17): cargo fmt harness/process.rs (CI lint, flagged by the coordinator) |
| (docs) | docs(12-17): summary, ROADMAP tick, STATE line |

No `chore(12-17): tuned replication defaults` commit was made: the shipped defaults already meet the rule, so `config.rs` is unchanged (see the sweep below).

## Task 1: SC2 kill path, follower power loss, tier-C mutant

**Scenario layer generalized (12-09 handover).** `Scenario<C = InProcCluster>` is generic over the harness. Every tier-B scenario literal is unchanged, because the default type parameter keeps them `Scenario<InProcCluster>`. `scenarios::all()` lists tier B and `scenarios::all_process()` lists the tier-C `process` group. `runner::run_on_cluster<C: ClusterHarness>` runs the measured `cluster_formed` precondition plus the body under its deadline on a cluster the caller started; tier C needs per-process failpoints at start. `cluster_formed::check(_within)` now takes any `ClusterHarness`; it only reads data dirs offline. No oracle semantics changed.

**`process_leader_dies_with_unreplicated_writes`** (`scenarios/process.rs`). The test picks a partition with a full ISR whose leader is not the meta leader and acknowledges 3 runs. It then blackholes every follower's proxy link to the leader: no FIN and no RST, and a parked long-poll answer is held too. It sends K=3 starts to the leader, waits until they are on the leader's own WAL, and `kill -9`s the leader. Once a new leader at a higher epoch is in place, it acknowledges 3 more runs, heals, and restarts the old leader. Checks: the old leader rejoins the ISR, none of the unacknowledged commands sits below the HW on any node, every acked run completes, then replica identity below the HW, `acked_on_disk`, the LA rule on every log, and an ISR-only election audit.

**`follower_power_loss`.** The partition is led by the meta leader, so taking followers down never stalls the controller. Steps:
1. Acknowledge 3 runs.
2. `kill -9` follower G and wait for the ISR to become {L, F}.
3. Restart F with `wal::write=return`: the WAL writer fails every write before writing a byte. Wait until F is back in the ISR.
4. Offer 5 starts.
5. `kill -9` F, then L.
6. Restart G and F together, without failpoints (`restart_many`, since two nodes make the quorum).

The partition then goes offline (correct build: F dropped out of the ISR because it was never durable) or F is elected. Either way the disk oracle must hold, and a new leader must hold every acked `StartRun`. Then L restarts, the acked runs complete, and the final checks run.

**Mutant (`tests/process_mutants.rs`).** The baseline passes. With `FAILPOINTS=repl::mutant::count_nondurable_leo=return` on every process it fails on its target oracle every time:

```
BASELINE follower_power_loss: pass
MUTANT repl::mutant::count_nondurable_leo: follower_power_loss FAILED as expected (disk: acked run 001038gaxra4gw60kwsj7tt2kwfgp (partition 2) is on disk on 1 node(s) [3] < min_insync 2)
```

Results: 5 of 5 mutant runs bit, all on `disk` with "on 1 node(s) < min_insync 2", and all 5 baselines were green. `pending/process.rs` stays empty. The scenarios were green when written, so no pending entry was needed. `skeleton::no_pending_left` passes, so all three pending lists are empty.

| Run | Result |
| --- | --- |
| `process_divergence` default 10 iterations (at f5c281b content) | 10/10 pass, 3.1-3.8 s each |
| `process_power_loss` default 10 iterations | 10/10 pass, 10.1-12.3 s each |
| `process_mutants` | 1 + 3 runs: baseline pass, mutant FAILED as expected each time |
| after the last harness change (587714e): `OSTRO_PROC_ITERS=3` all three tests | 3/3, 3/3, baseline pass + mutant bites |
| `process_smoke` (incl. ignored 3-process test, now asserting formation) | 3/3 |

## Task 2: Failover p99 (200 iterations, shipped defaults)

Configuration: 3 processes, RF 3, min_insync 2, `ReplicationSection::default()`, which is the same in both profiles: session_timeout 3 s, meta_tick_interval 100 ms, meta_election_tick 10, meta_heartbeat_tick 2, replica_lag_max 10 s, fetch_max_wait 500 ms. A steady workload starts a `busy_steps` run on every partition every 200 ms through hint-following clients.

Schedule: every third iteration targets the controller (meta leader). In a third of the iterations, chosen independently, the fault is a chaos-proxy blackhole of all the target's links (no RST) instead of `kill -9`. Measurement: per partition the target led, `max(pre-fault 300 ms durable-sleep run completes, new start acked)`, with a 50 ms client following 503 hints. The worst partition counts for the iteration.

`target/failover-p99/report.txt` (header; the full per-iteration table is in the file):

```
failover p99 (12-17): 200 iterations, 3 processes, RF 3, min_insync 2
config: session_timeout 3s, meta_tick_interval 100ms, meta_election_tick 10, meta_heartbeat_tick 2, replica_lag_max 10s, fetch_max_wait 500ms
steady workload: 10726 starts acknowledged, 2266 not (during faults)
overall                            n=200  p50=  2468 p99=  5419 max=  5699 ms
kill -9                            n=134  p50=  2449 p99=  3486 max=  3487 ms
blackhole (no RST)                 n=66   p50=  4081 p99=  5699 max=  5699 ms
controller target                  n=67   p50=  2476 p99=  5699 max=  5699 ms
non-controller target              n=133  p50=  2463 p99=  4184 max=  4225 ms
kill / non-controller              n=89   p50=  2446 p99=  2496 max=  2496 ms
kill / controller                  n=45   p50=  2462 p99=  3487 max=  3487 ms
blackhole / non-controller         n=44   p50=  4062 p99=  4225 max=  4225 ms
blackhole / controller             n=22   p50=  4795 p99=  5699 max=  5699 ms
step: detection                    n=200  p50=  2323 p99=  4033 max=  4074 ms
step: meta re-election (ctl)       n=67   p50=  1200 p99=  1782 max=  1782 ms
step: ElectLeader apply            n=200  p50=    31 p99=    58 max=    60 ms
step: LA commit                    n=200  p50=     0 p99=    55 max=    56 ms
step: catch-up + re-grant          n=200  p50=    88 p99=  2241 max=  2801 ms
detection paths: {"new-controller seeding": 67, "session-timeout sample": 44, "suspect report confirmed": 89}
controller iterations: 67, blackhole iterations: 66, targets that led no partition: 0, not resumed: 0
```

The concurrent 500-fault campaign (Task 3, light load) ran during these 200 iterations; load average was about 1-2.

**Observations.**
- `kill -9` of a non-controller leader takes 2.4-2.5 s, almost all of it detection: suspect report → confirmation → `ElectLeader` at about 2.32 s.
- A controller kill adds the meta re-election (p50 1.2 s, max 1.8 s), but the new controller's seeding runs concurrently, so p99 is still 3.5 s.
- A no-RST blackhole is detected only by the session-timeout sample, at about 3.9-4.1 s.
- A blackholed controller is the worst case (p99 5.7 s). There, a new start is acknowledged about 3.0 s after the fault, but the pre-fault durable-sleep run completes only about 2.6 s later. This is the "catch-up + re-grant" step, p99 2.2 s. It appears only when the old leader is alive but cut off, so the likely cause is the run's attempt/lease handover. It stays well inside the 10 s budget and is recorded as an open item for 12-18.
- "LA commit" shows 0 in most samples because the new leader's HW had already passed the LA by the first 50 ms monitor sample after its log led.

## Meta election tick tuning sweep

`OSTRO_FAILOVER_SWEEP=1` ran each setting as a fresh cluster: 60 failover iterations with the same schedule (20 controller faults and 20 to 21 blackholes per setting), then a 10-minute steady state under the same workload with no faults, polling every node's `meta.term` once a second. `target/failover-p99/sweep.txt`:

```
meta election tick tuning sweep (12-17): 60 failover iterations + 600 s loaded steady state per setting
tick   session   p50_ms  p99_ms  max_ms  term_changes  steady_acks
10     3s          2474    5612    5612             0        11578
10     2s          2172    4119    4119             0        11326
6      3s          2475    4982    4982             0        11642
6      2s          1412    3371    3371             0        11654
4      3s          2456    4367    4367             0        11662
4      2s          1411    3162    3162             0        11666

pick (most conservative with p99 < 8 s and zero spurious term changes): meta_election_tick 10, session_timeout 3s (p99 5612 ms)
```

**Chosen defaults: unchanged** (`meta_election_tick = 10`, `session_timeout = "3s"`; the other D-P12-10 values stay as they are too). The rule is: the most conservative setting (largest timeouts) whose p99 is under 8 s, which leaves 2 s of margin, and which had zero spurious term changes. The shipped defaults pass with p99 5.6 s and 0 term changes. No setting showed a term change in its 10-minute loaded steady state on this host, and shorter timeouts give proportionally faster failover (tick 4 / 2 s: p99 3.2 s). If native-Linux runs show headroom, `session_timeout = 2s` would be the next step. That is not taken here, because the defaults already meet SC4 with margin and the rule prefers the conservative setting. `config.rs` was not touched.

Interference note: the two benchmark runs (about 02:37-02:55, CPU heavy) overlapped the failover iterations of row 2 and the start of row 3, and the `process_*` re-runs overlapped row 3. Row 1, which decides the pick, ran without other load: load average about 1. Rows 2-6 are evidence for the trend only.

## Task 3: RF=3 bytes-on-disk benchmark (60 s per cluster)

`target/bench-rf3/report.txt`:

```
RF=3 benchmark (12-17, profile A: trivial-steps n=10, 16 closed-loop clients, 60 s per cluster; failpoint debug server build, WSL2: advisory)
RF=3 (3 processes, min_insync 2): 60.0 s, runs acknowledged 90830, completed 90830, verified on disk 90830 | steps/s acknowledged 15135.7, verified 15135.7 | commit latency p50 10.1 ms p99 20.8 ms | max envelopes/fetch 19 | WAL bytes per node {1: 134781876, 2: 134781876, 3: 134781876}
RF=1 (1 process): 60.0 s, runs acknowledged 200520, completed 200520, verified on disk 200520 | steps/s acknowledged 33417.4, verified 33417.4 | commit latency p50 4.5 ms p99 10.1 ms | max envelopes/fetch 0 | WAL bytes per node {1: 294394425}
RF=3 / RF=1 verified steps/s ratio: 0.45
```

Every acknowledged run completed. After the cluster stopped, its `StartRun` (the `acked_on_disk` oracle) and all 10 `StepCompleted` journal events were in the verified WAL scan of at least 2 of the 3 replicas: 90,830 of 90,830 runs. Batching on user partitions is present (max 19 envelopes in one fetch response; at least 2 is asserted). The numbers come from a debug server build and are reported, not gated (Phase 14 QA-05). The benchmark ran while sweep row 3 was measuring (see the sweep note).

## Task 3: cluster crash-campaign smoke (500 faults)

`target/campaign-p12/report.txt`:

```
cluster crash campaign (12-17): seed 1741, 500 faults, 3 processes, RF 3, min_insync 2, 2038.980204252s
faults by kind: {LeaderKill: 114, FollowerKill: 102, ControllerKill: 98, ProxyPartition: 86, WalFailKill: 100}
acknowledged runs: 54470; batch disk checks: 50 (every 10 faults)
lost acked runs: 0
duplicate journal entries: 0
final checks (replica identity, acked on disk, LA rule, ISR-only elections): pass
```

The workload was 4 closed-loop clients, 100 ms apart, starting 3-step `trivial-steps` runs, with an ack log. Faults were injected one at a time, each followed by recovery and a healthy cluster (meta leader, every partition led, full ISRs). The disk oracle ran every 10 faults (50 checks). At the end: every acked run completed; every run's leader history had strictly increasing seqs and exactly 3 distinct `step_completed`; replica identity, LA rule and ISR-only election audit all held.

## Reproduce on native Linux (human_needed, D15)

```bash
cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
export OSTRO_FAILPOINT_BIN=$PWD/target/failpoints/debug/ostro   # or a --release build of the same
cargo build -p ostro --features failpoints --release --target-dir target/failpoints --locked  # optional: release server
T="cargo test --release -p ostro-repl-conformance --locked"
OSTRO_PROC_ITERS=10 $T --test process_divergence --test process_power_loss --test process_mutants -- --ignored --test-threads=1 --nocapture
OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 $T --test failover_p99 -- --ignored --nocapture --test-threads=1 --exact failover_p99
OSTRO_FAILOVER_SWEEP=1 $T --test failover_p99 -- --ignored --nocapture --test-threads=1 --exact sweep
OSTRO_REPL_BENCH_SECS=60 $T --test bench_rf3 -- --ignored --nocapture --test-threads=1
OSTRO_CAMPAIGN_FAULTS=500 $T --test cluster_campaign -- --ignored --nocapture --test-threads=1
# reports: target/failover-p99/{report,sweep}.txt, target/bench-rf3/report.txt, target/campaign-p12/report.txt
```

## Deviations from Plan

### Auto-fixed issues

1. **[Rule 3] Tier-C cluster formation.** The 12-09 harness did not set `[cluster] advertise`. Since 12-14, node certificates carry `advertise` plus loopback as SANs, so the proxied peers failed the TLS name check, meta never elected, and the 3-process smoke failed. `node_config` now advertises the node's `127.r.r.i`. `process_smoke`'s ignored test now asserts voters [1,2,3] and a green `cluster_formed` instead of the pre-12-14 "each node is its own group".
2. **[Rule 3] `restart` healed links after the ready line.** Since 12-14 a starting node waits for a meta leader, so a node restarted behind cut links never became ready. `restart` now heals before spawning. Added `restart_many`: two nodes that together make the quorum must boot concurrently.
3. **[Rule 1] Tier-C API port changed on every restart.** Long-lived clients (the failover steady workload, the campaign workload) lost every node once each had restarted. The first measured 200-run showed 391 acked versus 24,697 failed workload starts. The first bound port is now pinned in the node's config, and the 200-run and campaign were re-run on a8fa64d.
4. **[Rule 1] Leader hints.** The hint lives in `error.details.leader_addr`. A cut-off old leader hints at itself, which looped clients forever (the first blackhole iterations never finished). `HintClient` reads the details and rotates instead of following a self-referential hint.
5. **[Rule 3] `oracle::disk::acked_on_scans` was quadratic** (one full decode of the log per ack). With 90k-run benchmark and 54k-run campaign ack logs it ran for hours. It now decodes each scan's `StartRun` ids once (`start_run_ids`) and gives the same answer. `oracle_selftest` 10/10.
6. **[Rule 1] bench_rf3 completion check.** A sequential loop with one shared 300 s deadline gave up on completed runs (62k of 89k counted although all were verified on disk). It is now concurrent with a per-run bound.
7. **[Design within the plan] `follower_power_loss` uses `wal::write=return` and a dead second follower, not a pause failpoint.** The first version paused G's fetcher (`fetch::before_append=pause`) and F's WAL (`wal::write=pause`). `fail`'s pause blocks OS threads. In the paused process this blocked tokio workers (and tasks in their LIFO slots), so its metadata Raft answered votes about 400 ms late and lost quorum: no election in 20 s (seen 2 of 4 runs). EIO before the write puts the same bytes on disk as a power loss (nothing F fetched after its restart reaches its files, then `kill -9`) without blocking. A dead G gives the same ISR {L, F} as a slow G.
8. **Files outside `files_modified`** (minimal hunks): `src/scenarios/{mod,runner}.rs`, `src/oracle/{cluster_formed,disk}.rs`, `src/harness/process.rs`, `tests/process_smoke.rs`. These are the scenario-layer generalization the 12-09 summary handed to this plan, plus fixes 1-5. No server, Cargo, proto, format, CI or FAILPOINT_CRATES change; no new dependency; no new env name (the sweep's steady-state length for smoke runs is the value of `OSTRO_FAILOVER_SWEEP` when greater than 1).
9. **Build profile.** The plan's verify builds into `target/failpoints` and runs the tests with `--release`. Per the executor brief, the failpoint binary was built into slot-4 (`OSTRO_FAILPOINT_BIN`) and every measurement ran a debug test harness against the debug failpoint server. Failover numbers are dominated by timeouts; benchmark numbers are lower than a release build would give.

## Open items (for 12-18)

- CI macOS `stale_epoch` "answered SnapshotRequired, expected None" (see the plan gate): untouched by 12-17, needs a look.

- Blackholed-controller "catch-up + re-grant": the pre-fault run completes about 2.6 s after the new leader acks new starts (worst iterations 5.2-5.7 s total). Inside budget; worth a look at the attempt-lease handover from an alive-but-cut-off old leader.
- Detection of a `kill -9` is about 2.3 s even on the suspect-report path. The `confirm_window` (session/4 = 750 ms) would allow faster; check whether the suspect report reaches the controller before its regular samples do.
- The native-Linux p99 / benchmark run (human_needed).
- The 12-18 gate campaign (10,000 faults): at about 4 s per fault on WSL2 with a debug server, 10k faults is about 11 h, and the ack log grows by about 100 runs per fault, so budget the time box.

## Plan gate

I ran `scripts/ci-local.sh --no-musl` in a detached worktree at 454b69b (main, with all 12-17 code commits up to 587714e), slot-4, after the measurements had finished (load about 1-5):

```
cargo fmt --check                    FAIL  3s     <- harness/process.rs:565 (a8fa64d), fixed in 30247b8
cargo clippy -D warnings             PASS  79s
cargo build (warning-free)           PASS  69s
sdk workspace / guests / order-saga  PASS
external PG / MySQL / S3             SKIP
cargo test                           PASS  3250s
polyglot toolchains, python/ts sdk, polyglot conformance, clippy polyglot, clients py/ts/go   PASS
ostro-build polyglot                 PASS  515s
semantics sleepers (serial)          PASS  66s
failpoint list sync (ci.yml)         PASS
failpoint suites                     PASS  3952s
server campaign smoke (20 kills)     PASS  1062s
phase 7 failpoint legs, cargo deny, check-vendored, deny fixtures, no chronik, one log path, workflow validation   PASS
musl legs                            SKIP (--no-musl)
```

The only red was formatting. The coordinator also caught it on CI; it is fixed in 30247b8, and `cargo fmt --all -- --check` is clean at that commit, which touches whitespace only. The ignored measurement tests do not run in the gate. Without a failpoint binary in the gate tree, the tier-C non-ignored test prints SKIP as designed.

**Seen on CI macOS (run 45309300428), reported by the coordinator:** tier-B `stale_epoch` failed with "answered SnapshotRequired, expected None". That is the second `fetch_expect(.., FetchError::None)` in `fetch_with_stale_epoch_fenced` or `fetch_with_future_epoch_unknown`. 12-17 did not touch that path. `stale_epoch.rs`, its oracle and the fetch handler are unchanged; the scenario-layer change only made the `Scenario` type generic, and there was no tick change. Handed to 12-18 as a possible real race: the raw fetch uses the follower's offline-scanned durable end as `fetch_offset`, and the leader answers SNAPSHOT_REQUIRED when that is below its log start. That could follow a snapshot or trim after 12-16, or come from a torn read on macOS. The oracle is not weakened.


## Self-Check: PASSED

- Files: tests/{process_divergence,process_power_loss,process_mutants,failover_p99,bench_rf3,cluster_campaign}.rs and src/scenarios/process.rs are present at main.
- Commits: f5c281b, de6622e, dfa8ddb, a8fa64d, 587714e and 30247b8 are on main.
- Reports read back as quoted: failover 200 (report.txt), sweep.txt, bench-rf3, campaign-p12.
