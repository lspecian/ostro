---
phase: 14-multi-tenancy-security-1-0-release
plan: 22
subsystem: ostro-jepsen (QA-05 Jepsen-style runs on the 1.0 configuration)
tags: [p14, jepsen, qa-05, nemesis, auth, audit, quorum-loss, rolling-restart]
requires:
  - phase: 14-09
    provides: "RunSpec / run() / Nemesis trait / checkers (lost, duplicate, fencing) / reports"
  - phase: 14-10
    provides: "ClockNemesis (bump, strobe, drift), DiskNemesis (wal::fsync, state::commit, state::persist stalls), failpoints test endpoints"
  - phase: 14-03 / 14-17
    provides: "token mode, OSTRO_BOOTSTRAP_TOKEN bootstrap, POST /v1/auth/tokens (tenant `_` accepted since 36d3956)"
  - phase: 14-18
    provides: "guard-recorded audit (run.cancel, token.create), audit::verify::verify_dir, segment::Journal::open (torn-tail repair)"
provides:
  - "crates/ostro-jepsen/tests/jepsen_smoke.rs (#[ignore]): all fault classes, auth + audit on, 180 s default, seed 1401"
  - "crates/ostro-jepsen/tests/jepsen_gate.rs (#[ignore]): OSTRO_JEPSEN_SEEDS (1402,1403) x OSTRO_JEPSEN_SECS (1800) + pinned quorum-loss window + rolling restart; target/jepsen/gate-summary.txt"
  - "crates/ostro-jepsen/tests/support/p14.rs: 1.0 spec (token bootstrap via proc_options.env), weighted all-classes menu, operator (audited admin actions), QuorumLossPartition, RollingRestart, post-run audit verification"
  - "scripts/jepsen-run.sh {smoke|gate|long} [--seed S] [--secs N] [--hosts h1,h2,h3]"
  - "run.rs: RunSpec::pinned (fixed-time faults) and RunSpec::keep_dirs (harness hook)"
affects: [14-23, 14-24, 14-27]
tech-stack:
  added: []
  patterns:
    - "fixed-time faults go through RunSpec::pinned; with none pinned the seeded schedule is byte-identical to before"
    - "the audit journal is verified offline after the run: each node's dir opened like a restart would open it (torn tail repaired), then verify_dir, then a scan of the records"
key-files:
  created:
    - crates/ostro-jepsen/tests/jepsen_smoke.rs
    - crates/ostro-jepsen/tests/jepsen_gate.rs
    - crates/ostro-jepsen/tests/support/p14.rs
    - scripts/jepsen-run.sh
  modified:
    - crates/ostro-jepsen/src/run.rs (deviation, see below)
key-decisions:
  - "Harness token = the bootstrap system-admin bearer (OSTRO_BOOTSTRAP_TOKEN on every node through proc_options.env; OSTRO_JEPSEN_TOKEN supplies it, else a fresh ostro_otk_ bearer). run() uses one token for deploy, workload and observation, and a trigger+read tenant token cannot deploy the module, so the tenant token is exercised by the operator instead: it creates a tenant `_` token (trigger + read) every round and starts a run with it"
  - "Operator round every 30 s (the plan said per minute): token.create through a random node, a tenant-token start, then an admin run.cancel through a random node. Each request is sent once, so an acknowledged action must appear in the nodes' journals exactly once and an unacknowledged one at most once. Acknowledged workload cancels (which are retried) need at least one ok run.cancel record"
  - "The quorum-loss window is a partition that isolates every node (nobody has a majority) for 20 s. The plan says 'two nodes partitioned from the leader', but with 3 nodes that still leaves a majority, so it would not lose quorum. The kill-based NemesisKind::QuorumLoss is not used"
  - "Rolling restart = SIGTERM (controlled shutdown), restart, wait healthy, next node; pinned at 2/3 of the fault phase; quorum loss pinned at 1/3"
  - "Menu weighted per class (partition modes x3, kill targets x3, clock faults x4, disk stall x12) so each of the four classes is picked about equally often"
requirements-completed: []
duration: ~1h15m
completed: 2026-11-27
---

# Phase 14 Plan 22: Jepsen runs on the 1.0 configuration Summary

**The QA-05 Jepsen harness now runs with auth and audit on. All three nodes start in token mode with a bootstrapped admin bearer, and every node writes its audit journal. The runs use all four fault classes (partitions incl. leader isolation and one-way, kill -9 of leader/follower/meta leader, clock bump/strobe/±5% drift, disk-fsync stalls), plus an operator that keeps creating tenant tokens and cancelling runs through random nodes. After each run, every node's audit journal is verified. The 180 s smoke (seed 1401) passed locally and again as a release run on dell-4. A local 300 s gate run (seed 1404, with a pinned 20 s quorum-loss window and a rolling restart) both PASSED. They found 0 lost acknowledged runs, 0 duplicates and no fencing violation, every journal verified, and every acknowledged operator action was recorded exactly once.**

QA-05 is not ticked here (14-27's, D-P14-35).

## Gate

**Gate incomplete: the consolidated gate and the long Jepsen runs are pending on dell-4.** The cluster was withdrawn, and the plan-level `ostro-k8s-ci --parallel` could not run. What is unverified:

- **The 2 × 30 min gate** (`jepsen_gate`, seeds 1402 and 1403, 1800 s each). Locally only one 300 s run (seed 1404) was done; the plan's own Task 2 verify uses that length and seed.
- **The gate in release.** The local gate run used a debug test harness and the debug failpoint binary (`target/slot-5/debug/ostro`). The smoke has since run in release on dell-4 through `scripts/jepsen-run.sh smoke`, which is exactly the plan's Task 1 command. The gate has not yet run in release.
- **The 4 h `long` run** and the 3-machine procedure (`--hosts`). Both are human_needed by design.
- **Every ci-local step** (consolidated gate). This plan changes only ostro-jepsen test code, the `run.rs` hook and a script. No crate in the `ostro` binary changed, so the musl size gate is unaffected.
- **No run has produced a torn audit tail yet.** `torn_bytes_repaired = 0` and `audit_recovered = 0` in every run. The repair path (`Journal::open` before `verify_dir`) runs, but the short runs never cut a record in half. The long runs, with more kills, may.

### Commands for dell-4 (orchestrator)

```bash
# the 2 x 30 min gate (writes target/jepsen/gate-summary.txt in the container; the test prints it at the end)
/home/lspecian/.local/bin/ostro-dell4-ci --cmd 'bash scripts/jepsen-run.sh gate' <main-sha>
# the plan's smoke, release (180 s, seed 1401)
/home/lspecian/.local/bin/ostro-dell4-ci --cmd 'bash scripts/jepsen-run.sh smoke' <main-sha>
# optional multi-hour run (4 h, seed 1405); data dirs on NVMe, not /dev/shm
/home/lspecian/.local/bin/ostro-dell4-ci --cmd 'mkdir -p /home/ci/jepsen-proc && OSTRO_PROC_ROOT=/home/ci/jepsen-proc bash scripts/jepsen-run.sh long' <main-sha>
# the consolidated ci-local gate
/home/lspecian/.local/bin/ostro-dell4-ci <main-sha>
```

`jepsen-run.sh` builds `target/failpoints/debug/ostro` (the harness's default binary), then prints the binary's sha256, `HEAD`, and load before and after (`uptime`, `nproc`, free memory). After that it runs `cargo test --release`. Without other flags: smoke = seed 1401 for 180 s; gate = seeds 1402,1403 for 1800 s each; long = seed 1405 for 14,400 s. If a run fails, it keeps its cluster directory and prints the path. Its report is in `target/jepsen/<seed>/`.

## Results (local, debug, nice 10, shared 12-core machine)

The failpoint binary was built at base 9e516df, debug, sha256 `52c0f6fd64bad9917734f5c20449ea71d772ad1f2f546f195dd8693da1e863f9`. The `ostro` crates did not change between that base and the commits here.

### Smoke: `OSTRO_JEPSEN_SECS=180 OSTRO_JEPSEN_SEED=1401` (test tree = 22cc938)

Load: 2.55 at the start (12 cores, 40.9 GB available), 7.97 at the end (other executors were building). The run took 222.8 s.

```
ostro-jepsen run: seed 1401 | 180 s of faults | 3 nodes, 4 partitions | PASS
ops: cancel/ok 38, result/fail 41, result/info 14, result/ok 4997, signal/fail 15, signal/info 7,
     signal/ok 2774, start/fail 9, start/info 12, start/ok 1524
faults: clock:bump 2, clock:drift 1, clock:strobe 2, disk:stall 2, kill:follower 1,
        partition:bridge 2, partition:one_way 1   (+ operator:start 1)
faults by class: clock 5, disk 2, kill 1, partition 3
lost: PASS (acked_signals=2774, acked_starts=1524, cancelled=38, completed=1486, failed_starts=9,
            info_starts=12, keys_in_log=1530, runs_checked=1524)
duplicate: PASS (journal_entries=72927, journals_checked=1524, records_compared=77304)
fencing: PASS (commands_reapplied=77469, entries_compared=51646, epochs=7, leader_announcements=21,
               tokens_checked=4336)
audit: PASS (actions:module.upload=1, actions:run.cancel=44, actions:segment_open=4,
             actions:token.create=6, operator_acked=12, records=55, segments=4,
             torn_bytes_repaired=0, workload_cancels=38)
  node 1: 15 records in 1 segments verified (seq 1..=15)
  node 2: 17 records in 1 segments verified (seq 1..=17)
  node 3: 23 records in 2 segments verified (seq 1..=23)
operator: 12 actions (12 acknowledged), tenant-token starts {"http 201": 6}
max unavailability: 2482 ms
test result: ok. 1 passed (222.82 s)
```

A 60 s pre-run with the same seed also passed: 562 acked runs, faults clock 1 and disk 2, audit 24 records, 4 operator actions all acknowledged.

### Smoke, release, on dell-4 (lane 14-22, commit 07a84ee = main with 6263eb1/22cc938/57787bc; `ostro-dell4-ci --lane 14-22 --cmd 'cargo clippy -p ostro-jepsen --all-targets --locked -- -D warnings; bash scripts/jepsen-run.sh smoke'`)

- Clippy `-D warnings` was clean.
- Failpoint binary `target/failpoints/debug/ostro`, sha256 `b958d54f8368c31ef5fe73dc8ce33e598b506bb4e497834b6b356f9d3e7d8b4a`. Test harness in release.
- Load: about 32 at both start and end (24 cpus, other lanes running), 80 GB free. The run took 273.2 s; the whole job, including cold builds, took 1,686 s.
- Log: `target/dell4-ci-logs/20261127-130225-14-22-07a84ee7-cmd.log`.

```
ostro-jepsen run: seed 1401 | 180 s of faults | 3 nodes, 4 partitions | PASS
ops: cancel/ok 38, start/ok 1352 (fail 13, info 16), signal/ok 2466, result/ok 4613
faults: clock:bump 2, clock:drift 1, clock:strobe 2, disk:stall 2, kill:follower 1,
        partition:bridge 2, partition:one_way 1   (+ operator:start 1)
lost: PASS (acked_starts=1352, cancelled=38, completed=1314, runs_checked=1352)
duplicate: PASS (journal_entries=63927, journals_checked=1352, records_compared=69429)
fencing: PASS (commands_reapplied=69741, entries_compared=46494, epochs=7, tokens_checked=3856)
audit: PASS (run.cancel=44, token.create=6, operator_acked=12, records=55, segments=4,
             torn_bytes_repaired=0, workload_cancels=38); nodes 19 / 17 / 19 records verified
operator: 12 actions (12 acknowledged), tenant-token starts {"http 201": 6}
max unavailability: 4162 ms
test result: ok. 1 passed (273.18 s)
```

### Gate (plan's Task 2 verify): `OSTRO_JEPSEN_SECS=300 OSTRO_JEPSEN_SEEDS=1404` (tree = 57787bc)

Load: 5.71 at the end. The run took 353.4 s. `gate-summary.txt`:

```
seed 1404 | 300 s of faults | PASS
ops: cancel/info 3, cancel/ok 72, result/fail 98, result/info 487, result/ok 6816, signal/fail 52,
     signal/info 139, signal/ok 4066, start/fail 40, start/info 241, start/ok 2263
acked runs: 2263
faults: clock:bump 1, clock:drift 1, clock:strobe 1, disk:stall 6, kill:any 1, kill:follower 1,
        kill:leader 1, partition:bridge 5, partition:leader_isolation 2,
        partition:majority_minority 1, partition:one_way 2, partition:quorum_loss 1,
        restart:rolling 1   (+ operator:start 1)
faults by class: clock 3, disk 6, kill 3, partition 11, restart 1
lost: PASS (acked_signals=4066, acked_starts=2263, cancelled=72, completed=2191, failed_starts=40,
            info_starts=241, keys_in_log=2273, runs_checked=2263)
duplicate: PASS (journal_entries=107670, journals_checked=2263, records_compared=114933)
fencing: PASS (commands_reapplied=115284, entries_compared=76856, epochs=27,
               leader_announcements=81, tokens_checked=6401)
audit: PASS (actions:module.upload=1, actions:run.cancel=84, actions:segment_open=9,
             actions:token.create=10, operator_acked=19, operator_unacked=1, records=104,
             segments=9, torn_bytes_repaired=0, workload_cancels=72)
  node 1: 36 records in 3 segments verified (seq 1..=36)
  node 2: 37 records in 2 segments verified (seq 1..=37)
  node 3: 31 records in 4 segments verified (seq 1..=31)
operator: 20 actions (19 acknowledged), tenant-token starts {"error": 1, "http 201": 10}
max unavailability window: 21629 ms (from 111401 ms on the history clock)
test result: ok. 1 passed (353.39 s)
```

The two pinned faults, as recorded in `nemesis.jsonl`:

- **Quorum loss:** every node of [1, 2, 3] was isolated from 111,559 to 131,560 ms. The longest unavailability window, 21.6 s from 111,401 ms, is this window: no acknowledged write was lost, writes just stopped.
- **Rolling restart:** 211,559 to 216,585 ms. Nodes 1, 2 and 3 each exited cleanly on SIGTERM and were healthy again 17–119 ms after their restart.

The single unacknowledged operator action (a transport error) has at most one record, as required.

## Auth bootstrap on the harness cluster (p14-notes hand-off)

- Every node gets `OSTRO_AUTH__MODE=token`, `OSTRO_BOOTSTRAP_TOKEN=<bearer>` and `OSTRO_AUDIT__ENABLED=true` through `RunSpec::proc_options.env`. The harness's own `[auth] mode = "none"` default (14-03) is overridden by these env settings.
- The bearer is `OSTRO_JEPSEN_TOKEN` when that is set (it must parse as `ostro_otk_<16>_<52>`). Otherwise it is a fresh one from `ostro_server::p14::tokens::generate_os()`.
- The bearer goes into `RunSpec::auth`, so deploy, the workload, observation and the 14-10 test endpoints (clock and disk nemeses) all carry it.

## Deviations from Plan

1. **[Rule 3 - blocking] `crates/ostro-jepsen/src/run.rs`** (not in files_modified; 14-09's file, done; harness code, not a checker or oracle). Commit 6263eb1 adds two fields to `RunSpec`:
   - `pinned`: faults at fixed times. Without them, "one quorum-loss window and one rolling restart per run" cannot be guaranteed, because slots are drawn uniformly from the menu.
   - `keep_dirs`: a passing run deleted the cluster directory, so "after stop: verify_dir on each node's `<data>/audit`" was impossible.

   Both default to the old behaviour. With nothing pinned, the schedule bytes are unchanged (checker_selftest 10/10; harness_smoke's same-seed check uses the unchanged path). One side effect: with `keep_dirs`, a passing report's notes include `cluster directory kept: <path>`. The test deletes that directory after the audit verification passes, so the path in `report.json` no longer exists. **Orchestrator: please confirm this hook (as was done for 14-10's server.rs line).**
2. **`crates/ostro-jepsen/tests/support/p14.rs`** (a fourth file). It holds the code shared by the two tests and is included with `#[path]`. Cargo does not build it as a separate test target.
3. **Workload token:** the workload uses the bootstrap system admin, not a tenant trigger+read token (see key decisions). The tenant `_` token is still exercised: it is created through the admin API every round and starts a run. No 403 was seen; 16 of 17 tenant starts were 201 and one was a transport error.
4. **Operator cadence:** every 30 s instead of every minute, for more samples. Its runs use the key prefix `op<seed>-` (the lost checker counts them in `keys_in_log` and checks only the workload's acknowledged runs, the same as 14-10's RYW sessions).
5. **Quorum-loss shape:** all three nodes isolated, not "two nodes from the leader" (see key decisions).
6. **Local runs in debug** against the debug failpoint binary (see Gate).

## Product anomalies

None found (no STOP).

## Observations for later plans

- **14-27 (CI):** the smoke is `#[ignore]` and long-running. If it goes into CI, use `bash scripts/jepsen-run.sh smoke` as a ci-local step after the failpoint build. No workflow hunk is proposed here.
- **14-23:** `support/p14.rs::spec` is the 1.0 cluster configuration for bench/soak runs, if they want auth and audit on.
- **14-24:** two points for the threat model:
  - Token verification under partitions was exercised only from the client side: 0 × 401 in tenant starts.
  - The audit journals were checked for exactly-once at the ingress node, under partitions, kills and disk stalls. The audit fsync itself was never stalled: the disk nemesis sites are the WAL and the state store, not `audit::before_fsync`.

## Self-Check

PASSED:
- All four created files exist, and `scripts/jepsen-run.sh` is mode 100755.
- Release smoke plus clippy on dell-4 lane 14-22: PASS (exit 0).
- Commits 6263eb1, 22cc938 and 57787bc are on main.
- `jepsen_gate.rs` contains `OSTRO_JEPSEN_SECS`, `jepsen-run.sh` contains `OSTRO_JEPSEN_SEED`, and the tests use `RunSpec`.
- `cargo clippy -p ostro-jepsen --all-targets -- -D warnings` is clean, rustfmt is clean, and the non-ignored ostro-jepsen tests pass (checker_selftest 10, skeleton 1).
- `jepsen-run.sh` was dry-run with a stub cargo for the smoke, gate (`--seed`/`--secs`) and long modes, plus `--hosts` and a bad mode (exit 2).
- No `/dev/shm` dirs are left from these runs.
