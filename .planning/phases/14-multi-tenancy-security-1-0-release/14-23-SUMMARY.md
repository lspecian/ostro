---
phase: 14-multi-tenancy-security-1-0-release
plan: 23
subsystem: ostro-jepsen (QA-04 bytes-on-disk benchmarks, QA-05 soak driver)
tags: [p14, bench, soak, qa-04, qa-05, d16, bytes-on-disk, pre-registration]
requires:
  - phase: 14-09
    provides: "ostro-jepsen crate, report::workspace_root, workload::pause"
  - phase: 14-22
    provides: "the 1.0 harness configuration pattern (OSTRO_AUTH__MODE=token, OSTRO_BOOTSTRAP_TOKEN, OSTRO_AUDIT__ENABLED via ProcOptions.env)"
  - phase: 14-06 / 14-16 / 14-17 / 14-20
    provides: "tenant quotas, tenant-scoped names, tenant + token admin API, ingress admission"
  - phase: 12-17
    provides: "offline WAL scan method (bench_rf3 in ostro-repl-conformance), acked_on_disk oracle"
provides:
  - "ostro_jepsen::bench: BenchSpec (single / rf3), Rig (1.0 cluster: token auth, tenant 'bench' with generous quotas + tenant token, audit on), Api (bearer, leader hints), verify_on_disk, BenchReport, OSTRO_BENCH_ENFORCE"
  - "tests/bench_single.rs, tests/bench_rf3.rs (#[ignore], release): target/bench/{single,rf3}.json"
  - "docs/soak.md: pre-registered criteria C1-C9, 7-day command, submission (+ dated amendment 2026-11-27)"
  - "ostro_jepsen::soak + bin ostro-soak: open-loop driver, samples.jsonl, verdict.json, exit 0/1/2"
  - "tests/soak_smoke.rs (#[ignore], release): target/soak/"
  - "docs/benchmarks.md: Phase 14 (1.0 configuration) section"
affects: [14-24, 14-25, 14-26, 14-27]
tech-stack:
  added: []
  patterns:
    - "a step counts only when found on disk: single node kill -9 + offline WAL scan + restart read-back; RF=3 offline scan of every replica, >= min_insync"
    - "thresholds behind OSTRO_BENCH_ENFORCE=1; correctness always asserted"
    - "soak criteria pre-registered in their own commit; later changes only as dated amendments, criteria unchanged"
key-files:
  created:
    - crates/ostro-jepsen/tests/bench_single.rs
    - crates/ostro-jepsen/tests/bench_rf3.rs
    - crates/ostro-jepsen/tests/soak_smoke.rs
    - docs/soak.md
  modified:
    - crates/ostro-jepsen/src/bench.rs
    - crates/ostro-jepsen/src/soak.rs
    - crates/ostro-jepsen/src/bin/ostro-soak.rs
    - crates/ostro-jepsen/src/lib.rs (clap anchor removed, p14-notes rule)
    - docs/benchmarks.md (Phase 14 section appended)
key-decisions:
  - "Commit latency = client-side start request -> start ack (no Phase 12 commit-latency histogram exists, 14-01); server ostro_apply_batch_seconds p99 reported alongside"
  - "Load runs with a tenant token (deploy, trigger, read, operate) of tenant 'bench' with generous quotas; the tenant deploys its own modules"
  - "Soak step-latency SLO is measured on the whole 10-step profile-A run (upper bound of one step)"
  - "Soak smokes (< 24 h) judge C1 as RSS growth <= 10% over the post-warm-up window; >= 24 h runs use the 2%/24 h slope (pre-registered)"
  - "Soak in-flight cap min(4096, soft nofile / 4) and runs with ulimit -n 65536 (dated amendment after smoke 2; criteria unchanged)"
requirements-completed: []
duration: ~4h
completed: 2026-11-27
---

# Phase 14 Plan 23: Bytes-on-disk benchmarks and the soak driver Summary

**What is in place:**

- **Two benchmarks of the 1.0 configuration.** `bench_single` and `bench_rf3` run with token
  auth, a tenant token with quotas on the admission path, and the audit journal on. They
  count only steps found on disk:
  - single node: `kill -9`, an offline WAL scan, then a restart and a read-back of every
    acknowledged run through the API;
  - RF=3: an offline scan of every replica, requiring at least `min_insync` copies.

  They report steps/s, bytes per step, and commit and client-ack p50/p99. Thresholds are
  asserted only with `OSTRO_BENCH_ENFORCE=1`.
- **The soak.** Its criteria were pre-registered in `docs/soak.md` and committed before the
  driver code and before any soak run. The `ostro-soak` driver writes `samples.jsonl` and
  `verdict.json`.

**Results:**

- In every bench and soak run, acked == on disk held. The soak's correctness criteria also
  held: 0 lost, 0 duplicates, the audit chain verified at every sample, and 0 restarts.
- **The soak smoke has not passed.** The third smoke (300 s, 0.7 × 41,472 steps/s = 2,903
  runs/s) failed C1 (RSS growth) and C2 (fds).
- An earlier smoke, at the default soft fd limit of 1024, exposed a product robustness
  problem. The node hit EMFILE, its metadata raft driver stopped permanently, and the node
  stayed up but served nothing. Details are in the findings below.

QA-04 and QA-05 are not ticked here (14-27, D-P14-35).

## Gate

**Gate incomplete: the consolidated gate and the final measurements are pending on a quiet
dell-4.** dell-4 was shared with a full CI run in lane gate and two crash campaigns, with
load averages of 20 to 63. None of the numbers below are final. The following are
unverified:

- **Every performance number.** The benchmarks so far are tooling-validation runs: two
  short runs (5 s + 20 s) and two full-length single-node runs used as the soak's rated
  load. The thresholds (single ≥ 10,000 steps/s; RF=3 ≥ 5,000 steps/s and commit p99 < 20 ms)
  have **not** been judged. Not one run was on a quiet machine, and none used
  `OSTRO_BENCH_ENFORCE=1`.
- **The plan's Task 1 verify at full length** (`bench_rf3` with a 60 s window). Only a
  20 s window ran at RF=3.
- **A passing soak smoke** (the must-have "a soak smoke at 70% passes"): **not met**. The
  smoke failed C1 and C2 (see Findings). The 30 min gate smoke has not run.
- **The plan-level gate** (consolidated ci-local on dell-4). Only
  `cargo clippy -p ostro-jepsen --all-targets -- -D warnings` and the 11 `ostro-jepsen`
  unit tests ran, both clean on dell-4. This plan changes only `ostro-jepsen` (test
  harness) and docs: no crate in the `ostro` binary changed, so the musl size gate is
  unaffected.
- **The 7-day soak and the native NVMe / LAN thresholds:** human_needed by design.

### Commands for the quiet dell-4 (orchestrator)

```bash
# benchmarks (writes target/bench/{single,rf3}.json in the lane; the test prints the table)
/home/lspecian/.local/bin/ostro-dell4-ci --lane bench --cmd 'set -e; cargo build -p ostro --release --locked; cargo build -p ostro --features failpoints --target-dir target/failpoints --locked; ulimit -n 65536; for t in bench_single bench_rf3; do cargo test --release -p ostro-jepsen --locked --test $t -- --ignored --nocapture --test-threads=1; done; cat target/bench/single.json target/bench/rf3.json' <main-sha>
# the same with thresholds enforced (gate box)
/home/lspecian/.local/bin/ostro-dell4-ci --lane bench --cmd 'ulimit -n 65536; OSTRO_BENCH_ENFORCE=1 cargo test --release -p ostro-jepsen --locked --test bench_single -- --ignored --nocapture --test-threads=1; OSTRO_BENCH_ENFORCE=1 cargo test --release -p ostro-jepsen --locked --test bench_rf3 -- --ignored --nocapture --test-threads=1' <main-sha>
# soak smoke, 30 min (the gate), rated measured first (auto); never concurrently with the benches
/home/lspecian/.local/bin/ostro-dell4-ci --lane bench --cmd 'ulimit -n 65536; OSTRO_SOAK_SECS=1800 cargo test --release -p ostro-jepsen --locked --test soak_smoke -- --ignored --nocapture --test-threads=1; cat target/soak/verdict.json' <main-sha>
# 7-day soak (human_needed; NVMe proc root, rated from bench_rf3 on the same box)
ulimit -n 65536; mkdir -p /srv/ostro-soak/proc; OSTRO_PROC_ROOT=/srv/ostro-soak/proc target/release/ostro-soak --nodes 3 --duration 168h --load 0.7 --rated <rf3 steps_per_s> --interval 1h --out /srv/ostro-soak/out
```

`ostro-soak` is built with `cargo build -p ostro-jepsen --release --locked --bin ostro-soak`.
Cold release builds on the loaded dell-4 took 26 min (`ostro` with fat LTO) + 6 min
(failpoints) + 12 min (the `ostro-jepsen` test binaries).

## Results (dell-4 lane 14-23, loaded; advisory)

Release binary sha256 `899957cafb0214bc624a41b203c13dd95228b2b96d335596c8b82a149d53e8d5`
(HEAD 84021e2; the `ostro` crates have not changed in later commits of this plan). 24 cpus
visible, 80 GiB, kernel 6.8.0-151, `/dev/shm` proc root.

### Benchmarks: tooling validation (`OSTRO_BENCH_SECS=20 OSTRO_BENCH_WARMUP_SECS=5`, 64 clients)

| bench | load avg start → end | acked runs | verified on disk | steps/s | commit p50 / p99 | client ack p50 / p99 | apply batch p99 | bytes/step |
|---|---|---|---|---|---|---|---|---|
| single (RF=1, P=16) | 34 → 58 | 81,585 | 81,585 (+ 81,585 read back after restart) | 29,421 | 7.50 / 49.30 ms | 17.13 / 73.90 ms | 5 ms | wal 147.7 + state 98.1 = 245.8 (audit 0, blobs 0) |
| RF=3 loopback (P=4, min_insync 2) | 59 → 63 | 11,091 | 11,091 on ≥ 2 replicas | 4,358.5 | 67.41 / 180.71 ms | 138.31 / 313.53 ms | 10 ms | wal 442.9 (three WALs) |

There were 0 trimmed scans and no violations. Both tests passed (single node in 131.9 s,
RF=3 in 30.1 s). The two full-length single-node runs (10 s + 60 s), used as the soak's
rated load:

| run | load avg start | acked runs | verified | steps/s |
|---|---|---|---|---|
| soak smoke 2 rated | 23 | — | all | 42,002.0 |
| soak smoke 3 rated | 21 | — | all | 41,471.7 |

A smoke-1 rated run at load 63 measured 9,247.5 steps/s: 62,446 runs, all verified and
read back, commit p99 143.5 ms. The machine load clearly dominates these numbers.

### Soak smokes (one node, interval 30 s, warm-up 60 s, `--rated auto`)

**Smoke 3** (the one that counts; after the amendment; `ulimit -n 65536`; 300 s at
0.7 × 41,471.7 = 2,903.0 runs/s): **FAIL**.

```
C1 RSS                        FAIL | node 1: median 2260464 kB, growth over the window 59.32% | bound <= 10% (run < 24 h)
C2 open fds                   FAIL | node 1: median 385, range 263..5270, band ±64
C3 disk bounded               PASS | WAL floor n/a (< 2 h); snapshots max 0; audit dir max 12711 B
C4 latency SLO                PASS | p99 start-ack 47.6 ms, p99 step (whole run) 135.7 ms over 696,760 runs; worst interval start-ack p99 426.0 ms
C5 restarts                   PASS | 0
C6 audit chain                PASS | verified at all 10 samples; last seq 28
C7 lost acked runs            PASS | 0 lost
C8 duplicate journal entries  PASS | 2000 runs read back: 0 duplicates / wrong step count, 0 unreadable
C9 load sustained             PASS | 2903.1 of 2903.0 acked starts/s (100.0%), shed 0
notes: fd limit 65536 -> in-flight cap 4096; cron schedule soak-tick created and present at the end
```

Samples (t s: RSS / RssAnon / RssFile kB, node fds, acked starts per 30 s, in flight,
start-ack p99 ms, run p99 ms, load avg):

```
 30 1533284 1489844 43392  252 87167 21  24.4  39.3  48.21
 60 1991852 1948352 43392  263 87100 18  22.2  35.7  40.50
 90 2069040 2025612 43392  284 87066 22  26.9  47.6  38.68
120 2178936 2135484 43392  385 87070 19  26.9  47.6  44.83
150 2093292 2049756 43392  360 87143 31  26.9  43.2  41.99
180 2260464 2216976 43392  375 87119 58  24.4  39.3  40.12
210 2987228 2943724 43392 5169 87087 51 426.0 686.1  42.73
240 3110860 3067408 43392 5227 87053  5  32.5  57.6  50.22
270 3015952 2972448 43392 5270 87087 50  52.3 198.7  60.44
300 3106516 3063076 43392 5244 87023  0  32.5  57.6  58.92
```

**Smoke 1** (180 s, 0.7 × 9,247.5, fd limit 1024): FAIL on C1 only (+41.35%, 1.02 → 1.73 GB).
Everything else passed: 0 lost, audit OK, load 100%. Cron was not created (403
`missing_scope admin`).

**Smoke 2** (300 s, 0.7 × 42,002 = 2,940 runs/s, fd limit 1024): FAIL on C2, C4, C6, C7, C8
and C9. About 60 s in, a latency spike piled up in-flight runs (cap 4,096 then), and the
node ran out of fds (finding 1). The driver process hit EMFILE too, so its samples went
blank. 13 acknowledged runs were not terminal 60 s after the stop: the node had stopped
serving them. They are not shown to be lost from disk.

## Findings (product; need an orchestrator decision)

1. **A node that hits EMFILE stops its metadata raft driver for good and keeps running
   without serving anything (robustness, liveness).** Smoke 2's node log:
   `controller proposal failed; ... error=metadata proposal failed: raft driver stopped: raft storage: Too many open files (os error 24)`,
   repeated for every partition. Every API read then answered 503 `no metadata lease (meta
   contact lost)`. The process did not exit, so nothing restarted it (C5 saw no restart).
   The trigger was the soft `ulimit -n` of 1024 plus about 1,000 client connections.
   Suggested owner: 14-26 (hardening).
   - Either exit the process when the raft driver stops (so a supervisor restarts it), or
     treat storage EMFILE as retryable.
   - Cap concurrent API connections below the fd limit, or refuse new connections early.
   - Document `LimitNOFILE` in the systemd unit and Helm chart (14-25 / 14-13).

   No durability loss was shown, but there was no restart test after it either.
2. **Anonymous memory grows under sustained load (C1).** RssAnon rose from 1.49 GB to
   3.06 GB in 270 s at 2,900 runs/s, with RssFile flat at 43 MB. Smoke 1 showed the same
   at a quarter of the rate: +0.7 GB in 150 s. This may be caches or memtables filling up
   to a bound (5 min is too short to tell), or a leak. It needs a longer run (≥ 1 h) with
   RssAnon sampled, and a heap profile if it does not level off. Suggested owner: a fixer,
   or 14-26.
3. **The node's fd count stays at about 5,200 after a latency spike (C2).** During the
   spike at t ≈ 200 s, in-flight runs grew, and the driver's HTTP pool opened about 4,900
   connections. They stayed open (kept alive and reused) once in-flight dropped back to
   0–50. That is largely a driver artifact. The harness's `ApiClient` reqwest client cannot
   be configured from ostro-jepsen without adding `reqwest` to its manifest (a Cargo edit,
   14-01 only). **Decision needed:** either allow `reqwest` in `crates/ostro-jepsen/Cargo.toml`
   for a bounded pool (`pool_max_idle_per_host`), or lower the soak's in-flight cap (that
   is a dated amendment). The server keeping those connections is correct. It does show
   that the server has no connection limit (see finding 1).
4. **Tenant schedules cannot be created through the API.** `POST /v1/schedules` needs the
   `admin` scope (a tenant token with deploy/trigger/read/operate gets 403
   `missing_scope admin`). A system admin cannot give a schedule a tenant-qualified name
   either (400: names allow only `[A-Za-z0-9_.-]`). The soak's cron slice therefore uses a
   plain-named schedule (created by the admin) that starts `bench/trivial-steps`. Possibly
   intended in 14-15/14-16; if not, it is an authz or naming gap for 14-26.

## Deviations from Plan

1. **[Rule 3] `crates/ostro-jepsen/src/lib.rs`** (not in files_modified): the `use clap as
   _;` anchor is deleted because soak.rs uses clap (the p14-notes rule).
2. **The soak driver's mix has no secret-using effects against a local mock.** The effects
   bridge refuses loopback targets (SSRF guard), and the harness has no allowlisted mock
   or secret store. This is stated in `docs/soak.md` §2. The mix is profile-A starts,
   `signals` (a durable sleep plus a signal), `child-parent`/`leaf_echo`, the cron
   schedule, and an admin token create + revoke every 30 s.
3. **A dated amendment to `docs/soak.md`** (commit 9ea947b, before smoke 3) after smokes
   1 and 2:
   - in-flight cap `min(4096, nofile/4)`;
   - run with `ulimit -n 65536`;
   - RssAnon/RssFile recorded;
   - the cron slice created by the admin.

   C1–C9 are unchanged, and smokes 1 and 2 stay recorded as FAIL. The original doc commit
   84021e2 is an ancestor of the driver commit 4e403dd, and so is the amendment.
4. **Pre-registration detail:** smokes (< 24 h) judge C1 as growth ≤ 10% over the window,
   and C2 has a ±64 fd floor. Both were written into `docs/soak.md` before any run.
5. **Plan gate:** the plan names `ostro-k8s-ci --parallel`. The cluster was withdrawn, so
   it runs on dell-4 (see Gate).
6. **The 14-22 support module (`tests/support/p14.rs`) is not reused.** The bench needs a
   tenant token and quotas, which `bench::Rig` builds itself on the same env settings
   (`OSTRO_AUTH__MODE`, `OSTRO_BOOTSTRAP_TOKEN`, `OSTRO_AUDIT__ENABLED`).

## Workflow hunks

None proposed. The benchmarks and the soak smoke are `#[ignore]` and long. For 14-27: if
the 30 min soak smoke becomes a gate step, run it after the failpoint build, with
`ulimit -n 65536` and nothing else running.

## Commits

- 0f53423 test(14-23): bytes-on-disk verified benchmarks
- 84021e2 docs(14-23): soak pass criteria (pre-registered)
- 9ea947b docs(14-23): soak criteria amendment 2026-11-27 (driver in-flight cap, cron slice)
- 4e403dd feat(14-23): soak driver + smoke
- 38ce989 docs(14-23): Phase 14 benchmark results

## Self-Check

Files exist on main: `crates/ostro-jepsen/src/{bench,soak}.rs`, `src/bin/ostro-soak.rs`,
`tests/{bench_single,bench_rf3,soak_smoke}.rs`, `docs/soak.md`, and `docs/benchmarks.md`
with "Phase 14 (1.0 configuration)". All five commits are on main. The doc commit is an
ancestor of the soak code commit. Self-Check: PASSED (files and commits). The plan's
must-have "soak smoke passes" is NOT met (see Gate and Findings).
