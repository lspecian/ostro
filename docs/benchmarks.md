# Benchmarks

Phase 5 success criterion 4 and NFR-2 (docs/02-prd.md): single node, trivial Rust steps,
fsync on. The harness is `crates/ostro-server/tests/perf.rs` (plan 05-16), an `#[ignore]`d
test that drives the real in-process server.

## Definitions (D16)

- **A step counts** only when its `StepCompleted` was **acknowledged** and is **found on
  disk after a restart**. Acknowledged: the run's result came back through the API as
  `completed`, carrying the guest's own step count. On disk: after the load the harness
  shuts the server down, starts it again on the same data dir and counts `step_completed`
  events of every run through the history API. Steps/s uses this verified count only; a
  mismatch with the acknowledged count fails the bench.
- **Profile A** (the gate): runs of the `trivial-steps` guest with `{n: 10}` (10 steps per
  run), 2,000 runs by default.
- **Profile B** (reported): `{n: 1}`, 10,000 runs by default; runs/s and steps/s.
- **Load:** a closed loop of `OSTRO_BENCH_CONCURRENCY` (default 64) clients, each starting a
  run and long-polling its result, until the run count is reached. Elapsed time is from the
  first start to the last acknowledged result (after a warm-up of 16 runs).
- **Start → first step:** from the client's wall clock just before the start request to the
  `ts` of the run's first `StepCompleted` in the journal (the engine's wall clock when the
  step was recorded). NFR-2.4 asks for it on an **idle** node: measure it with
  `OSTRO_BENCH_CONCURRENCY=1`. Under the 64-client load it is mostly queueing.
- **fsyncs/s:** completed flush requests of the block device holding the data dir
  (`/proc/diskstats` field 19) over the load window. A device-wide proxy: other writers on
  the same device are counted too. The harness also measures the raw fdatasync cost
  (median of 200 4 KiB write + fdatasync pairs on the data dir).
- **CPU:** the process's `utime + stime` over the load window (`/proc/self/stat`).
- **Bytes on disk:** apparent size of every file in the data dir after the restart.
- **Expiry cost (OPS-06):** one run with 50,000 journal events completed with retention 1 s.
  A probe submits a rejected `SignalRun` (unknown run) to the same partition every
  millisecond; the worst probe latency around the expiry is how long apply was blocked by
  the `EXPIRE_OP` chunk that deletes the run.
- Percentiles are computed by sorting; no benchmark crates are used.

The data dir defaults to `CARGO_TARGET_TMPDIR` (same filesystem as `target/`). The harness
prints the filesystem type and refuses tmpfs/ramfs unless `OSTRO_BENCH_ALLOW_TMPFS=1`.

## Running

```text
# Gate (profile A, 64 clients, throughput)
OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
# Gate (start -> first step on an idle node)
OSTRO_BENCH_PROFILE=a OSTRO_BENCH_CONCURRENCY=1 OSTRO_BENCH_RUNS=300 cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
# Reported
OSTRO_BENCH_PROFILE=b cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
OSTRO_BENCH_PROFILE=expiry cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
```

| variable | default | meaning |
|---|---|---|
| `OSTRO_BENCH_PROFILE` | `a` | `a`, `b` or `expiry` |
| `OSTRO_BENCH_RUNS` | A 2,000 / B 10,000 | runs to complete |
| `OSTRO_BENCH_CONCURRENCY` | 64 | clients in the closed loop |
| `OSTRO_BENCH_TRANSPORT` | `inproc` | `inproc` (ControlPlane calls) or `http` (REST on the server's port) |
| `OSTRO_BENCH_PARTITIONS` | 16 (server default) | partitions |
| `OSTRO_BENCH_DATA_DIR` | `CARGO_TARGET_TMPDIR` | parent of the data dir |
| `OSTRO_BENCH_ALLOW_TMPFS` | unset | `1` allows tmpfs/ramfs |
| `OSTRO_BENCH_EXPIRY_EVENTS` | 50,000 | journal events of the expiry run |

Each run prints one `PERF_JSON {...}` line and a table.

## Native NVMe gate procedure (human_needed, D15)

The gate numbers come from **native Linux on NVMe, 8 vCPU**, run by a human. WSL2 numbers
are advisory: measured and reported, never a failure reason on their own.

1. Native Linux (no WSL, no VM with a virtual disk), NVMe SSD, 8 vCPU (`taskset -c 0-7`
   or an 8 vCPU machine), ext4 or xfs, nothing else running.
2. Record the hardware: `lscpu | head -20`, `nproc`, `uname -r`, the NVMe model
   (`cat /sys/block/nvme0n1/device/model`), `findmnt -T target` and the filesystem.
3. Build once, then run each command three times and keep the median:
   ```text
   OSTRO_BENCH_PROFILE=a cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
   OSTRO_BENCH_PROFILE=a OSTRO_BENCH_CONCURRENCY=1 OSTRO_BENCH_RUNS=300 cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
   OSTRO_BENCH_PROFILE=b cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
   OSTRO_BENCH_PROFILE=expiry cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture
   ```
4. Pass: profile A `steps_per_s >= 5000` (Phase 5 exit; NFR-2.5 asks 10,000) and, at
   concurrency 1, `start_first_step_p50_ms < 5`. Record the `PERF_JSON` lines and the
   hardware in `.planning/phases/05-workflow-semantics-core-hardening/05-VALIDATION.md`.

## WSL2 results (advisory)

Machine: WSL2 kernel 6.6.87.2-microsoft-standard-WSL2, 12 logical CPUs, ext4 on the WSL
virtual disk (`stat -f` reports `ext2/ext3`), raw fdatasync p50 2.4-3.1 ms. Release build,
16 partitions, fsync on. Single runs; expect a few percent of noise.

### Baseline (HEAD 21d9e58, before the 05-16 start-path changes)

| profile | transport | clients | steps/s | runs/s | start→first step p50 / p99 | CPU (cores) | flushes/s | bytes on disk |
|---|---|---|---|---|---|---|---|---|
| A | inproc | 64 | 15,598 / 16,513 / 16,184 | 1,560-1,651 | 29.9 / 40.3 ms | 2.3 | 766-798 | 9.44 MB |
| A | http | 64 | 15,839 | 1,584 | 29.1 / 72.3 ms | 2.6 | 786 | 9.44 MB |
| A | inproc | 1 | 728 | 73 | **9.83 / 13.4 ms** | 0.33 | 700 | 2.31 MB |
| A | http | 1 | 514 | 51 | 11.1 / 43.3 ms | 0.28 | 505 | 2.32 MB |
| B | inproc | 64 | 1,667 | 1,667 | 28.7 / 38.4 ms | 2.3 | 795 | 27.6 MB |
| B | http | 64 | 1,539 | 1,539 | 30.8 / 41.1 ms | 3.0 | 743 | 27.6 MB |

Expiry (OPS-06), 50,000 events: the run completed in 0.26 s; probe p50 3.1 ms before the
expiry; worst probe latency (apply blocked by the EXPIRE_OP chunk) **44.7 ms**; record gone
47 ms after its due time. Far below 1 s: no bounded-delete continuation is needed at the
journal size measured (05-09 measured 252 ms in debug at the apply level).

Reading the baseline: at concurrency 1 the first step runs after three sequential WAL
commits (StartRun, the awaited AttemptGranted, the executor's RunStarted append), each
about one fsync (2.5-3 ms here), plus instantiation. Throughput under load is bounded by the group commit: about 800 device
flushes/s carry about 1,600 runs/s.

### Start-path optimisations, before / after (plan 05-16)

Same machine, release, P=16. A = profile A, B = profile B; "idle" = 1 client, 300 runs.

| change | A inproc steps/s (64 clients) | A http steps/s | idle start→first step p50 / p99 (inproc) | idle p50 (http) | B inproc runs/s | kept |
|---|---|---|---|---|---|---|
| baseline (HEAD 21d9e58) | 15,598 / 16,513 / 16,184 | 15,839 | 9.83 / 13.4 ms | 11.1 ms | 1,667 | - |
| 1. pipelined AttemptGranted + one record read per dispatch | 20,061 / 20,854 | 18,966 | 9.72 / 15.1 ms | 10.7 ms | 1,622 / 1,428 (noisy, see below) | **yes** |
| 2. `StartRun.run_started` set by the API (experiment) | 30,001 / 30,363 | 29,759 | **3.38 / 5.5 ms**, 3.36 / 5.3 ms | 3.56 ms | 3,213 | **no** (apply support kept, API leaves the field empty) |

1. **Pipelined grant** (dispatcher): the job starts once the grant's log offset is fixed,
   without waiting for its fsync and apply; the executor's first append shares the grant's
   group commit. Under load the 64-client latency drops from 29-30 ms to 20 ms and
   throughput rises about 28%. On an idle node it changes little: the executor reaches its
   RunStarted append while the grant's fsync is still running, so the two commits stay
   back to back.
2. **`StartRun.run_started`**: when the API pre-encodes RunStarted into the StartRun, apply
   journals it at seq 0 and the executor's own RunStarted commit disappears; the first step
   then runs right after StartRun's commit. Measured with the API setting the field, it is
   the largest single win (idle p50 9.7 → 3.4 ms, under NFR-2.4's 5 ms even on WSL2;
   profile A about 30,000 steps/s). It is **not enabled**: a run that never started would
   no longer have an empty journal (`next_seq == 0`), which D3 and the 05-13/05-15 suites
   pin (`schedule_to_start_without_capacity`, `signal_immediately_after_start`,
   `lifecycle_api::cancel_before_start_is_flag_only_and_idempotent` fail with it). Apply
   accepts the field (tests in `crates/ostro-partition/tests/apply_run_started.rs`) and
   `run_started::encode_for_start` produces byte-identical events, so enabling it is a
   start-path switch once D3 is amended (see the 05-16 SUMMARY).

Profile B on this machine is noisy (1,119 to 1,984 runs/s across three runs of the same
build): the WSL2 virtual disk's flush rate varied between 400 and 770 flushes/s in those
runs, and runs/s follows it.

## Phase 5 results

Final start path of Phase 5 (plan 05-16): pipelined AttemptGranted, one record read per
dispatch, executor RunStarted append (the API does not set `StartRun.run_started`).
Environment as above (WSL2 6.6.87.2, 12 logical CPUs, ext4 on the virtual disk, release,
P=16). The virtual disk's fdatasync cost moved between 2.2 and 6.9 ms during the final
session, so each row carries the harness's own fsync measurement taken right after the load.

| profile | transport | clients | steps/s | runs/s | start→first step p50 / p99 | flushes/s | CPU (cores) | bytes on disk | raw fsync p50 |
|---|---|---|---|---|---|---|---|---|---|
| A | inproc | 64 | **20,110 / 19,589 / 20,028** | 1,959-2,011 | 20.6-21.0 / 28.8-35.8 ms | 721-745 | 2.7 | 8.97 MB | 2.6-3.1 ms |
| A | http | 64 | 19,385 | 1,939 | 21.0 / 43.6 ms | 722 | 3.3 | 8.97 MB | 2.6 ms |
| A | inproc | 64 | 12,652 / 12,068 | 1,207-1,265 | 34.0-35.5 / 42-44 ms | 444-460 | 1.2-1.3 | 8.98 MB | 4.2-4.3 ms |
| A | http | 64 | 15,740 | 1,574 | 22.9 / 57.4 ms | 594 | 1.9 | 8.98 MB | 6.9 ms |
| A | inproc | 1 (idle) | 960 / 934 | 93-96 | **7.38 / 11.4 ms, 7.50 / 11.8 ms** | 849-853 | 0.22 | 2.29 MB | 2.2 ms |
| A | inproc | 1 (idle) | 737 | 74 | 9.72 / 14.1 ms | 706 | 0.31 | 2.31 MB | 2.6 ms |
| A | inproc | 1 (idle) | 411 / 399 | 40-41 | 18.6-19.5 / 26.7 ms | 393-412 | 0.22 | 2.32 MB | 4.6-4.7 ms |
| A | http | 1 (idle) | 678 | 68 | 9.08 / 20.9 ms | 654 | 0.20 | 2.32 MB | 4.1 ms |
| B | inproc | 64 | 1,271 / 1,265 (1,018-1,130 at 4.7-4.9 ms fsync) | same | 33.6-34.1 / 43.5-44.7 ms | 455-459 | 1.24 | 25.3 MB | 4.4 ms |
| B | http | 64 | 1,261 (1,157 at 5.1 ms fsync) | same | 33.6 / 47.2 ms | 456 | 1.5 | 25.3 MB | 4.2 ms |

Expiry (OPS-06), 50,000 events: apply blocked **33.6-44.8 ms** by the EXPIRE_OP chunk
(record gone 37-48 ms after due). No bounded-delete continuation needed.

**WSL2 advisory: target ≥ 5,000 steps/s (profile A) and p50 < 5 ms on native NVMe;
measured here 19,589-20,110 steps/s (12,068-15,740 while the virtual disk's fsync was
4-7 ms) / idle p50 7.4-9.7 ms at 2.2-2.6 ms fsync.** Throughput clears the Phase 5 exit
target with room to spare even on WSL2. Idle latency does not: it is **fsync-bound**. Three
WAL commits sit in series before the first step (StartRun, the grant, which the executor's
RunStarted append queues behind, and that append), at 2.2-4.7 ms each here, and the
measured p50 tracks the fsync cost (7.4 ms at 2.2 ms, 9.7 ms at 2.6 ms, 18.6-19.5 ms at
4.6-4.7 ms). With `StartRun.run_started` set by the API (not enabled, see above) the
same machine measured 3.4 ms.

**Gate status: human_needed.** Run the documented command on native Linux NVMe 8 vCPU and
record the results in 05-VALIDATION.md.

### D24: `StartRun.run_started` set by the API (plan 05-25)

From 05-25 the API start and retry-restart paths set `StartRun.run_started` (D24, amends
D3), so `RunStarted` is journaled at seq 0 inside the StartRun apply and the executor's own
RunStarted append (one WAL commit in series before the first step) is gone. The paragraph
above that says the switch is "not enabled" describes the state at 05-16.

Same-session A/B on WSL2 (release, P=16, 12 logical CPUs, ext4 virtual disk), with
interleaved runs of two perf binaries built from the same tree: **before** = the API leaves
the field empty (05-16 state), **after** = 05-25. The virtual disk was slower than during
05-16 (raw fsync p50 7.8-8.6 ms, other builds running on the machine), so compare rows
within this table, not with the 05-16 tables.

Command (idle): `OSTRO_BENCH_PROFILE=a OSTRO_BENCH_CONCURRENCY=1 OSTRO_BENCH_RUNS=300 cargo test --release -p ostro-server --test perf --locked -- --ignored --nocapture`
(64 clients: `OSTRO_BENCH_CONCURRENCY=64 OSTRO_BENCH_RUNS=2000`).

| profile | clients | variant | steps/s | runs/s | start→first step p50 / p99 | flushes/s | CPU (cores) | bytes on disk | raw fsync p50 |
|---|---|---|---|---|---|---|---|---|---|
| A | 1 (idle) | before | 315 / 273 / 315 | 27-32 | **24.10 / 28.2, 24.33 / 58.6, 23.89 / 33.3 ms** | 418-491 | 0.11-0.13 | 2.32 MB | 7.9-8.2 ms |
| A | 1 (idle) | after | 386 / 420 / 408 | 39-42 | **8.53 / 13.3, 8.41 / 14.7, 8.51 / 14.6 ms** | 469-471 | 0.13-0.14 | 2.19 MB | 8.1-8.6 ms |
| A | 64 | before | **12,592 / 11,907** | 1,191-1,259 | 33.6-34.6 / 48.7-64.2 ms | 448-463 | 1.2-1.3 | 8.97-8.99 MB | 7.8-8.1 ms |
| A | 64 | after | **18,373 / 17,766** | 1,777-1,837 | 17.3-17.9 / 23.9-27.7 ms | 435-455 | 1.5 | 8.18-8.19 MB | 8.2-8.3 ms |

A single "after" idle run earlier in the session, while the disk's fsync p50 was 3.3 ms,
measured **5.71 / 23.9 ms** (450 steps/s, 2.19 MB). 05-16 measured 3.38 ms at 2.4-3.1 ms
fsync with the same switch.

- Idle start → first step p50: **~24 ms → ~8.5 ms** at ~8 ms fsync (-65%). The p50 now
  tracks about one fsync instead of about three: StartRun (with RunStarted) commits, and
  the pipelined grant and the first step's flush follow it.
- Profile A, 64 clients: **~12.2k → ~18.1k steps/s** (+48%) at the same disk speed. 05-16
  measured ~20k → ~30k at 2.4-3.1 ms fsync.
- Bytes on disk drop by about 0.4 KB per run (one `EventsAppended` record fewer per run;
  RunStarted moves into the StartRun record), so 2,000 × 10-step runs take 8.18 MB
  instead of 8.97 MB.
- Every run's steps were verified on disk after a restart (D16) in all rows.

**Gate status: unchanged, human_needed.** NFR-2.4 (idle p50 < 5 ms) is judged on native
Linux NVMe (D15). On WSL2 the idle figure is fsync-bound and moves with the virtual disk.

## Phase 14 (1.0 configuration)

Plan 14-23 (QA-04, D-P14-21). These benchmarks run the 1.0 configuration: token auth (a
bootstrapped system admin creates tenant `bench` and a tenant token, and the load uses that
token), the audit journal on, and generous tenant quotas on the admission path. They run as
loopback processes with the release `ostro` binary. The harness is `crates/ostro-jepsen`
(`src/bench.rs`, tests `bench_single` and `bench_rf3`).

### Method

- **A step counts only when it is found on disk.** For the single node, after the load the
  node is killed with `kill -9`, and its WAL is scanned offline. Each acknowledged run needs
  its `StartRun` and its 10 distinct `StepCompleted` events. The node is then restarted on
  the same data dir, and every acknowledged run is read back through the API: status
  `completed`, and 10 distinct `step_completed` events in its history. For RF=3 (three
  processes, `min_insync` 2), the cluster is stopped with `kill -9`. Every replica's WAL is
  then scanned offline, and each acknowledged run needs all of that on at least `min_insync`
  replicas. **acked == on disk is always asserted.**
- **Load:** 64 closed-loop clients, each starting a profile-A run (`trivial-steps`, n=10)
  and long-polling its result. There is a 10 s warm-up, then a 60 s measured window.
- **steps/s** = verified steps of the runs whose completion was acknowledged inside the
  window, divided by the window length.
- **Bytes on disk per step** = growth of `wal/`, `state/`, `blobs/` and `audit/`, summed
  over every node, from before the load to after the stop, divided by the verified steps.
- **Commit latency:** there is no Phase 12 commit-latency histogram (14-01 finding). Commit
  latency is therefore measured client-side as start request to start acknowledgement
  (the 12-17 method). The server's `ostro_apply_batch_seconds` p99 (a bucket bound) is
  reported next to it.
- **Client ack latency:** start request to the completed result.
- **Thresholds** are asserted only with `OSTRO_BENCH_ENFORCE=1`:
  - single node: ≥ 10,000 steps/s (NFR-2.5);
  - RF=3: ≥ 5,000 steps/s and commit p99 < 20 ms.
- **Reports:** `target/bench/{single,rf3}.json` (workspace root), with the binary sha256,
  HEAD and machine load at start and end.

```text
cargo build -p ostro --release --locked
cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
cargo test --release -p ostro-jepsen --locked --test bench_single -- --ignored --nocapture --test-threads=1
cargo test --release -p ostro-jepsen --locked --test bench_rf3 -- --ignored --nocapture --test-threads=1
# gate box: add OSTRO_BENCH_ENFORCE=1
```

| variable | default | meaning |
|---|---|---|
| `OSTRO_BENCH_ENFORCE` | unset | `1` asserts the thresholds |
| `OSTRO_BENCH_SECS` / `OSTRO_BENCH_WARMUP_SECS` | 60 / 10 | measured window, warm-up |
| `OSTRO_BENCH_CLIENTS` | 64 | closed-loop clients |
| `OSTRO_BENCH_BIN` | `target/release/ostro` | node binary (falls back to the failpoint build, advisory) |
| `OSTRO_BENCH_OUT` | `target/bench` | report directory |

### Tooling validation on a loaded dell-4 (NOT the gate numbers)

These runs were short (5 s warm-up and a 20 s window) on dell-4 (24 cpus visible to the
container, NVMe). Other lanes were running a full CI run and two crash campaigns at the
same time: the load average was 34 to 63. The release binary had sha256 `899957ca…`, and
HEAD was 84021e2. **The numbers only show that the instruments work. They are not
performance results.**

| bench | acked runs | verified on disk | steps/s | commit p50 / p99 | client ack p50 / p99 | bytes/step (wal + state) |
|---|---|---|---|---|---|---|
| single node | 81,585 | 81,585 (and 81,585 read back after the restart) | 29,421 | 7.5 / 49.3 ms | 17.1 / 73.9 ms | 147.7 + 98.1 |
| RF=3 loopback | 11,091 | 11,091 on ≥ 2 replicas | 4,359 | 67.4 / 180.7 ms | 138.3 / 313.5 ms | 442.9 (three WALs) |

The audit journal does not grow with starts: only admin actions are audited, so audit bytes
per step are 0.

Two full-length (10 s + 60 s) single-node runs used as the soak's rated load measured
**42,002** and **41,472** verified steps/s, every acknowledged run on disk. Both ran
while dell-4 was less loaded (load average 20 to 24) and are still advisory.

### Soak

The soak criteria, the driver (`ostro-soak`) and the smoke are in `docs/soak.md`. The
third 300 s smoke on the loaded dell-4 ran one node at 0.7 × 41,472 = 2,903 runs/s. It
**FAILED on C1 (RSS) and C2 (fds)** and passed every correctness criterion:

- 0 lost acknowledged runs;
- 0 duplicate journal entries in 2,000 runs read back;
- the audit chain verified at every sample;
- 0 restarts;
- the load was sustained at 100%;
- start-ack p99 47.6 ms and run p99 135.7 ms over 696,760 runs.

Over the window, anonymous memory grew from 1.49 GB to 3.06 GB. The node's fds stayed at
about 5,200 after a latency spike (the driver's connection pool). Both are open findings
(14-23 SUMMARY). The smoke has not passed yet.

### Gate procedure (human_needed / orchestrator)

1. Run on a quiet dell-4 (nothing in the other lanes), then on the native NVMe 8 vCPU box
   (D15). Run each bench three times and keep the median.
2. For the gate, set `OSTRO_BENCH_ENFORCE=1`. Record `target/bench/*.json` in
   `14-VERIFICATION.md`.
3. The RF=3 thresholds are a loopback figure here. The LAN figure needs three machines.
