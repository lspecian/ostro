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
