# Soak test: pre-registered pass criteria

Phase 14 (QA-05 soak half, D-P14-22). This document fixes the workload, the sampling and
the **PASS / FAIL criteria before any soak run**. It was committed on its own, before the
driver code (`crates/ostro-jepsen/src/soak.rs`, `ostro-soak`). A criterion that changes
after a run has seen data needs a new dated section below that says why; the old text
stays.

## 1. Purpose

A soak shows that Ostro stays stable under sustained load: memory, file descriptors and
disk stay bounded, latency stays inside the 1.0 SLO, nothing restarts, the audit chain
always verifies, and no acknowledged work is lost or duplicated. Benchmarks
(`docs/benchmarks.md`) show peak throughput. A soak shows that a fraction of that load
can run for days.

## 2. Load

- **Rated load** = the verified steps/s measured by `bench_single` (one node) or
  `bench_rf3` (three nodes) **on the same machine with the same binary**. Pass it with
  `--rated <steps/s>`. With `--rated auto`, the driver first runs a 60 s benchmark (after
  a 10 s warm-up) with the same node count, and uses its verified steps/s.
- **Soak load** = `--load` × rated. The default and the pre-registered value is **0.7**.
  The driver issues profile-A starts (`trivial-steps`, 10 steps) **open loop**, at
  `load × rated / 10` runs per second. Every run is long-polled to completion.
- **Configuration** = the 1.0 configuration of the benchmarks: token auth (a bootstrap
  system admin creates tenant `bench` and a tenant token; all load uses the tenant token),
  the audit journal on, and generous tenant quotas on the tenant (on the admission path,
  never binding).

### Workload mix

| slice | rate | what it exercises |
|---|---|---|
| profile-A starts | `load × rated / 10` runs/s | the start path, steps, group commit |
| `signals` runs (mode `before`: a 200 ms durable sleep, then a wait for signal `x`) and the signal | 1 per second | timers, signals, eviction and resume |
| `child-parent` `parent_attached` runs with a `leaf_echo` child | 1 per second | child runs (start, completion, parent resume) |
| a cron schedule `soak-tick` (`*/10 * * * * *`, profile A) | every 10 s | the cron driver |
| admin actions: create a tenant `read` token, then revoke it | every 30 s | audited admin path (two audit records per round) |

**Not in the 1.0 driver: secret-using effects against a local mock.** The effects bridge
refuses loopback targets by default (SSRF guard). Exercising it needs an allowlisted mock
endpoint plus a secret store configured in the harness, and the harness does not have that
yet. The soak verdict therefore says nothing about effects or secret resolution. This gap
is recorded in the 14-23 SUMMARY.

The extra slices are a small, fixed load (about 2 runs/s plus a cron run every 10 s). The
acknowledged `signals` and `child-parent` runs go through the same lost check as the
profile-A runs (C7). Cron runs are never acknowledged to a client, so they are not checked
run by run: the driver checks that the schedule was created and still exists at the end.

## 3. Sampling

- **Interval:** 1 h for the 7-day run; 60 s for ad-hoc smokes; 30 s for the `soak_smoke`
  test (`--interval`).
- **Warm-up:** 1 h for runs of 24 h or more. **60 s for smokes** (runs under 24 h).
  Samples taken during the warm-up are written, but no criterion reads them.
- **Each sample** (one JSON line in `samples.jsonl`) records, per node: pid, VmRSS
  (`/proc/<pid>/status`), open fds (`/proc/<pid>/fd`), bytes of `wal/`, `state/`,
  `snapshots/`, `blobs/`, `audit/`, `meta/`, local snapshot files per partition
  (`snapshots/pNNNN/*.osnap`), `ostro_wal_log_start` per partition (from `/metrics`), the
  audit chain check (`audit::verify::verify_dir` on `<data>/audit`), and whether the
  process restarted. It also records the driver's counters for the interval: starts
  acknowledged, completed, lost, shed, mix successes and errors, start-ack and run p99, the
  achieved start rate, and the machine load (`/proc/loadavg`, MemAvailable).

## 4. PASS criteria

The verdict is **PASS** only if every criterion below passes. A criterion marked n/a does
not fail the run. Otherwise the verdict is **FAIL**, and `verdict.json` lists each
criterion with its measured value and its bound.

| # | criterion | bound |
|---|---|---|
| C1 | **RSS** | Runs of 24 h or more: the linear-regression slope of each node's RSS over the post-warm-up samples is **< 2% per 24 h**, relative to that node's post-warm-up median RSS. Runs under 24 h (smokes): the regression-fitted RSS growth over the post-warm-up window is **≤ 10%** of the median. With fewer than 3 post-warm-up samples C1 **fails** (insufficient evidence). |
| C2 | **open fds** | Every post-warm-up sample is within **±5%** of the node's post-warm-up median, or within ±64 fds, whichever is larger. The driver's own connections can move the count by tens: each long-poll in flight holds one connection. With fewer than 3 post-warm-up samples C2 fails. |
| C3 | **disk bounded** | (a) **WAL floor advances**: for each partition, `ostro_wal_log_start` at the last sample is greater than at the first post-warm-up sample. This is judged only on runs of at least 2 h (snapshots come every 1 h or every 256 MiB appended, and the WAL is trimmed only behind a snapshot); shorter runs record it as n/a. (b) **At most 2 local snapshots per partition** (`[snapshots] keep = 2`). A third file counts only if it is still there at the next sample, since it may be a snapshot that is being published. (c) **Audit within retention**: `audit/` stays ≤ `[audit] max_bytes` (10 GiB). |
| C4 | **latency SLO** | Single node: **p99 start-ack ≤ 50 ms** and **p99 step latency ≤ 200 ms** over all post-warm-up observations. This is the 1.0 SLO. Step latency is measured on the whole profile-A run (start request → completed result). It is an upper bound on any one step's latency, because the run's 10 steps run in sequence inside it. Three-node loopback/LAN cluster: the same values are recorded but not judged (n/a). |
| C5 | **restarts** | **0** restarts or crashes of any node (a pid change or a process not running at a sample). |
| C6 | **audit chain** | `verify_dir` passes on every node at **every** sample. A failure is retried twice, 250 ms apart, because a record may be half-written at that moment; three failures in a row fail the sample. |
| C7 | **lost acked runs** | **0**. A run counts as lost when its start was acknowledged and then its result poll answered 404, it ended in any status other than `completed` (or the mix slice's expected result), or it was not terminal 60 s after the load stopped. |
| C8 | **duplicate journal entries** | **0** in the final check. The last 2,000 acknowledged profile-A runs are read back through the history API. Each must have exactly 10 distinct `step_completed` seqs, and no seq may repeat in its journal. |
| C9 | **load sustained** | The achieved post-warm-up start rate is **≥ 95%** of the target rate. A soak that sheds its load proves nothing. Starts the driver could not issue because 4,096 runs were already in flight count as shed. |

The 7-day run and the smokes use these same criteria. Only the warm-up and the C1 form
change, as stated above.

## 5. Commands

Prerequisites, from the repository root (the harness builds the guests itself):

```text
cargo build -p ostro --release --locked
cargo build -p ostro --features failpoints --target-dir target/failpoints --locked   # harness requirement
cargo build -p ostro-jepsen --release --locked --bin ostro-soak
```

The nodes run `target/release/ostro` (or `--bin <path>`). The harness also needs the failpoint
build to exist (it checks for it at cluster start). Data dirs go under `OSTRO_PROC_ROOT`
(default `/dev/shm` when it is large enough). For multi-day runs, point it at an NVMe
directory.

**The 7-day run (human_needed):**

```text
mkdir -p /srv/ostro-soak/proc
OSTRO_PROC_ROOT=/srv/ostro-soak/proc \
  target/release/ostro-soak --nodes 3 --duration 168h --load 0.7 --rated <steps/s> \
  --interval 1h --out /srv/ostro-soak/out
```

`<steps/s>` is the verified steps/s from `bench_rf3` on the same machine with the same
binary (from `target/bench/rf3.json`, field `steps_per_s`). Use `bench_single` for a
`--nodes 1` soak. The exit code is 0 on PASS, 1 on FAIL and 2 when the run could not
start.

**Smoke (30 min, the gate):**

```text
OSTRO_SOAK_SECS=1800 cargo test --release -p ostro-jepsen --locked --test soak_smoke -- --ignored --nocapture --test-threads=1
```

The default smoke is 300 s (`OSTRO_SOAK_SECS`), at load 0.7 (`OSTRO_SOAK_LOAD`), with a
30 s interval, a 60 s warm-up and `--rated auto` on one node (`OSTRO_SOAK_RATED=<steps/s>`
skips the rated benchmark). It writes `target/soak/{samples.jsonl,verdict.json}` relative
to the workspace root.

## 6. Submitting results (human_needed)

Attach the following to the Phase 14 VERIFICATION record (`14-VERIFICATION.md`, QA-05):

- `verdict.json` and `samples.jsonl` from the `--out` directory;
- the `bench_rf3` (or `bench_single`) JSON that gave `--rated`;
- the hardware (`lscpu | head -20`, `nproc`, `uname -r`, the NVMe model, `findmnt -T <proc root>`);
- the binary sha256 and HEAD (both are in every sample and in the verdict).

A FAIL verdict is a finding, not a test to retry. Record which criterion failed and its
value, then open a fix before running again.

## 7. Amendments

### 2026-11-27 (after the first two driver smokes, before the third)

**What happened.** Both smokes ran on a shared dell-4, with soft `ulimit -n` 1024.

- **Smoke 1 (180 s):** every criterion passed except **C1**. RSS grew 41% over the window,
  from 1.0 GB to 1.7 GB.
- **Smoke 2 (300 s, rated 42,002 steps/s):** about 60 s in, a latency spike piled up
  in-flight runs. The driver allowed up to 4,096 in flight, and every one held a connection.
  The node, which inherits the driver's fd limit, ran out of file descriptors. Its
  metadata raft driver then stopped ("raft storage: Too many open files"), and it never
  recovered. The driver process also hit EMFILE, so its own samples went blank. That run
  failed C2, C4, C6, C7, C8 and C9.

The node-side behaviour is a product finding, recorded in the 14-23 SUMMARY. None of the
criteria C1 to C9 change. The FAILs above stand as recorded.

**Driver changes (they change what C9 counts as "shed"):**

- The in-flight cap is now `min(4096, soft RLIMIT_NOFILE / 4)`, and at least 16. Starts
  beyond that cap count as shed, as before. The fd limit and the cap are written into the
  verdict notes.
- Run the soak with a raised fd limit (`ulimit -n 65536`) so that the cap is 4,096. The
  commands in §5 should be read with that prefix.
- Samples also record `RssAnon` and `RssFile` per node, to tell heap growth from mapped
  files. C1 still judges VmRSS.
- The cron slice: schedule creation needs the `admin` scope, and schedule names cannot be
  tenant-qualified. The system admin therefore creates `soak-tick` with a plain name and
  the workflow `bench/trivial-steps`.

### 2026-11-27 (p14-fixups-4, after smoke 3)

**What changed.** Only the driver changed; C1 to C9 did not, and smoke 3 still counts as a
FAIL.

- **The driver's HTTP pool is bounded.** Every driver request now goes through one client
  that keeps at most 128 idle keep-alive connections per node, each for at most 15 s
  (`soak::driver_http`). Before this, the connections opened during a latency spike stayed
  open for the rest of the run, about 4,900 in smoke 3, and counted in the node's fds (C2).
  The in-flight cap and what counts as shed are unchanged.
- **The node fail-stops on a stopped metadata driver.** If the node's metadata Raft driver
  stops (for example on EMFILE), the node now exits non-zero instead of serving 503. The
  harness does not restart it. C5 counts it as a "not running" observation.
- **The cron slice stays as pre-registered.** A tenant admin can create tenant schedules (it
  always could: `admin` scope on the tenant). The load token has no `admin` scope, so the
  system admin still creates `soak-tick`.
