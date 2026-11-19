---
phase: 02-durable-partition-log-state-machine
plan: 16
subsystem: storage
tags: [fjall, reopen-time, throughput, write-stall, wal-recovery, on-disk-format, docs, ci]
requires:
  - phase: 02-15
    provides: "ostro-crashtest crash-target + crash loops, the segment-growth reopen finding"
provides:
  - "crates/ostro-crashtest/tests/reopen_time.rs: StoreConfig variant reopen after kill -9 at the journal cap + reopen vs WAL segment count (ignored, prints tables)"
  - "crates/ostro-crashtest/tests/apply_throughput.rs: S3 RMW workload at machine and store level with batch/checkpoint latency (ignored, prints tables)"
  - "crash-target flags: --pipeline, --blob-pct, --journal-mib, --memtable-mib, --workers, --cache-mib (defaults unchanged)"
  - "ostro-crashtest lib: run_and_kill_with, dir_bytes, loadavg"
  - "ostro-log recovery fsyncs only the last non-empty segment (reopen no longer grows with segment count)"
  - "docs/storage-format.md: the frozen Phase 2 on-disk format + fjall measurements"
affects: [04-server-startup, 05-hardening, 08-snapshots]
tech-stack:
  added: []
  patterns:
    - "Measurement tests are #[ignore], print markdown tables with loadavg, and assert only sanity bounds (reopen < 30 s, outcomes Accepted)"
    - "Machine-internal latency is captured with a tiny global metrics::Recorder in the test binary (histograms by name), no production hook needed"
    - "Recovery durability by induction: only the last non-empty WAL segment can hold never-fsynced bytes"
key-files:
  created:
    - crates/ostro-crashtest/tests/reopen_time.rs
    - crates/ostro-crashtest/tests/apply_throughput.rs
    - docs/storage-format.md
  modified:
    - crates/ostro-log/src/recovery.rs
    - crates/ostro-log/src/rf1.rs
    - crates/ostro-crashtest/src/bin/crash-target.rs
    - crates/ostro-crashtest/src/lib.rs
    - crates/ostro-crashtest/Cargo.toml
    - crates/ostro-partition/src/store/config.rs
    - docs/03-architecture.md
    - scripts/ci-local.sh
    - Cargo.lock
decisions:
  - "WAL recovery fsyncs only the last non-empty segment: sealed segments were fsynced by the WAL before rotation/truncation, and a crashed tail is fsynced by the next recovery before LogNode::open can create a newer segment. At 300 restarts Inspect reopen 741 -> 154 ms"
  - "StoreConfig defaults kept (1 worker / 64 MiB journal / 8 MiB memtable / 16 MiB cache): reopen at the journal cap 604-672 ms per partition (< 1 s target), no apply batch >= 100 ms; 32 MiB memtables are slower to reopen, 64 MiB memtables bring back the S3 120 ms stall, 2 workers are within noise"
  - "Store-level RMW is cache-bound: 80.8k cmds/s at the 16 MiB default cache vs 153k at 128 MiB (1M runs). Not fixed by raising the per-partition default (memory scales with P; fjall 3.1.13 cannot share one cache across databases); recorded as an open concern for a node memory budget"
  - "fjall journal files are preallocated to 64 MiB, so journal file size/blocks do not show the un-flushed amount; the replay window is about one active journal plus sealed journals up to max_journaling_size"
metrics:
  duration: ~2h10m wall (mostly gate runs: kill9 500 iterations 73 min and proptest 1000 cases 84 min in parallel, then ci-local ~22 min)
  completed: 2026-11-19
---

# Phase 2 Plan 16: fjall Costs, WAL Reopen Fix, Frozen Format and Phase Gate Summary

This plan measured the fjall costs carried over from spike S3. It also fixed the reopen-time growth found in 02-15 and wrote the frozen Phase 2 on-disk format into `docs/storage-format.md`. The full phase gate then ran on the result.

- **Reopen is bounded.** With the default `StoreConfig`, after kill -9 at the fjall journal cap, store open is 604–672 ms per partition and a Leader `Node::open` takes 828–923 ms. S3 measured 3.2–3.4 s with fjall's 512 MiB default journal.
- **Reopen no longer grows with segment count.** Recovery used to fsync every WAL segment on every reopen. It now fsyncs only the last non-empty one. At 300 kill -9 restarts the Inspect reopen dropped from 741 ms to 154 ms.
- **The S3 write stall is gone.** No apply batch reached 100 ms, and the worst was 50 ms.
- **Store-level RMW throughput misses the 90k rule at the default 16 MiB block cache: 80.8k cmds/s.** It reaches 153k with a 128 MiB cache. That is a memory-budget decision, recorded below as an open concern.

## What was built

- **`crates/ostro-log/src/recovery.rs`** (fix, deviation): after the verified scan, only the last non-empty retained segment is fsynced. The induction argument is in the code comment and in docs/storage-format.md §10. Earlier segments were fsynced by the vendored WAL before rotation or truncation (`group_commit.rs` seal/force-seal/truncate paths call `sync_all` first). Or they were the tail at an earlier recovery, which fsynced them before `LogNode::open` could create a newer segment.
- **`crash-target` flags**: `--pipeline N` (N load tasks per partition), `--blob-pct`, `--journal-mib`, `--memtable-mib`, `--workers`, `--cache-mib`. With the defaults the RNG seeds and behaviour are exactly as in 02-15.
- **lib**: `run_and_kill_with` (extra args), `dir_bytes`, `loadavg`.
- **`tests/reopen_time.rs`** (ignored):
  - `reopen_time_store_variants`: 4 StoreConfig variants × (20 s of `--pipeline 64` load, then SIGKILL, then two copies). Times `PartitionStore::open` per partition, `recover_partition` per partition and a Leader `Node::open` until every slot is Ready.
  - `reopen_time_segments`: N restarts. Times the Inspect reopen at 5 marks, with a breakdown of read-all vs fsync-all vs fsync-last.
- **`tests/apply_throughput.rs`** (ignored), using the S3 `--rmw` workload over 1M preloaded and granted runs:
  - `apply_throughput_machine`: P=1 Node and `submit` from 32 submitters, with runs split by index so per-run order holds. A tiny global `metrics::Recorder` captures `ostro_apply_batch_seconds` and `ostro_state_checkpoint_seconds`.
  - `apply_throughput_store`: direct `apply_command` + `ApplyBatch::commit` with the machine's checkpoint cadence. Env overrides allow tuning experiments.
- **`docs/storage-format.md`**: data dir layout, WAL V2 frame, OSTR envelope, every command field number, ids and routing, 13 keyspaces with key and value encodings, meta keys, outcome codes, rolling hash and digest, recovery rules, fjall configuration and all measurements, freeze policy. Every item was checked against the source constants.
- **`docs/03-architecture.md`**: only the storage passages changed. §2.4 is now "fjall keyspaces" with a link. The data-dir tree shows `wal/p/<id>/wal_<id>_<seg>.log` and `state/pNNNN/`. The `state.redb` mentions and the redb StateStore comment are gone, and the file links to storage-format.md.
- **`scripts/ci-local.sh`**: comments only. `FAILPOINT_CRATES` (ostro-wal, ostro-blob, ostro-log, ostro-partition, ostro-crashtest) is unchanged and matches the `.github/workflows/ci.yml` failpoint step. The measurement commands are listed.
- **`StoreConfig::cache_size` doc**: notes the measured cache-bound RMW rate.

## Measurements (release, WSL2 ext4 tempdirs, 12 threads; loadavg in each table)

### Reopen after kill -9 at the journal cap (P=4, ~800k acks / 1.3M applied in 20 s)

| variant | store open ms (p50 / max) | WAL scan ms (p50 / max) | Node::open ms (Leader) |
|---|---|---|---|
| **default** | **637.7 / 660.1** (2nd run 620.0 / 672.0) | 62.1 / 80.5 | **871.8** (2nd run 922.7) |
| journal 128 MiB | 616.3 / 632.5 | 78.7 / 80.9 | 1,037.9 |
| memtable 32 MiB | 876.3 / 897.8 | 62.1 / 100.1 | 1,403.9 |
| workers 2 | 604.0 / 605.1 | 69.6 / 94.7 | 827.9 |

Loadavg was 1.92 before the first variant and 3–6 during the runs, because the saturating load is the workload itself.

### Reopen vs WAL segment count (P=4, 300 kill -9 cycles, ~24.7k offsets/partition)

| | Inspect Node::open @300 | recover_partition max @300 | fsync-all segs (sum P) | fsync-last (sum P) | read-all (sum P) |
|---|---|---|---|---|---|
| before (fsync every segment) | **741.1 ms** | 182.6 ms | 660.4 ms | 2.2 ms | 8.6 ms |
| after (fsync last non-empty) | **153.8 ms** | 11.5 ms | (670.5 ms, not done any more) | 2.2 ms | 7.4 ms |

Intermediate marks (before → after): 76 restarts 198.9 → 55.3 ms, 151 restarts 395.6 → 92.4 ms, 226 restarts 590.4 → 125.4 ms. Some growth remains (about 0.5 ms per restart): it comes from other per-segment work (opening/reading ~300 small files per partition). See the todo below.

### Apply throughput, RMW (1M preloaded + granted runs, 200k measured)

| level | config | cmds/s | reads/cmd | batch ms p50 / p99 / max | ≥ 100 ms | checkpoint ms p50 / max |
|---|---|---|---|---|---|---|
| store, chunk 64 | default | **80,832** | 1.00 | 0.68 / 1.28 / 33.38 | 0 | 4.42 / 6.21 |
| store, chunk 1024 | default | 76,286 | 1.00 | 11.83 / 36.17 / 45.14 | 0 | 4.46 / 6.63 |
| store, chunk 64 | cache 32 / 64 MiB | 79,529 / 84,589 | 1.00 | max 44.50 / 40.56 | 0 | |
| store, chunk 64 | cache 128 / 256 MiB | **152,985** / 152,355 | 1.00 | max 31.21 / 37.01 | 0 | |
| store, chunk 64 | memtable 64 MiB | 66,559 | 1.00 | 0.37 / 3.50 / **120.65** | **1** | 3.94 / 6.24 |
| store, chunk 64 | workers 2 | 84,180 | 1.00 | 0.65 / 1.24 / 37.47 | 0 | 4.46 / 6.22 |
| machine, 64 cmds/envelope, 32 submitters | default | **83,634** end to end | | 10.68 / 26.15 / 50.08 | 0 | 4.62 / 11.11 |
| machine, 1 cmd/envelope, 32 submitters | default | 6,581 (latency-bound) | | 0.26 / 0.93 / 51.88 | 0 | 3.00 / 8.45 |

Loadavg was 1.1–1.8. Machine preload (2M commands through the WAL) took 84.4 s. Store preload took 8.7 s.

**Decision:** `StoreConfig::default()` is unchanged. The plan's retune triggers (reopen > ~1 s per partition, or a ≥ 100 ms stall per 200k commands) did not fire. 32 MiB memtables reopen slower, 64 MiB memtables bring back the stall, and 2 workers are within noise at the cost of P extra threads.

## Phase gate

| command | result |
|---|---|
| `cargo nextest run --workspace` | 675 passed, 9 skipped (217.8 s, alongside the proptest) |
| `cargo test --doc --workspace` | green (16 passed) |
| `PROPTEST_CASES=1000 cargo test --release -p ostro-partition --test rebuild_digest` | **passed: 2 tests (1000 cases + fixed scenario), 5,040 s** (alongside the kill9 gate run) |
| `OSTRO_CRASH_ITERS=500 … crash_loop_kill9` | **passed: 500 iterations, 88,778 acks + 58,741 runs verified, max Inspect reopen 348 ms** (02-15: 2,865 ms), 4,364 s |
| `OSTRO_CRASH_ITERS=50 … crash_loop` (kill9 / torn / corruption) | passed: kill9 13,441 acks (max reopen 44 ms); torn tail 50/50 cut, 13,598 acks; corruption 49/49 detected (iteration 0 skipped, no sealed segment yet) |
| `OSTRO_CRASH_ITERS=50 --features failpoints … crash_loop_io_errors` | passed: 8,603 acks verified, 93 injected partition failures, no ACK lost |
| `cargo test -p ostro-wal` (vendored) / `--features failpoints` | 95 / 99 passed |
| `cargo test -p ostro-log` | all green (incl. torn_tail, corruption) |
| `bash scripts/ci-local.sh` (full, with musl) | **all 14 steps passed**: fmt, clippy, warning-free build, cargo test 436 s, failpoint suites 440 s, deny, vendored, deny fixtures, forbidden crates, workflow validation, zig, musl release (static, mimalloc) 35 s, musl zigbuild test 376 s |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug/perf] WAL recovery fsynced every retained segment on every reopen**
- **Found during:** Task 1 (input from 02-15, folded in by the orchestrator)
- **Issue:** every process start opens a new segment per partition, and recovery fsynced all of them. On WSL2 that costs about 0.55 ms per clean segment, so reopen grew linearly (741 ms at 300 restarts, 2.9 s peak in 02-15).
- **Fix:** fsync only the last non-empty segment. The durability argument is documented in the code. The torn-tail rule, the floor rule and the corruption classification are unchanged.
- **Files modified:** `crates/ostro-log/src/recovery.rs`, `crates/ostro-log/src/rf1.rs` (comment)
- **Verification:** ostro-log suites, vendored ostro-wal tests, all four crash legs at 50 iterations, kill9 at 500 iterations.
- **Commit:** 22b39be
- **Verifier note:** 02-07's must-have wording "every retained segment has been fsynced once" is now satisfied as "every retained segment is durable after recovery". The tail is fsynced here, and the other segments were fsynced earlier (on rotation, or as the tail at a previous recovery).

**2. [Rule 1 - Measurement bug] fjall journal size**
- fjall preallocates every journal file to 64 MiB (`set_len`), so file length, and even allocated blocks once the file is full, do not show the un-flushed amount. The reopen table reports journal files and allocated MiB as context only. The replay bound is described from fjall's source in storage-format §11.

**3. [Rule 3 - Blocking] Added `metrics` as an ostro-crashtest dev-dependency**
- Needed to capture `ostro_apply_batch_seconds` without a production hook. The workspace dependency was already pinned. Cargo.lock gained one edge.

### Plan items handled differently

- **crash-target throughput:** one load task per partition gives about 6.5k acks/s and fills only about 7.5 MiB of journal in 20 s. `--pipeline 64` (about 40k acks/s) reaches the cap. The 20 s run therefore uses `--pipeline 64 --blob-pct 0`.
- **CI:** the GitHub failpoint step already ran ostro-wal, ostro-blob, ostro-log, ostro-partition and ostro-crashtest (02-15), as explicit lines that match FAILPOINT_CRATES. It was not rewritten into a loop, and `.github/workflows/ci.yml` is unchanged.
- **Batch latency:** taken from the existing `ostro_apply_batch_seconds` histogram via a test recorder. No MachineHandle stats hook was needed.

### Open concern (prominent, per plan): store-level RMW below 90k cmds/s

- **Measured:** 80,832 cmds/s (chunk 64) and 76,286 (chunk 1024) at the default 16 MiB per-partition block cache, with 1M runs. Machine level is 83,634 end to end. S3's synthetic RMW measured 106.9k with fjall's default cache.
- **Likely cause:** block-cache misses, not the apply path. Apply does exactly 1.00 keyspace read per command. The rate jumps to 153k at 128 MiB, with a cliff between 64 and 128 MiB, because the 10% uniform picks touch the whole 1M-run `runs` keyspace.
- **Why not retuned:** the cache is per partition (P=16 by default), and fjall 3.1.13 has no public API to share one cache across databases. 128 MiB × 16 = 2 GiB is a product decision about memory footprint.
- **Recommendation:** size `cache_size` from a node-level memory budget (`budget / P`) in server configuration (Phase 4 ServerConfig or the Phase 5 perf plan 05-15). Re-measure on native NVMe.

### Todo

- The remaining reopen growth of about 0.5 ms per restart comes from many small per-restart WAL segments. Candidate fix: reuse or merge the tail segment instead of opening a new one per process start. That touches the vendored `GroupCommitWal` queue creation (research W4), so it is deferred and recorded in STATE.md pending todos.

## Requirements

- **STOR-05 complete.** Deterministic fjall apply, one `WriteBatch` per chunk, `persist(SyncAll)` checkpoint every 100 ms / 256 chunks, recovery from the state's own `applied_offset`, apply never passes HW (02-05/02-09/02-11). Now also: bounded reopen measured (< 1 s per partition), no ≥ 100 ms stall, one point read per command. The throughput shortfall is recorded above as a tuning concern. It is not part of STOR-05's wording.
- STOR-01..04 and 06..14 were already complete. They were re-verified by this gate.

## Commits

- 22b39be fix(02-16): recovery fsyncs only the last non-empty WAL segment
- df055fd test(02-16): reopen-time and RMW apply-throughput measurements (STOR-05)
- 34ad7f9 docs(02-16): freeze the Phase 2 on-disk format in docs/storage-format.md

## Self-Check: PASSED

Created files exist (reopen_time.rs, apply_throughput.rs, docs/storage-format.md); commits 22b39be, df055fd, 34ad7f9 present.
