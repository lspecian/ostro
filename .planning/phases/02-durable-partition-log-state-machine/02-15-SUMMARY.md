---
phase: 02-durable-partition-log-state-machine
plan: 15
subsystem: storage
tags: [crash-test, kill-9, torn-tail, corruption, failpoints, rebuild, proptest, digest]
requires:
  - phase: 02-12
    provides: "Node (Leader/Inspect), startup events, tests/common open_node/copy_dir"
  - phase: 02-13
    provides: "live shuffler (cross-partition delivery in the workloads), settle-style quiescence"
  - phase: 02-14
    provides: "live timer service (TimerFired in the workloads)"
provides:
  - "crates/ostro-partition/tests/rebuild_digest.rs: STOR-06 rebuild property (proptest + fixed heavy scenario)"
  - "PartitionHandle::submit_with_offsets -> Vec<(Offset, ApplyOutcome)> (additive; submit delegates to it)"
  - "crates/ostro-crashtest: crash-target bin (ACK protocol), lib helpers (Rng, parse_acks, parse_failed, run_and_kill, open_node, scan_log, verify_acks, rebuild_and_compare, stateless_copy, non_empty_segments, record_spans, crash_iters)"
  - "tests/crash_loop.rs: crash_loop_kill9, crash_loop_torn_tail, crash_loop_corruption_detected, crash_loop_io_errors (failpoints), all #[ignore]"
affects: [02-16, 05-hardening-QA-01]
tech-stack:
  added: []
  patterns:
    - "ACK protocol: the target prints `ACK <p> <offset> <blake3 hex>[ RUN <id>]` only after an Accepted outcome (WAL fsync + apply commit); the checker ignores a torn last stdout line"
    - "Post-kill check = Inspect open -> every ACK offset in the verified log scan with the same hash + acked runs exist -> shutdown -> rebuild_and_compare (copy, delete state/, Inspect both, digest/state_hash/applied)"
    - "Torn tails are injected offline as zeros, a genuine record header + random bytes short of the declared length, or < 12 header bytes; random complete-looking garbage is corruption, not a torn write"
    - "Chunking leakage is checked on one log rebuilt with two different chunk sizes (the live services make a from-scratch replay produce a different log)"
key-files:
  created:
    - crates/ostro-partition/tests/rebuild_digest.rs
    - crates/ostro-crashtest/Cargo.toml
    - crates/ostro-crashtest/src/lib.rs
    - crates/ostro-crashtest/src/bin/crash-target.rs
    - crates/ostro-crashtest/tests/crash_loop.rs
  modified:
    - crates/ostro-partition/src/handle.rs
    - Cargo.lock
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
decisions:
  - "Chunking-never-leaks is asserted by rebuilding the same log with two different max_chunk_cmds and comparing both to the live state; replaying the op list from scratch is not deterministic at log level (shuffler batching, timer fired_at come from the live services/clock)"
  - "Torn-tail leg generates only shapes a partial write can produce (zeros, partial record, partial header); arbitrary garbage that parses as a complete bad record is corruption by the 02-07 rule and is not generated"
  - "ostro-crashtest has a failpoints feature, so it is in FAILPOINT_CRATES and the CI failpoint step (lint + unit tests; the loops stay #[ignore])"
  - "crash-target grants attempts by submitting AttemptGranted itself (n = 1 under its own epoch) so every accepted command gets an offset and an ACK line"
metrics:
  duration: ~4h wall (most of it the gate runs: proptest 1000 cases 100 min, kill9 500 iterations 113 min)
  completed: 2026-11-18
---

# Phase 2 Plan 15: Rebuild-Digest Property and Crash Harness Summary

Two things are new. First, a proptest for STOR-06: for random workloads, chunk sizes, checkpoint cadences and restarts, the live state equals a rebuild from the log, by digest, rolling hash and applied offset. Second, a new `ostro-crashtest` crate. Its `crash-target` process is killed with SIGKILL at random points under load. After each kill a checker confirms that every acknowledged command is still at its offset with the same bytes, that every acknowledged run exists, and that the live state equals a rebuilt one. The same loop also covers offline torn tails, bit flips in sealed segments, and injected fsync/commit failures. No bug turned up, so no apply or machine code changed.

## What was built

- **`tests/rebuild_digest.rs`** (ostro-partition)
  - Proptest with P=2, `max_chunk_cmds` 1..=64, `checkpoint_batches` 1..=300, and `checkpoint_every` of 1 ms, 50 ms or 1 h.
  - Each case runs 20–200 ops:
    - StartRun: keyed, delayed, or oversize inline (must be Rejected).
    - Grants.
    - EventsAppended: valid, stale token, or out-of-sequence, carrying ScheduleTimer, cross-partition signal and CompleteRun intents.
    - Raw invalid bytes appended to the log.
    - Restart: graceful, then reopen with a newly drawn MachineConfig.
  - The live shuffler and timer service run throughout. The node settles (outboxes empty, logs quiet for 150 ms) before every restart and at the end.
  - The finished directory is copied twice and every `state/` is deleted in both copies. The two copies are rebuilt in Inspect mode with two different chunk sizes. For every partition, live digest, `state_hash` and `applied` (= LEO) must equal both rebuilds.
  - There is also a fixed heavy scenario: chunk 1 with a checkpoint every batch, two restarts, rebuilt with chunk 1 and with chunk 1024.
- **`PartitionHandle::submit_with_offsets`**: a small additive API. `submit` now delegates to it.
- **`ostro-crashtest`** (new workspace crate under `crates/*`)
  - **`crash-target <dir> <seed> [--partitions N]`**: opens a Leader node with default config and SystemClock, then prints `READY`. One seeded load task runs per partition:
    - batches of 1–8 starts: about half keyed from 5,000 keys (so Duplicates happen), about 2% with 300 KiB inputs via `prepare_payload` (blob store);
    - AttemptGranted;
    - EventsAppended with timer, cross-partition signal and complete intents.
  - It prints ACK lines only for Accepted outcomes and flushes after every line. A submit error prints `FAILED <p> <reason>` and stops that partition. With the failpoints feature, it calls `fail::FailScenario::setup()`.
  - **lib**: the helpers listed in `provides`. `scan_log` also asserts there are no gaps between batches.
  - **`tests/crash_loop.rs`**: four `#[ignore]` legs (results below), plus a negative control proving the checker catches a wrong hash and a lost offset.
- **CI**
  - `ostro-crashtest` was added to `FAILPOINT_CRATES` in `scripts/ci-local.sh` and to the `.github/workflows/ci.yml` failpoint step.
  - A comment block next to `FAILPOINT_CRATES` lists the explicit crash-loop, proptest and phase-gate commands. The default gate stays fast and deterministic.

## Results (real runs, release, WSL2 ext4 tempdirs)

| Run | Result |
|-----|--------|
| `cargo test --release -p ostro-partition --test rebuild_digest` (24 cases + fixed) | 2 passed, 97 s |
| `PROPTEST_CASES=1000 … rebuild_digest live_state_equals_rebuilt_state` | **passed, 6010 s (1 h 40 min)**, about 6 s per case. It ran alongside the kill9 gate run. |
| `OSTRO_CRASH_ITERS=10 … crash_loop_kill9` | passed, 2,265 acks verified, 17 s |
| `OSTRO_CRASH_ITERS=500 … crash_loop_kill9` (phase gate) | **passed: 500 iterations, 76,693 acks verified, 51,296 runs verified, max Inspect reopen 2,865 ms, max target startup 3,460 ms; 6,807 s** |
| `OSTRO_CRASH_ITERS=50 … crash_loop_torn_tail` | passed, 13,503 acks verified. Every injected tail was reported `torn: true` and cut back to its pre-injection length. Each new term started at the recovered LEO, and appends after it were contiguous. |
| `OSTRO_CRASH_ITERS=50 … crash_loop_corruption_detected` | passed. 49 bit flips in sealed segments were each detected, failing only that partition, with a reason naming the segment file and `at byte <record start>`; the other partitions were Ready. Iteration 0 was skipped because no segment was sealed yet. |
| `OSTRO_CRASH_ITERS=50 --features failpoints … crash_loop_io_errors` | passed: 8,673 acks verified, 88 partition failures injected (`wal::fsync` / `state::commit` at 1%). Every ACK survived, and live state equalled the rebuild every time. |
| 10-iteration runs of every leg (the plan's verify commands) | all passed |

### Observation for 02-16 (reopen time)

Reopen time grows with total log size and with the number of segments. Each process start opens a new segment per partition, so after 500 kills there are about 500 segments per partition. Recovery reads and verifies every segment and fsyncs each retained one. Inspect reopen went from about 20 ms at iteration 0 to about 1.3 s at iteration 499 (about 28k offsets per partition), with a 2.9 s peak. The loop also slows down over time because `rebuild_and_compare` replays the whole log every iteration. This does not affect correctness, but it is direct input for 02-16's reopen-time measurement. Candidate improvements: fsync only the last segment(s) at recovery, and merge or avoid empty and small segments per restart.

## Deviations from Plan

### Auto-fixed Issues

None. The property and the crash loops found no mismatch, so no apply/machine fix and no shrunk regression case were needed.

### Design choices within the plan's latitude

1. **Chunking check:** the plan's "same scenario replayed with a different chunk size from scratch" is not reproducible at log level, because the shuffler and timers append on their own schedule. Instead, one log is rebuilt with two different chunk sizes and both rebuilds must equal the live state. The decision is recorded above.
2. **Torn-tail shapes:** the plan says "garbage or zero-filled partial record". Random garbage of 12 bytes or more without the WAL magic (or forming a complete bad record) is classified as corruption by 02-07's rule, which is the intended behaviour. The leg therefore generates zeros, a real record header followed by random bytes cut short of the declared length, or fewer than 12 random header bytes.
3. **Additive test-harness items:** `parse_failed`, `run_and_kill`, `verify_acks`, `non_empty_segments`, `record_spans` and `crash_iters` in the lib, and the checker negative control in `crash_loop_kill9`.
4. **CI registration:** `.github/workflows/ci.yml` gained one line (`ostro-crashtest` in the failpoint step), per the orchestrator's concurrency note. That file was not in `files_modified`.

### TDD note

Task 1 is test-only. It passed on the first run against the existing implementation, so the commit is a single `test(...)` commit with no RED/GREEN split. There was nothing to fix.

## Requirements

- **STOR-06 complete:** the rebuild property (1,000 cases), rebuild after kill -9 in all 500 iterations, and rebuild_basic (02-12).
- **STOR-02 complete:** reads verify CRCs (02-07/02-08). A flip in a sealed segment fails that partition loudly, naming the segment and byte, in 49 of 49 crash-loop cases plus the ostro-log suites.
- **STOR-03 complete:** the log end is recovered by a segment scan, and torn tails are cut before any append in 50 of 50 crash-loop cases, with no acknowledged command lost.
- **STOR-07 complete:** fail-stop per partition (02-02/02-08/02-11). End to end, injected fsync/commit failures were never acknowledged, and every ACK survived (88 injected failures).
- STOR-05 is left to 02-16 (fjall reopen, throughput and stall measurement).

## Commits

- c6991d2 test(02-15): rebuild-digest property over random workloads, chunkings, cadences and restarts (STOR-06)
- 96aa590 feat(02-15): ostro-crashtest crate, crash-target load generator, kill -9 loop (STOR-06)
- cfe022c test(02-15): torn-tail, sealed-segment corruption and failpoint I/O crash legs (STOR-02, STOR-03, STOR-07)

`bash scripts/ci-local.sh --no-musl`: all steps passed (fmt, clippy, warning-free build, cargo test 306 s, failpoint suites incl. ostro-crashtest 284 s, deny, vendored, workflow validation).

## Self-Check: PASSED

All 5 created files exist; commits c6991d2, 96aa590, cfe022c present.
