---
phase: 01-foundation-vendoring-spikes
plan: 02
subsystem: storage-spike
tags: [redb, fjall, durability, benchmark, spike-s3, crash-test]
requires: []
provides:
  - "spikes/s3-redb-bench: apply_batch emulation bench (redb tiers t1/t2/t3/t2-2pc/t2-qr, fjall t1/t2/t3 behind --features fjall, kill -9 crash-test)"
  - "docs/spikes/S3-redb-durability.md: S3 report with decision-rule verdict (FAIL for redb) and fjall comparison"
  - "docs/spikes/data/S3/: env.txt, results.jsonl, crash.jsonl, fjall.jsonl"
affects: [01-05, 01-08, phase-02-storage STOR-05]
tech-stack:
  added: ["redb =4.6.0 (spike only)", "fjall =3.1.13 (spike only, optional feature)"]
  patterns: ["standalone excluded spike workspace with empty [workspace]", "paired interleaved reps for cross-engine comparison under the same load"]
key-files:
  created:
    - spikes/s3-redb-bench/Cargo.toml
    - spikes/s3-redb-bench/Cargo.lock
    - spikes/s3-redb-bench/src/main.rs
    - spikes/s3-redb-bench/run.sh
    - docs/spikes/S3-redb-durability.md
    - docs/spikes/data/S3/env.txt
    - docs/spikes/data/S3/results.jsonl
    - docs/spikes/data/S3/crash.jsonl
    - docs/spikes/data/S3/fjall.jsonl
    - docs/spikes/data/S3/.gitignore
  modified: []
decisions:
  - "S3 decision rule: FAIL for redb 4.6. T2 median 62,404 cmds/s (20,801 steps/s) vs 90,000 threshold. The quieter paired re-run gave 65,831. Even T3 (no fsync) only reached 90.6k, so the limit is CPU, not fsync."
  - "fjall 3.1.13 T2 analogue (WriteBatch per batch + persist(SyncAll) every 100 ms) gave 242,412 cmds/s, 3.7x redb. Recommended for STOR-05, conditional on a fjall crash test and a read-before-write benchmark. Needs user confirmation as part of the FOUND-06 decision logging."
  - "redb T2 crash semantics: after kill -9 the db reopens and applied_offset == last durable checkpoint (gap 0, 5/5). None commits are always discarded on reopen."
metrics:
  duration: "~22 min"
  completed: 2026-11-18
  tasks: 2
  files: 10
---

# Phase 1 Plan 02: Spike S3 redb durability tiers Summary

redb 4.6 sustains only about 62k commands/s in the planned None + 100 ms Immediate-checkpoint mode, so it fails the locked 90k rule. The kill -9 crash sanity passed 5/5. The triggered fjall 3.1.13 comparison reached 242k commands/s under the same workload.

## What was built

- **`spikes/s3-redb-bench`** is a standalone workspace (empty `[workspace]`), built with `cargo +1.100.0`. Subcommands:
  - `bench --engine redb|fjall --tier ...`: B=64, 3 inserts + 1 delete per command, `applied_offset` per batch, random 16-byte keys. Outputs one JSON line with throughput, commit/txn/checkpoint p50/p99/max, db size and loadavg before/after.
  - `crash-child`: runs t2 forever and prints `CKPT <offset>` after each Immediate commit.
  - `crash-test`: SIGKILL after 3 s, reopen with a repair-callback counter, then assert the durable checkpoint and that no batch is torn.
- **`run.sh`** captures the environment and runs 3 reps x 5 tiers plus 5 crash tests. Modes:
  - `--quick`: smoke run, writes the uncommitted `quick.jsonl`.
  - `--fjall`: paired reps of redb t2 and fjall t1/t2/t3.
  - Guards: it refuses db dirs under /mnt, on tmpfs/9p, or inside the repo.

## Key results

| Tier | median cmds/s | steps/s | notes |
|------|---------------|---------|-------|
| redb T1 | 9,863 | 3,288 | commit p99 15.7 ms |
| redb T2 | 62,404 | 20,801 | checkpoint p50 53 ms |
| redb T3 | 90,612 | 30,204 | no-fsync upper bound |
| redb T2-2pc | 63,623 | 21,208 | noise-level cost |
| redb T2-qr | 61,393 | 20,464 | noise-level cost, instant recovery |
| fjall T2 | 242,412 | 80,804 | checkpoint p50 5 ms |

The main run was taken at load average 5–7 (concurrent wave-1 builds). The fjall run with its paired redb T2 was taken at 1.6–2.5. All load values are recorded in the raw data.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] redb 4.6 2PC/quick-repair API location**
- **Found during:** Task 1
- **Issue:** The plan assumed `Database::builder().set_two_phase_commit/set_quick_repair`. In redb 4.6 these are `WriteTransaction` methods.
- **Fix:** Both are applied per transaction to every txn in the t2-2pc and t2-qr tiers. This is documented in the code and the report.
- **Commit:** c483670

**2. [Rule 2 - Correctness] run.sh refuses tmpfs/9p/in-repo db dirs**
- **Issue:** The /mnt refusal alone would still let a tmpfs `/tmp` produce meaningless fsync numbers.
- **Fix:** run.sh now checks the filesystem type with `df --output=fstype`, and it refuses db dirs inside the repo.
- **Commit:** c483670

**3. [Plan-directed fallback] fjall variant built and run**
- The rule failed, so Task 2 step 3 was executed: `--engine fjall` (t1/t2/t3 analogues) and `run.sh --fjall`, with redb t2 interleaved so both engines saw the same load.
- **Commit:** 64ca31d

**4. Orchestrator instruction:** `roadmap update-plan-progress` and `requirements mark-complete FOUND-06` were not run. FOUND-06 also depends on 01-05 and 01-08. ROADMAP.md was edited by hand.

## Deferred Issues / Open items

- A fjall crash test (port `crash-test` to `--engine fjall`) is required before adopting fjall.
- A read-before-write workload variant is needed to compare point-read cost (LSM vs B-tree).
- Plan 01-08 runs a quiet-machine confirmation run of T2 for both engines.
- **Decision for the user:** switching STOR-05 from redb to fjall is a storage-engine change and needs confirmation when the FOUND-06 decisions are logged.

## Commits

- c483670: feat(01-02): add redb apply_batch durability-tier bench and crash test
- 64ca31d: feat(01-02): run spike S3 and report redb durability tiers vs fjall

## Self-Check: PASSED
