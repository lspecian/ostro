---
phase: 01-foundation-vendoring-spikes
plan: 08
subsystem: spikes-decisions
tags: [fjall, redb, wasmtime, spike-s2, spike-s3, crash-test, decisions, found-06]
requires:
  - phase: 01-02
    provides: "s3-redb-bench (redb tiers, fjall t1/t2/t3, redb crash test), S3 report with FAIL verdict"
  - phase: 01-05
    provides: "S2 measure.sh protocol, S2 report with resource recommendation"
  - phase: 01-07
    provides: "green local CI gate (all builds done before the quiet run)"
provides:
  - "s3-redb-bench: --rmw read-modify-write workload with --preload, crash-test/crash-child --engine fjall, row-level torn-batch check for both engines"
  - "spikes/s3-redb-bench/confirm.sh: interleaved redb/fjall T2 plain+RMW x3 plus 5 crash runs per engine"
  - "measure.sh confirm phase (instantiate n=1000 + cold x10 per guest/allocator)"
  - "docs/spikes/data/S3/confirm.jsonl, crash-confirm.jsonl; docs/spikes/data/S2/confirm.jsonl"
  - "Confirmation sections in both spike reports"
  - "PROJECT.md Key Decisions: step handle (resource), state store (fjall 3.1.13), Wasmtime pin policy"
affects: [phase-02-storage STOR-05, phase-03-engine ENG-02 ENG-08, phase-09-sdks, phase-10-cli CLI-04, phase-14-release]
tech-stack:
  added: []
  patterns: ["interleaved cross-engine reps under the same load", "wait-for-quiet (load < 2.0, max 120 s) before decision-critical runs", "row-count torn-batch check (rows == applied_offset)"]
key-files:
  created:
    - spikes/s3-redb-bench/confirm.sh
    - docs/spikes/data/S3/confirm.jsonl
    - docs/spikes/data/S3/crash-confirm.jsonl
    - docs/spikes/data/S2/confirm.jsonl
  modified:
    - spikes/s3-redb-bench/src/main.rs
    - spikes/s2-guests/measure.sh
    - docs/spikes/S3-redb-durability.md
    - docs/spikes/S2-guest-toolchains.md
    - docs/spikes/data/S3/env.txt
    - .planning/PROJECT.md
    - .planning/STATE.md
key-decisions:
  - "Step handle: resource (S2; engine must not rely on HostStep::drop)"
  - "State store: fjall 3.1.13 replaces redb for STOR-05. Quiet re-run: redb T2 67.5k plain / 30.9k RMW (FAIL); fjall T2 233.8k plain / 106.9k RMW (PASS); fjall crash 5/5"
  - "fjall crash semantics: state reopens at a batch-consistent applied_offset >= last fsynced checkpoint (OS-buffered commits survive a process kill); recovery must replay the WAL from the state's own applied_offset"
  - "Wasmtime pin policy: track latest major monthly, pinned exactly (=50.0.2), 1.0 on 60 LTS, 48 LTS fallback, bump rust-toolchain.toml with Wasmtime MSRV"
  - "Python embedded by default (6.5 MB RSS/instance); pools sized per language class (Python 38 core instances/instance)"
metrics:
  duration: ~14min
  completed: 2026-11-18
  tasks: 2
  files: 11
---

# Phase 1 Plan 08: Quiet-machine confirmation and Phase 1 decisions Summary

The quiet re-run confirmed the 01-02 result: redb 4.6 fails the 90k cmds/s rule. It reached 67.5k on the insert-only workload and 30.9k on the new read-modify-write one. fjall 3.1.13 passes both 01-02 preconditions: 106.9k cmds/s with read-before-write against a db of 1M+ runs, and 5/5 kill -9 crash runs with a row-level torn-batch check. The three Phase 1 decisions are now logged in PROJECT.md: step handle = resource, state store = fjall, and the Wasmtime pin policy (track monthly releases, pinned exactly, ship 1.0 on the 60 LTS).

## Tasks

| # | Task | Commit |
|---|------|--------|
| 1 | Quiet-machine confirmation runs; fjall crash mode and RMW bench; confirmation sections in both reports | c927f69 |
| 2 | Three decision rows in PROJECT.md; STATE.md gaps resolved and new concerns added | e5157ba |

## Key results

**S3** (load 0.19–1.34 for the bench runs, 1.29–1.50 for the crash runs, nproc 12):

| T2, median of 3, interleaved | cmds/s | steps/s | rule |
|---|---|---|---|
| redb plain | 67,477 | 22,492 | FAIL |
| fjall plain | 233,784 | 77,928 | PASS |
| redb RMW (1M preload) | 30,947 | 10,316 | FAIL |
| fjall RMW (1M preload) | **106,873** | 35,624 | **PASS** (1.19x margin) |

- **Crash (5 runs per engine, now also checking `runs` rows == applied_offset and outbox last == applied − 1):**
  - redb: 5/5, gap 0, reopen 83–97 ms.
  - fjall: 5/5. The reopened state is 3.3k–16.4k commands ahead of the last fsynced checkpoint (buffered commits survive a process kill) and is always batch-aligned and row-exact. Reopen takes 3.2–3.4 s (journal replay).
- **fjall caveats carried into Phase 2:** journal-replay reopen time, one ~120 ms write stall per RMW run (p99 < 0.85 ms), and a thin RMW margin.

**S2** (load 0.18–0.24): instantiate p50 and cold start match the main run within noise in all six guest × allocator cells. Examples: py 440/445 µs, ts 19.4/9.9 µs, rust 16.5/9.1 µs (on-demand/pooling); cold start py 5.07 ms, ts 2.87 ms, rust 0.58 ms. The NFR-2.3 conclusions did not change.

## Deviations from Plan

### Orchestrator-directed additions (beyond the written plan)

**1. fjall kill -9 crash test (precondition 1)**
- `crash-child`/`crash-test` take `--engine redb|fjall`.
- The fjall child commits `checkpointed_offset`, then calls `persist(SyncAll)`, then prints CKPT.
- redb was re-run with the same, stronger row-level check.
- **Note:** the orchestrator's wording "applied_offset == last reported durable checkpoint" describes redb's behaviour. fjall legitimately keeps commits that were flushed to the OS buffers. The correct invariant is a consistent prefix at ≥ the last durable checkpoint, and it held 5/5. This is documented in the report and the decision row, with the resulting Phase 2 recovery rule: replay from the state's own `applied_offset`.
- Commit: c927f69

**2. Read-modify-write benchmark (precondition 2)**
- Added `--rmw`, `--new-ratio` and `--preload`: read the run row, write it back, move it in the status index.
- The fjall path reads through a per-batch overlay because a WriteBatch has no read-your-writes.
- Ran interleaved with redb under the same load.
- Commit: c927f69

### Auto-fixed Issues

**3. [Rule 1 - Bug] Crash-test "gap 0" was partly timing luck**
- **Found during:** Task 1 smoke test.
- **Issue:** a redb smoke run showed `checkpointed_offset` > the last CKPT line, because the child was killed between the Immediate commit and printing CKPT.
- **Resolution:** no code change needed. The assertion is ≥, which is correct. The report explains why the gap can be non-zero.

**4. [Rule 2 - Correctness] Row-level torn-batch check**
- `applied % 64 == 0` alone cannot detect a meta/data mismatch, so the test now also requires the `runs` row count to equal `applied_offset` and the last outbox key to equal `applied_offset − 1`.
- Commit: c927f69

### Other

- Added `measure.sh confirm` instead of ad-hoc commands, so the S2 confirmation can be repeated.
- `.planning/PROJECT.md`: the Active and Constraints lines and the original log-first row now say fjall replaces redb, so the document stays consistent.
- `REQUIREMENTS.md`: the STOR-05 wording now carries a note that it is superseded.
- Per the orchestrator, ROADMAP.md, STATE.md and REQUIREMENTS.md were edited by hand. `roadmap update-plan-progress` and `state advance-plan` were not used.
- The spike crate was already not rustfmt-clean before this plan, and it is outside the CI fmt gate. It was left unformatted.

## Verification

- Task 1 and Task 2 automated checks: pass.
- Phase gate: `cargo build --workspace`, `cargo test --workspace` (0 failures), `cargo deny check` (advisories, bans, licenses and sources ok), `check-vendored.sh` and `check-deny-fixtures.sh` all pass.
- FOUND-06 grep (`Step handle|redb vs fjall|Wasmtime pin`): pass.
- **FOUND-06: complete.** FOUND-05 stays Partial (remote CI is human_needed).

## Self-Check: PASSED
