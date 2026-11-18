---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 09
subsystem: engine
tags: [determinism, replay, divergence, golden, replay-corpus, guests]
requires:
  - "03-07: det-basic guest (shared workflow.rs with all five DIVERGE modes)"
  - "03-08: Runtime, FakeClock::with_step, RunSpec, Script, drive_to_completion, replay_journal"
provides:
  - "guests/det-divergent (det-basic workflow.rs with DIVERGE = true)"
  - "crates/ostro-engine/tests/determinism.rs: divergence_* (7), thousand_records, thousand_replays, golden_det_basic_journal"
  - "First replay-corpus entry: tests/snapshots/determinism__golden_det_basic_journal.snap"
affects: [03-11, phase-05 QA-02]
tech-stack:
  added: []
  patterns:
    - "One shared Runtime with a frozen FakeClock (step 0) gives reproducible journals across runs without a fresh engine per run"
    - "Golden journal rendering: `seq kind hash_hex|- ts_nanos len sha256[..8]` per event"
key-files:
  created:
    - guests/det-divergent/Cargo.toml
    - guests/det-divergent/src/lib.rs
    - crates/ostro-engine/tests/determinism.rs
    - crates/ostro-engine/tests/snapshots/determinism__golden_det_basic_journal.snap
  modified: []
key-decisions:
  - "Determinism tests share one Runtime whose FakeClock is frozen (with_step(DEFAULT_ACCEPTED_AT, 0)) instead of a fresh stepping FakeClock per run, because the clock belongs to the Runtime and the plan requires reusing one Runtime + Prepared for all 1,000 iterations"
  - "OSTRO_DET_ITERS overrides the iteration count (default 1000). The thousand_* tests take ~1.9 s together in the test profile, so no nextest slow group was needed"
requirements-completed: [ENG-03, ENG-04]
duration: 15min
completed: 2026-11-18
---

# Phase 3 Plan 09: Determinism Suite Summary

Roadmap Phase 3 SC1 now holds on a real component. The det-basic guest uses native `SystemTime::now`, std `HashMap` iteration order and `rand`. Recorded 1,000 times with the same seed and script, it produces byte-identical encoded journals. 1,000 replays of a recorded journal each append 0 events and return identical output. Replaying a det-basic journal with det-divergent fails as nondeterminism at the exact recorded seq, with expected vs actual in the DLQ reason, for all five divergence kinds.

## What was built

- **`guests/det-divergent`**: a cdylib with the same dependencies as det-basic. `lib.rs` is `const DIVERGE: bool = true; include!("../../det-basic/src/workflow.rs");`. det-basic's `workflow.rs` already implemented every mode (03-07), so it was not touched.
- **`tests/determinism.rs`**: one shared `Runtime` and the two `Prepared` modules, held in a `OnceLock`.
  - Helpers:
    - `record(mode)` drives det-basic with seed `[1; 32]`, accepted_at 1.7e18 and `Script::default()`.
    - `seq_of_step` finds a step's seq; `encoded` collects `encode_event` triples.
  - `divergence_changed_args`: seq is s2's seq, expected `step "s2"`, actual `step "s2x"`. `dlq_reason()` equals `nondeterminism: seq={seq} expected=.. actual=..` exactly, and 0 events are appended.
  - `divergence_extra`: seq is s2's seq, actual `step "extra"`.
  - `divergence_missing`: seq is loop-1's seq, actual `step "loop-2"`.
  - `divergence_reordered`: seq is loop-0's seq, actual `step "loop-1"`.
  - `divergence_early_return`: seq is loop-0's seq, actual is exactly `run returned`.
  - `divergence_same_module_no_error` (control): `Completed` with the same output and 0 appends.
  - `divergence_seed_matters`: seeds `[1;32]` and `[2;32]` give different s1 bytes, different journals and different outputs, and both runs complete.
  - `thousand_records`: 1,000 records, each byte-identical to the first with an identical outcome. Takes 1.25 s (~1.25 ms per run, 2 activations each).
  - `thousand_replays`: 1,000 replays, each returning the same `Completed` with 0 appends. Takes 0.57 s (~0.57 ms per replay).
  - `golden_det_basic_journal`: 13 events snapshotted with insta. A comment records the rules: this is the first replay-corpus entry, never blindly re-bless it, and an x86_64/aarch64 difference is a real bug.

## Verification

- `cargo test -p ostro-engine --test determinism`: 10 pass in 1.4 s with the default 1,000 iterations. A second run leaves `git status` clean (the snapshot is stable).
- `cargo clippy -p ostro-engine --all-targets --all-features -- -D warnings` and `cargo fmt -p ostro-engine --check` are clean. Guest clippy for det-divergent (wasm32-wasip2, `-D warnings`) is clean.
- `bash scripts/ci-local.sh --no-musl`: all steps passed (fmt, clippy, build, test, failpoints, deny, vendored, fixtures, workflow).

## Deviations from Plan

### Auto-fixed Issues / design choices

**1. [Rule 3 - Blocking] Frozen shared clock instead of a fresh FakeClock per run**
- **Found during:** Task 1
- **Issue:** The plan says to use "a fresh `FakeClock` per run" but also to reuse one `Runtime` for every iteration. The clock is a `RuntimeOptions` field, so the two conflict, and a shared stepping clock would make every run's timestamps different.
- **Fix:** All runs share `FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)`, the same technique 03-08 used for its conflict test. Live timestamps then depend only on the journal.
- **Commit:** 67412da

**2. Commit granularity**
- I wrote `determinism.rs` in full, then trimmed it to the Task 1 tests for the first commit and restored the full file for Task 2.

### Concurrency notes
- In the middle of the plan, the guests workspace failed to build because 03-10's `guests/sandbox` was half-written (manifest present, `src/lib.rs` missing). It resolved within about 45 s, and then I ran verification on the main tree. `guests/Cargo.toml` needed no edit because of the `members = ["*"]` glob.

## Observations (not bugs, for 03-11 / Phase 5 awareness)

- With a frozen clock, journal timestamps go backwards after an input: TimerFired is at accepted_at + 5 s, while the following WaitResolved and steps are at accepted_at. This follows the 03-05 rule, live ts = max(clock, last *command* ts), which ignores inputs. With a real clock the leader's commit time keeps the timestamps monotonic, but across nodes with clock skew (cluster phases) the guest's `now()` after `sleep` could read earlier than the timer's fire time. Consider including input ts in the max. That change would alter the golden snapshot, which is intended.

## Known Stubs

None.

## Requirements

- **ENG-03:** complete. Every divergence kind fails at the exact seq with expected vs actual in the `nondeterminism` DLQ reason. The DLQ-shaped outcome itself came from 03-08.
- **ENG-04:** was already complete (03-07). The 1,000-replay byte-identical outputs reconfirm it.

## Commits

- 67412da feat(03-09): det-divergent guest and divergence-at-exact-seq tests
- 169eeca test(03-09): 1,000-run record/replay determinism and golden det-basic journal

## Self-Check: PASSED

All four created files exist; commits 67412da and 169eeca are in git log.
