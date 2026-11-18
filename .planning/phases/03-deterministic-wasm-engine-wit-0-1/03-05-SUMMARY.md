---
phase: 03-deterministic-wasm-engine-wit-0-1
plan: 05
subsystem: engine
tags: [replay, determinism, nondeterminism, chacha20, hmac, base32, proptest, tdd]
requires:
  - phase: 03-01
    provides: "ostro_types::journal event types, crate::error::NonDeterminism/ProtocolViolation, env::{Clock, RandomStream}"
provides:
  - "ostro_engine::replay::ReplayState (cursor, command/patched/begin_step/complete_step/abandon_step/wait/finish/deliver_inputs, logical time, continue-as-new suggestion)"
  - "ostro_engine::replay::{CommandResult, StepBegin, ReplayError, WaitOutcome}"
  - "ostro_engine::replay::rng::{ChaChaStream, RngStreams, insecure_seed, uuid_v7} (golden-pinned)"
  - "ostro_engine::replay::token::{mint, parse, verify, TokenParts} (ot1_ await tokens, golden-pinned)"
affects: [03-07, 03-08, phase-4]
tech-stack:
  added: []
  patterns:
    - "Replay core is pure Rust: it takes precomputed (CommandKind, CommandHash) and never calls the codec or command_hash"
    - "TDD RED commit uses todo!() stubs so the shared crate keeps compiling for parallel plans"
    - "Golden vectors computed independently in Python (cryptography ChaCha20, hashlib, hmac, base64)"
key-files:
  created:
    - crates/ostro-engine/src/replay/cursor.rs
    - crates/ostro-engine/src/replay/wait.rs
    - crates/ostro-engine/src/replay/rng.rs
    - crates/ostro-engine/src/replay/token.rs
    - crates/ostro-engine/src/replay/tests.rs
  modified:
    - crates/ostro-engine/src/replay/mod.rs
key-decisions:
  - "patched() returns Result<bool, ReplayError> (not bool) so the open-step guard can reject it"
  - "Live event ts = max(clock, last guest-command ts): logical time never goes backwards"
  - "wait validates ops: each must be an already-issued op-scheduling command (or CANCELLATION_OP); wait(any, []) is a protocol violation; wait(all, []) resolves to []"
  - "finish() abandons an open step; deliver_inputs is rejected while a step is open"
  - "Await-token base32 decoder accepts only canonical encodings, so a flipped trailing char cannot verify"
requirements-completed: []  # core semantics for ENG-03, ENG-04, WIT-02..05 done; end-to-end closure needs 03-07 (host adapter) and 03-08/03-09 (DLQ, determinism suite)
duration: 30min
completed: 2026-11-18
---

# Phase 3 Plan 05: Replay Core Summary

This plan adds a pure-Rust replay core. It matches every guest command against the journal by kind family and hash, and reports a `NonDeterminism` at the exact journal seq. It also covers logical time, wait ordering, patch markers, the step guard and the continue-as-new suggestion. Per-run ChaCha20 streams and `ot1_` HMAC await tokens are pinned by golden vectors.

## Performance

- **Duration:** about 30 min
- **Completed:** 2026-11-18
- **Tasks:** 1 TDD feature (RED, GREEN, REFACTOR)
- **Files:** 6

## What was built

- **`ReplayState` (cursor.rs)**
  - `next_guest_command` skips input events.
  - `family_matches(EventKind, CommandKind)` checks the kind family, then the hash is compared. On a mismatch it returns `NonDeterminism { seq, expected: "<summary> #<hash8>", actual: "<actual()> #<hash8>" }`.
  - Live commands get `seq = next_seq`. Each one is stamped by the clock and pushed to both the journal and `pending`.
  - `finish()` reports unconsumed commands with actual `"run returned"`. A trailing terminal event is left to `command(Terminal)`.
  - Also here:
    - `patched`
    - `begin_step` / `complete_step` / `abandon_step` (reserved seq plus guard)
    - `deliver_inputs` (contiguous seqs, input events only, nothing pending)
    - `continue_as_new_suggested`
    - `next_command_hash_ver`
    - `insecure_seed`
    - `new_uuid_v7` (uses logical-now ms)
- **wait.rs:** a live wait returns the completed ops in the journal order of their completion events. `Any` resolves on at least one completion and `All` on every one; otherwise it returns `Pending` and appends nothing. On replay it returns the recorded `WaitResolved` list. `CANCELLATION_OP` resolves through `CancelRequested`. Also provides `completion_for` and `op_kind`.
- **rng.rs:** RFC 8439 ChaCha20 with key = seed and nonce = stream u32 LE ‖ 8 zero bytes, consumed in order. The four streams are independent. `insecure_seed` is sha256-domain-separated. `uuid_v7` follows RFC 9562.
- **token.rs:** the token is `ot1_` + base32 of (partition BE, run_id length + bytes, op BE, the first 10 bytes of HMAC-SHA256). `verify` uses `verify_truncated_left` (constant time). The canonical RFC 4648 base32 encoder and decoder are written by hand.

## Verification

- `PROPTEST_CASES=256 cargo test -p ostro-engine --lib replay` passes all 30 tests. I also ran it once with `PROPTEST_CASES=4096`, and that passed too.
- `replay_property` records 1 to 40 random commands with inputs between them, then replays either the same sequence or one with a single mutation:
  - Faithful replay appends nothing and leaves `is_replaying` false.
  - A swap, insert, delete or changed hash fails at `seqs[i]`.
  - Deleting the last command is caught by `finish()`.
  - An extra command at the end goes live (1 pending), which is a legitimate continuation.
- `cargo clippy -p ostro-engine --lib --profile test -- -D warnings` and `cargo clippy -p ostro-engine --lib -- -D warnings` are both clean.
- `grep -rn wasmtime crates/ostro-engine/src/replay/` returns nothing.

## Commits

- b478ba0 test(03-05): add failing replay core tests (RED: compiling `todo!()` stubs, all 30 failed)
- 9366fc8 feat(03-05): implement replay core (GREEN)
- f6ecf3a refactor(03-05): share open-step guard and protocol helper with wait

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] `patched` signature returns `Result<bool, ReplayError>`**
- **Issue:** The plan's API has `patched(..) -> bool`, but its own step_guard case requires `patched()` inside an open step to return `ReplayError::Protocol`.
- **Fix:** It now returns `Result<bool, ReplayError>`. 03-07 should use `?`.
- **Commit:** b478ba0 / 9366fc8

**2. [Rule 2 - Correctness] Extra guards beyond the plan text**
- **Changes:**
  - `wait` rejects ops that are not already-issued op-scheduling commands, because waiting on one would block forever.
  - `wait(any, [])` is rejected.
  - Live events are checked to belong to the issued command family.
  - `deliver_inputs` is rejected while a step is open, because the reserved seq would collide.
  - `new()` rejects a second `RunStarted`.
  - Live ts is clamped so it never goes below the last command ts.
- **Commit:** 9366fc8

**3. Added API**
- `next_command_hash_ver()` was already in the plan's implementation notes. `insecure_seed()` and `new_uuid_v7()` were added on `ReplayState` for the 03-07 adapter.

### Concurrency notes
- `cargo clippy -p ostro-engine --all-targets` currently fails in `tests/pool.rs`, which belongs to another plan (03-03) and is still in progress. The error is `Workflow` lacking `Debug`. I scoped clippy to the lib and lib-test targets and did not touch that file.
- `crates/ostro-engine/src/host/wasi/io.rs` (03-04) has a pending rustfmt diff. That is not my file, so I left it.

## Known Stubs

None.

## Next Phase Readiness

The 03-07 host adapter can wrap `ReplayState`. It hashes with `ostro_types::journal::command_hash` from 03-02, using `next_command_hash_ver()` on replay, and passes `(CommandKind, CommandHash)` in.

## Self-Check: PASSED

All 6 replay files exist, and commits b478ba0, 9366fc8 and f6ecf3a are present.
