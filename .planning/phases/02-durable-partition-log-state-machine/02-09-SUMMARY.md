---
phase: 02-durable-partition-log-state-machine
plan: 09
subsystem: storage
tags: [apply, fencing, determinism, rolling-hash, blake3, outbox, idempotency, fjall]
requires:
  - phase: 02-05
    provides: "ApplyBatch overlay + reads(), MetaUpdate, keys/values codecs, digest()"
  - phase: 02-06
    provides: "Command/Cmd, StartRun/AttemptGranted/EventsAppended, Derived intents, OutboxMsg::target_partition"
  - phase: 02-07
    provides: "ostro_log::VerifiedBatch::commands_with_offsets"
provides:
  - "ostro_partition::apply::{apply_command, apply_chunk, fold_hash, START_DELAY_OP}"
  - "apply::outcome::{ApplyOutcome, RejectReason, InvalidCode, Action} with frozen outcome codes"
  - "apply::cursor::Cursor (from_meta, epoch, meta_update; pub(crate) set_inbound_seen)"
  - "pub(crate) run::{start_run, enqueue_ready, remove_ready, load_run, store_run, local_run, terminal_status}, outbox::{emit, msg_target, msg_oversize}"
  - "stubs with fixed signatures: timer::apply_timer_fired, inbound::apply_inbound_batch, outbox::apply_outbox_truncated (02-10)"
affects: [02-10, 02-11, 02-12, 02-15]
tech-stack:
  added: [prost (ostro-partition dependency)]
  patterns:
    - "Validate everything, then write: rejections never touch the overlay"
    - "One keyspace point read per hot-path command; every other write is blind"
    - "Rolling hash H_n = blake3(H_{n-1} ‖ off_le ‖ code ‖ cmd_bytes) folded for every command, accepted or not"
key-files:
  created:
    - crates/ostro-partition/src/apply/mod.rs
    - crates/ostro-partition/src/apply/outcome.rs
    - crates/ostro-partition/src/apply/cursor.rs
    - crates/ostro-partition/src/apply/run.rs
    - crates/ostro-partition/src/apply/outbox.rs
    - crates/ostro-partition/src/apply/timer.rs
    - crates/ostro-partition/src/apply/inbound.rs
    - crates/ostro-partition/tests/apply_core.rs
  modified:
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/Cargo.toml
    - Cargo.lock
decisions:
  - "Outcome codes frozen: Accepted=0, Duplicate=1, Rejected=16+reason index (StaleEpoch 16 … Invalid 26); InvalidCode detail is not part of the hash code"
  - "Cursor.next = persisted applied_offset = next offset to apply (fresh store 0)"
  - "Command decode happens before the epoch gate (needed to recognise LeaderAnnounced): undecodable bytes are Invalid(Decode) regardless of envelope epoch"
  - "Added InvalidCode::BadIntent (missing/unknown CompleteRun status, any intent after CompleteRun, ScheduleTimer/CancelTimer using the reserved START_DELAY_OP) and InvalidCode::FieldTooLong (workflow/queue > 65535 bytes would panic the key codec)"
  - "Unknown ConflictPolicy value → Invalid(Decode); outbox target outside the partition count → Invalid(BadRunId)"
  - "Timer and children rows have empty values; dedupe row value = 18 raw run-id bytes; events = prost JournalEvent; payloads(run,0/1) = prost PayloadRef; outbox = prost OutboxMsg"
  - "RunRecord.created_at_ms = StartRun.requested_at_ms; inbox_next starts at 0"
metrics:
  duration: 25min
  completed: 2026-11-18
---

# Phase 2 Plan 09: Deterministic Apply Core Summary

This plan adds the pure apply function that drives every partition. `apply_command` covers epoch fencing (`LeaderAnnounced`), idempotent `StartRun` with a delayed-start timer, and logged `AttemptGranted` grants. It also applies fenced `EventsAppended`, which writes the Phase-2 `Derived` intents in the same step. Each command is checked completely before anything is written, does at most one keyspace read, enforces the 256 KiB inline payload limit, and is folded into a pinned blake3 rolling hash. All writes go through the `ApplyBatch` overlay.

## What was built

- **`apply/outcome.rs`:** `ApplyOutcome`, `RejectReason`, `InvalidCode` and `Action`, matching the interface block. Two `InvalidCode` variants were added (see Deviations). The module doc holds the frozen code table.
- **`apply/cursor.rs`:** `Cursor`.
  - `from_meta` maps the stored `applied_offset` to `next`.
  - `epoch()` returns the current epoch.
  - `meta_update()` drains the changed `inbound_seen` sources.
  - `pub(crate) set_inbound_seen` is there for 02-10.
- **`apply/mod.rs`:**
  - `fold_hash` is blake3 over `prev ‖ off_le ‖ code ‖ bytes`.
  - `apply_command` asserts `off == cur.next`, then decodes the command, runs the epoch gate and dispatches. It always folds the hash and advances `next`.
  - `apply_chunk` is a plain loop using each `VerifiedBatch`'s own envelope epoch.
  - `START_DELAY_OP = u64::MAX`.
- **`apply/run.rs`:**
  - `start_run` validates the run id (bad id, or another partition), the key string lengths, the parent id, the conflict policy and the inline input size. It then does **one** read: the dedupe row for a keyed start, otherwise the run row. A ready run gets a ready row and `EnqueueReady`; a delayed run gets a timer row at `START_DELAY_OP` and `ScheduleTimer`.
  - `AttemptGranted` checks, in order: token, run, terminal, token epoch, `token > current`. On success it sets the run to Running and blind-removes the ready row.
  - `EventsAppended` checks, in order: token, run, terminal, token epoch, token equals current, `from_seq`, contiguity and overflow, intent structure, payload sizes. Only then does it write the events and each intent in order. A StartChild message also writes a `children` row and increments `run.children`. `CompleteRun` stores the result payload, removes the ready row, emits `RunCompleted`, and emits `ChildCompleted` to the parent when `run.parent` is set.
- **`apply/outbox.rs`:** `emit` uses a dense per-target seq that starts at 1 and lives in `CursorMeta.outbox_next_seq`. Rows go to the same partition through the same code path. `msg_target` and `msg_oversize` serve as validators, and `apply_outbox_truncated` is a stub.
- **`apply/timer.rs`, `apply/inbound.rs`:** stubs with the exact signatures, returning `Rejected(Invalid(NotImplemented))`.
- **`lib.rs`:** adds `pub mod apply;` plus re-exports (`Action, ApplyOutcome, Cursor, RejectReason, apply_chunk, apply_command`). The `ostro_log` anchor was dropped because the crate is now used.

## Frozen outcome-code table (folded into the rolling hash)

| outcome | code |
|---|---|
| Accepted | 0 |
| Duplicate | 1 |
| Rejected(StaleEpoch) | 16 |
| Rejected(Fenced) | 17 |
| Rejected(SeqMismatch) | 18 |
| Rejected(SeqGap) | 19 |
| Rejected(RunNotFound) | 20 |
| Rejected(RunTerminal) | 21 |
| Rejected(RunExists) | 22 |
| Rejected(IdempotencyConflict) | 23 |
| Rejected(PayloadTooLarge) | 24 |
| Rejected(WrongPartition) | 25 |
| Rejected(Invalid(any)) | 26 |

The rolling hash golden value is `fold_hash([0;32], Offset(0), 0, b"x") = 2044af1ee71a81a4a6c36183d045adb1459b0d08fe1b1f911510794ae431f75e`. A first-principles `blake3::hash` of the concatenated input cross-checks it in the same test.

## Verification

- `cargo test -p ostro-partition --test apply_core`: 19 tests pass.
  - `fencing_leader_announced`, `fencing_attempts`, `fencing_zombie`
  - `events_seq`, which includes the all-or-nothing check: an oversize event 3 leaves no event 2 row and no timer row
  - `start_run_dedupe`, `start_run_delayed`, `payload_limits`, `complete_run`, `complete_run_then_more_intents_is_invalid`, `enqueue_ready_intent`, `malformed`
  - `rolling_hash_golden`, `rolling_hash_outcome_codes_frozen`
  - `rolling_hash_chunking_invariant`: a 200-command mix with codes 0, 1, 16, 17 and 26, applied in chunks of 1, 7 and 200, gives equal hash, `digest()` and outcomes
  - `rolling_hash_rejections`, `reads_budget`, `outbox_emit` (persisted and reloaded on reopen), `outbox_child`, `apply_chunk_multi_batch`
- `cargo test -p ostro-partition`: all suites pass.
- `cargo clippy -p ostro-partition --all-targets -- -D warnings` (with the determinism lint) is clean, both with and without `--features failpoints`.
- `cargo build --workspace` produces no warnings.
- `cargo fmt -p ostro-partition --check` is clean. The only workspace fmt diffs are in `crates/ostro-log/tests/replicated_log_conformance.rs`, which belongs to 02-08's in-progress work and is outside this plan.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Added `prost` to ostro-partition dependencies**
- **Found during:** Task 1
- **Issue:** Event, payload and outbox rows are prost-encoded (`encode_to_vec`), and `prost` was not a dependency of ostro-partition.
- **Fix:** Added `prost.workspace = true` to `crates/ostro-partition/Cargo.toml`. The only `Cargo.lock` change is one dependency line.
- **Commit:** ebe2aed

**2. [Rule 2 - Correctness] `InvalidCode::FieldTooLong` and `InvalidCode::BadIntent`**
- **Issue:** `keys::put_str` panics on strings longer than 65535 bytes, so an unchecked workflow or queue would make apply panic on malformed input. The spec also had no code for an intent after `CompleteRun` (which would reopen a terminal run), an unknown `TerminalStatus`, or a timer intent that uses the reserved `START_DELAY_OP`.
- **Fix:** Added both variants. This is additive. Both map to hash code 26, like every other `Invalid`.
- **Commit:** 32b06d0

**3. [TDD ordering] Task 2's `fold_hash` and `apply_chunk` were implemented in Task 1**
- The plan allowed this. As a result, Task 2's RED step failed only on the placeholder golden value. The golden hex was taken from the implementation and cross-checked against a first-principles `blake3::hash` in the same test. No external blake3 tool was available.

## Notes for later plans

- **02-10:**
  - Replace the bodies of `timer::apply_timer_fired`, `inbound::apply_inbound_batch` and `outbox::apply_outbox_truncated`.
  - Reuse `run::start_run(b, cur, s, dedupe, actions)` for StartChild and CronStart.
  - Use `cur.set_inbound_seen` (and remove its `#[allow(dead_code)]`).
  - A delayed start's timer is `(ready_at_ms, run, START_DELAY_OP)`. Firing it should call `run::enqueue_ready`.
  - `RejectReason::RunExists` and `SeqGap` are defined but not yet produced.
- **02-11:** `apply_chunk` returns per-offset outcomes for `PendingAcks`. `Action`s are produced only by accepted commands and must be acted on after `ApplyBatch::commit`.
- **CompleteRun** does not delete the run's pending timers. A later `TimerFired` on a terminal run should be a no-op `Duplicate` (02-10).

## Requirements

None are marked complete. This plan delivers the apply-side half of each requirement:
- **STOR-10:** `LeaderAnnounced` epoch gate and `(epoch, attempt)` token validation at apply are done. Appending `LeaderAnnounced` at each new term is 02-12.
- **STOR-11:** validation happens at apply and the outcomes are defined. The ack rendezvous (acks sent after apply) is 02-11.
- **STOR-14:** oversize inline payloads are rejected at apply (and blob storage exists from 02-04). Blob offload on the submit path is 02-11.
- **STOR-05:** the deterministic apply core is done. The apply thread, checkpoints and rebuild are 02-11, 02-12 and 02-15.

## Commits

- ebe2aed test(02-09): add failing apply core tests (fencing, dedupe, seq, payloads)
- 32b06d0 feat(02-09): deterministic apply core with epoch/attempt fencing and Derived intents
- 65b790d test(02-09): add rolling hash, read budget, outbox and apply_chunk tests
- 12a1468 feat(02-09): pin rolling hash golden; chunk-invariant apply verified

## Self-Check: PASSED

All 8 created files exist; commits ebe2aed, 32b06d0, 65b790d, 12a1468 present.
