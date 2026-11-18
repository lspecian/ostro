---
phase: 02-durable-partition-log-state-machine
plan: 11
subsystem: storage
tags: [apply-machine, high-watermark, fjall, checkpoint, acks, submit, blob-offload, fail-stop, failpoints]
requires:
  - phase: 02-04
    provides: "BlobStore::{open, put, get, contains, path_of} (durable put, sha-verified get)"
  - phase: 02-08
    provides: "ReplicatedLog trait, Rf1Log, LogNode::open, HW watch, log health watch, StaleEpoch fencing"
  - phase: 02-09
    provides: "apply_chunk, Cursor, ApplyOutcome/RejectReason/Action, frozen outcome codes"
provides:
  - "ostro_partition::machine::{MachineConfig (Default 1024 cmds/1 MiB/100 ms/256), PartitionHealth, MachineHandle::{spawn, applied, durable_checkpoint, health, subscribe_actions, set_emit_actions, check_healthy, force_checkpoint, wait_applied, shutdown}}"
  - "ostro_partition::acks::{PendingAcks::{new, register, resolve, fail_all}, ACK_RETAIN, AckReceiver}"
  - "ostro_partition::handle::{PartitionError, PartitionHandle::{new, partition, set_epoch, epoch, submit, submit_one, grant_attempt, prepare_payload, fetch_payload, read_run, reader, machine, log, store, blobs}}"
  - "tests/common: open_single, open_with(dir, cfg, announce, wrap), rebuild_digest, TestPartition::{close, applied, durable}, start_run/start_cmd/new_run; tests/common/manual_hw.rs ManualHwLog<L>"
  - "ostro-partition feature failpoints = [dep:fail, fail/failpoints, ostro-log/failpoints, ostro-blob/failpoints]; in FAILPOINT_CRATES + CI failpoint step"
affects: [02-12, 02-13, 02-14, 02-15, 02-16]
tech-stack:
  added: []
  patterns:
    - "Apply thread = std::thread + rt.block_on only for log reads and a select! wait (HW change, control msg, log health, checkpoint deadline)"
    - "Publish order after commit: Healthy (if caught up) -> applied watch -> acks.resolve -> actions broadcast -> maybe checkpoint"
    - "Fail-stop: one set_failed (send_if_modified) + acks.fail_all; pending control replies get Err; thread exits; panics caught and turned into Failed"
    - "submit: health check -> append -> register -> await each receiver in a biased select against health Failed"
key-files:
  created:
    - crates/ostro-partition/src/machine.rs
    - crates/ostro-partition/src/acks.rs
    - crates/ostro-partition/src/handle.rs
    - crates/ostro-partition/tests/common/mod.rs
    - crates/ostro-partition/tests/common/manual_hw.rs
    - crates/ostro-partition/tests/apply_hw.rs
    - crates/ostro-partition/tests/ack_after_apply.rs
    - crates/ostro-partition/tests/blob_offload.rs
    - crates/ostro-partition/tests/fatal_io_state.rs
  modified:
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-partition/Cargo.toml
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
decisions:
  - "checkpoint_batches counts committed chunks (one ApplyBatch each), not log batches; a checkpoint happens only when the state is dirty (applied > durable), except force_checkpoint which always persists"
  - "Acks resolve before the checkpoint: an ack means committed to fjall (buffered), not persisted; durability comes from the WAL (HW) plus replay from the state's applied_offset. So a submit whose chunk committed may return Ok even if the following persist fails"
  - "Graceful shutdown drains to the HW seen when the request arrives, checkpoints, then fail_all('partition shut down') so late submits get Err, never hang; dropping every MachineHandle does the same without a reply"
  - "Any log read error (incl. empty read below HW or base != cursor.next) fails the machine, as does a batch ending above HW ('apply passed HW')"
  - "PendingAcks: registering an offset below applied that is not in ready (evicted or already claimed) -> AckLost; unresolved waiters below applied (defensive) -> AckLost; after fail_all every register returns Failed"
  - "grant_attempt maps a (never expected) Duplicate outcome to Rejected(Fenced{current: None}); fetch_payload maps a malformed blob ref to BlobError::NotFound"
  - "PartitionHandle clones share the epoch (Arc'd AtomicU32); added blobs() accessor and MachineHandle::check_healthy (additive to the interface)"
  - "Machine publishes Healthy before the catch-up offset; test helpers additionally wait for health != Starting (no-replay opens never change `applied`)"
metrics:
  duration: 30min
  completed: 2026-11-18
---

# Phase 2 Plan 11: Apply Machine, Acks, PartitionHandle Summary

A single partition now runs end to end: submit -> durable log -> HW-capped apply into fjall -> ack. `MachineHandle::spawn` starts one `ostro-apply-p{N}` thread per partition. The thread reads verified batches only below HW, applies each chunk through `apply_chunk` into one `ApplyBatch` committed together with `applied_offset`, the rolling hash and the cursor, then resolves acks and checkpoints (`persist(SyncAll)`) every 100 ms / 256 chunks, on demand and on shutdown. Any storage or log failure fails the partition, and nothing in flight is ever acknowledged as success.

## What was built

- **machine.rs:**
  - Loop order: control messages, log health check, shutdown target check, then apply if `HW > cursor.next`, otherwise mark Healthy, check the checkpoint cadence and block in `rt.block_on(select!)`. The select wakes on an HW change, a control message, a log health change, or the checkpoint deadline (only when dirty).
  - Each chunk is capped at `max_chunk_bytes`/`max_chunk_cmds` and invariant-checked (base == next, no batch end > HW).
  - Metrics, labelled by partition: `ostro_apply_batches_total`, `ostro_apply_commands_total`, `ostro_apply_batch_seconds`, `ostro_state_checkpoint_seconds`.
  - `Instant::now` is used only for the cadence and the metrics, each with `#[allow(clippy::disallowed_methods)]` and a comment.
- **acks.rs:** a two-sided `PendingAcks` rendezvous guarded by a std `Mutex` (never held across `.await`). Retention of resolved-but-unclaimed outcomes is bounded by `ACK_RETAIN`.
- **handle.rs:**
  - `PartitionError` as specified.
  - `PartitionHandle` provides `submit`/`submit_one` (one envelope, outcomes in order and only after apply), `grant_attempt` (next `(epoch, n)` from the run record), `prepare_payload` (inline up to 256 KiB, otherwise `spawn_blocking` `BlobStore::put` before append), `fetch_payload` (sha-verified blob get) and read helpers.
- **Test infrastructure:**
  - `open_single`/`open_with`: store, LogNode with P=1 and floor = applied, machine, catch-up, Healthy, then epoch = latest + 1 and `LeaderAnnounced`, or no announce.
  - `rebuild_digest` replays the log into a fresh store.
  - `ManualHwLog<L>` publishes `min(inner HW, cap)`.

## Verification

- `cargo test -p ostro-partition`: all suites pass. New suites: apply_hw 5, ack_after_apply 6, blob_offload 4.
- `cargo test -p ostro-partition --features failpoints -- --test-threads=1`: all pass, including fatal_io_state 3/3. fatal_io_state passed 4 runs in a row.
- `cargo clippy -p ostro-partition --all-targets [--features failpoints] -- -D warnings`: clean.
- `cargo fmt --all --check`: clean.
- `bash scripts/ci-local.sh --no-musl`: every leg passes, including the failpoint suites with ostro-partition.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Race between the catch-up offset and Healthy**
- **Found during:** Task 2 (fatal_io_state reopen check)
- **Issue:** On reopen with nothing to replay, `applied` never changes. The `Starting -> Healthy` flip happened on the thread's next loop iteration, so an observer could see the caught-up offset while health was still `Starting`.
- **Fix:** The machine now publishes Healthy before the `applied` watch when a chunk catches up to HW. The test helper also waits for `health != Starting`.
- **Files modified:** crates/ostro-partition/src/machine.rs, crates/ostro-partition/tests/common/mod.rs
- **Commit:** e438060

**2. [Rule 2 - Correctness] Additional safety beyond the spec**
- A panic in the apply thread is caught and turned into `Failed` plus `fail_all`, so submitters can never hang.
- Graceful shutdown fails late registrations with `Failed`.
- `submit` checks machine health before appending, so it does not append to a log whose state machine is already failed.
- The machine checks `base == cursor.next` and fails cleanly instead of reaching the `apply_command` assert.

**3. Failpoints feature union**
- The plan specified `["dep:fail", "ostro-log/failpoints", "ostro-blob/failpoints"]`. The orchestrator and the Phase 2 convention require `fail/failpoints` (without it the `state::*` failpoints compile to no-ops). The feature uses the union of both.

**4. CI workflow edited (not in files_modified)**
- `.github/workflows/ci.yml` gained the `ostro-partition` failpoint line, which must stay in sync with FAILPOINT_CRATES. The orchestrator requested this.

### TDD note
- Task 1 had a real RED commit (61c4ff1): the tests did not compile without the modules.
- `prepare_payload`/`fetch_payload` were written in Task 1 along with the rest of `handle.rs`, so blob_offload passed on its first run.
- fatal_io_state was genuinely RED (7c05a25): the `state::*` and `wal::*` failpoints were inactive until the Cargo feature was fixed in e438060.

## Requirements

- **STOR-11** complete. Validation at apply came in 02-09; this plan adds acks sent only after the state commit, with rejections surfaced as `Rejected`.
- **STOR-14** complete. The durable blob store came in 02-04 and the apply-side oversize rejection in 02-09; this plan adds blob offload before append and sha-verified reads.
- **STOR-05** not marked. The apply thread and checkpoint cadence are done here; the rebuild path (02-12) and the rebuild-digest proptest and crash loops (02-15) remain.
- **STOR-07** not marked. Log and state fail-stop are now covered online; the offline crashtest legs in 02-15 remain.

## Notes for later plans

- **02-12 (Node):** reuse `open_with` as the template for startup ordering. Call `set_emit_actions(false)` during catch-up, and use `wait_applied` / `health()` for the ReadyBarrier.
- **02-14 (retention):** call `force_checkpoint()` before trimming.
- **Waiting for state:** `MachineHandle::health()` starts as `Starting`. Use `wait_applied` rather than polling `applied`.

## Commits

- 61c4ff1 test(02-11): add failing HW-capped apply, checkpoint cadence and ack-after-apply tests
- 6f7f4d9 feat(02-11): apply thread, ack rendezvous, PartitionHandle submit/grant (STOR-05, STOR-11)
- 7c05a25 test(02-11): add blob offload and failing fatal state-I/O tests
- e438060 feat(02-11): failpoint wiring and fail-stop on storage errors (STOR-07, STOR-14)

## Self-Check: PASSED

All 9 created files exist; commits 61c4ff1, 6f7f4d9, 7c05a25, e438060 present.
