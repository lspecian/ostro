---
phase: 02-durable-partition-log-state-machine
plan: 02
subsystem: storage
tags: [wal, fsyncgate, durability, failpoints, vendored]
requires: []
provides:
  - "WalError::Poisoned + GroupCommitWal::is_poisoned (ReplicatedLog health input)"
  - "fsync_dir / fsync_dir_blocking (pub, ostro_wal::group_commit)"
  - "failpoints wal::write (eio|enospc), wal::fsync, wal::dir_fsync behind ostro-wal feature `failpoints`"
  - "FAILPOINT_CRATES array + 'failpoint suites' step in scripts/ci-local.sh"
affects: [02-ostro-log, 02-ostro-partition, 02-ostro-blob, ci]
tech-stack:
  added: ["fail 0.5.1 (optional, ostro-wal only)"]
  patterns: ["poison-on-I/O-error fail-stop partition", "poison flag checked under the pending lock", "dir fsync after create/rotate/truncate/unlink"]
key-files:
  created:
    - vendor/ostro-wal/tests/poison.rs
    - vendor/ostro-wal/tests/dir_fsync.rs
  modified:
    - vendor/ostro-wal/src/group_commit.rs
    - vendor/ostro-wal/src/error.rs
    - vendor/ostro-wal/Cargo.toml
    - VENDORED.md
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
decisions:
  - "WAL poison flag is checked under the pending lock in enqueue, so a write either lands before the poisoning drain (and is failed by it) or sees the flag; no waiter can hang on a stopped worker"
  - "Any I/O failure in seal, rotation (incl. dir fsync) or truncate_to poisons the partition; truncate_to on a poisoned partition returns Poisoned (fail-stop until the WAL is reopened)"
  - "fsync_dir is pub (not pub(crate)) so the integration test and later Ostro crates can call it; queue creation also fsyncs the topic and base dirs"
  - "Feature failpoints enables fail/failpoints (the fail_point! macro is gated on the fail crate's own feature)"
metrics:
  duration: 14min
  completed: 2026-11-18
---

# Phase 2 Plan 02: Vendored WAL patches P1/P2/P3 Summary

The vendored group-commit WAL is now fail-stop on I/O errors. A write or fsync error poisons that partition's queue: waiters get `WalError::Poisoned`, the commit callback and `committed_through` stay put, and the worker stops. Segment create, rotate, truncate and unlink now fsync the parent directory. `fail` failpoints for write, fsync and dir fsync exist behind a `failpoints` feature, and a failpoint suite step runs in local and GitHub CI.

## What was built

**P1, poison (STOR-07 / fsyncgate).** `PartitionCommitQueue` gains three fields: `poisoned: AtomicBool`, `poison_reason` and `durable_len: AtomicU64`. When `write_all` or `sync_all` fails in `commit_batch`, the code:
- marks the queue poisoned;
- calls `set_len(durable_len)` on the active segment, best effort, so a torn partial write is cut back;
- clears the tail cache;
- fails the batch and every pending waiter with `Poisoned`;
- returns `Err(Poisoned)`.

The worker loop checks the flag at the top of each iteration and after a failed `commit_batch`. In either case it drains the queue and `return`s. `enqueue_with_receipt` and `enqueue_nowait` refuse writes under the pending lock. Failed fsyncs in the seal paths (`force_seal_segment`, `check_and_seal_segment`) also poison. So does any failure in rotation, including its dir fsync, and any failure inside `truncate_to` (fsync, file surgery, rotation); dropped writes then get `Poisoned`. `GroupCommitWal::is_poisoned(topic, partition)` is added.

**P2, directory fsync.** `fsync_dir_blocking` (std, contains failpoint `wal::dir_fsync`) and its async wrapper `fsync_dir` (spawn_blocking) are added. Call sites:
- after queue creation: partition, topic and base directories;
- in `rotate_to_new_segment` before the new writer is published (failure poisons);
- after the truncation `set_len` + fsync;
- after every segment unlink (`remove_truncated_segment`, `delete_segment_file`, `remove_segment`).

**P3, failpoints.** `WalWriter::write_all` uses `fail::eval("wal::write", ..)`. `return(enospc)` writes half the buffer and fails with ENOSPC; any other argument fails with EIO. `WalWriter::sync_all` has `fail_point!("wal::fsync")` (EIO). `commit_batch` already goes through these `WalWriter` methods, so the failpoints fire on the group-commit path. All failpoint code is `#[cfg(feature = "failpoints")]`, and `cargo tree -e normal` has no `fail` without the feature.

**Tests.**
- `tests/poison.rs` (required-features `failpoints`, 4 tests):
  - a failed fsync is never Ok, the callback never fires, later acks=1 and acks=0 appends get `Poisoned`, another partition stays healthy, and `durable_end_offset` does not advance;
  - an ENOSPC partial write is cut back to the durable length;
  - an EIO write poisons;
  - 50 waiters piled up behind a paused write whose fsync then fails all resolve Err within the timeout, and none get Ok.
- `tests/dir_fsync.rs` (4 tests): `fsync_dir` is Ok on a directory and Err on a missing path; a new segment is visible after the first ack; rotation leaves a parseable sealed segment; truncation still cuts with the dir fsync in place.

**CI.** `scripts/ci-local.sh` gets `FAILPOINT_CRATES=(ostro-wal)`, `check_failpoints` (clippy is skipped for ostro-wal; `cargo test --features failpoints -- --test-threads=1` runs for each crate) and a `failpoint suites` step after `cargo test`. `.github/workflows/ci.yml` gets a "Failpoint suites" step on the linux-x86_64-gnu leg.

## Verification

- `cargo test -p ostro-wal`: 73 unit + 4 dir_fsync + 3 idle_seal + 14 suffix_truncation + 1 doc, all pass.
- `cargo test -p ostro-wal --features failpoints -- --test-threads=1`: the above plus 4 poison tests, all pass; the poison suite passed 3 times in a row.
- `cargo build -p ostro-wal` is warning-free with and without the feature.
- `bash scripts/check-vendored.sh`: all 5 checks pass.
- `bash scripts/ci-local.sh --no-musl`: every step passes, including the new failpoint suites step.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Pile-up test used a failpoint expression that never failed**
- **Found during:** Task 1 (GREEN)
- **Issue:** `sleep(50)->return` in `fail` 0.5 runs only the first action (sleep) and then continues normally, so the fsync succeeded and all 50 appends were Ok. The test was wrong, not the patch.
- **Fix:** Park the worker with `wal::write = pause`, let the 50 appends queue up, set `wal::fsync = return`, then `fail::remove("wal::write")` to release the worker.
- **Files modified:** vendor/ostro-wal/tests/poison.rs
- **Commit:** 1a7c297

**2. [Rule 3 - Blocking] `fsync_dir` made `pub` instead of `pub(crate)`**
- The plan's dir_fsync.rs test calls `fsync_dir` from an integration test, which can only reach public items. `fsync_dir_blocking` is also `pub`, because the sync deletion paths need it.

**3. [Rule 2 - Critical] Extra hardening beyond the plan text**
- The poison flag is checked under the pending lock, which closes a race where a waiter could be queued after the final drain and hang forever.
- `commit_batch` re-checks the flag after taking the writer lock, in case a seal or truncation poisoned the queue in between.
- Queue creation fsyncs the topic and base dirs, which `create_dir_all` may have just created.
- `truncate_to` on a poisoned partition returns `Poisoned`.

### Notes

- Cargo.lock: the `fail` 0.5.1 entry ended up in the concurrent 03-01 commit `6ae0f63`, because that executor rebuilt the lock after this plan's manifest change. No separate lock commit was needed here.
- Requirements STOR-07 and STOR-01 are **not** marked complete. This plan delivers only the WAL-level part of STOR-07; partition-level fatal handling through `ReplicatedLog` health, and STOR-01's envelope/journal, come in later Phase 2 plans.

## Commits

- 06344f6: test(02-02): add failing WAL poison and dir-fsync tests
- 1a7c297: feat(02-02): WAL patches P1 poison, P2 dir fsync, P3 failpoints
- bfac327: chore(02-02): record WAL patches P1-P3 in VENDORED.md, add failpoint CI step

## Self-Check: PASSED
