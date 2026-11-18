---
phase: 02-durable-partition-log-state-machine
plan: 08
subsystem: storage
tags: [ostro-log, replicated-log, rf1, group-commit, high-watermark, leader-epoch, trim, failpoints]
requires:
  - phase: 02-02
    provides: "WAL poison (WalError::Poisoned, is_poisoned), failpoints wal::write/wal::fsync"
  - phase: 02-07
    provides: "recover_partition, verify_record, segment_dir/list_segments, LogError"
provides:
  - "ostro_log::ReplicatedLog trait + AppendReceipt, Isr, Role, LogHealth, EpochHistory"
  - "ostro_log::Rf1Log (sequencer, HW watch, verified reads, fail-stop, truncate_to, trim_prefix, set_role, recovery())"
  - "ostro_log::LogNode::open(LogNodeConfig, floors) -> (LogNode, Vec<Result<Arc<Rf1Log>, LogError>>); LogNode::{shutdown, commit_batches, partitions}; LogNodeConfig::new(wal_dir, partitions, node_id)"
  - "ostro_log::{compute_hw, ReplicaProgress} (shared with Phase 12)"
  - "ostro_log::{ReadyBarrier, TrimPermit} (permit only via ReadyBarrier::mint)"
  - "ostro_log::epoch_cache::EpochCache (ported Kafka leader-epoch semantics)"
  - "ostro-log feature `failpoints`; ostro-log in FAILPOINT_CRATES + CI failpoint step"
affects: [02-09, 02-11, 02-12, 02-14, 02-15, 02-16, 12-cluster]
tech-stack:
  added: ["fail 0.5.1 (optional, ostro-log feature failpoints)"]
  patterns:
    - "Sequencer: tokio Mutex<SeqState>; next advances only after append_enqueued Ok; inflight Semaphore permit held until the CommitWait resolves"
    - "HW published only from the WAL commit callback via compute_hw (monotonic send_if_modified); log_end = max(sequencer next, HW)"
    - "Per-append commit watcher task: CommitWait Err -> Status::fail (health Failed + role Offline, once)"
    - "wait_committed = select over HW, health and truncation watches; never hangs on a failed partition"
    - "truncate_to quiesces by acquiring all inflight permits under the sequencer lock before GroupCommitWal::truncate_to"
key-files:
  created:
    - crates/ostro-log/src/replicated.rs
    - crates/ostro-log/src/rf1.rs
    - crates/ostro-log/src/node.rs
    - crates/ostro-log/src/hw.rs
    - crates/ostro-log/src/barrier.rs
    - crates/ostro-log/src/epoch_cache.rs
    - crates/ostro-log/tests/replicated_log_conformance.rs
    - crates/ostro-log/tests/append_group_commit.rs
    - crates/ostro-log/tests/fatal_io.rs
    - crates/ostro-log/tests/trim_prefix.rs
  modified:
    - crates/ostro-log/src/lib.rs
    - crates/ostro-log/Cargo.toml
    - VENDORED.md
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - Cargo.lock
decisions:
  - "log_end() = max(sequencer next, HW): the commit callback can publish HW an instant before the appending task stores next; a committed record is in the log, so LEO >= HW always holds"
  - "Appends do not require Role::Leader; the epoch check (StaleEpoch) is the fence. Role starts Offline, set_role is ignored once Failed"
  - "Backpressure from append_enqueued returns LogError::Wal without failing the partition; every other enqueue/commit error fails it (no retry, no reopen until restart)"
  - "truncate_to quiesces in-flight appends (acquire_many on the inflight semaphore under the sequencer lock) so a late commit callback cannot raise HW above the cut; truncation-dropped writes do not fail the partition"
  - "trim_prefix clamps a floor above HW to HW; deletes a prefix of whole non-empty segments with next_base <= floor via GroupCommitWal::delete_segment_file, never the last non-empty or active segment, leaves empty segments, fsyncs the dir, logs every pass at info"
  - "read caps upto at HW; from inside a batch -> OutOfRange (caller error, no fail); a gap, overlap, verify failure or reader error below HW -> health Failed + LogError::Failed (never a silently short read)"
  - "EpochHistory::end_offset_for_epoch(e) answers only earlier epochs (None for latest/unknown); end_offset_for_epoch_at(e, leo) gives the full Kafka answer. EpochCache uses None for UNDEFINED"
  - "ostro-log failpoints feature = [dep:fail, fail/failpoints, ostro-wal/failpoints] (union of plan and Phase 2 convention)"
metrics:
  duration: 35min
  completed: 2026-11-18
---

# Phase 2 Plan 08: RF=1 Replicated Log Summary

`ostro-log` now exposes the `ReplicatedLog` trait and its RF=1 implementation. `Rf1Log` sequences appends over the vendored WAL's `append_enqueued` (contiguous offsets, one verified envelope per WAL record, group-committed fsyncs) and publishes HW only from the WAL commit callback through the shared `compute_hw`. It fails stop on any WAL write/fsync error and never reports such an append as committed. `LogNode::open` recovers every partition before the shared `GroupCommitWal` exists, and `trim_prefix` deletes only whole segments below a floor that only an opened `ReadyBarrier` can mint.

## What was built

- **`replicated.rs`**: the trait exactly as specified (RPITIT `impl Future + Send` methods), plus `AppendReceipt`, `Isr`, `Role`, `LogHealth` and `EpochHistory` (`latest`, `end_offset_for_epoch`, `end_offset_for_epoch_at`).
- **`rf1.rs` (`Rf1Log`)**:
  - `append` checks for an empty batch and the envelope size (`max_envelope_bytes`) before taking the lock, so neither touches sequencer state or health. It then checks health and the WAL poison flag, takes an inflight permit and the sequencer lock, applies the `StaleEpoch` check, encodes the envelope and calls `append_enqueued(acks = 1)`. Only after that returns Ok does it update the epoch cache and `next`. A spawned watcher holds the permit until the `CommitWait` resolves and fails the partition on Err.
  - `wait_committed` selects over the HW, health and truncation watches.
  - `read` serves from `read_tail`, falling back to a lazily built `PartitionIndex`, and verifies every record with `verify_record`.
  - Also provided: `truncate_to`, `trim_prefix`, `set_role` and `recovery()`.
- **`node.rs` (`LogNode`)**: `open` checks `floors.len() == P`, runs `recover_partition` in parallel `spawn_blocking` tasks, then calls `GroupCommitWal::new` and registers one commit callback (partition → HW sender, `compute_hw`, monotonic, and a batch counter exposed as `commit_batches()`). It builds an `Rf1Log` for each recovered partition and returns recovery errors per slot. Each partition's LEO, log start and torn cut are logged at info. `LogNodeConfig::new` sets the defaults (1024 in flight, `DEFAULT_MAX_ENVELOPE_BYTES`).
- **`hw.rs`**: `compute_hw` takes the minimum LEO over in-ISR replicas and panics on an empty ISR.
- **`barrier.rs`**: `ReadyBarrier` wraps a shared `watch<bool>` (`new`, `open`, `is_open`, `wait_open`, `mint`). `TrimPermit` has a private field, enforced by a `compile_fail` doctest.
- **`epoch_cache.rs`**: a port of Chronik's `LeaderEpochCache` (upstream L36–187) and its unit tests (L435–621), adapted to Ostro's `Epoch`/`Offset` types, with UNDEFINED returned as `None`. It is recorded under a new `## Ported into Ostro crates` section in VENDORED.md. The upstream source was read from a local read-only clone at 08715f4.
- **Failpoints**: the `failpoints` feature, `[[test]] fatal_io required-features`, `ostro-log` added to `FAILPOINT_CRATES`, and a line in the CI "Failpoint suites" step.

## Verification

- `cargo test -p ostro-log`: 21 unit, append_group_commit 2, corruption 16, replicated_log_conformance 6, torn_tail 11, trim_prefix 3, wal_smoke 1 and doc 1 (compile_fail): all green.
- `cargo test -p ostro-log --features failpoints -- --test-threads=1`: all of the above plus fatal_io 3/3. The fatal_io suite passed 4 runs in a row.
- `cargo clippy -p ostro-log --all-targets [--features failpoints] -- -D warnings`: clean.
- `cargo fmt --all --check`: clean for ostro-log. The only diff is in `crates/ostro-partition/tests/apply_cross.rs`, which belongs to the concurrent 02-09 executor and is out of scope.
- `bash scripts/check-vendored.sh`: all 5 checks pass. `! grep -rn 'read_segment\|acks.*0)' crates/ostro-log/src` also passes.
- Group commit: 200 concurrent appends from 16 tasks produced fewer than 200 WAL batches. Offsets were contiguous, and `recover_partition` verified all 200 records with matching LEO and epochs.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] LEO could briefly read below HW**
- **Found during:** Task 1 design.
- **Issue:** The WAL commit callback can fire before the appending task stores the new `next`, which would make `HW > LEO` visible for a moment.
- **Fix:** `log_end()` returns `max(sequencer next, HW)`.
- **Files modified:** crates/ostro-log/src/rf1.rs
- **Commit:** 95ff834

**2. [Rule 1 - Bug] Late commit callback vs truncate_to**
- **Issue:** The vendored `commit_batch` releases the file lock before it invokes the callback. A truncation slipping into that window would see its lowered HW raised again by the late callback.
- **Fix:** `truncate_to` acquires every inflight permit under the sequencer lock before calling `GroupCommitWal::truncate_to`. Permits are released only after each `CommitWait` resolves, which happens after its callback. Writes dropped by a truncation are not treated as I/O failures, and their waiters get `Truncated`.
- **Commit:** 95ff834

**3. [Rule 2 - Correctness] wait_committed for truncated offsets**
- Added a truncation watch, so a waiter whose offset was cut gets `LogError::Truncated` instead of hanging. `wait_committed` for an offset at or above LEO returns `OutOfRange`.

**4. [Rule 3 - Blocking] `Rf1Log::new` takes `&LogNodeConfig`**
- Clippy's `too_many_arguments` lint fired, so the constructor (crate-private) takes `(pid, &cfg, wal, hw, recovery)`.

**5. Failpoints feature union**
- The plan specified `["dep:fail", "ostro-wal/failpoints"]`, while the Phase 2 convention (and the orchestrator) specified `["dep:fail", "fail/failpoints"]`. Both are needed, so the feature uses the union.
- `fail` is anchored with `#[cfg(feature = "failpoints")] use fail as _;` in lib.rs to satisfy the unused-dependency lint.

**6. Trim floor above HW is clamped, not rejected**
- The plan says to "verify `permit.floor() <= HW`". The test requires that a floor at or above LEO still keeps the active segment, so a higher floor is clamped to HW with a warn. This is strictly more conservative.

### TDD note
- Task 1 had a RED commit (c166efa; the tests did not compile without the modules). Task 2's truncate and trim logic was implemented in Task 1, because `Rf1Log` needs both to satisfy the trait. Its tests therefore went green on first run and were committed together with the feature and CI wiring (41b766a).

## Requirements

- **STOR-04** complete: the trait exists from day one with RF=1 and ISR={self}.
- **STOR-01** complete: an append-only journal per partition on the vendored WAL with group commit, and every batch carries the verified envelope.
- **STOR-07** is **not** marked complete. This plan delivers log-level fail-stop; state-store fatal handling (02-11) and the offline legs (02-15) still list it.
- **STOR-08** is **not** marked complete. This plan delivers the trim rule and the barrier/permit; the startup barrier wiring (02-12) and retention (02-14) still list it.

## Commits

- c166efa: test(02-08): add failing ReplicatedLog conformance and group-commit tests
- 95ff834: feat(02-08): ReplicatedLog trait, Rf1Log sequencer + HW, LogNode open (STOR-04, STOR-01)
- 41b766a: feat(02-08): fatal I/O failpoint suite, trim and truncate tests (STOR-07, STOR-08)

## Self-Check: PASSED
