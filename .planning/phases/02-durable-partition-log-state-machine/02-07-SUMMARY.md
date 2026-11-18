---
phase: 02-durable-partition-log-state-machine
plan: 07
subsystem: storage
tags: [ostro-log, recovery, torn-tail, corruption, crc, wal, fsync]
requires:
  - phase: 02-01
    provides: "ostro-log deps + ostro-testkit corrupt::* / fs::tempdir"
  - phase: 02-02
    provides: "vendored WAL (dir fsync, poison) whose segments are scanned"
  - phase: 02-03
    provides: "ostro_types::envelope::verify, PartitionId/Offset/Epoch"
provides:
  - "ostro_log::verify::{verify_record, VerifiedBatch, Corruption}"
  - "ostro_log::recovery::{recover_partition, RecoveryReport, TornCut, segment_dir, list_segments, TOPIC}"
  - "ostro_log::error::LogError (Corrupt/StaleEpoch/Failed/Io/Wal/NotReady/OutOfRange/Truncated)"
affects: [02-08, 02-12, 02-15, 02-16]
tech-stack:
  added: []
  patterns:
    - "Sequential declared_record_size + WalRecord::from_bytes scan over a whole segment read into memory; never the vendored readers"
    - "Torn iff last non-empty segment AND expected base >= durable floor AND (incomplete at EOF OR aligned all-zero 512-byte sector OR all-zero headerless remainder)"
    - "Cut = set_len + fsync file + fsync dir, before any GroupCommitWal queue exists"
key-files:
  created:
    - crates/ostro-log/src/error.rs
    - crates/ostro-log/src/verify.rs
    - crates/ostro-log/src/recovery.rs
    - crates/ostro-log/tests/common/mod.rs
    - crates/ostro-log/tests/corruption.rs
    - crates/ostro-log/tests/torn_tail.rs
  modified:
    - crates/ostro-log/src/lib.rs
    - crates/ostro-types/src/envelope.rs
decisions:
  - "Floor condition is `expected base >= floor` (the bad record holds no offset below applied_offset), stronger than the plan's 'starts after the last record with last < floor': it also protects a batch straddling the floor and the bad record itself"
  - "Zero-sector rule counts only full 512-byte aligned sectors (not etcd's partial chunks); added: a headerless remainder that is entirely zero up to EOF is torn (small unflushed tail with no full sector)"
  - "verify_record reports a base mismatch as Gap{expected, found} (Misplaced only for a foreign partition or a negative base); frame length field must equal 12 + calculate_length (else Unparseable)"
  - "RecoveryReport.segments lists every segment file incl. empty ones; empty files are skipped, never deleted in Phase 2; an empty/missing log has log_start = leo = 0"
metrics:
  duration: 20min
  completed: 2026-11-18
---

# Phase 2 Plan 07: Partition Log Recovery Summary

Opening a partition now runs a verified scan of every WAL segment. Each record must pass five checks: the frame CRC, the envelope CRC32C, the partition, `base == prev_last + 1` and a non-decreasing epoch. A torn tail above the durable floor is physically cut. Any other bad record fails the open with `LogError::Corrupt { partition, segment, byte_pos, first, last, reason }`. Recovery then rebuilds the LEO, the log start and the epoch history, and fsyncs every retained segment once.

## What was built

- **`verify.rs`:** `verify_record(rec, expect_base, pid)` runs these checks in order:
  1. frame CRC (`calculate_checksum` against the stored value, covering W1);
  2. V2 only;
  3. the frame's partition (`Misplaced`);
  4. the frame's base (`Gap`);
  5. `envelope::verify` (`Envelope(e)`);
  6. the envelope's partition;
  7. that the envelope base, count and last offset match the WAL trailer (`TrailerMismatch`).

  `VerifiedBatch` provides `last()`, `end()` and `commands_with_offsets()`.
- **`recovery.rs`:**
  - `segment_dir` and `list_segments` (sorted by segment id; empty files included; a missing directory gives an empty list).
  - `recover_partition(wal_dir, pid, floor)` reads each segment with `std::fs::read` and walks the records using `declared_record_size` and `WalRecord::from_bytes`.
  - The first failure is classified as torn or corrupt. A torn tail is cut with `set_len`, then `sync_all` on the file, then an fsync of the partition directory. Records after the cut that still parse are counted and logged at `warn`.
  - The doc comments explain why `kill -9` never produces torn writes, why records after a cut cannot have been acked, and why the cut must happen before a WAL queue opens a new segment (W4).
- **`error.rs`:** `LogError`, as specified in the plan.
- **`lib.rs`:** adds the `error`, `recovery` and `verify` modules and re-exports `LogError`, `Corruption`, `VerifiedBatch`, `verify_record`, `RecoveryReport`, `TornCut` and `recover_partition`. The `tracing` anchor was removed because the crate now uses it; the `metrics` and `tokio` anchors stay.
- **Tests:**
  - `tests/common` writes real segments through `GroupCommitWal` with `acks = 1`. Each instance writes one segment.
  - `corruption.rs` has 16 tests covering: sealed and last-segment bit flips, a final record protected by the floor, a forged envelope CRC under a valid frame, a record from another partition, an offset gap, an epoch regression, a short tail in a sealed segment, and the `verify_record` unit cases.
  - `torn_tail.rs` has 11 tests covering:
    - truncation at every byte of the final two records;
    - a zero sector above the floor, and in the second-to-last record (`discarded_records = 1`);
    - a zero sector below or straddling the floor, which gives `Corrupt`;
    - torn conditions in a sealed segment;
    - an idempotent reopen;
    - contiguous appends after a cut;
    - an empty last segment and a torn first record;
    - zero, short and non-zero garbage tails.

## Verification

- `cargo test -p ostro-log`: corruption 16/16, torn_tail 11/11, wal_smoke 1/1.
- `cargo clippy -p ostro-log --all-targets -- -D warnings`: clean.
- `cargo fmt --all --check`: clean.
- `! grep -rn read_segment crates/ostro-log/src`: passes.
- `cargo test -p ostro-types`: still green after the `Clone` derive.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `EnvelopeError` derives `Clone`**
- **Found during:** Task 1.
- **Issue:** The `Corruption` interface requires `Clone`, but `Corruption::Envelope(EnvelopeError)` would not compile because `EnvelopeError` was not `Clone`.
- **Fix:** Added `Clone` to the derive in `crates/ostro-types/src/envelope.rs`. This is additive only; the byte layout is unchanged.
- **Commit:** c38057f

**2. [Rule 1/2 - Correctness] Stronger floor condition**
- The plan's condition "starts at or after the end of the last record with last offset < floor" can cut a batch that straddles the floor. Some of that batch's offsets were already applied, so cutting it leaves the state ahead of the log.
- It also does not protect the bad record itself when the floor equals the LEO and that record has a zeroed sector.
- The implementation uses `expected base >= floor` instead, which also implies the plan's byte-position rule. Tests: `zero_sector_below_floor_fails_open`, which covers floor = leo and floor inside the final batch.

**3. [Rule 2 - Availability] All-zero headerless remainder counts as torn**
- A small unflushed tail (under 512 bytes, with its header zeroed) contains no full aligned sector, so under the strict plan rule a power loss would block the open.
- When the frame header cannot be read and everything from the record start to EOF is zero, the tail is now treated as torn.
- Partial chunks inside a structured record do not count. Unlike etcd, this avoids a false torn verdict caused by naturally zero trailer bytes.

**4. [Rule 1] Frame length self-consistency**
- `from_bytes` ignores bytes past the parsed fields, so recovery also requires `12 + calculate_length() == declared size`. A mismatch is reported as `Unparseable`.

## Requirements

STOR-02 and STOR-03 are **not** marked complete. This plan delivers the log-level scan, the classification and the cut. The node-open integration (02-12), the offline torn/corruption legs (02-15) and the phase gate (02-16) still list both requirements.

## Commits

- bd3a61d: test(02-07): add failing corruption-at-open tests (STOR-02)
- c38057f: feat(02-07): verified record reader and corruption detection at open (STOR-02)
- f8695d3: test(02-07): add failing torn-tail tests (STOR-03)
- f6c0d84: feat(02-07): torn-tail classification and physical cut (STOR-03)

## Self-Check: PASSED
