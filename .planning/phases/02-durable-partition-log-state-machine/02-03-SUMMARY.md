---
phase: 02-durable-partition-log-state-machine
plan: 03
subsystem: storage
tags: [ostro-types, envelope, crc32c, xxh3, routing, run-id, crockford-base32, prost, golden-vectors]
requires:
  - phase: 02-01
    provides: "ostro-types deps (bytes, prost, crc32c, xxhash-rust, uuid, thiserror) wired with anchors"
provides:
  - "ostro_types::ids: PartitionId, PartitionCount, Offset, Epoch, AttemptToken, RunId (+ IdError)"
  - "ostro_types::routing: partition_for_key, partition_for_new_run (frozen by tests/golden/routing.txt)"
  - "ostro_types::payload: PayloadRef / payload_ref::Kind / BlobRef prost messages, INLINE_PAYLOAD_LIMIT"
  - "ostro_types::envelope: encode, verify, peek_header, EnvelopeHeader, EnvelopeView, EnvelopeError, MAGIC, HEADER_LEN, DEFAULT_MAX_ENVELOPE_BYTES"
affects: [02-05, 02-06, 02-07, 02-08, 02-09, phase-3]
tech-stack:
  added: []
  patterns:
    - "Golden vectors pinned twice: a committed file (OSTRO_BLESS=1 regenerates) plus an in-test computation from first principles"
    - "Envelope golden hex computed independently in Python (struct + bitwise CRC32C), never from encode()"
key-files:
  created:
    - crates/ostro-types/src/ids.rs
    - crates/ostro-types/src/routing.rs
    - crates/ostro-types/src/payload.rs
    - crates/ostro-types/src/envelope.rs
    - crates/ostro-types/tests/ids.rs
    - crates/ostro-types/tests/envelope.rs
    - crates/ostro-types/tests/golden/routing.txt
  modified:
    - crates/ostro-types/src/lib.rs
decisions:
  - "RunId FromStr rejects non-zero padding bits in the final character (canonical form only), so every RunId has exactly one string"
  - "Body parse errors: clean end before cmd_count commands -> CountMismatch; a length prefix or command overrunning the body -> BodyMalformed; extra bytes -> TrailingBytes"
  - "Crate-root re-exports cover the types (ids, PayloadRef, envelope types, routing fns); envelope::encode/verify stay module-qualified"
metrics:
  duration: 15min
  completed: 2026-11-18
---

# Phase 2 Plan 03: ostro-types Core (ids, routing, payload, envelope) Summary

This plan adds the Phase-2-owned core of `ostro-types`:
- `RunId`: 18 bytes (u16 BE partition followed by a uuid), with a canonical 29-character lowercase Crockford base32 string form.
- The frozen xxh3 routing hash, pinned by 48 golden vectors.
- The prost `PayloadRef` (Inline or Blob, with a 256 KiB inline limit).
- The 32-byte LE header envelope codec with CRC32C. Every single-bit flip, truncation and header tamper is rejected.

## What was built

- **`ids.rs`:**
  - `PartitionId`, `PartitionCount` (NonZeroU16; defaults 16 and 64; `ids()` and `contains()`), `Offset` (implements `Add<u64>` and `Sub`, with i64 conversions for the WAL), `Epoch`, and `AttemptToken {epoch, n}` (derived lexicographic `Ord`).
  - `RunId` with `new`, `new_v7`, `from_bytes`, `as_bytes`, `partition` and `uuid`. `Display`, `FromStr` and `Debug` (`RunId(<base32>)`) use a hand-written base32 encoder/decoder. No new dependency was added.
  - The decoder accepts upper case and the aliases i/l→1 and o→0. It rejects the wrong length, invalid characters and non-zero padding bits.
- **`routing.rs`:**
  - `partition_for_key = xxh3_64(u32 LE len(workflow) ‖ workflow ‖ idem_key) % P`.
  - `partition_for_new_run = xxh3_64(uuid bytes) % P`.
- **`payload.rs`:** hand-written prost messages `PayloadRef { oneof kind { Inline(bytes) = 1; Blob(BlobRef) = 2 } }` and `BlobRef { sha256 = 1, len = 2 }`. Also `inline_len()` and `is_oversize_inline()`; a payload of exactly 256 KiB is allowed inline.
- **`envelope.rs`:**
  - The frozen layout is documented in a module table.
  - `encode` debug-asserts that there is at least one command.
  - `peek_header` checks, in order: length, magic, codec, flags, reserved, then CRC.
  - `verify` adds `Empty` (when `cmd_count` is 0), then parses exactly `cmd_count` commands and rejects trailing bytes.
  - `verify` caps the `Vec` preallocation at `rest.len()/4`, so a hostile count cannot force a huge allocation.
  - `EnvelopeView::last()` and `commands_with_offsets()`.
- **`lib.rs`:**
  - Adds `pub mod envelope; ids; payload; routing;` next to Phase 3's `pub mod journal;` and the re-exports.
  - Keeps `CODEC_VERSION = 1`.
  - Removes all six 02-01 `use x as _;` anchors (bytes, crc32c, prost, thiserror, uuid, xxhash_rust), because the crate now uses all of these dependencies.

## Ownership for Phase 3

- **Phase 2 owns:** `ids`, `routing`, `payload`, `envelope`, and the upcoming `command` module (plan 02-06).
- **Phase 3 may extend:** `journal` (theirs entirely: `journal.rs` and `tests/golden/journal_v1.txt`). They may also add new modules and re-exports to `lib.rs`.
- **Phase 3 must not change:** the byte layouts of the Phase 2 modules. They are pinned by golden tests.
- `journal.rs` was not touched by this plan.

## Verification

- `cargo test -p ostro-types`: everything passes. That is 7 unit tests, 11 envelope tests (including a 256-case round-trip proptest with 1..64 commands of 0..4 KiB each), 17 ids/routing/payload tests and Phase 3's 10 journal_golden tests.
- `cargo clippy -p ostro-types --all-targets -- -D warnings`: clean.
- `cargo fmt --all --check`: clean.
- `cargo build --workspace`: no warnings at the time of the run.

## Deviations from Plan

None. The plan was executed as written. A few minor choices fell within the plan's latitude:
- The golden routing file is regenerated with `OSTRO_BLESS=1`, following the journal_golden convention. The test also asserts that the file matches a fixed input list, so the set of vectors cannot drift silently.
- The plan listed `journal.rs` under files_modified. Per the coordination rules it already existed (Phase 3) and was left untouched.

## Requirements

STOR-01, STOR-09 and STOR-14 are only partially delivered here: this plan supplies the types and codecs. They stay Pending until the WAL journal (02-08), partition configuration (02-05/02-08) and blob offload wiring land.

## Commits

- 772fc67 test(02-03): add failing tests for ids, RunId string form, routing, PayloadRef
- a030414 feat(02-03): implement ids, RunId base32, frozen routing hash, PayloadRef
- 0c22cc9 test(02-03): add failing envelope codec tests
- 0598167 feat(02-03): implement envelope codec (32-byte header + CRC32C)

## Self-Check: PASSED

All created files exist; commits 772fc67, a030414, 0c22cc9, 0598167 present.
