---
phase: 02-durable-partition-log-state-machine
plan: 05
subsystem: storage
tags: [fjall, state-store, write-batch, overlay, checkpoint, digest, blake3, postcard]
requires:
  - phase: 02-01
    provides: "fjall =3.1.13 pin, ostro-partition deps + clippy.toml, ostro-testkit"
  - phase: 02-03
    provides: "PartitionId, PartitionCount, Offset, RunId"
provides:
  - "ostro_partition::store::{PartitionStore, StoreReader, StoreMeta, StoreError, Table, TABLES, Rows}"
  - "store::config::{StoreConfig, state_dir}"
  - "store::batch::{ApplyBatch, MetaUpdate} (overlay get/insert/remove/range_prefix, reads(), commit)"
  - "store::keys: run/event/payload/ready/timer/inbox/dedupe/outbox/inbound/children keys + decode_{ready,timer,outbox}_key, ready_prefix"
  - "store::values: CursorMeta, RunStatus, RunRecord, ReadySlot, ReadyValue, InboxMsg, encode/decode (format byte 1)"
  - "store::meta: meta key constants, FORMAT_VERSION = 1"
  - "PartitionStore::digest(): logical blake3 digest"
affects: [02-09, 02-10, 02-11, 02-12, 02-13, 02-14, 02-15, 02-16]
tech-stack:
  added: []
  patterns:
    - "One fjall Database per partition; one keyspace per Table"
    - "ApplyBatch = BTreeMap overlay over keyspaces + read counter, committed as one fjall WriteBatch with meta"
    - "Digest from one fjall snapshot, TABLES order, excludes only meta checkpointed_offset"
key-files:
  created:
    - crates/ostro-partition/src/store/mod.rs
    - crates/ostro-partition/src/store/config.rs
    - crates/ostro-partition/src/store/keys.rs
    - crates/ostro-partition/src/store/values.rs
    - crates/ostro-partition/src/store/batch.rs
    - crates/ostro-partition/src/store/meta.rs
    - crates/ostro-partition/src/store/digest.rs
    - crates/ostro-partition/tests/store.rs
    - crates/ostro-partition/tests/store_digest.rs
  modified:
    - crates/ostro-partition/src/lib.rs
decisions:
  - "One fjall Database per partition at <data>/state/pNNNN (rm -rf rebuild, partition-local poisoning, bounded per-partition journal replay)"
  - "StoreConfig::default(): worker_threads 1, max_journaling_size 64 MiB, max_memtable_size 8 MiB per keyspace, cache_size 16 MiB (02-16 may retune)"
  - "inbound_seen rows live in the inbound_seen keyspace (u16 BE src -> u64 BE seq) and are folded into StoreMeta on read"
  - "ApplyBatch::get counts every keyspace lookup (hit or miss); overlay hits/tombstones are free; range_prefix counts 1"
  - "digest reads all keyspaces from one fjall snapshot (db.snapshot()) for cross-table consistency"
  - "Fresh-store detection = missing format_ver key (a crash before the first meta persist re-initialises)"
metrics:
  duration: 20min
  completed: 2026-11-18
---

# Phase 2 Plan 05: fjall State Store Layer Summary

This plan adds the per-partition state store on fjall 3.1.13. Each partition gets its own bounded fjall `Database` with 13 fixed keyspaces, big-endian key codecs and postcard values. `ApplyBatch` is the single write seam: it has a `BTreeMap` overlay, counts keyspace reads, and commits data and meta in one atomic `WriteBatch`. `checkpoint()` uses `persist(SyncAll)`. A logical blake3 digest gives the same result regardless of how batches were chunked or when checkpoints ran.

## What was built

- **`config.rs`:** `StoreConfig` and `state_dir`.
  - Defaults: 1 worker, 64 MiB journal (the fjall minimum), 8 MiB memtable per keyspace, 16 MiB cache.
  - The module doc records why there is one Database per partition.
- **`mod.rs`:** `Table` (`#[repr(u8)]`, in declaration order) and `TABLES`, plus `Table::name()`. Also `StoreMeta`, `StoreError` (variants exactly as specified), `PartitionStore` and the `Rows` alias.
  - `PartitionStore` is `Send + Sync` and has a manual `Debug` impl.
  - `open()`: a fresh store gets all meta keys in one batch, then `persist(SyncAll)`. An existing store is validated in this order: format version, then partition id, then partition count.
  - `exists()`, `batch()`, `checkpoint()` (inserts `checkpointed_offset`; failpoint `state::persist` runs before `persist(SyncAll)`), `read_meta()`, `reader()`.
  - `StoreReader` (cloneable keyspace handles): `get`, `prefix(limit)`, and `range(from..to, limit)` with an exclusive upper bound.
- **`keys.rs`:** every key codec in the interface block, plus `ready_prefix(queue)`.
  - In `ready_key`, priority is mapped to `!((p as u32) ^ 0x8000_0000)`, so higher priority sorts first.
  - Decoders return `None` when the length is wrong.
- **`values.rs`:** all value types; `encode` is `1 ‖ postcard`. `decode` rejects an unknown format byte or an empty value with `StoreError::Codec`.
- **`meta.rs`:** documents the meta key layout table, `FORMAT_VERSION = 1`, `write_initial` and `read`.
- **`batch.rs`:** `ApplyBatch` and `MetaUpdate`.
  - `commit` writes the overlay in BTreeMap order, then `applied_offset`, `state_hash`, `cursor` and the changed `inbound_seen` rows.
  - Failpoint `state::commit` runs before `WriteBatch::commit`.
  - The default buffered journal flush is used.
- **`digest.rs`:** blake3 over `TABLES` order.
  - Per table it feeds `u16 LE name_len ‖ name`, then each row as `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`, then `u64 LE row_count`.
  - It skips only `checkpointed_offset`.
  - It reads from one `db.snapshot()`. Callers must take it while the partition is quiesced (this is documented).
- **`lib.rs`:**
  - Removed the 02-01 `open_smoke` skeleton and its test.
  - Added `pub mod store` and re-exports.
  - Dropped anchors that are no longer needed: `fail`, `ostro_types`, `postcard`, `serde`, `thiserror`, and `blake3` (removed in Task 2).
  - Remaining anchors: `bytes`, `metrics`, `ostro_blob`, `ostro_log`, `tokio`, `tracing`, plus the dev-deps `ostro_testkit`, `proptest`, `proptest_state_machine` and `tempfile` under `#[cfg(test)]`.

## Verification

- `cargo test -p ostro-partition`: 1 unit test, 17 store tests (including 3 key-ordering proptests) and 4 digest tests all pass.
- `cargo clippy -p ostro-partition --all-targets -- -D warnings` is clean, both with and without `--features failpoints`.
- `cargo fmt --all --check` is clean.
- `cargo build --workspace` produces no warnings.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] `Debug` for `PartitionStore`**
- The tests format `Result<(PartitionStore, StoreMeta), _>` with `{:?}`, so I added a manual `Debug` impl that shows the path.
- Commit 780445b.

**2. [Rule 3 - Blocking] clippy `type_complexity`**
- `Result<Vec<(Vec<u8>, Vec<u8>)>, _>` triggered `type_complexity`. I added `pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;`.
- Commit 780445b.

**3. Minor additions (additive, not in the interface block):**
- `Table` also derives `Hash`.
- `ApplyBatch::len` / `is_empty` and `keys::ready_prefix` were added.
- `meta` is a `pub` module, so tests and later plans can reach the key constants.

## Notes for later plans

- **02-11 (failpoints):** `Cargo.toml` still has `failpoints = ["dep:fail"]`. Without `fail/failpoints` the `fail_point!` macros compile to no-ops. Plan 02-11 should change the feature to `["dep:fail", "fail/failpoints"]` when it registers ostro-partition in `FAILPOINT_CRATES`. I left Cargo.toml alone because it is outside this plan's files.
- `checkpoint()` writes `checkpointed_offset` as a standalone keyspace insert followed by `persist(SyncAll)`. The digest excludes that key, so checkpoint timing never affects it.

## Requirements

STOR-05, STOR-06 and STOR-09 are only partially delivered here; this plan supplies the store layer. They stay Pending until the following land:
- apply and recovery (02-09/02-10/02-12)
- the end-to-end rebuild test
- partition configuration and routing wiring

## Commits

- 677eff8 test(02-05): add failing partition store tests
- 780445b feat(02-05): fjall partition store with ApplyBatch overlay, meta and checkpoint
- 0d4dc24 test(02-05): add failing logical state digest tests
- 676796f feat(02-05): logical state digest over all tables in fixed order

## Self-Check: PASSED

All created files exist; commits 677eff8, 780445b, 0d4dc24, 676796f present.
