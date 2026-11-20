---
phase: 08-triggers-bottomless-observability
plan: 02
subsystem: storage
tags: [snapshot, osnp, fjall, crc32c, lz4, blob-refs, install, crash-safety, backup]

requires:
  - phase: 08-triggers-bottomless-observability
    provides: 08-01 store/snapshot.rs + store/blobrefs.rs stubs, crc32c/lz4_flex pins, Table::ScheduleState
provides:
  - OSNP v1 writer/reader (OsnpWriter, streaming OsnpReader next_raw/next_frame, RawFrame::encode, verify_snapshot)
  - PartitionStore::export_snapshot / export_snapshot_with (ONE fjall snapshot, batch-atomic, no apply pause)
  - install_snapshot(data_dir, p, count, cfg, reader) staged + verified install, recover_install crash rule
  - install_staging_dir / install_old_dir path helpers (state/.install-pNNNN, state/.old-pNNNN)
  - shared incremental DigestBuilder (digest() byte-identical)
  - blobrefs::visit_row exhaustive Table visitor + PartitionStore::blob_refs_snapshot
  - migrate_row hook + state_format rule (D18 amended)
  - docs/snapshot-format.md (OSNP v1 spec for Phase 12)
affects: [08-09 snapshot service + Node::open install, 08-13 uploader, 08-18 restore, 08-19 blob GC, Phase 12 follower install]

tech-stack:
  added: []
  patterns:
    - "Export = one Database::snapshot(); meta (applied/state_hash/cursor) read from the same snapshot"
    - "Install = staging dir + reopen + digest check + rename-aside swap + dir fsync; recover_install before open"
    - "fjall Ingestion is not nameable: a none_of(|| ks.start_ingestion()) helper types the Option"

key-files:
  created:
    - docs/snapshot-format.md
    - crates/ostro-partition/tests/snapshot.rs
    - crates/ostro-partition/tests/blobrefs.rs
  modified:
    - crates/ostro-partition/src/store/snapshot.rs
    - crates/ostro-partition/src/store/blobrefs.rs
    - crates/ostro-partition/src/store/digest.rs

key-decisions:
  - "Install writes rows with ~4 MiB WriteBatch chunks + persist(SyncAll) (DEFAULT_INSTALL_WRITER); ingestion was not consistently faster (spike numbers below). Both writers stay implemented and tested"
  - "install_snapshot takes partition_count (PartitionId + PartitionCount, like PartitionStore::open) so a different count is Mismatch before anything is written"
  - "Header reserved area is 24 bytes (the 64-byte layout); the plan sketch's 16 was arithmetic slack"
  - "lz4 off by default; measured 0.38 on a synthetic 100k-run store; compression is the upload path's choice (08-13)"
  - "Crash rule: .install-p removed; .old-p without p renamed back; .old-p next to p removed"

requirements-completed: []  # BACK-04 completes with 08-09 (trim coordination)

duration: ~2h (two sessions; first cut off by a machine restart after Task 1)
completed: 2026-11-20
---

# Phase 8 Plan 02: OSNP v1 Snapshot Format Summary

**One logical OSNP v1 snapshot format: a batch-atomic export from a single fjall snapshot, CRC32C frames with a logical-digest trailer, a staged and digest-verified install with crash-safe rename-aside, an exhaustive blob-ref visitor, and docs/snapshot-format.md for Phase 12.**

## Performance

- Duration: about 2 h over two sessions (Task 1 landed in 52f8536 before a machine restart; Task 2 was finished in this session)
- Completed: 2026-11-20
- Tasks: 2/2
- Files: 6 code/docs files

## Accomplishments

- **Export** (`PartitionStore::export_snapshot[_with]`): one `Database::snapshot()`. `applied_offset`, `state_hash`, `cursor`, `partition` and `partition_count` are read from that same snapshot. `checkpointed_offset` is skipped. Every row goes to the writer, the shared `DigestBuilder` and the blob visitor. Rows frames are ≤ 1 MiB, then BlobRefs, then the Trailer. 20 exports during a 3000-chunk concurrent apply each equal the replay digest at their own `applied_offset`.
- **Reader**: `OsnpReader` streams frames one at a time (`next_raw` returns them as stored for forwarding, `next_frame` decodes them). `verify_snapshot` checks a whole stream: header CRC, frame CRCs, structure, key order, recomputed blob refs, trailer digest and counts, meta/header agreement, and no bytes after the trailer. Every single-byte flip (exhaustive over positions with two masks, plain and lz4, plus a proptest with ≥ 256 cases) and every truncation is detected. Kind 200 is skipped; kind 9 is `UnknownFrame`. A newer `state_format` is `FutureFormat`.
- **Install** (`install_snapshot`):
  - refuses another partition or partition count up front;
  - runs `recover_install`;
  - builds `state/.install-pNNNN` with meta last (`checkpointed_offset = applied_offset`) and `persist(SyncAll)`;
  - closes and reopens the store, then checks `format_ver`, applied/checkpointed, partition, `state_hash` and `digest() == trailer` (the migrated digest);
  - renames `pNNNN` to `.old-pNNNN`, renames staging to `pNNNN`, fsyncs `state/` and removes `.old`.
  
  Corrupt, forged-digest, truncated or mismatched streams leave the live state byte-for-byte untouched and leave no leftovers.
- **Crash safety**: failpoints `snapshot::install_after_write`, `install_before_swap`, `install_after_old_rename` and `install_before_old_remove` were each injected with and without a prior state. After `recover_install`, the result is either the old state or the complete new state, never a half-installed `pNNNN`, and a retried install then succeeds.
- **Rebuild equivalence** (proptest, 32 cases): commands come from the `rebuild_digest` generator hooks (`tests/common/gen_waits`, `gen_lifecycle`, read-only) plus starts and grants, applied through the pure apply core. Exporting at a random k, installing, and replaying `[k, end)` gives the same `digest()`, rolling hash and next offset as the full replay.
- **docs/snapshot-format.md**: covers the purpose, header table, frame kinds and forward compatibility, row ordering and the `checkpointed_offset` exclusion, trailer, digest and blob refs, `state_format` and migrators (D18 amended), local files, staged install and the crash-recovery table, the remote key, Phase 12 frame streaming with resume at frame boundaries, and the epoch history in the cursor row (F7).

## Ingestion spike (Open Question 3)

Release build, 200,006-row store (15.3 MB OSNP). Times are the whole install: two fjall opens, writing, `persist`, reopen and digest. The machine was at load 17–26 while 4 other executors were building.

| Writer | Run 1 | Run 2 |
|--------|-------|-------|
| WriteBatch (~4 MiB chunks) | 6.15 s | 4.45 s |
| `Keyspace::start_ingestion()` | 4.80 s | 5.23 s |

Ingestion reopen without an explicit persist (20k rows, drop without `persist(SyncAll)`, reopen, digest) passed **20/20**.

**Choice: WriteBatch**, because ingestion was not consistently faster. The plan's rule keeps ingestion only if it is faster *and* passes 20/20. Both writers stay implemented (`InstallWriter`, `#[doc(hidden)] install_snapshot_with`) and both pass the install round-trip and corruption tests. Correctness never depends on either: the reopen + digest check and the directory renames decide. A quiet-machine re-measure could flip the default by changing one constant (`DEFAULT_INSTALL_WRITER`).

## Snapshot size (100k-run store)

`tests/snapshot.rs::snapshot_size_for_100k_runs` (ignored, release). It uses the `chunk_ops` mix: a run record, one event, one payload and one timer per run, older runs' events and timers deleted, and every 5th payload a blob ref.

| | bytes |
|---|---|
| rows | 160,029 (31,858 blob refs) |
| OSNP plain | 10,175,723 |
| OSNP lz4 | 3,831,323 (ratio 0.377) |

The synthetic values are repetitive, so real payloads will compress less. The 500-chunk store in `lz4_flag_round_trips` prints its own ratio. lz4 stays **off by default**: local snapshots are read on the install path, and compression is a decision for 08-13 (upload).

## Per-table blob-ref locations (store/blobrefs.rs)

| table | refs |
|---|---|
| meta, runs, ready, timers, dedupe, inbound_seen, children, dlq, effects, running, waits, signal_seen | none. `RunRecord.module_sha` names a module, owned by meta `Module` records, never collected via run state |
| events | prost `JournalEvent.payload` |
| payloads | the prost `PayloadRef` row (run input/result) |
| inbox | signal rows: `SignalSlot::Pending.event`; legacy rows: `InboxMsg::ChildCompleted.result` (prost `PayloadRef`, empty = none) |
| outbox | prost `OutboxMsg`: `StartChild.start` / `CronStart.start` / `StartRun` (`input`, `run_started.payload`), `ChildCompleted.result` + `.event`, `Signal.payload` |
| schedule_state | `0x01` rows: `ScheduleStateRow.buffered.start` (prost `StartRun`: `input`, `run_started.payload`); `0x02` rows none |

A row that can carry refs but does not decode is an error, never a skip, so GC cannot delete a live blob.

## Task Commits

1. **Task 1: blob-ref visitor + OSNP v1 writer/reader + export_snapshot**: `52f8536` (previous session)
2. **Task 2: staged install (ingestion spike, WriteBatch default) + docs/snapshot-format.md**: `bb2071c`

## Decisions Made

See key-decisions in the frontmatter.

## Deviations from Plan

1. **[Rule 3 - Blocking] `install_snapshot` signature.** The plan's `install_snapshot(state_root, partition: u16, cfg, r)` has no partition count, but "different partition_count → Mismatch" needs one. The function now takes `(data_dir: &Path, p: PartitionId, count: PartitionCount, cfg: &StoreConfig, r)`, like `PartitionStore::open`. The data dir (not `state/`) is the root, matching `state_dir`.
2. **[Rule 3 - Blocking] Staging open without editing `store/mod.rs`.** `PartitionStore::open` hard-codes `state/pNNNN` and writes initial meta. `snapshot.rs` has a private `open_raw(path, cfg)` that repeats the production builder settings (worker threads, journal size, cache and memtable sizes) and builds `PartitionStore { db, ks }` directly. A child module may do this; the rule against editing `mod.rs` stays satisfied.
3. **Extra failpoints.** Besides `install_before_swap` and `install_after_old_rename`, the code adds `snapshot::install_after_write` (before persist and verification) and `snapshot::install_before_old_remove` (after the swap), so the crash test covers every step. All use the reserved `snapshot::` prefix.
4. **`.old-pNNNN`.** The plan names this data-dir entry. RESERVATIONS §11 lists only `state/.install-pNNNN`, so `.old-pNNNN` is recorded here and in docs/snapshot-format.md §7 for 08-22's docs freeze. No ordinal is involved.
5. **Header reserved bytes.** The interface sketch says reserved [u8;16], but the fields plus the CRC add up to 64 bytes only with 24 reserved bytes. Task 1 implemented 24, and the doc states that the table is normative.

## Issues Encountered

- fjall 3.1.13 does not export the `Ingestion` type, so it cannot be named in a struct or an annotation. A `none_of(|| ks.start_ingestion())` helper infers the `Option` type.
- Under load 17–34, the debug `snapshot` suite takes about 6 min (the rebuild proptest is most of that, because of fjall open fsyncs) and the failpoints run takes about 10 min.

## Verification

- `cargo test -p ostro-partition --test snapshot --locked`: 15 passed (1 ignored measurement)
- `cargo test -p ostro-partition --features failpoints --test snapshot --locked -- --test-threads=1`: 16 passed (1 ignored)
- `cargo test -p ostro-partition --test snapshot --test blobrefs --test store_digest --locked` (resume check of Task 1): 10 + 5 + 4 passed
- `cargo test -p ostro-partition --locked` (whole crate): 31 test binaries (incl. doctests), 278 passed, 0 failed (store_digest 4/4 and format_corpus unchanged and green)
- `cargo clippy -p ostro-partition --all-targets --locked -- -D warnings`, with and without `--features failpoints`: clean. `cargo fmt --check` clean.
- `grep state_format docs/snapshot-format.md` and `grep follower docs/snapshot-format.md`: both present
- Cargo.toml and Cargo.lock untouched. No WIT or frozen-format change.

## Next Phase Readiness

- 08-09 can call `recover_install` and then `install_snapshot` from `Node::open`, and `export_snapshot` from the snapshot service. Its local file naming is in the doc, §7.
- 08-19 GC uses `blob_refs_snapshot()` and the BlobRefs frame. 08-13 uploads the same bytes. Phase 12 forwards `RawFrame`s.

## Self-Check: PASSED

- Files: store/snapshot.rs, store/blobrefs.rs, store/digest.rs, tests/snapshot.rs, tests/blobrefs.rs, docs/snapshot-format.md all present on main
- Commits: 52f8536 and bb2071c present on main
