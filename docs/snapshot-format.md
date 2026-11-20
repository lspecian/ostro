# OSNP v1: Ostro partition snapshot format

Status: Phase 8 (plan 08-02, decision P8-D1). Implementation: `crates/ostro-partition/src/store/snapshot.rs`
(writer, reader, export, install) and `store/blobrefs.rs` (blob-ref visitor). Tests:
`crates/ostro-partition/tests/snapshot.rs`, `tests/blobrefs.rs`.

All integers are little-endian unless stated otherwise. Row keys and values are copied
verbatim from the state keyspaces (their own encodings, big-endian keys included, are
described in [storage-format.md](storage-format.md) §6, §7 and §13).

## 1. Purpose

One logical snapshot format, used for:

- **Local snapshots** (08-09): the bound on replay after WAL trimming. `Node::open` installs the
  newest local snapshot and replays the log tail on top of it.
- **Bottomless restore** (08-13 upload, 08-18 restore): the same bytes are uploaded to object
  storage and installed by `ostro restore`.
- **Follower install** (Phase 12): a follower that is too far behind the leader's `log_start`
  fetches the leader's snapshot and installs it. A follower may run a different (older or
  newer) binary, which is why the format is logical and versioned (see §6).

Physical fjall file copies are rejected: fjall has no checkpoint API, and its file layout is
not an Ostro format (storage-format.md §1).

A snapshot is the **complete** state of one partition at one offset: every row of every
keyspace in `TABLES`, exported from one fjall `Database::snapshot()`.

**Batch consistency.** fjall commits a `WriteBatch` at a single sequence number and publishes
it after the last item. Every apply chunk (its data rows plus the meta rows `applied_offset`,
`state_hash` and `cursor`) is one batch. So a fjall snapshot sees each chunk entirely or not at
all, and the `applied_offset` read from the same snapshot names exactly the exported rows. The
apply thread is never paused by an export.

## 2. Header (64 bytes)

| Offset | Size | Field | Notes |
| ------ | ---- | ----- | ----- |
| 0 | 4 | `magic` | ASCII `OSNP` |
| 4 | 2 | `snap_ver` | `1`. Any other value: `UnsupportedVersion` |
| 6 | 2 | `flags` | bit 0 = `Rows` frames are lz4-compressed. Other bits must be 0 |
| 8 | 2 | `partition` | partition id; must be `< partition_count` |
| 10 | 2 | `partition_count` | the cluster's partition count (STOR-09) |
| 12 | 4 | `state_format` | the meta `format_ver` of the exported rows (§6) |
| 16 | 8 | `applied_offset` | **exclusive**: the next log offset to apply on top of this state |
| 24 | 4 | `latest_epoch` | `cursor.epoch` of the exported state (informational; the authoritative epoch history is the `cursor` row, §10) |
| 28 | 8 | `created_at_ms` | wall time from the node's injected clock; informational, never compared |
| 36 | 24 | reserved | zero |
| 60 | 4 | `header_crc32c` | CRC32C (Castagnoli) of bytes 0..60 |

The reader checks the header CRC **first**, then magic, version, flags, partition range and
`state_format`. (The 08-02 interface sketch listed 16 reserved bytes; the encoded header is
64 bytes, so the reserved area is 24 bytes. The table above is normative.)

## 3. Frames

After the header the file is a sequence of frames:

```
u32 len | u8 kind | payload[len] | u32 crc32c(kind ‖ payload)
```

| Kind | Name | Payload |
| ---- | ---- | ------- |
| 1 | `TableBegin` | `u16 name_len ‖ name` (the fjall keyspace name, UTF-8) |
| 2 | `Rows` | `(u32 key_len ‖ key ‖ u32 value_len ‖ value)*`, at least one row. Uncompressed payloads target ≤ 1 MiB; a single larger row gets a frame of its own. With header flag bit 0 the payload is lz4 block-compressed with the uncompressed size prepended (`lz4_flex::compress_prepend_size`). |
| 3 | `TableEnd` | `u64 row_count` of the table |
| 4 | `BlobRefs` | `u32 n ‖ [sha256; n]`, sorted ascending, deduplicated: every blob referenced by the state (§5) |
| 5 | `Trailer` | `[32] logical_digest ‖ [32] state_hash ‖ u64 total_rows ‖ u64 frames` |

Order: for every table in `TABLES` order, `TableBegin`, zero or more `Rows`, `TableEnd`; then
exactly one `BlobRefs`; then the `Trailer`, which must be the last bytes of the stream.

Limits and guards: a frame payload above 64 MiB is rejected (corruption guard), as is an lz4
frame whose declared uncompressed size exceeds it, an empty `Rows` frame, trailing bytes inside
a payload, and any byte after the trailer.

**Forward compatibility.** An unknown frame kind `< 128` is an error (`UnknownFrame`). A kind
`≥ 128` is an extension: readers skip it (it still has to pass its CRC and is counted in
`Trailer.frames`). Extension frames may appear between tables, never inside one. New mandatory
information requires a new `snap_ver`; optional information uses an extension kind.

## 4. Row ordering and exclusions

- Tables appear in `TABLES` order (= the logical-digest order, storage-format.md §6 and §15),
  every table, empty ones included. A reader that knows a table which an older snapshot lacks
  treats it as empty (a keyspace added later). A table name the reader does not know is an
  error: that is a newer state format, already refused through `state_format`.
- Rows within a table are in strictly ascending key order (fjall order). Readers reject
  non-ascending keys.
- The meta row `checkpointed_offset` is **not exported**, exactly as it is excluded from the
  logical digest. Install writes it as `applied_offset`, so the installed store is a clean
  checkpoint at the snapshot's offset.
- Every other meta row (`format_ver`, `partition`, `partition_count`, `applied_offset`,
  `state_hash`, `cursor`) is exported verbatim. Readers check that the meta rows agree with the
  header (`applied_offset`, `partition`, `partition_count`) and with the trailer
  (`state_hash`).

## 5. Trailer, digest and blob refs

- `logical_digest` is computed with **exactly** the `store/digest.rs` layout
  (storage-format.md §9): blake3 over, for each table in `TABLES` order, `u16 LE name_len ‖
  name`, every row as `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`, then `u64 LE
  row_count`. So `store.digest()` of the source, the digest recomputed from the frames and
  `store.digest()` of the installed store are all equal. The writer and `digest()` share one
  incremental builder (`DigestBuilder`).
- `state_hash` is the meta `state_hash` (rolling hash) of the exported state.
- `total_rows` counts rows over all tables; `frames` counts every frame before the trailer
  (skipped extension frames included).
- `BlobRefs` lists the SHA-256 of every blob referenced by the state, found by the exhaustive
  table visitor `store/blobrefs.rs::visit_row` (an exhaustive `match` over `Table` without a
  wildcard arm, so a new keyspace does not compile until its refs are declared). It reads only
  typed `PayloadRef` fields of prost messages and documented postcard values; module blobs are
  never inferred from opaque payload bytes (F5). Readers recompute the set from the rows and
  reject a mismatch. Blob GC (08-19) and restore (08-18) use the frame without decoding rows.

A single flipped bit anywhere in the file is detected before anything is installed: in the
header by the header CRC, in a frame by its CRC32C, in a length field by truncation or
structure checks, and otherwise by the digest.

## 6. `state_format` and migrators (D18 amended)

D18 (amended, storage-format.md §13 "On-disk freeze policy" and §14.2): state values are derived
**while the full log is retained**. Once Phase 8 trims the WAL, snapshot rows become a source
of truth: the log prefix they summarise is gone.

Therefore:

- The header carries `state_format`, the meta `format_ver` of the exported rows.
- A reader **accepts every `state_format ≤` its own** and passes each row through
  `migrate_row(table, from_format, key, value)` before installing it. Today there is one format
  (1) and the migrator is the identity.
- A `state_format` newer than the reader's is refused (`FutureFormat`); the snapshot is not
  touched. (Phase 13 N/N-1 rolling upgrades: an N-1 follower cannot install an N leader's
  snapshot; upgrade the follower first.)
- **Every later STATE_FORMAT / VALUE_FORMAT change must ship a row migrator** `vN → vN+1` in
  `migrate_row` (rewriting the meta `format_ver` row too) and a format-corpus generation
  (storage-format.md §14.2). The same migrators run at store open.
- With migration, the trailer digest is checked against the rows **as read**, and the installed
  store is checked against the digest of the **migrated** rows.

## 7. Local files and staged install

**Local files** (08-09 owns the service; names are reserved in RESERVATIONS.md):

```
<data>/snapshots/pNNNN/<applied_offset, 20 digits>.osnap
<data>/snapshots/pNNNN/<applied_offset, 20 digits>.blobrefs   (cache of the BlobRefs frame)
```

A snapshot is written to `snapshots/pNNNN/.tmp-<offset>`, `fsync`ed, renamed to its final name
and the directory is `fsync`ed; only then may the trim floor move (P8-D2).

**Install** (`install_snapshot(data_dir, partition, partition_count, cfg, reader)`):

1. Refuse a snapshot of another partition or partition count (`Mismatch`) before writing
   anything. Run crash recovery (below). Build a fresh fjall database in
   `<data>/state/.install-pNNNN` with the production store configuration: every table's rows in
   order, meta last (with `checkpointed_offset = applied_offset`), then `persist(SyncAll)`.
   The stream is fully verified while it is read (CRCs, structure, key order, blob refs,
   trailer digest and counts). Any error removes the staging directory; the live state is never
   touched.
2. Close, reopen the staging database and check `digest() == trailer.logical_digest`, meta
   `applied_offset == checkpointed_offset == header.applied_offset`, partition, partition count,
   `format_ver` and `state_hash`.
3. Rename the live `state/pNNNN` (if any) to `state/.old-pNNNN`; rename `state/.install-pNNNN` to
   `state/pNNNN`; `fsync` `state/`; remove `state/.old-pNNNN`.

The partition's store must be closed during the install. Rows are written with `WriteBatch`
chunks of about 4 MiB; fjall ingestion was measured as an alternative (08-02 SUMMARY).
Correctness never depends on how rows reach disk: the reopen + digest check and the directory
renames decide.

**Crash recovery rule** (`recover_install`, run at the start of every install and by `Node::open`
before a partition's store is opened):

| Found under `state/` | Meaning | Action |
| -------------------- | ------- | ------ |
| `.install-pNNNN` | crash while building/verifying | remove it (the live state is intact) |
| `.old-pNNNN`, no `pNNNN` | crash between the two renames | rename `.old-pNNNN` back to `pNNNN` |
| `.old-pNNNN` and `pNNNN` | crash after the swap | remove `.old-pNNNN` (the new state is complete) |

So a reopen never sees a half-installed `pNNNN`: it is either the old directory or the fully
verified staging directory, each renamed whole. After recovery the install can simply be
retried.

## 8. Remote key (bottomless, 08-13)

```
p/<pppp>/snap/<applied_offset, 20 digits>-<sha256 hex of the whole file>.osnap
```

The object is immutable and written with `put_if_absent` (same sha = success, different sha =
fenced). A manifest only references snapshots with `snapshot_offset ≤ cut` (P8-D3). The sha256
covers the whole file; the frame CRCs and the trailer digest are checked again on install.

## 9. Streaming (Phase 12 follower install)

`OsnpReader` reads the header on construction, then one CRC-verified frame at a time
(`next_raw()` returns the frame as stored, `next_frame()` decodes it). `RawFrame::encode()`
re-emits a frame's exact bytes. A transport can therefore forward a snapshot frame by frame
without buffering it, and **resume at a frame boundary**: the receiver keeps the header and the
frames received so far and asks for the stream from frame index *i*. The receiver installs with
the same staged install (§7), so a partial transfer never reaches the live state. Because the
`Rows` frame target is 1 MiB, a resume loses at most about one frame of work.

Phase 12 also defines the divergent-tail rules: after install the follower's log starts at
`applied_offset`, and its log is truncated / refetched from there under the leader's epoch
history (§10).

## 10. Epoch history (F7)

The meta `cursor` row (postcard `CursorMeta`: `epoch`, `epoch_history: [(epoch, start_offset)]`,
`outbox_next_seq`, `outbox_truncated`) is exported verbatim, so the installed state carries the
full epoch history up to `applied_offset`. The header's `latest_epoch` is a convenience copy of
`cursor.epoch`. Phase 12 uses the epoch history from the snapshot to validate the leader's log
tail after `applied_offset` (fencing and divergence checks need the epoch boundaries even though
the log prefix was trimmed). `inbound_seen` rows (per-source shuffler dedupe watermarks) are an
ordinary table and are exported with it, which keeps cross-partition exactly-once across a
snapshot install.
