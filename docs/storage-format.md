# Ostro on-disk format (Phase 2, with Phase 4 additions in §12, Phase 5 additions in §13, the freeze rules in §14, Phase 8 additions in §15, Phase 6 additions in §16 and Phase 7 additions in §17)

**Status: FROZEN v0.1 (2026-11-19, plan 05-20).** First fixed at the end of Phase 2 (plan 02-16). This document describes what the code writes today. Each section names the source file that defines the format, and the golden files and tests that pin it. The frozen sources of truth, the evolution rules and the mixed-version rules are in **§14**; the on-disk corpus that pins this format is `tests/format-corpus/v0.1` (`crates/ostro-partition/tests/format_corpus.rs`). Ordinals and reserved names come only from `.planning/RESERVATIONS.md`.

**Freeze policy:** the format is **frozen** (FROZEN v0.1). From now on only additive changes are allowed: new command fields, new oneof variants, new enum values, new keyspaces, new meta keys, new outcome codes appended at the end, new meta variants or ext tags under D20 (§14). Field numbers, tags, codes and key layouts are never reused or renumbered. An incompatible change to a versioned format bumps its version (`node.toml` `format_version`, `META_FORMAT`, envelope `codec_ver`, value format byte), ships the decoders or migrators §14 requires, and updates this document in the same commit.

All multi-byte integers are **little-endian in the log** (WAL frame, envelope) and **big-endian in state keys** (so byte order equals numeric order).

---

## 1. Data directory layout

```
<data>/
├── node.toml                       # format_version = 1, partitions = P   (crates/ostro-partition/src/node.rs)
├── wal/
│   └── p/<id>/wal_<id>_<seg>.log   # partition <id>'s log segments (vendored ostro-wal), <seg> = 0, 1, 2, ...
├── state/
│   └── pNNNN/                      # partition NNNN's fjall database (zero-padded to 4 digits)
│       ├── <n>.jnl                 # fjall journal(s)
│       ├── keyspaces/<k>/...       # one LSM tree per keyspace (fjall-internal layout)
│       ├── version, lock           # fjall-internal
│   ├── .install-pNNNN/             # Phase 8: staging dir of a snapshot install, renamed over pNNNN (§15)
│   └── .old-pNNNN/                 # Phase 8: the replaced state during the install swap (§15)
├── snapshots/
│   └── pNNNN/<applied 20d>.osnap   # Phase 8: local OSNP v1 snapshots (+ .blobrefs sidecar, .tmp-* in flight), §15
├── backup/gc-ledger                # Phase 8: remote blob deletion ledger (only with bottomless on), §15
├── blobs/<hh>/<sha256 hex>         # content-addressed blobs, <hh> = first two hex chars (crates/ostro-blob)
└── tmp/<sha256 hex>.<pid>.<n>      # in-flight blob writes; deleted by BlobStore::open
```

- **`node.toml`**: two `key = value` lines, `format_version = 1` and `partitions = <P>`. Since Phase 12 it also holds `node_id` and `cluster_id` (additive keys, D-P12-16): both are bound **before meta starts, at the first boot, single-node included**. Absent → written durably (tmp + fsync + rename + dir fsync); present and different from the configured or resolved value → fatal. `cluster_id` comes from, in order: `[cluster] cluster_id`, `node.toml`, the existing CA's stored id, the `ClusterInfo.cluster_id` of a non-blank persisted meta, and only for a blank meta (single node) a fresh one; two present sources that disagree are fatal. `node_id = 0` is refused (the no-leader sentinel). A wiped meta directory over existing partition data fail-stops instead of creating a new cluster id (D-P12-14). The file is written durably (tmp + fsync + rename + dir fsync) on first start. A different `partitions` value at open is fatal (`NodeError::PartitionCountMismatch`, STOR-09). The partition count is also recorded in every state store.
- **WAL**: one shared group-commit WAL per node (`wal/`), topic `p`, one partition directory per partition. The vendored WAL opens a **new** segment on each process's first append to a partition, rotates at 250 MB or 30 min (vendored defaults), and fsyncs a segment before rotating away from it. Empty segment files can exist and are ignored. Retention deletes only whole segments below the safe floor (§8).
- **State**: one fjall `Database` per partition. Deleting `state/pNNNN` and reopening rebuilds that partition from its log (STOR-06). The fjall-internal file layout is not part of Ostro's format. Ostro's format is the logical content (keyspaces, keys, values) in §5–§6.
- **Blobs**: payloads over 256 KiB are written as `blobs/<hh>/<sha256>` (tmp write, fsync, rename, dir fsync) **before** the command that references them is appended. A visible blob is always complete. A `put` of a blob that already exists touches its mtime (Phase 8, 08-19), and blob GC (§15) deletes only unreferenced blobs older than its grace.

## 2. WAL record (vendored `ostro-wal` V2 frame)

Defined in `vendor/ostro-wal/src/record.rs`, parsed by `vendor/ostro-wal/src/truncate.rs` and `crates/ostro-log/src/recovery.rs`. Little-endian:

| off | field | type | value |
|---|---|---|---|
| 0 | magic | u16 | `0xCA7E` |
| 2 | version | u8 | 2 |
| 3 | flags | u8 | |
| 4 | length | u32 | bytes after this 12-byte header |
| 8 | crc32 | u32 | frame CRC32 (`WalRecord::calculate_checksum`) |
| 12 | topic_len | u16 | 1 |
| 14 | topic | bytes | `p` |
| 14+tl | partition | i32 | partition id |
| 18+tl | data_len | u32 | envelope length |
| 22+tl | canonical_data | bytes | **exactly one Ostro envelope** (§3) |
| 22+tl+dl | base_offset | i64 | envelope `base_off` |
| 30+tl+dl | last_offset | i64 | `base_off + cmd_count - 1` |
| 38+tl+dl | record_count | i32 | envelope `cmd_count` |

A record occupies `12 + length` bytes, and `12 + length == 42 + topic_len + data_len`. The vendored V2 `verify_checksum` is a no-op, so Ostro's verified reader (`crates/ostro-log/src/verify.rs`) checks every record itself, in this order: frame CRC, V2 only, partition, envelope (§3), envelope header vs trailer (base, count, last), `base == previous last + 1`, epoch non-decreasing. Offsets are contiguous across records and segments.

## 3. Envelope (`crates/ostro-types/src/envelope.rs`)

32-byte little-endian header followed by the body:

| off | field | type | value |
|---|---|---|---|
| 0 | magic | u32 | `b"OSTR"` |
| 4 | codec_ver | u16 | 1 (`CODEC_VERSION`) |
| 6 | flags | u16 | must be 0 |
| 8 | partition | u16 | |
| 10 | reserved | u16 | must be 0 |
| 12 | epoch | u32 | leader epoch that wrote the batch |
| 16 | base_off | u64 | offset of the first command |
| 24 | cmd_count | u32 | ≥ 1 |
| 28 | crc32c | u32 | CRC32C over `header[0..28] ‖ body` |

Body = `cmd_count × (u32 LE len ‖ command bytes)`. Verification order: length ≥ 32 → magic → codec → flags → reserved → CRC32C → `cmd_count ≥ 1` → exactly `cmd_count` commands (a clean short body is `CountMismatch`, an overrun is `BodyMalformed`) → no trailing bytes. The envelope CRC is the only integrity check that covers the command bytes. Default maximum envelope size is 4 MiB (`DEFAULT_MAX_ENVELOPE_BYTES`). Callers split larger batches.

## 4. Command set (`crates/ostro-types/src/command.rs`)

Commands are hand-written prost (protobuf) messages. Unknown fields are skipped on decode. Golden bytes: `crates/ostro-types/tests/golden/commands.txt`.

`Command { oneof cmd }` tags (8..=63 reserved for future variants):

| tag | variant | fields (number = name) |
|---|---|---|
| 1 | `LeaderAnnounced` | 1 epoch u32, 2 node_id u64, 3 announced_at_ms u64 |
| 2 | `StartRun` | 1 run_id bytes(18), 2 workflow string, 3 workflow_version u32, 4 queue string, 5 input PayloadRef, 6 idempotency_key bytes, 7 conflict_policy enum, 8 concurrency_key string, 9 priority i32, 10 ready_at_ms u64, 11 requested_at_ms u64, 12 parent ParentRef, 13 rng_seed bytes(32) |
| 3 | `AttemptGranted` | 1 run_id bytes, 2 token AttemptTokenMsg |
| 4 | `EventsAppended` | 1 run_id bytes, 2 token AttemptTokenMsg, 3 from_seq u64, 4 events repeated JournalEvent, 5 derived repeated Derived |
| 5 | `TimerFired` | 1 run_id bytes, 2 op u64, 3 due_at_ms u64, 4 fired_at_ms u64 |
| 6 | `InboundBatch` | 1 src u32, 2 msgs repeated InboundMsg |
| 7 | `OutboxTruncated` | 1 target u32, 2 upto_seq u64 |

Nested messages:

| message | fields |
|---|---|
| `ParentRef` | 1 run_id bytes, 2 op u64 |
| `AttemptTokenMsg` | 1 epoch u32, 2 n u32 |
| `JournalEvent` | 1 seq u64, 2 kind u32, 3 payload PayloadRef. **Opaque to Phase 2**: Phase 3 (`ostro-types/src/journal.rs`) owns kinds and payload semantics |
| `Derived { oneof intent }` | 1 ScheduleTimer, 2 CancelTimer, 3 SendMessage, 4 EnqueueReady, 5 CompleteRun |
| `ScheduleTimer` / `CancelTimer` | 1 op u64, 2 due_at_ms u64 |
| `SendMessage` | 1 msg OutboxMsg |
| `EnqueueReady` | 1 ready_at_ms u64 |
| `CompleteRun` | 1 status TerminalStatus, 2 result PayloadRef, 3 completed_at_ms u64 |
| `OutboxMsg { oneof kind }` | 1 StartChild, 2 ChildCompleted, 3 Signal, 4 CancelRun, 5 CronStart |
| `StartChild` | 1 start StartRun |
| `ChildCompleted` | 1 parent_run bytes, 2 child_run bytes, 3 op u64, 4 status TerminalStatus, 5 result PayloadRef |
| `Signal` | 1 target_run bytes, 2 name string, 3 payload PayloadRef |
| `CancelRun` | 1 target_run bytes, 2 reason string |
| `CronStart` | 1 schedule_id string, 2 fire_time_ms u64, 3 start StartRun |
| `InboundMsg` | 1 seq u64, 2 msg OutboxMsg |
| `PayloadRef { oneof kind }` (`payload.rs`) | 1 inline bytes, 2 blob BlobRef |
| `BlobRef` | 1 sha256 bytes(32), 2 len u64 |

Enums: `ConflictPolicy` ReturnExisting = 0, Reject = 1. `TerminalStatus` Completed = 0, Failed = 1, Cancelled = 2.

Inline payloads are limited to 256 KiB (`INLINE_PAYLOAD_LIMIT`). Apply rejects a larger inline payload with `PayloadTooLarge`.

## 5. Ids and routing (`crates/ostro-types/src/ids.rs`, `routing.rs`)

- **RunId**: 18 bytes = `partition u16 BE ‖ uuid (16 bytes, UUIDv7 at submit time)`. `RunId::partition()` is a pure byte read. String form: 29 characters of lowercase Crockford base32 (`0123456789abcdefghjkmnpqrstvwxyz`) over the 144 bits, with the final character's padding bits required to be zero (only canonical strings parse). `i`/`l` decode as `1` and `o` as `0`.
- **Routing**: keyed submission → `xxh3_64(u32 LE len(workflow) ‖ workflow ‖ idempotency_key) % P`. Unkeyed → `xxh3_64(uuid bytes) % P`. Pinned by `crates/ostro-types/tests/golden/routing.txt` (re-bless with `OSTRO_BLESS=1`) and an in-test xxh3 computation.
- Offsets are u64. Epochs are u32. Partition ids are u16. `PartitionCount` is 1..=65535 (defaults: 16 single node, 64 cluster).

## 6. State keyspaces (`crates/ostro-partition/src/store/`)

There are 16 fjall keyspaces per partition (13 in Phase 2; `running` added by 04-12, see §12; `waits` and `signal_seen` added by 05-02, see §13). Keyspace ordinals and on-disk names are allocated only in `.planning/RESERVATIONS.md` §6. Their declaration order (`TABLES`) is the digest order. Keys use big-endian integers. A string inside a key is `u16 BE len ‖ utf8`. A run id inside a key is its 18 raw bytes. Values marked *postcard* are `VALUE_FORMAT (0x01) ‖ postcard(value)` (`values.rs`). Values marked *prost* are the protobuf bytes of the named message.

| # | keyspace | key | value |
|---|---|---|---|
| 0 | `meta` | ASCII key name (§7) | see §7 |
| 1 | `runs` | run (18) | postcard `RunRecord` |
| 2 | `events` | run ‖ seq u64 | prost `JournalEvent` |
| 3 | `payloads` | run ‖ kind u8 (0 = input, 1 = result) | prost `PayloadRef` |
| 4 | `ready` | str queue ‖ !(priority as u32 ^ 0x8000_0000) u32 (higher priority first) ‖ ready_at_ms u64 ‖ run | postcard `ReadyValue { concurrency_key }` |
| 5 | `timers` | due_at_ms u64 ‖ run ‖ op u64 (op `u64::MAX` = delayed run start) | empty |
| 6 | `inbox` | two shapes (§13): legacy run ‖ `0xFFFF` u16 ‖ n u64, and signal run ‖ str name ‖ n u64 | legacy: postcard `InboxMsg` (ChildCompleted / CancelRequested / TimerFired; TimerFired is no longer written since 04-13); signal: postcard `SignalSlot` |
| 7 | `dedupe` | str workflow ‖ idempotency key (raw, rest of key); cron: str `"__cron/<schedule_id>"` ‖ fire_time_ms u64 | run id (18) |
| 8 | `outbox` | target u16 ‖ seq u64 (seqs start at 1) | prost `OutboxMsg` |
| 9 | `inbound_seen` | source partition u16 | highest applied seq u64 BE |
| 10 | `children` | parent run ‖ child run | empty |
| 11 | `dlq` | reserved (no rows written in Phase 2) | |
| 12 | `effects` | reserved (no rows written in Phase 2) | |
| 13 | `running` | run (18) | postcard `(epoch u32, n u32)`: the current attempt token (Phase 4, §12) |
| 14 | `waits` | `0x01` ‖ run ‖ op u64, or `0x02` ‖ run ‖ str name ‖ op u64 (§13) | `0x01`: postcard `WaitRow`; `0x02`: empty |
| 15 | `signal_seen` | run ‖ request_id (raw, rest of key) | empty (§13) |

`RunRecord` (postcard, field order is the encoding): workflow String, workflow_version u32, queue String, status `RunStatus` (Scheduled, Ready, Running, Completed, Failed, Cancelled), current_attempt `Option<(u32, u32)>`, next_seq u64, ready `Option<ReadySlot{priority i32, ready_at_ms u64}>`, parent `Option<([u8;18], u64)>`, cancel_requested bool, concurrency_key String, priority i32, created_at_ms u64, rng_seed `Vec<u8>`, inbox_next u64, children u32, then the Phase 4 trailing fields (§12): module_sha `Option<[u8;32]>`, tags `Vec<String>`, tenant `Option<String>`, traceparent `Option<String>`, run_timeout_ms `Option<u64>`, completed_at_ms `Option<u64>`. Because postcard is positional, changing a stored struct means a new `VALUE_FORMAT` byte or a new key. After the Phase 5 freeze this is binding; the Phase 4 change in §12 is the one deliberate pre-freeze exception.

## 7. Meta keys (`store/meta.rs`)

| key | value |
|---|---|
| `format_ver` | u32 BE = 1 |
| `partition` | u16 BE |
| `partition_count` | u16 BE |
| `applied_offset` | u64 BE: next offset to apply (every offset below it is applied) |
| `state_hash` | 32 bytes: rolling hash (§9) |
| `checkpointed_offset` | u64 BE: `applied_offset` at the last `persist(SyncAll)`. **Excluded from the digest** |
| `cursor` | postcard `CursorMeta { epoch u32, epoch_history Vec<(u32, u64)>, outbox_next_seq BTreeMap<u16,u64>, outbox_truncated BTreeMap<u16,u64> }` |

A fresh store writes every meta key in one batch and then persists. An existing store must have `format_ver = 1` and the configured partition id and count. Each apply chunk commits its data plus `applied_offset`, `state_hash`, `cursor` and changed `inbound_seen` rows in **one** atomic fjall `WriteBatch`.

## 8. Outcome codes (`crates/ostro-partition/src/apply/outcome.rs`)

Folded into the rolling hash, so they are part of the format. New codes are appended.

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
| Rejected(Invalid(_)) (any detail) | 26 |
| Rejected(RunCompleted) | 27 (Phase 5, §13) |
| Rejected(ReservedP6_12..14) | 28–30 (placeholders, never emitted; Phase 7, §17) |
| Rejected(RunPaused) | 31 (Phase 7, §17) |

`InvalidCode` details (Decode, Empty, BadRunId, MissingToken, EventsNotContiguous, MissingField, NotImplemented, BadIntent, FieldTooLong, BadSource, TruncateBeyondEnd; Phase 4: BadLength, NotRunning) are not hashed separately, because the command bytes that determine them are hashed. They are append-only.

## 9. Rolling hash and logical digest

- **Rolling hash** (`apply/mod.rs::fold_hash`), per applied command: `state_hash = blake3(prev ‖ offset u64 LE ‖ [outcome code] ‖ command bytes)`, starting from 32 zero bytes. Rejected commands are folded too, because they consume their offset.
- **Logical digest** (`store/digest.rs`): blake3 over one fjall snapshot. For each keyspace in `TABLES` order it hashes `u16 LE name_len ‖ name`, then every row in key order as `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`, then `u64 LE row_count`. The only skipped row is meta `checkpointed_offset`. Always compare digests, never fjall file bytes: file layout depends on flush and compaction timing.

## 10. Recovery rules

Order (`Node::open`, `crates/ostro-partition/src/node.rs`):
1. `node.toml` (format and partition count).
2. Open every state store in parallel. Its `applied_offset` is the partition's durable floor. An unreadable store gets floor `u64::MAX`, so its log tail is never cut.
3. Recover every log in parallel (`crates/ostro-log/src/recovery.rs`): list segments, read and verify every record (§2–§3), classify the first failure, then make the log durable.
   **Empty-log anchoring (Phase 12 F3, plan 05-21).** If a partition's log has no records (no segment files, only empty ones, or nothing left after a torn-tail cut) and its state store is readable, the log is anchored at the state's applied offset: `log_start = LEO = HW = applied_offset`, and the next append is assigned offset `applied_offset`. The log epoch cache, which is otherwise derived from envelope epochs, is seeded from `cursor.epoch_history` (`PartitionLog::seed_epochs`, called by `Node::open` before catch-up), so the new term's epoch is above every applied `LeaderAnnounced` epoch. No bytes are written by anchoring. An unreadable state (floor sentinel `u64::MAX`) anchors at 0. A **non-empty** log with `applied > LEO` stays fatal ("state ahead of log"), and `applied < log_start` stays "snapshot required". Anchoring does not change the torn-tail rule: with no verified record before it, a bad first record counts as above the floor only when `applied_offset == 0`.
4. Validate: for a non-empty log, `applied > LEO` is "state ahead of log" (fatal for the partition); `applied < log_start` is "snapshot required". An empty log is anchored (above) and always validates.
5. Replay from the state's **own `applied_offset`** (not `checkpointed_offset`), up to the HW. Then the epoch bump, `LeaderAnnounced{e+1}` as the first entry of the new term, readiness barrier, leader services.

Any per-partition failure marks only that partition `Failed`.

**Torn tail vs corruption.** The first bad record is torn, and its segment is cut at the record start (`set_len` + fsync file + fsync dir) before the WAL opens, only if all of the following hold:
- it is in the **last non-empty segment**;
- it starts at or above the floor (`expected base ≥ applied_offset`);
- it is incomplete at EOF, or contains an all-zero 512-byte aligned sector, or has an unreadable header followed by zeros to EOF.

Anything else fails the partition with `LogError::Corrupt`, naming the segment and byte position. Nothing is ever skipped. Records after a cut are discarded too: they were in the same un-fsynced window and cannot have been acknowledged.

**Durability after recovery.** Only the last non-empty segment can hold bytes that were never fsynced. Earlier segments were fsynced by the WAL before it rotated away from them, or were the tail at an earlier recovery, which fsynced them before any newer segment could be created. Recovery therefore fsyncs the last non-empty segment, which makes the whole retained log durable, and HW starts at the recovered LEO. Before plan 02-16 recovery fsynced every segment. That cost about 0.55 ms per segment and made reopen grow with the restart count.

**State durability.** Apply commits use fjall's buffered journal (`write(2)`, no fsync). `checkpoint` writes `checkpointed_offset` and calls `persist(SyncAll)` every 100 ms or 256 chunks (`MachineConfig`), on `force_checkpoint`, and on graceful shutdown. After a crash, fjall may reopen ahead of the last checkpoint. That is safe because the state only ever applies WAL-durable commands (HW = fsynced LEO at RF=1).

**Trim floor.** Retention deletes whole segments below `min(durable checkpoint, snapshot_floor, retention_floor)`, and only with a `TrimPermit` minted after the partition's readiness barrier opened. `snapshot_floor` stayed 0 until Phase 8, so Phases 2–7 never deleted log segments. From Phase 8 on the floor is `min(durable checkpoint, local snapshot floor, upload floor?, retention floor?)` (P8-D2, §15), so trimming keeps the invariant `log_start ≤ applied_offset(oldest retained local snapshot)`.

### 10.1 Phase 12 behavioural notes (post-freeze; no byte format changed)

Phase 12 (replication) changed **no on-disk byte format**: WAL frames, envelopes, commands,
state keyspaces, OSNP v1 and the meta encodings are as frozen in §14. Replication carries
envelopes verbatim (§14.5 F1). The changes below are recovery and open semantics only, which
the freeze allows (§14.2).

- **HW at open = `applied_offset` (F4).** A partition log opens unassigned (role `Offline`)
  with its HW at the state store's `applied_offset`, clamped into `[log_start, LEO]`, never at
  the recovered LEO: in a cluster the tail above `applied_offset` may be uncommitted, and a
  follower must not apply it. A leader whose ISR is `{self}` (RF=1) raises the HW to its
  durable LEO the moment it becomes leader; with RF > 1 the HW moves only through the replica
  tracker (`min(durable LEO over the ISR)`, published at the single `HwVector::publish_hw`
  point). Step 5 above therefore replays up to the HW the role sets, and the epoch of the new
  term is the meta-assigned one (§14.5 F2).
- **Torn first record of an anchored log (D-P12-4, parsed-base rule).** In
  `recover_partition`, with `floor > 0` and no verified record before the bad one, the first
  bad record counts as **torn** (cut, as at floor 0) iff all hold: it is in the last segment;
  it has the existing torn shape (runs to EOF / zero-filled tail); and **either** its frame
  header is complete and its parsed base offset equals `floor`, **or** fewer bytes than one
  frame header remain from its start to EOF. A complete header whose base differs from
  `floor` stays `Corrupt`. As built: the WAL V2 frame header carries no offset (base and last
  are in the trailer after the payload), so the base is read from the envelope header right
  after the topic, partition and payload length, i.e. the first 47 bytes of the record
  (`BASE_PREFIX_LEN`); "one frame header" above means that prefix. A prefix that does not
  parse also stays `Corrupt`, and the "state unreadable" floor sentinel never qualifies. Why
  this shape can only be a torn write: install (`reset_to`) fsyncs the emptied partition
  directory, so after a crash the log is either the old one or empty, and the first append
  after an anchor is the only way to reach it (power loss before its first fsync). Pinned by
  `torn_first_record_of_anchored_log` and a header-mismatch variant that stays `Corrupt`
  (`crates/ostro-log/tests/recovery_empty_anchor.rs`). The marker-file alternative was
  rejected because it would add an on-disk artefact next to frozen WAL files.
- **Snapshot install resets the log first** (`reset_to(S)`: delete all segments, fsync the
  directory, anchor at `S`, seed the epoch cache from the snapshot's `cursor.epoch_history`),
  then installs the state. A crash between the two leaves an empty log with the old state,
  which empty-log anchoring (step 3) opens at the old `applied_offset`; the follower then asks
  for the snapshot again. See `docs/snapshot-format.md` § "Phase 12 follower install".
- **Trimming on followers.** Followers take their own snapshots and trim their own logs with
  the same floor rule (above); a follower whose fetch offset falls below the leader's
  `log_start` installs a snapshot (`SNAPSHOT_REQUIRED`).

## 11. fjall configuration and measurements

`StoreConfig::default()` (`crates/ostro-partition/src/store/config.rs`), one fjall 3.1.13 database per partition:

| setting | default | why |
|---|---|---|
| `worker_threads` | 1 | P databases means P worker pools; 2 workers measured within noise |
| `max_journaling_size` | 64 MiB (fjall's minimum) | bounds the journal replayed on reopen |
| `max_memtable_size` (per keyspace) | 8 MiB | fjall's 64 MiB default reproduces the S3 ~120 ms write stall |
| `cache_size` (block cache) | 16 MiB | per partition, so memory scales with P; see the throughput note below |

How fjall bounds the replay window: each journal file is preallocated to 64 MiB. The active journal is rotated during a memtable flush once its write position passes 64,000,000 bytes. When sealed journals reach `max_journaling_size`, the keyspaces pinning the oldest journal are flushed. Reopen replays the un-flushed journal(s): about one active journal plus sealed ones up to the cap.

Measurements from plan 02-16, 2026-11-18/19. Same dev machine as spike S3: WSL2, ext4 tempdirs, 12 threads, release builds. These are not NFR evidence; they are the bounds the defaults were checked against. Commands: `cargo test --release -p ostro-crashtest --test reopen_time|apply_throughput -- --ignored --nocapture --test-threads=1`.

**Reopen after kill -9 at the journal cap** (`reopen_time_store_variants`). P=4, `crash-target --pipeline 64 --blob-pct 0` for 20 s (about 800k acks, 1.3M commands applied), then SIGKILL. Loadavg before the first variant was 1.92. The workload itself drives it to 3–6, which is why the later variants show higher values.

| variant | store open ms per partition (p50 / max) | WAL scan ms (p50 / max) | Node::open ms (Leader, all Ready) |
|---|---|---|---|
| **default** | **637.7 / 660.1** | 62.1 / 80.5 | **871.8** |
| journal 128 MiB | 616.3 / 632.5 | 78.7 / 80.9 | 1,037.9 |
| memtable 32 MiB | 876.3 / 897.8 | 62.1 / 100.1 | 1,403.9 |
| workers 2 | 604.0 / 605.1 | 69.6 / 94.7 | 827.9 |

A second run of the default gave store open 620.0 / 672.0 ms and Node::open 922.7 ms. Reopen stays under the 1 s per partition target. S3 measured 3.2–3.4 s with fjall's 512 MiB default journal.

**Reopen vs WAL segment count** (`reopen_time_segments`). P=4, 300 kill -9 cycles of 5–300 ms each. Every process start opens a new segment per partition, so there are 300 segments per partition by the end (about 24.7k offsets each).

| | Inspect Node::open at 300 restarts | recover_partition (max partition) |
|---|---|---|
| fsync every retained segment (before 02-16) | 741.1 ms | 182.6 ms |
| fsync only the last non-empty segment (§10) | 153.8 ms | 11.5 ms |

Breakdown at 300 segments, summed over P=4: reading and verifying every segment takes 7–9 ms, fsyncing every (already clean) segment 660–671 ms, fsyncing the last one 2.2 ms. The remaining growth (about 0.5 ms per restart) comes from other per-segment work (listing, opening, reading). Not merging the small per-restart segments is tracked as a todo.

**Apply throughput, read-modify-write** (`apply_throughput_*`). S3's `--rmw` workload: 1M preloaded and granted runs, 200k measured commands, 25% new `StartRun`, 75% update (90% of picks from the 64Ki most recent runs, 10% uniform). An update is `AttemptGranted` or `EventsAppended`. Loadavg 1.1–1.8.

| level | config | cmds/s | keyspace reads/cmd | batch ms p50 / p99 / max | batches ≥ 100 ms | checkpoint ms p50 / max |
|---|---|---|---|---|---|---|
| store, chunk 64 | default (16 MiB cache) | **80,832** | 1.00 | 0.68 / 1.28 / 33.38 | 0 | 4.42 / 6.21 |
| store, chunk 1024 | default | 76,286 | 1.00 | 11.83 / 36.17 / 45.14 | 0 | 4.46 / 6.63 |
| store, chunk 64 | cache 64 MiB | 84,589 | 1.00 | 0.66 / 1.21 / 40.56 | 0 | 4.31 / 6.86 |
| store, chunk 64 | **cache 128 MiB** | **152,985** | 1.00 | 0.33 / 0.56 / 31.21 | 0 | 4.81 / 5.11 |
| store, chunk 64 | memtable 64 MiB | 66,559 | 1.00 | 0.37 / 3.50 / **120.65** | 1 | 3.94 / 6.24 |
| store, chunk 64 | workers 2 | 84,180 | 1.00 | 0.65 / 1.24 / 37.47 | 0 | 4.46 / 6.22 |
| machine (P=1 Node, WAL + apply thread), 64 cmds/envelope, 32 submitters | default | **83,634** end to end | | 10.68 / 26.15 / 50.08 | 0 | 4.62 / 11.11 |
| machine, 1 cmd/envelope, 32 submitters | default | 6,581 (latency-bound) | | 0.26 / 0.93 / 51.88 | 0 | 3.00 / 8.45 |

Findings:
- The S3 ~120 ms write stall does **not** appear with the 8 MiB memtables: no apply batch reached 100 ms, and the worst was 50 ms at machine level. Setting fjall's 64 MiB memtables reproduces it (120.65 ms).
- Apply keeps to exactly one keyspace point read per command.
- **Store-level RMW is below the S3 90k cmds/s rule at the default 16 MiB block cache (80.8k).** It reaches 153k once the read working set fits the block cache (cliff between 64 and 128 MiB for 1M runs, because the 10% uniform picks touch the whole `runs` keyspace). This is a per-partition memory-budget question: fjall 3.1.13 has no public API to share one cache across databases. The default stays at 16 MiB for now. The open concern is recorded in STATE.md: size the cache from a node-level memory budget (`budget / P`) in server configuration.

## 12. Phase 4 additions (pre-freeze)

Plans 04-04, 04-09 and 04-12 changed the format before the Phase 5 freeze. The command-set changes are additive: old bytes decode, and re-encode to the same bytes. The state change to `RunRecord` is **not** additive. It is a pre-freeze format change with **no migration** and no `VALUE_FORMAT` bump. **Delete dev data dirs created before Phase 4**: an old `runs` row fails to decode (`StoreError::Codec`).

**Commands** (`crates/ostro-types/src/command.rs`; golden vectors `start_run_v4`, `events_appended_suspend` appended to `commands.txt`):

| change | detail |
|---|---|
| `StartRun` 14 `module_sha` bytes | empty = none, else exactly 32 bytes (`MODULE_SHA_LEN`, sha256 of the module). Any other length is rejected `Invalid(BadLength)` |
| `StartRun` 15 `tags` repeated string | at most 64 tags (`MAX_RUN_TAGS`), each at most 128 bytes (`MAX_RUN_TAG_LEN`), else `Invalid(FieldTooLong)` |
| `StartRun` 16 `tenant` string | empty = none, at most 128 bytes (`MAX_TENANT_LEN`), else `Invalid(FieldTooLong)` |
| `StartRun` 17 `traceparent` string | empty = none, at most 256 bytes (`MAX_TRACEPARENT_LEN`), length checked only (no W3C syntax check), else `Invalid(FieldTooLong)` |
| `StartRun` 18 `run_timeout_ms` u64 | 0 = none |
| `Derived` 6 = `Suspend {}` | empty message. See the apply rules below |

Tags still free: `Derived` oneof 7, `StartRun` 19 and `Command` 8. Phase 5 reserves `StartRun` 19..23 and `Command` 8..12.

**Journal:** event kind 29 = `StepAttemptFailed` (plan 04-04). Additive, no journal codec bump: replay of a journal without kind 29 is unchanged.

**State** (`crates/ostro-partition/src/store/`):

- `RunRecord` gains trailing fields, in this order: module_sha `Option<[u8;32]>`, tags `Vec<String>`, tenant `Option<String>`, traceparent `Option<String>`, run_timeout_ms `Option<u64>` (all from `StartRun`, empty/0 stored as `None`), completed_at_ms `Option<u64>` (from `CompleteRun.completed_at_ms`, 0 stored as `None`).
- New keyspace 13 `running`: key = run id (18), value = postcard `(epoch u32, n u32)`. A row exists **exactly while the run's status is `Running`**. `AttemptGranted` inserts it, or overwrites it with the new token. Every transition out of `Running` blind-removes it: `EnqueueReady`, `Suspend` and `CompleteRun`. Inbound messages and timers only wake `Scheduled` runs, so they never touch it. A new leader rebuilds its in-flight set from this keyspace (`ostro_partition::query::running_runs`) without scanning `runs`. Appending a keyspace changes the logical digest of every store, because the digest hashes each keyspace's name and row count. Existing rows are unchanged.

**Apply rules** (`crates/ostro-partition/src/apply/run.rs`):

- `Suspend` must be the **last** intent of its `EventsAppended`. It cannot be combined with `EnqueueReady` or `CompleteRun` (either order) or with a second `Suspend`, else `Invalid(BadIntent)`. The run must be `Running`, else `Invalid(NotRunning)`.
- Effect: status becomes `Scheduled`; the ready row (if any) and the `running` row are removed; `current_attempt` is kept (for fencing). No action is emitted. A later timer, signal, child result or cancel request makes the run Ready again through the existing wake-up path.
- **CAS:** the `from_seq == next_seq` check that every `EventsAppended` already passes acts as the compare-and-set. If an input is journaled after the executor's last flush, `next_seq` moves (plan 04-13), the Suspend is rejected `SeqMismatch{expected}`, and the executor re-activates. A rejected Suspend leaves the state unchanged.
- **`TimerFired` journals instead of using the inbox (plan 04-13).** For a run's timer op (not the start-delay op `u64::MAX`, which keeps its Phase 2 behavior), apply writes a journal event `TimerFired { op }` at `rec.next_seq` into `events`: a prost `JournalEvent{seq, kind 64, payload: inline}` holding the `encode_event` bytes, with `ts_nanos = fired_at_ms * 1_000_000`. Then `next_seq` moves by one. No inbox row is written and `inbox_next` is not touched. Signals, child results and cancel requests still go to `inbox` until Phase 5. The wake-up depends on the status. `Scheduled` becomes `Ready` (`EnqueueReady`, ready at `fired_at_ms`). `Running` stays Running, gets `Action::InputAppended{run, seq}`, and its `running` row is untouched. `Ready` only gets the event. Terminal and absent runs: the timer row is removed, nothing else. A re-fire finds no timer row and is `Duplicate`. Any `EventsAppended` built before the input was seen gets `SeqMismatch{expected: seq + 1}`, a `Suspend` included. Reads are unchanged: at most two, the timer row and then the run record.
- Read budget: unchanged. `StartRun` field checks run before the point read. `AttemptGranted` adds one blind insert. `Suspend`, `EnqueueReady` and `CompleteRun` add one blind remove each. No new point reads.

**Actions** (`apply/outcome.rs`, not persisted): `Action::InputAppended { run, seq }`. It means an input was journaled at `seq` for a `Running` run, so the dispatcher can forward it to the hot activation. The variant exists since 04-12; plan 04-13 emits it.

**Outcome codes:** unchanged. New `InvalidCode` details `BadLength` and `NotRunning` fold as code 26, like every `Invalid`.

## 13. Phase 5 additions (pre-freeze, plan 05-02)

Plan 05-02 changed the state format before the Phase 5 freeze. The command-set and journal additions (Cmd 8..12, StartRun 19..23, Derived 7..8, statuses 3..5, outbox kind 6, journal kinds 70/71/96/97) are additive and pinned by the appended goldens of plan 05-01. The state changes below are **not** additive: this is a pre-freeze positional change with **no migration** and no `VALUE_FORMAT` bump. **Delete dev data dirs created before Phase 5**: an old `runs` row fails to decode (`StoreError::Codec`). Plan 05-20 marked this document FROZEN v0.1 (2026-11-19); this section is now frozen like the rest (§14). Empty-log anchoring (plan 05-21) is in §10.

Ordinals and names (keyspaces, outcome codes, `InvalidCode` details, commands, journal kinds) come only from `.planning/RESERVATIONS.md`. Its **placeholder rule** binds keyspaces: a phase that lands a keyspace above a reserved slot that does not exist yet also declares the lower slot as an empty keyspace with the name fixed there, so the digest order is the same whatever order the phases land in. An empty keyspace adds only `name + row_count 0` to the digest.

### Keyspaces

| # | keyspace | key | value |
|---|---|---|---|
| 14 | `waits` | wait row: `0x01` ‖ run ‖ op u64 BE | postcard `WaitRow { kind: WaitKind (Signal, Token), name String, timeout_due_ms u64 }` |
| 14 | `waits` | FIFO index: `0x02` ‖ run ‖ name_len u16 BE ‖ name ‖ op u64 BE | empty |
| 15 | `signal_seen` | run ‖ request_id (raw, rest of key; at most `MAX_REQUEST_ID_LEN` = 64 bytes) | empty |

- `waits` uses **one keyspace with two key shapes**, told apart by the first byte. `wait_prefixes(run)` returns both run prefixes, and `wait_name_prefix(run, name)` scans one name's waits **oldest op first** (FIFO: one signal satisfies exactly one wait, the oldest). `RunRecord.open_waits` counts the live wait rows.
- A wait with `timeout_due_ms > 0` also has the timer row `(timeout_due_ms, run, op)` in `timers`. When it fires while the wait row exists, apply journals `WaitTimedOut{op}` (kind 70, plan 05-08).
- `signal_seen` holds D13 signal dedupe keys (`SignalRun.request_id`). Its rows are deleted with the run (prefix `run`).

### Inbox re-key (signals) and `SignalSlot`

The `inbox` keyspace now has two key shapes. Both use `RunRecord.inbox_next` for `n`.

| shape | key | value |
|---|---|---|
| signal | run ‖ name_len u16 BE ‖ name ‖ n u64 BE (`signal_inbox_key`) | postcard `SignalSlot` |
| legacy | run ‖ `0xFFFF` u16 ‖ n u64 BE (`inbox_key`) | postcard `InboxMsg` |

- Signal names are at most `MAX_SIGNAL_NAME_LEN` (256) bytes, so a signal key never has the name length `0xFFFF`, and a `signal_inbox_prefix(run, name)` scan never sees a legacy row. The scan yields one name's buffered signals in **arrival order**.
- `SignalSlot` (postcard enum, variant order is the encoding):
  - `Journaled { payload_seq u64, sent_at_ms u64 }`: the signal's `SignalPayload{name, payload}` event (kind 96) is already journaled at `payload_seq`. A later match journals `SignalMatched{op, payload_seq}` (kind 97). The slot holds no payload bytes (D21).
  - `Pending { event PayloadRef, sent_at_ms u64 }` (B1): the signal arrived while the run's journal was empty (`next_seq == 0`), so its submitter-encoded `SignalPayload` event bytes are kept here. `event` is the prost encoding of a `PayloadRef` (inline ≤ 256 KiB, or a blob), stored as a postcard byte string. The seq-0 batch (`EventsAppended` with `from_seq == 0`, or `StartRun.run_started` from 05-16) journals each pending event right after its own events and before its intents, and converts the slot to `Journaled`.
- Inbound outbox `Signal` (its `payload` is the sender-encoded `SignalPayload` event, D21) writes a signal row: `Pending{event, sent_at_ms: 0}` while `next_seq == 0`, else it journals the event verbatim and stores `Journaled{payload_seq, sent_at_ms: 0}` (the outbox message carries no send time). Plan 05-08 adds the wait match in front of this branch.
- Legacy rows (`InboxMsg`, variant order is the encoding: `ChildCompleted`, `CancelRequested`, `TimerFired`; the Phase 2 `Signal` variant was removed by 05-02): inbound `ChildCompleted` keeps writing `ChildCompleted` until Phase 9 journals it as an input. Inbound `CancelRun` keeps writing `CancelRequested` until plan 05-08 journals it. `TimerFired` has not been written since 04-13.

### `RunRecord` trailing fields

Appended after `completed_at_ms`, in this order (the encoding):

| field | type | meaning |
|---|---|---|
| schedule_to_start_ms | u64 | `StartRun` 19 (0 = none; initial start only, D2) |
| retention_ms | u64 | `StartRun` 20 (0 = keep forever; D9) |
| idempotency_key | `Vec<u8>` | `StartRun` 6, so expiry can delete the `(workflow, key)` dedupe row |
| terminal_reason | String | `run_timeout`, `schedule_to_start` or an operator's terminate reason; empty for executor completions |
| resumable | bool | `CompleteRun.resumable` (resume-in-place retry, D1) |
| retry_of | `Option<[u8;18]>` | `StartRun` 21 (empty = none, else an 18-byte run id, else `Invalid(BadRunId)`) |
| continued_from | `Option<[u8;18]>` | `StartRun` 22 (same rule) |
| cancel_reason | String | reason of the first graceful cancel request |
| resumed_count | u32 | resume-in-place retries |
| open_waits | u32 | live `waits` wait rows |
| s2s_due_ms | u64 | due time of the pending `SCHEDULE_TO_START_OP` timer (0 = none) |
| attempt_failures | u32 | durable retryable attempt failures (F9) |
| status_seq | `Option<u64>` | seq of the last `StatusPublished` event (F10) |
| continued_to | `Option<[u8;18]>` | successor of a continued-as-new run (PF-F2) |
| pause | `Option<PauseMark { reason u8, at_ms u64, detail String }>` | Phase 7 F1; stored only, never set in Phase 5 |

`RunStatus` gained `Terminated`, `TimedOut`, `ContinuedAsNew` (postcard indices 6, 7, 8; plan 05-01). `RunRecord`, `ReadyValue` and `PauseMark` derive `Default` so that code building records by hand keeps compiling when fields are appended.

**`StartRun` 23 `run_started` (optional `JournalEvent`; apply 05-16, set by the API from 05-25, D24).** The submitter's fully encoded `RunStarted` (D21 form, `PayloadRef` inline ≤ 256 KiB or a blob), journaled verbatim at seq 0 by the `StartRun` apply, which then sets `next_seq = 1` and runs the B1 first-events hook. Apply checks only `seq == 0`, `kind == RunStarted`, presence and the inline limit; it never decodes the bytes. **From 05-25 the API always sets it**: `POST /v1/workflows/{w}/runs` (and gRPC `StartRun`) and the retry-restart path (`StartRun.retry_of`). So a run that was accepted but never activated has `RunStarted` at seq 0 (`next_seq == 1`), signals and cancels sent before its first activation are journaled directly at seq ≥ 1, and terminate or a schedule-to-start timeout before start journals `RunTerminated` at seq 1 (D3's "only when `next_seq > 0`" rule still holds; it is simply always true on these runs). The bytes equal the executor's own synthesis (`run_started::encode_for_start`), and the executor skips its RunStarted append when `next_seq > 0`. The field stays empty on `StartRun`s built by inbound outbox messages, child starts and continue-as-new successors: those runs still start with an empty journal and use the B1 `Pending` signal slots. A dry run builds nothing.

### Apply seams

- **Journaling helpers** (`apply/inputs.rs`). `journal_input` encodes an apply-built event without user-sized data (TimerFired, WaitTimedOut, CancelRequested, RunTerminated, SignalMatched) at `next_seq`, with `ts_nanos = ts_ms * 1_000_000`. `journal_encoded` writes submitter-encoded event bytes **verbatim** (D21): the row is `JournalEvent{seq: next_seq, kind, payload}` with the command's `PayloadRef`, inline or blob, never decoded, re-encoded or fetched. Both move `next_seq` by one and wake the run: `Scheduled` → `Ready` + `EnqueueReady`, `Running` → `Action::InputAppended{run, seq}`, `Ready` → nothing. The 04-13 `TimerFired` path uses `journal_input` with unchanged behaviour.
- **Terminal hook** (`apply/terminal.rs`). Every terminal transition calls `terminal::on_terminal`. When `open_waits > 0` it scans both `waits` prefixes, deletes every wait row, FIFO index row and pending wait-timeout timer row (emitting `Action::CancelTimer` for each timer), and sets `open_waits = 0`. With `open_waits == 0` it issues no read at all. So a stale wait timer can never surface after a resume (D4). `CompleteRun` sets `resumable` from the intent and clears `terminal_reason`.
- **Reserved timer ops.** `TimerFired` for `RUN_TIMEOUT_OP` / `SCHEDULE_TO_START_OP` routes to `lifecycle::on_reserved_timer` and for `EXPIRE_OP` to `retention::on_expire` (05-09). These are routed before the terminal-run check, because expiry acts on terminal runs. `START_DELAY_OP` keeps its Phase 2 path. Any other op first tries `waits::try_wait_timeout` (05-08), then the ordinary `TimerFired` journal path. "Timer row absent = `Duplicate`" is unchanged.
- **First seq-0 write** (B1). After an `EventsAppended` with `from_seq == 0` writes its events and before its intents are applied, apply calls `waits::on_first_events`, which journals pending signals (05-08).
- **`CompleteRun.parent_event`**: an inline `PayloadRef` over 256 KiB is rejected `PayloadTooLarge`, like every other inline payload. A blob ref is accepted.
- **New outcome code**: `RejectReason::RunCompleted` = index 11, code 27 (RESERVATIONS §7). `InvalidCode::NotImplemented` already existed at index 6 and is reused.

### Read budget exceptions

The hot path keeps at most one keyspace read per command (`range_prefix` counts as one). The exceptions below are documented here and asserted by tests (05-02, 05-08):

| command / path | reads |
|---|---|
| `SignalRun` | run record + waits-by-name prefix + `signal_seen` point read when `request_id` is set |
| `RegisterWait` intent | + one inbox-by-name prefix (buffered signal match) |
| terminal `waits` cleanup | + the two `waits` prefixes, only when `open_waits > 0` |
| first seq-0 write | + one inbox prefix scan, only when the run may hold pending slots (`inbox_next > 0`) |
| `TimerFired` | timer row + run record (unchanged), plus the reserved-op handler's own reads |

### On-disk freeze policy (amended D18)

(Superseded in detail by §14, which is the frozen text; kept here as written by 05-02.)

- **Frozen sources of truth:** the log (WAL frame, envelope, `Command` prost encoding, journal event codec, command hash), `node.toml`, the blob layout, and the meta Raft log and snapshot encoding.
- **`node.toml`**: readers ignore unknown keys, and new keys are additive (Phase 8 F6, Phase 12 `node_id`/`cluster_id`). An existing key never changes meaning.
- **Partition state-store values are derived** while the full log is retained: deleting `state/pNNNN` rebuilds them from the log. That is why pre-freeze value changes (this section, §12) need no migration.
- **After Phase 8 enables log trimming**, snapshot rows become a source of truth. From then on every `STATE_FORMAT` / `VALUE_FORMAT` change ships a **row migrator** (vN → vN+1), applied at snapshot install and at store open. Snapshots carry `state_format`, and readers accept every older snapshot format (also for Phase 12 follower install and Phase 13 N/N-1 upgrades).
- **BlobRef invariant** (Phase 8 F5): a `BlobRef` appears only in a `PayloadRef` field of a prost message (log commands included) or of a documented state value (postcard `SignalSlot::Pending.event`, the prost `PayloadRef` rows of `payloads`, the prost `JournalEvent` rows of `events`), **never inside opaque payload bytes**. Blob GC and backups can therefore find every reference without decoding journal payloads.
- Node open, backup/restore and blob GC ignore unknown data-dir entries (Phase 14 F8, e.g. `<data>/audit/`).
- **`<data>/LOCK`** (plan 05-26): the data-dir lock file. `ostro dev` / `ostro server` hold `flock(LOCK_EX|LOCK_NB)` on it for the server's lifetime, taken before any other disk work; a second process on the same directory fails with `data dir <data> is in use by another ostro process (lock <data>/LOCK)`. The content is unused (empty) and not part of the format. The lock belongs to the open descriptor, so a `LOCK` file left by a killed process does not block the next start. Tools that open `Node` / meta directly (Inspect mode, `export_run`, the format-corpus generator) do not take it and must run on a copy or a stopped directory. Older code ignores the entry (F8).

## 14. FROZEN v0.1 (2026-11-19, plan 05-20)

This section is the frozen text. It binds every later phase. Where it and an earlier section disagree, this section wins. Ordinals and reserved names come only from `.planning/RESERVATIONS.md`, and pinned bytes come only from the goldens and corpora listed here.

### 14.1 Frozen sources of truth and their pins

| Source of truth | Defined in | Pinned by |
|---|---|---|
| WAL frame (vendored `ostro-wal` V2) | §2 | `crates/ostro-log` tests, format corpus |
| Envelope (32-byte LE header + CRC32C, `codec_ver`) | §3, `crates/ostro-types/src/envelope.rs` | format corpus |
| Partition `Command` prost encoding (Cmd tags 1–12, `StartRun` 1–23, `Derived` 1–8, outbox kinds 1–6) | §4, §12, §13, `crates/ostro-types/src/command.rs` | `crates/ostro-types/tests/golden/commands.txt` (22 lines) |
| Journal event codec (`EVENT_CODEC_VER` 1) and the per-kind command hash | `crates/ostro-types/src/journal.rs` | `journal_v1.txt` (27 event, 14 result, 14 hash lines) |
| Ids and routing | §5 | `routing.txt` (49 lines) |
| Meta Raft log and `MetaState` snapshot (postcard, positional), **`META_FORMAT = 2`** (D20 frozen baseline; the pre-Phase-5 format 1 is rejected explicitly) | `crates/ostro-meta/src/{command,records,state,ext}.rs` | `crates/ostro-meta/tests/golden/meta_v2.txt` (17 record, 4 command, 2 snapshot lines) |
| `node.toml` | §1, §14.4 | `crates/ostro-partition` node tests |
| Blob layout (`blobs/<hh>/<sha256>`) | §1, `crates/ostro-blob` | format corpus (one blob) |

Every golden above is pinned as an append-only prefix by `crates/ostro-types/tests/golden_frozen.rs` (line count + sha256 per section). A pin may only be extended over an unchanged prefix, never changed. `wit/FROZEN.toml` byte-pins `wit/ostro-runtime-0.1.0/runtime.wit` the same way (WIT-07).

**Format corpus.** `tests/format-corpus/v0.1/{data/, format.toml}` is a P=2 data dir (48 runs, about 410 KiB, WAL rotation 4096 bytes, several segments, one blob). It covers every command kind, every outbox kind and every `Derived` intent, including `StartRun.run_started` (tag 23, D24) with an inline and a blob `PayloadRef`. `format_corpus.rs` opens a copy in Inspect mode, compares `state_hash`, LEO, applied and every recorded per-table digest with `format.toml`, rebuilds a second copy from the log alone and compares again. The corpus is never re-generated. **Versioning (D18 caveat):** per-table digests pin *derived* state values. A later `STATE_FORMAT`/`VALUE_FORMAT` change that ships a row migrator changes the digests of a rebuilt store, so it adds a new `tests/format-corpus/v0.N/` next to `v0.1` (generated at that change) and keeps `v0.1` verifying through the migrator: `v0.1` must still open, its log must still decode, and its migrated state must equal a rebuild from its log. `v0.1` itself is never edited.

### 14.2 Evolution rules (amended D18, amended D20)

- **Additive only.** New prost fields, oneof variants, enum values, keyspaces (appended to `TABLES` in RESERVATIONS order, placeholder rule §13), meta keys and outcome codes. Nothing is ever reused or renumbered; a removed item is `retired`. `Cmd` tags 1–7 were frozen in Phase 2; journal kinds are append-only; `EVENT_CODEC_VER` stays 1.
- **Partition state-store values are derived only while the full log is retained.** Deleting `state/pNNNN` rebuilds them from the log. **Once Phase 8 enables log trimming, snapshot rows become a source of truth: every `STATE_FORMAT`/`VALUE_FORMAT` change must then ship a row migrator (vN → vN+1)**, applied at snapshot install and at store open. Snapshots carry `state_format`, and readers accept every older snapshot format (also for Phase 12 follower install and Phase 13 N/N-1 upgrades).
- **Meta (D20, amended by Phase 14 F2 and Phase 12 F8).** Meta records and the snapshot are positional postcard, so fields are never added to an existing meta struct. A post-freeze meta change is either (a) a **registered ext tag** on an existing record, or (b) a new `MetaRecord` variant appended in RESERVATIONS §10 order **plus a `META_FORMAT` bump** with decoders for every older format. Every meta record ends with `ext: Vec<u8>` in one TLV codec (`crates/ostro-meta/src/ext.rs`): `varint(tag u32) ‖ varint(len u32) ‖ postcard(value)`, tags strictly increasing, tag 0 invalid, unknown tags preserved on read-modify-write and ignored by readers. Tags are one global space registered in RESERVATIONS §10b (15 = golden/test only; 96–102 registered for Phase 13; 144–145 for Phase 11). An ext tag is the **bump-free extension path**: no new variant, no META_FORMAT bump. Appending a `MetaCommand` variant changes no existing encoding and needs no bump either.
- **BlobRef invariant** (Phase 8 F5, Phase 12 F11). A `BlobRef` appears only in a `PayloadRef` field of a prost message or of a documented state value (postcard `SignalSlot::Pending.event`, the prost `PayloadRef` rows of `payloads`, the prost `JournalEvent` rows of `events`), **never inside opaque payload bytes**. This covers log commands too: every BlobRef in a log command sits in a `PayloadRef` field of the prost `Command` tree, so a follower can fetch the blobs before acknowledging, and blob GC and backups find every reference without decoding journal payloads.
- **Qualified names (D23).** Resources are stored as `<tenant>/<name>`; the default tenant is unqualified. `StartRun.workflow` (and queue) carry the qualified form, so dedupe, routing, cron and webhook keys are tenant-scoped with no format change. The grammar is validated at the API, never in apply. `RunStarted.workflow` stays unqualified, with the tenant in `RunStarted.tenant`.
- **Derived keyspaces (Phase 11 PF-11-2).** A keyspace whose on-disk name starts with `x_` is **not** in `TABLES` and not in the digest; it is not exported in OSNP snapshots and not visited by blob GC; it is rebuilt from digest state when its version marker is missing or stale, including after a snapshot install; older binaries ignore it. `x_run_index` is reserved (Phase 11).

### 14.3 Data directory

- **Unknown entries are ignored (Phase 14 F8).** Node open, backup/restore and blob GC ignore unknown top-level data-dir entries such as `<data>/audit/`, and never delete or rewrite them. Node open is tested (`format_corpus.rs::unknown_data_dir_entries_ignored`: Inspect open, rebuild from the log and a Leader start on a corpus copy with `<data>/audit/x` and `<data>/unknown_dir/`). Backup/restore and blob GC (Phase 8) inherit the obligation.
- **`<data>/LOCK` (plan 05-26).** The data-dir lock file, described in §13 (last bullet of the freeze policy): `ostro dev`/`ostro server` hold `flock(LOCK_EX|LOCK_NB)` on it for the process lifetime, taken before any other disk work, and a second process on the same directory fails to start. Its content is unused and **not part of the format**; a stale `LOCK` left by a killed process never blocks the next start. Offline tooling that opens `Node` or meta directly (Inspect mode, `export_run`, the format-corpus generator) does not take it and must run on a stopped directory or a copy. Older binaries ignore it (F8).

### 14.4 `node.toml`

- Readers ignore unknown keys; new keys are additive (Phase 8 F6); an existing key never changes meaning.
- **Reserved keys `node_id` and `cluster_id` (Phase 12 F6).** Absent: adopted and written durably (tmp + fsync + rename + dir fsync). Present: a mismatch with the cluster is fatal. `node_id = 0` means "no leader" and is never a node's id. Peers live in config (`[[cluster.peers]]`), never in `node.toml`.
- **First boot (Phase 13 P13-F4).** `node_id` and `cluster_id` are written at the **first boot, single-node included**, not only at the first cluster start. From Phase 13 on, `[[cluster.peers]]` is only a **bootstrap seed list**; membership lives in meta.

### 14.5 Replication and epochs (Phase 12 F1, F2, F5)

- **F1.** Replication carries envelopes **verbatim**: followers never re-encode them. The envelope epoch is the meta-assigned leader epoch. `DEFAULT_MAX_ENVELOPE_BYTES` (4 MiB, `crates/ostro-types/src/envelope.rs`) is cluster-uniform.
- **F2.** In a cluster, meta assigns the term epoch. `next_term_epoch` (`crates/ostro-partition/src/leadership.rs`) is the RF=1 bootstrap rule only, and `LeaderAnnounced.node_id` is the meta leader for that epoch.
- **F3.** Empty-log anchoring: §10, step 3.
- **F5.** The cursor row is unchanged. Cursor history holds the applied `LeaderAnnounced` epochs; the log epoch cache holds envelope epochs and is seeded from the cursor after an install or an anchoring.

### 14.6 Mixed-version and downgrade rules (Phase 13 P13-F1)

These rules govern how a binary treats data written by a newer one. The rule text was frozen in 05-20; **all six rules are implemented since Phase 13** (feature levels `LEVEL_MIN = 1`, `LEVEL = 2`; see §19 and `docs/compat.md`).

1. New commands, fields, ext tags and versioned formats are emitted only when the cluster's **finalized feature level** (meta `ClusterInfo` ext tag 98) allows them.
2. An unknown `Cmd` tag, unknown fields in a command or record, or an unknown `MetaCommand` variant → **fail-stop**, never reject-and-continue. The partition or meta replica halts with a clear "newer data, upgrade the binary" error.
3. Every versioned format is written at the **finalized level's version** until a newer level is finalized.
4. The apply semantics of existing commands never change.
5. **Ext tag values are never validated at apply time.** TLV structure (ordering, lengths) may be checked; tag contents never are, so old and new voters cannot diverge.
6. Meta and the cluster version (the downgrade fence) are checked **before `Node::open` touches partition data**.

## 15. Phase 8 additions (post-freeze, additive; plan 08-01)

Every ordinal below comes from `.planning/RESERVATIONS.md` (Phase 8 block, P8-D20). Nothing frozen in §14 changes: the additions follow §14.2.

- **`Cmd::ScheduleFire` = tag 27** (`crates/ostro-types/src/command.rs`, P8-D10). Fields: `schedule` string 1; `fire_time_ms` u64 2 (nominal UTC instant; for `Manual`, the request time); `kind` enum `ScheduleFireKind` 3 (`Fire` 0, `Manual` 1, `Advance` 2: moves the watermark without a start, used for catch-up `Skip`); `start` optional `StartRun` 4 (required for `Fire`/`Manual`, absent for `Advance`); `overlap` u32 5 (the meta `Schedule.overlap` at fire time: 0 Skip, 1 BufferOne, 2 AllowAll, 3 CancelOther, 4 TerminateOther); `request_id` bytes 6 (`Manual` only, ≤ 64 bytes); `skipped` u32 7 (stats only); `schedule_created_at_ms` u64 8 (the schedule incarnation, W4). Golden vectors `schedule_fire_fire`, `schedule_fire_manual` and `schedule_fire_advance` are appended to `commands.txt` after the 21 frozen vectors, which are unchanged. Apply (`crates/ostro-partition/src/apply/schedule.rs`, 08-07) is exactly-once per `(schedule, fire_time_ms)` through the `schedule_state` watermark, independent of dedupe-row retention; overlap is evaluated in apply, catch-up by the cron driver (08-14).
- **Cron idempotency keys.** A fire's run uses `__cron/<schedule>/<fire_time_ms>`, a trigger-now's `__cron/<schedule>/manual/<lowercase hex request_id>` (the `__cron/` namespace is reserved, D23). The owner partition is `routing::schedule_owner(name, P) = xxh3_64("__cron/" ‖ name) % P`.
- **Keyspace 16 `schedule_state`** (appended to `TABLES`; an existing store gains it empty at open, and an empty keyspace adds only `name + row_count 0` to the digest). Two key shapes:
  - `0x01 ‖ name` (raw UTF-8, rest of key) → `ScheduleStateRow { schedule_created_at_ms: u64, watermark_ms: u64, last_run: Option<RunId> (18 bytes), buffered: Option<BufferedFire { fire_time_ms: u64, start: Vec<u8> /* prost StartRun */ }>, fired: u64, skipped: u64, duplicates: u64 }`;
  - `0x02 ‖ run` (18 bytes) → `ScheduleRunRow { schedule: String, schedule_created_at_ms: u64 }`, the reverse index read when the run ends.

  Both values are `VALUE_FORMAT ‖ postcard` (format byte 1, unchanged; positional, fields only appended).
- **`InvalidCode` 22 `BadScheduleFire`, 23 `ScheduleOwnerMismatch`.** `InvalidCode` is declaration-indexed, so indices 13–21 are held by `#[doc(hidden)]`, never-emitted placeholders `ReservedP5_13`, `ReservedP5_14`, `ReservedP6_15` … `ReservedP7_21` (RESERVATIONS §8 placeholder rule); the owning phase renames its placeholder in place. All of them fold into the hash as outcome code 26 (`Rejected(Invalid(_))`), like every `InvalidCode`.
- **`node.toml` key `incarnation`** (additive, §14.4: readers ignore unknown keys). Written by 08-08/08-13 for bottomless prefix fencing (P8-D4) and replaced by 08-18 on restore, together with `restored_from`.
- **`snapshots/pNNNN/`** holds the local OSNP v1 snapshots `<applied 20d>.osnap` (plus a `.blobrefs` cache), and **`state/.install-pNNNN`** is the staging directory of a snapshot install, renamed atomically over `state/pNNNN` after the digest check (P8-D1). The snapshot format itself is specified in `docs/snapshot-format.md` (created by 08-02). Both directories follow §14.3: other tooling ignores them.

### 15.1 Final Phase 8 on-disk rules (plan 08-22, checked against the code)

- **Local snapshots** (`crates/ostro-partition/src/leader/snapshot.rs`, 08-09). File name `<applied_offset, 20 decimal digits>.osnap` in `snapshots/pNNNN/`. A snapshot is written as `.tmp-<offset>` (sha256 computed while writing), fsynced, its `.blobrefs` sidecar written (tmp + fsync + rename; payload = the OSNP `BlobRefs` frame: `u32 LE n`, then `n` sorted 32-byte digests), renamed to the final name, and the directory fsynced. Only then is the new local trim floor published. Snapshots beyond `[snapshots] keep` (default 2) are deleted oldest first. `.tmp-*` leftovers are deleted by the startup scan in `Node::open` (never while services run). Fail points: `snapshot::before_rename`, `snapshot::before_dir_fsync`.
- **Trim rule (SC4).** Retention (`leader/retention.rs`) trims whole segments below `floor = min(ckpt, local, upload?, retention?)`: `local` = the `applied_offset` of the **oldest retained** local snapshot (0 while none exists, so nothing is trimmed); `upload` = `None` with backup off, else initialised to `Some(log_start)` by `Node::open` and raised by the uploader only after a durable manifest (B4). Hence **`log_start ≤ applied_offset(oldest retained snapshot)`** at all times: every retained snapshot plus the log rebuilds the state. Fail point `trim::after_delete`. The retention pass runs every 60 s (fixed leader interval).
- **Install and "snapshot required"** (`startup.rs`, `node.rs`). A partition whose state is missing, unreadable or below `log_start` installs the newest usable local snapshot `S` with `log_start ≤ S ≤ LEO` (falling back to older ones on any verification failure), then replays `[S, HW)`. With none usable over a trimmed log it fails with `snapshot required: run 'ostro restore' (applied A below log start L, no usable local snapshot)`. There is no automatic fetch from object storage. The install swap: build and verify `state/.install-pNNNN`, rename `pNNNN` → `.old-pNNNN`, rename staging → `pNNNN`, fsync `state/`, remove `.old-pNNNN`; `recover_install` resolves every crash point (stale staging removed, `.old` without `pNNNN` renamed back, `.old` next to `pNNNN` removed). Fail points `snapshot::install_after_write`, `install_before_swap`, `install_after_old_rename`, `install_before_old_remove`.
- **Blob GC** (`crates/ostro-blob`, `services/blob_gc.rs`, 08-19). `put` of an existing blob touches its mtime (filesystem clock, `clock-exempt`) under the shared GC lock; a sweep (exclusive lock, batches of 1,024 unlinks, fail point `blobgc::before_unlink`) deletes blobs that are unmarked and older than `gc_start − [blob_gc] grace` (`gc_start` from the injected clock). Mark = every partition's `blob_refs_snapshot` ∪ meta `Module` shas ∪ the `.blobrefs` of every retained local snapshot. Module blobs are never deleted (Phase 13 owns module GC).
- **`backup/gc-ledger`** (`crates/ostro-backup/src/remote_gc.rs`): append-only fsynced lines `"<sha hex> <deleted_at_ms>\n"` of locally deleted blobs whose remote copies await the delayed deletion window (age of the oldest retained manifest). Written only with bottomless on; a torn last line is dropped on open.
- **`node.toml` `restored_from`** (08-18): written by `ostro restore` together with a new `incarnation`; it records the source without credentials. Additive (§14.4).
- **Remote objects.** The bottomless layout (`LOCK`, `manifest/`, `meta/`, `blobs/`, `p/<pppp>/{snap,seg}/`) and the OSEG v1 chunk and Manifest v1 formats are specified in `docs/bottomless.md`; OSNP v1 in `docs/snapshot-format.md`. Neither is part of the data directory.

## 16. Phase 6 additions (additive; plan 06-03)

Every ordinal below comes from `.planning/RESERVATIONS.md` (Phase 6 block). Nothing frozen in §14 changes: the additions follow §14.2. `Cmd::CompleteEffect` (13), `Intent::RegisterEffect`/`CancelEffect` (9/10) and `InvalidCode::BadEffectCompletion` (15) were added to the command set by 06-02 (goldens `complete_effect_inline`, `complete_effect_blob`, `events_appended_effects`); 06-03 implements their apply (`crates/ostro-partition/src/apply/effects.rs`).

- **Keyspace 12 `effects` rows** (the keyspace existed since Phase 2 with no rows; RESERVATIONS §6 #12; no new keyspace). Key `effect_key(run, op) = run (18 B) ‖ op u64 BE` (`store/keys.rs`, `effect_run_prefix(run)` = the 18 run bytes). Value `EffectRow { kind: u8 /* EffectKind: Http 0, Sql 1, Object 2 */, scheduled_at_ms: u64 }` as `VALUE_FORMAT ‖ postcard` (format byte 1, unchanged). First written in Phase 6. Derived state (amended D18): every row follows from the log, and the value carries no payload (no `BlobRef`, `store/blobrefs.rs`). A pre-Phase-6 store has no rows, so its digest is unchanged.
- **`RegisterEffect{op, kind, scheduled_at_ms}`** (inside `EventsAppended`): `kind > 2` or an apply-reserved op → `Invalid(BadIntent)` before any write. An op `>= next_seq` (after the batch's events) is dropped deterministically, like `RegisterWait`. Otherwise the row is blind-inserted; a repeat carries the same facts, so the row stays byte-identical. **`CancelEffect{op}`**: blind delete, a missing row is fine.
- **`CompleteEffect{run_id, op, outcome, completed_at_ms}`** (P-10, B2): attempt-independent, first completion wins (the row is the token, like a `waits` row for `CompleteToken`). Validation, in order: run id (`BadRunId` / `WrongPartition`); missing `outcome` → `Invalid(BadEffectCompletion)`; inline outcome over 256 KiB → `PayloadTooLarge`; unknown run → `RunNotFound`; terminal run → `RunTerminal`; `op >= next_seq` → `Invalid(BadEffectCompletion)`. With the row present, apply stores `JournalEvent{seq: next_seq, kind: 65 (EffectCompleted), payload: outcome}` verbatim (the submitter-encoded event bytes, inline or a blob ref; never decoded or fetched), deletes the row and wakes the run (`InputAppended` if Running; ready row + `EnqueueReady` at `created_at_ms` if Scheduled) → `Accepted`. Without the row (already completed, or cancelled) → `Duplicate` (outcome code 1), nothing written.
- **Terminal cleanup (P-8).** `terminal::on_terminal` deletes every `effects` row of the run when the run is **not** `resumable`. Resumable terminals (Terminated, TimedOut, executor-level failures with `CompleteRun.resumable`) keep their rows, so a run resumed in place by `RetryRun` still journals the completion of an effect that was in flight when it stopped. Retention expiry (`EXPIRE_OP`, §13) deletes the rows with the run.
- **Leader-start wake (P-9).** When a `LeaderAnnounced` is accepted (the epoch gate in `apply/mod.rs::dispatch`, before its early return), apply scans the whole `effects` keyspace in key order and gives every distinct run whose status is `Scheduled` (a suspended run; there is no `Suspended` status) and that is not paused a ready row, exactly as `EnqueueReady` does (ready at `created_at_ms`, `Action::EnqueueReady`). Ready, Running, paused and terminal runs are untouched. A rejected (stale) `LeaderAnnounced` does nothing. With no rows it reads the empty scan and writes nothing, so every pre-Phase-6 log (format corpus v0.1, rebuild cases) applies to the same digest.

### Read budget exceptions (Phase 6)

Asserted with the `ApplyBatch` read counter in `crates/ostro-partition/tests/apply_effects.rs`:

| command / path | reads |
|---|---|
| `RegisterEffect` / `CancelEffect` intent | 0 beyond `EventsAppended`'s run-record read (blind write / delete) |
| `CompleteEffect` | run record + effects point read = **2** (1 when rejected before the row read) |
| non-resumable terminal transition | **+ 1** effects prefix scan by run |
| accepted `LeaderAnnounced` | **1** full effects scan **+ 1** run-record read per distinct run with rows |

## 17. Phase 7 additions (additive; plan 07-03)

Every ordinal below comes from the Phase 7 block of `.planning/RESERVATIONS.md` (D-W18). Nothing frozen in §14 changes: the additions follow §14.2. Golden vectors `pause_run_operator`, `pause_run_auto` and `resume_run` are appended to `crates/ostro-types/tests/golden/commands.txt`; every older line is byte-identical and old bytes decode unchanged. Apply routes both commands to `apply/pause.rs`, which rejects them with `Invalid(NotImplemented)` (outcome code 26) until 07-04 implements them.

**`Cmd::PauseRun` = tag 20** (`crates/ostro-types/src/command.rs`). Routed by run id (`PauseRun::run_id`), like the Phase 5 run commands.

| Field | Number | Type | Meaning |
|---|---|---|---|
| `run_id` | 1 | bytes | target run (18 bytes) |
| `reason` | 2 | u32 | `0` operator (`PAUSE_REASON_OPERATOR`), `1` retries exhausted (`PAUSE_REASON_RETRIES_EXHAUSTED`); same numbering as `PauseMark.reason` |
| `detail` | 3 | string | human-readable detail, at most 4 KiB (`MAX_PAUSE_DETAIL_LEN` = 4096) |
| `token` | 4 | `AttemptTokenMsg` (optional) | present = executor auto-pause, fenced like `EventsAppended` (only the current attempt may issue it); absent = operator pause |
| `requested_at_ms` | 5 | u64 | request time set by the submitter; becomes `PauseMark.at_ms` |

**`Cmd::ResumeRun` = tag 21.**

| Field | Number | Type | Meaning |
|---|---|---|---|
| `run_id` | 1 | bytes | target run (18 bytes) |
| `requested_at_ms` | 2 | u64 | request time set by the submitter (the resumed run's ready time) |

Cmd tags 22–26 stay spare (reserved P7).

**`RejectReason::RunPaused` = declaration index 15, outcome code 31** (`apply/outcome.rs`). `RejectReason` is declaration-indexed (code = 16 + index), so indices 12–14, which Phase 6 never used, are held by the canonical `#[doc(hidden)]` placeholders `ReservedP6_12`, `ReservedP6_13`, `ReservedP6_14` (codes 28–30). Apply never emits them, so they never reach the state hash; a phase claiming one renames it in place. Indices 16–17 stay spare (reserved P7).

**No new keyspace, meta variant or journal kind.** The pre-freeze findings F1/F2 landed in Phase 5, so Phase 7 needs none: keyspaces 17/18 and meta variants 19/20 stay unused, `META_FORMAT` and `EVENT_CODEC_VER` are unchanged. Phase 7 also uses these pre-freeze fields:
- **F7 `ReadyValue.first`** (carried as `first_activation` on `query::ReadyEntryRow` and `Action::EnqueueReady`): `true` when no attempt was ever granted (`RunRecord.current_attempt.is_none()` at enqueue). It feeds the first-activation throttle.
- **F8 `ApiToken.expires_at_ms: Option<u64>`** (meta): the expiry of worker join tokens (`wkr_`).

**In-memory actions** (not an on-disk format, not a RESERVATIONS space): `Action::RunPaused { run }` and `Action::RunResumed { run }`, emitted by 07-04 after commit and consumed by the dispatcher in 07-05. Until then its wildcard arm ignores them.

**`RunRecord.pause` semantics** (F1 `Option<PauseMark { reason u8, at_ms, detail }>`, stored since Phase 5; enforced by 07-04):
- `PauseRun` on a Scheduled, Ready or Running run sets `pause = Some(PauseMark { reason, at_ms: requested_at_ms, detail })`, removes any ready row and emits `Action::RunPaused`. A Running run stays Running in the store. It costs exactly 1 point read (the run record). On a run that is already paused it is `Accepted` with no change; on a terminal run it is `RunTerminal`; on an unknown run, `RunNotFound`. A token that is not the current attempt is rejected `Fenced`/`StaleEpoch`, exactly as for `EventsAppended`.
- While a run is paused, `enqueue_ready` (every caller) writes no ready row and pushes no `EnqueueReady`: the run is left `Ready` with `rec.ready = None`. `EventsAppended` and `AttemptGranted` are rejected `RunPaused` atomically (nothing in the batch applies). Inputs (timers, signals, tokens, effect completions, cancel requests) are still journaled but do not wake the run. `TerminateRun` and the run-timeout timer still terminate it.
- Every terminal transition clears `pause` (Phase 5's `terminal::on_terminal` hook), so no terminal run is paused, and no run is both paused and has a ready row.
- `ResumeRun` on a paused run clears the mark. A Ready or Running run is re-enqueued through `enqueue_ready` at `requested_at_ms` (ready row + `EnqueueReady`, `first_activation` from F7); a Scheduled run is only cleared. It emits `Action::RunResumed`. On a run that is not paused it is `Accepted` with no change; on a terminal run, `RunTerminal`.

**Implemented by 07-04** (`apply/pause.rs`; the 07-03 `Invalid(NotImplemented)` stubs are gone):
- **Reject order.** `EventsAppended`: decode/token/run checks, `RunNotFound`, `RunTerminal`, `StaleEpoch`, `Fenced`, then `RunPaused`, then `SeqMismatch` and the intent checks. A stale zombie therefore still sees `Fenced`, and a paused run never reaches `SeqMismatch`. `AttemptGranted`: `RunTerminal`, `StaleEpoch`, `Fenced` (token not newer), then `RunPaused`. `PauseRun`: run id, detail length (`Invalid(FieldTooLong)`) and reason code (`Invalid(Decode)` for anything but 0/1) before the read; then `RunNotFound`, `RunTerminal`, auto-pause `StaleEpoch`/`Fenced`, idempotence.
- **Read budget.** `PauseRun` 1, `ResumeRun` 1 (the run record; the resume's `enqueue_ready` is blind). Asserted in `crates/ostro-partition/tests/pause.rs`.
- **Inputs while paused.** The first input that wakes a `Scheduled` paused run moves it to `Ready` without a row (the gate in `enqueue_ready`); later inputs see `Ready` and do nothing more. A paused run that is still `Running` (its activation not yet revoked) gets the usual in-memory `Action::InputAppended`; the dispatcher ignores it once it has revoked the run (07-05). The P-9 leader-start effect wake already skips paused runs.
- **TimedOut while paused (D1/D2).** The run timeout keeps ticking while paused: the `RUN_TIMEOUT_OP` timer hard-stops the run `TimedOut` (`resumable = true`), and the terminal hook clears `pause`. `RetryRun` (D1, resume in place, no new run timeout) then goes through `enqueue_ready` without the gate: ready row + `EnqueueReady`, and the run can be granted again. To keep a run paused across a retry, pause it again after the `RetryRun`.
- **Server.** `PartitionSink` maps `Rejected(RunPaused)` to `AppendError::Fenced`: the dispatcher has revoked the paused run, so the activation stops.

## 18. Phase 9 additions (additive; plan 09-05)

Every ordinal below comes from `.planning/RESERVATIONS.md` (Phase 9 fallback block). Nothing frozen in §14 changes; the v0.1 format corpus still opens and rebuilds unchanged.

- **`Derived` intent `ChildJournal` = tag 19** (`crates/ostro-types/src/command.rs`): `{child_run bytes 1}`. Opt-in child-completion journaling. It is valid only **directly after** the `SendMessage(StartChild)` of the same child in the same `EventsAppended` (anything else is `Rejected(Invalid(BadIntent))`); it is also accepted among the intents of a continue-as-new batch. Apply rewrites the parent's `children` row of that child from the empty value to `[1]` (`CHILD_ROW_JOURNAL`); `rec.children` was already counted by the StartChild.
- **Inbound `ChildCompleted` (§14.6 rule 4).** For a parent whose `children` row of `child_run` holds `[1]`, apply journals the completion at `next_seq` as kind 67 and wakes the parent like any input (Running: `InputAppended`, Scheduled: Ready): the outbox `event` `PayloadRef` (the child's `CompleteRun.parent_event`, submitter-encoded, D21) is stored **verbatim**; without `event`, apply encodes a no-user-data `ChildCompleted{op, Err(failure)}` itself (Terminated and Completed/Failed: non-retryable `child_terminated`; TimedOut: `Timeout`; Cancelled: `Cancelled`) with ts = the parent's `created_at_ms` (the message carries no time). A missing or empty row keeps the v0.1 semantics (legacy `InboxMsg::ChildCompleted` row), so every command logged before Phase 9 applies exactly as before. The row is removed and `rec.children` decremented in both cases; a terminal or unknown parent is still a per-message drop. Read budget: one more point read per inbound `ChildCompleted` (the `children` row).
- Server producers (09-05): `crates/ostro-server/src/children.rs` (deterministic `child_run_id`, D-P9-7) and the sink emit the marker after every attached StartChild and set `CompleteRun.parent_event` on a child's guest terminal.

### 18.1 Phase 9 children apply rules (09-08; no new ordinal, recorded by 09-20)

- **Row guard.** An inbound `ChildCompleted` is matched against the parent's `children` rows: the exact `(parent, child_run)` row first; only on a miss and only when `rec.children > 0`, one prefix scan finds the row whose child uuid equals `ids::child_run_uuid(parent, op)` (the last run of a continue-as-new chain reports under its own run id). The matched row's value decides: `[1]` journals kind 67 (above), an empty value takes the legacy inbox path. The matched row is removed and `rec.children` decremented, so a re-completion is never journaled twice. **No matching row: the v0.1 semantics stay** (legacy `InboxMsg::ChildCompleted` row and wake, §14.6 rule 4), except that no row is removed and no counter decremented (no counter drift). A chain (uuid) match also emits `CancelRun{original child, reason "chain_done"}`.
- **Detached starts write no row.** `apply/run.rs` and the continue-as-new batch (`apply/lifecycle.rs`) insert a `children` row only for a `StartChild` with a `ParentRef`; a `ChildJournal` after a detached start is `Rejected(Invalid(BadIntent))`.
- **Parent close.** At every terminal transition with `rec.children > 0`, `terminal::close_children` sends a graceful `CancelRun` to every row's child, deletes the rows and sets `rec.children = 0` (reason `parent_closed`; hard stops keep their own reason and their 05-09 outbox order).
- **Duplicate StartChild (resume after close, D-P9-24).** A `StartChild` for an existing run whose `RunRecord.parent == (parent, op)`: terminal child → outbox `ChildCompleted{status, result: None, event: None}`; ContinuedAsNew with `continued_to` → the StartChild is forwarded to `continued_to`; live child, other parent, or detached → dropped. One extra point read.
- **Deferred-expiry mark (W3, D-P9-27).** EXPIRE of a ContinuedAsNew run that has a parent and a `continued_to` deletes only its events and payloads, resets `next_seq`/`status_seq`, keeps the `RunRecord` and re-arms EXPIRE at `completed_at_ms + 2 × retention_ms`. The mark is `status == ContinuedAsNew && parent.is_some() && next_seq == 0` (`retention::is_deferred`). A `chain_done` reaching a kept record forwards it and deletes the record (cancelling the timer); otherwise it clears `continued_to`. At the bound the record is deleted fully. No field was added; the mark is derived from existing fields. The v0.1 format corpus rebuilds unchanged (`format_corpus`), and the live state of a two-partition children workload equals its rebuild from the log (`children_chain`).

## Phase 11 additions (post-freeze, additive)

Every ordinal below comes from the Phase 11 block of `.planning/RESERVATIONS.md` (commit de4e303); decision ids refer to `.planning/phases/11-run-operations-embedded-ui/11-DECISIONS.md`. Nothing frozen in §14 changes: the additions follow §14.2 and §14.6 rule 4 (the apply semantics of existing commands are unchanged). Phase 11 adds no `RejectReason`, `Derived` intent, journal kind, `TerminalStatus`, `TABLES` keyspace, outbox kind, meta variant or `MetaCommand`. The command-set additions (plan 11-02) default-omit in proto3, so every older command re-encodes byte-identically; golden vectors `dlq_update_*`, `start_run_rerun` and `events_appended_attempt` are appended to `crates/ostro-types/tests/golden/commands.txt`. The v0.1 format corpus opens and rebuilds unchanged.

### Cmd 45 `DlqUpdate` and the `dlq` row (plans 11-02, 11-08; OPS-04, D-P11-11)

**`Cmd::DlqUpdate` = tag 45** (`crates/ostro-types/src/command.rs`; tags 46–49 spare). Routed by run id (`DlqUpdate::run_id`).

| Field | Number | Type | Meaning |
|---|---|---|---|
| `run_id` | 1 | bytes | target run (18 bytes) |
| `op` | oneof 2/3/4 | | the mark (below) |
| `episode_completed_at_ms` | 5 | u64 | the failure episode the mark refers to: the run's `RunRecord.completed_at_ms` of that failure (W5) |

| `op` variant | Tag | Fields |
|---|---|---|
| `HookStarted` | 2 | `hook_run_id` bytes 1 (18 bytes): the on-failure hook run |
| `Dismissed` | 3 | `reason` string 1 (≤ 1024 bytes, `DLQ_DISMISS_REASON_MAX`), `at_ms` u64 2 |
| `Requeued` | 4 | `mode` u32 1 (`0` resume, `1` restart, `2` rerun: `dlq_update::REQUEUE_RESUME/RESTART/RERUN`), `new_run_id` bytes 2 (18 bytes for restart/rerun, empty for resume), `at_ms` u64 3 |

**`InvalidCode` 30 `BadDlqUpdate`** (`apply/outcome.rs`; 29 is `BadRerunPrefix`, below). `InvalidCode` is declaration-indexed, so 24–28 are held by the canonical `#[doc(hidden)]` never-emitted placeholders `ReservedP8_24`, `ReservedP9_25` … `ReservedP9_28`. Every `InvalidCode` folds into the rolling hash as outcome code 26.

**`dlq` row** (keyspace 11, existing since Phase 2; its first rows are written in Phase 11). Key `run_key(run)` = the 18 run-id bytes. Value `VALUE_FORMAT ‖ postcard(DlqRow)` (format byte 1, unchanged; `crates/ostro-partition/src/store/values.rs`). Positional field order, which is the format (never reordered; a future field needs a row migrator, D18):

1. `hook_run: Option<[u8; 18]>`
2. `dismissed_at_ms: Option<u64>`
3. `dismiss_reason: Option<String>`
4. `requeued_to: Option<[u8; 18]>` (restart / rerun target; `None` for a resume)
5. `requeued_at_ms: Option<u64>`
6. `requeue_mode: Option<u8>` (0 resume, 1 restart, 2 rerun)
7. `episode_completed_at_ms: u64`

The row carries no `PayloadRef` (no blob references; `store/blobrefs.rs`). **Apply writes `dlq` rows only for Cmd 45; a log without Cmd 45 produces no `dlq` rows**, so every older log applies to the same digest. Retention expiry (`EXPIRE_OP`, §13) removes the row with the run (the existing `dlq` prefix delete). The dead-letter queue itself is a view: failed / timed-out runs minus the runs whose row, for the run's current episode, carries a mark.

**Apply** (`crates/ostro-partition/src/apply/dlq.rs`, 11-08). Validation first, then at most 2 reads (the run record, then the `dlq` row); no payload is decoded, no blob fetched, the `runs` row is never written; no actions; times come from the command.

1. Run id not 18 bytes → `Invalid(BadDlqUpdate)` (not `BadRunId`); run on another partition → `WrongPartition`.
2. Missing `op`, dismiss reason > 1024 bytes, hook run id not 18 bytes, `mode > 2`, restart/rerun `new_run_id` not 18 bytes, or a resume with a non-empty `new_run_id` → `Invalid(BadDlqUpdate)`.
3. Unknown run → `RunNotFound`.
4. `HookStarted`, `Dismissed` and `Requeued{restart|rerun}` require status `Failed` or `TimedOut`, else `Invalid(BadDlqUpdate)`. `Requeued{resume}` is accepted for any status (the `RetryRun` resume already moved the run out of `Failed`).
5. On a `Failed`/`TimedOut` run (every op, resume included), `episode_completed_at_ms` must equal the run's `completed_at_ms` (0 when unset), else `Invalid(BadDlqUpdate)`: a stale mark of an earlier failure is refused.
6. **Failure episode (W5).** If the stored row names a different episode than the command, the row is replaced by a fresh `DlqRow { episode_completed_at_ms: <command's episode>, .. }` before merging, so marks of an earlier failure never hide a later one.
7. **Merge.** `HookStarted` sets `hook_run` once: the same hook run again is a no-op, a different one → `Invalid(BadDlqUpdate)`. `Dismissed` sets `dismissed_at_ms` and `dismiss_reason`. `Requeued` sets `requeued_to`, `requeued_at_ms` and `requeue_mode`. Re-applying an identical update → `Accepted` with no write; any accepted change is one blind insert of the whole row.

Read budget: 0 when rejected in steps 1–2, 1 when rejected in steps 3–5, otherwise 2. Pinned by `crates/ostro-partition/tests/apply_dlq.rs` and covered by `tests/rebuild_digest.rs` (`DlqUpdate` generator, `fixed_scenario_dlq`).

### StartRun 40 `rerun_prefix` / 41 `rerun_from_seq` (plans 11-02, 11-07; OPS-02, D-P11-3/6)

`StartRun.rerun_prefix` = field 40 (repeated `JournalEvent`), `StartRun.rerun_from_seq` = field 41 (u64); fields 42–43 spare. The rerun path of `start_run` (`crates/ostro-partition/src/apply/rerun.rs`) applies **only when field 41 is non-zero**. A StartRun without fields 40/41 (every command written before Phase 11) applies exactly as before. `rerun_from_seq` is the cut, the first seq the rerun re-executes; a cut of 1 with an empty field 40 is valid (only the verbatim `RunStarted`). The checks run in `start_run`'s validation block, before the point read and before any write.

Requirements, every violation → `Invalid(BadRerunPrefix)` (`InvalidCode` 29):
- field 40 is empty when field 41 is 0;
- `run_started` (field 23) is present and its event is kind `RunStarted` at seq 0;
- the prefix seqs are contiguous from 1 and `rerun_from_seq == len(rerun_prefix) + 1`;
- every prefix kind is in 16–127, excluding the terminal kinds 26 (`RunCompleted`), 27 (`RunFailed`), 28 (`RunContinuedAsNew`) and 71 (`RunTerminated`);
- every prefix event has a payload, and an inline one is ≤ the inline limit (`INLINE_PAYLOAD_LIMIT`, 256 KiB);
- the run carries exactly one tag `__ostro/rerun_from=<n>`, with `<n>` the canonical decimal of field 41.

Effects: the events `0..cut` (seq 0 from `run_started`, then the prefix) are journaled verbatim, `next_seq = cut`, and `status_seq` is the seq of the last `StatusPublished` event in the prefix (if any). No payload is decoded (D21); blob references in the prefix are `PayloadRef` fields of the prost command, as §14.2 requires.

### `JournalEvent.4 attempt` (plan 11-02)

`JournalEvent` field 4 `attempt` (fixed64) = `epoch << 32 | n` of the attempt that produced the event. It is set by the submitter (the executor) and stored verbatim inside the prost `JournalEvent` row of `events`; apply never stamps it. It is 0 for apply-originated inputs (timers, signals, tokens, effect and child completions, `RunTerminated`) and for every event written before Phase 11. Proto3 omits it at 0, so older events re-encode byte-identically.

### `x_run_index` (plan 11-05; OPS-05, D-P11-9)

A derived keyspace under the frozen rule of §14.2 ("Derived keyspaces", PF-11-2), defined in `crates/ostro-partition/src/store/derived.rs`: the fjall keyspace `x_run_index` in the partition's database. It is **not** a `Table` and **never in `TABLES`, the digest, OSNP snapshots or blob GC**; older binaries ignore it. Its content is a pure function of the `runs` keyspace, so it can always be rebuilt from digest state. Apply writes it through `ApplyBatch::insert_derived/remove_derived`, in the same fjall batch as the state change (atomic with the applied offset).

Keys end with the ordering suffix `sfx = !created_at_ms (u64 BE, bit-inverted) ‖ !run_uuid (the 16 uuid bytes of the run id, bit-inverted)`, so a forward scan is newest first even for non-time-ordered ids (child run ids are uuid v8, D-P9-7); the uuid breaks ties. **All values are empty** (no user payloads).

| Key | Kind | Rows |
|---|---|---|
| `0x01 ‖ tag ‖ 0x00 ‖ sfx` | Tag | one per run tag (an empty tag is indexed under `0x01 0x00`) |
| `0x02 ‖ workflow ‖ 0x00 ‖ sfx` | Workflow | one per run |
| `0x03 ‖ status u8 ‖ sfx` | Status | terminal statuses only: Completed 3, Failed 4, Cancelled 5, Terminated 6, TimedOut 7, ContinuedAsNew 8 |
| `0x04 ‖ tenant ‖ 0x00 ‖ sfx` | Tenant | one per run with a tenant |
| `0x05 ‖ sfx` | All | one per run |
| `0x00 'v'` | marker | `index_version u32 BE (= 1, RUN_INDEX_VERSION) ‖ built_at_applied_offset u64 BE` |

A tag, workflow or tenant containing `0x00` gets no row of that kind. **Marker:** every apply commit while the index is ready rewrites it with the batch's applied offset (one insert per chunk), so `built_at_applied_offset == applied_offset` holds after any crash. **Hooks** (write-only; apply read counts are unchanged): the shared start path `start_run` (API `StartRun`, inbound `StartChild`, `CronStart`, `ScheduleFire`, the continue-as-new successor, restart-mode `RetryRun`) writes the All, Tag, Workflow and Tenant rows; `terminal::on_terminal` writes the Status row; a `RetryRun` resume deletes it; retention removes the run's rows at the actual delete point (`delete_run`, after Phase 9's deferred-EXPIRE branch). Ready/Running churn writes nothing.

**Rebuild:** at open, a missing marker, another version, or `built_at_applied_offset != applied_offset` marks the index stale. On a **Leader start only** (never in `StartMode::Inspect`) a background thread removes the marker, clears the keyspace, walks `runs` in batches of 1,000 under the apply lock (lock order: apply lock, then commit gate), and finally writes the marker at the current applied offset; until then queries fall back to the budgeted scan. A fresh store writes the marker at creation (an empty index is complete). A snapshot install never carries the keyspace (OSNP holds only `TABLES`) and writes no marker, so an installed store opens stale and is rebuilt.

### System tags (constants: plan 11-02)

`__ostro/rerun_from=<seq>` (`RERUN_FROM_TAG`, the rerun lineage, decimal seq) and `__ostro/on_failure_of=<run>` (`ON_FAILURE_OF_TAG`, an on-failure hook run) are stored in `RunRecord.tags` like any other tag (no format change; tag limits of §12 apply). The prefix `__ostro/` (`SYSTEM_TAG_PREFIX`) is reserved: the API refuses user-supplied tags that start with it.

## 19. Phase 13 additions (post-freeze, behavioural notes; no byte format changed)

Every ordinal comes from the Phase 13 block of `.planning/RESERVATIONS.md` (D-P13-22). No existing encoding changed: the v0.1 format corpus rebuilds unchanged and every `commands.txt` golden re-encodes byte-identically (the fail-stop detector relies on it). The additions follow §14.2 and §14.6.

### 19.1 P13-F1 rules as implemented (§14.6)

Levels: `LEVEL_MIN = 1` (the frozen v0.1 formats plus Phase 12's MetaCommands 4–6 and ext tag 80) and `LEVEL = 2` (everything below except the machinery tags). Rule 1: every emitter of a level-2 novelty calls `compat::require(feature, level_max)` against the finalized level (ext 98). Rule 2: a partition command whose prost re-encoding is shorter than its bytes, or a non-empty command without a known oneof tag, makes the partition halt with `StoreError::NewerFormat` before the state batch commits (`applied` does not advance); `MetaSm::apply` returns `FatalApply` for an undecodable non-empty entry and the vendored driver halts before `applied` advances; genuinely empty partition bytes stay `Rejected(Invalid(Empty))`. Rule 3: nothing is written above the finalized level. Rule 4: no existing command's apply changed. Rule 5: ext tag contents are never validated at apply time (`AlterReplicas` writes ext 100 from the command verbatim). Rule 6: the boot fence runs after meta catch-up and before `Node::open`; a running fence fail-stops a node that later sees a finalized level above its `level_max`.

### 19.2 `Cmd 43 RepinRun` (13-17, CLUS-07, D-P13-21)

`RepinRun{run_id, from_sha, to_sha, expected_next_seq, requested_at_ms}` (tag 43; 44 spare) moves a non-terminal run to another module. Gated at level 2 (`Feature::RepinRun`). Apply check order: validation (run id, both shas 32 bytes) → `RunNotFound` → `RunTerminal` → `RunRecord.module_sha ≠ from_sha` → **RejectReason 23 `ModuleMismatch`** (outcome code 39) → `next_seq ≠ expected_next_seq` → existing `SeqMismatch{expected: next_seq}` → a leased run (status `Running`, the running row exists) → **RejectReason 24 `RunBusy`** (outcome code 40) → accept: `RunRecord.module_sha = to_sha`, nothing else changes. The journal is untouched, so a repinned run replays its own history on the new module (the replay check passed before submission). Indices 16–22 below `ModuleMismatch` are held by the `#[doc(hidden)]` placeholders `ReservedP7_16`, `ReservedP7_17`, `ReservedP8_18`, `ReservedP8_19`, `ReservedP9_20..22` (never emitted). P13-F3: `run-info.module-hash` reports the current `module_sha`, so it may change after a repin (logs and metrics only).

### 19.3 MetaCommand 8 `AlterReplicas` (13-10, D-P13-12)

`AlterReplicas{partition, epoch, expected_isr_version, replicas, isr, min_insync, reassignment: Option<Reassignment>}` at postcard index 8; index 7 is the `#[doc(hidden)] ReservedP12_7` placeholder (apply → `Invalid("reserved")`), 9–11 spare. Apply requires `epoch == current` (and ≠ 0), `isr_version == expected_isr_version`, `leader ∈ replicas` unless offline, `isr ⊆ replicas`, `leader ∈ isr` unless 0, `1 ≤ min_insync ≤ |isr|`; it then sets replicas / ISR / min_insync, bumps `isr_version` (the frozen field name also covers replica-set changes) and writes or clears ext 100 from the command. Gated at level 2.

### 19.4 Meta ext tags 96–102

TLV values (postcard, §14.2 meta rule); unknown tags preserved and ignored. Tags 96–99 are the gate's own machinery and are written at every level (`docs/compat.md` §1); 100–102 are gated at level 2; 103–111 reserved P13, unused.

| tag | record | value struct |
|---|---|---|
| 96 `NodeVersion` | `Member` | `{ostro_version: String, level_min: u32, level_max: u32, wasmtime: String, event_codec_ver: u32, wit_lines: Vec<String>, reported_at_ms: u64}` |
| 97 `MemberState` | `Member` | `{state: u8 (joining / active / draining / removed; unknown kept), changed_at_ms: u64}`; absent = active |
| 98 `ClusterVersion` | `ClusterInfo` | `{finalized: u32, hold: Option<u32>, finalized_at_ms: u64}`; absent = `LEVEL_MIN`; undecodable reads as `LEVEL_MIN` and is never overwritten by the finalizer |
| 99 `ReplicationPolicy` | `ClusterInfo` | `{rf: u8, min_insync: u8, max_voters: u8}`; absent = the Phase 12 bootstrap seeds as placed |
| 100 `Reassignment` | partition assignment | `{target: Vec<u64>, adding: Vec<u64>, removing: Vec<u64>, started_at_ms: u64}` |
| 101 `WitRequirement` | `Module` | `{line: String, min_level: u32}`; written only for non-baseline modules (absent = `{"0.1", 1}`) |
| 102 `Retired` | `Module`, `WorkflowVersion` | `{retired_at_ms: u64}` |

### 19.5 Data directory (13-09)

`<data>/cluster-seeds.toml` (`{node_id, cluster_id, seeds}`) is the durable join marker, written first by `ostro cluster join` (tmp, fsync, rename, dir fsync); with blank meta it makes the node a joiner. The joined certificate set lives under `<data>/tls/joined/{ca.pem,node.pem,node.key}` (Phase 7's `<data>/tls/node.{crt,key}` are untouched). `node.toml` is completed from the marker when missing.

