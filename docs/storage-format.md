# Ostro on-disk format (Phase 2, with Phase 4 additions in §12 and Phase 5 additions in §13)

Status: fixed at the end of Phase 2 (plan 02-16). This document describes what the code writes today. Each section names the source file that defines the format, and the golden files and tests that pin it.

**Freeze policy:** the format is **frozen at the end of Phase 5**. After that, only additive changes are allowed: new command fields, new oneof variants, new enum values, new keyspaces, new meta keys, new outcome codes appended at the end. Field numbers, tags, codes and key layouts are never reused or renumbered. Until Phase 5 ends, any incompatible change must bump the relevant version (`node.toml` `format_version`, meta `format_ver`, envelope `codec_ver`, value format byte) and update this document in the same commit.

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
├── blobs/<hh>/<sha256 hex>         # content-addressed blobs, <hh> = first two hex chars (crates/ostro-blob)
└── tmp/<sha256 hex>.<pid>.<n>      # in-flight blob writes; deleted by BlobStore::open
```

- **`node.toml`**: two `key = value` lines, `format_version = 1` and `partitions = <P>`. The file is written durably (tmp + fsync + rename + dir fsync) on first start. A different `partitions` value at open is fatal (`NodeError::PartitionCountMismatch`, STOR-09). The partition count is also recorded in every state store.
- **WAL**: one shared group-commit WAL per node (`wal/`), topic `p`, one partition directory per partition. The vendored WAL opens a **new** segment on each process's first append to a partition, rotates at 250 MB or 30 min (vendored defaults), and fsyncs a segment before rotating away from it. Empty segment files can exist and are ignored. Retention deletes only whole segments below the safe floor (§8).
- **State**: one fjall `Database` per partition. Deleting `state/pNNNN` and reopening rebuilds that partition from its log (STOR-06). The fjall-internal file layout is not part of Ostro's format. Ostro's format is the logical content (keyspaces, keys, values) in §5–§6.
- **Blobs**: payloads over 256 KiB are written as `blobs/<hh>/<sha256>` (tmp write, fsync, rename, dir fsync) **before** the command that references them is appended. A visible blob is always complete. Blob GC and object-store backends come in Phase 8.

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

`InvalidCode` details (Decode, Empty, BadRunId, MissingToken, EventsNotContiguous, MissingField, NotImplemented, BadIntent, FieldTooLong, BadSource, TruncateBeyondEnd; Phase 4: BadLength, NotRunning) are not hashed separately, because the command bytes that determine them are hashed. They are append-only.

## 9. Rolling hash and logical digest

- **Rolling hash** (`apply/mod.rs::fold_hash`), per applied command: `state_hash = blake3(prev ‖ offset u64 LE ‖ [outcome code] ‖ command bytes)`, starting from 32 zero bytes. Rejected commands are folded too, because they consume their offset.
- **Logical digest** (`store/digest.rs`): blake3 over one fjall snapshot. For each keyspace in `TABLES` order it hashes `u16 LE name_len ‖ name`, then every row in key order as `u32 LE key_len ‖ key ‖ u32 LE value_len ‖ value`, then `u64 LE row_count`. The only skipped row is meta `checkpointed_offset`. Always compare digests, never fjall file bytes: file layout depends on flush and compaction timing.

## 10. Recovery rules

Order (`Node::open`, `crates/ostro-partition/src/node.rs`):
1. `node.toml` (format and partition count).
2. Open every state store in parallel. Its `applied_offset` is the partition's durable floor. An unreadable store gets floor `u64::MAX`, so its log tail is never cut.
3. Recover every log in parallel (`crates/ostro-log/src/recovery.rs`): list segments, read and verify every record (§2–§3), classify the first failure, then make the log durable.
4. Validate: `applied > LEO` is "state ahead of log" (fatal for the partition). `applied < log_start` is "snapshot required".
5. Replay from the state's **own `applied_offset`** (not `checkpointed_offset`), up to the HW. Then the epoch bump, `LeaderAnnounced{e+1}` as the first entry of the new term, readiness barrier, leader services.

Any per-partition failure marks only that partition `Failed`.

**Torn tail vs corruption.** The first bad record is torn, and its segment is cut at the record start (`set_len` + fsync file + fsync dir) before the WAL opens, only if all of the following hold:
- it is in the **last non-empty segment**;
- it starts at or above the floor (`expected base ≥ applied_offset`);
- it is incomplete at EOF, or contains an all-zero 512-byte aligned sector, or has an unreadable header followed by zeros to EOF.

Anything else fails the partition with `LogError::Corrupt`, naming the segment and byte position. Nothing is ever skipped. Records after a cut are discarded too: they were in the same un-fsynced window and cannot have been acknowledged.

**Durability after recovery.** Only the last non-empty segment can hold bytes that were never fsynced. Earlier segments were fsynced by the WAL before it rotated away from them, or were the tail at an earlier recovery, which fsynced them before any newer segment could be created. Recovery therefore fsyncs the last non-empty segment, which makes the whole retained log durable, and HW starts at the recovered LEO. Before plan 02-16 recovery fsynced every segment. That cost about 0.55 ms per segment and made reopen grow with the restart count.

**State durability.** Apply commits use fjall's buffered journal (`write(2)`, no fsync). `checkpoint` writes `checkpointed_offset` and calls `persist(SyncAll)` every 100 ms or 256 chunks (`MachineConfig`), on `force_checkpoint`, and on graceful shutdown. After a crash, fjall may reopen ahead of the last checkpoint. That is safe because the state only ever applies WAL-durable commands (HW = fsynced LEO at RF=1).

**Trim floor.** Retention deletes whole segments below `min(durable checkpoint, snapshot_floor, retention_floor)`, and only with a `TrimPermit` minted after the partition's readiness barrier opened. `snapshot_floor` stays 0 until Phase 8, so Phase 2 never deletes log segments.

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

Plan 05-02 changed the state format before the Phase 5 freeze. The command-set and journal additions (Cmd 8..12, StartRun 19..23, Derived 7..8, statuses 3..5, outbox kind 6, journal kinds 70/71/96/97) are additive and pinned by the appended goldens of plan 05-01. The state changes below are **not** additive: this is a pre-freeze positional change with **no migration** and no `VALUE_FORMAT` bump. **Delete dev data dirs created before Phase 5**: an old `runs` row fails to decode (`StoreError::Codec`). Plan 05-20 marks this document FROZEN v0.1; until then this section may still change. Empty-log anchoring is documented by plan 05-21 in §10.

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

- **Frozen sources of truth:** the log (WAL frame, envelope, `Command` prost encoding, journal event codec, command hash), `node.toml`, the blob layout, and the meta Raft log and snapshot encoding.
- **`node.toml`**: readers ignore unknown keys, and new keys are additive (Phase 8 F6, Phase 12 `node_id`/`cluster_id`). An existing key never changes meaning.
- **Partition state-store values are derived** while the full log is retained: deleting `state/pNNNN` rebuilds them from the log. That is why pre-freeze value changes (this section, §12) need no migration.
- **After Phase 8 enables log trimming**, snapshot rows become a source of truth. From then on every `STATE_FORMAT` / `VALUE_FORMAT` change ships a **row migrator** (vN → vN+1), applied at snapshot install and at store open. Snapshots carry `state_format`, and readers accept every older snapshot format (also for Phase 12 follower install and Phase 13 N/N-1 upgrades).
- **BlobRef invariant** (Phase 8 F5): a `BlobRef` appears only in a `PayloadRef` field of a prost message (log commands included) or of a documented state value (postcard `SignalSlot::Pending.event`, the prost `PayloadRef` rows of `payloads`, the prost `JournalEvent` rows of `events`), **never inside opaque payload bytes**. Blob GC and backups can therefore find every reference without decoding journal payloads.
- Node open, backup/restore and blob GC ignore unknown data-dir entries (Phase 14 F8, e.g. `<data>/audit/`).
