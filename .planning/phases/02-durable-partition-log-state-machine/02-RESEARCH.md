# Phase 2: Durable Partition Log & State Machine - Research

**Researched:** 2026-11-18
**Domain:** Crash-safe partitioned journal (vendored Chronik WAL behind an Ostro `ReplicatedLog` at RF=1), deterministic redb state machine, cross-partition outbox/shuffler, lazy timers, fencing epochs, fault-injection testing
**Confidence:** HIGH on vendored-WAL behaviour (read from source at `08715f4`), redb 4.6 API (docs.rs) and the test tooling. MEDIUM on the torn-tail classification rule and on the post-fsync-failure behaviour, which depend on kernel and filesystem semantics.

> **Dependency on the Phase 1 S3 spike (redb vs fjall):** this document assumes **redb 4.6.0**. Every section or row that depends on that choice is tagged **[S3]**. If S3 picks fjall, only the [S3] items change. The log layer, envelope, `ReplicatedLog`, outbox/shuffler semantics, fencing and timers do not depend on the state-store choice.

<user_constraints>
## User Constraints

There is no `02-CONTEXT.md` (no `/gsd:discuss-phase` was run). The constraints below are the **project-level locked decisions** from PROJECT.md, research/SUMMARY.md, STATE.md and the Phase 1 research. The planner must honour them.

### Locked Decisions
- **Log-first storage.** The partitioned journal runs on the vendored Chronik WAL (group commit). The per-partition state machine runs on redb. Recovery is snapshot + log tail. Snapshots are Phase 8; in Phase 2 recovery is the redb checkpoint + log tail.
- **Vendor, don't depend.** Chronik code lives in the Ostro workspace as `vendor/ostro-wal` (and the raft crates), with VENDORED.md entries for every local modification. The Chronik repo is never modified.
- **One replication path (D5).** An Ostro-owned `ReplicatedLog` trait (append, HW, epoch cache, ISR) exists from day one. A single node runs it with RF=1 and ISR={self}. There is no "RF=1 special case" path.
- **WAL format.** V2 records only, with Ostro's envelope `{epoch, codec_ver, crc32c, cmds}` in `canonical_data`. Ostro verifies CRCs on **every** read. The log end (LEO) is recovered by scanning segments. io_uring is removed.
- **State.** redb **4.6** (not 5.0), with `Durability::None` plus a periodic `Durability::Immediate` checkpoint of `applied_offset` in the same transaction. **[S3]**
- **Apply ≤ HW.** Validation happens at apply time. `AppendAck` is sent only after apply. Every term begins with `LeaderAnnounced{epoch}`. `(epoch, attempt)` fencing tokens are checked at apply.
- **Cross-partition** traffic goes through an outbox + shuffler with `(src_partition, seq)` dedupe, even on one node with P=16.
- **Lazy windowed timers.** `TimerFired` is idempotent.
- **fsync/write errors are fatal for the partition.** They are never retried and never reported as success.
- **Trim/readiness.** WAL segments are deleted only below the last durable checkpoint, and never before the startup readiness barrier.
- **Fixed partition count** at creation (16 single node, 64 cluster). Run ids embed their partition.
- **Inline payload limit is 256 KiB.** Larger payloads go to the blob store.
- **Toolchain:** Rust 1.100 (pinned via `rust-toolchain.toml` from Phase 1), edition 2024 for `crates/*`, edition 2021 kept for `vendor/*`. The `metrics` facade. **No chrono** (RUSTSEC-2026-0543; use jiff if calendar types are ever needed). No bincode (archived). No C/C++ dependencies.
- **On-disk format and command envelope are fixed in this phase.** The command set and format freeze at the end of Phase 5; after that, changes are additive only.

### Claude's Discretion (researched below, with recommendations)
- The exact shape of the `ReplicatedLog` trait.
- Command encoding: prost-derive (recommended) vs postcard.
- The envelope's byte layout.
- The torn-tail vs corruption classification rule.
- Whether attempt grants are logged (recommended: yes, `AttemptGranted`).
- Outbox keying and delivery discipline.
- Timer window sizing.
- Checkpoint cadence.
- Full-scan vs partial verification at open.
- Test harness design.

### Deferred Ideas (OUT OF SCOPE for Phase 2)
- Snapshot format, upload and install (Phase 8 / Phase 12).
- Follower fetch, real ISR math, network transport (Phase 12).
- Metadata Raft as the epoch source (Phase 4). Phase 2 uses a local leadership source.
- The Dispatcher (DISP-01, Phase 4). Phase 2 only produces `EnqueueReady` actions and the `ready` table.
- The 10k-fault QA-01 gate (Phase 5). Phase 2 builds the harness and runs a smaller loop.
- Blob GC and object-store backends (Phase 8).
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| STOR-01 | Append-only journal on the vendored WAL with group commit. Every batch carries `{epoch, codec_ver, crc32c, cmds}` | §Envelope layout; §Sequencer pattern (`append_enqueued` FIFO contract); §Vendored WAL findings |
| STOR-02 | Every read verifies CRC. A corrupt record outside a torn tail fails partition open | §Vendored WAL findings: V2 `verify_checksum()` is a no-op, and the read path *silently stops* at bad bytes. §Verified reader; §Torn-tail rule |
| STOR-03 | LEO recovered by scanning on restart. A torn tail is truncated | §Open sequence; §Torn-tail rule. The WAL does no recovery itself (`WalManager::recover` is a no-op, and a restart opens a *new* segment, leaving the torn tail behind) |
| STOR-04 | `ReplicatedLog` trait (append, HW, epoch cache, ISR); RF=1 with ISR={self} | §ReplicatedLog trait; HW published from the WAL `commit_callback`; port of `LeaderEpochCache` |
| STOR-05 | Deterministic apply into redb, `None` + periodic `Immediate` checkpoint of `applied_offset`; apply never passes HW | §Apply loop; §Durability tiers [S3]; `ManualHwLog` test double |
| STOR-06 | Deleting the state store and restarting rebuilds byte-identical state (rolling hash matches) | §Rolling hash + logical state digest. Pitfall: redb *file bytes* are not comparable; compare a logical digest |
| STOR-07 | EIO/ENOSPC fatal, never retried or reported as success | §Vendored WAL findings: `commit_batch` logs and **continues** after an fsync error. A vendored patch is required (poison). redb commit errors are also fatal [S3] |
| STOR-08 | Segments deleted only below the durable checkpoint; no GC before the readiness barrier | §Trim rule + `TrimPermit`; §Startup state machine |
| STOR-09 | Fixed P configured at creation; run ids embed their partition | §Ids: `RunId = u16 partition ‖ uuidv7`, stable xxh3 routing hash with golden vectors |
| STOR-10 | `LeaderAnnounced{epoch}` begins every term; `(epoch, attempt)` tokens validated at apply | §Fencing: `LeaderAnnounced` + logged `AttemptGranted` + apply rules |
| STOR-11 | Validation at apply; `AppendAck` only after apply | §Apply outcomes + `PendingAcks` resolved after the redb txn commits |
| STOR-12 | Per-partition outbox + shuffler, `(src_partition, seq)` dedupe | §Outbox/shuffler: per-target ordered, single in-flight batch per target, `inbound_seen[src]` high-water mark |
| STOR-13 | Lazy windowed timers; idempotent `TimerFired` | §Timer service: due-window range scan, refill, apply no-ops when the row is gone |
| STOR-14 | Payloads > 256 KiB stored in the blob store and referenced from the journal | §Blob store: content-addressed, durable *before* the referencing append, apply rejects oversize inline payloads |
</phase_requirements>

## Summary

Phase 2 builds three crates on top of Phase 1's `vendor/ostro-wal`:

- `ostro-types`: ids, offsets, epochs, tokens, the envelope codec and the `Command` types.
- `ostro-log`: the RF=1 `ReplicatedLog`. It owns recovery, verified reads, the sequencer, the HW and the epoch cache.
- `ostro-partition`: the redb state machine, apply loop, outbox/shuffler, timers and retention.

It also adds a small local-disk `ostro-blob`. The architecture is already decided in project research. This phase is mostly about getting the **byte-level and failure-path details right**, because the format freezes soon after.

Reading the Chronik WAL source at `08715f4` settled the open gap and turned up four things the plan must address.

1. **The open gap: no recovery path decodes `canonical_data` as Kafka.** `extract_offset_range` is the only bincode decoder, it has zero callers, and Phase 1 deletes it. HIGH confidence; this agrees with the Phase 1 research.
2. **The WAL has no real recovery.** `WalManager::recover()` only constructs the manager. `GroupCommitWal` discovers segment files by name, and on the first write it opens a **new** segment (`max_id + 1`). A torn tail is left in the previous segment, where it now sits *mid-log* structurally. The read path (`segment_index::parse_record`) treats unparseable bytes as "end of this segment" and **silently moves on**. So Ostro must scan, classify and physically cut the torn tail (with `set_len` and fsync) **before** the first append to that partition. Otherwise the next open sees mid-log corruption.
3. **`verify_checksum()` returns `Ok` for V2 and the indexed read path never checks CRCs.** Ostro must verify the WAL frame CRC (`calculate_checksum() == get_crc32()`) *and* its own envelope CRC32C on every batch it reads.
4. **`commit_batch` violates STOR-07.** On a `write_all`/`sync_all` error it returns early. The waiters get an error because the dropped oneshot is mapped to `CommitFailed`, which is fine. But the worker loop **logs and keeps going** with the same file handle. The next batch is appended after possibly-partial bytes and can be fsynced "successfully", which is the classic fsyncgate failure. A vendored patch must **poison the partition queue**: fail all pending and future appends, stop the worker, and record the last durable byte length. Segment creation, rotation and deletion also never fsync the parent directory. That must be patched too.

**Primary recommendation:** Build Phase 2 bottom-up in these waves:

1. **types/envelope.** Fixed 32-byte LE header plus length-prefixed prost-encoded commands, with CRC32C over everything.
2. **`ostro-log` RF=1.** A full verified scan at open, an etcd-style torn-tail rule, a sequencer over `append_enqueued`, the HW from the WAL commit callback, and a ported `LeaderEpochCache`.
3. **`ostro-partition` apply loop.** One OS thread per partition and pure apply into one redb write txn per chunk, with `applied_offset` and the rolling hash in the same txn. Acks go out after commit.
4. **Leader services.** Shuffler, timers and retention, all behind a readiness barrier.
5. **The crash/corruption harness.** Failpoints, kill -9 loops, offline bit-flip and torn-tail injection, and proptest model tests, including a rebuild-digest property.

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `vendor/ostro-wal` (vendored chronik-wal) | Chronik `08715f4`, modernised in Phase 1 | Segments, group commit, sparse index, suffix truncation | Locked decision. Battle-tested group commit with the documented FIFO "return order = file order" contract |
| redb **[S3]** | **4.6.0** (2026-11-13) | Per-partition derived state (`state.redb`) | Locked. Pure Rust (libc only). `Durability::{None, Immediate}`, MVCC readers, default `ExclusiveWriter` mode (which supports `None`) |
| prost | **0.14.x** (already in the workspace for tonic) | `Command` wire encoding via `#[derive(prost::Message)]`, with no build step | Protobuf field numbers give *additive* evolution after the Phase 5 freeze. The bytes are CRC'd once, and Ostro never relies on re-encoding |
| crc32c | **0.6.9** | Envelope checksum (Castagnoli). Hardware-accelerated (SSE4.2 detected at runtime) | Same polynomial as Kafka/iSCSI. The requirement names crc32c |
| crc32fast | 1.5.x (already a WAL dependency) | Verify the WAL V2 frame CRC (CRC-32/IEEE) on read | That is what the vendored `calculate_checksum()` uses |
| blake3 | **1.8.7** | Rolling apply hash and logical state digest | Fast, already in the stack. Internal only |
| sha2 | 0.11.x | Blob content address (sha256) | Same identity scheme as modules (user-visible, interoperable) |
| xxhash-rust | **0.8.21** (feature `xxh3`) | Stable partition routing hash `xxh3_64` | The XXH3 output is a frozen spec. `std::hash` is randomly keyed and unstable across Rust releases, so it cannot be used |
| uuid | 1.x (feature `v7`) | uuidv7 run-id suffix | Time-ordered keys give B-tree locality |
| tokio | **1.54.x** | Async runtime; `watch` for HW and health, `oneshot` for acks | Locked |
| metrics | 0.24.x (Phase 1 facade) | WAL/apply/shuffler/timer counters and histograms | Locked (replaces Chronik's MetricsRecorder) |
| thiserror | 2.0.x | Error enums | Phase 1 already moved the vendored code to 2.x |

### Supporting (dev/test)
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| fail | **0.5.1** | Failpoints (`fail_point!`) in `vendor/ostro-wal` and `ostro-log` for EIO/ENOSPC, partial writes and crash-at-point | Behind a `failpoints` feature. Tests live in their own test binary with `required-features` |
| proptest | **1.12.0** | Property tests: envelope round trip, rebuild digest, outbox exactly-once | Always |
| proptest-state-machine | **0.9.0** | Model-based tests: log (append/crash/corrupt/reopen) vs a `Vec` model, and partition apply vs a reference model | Log and partition model tests |
| tempfile | **3.27.0** | Per-test data dirs | Use them on the Linux filesystem. Under WSL2, never use `/mnt/c` |
| cargo-nextest | **0.9.152** (not installed locally, so Wave 0 installs it) | Process-per-test isolation (failpoints are process-global) and retries/timeouts for the crash loops | CI and local |
| LazyFS (optional) | dsrhaslab/lazyfs | A FUSE filesystem that drops un-fsynced data, which simulates power loss. Used by Jepsen and etcd | Nightly/manual only. FUSE is available in this WSL2 kernel (`/proc/filesystems` lists `fuse`). It is a C++ build, so keep it out of the Cargo graph |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| prost-derive commands | postcard 1.1.3 + serde enums | postcard is compact and simple, but not self-describing. Adding a field to a struct breaks old readers, so every change after the freeze needs a new variant or a `codec_ver` bump. prost gives field-number evolution, and it is already in the graph. **Use postcard for redb values** (derived and rebuildable), not for the log |
| Full verified scan at open | Kafka-style epoch checkpoint file + verifying only the tail | Faster opens on big logs. But STOR-02 says corruption must "fail the partition open", which only a full scan guarantees, and Phase 2 has no trimming to keep logs small. Do the full scan now and optimise when measured |
| One OS thread per partition for apply | `spawn_blocking` per chunk | redb is a sync API. A dedicated thread gives a deterministic single writer and no blocking-pool starvation. 16–64 threads is fine |
| Logged `AttemptGranted` | Soft (unlogged) attempt counter | Soft state can reuse attempt numbers across double failovers and lets a zombie win the `from_seq` race (Pitfall 14). One extra small command per activation, absorbed by group commit |
| redb **[S3]** | fjall 3.1.13 | Only if S3 shows redb T2 below threshold. Keep apply code behind a narrow `KvTxn` seam (see Architecture) so the swap is contained |

**Installation (workspace deps added in Phase 2):**
```toml
# Cargo.toml [workspace.dependencies] — additions
prost        = "0.14"
crc32c       = "0.6.9"
blake3       = "1.8.7"
sha2         = "0.11"
xxhash-rust  = { version = "0.8.21", features = ["xxh3"] }
uuid         = { version = "1", features = ["v7"] }
postcard     = { version = "1.1.3", features = ["use-std"] }
fail         = "0.5.1"
proptest     = "1.12.0"
proptest-state-machine = "0.9.0"
tempfile     = "3.27.0"
```
```bash
cargo install cargo-nextest --locked   # 0.9.152; not installed on this machine
```

## Vendored WAL: Verified Behaviour and Required Modifications

Source: `crates/chronik-wal/src/{group_commit.rs, manager.rs, record.rs, segment_index.rs, truncate.rs}` at `08715f4867c9f40e409c68d8e212bd9479d620c0`. Phase 1 vendors these files as `vendor/ostro-wal`.

### Answer to the open gap (HIGH)
**No WAL V2 recovery path decodes `canonical_data` as Kafka.**
- The only decoder is `manager.rs::extract_offset_range` (bincode → `MinimalCanonicalRecord`), and it has **zero callers**. Phase 1 deletes it.
- `WalManager::recover()` just calls `new()`.
- `GroupCommitWal::new()` → `discover_sealed_segments()` reads only file names and sizes.
- `truncate.rs` and `segment_index.rs` parse only V2 *framing* (header, length, trailer offsets) and treat `canonical_data` as opaque bytes.

### Behaviour the plan must design around
| # | Finding (file:line @08715f4) | Consequence for Ostro | Action |
|---|---|---|---|
| W1 | `record.rs:396` `verify_checksum()` returns `Ok(())` for V2. `segment_index.rs` `to_record()` copies payloads without any CRC check | Bit rot would be applied silently | `ostro-log` verifies the frame CRC **and** the envelope CRC32C on every batch returned by any read |
| W2 | `segment_index.rs` `parse_record` → `Parsed::Invalid` ⇒ "stop reading this segment", then the next segment is read. `truncate.rs` `plan_segment_truncation` also "stops at the first byte that does not parse" | Mid-log corruption is invisible: reads return a short or gapped log. **Violates STOR-02** | Ostro's open scan and its reads check offset continuity (`base == prev_last + 1`) and fail on any gap or Invalid outside the torn-tail region |
| W3 | `manager.rs:889` `read_segment()` **skips forward byte by byte** on parse errors (up to 3) | Could resynchronise onto garbage | **Never call `read_segment`.** Consider deleting it in the vendored copy (VENDORED.md) |
| W4 | `group_commit.rs:1102` `get_or_create_queue` always opens `wal_<p>_<max+1>.log`. Restarts never append to the old segment | The torn tail stays in segment N while new writes go to N+1. Each restart adds a (possibly empty) segment | Cut the torn tail **before** the first append, and skip empty segments in scans. Expect many small segments under frequent restarts |
| W5 | `group_commit.rs:1378–1386` `write_all(..).await?; sync_all().await?` returns early. The worker loop (`:1280`) logs `WORKER_ERROR` and **continues** with the same fd | After EIO/ENOSPC, later batches are written after partial bytes and can "succeed". **Violates STOR-07** | **Vendored patch P1: poison** (see the Code Examples) |
| W6 | No directory fsync after `WalWriter::create` (new segment), `rotate_to_new_segment`, `remove_file` or `set_len` | After power loss a new segment's directory entry can vanish, together with acked records in it | **Vendored patch P2:** `fsync_dir(partition_dir)` after create/rotate (before the first ack in that file) and after deletions and truncations |
| W7 | `durable_end_offset()` is `None` until something commits in this process. `committed_through` starts at `i64::MIN` | There is no LEO after a restart | `ostro-log` computes LEO from its own open scan. The WAL value is used only after the first commit |
| W8 | `enqueue_with_receipt` returns `Backpressure` when the depth is at or above `max_queue_depth` | Offsets reserved by the sequencer would be skipped if the sequencer advanced before enqueue succeeded | The sequencer advances `next_offset` **only after** `append_enqueued` returns `Ok`, under the same lock. It also bounds outstanding appends with a semaphore so backpressure is rare |
| W9 | `commit_callback: Fn(topic, partition, min_off, max_off)` is invoked after a successful batch fsync and before waiters are notified | This is the natural HW publisher for RF=1 | Register one callback that advances `hw[partition]` (`watch::Sender::send_if_modified`, monotonic max) |
| W10 | `GroupCommitWal::new` captures `Handle::current()` | It must be constructed inside the Tokio runtime | Construct it in async init |
| W11 | Layout is `base_dir/<topic>/<partition>/wal_<partition>_<seg>.log`. `discover_sealed_segments` skips topics starting with `__` | It differs from docs/03's `partitions/pNNNN/wal/<base>.seg` | Use one `GroupCommitWal` per node, `base_dir = <data>/wal`, topic `"p"`. Record the layout change for docs/03 before the Phase 5 freeze. Don't rename the vendored files |
| W12 | The tail cache is filled at append time and served only up to `committed_through` | Safe (it never serves undurable records) | Reads may use `read_tail` and fall back to `PartitionIndex`. Both go through Ostro verification |
| W13 | `truncate_to` quiesces (both locks), bumps `truncation_epoch` so in-flight batches are dropped, cuts files, then rotates | It is correct, and it is the primitive Phase 12 needs | Expose it through `ReplicatedLog::truncate_to`. At RF=1 it is used only by tests and repair |

## Architecture Patterns

### Recommended Project Structure
```
crates/
├── ostro-types/src/
│   ├── ids.rs            # PartitionId(u16), PartitionCount, RunId(18B), Offset(u64), Epoch(u32), AttemptToken
│   ├── routing.rs        # partition_for_key (xxh3, golden vectors), RunId::partition()
│   ├── envelope.rs       # 32-byte header + length-prefixed cmds, CRC32C, encode/verify
│   ├── command.rs        # prost-derived Command oneof (log format, FROZEN at Phase 5)
│   └── payload.rs        # PayloadRef { Inline(bytes ≤ 256 KiB) | Blob { sha256, len } }
├── ostro-log/src/
│   ├── replicated.rs     # trait ReplicatedLog + Isr, Role, LogHealth, EpochHistory
│   ├── rf1.rs            # Rf1Log: Sequencer + GroupCommitWal + HW watch (the RF=1 impl)
│   ├── recovery.rs       # open scan, torn-tail classification + cut, LEO, epoch rebuild
│   ├── reader.rs         # verified reads (frame CRC + envelope CRC + continuity + ≤ upto)
│   ├── epoch_cache.rs    # port of Chronik LeaderEpochCache (attribution in VENDORED.md)
│   └── hw.rs             # compute_hw(isr progress) — the ONE function (RF=1 and Phase 12)
├── ostro-partition/src/
│   ├── tables.rs         # redb TableDefinitions + format version  [S3]
│   ├── apply/            # PURE: apply_command(txn, cursor, offset, epoch, cmd) -> ApplyOutcome
│   ├── machine.rs        # apply thread, chunking, durability tiers, PendingAcks, digest  [S3 parts]
│   ├── startup.rs        # ordered startup state machine + ReadyBarrier
│   ├── leader/{shuffler.rs, timers.rs, retention.rs}
│   └── handle.rs         # PartitionHandle::submit / grant_attempt / subscribe_actions
├── ostro-blob/src/       # local content-addressed BlobStore (tmp+fsync+rename+dir fsync)
└── ostro-testkit/        # (dev) corrupt/tear helpers, ManualHwLog, FaultyBackend, crash-target bin
vendor/ostro-wal/         # + patches P1 (poison), P2 (dir fsync), P3 (failpoints)
```

### Pattern 1: The `ReplicatedLog` trait (STOR-04, future-proof for Phase 12)
**What:** A crate-boundary trait between `ostro-log` and `ostro-partition`. Both RF=1 and the Phase 12 cluster implement it. Offsets are **exclusive ends** everywhere (HW, LEO and log_start follow Kafka semantics). That is enforced in one type (`Offset`), and the invariant `log_start ≤ HW ≤ LEO` is checked in one function.
**When:** Always. `ostro-partition` is generic over `L: ReplicatedLog` (static dispatch), so the conformance tests instantiate RF=1 now and the cluster impl later.
**Async in traits:** Write `fn x(&self) -> impl Future<Output = T> + Send` explicitly. That works on stable since 1.75 and avoids the `Send`-bound problem of bare `async fn` in public traits.

```rust
// crates/ostro-log/src/replicated.rs
pub trait ReplicatedLog: Send + Sync + 'static {
    fn partition(&self) -> PartitionId;

    /// Leader only. Under the sequencer lock: reject `epoch < current_epoch`
    /// (StaleEpoch), assign [base, last], build the envelope, enqueue to the WAL.
    /// Returns once the batch's position in the file is fixed (NOT durable).
    fn append(&self, epoch: Epoch, cmds: Vec<Bytes>)
        -> impl Future<Output = Result<AppendReceipt, LogError>> + Send;

    /// Resolves when HW > receipt.last. Err if the partition failed or the
    /// entries were truncated. Never Ok for data that is not durable on ISR.
    fn wait_committed(&self, last: Offset) -> impl Future<Output = Result<(), LogError>> + Send;

    fn high_watermark(&self) -> watch::Receiver<Offset>; // exclusive
    fn log_end(&self) -> Offset;                         // LEO, exclusive
    fn log_start(&self) -> Offset;

    /// Verified batches covering [from, upto). `upto` is capped by the caller at HW.
    /// Every batch: frame CRC + envelope CRC32C + base/partition match + continuity.
    fn read(&self, from: Offset, upto: Offset, max_bytes: usize)
        -> impl Future<Output = Result<Vec<VerifiedBatch>, LogError>> + Send;

    fn epochs(&self) -> EpochHistory;              // leader-epoch cache snapshot
    fn isr(&self) -> Isr;                          // RF=1: {self}; never "assumed"
    fn role(&self) -> watch::Receiver<Role>;       // Leader{epoch} | Follower{..} | Offline
    fn health(&self) -> watch::Receiver<LogHealth>; // Healthy | Failed{reason}

    fn truncate_to(&self, end: Offset) -> impl Future<Output = Result<Offset, LogError>> + Send;
    /// Delete whole segments with last < floor. Requires a TrimPermit, which only
    /// Retention can mint after the ReadyBarrier (STOR-08).
    fn trim_prefix(&self, permit: TrimPermit) -> impl Future<Output = Result<Offset, LogError>> + Send;
}
```
Phase 12 adds follower-side methods (verbatim append of fetched batches, fetch serving) **additively**. The HW computation already lives in `hw.rs::compute_hw(&[ReplicaProgress])`, where RF=1 passes `[self.durable_end]`. That keeps D5 literal: the same function runs in both modes.

### Pattern 2: Envelope layout (STOR-01, frozen this phase)
```
canonical_data = Header (32 bytes, little-endian) ‖ Body
  0  magic      u32 = 0x5254534F ("OSTR")
  4  codec_ver  u16 = 1
  6  flags      u16 = 0 (reserved; must be 0 in v1 → reject otherwise)
  8  partition  u16
 10  reserved   u16 = 0
 12  epoch      u32
 16  base_off   u64        (must equal the WAL trailer base_offset)
 24  cmd_count  u32        (must equal WAL record_count; last = base + count - 1)
 28  crc32c     u32        = crc32c(header[0..28] ‖ body)
Body = cmd_count × ( len u32 ‖ prost-encoded Command )
```
- **Why the fixed header lives outside protobuf:** the epoch, partition and base can be read without decoding the commands. The epoch cache rebuild and (in Phase 12) fetch validation need that. The CRC covers the header, so a misdirected or misplaced record (wrong partition or offset) is detected even when its bytes are intact.
- **Why commands are length-prefixed:** the rolling hash and the per-command offsets work on the **stored command bytes**, so nothing ever depends on re-encoding being deterministic.
- **Offsets are assigned one per command.** A WAL record is one envelope, so `base_offset`, `last_offset` and `record_count` in the V2 trailer are always populated. That guarantees no code path needs to look inside `canonical_data` for offsets.
- **Size cap:** one envelope ≤ `max_envelope_bytes`, default 4 MiB. With a 256 KiB inline payload cap, any batch can be split.

### Pattern 3: Open / recovery sequence (STOR-03, STOR-02, STOR-08)
An explicit ordered state machine is tested for order (Pitfall 12):
```
Opening
 → LogRecovered     (per partition, before any append creates a queue)
     1. list wal/p/<id>/wal_<id>_*.log sorted by id; ignore empty files
     2. verified scan of every record: frame CRC, envelope CRC32C, partition, base == prev_last+1,
        epoch non-decreasing, codec_ver known
     3. classify the first failure (see Torn-tail rule): TORN → set_len(cut) + fsync(file) + fsync(dir);
        CORRUPT → open fails with {segment, byte_pos, offset_range, reason}
     4. fsync every retained segment once (after a crash, readable ≠ durable), rebuild the epoch cache,
        LEO = last+1, log_start = first base, HW := LEO (RF=1)
 → StateOpened      redb open (auto-repair, repair_callback logs progress) [S3]; verify meta:
                    format_ver, partition id, P; applied_offset A; state_hash
                    A > LEO → fail ("state ahead of log"; operator: rebuild). A < log_start → fail ("snapshot required", Phase 8)
 → CaughtUp         replay [A, HW) through apply in chunks; Actions DISCARDED
 → TermStarted      epoch = max(leadership_source.epoch, epochs.latest + 1); append LeaderAnnounced{epoch}; wait applied
 → Ready            rebuild leader services from tables (ready, timer window, outbox); open ReadyBarrier
                    → Retention / GC may now run; API may submit
```

### Pattern 4: Torn-tail vs corruption rule (STOR-02/03), adapted from etcd + Chronik raft `log_store`
Every "bad record" is: incomplete at EOF, bad magic/version, a frame length that disagrees with the trailer identity, a frame CRC mismatch, an envelope CRC mismatch, or an envelope/trailer mismatch.

**TORN** (cut at the start of the bad record) only if **all** of these hold:
1. It is in the **last non-empty segment**. Sealed segments were fully fsynced before rotation, so damage there is real.
2. The bad record starts **at or after** the end of the record containing the **durable checkpoint** offset (redb `applied_offset` at the last `Immediate` commit). Anything at or below that was fsynced, so damage there is real. **[S3: reads the checkpoint from redb meta]**
3. Either the record is incomplete at EOF, or the bad record's byte range contains an **all-zero 512-byte sector-aligned chunk** (etcd `isTornEntry`). Unflushed extents read back as zeros on ext4/xfs. A non-zero bit flip is corruption.

Everything else is **CORRUPT**: the open fails loudly and nothing is skipped. Records after a torn cut are discarded even if they look valid. They were in the same un-fsynced window and could not have been acked: an fsync would have made the earlier bytes durable too. Log the count at `warn`.

> Note: `kill -9` never produces torn writes, because the page cache survives the process. At most it leaves a short final write. Torn and zero-filled tails come only from power loss or kernel crashes. Test them with offline injection (and optionally LazyFS), not with kill loops.

### Pattern 5: Sequencer (STOR-01; from the documented `append_enqueued` contract)
```rust
// crates/ostro-log/src/rf1.rs (sketch)
pub async fn append(&self, epoch: Epoch, cmds: Vec<Bytes>) -> Result<AppendReceipt, LogError> {
    let _slot = self.inflight.acquire().await?;            // bounds queue depth (W8)
    let (receipt, wait) = {
        let mut seq = self.seq.lock().await;               // per-partition
        self.health.ensure_healthy()?;
        if epoch < seq.current_epoch { return Err(LogError::StaleEpoch { current: seq.current_epoch }); }
        let base = seq.next;
        let n = cmds.len() as u64;
        let env = envelope::encode(self.pid, epoch, base, &cmds);
        let rec = WalRecord::new_v2("p".into(), self.pid.as_i32(), env, base.as_i64(), (base + n - 1).as_i64(), n as i32);
        let wait = self.wal.append_enqueued("p".into(), self.pid.as_i32(), rec, /*acks*/ 1).await
            .map_err(LogError::from)?;                     // on Err: `next` NOT advanced
        if epoch > seq.current_epoch { seq.current_epoch = epoch; self.epochs.write().assign(epoch, base); }
        seq.next = base + n;
        (AppendReceipt { base, last: base + n - 1, epoch }, wait)
    };                                                      // lock released before fsync
    if let Err(e) = wait.wait().await { self.fail(e.to_string()); return Err(LogError::Failed); }
    Ok(receipt)
}
```
Never use `acks = 0`.

### Pattern 6: Apply loop with durability tiers (STOR-05, STOR-11) [S3]
```rust
// crates/ostro-partition/src/machine.rs (sketch; runs on a dedicated std::thread per partition)
loop {
    let hw = wait_for_hw_above(&mut hw_rx, cursor.next)?;            // blocks; never reads ≥ hw
    let batches = rt.block_on(log.read(cursor.next, hw, CHUNK_BYTES))?; // verified
    let checkpoint = cursor.since_ckpt >= CKPT_CMDS || last_ckpt.elapsed() >= CKPT_EVERY || trim_requested;
    let mut txn = db.begin_write()?;
    txn.set_durability(if checkpoint { Durability::Immediate } else { Durability::None })?;
    let mut done = Vec::new(); let mut actions = Vec::new();
    for b in &batches {
        assert!(b.last() < hw, "apply passed HW");                   // invariant → Failed partition
        for (off, cmd_bytes) in b.commands() {
            let outcome = apply::apply_command(&txn, &mut cursor, off, b.epoch(), cmd_bytes, &mut actions)?; // Err = storage error = fatal
            cursor.hash = blake3_chain(cursor.hash, off, outcome.code(), cmd_bytes);
            done.push((off, outcome));
        }
    }
    tables::write_meta(&txn, cursor.next, &cursor.hash, cursor.epoch)?;  // SAME txn as the data
    txn.commit()?;                                                    // any error: partition Failed, reopen = replay
    if checkpoint { durable_ckpt.send_replace(cursor.next); last_ckpt = Instant::now(); cursor.since_ckpt = 0; }
    acks.resolve(done);                     // AppendAck AFTER commit → readers see the state
    if role.is_leader() { actions_tx.send(actions)?; }
}
```
- **Cadence defaults:** `CKPT_EVERY = 100 ms`, `CKPT_CMDS = 4096`. Also checkpoint before any trim and on graceful shutdown (an explicit final `Immediate`; don't rely on Drop).
- **The checkpoint decision is local and wall-clock driven, but it never changes logical state.** The digest property test proves that.
- **redb's commit contract (docs.rs 4.6):** on a commit error other than `TransactionPoisoned`, "changes apply atomically, but they may already be durable", and the database refuses writes until it is reopened. So the response is always: partition Failed → reopen → replay from the persisted `applied_offset`.

### Pattern 7: Fencing — `LeaderAnnounced` + logged `AttemptGranted` (STOR-10)
| Command | Appended by | Apply accepts iff | Effect |
|---|---|---|---|
| `LeaderAnnounced{epoch, node, announced_at_ms}` | new leader, first entry of term | `epoch > cursor.epoch` **and** envelope epoch == epoch | `cursor.epoch = epoch`; push to epoch history in `meta` |
| `AttemptGranted{run, token}` | dispatcher (Phase 4), via `PartitionHandle::grant_attempt` | `token.epoch == cursor.epoch` ∧ `token > run.current_attempt` ∧ run non-terminal | `run.current_attempt = token` |
| `EventsAppended{run, token, from_seq, events}` | executor via leader | `token == run.current_attempt` ∧ `token.epoch == cursor.epoch` ∧ `from_seq == run.next_seq` ∧ limits | append events, derive timers/outbox/ready |
| any command | — | envelope epoch == `cursor.epoch` (or the command is the `LeaderAnnounced` raising it) | otherwise `Rejected(StaleEpoch)` |

- `AttemptToken{epoch: u32, n: u32}` is ordered lexicographically.
- RF=1 restart = new epoch. Its source is `LocalLeadership`: `epochs.latest + 1`. Phase 4 replaces it with the metadata partition map's epoch through a `LeadershipSource` trait (`watch::Receiver<Assignment>`).
- **The grant is logged** so tokens are monotonic across any number of failovers, and a zombie with an older token is rejected even if it appends first (Pitfall 14). The grant is enqueued before `Activate` is sent, so the sequencer's FIFO order guarantees the executor's appends land after it.

### Pattern 8: Apply outcomes and acks (STOR-11)
- `apply_command` returns `Result<ApplyOutcome, StorageError>`.
- `ApplyOutcome = Accepted{..} | Duplicate | Rejected{reason}`. Reasons: `StaleEpoch`, `Fenced{current}`, `SeqMismatch{expected}`, `RunNotFound`, `RunTerminal`, `PayloadTooLarge`, `Invalid`.
- Rejections are **deterministic no-ops**. They still consume their offset and are folded into the rolling hash.
- `PendingAcks: BTreeMap<Offset, oneshot::Sender<ApplyOutcome>>` is registered at append and resolved after `txn.commit()`. A `Rejected` outcome is returned to the caller as a rejection, never as Ok.
- The leader's optional speculative pre-check (G3) is **not** in Phase 2.

### Pattern 9: Outbox + shuffler (STOR-12)
**Tables:**
- `outbox: (target: u16, seq: u64) → OutboxMsg`. `seq` comes from a per-source counter in `meta` (`outbox_next_seq`).
- `inbound_seen: src: u16 → u64` holds the highest seq applied from that source.

**Apply (source):** a command that produces a cross-partition message (child start/complete, signal-other-run, cancel cascade, `ScheduleFired` → start) writes an outbox row in the same txn. **Same-partition targets also go through the outbox** (one code path).

**Shuffler (leader-only task per source):**
- For each target, it reads the outbox rows in ascending seq and sends **one `InboundBatch{src, msgs: [(seq, msg)…]}` at a time per target**. It waits for the target's `AppendAck` (after apply) before sending the next batch to that target.
- On ack it appends `OutboxTruncated{target, upto}`, and apply deletes those rows.
- On leadership change or restart, it restarts from the table. Re-sends are expected.

**Apply (target):**
- Each `(seq, msg)` with `seq <= inbound_seen[src]` is a `Duplicate` (no-op, acked).
- Otherwise it is processed and `inbound_seen[src] = seq`.

> **The ordering rule is load-bearing.** "Max seen" dedupe drops a message if a higher seq for the same (src → target) is applied first. So per target there is never more than one batch in flight, and each batch is in ascending order.

Routing goes through a `PartitionRouter` trait: an in-process call now, network forwarding in Phase 13.

### Pattern 10: Lazy windowed timers (STOR-13)
**Tables:**
- `timers: (due_at_ms: u64, run: [u8;18], op: u64) → ()`. redb tuple keys compare element-wise and u64 compares numerically, so a range scan is in due order.
- The reverse index `timer_of: (run, op) → due_at_ms` is used for cancellation.

**TimerService (leader-only):**
- It holds a `BTreeSet<(due, run, op)>` **window** of at most `WINDOW_MAX = 10_000` entries or `WINDOW_SPAN = 60 s`, whichever comes first.
- Refills come from a read txn: the range from `0` (overdue timers come back after restart) to `now + span`, with a limit.
- `ScheduleTimer` actions with `due < window_end` are inserted directly. Later ones are left to the next refill.
- `CancelTimer` removes the entry.
- It sleeps until the earliest entry, then appends `TimerFired{run, op, due_at_ms, fired_at_ms}` in batches.

**Apply:** if the `timers` row exists, delete it plus the index row, append a `timer_fired` journal event and enqueue ready. Otherwise the command is a `Duplicate` no-op. A new leader may re-fire, and that is safe.

Wall clock is used only in the leader service. The command carries `fired_at_ms` (G7).

### Pattern 11: Blob offload (STOR-14)
- **`BlobStore::put` happens before the referencing append**, done by the submitter (API/executor path), never in apply. The steps are: sha256 → `blobs/<hex[0:2]>/<hex>`; write a tmp file in `<data>/tmp`, `sync_all`, `rename`, then fsync the parent dir. If the file already exists, it is complete (only renamed after fsync), so `put` is idempotent.
- The journal carries `PayloadRef::Blob{sha256, len}`. **Apply rejects `Inline` payloads > 256 KiB** with `PayloadTooLarge`, deterministically.
- **Reads verify the sha256.**
- A crash between blob write and append leaves an orphan blob, which is harmless. Blob GC is Phase 8, behind the readiness barrier with N-pass confirmation.

### Pattern 12: Trim rule + readiness barrier (STOR-08)
- `TrimPermit` can be minted only by `Retention` after `ReadyBarrier::is_open()`. Its floor is `min(durable_checkpoint, snapshot_floor, retention_floor)`.
- **`snapshot_floor` defaults to `Offset(0)` in Phase 2**, because no snapshots exist yet. Trimming is therefore *implemented and tested, but inert by default*. This is necessary because STOR-06 requires rebuilding from the full log until Phase 8 snapshots exist. Tests inject a snapshot floor.
- Before deleting, force an `Immediate` checkpoint and re-read `durable_checkpoint`.
- Delete only whole segments with `last < floor`, then fsync the dir.
- Log every pass at `info`, including "nothing to trim".

### Pattern 13: Ids and routing (STOR-09)
- `RunId` is 18 bytes: `partition u16 BE ‖ uuidv7 (16)`. Its string form is lowercase Crockford base32 without padding.
- `RunId::partition()` is a pure byte read.
- For new runs: `partition_for_key(workflow, idem_key, P) = xxh3_64(u32 len ‖ workflow ‖ idem_key) % P`. Without a key, use any partition (e.g. xxh3 of the uuid), because the id embeds it.
- **Golden test vectors** freeze the hash.
- P is persisted in `<data>/node.toml` and in every `state.redb` `meta`. A mismatch at open is fatal.

### Pattern 14: Determinism guardrails for apply
- Apply takes no clock, no randomness, no I/O besides the txn, and no `HashMap`/`HashSet` iteration. Timestamps come from commands.
- Enforce this with `ostro-partition/clippy.toml` `disallowed-methods` (`std::time::SystemTime::now`, `std::time::Instant::now`, `rand::random`, `rand::thread_rng`) and `disallowed-types` (`std::collections::HashMap`, `HashSet`). Add `#[allow]` only in `leader/*` and `machine.rs`.
- **Rolling hash:** `H_n = blake3(H_{n-1} ‖ offset_le ‖ outcome_code ‖ cmd_bytes)`, stored with `applied_offset`.
- **Logical state digest:** iterate every table in a fixed order, key-ordered, and feed `len‖key‖len‖value` into blake3. **Compare digests, never redb file bytes**, because page layout depends on transaction chunking.
- **[S3]** Narrow seam: apply code uses a small `KvTxn` trait (`get/insert/remove/range` per named table) implemented for redb's `WriteTransaction`. If S3 picks fjall, only that impl changes.

### redb table set (Phase 2) [S3]
Tables:
- `runs`, `events (run, seq)`, `payloads (run, kind) → PayloadRef`
- `ready (queue, priority, ready_at_ms, run)`, whose value includes the **concurrency key** (DISP-02 from day one)
- `timers` and `timer_of`, `inbox (run, name, n)`, `dedupe (workflow, idem_key) → run`
- `outbox (target, seq)`, `inbound_seen (src)`
- `dlq`, `effects`: defined now, used later
- `meta`: `format_ver`, `partition`, `P`, `applied_offset`, `state_hash`, `epoch`, `epoch_history`, `outbox_next_seq`

Values are postcard-encoded structs plus a per-table format version. **Avoid `Option<&str/String/bool/char>` in persisted redb types** (redb 5.0 changes their encoding).

### Anti-Patterns to Avoid
- **Trusting `WalManager::read_from*` output as-is.** It skips CRC checks and silently stops at bad bytes (W1/W2). Always go through `ostro-log::reader`.
- **Appending before the torn tail is cut.** A restart opens a new segment, so the torn bytes become a permanent mid-log hole (W4).
- **Retrying fsync, or reusing the fd after an error.** That is fsyncgate (W5).
- **Advancing the sequencer before enqueue succeeds.** That leaves offset gaps (W8).
- **Comparing `state.redb` files byte for byte.** Compare logical digests.
- **Concurrent shuffler batches to one target.** "Max seen" dedupe then drops messages.
- **`applied_offset` in a different txn than the data** (Pitfall 10).
- **Default redb cache size.** 1 GiB *per Database* × 16 partitions; see the Pitfalls.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Group commit / segment rotation / sparse index / suffix truncation | A new WAL | `vendor/ostro-wal` (`GroupCommitWal`, `PartitionIndex`, `truncate_to`, `plan_segment_truncation`) | Years of Chronik bug fixes (RP-3.3 in-flight batch vs truncation, out-of-order batches, an unlinked open segment). Patch only P1–P3 |
| Leader-epoch history | A new epoch map | Port `LeaderEpochCache` (`chronik-server/src/replication/leader_epoch.rs` L40–190, pure, ~150 LOC) with a VENDORED.md entry | Kafka-exact `end_offset_for_epoch`, including the UNDEFINED answers that prevent truncation data loss |
| Torn-tail detection heuristic | Ad-hoc "stop at first error" | etcd `isTornEntry` (zero-sector rule) + Chronik raft `log_store` "last segment only" rule | Proven rules. "Stop at first error" is exactly the W2 bug |
| CRC32C | Table-driven CRC | `crc32c` crate (hardware-accelerated) | Speed and correctness |
| Failpoints | `cfg(test)` flags sprinkled by hand | `fail` 0.5.1 `fail_point!` + `FailScenario` | Standard (TiKV). Compiles to nothing without the feature |
| Model-based crash tests | Handwritten random loops | `proptest-state-machine` 0.9 (`ReferenceStateMachine` + `StateMachineTest`) | Shrinking gives minimal failing histories |
| Fault injection under redb **[S3]** | Patching redb | A wrapper implementing `redb::StorageBackend` (`len/read/set_len/sync_data/write`) around `FileBackend`, passed to `Builder::create_with_backend` | An official extension point |
| Power-loss simulation | Custom FUSE | LazyFS (optional, nightly) | Used by Jepsen, etcd and MongoDB |
| Stable hashing for routing | `std::hash::DefaultHasher` | `xxhash-rust` xxh3 | DefaultHasher is not stable across Rust versions |

**Key insight:** in this domain, nearly every "simple" storage shortcut is a known data-loss bug: stop-at-first-error, retry fsync, skip the dir fsync, trust the checksum function, compare files. Reuse the primitives that have their scars documented, and spend the effort on the failure-path tests.

## Common Pitfalls

### Pitfall 1: Silent mid-log truncation through the vendored read path
**What goes wrong:** A bit flip in segment 3 makes `PartitionIndex` stop reading segment 3 and continue in segment 4. Apply sees a gap, or worse, the open "succeeds" with a shorter log.
**Why:** `Parsed::Invalid` means "end of segment" by design (W2).
**How to avoid:** Ostro's open scan and reader check continuity and fail on any Invalid outside the torn-tail rule.
**Warning signs:** `base != prev_last + 1` anywhere. Fewer records after a restart than before.

### Pitfall 2: Treating a kill -9 loop as torn-write coverage
**What goes wrong:** The torn-tail code is never exercised, because the page cache survives SIGKILL.
**How to avoid:** Offline injection: truncate at every byte of the last record, zero-fill sector ranges, flip bits at random positions. Add a `wal::partial_write_then_abort` failpoint, and optionally LazyFS.
**Warning signs:** The torn-tail branch has no coverage.

### Pitfall 3: fsyncgate in the group-commit worker
**What goes wrong:** The batch after an EIO "succeeds", so a later ack covers data that never reached disk.
**How to avoid:** Patch P1 (poison): fail all waiters, drain and fail pending, stop the worker, mark the partition Failed, and don't reopen automatically in Phase 2. Best effort: `set_len(durable_len)` to drop non-durable bytes. Test `append_failure_is_never_reported_as_success`.
**Note (MEDIUM):** after an fsync failure Linux may mark pages clean while they're still in cache, so a later reader can see bytes that aren't durable. The partition stays Failed until a restart re-scans and fsyncs. Document this as the "outcome unknown" boundary: those callers got an error, never an Ok.

### Pitfall 4: redb page cache × partitions [S3]
**What goes wrong:** `Builder::new()` defaults to a **1 GiB cache per Database**. With 16 (or 64) `state.redb` files that's 16–64 GiB potential RSS.
**How to avoid:** `Builder::new().set_cache_size(node_budget / P)` (e.g. 32–64 MiB each), configurable.

### Pitfall 5: `Durability::None` file growth and lost commits [S3]
**What goes wrong:** `None` commits are lost on crash (expected). Pages are reused only after durable commits (redb 4.2 note), so a long run without `Immediate` grows the file.
**How to avoid:** Time- and count-based `Immediate` checkpoints, a final `Immediate` at shutdown, and a gauge for the file size.

### Pitfall 6: The torn-tail rule cutting acknowledged data
**What goes wrong:** A real bit flip in the final, already-acked batch looks torn and is cut, losing acked commands.
**How to avoid:** Rule condition 2 (never cut at or below the durable checkpoint) and condition 3 (a zero-sector requirement; plain bit flips are CORRUPT). Residual risk: a crash after the ack but before the next checkpoint, *combined with* zeroed sectors in already-fsynced data. That requires media failure and is accepted.

### Pitfall 7: Outbox reordering drops messages
**What goes wrong:** Concurrent delivery of seq 5 and seq 7 to one target. If 7 applies first, 5 is dropped as a duplicate.
**How to avoid:** One in-flight batch per (source, target), in ascending order. A property test with randomised scheduling covers it.

### Pitfall 8: Startup races (Chronik RP-3.3 deleted live WALs 2 ms after recovery)
**How to avoid:** `TrimPermit` requires the barrier, and the test asserts the order. Trim is inert in Phase 2 (snapshot floor 0).

### Pitfall 9: Rebuild digest mismatches caused by apply-chunking leakage
**What goes wrong:** Apply logic depends on chunk boundaries or checkpoint timing (e.g. "every N commands do X"), so rebuilt state differs.
**How to avoid:** The proptest varies chunk sizes and checkpoint points. Live and rebuilt digests must be equal.

### Pitfall 10: Failpoints leaking across tests
**What goes wrong:** The `fail` registry is process-global, so parallel tests trip each other's failpoints.
**How to avoid:** Put failpoint tests in a separate test binary (`required-features = ["failpoints"]`), use `FailScenario::setup()`, and run under nextest (process per test).

### Pitfall 11: WSL2 and macOS fsync semantics
**What goes wrong:** Measurements and tests on `/mnt/c` (9p/drvfs) are meaningless. macOS `sync_all` maps to `F_FULLFSYNC` (MEDIUM: Rust std behaviour), so it is very slow. Tests pass but look hung.
**How to avoid:** Use tempdirs on ext4 (`/tmp` here is ext4 on `/dev/sdd`, verified). On macOS CI, use small iteration counts.

### Pitfall 12: Phase 3 parallel edits to `ostro-types`
**What goes wrong:** Both phases define event and command types and conflict.
**How to avoid:** **Phase 2 owns `ostro-types::{ids, routing, envelope, command, payload}`.** The journal event inside `EventsAppended` is `JournalEvent{seq, kind: u32, payload: PayloadRef}` with **opaque canonical bytes**. Phase 3 owns the meaning of `kind` and the payload contents, and must not change the envelope or the command shapes.

## Code Examples

### Vendored patch P1: poison on write/fsync error (`vendor/ostro-wal/src/group_commit.rs`, `commit_batch`)
```rust
// Replaces: file.write_all(&combined_buffer).await?; file.sync_all().await?;
// VENDORED.md: "P1 — write/fsync errors poison the partition queue (fsyncgate, Ostro STOR-07)"
let io = match file.write_all(&combined_buffer).await {
    Ok(()) => file.sync_all().await,
    Err(e) => Err(e),
};
if let Err(e) = io {
    let reason = format!("WAL write/fsync failed for {}/{}: {e}", queue.topic, queue.partition);
    queue.poisoned.store(true, Ordering::SeqCst);       // new field: AtomicBool
    drop(file);
    queue.tail.lock().await.clear();
    for w in batch { if let Some(tx) = w.response_tx { let _ = tx.send(Err(WalError::Poisoned(reason.clone()))); } }
    for w in queue.pending.lock().await.drain(..) { if let Some(tx) = w.response_tx { let _ = tx.send(Err(WalError::Poisoned(reason.clone()))); } }
    return Err(WalError::Poisoned(reason));             // new variant
}
queue.durable_len.store(/* segment bytes after this batch */, Ordering::SeqCst); // new field
// + enqueue_with_receipt / enqueue_nowait: `if queue.poisoned.load(SeqCst) { return Err(WalError::Poisoned(..)) }`
// + worker loop: on Err(Poisoned) → `return` (do not continue)
```

### Vendored patch P3: failpoints in the writer
```rust
// vendor/ostro-wal/src/group_commit.rs, WalWriter::Standard paths (feature = "failpoints")
async fn write_all(&mut self, data: &[u8]) -> Result<()> {
    fail::fail_point!("wal::write", |arg| {
        // "enospc": write a prefix then fail; "eio": fail immediately
        let code = if arg.as_deref() == Some("enospc") { libc::ENOSPC } else { libc::EIO };
        Err(std::io::Error::from_raw_os_error(code).into())
    });
    /* ... */
}
async fn sync_all(&self) -> Result<()> {
    fail::fail_point!("wal::fsync", |_| Err(std::io::Error::from_raw_os_error(libc::EIO).into()));
    /* ... */
}
```

### Envelope encode/verify
```rust
// crates/ostro-types/src/envelope.rs
pub const MAGIC: u32 = u32::from_le_bytes(*b"OSTR");
pub const HEADER_LEN: usize = 32;

pub fn encode(p: PartitionId, epoch: Epoch, base: Offset, cmds: &[Bytes]) -> Vec<u8> {
    let body: usize = cmds.iter().map(|c| 4 + c.len()).sum();
    let mut b = Vec::with_capacity(HEADER_LEN + body);
    b.extend_from_slice(&MAGIC.to_le_bytes());
    b.extend_from_slice(&1u16.to_le_bytes());          // codec_ver
    b.extend_from_slice(&0u16.to_le_bytes());          // flags
    b.extend_from_slice(&p.0.to_le_bytes());
    b.extend_from_slice(&0u16.to_le_bytes());          // reserved
    b.extend_from_slice(&epoch.0.to_le_bytes());
    b.extend_from_slice(&base.0.to_le_bytes());
    b.extend_from_slice(&(cmds.len() as u32).to_le_bytes());
    b.extend_from_slice(&[0u8; 4]);                    // crc placeholder
    for c in cmds { b.extend_from_slice(&(c.len() as u32).to_le_bytes()); b.extend_from_slice(c); }
    let crc = crc32c::crc32c_append(crc32c::crc32c(&b[..28]), &b[HEADER_LEN..]);
    b[28..32].copy_from_slice(&crc.to_le_bytes());
    b
}
// verify(): len ≥ 32, magic, codec_ver == 1, flags == 0, crc matches, partition/base/count match the
// WAL trailer, the body parses into exactly cmd_count length-prefixed commands with no trailing bytes.
```

### Verified read of one WAL record (frame + envelope)
```rust
// crates/ostro-log/src/reader.rs
fn verify(rec: &WalRecord, expect_base: Offset, pid: PartitionId) -> Result<VerifiedBatch, Corruption> {
    if rec.calculate_checksum() != rec.get_crc32() { return Err(Corruption::FrameCrc); }   // W1
    let WalRecord::V2 { canonical_data, base_offset, last_offset, record_count, partition, .. } = rec
        else { return Err(Corruption::NotV2) };
    if *partition != pid.as_i32() || *base_offset != expect_base.as_i64() { return Err(Corruption::Misplaced); }
    let env = envelope::verify(canonical_data)?;                                          // CRC32C etc.
    if env.base != expect_base || env.cmd_count as i32 != *record_count
        || *last_offset != base_offset + *record_count as i64 - 1 { return Err(Corruption::TrailerMismatch); }
    Ok(VerifiedBatch::new(env))
}
```

### redb transaction with durability tier **[S3]**
```rust
// Source: docs.rs redb 4.6.0 — WriteTransaction::set_durability -> Result<(), SetDurabilityError>
let mut txn = db.begin_write()?;
txn.set_durability(if checkpoint { Durability::Immediate } else { Durability::None })?;
{
    let mut meta = txn.open_table(META)?;
    meta.insert("applied_offset", next.0.to_be_bytes().as_slice())?;
    meta.insert("state_hash", hash.as_bytes().as_slice())?;
}
txn.commit()?; // Err ⇒ partition Failed (db refuses writes until reopened)
```

### Fault-injecting redb backend **[S3]**
```rust
// Source: docs.rs redb 4.6.0 StorageBackend (len/read/set_len/sync_data/write)
#[derive(Debug)]
struct FaultyBackend { inner: redb::backends::FileBackend, plan: Arc<FaultPlan> }
impl redb::StorageBackend for FaultyBackend {
    fn len(&self) -> std::io::Result<u64> { self.inner.len() }
    fn read(&self, off: u64, out: &mut [u8]) -> std::io::Result<()> { self.inner.read(off, out) }
    fn set_len(&self, len: u64) -> std::io::Result<()> { self.plan.check("set_len")?; self.inner.set_len(len) }
    fn sync_data(&self) -> std::io::Result<()> { self.plan.check("sync")?; self.inner.sync_data() }
    fn write(&self, off: u64, data: &[u8]) -> std::io::Result<()> { self.plan.check("write")?; self.inner.write(off, data) }
}
// Builder::new().set_cache_size(64 << 20).create_with_backend(FaultyBackend { .. })
```
(Check the exact `read` signature against the redb version pinned in Cargo.lock. Without `experimental-api-5`, the I/O methods use `std::io::Error`, per docs.rs.)

### kill -9 crash loop (process level)
```rust
// crates/ostro-testkit/tests/crash_loop.rs (#[ignore], run explicitly / nightly)
for i in 0..iters {
    let mut child = Command::new(env!("CARGO_BIN_EXE_crash-target"))
        .arg(&dir).stdout(Stdio::piped()).spawn()?;     // appends; prints "ACK <offset> <cmd_hash>" after AppendAck
    std::thread::sleep(Duration::from_millis(rng.random_range(5..300)));
    child.kill()?;                                       // SIGKILL on Unix
    let acked = parse_acks(child.wait_with_output()?.stdout);
    let reopened = open_partition_readonly(&dir)?;       // full open sequence
    assert!(acked.iter().all(|(off, h)| reopened.cmd_hash_at(*off) == Some(*h)), "lost acked command (iter {i})");
    assert_no_duplicate_offsets(&reopened);
    assert_eq!(reopened.live_digest(), rebuild_from_log(&dir).digest()); // STOR-06 under crashes
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| PostgreSQL retrying fsync | PANIC / fatal on fsync failure (`data_sync_retry=off`) | 2018 (fsyncgate) | P1 poison patch; the partition goes Failed |
| `kill -9` as crash testing | + loss of un-fsynced data (LazyFS, VLDB 2024; Jepsen `lose-unfsynced-writes!`) | 2023–2025 | Torn-tail tests need injection, not just kills |
| redb `Durability::{None, Eventual, Immediate, Paranoid}` | **4.x: only `None`/`Immediate`**. `set_durability` returns `Result`. `set_two_phase_commit` / `set_quick_repair` are separate | redb 3.0 → 4.x | Use 4.6 names. 2PC/quick-repair are S3 variants, off by default |
| Soft attempt bump on failover (docs/03 §2.2) | Logged epoch record + logged attempt grant | Project research (Restate epochs, Pitfall 14) | Changes the command set (this phase) |
| chrono / bincode | jiff / postcard / prost | 2025–2026 (RUSTSEC-2026-0543; bincode repo archived) | Don't add them |

**Deprecated/outdated:**
- docs/03 §2.2's "new leader bumps attempts" is replaced by `LeaderAnnounced` + `AttemptGranted`.
- docs/03 §3's `wal/<base_offset>.seg` naming is replaced by the vendored `wal/p/<id>/wal_<id>_<seg>.log`. Update docs/03 before the Phase 5 freeze.
- `WalManager::read_segment` (byte-skipping) must not be used.

## Open Questions

1. **Is a full verified scan at every open fast enough?**
   - What we know: it is O(log bytes). There is no trimming in Phase 2 because the snapshot floor is 0.
   - What's unclear: startup time on multi-GB dev logs.
   - Recommendation: run partitions in parallel and measure in the crash-harness plan. If it exceeds ~5 s/GB, add a Kafka-style epoch checkpoint plus "verify segments overlapping [checkpoint, LEO) at open, scrub the rest in the background", and record the deviation from the literal STOR-02 wording.
2. **Should an fsync failure ever reopen the partition automatically?**
   - What we know: STOR-07 forbids retrying.
   - What's unclear: whether operators want automatic recovery after ENOSPC is relieved.
   - Recommendation: Phase 2 stays Failed until restart. Add an admin "reopen partition" (full recovery) in a later phase.
3. **Same-partition outbox messages.**
   - The uniform path costs one extra command per child start in the same partition.
   - Recommendation: keep it uniform (one code path). Optimise only if benchmarks demand it (Phase 5 throughput gate).
4. **Should the epoch history live in redb `meta` (deterministic via apply) or in a log-side checkpoint file?**
   - Recommendation for Phase 2: rebuild the log-side cache from the open scan, and also record `epoch_history` in `meta` through apply. Revisit in Phase 12, where follower truncation needs log-side history beyond HW.
5. **[S3] fjall fallback.** If chosen, re-derive: durability tiers (`PersistMode::SyncAll` checkpoints), the digest iteration API, the fault-injection seam (fjall has no `StorageBackend` equivalent, so use failpoints around persist), and cache sizing. The `KvTxn` seam limits the blast radius.
6. **Are the ownership boundaries with Phase 3 for `ostro-types` accepted?** The planner should create the `ostro-types` command and envelope plan first in Wave 1 and tell Phase 3 about it.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | `cargo test` / **cargo-nextest 0.9.152** (install in Wave 0) + proptest 1.12 + proptest-state-machine 0.9 + fail 0.5.1 |
| Config file | none yet. Wave 0 adds `.config/nextest.toml` (a `crash` profile with long timeouts and `test-threads = 1` for failpoint binaries) |
| Quick run command | `cargo nextest run -p ostro-types -p ostro-log -p ostro-partition -p ostro-blob` (target < 30 s) |
| Failpoint suite | `cargo nextest run -p ostro-log -p ostro-partition --features failpoints --test fatal_io` |
| Crash loop (quick) | `OSTRO_CRASH_ITERS=50 cargo nextest run -p ostro-testkit --run-ignored only --test crash_loop` |
| Full suite command | `cargo nextest run --workspace --all-features && cargo test --doc --workspace && PROPTEST_CASES=1000 cargo nextest run -p ostro-partition --test rebuild_digest` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| STOR-01 | Envelope round trip; group commit shares fsyncs; contiguous offsets; on-disk records carry epoch/codec/crc | unit + proptest + integration | `cargo nextest run -p ostro-types --test envelope` ; `-p ostro-log --test append_group_commit` | ❌ Wave 0 |
| STOR-02 | Bit flip in a sealed segment → open Err; flip mid-last-segment (non-zero) → Err; envelope CRC bad with valid frame CRC → Err; corruption after open → read Err | integration (offline injection) | `cargo nextest run -p ostro-log --test corruption` | ❌ Wave 0 |
| STOR-03 | Cut at every byte of the last record → open OK, LEO = prev end; zero-sector tail → cut; appends continue; double reopen idempotent | integration (exhaustive small) | `cargo nextest run -p ostro-log --test torn_tail` | ❌ Wave 0 |
| STOR-04 | Generic conformance over `L: ReplicatedLog`: `log_start ≤ HW ≤ LEO`, stale-epoch append rejected, ISR={self}, `wait_committed` only after fsync | integration (generic) | `cargo nextest run -p ostro-log --test replicated_log_conformance` | ❌ Wave 0 |
| STOR-05 | With `ManualHwLog` holding HW: apply stops at HW, no acks beyond; `Immediate` cadence; crash between `None` commits → applied ≤ checkpoint → replay converges | integration | `cargo nextest run -p ostro-partition --test apply_hw` | ❌ Wave 0 |
| STOR-06 | Random cmd sequences × random chunking/checkpoints/crash points → live digest == rebuilt digest and rolling hash equal; also after kill -9 + delete `state.redb` | proptest + crash | `cargo nextest run -p ostro-partition --test rebuild_digest` ; crash loop asserts the digest | ❌ Wave 0 |
| STOR-07 | EIO on fsync, ENOSPC partial write, EIO on redb commit → waiters Err, health Failed, later appends Err, **never Ok** (`append_failure_is_never_reported_as_success`) | failpoint integration | `cargo nextest run -p ostro-log -p ostro-partition --features failpoints --test fatal_io` | ❌ Wave 0 |
| STOR-08 | Trim before Ready → `NotReady`; floor = min(ckpt, snapshot_floor); segments with offsets ≥ floor survive; startup order asserted | integration | `cargo nextest run -p ostro-partition --test trim_safety --test startup_order` | ❌ Wave 0 |
| STOR-09 | Golden xxh3 vectors; RunId string round trip; `partition()` correct; P mismatch at open → Err | unit | `cargo nextest run -p ostro-types --test ids` | ❌ Wave 0 |
| STOR-10 | Each restart's first term entry is `LeaderAnnounced{e+1}`; stale-epoch / lower-attempt / wrong-token `EventsAppended` rejected; zombie appending first still rejected | integration | `cargo nextest run -p ostro-partition --test fencing` | ❌ Wave 0 |
| STOR-11 | `submit` returns after apply; a read txn right after the ack sees the state; a rejected command returns `Rejected` | integration | `cargo nextest run -p ostro-partition --test ack_after_apply` | ❌ Wave 0 |
| STOR-12 | P=16, in-process router, random source/target restarts and duplicate deliveries → each message applied exactly once; outbox drains | proptest-state-machine | `cargo nextest run -p ostro-partition --test shuffler_exactly_once` | ❌ Wave 0 |
| STOR-13 | Window ≤ WINDOW_MAX; refill; overdue fire after restart; duplicate `TimerFired` = no-op; 100k timers → bounded window memory | integration | `cargo nextest run -p ostro-partition --test timers` | ❌ Wave 0 |
| STOR-14 | 300 KiB payload → `Blob` ref, file present before append; failpoint between blob write and append → orphan only; inline > 256 KiB rejected at apply; sha mismatch on read → Err | integration | `cargo nextest run -p ostro-blob -p ostro-partition --test blob_offload` | ❌ Wave 0 |

Manual-only: the LazyFS power-loss run (`scripts/lazyfs-crash.sh`, nightly/optional). It needs a FUSE mount and a C++ build.

### Sampling Rate
- **Per task commit:** the quick run command.
- **Per wave merge:** quick + failpoint suite + crash loop (50 iterations).
- **Phase gate:** the full suite + crash loop with `OSTRO_CRASH_ITERS=500` + `PROPTEST_CASES=1000`, all green before `/gsd:verify-work`.

### Wave 0 Gaps
- [ ] `cargo install cargo-nextest --locked` and `.config/nextest.toml` (profiles `default`, `crash`).
- [ ] `vendor/ostro-wal`: a `failpoints` feature (`fail/failpoints`) + patches P1/P2/P3 + VENDORED.md rows.
- [ ] `crates/ostro-testkit`: Linux-FS tempdir helper, `corrupt::{flip_bit, zero_sector, truncate_at}`, `ManualHwLog<L>`, `FaultyBackend` [S3], a `crash-target` bin, and the digest/rebuild helpers.
- [ ] `ostro-partition/clippy.toml` determinism lint (disallowed methods/types).
- [ ] Dev-deps: proptest, proptest-state-machine, tempfile, fail.
- [ ] Golden vector file `crates/ostro-types/tests/golden/routing.txt`.

## Sources

### Primary (HIGH confidence)
- Chronik source @ `08715f4867c9f40e409c68d8e212bd9479d620c0` (read-only clone in the scratchpad): `crates/chronik-wal/src/{record.rs (V2 layout, verify_checksum L396), group_commit.rs (CommitWait L344, append_enqueued L930, get_or_create_queue L1102, worker loop L1191–1295, commit_batch L1302–1470, durable_end_offset L1724, truncate_to L1762–1860), manager.rs (recover L99, read_from_bounded L300, read_segment L889, extract_offset_range L1114 — no callers), segment_index.rs (parse_record/Parsed::Invalid), truncate.rs (plan_segment_truncation)}`, `crates/chronik-raft/src/log_store.rs` (torn-tail-only rule L19, L600–720), `crates/chronik-server/src/replication/leader_epoch.rs` (`LeaderEpochCache` L40–190)
- Phase 1 research (`.planning/phases/01-foundation-vendoring-spikes/01-RESEARCH.md`): the vendoring manifest for `vendor/ostro-wal`, confirmation that `extract_offset_range` has no callers, S3 procedure, chrono ban
- docs.rs redb 4.6.0: `Durability` (None/Immediate text), `WriteTransaction` (`set_durability -> Result<(), SetDurabilityError>`, commit error semantics, 2PC, quick repair), `Builder` (default cache 1 GiB, default `ExclusiveWriter`, `create_with_backend`), `StorageBackend` trait
- redb CHANGELOG (master): 4.0–4.6 fixes (None-commit and repair fixes, `TransactionPoisoned`), 5.0 breaking notes
- docs.rs `fail` 0.5.1 (feature `failpoints`, `FailScenario`, global-registry caveat); docs.rs `proptest-state-machine` 0.9.0 trait signatures
- crates.io API (2026-11-18): fail 0.5.1, proptest 1.12.0, proptest-state-machine 0.9.0, crc32c 0.6.9, crc32fast 1.5.5, postcard 1.1.3, redb 4.6.0, fjall 3.1.13, tempfile 3.27.0, blake3 1.8.7, xxhash-rust 0.8.21, cargo-nextest 0.9.152, tokio 1.54.1

### Secondary (MEDIUM confidence)
- etcd `server/storage/wal/decoder.go` `isTornEntry` (zero-sector rule, last file only): https://github.com/etcd-io/etcd/blob/main/server/storage/wal/decoder.go
- LazyFS (VLDB 2024, Ramos/Azevedo/Kingsbury): https://www.vldb.org/pvldb/vol17/p3017-ramos.pdf ; Jepsen LazyFS docs: https://jepsen-io.github.io/jepsen/jepsen.lazyfs.html ; Jepsen NATS 2.12.1 analysis: https://jepsen.io/analyses/nats-2.12.1
- docs.rs crc32c (functions, SSE4.2 runtime detection)
- Project research (ARCHITECTURE.md G1–G10, PITFALLS.md 10–14), built on, not redone

### Tertiary (LOW confidence; validate during execution)
- Linux page-cache behaviour after an fsync error (pages marked clean while still cached); relevant to Pitfall 3's residual risk
- Rust std `sync_all` → `F_FULLFSYNC` on macOS (affects CI timing only)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH. Versions verified on crates.io today; redb API verified on docs.rs.
- Vendored-WAL findings: HIGH. Read directly from source with line references; consistent with Phase 1's compile-and-grep.
- Architecture (trait, envelope, apply, fencing, outbox, timers): MEDIUM-HIGH. It follows locked project research and prior art (Restate, Temporal, Kafka). Byte layouts are recommendations, not yet prototyped.
- Torn-tail rule and post-fsync-failure behaviour: MEDIUM. They depend on filesystem and kernel semantics. Mitigated by conservative rules and injection tests.
- Pitfalls: HIGH for W1–W13 (source-verified) and the redb cache default (docs.rs); MEDIUM for the macOS and LazyFS notes.

**Research date:** 2026-11-18
**Valid until:** 2026-12-18 (30 days; the stack is stable. Re-check if the S3 outcome is fjall or redb ships 5.0.)
