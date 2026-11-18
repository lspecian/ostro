# Architecture Research

**Domain:** Log-first durable execution engine (single Rust binary, Wasm-hosted workflows, partitioned journal, Raft metadata, follower-pull ISR replication)
**Researched:** 2026-11-18
**Confidence:** HIGH on the Chronik code findings (read from source at commit `08715f48`, 2026-11-18). MEDIUM on the Restate and Temporal comparisons (official docs plus community sources; internals not read from source).

---

## Verdict Up Front

The shape of the `docs/03` design is right. It is a Kafka/KRaft-style log, one single-writer leader per partition, a derived state store, and deterministic replay, which is the same shape as Restate's partition processors and Temporal's history shards. The design has **five structural gaps** that prior art handles and Ostro's spec does not. All five are cheap to fix on paper and expensive to retrofit:

1. **Leadership change is not a logged event.** Ostro's "new leader bumps attempts of in-flight runs" is soft state, but the fencing check runs against derived state that followers also hold. A zombie executor talking to the *new* leader passes the check. Fix: the new leader's first log entry is `LeaderAnnounced{epoch}` (Restate's epoch-bump record), and the apply-time fencing compares the attempt's epoch.
2. **No cross-partition messaging.** `start-child`, parent completion, signals to other runs, cancel propagation and `ScheduleFired` (schedule partition ≠ run partition) all cross partitions, even on a single node with P=16. Restate has a shuffle with sequence-number dedup for this. Temporal uses transfer tasks written in the same transaction (an outbox). Ostro needs an **outbox table + shuffler** in Phase 1.
3. **Fencing and validation must happen at apply time, not only on the leader before append.** The leader can pipeline several uncommitted appends, so `from_seq == run.next_seq` checked against *applied* state is wrong while earlier appends are in flight. Make validation part of deterministic apply, with an optional speculative pre-check on the leader.
4. **Snapshot install and trim coordination for replicas.** Once the log is trimmed, a new or lagging follower cannot fetch from offset 0. Restate requires snapshots for multi-node clusters for exactly this reason. Ostro needs a "fetch offset below log-start-offset, so install a snapshot" path, plus a trim rule. Neither appears in the spec.
5. **The RF=1 = cluster code path (D5) contradicts replication Option A.** Chronik's fetcher is a Kafka Fetch *client* that needs Kafka RecordBatch bytes and the Kafka Fetch *server* (`fetch_handler.rs` 3.7k lines + `produce_handler.rs` 6.1k lines + `chronik-protocol`). It cannot be the RF=1 path in Phase 1. If Phase 4 bolts it on, a second path appears, which is exactly the bug class Chronik's own roadmap warns about. **Recommendation: build Ostro's own `ReplicatedLog` abstraction in Phase 1** (HW, epochs, ISR={self}), and port Chronik's *pure* algorithms (epoch cache, batch planning, ISR math, failover planner) to a small tonic `Fetch` in Phase 4 (Option B).

The Chronik reuse claims in `docs/04` are **too optimistic in two places**: the WAL record format (V2 skips CRC verification on read, and the read/truncate paths are V2-only) and replication (not "nearly as is"). `chronik-raft`'s driver and log store are as good as claimed.

---

## Standard Architecture

### System Overview (revised: adds shuffler, log layer, snapshot path)

```
┌──────────────────────────────────────── ostro server node ──────────────────────────────────────────┐
│  INGRESS        ostro-api (axum + tonic, :7700)  ── route by run-id partition ──► local or forward   │
│                      │ propose (meta)                         │ submit command                       │
│  ────────────────────┼────────────────────────────────────────┼──────────────────────────────────── │
│  CONTROL        ostro-meta  (StateMachine on chronik-raft RaftDriver; partition map, epochs, ISR,     │
│                  modules, workflows, queues, schedules, security)                                    │
│                      │ watch: "I lead p7 at epoch 12, ISR {1,3}"                                     │
│  ────────────────────┼────────────────────────────────────────┼──────────────────────────────────── │
│  PARTITION      ┌────▼────────────────────────────────────────▼───────────────────────────────┐     │
│  (×P)           │ PartitionReplica                                                             │     │
│                 │  ostro-log:  Sequencer (leader) ─► chronik-wal GroupCommitWal ─► segments    │     │
│                 │              ReplicatedLog: LEO, HW, epoch cache, ISR (RF=1: ISR={self})     │     │
│                 │                       │ committed entries (≤ HW), in order                   │     │
│                 │  ostro-partition: StateMachine.apply(cmd) ─► redb txn ─► Vec<Action>         │     │
│                 │                       │ actions consumed ONLY if leader                      │     │
│                 │  Leader services: Dispatcher · TimerService · Shuffler(outbox) · Retention   │     │
│                 └──────▲──────────────────────────┬───────────────────────────▲────────────────┘     │
│                        │ Append / Done            │ Activate                  │ Fetch (followers)    │
│  ────────────────────────────────────────────────────────────────────────────────────────────────── │
│  EXECUTION      ostro-engine (Wasmtime host, replay cursor, module cache)  ◄── ActivationSink ──►    │
│                 ostro-bridge (HTTP/SQL/object/secrets/egress; Tokio)        local or gRPC worker     │
│  ────────────────────────────────────────────────────────────────────────────────────────────────── │
│  STORAGE        partitions/pNNNN/{wal/, state.redb} · meta/ (RaftLogStore) · blobs/ · cache/aot/    │
│                 Uploader ──► object store (sealed segments, state snapshots, meta snapshots, blobs) │
└──────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Component Responsibilities

| Component | Owns | Talks to | Implementation |
|---|---|---|---|
| **ostro-api** | HTTP/gRPC ingress, auth, request routing/forwarding, webhooks, UI assets | meta (proposals, reads), partition leaders (submit command, await commit), history reads | axum 0.8 + tonic 0.14 on one listener |
| **ostro-meta** | Cluster-wide low-volume config: membership, partition map (leader, **epoch**, ISR), modules, workflows, queues, schedules, tokens/secrets | chronik-raft driver; publishes a `watch` of partition assignments to the partition manager | `impl chronik_raft::StateMachine`; snapshot = whole-state bytes (fine at metadata scale) |
| **ostro-log** | Per-partition ordered, durable, replicated log: offset assignment, batch envelope (epoch, CRC, codec version), LEO/HW, leader-epoch cache, truncation, ISR/HW computation, follower fetch (Phase 4) | chronik-wal (storage); ostro-partition (delivers committed entries); peers (fetch) | Wrapper over `chronik_wal::WalManager`/`GroupCommitWal`; epoch cache ported from Chronik |
| **ostro-partition** | Deterministic state machine over redb; derived tables; snapshot/restore; emits `Action`s | ostro-log (input), leader services (actions), uploader (snapshots) | `apply_batch(offset, cmds) -> Vec<Action>`; one task per replica |
| **Dispatcher** (leader-only) | Ready queues, placement, concurrency/quotas, soft leases, attempt tokens | executor slots via `ActivationSink`; ostro-log (submits commands) | In-memory; rebuilt from `ready` table on leadership |
| **TimerService** (leader-only) | Lazy timer window (next N or next Δt from `timers` table), fires `TimerFired` | ostro-log | Hierarchical wheel over a **window**, not all timers |
| **Shuffler** (leader-only) | Delivers `outbox` entries to target partitions exactly once (dedupe by `(src_partition, outbox_seq)`), truncates outbox on ack | target partition leader (local call or forward) | Restate-style shuffle; Temporal-style transactional outbox |
| **ostro-engine** | Wasmtime component host, WASI virtualisation, replay/divergence detection, activation lifecycle, eviction | `ActivationSink` (append/done/secret/blob), ostro-bridge | Same crate embedded and in `ostro worker` |
| **ostro-bridge** | Effect execution with idempotency keys, secrets substitution, egress/SSRF guard | external systems; never touches the log directly (goes through the engine → sink) | Tokio; reqwest/hyper; sqlx-style connectors |
| **Uploader / Restore** | Bottomless: sealed segments, state snapshots, meta snapshots, blobs to S3 | ostro-log (sealed segment list), ostro-partition (snapshots), object store | `object_store` crate (see verdict below) |

---

## Prior-Art Comparison

### Restate (closest analogue)

| Restate concept | Ostro equivalent | Gap / lesson |
|---|---|---|
| **Bifrost**: a segmented virtual log per partition. The controller *seals* the active segment, finds the authoritative tail, and publishes a new segment with a metadata CAS. Single-node uses a local loglet, cluster a replicated loglet | chronik-wal segments + follower-pull ISR | Ostro couples log and processor in one node (the Kafka model). That is simpler and fine. Restate's seal-then-reconfigure is equivalent to Kafka "elect from ISR + truncate to epoch end". **Keep Ostro's model**, but note that Restate uses different loglet implementations for single-node and cluster. Ostro's D5 is stricter, and only holds if `ReplicatedLog` exists from Phase 1 |
| **Partition processor** (leader + followers apply the same log; RocksDB holds journals, idempotency, state, timer indices; "a derivative cache rebuildable from the log") | ostro-partition on redb | Matches. Adopt Restate's split: **apply is pure and identical on all replicas; side effects are `Action`s that only the leader executes** |
| **Epochs**: each leadership term and each *attempt* carries an increasing epoch. A new leader "appends an epoch-bump record"; superseded epochs are rejected | Soft attempt bump on leadership | **Gap G1.** Log `LeaderAnnounced{epoch}`; attempt tokens = `(leader_epoch, counter)` |
| **Leader reads the committed entry back, adds it to the journal, then acks the handler** | `AppendAck` "after commit" | Make it **after apply** on the leader so reads and dispatch see it. Validation happens at apply (**Gap G3**) |
| **Shuffle**: cross-partition events delivered from the origin partition's log to the destination exactly once, deduplicated by sequence number | none | **Gap G2.** Required for children, signals, cron, cancel |
| **Snapshots** to S3. A new or lagging processor restores the latest snapshot and replays the suffix. **Log trimmed only once the partition store is durably persisted**. Snapshots required for multi-node | "Bottomless" uploads only | **Gap G4.** Trim rule plus snapshot install for followers |
| **Metadata store** (embedded Raft) holds config, placement, epochs, segment descriptors. The data path "borrows consensus" through leader/epoch grants | ostro-meta on chronik-raft | Matches exactly |
| **Cluster controller** (heartbeats, failover, gossip) | partition failover on the Raft leader (Chronik RP-5) | Matches. Chronik notes one-way partitions look alive (`ROADMAP_REPLICATION.md`, RP-5 "Known limitation") |
| **Invoker**: leader opens a stream to the service and pushes the journal; retries/backoff; suspension | Dispatcher + ActivationSink + evict | Matches. Ostro hosts the code; Restate calls out to the user's service |

### Temporal

| Temporal concept | Ostro equivalent | Gap / lesson |
|---|---|---|
| **History shards** (fixed count at cluster creation), each owning event history + mutable state | Partitions (fixed P) with `events` + `runs` | Matches. Fixed partition count is a known operational wart in Temporal. Pick P generously (64 for cluster) |
| **Range ID**: every shard write is a conditional update on `range_id`, which fences a stale owner | leader epoch in batches | Equivalent. Ostro must also fence *executors* (G1), not only the log writer |
| **Transfer queue** (immediate tasks) and **timer queue** (delayed tasks), written **in the same transaction** as the state change (outbox). On shard move the new owner re-drives all unacked tasks | `ready`, `timers` tables derived at apply | Matches for local work. **Missing: the cross-shard part** (Temporal's transfer tasks to other shards and matching = G2) |
| **Matching service**: separate task-queue service, sync match, sticky queues | Dispatcher inside the partition leader, workers connect to all nodes; hot-wait = sticky | Ostro's merge is simpler and right for its scale. Cost: workers hold N streams (one per server node), fine at 3–5 nodes |
| **History size limits + continue-as-new** | 50k events cap, `continue_as_new` hint | Matches |
| **Activity heartbeats / timeouts** (schedule-to-start, start-to-close) | Soft leases, `lease_ttl` | Add schedule-to-start lag as a metric (queue lag already planned) |

### What Ostro's spec misses (consolidated)

| # | Gap | Why it matters | Fix (architecture) | Phase |
|---|---|---|---|---|
| G1 | Leadership/attempt fencing not in the log | Followers and the new leader can't fence a zombie executor deterministically | `LeaderAnnounced{epoch}` is the first entry of every leadership term. Attempt token `(epoch:u32, n:u32)` issued by the dispatcher. Apply accepts `EventsAppended` iff `token.epoch == partition.current_epoch && token >= run.highest_attempt && from_seq == run.next_seq`. RF=1 restart = new epoch, so the same code path covers it | 1 |
| G2 | No cross-partition messaging | Children, parent notify, signal-other-run, cancel cascade and cron start break on a 16-partition single node | `outbox` table written by apply; leader **Shuffler** submits `Inbound{src_partition, src_seq, msg}` to the target; target dedupes via `inbound_seen[src_partition] = max seq`; source truncates outbox on ack (`OutboxTruncated{upto}`) | 1 (children may slip to 2, but cron needs it) |
| G3 | Validation against applied state with pipelined appends | False rejects/accepts under load | All validity rules in deterministic apply (rejections become no-ops with a recorded reason). Leader keeps a speculative "pending next_seq" for fast reject only. `AppendAck` after apply | 1 |
| G4 | Trim and follower bootstrap | Followers below log-start-offset are stuck forever | Trim below `min(last durable local snapshot offset, last uploaded snapshot offset if bottomless, min follower fetch offset over ISR)`. Fetch below log start returns `SnapshotRequired`, and the follower streams `state.redb` snapshot + meta (offset, epoch history), then fetches the tail | 2 (trim), 4 (install) |
| G5 | Replication Option A cannot be the RF=1 path | Two paths: the bug class Chronik hit twice | Option B from the start: `ReplicatedLog` trait in Phase 1 (HW = durable LEO when ISR={self}); tonic `Fetch` in Phase 4 | 1 (trait), 4 (network) |
| G6 | Timer wheel loads all timers | Millions of 24h sleeps blow memory and failover time | Lazy window from `timers` (range scan by `due_at`), refill when the window drains. `TimerFired` apply is idempotent (ignore if timer gone) because a new leader may re-fire | 1 |
| G7 | Leader-decided non-determinism | Apply must not read clocks | Every command carries the leader's timestamps/choices (e.g. `RunStarted.accepted_at`, `RunsExpired{runs[]}`: already done for retention). Lint: apply takes no `now()` | 1 |
| G8 | redb commit = fsync per apply chunk | Doubles fsyncs (WAL + redb) and caps throughput | `Durability::None` for routine apply txns, `Durability::Immediate` every N entries/ms, `applied_offset` written in the same txn. On crash redb rolls back to the last immediate commit and the log tail replays (redb 4.6 has exactly `None`/`Immediate`) | 1 (Spike S3) |
| G9 | Read-your-writes on history/status | Reading a follower or a pre-apply state returns stale data | Reads go to the leader by default, or wait for `applied_offset >= token` (return the commit offset from writes) | 1 |
| G10 | Dedupe table retention | `dedupe` grows forever | Dedupe entries expire with run retention; document the window (Restate and Temporal both bound it) | 2 |

---

## Chronik Reuse: Verdict per Crate

Evidence from the clone at `08715f4867c9f40e409c68d8e212bd9479d620c0` (2026-11-18).

### `chronik-raft`: **USE `driver` + `log_store` AS-IS; REPLACE `net` transport; IGNORE legacy modules.** HIGH confidence.

- **Good APIs (as claimed):** `crates/chronik-raft/src/driver.rs`
  - `trait StateMachine: Send + 'static { fn apply(&mut self, index, term, command: &[u8]) -> Vec<u8>; fn snapshot(&self) -> Vec<u8>; fn restore(&mut self, index, data: &[u8]) -> Result<(), String>; fn on_conf_change(..) {} }`
  - `trait Outbox: Send + 'static { fn send(&mut self, messages: Vec<raft::eraftpb::Message>); }`: **this is the transport seam**
  - `RaftDriver::start(dir, DriverConfig, initial_voters: &[u64], sm, outbox) -> io::Result<RaftDriver>`; `RaftHandle::{propose, propose_conf_change, step, wait_applied, wait_for_leader, status, subscribe, sample_live, campaign}`; `ProposeError::{NotLeader{leader}, Dropped, OutcomeUnknown, Stopped}`
  - One OS thread owns `RawNode`, event-driven (no polling), `check_quorum` + `pre_vote` on, bootstrap snapshot at index 1 from `initial_voters`, snapshot every `snapshot_every` entries.
  - `log_store.rs` (`RaftLogStore`): `state`/`snapshot` via tmp+fsync+rename, `log/<index>.seg` with `[len u32][crc32 u32][Entry]`, torn-tail-only tolerance, all post-snapshot entries held **in memory** (fine for metadata, unsuitable for anything large).
- **Coupling found:**
  - `Cargo.toml` declares `chronik-common` and `chronik-monitoring`, but **no source file uses them** (grep of `src/` finds no `chronik_common`/`chronik_monitoring`). These are dead deps that still get compiled, along with prometheus, opentelemetry 0.22, axum 0.6, hyper and tower-http.
  - **tonic 0.11 / prost 0.12** (workspace pins) plus `build.rs` compiling `proto/raft_rpc.proto` and `proto/catalog_rpc.proto` with `tonic-build`. The **`protoc` binary is required at build time** (prost-build 0.12 does not bundle it). Current tonic is 0.14.9 and prost 0.14.6. Ostro would carry two tonic stacks, and the Raft service (`net::CatalogRaftServer::into_service()`) **cannot be mounted on Ostro's tonic 0.14 router**: it needs a separate port, or Ostro writes its own transport.
  - **`[patch.crates-io] raft = { path = "crates/raft" }, raft-proto = …` lives in Chronik's root `Cargo.toml`. Patches do not propagate through git dependencies.** Ostro's workspace must repeat `[patch.crates-io] raft = { git = "…/chronik-stream", rev = "…" }` and `raft-proto = { git = …, rev = … }`, or it resolves crates.io `raft 0.7.0` (last released 2023-03, prost 0.11 codec) and fails to build or links mismatched types.
  - Legacy modules `storage.rs` ("STUB"), `transport/*`, `rpc.rs`, `config.rs` ("STUB") and `prost_bridge` are superseded by `driver`/`log_store`/`net`. Don't use them.
- **Effort:** git dep + patch section: 0.5 day. Own `Outbox` over Ostro's tonic version + inbound `step()` + forward-propose: ~300–400 LOC, 2–3 days (mirror `net.rs`: per-peer queue task, `report_unreachable`, `report_snapshot_failed`). **Recommended extraction for `chronik-core`:** split `chronik-raft-core` (driver + log_store; deps: raft, prost 0.12, tokio, slog, crc32fast) from `chronik-raft-grpc` (net + protos). About 1 day, and it removes protoc + tonic 0.11 from Ostro.

### `chronik-wal`: **ADAPT (use `WalManager`/`GroupCommitWal`, own the record envelope, patch metrics deps).** HIGH confidence.

- **Good APIs:** `crates/chronik-wal/src/group_commit.rs`, `manager.rs`
  - `GroupCommitWal::append_enqueued(topic: String, partition: i32, record: WalRecord, acks: i16) -> Result<CommitWait>`; `CommitWait::wait()` resolves on fsync. FIFO: *return order = file order*, so the caller **reserves offsets and enqueues under one lock**, then waits outside it (this is Ostro's Sequencer).
  - `durable_end_offset(topic, partition) -> Option<i64>`, `read_tail(..)` (durable-gated tail cache), `truncate_to(topic, partition, target) -> TruncateOutcome` (quiesces writer, bumps truncation epoch, cuts files, resets `committed_through`), `get_sealed_segments`, `seal_stale_segments`, `delete_segment_file`.
  - `WalManager::{append_canonical_enqueued, read_from_bounded, truncate_to, truncate_before, delete_records_before, durable_end_offset, get_sealed_segments_with_size}`.
  - `segment_index.rs` `PartitionIndex::read_from_bounded(offset, max_records, max_bytes)`: sparse index, seeks, contiguous-run ordering fix (RP-9).
  - `truncate.rs`: pure `plan_segment_truncation(&[u8], target) -> SegmentVerdict`, unit-testable.
  - Layout: `base_dir/<topic>/<partition>/wal_<partition>_<segment_id>.log`. Ostro uses topic `"p"` and partition = Ostro partition id.
- **Record-format findings (correct `docs/04` §3.1):**
  - `WalRecord::V1` has key/value/headers and per-record CRC, but **the partition read path and truncation are V2-only** (`truncate.rs`: "V1 records are deliberately not handled: the partition WAL read path is V2-only"). So `docs/04`'s "encode as `value` with key = run id in V2 batches" mixes the two formats. **V2 it is**, with Ostro's bytes in `canonical_data`, `topic`/`partition`, and `base_offset`/`last_offset`/`record_count` in the trailer.
  - **`WalRecord::verify_checksum()` returns `Ok(())` for V2 without checking** (`record.rs` ~L396: relies on the Kafka CRC-32C inside `canonical_data`). The CRC *is computed* on write (`calculate_checksum` covers `canonical_data`), and `calculate_checksum()`/`get_crc32()` are public. **Ostro must verify V2 CRCs itself on every read** (or patch the WAL to verify when a flag bit is set), and should also put its own CRC in the envelope.
  - **There is no leader-epoch field in WAL framing.** Chronik's epoch lives inside the Kafka batch header (`replication/leader_epoch.rs` `stamp_leader_epoch(wire, epoch)`/`read_leader_epoch(wire)`). Ostro's batch envelope inside `canonical_data` must carry `leader_epoch`, `codec_version`, `crc32c`, and the command list.
  - `manager.rs::extract_offset_range()` bincode-decodes `canonical_data` as a Kafka `CanonicalRecord`. Ostro must always set the V2 trailer offsets so that path is never needed. Verify in Spike S1 that no recovery path calls it for records with populated trailers.
  - `durable_end_offset` is `None` until something is committed in this process, so **at startup Ostro must recover LEO by scanning** (last segment via `PartitionIndex`). Chronik's leader-epoch cache is likewise rebuilt by a WAL scan (`warm_up_leader_epochs`).
- **Coupling:** `chronik_monitoring::MetricsRecorder::record_wal_batch` (group_commit.rs:30) and `chronik_common::metrics::Metrics::get()` (fsync.rs:15) are the only uses. They pull prometheus, opentelemetry 0.22, opentelemetry-otlp 0.15, axum 0.6, hyper and tower-http. Default feature `async-io` enables the io_uring path, which Chronik itself measured as **33% slower** (group_commit.rs config doc). `tokio-uring 0.4` is a **non-optional Linux dependency**.
- **Effort:** use with `default-features = false`: 0.5 day. Envelope + CRC verify + Sequencer + LEO recovery: 1–1.5 weeks. Optional upstream patch: feature-gate metrics (`metrics` feature) and make `tokio-uring` optional (~1 day, benefits Chronik too). Generic `V3` envelope: defer, it isn't needed if Ostro owns the payload.

### Replication (`chronik-server/src/replication/*`, `isr_*.rs`, `partition_failover.rs`, `assignment_repair.rs`): **EXTRACT ALGORITHMS, REWRITE TRANSPORT (Option B).** HIGH confidence on coupling, MEDIUM on effort.

- **Size:** `replication/` 5,490 lines (fetcher.rs 2,237, connection.rs 874, apply.rs 799, protocol.rs 587, leader_epoch.rs 956) + `isr_tracker.rs` 1,213 + `isr_ack_tracker.rs` 518 + `isr_publisher.rs` 391 + `partition_failover.rs` 1,038 + `assignment_repair.rs` 396 = **9,046 lines**.
- **Coupling (from imports):**
  - `replica_fetcher/connection.rs` → `chronik_protocol::{parser::Decoder, offset_for_leader_epoch_types}`, `crate::inter_broker::secret`
  - `replica_fetcher/apply.rs` → `chronik_storage::canonical_record::CanonicalRecord`, `crate::produce_handler::ProduceHandler`, `MetadataStore`. It appends **Kafka RecordBatch wire bytes** byte-identically (CRC-32C)
  - `replica_fetcher/fetcher.rs` → `MetadataStore`, `crate::metadata_events::MetadataEventBus`, `ProduceHandler`
  - `protocol.rs`: hand-rolled Kafka Fetch v11 *client* codec, round-trip tested against the server's `ProtocolHandler`. **The leader side is Chronik's Kafka Fetch handler (`fetch_handler.rs` 3,724 lines) and HW logic in `produce_handler.rs` (6,118 lines).**
  - `isr_publisher.rs`, `partition_failover.rs` → `chronik_common::metadata::{PartitionAssignment, MetadataStore, catalog::IsrUpdate}`, `crate::cluster_catalog::ClusterCatalog`
- **Conclusion:** Option A ("reuse the fetcher nearly as is over Kafka Fetch") actually means embedding a Kafka broker's fetch/produce path and storing Ostro commands as Kafka RecordBatches. It also cannot run at RF=1 in Phase 1 (G5). **Reject A.**
- **Directly portable (pure, well-tested logic, copy with attribution into `ostro-log::replication`):**
  - `leader_epoch.rs`: `LeaderEpochCache { assign, epoch_for_offset, end_offset_for_epoch, truncate_from_end, truncate_from_start }` and `LeaderEpochStore` (checkpointing). Kafka `LeaderEpochFileCache` semantics. ~400 LOC without the Kafka tests
  - `apply.rs::plan_batches(bytes, expected_offset) -> Result<Vec<BatchAction>, ApplyRefusal>`: the *pattern* (pure duplicate/gap/straddle classification). Re-implement over Ostro's envelope
  - `isr_tracker.rs` (lag-by-time and optional lag-by-entries, liveness), `isr_publisher::in_sync_replicas(..)`. **Drop** the "unknown → full replica set" fallback (isr_publisher.rs doc: "Callers treat `None` as unknown and fall back to the full replica set"). That is a residue of "ISR is fiction"
  - `isr_ack_tracker.rs`: watermark (monotonic) ack matching, not exact-offset
  - `partition_failover.rs::plan_failover(assignments, live) -> FailoverPlan` and `plan_preferred_leaders`: pure planners
  - `FollowerState` trait shape (`leader_epochs`, `local_log_end` = **LEO not HW**, `reset_after_truncation`)
- **Not needed:** `assignment_repair.rs` repairs Kafka topics with unassigned partitions. Ostro creates all P partitions with assignments at `cluster init`. Port the "holder of data must be leader" rule as an invariant check only.
- **Known open issues to design around (ROADMAP_REPLICATION.md):** the leader does not fence stale `current_leader_epoch` on Fetch ("parses it but never validates it"). Ostro must validate from day one. One-way partitions look alive to Raft `recent_active` (RP-5). Truncation is local to the WAL, so already-uploaded segments are not retracted: **gate segment upload on "committed (≤ HW)" and on leadership.**
- **Effort:** `ostro-log` replication (tonic `Fetch{partition, fetch_offset, current_epoch, replica_id, max_bytes, max_wait}` + `OffsetForLeaderEpoch` + leader HW/ISR + follower loop with one task per leader + truncation via `GroupCommitWal::truncate_to`): **4–6 weeks** including porting the conformance approach. Comparable to Option A's real cost, with no Kafka dependency and a single code path.

### `chronik-storage/src/object_store`: **DON'T EXTRACT; use the `object_store` crate (or current `opendal`).** MEDIUM confidence.

- 5,272 lines: `ObjectStore` trait (`put/get/get_range/delete/list/head/copy/multipart/presign_*`), backends S3 (**`aws-sdk-s3` 1.x**), GCS/Azure (**opendal 0.45**), local; `chronik_integration.rs` is Chronik-specific.
- Coupling is small (only `chronik_common::{Error, Result}` in `storage.rs`/`errors.rs`), but the crate it lives in pulls tantivy, columnar, embeddings, protocol and the WAL, so extraction is required to use it at all. The S3 backend's AWS SDK brings a large dependency tree and, by default, `aws-lc-rs`, which needs a C toolchain/cmake. That cuts against the single static binary (musl) constraint. Verify in the stack spike.
- The arrow-rs **`object_store` 0.14.2** (pure Rust, rustls, S3/GCS/Azure/local, multipart, used by DataFusion/Delta/Iceberg) or **opendal 0.59.5** cover Ostro's needs: put sealed segment, put snapshot, list, get, presign for the `object-store` effect. ~200 LOC adapter vs ~1 week extraction. **Verdict: rewrite thin adapter.**

### `chronik-common` / `chronik-monitoring`: **AVOID as runtime deps; transitively tolerated only until the metrics feature-gate lands.**

- `chronik-common`: prometheus, bincode 1.3, metadata traits, partition assignment types. `chronik-monitoring`: opentelemetry 0.22 / otlp 0.15 / axum 0.6 / hyper. These are older majors than Ostro will use (axum 0.8.9, current OTel). They don't break the build (cargo allows multiple majors) but they duplicate HTTP stacks in the binary and confuse metrics registries. Ostro should register its own metrics, not Chronik's global `Metrics::get()`.

### Git-dependency consumption summary

```toml
# ostro/Cargo.toml (workspace)
[workspace.dependencies]
chronik-raft = { git = "https://github.com/lspecian/chronik-stream", rev = "08715f4867c9f40e409c68d8e212bd9479d620c0" }
chronik-wal  = { git = "https://github.com/lspecian/chronik-stream", rev = "08715f4867c9f40e409c68d8e212bd9479d620c0", default-features = false }

[patch.crates-io]   # REQUIRED: Chronik's own [patch] does not propagate through git deps
raft       = { git = "https://github.com/lspecian/chronik-stream", rev = "08715f4867c9f40e409c68d8e212bd9479d620c0" }
raft-proto = { git = "https://github.com/lspecian/chronik-stream", rev = "08715f4867c9f40e409c68d8e212bd9479d620c0" }
```

Build needs `protoc` (raft-proto `build.rs` uses prost-build 0.12, chronik-raft `build.rs` uses tonic-build 0.11). Workspace-inherited deps (`{ workspace = true }`) in Chronik members resolve against Chronik's root, which cargo supports for git deps. Spike S1 answers itself: **yes, no Kafka/server crates are pulled**, at the cost of chronik-common + chronik-monitoring + tonic 0.11 + tokio-uring + protoc. Plan the `chronik-core` split (raft-core without gRPC, wal with a `metrics` feature) as a ~3-day task early in Phase 1, not as a fallback.

---

## Recommended Project Structure

```
ostro/
├── wit/                          # ostro:runtime@0.1.0 (source of truth)
├── crates/
│   ├── ostro-types/              # ids (RunId w/ partition), Command, Event, AttemptToken, codec + envelope (epoch, crc)
│   ├── ostro-log/                # Sequencer, ReplicatedLog trait, RF=1 impl, epoch cache, (P4) fetch server/client, ISR/HW
│   ├── ostro-partition/          # StateMachine (redb), apply → Actions, snapshot/restore, dispatcher, timers, shuffler
│   ├── ostro-meta/               # chronik-raft StateMachine + Outbox transport on Ostro's tonic
│   ├── ostro-engine/             # Wasmtime host, WASI virt, replay cursor, module cache, executor loop
│   ├── ostro-bridge/             # effects: http, sql, object, secrets, egress
│   ├── ostro-proto/              # tonic protos: worker Execute stream, cluster Fetch, raft transport, admin
│   ├── ostro-api/                # axum/tonic ingress, routing/forwarding, auth, webhooks, UI
│   ├── ostro-blob/               # content-addressed blobs + object-store adapter + uploader/restore
│   ├── ostro/                    # binary: dev | server | worker | cluster | client cmds
│   └── ostro-cli/                # build pipeline (cargo / componentize-py / jco)
├── conformance/{sdk,replication,crash}/
└── sdks/{rust,python,typescript}/
```

### Structure Rationale

- **ostro-types is separate** because the log, the partition, the engine (worker side) and the protos all share `Command`/`Event`/`AttemptToken`. The worker binary path must not depend on redb or chronik-wal.
- **ostro-log is split from ostro-partition** (`docs/03` merges them) so the `ReplicatedLog` boundary is a crate boundary: RF=1 and the cluster implementation sit behind one trait, and the conformance suite targets that crate.
- **The Dispatcher, timers and shuffler live in ostro-partition**, because they consume `Action`s from apply and rebuild from partition tables on leadership.
- **ostro-proto is one crate** so a single tonic/prost version serves worker, cluster and Raft traffic on one listener.

---

## Architectural Patterns

### Pattern 1: Pure apply, leader-only actions

**What:** `apply` mutates the store and returns `Action`s (`EnqueueReady`, `ScheduleTimer`, `CancelTimer`, `NotifyAppender{run, seq}`, `ShuffleOut{...}`, `RunTerminated`). Every replica runs apply. Only the leader executes actions.
**When:** Always. This is how RF=1 and cluster share one path, and how followers stay hot standbys.
**Trade-offs:** The `Action` list must be complete. On leadership the leader rebuilds the same in-memory structures from tables (`ready`, `timers`, `outbox`), so actions are an optimisation of "rescan", not a source of truth.

```rust
pub trait PartitionStateMachine {
    /// Deterministic. No clocks, no randomness, no I/O besides the redb txn.
    fn apply_batch(&mut self, txn: &mut WriteTxn, first_offset: u64, cmds: &[Command]) -> Result<Vec<Action>>;
}
```

### Pattern 2: Epoch-scoped attempt tokens (fixes G1)

**What:** The new leader appends `LeaderAnnounced{epoch}` before serving. The dispatcher issues `AttemptToken{epoch, n}`. Apply rejects `EventsAppended` whose token epoch is not the current partition epoch, or whose token is lower than the run's highest attempt, or whose `from_seq` is wrong.
**When:** Phase 1, because a single-node restart is also a new epoch.
**Trade-offs:** One log record per leadership change. It needs no per-run attempt bump writes.

### Pattern 3: Sequencer under one lock, fsync outside it

**What:** The leader assigns `[base, last]` offsets and calls `GroupCommitWal::append_enqueued` under a per-partition mutex, then awaits `CommitWait` without the lock (documented contract in `group_commit.rs`). Commit = HW advance: RF=1 HW = durable end, cluster HW = min LEO over ISR.
**Trade-offs:** Per-partition throughput is bounded by group commit, which is fine with P≥16.

### Pattern 4: Transactional outbox + deduplicating shuffle (fixes G2)

**What:** Apply writes `outbox[(seq)] = msg` in the same redb txn. The leader's shuffler submits `Inbound{src, seq, msg}` to the target leader. Target apply ignores `seq <= inbound_seen[src]`. On target commit the source appends `OutboxTruncated{upto}`.
**When:** Any interaction between runs in different partitions. On a single node it's an in-process call, same code.

### Pattern 5: Durability tiers (fixes G8)

**What:** The WAL is the only fsync-per-commit file. redb applies with `Durability::None`, then `Immediate` every N entries / T ms, with `applied_offset` in-txn. Snapshots are taken from a consistent read txn.
**Trade-offs:** After a crash, recovery replays more of the log. The bound is set by N/T.

---

## Data Flow

### Request flow: start run (direction explicit)

```
Client ─POST /runs─► ostro-api ─(partition = hash(wf, idem) or hash(uuid))─► [forward if not leader]
   ─► Partition leader Sequencer ─append RunStarted{accepted_at}─► chronik-wal (group commit, fsync)
   ─► ReplicatedLog: HW ≥ offset? (RF=1: durable end; cluster: followers fetched)
   ─► StateMachine.apply (redb: runs, dedupe, ready) ─► Actions{EnqueueReady}
   ─► ostro-api ◄─ 202 {run_id, commit_offset}          (ack AFTER apply on leader)
   ─► Dispatcher ─Activate{run, AttemptToken, journal page}─► ActivationSink (embedded | worker gRPC)
```

### Activation flow

```
Executor ─Append{run, token, from_seq, events}─► leader Sequencer ─► WAL ─► HW ─► apply (validate token/from_seq)
         ◄─ AppendAck{committed_seq} | Reject{fenced|seq_mismatch} ───────────────────┘
effect_scheduled committed ─► bridge performs I/O (idempotency key = run‖op‖seq) ─► Append{effect_completed, wait_resolved}
long wait ─► evict ─► Done{evicted}; TimerService/inbound signal/child completion ─► EnqueueReady ─► re-Activate ─► replay
```

### Cross-partition flow (new)

```
p3 apply(EventsAppended{child_started}) ─► outbox[p3, 41] ─► Shuffler(p3 leader) ─► p9 leader: Inbound{src=p3, seq=41, StartChild}
p9 apply: dedupe(src,seq) ─► RunStarted(child) ─► … child terminates ─► outbox[p9, k] ─► p3 Inbound{ChildCompleted} ─► parent ready
```

### Recovery and leadership flow

```
Start: open WAL ─► scan LEO + rebuild epoch cache ─► open redb (applied_offset) ─► [restore snapshot if missing]
     ─► replay (applied_offset, LEO] through apply (actions discarded)
     ─► meta says "leader of p at epoch e" ─► truncate-to-epoch-end check (cluster) ─► append LeaderAnnounced{e}
     ─► wait apply ≥ that offset ─► rebuild ready/timer window/outbox ─► start Dispatcher/Timer/Shuffler
Follower: fetch(offset, epoch) ─► [diverged? OffsetForLeaderEpoch ─► truncate_to] ─► append verbatim ─► apply ≤ HW
         [offset < log_start ─► SnapshotRequired ─► install snapshot ─► resume]
```

### Key Data Flows

1. **Commands only flow inward through the Sequencer.** API, executor, timer, shuffler and retention all *submit*. Nothing writes redb except apply.
2. **State flows outward only via apply → actions (leader) or reads (any replica, gated on `applied_offset`).**
3. **Metadata flows from Raft to partitions as a watch** (assignment, epoch, ISR). Partitions publish measured ISR back to meta (leader only), and only the Raft leader writes assignments (Chronik RP-7).

---

## Suggested Build Order

Dependency graph (A → B means B needs A):

```
ostro-types ─┬─► ostro-log (RF=1 ReplicatedLog on chronik-wal) ─► ostro-partition (apply, snapshot, recovery)
             │                                                   └─► Dispatcher/Timers ─► Shuffler (outbox)
             ├─► ostro-engine (against in-memory ActivationSink + fake log) ─────────────┐
             └─► ostro-meta (chronik-raft, single voter, own Outbox) ─┐                    │
                                                                     └─► ostro-api + binary (dev/server) ◄─┘
   then: ostro-bridge (http/secrets) ─► ostro-proto worker stream ─► uploader/snapshots (bottomless)
   then: SDKs/CLI/UI
   last: cluster = conformance suite ─► Fetch server/client ─► ISR/HW ─► epochs+truncation ─► failover via meta ─► snapshot install
```

| Step | Build | Depends on | Why this order |
|---|---|---|---|
| 0 | **Spikes:** `chronik-core` split (raft-core w/o gRPC; wal metrics feature), redb Durability tiers throughput (S3), componentize-py (S2) | — | Removes the protoc/tonic-0.11/OTel-0.22 baggage before anything depends on it |
| 1 | `ostro-types`: ids, `Command`, `Event`, `AttemptToken`, envelope `{epoch, codec_ver, crc32c, cmds}` | — | Everything shares it. The envelope must carry the epoch from day one, otherwise logs written in Phase 1 can't be replicated in Phase 4 without a migration |
| 2 | `ostro-log` RF=1: Sequencer, CRC-verified reads, LEO recovery, epoch cache (ported), `ReplicatedLog` trait with HW watch, truncation API | 1, chronik-wal | D5: cluster later implements the same trait. Crash tests (kill -9 mid-group-commit) start here |
| 3 | `ostro-partition`: apply (with validation), redb tables incl. `outbox`/`inbound_seen`, `LeaderAnnounced`, snapshot/restore, delete-and-rebuild hash test | 2 | Proves the log-first invariant before any executor exists |
| 4 | Dispatcher + lazy TimerService + Shuffler | 3 | Leader-only consumers of actions |
| 4′ | `ostro-engine` (parallel with 2–4) against a fake `ActivationSink` | 1, wit | Largest independent risk (Wasmtime/replay). Don't serialize it behind storage |
| 4″ | `ostro-meta` single-voter (parallel) | chronik-raft | Small: modules/workflows/queues. Write the own `Outbox` transport now, even unused at 1 voter |
| 5 | `ostro-api` + `ostro` binary (`dev`, `server`, `deploy`, `run`) | 3, 4, 4′, 4″ | First end-to-end slice. Phase 1 exit criteria |
| 6 | `ostro-bridge`, worker gRPC (`ActivationSink` over `ostro-proto`), uploader + snapshots + restore + trim rule | 5 | Phase 2. Trim rule needed before the log grows unbounded |
| 7 | SDKs, CLI, UI | 5 (WIT frozen) | Phase 3 |
| 8 | Cluster: replication conformance suite first, then Fetch + ISR/HW + epoch truncation + failover planner + snapshot install + multi-voter meta | 2, 3, 6 (snapshots) | Phase 4. Only adds a network transport and multi-replica ISR behind the existing trait |

**Phase implications for the roadmap:**
- Move G1 (epoch record), G2 (outbox/shuffle), G3 (apply-time validation), G6 (lazy timers) and the `ReplicatedLog` trait **into Phase 1**. They shape the command set and on-disk format.
- Replace Phase 4's "Option A: reuse Chronik's fetcher" with "port Chronik's algorithms behind Ostro's `ReplicatedLog`". The estimate stays at roughly 12 weeks, but the risk profile improves (no Kafka codec, one path).
- Snapshot *install* for followers depends on Phase 2's snapshot format. Design the snapshot format once (for S3 restore and follower install).

---

## Scaling Considerations

| Scale | Architecture adjustments |
|---|---|
| Single node, ≤5k steps/s | P=16, one group commit per partition, redb `None` + periodic `Immediate`. Embedded executor only |
| Single node, 10–50k steps/s | Batch executor appends (multiple runs per WAL record), apply chunking, raise P. Watch redb write amplification (fjall fallback per S3) |
| 3–5 nodes | P=64, leader balancing via `plan_preferred_leaders`, dedicated replication address (Chronik RP-10: +31% throughput), followers apply hot |

### Scaling Priorities

1. **First bottleneck: fsync count.** Two fsyncs per command (WAL + redb) halves throughput. Durability tiers fix it.
2. **Second: replay cost for long journals on eviction.** Hot-wait plus the journal cap, and stream journal pages instead of whole journals.
3. **Third: timer memory and failover time.** The lazy timer window fixes it.

---

## Anti-Patterns

### Anti-Pattern 1: Leader-side soft state used for fencing

**What people do:** Bump attempts in leader memory on failover and check appends against it.
**Why it's wrong:** Followers and restarted nodes can't reproduce it, so apply diverges or zombies get through.
**Do this instead:** Log `LeaderAnnounced{epoch}` and validate in apply.

### Anti-Pattern 2: A second replication path for "real" clusters

**What people do:** Ship RF=1 as a plain local log and add Kafka-Fetch replication later.
**Why it's wrong:** Chronik lost user-data replication for ~9 months to exactly this ("wired for the internal topic, silently not for user data").
**Do this instead:** One `ReplicatedLog` trait from Phase 1. Conformance tests run against RF=1 and RF=3.

### Anti-Pattern 3: Trusting WAL V2 integrity

**What people do:** Assume `chronik-wal` verifies checksums.
**Why it's wrong:** V2 verification is skipped (`record.rs` `verify_checksum`), so bit rot in an Ostro payload would be applied silently.
**Do this instead:** Verify `calculate_checksum() == get_crc32()` plus an envelope CRC on every read and fetch.

### Anti-Pattern 4: ISR or status from assignment

**What people do:** Report replicas as in sync because they are assigned (or fall back to the full set when unknown).
**Why it's wrong:** "ISR is fiction" (Chronik), and unclean election destroyed acked writes.
**Do this instead:** ISR only from measured LEOs. Unknown means not in sync. Elect only from the published ISR.

### Anti-Pattern 5: Uploading uncommitted segments

**What people do:** Upload sealed segments from any replica.
**Why it's wrong:** Truncation doesn't retract uploaded objects (Chronik "Known hazard: truncation is local to the WAL").
**Do this instead:** Upload only from the leader, only below HW, keyed by `(partition, epoch, base_offset)`.

---

## Integration Points

### External Services

| Service | Integration pattern | Notes |
|---|---|---|
| S3/GCS/Azure | `object_store` crate adapter | Static-binary friendly. Avoid aws-sdk default TLS (aws-lc C build) |
| User HTTP/SQL | ostro-bridge effects with idempotency keys | Only after `effect_scheduled` is committed |
| KEDA/Prometheus | `/metrics`, `/v1/queues/{q}/demand` | Ostro-owned registry, not Chronik's global |

### Internal Boundaries

| Boundary | Communication | Notes |
|---|---|---|
| api ↔ partition | in-process `submit(cmd) -> CommitOffset` / tonic forward | Return commit offset for read-your-writes |
| partition ↔ log | `ReplicatedLog` trait (append, committed stream, HW watch, truncate) | RF=1 and cluster implement it |
| partition ↔ engine | `ActivationSink` (in-process or gRPC) | Token-fenced |
| partition ↔ partition | Shuffler `Inbound{src, seq}` | Exactly-once via dedupe |
| meta ↔ partition | `watch<PartitionAssignments>`; ISR publish (leader) | Only the Raft leader writes assignments |
| meta ↔ peers | chronik-raft `Outbox` + `RaftHandle::step` over Ostro tonic | Not Chronik's `CatalogRaftServer` (tonic 0.11) |

---

## Sources

- Chronik Stream source, commit `08715f4867c9f40e409c68d8e212bd9479d620c0`: `crates/chronik-raft/src/{driver.rs,log_store.rs,net.rs,lib.rs,storage.rs}`, `crates/chronik-raft/{Cargo.toml,build.rs,proto/}`, `crates/chronik-wal/src/{group_commit.rs,record.rs,truncate.rs,segment_index.rs,manager.rs,lib.rs}`, `crates/chronik-wal/Cargo.toml`, `crates/chronik-server/src/replication/**`, `isr_tracker.rs`, `isr_ack_tracker.rs`, `isr_publisher.rs`, `partition_failover.rs`, `assignment_repair.rs`, `crates/chronik-storage/src/object_store/**`, root `Cargo.toml` (`[patch.crates-io]`), `docs/ROADMAP_REPLICATION.md`. HIGH
- [Restate architecture reference](https://docs.restate.dev/references/architecture): Bifrost sealing/reconfiguration, partition processor, epochs, shuffler, snapshots, trim. MEDIUM-HIGH (official)
- [Restate snapshots](https://docs.restate.dev/server/snapshots): snapshots required for multi-node, restore + suffix replay. MEDIUM-HIGH
- [Restate 1.2 announcement](https://restate.dev/blog/announcing-restate-1.2), [Distributed Restate: a first look](https://restate.dev/blog/distributed-restate-a-first-look), [Anatomy of a durable execution stack](https://restate.dev/blog/the-anatomy-of-a-durable-execution-stack-from-first-principles/). MEDIUM
- [Temporal Server docs](https://docs.temporal.io/temporal-service/temporal-server): shards, history/matching. MEDIUM
- [Temporal history service (mirror)](https://www.mintlify.com/temporalio/temporal/architecture/history-service), [range_id discussion](https://community.temporal.io/t/how-history-service-moudle-process-the-field-range-id/3386), [Understanding Temporal internals](https://community.temporal.io/t/understanding-temporal-internals/569): transfer/timer queues as outbox, range-ID fencing. LOW-MEDIUM (community)
- [redb `Durability`](https://docs.rs/redb/latest/redb/enum.Durability.html): redb 4.6.0 has `None` / `Immediate`. HIGH
- crates.io API (2026-11-18): tonic 0.14.9, prost 0.14.6, axum 0.8.9, redb 4.6.0, wasmtime 50.0.2, raft 0.7.0 (2023-03), opendal 0.59.5, object_store 0.14.2. HIGH

---
*Architecture research for: log-first durable execution engine (Ostro)*
*Researched: 2026-11-18*
