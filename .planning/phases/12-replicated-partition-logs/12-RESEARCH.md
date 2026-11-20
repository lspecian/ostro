# Phase 12: Replicated Partition Logs - Research

**Researched:** 2026-11-19
**Domain:** follower-pull ISR log replication (Kafka KIP-101/279/320/497/595 semantics) behind Ostro's `ReplicatedLog`, metadata-driven leader election, hot-standby apply, OSNP snapshot install, failure detection, and multi-node test harnesses
**Confidence:** HIGH for the code seams and pre-freeze findings (read from code at `9a79ca8` and the Phase 5/7/8 plans). MEDIUM for the Kafka protocol details (official KIP pages plus secondary sources, cross-checked). MEDIUM for the timing budget (derived from current config defaults; it needs measurement).

> **No CONTEXT.md exists for Phase 12** (no discuss-phase was run). The constraints below come from the orchestrator brief, `docs/04-chronik-reuse.md` §4 (binding design rules), PROJECT.md and the Phase 5 standing rules. Treat them as locked.

<user_constraints>
## User Constraints (no CONTEXT.md; sources: orchestrator brief, docs/04 §4, PROJECT.md, 05-DECISIONS standing rules)

### Locked Decisions
- **Option B:** port Chronik's pure algorithms (epoch cache, ISR math, watermarks, failover planner) behind Ostro's own `ReplicatedLog`. Fetch is a small tonic service `Fetch{partition, offset, epoch, replica_id}`. Option A (Kafka Fetch) and multi-raft (Option C) are rejected.
- **One replication path for everything, RF=1 included (D5).** A single node is ISR = {self}. There is no `if rf == 1` branch and no second log implementation.
- **ISR is computed only from measured follower LEOs** and published to metadata by the leader. Status endpoints report measured values only.
- **Elect only from the published ISR.** `min_insync_replicas` is enforced. Unclean election is not offered.
- **The conformance suite is written first** (before integration) and shown to fail on a deliberately broken build. It covers placement, ISR honesty, divergence/truncation, failover and min-insync.
- **Benchmark the user data path**, never an internal one. CI asserts batching on user partitions.
- **Benchmarks verify bytes on disk and acknowledged counts**, not just rates.
- **Durable-gated tail cache + sparse index** on the fetch read path (inherited from the vendored WAL).
- **Separate "wait for new data" from "read existing data" timeouts** in the fetch loop (Chronik RP-12).
- **Optional dedicated replication address** on the cluster listener (RP-10).
- The snapshot format is Phase 8's **OSNP v1**. Install is the Phase 8 staged directory + digest check + rename. There is no second snapshot format.
- Object storage uses the in-house `ostro-objstore` (D19). No `object_store`, no `chrono`.
- The Chronik clone must not be read or referenced by path. Ports carry an attribution header and a VENDORED.md row, as `epoch_cache.rs` does.
- Ordinals and reserved names come only from `.planning/RESERVATIONS.md`.
- Post-Phase-5 format rules: D18 (amended) and D20. After 05-20, changes are additive only. Meta changes are new variants plus a META_FORMAT bump.
- No Docker in WSL. SeaweedFS tests stay env-gated and human_needed (P8-D18).

### Claude's Discretion
- Crate layout, harness design, liveness detector design, timing defaults, plan breakdown.

### Deferred Ideas (OUT OF SCOPE)
- Dynamic membership (`ostro cluster init/join/remove`), single-node promotion, request forwarding, rolling upgrades and module GC: Phase 13 (CLUS-05..07).
- Jepsen/soak and the 3-node RF=3 ≥ 5,000 steps/s / p99 commit < 20 ms gate: Phase 14 (QA-04/05). Phase 12 measures and reports these numbers but does not gate on them.
- Live partition splitting, multi-region.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CLUS-01 | Replication conformance suite (placement, ISR honesty from measured LEOs, divergence/truncation, failover, min-insync) exists and fails on a deliberately broken build before replication is integrated | §Conformance Suite Design: written in wave 2 against the harness API and run red against the RF=1-only build; permanent failpoint mutants (`repl::mutant::*`) each of which must turn the suite red in CI |
| CLUS-02 | Follower-pull Fetch over the cluster listener; HW = min(LEO over ISR); leader epochs validated on every fetch; divergent tails truncated (incl. in object storage) | §Fetch Protocol, §ISR/HW math (maximal ISR, durable LEO), §Divergence (KIP-595-style DivergingEpoch in the fetch response), §Object-storage chain repair |
| CLUS-03 | Leaders elected only from the published ISR; writes stop when ISR < `min_insync_replicas` | §Election (new `ElectLeader` meta command validated at apply: leader ∈ published ISR), §min_insync gate (append rejected + HW frozen) |
| CLUS-04 | Followers apply committed entries (hot standby), bootstrap from a snapshot; failover p99 < 10 s | §Hot standby (the apply machine runs unchanged up to the follower HW), §Snapshot install ordering, §Liveness and timing budget, §Multi-process p99 harness |
</phase_requirements>

## Summary

Most of what Phase 12 needs already exists in shape. `ReplicatedLog` is the crate boundary. `compute_hw` takes a replica list. `EpochCache` is ported. Every envelope carries the leader epoch. Every term starts with `LeaderAnnounced`. Apply fences `(epoch, attempt)` tokens. The dispatcher rebuilds from the `running` keyspace and re-grants immediately on a new term. Phase 8 designs OSNP and a crash-safe install. The work is to (1) turn `Rf1Log` into the single ISR log implementation, with RF=1 as configuration rather than a separate type, (2) add the leader-side fetch service and the follower fetcher, (3) give the cluster a fault-tolerant controller that publishes ISR and elects only from it, (4) add a per-partition role lifecycle (leader ⇄ follower) to `Node`, and (5) build the conformance suite and harnesses that prove all of this.

The trait, as it stands, lacks five capabilities that replication needs. First, a follower-side verbatim append of leader envelopes. Second, a replica read capped at the local **durable LEO**: the existing `read` is HW-capped, and using it for replication would deadlock the HW at RF>1. Third, role transitions with an externally assigned epoch. Fourth, an HW that is fed by a replica tracker instead of directly by the commit callback. Fifth, a "reset/anchor at offset S" for snapshot install. All five are code changes. **None changes the 32-byte envelope, the WAL frame, the command set or the fencing commands.**

Two structural gaps matter more than any byte format. (a) **Phase 12 cannot meet SC3/SC4 with a single-voter metadata Raft.** Kill the node holding the only meta voter and no election can happen. Phase 12 therefore needs a networked Raft transport with **static voters** from `[[cluster.peers]]`. The vendored driver already supports multiple voters and has a `ChannelOutbox` for in-process tests. Dynamic membership stays in Phase 13. (b) Phase 8's bottomless design is single-node by construction: a node-level LOCK/incarnation, one `manifest/current` writer, and an `HwVector` updated in the RF=1 commit callback. In cluster mode, upload fencing must be per partition by leader epoch, and the HW publication point moves into the replica tracker.

**Primary recommendation:** before 05-20 freezes the formats, land the meta-encoding additions in 05-07 (F7) and the doc-only rules and reservations (F1–F6, F8, F9, F12). Then build Phase 12 as 11 plans in 6 waves. Wave 2 writes the conformance suite and permanent failpoint mutants **before** any replication code lands. Pure algorithms are property-tested in `ostro-log::replica`. An in-process multi-node harness (in-memory tonic transport with fault injection, 3-voter meta over `ChannelOutbox`) covers most scenarios. A multi-process loopback harness with real `kill -9` and a Rust TCP chaos proxy covers SC2 and the failover p99 measurement.

## PRE-FREEZE FINDINGS (key output)

Phase 5 freezes the log envelope, `node.toml`, the blob layout, the meta encoding and WIT in 05-19/05-20 (D18 amended, D20). Each finding below gives its type (**format change** or **doc-only**), the owning plan and a severity. "Format change" means bytes written by v0.1 change.

| ID | Area | Finding | Type | Owner | Severity |
|----|------|---------|------|-------|----------|
| **F1** | Log envelope (32-byte header, epoch, CRC32C) | **No change needed.** Replication carries the leader's OSTR envelopes **verbatim**: the follower writes the same envelope bytes inside its own local WAL V2 frame, so replicas hold byte-identical envelopes, which is a strong conformance check. `epoch` u32 is enough (meta epochs are u32). `flags`/`reserved` stay 0. Replication compression, if ever wanted, belongs at the transport level (gRPC compression), not in the envelope. Document three rules: (i) the envelope epoch **is** the meta-assigned leader epoch; (ii) followers never re-encode an envelope; (iii) `DEFAULT_MAX_ENVELOPE_BYTES` (4 MiB) is cluster-uniform, and every replication transport limit must exceed it. | doc-only | 05-20 (storage-format §3 note in the FROZEN text) | SHOULD |
| **F2** | `LeaderAnnounced` / fencing commands | **No change needed.** `LeaderAnnounced{epoch, node_id, announced_at_ms}` and `AttemptTokenMsg{epoch, n}` cover the cluster. Apply's epoch gate (`la.epoch > cursor.epoch && env_epoch == la.epoch`) already tolerates epochs that skip numbers (meta may assign 6 and 7 before any of their LAs commits). Document: in cluster mode the term epoch is **assigned by meta** (the `next_term_epoch(floor, log+1)` local rule is RF=1-bootstrap legacy and must not pick epochs in a cluster); `LeaderAnnounced.node_id` equals the meta leader for that epoch (apply cannot check this; the conformance suite does); a failover is semantically a restart (the dispatcher re-grants every `running` row immediately). | doc-only | 05-20 (storage-format §10 "term start") | SHOULD |
| **F3** | Recovery rule (§10) | **Empty log anchored at the state's `applied_offset`.** Today an empty log recovers as `log_start = LEO = 0`, so a state with `applied = S > 0` fails as "state ahead of log". Two Phase 8/12 paths produce exactly that state: Phase 8 restore of an idle partition whose snapshot equals the cut (08-18 `ImportMode::Create` with zero batches), and Phase 12 follower install (the window between the log reset and the first fetched batch). Rule: *if a partition's log has no records and its state store is readable, `log_start = LEO = applied_offset`. The log epoch cache is seeded from `cursor.epoch_history`. A non-empty log with `applied > LEO` stays fatal.* This changes recovery semantics only. No bytes change. | doc-only for Phase 5 (rule text); code in 08-09 (first producer) or 12-04 | 05-20 doc; **must be implemented before 08-18 ships** | SHOULD (likely Phase 8 bug otherwise) |
| **F4** | HW at open | Today `HW = recovered LEO` at open. That is correct only at RF=1. For a follower it would apply uncommitted, possibly divergent entries and corrupt the hot standby. One-path rule: **HW at open = `applied_offset`**. HW rises only through the replica tracker: as leader with ISR = {self} it immediately becomes the durable LEO (same observable RF=1 behaviour); as follower it becomes `min(leader HW, local durable LEO)`. No persisted HW checkpoint is needed. | doc-only (behaviour), code in 12-04 | Phase 12 (storage-format §10 text update is behavioural, allowed post-freeze) | INFO for Phase 5 |
| **F5** | Cursor row / epoch history | **No change needed.** `CursorMeta.epoch_history` already gives snapshot-installed followers and post-trim leaders the history below `log_start` (P8 F7; OSNP carries the cursor row verbatim). Document the semantic difference: the cursor history lists **applied, accepted** `LeaderAnnounced` epochs; the log epoch cache lists **envelope epochs** in the retained log. After install or anchoring, the log cache is seeded from the cursor history. | doc-only | 05-20 / 08-02 (snapshot-format.md Phase 12 checklist) | INFO |
| **F6** | `node.toml` (node id, cluster id, peers) | Today `node_id` lives only in server config (`[node] node_id`, default 1) and is not bound to the data directory. In a cluster, starting node 2's data dir with `node_id = 3` would present someone else's log as a fresh replica, which is a silent safety break. **Reserve additive keys now:** `node_id`, `cluster_id` (next to P8's `incarnation`/`restored_from`). Rule: absent = adopt from config and meta on the first cluster start and write durably; present = a mismatch is fatal. **`node_id = 0` is reserved** (meta "no leader" sentinel, F7). Peers do **not** go in `node.toml`: they are config (`[[cluster.peers]]`) and, from Phase 13, meta `Member` records. | doc-only now (name reservation + rule); additive keys later (allowed by D18/F6) | 05-20 (RESERVATIONS §11 + storage-format §1) | SHOULD |
| **F7** | Meta records (ISR, partition assignment, replica sets) | `PartitionAssignment{partition, leader, epoch, isr}` lacks the assigned replica set, an ISR-change CAS token and a min-ISR value. Partition records use **version = epoch**, so an ISR change cannot be expressed without bumping the leader epoch, and `SetPartitionLeader` with an equal epoch and a different ISR returns `Conflict`. D20 forbids adding fields after the freeze. **Add pre-freeze, trailing, in 05-07 (which already bumps META_FORMAT and writes the meta golden):** `PartitionAssignment += replicas: Vec<u64>` (preferred leader first; RF = len), `isr_version: u64` (bumped on every ISR change; AlterIsr compare token), `min_insync: u8`, `ext: Vec<u8>`; `Member += cluster_addr: Option<String>, repl_addr: Option<String>, ext: Vec<u8>`; `ClusterInfo += ext: Vec<u8>`. `InitCluster` writes `replicas = [node]`, `isr_version = 0`, `min_insync = 1`. **Fallback if rejected:** Phase 12 appends MetaRecord variant 24 `PartitionReplication{partition, replicas, isr_version, min_insync}` with a META_FORMAT bump and old-format decoders (D20). That works, but it splits one partition's view across two records (non-atomic reads for the leader), and AlterIsr then compares ISRs by value. | **format change** (meta encoding) | **05-07** | SHOULD (cheap now, costly after 05-20) |
| **F8** | Meta command ordinal space | RESERVATIONS §10 registers `MetaRecord`/`MetaKind`/`MetaState` order but **not `MetaCommand` or `MetaResult` variants** (today: `InitCluster 0, Put 1, Delete 2, SetPartitionLeader 3`). Phase 12 needs `ElectLeader`, `AlterIsr` and `ResignLeader` (plus a spare). Register the space: 0–3 `exists`, 4–7 `reserved P12`, 8+ `unallocated`. **Clarify D20:** appending a `MetaCommand` variant changes no existing encoding, so it needs **no META_FORMAT bump** (the bump is for record and snapshot struct changes). Mixed-version proposal gating is Phase 13's problem. **Do not change `SetPartitionLeader` validation**: adding "leader ∈ ISR" to an existing command changes how existing logs replay. Put the ISR-only rule in the new `ElectLeader` variant instead. | doc-only | 05-20 (RESERVATIONS flip) or a docs commit before it | SHOULD |
| **F9** | RESERVATIONS Phase 12 block | Phase 12 can run in parallel with Phases 9–11 (it depends only on 5 and 8). Phases 10 and 11 hold no blocks today. Proposed P12 block: **Cmd 41–42** reserved P12 (contingency, expected use 0); **MetaRecord 24** reserved P12 (F7 fallback only); **MetaCommand 4–7** (F8); keyspaces 0; journal kinds 0; RejectReason/InvalidCode 0; outbox kinds 0. Names: crate `ostro-repl-conformance` (P9 owns `ostro-conformance`); gRPC package `ostro.cluster.v1` (services `Replication`, `MetaPeer`); config `[replication]` plus `[[cluster.peers]]` and `replication_listen` inside P7's `[cluster]`; failpoint prefixes `repl::`, `fetch::`, `isr::`, `elect::`, `install::`; metrics prefix `ostro_repl_`; node.toml keys `node_id`, `cluster_id` (F6); data-dir temp prefix `snapshots/pNNNN/.incoming-*`. | doc-only | 05-20 (or a separate docs commit per rule 2) | SHOULD |
| **F10** | WIT | **No change.** Replication is invisible to guests. | — | — | — |
| **F11** | Blob layout | **No layout change.** Followers must hold every blob a replicated command references **before** their durable LEO passes that command. Otherwise a committed entry would reference a blob that exists only on a dead leader, and the failover loses data. Followers find the refs by walking the prost `Command` tree for `PayloadRef::Blob`. That depends on the **BlobRef invariant** (D18 amended / P8 F5) covering log commands, not only state rows. Document it explicitly: *"every BlobRef in a log command sits in a `PayloadRef` field of the prost Command tree; no future command may hide one in opaque bytes."* Module blobs: no meta change. Rule: a `Module` record is committed only after ≥ `min_insync` nodes hold the blob. | doc-only | 05-20 (storage-format §1/§4 blob note) | SHOULD |
| **F12** | Bottomless (Phase 8, not Phase 5) | (a) `HwVector` must be updated at the **single HW publication point**, not inside the WAL commit callback (08-11 puts it "next to `send_if_modified`" in the callback; at RF>1 the HW moves on follower fetch). (b) 08-18 restore lists `p/<pppp>/seg/` from `chain_start` and then runs `verify_chain`. It should instead **walk `prev_chunk_sha256` back from the manifest's `last_chunk_key`**, so that a forked or divergent chunk (same base, different epoch/end) is ignored. Remote retention should delete chunks the head no longer reaches. (c) The node-level `LOCK{cluster_id, node_id, incarnation}` and the single `manifest/current` writer are single-node only. Cluster mode needs per-partition writer fencing by leader epoch (§Object storage). | plan/doc notes; the bottomless layout is Phase 8's own format | 08-11, 08-13, 08-18 | SHOULD (cheap before Phase 8 executes) |
| **F13** | mTLS identity (Phase 7, not Phase 5) | The Replication and MetaPeer services must authorize `replica_id` against the peer certificate. Node certificates should carry SAN URI `ostro://<cluster_id>/node/<node_id>`. Worker certs (`ostro://<cluster>/worker/...`) must be rejected by the cluster-internal services. All nodes need **one** cluster CA: Phase 7 generates a per-node CA in `<data>/tls/` (D-W5 defers distribution to Phase 12). Static clusters use `[cluster.tls]` files mode, and the test harness generates a CA plus per-node certs with rcgen. | plan note | 07-06 (node cert SANs), Phase 12 (authz) | SHOULD |
| **F14** | Roadmap dependencies | (a) Phase 12 needs a fault-tolerant meta (static multi-voter Raft over tonic). That pulls the *transport* part of CLUS-05 forward; join/remove/promotion stay in Phase 13. (b) Phase 12 reuses the Phase 7 cluster listener, CA and mTLS, but ROADMAP lists only Phases 5 and 8 as dependencies. Add Phase 7, or plan for Phase 12 to create the listener if Phase 7 has not run. (c) QA-01 asks for ≥ 10,000 injected faults "per phase gate". The Phase 12 gate needs a cluster crash campaign (see Validation). | planning | ROADMAP/STATE edit before planning Phase 12 | SHOULD |

**Net for the Phase 5 freeze:** one format change (F7, meta encoding, in 05-07) and doc-only items for 05-20 (F1, F2, F3, F5, F6, F8, F9, F11). Nothing touches the envelope, the WAL frame, `Command` tags 1–7, `LeaderAnnounced`, `AttemptTokenMsg`, the cursor row, the keyspaces or WIT.

## Standard Stack

### Core (all already pinned in the workspace; no new third-party crates required)
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| tonic / tonic-prost / prost | 0.14.9 / 0.14.9 / 0.14.6 | `ostro.cluster.v1.Replication` (Fetch, FetchSnapshot, FetchBlob) and `MetaPeer` (Raft message stream, Propose forward) on the cluster listener | Already the gRPC stack; Phase 7 adds `tls-aws-lc` + `tls-connect-info` |
| vendored `ostro-raft-driver` / `ostro-raft-rs` | in-tree | Static 3/5-voter metadata Raft; `Outbox` trait for the transport; `sample_live(within)` for liveness; `leader_contact_age()` | Supports multiple voters today; `ChannelOutbox` exists for in-process tests |
| vendored `ostro-wal` | in-tree | Group commit, **durable-gated** tail cache (`read_tail` stops at `durable_through`), sparse `PartitionIndex`, `truncate_to` | docs/04 rule satisfied by the existing code |
| `ostro-log` (`epoch_cache`, `hw`, `verify`) | in-tree | Base for the ported algorithms | `EpochCache` was already ported from Chronik `leader_epoch.rs` |
| fail | 0.5.1 | Failpoints, including the permanent `repl::mutant::*` "broken build" switches | Existing FAILPOINT_CRATES infrastructure |
| proptest / proptest-state-machine | 1.12.0 / 0.9.0 | Model-based tests of ISR/HW/divergence/election (reference model vs implementation) | Already pinned |
| `ostro-objstore` (LocalFs) | Phase 6/8 crate | Shared bucket directory for multi-node object-storage tests without Docker | D19 |
| rcgen / x509-parser | Phase 7 pins | Test CA + per-node certs; SAN URI node identity check | Phase 7 adds them |

### Supporting
| Library | Purpose | When to Use |
|---------|---------|-------------|
| `tokio::io::duplex` + tonic `Endpoint::connect_with_connector` (hyper-util `TokioIo`) | In-memory tonic channel: the real proto encoding with no sockets | In-process harness transport |
| A hand-written tokio TCP chaos proxy (test crate, about 200 lines) | Cut, delay or blackhole node↔node links in multi-process tests without iptables/root | Network-partition scenarios in the loopback tier |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Custom in-memory transport | `turmoil` 0.7 | It simulates the network deterministically only on its single-threaded sim runtime. Ostro's apply machine is a `std::thread`, recovery uses `spawn_blocking`, and fjall and the WAL own threads, so determinism is lost and the integration is invasive. Rejected. |
| Custom in-memory transport | `madsim` | It replaces tokio and std time/threads. That conflicts with fjall and the vendored WAL. Rejected. |
| Static multi-voter meta in Phase 12 | Single meta voter on node 1 | Killing node 1 makes election impossible, so SC3/SC4 could only be shown by killing non-meta nodes. Weak; rejected. |
| KIP-595 DivergingEpoch inside Fetch | A separate `OffsetForLeaderEpoch` RPC (KIP-101/279) | Kafka itself moved truncation onto Fetch in 2.7 (KAFKA-10435). Use one RPC; keep `EpochHistory::end_offset_for_epoch_at` as the shared pure function. |
| Failpoint mutants | Cargo-feature mutants | Features multiply build configurations, while failpoints reuse the `--features failpoints` build already in CI. Use failpoints. |

**Installation:** no new third-party crates. Workspace additions: crate `crates/ostro-repl-conformance` (dev/test only), proto file `crates/ostro-proto/proto/ostro/cluster/v1/cluster.proto`.

## Architecture Patterns

### Recommended Project Structure
```
crates/ostro-log/src/
├── replica/                 # NEW: pure, no I/O, proptested (ported algorithms; attribution headers)
│   ├── tracker.rs           # ReplicaTracker: per-replica durable LEO, last_caught_up_ms, fetch state
│   ├── isr.rs               # in_sync / expand / shrink decisions, maximal ISR, min_insync gate
│   ├── watermark.rs         # HW = min(durable LEO over maximal ISR), never decreasing, frozen < min_insync
│   ├── divergence.rs        # (fetch_offset, last_fetched_epoch) vs EpochHistory -> Ok | Diverging{epoch,end} | SnapshotRequired
│   └── failover.rs          # planner: (assignment, live set) -> ElectLeader | ShrinkIsr | Offline
├── partition_log.rs         # Rf1Log evolved into the ONE impl (RF=1 = replicas [self]); Rf1Log name removed
├── hw.rs                    # compute_hw stays the single HW function (extended with min_insync)
└── cut.rs                   # (Phase 8) HwVector, updated from the single publish_hw()
crates/ostro-cluster/        # NEW (or a module inside ostro-server): transport + controller
├── proto glue, replication service (leader side), fetcher (follower side)
├── meta_peer.rs             # Raft Outbox over tonic, Propose forwarding
├── controller.rs            # runs on the meta Raft leader: liveness -> failover planner -> ElectLeader/AlterIsr
└── role.rs                  # per-partition role lifecycle driven by meta assignments
crates/ostro-partition/src/
├── node.rs                  # generic over the one log type; per-partition role transitions
└── replica_services/        # snapshot + retention run in BOTH roles (moved out of leader services)
crates/ostro-repl-conformance/
├── src/harness/{inproc.rs, process.rs, chaos_proxy.rs, oracle.rs}
└── tests/{placement,isr_honesty,divergence,failover,min_insync,standby,bootstrap,one_path,mutants}.rs
```

### Pattern 1: One log implementation, RF=1 as configuration (D5)
**What:** evolve `Rf1Log` in place into `PartitionLog`. HW is published only by `ReplicaTracker::publish()`. The local WAL commit callback reports `(self, durable_end)` to the tracker. Follower fetches report `(replica, durable_leo)`. At RF=1 the replica set is `[self]`, `min_insync = 1`, and the tracker computes HW = local durable LEO. The behaviour is identical and the code path is the same.
**Enforcement:**
- remove the `Rf1Log` type, with a ci-local grep gate: no `Rf1Log` and no `rf == 1`-style branches in `crates/ostro-log/src`;
- a test asserts that an RF=1 node's HW advances through the tracker (counter `ostro_repl_hw_publish_total{partition}` > 0);
- `compute_hw` is the only HW function.
**Trait additions (signatures for 12-01):**
```rust
// ReplicatedLog additions (code change; no format change)
fn read_replica(&self, from: Offset, max_bytes: usize)            // capped at local DURABLE LEO, raw envelopes
    -> impl Future<Output = Result<Vec<RawBatch>, LogError>> + Send;   // RawBatch = Phase 8 read_envelopes unit
fn append_replica(&self, batches: Vec<RawBatch>)                   // follower: verbatim, base == LEO, epoch >= last
    -> impl Future<Output = Result<Offset, LogError>> + Send;          // returns new (not yet durable) LEO
fn become_leader(&self, epoch: Epoch, replicas: ReplicaSet) -> Result<(), LogError>;
fn become_follower(&self, epoch: Epoch, leader: u64) -> Result<(), LogError>;
fn set_follower_hw(&self, leader_hw: Offset);                      // HW = min(leader_hw, durable LEO)
fn reset_to(&self, start: Offset, epochs: EpochHistory)            // install: delete all segments, anchor at start
    -> impl Future<Output = Result<(), LogError>> + Send;
fn durable_end(&self) -> Offset;                                   // from the commit callback
```

### Pattern 2: ISR and HW math (port; Kafka semantics)
- **Progress:** a follower's `durable_leo` (fsynced) is what counts for the HW. The HW contract is "Never `Ok` for data that is not durable on the ISR" (`replicated.rs`). A follower reports `durable_leo` **separately** from `fetch_offset` (proto field from day one, even if v1 sets both equal by fetching only after fsync).
- **In-sync:** `now - last_caught_up_ms ≤ replica_lag_max` (default 10 s). `last_caught_up_ms` is set when a fetch's `fetch_offset ≥ leader LEO as of the previous response` (Kafka `lastCaughtUpTime`).
- **Expand:** a follower outside the ISR with `durable_leo ≥ max(HW, start offset of the leader's current epoch)` is proposed for addition. The epoch-start clause is Kafka's `isFollowerInSync` guard; without it a replica could join the ISR without holding the current term's first entry.
- **Shrink:** a follower in the ISR whose lag exceeds the limit is proposed for removal through `AlterIsr` (leader-driven). Dead nodes are also removed by the controller (fast path, §Liveness).
- **Maximal ISR (KIP-497):** `HW = min(durable LEO over published ISR ∪ pending adds)`. A pending shrink keeps counting the removed replica until meta commits it, because if the shrink fails, that replica could be elected and must hold everything below HW. The HW never decreases on a given leader.
- **min_insync:** `append` returns `LogError::NotEnoughReplicas` when `|published ISR| < min_insync`, **and** the HW is frozen while that holds. That means an entry appended before a shrink can never be acked on a single copy. Kafka returns `NotEnoughReplicasAfterAppend` to the producer; Ostro acks only after apply, which happens only below HW, so freezing HW is the equivalent.
- **ISR publication:** only through meta commands carrying `(epoch, isr_version)`. Apply rejects a stale epoch or version. The leader's in-memory ISR follows the **published** one, never the assignment. Status endpoints read the published ISR plus the measured tracker values.

### Pattern 3: Fetch protocol (tonic, follower-pull, long-poll)
```proto
// crates/ostro-proto/proto/ostro/cluster/v1/cluster.proto (package ostro.cluster.v1)
service Replication {
  rpc Fetch(FetchRequest) returns (FetchResponse);                     // unary long-poll
  rpc FetchSnapshot(FetchSnapshotRequest) returns (stream SnapshotFrame); // OSNP frames, resumable at frame boundaries
  rpc FetchBlob(FetchBlobRequest) returns (stream BlobChunk);           // payload/module blobs by sha256
}
message FetchRequest {
  uint32 partition = 1; uint64 replica_id = 2;
  uint32 leader_epoch = 3;        // follower's current view of the leader epoch (KIP-320 CurrentLeaderEpoch)
  uint64 fetch_offset = 4;        // next offset wanted (follower LEO)
  uint32 last_fetched_epoch = 5;  // epoch of the follower's last batch (KIP-595); snapshot latest_epoch for an anchored empty log
  uint64 durable_leo = 6;         // follower's fsynced end: the ONLY value used for HW
  uint32 max_bytes = 7; uint32 max_wait_ms = 8; uint32 min_bytes = 9;
}
message FetchResponse {
  FetchError error = 1;           // NONE | FENCED_LEADER_EPOCH | UNKNOWN_LEADER_EPOCH | NOT_LEADER | NOT_REPLICA | SNAPSHOT_REQUIRED
  uint32 leader_epoch = 2; uint64 high_watermark = 3; uint64 log_start = 4; uint64 leader_durable_end = 5;
  optional DivergingEpoch diverging = 6;   // {epoch, end_offset}: truncate, then refetch (no error code, as in Kafka)
  repeated bytes envelopes = 7;            // raw OSTR envelopes, verbatim, whole batches only, >= 1 if any is available
}
```
Leader-side rules, evaluated in this order:
1. The mTLS peer identity must equal `replica_id` and the replica must be in `replicas`, else `NOT_REPLICA`.
2. `leader_epoch < current` → `FENCED_LEADER_EPOCH` (the follower refreshes meta). `> current` → `UNKNOWN_LEADER_EPOCH` (this leader is behind on meta; the follower retries).
3. If not leader → `NOT_LEADER`.
4. Divergence check `(fetch_offset, last_fetched_epoch)` against `EpochHistory::end_offset_for_epoch_at`: a mismatch returns `diverging{epoch, end}`. If `fetch_offset < log_start` → `SNAPSHOT_REQUIRED`.
5. Record progress `(replica, durable_leo, fetch_offset)` in the tracker, before any wait.
6. Data available (`fetch_offset < durable end`) → read **immediately** under `read_timeout`. Otherwise long-poll on the durable-end watch for up to `max_wait_ms`, then read.

**The two timeouts never mix:** `max_wait_ms` bounds only the idle wait. An in-progress read is bounded by the server-side `fetch_read_timeout` (default 5 s) and is never cut short by `max_wait` (RP-12).

Reads come from `read_replica` (durable-gated tail cache, then sparse index), capped at the **leader's durable end**, not at HW.

Follower loop:
1. Fetch.
2. On `diverging`: `truncate_to(min(end, own end of that epoch))` and loop (KIP-279 iteration).
3. On `SNAPSHOT_REQUIRED`: run the install flow.
4. Verify every envelope (CRC, partition, `base == LEO`, epoch non-decreasing).
5. Fetch the referenced blobs (F11).
6. `append_replica`; wait for the commit callback; set `durable_leo`.
7. `set_follower_hw(resp.high_watermark)`. The apply machine follows the HW.

### Pattern 4: Controller, election and liveness
- **Where:** the controller runs on the metadata Raft leader (Chronik RP-7: only the Raft leader writes assignments). New meta commands, F8 ordinals 4–6:
  - `ElectLeader{partition, epoch: current+1, leader, isr, expected_isr_version}`. Apply validates `leader ∈ current published ISR`, `isr ⊆ replicas`, `leader ∈ isr` and the expected version. `leader = 0` means offline: when no ISR member is live, the ISR keeps the last member, Kafka-style, so that it can return cleanly.
  - `AlterIsr{partition, epoch, expected_isr_version, isr}`. Apply validates the epoch equals the current one, the version, `isr ⊆ replicas` and `leader ∈ isr`. It bumps `isr_version`.
  - `ResignLeader{partition, epoch, node}`. Used when a leader's partition fail-stops (`PartitionSlot::Failed`/`LogHealth::Failed`), so the controller moves leadership.
- **Liveness:** with static voters (Phase 12), node liveness is the controller's accumulated `RaftHandle::sample_live(within = session_timeout)`, which is free because Raft heartbeats already flow. Phase 13 non-voters will need a heartbeat RPC. Accelerator: a follower whose fetch to the leader fails with a connection-refused or reset error reports a "suspect" to the controller, which confirms it with the liveness sample. A `kill -9` on loopback is detected in milliseconds; a partition by the session timeout.
- **Failover planner (pure):** for each partition whose leader is not live → `ElectLeader` with the first live member of `isr` in `replicas` order (preferred leader), and `isr' = isr ∩ live`. For partitions where a dead node is only a follower → `AlterIsr` removing it, which un-stalls the HW quickly instead of waiting `replica_lag_max`.
- **Zombie safety does not depend on timing:** an isolated old leader cannot commit, because (a) its maximal ISR still includes the followers, which have stopped fetching from it, and (b) its `AlterIsr` carries a stale epoch and is rejected. Its executors' appends never commit, so no effect is ever released (effects run only after `EffectScheduled` commits). It also self-demotes after losing meta contact for `session_timeout` (`leader_contact_age()`), so clients fail fast instead of hanging.

### Pattern 5: Partition role lifecycle (the same for RF=1 and RF=N)
- **Become leader** (meta says leader = self at epoch e):
  1. stop the fetcher;
  2. `become_leader(e)`;
  3. wait until applied ≥ local HW (the hot standby is already close);
  4. append `LeaderAnnounced{e}` (min_insync gate applies; on `NotEnoughReplicas` retry with backoff and report the partition unavailable);
  5. wait until LA is committed and applied (this also commits every earlier-epoch entry below it);
  6. spawn the leader services (dispatcher rebuild re-grants `running` rows immediately under e, timers, shuffler, uploader).
- **Become follower:**
  1. `LeaderTasks::shutdown()`;
  2. `acks.fail_all(NotLeader{leader})`;
  3. `become_follower(e, leader)`;
  4. start the fetcher (the first fetch carries `last_fetched_epoch`, so divergence is resolved before anything else);
  5. the apply machine keeps running with actions off.
- **Single node:** the local meta leader runs the same controller and elects self from ISR = {self} with epoch+1 on every start. This replaces "Node picks an epoch, then `SetPartitionLeader`" (04-17 order) with meta first, then LA, so the bootstrap order is the cluster order.
- **Replica services** (snapshot service, retention) run in **both** roles. Phase 8 spawns them inside `leader::spawn_all`; followers need snapshots and trimming too.

### Pattern 6: Snapshot bootstrap (install ordering that survives a crash at every step)
1. Fetch `SNAPSHOT_REQUIRED`.
2. `FetchSnapshot` streams the leader's newest local OSNP (from `snapshots/pNNNN/`, or one produced on demand) frame by frame into `snapshots/pNNNN/.incoming-<applied>`. It is resumable from the last complete frame.
3. Verify the header CRC, every frame CRC and the whole-file sha256.
4. **Reset the log first:** `reset_to(S)` deletes every segment and fsyncs the directory.
5. Phase 8 `install_snapshot` (staged dir, digest check, atomic rename, fsync `state/`).
6. Anchor the log at S and seed the epoch cache from `cursor.epoch_history`.
7. Fetch from S with `last_fetched_epoch = header.latest_epoch`.

Crash between steps 4 and 5: empty log plus old state `applied = A`. F3 anchors the log at A, the follower fetches from A, gets `SNAPSHOT_REQUIRED` again, and retries. Installing state first and resetting the log second would leave "state ahead of log", which is fatal. **Do not reorder.**

### Pattern 7: Object-storage chain repair (cluster bottomless)
- Each partition's leader uploads that partition's chunks only (leader services). Writer fencing is per partition: the head object `p/<pppp>/head = {epoch, last_chunk_key, sha}` is written with `If-Match`, and a writer whose epoch is below the head's epoch is fenced. Chunk keys already include the epoch, and `put_if_absent` holds.
- Uploads stay HW-capped. **With a correct HW, the remote chain can only hold committed entries**, so in correct operation a "divergent remote tail" can only come from a bug or a mutant. The conformance suite's object-storage check is therefore an oracle: remote chain == committed log prefix, byte for byte.
- Defensive reconcile on takeover: walk back from the head. Any chunk with `end > end_offset_for_epoch(chunk.epoch)` (per the new leader's epoch history) is divergent. Re-upload the consistent prefix of a straddling chunk as a new chunk (new key, because the end differs), continue the chain from it (`prev` = last good chunk), move the head, and let retention delete the unreachable divergent objects after the head is durable.
- A cluster-wide consistent cut and whole-cluster restore are **not** solved by Phase 8's node-local `HwVector` (§Open Questions).

### Anti-Patterns to Avoid
- **Serving replication through the HW-capped `read`:** followers never receive anything above HW, so HW never advances at RF>1. Use `read_replica`, capped at the durable end.
- **Counting non-durable follower LEO toward HW:** after a power loss on the follower, "committed" data disappears. Count `durable_leo` only.
- **Computing ISR from the assignment:** that is Chronik's "ISR is fiction". The ISR comes from the tracker and meta only.
- **Truncating a follower to its local HW when the leader answers UNDEFINED:** this is the KIP-101 loss scenario. Treat UNDEFINED (follower epoch older than the leader's oldest retained epoch) as `SNAPSHOT_REQUIRED` and assert the follower is not in the published ISR.
- **A restarted node self-electing in a cluster:** a restart always comes up as follower (or Offline) until meta assigns it. The local `next_term_epoch` rule is RF=1-bootstrap only.
- **Using `SetPartitionLeader` for failover:** it has no ISR check. Use `ElectLeader`.
- **Leader services owning snapshot and retention:** followers would never snapshot or trim.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Leader-epoch history and "where did epoch e end" | A new structure | Existing `ostro_log::EpochCache` / `EpochHistory::end_offset_for_epoch_at` | Already ported (Kafka semantics, UNDEFINED = None) and tested |
| Metadata consensus | Ad-hoc controller state, or leases in files | Vendored `RaftDriver` with multiple voters + an `Outbox` over tonic | Election, persistence and `OutcomeUnknown` retry semantics already exist |
| Failure detection for voters | A heartbeat protocol | `RaftHandle::sample_live()` (+ `leader_contact_age()` for self-demotion) | Raft heartbeats already flow every `heartbeat_tick` |
| Snapshot format, streaming and install | A second format or physical copies | Phase 8 OSNP v1 `OsnpReader::next_frame` + `install_snapshot` | D18/P8-D1: one format for restore and follower install |
| Tail reads | A new cache | Vendored WAL `read_tail` (durable-gated) + `PartitionIndex` | docs/04 RP-9 lesson; already durable-gated |
| Suffix truncation | Hand file cuts | `ReplicatedLog::truncate_to` (quiesces in-flight appends, then WAL `truncate_to`) | Existing, failpoint-tested |
| Network simulation framework | turmoil/madsim adoption | In-memory tonic transport with a fault-injection layer | Determinism frameworks fight fjall/WAL threads (see Alternatives) |
| "Broken build" demonstrations | A one-off branch | Permanent failpoint mutants `repl::mutant::*` checked in CI | The red-before-green proof stays reproducible forever |

**Key insight:** every replication bug class Chronik hit (an unreachable path, fictional ISR, unclean election, a read deadline used as a wait, benchmarks that miscounted) is a *wiring* bug, not an algorithm bug. The defence is a single code path plus an oracle that inspects bytes on disk, not cleverer algorithms.

## Common Pitfalls

### Pitfall 1: The HW deadlock from the HW-capped read
**What goes wrong:** the fetch service calls `ReplicatedLog::read`, which caps at HW, so the HW stays put forever at RF=3.
**How to avoid:** `read_replica`, capped at the leader's durable end. Conformance test: RF=3 appends commit within N fetch rounds.
**Warning signs:** HW frozen at the value it had when the leader was elected; followers' fetches always empty.

### Pitfall 2: Pending ISR shrink counted as effective
**What goes wrong:** the leader drops a lagging follower from its in-memory ISR, advances HW, and then the meta CAS fails (stale epoch). The excluded follower is elected and lacks acked data.
**How to avoid:** maximal-ISR rule (KIP-497). The proptest model includes "AlterIsr rejected" transitions.

### Pitfall 3: Follower HW initialised from local LEO
**What goes wrong:** a restarted follower applies its divergent tail; a later truncation leaves "state ahead of log" (fatal) or, worse, a silently diverged standby.
**How to avoid:** F4 (HW at open = applied). The conformance standby test compares `state_hash` across replicas at equal `applied_offset`.

### Pitfall 4: Committed data on fewer than min_insync copies
**What goes wrong:** the ISR shrinks to {leader}; HW advances on one copy; the ack goes out; the leader's disk dies.
**How to avoid:** freeze HW while `|published ISR| < min_insync`, and reject appends. Test: with the ISR forced below min, zero acks occur, and acks resume when the follower rejoins.

### Pitfall 5: Blob not replicated before the commit
**What goes wrong:** a >256 KiB payload exists only on the leader; it fails over; runs fail reading the input or step result.
**How to avoid:** the follower fetches every `PayloadRef::Blob` in a batch before advancing `durable_leo` (F11). The conformance test kills the leader immediately after a 1 MiB start is acked and asserts the new leader serves it.

### Pitfall 6: gRPC message size vs envelope size
**What goes wrong:** tonic's default `max_decoding_message_size` is **4 MB**. A 4 MiB envelope plus framing exceeds it, and the follower is stuck forever on that offset.
**How to avoid:** set the client and server decoding limit to `max_envelope_bytes + 64 KiB`, always return at least one whole batch, and treat `DEFAULT_MAX_ENVELOPE_BYTES` as cluster-uniform (F1). Test with a maximum-size envelope.

### Pitfall 7: Install ordering
**What goes wrong:** installing state first and resetting the log afterwards; a crash in between leaves "state ahead of log".
**How to avoid:** Pattern 6 order plus F3. Failpoints `install::after_log_reset` and `install::before_rename` run in a kill loop.

### Pitfall 8: In-process "crash" is not a crash
**What goes wrong:** dropping a `Node` in-process keeps the page cache and may leave threads holding the fjall lock. The test passes but proves nothing about `kill -9`.
**How to avoid:** use in-process "isolate + stop" for logic and zombie scenarios. SC2 ("leader dies holding unreplicated writes") and the failover p99 must run in the **multi-process tier** with real `kill -9`.

### Pitfall 9: Process-global failpoints in an in-process cluster
**What goes wrong:** `fail::cfg` affects every node in the process, so targeted single-node faults are impossible.
**How to avoid:** use failpoints only for whole-cluster mutants and for multi-process nodes (per-process `FAILPOINTS` env). Use transport-level injectors and per-node hooks for targeted in-process faults.

### Pitfall 10: Data dir bound to the wrong identity
**What goes wrong:** a config typo starts node 2's data with `node_id = 3`, and its log is counted as a fresh replica's.
**How to avoid:** F6 `node.toml` binding; mismatch is fatal at open.

### Pitfall 11: The failover budget blown by meta re-election plus a fresh session timeout
**What goes wrong:** the dead node was also the meta Raft leader. Meta election takes 2–4 s (tick 100 ms × election_tick 20, randomized ×1–2). The new controller then waits a whole session timeout before declaring anyone dead.
**How to avoid:** cluster profile `election_tick = 10` (1–2 s). The new controller seeds suspicion from nodes absent from its first `sample_live` samples (they did not vote for it). Measure p99, don't assume it. **MEDIUM confidence:** raft-rs `recent_active` behaviour right after a leader change must be verified in a test.

### Pitfall 12: Leader-only services that followers need
**What goes wrong:** snapshot and retention never run on followers; a follower's log grows unbounded, and a later promotion replays everything.
**How to avoid:** Pattern 5 replica services.

## Code Examples

### Pure HW with the maximal ISR and the min_insync freeze (extends the existing `compute_hw`)
```rust
// crates/ostro-log/src/replica/watermark.rs (sketch; semantics per KIP-497 + Ostro durability contract)
pub fn next_hw(prev_hw: Offset, progress: &[ReplicaProgress], published_isr: &[u64],
               pending_add: &[u64], min_insync: usize) -> Offset {
    if published_isr.len() < min_insync { return prev_hw; }            // writes stop; nothing new commits
    let maximal = |n: u64| published_isr.contains(&n) || pending_add.contains(&n);
    let candidate = progress.iter().filter(|p| maximal(p.node)).map(|p| p.leo).min()
        .expect("leader is always in its own ISR");
    candidate.max(prev_hw)                                              // never decreases on this leader
}
```

### Divergence decision on the leader (KIP-595 style; reuses EpochHistory)
```rust
// crates/ostro-log/src/replica/divergence.rs (sketch)
pub enum FetchCheck { Ok, Diverging { epoch: Epoch, end: Offset }, SnapshotRequired }
pub fn check(h: &EpochHistory, log_start: Offset, leo: Offset,
             fetch_offset: Offset, last_fetched_epoch: Option<Epoch>) -> FetchCheck {
    if fetch_offset < log_start { return FetchCheck::SnapshotRequired; }
    let Some(e) = last_fetched_epoch else { return FetchCheck::Ok };   // empty follower log at log_start
    match h.end_offset_for_epoch_at(e, leo) {
        None => FetchCheck::SnapshotRequired,                           // UNDEFINED: never truncate-to-HW (KIP-101)
        Some((le, end)) if le != e || end < fetch_offset => FetchCheck::Diverging { epoch: le, end },
        Some(_) => FetchCheck::Ok,
    }
}
```

### Election validation at meta apply (new command; never retrofit SetPartitionLeader)
```rust
// crates/ostro-meta/src/state.rs (sketch)
MetaCommand::ElectLeader { partition, epoch, leader, isr, expected_isr_version } => {
    let cur = self.partitions.get(&partition)?;                   // NotFound otherwise
    if u64::from(epoch) <= cur.version { return StaleVersion { current: cur.version } }
    if cur.value.isr_version != expected_isr_version { return StaleVersion { .. } }
    if leader != NO_LEADER && !cur.value.isr.contains(&leader) { return Invalid("leader not in published ISR") }
    if !isr.iter().all(|n| cur.value.replicas.contains(n)) { return Invalid("isr not subset of replicas") }
    // write leader, epoch, isr, isr_version + 1
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Follower truncates to its HW on restart | Truncate by leader epoch (KIP-101), iterated for fast leader changes (KIP-279) | Kafka 0.11 / 2.0 | Use the epoch cache; never truncate to HW |
| Separate OffsetsForLeaderEpoch round trip | `LastFetchedEpoch` in Fetch → `DivergingEpoch` in the response (KIP-595 fetch changes, KAFKA-10435) | Kafka 2.7 (Fetch v12) | One RPC (Pattern 3) |
| Leader writes the ISR directly to ZooKeeper | AlterPartition/AlterIsr to the controller, with a "maximal ISR" for HW (KIP-497) | Kafka 2.7 | `AlterIsr` meta command plus the maximal-ISR HW rule |
| CurrentLeaderEpoch not checked on fetch | Fetch carries CurrentLeaderEpoch; FENCED/UNKNOWN_LEADER_EPOCH (KIP-320) | Kafka 2.1/2.2 | Stale-epoch fetch rejection (SC2) |
| ISR-only election may still pick a replica lacking data when the ISR shrinks to one | Eligible Leader Replicas (KIP-966): wait for the controller to commit the ISR before advancing HW | Kafka 3.7+/4.x | Ostro's maximal-ISR freeze plus "keep the last ISR member, leader = 0" covers the Phase 12 needs; ELR is not needed |

**Deprecated/outdated:** docs/04 §3.2 "Option A for Phase 4 MVP" is superseded (Option B). docs/03 §4.2 says "the new leader waits until its state store has applied up to its LEO". It cannot: apply stops at HW. The correct rule is: append LA, wait for LA to be committed and applied (Pattern 5).

## Liveness and Failover Timing Budget (p99 < 10 s target)

| Step | Default | Worst case |
|------|---------|-----------|
| Detect (kill -9 on loopback: suspect report + liveness confirmation) | ~0.5 s | — |
| Detect (network partition: `session_timeout`) | 3 s | 3.5 s |
| Meta re-election if the controller died (`election_tick` 10 × 100 ms, randomized) | 0 | 2 s |
| New controller suspicion seeding | 0 | 1 s |
| `ElectLeader` propose + apply (forwarded, one Raft round) | <50 ms | 0.2 s |
| New leader notices (meta apply watch) + followers re-point the fetcher | 0.1 s | 0.5 s |
| LA append + commit (one follower fetch round + fsyncs; WSL2 fsync is slow) | 50 ms | 0.5 s |
| Apply catch-up (hot standby) + dispatcher rebuild (`running` scan) + re-grant | 0.2 s | 1 s |
| **Total** | **~1–4 s** | **~8.7 s** |

The Phase 12 config defaults: `session_timeout = 3s`, `replica_lag_max = 10s`, meta `election_tick = 10`, `heartbeat_tick = 2`, `fetch_max_wait = 500ms`, `fetch_read_timeout = 5s`. The worst-case row stays under 10 s only with the shortened meta election tick, so the p99 harness must include iterations where the controller node is the one killed.

## Conformance Suite Design (CLUS-01)

**Written first (wave 2).** The suite targets a harness API defined in 12-01: `Cluster::start(n, rf, min_insync)`, `append/start_run`, `isolate(node)`, `heal`, `kill(node)`, `restart(node)`, `partition(a, b)`, `assignment(p)`, `node_disk(node)` for on-disk oracles, and `bucket()`. In wave 2 the implementation behind RF=3 does not exist, so the suite runs **red**. The run output (scenario names plus failure reasons) is committed in 12-02-SUMMARY as the "fails before integration" evidence. The RF=1 scenarios pass from day one, which proves the oracles themselves work.

**Permanent "deliberately broken build" proof:** failpoint mutants in the production code (`--features failpoints`), each a one-line behaviour switch:

| Mutant failpoint | Broken behaviour | Scenario(s) that must fail |
|---|---|---|
| `repl::mutant::isr_from_assignment` | ISR = replicas (Chronik "ISR is fiction") | isr_honesty (follower with 0 bytes on disk listed in ISR) |
| `repl::mutant::hw_leader_leo` | HW = leader durable LEO | divergence (acked write lost after failover), bytes-on-disk oracle |
| `repl::mutant::count_nondurable_leo` | HW uses fetch_offset, not durable_leo | kill -9 follower power-loss scenario (multi-process) |
| `repl::mutant::skip_epoch_check` | Fetch ignores leader_epoch | stale_epoch_fetch |
| `repl::mutant::no_truncate` | Diverging response ignored | divergence (replica logs differ below HW) |
| `repl::mutant::unclean_election` | Planner may pick an out-of-ISR replica | failover_isr_only |
| `repl::mutant::ignore_min_insync` | No append rejection or HW freeze | min_insync |
| `repl::mutant::shrink_before_commit` | Pending shrink treated as effective | isr_shrink_race (proptest model) |
| `repl::mutant::upload_above_hw` | Uploader reads past HW | object_storage_prefix oracle |
| `repl::mutant::rf1_bypass` | RF=1 HW set directly by the commit callback | one_path (tracker counter stays 0) |

`tests/mutants.rs` runs the relevant scenario under each failpoint and asserts that it **fails**. CI and ci-local run the matrix, so the suite stays proven able to catch each mutant.

**Oracles (all measured, never self-reported):**
- **Acked ⇒ on disk:** for every ack, read `wal/p/<id>` of ≥ `min_insync` nodes' data dirs offline (verified scan) and find the command bytes.
- **Replica identity:** envelopes below HW are byte-identical across ISR members.
- **Standby identity:** `state_hash` and per-table logical digest are equal at equal `applied_offset`.
- **LA rule:** every epoch in the committed log starts with `LeaderAnnounced{e}` whose `node_id` equals meta's leader for e.
- **Election:** every `ElectLeader` in the meta log picked a member of the then-published ISR.
- **Object storage:** the remote chain reached from the head equals the committed prefix.
- **Status honesty:** `/admin` ISR equals the published ISR and is a subset of `{replicas whose measured durable_leo ≥ HW}`.

**Scenario list:** placement; ISR honesty (empty follower, slow follower, lagging follower re-joining); HW = min over ISR; stale-epoch fetch (both directions); divergence (leader dies or is isolated holding unreplicated writes; rejoin truncates, including object storage); failover ISR-only; offline when the whole ISR is dead (no unclean election), recovering when the last member returns; min_insync (append rejected, no acks, resume); hot standby identity; snapshot bootstrap (empty node and node behind `log_start`; crash at each install step); blob-before-commit; max-size envelope; separate timeouts (stalled disk read longer than `max_wait` still completes); batching on user partitions (≥ 2 envelopes per fetch under load); one path (RF=1 through the tracker); zombie leader (isolated, cannot commit, executors fenced).

## Multi-Node Test Harness Options (WSL, no Docker)

| Tier | How | Use for | Cost |
|------|-----|---------|------|
| **A. Pure model** | proptest-state-machine reference model vs `replica::*` | ISR/HW/divergence/election invariants, KIP-497 races | Seconds; default suite |
| **B. In-process cluster** (recommended bulk) | N `Node`s in one process, each with its own tempdir. Replication over **in-memory tonic channels** (`tokio::io::duplex` connector) wrapped in a `FaultLayer` (drop / delay / blackhole per directed link, per RPC). Meta = N `RaftDriver`s over `ChannelOutbox`-style links with the same fault layer. Shared LocalFs bucket dir. | Most conformance scenarios, zombie and partition cases, mutants | Sub-second scenarios; deterministic-enough (seeded fault schedule); runs in `cargo test` |
| **C. Multi-process loopback** | Spawn N `ostro server` processes (failpoint build from `target/failpoints`, as in 05-19). Each binds a distinct loopback IP (`127.<r>.<r>.{1,2,3}`, random per test, so parallel tests never collide on ports; Linux routes all of 127/8 on `lo`). Peers point at a **Rust TCP chaos proxy** in the test process (TLS passthrough). Real `kill -9`. | SC2 kill path, power-loss-ish follower crash (failpoint before fsync + kill), failover p99 (≥ 200 iterations, `#[ignore]`), cluster crash campaign | Minutes; `#[ignore]` + env-tuned iteration counts |

Recommend all three. Tier C reuses `ostro-crashtest` server spawning and the ack-log pattern from 05-19. Certificates: the harness generates one CA plus per-node certs with node SAN URIs (F13).

## Proposed Plan Breakdown (11 plans, 6 waves)

| Plan | Wave | Depends | Scope | Reqs |
|------|------|---------|-------|------|
| **12-01** Contracts, reservations, harness API | 1 | — | RESERVATIONS P12 block (if not landed pre-freeze); `cluster.proto`; `[replication]` config + `[[cluster.peers]]`; trait additions (signatures, `unimplemented` behind RF>1); meta command shapes (ElectLeader/AlterIsr/ResignLeader); harness trait + `ostro-repl-conformance` skeleton; **the only plan touching Cargo files**; module stubs pre-declared (as P8-D21 did) | all |
| **12-02** Conformance suite FIRST + mutants | 2 | 01 | All scenarios + oracles against the harness API; mutant failpoint names declared; red run recorded in SUMMARY; RF=1 scenarios green | CLUS-01 |
| **12-03** Pure algorithms (port) | 2 | 01 | `replica::{tracker, isr, watermark, divergence, failover}` + proptest-state-machine model; Chronik attribution headers + VENDORED.md rows | CLUS-02, 03 |
| **12-04** One log implementation | 3 | 03 | `Rf1Log` → `PartitionLog`; tracker-driven HW; `read_replica`, `append_replica`, roles, `set_follower_hw`, `reset_to`; F3 empty-log anchor (if Phase 8 has not landed it); F4 HW at open; single `publish_hw()` feeding the Phase 8 HwVector; all Phase 2 log suites green; `Rf1Log` grep gate | CLUS-02, 04 |
| **12-05** Meta cluster | 3 | 01, 03 | Static multi-voter Raft transport (`Outbox` over `MetaPeer` stream), Propose forwarding, new commands at apply, controller (liveness via `sample_live`, suspect reports, planner driver), node.toml identity binding (F6) | CLUS-03 |
| **12-06** Fetch service + fetcher | 4 | 04, 05 | Leader service (validation order, two timeouts, durable-gated reads, size limits, mTLS replica authz); follower loop (diverging/truncate, blobs-before-LEO, durable_leo, HW propagation); optional `replication_listen` | CLUS-02 |
| **12-07** Snapshot bootstrap | 4 | 04 | `FetchSnapshot` frame streaming + resume; install ordering + failpoints; replica services (snapshot/retention) in both roles | CLUS-04 |
| **12-08** Role lifecycle + server wiring | 5 | 05, 06, 07 | `Node` per-partition role transitions; meta-first bootstrap for RF=1 too; acks `NotLeader{leader}` → API 503/`UNAVAILABLE` with leader hint (forwarding is Phase 13); self-demotion on lost meta contact; worker sessions observe demotion | CLUS-03, 04 |
| **12-09** Bottomless in cluster mode | 5 | 04, 08 | Per-partition head + epoch fencing, takeover reconcile/repair, retention of unreachable chunks; object-storage oracle green | CLUS-02 |
| **12-10** Multi-process tier | 6 | 08 (09 for bucket checks) | Process harness, chaos proxy, SC2 kill scenario, failover p99 (≥ 200 iters, reports p50/p99/max + per-step breakdown), bytes-on-disk benchmark (RF=3 steps/s and p99 commit reported, not gated), cluster crash-campaign smoke | CLUS-04 |
| **12-11** Gate | 6 | all | Mutant matrix in ci-local + ci.yml; suite 100% green; docs (`docs/replication.md`, storage-format behavioural notes F3/F4, operations); RESERVATIONS flip; VALIDATION; cluster crash-campaign gate (QA-01) recorded | CLUS-01..04 |

File ownership per wave must not overlap. 12-04 alone owns `ostro-log/src` in wave 3, 12-05 alone owns `ostro-meta`, and so on, the same approach as 05/08.

## Open Questions

1. **Static multi-voter meta in Phase 12 (F14a).**
   - What we know: SC3/SC4 need a controller that survives a node loss. The driver supports multiple voters.
   - Unclear: whether the user accepts pulling the CLUS-05 transport forward.
   - Recommendation: yes. Static voters from `[[cluster.peers]]`, no join/remove. Record it as a ROADMAP note before planning.
2. **Cluster-wide bottomless restore.**
   - What we know: Phase 8's causal consistent cut relies on one node's `HwVector`. In a cluster, partitions on different nodes have no common instant.
   - Unclear: whether 1.0 needs whole-cluster restore (BACK-* are single node; PROJECT says "Bottomless single node").
   - Recommendation: Phase 12 delivers per-partition chain integrity plus divergence repair (CLUS-02) only. Defer the cross-node cut (for example, a meta-coordinated barrier or post-hoc outbox reconciliation) to Phase 13/14. Flag it in docs.
3. **Where `min_insync` lives** if F7 is rejected.
   - Recommendation: a fallback MetaRecord 24. Never server config: config drift across nodes would let one leader ack on fewer copies.
4. **Follower fetch pipelining.**
   - v1: fetch only after the previous batch is durable (`fetch_offset == durable_leo`).
   - The proto separates the two values, so pipelining can come later without a protocol change. Measure RF=3 p99 commit in 12-10 before optimising.
5. **Indeterminate client outcomes on failover.**
   - An append committed by the new leader after the old one failed the client's ack. Idempotency keys, `(epoch, attempt)` and signal `request_id` already make retries safe.
   - Document it in the API docs. No code change.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | cargo test (Rust 1.100), proptest 1.12 + proptest-state-machine 0.9, fail 0.5.1 failpoints |
| Config file | none new; `scripts/ci-local.sh` (`FAILPOINT_CRATES` gains `ostro-repl-conformance`, plus `ostro-cluster` if created), `.github/workflows/ci.yml` kept in sync |
| Quick run command | `cargo test -p ostro-log --lib replica:: --locked && cargo test -p ostro-repl-conformance --locked` |
| Full suite command | `cargo test -p ostro-repl-conformance --features failpoints --locked -- --test-threads=1 && bash scripts/ci-local.sh` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CLUS-01 | Suite exists, ran red before integration (SUMMARY evidence) and catches every mutant | integration + mutant matrix | `cargo test -p ostro-repl-conformance --features failpoints --test mutants --locked -- --test-threads=1` | ❌ Wave 0 |
| CLUS-02 | HW = min(durable LEO over maximal ISR); KIP-497 race | proptest model | `cargo test -p ostro-log --lib replica::watermark --locked` | ❌ Wave 0 |
| CLUS-02 | Stale-epoch fetch rejected (both directions) | integration (tier B) | `cargo test -p ostro-repl-conformance --test stale_epoch --locked` | ❌ Wave 0 |
| CLUS-02 | Divergent tail truncated on rejoin; replicas byte-identical below HW; remote chain == committed prefix | integration (B) + multi-process (C) | `cargo test -p ostro-repl-conformance --test divergence --locked`; `cargo test -p ostro-repl-conformance --test process_divergence -- --ignored` | ❌ Wave 0 |
| CLUS-02 | Fetch timeouts separated; max-size envelope; batching on user partitions | integration (B) | `cargo test -p ostro-repl-conformance --test fetch_path --locked` | ❌ Wave 0 |
| CLUS-03 | Election only from the published ISR; offline (no unclean election) when the ISR is dead | unit (meta apply) + integration | `cargo test -p ostro-meta --test elect_leader --locked && cargo test -p ostro-repl-conformance --test failover --locked` | ❌ Wave 0 |
| CLUS-03 | ISR < min_insync → appends rejected, zero acks, HW frozen; resume on rejoin | integration (B) | `cargo test -p ostro-repl-conformance --test min_insync --locked` | ❌ Wave 0 |
| CLUS-04 | Hot standby: equal state_hash/digest at equal applied offset | integration (B) | `cargo test -p ostro-repl-conformance --test standby --locked` | ❌ Wave 0 |
| CLUS-04 | Snapshot bootstrap (empty node, behind log_start, crash at each install step) | integration + failpoint kill loop | `cargo test -p ostro-repl-conformance --features failpoints --test bootstrap --locked -- --test-threads=1` | ❌ Wave 0 |
| CLUS-04 | Failover p99 < 10 s, runs resume (≥ 200 kill -9 iterations incl. controller kills) | multi-process measurement | `OSTRO_FAILOVER_ITERS=200 cargo test --release -p ostro-repl-conformance --test failover_p99 -- --ignored --nocapture` | ❌ Wave 0 |
| CLUS-04 | RF=1 uses the same code path | unit + grep gate | `cargo test -p ostro-repl-conformance --test one_path --locked && ! grep -rn "Rf1Log" crates/` | ❌ Wave 0 |
| (QA-01 gate) | Cluster crash campaign: 0 lost acked runs, 0 duplicate journal entries | multi-process campaign | `OSTRO_CAMPAIGN_FAULTS=… cargo test --release -p ostro-repl-conformance --test cluster_campaign -- --ignored` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** the quick run command (pure algorithms + tier-B suite, < 30 s target).
- **Per wave merge:** the full suite command, including the mutant matrix and `ci-local.sh`.
- **Phase gate:** full suite green, the failover p99 report (≥ 200 iterations) and the cluster crash-campaign report recorded in 12-VALIDATION.md, before `/gsd:verify-work`. The native-Linux numbers are human_needed (D15: WSL2 numbers are advisory).

### Wave 0 Gaps
- [ ] `crates/ostro-repl-conformance/` crate: harness traits, tier-B in-process cluster, oracles, mutant runner (12-01/12-02).
- [ ] `crates/ostro-log/src/replica/` module + proptest-state-machine model (12-03).
- [ ] `crates/ostro-meta/tests/elect_leader.rs` (12-05).
- [ ] Tier-C process harness + chaos proxy + test CA generator (12-10).
- [ ] `FAILPOINT_CRATES` and ci.yml failpoint step updated (12-01, kept in sync).

## Sources

### Primary (HIGH confidence)
- Repository code at `9a79ca8`: `crates/ostro-log/src/{replicated,rf1,node,hw,epoch_cache,recovery,barrier}.rs`, `crates/ostro-partition/src/{node,leadership,apply/{mod,cursor},leader/*}.rs`, `crates/ostro-meta/src/{records,command,state}.rs`, `vendor/ostro-raft-driver/src/driver.rs`, `vendor/ostro-wal/src/group_commit.rs` (durable-gated tail cache).
- `docs/04-chronik-reuse.md` §4, `docs/03-architecture.md` §3–4, `docs/storage-format.md`, `.planning/RESERVATIONS.md`, `05-DECISIONS.md` (D18 amended, D20), Phase 5 plans 05-07/05-12/05-19/05-20, Phase 8 `08-RESEARCH.md`, `08-DECISIONS.md`, plans 08-02/08-09/08-13/08-18, Phase 7 `07-RESEARCH.md`/`07-DECISIONS.md`, `04-VERIFICATION.md` (META-01 ruling).
- tonic `Grpc` docs (default `max_decoding_message_size` 4 MB): https://docs.rs/tonic/0.14.2/tonic/client/struct.Grpc.html

### Secondary (MEDIUM confidence)
- KIP-497 (AlterIsr, maximal ISR): https://cwiki.apache.org/confluence/display/KAFKA/KIP-497%3A+Add+inter-broker+API+to+alter+ISR
- KIP-320 (fetch fencing, CurrentLeaderEpoch): https://cwiki.apache.org/confluence/display/KAFKA/KIP-320:+Allow+fetchers+to+detect+and+handle+log+truncation
- KIP-595 fetch changes / DivergingEpoch in Fetch (KAFKA-10435, KAFKA-13141): https://issues.apache.org/jira/browse/KAFKA-13141 ; Fetch wire changelog https://kafka-options-explorer.conduktor.io/wire-format/Fetch/changelog/
- KIP-966 (ELR, maximal ISR caveat): https://cwiki.apache.org/confluence/spaces/KAFKA/pages/263426970/KIP-966+Eligible+Leader+Replicas
- `isFollowerInSync` epoch-start-offset condition (quoted in Kafka JIRA/mail threads; KAFKA-7128): https://issues.apache.org/jira/browse/KAFKA-7128
- KRaft broker heartbeat/session defaults (2 s / 9 s): https://kafka-options-explorer.conduktor.io/config/broker-session-timeout-ms/ , https://issues.apache.org/jira/browse/KAFKA-14392

### Tertiary (LOW confidence; validate during execution)
- turmoil 0.7 capabilities and limitations (gRPC example, host crash support open): https://docs.rs/turmoil/0.7.2 , https://github.com/tokio-rs/turmoil. Rejected here on architectural grounds, not tested.
- raft-rs `recent_active` behaviour right after a leader change (affects Pitfall 11). Verify in 12-05.
- Chronik internals (`isr_tracker`, `partition_failover`, `assignment_repair`) were **not read** (constraint). Their semantics are taken from docs/04 and Kafka. The executor ports from the upstream repository at `08715f4` with attribution, and must reconcile any difference with this document.

## Metadata

**Confidence breakdown:**
- Pre-freeze findings: HIGH. Each one is grounded in the current code and plan text (file and line evidence above).
- Standard stack: HIGH. No new crates; everything is pinned or vendored.
- Architecture: MEDIUM-HIGH. Kafka semantics are well documented; the Ostro-specific durable-LEO and min_insync freeze rules follow from the existing HW contract.
- Timing budget: MEDIUM. Derived from config defaults; must be measured (12-10).
- Pitfalls: HIGH for the code-derived ones (1, 3, 6, 7, 8, 9, 10, 12); MEDIUM for 11.

**Research date:** 2026-11-19
**Valid until:** 2026-12-19, or until 05-07/05-20 or Phase 8 execution changes the meta encoding, recovery rules or bottomless layout, whichever comes first.
