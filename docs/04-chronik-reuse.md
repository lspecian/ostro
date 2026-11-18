# 04 — Reusing Chronik Stream

> **Superseded in part (2026-11-18), after research of the Chronik source** (see `.planning/research/SUMMARY.md` and `ARCHITECTURE.md`):
> - **Vendor, don't depend.** The needed Chronik code is copied into Ostro with attribution and modernised there. Chronik is not modified, and there is no `chronik-core` extraction.
> - **Replication Option A is rejected.** Ostro ports Chronik's pure algorithms (epoch cache, ISR math, watermarks, failover planner) behind its own `ReplicatedLog` trait, which runs at RF=1 from Phase 1. Fetch is a small tonic service (Option B).
> - **Object store:** use the `object_store` crate, not Chronik's backends.
> - **WAL:** use V2 records only, carrying Ostro's own envelope `{epoch, codec_ver, crc32c, cmds}`. CRCs are verified on every read, because `verify_checksum()` skips V2. io_uring is disabled.
>
> The §4 lessons still apply in full.

Chronik Stream (github.com/lspecian/chronik-stream, Apache-2.0, same author) is a single-binary, Kafka-compatible event platform in Rust. It has already solved Ostro's hardest infrastructure problems: a durable WAL, Raft for metadata, follower-pull log replication with leader epochs, and object-store tiering. This document maps what to reuse, what needs extraction, and what Chronik's own history teaches.

*Based on a read of the Chronik `main` branch (workspace version 2.18.0) on 2026-11-18. Line counts and coupling are approximate.*

---

## 1. Component map

| Ostro need | Chronik source | Reuse mode | Notes |
|---|---|---|---|
| Metadata consensus | `crates/chronik-raft` — `RaftDriver` (event-driven, one OS thread owns `RawNode`), `StateMachine` trait (`apply`/`snapshot`/`restore`/`on_conf_change`), `RaftLogStore` (durable, CRC'd segments, torn-tail-only tolerance), gRPC + in-memory transports | **Direct dependency** | Depends only on `chronik-common`, `chronik-monitoring`, `chronik-raft-bridge` + vendored `raft`/`raft-proto` (tikv raft-rs 0.7, prost compat). Implement `ostro-meta` as a `StateMachine` |
| Partition journal | `crates/chronik-wal` — segments, `GroupCommitWal` (Postgres-style group commit, per-partition queues, backpressure), `truncate` (suffix truncation of divergent tails), `segment_index` (sparse offset → byte position), checkpointing, optional io_uring thread | **Direct dependency** (with a record-format decision, §3.1) | Depends on `chronik-common`, `chronik-monitoring` |
| Log replication | `chronik-server/src/replication/` (`leader_epoch.rs`, `replica_fetcher/`), `isr_tracker.rs`, `isr_ack_tracker.rs`, `isr_publisher.rs`, `partition_failover.rs`, `assignment_repair.rs` (~8.6k lines together) | **Extract** into `chronik-replication` | Currently lives inside the Kafka server and speaks Kafka Fetch (§3.2) |
| Cluster config & membership | `chronik-server/src/cluster/` (`[[peers]]` TOML, broker registration, listener setup) | **Pattern + partial code** | Ostro adopts the same `[[peers]]` config shape |
| Object storage | `chronik-storage/src/object_store/` (S3, GCS, Azure, local backends; retry; auth; metrics) | **Extract** into `chronik-object-store` | Today `chronik-storage` pulls in columnar, embeddings and protocol crates |
| Backup / DR | `crates/chronik-backup`, `metadata_dr.rs` | **Study / partial** | Inform bottomless mode |
| Kubernetes operator | `crates/chronik-operator` (kube-rs) | **Pattern** | Ostro's Helm chart first; operator post-1.0 |
| Replication conformance tests | RP-0 suite (placement, ISR honesty, divergence, failover), `tests/cluster` scripts | **Port the approach** | Most valuable artefact — see §4 |
| Monitoring | `crates/chronik-monitoring` | **Direct dependency** (transitively required) | Prometheus metrics plumbing |

## 2. What it buys

| Area | Without Chronik | With Chronik |
|---|---|---|
| Raft metadata | Choose/adopt `openraft` or raft-rs, write storage, transport, driver, proposal tracking | Implement one `StateMachine` |
| WAL | Design segments, CRC, group commit, recovery, truncation | Configure and wrap |
| Data replication | Design ISR, HW, epochs, truncation, failover; discover the edge cases | Extract and adapt a design that already went through those edge cases on real clusters |
| Object store | Write backends | Extract module |
| Test strategy | Invent | Port RP-0 conformance + cluster scripts |

Phase 4 drops from "build a replicated log" to "extract, integrate, and prove". It is still the riskiest phase, but no longer open-ended.

## 3. What needs extraction work

### 3.1 WAL record format

`WalRecord` is Kafka-shaped: V1 is a single record (`offset`, `timestamp`, `key`, `value`, `headers`, CRC); V2 is a `CanonicalRecord` batch. Options:

| Option | Effort | Verdict |
|---|---|---|
| Encode Ostro commands as `value` (key = run id) in V2 batches | Low | **Start here.** Zero WAL changes; headers carry the command type and format version |
| Add a generic `V3 { batch: Bytes, epoch, base_offset, crc }` envelope to `chronik-wal` | Medium | Do it if V2 batch overhead shows up in profiles; also benefits Chronik |

### 3.2 Replication protocol

Chronik's follower fetcher reuses the **Kafka Fetch** protocol ("a follower is a consumer with a `replica_id`"), which is exactly why it is well-tested: every consumer test exercises the replication read path.

| Option | Effort | Trade-off |
|---|---|---|
| **A. Speak Kafka Fetch internally** between Ostro nodes on the cluster listener, reusing the fetcher nearly as is | Low–medium | Brings a Kafka protocol dependency into Ostro's internals (not exposed to users). Keeps the battle-tested path |
| **B. Port the algorithm to a small gRPC `Fetch{partition, offset, epoch, replica_id}`** in a new `chronik-replication` crate that both products use | Medium–high | Cleaner and shared. Risks the "two paths" bug class unless Chronik itself moves onto the extracted crate |
| C. Raft per partition (multi-raft) | High | Rejected: Chronik's own Raft data-replication attempt failed on batching, and ISR is already proven |

**Recommendation: A for Phase 4 MVP, B as a follow-up once Chronik also consumes the extracted crate.** That way there is only ever one implementation in production.

### 3.3 Workspace strategy

Create a **`chronik-core` workspace** (separate repo, or a published crate set) containing `chronik-common`, `chronik-monitoring`, `chronik-wal`, `chronik-raft` (+ vendored raft/raft-proto, `chronik-raft-bridge`), `chronik-object-store`, and later `chronik-replication`. Chronik Stream and Ostro both depend on it. Until then, Ostro uses git dependencies pinned to a Chronik commit.

## 4. Lessons from Chronik's replication history (design rules for Ostro)

From `docs/ROADMAP_REPLICATION.md`:

| What happened in Chronik | Rule for Ostro |
|---|---|
| Push replication was wired for metadata but unreachable for user data when `acks != 0`; it went **undetected for ~9 months**. Live clusters ran 33 days with every partition on one node | **One replication path for everything, including RF=1 single node** (D5). Single node is just ISR = {self} |
| `/admin/status` reported `isr:[1,2,3]` with zero follower copies on disk ("ISR is fiction"; ISR derived from assignment, not acknowledgements) | **ISR is computed only from measured follower LEOs**, published to metadata by the leader. Status endpoints report measured values only |
| The Raft data-replication attempt measured 61 msg/s because batching was wired only for the internal topic | Benchmark the **user data path**, never the internal one; CI asserts batching on user partitions |
| An unclean election destroyed `acks=all`-acknowledged records; fixed by electing only from the published ISR | Elect only from ISR; `min_insync_replicas` enforced; unclean election not offered |
| Every fetch re-read the active segment from byte zero (RP-9); fixing it raised throughput 4× and exposed a data-loss-on-failover bug | Durable-gated tail cache + sparse index from day one (inherited from `chronik-wal`) |
| Client `max_wait_ms` used as a read deadline caused bimodal throughput collapse (RP-12) | Separate "wait for new data" from "read existing data" timeouts in the fetch loop |
| Sharing one address for client and replication traffic cost +31% throughput and −29% p99 (RP-10) | Optional dedicated replication address on the cluster listener |
| A benchmark "regression" was really the broker rejecting writes that the client counted as success (RP-11) | Benchmarks verify bytes on disk and acknowledged counts, not just rates |
| Conformance suite (RP-0) written **first**, failing before the fix and passing after | **Write Ostro's replication conformance suite before integrating replication** (Phase 4 week 1) |

## 5. Relationship options: separate product vs Chronik capability

| Option | Description | Pros | Cons |
|---|---|---|---|
| **1. Separate product on shared crates** (recommended) | Ostro is its own binary built on `chronik-core` | Clean positioning ("durable workflows, one binary"); small binary (no DataFusion/Tantivy/HNSW); independent release cadence | Two products to maintain; needs the crate extraction |
| 2. Ostro as a Chronik capability | `chronik` gains workflow execution; journals are Chronik topics | One binary does everything; journals instantly queryable via SQL/search | Big binary and a confusing pitch ("Kafka + search + vectors + workflows"); Kafka semantics leak into the workflow design |
| 3. Ostro on top of Chronik as a client | Ostro stores journals in a Chronik cluster over Kafka protocol | Fastest replication story | Breaks the "one binary" moat (now two systems) |

**Recommendation: Option 1**, with integrations rather than merging:
- **Triggers:** consume a Chronik/Kafka topic to start or signal workflows (FR-3.10).
- **Effects:** produce to Chronik topics (FR-4.6).
- **Analytics:** optionally mirror run journals to a Chronik topic with `columnar.enabled=true` for SQL over workflow history.

If you name the product **Chronflow**, Option 1 still applies; the shared name just signals the family.
