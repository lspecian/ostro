# 03 — Technical Architecture

- [1. Topology & node roles](#1-topology--node-roles)
- [2. Data model: partitions, commands, state machines](#2-data-model-partitions-commands-state-machines)
- [3. Storage layout](#3-storage-layout)
- [4. Replication & cluster metadata](#4-replication--cluster-metadata)
- [5. Execution: dispatcher, executor, workers](#5-execution-dispatcher-executor-workers)
- [6. Unified WIT interface specification](#6-unified-wit-interface-specification)
- [7. The suspend/resume lifecycle](#7-the-suspendresume-lifecycle)
- [8. Placement & spillover](#8-placement--spillover)
- [9. Security & multi-tenancy](#9-security--multi-tenancy)
- [10. Observability](#10-observability)
- [11. Code organisation](#11-code-organisation)

---

## 1. Topology & node roles

### 1.1 One binary, three shapes

```mermaid
flowchart TB
  subgraph A["Day 1: ostro server (single node)"]
    direction LR
    API1[API :7700<br/>HTTP · gRPC · webhooks · UI]
    META1[Metadata<br/>Raft group of 1]
    P1[Partitions 0..15<br/>leader = self, RF=1]
    EX1[Embedded executor<br/>Wasmtime + I/O bridge]
    API1 --> P1
    P1 <--> EX1
    META1 --- P1
  end

  subgraph B["Day 30: + workers, + S3"]
    direction LR
    S2[ostro server] -- "gRPC Execute stream" --> W2a[ostro worker]
    S2 -- "gRPC Execute stream" --> W2b[ostro worker]
    S2 -. "sealed segments + snapshots" .-> OBJ2[(S3)]
  end

  subgraph C["Day 100: 3-node cluster + autoscaled workers"]
    direction LR
    N1[ostro server n1] <-- "Raft (metadata)<br/>follower-pull (partitions)" --> N2[ostro server n2]
    N2 <--> N3[ostro server n3]
    N1 <--> N3
    N1 & N2 & N3 -- Execute --> WK["ostro worker ×N<br/>(KEDA)"]
    N1 & N2 & N3 -.-> OBJ3[(S3)]
  end
```

### 1.2 Inside a server node

```
┌──────────────────────────────────── ostro server ────────────────────────────────────┐
│  Client listener :7700  (HTTP/1.1 + HTTP/2 + gRPC, TLS)   ─ routes by run-id/partition│
│     │  forwards to partition leader if not local                                      │
│  ┌──▼──────────────────────────────────────────────────────────────────────────────┐  │
│  │ Partition replicas (one per assigned partition)                                 │  │
│  │   journal log (chronik-wal: segments, group commit)                             │  │
│  │   state machine  ── apply(committed entries) ──► fjall state store              │  │
│  │   if leader:  dispatcher · ready queues · timer wheel · effect index            │  │
│  └──▲───────────────────────────────▲──────────────────────────────────────────────┘  │
│     │ Activate / Append / Ack       │ follower fetch (replication)                    │
│  ┌──┴──────────────────────┐   ┌────┴────────────────────────────┐                    │
│  │ Embedded executor       │   │ Cluster listener :7701          │                    │
│  │  Wasmtime engine        │   │  Raft (metadata, chronik-raft)  │                    │
│  │  module cache (AOT)     │   │  replication fetch (opt. own    │                    │
│  │  I/O bridge (Tokio)     │   │   address — Chronik RP-10)      │                    │
│  └─────────────────────────┘   └─────────────────────────────────┘                    │
│  Metadata replica (Raft state machine) · blob store (modules) · object-store uploader │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

A **worker** is the "Embedded executor" box alone, connected over the network.

### 1.3 Process model

- **Tokio** multi-thread runtime for network, I/O bridge, replication.
- **Raft driver thread** (one OS thread owns `RawNode`, as in `chronik-raft::driver`).
- **Wasm executor pool**: K threads running Wasmtime fibers (`async_support`), epoch ticker on Tokio.
- **Partition apply**: one task per partition replica; state store writes are batched per apply chunk.

---

## 2. Data model: partitions, commands, state machines

### 2.1 Partitioning

- A fixed number of partitions **P** (default 16 single-node, 64 cluster), set at creation.
- **Run ids embed their partition**: `run_id = base32(partition:u16 ‖ uuidv7)`, so any node routes a run-id request without lookup.
- New runs: `partition = hash(workflow, idempotency_key)` if a key is given (so deduplication is a local lookup), else `hash(uuidv7)`.
- Each partition has exactly **one leader** (single writer). All mutations of a run go through its partition's log.

### 2.2 Log commands (partition journal)

Every state change is a command appended to the partition log and applied deterministically by every replica. The frozen encodings (field numbers, enums, goldens) are in [storage-format.md](storage-format.md) §4, §12 and §13; tags come only from `.planning/RESERVATIONS.md`.

| Tag | Command | Appended by | Effect on state |
|---|---|---|---|
| 1 | `LeaderAnnounced{epoch, node_id}` | new partition leader | leader epoch boundary; fences older epochs |
| 2 | `StartRun{run, workflow, version, module_sha, input, idempotency_key, …, run_started}` | API (via leader), outbox (child, continue-as-new successor) | create run, dedupe index, ready entry, run-timeout / schedule-to-start / start-delay timers. With D24 the API pre-encodes `RunStarted` (field 23) and apply journals it at seq 0 |
| 3 | `AttemptGranted{run, token}` | dispatcher | new attempt token (fencing), `running` row |
| 4 | `EventsAppended{run, token, from_seq, events[], derived[]}` | executor (via leader) | append journal events; derived intents: timers, outbox messages, ready, waits, `CompleteRun`, `Suspend` |
| 5 | `TimerFired{run, op, due_at, fired_at}` | leader timer service | journal `timer_fired` (guest timers), `wait_timed_out` (wait timeouts), or route a reserved op (below) |
| 6 | `InboundBatch{src, msgs[]}` | shuffler (other partitions' outboxes) | child start / child completed / signal / cancel / continue-as-new start, exactly once per source seq |
| 7 | `OutboxTruncated{target, upto_seq}` | shuffler | drop delivered outbox rows |
| 8 | `SignalRun{run, name, event, request_id}` | API | journal `SignalPayload` verbatim (D21); match the oldest open wait (`SignalMatched`) or buffer in `inbox`; `request_id` dedupe (`signal_seen`) |
| 9 | `RequestCancel{run, reason}` | API | journal `CancelRequested` once and wake the run (graceful, D11); forwarded to attached children and continue-as-new successors |
| 10 | `TerminateRun{run, reason}` | API | hard stop → `Terminated`, `RunTerminated{reason}` marker, children get a graceful cancel (D3) |
| 11 | `RetryRun{run}` | API | resume a `resumable` terminal run in place (D1); the restart mode is a new `StartRun` with `retry_of` |
| 12 | `CompleteToken{run, op, event}` | API | journal `TokenCompleted` verbatim (D21), first completion wins |

Phase 5 replaced two design-era commands. **There is no batch `RunsExpired` command**: retention is a per-run reserved timer (`EXPIRE_OP`, below) armed at the terminal transition, so expiry is an ordinary `TimerFired` that deletes one run's rows and its dedupe key. **There is no `CancelRequested` command with a mode**: graceful cancel is `RequestCancel` (tag 9) and the hard stop is `TerminateRun` (tag 10). Run completion is the `CompleteRun` intent inside `EventsAppended`, not a separate command. Cron (`ScheduleFired`) arrives in Phase 8 as the outbox `CronStart` message with the dedupe key `"__cron/<schedule>"` ‖ fire_time_ms (storage-format §6).

**Reserved timer ops.** Ops `u64::MAX - 15 ..= u64::MAX` are owned by apply and never allocated to guest operations (a guest intent naming one is rejected `BadIntent`): `START_DELAY_OP` (`u64::MAX`, delayed start), `RUN_TIMEOUT_OP` (`u64::MAX - 1`, hard stop → `TimedOut`, reason `run_timeout`), `SCHEDULE_TO_START_OP` (`u64::MAX - 2`, initial start only, reason `schedule_to_start`) and `EXPIRE_OP` (`u64::MAX - 3`, retention expiry). `TimerFired` routes them before the terminal-run check, because expiry acts on terminal runs.

Leases are **soft state** on the leader (not logged). Fencing: `EventsAppended` carries the attempt token and `from_seq`; apply rejects it unless the token is the run's current attempt and `from_seq == run.next_seq`. A new leader re-grants every run that was in flight in its new epoch, so zombie executors are fenced after failover. The `from_seq` check is also the compare-and-set that keeps a `Suspend` from losing a wake-up.

### 2.3 Journal event kinds (inside `EventsAppended`)

| Kind | Meaning |
|---|---|
| `activation_started` | attempt number, executor id, logical start time |
| `step_completed` / `step_failed` | step result / failure + attempt |
| `effect_scheduled` / `effect_completed` | outbox intent (request redacted, idempotency key) / response |
| `timer_started` / `timer_fired` | durable sleep |
| `signal_consumed` | payload delivered to a `wait-signal` |
| `child_started` / `child_completed` | child workflow lifecycle |
| `wait_resolved` | **ordered** list of op-ids returned by `wait` (determinism anchor for concurrency) |
| `clock_read` / `patch_marker` | logical time observations / versioning markers |
| `run_completed` / `run_failed` | terminal |
| `wait_timed_out` | a signal / token wait's timeout expired (input, journaled by apply; D4) |
| `run_terminated` | terminate or timeout marker, `reason` = operator reason, `run_timeout` or `schedule_to_start` (D3) |
| `signal_payload` / `signal_matched` | a signal's payload (journaled on arrival) / its match to a wait op, pointing back at the payload (D21; replaces `signal_consumed`, and `signal_received` is no longer produced) |
| `token_completed` / `cancel_requested` / `run_continued_as_new` | await-token completion / graceful cancel request / continue-as-new terminal |

The current kind numbers and payloads are defined in `crates/ostro-types/src/journal.rs`; the REST history uses the snake_case names above.

### 2.4 Partition state (fjall keyspaces)

One fjall database per partition at `state/pNNNN/`, one keyspace per table (fjall replaced redb after spike S3). The table below is the design view; the frozen key and value encodings (16 keyspaces `meta` … `signal_seen`, big-endian keys, postcard values, meta keys) are in [storage-format.md](storage-format.md) §6 and §13.

| Table | Key → Value | Purpose |
|---|---|---|
| `runs` | run_id → `RunMeta{status, workflow, version, module_sha, queue, attempt, next_seq, tenant, timestamps}` | run index |
| `events` | (run_id, seq) → event | per-run journal for replay (log segments can be truncated after snapshot) |
| `payloads` | (run_id, kind) → bytes | input/output; large values offloaded to blob/object store |
| `ready` | (queue, priority, ready_at, run_id) → () | dispatcher queue (rebuilt into memory on leadership) |
| `timers` | (due_at, run_id, op_id) → () | timer wheel source: guest timers, wait timeouts and the reserved ops (§2.2) |
| `effects` | (run_id, op_id) → status | in-flight effects (observability; replay re-drives them) |
| `inbox` | (run_id, name, n) → `SignalSlot` | **only unmatched signals**: a signal that arrives while a wait for its name is open is matched at once and never buffered. The slot points at the already journaled `SignalPayload` (`payload_seq`), or holds the event bytes while the journal is still empty (B1) |
| `waits` | (run_id, op) → `WaitRow`; (run_id, name, op) → () | open signal / token waits and the per-name FIFO index (oldest wait first) |
| `signal_seen` | (run_id, request_id) → () | signal dedupe (D13), deleted with the run |
| `running` | run_id → attempt token | exactly the runs in `Running`; a new leader rebuilds its in-flight set from it |
| `dedupe` | (qualified workflow, idem_key) → run_id | start idempotency (tenant-scoped through the qualified name, D23); freed when the run expires |
| `dlq` | (failed_at, run_id) → reason | dead letters |
| `meta` | `applied_offset`, `leader_epoch_history`, format version | recovery |

**Inputs are journaled by apply.** Timer firings, wait timeouts, signal payloads and matches, token completions and cancel requests are written into `events` by apply at the run's `next_seq` (moving it by one), then the run is woken: `Scheduled` → `Ready`, `Running` → `InputAppended` to the hot activation. The executor never invents inputs, so live execution and replay see the same order. Events carrying user-sized data are encoded by the submitter and journaled verbatim (D21); apply never decodes payloads.

The state store is derived: deleting it and replaying the log (or restoring a snapshot + log tail) must reproduce it byte-for-byte. A **state-hash check** (rolling hash of applied commands) compared across replicas detects apply divergence.

---

## 3. Storage layout

The single-node data directory as it exists today (`ostro dev` uses `./.ostro`, `ostro server` `./ostro-data`; [storage-format.md](storage-format.md) §1 is authoritative):

```
<data>/
├── node.toml                  # format_version = 1, partitions = P (Phase 12/13 add node_id, cluster_id)
├── meta/                      # metadata Raft log + snapshot (chronik-raft RaftLogStore)
│   ├── state                  # HardState + ConfState (tmp+fsync+rename)
│   ├── snapshot
│   └── log/<index>.seg        # [len u32][crc32 u32][entry]
├── wal/p/<id>/wal_<id>_<seg>.log   # partition <id>'s log (vendored ostro-wal, one shared group-commit
│                              #   WAL per node, topic `p`); one OSTR envelope per record, leader epoch in
│                              #   every envelope; e.g. wal/p/7/wal_7_0.log
├── state/pNNNN/               # partition NNNN's state machine: one fjall database (17 keyspaces incl. Phase 8 schedule_state)
│                              #   (+ transient .install-pNNNN / .old-pNNNN during a snapshot install)
├── snapshots/pNNNN/<applied 20d>.osnap   # local OSNP v1 snapshots (+ .blobrefs), keep 2 (Phase 8)
├── backup/gc-ledger           # remote blob deletion ledger (only with bottomless on, Phase 8)
├── blobs/<hh>/<sha256>        # content-addressed: uploaded components and payloads > 256 KiB
├── cache/aot/                 # Wasmtime AOT artefacts (rebuildable; HMAC-keyed)
└── tmp/                       # in-flight blob writes (renamed into blobs/ when complete)
```

Retention expiry (§2.2, `EXPIRE_OP`) deletes a run from `state/` only. Since Phase 8 the log is trimmed behind local snapshots: retention deletes whole WAL segments below `min(durable checkpoint, oldest retained snapshot, upload floor if bottomless is on)`, so `log_start ≤ applied_offset(oldest retained snapshot)` always holds, and a partition whose state is lost reinstalls the newest usable local snapshot plus the log tail (or fails with "snapshot required: run `ostro restore`"). Blobs no longer referenced (expired runs) are removed by blob GC (mark/sweep with a 1 h grace; module blobs are kept). Formats: [storage-format.md](storage-format.md) §15, [snapshot-format.md](snapshot-format.md). Node open, backup/restore and blob GC ignore unknown data-dir entries (for example a future `<data>/audit/`).

The frozen Phase 2 on-disk format (directory layout, WAL record and envelope bytes, command field numbers, keyspaces and key encodings, outcome codes, rolling hash and digest, recovery rules) is specified in [storage-format.md](storage-format.md). The leader-epoch history is not stored separately: recovery rebuilds it from the epochs in the envelopes.

**Bottomless (D9, Phase 8):** one node-level uploader (leader services only) takes an HwVector consistent cut every `seal_interval` (default 1 s) and uploads blobs, then per-partition **uploader-sealed OSEG v1 chunks** of the raw WAL envelopes (hash-chained, split at epoch boundaries; the WAL's own segment files are never uploaded), the **logical OSNP v1 partition snapshots** the local snapshot service already wrote (fjall directories are never copied), a metadata snapshot, and finally `manifest/current` under prefix fencing (`LOCK` + conditional PUT). The object stores are the in-house `ostro-objstore` crate (S3-compatible on rusty-s3, and a durable local filesystem backend; D19). It supersedes the earlier plan to extract a Chronik object-store crate; Azure Blob is deferred until a native backend exists. `ostro restore --from s3://…|file://…|backup.tar` rebuilds a node: manifest → `node.toml` with a new incarnation → native Raft install of the metadata snapshot → blobs → per partition: OSNP install, chunk-chain verification, two-step verified replay up to the cut. Details: [bottomless.md](bottomless.md).

---

## 4. Replication & cluster metadata

The model is Kafka-with-KRaft as implemented in Chronik Stream: **Raft for metadata, follower-pull ISR replication for data logs.** Details on which code is reused: [04](04-chronik-reuse.md).

### 4.1 Metadata (Raft)

A single Raft group (members = server nodes; 1, 3 or 5) driven by `chronik-raft`'s `RaftDriver` with a `StateMachine` implementation:

| Metadata | Contents |
|---|---|
| Membership | nodes, addresses (client, cluster, optional replication), roles, versions |
| Partition map | per partition: replicas, **leader**, **leader epoch**, **ISR** (published by leaders, only the Raft leader writes assignments — Chronik RP-7) |
| Modules | sha256, language, size, manifest, holders |
| Workflows | name/version → module sha, active version, default queue |
| Queues | executor policy, concurrency, rate limits, spillover thresholds |
| Schedules | cron definitions (firing is done by the owning partition) |
| Security | API token hashes + scopes, tenants + quotas, sealed secrets, secret grants, connections, egress policies |

Proposals return the state machine's result (validated at apply), and retrying an `OutcomeUnknown` proposal is safe — semantics inherited from the Chronik driver.

### 4.2 Partition logs (follower-pull ISR)

- **"A follower is a consumer with a replica id"** (Chronik's principle): followers fetch from the leader using the same read path as executors/history readers.
- Leader tracks each follower's log end offset (LEO); **high watermark HW = min(LEO over ISR)**. Commands are *committed* when HW passes them. Single node: ISR = {self}, HW = fsynced LEO — **same code path** (D5).
- Every batch is stamped with the **leader epoch**. A returning replica asks "where did epoch *e* end?" and truncates its divergent tail (Kafka KIP-101/279 semantics, as in `chronik-server::replication::leader_epoch` + `chronik-wal::truncate`).
- **Failover:** metadata leader elects the new partition leader **only from the ISR** recorded in metadata. The new leader: waits until its state store has applied up to its LEO, bumps attempts of in-flight runs, rebuilds ready queue + timer wheel from state, starts dispatching.
- Followers **apply committed entries** to their own `state/pNNNN` fjall store (hot standby), so failover does not need a replay from scratch.
- `min_insync_replicas` (default 2 of 3): if the ISR shrinks below it, the partition stops accepting writes (availability traded for durability, explicitly).

### 4.3 Commit semantics exposed to the engine

| Operation | Returns when |
|---|---|
| API `start run` | `RunStarted` committed (HW) |
| Executor `append` before an effect | `effect_scheduled` committed — **only then** may I/O happen |
| Executor `append` after a step/effect | committed before the guest is resumed past it |
| Signal | `SignalReceived` committed |

---

## 5. Execution: dispatcher, executor, workers

### 5.1 Dispatcher (per partition leader)

- In-memory ready queues per (queue, priority) and a hierarchical timer wheel — rebuilt from `ready`/`timers` on leadership. No polling.
- Assigns **activations** to executor slots honouring queue concurrency, tenant quotas, rate limits and placement policy (§8).
- Tracks soft leases: if an executor misses heartbeats for `lease_ttl`, the run is re-dispatched with `attempt + 1`.

### 5.2 Executor (embedded or worker — same crate)

```
Activate{run, attempt, module_sha, journal (or cursor), input}
  → module cache: L2 (InstancePre in memory) → L1 (AOT .cwasm on disk) → fetch blob by sha + compile
  → instantiate (pooling allocator, CoW memory image)
  → workflows.run(name, input)
  → replay: each guest command compared with journal[seq] (kind + command hash)
       match → return recorded result      mismatch → NonDeterminism → DLQ
  → live: new commands batched into Append{run, attempt, from_seq, events}
       effects only after their effect_scheduled is committed
  → stop on completion / failure / long wait (evict) / memory pressure (evict)
```

- **Hot wait** (default ≤ 2 s per queue): keep the instance resident while short effects complete; otherwise **evict** (drop the call future and Store) and let the timer/signal/effect completion re-dispatch the run, which replays.
- **In-flight effect recovery** (Phase 6) has no sweeper that re-sends from the journal, but it does need a wake. A re-dispatched run replays to an `effect_scheduled` without `effect_completed` and re-drives it with the **same idempotency key** (HTTP) or the same token (exactly-once SQL). A run that evicted on a hot-wait timeout with an effect in flight is not due anywhere, so a crash would strand it. Every in-flight effect therefore has an `effects` row (keyspace #12, `Intent::RegisterEffect`), and an accepted `LeaderAnnounced` gives every `Scheduled` run that still has one a ready row: runs suspended on in-flight effects are woken on leader start through their effects rows. Completions arrive only as `Cmd::CompleteEffect` (attempt-independent, first wins). See [connectors.md](connectors.md) §1.
- **`EffectDriver` seam.** The engine never performs I/O itself. `ActivationSink::effect_driver()` (default `None`) hands it an `EffectDriver`; the engine calls `start` only after the append carrying `EffectScheduled` has committed, and again with `redrive = true` from replay. On server nodes the driver is the in-process bridge (`ServerEffectDriver` over `ostro-bridge`). On a worker (Phase 7, D-W3) the driver is a proxy: `StartEffect` asks the leader, which verifies the request against the journal and runs the same `ServerEffectDriver`, so secrets never reach a worker (§5.3).

### 5.3 Worker protocol (gRPC `ostro.worker.v1`, one control stream + bulk RPCs per server node)

As shipped in Phase 7 (D-W2, D-W3; details in [workers.md](workers.md)). Everything runs on one mTLS h2 connection to the cluster listener (7701), so a bulk transfer never delays a heartbeat.

```
Execute.Session (bidirectional control stream)
worker → server                                  server → worker
Hello{worker_id, proto_version, ostro_version,   Welcome{session_id, lease_ttl, heartbeat,
      wasmtime_version, event_codec, wit,                max_frame_bytes, engine limits, members}
      labels, slots, memory}                     GoAway{reason}   (version mismatch, revoke, shutdown)
Credit{free_slots}                               Activate{run, token, module_sha, journal_len, …}
Heartbeat{leases, free_slots, memory_used}       Input{run, token, up_to_seq}
Done{run, token, outcome}                        Cancel{run, token} | CancelEffect{run, op}
GuestLogs{…}   Drain{}

Bulk RPCs (each checks peer certificate → session → lease of (run, token))
FetchJournal{run, token, from, to}   → journal pages (≤ max_frame_bytes)
FetchBlob{run, token, sha, offset}   → chunks (the run's module and the blobs its journal references)
Append(stream of frames)             → AppendResult{next | fenced | conflict_expected | paused | rejected}
StartEffect{token, op, redrive, redacted header values, http_retry} → started | rejected
Renew{csr}                           → new client certificate
```

- The leader stays authoritative: it decodes every appended event and writes it through the run's `PartitionSink` (the same path as an embedded activation), re-mints await tokens with its own key (D-W27), and maps `Done` through the embedded outcome mapper.
- **Effects are bridged back to the leader** instead of a `ResolveSecret` RPC: the leader checks the lease, loads the committed `EffectScheduled(op)`, checks the redacted header names against the journal copy, rebuilds the `LiveEffect` from the journal and runs the run's `ServerEffectDriver` (grants, egress, per-(run, attempt) secret resolution). Secret values never cross the stream.
- Workers join with a `wkr_` token over the client listener (`POST /v1/workers/join`, CA pinned by `--ca-hash`) and get a 24 h client certificate; they learn the cluster endpoints from the join response and `Welcome.members`. Phase 7 keeps one session to the current endpoint; connecting to every node at once comes with Phase 12/13.
- Workers are stateless apart from the AOT module cache. A killed worker's stream end abandons its jobs at once (re-grant, no failure counted); a stalled one loses its leases after `lease_ttl` (30 s default) and its late messages are fenced.

### 5.4 Resource governance

| Limit | Mechanism | On breach |
|---|---|---|
| Memory | `StoreLimits`, pooling slots, node budget with LRU eviction | `memory_exceeded` → DLQ |
| CPU | Fuel per step/run; epoch slices with async yield | `fuel_exhausted` (retry once) / `timeout` |
| Concurrency | per queue, per tenant, per node slots | backpressure / spill to workers |
| Journal size | max events per run (50k), max inline payload (256 KiB → blob) | warn at 80%, then fail with `continue_as_new` hint |
| Children | max per run, max depth 8 | fail the start |

---

## 6. Unified WIT interface specification

Design principles (unchanged from pgflow): the guest asks, the host decides; non-blocking op handles plus one deterministic `wait`; suspension is invisible to the guest; steps are a guest construct with host bookkeeping; a forward path to WASI 0.3 native async.

```wit
package ostro:runtime@0.1.0;

interface types {
    record payload { content-type: string, data: list<u8> }
    type op-id = u64;
    record datetime { seconds: u64, nanoseconds: u32 }
    record error-info { kind: string, message: string, details: option<payload> }

    variant failure {
        retryable(error-info),
        fatal(error-info),
        timeout,
        cancelled,
    }

    record retry-policy {
        max-attempts: u32,
        initial-interval-ms: u64,
        max-interval-ms: u64,
        backoff-coefficient: f64,
        non-retryable-kinds: list<string>,
    }

    record step-options {
        retry: option<retry-policy>,
        timeout-ms: option<u64>,
        fuel: option<u64>,
    }

    record run-info {
        run-id: string,
        workflow: string,
        workflow-version: u32,
        attempt: u32,
        module-hash: string,
        tenant: option<string>,
        parent-run-id: option<string>,
    }
}

/// Deterministic ambient context (journaled or derived from journaled data).
interface context {
    use types.{run-info, datetime};
    info: func() -> run-info;
    now: func() -> datetime;
    random-u64: func() -> u64;
    random-bytes: func(len: u32) -> list<u8>;
    uuid-v7: func() -> string;
    /// For SDK log suppression only; workflow logic must not branch on it.
    is-replaying: func() -> bool;
    patched: func(change-id: string) -> bool;
}

/// Step bookkeeping for guest-side computation.
interface journal {
    use types.{payload, step-options, failure};

    variant completion-ack {
        recorded,
        /// Short-backoff retryable failure: run the body again now.
        /// (Long backoffs suspend the run; the call never returns in this activation.)
        retry-now,
    }

    resource step {
        seq: func() -> u64;
        complete: func(outcome: result<payload, failure>) -> completion-ack;
    }

    variant step-start {
        replayed(result<payload, failure>),
        execute(step),
    }

    begin-step: func(name: string, opts: step-options) -> step-start;
}

/// Durable side effects executed by the host I/O bridge.
interface effects {
    use types.{payload, op-id, datetime, failure, retry-policy};

    record header {
        name: string,
        /// May contain {{secret:<name>}}; resolved by the host, never visible to the guest.
        value: string,
    }

    record http-request {
        method: string,
        url: string,
        headers: list<header>,
        body: option<list<u8>>,
        timeout-ms: option<u64>,
        idempotency-key: option<string>,
        retry: option<retry-policy>,
        /// JSON pointers in the response body to journal as hashes.
        redact: list<string>,
    }

    record http-response { status: u16, headers: list<header>, body: list<u8> }

    variant sql-value {
        null,
        boolean(bool),
        int8(s64),
        float8(f64),
        numeric(string),
        text(string),
        bytea(list<u8>),
        json(string),
        timestamptz(datetime),
    }

    record sql-request {
        /// Name of a connection registered in Ostro (DSN + secret refs live server-side).
        connection: string,
        statement: string,
        params: list<sql-value>,
        /// Wrap in a transaction with an ostro_effects idempotency token (exactly-once).
        exactly-once: bool,
    }

    record sql-result {
        columns: list<string>,
        rows: list<list<sql-value>>,
        rows-affected: u64,
    }

    record object-request {
        connection: string,
        op: string,          // "get" | "put" | "presign-get" | "presign-put"
        key: string,
        body: option<list<u8>>,
    }

    record child-options {
        queue: option<string>,
        version: option<u32>,
        detached: bool,
    }

    variant op-output {
        http(http-response),
        sql(sql-result),
        object(list<u8>),
        timer,
        signal(payload),
        child(payload),
    }

    record completion { op: op-id, outcome: result<op-output, failure> }

    enum wait-mode { all, any }

    http-fetch: func(req: http-request) -> op-id;
    sql-query: func(req: sql-request) -> op-id;
    object-store: func(req: object-request) -> op-id;
    sleep: func(duration-ms: u64) -> op-id;
    sleep-until: func(at: datetime) -> op-id;
    wait-signal: func(name: string, timeout-ms: option<u64>) -> op-id;
    start-child: func(workflow: string, input: payload, opts: child-options) -> op-id;
    cancel-op: func(op: op-id);

    /// Completions are returned in journaled order — identical on every replay.
    wait: func(ops: list<op-id>, mode: wait-mode) -> list<completion>;
}

interface log {
    enum level { trace, debug, info, warn, error }
    emit: func(level: level, message: string, fields: list<tuple<string, string>>);
}

interface workflows {
    use types.{payload, failure};
    record workflow-meta { name: string, version: u32, default-queue: option<string> }
    describe: func() -> list<workflow-meta>;
    run: func(name: string, input: payload) -> result<payload, failure>;
}

world workflow {
    import context;
    import journal;
    import effects;
    import log;
    export workflows;
}

// The host also satisfies the WASI 0.2 imports pulled in by language runtimes,
// with deterministic, capability-free implementations:
//   wasi:clocks/*  -> journaled logical time      wasi:random/* -> seeded ChaCha20
//   wasi:cli/env   -> empty / manifest vars       stdout/stderr -> log
//   wasi:filesystem -> none or read-only in-memory
//   wasi:sockets, wasi:http -> not provided (deploy-time validation rejects them)
```

**SDK mapping:** Rust (`wit_bindgen::generate!`, mini executor, `join!`), Python (componentize-py bindings, custom `asyncio` loop, `gather`), TS (`jco` types, job loop, `Promise.all`). **Delivery rule:** SDK loops resolve `wait` completions one at a time in the given order and drain the language's ready/microtask queue between them.

**Forward path (`ostro:runtime@0.2.0`, WASI 0.3):** effects become `async func … -> result<…>`; the host still journals completion order.

---

## 7. The suspend/resume lifecycle

### 7.1 Scenario (Python; TS identical apart from the guest loop)

```python
@workflow(name="enrich-lead", version=3)
async def enrich_lead(lead: dict) -> dict:
    profile = await http.get(f"https://api.enrich.example/v2/people?email={lead['email']}",
                             headers={"Authorization": "Bearer {{secret:enrich}}"})
    await ctx.sql("crm", "UPDATE leads SET profile=$1 WHERE id=$2",
                  [profile.json(), lead["id"]], exactly_once=True)
    await ctx.sleep(hours=24)
    return {"ok": True}
```

### 7.2 Sequence

```mermaid
sequenceDiagram
  autonumber
  participant C as Client / webhook
  participant L as Partition leader (log + state + dispatcher)
  participant F as Followers (cluster only)
  participant E as Executor (embedded or worker)
  participant G as Guest (CPython in Wasm)
  participant B as I/O bridge (Tokio)
  participant X as External API / CRM DB

  C->>L: POST /v1/workflows/enrich-lead/runs
  L->>L: append RunStarted (group commit, fsync)
  L-->>F: followers fetch; HW advances
  L-->>C: 202 {run_id}  (after commit)
  L->>E: Activate{run, attempt=1, journal=[]}
  E->>E: InstancePre hit → instantiate (pooling, CoW)
  E->>G: workflows.run("enrich-lead", input)
  G->>E: effects.http-fetch(req) → op0 ; effects.wait([op0])
  E->>L: Append{attempt 1, seq0 activation_started, seq1 effect_scheduled(op0, idem key)}
  L-->>E: AppendAck(committed seq1)
  E->>B: perform op0 (fiber parked; thread serves other runs)
  B->>L: ResolveSecret("enrich") — local call, or mTLS from a worker
  B->>X: GET … Authorization + Idempotency-Key
  X-->>B: 200 {profile}
  E->>L: Append{seq2 effect_completed(op0), seq3 wait_resolved[op0]}
  L-->>E: AppendAck
  E->>G: wait returns — fiber resumes
  G->>E: sql-query(crm, exactly_once) → op1 ; wait([op1])
  E->>L: Append{seq4 effect_scheduled(op1, key)} → Ack
  B->>X: BEGIN; INSERT ostro_effects(key); UPDATE leads…; COMMIT
  E->>L: Append{seq5 effect_completed(op1), seq6 wait_resolved[op1]} → Ack
  G->>E: sleep(24h) → op2 ; wait([op2])
  E->>L: Append{seq7 timer_started(op2, due)} → Ack
  E->>E: due > hot-wait → EVICT (drop future + Store)
  E->>L: Done{run, evicted}
  Note over L: timer wheel holds (due, run, op2). No Wasm memory held.
  L->>L: 24h later: append TimerFired(op2) → run ready
  L->>E: Activate{run, attempt=2, journal seq0..8}
  E->>G: instantiate + run — REPLAY
  G->>E: http-fetch / wait → seq1–3 recorded, no I/O
  G->>E: sql-query / wait → seq4–6 recorded, no SQL
  G->>E: sleep / wait → seq7–8 recorded (timer_fired)
  G-->>E: return {"ok": true}
  E->>L: Append{seq9 activation_started(2)…, run_completed} → RunTerminated
```

### 7.3 Crash analysis

| Crash point | Outcome |
|---|---|
| Before `RunStarted` commits | Client gets an error/timeout; retry with `idempotency_key` deduplicates |
| Executor dies after `effect_scheduled` (seq1), before/while calling the API | Lease expires → re-dispatch (attempt 2) → replay reaches seq1 with no completion → re-send with the **same key** → provider deduplicates |
| Executor dies after the API responded, before `effect_completed` | Same as above; idempotent provider returns the original response |
| SQL effect: crash after the CRM commit, before `effect_completed` | Retry hits `ostro_effects` primary key → connector returns stored result → exactly-once |
| Partition leader dies (cluster) | Metadata elects a leader from the ISR; nothing below HW is lost; the in-flight attempt is fenced (attempt bump); run re-dispatched |
| Single node dies | Restart: state store + log tail; in-flight runs re-dispatched with attempt + 1 |
| Guest traps / exceeds memory | Attempt fails; retry per policy or DLQ; node unaffected |

### 7.4 Versioning

Runs are pinned to `module_sha`; new runs use the active version; modules are retained while referenced. `ctx.patched()` for in-flight evolution. `ostro replay --sample N --module new.wasm` replays recent histories (pulled via the API) locally as a CI gate. `ostro migrate-runs` re-pins runs only after an automatic dry-run replay passes. The host links `ostro:runtime@0.1.x` and `0.2.x` side by side.

---

## 8. Placement & spillover

Unlike the pgflow design (pull-based work stealing from a shared table), the partition leader **is** the dispatcher, so placement is a local decision:

| Queue policy | Behaviour |
|---|---|
| `embedded` | Only the server node's executor |
| `workers` | Only connected workers matching the queue's label selector |
| `auto` | Embedded first; spill to workers when any threshold trips (node CPU > 70% for 10 s, executor slots > 90%, resident Wasm memory > 85%, queue lag > 5 s); hysteresis −15% with a 30 s cool-down |

**Demand (D-W12, as shipped).** `demand(q) = running_on_workers(q) + admissible_backlog_for_workers(q)`: for a `workers` queue the admissible backlog is the ready entries that pass flow admission (not paused, not throttled), capped by the queue's concurrency headroom; for an `auto` queue the same only while it is spilling, minus the free embedded slots (0 otherwise); an `embedded` queue's demand is 0. Running work counts, so KEDA does not scale a pool down under its running activations. Each node sets `ostro_queue_worker_demand{queue}` every second (`workers/demand.rs` owns the value), plus `ostro_queue_worker_backlog{queue}`, `ostro_workers_connected` and `ostro_worker_slots_free`, and serves `GET /v1/queues/{q}/demand` → `{queue, demand, backlog, running_on_workers, placement, spilling, workers, worker_slots_free}`.

**KEDA:** a `ScaledObject` with the `metrics-api` scaler (single node, the demand endpoint) or the `prometheus` scaler (`sum(ostro_queue_worker_demand{queue="heavy"})`, clusters) scales a worker Deployment, including to zero; `targetValue` is the worker's `--slots`. Manifests: `deploy/keda/`, `deploy/k8s/`.

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: ostro-workers-heavy }
spec:
  scaleTargetRef: { name: ostro-worker-heavy }
  minReplicaCount: 0
  maxReplicaCount: 50
  triggers:
    - type: metrics-api
      metadata:
        url: "https://ostro.ostro.svc:7700/v1/queues/heavy/demand"
        valueLocation: "demand"
        targetValue: "8"              # = worker --slots
        activationTargetValue: "0"
```

---

## 9. Security & multi-tenancy

### 9.1 Threat model

| Threat | Control |
|---|---|
| Malicious/buggy guest exhausts node | Fuel, epoch slicing, memory caps, global budgets, eviction |
| Guest exfiltrates secrets | Secrets never enter guest memory; placeholders allowed only in headers / marked body fields, never in URLs; responses redactable |
| Guest attacks internal network (SSRF) | No sockets; HTTP via bridge with per-module egress allow-lists; private/link-local/metadata ranges blocked by default; DNS-rebinding-safe connect |
| Native crash in Wasmtime/host | Affects one node: restart + (cluster) failover; continuous fuzzing (`wasm-smith` components, malformed WIT values); prompt Wasmtime security upgrades |
| Compromised worker | Workers get activations only for queues they are labelled for; secrets resolved per (run, attempt, name) and only if granted; mTLS with per-worker certs; tokens revocable |
| Tampered module | Content-addressed (sha256); optional signature enforcement (minisign/cosign); deploy-time import validation |
| Tampered AOT cache | HMAC per artefact; mismatch → delete + recompile |
| API abuse | Scoped tokens, per-tenant rate limits, audit log |

### 9.2 Multi-tenancy

| Layer | Isolation |
|---|---|
| Code | One Store per run; no shared memory; cross-workflow interaction only via host-mediated child workflows/signals |
| Data | `tenant` on runs/modules/schedules; tenant-scoped tokens can only see their own resources |
| Secrets & connections | Per-tenant namespaces + per-module grants |
| Capacity | Per-tenant concurrency, fuel/s, memory, event-rate quotas; weighted fair dispatch |
| Network | Per-module egress policy |

### 9.3 Transport & at-rest

TLS on the client listener (self-signed in dev, ACME or provided certs in production); mTLS on the cluster listener and worker streams (cluster CA generated at `cluster init`). Secrets sealed with a key from env/file/KMS; data directory encryption delegated to the filesystem/volume.

---

## 10. Observability

| Signal | Implementation |
|---|---|
| Traces | `workflow.run` (linked across activations via the run's `traceparent`) → `activation` → `step` / `effect:http` / `effect:sql` / `timer`; outbound HTTP gets a child `traceparent`; OTLP export; replayed spans not re-emitted |
| Metrics | `ostro_runs_started_total`, `ostro_queue_ready{queue}`, `ostro_queue_lag_seconds`, `ostro_queue_worker_demand`, `ostro_step_duration_seconds`, `ostro_replay_events_total`, `ostro_fuel_consumed_total{module}`, `ostro_instance_memory_bytes`, `ostro_dlq_total{reason}`, `ostro_partition_hw_lag{partition,replica}`, `ostro_isr_size{partition}`, `ostro_raft_*` |
| Logs | Guest `log.emit` → bounded per-run log (ring buffer) + node logs/OTLP |
| UI | Run list/filter, history timeline with replay markers, DLQ triage, modules, queues, cluster/partition health |

---

## 11. Code organisation

```
ostro/
├── wit/                         # ostro:runtime WIT (source of truth)
├── crates/
│   ├── ostro/                   # the binary: clap subcommands dev|server|worker|deploy|run|…
│   ├── ostro-engine/            # replay, journal model, Wasmtime host, module cache, executor
│   ├── ostro-bridge/            # I/O: http, sql, object store, secrets, egress, rate limits
│   ├── ostro-objstore/          # S3-compatible + local object storage (rusty-s3; D19): effects, backups, blob GC
│   ├── ostro-partition/         # partition log + state machine + dispatcher + timer wheel
│   ├── ostro-meta/              # metadata StateMachine (on chronik-raft driver)
│   ├── ostro-api/               # HTTP/gRPC API, webhooks, auth, UI assets
│   ├── ostro-worker-proto/      # Execute stream protobufs
│   └── ostro-cli/               # build pipeline (cargo / componentize-py / componentize-js)
├── shared/ (or git deps on a chronik-core workspace — see 04)
│   ├── chronik-wal              # segments, group commit, truncation, sparse index
│   ├── chronik-raft             # RaftDriver, RaftLogStore, transport (+ vendored raft-rs)
│   ├── chronik-replication      # (extracted) follower fetch, leader epochs, ISR tracking
│   └── (object storage)         # superseded: ostro-objstore (crates/, rusty-s3; D19), not extracted from Chronik
├── sdks/{rust,python,typescript}/
├── conformance/
│   ├── sdk/                     # language-neutral workflow scenarios
│   └── replication/             # placement, ISR honesty, divergence, failover (Chronik RP-0 style)
└── deploy/                      # container image, Helm chart, KEDA examples
```

Seams that keep "one engine, many shapes" honest:

```rust
/// Implemented by the partition leader (in-process) and by the worker client (gRPC).
#[async_trait::async_trait]
pub trait ActivationSink: Send + Sync {
    async fn append(&self, run: RunId, attempt: u32, from_seq: u32, events: Vec<Event>)
        -> Result<CommittedSeq, AppendError>;          // AppendError::Fenced on stale attempt/seq
    async fn resolve_secret(&self, run: RunId, attempt: u32, name: &str) -> Result<Secret>;
    async fn fetch_blob(&self, sha: &Sha256) -> Result<Bytes>;
    async fn done(&self, run: RunId, outcome: ActivationOutcome);
}

/// Implemented by fjall (STOR-05; redb was rejected in spike S3).
pub trait StateStore: Send + Sync {
    fn apply_batch(&self, offset: u64, cmds: &[Command]) -> Result<()>;
    fn applied_offset(&self) -> Result<u64>;
    fn snapshot_to(&self, w: &mut dyn std::io::Write) -> Result<()>;   // OSNP v1 (snapshot-format.md); as built: PartitionStore::export_snapshot
}
```
