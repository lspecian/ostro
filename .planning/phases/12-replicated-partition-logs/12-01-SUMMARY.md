---
phase: 12-replicated-partition-logs
plan: 01
subsystem: planning-gate
tags: [gate, reservations, decisions, coordination, cargo, failpoints, replication]
requires:
  - phase: 08-triggers-bottomless-observability
    provides: "22/22 SUMMARYs, 08-VERIFICATION passed (10k campaign PASS 2026-11-22); publish_hw/HwVector, RawBatch/read_envelopes, OSNP install_snapshot, snapshot service, uploader, chunk::walk_back, restore"
  - phase: 07-workers-placement-flow-control
    provides: "22/22 SUMMARYs, 07-VERIFICATION human_needed (by-design only); ClusterCa::import_pem, issue_node_cert, NodeIdentity, mTLS cluster listener"
  - phase: 05-workflow-semantics-core-hardening
    provides: "WIT freeze, F7 meta fields (05-07), seed_epochs + anchoring (05-21)"
provides:
  - "PHASE-12 GATE passed (baseline ci-local --no-musl at 237dbe1)"
  - "Phase 7/8 seam signatures (below) for 12-02 to copy"
  - "RESERVATIONS.md: MetaCommand 4-6 + ext tag 80 planned P12, Cmd 41-42 / MetaRecord 26 unused, D-P12-12 section 11 rows, Phase 12 gate note"
  - "STATE.md: D-P12-1..D-P12-26, [USER REVIEW] D-P12-2/3/4, [HUMAN] Phase 12 items, Phase 12 <-> Phases 9-11 coordination"
  - "crates ostro-cluster and ostro-repl-conformance (manifest + doc-only lib.rs) with every Phase 12 dependency"
  - "failpoint chain ostro -> ostro-server -> ostro-cluster / ostro-meta / ostro-backup; FAILPOINT_CRATES == ci.yml Failpoint suites"
affects: [12-02, 12-03, 12-04, 12-05, 12-06, 12-07, 12-08, 12-09, 12-10, 12-11, 12-12, 12-13, 12-14, 12-15, 12-16, 12-17, 12-18]
tech-stack:
  added: ["tokio-stream 0.1.19 (workspace pin)", "hyper-util 0.1.21 (workspace pin)", "tower 0.5.3 (workspace pin)", "rand 0.9.5 (workspace pin)"]
  patterns: ["docs/code commits through a private index (index-12-01) built on current main + CAS update-ref; worktree never touched by the landing"]
key-files:
  created:
    - crates/ostro-cluster/Cargo.toml
    - crates/ostro-cluster/src/lib.rs
    - crates/ostro-repl-conformance/Cargo.toml
    - crates/ostro-repl-conformance/src/lib.rs
    - .planning/phases/12-replicated-partition-logs/12-01-SUMMARY.md
  modified:
    - Cargo.toml
    - Cargo.lock
    - crates/ostro-log/Cargo.toml
    - crates/ostro-partition/Cargo.toml
    - crates/ostro-server/Cargo.toml
    - crates/ostro/Cargo.toml
    - scripts/ci-local.sh
    - .github/workflows/ci.yml
    - .planning/RESERVATIONS.md
    - .planning/STATE.md
    - .planning/ROADMAP.md
key-decisions:
  - "Gate passed: Phase 8 passed, Phase 7 human_needed with by-design items only; F7 and 05-21 in code, so MetaRecord 26 and Cmd 41-42 stay unused"
  - "rand 0.9.5 (already in the lock via proptest) added for 12-03's seeded StdRng FaultLayer, which the plan list missed; later plans may not touch Cargo files"
  - "ostro-cluster also gets ostro-raft-driver (eraftpb::Message for MetaPeer.Raft), tokio-util, sha2 (12-12 snapshot sha256) and serde/serde_json (12-14 status document); ostro-repl-conformance gets sha2 (module upload key), rand, libc (tier-C signals), serde"
  - "The CA's cluster id lives only in the CA certificate's subject CN (`ostro cluster <id>`); no public reader exists yet - 12-14 must parse ca.crt's CN (x509-parser) for the D-P12-14 source order"
  - "check_failpoint_sync already existed in ci-local (05-18, stricter than the plan's pattern: it handles crate:target entries); reused, not duplicated"
requirements-completed: []
duration: ~5 h 45 min wall (09:35-15:20), of which about 2 h 20 min baseline ci-local, 2 h 05 min gate ci-local and 1 h the no-fail-fast cargo test rerun
completed: 2026-11-22
---


# Phase 12 Plan 01: PHASE-12 GATE Summary

**The gate passed. Phase 8 is complete (08-VERIFICATION `passed`, 10k campaign PASS on 2026-11-22). Phase 7 is complete (human_needed, 5 items, all by design), and its cluster CA and mTLS cluster listener are in the code. The Phase 5 freeze is in force. 05-07 F7 and 05-21 (`seed_epochs`, anchoring) are in the code, so MetaRecord 26 and Cmd 41-42 stay unused. The code matches RESERVATIONS.md, and the P12 block was empty. Every Phase 12 dependency is now declared in two new crates and five member manifests, with no new third-party package. The failpoint chain reaches ostro-cluster, ostro-meta and ostro-backup from the `ostro` binary, and FAILPOINT_CRATES equals the ci.yml Failpoint suites step.**

## Performance

- Started 2026-11-22 09:35, completed 2026-11-22 15:20 CET
- Tasks: 2 of 2
- Commits: 8cf9bc9 (RESERVATIONS), b8e5a17 (STATE), 6f75fb3 (deps/crates/CI), plus the final docs commit
- Baseline ci-local: 2 h 20 min. Gate ci-local: 2 h 05 min, plus 1 h for the no-fail-fast cargo test rerun

## Gate report (Task 1)

Checks 1-3 (script `12-01-gate.sh`, run at 237dbe1):

```
phase 07: plans=22 summaries=all-present status=human_needed
  human item: "Real KEDA ScaledObject drives a worker Deployment 0 -> N -> 0 (H1, F10)"... -> by-design(07 own list)
  human item: "ACME against Let's Encrypt staging on a public host (H3)"... -> by-design(07 own list)
  human item: "ACME against Pebble in CI (H2)"... -> by-design(07 own list)
  human item: "Remote CI legs (H4): linux-aarch64-musl, macos-aarch64, linux-x86_64-gnu with P... -> by-design(e)
  human item: "User review: D-W27, D-W28 (+D-W11), W6 template rule (+D-W3), F4 attempt_failur... -> by-design(f)
phase 08: plans=22 summaries=all-present status=passed
ok   ca.rs fn import_pem
ok   ca.rs NodeIdentity
ok   ca.rs issue_node_cert
ok   cluster listener client_ca_root
ok   wit/FROZEN.toml
ok   tag wit/ostro-runtime-v0.1.0
ok   no 'planned P5' rows
ok   F7 field isr_version
ok   F7 field min_insync
ok   F7 field replicas
ok   F7 field cluster_addr
ok   F7 field repl_addr
ok   05-21 fn seed_epochs
ok   05-21 torn_first_record_of_anchored_log
GATE 1-3: PASS
```

- **Check 1 (Phase 8).** 22/22 SUMMARYs. `status: passed`, so the by-design rule does not apply. The remaining human items are all external: SeaweedFS/R2 S3 legs (a), fresh-machine restore (b), Jaeger/Grafana (c), the user review of the P8 decisions (f), and "TLS roots in a FROM scratch image", which is a Phase 14 artefact. The two items that were pending (the 10k campaign and the quiet restore_rpo run) were closed on 2026-11-22: RESULT PASS, and p50 460 / max 712 ms.
- **Check 2 (Phase 7).** 22/22 SUMMARYs. `status: human_needed`. Every human item is on 07-VERIFICATION's own list (KEDA, LE staging, Pebble CI) or in (e)/(f).
- **Check 3.** Freeze, tag, F7 and 05-21 are present.
- **Check 5 (working tree).** Judged in the 12-01 worktree at main, which was clean. The main checkout is a stale tree, not in-flight work. The other agents running at the time (09-14/09-15, 11-01) committed through their own worktrees. None of their files is in this plan's `files_modified`, apart from STATE/RESERVATIONS, which use named sections built from the current main blob.
- **Check 6 (baseline).** `ci-local --no-musl` at **237dbe1**, 2026-11-22 09:40 to about 12:00. Every step passed except two, and both were fixed on main during the run by the coordinator's ci-fix3 commits. This plan landed on top of those fixes:
  - `cargo test`: one failure, `ostro-server --test worker_remote remote_run_journal_equals_embedded`. It compared OpCancelled journals by a seq-derived hash. Fixed by 9cdafa1, "compare OpCancelled by target".
  - `cargo deny check`: RUSTSEC-2026-0592 (url 2.5.8 `Url::origin` stack overflow) was published during the run. My earlier deny run at about 09:50 was clean. Fixed by 7f40156 (url 2.5.9).

Baseline step timings:

```
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  2s
cargo clippy -D warnings             PASS  64s
cargo build (warning-free)           PASS  58s
sdk workspace fmt + clippy           PASS  13s
sdk workspace test + wasm build      PASS  6s
guests build (wasm32-wasip2)         PASS  22s
order-saga example                   PASS  26s
external: OSTRO_TEST_PG_URL          SKIP  -
external: OSTRO_TEST_MYSQL_URL       SKIP  -
external: OSTRO_TEST_S3_URL          SKIP  -
cargo test                           FAIL  3886s
semantics sleepers (serial)          PASS  31s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  3726s
server campaign smoke (20 kills)     PASS  605s
phase 7 failpoint legs + manifests   PASS  52s
cargo deny check                     FAIL  1s
check-vendored.sh                    PASS  1s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  1s
zig + cargo-zigbuild present         SKIP  -
musl release (static, mimalloc)      SKIP  -
musl zigbuild test                   SKIP  -
```

## Phase 7/8 seams

Recorded verbatim at 237dbe1. 12-02 copies them.

| Seam | Location | Signature / fact |
|------|----------|------------------|
| HW publication (08-08) | `crates/ostro-log/src/cut.rs` | `pub struct HwVector { cut: Mutex<Vec<Offset>>, watches: Box<[Option<Arc<watch::Sender<Offset>>>]>, .. }`; `pub fn new(initial: &[Option<Offset>]) -> Self`; `pub fn publish_hw(&self, p: PartitionId, hw: Offset)`. This is "the single HW publication point": it raises the vector under its lock first, then sends on the watch. It is monotonic, and a partition without a watch (failed recovery) is ignored. |
| Raw envelope reads (08-08) | `crates/ostro-log/src/replicated.rs` | `pub struct RawBatch { pub base: Offset, pub end: Offset, pub epoch: u32, pub envelopes: Vec<Bytes> }`, which never spans an epoch change. Trait method `fn read_envelopes(&self, from: Offset, max_bytes: usize) -> impl Future<Output = Result<Vec<RawBatch>, LogError>> + Send` is HW-capped, and Rf1Log overrides it with the stored bytes (`rf1.rs:562`). Node wrapper: `Node::read_envelopes(&self, p: PartitionId, from: Offset, max_bytes: usize) -> Result<Vec<RawBatch>, LogError>` (`ostro-partition/src/node.rs:569`). |
| Snapshot install (08-02) | `crates/ostro-partition/src/store/snapshot.rs` | `pub fn install_snapshot<R: Read>(data_dir: &Path, p: PartitionId, count: PartitionCount, cfg: &StoreConfig, r: R) -> Result<SnapshotInfo, SnapshotError>`, plus `#[doc(hidden)] install_snapshot_with(.., writer: InstallWriter)`. The staged install goes `.install-pNNNN`, then a digest check, then rename aside `.old-pNNNN`, then fsync `state/`. The caller must have closed the store. `OsnpReader::next_frame(&mut self) -> Result<Option<Frame>, SnapshotError>`. `pub struct SnapshotInfo { partition: u16, partition_count: u16, state_format: u32, applied_offset: u64, state_hash: [u8; 32], latest_epoch: u32, logical_digest: [u8; 32], total_rows: u64, bytes: u64, blob_refs: Vec<[u8; 32]> }` (all pub). |
| Snapshot service (08-09) | `crates/ostro-partition/src/leader/snapshot.rs` | `pub struct SnapshotService`; `SnapshotService::new(partition: PartitionId, count: PartitionCount, data_dir: &Path, store: Arc<PartitionStore>, cfg: SnapshotConfig, clock: Arc<dyn Clock>, shared: Arc<SnapshotShared>, retained: Vec<LocalSnapshot>) -> Arc<Self>`; `take(&self) -> Result<Option<SnapshotTaken>, SnapshotError>`; `local_floor() -> watch::Receiver<Offset>`; `pub fn spawn(ctx: LeaderCtx, svc: Arc<SnapshotService>) -> JoinHandle<()>`. **Started by** `leader::spawn_all(ctx: LeaderCtx, cfg: &LeaderConfig) -> LeaderTasks` (`leader/mod.rs:108`), when `LeaderCtx.snapshots: Option<Arc<SnapshotService>>` is `Some` and `config().enabled` is true (leader only today; 12-13 moves it to both roles). |
| Uploader (08-13/08-23) | `crates/ostro-backup/src/uploader.rs` | `Uploader::new(node: Arc<Node>, meta: Option<MetaView>, clock: Arc<dyn Clock>, cfg: UploaderConfig) -> Self`. There is no separate deps struct: the deps are those four arguments, `UploaderConfig` carries `cluster_id`/`node_id`, and the blob store comes from a node handle. Key helpers: `snap_key(partition, applied, sha)`, `snap_prefix`, `meta_key`, `blob_key`. |
| Chain walk-back (08-13) | `crates/ostro-backup/src/chunk.rs:379` | `pub async fn walk_back(store: &Arc<dyn ObjectStore>, last_chunk_key: &str, last_chunk_sha256: [u8; 32], stop: u64) -> Result<Vec<ChunkRef>, ChunkError>` returns the chunks in forward order, chain-verified. |
| Fenced metric (08-13) | `uploader.rs:80` | `pub const METRIC_FENCED: &str = "ostro_backup_fenced"` is a gauge, set to 1 once another writer fenced this uploader. The Phase 12 per-partition metric is `ostro_repl_upload_fenced{partition}`. |
| Restore (08-18) | `crates/ostro-backup/src/restore.rs:695` | `pub async fn restore(source: RestoreSource, opts: RestoreOptions) -> Result<RestoreReport, RestoreError>` (refuses a non-empty data dir). |
| Cluster CA (07-06) | `crates/ostro-server/src/tls/ca.rs` | `pub struct ClusterCa { cluster_id, dir, cert_der, issuer }`; `load_or_create(dir: &Path, cluster_id: &str)`; `import_pem(dir: &Path, cluster_id: &str, ca_crt_pem: &str, ca_key_pem: &str)`; `issue_node_cert(&self, node_id: u64, sans: &[String]) -> Result<CertifiedKeyPair, TlsError>`; `load_or_issue_node_cert(&self, node_id, advertise)`; `root_store()`; `cluster_id(&self) -> &str`. `pub struct NodeIdentity { pub cluster_id: String, pub node_id: u64, pub not_after: i64 }`; `NodeIdentity::uri(cluster_id, node_id)` gives `ostro://<cluster_id>/node/<node_id>`; `NodeIdentity::from_der(cert_der)` rejects worker certs. |
| **Where the CA's cluster id is stored** | `ca.rs` `from_pem_bytes` / `ca_cn` | It is stored **only in the CA certificate's subject CN**, `"ostro cluster <cluster_id>"` (`<data_dir>/tls/ca.crt`). Loading checks that the CN equals `ca_cn(cluster_id)`, otherwise "CA belongs to another cluster". In memory it is read with `ClusterCa::cluster_id()`, but that needs the id up front. **There is no public "read the cluster id from an existing CA" function.** D-P12-14's source order (config, then node.toml, then **CA**, then ClusterInfo) needs 12-14 to parse `ca.crt`'s CN with x509-parser (already a dep) and strip the `ostro cluster ` prefix. The authoritative id is `ClusterInfo.cluster_id` (`ostro-meta/src/records.rs:50`). `server.rs::init_cluster` generates it as a UUIDv7 on blank meta and reads it with `meta.view().read(|s| s.cluster())`. |
| Cluster listener (07-09) | `crates/ostro-server/src/workers/service.rs` | `pub fn start(ctx: &P7Ctx) -> Result<Option<JoinHandle<()>>, P7Error>` binds the join service and returns `None` when `[workers] enabled = false` (D-P12-13 must bind it in every profile). `pub fn serve(listener: std::net::TcpListener, deps: ClusterDeps) -> Result<(SocketAddr, JoinHandle<()>), P7Error>` serves Execute with `.client_ca_root(..)` (client cert always required). `P7Ctx { clock: Arc<dyn Clock>, cluster_id, .. }` is in `p7.rs`. |
| Derived (`x_*`) keyspaces | `crates/ostro-partition/src/store/derived.rs`, `node.rs:899` | At 237dbe1 there were **none**: Phase 8 added no derived keyspace, and `schedule_state` is real state carried by the snapshot. **Phase 11 landed `x_run_index` (11-05, bd15cb5) during this plan.** Its rebuild hook is `PartitionStore::start_run_index_rebuild(&self) -> bool`. It starts a background thread (never the apply thread) when the marker is stale or missing, and `IndexState::Rebuilding` makes queries fall back to the scan. Related calls: `run_index_state()`, `join_run_index_rebuild()`, `delete_run_index_marker()`. The partition startup calls it after recovery and before the new term (`node.rs`: "a stale or missing x_run_index rebuilds in the background (never in Inspect)"). **After a snapshot install (12-12/12-13), the reopened store must go through the same call; it is not in the snapshot's digest scope.** |
| Wiped-voter detection | `vendor/ostro-raft-driver/src/driver.rs:470-484` | `Event::Step` with `MsgHeartbeat && msg.commit > raft_log.last_index()` sets `self.fatal` and stops the loop. Text: `"leader {from} has committed through {commit} but this node's catalog log ends at {last}: this node lost raft state it had acknowledged (data wiped or restored). Remove it from the cluster (cluster remove-node {id} --force), add it back (cluster add-node), and start it with CHRONIK_CLUSTER_JOIN=true."` **The text still names `CHRONIK_CLUSTER_JOIN`.** 12-10 replaces it with the Ostro remedy (D-P12-14). |
| Clock | `crates/ostro-partition/src/clock.rs`; `ostro-server/src/server.rs` | `pub trait Clock: Send + Sync + 'static { fn now_ms(&self) -> u64; }` (`SystemClock`, `ManualClock`). Server: `ServerOptions.clock: Arc<dyn Clock>` + `with_clock(..)`; `Server::clock(&self) -> &Arc<dyn Clock>` (`server.rs:930`); `P7Ctx.clock`. |

## Reservations check (Task 1 step 7)

Script `12-01-rescheck.py`, modelled on 08-01 step 7 and 09-01/11-01. It extracts Cmd tags, StartRun and JournalEvent fields, intents, outbox kinds, TerminalStatus, journal kinds, TABLES order and names, RejectReason, InvalidCode, MetaRecord/MetaKind, **MetaCommand** and registered ext tags. It asserts that the P12 block is empty: no Cmd 41-42, no MetaRecord/MetaKind 26, MetaCommand exactly 0-3, no ext tag 80-95, and no `ElectLeader`/`AlterIsr`/`ResignLeader`/`ElectionInfo`/`PartitionReplication` identifiers in code. Doc comments are excluded, because `records.rs` documents `isr_version` as the "AlterIsr CAS token". The core of the P12 assertion:

```python
p12 = []
p12 += [f"Cmd {t}" for t, _ in cmds if 41 <= t <= 42]
p12 += [f"MetaRecord {i}" for i in range(len(mr)) if i == 26]
p12 += [f"MetaKind {i}" for i in range(len(mk)) if i == 26]
p12 += [f"MetaCommand {i} {n}" for i, n in enumerate(mc) if i >= 4]
p12 += [f"ext tag {v}" for v, _, _ in tags if 80 <= v <= 95]
# + identifier scan over crates/*/src (code lines only)
```

Output after Task 2, at 6f75fb3 (before Task 2 at 237dbe1 it was identical, plus the "crates not yet present" line):

```
OK   Cmd 1 LeaderAnnounced (registry: LeaderAnnounced)
OK   Cmd 2 StartRun (registry: StartRun)
OK   Cmd 3 AttemptGranted (registry: AttemptGranted)
OK   Cmd 4 EventsAppended (registry: EventsAppended)
OK   Cmd 5 TimerFired (registry: TimerFired)
OK   Cmd 6 InboundBatch (registry: InboundBatch)
OK   Cmd 7 OutboxTruncated (registry: OutboxTruncated)
OK   Cmd 8 SignalRun (registry: SignalRun)
OK   Cmd 9 RequestCancel (registry: RequestCancel)
OK   Cmd 10 TerminateRun (registry: TerminateRun)
OK   Cmd 11 RetryRun (registry: RetryRun)
OK   Cmd 12 CompleteToken (registry: CompleteToken)
OK   Cmd 27 ScheduleFire (registry: ScheduleFire)
OK   Cmd 13 CompleteEffect (registry: CompleteEffect)
OK   Cmd 20 PauseRun (registry: PauseRun)
OK   Cmd 21 ResumeRun (registry: ResumeRun)
     Cmd tags in code: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 27, 13, 20, 21]
     StartRun fields: [(1, 'run_id'), (2, 'workflow'), (3, 'workflow_version'), (4, 'queue'), (5, 'input'), (6, 'idempotency_key'), (7, 'conflict_policy'), (8, 'concurrency_key'), (9, 'priority'), (10, 'ready_at_ms'), (11, 'requested_at_ms'), (12, 'parent'), (13, 'rng_seed'), (14, 'module_sha'), (15, 'tags'), (16, 'tenant'), (17, 'traceparent'), (18, 'run_timeout_ms'), (19, 'schedule_to_start_ms'), (20, 'retention_ms'), (21, 'retry_of'), (22, 'continued_from'), (23, 'run_started')]
OK   StartRun fields 1..=23 exist, max 23
     JournalEvent fields: [(1, 'seq'), (2, 'kind'), (3, 'payload')]
OK   JournalEvent fields 1..=3 only (4 reserved P11)
OK   Intent 1 ScheduleTimer
OK   Intent 2 CancelTimer
OK   Intent 3 SendMessage
OK   Intent 4 EnqueueReady
OK   Intent 5 CompleteRun
OK   Intent 6 Suspend
OK   Intent 7 RegisterWait
OK   Intent 8 CancelWait
OK   Intent 9 RegisterEffect
OK   Intent 10 CancelEffect
OK   Intent 19 ChildJournal
OK   Outbox kind 1 StartChild
OK   Outbox kind 2 ChildCompleted
OK   Outbox kind 3 Signal
OK   Outbox kind 4 CancelRun
OK   Outbox kind 5 CronStart
OK   Outbox kind 6 StartRun
OK   TerminalStatus [(0, 'Completed'), (1, 'Failed'), (2, 'Cancelled'), (3, 'Terminated'), (4, 'TimedOut'), (5, 'ContinuedAsNew')]
OK   EventKind codes [1, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 64, 65, 66, 67, 68, 69, 70, 71, 96, 97] (outside exists: [])
OK   TABLES order/names ['meta', 'runs', 'events', 'payloads', 'ready', 'timers', 'inbox', 'dedupe', 'outbox', 'inbound_seen', 'children', 'dlq', 'effects', 'running', 'waits', 'signal_seen', 'schedule_state']
OK   no x_* keyspace in TABLES
OK   RejectReason [(0, 'StaleEpoch'), (1, 'Fenced'), (2, 'SeqMismatch'), (3, 'SeqGap'), (4, 'RunNotFound'), (5, 'RunTerminal'), (6, 'RunExists'), (7, 'IdempotencyConflict'), (8, 'PayloadTooLarge'), (9, 'WrongPartition'), (10, 'Invalid'), (11, 'RunCompleted'), (12, 'ReservedP6_12'), (13, 'ReservedP6_13'), (14, 'ReservedP6_14'), (15, 'RunPaused')]
OK   InvalidCode [(0, 'Decode'), (1, 'Empty'), (2, 'BadRunId'), (3, 'MissingToken'), (4, 'EventsNotContiguous'), (5, 'MissingField'), (6, 'NotImplemented'), (7, 'BadIntent'), (8, 'FieldTooLong'), (9, 'BadSource'), (10, 'TruncateBeyondEnd'), (11, 'BadLength'), (12, 'NotRunning'), (13, 'ReservedP5_13'), (14, 'ReservedP5_14'), (15, 'BadEffectCompletion'), (16, 'ReservedP6_16'), (17, 'ReservedP6_17'), (18, 'ReservedP7_18'), (19, 'ReservedP7_19'), (20, 'ReservedP7_20'), (21, 'ReservedP7_21'), (22, 'BadScheduleFire'), (23, 'ScheduleOwnerMismatch')]
     MetaRecord [(0, 'Cluster'), (1, 'Member'), (2, 'Partition'), (3, 'Module'), (4, 'Workflow'), (5, 'Active'), (6, 'Queue'), (7, 'Schedule'), (8, 'Token'), (9, 'Tenant'), (10, 'Secret'), (11, 'Grant'), (12, 'Connection'), (13, 'Egress'), (14, 'WorkflowPolicy'), (15, 'QueuePolicy'), (16, 'Webhook')]
     MetaKind [(0, 'Cluster'), (1, 'Member'), (2, 'Partition'), (3, 'Module'), (4, 'Workflow'), (5, 'Active'), (6, 'Queue'), (7, 'Schedule'), (8, 'Token'), (9, 'Tenant'), (10, 'Secret'), (11, 'Grant'), (12, 'Connection'), (13, 'Egress'), (14, 'WorkflowPolicy'), (15, 'QueuePolicy'), (16, 'Webhook')]
OK   MetaRecord/MetaKind 0..=16 (len 17/17)
OK   MetaCommand ['InitCluster', 'Put', 'Delete', 'SetPartitionLeader']
OK   META_FORMAT = 2
OK   ext tag 15 EXT_TAG_TEST (crates/ostro-meta/src/ext.rs) (registry: 15 = golden/test only)
OK   Phase 12 block empty in code (found: [])
RESERVATIONS CHECK: PASS
```

## Final P12 ordinal table

| Space | Ordinal | Item | Status |
|---|---|---|---|
| MetaCommand | 4 | `ElectLeader` | planned P12 (12-02 shape, 12-05 apply) |
| MetaCommand | 5 | `AlterIsr` | planned P12 (12-02 shape, 12-05 apply) |
| MetaCommand | 6 | `ResignLeader` | planned P12 (12-02 shape, 12-05 apply) |
| MetaCommand | 7 | spare | reserved P12 |
| Meta ext tag | 80 | `PartitionAssignment` ElectionInfo `{reason u8: 0 Bootstrap 1 Startup 2 Failover 3 Resign 4 Offline, elected_at_ms u64, prev_leader u64}` | planned P12 (12-02) |
| Meta ext tag | 81-95 | spare | reserved P12 |
| Cmd tag | 41-42 | contingency | reserved P12, **unused** (F7 landed in 05-07) |
| MetaRecord | 26 | PartitionReplication fallback | reserved P12, **unused** (F7 landed in 05-07) |

The section 11 rows (D-P12-12) are all in RESERVATIONS.md, including the env vars `OSTRO_PROC_ITERS`, `OSTRO_FAILOVER_ASSERT`, `OSTRO_FAILOVER_SWEEP`, `OSTRO_FAILPOINT_BIN`, `OSTRO_PROC_BUILD` and the metrics `ostro_repl_upload_fenced{partition}` and `ostro_repl_self_demotions_total`.

## Dependencies (Task 2)

Everything resolves to versions already in Cargo.lock. `cargo update --workspace --offline` added only the two workspace packages, and the lock diff has 76 lines of additions only. `cargo tree -d --depth 0` gives the same duplicate set before and after. There is no duplicate of tonic, prost, hyper, http, axum or rustls, and the normal tree has no chrono, bincode, object_store, openssl-sys, tokio-uring, prometheus or native-tls. `cargo deny check` reports advisories, bans, licenses and sources ok. **deny.toml is unchanged** (no new licence).

New workspace pins: `tokio-stream 0.1.19` (features net, sync), `hyper-util 0.1.21` (tokio), `tower 0.5.3` (util), `rand 0.9.5` (std, std_rng; the same rand as proptest's).

| Crate | Additions (resolved versions) |
|---|---|
| **ostro-cluster** (new) | ostro-types, ostro-log, ostro-meta, ostro-proto, ostro-blob, ostro-partition, ostro-raft-driver; tonic 0.14.9 (`tls-aws-lc`, `tls-connect-info`), tonic-prost 0.14.9, prost 0.14.6, tokio 1.54.1, tokio-stream 0.1.19, tokio-util 0.7.20, futures-util 0.3.34, bytes, tracing, metrics 0.24.6, thiserror 2, x509-parser 0.18.1, hyper-util 0.1.21, tower 0.5.3, sha2 0.11.1, serde, serde_json, fail 0.5.1 (optional). Dev: rcgen 0.14.10, tempfile, tokio (`test-util`, `macros`, `rt-multi-thread`), proptest 1.12. Feature `failpoints = ["dep:fail", "fail/failpoints", "ostro-log/failpoints", "ostro-partition/failpoints"]` |
| **ostro-repl-conformance** (new, `publish = false`) | ostro-cluster, ostro-server, ostro-log, ostro-partition, ostro-meta, **ostro-backup**, ostro-types, ostro-proto, ostro-objstore, ostro-testkit; tokio (full + test-util), tokio-stream, futures-util, tonic (TLS features), hyper-util, tower, reqwest 0.13.5 (rustls, json), serde, serde_json, bytes, tempfile, tracing, thiserror, rcgen, x509-parser, sha2, rand 0.9.5, libc, proptest, fail (optional). Feature `failpoints` chains cluster/server/log/backup/meta |
| ostro-log | dev: proptest-state-machine 0.9.0 (12-04) |
| ostro-meta | none: `failpoints = ["dep:fail", "fail/failpoints", "ostro-raft-driver/failpoints"]` already existed (05-18) |
| ostro-partition | futures-util 0.3.34 (BoxFuture role contracts, 12-02) |
| ostro-backup | none (it already depends on ostro-log and ostro-partition) |
| ostro-server | ostro-cluster; `failpoints` += `ostro-cluster/failpoints` |
| ostro (binary) | ostro-cluster, ostro-meta; `failpoints` += `ostro-cluster/failpoints`, `ostro-meta/failpoints` (ostro-backup/failpoints was already there) |

CI: `FAILPOINT_CRATES` += `ostro-cluster ostro-repl-conformance` (appended last), mirrored as two lines at the end of the ci.yml "Failpoint suites" step. ostro-meta was already in both lists. `check_failpoint_sync` already existed (05-18) and passes. The plan's strict-pattern diff, extended to `crate:target` entries, prints identical lists.

## Deviations from Plan

### Auto-fixed issues

**1. [Rule 3 - Blocking] Doc-only `lib.rs` tripped cargo's `unused_dependencies` lint (warning-free build gate).**
- **Found during:** Task 2, while building warning-free.
- **Issue:** Cargo 1.100 warns on manifest dependencies that are never used. The flagged ones were ostro-cluster (ostro-meta, ostro-proto, ostro-partition, x509-parser), ostro-repl-conformance (ostro-server, ostro-testkit, proptest), ostro-partition (futures-util) and ostro-server (ostro-cluster).
- **Fix:** `mod dep_anchors { use <crate> as _; }` blocks, the pattern 06-02 and 08-01 established: in the two new `lib.rs` files (still doc-only otherwise; 12-02 replaces them) and appended to `crates/ostro-partition/src/lib.rs` and `crates/ostro-server/src/lib.rs`. Those two files are outside `files_modified`, but each change is a 6-line append.
- **Commit:** 6f75fb3

**2. [Rule 2 - Missing critical] Dependencies later plans need that the plan's list missed.** 12-DECISIONS forbids later Phase 12 plans from touching Cargo files.
- `rand 0.9.5`: 12-03's FaultLayer is a "seeded `StdRng`".
- `sha2`: in ostro-cluster for 12-12's whole-file snapshot sha256, and in ostro-repl-conformance for `PUT /v1/modules/{sha256}`.
- `ostro-raft-driver`: in ostro-cluster for `raft::eraftpb::Message` (12-05 MetaPeer.Raft).
- `serde`/`serde_json`: in ostro-cluster for the 12-14 status document.
- `tokio-util` in ostro-cluster, and `libc` in ostro-repl-conformance for tier-C signals.
- All of these were already in the lock. Commit: 6f75fb3.

**3. [Rule 3] Baseline reds fixed upstream, not here.** After the baseline, main already carried 7f40156 (url 2.5.9) and 9cdafa1 (worker_remote). I dropped my own `cargo update -p url` and rebased the Task 2 edits onto that main.

### Other notes
- ROADMAP: 12-DECISIONS says "executors never edit" ROADMAP, but the coordinator's executor brief asks for the plan tick. I ticked only the 12-01 line and added a `Phase 12 plan:` line to STATE Current Position, as 11-01 did.
- The Task 2 verify diff (`awk '{print $4}'` on a strict single-pattern grep) cannot express the existing `crate:target` entries (`ostro-engine:aot_failpoints`, `ostro:fx_redrive`, ...). I used ci-local's `check_failpoint_sync`, which compares full commands in order, plus an extended diff.

## Plan gate

Ran `ci-local --no-musl` in the detached worktree at **6f75fb3** (the Task 2 commit on top of main a0a016a), 12:09 to 14:14 CET:

```
================ ci-local summary ================
toolchain 1.100.0                    PASS  0s
cargo fmt --check                    PASS  2s
cargo clippy -D warnings             PASS  42s
cargo build (warning-free)           PASS  50s
sdk workspace fmt + clippy           PASS  4s
sdk workspace test + wasm build      PASS  6s
guests build (wasm32-wasip2)         PASS  23s
order-saga example                   PASS  24s
external: OSTRO_TEST_PG_URL          SKIP  -
external: OSTRO_TEST_MYSQL_URL       SKIP  -
external: OSTRO_TEST_S3_URL          SKIP  -
cargo test                           FAIL  3443s
semantics sleepers (serial)          PASS  33s
failpoint list sync (ci.yml)         PASS  0s
failpoint suites                     PASS  3074s
server campaign smoke (20 kills)     PASS  605s
phase 7 failpoint legs + manifests   PASS  52s
cargo deny check                     PASS  3s
check-vendored.sh                    PASS  1s
check-deny-fixtures.sh               PASS  1s
no chronik/tokio-uring/prometheus    PASS  0s
workflow validation                  PASS  6s
zig + cargo-zigbuild present         SKIP  -
musl release (static, mimalloc)      SKIP  -
musl zigbuild test                   SKIP  -
==================================================
1 step(s) FAILED
EXIT=1
```

The only red was `cargo test`: `ostro-triggers --test webhook_verify`, 2 of 33 failed with `NotFound` on `concat!(env!("CARGO_MANIFEST_DIR"), "/src/webhook.rs")` and `.../ostro-meta/src/records.rs`. **This is an environment artefact, not a regression.** ostro-triggers did not change between the baseline worktree and the gate worktree, so cargo reused the test binary built in the shared slot from the baseline worktree, with that worktree's compile-time `CARGO_MANIFEST_DIR`. I had removed that worktree before the gate.
- After a rebuild (touch, recompile), `cargo test -p ostro-triggers --test webhook_verify` passes 33/33.
- `cargo test` without `--no-fail-fast` stops at the first failing binary, so I re-ran `cargo test --workspace --locked --no-fail-fast` at 6f75fb3 (14:15 to 15:18): **EXIT=0, 263 test binaries, 2457 passed, 0 failed.** That includes the new `ostro_cluster` and `ostro_repl_conformance` lib and doc-test targets (0 tests each).
- Every other step passed, including the failpoint suites with the two new crates, cargo deny and the failpoint list sync.

**Gate verdict: green at 6f75fb3.**

Post-gate recheck on main at 894fafb, which by then carried 09-17's ci-local/ci.yml edits and 11-03's Cargo edits: `check_failpoint_sync` passes. 09-17 appended `ostro-server:children_failpoints` after the two Phase 12 entries in both lists. `cargo deny check` reports advisories, bans, licenses and sources ok, and the `ostro` failpoints chain still names `ostro-cluster/failpoints` and `ostro-meta/failpoints`.

## Open items for later plans
- 12-14: a reader for the cluster id stored in an existing CA (parse the `ca.crt` CN). See the seam table.
- 12-10: the vendored wiped-voter message still says `CHRONIK_CLUSTER_JOIN=true`. 12-10 owns the Ostro remedy text and its VENDORED note.
- `workers::service::start` returns `None` with workers disabled. D-P12-13 (12-14) binds the cluster listener in every profile.
- User review pending: D-P12-2, D-P12-3, D-P12-4. These are adopted defaults, recorded in STATE Pending Todos.

## Self-Check: PASSED

- Files: crates/ostro-cluster/{Cargo.toml,src/lib.rs}, crates/ostro-repl-conformance/{Cargo.toml,src/lib.rs} present at main.
- Commits: 8cf9bc9, b8e5a17 and 6f75fb3 are on main.
