---
phase: 12-replicated-partition-logs
plan: 02
subsystem: contracts
tags: [contracts, proto, meta, replication, roles, config, conformance, stubs]
requires:
  - phase: 12-replicated-partition-logs
    provides: "12-01 gate passed; P12 ordinal block (MetaCommand 4-6, ext tag 80); crates ostro-cluster / ostro-repl-conformance with every Phase 12 dependency"
provides:
  - "ostro_proto::cluster::v1 (cluster.proto: Replication {Fetch, FetchSnapshot, FetchBlob, PushBlob}, MetaPeer {Raft, Propose, ReportSuspect}); frozen for Phase 12"
  - "MetaCommand 4 ElectLeader / 5 AlterIsr / 6 ResignLeader shapes (apply = Invalid until 12-05); ext tag 80 ElectionInfo + helpers"
  - "ReplicatedLog replica API (stubbed on the RF=1 log until 12-08); LogError::{NotEnoughReplicas, NotLeader}; ostro_log::mutants (10 names + ALL); ostro_log::replica module list"
  - "ostro_partition::roles: PartitionLogHandle alias + open_partition_log_for_tests, PartitionAssignmentView, AssignmentSource, FollowerDriver, FollowerTask, PartitionRoles, PartitionControl, InstallGuard"
  - "ostro-cluster module list + transport contract (PeerNode, ClusterTransport, ClusterIncoming, MemConn, TransportError), SuspectSink, install/identity/authz/service signatures"
  - "ostro-backup cluster module + UploaderClusterDeps / Uploader::with_cluster"
  - "ostro-server: [replication] section, [cluster] cluster_id / replication_listen / peers / tls.ca_cert_file / tls.ca_key_file with validation; ServerOptions::cluster_transport hook; cluster module"
  - "ostro-repl-conformance: harness API (ClusterSpec, ClusterHarness, ApiClient, DataDirView, MetaLogEntry, HarnessError), oracle/scenario/pending/mutants module lists, Scenario registry + all(), tests/skeleton.rs, tests/red_run.rs"
affects: [12-03, 12-04, 12-05, 12-06, 12-07, 12-08, 12-09, 12-10, 12-11, 12-12, 12-13, 12-14, 12-15, 12-16, 12-17, 12-18]
tech-stack:
  added: []
  patterns: ["interface-first: every Phase 12 module declared once with a stub naming its owner; later plans fill files, never module lists"]
key-files:
  created:
    - crates/ostro-proto/proto/ostro/cluster/v1/cluster.proto
    - crates/ostro-log/src/mutants.rs
    - crates/ostro-log/src/replica/{mod,tracker,isr,watermark,divergence,failover}.rs
    - crates/ostro-partition/src/roles.rs
    - crates/ostro-partition/src/replica_services.rs
    - crates/ostro-cluster/src/** (18 modules + transport/{mod,tcp}.rs)
    - crates/ostro-backup/src/cluster.rs
    - crates/ostro-server/src/cluster.rs
    - crates/ostro-repl-conformance/src/** (harness, oracle, scenarios, pending, mutants)
    - crates/ostro-repl-conformance/tests/{skeleton,red_run}.rs
  modified:
    - crates/ostro-proto/{build.rs,src/lib.rs}
    - crates/ostro-meta/src/{command,ext,state}.rs
    - crates/ostro-log/src/{lib,replicated,rf1,error}.rs
    - crates/ostro-partition/src/lib.rs
    - crates/ostro-backup/src/{lib,uploader}.rs
    - crates/ostro-server/src/{lib,config,server}.rs
    - crates/ostro-server/tests/config.rs
    - crates/ostro-log/tests/{hw_cut,read_envelopes}.rs
    - crates/ostro-partition/tests/common/manual_hw.rs
    - .planning/RESERVATIONS.md
key-decisions:
  - "The server transport hook is ServerOptions::cluster_transport (+ with_cluster_transport), not ExecutorHooks (executor.rs is outside files_modified; the plan allowed either)"
  - "The uploader has no deps struct (12-01 seam), so UploaderClusterDeps is attached with Uploader::with_cluster(deps); Uploader::new is unchanged and None keeps Phase 8 behaviour"
  - "identity::resolve_cluster_id takes the five inputs 12-14 lists (configured, node_toml, stored_ca, persisted_cluster_info, meta_blank) and returns Result<ClusterIdSource, BootError>"
  - "open_partition_log_for_tests returns (LogNode, Arc<PartitionLogHandle>): the LogNode must stay alive and be shut down by the fixture"
  - "The ReplicatedLog additions are required trait methods (no defaults), so every wrapper (test doubles included) must forward them; 12-08 cannot forget one"
requirements-completed: []
duration: ~6 h wall (15:30-21:30), of which about 2 h 40 min ci-local and 55 min the no-fail-fast cargo test rerun
completed: 2026-11-22
---

# Phase 12 Plan 02: Contracts Summary

**Every Phase 12 contract is in place. cluster.proto compiles into `ostro_proto::cluster::v1`. MetaCommand 4-6 and ext tag 80 have fixed shapes, and their apply is stubbed until 12-05. The `ReplicatedLog` replica API is stubbed on the RF=1 log, so RF=1 still runs exactly the same code. The ten `repl::mutant::*` names exist. The ostro-partition role contracts exist. Every Phase 12 module (ostro-cluster, conformance harness/oracles/scenarios/pending/mutants, backup cluster, server cluster) is declared once, with a stub that names its owning plan. The `[replication]` and `[cluster]` peer config is validated. The server has a transport hook.**

## Performance

- Tasks: 2 of 2
- Commits: 4c165f7 (Task 1, contracts), 23e6fe0 (Task 2, skeletons/config/hook/harness API), plus the docs commit (SUMMARY, RESERVATIONS flips, ROADMAP tick, STATE line)

## Phase 7/8 seams (copied from 12-01-SUMMARY, recorded at 237dbe1)

| Seam | Location | Signature / fact |
|------|----------|------------------|
| HW publication (08-08) | `ostro-log/src/cut.rs` | `pub struct HwVector`; `HwVector::new(initial: &[Option<Offset>])`; `pub fn publish_hw(&self, p: PartitionId, hw: Offset)`: the single HW publication point. It raises the vector under its lock, then sends on the watch. It is monotonic and ignores a partition without a watch. `retract_hw(p, new)` is used by truncation. |
| Raw envelope reads (08-08) | `ostro-log/src/replicated.rs` | `pub struct RawBatch { pub base: Offset, pub end: Offset, pub epoch: u32, pub envelopes: Vec<Bytes> }` (never spans an epoch change). `ReplicatedLog::read_envelopes(from, max_bytes)` is HW-capped, and the RF=1 log returns the stored bytes. `Node::read_envelopes(p, from, max_bytes)` (`ostro-partition/src/node.rs`). |
| Snapshot install (08-02) | `ostro-partition/src/store/snapshot.rs` | `pub fn install_snapshot<R: Read>(data_dir: &Path, p: PartitionId, count: PartitionCount, cfg: &StoreConfig, r: R) -> Result<SnapshotInfo, SnapshotError>`, plus `#[doc(hidden)] install_snapshot_with(.., writer: InstallWriter)`. The order is staged `.install-pNNNN`, digest check, rename aside `.old-pNNNN`, fsync `state/`. The caller must have closed the store. `OsnpReader::next_frame()`. `SnapshotInfo { partition, partition_count, state_format, applied_offset, state_hash, latest_epoch, logical_digest, total_rows, bytes, blob_refs }`. |
| Snapshot service (08-09) | `ostro-partition/src/leader/snapshot.rs` | `SnapshotService::new(partition, count, data_dir, store, cfg: SnapshotConfig, clock, shared: Arc<SnapshotShared>, retained: Vec<LocalSnapshot>) -> Arc<Self>`; `take()`; `local_floor() -> watch::Receiver<Offset>`; `spawn(ctx: LeaderCtx, svc)`. Started by `leader::spawn_all(ctx, cfg)` when `LeaderCtx.snapshots` is `Some` and enabled. Today it runs on the leader only; 12-13 moves it to both roles. |
| Uploader (08-13/08-23) | `ostro-backup/src/uploader.rs` | `Uploader::new(node: Arc<Node>, meta: Option<MetaView>, clock: Arc<dyn Clock>, cfg: UploaderConfig) -> Self`. There is no deps struct. **12-02 added** `Uploader::with_cluster(self, UploaderClusterDeps) -> Self` and `cluster() -> Option<&UploaderClusterDeps>`. Key helpers: `snap_key`, `snap_prefix`, `meta_key`, `blob_key`. |
| Chain walk-back (08-13) | `ostro-backup/src/chunk.rs` | `pub async fn walk_back(store: &Arc<dyn ObjectStore>, last_chunk_key: &str, last_chunk_sha256: [u8; 32], stop: u64) -> Result<Vec<ChunkRef>, ChunkError>` (forward order, chain-verified). |
| Fenced metric (08-13) | `uploader.rs` | `METRIC_FENCED = "ostro_backup_fenced"` (gauge). The Phase 12 per-partition metric is `ostro_repl_upload_fenced{partition}`. |
| Restore (08-18) | `ostro-backup/src/restore.rs` | `pub async fn restore(source: RestoreSource, opts: RestoreOptions) -> Result<RestoreReport, RestoreError>` (refuses a non-empty data dir). |
| Cluster CA (07-06) | `ostro-server/src/tls/ca.rs` | `ClusterCa::{load_or_create(dir, cluster_id), import_pem(dir, cluster_id, ca_crt_pem, ca_key_pem), issue_node_cert(&self, node_id, sans), load_or_issue_node_cert, root_store(), cluster_id()}`. `NodeIdentity { cluster_id, node_id, not_after }`; `NodeIdentity::uri(cluster_id, node_id)` gives `ostro://<cluster_id>/node/<node_id>`; `NodeIdentity::from_der` rejects worker certs. |
| CA cluster id storage | `ca.rs` | It is stored only in the CA certificate's subject CN, `"ostro cluster <id>"`. There is no public reader: 12-14 parses the `ca.crt` CN with x509-parser. The authoritative id is `ClusterInfo.cluster_id`. |
| Cluster listener (07-09) | `ostro-server/src/workers/service.rs` | `start(ctx: &P7Ctx) -> Result<Option<JoinHandle<()>>, P7Error>` returns **`None` when `[workers] enabled = false`**. D-P12-13 (12-14) must bind it in every profile. `serve(listener, deps: ClusterDeps)` serves with `.client_ca_root(..)`. |
| Derived keyspaces | `ostro-partition/src/store/derived.rs`, `node.rs` | `x_run_index` (Phase 11, 11-05): `PartitionStore::start_run_index_rebuild(&self) -> bool` (background thread), plus `run_index_state()`, `join_run_index_rebuild()`, `delete_run_index_marker()`. **After a snapshot install, the reopened store must call `start_run_index_rebuild()`** (12-12/12-13; written into the `InstallGuard::new` and `install.rs` docs). |
| Wiped-voter detection | `vendor/ostro-raft-driver/src/driver.rs` | `MsgHeartbeat` with `commit > last_index` is fatal. The text still says `CHRONIK_CLUSTER_JOIN=true`, and 12-10 replaces it. |
| Clock | `ostro-partition/src/clock.rs`, `ostro-server/src/server.rs` | `trait Clock { fn now_ms(&self) -> u64 }` (`SystemClock`, `ManualClock`); `ServerOptions.clock` + `with_clock`; `Server::clock()`; `P7Ctx.clock`. |

## Contracts as landed

### cluster.proto (`crates/ostro-proto/proto/ostro/cluster/v1/cluster.proto`, package `ostro.cluster.v1`)

- `service Replication { Fetch(FetchRequest) -> FetchResponse; FetchSnapshot(FetchSnapshotRequest) -> stream SnapshotFrame; FetchBlob(FetchBlobRequest) -> stream BlobChunk; PushBlob(stream BlobChunk) -> PushBlobAck }`
- `FetchRequest { partition u32 = 1, replica_id u64 = 2, leader_epoch u32 = 3, fetch_offset u64 = 4, last_fetched_epoch u32 = 5, durable_leo u64 = 6, max_bytes u32 = 7, max_wait_ms u32 = 8, min_bytes u32 = 9 }`
- `FetchResponse { error FetchError = 1, leader_epoch u32 = 2, high_watermark u64 = 3, log_start u64 = 4, leader_durable_end u64 = 5, optional DivergingEpoch diverging = 6, repeated bytes envelopes = 7 }`; `DivergingEpoch { epoch u32 = 1, end_offset u64 = 2 }`
- `enum FetchError { FETCH_ERROR_NONE, FETCH_ERROR_FENCED_LEADER_EPOCH, FETCH_ERROR_UNKNOWN_LEADER_EPOCH, FETCH_ERROR_NOT_LEADER, FETCH_ERROR_NOT_REPLICA, FETCH_ERROR_SNAPSHOT_REQUIRED }` (0-5). In Rust: `FetchError::{None, FencedLeaderEpoch, UnknownLeaderEpoch, NotLeader, NotReplica, SnapshotRequired}`.
- `FetchSnapshotRequest { partition u32, replica_id u64, resume_frame u64 }`, `SnapshotFrame { index u64, bytes bytes, last bool }`, `FetchBlobRequest { sha256 bytes }`, `BlobChunk { sha256 bytes, offset u64, bytes bytes, last bool }`, `PushBlobAck { sha256 bytes, stored bool }`
- `service MetaPeer { Raft(stream RaftBatch) -> RaftAck; Propose(ProposeRequest) -> ProposeResponse; ReportSuspect(SuspectReport) -> SuspectAck }`
- `RaftBatch { from u64, repeated bytes messages }` (each entry an encoded `eraftpb::Message`), `RaftAck {}`, `ProposeRequest { command bytes }` (encoded MetaCommand), `ProposeResponse { index u64, term u64, result bytes, error_kind string, leader_hint u64 }`, `SuspectReport { reporter u64, suspect u64, optional uint32 partition, reason string }`, `SuspectAck {}`

### ostro-meta

```rust
// command.rs (appended; ordinals 0-3 byte-identical, pinned by a test and the golden)
MetaCommand::ElectLeader { partition: u16, epoch: u32, leader: u64, isr: Vec<u64>, expected_isr_version: u64, info: ElectionInfo } // 4
MetaCommand::AlterIsr { partition: u16, epoch: u32, expected_isr_version: u64, isr: Vec<u64> }                                 // 5
MetaCommand::ResignLeader { partition: u16, epoch: u32, node: u64, info: ElectionInfo }                                         // 6
// state.rs apply: all three -> MetaResult::Invalid("Phase 12: implemented in 12-05")
// ext.rs (path ostro_meta::ext::*; lib.rs re-exports are unchanged)
pub const EXT_ELECTION_INFO: u32 = 80;
pub const ELECTION_REASON_BOOTSTRAP: u8 = 0; STARTUP = 1; FAILOVER = 2; RESIGN = 3; OFFLINE = 4;  // ELECTION_REASON_*
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ElectionInfo { pub reason: u8, pub elected_at_ms: u64, pub prev_leader: u64 }
pub fn get_election_info(ext: &[u8]) -> Result<Option<ElectionInfo>, ExtError>;
pub fn put_election_info(ext: &[u8], info: &ElectionInfo) -> Result<Vec<u8>, ExtError>;
```

### ostro-log

```rust
// replicated.rs (re-exported from the crate root)
pub struct ReplicaSet { pub replicas: Vec<u64>, pub isr: Vec<u64>, pub isr_version: u64, pub min_insync: u8 }            // Clone, Debug, Default, PartialEq
pub struct FollowerProgress { pub replica: u64, pub durable_leo: Offset, pub fetch_offset: Offset, pub now_ms: u64 }       // Copy
pub struct FollowerStatus { pub replica: u64, pub durable_leo: Offset, pub fetch_offset: Offset, pub last_caught_up_ms: u64, pub in_sync: bool }
pub struct ReplicaStatus { pub role: Role, pub epoch: Epoch, pub hw: Offset, pub durable_end: Offset, pub published_isr: Vec<u64>, pub isr_version: u64,
    pub min_insync: u8, pub followers: Vec<FollowerStatus>, pub hw_publishes: u64, pub max_envelopes_per_fetch: u32 }
// trait ReplicatedLog, required methods (no defaults):
fn read_replica(&self, from: Offset, max_bytes: usize) -> impl Future<Output = Result<Vec<RawBatch>, LogError>> + Send;
fn append_replica(&self, batches: Vec<RawBatch>) -> impl Future<Output = Result<Offset, LogError>> + Send;
fn durable_end(&self) -> Offset;
fn durable_end_watch(&self) -> watch::Receiver<Offset>;
fn latest_epoch(&self) -> Option<Epoch>;
fn become_leader(&self, epoch: Epoch, set: ReplicaSet) -> Result<(), LogError>;
fn become_follower(&self, epoch: Epoch, leader: u64) -> Result<(), LogError>;
fn set_follower_hw(&self, leader_hw: Offset);
fn record_progress(&self, p: FollowerProgress) -> Result<(), LogError>;
fn set_published_isr(&self, epoch: Epoch, set: ReplicaSet) -> Result<(), LogError>;
fn reset_to(&self, start: Offset, epochs: EpochHistory) -> impl Future<Output = Result<(), LogError>> + Send;
fn replica_status(&self) -> ReplicaStatus;
// error.rs
LogError::NotEnoughReplicas { isr: usize, min: usize }   // "not enough replicas: isr {isr} < min_insync {min}"
LogError::NotLeader { leader: Option<u64> }
// mutants.rs: ISR_FROM_ASSIGNMENT, HW_LEADER_LEO, COUNT_NONDURABLE_LEO, SKIP_EPOCH_CHECK, NO_TRUNCATE, UNCLEAN_ELECTION,
//   IGNORE_MIN_INSYNC, SHRINK_BEFORE_COMMIT, UPLOAD_ABOVE_HW, RF1_BYPASS: &str = "repl::mutant::<name>"; pub const ALL: [&str; 10]
```

RF=1 stub behaviour: `read_replica`, `append_replica`, `become_leader`, `become_follower`, `record_progress`, `set_published_isr` and `reset_to` return `Err(LogError::Failed("Phase 12: implemented in 12-08"))`. `set_follower_hw` is a no-op. `durable_end` returns the HW, which RF=1 publishes only after fsync. `durable_end_watch` returns the HW watch. `latest_epoch` returns `epochs().latest()`. `replica_status` returns the current role/epoch/HW, `published_isr = [node_id]`, `min_insync = 1` and `hw_publishes = 0`. Wrappers forward every method: `ManualHwLog` (ostro-partition tests) and `CappedLog` (ostro-log `read_envelopes` test).

### ostro-partition `roles.rs` (re-exported from the crate root)

```rust
pub type PartitionLogHandle = ostro_log::Rf1Log;    // the only line that names the type (12-08 changes it)
#[doc(hidden)] pub async fn open_partition_log_for_tests(wal_dir: &Path, partition: PartitionId, node_id: u64, floor: Offset)
    -> Result<(LogNode, Arc<PartitionLogHandle>), LogError>;   // partition count = partition + 1; keep LogNode alive, shutdown() at the end
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionAssignmentView { pub partition: PartitionId, pub leader: u64, pub epoch: Epoch, pub replicas: Vec<u64>, pub isr: Vec<u64>, pub isr_version: u64, pub min_insync: u8 }
pub trait AssignmentSource: Send + Sync + 'static {
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>>;
    fn local_node(&self) -> u64;
    fn request_term(&self, p: PartitionId, min_epoch: Epoch) -> BoxFuture<'_, ()>;
    fn resign(&self, p: PartitionId, epoch: Epoch) -> BoxFuture<'_, ()>;
}
pub trait FollowerDriver: Send + Sync + 'static { fn start(&self, p: PartitionId, epoch: Epoch, leader: u64, log: Arc<PartitionLogHandle>) -> Box<dyn FollowerTask>; }
pub trait FollowerTask: Send { fn stop(self: Box<Self>) -> BoxFuture<'static, ()>; }
pub trait PartitionRoles: Send + Sync + 'static { fn led(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>>; }
pub trait PartitionControl: Send + Sync + 'static { fn quiesce_for_install(&self, p: PartitionId) -> BoxFuture<'_, Result<InstallGuard, PartitionError>>; }
pub struct InstallGuard { pub state_root: PathBuf, pub log: Arc<PartitionLogHandle>, pub store_cfg: StoreConfig, /* private reopen closure */ }
impl InstallGuard {
    pub fn new(state_root: PathBuf, log: Arc<PartitionLogHandle>, store_cfg: StoreConfig,
               reopen: impl FnOnce() -> BoxFuture<'static, Result<(), PartitionError>> + Send + 'static) -> Self;
    pub fn reopen(self) -> BoxFuture<'static, Result<(), PartitionError>>;   // reopen must run start_run_index_rebuild()
}
```

### ostro-cluster

```rust
// transport/mod.rs
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)] pub struct PeerNode(pub u64);
pub struct MemConn { pub io: tokio::io::DuplexStream, pub peer: PeerNode }
pub enum ClusterIncoming { Tcp, Memory(mpsc::Receiver<MemConn>) }
pub enum TransportError { UnknownNode(u64), Connect { node: u64, reason: String } }
pub trait ClusterTransport: Send + Sync + 'static {
    fn channel(&self, node: u64) -> Result<tonic::transport::Channel, TransportError>;
    fn incoming(&self) -> ClusterIncoming;      // called once by the listener owner
}
// liveness.rs
pub trait SuspectSink: Send + Sync { fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>); }
// authz.rs (stub -> Status::unimplemented until 12-05)
pub fn require_node<T>(req: &tonic::Request<T>) -> Result<u64, Status>;
// install.rs (shapes owned by 12-12, #[non_exhaustive] placeholders)
pub struct InstallCtx {}  pub enum InstallOutcome { Installed }  pub enum InstallError { NotImplemented }
pub async fn install_from_leader(ctx: &InstallCtx, p: PartitionId, leader: u64) -> Result<InstallOutcome, InstallError>;
// identity.rs (12-14 implements)
pub enum ClusterIdSource { Configured(String), NodeToml(String), StoredCa(String), Meta(String), GenerateFresh }
pub struct ConfStateView { pub voters: Vec<u64>, pub learners: Vec<u64> }
pub enum BootError { NotImplemented }   // #[non_exhaustive]; 12-14 adds variants
pub fn resolve_cluster_id(configured: Option<&str>, node_toml: Option<&str>, stored_ca: Option<&str>,
                          persisted_cluster_info: Option<&str>, meta_blank: bool) -> Result<ClusterIdSource, BootError>;
pub fn is_joiner(meta_blank: bool, partition_data_exists: bool) -> bool;     // already implemented (a && b)
pub fn check_meta_voters(persisted: Option<ConfStateView>, expected: &[u64]) -> Result<(), BootError>;
// service.rs
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;
pub struct ReplicationService {}   // #[non_exhaustive]; impl Replication: every RPC Status::unimplemented until 12-14
```

### ostro-backup

```rust
// uploader.rs
#[derive(Clone)] pub struct UploaderClusterDeps { pub roles: Arc<dyn ostro_partition::roles::PartitionRoles>, pub members: watch::Receiver<usize> }
impl Uploader { pub fn with_cluster(self, deps: UploaderClusterDeps) -> Self; pub fn cluster(&self) -> Option<&UploaderClusterDeps>; }
```

### ostro-server

- `config.rs`: `ServerConfig.replication: ReplicationSection { session_timeout "3s", replica_lag_max "10s", meta_tick_interval "100ms", meta_election_tick 10, meta_heartbeat_tick 2, fetch_max_wait "500ms", fetch_read_timeout "5s", fetch_max_bytes 8 MiB (ByteSize), rf: Option<u8>, min_insync: Option<u8> }`. `"replication"` is appended to `SECTIONS`, so `OSTRO_REPLICATION__*` env vars work.
- `ClusterSection` gains `cluster_id: Option<String>`, `replication_listen: Option<SocketAddr>` and `peers: Vec<PeerConfig { id: u64, addr: String, repl_addr: Option<String> }>`. `ClusterTlsSection` gains `ca_cert_file` and `ca_key_file: Option<PathBuf>`.
- `ServerConfig::validate_phase12()` is called from `load_layered`:
  - non-empty peers require `cluster.cluster_id`, `cluster.tls.ca_cert_file` and `cluster.tls.ca_key_file` (the error lists all missing keys);
  - each peer id is non-zero, unique, not equal to `[node] node_id`, and has a non-empty addr;
  - `cluster_id`, when set, is non-blank;
  - the replication durations are non-zero, `1 <= meta_heartbeat_tick < meta_election_tick`, `fetch_max_bytes >= 1`, `rf >= 1` and `min_insync >= 1` when set, and `min_insync <= rf`.
- `--print-config` renders `peers` as an array of inline tables, and it round-trips.
- `server.rs`: `ServerOptions.cluster_transport: Option<Arc<dyn ostro_cluster::transport::ClusterTransport>>` + `ServerOptions::with_cluster_transport(..)`. `start_inner` takes it out and drops it until 12-14.

### ostro-repl-conformance

```rust
// harness/mod.rs
pub struct ClusterSpec { pub nodes: usize, pub rf: usize, pub min_insync: usize, pub partitions: u16, pub objstore: bool, pub seed: u64 } // Default 3/3/2/4/false/0; ClusterSpec::single()
pub enum HarnessError { Start(String), Deadline(Duration), Panic(String), Other(String) }
pub struct ApiClient { pub base_url: String, pub http: reqwest::Client }   // 12-03 adds inherent methods in inproc.rs
pub struct DataDirView { pub node: u64, pub root: PathBuf }
pub struct MetaLogEntry { pub index: u64, pub command: MetaCommand, pub applied_result: MetaResult }
pub trait ClusterHarness { start(spec) -> impl Future<Output = Result<Self, HarnessError>>; node_ids; client; assignment(p: u16) -> impl Future<Output = PartitionAssignmentView>;
    isolate; heal; partition(a, b); kill -> impl Future<Output = ()>; restart -> impl Future<Output = Result<(), HarnessError>>; node_disk -> DataDirView;
    bucket -> Option<PathBuf>; meta_log -> impl Future<Output = Vec<MetaLogEntry>>; admin_status -> impl Future<Output = serde_json::Value>; stop(self) }
// harness/inproc.rs: pub struct InProcCluster {}  (#[non_exhaustive] placeholder, 12-03)
// scenarios/mod.rs
pub const DEFAULT_DEADLINE: Duration = 120 s;  pub enum Tier { B, C }
pub struct ScenarioFailure { pub oracle: &'static str, pub detail: String }   // Display "oracle: detail"
pub type ScenarioResult = Result<(), ScenarioFailure>;
#[derive(Clone, Copy)] pub struct Scenario { pub name, pub group, pub tier: Tier, pub deadline: Duration, pub spec: fn() -> ClusterSpec,
    pub run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult> }
pub fn all() -> Vec<Scenario>;
// scenarios/runner.rs: pub async fn red_run_all()  (panics until 12-03)
// pending/mod.rs: pub fn is_pending(name: &str) -> bool; pub fn all_lists() -> [&'static [&'static str]; 3]   (core, rest, process)
// pending/{core,rest,process}.rs: pub const PENDING: &[&str] = &[];
// mutants.rs: pub const MATRIX: &[(&str, &[&str])] = &[];
```

`tests/skeleton.rs` checks that scenario names are unique and that every pending entry and matrix entry names a registered scenario. It also holds `#[ignore] no_pending_left` (run by 12-17/12-18). `tests/red_run.rs` is `#[ignore] #[tokio::test(flavor = "multi_thread")]`, calls `runner::red_run_all().await`, and is never edited later.

## Stub-module map (file → owning plan)

| File(s) | Owner |
|---------|-------|
| `ostro-log/src/replica/{tracker,isr,watermark,divergence,failover}.rs` | 12-04 |
| `ostro-cluster/src/{transport/tcp,authz,address_book,meta_peer}.rs` | 12-05 |
| `ostro-cluster/src/{controller,liveness,bootstrap,members}.rs` | 12-10 (`liveness::SuspectSink` trait fixed) |
| `ostro-cluster/src/{fetch,fetcher,blob,isr_publisher}.rs` | 12-11 |
| `ostro-cluster/src/{snapshot,install}.rs` | 12-12 |
| `ostro-cluster/src/{service,status,config,identity}.rs` | 12-14 |
| `ostro-partition/src/replica_services.rs` | 12-13 |
| RF=1 log replica methods (`rf1.rs`) | 12-08 |
| MetaCommand 4-6 apply (`state.rs`) | 12-05 |
| `ostro-backup/src/cluster.rs` | 12-15 |
| `ostro-server/src/cluster.rs` | 12-14 |
| `ostro-repl-conformance/src/harness/{certs,fault,inproc}.rs`, `oracle/*.rs`, `scenarios/runner.rs` | 12-03 |
| `ostro-repl-conformance/src/harness/{process,chaos_proxy}.rs` | 12-09 |
| `scenarios/{placement,isr_honesty,hw,stale_epoch,divergence,failover,min_insync}.rs`, `pending/core.rs` | 12-06 |
| `scenarios/{standby,bootstrap,fetch_path,blobs,one_path,zombie,object_storage}.rs`, `pending/rest.rs`, `mutants.rs` | 12-07 |
| `scenarios/process.rs`, `pending/process.rs` | 12-17 |

## Deviations from Plan

### Exhaustive-match / trait-impl arms outside `files_modified`

- `crates/ostro-partition/tests/common/manual_hw.rs` and `crates/ostro-log/tests/read_envelopes.rs` (`CappedLog`): the two other `ReplicatedLog` implementations forward the 12 new methods. No other crate matches exhaustively on `MetaCommand`, so nothing else needed an arm.
- `crates/ostro-server/tests/config.rs`: the Phase 12 parse and rejection tests, in both profiles, live here because the plan's verify runs `--test config`.

### Auto-fixed issues

**1. [Rule 3 - Blocking] `ostro-log/tests/hw_cut.rs` could not read the new `src/replica/` directory.**
- **Found during:** Task 1 verify.
- **Issue:** `hw_watch_written_only_in_publish_point` scanned `src/` with `read_to_string` on every entry and panicked on a directory.
- **Fix:** the scan now recurses, so `replica/*.rs` (12-04) is also checked for watch writes outside `cut.rs`. The guarantee is wider, not weaker.
- **Commit:** 4c165f7.

**2. [Plan inconsistency] `PartitionLogHandle` must name the concrete type.**
- **Issue:** The Task 2 grep `! grep -rn Rf1Log ... crates/ostro-partition/src/roles.rs` cannot pass. The plan defines `PartitionLogHandle` as "the concrete log handle Node holds today", and 12-08 is told to change that alias line.
- **Resolution:** exactly one line in roles.rs names it: `pub type PartitionLogHandle = ostro_log::Rf1Log;`. No stub and nothing in ostro-cluster or ostro-repl-conformance names `Rf1Log`, and the grep over those passes.

**3. Dep anchors.** I removed `phase12_dep_anchors` from ostro-partition (futures-util) and ostro-server (ostro-cluster), because both crates are now used. The ostro-cluster and ostro-repl-conformance `dep_anchors` are kept.

## Plan gate

I ran `ci-local --no-musl` in a detached worktree at **23e6fe0** (the Task 2 commit, landed on main):

```
cargo fmt --check                    PASS
cargo clippy -D warnings             PASS
cargo build (warning-free)           PASS
sdk/guests/order-saga                PASS
cargo test                           FAIL  2847s
polyglot (python/ts/conformance)     PASS
semantics sleepers (serial)          PASS
failpoint list sync (ci.yml)         PASS
failpoint suites                     PASS  4354s
server campaign smoke (20 kills)     PASS
phase 7 failpoint legs + manifests   PASS
cargo deny / vendored / deny-fixtures / no-chronik / workflow validation  PASS
```

The only red was a timing assertion, `ostro-raft-driver --lib driver::tests::commits_do_not_wait_for_ticks`. It measured 61.46 ms per sequential commit against a 60 ms bound, on a loaded machine with other agents' builds running. The vendored driver is untouched by this plan. The test passed 3 of 3 times on rerun (about 4.4 s each). `cargo test` stops at the first failing binary, so I re-ran `cargo test --workspace --locked --no-fail-fast` at 23e6fe0: **EXIT=0, 316 result lines, 2678 passed, 0 failed.**

Per-task verifies:
- Task 1: workspace build warning-free. ostro-proto, ostro-meta, ostro-log (`--no-fail-fast`, after the hw_cut fix) and ostro-partition (463 passed) are all green. No golden line removed (`tests/golden` untouched).
- Task 2: build warning-free, clippy `-D warnings` clean, `--test skeleton` 2 passed (1 ignored), `--test config` 44 passed. No `Rf1Log` in ostro-cluster or ostro-repl-conformance.
- After rebasing onto the moved main (2a03bf9), `cargo check --workspace --all-targets` was clean before the CAS.

**Gate verdict: green at 23e6fe0.**

## Open items for later plans

- 12-12/12-13: the `InstallGuard` reopen closure must call `PartitionStore::start_run_index_rebuild()`. This is documented on `InstallGuard::new` and in `install.rs`.
- 12-14: D-P12-13 needs the cluster listener in every profile, but `workers::service::start` returns `None` when workers are disabled. 12-14 also needs a reader for the cluster id in `ca.crt`'s CN, and wires `ServerOptions::cluster_transport` (currently dropped in `start_inner`).
- 12-03: `InProcCluster`, `ApiClient` methods, `red_run_all`, and the oracles fill the placeholders. The `MemConn.io` type is `tokio::io::DuplexStream`.
- 12-08: renames the type by changing the alias line in `roles.rs`. The replica methods on the RF=1 log are stubs, and `durable_end` currently equals the HW.

## Self-Check: PASSED

- Files: cluster.proto, ostro-log mutants.rs + replica/, ostro-partition roles.rs, ostro-cluster modules, conformance harness/scenarios/pending/tests are all present at main.
- Commits: 4c165f7 and 23e6fe0 are on main.
