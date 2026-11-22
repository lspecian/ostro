//! Test support for the 12-12 snapshot suites: a leader with real state (a `PartitionStore`
//! built by `apply_command`) and a real `PartitionLog` anchored at the state's applied
//! offset, a follower opened the way `Node::open` opens a partition, a fake
//! `PartitionControl` over that follower (12-13 implements the real one on `Node`), and a
//! test `Replication` service serving `Fetch` + `FetchSnapshot` over loopback mTLS with
//! stream cuts and frame corruption hooks.
//!
//! Needs `mod support_meta;` in the including test (CA and node certificates).

#![allow(dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;
use ostro_cluster::address_book::{AddressBook, PeerEntry};
use ostro_cluster::fetch::{self, FetchConfig, FetchCtx, fetch_decode_limit};
use ostro_cluster::fetcher::FetcherConfig;
use ostro_cluster::service::BoxStream;
use ostro_cluster::snapshot::{
    Exporter, SNAPSHOT_MESSAGE_LIMIT, SnapshotCtx, SnapshotServeConfig, serve_snapshot,
};
use ostro_cluster::transport::tcp::TcpTransport;
use ostro_log::{LogNode, LogNodeConfig, PartitionLog, ReplicaSet, ReplicatedLog};
use ostro_partition::apply::{ApplyOutcome, Cursor, apply_command};
use ostro_partition::leader::snapshot::{snapshot_dir, snapshot_file_name};
use ostro_partition::roles::{InstallGuard, PartitionAssignmentView, PartitionControl};
use ostro_partition::store::derived::IndexState;
use ostro_partition::store::snapshot::{RawFrame, recover_install};
use ostro_partition::{Clock, PartitionError, PartitionStore, StoreConfig, SystemClock};
use ostro_proto::cluster::v1::replication_client::ReplicationClient;
use ostro_proto::cluster::v1::replication_server::{Replication, ReplicationServer};
use ostro_proto::cluster::v1::{
    BlobChunk, FetchBlobRequest, FetchRequest, FetchResponse, FetchSnapshotRequest, PushBlobAck,
    SnapshotFrame,
};
use ostro_types::command::{Cmd, ConflictPolicy, StartRun};
use ostro_types::payload::{PayloadRef, payload_ref};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId, RunId};
use prost::Message;
use sha2::{Digest as _, Sha256};
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Server;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status, Streaming};

use crate::support_meta::{CLUSTER, TestCa};

pub const P: PartitionId = PartitionId(0);
pub const LEADER: u64 = 1;
pub const FOLLOWER: u64 = 2;
/// The leader's term (its log history: epoch 1 from the state, epoch 2 its own).
pub const EPOCH: u32 = 2;

pub fn count() -> PartitionCount {
    PartitionCount::new(1).unwrap()
}

pub fn test_ca() -> TestCa {
    TestCa::new(CLUSTER)
}

/// Poll `f` every 5 ms until it holds (panics after `within`).
pub async fn wait_until(what: &str, within: Duration, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + within;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

// --------------------------------------------------------------------------- state

/// Run `i` of partition 0 (18-byte id: partition, 16 bytes derived from `i`).
pub fn run_id(i: u64) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&P.0.to_be_bytes());
    b[2] = 0x70;
    b[10..].copy_from_slice(&i.to_be_bytes());
    RunId::from_bytes(&b).unwrap()
}

/// The `i`-th state command: offset 0 is `LeaderAnnounced{1}`, then `StartRun`s with a
/// `payload`-byte inline input.
pub fn state_cmd(i: u64, payload: usize) -> Bytes {
    if i == 0 {
        return Bytes::from(Command::leader_announced(1, LEADER, 0).encode_to_vec());
    }
    let mut input = format!("input-{i}-").into_bytes();
    input.resize(payload.max(input.len()), b'x');
    let c = Command {
        cmd: Some(Cmd::StartRun(StartRun {
            run_id: run_id(i).as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "q".into(),
            input: Some(PayloadRef {
                kind: Some(payload_ref::Kind::Inline(Bytes::from(input))),
            }),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            ready_at_ms: 100,
            requested_at_ms: 100,
            rng_seed: vec![7; 32],
            retention_ms: 1_000,
            ..StartRun::default()
        })),
    };
    Bytes::from(c.encode_to_vec())
}

/// Apply state commands `from..to` to `store` (at offsets `from..to`, epoch 1).
pub fn apply_state(store: &PartitionStore, from: u64, to: u64, payload: usize) {
    let meta = store.read_meta().unwrap();
    assert_eq!(
        meta.applied.0, from,
        "apply_state from the store's applied offset"
    );
    let mut cur = Cursor::from_meta(&meta);
    for i in from..to {
        let bytes = state_cmd(i, payload);
        let mut b = store.batch();
        let mut actions = Vec::new();
        let out = apply_command(&mut b, &mut cur, Offset(i), Epoch(1), &bytes, &mut actions)
            .expect("apply");
        assert_eq!(out, ApplyOutcome::Accepted, "state command {i}");
        let mu = cur.meta_update();
        b.commit(&mu).unwrap();
    }
    store.checkpoint(Offset(to)).unwrap();
}

/// sha256 over every `x_run_index` row (marker excluded) once the index is ready.
pub fn run_index_digest(store: &PartitionStore) -> ([u8; 32], usize) {
    store.join_run_index_rebuild();
    assert_eq!(
        store.run_index_state(),
        IndexState::Ready,
        "x_run_index ready"
    );
    let rows = store
        .reader()
        .run_index_rows(&[], None, usize::MAX)
        .unwrap();
    let mut h = Sha256::new();
    let mut n = 0;
    for (k, v) in rows.iter().filter(|(k, _)| k[0] != 0x00) {
        h.update((k.len() as u32).to_le_bytes());
        h.update(k);
        h.update((v.len() as u32).to_le_bytes());
        h.update(v);
        n += 1;
    }
    (h.finalize().into(), n)
}

/// An encoded `StartRun` used as a log-only record (never applied by the follower here).
pub fn log_cmd(i: u64) -> Bytes {
    state_cmd(10_000 + i, 16)
}

pub async fn wait_durable(log: &PartitionLog, end: Offset) {
    let mut rx = log.durable_end_watch();
    tokio::time::timeout(Duration::from_secs(10), rx.wait_for(|d| *d >= end))
        .await
        .expect("durable in time")
        .expect("durable watch");
}

/// Every stored envelope of `log` from `from` (durable-capped), verbatim.
pub async fn envelopes(log: &PartitionLog, from: Offset) -> Vec<Bytes> {
    let mut out = Vec::new();
    let mut at = from;
    loop {
        let batches = log.read_replica(at, 1 << 20).await.expect("read_replica");
        if batches.is_empty() {
            return out;
        }
        for b in batches {
            at = b.end;
            out.extend(b.envelopes);
        }
    }
}

// --------------------------------------------------------------------------- leader

/// The leader (node 1): state applied to `S`, log anchored at `S` (history `[(1, 0)]`),
/// leading epoch 2 with replicas {1, 2}.
pub struct LeaderFx {
    pub dir: TempDir,
    pub store: Arc<PartitionStore>,
    pub log_node: LogNode,
    pub log: Arc<PartitionLog>,
    pub s: Offset,
}

impl LeaderFx {
    /// State = `runs` runs (`S = runs + 1`), each with a `payload`-byte input.
    pub async fn new(runs: u64, payload: usize) -> LeaderFx {
        let dir = TempDir::new().unwrap();
        let (store, _) =
            PartitionStore::open(dir.path(), P, count(), &StoreConfig::default()).unwrap();
        apply_state(&store, 0, runs + 1, payload);
        let s = Offset(runs + 1);
        let cfg = LogNodeConfig::new(dir.path().join("wal"), count(), LEADER);
        let (log_node, mut logs) = LogNode::open(cfg, &[s]).await.unwrap();
        let log = logs.swap_remove(0).unwrap();
        assert_eq!(log.log_start(), s);
        log.seed_epochs(&[(Epoch(1), Offset(0))]).await.unwrap();
        log.become_leader(
            Epoch(EPOCH),
            ReplicaSet {
                replicas: vec![LEADER, FOLLOWER],
                isr: vec![LEADER],
                isr_version: 0,
                min_insync: 1,
            },
        )
        .unwrap();
        LeaderFx {
            dir,
            store: Arc::new(store),
            log_node,
            log,
            s,
        }
    }

    pub fn data_dir(&self) -> &Path {
        self.dir.path()
    }

    /// Append `n` log-only records at epoch 2 (offsets `S..`), wait until committed.
    pub async fn append(&self, n: u64) -> Offset {
        let base = self.log.log_end().0;
        let cmds = (0..n).map(|i| log_cmd(base + i)).collect();
        let r = self.log.append(Epoch(EPOCH), cmds).await.expect("append");
        let end = r.last.next();
        wait_durable(&self.log, end).await;
        self.log.wait_committed(r.last).await.unwrap();
        end
    }

    /// Write the current state as the local snapshot `snapshots/p0000/<applied>.osnap`.
    pub fn write_local_snapshot(&self) -> PathBuf {
        let dir = snapshot_dir(self.data_dir(), P);
        std::fs::create_dir_all(&dir).unwrap();
        let mut buf = Vec::new();
        let info = self.store.export_snapshot(&mut buf, 1).unwrap();
        let path = dir.join(snapshot_file_name(Offset(info.applied_offset)));
        std::fs::write(&path, &buf).unwrap();
        path
    }

    /// The leader's snapshot bytes as exported now.
    pub fn export_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.store.export_snapshot(&mut buf, 1).unwrap();
        buf
    }

    pub fn snapshot_ctx(
        &self,
        assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
        exporter: Exporter,
        clock: Arc<dyn Clock>,
        cfg: SnapshotServeConfig,
    ) -> SnapshotCtx {
        SnapshotCtx::new(
            Arc::clone(&self.log),
            count(),
            self.data_dir(),
            assignments,
            exporter,
            clock,
            cfg,
        )
    }

    pub fn fetch_ctx(
        &self,
        assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    ) -> FetchCtx {
        FetchCtx::new(
            Arc::clone(&self.log),
            assignments,
            Arc::new(SystemClock),
            FetchConfig {
                max_wait: Duration::from_millis(200),
                ..FetchConfig::default()
            },
        )
    }

    pub async fn shutdown(self) {
        self.log_node.shutdown().await;
    }
}

pub type Assignments = watch::Sender<Arc<Vec<PartitionAssignmentView>>>;

/// Partition 0 led by node 1 at epoch 2 with `replicas`.
pub fn assignments(replicas: &[u64]) -> Assignments {
    watch::channel(Arc::new(vec![PartitionAssignmentView {
        partition: P,
        leader: LEADER,
        epoch: Epoch(EPOCH),
        replicas: replicas.to_vec(),
        isr: vec![LEADER],
        isr_version: 0,
        min_insync: 1,
    }]))
    .0
}

// --------------------------------------------------------------------------- follower

/// Opens partition 0 of `data_dir` the way `Node::open` does: finish an interrupted
/// install, open (or create) the state, recover the log anchored at the state's applied
/// offset, seed an empty log's epochs from the cursor.
pub async fn open_partition(
    data_dir: &Path,
    node: u64,
) -> (Arc<PartitionStore>, LogNode, Arc<PartitionLog>) {
    recover_install(data_dir, P).unwrap();
    let (store, meta) = PartitionStore::open(data_dir, P, count(), &StoreConfig::default())
        .expect("open follower state");
    let cfg = LogNodeConfig::new(data_dir.join("wal"), count(), node);
    let (log_node, mut logs) = LogNode::open(cfg, &[meta.applied]).await.unwrap();
    let log = logs.swap_remove(0).unwrap();
    assert!(
        meta.applied <= log.log_end() && meta.applied >= log.log_start(),
        "state {} outside the log [{}, {})",
        meta.applied,
        log.log_start(),
        log.log_end()
    );
    if log.log_end() == log.log_start() && log.epochs().0.is_empty() {
        let h: Vec<(Epoch, Offset)> = meta
            .cursor
            .epoch_history
            .iter()
            .map(|&(e, o)| (Epoch(e), Offset(o)))
            .collect();
        log.seed_epochs(&h).await.unwrap();
    }
    store.start_run_index_rebuild();
    (Arc::new(store), log_node, log)
}

/// The fake partition lifecycle of the follower: quiesce = close the store; reopen = open
/// it again, check it agrees with the log, run the x_* rebuild hook.
pub struct FakeControl {
    pub data_dir: PathBuf,
    pub log: Arc<PartitionLog>,
    pub store: Arc<Mutex<Option<Arc<PartitionStore>>>>,
    pub quiesces: AtomicU64,
    pub reopens: Arc<AtomicU64>,
}

impl FakeControl {
    pub fn store(&self) -> Arc<PartitionStore> {
        Arc::clone(self.store.lock().unwrap().as_ref().expect("store open"))
    }
}

impl PartitionControl for FakeControl {
    fn quiesce_for_install(
        &self,
        p: PartitionId,
    ) -> BoxFuture<'_, Result<InstallGuard, PartitionError>> {
        Box::pin(async move {
            assert_eq!(p, P);
            self.quiesces.fetch_add(1, Ordering::SeqCst);
            let s = self.store.lock().unwrap().take();
            if let Some(s) = s {
                // The store must really close: nobody else may hold it.
                let s = Arc::try_unwrap(s)
                    .map_err(|_| PartitionError::Failed("state store still in use".into()))?;
                drop(s);
            }
            let slot = Arc::clone(&self.store);
            let log = Arc::clone(&self.log);
            let dir = self.data_dir.clone();
            let reopens = Arc::clone(&self.reopens);
            Ok(InstallGuard::new(
                self.data_dir.clone(),
                Arc::clone(&self.log),
                StoreConfig::default(),
                move || {
                    Box::pin(async move {
                        let (store, meta) = tokio::task::spawn_blocking(move || {
                            PartitionStore::open(&dir, P, count(), &StoreConfig::default())
                        })
                        .await
                        .map_err(|e| PartitionError::Failed(e.to_string()))??;
                        if meta.applied > log.log_end() || meta.applied < log.log_start() {
                            return Err(PartitionError::Failed(format!(
                                "state {} outside the log [{}, {})",
                                meta.applied,
                                log.log_start(),
                                log.log_end()
                            )));
                        }
                        // The obligation of every InstallGuard reopen (12-02/12-01).
                        store.start_run_index_rebuild();
                        *slot.lock().unwrap() = Some(Arc::new(store));
                        reopens.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                },
            ))
        })
    }
}

/// The follower (node 2) on its own data dir.
pub struct FollowerFx {
    pub dir: TempDir,
    pub log_node: Option<LogNode>,
    pub log: Arc<PartitionLog>,
    pub control: Arc<FakeControl>,
}

impl FollowerFx {
    /// An empty follower.
    pub async fn empty() -> FollowerFx {
        Self::reopen(TempDir::new().unwrap()).await
    }

    /// A follower whose state holds the first `a` state commands (a prefix of the leader's
    /// state) and whose log holds `log_records` records at epoch 1 (offsets `0..`).
    pub async fn with_old_state(a: u64, log_records: u64, payload: usize) -> FollowerFx {
        let dir = TempDir::new().unwrap();
        {
            let cfg = LogNodeConfig::new(dir.path().join("wal"), count(), FOLLOWER);
            let (node, mut logs) = LogNode::open(cfg, &[Offset(0)]).await.unwrap();
            let log = logs.swap_remove(0).unwrap();
            log.become_leader(Epoch(1), ReplicaSet::solo(FOLLOWER))
                .unwrap();
            if log_records > 0 {
                let cmds = (0..log_records).map(|i| state_cmd(i, payload)).collect();
                let r = log.append(Epoch(1), cmds).await.unwrap();
                log.wait_committed(r.last).await.unwrap();
            }
            node.shutdown().await;
            let (store, _) =
                PartitionStore::open(dir.path(), P, count(), &StoreConfig::default()).unwrap();
            apply_state(&store, 0, a, payload);
        }
        Self::reopen(dir).await
    }

    /// Open the follower on `dir` (from disk), following node 1 at epoch 2.
    pub async fn reopen(dir: TempDir) -> FollowerFx {
        let (store, log_node, log) = open_partition(dir.path(), FOLLOWER).await;
        log.become_follower(Epoch(EPOCH), LEADER).unwrap();
        let control = Arc::new(FakeControl {
            data_dir: dir.path().to_path_buf(),
            log: Arc::clone(&log),
            store: Arc::new(Mutex::new(Some(store))),
            quiesces: AtomicU64::new(0),
            reopens: Arc::new(AtomicU64::new(0)),
        });
        FollowerFx {
            dir,
            log_node: Some(log_node),
            log,
            control,
        }
    }

    pub fn data_dir(&self) -> &Path {
        self.dir.path()
    }

    /// Close everything (the dir stays); returns the dir for a reopen.
    pub async fn close(mut self) -> TempDir {
        self.control.store.lock().unwrap().take();
        if let Some(n) = self.log_node.take() {
            n.shutdown().await;
        }
        self.dir
    }

    /// Files named `.incoming-*` in the follower's snapshot dir.
    pub fn incoming_files(&self) -> Vec<String> {
        let dir = snapshot_dir(self.data_dir(), P);
        std::fs::read_dir(dir)
            .map(|rd| {
                rd.filter_map(|e| e.ok())
                    .map(|e| e.file_name().to_string_lossy().into_owned())
                    .filter(|n| n.starts_with(".incoming-"))
                    .collect()
            })
            .unwrap_or_default()
    }
}

// --------------------------------------------------------------------------- service

/// Rewrites one frame's bytes.
pub type Mangle = fn(Vec<u8>) -> Vec<u8>;

/// Changes applied to the frames of the next `FetchSnapshot` streams.
#[derive(Default)]
pub struct StreamFaults {
    /// Per stream (front first): deliver this many frames, then fail with UNAVAILABLE.
    pub cuts: VecDeque<u64>,
    /// Every stream: replace frame `index`'s bytes with `f(bytes)`.
    pub mangle: Option<(u64, Mangle)>,
}

/// The test `Replication` service (what 12-14's does: require_node, then delegate).
#[derive(Clone)]
pub struct TestReplication {
    pub fetch: Option<Arc<FetchCtx>>,
    pub snapshot: Arc<SnapshotCtx>,
    pub faults: Arc<Mutex<StreamFaults>>,
}

#[tonic::async_trait]
impl Replication for TestReplication {
    async fn fetch(&self, req: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status> {
        let ctx = self
            .fetch
            .clone()
            .ok_or_else(|| Status::unimplemented("no fetch ctx"))?;
        fetch::serve_fetch(&ctx, req).await
    }

    type FetchSnapshotStream = BoxStream<SnapshotFrame>;

    async fn fetch_snapshot(
        &self,
        req: Request<FetchSnapshotRequest>,
    ) -> Result<Response<Self::FetchSnapshotStream>, Status> {
        let stream = serve_snapshot(&self.snapshot, req).await?.into_inner();
        let (cut, mangle) = {
            let mut f = self.faults.lock().unwrap();
            (f.cuts.pop_front(), f.mangle)
        };
        let mut sent = 0u64;
        let s = stream
            .map(move |item| {
                item.map(|mut fr| {
                    if let Some((i, f)) = mangle
                        && fr.index == i
                    {
                        fr.bytes = f(std::mem::take(&mut fr.bytes));
                    }
                    fr
                })
            })
            .take_while(move |_| {
                let go = cut.is_none_or(|c| sent < c);
                sent += 1;
                std::future::ready(go)
            })
            .chain(futures_util::stream::iter(
                cut.map(|_| Err(Status::unavailable("test cut"))),
            ));
        Ok(Response::new(Box::pin(s)))
    }

    type FetchBlobStream = BoxStream<BlobChunk>;

    async fn fetch_blob(
        &self,
        _req: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        Err(Status::unimplemented("not in the 12-12 suites"))
    }

    async fn push_blob(
        &self,
        _req: Request<Streaming<BlobChunk>>,
    ) -> Result<Response<PushBlobAck>, Status> {
        Err(Status::unimplemented("not in the 12-12 suites"))
    }
}

/// The gRPC limit of the test listener: Fetch and FetchSnapshot on one service.
pub fn server_limit() -> usize {
    fetch_decode_limit(
        ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES,
        FetcherConfig::default().max_bytes,
    )
    .max(SNAPSHOT_MESSAGE_LIMIT)
}

/// Node 1's cluster listener serving [`TestReplication`] over mTLS.
pub struct ReplServer {
    pub addr: SocketAddr,
    pub svc: TestReplication,
    shutdown: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl ReplServer {
    pub async fn start(ca: &TestCa, svc: TestReplication) -> ReplServer {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        let book = Arc::new(AddressBook::from_peers([PeerEntry {
            id: LEADER,
            addr: addr.to_string(),
            repl_addr: None,
        }]));
        let transport = TcpTransport::new(&ca.cluster_id, LEADER, ca.node(LEADER), book).unwrap();
        let shutdown = CancellationToken::new();
        let limit = server_limit();
        let router = Server::builder()
            .tls_config(transport.server_tls())
            .unwrap()
            .add_service(InterceptedService::new(
                ReplicationServer::new(svc.clone())
                    .max_decoding_message_size(limit)
                    .max_encoding_message_size(limit),
                transport.identity_interceptor(),
            ));
        let sd = shutdown.clone();
        let join = tokio::spawn(async move {
            let incoming = TcpIncoming::from(l).with_nodelay(Some(true));
            let _ = router
                .serve_with_incoming_shutdown(incoming, sd.cancelled_owned())
                .await;
        });
        ReplServer {
            addr,
            svc,
            shutdown,
            join,
        }
    }

    pub async fn stop(self) {
        self.shutdown.cancel();
        let mut j = self.join;
        if tokio::time::timeout(Duration::from_secs(5), &mut j)
            .await
            .is_err()
        {
            j.abort();
        }
    }
}

/// A client transport of node `me` reaching node 1 at `addr`.
pub fn node_transport(ca: &TestCa, me: u64, addr: SocketAddr) -> Arc<TcpTransport> {
    let book = Arc::new(AddressBook::from_peers([PeerEntry {
        id: LEADER,
        addr: addr.to_string(),
        repl_addr: None,
    }]));
    Arc::new(TcpTransport::new(&ca.cluster_id, me, ca.node(me), book).unwrap())
}

/// A `FetchSnapshot` client of node `me`.
pub fn snapshot_client(
    ca: &TestCa,
    me: u64,
    addr: SocketAddr,
) -> ReplicationClient<tonic::transport::Channel> {
    use ostro_cluster::transport::ClusterTransport as _;
    let ch = node_transport(ca, me, addr).channel(LEADER).unwrap();
    ReplicationClient::new(ch)
        .max_decoding_message_size(SNAPSHOT_MESSAGE_LIMIT)
        .max_encoding_message_size(SNAPSHOT_MESSAGE_LIMIT)
}

/// Every frame of one `FetchSnapshot` call (panics on a stream error).
pub async fn fetch_frames(
    client: &mut ReplicationClient<tonic::transport::Channel>,
    replica: u64,
    resume_frame: u64,
) -> Result<Vec<SnapshotFrame>, Status> {
    let mut s = client
        .fetch_snapshot(FetchSnapshotRequest {
            partition: u32::from(P.0),
            replica_id: replica,
            resume_frame,
        })
        .await?
        .into_inner();
    let mut out = Vec::new();
    while let Some(f) = s.message().await? {
        out.push(f);
    }
    Ok(out)
}

/// Collect a server stream (in-process).
pub async fn collect(mut s: BoxStream<SnapshotFrame>) -> Result<Vec<SnapshotFrame>, Status> {
    let mut out = Vec::new();
    while let Some(f) = s.next().await {
        out.push(f?);
    }
    Ok(out)
}

pub fn concat(frames: &[SnapshotFrame]) -> Vec<u8> {
    frames
        .iter()
        .flat_map(|f| f.bytes.iter().copied())
        .collect()
}

/// Flip a byte inside a stored frame's payload and re-encode it with a valid CRC (only the
/// trailer digest can catch it).
pub fn mangle_payload_keep_crc(b: Vec<u8>) -> Vec<u8> {
    let len = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize;
    let mut payload = b[5..5 + len].to_vec();
    let last = payload.len() - 1;
    payload[last] ^= 0x01;
    RawFrame {
        kind: b[4],
        payload,
    }
    .encode()
}

/// Flip a payload byte without fixing the CRC.
pub fn mangle_payload_break_crc(mut b: Vec<u8>) -> Vec<u8> {
    let i = b.len() - 6;
    b[i] ^= 0x01;
    b
}

/// A small deterministic generator (no rand dependency in this crate's tests).
pub struct Lcg(pub u64);

impl Lcg {
    pub fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}
