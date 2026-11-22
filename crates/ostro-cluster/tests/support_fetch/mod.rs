//! Test support for the 12-11 fetch suites: partition logs on temp dirs, a test
//! `Replication` service delegating to `fetch` / `blob` (12-14 owns the real one), served
//! over real loopback mTLS, an assignment watch over a `MetaHandle`, and command builders.
//!
//! Needs `mod support_meta;` in the including test (CA and client transports).

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ostro_blob::BlobStore;
use ostro_cluster::address_book::{AddressBook, PeerEntry};
use ostro_cluster::blob;
use ostro_cluster::fetch::{self, FetchCtx};
use ostro_cluster::liveness::SuspectSink;
use ostro_cluster::service::BoxStream;
use ostro_cluster::transport::tcp::TcpTransport;
use ostro_log::{LogNode, LogNodeConfig, PartitionLog, ReplicatedLog};
use ostro_meta::MetaHandle;
use ostro_partition::roles::PartitionAssignmentView;
use ostro_proto::cluster::v1::replication_server::{Replication, ReplicationServer};
use ostro_proto::cluster::v1::{
    BlobChunk, FetchBlobRequest, FetchRequest, FetchResponse, FetchSnapshotRequest, PushBlobAck,
    SnapshotFrame,
};
use ostro_types::command::{Cmd, Command, StartRun};
use ostro_types::payload::{BlobRef, PayloadRef, payload_ref};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};
use prost::Message;
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

/// One partition log (partition 0) of node `node` on its own temp dir.
pub struct LogFx {
    pub node: u64,
    pub dir: TempDir,
    pub log_node: LogNode,
    pub log: Arc<PartitionLog>,
    pub blobs: Arc<BlobStore>,
}

impl LogFx {
    pub async fn open(node: u64) -> LogFx {
        Self::open_with(node, |_| {}).await
    }

    pub async fn open_with(node: u64, tweak: impl FnOnce(&mut LogNodeConfig)) -> LogFx {
        let dir = TempDir::new().unwrap();
        let mut cfg = LogNodeConfig::new(
            dir.path().join("wal"),
            PartitionCount::new(1).unwrap(),
            node,
        );
        tweak(&mut cfg);
        let (log_node, mut logs) = LogNode::open(cfg, &[Offset::ZERO]).await.unwrap();
        let log = logs.swap_remove(0).unwrap();
        let blobs = Arc::new(BlobStore::open(&dir.path().join("data")).unwrap());
        LogFx {
            node,
            dir,
            log_node,
            log,
            blobs,
        }
    }

    pub async fn shutdown(self) {
        self.log_node.shutdown().await;
    }
}

/// Append `cmds` as one batch at `epoch` and wait until it is durable locally.
pub async fn append(log: &PartitionLog, epoch: Epoch, cmds: Vec<Bytes>) -> Offset {
    let r = log.append(epoch, cmds).await.expect("append");
    let end = r.last.next();
    wait_durable(log, end).await;
    end
}

pub async fn wait_durable(log: &PartitionLog, end: Offset) {
    let mut rx = log.durable_end_watch();
    tokio::time::timeout(Duration::from_secs(10), rx.wait_for(|d| *d >= end))
        .await
        .expect("durable in time")
        .expect("durable watch");
}

/// An encoded `StartRun` command with an inline input.
pub fn cmd(i: u64) -> Bytes {
    start_with(
        i,
        PayloadRef {
            kind: Some(payload_ref::Kind::Inline(Bytes::from(format!("input-{i}")))),
        },
    )
}

/// An encoded `StartRun` command whose input is the blob `sha` of `len` bytes.
pub fn blob_cmd(i: u64, sha: [u8; 32], len: u64) -> Bytes {
    start_with(
        i,
        PayloadRef {
            kind: Some(payload_ref::Kind::Blob(BlobRef {
                sha256: sha.to_vec(),
                len,
            })),
        },
    )
}

fn start_with(i: u64, input: PayloadRef) -> Bytes {
    let c = Command {
        cmd: Some(Cmd::StartRun(StartRun {
            run_id: format!("run-{i:08}").into_bytes(),
            input: Some(input),
            ..StartRun::default()
        })),
    };
    Bytes::from(c.encode_to_vec())
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

/// A partition assignment view of partition 0.
pub fn view(leader: u64, epoch: u32, replicas: &[u64], isr: &[u64]) -> PartitionAssignmentView {
    PartitionAssignmentView {
        partition: P,
        leader,
        epoch: Epoch(epoch),
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version: 0,
        min_insync: 1,
    }
}

pub type Assignments = watch::Sender<Arc<Vec<PartitionAssignmentView>>>;

pub fn assignments(v: PartitionAssignmentView) -> Assignments {
    watch::channel(Arc::new(vec![v])).0
}

/// The meta-published assignments of `meta`, refreshed on every apply.
pub fn meta_assignments(
    meta: &MetaHandle,
    shutdown: CancellationToken,
) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>> {
    let v = meta.view().clone();
    let read = move |v: &ostro_meta::sm::MetaView| -> Arc<Vec<PartitionAssignmentView>> {
        Arc::new(v.read(|s| {
            s.partitions()
                .map(|a| {
                    let a = &a.value;
                    PartitionAssignmentView {
                        partition: PartitionId(a.partition),
                        leader: a.leader,
                        epoch: Epoch(a.epoch),
                        replicas: a.replicas.clone(),
                        isr: a.isr.clone(),
                        isr_version: a.isr_version,
                        min_insync: a.min_insync,
                    }
                })
                .collect()
        }))
    };
    let (tx, rx) = watch::channel(read(&v));
    let mut applied = v.subscribe();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                () = shutdown.cancelled() => return,
                r = applied.changed() => if r.is_err() { return },
            }
            let next = read(&v);
            tx.send_if_modified(|cur| {
                if **cur != *next {
                    *cur = next;
                    true
                } else {
                    false
                }
            });
        }
    });
    rx
}

/// Records suspect reports.
#[derive(Default)]
pub struct Suspects(pub Mutex<Vec<(u64, u64, Option<u16>)>>);

impl SuspectSink for Suspects {
    fn report(&self, reporter: u64, suspect: u64, partition: Option<u16>) {
        self.0.lock().unwrap().push((reporter, suspect, partition));
    }
}

/// The test `Replication` service (what 12-14's service does: require_node, then delegate).
#[derive(Clone)]
pub struct TestReplication {
    pub ctxs: Arc<Mutex<BTreeMap<u32, Arc<FetchCtx>>>>,
    pub blobs: Arc<BlobStore>,
}

#[tonic::async_trait]
impl Replication for TestReplication {
    async fn fetch(&self, req: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status> {
        let ctx = self
            .ctxs
            .lock()
            .unwrap()
            .get(&req.get_ref().partition)
            .cloned()
            .ok_or_else(|| Status::not_found("partition"))?;
        fetch::serve_fetch(&ctx, req).await
    }

    type FetchSnapshotStream = BoxStream<SnapshotFrame>;

    async fn fetch_snapshot(
        &self,
        _req: Request<FetchSnapshotRequest>,
    ) -> Result<Response<Self::FetchSnapshotStream>, Status> {
        Err(Status::unimplemented("12-12"))
    }

    type FetchBlobStream = blob::BlobStream;

    async fn fetch_blob(
        &self,
        req: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        ostro_cluster::authz::require_node(&req)?;
        blob::fetch_blob(Arc::clone(&self.blobs), req.into_inner())
            .await
            .map(Response::new)
    }

    async fn push_blob(
        &self,
        req: Request<Streaming<BlobChunk>>,
    ) -> Result<Response<PushBlobAck>, Status> {
        ostro_cluster::authz::require_node(&req)?;
        blob::push_blob(Arc::clone(&self.blobs), req.into_inner())
            .await
            .map(Response::new)
    }
}

/// A node's cluster listener serving [`TestReplication`] over mTLS.
pub struct ReplServer {
    pub id: u64,
    pub addr: SocketAddr,
    pub svc: TestReplication,
    shutdown: CancellationToken,
    join: tokio::task::JoinHandle<()>,
}

impl ReplServer {
    pub async fn start(
        ca: &TestCa,
        id: u64,
        ctx: Option<Arc<FetchCtx>>,
        blobs: Arc<BlobStore>,
        limit: usize,
    ) -> ReplServer {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let svc = TestReplication {
            ctxs: Arc::new(Mutex::new(
                ctx.into_iter()
                    .map(|c| (u32::from(c.partition().0), c))
                    .collect(),
            )),
            blobs,
        };
        Self::serve(ca, id, l, svc, limit).await
    }

    pub async fn serve(
        ca: &TestCa,
        id: u64,
        l: TcpListener,
        svc: TestReplication,
        limit: usize,
    ) -> ReplServer {
        let addr = l.local_addr().unwrap();
        let book = Arc::new(AddressBook::from_peers([PeerEntry {
            id,
            addr: addr.to_string(),
            repl_addr: None,
        }]));
        let transport = TcpTransport::new(&ca.cluster_id, id, ca.node(id), book).unwrap();
        let shutdown = CancellationToken::new();
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
            id,
            addr,
            svc,
            shutdown,
            join,
        }
    }

    /// Stop serving (connections are dropped).
    pub async fn stop(self) -> (SocketAddr, TestReplication) {
        self.shutdown.cancel();
        let mut j = self.join;
        if tokio::time::timeout(Duration::from_secs(5), &mut j)
            .await
            .is_err()
        {
            j.abort();
        }
        (self.addr, self.svc)
    }

    /// Serve again on the same address.
    pub async fn restart(
        ca: &TestCa,
        id: u64,
        addr: SocketAddr,
        svc: TestReplication,
        limit: usize,
    ) -> ReplServer {
        let deadline = Instant::now() + Duration::from_secs(10);
        let l = loop {
            match TcpListener::bind(addr).await {
                Ok(l) => break l,
                Err(e) => {
                    assert!(Instant::now() < deadline, "rebind {addr}: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };
        Self::serve(ca, id, l, svc, limit).await
    }
}

/// A client transport of node `me` (its own node certificate) reaching node `target`.
pub fn node_transport(ca: &TestCa, me: u64, target: u64, addr: SocketAddr) -> Arc<TcpTransport> {
    let book = Arc::new(AddressBook::from_peers([PeerEntry {
        id: target,
        addr: addr.to_string(),
        repl_addr: None,
    }]));
    Arc::new(TcpTransport::new(&ca.cluster_id, me, ca.node(me), book).unwrap())
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
