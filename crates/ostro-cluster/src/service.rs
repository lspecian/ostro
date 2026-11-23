//! The tonic `Replication` service on the cluster listener (12-14), and the listener
//! plumbing shared by the cluster services.
//!
//! - [`ReplicationService`]: each RPC runs `authz::require_node` (inside the 12-11 / 12-12
//!   handlers or here) and delegates to its module: `fetch::serve_fetch`,
//!   `snapshot::serve_snapshot`, `blob::{fetch_blob, push_blob}`. The per-partition contexts
//!   are bound late ([`ReplicationService::bind`]): the listener serves before the node's
//!   partitions exist (meta first, P13-F1 rule 6), and until then every RPC is
//!   `UNAVAILABLE`.
//! - [`replication_server`]: the tonic server with the one encode/decode limit
//!   `max(fetch_decode_limit(..), SNAPSHOT_MESSAGE_LIMIT)` (snapshot frames reach 64 MiB plus
//!   framing), behind the [`PeerIdentity`] interceptor.
//! - [`serve`]: serve a set of routes on the TCP listener (mTLS) or on the in-memory
//!   incoming of the conformance transport, where each connection presents its caller as
//!   [`PeerNode`] in the request extensions.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::Stream;
use futures_util::future::BoxFuture;
use ostro_blob::BlobStore;
use ostro_proto::cluster::v1::replication_server::{Replication, ReplicationServer};
use ostro_proto::cluster::v1::{
    BlobChunk, FetchBlobRequest, FetchRequest, FetchResponse, FetchSnapshotRequest, PushBlobAck,
    SnapshotFrame,
};
use ostro_types::PartitionId;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::server::{Connected, TcpIncoming};
use tonic::transport::{Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::warn;

use crate::authz::require_node;
use crate::blob;
use crate::fetch::{FetchCtx, fetch_decode_limit, serve_fetch};
use crate::snapshot::{SNAPSHOT_MESSAGE_LIMIT, SnapshotCtx, serve_snapshot};
use crate::transport::tcp::PeerIdentity;
use crate::transport::{MemConn, PeerNode};

/// A boxed server stream of `T`.
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Called before a fresh (`resume_frame == 0`) snapshot stream of a partition, so the node
/// can make sure a local snapshot covering its log start exists.
pub type SnapshotPrepare = Arc<dyn Fn(PartitionId) -> BoxFuture<'static, ()> + Send + Sync>;

/// The node's replication contexts, bound once its partitions are open.
#[derive(Default)]
struct Bound {
    fetch: Vec<Option<Arc<FetchCtx>>>,
    snapshot: Vec<Option<Arc<SnapshotCtx>>>,
    blobs: Option<Arc<BlobStore>>,
    prepare: Option<SnapshotPrepare>,
}

/// The `Replication` service of one node (cheap to clone; clones share the bound state).
#[derive(Clone, Default)]
pub struct ReplicationService {
    bound: Arc<RwLock<Bound>>,
}

impl std::fmt::Debug for ReplicationService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.bound.read().unwrap_or_else(|e| e.into_inner());
        f.debug_struct("ReplicationService")
            .field("partitions", &b.fetch.len())
            .field("blobs", &b.blobs.is_some())
            .finish()
    }
}

fn unavailable() -> Status {
    Status::unavailable("replication is not serving yet (the node is starting)")
}

impl ReplicationService {
    /// An unbound service (every RPC `UNAVAILABLE` until [`ReplicationService::bind`]).
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind the per-partition contexts (indexed by partition) and the node's blob store.
    pub fn bind(
        &self,
        fetch: Vec<Option<Arc<FetchCtx>>>,
        snapshot: Vec<Option<Arc<SnapshotCtx>>>,
        blobs: Arc<BlobStore>,
        prepare: Option<SnapshotPrepare>,
    ) {
        let mut b = self.bound.write().unwrap_or_else(|e| e.into_inner());
        *b = Bound {
            fetch,
            snapshot,
            blobs: Some(blobs),
            prepare,
        };
    }

    /// Drop every bound context (shutdown: releases the partition logs).
    pub fn unbind(&self) {
        *self.bound.write().unwrap_or_else(|e| e.into_inner()) = Bound::default();
    }

    fn fetch_ctx(&self, p: u32) -> Result<Arc<FetchCtx>, Status> {
        let b = self.bound.read().unwrap_or_else(|e| e.into_inner());
        if b.blobs.is_none() {
            return Err(unavailable());
        }
        usize::try_from(p)
            .ok()
            .and_then(|i| b.fetch.get(i).cloned().flatten())
            .ok_or_else(|| Status::not_found(format!("partition {p} is not served here")))
    }

    fn snapshot_ctx(&self, p: u32) -> Result<(Arc<SnapshotCtx>, Option<SnapshotPrepare>), Status> {
        let b = self.bound.read().unwrap_or_else(|e| e.into_inner());
        if b.blobs.is_none() {
            return Err(unavailable());
        }
        let ctx = usize::try_from(p)
            .ok()
            .and_then(|i| b.snapshot.get(i).cloned().flatten())
            .ok_or_else(|| Status::not_found(format!("partition {p} is not served here")))?;
        Ok((ctx, b.prepare.clone()))
    }

    fn blobs(&self) -> Result<Arc<BlobStore>, Status> {
        let b = self.bound.read().unwrap_or_else(|e| e.into_inner());
        b.blobs.clone().ok_or_else(unavailable)
    }
}

#[tonic::async_trait]
impl Replication for ReplicationService {
    async fn fetch(&self, req: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status> {
        require_node(&req)?;
        let ctx = self.fetch_ctx(req.get_ref().partition)?;
        serve_fetch(&ctx, req).await
    }

    type FetchSnapshotStream = BoxStream<SnapshotFrame>;

    async fn fetch_snapshot(
        &self,
        req: Request<FetchSnapshotRequest>,
    ) -> Result<Response<Self::FetchSnapshotStream>, Status> {
        require_node(&req)?;
        let p = req.get_ref().partition;
        let (ctx, prepare) = self.snapshot_ctx(p)?;
        if req.get_ref().resume_frame == 0
            && let Some(prepare) = prepare
            && let Ok(p) = u16::try_from(p)
        {
            prepare(PartitionId(p)).await;
        }
        serve_snapshot(&ctx, req).await
    }

    type FetchBlobStream = blob::BlobStream;

    async fn fetch_blob(
        &self,
        req: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        require_node(&req)?;
        let blobs = self.blobs()?;
        blob::fetch_blob(blobs, req.into_inner())
            .await
            .map(Response::new)
    }

    async fn push_blob(
        &self,
        req: Request<Streaming<BlobChunk>>,
    ) -> Result<Response<PushBlobAck>, Status> {
        require_node(&req)?;
        let blobs = self.blobs()?;
        blob::push_blob(blobs, req.into_inner())
            .await
            .map(Response::new)
    }
}

/// The encode/decode limit of the `Replication` server: one limit covers every method, so
/// it is the larger of the fetch limit and the snapshot frame limit.
pub fn replication_message_limit(max_envelope_bytes: usize, fetch_max_bytes: usize) -> usize {
    fetch_decode_limit(max_envelope_bytes, fetch_max_bytes).max(SNAPSHOT_MESSAGE_LIMIT)
}

/// The tonic `Replication` server behind the identity interceptor.
pub fn replication_server(
    svc: ReplicationService,
    identity: PeerIdentity,
    limit: usize,
) -> InterceptedService<ReplicationServer<ReplicationService>, PeerIdentity> {
    InterceptedService::new(
        ReplicationServer::new(svc)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit),
        identity,
    )
}

/// Send `MetaPeer.ReportSuspect{reporter, suspect, partition}` over `channel` (a follower's
/// accelerated liveness hint to the meta leader, D-P12-9). Errors are returned, not retried.
pub async fn report_suspect(
    channel: tonic::transport::Channel,
    reporter: u64,
    suspect: u64,
    partition: Option<u16>,
    reason: &str,
) -> Result<(), Status> {
    let mut client = ostro_proto::cluster::v1::meta_peer_client::MetaPeerClient::new(channel);
    client
        .report_suspect(ostro_proto::cluster::v1::SuspectReport {
            reporter,
            suspect,
            partition: partition.map(u32::from),
            reason: reason.to_owned(),
        })
        .await
        .map(|_| ())
}

/// Where a cluster listener's connections come from.
pub enum ListenerIo {
    /// A bound TCP listener served with mTLS.
    Tcp(std::net::TcpListener, ServerTlsConfig),
    /// The in-memory incoming of the conformance transport.
    Memory(mpsc::Receiver<MemConn>),
}

impl std::fmt::Debug for ListenerIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ListenerIo::Tcp(l, _) => f.debug_tuple("Tcp").field(&l.local_addr().ok()).finish(),
            ListenerIo::Memory(_) => f.write_str("Memory"),
        }
    }
}

/// Serve `routes` on `io` until `shutdown` (h2 keepalive `keepalive` when non-zero). The
/// returned task ends once the server stopped (5 s after `shutdown` at the latest).
pub fn serve(
    io: ListenerIo,
    routes: Routes,
    keepalive: Option<Duration>,
    shutdown: CancellationToken,
) -> io::Result<JoinHandle<()>> {
    let keepalive = keepalive.filter(|k| !k.is_zero());
    let mut builder = Server::builder()
        .http2_keepalive_interval(keepalive)
        .http2_keepalive_timeout(keepalive.map(|k| k * 2));
    let sd = shutdown.clone();
    let server: BoxFuture<'static, Result<(), tonic::transport::Error>> = match io {
        ListenerIo::Tcp(listener, tls) => {
            listener.set_nonblocking(true)?;
            let listener = tokio::net::TcpListener::from_std(listener)?;
            let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
            let mut builder = builder
                .tls_config(tls)
                .map_err(|e| io::Error::other(format!("cluster listener TLS: {e}")))?;
            let router = builder.add_routes(routes);
            Box::pin(router.serve_with_incoming_shutdown(incoming, sd.cancelled_owned()))
        }
        ListenerIo::Memory(rx) => {
            let stream = futures_util::stream::unfold(rx, |mut rx| async move {
                rx.recv()
                    .await
                    .map(|c| (Ok::<_, io::Error>(PeerIo::from(c)), rx))
            });
            let router = builder.add_routes(routes);
            Box::pin(router.serve_with_incoming_shutdown(stream, sd.cancelled_owned()))
        }
    };
    Ok(tokio::spawn(async move {
        let mut server = tokio::spawn(server);
        tokio::select! {
            r = &mut server => log_end(r),
            _ = shutdown.cancelled() => {
                match tokio::time::timeout(Duration::from_secs(5), &mut server).await {
                    Ok(r) => log_end(r),
                    Err(_) => {
                        warn!("cluster listener did not stop within 5 s; aborting");
                        server.abort();
                    }
                }
            }
        }
    }))
}

fn log_end(r: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>) {
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "cluster listener stopped with an error"),
        Err(e) if e.is_cancelled() => {}
        Err(e) => warn!(error = %e, "cluster listener task failed"),
    }
}

/// The server half of an accepted [`MemConn`]: its tonic connect info is the caller's
/// [`PeerNode`], which tonic puts into every request's extensions.
#[derive(Debug)]
pub struct PeerIo {
    io: DuplexStream,
    peer: PeerNode,
}

impl From<MemConn> for PeerIo {
    fn from(c: MemConn) -> Self {
        Self {
            io: c.io,
            peer: c.peer,
        }
    }
}

impl AsyncRead for PeerIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl AsyncWrite for PeerIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

impl Connected for PeerIo {
    type ConnectInfo = PeerNode;

    fn connect_info(&self) -> PeerNode {
        self.peer
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn every_rpc_is_unavailable_until_bound() {
        let svc = ReplicationService::new();
        let mut req = Request::new(FetchRequest::default());
        req.extensions_mut().insert(PeerNode(2));
        let s = svc.fetch(req).await.unwrap_err();
        assert_eq!(s.code(), tonic::Code::Unavailable);
        let mut req = Request::new(FetchBlobRequest::default());
        req.extensions_mut().insert(PeerNode(2));
        let s = svc.fetch_blob(req).await.err().unwrap();
        assert_eq!(s.code(), tonic::Code::Unavailable);
        // Without an identity the caller is refused before anything else.
        let s = svc
            .fetch_snapshot(Request::new(FetchSnapshotRequest::default()))
            .await
            .err()
            .unwrap();
        assert_eq!(s.code(), tonic::Code::Unauthenticated);
        let limit = replication_message_limit(4 << 20, 8 << 20);
        assert!(limit >= SNAPSHOT_MESSAGE_LIMIT);
        let _server = replication_server(svc, PeerIdentity::new("c"), limit);
    }

    #[test]
    fn require_node_without_identity_is_unauthenticated() {
        // 12-05 implemented require_node (authz.rs); its full matrix is tested there.
        let req = Request::new(());
        assert_eq!(
            crate::authz::require_node(&req).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }
}
