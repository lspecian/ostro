//! In-memory cluster transport with seeded fault injection (12-03, D-P12-18 tier B).
//!
//! # Topology
//!
//! One [`MemNet`] per test cluster. [`MemNet::transport`] gives node `n` a [`MemTransport`]
//! (the `ClusterTransport` the server hook takes); its `incoming()` is the receiving end of
//! node `n`'s in-memory listener. [`MemNet::channel_as`]`(from, to)` is a real tonic
//! [`Channel`] built with `Endpoint::connect_with_connector_lazy` over `tokio::io::duplex`
//! pipes (hyper-util `TokioIo`): HTTP/2 and prost run end to end, nothing is mocked.
//!
//! Every connection runs through a per-connection fault proxy inside the test process:
//!
//! ```text
//! client Channel ──duplex A──▶ FaultProxy (tonic server, reads :path) ──duplex B──▶ node `to`
//!                                                     MemConn { io: B server half, peer: PeerNode(from) }
//! ```
//!
//! The proxy is a `tonic::transport::Server` serving one generic tower service on pipe A;
//! it classifies each request by its HTTP/2 `:path` into an [`RpcKind`], asks the shared
//! [`FaultLayer`] what to do, and forwards to an inner channel over pipe B. The target gets
//! `MemConn { peer: PeerNode(from) }`, so its listener can insert the caller identity into
//! request extensions exactly like the mTLS transport does from the peer certificate.
//!
//! # Faults
//!
//! | fault | effect |
//! |-------|--------|
//! | `Drop(p)` | with probability `p` (seeded `StdRng`), the RPC fails at once with `UNAVAILABLE` "dropped by fault layer" |
//! | `Delay(ms)` | the request is held `ms` before it is forwarded |
//! | `Blackhole` | the request is accepted and never answered (the client deadline fires) |
//! | `Cut` | for `RpcKind::Any`: new connections are refused (`ConnectionRefused`) and every live pipe of the link fails with `ConnectionReset` (streams reset); for a specific kind: those RPCs fail with `UNAVAILABLE` "connection reset" |
//!
//! Rules are per directed link `(from, to)` (either side may be "any node", used by
//! `isolate`) and per [`RpcKind`]. The most recently added matching rule decides. The
//! random draws come from one seeded RNG per `MemNet`, so the same seed and the same
//! sequence of RPCs give the same drop pattern.

use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::future::BoxFuture;
use hyper_util::rt::TokioIo;
use ostro_cluster::transport::{
    ClusterIncoming, ClusterTransport, MemConn, PeerNode, TransportError,
};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};
use tokio::sync::{mpsc, watch};
use tonic::body::Body;
use tonic::codegen::http;
use tonic::transport::server::Connected;
use tonic::transport::{Channel, Endpoint, Server, Uri};

/// Pipe buffer of each in-memory connection direction.
const PIPE_BYTES: usize = 256 * 1024;
/// Pending (not yet accepted) connections per node listener.
const BACKLOG: usize = 256;

/// The RPC a request belongs to (from the HTTP/2 `:path`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RpcKind {
    /// `ostro.cluster.v1.Replication/Fetch`.
    Fetch,
    /// `ostro.cluster.v1.Replication/FetchSnapshot`.
    FetchSnapshot,
    /// `ostro.cluster.v1.Replication/FetchBlob`.
    FetchBlob,
    /// `ostro.cluster.v1.Replication/PushBlob`.
    PushBlob,
    /// `ostro.cluster.v1.MetaPeer/Raft`.
    Raft,
    /// `ostro.cluster.v1.MetaPeer/Propose`.
    Propose,
    /// `ostro.cluster.v1.MetaPeer/ReportSuspect`.
    ReportSuspect,
    /// Any other path (only matched by [`RpcKind::Any`] rules).
    Other,
    /// Rule wildcard: every RPC of the link.
    Any,
}

impl RpcKind {
    /// Classify a gRPC request path.
    pub fn from_path(path: &str) -> RpcKind {
        match path {
            "/ostro.cluster.v1.Replication/Fetch" => RpcKind::Fetch,
            "/ostro.cluster.v1.Replication/FetchSnapshot" => RpcKind::FetchSnapshot,
            "/ostro.cluster.v1.Replication/FetchBlob" => RpcKind::FetchBlob,
            "/ostro.cluster.v1.Replication/PushBlob" => RpcKind::PushBlob,
            "/ostro.cluster.v1.MetaPeer/Raft" => RpcKind::Raft,
            "/ostro.cluster.v1.MetaPeer/Propose" => RpcKind::Propose,
            "/ostro.cluster.v1.MetaPeer/ReportSuspect" => RpcKind::ReportSuspect,
            _ => RpcKind::Other,
        }
    }

    fn matches(self, actual: RpcKind) -> bool {
        self == RpcKind::Any || self == actual
    }
}

/// A fault injected on a link.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Fault {
    /// Fail the RPC with probability `p` (`0.0..=1.0`).
    Drop(f64),
    /// Hold the request this many milliseconds before forwarding it.
    Delay(u64),
    /// Accept and never answer.
    Blackhole,
    /// Refuse connections and reset live ones (kind `Any`), or fail the kind's RPCs.
    Cut,
}

#[derive(Clone, Copy, Debug)]
struct Rule {
    /// `None` = any node.
    from: Option<u64>,
    to: Option<u64>,
    kind: RpcKind,
    fault: Fault,
}

impl Rule {
    fn on_link(&self, from: u64, to: u64) -> bool {
        self.from.is_none_or(|f| f == from) && self.to.is_none_or(|t| t == to)
    }
}

/// What the proxy does with one request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Forward unchanged.
    Pass,
    /// Fail with `UNAVAILABLE` and this message.
    Fail(String),
    /// Forward after this delay.
    Delay(Duration),
    /// Never answer.
    Blackhole,
}

/// The shared, seeded fault rules of one [`MemNet`].
pub struct FaultLayer {
    rules: Mutex<Vec<Rule>>,
    rng: Mutex<StdRng>,
    /// Bumped on every rule change; live pipes re-check whether their link is cut.
    changed: watch::Sender<u64>,
}

impl std::fmt::Debug for FaultLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultLayer")
            .field(
                "rules",
                &*self.rules.lock().unwrap_or_else(PoisonError::into_inner),
            )
            .finish_non_exhaustive()
    }
}

impl FaultLayer {
    /// No rules; draws seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Self {
            rules: Mutex::new(Vec::new()),
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            changed: watch::Sender::new(0),
        }
    }

    fn edit(&self, f: impl FnOnce(&mut Vec<Rule>)) {
        f(&mut self.rules.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.send_modify(|g| *g += 1);
    }

    /// Add `fault` for `kind` on the directed link `from -> to`.
    pub fn set(&self, from: u64, to: u64, kind: RpcKind, fault: Fault) {
        self.edit(|r| {
            r.push(Rule {
                from: Some(from),
                to: Some(to),
                kind,
                fault,
            })
        });
    }

    /// Remove every rule of the directed link `from -> to` (wildcard rules stay).
    pub fn clear_link(&self, from: u64, to: u64) {
        self.edit(|r| r.retain(|x| !(x.from == Some(from) && x.to == Some(to))));
    }

    /// Cut every link to and from `n`.
    pub fn isolate(&self, n: u64) {
        self.edit(|r| {
            for (from, to) in [(Some(n), None), (None, Some(n))] {
                r.push(Rule {
                    from,
                    to,
                    kind: RpcKind::Any,
                    fault: Fault::Cut,
                });
            }
        });
    }

    /// Cut both directions between `a` and `b`.
    pub fn partition(&self, a: u64, b: u64) {
        self.edit(|r| {
            for (from, to) in [(a, b), (b, a)] {
                r.push(Rule {
                    from: Some(from),
                    to: Some(to),
                    kind: RpcKind::Any,
                    fault: Fault::Cut,
                });
            }
        });
    }

    /// Remove every rule naming `n` (as source or target, including `isolate` rules).
    pub fn heal(&self, n: u64) {
        self.edit(|r| r.retain(|x| x.from != Some(n) && x.to != Some(n)));
    }

    /// Remove every rule.
    pub fn heal_all(&self) {
        self.edit(Vec::clear);
    }

    /// Whether the link `from -> to` is cut at the connection level.
    pub fn link_cut(&self, from: u64, to: u64) -> bool {
        self.rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .any(|r| r.on_link(from, to) && r.kind == RpcKind::Any && r.fault == Fault::Cut)
    }

    /// Decide one request of `kind` on `from -> to` (draws from the seeded RNG for `Drop`).
    pub fn decide(&self, from: u64, to: u64, kind: RpcKind) -> Action {
        let rule = self
            .rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .rev()
            .find(|r| r.on_link(from, to) && r.kind.matches(kind))
            .copied();
        match rule.map(|r| r.fault) {
            None => Action::Pass,
            Some(Fault::Cut) => Action::Fail(format!(
                "connection reset by fault layer (link {from}->{to} cut)"
            )),
            Some(Fault::Drop(p)) => {
                let x: f64 = self
                    .rng
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .random();
                if x < p {
                    Action::Fail(format!("dropped by fault layer ({kind:?} on {from}->{to})"))
                } else {
                    Action::Pass
                }
            }
            Some(Fault::Delay(ms)) => Action::Delay(Duration::from_millis(ms)),
            Some(Fault::Blackhole) => Action::Blackhole,
        }
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

// ------------------------------------------------------------------------------------
// Pipes
// ------------------------------------------------------------------------------------

/// One end of an in-memory pipe that fails with `ConnectionReset` as soon as its link is
/// cut (woken by the fault layer's change signal, so idle streams reset too).
pub struct CutIo {
    io: DuplexStream,
    faults: Arc<FaultLayer>,
    from: u64,
    to: u64,
    /// Identity presented to a server reading this end.
    peer: PeerNode,
    wake: Pin<Box<dyn Future<Output = ()> + Send>>,
    reset: bool,
}

impl std::fmt::Debug for CutIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CutIo")
            .field("from", &self.from)
            .field("to", &self.to)
            .finish_non_exhaustive()
    }
}

fn waiter(mut rx: watch::Receiver<u64>) -> Pin<Box<dyn Future<Output = ()> + Send>> {
    Box::pin(async move {
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    })
}

impl CutIo {
    fn new(io: DuplexStream, faults: Arc<FaultLayer>, from: u64, to: u64, peer: PeerNode) -> Self {
        // Subscribe before any cut check, so no change can slip between the two.
        let wake = waiter(faults.subscribe());
        Self {
            io,
            faults,
            from,
            to,
            peer,
            wake,
            reset: false,
        }
    }

    /// Whether the link is cut (registers the waker on the change signal otherwise).
    fn poll_cut(&mut self, cx: &mut Context<'_>) -> bool {
        if self.reset {
            return true;
        }
        loop {
            if self.faults.link_cut(self.from, self.to) {
                self.reset = true;
                return true;
            }
            match self.wake.as_mut().poll(cx) {
                // Re-subscribe first, then re-check the link at the top of the loop.
                Poll::Ready(()) => self.wake = waiter(self.faults.subscribe()),
                Poll::Pending => return false,
            }
        }
    }

    fn reset_err(&self) -> io::Error {
        io::Error::new(
            io::ErrorKind::ConnectionReset,
            format!("link {}->{} cut by fault layer", self.from, self.to),
        )
    }
}

impl AsyncRead for CutIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.poll_cut(cx) {
            return Poll::Ready(Err(this.reset_err()));
        }
        Pin::new(&mut this.io).poll_read(cx, buf)
    }
}

impl AsyncWrite for CutIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.poll_cut(cx) {
            return Poll::Ready(Err(this.reset_err()));
        }
        Pin::new(&mut this.io).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.poll_cut(cx) {
            return Poll::Ready(Err(this.reset_err()));
        }
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

impl Connected for CutIo {
    type ConnectInfo = PeerNode;

    fn connect_info(&self) -> PeerNode {
        self.peer
    }
}

/// The server half of an accepted [`MemConn`], presenting the caller as `PeerNode` in every
/// request's extensions (what a node's in-memory listener serves; see [`serve_memory`]).
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

/// Serve `routes` on an in-memory listener (`ClusterIncoming::Memory`) until the sender
/// side is gone. Each request carries the caller's [`PeerNode`] in its extensions.
pub async fn serve_memory(
    incoming: mpsc::Receiver<MemConn>,
    routes: tonic::service::Routes,
) -> Result<(), tonic::transport::Error> {
    let stream = futures_util::stream::unfold(incoming, |mut rx| async move {
        rx.recv()
            .await
            .map(|c| (Ok::<_, io::Error>(PeerIo::from(c)), rx))
    });
    Server::builder().serve_with_incoming(routes, stream).await
}

// ------------------------------------------------------------------------------------
// Network
// ------------------------------------------------------------------------------------

/// The in-memory network of one test cluster: node listeners plus the shared
/// [`FaultLayer`].
pub struct MemNet {
    faults: Arc<FaultLayer>,
    listeners: Mutex<HashMap<u64, mpsc::Sender<MemConn>>>,
}

impl std::fmt::Debug for MemNet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemNet")
            .field("faults", &self.faults)
            .finish_non_exhaustive()
    }
}

impl MemNet {
    /// An empty network whose fault draws are seeded with `seed`.
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            faults: Arc::new(FaultLayer::new(seed)),
            listeners: Mutex::new(HashMap::new()),
        })
    }

    /// The shared fault rules.
    pub fn faults(&self) -> &Arc<FaultLayer> {
        &self.faults
    }

    /// A transport for `node` with a fresh listener (replacing any earlier one: a restarted
    /// node gets a new transport).
    pub fn transport(self: &Arc<Self>, node: u64) -> Arc<MemTransport> {
        let (tx, rx) = mpsc::channel(BACKLOG);
        self.listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(node, tx);
        Arc::new(MemTransport {
            node,
            net: Arc::clone(self),
            incoming: Mutex::new(Some(rx)),
        })
    }

    /// Whether `node` has a listener.
    pub fn knows(&self, node: u64) -> bool {
        self.listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains_key(&node)
    }

    /// A lazily connecting channel from `from` to `to`'s listener, through the fault
    /// proxy; `to` sees the caller as `PeerNode(from)`.
    pub fn channel_as(self: &Arc<Self>, from: u64, to: u64) -> Channel {
        Endpoint::from_static("http://mem-proxy.invalid").connect_with_connector_lazy(
            ProxyConnector {
                net: Arc::clone(self),
                from,
                to,
            },
        )
    }

    fn listener(&self, node: u64) -> Option<mpsc::Sender<MemConn>> {
        self.listeners
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&node)
            .cloned()
    }
}

/// Node `node`'s view of the [`MemNet`]: the `ClusterTransport` the server hook takes.
pub struct MemTransport {
    node: u64,
    net: Arc<MemNet>,
    incoming: Mutex<Option<mpsc::Receiver<MemConn>>>,
}

impl std::fmt::Debug for MemTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTransport")
            .field("node", &self.node)
            .finish_non_exhaustive()
    }
}

impl MemTransport {
    /// The node this transport belongs to.
    pub fn node(&self) -> u64 {
        self.node
    }

    /// Take the listener's receiving end (what `incoming()` returns), once.
    pub fn take_incoming(&self) -> Option<mpsc::Receiver<MemConn>> {
        self.incoming
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }
}

impl ClusterTransport for MemTransport {
    fn channel(&self, node: u64) -> Result<Channel, TransportError> {
        if !self.net.knows(node) {
            return Err(TransportError::UnknownNode(node));
        }
        Ok(self.net.channel_as(self.node, node))
    }

    fn incoming(&self) -> ClusterIncoming {
        match self.take_incoming() {
            Some(rx) => ClusterIncoming::Memory(rx),
            // A second call gets a closed listener (the contract says "called once").
            None => ClusterIncoming::Memory(mpsc::channel(1).1),
        }
    }
}

// ------------------------------------------------------------------------------------
// Connectors and the proxy
// ------------------------------------------------------------------------------------

fn refused(from: u64, to: u64, why: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::ConnectionRefused,
        format!("connection refused {from}->{to}: {why}"),
    )
}

/// Client side: opens pipe A and spawns the fault proxy on its far end.
#[derive(Clone)]
struct ProxyConnector {
    net: Arc<MemNet>,
    from: u64,
    to: u64,
}

impl tower::Service<Uri> for ProxyConnector {
    type Response = TokioIo<CutIo>;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, io::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let (net, from, to) = (Arc::clone(&self.net), self.from, self.to);
        Box::pin(async move {
            if net.faults.link_cut(from, to) {
                return Err(refused(from, to, "link cut by fault layer"));
            }
            if !net.knows(to) {
                return Err(refused(from, to, "unknown node"));
            }
            let (client, server) = tokio::io::duplex(PIPE_BYTES);
            let faults = Arc::clone(&net.faults);
            let inner = Endpoint::from_static("http://mem-node.invalid")
                .connect_with_connector_lazy(TargetConnector {
                    net: Arc::clone(&net),
                    from,
                    to,
                });
            let proxy = FaultProxy {
                faults: Arc::clone(&faults),
                from,
                to,
                inner,
            };
            let server_end = CutIo::new(server, Arc::clone(&faults), from, to, PeerNode(from));
            tokio::spawn(async move {
                let once =
                    futures_util::stream::once(async move { Ok::<_, io::Error>(server_end) });
                let _ = Server::builder().serve_with_incoming(proxy, once).await;
            });
            Ok(TokioIo::new(CutIo::new(
                client,
                faults,
                from,
                to,
                PeerNode(to),
            )))
        })
    }
}

/// Proxy side: opens pipe B and hands its server half to `to`'s listener.
#[derive(Clone)]
struct TargetConnector {
    net: Arc<MemNet>,
    from: u64,
    to: u64,
}

impl tower::Service<Uri> for TargetConnector {
    type Response = TokioIo<CutIo>;
    type Error = io::Error;
    type Future = BoxFuture<'static, Result<Self::Response, io::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _uri: Uri) -> Self::Future {
        let (net, from, to) = (Arc::clone(&self.net), self.from, self.to);
        Box::pin(async move {
            if net.faults.link_cut(from, to) {
                return Err(refused(from, to, "link cut by fault layer"));
            }
            let tx = net
                .listener(to)
                .ok_or_else(|| refused(from, to, "unknown node"))?;
            let (client, server) = tokio::io::duplex(PIPE_BYTES);
            tx.try_send(MemConn {
                io: server,
                peer: PeerNode(from),
            })
            .map_err(|_| refused(from, to, "node is not listening"))?;
            Ok(TokioIo::new(CutIo::new(
                client,
                Arc::clone(&net.faults),
                from,
                to,
                PeerNode(to),
            )))
        })
    }
}

/// Classifies each request by `:path`, applies the fault layer's decision, forwards.
#[derive(Clone)]
struct FaultProxy {
    faults: Arc<FaultLayer>,
    from: u64,
    to: u64,
    inner: Channel,
}

fn unavailable(msg: String) -> http::Response<Body> {
    tonic::Status::unavailable(msg).into_http()
}

impl tower::Service<http::Request<Body>> for FaultProxy {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let kind = RpcKind::from_path(req.uri().path());
        let action = self.faults.decide(self.from, self.to, kind);
        let mut inner = self.inner.clone();
        Box::pin(async move {
            use tower::ServiceExt as _;
            match action {
                Action::Pass => {}
                Action::Fail(msg) => return Ok(unavailable(msg)),
                Action::Delay(d) => tokio::time::sleep(d).await,
                Action::Blackhole => std::future::pending::<()>().await,
            }
            let svc = match inner.ready().await {
                Ok(s) => s,
                Err(e) => return Ok(unavailable(format!("fault proxy: {e}"))),
            };
            match tower::Service::call(svc, req).await {
                Ok(resp) => Ok(resp),
                Err(e) => Ok(unavailable(format!("fault proxy forward: {e}"))),
            }
        })
    }
}
