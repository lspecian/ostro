//! The cluster listener and the `ostro.worker.v1` Execute service (plan 07-09, D-W2,
//! D-W4, D-W16, D-W25, SEC-01).
//!
//! - **Listener.** `[cluster] listen`, tonic over TLS with the node certificate and a
//!   client certificate always required (`client_ca_root` = the cluster CA, plus
//!   `[cluster.tls] client_ca` when configured), h2 keepalive `[workers] keepalive`,
//!   encode/decode limit 4 MiB. The bound address (or `[cluster] advertise`) is handed to
//!   the join service (join `endpoints`) and sent in `Welcome.members`.
//! - **Identity.** Every RPC derives a [`PeerIdentity`] from `peer_certs()`: a certificate
//!   issued by the cluster CA must carry `ostro://<cluster>/worker/<token_id>/<worker_id>`
//!   whose join token still exists; a certificate from the external CA must carry
//!   `ostro://<cluster>/worker/ext/<worker_id>` and clientAuth (it skips the token check;
//!   one session per external worker id). Node certificates and other clusters' are
//!   refused.
//! - **Session.** First message `Hello` (worker id = the certificate's), version checks
//!   (`proto_version` and `event_codec_ver` always; `ostro_version` and
//!   `wasmtime_version` when `[workers] require_same_version`) answered with
//!   `GoAway{reason}` (the reason names the version) on mismatch, then
//!   `Welcome{session_id, node_id, cluster_id, lease_ttl_ms,
//!   heartbeat_ms = min(heartbeat, lease_ttl / 3), max_frame_bytes, limits, members}`.
//!   `Credit`, `Heartbeat`, `Drain` go to the [`WorkerRegistry`]; `Done`, `GuestLogs`,
//!   `CancelEffect` to the attached job. A registry close (token revocation) or server
//!   shutdown sends `GoAway` and ends the stream; the end of a stream abandons its jobs.
//! - **Bulk RPCs.** `FetchJournal`, `FetchBlob`, `Append`, `StartEffect` require that the
//!   caller's identity owns `session_id` and that the session leases `(run, token)` (W6):
//!   `PERMISSION_DENIED` otherwise. Journal pages are at most `max_frame_bytes` (inline
//!   rows; blob rows as `BlobRef`); a blob is served only when it is the run's module or a
//!   blob ref of the run's journal; 1 MiB chunks. `Append` frames are reassembled (at most
//!   `[workers] max_append_bytes`) and handed with `StartEffect` to the job's handler
//!   (07-12); without one they answer `UNIMPLEMENTED`. A successful append renews the
//!   lease. `Renew` signs a new certificate for a caller holding a session.
//! - **Revocation.** A meta watch closes every session whose join token disappeared.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ostro_api::{ApiError, ErrorCode};
use ostro_blob::BlobRef as StoreBlobRef;
use ostro_meta::MetaView;
use ostro_partition::{Clock, query};
use ostro_types::payload::payload_ref::Kind;
use ostro_worker_proto::v1::execute_server::{Execute, ExecuteServer};
use ostro_worker_proto::{PROTO_VERSION, v1};
use rustls_pki_types::CertificateDer;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::codegen::tokio_stream::Stream;
use tonic::codegen::tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};
use x509_parser::prelude::{FromDer, X509Certificate};

use crate::config::{ClusterSection, ServerConfig};
use crate::p7::{P7Ctx, P7Error};
use crate::tls::TlsState;
use crate::tls::ca::WorkerIdentity;
use crate::workers::join::JoinService;
use crate::workers::registry::{
    AppendBatch, AuthzError, JobRpc, PeerIdentity, RemoteEvent, WorkerRegistry,
};

/// `Hello.ostro_version` this server accepts (`require_same_version`).
pub const OSTRO_VERSION: &str = env!("CARGO_PKG_VERSION");
/// `Hello.wasmtime_version` this server accepts: the engine's Wasmtime pin (the same
/// constant the worker announces, 07-22).
pub const WASMTIME_VERSION: &str = ostro_engine::cache::WASMTIME_VERSION;
/// tonic encode/decode limit (D-W2).
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;
/// `FetchBlob` chunk size.
pub const BLOB_CHUNK: usize = 1024 * 1024;
/// How long a new session may take to send `Hello`.
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// Per-page / per-chunk framing reserve inside `max_frame_bytes`.
const FRAME_RESERVE: usize = 256;
/// Journal rows read per store scan.
const ROWS_PER_SCAN: usize = 256;
/// Graceful-shutdown bound of the cluster listener.
const SHUTDOWN_BOUND: Duration = Duration::from_secs(5);
/// `token_id` of external identities (`ostro://<cluster>/worker/ext/<id>`, D-W25).
pub const EXTERNAL_TOKEN_ID: &str = "ext";

type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// Everything the cluster listener needs (built from [`P7Ctx`] by [`start`]; tests build
/// it directly).
pub struct ClusterDeps {
    pub config: Arc<ServerConfig>,
    pub tls: Arc<TlsState>,
    pub registry: Arc<WorkerRegistry>,
    pub join: Arc<JoinService>,
    pub meta: MetaView,
    pub clock: Arc<dyn Clock>,
    pub node_id: u64,
    pub lease_ttl: Duration,
    pub shutdown: CancellationToken,
    /// Payload sizes per message (tests; production `None`).
    pub wire: Option<Arc<WireStats>>,
}

/// Direction of a recorded message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireDir {
    /// Worker -> server.
    In,
    /// Server -> worker.
    Out,
}

/// One recorded bulk message: its payload bytes (journal rows, blob chunk, append chunk).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WireRecord {
    pub dir: WireDir,
    /// `fetch_journal` | `fetch_blob` | `append`.
    pub rpc: &'static str,
    pub bytes: usize,
}

/// Per-message payload sizes of the bulk RPCs, both directions (the Phase 7 harness's
/// message-size recorder). ostro-server has no direct prost dependency (07-02 owns the
/// Cargo files), so the recorded size is the payload carried by the message (inline row
/// bytes + a fixed per-row reserve, blob chunk, append chunk), not the encoded length.
#[derive(Debug, Default)]
pub struct WireStats {
    records: std::sync::Mutex<Vec<WireRecord>>,
}

impl WireStats {
    pub fn record(&self, dir: WireDir, rpc: &'static str, bytes: usize) {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(WireRecord { dir, rpc, bytes });
    }

    /// Every record so far.
    pub fn records(&self) -> Vec<WireRecord> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// The largest payload recorded for `rpc` in `dir`.
    pub fn max(&self, dir: WireDir, rpc: &str) -> usize {
        self.records()
            .iter()
            .filter(|r| r.dir == dir && r.rpc == rpc)
            .map(|r| r.bytes)
            .max()
            .unwrap_or(0)
    }
}

/// Start the cluster listener and the Execute service; `None` when `[workers] enabled` is
/// false. Always binds the join service (meta handle + clock) so tokens work either way.
pub fn start(ctx: &P7Ctx) -> Result<Option<JoinHandle<()>>, P7Error> {
    ctx.shared
        .join
        .attach(ctx.meta_handle.clone(), Arc::clone(&ctx.clock));
    if !ctx.config.workers.enabled {
        return Ok(None);
    }
    let listener = std::net::TcpListener::bind(ctx.config.cluster.listen)?;
    let (addr, task) = serve(
        listener,
        ClusterDeps {
            config: Arc::clone(&ctx.config),
            tls: Arc::clone(&ctx.shared.tls),
            registry: Arc::clone(&ctx.shared.registry),
            join: Arc::clone(&ctx.shared.join),
            meta: ctx.meta.clone(),
            clock: Arc::clone(&ctx.clock),
            node_id: ctx.node_id,
            lease_ttl: ctx.lease_ttl,
            shutdown: ctx.shutdown.clone(),
            wire: None,
        },
    )?;
    info!(%addr, "cluster listener (worker protocol, mTLS) bound");
    Ok(Some(task))
}

/// The endpoints advertised to workers: `[cluster] advertise` (the bound port appended
/// where an entry has none) or the bound address (`127.0.0.1` for an unspecified IP).
pub fn advertised_endpoints(cluster: &ClusterSection, bound: SocketAddr) -> Vec<String> {
    if cluster.advertise.is_empty() {
        let ip = if bound.ip().is_unspecified() {
            warn!(
                "[cluster] listen is an unspecified address and [cluster] advertise is empty: \
                 workers are told 127.0.0.1; set [cluster] advertise for remote workers"
            );
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        } else {
            bound.ip()
        };
        return vec![SocketAddr::new(ip, bound.port()).to_string()];
    }
    cluster
        .advertise
        .iter()
        .map(|a| {
            if a.parse::<SocketAddr>().is_ok() {
                a.clone()
            } else if let Ok(ip) = a.parse::<IpAddr>() {
                SocketAddr::new(ip, bound.port()).to_string()
            } else if a
                .rsplit_once(':')
                .is_some_and(|(_, p)| p.parse::<u16>().is_ok())
            {
                a.clone()
            } else {
                format!("{a}:{}", bound.port())
            }
        })
        .collect()
}

/// The `Welcome.limits` of this server (its `[engine]` configuration plus the `[limits]`
/// run limits, Pitfall 5).
pub fn engine_limits(cfg: &ServerConfig) -> v1::EngineLimits {
    let e = cfg.to_engine_config();
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    v1::EngineLimits {
        hot_wait_ms: ms(e.hot_wait),
        activation_timeout_ms: ms(e.activation_timeout),
        activation_fuel: e.activation_fuel,
        continue_as_new_events: e.continue_as_new_events,
        max_memory_bytes: e.pool.max_memory_bytes,
        max_wasm_stack: e.max_wasm_stack as u64,
        max_table_elements: e.max_table_elements as u64,
        async_stack_size: e.async_stack_size as u64,
        epoch_tick_ms: ms(e.epoch_tick),
        max_history_events: e.run_limits.max_history_events,
        max_children_per_run: e.run_limits.max_children_per_run,
        max_payload_bytes: e.run_limits.max_payload_bytes,
    }
}

/// Serve the Execute service on `listener` (must be called inside a tokio runtime).
/// Returns the bound address and the task (it ends after `deps.shutdown`).
pub fn serve(
    listener: std::net::TcpListener,
    deps: ClusterDeps,
) -> Result<(SocketAddr, JoinHandle<()>), P7Error> {
    let addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let listener = tokio::net::TcpListener::from_std(listener)?;

    let node = deps
        .tls
        .node_cert()
        .ok_or_else(|| P7Error::Config("no node certificate (TLS state is off)".into()))?;
    let mut roots = deps
        .tls
        .ca_pem()
        .ok_or_else(|| P7Error::Config("no cluster CA (TLS state is off)".into()))?;
    let cluster_ca_der = deps.tls.ca_der().unwrap_or_default();
    let external_ca = match &deps.config.cluster.tls.client_ca {
        Some(path) => {
            let pem = std::fs::read_to_string(path).map_err(|e| {
                P7Error::Config(format!("[cluster.tls] client_ca {}: {e}", path.display()))
            })?;
            if !pem.contains("-----BEGIN CERTIFICATE-----") {
                return Err(P7Error::Config(format!(
                    "[cluster.tls] client_ca {}: no PEM certificate",
                    path.display()
                )));
            }
            if !roots.ends_with('\n') {
                roots.push('\n');
            }
            roots.push_str(&pem);
            true
        }
        None => false,
    };
    let tls = ServerTlsConfig::new()
        .identity(Identity::from_pem(node.chain_pem(), node.key_pem()))
        .client_ca_root(Certificate::from_pem(roots));

    let endpoints = advertised_endpoints(&deps.config.cluster, addr);
    deps.join.set_endpoints(endpoints.clone());
    let w = &deps.config.workers;
    let ms = |d: Duration| u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
    let st = Arc::new(SvcState {
        registry: Arc::clone(&deps.registry),
        join: Arc::clone(&deps.join),
        clock: Arc::clone(&deps.clock),
        cluster_id: deps.tls.cluster_id().to_owned(),
        node_id: deps.node_id,
        lease_ttl_ms: ms(deps.lease_ttl),
        heartbeat_ms: ms(w.effective_heartbeat(deps.lease_ttl)).max(1),
        max_frame: u32::try_from(w.max_frame_bytes.clamp(4096, MAX_MESSAGE_BYTES as u64))
            .unwrap_or(u32::MAX),
        max_append: w.max_append_bytes,
        limits: engine_limits(&deps.config),
        members: endpoints
            .iter()
            .map(|e| v1::Member {
                node_id: deps.node_id,
                endpoint: e.clone(),
            })
            .collect(),
        require_same_version: w.require_same_version,
        cluster_ca_der,
        external_ca,
        shutdown: deps.shutdown.clone(),
        wire: deps.wire.clone(),
    });
    let svc = ExecuteServer::new(ExecuteSvc {
        st: Arc::clone(&st),
    })
    .max_decoding_message_size(MAX_MESSAGE_BYTES)
    .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let keepalive = w.keepalive.get();
    let router = Server::builder()
        .tls_config(tls)
        .map_err(|e| P7Error::Config(format!("cluster listener TLS: {e}")))?
        .http2_keepalive_interval((!keepalive.is_zero()).then_some(keepalive))
        .http2_keepalive_timeout((!keepalive.is_zero()).then_some(keepalive * 2))
        .add_service(svc);
    let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
    let shutdown = deps.shutdown.clone();

    let watch = tokio::spawn(revocation_watch(
        deps.meta.clone(),
        Arc::clone(&deps.registry),
        Arc::clone(&deps.join),
        deps.shutdown.clone(),
    ));
    let task = tokio::spawn(async move {
        let sd = shutdown.clone();
        let mut server = tokio::spawn(async move {
            router
                .serve_with_incoming_shutdown(incoming, sd.cancelled_owned())
                .await
        });
        tokio::select! {
            r = &mut server => log_end(r),
            _ = shutdown.cancelled() => {
                match tokio::time::timeout(SHUTDOWN_BOUND, &mut server).await {
                    Ok(r) => log_end(r),
                    Err(_) => {
                        warn!("cluster listener did not stop within 5 s; aborting");
                        server.abort();
                    }
                }
            }
        }
        watch.abort();
    });
    Ok((addr, task))
}

fn log_end(r: Result<Result<(), tonic::transport::Error>, tokio::task::JoinError>) {
    match r {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(error = %e, "cluster listener failed"),
        Err(e) => warn!(error = %e, "cluster listener task failed"),
    }
}

/// Close sessions whose join token disappeared (revocation, D-W4), on every meta change.
async fn revocation_watch(
    meta: MetaView,
    registry: Arc<WorkerRegistry>,
    join: Arc<JoinService>,
    shutdown: CancellationToken,
) {
    let mut rx = meta.subscribe();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return,
            r = rx.changed() => if r.is_err() { return },
        }
        let n = registry.close_where("token revoked", |p| {
            p.token_id.as_deref().is_some_and(|t| !join.token_exists(t))
        });
        if n > 0 {
            info!(
                sessions = n,
                "closed worker sessions of a revoked join token"
            );
        }
    }
}

fn session_metric(result: &'static str) {
    metrics::counter!("ostro_worker_sessions_total", "result" => result).increment(1);
}

struct SvcState {
    registry: Arc<WorkerRegistry>,
    join: Arc<JoinService>,
    clock: Arc<dyn Clock>,
    cluster_id: String,
    node_id: u64,
    lease_ttl_ms: u64,
    heartbeat_ms: u64,
    max_frame: u32,
    max_append: u64,
    limits: v1::EngineLimits,
    members: Vec<v1::Member>,
    require_same_version: bool,
    cluster_ca_der: Vec<u8>,
    external_ca: bool,
    shutdown: CancellationToken,
    wire: Option<Arc<WireStats>>,
}

fn issued_by(leaf: &X509Certificate<'_>, ca_der: &[u8]) -> bool {
    match X509Certificate::from_der(ca_der) {
        Ok((_, ca)) => leaf.verify_signature(Some(ca.public_key())).is_ok(),
        Err(_) => false,
    }
}

fn has_client_auth(leaf: &X509Certificate<'_>) -> bool {
    matches!(leaf.extended_key_usage(), Ok(Some(e)) if e.value.client_auth)
}

impl SvcState {
    fn wire(&self, dir: WireDir, rpc: &'static str, bytes: usize) {
        if let Some(w) = &self.wire {
            w.record(dir, rpc, bytes);
        }
    }

    /// The caller's identity (see the module docs).
    fn peer(
        &self,
        certs: Option<Arc<Vec<CertificateDer<'static>>>>,
    ) -> Result<PeerIdentity, Status> {
        let certs = certs.ok_or_else(|| Status::unauthenticated("client certificate required"))?;
        let leaf_der = certs
            .first()
            .ok_or_else(|| Status::unauthenticated("client certificate required"))?;
        let id = WorkerIdentity::from_der(leaf_der)
            .map_err(|e| Status::permission_denied(format!("not a worker certificate: {e}")))?;
        if id.cluster_id != self.cluster_id {
            return Err(Status::permission_denied(
                "worker certificate of another cluster",
            ));
        }
        let (_, leaf) = X509Certificate::from_der(leaf_der)
            .map_err(|_| Status::permission_denied("unparseable client certificate"))?;
        let from_cluster_ca = !self.external_ca || issued_by(&leaf, &self.cluster_ca_der);
        if from_cluster_ca {
            if id.token_id == EXTERNAL_TOKEN_ID {
                return Err(Status::permission_denied(
                    "external worker identities must be issued by [cluster.tls] client_ca",
                ));
            }
            if !self.join.token_exists(&id.token_id) {
                return Err(Status::unauthenticated("join token revoked"));
            }
            Ok(PeerIdentity {
                cluster_id: id.cluster_id,
                token_id: Some(id.token_id),
                worker_id: id.worker_id,
            })
        } else {
            if id.token_id != EXTERNAL_TOKEN_ID {
                return Err(Status::permission_denied(
                    "an external worker certificate must carry ostro://<cluster>/worker/ext/<worker_id>",
                ));
            }
            if !has_client_auth(&leaf) {
                return Err(Status::permission_denied(
                    "an external worker certificate needs the clientAuth EKU",
                ));
            }
            Ok(PeerIdentity {
                cluster_id: id.cluster_id,
                token_id: None,
                worker_id: id.worker_id,
            })
        }
    }

    fn check_versions(&self, h: &v1::Hello) -> Result<(), String> {
        let codec = u32::from(ostro_types::journal::EVENT_CODEC_VER);
        if h.proto_version != PROTO_VERSION {
            return Err(format!(
                "version mismatch: proto_version {} (server {PROTO_VERSION})",
                h.proto_version
            ));
        }
        if h.event_codec_ver != codec {
            return Err(format!(
                "version mismatch: event_codec_ver {} (server {codec})",
                h.event_codec_ver
            ));
        }
        if self.require_same_version {
            if h.ostro_version != OSTRO_VERSION {
                return Err(format!(
                    "version mismatch: ostro_version {} (server {OSTRO_VERSION})",
                    h.ostro_version
                ));
            }
            if h.wasmtime_version != WASMTIME_VERSION {
                return Err(format!(
                    "version mismatch: wasmtime_version {} (server {WASMTIME_VERSION})",
                    h.wasmtime_version
                ));
            }
        }
        Ok(())
    }

    fn authorize(
        &self,
        peer: &PeerIdentity,
        session: &[u8],
        run: Option<&v1::RunRef>,
        token: Option<&v1::Token>,
    ) -> Result<crate::workers::registry::JobAccess, Status> {
        self.registry
            .authorize(session, peer, run, token)
            .map_err(|e| match e {
                AuthzError::NoSession => Status::permission_denied("unknown session"),
                AuthzError::NotOwner => {
                    Status::permission_denied("the session belongs to another worker identity")
                }
                AuthzError::NoLease => {
                    Status::permission_denied("the session holds no lease for this run and token")
                }
            })
    }

    fn send(tx: &crate::workers::registry::SessionTx, msg: v1::server_msg::Msg) {
        let _ = tx.send(Ok(v1::ServerMsg { msg: Some(msg) }));
    }

    async fn run_session(
        self: Arc<Self>,
        peer: PeerIdentity,
        mut inbound: Streaming<v1::WorkerMsg>,
        tx: crate::workers::registry::SessionTx,
    ) {
        let hello = match tokio::time::timeout(HELLO_TIMEOUT, inbound.message()).await {
            Ok(Ok(Some(v1::WorkerMsg {
                msg: Some(v1::worker_msg::Msg::Hello(h)),
            }))) => h,
            _ => {
                session_metric("refused_protocol");
                let _ = tx.send(Err(Status::invalid_argument(
                    "the first session message must be Hello",
                )));
                return;
            }
        };
        if hello.worker_id != peer.worker_id {
            session_metric("refused_identity");
            let _ = tx.send(Err(Status::permission_denied(format!(
                "Hello.worker_id {:?} does not match the certificate's worker id {:?}",
                hello.worker_id, peer.worker_id
            ))));
            return;
        }
        if let Err(reason) = self.check_versions(&hello) {
            session_metric("refused_version");
            warn!(worker_id = %peer.worker_id, %reason, "worker refused");
            Self::send(&tx, v1::server_msg::Msg::GoAway(v1::GoAway { reason }));
            return;
        }
        let worker_id = peer.worker_id.clone();
        let ctl = match self
            .registry
            .open_session(peer, &hello, self.clock.now_ms(), tx.clone())
        {
            Ok(c) => c,
            Err(e) => {
                session_metric("refused_busy");
                let _ = tx.send(Err(Status::already_exists(e.to_string())));
                return;
            }
        };
        session_metric("accepted");
        info!(%worker_id, slots = hello.slots, "worker session opened");
        Self::send(
            &tx,
            v1::server_msg::Msg::Welcome(v1::Welcome {
                session_id: ctl.id.clone(),
                node_id: self.node_id,
                cluster_id: self.cluster_id.clone(),
                lease_ttl_ms: self.lease_ttl_ms,
                heartbeat_ms: self.heartbeat_ms,
                max_frame_bytes: self.max_frame,
                limits: Some(self.limits),
                members: self.members.clone(),
            }),
        );
        let reason = loop {
            tokio::select! {
                _ = ctl.close.cancelled() => {
                    let reason = ctl.reason().unwrap_or_else(|| "session closed".into());
                    Self::send(&tx, v1::server_msg::Msg::GoAway(v1::GoAway { reason: reason.clone() }));
                    break reason;
                }
                _ = self.shutdown.cancelled() => {
                    let reason = "server shutting down".to_owned();
                    Self::send(&tx, v1::server_msg::Msg::GoAway(v1::GoAway { reason: reason.clone() }));
                    break reason;
                }
                m = inbound.message() => match m {
                    Ok(Some(v1::WorkerMsg { msg: Some(msg) })) => self.route(&ctl.id, msg),
                    Ok(Some(v1::WorkerMsg { msg: None })) => {}
                    Ok(None) => break "session stream ended".to_owned(),
                    Err(e) => break format!("session stream: {}: {}", e.code(), e.message()),
                },
            }
        };
        self.registry.close_session(&ctl.id, &reason);
        session_metric("closed");
        info!(%worker_id, %reason, "worker session closed");
    }

    fn route(&self, id: &[u8], msg: v1::worker_msg::Msg) {
        use v1::worker_msg::Msg;
        match msg {
            Msg::Hello(_) => warn!("second Hello on a session: ignored"),
            Msg::Credit(c) => self.registry.credit(id, c.free_slots),
            Msg::Heartbeat(h) => {
                self.registry.heartbeat(id, &h);
            }
            Msg::Drain(_) => self.registry.worker_drain(id),
            Msg::Done(d) => {
                let (run, token) = (d.run.clone(), d.token);
                if !self
                    .registry
                    .job_event(id, run.as_ref(), token.as_ref(), RemoteEvent::Done(d))
                {
                    warn!("Done for a job this session does not hold: ignored");
                }
            }
            Msg::GuestLogs(g) => {
                let (run, token) = (g.run.clone(), g.token);
                self.registry.job_event(
                    id,
                    run.as_ref(),
                    token.as_ref(),
                    RemoteEvent::GuestLogs(g),
                );
            }
            Msg::CancelEffect(c) => {
                let (run, token) = (c.run.clone(), c.token);
                self.registry.job_event(
                    id,
                    run.as_ref(),
                    token.as_ref(),
                    RemoteEvent::CancelEffect(c),
                );
            }
        }
    }
}

fn api_status(e: ApiError) -> Status {
    let reason = e
        .details
        .as_ref()
        .and_then(|d| d.get("reason"))
        .and_then(|r| r.as_str())
        .unwrap_or_default()
        .to_owned();
    if reason == ostro_api::phase7::REASON_JOIN_TOKEN_INVALID {
        return Status::unauthenticated(e.message);
    }
    match e.code {
        ErrorCode::InvalidArgument => Status::invalid_argument(e.message),
        ErrorCode::NotFound => Status::not_found(e.message),
        ErrorCode::PreconditionFailed | ErrorCode::Conflict => {
            Status::failed_precondition(e.message)
        }
        ErrorCode::Unavailable => Status::unavailable(e.message),
        ErrorCode::Unimplemented => Status::unimplemented(e.message),
        _ => Status::internal(e.message),
    }
}

#[derive(Clone)]
struct ExecuteSvc {
    st: Arc<SvcState>,
}

/// Journal rows `[from, to)` of `run`, paged within `max_frame` bytes.
async fn journal_pages(
    st: Arc<SvcState>,
    access: crate::workers::registry::JobAccess,
    from: u64,
    to: u64,
    out: mpsc::Sender<Result<v1::JournalPage, Status>>,
) {
    let budget = (st.max_frame as usize).saturating_sub(FRAME_RESERVE).max(1);
    let mut page: Vec<v1::JournalRow> = Vec::new();
    let mut page_bytes = 0usize;
    let mut next = from;
    'scan: while next < to {
        let reader = access.handle.reader();
        let run = access.run;
        let limit = usize::try_from((to - next).min(ROWS_PER_SCAN as u64)).unwrap_or(1);
        let rows = match tokio::task::spawn_blocking(move || {
            query::journal_rows(&reader, &run, next, limit)
        })
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                let _ = out
                    .send(Err(Status::internal(format!("journal read: {e}"))))
                    .await;
                return;
            }
            Err(e) => {
                let _ = out
                    .send(Err(Status::internal(format!("journal read task: {e}"))))
                    .await;
                return;
            }
        };
        if rows.is_empty() {
            break;
        }
        for (seq, kind, payload) in rows {
            if seq != next || seq >= to {
                break 'scan;
            }
            let (payload, size) = match payload.kind {
                Some(Kind::Inline(b)) => {
                    let n = b.len();
                    (v1::journal_row::Payload::Inline(b.to_vec()), n + 24)
                }
                Some(Kind::Blob(b)) => {
                    if let Ok(sha) = <[u8; 32]>::try_from(b.sha256.as_slice()) {
                        st.registry.note_blob(access.job_id, sha, b.len);
                    }
                    (
                        v1::journal_row::Payload::Blob(v1::BlobRef {
                            sha: b.sha256.clone(),
                            len: b.len,
                        }),
                        64,
                    )
                }
                None => {
                    let _ = out
                        .send(Err(Status::internal(format!(
                            "journal row {seq} has no payload"
                        ))))
                        .await;
                    return;
                }
            };
            if !page.is_empty() && page_bytes + size > budget {
                let rows = std::mem::take(&mut page);
                st.wire(WireDir::Out, "fetch_journal", page_bytes);
                page_bytes = 0;
                if out
                    .send(Ok(v1::JournalPage { rows, last: false }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            page.push(v1::JournalRow {
                seq,
                kind,
                payload: Some(payload),
            });
            page_bytes += size;
            next += 1;
        }
    }
    st.wire(WireDir::Out, "fetch_journal", page_bytes);
    let _ = out
        .send(Ok(v1::JournalPage {
            rows: page,
            last: true,
        }))
        .await;
}

/// Whether `sha` is a blob ref of `access.run`'s journal (full scan; the fallback when
/// the blob was not served through `FetchJournal` yet). Returns its length.
async fn journal_blob_len(
    access: &crate::workers::registry::JobAccess,
    sha: [u8; 32],
) -> Option<u64> {
    let mut next = 0u64;
    loop {
        let reader = access.handle.reader();
        let run = access.run;
        let rows = tokio::task::spawn_blocking(move || {
            query::journal_rows(&reader, &run, next, ROWS_PER_SCAN)
        })
        .await
        .ok()?
        .ok()?;
        let n = rows.len();
        for (seq, _, p) in rows {
            next = seq + 1;
            if let Some(Kind::Blob(b)) = p.kind
                && b.sha256.as_slice() == sha
            {
                return Some(b.len);
            }
        }
        if n < ROWS_PER_SCAN {
            return None;
        }
    }
}

#[tonic::async_trait]
impl Execute for ExecuteSvc {
    type SessionStream = BoxStream<v1::ServerMsg>;
    type FetchJournalStream = BoxStream<v1::JournalPage>;
    type FetchBlobStream = BoxStream<v1::BlobChunk>;

    async fn session(
        &self,
        req: Request<Streaming<v1::WorkerMsg>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        let peer = match self.st.peer(req.peer_certs()) {
            Ok(p) => p,
            Err(s) => {
                session_metric("refused_identity");
                return Err(s);
            }
        };
        let inbound = req.into_inner();
        let (tx, rx) = mpsc::unbounded_channel();
        tokio::spawn(Arc::clone(&self.st).run_session(peer, inbound, tx));
        Ok(Response::new(Box::pin(UnboundedReceiverStream::new(rx))))
    }

    async fn fetch_journal(
        &self,
        req: Request<v1::FetchJournalRequest>,
    ) -> Result<Response<Self::FetchJournalStream>, Status> {
        let peer = self.st.peer(req.peer_certs())?;
        let r = req.into_inner();
        let access = self
            .st
            .authorize(&peer, &r.session_id, r.run.as_ref(), r.token.as_ref())?;
        if r.to_seq < r.from_seq {
            return Err(Status::invalid_argument("to_seq < from_seq"));
        }
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(journal_pages(
            Arc::clone(&self.st),
            access,
            r.from_seq,
            r.to_seq,
            tx,
        ));
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn fetch_blob(
        &self,
        req: Request<v1::FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        let peer = self.st.peer(req.peer_certs())?;
        let r = req.into_inner();
        let access = self
            .st
            .authorize(&peer, &r.session_id, r.run.as_ref(), r.token.as_ref())?;
        let sha: [u8; 32] = r
            .sha
            .as_slice()
            .try_into()
            .map_err(|_| Status::invalid_argument("sha must be 32 bytes"))?;
        let blobs = Arc::clone(access.handle.blobs());
        let len = if access.module_sha == Some(sha) {
            let path = blobs.path_of(&StoreBlobRef {
                sha256: sha,
                len: 0,
            });
            tokio::fs::metadata(&path)
                .await
                .map_err(|_| Status::not_found("module blob not found"))?
                .len()
        } else if let Some(len) = self.st.registry.known_blob(access.job_id, &sha) {
            len
        } else if let Some(len) = journal_blob_len(&access, sha).await {
            self.st.registry.note_blob(access.job_id, sha, len);
            len
        } else {
            return Err(Status::permission_denied(
                "the blob is neither the leased run's module nor referenced by its journal",
            ));
        };
        let bytes =
            tokio::task::spawn_blocking(move || blobs.get(&StoreBlobRef { sha256: sha, len }))
                .await
                .map_err(|e| Status::internal(format!("blob read task: {e}")))?
                .map_err(|e| Status::not_found(format!("blob: {e}")))?;
        let total = bytes.len() as u64;
        let offset = usize::try_from(r.offset).unwrap_or(usize::MAX);
        if offset > bytes.len() {
            return Err(Status::out_of_range("offset beyond the blob"));
        }
        let chunk = BLOB_CHUNK
            .min((self.st.max_frame as usize).saturating_sub(FRAME_RESERVE))
            .max(1);
        let rest = &bytes[offset..];
        let mut out: Vec<Result<v1::BlobChunk, Status>> = rest
            .chunks(chunk)
            .map(|c| {
                self.st.wire(WireDir::Out, "fetch_blob", c.len());
                Ok(v1::BlobChunk {
                    data: c.to_vec(),
                    last: false,
                    total_len: total,
                })
            })
            .collect();
        match out.last_mut() {
            Some(Ok(c)) => c.last = true,
            _ => out.push(Ok(v1::BlobChunk {
                data: Vec::new(),
                last: true,
                total_len: total,
            })),
        }
        Ok(Response::new(Box::pin(tonic::codegen::tokio_stream::iter(
            out,
        ))))
    }

    async fn append(
        &self,
        req: Request<Streaming<v1::AppendFrame>>,
    ) -> Result<Response<v1::AppendResult>, Status> {
        use v1::append_result::Result as R;
        let peer = self.st.peer(req.peer_certs())?;
        let mut s = req.into_inner();
        let first = s
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty append"))?;
        let access = self.st.authorize(
            &peer,
            &first.session_id,
            first.run.as_ref(),
            first.token.as_ref(),
        )?;
        let handler = self.st.registry.rpc_handler(access.job_id).ok_or_else(|| {
            Status::unimplemented("no remote job handler is attached to this lease (07-12)")
        })?;
        let rejected = |m: String| {
            Ok(Response::new(v1::AppendResult {
                result: Some(R::Rejected(m)),
            }))
        };
        let from_seq = first.from_seq;
        let session_id = first.session_id.clone();
        let token = first.token;
        let mut events: Vec<(u32, Vec<u8>)> = Vec::new();
        let mut cur: Vec<u8> = Vec::new();
        let mut total: u64 = 0;
        let mut frame = Some(first);
        loop {
            let f = match frame.take() {
                Some(f) => f,
                None => match s.message().await? {
                    Some(f) => f,
                    None => return rejected("append stream ended without a last frame".into()),
                },
            };
            if f.session_id != session_id || f.from_seq != from_seq || f.token != token {
                return rejected("append frames disagree on session, token or from_seq".into());
            }
            if f.run.as_ref().map(|r| r.run_id.as_slice()) != Some(access.run.as_bytes().as_slice())
            {
                return rejected("append frames disagree on run".into());
            }
            if usize::try_from(f.event_index).ok() != Some(events.len()) {
                return rejected(format!(
                    "frame for event {} where event {} was expected",
                    f.event_index,
                    events.len()
                ));
            }
            self.st.wire(WireDir::In, "append", f.chunk.len());
            total += f.chunk.len() as u64;
            if total > self.st.max_append {
                return rejected(format!(
                    "append exceeds [workers] max_append_bytes ({})",
                    self.st.max_append
                ));
            }
            cur.extend_from_slice(&f.chunk);
            if f.event_end {
                events.push((f.kind, std::mem::take(&mut cur)));
            }
            if f.last {
                if !cur.is_empty() {
                    return rejected("last frame inside an unfinished event".into());
                }
                break;
            }
        }
        let (reply, rx) = oneshot::channel();
        handler
            .send(JobRpc::Append {
                batch: AppendBatch { from_seq, events },
                reply,
            })
            .map_err(|_| Status::unavailable("the remote job handler is gone"))?;
        let res = rx
            .await
            .map_err(|_| Status::unavailable("the remote job handler dropped the append"))??;
        if matches!(res.result, Some(R::Next(_))) {
            self.st.registry.renew_job(access.job_id);
        }
        Ok(Response::new(res))
    }

    async fn start_effect(
        &self,
        req: Request<v1::StartEffectRequest>,
    ) -> Result<Response<v1::StartEffectResponse>, Status> {
        let peer = self.st.peer(req.peer_certs())?;
        let r = req.into_inner();
        let access = self
            .st
            .authorize(&peer, &r.session_id, r.run.as_ref(), r.token.as_ref())
            .inspect_err(|status| {
                // A fenced (zombie or revoked) session tried to start an effect: refused
                // before any journal read, nothing is sent. Logged so operators see zombie
                // traffic on the leader (07-18 observation, 07-22).
                let run = r
                    .run
                    .as_ref()
                    .and_then(|x| ostro_types::RunId::from_bytes(&x.run_id).ok());
                warn!(
                    worker = %peer.worker_id,
                    run = ?run,
                    token = ?r.token.as_ref().map(|t| (t.epoch, t.n)),
                    op = r.op,
                    error = %status.message(),
                    "refused StartEffect from a session without the lease"
                );
            })?;
        let handler = self.st.registry.rpc_handler(access.job_id).ok_or_else(|| {
            Status::unimplemented("no remote job handler is attached to this lease (07-12)")
        })?;
        let (reply, rx) = oneshot::channel();
        handler
            .send(JobRpc::StartEffect { req: r, reply })
            .map_err(|_| Status::unavailable("the remote job handler is gone"))?;
        let res = rx
            .await
            .map_err(|_| Status::unavailable("the remote job handler dropped the request"))??;
        Ok(Response::new(res))
    }

    async fn renew(
        &self,
        req: Request<v1::RenewRequest>,
    ) -> Result<Response<v1::RenewResponse>, Status> {
        let peer = self.st.peer(req.peer_certs())?;
        let Some(token_id) = peer.token_id.clone() else {
            return Err(Status::failed_precondition(
                "external worker certificates are renewed by their issuer",
            ));
        };
        if !self.st.registry.has_session(&peer) {
            return Err(Status::permission_denied(
                "renewal requires a live session of this identity",
            ));
        }
        let r = req.into_inner();
        let resp = self
            .st
            .join
            .renew(&token_id, &peer.worker_id, &r.csr)
            .await
            .map_err(api_status)?;
        Ok(Response::new(v1::RenewResponse {
            client_cert: resp.client_cert_pem.into_bytes(),
            renew_after_ms: resp.renew_after_ms,
        }))
    }
}
