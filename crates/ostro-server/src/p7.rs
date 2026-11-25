//! The single Phase 7 wiring seam (D-W17, plan 07-02).
//!
//! `server.rs` calls this module in one place per startup step and nothing else; every
//! Phase 7 feature lives behind a fixed-signature hook in a Phase 7-owned module file:
//!
//! | Step in `server.rs` | Hook | Calls | Owner |
//! |---|---|---|---|
//! | after the engine runtime (3) | [`init`] | `tls::init` (+ registry, flow, spill, join) | 07-06 / 07-09 |
//! | executor (4) | [`executor`] | stores the `EngineExecutor` in [`P7Shared::engine`] | 07-08 (exhaust policy), 07-12 (`sink_for`) |
//! | `NodeConfig.leader.dispatcher` (4) | [`dispatcher_config`] | `flow::install`, `workers::placement::install` | 07-05 / 07-08 / 07-12 |
//! | right after `Node::open` (4) | [`attach_node`] | fills [`P7Shared::node`] | 07-12 |
//! | before serving (4b) | [`start_services`] | `workers::service::start`, `workers::spill::start`, `workers::demand::start` | 07-09 / 07-13 / 07-16 |
//! | the client listener (5) | [`serve_client`] | `tls::listener::serve` | 07-06 |
//! | the ready line | [`client_endpoint`] | `TlsState::endpoint` | 07-06 / 07-15 |
//!
//! [`P7Shared`] is held by the control plane (`ServerControlPlane::p7`, B3) so the Phase 7
//! API (07-11/07-14) reaches the registry, flow control, demand and the join service.
//! Behaviour after 07-02 is unchanged: every hook is a stub that preserves today's path
//! (the dispatcher config is exactly the old literal, the listener is plaintext
//! `axum::serve`, no Phase 7 task runs).
//!
//! Shutdown: [`P7Ctx::shutdown`] is cancelled when the server stops serving; the handles
//! from [`start_services`] are joined before the node shuts down. Nothing in this module
//! holds a strong `MetaHandle` or `Node` reference (the server needs to be their last owner
//! at shutdown): meta is reached through a `MetaView` and a `Weak<MetaHandle>`, the node
//! through its partition handles.

use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};
use std::time::Duration;

use axum::Router;
use ostro_blob::BlobStore;
use ostro_engine::runtime::Runtime;
use ostro_meta::{MetaHandle, MetaView};
use ostro_partition::{
    Clock, DispatcherConfig, DispatcherStats, Executor, Node, PartitionSlot, QueueGauges,
};
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::bridge_services::BridgeServices;
use crate::config::ServerConfig;
use crate::executor::EngineExecutor;
use crate::flow::NodeFlow;
use crate::tls::{self, ClientEndpoint, TlsError, TlsProfile, TlsState};
use crate::workers::join::JoinService;
use crate::workers::registry::WorkerRegistry;
use crate::workers::spill::SpillHandle;
use crate::workers::{demand, service, spill};

/// Phase 7 setup failure.
#[derive(Debug, thiserror::Error)]
pub enum P7Error {
    #[error("{0}")]
    Tls(#[from] TlsError),
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("cluster listener: {0}")]
    Io(#[from] io::Error),
}

/// The started node, late-bound by [`attach_node`] (no strong `Node` reference).
#[derive(Clone)]
pub struct NodeRefs {
    /// One entry per partition: `true` when the slot started. Handles are not kept here
    /// (12-16: a cached handle pins the partition's state store; look them up in the node).
    pub partitions: Vec<bool>,
    /// The node-wide blob store (`None` if no partition started).
    pub blobs: Option<Arc<BlobStore>>,
}

impl std::fmt::Debug for NodeRefs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeRefs")
            .field("partitions", &self.partitions.len())
            .field("blobs", &self.blobs.is_some())
            .finish()
    }
}

/// Phase 7 state shared by the seam, the dispatcher hooks and the control plane.
pub struct P7Shared {
    /// Per-(partition, queue) gauges written by the dispatchers (07-05).
    pub gauges: Arc<QueueGauges>,
    /// Connected workers (07-09).
    pub registry: Arc<WorkerRegistry>,
    /// The node's flow control (07-08).
    pub flow: Arc<NodeFlow>,
    /// Spill state (07-13).
    pub spill: Arc<SpillHandle>,
    /// Client-listener TLS and the cluster CA (07-06).
    pub tls: Arc<TlsState>,
    /// Worker join / renewal (07-09).
    pub join: Arc<JoinService>,
    /// The embedded executor slots (`[executor] slots`), the dispatcher's semaphore.
    pub embedded_slots: Arc<Semaphore>,
    /// The embedded executor, set by [`executor`]; 07-12 builds remote sinks through it.
    pub engine: OnceLock<Arc<EngineExecutor>>,
    /// The Phase 6 effects bridge (06-14), the same instance the embedded executor uses.
    pub bridge: Option<Arc<BridgeServices>>,
    /// Partition handles + blob store, set by [`attach_node`] after `Node::open`.
    pub node: OnceLock<NodeRefs>,
    /// The client listener's bound address (07-22: the join command shows the real port
    /// when `[api] listen` asks for an ephemeral one). `None` when the caller did not
    /// pass it; `[api] listen` is used then.
    pub client_addr: Option<SocketAddr>,
}

impl std::fmt::Debug for P7Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P7Shared")
            .field("tls", &self.tls.mode())
            .field("embedded_slots", &self.embedded_slots.available_permits())
            .field("engine", &self.engine.get().is_some())
            .field("node", &self.node.get())
            .finish_non_exhaustive()
    }
}

/// Everything a Phase 7 hook needs (built once by [`init`]).
pub struct P7Ctx {
    pub config: Arc<ServerConfig>,
    /// The injected clock: the same instance as `NodeConfig.clock` and the services' clock
    /// (Phase 10 F5, D-W26). Phase 7 logic reads time only through it.
    pub clock: Arc<dyn Clock>,
    pub data_dir: PathBuf,
    /// Read view of the metadata state.
    pub meta: MetaView,
    /// Proposals (token use, worker records); weak so the server stays meta's last owner.
    pub meta_handle: Weak<MetaHandle>,
    pub runtime: Arc<Runtime>,
    pub cluster_id: String,
    pub node_id: u64,
    pub lease_ttl: Duration,
    /// Cancelled when the server stops serving (client listener, cluster listener, tasks).
    pub shutdown: CancellationToken,
    pub shared: Arc<P7Shared>,
    /// Phase 12 (D-P12-13): the always-bound cluster listener's Execute slot and bound
    /// address; `workers::service::start` attaches the worker protocol to it. `None` = bind
    /// a listener of its own (pre-Phase-12 behaviour; tests).
    pub cluster_listener: Option<(crate::workers::service::LateExecute, SocketAddr)>,
}

impl std::fmt::Debug for P7Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P7Ctx")
            .field("data_dir", &self.data_dir)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("lease_ttl", &self.lease_ttl)
            .finish_non_exhaustive()
    }
}

/// Inputs of [`init`] (what `server.rs` has after step 3).
pub struct P7Init {
    pub config: Arc<ServerConfig>,
    pub clock: Arc<dyn Clock>,
    pub data_dir: PathBuf,
    pub meta: Arc<MetaHandle>,
    pub runtime: Arc<Runtime>,
    pub bridge: Option<Arc<BridgeServices>>,
    pub cluster_id: String,
    pub node_id: u64,
    pub lease_ttl: Duration,
    /// The profile whose `[api.tls] mode = "auto"` rule applies (D-W8). `None` falls back
    /// to [`TlsProfile::of`] (the `auto_sealing_key` heuristic) for library callers that
    /// do not know it; the binary always passes the profile it started (07-22).
    pub tls_profile: Option<TlsProfile>,
    /// The client listener's bound address (see [`P7Shared::client_addr`]).
    pub client_addr: Option<SocketAddr>,
}

/// Build the Phase 7 context: TLS (`tls::init`), registry, flow control, spill, join.
pub async fn init(i: P7Init) -> Result<P7Ctx, P7Error> {
    let P7Init {
        config,
        clock,
        data_dir,
        meta,
        runtime,
        bridge,
        cluster_id,
        node_id,
        lease_ttl,
        tls_profile,
        client_addr,
    } = i;
    let profile = tls_profile.unwrap_or_else(|| TlsProfile::of(&config));
    // 13-09: a joined node has no CA key of its own; take the cluster CA from meta first.
    crate::p13::joiner::import_cluster_ca(&data_dir, &cluster_id, meta.view());
    let tls = tls::init_with_profile(&config, &data_dir, &cluster_id, profile)?;
    // ACME renewal scheduling reads the injected clock (Phase 10 F5, D-W26). First call
    // wins: in a process that starts several servers (tests) the first clock stays.
    tls::acme::set_clock(Arc::clone(&clock));
    let view = meta.view().clone();
    let gauges = Arc::new(QueueGauges::new());
    let shared = Arc::new(P7Shared {
        registry: Arc::new(WorkerRegistry::new(&config.workers)),
        flow: Arc::new(NodeFlow::new(view.clone(), Arc::clone(&gauges))),
        spill: Arc::new(SpillHandle::new()),
        join: Arc::new(JoinService::new(
            view.clone(),
            Arc::clone(&tls),
            &config.workers,
        )),
        tls,
        gauges,
        embedded_slots: Arc::new(Semaphore::new(
            usize::try_from(config.executor.slots).unwrap_or(usize::MAX),
        )),
        engine: OnceLock::new(),
        bridge,
        node: OnceLock::new(),
        client_addr,
    });
    Ok(P7Ctx {
        config,
        clock,
        data_dir,
        meta: view,
        meta_handle: Arc::downgrade(&meta),
        runtime,
        cluster_id,
        node_id,
        lease_ttl,
        shutdown: CancellationToken::new(),
        shared,
        cluster_listener: None,
    })
}

/// The dispatcher's executor: keeps the `EngineExecutor` in [`P7Shared::engine`] and
/// returns it as the node's executor (07-08 adds the exhaust policy here).
pub fn executor(engine: EngineExecutor, ctx: &P7Ctx) -> Arc<dyn Executor> {
    // 07-08 (OPS-01): the flow's `on_retries_exhausted` decides fail vs auto-pause.
    let policy: Arc<dyn crate::executor::ExhaustPolicy> = ctx.shared.flow.clone();
    let engine = Arc::new(engine.with_exhaust_policy(policy));
    if ctx.shared.engine.set(Arc::clone(&engine)).is_err() {
        warn!("p7::executor called twice; keeping the first executor");
    }
    engine
}

/// The node's dispatcher configuration: `DispatcherConfig::new` (never a literal) with the
/// lease TTL, the embedded slot semaphore and the stats, then the flow-control and
/// placement hooks. There is no separate dispatcher-hooks field (05-11 kept its hooks on
/// the executor: `ExecutorHooks`), so nothing else is carried.
pub fn dispatcher_config(
    executor: Arc<dyn Executor>,
    stats: Option<Arc<DispatcherStats>>,
    ctx: &P7Ctx,
) -> DispatcherConfig {
    let mut cfg = DispatcherConfig::new(executor);
    cfg.lease_ttl = ctx.lease_ttl;
    cfg.slots = Arc::clone(&ctx.shared.embedded_slots);
    cfg.stats = stats;
    crate::flow::install(ctx, &mut cfg);
    crate::workers::placement::install(ctx, &mut cfg);
    cfg
}

/// Record the opened node (partition handles + blob store) in [`P7Shared::node`].
pub fn attach_node(ctx: &P7Ctx, node: &Node) {
    let partitions: Vec<bool> = node
        .slots()
        .iter()
        .map(|s| matches!(s, PartitionSlot::Ready(_)))
        .collect();
    let blobs = node.slots().iter().find_map(|s| match s {
        PartitionSlot::Ready(h) => Some(Arc::clone(h.blobs())),
        PartitionSlot::Failed { .. } => None,
    });
    if ctx.shared.node.set(NodeRefs { partitions, blobs }).is_err() {
        warn!("p7::attach_node called twice; keeping the first node");
    }
}

/// Background tasks started by [`start_services`]; joined at shutdown.
#[derive(Debug, Default)]
pub struct P7Handles {
    tasks: Vec<JoinHandle<()>>,
}

impl P7Handles {
    /// Number of running Phase 7 tasks.
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Join every task (they stop on [`P7Ctx::shutdown`], cancelled before this).
    pub async fn join(self) {
        for t in self.tasks {
            if let Err(e) = t.await
                && e.is_panic()
            {
                warn!(error = %e, "a Phase 7 task panicked");
            }
        }
    }
}

/// Start the cluster listener / Execute service, the spill sampler and the demand ticker.
pub async fn start_services(ctx: &P7Ctx) -> Result<P7Handles, P7Error> {
    let mut tasks = Vec::new();
    tasks.extend(service::start(ctx)?);
    tasks.extend(spill::start(ctx));
    tasks.extend(demand::start(ctx));
    Ok(P7Handles { tasks })
}

/// Serve the client listener (plaintext or TLS per [`P7Shared::tls`]) until
/// [`P7Ctx::shutdown`] fires. The returned future owns everything it needs.
pub fn serve_client(
    ctx: &P7Ctx,
    listener: TcpListener,
    app: Router,
) -> impl Future<Output = io::Result<()>> + Send + 'static {
    tls::listener::serve(
        Arc::clone(&ctx.shared.tls),
        listener,
        app,
        ctx.shutdown.clone(),
    )
}

/// How clients reach the bound client listener (scheme, address, fingerprint).
pub fn client_endpoint(ctx: &P7Ctx, addr: SocketAddr) -> ClientEndpoint {
    ctx.shared.tls.endpoint(addr)
}
