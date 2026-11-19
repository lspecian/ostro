//! Single-node server bootstrap (plan 04-17): meta -> runtime -> node (+ dispatcher) ->
//! API on one port, and the reverse on shutdown.
//!
//! # Startup order
//!
//! 0. Resolve the data directory. `node.ephemeral` uses a fresh `TempDir` on `/dev/shm`
//!    when it is writable (tmpfs: fsync is nearly free), otherwise in the OS temp dir.
//!    Everything below runs on the same code path either way (D5: no in-memory store).
//!    The API listener is bound right away so a busy port fails before any disk work.
//! 1. `MetaHandle::start(<data>/meta)`: single voter, campaigns immediately.
//! 2. `InitCluster` (idempotent for the same partition count; a different count is a
//!    fatal [`ServerError::PartitionCountMismatch`]). The first boot generates the cluster
//!    id and the await-token key; every boot reads the stored key back from meta.
//! 3. `Runtime::new` with the cluster token key and the AOT cache under
//!    `<data>/cache/aot`.
//! 4. `Node::open` with [`MetaLeadership`] as the epoch floor and the dispatcher running
//!    [`EngineExecutor`] (continue-as-new successors resolve their module through meta,
//!    [`MetaModules`], and their retention / schedule-to-start through
//!    [`MetaPolicies`] over the `[runs]` defaults, D9); then every started partition's term
//!    is recorded in meta with
//!    `SetPartitionLeader` (meta rejects a lower epoch, so a stale term never overwrites
//!    a newer one).
//! 5. `ostro_api::router(ServerControlPlane)` served with graceful shutdown. `/readyz`
//!    is 200 only while meta has a leader and every partition slot is ready.
//!
//! # Shutdown ([`ServerHandle::shutdown`])
//!
//! `/readyz` turns 503 -> the listener stops accepting and in-flight requests finish ->
//! `Node::shutdown` (the dispatcher revokes leases, activations stop) -> meta stops ->
//! the ephemeral directory is removed.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_api::{ApiConfig, ControlPlane};
use ostro_engine::error::EngineError;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaResult};
use ostro_partition::{
    DispatcherConfig, DispatcherStats, Executor, Node, NodeConfig, NodeError, PartitionSlot,
};
use ostro_types::PartitionCount;
use tempfile::TempDir;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::config::ServerConfig;
use crate::control::ServerControlPlane;
use crate::derive::MetaModules;
use crate::executor::{EngineExecutor, ExecutorConfig, ExecutorHooks};
use crate::leadership::MetaLeadership;
use crate::retention::MetaPolicies;

/// How long shutdown waits for in-flight HTTP/gRPC requests before closing them.
const SERVE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Preferred parent of ephemeral data directories (tmpfs on Linux).
const SHM_DIR: &str = "/dev/shm";

/// Why the server could not start (or stop cleanly).
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("data directory {path}: {source}")]
    DataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("cannot listen on {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        source: std::io::Error,
    },
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("metadata: {0}")]
    Meta(#[from] MetaError),
    #[error(
        "partition count mismatch: the cluster metadata was initialised with {stored} partitions, \
         but node.partitions is {configured} (the partition count is fixed at creation)"
    )]
    PartitionCountMismatch { stored: u16, configured: u16 },
    #[error("engine runtime: {0}")]
    Runtime(#[from] EngineError),
    #[error("node: {0}")]
    Node(#[from] NodeError),
    #[error("invariant violated: {0}")]
    Invariant(String),
}

/// Wall-clock durations of the startup steps (logged at info; the 04-19 startup test
/// reads them).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StartupTimings {
    /// Data directory resolution + listener bind.
    pub prepare: Duration,
    /// `MetaHandle::start` until this node leads and has caught up.
    pub meta_leader: Duration,
    /// `InitCluster` + token key read back.
    pub init_cluster: Duration,
    /// Engine runtime construction.
    pub runtime: Duration,
    /// `Node::open` (every partition recovered, term started, services spawned).
    pub node_open: Duration,
    /// `SetPartitionLeader` for every started partition.
    pub record_leaders: Duration,
    /// `start` call to serving.
    pub total: Duration,
}

/// Entry point (`Server::start`).
#[derive(Debug, Clone, Copy)]
pub struct Server;

impl Server {
    /// Boot meta, runtime, node and API; see the module docs for the order.
    pub async fn start(cfg: ServerConfig) -> Result<ServerHandle, ServerError> {
        start(cfg).await
    }
}

/// Boot meta, runtime, node and API; see the module docs for the order.
pub async fn start(cfg: ServerConfig) -> Result<ServerHandle, ServerError> {
    start_with_hooks(cfg, Arc::new(ExecutorHooks::default())).await
}

/// [`start`] with executor test hooks (full-server lease/zombie tests). The binary never
/// sets hooks.
#[doc(hidden)]
pub async fn start_with_hooks(
    cfg: ServerConfig,
    hooks: Arc<ExecutorHooks>,
) -> Result<ServerHandle, ServerError> {
    let t0 = Instant::now();
    let count = PartitionCount::new(cfg.node.partitions)
        .ok_or_else(|| ServerError::Config("node.partitions must be at least 1".into()))?;
    let node_id = cfg.node.node_id;
    let (data_dir, tempdir) = resolve_data_dir(&cfg)?;
    let listener = tokio::net::TcpListener::bind(cfg.api.listen)
        .await
        .map_err(|source| ServerError::Bind {
            addr: cfg.api.listen,
            source,
        })?;
    let local_addr = listener.local_addr().map_err(|source| ServerError::Bind {
        addr: cfg.api.listen,
        source,
    })?;
    let mut timings = StartupTimings {
        prepare: t0.elapsed(),
        ..StartupTimings::default()
    };

    // 1. meta (campaigns immediately).
    let t = Instant::now();
    let meta = MetaHandle::start(&data_dir.join("meta"), node_id).await?;
    timings.meta_leader = t.elapsed();

    // 2. cluster identity + token key.
    let t = Instant::now();
    let (cluster_id, token_key) = match init_cluster(&meta, node_id, count, now_ms()).await {
        Ok(v) => v,
        Err(e) => {
            meta.stop().await;
            return Err(e);
        }
    };
    timings.init_cluster = t.elapsed();

    // 3. engine runtime.
    let t = Instant::now();
    let runtime = match Runtime::new(
        cfg.engine.to_engine_config(),
        RuntimeOptions {
            token_key,
            cache_dir: Some(data_dir.join("cache").join("aot")),
            ..RuntimeOptions::default()
        },
    ) {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            meta.stop().await;
            return Err(e.into());
        }
    };
    timings.runtime = t.elapsed();

    // 4. node + dispatcher(EngineExecutor).
    let t = Instant::now();
    let lease_ttl = cfg.executor.lease_ttl.get();
    let exec_cfg = ExecutorConfig {
        heartbeat: cfg.executor.effective_heartbeat(),
        lease_ttl,
        self_fence_margin: lease_ttl / 10,
        max_attempt_failures: cfg.executor.max_attempt_failures,
        retry_backoff: cfg.executor.retry_backoff.get(),
    };
    let executor: Arc<dyn Executor> = Arc::new(
        EngineExecutor::new(Arc::clone(&runtime), exec_cfg)
            .with_hooks(hooks)
            .with_resolvers(
                Arc::new(MetaModules(meta.view().clone())),
                Arc::new(MetaPolicies {
                    meta: meta.view().clone(),
                    defaults: cfg.runs.clone(),
                }),
            ),
    );
    let stats = Arc::new(DispatcherStats::default());
    let mut node_cfg = NodeConfig::new(data_dir.clone(), count);
    node_cfg.node_id = node_id;
    node_cfg.leadership = Arc::new(MetaLeadership::new(meta.view().clone()));
    node_cfg.leader.dispatcher = Some(DispatcherConfig {
        executor,
        lease_ttl,
        slots: Arc::new(Semaphore::new(
            usize::try_from(cfg.executor.slots).unwrap_or(usize::MAX),
        )),
        stats: Some(Arc::clone(&stats)),
    });
    let node = match Node::open(node_cfg).await {
        Ok(n) => n,
        Err(e) => {
            meta.stop().await;
            return Err(e.into());
        }
    };
    timings.node_open = t.elapsed();

    let t = Instant::now();
    if let Err(e) = record_partition_leaders(&meta, &node, node_id).await {
        shutdown_node(node).await;
        meta.stop().await;
        return Err(e);
    }
    timings.record_leaders = t.elapsed();

    // 5. API.
    let partitions = node
        .slots()
        .iter()
        .map(|s| match s {
            PartitionSlot::Ready(h) => Some(h.clone()),
            PartitionSlot::Failed { .. } => None,
        })
        .collect();
    let cfg = Arc::new(cfg);
    let control = Arc::new(ServerControlPlane::new(
        Arc::new(meta),
        partitions,
        count,
        Arc::clone(&runtime),
        Arc::clone(&cfg),
    ));
    let api_cfg = ApiConfig {
        max_module_bytes: usize::try_from(cfg.api.max_module_bytes).unwrap_or(usize::MAX),
        max_request_bytes: usize::try_from(cfg.api.max_request_bytes).unwrap_or(usize::MAX),
    };
    let app = ostro_api::router(Arc::clone(&control) as Arc<dyn ControlPlane>, api_cfg);
    let (stop_tx, mut stop_rx) = watch::channel(false);
    let (done_tx, done_rx) = watch::channel(false);
    let serve = tokio::spawn(async move {
        let res = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.wait_for(|stop| *stop).await;
            })
            .await;
        if let Err(e) = &res {
            error!(error = %e, "API server stopped with an error");
        }
        done_tx.send_replace(true);
        res
    });
    timings.total = t0.elapsed();

    info!(
        addr = %local_addr,
        data_dir = %data_dir.display(),
        ephemeral = tempdir.is_some(),
        cluster_id = %cluster_id,
        partitions = count.get(),
        meta_leader_ms = timings.meta_leader.as_millis() as u64,
        runtime_ms = timings.runtime.as_millis() as u64,
        node_open_ms = timings.node_open.as_millis() as u64,
        total_ms = timings.total.as_millis() as u64,
        ready = control.ready(),
        "ostro server started"
    );

    Ok(ServerHandle {
        local_addr,
        data_dir,
        cluster_id,
        token_key,
        timings,
        control,
        node: Some(node),
        stats,
        stop_tx,
        serve: Some(serve),
        done_rx,
        tempdir,
    })
}

/// Resolve (and create) the data directory; `Some(TempDir)` for ephemeral mode.
fn resolve_data_dir(cfg: &ServerConfig) -> Result<(PathBuf, Option<TempDir>), ServerError> {
    if cfg.node.ephemeral {
        let dir = ephemeral_dir()?;
        Ok((dir.path().to_path_buf(), Some(dir)))
    } else {
        let path = cfg.node.data_dir.clone();
        std::fs::create_dir_all(&path).map_err(|source| ServerError::DataDir {
            path: path.clone(),
            source,
        })?;
        Ok((path, None))
    }
}

/// A fresh `ostro-*` directory on `/dev/shm` when it exists and accepts a probe file,
/// else in `std::env::temp_dir()`.
fn ephemeral_dir() -> Result<TempDir, ServerError> {
    let builder = {
        let mut b = tempfile::Builder::new();
        b.prefix("ostro-");
        b
    };
    let shm = Path::new(SHM_DIR);
    if shm.is_dir() {
        match builder.tempdir_in(shm) {
            Ok(dir) if probe(dir.path()) => return Ok(dir),
            Ok(_) => warn!("{SHM_DIR} is not writable; using the OS temp directory"),
            Err(e) => warn!(error = %e, "cannot use {SHM_DIR}; using the OS temp directory"),
        }
    }
    builder.tempdir().map_err(|source| ServerError::DataDir {
        path: std::env::temp_dir(),
        source,
    })
}

fn probe(dir: &Path) -> bool {
    let p = dir.join(".probe");
    let ok = std::fs::write(&p, b"ok").is_ok();
    let _ = std::fs::remove_file(&p);
    ok
}

/// ms since the Unix epoch (proposers supply every metadata timestamp).
fn now_ms() -> u64 {
    use ostro_partition::Clock;
    ostro_partition::SystemClock.now_ms()
}

/// `InitCluster` (first boot creates identity + token key; later boots read them).
async fn init_cluster(
    meta: &MetaHandle,
    node_id: u64,
    count: PartitionCount,
    now_ms: u64,
) -> Result<(String, [u8; 32]), ServerError> {
    let configured = count.get();
    if let Some(stored) = meta
        .view()
        .read(|s| s.cluster().map(|c| c.value.partitions))
        && stored != configured
    {
        return Err(ServerError::PartitionCountMismatch { stored, configured });
    }
    let mut key = [0u8; 32];
    getrandom::fill(&mut key)
        .map_err(|e| ServerError::Invariant(format!("OS random source failed: {e}")))?;
    let cluster_id = uuid::Uuid::now_v7().to_string();
    let info = match meta
        .init_cluster(&cluster_id, node_id, configured, now_ms, key)
        .await
    {
        Ok(info) => info,
        Err(e) => {
            if let Some(stored) = meta
                .view()
                .read(|s| s.cluster().map(|c| c.value.partitions))
                && stored != configured
            {
                return Err(ServerError::PartitionCountMismatch { stored, configured });
            }
            return Err(e.into());
        }
    };
    let token_key = meta
        .view()
        .read(|s| s.token_key())
        .ok_or_else(|| ServerError::Invariant("token key missing after InitCluster".into()))?;
    Ok((info.cluster_id, token_key))
}

/// Record every started partition's term in meta. A rejection means meta already holds a
/// newer epoch than the term we just started, which `MetaLeadership` makes impossible:
/// fatal.
async fn record_partition_leaders(
    meta: &MetaHandle,
    node: &Node,
    node_id: u64,
) -> Result<(), ServerError> {
    for slot in node.slots() {
        match slot {
            PartitionSlot::Ready(h) => {
                let p = h.partition();
                let epoch = h.epoch().0;
                let res = meta
                    .propose(MetaCommand::SetPartitionLeader {
                        partition: p.0,
                        epoch,
                        leader: node_id,
                        isr: vec![node_id],
                    })
                    .await?;
                if !matches!(res, MetaResult::Ok { .. }) {
                    let msg = format!(
                        "SetPartitionLeader(p{}, epoch {epoch}) rejected: {res:?}",
                        p.0
                    );
                    error!(partition = p.0, epoch, result = ?res, "{msg}");
                    return Err(ServerError::Invariant(msg));
                }
            }
            PartitionSlot::Failed { partition, reason } => {
                error!(partition = partition.0, %reason, "partition failed to start");
            }
        }
    }
    Ok(())
}

async fn shutdown_node(node: Node) {
    if let Err(e) = node.shutdown().await {
        warn!(error = %e, "node shutdown");
    }
}

/// A running server.
pub struct ServerHandle {
    local_addr: SocketAddr,
    data_dir: PathBuf,
    cluster_id: String,
    token_key: [u8; 32],
    timings: StartupTimings,
    control: Arc<ServerControlPlane>,
    node: Option<Node>,
    stats: Arc<DispatcherStats>,
    stop_tx: watch::Sender<bool>,
    serve: Option<JoinHandle<std::io::Result<()>>>,
    done_rx: watch::Receiver<bool>,
    tempdir: Option<TempDir>,
}

impl std::fmt::Debug for ServerHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerHandle")
            .field("local_addr", &self.local_addr)
            .field("data_dir", &self.data_dir)
            .field("ephemeral", &self.tempdir.is_some())
            .field("cluster_id", &self.cluster_id)
            .finish_non_exhaustive()
    }
}

impl ServerHandle {
    /// The bound API address (HTTP/1.1, h2c and gRPC).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The data directory in use (a temp dir in ephemeral mode).
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Whether the data directory is an ephemeral temp dir (removed on shutdown).
    pub fn is_ephemeral(&self) -> bool {
        self.tempdir.is_some()
    }

    /// The cluster id stored in meta.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// The await-token key stored in meta and handed to the engine runtime.
    pub fn token_key(&self) -> [u8; 32] {
        self.token_key
    }

    /// Startup step durations.
    pub fn timings(&self) -> StartupTimings {
        self.timings
    }

    /// `/readyz` state.
    pub fn ready(&self) -> bool {
        self.control.ready()
    }

    /// The control plane behind the API.
    pub fn control(&self) -> &Arc<ServerControlPlane> {
        &self.control
    }

    /// The metadata handle.
    pub fn meta(&self) -> &MetaHandle {
        self.control.meta()
    }

    /// The engine runtime.
    pub fn runtime(&self) -> &Arc<Runtime> {
        self.control.runtime()
    }

    /// The partition node.
    pub fn node(&self) -> &Node {
        self.node.as_ref().expect("node present until shutdown")
    }

    /// Dispatcher counters (shared by every partition).
    pub fn dispatcher_stats(&self) -> &Arc<DispatcherStats> {
        &self.stats
    }

    /// Resolves when the API server stops (shutdown, or a listener error).
    pub async fn wait(&self) {
        let mut rx = self.done_rx.clone();
        let _ = rx.wait_for(|done| *done).await;
    }

    /// Graceful shutdown: stop accepting and drain requests, shut the node down
    /// (leases revoked, activations stop, final checkpoints), stop meta, then remove the
    /// ephemeral directory.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let t = Instant::now();
        let ServerHandle {
            control,
            node,
            stop_tx,
            serve,
            tempdir,
            ..
        } = self;
        control.stop_serving();
        stop_tx.send_replace(true);
        if let Some(mut serve) = serve {
            match tokio::time::timeout(SERVE_DRAIN_TIMEOUT, &mut serve).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => warn!(error = %e, "API server error at shutdown"),
                Ok(Err(e)) => warn!(error = %e, "API server task failed"),
                Err(_) => {
                    warn!("API requests still in flight after {SERVE_DRAIN_TIMEOUT:?}; closing");
                    serve.abort();
                }
            }
        }
        let node_res = match node {
            Some(node) => node.shutdown().await,
            None => Ok(()),
        };
        if let Err(e) = &node_res {
            warn!(error = %e, "node shutdown");
        }
        // The serve task (and every connection) has dropped its control-plane clone, so
        // this is normally the last reference.
        match Arc::try_unwrap(control) {
            Ok(cp) => stop_meta(cp.meta).await,
            Err(cp) => {
                warn!("control plane still referenced at shutdown; meta stops when released");
                let _ = tokio::task::spawn_blocking(move || drop(cp)).await;
            }
        }
        if let Some(dir) = tempdir {
            let path = dir.path().to_path_buf();
            if let Err(e) = dir.close() {
                warn!(path = %path.display(), error = %e, "removing the ephemeral data directory");
            }
        }
        info!(
            elapsed_ms = t.elapsed().as_millis() as u64,
            "ostro server stopped"
        );
        node_res.map_err(ServerError::from)
    }
}

async fn stop_meta(meta: Arc<MetaHandle>) {
    match Arc::try_unwrap(meta) {
        Ok(m) => m.stop().await,
        Err(m) => {
            warn!("metadata handle still referenced at shutdown; stopping when released");
            // Dropping the last reference stops the driver (a blocking join).
            let _ = tokio::task::spawn_blocking(move || drop(m)).await;
        }
    }
}
