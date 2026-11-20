//! Single-node server bootstrap (plan 04-17): meta -> runtime -> node (+ dispatcher) ->
//! API on one port, and the reverse on shutdown.
//!
//! # Startup order
//!
//! 0. Resolve the data directory. `node.ephemeral` uses a fresh `TempDir` on `/dev/shm`
//!    when it is writable (tmpfs: fsync is nearly free), otherwise in the OS temp dir.
//!    Everything below runs on the same code path either way (D5: no in-memory store).
//!    Then the exclusive data-dir lock is taken (`flock(LOCK_EX|LOCK_NB)` on
//!    `<data>/LOCK`, plan 05-26) before any other disk work: a second `ostro` process on
//!    the same directory fails with [`ServerError::DataDirInUse`]. The lock is held by the
//!    open file description for the server's lifetime, so a `LOCK` file left behind by a
//!    killed process never blocks the next start. The API listener is bound right after,
//!    so a busy port fails before meta, node, blob or AOT cache work.
//! 1. `MetaHandle::start(<data>/meta)`: single voter, campaigns immediately.
//! 2. `InitCluster` (idempotent for the same partition count; a different count is a
//!    fatal [`ServerError::PartitionCountMismatch`]). The first boot generates the cluster
//!    id and the await-token key; every boot reads the stored key back from meta.
//!
//!    Then [`BridgeServices::build`] (06-14): the Phase 6 effects bridge (sealing key,
//!    secret providers, egress, connectors, the bridge and its SQL GC task) over the live
//!    meta state. It exists before the node because every activation's sink hands its
//!    `EffectDriver` to the engine; the started partitions are registered in its partition
//!    table right after step 4.
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
//!    Building the control plane starts the long-poll hub ([`crate::control::RunWatchers`]):
//!    one task per started partition subscribed to its apply machine's action broadcast
//!    (05-13).
//!
//! Before 0, [`Telemetry::init`] (08-04) installs the Prometheus recorder (before any
//! partition caches its metric handles) and, with `[telemetry] otlp_endpoint`, builds the
//! OTLP trace and log providers inside this runtime. `GET /metrics` is merged into the
//! API router when `[telemetry] metrics` is on.
//!
//! The Phase 8 API controllers (08-10) are wired at 5: one
//! [`crate::control::ControlCtx`] (node, meta, config, blobs, guest log sink, the control
//! plane as partition client, sealed secrets, the clock, the data dir) and
//! [`crate::control::phase8_extensions`] fill every `ostro_api::ApiExtensions` slot; the
//! hook body limit is `[triggers] webhook_body_limit`. Feature plans fill
//! `control/{schedules,webhooks,run_logs,admin}.rs` only.
//!
//! Between 4 and 5 the **services seam** starts ([`crate::services::Services::start`],
//! P8-D21): `Node::open` has returned (every partition's readiness barrier is open and
//! its leader services run) and the leaders are recorded in meta.
//!
//! One injectable clock (Phase 10 F5): [`ServerOptions::clock`] (the system clock by
//! default) is the single `Arc<dyn Clock>` handed to `InitCluster`'s timestamp,
//! `NodeConfig.clock` and the services' [`crate::services::ServiceCtx::clock`].
//!
//! # Shutdown ([`ServerHandle::shutdown`])
//!
//! `/readyz` turns 503 -> the listener stops accepting and in-flight requests finish ->
//! the services stop (cancel + join) -> the effects bridge stops (in-flight effect tasks get
//! a short grace, the SQL GC stops, partition handles are released) -> `Node::shutdown` (the dispatcher revokes leases,
//! activations stop) -> meta stops -> the data-dir lock is released -> the ephemeral
//! directory is removed -> the telemetry providers are flushed and shut down (Pitfall 11).

use std::fs::File;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_api::{ApiConfig, ControlPlane};
use ostro_engine::error::EngineError;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_meta::{MetaCommand, MetaError, MetaHandle, MetaResult};
use ostro_partition::{
    Clock, DispatcherConfig, DispatcherStats, Executor, Node, NodeConfig, NodeError, PartitionSlot,
    SystemClock,
};
use ostro_types::PartitionCount;
use tempfile::TempDir;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::bridge_services::{BridgeEnv, BridgeServices};
use crate::config::ServerConfig;
use crate::control::{ControlCtx, ServerControlPlane, phase8_extensions};
use crate::derive::MetaModules;
use crate::executor::{EngineExecutor, ExecutorConfig, ExecutorHooks};
use crate::leadership::MetaLeadership;
use crate::retention::MetaPolicies;
use crate::services::guest_logs::GuestLogSink;
use crate::services::{ServiceCtx, Services};
use crate::telemetry::{Telemetry, TelemetryError, TelemetryGuard};

/// How long shutdown waits for in-flight HTTP/gRPC requests before closing them.
const SERVE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
/// Preferred parent of ephemeral data directories (tmpfs on Linux).
const SHM_DIR: &str = "/dev/shm";
/// The data-dir lock file (`<data>/LOCK`, plan 05-26). Its content is unused; only the
/// advisory lock on an open descriptor matters.
pub const LOCK_FILE: &str = "LOCK";

/// Why the server could not start (or stop cleanly).
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("data directory {path}: {source}")]
    DataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error(
        "data dir {} is in use by another ostro process (lock {})",
        path.display(),
        lock.display()
    )]
    DataDirInUse { path: PathBuf, lock: PathBuf },
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
    #[error("telemetry: {0}")]
    Telemetry(#[from] TelemetryError),
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
    start_with_options(cfg, ServerOptions::default()).await
}

/// Process-level knobs that are not configuration: the clock and executor test hooks.
#[derive(Clone)]
pub struct ServerOptions {
    /// The one injectable clock (system clock by default; a `ManualClock` in tests, the
    /// Phase 10 DevClock). The same instance reaches `InitCluster`, the node and the
    /// services.
    pub clock: Arc<dyn Clock>,
    /// Executor test hooks (full-server lease/zombie tests). The binary never sets hooks.
    pub hooks: Arc<ExecutorHooks>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            clock: Arc::new(SystemClock),
            hooks: Arc::new(ExecutorHooks::default()),
        }
    }
}

impl std::fmt::Debug for ServerOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerOptions").finish_non_exhaustive()
    }
}

impl ServerOptions {
    /// Use `clock` instead of the system clock.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Use executor test hooks.
    pub fn with_hooks(mut self, hooks: Arc<ExecutorHooks>) -> Self {
        self.hooks = hooks;
        self
    }
}

/// [`start`] with executor test hooks (full-server lease/zombie tests). The binary never
/// sets hooks.
#[doc(hidden)]
pub async fn start_with_hooks(
    cfg: ServerConfig,
    hooks: Arc<ExecutorHooks>,
) -> Result<ServerHandle, ServerError> {
    start_with_options(cfg, ServerOptions::default().with_hooks(hooks)).await
}

/// [`start`] with explicit [`ServerOptions`] (clock, hooks).
pub async fn start_with_options(
    cfg: ServerConfig,
    opts: ServerOptions,
) -> Result<ServerHandle, ServerError> {
    let mut telemetry = Some(Telemetry::init(&cfg)?);
    let res = start_inner(cfg, opts, &mut telemetry).await;
    if res.is_err()
        && let Some(mut guard) = telemetry
    {
        guard.shutdown().await;
    }
    res
}

/// The startup sequence; takes the telemetry guard into the handle on success.
async fn start_inner(
    cfg: ServerConfig,
    opts: ServerOptions,
    telemetry: &mut Option<TelemetryGuard>,
) -> Result<ServerHandle, ServerError> {
    let ServerOptions { clock, hooks } = opts;
    // clock-exempt: startup step durations are reported (StartupTimings), never used for logic.
    let t0 = Instant::now();
    let count = PartitionCount::new(cfg.node.partitions)
        .ok_or_else(|| ServerError::Config("node.partitions must be at least 1".into()))?;
    let node_id = cfg.node.node_id;
    let (data_dir, tempdir) = resolve_data_dir(&cfg)?;
    // Before any other disk work (meta, node, blobs, AOT cache): one process per data dir.
    let data_lock = lock_data_dir(&data_dir)?;
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
    let (cluster_id, token_key) = match init_cluster(&meta, node_id, count, clock.now_ms()).await {
        Ok(v) => v,
        Err(e) => {
            meta.stop().await;
            return Err(e);
        }
    };
    timings.init_cluster = t.elapsed();
    // From here meta is shared (the bridge holds it weakly for presign key writes).
    let meta = Arc::new(meta);

    // 2b. effects bridge (before the executor: every sink hands out its driver).
    let bridge = match BridgeServices::build(
        &cfg,
        BridgeEnv {
            meta: Arc::downgrade(&meta),
            view: meta.view().clone(),
            data_dir: data_dir.clone(),
            partitions: Arc::new(crate::effects::PartitionTable::new()),
            cluster_id: cluster_id.clone(),
            node_id,
            clock: Arc::clone(&clock),
            public_base_url: cfg.public_base_url(local_addr),
        },
    )
    .await
    {
        Ok(b) => b,
        Err(e) => {
            stop_meta(meta).await;
            return Err(ServerError::Config(format!("effects bridge: {e}")));
        }
    };

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
            bridge.shutdown().await;
            stop_meta(meta).await;
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
    // The guest log sink exists before the node (the executor is part of NodeConfig); the
    // services seam hands out the same instance.
    let guest_logs = Arc::new(
        GuestLogSink::new(&cfg.guest_logs)
            .with_clock(Arc::clone(&clock))
            .with_logger_provider(telemetry.as_ref().and_then(TelemetryGuard::logger_provider)),
    );
    let executor: Arc<dyn Executor> = Arc::new(
        EngineExecutor::new(Arc::clone(&runtime), exec_cfg)
            .with_hooks(hooks)
            .with_guest_log_sink(Arc::clone(&guest_logs))
            .with_bridge(Arc::clone(&bridge))
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
    node_cfg.clock = Arc::clone(&clock);
    // Phase 8 contracts (08-01): snapshots and cron from `[snapshots]` / `[triggers]`;
    // the upload floor exists only with bottomless on (B4, 08-09). `schedule_source` is
    // set by 08-14.
    node_cfg.snapshots = cfg.snapshots.to_snapshot_config();
    node_cfg.cron = cfg.triggers.to_cron_config();
    node_cfg.backup_enabled = cfg.backup.enabled();
    node_cfg.leader.dispatcher = Some(DispatcherConfig {
        executor,
        lease_ttl,
        slots: Arc::new(Semaphore::new(
            usize::try_from(cfg.executor.slots).unwrap_or(usize::MAX),
        )),
        stats: Some(Arc::clone(&stats)),
    });
    let node = match Node::open(node_cfg).await {
        Ok(n) => Arc::new(n),
        Err(e) => {
            bridge.shutdown().await;
            stop_meta(meta).await;
            return Err(e.into());
        }
    };
    timings.node_open = t.elapsed();
    for slot in node.slots() {
        if let PartitionSlot::Ready(h) = slot {
            bridge.partitions().insert(h.clone());
        }
    }

    let t = Instant::now();
    if let Err(e) = record_partition_leaders(&meta, &node, node_id).await {
        bridge.shutdown().await;
        shutdown_node(node).await;
        stop_meta(meta).await;
        return Err(e);
    }
    timings.record_leaders = t.elapsed();

    let partitions: Vec<_> = node
        .slots()
        .iter()
        .map(|s| match s {
            PartitionSlot::Ready(h) => Some(h.clone()),
            PartitionSlot::Failed { .. } => None,
        })
        .collect();
    let blobs = partitions
        .iter()
        .flatten()
        .next()
        .map(|h| Arc::clone(h.blobs()));
    let ctl_blobs = blobs.clone();
    let cfg = Arc::new(cfg);

    // 4b. Services seam (P8-D21): every barrier is open and the leaders are recorded.
    let services = match blobs {
        Some(blobs) => Services::start(
            ServiceCtx {
                node: Arc::clone(&node),
                meta: Arc::clone(&meta),
                blobs,
                config: Arc::clone(&cfg),
                data_dir: data_dir.clone(),
                shutdown: tokio_util::sync::CancellationToken::new(),
                clock: Arc::clone(&clock),
            },
            Arc::clone(&guest_logs),
        ),
        None => {
            warn!("no partition started; node services stay idle");
            Services::idle(Arc::clone(&guest_logs), Arc::clone(&clock))
        }
    };

    // 5. API.
    let control = Arc::new(ServerControlPlane::new(
        Arc::clone(&meta),
        partitions,
        count,
        Arc::clone(&runtime),
        Arc::clone(&cfg),
        Arc::clone(&bridge),
    ));
    let api_cfg = ApiConfig {
        max_module_bytes: usize::try_from(cfg.api.max_module_bytes).unwrap_or(usize::MAX),
        max_request_bytes: usize::try_from(cfg.api.max_request_bytes).unwrap_or(usize::MAX),
    };
    // Phase 8 controllers (08-10): one context, every slot filled.
    let ctl = ControlCtx::new(
        &node,
        meta,
        Arc::clone(&cfg),
        ctl_blobs,
        Arc::clone(&guest_logs),
        Arc::clone(&control),
        Arc::clone(&clock),
        data_dir.clone(),
    );
    let mut app = ostro_api::router_with(
        Arc::clone(&control) as Arc<dyn ControlPlane>,
        api_cfg,
        phase8_extensions(&ctl),
    );
    drop(ctl);
    if cfg.telemetry.metrics {
        app = app.merge(crate::telemetry::metrics_router());
    }
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
        services: Some(services),
        clock,
        telemetry: telemetry.take().unwrap_or_else(TelemetryGuard::disabled),
        stats,
        stop_tx,
        serve: Some(serve),
        done_rx,
        data_lock,
        tempdir,
    })
}

/// Take the exclusive advisory lock on `<data>/LOCK` (`flock(LOCK_EX|LOCK_NB)`); the
/// returned file must stay open for as long as the directory is in use. The lock belongs
/// to the open descriptor, so the OS drops it when the process dies (even on SIGKILL) and
/// a stale `LOCK` file does not block the next start. Tooling that opens `Node` / meta
/// directly (Inspect mode, `export_run`, the format-corpus generator, the crash checker)
/// does not take this lock: it must run on a copy or on a stopped directory.
fn lock_data_dir(data_dir: &Path) -> Result<File, ServerError> {
    let lock = data_dir.join(LOCK_FILE);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock)
        .map_err(|source| ServerError::DataDir {
            path: lock.clone(),
            source,
        })?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(ServerError::DataDirInUse {
            path: data_dir.to_path_buf(),
            lock,
        }),
        Err(std::fs::TryLockError::Error(source)) => {
            Err(ServerError::DataDir { path: lock, source })
        }
    }
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
    // All proposals are submitted concurrently so the meta driver can group-commit them
    // (one log append for P partitions instead of P sequential round trips); each result
    // is still checked per partition, in partition order.
    let mut pending = Vec::new();
    for slot in node.slots() {
        match slot {
            PartitionSlot::Ready(h) => {
                let p = h.partition().0;
                let epoch = h.epoch().0;
                let fut = meta.propose(MetaCommand::SetPartitionLeader {
                    partition: p,
                    epoch,
                    leader: node_id,
                    isr: vec![node_id],
                });
                pending.push((p, epoch, Box::pin(fut)));
            }
            PartitionSlot::Failed { partition, reason } => {
                error!(partition = partition.0, %reason, "partition failed to start");
            }
        }
    }
    let results = join_all(pending.iter_mut().map(|(_, _, f)| f.as_mut())).await;
    for ((p, epoch, _), res) in pending.iter().zip(results) {
        let res = res?;
        if !matches!(res, MetaResult::Ok { .. }) {
            let msg = format!("SetPartitionLeader(p{p}, epoch {epoch}) rejected: {res:?}");
            error!(partition = p, epoch, result = ?res, "{msg}");
            return Err(ServerError::Invariant(msg));
        }
    }
    Ok(())
}

/// Poll every future to completion concurrently (no `futures` dependency); results in input
/// order.
async fn join_all<F: std::future::Future + Unpin>(
    futs: impl IntoIterator<Item = F>,
) -> Vec<F::Output> {
    let mut slots: Vec<(F, Option<F::Output>)> = futs.into_iter().map(|f| (f, None)).collect();
    std::future::poll_fn(|cx| {
        let mut done = true;
        for (f, out) in slots.iter_mut() {
            if out.is_none() {
                match std::pin::Pin::new(f).poll(cx) {
                    std::task::Poll::Ready(v) => *out = Some(v),
                    std::task::Poll::Pending => done = false,
                }
            }
        }
        if done {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    slots.into_iter().filter_map(|(_, o)| o).collect()
}

async fn shutdown_node(node: Arc<Node>) {
    if let Err(e) = shutdown_shared_node(node).await {
        warn!(error = %e, "node shutdown");
    }
}

/// `Node::shutdown` on the last reference (the services have released theirs).
async fn shutdown_shared_node(node: Arc<Node>) -> Result<(), ServerError> {
    match Arc::try_unwrap(node) {
        Ok(node) => node.shutdown().await.map_err(ServerError::from),
        Err(_) => Err(ServerError::Invariant(
            "node still referenced at shutdown (a service did not release it)".into(),
        )),
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
    node: Option<Arc<Node>>,
    /// The services seam (P8-D21); stopped before the node.
    services: Option<Services>,
    /// The one injectable clock (Phase 10 F5).
    clock: Arc<dyn Clock>,
    /// OTel providers + Prometheus upkeep; shut down last.
    telemetry: TelemetryGuard,
    stats: Arc<DispatcherStats>,
    stop_tx: watch::Sender<bool>,
    serve: Option<JoinHandle<std::io::Result<()>>>,
    done_rx: watch::Receiver<bool>,
    /// The exclusive `<data>/LOCK` descriptor, held until shutdown finishes (or drop).
    data_lock: File,
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

    /// The effects bridge (06-14).
    pub fn bridge(&self) -> &Arc<BridgeServices> {
        self.control.bridge()
    }

    /// The node services (P8-D21 seam).
    pub fn services(&self) -> &Services {
        self.services
            .as_ref()
            .expect("services present until shutdown")
    }

    /// The clock shared by meta timestamps, the node and the services.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// The telemetry guard (OTLP providers, Prometheus upkeep).
    pub fn telemetry(&self) -> &TelemetryGuard {
        &self.telemetry
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
            services,
            mut telemetry,
            stop_tx,
            serve,
            data_lock,
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
        // Services first: they hold node / meta clones until joined.
        if let Some(services) = services {
            services.shutdown().await;
        }
        // The bridge next: no effect task outlives the node, and the partition handles in
        // its table are released before `Node::shutdown`.
        control.bridge().shutdown().await;
        let node_res = match node {
            Some(node) => shutdown_shared_node(node).await,
            None => Ok(()),
        };
        // The action broadcasts closed with the node (the forwarding tasks woke every
        // long-poll waiter and exited); abort any that are still running.
        control.stop_watchers();
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
        // Meta and node are stopped: no writer is left, so another process may open the
        // directory from here on.
        if let Err(e) = data_lock.unlock() {
            warn!(error = %e, "releasing the data-dir lock");
        }
        drop(data_lock);
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
        // Last: every span/log of the shutdown above is exported (Pitfall 11).
        telemetry.shutdown().await;
        node_res
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

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_partition::clock::ManualClock;

    /// Phase 10 F5: one injected clock reaches InitCluster's timestamp and the services.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn injected_clock_reaches_cluster_init_and_services() {
        const FAKE_MS: u64 = 1_234_567_000;
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.node.data_dir = dir.path().to_path_buf();
        cfg.node.partitions = 2;
        let clock = ManualClock::new(FAKE_MS);
        let h = start_with_options(
            cfg,
            ServerOptions::default().with_clock(Arc::new(clock.clone())),
        )
        .await
        .expect("server start");
        let created = h
            .meta()
            .view()
            .read(|s| s.cluster().map(|c| c.value.created_at_ms));
        assert_eq!(
            created,
            Some(FAKE_MS),
            "InitCluster used the injected clock"
        );
        assert_eq!(h.clock().now_ms(), FAKE_MS);
        assert_eq!(h.services().clock().now_ms(), FAKE_MS);
        clock.advance(5_000);
        assert_eq!(
            h.services().clock().now_ms(),
            FAKE_MS + 5_000,
            "the services share the very same clock instance"
        );
        h.shutdown().await.expect("shutdown");
    }
}
