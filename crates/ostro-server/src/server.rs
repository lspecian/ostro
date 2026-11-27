//! Single-node server bootstrap (plan 04-17): meta -> runtime -> node (+ dispatcher) ->
//! API on one port, and the reverse on shutdown.
//!
//! # Startup order
//!
//! 0. Resolve the data directory. `node.ephemeral` uses a fresh `TempDir` on `/dev/shm`
//!    when it exists, is writable and has at least 512 MiB free (tmpfs: fsync is nearly
//!    free), otherwise in the OS temp dir ([`crate::ephemeral`], one log line says which).
//!    Everything below runs on the same code path either way (D5: no in-memory store).
//!    Then the exclusive data-dir lock is taken (`flock(LOCK_EX|LOCK_NB)` on
//!    `<data>/LOCK`, plan 05-26) before any other disk work: a second `ostro` process on
//!    the same directory fails with [`ServerError::DataDirInUse`]. The lock is held by the
//!    open file description for the server's lifetime, so a `LOCK` file left behind by a
//!    killed process never blocks the next start. The API listener is bound right after,
//!    so a busy port fails before meta, node, blob or AOT cache work.
//! 1. [`ClusterServices::start`] (12-14, D-P12-13/14/16; [`crate::cluster`] lists the
//!    steps): node id and cluster id bound in `node.toml`, joiner and voter checks against
//!    the persisted meta, TLS, the cluster listener (always bound: `MetaPeer`,
//!    `Replication`, later the worker protocol), meta over the live transport
//!    (`MetaHandle::start_with`, a single node is a one-voter group with zero peers), then
//! 2. `InitCluster` with the bound cluster id (idempotent for the same partition count; a
//!    different count is a fatal [`ServerError::PartitionCountMismatch`]); the first boot
//!    stores the await-token key, every boot reads it back. The controller and the
//!    meta-driven assignment source start (meta before partitions, P13-F1 rule 6).
//!
//!    Then [`BridgeServices::build`] (06-14): the Phase 6 effects bridge (sealing key,
//!    secret providers, egress, connectors, the bridge and its SQL GC task) over the live
//!    meta state. It exists before the node because every activation's sink hands its
//!    `EffectDriver` to the engine; the started partitions are registered in its partition
//!    table right after step 4.
//! 3. `Runtime::new` with the cluster token key and the AOT cache under
//!    `<data>/cache/aot`.
//! 4. `Node::open` with the meta-driven assignment source and the follower fetcher, and the
//!    dispatcher running [`EngineExecutor`] (continue-as-new successors resolve their module
//!    through meta, [`MetaModules`], and their retention / schedule-to-start through
//!    [`MetaPolicies`] over the `[runs]` defaults, D9). Every partition opens Offline and its
//!    role driver leads, follows or stays offline as meta says: the first boot places every
//!    partition (`Put(Partition)` epoch 0 -> 1), every later start takes one fresh
//!    `ElectLeader` term per partition, and the leader appends `LeaderAnnounced` at that
//!    epoch. A single node waits until it leads every partition (`first_terms`).
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
//! Phase 7 (07-02) has one seam, [`crate::p7`] (D-W17): [`p7::init`] after the engine
//! runtime (TLS state, worker registry, flow control, spill, join), [`p7::executor`] and
//! [`p7::dispatcher_config`] build the dispatcher, [`p7::attach_node`] runs right after
//! `Node::open`, [`p7::start_services`] after the services seam, and the client listener is
//! served by [`p7::serve_client`] (plaintext until 07-06). The control plane holds
//! [`p7::P7Shared`]; the ready line reads [`ServerHandle::client_endpoint`].
//!
//! Phase 13 (13-02, D-P13-24) has one seam, [`crate::p13`] (final after 13-02):
//! [`ClusterServices::boot_fence`] runs immediately before `Node::open`; the single
//! [`ServerControlPlane`] construction is wrapped by [`p13::wrap_control_plane`] and every
//! consumer (REST, gRPC, the UI, triggers, bulk, on_failure hooks, DLQ requeue) gets the
//! wrapped `Arc<dyn ControlPlane>` through `ControlCtx::routed`; [`p13::fill_extensions`]
//! fills the Phase 13 API slots after the Phase 7/8/11 ones; the cluster listener gains
//! [`p13::cluster_listener_services`]; [`p13::start`] starts the Phase 13 tasks; and
//! [`p13::before_shutdown`] runs first in [`ServerHandle::shutdown`]. The node id is the
//! cluster's effective one (`[node] node_id` or the join marker's), written back into the
//! configuration every later consumer reads.
//!
//! Phase 14 (14-01b, D-P14-33) has one seam, [`crate::p14`] (final after 14-01b):
//! [`p14::wrap_clock`] at the clock construction; [`p14::build_ctx`] right after the cluster
//! start (meta + clock) and before the bridge (its audit sink wraps the bridge's resolver)
//! and the worker join service ([`p14::worker_event`]); [`p14::P14Ctx::attach_node`] after
//! `Node::open`; [`p14::attach`] on the control plane (upload / version / activation gates,
//! per-tenant registration, ingress admission); [`p14::authenticator`] into `ApiConfig`;
//! inside `p11::wire` [`p14::fill_extensions`] (before the Phase 13 slot routing) and
//! [`p14::wrap_ingress`] for REST, gRPC and `/ui` (with [`p14::ui_auth`] and
//! [`p14::ui_scope_layer`]) while every in-process consumer keeps the routed instance;
//! [`p14::start`] after [`p13::start`]; [`p14::start_triggers`] once the server runs (the
//! routed instance and the node's roles, like the Phase 8 drivers; an error shuts the
//! server down again); [`p14::before_shutdown`] first in [`ServerHandle::shutdown`]. The
//! `[auth]` listener rule ([`ServerConfig::validate_auth`]) runs before anything starts.
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
use ostro_meta::{MetaError, MetaHandle};
use ostro_partition::{
    Clock, DispatcherStats, Executor, Node, NodeConfig, NodeError, PartitionRole, PartitionSlot,
};
use ostro_types::{PartitionCount, PartitionId};
use tempfile::TempDir;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::bridge_services::{BridgeEnv, BridgeServices};
use crate::cluster::{ClusterServices, ClusterStart};
use crate::config::ServerConfig;
use crate::control::{ControlCtx, ServerControlPlane};
use crate::derive::MetaModules;
use crate::executor::{EngineExecutor, ExecutorConfig, ExecutorHooks};
use crate::leadership::MetaLeadership;
use crate::p7::{self, P7Handles, P7Shared};
use crate::p13::{self, P13Ctx, P13Init, P13Services};
use crate::p14::{self, P14Ctx, P14Init, P14Services};
use crate::retention::MetaPolicies;
use crate::services::guest_logs::GuestLogSink;
use crate::services::{ServiceCtx, Services};
use crate::telemetry::{Telemetry, TelemetryError, TelemetryGuard};
use crate::tls::ClientEndpoint;

/// How long shutdown waits for in-flight HTTP/gRPC requests before closing them.
const SERVE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
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
    #[error("workers / TLS: {0}")]
    P7(#[from] p7::P7Error),
}

/// Wall-clock durations of the startup steps (logged at info; the 04-19 startup test
/// reads them).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StartupTimings {
    /// Data directory resolution + listener bind.
    pub prepare: Duration,
    /// Cluster identity, TLS, listener and `MetaHandle::start_with` until meta is ready.
    pub meta_leader: Duration,
    /// `InitCluster` + token key read back.
    pub init_cluster: Duration,
    /// Engine runtime construction.
    pub runtime: Duration,
    /// `Node::open` (every partition recovered, term started, services spawned).
    pub node_open: Duration,
    /// [`ClusterServices::start`] in total (identity, TLS, listener, meta, `InitCluster`).
    pub cluster_start: Duration,
    /// After `Node::open`: until every partition took its first term (single node: leads).
    pub first_terms: Duration,
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
    /// The profile whose `[api.tls] mode = "auto"` rule applies (D-W8). The binary sets
    /// it (`ostro dev` / `ostro server`); `None` derives it from the configuration
    /// ([`crate::tls::TlsProfile::of`]).
    pub tls_profile: Option<crate::tls::TlsProfile>,
    /// Phase 12 (12-02 hook, wired by 12-14): the node-to-node transport. `None` = the
    /// production mTLS TCP transport; the conformance harness injects an in-memory one.
    pub cluster_transport: Option<Arc<dyn ostro_cluster::transport::ClusterTransport>>,
    /// Test hook (12-16): rotate WAL segments at this size instead of the 250 MB default,
    /// so retention can trim a small log (whole segments only) and replicas behind the
    /// leader's `log_start` exercise snapshot installs. The binary never sets it.
    pub wal_rotation_bytes: Option<u64>,
    /// Phase 13 test hook (D-P13-7, tier B): this node's `level_max` (gate, advertisement
    /// and fences) instead of `ostro_types::compat::level_max()`; in-process nodes share the
    /// process globals, so the env emulation cannot tell them apart. The binary never sets it.
    #[doc(hidden)]
    pub level_max_override: Option<u32>,
}

impl Default for ServerOptions {
    fn default() -> Self {
        Self {
            // The one production clock (injection root); tests pass their own.
            clock: Arc::new(ostro_partition::SystemClock), // clock-exempt: injection root
            hooks: Arc::new(ExecutorHooks::default()),
            tls_profile: None,
            cluster_transport: None,
            wal_rotation_bytes: None,
            level_max_override: None,
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

    /// Apply `profile`'s `[api.tls] mode = "auto"` rule (D-W8) instead of deriving the
    /// profile from the configuration.
    pub fn with_tls_profile(mut self, profile: crate::tls::TlsProfile) -> Self {
        self.tls_profile = Some(profile);
        self
    }

    /// Use `transport` for node-to-node traffic instead of mTLS TCP (Phase 12 test hook).
    pub fn with_cluster_transport(
        mut self,
        transport: Arc<dyn ostro_cluster::transport::ClusterTransport>,
    ) -> Self {
        self.cluster_transport = Some(transport);
        self
    }

    /// Rotate WAL segments at `bytes` (test hook, see [`ServerOptions::wal_rotation_bytes`]).
    pub fn with_wal_rotation_bytes(mut self, bytes: u64) -> Self {
        self.wal_rotation_bytes = Some(bytes);
        self
    }

    /// Emulate a binary whose `level_max` is `level` (Phase 13 test hook, see
    /// [`ServerOptions::level_max_override`]).
    #[doc(hidden)]
    pub fn with_level_max_override(mut self, level: Option<u32>) -> Self {
        self.level_max_override = level;
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
    mut cfg: ServerConfig,
    opts: ServerOptions,
    telemetry: &mut Option<TelemetryGuard>,
) -> Result<ServerHandle, ServerError> {
    let ServerOptions {
        clock,
        hooks,
        tls_profile,
        cluster_transport,
        wal_rotation_bytes,
        level_max_override,
    } = opts;
    // Phase 13 (D-P13-7): this node's feature level ceiling.
    let level_max = level_max_override.unwrap_or_else(ostro_types::compat::level_max);
    // Phase 10 (10-10, D-P10-6): with `[dev] time_skipping` on (ephemeral `ostro dev`
    // only), the dev clock over the seam clock becomes the one clock. Off: no DevClock.
    let server_profile = tls_profile.unwrap_or_else(|| crate::tls::TlsProfile::of(&cfg))
        == crate::tls::TlsProfile::Server;
    cfg.validate_time_skipping(server_profile)
        .map_err(|e| ServerError::Config(e.to_string()))?;
    // Phase 14 (D-P14-4): `[auth] mode = "none"` on a public listener needs the opt-in.
    cfg.validate_auth(server_profile)
        .map_err(|e| ServerError::Config(e.to_string()))?;
    // Phase 14 (14-10): the failpoints-only SkewClock wraps the seam clock (identity stub).
    let clock = p14::wrap_clock(clock);
    let dev_clock = cfg
        .dev
        .time_skipping
        .mode()
        .map(|mode| crate::devclock::DevClock::new(Arc::clone(&clock), mode));
    let clock: Arc<dyn Clock> = match &dev_clock {
        Some(d) => d.clone(),
        None => clock,
    };
    // clock-exempt: startup step durations are reported (StartupTimings), never used for logic.
    let t0 = Instant::now(); // clock-exempt: startup timing
    let count = PartitionCount::new(cfg.node.partitions)
        .ok_or_else(|| ServerError::Config("node.partitions must be at least 1".into()))?;
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

    // 1./2. cluster services: identity, TLS, listener, meta, InitCluster, controller.
    let t = Instant::now(); // clock-exempt: startup timing
    let cluster = ClusterServices::start(ClusterStart {
        cfg: &cfg,
        data_dir: &data_dir,
        count,
        clock: Arc::clone(&clock),
        transport: cluster_transport,
        api_addr: local_addr,
        level_max,
    })
    .await?;
    timings.cluster_start = t.elapsed();
    timings.meta_leader = cluster.timings.meta_ready;
    timings.init_cluster = cluster.timings.init_cluster;
    // Phase 13: the effective node id (the join marker's on a joined node); every later
    // consumer of the configuration sees it.
    let node_id = cluster.node_id();
    cfg.node.node_id = node_id;
    let cluster_id = cluster.cluster_id().to_owned();
    let token_key = cluster.token_key();
    // Meta is shared (the bridge holds it weakly for presign key writes).
    let meta = Arc::clone(cluster.meta());
    // Phase 14 (14-01b): the seam context (audit sink, quota view), before the bridge and
    // the worker join service that report into it.
    let p14_ctx = match p14::build_ctx(P14Init {
        cfg: Arc::new(cfg.clone()),
        meta: Arc::clone(&meta),
        clock: Arc::clone(&clock),
        node_id,
        data_dir: data_dir.clone(),
        level_max,
    }) {
        Ok(c) => c,
        Err(e) => {
            drop(meta);
            abort_cluster(cluster).await;
            return Err(ServerError::Config(format!("Phase 14: {e}")));
        }
    };

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
            audit: p14::audit_sink(&p14_ctx),
        },
    )
    .await
    {
        Ok(b) => b,
        Err(e) => {
            drop(p14_ctx);
            drop(meta);
            abort_cluster(cluster).await;
            return Err(ServerError::Config(format!("effects bridge: {e}")));
        }
    };

    // 2c. OBS-01 effect tracing (08-21): outbound traceparent + effect spans, with traces on.
    if telemetry
        .as_ref()
        .and_then(TelemetryGuard::tracer_provider)
        .is_some()
    {
        bridge
            .driver()
            .set_hooks(Arc::new(crate::effect_trace::TraceEffectHooks::new(
                Arc::clone(bridge.partitions()),
            )));
    }

    // 3. engine runtime.
    let t = Instant::now(); // clock-exempt: startup timing
    let mut runtime_opts = RuntimeOptions {
        token_key,
        cache_dir: Some(data_dir.join("cache").join("aot")),
        ..RuntimeOptions::default()
    };
    if let Some(d) = &dev_clock {
        // Phase 10 (10-10): journal timestamps come from the same dev clock (Pitfall 9).
        runtime_opts.clock = d.clone();
    }
    let runtime = match Runtime::new(cfg.to_engine_config(), runtime_opts) {
        Ok(rt) => Arc::new(rt),
        Err(e) => {
            bridge.shutdown().await;
            drop(p14_ctx);
            drop(meta);
            abort_cluster(cluster).await;
            return Err(e.into());
        }
    };
    timings.runtime = t.elapsed();

    // 3b. Phase 7 seam (D-W17): TLS state, worker registry, flow control, spill, join.
    let mut p7 = match p7::init(p7::P7Init {
        config: Arc::new(cfg.clone()),
        clock: Arc::clone(&clock),
        data_dir: data_dir.clone(),
        meta: Arc::clone(&meta),
        runtime: Arc::clone(&runtime),
        bridge: Some(Arc::clone(&bridge)),
        cluster_id: cluster_id.clone(),
        node_id,
        lease_ttl: cfg.executor.lease_ttl.get(),
        tls_profile,
        client_addr: Some(local_addr),
    })
    .await
    {
        Ok(p) => p,
        Err(e) => {
            bridge.shutdown().await;
            drop(p14_ctx);
            drop(meta);
            abort_cluster(cluster).await;
            return Err(e.into());
        }
    };

    // Phase 12 (D-P12-13): the worker protocol attaches to the always-bound listener.
    p7.cluster_listener = Some(cluster.execute_slot());
    // Phase 14 (14-01b): worker join / renew / token revoke report to `p14::worker_event`.
    p7.shared.join.attach_p14(&p14_ctx);

    // 4. node + dispatcher(EngineExecutor).
    let t = Instant::now(); // clock-exempt: startup timing
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
    let executor: Arc<dyn Executor> = p7::executor(
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
        &p7,
    );
    let stats = Arc::new(DispatcherStats::default());
    let mut node_cfg = NodeConfig::new(data_dir.clone(), count);
    node_cfg.node_id = node_id;
    node_cfg.leadership = Arc::new(MetaLeadership::new(meta.view().clone()));
    node_cfg.clock = Arc::clone(&clock);
    if let Some(bytes) = wal_rotation_bytes {
        node_cfg.wal.enable_rotation = true;
        node_cfg.wal.rotation_size_bytes = bytes;
    }
    // Phase 8 contracts (08-01): snapshots and cron from `[snapshots]` / `[triggers]`;
    // the upload floor exists only with bottomless on (B4, 08-09). The cron drivers read
    // the meta-backed schedule set (08-14).
    node_cfg.snapshots = cfg.snapshots.to_snapshot_config();
    node_cfg.cron = cfg.triggers.to_cron_config();
    node_cfg.schedule_source = Some(crate::control::schedules::MetaScheduleSource::start(
        meta.view().clone(),
        cfg.runs.clone(),
    ));
    node_cfg.backup_enabled = cfg.backup.enabled();
    // Phase 12 (12-13/12-14): every role comes from meta, RF=1 included.
    node_cfg.assignments = Some(cluster.assignments());
    node_cfg.follower_driver = Some(cluster.follower_driver());
    // p13-bugs2: outbox batches for partitions another node leads go over NodeOps
    // (`p13::inbound`); routed once the Phase 13 context is installed below.
    let inbound = p13::inbound::InboundRoute::default();
    node_cfg.router_wrapper = Some(inbound.wrapper());
    node_cfg.leader.dispatcher = Some(p7::dispatcher_config(
        executor,
        Some(Arc::clone(&stats)),
        &p7,
    ));
    // Phase 13 (P13-F1 rule 6): the boot fence, immediately before any partition opens.
    if let Err(e) = cluster.boot_fence(level_max).await {
        bridge.shutdown().await;
        drop(p14_ctx);
        drop(meta);
        abort_cluster(cluster).await;
        return Err(e);
    }
    let node = match Node::open(node_cfg).await {
        Ok(n) => Arc::new(n),
        Err(e) => {
            bridge.shutdown().await;
            drop(p14_ctx);
            drop(meta);
            abort_cluster(cluster).await;
            return Err(e.into());
        }
    };
    timings.node_open = t.elapsed();
    cluster.attach_node(&node);
    let t = Instant::now(); // clock-exempt: startup timing
    wait_first_terms(&node, &cluster, count).await;
    timings.first_terms = t.elapsed();
    p7::attach_node(&p7, &node);
    // The bridge and the control plane look partition handles up in the node per use (a
    // cached handle would pin a follower's state store and go stale on a restart, 12-16).
    bridge.partitions().bind_node(&node);
    let blobs = node.slots().iter().find_map(|s| match s {
        PartitionSlot::Ready(h) => Some(Arc::clone(h.blobs())),
        PartitionSlot::Failed { .. } => None,
    });
    let ctl_blobs = blobs.clone();
    // Phase 14 (14-01b): module bytes (signature re-verification) and the leadership share.
    p14_ctx.attach_node(blobs.clone(), node.roles());
    let cfg = Arc::clone(&p7.config);

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

    // 4c. Phase 7 services (cluster listener / Execute service, spill, demand).
    let p7_handles = match p7::start_services(&p7).await {
        Ok(h) => h,
        Err(e) => {
            drop(p7);
            services.shutdown().await;
            bridge.shutdown().await;
            cluster.stop_tasks().await;
            shutdown_node(node).await;
            drop(p14_ctx);
            drop(meta);
            abort_cluster(cluster).await;
            return Err(e.into());
        }
    };

    // 5. API.
    let cluster = Arc::new(cluster);
    let control = Arc::new(
        ServerControlPlane::new(
            Arc::clone(&meta),
            &node,
            count,
            Arc::clone(&runtime),
            Arc::clone(&cfg),
            Arc::clone(&bridge),
            Arc::clone(&p7.shared),
        )
        .with_dev(dev_clock.clone())
        .with_leader_hints(cluster.leader_hints(&node))
        .with_module_push(cluster.module_push()),
    );
    // Phase 14 (14-01b): the control-plane hooks (gates, registration, ingress admission).
    p14::attach(&control, &p14_ctx);
    // Phase 13 (13-02): the seam context, the routed control plane (every consumer below
    // gets it), the cluster-listener services and the Phase 13 tasks.
    let p13_ctx = P13Ctx::new(P13Init {
        cfg: Arc::clone(&cfg),
        meta: Arc::clone(&meta),
        cluster: Arc::clone(&cluster),
        node: Arc::clone(&node),
        runtime: Arc::clone(&runtime),
        local_cp: Arc::clone(&control),
        level_max,
        clock: Arc::clone(&clock),
    });
    let routed = p13::wrap_control_plane(&p13_ctx, Arc::clone(&control));
    inbound.install(&p13_ctx);
    cluster.attach_p13_services(p13::cluster_listener_services(&p13_ctx));
    let p13_services = p13::start(Arc::clone(&p13_ctx)).await;
    // Phase 14 (14-01b): audit writer, bootstrap, reseal, quota view. A failure stops the
    // server again once its handle exists (below).
    let p14_started = p14::start(Arc::clone(&p14_ctx)).await;
    // Phase 14 (14-32): the Kafka triggers start like the Phase 8 drivers, on the routed
    // (unguarded) control plane and the node's roles.
    let trigger_cp = Arc::clone(&routed);
    let trigger_roles = node.roles();
    let fatal_rx = p13_ctx.fatal_watch();
    // Phase 10 (10-13): the idle detector and its API write counter, only with time
    // skipping on.
    let dev_time = dev_clock.as_ref().map(|d| {
        crate::devtime::DevTime::new(
            Arc::clone(d),
            &control,
            Arc::clone(&stats),
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        )
    });
    let mut api_cfg = ApiConfig {
        max_module_bytes: usize::try_from(cfg.api.max_module_bytes).unwrap_or(usize::MAX),
        max_request_bytes: usize::try_from(cfg.api.max_request_bytes).unwrap_or(usize::MAX),
        // Phase 14 (14-03): bearer → principal (`AllowDev` in mode none).
        authenticator: p14::authenticator(&p14_ctx),
        ..Default::default()
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
    )
    .with_routed(routed);
    // Phase 11 (11-04): the single seam call; builds the API + UI router. Phase 13 fills
    // its slots there, after the Phase 7/8/11 ones.
    let mut app = crate::p11::wire(
        &ctl,
        Arc::clone(&runtime),
        &services,
        &control.p11,
        &mut api_cfg,
        axum::Router::new(),
        &cfg,
        // Phase 14 (14-01b): the Phase 14 slots first, so the Phase 13 routing covers them.
        &|ext| {
            p14::fill_extensions(&p14_ctx, ext);
            p13::fill_extensions(&p13_ctx, ext);
        },
        // Phase 14 (14-01b): REST, gRPC and `/ui` get the guarded instances; in-process
        // consumers (`ControlCtx::routed`) keep the routed ones.
        &|routed, ext| {
            let ui_auth = p14::ui_auth(&p14_ctx);
            crate::p11::ApiFront {
                guarded: p14::wrap_ingress(&p14_ctx, routed, ext),
                ui_layer: p14::ui_scope_layer(&p14_ctx, Arc::clone(&ui_auth)),
                ui_auth,
            }
        },
    );
    drop(ctl);
    if cfg.telemetry.metrics {
        app = app.merge(crate::telemetry::metrics_router());
    }
    // Phase 12 (12-14): `GET /v1/admin/replication`, server-local like `/metrics`.
    app = app.merge(cluster.status_router(&node));
    // Phase 14 (14-10): the failpoints-only test endpoints `/v1/admin/test/{clock,failpoints}`
    // (a default build mounts nothing).
    app = p14::test_hooks::mount(app, &p14_ctx);
    // Phase 10 (10-13): the `ApiWriteCounter` layer (mutating requests in flight, read by
    // the idle detector); not installed with time skipping off.
    if let Some(t) = &dev_time {
        app = app.layer(axum::middleware::from_fn_with_state(
            t.api_writes(),
            crate::devtime::count_api_writes,
        ));
    }
    let (done_tx, done_rx) = watch::channel(false);
    let client_endpoint = p7::client_endpoint(&p7, local_addr);
    let serve_client = p7::serve_client(&p7, listener, app);
    let serve = tokio::spawn(async move {
        let res = serve_client.await;
        if let Err(e) = &res {
            error!(error = %e, "API server stopped with an error");
        }
        done_tx.send_replace(true);
        res
    });
    timings.total = t0.elapsed();
    // Phase 10 (10-13): auto time skipping (auto mode only); joined first at shutdown.
    let dev_auto = dev_time
        .as_ref()
        .filter(|t| t.clock().mode() == ostro_api::domain::DevTimeMode::Auto)
        .map(|t| {
            let stop = CancellationToken::new();
            (stop.clone(), Arc::clone(t).spawn_auto(stop))
        });

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

    let mut handle = ServerHandle {
        local_addr,
        data_dir,
        cluster_id,
        token_key,
        timings,
        control,
        node: Some(node),
        services: Some(services),
        cluster: Some(cluster),
        clock,
        telemetry: telemetry.take().unwrap_or_else(TelemetryGuard::disabled),
        stats,
        dev_time,
        dev_auto,
        p7_shutdown: p7.shutdown.clone(),
        p7_handles: Some(p7_handles),
        p13: Some(p13_ctx),
        p13_services: Some(p13_services),
        p14: Some(Arc::clone(&p14_ctx)),
        p14_services: None,
        fatal_rx,
        client_endpoint,
        serve: Some(serve),
        done_rx,
        data_lock,
        tempdir,
    };
    // Phase 14 (14-01b): the services started above, then the triggers; either failure stops
    // the running server again (the handle owns every part of it by now).
    let fail = |what: &str, e: p14::P14Error| ServerError::Config(format!("{what}: {e}"));
    match p14_started {
        Ok(s) => handle.p14_services = Some(s),
        Err(e) => {
            drop(p14_ctx);
            if let Err(se) = handle.shutdown().await {
                warn!(error = %se, "shutdown after a Phase 14 startup failure");
            }
            return Err(fail("Phase 14 services", e));
        }
    }
    match p14::start_triggers(&p14_ctx, trigger_cp, trigger_roles).await {
        Ok(Some(task)) => {
            if let Some(s) = handle.p14_services.as_mut() {
                s.push(task);
            }
        }
        Ok(None) => {}
        Err(e) => {
            drop(p14_ctx);
            if let Err(se) = handle.shutdown().await {
                warn!(error = %se, "shutdown after a trigger startup failure");
            }
            return Err(fail("triggers", e));
        }
    }
    drop(p14_ctx);
    Ok(handle)
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

/// A fresh `ostro-*` directory under the base [`crate::ephemeral::create_dir`] picks.
fn ephemeral_dir() -> Result<TempDir, ServerError> {
    crate::ephemeral::create_dir().map_err(|source| ServerError::DataDir {
        path: std::env::temp_dir(),
        source,
    })
}

/// `InitCluster` with the bound `cluster_id` (the first boot stores it and the await-token
/// key; later boots and the other voters get `Ok` for the same partition count); returns the
/// stored token key, opened with `sealed` once `p14::reseal` sealed it (p14-fixups-2).
pub(crate) async fn init_cluster(
    meta: &MetaHandle,
    cluster_id: &str,
    node_id: u64,
    count: PartitionCount,
    now_ms: u64,
    sealed: Option<&ostro_bridge::secrets::SealedStore>,
) -> Result<[u8; 32], ServerError> {
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
    if let Err(e) = meta
        .init_cluster(cluster_id, node_id, configured, now_ms, key)
        .await
    {
        if let Some(stored) = meta
            .view()
            .read(|s| s.cluster().map(|c| c.value.partitions))
            && stored != configured
        {
            return Err(ServerError::PartitionCountMismatch { stored, configured });
        }
        return Err(e.into());
    }
    meta.view()
        .read(|s| crate::p14::reseal::token_key(s, sealed))
        .map_err(|e| ServerError::Config(e.to_string()))?
        .ok_or_else(|| ServerError::Invariant("token key missing after InitCluster".into()))
}

/// How long a multi-node server waits at start for its partitions to take a role.
const FIRST_TERMS_CLUSTER_WAIT: Duration = Duration::from_secs(10);
/// How long a single node waits to lead every partition.
const FIRST_TERMS_SINGLE_WAIT: Duration = Duration::from_secs(60);

/// After `Node::open`: a single node waits until it leads every started partition (or the
/// partition failed); a cluster node waits, bounded, until each started partition leads,
/// takes over or follows, or is one this node holds no replica of. The server serves
/// either way (`/readyz`, 503 not-leader answers).
///
/// "Single" is the Phase 12 RF=1 node (12-14: it must lead every partition when `start`
/// returns): this node is the sole metadata voter **and** the only non-removed member.
/// p13-bugs2: a one-voter group alone is not enough: a joiner's voters are the group's
/// (`{seed}`), and it waited the whole single-node bound for leaderships it never gets.
async fn wait_first_terms(node: &Node, cluster: &ClusterServices, count: PartitionCount) {
    let me = cluster.node_id();
    let view = cluster.meta().view();
    let single = cluster.voters() == [me]
        && view.read(|s| {
            s.members()
                .all(|m| m.value.node_id == me || ostro_cluster::members::is_removed(&m.value))
        });
    let replica_here = |p: PartitionId| {
        view.read(|s| {
            s.partition(p.0)
                .is_some_and(|a| a.value.replicas.contains(&me))
        })
    };
    let wait = async {
        for p in count.ids() {
            let (Some(mut role), Some(h)) = (node.role(p), node.handle(p)) else {
                continue;
            };
            let mut health = h.machine().health();
            drop(h);
            loop {
                let settled = match &*role.borrow_and_update() {
                    PartitionRole::Leader { .. } => true,
                    PartitionRole::Follower { .. } | PartitionRole::Announcing { .. } => !single,
                    PartitionRole::Offline => !single && !replica_here(p),
                };
                if settled
                    || matches!(
                        *health.borrow_and_update(),
                        ostro_partition::PartitionHealth::Failed { .. }
                    )
                {
                    break;
                }
                tokio::select! {
                    r = role.changed() => if r.is_err() { break },
                    r = health.changed() => if r.is_err() { break },
                }
            }
        }
    };
    let bound = if single {
        FIRST_TERMS_SINGLE_WAIT
    } else {
        FIRST_TERMS_CLUSTER_WAIT
    };
    if tokio::time::timeout(bound, wait).await.is_err() {
        warn!(
            ?bound,
            "not every partition took a role at start; serving anyway"
        );
    }
}

/// Stop a cluster side whose server failed to start: tasks, meta, then the listener.
async fn abort_cluster(cluster: ClusterServices) {
    let (meta, rest) = cluster.into_meta().await;
    stop_meta(meta).await;
    rest.stop().await;
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
    /// Phase 12 cluster side (12-14): tasks stop before the node, the listener after meta.
    cluster: Option<Arc<ClusterServices>>,
    /// The one injectable clock (Phase 10 F5).
    clock: Arc<dyn Clock>,
    /// OTel providers + Prometheus upkeep; shut down last.
    telemetry: TelemetryGuard,
    stats: Arc<DispatcherStats>,
    /// Phase 10 (10-13): the idle detector (time skipping on).
    dev_time: Option<Arc<crate::devtime::DevTime>>,
    /// Phase 10 (10-13): the auto-skip loop (auto mode): its stop token and task.
    dev_auto: Option<(CancellationToken, JoinHandle<()>)>,
    /// Phase 7 shutdown (stops the client listener and the Phase 7 tasks).
    p7_shutdown: CancellationToken,
    /// Phase 7 tasks (07-02 seam); joined after the listener drained, before the services.
    p7_handles: Option<P7Handles>,
    /// Phase 13 seam context (13-02); `before_shutdown` uses it first, then it is dropped
    /// with the Phase 13 tasks before the node shuts down.
    p13: Option<Arc<P13Ctx>>,
    /// Phase 13 background tasks.
    p13_services: Option<P13Services>,
    /// Phase 14 seam context (14-01b): `before_shutdown` first, dropped with the Phase 13
    /// context (it holds meta).
    p14: Option<Arc<P14Ctx>>,
    /// Phase 14 background tasks (audit writer, reseal, quota view, triggers).
    p14_services: Option<P14Services>,
    /// Phase 13 fail-stop (running fence, removed node): `wait()` returns once it is set.
    fatal_rx: watch::Receiver<Option<String>>,
    /// How clients reach the client listener (scheme, address, fingerprint).
    client_endpoint: ClientEndpoint,
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

    /// How clients reach the client listener: `http`/`https`, the bound address and the
    /// certificate fingerprint (Phase 7 seam; plaintext until 07-06).
    pub fn client_endpoint(&self) -> &ClientEndpoint {
        &self.client_endpoint
    }

    /// Phase 7 shared state (worker registry, flow control, gauges, TLS, join).
    pub fn p7(&self) -> &Arc<P7Shared> {
        &self.control.p7
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

    /// Phase 10 (10-10): the dev clock when time skipping is on (it is also
    /// [`Self::clock`]); `None` otherwise.
    pub fn dev_clock(&self) -> Option<&Arc<crate::devclock::DevClock>> {
        self.control.dev.as_ref()
    }

    /// Phase 10 (10-13): the idle detector when time skipping is on.
    pub fn dev_time(&self) -> Option<&Arc<crate::devtime::DevTime>> {
        self.dev_time.as_ref()
    }

    /// The telemetry guard (OTLP providers, Prometheus upkeep).
    pub fn telemetry(&self) -> &TelemetryGuard {
        &self.telemetry
    }

    /// Dispatcher counters (shared by every partition).
    pub fn dispatcher_stats(&self) -> &Arc<DispatcherStats> {
        &self.stats
    }

    /// Resolves when the API server stops (shutdown, or a listener error), or when a Phase 13
    /// fail-stop fired ([`ServerHandle::fatal`]).
    pub async fn wait(&self) {
        let mut rx = self.done_rx.clone();
        let mut fatal = self.fatal_rx.clone();
        tokio::select! {
            _ = rx.wait_for(|done| *done) => {}
            Ok(_) = fatal.wait_for(Option::is_some) => {}
        }
    }

    /// Phase 13: why this node must stop (the running fence saw a finalized level above its
    /// `level_max`, or the node was removed), if it must. The binary exits non-zero.
    pub fn fatal(&self) -> Option<String> {
        self.fatal_rx.borrow().clone()
    }

    /// Phase 13 seam context (tests).
    #[doc(hidden)]
    pub fn p13(&self) -> Option<&Arc<P13Ctx>> {
        self.p13.as_ref()
    }

    /// Graceful shutdown: stop accepting and drain requests, shut the node down
    /// (leases revoked, activations stop, final checkpoints), stop meta, then remove the
    /// ephemeral directory.
    pub async fn shutdown(self) -> Result<(), ServerError> {
        let t = Instant::now(); // clock-exempt: shutdown timing
        let ServerHandle {
            control,
            node,
            services,
            cluster,
            mut telemetry,
            p7_shutdown,
            p7_handles,
            serve,
            data_lock,
            tempdir,
            dev_auto,
            p13,
            p13_services,
            p14,
            p14_services,
            ..
        } = self;
        // Phase 14 (14-01b): flush the audit writer and coalescers first.
        if let Some(ctx) = &p14 {
            p14::before_shutdown(ctx).await;
        }
        // Phase 13 (13-11): controlled shutdown first (hand off led partitions, transfer
        // meta leadership), while the node still serves.
        if let Some(ctx) = &p13 {
            p13::before_shutdown(ctx).await;
        }
        // Phase 10 (10-13): no auto skip during shutdown.
        if let Some((stop, task)) = dev_auto {
            stop.cancel();
            let _ = task.await;
        }
        control.stop_serving();
        p7_shutdown.cancel();
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
        // Phase 13 tasks, then the seam context (it holds the cluster side, meta and the
        // control plane).
        if let Some(s) = p13_services {
            s.stop().await;
        }
        drop(p13);
        // Phase 14 tasks (incl. the trigger driver), then the context (it holds meta).
        if let Some(s) = p14_services {
            s.stop().await;
        }
        drop(p14);
        // Phase 7 tasks (stopped by `p7_shutdown` above).
        if let Some(h) = p7_handles {
            h.join().await;
        }
        // Services first: they hold node / meta clones until joined.
        if let Some(services) = services {
            services.shutdown().await;
        }
        // The bridge next: no effect task outlives the node, and the partition handles in
        // its table are released before `Node::shutdown`.
        control.bridge().shutdown().await;
        // Phase 12: controller, ISR publishers, fetch contexts, module sync stop before the
        // node (they hold meta and partition logs).
        if let Some(c) = &cluster {
            c.stop_tasks().await;
        }
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
        // The cluster's meta reference goes first (control holds the other one); its
        // listener stops after meta.
        let rest = match cluster.map(Arc::try_unwrap) {
            Some(Ok(c)) => {
                let (m, rest) = c.into_meta().await;
                drop(m);
                Some(rest)
            }
            Some(Err(c)) => {
                warn!("cluster services still referenced at shutdown");
                c.stop_listener().await;
                None
            }
            None => None,
        };
        match Arc::try_unwrap(control) {
            Ok(cp) => stop_meta(cp.meta).await,
            Err(cp) => {
                warn!("control plane still referenced at shutdown; meta stops when released");
                let _ = tokio::task::spawn_blocking(move || drop(cp)).await;
            }
        }
        if let Some(rest) = rest {
            rest.stop().await;
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
        cfg.cluster.listen = "127.0.0.1:0".parse().expect("addr");
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

    /// Phase 7 seam (07-02): the stubs keep today's behaviour (plaintext endpoint, the
    /// `[executor] slots` semaphore) and the shared state is wired and late-bound.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn p7_seam_is_wired_and_behaviour_preserving() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.cluster.listen = "127.0.0.1:0".parse().expect("addr");
        cfg.node.data_dir = dir.path().to_path_buf();
        cfg.node.partitions = 2;
        cfg.executor.slots = 7;
        let h = start(cfg).await.expect("server start");
        let ep = h.client_endpoint().clone();
        assert_eq!(ep.scheme, "http");
        assert_eq!(ep.addr, h.local_addr());
        assert_eq!(ep.fingerprint, None);
        assert_eq!(ep.url(), format!("http://{}", h.local_addr()));
        let p7 = Arc::clone(h.p7());
        assert_eq!(p7.tls.mode(), crate::tls::TlsMode::Off);
        assert!(p7.engine.get().is_some(), "p7::executor stored the engine");
        assert!(p7.bridge.is_some(), "the Phase 6 bridge is shared");
        let node = p7.node.get().expect("p7::attach_node ran");
        assert_eq!(node.partitions.iter().filter(|r| **r).count(), 2);
        assert!(node.blobs.is_some());
        assert_eq!(p7.embedded_slots.available_permits(), 7);
        assert_eq!(p7.registry.connected(), 0);
        assert!(p7.gauges.snapshot().is_empty());
        let d = crate::workers::demand::snapshot(&p7, "default");
        assert_eq!((d.demand, d.placement.as_str()), (0, "embedded"));
        drop(p7);
        h.shutdown().await.expect("shutdown");
    }
}
