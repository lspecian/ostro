//! The single Phase 13 seam (D-P13-24, plan 13-02). **Final after 13-02**: later Phase 13
//! plans implement only the hook file each call below names; `server.rs`, `cluster.rs`,
//! `config.rs`, `crates/ostro/src/main.rs` and this file stay untouched (a missing hook is a
//! STOP for the later plan).
//!
//! # Call sites (fixed)
//!
//! | Where | Call | Hook (owner) |
//! |-------|------|--------------|
//! | `cluster.rs`, first in boot (before `bind_node_id` and any TLS load) | [`join_identity`] | `joiner::join_identity` (13-09) |
//! | `cluster.rs`, identity | the marker feeds node_id (node.toml → marker → config) and cluster_id (config → node.toml → marker → CA → ClusterInfo → generated; never generated while a marker exists) | — |
//! | `cluster.rs`, after the cluster id resolution | [`boot_mode`] over a [`DataDirProbe`] | `joiner::boot_mode` (13-09) |
//! | `cluster.rs`, PreStart (persisted ConfState, before `MetaHandle::start_with`) | [`voter_check`] `VoterCheckPhase::PreStart` | `joiner::voter_check` (13-09) |
//! | `cluster.rs`, TLS | [`joined_tls`] (a joined node's certificate; `None` = the Phase 12 CA path) | `joiner::joined_tls` (13-09) |
//! | `cluster.rs`, PostReady (Raft voters after `wait_ready`) | [`voter_check`] `VoterCheckPhase::PostReady` | `joiner::voter_check` (13-09) |
//! | `cluster.rs` / `server.rs`, immediately before `Node::open` | [`boot_fence`] | `ostro_cluster::compat::boot_fence` (13-04) |
//! | `server.rs`, the single `ServerControlPlane` construction site | [`wrap_control_plane`] | `route::wrap_control_plane` (13-12) |
//! | `server.rs` → `p11::wire`, after the Phase 7/8/11 slots | [`fill_extensions`] | `cluster_api`, `module_lifecycle` (13-02), `route::wrap_extensions` (13-12) |
//! | `server.rs` → `ClusterServices::attach_p13_services` | [`cluster_listener_services`] | `membership::service` (13-09), `node_ops` (13-02/13-03), `route::node_api_services` (13-12) |
//! | `server.rs`, after `ClusterServices` start and the control plane | [`start`] | 13-04, `membership::start` (13-09), placement controller (13-10), `module_gc::start` (13-15), `prewarm::start` (13-14) |
//! | `server.rs`, first in `ServerHandle::shutdown` | [`before_shutdown`] | `ostro_cluster::shutdown::controlled_shutdown` (13-11) |
//!
//! Every hook is a stub that keeps the Phase 12 behaviour exactly until its plan lands: the
//! boot rule and both voter checks are the Phase 12 rules, the control plane is passed
//! through, gates allow, fences pass, loops idle, handoffs answer `Aborted`.
//!
//! # Ownership
//!
//! [`P13Ctx`] holds the node weakly (the server handle owns the last strong reference, so
//! `Node::shutdown` can take it back). The server drops its own `Arc<P13Ctx>` and joins
//! [`P13Services`] before the node shuts down; the API router's clones go with the router,
//! and the cluster listener's services are detached in `ClusterServices::stop_tasks`.

pub mod cluster_api;
pub mod join;
pub mod joiner;
pub mod membership;
pub mod module_gc;
pub mod module_lifecycle;
pub mod module_refs;
pub mod node_ops;
pub mod prewarm;
pub mod promote;
pub mod status;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ostro_api::{ApiExtensions, ControlPlane};
use ostro_cluster::address_book::Seed;
use ostro_cluster::compat::CompatError;
use ostro_cluster::contracts::HandoffClient;
use ostro_cluster::handoff::{HandoffConfig, LocalHandoff, LogLookup, NodeOpsHandoffClient};
use ostro_cluster::identity::{BootError, ConfStateView};
use ostro_cluster::placement::controller::{
    PlacementConfig, PlacementController, PlacementTrigger,
};
use ostro_engine::runtime::Runtime;
use ostro_meta::ext::NodeVersion;
use ostro_meta::{MetaHandle, MetaView};
use ostro_partition::{Clock, HandoffControl, HandoffGuard, Node, PartitionError};
use ostro_types::PartitionId;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tonic::service::Routes;
use tracing::{error, info, warn};

use crate::cluster::ClusterServices;
use crate::config::ServerConfig;
use crate::control::ServerControlPlane;

/// `BoxFuture` without a `futures` dependency in this crate.
type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// The answer of a hook that is still a stub: `Unimplemented` (HTTP 501) naming its plan.
pub(crate) fn lands_in(method: &str, plan: &str) -> ostro_api::ApiError {
    ostro_api::ApiError::new(
        ostro_api::ErrorCode::Unimplemented,
        format!("{method}: Phase 13: implemented in {plan}"),
    )
    .with_details(serde_json::json!({ "method": method }))
}

// ---------------------------------------------------------------------------------
// Boot types (cluster.rs)
// ---------------------------------------------------------------------------------

/// The durable join marker `<data>/cluster-seeds.toml` (D-P13-9 / D-P13-29b), written first
/// by `ostro cluster join`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinMarker {
    /// The node id the seed allocated.
    pub node_id: u64,
    /// The seed's cluster id (never regenerated, D-P13-29d).
    pub cluster_id: String,
    /// Seed peers: their cluster (and replication) addresses.
    pub seeds: Vec<Seed>,
}

/// What `cluster.rs` knows about the data dir before meta starts ([`boot_mode`] input).
#[derive(Clone, Debug)]
pub struct DataDirProbe {
    /// The data dir.
    pub data_dir: PathBuf,
    /// `<data>/meta`.
    pub meta_dir: PathBuf,
    /// The persisted meta is blank (no Raft state or snapshot).
    pub meta_blank: bool,
    /// The data dir already holds partition data (state store or WAL segments).
    pub has_partition_data: bool,
    /// The join marker, if one exists ([`join_identity`]).
    pub join_marker: Option<JoinMarker>,
    /// `<data>/tls` holds a complete joined-node certificate set ([`tls_complete`]).
    pub tls_complete: bool,
}

/// How this boot starts meta (D-P13-29b).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootMode {
    /// Blank meta, no partition data, no marker: the Phase 12 bootstrap
    /// (`initial_voters = self ∪ peers`, `InitCluster`).
    Fresh,
    /// Non-blank meta: restart (`InitCluster` is an idempotent check).
    Existing,
    /// Blank meta + join marker: `initial_voters = []`, seeds from the marker, wait for a
    /// leader (bounded by `[cluster] join_timeout`), **never** `InitCluster`.
    Joiner,
}

/// Which of the two Phase 12 voter checks runs (D-P13-29a, plan check B2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoterCheckPhase {
    /// The persisted ConfState, before `MetaHandle::start_with`.
    PreStart,
    /// The live Raft voters, after `wait_ready`.
    PostReady,
}

/// The voters / learners a voter check sees (PreStart: the persisted ConfState, empty when
/// meta is blank; PostReady: the live Raft configuration).
pub type ConfView = ConfStateView;

/// A joined node's TLS material from `<data>/tls` (written by `ostro cluster join`; the node
/// holds no CA key). [`joined_tls`] returns it in place of the Phase 12 CA path.
#[derive(Clone)]
pub struct JoinedTls {
    /// The cluster CA certificate (PEM).
    pub ca_pem: String,
    /// The node certificate chain (PEM).
    pub cert_chain_pem: String,
    /// The node private key (PEM).
    pub key_pem: String,
}

impl std::fmt::Debug for JoinedTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinedTls").finish_non_exhaustive()
    }
}

/// `ostro cluster join` arguments (the `ostro` binary intercepts the CLI variant and hands
/// them to [`joiner::run_join`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct JoinArgs {
    /// The seed's API URL (`https://seed:7700`).
    pub seed: String,
    /// The node join token (`njt_...`).
    pub token: String,
    /// The CA pin (`sha256:<hex>`, D-W20).
    pub ca_hash: String,
    /// The data dir to prepare (`--data`); `None` = the server profile's default.
    pub data_dir: Option<PathBuf>,
    /// The server configuration file to start with (`--config`).
    pub config: Option<PathBuf>,
    /// Advertised API address (`--advertise-api`).
    pub advertise_api: Option<String>,
    /// Advertised cluster listener address (`--advertise-cluster`).
    pub advertise_cluster: Option<String>,
    /// Advertised replication listener address (`--advertise-repl`).
    pub advertise_repl: Option<String>,
    /// Prepare only; do not start the server (`--no-start`).
    pub no_start: bool,
}

/// What a successful [`joiner::run_join`] prepared (the binary starts the server from it
/// unless `--no-start`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinPrepared {
    /// The allocated (or resumed) node id.
    pub node_id: u64,
    /// The seed's cluster id.
    pub cluster_id: String,
    /// The prepared data dir.
    pub data_dir: PathBuf,
}

/// Read the join marker, first in boot (NB1). Also refuses ("re-run `ostro cluster join`")
/// when a marker exists but `<data>/tls` is incomplete, before any TLS load.
pub fn join_identity(data_dir: &Path) -> Result<Option<JoinMarker>, BootError> {
    joiner::join_identity(data_dir)
}

/// Whether `<data>/tls` holds a complete joined-node certificate set.
pub fn tls_complete(data_dir: &Path) -> bool {
    joiner::tls_complete(data_dir)
}

/// A joined node's TLS material (`None` = the Phase 12 CA path: `[cluster.tls]` files or
/// this node's own CA).
pub fn joined_tls(data_dir: &Path) -> Result<Option<JoinedTls>, BootError> {
    joiner::joined_tls(data_dir)
}

/// The boot rule (D-P13-29b). Stub: exactly the Phase 12 rule (blank meta over partition
/// data fail-stops; blank meta bootstraps; otherwise existing).
pub fn boot_mode(cfg: &ServerConfig, probe: &DataDirProbe) -> Result<BootMode, BootError> {
    joiner::boot_mode(cfg, probe)
}

/// Whether dynamic membership is in use (Member records with a MemberState exist), the
/// `members_known` input of [`voter_check`]. PreStart passes `None` (meta is not open),
/// PostReady the live view. Stub: `false` (both Phase 12 rules apply).
pub fn members_known(data_dir: &Path, view: Option<&MetaView>) -> bool {
    joiner::members_known(data_dir, view)
}

/// Both Phase 12 meta voter checks (D-P13-29a). Stub: PreStart = persisted voters (when any)
/// must equal self ∪ peers; PostReady = live voters must equal self ∪ peers.
pub fn voter_check(
    view: &ConfView,
    cfg: &ServerConfig,
    members_known: bool,
    phase: VoterCheckPhase,
) -> Result<(), BootError> {
    joiner::voter_check(view, cfg, members_known, phase)
}

/// The boot fence (P13-F1 rule 6): after meta catch-up, immediately before `Node::open`.
pub async fn boot_fence(meta: &MetaHandle, level_max: u32) -> Result<(), CompatError> {
    ostro_cluster::compat::boot_fence(meta, level_max).await
}

// ---------------------------------------------------------------------------------
// Runtime context (server.rs)
// ---------------------------------------------------------------------------------

/// Everything a Phase 13 hook reaches, built once in `server.rs` after the control plane.
pub struct P13Ctx {
    /// The effective configuration (the Phase 13 keys of `[cluster]`, `[api]`,
    /// `[modules]`, `[workers]`).
    pub cfg: Arc<ServerConfig>,
    /// Metadata.
    pub meta: Arc<MetaHandle>,
    /// The node's cluster side (transport, address book, liveness, assignments).
    pub cluster: Arc<ClusterServices>,
    node: Weak<Node>,
    /// The engine runtime (AOT pre-warm, replay checks).
    pub runtime: Arc<Runtime>,
    /// The local (unrouted) control plane.
    pub local_cp: Arc<ServerControlPlane>,
    /// This node's highest feature level ([`ServerOptions::level_max_override`] else
    /// `ostro_types::compat::level_max()`).
    ///
    /// [`ServerOptions::level_max_override`]: crate::ServerOptions::level_max_override
    pub level_max: u32,
    /// The one injectable clock.
    pub clock: Arc<dyn Clock>,
    /// The node-local handoff executor (NodeOps.HandOff server side, 13-11).
    pub handoff: Arc<LocalHandoff>,
    /// The handoff client (placement controller, controlled shutdown; 13-11).
    pub handoff_client: Arc<dyn HandoffClient>,
    /// NodeOps.ModuleRefs answers (13-15).
    pub module_refs: Arc<module_refs::LeaderModuleRefs>,
    /// Wakes the placement controller (`POST /v1/cluster/rebalance`, 13-10).
    pub placement_trigger: PlacementTrigger,
    fatal: watch::Sender<Option<String>>,
}

impl std::fmt::Debug for P13Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P13Ctx")
            .field("node_id", &self.cluster.node_id())
            .field("level_max", &self.level_max)
            .finish_non_exhaustive()
    }
}

/// [`P13Ctx::new`] inputs.
pub struct P13Init {
    pub cfg: Arc<ServerConfig>,
    pub meta: Arc<MetaHandle>,
    pub cluster: Arc<ClusterServices>,
    pub node: Arc<Node>,
    pub runtime: Arc<Runtime>,
    pub local_cp: Arc<ServerControlPlane>,
    pub level_max: u32,
    pub clock: Arc<dyn Clock>,
}

impl P13Ctx {
    /// Build the context and the Phase 13 collaborators over the 13-01 constructors.
    pub fn new(i: P13Init) -> Arc<Self> {
        let weak = Arc::downgrade(&i.node);
        let c = &i.cfg.cluster;
        let logs_node = weak.clone();
        let logs: LogLookup = Arc::new(move |p: PartitionId| {
            logs_node
                .upgrade()
                .and_then(|n| n.handle(p))
                .map(|h| Arc::clone(h.log()))
        });
        let handoff = Arc::new(LocalHandoff::new(
            Arc::new(NodeHandoff(weak.clone())),
            Arc::clone(&i.meta),
            logs,
            HandoffConfig {
                grace: c.handoff_grace.get(),
                catchup_timeout: c.handoff_catchup_timeout.get(),
            },
        ));
        let handoff_client: Arc<dyn HandoffClient> = Arc::new(NodeOpsHandoffClient::new(
            Arc::clone(i.cluster.transport()),
            Arc::clone(&handoff),
        ));
        let module_refs = Arc::new(module_refs::LeaderModuleRefs::new(
            weak.clone(),
            i.node.roles(),
        ));
        let (fatal, _) = watch::channel(None);
        Arc::new(Self {
            cfg: i.cfg,
            meta: i.meta,
            cluster: i.cluster,
            node: weak,
            runtime: i.runtime,
            local_cp: i.local_cp,
            level_max: i.level_max,
            clock: i.clock,
            handoff,
            handoff_client,
            module_refs,
            placement_trigger: PlacementTrigger::new(),
            fatal,
        })
    }

    /// This node's id.
    pub fn node_id(&self) -> u64 {
        self.cluster.node_id()
    }

    /// The node, `None` once shutdown released it.
    pub fn node(&self) -> Option<Arc<Node>> {
        self.node.upgrade()
    }

    /// Fail-stop this node (running fence, removed node): logged once; the server's
    /// `wait()` returns and the binary shuts down with a non-zero exit.
    pub fn fatal(&self, msg: String) {
        if self.fatal.borrow().is_some() {
            return;
        }
        error!(reason = %msg, "fatal: this node stops");
        self.fatal.send_replace(Some(msg));
    }

    /// Subscribe to [`P13Ctx::fatal`].
    pub fn fatal_watch(&self) -> watch::Receiver<Option<String>> {
        self.fatal.subscribe()
    }

    /// This node's `NodeVersion` advertisement (ext 96).
    pub fn node_version(&self) -> NodeVersion {
        NodeVersion {
            ostro_version: env!("CARGO_PKG_VERSION").to_owned(),
            level_min: ostro_types::compat::LEVEL_MIN,
            level_max: self.level_max,
            wasmtime: ostro_engine::cache::WASMTIME_VERSION.to_owned(),
            event_codec_ver: u32::from(ostro_types::journal::EVENT_CODEC_VER),
            wit_lines: ostro_engine::host::linker_set::LinkerSet::default()
                .lines()
                .iter()
                .map(ToString::to_string)
                .collect(),
            reported_at_ms: self.clock.now_ms(),
        }
    }
}

/// [`HandoffControl`] over the node, held weakly (the cluster listener's NodeOps service
/// outlives the node).
struct NodeHandoff(Weak<Node>);

impl HandoffControl for NodeHandoff {
    fn prepare_handoff(
        &self,
        p: PartitionId,
        grace: Duration,
    ) -> BoxFuture<'_, Result<HandoffGuard, PartitionError>> {
        let node = self.0.upgrade();
        Box::pin(async move {
            let node = node.ok_or_else(|| PartitionError::Failed("node shutting down".into()))?;
            node.prepare_handoff(p, grace).await
        })
    }

    fn in_handoff(&self, p: PartitionId) -> bool {
        self.0.upgrade().is_some_and(|n| n.in_handoff(p))
    }
}

/// Wrap the local control plane (D-P13-14): every API, UI and in-process consumer gets the
/// result. Stub (until 13-12): the local control plane itself.
pub fn wrap_control_plane(
    ctx: &Arc<P13Ctx>,
    local: Arc<ServerControlPlane>,
) -> Arc<dyn ControlPlane> {
    crate::route::wrap_control_plane(ctx, local)
}

/// Fill the Phase 13 API slots and route every slot (after the Phase 7/8/11 slots are set).
pub fn fill_extensions(ctx: &Arc<P13Ctx>, ext: &mut ApiExtensions) {
    ext.cluster = Some(Arc::new(cluster_api::P13ClusterApi::new(Arc::clone(ctx))));
    ext.module_lifecycle = Some(Arc::new(module_lifecycle::P13ModuleLifecycle::new(
        Arc::clone(ctx),
    )));
    crate::route::wrap_extensions(ctx, ext);
}

/// The Phase 13 services of the cluster listener (node identities only): Membership,
/// NodeOps and the forwarded `ostro.api.v1.*` API.
pub fn cluster_listener_services(ctx: &Arc<P13Ctx>) -> Vec<Routes> {
    let mut routes = Vec::new();
    routes.extend(membership::service(ctx));
    routes.extend(
        node_ops::NodeOpsService::new(
            Arc::clone(&ctx.handoff) as Arc<dyn ostro_cluster::contracts::HandoffExecutor>,
            Arc::clone(&ctx.module_refs) as Arc<dyn ostro_cluster::contracts::ModuleRefsSource>,
        )
        .routes(),
    );
    routes.extend(crate::route::node_api_services(ctx));
    routes
}

/// The running Phase 13 background tasks ([`start`]); stopped by the server before the
/// node shuts down.
#[derive(Debug, Default)]
pub struct P13Services {
    stop: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl P13Services {
    /// Cancel and join every task.
    pub async fn stop(self) {
        self.stop.cancel();
        for t in self.tasks {
            if let Err(e) = t.await
                && e.is_panic()
            {
                warn!(error = %e, "a Phase 13 task panicked");
            }
        }
    }
}

/// Start the Phase 13 background tasks (every one runs on every node and acts only where
/// its rule says, e.g. as meta leader).
pub async fn start(ctx: Arc<P13Ctx>) -> P13Services {
    let stop = CancellationToken::new();
    let c = &ctx.cfg.cluster;
    let mut tasks = Vec::new();
    // 13-04: NodeVersion advertisement, auto-finalizer, running fence.
    // The one injectable clock (Phase 10 F5) reaches both.
    tasks.push(ostro_cluster::compat::start_advertiser_with(
        Arc::clone(&ctx.meta),
        ctx.node_id(),
        ctx.node_version(),
        Arc::clone(&ctx.clock),
        ostro_cluster::compat::AdvertiserConfig::default(),
        stop.clone(),
    ));
    tasks.push(ostro_cluster::upgrade::start_finalizer_with(
        Arc::clone(&ctx.meta),
        ostro_cluster::upgrade::FinalizerConfig {
            auto_finalize: c.auto_finalize,
            ..ostro_cluster::upgrade::FinalizerConfig::default()
        },
        Arc::clone(&ctx.clock),
        stop.clone(),
    ));
    let fence_ctx = Arc::downgrade(&ctx);
    tasks.push(ostro_cluster::compat::start_running_fence(
        Arc::clone(&ctx.meta),
        ctx.level_max,
        Arc::new(move |msg: String| {
            if let Some(ctx) = fence_ctx.upgrade() {
                ctx.fatal(msg);
            }
        }),
        stop.clone(),
    ));
    // 13-09: membership loop (+ the Removed handler).
    tasks.extend(membership::start(&ctx, stop.clone()));
    // 13-10: placement controller and preferred-leader balancer.
    tasks.push(
        PlacementController::new(
            Arc::clone(&ctx.meta),
            Arc::clone(&ctx.handoff_client),
            PlacementConfig {
                max_concurrent_moves: usize::try_from(c.max_concurrent_moves).unwrap_or(1),
                leader_rebalance_interval: c.leader_rebalance_interval.get(),
                level_max: ctx.level_max,
                ..PlacementConfig::default()
            },
        )
        .with_trigger(ctx.placement_trigger.clone())
        .start(stop.clone()),
    );
    // 13-15: module GC (meta leader).
    tasks.extend(module_gc::start(&ctx, stop.clone()));
    // 13-14: AOT pre-warm.
    tasks.extend(prewarm::start(&ctx, stop.clone()));
    info!(
        node_id = ctx.node_id(),
        level_max = ctx.level_max,
        tasks = tasks.len(),
        "Phase 13 services started"
    );
    P13Services { stop, tasks }
}

/// First in the shutdown path: hand off every led partition and transfer meta leadership,
/// bounded by `[cluster] shutdown_handoff_timeout` (13-11). Stub: hands off nothing.
pub async fn before_shutdown(ctx: &P13Ctx) {
    let report = ostro_cluster::shutdown::controlled_shutdown(
        ctx.node_id(),
        Arc::clone(&ctx.meta),
        Arc::clone(&ctx.handoff_client),
        ctx.cfg.cluster.shutdown_handoff_timeout.get(),
    )
    .await;
    if !report.moved.is_empty() || !report.kept.is_empty() || report.meta_transferred {
        info!(
            moved = ?report.moved,
            kept = ?report.kept,
            meta_transferred = report.meta_transferred,
            "controlled shutdown"
        );
    }
}
