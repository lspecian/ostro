//! The real [`ControlPlane`] (plans 04-17 / 04-18): every REST and gRPC adapter in
//! `ostro-api` calls into [`ServerControlPlane`], which reads and writes the metadata
//! state machine, the partitions and the engine runtime.
//!
//! - [`modules`]: module upload (validate + AOT + register workflows), module lookup,
//!   workflow listing and activation (API-02).
//! - [`runs`]: start (API-03) and get (API-04 / WF-01) (04-18).
//! - [`history`]: a run's decoded journal (API-04) (04-18).
//! - [`list`]: filtered, paginated run listing across partitions (API-04) (04-18).
//! - [`names`]: the D23 tenant-id and resource-name grammar and tenant qualification
//!   (05-24).
//! - [`policy`]: workflow / queue run policies in meta (D9, 05-13).
//! - [`lifecycle`]: signal, cancel, terminate, retry and await-token completion, each with
//!   a no-effect `dry_run` (API-05/06/07, 05-13).
//! - [`watch`]: the long-poll [`RunWatchers`] hub and `await_result` (05-13).
//! - [`bridge`]: Phase 6 bridge administration: sealed secrets, grants, connections,
//!   egress policies and the local presign route (06-16).
//! - [`p7_flow`] / [`p7_pause`] (07-14): the Phase 7 control plane: queue flow (meta
//!   `QueuePolicy` F2 fields), queue / workflow / run pause and resume, and the paused-run
//!   `/retry` alias.
//! - [`p7_workers`] (07-14): worker list / drain, join tokens, join and the demand
//!   endpoint over `P7Shared` (registry, join service, demand snapshot).
//!
//! Phase 8 (08-10): the controllers behind `ostro_api::phase8`, each holding the one
//! [`ControlCtx`] built in `server.rs` and installed through [`phase8_extensions`]:
//!
//! | File | Traits | Owner | Now |
//! |---|---|---|---|
//! | [`schedules`] | `ScheduleApi` | 08-14 | done |
//! | [`webhooks`] | `WebhookApi`, `HookIngress` | 08-15 | done |
//! | [`run_logs`] | `RunLogsApi` | 08-17 | done |
//! | [`admin`] | `AdminApi` | 08-18 | done |
//!
//! A stub method answers `Unavailable` (HTTP 503, gRPC `UNAVAILABLE`) with the message
//! `not yet implemented (08-NN)`. Feature plans fill only their controller file;
//! `server.rs` and this wiring stay untouched.
//!
//! Phase 11 (11-04, D-P11-23): free functions over [`crate::p11::P11Ctx`], called by
//! [`crate::p11::P11Controllers`] (`ostro_api::phase11::{OpsApi, InventoryApi}`):
//!
//! | File | Operations | Owner |
//! |---|---|---|
//! | [`p11_search`] | indexed `list_runs` (tried first by [`ServerControlPlane`]) | 11-11 |
//! | [`p11_inventory`] | limits, modules, queues, partitions | 11-11 |
//! | [`p11_rerun`] | rerun, rerun cuts | 11-10 |
//! | [`p11_dlq`] | DLQ list / requeue / dismiss, failure policy, `is_dlq_member` | 11-12 |
//! | [`p11_bulk`] | bulk jobs | 11-13 |

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use ostro_api::domain::{
    ActivateWorkflow, AwaitResultRequest, AwaitResultResponse, CancelRequest, CancelResponse,
    CompleteTokenRequest, CompleteTokenResponse, ConnectionInfo, ControlPlane, EgressInfo,
    GrantInfo, HistoryPage, HistoryRequest, ListRunsRequest, ModuleInfo, PresignedObjectRequest,
    PresignedObjectResponse, PutSecret, RetryRequest, RetryResponse, RunInfo, RunPage, RunPolicy,
    SecretInfo, SignalRequest, SignalResponse, StartRunRequest, StartRunResult, TerminateRequest,
    TerminateResponse, UploadModule, UploadModuleResult, WorkflowInfo,
};
use ostro_api::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, DrainResult, JoinRequest, JoinResponse, PauseState,
    QueueFlow, QueueFlowInfo, RunPauseState, WorkerInfo as P7WorkerInfo, WorkerTokenCreated,
    WorkerTokenInfo,
};
use ostro_api::{ApiError, ApiExtensions};
use ostro_blob::BlobStore;
use ostro_engine::runtime::Runtime;
use ostro_log::Rf1Log;
use ostro_meta::records::SealedSecret;
use ostro_meta::{MetaHandle, MetaKey, MetaRecord, MetaView};
use ostro_partition::{Clock, Node, PartitionHandle, PartitionHealth};
use ostro_types::{PartitionCount, PartitionId};

use crate::bridge_services::BridgeServices;
use crate::config::ServerConfig;
use crate::p7::P7Shared;
use crate::p11::P11Ctx;
use crate::services::guest_logs::GuestLogSink;
pub use watch::RunWatchers;

pub mod admin;
pub mod bridge;
pub mod history;
pub mod lifecycle;
pub mod list;
pub mod modules;
pub mod names;
pub mod p11_bulk;
pub mod p11_dlq;
pub mod p11_inventory;
pub mod p11_rerun;
pub mod p11_search;
pub mod p7_flow;
pub mod p7_pause;
pub mod p7_workers;
pub mod policy;
pub mod run_logs;
pub mod runs;
pub mod schedules;
pub mod watch;
pub mod webhooks;

/// Shared state behind every API call.
pub struct ServerControlPlane {
    pub(crate) meta: Arc<MetaHandle>,
    /// One entry per partition: `Some` when the slot started (`PartitionSlot::Ready`).
    pub(crate) partitions: Vec<Option<PartitionHandle<Rf1Log>>>,
    pub(crate) count: PartitionCount,
    /// The node-wide blob store (shared by every partition); `None` if no partition started.
    pub(crate) blobs: Option<Arc<BlobStore>>,
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) cfg: Arc<ServerConfig>,
    /// Cleared when shutdown begins (`/readyz` turns 503 before the listener closes).
    pub(crate) serving: AtomicBool,
    /// Journal rows GET run read for the published status (F10 test probe).
    pub(crate) status_rows_read: Arc<AtomicU64>,
    /// Long-poll waiters (05-13); fed by every started partition's action broadcast.
    pub(crate) watchers: RunWatchers,
    /// The effects bridge (06-14): sealing key, secret resolver, object connector. 06-16
    /// implements the Phase 6 admin methods over it.
    pub(crate) bridge: Arc<BridgeServices>,
    /// Phase 7 shared state (07-02 seam, B3): worker registry, flow control, gauges, TLS,
    /// join. 07-14 implements the Phase 7 control-plane methods over it.
    pub(crate) p7: Arc<P7Shared>,
    /// The Phase 11 context (W7), set once by `p11::wire`. Weak: `P11Ctx::cc` holds this
    /// control plane, so a strong reference would be a cycle. Read it through
    /// [`ServerControlPlane::p11`].
    pub(crate) p11: Arc<OnceLock<Weak<P11Ctx>>>,
}

impl std::fmt::Debug for ServerControlPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerControlPlane")
            .field("partitions", &self.count.get())
            .field("serving", &self.serving.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ServerControlPlane {
    pub(crate) fn new(
        meta: Arc<MetaHandle>,
        partitions: Vec<Option<PartitionHandle<Rf1Log>>>,
        count: PartitionCount,
        runtime: Arc<Runtime>,
        cfg: Arc<ServerConfig>,
        bridge: Arc<BridgeServices>,
        p7: Arc<P7Shared>,
    ) -> Self {
        let blobs = partitions
            .iter()
            .flatten()
            .next()
            .map(|h| Arc::clone(h.blobs()));
        let watchers = RunWatchers::new(usize::from(count.get()));
        for h in partitions.iter().flatten() {
            watchers.watch_partition(h.partition(), h.machine().subscribe_actions());
        }
        Self {
            meta,
            partitions,
            count,
            blobs,
            runtime,
            cfg,
            serving: AtomicBool::new(true),
            status_rows_read: Arc::new(AtomicU64::new(0)),
            watchers,
            bridge,
            p7,
            p11: Arc::new(OnceLock::new()),
        }
    }

    /// The Phase 11 context: `None` before `p11::wire` ran and after the API stopped.
    pub fn p11(&self) -> Option<Arc<P11Ctx>> {
        self.p11.get().and_then(Weak::upgrade)
    }

    /// The effects bridge (06-14).
    pub fn bridge(&self) -> &Arc<BridgeServices> {
        &self.bridge
    }

    /// The long-poll waiter hub.
    pub fn watchers(&self) -> &RunWatchers {
        &self.watchers
    }

    /// Journal rows GET run has read to find published statuses (F10: one per GET of a run
    /// that published, none otherwise). Test probe.
    #[doc(hidden)]
    pub fn status_rows_read(&self) -> u64 {
        self.status_rows_read.load(Ordering::Relaxed)
    }

    /// The metadata handle.
    pub fn meta(&self) -> &MetaHandle {
        &self.meta
    }

    /// The engine runtime.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// The effective server configuration.
    pub fn config(&self) -> &ServerConfig {
        &self.cfg
    }

    /// The fixed partition count.
    pub fn partition_count(&self) -> PartitionCount {
        self.count
    }

    /// Partition `p`'s handle, or `Unavailable` when its slot failed to start.
    pub fn partition(&self, p: PartitionId) -> Result<&PartitionHandle<Rf1Log>, ApiError> {
        self.partitions
            .get(p.index())
            .and_then(Option::as_ref)
            .ok_or_else(|| ApiError::unavailable(format!("partition {} is not available", p.0)))
    }

    /// The node-wide blob store.
    pub fn blobs(&self) -> Result<&Arc<BlobStore>, ApiError> {
        self.blobs
            .as_ref()
            .ok_or_else(|| ApiError::unavailable("no partition is available (blob store)"))
    }

    /// Wall-clock ms since the Unix epoch (proposers supply every metadata timestamp).
    pub fn now_ms(&self) -> u64 {
        self.runtime.clock().now_unix_nanos() / 1_000_000
    }

    pub(crate) fn stop_serving(&self) {
        self.serving.store(false, Ordering::SeqCst);
    }

    /// Stop the waiter forwarding tasks (after the node shut down).
    pub(crate) fn stop_watchers(&self) {
        self.watchers.stop();
    }
}

#[async_trait::async_trait]
impl ControlPlane for ServerControlPlane {
    async fn upload_module(&self, req: UploadModule) -> Result<UploadModuleResult, ApiError> {
        modules::upload_module(self, req).await
    }

    async fn get_module(&self, sha256: String) -> Result<ModuleInfo, ApiError> {
        modules::get_module(self, &sha256)
    }

    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        Ok(modules::list_workflows(self))
    }

    async fn activate_workflow(&self, req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        modules::activate_workflow(self, req).await
    }

    async fn start_run(&self, req: StartRunRequest) -> Result<StartRunResult, ApiError> {
        runs::start_run(self, req).await
    }

    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError> {
        runs::get_run(self, run_id).await
    }

    async fn run_history(&self, req: HistoryRequest) -> Result<HistoryPage, ApiError> {
        history::run_history(self, req).await
    }

    async fn list_runs(&self, req: ListRunsRequest) -> Result<RunPage, ApiError> {
        if let Some(ctx) = self.p11()
            && let Some(page) = p11_search::list_runs_indexed(&ctx, &req).await
        {
            return page;
        }
        list::list_runs(self, req).await
    }

    async fn signal_run(&self, r: SignalRequest) -> Result<SignalResponse, ApiError> {
        lifecycle::signal_run(self, r).await
    }

    async fn cancel_run(&self, r: CancelRequest) -> Result<CancelResponse, ApiError> {
        lifecycle::cancel_run(self, r).await
    }

    async fn terminate_run(&self, r: TerminateRequest) -> Result<TerminateResponse, ApiError> {
        lifecycle::terminate_run(self, r).await
    }

    async fn retry_run(&self, r: RetryRequest) -> Result<RetryResponse, ApiError> {
        // 07-14: `/retry` on a paused, non-terminal run resumes it (see [`p7_pause`]).
        if let Some(resumed) = p7_pause::retry_alias(self, &r).await? {
            return Ok(resumed);
        }
        lifecycle::retry_run(self, r).await
    }

    async fn await_result(&self, r: AwaitResultRequest) -> Result<AwaitResultResponse, ApiError> {
        watch::await_result(self, r).await
    }

    async fn complete_token(
        &self,
        r: CompleteTokenRequest,
    ) -> Result<CompleteTokenResponse, ApiError> {
        lifecycle::complete_token(self, r).await
    }

    async fn set_workflow_policy(
        &self,
        workflow: String,
        p: RunPolicy,
    ) -> Result<RunPolicy, ApiError> {
        policy::set_workflow_policy(self, workflow, p).await
    }

    async fn get_workflow_policy(&self, workflow: String) -> Result<RunPolicy, ApiError> {
        policy::get_workflow_policy(self, workflow).await
    }

    async fn set_queue_policy(&self, queue: String, p: RunPolicy) -> Result<RunPolicy, ApiError> {
        policy::set_queue_policy(self, queue, p).await
    }

    async fn get_queue_policy(&self, queue: String) -> Result<RunPolicy, ApiError> {
        policy::get_queue_policy(self, queue).await
    }

    // --- Phase 6 bridge administration (06-16): see [`bridge`] ---

    async fn put_secret(&self, req: PutSecret) -> Result<SecretInfo, ApiError> {
        bridge::put_secret(self, req).await
    }

    async fn list_secrets(&self, tenant: Option<String>) -> Result<Vec<SecretInfo>, ApiError> {
        bridge::list_secrets(self, tenant).await
    }

    async fn delete_secret(&self, name: String, tenant: Option<String>) -> Result<(), ApiError> {
        bridge::delete_secret(self, name, tenant).await
    }

    async fn put_grant(&self, g: GrantInfo) -> Result<GrantInfo, ApiError> {
        bridge::put_grant(self, g).await
    }

    async fn list_grants(&self, module: Option<String>) -> Result<Vec<GrantInfo>, ApiError> {
        bridge::list_grants(self, module).await
    }

    async fn delete_grant(&self, id: String) -> Result<(), ApiError> {
        bridge::delete_grant(self, id).await
    }

    async fn put_connection(&self, c: ConnectionInfo) -> Result<ConnectionInfo, ApiError> {
        bridge::put_connection(self, c).await
    }

    async fn get_connection(&self, name: String) -> Result<ConnectionInfo, ApiError> {
        bridge::get_connection(self, name).await
    }

    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>, ApiError> {
        bridge::list_connections(self).await
    }

    async fn delete_connection(&self, name: String) -> Result<(), ApiError> {
        bridge::delete_connection(self, name).await
    }

    async fn put_egress(&self, e: EgressInfo) -> Result<EgressInfo, ApiError> {
        bridge::put_egress(self, e).await
    }

    async fn get_egress(&self, module: String) -> Result<EgressInfo, ApiError> {
        bridge::get_egress(self, module).await
    }

    async fn list_egress(&self) -> Result<Vec<EgressInfo>, ApiError> {
        bridge::list_egress(self).await
    }

    async fn delete_egress(&self, module: String) -> Result<(), ApiError> {
        bridge::delete_egress(self, module).await
    }

    async fn presigned_object(
        &self,
        req: PresignedObjectRequest,
    ) -> Result<PresignedObjectResponse, ApiError> {
        bridge::presigned_object(self, req).await
    }

    // --- Phase 7 control plane (07-14): see [`p7_flow`], [`p7_pause`] ---

    async fn get_queue_flow(&self, queue: String) -> Result<QueueFlowInfo, ApiError> {
        p7_flow::get_queue_flow(self, queue).await
    }

    async fn put_queue_flow(
        &self,
        queue: String,
        flow: QueueFlow,
        expected_version: Option<u64>,
    ) -> Result<QueueFlowInfo, ApiError> {
        p7_flow::put_queue_flow(self, queue, flow, expected_version).await
    }

    async fn pause_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        p7_flow::pause_queue(self, queue).await
    }

    async fn resume_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        p7_flow::resume_queue(self, queue).await
    }

    async fn pause_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        p7_flow::pause_workflow(self, workflow).await
    }

    async fn resume_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        p7_flow::resume_workflow(self, workflow).await
    }

    async fn pause_run(
        &self,
        run_id: String,
        detail: Option<String>,
    ) -> Result<RunPauseState, ApiError> {
        p7_pause::pause_run(self, run_id, detail).await
    }

    async fn resume_run(&self, run_id: String) -> Result<RunPauseState, ApiError> {
        p7_pause::resume_run(self, run_id).await
    }

    // --- Phase 7 workers (07-14): see [`p7_workers`] ---

    async fn queue_demand(&self, queue: String) -> Result<DemandSnapshot, ApiError> {
        p7_workers::queue_demand(self, queue).await
    }

    async fn list_workers(&self) -> Result<Vec<P7WorkerInfo>, ApiError> {
        p7_workers::list_workers(self).await
    }

    async fn drain_worker(&self, worker_id: String) -> Result<DrainResult, ApiError> {
        p7_workers::drain_worker(self, worker_id).await
    }

    async fn create_worker_token(
        &self,
        req: CreateWorkerTokenRequest,
    ) -> Result<WorkerTokenCreated, ApiError> {
        p7_workers::create_worker_token(self, req).await
    }

    async fn list_worker_tokens(&self) -> Result<Vec<WorkerTokenInfo>, ApiError> {
        p7_workers::list_worker_tokens(self).await
    }

    async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError> {
        p7_workers::revoke_worker_token(self, token_id).await
    }

    async fn join_worker(&self, req: JoinRequest) -> Result<JoinResponse, ApiError> {
        p7_workers::join_worker(self, req).await
    }

    /// Serving, meta has a leader, and every partition slot is `Ready` and not failed.
    fn ready(&self) -> bool {
        self.serving.load(Ordering::SeqCst)
            && self.meta.is_leader()
            && self.partitions.iter().all(|slot| {
                slot.as_ref().is_some_and(|h| {
                    !matches!(
                        *h.machine().health().borrow(),
                        PartitionHealth::Failed { .. }
                    )
                })
            })
    }
}

// ---------------------------------------------------------------------------------
// Phase 8 controllers (08-10)
// ---------------------------------------------------------------------------------

/// Read access to sealed meta secrets (webhook verifier secrets, 08-15).
///
/// Returns the sealed record only; unsealing goes through the Phase 6 secrets resolver
/// when the server wires one. Secret material never leaves the server through the API.
#[derive(Clone)]
pub struct SecretsAccess {
    meta: MetaView,
}

impl std::fmt::Debug for SecretsAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SecretsAccess").finish_non_exhaustive()
    }
}

impl SecretsAccess {
    /// Over the meta view.
    pub fn new(meta: MetaView) -> Self {
        Self { meta }
    }

    /// The sealed secret stored under `name` (the full meta key, tenant-qualified per
    /// D23), if any.
    pub fn sealed(&self, name: &str) -> Option<SealedSecret> {
        match self
            .meta
            .read(|s| s.get(&MetaKey::Secret(name.to_owned())))?
            .value
        {
            MetaRecord::Secret(sec) => Some(sec),
            _ => None,
        }
    }
}

/// Everything a Phase 8 controller can reach, built once in `server.rs` (08-10) so the
/// feature plans (08-14, 08-15, 08-17, 08-18) never edit it.
#[derive(Clone)]
pub struct ControlCtx {
    /// The node. Held weakly: the server handle owns the last strong reference, so
    /// `Node::shutdown` can take it back even if a connection outlives the API drain.
    /// Use [`ControlCtx::node`].
    node: Weak<Node>,
    /// Metadata (schedules, webhooks, secrets: CAS proposals and reads).
    pub meta: Arc<MetaHandle>,
    /// The effective configuration (`[triggers]`, `[guest_logs]`, `[backup]`, ...).
    pub config: Arc<ServerConfig>,
    /// The node-wide blob store; `None` when no partition started.
    pub blobs: Option<Arc<BlobStore>>,
    /// The guest log sink: the very instance the executor writes and
    /// `Services::guest_log_sink()` returns (08-17 reads its ring buffers).
    pub guest_logs: Arc<GuestLogSink>,
    /// The partition client: start / signal / complete-token through the same code path
    /// as the API (submit, wait for apply), plus partition handles and the run watchers.
    pub partitions: Arc<ServerControlPlane>,
    /// Sealed meta secrets.
    pub secrets: SecretsAccess,
    /// The one injectable clock (Phase 10 F5): the same instance as `NodeConfig.clock`
    /// and `ServiceCtx.clock`. Every time read that feeds logic goes through it.
    pub clock: Arc<dyn Clock>,
    /// The data directory.
    pub data_dir: PathBuf,
}

impl std::fmt::Debug for ControlCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlCtx")
            .field("data_dir", &self.data_dir)
            .field("blobs", &self.blobs.is_some())
            .finish_non_exhaustive()
    }
}

impl ControlCtx {
    /// Assemble the context (server.rs).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node: &Arc<Node>,
        meta: Arc<MetaHandle>,
        config: Arc<ServerConfig>,
        blobs: Option<Arc<BlobStore>>,
        guest_logs: Arc<GuestLogSink>,
        partitions: Arc<ServerControlPlane>,
        clock: Arc<dyn Clock>,
        data_dir: PathBuf,
    ) -> Self {
        let secrets = SecretsAccess::new(meta.view().clone());
        Self {
            node: Arc::downgrade(node),
            meta,
            config,
            blobs,
            guest_logs,
            partitions,
            secrets,
            clock,
            data_dir,
        }
    }

    /// The node, or `Unavailable` once shutdown has released it.
    pub fn node(&self) -> Result<Arc<Node>, ApiError> {
        self.node
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the node is shutting down"))
    }

    /// `clock.now_ms()`.
    pub fn now_ms(&self) -> u64 {
        self.clock.now_ms()
    }
}

/// The Phase 8 API slots, every one filled with this server's controller, and the hook
/// body limit from `[triggers] webhook_body_limit`.
pub fn phase8_extensions(ctx: &ControlCtx) -> ApiExtensions {
    let webhooks = Arc::new(webhooks::WebhookController::new(ctx.clone()));
    ApiExtensions {
        schedules: Some(Arc::new(schedules::ScheduleController::new(ctx.clone()))),
        webhooks: Some(webhooks.clone()),
        hooks: Some(webhooks),
        run_logs: Some(Arc::new(run_logs::RunLogsController::new(ctx.clone()))),
        admin: Some(Arc::new(admin::AdminController::new(ctx.clone()))),
        hook_body_limit: usize::try_from(ctx.config.triggers.webhook_body_limit)
            .unwrap_or(usize::MAX),
        ..Default::default()
    }
}
