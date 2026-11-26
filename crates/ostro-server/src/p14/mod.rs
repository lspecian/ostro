//! The single Phase 14 seam (D-P14-33, plan 14-01b). **Final after 14-01b**: later Phase 14
//! plans implement only the hook file each call below names; `server.rs`, `config.rs`,
//! `crates/ostro/src/main.rs`, this file and the call sites stay untouched (a missing hook is
//! a STOP for the later plan).
//!
//! # Call sites (fixed)
//!
//! | Where | Call | Hook (owner) |
//! |-------|------|--------------|
//! | `server.rs`, the clock construction | [`wrap_clock`] | `test_hooks::wrap_clock` (14-10, failpoints builds) |
//! | `server.rs`, right after cluster start (meta + clock exist; before the bridge) | [`build_ctx`] | `audit::writer::open` (14-04), `quota_view::TenantQuotaView::new` (14-06) |
//! | `server.rs` → `BridgeServices::build` | [`audit_sink`] wrapped around the bridge's resolver (`Audited`) | 14-18 (`ostro-bridge/src/secrets/audited.rs`) |
//! | `server.rs`, after `p7::init` | `JoinService::attach_p14` → [`worker_event`] from `workers/join.rs` (join, renew, token revoke) | `audit_hooks::worker_event` (14-18) |
//! | `server.rs`, after `Node::open` | [`P14Ctx::attach_node`] (blob store, partition roles) | — |
//! | `server.rs`, the `ServerControlPlane` | [`attach`] (control-plane hooks below reach the context through it) | — |
//! | `server.rs`, `ApiConfig` | [`authenticator`] | `authn::authenticator` (14-03) |
//! | `server.rs` → `p11::wire`, before the Phase 13 routing of the slots | [`fill_extensions`] | `tenants_api`/`tokens_api` (14-17), `audit_api` (14-18), `security_api` (14-19) |
//! | `server.rs` → `p11::wire`, the REST/gRPC/UI construction | [`wrap_ingress`] (REST, gRPC and `/ui` get [`Guarded`]; in-process consumers keep the routed instances) | `guard::wrap` (14-03) |
//! | `server.rs` → `p11::wire`, the UI router | [`ui_auth`], [`ui_scope_layer`] | `ui_auth` (14-21) |
//! | `server.rs`, after `p13::start` | [`start`] | audit writer (14-04), bootstrap (14-03), reseal (14-19), quota view (14-06) |
//! | `server.rs`, after the API is served (the routed, unguarded instance and the node's roles, like the Phase 8 cron/webhook drivers) | [`start_triggers`] | `kafka::start` (14-32) |
//! | `server.rs`, first in `ServerHandle::shutdown` (before `p13::before_shutdown`) | [`before_shutdown`] | audit + coalescer flush (14-04 / 14-18) |
//! | `control/modules.rs::upload_module`, right after the sha check, before the existing-module lookup, compilation, every meta write and the auto-activation | [`upload_gate`] (+ [`evidence_ext`] / [`merge_evidence`]) | `module_sign` (14-19) |
//! | `control/modules.rs::upload_module`, the existing-sha branch | [`register_existing`] | `upload_tenant` (14-16) |
//! | `control/modules.rs::activate_workflow` | [`activation_gate`] | `module_sign` (14-19) |
//! | `control/runs.rs::start_run` (explicit version), `control/migrate.rs::migrate_runs` (target), `control/p11_rerun.rs::rerun` (every target) | [`version_gate`] | `module_sign` (14-19) |
//! | `control/runs.rs::start_run` (after the dedupe lookup), `control/lifecycle.rs::{signal_run, complete_token}` | [`admit_ingress`] | `ingress` (14-20) |
//!
//! `retry_run` has no version override (14-01 inventory), so it needs no `version_gate`.
//!
//! Every hook is a stub that keeps the Phase 13 behaviour exactly until its plan lands: the
//! guard passes through, gates allow, the authenticator and the UI answer the dev principal,
//! the audit sink is [`NoopAuditSink`], loops idle, and a non-empty `[[triggers.kafka]]` list
//! fails startup ("Kafka triggers land in 14-32").
//!
//! # Ownership
//!
//! [`P14Ctx`] holds meta strongly; the server owns it (`ServerHandle`) and drops it with the
//! Phase 13 context, before meta stops. Everything else holds it weakly: the control plane
//! ([`attach`]), the worker join service, the UI scope layer. Extension slot implementations
//! (14-17/18/19) and the guard (14-03) must also hold it weakly (as `P11Ctx` does), because
//! the API routers outlive the shutdown sequence's meta stop only through such references.

pub mod audit_api;
pub mod audit_hooks;
pub mod authn;
pub mod authz;
pub mod bootstrap;
pub mod guard;
pub mod ingress;
pub mod kafka;
pub mod module_sign;
pub mod principal_fwd;
pub mod quota_view;
pub mod reseal;
pub mod security_api;
pub mod signing;
pub mod tenant_scope;
pub mod tenants_api;
pub mod test_hooks;
pub mod tokens;
pub mod tokens_api;
pub mod ui_auth;
pub mod upload_tenant;

use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};

use ostro_api::auth::Authenticator;
use ostro_api::{ApiError, ApiExtensions, ControlPlane};
use ostro_blob::BlobStore;
use ostro_meta::MetaHandle;
use ostro_meta::ext::ModuleSig;
use ostro_partition::{Clock, PartitionRoles};
use ostro_types::audit::{AuditOutcome, AuditSink};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::config::ServerConfig;
use crate::control::ServerControlPlane;

pub use ostro_types::audit::NoopAuditSink;
pub use ui_auth::UiScopeLayer;

/// The answer of a hook that is still a stub: `Unimplemented` (HTTP 501) naming its plan.
pub fn lands_in(method: &str, plan: &str) -> ApiError {
    ApiError::new(
        ostro_api::ErrorCode::Unimplemented,
        format!("{method}: Phase 14: implemented in {plan}"),
    )
    .with_details(serde_json::json!({ "method": method }))
}

/// A Phase 14 startup failure (configuration, bootstrap, audit journal, triggers); the
/// server maps it to `ServerError::Config`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct P14Error(pub String);

impl P14Error {
    /// A failure with this message.
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

// ---------------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------------

/// Everything a Phase 14 hook reaches, built once in `server.rs` right after meta and the
/// clock exist (before the bridge, the worker join service and the node).
pub struct P14Ctx {
    /// The effective configuration (`[auth]`, `[tenants]`, `[audit]`, `[security]`,
    /// `[triggers] kafka`).
    pub cfg: Arc<ServerConfig>,
    /// Metadata.
    pub meta: Arc<MetaHandle>,
    /// The one injectable clock (after [`wrap_clock`]).
    pub clock: Arc<dyn Clock>,
    /// This node's id.
    pub node_id: u64,
    /// A random id of this process start (audit segment names, 14-04).
    pub incarnation: u64,
    /// The data directory (`<data>/audit`, `<data>/admin.token`).
    pub data_dir: PathBuf,
    /// The audit sink ([`audit_sink`]); `audit::writer::open`, a [`NoopAuditSink`] until
    /// 14-04.
    pub audit: Arc<dyn AuditSink>,
    /// Tenant quotas as this node applies them (14-06).
    pub quotas: Arc<quota_view::TenantQuotaView>,
    /// This node's feature level ceiling.
    pub level_max: u32,
    /// Cancelled when the Phase 14 services stop ([`P14Services::stop`]); long-running hook
    /// tasks (the Kafka driver, 14-32) watch it.
    pub shutdown: CancellationToken,
    blobs: OnceLock<Arc<BlobStore>>,
    roles: OnceLock<Arc<dyn PartitionRoles>>,
}

impl std::fmt::Debug for P14Ctx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P14Ctx")
            .field("node_id", &self.node_id)
            .field("incarnation", &self.incarnation)
            .field("auth_mode", &self.cfg.auth.mode)
            .field("level_max", &self.level_max)
            .finish_non_exhaustive()
    }
}

impl P14Ctx {
    /// Bind the node's blob store and partition roles (after `Node::open`). The first call
    /// wins.
    pub fn attach_node(&self, blobs: Option<Arc<BlobStore>>, roles: Arc<dyn PartitionRoles>) {
        if let Some(b) = blobs {
            let _ = self.blobs.set(b);
        }
        let _ = self.roles.set(roles);
    }

    /// The blob store (module bytes for signature re-verification), once the node is open.
    pub fn blobs(&self) -> Option<&Arc<BlobStore>> {
        self.blobs.get()
    }

    /// The node's partition roles (leadership share of quotas), once the node is open.
    pub fn roles(&self) -> Option<&Arc<dyn PartitionRoles>> {
        self.roles.get()
    }
}

/// [`build_ctx`] inputs.
pub struct P14Init {
    pub cfg: Arc<ServerConfig>,
    pub meta: Arc<MetaHandle>,
    pub clock: Arc<dyn Clock>,
    pub node_id: u64,
    pub data_dir: PathBuf,
    pub level_max: u32,
}

/// Build the context: opens the audit sink (`audit::writer::open`, 14-04) and the quota
/// view (14-06).
pub fn build_ctx(i: P14Init) -> Result<Arc<P14Ctx>, P14Error> {
    let mut b = [0u8; 8];
    getrandom::fill(&mut b)
        .map_err(|e| P14Error::new(format!("OS random source unavailable: {e}")))?;
    let incarnation = u64::from_le_bytes(b);
    let audit = crate::audit::writer::open(&i.cfg, &i.data_dir, i.node_id, incarnation)?;
    Ok(Arc::new(P14Ctx {
        quotas: Arc::new(quota_view::TenantQuotaView::new(&i.cfg)),
        cfg: i.cfg,
        meta: i.meta,
        clock: i.clock,
        node_id: i.node_id,
        incarnation,
        data_dir: i.data_dir,
        audit,
        level_max: i.level_max,
        shutdown: CancellationToken::new(),
        blobs: OnceLock::new(),
        roles: OnceLock::new(),
    }))
}

/// Make the context reachable from the control-plane hooks (`upload_gate`, `version_gate`,
/// `activation_gate`, `register_existing`, `admit_ingress`). The first call wins.
pub fn attach(cp: &ServerControlPlane, ctx: &Arc<P14Ctx>) {
    if cp.p14.set(Arc::downgrade(ctx)).is_err() {
        warn!("Phase 14 context was already attached to the control plane; keeping the first");
    }
}

/// The context of a control plane: `Ok(None)` when none was ever attached (a control plane
/// built outside `server.rs`: hooks are skipped, Phase 13 behaviour), `Err(Unavailable)`
/// once the server released it (shutting down: gates fail closed).
pub fn ctx_of(cp: &ServerControlPlane) -> Result<Option<Arc<P14Ctx>>, ApiError> {
    match cp.p14.get() {
        None => Ok(None),
        Some(w) => w
            .upgrade()
            .map(Some)
            .ok_or_else(|| ApiError::unavailable("the server is shutting down")),
    }
}

/// The weak control-plane slot ([`attach`]); `ServerControlPlane::new` creates it empty.
pub type CtxSlot = Arc<OnceLock<Weak<P14Ctx>>>;

// ---------------------------------------------------------------------------------
// Construction hooks (server.rs)
// ---------------------------------------------------------------------------------

/// What REST, gRPC and the UI receive: the guarded control plane and extension slots.
#[derive(Clone)]
pub struct Guarded {
    /// The guarded control plane.
    pub control: Arc<dyn ControlPlane>,
    /// The guarded extension slots.
    pub ext: ApiExtensions,
}

impl std::fmt::Debug for Guarded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guarded").finish_non_exhaustive()
    }
}

/// The bearer authenticator of the API (`ApiConfig.authenticator`). `[auth] mode = "none"`:
/// `AllowDev`. Stub (until 14-03): `AllowDev`.
pub fn authenticator(ctx: &Arc<P14Ctx>) -> Arc<dyn Authenticator> {
    authn::authenticator(ctx)
}

/// Wrap the routed control plane and slots for the client-facing routers (REST, gRPC, UI).
/// In-process consumers (webhooks, schedules, DLQ, bulk, on_failure hooks, the Kafka
/// trigger) keep the routed instances. Stub (until 14-03): passthrough clones.
pub fn wrap_ingress(
    ctx: &Arc<P14Ctx>,
    routed: Arc<dyn ControlPlane>,
    routed_ext: ApiExtensions,
) -> Guarded {
    guard::wrap(ctx, routed, routed_ext)
}

/// Fill the Phase 14 API slots (`tenants`, `api_tokens`, `audit`, `security`). Called before
/// the Phase 13 routing of every slot. Stub: every slot stays `None` (404).
pub fn fill_extensions(ctx: &Arc<P14Ctx>, ext: &mut ApiExtensions) {
    tenants_api::install(ctx, ext);
    tokens_api::install(ctx, ext);
    audit_api::install(ctx, ext);
    security_api::install(ctx, ext);
}

/// The UI's [`ostro_ui::auth::UiAuth`]. Stub (until 14-21): `AllowAll`.
pub fn ui_auth(ctx: &Arc<P14Ctx>) -> Arc<dyn ostro_ui::auth::UiAuth> {
    ui_auth::ui_auth(ctx)
}

/// The `/ui` scope layer (N2): `[auth] mode = "none"` runs every UI handler inside
/// `PRINCIPAL.scope(Principal::dev(), …)`; token mode resolves the principal through the
/// UI auth (stub until 14-21: the dev principal).
pub fn ui_scope_layer(ctx: &Arc<P14Ctx>, auth: Arc<dyn ostro_ui::auth::UiAuth>) -> UiScopeLayer {
    ui_auth::scope_layer(ctx, auth)
}

/// Wrap the one injectable clock (failpoints builds: the SkewClock, 14-10). Stub: identity.
pub fn wrap_clock(clock: Arc<dyn Clock>) -> Arc<dyn Clock> {
    test_hooks::wrap_clock(clock)
}

/// The running Phase 14 background tasks ([`start`]); stopped by the server after the API
/// drained, before the node shuts down.
#[derive(Debug, Default)]
pub struct P14Services {
    stop: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
}

impl P14Services {
    /// Add a task (the trigger driver from [`start_triggers`]).
    pub fn push(&mut self, task: JoinHandle<()>) {
        self.tasks.push(task);
    }

    /// Cancel ([`P14Ctx::shutdown`]) and join every task.
    pub async fn stop(self) {
        self.stop.cancel();
        for t in self.tasks {
            if let Err(e) = t.await
                && e.is_panic()
            {
                warn!(error = %e, "a Phase 14 task panicked");
            }
        }
    }
}

/// Start the Phase 14 background tasks: audit writer (14-04), admin bootstrap (14-03),
/// sealed-secret reseal (14-19), quota view (14-06).
pub async fn start(ctx: Arc<P14Ctx>) -> Result<P14Services, P14Error> {
    let stop = ctx.shutdown.clone();
    let mut tasks = Vec::new();
    tasks.extend(crate::audit::writer::start(&ctx, stop.clone()));
    bootstrap::ensure_admin(&ctx).await?;
    tasks.extend(reseal::start(&ctx, stop.clone()));
    tasks.extend(quota_view::start(&ctx, stop.clone()));
    info!(
        node_id = ctx.node_id,
        auth_mode = ctx.cfg.auth.mode.as_str(),
        tasks = tasks.len(),
        "Phase 14 services started"
    );
    Ok(P14Services { stop, tasks })
}

/// First in the shutdown path (before `p13::before_shutdown`): flush the audit writer and
/// the coalescers.
pub async fn before_shutdown(ctx: &P14Ctx) {
    crate::audit::writer::flush(ctx).await;
}

// ---------------------------------------------------------------------------------
// Triggers (revision 2026-11-26)
// ---------------------------------------------------------------------------------

/// How a start entered the cluster (D-P14-43; 14-20 charges Kafka like Webhook).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TriggerKind {
    /// A schedule (`__cron/` key: the manual trigger; cron fires bypass the API).
    Cron,
    /// A webhook delivery (`__hook/` key).
    Webhook,
    /// A Kafka record (`__kafka/` key, 14-32).
    Kafka,
}

/// Idempotency-key prefix of schedule starts (`ostro_partition` `CRON_KEY_PREFIX`).
pub const CRON_KEY_PREFIX: &str = "__cron/";
/// Idempotency-key prefix of Kafka-triggered starts (D-P14-43, RESERVATIONS §11).
pub const KAFKA_KEY_PREFIX: &str = "__kafka/";

impl TriggerKind {
    /// The trigger of a start from its idempotency key (`StartRunRequest` has no trigger
    /// field): `__kafka/` Kafka, `__hook/` Webhook, `__cron/` Cron, anything else `None`.
    pub fn of_key(idempotency_key: Option<&str>) -> Option<TriggerKind> {
        let k = idempotency_key?;
        if k.starts_with(KAFKA_KEY_PREFIX) {
            Some(TriggerKind::Kafka)
        } else if k.starts_with(crate::control::webhooks::HOOK_KEY_PREFIX) {
            Some(TriggerKind::Webhook)
        } else if k.starts_with(CRON_KEY_PREFIX) {
            Some(TriggerKind::Cron)
        } else {
            None
        }
    }
}

/// What [`admit_ingress`] admits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IngressKind {
    /// A run start; `dedupe_hit`: the idempotency key already names a run (not charged).
    Start {
        dedupe_hit: bool,
        trigger: Option<TriggerKind>,
    },
    /// A signal.
    Signal,
    /// An await-token completion.
    TokenComplete,
}

/// Start the Kafka topic-consumer triggers (`[[triggers.kafka]]`, 14-32) on the routed,
/// unguarded control plane (they start runs as System). `Ok(None)` when none are
/// configured. Stub: `Ok(None)` for an empty list, a startup error otherwise.
pub async fn start_triggers(
    ctx: &Arc<P14Ctx>,
    routed: Arc<dyn ControlPlane>,
    roles: Arc<dyn PartitionRoles>,
) -> Result<Option<JoinHandle<()>>, P14Error> {
    kafka::start(ctx, routed, roles).await
}

// ---------------------------------------------------------------------------------
// Control-plane hooks
// ---------------------------------------------------------------------------------

/// Admission of an ingress event for `tenant` (suspended tenants, event-rate budgets;
/// 14-20). Stub: admits.
pub async fn admit_ingress(
    ctx: &P14Ctx,
    kind: IngressKind,
    tenant: Option<&str>,
) -> Result<(), ApiError> {
    ingress::admit(ctx, kind, tenant).await
}

/// The verified signatures of an upload (14-19), written into the module record's ext 113.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct UploadEvidence {
    /// Valid signatures over the uploaded bytes.
    pub sigs: Vec<ModuleSig>,
}

impl UploadEvidence {
    /// No evidence.
    pub fn is_empty(&self) -> bool {
        self.sigs.is_empty()
    }
}

/// The upload gate (B1b), called right after the sha check and before the existing-module
/// lookup, compilation, every meta write and the auto-activation: verifies `sigs` (the
/// `Ostro-Module-Signature` carriage, 14-02) for `tenant` and enforces `require_signed`.
/// Stub: no evidence, allowed.
pub async fn upload_gate(
    ctx: &P14Ctx,
    tenant: Option<&str>,
    sha: [u8; 32],
    bytes: &[u8],
    sigs: &[Vec<u8>],
) -> Result<UploadEvidence, ApiError> {
    module_sign::upload_gate(ctx, tenant, sha, bytes, sigs).await
}

/// Write `evidence` into the ext of a new module record (ext 113). Stub: no-op.
pub fn evidence_ext(evidence: &UploadEvidence, ext: &mut Vec<u8>) {
    module_sign::evidence_ext(evidence, ext);
}

/// Merge `evidence` into an already stored module record (re-upload of an existing sha).
/// Stub: no-op.
pub async fn merge_evidence(
    ctx: &P14Ctx,
    sha: [u8; 32],
    evidence: UploadEvidence,
) -> Result<(), ApiError> {
    module_sign::merge_evidence(ctx, sha, evidence).await
}

/// The version gate (B1a): every explicit workflow version or module sha selection (start
/// with `version`, migrate target, rerun target). Accepts immediately when the target is
/// the active version or already carries valid evidence (W3). Stub: allowed.
pub async fn version_gate(
    ctx: &P14Ctx,
    tenant: Option<&str>,
    workflow: &str,
    version: u32,
    sha: [u8; 32],
) -> Result<(), ApiError> {
    module_sign::version_gate(ctx, tenant, workflow, version, sha).await
}

/// The activation gate (B1c) for the activating `tenant`. Stub: allowed.
pub async fn activation_gate(
    ctx: &P14Ctx,
    tenant: Option<&str>,
    sha: [u8; 32],
) -> Result<(), ApiError> {
    module_sign::activation_gate(ctx, tenant, sha).await
}

/// What an upload of an already stored sha may do for `tenant` (14-16).
#[derive(Clone, Debug, PartialEq)]
pub enum RegisterDecision {
    /// Register this tenant's versions from the stored module (the Phase 13 behaviour).
    Proceed,
    /// Refuse the upload with this error.
    Deny(ApiError),
}

/// The existing-sha branch of an upload (14-16: per-tenant module registration, ext 114).
/// Stub: [`RegisterDecision::Proceed`].
pub async fn register_existing(
    ctx: &P14Ctx,
    sha: [u8; 32],
    tenant: Option<&str>,
) -> RegisterDecision {
    upload_tenant::register_existing(ctx, sha, tenant).await
}

/// A worker identity event (D-P14-13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerAction {
    /// `POST /v1/workers/join` (certificate issued for a join token).
    Join,
    /// Certificate renewal over the worker protocol.
    Renew,
    /// A worker join token revoked.
    RevokeToken,
}

impl WorkerAction {
    /// Audit action name.
    pub fn as_str(self) -> &'static str {
        match self {
            WorkerAction::Join => "worker.join",
            WorkerAction::Renew => "worker.renew",
            WorkerAction::RevokeToken => "worker.token.revoke",
        }
    }
}

/// [`worker_event`] input.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerAuditEvent {
    /// What happened.
    pub action: WorkerAction,
    /// The join token id.
    pub token_id: String,
    /// The worker id (join, renew).
    pub worker_id: Option<String>,
    /// The outcome (failures are recorded too).
    pub outcome: AuditOutcome,
}

/// Record a worker join / renew / token revoke (14-18). Stub: no-op.
pub fn worker_event(ctx: &P14Ctx, ev: WorkerAuditEvent) {
    audit_hooks::worker_event(ctx, ev);
}

/// The audit sink (bridge secret-access capture, 14-18).
pub fn audit_sink(ctx: &P14Ctx) -> Arc<dyn AuditSink> {
    Arc::clone(&ctx.audit)
}
