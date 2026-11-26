//! Phase 14 authentication contract (14-01; D-P14-3/4/6, revision B2).
//!
//! - [`Scope`] / [`ScopeSet`]: the API scope vocabulary (`admin`, `deploy`, `operate`,
//!   `trigger`, `read`). No implication except that `admin` allows everything.
//! - [`Principal`]: who is calling. Requests run inside the [`PRINCIPAL`] task-local
//!   ([`scope`]); [`current`] outside any scope is **[`Principal::anonymous`]** (no scopes,
//!   denied everywhere). `System` is only ever set explicitly (webhook and join routes, the
//!   node API, internal callers of guarded instances).
//! - [`Authenticator`]: bearer → principal. The meta-backed implementation is the server's
//!   (`p14/authn.rs`, 14-03); [`AllowDev`] is the `[auth] mode = "none"` one.
//! - [`Method`]: the inventory of every control-plane and extension-trait method (14-01
//!   Task 1) with its scope (D-P14-3), tenant class (D-P14-7), audited flag (D-P14-13) and the
//!   wave in which it is reachable. The guard (14-03/14-15) and the route × scope matrix
//!   (14-15/14-24) read it.
//!
//! Forwarded calls (Phase 13 one hop) carry the principal in the `ostro-principal` header as
//! [`Principal::encode_forwarded`] (`p1;<token_id>;<tenant|*>;<scopes>`); it is trusted only
//! from node identities and used only for node-local filtering and logging, never for
//! authorization (D-P14-6).

use std::fmt;
use std::future::Future;

use crate::error::ApiError;

/// An API scope (D-P14-3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Scope {
    /// Everything (system admin with `tenant = None`; tenant admin otherwise).
    Admin,
    /// Upload, attach signatures, activate, workflow policies.
    Deploy,
    /// Cancel, terminate, retry, pause/resume, rerun, bulk, DLQ, queue policies, schedule
    /// pause/trigger.
    Operate,
    /// Start runs, signal, complete await tokens.
    Trigger,
    /// Reads (get/list/history/logs), the demand endpoint.
    Read,
}

impl Scope {
    /// Every scope, in declaration order.
    pub const ALL: [Scope; 5] = [
        Scope::Admin,
        Scope::Deploy,
        Scope::Operate,
        Scope::Trigger,
        Scope::Read,
    ];

    /// Wire name (`ApiToken.scopes` entries, CLI flags).
    pub const fn as_str(self) -> &'static str {
        match self {
            Scope::Admin => "admin",
            Scope::Deploy => "deploy",
            Scope::Operate => "operate",
            Scope::Trigger => "trigger",
            Scope::Read => "read",
        }
    }

    /// Parse a wire name. Non-API scopes (`worker:join`, `node:join`) are `None`.
    pub fn parse(s: &str) -> Option<Scope> {
        Scope::ALL.into_iter().find(|c| c.as_str() == s)
    }

    const fn bit(self) -> u8 {
        1 << self as u8
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A set of [`Scope`]s. [`ScopeSet::allows`] is exact membership, except that `Admin`
/// allows every scope.
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct ScopeSet(u8);

impl ScopeSet {
    /// No scopes.
    pub const EMPTY: ScopeSet = ScopeSet(0);

    /// The set holding exactly `scopes`.
    pub fn of(scopes: &[Scope]) -> Self {
        ScopeSet(scopes.iter().fold(0, |acc, s| acc | s.bit()))
    }

    /// `admin` only.
    pub fn admin() -> Self {
        ScopeSet::of(&[Scope::Admin])
    }

    /// Whether `scope` is in the set.
    pub fn contains(self, scope: Scope) -> bool {
        self.0 & scope.bit() != 0
    }

    /// Whether a method needing `needed` may run: `admin` allows everything, otherwise
    /// exact membership (no other implication).
    pub fn allows(self, needed: Scope) -> bool {
        self.contains(Scope::Admin) || self.contains(needed)
    }

    /// Whether the set is empty.
    pub fn is_empty(self) -> bool {
        self.0 == 0
    }

    /// The scopes, in declaration order.
    pub fn iter(self) -> impl Iterator<Item = Scope> {
        Scope::ALL.into_iter().filter(move |s| self.contains(*s))
    }

    /// Wire names in declaration order (what `ApiToken.scopes` stores).
    pub fn to_strings(self) -> Vec<String> {
        self.iter().map(|s| s.as_str().to_owned()).collect()
    }

    /// Parse stored scope names. Any non-API name (`worker:join`, `node:join`, typos)
    /// rejects the whole set: such tokens are never API principals (D-P14-3).
    pub fn parse<S: AsRef<str>>(names: &[S]) -> Result<ScopeSet, String> {
        let mut out = ScopeSet::EMPTY;
        for n in names {
            let n = n.as_ref();
            let s = Scope::parse(n).ok_or_else(|| format!("{n:?} is not an API scope"))?;
            out.0 |= s.bit();
        }
        Ok(out)
    }

    /// Comma-separated wire names (`admin,read`); empty set = `""`.
    pub fn to_csv(self) -> String {
        self.to_strings().join(",")
    }

    /// Inverse of [`ScopeSet::to_csv`].
    pub fn from_csv(s: &str) -> Result<ScopeSet, String> {
        if s.is_empty() {
            return Ok(ScopeSet::EMPTY);
        }
        ScopeSet::parse(&s.split(',').collect::<Vec<_>>())
    }
}

impl fmt::Debug for ScopeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.iter()).finish()
    }
}

/// What kind of caller a [`Principal`] is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    /// No credentials: the default outside any scope; denied everywhere (B2).
    Anonymous,
    /// An API token (`otk_…`).
    Token,
    /// The synthetic dev admin of `[auth] mode = "none"` (default tenant).
    Dev,
    /// The server itself (webhooks, join routes, internal callers of guarded instances).
    System,
    /// A cluster node (node API; forwarded calls).
    Node,
}

impl PrincipalKind {
    /// Stable name (logs, audit `actor_kind` mapping).
    pub const fn as_str(self) -> &'static str {
        match self {
            PrincipalKind::Anonymous => "anonymous",
            PrincipalKind::Token => "token",
            PrincipalKind::Dev => "dev",
            PrincipalKind::System => "system",
            PrincipalKind::Node => "node",
        }
    }
}

/// The caller of a request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Principal {
    /// Token id (`otk_…`) for `Token`; `anonymous`, `dev`, `system` or `node:<id>` otherwise.
    pub token_id: String,
    /// The principal's tenant; `None` = system scope (allowed only with `admin` or `read`).
    pub tenant: Option<String>,
    /// Granted scopes.
    pub scopes: ScopeSet,
    /// Kind of caller.
    pub kind: PrincipalKind,
}

/// Version tag of [`Principal::encode_forwarded`].
pub const FORWARDED_VERSION: &str = "p1";

impl Principal {
    /// No credentials, no scopes: what [`current`] returns outside any scope.
    pub fn anonymous() -> Self {
        Principal {
            token_id: "anonymous".into(),
            tenant: None,
            scopes: ScopeSet::EMPTY,
            kind: PrincipalKind::Anonymous,
        }
    }

    /// The `[auth] mode = "none"` principal: admin on the default tenant (D-P14-5).
    pub fn dev() -> Self {
        Principal {
            token_id: "dev".into(),
            tenant: None,
            scopes: ScopeSet::admin(),
            kind: PrincipalKind::Dev,
        }
    }

    /// The server itself: system admin. Set only explicitly (B2).
    pub fn system() -> Self {
        Principal {
            token_id: "system".into(),
            tenant: None,
            scopes: ScopeSet::admin(),
            kind: PrincipalKind::System,
        }
    }

    /// A token principal.
    pub fn token(token_id: impl Into<String>, tenant: Option<String>, scopes: ScopeSet) -> Self {
        Principal {
            token_id: token_id.into(),
            tenant,
            scopes,
            kind: PrincipalKind::Token,
        }
    }

    /// Whether this principal is anonymous (the guard answers 401 before any hook).
    pub fn is_anonymous(&self) -> bool {
        self.kind == PrincipalKind::Anonymous
    }

    /// System scope: `tenant = None` (sees every tenant with qualified names).
    pub fn is_system_scope(&self) -> bool {
        self.tenant.is_none()
    }

    /// `ostro-principal` header value: `p1;<token_id>;<tenant|*>;<scopes csv>`.
    pub fn encode_forwarded(&self) -> String {
        format!(
            "{FORWARDED_VERSION};{};{};{}",
            self.token_id,
            self.tenant.as_deref().unwrap_or("*"),
            self.scopes.to_csv()
        )
    }

    /// Inverse of [`Principal::encode_forwarded`]. The result is `Token` kind for `otk_` ids,
    /// `System` / `Dev` for those ids, `Node` otherwise; it is never used for authorization.
    pub fn decode_forwarded(s: &str) -> Result<Principal, String> {
        let mut parts = s.split(';');
        let (Some(v), Some(id), Some(tenant), Some(scopes), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err("expected p1;<token_id>;<tenant|*>;<scopes>".into());
        };
        if v != FORWARDED_VERSION {
            return Err(format!("unknown forwarded principal version {v:?}"));
        }
        if id.is_empty() || id.contains(|c: char| c.is_control() || c == ',') {
            return Err("invalid token id".into());
        }
        let tenant = match tenant {
            "*" => None,
            "" => return Err("empty tenant".into()),
            t => Some(t.to_owned()),
        };
        let scopes = ScopeSet::from_csv(scopes)?;
        let kind = match id {
            "anonymous" => PrincipalKind::Anonymous,
            "system" => PrincipalKind::System,
            "dev" => PrincipalKind::Dev,
            id if id.starts_with("otk_") => PrincipalKind::Token,
            _ => PrincipalKind::Node,
        };
        Ok(Principal {
            token_id: id.to_owned(),
            tenant,
            scopes,
            kind,
        })
    }
}

tokio::task_local! {
    /// The principal of the current request (D-P14-6). Read it with [`current`].
    pub static PRINCIPAL: Principal;
}

/// The current principal: the innermost [`PRINCIPAL`] scope, or
/// [`Principal::anonymous`] when none is set (deny by default, B2).
pub fn current() -> Principal {
    PRINCIPAL
        .try_with(Clone::clone)
        .unwrap_or_else(|_| Principal::anonymous())
}

/// Run `fut` with `p` as the current principal.
pub async fn scope<F: Future>(p: Principal, fut: F) -> F::Output {
    PRINCIPAL.scope(p, fut).await
}

/// Authentication failure. Every variant answers 401 `unauthenticated`; the variant is for
/// logs and the coalesced audit summary only (never echoed to the client).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthError {
    /// No `Authorization: Bearer` header (token mode).
    Missing,
    /// Not `ostro_otk_<16 base32>_<52 base32>` (rejected before any meta lookup).
    Malformed,
    /// Unknown token id, wrong secret or revoked.
    Invalid,
    /// Past `expires_at_ms`.
    Expired,
    /// The token exists but is not an API token (`wkr_`, `njt_`, non-API scopes).
    NotApiToken,
    /// The authenticator could not reach its store (503, not 401).
    Unavailable(String),
}

impl AuthError {
    /// The API error the transport answers with.
    pub fn to_api_error(&self) -> ApiError {
        match self {
            AuthError::Unavailable(m) => ApiError::unavailable(format!("authentication: {m}")),
            AuthError::Missing => ApiError::unauthenticated("missing bearer token"),
            _ => ApiError::unauthenticated("invalid or expired bearer token"),
        }
    }

    /// Stable short name (logs, audit summaries).
    pub const fn as_str(&self) -> &'static str {
        match self {
            AuthError::Missing => "missing",
            AuthError::Malformed => "malformed",
            AuthError::Invalid => "invalid",
            AuthError::Expired => "expired",
            AuthError::NotApiToken => "not_api_token",
            AuthError::Unavailable(_) => "unavailable",
        }
    }
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthError::Unavailable(m) => write!(f, "unavailable: {m}"),
            other => f.write_str(other.as_str()),
        }
    }
}

impl std::error::Error for AuthError {}

/// Bearer → principal (D-P14-6). `bearer` is the token after `Bearer `, `None` when the
/// request has no `Authorization` header.
#[async_trait::async_trait]
pub trait Authenticator: Send + Sync + 'static {
    /// Authenticate one request.
    async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError>;
}

/// `[auth] mode = "none"`: every request is [`Principal::dev`].
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowDev;

#[async_trait::async_trait]
impl Authenticator for AllowDev {
    async fn authenticate(&self, _bearer: Option<&str>) -> Result<Principal, AuthError> {
        Ok(Principal::dev())
    }
}

/// How a method is scoped to the principal's tenant (D-P14-7).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TenantClass {
    /// Addresses a run by id: `RunRecord.tenant` must match, else 404.
    RunId,
    /// Takes (or returns) a D23 name: qualified with the principal's tenant on input,
    /// stripped on output; a qualified name from a tenant principal is 400.
    QualifiedName,
    /// Lists runs: filtered by tenant (x_run_index Tenant rows, budgeted-scan fallback).
    ListRuns,
    /// Lists meta records: the `"<tenant>/"` prefix range.
    MetaList,
    /// System admin only (`tenant = None`).
    SystemOnly,
    /// Tenant administration (tokens, tenant's own resources): admin of that tenant or system.
    TenantAdmin,
    /// No tenant data (health, limits, version).
    Unscoped,
}

impl TenantClass {
    /// Stable name (auth matrix).
    pub const fn as_str(self) -> &'static str {
        match self {
            TenantClass::RunId => "run_id",
            TenantClass::QualifiedName => "qualified_name",
            TenantClass::ListRuns => "list_runs",
            TenantClass::MetaList => "meta_list",
            TenantClass::SystemOnly => "system_only",
            TenantClass::TenantAdmin => "tenant_admin",
            TenantClass::Unscoped => "unscoped",
        }
    }
}

/// One row of the method inventory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MethodSpec {
    /// The trait the method belongs to (`ControlPlane`, `ScheduleApi`, …).
    pub api: &'static str,
    /// The method name as written in the trait.
    pub name: &'static str,
    /// The scope it needs (D-P14-3).
    pub scope: Scope,
    /// Its tenant class (D-P14-7).
    pub tenant: TenantClass,
    /// Whether a successful (or denied) call is audited (D-P14-13).
    pub audited: bool,
    /// The Phase 14 wave in which the method is reachable behind the guard (3 = with the
    /// guard itself, 14-03; Phase 14 traits from their owner plan's wave).
    pub wave: u8,
}

macro_rules! methods {
    ($( $variant:ident => ($api:literal, $name:literal, $scope:ident, $tenant:ident, $audited:literal, $wave:literal) ),* $(,)?) => {
        /// Every control-plane and extension-trait method (14-01 Task 1 inventory).
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub enum Method {
            $( #[doc = concat!("`", $api, "::", $name, "`")] $variant, )*
        }

        impl Method {
            /// Every method, in inventory order.
            pub const ALL: &'static [Method] = &[$( Method::$variant, )*];

            /// Every method, in inventory order.
            pub fn all() -> &'static [Method] {
                Method::ALL
            }

            /// The inventory row.
            pub const fn spec(self) -> MethodSpec {
                match self {
                    $( Method::$variant => MethodSpec {
                        api: $api,
                        name: $name,
                        scope: Scope::$scope,
                        tenant: TenantClass::$tenant,
                        audited: $audited,
                        wave: $wave,
                    }, )*
                }
            }
        }
    };
}

methods! {
    // ---- ControlPlane (domain.rs) ----
    UploadModule => ("ControlPlane", "upload_module", Deploy, QualifiedName, true, 3),
    GetModule => ("ControlPlane", "get_module", Read, QualifiedName, false, 3),
    ListWorkflows => ("ControlPlane", "list_workflows", Read, MetaList, false, 3),
    ActivateWorkflow => ("ControlPlane", "activate_workflow", Deploy, QualifiedName, true, 3),
    StartRun => ("ControlPlane", "start_run", Trigger, QualifiedName, false, 3),
    GetRun => ("ControlPlane", "get_run", Read, RunId, false, 3),
    RunHistory => ("ControlPlane", "run_history", Read, RunId, false, 3),
    ListRuns => ("ControlPlane", "list_runs", Read, ListRuns, false, 3),
    Ready => ("ControlPlane", "ready", Read, Unscoped, false, 3),
    SignalRun => ("ControlPlane", "signal_run", Trigger, RunId, false, 3),
    CancelRun => ("ControlPlane", "cancel_run", Operate, RunId, true, 3),
    TerminateRun => ("ControlPlane", "terminate_run", Operate, RunId, true, 3),
    RetryRun => ("ControlPlane", "retry_run", Operate, RunId, true, 3),
    AwaitResult => ("ControlPlane", "await_result", Read, RunId, false, 3),
    CompleteToken => ("ControlPlane", "complete_token", Trigger, RunId, false, 3),
    SetWorkflowPolicy => ("ControlPlane", "set_workflow_policy", Deploy, QualifiedName, true, 3),
    GetWorkflowPolicy => ("ControlPlane", "get_workflow_policy", Read, QualifiedName, false, 3),
    SetQueuePolicy => ("ControlPlane", "set_queue_policy", Operate, QualifiedName, true, 3),
    GetQueuePolicy => ("ControlPlane", "get_queue_policy", Read, QualifiedName, false, 3),
    PutSecret => ("ControlPlane", "put_secret", Admin, TenantAdmin, true, 3),
    ListSecrets => ("ControlPlane", "list_secrets", Admin, TenantAdmin, false, 3),
    DeleteSecret => ("ControlPlane", "delete_secret", Admin, TenantAdmin, true, 3),
    PutGrant => ("ControlPlane", "put_grant", Admin, TenantAdmin, true, 3),
    ListGrants => ("ControlPlane", "list_grants", Admin, TenantAdmin, false, 3),
    DeleteGrant => ("ControlPlane", "delete_grant", Admin, TenantAdmin, true, 3),
    PutConnection => ("ControlPlane", "put_connection", Admin, TenantAdmin, true, 3),
    GetConnection => ("ControlPlane", "get_connection", Admin, TenantAdmin, false, 3),
    ListConnections => ("ControlPlane", "list_connections", Admin, TenantAdmin, false, 3),
    DeleteConnection => ("ControlPlane", "delete_connection", Admin, TenantAdmin, true, 3),
    PutEgress => ("ControlPlane", "put_egress", Admin, TenantAdmin, true, 3),
    GetEgress => ("ControlPlane", "get_egress", Admin, TenantAdmin, false, 3),
    ListEgress => ("ControlPlane", "list_egress", Admin, TenantAdmin, false, 3),
    DeleteEgress => ("ControlPlane", "delete_egress", Admin, TenantAdmin, true, 3),
    PresignedObject => ("ControlPlane", "presigned_object", Read, Unscoped, false, 3),
    GetQueueFlow => ("ControlPlane", "get_queue_flow", Read, QualifiedName, false, 3),
    PutQueueFlow => ("ControlPlane", "put_queue_flow", Operate, QualifiedName, true, 3),
    PauseQueue => ("ControlPlane", "pause_queue", Operate, QualifiedName, true, 3),
    ResumeQueue => ("ControlPlane", "resume_queue", Operate, QualifiedName, true, 3),
    PauseWorkflow => ("ControlPlane", "pause_workflow", Operate, QualifiedName, true, 3),
    ResumeWorkflow => ("ControlPlane", "resume_workflow", Operate, QualifiedName, true, 3),
    PauseRun => ("ControlPlane", "pause_run", Operate, RunId, true, 3),
    ResumeRun => ("ControlPlane", "resume_run", Operate, RunId, true, 3),
    QueueDemand => ("ControlPlane", "queue_demand", Read, QualifiedName, false, 3),
    ListWorkers => ("ControlPlane", "list_workers", Read, SystemOnly, false, 3),
    DrainWorker => ("ControlPlane", "drain_worker", Admin, SystemOnly, true, 3),
    CreateWorkerToken => ("ControlPlane", "create_worker_token", Admin, SystemOnly, true, 3),
    ListWorkerTokens => ("ControlPlane", "list_worker_tokens", Admin, SystemOnly, false, 3),
    RevokeWorkerToken => ("ControlPlane", "revoke_worker_token", Admin, SystemOnly, true, 3),
    JoinWorker => ("ControlPlane", "join_worker", Admin, SystemOnly, true, 3),
    DevTime => ("ControlPlane", "dev_time", Read, Unscoped, false, 3),
    DevSkip => ("ControlPlane", "dev_skip", Admin, SystemOnly, false, 3),
    ReplayModule => ("ControlPlane", "replay_module", Read, QualifiedName, false, 3),
    // ---- Phase 8 (phase8.rs) ----
    ListSchedules => ("ScheduleApi", "list_schedules", Read, MetaList, false, 3),
    GetSchedule => ("ScheduleApi", "get_schedule", Read, QualifiedName, false, 3),
    CreateSchedule => ("ScheduleApi", "create_schedule", Admin, TenantAdmin, true, 3),
    UpdateSchedule => ("ScheduleApi", "update_schedule", Admin, TenantAdmin, true, 3),
    DeleteSchedule => ("ScheduleApi", "delete_schedule", Admin, TenantAdmin, true, 3),
    PauseSchedule => ("ScheduleApi", "pause_schedule", Operate, QualifiedName, true, 3),
    ResumeSchedule => ("ScheduleApi", "resume_schedule", Operate, QualifiedName, true, 3),
    TriggerSchedule => ("ScheduleApi", "trigger_schedule", Operate, QualifiedName, true, 3),
    ListWebhooks => ("WebhookApi", "list_webhooks", Read, MetaList, false, 3),
    GetWebhook => ("WebhookApi", "get_webhook", Read, QualifiedName, false, 3),
    CreateWebhook => ("WebhookApi", "create_webhook", Admin, TenantAdmin, true, 3),
    UpdateWebhook => ("WebhookApi", "update_webhook", Admin, TenantAdmin, true, 3),
    DeleteWebhook => ("WebhookApi", "delete_webhook", Admin, TenantAdmin, true, 3),
    DeliverHook => ("HookIngress", "deliver", Trigger, QualifiedName, false, 3),
    RunLogs => ("RunLogsApi", "run_logs", Read, RunId, false, 3),
    Backup => ("AdminApi", "backup", Admin, SystemOnly, true, 3),
    // ---- Phase 11 (phase11.rs) ----
    RerunRun => ("OpsApi", "rerun_run", Operate, RunId, true, 3),
    AnalyzeRerun => ("OpsApi", "analyze_rerun", Read, RunId, false, 3),
    ListDlq => ("OpsApi", "list_dlq", Read, MetaList, false, 3),
    RequeueDlq => ("OpsApi", "requeue_dlq", Operate, RunId, true, 3),
    DismissDlq => ("OpsApi", "dismiss_dlq", Operate, RunId, true, 3),
    CreateBulk => ("OpsApi", "create_bulk", Operate, ListRuns, true, 3),
    ListBulk => ("OpsApi", "list_bulk", Read, MetaList, false, 3),
    GetBulk => ("OpsApi", "get_bulk", Read, MetaList, false, 3),
    CancelBulk => ("OpsApi", "cancel_bulk", Operate, MetaList, true, 3),
    Limits => ("OpsApi", "limits", Read, Unscoped, false, 3),
    GetFailurePolicy => ("OpsApi", "get_failure_policy", Read, QualifiedName, false, 3),
    SetFailurePolicy => ("OpsApi", "set_failure_policy", Deploy, QualifiedName, true, 3),
    ListModules => ("InventoryApi", "list_modules", Read, MetaList, false, 3),
    ListQueues => ("InventoryApi", "list_queues", Read, MetaList, false, 3),
    ListPartitions => ("InventoryApi", "list_partitions", Read, SystemOnly, false, 3),
    // ---- Phase 13 (phase13.rs) ----
    ClusterInit => ("ClusterApi", "init", Admin, SystemOnly, true, 3),
    ClusterJoin => ("ClusterApi", "join", Admin, SystemOnly, true, 3),
    ClusterStatus => ("ClusterApi", "status", Read, SystemOnly, false, 3),
    CreateNodeToken => ("ClusterApi", "create_node_token", Admin, SystemOnly, true, 3),
    ClusterRemove => ("ClusterApi", "remove", Admin, SystemOnly, true, 3),
    ClusterDrain => ("ClusterApi", "drain", Admin, SystemOnly, true, 3),
    ClusterReassign => ("ClusterApi", "reassign", Admin, SystemOnly, true, 3),
    ClusterRebalance => ("ClusterApi", "rebalance", Admin, SystemOnly, true, 3),
    UpgradeStatus => ("ClusterApi", "upgrade_status", Read, SystemOnly, false, 3),
    UpgradeFinalize => ("ClusterApi", "upgrade_finalize", Admin, SystemOnly, true, 3),
    UpgradeHold => ("ClusterApi", "upgrade_hold", Admin, SystemOnly, true, 3),
    UpgradeRelease => ("ClusterApi", "upgrade_release", Admin, SystemOnly, true, 3),
    RetireModule => ("ModuleLifecycleApi", "retire_module", Deploy, QualifiedName, true, 3),
    RunModuleGc => ("ModuleLifecycleApi", "run_module_gc", Admin, SystemOnly, true, 3),
    MigrateRuns => ("ModuleLifecycleApi", "migrate_runs", Deploy, QualifiedName, true, 3),
    // ---- Phase 14 (phase14.rs; reachable from their owner plans) ----
    CreateTenant => ("TenantsApi", "create_tenant", Admin, SystemOnly, true, 4),
    GetTenant => ("TenantsApi", "get_tenant", Read, SystemOnly, false, 4),
    ListTenants => ("TenantsApi", "list_tenants", Read, SystemOnly, false, 4),
    UpdateTenant => ("TenantsApi", "update_tenant", Admin, SystemOnly, true, 4),
    SuspendTenant => ("TenantsApi", "suspend_tenant", Admin, SystemOnly, true, 4),
    ResumeTenant => ("TenantsApi", "resume_tenant", Admin, SystemOnly, true, 4),
    DeleteTenant => ("TenantsApi", "delete_tenant", Admin, SystemOnly, true, 4),
    CreateApiToken => ("ApiTokensApi", "create_token", Admin, TenantAdmin, true, 4),
    ListApiTokens => ("ApiTokensApi", "list_tokens", Admin, TenantAdmin, false, 4),
    RevokeApiToken => ("ApiTokensApi", "revoke_token", Admin, TenantAdmin, true, 4),
    RotateApiToken => ("ApiTokensApi", "rotate_token", Admin, TenantAdmin, true, 4),
    WhoAmI => ("ApiTokensApi", "whoami", Read, Unscoped, false, 4),
    QueryAudit => ("AuditApi", "query", Admin, TenantAdmin, false, 4),
    GetModulePolicy => ("SecurityApi", "get_module_policy", Read, SystemOnly, false, 4),
    SetModulePolicy => ("SecurityApi", "set_module_policy", Admin, SystemOnly, true, 4),
    AddTrustedKey => ("SecurityApi", "add_trusted_key", Admin, TenantAdmin, true, 4),
    RemoveTrustedKey => ("SecurityApi", "remove_trusted_key", Admin, TenantAdmin, true, 4),
    AttachSignatures => ("SecurityApi", "attach_signatures", Deploy, QualifiedName, true, 4),
}

impl Method {
    /// `<api>.<method>` (unique; the auth matrix row key).
    pub fn name(self) -> String {
        let s = self.spec();
        format!("{}.{}", s.api, s.name)
    }

    /// The scope the method needs.
    pub fn scope(self) -> Scope {
        self.spec().scope
    }

    /// Look up a method by trait and method name.
    pub fn find(api: &str, name: &str) -> Option<Method> {
        Method::ALL.iter().copied().find(|m| {
            let s = m.spec();
            s.api == api && s.name == name
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn scope_parsing_and_admin_rule() {
        for s in Scope::ALL {
            assert_eq!(Scope::parse(s.as_str()), Some(s));
        }
        assert_eq!(Scope::parse("worker:join"), None);
        assert_eq!(Scope::parse("node:join"), None);
        assert_eq!(Scope::parse("Admin"), None);
        let set = ScopeSet::parse(&["read", "trigger"]).unwrap();
        assert!(set.allows(Scope::Read) && set.allows(Scope::Trigger));
        // No implication except admin.
        assert!(!set.allows(Scope::Operate));
        assert!(!ScopeSet::of(&[Scope::Deploy]).allows(Scope::Read));
        assert!(!ScopeSet::of(&[Scope::Operate]).allows(Scope::Trigger));
        for s in Scope::ALL {
            assert!(ScopeSet::admin().allows(s));
            assert!(!ScopeSet::EMPTY.allows(s));
        }
        assert!(ScopeSet::parse(&["read", "worker:join"]).is_err());
        assert_eq!(set.to_strings(), ["trigger", "read"]);
        assert_eq!(ScopeSet::from_csv(&set.to_csv()).unwrap(), set);
        assert_eq!(ScopeSet::from_csv("").unwrap(), ScopeSet::EMPTY);
    }

    #[test]
    fn forwarded_principal_round_trips() {
        let cases = [
            Principal::token(
                "otk_abcdefghijklmnop",
                Some("acme".into()),
                ScopeSet::of(&[Scope::Read, Scope::Operate]),
            ),
            Principal::token("otk_qrstuvwxyz234567", None, ScopeSet::admin()),
            Principal::system(),
            Principal::dev(),
            Principal::anonymous(),
        ];
        for p in cases {
            let enc = p.encode_forwarded();
            assert!(enc.starts_with("p1;"), "{enc}");
            assert_eq!(Principal::decode_forwarded(&enc).unwrap(), p, "{enc}");
        }
        assert_eq!(
            Principal::token("otk_x", Some("acme".into()), ScopeSet::of(&[Scope::Read]))
                .encode_forwarded(),
            "p1;otk_x;acme;read"
        );
        let node = Principal::decode_forwarded("p1;node:3;*;read").unwrap();
        assert_eq!(node.kind, PrincipalKind::Node);
        for bad in [
            "",
            "p2;otk_x;*;read",
            "p1;otk_x;*",
            "p1;otk_x;*;read;extra",
            "p1;;*;read",
            "p1;otk_x;;read",
            "p1;otk_x;*;worker:join",
        ] {
            assert!(Principal::decode_forwarded(bad).is_err(), "{bad:?}");
        }
    }

    #[tokio::test]
    async fn current_outside_a_scope_is_anonymous() {
        let p = current();
        assert_eq!(p, Principal::anonymous());
        assert!(p.is_anonymous());
        assert!(p.scopes.is_empty());
        for s in Scope::ALL {
            assert!(!p.scopes.allows(s));
        }
        let inner = scope(Principal::system(), async { current() }).await;
        assert_eq!(inner.kind, PrincipalKind::System);
        // Leaving the scope restores the default.
        assert!(current().is_anonymous());
        // A spawned task does not inherit the scope (14-02 uses spawn_scoped for that).
        let spawned = scope(Principal::dev(), async {
            tokio::spawn(async { current() }).await.unwrap()
        })
        .await;
        assert!(spawned.is_anonymous());
    }

    #[tokio::test]
    async fn allow_dev_authenticates_everything_as_dev() {
        let a: &dyn Authenticator = &AllowDev;
        assert_eq!(a.authenticate(None).await.unwrap(), Principal::dev());
        assert_eq!(a.authenticate(Some("x")).await.unwrap(), Principal::dev());
    }

    #[test]
    fn auth_errors_map_to_401_except_unavailable() {
        for e in [
            AuthError::Missing,
            AuthError::Malformed,
            AuthError::Invalid,
            AuthError::Expired,
            AuthError::NotApiToken,
        ] {
            assert_eq!(e.to_api_error().http_status().as_u16(), 401, "{e}");
        }
        assert_eq!(
            AuthError::Unavailable("meta".into())
                .to_api_error()
                .http_status()
                .as_u16(),
            503
        );
    }

    #[test]
    fn method_names_are_unique_and_complete() {
        let names: BTreeSet<String> = Method::all().iter().map(|m| m.name()).collect();
        assert_eq!(names.len(), Method::all().len(), "method names unique");
        for m in Method::all() {
            let s = m.spec();
            assert_eq!(Method::find(s.api, s.name), Some(*m));
            // tenant = None principals hold only admin or read (D-P14-3): a system-only
            // method therefore needs one of them.
            if s.tenant == TenantClass::SystemOnly {
                assert!(
                    matches!(s.scope, Scope::Admin | Scope::Read),
                    "{}",
                    m.name()
                );
            }
        }
        // Per-trait counts match the 14-01 inventory (52 ControlPlane methods, ...).
        let count = |api: &str| Method::all().iter().filter(|m| m.spec().api == api).count();
        assert_eq!(count("ControlPlane"), 52);
        assert_eq!(count("ScheduleApi"), 8);
        assert_eq!(count("WebhookApi"), 5);
        assert_eq!(count("OpsApi"), 12);
        assert_eq!(count("InventoryApi"), 3);
        assert_eq!(count("ClusterApi"), 12);
        assert_eq!(count("ModuleLifecycleApi"), 3);
        assert_eq!(count("TenantsApi"), 7);
        assert_eq!(count("ApiTokensApi"), 5);
        assert_eq!(count("AuditApi"), 1);
        assert_eq!(count("SecurityApi"), 5);
    }
}
