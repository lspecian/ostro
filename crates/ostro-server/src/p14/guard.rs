//! Phase 14 (14-03): the guard decorator (D-P14-6, revision B2). **Final after 14-03**: later
//! plans change behaviour only in their hook files (`authz.rs` 14-15, `tenant_scope.rs`
//! 14-16, `audit_hooks.rs` 14-18).
//!
//! [`wrap`] returns [`GuardedControlPlane`] and one guarded wrapper per populated
//! [`ApiExtensions`] slot (Phase 8 `schedules`, `webhooks`, `hooks`, `run_logs`, `admin`;
//! Phase 11 `ops`, `inventory`; Phase 13 `cluster`, `module_lifecycle`; Phase 14 `tenants`,
//! `api_tokens`, `audit`, `security`). Every method runs exactly ([`Core::call`]):
//!
//! 1. `p = ostro_api::auth::current()`; an `Anonymous` principal (no `PRINCIPAL` scope was
//!    set) is refused with 401 `unauthenticated` before any hook (B2);
//! 2. `authz::require(&p, Method::X)`;
//! 3. `req = tenant_scope::before(&p, Method::X, routed_control_plane, req)`;
//! 4. `permit = audit_hooks::reserve(ctx, &p, Method::X)`;
//! 5. the inner (routed) call;
//! 6. `audit_hooks::after(ctx, permit, &p, Method::X, target, outcome)`;
//! 7. `tenant_scope::after(&p, Method::X, out)`.
//!
//! The request handed to the hooks is the method's single argument, `()` for none, or a
//! tuple of all arguments in parameter order. `target` is computed from the scoped request by
//! a small per-method helper (run id, name, sha, …; empty when there is none); `outcome` is
//! `Ok` or `Failed(<error code>)`. Streaming and long-poll methods (`await_result`,
//! `run_logs`, `backup`) apply the hooks around the call that sets the stream up.
//!
//! `ControlPlane::ready` is a synchronous readiness probe (served by the unauthenticated
//! `/readyz`) carrying no tenant data: it passes through without a principal check.
//!
//! Every stage emits a `TRACE` event (target [`TRACE_TARGET`], fields `method`, `stage`),
//! which is how the hook order is observed in tests without test-only hooks.
//!
//! The guard holds the Phase 14 context weakly (as every router-held reference must, see
//! `p14/mod.rs`); once the server released it, guarded calls answer 503.

use std::future::Future;
use std::sync::{Arc, Weak};

use ostro_api::ControlPlane;
use ostro_api::auth::{self, Method, Principal, PrincipalKind};
use ostro_api::domain::{
    ActivateWorkflow, AwaitResultRequest, AwaitResultResponse, CancelRequest, CancelResponse,
    CompleteTokenRequest, CompleteTokenResponse, ConnectionInfo, DevSkip, DevSkipResult,
    DevTimeInfo, EgressInfo, GrantInfo, HistoryPage, HistoryRequest, ListRunsRequest, ModuleInfo,
    PresignedObjectRequest, PresignedObjectResponse, PutSecret, ReplayRequest, ReplayResult,
    RetryRequest, RetryResponse, RunInfo, RunPage, RunPolicy, SecretInfo, SignalRequest,
    SignalResponse, StartRunRequest, StartRunResult, TerminateRequest, TerminateResponse,
    UploadModule, UploadModuleResult, WorkflowInfo,
};
use ostro_api::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, DrainResult, JoinRequest as WorkerJoinRequest,
    JoinResponse as WorkerJoinResponse, PauseState, QueueFlow, QueueFlowInfo, RunPauseState,
    WorkerInfo, WorkerTokenCreated, WorkerTokenInfo,
};
use ostro_api::phase8::{
    AdminApi, BackupStream, HookIngress, HookRequest, HookResponse, RunLogsApi, RunLogsRequest,
    RunLogsResponse, ScheduleApi, ScheduleDto, WebhookApi, WebhookDto,
};
use ostro_api::phase11::{
    BulkCreateRequest, BulkCreateResponse, BulkJobView, DlqDismissRequest, DlqEntry,
    DlqListRequest, DlqPage, DlqRequeueRequest, DlqRequeueResponse, FailurePolicy, InventoryApi,
    LimitsView, ListModulesRequest, ModulePage, OpsApi, PartitionView, PolicyScope, QueueView,
    RerunCutsView, RerunRequest, RerunResponse,
};
use ostro_api::phase13::{
    ClusterApi, ClusterStatus, GcRunReport, InitRequest, JoinRequest as NodeJoinRequest,
    JoinResponse as NodeJoinResponse, MigrateRunsReport, MigrateRunsRequest, ModuleLifecycleApi,
    NodeTokenRequest, NodeTokenResponse, ReassignRequest, RetireModuleResponse, UpgradeStatus,
};
use ostro_api::phase14::{
    ApiTokensApi, AttachSignaturesRequest, AttachSignaturesResult, AuditApi, AuditPage, AuditQuery,
    CreateTokenRequest, CreatedToken, ModulePolicy, RotateTokenRequest, SecurityApi, TenantInfo,
    TenantPatch, TenantSpec, TenantsApi, TokenInfo, TrustedKeySpec, WhoAmI,
};
use ostro_api::{ApiError, ApiExtensions};
use ostro_types::audit::{AuditOutcome, AuditPermit};
use tracing::trace;

use super::{Guarded, P14Ctx, audit_hooks, authz, tenant_scope};

/// Tracing target of the per-stage events.
pub const TRACE_TARGET: &str = "ostro::p14::guard";

/// Wrap the routed control plane and every populated extension slot.
pub fn wrap(
    ctx: &Arc<P14Ctx>,
    routed: Arc<dyn ControlPlane>,
    routed_ext: ApiExtensions,
) -> Guarded {
    // 14-17: `delete_tenant` counts the tenant's runs through the routed control plane.
    super::tenants_api::bind_routed(ctx, &routed);
    wrap_weak(Arc::downgrade(ctx), routed, routed_ext)
}

/// [`wrap`] over a weak context (tests drop the context to observe the 503 path).
pub fn wrap_weak(
    ctx: Weak<P14Ctx>,
    routed: Arc<dyn ControlPlane>,
    routed_ext: ApiExtensions,
) -> Guarded {
    let core = Arc::new(Core {
        ctx,
        control: Arc::clone(&routed),
    });
    let ext = ApiExtensions {
        schedules: routed_ext
            .schedules
            .map(|i| Arc::new(GuardedSchedules::new(&core, i)) as Arc<dyn ScheduleApi>),
        webhooks: routed_ext
            .webhooks
            .map(|i| Arc::new(GuardedWebhooks::new(&core, i)) as Arc<dyn WebhookApi>),
        hooks: routed_ext
            .hooks
            .map(|i| Arc::new(GuardedHooks::new(&core, i)) as Arc<dyn HookIngress>),
        run_logs: routed_ext
            .run_logs
            .map(|i| Arc::new(GuardedRunLogs::new(&core, i)) as Arc<dyn RunLogsApi>),
        admin: routed_ext
            .admin
            .map(|i| Arc::new(GuardedAdmin::new(&core, i)) as Arc<dyn AdminApi>),
        hook_body_limit: routed_ext.hook_body_limit,
        ops: routed_ext
            .ops
            .map(|i| Arc::new(GuardedOps::new(&core, i)) as Arc<dyn OpsApi>),
        inventory: routed_ext
            .inventory
            .map(|i| Arc::new(GuardedInventory::new(&core, i)) as Arc<dyn InventoryApi>),
        cluster: routed_ext
            .cluster
            .map(|i| Arc::new(GuardedCluster::new(&core, i)) as Arc<dyn ClusterApi>),
        module_lifecycle: routed_ext.module_lifecycle.map(|i| {
            Arc::new(GuardedModuleLifecycle::new(&core, i)) as Arc<dyn ModuleLifecycleApi>
        }),
        tenants: routed_ext
            .tenants
            .map(|i| Arc::new(GuardedTenants::new(&core, i)) as Arc<dyn TenantsApi>),
        api_tokens: routed_ext
            .api_tokens
            .map(|i| Arc::new(GuardedApiTokens::new(&core, i)) as Arc<dyn ApiTokensApi>),
        audit: routed_ext
            .audit
            .map(|i| Arc::new(GuardedAudit::new(&core, i)) as Arc<dyn AuditApi>),
        security: routed_ext
            .security
            .map(|i| Arc::new(GuardedSecurity::new(&core, i)) as Arc<dyn SecurityApi>),
    };
    Guarded {
        control: Arc::new(GuardedControlPlane::new(&core, routed)),
        ext,
    }
}

/// Every guarded method, one entry per wrapper method, plus the pass-through
/// `ControlPlane::ready` (the completeness test compares it with `Method::all()`).
pub fn guarded_methods() -> Vec<Method> {
    [
        GuardedControlPlane::METHODS,
        &[Method::Ready],
        GuardedSchedules::METHODS,
        GuardedWebhooks::METHODS,
        GuardedHooks::METHODS,
        GuardedRunLogs::METHODS,
        GuardedAdmin::METHODS,
        GuardedOps::METHODS,
        GuardedInventory::METHODS,
        GuardedCluster::METHODS,
        GuardedModuleLifecycle::METHODS,
        GuardedTenants::METHODS,
        GuardedApiTokens::METHODS,
        GuardedAudit::METHODS,
        GuardedSecurity::METHODS,
    ]
    .concat()
}

/// The guarded wrapper serving `m` (compile-time completeness: a new [`Method`] variant
/// breaks this match until a wrapper method is written for it).
pub const fn wrapper_of(m: Method) -> &'static str {
    use Method::*;
    match m {
        UploadModule | GetModule | ListWorkflows | ActivateWorkflow | StartRun | GetRun
        | RunHistory | ListRuns | Ready | SignalRun | CancelRun | TerminateRun | RetryRun
        | AwaitResult | CompleteToken | SetWorkflowPolicy | GetWorkflowPolicy | SetQueuePolicy
        | GetQueuePolicy | PutSecret | ListSecrets | DeleteSecret | PutGrant | ListGrants
        | DeleteGrant | PutConnection | GetConnection | ListConnections | DeleteConnection
        | PutEgress | GetEgress | ListEgress | DeleteEgress | PresignedObject | GetQueueFlow
        | PutQueueFlow | PauseQueue | ResumeQueue | PauseWorkflow | ResumeWorkflow | PauseRun
        | ResumeRun | QueueDemand | ListWorkers | DrainWorker | CreateWorkerToken
        | ListWorkerTokens | RevokeWorkerToken | JoinWorker | DevTime | DevSkip | ReplayModule => {
            "GuardedControlPlane"
        }
        ListSchedules | GetSchedule | CreateSchedule | UpdateSchedule | DeleteSchedule
        | PauseSchedule | ResumeSchedule | TriggerSchedule => "GuardedSchedules",
        ListWebhooks | GetWebhook | CreateWebhook | UpdateWebhook | DeleteWebhook => {
            "GuardedWebhooks"
        }
        DeliverHook => "GuardedHooks",
        RunLogs => "GuardedRunLogs",
        Backup => "GuardedAdmin",
        RerunRun | AnalyzeRerun | ListDlq | RequeueDlq | DismissDlq | CreateBulk | ListBulk
        | GetBulk | CancelBulk | Limits | GetFailurePolicy | SetFailurePolicy => "GuardedOps",
        ListModules | ListQueues | ListPartitions => "GuardedInventory",
        ClusterInit | ClusterJoin | ClusterStatus | CreateNodeToken | ClusterRemove
        | ClusterDrain | ClusterReassign | ClusterRebalance | UpgradeStatus | UpgradeFinalize
        | UpgradeHold | UpgradeRelease => "GuardedCluster",
        RetireModule | RunModuleGc | MigrateRuns => "GuardedModuleLifecycle",
        CreateTenant | GetTenant | ListTenants | UpdateTenant | SuspendTenant | ResumeTenant
        | DeleteTenant => "GuardedTenants",
        CreateApiToken | ListApiTokens | RevokeApiToken | RotateApiToken | WhoAmI => {
            "GuardedApiTokens"
        }
        QueryAudit => "GuardedAudit",
        GetModulePolicy | SetModulePolicy | AddTrustedKey | RemoveTrustedKey | AttachSignatures => {
            "GuardedSecurity"
        }
    }
}

/// Shared state of every wrapper: the weak context and the routed control plane (the
/// `tenant_scope::before` lookup target for extension methods too).
struct Core {
    ctx: Weak<P14Ctx>,
    control: Arc<dyn ControlPlane>,
}

impl Core {
    /// The fixed hook sequence around one inner call (see the module docs). Only the
    /// request-typed steps (`tenant_scope::before/after`, the target helper and the inner
    /// call) are generic; everything else lives in the non-generic helpers below, so the ~115
    /// instantiations stay small (binary size, NFR-2.1).
    async fn call<I, Req, Out, Fut>(
        &self,
        inner: &Arc<I>,
        m: Method,
        req: Req,
        target: impl FnOnce(&Req) -> String + Send,
        f: impl FnOnce(Arc<I>, Req) -> Fut + Send,
    ) -> Result<Out, ApiError>
    where
        I: ?Sized + Send + Sync,
        // 'static (14-16): `tenant_scope` inspects the request/response as `dyn Any`.
        Req: Send + 'static,
        Out: Send + 'static,
        Fut: Future<Output = Result<Out, ApiError>> + Send,
    {
        let p = admit(m)?;
        let req = tenant_scope::before(&p, m, &*self.control, req).await?;
        let (ctx, permit) = self.reserve(&p, m).await?;
        let target = target(&req);
        stage(m, "inner");
        let out = f(Arc::clone(inner), req).await;
        finish(ctx, permit, &p, m, target, outcome_of(out.as_ref().err())).await?;
        tenant_scope::after(&p, m, out)
    }

    /// Step 4: the context (503 once released) and the audit permit.
    async fn reserve(
        &self,
        p: &Principal,
        m: Method,
    ) -> Result<(Arc<P14Ctx>, Option<AuditPermit>), ApiError> {
        let ctx = self
            .ctx
            .upgrade()
            .ok_or_else(|| ApiError::unavailable("the server is shutting down"))?;
        stage(m, "audit_reserve");
        let permit = audit_hooks::reserve(&ctx, p, m).await?;
        Ok((ctx, permit))
    }
}

/// Steps 1-2 (and the trace of step 3): the principal, `Anonymous` refused, `authz`.
fn admit(m: Method) -> Result<Principal, ApiError> {
    let p = auth::current();
    if p.kind == PrincipalKind::Anonymous {
        stage(m, "deny_anonymous");
        return Err(ApiError::unauthenticated("authentication required"));
    }
    stage(m, "authz");
    authz::require(&p, m)?;
    stage(m, "tenant_before");
    Ok(p)
}

/// Step 6 (and the trace of step 7): commit the audit record.
async fn finish(
    ctx: Arc<P14Ctx>,
    permit: Option<AuditPermit>,
    p: &Principal,
    m: Method,
    target: String,
    outcome: AuditOutcome,
) -> Result<(), ApiError> {
    stage(m, "audit_after");
    audit_hooks::after(&ctx, permit, p, m, &target, outcome).await?;
    stage(m, "tenant_after");
    Ok(())
}

/// The per-stage TRACE event (one callsite).
fn stage(m: Method, stage: &'static str) {
    trace!(target: TRACE_TARGET, method = ?m, stage);
}

/// The audit outcome of an inner result.
fn outcome_of(err: Option<&ApiError>) -> AuditOutcome {
    match err {
        None => AuditOutcome::Ok,
        Some(e) => AuditOutcome::Failed(e.code.as_str().to_owned()),
    }
}

// --- per-method target helpers ---------------------------------------------------------

fn no_target<T: ?Sized>(_: &T) -> String {
    String::new()
}

fn text<S: AsRef<str> + ?Sized>(s: &S) -> String {
    s.as_ref().to_owned()
}

fn first<B>(t: &(String, B)) -> String {
    t.0.clone()
}

fn first3<B, C>(t: &(String, B, C)) -> String {
    t.0.clone()
}

fn opt_text(s: &Option<String>) -> String {
    s.clone().unwrap_or_default()
}

fn node_id<B>(t: &(u64, B)) -> String {
    format!("node:{}", t.0)
}

fn node(id: &u64) -> String {
    format!("node:{id}")
}

fn policy_scope(s: &PolicyScope) -> String {
    match s {
        PolicyScope::Workflow(w) => format!("workflow:{w}"),
        PolicyScope::Queue(q) => format!("queue:{q}"),
    }
}

// --- the wrapper generator -------------------------------------------------------------

/// Pack a method's arguments into the hook request (`()`, the single argument, or a tuple).
macro_rules! pack {
    () => {
        ()
    };
    ($a:ident) => {
        $a
    };
    ($($a:ident),+) => {
        ($($a),+)
    };
}

/// Generate a guarded wrapper `$w` for trait `$tr` over `Arc<dyn $tr>`: one method per
/// `fn name(args) -> Out => Method, target;` line, plus `$w::METHODS`.
macro_rules! guarded {
    (
        $(#[$doc:meta])*
        $w:ident : $tr:ident {
            $( fn $name:ident ( $($arg:ident : $ty:ty),* ) -> $out:ty => $m:ident, $tgt:expr ; )*
        }
        $( extra { $($extra:item)* } )?
    ) => {
        $(#[$doc])*
        pub struct $w {
            core: Arc<Core>,
            inner: Arc<dyn $tr>,
        }

        impl $w {
            fn new(core: &Arc<Core>, inner: Arc<dyn $tr>) -> Self {
                Self { core: Arc::clone(core), inner }
            }

            /// The methods this wrapper guards (one per guarded trait method).
            pub const METHODS: &'static [Method] = &[$(Method::$m),*];
        }

        impl std::fmt::Debug for $w {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($w)).finish_non_exhaustive()
            }
        }

        #[async_trait::async_trait]
        impl $tr for $w {
            $(
                async fn $name(&self, $($arg: $ty),*) -> Result<$out, ApiError> {
                    self.core
                        .call(
                            &self.inner,
                            Method::$m,
                            pack!($($arg),*),
                            $tgt,
                            |inner: Arc<dyn $tr>, req| async move {
                                #[allow(clippy::let_unit_value, unused_parens)]
                                let pack!($($arg),*) = req;
                                inner.$name($($arg),*).await
                            },
                        )
                        .await
                }
            )*
            $( $($extra)* )?
        }
    };
}

guarded! {
    /// The guarded [`ControlPlane`] (REST, gRPC and the UI receive it).
    GuardedControlPlane: ControlPlane {
        fn upload_module(req: UploadModule) -> UploadModuleResult => UploadModule, |r: &UploadModule| r.sha256.clone();
        fn get_module(sha256: String) -> ModuleInfo => GetModule, text;
        fn list_workflows() -> Vec<WorkflowInfo> => ListWorkflows, no_target;
        fn activate_workflow(req: ActivateWorkflow) -> WorkflowInfo => ActivateWorkflow, |r: &ActivateWorkflow| format!("{}@{}", r.name, r.version);
        fn start_run(req: StartRunRequest) -> StartRunResult => StartRun, |r: &StartRunRequest| r.workflow.clone();
        fn get_run(run_id: String) -> RunInfo => GetRun, text;
        fn run_history(req: HistoryRequest) -> HistoryPage => RunHistory, |r: &HistoryRequest| r.run_id.clone();
        fn list_runs(req: ListRunsRequest) -> RunPage => ListRuns, no_target;
        fn signal_run(r: SignalRequest) -> SignalResponse => SignalRun, |r: &SignalRequest| r.run_id.clone();
        fn cancel_run(r: CancelRequest) -> CancelResponse => CancelRun, |r: &CancelRequest| r.run_id.clone();
        fn terminate_run(r: TerminateRequest) -> TerminateResponse => TerminateRun, |r: &TerminateRequest| r.run_id.clone();
        fn retry_run(r: RetryRequest) -> RetryResponse => RetryRun, |r: &RetryRequest| r.run_id.clone();
        fn await_result(r: AwaitResultRequest) -> AwaitResultResponse => AwaitResult, |r: &AwaitResultRequest| r.run_id.clone();
        fn complete_token(r: CompleteTokenRequest) -> CompleteTokenResponse => CompleteToken, no_target;
        fn set_workflow_policy(workflow: String, p: RunPolicy) -> RunPolicy => SetWorkflowPolicy, first;
        fn get_workflow_policy(workflow: String) -> RunPolicy => GetWorkflowPolicy, text;
        fn set_queue_policy(queue: String, p: RunPolicy) -> RunPolicy => SetQueuePolicy, first;
        fn get_queue_policy(queue: String) -> RunPolicy => GetQueuePolicy, text;
        fn put_secret(req: PutSecret) -> SecretInfo => PutSecret, |r: &PutSecret| r.name.clone();
        fn list_secrets(tenant: Option<String>) -> Vec<SecretInfo> => ListSecrets, opt_text;
        fn delete_secret(name: String, tenant: Option<String>) -> () => DeleteSecret, first;
        fn put_grant(g: GrantInfo) -> GrantInfo => PutGrant, |g: &GrantInfo| g.id.clone();
        fn list_grants(module: Option<String>) -> Vec<GrantInfo> => ListGrants, opt_text;
        fn delete_grant(id: String) -> () => DeleteGrant, text;
        fn put_connection(c: ConnectionInfo) -> ConnectionInfo => PutConnection, |c: &ConnectionInfo| c.name.clone();
        fn get_connection(name: String) -> ConnectionInfo => GetConnection, text;
        fn list_connections() -> Vec<ConnectionInfo> => ListConnections, no_target;
        fn delete_connection(name: String) -> () => DeleteConnection, text;
        fn put_egress(e: EgressInfo) -> EgressInfo => PutEgress, |e: &EgressInfo| e.module.clone();
        fn get_egress(module: String) -> EgressInfo => GetEgress, text;
        fn list_egress() -> Vec<EgressInfo> => ListEgress, no_target;
        fn delete_egress(module: String) -> () => DeleteEgress, text;
        fn presigned_object(req: PresignedObjectRequest) -> PresignedObjectResponse => PresignedObject, |r: &PresignedObjectRequest| r.connection.clone();
        fn get_queue_flow(queue: String) -> QueueFlowInfo => GetQueueFlow, text;
        fn put_queue_flow(queue: String, flow: QueueFlow, expected_version: Option<u64>) -> QueueFlowInfo => PutQueueFlow, first3;
        fn pause_queue(queue: String) -> PauseState => PauseQueue, text;
        fn resume_queue(queue: String) -> PauseState => ResumeQueue, text;
        fn pause_workflow(workflow: String) -> PauseState => PauseWorkflow, text;
        fn resume_workflow(workflow: String) -> PauseState => ResumeWorkflow, text;
        fn pause_run(run_id: String, detail: Option<String>) -> RunPauseState => PauseRun, first;
        fn resume_run(run_id: String) -> RunPauseState => ResumeRun, text;
        fn queue_demand(queue: String) -> DemandSnapshot => QueueDemand, text;
        fn list_workers() -> Vec<WorkerInfo> => ListWorkers, no_target;
        fn drain_worker(worker_id: String) -> DrainResult => DrainWorker, text;
        fn create_worker_token(req: CreateWorkerTokenRequest) -> WorkerTokenCreated => CreateWorkerToken, no_target;
        fn list_worker_tokens() -> Vec<WorkerTokenInfo> => ListWorkerTokens, no_target;
        fn revoke_worker_token(token_id: String) -> () => RevokeWorkerToken, text;
        fn join_worker(req: WorkerJoinRequest) -> WorkerJoinResponse => JoinWorker, no_target;
        fn dev_time() -> DevTimeInfo => DevTime, no_target;
        fn dev_skip(s: DevSkip) -> DevSkipResult => DevSkip, no_target;
        fn replay_module(r: ReplayRequest) -> ReplayResult => ReplayModule, |r: &ReplayRequest| r.module_sha256.clone();
    }
    extra {
        /// Readiness probe (unauthenticated `/readyz`, no tenant data): passes through.
        fn ready(&self) -> bool {
            self.inner.ready()
        }
    }
}

guarded! {
    /// The guarded Phase 8 [`ScheduleApi`].
    GuardedSchedules: ScheduleApi {
        fn list_schedules() -> Vec<ScheduleDto> => ListSchedules, no_target;
        fn get_schedule(name: String) -> ScheduleDto => GetSchedule, text;
        fn create_schedule(s: ScheduleDto) -> ScheduleDto => CreateSchedule, |s: &ScheduleDto| s.name.clone();
        fn update_schedule(s: ScheduleDto) -> ScheduleDto => UpdateSchedule, |s: &ScheduleDto| s.name.clone();
        fn delete_schedule(name: String, version_tag: Option<u64>) -> () => DeleteSchedule, first;
        fn pause_schedule(name: String) -> ScheduleDto => PauseSchedule, text;
        fn resume_schedule(name: String) -> ScheduleDto => ResumeSchedule, text;
        fn trigger_schedule(name: String, request_id: Option<String>) -> String => TriggerSchedule, first;
    }
}

guarded! {
    /// The guarded Phase 8 [`WebhookApi`].
    GuardedWebhooks: WebhookApi {
        fn list_webhooks() -> Vec<WebhookDto> => ListWebhooks, no_target;
        fn get_webhook(name: String) -> WebhookDto => GetWebhook, text;
        fn create_webhook(w: WebhookDto) -> WebhookDto => CreateWebhook, |w: &WebhookDto| w.name.clone();
        fn update_webhook(w: WebhookDto) -> WebhookDto => UpdateWebhook, |w: &WebhookDto| w.name.clone();
        fn delete_webhook(name: String, version_tag: Option<u64>) -> () => DeleteWebhook, first;
    }
}

guarded! {
    /// The guarded Phase 8 [`HookIngress`] (`POST /hooks/*` runs as `System`, 14-02).
    GuardedHooks: HookIngress {
        fn deliver(req: HookRequest) -> HookResponse => DeliverHook, |r: &HookRequest| r.name.clone();
    }
}

guarded! {
    /// The guarded Phase 8 [`RunLogsApi`].
    GuardedRunLogs: RunLogsApi {
        fn run_logs(req: RunLogsRequest) -> RunLogsResponse => RunLogs, |r: &RunLogsRequest| r.run_id.clone();
    }
}

guarded! {
    /// The guarded Phase 8 [`AdminApi`] (the hooks run around the stream setup).
    GuardedAdmin: AdminApi {
        fn backup() -> BackupStream => Backup, no_target;
    }
}

guarded! {
    /// The guarded Phase 11 [`OpsApi`].
    GuardedOps: OpsApi {
        fn rerun_run(req: RerunRequest) -> RerunResponse => RerunRun, |r: &RerunRequest| r.run_id.clone();
        fn analyze_rerun(run_id: String) -> RerunCutsView => AnalyzeRerun, text;
        fn list_dlq(req: DlqListRequest) -> DlqPage => ListDlq, no_target;
        fn requeue_dlq(req: DlqRequeueRequest) -> DlqRequeueResponse => RequeueDlq, |r: &DlqRequeueRequest| r.run_id.clone();
        fn dismiss_dlq(req: DlqDismissRequest) -> DlqEntry => DismissDlq, |r: &DlqDismissRequest| r.run_id.clone();
        fn create_bulk(req: BulkCreateRequest) -> BulkCreateResponse => CreateBulk, no_target;
        fn list_bulk() -> Vec<BulkJobView> => ListBulk, no_target;
        fn get_bulk(job_id: String) -> BulkJobView => GetBulk, text;
        fn cancel_bulk(job_id: String) -> BulkJobView => CancelBulk, text;
        fn limits() -> LimitsView => Limits, no_target;
        fn get_failure_policy(scope: PolicyScope) -> FailurePolicy => GetFailurePolicy, policy_scope;
        fn set_failure_policy(scope: PolicyScope, p: FailurePolicy) -> FailurePolicy => SetFailurePolicy, |t: &(PolicyScope, FailurePolicy)| policy_scope(&t.0);
    }
}

guarded! {
    /// The guarded Phase 11 [`InventoryApi`].
    GuardedInventory: InventoryApi {
        fn list_modules(req: ListModulesRequest) -> ModulePage => ListModules, no_target;
        fn list_queues() -> Vec<QueueView> => ListQueues, no_target;
        fn list_partitions() -> Vec<PartitionView> => ListPartitions, no_target;
    }
}

guarded! {
    /// The guarded Phase 13 [`ClusterApi`].
    GuardedCluster: ClusterApi {
        fn init(r: InitRequest) -> ClusterStatus => ClusterInit, no_target;
        fn join(r: NodeJoinRequest) -> NodeJoinResponse => ClusterJoin, no_target;
        fn status() -> ClusterStatus => ClusterStatus, no_target;
        fn create_node_token(r: NodeTokenRequest) -> NodeTokenResponse => CreateNodeToken, no_target;
        fn remove(id: u64, force: bool) -> ClusterStatus => ClusterRemove, node_id;
        fn drain(id: u64) -> ClusterStatus => ClusterDrain, node;
        fn reassign(r: ReassignRequest) -> ClusterStatus => ClusterReassign, |r: &ReassignRequest| format!("partition:{}", r.partition);
        fn rebalance() -> ClusterStatus => ClusterRebalance, no_target;
        fn upgrade_status() -> UpgradeStatus => UpgradeStatus, no_target;
        fn upgrade_finalize() -> UpgradeStatus => UpgradeFinalize, no_target;
        fn upgrade_hold() -> UpgradeStatus => UpgradeHold, no_target;
        fn upgrade_release() -> UpgradeStatus => UpgradeRelease, no_target;
    }
}

guarded! {
    /// The guarded Phase 13 [`ModuleLifecycleApi`].
    GuardedModuleLifecycle: ModuleLifecycleApi {
        fn retire_module(sha: String) -> RetireModuleResponse => RetireModule, text;
        fn run_module_gc() -> GcRunReport => RunModuleGc, no_target;
        fn migrate_runs(r: MigrateRunsRequest) -> MigrateRunsReport => MigrateRuns, |r: &MigrateRunsRequest| r.workflow.clone();
    }
}

guarded! {
    /// The guarded Phase 14 [`TenantsApi`] (14-17).
    GuardedTenants: TenantsApi {
        fn create_tenant(spec: TenantSpec) -> TenantInfo => CreateTenant, |s: &TenantSpec| s.id.clone();
        fn get_tenant(id: String) -> TenantInfo => GetTenant, text;
        fn list_tenants() -> Vec<TenantInfo> => ListTenants, no_target;
        fn update_tenant(id: String, patch: TenantPatch) -> TenantInfo => UpdateTenant, first;
        fn suspend_tenant(id: String) -> TenantInfo => SuspendTenant, text;
        fn resume_tenant(id: String) -> TenantInfo => ResumeTenant, text;
        fn delete_tenant(id: String) -> () => DeleteTenant, text;
    }
}

guarded! {
    /// The guarded Phase 14 [`ApiTokensApi`] (14-17).
    GuardedApiTokens: ApiTokensApi {
        fn create_token(req: CreateTokenRequest) -> CreatedToken => CreateApiToken, |r: &CreateTokenRequest| r.name.clone();
        fn list_tokens(tenant: Option<String>) -> Vec<TokenInfo> => ListApiTokens, opt_text;
        fn revoke_token(id: String) -> () => RevokeApiToken, text;
        fn rotate_token(id: String, req: RotateTokenRequest) -> CreatedToken => RotateApiToken, first;
        fn whoami() -> WhoAmI => WhoAmI, no_target;
    }
}

guarded! {
    /// The guarded Phase 14 [`AuditApi`] (14-18).
    GuardedAudit: AuditApi {
        fn query(q: AuditQuery) -> AuditPage => QueryAudit, no_target;
    }
}

guarded! {
    /// The guarded Phase 14 [`SecurityApi`] (14-19).
    GuardedSecurity: SecurityApi {
        fn get_module_policy() -> ModulePolicy => GetModulePolicy, no_target;
        fn set_module_policy(policy: ModulePolicy) -> ModulePolicy => SetModulePolicy, no_target;
        fn add_trusted_key(key: TrustedKeySpec) -> ModulePolicy => AddTrustedKey, |k: &TrustedKeySpec| k.key_id.clone();
        fn remove_trusted_key(key_id: String) -> ModulePolicy => RemoveTrustedKey, text;
        fn attach_signatures(req: AttachSignaturesRequest) -> AttachSignaturesResult => AttachSignatures, |r: &AttachSignaturesRequest| r.sha256.clone();
    }
}
