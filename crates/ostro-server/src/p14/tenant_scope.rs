//! Phase 14 (14-16): tenant scoping (SEC-02, D-P14-7, D-P14-9, D-P14-38) on the D23
//! qualified-name model.
//!
//! Called by the guard (`guard.rs`) for every guarded method: `before(p, m, inner, req)` with
//! the method's request (the single argument; `()` for none; a tuple for several, in
//! parameter order) and the routed control plane for lookups, and `after(p, m, out)` with the
//! inner call's result.
//!
//! # Who is scoped
//!
//! A **tenant principal** is one whose `tenant` is `Some(t)`. Everything below applies to
//! tenant principals only; system-scope principals (`tenant = None`: system tokens, `Dev`,
//! `System`, nodes) pass through unchanged and see every tenant with qualified names.
//!
//! **Default tenant (14-17).** A token of the default tenant (`tenant = Some("_")`) is a
//! tenant principal confined to the default tenant, never system scope: its names stay bare
//! (the default tenant's storage form), it owns only unqualified names and runs whose tenant
//! is `None`/`""`, and the request fields the server reads as "`None` = default tenant"
//! (`UploadModule`, `StartRunRequest`, the secret calls) are forced to `None` instead of
//! `"_"` (which the server refuses). Filter fields where `None` means "every tenant"
//! (`ListRunsRequest`, `list_tokens`, `AuditQuery`) and `CreateTokenRequest` keep `"_"`.
//!
//! # Rules by tenant class (one `match` over [`Method`], no wildcard)
//!
//! - **qualified names**: a client name must be a plain resource name (no `/`, `:` or
//!   leading `__`; otherwise 400 `invalid_name`, even when it carries the caller's own
//!   tenant) and is qualified to `<t>/<name>`; nested names are qualified too (schedule and
//!   webhook workflows, on-failure hook workflow/queue, bulk and DLQ filters, the replay
//!   sample workflow). Webhook `secret_ref`s are prefixed with `<t>/` (secret names may
//!   contain `/`; the reference always stays inside the tenant's namespace).
//! - **run ids**: the run's `RunInfo.tenant` (from `RunRecord.tenant`, `None`/`""` = the
//!   default tenant) must equal `t`, else 404 with the body of a genuinely missing run; the
//!   run is never touched. Await-token completion parses the token's run id (no MAC check
//!   here; the inner call verifies it) and applies the same rule.
//! - **modules by sha** (`get_module`, `replay_module`, `attach_signatures`,
//!   `retire_module`): visible iff the module registers a workflow in `t`, else 404;
//!   retiring a module that other tenants also registered is 403 `forbidden_tenant`.
//! - **tenant-carrying fields (B7)**: `UploadModule.tenant`, `StartRunRequest.tenant`,
//!   `ListRunsRequest.tenant`, `PutSecret.tenant`, the `list_secrets`/`delete_secret` tenant,
//!   `CreateTokenRequest.tenant`, the `list_tokens` tenant, `AuditQuery.tenant`,
//!   `TrustedKeySpec.tenant` and `ModulePolicy.tenant` (p14-fixups-2) are forced to `t` when empty and refused with 403
//!   `forbidden_tenant` when they name another tenant (`_` included). The bulk job tenant
//!   has no request field: `control::p11_bulk` stamps the job with the caller's tenant.
//! - **system-only methods**: 403 `forbidden_tenant` (14-15's scope check usually answers
//!   first with `missing_scope`).
//! - **connections** are cluster-scoped in 1.0 (the bridge resolves them by bare name for
//!   every tenant): tenant principals list none, `get` is 404, `put`/`delete` are 403.
//!
//! `after` strips `<t>/` from every name in a response (workflow, queue, module workflow
//! lists, schedule, webhook and its workflow / secret reference, grant module, egress
//! module, failure-policy hook) and drops any item of another tenant from lists and pages
//! (defence in depth: the inner listings are already tenant-filtered where they can be).
//!
//! The generic entry points only pass the request as `&mut dyn Any`; the per-method work is
//! non-generic (binary size, NFR-2.1). A request or response of an unexpected type is a
//! programming error and answers 500 (the `tenant_isolation` sweep calls every method).

use std::any::Any;

use ostro_api::auth::{Method, Principal};
use ostro_api::domain::{
    AwaitResultRequest, CancelRequest, CompleteTokenRequest, ConnectionInfo, EgressInfo, GrantInfo,
    HistoryRequest, ListRunsRequest, ModuleInfo, ReplayRequest, ReplayResult, ReplaySelect,
    ReplaySummary, RetryRequest, RunInfo, RunPage, SecretInfo, SignalRequest, StartRunRequest,
    TerminateRequest, UploadModuleResult, WorkflowInfo,
};
use ostro_api::error::reason;
use ostro_api::phase7::{DemandSnapshot, PauseState, QueueFlow, QueueFlowInfo};
use ostro_api::phase8::{HookRequest, RunLogsRequest, ScheduleDto, WebhookActionDto, WebhookDto};
use ostro_api::phase11::{
    BulkCreateRequest, DlqDismissRequest, DlqEntry, DlqListRequest, DlqPage, DlqRequeueRequest,
    FailurePolicy, ModulePage, PolicyScope, QueueView, RerunRequest, RerunResponse,
};
use ostro_api::phase13::{MigrateRunsRequest, RetireModuleResponse};
use ostro_api::phase14::{
    AttachSignaturesRequest, AuditPage, AuditQuery, CreateTokenRequest, ModulePolicy, TokenInfo,
    TrustedKeySpec,
};
use ostro_api::{ApiError, ControlPlane, ErrorCode};
use ostro_meta::is_resource_name;

use crate::control::names::{DEFAULT_TENANT_TOKEN, split_qualified};

/// The principal's tenant when it is a tenant principal (`None` = system scope).
pub fn tenant_of(p: &Principal) -> Option<&str> {
    p.tenant.as_deref().filter(|t| !t.is_empty())
}

/// Before the inner call: check/force the request's tenant for `p` (see the module docs).
pub async fn before<Req: Send + 'static>(
    p: &Principal,
    m: Method,
    inner: &dyn ControlPlane,
    mut req: Req,
) -> Result<Req, ApiError> {
    if let Some(t) = tenant_of(p) {
        scope_request(t, m, inner, &mut req).await?;
    }
    Ok(req)
}

/// After the inner call: filter/strip the response for `p` (see the module docs).
pub fn after<Resp: 'static>(
    p: &Principal,
    m: Method,
    out: Result<Resp, ApiError>,
) -> Result<Resp, ApiError> {
    match (tenant_of(p), out) {
        (Some(t), Ok(mut v)) => {
            scope_response(t, m, &mut v)?;
            Ok(v)
        }
        (_, out) => out,
    }
}

// ---------------------------------------------------------------------------------------
// Errors and small helpers
// ---------------------------------------------------------------------------------------

fn mismatch(m: Method, what: &str) -> ApiError {
    ApiError::internal(format!(
        "tenant_scope: unexpected {what} type for {}",
        m.name()
    ))
}

fn req_of<T: 'static>(m: Method, req: &mut (dyn Any + Send)) -> Result<&mut T, ApiError> {
    req.downcast_mut::<T>()
        .ok_or_else(|| mismatch(m, "request"))
}

fn resp_of<T: 'static>(m: Method, resp: &mut dyn Any) -> Result<&mut T, ApiError> {
    resp.downcast_mut::<T>()
        .ok_or_else(|| mismatch(m, "response"))
}

/// 403 `forbidden_tenant` naming the field (never the other tenant or its resources).
fn forbidden(field: &str) -> ApiError {
    ApiError::permission_denied(
        reason::FORBIDDEN_TENANT,
        format!("{field} names another tenant"),
    )
    .with_details(serde_json::json!({ "reason": reason::FORBIDDEN_TENANT, "field": field }))
}

/// 403 `forbidden_tenant` for a system-only method.
fn system_only(m: Method) -> ApiError {
    ApiError::permission_denied(
        reason::FORBIDDEN_TENANT,
        format!("{} is reserved to system principals", m.name()),
    )
}

/// 403 for the cluster-scoped connection writes.
fn connections_are_cluster_scoped() -> ApiError {
    ApiError::permission_denied(
        reason::FORBIDDEN_TENANT,
        "connections are cluster-scoped and managed by system principals",
    )
}

/// 400 `invalid_name`: a tenant request carries a name that is not a plain resource name.
fn invalid_name(field: &str, raw: &str) -> ApiError {
    ApiError::invalid_argument(format!(
        "{field} {raw:?} must be a plain name matching ^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$ \
         (no '/', no ':', no leading '__'; tenant tokens never qualify names)"
    ))
    .with_details(serde_json::json!({ "field": field, "reason": "invalid_name" }))
}

/// 14-17: a principal of the default tenant (`tenant = Some("_")`). It stays a tenant
/// principal (confined to the default tenant, never system scope), but the default tenant's
/// storage names are bare and its request tenant fields use the server's default spelling.
fn is_default(t: &str) -> bool {
    t == DEFAULT_TENANT_TOKEN
}

/// Whether a stored/listed tenant value `have` is tenant `t` (`None`, `""` and `_` are the
/// default tenant).
fn tenant_is(have: Option<&str>, t: &str) -> bool {
    match have.filter(|h| !h.is_empty()) {
        None => is_default(t),
        Some(h) => h == t,
    }
}

/// The plain name `raw`, qualified to `<t>/<raw>` (bare for the default tenant).
fn qualified(t: &str, field: &str, raw: &str) -> Result<String, ApiError> {
    if !is_resource_name(raw) {
        return Err(invalid_name(field, raw));
    }
    if is_default(t) {
        return Ok(raw.to_owned());
    }
    Ok(format!("{t}/{raw}"))
}

/// Qualify `name` in place.
fn q(t: &str, field: &str, name: &mut String) -> Result<(), ApiError> {
    *name = qualified(t, field, name)?;
    Ok(())
}

/// Qualify an optional name in place (absent or empty stays as it is).
fn q_opt(t: &str, field: &str, name: &mut Option<String>) -> Result<(), ApiError> {
    match name {
        Some(n) if !n.is_empty() => q(t, field, n),
        _ => Ok(()),
    }
}

/// D-P14-38: force an optional tenant field to `t`, or refuse another tenant.
fn force(t: &str, field: &str, v: &mut Option<String>) -> Result<(), ApiError> {
    match v.as_deref() {
        None | Some("") => {
            *v = Some(t.to_owned());
            Ok(())
        }
        Some(x) if x == t => Ok(()),
        Some(_) => Err(forbidden(field)),
    }
}

/// [`force`] for request fields where the server reads `None` as the default tenant (and
/// refuses `"_"`): a default-tenant principal's value becomes `None`.
fn force_default(t: &str, field: &str, v: &mut Option<String>) -> Result<(), ApiError> {
    if !is_default(t) {
        return force(t, field, v);
    }
    match v.as_deref() {
        None | Some("") | Some(DEFAULT_TENANT_TOKEN) => {
            *v = None;
            Ok(())
        }
        Some(_) => Err(forbidden(field)),
    }
}

/// Whether the stored (qualified) name belongs to `t`.
fn owns(t: &str, key: &str) -> bool {
    if is_default(t) {
        return split_qualified(key).0.is_none();
    }
    split_qualified(key).0 == Some(t)
}

/// `<t>/x` → `x` in place (other names are left alone).
fn strip(t: &str, name: &mut String) {
    if owns(t, name) {
        let local = split_qualified(name).1.to_owned();
        *name = local;
    }
}

fn strip_opt(t: &str, name: &mut Option<String>) {
    if let Some(n) = name {
        strip(t, n);
    }
}

/// A run's tenant equals `t` (`None` / `""` is the default tenant, never a tenant principal).
fn same_tenant(run_tenant: Option<&str>, t: &str) -> bool {
    tenant_is(run_tenant, t)
}

/// The run-id rule: `RunRecord.tenant == t`, else the 404 of a missing run.
async fn own_run(t: &str, inner: &dyn ControlPlane, run_id: &str) -> Result<(), ApiError> {
    match inner.get_run(run_id.to_owned()).await {
        Ok(info) if same_tenant(info.tenant.as_deref(), t) => Ok(()),
        Ok(_) => Err(crate::control::runs::not_found(run_id)),
        Err(e) if e.code == ErrorCode::NotFound => Err(crate::control::runs::not_found(run_id)),
        Err(e) => Err(e),
    }
}

/// The 404 of a missing module (the server's `get_module` body).
fn module_not_found(sha: &str) -> ApiError {
    ApiError::not_found(format!("module {sha} not found"))
}

/// Module visibility: the module registers a workflow in `t`. Returns the module.
async fn own_module(t: &str, inner: &dyn ControlPlane, sha: &str) -> Result<ModuleInfo, ApiError> {
    match inner.get_module(sha.to_owned()).await {
        Ok(info) if info.workflows.iter().any(|w| owns(t, &w.name)) => Ok(info),
        Ok(_) => Err(module_not_found(sha)),
        Err(e) if e.code == ErrorCode::NotFound => Err(module_not_found(sha)),
        Err(e) => Err(e),
    }
}

/// `secret:<module>:<secret>` with the module qualified (other ids are left for the inner
/// call to refuse).
fn qualify_grant_id(t: &str, id: &mut String) -> Result<(), ApiError> {
    let prefix = ostro_api::domain::GRANT_ID_PREFIX;
    if let Some(rest) = id.strip_prefix(prefix)
        && let Some((module, secret)) = rest.split_once(':')
    {
        let module = qualified(t, "id", module)?;
        *id = format!("{prefix}{module}:{secret}");
    }
    Ok(())
}

fn strip_grant(t: &str, g: &mut GrantInfo) {
    strip(t, &mut g.module);
    let prefix = ostro_api::domain::GRANT_ID_PREFIX;
    let own = format!("{prefix}{t}/");
    if let Some(rest) = g.id.strip_prefix(&own) {
        g.id = format!("{prefix}{rest}");
    }
}

fn q_policy_scope(t: &str, s: &mut PolicyScope) -> Result<(), ApiError> {
    match s {
        PolicyScope::Workflow(w) => q(t, "workflow", w),
        PolicyScope::Queue(qn) => q(t, "queue", qn),
    }
}

fn q_failure_policy(t: &str, p: &mut FailurePolicy) -> Result<(), ApiError> {
    if let Some(h) = &mut p.on_failure {
        q(t, "on_failure.workflow", &mut h.workflow)?;
        q_opt(t, "on_failure.queue", &mut h.queue)?;
    }
    Ok(())
}

fn strip_failure_policy(t: &str, p: &mut FailurePolicy) {
    if let Some(h) = &mut p.on_failure {
        strip(t, &mut h.workflow);
        strip_opt(t, &mut h.queue);
    }
}

fn q_schedule(t: &str, s: &mut ScheduleDto) -> Result<(), ApiError> {
    q(t, "name", &mut s.name)?;
    q(t, "workflow", &mut s.workflow)
}

fn strip_schedule(t: &str, s: &mut ScheduleDto) {
    strip(t, &mut s.name);
    strip(t, &mut s.workflow);
}

fn q_webhook(t: &str, w: &mut WebhookDto) -> Result<(), ApiError> {
    q(t, "name", &mut w.name)?;
    match &mut w.action {
        WebhookActionDto::Start { workflow, .. } | WebhookActionDto::Signal { workflow, .. } => {
            q(t, "workflow", workflow)?;
        }
        WebhookActionDto::CompleteToken { .. } => {}
    }
    if !w.verifier.secret_ref.is_empty() && !is_default(t) {
        w.verifier.secret_ref = format!("{t}/{}", w.verifier.secret_ref);
    }
    Ok(())
}

fn strip_webhook(t: &str, w: &mut WebhookDto) {
    strip(t, &mut w.name);
    match &mut w.action {
        WebhookActionDto::Start { workflow, .. } | WebhookActionDto::Signal { workflow, .. } => {
            strip(t, workflow);
        }
        WebhookActionDto::CompleteToken { .. } => {}
    }
    if let Some(rest) = w.verifier.secret_ref.strip_prefix(&format!("{t}/")) {
        w.verifier.secret_ref = rest.to_owned();
    }
}

/// Keep only `t`'s workflows of a module and strip them.
fn scope_module_info(t: &str, info: &mut ModuleInfo) {
    info.workflows.retain(|w| owns(t, &w.name));
    for w in &mut info.workflows {
        strip(t, &mut w.name);
    }
}

fn strip_run_info(t: &str, r: &mut RunInfo) {
    strip(t, &mut r.workflow);
    strip(t, &mut r.queue);
}

fn strip_dlq_entry(t: &str, e: &mut DlqEntry) {
    strip(t, &mut e.workflow);
    strip(t, &mut e.queue);
}

// ---------------------------------------------------------------------------------------
// Before
// ---------------------------------------------------------------------------------------

/// The per-method request rules for tenant `t` (one arm per [`Method`], no wildcard).
async fn scope_request(
    t: &str,
    m: Method,
    inner: &dyn ControlPlane,
    req: &mut (dyn Any + Send),
) -> Result<(), ApiError> {
    use Method::*;
    match m {
        // ---- ControlPlane ----
        UploadModule => {
            let r = req_of::<ostro_api::domain::UploadModule>(m, req)?;
            force_default(t, "tenant", &mut r.tenant)?;
            if let Some(n) = r.name.as_deref().filter(|n| !n.is_empty())
                && !is_resource_name(n)
            {
                return Err(invalid_name("name", n));
            }
        }
        GetModule => {
            let sha = req_of::<String>(m, req)?.clone();
            own_module(t, inner, &sha).await?;
        }
        ListWorkflows | ListConnections | ListEgress | ListSchedules | ListWebhooks | ListBulk
        | Limits | ListQueues | WhoAmI | DevTime => {
            req_of::<()>(m, req)?;
        }
        ActivateWorkflow => {
            let r = req_of::<ostro_api::domain::ActivateWorkflow>(m, req)?;
            q(t, "name", &mut r.name)?;
        }
        StartRun => {
            let r = req_of::<StartRunRequest>(m, req)?;
            force_default(t, "tenant", &mut r.tenant)?;
            q(t, "workflow", &mut r.workflow)?;
            q_opt(t, "queue", &mut r.queue)?;
        }
        GetRun | ResumeRun | AnalyzeRerun => {
            let id = req_of::<String>(m, req)?.clone();
            own_run(t, inner, &id).await?;
        }
        RunHistory => {
            let id = req_of::<HistoryRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        ListRuns => {
            let r = req_of::<ListRunsRequest>(m, req)?;
            force(t, "tenant", &mut r.tenant)?;
            q_opt(t, "workflow", &mut r.workflow)?;
            q_opt(t, "queue", &mut r.queue)?;
        }
        // Never reaches the hooks (the guard passes `ready` through).
        Ready => {}
        SignalRun => {
            let id = req_of::<SignalRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        CancelRun => {
            let id = req_of::<CancelRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        TerminateRun => {
            let id = req_of::<TerminateRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        RetryRun => {
            let id = req_of::<RetryRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        AwaitResult => {
            let id = req_of::<AwaitResultRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        CompleteToken => {
            let r = req_of::<CompleteTokenRequest>(m, req)?;
            // A token that does not parse is refused by the inner call (token_invalid).
            if let Ok(parts) = ostro_engine::token::parse(&r.token) {
                own_run(t, inner, &parts.run_id).await?;
            }
        }
        SetWorkflowPolicy => {
            let r = req_of::<(String, ostro_api::domain::RunPolicy)>(m, req)?;
            q(t, "workflow", &mut r.0)?;
        }
        SetQueuePolicy => {
            let r = req_of::<(String, ostro_api::domain::RunPolicy)>(m, req)?;
            q(t, "queue", &mut r.0)?;
        }
        GetWorkflowPolicy | PauseWorkflow | ResumeWorkflow => {
            q(t, "workflow", req_of::<String>(m, req)?)?;
        }
        GetQueuePolicy | GetQueueFlow | PauseQueue | ResumeQueue | QueueDemand => {
            q(t, "queue", req_of::<String>(m, req)?)?;
        }
        PutSecret => {
            let r = req_of::<ostro_api::domain::PutSecret>(m, req)?;
            force_default(t, "tenant", &mut r.tenant)?;
        }
        ListSecrets => {
            force_default(t, "tenant", req_of::<Option<String>>(m, req)?)?;
        }
        DeleteSecret => {
            let r = req_of::<(String, Option<String>)>(m, req)?;
            force_default(t, "tenant", &mut r.1)?;
        }
        PutGrant => {
            let g = req_of::<GrantInfo>(m, req)?;
            q(t, "module", &mut g.module)?;
            g.id = g.expected_id();
        }
        ListGrants => {
            q_opt(t, "module", req_of::<Option<String>>(m, req)?)?;
        }
        DeleteGrant => {
            qualify_grant_id(t, req_of::<String>(m, req)?)?;
        }
        PutConnection => {
            req_of::<ConnectionInfo>(m, req)?;
            return Err(connections_are_cluster_scoped());
        }
        DeleteConnection => {
            req_of::<String>(m, req)?;
            return Err(connections_are_cluster_scoped());
        }
        GetConnection => {
            let name = req_of::<String>(m, req)?;
            return Err(ApiError::not_found(format!(
                "connection {name:?} not found"
            )));
        }
        PutEgress => {
            q(t, "module", &mut req_of::<EgressInfo>(m, req)?.module)?;
        }
        GetEgress | DeleteEgress => {
            q(t, "module", req_of::<String>(m, req)?)?;
        }
        PresignedObject => {
            // Unscoped: the presign route's query signature is the credential (14-02).
            req_of::<ostro_api::domain::PresignedObjectRequest>(m, req)?;
        }
        PutQueueFlow => {
            let r = req_of::<(String, QueueFlow, Option<u64>)>(m, req)?;
            q(t, "queue", &mut r.0)?;
        }
        PauseRun => {
            let id = req_of::<(String, Option<String>)>(m, req)?.0.clone();
            own_run(t, inner, &id).await?;
        }
        ReplayModule => {
            let r = req_of::<ReplayRequest>(m, req)?;
            let sha = r.module_sha256.clone();
            let runs = match &mut r.select {
                ReplaySelect::Runs(ids) => ids.clone(),
                ReplaySelect::Sample { workflow, .. } => {
                    q_opt(t, "workflow", workflow)?;
                    Vec::new()
                }
            };
            own_module(t, inner, &sha).await?;
            for id in &runs {
                own_run(t, inner, id).await?;
            }
        }
        // ---- Phase 8 ----
        GetSchedule | PauseSchedule | ResumeSchedule | GetWebhook => {
            q(t, "name", req_of::<String>(m, req)?)?;
        }
        CreateSchedule | UpdateSchedule => {
            q_schedule(t, req_of::<ScheduleDto>(m, req)?)?;
        }
        DeleteSchedule | DeleteWebhook => {
            q(t, "name", &mut req_of::<(String, Option<u64>)>(m, req)?.0)?;
        }
        TriggerSchedule => {
            q(
                t,
                "name",
                &mut req_of::<(String, Option<String>)>(m, req)?.0,
            )?;
        }
        CreateWebhook | UpdateWebhook => {
            q_webhook(t, req_of::<WebhookDto>(m, req)?)?;
        }
        DeliverHook => {
            q(t, "name", &mut req_of::<HookRequest>(m, req)?.name)?;
        }
        RunLogs => {
            let id = req_of::<RunLogsRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        // ---- Phase 11 ----
        RerunRun => {
            let id = req_of::<RerunRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        ListDlq => {
            let r = req_of::<DlqListRequest>(m, req)?;
            q_opt(t, "workflow", &mut r.workflow)?;
            q_opt(t, "queue", &mut r.queue)?;
        }
        RequeueDlq => {
            let id = req_of::<DlqRequeueRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        DismissDlq => {
            let id = req_of::<DlqDismissRequest>(m, req)?.run_id.clone();
            own_run(t, inner, &id).await?;
        }
        CreateBulk => {
            let r = req_of::<BulkCreateRequest>(m, req)?;
            q_opt(t, "filter.workflow", &mut r.filter.workflow)?;
            q_opt(t, "filter.queue", &mut r.filter.queue)?;
        }
        GetBulk | CancelBulk => {
            // The job's tenant is checked by `control::p11_bulk` (404 for another tenant).
            req_of::<String>(m, req)?;
        }
        GetFailurePolicy => {
            q_policy_scope(t, req_of::<PolicyScope>(m, req)?)?;
        }
        SetFailurePolicy => {
            let r = req_of::<(PolicyScope, FailurePolicy)>(m, req)?;
            q_policy_scope(t, &mut r.0)?;
            q_failure_policy(t, &mut r.1)?;
        }
        ListModules => {
            req_of::<ostro_api::phase11::ListModulesRequest>(m, req)?;
        }
        // ---- Phase 13 ----
        RetireModule => {
            let sha = req_of::<String>(m, req)?.clone();
            let info = own_module(t, inner, &sha).await?;
            if info.workflows.iter().any(|w| !owns(t, &w.name)) {
                return Err(ApiError::permission_denied(
                    reason::FORBIDDEN_TENANT,
                    format!("module {sha} is also registered outside this tenant"),
                ));
            }
        }
        MigrateRuns => {
            q(
                t,
                "workflow",
                &mut req_of::<MigrateRunsRequest>(m, req)?.workflow,
            )?;
        }
        // ---- Phase 14 ----
        CreateApiToken => {
            force(
                t,
                "tenant",
                &mut req_of::<CreateTokenRequest>(m, req)?.tenant,
            )?;
        }
        ListApiTokens => {
            force(t, "tenant", req_of::<Option<String>>(m, req)?)?;
        }
        RevokeApiToken | RemoveTrustedKey => {
            // The token / key record's tenant is checked by its owner (14-17 / 14-19).
            req_of::<String>(m, req)?;
        }
        RotateApiToken => {
            req_of::<(String, ostro_api::phase14::RotateTokenRequest)>(m, req)?;
        }
        QueryAudit => {
            force(t, "tenant", &mut req_of::<AuditQuery>(m, req)?.tenant)?;
        }
        AddTrustedKey => {
            force(t, "tenant", &mut req_of::<TrustedKeySpec>(m, req)?.tenant)?;
        }
        // p14-fixups-2: a tenant principal sets its own tenant's requirement only.
        SetModulePolicy => {
            force(t, "tenant", &mut req_of::<ModulePolicy>(m, req)?.tenant)?;
        }
        AttachSignatures => {
            let sha = req_of::<AttachSignaturesRequest>(m, req)?.sha256.clone();
            own_module(t, inner, &sha).await?;
        }
        // ---- system only ----
        ListWorkers | DrainWorker | CreateWorkerToken | ListWorkerTokens | RevokeWorkerToken
        | JoinWorker | DevSkip | Backup | ListPartitions | ClusterInit | ClusterJoin
        | ClusterStatus | CreateNodeToken | ClusterRemove | ClusterDrain | ClusterReassign
        | ClusterRebalance | UpgradeStatus | UpgradeFinalize | UpgradeHold | UpgradeRelease
        | RunModuleGc | CreateTenant | GetTenant | ListTenants | UpdateTenant | SuspendTenant
        | ResumeTenant | DeleteTenant | GetModulePolicy | GetSealingStatus | Reseal => {
            return Err(system_only(m));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------
// After
// ---------------------------------------------------------------------------------------

/// The per-method response rules for tenant `t` (one arm per [`Method`], no wildcard).
fn scope_response(t: &str, m: Method, resp: &mut dyn Any) -> Result<(), ApiError> {
    use Method::*;
    match m {
        UploadModule => {
            scope_module_info(t, &mut resp_of::<UploadModuleResult>(m, resp)?.module);
        }
        GetModule => scope_module_info(t, resp_of::<ModuleInfo>(m, resp)?),
        ListWorkflows => {
            let v = resp_of::<Vec<WorkflowInfo>>(m, resp)?;
            v.retain(|w| owns(t, &w.name));
            for w in v {
                strip(t, &mut w.name);
            }
        }
        ActivateWorkflow => strip(t, &mut resp_of::<WorkflowInfo>(m, resp)?.name),
        GetRun => strip_run_info(t, resp_of::<RunInfo>(m, resp)?),
        ListRuns => {
            let page = resp_of::<RunPage>(m, resp)?;
            page.runs.retain(|r| owns(t, &r.workflow));
            for r in &mut page.runs {
                strip(t, &mut r.workflow);
                strip(t, &mut r.queue);
            }
        }
        ListSecrets => {
            resp_of::<Vec<SecretInfo>>(m, resp)?.retain(|s| tenant_is(s.tenant.as_deref(), t));
        }
        PutGrant => strip_grant(t, resp_of::<GrantInfo>(m, resp)?),
        ListGrants => {
            let v = resp_of::<Vec<GrantInfo>>(m, resp)?;
            v.retain(|g| owns(t, &g.module));
            for g in v {
                strip_grant(t, g);
            }
        }
        ListConnections => resp_of::<Vec<ConnectionInfo>>(m, resp)?.clear(),
        GetConnection => {
            resp_of::<ConnectionInfo>(m, resp)?;
        }
        PutEgress | GetEgress => strip(t, &mut resp_of::<EgressInfo>(m, resp)?.module),
        ListEgress => {
            let v = resp_of::<Vec<EgressInfo>>(m, resp)?;
            v.retain(|e| owns(t, &e.module));
            for e in v {
                strip(t, &mut e.module);
            }
        }
        GetQueueFlow | PutQueueFlow => strip(t, &mut resp_of::<QueueFlowInfo>(m, resp)?.queue),
        PauseQueue | ResumeQueue | PauseWorkflow | ResumeWorkflow => {
            strip(t, &mut resp_of::<PauseState>(m, resp)?.name);
        }
        QueueDemand => strip(t, &mut resp_of::<DemandSnapshot>(m, resp)?.queue),
        ReplayModule => {
            let r = resp_of::<ReplayResult>(m, resp)?;
            r.verdicts.retain(|v| owns(t, &v.workflow));
            for v in &mut r.verdicts {
                strip(t, &mut v.workflow);
            }
            r.summary = ReplaySummary::of(&r.verdicts);
        }
        ListSchedules => {
            let v = resp_of::<Vec<ScheduleDto>>(m, resp)?;
            v.retain(|s| owns(t, &s.name));
            for s in v {
                strip_schedule(t, s);
            }
        }
        GetSchedule | CreateSchedule | UpdateSchedule | PauseSchedule | ResumeSchedule => {
            strip_schedule(t, resp_of::<ScheduleDto>(m, resp)?);
        }
        ListWebhooks => {
            let v = resp_of::<Vec<WebhookDto>>(m, resp)?;
            v.retain(|w| owns(t, &w.name));
            for w in v {
                strip_webhook(t, w);
            }
        }
        GetWebhook | CreateWebhook | UpdateWebhook => {
            strip_webhook(t, resp_of::<WebhookDto>(m, resp)?);
        }
        RerunRun => strip(t, &mut resp_of::<RerunResponse>(m, resp)?.workflow),
        ListDlq => {
            let page = resp_of::<DlqPage>(m, resp)?;
            page.entries.retain(|e| owns(t, &e.workflow));
            for e in &mut page.entries {
                strip_dlq_entry(t, e);
            }
        }
        DismissDlq => strip_dlq_entry(t, resp_of::<DlqEntry>(m, resp)?),
        GetFailurePolicy | SetFailurePolicy => {
            strip_failure_policy(t, resp_of::<FailurePolicy>(m, resp)?);
        }
        ListModules => {
            let page = resp_of::<ModulePage>(m, resp)?;
            for s in &mut page.modules {
                s.workflows.retain(|w| owns(t, w));
                for w in &mut s.workflows {
                    strip(t, w);
                }
            }
            page.modules.retain(|s| !s.workflows.is_empty());
        }
        ListQueues => {
            let v = resp_of::<Vec<QueueView>>(m, resp)?;
            v.retain(|qv| owns(t, &qv.name));
            for qv in v {
                strip(t, &mut qv.name);
            }
        }
        RetireModule => {
            let r = resp_of::<RetireModuleResponse>(m, resp)?;
            r.versions.retain(|v| owns(t, &v.name));
            for v in &mut r.versions {
                strip(t, &mut v.name);
            }
        }
        ListApiTokens => {
            resp_of::<Vec<TokenInfo>>(m, resp)?.retain(|k| tenant_is(k.tenant.as_deref(), t));
        }
        QueryAudit => {
            resp_of::<AuditPage>(m, resp)?
                .records
                .retain(|r| tenant_is(r.tenant.as_deref(), t));
        }
        // Responses with no tenant data (ids, statuses, counts, policies, streams).
        StartRun | RunHistory | Ready | SignalRun | CancelRun | TerminateRun | RetryRun
        | AwaitResult | CompleteToken | SetWorkflowPolicy | GetWorkflowPolicy | SetQueuePolicy
        | GetQueuePolicy | PutSecret | DeleteSecret | DeleteGrant | PutConnection
        | DeleteConnection | DeleteEgress | PresignedObject | PauseRun | ResumeRun
        | ListWorkers | DrainWorker | CreateWorkerToken | ListWorkerTokens | RevokeWorkerToken
        | JoinWorker | DevTime | DevSkip | DeleteSchedule | TriggerSchedule | DeleteWebhook
        | DeliverHook | RunLogs | Backup | AnalyzeRerun | RequeueDlq | CreateBulk | ListBulk
        | GetBulk | CancelBulk | Limits | ListPartitions | ClusterInit | ClusterJoin
        | ClusterStatus | CreateNodeToken | ClusterRemove | ClusterDrain | ClusterReassign
        | ClusterRebalance | UpgradeStatus | UpgradeFinalize | UpgradeHold | UpgradeRelease
        | RunModuleGc | MigrateRuns | CreateTenant | GetTenant | ListTenants | UpdateTenant
        | SuspendTenant | ResumeTenant | DeleteTenant | CreateApiToken | RevokeApiToken
        | RotateApiToken | WhoAmI | GetModulePolicy | SetModulePolicy | AddTrustedKey
        | RemoveTrustedKey | AttachSignatures | GetSealingStatus | Reseal => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wf(name: &str) -> WorkflowInfo {
        WorkflowInfo {
            name: name.into(),
            active_version: None,
            versions: vec![],
            record_version: 0,
        }
    }

    #[test]
    fn default_tenant_principal_keeps_bare_names_and_none_fields() {
        // 14-17: `_` is a tenant principal confined to the default tenant.
        assert_eq!(qualified("_", "workflow", "orders").unwrap(), "orders");
        assert!(qualified("_", "workflow", "acme/orders").is_err());
        assert!(owns("_", "orders"));
        assert!(!owns("_", "acme/orders"));
        assert!(!owns("acme", "orders"));
        assert!(same_tenant(None, "_") && same_tenant(Some(""), "_"));
        assert!(!same_tenant(Some("acme"), "_"));
        assert!(!same_tenant(None, "acme"));
        for start in [None, Some(String::new()), Some("_".to_owned())] {
            let mut v = start;
            force_default("_", "tenant", &mut v).expect("default");
            assert_eq!(v, None);
        }
        let mut v = Some("acme".to_owned());
        let e = force_default("_", "tenant", &mut v).unwrap_err();
        assert_eq!(e.reason(), Some(reason::FORBIDDEN_TENANT));
        let mut v = None;
        force_default("acme", "tenant", &mut v).expect("tenant");
        assert_eq!(v.as_deref(), Some("acme"));
        let mut v = None;
        force("_", "tenant", &mut v).expect("filter field");
        assert_eq!(v.as_deref(), Some("_"));
    }

    #[test]
    fn force_fills_accepts_own_and_refuses_others() {
        let mut v = None;
        force("a", "tenant", &mut v).expect("fill");
        assert_eq!(v.as_deref(), Some("a"));
        let mut v = Some(String::new());
        force("a", "tenant", &mut v).expect("fill empty");
        assert_eq!(v.as_deref(), Some("a"));
        let mut v = Some("a".to_owned());
        force("a", "tenant", &mut v).expect("own");
        for other in ["b", "_", "zz"] {
            let mut v = Some(other.to_owned());
            let e = force("a", "tenant", &mut v).expect_err(other);
            assert_eq!(e.code, ErrorCode::PermissionDenied);
            assert_eq!(e.reason(), Some(reason::FORBIDDEN_TENANT));
            assert!(!e.message.contains(other), "{}", e.message);
        }
    }

    #[test]
    fn names_are_plain_then_qualified_and_stripped() {
        assert_eq!(qualified("a", "workflow", "w").unwrap(), "a/w");
        for bad in ["a/w", "b/w", "x:y", "__z", "", "a b"] {
            let e = qualified("a", "workflow", bad).expect_err(bad);
            assert_eq!(e.code, ErrorCode::InvalidArgument);
            assert_eq!(e.reason(), Some("invalid_name"));
        }
        let mut n = "a/w".to_owned();
        strip("a", &mut n);
        assert_eq!(n, "w");
        let mut n = "b/w".to_owned();
        strip("a", &mut n);
        assert_eq!(n, "b/w");
        assert!(owns("a", "a/w") && !owns("a", "w") && !owns("a", "b/w"));
    }

    #[test]
    fn grant_ids_qualify_the_module() {
        let mut id = "secret:mod:s".to_owned();
        qualify_grant_id("a", &mut id).unwrap();
        assert_eq!(id, "secret:a/mod:s");
        let mut id = "secret:b/mod:s".to_owned();
        assert!(qualify_grant_id("a", &mut id).is_err());
        let mut g = GrantInfo {
            id: "secret:a/mod:s".into(),
            module: "a/mod".into(),
            secret: "s".into(),
            origins: vec![],
            body: false,
        };
        strip_grant("a", &mut g);
        assert_eq!((g.id.as_str(), g.module.as_str()), ("secret:mod:s", "mod"));
    }

    #[test]
    fn system_scope_passes_through_and_tenants_are_filtered() {
        let p = Principal::system();
        let out: Result<Vec<WorkflowInfo>, ApiError> = Ok(vec![wf("b/w")]);
        assert_eq!(after(&p, Method::ListWorkflows, out).unwrap().len(), 1);
        let p = Principal::token(
            "otk_x",
            Some("a".into()),
            ostro_api::auth::ScopeSet::admin(),
        );
        let out: Result<Vec<WorkflowInfo>, ApiError> = Ok(vec![wf("b/w"), wf("a/w"), wf("w")]);
        let v = after(&p, Method::ListWorkflows, out).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "w");
        // A wrong response type is a 500, never a silent pass.
        let e = after(&p, Method::ListWorkflows, Ok(7u32)).unwrap_err();
        assert_eq!(e.code, ErrorCode::Internal);
    }
}
