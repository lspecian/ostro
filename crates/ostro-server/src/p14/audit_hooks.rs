//! Phase 14 (14-18): audit capture through the guard (SEC-04, D-P14-11/12/13).
//!
//! # What is recorded, and how
//!
//! | Source | Call | Record |
//! |--------|------|--------|
//! | guard, audited method ([`classify`] is `Some`) | [`reserve`] before the inner call, [`after`] after it | one chained record per call: actor, tenant, action, target, outcome |
//! | guard, `authz::require` refused the call | [`denied`] | coalesced `authz.denied` per (token id, method) and `[audit] auth_failure_coalesce` window |
//! | bearer authentication failed (invalid, expired, missing, malformed) | [`AuditingAuthenticator`] | coalesced `auth.failure` per (token id or `malformed`/`missing`, client ip) and window |
//! | worker join / certificate renewal | [`worker_event`] | one chained record (actor = the worker id, kind `worker`) |
//!
//! **Fail closed (D-P14-11):** [`reserve`] takes a journal permit before the inner call. When
//! none is available within `[audit] reserve_timeout` (writer stalled, queue full, poisoned)
//! the guard answers 503 `audit_unavailable` (`retry_after_ms`) and never calls the inner
//! method. [`after`] commits the record and returns only once it is durable. If that commit
//! fails after the action executed (W8: fsync error), the answer is 500
//! `audit_commit_failed` with `details.executed = true`, the writer stays poisoned (later
//! audited calls are refused before acting) and the full record is logged at ERROR.
//!
//! Records never carry request bodies or secret values: the target is a run id, a
//! (qualified) resource name, a module sha, a token / key id, `node:<id>`, `partition:<n>`
//! or a policy scope, as computed by the guard's per-method target helper.
//!
//! **Exceptions to the inventory's `audited` column** (checked by a unit test):
//! `JoinWorker` is recorded by [`worker_event`] (the join service knows the join token id
//! and the worker id; the guard only sees the `System` principal of the join route), so
//! the guard does not record it a second time. Conversely the worker join service's
//! `RevokeToken` event is ignored here, because `revoke_worker_token` is only reachable
//! through the guard, which records it with the real actor.
//!
//! **Client ip:** the API layer does not expose the peer address to the authenticator, so
//! authentication failures are keyed and recorded with client ip `unknown` (see the 14-18
//! SUMMARY).

use std::sync::{Arc, Weak};

use ostro_api::auth::{AuthError, Authenticator, Method, Principal, PrincipalKind};
use ostro_api::{ApiError, ErrorCode, error::reason};
use ostro_types::audit::{
    ActorKind, AuditEvent, AuditOutcome, AuditPermit, AuditSink, CoalesceKey,
};
use tracing::{error, warn};

use super::{P14Ctx, WorkerAction, WorkerAuditEvent, tokens};

/// `details.retry_after_ms` of a 503 `audit_unavailable`.
pub const RETRY_AFTER_MS: u64 = 1_000;

/// Action of a coalesced authorization failure.
pub const AUTHZ_DENIED: &str = "authz.denied";

/// Action of a coalesced authentication failure.
pub const AUTH_FAILURE: &str = "auth.failure";

/// Client ip recorded for authentication failures (the API layer does not expose it).
pub const UNKNOWN_IP: &str = "unknown";

/// The audit action of an audited method; `None` for methods that are not audited (reads,
/// starts, signals, token completions, and `JoinWorker`, which [`worker_event`] records).
/// Exhaustive: a new [`Method`] does not compile until it is classified here.
pub const fn classify(m: Method) -> Option<&'static str> {
    use Method::*;
    let a = match m {
        // ---- ControlPlane ----
        UploadModule => "module.upload",
        ActivateWorkflow => "workflow.activate",
        CancelRun => "run.cancel",
        TerminateRun => "run.terminate",
        RetryRun => "run.retry",
        SetWorkflowPolicy => "workflow.policy.set",
        SetQueuePolicy => "queue.policy.set",
        PutSecret => "secret.set",
        DeleteSecret => "secret.delete",
        PutGrant => "grant.put",
        DeleteGrant => "grant.delete",
        PutConnection => "connection.put",
        DeleteConnection => "connection.delete",
        PutEgress => "egress.put",
        DeleteEgress => "egress.delete",
        PutQueueFlow => "queue.flow.set",
        PauseQueue => "queue.pause",
        ResumeQueue => "queue.resume",
        PauseWorkflow => "workflow.pause",
        ResumeWorkflow => "workflow.resume",
        PauseRun => "run.pause",
        ResumeRun => "run.resume",
        DrainWorker => "worker.drain",
        CreateWorkerToken => "worker.token.create",
        RevokeWorkerToken => "worker.token.revoke",
        GetModule | ListWorkflows | StartRun | GetRun | RunHistory | ListRuns | Ready
        | SignalRun | AwaitResult | CompleteToken | GetWorkflowPolicy | GetQueuePolicy
        | ListSecrets | ListGrants | GetConnection | ListConnections | GetEgress | ListEgress
        | PresignedObject | GetQueueFlow | QueueDemand | ListWorkers | ListWorkerTokens
        | DevTime | DevSkip | ReplayModule => return None,
        // Recorded by `worker_event` (token id + worker id), not by the guard.
        JoinWorker => return None,
        // ---- Phase 8 ----
        CreateSchedule => "schedule.create",
        UpdateSchedule => "schedule.update",
        DeleteSchedule => "schedule.delete",
        PauseSchedule => "schedule.pause",
        ResumeSchedule => "schedule.resume",
        TriggerSchedule => "schedule.trigger",
        CreateWebhook => "webhook.create",
        UpdateWebhook => "webhook.update",
        DeleteWebhook => "webhook.delete",
        Backup => "backup.download",
        ListSchedules | GetSchedule | ListWebhooks | GetWebhook | DeliverHook | RunLogs => {
            return None;
        }
        // ---- Phase 11 ----
        RerunRun => "run.rerun",
        RequeueDlq => "dlq.requeue",
        DismissDlq => "dlq.dismiss",
        CreateBulk => "bulk.create",
        CancelBulk => "bulk.cancel",
        SetFailurePolicy => "failure_policy.set",
        AnalyzeRerun | ListDlq | ListBulk | GetBulk | Limits | GetFailurePolicy | ListModules
        | ListQueues | ListPartitions => return None,
        // ---- Phase 13 ----
        ClusterInit => "cluster.init",
        ClusterJoin => "cluster.join",
        CreateNodeToken => "cluster.token.create",
        ClusterRemove => "cluster.remove",
        ClusterDrain => "cluster.drain",
        ClusterReassign => "cluster.reassign",
        ClusterRebalance => "cluster.rebalance",
        UpgradeFinalize => "cluster.upgrade.finalize",
        UpgradeHold => "cluster.upgrade.hold",
        UpgradeRelease => "cluster.upgrade.release",
        RetireModule => "module.retire",
        RunModuleGc => "module.gc",
        MigrateRuns => "run.migrate",
        ClusterStatus | UpgradeStatus => return None,
        // ---- Phase 14 ----
        CreateTenant => "tenant.create",
        UpdateTenant => "tenant.update",
        SuspendTenant => "tenant.suspend",
        ResumeTenant => "tenant.resume",
        DeleteTenant => "tenant.delete",
        CreateApiToken => "token.create",
        RevokeApiToken => "token.revoke",
        RotateApiToken => "token.rotate",
        SetModulePolicy => "security.policy.set",
        AddTrustedKey => "security.key.add",
        RemoveTrustedKey => "security.key.remove",
        AttachSignatures => "module.signatures.attach",
        GetTenant | ListTenants | ListApiTokens | WhoAmI | QueryAudit | GetModulePolicy => {
            return None;
        }
    };
    Some(a)
}

/// The journal `actor_kind` of a principal.
pub const fn actor_kind(k: PrincipalKind) -> ActorKind {
    match k {
        PrincipalKind::Anonymous => ActorKind::Anonymous,
        PrincipalKind::Token => ActorKind::Token,
        PrincipalKind::Dev => ActorKind::Dev,
        PrincipalKind::System => ActorKind::System,
        PrincipalKind::Node => ActorKind::Node,
    }
}

/// The tenant a call applies to: the principal's own tenant; for system-scope principals
/// the tenant prefix of a qualified target (`<tenant>/<name>`), if any.
fn tenant_of(p: &Principal, target: &str) -> Option<String> {
    if let Some(t) = &p.tenant {
        return Some(t.clone());
    }
    let (t, rest) = target.split_once('/')?;
    let valid = !t.is_empty()
        && t != "_"
        && !rest.is_empty()
        && t.bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    valid.then(|| t.to_owned())
}

/// The record of a guarded call.
fn event(
    p: &Principal,
    m: Method,
    action: &str,
    target: &str,
    outcome: AuditOutcome,
) -> AuditEvent {
    let mut ev = AuditEvent::new(action, p.token_id.clone(), actor_kind(p.kind), target);
    ev.tenant = tenant_of(p, target);
    ev.outcome = outcome;
    ev.details.insert("method".into(), m.name());
    ev
}

/// The 503 of an audited call that could not reserve a journal slot.
pub fn unavailable(why: &str) -> ApiError {
    ApiError::unavailable(format!("audit journal unavailable: {why}")).with_details(
        serde_json::json!({ "reason": reason::AUDIT_UNAVAILABLE, "retry_after_ms": RETRY_AFTER_MS }),
    )
}

/// Before an audited call: a writer permit (fail closed: 503 `audit_unavailable`).
/// `Ok(None)` for methods that are not audited. Called by the guard (`guard.rs`) after
/// `tenant_scope::before`, before the inner call.
pub async fn reserve(
    ctx: &P14Ctx,
    _p: &Principal,
    m: Method,
) -> Result<Option<AuditPermit>, ApiError> {
    if classify(m).is_none() {
        return Ok(None);
    }
    // `AuditSink::reserve` waits up to `[audit] reserve_timeout` itself (block_in_place on
    // the multi-thread runtime).
    match ctx.audit.reserve() {
        Ok(permit) => Ok(Some(permit)),
        Err(e) => {
            warn!(method = %m.name(), error = %e, "audited call refused: no audit permit");
            Err(unavailable(&e.0))
        }
    }
}

/// The outcome recorded for an inner result: refusals (`permission_denied`, e.g. a module
/// signature policy) are `Denied`, other errors `Failed(<code>)`.
fn recorded_outcome(outcome: AuditOutcome) -> AuditOutcome {
    match outcome {
        AuditOutcome::Failed(code) if code == ErrorCode::PermissionDenied.as_str() => {
            AuditOutcome::Denied(code)
        }
        other => other,
    }
}

/// After the call: commit the record (durable before the response). A commit failure after
/// the action executed is 500 `audit_commit_failed` with `details.executed = true`.
pub async fn after(
    _ctx: &P14Ctx,
    permit: Option<AuditPermit>,
    p: &Principal,
    m: Method,
    target: &str,
    outcome: AuditOutcome,
) -> Result<(), ApiError> {
    let (Some(permit), Some(action)) = (permit, classify(m)) else {
        return Ok(());
    };
    let ev = event(p, m, action, target, recorded_outcome(outcome));
    let record = format!("{ev:?}");
    match permit.commit(ev).await {
        Ok(()) => Ok(()),
        Err(e) => {
            error!(
                method = %m.name(),
                error = %e,
                record = %record,
                "audit commit failed after the action executed; the audit writer is poisoned"
            );
            Err(ApiError::internal(format!(
                "the action executed but its audit record could not be committed: {e}"
            ))
            .with_details(serde_json::json!({
                "reason": reason::AUDIT_COMMIT_FAILED,
                "executed": true,
            })))
        }
    }
}

/// An authorization failure from `authz::require` (the guard returns before [`reserve`]):
/// recorded coalesced per (token id, method). Only refusals of an identified principal are
/// recorded (`permission_denied`, or `unauthenticated` for an invalid system token);
/// `Anonymous` internal callers are not.
pub fn denied(ctx: &Weak<P14Ctx>, m: Method, err: &ApiError) {
    let p = ostro_api::auth::current();
    if p.kind == PrincipalKind::Anonymous
        || !matches!(
            err.code,
            ErrorCode::PermissionDenied | ErrorCode::Unauthenticated
        )
    {
        return;
    }
    let Some(ctx) = ctx.upgrade() else { return };
    let why = err.reason().unwrap_or(err.code.as_str()).to_owned();
    let mut ev = event(
        &p,
        m,
        AUTHZ_DENIED,
        &m.name(),
        AuditOutcome::Denied(why.clone()),
    );
    ev.details.insert("reason".into(), why);
    ctx.audit.record_coalesced(
        ev,
        CoalesceKey::AuthFailure {
            token_id: p.token_id.clone(),
            client_ip: format!("method:{}", m.name()),
        },
    );
}

/// A worker join / renew ([`super::worker_event`]): one chained record (actor = the worker
/// id, else the join token id; kind `worker`; target = the join token id). `RevokeToken` is
/// recorded by the guard (`worker.token.revoke`, real actor) and ignored here.
pub fn worker_event(ctx: &P14Ctx, ev: WorkerAuditEvent) {
    if ev.action == WorkerAction::RevokeToken {
        return;
    }
    let actor = ev
        .worker_id
        .clone()
        .filter(|w| !w.is_empty())
        .unwrap_or_else(|| ev.token_id.clone());
    let mut rec = AuditEvent::new(ev.action.as_str(), actor, ActorKind::Worker, &ev.token_id);
    rec.outcome = ev.outcome;
    if let Some(w) = &ev.worker_id {
        rec.details.insert("worker_id".into(), w.clone());
    }
    match ctx.audit.reserve() {
        Ok(permit) => {
            let fut = permit.commit(rec);
            let action = ev.action.as_str();
            // The join / renewal already happened (the seam is synchronous): the commit is
            // awaited off the caller.
            tokio::spawn(async move {
                if let Err(e) = fut.await {
                    error!(action, error = %e, "worker audit record could not be committed");
                }
            });
        }
        Err(e) => error!(
            action = ev.action.as_str(),
            token_id = %ev.token_id,
            error = %e,
            "worker audit record dropped: no audit permit"
        ),
    }
}

/// The coalescing token id of a failed bearer: its public id when it parses as an API
/// token, else `malformed` (`missing` without a bearer).
fn failure_token_id(bearer: Option<&str>) -> String {
    match bearer {
        None => "missing".to_owned(),
        Some(b) => match tokens::parse(b) {
            Ok((id, _)) => id.into_string(),
            Err(_) => "malformed".to_owned(),
        },
    }
}

/// The API authenticator with authentication failures recorded (coalesced per token id
/// and client ip, `[audit] auth_failure_coalesce`). Store outages (`Unavailable`, 503) are
/// not authentication failures and are not recorded.
pub struct AuditingAuthenticator {
    inner: Arc<dyn Authenticator>,
    sink: Arc<dyn AuditSink>,
}

impl std::fmt::Debug for AuditingAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditingAuthenticator")
            .finish_non_exhaustive()
    }
}

impl AuditingAuthenticator {
    /// Record `inner`'s failures to `sink`.
    pub fn new(inner: Arc<dyn Authenticator>, sink: Arc<dyn AuditSink>) -> Self {
        Self { inner, sink }
    }

    fn record(&self, bearer: Option<&str>, e: &AuthError) {
        let token_id = failure_token_id(bearer);
        let mut ev = AuditEvent::new(AUTH_FAILURE, token_id.clone(), ActorKind::Anonymous, "");
        ev.outcome = AuditOutcome::Denied(e.as_str().to_owned());
        ev.details.insert("reason".into(), e.as_str().to_owned());
        ev.details.insert("client_ip".into(), UNKNOWN_IP.to_owned());
        self.sink.record_coalesced(
            ev,
            CoalesceKey::AuthFailure {
                token_id,
                client_ip: UNKNOWN_IP.to_owned(),
            },
        );
    }
}

#[async_trait::async_trait]
impl Authenticator for AuditingAuthenticator {
    async fn authenticate(&self, bearer: Option<&str>) -> Result<Principal, AuthError> {
        let r = self.inner.authenticate(bearer).await;
        if let Err(e) = &r
            && !matches!(e, AuthError::Unavailable(..))
        {
            self.record(bearer, e);
        }
        r
    }
}

/// Wrap the API authenticator so its failures are audited (`authn::authenticator`).
pub fn auditing(inner: Arc<dyn Authenticator>, ctx: &P14Ctx) -> Arc<dyn Authenticator> {
    Arc::new(AuditingAuthenticator::new(inner, Arc::clone(&ctx.audit)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classification_follows_the_inventory() {
        // The documented exception: worker joins are recorded by `worker_event`.
        for &m in Method::all() {
            let want = m.spec().audited && m != Method::JoinWorker;
            assert_eq!(classify(m).is_some(), want, "{m:?}");
        }
        let mut names: Vec<&str> = Method::all().iter().filter_map(|&m| classify(m)).collect();
        let n = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), n, "action names are unique");
        assert!(names.iter().all(|a| a.contains('.') && !a.contains(' ')));
    }

    #[test]
    fn tenant_comes_from_the_principal_or_a_qualified_target() {
        let sys = Principal::system();
        assert_eq!(tenant_of(&sys, "acme/orders"), Some("acme".into()));
        assert_eq!(tenant_of(&sys, "_/orders"), None);
        assert_eq!(tenant_of(&sys, "orders"), None);
        assert_eq!(tenant_of(&sys, "Not A/tenant"), None);
        let t = Principal::token(
            "otk_a",
            Some("b".into()),
            ostro_api::auth::ScopeSet::admin(),
        );
        assert_eq!(tenant_of(&t, "x"), Some("b".into()));
    }

    #[test]
    fn denials_are_denied_outcomes() {
        assert_eq!(
            recorded_outcome(AuditOutcome::Failed("permission_denied".into())),
            AuditOutcome::Denied("permission_denied".into())
        );
        assert_eq!(
            recorded_outcome(AuditOutcome::Failed("not_found".into())),
            AuditOutcome::Failed("not_found".into())
        );
    }

    #[test]
    fn failure_ids_never_carry_the_secret() {
        assert_eq!(failure_token_id(None), "missing");
        assert_eq!(failure_token_id(Some("garbage")), "malformed");
        let (id, _secret, bearer) = tokens::generate_os().expect("generate");
        assert_eq!(failure_token_id(Some(bearer.expose())), id.as_str());
    }
}
