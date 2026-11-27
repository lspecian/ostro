//! Phase 14 (14-15): scope enforcement (SEC-02, D-P14-3).
//!
//! [`required`] is the method → requirement table: one arm per [`Method`] variant (no
//! wildcard, so a new method does not build until it declares its requirement). It follows
//! the 14-01 inventory ([`Method::spec`]): the scope column, with the tenant class
//! `system_only` / `tenant_admin` deciding whether an `admin` (or `read`) method also needs
//! system authority. The route × scope matrix test (`tests/auth_scopes.rs`) checks every REST
//! route and gRPC method against it and exports the matrix (`OSTRO_EXPORT_AUTH_MATRIX`).
//!
//! [`require`] is step 2 of the guard (`p14/guard.rs`, after the `Anonymous` refusal):
//!
//! | Principal | Rule |
//! |-----------|------|
//! | `Dev`, `System` | always allowed (mode none; webhooks, joins, internal callers) |
//! | `Node` | only the read-class methods ([`node_api_method`]) |
//! | `Anonymous` | 401 (the guard refuses it first; kept here as defence in depth) |
//! | `Token`, tenant `None`, scopes other than `admin` / `read` | 401 `invalid_token` (such tokens cannot be created) |
//! | `Token` | per [`Requirement`]: `admin` covers every scope, no other implication |
//!
//! A denial counts `ostro_auth_denied_total{reason}` and answers 403 / `PERMISSION_DENIED`
//! with `details = {reason: "missing_scope", method, scope}` (plus `authority: "system"` when
//! the method needs a tenant-less principal). Authorization happens once, at the ingress
//! node: forwarded calls are never re-authorized from their `ostro-principal` header
//! (`p14/principal_fwd.rs`).

use ostro_api::ApiError;
use ostro_api::auth::{Method, Principal, PrincipalKind, Scope, ScopeSet};
use ostro_api::error::reason;

/// What a method needs from a token principal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Requirement {
    /// The scope, on any tenant (`admin` covers every scope).
    Scope(Scope),
    /// The scope with system authority (`tenant = None`): system-wide status reads.
    SystemScope(Scope),
    /// `admin` with system authority (`tenant = None`): tenants, cluster, workers, backups,
    /// cluster security policy.
    SystemAdmin,
    /// `admin` on any tenant (tenant administration; the tenant hook narrows it to the
    /// principal's own tenant, 14-16).
    TenantAdmin,
    /// Any authenticated principal (`whoami`, the readiness probe).
    Authenticated,
}

impl Requirement {
    /// Stable name (auth matrix).
    pub fn as_str(self) -> String {
        match self {
            Requirement::Scope(s) => s.as_str().to_owned(),
            Requirement::SystemScope(s) => format!("system {}", s.as_str()),
            Requirement::SystemAdmin => "system admin".to_owned(),
            Requirement::TenantAdmin => "tenant admin".to_owned(),
            Requirement::Authenticated => "authenticated".to_owned(),
        }
    }

    /// The scope named in a denial.
    fn scope(self) -> Scope {
        match self {
            Requirement::Scope(s) | Requirement::SystemScope(s) => s,
            Requirement::SystemAdmin | Requirement::TenantAdmin => Scope::Admin,
            Requirement::Authenticated => Scope::Read,
        }
    }

    /// Whether the requirement needs a tenant-less principal.
    fn system(self) -> bool {
        matches!(self, Requirement::SystemScope(_) | Requirement::SystemAdmin)
    }

    /// Whether a token with `scopes` on `tenant` satisfies the requirement.
    pub fn allows(self, scopes: ScopeSet, tenant: Option<&str>) -> bool {
        match self {
            Requirement::Scope(s) => scopes.allows(s),
            Requirement::SystemScope(s) => tenant.is_none() && scopes.allows(s),
            Requirement::SystemAdmin => tenant.is_none() && scopes.contains(Scope::Admin),
            Requirement::TenantAdmin => scopes.contains(Scope::Admin),
            Requirement::Authenticated => true,
        }
    }
}

/// The requirement of `m` (D-P14-3; the 14-01 inventory's scope and tenant class).
pub const fn required(m: Method) -> Requirement {
    use Requirement::{Authenticated, Scope as S, SystemAdmin, SystemScope, TenantAdmin};
    use Scope::{Deploy, Operate, Read, Trigger};
    match m {
        // ---- ControlPlane ----
        Method::UploadModule => S(Deploy),
        Method::GetModule => S(Read),
        Method::ListWorkflows => S(Read),
        Method::ActivateWorkflow => S(Deploy),
        Method::StartRun => S(Trigger),
        Method::GetRun => S(Read),
        Method::RunHistory => S(Read),
        Method::ListRuns => S(Read),
        Method::Ready => Authenticated,
        Method::SignalRun => S(Trigger),
        Method::CancelRun => S(Operate),
        Method::TerminateRun => S(Operate),
        Method::RetryRun => S(Operate),
        Method::AwaitResult => S(Read),
        Method::CompleteToken => S(Trigger),
        Method::SetWorkflowPolicy => S(Deploy),
        Method::GetWorkflowPolicy => S(Read),
        Method::SetQueuePolicy => S(Operate),
        Method::GetQueuePolicy => S(Read),
        Method::PutSecret => TenantAdmin,
        Method::ListSecrets => TenantAdmin,
        Method::DeleteSecret => TenantAdmin,
        Method::PutGrant => TenantAdmin,
        Method::ListGrants => TenantAdmin,
        Method::DeleteGrant => TenantAdmin,
        // Connections are cluster-scoped in 1.0 (14-16 policy; 14-17 aligns authz): system
        // admins only, so tenant admins get `missing_scope` here instead of passing the scope
        // check and being refused by the tenant hook.
        Method::PutConnection => SystemAdmin,
        Method::GetConnection => SystemAdmin,
        Method::ListConnections => SystemAdmin,
        Method::DeleteConnection => SystemAdmin,
        Method::PutEgress => TenantAdmin,
        Method::GetEgress => TenantAdmin,
        Method::ListEgress => TenantAdmin,
        Method::DeleteEgress => TenantAdmin,
        Method::PresignedObject => S(Read),
        Method::GetQueueFlow => S(Read),
        Method::PutQueueFlow => S(Operate),
        Method::PauseQueue => S(Operate),
        Method::ResumeQueue => S(Operate),
        Method::PauseWorkflow => S(Operate),
        Method::ResumeWorkflow => S(Operate),
        Method::PauseRun => S(Operate),
        Method::ResumeRun => S(Operate),
        Method::QueueDemand => S(Read),
        Method::ListWorkers => SystemScope(Read),
        Method::DrainWorker => SystemAdmin,
        Method::CreateWorkerToken => SystemAdmin,
        Method::ListWorkerTokens => SystemAdmin,
        Method::RevokeWorkerToken => SystemAdmin,
        Method::JoinWorker => SystemAdmin,
        Method::DevTime => S(Read),
        Method::DevSkip => SystemAdmin,
        Method::ReplayModule => S(Read),
        // ---- Phase 8 ----
        Method::ListSchedules => S(Read),
        Method::GetSchedule => S(Read),
        Method::CreateSchedule => TenantAdmin,
        Method::UpdateSchedule => TenantAdmin,
        Method::DeleteSchedule => TenantAdmin,
        Method::PauseSchedule => S(Operate),
        Method::ResumeSchedule => S(Operate),
        Method::TriggerSchedule => S(Operate),
        Method::ListWebhooks => S(Read),
        Method::GetWebhook => S(Read),
        Method::CreateWebhook => TenantAdmin,
        Method::UpdateWebhook => TenantAdmin,
        Method::DeleteWebhook => TenantAdmin,
        Method::DeliverHook => S(Trigger),
        Method::RunLogs => S(Read),
        Method::Backup => SystemAdmin,
        // ---- Phase 11 ----
        Method::RerunRun => S(Operate),
        Method::AnalyzeRerun => S(Read),
        Method::ListDlq => S(Read),
        Method::RequeueDlq => S(Operate),
        Method::DismissDlq => S(Operate),
        Method::CreateBulk => S(Operate),
        Method::ListBulk => S(Read),
        Method::GetBulk => S(Read),
        Method::CancelBulk => S(Operate),
        Method::Limits => S(Read),
        Method::GetFailurePolicy => S(Read),
        Method::SetFailurePolicy => S(Deploy),
        Method::ListModules => S(Read),
        Method::ListQueues => S(Read),
        Method::ListPartitions => SystemScope(Read),
        // ---- Phase 13 ----
        Method::ClusterInit => SystemAdmin,
        Method::ClusterJoin => SystemAdmin,
        Method::ClusterStatus => SystemScope(Read),
        Method::CreateNodeToken => SystemAdmin,
        Method::ClusterRemove => SystemAdmin,
        Method::ClusterDrain => SystemAdmin,
        Method::ClusterReassign => SystemAdmin,
        Method::ClusterRebalance => SystemAdmin,
        Method::UpgradeStatus => SystemScope(Read),
        Method::UpgradeFinalize => SystemAdmin,
        Method::UpgradeHold => SystemAdmin,
        Method::UpgradeRelease => SystemAdmin,
        Method::RetireModule => S(Deploy),
        Method::RunModuleGc => SystemAdmin,
        Method::MigrateRuns => S(Deploy),
        // ---- Phase 14 ----
        Method::CreateTenant => SystemAdmin,
        Method::GetTenant => SystemScope(Read),
        Method::ListTenants => SystemScope(Read),
        Method::UpdateTenant => SystemAdmin,
        Method::SuspendTenant => SystemAdmin,
        Method::ResumeTenant => SystemAdmin,
        Method::DeleteTenant => SystemAdmin,
        Method::CreateApiToken => TenantAdmin,
        Method::ListApiTokens => TenantAdmin,
        Method::RevokeApiToken => TenantAdmin,
        Method::RotateApiToken => TenantAdmin,
        Method::WhoAmI => Authenticated,
        Method::QueryAudit => TenantAdmin,
        Method::GetModulePolicy => SystemScope(Read),
        Method::SetModulePolicy => SystemAdmin,
        Method::AddTrustedKey => TenantAdmin,
        Method::RemoveTrustedKey => TenantAdmin,
        Method::AttachSignatures => S(Deploy),
    }
}

/// The methods a `Node` principal may call: the read class (fan-out legs and status
/// probes). Forwarded calls carry the original caller's principal, not `Node`, and the node
/// API on the cluster listener is not guarded, so this is defence in depth only.
pub const fn node_api_method(m: Method) -> bool {
    matches!(
        required(m),
        Requirement::Scope(Scope::Read)
            | Requirement::SystemScope(Scope::Read)
            | Requirement::Authenticated
    )
}

/// Whether a tenant-less token's scopes are well-formed (D-P14-3: `tenant = None` only with
/// `admin` or `read`).
fn tenantless_ok(scopes: ScopeSet) -> bool {
    scopes
        .iter()
        .all(|s| matches!(s, Scope::Admin | Scope::Read))
}

fn denied(reason: &'static str) {
    metrics::counter!("ostro_auth_denied_total", "reason" => reason).increment(1);
}

fn missing_scope(m: Method, r: Requirement) -> ApiError {
    denied(reason::MISSING_SCOPE);
    let name = m.name();
    let scope = r.scope().as_str();
    let mut details = serde_json::json!({
        "reason": reason::MISSING_SCOPE,
        "method": name,
        "scope": scope,
    });
    let message = if r.system() {
        details["authority"] = serde_json::Value::from("system");
        format!("{name} needs the {scope:?} scope on a system (tenant-less) token")
    } else {
        format!("{name} needs the {scope:?} scope")
    };
    ApiError::permission_denied(reason::MISSING_SCOPE, message).with_details(details)
}

/// Whether `p` may call `m` (see the module docs).
pub fn require(p: &Principal, m: Method) -> Result<(), ApiError> {
    let r = required(m);
    match p.kind {
        PrincipalKind::Dev | PrincipalKind::System => Ok(()),
        PrincipalKind::Anonymous => {
            denied(reason::UNAUTHENTICATED);
            Err(ApiError::unauthenticated("authentication required"))
        }
        PrincipalKind::Node => {
            if node_api_method(m) {
                Ok(())
            } else {
                Err(missing_scope(m, r))
            }
        }
        PrincipalKind::Token => {
            if p.tenant.is_none() && !tenantless_ok(p.scopes) {
                denied("invalid_token");
                return Err(ApiError::unauthenticated(
                    "the token is invalid (a system token may only hold admin or read)",
                )
                .with_details(serde_json::json!({ "reason": "invalid_token" })));
            }
            if r.allows(p.scopes, p.tenant.as_deref()) {
                Ok(())
            } else {
                Err(missing_scope(m, r))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use ostro_api::auth::TenantClass;
    use ostro_api::error::ErrorCode;

    use super::*;

    /// The table agrees with the 14-01 inventory: the scope column, and the tenant class
    /// decides system authority.
    #[test]
    fn table_follows_the_inventory() {
        for &m in Method::all() {
            let spec = m.spec();
            let want = match (m, spec.scope, spec.tenant) {
                (Method::Ready | Method::WhoAmI, _, _) => Requirement::Authenticated,
                // 14-17: connections are cluster-scoped in 1.0 (the inventory classes them
                // tenant-admin; the 14-16 policy makes them system-only).
                (
                    Method::PutConnection
                    | Method::GetConnection
                    | Method::ListConnections
                    | Method::DeleteConnection,
                    _,
                    _,
                ) => Requirement::SystemAdmin,
                (_, Scope::Admin, TenantClass::SystemOnly) => Requirement::SystemAdmin,
                (_, Scope::Admin, TenantClass::TenantAdmin) => Requirement::TenantAdmin,
                (_, s, TenantClass::SystemOnly) => Requirement::SystemScope(s),
                (_, s, _) => Requirement::Scope(s),
            };
            assert_eq!(required(m), want, "{}", m.name());
        }
    }

    fn tok(tenant: Option<&str>, scopes: &[Scope]) -> Principal {
        Principal::token("otk_t", tenant.map(str::to_owned), ScopeSet::of(scopes))
    }

    #[test]
    fn require_rules() {
        let t = Some("_");
        // Dev and System pass everything; Anonymous nothing.
        for &m in Method::all() {
            assert!(require(&Principal::dev(), m).is_ok());
            assert!(require(&Principal::system(), m).is_ok());
            assert_eq!(
                require(&Principal::anonymous(), m).unwrap_err().code,
                ErrorCode::Unauthenticated
            );
        }
        // Admin covers every scope; no other implication.
        assert!(require(&tok(t, &[Scope::Admin]), Method::StartRun).is_ok());
        assert!(require(&tok(t, &[Scope::Read]), Method::GetRun).is_ok());
        let e = require(&tok(t, &[Scope::Read]), Method::StartRun).unwrap_err();
        assert_eq!(e.code, ErrorCode::PermissionDenied);
        assert_eq!(e.reason(), Some(reason::MISSING_SCOPE));
        let d = e.details.expect("details");
        assert_eq!(d["scope"], "trigger");
        assert_eq!(d["method"], "ControlPlane.start_run");
        assert!(require(&tok(t, &[Scope::Deploy]), Method::CancelRun).is_err());
        // System admin needs tenant None.
        assert!(require(&tok(None, &[Scope::Admin]), Method::CreateTenant).is_ok());
        let e = require(&tok(t, &[Scope::Admin]), Method::CreateTenant).unwrap_err();
        assert_eq!(e.details.expect("details")["authority"], "system");
        // Tenant admin: admin on any tenant.
        assert!(require(&tok(t, &[Scope::Admin]), Method::PutSecret).is_ok());
        assert!(require(&tok(Some("acme"), &[Scope::Admin]), Method::CreateApiToken).is_ok());
        assert!(require(&tok(t, &[Scope::Operate]), Method::PutSecret).is_err());
        // System reads.
        assert!(require(&tok(None, &[Scope::Read]), Method::ClusterStatus).is_ok());
        assert!(require(&tok(t, &[Scope::Read]), Method::ClusterStatus).is_err());
        assert!(require(&tok(None, &[Scope::Read]), Method::CreateTenant).is_err());
        // whoami for any token.
        assert!(require(&tok(t, &[Scope::Trigger]), Method::WhoAmI).is_ok());
        // A tenant-less token with other scopes is invalid everywhere.
        let e = require(&tok(None, &[Scope::Deploy]), Method::UploadModule).unwrap_err();
        assert_eq!(e.code, ErrorCode::Unauthenticated);
        assert_eq!(e.reason(), Some("invalid_token"));
        // Node principals: the read class only.
        let node = Principal {
            kind: PrincipalKind::Node,
            ..Principal::system()
        };
        assert!(require(&node, Method::ListRuns).is_ok());
        assert!(require(&node, Method::StartRun).is_err());
        assert!(require(&node, Method::CreateTenant).is_err());
    }
}
