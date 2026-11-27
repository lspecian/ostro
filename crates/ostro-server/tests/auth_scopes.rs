//! 14-15 Task 1: the route × scope matrix (SEC-02, D-P14-3, W12) and the separation of API
//! and join tokens.
//!
//! **Matrix.** The production API router (`ostro_api::router_with`: the bearer layer, the
//! REST and gRPC adapters) over the real guard (`p14::wrap_ingress`, so `authz::require`
//! and the hook sequence) and the real meta-backed authenticator in `[auth] mode = "token"`.
//! The inner control plane and **every** extension slot, including the four Phase 14 slots
//! (`tenants`, `api_tokens`, `audit`, `security`, whose implementations land in 14-17/18/19),
//! are the ostro-api in-memory fakes, so each route's dummy input reaches the guard
//! deterministically. Tokens are written straight into meta (the token admin API is 14-17's).
//!
//! Every REST operation of the OpenAPI document and every gRPC method of the ostro-proto
//! descriptor set (`ostro_proto::GRPC_METHODS`) is called with no token and with each token
//! of [`TOKENS`]; the outcome (401 / 403 / allowed, where allowed = neither; 404 / 400 on the
//! dummy inputs is fine) must equal the expectation derived from the 14-01 method inventory
//! (scope column + tenant class, D-P14-3). Each row also declares the guarded method the
//! route reaches, and the test checks it against the guard's own `authz` trace event, so the
//! route → method map cannot drift. Coverage is exact: the exercised keys equal the table's
//! keys. Every mismatch is reported at once.
//!
//! `OSTRO_EXPORT_AUTH_MATRIX=<absolute path>` writes the matrix as Markdown (14-24 publishes it
//! as `docs/security/auth-matrix.md`).
//!
//! **Full server.** A token-mode server checks the join-token separation (worker `wkr_` and
//! node `njt_` join tokens get 401 on the API; an `otk_` API token is refused by both join
//! routes), the demand endpoint (401 without a bearer, 200 with a read token), the
//! non-OpenAPI unauthenticated routes and a sample of the matrix end to end.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Body;
use axum::http::{HeaderValue, Method as HttpMethod, Request, StatusCode};
use ostro_api::auth::{Method, Scope, ScopeSet, TenantClass};
use ostro_api::authn_layer::{self, RouteAuth};
use ostro_api::fake::{
    FakeCluster, FakeControlPlane, FakeInventory, FakeModuleLifecycle, FakeOps, FakeP14, FakePhase8,
};
use ostro_api::phase7::REASON_JOIN_TOKEN_INVALID;
use ostro_api::{ApiConfig, ControlPlane};
use ostro_meta::{ApiToken, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_partition::Clock;
use ostro_partition::clock::ManualClock;
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p14::authz;
use ostro_server::p14::guard::TRACE_TARGET;
use ostro_server::p14::{self, P14Init, tokens};
use serde_json::Value;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

use common::{ServerOpts, TestServer};

const LIMIT: Duration = Duration::from_secs(120);
/// Passes the join routes' shape checks, fails CSR parsing (after the token check).
const DUMMY_CSR: &str =
    "-----BEGIN CERTIFICATE REQUEST-----\nAAAA\n-----END CERTIFICATE REQUEST-----\n";
const HEX64: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

// ---------------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------------

/// One test token: label, scopes, tenant (`None` = system).
struct Tok {
    label: &'static str,
    scopes: &'static [&'static str],
    tenant: Option<&'static str>,
}

/// Each single scope on the default tenant, tenant admin, system admin, system read.
const TOKENS: &[Tok] = &[
    Tok {
        label: "admin@_",
        scopes: &["admin"],
        tenant: Some("_"),
    },
    Tok {
        label: "deploy",
        scopes: &["deploy"],
        tenant: Some("_"),
    },
    Tok {
        label: "operate",
        scopes: &["operate"],
        tenant: Some("_"),
    },
    Tok {
        label: "trigger",
        scopes: &["trigger"],
        tenant: Some("_"),
    },
    Tok {
        label: "read",
        scopes: &["read"],
        tenant: Some("_"),
    },
    Tok {
        label: "system admin",
        scopes: &["admin"],
        tenant: None,
    },
    Tok {
        label: "system read",
        scopes: &["read"],
        tenant: None,
    },
];

async fn put_record(meta: &MetaHandle, rec: ApiToken) {
    let res = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Token(rec.id.clone()),
            expected_version: None,
            record: MetaRecord::Token(rec),
        })
        .await
        .expect("propose");
    assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
}

/// A fresh API token stored in `meta`; returns its bearer.
async fn put_token(meta: &MetaHandle, scopes: &[&str], tenant: Option<&str>) -> String {
    let (id, secret, bearer) = tokens::generate_os().expect("generate");
    put_record(
        meta,
        ApiToken {
            id: id.as_str().to_owned(),
            hash: tokens::hash(&secret),
            scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
            tenant: tenant.map(str::to_owned),
            created_at_ms: 1,
            expires_at_ms: None,
            ext: Vec::new(),
        },
    )
    .await;
    bearer.expose().to_owned()
}

// ---------------------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------------------

/// What a route is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Row {
    /// Authenticated; reaches the guarded method.
    M(Method),
    /// Authenticated; no guarded method (`/openapi.json`).
    Authn,
    /// Unauthenticated, runs as `System` (its own credential); reaches the method when given.
    System(Option<Method>),
    /// Unauthenticated, no principal (health, shell page).
    Anon,
    /// Node-to-node protocol (`ostro.cluster.v1`): served only on the cluster listener to
    /// mTLS node identities; on the client listener it authenticates, then `UNIMPLEMENTED`.
    ClusterListener,
}

use Row::{Anon, Authn, ClusterListener, M, System};

/// Every REST operation (OpenAPI document): `"<METHOD> <path template>"`.
const REST: &[(&str, Row)] = &[
    ("GET /", Anon),
    ("GET /healthz", Anon),
    ("GET /readyz", Anon),
    ("GET /openapi.json", Authn),
    ("POST /hooks/{name}", System(Some(Method::DeliverHook))),
    (
        "POST /hooks/{tenant}/{name}",
        System(Some(Method::DeliverHook)),
    ),
    ("POST /v1/workers/join", System(Some(Method::JoinWorker))),
    ("POST /v1/cluster/join", System(Some(Method::ClusterJoin))),
    // Presigned object URLs: the query signature is the credential (the dummy has none).
    ("GET /v1/objects/{connection}/{key}", System(None)),
    ("PUT /v1/objects/{connection}/{key}", System(None)),
    ("GET /v1/admin/backup", M(Method::Backup)),
    ("GET /v1/audit", M(Method::QueryAudit)),
    ("GET /v1/auth/tokens", M(Method::ListApiTokens)),
    ("POST /v1/auth/tokens", M(Method::CreateApiToken)),
    ("DELETE /v1/auth/tokens/{id}", M(Method::RevokeApiToken)),
    (
        "POST /v1/auth/tokens/{id}/rotate",
        M(Method::RotateApiToken),
    ),
    ("GET /v1/auth/whoami", M(Method::WhoAmI)),
    ("GET /v1/bulk", M(Method::ListBulk)),
    ("POST /v1/bulk", M(Method::CreateBulk)),
    ("GET /v1/bulk/{job_id}", M(Method::GetBulk)),
    ("POST /v1/bulk/{job_id}/cancel", M(Method::CancelBulk)),
    ("POST /v1/cluster/init", M(Method::ClusterInit)),
    (
        "POST /v1/cluster/members/{id}/drain",
        M(Method::ClusterDrain),
    ),
    (
        "POST /v1/cluster/members/{id}/remove",
        M(Method::ClusterRemove),
    ),
    ("POST /v1/cluster/reassign", M(Method::ClusterReassign)),
    ("POST /v1/cluster/rebalance", M(Method::ClusterRebalance)),
    ("GET /v1/cluster/status", M(Method::ClusterStatus)),
    ("POST /v1/cluster/tokens", M(Method::CreateNodeToken)),
    ("GET /v1/cluster/upgrade", M(Method::UpgradeStatus)),
    (
        "POST /v1/cluster/upgrade/finalize",
        M(Method::UpgradeFinalize),
    ),
    ("POST /v1/cluster/upgrade/hold", M(Method::UpgradeHold)),
    (
        "POST /v1/cluster/upgrade/release",
        M(Method::UpgradeRelease),
    ),
    ("GET /v1/connections", M(Method::ListConnections)),
    ("DELETE /v1/connections/{name}", M(Method::DeleteConnection)),
    ("GET /v1/connections/{name}", M(Method::GetConnection)),
    ("PUT /v1/connections/{name}", M(Method::PutConnection)),
    ("GET /v1/dev/time", M(Method::DevTime)),
    ("POST /v1/dev/time/skip", M(Method::DevSkip)),
    ("GET /v1/dlq", M(Method::ListDlq)),
    ("POST /v1/dlq/{run_id}/dismiss", M(Method::DismissDlq)),
    ("POST /v1/dlq/{run_id}/requeue", M(Method::RequeueDlq)),
    ("GET /v1/egress", M(Method::ListEgress)),
    ("DELETE /v1/egress/{module}", M(Method::DeleteEgress)),
    ("GET /v1/egress/{module}", M(Method::GetEgress)),
    ("PUT /v1/egress/{module}", M(Method::PutEgress)),
    ("GET /v1/grants", M(Method::ListGrants)),
    ("PUT /v1/grants", M(Method::PutGrant)),
    ("DELETE /v1/grants/{id}", M(Method::DeleteGrant)),
    ("GET /v1/limits", M(Method::Limits)),
    ("GET /v1/modules", M(Method::ListModules)),
    ("POST /v1/modules/gc", M(Method::RunModuleGc)),
    ("DELETE /v1/modules/{sha256}", M(Method::RetireModule)),
    ("GET /v1/modules/{sha256}", M(Method::GetModule)),
    ("PUT /v1/modules/{sha256}", M(Method::UploadModule)),
    ("POST /v1/modules/{sha}/replay", M(Method::ReplayModule)),
    (
        "POST /v1/modules/{sha}/signatures",
        M(Method::AttachSignatures),
    ),
    ("GET /v1/partitions", M(Method::ListPartitions)),
    ("GET /v1/queues", M(Method::ListQueues)),
    ("GET /v1/queues/{name}/demand", M(Method::QueueDemand)),
    (
        "GET /v1/queues/{name}/failure-policy",
        M(Method::GetFailurePolicy),
    ),
    (
        "PUT /v1/queues/{name}/failure-policy",
        M(Method::SetFailurePolicy),
    ),
    ("GET /v1/queues/{name}/flow", M(Method::GetQueueFlow)),
    ("PUT /v1/queues/{name}/flow", M(Method::PutQueueFlow)),
    ("POST /v1/queues/{name}/pause", M(Method::PauseQueue)),
    ("GET /v1/queues/{name}/policy", M(Method::GetQueuePolicy)),
    ("PUT /v1/queues/{name}/policy", M(Method::SetQueuePolicy)),
    ("POST /v1/queues/{name}/resume", M(Method::ResumeQueue)),
    ("GET /v1/runs", M(Method::ListRuns)),
    ("POST /v1/runs/migrate", M(Method::MigrateRuns)),
    ("GET /v1/runs/{run_id}", M(Method::GetRun)),
    ("POST /v1/runs/{run_id}/cancel", M(Method::CancelRun)),
    ("GET /v1/runs/{run_id}/history", M(Method::RunHistory)),
    ("GET /v1/runs/{run_id}/logs", M(Method::RunLogs)),
    ("POST /v1/runs/{run_id}/pause", M(Method::PauseRun)),
    ("POST /v1/runs/{run_id}/rerun", M(Method::RerunRun)),
    ("GET /v1/runs/{run_id}/rerun-cuts", M(Method::AnalyzeRerun)),
    ("GET /v1/runs/{run_id}/result", M(Method::AwaitResult)),
    ("POST /v1/runs/{run_id}/resume", M(Method::ResumeRun)),
    ("POST /v1/runs/{run_id}/retry", M(Method::RetryRun)),
    (
        "POST /v1/runs/{run_id}/signals/{name}",
        M(Method::SignalRun),
    ),
    ("POST /v1/runs/{run_id}/terminate", M(Method::TerminateRun)),
    ("GET /v1/schedules", M(Method::ListSchedules)),
    ("POST /v1/schedules", M(Method::CreateSchedule)),
    ("DELETE /v1/schedules/{name}", M(Method::DeleteSchedule)),
    ("GET /v1/schedules/{name}", M(Method::GetSchedule)),
    ("PUT /v1/schedules/{name}", M(Method::UpdateSchedule)),
    ("POST /v1/schedules/{name}/pause", M(Method::PauseSchedule)),
    (
        "POST /v1/schedules/{name}/resume",
        M(Method::ResumeSchedule),
    ),
    (
        "POST /v1/schedules/{name}/trigger",
        M(Method::TriggerSchedule),
    ),
    ("GET /v1/secrets", M(Method::ListSecrets)),
    ("DELETE /v1/secrets/{name}", M(Method::DeleteSecret)),
    ("PUT /v1/secrets/{name}", M(Method::PutSecret)),
    ("POST /v1/security/modules/keys", M(Method::AddTrustedKey)),
    (
        "DELETE /v1/security/modules/keys/{key_id}",
        M(Method::RemoveTrustedKey),
    ),
    (
        "GET /v1/security/modules/policy",
        M(Method::GetModulePolicy),
    ),
    (
        "PUT /v1/security/modules/policy",
        M(Method::SetModulePolicy),
    ),
    ("GET /v1/security/sealing", M(Method::GetSealingStatus)),
    ("POST /v1/security/sealing/reseal", M(Method::Reseal)),
    ("GET /v1/tenants", M(Method::ListTenants)),
    ("POST /v1/tenants", M(Method::CreateTenant)),
    ("DELETE /v1/tenants/{id}", M(Method::DeleteTenant)),
    ("GET /v1/tenants/{id}", M(Method::GetTenant)),
    ("PATCH /v1/tenants/{id}", M(Method::UpdateTenant)),
    ("POST /v1/tenants/{id}/resume", M(Method::ResumeTenant)),
    ("POST /v1/tenants/{id}/suspend", M(Method::SuspendTenant)),
    ("POST /v1/tokens/complete", M(Method::CompleteToken)),
    ("GET /v1/webhooks", M(Method::ListWebhooks)),
    ("POST /v1/webhooks", M(Method::CreateWebhook)),
    ("DELETE /v1/webhooks/{name}", M(Method::DeleteWebhook)),
    ("GET /v1/webhooks/{name}", M(Method::GetWebhook)),
    ("PUT /v1/webhooks/{name}", M(Method::UpdateWebhook)),
    ("GET /v1/workers", M(Method::ListWorkers)),
    ("GET /v1/workers/tokens", M(Method::ListWorkerTokens)),
    ("POST /v1/workers/tokens", M(Method::CreateWorkerToken)),
    (
        "DELETE /v1/workers/tokens/{id}",
        M(Method::RevokeWorkerToken),
    ),
    ("POST /v1/workers/{id}/drain", M(Method::DrainWorker)),
    ("GET /v1/workflows", M(Method::ListWorkflows)),
    (
        "PUT /v1/workflows/{name}/active",
        M(Method::ActivateWorkflow),
    ),
    (
        "GET /v1/workflows/{name}/failure-policy",
        M(Method::GetFailurePolicy),
    ),
    (
        "PUT /v1/workflows/{name}/failure-policy",
        M(Method::SetFailurePolicy),
    ),
    ("POST /v1/workflows/{name}/pause", M(Method::PauseWorkflow)),
    (
        "GET /v1/workflows/{name}/policy",
        M(Method::GetWorkflowPolicy),
    ),
    (
        "PUT /v1/workflows/{name}/policy",
        M(Method::SetWorkflowPolicy),
    ),
    (
        "POST /v1/workflows/{name}/resume",
        M(Method::ResumeWorkflow),
    ),
    ("POST /v1/workflows/{name}/runs", M(Method::StartRun)),
];

/// Every gRPC method of the descriptor set (full method path).
const GRPC: &[(&str, Row)] = &[
    ("/ostro.health.v1.Health/Check", Anon),
    (
        "/ostro.api.v1.Cluster/Join",
        System(Some(Method::ClusterJoin)),
    ),
    (
        "/ostro.api.v1.Workers/JoinWorker",
        System(Some(Method::JoinWorker)),
    ),
    ("/ostro.api.v1.ApiTokens/Create", M(Method::CreateApiToken)),
    ("/ostro.api.v1.ApiTokens/List", M(Method::ListApiTokens)),
    ("/ostro.api.v1.ApiTokens/Revoke", M(Method::RevokeApiToken)),
    ("/ostro.api.v1.ApiTokens/Rotate", M(Method::RotateApiToken)),
    ("/ostro.api.v1.ApiTokens/WhoAmI", M(Method::WhoAmI)),
    ("/ostro.api.v1.Audit/Query", M(Method::QueryAudit)),
    (
        "/ostro.api.v1.Cluster/CreateNodeToken",
        M(Method::CreateNodeToken),
    ),
    ("/ostro.api.v1.Cluster/Drain", M(Method::ClusterDrain)),
    ("/ostro.api.v1.Cluster/Init", M(Method::ClusterInit)),
    ("/ostro.api.v1.Cluster/Reassign", M(Method::ClusterReassign)),
    (
        "/ostro.api.v1.Cluster/Rebalance",
        M(Method::ClusterRebalance),
    ),
    ("/ostro.api.v1.Cluster/Remove", M(Method::ClusterRemove)),
    ("/ostro.api.v1.Cluster/Status", M(Method::ClusterStatus)),
    (
        "/ostro.api.v1.Cluster/UpgradeFinalize",
        M(Method::UpgradeFinalize),
    ),
    ("/ostro.api.v1.Cluster/UpgradeHold", M(Method::UpgradeHold)),
    (
        "/ostro.api.v1.Cluster/UpgradeRelease",
        M(Method::UpgradeRelease),
    ),
    (
        "/ostro.api.v1.Cluster/UpgradeStatus",
        M(Method::UpgradeStatus),
    ),
    (
        "/ostro.api.v1.Connectors/DeleteConnection",
        M(Method::DeleteConnection),
    ),
    (
        "/ostro.api.v1.Connectors/DeleteEgress",
        M(Method::DeleteEgress),
    ),
    (
        "/ostro.api.v1.Connectors/GetConnection",
        M(Method::GetConnection),
    ),
    ("/ostro.api.v1.Connectors/GetEgress", M(Method::GetEgress)),
    (
        "/ostro.api.v1.Connectors/ListConnections",
        M(Method::ListConnections),
    ),
    ("/ostro.api.v1.Connectors/ListEgress", M(Method::ListEgress)),
    (
        "/ostro.api.v1.Connectors/PutConnection",
        M(Method::PutConnection),
    ),
    ("/ostro.api.v1.Connectors/PutEgress", M(Method::PutEgress)),
    (
        "/ostro.api.v1.Inventory/ListModules",
        M(Method::ListModules),
    ),
    (
        "/ostro.api.v1.Inventory/ListPartitions",
        M(Method::ListPartitions),
    ),
    ("/ostro.api.v1.Inventory/ListQueues", M(Method::ListQueues)),
    (
        "/ostro.api.v1.ModuleLifecycle/MigrateRuns",
        M(Method::MigrateRuns),
    ),
    (
        "/ostro.api.v1.ModuleLifecycle/RetireModule",
        M(Method::RetireModule),
    ),
    (
        "/ostro.api.v1.ModuleLifecycle/RunModuleGc",
        M(Method::RunModuleGc),
    ),
    ("/ostro.api.v1.Modules/Get", M(Method::GetModule)),
    ("/ostro.api.v1.Modules/Upload", M(Method::UploadModule)),
    (
        "/ostro.api.v1.Queues/GetQueueDemand",
        M(Method::QueueDemand),
    ),
    ("/ostro.api.v1.Queues/GetQueueFlow", M(Method::GetQueueFlow)),
    ("/ostro.api.v1.Queues/PauseQueue", M(Method::PauseQueue)),
    ("/ostro.api.v1.Queues/PutQueueFlow", M(Method::PutQueueFlow)),
    ("/ostro.api.v1.Queues/ResumeQueue", M(Method::ResumeQueue)),
    ("/ostro.api.v1.RunLogs/GetRunLogs", M(Method::RunLogs)),
    ("/ostro.api.v1.RunOps/CancelBulk", M(Method::CancelBulk)),
    ("/ostro.api.v1.RunOps/CreateBulk", M(Method::CreateBulk)),
    ("/ostro.api.v1.RunOps/DismissDlq", M(Method::DismissDlq)),
    ("/ostro.api.v1.RunOps/GetBulk", M(Method::GetBulk)),
    (
        "/ostro.api.v1.RunOps/GetFailurePolicy",
        M(Method::GetFailurePolicy),
    ),
    ("/ostro.api.v1.RunOps/GetLimits", M(Method::Limits)),
    ("/ostro.api.v1.RunOps/ListBulk", M(Method::ListBulk)),
    ("/ostro.api.v1.RunOps/ListDlq", M(Method::ListDlq)),
    ("/ostro.api.v1.RunOps/RequeueDlq", M(Method::RequeueDlq)),
    ("/ostro.api.v1.RunOps/Rerun", M(Method::RerunRun)),
    ("/ostro.api.v1.RunOps/RerunCuts", M(Method::AnalyzeRerun)),
    (
        "/ostro.api.v1.RunOps/SetFailurePolicy",
        M(Method::SetFailurePolicy),
    ),
    ("/ostro.api.v1.Runs/AwaitResult", M(Method::AwaitResult)),
    ("/ostro.api.v1.Runs/Cancel", M(Method::CancelRun)),
    ("/ostro.api.v1.Runs/Get", M(Method::GetRun)),
    ("/ostro.api.v1.Runs/History", M(Method::RunHistory)),
    ("/ostro.api.v1.Runs/List", M(Method::ListRuns)),
    ("/ostro.api.v1.Runs/PauseRun", M(Method::PauseRun)),
    ("/ostro.api.v1.Runs/ResumeRun", M(Method::ResumeRun)),
    ("/ostro.api.v1.Runs/Retry", M(Method::RetryRun)),
    ("/ostro.api.v1.Runs/Signal", M(Method::SignalRun)),
    ("/ostro.api.v1.Runs/Start", M(Method::StartRun)),
    ("/ostro.api.v1.Runs/Terminate", M(Method::TerminateRun)),
    ("/ostro.api.v1.Schedules/Create", M(Method::CreateSchedule)),
    ("/ostro.api.v1.Schedules/Delete", M(Method::DeleteSchedule)),
    ("/ostro.api.v1.Schedules/Get", M(Method::GetSchedule)),
    ("/ostro.api.v1.Schedules/List", M(Method::ListSchedules)),
    ("/ostro.api.v1.Schedules/Pause", M(Method::PauseSchedule)),
    ("/ostro.api.v1.Schedules/Resume", M(Method::ResumeSchedule)),
    (
        "/ostro.api.v1.Schedules/Trigger",
        M(Method::TriggerSchedule),
    ),
    ("/ostro.api.v1.Schedules/Update", M(Method::UpdateSchedule)),
    ("/ostro.api.v1.Secrets/DeleteGrant", M(Method::DeleteGrant)),
    (
        "/ostro.api.v1.Secrets/DeleteSecret",
        M(Method::DeleteSecret),
    ),
    ("/ostro.api.v1.Secrets/ListGrants", M(Method::ListGrants)),
    ("/ostro.api.v1.Secrets/ListSecrets", M(Method::ListSecrets)),
    ("/ostro.api.v1.Secrets/PutGrant", M(Method::PutGrant)),
    ("/ostro.api.v1.Secrets/PutSecret", M(Method::PutSecret)),
    (
        "/ostro.api.v1.Security/AddTrustedKey",
        M(Method::AddTrustedKey),
    ),
    (
        "/ostro.api.v1.Security/AttachSignatures",
        M(Method::AttachSignatures),
    ),
    (
        "/ostro.api.v1.Security/GetModulePolicy",
        M(Method::GetModulePolicy),
    ),
    (
        "/ostro.api.v1.Security/GetSealingStatus",
        M(Method::GetSealingStatus),
    ),
    (
        "/ostro.api.v1.Security/RemoveTrustedKey",
        M(Method::RemoveTrustedKey),
    ),
    ("/ostro.api.v1.Security/Reseal", M(Method::Reseal)),
    (
        "/ostro.api.v1.Security/SetModulePolicy",
        M(Method::SetModulePolicy),
    ),
    ("/ostro.api.v1.Tenants/Create", M(Method::CreateTenant)),
    ("/ostro.api.v1.Tenants/Delete", M(Method::DeleteTenant)),
    ("/ostro.api.v1.Tenants/Get", M(Method::GetTenant)),
    ("/ostro.api.v1.Tenants/List", M(Method::ListTenants)),
    ("/ostro.api.v1.Tenants/Resume", M(Method::ResumeTenant)),
    ("/ostro.api.v1.Tenants/Suspend", M(Method::SuspendTenant)),
    ("/ostro.api.v1.Tenants/Update", M(Method::UpdateTenant)),
    ("/ostro.api.v1.Tokens/Complete", M(Method::CompleteToken)),
    ("/ostro.api.v1.Webhooks/Create", M(Method::CreateWebhook)),
    ("/ostro.api.v1.Webhooks/Delete", M(Method::DeleteWebhook)),
    ("/ostro.api.v1.Webhooks/Get", M(Method::GetWebhook)),
    ("/ostro.api.v1.Webhooks/List", M(Method::ListWebhooks)),
    ("/ostro.api.v1.Webhooks/Update", M(Method::UpdateWebhook)),
    (
        "/ostro.api.v1.Workers/CreateWorkerToken",
        M(Method::CreateWorkerToken),
    ),
    ("/ostro.api.v1.Workers/DrainWorker", M(Method::DrainWorker)),
    (
        "/ostro.api.v1.Workers/ListWorkerTokens",
        M(Method::ListWorkerTokens),
    ),
    ("/ostro.api.v1.Workers/ListWorkers", M(Method::ListWorkers)),
    (
        "/ostro.api.v1.Workers/RevokeWorkerToken",
        M(Method::RevokeWorkerToken),
    ),
    (
        "/ostro.api.v1.Workflows/Activate",
        M(Method::ActivateWorkflow),
    ),
    (
        "/ostro.api.v1.Workflows/GetPolicy",
        M(Method::GetWorkflowPolicy),
    ),
    (
        "/ostro.api.v1.Workflows/GetQueuePolicy",
        M(Method::GetQueuePolicy),
    ),
    ("/ostro.api.v1.Workflows/List", M(Method::ListWorkflows)),
    (
        "/ostro.api.v1.Workflows/PauseWorkflow",
        M(Method::PauseWorkflow),
    ),
    (
        "/ostro.api.v1.Workflows/ResumeWorkflow",
        M(Method::ResumeWorkflow),
    ),
    (
        "/ostro.api.v1.Workflows/SetPolicy",
        M(Method::SetWorkflowPolicy),
    ),
    (
        "/ostro.api.v1.Workflows/SetQueuePolicy",
        M(Method::SetQueuePolicy),
    ),
    ("/ostro.cluster.v1.Membership/Removed", ClusterListener),
    ("/ostro.cluster.v1.MetaPeer/Propose", ClusterListener),
    ("/ostro.cluster.v1.MetaPeer/Raft", ClusterListener),
    ("/ostro.cluster.v1.MetaPeer/ReportSuspect", ClusterListener),
    ("/ostro.cluster.v1.NodeOps/DeliverInbound", ClusterListener),
    ("/ostro.cluster.v1.NodeOps/HandOff", ClusterListener),
    ("/ostro.cluster.v1.NodeOps/ModuleRefs", ClusterListener),
    ("/ostro.cluster.v1.NodeOps/StartRestart", ClusterListener),
    ("/ostro.cluster.v1.Replication/Fetch", ClusterListener),
    ("/ostro.cluster.v1.Replication/FetchBlob", ClusterListener),
    (
        "/ostro.cluster.v1.Replication/FetchSnapshot",
        ClusterListener,
    ),
    ("/ostro.cluster.v1.Replication/PushBlob", ClusterListener),
];

/// Client-listener routes outside the OpenAPI document and the API router (served by the
/// server around it): all unauthenticated (UiAuth owns `/ui`, 14-21). Checked against
/// `authn_layer::classify` here and on the full server below.
const EXTRA: &[(&str, &str)] = &[
    ("GET", "/metrics"),
    ("GET", "/ui"),
    ("GET", "/ui/runs"),
    ("POST", "/ui/login"),
    ("POST", "/grpc.health.v1.Health/Check"),
];

// ---------------------------------------------------------------------------------
// Expectations (from the 14-01 inventory, independent of authz.rs)
// ---------------------------------------------------------------------------------

/// The outcome of one call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    /// Neither 401 nor 403 (the handler ran; 400 / 404 on dummy inputs is fine).
    Allow,
    /// 403 / `PERMISSION_DENIED` (`missing_scope`).
    Deny,
    /// 403 / `PERMISSION_DENIED` from the tenant hook (`forbidden_tenant`, 14-16): the scope
    /// check passed; counts as `Allow` for this matrix.
    TenantDeny,
    /// 401 / `UNAUTHENTICATED`.
    Unauth,
    /// gRPC `UNIMPLEMENTED` (not served on this listener).
    NotServed,
}

impl Outcome {
    fn cell(self) -> &'static str {
        match self {
            Outcome::Allow => "allow",
            Outcome::Deny => "403",
            Outcome::TenantDeny => "allow (403 tenant)",
            Outcome::Unauth => "401",
            Outcome::NotServed => "n/s",
        }
    }
}

/// D-P14-3 over the 14-01 inventory row: `admin` covers everything, no other implication;
/// `system_only` needs `tenant = None`; `tenant_admin` admin methods take admin on any
/// tenant; `whoami` / `ready` need only authentication.
fn inventory_allows(m: Method, scopes: ScopeSet, tenant: Option<&str>) -> bool {
    if matches!(m, Method::WhoAmI | Method::Ready) {
        return true;
    }
    let spec = m.spec();
    // 14-17: connections are cluster-scoped in 1.0 (14-16 policy), so all four connection
    // methods need a system admin although the 14-01 inventory classes them tenant-admin.
    let tenant_class = match m {
        Method::PutConnection
        | Method::GetConnection
        | Method::ListConnections
        | Method::DeleteConnection => TenantClass::SystemOnly,
        _ => spec.tenant,
    };
    match (spec.scope, tenant_class) {
        (Scope::Admin, TenantClass::SystemOnly) => {
            tenant.is_none() && scopes.contains(Scope::Admin)
        }
        (Scope::Admin, _) => scopes.contains(Scope::Admin),
        (s, TenantClass::SystemOnly) => tenant.is_none() && scopes.allows(s),
        (s, _) => scopes.allows(s),
    }
}

/// Expected outcome of `row` for `tok` (`None` = no bearer).
fn expected(row: Row, tok: Option<&Tok>, grpc: bool) -> Outcome {
    match (row, tok) {
        (Anon | System(_), _) => Outcome::Allow,
        (ClusterListener, None) | (Authn | M(_), None) => Outcome::Unauth,
        (ClusterListener, Some(_)) => {
            if grpc {
                Outcome::NotServed
            } else {
                Outcome::Allow
            }
        }
        (Authn, Some(_)) => Outcome::Allow,
        (M(m), Some(t)) => {
            let scopes = ScopeSet::parse(t.scopes).expect("scopes");
            if inventory_allows(m, scopes, t.tenant) {
                Outcome::Allow
            } else {
                Outcome::Deny
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Inputs that reach the guard
// ---------------------------------------------------------------------------------

/// A concrete path for a template: `{sha*}` → 64 hex digits, cluster member ids → `2`,
/// everything else `x`.
fn concrete(template: &str) -> String {
    template
        .split('/')
        .map(|seg| match seg {
            "{sha}" | "{sha256}" => HEX64.to_owned(),
            "{id}" if template.starts_with("/v1/cluster/members/") => "2".to_owned(),
            s if s.starts_with('{') => "x".to_owned(),
            s => s.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// The JSON body of a REST operation (default `{}`).
fn rest_body(key: &str) -> Value {
    use serde_json::json;
    match key {
        "POST /v1/auth/tokens" => json!({"name": "t1", "scopes": ["read"]}),
        "POST /v1/bulk" => json!({"action": "cancel"}),
        "POST /v1/cluster/join" => json!({
            "token": "njt_x.secret",
            "csr_pem": "x",
            "advertise": {"api": "127.0.0.1:1", "cluster": "127.0.0.1:2"},
            "node_version": {
                "ostro_version": "1.0.0", "level_min": 0, "level_max": 9, "wasmtime": "1",
                "event_codec_ver": 1, "wit_lines": []
            }
        }),
        "POST /v1/workers/join" => {
            json!({"token": "wkr_x.secret", "csr_pem": "x", "worker_id": "w1"})
        }
        "POST /v1/cluster/reassign" => json!({"partition": 0, "cancel": true}),
        "PUT /v1/connections/{name}" => json!({"kind": "http", "config": {}}),
        "POST /v1/dev/time/skip" => json!({"by": "1h"}),
        "POST /v1/dlq/{run_id}/dismiss" => json!({"reason": "r"}),
        "PUT /v1/egress/{module}" => json!({"allow": ["example.com"]}),
        "PUT /v1/grants" => {
            json!({"module": "m1", "secret": "s1", "origins": ["https://example.com"]})
        }
        "POST /v1/modules/{sha}/replay" => json!({"sample": 1}),
        "POST /v1/modules/{sha}/signatures" => json!({"signatures": ["c2ln"]}),
        "POST /v1/runs/migrate" => {
            json!({"workflow": "w1", "from": {"version": 1}, "to_version": 2})
        }
        "POST /v1/runs/{run_id}/rerun" => json!({"from": {"seq": 1}}),
        "POST /v1/schedules" => json!({"name": "s1", "workflow": "w1", "cron": "* * * * *"}),
        "PUT /v1/schedules/{name}" => json!({"workflow": "w1", "cron": "* * * * *"}),
        "PUT /v1/secrets/{name}" => json!({"value": "v"}),
        "POST /v1/security/modules/keys" => {
            json!({"alg": "minisign", "key_id": "k1", "public_key": "RWQ="})
        }
        "PUT /v1/security/modules/policy" => json!({"require_signed_modules": false}),
        "POST /v1/tenants" => json!({"id": "acme"}),
        "POST /v1/tokens/complete" => json!({"token": "x"}),
        "POST /v1/webhooks" => json!({"name": "h1", "action": {"kind": "start", "workflow": "w1"}}),
        "PUT /v1/webhooks/{name}" => json!({"action": {"kind": "start", "workflow": "w1"}}),
        "PUT /v1/workflows/{name}/active" => json!({"version": 1}),
        _ => json!({}),
    }
}

/// A protobuf field of a hand-encoded message.
enum F<'a> {
    S(u32, &'a str),
    V(u32, u64),
    M(u32, Vec<u8>),
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Encode a message (enough of the wire format for these samples).
fn pb(fields: &[F<'_>]) -> Vec<u8> {
    let mut out = Vec::new();
    for f in fields {
        match f {
            F::S(n, s) => {
                varint(&mut out, u64::from(n << 3 | 2));
                varint(&mut out, s.len() as u64);
                out.extend_from_slice(s.as_bytes());
            }
            F::V(n, v) => {
                varint(&mut out, u64::from(n << 3));
                varint(&mut out, *v);
            }
            F::M(n, m) => {
                varint(&mut out, u64::from(n << 3 | 2));
                varint(&mut out, m.len() as u64);
                out.extend_from_slice(m);
            }
        }
    }
    out
}

/// The request message of a gRPC method (default: empty).
fn grpc_body(path: &str) -> Vec<u8> {
    use F::{M as Msg, S, V};
    let method = path.rsplit_once('.').map_or(path, |(_, m)| m);
    match method {
        "ApiTokens/Revoke" | "ApiTokens/Rotate" => pb(&[S(1, "otk_aaaaaaaaaaaaaaaa")]),
        "Cluster/Drain" | "Cluster/Remove" => pb(&[V(1, 2)]),
        "Cluster/Join" => pb(&[
            S(1, "njt_x.secret"),
            S(2, "x"),
            Msg(3, pb(&[S(1, "127.0.0.1:1"), S(2, "127.0.0.1:2")])),
            Msg(4, pb(&[S(1, "1.0.0"), V(3, 9), S(4, "1"), V(5, 1)])),
        ]),
        "Cluster/Reassign" => pb(&[V(1, 0), V(3, 1)]),
        "Connectors/PutConnection" => pb(&[Msg(1, pb(&[S(1, "c1"), S(2, "http"), S(3, "{}")]))]),
        "Connectors/PutEgress" => pb(&[Msg(1, pb(&[S(1, "m1"), S(2, "example.com")]))]),
        "ModuleLifecycle/MigrateRuns" => pb(&[S(1, "w1"), V(2, 1), V(4, 2)]),
        "ModuleLifecycle/RetireModule" => pb(&[S(1, HEX64)]),
        "Security/AttachSignatures" => pb(&[S(1, HEX64), S(2, "sig")]),
        "RunLogs/GetRunLogs" | "RunOps/RequeueDlq" | "RunOps/RerunCuts" => pb(&[S(1, "x")]),
        "RunOps/CancelBulk" | "RunOps/GetBulk" => pb(&[S(1, "x")]),
        "RunOps/CreateBulk" => pb(&[Msg(1, pb(&[Msg(1, Vec::new())]))]),
        "RunOps/DismissDlq" => pb(&[S(1, "x"), S(2, "r")]),
        "RunOps/GetFailurePolicy" => pb(&[S(1, "w1")]),
        "RunOps/Rerun" => pb(&[S(1, "x"), Msg(2, pb(&[V(1, 1)]))]),
        "RunOps/SetFailurePolicy" => pb(&[S(1, "w1"), Msg(3, Vec::new())]),
        "Schedules/Create" | "Schedules/Update" => pb(&[Msg(
            1,
            pb(&[S(1, "s1"), S(2, "w1"), S(4, "* * * * *"), S(5, "UTC")]),
        )]),
        "Schedules/Delete" | "Schedules/Get" | "Schedules/Pause" | "Schedules/Resume"
        | "Schedules/Trigger" => pb(&[S(1, "s1")]),
        "Secrets/PutGrant" => pb(&[Msg(
            1,
            pb(&[S(2, "m1"), S(3, "s1"), S(4, "https://example.com")]),
        )]),
        "Security/AddTrustedKey" => {
            pb(&[Msg(1, pb(&[S(1, "minisign"), S(2, "k1"), S(3, "RWQ=")]))])
        }
        "Security/RemoveTrustedKey" => pb(&[S(1, "k1")]),
        "Security/SetModulePolicy" => pb(&[Msg(1, Vec::new())]),
        "Tenants/Create" => pb(&[Msg(1, pb(&[S(1, "acme")]))]),
        "Tenants/Delete" | "Tenants/Get" | "Tenants/Resume" | "Tenants/Suspend"
        | "Tenants/Update" => pb(&[S(1, "acme")]),
        "Webhooks/Create" | "Webhooks/Update" => pb(&[Msg(
            1,
            pb(&[
                S(1, "h1"),
                Msg(2, pb(&[Msg(1, pb(&[S(1, "w1"), S(3, "body")]))])),
            ]),
        )]),
        "Webhooks/Delete" | "Webhooks/Get" => pb(&[S(1, "h1")]),
        _ => Vec::new(),
    }
}

// ---------------------------------------------------------------------------------
// The harness: router + guard trace capture
// ---------------------------------------------------------------------------------

/// The methods whose guard `authz` stage ran during a call.
#[derive(Clone, Default)]
struct Reached(Arc<Mutex<Vec<String>>>);

struct StageVisitor {
    method: String,
    stage: String,
}

impl Visit for StageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "method" => self.method = format!("{value:?}"),
            "stage" => self.stage = format!("{value:?}").trim_matches('"').to_owned(),
            _ => {}
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "stage" {
            self.stage = value.to_owned();
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Reached {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        if event.metadata().target() != TRACE_TARGET {
            return;
        }
        let mut v = StageVisitor {
            method: String::new(),
            stage: String::new(),
        };
        event.record(&mut v);
        if v.stage == "authz" {
            self.0
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(v.method);
        }
    }
}

impl Reached {
    fn take(&self) -> Vec<Method> {
        let names = std::mem::take(&mut *self.0.lock().unwrap_or_else(|e| e.into_inner()));
        names
            .iter()
            .map(|n| {
                *Method::all()
                    .iter()
                    .find(|m| format!("{m:?}") == *n)
                    .unwrap_or_else(|| panic!("unknown method {n}"))
            })
            .collect()
    }
}

struct Fixture {
    router: axum::Router,
    meta: Arc<MetaHandle>,
    _ctx: Arc<p14::P14Ctx>,
    _dir: tempfile::TempDir,
}

async fn fixture() -> Fixture {
    let dir = tempfile::tempdir().expect("tempdir");
    let meta = Arc::new(MetaHandle::start(dir.path(), 1).await.expect("meta"));
    let clock: Arc<dyn Clock> = Arc::new(ManualClock::new(1_000));
    let mut cfg = ServerConfig::server_defaults();
    cfg.auth.mode = AuthMode::Token;
    let ctx = p14::build_ctx(P14Init {
        cfg: Arc::new(cfg),
        meta: Arc::clone(&meta),
        clock,
        node_id: 1,
        data_dir: dir.path().to_path_buf(),
        level_max: ostro_types::compat::level_max(),
    })
    .expect("build_ctx");
    let cp = Arc::new(FakeControlPlane::new());
    let mut ext = FakePhase8::new(Arc::clone(&cp)).extensions();
    ext.ops = Some(FakeOps::new());
    ext.inventory = Some(FakeInventory::new());
    ext.cluster = Some(FakeCluster::new());
    ext.module_lifecycle = Some(FakeModuleLifecycle::new());
    FakeP14::new().install(&mut ext);
    let guarded = p14::wrap_ingress(&ctx, cp as Arc<dyn ControlPlane>, ext);
    let api_cfg = ApiConfig {
        authenticator: p14::authenticator(&ctx),
        ..ApiConfig::default()
    };
    Fixture {
        router: ostro_api::router_with(guarded.control, api_cfg, guarded.ext),
        meta,
        _ctx: ctx,
        _dir: dir,
    }
}

/// One call through the router, inline on this task (so the guard's trace events reach
/// this thread's subscriber).
async fn call(router: &axum::Router, req: Request<Body>, grpc: bool) -> (Outcome, String) {
    let mut r = router.clone();
    let res = tonic::codegen::Service::call(&mut r, req)
        .await
        .expect("infallible");
    if grpc {
        let status = res
            .headers()
            .get("grpc-status")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let msg = res
            .headers()
            .get("grpc-message")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        let reason = tonic::Status::from_header_map(res.headers())
            .and_then(|s| ostro_api::grpc::status_details(&s))
            .and_then(|d| d["reason"].as_str().map(str::to_owned));
        let o = match status.as_deref() {
            Some("16") => Outcome::Unauth,
            Some("7") if reason.as_deref() == Some("missing_scope") => Outcome::Deny,
            Some("7") => Outcome::TenantDeny,
            Some("12") => Outcome::NotServed,
            _ => Outcome::Allow,
        };
        return (o, format!("grpc-status {status:?} {msg}"));
    }
    let st = res.status();
    let body = axum::body::to_bytes(res.into_body(), 1 << 20)
        .await
        .unwrap_or_default();
    let text: String = String::from_utf8_lossy(&body).chars().take(200).collect();
    let o = match st {
        StatusCode::UNAUTHORIZED => Outcome::Unauth,
        StatusCode::FORBIDDEN => {
            let reason = serde_json::from_slice::<Value>(&body)
                .ok()
                .and_then(|v| v["error"]["details"]["reason"].as_str().map(str::to_owned));
            if reason.as_deref() == Some("missing_scope") {
                Outcome::Deny
            } else {
                Outcome::TenantDeny
            }
        }
        _ => Outcome::Allow,
    };
    (o, format!("{st} {text}"))
}

fn rest_request(key: &str, bearer: Option<&str>) -> Request<Body> {
    let (method, template) = key.split_once(' ').expect("key");
    let mut b = Request::builder()
        .method(HttpMethod::from_bytes(method.as_bytes()).expect("method"))
        .uri(concrete(template))
        .header("content-type", "application/json");
    if let Some(t) = bearer {
        b = b.header(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {t}")).expect("header"),
        );
    }
    b.body(Body::from(rest_body(key).to_string()))
        .expect("request")
}

fn grpc_request(path: &str, bearer: Option<&str>) -> Request<Body> {
    let msg = grpc_body(path);
    let mut frame = vec![0u8];
    frame.extend_from_slice(&u32::try_from(msg.len()).expect("len").to_be_bytes());
    frame.extend_from_slice(&msg);
    let mut b = Request::builder()
        .method(HttpMethod::POST)
        .uri(path)
        .header("content-type", "application/grpc")
        .header("te", "trailers");
    if let Some(t) = bearer {
        b = b.header(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {t}")).expect("header"),
        );
    }
    b.body(Body::from(frame)).expect("request")
}

fn row_label(row: Row) -> (String, String) {
    match row {
        M(m) => (m.name(), authz::required(m).as_str()),
        Authn => ("-".into(), "authenticated".into()),
        System(Some(m)) => (m.name(), "unauthenticated (System; own credential)".into()),
        System(None) => (
            "-".into(),
            "unauthenticated (System; own credential)".into(),
        ),
        Anon => ("-".into(), "unauthenticated".into()),
        ClusterListener => (
            "-".into(),
            "cluster listener only (node mTLS); not served here".into(),
        ),
    }
}

// ---------------------------------------------------------------------------------
// The matrix
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn every_route_enforces_its_scope() {
    let reached = Reached::default();
    let _sub =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(reached.clone()));
    let f = fixture().await;
    let mut bearers = Vec::new();
    for t in TOKENS {
        bearers.push(put_token(&f.meta, t.scopes, t.tenant).await);
    }
    let sys_admin = bearers[5].clone();
    // Warm the guard's callsites, then recompute their cached interest (other tests of this
    // binary may have registered them on threads without a subscriber).
    let _ = call(
        &f.router,
        rest_request("GET /v1/runs", Some(&sys_admin)),
        false,
    )
    .await;
    tracing::callsite::rebuild_interest_cache();
    let _ = reached.take();

    // Exact coverage (W12).
    let doc = serde_json::to_value(ostro_api::openapi()).expect("doc");
    let mut documented = BTreeSet::new();
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for method in item.as_object().expect("item").keys() {
            if ["get", "put", "post", "delete", "patch"].contains(&method.as_str()) {
                documented.insert(format!("{} {path}", method.to_uppercase()));
            }
        }
    }
    let rest_keys: BTreeSet<String> = REST.iter().map(|(k, _)| (*k).to_owned()).collect();
    assert_eq!(REST.len(), rest_keys.len(), "duplicate REST rows");
    let grpc_keys: BTreeSet<String> = GRPC.iter().map(|(k, _)| (*k).to_owned()).collect();
    assert_eq!(GRPC.len(), grpc_keys.len(), "duplicate gRPC rows");
    let descriptor: BTreeSet<String> = ostro_proto::GRPC_METHODS
        .iter()
        .map(|m| (*m).to_owned())
        .collect();
    let mut mismatches = Vec::new();
    for k in documented.difference(&rest_keys) {
        mismatches.push(format!("REST route without a row: {k}"));
    }
    for k in rest_keys.difference(&documented) {
        mismatches.push(format!("REST row without a route: {k}"));
    }
    for k in descriptor.difference(&grpc_keys) {
        mismatches.push(format!("gRPC method without a row: {k}"));
    }
    for k in grpc_keys.difference(&descriptor) {
        mismatches.push(format!("gRPC row without a method: {k}"));
    }

    // The documented unauthenticated routes are exactly the bearer layer's list.
    for (key, row) in REST {
        let (m, p) = key.split_once(' ').expect("key");
        let class = authn_layer::classify(
            &HttpMethod::from_bytes(m.as_bytes()).expect("method"),
            &concrete(p),
        );
        let want = match row {
            Anon => RouteAuth::Anonymous,
            System(_) => RouteAuth::System,
            _ => RouteAuth::Authenticate,
        };
        if class != want {
            mismatches.push(format!(
                "{key}: bearer layer class {class:?}, table {want:?}"
            ));
        }
    }
    for (key, row) in GRPC {
        let class = authn_layer::classify(&HttpMethod::POST, key);
        let want = match row {
            Anon => RouteAuth::Anonymous,
            System(_) => RouteAuth::System,
            _ => RouteAuth::Authenticate,
        };
        if class != want {
            mismatches.push(format!(
                "{key}: bearer layer class {class:?}, table {want:?}"
            ));
        }
    }
    for (m, p) in EXTRA {
        let class = authn_layer::classify(&HttpMethod::from_bytes(m.as_bytes()).expect("m"), p);
        if class != RouteAuth::Anonymous {
            mismatches.push(format!(
                "{m} {p}: bearer layer class {class:?}, expected Anonymous"
            ));
        }
    }

    // Every route × every token.
    let mut matrix: Vec<(String, Row, Vec<Outcome>)> = Vec::new();
    let all: Vec<(&str, Row, bool)> = REST
        .iter()
        .map(|(k, r)| (*k, *r, false))
        .chain(GRPC.iter().map(|(k, r)| (*k, *r, true)))
        .collect();
    let mut routed_methods = BTreeSet::new();
    for (key, row, grpc) in &all {
        let req = |b: Option<&str>| {
            if *grpc {
                grpc_request(key, b)
            } else {
                rest_request(key, b)
            }
        };
        // The route reaches the declared guarded method (system admin, or no bearer for the
        // System routes).
        let probe = if matches!(row, System(_)) {
            None
        } else {
            Some(sys_admin.as_str())
        };
        let (_, detail) = call(&f.router, req(probe), *grpc).await;
        let got = reached.take();
        let want: Vec<Method> = match row {
            M(m) | System(Some(m)) => vec![*m],
            _ => Vec::new(),
        };
        if got != want {
            mismatches.push(format!("{key}: reached {got:?}, table {want:?} ({detail})"));
        }
        routed_methods.extend(want);
        let mut outcomes = Vec::new();
        for (i, t) in std::iter::once(None)
            .chain(TOKENS.iter().map(Some))
            .enumerate()
        {
            let bearer = (i > 0).then(|| bearers[i - 1].as_str());
            let (got, detail) = call(&f.router, req(bearer), *grpc).await;
            let _ = reached.take();
            let want = expected(*row, t, *grpc);
            let scope_view = if got == Outcome::TenantDeny {
                Outcome::Allow
            } else {
                got
            };
            if scope_view != want {
                mismatches.push(format!(
                    "{key} [{}]: got {got:?}, expected {want:?} ({detail})",
                    t.map_or("no token", |t| t.label)
                ));
            }
            outcomes.push(got);
        }
        matrix.push(((*key).to_owned(), *row, outcomes));
    }
    assert!(matrix.len() > 200, "{} rows", matrix.len());

    if let Ok(path) = std::env::var("OSTRO_EXPORT_AUTH_MATRIX") {
        let md = markdown(&matrix, &routed_methods);
        std::fs::write(&path, md).unwrap_or_else(|e| panic!("write {path}: {e}"));
    }
    assert!(
        mismatches.is_empty(),
        "{} mismatches:\n{}",
        mismatches.len(),
        mismatches.join("\n")
    );
}

fn markdown(matrix: &[(String, Row, Vec<Outcome>)], routed: &BTreeSet<Method>) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "# Ostro API authorization matrix\n");
    let _ = writeln!(
        s,
        "Generated by `crates/ostro-server/tests/auth_scopes.rs` \
         (`OSTRO_EXPORT_AUTH_MATRIX=<path> cargo test -p ostro-server --test auth_scopes`). \
         Each cell is the observed outcome of the route called with that bearer through the \
         production router, bearer layer, guard and `authz::require` (D-P14-3): `allow` = the \
         handler ran (400 / 404 on dummy inputs included), `403` = `missing_scope`, \
         `allow (403 tenant)` = the scope check passed and the tenant hook (14-16) refused \
         (`forbidden_tenant`), `401` = \
         unauthenticated, `n/s` = not served on the client listener. Tokens: one scope each on \
         the default tenant `_` (`admin@_` = tenant admin), `system admin` / `system read` = \
         tenant-less tokens. Join tokens (`wkr_`, `njt_`) are rejected by every API route \
         (401); API tokens are rejected by the join routes.\n"
    );
    let _ = write!(s, "| Route | Method | Requirement | no token |");
    for t in TOKENS {
        let _ = write!(s, " {} |", t.label);
    }
    let _ = writeln!(s);
    let _ = write!(s, "|---|---|---|---|");
    for _ in TOKENS {
        let _ = write!(s, "---|");
    }
    let _ = writeln!(s);
    for (key, row, outcomes) in matrix {
        let (method, req) = row_label(*row);
        let _ = write!(s, "| `{key}` | {method} | {req} |");
        for o in outcomes {
            let _ = write!(s, " {} |", o.cell());
        }
        let _ = writeln!(s);
    }
    let _ = writeln!(s, "\n## Method → requirement (`p14/authz.rs::required`)\n");
    let _ = writeln!(s, "| Method | Requirement | Tenant class | Routes |");
    let _ = writeln!(s, "|---|---|---|---|");
    let mut routes: BTreeMap<Method, usize> = BTreeMap::new();
    for (_, row, _) in matrix {
        if let M(m) | System(Some(m)) = row {
            *routes.entry(*m).or_default() += 1;
        }
    }
    for &m in Method::all() {
        let n = routes.get(&m).copied().unwrap_or(0);
        let note = if routed.contains(&m) {
            n.to_string()
        } else if m == Method::Ready {
            "0 (`/readyz`, unauthenticated pass-through)".to_owned()
        } else if m == Method::PresignedObject {
            "0 (`/v1/objects/*` as System once the presign signature verifies)".to_owned()
        } else {
            "0 (no client route)".to_owned()
        };
        let _ = writeln!(
            s,
            "| {} | {} | {} | {note} |",
            m.name(),
            authz::required(m).as_str(),
            m.spec().tenant.as_str()
        );
    }
    s
}

// ---------------------------------------------------------------------------------
// Full server: join-token separation, demand, extras
// ---------------------------------------------------------------------------------

/// A Phase 7 / 13 join-token record (`wkr_` / `njt_`) and its two bearer spellings: the
/// join form `<id>.<secret>` and the API form `ostro_<id>_<52 base32>`.
async fn put_join_token(meta: &MetaHandle, prefix: &str, scope: &str) -> (String, String) {
    let id = format!("{prefix}abcdefghijklmnop");
    let secret = "s3cr3t-join-secret";
    put_record(
        meta,
        ApiToken {
            id: id.clone(),
            hash: sha256(secret.as_bytes()),
            scopes: vec![scope.to_owned()],
            tenant: None,
            created_at_ms: 1,
            expires_at_ms: None,
            ext: Vec::new(),
        },
    )
    .await;
    (
        format!("{id}.{secret}"),
        format!("ostro_{id}_{}", "a".repeat(52)),
    )
}

fn sha256(b: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    sha2::Sha256::digest(b).into()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_tokens_and_api_tokens_are_separate() {
    let s = TestServer::start_with(ServerOpts::default(), |c| c.auth.mode = AuthMode::Token).await;
    s.wait_ready(LIMIT).await;
    let meta = s.h().meta();
    let admin = std::fs::read_to_string(s.h().data_dir().join("admin.token")).expect("token");
    let admin = admin.trim().to_owned();
    let read = put_token(meta, &["read"], Some("_")).await;
    let trigger = put_token(meta, &["trigger"], Some("_")).await;
    let (wkr_join, wkr_api) = put_join_token(meta, "wkr_", "worker:join").await;
    let (njt_join, njt_api) = put_join_token(meta, "njt_", "node:join").await;

    let get = |path: &str, bearer: Option<&str>| {
        let mut r = s.http.get(s.url(path));
        if let Some(b) = bearer {
            r = r.bearer_auth(b);
        }
        r
    };

    // Join tokens are never API credentials.
    for (label, b) in [
        ("wkr join form", &wkr_join),
        ("wkr api form", &wkr_api),
        ("njt join form", &njt_join),
        ("njt api form", &njt_api),
    ] {
        for path in ["/v1/runs", "/v1/auth/whoami", "/v1/queues/q1/demand"] {
            let r = get(path, Some(b)).send().await.expect("GET");
            assert_eq!(r.status(), StatusCode::UNAUTHORIZED, "{label} on {path}");
        }
    }

    // API tokens are never join credentials; the join tokens pass the token check (the
    // dummy CSR fails after it).
    let worker_join =
        |token: &str| serde_json::json!({"token": token, "csr_pem": DUMMY_CSR, "worker_id": "w1"});
    let node_join = |token: &str| {
        serde_json::json!({
            "token": token,
            "csr_pem": DUMMY_CSR,
            "advertise": {"api": "127.0.0.1:1", "cluster": "127.0.0.1:2"},
            "node_version": {
                "ostro_version": "1.0.0", "level_min": 0, "level_max": 999, "wasmtime": "1",
                "event_codec_ver": 1, "wit_lines": []
            }
        })
    };
    let reason = |v: &Value| v["error"]["details"]["reason"].as_str().map(str::to_owned);
    for (path, body_of) in [
        ("/v1/workers/join", &worker_join as &dyn Fn(&str) -> Value),
        ("/v1/cluster/join", &node_join),
    ] {
        for (label, tok) in [("admin otk", admin.as_str()), ("read otk", read.as_str())] {
            let r = s
                .http
                .post(s.url(path))
                .json(&body_of(tok))
                .send()
                .await
                .expect("POST");
            let st = r.status();
            let v: Value = r.json().await.expect("json");
            assert!(!st.is_success(), "{label} at {path}: {st} {v}");
            assert_eq!(
                reason(&v).as_deref(),
                Some(REASON_JOIN_TOKEN_INVALID),
                "{label} at {path}: {v}"
            );
            // A bearer header does not help either.
            let r = s
                .http
                .post(s.url(path))
                .bearer_auth(&admin)
                .json(&body_of(tok))
                .send()
                .await
                .expect("POST");
            let v: Value = r.json().await.expect("json");
            assert_eq!(
                reason(&v).as_deref(),
                Some(REASON_JOIN_TOKEN_INVALID),
                "{v}"
            );
        }
    }
    // Positive control: the worker join token passes the token check at the worker route.
    let r = s
        .http
        .post(s.url("/v1/workers/join"))
        .json(&worker_join(&wkr_join))
        .send()
        .await
        .expect("POST");
    let v: Value = r.json().await.expect("json");
    assert_ne!(
        reason(&v).as_deref(),
        Some(REASON_JOIN_TOKEN_INVALID),
        "{v}"
    );
    // Crossed join tokens are refused.
    let r = s
        .http
        .post(s.url("/v1/workers/join"))
        .json(&worker_join(&njt_join))
        .send()
        .await
        .expect("POST");
    let v: Value = r.json().await.expect("json");
    assert_eq!(
        reason(&v).as_deref(),
        Some(REASON_JOIN_TOKEN_INVALID),
        "{v}"
    );
    let r = s
        .http
        .post(s.url("/v1/cluster/join"))
        .json(&node_join(&wkr_join))
        .send()
        .await
        .expect("POST");
    let v: Value = r.json().await.expect("json");
    assert_eq!(
        reason(&v).as_deref(),
        Some(REASON_JOIN_TOKEN_INVALID),
        "{v}"
    );

    // The demand endpoint (the KEDA signal) needs a read bearer.
    let r = get("/v1/queues/q1/demand", None).send().await.expect("GET");
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);
    let r = get("/v1/queues/q1/demand", Some(&read))
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), StatusCode::OK);
    let r = get("/v1/queues/q1/demand", Some(&trigger))
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let v: Value = r.json().await.expect("json");
    assert_eq!(v["error"]["details"]["reason"], "missing_scope");
    assert_eq!(v["error"]["details"]["scope"], "read");

    // Non-OpenAPI routes on the client listener stay unauthenticated.
    for path in ["/metrics", "/healthz", "/readyz"] {
        let r = get(path, None).send().await.expect("GET");
        assert_ne!(r.status(), StatusCode::UNAUTHORIZED, "{path}");
    }

    // The matrix end to end on the real server: scope denial, system authority.
    let r = s
        .http
        .post(s.url("/v1/workflows/w1/runs"))
        .bearer_auth(&read)
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("POST");
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let tenant_admin = put_token(meta, &["admin"], Some("_")).await;
    let r = get("/v1/cluster/status", Some(&tenant_admin))
        .send()
        .await
        .expect("GET");
    assert_eq!(r.status(), StatusCode::FORBIDDEN);
    let r = get("/v1/cluster/status", Some(&admin))
        .send()
        .await
        .expect("GET");
    assert_ne!(r.status(), StatusCode::FORBIDDEN);
    assert_ne!(r.status(), StatusCode::UNAUTHORIZED);
}
