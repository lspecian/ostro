//! REST adapter: serde + utoipa DTOs and axum handlers over [`ControlPlane`].
//!
//! Every handler takes `State<Arc<dyn ControlPlane>>`, converts its DTO into a domain
//! request, and renders the domain result (or [`ApiError`]) back. No business logic
//! lives here.

use std::sync::{Arc, OnceLock};

use axum::Json;
use axum::extract::rejection::{BytesRejection, JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, OpenApi, ToSchema};
use utoipa_axum::router::{OpenApiRouter, UtoipaMethodRouterExt};
use utoipa_axum::routes;

use crate::domain::{self, ControlPlane};
use crate::error::{ApiError, ErrorBody};

/// Router state: the control plane.
pub type CpState = Arc<dyn ControlPlane>;

// ---------------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------------

/// Run status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunStatusDto {
    /// Waiting for its start delay.
    Scheduled,
    /// Ready to be picked up.
    Ready,
    /// Executing.
    Running,
    /// Finished successfully.
    Completed,
    /// Finished with a failure.
    Failed,
    /// Cancelled.
    Cancelled,
    /// Hard-stopped by an operator.
    Terminated,
    /// Run timeout or schedule-to-start timeout.
    TimedOut,
    /// Continued as a new run.
    ContinuedAsNew,
}

impl From<domain::RunStatus> for RunStatusDto {
    fn from(s: domain::RunStatus) -> Self {
        use domain::RunStatus as S;
        match s {
            S::Scheduled => RunStatusDto::Scheduled,
            S::Ready => RunStatusDto::Ready,
            S::Running => RunStatusDto::Running,
            S::Completed => RunStatusDto::Completed,
            S::Failed => RunStatusDto::Failed,
            S::Cancelled => RunStatusDto::Cancelled,
            S::Terminated => RunStatusDto::Terminated,
            S::TimedOut => RunStatusDto::TimedOut,
            S::ContinuedAsNew => RunStatusDto::ContinuedAsNew,
        }
    }
}

impl From<RunStatusDto> for domain::RunStatus {
    fn from(s: RunStatusDto) -> Self {
        use domain::RunStatus as S;
        match s {
            RunStatusDto::Scheduled => S::Scheduled,
            RunStatusDto::Ready => S::Ready,
            RunStatusDto::Running => S::Running,
            RunStatusDto::Completed => S::Completed,
            RunStatusDto::Failed => S::Failed,
            RunStatusDto::Cancelled => S::Cancelled,
            RunStatusDto::Terminated => S::Terminated,
            RunStatusDto::TimedOut => S::TimedOut,
            RunStatusDto::ContinuedAsNew => S::ContinuedAsNew,
        }
    }
}

/// Idempotency-key collision policy.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ConflictPolicyDto {
    /// Return the existing run (`created: false`).
    #[default]
    ReturnExisting,
    /// Fail with 409 and `details.existing_run_id`.
    Reject,
}

impl From<ConflictPolicyDto> for domain::ConflictPolicy {
    fn from(p: ConflictPolicyDto) -> Self {
        match p {
            ConflictPolicyDto::ReturnExisting => domain::ConflictPolicy::ReturnExisting,
            ConflictPolicyDto::Reject => domain::ConflictPolicy::Reject,
        }
    }
}

/// Workflow name + version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowRefDto {
    /// Workflow name.
    pub name: String,
    /// Workflow version.
    pub version: u32,
}

/// A stored module.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ModuleDto {
    /// Lowercase hex sha256.
    pub sha256: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Source language, when known.
    pub language: Option<String>,
    /// Exported workflows.
    pub workflows: Vec<WorkflowRefDto>,
    /// Upload time (RFC 3339).
    #[schema(format = DateTime)]
    pub uploaded_at: String,
    /// Upload time, ms since the Unix epoch.
    pub uploaded_at_ms: u64,
}

/// Upload result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct UploadModuleResponse {
    /// The stored module.
    pub module: ModuleDto,
    /// `false` on idempotent re-upload.
    pub created: bool,
}

/// Raw Wasm component bytes (documentation-only schema of the upload body).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
pub struct WasmBytes(#[allow(dead_code)] Vec<u8>);

/// Upload query parameters.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct UploadModuleQuery {
    /// Source language hint ("rust", "python", "typescript").
    pub language: Option<String>,
}

/// One workflow version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowVersionDto {
    /// Version.
    pub version: u32,
    /// Implementing module.
    pub module_sha256: String,
    /// Default queue.
    pub default_queue: Option<String>,
    /// Default run timeout in ms.
    pub run_timeout_ms: Option<u64>,
}

/// A workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowDto {
    /// Name.
    pub name: String,
    /// Active version.
    pub active_version: Option<u32>,
    /// Versions, ascending.
    pub versions: Vec<WorkflowVersionDto>,
    /// Record version (CAS token for activation; also accepted via `If-Match`).
    pub record_version: u64,
}

/// Workflow listing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ListWorkflowsResponse {
    /// Workflows by name.
    pub workflows: Vec<WorkflowDto>,
}

/// Activation body.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ActivateWorkflowBody {
    /// Version to activate.
    pub version: u32,
    /// Required current record version (alternatively the `If-Match` header).
    #[serde(default)]
    pub expected_version: Option<u64>,
}

/// Start-run body.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StartRunBody {
    /// Workflow input (any JSON value; defaults to null).
    #[serde(default)]
    pub input: serde_json::Value,
    /// Explicit version (else the active one).
    #[serde(default)]
    pub version: Option<u32>,
    /// Queue override.
    #[serde(default)]
    pub queue: Option<String>,
    /// Delay before the run becomes ready, ms.
    #[serde(default)]
    pub delay_ms: Option<u64>,
    /// Priority.
    #[serde(default)]
    pub priority: Option<i32>,
    /// Tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// W3C traceparent.
    #[serde(default)]
    pub traceparent: Option<String>,
    /// Tenant.
    #[serde(default)]
    pub tenant: Option<String>,
    /// Idempotency key.
    #[serde(default)]
    pub idempotency_key: Option<String>,
    /// Collision policy (default `return_existing`).
    #[serde(default)]
    pub conflict_policy: Option<ConflictPolicyDto>,
    /// Concurrency key.
    #[serde(default)]
    pub concurrency_key: Option<String>,
}

/// Start-run result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StartRunResponse {
    /// Run id.
    pub run_id: String,
    /// `false` when an existing run was returned.
    pub created: bool,
}

/// Full run view.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunDto {
    /// Run id.
    pub run_id: String,
    /// Workflow.
    pub workflow: String,
    /// Version.
    pub version: u32,
    /// Implementing module.
    pub module_sha256: String,
    /// Queue.
    pub queue: String,
    /// Status.
    pub status: RunStatusDto,
    /// Current attempt.
    pub attempt: Option<u32>,
    /// Tags.
    pub tags: Vec<String>,
    /// Tenant.
    pub tenant: Option<String>,
    /// W3C traceparent.
    pub traceparent: Option<String>,
    /// Priority.
    pub priority: i32,
    /// Concurrency key.
    pub concurrency_key: Option<String>,
    /// Creation time (RFC 3339).
    #[schema(format = DateTime)]
    pub created_at: String,
    /// Creation time, ms since the Unix epoch.
    pub created_at_ms: u64,
    /// Completion time (RFC 3339).
    #[schema(format = DateTime)]
    pub completed_at: Option<String>,
    /// Completion time, ms since the Unix epoch.
    pub completed_at_ms: Option<u64>,
    /// Latest published status (embedded JSON).
    pub published_status: Option<serde_json::Value>,
    /// Result of a completed run (embedded JSON).
    pub result: Option<serde_json::Value>,
    /// Failure message.
    pub failure: Option<String>,
}

/// History query.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct HistoryQuery {
    /// First seq to return (inclusive, default 0).
    pub from_seq: Option<u64>,
    /// Page size (default 100, max 1000).
    pub limit: Option<u32>,
}

/// One history event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HistoryEventDto {
    /// Journal seq.
    pub seq: u64,
    /// Kind.
    pub kind: String,
    /// Event time (RFC 3339).
    #[schema(format = DateTime)]
    pub ts: String,
    /// Event time, ms since the Unix epoch.
    pub ts_ms: u64,
    /// One-line summary.
    pub summary: String,
    /// Details (embedded JSON).
    pub details: serde_json::Value,
}

/// History page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct HistoryResponse {
    /// Events, ascending seq.
    pub events: Vec<HistoryEventDto>,
    /// `from_seq` of the next page, if any.
    pub next_from_seq: Option<u64>,
}

/// Run listing query.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ListRunsQuery {
    /// Workflow filter.
    pub workflow: Option<String>,
    /// Status filter.
    pub status: Option<RunStatusDto>,
    /// Queue filter.
    pub queue: Option<String>,
    /// Tag filter.
    pub tag: Option<String>,
    /// Created at or after (RFC 3339).
    pub created_after: Option<String>,
    /// Created before (RFC 3339).
    pub created_before: Option<String>,
    /// Page size (default 100, capped at 1000).
    pub limit: Option<u32>,
    /// Token from a previous page.
    pub page_token: Option<String>,
}

/// Run listing row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunSummaryDto {
    /// Run id.
    pub run_id: String,
    /// Workflow.
    pub workflow: String,
    /// Version.
    pub version: u32,
    /// Status.
    pub status: RunStatusDto,
    /// Queue.
    pub queue: String,
    /// Creation time (RFC 3339).
    #[schema(format = DateTime)]
    pub created_at: String,
    /// Creation time, ms since the Unix epoch.
    pub created_at_ms: u64,
    /// Tags.
    pub tags: Vec<String>,
}

/// A page of runs.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunListResponse {
    /// Runs.
    pub runs: Vec<RunSummaryDto>,
    /// Token for the next page, if any.
    pub next_page_token: Option<String>,
}

// ---------------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------------

/// RFC 3339 rendering of a ms-since-epoch timestamp.
pub fn rfc3339_ms(ms: u64) -> String {
    i64::try_from(ms)
        .ok()
        .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        .map(|t| t.to_string())
        .unwrap_or_default()
}

fn parse_rfc3339_ms(field: &str, s: &str) -> Result<u64, ApiError> {
    let ts: jiff::Timestamp = s.parse().map_err(|e| {
        ApiError::invalid_argument(format!("{field}: not an RFC 3339 timestamp ({e})"))
    })?;
    u64::try_from(ts.as_millisecond())
        .map_err(|_| ApiError::invalid_argument(format!("{field}: before the Unix epoch")))
}

/// Embedded JSON value from JSON text (invalid text renders as a JSON string).
fn embed_json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

impl From<domain::ModuleInfo> for ModuleDto {
    fn from(m: domain::ModuleInfo) -> Self {
        ModuleDto {
            sha256: m.sha256,
            size_bytes: m.size_bytes,
            language: m.language,
            workflows: m
                .workflows
                .into_iter()
                .map(|w| WorkflowRefDto {
                    name: w.name,
                    version: w.version,
                })
                .collect(),
            uploaded_at: rfc3339_ms(m.uploaded_at_ms),
            uploaded_at_ms: m.uploaded_at_ms,
        }
    }
}

impl From<domain::WorkflowInfo> for WorkflowDto {
    fn from(w: domain::WorkflowInfo) -> Self {
        WorkflowDto {
            name: w.name,
            active_version: w.active_version,
            versions: w
                .versions
                .into_iter()
                .map(|v| WorkflowVersionDto {
                    version: v.version,
                    module_sha256: v.module_sha256,
                    default_queue: v.default_queue,
                    run_timeout_ms: v.run_timeout_ms,
                })
                .collect(),
            record_version: w.record_version,
        }
    }
}

impl From<domain::RunInfo> for RunDto {
    fn from(r: domain::RunInfo) -> Self {
        RunDto {
            run_id: r.run_id,
            workflow: r.workflow,
            version: r.version,
            module_sha256: r.module_sha256,
            queue: r.queue,
            status: r.status.into(),
            attempt: r.attempt,
            tags: r.tags,
            tenant: r.tenant,
            traceparent: r.traceparent,
            priority: r.priority,
            concurrency_key: r.concurrency_key,
            created_at: rfc3339_ms(r.created_at_ms),
            created_at_ms: r.created_at_ms,
            completed_at: r.completed_at_ms.map(rfc3339_ms),
            completed_at_ms: r.completed_at_ms,
            published_status: r.published_status_json.as_deref().map(embed_json),
            result: r.result_json.as_deref().map(embed_json),
            failure: r.failure,
        }
    }
}

impl From<domain::RunSummary> for RunSummaryDto {
    fn from(r: domain::RunSummary) -> Self {
        RunSummaryDto {
            run_id: r.run_id,
            workflow: r.workflow,
            version: r.version,
            status: r.status.into(),
            queue: r.queue,
            created_at: rfc3339_ms(r.created_at_ms),
            created_at_ms: r.created_at_ms,
            tags: r.tags,
        }
    }
}

fn body_rejection(status: StatusCode, text: String) -> ApiError {
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(text)
    } else {
        ApiError::invalid_argument(text)
    }
}

fn json_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    body.map(|Json(v)| v)
        .map_err(|r| body_rejection(r.status(), r.body_text()))
}

fn query<T>(q: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    q.map(|Query(v)| v)
        .map_err(|r| ApiError::invalid_argument(r.body_text()))
}

/// `If-Match: "7"` / `W/"7"` / `7` -> 7.
fn if_match_version(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    let Some(v) = headers.get(header::IF_MATCH) else {
        return Ok(None);
    };
    let s = v
        .to_str()
        .map_err(|_| ApiError::invalid_argument("If-Match: not ASCII"))?
        .trim();
    let s = s.strip_prefix("W/").unwrap_or(s).trim_matches('"');
    s.parse::<u64>()
        .map(Some)
        .map_err(|_| ApiError::invalid_argument("If-Match: expected a record version number"))
}

// ---------------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------------

/// Upload a Wasm module (content-addressed, idempotent).
#[utoipa::path(
    put,
    path = "/v1/modules/{sha256}",
    tag = "modules",
    params(
        ("sha256" = String, Path, description = "Lowercase hex sha256 of the body"),
        UploadModuleQuery,
    ),
    request_body(
        description = "Raw Wasm component bytes",
        content((WasmBytes = "application/wasm"), (WasmBytes = "application/octet-stream")),
    ),
    responses(
        (status = 201, description = "Module stored", body = UploadModuleResponse),
        (status = 200, description = "Module already existed", body = UploadModuleResponse),
        (status = 400, description = "sha256 mismatch or invalid module", body = ErrorBody),
        (status = 409, description = "Workflow version registered by another module", body = ErrorBody),
        (status = 413, description = "Module exceeds api.max_module_bytes", body = ErrorBody),
    )
)]
async fn upload_module(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(sha256): Path<String>,
    q: Result<Query<UploadModuleQuery>, QueryRejection>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let q = query(q)?;
    let wasm = body.map_err(|r| body_rejection(r.status(), r.body_text()))?;
    let res = cp
        .upload_module(domain::UploadModule {
            sha256,
            wasm,
            language: q.language,
        })
        .await?;
    let status = if res.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let dto = UploadModuleResponse {
        module: res.module.into(),
        created: res.created,
    };
    Ok((status, Json(dto)).into_response())
}

/// Get a stored module.
#[utoipa::path(
    get,
    path = "/v1/modules/{sha256}",
    tag = "modules",
    params(("sha256" = String, Path, description = "Lowercase hex sha256")),
    responses(
        (status = 200, description = "The module", body = ModuleDto),
        (status = 404, description = "Unknown module", body = ErrorBody),
    )
)]
async fn get_module(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(sha256): Path<String>,
) -> Result<Json<ModuleDto>, ApiError> {
    Ok(Json(cp.get_module(sha256).await?.into()))
}

/// List workflows.
#[utoipa::path(
    get,
    path = "/v1/workflows",
    tag = "workflows",
    responses((status = 200, description = "All workflows", body = ListWorkflowsResponse))
)]
async fn list_workflows(
    State(cp): State<Arc<dyn ControlPlane>>,
) -> Result<Json<ListWorkflowsResponse>, ApiError> {
    let workflows = cp
        .list_workflows()
        .await?
        .into_iter()
        .map(Into::into)
        .collect();
    Ok(Json(ListWorkflowsResponse { workflows }))
}

/// Activate a workflow version.
#[utoipa::path(
    put,
    path = "/v1/workflows/{name}/active",
    tag = "workflows",
    params(
        ("name" = String, Path, description = "Workflow name"),
        ("If-Match" = Option<String>, Header, description = "Expected record version (alternative to expected_version)"),
    ),
    request_body = ActivateWorkflowBody,
    responses(
        (status = 200, description = "Updated workflow", body = WorkflowDto),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown workflow or version", body = ErrorBody),
        (status = 412, description = "Record version mismatch", body = ErrorBody),
    )
)]
async fn activate_workflow(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ActivateWorkflowBody>, JsonRejection>,
) -> Result<Json<WorkflowDto>, ApiError> {
    let body = json_body(body)?;
    let header_version = if_match_version(&headers)?;
    let expected_version = match (body.expected_version, header_version) {
        (Some(b), Some(h)) if b != h => {
            return Err(ApiError::invalid_argument(
                "expected_version and If-Match disagree",
            ));
        }
        (b, h) => b.or(h),
    };
    let wf = cp
        .activate_workflow(domain::ActivateWorkflow {
            name,
            version: body.version,
            expected_version,
        })
        .await?;
    Ok(Json(wf.into()))
}

/// Start a run.
#[utoipa::path(
    post,
    path = "/v1/workflows/{name}/runs",
    tag = "runs",
    params(("name" = String, Path, description = "Workflow name")),
    request_body = StartRunBody,
    responses(
        (status = 201, description = "Run created", body = StartRunResponse),
        (status = 200, description = "Existing run returned (idempotency key)", body = StartRunResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown workflow or version", body = ErrorBody),
        (status = 409, description = "Idempotency key used (conflict_policy=reject); details.existing_run_id", body = ErrorBody),
        (status = 412, description = "Workflow has no active version", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn start_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    body: Result<Json<StartRunBody>, JsonRejection>,
) -> Result<Response, ApiError> {
    let b = json_body(body)?;
    let input_json = serde_json::to_vec(&b.input)
        .map_err(|e| ApiError::invalid_argument(format!("input: {e}")))?;
    let res = cp
        .start_run(domain::StartRunRequest {
            workflow: name,
            input_json,
            version: b.version,
            queue: b.queue,
            delay_ms: b.delay_ms,
            priority: b.priority,
            tags: b.tags,
            traceparent: b.traceparent,
            tenant: b.tenant,
            idempotency_key: b.idempotency_key,
            conflict_policy: b.conflict_policy.unwrap_or_default().into(),
            concurrency_key: b.concurrency_key,
        })
        .await?;
    let status = if res.created {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let dto = StartRunResponse {
        run_id: res.run_id,
        created: res.created,
    };
    Ok((status, Json(dto)).into_response())
}

/// Get a run.
#[utoipa::path(
    get,
    path = "/v1/runs/{run_id}",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id")),
    responses(
        (status = 200, description = "The run", body = RunDto),
        (status = 404, description = "Unknown run", body = ErrorBody),
    )
)]
async fn get_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
) -> Result<Json<RunDto>, ApiError> {
    Ok(Json(cp.get_run(run_id).await?.into()))
}

/// A page of a run's history.
#[utoipa::path(
    get,
    path = "/v1/runs/{run_id}/history",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id"), HistoryQuery),
    responses(
        (status = 200, description = "History page", body = HistoryResponse),
        (status = 404, description = "Unknown run", body = ErrorBody),
    )
)]
async fn run_history(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
    q: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<HistoryResponse>, ApiError> {
    let q = query(q)?;
    let page = cp
        .run_history(domain::HistoryRequest {
            run_id,
            from_seq: q.from_seq.unwrap_or(0),
            limit: domain::effective_limit(q.limit.unwrap_or(0)),
        })
        .await?;
    Ok(Json(HistoryResponse {
        events: page
            .events
            .into_iter()
            .map(|e| HistoryEventDto {
                seq: e.seq,
                kind: e.kind,
                ts: rfc3339_ms(e.ts_ms),
                ts_ms: e.ts_ms,
                summary: e.summary,
                details: embed_json(&e.details_json),
            })
            .collect(),
        next_from_seq: page.next_from_seq,
    }))
}

/// List runs.
#[utoipa::path(
    get,
    path = "/v1/runs",
    tag = "runs",
    params(ListRunsQuery),
    responses(
        (status = 200, description = "A page of runs", body = RunListResponse),
        (status = 400, description = "Invalid filter or page token", body = ErrorBody),
    )
)]
async fn list_runs(
    State(cp): State<Arc<dyn ControlPlane>>,
    q: Result<Query<ListRunsQuery>, QueryRejection>,
) -> Result<Json<RunListResponse>, ApiError> {
    let q = query(q)?;
    let created_after_ms = q
        .created_after
        .as_deref()
        .map(|s| parse_rfc3339_ms("created_after", s))
        .transpose()?;
    let created_before_ms = q
        .created_before
        .as_deref()
        .map(|s| parse_rfc3339_ms("created_before", s))
        .transpose()?;
    let page = cp
        .list_runs(domain::ListRunsRequest {
            workflow: q.workflow,
            status: q.status.map(Into::into),
            queue: q.queue,
            tag: q.tag,
            created_after_ms,
            created_before_ms,
            limit: domain::effective_limit(q.limit.unwrap_or(0)),
            page_token: q.page_token,
        })
        .await?;
    Ok(Json(RunListResponse {
        runs: page.runs.into_iter().map(Into::into).collect(),
        next_page_token: page.next_page_token,
    }))
}

/// Liveness: always 200.
#[utoipa::path(
    get,
    path = "/healthz",
    tag = "health",
    responses((status = 200, description = "Process is alive", body = String, content_type = "text/plain"))
)]
async fn healthz() -> &'static str {
    "ok"
}

/// Readiness: 200 iff meta has a leader and every partition is ready.
#[utoipa::path(
    get,
    path = "/readyz",
    tag = "health",
    responses(
        (status = 200, description = "Ready", body = String, content_type = "text/plain"),
        (status = 503, description = "Not ready", body = String, content_type = "text/plain"),
    )
)]
async fn readyz(State(cp): State<Arc<dyn ControlPlane>>) -> Response {
    if cp.ready() {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
    }
}

/// The OpenAPI document of this API.
#[utoipa::path(
    get,
    path = "/openapi.json",
    tag = "meta",
    responses((status = 200, description = "OpenAPI document", content_type = "application/json"))
)]
async fn openapi_json() -> Response {
    static JSON: OnceLock<String> = OnceLock::new();
    let body = JSON.get_or_init(|| {
        serde_json::to_string(&crate::openapi::openapi()).unwrap_or_else(|_| "{}".to_owned())
    });
    ([(header::CONTENT_TYPE, "application/json")], body.clone()).into_response()
}

/// Web UI shell (placeholder until the UI phase).
#[utoipa::path(
    get,
    path = "/",
    tag = "meta",
    responses((status = 200, description = "UI shell", body = String, content_type = "text/html"))
)]
async fn ui_shell() -> Html<String> {
    Html(crate::ui_shell_html())
}

// ---------------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------------

/// Base OpenAPI metadata; paths are added by [`api_routes`].
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Ostro API",
        description = "Ostro control plane: modules, workflows and runs. The same operations are served over gRPC (ostro.api.v1) on the same port."
    ),
    tags(
        (name = "modules", description = "Wasm module storage"),
        (name = "workflows", description = "Workflow versions and activation"),
        (name = "runs", description = "Starting and inspecting runs"),
        (name = "health", description = "Liveness and readiness"),
        (name = "meta", description = "API document and UI shell"),
    )
)]
pub struct ApiDoc;

/// The REST routes with their OpenAPI description, without state.
///
/// `max_module_bytes` applies only to `PUT /v1/modules/{sha256}`; every other route
/// gets the router-wide limit installed by [`crate::router`].
pub fn api_routes(max_module_bytes: usize) -> OpenApiRouter<CpState> {
    let mut doc = ApiDoc::openapi();
    doc.info.version = env!("CARGO_PKG_VERSION").to_owned();
    OpenApiRouter::with_openapi(doc)
        .routes(routes!(upload_module).layer(DefaultBodyLimit::max(max_module_bytes)))
        .routes(routes!(get_module))
        .routes(routes!(list_workflows))
        .routes(routes!(activate_workflow))
        .routes(routes!(start_run))
        .routes(routes!(list_runs))
        .routes(routes!(get_run))
        .routes(routes!(run_history))
        .routes(routes!(healthz))
        .routes(routes!(readyz))
        .routes(routes!(openapi_json))
        .routes(routes!(ui_shell))
}
