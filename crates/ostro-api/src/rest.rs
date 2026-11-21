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
use crate::phase7;

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
    /// Uploading tenant (`^[a-z0-9][a-z0-9-]{0,62}$`); absent = the default tenant.
    pub tenant: Option<String>,
    /// Deployed module name (`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`), recorded on the
    /// workflow versions this upload registers.
    pub name: Option<String>,
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
    /// Schedule-to-start timeout override (friendly duration, e.g. `"30s"`; `"0s"` = none).
    /// Unset: the workflow policy, then the queue policy.
    #[serde(default)]
    pub schedule_to_start_timeout: Option<String>,
    /// Retention override for the terminal run (friendly duration in hours or smaller
    /// units, e.g. `"168h"`; `7d` is rejected). Unset: workflow policy, queue policy, then
    /// the server's `[runs] retention`.
    #[serde(default)]
    pub retention: Option<String>,
    /// Validate only: the same checks as a real start, nothing is submitted. The response
    /// reports the would-be outcome (`run_id` = the existing run for a reused idempotency
    /// key, else empty).
    #[serde(default)]
    pub dry_run: bool,
}

/// Start-run result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct StartRunResponse {
    /// Run id.
    pub run_id: String,
    /// `false` when an existing run was returned.
    pub created: bool,
    /// `true` for a dry run (omitted otherwise). A dry run returns the existing run's id
    /// for a reused idempotency key, else an empty `run_id`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dry_run: bool,
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
    /// Durable count of retryable attempt failures: the meaningful retry count
    /// (`attempt` counts dispatches, including resumes).
    #[serde(default)]
    pub attempt_failures: u32,
    /// Resolved retention of the terminal run, ms (absent = keep forever).
    #[serde(default)]
    pub retention_ms: Option<u64>,
    /// Resolved schedule-to-start timeout, ms (absent = none).
    #[serde(default)]
    pub schedule_to_start_ms: Option<u64>,
    /// Pause mark while the run is paused (reason, detail, time); absent = not paused.
    #[serde(default)]
    pub pause: Option<crate::phase7::RunPauseMark>,
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

// --- Phase 5 lifecycle (API-05/06/07) and run policies -----------------------------

/// Signal body.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct SignalBody {
    /// Signal payload (any JSON value; defaults to null).
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Optional dedupe id (<= 64 bytes): a repeat is acknowledged with
    /// `duplicate: true` and not delivered again.
    #[serde(default)]
    pub request_id: Option<String>,
    /// Validate only; nothing is submitted.
    #[serde(default)]
    pub dry_run: bool,
}

/// Signal result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SignalResponseDto {
    /// The signal was (or would be) accepted.
    pub accepted: bool,
    /// `request_id` was already seen for this run.
    pub duplicate: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Cancel / terminate body (the whole body is optional).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct StopRunBody {
    /// Reason (<= 1024 bytes), recorded in history.
    #[serde(default)]
    pub reason: Option<String>,
    /// Validate only; nothing is submitted.
    #[serde(default)]
    pub dry_run: bool,
}

/// Cancel result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CancelResponseDto {
    /// Cancellation had already been requested.
    pub already_requested: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Terminate result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TerminateResponseDto {
    /// Status afterwards (`terminated`).
    pub status: RunStatusDto,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Retry body (the whole body is optional).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RetryBody {
    /// Validate only; reports the mode the retry would use.
    #[serde(default)]
    pub dry_run: bool,
}

/// How a retry was carried out.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetryModeDto {
    /// The same run resumes from its journal.
    Resumed,
    /// A new run linked to the old one was started.
    Restarted,
}

impl From<domain::RetryMode> for RetryModeDto {
    fn from(m: domain::RetryMode) -> Self {
        match m {
            domain::RetryMode::Resumed => RetryModeDto::Resumed,
            domain::RetryMode::Restarted => RetryModeDto::Restarted,
        }
    }
}

/// Retry result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RetryResponseDto {
    /// Resume or restart.
    pub mode: RetryModeDto,
    /// The resumed run, or the new run (`restarted`; the original id for dry_run).
    pub run_id: String,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Long-poll query.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ResultQuery {
    /// Maximum wait as a friendly duration (`30s`, `500ms`); default 0 (answer now),
    /// capped at 60s.
    pub wait: Option<String>,
    /// Follow continue-as-new successors to the last run of the chain.
    pub follow_continuations: Option<bool>,
}

/// Long-poll result (200 whether or not the run is terminal).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunResultDto {
    /// The run answered for (the last successor when following continuations).
    pub run_id: String,
    /// Current status.
    pub status: RunStatusDto,
    /// True when `status` is terminal.
    pub completed: bool,
    /// Result of a completed run (absent otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    /// Failure of a failed run (absent otherwise).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<serde_json::Value>,
}

/// Await-token completion body. The token travels only here, never in a URL.
#[derive(Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct CompleteTokenBody {
    /// The `ot1_...` token.
    pub token: String,
    /// Completion payload (any JSON value; defaults to null).
    #[serde(default)]
    pub payload: serde_json::Value,
    /// Validate only (format, MAC, op still open); nothing is submitted.
    #[serde(default)]
    pub dry_run: bool,
}

impl std::fmt::Debug for CompleteTokenBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteTokenBody")
            .field("token", &"<redacted>")
            .field("dry_run", &self.dry_run)
            .finish_non_exhaustive()
    }
}

/// Await-token completion result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct CompleteTokenResponseDto {
    /// The run the token belongs to.
    pub run_id: String,
    /// The completion was (or would be) accepted.
    pub accepted: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Run policy body: friendly durations (`168h`, `30s`; write days as hours).
/// Omitted fields are cleared (PUT replaces the whole policy).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RunPolicyBody {
    /// Retention of terminal runs and their journals.
    #[serde(default)]
    pub retention: Option<String>,
    /// Schedule-to-start timeout for the initial start.
    #[serde(default)]
    pub schedule_to_start_timeout: Option<String>,
}

/// A stored run policy (unset = no override at this level).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RunPolicyDto {
    /// Retention as a friendly duration.
    pub retention: Option<String>,
    /// Retention in ms.
    pub retention_ms: Option<u64>,
    /// Schedule-to-start timeout as a friendly duration.
    pub schedule_to_start_timeout: Option<String>,
    /// Schedule-to-start timeout in ms.
    pub schedule_to_start_ms: Option<u64>,
}

impl From<domain::RunPolicy> for RunPolicyDto {
    fn from(p: domain::RunPolicy) -> Self {
        RunPolicyDto {
            retention: p.retention_ms.map(friendly_ms),
            retention_ms: p.retention_ms,
            schedule_to_start_timeout: p.schedule_to_start_ms.map(friendly_ms),
            schedule_to_start_ms: p.schedule_to_start_ms,
        }
    }
}

// ---------------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------------

static SPAN_PARSER: jiff::fmt::friendly::SpanParser = jiff::fmt::friendly::SpanParser::new();
static SPAN_PRINTER: jiff::fmt::friendly::SpanPrinter = jiff::fmt::friendly::SpanPrinter::new();

/// Parse a friendly duration (`"30s"`, `"500ms"`, `"168h"`, `"1m 30s"`) to whole ms,
/// with the same jiff parser `ServerConfig` uses. Calendar units (`7d`) are rejected;
/// sub-millisecond parts are truncated.
pub fn parse_friendly_ms(field: &str, s: &str) -> Result<u64, ApiError> {
    let d = SPAN_PARSER.parse_unsigned_duration(s.trim()).map_err(|e| {
        ApiError::invalid_argument(format!(
            "{field}: invalid duration {s:?} (expected e.g. \"30s\", \"500ms\", \"168h\"): {e}"
        ))
        .with_details(serde_json::json!({ "field": field }))
    })?;
    u64::try_from(d.as_millis()).map_err(|_| {
        ApiError::invalid_argument(format!("{field}: duration {s:?} is too large"))
            .with_details(serde_json::json!({ "field": field }))
    })
}

/// Friendly rendering of a ms duration (`604800000` -> `"168h"`).
pub fn friendly_ms(ms: u64) -> String {
    SPAN_PRINTER.unsigned_duration_to_string(&std::time::Duration::from_millis(ms))
}

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
            attempt_failures: r.attempt_failures,
            retention_ms: r.retention_ms,
            schedule_to_start_ms: r.schedule_to_start_ms,
            pause: r.pause,
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

pub(crate) fn body_rejection(status: StatusCode, text: String) -> ApiError {
    if status == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large(text)
    } else {
        ApiError::invalid_argument(text)
    }
}

pub(crate) fn json_body<T>(body: Result<Json<T>, JsonRejection>) -> Result<T, ApiError> {
    body.map(|Json(v)| v)
        .map_err(|r| body_rejection(r.status(), r.body_text()))
}

/// JSON body that may be omitted entirely (empty body = `T::default()`). Used by the
/// lifecycle POSTs whose every field is optional, so `curl -X POST .../cancel` works.
pub(crate) fn optional_json_body<T: Default + serde::de::DeserializeOwned>(
    body: Result<Bytes, BytesRejection>,
) -> Result<T, ApiError> {
    let bytes = body.map_err(|r| body_rejection(r.status(), r.body_text()))?;
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(T::default());
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| ApiError::invalid_argument(format!("invalid JSON body: {e}")))
}

pub(crate) fn query<T>(q: Result<Query<T>, QueryRejection>) -> Result<T, ApiError> {
    q.map(|Query(v)| v)
        .map_err(|r| ApiError::invalid_argument(r.body_text()))
}

/// `If-Match: "7"` / `W/"7"` / `7` -> 7.
pub(crate) fn if_match_version(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
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
            tenant: q.tenant,
            name: q.name,
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
    params(
        ("traceparent" = Option<String>, Header, description = "W3C traceparent (00-<trace-id>-<parent-id>-<flags>); used when the body's traceparent is empty. Invalid: 400, details.reason = invalid_traceparent"),
        ("name" = String, Path, description = "Workflow name"),
    ),
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
    headers: HeaderMap,
    body: Result<Json<StartRunBody>, JsonRejection>,
) -> Result<Response, ApiError> {
    let b = json_body(body)?;
    let input_json = serde_json::to_vec(&b.input)
        .map_err(|e| ApiError::invalid_argument(format!("input: {e}")))?;
    // Body wins; else the `traceparent` header (08-10). Validated either way.
    let traceparent = crate::phase8::resolve_traceparent(
        b.traceparent,
        crate::phase8::traceparent_header(&headers)?,
    )?;
    let res = cp
        .start_run(domain::StartRunRequest {
            workflow: name,
            input_json,
            version: b.version,
            queue: b.queue,
            delay_ms: b.delay_ms,
            priority: b.priority,
            tags: b.tags,
            traceparent,
            tenant: b.tenant,
            idempotency_key: b.idempotency_key,
            conflict_policy: b.conflict_policy.unwrap_or_default().into(),
            concurrency_key: b.concurrency_key,
            schedule_to_start_ms: b
                .schedule_to_start_timeout
                .as_deref()
                .map(|s| parse_friendly_ms("schedule_to_start_timeout", s))
                .transpose()?,
            retention_ms: b
                .retention
                .as_deref()
                .map(|s| parse_friendly_ms("retention", s))
                .transpose()?,
            dry_run: b.dry_run,
        })
        .await?;
    // A dry run never creates anything: always 200.
    let status = if res.created && !res.dry_run {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    let dto = StartRunResponse {
        run_id: res.run_id,
        created: res.created,
        dry_run: res.dry_run,
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

/// Send a named signal to a run.
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/signals/{name}",
    tag = "runs",
    params(
        ("run_id" = String, Path, description = "Run id"),
        ("name" = String, Path, description = "Signal name (1..=256 bytes)"),
    ),
    request_body = SignalBody,
    responses(
        (status = 200, description = "Signal accepted (or validated, for dry_run)", body = SignalResponseDto),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
        (status = 409, description = "Run already terminal (details.reason = run_terminal)", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn signal_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path((run_id, name)): Path<(String, String)>,
    body: Result<Json<SignalBody>, JsonRejection>,
) -> Result<Json<SignalResponseDto>, ApiError> {
    let b = json_body(body)?;
    let r = cp
        .signal_run(domain::SignalRequest {
            run_id,
            name,
            payload: b.payload,
            request_id: b.request_id,
            dry_run: b.dry_run,
        })
        .await?;
    Ok(Json(SignalResponseDto {
        accepted: r.accepted,
        duplicate: r.duplicate,
        dry_run: r.dry_run,
    }))
}

/// Request graceful cancellation of a run (idempotent).
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/cancel",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id")),
    request_body(content = StopRunBody, description = "Optional; an empty body means no reason, not a dry run"),
    responses(
        (status = 200, description = "Cancellation requested (already_requested on repeats)", body = CancelResponseDto),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
        (status = 409, description = "Run already terminal (details.reason = run_terminal)", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn cancel_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<CancelResponseDto>, ApiError> {
    let b: StopRunBody = optional_json_body(body)?;
    let r = cp
        .cancel_run(domain::CancelRequest {
            run_id,
            reason: b.reason,
            dry_run: b.dry_run,
        })
        .await?;
    Ok(Json(CancelResponseDto {
        already_requested: r.already_requested,
        dry_run: r.dry_run,
    }))
}

/// Hard-stop a run (status `terminated`; no guest cleanup).
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/terminate",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id")),
    request_body(content = StopRunBody, description = "Optional; an empty body means no reason, not a dry run"),
    responses(
        (status = 200, description = "Run terminated (or validated, for dry_run)", body = TerminateResponseDto),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
        (status = 409, description = "Run already terminal (details.reason = run_terminal)", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn terminate_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<TerminateResponseDto>, ApiError> {
    let b: StopRunBody = optional_json_body(body)?;
    let r = cp
        .terminate_run(domain::TerminateRequest {
            run_id,
            reason: b.reason,
            dry_run: b.dry_run,
        })
        .await?;
    Ok(Json(TerminateResponseDto {
        status: r.status.into(),
        dry_run: r.dry_run,
    }))
}

/// Retry a terminal run: resume in place when resumable, else restart as a new run.
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/retry",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id")),
    request_body(content = RetryBody, description = "Optional; an empty body is not a dry run"),
    responses(
        (status = 200, description = "Run resumed or restarted (or the would-be mode, for dry_run)", body = RetryResponseDto),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
        (status = 409, description = "Run completed successfully (details.reason = run_completed) or is not terminal (run_not_terminal)", body = ErrorBody),
    )
)]
async fn retry_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<RetryResponseDto>, ApiError> {
    let b: RetryBody = optional_json_body(body)?;
    let r = cp
        .retry_run(domain::RetryRequest {
            run_id,
            dry_run: b.dry_run,
        })
        .await?;
    Ok(Json(RetryResponseDto {
        mode: r.mode.into(),
        run_id: r.run_id,
        dry_run: r.dry_run,
    }))
}

/// Long-poll a run's outcome.
#[utoipa::path(
    get,
    path = "/v1/runs/{run_id}/result",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id"), ResultQuery),
    responses(
        (status = 200, description = "Current outcome; completed = false when the wait ended first", body = RunResultDto),
        (status = 400, description = "Invalid wait duration", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
    )
)]
async fn run_result(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
    q: Result<Query<ResultQuery>, QueryRejection>,
) -> Result<Json<RunResultDto>, ApiError> {
    let q = query(q)?;
    let wait_ms = q
        .wait
        .as_deref()
        .map(|w| parse_friendly_ms("wait", w))
        .transpose()?
        .unwrap_or(0);
    let r = cp
        .await_result(domain::AwaitResultRequest {
            run_id,
            wait_ms: domain::effective_wait_ms(wait_ms),
            follow_continuations: q.follow_continuations.unwrap_or(false),
        })
        .await?;
    Ok(Json(RunResultDto {
        run_id: r.run_id,
        status: r.status.into(),
        completed: r.completed,
        result: r.result,
        failure: r.failure,
    }))
}

/// Complete an await token.
#[utoipa::path(
    post,
    path = "/v1/tokens/complete",
    tag = "tokens",
    request_body = CompleteTokenBody,
    responses(
        (status = 200, description = "Token completed (or validated, for dry_run)", body = CompleteTokenResponseDto),
        (status = 400, description = "Malformed or unverifiable token (details.reason = token_invalid)", body = ErrorBody),
        (status = 404, description = "The token's run no longer exists", body = ErrorBody),
        (status = 409, description = "Token already completed (token_already_completed) or run terminal (run_terminal)", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn complete_token(
    State(cp): State<Arc<dyn ControlPlane>>,
    body: Result<Json<CompleteTokenBody>, JsonRejection>,
) -> Result<Json<CompleteTokenResponseDto>, ApiError> {
    let b = json_body(body)?;
    let r = cp
        .complete_token(domain::CompleteTokenRequest {
            token: b.token,
            payload: b.payload,
            dry_run: b.dry_run,
        })
        .await?;
    Ok(Json(CompleteTokenResponseDto {
        run_id: r.run_id,
        accepted: r.accepted,
        dry_run: r.dry_run,
    }))
}

fn policy_from_body(b: RunPolicyBody) -> Result<domain::RunPolicy, ApiError> {
    Ok(domain::RunPolicy {
        retention_ms: b
            .retention
            .as_deref()
            .map(|s| parse_friendly_ms("retention", s))
            .transpose()?,
        schedule_to_start_ms: b
            .schedule_to_start_timeout
            .as_deref()
            .map(|s| parse_friendly_ms("schedule_to_start_timeout", s))
            .transpose()?,
    })
}

/// Replace a workflow's run policy (future runs only).
#[utoipa::path(
    put,
    path = "/v1/workflows/{name}/policy",
    tag = "workflows",
    params(("name" = String, Path, description = "Workflow name")),
    request_body = RunPolicyBody,
    responses(
        (status = 200, description = "Stored policy", body = RunPolicyDto),
        (status = 400, description = "Invalid duration", body = ErrorBody),
        (status = 404, description = "Unknown workflow", body = ErrorBody),
    )
)]
async fn set_workflow_policy(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    body: Result<Json<RunPolicyBody>, JsonRejection>,
) -> Result<Json<RunPolicyDto>, ApiError> {
    let p = policy_from_body(json_body(body)?)?;
    Ok(Json(cp.set_workflow_policy(name, p).await?.into()))
}

/// A workflow's run policy.
#[utoipa::path(
    get,
    path = "/v1/workflows/{name}/policy",
    tag = "workflows",
    params(("name" = String, Path, description = "Workflow name")),
    responses(
        (status = 200, description = "Stored policy (all null when never set)", body = RunPolicyDto),
        (status = 404, description = "Unknown workflow", body = ErrorBody),
    )
)]
async fn get_workflow_policy(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<RunPolicyDto>, ApiError> {
    Ok(Json(cp.get_workflow_policy(name).await?.into()))
}

/// Replace a queue's run policy (future runs only).
#[utoipa::path(
    put,
    path = "/v1/queues/{name}/policy",
    tag = "queues",
    params(("name" = String, Path, description = "Queue name")),
    request_body = RunPolicyBody,
    responses(
        (status = 200, description = "Stored policy", body = RunPolicyDto),
        (status = 400, description = "Invalid duration", body = ErrorBody),
    )
)]
async fn set_queue_policy(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    body: Result<Json<RunPolicyBody>, JsonRejection>,
) -> Result<Json<RunPolicyDto>, ApiError> {
    let p = policy_from_body(json_body(body)?)?;
    Ok(Json(cp.set_queue_policy(name, p).await?.into()))
}

/// A queue's run policy.
#[utoipa::path(
    get,
    path = "/v1/queues/{name}/policy",
    tag = "queues",
    params(("name" = String, Path, description = "Queue name")),
    responses((status = 200, description = "Stored policy (all null when never set)", body = RunPolicyDto))
)]
async fn get_queue_policy(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<RunPolicyDto>, ApiError> {
    Ok(Json(cp.get_queue_policy(name).await?.into()))
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
// Phase 6 bridge administration (D-FX9, P-16; 06-08): secrets, grants, connections,
// egress policies, and the unauthenticated local presign route.
//
// Request bodies of these routes are never logged: the trace layer records only the
// request line and status, and `PutSecretBody`'s `Debug` redacts the value.
// ---------------------------------------------------------------------------------

/// `PUT /v1/secrets/{name}` body: exactly one of `value` (UTF-8) or `value_b64`
/// (standard base64, padding optional). Write-only.
#[derive(Clone, Default, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PutSecretBody {
    /// The value as UTF-8 text.
    #[schema(write_only)]
    pub value: Option<String>,
    /// The value as standard base64.
    #[schema(write_only)]
    pub value_b64: Option<String>,
}

impl std::fmt::Debug for PutSecretBody {
    /// Redacts both value fields so they never reach logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutSecretBody")
            .field("value", &self.value.as_ref().map(|_| "<redacted>"))
            .field("value_b64", &self.value_b64.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

/// `?tenant=` of the secret routes.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct TenantQuery {
    /// Tenant scope; absent = global secrets.
    pub tenant: Option<String>,
}

/// Metadata of a stored secret (never its value).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SecretInfoDto {
    /// Secret name.
    pub name: String,
    /// Id of the sealing key the value is sealed under.
    pub key_id: String,
    /// Tenant scope; null = global.
    pub tenant: Option<String>,
}

impl From<domain::SecretInfo> for SecretInfoDto {
    fn from(s: domain::SecretInfo) -> Self {
        SecretInfoDto {
            name: s.name,
            key_id: s.key_id,
            tenant: s.tenant,
        }
    }
}

/// `GET /v1/secrets` response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SecretListResponse {
    /// Secrets sorted by name.
    pub secrets: Vec<SecretInfoDto>,
}

/// An origin-bound secret grant.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GrantDto {
    /// `secret:<module>:<secret>`; computed server-side (may be omitted on PUT).
    #[serde(default)]
    pub id: String,
    /// Tenant-qualified deployed module name (`<tenant>/<name>`).
    pub module: String,
    /// Secret name.
    pub secret: String,
    /// Origin patterns (`scheme://host[:port]`, `scheme://*.domain[:port]`).
    pub origins: Vec<String>,
    /// Placeholders allowed in the request body.
    #[serde(default)]
    pub body: bool,
}

impl From<domain::GrantInfo> for GrantDto {
    fn from(g: domain::GrantInfo) -> Self {
        GrantDto {
            id: g.id,
            module: g.module,
            secret: g.secret,
            origins: g.origins,
            body: g.body,
        }
    }
}

impl From<GrantDto> for domain::GrantInfo {
    fn from(g: GrantDto) -> Self {
        domain::GrantInfo {
            id: g.id,
            module: g.module,
            secret: g.secret,
            origins: g.origins,
            body: g.body,
        }
    }
}

/// `GET /v1/grants` response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct GrantListResponse {
    /// Grants sorted by id.
    pub grants: Vec<GrantDto>,
}

/// `?module=` of `GET /v1/grants`.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct GrantListQuery {
    /// Only this module's grants.
    pub module: Option<String>,
}

/// `PUT /v1/connections/{name}` body.
#[derive(Clone, Debug, PartialEq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PutConnectionBody {
    /// `sql-postgres`, `sql-mysql`, `object` or `http-target`.
    pub kind: String,
    /// Kind-specific config object with `"v": 1`; secrets only as `{{secret:NAME}}`.
    pub config: serde_json::Value,
}

/// A connection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionDto {
    /// Connection name.
    pub name: String,
    /// Connection kind.
    pub kind: String,
    /// Kind-specific config.
    pub config: serde_json::Value,
}

impl From<domain::ConnectionInfo> for ConnectionDto {
    fn from(c: domain::ConnectionInfo) -> Self {
        ConnectionDto {
            name: c.name,
            kind: c.kind,
            config: c.config,
        }
    }
}

/// `GET /v1/connections` response.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ConnectionListResponse {
    /// Connections sorted by name.
    pub connections: Vec<ConnectionDto>,
}

/// `PUT /v1/egress/{module}` body.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PutEgressBody {
    /// Origin patterns or `connection:<name>` entries.
    pub allow: Vec<String>,
}

/// A module's egress policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EgressDto {
    /// Tenant-qualified deployed module name.
    pub module: String,
    /// Origin patterns or `connection:<name>` entries.
    pub allow: Vec<String>,
}

impl From<domain::EgressInfo> for EgressDto {
    fn from(e: domain::EgressInfo) -> Self {
        EgressDto {
            module: e.module,
            allow: e.allow,
        }
    }
}

/// `GET /v1/egress` response.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct EgressListResponse {
    /// Policies sorted by module.
    pub policies: Vec<EgressDto>,
}

/// `?exp=&sig=` of a presigned object URL.
#[derive(Clone, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PresignQuery {
    /// Expiry, Unix seconds.
    pub exp: Option<u64>,
    /// Hex signature.
    pub sig: Option<String>,
}

/// Object bytes (documentation-only schema of presigned object bodies).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
pub struct ObjectBytes(#[allow(dead_code)] Vec<u8>);

/// Standard base64 (RFC 4648 §4), padding optional, no whitespace. ostro-api has no
/// base64 dependency and the lock file is fixed for this plan.
pub(crate) fn decode_base64(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    }
    let b = s.as_bytes();
    let b = b
        .strip_suffix(b"==")
        .or_else(|| b.strip_suffix(b"="))
        .unwrap_or(b);
    if b.len() % 4 == 1 || (s.len() != b.len() && !s.len().is_multiple_of(4)) {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() * 3 / 4);
    for chunk in b.chunks(4) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= val(c)? << (18 - 6 * i);
        }
        let bytes = acc.to_be_bytes();
        let n = chunk.len() - 1;
        // Non-canonical trailing bits are rejected.
        if chunk.len() < 4 && (acc & ((1 << (8 * (3 - n))) - 1)) != 0 {
            return None;
        }
        out.extend_from_slice(&bytes[1..=n]);
    }
    Some(out)
}

fn secret_value(b: PutSecretBody) -> Result<Vec<u8>, ApiError> {
    match (b.value, b.value_b64) {
        (Some(v), None) => Ok(v.into_bytes()),
        (None, Some(v)) => decode_base64(&v).ok_or_else(|| {
            ApiError::invalid_argument("value_b64: not valid standard base64")
                .with_details(serde_json::json!({ "field": "value_b64" }))
        }),
        _ => Err(
            ApiError::invalid_argument("exactly one of value or value_b64 is required")
                .with_details(serde_json::json!({ "field": "value" })),
        ),
    }
}

fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

/// Store a secret (create or replace). The value is write-only.
#[utoipa::path(
    put,
    path = "/v1/secrets/{name}",
    tag = "secrets",
    params(
        ("name" = String, Path, description = "Secret name (may contain `/`)"),
        TenantQuery,
    ),
    request_body = PutSecretBody,
    responses(
        (status = 200, description = "Secret stored (metadata only)", body = SecretInfoDto),
        (status = 400, description = "Invalid name, reserved name (secret_reserved) or value shorter than 8 bytes (secret_too_short)", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn put_secret(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    q: Result<Query<TenantQuery>, QueryRejection>,
    body: Result<Json<PutSecretBody>, JsonRejection>,
) -> Result<Json<SecretInfoDto>, ApiError> {
    let tenant = query(q)?.tenant;
    let value = secret_value(json_body(body)?)?;
    let info = cp
        .put_secret(domain::PutSecret {
            name,
            value,
            tenant,
        })
        .await?;
    Ok(Json(info.into()))
}

/// List secrets (metadata only; values are never returned).
#[utoipa::path(
    get,
    path = "/v1/secrets",
    tag = "secrets",
    params(TenantQuery),
    responses((status = 200, description = "Secrets", body = SecretListResponse))
)]
async fn list_secrets(
    State(cp): State<Arc<dyn ControlPlane>>,
    q: Result<Query<TenantQuery>, QueryRejection>,
) -> Result<Json<SecretListResponse>, ApiError> {
    let secrets = cp.list_secrets(query(q)?.tenant).await?;
    Ok(Json(SecretListResponse {
        secrets: secrets.into_iter().map(Into::into).collect(),
    }))
}

/// Delete a secret.
#[utoipa::path(
    delete,
    path = "/v1/secrets/{name}",
    tag = "secrets",
    params(
        ("name" = String, Path, description = "Secret name (may contain `/`)"),
        TenantQuery,
    ),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Unknown secret", body = ErrorBody),
    )
)]
async fn delete_secret(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    q: Result<Query<TenantQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    cp.delete_secret(name, query(q)?.tenant).await?;
    Ok(no_content())
}

/// Create or replace a secret grant (the id is computed server-side).
#[utoipa::path(
    put,
    path = "/v1/grants",
    tag = "secrets",
    request_body = GrantDto,
    responses(
        (status = 200, description = "Stored grant", body = GrantDto),
        (status = 400, description = "Invalid module, secret name, origin pattern or id", body = ErrorBody),
    )
)]
async fn put_grant(
    State(cp): State<Arc<dyn ControlPlane>>,
    body: Result<Json<GrantDto>, JsonRejection>,
) -> Result<Json<GrantDto>, ApiError> {
    let g = cp.put_grant(json_body(body)?.into()).await?;
    Ok(Json(g.into()))
}

/// List grants.
#[utoipa::path(
    get,
    path = "/v1/grants",
    tag = "secrets",
    params(GrantListQuery),
    responses((status = 200, description = "Grants", body = GrantListResponse))
)]
async fn list_grants(
    State(cp): State<Arc<dyn ControlPlane>>,
    q: Result<Query<GrantListQuery>, QueryRejection>,
) -> Result<Json<GrantListResponse>, ApiError> {
    let grants = cp.list_grants(query(q)?.module).await?;
    Ok(Json(GrantListResponse {
        grants: grants.into_iter().map(Into::into).collect(),
    }))
}

/// Delete a grant.
#[utoipa::path(
    delete,
    path = "/v1/grants/{id}",
    tag = "secrets",
    params(("id" = String, Path, description = "Grant id `secret:<module>:<secret>` (may contain `/`)")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Unknown grant", body = ErrorBody),
    )
)]
async fn delete_grant(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    cp.delete_grant(id).await?;
    Ok(no_content())
}

/// Create or replace a connection.
#[utoipa::path(
    put,
    path = "/v1/connections/{name}",
    tag = "connectors",
    params(("name" = String, Path, description = "Connection name, percent-decoded (http-target names are origins)")),
    request_body = PutConnectionBody,
    responses(
        (status = 200, description = "Stored connection", body = ConnectionDto),
        (status = 400, description = "Unknown kind or invalid config", body = ErrorBody),
    )
)]
async fn put_connection(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    body: Result<Json<PutConnectionBody>, JsonRejection>,
) -> Result<Json<ConnectionDto>, ApiError> {
    let b = json_body(body)?;
    let c = cp
        .put_connection(domain::ConnectionInfo {
            name,
            kind: b.kind,
            config: b.config,
        })
        .await?;
    Ok(Json(c.into()))
}

/// One connection.
#[utoipa::path(
    get,
    path = "/v1/connections/{name}",
    tag = "connectors",
    params(("name" = String, Path, description = "Connection name, percent-decoded")),
    responses(
        (status = 200, description = "Connection", body = ConnectionDto),
        (status = 404, description = "Unknown connection", body = ErrorBody),
    )
)]
async fn get_connection(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<ConnectionDto>, ApiError> {
    Ok(Json(cp.get_connection(name).await?.into()))
}

/// List connections.
#[utoipa::path(
    get,
    path = "/v1/connections",
    tag = "connectors",
    responses((status = 200, description = "Connections", body = ConnectionListResponse))
)]
async fn list_connections(
    State(cp): State<Arc<dyn ControlPlane>>,
) -> Result<Json<ConnectionListResponse>, ApiError> {
    let connections = cp.list_connections().await?;
    Ok(Json(ConnectionListResponse {
        connections: connections.into_iter().map(Into::into).collect(),
    }))
}

/// Delete a connection.
#[utoipa::path(
    delete,
    path = "/v1/connections/{name}",
    tag = "connectors",
    params(("name" = String, Path, description = "Connection name, percent-decoded")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "Unknown connection", body = ErrorBody),
    )
)]
async fn delete_connection(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Response, ApiError> {
    cp.delete_connection(name).await?;
    Ok(no_content())
}

/// Create or replace a module's egress policy.
#[utoipa::path(
    put,
    path = "/v1/egress/{module}",
    tag = "connectors",
    params(("module" = String, Path, description = "Tenant-qualified module name (contains `/`)")),
    request_body = PutEgressBody,
    responses(
        (status = 200, description = "Stored policy", body = EgressDto),
        (status = 400, description = "Invalid module name or allow entry", body = ErrorBody),
    )
)]
async fn put_egress(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(module): Path<String>,
    body: Result<Json<PutEgressBody>, JsonRejection>,
) -> Result<Json<EgressDto>, ApiError> {
    let b = json_body(body)?;
    let e = cp
        .put_egress(domain::EgressInfo {
            module,
            allow: b.allow,
        })
        .await?;
    Ok(Json(e.into()))
}

/// One module's egress policy.
#[utoipa::path(
    get,
    path = "/v1/egress/{module}",
    tag = "connectors",
    params(("module" = String, Path, description = "Tenant-qualified module name (contains `/`)")),
    responses(
        (status = 200, description = "Policy", body = EgressDto),
        (status = 404, description = "No policy for this module", body = ErrorBody),
    )
)]
async fn get_egress(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(module): Path<String>,
) -> Result<Json<EgressDto>, ApiError> {
    Ok(Json(cp.get_egress(module).await?.into()))
}

/// List egress policies.
#[utoipa::path(
    get,
    path = "/v1/egress",
    tag = "connectors",
    responses((status = 200, description = "Policies", body = EgressListResponse))
)]
async fn list_egress(
    State(cp): State<Arc<dyn ControlPlane>>,
) -> Result<Json<EgressListResponse>, ApiError> {
    let policies = cp.list_egress().await?;
    Ok(Json(EgressListResponse {
        policies: policies.into_iter().map(Into::into).collect(),
    }))
}

/// Delete a module's egress policy.
#[utoipa::path(
    delete,
    path = "/v1/egress/{module}",
    tag = "connectors",
    params(("module" = String, Path, description = "Tenant-qualified module name (contains `/`)")),
    responses(
        (status = 204, description = "Deleted"),
        (status = 404, description = "No policy for this module", body = ErrorBody),
    )
)]
async fn delete_egress(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(module): Path<String>,
) -> Result<Response, ApiError> {
    cp.delete_egress(module).await?;
    Ok(no_content())
}

fn presign_parts(
    q: Result<Query<PresignQuery>, QueryRejection>,
) -> Result<(u64, String), ApiError> {
    let q = query(q)?;
    match (q.exp, q.sig) {
        (Some(exp), Some(sig)) if !sig.is_empty() => Ok((exp, sig)),
        _ => Err(
            ApiError::invalid_argument("presigned object URLs need exp and sig")
                .with_details(serde_json::json!({ "field": "sig" })),
        ),
    }
}

fn presign_response(r: domain::PresignedObjectResponse) -> Response {
    let status = StatusCode::from_u16(r.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut resp = (status, axum::body::Body::from(r.body)).into_response();
    if let Some(ct) = r
        .content_type
        .and_then(|ct| axum::http::HeaderValue::from_str(&ct).ok())
    {
        resp.headers_mut().insert(header::CONTENT_TYPE, ct);
    }
    resp
}

/// Download through a locally presigned URL. Not behind API authentication: the
/// signature is the authorisation.
#[utoipa::path(
    get,
    path = "/v1/objects/{connection}/{key}",
    tag = "connectors",
    params(
        ("connection" = String, Path, description = "Object connection name"),
        ("key" = String, Path, description = "Object key (may contain `/`)"),
        PresignQuery,
    ),
    responses(
        (status = 200, description = "Object bytes", body = ObjectBytes, content_type = "application/octet-stream"),
        (status = 400, description = "Missing exp or sig", body = ErrorBody),
        (status = 403, description = "Bad or expired signature"),
        (status = 404, description = "Unknown object"),
    )
)]
async fn get_presigned_object(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path((connection, key)): Path<(String, String)>,
    q: Result<Query<PresignQuery>, QueryRejection>,
) -> Result<Response, ApiError> {
    let (exp, sig) = presign_parts(q)?;
    let r = cp
        .presigned_object(domain::PresignedObjectRequest {
            connection,
            key,
            method: domain::PresignMethod::Get,
            exp,
            sig,
            body: None,
            content_type: None,
        })
        .await?;
    Ok(presign_response(r))
}

/// Upload through a locally presigned URL. Not behind API authentication: the
/// signature is the authorisation.
#[utoipa::path(
    put,
    path = "/v1/objects/{connection}/{key}",
    tag = "connectors",
    params(
        ("connection" = String, Path, description = "Object connection name"),
        ("key" = String, Path, description = "Object key (may contain `/`)"),
        PresignQuery,
    ),
    request_body(description = "Object bytes", content((ObjectBytes = "application/octet-stream"))),
    responses(
        (status = 200, description = "Stored"),
        (status = 400, description = "Missing exp or sig", body = ErrorBody),
        (status = 403, description = "Bad or expired signature"),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn put_presigned_object(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path((connection, key)): Path<(String, String)>,
    q: Result<Query<PresignQuery>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let (exp, sig) = presign_parts(q)?;
    let body = body.map_err(|r| body_rejection(r.status(), r.body_text()))?;
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let r = cp
        .presigned_object(domain::PresignedObjectRequest {
            connection,
            key,
            method: domain::PresignMethod::Put,
            exp,
            sig,
            body: Some(body.to_vec()),
            content_type,
        })
        .await?;
    Ok(presign_response(r))
}

/// Register `umr` at `axum_path` (a `{*wildcard}` path) while documenting it under the
/// OpenAPI path its `#[utoipa::path]` names (`{name}` style): utoipa-axum would
/// otherwise mount the documented single-segment path.
fn wildcard_routes<S: Clone + Send + Sync + 'static>(
    mut router: OpenApiRouter<S>,
    (schemas, paths, method_router): utoipa_axum::router::UtoipaMethodRouter<S>,
    axum_path: &str,
) -> OpenApiRouter<S> {
    let doc = router.get_openapi_mut();
    for (path, item) in paths.paths {
        if let Some(it) = doc.paths.paths.get_mut(&path) {
            it.merge_operations(item);
        } else {
            doc.paths.paths.insert(path, item);
        }
    }
    doc.components
        .get_or_insert_with(utoipa::openapi::Components::new)
        .schemas
        .extend(schemas);
    router.route(axum_path, method_router)
}

/// Phase 6 bridge admin routes (06-08), merged into [`api_routes`].
fn bridge_routes(r: OpenApiRouter<CpState>) -> OpenApiRouter<CpState> {
    let r = r
        .routes(routes!(list_secrets))
        .routes(routes!(put_grant, list_grants))
        .routes(routes!(list_connections))
        .routes(routes!(list_egress));
    let r = wildcard_routes(r, routes!(put_secret, delete_secret), "/v1/secrets/{*name}");
    let r = wildcard_routes(r, routes!(delete_grant), "/v1/grants/{*id}");
    let r = wildcard_routes(
        r,
        routes!(put_connection, get_connection, delete_connection),
        "/v1/connections/{*name}",
    );
    wildcard_routes(
        r,
        routes!(put_egress, get_egress, delete_egress),
        "/v1/egress/{*module}",
    )
}

/// `GET|PUT /v1/objects/{connection}/{*key}` (06-08): the local presign route. It is
/// mounted by [`crate::router_with`] **outside** the API middleware stack, so API
/// authentication (when added) never applies to it; the signature is the
/// authorisation. It carries no trace layer either: the query holds the signature.
pub fn object_routes() -> OpenApiRouter<CpState> {
    let mut doc = utoipa::openapi::OpenApi::default();
    doc.info.title = ApiDoc::openapi().info.title;
    wildcard_routes(
        OpenApiRouter::with_openapi(doc),
        routes!(get_presigned_object, put_presigned_object),
        "/v1/objects/{connection}/{*key}",
    )
}

// ---------------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------------

// ---------------------------------------------------------------------------------
// Phase 7 (07-11): queue flow, pause/resume, demand, workers, join tokens, join
// ---------------------------------------------------------------------------------

/// `Cache-Control: no-store` for responses that carry a secret.
fn no_store<T: IntoResponse>(status: StatusCode, body: T) -> Response {
    let mut res = (status, body).into_response();
    res.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    res
}

/// A queue's flow configuration and record version.
#[utoipa::path(
    get,
    path = "/v1/queues/{name}/flow",
    tag = "queues",
    params(("name" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Flow configuration (version 0 = never stored)", body = phase7::QueueFlowInfo),
        (status = 400, description = "Invalid queue name", body = ErrorBody),
    )
)]
async fn get_queue_flow(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<phase7::QueueFlowInfo>, ApiError> {
    Ok(Json(cp.get_queue_flow(name).await?))
}

/// Replace a queue's flow configuration (placement, selector, spill, concurrency,
/// weight, throttles, rate limits, auto-pause, paused). CAS: body `expected_version`
/// or `If-Match` (both given = they must agree).
#[utoipa::path(
    put,
    path = "/v1/queues/{name}/flow",
    tag = "queues",
    params(
        ("name" = String, Path, description = "Queue name"),
        ("If-Match" = Option<String>, Header, description = "Expected record version (alternative to expected_version)"),
    ),
    request_body = phase7::PutQueueFlowBody,
    responses(
        (status = 200, description = "Stored flow configuration", body = phase7::QueueFlowInfo),
        (status = 400, description = "Invalid flow (selector, period_ms, percentages, enum names)", body = ErrorBody),
        (status = 412, description = "Stale expected_version", body = ErrorBody),
        (status = 413, description = "Body exceeds api.max_request_bytes", body = ErrorBody),
    )
)]
async fn put_queue_flow(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Result<Json<phase7::PutQueueFlowBody>, JsonRejection>,
) -> Result<Json<phase7::QueueFlowInfo>, ApiError> {
    let b = json_body(body)?;
    let expected = match (b.expected_version, if_match_version(&headers)?) {
        (Some(a), Some(h)) if a != h => {
            return Err(ApiError::invalid_argument(
                "expected_version and If-Match disagree",
            ));
        }
        (a, h) => a.or(h),
    };
    Ok(Json(cp.put_queue_flow(name, b.flow, expected).await?))
}

/// Pause a queue: no new activations are granted (idempotent).
#[utoipa::path(
    post,
    path = "/v1/queues/{name}/pause",
    tag = "queues",
    params(("name" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Paused (changed = false on repeats)", body = phase7::PauseState),
        (status = 400, description = "Invalid queue name", body = ErrorBody),
    )
)]
async fn pause_queue(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<phase7::PauseState>, ApiError> {
    Ok(Json(cp.pause_queue(name).await?))
}

/// Resume a paused queue (idempotent).
#[utoipa::path(
    post,
    path = "/v1/queues/{name}/resume",
    tag = "queues",
    params(("name" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Resumed (changed = false on repeats)", body = phase7::PauseState),
        (status = 400, description = "Invalid queue name", body = ErrorBody),
    )
)]
async fn resume_queue(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<phase7::PauseState>, ApiError> {
    Ok(Json(cp.resume_queue(name).await?))
}

/// A queue's worker demand (the KEDA scaling signal, D-W12).
#[utoipa::path(
    get,
    path = "/v1/queues/{name}/demand",
    tag = "queues",
    params(("name" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Demand snapshot", body = phase7::DemandSnapshot),
        (status = 400, description = "Invalid queue name", body = ErrorBody),
    )
)]
async fn queue_demand(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<phase7::DemandSnapshot>, ApiError> {
    Ok(Json(cp.queue_demand(name).await?))
}

/// Pause a workflow: its runs get no new activations (idempotent).
#[utoipa::path(
    post,
    path = "/v1/workflows/{name}/pause",
    tag = "workflows",
    params(("name" = String, Path, description = "Workflow name")),
    responses(
        (status = 200, description = "Paused (changed = false on repeats)", body = phase7::PauseState),
        (status = 404, description = "Unknown workflow", body = ErrorBody),
    )
)]
async fn pause_workflow(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<phase7::PauseState>, ApiError> {
    Ok(Json(cp.pause_workflow(name).await?))
}

/// Resume a paused workflow (idempotent).
#[utoipa::path(
    post,
    path = "/v1/workflows/{name}/resume",
    tag = "workflows",
    params(("name" = String, Path, description = "Workflow name")),
    responses(
        (status = 200, description = "Resumed (changed = false on repeats)", body = phase7::PauseState),
        (status = 404, description = "Unknown workflow", body = ErrorBody),
    )
)]
async fn resume_workflow(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(name): Path<String>,
) -> Result<Json<phase7::PauseState>, ApiError> {
    Ok(Json(cp.resume_workflow(name).await?))
}

/// Pause a run: no further activations until resumed (idempotent).
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/pause",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id")),
    request_body(content = phase7::PauseRunRequest, description = "Optional; an empty body means no detail"),
    responses(
        (status = 200, description = "Paused (changed = false on repeats)", body = phase7::RunPauseState),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
        (status = 409, description = "Run already terminal (details.reason = run_terminal)", body = ErrorBody),
    )
)]
async fn pause_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<phase7::RunPauseState>, ApiError> {
    let b: phase7::PauseRunRequest = optional_json_body(body)?;
    Ok(Json(cp.pause_run(run_id, b.detail).await?))
}

/// Resume a paused run (idempotent).
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/resume",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id")),
    responses(
        (status = 200, description = "Resumed (changed = false on repeats)", body = phase7::RunPauseState),
        (status = 404, description = "Unknown run", body = ErrorBody),
        (status = 409, description = "Run already terminal (details.reason = run_terminal)", body = ErrorBody),
    )
)]
async fn resume_run(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(run_id): Path<String>,
) -> Result<Json<phase7::RunPauseState>, ApiError> {
    Ok(Json(cp.resume_run(run_id).await?))
}

/// Connected workers.
#[utoipa::path(
    get,
    path = "/v1/workers",
    tag = "workers",
    responses((status = 200, description = "Workers, sorted by id", body = phase7::WorkerList))
)]
async fn list_workers(
    State(cp): State<Arc<dyn ControlPlane>>,
) -> Result<Json<phase7::WorkerList>, ApiError> {
    Ok(Json(phase7::WorkerList {
        workers: cp.list_workers().await?,
    }))
}

/// Drain a worker: no new work, in-flight activations finish (idempotent).
#[utoipa::path(
    post,
    path = "/v1/workers/{id}/drain",
    tag = "workers",
    params(("id" = String, Path, description = "Worker id")),
    responses(
        (status = 200, description = "Draining", body = phase7::DrainResult),
        (status = 404, description = "Unknown worker", body = ErrorBody),
    )
)]
async fn drain_worker(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(id): Path<String>,
) -> Result<Json<phase7::DrainResult>, ApiError> {
    Ok(Json(cp.drain_worker(id).await?))
}

/// Create a worker join token. The secret (`token`) is in this response only.
#[utoipa::path(
    post,
    path = "/v1/workers/tokens",
    tag = "workers",
    request_body(content = phase7::CreateWorkerTokenRequest, description = "Optional; an empty body means the default lifetime"),
    responses(
        (status = 201, description = "Created (Cache-Control: no-store)", body = phase7::WorkerTokenCreated),
        (status = 400, description = "Invalid ttl_ms", body = ErrorBody),
    )
)]
async fn create_worker_token(
    State(cp): State<Arc<dyn ControlPlane>>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Response, ApiError> {
    let req: phase7::CreateWorkerTokenRequest = optional_json_body(body)?;
    let created = cp.create_worker_token(req).await?;
    Ok(no_store(StatusCode::CREATED, Json(created)))
}

/// Worker join tokens, without their secrets.
#[utoipa::path(
    get,
    path = "/v1/workers/tokens",
    tag = "workers",
    responses((status = 200, description = "Tokens, sorted by id", body = phase7::WorkerTokenList))
)]
async fn list_worker_tokens(
    State(cp): State<Arc<dyn ControlPlane>>,
) -> Result<Json<phase7::WorkerTokenList>, ApiError> {
    Ok(Json(phase7::WorkerTokenList {
        tokens: cp.list_worker_tokens().await?,
    }))
}

/// Revoke a join token: join and renewal are refused and its sessions closed.
#[utoipa::path(
    delete,
    path = "/v1/workers/tokens/{id}",
    tag = "workers",
    params(("id" = String, Path, description = "Token id (wkr_...)")),
    responses(
        (status = 204, description = "Revoked"),
        (status = 404, description = "Unknown token", body = ErrorBody),
    )
)]
async fn revoke_worker_token(
    State(cp): State<Arc<dyn ControlPlane>>,
    Path(id): Path<String>,
) -> Result<Response, ApiError> {
    cp.revoke_worker_token(id).await?;
    Ok(no_content())
}

/// Join: exchange a join token and a CSR for a worker client certificate.
#[utoipa::path(
    post,
    path = "/v1/workers/join",
    tag = "workers",
    request_body = phase7::JoinRequest,
    responses(
        (status = 200, description = "Client certificate, CA and cluster endpoints", body = phase7::JoinResponse),
        (status = 400, description = "Invalid request, or the token is unknown, revoked or expired (details.reason = join_token_invalid)", body = ErrorBody),
    )
)]
async fn join_worker(
    State(cp): State<Arc<dyn ControlPlane>>,
    body: Result<Json<phase7::JoinRequest>, JsonRejection>,
) -> Result<Json<phase7::JoinResponse>, ApiError> {
    let req = json_body(body)?;
    Ok(Json(cp.join_worker(req).await?))
}

/// The Phase 7 routes (07-11), merged into [`api_routes`].
fn phase7_routes(r: OpenApiRouter<CpState>) -> OpenApiRouter<CpState> {
    r.routes(routes!(get_queue_flow, put_queue_flow))
        .routes(routes!(pause_queue))
        .routes(routes!(resume_queue))
        .routes(routes!(queue_demand))
        .routes(routes!(pause_workflow))
        .routes(routes!(resume_workflow))
        .routes(routes!(pause_run))
        .routes(routes!(resume_run))
        .routes(routes!(list_workers))
        .routes(routes!(drain_worker))
        .routes(routes!(create_worker_token, list_worker_tokens))
        .routes(routes!(revoke_worker_token))
        .routes(routes!(join_worker))
}

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
        (name = "tokens", description = "Await-token completion"),
        (name = "queues", description = "Queue run policies"),
        (name = "schedules", description = "Cron schedules (Phase 8)"),
        (name = "webhooks", description = "Inbound webhooks: definitions and POST /hooks/{name} (Phase 8)"),
        (name = "admin", description = "Online backup (Phase 8)"),
        (name = "secrets", description = "Write-only secrets and origin-bound secret grants (Phase 6)"),
        (name = "connectors", description = "Connections, module egress policies and presigned object URLs (Phase 6)"),
        (name = "workers", description = "Worker administration, join tokens and join (Phase 7)"),
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
    let r = OpenApiRouter::with_openapi(doc)
        .routes(routes!(upload_module).layer(DefaultBodyLimit::max(max_module_bytes)))
        .routes(routes!(get_module))
        .routes(routes!(list_workflows))
        .routes(routes!(activate_workflow))
        .routes(routes!(start_run))
        .routes(routes!(list_runs))
        .routes(routes!(get_run))
        .routes(routes!(run_history))
        .routes(routes!(signal_run))
        .routes(routes!(cancel_run))
        .routes(routes!(terminate_run))
        .routes(routes!(retry_run))
        .routes(routes!(run_result))
        .routes(routes!(complete_token))
        .routes(routes!(set_workflow_policy, get_workflow_policy))
        .routes(routes!(set_queue_policy, get_queue_policy))
        .routes(routes!(healthz))
        .routes(routes!(readyz))
        .routes(routes!(openapi_json))
        .routes(routes!(ui_shell));
    phase7_routes(bridge_routes(r))
}

#[cfg(test)]
mod tests {
    use super::decode_base64;

    #[test]
    fn base64_decoding() {
        assert_eq!(decode_base64("").as_deref(), Some(&b""[..]));
        assert_eq!(decode_base64("Zg==").as_deref(), Some(&b"f"[..]));
        assert_eq!(decode_base64("Zg").as_deref(), Some(&b"f"[..]));
        assert_eq!(decode_base64("Zm8=").as_deref(), Some(&b"fo"[..]));
        assert_eq!(decode_base64("Zm9v").as_deref(), Some(&b"foo"[..]));
        assert_eq!(decode_base64("Zm9vYg").as_deref(), Some(&b"foob"[..]));
        assert_eq!(
            decode_base64("+/+/").as_deref(),
            Some(&[0xfb, 0xff, 0xbf][..])
        );
        for bad in [
            "Z", "Zh==", "Zg=", "Zm9v=", "Zm 9v", "Zm9v-_", "Zg===", "=", "Zm8=Zm8=",
        ] {
            assert_eq!(decode_base64(bad), None, "{bad}");
        }
    }
}
