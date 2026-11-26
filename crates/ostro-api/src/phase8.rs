//! The Phase 8 API surface (08-10, P8-D21(b)): schedules, webhooks, hook ingress, run
//! logs and the admin backup, on REST and gRPC, plus inbound W3C `traceparent`
//! validation for start-run.
//!
//! # Shape
//!
//! Each feature is one trait ([`ScheduleApi`], [`WebhookApi`], [`HookIngress`],
//! [`RunLogsApi`], [`AdminApi`]) held in an optional slot of [`ApiExtensions`]. The
//! router always mounts the REST routes (so the OpenAPI document is complete); a route
//! whose slot is `None` answers 404 `not_found`, and its gRPC service is not mounted
//! (`UNIMPLEMENTED`). Existing users of [`crate::router`] therefore see no change apart
//! from those 404s.
//!
//! Unlike [`crate::domain`], the Phase 8 traits take and return the wire DTOs
//! directly (serde + utoipa derives): the shapes mirror the meta records (05-07)
//! one-to-one, and the gRPC adapter converts proto <-> DTO in this file. Validation
//! that needs no server state (name grammar, required fields, CAS tag agreement, body
//! limits, request id length) runs here, identically for both transports; cron
//! expressions and time zones are validated by the [`ScheduleApi`] implementation (the
//! server uses ostro-triggers, 08-14).
//!
//! # Routes
//!
//! | Method | Path | Trait method |
//! |---|---|---|
//! | GET / POST | `/v1/schedules` | `list_schedules` / `create_schedule` (201) |
//! | GET / PUT / DELETE | `/v1/schedules/{name}` | `get_schedule` / `update_schedule` / `delete_schedule` |
//! | POST | `/v1/schedules/{name}/pause`, `/resume`, `/trigger` | `pause_schedule` / `resume_schedule` / `trigger_schedule` |
//! | GET / POST | `/v1/webhooks` | `list_webhooks` / `create_webhook` (201) |
//! | GET / PUT / DELETE | `/v1/webhooks/{name}` | `get_webhook` / `update_webhook` / `delete_webhook` |
//! | POST | `/hooks/{name}` | [`HookIngress::deliver`] (raw body, REST only) |
//! | GET | `/v1/runs/{run_id}/logs` | [`RunLogsApi::run_logs`] |
//! | GET | `/v1/admin/backup` | [`AdminApi::backup`] (streaming tar, REST only) |
//!
//! PUT and DELETE take the meta record version as `version_tag` (body or query) or an
//! `If-Match` header; a stale tag is 409 `conflict` with `details.reason =
//! "version_mismatch"`.
//!
//! gRPC: `ostro.api.v1.Schedules` (List/Get/Create/Update/Delete/Pause/Resume/Trigger),
//! `ostro.api.v1.Webhooks` (List/Get/Create/Update/Delete) and `ostro.api.v1.RunLogs`
//! (GetRunLogs; a sibling service rather than an RPC appended to `Runs`, so the Phase 8
//! surface lives in its own proto files). Hook ingress and the backup have no gRPC
//! counterpart: a webhook signature covers the raw HTTP body and headers, and a tar
//! stream is an HTTP download.
//!
//! # `POST /hooks/{name}`
//!
//! The handler takes the raw [`Bytes`] and the [`HeaderMap`]: nothing parses the body
//! before the [`HookIngress`] implementation has verified the signature (P8-D12). The
//! route has its own body limit, [`ApiExtensions::hook_body_limit`] (`[triggers]
//! webhook_body_limit`, 1 MiB by default; over the limit is 413). It has no
//! authentication layer: the signature is the authentication.

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{run_logs_server, schedules_server, webhooks_server};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::{OpenApiRouter, UtoipaMethodRouterExt};
use utoipa_axum::routes;

use crate::domain::{MAX_REQUEST_ID_LEN, effective_limit};
use crate::error::{ApiError, ErrorBody};
use crate::grpc::to_status;
use crate::rest::{if_match_version, json_body, optional_json_body, query};

// ---------------------------------------------------------------------------------
// Constants and shared validation
// ---------------------------------------------------------------------------------

/// Default body limit of `POST /hooks/{name}` (`[triggers] webhook_body_limit`).
pub const DEFAULT_HOOK_BODY_LIMIT: usize = 1024 * 1024;
/// Longest schedule / webhook name.
pub const MAX_RESOURCE_NAME_LEN: usize = 128;
/// Longest schedule time zone name (meta `SCHEDULE_TIMEZONE_MAX`).
pub const MAX_TIMEZONE_LEN: usize = 64;
/// The W3C trace-context header (REST) and gRPC metadata key.
pub const TRACEPARENT_HEADER: &str = "traceparent";
/// Run tag carried by schedule-started runs (RESERVATIONS §11).
pub const SCHEDULE_RUN_TAG: &str = "ostro.schedule";

/// `details.reason` values of the Phase 8 surface.
pub mod reason {
    /// A start-run `traceparent` (body, header or metadata) is not W3C version 00. 400.
    pub const INVALID_TRACEPARENT: &str = "invalid_traceparent";
    /// A PUT/DELETE `version_tag` does not match the stored record. 409.
    pub const VERSION_MISMATCH: &str = "version_mismatch";
}

/// InvalidArgument, `details.reason = "invalid_traceparent"`.
pub fn invalid_traceparent(why: &str) -> ApiError {
    ApiError::invalid_argument(format!("invalid traceparent: {why}"))
        .with_details(serde_json::json!({ "reason": reason::INVALID_TRACEPARENT }))
}

/// Conflict, `details.reason = "version_mismatch"`, with the stored version.
pub fn version_mismatch(what: &str, name: &str, current: u64) -> ApiError {
    ApiError::conflict(format!(
        "{what} {name} changed: version_tag is now {current}"
    ))
    .with_details(serde_json::json!({
        "reason": reason::VERSION_MISMATCH,
        "current_version_tag": current,
    }))
}

fn is_lower_hex(b: &[u8]) -> bool {
    b.iter()
        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c))
}

fn all_zero(b: &[u8]) -> bool {
    b.iter().all(|c| *c == b'0')
}

/// Validate a W3C `traceparent`: `00-<32 lowercase hex, not all 0>-<16 lowercase hex,
/// not all 0>-<2 lowercase hex>`.
pub fn validate_traceparent(s: &str) -> Result<(), ApiError> {
    let b = s.as_bytes();
    if b.len() != 55 {
        return Err(invalid_traceparent("expected 55 characters"));
    }
    if &b[0..3] != b"00-" || b[35] != b'-' || b[52] != b'-' {
        return Err(invalid_traceparent(
            "expected 00-<trace-id>-<parent-id>-<flags>",
        ));
    }
    let (trace, span, flags) = (&b[3..35], &b[36..52], &b[53..55]);
    if !is_lower_hex(trace) || !is_lower_hex(span) || !is_lower_hex(flags) {
        return Err(invalid_traceparent("ids and flags must be lowercase hex"));
    }
    if all_zero(trace) {
        return Err(invalid_traceparent("trace-id is all zeros"));
    }
    if all_zero(span) {
        return Err(invalid_traceparent("parent-id is all zeros"));
    }
    Ok(())
}

/// The traceparent a start-run stores: the body field when non-empty, else the
/// header / metadata value when non-empty, else none. The chosen value is validated.
pub fn resolve_traceparent(
    body: Option<String>,
    header: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let chosen = match body.filter(|s| !s.is_empty()) {
        Some(b) => Some(b),
        None => header.filter(|h| !h.is_empty()).map(str::to_owned),
    };
    if let Some(t) = &chosen {
        validate_traceparent(t)?;
    }
    Ok(chosen)
}

/// The `traceparent` header as text (`None` when absent; non-ASCII is invalid).
pub fn traceparent_header(headers: &HeaderMap) -> Result<Option<&str>, ApiError> {
    headers
        .get(TRACEPARENT_HEADER)
        .map(|v| {
            v.to_str()
                .map(str::trim)
                .map_err(|_| invalid_traceparent("header is not ASCII"))
        })
        .transpose()
}

/// `[A-Za-z0-9_.-]{1,128}`.
pub fn validate_resource_name(kind: &str, name: &str) -> Result<(), ApiError> {
    if name.is_empty() || name.len() > MAX_RESOURCE_NAME_LEN {
        return Err(ApiError::invalid_argument(format!(
            "{kind} name must be 1..={MAX_RESOURCE_NAME_LEN} characters"
        )));
    }
    if !name
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'.' | b'-'))
    {
        return Err(ApiError::invalid_argument(format!(
            "{kind} name {name:?} may contain only A-Z, a-z, 0-9, '_', '.' and '-'"
        )));
    }
    Ok(())
}

fn validate_request_id(request_id: Option<&str>) -> Result<(), ApiError> {
    match request_id {
        Some(r) if r.is_empty() || r.len() > MAX_REQUEST_ID_LEN => Err(ApiError::invalid_argument(
            format!("request_id must be 1..={MAX_REQUEST_ID_LEN} bytes"),
        )),
        _ => Ok(()),
    }
}

fn validate_pointer(field: &str, p: &str) -> Result<(), ApiError> {
    if p.is_empty() || p.starts_with('/') {
        Ok(())
    } else {
        Err(ApiError::invalid_argument(format!(
            "{field}: {p:?} is not a JSON pointer (must start with '/')"
        )))
    }
}

/// Body and header/query `version_tag` must agree when both are given.
fn merge_tag(body: Option<u64>, other: Option<u64>) -> Result<Option<u64>, ApiError> {
    match (body, other) {
        (Some(b), Some(o)) if b != o => Err(ApiError::invalid_argument(
            "version_tag and If-Match disagree",
        )),
        (b, o) => Ok(b.or(o)),
    }
}

fn absent(what: &str) -> ApiError {
    ApiError::not_found(format!("the {what} API is not enabled on this server"))
}

// ---------------------------------------------------------------------------------
// DTOs
// ---------------------------------------------------------------------------------

/// What a fire does when the previous run of the schedule is still open. Numbering
/// ([`OverlapPolicyDto::as_u8`]) equals the meta `OVERLAP_*` constants (05-07).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OverlapPolicyDto {
    /// Skip the fire.
    #[default]
    Skip,
    /// Keep at most one buffered fire.
    BufferOne,
    /// Start every fire.
    AllowAll,
    /// Cancel the open run, then start.
    CancelOther,
    /// Terminate the open run, then start.
    TerminateOther,
}

impl OverlapPolicyDto {
    /// Every value, in meta numbering order.
    pub const ALL: [OverlapPolicyDto; 5] = [
        OverlapPolicyDto::Skip,
        OverlapPolicyDto::BufferOne,
        OverlapPolicyDto::AllowAll,
        OverlapPolicyDto::CancelOther,
        OverlapPolicyDto::TerminateOther,
    ];

    /// The meta `OVERLAP_*` value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// From the meta `OVERLAP_*` value.
    pub fn from_u8(v: u8) -> Option<Self> {
        Self::ALL.get(usize::from(v)).copied()
    }
}

/// Which missed fires are started after downtime. Numbering
/// ([`CatchupPolicyDto::as_u8`]) equals the meta `CATCHUP_*` constants (05-07).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum CatchupPolicyDto {
    /// Start none.
    #[default]
    Skip,
    /// Start the most recent one.
    One,
    /// Start every missed fire within the window (capped).
    All,
}

impl CatchupPolicyDto {
    /// Every value, in meta numbering order.
    pub const ALL: [CatchupPolicyDto; 3] = [
        CatchupPolicyDto::Skip,
        CatchupPolicyDto::One,
        CatchupPolicyDto::All,
    ];

    /// The meta `CATCHUP_*` value.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// From the meta `CATCHUP_*` value.
    pub fn from_u8(v: u8) -> Option<Self> {
        Self::ALL.get(usize::from(v)).copied()
    }
}

/// A cron schedule (meta `Schedule`). On PUT the name comes from the path (a body name
/// must match it); `version_tag` is the CAS input on PUT and the stored record version
/// on output; `next_fire_at_ms` / `last_fire_at_ms` are output only.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ScheduleDto {
    /// Schedule name (`[A-Za-z0-9_.-]{1,128}`).
    #[serde(default)]
    pub name: String,
    /// Workflow to start.
    #[serde(default)]
    pub workflow: String,
    /// Workflow version (unset = the active version at fire time).
    #[serde(default)]
    pub version: Option<u32>,
    /// Cron expression.
    #[serde(default)]
    pub cron: String,
    /// IANA time zone; empty = UTC.
    #[serde(default)]
    pub timezone: String,
    /// Run input (any JSON value; defaults to null).
    #[serde(default)]
    pub input: serde_json::Value,
    /// Paused schedules do not fire.
    #[serde(default)]
    pub paused: bool,
    /// Overlap policy.
    #[serde(default)]
    pub overlap: OverlapPolicyDto,
    /// Catch-up policy.
    #[serde(default)]
    pub catchup: CatchupPolicyDto,
    /// How far back missed fires are considered, ms (unset or 0 = server default).
    #[serde(default)]
    pub catchup_window_ms: Option<u64>,
    /// No fire before this time (ms since the Unix epoch).
    #[serde(default)]
    pub start_at_ms: Option<u64>,
    /// No fire after this time (ms since the Unix epoch).
    #[serde(default)]
    pub end_at_ms: Option<u64>,
    /// Meta record version (CAS input on PUT).
    #[serde(default)]
    pub version_tag: Option<u64>,
    /// Next fire time, computed by the server (output only).
    #[serde(default)]
    pub next_fire_at_ms: Option<u64>,
    /// Last fire time (output only).
    #[serde(default)]
    pub last_fire_at_ms: Option<u64>,
}

/// Schedule listing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ScheduleListResponse {
    /// Every schedule, by name.
    pub schedules: Vec<ScheduleDto>,
}

/// Manual trigger body (optional).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TriggerScheduleBody {
    /// Idempotency of the manual fire (1..=64 bytes): a repeat returns the same run.
    #[serde(default)]
    pub request_id: Option<String>,
}

/// Manual trigger result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct TriggerScheduleResponseDto {
    /// The started (or deduplicated) run.
    pub run_id: String,
}

/// Delete result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DeletedDto {
    /// Deleted record name.
    pub name: String,
    /// Always true on success.
    pub deleted: bool,
}

/// `version_tag` query parameter of DELETE.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct VersionTagQuery {
    /// Expected record version (alternatively an `If-Match` header).
    pub version_tag: Option<u64>,
}

/// How a signal webhook finds its run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunByDto {
    /// JSON pointer to the run id in the body.
    RunIdPointer(String),
    /// JSON pointer to the start idempotency key in the body.
    IdempotencyPointer(String),
}

fn default_hook_input() -> String {
    "body".to_owned()
}

/// What a verified delivery does.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WebhookActionDto {
    /// Start a run (idempotency key = the delivery key, conflict policy return-existing).
    Start {
        /// Workflow.
        workflow: String,
        /// Version (unset = active).
        #[serde(default)]
        version: Option<u32>,
        /// `"body"` (the whole body) or a JSON pointer into it.
        #[serde(default = "default_hook_input")]
        input: String,
    },
    /// Signal a run (request id = the delivery key).
    Signal {
        /// Workflow of the target run.
        workflow: String,
        /// How the run is found.
        run_by: RunByDto,
        /// Signal name.
        name: String,
    },
    /// Complete an await token.
    CompleteToken {
        /// JSON pointer to the token in the body.
        token_pointer: String,
    },
}

/// Signature verifier kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum VerifierKindDto {
    /// No signature (unauthenticated; the server may refuse it).
    #[default]
    None,
    /// Stripe (`Stripe-Signature`).
    Stripe,
    /// GitHub (`X-Hub-Signature-256`).
    Github,
    /// Slack (`X-Slack-Signature`, v0).
    Slack,
    /// Standard Webhooks.
    Standard,
    /// Generic HMAC.
    Hmac,
}

/// Signature verifier. The secret is never returned: `secret_ref` names a sealed
/// secret.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct VerifierDto {
    /// Kind.
    #[serde(default)]
    pub kind: VerifierKindDto,
    /// Sealed secret name (required unless kind = none).
    #[serde(default)]
    pub secret_ref: String,
    /// Replay window, seconds (unset = the verifier default).
    #[serde(default)]
    pub tolerance_s: Option<u32>,
    /// Generic HMAC: signature header.
    #[serde(default)]
    pub header: Option<String>,
    /// Generic HMAC: signature prefix (e.g. `sha256=`).
    #[serde(default)]
    pub prefix: Option<String>,
    /// Generic HMAC: `sha256` (default) or `legacy_sha1`.
    #[serde(default)]
    pub algorithm: Option<String>,
    /// Generic HMAC: `hex` or `base64`.
    #[serde(default)]
    pub encoding: Option<String>,
    /// Generic HMAC: timestamp header.
    #[serde(default)]
    pub timestamp_header: Option<String>,
    /// Generic HMAC: signed template (`{body}` or `{ts}.{body}`).
    #[serde(default)]
    pub template: Option<String>,
}

/// An inbound webhook (meta `Webhook`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WebhookDto {
    /// Webhook name (`[A-Za-z0-9_.-]{1,128}`), the `{name}` of `POST /hooks/{name}`.
    #[serde(default)]
    pub name: String,
    /// Action.
    pub action: WebhookActionDto,
    /// Verifier.
    #[serde(default)]
    pub verifier: VerifierDto,
    /// `header:<name>`, `json:<pointer>` or `none` (unset = the verifier's default).
    #[serde(default)]
    pub idempotency: Option<String>,
    /// Meta record version (CAS input on PUT).
    #[serde(default)]
    pub version_tag: Option<u64>,
}

/// Webhook listing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct WebhookListResponse {
    /// Every webhook, by name.
    pub webhooks: Vec<WebhookDto>,
}

/// Raw webhook payload (documentation-only schema of the hook body).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
pub struct HookPayload(#[allow(dead_code)] Vec<u8>);

/// A webhook delivery as received: the raw body and headers, nothing parsed.
#[derive(Clone, Debug)]
pub struct HookRequest {
    /// The `{name}` path segment (grammar-checked).
    pub name: String,
    /// Request headers.
    pub headers: HeaderMap,
    /// Raw body (at most [`ApiExtensions::hook_body_limit`] bytes).
    pub body: Bytes,
}

/// The HTTP answer to a delivery (any status and content type, so a Slack
/// `url_verification` challenge can be echoed as plain text).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HookResponse {
    /// HTTP status.
    pub status: u16,
    /// Body.
    pub body: Bytes,
    /// `Content-Type`.
    pub content_type: String,
}

impl HookResponse {
    /// JSON body.
    pub fn json(status: u16, v: &serde_json::Value) -> Self {
        HookResponse {
            status,
            body: Bytes::from(serde_json::to_vec(v).unwrap_or_default()),
            content_type: "application/json".to_owned(),
        }
    }

    /// Plain-text body.
    pub fn text(status: u16, s: impl Into<String>) -> Self {
        HookResponse {
            status,
            body: Bytes::from(s.into()),
            content_type: "text/plain; charset=utf-8".to_owned(),
        }
    }
}

impl IntoResponse for HookResponse {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let ct = HeaderValue::from_str(&self.content_type)
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream"));
        (status, [(header::CONTENT_TYPE, ct)], self.body).into_response()
    }
}

/// Guest log level.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum LogLevelDto {
    /// trace
    Trace,
    /// debug
    Debug,
    /// info
    Info,
    /// warn
    Warn,
    /// error
    Error,
}

/// One guest log line.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunLogLine {
    /// Emission time, ms since the Unix epoch.
    pub ts_ms: u64,
    /// Level.
    pub level: LogLevelDto,
    /// Message.
    pub message: String,
    /// Structured fields (a JSON object).
    pub fields: serde_json::Value,
    /// Attempt that emitted the line.
    pub attempt: u32,
}

/// Run-logs query (transport-neutral).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunLogsRequest {
    /// Run id.
    pub run_id: String,
    /// Most recent lines to return (already clamped: default 100, max 1000).
    pub limit: u32,
    /// Minimum level.
    pub level: Option<LogLevelDto>,
}

/// A run's recent guest log lines (non-durable ring buffer, P8-D16).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RunLogsResponse {
    /// Run id.
    pub run_id: String,
    /// Lines, oldest first.
    pub lines: Vec<RunLogLine>,
    /// Lines dropped for this run by rate limits, caps or ring eviction (best effort).
    pub dropped: u64,
}

/// `GET /v1/runs/{run_id}/logs` query.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct RunLogsQuery {
    /// Most recent lines to return (default 100, max 1000).
    pub limit: Option<u32>,
    /// Minimum level.
    #[param(inline)]
    pub level: Option<LogLevelDto>,
}

/// Tar archive (documentation-only schema of the backup download).
#[derive(ToSchema)]
#[schema(value_type = String, format = Binary)]
pub struct BackupTar(#[allow(dead_code)] Vec<u8>);

/// A backup download: a streaming `application/x-tar` body (P8-D7: the remote layout
/// packed as tar). The server builds the body from a stream (e.g.
/// `axum::body::Body::from_stream`).
pub struct BackupStream {
    /// The tar bytes.
    pub body: axum::body::Body,
    /// `Content-Disposition` file name.
    pub filename: String,
}

impl BackupStream {
    /// A backup named `ostro-backup.tar`.
    pub fn new(body: impl Into<axum::body::Body>) -> Self {
        BackupStream {
            body: body.into(),
            filename: "ostro-backup.tar".to_owned(),
        }
    }
}

impl std::fmt::Debug for BackupStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupStream")
            .field("filename", &self.filename)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------------

/// Cron schedules (implemented by the server in 08-14).
#[async_trait::async_trait]
pub trait ScheduleApi: Send + Sync + 'static {
    /// Every schedule, by name.
    async fn list_schedules(&self) -> Result<Vec<ScheduleDto>, ApiError>;
    /// One schedule.
    async fn get_schedule(&self, name: String) -> Result<ScheduleDto, ApiError>;
    /// Create (Conflict when the name exists). Validates cron and time zone.
    async fn create_schedule(&self, s: ScheduleDto) -> Result<ScheduleDto, ApiError>;
    /// Replace; `s.version_tag`, when set, must equal the stored version
    /// ([`version_mismatch`] otherwise).
    async fn update_schedule(&self, s: ScheduleDto) -> Result<ScheduleDto, ApiError>;
    /// Delete, with an optional CAS tag.
    async fn delete_schedule(&self, name: String, version_tag: Option<u64>)
    -> Result<(), ApiError>;
    /// Pause (idempotent).
    async fn pause_schedule(&self, name: String) -> Result<ScheduleDto, ApiError>;
    /// Resume (idempotent).
    async fn resume_schedule(&self, name: String) -> Result<ScheduleDto, ApiError>;
    /// Fire now (`ScheduleFire{kind: Manual}`); returns the run id.
    async fn trigger_schedule(
        &self,
        name: String,
        request_id: Option<String>,
    ) -> Result<String, ApiError>;
}

/// Webhook definitions (implemented by the server in 08-15).
#[async_trait::async_trait]
pub trait WebhookApi: Send + Sync + 'static {
    /// Every webhook, by name.
    async fn list_webhooks(&self) -> Result<Vec<WebhookDto>, ApiError>;
    /// One webhook.
    async fn get_webhook(&self, name: String) -> Result<WebhookDto, ApiError>;
    /// Create (Conflict when the name exists).
    async fn create_webhook(&self, w: WebhookDto) -> Result<WebhookDto, ApiError>;
    /// Replace; `w.version_tag`, when set, must equal the stored version.
    async fn update_webhook(&self, w: WebhookDto) -> Result<WebhookDto, ApiError>;
    /// Delete, with an optional CAS tag.
    async fn delete_webhook(&self, name: String, version_tag: Option<u64>) -> Result<(), ApiError>;
}

/// `POST /hooks/{name}` (implemented by the server in 08-15): verify the raw body,
/// then act; answer 2xx only after the command is applied.
#[async_trait::async_trait]
pub trait HookIngress: Send + Sync + 'static {
    /// Handle one delivery.
    async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError>;
}

/// Per-run guest logs (implemented by the server in 08-17).
#[async_trait::async_trait]
pub trait RunLogsApi: Send + Sync + 'static {
    /// The most recent `req.limit` lines at or above `req.level`, oldest first.
    async fn run_logs(&self, req: RunLogsRequest) -> Result<RunLogsResponse, ApiError>;
}

/// Online backup (implemented by the server in 08-18).
#[async_trait::async_trait]
pub trait AdminApi: Send + Sync + 'static {
    /// A tar of the bottomless layout (P8-D7).
    async fn backup(&self) -> Result<BackupStream, ApiError>;
}

/// The Phase 8 trait objects served next to the [`crate::ControlPlane`]. Every slot is
/// optional; see the module docs for absent slots.
#[derive(Clone)]
pub struct ApiExtensions {
    /// Schedules (REST + gRPC `Schedules`).
    pub schedules: Option<Arc<dyn ScheduleApi>>,
    /// Webhook definitions (REST + gRPC `Webhooks`).
    pub webhooks: Option<Arc<dyn WebhookApi>>,
    /// `POST /hooks/{name}`.
    pub hooks: Option<Arc<dyn HookIngress>>,
    /// Run logs (REST + gRPC `RunLogs`).
    pub run_logs: Option<Arc<dyn RunLogsApi>>,
    /// `GET /v1/admin/backup`.
    pub admin: Option<Arc<dyn AdminApi>>,
    /// Body limit of `POST /hooks/{name}` (`[triggers] webhook_body_limit`).
    pub hook_body_limit: usize,
    /// Phase 11 run operations (11-04 slot; routes from 11-09).
    pub ops: Option<Arc<dyn crate::phase11::OpsApi>>,
    /// Phase 11 inventory views (11-04 slot; routes from 11-09).
    pub inventory: Option<Arc<dyn crate::phase11::InventoryApi>>,
    /// Phase 13 cluster membership / upgrades (13-01 slot; routes from 13-03).
    pub cluster: Option<Arc<dyn crate::phase13::ClusterApi>>,
    /// Phase 13 module retire / GC / migrate-runs (13-01 slot; routes from 13-03).
    pub module_lifecycle: Option<Arc<dyn crate::phase13::ModuleLifecycleApi>>,
    /// Phase 14 tenants (14-01 slot; routes from 14-02, implementation 14-17).
    pub tenants: Option<Arc<dyn crate::phase14::TenantsApi>>,
    /// Phase 14 API tokens (14-01 slot; routes from 14-02, implementation 14-17).
    pub api_tokens: Option<Arc<dyn crate::phase14::ApiTokensApi>>,
    /// Phase 14 audit query (14-01 slot; routes from 14-02, implementation 14-18).
    pub audit: Option<Arc<dyn crate::phase14::AuditApi>>,
    /// Phase 14 module security (14-01 slot; routes from 14-02, implementation 14-19).
    pub security: Option<Arc<dyn crate::phase14::SecurityApi>>,
}

impl Default for ApiExtensions {
    /// Every slot absent; 1 MiB hook bodies.
    fn default() -> Self {
        ApiExtensions {
            schedules: None,
            webhooks: None,
            hooks: None,
            run_logs: None,
            admin: None,
            hook_body_limit: DEFAULT_HOOK_BODY_LIMIT,
            ops: None,
            inventory: None,
            cluster: None,
            module_lifecycle: None,
            tenants: None,
            api_tokens: None,
            audit: None,
            security: None,
        }
    }
}

impl std::fmt::Debug for ApiExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApiExtensions")
            .field("schedules", &self.schedules.is_some())
            .field("webhooks", &self.webhooks.is_some())
            .field("hooks", &self.hooks.is_some())
            .field("run_logs", &self.run_logs.is_some())
            .field("admin", &self.admin.is_some())
            .field("hook_body_limit", &self.hook_body_limit)
            .field("ops", &self.ops.is_some())
            .field("inventory", &self.inventory.is_some())
            .field("cluster", &self.cluster.is_some())
            .field("module_lifecycle", &self.module_lifecycle.is_some())
            .field("tenants", &self.tenants.is_some())
            .field("api_tokens", &self.api_tokens.is_some())
            .field("audit", &self.audit.is_some())
            .field("security", &self.security.is_some())
            .finish()
    }
}

impl ApiExtensions {
    fn schedules(&self) -> Result<&dyn ScheduleApi, ApiError> {
        self.schedules.as_deref().ok_or_else(|| absent("schedules"))
    }

    fn webhooks(&self) -> Result<&dyn WebhookApi, ApiError> {
        self.webhooks.as_deref().ok_or_else(|| absent("webhooks"))
    }

    fn hooks(&self) -> Result<&dyn HookIngress, ApiError> {
        self.hooks
            .as_deref()
            .ok_or_else(|| absent("webhook ingress"))
    }

    fn run_logs(&self) -> Result<&dyn RunLogsApi, ApiError> {
        self.run_logs.as_deref().ok_or_else(|| absent("run logs"))
    }

    fn admin(&self) -> Result<&dyn AdminApi, ApiError> {
        self.admin.as_deref().ok_or_else(|| absent("admin"))
    }
}

// ---------------------------------------------------------------------------------
// Transport-neutral operations (shared validation for REST and gRPC)
// ---------------------------------------------------------------------------------

fn validate_schedule(s: &ScheduleDto) -> Result<(), ApiError> {
    validate_resource_name("schedule", &s.name)?;
    if s.workflow.is_empty() {
        return Err(ApiError::invalid_argument("workflow is required"));
    }
    if s.cron.trim().is_empty() {
        return Err(ApiError::invalid_argument("cron is required"));
    }
    if s.timezone.len() > MAX_TIMEZONE_LEN {
        return Err(ApiError::invalid_argument(format!(
            "timezone is longer than {MAX_TIMEZONE_LEN} bytes"
        )));
    }
    if let (Some(a), Some(b)) = (s.start_at_ms, s.end_at_ms)
        && a > b
    {
        return Err(ApiError::invalid_argument("start_at_ms is after end_at_ms"));
    }
    Ok(())
}

/// Output-only fields are cleared on input.
fn schedule_input(mut s: ScheduleDto) -> ScheduleDto {
    s.next_fire_at_ms = None;
    s.last_fire_at_ms = None;
    s
}

async fn op_create_schedule(
    api: &dyn ScheduleApi,
    s: ScheduleDto,
) -> Result<ScheduleDto, ApiError> {
    let mut s = schedule_input(s);
    s.version_tag = None;
    validate_schedule(&s)?;
    api.create_schedule(s).await
}

async fn op_update_schedule(
    api: &dyn ScheduleApi,
    name: String,
    s: ScheduleDto,
    other_tag: Option<u64>,
) -> Result<ScheduleDto, ApiError> {
    validate_resource_name("schedule", &name)?;
    let mut s = schedule_input(s);
    if !s.name.is_empty() && s.name != name {
        return Err(ApiError::invalid_argument(format!(
            "body name {:?} does not match the path name {name:?}",
            s.name
        )));
    }
    s.name = name;
    s.version_tag = merge_tag(s.version_tag, other_tag)?;
    validate_schedule(&s)?;
    api.update_schedule(s).await
}

async fn op_trigger_schedule(
    api: &dyn ScheduleApi,
    name: String,
    request_id: Option<String>,
) -> Result<TriggerScheduleResponseDto, ApiError> {
    validate_resource_name("schedule", &name)?;
    validate_request_id(request_id.as_deref())?;
    let run_id = api.trigger_schedule(name, request_id).await?;
    Ok(TriggerScheduleResponseDto { run_id })
}

fn validate_webhook(w: &WebhookDto) -> Result<(), ApiError> {
    validate_resource_name("webhook", &w.name)?;
    match &w.action {
        WebhookActionDto::Start {
            workflow, input, ..
        } => {
            if workflow.is_empty() {
                return Err(ApiError::invalid_argument("action.workflow is required"));
            }
            if input != "body" {
                validate_pointer("action.input", input)?;
            }
        }
        WebhookActionDto::Signal {
            workflow,
            run_by,
            name,
        } => {
            if workflow.is_empty() {
                return Err(ApiError::invalid_argument("action.workflow is required"));
            }
            if name.is_empty() {
                return Err(ApiError::invalid_argument("action.name is required"));
            }
            let (field, p) = match run_by {
                RunByDto::RunIdPointer(p) => ("action.run_by.run_id_pointer", p),
                RunByDto::IdempotencyPointer(p) => ("action.run_by.idempotency_pointer", p),
            };
            if p.is_empty() {
                return Err(ApiError::invalid_argument(format!("{field} is required")));
            }
            validate_pointer(field, p)?;
        }
        WebhookActionDto::CompleteToken { token_pointer } => {
            if token_pointer.is_empty() {
                return Err(ApiError::invalid_argument(
                    "action.token_pointer is required",
                ));
            }
            validate_pointer("action.token_pointer", token_pointer)?;
        }
    }
    if w.verifier.kind != VerifierKindDto::None && w.verifier.secret_ref.is_empty() {
        return Err(ApiError::invalid_argument(
            "verifier.secret_ref is required unless verifier.kind is none",
        ));
    }
    if let Some(i) = &w.idempotency {
        let ok = i == "none"
            || i.strip_prefix("header:").is_some_and(|h| !h.is_empty())
            || i.strip_prefix("json:").is_some_and(|p| p.starts_with('/'));
        if !ok {
            return Err(ApiError::invalid_argument(format!(
                "idempotency {i:?}: expected header:<name>, json:<pointer> or none"
            )));
        }
    }
    Ok(())
}

async fn op_create_webhook(
    api: &dyn WebhookApi,
    mut w: WebhookDto,
) -> Result<WebhookDto, ApiError> {
    w.version_tag = None;
    validate_webhook(&w)?;
    api.create_webhook(w).await
}

async fn op_update_webhook(
    api: &dyn WebhookApi,
    name: String,
    mut w: WebhookDto,
    other_tag: Option<u64>,
) -> Result<WebhookDto, ApiError> {
    validate_resource_name("webhook", &name)?;
    if !w.name.is_empty() && w.name != name {
        return Err(ApiError::invalid_argument(format!(
            "body name {:?} does not match the path name {name:?}",
            w.name
        )));
    }
    w.name = name;
    w.version_tag = merge_tag(w.version_tag, other_tag)?;
    validate_webhook(&w)?;
    api.update_webhook(w).await
}

async fn op_run_logs(
    api: &dyn RunLogsApi,
    run_id: String,
    limit: u32,
    level: Option<LogLevelDto>,
) -> Result<RunLogsResponse, ApiError> {
    if run_id.is_empty() {
        return Err(ApiError::invalid_argument("run_id is required"));
    }
    api.run_logs(RunLogsRequest {
        run_id,
        limit: effective_limit(limit),
        level,
    })
    .await
}

// ---------------------------------------------------------------------------------
// REST handlers
// ---------------------------------------------------------------------------------

/// Router state of the Phase 8 routes.
pub type ExtState = Arc<ApiExtensions>;

/// List schedules.
#[utoipa::path(
    get,
    path = "/v1/schedules",
    tag = "schedules",
    responses(
        (status = 200, description = "Every schedule", body = ScheduleListResponse),
        (status = 404, description = "Schedules API not enabled", body = ErrorBody),
    )
)]
async fn list_schedules(
    State(ext): State<ExtState>,
) -> Result<Json<ScheduleListResponse>, ApiError> {
    let schedules = ext.schedules()?.list_schedules().await?;
    Ok(Json(ScheduleListResponse { schedules }))
}

/// Create a schedule.
#[utoipa::path(
    post,
    path = "/v1/schedules",
    tag = "schedules",
    request_body = ScheduleDto,
    responses(
        (status = 201, description = "Schedule created", body = ScheduleDto),
        (status = 400, description = "Invalid schedule (name, cron, time zone)", body = ErrorBody),
        (status = 409, description = "A schedule with this name exists", body = ErrorBody),
    )
)]
async fn create_schedule(
    State(ext): State<ExtState>,
    body: Result<Json<ScheduleDto>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = ext.schedules()?;
    let s = op_create_schedule(api, json_body(body)?).await?;
    Ok((StatusCode::CREATED, Json(s)).into_response())
}

/// Get a schedule.
#[utoipa::path(
    get,
    path = "/v1/schedules/{name}",
    tag = "schedules",
    params(("name" = String, Path, description = "Schedule name")),
    responses(
        (status = 200, description = "The schedule", body = ScheduleDto),
        (status = 404, description = "Unknown schedule", body = ErrorBody),
    )
)]
async fn get_schedule(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
) -> Result<Json<ScheduleDto>, ApiError> {
    let api = ext.schedules()?;
    validate_resource_name("schedule", &name)?;
    Ok(Json(api.get_schedule(name).await?))
}

/// Replace a schedule (CAS on `version_tag` / `If-Match`).
#[utoipa::path(
    put,
    path = "/v1/schedules/{name}",
    tag = "schedules",
    params(
        ("name" = String, Path, description = "Schedule name"),
        ("If-Match" = Option<String>, Header, description = "Expected version_tag"),
    ),
    request_body = ScheduleDto,
    responses(
        (status = 200, description = "The updated schedule", body = ScheduleDto),
        (status = 400, description = "Invalid schedule", body = ErrorBody),
        (status = 404, description = "Unknown schedule", body = ErrorBody),
        (status = 409, description = "Stale version_tag (details.reason = version_mismatch)", body = ErrorBody),
    )
)]
async fn update_schedule(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Result<Json<ScheduleDto>, JsonRejection>,
) -> Result<Json<ScheduleDto>, ApiError> {
    let api = ext.schedules()?;
    let s = json_body(body)?;
    let tag = if_match_version(&headers)?;
    Ok(Json(op_update_schedule(api, name, s, tag).await?))
}

/// Delete a schedule (optional CAS on `version_tag` / `If-Match`).
#[utoipa::path(
    delete,
    path = "/v1/schedules/{name}",
    tag = "schedules",
    params(
        ("name" = String, Path, description = "Schedule name"),
        ("If-Match" = Option<String>, Header, description = "Expected version_tag"),
        VersionTagQuery,
    ),
    responses(
        (status = 200, description = "Deleted", body = DeletedDto),
        (status = 404, description = "Unknown schedule", body = ErrorBody),
        (status = 409, description = "Stale version_tag (details.reason = version_mismatch)", body = ErrorBody),
    )
)]
async fn delete_schedule(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    q: Result<Query<VersionTagQuery>, QueryRejection>,
) -> Result<Json<DeletedDto>, ApiError> {
    let api = ext.schedules()?;
    validate_resource_name("schedule", &name)?;
    let tag = merge_tag(query(q)?.version_tag, if_match_version(&headers)?)?;
    api.delete_schedule(name.clone(), tag).await?;
    Ok(Json(DeletedDto {
        name,
        deleted: true,
    }))
}

/// Pause a schedule (idempotent).
#[utoipa::path(
    post,
    path = "/v1/schedules/{name}/pause",
    tag = "schedules",
    params(("name" = String, Path, description = "Schedule name")),
    responses(
        (status = 200, description = "The paused schedule", body = ScheduleDto),
        (status = 404, description = "Unknown schedule", body = ErrorBody),
    )
)]
async fn pause_schedule(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
) -> Result<Json<ScheduleDto>, ApiError> {
    let api = ext.schedules()?;
    validate_resource_name("schedule", &name)?;
    Ok(Json(api.pause_schedule(name).await?))
}

/// Resume a schedule (idempotent).
#[utoipa::path(
    post,
    path = "/v1/schedules/{name}/resume",
    tag = "schedules",
    params(("name" = String, Path, description = "Schedule name")),
    responses(
        (status = 200, description = "The resumed schedule", body = ScheduleDto),
        (status = 404, description = "Unknown schedule", body = ErrorBody),
    )
)]
async fn resume_schedule(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
) -> Result<Json<ScheduleDto>, ApiError> {
    let api = ext.schedules()?;
    validate_resource_name("schedule", &name)?;
    Ok(Json(api.resume_schedule(name).await?))
}

/// Fire a schedule now (does not move its watermark).
#[utoipa::path(
    post,
    path = "/v1/schedules/{name}/trigger",
    tag = "schedules",
    params(("name" = String, Path, description = "Schedule name")),
    request_body(content = TriggerScheduleBody, description = "Optional; an empty body means no request_id"),
    responses(
        (status = 200, description = "The started (or deduplicated) run", body = TriggerScheduleResponseDto),
        (status = 400, description = "Invalid request_id", body = ErrorBody),
        (status = 404, description = "Unknown schedule or workflow", body = ErrorBody),
    )
)]
async fn trigger_schedule(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<TriggerScheduleResponseDto>, ApiError> {
    let api = ext.schedules()?;
    let b: TriggerScheduleBody = optional_json_body(body)?;
    Ok(Json(op_trigger_schedule(api, name, b.request_id).await?))
}

/// List webhooks.
#[utoipa::path(
    get,
    path = "/v1/webhooks",
    tag = "webhooks",
    responses(
        (status = 200, description = "Every webhook", body = WebhookListResponse),
        (status = 404, description = "Webhooks API not enabled", body = ErrorBody),
    )
)]
async fn list_webhooks(State(ext): State<ExtState>) -> Result<Json<WebhookListResponse>, ApiError> {
    let webhooks = ext.webhooks()?.list_webhooks().await?;
    Ok(Json(WebhookListResponse { webhooks }))
}

/// Create a webhook.
#[utoipa::path(
    post,
    path = "/v1/webhooks",
    tag = "webhooks",
    request_body = WebhookDto,
    responses(
        (status = 201, description = "Webhook created", body = WebhookDto),
        (status = 400, description = "Invalid webhook", body = ErrorBody),
        (status = 409, description = "A webhook with this name exists", body = ErrorBody),
    )
)]
async fn create_webhook(
    State(ext): State<ExtState>,
    body: Result<Json<WebhookDto>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = ext.webhooks()?;
    let w = op_create_webhook(api, json_body(body)?).await?;
    Ok((StatusCode::CREATED, Json(w)).into_response())
}

/// Get a webhook.
#[utoipa::path(
    get,
    path = "/v1/webhooks/{name}",
    tag = "webhooks",
    params(("name" = String, Path, description = "Webhook name")),
    responses(
        (status = 200, description = "The webhook", body = WebhookDto),
        (status = 404, description = "Unknown webhook", body = ErrorBody),
    )
)]
async fn get_webhook(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
) -> Result<Json<WebhookDto>, ApiError> {
    let api = ext.webhooks()?;
    validate_resource_name("webhook", &name)?;
    Ok(Json(api.get_webhook(name).await?))
}

/// Replace a webhook (CAS on `version_tag` / `If-Match`).
#[utoipa::path(
    put,
    path = "/v1/webhooks/{name}",
    tag = "webhooks",
    params(
        ("name" = String, Path, description = "Webhook name"),
        ("If-Match" = Option<String>, Header, description = "Expected version_tag"),
    ),
    request_body = WebhookDto,
    responses(
        (status = 200, description = "The updated webhook", body = WebhookDto),
        (status = 400, description = "Invalid webhook", body = ErrorBody),
        (status = 404, description = "Unknown webhook", body = ErrorBody),
        (status = 409, description = "Stale version_tag (details.reason = version_mismatch)", body = ErrorBody),
    )
)]
async fn update_webhook(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Result<Json<WebhookDto>, JsonRejection>,
) -> Result<Json<WebhookDto>, ApiError> {
    let api = ext.webhooks()?;
    let w = json_body(body)?;
    let tag = if_match_version(&headers)?;
    Ok(Json(op_update_webhook(api, name, w, tag).await?))
}

/// Delete a webhook (optional CAS on `version_tag` / `If-Match`).
#[utoipa::path(
    delete,
    path = "/v1/webhooks/{name}",
    tag = "webhooks",
    params(
        ("name" = String, Path, description = "Webhook name"),
        ("If-Match" = Option<String>, Header, description = "Expected version_tag"),
        VersionTagQuery,
    ),
    responses(
        (status = 200, description = "Deleted", body = DeletedDto),
        (status = 404, description = "Unknown webhook", body = ErrorBody),
        (status = 409, description = "Stale version_tag (details.reason = version_mismatch)", body = ErrorBody),
    )
)]
async fn delete_webhook(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    q: Result<Query<VersionTagQuery>, QueryRejection>,
) -> Result<Json<DeletedDto>, ApiError> {
    let api = ext.webhooks()?;
    validate_resource_name("webhook", &name)?;
    let tag = merge_tag(query(q)?.version_tag, if_match_version(&headers)?)?;
    api.delete_webhook(name.clone(), tag).await?;
    Ok(Json(DeletedDto {
        name,
        deleted: true,
    }))
}

/// Deliver a webhook. The raw body is verified (signature) before anything parses it;
/// there is no other authentication. Body limit: `[triggers] webhook_body_limit`.
#[utoipa::path(
    post,
    path = "/hooks/{name}",
    tag = "webhooks",
    params(("name" = String, Path, description = "Webhook name")),
    request_body(
        description = "Raw payload, verified byte-for-byte",
        content((HookPayload = "application/json"), (HookPayload = "application/octet-stream")),
    ),
    responses(
        (status = 200, description = "Delivered (the body depends on the action; a Slack url_verification challenge is echoed as text/plain)"),
        (status = 202, description = "Accepted"),
        (status = 401, description = "Missing or invalid signature"),
        (status = 404, description = "Unknown webhook", body = ErrorBody),
        (status = 413, description = "Body exceeds triggers.webhook_body_limit", body = ErrorBody),
    )
)]
async fn deliver_hook(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Result<HookResponse, ApiError> {
    let api = ext.hooks()?;
    validate_resource_name("webhook", &name)?;
    let body = body.map_err(|r| {
        if r.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large(format!(
                "webhook body exceeds triggers.webhook_body_limit ({} bytes)",
                ext.hook_body_limit
            ))
        } else {
            ApiError::invalid_argument(r.body_text())
        }
    })?;
    api.deliver(HookRequest {
        name,
        headers,
        body,
    })
    .await
}

/// A run's recent guest log lines (non-durable, P8-D16).
#[utoipa::path(
    get,
    path = "/v1/runs/{run_id}/logs",
    tag = "runs",
    params(("run_id" = String, Path, description = "Run id"), RunLogsQuery),
    responses(
        (status = 200, description = "Recent lines, oldest first", body = RunLogsResponse),
        (status = 400, description = "Invalid query", body = ErrorBody),
        (status = 404, description = "Unknown run", body = ErrorBody),
    )
)]
async fn run_logs(
    State(ext): State<ExtState>,
    Path(run_id): Path<String>,
    q: Result<Query<RunLogsQuery>, QueryRejection>,
) -> Result<Json<RunLogsResponse>, ApiError> {
    let api = ext.run_logs()?;
    let q = query(q)?;
    Ok(Json(
        op_run_logs(api, run_id, q.limit.unwrap_or(0), q.level).await?,
    ))
}

/// Download an online backup (tar of the bottomless layout, P8-D7).
#[utoipa::path(
    get,
    path = "/v1/admin/backup",
    tag = "admin",
    responses(
        (status = 200, description = "Tar stream", body = BackupTar, content_type = "application/x-tar"),
        (status = 404, description = "Admin API not enabled", body = ErrorBody),
        (status = 503, description = "Backup not available", body = ErrorBody),
    )
)]
async fn backup(State(ext): State<ExtState>) -> Result<Response, ApiError> {
    let b = ext.admin()?.backup().await?;
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", b.filename))
        .unwrap_or_else(|_| HeaderValue::from_static("attachment"));
    Ok((
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/x-tar"),
            ),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        b.body,
    )
        .into_response())
}

/// The Phase 8 REST routes with their OpenAPI description, without state. The base
/// document is empty (no tags or info), so merging it into [`crate::rest::ApiDoc`]
/// only adds paths and schemas.
pub fn api_routes(hook_body_limit: usize) -> OpenApiRouter<ExtState> {
    OpenApiRouter::new()
        .routes(routes!(list_schedules, create_schedule))
        .routes(routes!(get_schedule, update_schedule, delete_schedule))
        .routes(routes!(pause_schedule))
        .routes(routes!(resume_schedule))
        .routes(routes!(trigger_schedule))
        .routes(routes!(list_webhooks, create_webhook))
        .routes(routes!(get_webhook, update_webhook, delete_webhook))
        .routes(routes!(deliver_hook).layer(DefaultBodyLimit::max(hook_body_limit)))
        .routes(routes!(run_logs))
        .routes(routes!(backup))
}

/// The Phase 8 REST router over `ext` (merged by [`crate::router_with`]).
pub fn router(ext: ApiExtensions) -> axum::Router {
    let (r, _doc) = api_routes(ext.hook_body_limit).split_for_parts();
    r.with_state(Arc::new(ext))
}

/// Mount the gRPC services whose slots are present.
pub(crate) fn add_grpc_services(
    mut routes: tonic::service::Routes,
    ext: &ApiExtensions,
    max_request_bytes: usize,
) -> tonic::service::Routes {
    if let Some(s) = &ext.schedules {
        routes = routes.add_service(
            schedules_server::SchedulesServer::new(GrpcSchedules(Arc::clone(s)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(w) = &ext.webhooks {
        routes = routes.add_service(
            webhooks_server::WebhooksServer::new(GrpcWebhooks(Arc::clone(w)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(l) = &ext.run_logs {
        routes = routes.add_service(
            run_logs_server::RunLogsServer::new(GrpcRunLogs(Arc::clone(l)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    routes
}

// ---------------------------------------------------------------------------------
// gRPC conversions
// ---------------------------------------------------------------------------------

fn json_from_proto(field: &str, bytes: &[u8]) -> Result<serde_json::Value, ApiError> {
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(bytes).map_err(|e| ApiError::invalid_argument(format!("{field}: {e}")))
}

fn json_to_proto(v: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(v).unwrap_or_default()
}

fn overlap_from_proto(v: i32) -> Result<OverlapPolicyDto, ApiError> {
    u8::try_from(v)
        .ok()
        .and_then(OverlapPolicyDto::from_u8)
        .ok_or_else(|| ApiError::invalid_argument(format!("overlap: invalid value {v}")))
}

fn catchup_from_proto(v: i32) -> Result<CatchupPolicyDto, ApiError> {
    u8::try_from(v)
        .ok()
        .and_then(CatchupPolicyDto::from_u8)
        .ok_or_else(|| ApiError::invalid_argument(format!("catchup: invalid value {v}")))
}

impl TryFrom<pb::Schedule> for ScheduleDto {
    type Error = ApiError;

    fn try_from(s: pb::Schedule) -> Result<Self, ApiError> {
        Ok(ScheduleDto {
            input: json_from_proto("input_json", &s.input_json)?,
            overlap: overlap_from_proto(s.overlap)?,
            catchup: catchup_from_proto(s.catchup)?,
            name: s.name,
            workflow: s.workflow,
            version: s.version,
            cron: s.cron,
            timezone: s.timezone,
            paused: s.paused,
            catchup_window_ms: s.catchup_window_ms,
            start_at_ms: s.start_at_ms,
            end_at_ms: s.end_at_ms,
            version_tag: s.version_tag,
            next_fire_at_ms: s.next_fire_at_ms,
            last_fire_at_ms: s.last_fire_at_ms,
        })
    }
}

impl From<ScheduleDto> for pb::Schedule {
    fn from(s: ScheduleDto) -> Self {
        pb::Schedule {
            input_json: json_to_proto(&s.input),
            overlap: i32::from(s.overlap.as_u8()),
            catchup: i32::from(s.catchup.as_u8()),
            name: s.name,
            workflow: s.workflow,
            version: s.version,
            cron: s.cron,
            timezone: s.timezone,
            paused: s.paused,
            catchup_window_ms: s.catchup_window_ms,
            start_at_ms: s.start_at_ms,
            end_at_ms: s.end_at_ms,
            version_tag: s.version_tag,
            next_fire_at_ms: s.next_fire_at_ms,
            last_fire_at_ms: s.last_fire_at_ms,
        }
    }
}

fn verifier_kind_from_proto(v: i32) -> Result<VerifierKindDto, ApiError> {
    use pb::WebhookVerifierKind as K;
    match K::try_from(v) {
        Ok(K::None) => Ok(VerifierKindDto::None),
        Ok(K::Stripe) => Ok(VerifierKindDto::Stripe),
        Ok(K::Github) => Ok(VerifierKindDto::Github),
        Ok(K::Slack) => Ok(VerifierKindDto::Slack),
        Ok(K::Standard) => Ok(VerifierKindDto::Standard),
        Ok(K::Hmac) => Ok(VerifierKindDto::Hmac),
        Err(_) => Err(ApiError::invalid_argument(format!(
            "verifier.kind: invalid value {v}"
        ))),
    }
}

fn verifier_kind_to_proto(k: VerifierKindDto) -> pb::WebhookVerifierKind {
    use pb::WebhookVerifierKind as K;
    match k {
        VerifierKindDto::None => K::None,
        VerifierKindDto::Stripe => K::Stripe,
        VerifierKindDto::Github => K::Github,
        VerifierKindDto::Slack => K::Slack,
        VerifierKindDto::Standard => K::Standard,
        VerifierKindDto::Hmac => K::Hmac,
    }
}

impl TryFrom<pb::Webhook> for WebhookDto {
    type Error = ApiError;

    fn try_from(w: pb::Webhook) -> Result<Self, ApiError> {
        use pb::webhook_action::Kind;
        use pb::webhook_signal_action::RunBy;
        let action = match w.action.and_then(|a| a.kind) {
            Some(Kind::Start(s)) => WebhookActionDto::Start {
                workflow: s.workflow,
                version: s.version,
                input: if s.input.is_empty() {
                    default_hook_input()
                } else {
                    s.input
                },
            },
            Some(Kind::Signal(s)) => WebhookActionDto::Signal {
                workflow: s.workflow,
                name: s.name,
                run_by: match s.run_by {
                    Some(RunBy::RunIdPointer(p)) => RunByDto::RunIdPointer(p),
                    Some(RunBy::IdempotencyPointer(p)) => RunByDto::IdempotencyPointer(p),
                    None => {
                        return Err(ApiError::invalid_argument("action.run_by is required"));
                    }
                },
            },
            Some(Kind::CompleteToken(c)) => WebhookActionDto::CompleteToken {
                token_pointer: c.token_pointer,
            },
            None => return Err(ApiError::invalid_argument("action is required")),
        };
        let v = w.verifier.unwrap_or_default();
        Ok(WebhookDto {
            name: w.name,
            action,
            verifier: VerifierDto {
                kind: verifier_kind_from_proto(v.kind)?,
                secret_ref: v.secret_ref,
                tolerance_s: v.tolerance_s,
                header: v.header,
                prefix: v.prefix,
                algorithm: v.algorithm,
                encoding: v.encoding,
                timestamp_header: v.timestamp_header,
                template: v.template,
            },
            idempotency: w.idempotency,
            version_tag: w.version_tag,
        })
    }
}

impl From<WebhookDto> for pb::Webhook {
    fn from(w: WebhookDto) -> Self {
        use pb::webhook_action::Kind;
        use pb::webhook_signal_action::RunBy;
        let kind = match w.action {
            WebhookActionDto::Start {
                workflow,
                version,
                input,
            } => Kind::Start(pb::WebhookStartAction {
                workflow,
                version,
                input,
            }),
            WebhookActionDto::Signal {
                workflow,
                run_by,
                name,
            } => Kind::Signal(pb::WebhookSignalAction {
                workflow,
                name,
                run_by: Some(match run_by {
                    RunByDto::RunIdPointer(p) => RunBy::RunIdPointer(p),
                    RunByDto::IdempotencyPointer(p) => RunBy::IdempotencyPointer(p),
                }),
            }),
            WebhookActionDto::CompleteToken { token_pointer } => {
                Kind::CompleteToken(pb::WebhookCompleteTokenAction { token_pointer })
            }
        };
        let v = w.verifier;
        pb::Webhook {
            name: w.name,
            action: Some(pb::WebhookAction { kind: Some(kind) }),
            verifier: Some(pb::WebhookVerifier {
                kind: verifier_kind_to_proto(v.kind) as i32,
                secret_ref: v.secret_ref,
                tolerance_s: v.tolerance_s,
                header: v.header,
                prefix: v.prefix,
                algorithm: v.algorithm,
                encoding: v.encoding,
                timestamp_header: v.timestamp_header,
                template: v.template,
            }),
            idempotency: w.idempotency,
            version_tag: w.version_tag,
        }
    }
}

fn level_from_proto(v: i32) -> Result<LogLevelDto, ApiError> {
    use pb::LogLevel as L;
    match L::try_from(v) {
        Ok(L::Trace) => Ok(LogLevelDto::Trace),
        Ok(L::Debug) => Ok(LogLevelDto::Debug),
        Ok(L::Info) => Ok(LogLevelDto::Info),
        Ok(L::Warn) => Ok(LogLevelDto::Warn),
        Ok(L::Error) => Ok(LogLevelDto::Error),
        Ok(L::Unspecified) | Err(_) => Err(ApiError::invalid_argument(format!(
            "level: invalid value {v}"
        ))),
    }
}

fn level_to_proto(l: LogLevelDto) -> pb::LogLevel {
    match l {
        LogLevelDto::Trace => pb::LogLevel::Trace,
        LogLevelDto::Debug => pb::LogLevel::Debug,
        LogLevelDto::Info => pb::LogLevel::Info,
        LogLevelDto::Warn => pb::LogLevel::Warn,
        LogLevelDto::Error => pb::LogLevel::Error,
    }
}

impl From<RunLogsResponse> for pb::GetRunLogsResponse {
    fn from(r: RunLogsResponse) -> Self {
        pb::GetRunLogsResponse {
            run_id: r.run_id,
            lines: r
                .lines
                .into_iter()
                .map(|l| pb::RunLogLine {
                    ts_ms: l.ts_ms,
                    level: level_to_proto(l.level) as i32,
                    message: l.message,
                    fields_json: json_to_proto(&l.fields),
                    attempt: l.attempt,
                })
                .collect(),
            dropped: r.dropped,
        }
    }
}

// ---------------------------------------------------------------------------------
// gRPC services
// ---------------------------------------------------------------------------------

type GrpcResult<T> = Result<tonic::Response<T>, tonic::Status>;

fn ok<T>(v: T) -> GrpcResult<T> {
    Ok(tonic::Response::new(v))
}

/// `ostro.api.v1.Schedules` over a [`ScheduleApi`].
#[derive(Clone)]
pub struct GrpcSchedules(pub Arc<dyn ScheduleApi>);

/// `ostro.api.v1.Webhooks` over a [`WebhookApi`].
#[derive(Clone)]
pub struct GrpcWebhooks(pub Arc<dyn WebhookApi>);

/// `ostro.api.v1.RunLogs` over a [`RunLogsApi`].
#[derive(Clone)]
pub struct GrpcRunLogs(pub Arc<dyn RunLogsApi>);

fn required<T>(field: &str, v: Option<T>) -> Result<T, ApiError> {
    v.ok_or_else(|| ApiError::invalid_argument(format!("{field} is required")))
}

#[tonic::async_trait]
impl schedules_server::Schedules for GrpcSchedules {
    async fn list(
        &self,
        _request: tonic::Request<pb::ListSchedulesRequest>,
    ) -> GrpcResult<pb::ListSchedulesResponse> {
        let schedules = self.0.list_schedules().await.map_err(to_status)?;
        ok(pb::ListSchedulesResponse {
            schedules: schedules.into_iter().map(Into::into).collect(),
        })
    }

    async fn get(
        &self,
        request: tonic::Request<pb::GetScheduleRequest>,
    ) -> GrpcResult<pb::Schedule> {
        let name = request.into_inner().name;
        validate_resource_name("schedule", &name).map_err(to_status)?;
        let s = self.0.get_schedule(name).await.map_err(to_status)?;
        ok(s.into())
    }

    async fn create(
        &self,
        request: tonic::Request<pb::CreateScheduleRequest>,
    ) -> GrpcResult<pb::Schedule> {
        let res = async {
            let s = ScheduleDto::try_from(required("schedule", request.into_inner().schedule)?)?;
            op_create_schedule(self.0.as_ref(), s).await
        }
        .await;
        ok(res.map_err(to_status)?.into())
    }

    async fn update(
        &self,
        request: tonic::Request<pb::UpdateScheduleRequest>,
    ) -> GrpcResult<pb::Schedule> {
        let res = async {
            let s = ScheduleDto::try_from(required("schedule", request.into_inner().schedule)?)?;
            let name = s.name.clone();
            op_update_schedule(self.0.as_ref(), name, s, None).await
        }
        .await;
        ok(res.map_err(to_status)?.into())
    }

    async fn delete(
        &self,
        request: tonic::Request<pb::DeleteScheduleRequest>,
    ) -> GrpcResult<pb::DeleteScheduleResponse> {
        let r = request.into_inner();
        validate_resource_name("schedule", &r.name).map_err(to_status)?;
        self.0
            .delete_schedule(r.name.clone(), r.version_tag)
            .await
            .map_err(to_status)?;
        ok(pb::DeleteScheduleResponse {
            name: r.name,
            deleted: true,
        })
    }

    async fn pause(
        &self,
        request: tonic::Request<pb::PauseScheduleRequest>,
    ) -> GrpcResult<pb::Schedule> {
        let name = request.into_inner().name;
        validate_resource_name("schedule", &name).map_err(to_status)?;
        ok(self.0.pause_schedule(name).await.map_err(to_status)?.into())
    }

    async fn resume(
        &self,
        request: tonic::Request<pb::ResumeScheduleRequest>,
    ) -> GrpcResult<pb::Schedule> {
        let name = request.into_inner().name;
        validate_resource_name("schedule", &name).map_err(to_status)?;
        ok(self
            .0
            .resume_schedule(name)
            .await
            .map_err(to_status)?
            .into())
    }

    async fn trigger(
        &self,
        request: tonic::Request<pb::TriggerScheduleRequest>,
    ) -> GrpcResult<pb::TriggerScheduleResponse> {
        let r = request.into_inner();
        let res = op_trigger_schedule(self.0.as_ref(), r.name, r.request_id)
            .await
            .map_err(to_status)?;
        ok(pb::TriggerScheduleResponse { run_id: res.run_id })
    }
}

#[tonic::async_trait]
impl webhooks_server::Webhooks for GrpcWebhooks {
    async fn list(
        &self,
        _request: tonic::Request<pb::ListWebhooksRequest>,
    ) -> GrpcResult<pb::ListWebhooksResponse> {
        let webhooks = self.0.list_webhooks().await.map_err(to_status)?;
        ok(pb::ListWebhooksResponse {
            webhooks: webhooks.into_iter().map(Into::into).collect(),
        })
    }

    async fn get(&self, request: tonic::Request<pb::GetWebhookRequest>) -> GrpcResult<pb::Webhook> {
        let name = request.into_inner().name;
        validate_resource_name("webhook", &name).map_err(to_status)?;
        ok(self.0.get_webhook(name).await.map_err(to_status)?.into())
    }

    async fn create(
        &self,
        request: tonic::Request<pb::CreateWebhookRequest>,
    ) -> GrpcResult<pb::Webhook> {
        let res = async {
            let w = WebhookDto::try_from(required("webhook", request.into_inner().webhook)?)?;
            op_create_webhook(self.0.as_ref(), w).await
        }
        .await;
        ok(res.map_err(to_status)?.into())
    }

    async fn update(
        &self,
        request: tonic::Request<pb::UpdateWebhookRequest>,
    ) -> GrpcResult<pb::Webhook> {
        let res = async {
            let w = WebhookDto::try_from(required("webhook", request.into_inner().webhook)?)?;
            let name = w.name.clone();
            op_update_webhook(self.0.as_ref(), name, w, None).await
        }
        .await;
        ok(res.map_err(to_status)?.into())
    }

    async fn delete(
        &self,
        request: tonic::Request<pb::DeleteWebhookRequest>,
    ) -> GrpcResult<pb::DeleteWebhookResponse> {
        let r = request.into_inner();
        validate_resource_name("webhook", &r.name).map_err(to_status)?;
        self.0
            .delete_webhook(r.name.clone(), r.version_tag)
            .await
            .map_err(to_status)?;
        ok(pb::DeleteWebhookResponse {
            name: r.name,
            deleted: true,
        })
    }
}

#[tonic::async_trait]
impl run_logs_server::RunLogs for GrpcRunLogs {
    async fn get_run_logs(
        &self,
        request: tonic::Request<pb::GetRunLogsRequest>,
    ) -> GrpcResult<pb::GetRunLogsResponse> {
        let r = request.into_inner();
        let res = async {
            let level = r.level.map(level_from_proto).transpose()?;
            op_run_logs(self.0.as_ref(), r.run_id, r.limit, level).await
        }
        .await;
        ok(res.map_err(to_status)?.into())
    }
}

// ---------------------------------------------------------------------------------
// In-memory fake (adapter and parity tests)
// ---------------------------------------------------------------------------------

/// In-memory implementations of every Phase 8 trait over a
/// [`crate::fake::FakeControlPlane`] (re-exported as `ostro_api::fake::FakePhase8`).
///
/// - Schedules / webhooks: CRUD with a per-record `version_tag` starting at 1; a stale
///   tag is [`version_mismatch`]. Any non-empty cron is accepted; `next_fire_at_ms` stays
///   unset.
/// - Trigger: starts the schedule's workflow on the fake control plane (tag
///   [`SCHEDULE_RUN_TAG`]; a `request_id` maps to an idempotency key, so a repeat
///   returns the same run).
/// - Hooks: an unknown name is 404; a verifier other than `none` answers 401 (the fake
///   verifies nothing); a Slack-style `url_verification` body echoes its challenge as
///   text; a `start` action starts the workflow with the JSON body as input (202); other
///   actions answer 202 `{"accepted": true}`.
/// - Run logs: lines added with [`fake::FakePhase8::push_log`]; an unknown run is 404.
/// - Backup: a fixed body ([`fake::FAKE_BACKUP`]).
#[cfg(any(test, feature = "fake"))]
pub mod fake {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::domain::{ControlPlane, StartRunRequest};
    use crate::fake::FakeControlPlane;

    /// The fake backup body.
    pub const FAKE_BACKUP: &[u8] = b"ostro-fake-backup";

    #[derive(Default)]
    struct State {
        schedules: BTreeMap<String, ScheduleDto>,
        webhooks: BTreeMap<String, WebhookDto>,
        logs: BTreeMap<String, Vec<RunLogLine>>,
        deliveries: Vec<(String, Bytes)>,
    }

    /// See the [module docs](self).
    pub struct FakePhase8 {
        cp: Arc<FakeControlPlane>,
        state: Mutex<State>,
    }

    impl FakePhase8 {
        /// Over `cp` (runs started by triggers and hooks land there).
        pub fn new(cp: Arc<FakeControlPlane>) -> Arc<Self> {
            Arc::new(FakePhase8 {
                cp,
                state: Mutex::new(State::default()),
            })
        }

        /// Every slot filled with this fake.
        pub fn extensions(self: &Arc<Self>) -> ApiExtensions {
            ApiExtensions {
                schedules: Some(Arc::clone(self) as Arc<dyn ScheduleApi>),
                webhooks: Some(Arc::clone(self) as Arc<dyn WebhookApi>),
                hooks: Some(Arc::clone(self) as Arc<dyn HookIngress>),
                run_logs: Some(Arc::clone(self) as Arc<dyn RunLogsApi>),
                admin: Some(Arc::clone(self) as Arc<dyn AdminApi>),
                hook_body_limit: DEFAULT_HOOK_BODY_LIMIT,
                ..ApiExtensions::default()
            }
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, State> {
            self.state.lock().unwrap_or_else(|p| p.into_inner())
        }

        /// Append a guest log line for `run_id`.
        pub fn push_log(&self, run_id: &str, line: RunLogLine) {
            self.lock()
                .logs
                .entry(run_id.to_owned())
                .or_default()
                .push(line);
        }

        /// `(name, body)` of every delivery that reached a known webhook, in order.
        pub fn deliveries(&self) -> Vec<(String, Bytes)> {
            self.lock().deliveries.clone()
        }

        fn schedule_mut<'a>(
            st: &'a mut State,
            name: &str,
        ) -> Result<&'a mut ScheduleDto, ApiError> {
            st.schedules
                .get_mut(name)
                .ok_or_else(|| ApiError::not_found(format!("schedule {name} not found")))
        }

        async fn start(&self, req: StartRunRequest) -> Result<String, ApiError> {
            Ok(self.cp.start_run(req).await?.run_id)
        }
    }

    fn check_tag(
        what: &str,
        name: &str,
        stored: Option<u64>,
        want: Option<u64>,
    ) -> Result<(), ApiError> {
        let cur = stored.unwrap_or(0);
        match want {
            Some(w) if w != cur => Err(version_mismatch(what, name, cur)),
            _ => Ok(()),
        }
    }

    #[async_trait::async_trait]
    impl ScheduleApi for FakePhase8 {
        async fn list_schedules(&self) -> Result<Vec<ScheduleDto>, ApiError> {
            Ok(self.lock().schedules.values().cloned().collect())
        }

        async fn get_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
            let mut st = self.lock();
            Ok(Self::schedule_mut(&mut st, &name)?.clone())
        }

        async fn create_schedule(&self, mut s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
            let mut st = self.lock();
            if st.schedules.contains_key(&s.name) {
                return Err(ApiError::conflict(format!("schedule {} exists", s.name)));
            }
            s.version_tag = Some(1);
            st.schedules.insert(s.name.clone(), s.clone());
            Ok(s)
        }

        async fn update_schedule(&self, mut s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
            let mut st = self.lock();
            let cur = Self::schedule_mut(&mut st, &s.name)?;
            check_tag("schedule", &s.name, cur.version_tag, s.version_tag)?;
            s.version_tag = Some(cur.version_tag.unwrap_or(0) + 1);
            s.last_fire_at_ms = cur.last_fire_at_ms;
            *cur = s.clone();
            Ok(s)
        }

        async fn delete_schedule(
            &self,
            name: String,
            version_tag: Option<u64>,
        ) -> Result<(), ApiError> {
            let mut st = self.lock();
            let cur = Self::schedule_mut(&mut st, &name)?;
            check_tag("schedule", &name, cur.version_tag, version_tag)?;
            st.schedules.remove(&name);
            Ok(())
        }

        async fn pause_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
            let mut st = self.lock();
            let cur = Self::schedule_mut(&mut st, &name)?;
            if !cur.paused {
                cur.paused = true;
                cur.version_tag = Some(cur.version_tag.unwrap_or(0) + 1);
            }
            Ok(cur.clone())
        }

        async fn resume_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
            let mut st = self.lock();
            let cur = Self::schedule_mut(&mut st, &name)?;
            if cur.paused {
                cur.paused = false;
                cur.version_tag = Some(cur.version_tag.unwrap_or(0) + 1);
            }
            Ok(cur.clone())
        }

        async fn trigger_schedule(
            &self,
            name: String,
            request_id: Option<String>,
        ) -> Result<String, ApiError> {
            let s = {
                let mut st = self.lock();
                Self::schedule_mut(&mut st, &name)?.clone()
            };
            self.start(StartRunRequest {
                workflow: s.workflow,
                input_json: json_to_proto(&s.input),
                version: s.version,
                tags: vec![SCHEDULE_RUN_TAG.to_owned()],
                idempotency_key: request_id.map(|r| format!("__cron/{name}/manual/{r}")),
                ..StartRunRequest::default()
            })
            .await
        }
    }

    #[async_trait::async_trait]
    impl WebhookApi for FakePhase8 {
        async fn list_webhooks(&self) -> Result<Vec<WebhookDto>, ApiError> {
            Ok(self.lock().webhooks.values().cloned().collect())
        }

        async fn get_webhook(&self, name: String) -> Result<WebhookDto, ApiError> {
            self.lock()
                .webhooks
                .get(&name)
                .cloned()
                .ok_or_else(|| ApiError::not_found(format!("webhook {name} not found")))
        }

        async fn create_webhook(&self, mut w: WebhookDto) -> Result<WebhookDto, ApiError> {
            let mut st = self.lock();
            if st.webhooks.contains_key(&w.name) {
                return Err(ApiError::conflict(format!("webhook {} exists", w.name)));
            }
            w.version_tag = Some(1);
            st.webhooks.insert(w.name.clone(), w.clone());
            Ok(w)
        }

        async fn update_webhook(&self, mut w: WebhookDto) -> Result<WebhookDto, ApiError> {
            let mut st = self.lock();
            let cur = st
                .webhooks
                .get_mut(&w.name)
                .ok_or_else(|| ApiError::not_found(format!("webhook {} not found", w.name)))?;
            check_tag("webhook", &w.name, cur.version_tag, w.version_tag)?;
            w.version_tag = Some(cur.version_tag.unwrap_or(0) + 1);
            *cur = w.clone();
            Ok(w)
        }

        async fn delete_webhook(
            &self,
            name: String,
            version_tag: Option<u64>,
        ) -> Result<(), ApiError> {
            let mut st = self.lock();
            let cur = st
                .webhooks
                .get(&name)
                .ok_or_else(|| ApiError::not_found(format!("webhook {name} not found")))?;
            check_tag("webhook", &name, cur.version_tag, version_tag)?;
            st.webhooks.remove(&name);
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl HookIngress for FakePhase8 {
        async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError> {
            let w = {
                let mut st = self.lock();
                let w = st.webhooks.get(&req.name).cloned().ok_or_else(|| {
                    ApiError::not_found(format!("webhook {} not found", req.name))
                })?;
                st.deliveries.push((req.name.clone(), req.body.clone()));
                w
            };
            if w.verifier.kind != VerifierKindDto::None {
                return Ok(HookResponse::json(
                    401,
                    &serde_json::json!({ "error": "the fake verifies no signatures" }),
                ));
            }
            // Verifier "none": the body may be parsed now.
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();
            if body["type"] == "url_verification"
                && let Some(c) = body["challenge"].as_str()
            {
                return Ok(HookResponse::text(200, c));
            }
            match w.action {
                WebhookActionDto::Start {
                    workflow, version, ..
                } => {
                    let run_id = self
                        .start(StartRunRequest {
                            workflow,
                            version,
                            input_json: json_to_proto(&body),
                            ..StartRunRequest::default()
                        })
                        .await?;
                    Ok(HookResponse::json(
                        202,
                        &serde_json::json!({ "run_id": run_id }),
                    ))
                }
                _ => Ok(HookResponse::json(
                    202,
                    &serde_json::json!({ "accepted": true }),
                )),
            }
        }
    }

    #[async_trait::async_trait]
    impl RunLogsApi for FakePhase8 {
        async fn run_logs(&self, req: RunLogsRequest) -> Result<RunLogsResponse, ApiError> {
            self.cp.get_run(req.run_id.clone()).await?;
            let st = self.lock();
            let all: Vec<RunLogLine> = st
                .logs
                .get(&req.run_id)
                .map(|v| {
                    v.iter()
                        .filter(|l| req.level.is_none_or(|min| l.level >= min))
                        .cloned()
                        .collect()
                })
                .unwrap_or_default();
            let skip = all.len().saturating_sub(req.limit as usize);
            Ok(RunLogsResponse {
                run_id: req.run_id,
                lines: all.into_iter().skip(skip).collect(),
                dropped: 0,
            })
        }
    }

    #[async_trait::async_trait]
    impl AdminApi for FakePhase8 {
        async fn backup(&self) -> Result<BackupStream, ApiError> {
            Ok(BackupStream::new(Bytes::from_static(FAKE_BACKUP)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    const TP: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn traceparent_validation() {
        validate_traceparent(TP).expect("valid");
        for bad in [
            "",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",
            "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01",
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-0g",
            "00_0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-x",
        ] {
            let e = validate_traceparent(bad).expect_err(bad);
            assert_eq!(e.code, ErrorCode::InvalidArgument);
            assert_eq!(e.reason(), Some(reason::INVALID_TRACEPARENT), "{bad}");
        }
    }

    #[test]
    fn traceparent_resolution_body_wins() {
        let other = "00-11111111111111111111111111111111-2222222222222222-00";
        assert_eq!(resolve_traceparent(None, None).unwrap(), None);
        assert_eq!(
            resolve_traceparent(None, Some(TP)).unwrap().as_deref(),
            Some(TP)
        );
        assert_eq!(
            resolve_traceparent(Some(TP.into()), None)
                .unwrap()
                .as_deref(),
            Some(TP)
        );
        assert_eq!(
            resolve_traceparent(Some(TP.into()), Some(other))
                .unwrap()
                .as_deref(),
            Some(TP)
        );
        assert_eq!(
            resolve_traceparent(Some(String::new()), Some(other))
                .unwrap()
                .as_deref(),
            Some(other)
        );
        assert_eq!(
            resolve_traceparent(Some(String::new()), Some("")).unwrap(),
            None
        );
        assert!(resolve_traceparent(None, Some("nope")).is_err());
        assert!(resolve_traceparent(Some("nope".into()), Some(TP)).is_err());
    }

    #[test]
    fn resource_names() {
        validate_resource_name("schedule", "a").unwrap();
        validate_resource_name("schedule", "Nightly_report-v1.2").unwrap();
        validate_resource_name("schedule", &"x".repeat(128)).unwrap();
        for bad in ["", "a/b", "a b", "ü", &"x".repeat(129)] {
            assert_eq!(
                validate_resource_name("schedule", bad).unwrap_err().code,
                ErrorCode::InvalidArgument,
                "{bad:?}"
            );
        }
    }

    #[test]
    fn policy_numbering_round_trips() {
        for (i, p) in OverlapPolicyDto::ALL.into_iter().enumerate() {
            assert_eq!(usize::from(p.as_u8()), i);
            assert_eq!(OverlapPolicyDto::from_u8(p.as_u8()), Some(p));
        }
        assert_eq!(OverlapPolicyDto::from_u8(5), None);
        for (i, p) in CatchupPolicyDto::ALL.into_iter().enumerate() {
            assert_eq!(usize::from(p.as_u8()), i);
            assert_eq!(CatchupPolicyDto::from_u8(p.as_u8()), Some(p));
        }
        assert_eq!(CatchupPolicyDto::from_u8(3), None);
        assert_eq!(
            serde_json::to_value(OverlapPolicyDto::TerminateOther).unwrap(),
            "terminate_other"
        );
        assert_eq!(serde_json::to_value(CatchupPolicyDto::One).unwrap(), "one");
    }

    #[test]
    fn proto_round_trips() {
        let s = ScheduleDto {
            name: "n".into(),
            workflow: "w".into(),
            cron: "* * * * *".into(),
            input: serde_json::json!({"a": 1}),
            overlap: OverlapPolicyDto::CancelOther,
            catchup: CatchupPolicyDto::All,
            version_tag: Some(2),
            ..ScheduleDto::default()
        };
        assert_eq!(
            ScheduleDto::try_from(pb::Schedule::from(s.clone())).unwrap(),
            s
        );
        let bad = pb::Schedule {
            overlap: 9,
            ..pb::Schedule::default()
        };
        assert!(ScheduleDto::try_from(bad).is_err());

        for action in [
            WebhookActionDto::Start {
                workflow: "w".into(),
                version: Some(2),
                input: "/data".into(),
            },
            WebhookActionDto::Signal {
                workflow: "w".into(),
                run_by: RunByDto::IdempotencyPointer("/k".into()),
                name: "go".into(),
            },
            WebhookActionDto::CompleteToken {
                token_pointer: "/t".into(),
            },
        ] {
            let w = WebhookDto {
                name: "h".into(),
                action,
                verifier: VerifierDto {
                    kind: VerifierKindDto::Hmac,
                    secret_ref: "s".into(),
                    header: Some("X-Sig".into()),
                    ..VerifierDto::default()
                },
                idempotency: Some("json:/id".into()),
                version_tag: Some(1),
            };
            assert_eq!(
                WebhookDto::try_from(pb::Webhook::from(w.clone())).unwrap(),
                w
            );
        }
        assert!(WebhookDto::try_from(pb::Webhook::default()).is_err());
    }

    #[test]
    fn webhook_validation() {
        let base = WebhookDto {
            name: "h".into(),
            action: WebhookActionDto::Start {
                workflow: "w".into(),
                version: None,
                input: "body".into(),
            },
            verifier: VerifierDto::default(),
            idempotency: None,
            version_tag: None,
        };
        validate_webhook(&base).unwrap();
        let mut w = base.clone();
        w.verifier.kind = VerifierKindDto::Stripe;
        assert!(validate_webhook(&w).is_err(), "secret_ref required");
        w.verifier.secret_ref = "stripe".into();
        validate_webhook(&w).unwrap();
        for bad in ["", "header:", "json:x", "both"] {
            let mut w = base.clone();
            w.idempotency = Some(bad.into());
            assert!(validate_webhook(&w).is_err(), "{bad}");
        }
        let mut w = base;
        w.action = WebhookActionDto::Start {
            workflow: "w".into(),
            version: None,
            input: "data".into(),
        };
        assert!(validate_webhook(&w).is_err());
    }

    #[test]
    fn webhook_action_json_shape() {
        let v = serde_json::json!({
            "name": "gh",
            "action": {"kind": "signal", "workflow": "w", "name": "push",
                       "run_by": {"run_id_pointer": "/run"}},
            "verifier": {"kind": "github", "secret_ref": "gh"},
            "idempotency": "header:X-GitHub-Delivery"
        });
        let w: WebhookDto = serde_json::from_value(v).expect("parse");
        assert_eq!(
            w.action,
            WebhookActionDto::Signal {
                workflow: "w".into(),
                run_by: RunByDto::RunIdPointer("/run".into()),
                name: "push".into()
            }
        );
        let start: WebhookActionDto =
            serde_json::from_value(serde_json::json!({"kind": "start", "workflow": "w"})).unwrap();
        assert_eq!(
            start,
            WebhookActionDto::Start {
                workflow: "w".into(),
                version: None,
                input: "body".into()
            }
        );
    }
}
