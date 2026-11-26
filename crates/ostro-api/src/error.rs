//! `ApiError`: the one error type of the control plane, and its transport mappings.
//!
//! HTTP and gRPC statuses come from the single tables in [`ApiError::http_status`] and
//! [`ApiError::grpc_code`]; adapters never pick statuses themselves.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Transport-neutral error category.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    /// Malformed or semantically invalid request.
    InvalidArgument,
    /// The named entity does not exist.
    NotFound,
    /// The request conflicts with existing state.
    Conflict,
    /// A precondition (e.g. expected record version) does not hold.
    PreconditionFailed,
    /// The request body exceeds a configured limit.
    PayloadTooLarge,
    /// The server cannot serve the request right now (retryable).
    Unavailable,
    /// Unexpected server failure.
    Internal,
    /// The operation is not implemented by this control plane (yet).
    Unimplemented,
    /// A rate limit rejected the request (Phase 7, D-W21): HTTP 429 with `Retry-After`,
    /// gRPC `RESOURCE_EXHAUSTED`; `details.retry_after_ms` says when to retry. gRPC shares
    /// `RESOURCE_EXHAUSTED` with [`ErrorCode::PayloadTooLarge`]; clients tell them apart with
    /// [`ErrorCode::from_grpc_code_and_details`].
    RateLimited,
    /// Phase 14 (14-01, D-P14-6): missing, malformed, unknown or expired credentials. HTTP
    /// 401 with `WWW-Authenticate: Bearer`, gRPC `UNAUTHENTICATED`.
    Unauthenticated,
    /// Phase 14 (14-01, D-P14-3/7/10/14): authenticated but not allowed (missing scope,
    /// foreign tenant field, suspended tenant, signature required). HTTP 403, gRPC
    /// `PERMISSION_DENIED`; `details.reason` says which.
    PermissionDenied,
}

impl ErrorCode {
    /// Every code, in declaration order.
    pub const ALL: [ErrorCode; 11] = [
        ErrorCode::InvalidArgument,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::PreconditionFailed,
        ErrorCode::PayloadTooLarge,
        ErrorCode::Unavailable,
        ErrorCode::Internal,
        ErrorCode::Unimplemented,
        ErrorCode::RateLimited,
        ErrorCode::Unauthenticated,
        ErrorCode::PermissionDenied,
    ];

    /// snake_case wire name (REST `error.code`).
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::InvalidArgument => "invalid_argument",
            ErrorCode::NotFound => "not_found",
            ErrorCode::Conflict => "conflict",
            ErrorCode::PreconditionFailed => "precondition_failed",
            ErrorCode::PayloadTooLarge => "payload_too_large",
            ErrorCode::Unavailable => "unavailable",
            ErrorCode::Internal => "internal",
            ErrorCode::Unimplemented => "unimplemented",
            ErrorCode::RateLimited => "rate_limited",
            ErrorCode::Unauthenticated => "unauthenticated",
            ErrorCode::PermissionDenied => "permission_denied",
        }
    }

    /// HTTP status for this code.
    pub fn http_status(self) -> StatusCode {
        match self {
            ErrorCode::InvalidArgument => StatusCode::BAD_REQUEST,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::Conflict => StatusCode::CONFLICT,
            ErrorCode::PreconditionFailed => StatusCode::PRECONDITION_FAILED,
            ErrorCode::PayloadTooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::Unauthenticated => StatusCode::UNAUTHORIZED,
            ErrorCode::PermissionDenied => StatusCode::FORBIDDEN,
        }
    }

    /// gRPC status code for this code.
    pub fn grpc_code(self) -> tonic::Code {
        match self {
            ErrorCode::InvalidArgument => tonic::Code::InvalidArgument,
            ErrorCode::NotFound => tonic::Code::NotFound,
            ErrorCode::Conflict => tonic::Code::AlreadyExists,
            ErrorCode::PreconditionFailed => tonic::Code::FailedPrecondition,
            ErrorCode::PayloadTooLarge => tonic::Code::ResourceExhausted,
            ErrorCode::Unavailable => tonic::Code::Unavailable,
            ErrorCode::Internal => tonic::Code::Internal,
            ErrorCode::Unimplemented => tonic::Code::Unimplemented,
            ErrorCode::RateLimited => tonic::Code::ResourceExhausted,
            ErrorCode::Unauthenticated => tonic::Code::Unauthenticated,
            ErrorCode::PermissionDenied => tonic::Code::PermissionDenied,
        }
    }

    /// Inverse of [`ErrorCode::grpc_code`] (unknown codes -> Internal). `RESOURCE_EXHAUSTED`
    /// maps to [`ErrorCode::PayloadTooLarge`]; use [`ErrorCode::from_grpc_code_and_details`]
    /// to recognise [`ErrorCode::RateLimited`].
    pub fn from_grpc_code(code: tonic::Code) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|c| c.grpc_code() == code)
            .unwrap_or(ErrorCode::Internal)
    }

    /// [`ErrorCode::from_grpc_code`] refined by the status details: `RESOURCE_EXHAUSTED`
    /// with a `retry_after_ms` detail is [`ErrorCode::RateLimited`].
    pub fn from_grpc_code_and_details(
        code: tonic::Code,
        details: Option<&serde_json::Value>,
    ) -> ErrorCode {
        if code == tonic::Code::ResourceExhausted
            && details.is_some_and(|d| d.get(RETRY_AFTER_MS).is_some())
        {
            return ErrorCode::RateLimited;
        }
        Self::from_grpc_code(code)
    }

    /// Inverse of [`ErrorCode::http_status`] (unknown statuses -> Internal).
    pub fn from_http_status(status: StatusCode) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|c| c.http_status() == status)
            .unwrap_or(ErrorCode::Internal)
    }
}

/// The `details` key of [`ErrorCode::RateLimited`]: milliseconds until a retry may succeed.
pub const RETRY_AFTER_MS: &str = "retry_after_ms";

/// Machine-readable `details.reason` values that refine an [`ErrorCode`].
///
/// Several conditions share one transport status (e.g. every 409), so the HTTP and
/// gRPC tables stay bijective and the specific condition travels in the details as
/// `{"reason": "<value>", ...}` on both transports.
pub mod reason {
    /// The run is already terminal (signal, cancel, terminate, token completion). 409.
    pub const RUN_TERMINAL: &str = "run_terminal";
    /// Retry of a run that completed successfully. 409.
    pub const RUN_COMPLETED: &str = "run_completed";
    /// Retry of a run that is not terminal yet. 409.
    pub const RUN_NOT_TERMINAL: &str = "run_not_terminal";
    /// The await token was already completed (or its op timed out / was cancelled). 409.
    pub const TOKEN_ALREADY_COMPLETED: &str = "token_already_completed";
    /// The await token is malformed or fails verification. 400.
    pub const TOKEN_INVALID: &str = "token_invalid";
    // Phase 14 (14-01; RESERVATIONS §11, D-P14-29).
    /// No, a malformed, an unknown, a revoked or an expired bearer. 401.
    pub const UNAUTHENTICATED: &str = "unauthenticated";
    /// The principal lacks the scope the method needs. 403.
    pub const MISSING_SCOPE: &str = "missing_scope";
    /// A tenant principal named another tenant in a request field (D-P14-38). 403.
    pub const FORBIDDEN_TENANT: &str = "forbidden_tenant";
    /// The principal's tenant is suspended. 403.
    pub const TENANT_SUSPENDED: &str = "tenant_suspended";
    /// The tenant's `event_rate` GCRA refused the request. 429.
    pub const TENANT_EVENT_RATE: &str = "tenant_event_rate";
    /// Tenant delete refused: it still owns versions, runs, tokens or secrets. 409.
    pub const TENANT_NOT_EMPTY: &str = "tenant_not_empty";
    /// Signed modules are required and the upload / activation has no valid evidence. 403.
    pub const SIGNATURE_REQUIRED: &str = "signature_required";
    /// A signature was supplied and does not verify. 400.
    pub const SIGNATURE_INVALID: &str = "signature_invalid";
    /// No audit permit within `[audit] reserve_timeout`: not executed. 503.
    pub const AUDIT_UNAVAILABLE: &str = "audit_unavailable";
    /// The audit record failed after the action executed (`details.executed = true`). 500.
    pub const AUDIT_COMMIT_FAILED: &str = "audit_commit_failed";
}

/// Control-plane error: code, human message and optional structured details.
#[derive(Clone, Debug, PartialEq)]
pub struct ApiError {
    /// Category (drives the transport status).
    pub code: ErrorCode,
    /// Human-readable message.
    pub message: String,
    /// Structured details, e.g. `{ "existing_run_id": "..." }`.
    pub details: Option<serde_json::Value>,
}

impl ApiError {
    /// Error without details.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        ApiError {
            code,
            message: message.into(),
            details: None,
        }
    }

    /// Attach structured details.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }

    /// InvalidArgument.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::InvalidArgument, message)
    }

    /// NotFound.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, message)
    }

    /// Conflict.
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, message)
    }

    /// PreconditionFailed.
    pub fn precondition_failed(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PreconditionFailed, message)
    }

    /// PayloadTooLarge.
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PayloadTooLarge, message)
    }

    /// Unavailable.
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unavailable, message)
    }

    /// Internal.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// Unimplemented (HTTP 501, gRPC UNIMPLEMENTED): the default body of every
    /// [`crate::ControlPlane`] method a control plane does not provide yet.
    pub fn unimplemented(method: &str) -> Self {
        Self::new(
            ErrorCode::Unimplemented,
            format!("{method} is not implemented by this server"),
        )
        .with_details(serde_json::json!({ "method": method }))
    }

    /// RateLimited (HTTP 429 + `Retry-After`, gRPC RESOURCE_EXHAUSTED) with
    /// `details.retry_after_ms` (D-W21).
    pub fn rate_limited(message: impl Into<String>, retry_after_ms: u64) -> Self {
        Self::new(ErrorCode::RateLimited, message)
            .with_details(serde_json::json!({ RETRY_AFTER_MS: retry_after_ms }))
    }

    /// `details.retry_after_ms`, when present.
    pub fn retry_after_ms(&self) -> Option<u64> {
        self.details.as_ref()?.get(RETRY_AFTER_MS)?.as_u64()
    }

    /// Conflict, `details.reason = "run_terminal"`, with the run's current status.
    pub fn run_terminal(run_id: &str, status: crate::domain::RunStatus) -> Self {
        Self::conflict(format!("run {run_id} is already {}", status.as_str())).with_details(
            serde_json::json!({ "reason": reason::RUN_TERMINAL, "status": status.as_str() }),
        )
    }

    /// Conflict, `details.reason = "run_completed"` (retry of a Completed run).
    pub fn run_completed(run_id: &str) -> Self {
        Self::conflict(format!(
            "run {run_id} completed successfully and cannot be retried"
        ))
        .with_details(serde_json::json!({ "reason": reason::RUN_COMPLETED }))
    }

    /// Conflict, `details.reason = "run_not_terminal"` (retry of a live run).
    pub fn run_not_terminal(run_id: &str, status: crate::domain::RunStatus) -> Self {
        Self::conflict(format!(
            "run {run_id} is {} and cannot be retried until it is terminal",
            status.as_str()
        ))
        .with_details(
            serde_json::json!({ "reason": reason::RUN_NOT_TERMINAL, "status": status.as_str() }),
        )
    }

    /// Conflict, `details.reason = "token_already_completed"`.
    pub fn token_already_completed(run_id: &str) -> Self {
        Self::conflict("await token was already completed").with_details(
            serde_json::json!({ "reason": reason::TOKEN_ALREADY_COMPLETED, "run_id": run_id }),
        )
    }

    /// InvalidArgument, `details.reason = "token_invalid"`. The message never echoes
    /// the token.
    pub fn token_invalid(why: &str) -> Self {
        Self::invalid_argument(format!("invalid await token: {why}"))
            .with_details(serde_json::json!({ "reason": reason::TOKEN_INVALID }))
    }

    /// Phase 14: 401 `unauthenticated` (the message never echoes the credential).
    pub fn unauthenticated(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthenticated, message)
            .with_details(serde_json::json!({ "reason": reason::UNAUTHENTICATED }))
    }

    /// Phase 14: 403 with `details.reason = reason` (e.g. [`reason::MISSING_SCOPE`]).
    pub fn permission_denied(reason: &str, message: impl Into<String>) -> Self {
        Self::new(ErrorCode::PermissionDenied, message)
            .with_details(serde_json::json!({ "reason": reason }))
    }

    /// Phase 14: 403 `missing_scope` naming the scope the method needs.
    pub fn missing_scope(method: &str, scope: &str) -> Self {
        Self::new(
            ErrorCode::PermissionDenied,
            format!("{method} needs the {scope:?} scope"),
        )
        .with_details(
            serde_json::json!({ "reason": reason::MISSING_SCOPE, "method": method, "scope": scope }),
        )
    }

    /// `details.reason`, when present.
    pub fn reason(&self) -> Option<&str> {
        self.details.as_ref()?.get("reason")?.as_str()
    }

    /// HTTP status for this error.
    pub fn http_status(&self) -> StatusCode {
        self.code.http_status()
    }

    /// gRPC status code for this error.
    pub fn grpc_code(&self) -> tonic::Code {
        self.code.grpc_code()
    }

    /// REST JSON body: `{ "error": { "code", "message", "details"? } }`.
    pub fn to_body(&self) -> ErrorBody {
        ErrorBody {
            error: ErrorDetail {
                code: self.code.as_str().to_owned(),
                message: self.message.clone(),
                details: self.details.clone(),
            },
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for ApiError {}

/// REST error envelope.
#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorBody {
    /// The error.
    pub error: ErrorDetail,
}

/// REST error payload.
#[derive(Clone, Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorDetail {
    /// snake_case code: invalid_argument, not_found, conflict, precondition_failed,
    /// payload_too_large, unavailable, internal, unimplemented.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Structured details, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        // D-W21: a rate-limited answer says when to come back (whole seconds, rounded up).
        // Phase 13 (13-12): a routing 503 (`unavailable` with `retry_after_ms`, e.g. an
        // offline partition or a leader that did not answer after one re-route) says it too.
        let retry_after = match self.code {
            ErrorCode::RateLimited | ErrorCode::Unavailable => {
                self.retry_after_ms().map(|ms| ms.div_ceil(1000))
            }
            _ => None,
        };
        let mut res = (self.http_status(), axum::Json(self.to_body())).into_response();
        if let Some(secs) = retry_after {
            res.headers_mut()
                .insert(axum::http::header::RETRY_AFTER, secs.into());
        }
        // Phase 14 (D-P14-6): a 401 names the scheme (RFC 6750).
        if self.code == ErrorCode::Unauthenticated {
            res.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: [(ErrorCode, u16, tonic::Code); 11] = [
        (
            ErrorCode::InvalidArgument,
            400,
            tonic::Code::InvalidArgument,
        ),
        (ErrorCode::NotFound, 404, tonic::Code::NotFound),
        (ErrorCode::Conflict, 409, tonic::Code::AlreadyExists),
        (
            ErrorCode::PreconditionFailed,
            412,
            tonic::Code::FailedPrecondition,
        ),
        (
            ErrorCode::PayloadTooLarge,
            413,
            tonic::Code::ResourceExhausted,
        ),
        (ErrorCode::Unavailable, 503, tonic::Code::Unavailable),
        (ErrorCode::Internal, 500, tonic::Code::Internal),
        (ErrorCode::Unimplemented, 501, tonic::Code::Unimplemented),
        (ErrorCode::RateLimited, 429, tonic::Code::ResourceExhausted),
        (
            ErrorCode::Unauthenticated,
            401,
            tonic::Code::Unauthenticated,
        ),
        (
            ErrorCode::PermissionDenied,
            403,
            tonic::Code::PermissionDenied,
        ),
    ];

    #[test]
    fn mapping_table_both_ways() {
        assert_eq!(TABLE.len(), ErrorCode::ALL.len());
        for (code, http, grpc) in TABLE {
            let e = ApiError::new(code, "m");
            assert_eq!(e.http_status().as_u16(), http, "{code:?}");
            assert_eq!(e.grpc_code(), grpc, "{code:?}");
            assert_eq!(
                ErrorCode::from_http_status(StatusCode::from_u16(http).unwrap()),
                code
            );
            // RESOURCE_EXHAUSTED is shared (PayloadTooLarge, RateLimited): the details
            // disambiguate.
            let details = (code == ErrorCode::RateLimited)
                .then(|| serde_json::json!({ RETRY_AFTER_MS: 1500 }));
            assert_eq!(
                ErrorCode::from_grpc_code_and_details(grpc, details.as_ref()),
                code
            );
            if code != ErrorCode::RateLimited {
                assert_eq!(ErrorCode::from_grpc_code(grpc), code);
            }
        }
        assert_eq!(
            ErrorCode::from_grpc_code(tonic::Code::DataLoss),
            ErrorCode::Internal
        );
    }

    #[test]
    fn lifecycle_reasons() {
        use crate::domain::RunStatus;
        let e = ApiError::run_terminal("r", RunStatus::Completed);
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::Conflict, Some("run_terminal"))
        );
        assert_eq!(e.details.as_ref().unwrap()["status"], "completed");
        let e = ApiError::run_completed("r");
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::Conflict, Some("run_completed"))
        );
        let e = ApiError::run_not_terminal("r", RunStatus::Running);
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::Conflict, Some("run_not_terminal"))
        );
        let e = ApiError::token_already_completed("r");
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::Conflict, Some("token_already_completed"))
        );
        let e = ApiError::token_invalid("bad prefix");
        assert_eq!(
            (e.code, e.reason()),
            (ErrorCode::InvalidArgument, Some("token_invalid"))
        );
        let e = ApiError::unimplemented("signal_run");
        assert_eq!(e.http_status().as_u16(), 501);
        assert_eq!(e.grpc_code(), tonic::Code::Unimplemented);
        assert_eq!(e.reason(), None);
        assert_eq!(ApiError::not_found("x").reason(), None);
    }

    #[test]
    fn wire_names_unique() {
        let mut names: Vec<_> = ErrorCode::ALL.iter().map(|c| c.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ErrorCode::ALL.len());
    }

    #[test]
    fn body_shape() {
        let e = ApiError::conflict("dup")
            .with_details(serde_json::json!({ "existing_run_id": "run-000001" }));
        let v = serde_json::to_value(e.to_body()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({
                "error": {
                    "code": "conflict",
                    "message": "dup",
                    "details": { "existing_run_id": "run-000001" }
                }
            })
        );
        let v = serde_json::to_value(ApiError::not_found("x").to_body()).unwrap();
        assert_eq!(
            v,
            serde_json::json!({ "error": { "code": "not_found", "message": "x" } })
        );
    }

    #[test]
    fn rate_limited_maps_to_429_with_retry_after() {
        let e = ApiError::rate_limited("queue q is rate limited", 1_500);
        assert_eq!(e.code, ErrorCode::RateLimited);
        assert_eq!(e.code.as_str(), "rate_limited");
        assert_eq!(e.retry_after_ms(), Some(1_500));
        assert_eq!(e.grpc_code(), tonic::Code::ResourceExhausted);
        let res = e.clone().into_response();
        assert_eq!(res.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(res.headers()[axum::http::header::RETRY_AFTER], "2");
        let v = serde_json::to_value(e.to_body()).unwrap();
        assert_eq!(v["error"]["code"], "rate_limited");
        assert_eq!(v["error"]["details"]["retry_after_ms"], 1_500);
        // Exactly 1 s stays 1 s; other codes never carry the header.
        let res = ApiError::rate_limited("x", 1_000).into_response();
        assert_eq!(res.headers()[axum::http::header::RETRY_AFTER], "1");
        let res = ApiError::payload_too_large("x").into_response();
        assert!(res.headers().get(axum::http::header::RETRY_AFTER).is_none());
        // 13-12: a routing 503 with `retry_after_ms` carries the header; a plain 503 does not.
        let res = ApiError::unavailable("x")
            .with_details(
                serde_json::json!({ "reason": "partition_offline", RETRY_AFTER_MS: 1_000 }),
            )
            .into_response();
        assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(res.headers()[axum::http::header::RETRY_AFTER], "1");
        let res = ApiError::unavailable("x").into_response();
        assert!(res.headers().get(axum::http::header::RETRY_AFTER).is_none());
    }

    #[test]
    fn auth_codes_map_to_401_and_403() {
        let e = ApiError::unauthenticated("missing bearer");
        assert_eq!(e.code.as_str(), "unauthenticated");
        assert_eq!(e.reason(), Some(reason::UNAUTHENTICATED));
        assert_eq!(e.grpc_code(), tonic::Code::Unauthenticated);
        let res = e.into_response();
        assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            res.headers()[axum::http::header::WWW_AUTHENTICATE],
            "Bearer"
        );

        let e = ApiError::missing_scope("cancel_run", "operate");
        assert_eq!(e.code, ErrorCode::PermissionDenied);
        assert_eq!(e.reason(), Some(reason::MISSING_SCOPE));
        assert_eq!(e.details.as_ref().unwrap()["scope"], "operate");
        assert_eq!(e.grpc_code(), tonic::Code::PermissionDenied);
        let res = e.into_response();
        assert_eq!(res.status(), StatusCode::FORBIDDEN);
        assert!(
            res.headers()
                .get(axum::http::header::WWW_AUTHENTICATE)
                .is_none()
        );

        let e = ApiError::permission_denied(reason::FORBIDDEN_TENANT, "tenant field names acme");
        assert_eq!(e.http_status().as_u16(), 403);
        assert_eq!(e.reason(), Some("forbidden_tenant"));
        assert_eq!(
            ErrorCode::from_grpc_code(tonic::Code::Unauthenticated),
            ErrorCode::Unauthenticated
        );
        assert_eq!(
            ErrorCode::from_http_status(StatusCode::FORBIDDEN),
            ErrorCode::PermissionDenied
        );
    }
}
