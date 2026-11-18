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
}

impl ErrorCode {
    /// Every code, in declaration order.
    pub const ALL: [ErrorCode; 7] = [
        ErrorCode::InvalidArgument,
        ErrorCode::NotFound,
        ErrorCode::Conflict,
        ErrorCode::PreconditionFailed,
        ErrorCode::PayloadTooLarge,
        ErrorCode::Unavailable,
        ErrorCode::Internal,
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
        }
    }

    /// Inverse of [`ErrorCode::grpc_code`] (unknown codes -> Internal).
    pub fn from_grpc_code(code: tonic::Code) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|c| c.grpc_code() == code)
            .unwrap_or(ErrorCode::Internal)
    }

    /// Inverse of [`ErrorCode::http_status`] (unknown statuses -> Internal).
    pub fn from_http_status(status: StatusCode) -> ErrorCode {
        ErrorCode::ALL
            .into_iter()
            .find(|c| c.http_status() == status)
            .unwrap_or(ErrorCode::Internal)
    }
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
    /// payload_too_large, unavailable, internal.
    pub code: String,
    /// Human-readable message.
    pub message: String,
    /// Structured details, when present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.http_status(), axum::Json(self.to_body())).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: [(ErrorCode, u16, tonic::Code); 7] = [
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
            assert_eq!(ErrorCode::from_grpc_code(grpc), code);
        }
        assert_eq!(
            ErrorCode::from_grpc_code(tonic::Code::DataLoss),
            ErrorCode::Internal
        );
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
}
