//! Step and workflow errors, and their mapping to the WIT `failure` variant.
//!
//! Two kinds are reserved: [`TIMEOUT`] and [`CANCELLED`]. A host `failure::timeout`
//! becomes a non-retryable `StepError { kind: "timeout" }` and a host `failure::cancelled`
//! becomes a non-retryable `StepError { kind: "cancelled" }`; converting such an error back
//! to a WIT failure yields `timeout` / `cancelled` again.

use std::fmt;

use crate::bindings::ostro::runtime::types::{ErrorInfo, Failure};

/// Reserved error kind for host timeouts (`failure::timeout`).
pub const TIMEOUT: &str = "timeout";
/// Reserved error kind for cancellations (`failure::cancelled`).
pub const CANCELLED: &str = "cancelled";
/// Error kind used when a JSON payload cannot be encoded or decoded.
pub const INVALID_JSON: &str = "invalid_json";

/// The failure of a step body or durable operation.
///
/// `kind` is matched against the step retry policy's non-retryable kinds by the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepError {
    /// Machine-readable error kind (e.g. `"http_5xx"`).
    pub kind: String,
    /// Human-readable message.
    pub message: String,
    /// Whether the host may retry the step under its retry policy.
    pub retryable: bool,
}

impl StepError {
    /// A retryable error.
    pub fn retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            retryable: true,
        }
    }

    /// A non-retryable error: fails the step immediately.
    pub fn non_retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            retryable: false,
        }
    }

    /// The error a host `failure::timeout` maps to.
    pub fn timeout() -> Self {
        Self::non_retryable(TIMEOUT, "timed out")
    }

    /// The error a host `failure::cancelled` maps to.
    pub fn cancelled() -> Self {
        Self::non_retryable(CANCELLED, "cancelled")
    }

    /// True if this is the reserved timeout error.
    pub fn is_timeout(&self) -> bool {
        !self.retryable && self.kind == TIMEOUT
    }

    /// True if this is the reserved cancellation error.
    pub fn is_cancelled(&self) -> bool {
        !self.retryable && self.kind == CANCELLED
    }

    pub(crate) fn invalid_json(err: impl fmt::Display) -> Self {
        Self::non_retryable(INVALID_JSON, err.to_string())
    }

    /// Convert to the WIT `failure` variant.
    pub(crate) fn to_failure(&self) -> Failure {
        failure_of(&self.kind, &self.message, self.retryable)
    }
}

fn failure_of(kind: &str, message: &str, retryable: bool) -> Failure {
    if !retryable && kind == TIMEOUT {
        return Failure::Timeout;
    }
    if !retryable && kind == CANCELLED {
        return Failure::Cancelled;
    }
    let info = ErrorInfo {
        kind: kind.to_string(),
        message: message.to_string(),
        details: None,
    };
    if retryable {
        Failure::Retryable(info)
    } else {
        Failure::NonRetryable(info)
    }
}

impl From<Failure> for StepError {
    fn from(f: Failure) -> Self {
        match f {
            Failure::Retryable(i) => Self::retryable(i.kind, i.message),
            Failure::NonRetryable(i) => Self::non_retryable(i.kind, i.message),
            Failure::Timeout => Self::timeout(),
            Failure::Cancelled => Self::cancelled(),
        }
    }
}

impl From<StepError> for Failure {
    fn from(e: StepError) -> Self {
        e.to_failure()
    }
}

impl From<serde_json::Error> for StepError {
    fn from(e: serde_json::Error) -> Self {
        Self::invalid_json(e)
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = if self.retryable {
            "retryable"
        } else {
            "non-retryable"
        };
        write!(f, "{} ({r}): {}", self.kind, self.message)
    }
}

impl std::error::Error for StepError {}

/// The failure of a workflow run. Same shape as [`StepError`]; a step error propagates
/// with `?`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowError {
    /// Machine-readable error kind.
    pub kind: String,
    /// Human-readable message.
    pub message: String,
    /// Whether the host may retry the run under its run retry policy.
    pub retryable: bool,
}

impl WorkflowError {
    /// A retryable workflow error.
    pub fn retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            retryable: true,
        }
    }

    /// A non-retryable workflow error.
    pub fn non_retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            retryable: false,
        }
    }

    /// Convert to the WIT `failure` variant.
    pub(crate) fn to_failure(&self) -> Failure {
        failure_of(&self.kind, &self.message, self.retryable)
    }
}

impl From<StepError> for WorkflowError {
    fn from(e: StepError) -> Self {
        Self {
            kind: e.kind,
            message: e.message,
            retryable: e.retryable,
        }
    }
}

impl From<serde_json::Error> for WorkflowError {
    fn from(e: serde_json::Error) -> Self {
        Self::non_retryable(INVALID_JSON, e.to_string())
    }
}

impl From<WorkflowError> for Failure {
    fn from(e: WorkflowError) -> Self {
        e.to_failure()
    }
}

impl fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let r = if self.retryable {
            "retryable"
        } else {
            "non-retryable"
        };
        write!(f, "{} ({r}): {}", self.kind, self.message)
    }
}

impl std::error::Error for WorkflowError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_roundtrip() {
        let cases = [
            StepError::retryable("http_5xx", "bad gateway"),
            StepError::non_retryable("bad_input", "nope"),
            StepError::timeout(),
            StepError::cancelled(),
        ];
        for e in cases {
            let f: Failure = e.clone().into();
            assert_eq!(StepError::from(f), e);
        }
    }

    #[test]
    fn reserved_kinds_map_to_unit_variants() {
        assert!(matches!(
            StepError::timeout().to_failure(),
            Failure::Timeout
        ));
        assert!(matches!(
            StepError::cancelled().to_failure(),
            Failure::Cancelled
        ));
        // A *retryable* "timeout" kind is an ordinary error, not the reserved variant.
        assert!(matches!(
            StepError::retryable(TIMEOUT, "x").to_failure(),
            Failure::Retryable(_)
        ));
        assert!(StepError::from(Failure::Timeout).is_timeout());
        assert!(StepError::from(Failure::Cancelled).is_cancelled());
    }

    #[test]
    fn workflow_error_conversions() {
        let w: WorkflowError = StepError::retryable("k", "m").into();
        assert_eq!(w, WorkflowError::retryable("k", "m"));
        let j = serde_json::from_str::<u32>("\"x\"").unwrap_err();
        let w: WorkflowError = j.into();
        assert_eq!(w.kind, INVALID_JSON);
        assert!(!w.retryable);
        match w.to_failure() {
            Failure::NonRetryable(i) => assert_eq!(i.kind, INVALID_JSON),
            other => panic!("unexpected {other:?}"),
        }
    }
}
