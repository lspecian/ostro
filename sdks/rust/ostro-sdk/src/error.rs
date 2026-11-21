//! Step and workflow errors, and their mapping to the WIT `failure` variant.
//!
//! Two kinds are reserved: [`TIMEOUT`] and [`CANCELLED`]. A host `failure::timeout`
//! becomes a non-retryable `StepError { kind: "timeout" }` and a host `failure::cancelled`
//! becomes a non-retryable `StepError { kind: "cancelled" }`; converting such an error back
//! to a WIT failure yields `timeout` / `cancelled` again.
//!
//! Continue-as-new is also returned through `Err`: [`WorkflowError::continue_as_new`] (or
//! [`Ctx::continue_as_new`](crate::Ctx::continue_as_new)) builds an error that carries a
//! [`ContinueAsNew`] request, and the SDK runtime turns it into the WIT
//! `run-result::continue-as-new` instead of a failure. Returning it through `Err` keeps the
//! [`Workflow`](crate::Workflow) trait unchanged and lets `?` short-circuit out of nested
//! helpers.

use std::fmt;

use serde::Serialize;

use crate::bindings::exports::ostro::runtime::workflows::Continuation;
use crate::bindings::ostro::runtime::types::{ErrorInfo, Failure};
use crate::step::{JSON, encode_value};

/// Reserved error kind for host timeouts (`failure::timeout`).
pub const TIMEOUT: &str = "timeout";
/// Reserved error kind for cancellations (`failure::cancelled`).
pub const CANCELLED: &str = "cancelled";
/// Error kind used when a JSON payload cannot be encoded or decoded.
pub const INVALID_JSON: &str = "invalid_json";
/// Kind of the [`WorkflowError`] that carries a continue-as-new request. It never reaches
/// the host as a failure: the SDK maps it to `run-result::continue-as-new`.
pub const CONTINUE_AS_NEW: &str = "continue_as_new";

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
        failure_of(&self.kind, &self.message, self.retryable, None)
    }
}

fn failure_of(kind: &str, message: &str, retryable: bool, details: Option<&[u8]>) -> Failure {
    if !retryable && kind == TIMEOUT {
        return Failure::Timeout;
    }
    if !retryable && kind == CANCELLED {
        return Failure::Cancelled;
    }
    let info = ErrorInfo {
        kind: kind.to_string(),
        message: message.to_string(),
        details: details.map(|d| crate::bindings::ostro::runtime::types::Payload {
            content_type: JSON.to_string(),
            data: d.to_vec(),
        }),
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

/// A continue-as-new request: end this run and start a fresh one with `input`.
///
/// By default the successor runs the same workflow at the version the server resolves for
/// it; [`workflow`](Self::workflow) and [`version`](Self::version) override that. The
/// workflow name is unqualified and resolves inside the run's tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContinueAsNew {
    input: Vec<u8>,
    workflow: Option<String>,
    version: Option<u32>,
}

impl ContinueAsNew {
    /// A request that continues with `input`, JSON-encoded.
    pub fn new<T: Serialize + ?Sized>(input: &T) -> Result<Self, WorkflowError> {
        let p = encode_value(input).map_err(WorkflowError::from)?;
        Ok(Self {
            input: p.data,
            workflow: None,
            version: None,
        })
    }

    /// Continue as a different workflow (unqualified name, same tenant).
    pub fn workflow(mut self, name: impl Into<String>) -> Self {
        self.workflow = Some(name.into());
        self
    }

    /// Pin the successor's workflow version.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// The JSON-encoded successor input.
    pub fn input_json(&self) -> &[u8] {
        &self.input
    }

    pub(crate) fn to_continuation(&self) -> Continuation {
        Continuation {
            input: crate::bindings::ostro::runtime::types::Payload {
                content_type: JSON.to_string(),
                data: self.input.clone(),
            },
            workflow: self.workflow.clone(),
            version: self.version,
        }
    }
}

/// The failure of a workflow run. Same shape as [`StepError`]; a step error propagates
/// with `?`.
///
/// It can also carry a [`ContinueAsNew`] request (see [`WorkflowError::continue_as_new`]);
/// such an error ends the run as continued-as-new, not as failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowError {
    /// Machine-readable error kind.
    pub kind: String,
    /// Human-readable message.
    pub message: String,
    /// Whether the host may retry the run under its run retry policy.
    pub retryable: bool,
    continuation: Option<Box<ContinueAsNew>>,
    /// JSON-encoded `error-info.details` (see [`WorkflowError::with_details`]).
    details: Option<Vec<u8>>,
}

impl WorkflowError {
    /// A retryable workflow error.
    pub fn retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            retryable: true,
            continuation: None,
            details: None,
        }
    }

    /// A non-retryable workflow error.
    pub fn non_retryable(kind: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            message: message.into(),
            retryable: false,
            continuation: None,
            details: None,
        }
    }

    /// End the run with continue-as-new, starting the same workflow with `input`:
    /// `return Err(WorkflowError::continue_as_new(&next))`.
    ///
    /// If `input` cannot be JSON-encoded, this is a non-retryable `invalid_json` error
    /// instead (the run fails). Use [`ContinueAsNew`] to change the workflow or version.
    pub fn continue_as_new<T: Serialize + ?Sized>(input: &T) -> Self {
        match ContinueAsNew::new(input) {
            Ok(c) => c.into(),
            Err(e) => e,
        }
    }

    /// Attach JSON `details` (WIT `error-info.details`, `application/json`), replacing any
    /// earlier details. They reach the host with a retryable or non-retryable failure; the
    /// reserved `timeout` / `cancelled` failures carry no details. If `details` cannot be
    /// JSON-encoded the error is returned unchanged.
    pub fn with_details<T: Serialize + ?Sized>(mut self, details: &T) -> Self {
        if let Ok(d) = serde_json::to_vec(details) {
            self.details = Some(d);
        }
        self
    }

    /// The JSON details attached with [`with_details`](Self::with_details), if any.
    pub fn details(&self) -> Option<serde_json::Value> {
        self.details
            .as_deref()
            .and_then(|d| serde_json::from_slice(d).ok())
    }

    /// The continue-as-new request this error carries, if any.
    pub fn continuation(&self) -> Option<&ContinueAsNew> {
        self.continuation.as_deref()
    }

    /// True if this error carries a continue-as-new request.
    pub fn is_continue_as_new(&self) -> bool {
        self.continuation.is_some()
    }

    /// Convert to the WIT `failure` variant.
    pub(crate) fn to_failure(&self) -> Failure {
        failure_of(
            &self.kind,
            &self.message,
            self.retryable,
            self.details.as_deref(),
        )
    }
}

impl From<ContinueAsNew> for WorkflowError {
    fn from(c: ContinueAsNew) -> Self {
        Self {
            kind: CONTINUE_AS_NEW.to_string(),
            message: "continue-as-new".to_string(),
            retryable: false,
            continuation: Some(Box::new(c)),
            details: None,
        }
    }
}

impl From<StepError> for WorkflowError {
    fn from(e: StepError) -> Self {
        Self {
            kind: e.kind,
            message: e.message,
            retryable: e.retryable,
            continuation: None,
            details: None,
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
    fn continue_as_new_carries_the_request() {
        let e = WorkflowError::continue_as_new(&serde_json::json!({"n": 2}));
        assert!(e.is_continue_as_new());
        assert_eq!(e.kind, CONTINUE_AS_NEW);
        let c = e.continuation().expect("continuation").to_continuation();
        assert_eq!(c.input.content_type, JSON);
        assert_eq!(c.input.data, br#"{"n":2}"#);
        assert_eq!((c.workflow, c.version), (None, None));

        let e: WorkflowError = ContinueAsNew::new(&7u32)
            .expect("encode")
            .workflow("other")
            .version(3)
            .into();
        let c = e.continuation().expect("continuation").to_continuation();
        assert_eq!(c.input.data, b"7");
        assert_eq!((c.workflow.as_deref(), c.version), (Some("other"), Some(3)));

        // Unencodable input fails the run instead (non-string map keys).
        let bad: std::collections::BTreeMap<Vec<u8>, u8> = [(vec![1], 1)].into();
        let e = WorkflowError::continue_as_new(&bad);
        assert!(!e.is_continue_as_new());
        assert_eq!(e.kind, INVALID_JSON);
        assert!(!WorkflowError::non_retryable("k", "m").is_continue_as_new());
    }

    #[test]
    fn workflow_error_details_reach_the_failure() {
        let e = WorkflowError::non_retryable("k", "m").with_details(&serde_json::json!({"a": 1}));
        assert_eq!(e.details(), Some(serde_json::json!({"a": 1})));
        match e.to_failure() {
            Failure::NonRetryable(i) => {
                let d = i.details.expect("details");
                assert_eq!(d.content_type, JSON);
                assert_eq!(d.data, br#"{"a":1}"#);
            }
            other => panic!("unexpected {other:?}"),
        }
        let c = WorkflowError::from(StepError::cancelled()).with_details(&1);
        assert!(matches!(c.to_failure(), Failure::Cancelled));
        assert_eq!(WorkflowError::retryable("k", "m").details(), None);
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
