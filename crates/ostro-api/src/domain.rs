//! Transport-neutral control-plane contract.
//!
//! REST (`rest.rs`) and gRPC (04-11) adapters translate their wire types into these
//! domain types and call [`ControlPlane`]; nothing else. The domain types carry no serde
//! or schema derives on purpose: wire DTOs live with their transport.

use crate::error::ApiError;

/// The one interface every API adapter calls.
#[async_trait::async_trait]
pub trait ControlPlane: Send + Sync + 'static {
    /// Store a module (content-addressed by `sha256`) and register its workflows.
    async fn upload_module(&self, req: UploadModule) -> Result<UploadModuleResult, ApiError>;
    /// Look up a stored module by lowercase hex sha256.
    async fn get_module(&self, sha256: String) -> Result<ModuleInfo, ApiError>;
    /// Every known workflow with its versions.
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError>;
    /// Make `req.version` the active version of `req.name`.
    async fn activate_workflow(&self, req: ActivateWorkflow) -> Result<WorkflowInfo, ApiError>;
    /// Start (or idempotently find) a run.
    async fn start_run(&self, req: StartRunRequest) -> Result<StartRunResult, ApiError>;
    /// One run by id.
    async fn get_run(&self, run_id: String) -> Result<RunInfo, ApiError>;
    /// A page of a run's journal history.
    async fn run_history(&self, req: HistoryRequest) -> Result<HistoryPage, ApiError>;
    /// A filtered page of runs.
    async fn list_runs(&self, req: ListRunsRequest) -> Result<RunPage, ApiError>;
    /// Meta has a leader and every partition slot is ready.
    fn ready(&self) -> bool;

    // --- Phase 5 lifecycle (API-05/06/07). Default bodies answer Unimplemented so a
    // control plane compiles before it implements them (ostro-server: 05-13).

    /// Deliver a named signal to a run.
    async fn signal_run(&self, r: SignalRequest) -> Result<SignalResponse, ApiError> {
        let _ = r;
        Err(ApiError::unimplemented("signal_run"))
    }
    /// Request graceful cancellation (idempotent).
    async fn cancel_run(&self, r: CancelRequest) -> Result<CancelResponse, ApiError> {
        let _ = r;
        Err(ApiError::unimplemented("cancel_run"))
    }
    /// Hard-stop a run.
    async fn terminate_run(&self, r: TerminateRequest) -> Result<TerminateResponse, ApiError> {
        let _ = r;
        Err(ApiError::unimplemented("terminate_run"))
    }
    /// Resume a resumable terminal run in place, or restart it as a new linked run.
    async fn retry_run(&self, r: RetryRequest) -> Result<RetryResponse, ApiError> {
        let _ = r;
        Err(ApiError::unimplemented("retry_run"))
    }
    /// Long-poll a run's outcome for up to `r.wait_ms` (adapters cap it at
    /// [`MAX_RESULT_WAIT_MS`]). Not-terminal-yet is `Ok` with `completed = false`.
    async fn await_result(&self, r: AwaitResultRequest) -> Result<AwaitResultResponse, ApiError> {
        let _ = r;
        Err(ApiError::unimplemented("await_result"))
    }
    /// Complete the await-token op a token names.
    async fn complete_token(
        &self,
        r: CompleteTokenRequest,
    ) -> Result<CompleteTokenResponse, ApiError> {
        let _ = r;
        Err(ApiError::unimplemented("complete_token"))
    }

    // --- OPS-06 / WF-09 run policies (D9).

    /// Replace a workflow's run policy; returns the stored policy.
    async fn set_workflow_policy(
        &self,
        workflow: String,
        p: RunPolicy,
    ) -> Result<RunPolicy, ApiError> {
        let _ = (workflow, p);
        Err(ApiError::unimplemented("set_workflow_policy"))
    }
    /// A workflow's run policy (all `None` when never set).
    async fn get_workflow_policy(&self, workflow: String) -> Result<RunPolicy, ApiError> {
        let _ = workflow;
        Err(ApiError::unimplemented("get_workflow_policy"))
    }
    /// Replace a queue's run policy; returns the stored policy.
    async fn set_queue_policy(&self, queue: String, p: RunPolicy) -> Result<RunPolicy, ApiError> {
        let _ = (queue, p);
        Err(ApiError::unimplemented("set_queue_policy"))
    }
    /// A queue's run policy (all `None` when never set).
    async fn get_queue_policy(&self, queue: String) -> Result<RunPolicy, ApiError> {
        let _ = queue;
        Err(ApiError::unimplemented("get_queue_policy"))
    }
}

/// Upload request: raw module bytes plus the client-claimed digest.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadModule {
    /// Lowercase hex sha256 the client claims for `wasm`.
    pub sha256: String,
    /// Module bytes.
    pub wasm: bytes::Bytes,
    /// Optional source language hint ("rust", "python", "typescript").
    pub language: Option<String>,
    /// Uploading tenant (D23); `None` or empty = the default tenant. `_` is reserved.
    pub tenant: Option<String>,
    /// Deployed module name (Phase 14 F4), stored tenant-qualified on every workflow
    /// version this upload registers.
    pub name: Option<String>,
}

/// Upload outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UploadModuleResult {
    /// The stored module.
    pub module: ModuleInfo,
    /// `false` when the module already existed (idempotent re-upload).
    pub created: bool,
}

/// A (workflow name, version) pair.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WorkflowRef {
    /// Workflow name.
    pub name: String,
    /// Workflow version.
    pub version: u32,
}

/// A stored module.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleInfo {
    /// Lowercase hex sha256.
    pub sha256: String,
    /// Module size in bytes.
    pub size_bytes: u64,
    /// Source language, when known.
    pub language: Option<String>,
    /// Workflows the module exports.
    pub workflows: Vec<WorkflowRef>,
    /// Upload time, ms since the Unix epoch.
    pub uploaded_at_ms: u64,
}

/// One registered version of a workflow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowVersionInfo {
    /// Version number.
    pub version: u32,
    /// Module implementing this version.
    pub module_sha256: String,
    /// Queue runs land on when the start request names none.
    pub default_queue: Option<String>,
    /// Default run timeout.
    pub run_timeout_ms: Option<u64>,
}

/// A workflow and its versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkflowInfo {
    /// Workflow name.
    pub name: String,
    /// Active version (used when a start request names none).
    pub active_version: Option<u32>,
    /// Registered versions, ascending.
    pub versions: Vec<WorkflowVersionInfo>,
    /// Meta record version (CAS token for activation).
    pub record_version: u64,
}

/// Activation request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivateWorkflow {
    /// Workflow name.
    pub name: String,
    /// Version to activate.
    pub version: u32,
    /// When set, the workflow's `record_version` must equal it (else PreconditionFailed).
    pub expected_version: Option<u64>,
}

/// What to do when an idempotency key already maps to a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Return the existing run with `created = false`.
    #[default]
    ReturnExisting,
    /// Fail with Conflict, details `{ "existing_run_id": ... }`.
    Reject,
}

/// Start-run request. Derives `Default` so literal sites can use `..Default::default()`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartRunRequest {
    /// Workflow name.
    pub workflow: String,
    /// Validated JSON text of the input.
    pub input_json: Vec<u8>,
    /// Explicit version (else the active one).
    pub version: Option<u32>,
    /// Queue override.
    pub queue: Option<String>,
    /// Delay before the run becomes ready.
    pub delay_ms: Option<u64>,
    /// Scheduling priority.
    pub priority: Option<i32>,
    /// Free-form tags.
    pub tags: Vec<String>,
    /// W3C traceparent.
    pub traceparent: Option<String>,
    /// Tenant.
    pub tenant: Option<String>,
    /// Idempotency key.
    pub idempotency_key: Option<String>,
    /// Behaviour on idempotency-key collision.
    pub conflict_policy: ConflictPolicy,
    /// Concurrency key.
    pub concurrency_key: Option<String>,
    /// Schedule-to-start timeout override, ms (D2/D9; `None` = resolve from the workflow
    /// policy, then the queue policy; `Some(0)` = none).
    pub schedule_to_start_ms: Option<u64>,
    /// Retention override for the terminal run, ms (D9; `None` or `0` = resolve from the
    /// workflow policy, the queue policy, then `[runs] retention`).
    pub retention_ms: Option<u64>,
    /// Validate only (W15): the same checks as a real start, the would-be idempotency
    /// outcome is reported, nothing is submitted.
    pub dry_run: bool,
}

/// Start-run outcome.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StartRunResult {
    /// Run id. For a dry run: the existing run an idempotency key maps to, else empty (no
    /// id is allocated).
    pub run_id: String,
    /// `false` when an existing run was returned (dry run: would be returned).
    pub created: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Run lifecycle status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RunStatus {
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
    /// Hard-stopped by an operator (Phase 5).
    Terminated,
    /// Run timeout or schedule-to-start timeout (Phase 5).
    TimedOut,
    /// Continued as a new run (Phase 5).
    ContinuedAsNew,
}

impl RunStatus {
    /// Every status, in declaration order.
    pub const ALL: [RunStatus; 9] = [
        RunStatus::Scheduled,
        RunStatus::Ready,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
        RunStatus::Terminated,
        RunStatus::TimedOut,
        RunStatus::ContinuedAsNew,
    ];

    /// Lowercase wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            RunStatus::Scheduled => "scheduled",
            RunStatus::Ready => "ready",
            RunStatus::Running => "running",
            RunStatus::Completed => "completed",
            RunStatus::Failed => "failed",
            RunStatus::Cancelled => "cancelled",
            RunStatus::Terminated => "terminated",
            RunStatus::TimedOut => "timed_out",
            RunStatus::ContinuedAsNew => "continued_as_new",
        }
    }

    /// Parse a lowercase wire name.
    pub fn parse(s: &str) -> Option<RunStatus> {
        RunStatus::ALL.into_iter().find(|st| st.as_str() == s)
    }

    /// Completed, Failed, Cancelled, Terminated, TimedOut or ContinuedAsNew.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Completed
                | RunStatus::Failed
                | RunStatus::Cancelled
                | RunStatus::Terminated
                | RunStatus::TimedOut
                | RunStatus::ContinuedAsNew
        )
    }
}

/// Full run view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunInfo {
    /// Run id.
    pub run_id: String,
    /// Workflow name.
    pub workflow: String,
    /// Workflow version.
    pub version: u32,
    /// Module implementing the version.
    pub module_sha256: String,
    /// Queue.
    pub queue: String,
    /// Status.
    pub status: RunStatus,
    /// Current attempt, when running.
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
    /// Creation time, ms since the Unix epoch.
    pub created_at_ms: u64,
    /// Completion time, ms since the Unix epoch.
    pub completed_at_ms: Option<u64>,
    /// Latest published status (JSON text).
    pub published_status_json: Option<Vec<u8>>,
    /// Result (JSON text) of a completed run.
    pub result_json: Option<Vec<u8>>,
    /// Failure message of a failed run.
    pub failure: Option<String>,
    /// Durable count of retryable attempt failures (F9): the meaningful retry count.
    /// `attempt` counts dispatches, including resumes.
    pub attempt_failures: u32,
    /// Resolved retention of the terminal run, ms (`None` = keep forever).
    pub retention_ms: Option<u64>,
    /// Resolved schedule-to-start timeout, ms (`None` = none).
    pub schedule_to_start_ms: Option<u64>,
}

/// History page request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryRequest {
    /// Run id.
    pub run_id: String,
    /// First journal seq to return (inclusive).
    pub from_seq: u64,
    /// Maximum events (0 = implementation default).
    pub limit: u32,
}

/// One rendered journal event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryEvent {
    /// Journal seq.
    pub seq: u64,
    /// Event kind name.
    pub kind: String,
    /// Event time, ms since the Unix epoch.
    pub ts_ms: u64,
    /// One-line human summary.
    pub summary: String,
    /// Event details (JSON text).
    pub details_json: Vec<u8>,
}

/// A page of history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HistoryPage {
    /// Events, ascending seq.
    pub events: Vec<HistoryEvent>,
    /// Seq to pass as `from_seq` for the next page, if any.
    pub next_from_seq: Option<u64>,
}

/// Run listing filter.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ListRunsRequest {
    /// Workflow name filter.
    pub workflow: Option<String>,
    /// Status filter.
    pub status: Option<RunStatus>,
    /// Queue filter.
    pub queue: Option<String>,
    /// Tag filter (run must carry it).
    pub tag: Option<String>,
    /// Only runs created at or after this time (ms).
    pub created_after_ms: Option<u64>,
    /// Only runs created before this time (ms).
    pub created_before_ms: Option<u64>,
    /// Page size (0 = implementation default).
    pub limit: u32,
    /// Opaque token from a previous page.
    pub page_token: Option<String>,
}

/// Run listing row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSummary {
    /// Run id.
    pub run_id: String,
    /// Workflow name.
    pub workflow: String,
    /// Workflow version.
    pub version: u32,
    /// Status.
    pub status: RunStatus,
    /// Queue.
    pub queue: String,
    /// Creation time, ms since the Unix epoch.
    pub created_at_ms: u64,
    /// Tags.
    pub tags: Vec<String>,
}

/// A page of runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunPage {
    /// Runs.
    pub runs: Vec<RunSummary>,
    /// Token for the next page, if any.
    pub next_page_token: Option<String>,
}

// ---------------------------------------------------------------------------------
// Phase 5 lifecycle (API-05/06/07) and run policies (OPS-06/WF-09)
// ---------------------------------------------------------------------------------

/// Signal request. `payload` travels as JSON; the transport body limit bounds it.
#[derive(Clone, Debug, PartialEq)]
pub struct SignalRequest {
    /// Run id.
    pub run_id: String,
    /// Signal name (1..=[`MAX_SIGNAL_NAME_LEN`] bytes).
    pub name: String,
    /// Signal payload.
    pub payload: serde_json::Value,
    /// Optional dedupe id (<= [`MAX_REQUEST_ID_LEN`] bytes).
    pub request_id: Option<String>,
    /// Validate only; nothing is submitted.
    pub dry_run: bool,
}

/// Signal outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignalResponse {
    /// The signal was (or, for dry_run, would be) accepted.
    pub accepted: bool,
    /// `request_id` had already been seen for this run; nothing new was delivered.
    pub duplicate: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Graceful-cancel request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelRequest {
    /// Run id.
    pub run_id: String,
    /// Optional reason (<= [`MAX_REASON_LEN`] bytes).
    pub reason: Option<String>,
    /// Validate only.
    pub dry_run: bool,
}

/// Graceful-cancel outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelResponse {
    /// Cancellation had already been requested.
    pub already_requested: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Terminate request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminateRequest {
    /// Run id.
    pub run_id: String,
    /// Optional reason (<= [`MAX_REASON_LEN`] bytes).
    pub reason: Option<String>,
    /// Validate only.
    pub dry_run: bool,
}

/// Terminate outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminateResponse {
    /// The run's status afterwards ([`RunStatus::Terminated`]).
    pub status: RunStatus,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Retry request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryRequest {
    /// Run id.
    pub run_id: String,
    /// Validate only (reports the mode the retry would use).
    pub dry_run: bool,
}

/// How a retry is carried out (D1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RetryMode {
    /// The same run resumes from its journal (resumable terminal runs).
    Resumed,
    /// A new run linked to the old one is started (guest-terminal runs).
    Restarted,
}

impl RetryMode {
    /// Lowercase wire name.
    pub fn as_str(self) -> &'static str {
        match self {
            RetryMode::Resumed => "resumed",
            RetryMode::Restarted => "restarted",
        }
    }
}

/// Retry outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetryResponse {
    /// Resume or restart.
    pub mode: RetryMode,
    /// The resumed run, or the new run for [`RetryMode::Restarted`] (for dry_run, the
    /// original run id: no new id is allocated).
    pub run_id: String,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Long-poll request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AwaitResultRequest {
    /// Run id.
    pub run_id: String,
    /// Maximum wait in ms (0 = answer now); see [`effective_wait_ms`].
    pub wait_ms: u64,
    /// Follow ContinuedAsNew successors to the last run of the chain.
    pub follow_continuations: bool,
}

/// Long-poll outcome (always `Ok` for a known run, terminal or not).
#[derive(Clone, Debug, PartialEq)]
pub struct AwaitResultResponse {
    /// The run answered for (the last successor when following continuations).
    pub run_id: String,
    /// Current status.
    pub status: RunStatus,
    /// `status.is_terminal()`.
    pub completed: bool,
    /// Result of a completed run.
    pub result: Option<serde_json::Value>,
    /// Failure of a failed run.
    pub failure: Option<serde_json::Value>,
}

/// Await-token completion request. The token travels only in request bodies.
#[derive(Clone, PartialEq)]
pub struct CompleteTokenRequest {
    /// The `ot1_...` token.
    pub token: String,
    /// Completion payload.
    pub payload: serde_json::Value,
    /// Validate only (format, MAC, op still open).
    pub dry_run: bool,
}

impl std::fmt::Debug for CompleteTokenRequest {
    /// Redacts the token so it never reaches logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompleteTokenRequest")
            .field("token", &"<redacted>")
            .field("payload", &self.payload)
            .field("dry_run", &self.dry_run)
            .finish()
    }
}

/// Await-token completion outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteTokenResponse {
    /// The run the token belongs to.
    pub run_id: String,
    /// The completion was (or would be) accepted.
    pub accepted: bool,
    /// Echo of the request flag.
    pub dry_run: bool,
}

/// Per-workflow or per-queue run defaults (D9). `None` = no override at this level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RunPolicy {
    /// Retention of terminal runs, ms.
    pub retention_ms: Option<u64>,
    /// Schedule-to-start timeout, ms.
    pub schedule_to_start_ms: Option<u64>,
}

/// Prefix of every await token (`ot1_` + base32 body).
pub const TOKEN_PREFIX: &str = "ot1_";
/// Upper bound on a long-poll wait; longer requests are capped, not rejected.
pub const MAX_RESULT_WAIT_MS: u64 = 60_000;
/// Mirrors `ostro_types::command::MAX_SIGNAL_NAME_LEN`.
pub const MAX_SIGNAL_NAME_LEN: usize = 256;
/// Mirrors `ostro_types::command::MAX_REASON_LEN`.
pub const MAX_REASON_LEN: usize = 1024;
/// Mirrors `ostro_types::command::MAX_REQUEST_ID_LEN`.
pub const MAX_REQUEST_ID_LEN: usize = 64;

/// Cap a requested long-poll wait at [`MAX_RESULT_WAIT_MS`].
pub fn effective_wait_ms(wait_ms: u64) -> u64 {
    wait_ms.min(MAX_RESULT_WAIT_MS)
}

fn check_len(field: &str, value: &str, max: usize) -> Result<(), ApiError> {
    if value.len() > max {
        return Err(ApiError::invalid_argument(format!(
            "{field}: {} bytes exceeds the limit of {max}",
            value.len()
        ))
        .with_details(serde_json::json!({ "field": field, "max": max })));
    }
    Ok(())
}

impl SignalRequest {
    /// Shape checks every control plane applies before looking at the run: non-empty
    /// name within [`MAX_SIGNAL_NAME_LEN`], request id within [`MAX_REQUEST_ID_LEN`].
    pub fn validate(&self) -> Result<(), ApiError> {
        if self.name.is_empty() {
            return Err(ApiError::invalid_argument("signal name must not be empty")
                .with_details(serde_json::json!({ "field": "name" })));
        }
        check_len("name", &self.name, MAX_SIGNAL_NAME_LEN)?;
        if let Some(id) = &self.request_id {
            check_len("request_id", id, MAX_REQUEST_ID_LEN)?;
        }
        Ok(())
    }
}

impl CancelRequest {
    /// Reason within [`MAX_REASON_LEN`].
    pub fn validate(&self) -> Result<(), ApiError> {
        self.reason
            .as_deref()
            .map_or(Ok(()), |r| check_len("reason", r, MAX_REASON_LEN))
    }
}

impl TerminateRequest {
    /// Reason within [`MAX_REASON_LEN`].
    pub fn validate(&self) -> Result<(), ApiError> {
        self.reason
            .as_deref()
            .map_or(Ok(()), |r| check_len("reason", r, MAX_REASON_LEN))
    }
}

/// Default page size for list/history calls with `limit = 0`.
pub const DEFAULT_PAGE_LIMIT: u32 = 100;
/// Upper bound on any page size.
pub const MAX_PAGE_LIMIT: u32 = 1000;

/// Normalise a requested page size: 0 -> default, capped at [`MAX_PAGE_LIMIT`].
pub fn effective_limit(limit: u32) -> u32 {
    match limit {
        0 => DEFAULT_PAGE_LIMIT,
        n => n.min(MAX_PAGE_LIMIT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn run_status_names_round_trip() {
        for st in RunStatus::ALL {
            assert_eq!(RunStatus::parse(st.as_str()), Some(st));
        }
        assert_eq!(RunStatus::parse("Running"), None);
    }

    #[test]
    fn lifecycle_validation() {
        let sig = SignalRequest {
            run_id: "r".into(),
            name: "go".into(),
            payload: serde_json::Value::Null,
            request_id: Some("x".repeat(MAX_REQUEST_ID_LEN)),
            dry_run: false,
        };
        sig.validate().expect("ok at the limits");
        let mut bad = sig.clone();
        bad.name = String::new();
        assert_eq!(bad.validate().unwrap_err().code, ErrorCode::InvalidArgument);
        bad.name = "n".repeat(MAX_SIGNAL_NAME_LEN + 1);
        assert_eq!(bad.validate().unwrap_err().code, ErrorCode::InvalidArgument);
        let mut bad = sig;
        bad.request_id = Some("x".repeat(MAX_REQUEST_ID_LEN + 1));
        let e = bad.validate().unwrap_err();
        assert_eq!(e.details.unwrap()["field"], "request_id");

        let c = CancelRequest {
            run_id: "r".into(),
            reason: Some("r".repeat(MAX_REASON_LEN + 1)),
            dry_run: false,
        };
        assert_eq!(c.validate().unwrap_err().code, ErrorCode::InvalidArgument);
        let t = TerminateRequest {
            run_id: "r".into(),
            reason: Some("r".repeat(MAX_REASON_LEN)),
            dry_run: false,
        };
        t.validate().expect("ok");
        assert_eq!(effective_wait_ms(5), 5);
        assert_eq!(effective_wait_ms(u64::MAX), MAX_RESULT_WAIT_MS);
        assert_eq!(RetryMode::Resumed.as_str(), "resumed");
        assert_eq!(RetryMode::Restarted.as_str(), "restarted");
    }

    #[test]
    fn token_request_debug_redacts() {
        let r = CompleteTokenRequest {
            token: "ot1_secretbody".into(),
            payload: serde_json::json!(1),
            dry_run: false,
        };
        let s = format!("{r:?}");
        assert!(!s.contains("secretbody"), "{s}");
        assert!(s.contains("<redacted>"));
    }

    struct Bare;

    #[async_trait::async_trait]
    impl ControlPlane for Bare {
        async fn upload_module(&self, _: UploadModule) -> Result<UploadModuleResult, ApiError> {
            unreachable!()
        }
        async fn get_module(&self, _: String) -> Result<ModuleInfo, ApiError> {
            unreachable!()
        }
        async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
            unreachable!()
        }
        async fn activate_workflow(&self, _: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
            unreachable!()
        }
        async fn start_run(&self, _: StartRunRequest) -> Result<StartRunResult, ApiError> {
            unreachable!()
        }
        async fn get_run(&self, _: String) -> Result<RunInfo, ApiError> {
            unreachable!()
        }
        async fn run_history(&self, _: HistoryRequest) -> Result<HistoryPage, ApiError> {
            unreachable!()
        }
        async fn list_runs(&self, _: ListRunsRequest) -> Result<RunPage, ApiError> {
            unreachable!()
        }
        fn ready(&self) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn lifecycle_defaults_are_unimplemented() {
        let cp = Bare;
        let unimpl = |e: ApiError, m: &str| {
            assert_eq!(e.code, ErrorCode::Unimplemented, "{m}");
            assert_eq!(e.details.unwrap()["method"], m);
        };
        let run_id = String::from("r");
        unimpl(
            cp.signal_run(SignalRequest {
                run_id: run_id.clone(),
                name: "n".into(),
                payload: serde_json::Value::Null,
                request_id: None,
                dry_run: false,
            })
            .await
            .unwrap_err(),
            "signal_run",
        );
        unimpl(
            cp.cancel_run(CancelRequest {
                run_id: run_id.clone(),
                reason: None,
                dry_run: false,
            })
            .await
            .unwrap_err(),
            "cancel_run",
        );
        unimpl(
            cp.terminate_run(TerminateRequest {
                run_id: run_id.clone(),
                reason: None,
                dry_run: false,
            })
            .await
            .unwrap_err(),
            "terminate_run",
        );
        unimpl(
            cp.retry_run(RetryRequest {
                run_id: run_id.clone(),
                dry_run: false,
            })
            .await
            .unwrap_err(),
            "retry_run",
        );
        unimpl(
            cp.await_result(AwaitResultRequest {
                run_id,
                wait_ms: 0,
                follow_continuations: false,
            })
            .await
            .unwrap_err(),
            "await_result",
        );
        unimpl(
            cp.complete_token(CompleteTokenRequest {
                token: "ot1_x".into(),
                payload: serde_json::Value::Null,
                dry_run: false,
            })
            .await
            .unwrap_err(),
            "complete_token",
        );
        let p = RunPolicy::default();
        unimpl(
            cp.set_workflow_policy("w".into(), p).await.unwrap_err(),
            "set_workflow_policy",
        );
        unimpl(
            cp.get_workflow_policy("w".into()).await.unwrap_err(),
            "get_workflow_policy",
        );
        unimpl(
            cp.set_queue_policy("q".into(), p).await.unwrap_err(),
            "set_queue_policy",
        );
        unimpl(
            cp.get_queue_policy("q".into()).await.unwrap_err(),
            "get_queue_policy",
        );
    }

    #[test]
    fn limits() {
        assert_eq!(effective_limit(0), DEFAULT_PAGE_LIMIT);
        assert_eq!(effective_limit(5), 5);
        assert_eq!(effective_limit(5000), MAX_PAGE_LIMIT);
    }
}
