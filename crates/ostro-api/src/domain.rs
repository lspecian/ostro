//! Transport-neutral control-plane contract.
//!
//! REST (`rest.rs`) and gRPC (04-11) adapters translate their wire types into these
//! domain types and call [`ControlPlane`]; nothing else. The domain types carry no serde
//! or schema derives on purpose: wire DTOs live with their transport.

use crate::error::ApiError;
use crate::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, DrainResult, JoinRequest, JoinResponse, P7_PENDING,
    PauseState, QueueFlow, QueueFlowInfo, RunPauseState, WorkerInfo, WorkerTokenCreated,
    WorkerTokenInfo,
};

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

    // --- Phase 6 bridge administration (D-FX9, P-16; 06-08). Default bodies answer
    // Unimplemented so a control plane compiles before it implements them
    // (ostro-server: 06-16). Implementations call the `validate` helpers below
    // ([`PutSecret::validate`], [`GrantInfo::validate`], [`ConnectionInfo::validate`],
    // [`EgressInfo::validate`]) and may validate more strictly.

    /// Seal and store a secret (create or replace). Secret values are write-only: no
    /// method ever returns one.
    async fn put_secret(&self, req: PutSecret) -> Result<SecretInfo, ApiError> {
        let _ = req;
        Err(ApiError::unimplemented("put_secret"))
    }
    /// Metadata of the stored secrets of `tenant` (`None` = global), sorted by name.
    async fn list_secrets(&self, tenant: Option<String>) -> Result<Vec<SecretInfo>, ApiError> {
        let _ = tenant;
        Err(ApiError::unimplemented("list_secrets"))
    }
    /// Delete a secret.
    async fn delete_secret(&self, name: String, tenant: Option<String>) -> Result<(), ApiError> {
        let _ = (name, tenant);
        Err(ApiError::unimplemented("delete_secret"))
    }
    /// Create or replace an origin-bound secret grant. The id is computed server-side
    /// ([`GrantInfo::expected_id`]); a client may leave it empty.
    async fn put_grant(&self, g: GrantInfo) -> Result<GrantInfo, ApiError> {
        let _ = g;
        Err(ApiError::unimplemented("put_grant"))
    }
    /// Grants, optionally only one module's, sorted by id.
    async fn list_grants(&self, module: Option<String>) -> Result<Vec<GrantInfo>, ApiError> {
        let _ = module;
        Err(ApiError::unimplemented("list_grants"))
    }
    /// Delete a grant by id.
    async fn delete_grant(&self, id: String) -> Result<(), ApiError> {
        let _ = id;
        Err(ApiError::unimplemented("delete_grant"))
    }
    /// Create or replace a connection.
    async fn put_connection(&self, c: ConnectionInfo) -> Result<ConnectionInfo, ApiError> {
        let _ = c;
        Err(ApiError::unimplemented("put_connection"))
    }
    /// One connection by name.
    async fn get_connection(&self, name: String) -> Result<ConnectionInfo, ApiError> {
        let _ = name;
        Err(ApiError::unimplemented("get_connection"))
    }
    /// Every connection, sorted by name.
    async fn list_connections(&self) -> Result<Vec<ConnectionInfo>, ApiError> {
        Err(ApiError::unimplemented("list_connections"))
    }
    /// Delete a connection.
    async fn delete_connection(&self, name: String) -> Result<(), ApiError> {
        let _ = name;
        Err(ApiError::unimplemented("delete_connection"))
    }
    /// Create or replace a module's egress policy.
    async fn put_egress(&self, e: EgressInfo) -> Result<EgressInfo, ApiError> {
        let _ = e;
        Err(ApiError::unimplemented("put_egress"))
    }
    /// One module's egress policy.
    async fn get_egress(&self, module: String) -> Result<EgressInfo, ApiError> {
        let _ = module;
        Err(ApiError::unimplemented("get_egress"))
    }
    /// Every egress policy, sorted by module.
    async fn list_egress(&self) -> Result<Vec<EgressInfo>, ApiError> {
        Err(ApiError::unimplemented("list_egress"))
    }
    /// Delete a module's egress policy.
    async fn delete_egress(&self, module: String) -> Result<(), ApiError> {
        let _ = module;
        Err(ApiError::unimplemented("delete_egress"))
    }
    /// Serve a locally presigned object URL (`GET|PUT /v1/objects/{connection}/{key}`).
    /// Reached without API authentication: the signature is the authorisation, so the
    /// implementation verifies `sig`/`exp` before touching the store. A bad or expired
    /// signature is `Ok` with `status = 403`.
    async fn presigned_object(
        &self,
        req: PresignedObjectRequest,
    ) -> Result<PresignedObjectResponse, ApiError> {
        let _ = req;
        Err(ApiError::unimplemented("presigned_object"))
    }

    // --- Phase 7 (07-11): queue flow, pause/resume, demand, workers, join tokens, join.
    // Default bodies answer Unavailable ([`P7_PENDING`], P-16) so a control plane
    // compiles before it implements them (ostro-server: 07-14). The argument and result
    // types are the [`crate::phase7`] wire types; implementations call
    // [`QueueFlow::validate`], [`crate::phase7::PauseRunRequest::validate`],
    // [`CreateWorkerTokenRequest::validate`] and [`JoinRequest::validate`].

    /// A queue's flow configuration (defaults and version 0 when never stored).
    async fn get_queue_flow(&self, queue: String) -> Result<QueueFlowInfo, ApiError> {
        let _ = queue;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Replace a queue's flow fields (the Phase 5 run-policy fields are untouched).
    /// `expected_version`: CAS on the record version; stale = PreconditionFailed.
    async fn put_queue_flow(
        &self,
        queue: String,
        flow: QueueFlow,
        expected_version: Option<u64>,
    ) -> Result<QueueFlowInfo, ApiError> {
        let _ = (queue, flow, expected_version);
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Pause a queue (idempotent).
    async fn pause_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        let _ = queue;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Resume a queue (idempotent).
    async fn resume_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        let _ = queue;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Pause a workflow's runs (idempotent).
    async fn pause_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        let _ = workflow;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Resume a workflow's runs (idempotent).
    async fn resume_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        let _ = workflow;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Pause one run (idempotent): unknown = NotFound, terminal = Conflict
    /// (`run_terminal`).
    async fn pause_run(
        &self,
        run_id: String,
        detail: Option<String>,
    ) -> Result<RunPauseState, ApiError> {
        let _ = (run_id, detail);
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Resume a paused run (idempotent).
    async fn resume_run(&self, run_id: String) -> Result<RunPauseState, ApiError> {
        let _ = run_id;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// A queue's worker demand (D-W12).
    async fn queue_demand(&self, queue: String) -> Result<DemandSnapshot, ApiError> {
        let _ = queue;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Connected workers, sorted by id.
    async fn list_workers(&self) -> Result<Vec<WorkerInfo>, ApiError> {
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Drain a worker (idempotent); unknown = NotFound.
    async fn drain_worker(&self, worker_id: String) -> Result<DrainResult, ApiError> {
        let _ = worker_id;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Create a worker join token; the secret is returned here only.
    async fn create_worker_token(
        &self,
        req: CreateWorkerTokenRequest,
    ) -> Result<WorkerTokenCreated, ApiError> {
        let _ = req;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Join tokens without secrets, sorted by id.
    async fn list_worker_tokens(&self) -> Result<Vec<WorkerTokenInfo>, ApiError> {
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Delete a join token; unknown = NotFound.
    async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError> {
        let _ = token_id;
        Err(ApiError::unavailable(P7_PENDING))
    }
    /// Exchange a join token and a CSR for a client certificate.
    async fn join_worker(&self, req: JoinRequest) -> Result<JoinResponse, ApiError> {
        let _ = req;
        Err(ApiError::unavailable(P7_PENDING))
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

// ---------------------------------------------------------------------------------
// Phase 6 bridge administration (D-FX9, P-16; 06-08)
//
// The validation helpers duplicate the minimal checks of ostro-bridge (secret-name
// grammar, origin patterns, connection kinds) on purpose: ostro-api does not depend on
// ostro-bridge. The server may validate more strictly (06-16).
// ---------------------------------------------------------------------------------

/// `details.reason` of a secret value shorter than [`MIN_SECRET_LEN`] (400).
pub const REASON_SECRET_TOO_SHORT: &str = "secret_too_short";
/// `details.reason` of a name under the reserved [`RESERVED_SECRET_PREFIX`] (400).
pub const REASON_SECRET_RESERVED: &str = "secret_reserved";
/// Shortest accepted secret value, bytes.
pub const MIN_SECRET_LEN: usize = 8;
/// Longest accepted secret name, bytes (template grammar).
pub const MAX_SECRET_NAME_LEN: usize = 128;
/// Secret names the API never accepts (server-internal sealed records).
pub const RESERVED_SECRET_PREFIX: &str = "__ostro/";
/// Grant id prefix (RESERVATIONS §11): `secret:<module>:<secret>`.
pub const GRANT_ID_PREFIX: &str = "secret:";
/// Egress allow-list prefix naming a connection (P-7).
pub const EGRESS_CONNECTION_PREFIX: &str = "connection:";
/// Connection kinds the bridge knows.
pub const CONNECTION_KINDS: [&str; 4] = ["sql-postgres", "sql-mysql", "object", "http-target"];
/// Longest accepted module or connection name, bytes.
pub const MAX_BRIDGE_NAME_LEN: usize = 256;

/// Store a secret. The value is write-only; `Debug` redacts it.
#[derive(Clone, PartialEq, Eq)]
pub struct PutSecret {
    /// Secret name (template grammar).
    pub name: String,
    /// The value (at least [`MIN_SECRET_LEN`] bytes).
    pub value: Vec<u8>,
    /// Tenant scope; `None` = global.
    pub tenant: Option<String>,
}

impl std::fmt::Debug for PutSecret {
    /// Redacts the value so it never reaches logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutSecret")
            .field("name", &self.name)
            .field("value", &"<redacted>")
            .field("tenant", &self.tenant)
            .finish()
    }
}

impl PutSecret {
    /// Name grammar ([`validate_secret_name`]) and minimum length. Errors never echo
    /// the value.
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_secret_name("name", &self.name)?;
        if self.value.len() < MIN_SECRET_LEN {
            return Err(ApiError::invalid_argument(format!(
                "secret value must be at least {MIN_SECRET_LEN} bytes"
            ))
            .with_details(serde_json::json!({
                "reason": REASON_SECRET_TOO_SHORT,
                "field": "value",
                "min": MIN_SECRET_LEN,
            })));
        }
        Ok(())
    }
}

/// Metadata of a stored secret. Never carries the value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecretInfo {
    /// Secret name.
    pub name: String,
    /// Id of the sealing key the value is sealed under.
    pub key_id: String,
    /// Tenant scope; `None` = global.
    pub tenant: Option<String>,
}

/// An origin-bound secret grant (`Grant{id: "secret:<module>:<secret>", ..}`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrantInfo {
    /// `secret:<module>:<secret>` (computed server-side; empty on input = omitted).
    pub id: String,
    /// Tenant-qualified deployed module name (`<tenant>/<name>`).
    pub module: String,
    /// Secret name.
    pub secret: String,
    /// Origin patterns the secret may be sent to (at least one).
    pub origins: Vec<String>,
    /// Placeholders allowed in the request body.
    pub body: bool,
}

impl GrantInfo {
    /// The id this grant is stored under.
    pub fn expected_id(&self) -> String {
        format!("{GRANT_ID_PREFIX}{}:{}", self.module, self.secret)
    }

    /// Module and secret names, at least one valid origin pattern, and (when given) an
    /// id equal to [`GrantInfo::expected_id`].
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_module_name("module", &self.module)?;
        validate_secret_name("secret", &self.secret)?;
        if self.origins.is_empty() {
            return Err(field_error("origins", "a grant needs at least one origin"));
        }
        for o in &self.origins {
            validate_origin_pattern("origins", o)?;
        }
        if !self.id.is_empty() && self.id != self.expected_id() {
            return Err(field_error(
                "id",
                format!("grant id must be {:?} (or empty)", self.expected_id()),
            ));
        }
        Ok(())
    }
}

/// A connection (SQL database, object store or HTTP target).
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectionInfo {
    /// Connection name (an origin for `http-target`).
    pub name: String,
    /// One of [`CONNECTION_KINDS`].
    pub kind: String,
    /// Kind-specific config: a JSON object with `"v": 1`.
    pub config: serde_json::Value,
}

impl ConnectionInfo {
    /// Name, a known kind, and a config object with `"v": 1`.
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_bridge_name("name", &self.name)?;
        if !CONNECTION_KINDS.contains(&self.kind.as_str()) {
            return Err(field_error(
                "kind",
                format!(
                    "unknown connection kind {:?} (expected one of {})",
                    self.kind,
                    CONNECTION_KINDS.join(", ")
                ),
            ));
        }
        let Some(obj) = self.config.as_object() else {
            return Err(field_error("config", "config must be a JSON object"));
        };
        if obj.get("v").and_then(serde_json::Value::as_u64) != Some(1) {
            return Err(field_error("config", "config must contain \"v\": 1"));
        }
        Ok(())
    }
}

/// A module's egress allow list.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EgressInfo {
    /// Tenant-qualified deployed module name.
    pub module: String,
    /// Origin patterns or `connection:<name>` entries.
    pub allow: Vec<String>,
}

impl EgressInfo {
    /// Module name and every allow entry ([`validate_egress_entry`]).
    pub fn validate(&self) -> Result<(), ApiError> {
        validate_module_name("module", &self.module)?;
        for e in &self.allow {
            validate_egress_entry("allow", e)?;
        }
        Ok(())
    }
}

/// Method of a presigned object request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PresignMethod {
    /// Download.
    Get,
    /// Upload.
    Put,
}

/// A request to a locally presigned object URL.
#[derive(Clone, PartialEq, Eq)]
pub struct PresignedObjectRequest {
    /// Object connection name.
    pub connection: String,
    /// Object key (may contain `/`).
    pub key: String,
    /// HTTP method.
    pub method: PresignMethod,
    /// Expiry, Unix seconds.
    pub exp: u64,
    /// Hex signature.
    pub sig: String,
    /// Upload body (`Put` only).
    pub body: Option<Vec<u8>>,
    /// Upload content type (`Put` only).
    pub content_type: Option<String>,
}

impl std::fmt::Debug for PresignedObjectRequest {
    /// Redacts the signature (a bearer capability) and elides the body.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PresignedObjectRequest")
            .field("connection", &self.connection)
            .field("key", &self.key)
            .field("method", &self.method)
            .field("exp", &self.exp)
            .field("sig", &"<redacted>")
            .field("body_len", &self.body.as_ref().map(Vec::len))
            .field("content_type", &self.content_type)
            .finish()
    }
}

/// Response of a presigned object request: any status (403 for a bad signature).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresignedObjectResponse {
    /// HTTP status.
    pub status: u16,
    /// Response body.
    pub body: Vec<u8>,
    /// Response content type.
    pub content_type: Option<String>,
}

fn field_error(field: &str, message: impl Into<String>) -> ApiError {
    let message = message.into();
    ApiError::invalid_argument(format!("{field}: {message}"))
        .with_details(serde_json::json!({ "field": field }))
}

/// Secret name grammar: `[a-z0-9][a-z0-9_.\-/]{0,127}`, no `..`, no empty or `.` path
/// segment, and not under [`RESERVED_SECRET_PREFIX`] (`details.reason =
/// "secret_reserved"`).
pub fn validate_secret_name(field: &str, name: &str) -> Result<(), ApiError> {
    if name.starts_with(RESERVED_SECRET_PREFIX) {
        return Err(ApiError::invalid_argument(format!(
            "{field}: names under {RESERVED_SECRET_PREFIX} are reserved"
        ))
        .with_details(serde_json::json!({ "field": field, "reason": REASON_SECRET_RESERVED })));
    }
    let b = name.as_bytes();
    let grammar = !b.is_empty()
        && b.len() <= MAX_SECRET_NAME_LEN
        && matches!(b[0], b'a'..=b'z' | b'0'..=b'9')
        && b.iter()
            .all(|c| matches!(c, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'/'));
    if !grammar {
        return Err(field_error(
            field,
            format!(
                "secret names are 1..={MAX_SECRET_NAME_LEN} bytes of [a-z0-9_.-/] starting with [a-z0-9]"
            ),
        ));
    }
    if name.contains("..") || name.split('/').any(|seg| seg.is_empty() || seg == ".") {
        return Err(field_error(field, "invalid path segment in secret name"));
    }
    Ok(())
}

/// Non-empty, at most [`MAX_BRIDGE_NAME_LEN`] bytes, no control characters.
fn validate_bridge_name(field: &str, name: &str) -> Result<(), ApiError> {
    if name.is_empty() {
        return Err(field_error(field, "must not be empty"));
    }
    check_len(field, name, MAX_BRIDGE_NAME_LEN)?;
    if name.chars().any(char::is_control) {
        return Err(field_error(field, "must not contain control characters"));
    }
    Ok(())
}

/// A tenant-qualified module name: a bridge name without `:`.
fn validate_module_name(field: &str, name: &str) -> Result<(), ApiError> {
    validate_bridge_name(field, name)?;
    if name.contains(':') {
        return Err(field_error(field, "module names must not contain ':'"));
    }
    Ok(())
}

fn is_label(l: &str) -> bool {
    !l.is_empty()
        && l.len() <= 63
        && l.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')
        && !l.starts_with('-')
        && !l.ends_with('-')
}

/// Origin pattern: `scheme://host[:port]` or `scheme://*.domain[:port]` (an optional
/// trailing `/` is accepted). The scheme is `http` or `https`; a bare `*`, a `*` other
/// than the whole first label, paths, queries, fragments and userinfo are rejected.
pub fn validate_origin_pattern(field: &str, pattern: &str) -> Result<(), ApiError> {
    let bad = |why: &str| field_error(field, format!("invalid origin pattern {pattern:?}: {why}"));
    let Some((scheme, rest)) = pattern.split_once("://") else {
        return Err(bad("missing scheme"));
    };
    if !matches!(scheme, "http" | "https") {
        return Err(bad("scheme must be http or https"));
    }
    let rest = rest.strip_suffix('/').unwrap_or(rest);
    if rest.is_empty() || rest.contains(['/', '?', '#', '@']) {
        return Err(bad("only scheme://host[:port] is allowed"));
    }
    // Split off the port; an IPv6 literal is bracketed.
    let (host, port) = if let Some(after) = rest.strip_prefix('[') {
        let (inside, tail) = after.split_once(']').ok_or_else(|| bad("unclosed '['"))?;
        if inside.is_empty() || !inside.bytes().all(|c| c.is_ascii_hexdigit() || c == b':') {
            return Err(bad("invalid IPv6 literal"));
        }
        let port = match tail {
            "" => None,
            t => Some(t.strip_prefix(':').ok_or_else(|| bad("junk after ']'"))?),
        };
        (None, port)
    } else {
        match rest.rsplit_once(':') {
            Some((h, p)) => (Some(h), Some(p)),
            None => (Some(rest), None),
        }
    };
    if let Some(p) = port
        && !matches!(p.parse::<u16>(), Ok(n) if n > 0)
    {
        return Err(bad("invalid port"));
    }
    let Some(host) = host else {
        return Ok(());
    };
    if host == "*" {
        return Err(bad("a bare `*` host is not allowed"));
    }
    let (wildcard, domain) = match host.strip_prefix("*.") {
        Some(d) => (true, d),
        None => (false, host),
    };
    if domain.contains('*') {
        return Err(bad("`*` is allowed only as the whole first label"));
    }
    let labels: Vec<&str> = domain.trim_end_matches('.').split('.').collect();
    if !labels.iter().all(|l| is_label(l)) {
        return Err(bad("invalid host"));
    }
    if wildcard && labels.len() < 2 {
        return Err(bad("a wildcard needs at least two labels after `*.`"));
    }
    Ok(())
}

/// An egress allow entry: an origin pattern or `connection:<name>`.
pub fn validate_egress_entry(field: &str, entry: &str) -> Result<(), ApiError> {
    match entry.strip_prefix(EGRESS_CONNECTION_PREFIX) {
        Some(name) => validate_bridge_name(field, name),
        None => validate_origin_pattern(field, entry),
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

#[cfg(test)]
mod bridge_tests {
    use super::*;
    use crate::error::ErrorCode;

    fn reason(e: &ApiError) -> Option<&str> {
        e.details.as_ref()?.get("reason")?.as_str()
    }

    #[test]
    fn secret_names() {
        for ok in [
            "a",
            "stripe/live-key",
            "db.pass_1",
            "0abc",
            &"a".repeat(128),
        ] {
            validate_secret_name("name", ok).unwrap_or_else(|e| panic!("{ok}: {e:?}"));
        }
        for bad in [
            "",
            "Upper",
            "_x",
            "-x",
            "/x",
            "a//b",
            "a/../b",
            "a..b",
            "a/./b",
            "a/",
            "sp ace",
            &"a".repeat(129),
        ] {
            let e = validate_secret_name("name", bad).expect_err(bad);
            assert_eq!(e.code, ErrorCode::InvalidArgument, "{bad}");
        }
        let e = validate_secret_name("name", "__ostro/token-key").expect_err("reserved");
        assert_eq!(reason(&e), Some(REASON_SECRET_RESERVED));
    }

    #[test]
    fn put_secret_value_is_redacted_and_checked() {
        let canary = "CANARY-value-0123456789";
        let p = PutSecret {
            name: "k".into(),
            value: canary.as_bytes().to_vec(),
            tenant: None,
        };
        assert!(!format!("{p:?}").contains(canary));
        p.validate().expect("ok");
        let short = PutSecret {
            value: b"1234567".to_vec(),
            ..p.clone()
        };
        let e = short.validate().expect_err("short");
        assert_eq!(e.code, ErrorCode::InvalidArgument);
        assert_eq!(reason(&e), Some(REASON_SECRET_TOO_SHORT));
        assert!(!e.message.contains("1234567"));
    }

    #[test]
    fn origin_patterns() {
        for ok in [
            "https://api.stripe.com",
            "https://api.stripe.com/",
            "http://localhost:8080",
            "https://*.example.com",
            "https://*.example.com:8443",
            "http://127.0.0.1:9000",
            "http://[::1]:8080",
            "https://[2001:db8::1]",
        ] {
            validate_origin_pattern("o", ok).unwrap_or_else(|e| panic!("{ok}: {e:?}"));
        }
        for bad in [
            "*",
            "https://*",
            "https://*:443",
            "https://*.com",
            "https://a.*.com",
            "api.stripe.com",
            "ftp://x.com",
            "https://x.com/path",
            "https://x.com?q",
            "https://u@x.com",
            "https://x.com:0",
            "https://x.com:99999",
            "https://",
            "https://-x.com",
            "https://[zz]",
        ] {
            assert!(validate_origin_pattern("o", bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn grants_connections_egress() {
        let g = GrantInfo {
            id: String::new(),
            module: "acme/billing".into(),
            secret: "stripe/live".into(),
            origins: vec!["https://api.stripe.com".into()],
            body: false,
        };
        g.validate().expect("ok");
        assert_eq!(g.expected_id(), "secret:acme/billing:stripe/live");
        GrantInfo {
            id: g.expected_id(),
            ..g.clone()
        }
        .validate()
        .expect("matching id");
        for bad in [
            GrantInfo {
                id: "secret:other:x".into(),
                ..g.clone()
            },
            GrantInfo {
                origins: vec![],
                ..g.clone()
            },
            GrantInfo {
                origins: vec!["*".into()],
                ..g.clone()
            },
            GrantInfo {
                module: "a:b".into(),
                ..g.clone()
            },
            GrantInfo {
                secret: "__ostro/presign/x".into(),
                ..g.clone()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }

        let c = ConnectionInfo {
            name: "https://api.example.com".into(),
            kind: "http-target".into(),
            config: serde_json::json!({ "v": 1 }),
        };
        c.validate().expect("ok");
        for kind in CONNECTION_KINDS {
            ConnectionInfo {
                kind: kind.into(),
                ..c.clone()
            }
            .validate()
            .expect(kind);
        }
        for bad in [
            ConnectionInfo {
                kind: "redis".into(),
                ..c.clone()
            },
            ConnectionInfo {
                config: serde_json::json!([1]),
                ..c.clone()
            },
            ConnectionInfo {
                config: serde_json::json!({ "v": 2 }),
                ..c.clone()
            },
            ConnectionInfo {
                name: String::new(),
                ..c.clone()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }

        let e = EgressInfo {
            module: "acme/billing".into(),
            allow: vec![
                "https://api.stripe.com".into(),
                "connection:db".into(),
                "https://*.example.com".into(),
            ],
        };
        e.validate().expect("ok");
        for bad in ["*", "connection:", "db", "https://x.com/p"] {
            let b = EgressInfo {
                allow: vec![bad.into()],
                ..e.clone()
            };
            assert!(b.validate().is_err(), "{bad}");
        }
    }

    #[test]
    fn presign_request_debug_redacts_signature() {
        let r = PresignedObjectRequest {
            connection: "files".into(),
            key: "a/b".into(),
            method: PresignMethod::Put,
            exp: 1,
            sig: "deadbeefcafe".into(),
            body: Some(b"hello".to_vec()),
            content_type: None,
        };
        let d = format!("{r:?}");
        assert!(!d.contains("deadbeefcafe") && !d.contains("hello"), "{d}");
    }
}
