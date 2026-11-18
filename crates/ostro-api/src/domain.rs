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

/// Start-run request.
#[derive(Clone, Debug, PartialEq, Eq)]
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
}

/// Start-run outcome.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartRunResult {
    /// Run id.
    pub run_id: String,
    /// `false` when an existing run was returned.
    pub created: bool,
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
}

impl RunStatus {
    /// Every status, in declaration order.
    pub const ALL: [RunStatus; 6] = [
        RunStatus::Scheduled,
        RunStatus::Ready,
        RunStatus::Running,
        RunStatus::Completed,
        RunStatus::Failed,
        RunStatus::Cancelled,
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
        }
    }

    /// Parse a lowercase wire name.
    pub fn parse(s: &str) -> Option<RunStatus> {
        RunStatus::ALL.into_iter().find(|st| st.as_str() == s)
    }

    /// Completed, Failed or Cancelled.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
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

    #[test]
    fn run_status_names_round_trip() {
        for st in RunStatus::ALL {
            assert_eq!(RunStatus::parse(st.as_str()), Some(st));
        }
        assert_eq!(RunStatus::parse("Running"), None);
    }

    #[test]
    fn limits() {
        assert_eq!(effective_limit(0), DEFAULT_PAGE_LIMIT);
        assert_eq!(effective_limit(5), 5);
        assert_eq!(effective_limit(5000), MAX_PAGE_LIMIT);
    }
}
