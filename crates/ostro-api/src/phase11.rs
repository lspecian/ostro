//! Phase 11 API contract (11-04, D-P11-22): run operations ([`OpsApi`]: rerun, DLQ,
//! bulk jobs, limits, failure policy) and inventory ([`InventoryApi`]: modules, queues,
//! partitions).
//!
//! Like Phase 8 ([`crate::phase8`]), the operations live in extension traits held in
//! optional slots of [`crate::ApiExtensions`] (`ops`, `inventory`), not in
//! [`crate::ControlPlane`]; an absent slot answers 404 once the routes land (11-09).
//! The traits take and return wire DTOs directly (serde + utoipa derives).
//!
//! Every trait method has a default body answering `Unimplemented` and naming the plan
//! that implements it, so an implementation compiles before its plan lands.
//!
//! Enum DTOs are externally tagged in `snake_case`: `{"seq": 12}`,
//! `{"step": {"name": "charge", "occurrence": 1}}`, `"active"`, `{"version": 3}`,
//! `{"terminate": {"reason": "..."}}`.
//!
//! Failure policy (on_failure hook, DLQ) is the separate [`FailurePolicy`] DTO (B2,
//! D-P11-29): [`crate::rest`]'s `RunPolicy` is untouched.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::error::{ApiError, ErrorCode};
use crate::rest::RunStatusDto;

/// The `Unimplemented` default body of a Phase 11 trait method.
fn lands_in(method: &str, plan: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Unimplemented,
        format!("{method} lands in {plan}"),
    )
    .with_details(serde_json::json!({ "method": method }))
}

fn one() -> u32 {
    1
}

// ---------------------------------------------------------------------------------
// Failure policy (D-P11-12, D-P11-29)
// ---------------------------------------------------------------------------------

/// Which policy record a failure policy belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyScope {
    /// The `WorkflowPolicy` of the named workflow.
    Workflow(String),
    /// The `QueuePolicy` of the named queue.
    Queue(String),
}

/// The `on_failure` hook (ext tag 144): start `workflow` when a run fails or times out.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct OnFailurePolicy {
    /// Hook workflow.
    pub workflow: String,
    /// Queue of the hook run; absent = the hook workflow's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Pass the failed run's input to the hook.
    #[serde(default)]
    pub include_input: bool,
}

/// DLQ policy of a queue (ext tag 145). Absent = enabled, 24 h catch-up.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqPolicyDto {
    /// Failed runs of this queue enter the DLQ.
    pub enabled: bool,
    /// Hook catch-up window at leader start, in ms.
    pub hook_catchup_ms: u64,
}

/// The failure policy of a workflow or a queue.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FailurePolicy {
    /// The `on_failure` hook (the workflow's wins over the queue's).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_failure: Option<OnFailurePolicy>,
    /// DLQ policy (queues only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dlq: Option<DlqPolicyDto>,
    /// Meta record version for CAS on write; returned on read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version_tag: Option<u64>,
}

// ---------------------------------------------------------------------------------
// Rerun (D-P11-3..8)
// ---------------------------------------------------------------------------------

/// Where a rerun cuts the source journal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RerunFrom {
    /// Journal sequence number: events `< seq` are copied.
    Seq(u64),
    /// The `occurrence`-th (1-based) execution of step `name`.
    Step {
        /// Step name.
        name: String,
        /// 1-based occurrence (default 1).
        #[serde(default = "one")]
        occurrence: u32,
    },
}

/// Which workflow version a rerun executes.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RerunVersion {
    /// The workflow's active version.
    #[default]
    Active,
    /// The source run's version.
    Source,
    /// An explicit version.
    Version(u32),
}

/// `POST /v1/runs/{id}/rerun`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RerunRequest {
    /// Source run (terminal).
    pub run_id: String,
    /// Cut point.
    pub from: RerunFrom,
    /// Version to execute.
    #[serde(default)]
    pub version: RerunVersion,
    /// Dedupe key of the new run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Validate only.
    #[serde(default)]
    pub dry_run: bool,
}

/// Result of a rerun.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RerunResponse {
    /// The new run; `None` on a dry run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_run_id: Option<String>,
    /// Source run.
    pub source_run_id: String,
    /// Effective cut sequence number.
    pub cut_seq: u64,
    /// Events copied from the source.
    pub copied_events: u64,
    /// Workflow of the new run.
    pub workflow: String,
    /// Version of the new run.
    pub version: u32,
    /// `"ok"` or `"ok_frontier"`.
    pub verdict: String,
    /// `true` when the idempotency key returned an existing run.
    #[serde(default)]
    pub existing: bool,
}

/// One candidate cut of a run (for the UI).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RerunCut {
    /// Journal sequence number.
    pub seq: u64,
    /// Step name at this point, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    /// 1-based occurrence of `step`.
    pub occurrence: u32,
    /// Whether a rerun may cut here.
    pub safe: bool,
    /// Why not, when unsafe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Nearest safe cut, when unsafe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nearest_safe_seq: Option<u64>,
}

/// `GET /v1/runs/{id}/rerun-cuts`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RerunCutsView {
    /// The analysed run.
    pub run_id: String,
    /// Candidate cuts in journal order.
    pub cuts: Vec<RerunCut>,
}

// ---------------------------------------------------------------------------------
// DLQ (D-P11-9..12)
// ---------------------------------------------------------------------------------

/// `GET /v1/dlq`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqListRequest {
    /// Filter by workflow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    /// Filter by queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Filter by failure reason prefix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Every tag must match.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Include dismissed entries.
    #[serde(default)]
    pub include_dismissed: bool,
    /// Page size (0 = server default).
    #[serde(default)]
    pub limit: u32,
    /// Continuation token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

/// One DLQ entry: a failed or timed-out run.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqEntry {
    /// Run id.
    pub run_id: String,
    /// Workflow.
    pub workflow: String,
    /// Workflow version.
    pub version: u32,
    /// Queue.
    pub queue: String,
    /// Terminal status.
    pub status: RunStatusDto,
    /// Failure reason.
    pub reason: String,
    /// When the run failed, ms since the epoch.
    pub failed_at_ms: u64,
    /// Run tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Whether the run can be resumed.
    pub resumable: bool,
    /// The on_failure hook run of the current episode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hook_run_id: Option<String>,
    /// When the entry was dismissed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dismissed_at_ms: Option<u64>,
    /// Why it was dismissed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dismiss_reason: Option<String>,
    /// The run it was requeued to (restart / rerun).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requeued_to: Option<String>,
    /// When it was requeued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requeued_at_ms: Option<u64>,
}

/// A page of DLQ entries.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqPage {
    /// Entries.
    pub entries: Vec<DlqEntry>,
    /// Continuation token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// How a DLQ entry is requeued.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequeueMode {
    /// Resume if resumable, else restart.
    #[default]
    Auto,
    /// Resume the run in place.
    Resume,
    /// Start a new run with the same input.
    Restart,
    /// Rerun from a cut.
    Rerun {
        /// Cut point.
        from: RerunFrom,
    },
}

/// `POST /v1/dlq/{run_id}/requeue`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqRequeueRequest {
    /// DLQ member.
    pub run_id: String,
    /// Mode.
    #[serde(default)]
    pub mode: RequeueMode,
    /// Validate only.
    #[serde(default)]
    pub dry_run: bool,
}

/// Result of a requeue.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqRequeueResponse {
    /// The requeued run.
    pub run_id: String,
    /// The mode actually used (`resume` | `restart` | `rerun`).
    pub mode: String,
    /// The new run (restart / rerun).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_run_id: Option<String>,
}

/// `POST /v1/dlq/{run_id}/dismiss`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DlqDismissRequest {
    /// DLQ member.
    pub run_id: String,
    /// Operator reason (≤ 1024 bytes).
    pub reason: String,
}

// ---------------------------------------------------------------------------------
// Bulk jobs (D-P11-20)
// ---------------------------------------------------------------------------------

/// The operation a bulk job applies to each matched run.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum BulkAction {
    /// Cancel.
    Cancel,
    /// Terminate.
    Terminate {
        /// Operator reason.
        reason: String,
    },
    /// Retry (resume) failed runs.
    Retry,
    /// Rerun from a cut.
    Rerun {
        /// Cut point.
        from: RerunFrom,
        /// Version to execute.
        #[serde(default)]
        version: RerunVersion,
    },
    /// Requeue DLQ members.
    Requeue {
        /// Mode.
        #[serde(default)]
        mode: RequeueMode,
    },
    /// Pause.
    Pause,
    /// Resume a paused run.
    Resume,
    /// Send a signal.
    Signal {
        /// Signal name.
        name: String,
        /// Signal payload.
        #[serde(default)]
        payload: serde_json::Value,
    },
}

/// Which runs a bulk job selects.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkFilter {
    /// Workflow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,
    /// Status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<RunStatusDto>,
    /// Queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Every tag must match.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Only DLQ members.
    #[serde(default)]
    pub dlq: bool,
    /// Created at or after, ms since the epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_after_ms: Option<u64>,
    /// Created before, ms since the epoch (the job's creation time is always added).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_before_ms: Option<u64>,
}

/// `POST /v1/bulk`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct BulkCreateRequest {
    /// Operation.
    pub action: BulkAction,
    /// Selection.
    #[serde(default)]
    pub filter: BulkFilter,
    /// Ops per second (default `[bulk] default_rate`, capped at `max_rate`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rate_per_sec: Option<u32>,
    /// Concurrent ops (default `[bulk] max_in_flight`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_in_flight: Option<u32>,
    /// Count and sample only.
    #[serde(default)]
    pub dry_run: bool,
}

/// Result of creating a bulk job.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkCreateResponse {
    /// The job (`blk_<uuidv7>`); `None` on a dry run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<String>,
    /// Estimated matches.
    pub matched_estimate: u64,
    /// Up to 20 matched run ids.
    #[serde(default)]
    pub sample: Vec<String>,
}

/// One failed bulk op.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkFailure {
    /// Run id.
    pub run_id: String,
    /// Error.
    pub reason: String,
}

/// A bulk job's progress.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkJobView {
    /// Job id.
    pub job_id: String,
    /// `pending` | `running` | `paused_backpressure` | `done` | `cancelled` | `failed`.
    pub state: String,
    /// Action name.
    pub action: String,
    /// Runs matched.
    pub matched: u64,
    /// Ops applied.
    pub done: u64,
    /// Runs skipped (no longer eligible).
    pub skipped: u64,
    /// The first 100 failures.
    #[serde(default)]
    pub failed: Vec<BulkFailure>,
    /// Total failures.
    pub failed_count: u64,
    /// Effective rate.
    pub rate_per_sec: u32,
    /// Created, ms since the epoch.
    pub created_at_ms: u64,
    /// Started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_ms: Option<u64>,
    /// Finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    /// Estimated time to completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eta_ms: Option<u64>,
}

// ---------------------------------------------------------------------------------
// Limits (D-P11-13)
// ---------------------------------------------------------------------------------

/// One published limit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LimitView {
    /// Limit name (e.g. `max_history_events`).
    pub name: String,
    /// Effective value.
    pub value: u64,
    /// Unit (`events`, `children`, `bytes`).
    pub unit: String,
    /// Whether the operator can change it.
    pub configurable: bool,
    /// Where it is enforced.
    pub enforced: String,
    /// The error a breach produces.
    pub error: String,
}

/// `GET /v1/limits`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct LimitsView {
    /// Every published limit.
    pub limits: Vec<LimitView>,
}

// ---------------------------------------------------------------------------------
// Inventory (D-P11-19)
// ---------------------------------------------------------------------------------

/// `GET /v1/modules`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ListModulesRequest {
    /// Page size (0 = server default).
    #[serde(default)]
    pub limit: u32,
    /// Continuation token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_token: Option<String>,
}

/// One stored module.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ModuleSummary {
    /// Content hash (hex).
    pub sha256: String,
    /// Size in bytes.
    pub size_bytes: u64,
    /// Upload time, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uploaded_at_ms: Option<u64>,
    /// Workflows whose versions reference it.
    #[serde(default)]
    pub workflows: Vec<String>,
}

/// A page of modules.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct ModulePage {
    /// Modules.
    pub modules: Vec<ModuleSummary>,
    /// Continuation token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_page_token: Option<String>,
}

/// One queue.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct QueueView {
    /// Queue name.
    pub name: String,
    /// Paused.
    pub paused: bool,
    /// Ready runs.
    pub ready: u64,
    /// Running runs.
    pub running: u64,
    /// Worker demand, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub demand: Option<u64>,
    /// Phase 7 flow configuration, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow: Option<serde_json::Value>,
}

/// One partition. Only fields the node exposes are filled (11-11 records the list).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PartitionView {
    /// Partition id.
    pub id: u16,
    /// Role on this node (`leader`, `follower`, ...).
    pub role: String,
    /// Leader node id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<u64>,
    /// Leader epoch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// Applied log offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_offset: Option<u64>,
    /// Log end offset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_end_offset: Option<u64>,
    /// Ready runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready: Option<u64>,
    /// Running runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running: Option<u64>,
    /// Healthy.
    pub healthy: bool,
    /// Health detail.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

// ---------------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------------

/// Run operations: rerun (11-10), DLQ and failure policy (11-12), bulk jobs (11-13),
/// limits (11-11).
#[async_trait::async_trait]
pub trait OpsApi: Send + Sync + 'static {
    /// Rerun a terminal run from a cut.
    async fn rerun_run(&self, req: RerunRequest) -> Result<RerunResponse, ApiError> {
        let _ = req;
        Err(lands_in("rerun_run", "11-10"))
    }

    /// Per-step safe/unsafe cuts of a run (UI).
    async fn analyze_rerun(&self, run_id: String) -> Result<RerunCutsView, ApiError> {
        let _ = run_id;
        Err(lands_in("analyze_rerun", "11-10"))
    }

    /// List DLQ entries.
    async fn list_dlq(&self, req: DlqListRequest) -> Result<DlqPage, ApiError> {
        let _ = req;
        Err(lands_in("list_dlq", "11-12"))
    }

    /// Requeue a DLQ entry.
    async fn requeue_dlq(&self, req: DlqRequeueRequest) -> Result<DlqRequeueResponse, ApiError> {
        let _ = req;
        Err(lands_in("requeue_dlq", "11-12"))
    }

    /// Dismiss a DLQ entry.
    async fn dismiss_dlq(&self, req: DlqDismissRequest) -> Result<DlqEntry, ApiError> {
        let _ = req;
        Err(lands_in("dismiss_dlq", "11-12"))
    }

    /// Create a bulk job (or count it on a dry run).
    async fn create_bulk(&self, req: BulkCreateRequest) -> Result<BulkCreateResponse, ApiError> {
        let _ = req;
        Err(lands_in("create_bulk", "11-13"))
    }

    /// Every bulk job on this node.
    async fn list_bulk(&self) -> Result<Vec<BulkJobView>, ApiError> {
        Err(lands_in("list_bulk", "11-13"))
    }

    /// One bulk job.
    async fn get_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
        let _ = job_id;
        Err(lands_in("get_bulk", "11-13"))
    }

    /// Cancel a bulk job.
    async fn cancel_bulk(&self, job_id: String) -> Result<BulkJobView, ApiError> {
        let _ = job_id;
        Err(lands_in("cancel_bulk", "11-13"))
    }

    /// The published limits.
    async fn limits(&self) -> Result<LimitsView, ApiError> {
        Err(lands_in("limits", "11-11"))
    }

    /// The failure policy of a workflow or queue.
    async fn get_failure_policy(&self, scope: PolicyScope) -> Result<FailurePolicy, ApiError> {
        let _ = scope;
        Err(lands_in("get_failure_policy", "11-12"))
    }

    /// Replace the failure policy of a workflow or queue (CAS on `version_tag`).
    async fn set_failure_policy(
        &self,
        scope: PolicyScope,
        p: FailurePolicy,
    ) -> Result<FailurePolicy, ApiError> {
        let _ = (scope, p);
        Err(lands_in("set_failure_policy", "11-12"))
    }
}

/// Inventory views (11-11).
#[async_trait::async_trait]
pub trait InventoryApi: Send + Sync + 'static {
    /// Stored modules.
    async fn list_modules(&self, req: ListModulesRequest) -> Result<ModulePage, ApiError> {
        let _ = req;
        Err(lands_in("list_modules", "11-11"))
    }

    /// Queues with counts.
    async fn list_queues(&self) -> Result<Vec<QueueView>, ApiError> {
        Err(lands_in("list_queues", "11-11"))
    }

    /// Partitions with role and health.
    async fn list_partitions(&self) -> Result<Vec<PartitionView>, ApiError> {
        Err(lands_in("list_partitions", "11-11"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Empty;
    impl OpsApi for Empty {}
    impl InventoryApi for Empty {}

    #[tokio::test]
    async fn default_bodies_are_unimplemented_and_name_the_plan() {
        let e = Empty;
        let err = e.limits().await.expect_err("default");
        assert_eq!(err.code, ErrorCode::Unimplemented);
        assert!(err.message.contains("11-11"), "{}", err.message);
        let err = e.list_bulk().await.expect_err("default");
        assert!(err.message.contains("11-13"));
        let err = e.list_partitions().await.expect_err("default");
        assert!(err.message.contains("11-11"));
        let err = e
            .get_failure_policy(PolicyScope::Queue("q".into()))
            .await
            .expect_err("default");
        assert!(err.message.contains("11-12"));
    }

    #[test]
    fn enum_wire_shapes() {
        let f: RerunFrom = serde_json::from_str(r#"{"step":{"name":"charge"}}"#).expect("step");
        assert_eq!(
            f,
            RerunFrom::Step {
                name: "charge".into(),
                occurrence: 1
            }
        );
        let f: RerunFrom = serde_json::from_str(r#"{"seq":7}"#).expect("seq");
        assert_eq!(f, RerunFrom::Seq(7));
        let v: RerunVersion = serde_json::from_str(r#""source""#).expect("source");
        assert_eq!(v, RerunVersion::Source);
        let r: RerunRequest =
            serde_json::from_str(r#"{"run_id":"r","from":{"seq":3}}"#).expect("req");
        assert_eq!(r.version, RerunVersion::Active);
        assert!(!r.dry_run);
        let a: BulkAction =
            serde_json::from_str(r#"{"terminate":{"reason":"x"}}"#).expect("terminate");
        assert_eq!(a, BulkAction::Terminate { reason: "x".into() });
        let m: DlqRequeueRequest = serde_json::from_str(r#"{"run_id":"r"}"#).expect("requeue");
        assert_eq!(m.mode, RequeueMode::Auto);
    }
}
