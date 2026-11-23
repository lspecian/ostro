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
//!
//! # Routes (11-09)
//!
//! | Method | Path | Trait method |
//! |---|---|---|
//! | POST | `/v1/runs/{run_id}/rerun` | [`OpsApi::rerun_run`] (201 new run; 200 dry run / `existing`) |
//! | GET | `/v1/runs/{run_id}/rerun-cuts` | [`OpsApi::analyze_rerun`] |
//! | GET | `/v1/dlq` | [`OpsApi::list_dlq`] (`tags` repeatable) |
//! | POST | `/v1/dlq/{run_id}/requeue`, `/dismiss` | [`OpsApi::requeue_dlq`] / [`OpsApi::dismiss_dlq`] |
//! | GET / POST | `/v1/bulk` | [`OpsApi::list_bulk`] / [`OpsApi::create_bulk`] (201 job; 200 dry run) |
//! | GET / POST | `/v1/bulk/{job_id}`, `/v1/bulk/{job_id}/cancel` | [`OpsApi::get_bulk`] / [`OpsApi::cancel_bulk`] |
//! | GET | `/v1/limits` | [`OpsApi::limits`] |
//! | GET / PUT | `/v1/{workflows,queues}/{name}/failure-policy` | [`OpsApi::get_failure_policy`] / [`OpsApi::set_failure_policy`] |
//! | GET | `/v1/modules`, `/v1/queues`, `/v1/partitions` | [`InventoryApi`] |
//!
//! An absent `ops` / `inventory` slot answers 404 `not_found` on REST, and its gRPC
//! service (`ostro.api.v1.RunOps` / `ostro.api.v1.Inventory`) is not mounted
//! (`UNIMPLEMENTED`), as for Phase 8. Request validation that needs no server state
//! (cut shape, reason length, tag counts, rates, D23 names) runs here for both
//! transports before the trait is called.
//!
//! The API caps of D-P11-13 also live here and are applied by the core start / signal /
//! token-completion handlers on both transports ([`check_payload`],
//! [`validate_user_tags`], [`search_tags`]); every limit error carries
//! `details{reason: "limit_exceeded", limit, max, actual}` ([`limit_error`]).

use std::sync::Arc;

use axum::Json;
use axum::extract::rejection::{BytesRejection, JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{inventory_server, run_ops_server};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::error::{ApiError, ErrorBody, ErrorCode};
use crate::phase8::{ApiExtensions, ExtState};
use crate::rest::{RunStatusDto, if_match_version, json_body, optional_json_body, query};

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
    /// Retry the selected failed runs (resume in place or restart as a new linked run).
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

// ---------------------------------------------------------------------------------
// Error vocabulary and shared validation (11-09, D-P11-21, D-P11-13, D-P11-31)
// ---------------------------------------------------------------------------------

/// Most tags a run search may carry (`GET /v1/runs?tags=`, DLQ and bulk filters;
/// D-P11-10).
pub const MAX_SEARCH_TAGS: usize = 8;

/// Prefix of system run tags (`__ostro/on_failure_of=<run>`, D-P11-25). User-supplied
/// tags never start with it.
pub const SYSTEM_TAG_PREFIX: &str = "__ostro/";

/// Default DLQ / module page size when the request says 0.
pub const DEFAULT_OPS_PAGE: u32 = 100;

/// Machine-readable `details.reason` values of Phase 11 (D-P11-21). No new
/// [`ErrorCode`] exists for them: the code says the transport status, the reason the
/// cause.
pub mod reason {
    /// A cap was exceeded: `details{limit, max, actual}` (413 for byte caps, 400 for
    /// count caps).
    pub const LIMIT_EXCEEDED: &str = "limit_exceeded";
    /// Rerun of a run that is not terminal. 409.
    pub const RUN_NOT_TERMINAL: &str = crate::error::reason::RUN_NOT_TERMINAL;
    /// The rerun cut is not a safe point: `details{seq, detail, nearest_safe_seq}`. 409.
    pub const RERUN_CUT_UNSAFE: &str = "rerun_cut_unsafe";
    /// The copied prefix does not replay on the target version: `details{seq, detail}`.
    /// 409.
    pub const RERUN_NONDETERMINISTIC: &str = "rerun_nondeterministic";
    /// The copied prefix exceeds the history limit or 3.5 MiB encoded. 413.
    pub const RERUN_PREFIX_TOO_LARGE: &str = "rerun_prefix_too_large";
    /// The run is not a DLQ member (not failed / timed out, or already dismissed or
    /// requeued in this failure episode). 409.
    pub const DLQ_NOT_MEMBER: &str = "dlq_not_member";
    /// `[bulk] max_jobs` jobs are running: [`crate::ErrorCode::RateLimited`] (429,
    /// Phase 7's code) with `details.retry_after_ms`.
    pub const BULK_TOO_MANY_JOBS: &str = "bulk_too_many_jobs";
    /// The filter matches more than `[bulk] max_matches` runs. 400.
    pub const BULK_TOO_MANY_MATCHES: &str = "bulk_too_many_matches";
}

/// A limit error: `details == {"reason": "limit_exceeded", "limit", "max", "actual"}`
/// (D-P11-13). `code` is [`ErrorCode::PayloadTooLarge`] for byte caps and
/// [`ErrorCode::InvalidArgument`] for count caps.
pub fn limit_error(code: ErrorCode, limit: &str, max: u64, actual: u64) -> ApiError {
    ApiError::new(
        code,
        format!("{limit}: {actual} exceeds the limit of {max}"),
    )
    .with_details(serde_json::json!({
        "reason": reason::LIMIT_EXCEEDED,
        "limit": limit,
        "max": max,
        "actual": actual,
    }))
}

/// An error refined by `details.reason`; the keys of `extra` (an object, else ignored)
/// are merged next to it.
pub fn reason_error(
    code: ErrorCode,
    reason: &str,
    msg: impl Into<String>,
    extra: serde_json::Value,
) -> ApiError {
    let mut details = serde_json::Map::new();
    details.insert("reason".to_owned(), reason.into());
    if let serde_json::Value::Object(m) = extra {
        for (k, v) in m {
            if k != "reason" {
                details.insert(k, v);
            }
        }
    }
    ApiError::new(code, msg).with_details(serde_json::Value::Object(details))
}

/// 409 `rerun_cut_unsafe` with the cut, why it is unsafe and the nearest safe cut.
pub fn rerun_cut_unsafe(seq: u64, detail: &str, nearest_safe_seq: Option<u64>) -> ApiError {
    reason_error(
        ErrorCode::Conflict,
        reason::RERUN_CUT_UNSAFE,
        format!("seq {seq} is not a safe rerun cut: {detail}"),
        serde_json::json!({ "seq": seq, "detail": detail, "nearest_safe_seq": nearest_safe_seq }),
    )
}

/// 409 `dlq_not_member`.
pub fn dlq_not_member(run_id: &str) -> ApiError {
    reason_error(
        ErrorCode::Conflict,
        reason::DLQ_NOT_MEMBER,
        format!("run {run_id} is not in the DLQ"),
        serde_json::json!({ "run_id": run_id }),
    )
}

/// 429 `bulk_too_many_jobs` (Phase 7's [`ErrorCode::RateLimited`], so clients honour
/// `retry_after_ms` as for any throttle).
pub fn bulk_too_many_jobs(max_jobs: u64, retry_after_ms: u64) -> ApiError {
    reason_error(
        ErrorCode::RateLimited,
        reason::BULK_TOO_MANY_JOBS,
        format!("{max_jobs} bulk jobs are already running"),
        serde_json::json!({
            crate::error::RETRY_AFTER_MS: retry_after_ms,
            "limit": "bulk_jobs",
            "max": max_jobs,
        }),
    )
}

/// The user payload cap (`[limits] max_payload_bytes`): a payload of `actual` bytes over
/// `max` is 413 [`limit_error`] `payload_bytes`. `field` names the payload in the
/// message (`input`, `payload`).
pub fn check_payload(field: &str, actual: usize, max: usize) -> Result<(), ApiError> {
    if actual <= max {
        return Ok(());
    }
    let mut e = limit_error(
        ErrorCode::PayloadTooLarge,
        "payload_bytes",
        max as u64,
        actual as u64,
    );
    e.message = format!("{field} is {actual} bytes, limits.max_payload_bytes is {max}");
    Err(e)
}

/// [`check_payload`] on the compact JSON encoding of `v` (what is stored).
pub fn check_json_payload(field: &str, v: &serde_json::Value, max: usize) -> Result<(), ApiError> {
    let len = serde_json::to_vec(v).map(|b| b.len()).unwrap_or(usize::MAX);
    check_payload(field, len, max)
}

/// User-supplied run tags: none may start with [`SYSTEM_TAG_PREFIX`] (400). The count
/// and length caps (64 × 128 bytes) stay where they are enforced today (the control
/// plane / apply).
pub fn validate_user_tags(tags: &[String]) -> Result<(), ApiError> {
    if let Some(t) = tags.iter().find(|t| t.starts_with(SYSTEM_TAG_PREFIX)) {
        return Err(ApiError::invalid_argument(format!(
            "tag {t:?}: the {SYSTEM_TAG_PREFIX:?} prefix is reserved for system tags"
        ))
        .with_details(serde_json::json!({ "field": "tags", "tag": t })));
    }
    Ok(())
}

/// The tags of a search: the legacy single `tag` (first) plus `tags`, duplicates
/// removed; more than [`MAX_SEARCH_TAGS`] is 400 [`limit_error`] `search_tags`.
pub fn search_tags(tag: Option<String>, tags: Vec<String>) -> Result<Vec<String>, ApiError> {
    let mut out: Vec<String> = Vec::with_capacity(tags.len() + 1);
    for t in tag.into_iter().chain(tags) {
        if t.is_empty() {
            return Err(ApiError::invalid_argument("tags: empty tag"));
        }
        if !out.contains(&t) {
            out.push(t);
        }
    }
    check_search_tags(out.len())?;
    Ok(out)
}

fn check_search_tags(n: usize) -> Result<(), ApiError> {
    if n > MAX_SEARCH_TAGS {
        return Err(limit_error(
            ErrorCode::InvalidArgument,
            "search_tags",
            MAX_SEARCH_TAGS as u64,
            n as u64,
        ));
    }
    Ok(())
}

/// D23 resource name `^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$` (the grammar of
/// `ostro_meta::is_resource_name`, which ostro-api does not depend on).
fn is_resource_name(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 128
        && b[0].is_ascii_alphanumeric()
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'_' | b'-'))
}

/// D23 tenant id `^[a-z0-9][a-z0-9-]{0,62}$`.
fn is_tenant_id(s: &str) -> bool {
    let b = s.as_bytes();
    !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

/// A D23 name, optionally tenant-qualified (`[<tenant>/]<name>`): 400 otherwise.
pub fn validate_qualified_name(field: &str, s: &str) -> Result<(), ApiError> {
    let ok = match s.split_once('/') {
        None => is_resource_name(s),
        Some((t, n)) => is_tenant_id(t) && is_resource_name(n),
    };
    if ok {
        Ok(())
    } else {
        Err(ApiError::invalid_argument(format!(
            "{field}: {s:?} is not a valid name ([<tenant>/]<name>, name = [A-Za-z0-9][A-Za-z0-9._-]{{0,127}})"
        )))
    }
}

fn validate_from(from: &RerunFrom) -> Result<(), ApiError> {
    if let RerunFrom::Step { name, occurrence } = from {
        if name.is_empty() {
            return Err(ApiError::invalid_argument("from.step.name is required"));
        }
        if *occurrence == 0 {
            return Err(ApiError::invalid_argument(
                "from.step.occurrence is 1-based (must be >= 1)",
            ));
        }
    }
    Ok(())
}

fn validate_run_id(run_id: &str) -> Result<(), ApiError> {
    if run_id.is_empty() {
        return Err(ApiError::invalid_argument("run_id is required"));
    }
    Ok(())
}

fn validate_reason(field: &str, r: &str) -> Result<(), ApiError> {
    if r.len() > crate::domain::MAX_REASON_LEN {
        let mut e = limit_error(
            ErrorCode::InvalidArgument,
            "reason_bytes",
            crate::domain::MAX_REASON_LEN as u64,
            r.len() as u64,
        );
        e.message = format!(
            "{field} is {} bytes, the limit is {}",
            r.len(),
            crate::domain::MAX_REASON_LEN
        );
        return Err(e);
    }
    Ok(())
}

/// Transport-neutral validation of the Phase 11 requests (shared by REST and gRPC).
pub(crate) mod check {
    use super::*;

    pub(crate) fn rerun(r: &RerunRequest) -> Result<(), ApiError> {
        validate_run_id(&r.run_id)?;
        validate_from(&r.from)?;
        if r.idempotency_key.as_deref() == Some("") {
            return Err(ApiError::invalid_argument(
                "idempotency_key must not be empty when set",
            ));
        }
        Ok(())
    }

    pub(crate) fn dlq_list(r: &DlqListRequest) -> Result<(), ApiError> {
        check_search_tags(r.tags.len())
    }

    pub(crate) fn requeue(r: &DlqRequeueRequest) -> Result<(), ApiError> {
        validate_run_id(&r.run_id)?;
        if let RequeueMode::Rerun { from } = &r.mode {
            validate_from(from)?;
        }
        Ok(())
    }

    pub(crate) fn dismiss(r: &DlqDismissRequest) -> Result<(), ApiError> {
        validate_run_id(&r.run_id)?;
        validate_reason("reason", &r.reason)
    }

    pub(crate) fn bulk(r: &BulkCreateRequest) -> Result<(), ApiError> {
        check_search_tags(r.filter.tags.len())?;
        if r.rate_per_sec == Some(0) {
            return Err(ApiError::invalid_argument(
                "rate_per_sec must be > 0 when set",
            ));
        }
        if r.max_in_flight == Some(0) {
            return Err(ApiError::invalid_argument(
                "max_in_flight must be > 0 when set",
            ));
        }
        match &r.action {
            BulkAction::Terminate { reason } => validate_reason("action.terminate.reason", reason),
            BulkAction::Rerun { from, .. } => validate_from(from),
            BulkAction::Requeue {
                mode: RequeueMode::Rerun { from },
            } => validate_from(from),
            BulkAction::Signal { name, .. } => {
                if name.is_empty() || name.len() > crate::domain::MAX_SIGNAL_NAME_LEN {
                    return Err(ApiError::invalid_argument(format!(
                        "action.signal.name must be 1..={} bytes",
                        crate::domain::MAX_SIGNAL_NAME_LEN
                    )));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub(crate) fn job_id(id: &str) -> Result<(), ApiError> {
        if id.is_empty() {
            return Err(ApiError::invalid_argument("job_id is required"));
        }
        Ok(())
    }

    pub(crate) fn scope(s: &PolicyScope) -> Result<(), ApiError> {
        match s {
            PolicyScope::Workflow(n) => validate_qualified_name("workflow", n),
            PolicyScope::Queue(n) => validate_qualified_name("queue", n),
        }
    }

    pub(crate) fn failure_policy(s: &PolicyScope, p: &FailurePolicy) -> Result<(), ApiError> {
        scope(s)?;
        if let Some(of) = &p.on_failure {
            validate_qualified_name("on_failure.workflow", &of.workflow)?;
            if let Some(q) = &of.queue {
                validate_qualified_name("on_failure.queue", q)?;
            }
        }
        if p.dlq.is_some() && matches!(s, PolicyScope::Workflow(_)) {
            return Err(ApiError::invalid_argument(
                "dlq policy is set per queue, not per workflow",
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------
// REST (11-09)
// ---------------------------------------------------------------------------------

/// Router-wide options the REST handlers of [`crate::rest`] read (installed by
/// [`crate::router_with`] as an axum `Extension`; absent = [`crate::ApiConfig`]
/// defaults).
#[derive(Clone, Debug, Default)]
pub struct RestOptions {
    /// `[limits] max_payload_bytes`.
    pub max_payload_bytes: usize,
    /// `GET /` redirect target.
    pub root_redirect: Option<String>,
}

impl RestOptions {
    /// From the router's [`crate::ApiConfig`].
    pub fn from_config(cfg: &crate::ApiConfig) -> Self {
        RestOptions {
            max_payload_bytes: cfg.max_payload_bytes,
            root_redirect: cfg.root_redirect.clone(),
        }
    }

    pub(crate) fn of(ext: Option<axum::Extension<RestOptions>>) -> RestOptions {
        ext.map(|axum::Extension(o)| o)
            .unwrap_or_else(|| RestOptions::from_config(&crate::ApiConfig::default()))
    }
}

/// `{"reason"}` body of `POST /v1/dlq/{run_id}/dismiss`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct DismissBody {
    /// Operator reason (≤ 1024 bytes).
    pub reason: String,
}

/// Body of `POST /v1/dlq/{run_id}/requeue` (optional: empty = auto, not a dry run).
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RequeueBody {
    /// Mode.
    #[serde(default)]
    pub mode: RequeueMode,
    /// Validate only.
    #[serde(default)]
    pub dry_run: bool,
}

/// Body of `POST /v1/runs/{run_id}/rerun`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct RerunBody {
    /// Cut point.
    pub from: RerunFrom,
    /// Version to execute (default `active`).
    #[serde(default)]
    pub version: RerunVersion,
    /// Dedupe key of the new run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Validate only.
    #[serde(default)]
    pub dry_run: bool,
}

/// `GET /v1/bulk`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct BulkJobList {
    /// Every bulk job on this node.
    pub jobs: Vec<BulkJobView>,
}

/// `GET /v1/queues`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct QueueList {
    /// Queues.
    pub queues: Vec<QueueView>,
}

/// `GET /v1/partitions`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct PartitionList {
    /// Partitions.
    pub partitions: Vec<PartitionView>,
}

/// Result order of `GET /v1/runs` (D-P11-10).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ListOrderDto {
    /// Oldest first (default; the pre-Phase 11 order).
    #[default]
    Oldest,
    /// Newest first.
    Newest,
}

impl From<ListOrderDto> for crate::domain::ListOrder {
    fn from(o: ListOrderDto) -> Self {
        match o {
            ListOrderDto::Oldest => crate::domain::ListOrder::Oldest,
            ListOrderDto::Newest => crate::domain::ListOrder::Newest,
        }
    }
}

/// `GET /v1/dlq` scalar parameters (`tags` is repeatable and read separately).
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct DlqListQuery {
    /// Workflow filter.
    pub workflow: Option<String>,
    /// Queue filter.
    pub queue: Option<String>,
    /// Failure reason prefix.
    pub reason: Option<String>,
    /// Include dismissed entries.
    pub include_dismissed: Option<bool>,
    /// Page size (default 100).
    pub limit: Option<u32>,
    /// Continuation token.
    pub page_token: Option<String>,
}

/// `GET /v1/modules` parameters.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct ModulesQuery {
    /// Page size (default 100).
    pub limit: Option<u32>,
    /// Continuation token.
    pub page_token: Option<String>,
}

/// `version_tag` of a failure-policy PUT as a query parameter.
#[derive(Clone, Debug, Default, Deserialize, IntoParams)]
#[into_params(parameter_in = Query)]
pub struct PolicyTagQuery {
    /// Meta record version (alternatively the body's `version_tag` or `If-Match`).
    pub version_tag: Option<u64>,
}

/// Every value of the repeatable query parameter `key` (`?tags=a&tags=b`).
pub(crate) fn repeated(pairs: &[(String, String)], key: &str) -> Vec<String> {
    pairs
        .iter()
        .filter(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
        .collect()
}

/// The query as `(key, value)` pairs (axum's `Query` cannot collect repeated keys into
/// a struct field).
pub(crate) fn query_pairs(
    q: Result<Query<Vec<(String, String)>>, QueryRejection>,
) -> Result<Vec<(String, String)>, ApiError> {
    q.map(|Query(v)| v)
        .map_err(|r| ApiError::invalid_argument(r.body_text()))
}

fn absent(what: &str) -> ApiError {
    ApiError::not_found(format!("the {what} API is not enabled on this server"))
}

fn ops(ext: &ApiExtensions) -> Result<&dyn OpsApi, ApiError> {
    ext.ops.as_deref().ok_or_else(|| absent("run operations"))
}

fn inventory(ext: &ApiExtensions) -> Result<&dyn InventoryApi, ApiError> {
    ext.inventory.as_deref().ok_or_else(|| absent("inventory"))
}

fn merge_tag(body: Option<u64>, other: Option<u64>) -> Result<Option<u64>, ApiError> {
    match (body, other) {
        (Some(b), Some(o)) if b != o => Err(ApiError::invalid_argument(
            "version_tag and If-Match / query disagree",
        )),
        (b, o) => Ok(b.or(o)),
    }
}

/// Rerun a terminal run from a cut.
#[utoipa::path(
    post,
    path = "/v1/runs/{run_id}/rerun",
    tag = "operations",
    params(("run_id" = String, Path, description = "Source run (terminal)")),
    request_body = RerunBody,
    responses(
        (status = 201, description = "New run started", body = RerunResponse),
        (status = 200, description = "Dry run, or an existing run for the idempotency key (existing = true)", body = RerunResponse),
        (status = 400, description = "Invalid request", body = ErrorBody),
        (status = 404, description = "Unknown run, or run operations not enabled", body = ErrorBody),
        (status = 409, description = "run_not_terminal, rerun_cut_unsafe, rerun_nondeterministic (details.reason)", body = ErrorBody),
        (status = 413, description = "rerun_prefix_too_large", body = ErrorBody),
    )
)]
async fn rerun_run(
    State(ext): State<ExtState>,
    Path(run_id): Path<String>,
    body: Result<Json<RerunBody>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = ops(&ext)?;
    let b = json_body(body)?;
    let req = RerunRequest {
        run_id,
        from: b.from,
        version: b.version,
        idempotency_key: b.idempotency_key,
        dry_run: b.dry_run,
    };
    check::rerun(&req)?;
    let dry = req.dry_run;
    let res = api.rerun_run(req).await?;
    let status = if dry || res.existing || res.new_run_id.is_none() {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(res)).into_response())
}

/// Candidate rerun cuts of a run (safe / unsafe per step).
#[utoipa::path(
    get,
    path = "/v1/runs/{run_id}/rerun-cuts",
    tag = "operations",
    params(("run_id" = String, Path, description = "Run id")),
    responses(
        (status = 200, description = "Cuts in journal order", body = RerunCutsView),
        (status = 404, description = "Unknown run, or run operations not enabled", body = ErrorBody),
    )
)]
async fn rerun_cuts(
    State(ext): State<ExtState>,
    Path(run_id): Path<String>,
) -> Result<Json<RerunCutsView>, ApiError> {
    let api = ops(&ext)?;
    validate_run_id(&run_id)?;
    Ok(Json(api.analyze_rerun(run_id).await?))
}

/// List DLQ entries (failed and timed-out runs).
#[utoipa::path(
    get,
    path = "/v1/dlq",
    tag = "operations",
    params(
        DlqListQuery,
        ("tags" = Option<Vec<String>>, Query, description = "Repeatable; every tag must match (max 8)"),
    ),
    responses(
        (status = 200, description = "A page of DLQ entries", body = DlqPage),
        (status = 400, description = "Invalid filter (> 8 tags: details.limit = search_tags)", body = ErrorBody),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
    )
)]
async fn list_dlq(
    State(ext): State<ExtState>,
    q: Result<Query<DlqListQuery>, QueryRejection>,
    pairs: Result<Query<Vec<(String, String)>>, QueryRejection>,
) -> Result<Json<DlqPage>, ApiError> {
    let api = ops(&ext)?;
    let q = query(q)?;
    let tags = repeated(&query_pairs(pairs)?, "tags");
    let req = DlqListRequest {
        workflow: q.workflow,
        queue: q.queue,
        reason: q.reason,
        tags,
        include_dismissed: q.include_dismissed.unwrap_or(false),
        limit: q.limit.unwrap_or(0),
        page_token: q.page_token,
    };
    check::dlq_list(&req)?;
    Ok(Json(api.list_dlq(req).await?))
}

/// Requeue a DLQ entry (resume, restart or rerun).
#[utoipa::path(
    post,
    path = "/v1/dlq/{run_id}/requeue",
    tag = "operations",
    params(("run_id" = String, Path, description = "DLQ member")),
    request_body(content = RequeueBody, description = "Optional; empty = mode auto"),
    responses(
        (status = 200, description = "Requeued (or validated, for dry_run)", body = DlqRequeueResponse),
        (status = 404, description = "Unknown run, or run operations not enabled", body = ErrorBody),
        (status = 409, description = "dlq_not_member (details.reason)", body = ErrorBody),
    )
)]
async fn requeue_dlq(
    State(ext): State<ExtState>,
    Path(run_id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<DlqRequeueResponse>, ApiError> {
    let api = ops(&ext)?;
    let b: RequeueBody = optional_json_body(body)?;
    let req = DlqRequeueRequest {
        run_id,
        mode: b.mode,
        dry_run: b.dry_run,
    };
    check::requeue(&req)?;
    Ok(Json(api.requeue_dlq(req).await?))
}

/// Dismiss a DLQ entry.
#[utoipa::path(
    post,
    path = "/v1/dlq/{run_id}/dismiss",
    tag = "operations",
    params(("run_id" = String, Path, description = "DLQ member")),
    request_body = DismissBody,
    responses(
        (status = 200, description = "The dismissed entry", body = DlqEntry),
        (status = 400, description = "Reason over 1024 bytes (details.limit = reason_bytes)", body = ErrorBody),
        (status = 404, description = "Unknown run, or run operations not enabled", body = ErrorBody),
        (status = 409, description = "dlq_not_member (details.reason)", body = ErrorBody),
    )
)]
async fn dismiss_dlq(
    State(ext): State<ExtState>,
    Path(run_id): Path<String>,
    body: Result<Json<DismissBody>, JsonRejection>,
) -> Result<Json<DlqEntry>, ApiError> {
    let api = ops(&ext)?;
    let b = json_body(body)?;
    let req = DlqDismissRequest {
        run_id,
        reason: b.reason,
    };
    check::dismiss(&req)?;
    Ok(Json(api.dismiss_dlq(req).await?))
}

/// Every bulk job on this node.
#[utoipa::path(
    get,
    path = "/v1/bulk",
    tag = "operations",
    responses(
        (status = 200, description = "Bulk jobs", body = BulkJobList),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
    )
)]
async fn list_bulk(State(ext): State<ExtState>) -> Result<Json<BulkJobList>, ApiError> {
    let jobs = ops(&ext)?.list_bulk().await?;
    Ok(Json(BulkJobList { jobs }))
}

/// Create a bulk job (or count its matches, for dry_run).
#[utoipa::path(
    post,
    path = "/v1/bulk",
    tag = "operations",
    request_body = BulkCreateRequest,
    responses(
        (status = 201, description = "Job created", body = BulkCreateResponse),
        (status = 200, description = "Dry run: estimate and up to 20 samples", body = BulkCreateResponse),
        (status = 400, description = "Invalid request; bulk_too_many_matches", body = ErrorBody),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
        (status = 429, description = "bulk_too_many_jobs (details.retry_after_ms)", body = ErrorBody),
    )
)]
async fn create_bulk(
    State(ext): State<ExtState>,
    body: Result<Json<BulkCreateRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let api = ops(&ext)?;
    let req = json_body(body)?;
    check::bulk(&req)?;
    let res = api.create_bulk(req).await?;
    let status = if res.job_id.is_some() {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };
    Ok((status, Json(res)).into_response())
}

/// One bulk job.
#[utoipa::path(
    get,
    path = "/v1/bulk/{job_id}",
    tag = "operations",
    params(("job_id" = String, Path, description = "Job id (blk_...)")),
    responses(
        (status = 200, description = "Progress", body = BulkJobView),
        (status = 404, description = "Unknown job, or run operations not enabled", body = ErrorBody),
    )
)]
async fn get_bulk(
    State(ext): State<ExtState>,
    Path(job_id): Path<String>,
) -> Result<Json<BulkJobView>, ApiError> {
    let api = ops(&ext)?;
    check::job_id(&job_id)?;
    Ok(Json(api.get_bulk(job_id).await?))
}

/// Cancel a bulk job.
#[utoipa::path(
    post,
    path = "/v1/bulk/{job_id}/cancel",
    tag = "operations",
    params(("job_id" = String, Path, description = "Job id (blk_...)")),
    responses(
        (status = 200, description = "The job (state cancelled)", body = BulkJobView),
        (status = 404, description = "Unknown job, or run operations not enabled", body = ErrorBody),
    )
)]
async fn cancel_bulk(
    State(ext): State<ExtState>,
    Path(job_id): Path<String>,
) -> Result<Json<BulkJobView>, ApiError> {
    let api = ops(&ext)?;
    check::job_id(&job_id)?;
    Ok(Json(api.cancel_bulk(job_id).await?))
}

/// The published limits.
#[utoipa::path(
    get,
    path = "/v1/limits",
    tag = "operations",
    responses(
        (status = 200, description = "Every published limit", body = LimitsView),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
    )
)]
async fn get_limits(State(ext): State<ExtState>) -> Result<Json<LimitsView>, ApiError> {
    Ok(Json(ops(&ext)?.limits().await?))
}

async fn get_policy(ext: &ApiExtensions, scope: PolicyScope) -> Result<FailurePolicy, ApiError> {
    let api = ops(ext)?;
    check::scope(&scope)?;
    api.get_failure_policy(scope).await
}

async fn put_policy(
    ext: &ApiExtensions,
    scope: PolicyScope,
    headers: &HeaderMap,
    q: Result<Query<PolicyTagQuery>, QueryRejection>,
    body: Result<Json<FailurePolicy>, JsonRejection>,
) -> Result<FailurePolicy, ApiError> {
    let api = ops(ext)?;
    let mut p = json_body(body)?;
    let other = merge_tag(query(q)?.version_tag, if_match_version(headers)?)?;
    p.version_tag = merge_tag(p.version_tag, other)?;
    check::failure_policy(&scope, &p)?;
    api.set_failure_policy(scope, p).await
}

/// A workflow's failure policy (on_failure hook).
#[utoipa::path(
    get,
    path = "/v1/workflows/{name}/failure-policy",
    tag = "operations",
    params(("name" = String, Path, description = "Workflow name")),
    responses(
        (status = 200, description = "The failure policy (empty when never set)", body = FailurePolicy),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
    )
)]
async fn get_workflow_failure_policy(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
) -> Result<Json<FailurePolicy>, ApiError> {
    Ok(Json(get_policy(&ext, PolicyScope::Workflow(name)).await?))
}

/// Replace a workflow's failure policy (CAS on version_tag / If-Match).
#[utoipa::path(
    put,
    path = "/v1/workflows/{name}/failure-policy",
    tag = "operations",
    params(("name" = String, Path, description = "Workflow name"), PolicyTagQuery),
    request_body = FailurePolicy,
    responses(
        (status = 200, description = "The stored policy", body = FailurePolicy),
        (status = 400, description = "Invalid policy (hook workflow name)", body = ErrorBody),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
        (status = 409, description = "Stale version_tag (details.reason = version_mismatch)", body = ErrorBody),
    )
)]
async fn put_workflow_failure_policy(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    q: Result<Query<PolicyTagQuery>, QueryRejection>,
    body: Result<Json<FailurePolicy>, JsonRejection>,
) -> Result<Json<FailurePolicy>, ApiError> {
    Ok(Json(
        put_policy(&ext, PolicyScope::Workflow(name), &headers, q, body).await?,
    ))
}

/// A queue's failure policy (on_failure hook, DLQ policy).
#[utoipa::path(
    get,
    path = "/v1/queues/{name}/failure-policy",
    tag = "operations",
    params(("name" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "The failure policy (empty when never set)", body = FailurePolicy),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
    )
)]
async fn get_queue_failure_policy(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
) -> Result<Json<FailurePolicy>, ApiError> {
    Ok(Json(get_policy(&ext, PolicyScope::Queue(name)).await?))
}

/// Replace a queue's failure policy (CAS on version_tag / If-Match).
#[utoipa::path(
    put,
    path = "/v1/queues/{name}/failure-policy",
    tag = "operations",
    params(("name" = String, Path, description = "Queue name"), PolicyTagQuery),
    request_body = FailurePolicy,
    responses(
        (status = 200, description = "The stored policy", body = FailurePolicy),
        (status = 400, description = "Invalid policy (hook workflow name)", body = ErrorBody),
        (status = 404, description = "Run operations not enabled", body = ErrorBody),
        (status = 409, description = "Stale version_tag (details.reason = version_mismatch)", body = ErrorBody),
    )
)]
async fn put_queue_failure_policy(
    State(ext): State<ExtState>,
    Path(name): Path<String>,
    headers: HeaderMap,
    q: Result<Query<PolicyTagQuery>, QueryRejection>,
    body: Result<Json<FailurePolicy>, JsonRejection>,
) -> Result<Json<FailurePolicy>, ApiError> {
    Ok(Json(
        put_policy(&ext, PolicyScope::Queue(name), &headers, q, body).await?,
    ))
}

/// Stored modules.
#[utoipa::path(
    get,
    path = "/v1/modules",
    tag = "inventory",
    params(ModulesQuery),
    responses(
        (status = 200, description = "A page of modules", body = ModulePage),
        (status = 404, description = "Inventory not enabled", body = ErrorBody),
    )
)]
async fn list_modules(
    State(ext): State<ExtState>,
    q: Result<Query<ModulesQuery>, QueryRejection>,
) -> Result<Json<ModulePage>, ApiError> {
    let api = inventory(&ext)?;
    let q = query(q)?;
    Ok(Json(
        api.list_modules(ListModulesRequest {
            limit: q.limit.unwrap_or(0),
            page_token: q.page_token,
        })
        .await?,
    ))
}

/// Queues with counts.
#[utoipa::path(
    get,
    path = "/v1/queues",
    tag = "inventory",
    responses(
        (status = 200, description = "Queues", body = QueueList),
        (status = 404, description = "Inventory not enabled", body = ErrorBody),
    )
)]
async fn list_queues(State(ext): State<ExtState>) -> Result<Json<QueueList>, ApiError> {
    let queues = inventory(&ext)?.list_queues().await?;
    Ok(Json(QueueList { queues }))
}

/// Partitions with role and health.
#[utoipa::path(
    get,
    path = "/v1/partitions",
    tag = "inventory",
    responses(
        (status = 200, description = "Partitions", body = PartitionList),
        (status = 404, description = "Inventory not enabled", body = ErrorBody),
    )
)]
async fn list_partitions(State(ext): State<ExtState>) -> Result<Json<PartitionList>, ApiError> {
    let partitions = inventory(&ext)?.list_partitions().await?;
    Ok(Json(PartitionList { partitions }))
}

/// The Phase 11 REST routes with their OpenAPI description, without state (merged into
/// the document by [`crate::openapi()`]).
pub fn api_routes() -> OpenApiRouter<ExtState> {
    OpenApiRouter::new()
        .routes(routes!(rerun_run))
        .routes(routes!(rerun_cuts))
        .routes(routes!(list_dlq))
        .routes(routes!(requeue_dlq))
        .routes(routes!(dismiss_dlq))
        .routes(routes!(list_bulk, create_bulk))
        .routes(routes!(get_bulk))
        .routes(routes!(cancel_bulk))
        .routes(routes!(get_limits))
        .routes(routes!(
            get_workflow_failure_policy,
            put_workflow_failure_policy
        ))
        .routes(routes!(get_queue_failure_policy, put_queue_failure_policy))
        .routes(routes!(list_modules))
        .routes(routes!(list_queues))
        .routes(routes!(list_partitions))
}

/// The Phase 11 REST router over `ext` (merged by [`crate::router_with`]).
pub fn router(ext: ApiExtensions) -> axum::Router {
    let (r, _doc) = api_routes().split_for_parts();
    r.with_state(Arc::new(ext))
}

// ---------------------------------------------------------------------------------
// gRPC (11-09): ostro.api.v1.{RunOps, Inventory}
// ---------------------------------------------------------------------------------

fn status_to_proto(s: RunStatusDto) -> i32 {
    pb::RunStatus::from(crate::domain::RunStatus::from(s)) as i32
}

fn status_from_proto(field: &str, v: i32) -> Result<RunStatusDto, ApiError> {
    crate::grpc::run_status_from_proto(field, v).map(RunStatusDto::from)
}

fn required<T>(field: &str, v: Option<T>) -> Result<T, ApiError> {
    v.ok_or_else(|| ApiError::invalid_argument(format!("{field} is required")))
}

impl TryFrom<pb::RerunFrom> for RerunFrom {
    type Error = ApiError;

    fn try_from(f: pb::RerunFrom) -> Result<Self, ApiError> {
        match required("from", f.point)? {
            pb::rerun_from::Point::Seq(s) => Ok(RerunFrom::Seq(s)),
            pb::rerun_from::Point::Step(st) => Ok(RerunFrom::Step {
                name: st.name,
                occurrence: st.occurrence.max(1),
            }),
        }
    }
}

impl From<RerunFrom> for pb::RerunFrom {
    fn from(f: RerunFrom) -> Self {
        pb::RerunFrom {
            point: Some(match f {
                RerunFrom::Seq(s) => pb::rerun_from::Point::Seq(s),
                RerunFrom::Step { name, occurrence } => {
                    pb::rerun_from::Point::Step(pb::RerunStep { name, occurrence })
                }
            }),
        }
    }
}

fn from_proto(f: Option<pb::RerunFrom>) -> Result<RerunFrom, ApiError> {
    required("from", f)?.try_into()
}

impl From<Option<pb::RerunVersion>> for RerunVersion {
    fn from(v: Option<pb::RerunVersion>) -> Self {
        match v.and_then(|v| v.choice) {
            None | Some(pb::rerun_version::Choice::Active(_)) => RerunVersion::Active,
            Some(pb::rerun_version::Choice::Source(_)) => RerunVersion::Source,
            Some(pb::rerun_version::Choice::Version(n)) => RerunVersion::Version(n),
        }
    }
}

impl From<RerunVersion> for pb::RerunVersion {
    fn from(v: RerunVersion) -> Self {
        pb::RerunVersion {
            choice: Some(match v {
                RerunVersion::Active => pb::rerun_version::Choice::Active(true),
                RerunVersion::Source => pb::rerun_version::Choice::Source(true),
                RerunVersion::Version(n) => pb::rerun_version::Choice::Version(n),
            }),
        }
    }
}

fn mode_from_proto(m: Option<pb::RequeueMode>) -> Result<RequeueMode, ApiError> {
    Ok(match m.and_then(|m| m.mode) {
        None | Some(pb::requeue_mode::Mode::Auto(_)) => RequeueMode::Auto,
        Some(pb::requeue_mode::Mode::Resume(_)) => RequeueMode::Resume,
        Some(pb::requeue_mode::Mode::Restart(_)) => RequeueMode::Restart,
        Some(pb::requeue_mode::Mode::Rerun(f)) => RequeueMode::Rerun {
            from: f.try_into()?,
        },
    })
}

impl From<RequeueMode> for pb::RequeueMode {
    fn from(m: RequeueMode) -> Self {
        pb::RequeueMode {
            mode: Some(match m {
                RequeueMode::Auto => pb::requeue_mode::Mode::Auto(true),
                RequeueMode::Resume => pb::requeue_mode::Mode::Resume(true),
                RequeueMode::Restart => pb::requeue_mode::Mode::Restart(true),
                RequeueMode::Rerun { from } => pb::requeue_mode::Mode::Rerun(from.into()),
            }),
        }
    }
}

impl TryFrom<pb::RerunRequest> for RerunRequest {
    type Error = ApiError;

    fn try_from(r: pb::RerunRequest) -> Result<Self, ApiError> {
        Ok(RerunRequest {
            run_id: r.run_id,
            from: from_proto(r.from)?,
            version: r.version.into(),
            idempotency_key: r.idempotency_key,
            dry_run: r.dry_run,
        })
    }
}

impl From<RerunRequest> for pb::RerunRequest {
    fn from(r: RerunRequest) -> Self {
        pb::RerunRequest {
            run_id: r.run_id,
            from: Some(r.from.into()),
            version: Some(r.version.into()),
            idempotency_key: r.idempotency_key,
            dry_run: r.dry_run,
        }
    }
}

impl From<RerunResponse> for pb::RerunResponse {
    fn from(r: RerunResponse) -> Self {
        pb::RerunResponse {
            new_run_id: r.new_run_id,
            source_run_id: r.source_run_id,
            cut_seq: r.cut_seq,
            copied_events: r.copied_events,
            workflow: r.workflow,
            version: r.version,
            verdict: r.verdict,
            existing: r.existing,
        }
    }
}

impl From<pb::RerunResponse> for RerunResponse {
    fn from(r: pb::RerunResponse) -> Self {
        RerunResponse {
            new_run_id: r.new_run_id,
            source_run_id: r.source_run_id,
            cut_seq: r.cut_seq,
            copied_events: r.copied_events,
            workflow: r.workflow,
            version: r.version,
            verdict: r.verdict,
            existing: r.existing,
        }
    }
}

impl From<RerunCutsView> for pb::RerunCutsView {
    fn from(v: RerunCutsView) -> Self {
        pb::RerunCutsView {
            run_id: v.run_id,
            cuts: v
                .cuts
                .into_iter()
                .map(|c| pb::RerunCut {
                    seq: c.seq,
                    step: c.step,
                    occurrence: c.occurrence,
                    safe: c.safe,
                    reason: c.reason,
                    nearest_safe_seq: c.nearest_safe_seq,
                })
                .collect(),
        }
    }
}

impl From<pb::RerunCutsView> for RerunCutsView {
    fn from(v: pb::RerunCutsView) -> Self {
        RerunCutsView {
            run_id: v.run_id,
            cuts: v
                .cuts
                .into_iter()
                .map(|c| RerunCut {
                    seq: c.seq,
                    step: c.step,
                    occurrence: c.occurrence,
                    safe: c.safe,
                    reason: c.reason,
                    nearest_safe_seq: c.nearest_safe_seq,
                })
                .collect(),
        }
    }
}

impl From<pb::ListDlqRequest> for DlqListRequest {
    fn from(r: pb::ListDlqRequest) -> Self {
        DlqListRequest {
            workflow: r.workflow,
            queue: r.queue,
            reason: r.reason,
            tags: r.tags,
            include_dismissed: r.include_dismissed,
            limit: r.limit,
            page_token: r.page_token,
        }
    }
}

impl From<DlqEntry> for pb::DlqEntry {
    fn from(e: DlqEntry) -> Self {
        pb::DlqEntry {
            run_id: e.run_id,
            workflow: e.workflow,
            version: e.version,
            queue: e.queue,
            status: status_to_proto(e.status),
            reason: e.reason,
            failed_at_ms: e.failed_at_ms,
            tags: e.tags,
            resumable: e.resumable,
            hook_run_id: e.hook_run_id,
            dismissed_at_ms: e.dismissed_at_ms,
            dismiss_reason: e.dismiss_reason,
            requeued_to: e.requeued_to,
            requeued_at_ms: e.requeued_at_ms,
        }
    }
}

impl TryFrom<pb::DlqEntry> for DlqEntry {
    type Error = ApiError;

    fn try_from(e: pb::DlqEntry) -> Result<Self, ApiError> {
        Ok(DlqEntry {
            status: status_from_proto("status", e.status)?,
            run_id: e.run_id,
            workflow: e.workflow,
            version: e.version,
            queue: e.queue,
            reason: e.reason,
            failed_at_ms: e.failed_at_ms,
            tags: e.tags,
            resumable: e.resumable,
            hook_run_id: e.hook_run_id,
            dismissed_at_ms: e.dismissed_at_ms,
            dismiss_reason: e.dismiss_reason,
            requeued_to: e.requeued_to,
            requeued_at_ms: e.requeued_at_ms,
        })
    }
}

impl From<DlqPage> for pb::DlqPage {
    fn from(p: DlqPage) -> Self {
        pb::DlqPage {
            entries: p.entries.into_iter().map(Into::into).collect(),
            next_page_token: p.next_page_token,
        }
    }
}

impl TryFrom<pb::DlqPage> for DlqPage {
    type Error = ApiError;

    fn try_from(p: pb::DlqPage) -> Result<Self, ApiError> {
        Ok(DlqPage {
            entries: p
                .entries
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<_, _>>()?,
            next_page_token: p.next_page_token,
        })
    }
}

impl From<DlqRequeueResponse> for pb::RequeueDlqResponse {
    fn from(r: DlqRequeueResponse) -> Self {
        pb::RequeueDlqResponse {
            run_id: r.run_id,
            mode: r.mode,
            new_run_id: r.new_run_id,
        }
    }
}

impl From<pb::RequeueDlqResponse> for DlqRequeueResponse {
    fn from(r: pb::RequeueDlqResponse) -> Self {
        DlqRequeueResponse {
            run_id: r.run_id,
            mode: r.mode,
            new_run_id: r.new_run_id,
        }
    }
}

impl TryFrom<pb::BulkAction> for BulkAction {
    type Error = ApiError;

    fn try_from(a: pb::BulkAction) -> Result<Self, ApiError> {
        use pb::bulk_action::Action as A;
        Ok(match required("action", a.action)? {
            A::Cancel(_) => BulkAction::Cancel,
            A::Terminate(t) => BulkAction::Terminate { reason: t.reason },
            A::Retry(_) => BulkAction::Retry,
            A::Rerun(r) => BulkAction::Rerun {
                from: from_proto(r.from)?,
                version: r.version.into(),
            },
            A::Requeue(r) => BulkAction::Requeue {
                mode: mode_from_proto(r.mode)?,
            },
            A::Pause(_) => BulkAction::Pause,
            A::Resume(_) => BulkAction::Resume,
            A::Signal(s) => BulkAction::Signal {
                payload: crate::grpc::payload_from_proto("payload_json", &s.payload_json)?,
                name: s.name,
            },
        })
    }
}

impl From<BulkAction> for pb::BulkAction {
    fn from(a: BulkAction) -> Self {
        use pb::bulk_action::Action as A;
        pb::BulkAction {
            action: Some(match a {
                BulkAction::Cancel => A::Cancel(pb::BulkNoArgs {}),
                BulkAction::Terminate { reason } => A::Terminate(pb::BulkTerminate { reason }),
                BulkAction::Retry => A::Retry(pb::BulkNoArgs {}),
                BulkAction::Rerun { from, version } => A::Rerun(pb::BulkRerun {
                    from: Some(from.into()),
                    version: Some(version.into()),
                }),
                BulkAction::Requeue { mode } => A::Requeue(pb::BulkRequeue {
                    mode: Some(mode.into()),
                }),
                BulkAction::Pause => A::Pause(pb::BulkNoArgs {}),
                BulkAction::Resume => A::Resume(pb::BulkNoArgs {}),
                BulkAction::Signal { name, payload } => A::Signal(pb::BulkSignal {
                    name,
                    payload_json: serde_json::to_vec(&payload).unwrap_or_default(),
                }),
            }),
        }
    }
}

impl TryFrom<pb::CreateBulkRequest> for BulkCreateRequest {
    type Error = ApiError;

    fn try_from(r: pb::CreateBulkRequest) -> Result<Self, ApiError> {
        let f = r.filter.unwrap_or_default();
        Ok(BulkCreateRequest {
            action: required("action", r.action)?.try_into()?,
            filter: BulkFilter {
                status: f
                    .status
                    .map(|s| status_from_proto("filter.status", s))
                    .transpose()?,
                workflow: f.workflow,
                queue: f.queue,
                tags: f.tags,
                dlq: f.dlq,
                created_after_ms: f.created_after_ms,
                created_before_ms: f.created_before_ms,
            },
            rate_per_sec: r.rate_per_sec,
            max_in_flight: r.max_in_flight,
            dry_run: r.dry_run,
        })
    }
}

impl From<BulkCreateRequest> for pb::CreateBulkRequest {
    fn from(r: BulkCreateRequest) -> Self {
        let f = r.filter;
        pb::CreateBulkRequest {
            action: Some(r.action.into()),
            filter: Some(pb::BulkFilter {
                workflow: f.workflow,
                status: f.status.map(status_to_proto),
                queue: f.queue,
                tags: f.tags,
                dlq: f.dlq,
                created_after_ms: f.created_after_ms,
                created_before_ms: f.created_before_ms,
            }),
            rate_per_sec: r.rate_per_sec,
            max_in_flight: r.max_in_flight,
            dry_run: r.dry_run,
        }
    }
}

impl From<BulkCreateResponse> for pb::CreateBulkResponse {
    fn from(r: BulkCreateResponse) -> Self {
        pb::CreateBulkResponse {
            job_id: r.job_id,
            matched_estimate: r.matched_estimate,
            sample: r.sample,
        }
    }
}

impl From<pb::CreateBulkResponse> for BulkCreateResponse {
    fn from(r: pb::CreateBulkResponse) -> Self {
        BulkCreateResponse {
            job_id: r.job_id,
            matched_estimate: r.matched_estimate,
            sample: r.sample,
        }
    }
}

impl From<BulkJobView> for pb::BulkJob {
    fn from(j: BulkJobView) -> Self {
        pb::BulkJob {
            job_id: j.job_id,
            state: j.state,
            action: j.action,
            matched: j.matched,
            done: j.done,
            skipped: j.skipped,
            failed: j
                .failed
                .into_iter()
                .map(|f| pb::BulkFailure {
                    run_id: f.run_id,
                    reason: f.reason,
                })
                .collect(),
            failed_count: j.failed_count,
            rate_per_sec: j.rate_per_sec,
            created_at_ms: j.created_at_ms,
            started_at_ms: j.started_at_ms,
            finished_at_ms: j.finished_at_ms,
            eta_ms: j.eta_ms,
        }
    }
}

impl From<pb::BulkJob> for BulkJobView {
    fn from(j: pb::BulkJob) -> Self {
        BulkJobView {
            job_id: j.job_id,
            state: j.state,
            action: j.action,
            matched: j.matched,
            done: j.done,
            skipped: j.skipped,
            failed: j
                .failed
                .into_iter()
                .map(|f| BulkFailure {
                    run_id: f.run_id,
                    reason: f.reason,
                })
                .collect(),
            failed_count: j.failed_count,
            rate_per_sec: j.rate_per_sec,
            created_at_ms: j.created_at_ms,
            started_at_ms: j.started_at_ms,
            finished_at_ms: j.finished_at_ms,
            eta_ms: j.eta_ms,
        }
    }
}

impl From<LimitsView> for pb::Limits {
    fn from(l: LimitsView) -> Self {
        pb::Limits {
            limits: l
                .limits
                .into_iter()
                .map(|l| pb::Limit {
                    name: l.name,
                    value: l.value,
                    unit: l.unit,
                    configurable: l.configurable,
                    enforced: l.enforced,
                    error: l.error,
                })
                .collect(),
        }
    }
}

impl From<pb::Limits> for LimitsView {
    fn from(l: pb::Limits) -> Self {
        LimitsView {
            limits: l
                .limits
                .into_iter()
                .map(|l| LimitView {
                    name: l.name,
                    value: l.value,
                    unit: l.unit,
                    configurable: l.configurable,
                    enforced: l.enforced,
                    error: l.error,
                })
                .collect(),
        }
    }
}

impl From<FailurePolicy> for pb::FailurePolicy {
    fn from(p: FailurePolicy) -> Self {
        pb::FailurePolicy {
            on_failure: p.on_failure.map(|o| pb::OnFailurePolicy {
                workflow: o.workflow,
                queue: o.queue,
                include_input: o.include_input,
            }),
            dlq: p.dlq.map(|d| pb::DlqPolicy {
                enabled: d.enabled,
                hook_catchup_ms: d.hook_catchup_ms,
            }),
            version_tag: p.version_tag,
        }
    }
}

impl From<pb::FailurePolicy> for FailurePolicy {
    fn from(p: pb::FailurePolicy) -> Self {
        FailurePolicy {
            on_failure: p.on_failure.map(|o| OnFailurePolicy {
                workflow: o.workflow,
                queue: o.queue,
                include_input: o.include_input,
            }),
            dlq: p.dlq.map(|d| DlqPolicyDto {
                enabled: d.enabled,
                hook_catchup_ms: d.hook_catchup_ms,
            }),
            version_tag: p.version_tag,
        }
    }
}

impl From<ModulePage> for pb::ListModulesResponse {
    fn from(p: ModulePage) -> Self {
        pb::ListModulesResponse {
            modules: p
                .modules
                .into_iter()
                .map(|m| pb::ModuleSummary {
                    sha256: m.sha256,
                    size_bytes: m.size_bytes,
                    uploaded_at_ms: m.uploaded_at_ms,
                    workflows: m.workflows,
                })
                .collect(),
            next_page_token: p.next_page_token,
        }
    }
}

impl From<pb::ListModulesResponse> for ModulePage {
    fn from(p: pb::ListModulesResponse) -> Self {
        ModulePage {
            modules: p
                .modules
                .into_iter()
                .map(|m| ModuleSummary {
                    sha256: m.sha256,
                    size_bytes: m.size_bytes,
                    uploaded_at_ms: m.uploaded_at_ms,
                    workflows: m.workflows,
                })
                .collect(),
            next_page_token: p.next_page_token,
        }
    }
}

impl From<QueueView> for pb::QueueSummary {
    fn from(q: QueueView) -> Self {
        pb::QueueSummary {
            name: q.name,
            paused: q.paused,
            ready: q.ready,
            running: q.running,
            demand: q.demand,
            flow_json: q.flow.map(|f| serde_json::to_vec(&f).unwrap_or_default()),
        }
    }
}

impl TryFrom<pb::QueueSummary> for QueueView {
    type Error = ApiError;

    fn try_from(q: pb::QueueSummary) -> Result<Self, ApiError> {
        Ok(QueueView {
            flow: q
                .flow_json
                .map(|b| crate::grpc::payload_from_proto("flow_json", &b))
                .transpose()?,
            name: q.name,
            paused: q.paused,
            ready: q.ready,
            running: q.running,
            demand: q.demand,
        })
    }
}

impl From<PartitionView> for pb::PartitionSummary {
    fn from(p: PartitionView) -> Self {
        pb::PartitionSummary {
            id: u32::from(p.id),
            role: p.role,
            leader: p.leader,
            epoch: p.epoch,
            applied_offset: p.applied_offset,
            log_end_offset: p.log_end_offset,
            ready: p.ready,
            running: p.running,
            healthy: p.healthy,
            detail: p.detail,
        }
    }
}

impl TryFrom<pb::PartitionSummary> for PartitionView {
    type Error = ApiError;

    fn try_from(p: pb::PartitionSummary) -> Result<Self, ApiError> {
        Ok(PartitionView {
            id: u16::try_from(p.id)
                .map_err(|_| ApiError::invalid_argument(format!("id: {} > 65535", p.id)))?,
            role: p.role,
            leader: p.leader,
            epoch: p.epoch,
            applied_offset: p.applied_offset,
            log_end_offset: p.log_end_offset,
            ready: p.ready,
            running: p.running,
            healthy: p.healthy,
            detail: p.detail,
        })
    }
}

fn scope_from_get(
    s: Option<pb::get_failure_policy_request::Scope>,
) -> Result<PolicyScope, ApiError> {
    Ok(match required("scope", s)? {
        pb::get_failure_policy_request::Scope::Workflow(n) => PolicyScope::Workflow(n),
        pb::get_failure_policy_request::Scope::Queue(n) => PolicyScope::Queue(n),
    })
}

fn scope_from_set(
    s: Option<pb::set_failure_policy_request::Scope>,
) -> Result<PolicyScope, ApiError> {
    Ok(match required("scope", s)? {
        pb::set_failure_policy_request::Scope::Workflow(n) => PolicyScope::Workflow(n),
        pb::set_failure_policy_request::Scope::Queue(n) => PolicyScope::Queue(n),
    })
}

type GrpcResult<T> = Result<tonic::Response<T>, tonic::Status>;

fn ok<T>(v: T) -> GrpcResult<T> {
    Ok(tonic::Response::new(v))
}

fn st(e: ApiError) -> tonic::Status {
    crate::grpc::to_status(e)
}

/// `ostro.api.v1.RunOps` over an [`OpsApi`] (same validation as REST).
#[derive(Clone)]
pub struct GrpcRunOps(pub Arc<dyn OpsApi>);

/// `ostro.api.v1.Inventory` over an [`InventoryApi`].
#[derive(Clone)]
pub struct GrpcInventory(pub Arc<dyn InventoryApi>);

#[tonic::async_trait]
impl run_ops_server::RunOps for GrpcRunOps {
    async fn rerun(&self, r: tonic::Request<pb::RerunRequest>) -> GrpcResult<pb::RerunResponse> {
        let req = RerunRequest::try_from(r.into_inner()).map_err(st)?;
        check::rerun(&req).map_err(st)?;
        ok(self.0.rerun_run(req).await.map_err(st)?.into())
    }

    async fn rerun_cuts(
        &self,
        r: tonic::Request<pb::RerunCutsRequest>,
    ) -> GrpcResult<pb::RerunCutsView> {
        let run_id = r.into_inner().run_id;
        validate_run_id(&run_id).map_err(st)?;
        ok(self.0.analyze_rerun(run_id).await.map_err(st)?.into())
    }

    async fn list_dlq(&self, r: tonic::Request<pb::ListDlqRequest>) -> GrpcResult<pb::DlqPage> {
        let req = DlqListRequest::from(r.into_inner());
        check::dlq_list(&req).map_err(st)?;
        ok(self.0.list_dlq(req).await.map_err(st)?.into())
    }

    async fn requeue_dlq(
        &self,
        r: tonic::Request<pb::RequeueDlqRequest>,
    ) -> GrpcResult<pb::RequeueDlqResponse> {
        let r = r.into_inner();
        let req = DlqRequeueRequest {
            run_id: r.run_id,
            mode: mode_from_proto(r.mode).map_err(st)?,
            dry_run: r.dry_run,
        };
        check::requeue(&req).map_err(st)?;
        ok(self.0.requeue_dlq(req).await.map_err(st)?.into())
    }

    async fn dismiss_dlq(
        &self,
        r: tonic::Request<pb::DismissDlqRequest>,
    ) -> GrpcResult<pb::DlqEntry> {
        let r = r.into_inner();
        let req = DlqDismissRequest {
            run_id: r.run_id,
            reason: r.reason,
        };
        check::dismiss(&req).map_err(st)?;
        ok(self.0.dismiss_dlq(req).await.map_err(st)?.into())
    }

    async fn create_bulk(
        &self,
        r: tonic::Request<pb::CreateBulkRequest>,
    ) -> GrpcResult<pb::CreateBulkResponse> {
        let req = BulkCreateRequest::try_from(r.into_inner()).map_err(st)?;
        check::bulk(&req).map_err(st)?;
        ok(self.0.create_bulk(req).await.map_err(st)?.into())
    }

    async fn list_bulk(
        &self,
        _r: tonic::Request<pb::ListBulkRequest>,
    ) -> GrpcResult<pb::ListBulkResponse> {
        let jobs = self.0.list_bulk().await.map_err(st)?;
        ok(pb::ListBulkResponse {
            jobs: jobs.into_iter().map(Into::into).collect(),
        })
    }

    async fn get_bulk(&self, r: tonic::Request<pb::GetBulkRequest>) -> GrpcResult<pb::BulkJob> {
        let id = r.into_inner().job_id;
        check::job_id(&id).map_err(st)?;
        ok(self.0.get_bulk(id).await.map_err(st)?.into())
    }

    async fn cancel_bulk(
        &self,
        r: tonic::Request<pb::CancelBulkRequest>,
    ) -> GrpcResult<pb::BulkJob> {
        let id = r.into_inner().job_id;
        check::job_id(&id).map_err(st)?;
        ok(self.0.cancel_bulk(id).await.map_err(st)?.into())
    }

    async fn get_limits(&self, _r: tonic::Request<pb::GetLimitsRequest>) -> GrpcResult<pb::Limits> {
        ok(self.0.limits().await.map_err(st)?.into())
    }

    async fn get_failure_policy(
        &self,
        r: tonic::Request<pb::GetFailurePolicyRequest>,
    ) -> GrpcResult<pb::FailurePolicy> {
        let scope = scope_from_get(r.into_inner().scope).map_err(st)?;
        check::scope(&scope).map_err(st)?;
        ok(self.0.get_failure_policy(scope).await.map_err(st)?.into())
    }

    async fn set_failure_policy(
        &self,
        r: tonic::Request<pb::SetFailurePolicyRequest>,
    ) -> GrpcResult<pb::FailurePolicy> {
        let r = r.into_inner();
        let scope = scope_from_set(r.scope).map_err(st)?;
        let p = FailurePolicy::from(r.policy.unwrap_or_default());
        check::failure_policy(&scope, &p).map_err(st)?;
        ok(self
            .0
            .set_failure_policy(scope, p)
            .await
            .map_err(st)?
            .into())
    }
}

#[tonic::async_trait]
impl inventory_server::Inventory for GrpcInventory {
    async fn list_modules(
        &self,
        r: tonic::Request<pb::ListModulesRequest>,
    ) -> GrpcResult<pb::ListModulesResponse> {
        let r = r.into_inner();
        let page = self
            .0
            .list_modules(ListModulesRequest {
                limit: r.limit,
                page_token: r.page_token,
            })
            .await
            .map_err(st)?;
        ok(page.into())
    }

    async fn list_queues(
        &self,
        _r: tonic::Request<pb::ListQueuesRequest>,
    ) -> GrpcResult<pb::ListQueuesResponse> {
        let queues = self.0.list_queues().await.map_err(st)?;
        ok(pb::ListQueuesResponse {
            queues: queues.into_iter().map(Into::into).collect(),
        })
    }

    async fn list_partitions(
        &self,
        _r: tonic::Request<pb::ListPartitionsRequest>,
    ) -> GrpcResult<pb::ListPartitionsResponse> {
        let partitions = self.0.list_partitions().await.map_err(st)?;
        ok(pb::ListPartitionsResponse {
            partitions: partitions.into_iter().map(Into::into).collect(),
        })
    }
}

/// Mount `RunOps` / `Inventory` for the slots that are present (absent = not mounted,
/// so clients see `UNIMPLEMENTED`, as for Phase 8).
pub(crate) fn add_grpc_services(
    mut routes: tonic::service::Routes,
    ext: &ApiExtensions,
    max_request_bytes: usize,
) -> tonic::service::Routes {
    if let Some(o) = &ext.ops {
        routes = routes.add_service(
            run_ops_server::RunOpsServer::new(GrpcRunOps(Arc::clone(o)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    if let Some(i) = &ext.inventory {
        routes = routes.add_service(
            inventory_server::InventoryServer::new(GrpcInventory(Arc::clone(i)))
                .max_decoding_message_size(max_request_bytes),
        );
    }
    routes
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
