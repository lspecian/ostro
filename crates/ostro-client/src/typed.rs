//! Typed convenience surface over the raw [`Client`] (SDK-07, 10-09).
//!
//! [`TypedClient`] wraps a [`Client`] and returns typed answers or a [`ClientError`]. The
//! raw client keeps its method names (`start_run`, `get_run`, ... returning
//! [`crate::Response`]), so the typed methods live on their own type rather than on
//! `Client` itself (two inherent methods cannot share a name, and an extension trait would
//! be shadowed by the inherent ones).
//!
//! Every response struct keeps the fields it does not name in `extra` (`#[serde(flatten)]`),
//! so additive server changes never break a client. [`ROUTES`] lists every
//! `(method, path template)` this crate calls; the contract test (`tests/contract.rs`)
//! checks it against the server's OpenAPI document, together with the ROUTES tables of the
//! Python, TypeScript and Go clients.

use std::time::{Duration, Instant};

use reqwest::Method;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::client::{CallError, Client, Response};
use crate::error::ClientError;

/// Every `(METHOD, path template)` the client calls (raw named methods and the typed
/// surface). Path parameter names follow the OpenAPI document; the contract test compares
/// templates with every `{param}` normalised to `{}`.
pub const ROUTES: &[(&str, &str)] = &[
    // modules / workflows (05-14)
    ("PUT", "/v1/modules/{sha256}"),
    ("GET", "/v1/workflows"),
    ("PUT", "/v1/workflows/{name}/active"),
    // runs (05-14)
    ("POST", "/v1/workflows/{name}/runs"),
    ("GET", "/v1/runs"),
    ("GET", "/v1/runs/{run_id}"),
    ("GET", "/v1/runs/{run_id}/history"),
    ("GET", "/v1/runs/{run_id}/result"),
    // lifecycle (05-14)
    ("POST", "/v1/runs/{run_id}/signals/{name}"),
    ("POST", "/v1/runs/{run_id}/cancel"),
    ("POST", "/v1/runs/{run_id}/terminate"),
    ("POST", "/v1/runs/{run_id}/retry"),
    ("POST", "/v1/tokens/complete"),
    // Phase 10 tooling (10-05, REST-only)
    ("GET", "/v1/dev/time"),
    ("POST", "/v1/dev/time/skip"),
    ("POST", "/v1/modules/{sha}/replay"),
    // Phase 11 run operations (11-14)
    ("POST", "/v1/runs/{run_id}/rerun"),
    ("GET", "/v1/runs/{run_id}/rerun-cuts"),
    ("GET", "/v1/dlq"),
    ("POST", "/v1/dlq/{run_id}/requeue"),
    ("POST", "/v1/dlq/{run_id}/dismiss"),
    ("POST", "/v1/bulk"),
    ("GET", "/v1/bulk"),
    ("GET", "/v1/bulk/{job_id}"),
    ("POST", "/v1/bulk/{job_id}/cancel"),
    ("GET", "/v1/limits"),
    // Phase 11 inventory and failure policies (11-04 / 11-09)
    ("GET", "/v1/modules"),
    ("GET", "/v1/queues"),
    ("GET", "/v1/partitions"),
    ("GET", "/v1/workflows/{name}/failure-policy"),
    ("PUT", "/v1/workflows/{name}/failure-policy"),
    ("GET", "/v1/queues/{name}/failure-policy"),
    ("PUT", "/v1/queues/{name}/failure-policy"),
];

/// Unknown fields of a response, kept so additive server changes are visible.
pub type Extra = Map<String, Value>;

// --- requests ---------------------------------------------------------------------------

/// `POST /v1/workflows/{workflow}/runs` request. `workflow` is the path; every other field
/// is the JSON body (unset options are omitted, so the server defaults apply). Fields this
/// struct does not name can be passed through `extra`.
///
/// The server assigns run ids; `idempotency_key` is how a caller makes a start repeatable.
#[derive(Debug, Clone, Default, Serialize)]
pub struct StartRun {
    /// Workflow name (`tenant/name` for a tenant-qualified one).
    #[serde(skip)]
    pub workflow: String,
    /// Workflow input.
    pub input: Value,
    /// Idempotency key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    /// Collision policy for `idempotency_key`: `return_existing` (default) or `reject`.
    #[serde(rename = "conflict_policy", skip_serializing_if = "Option::is_none")]
    pub conflict: Option<String>,
    /// Tags (`key=value` or plain).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    /// Retention after completion, a friendly duration (`72h`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention: Option<String>,
    /// Pinned workflow version (default: the active one).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<u64>,
    /// Queue override.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
    /// Any other body field.
    #[serde(flatten)]
    pub extra: Extra,
}

impl StartRun {
    /// A start of `workflow` with `input` and server defaults for everything else.
    pub fn new(workflow: impl Into<String>, input: Value) -> Self {
        Self {
            workflow: workflow.into(),
            input,
            ..Self::default()
        }
    }
}

/// `GET /v1/runs` filter. Empty fields are not sent.
#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    /// Workflow name.
    pub workflow: Option<String>,
    /// Run status (`running`, `completed`, ...).
    pub status: Option<String>,
    /// Queue name.
    pub queue: Option<String>,
    /// Tags; every one must match.
    pub tags: Vec<String>,
    /// Page size.
    pub limit: Option<u32>,
    /// `next_page_token` of the previous page.
    pub page_token: Option<String>,
}

impl RunFilter {
    fn query(&self) -> Vec<(&'static str, String)> {
        let mut q = Vec::new();
        for (k, v) in [
            ("workflow", &self.workflow),
            ("status", &self.status),
            ("queue", &self.queue),
        ] {
            if let Some(v) = v {
                q.push((k, v.clone()));
            }
        }
        for t in &self.tags {
            q.push(("tag", t.clone()));
        }
        if let Some(l) = self.limit {
            q.push(("limit", l.to_string()));
        }
        if let Some(p) = &self.page_token {
            q.push(("page_token", p.clone()));
        }
        q
    }
}

/// Which runs a server-side replay checks.
#[derive(Debug, Clone)]
pub enum ReplaySelect {
    /// Explicit run ids (1..=1000).
    Runs(Vec<String>),
    /// A sample of up to `n` runs (≤ 1000), optionally narrowed.
    Sample {
        /// Sample size.
        n: u64,
        /// Only runs of this workflow.
        workflow: Option<String>,
        /// Only runs in this status.
        status: Option<String>,
    },
}

/// Target of a failure policy (`/v1/workflows/{name}/failure-policy` or
/// `/v1/queues/{name}/failure-policy`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyTarget {
    /// A workflow.
    Workflow,
    /// A queue.
    Queue,
}

impl PolicyTarget {
    fn segment(self) -> &'static str {
        match self {
            PolicyTarget::Workflow => "workflows",
            PolicyTarget::Queue => "queues",
        }
    }
}

// --- responses --------------------------------------------------------------------------

/// Answer of a start.
#[derive(Debug, Clone, Deserialize)]
pub struct StartedRun {
    /// Run id.
    pub run_id: String,
    /// `false` when an existing run was returned (idempotency key).
    pub created: bool,
    /// Dry run (nothing was started).
    #[serde(default)]
    pub dry_run: bool,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One run (`GET /v1/runs/{id}`, list entries).
#[derive(Debug, Clone, Deserialize)]
pub struct RunInfo {
    /// Run id.
    pub run_id: String,
    /// Workflow name.
    pub workflow: String,
    /// Workflow version.
    pub version: u64,
    /// Module sha256.
    #[serde(default)]
    pub module_sha256: String,
    /// Queue.
    #[serde(default)]
    pub queue: String,
    /// Status (`scheduled`, `running`, `completed`, `failed`, ...).
    pub status: String,
    /// Tags.
    #[serde(default)]
    pub tags: Vec<String>,
    /// Workflow output once completed.
    #[serde(default)]
    pub result: Option<Value>,
    /// Failure once failed.
    #[serde(default)]
    pub failure: Option<Value>,
    /// Creation time (ms since the epoch).
    #[serde(default)]
    pub created_at_ms: Option<i64>,
    /// Completion time (ms since the epoch).
    #[serde(default)]
    pub completed_at_ms: Option<i64>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One page of `GET /v1/runs`.
#[derive(Debug, Clone, Deserialize)]
pub struct RunList {
    /// Runs.
    pub runs: Vec<RunInfo>,
    /// Token of the next page.
    #[serde(default)]
    pub next_page_token: Option<String>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One page of `GET /v1/runs/{id}/history`.
#[derive(Debug, Clone, Deserialize)]
pub struct HistoryPage {
    /// Events (`{seq, kind, details, ...}`).
    pub events: Vec<Value>,
    /// `from_seq` of the next page; `None` at the end.
    #[serde(default, rename = "next_from_seq")]
    pub next: Option<u64>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// `GET /v1/runs/{id}/result`.
#[derive(Debug, Clone, Deserialize)]
pub struct RunResult {
    /// Run id.
    #[serde(default)]
    pub run_id: String,
    /// `false` when the wait ended before the run did.
    pub completed: bool,
    /// Run status.
    #[serde(default)]
    pub status: Option<String>,
    /// Workflow output (wire field `result`).
    #[serde(default, rename = "result")]
    pub output: Option<Value>,
    /// Failure.
    #[serde(default)]
    pub failure: Option<Value>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Answer of a signal.
#[derive(Debug, Clone, Deserialize)]
pub struct SignalOutcome {
    /// Accepted for delivery.
    pub accepted: bool,
    /// The request id was seen before.
    #[serde(default)]
    pub duplicate: bool,
    /// Dry run.
    #[serde(default)]
    pub dry_run: bool,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Answer of a cancel.
#[derive(Debug, Clone, Deserialize)]
pub struct CancelOutcome {
    /// A cancel was already requested.
    pub already_requested: bool,
    /// Dry run.
    #[serde(default)]
    pub dry_run: bool,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Answer of a terminate.
#[derive(Debug, Clone, Deserialize)]
pub struct TerminateOutcome {
    /// Run status after the call.
    pub status: String,
    /// Dry run.
    #[serde(default)]
    pub dry_run: bool,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Answer of a retry.
#[derive(Debug, Clone, Deserialize)]
pub struct RetryOutcome {
    /// `resumed` or `restarted`.
    pub mode: String,
    /// Run id.
    pub run_id: String,
    /// Dry run.
    #[serde(default)]
    pub dry_run: bool,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Answer of a token completion.
#[derive(Debug, Clone, Deserialize)]
pub struct TokenOutcome {
    /// Run the token belongs to.
    pub run_id: String,
    /// Accepted.
    pub accepted: bool,
    /// Dry run.
    #[serde(default)]
    pub dry_run: bool,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// A workflow version inside a module.
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowVersion {
    /// Workflow name.
    pub name: String,
    /// Version.
    pub version: u64,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// An uploaded module.
#[derive(Debug, Clone, Deserialize)]
pub struct ModuleInfo {
    /// sha256 (lowercase hex).
    pub sha256: String,
    /// Size in bytes.
    #[serde(default)]
    pub size_bytes: u64,
    /// Workflows the module provides.
    #[serde(default)]
    pub workflows: Vec<WorkflowVersion>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Answer of a module upload.
#[derive(Debug, Clone, Deserialize)]
pub struct UploadOutcome {
    /// `false` when the module was already stored (same sha).
    pub created: bool,
    /// The module.
    pub module: ModuleInfo,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One workflow (`GET /v1/workflows` entries, activate answer).
#[derive(Debug, Clone, Deserialize)]
pub struct WorkflowInfo {
    /// Name.
    pub name: String,
    /// Active version.
    #[serde(default)]
    pub active_version: Option<u64>,
    /// Versions.
    #[serde(default)]
    pub versions: Vec<Value>,
    /// Record version (optimistic concurrency).
    #[serde(default)]
    pub record_version: u64,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

#[derive(Debug, Deserialize)]
struct WorkflowList {
    workflows: Vec<WorkflowInfo>,
}

/// `GET /v1/dev/time`.
#[derive(Debug, Clone, Deserialize)]
pub struct DevTime {
    /// Server clock (ms since the epoch, offset included).
    pub now_ms: i64,
    /// Total skipped time.
    pub offset_ms: i64,
    /// `off`, `auto` or `manual`.
    pub mode: String,
    /// No run is runnable right now.
    pub idle: bool,
    /// Next due timer, if any.
    #[serde(default)]
    pub next_due_ms: Option<i64>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// `POST /v1/dev/time/skip`.
#[derive(Debug, Clone, Deserialize)]
pub struct DevSkip {
    /// Server clock after the skip.
    pub now_ms: i64,
    /// Total skipped time.
    pub offset_ms: i64,
    /// Time skipped by this call.
    pub skipped_ms: i64,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// One replay verdict.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplayVerdict {
    /// Run id.
    pub run_id: String,
    /// Workflow.
    #[serde(default)]
    pub workflow: String,
    /// `ok`, `ok_frontier`, `diverged`, `skipped` or `error`.
    pub verdict: String,
    /// Divergence sequence number.
    #[serde(default)]
    pub seq: Option<u64>,
    /// Detail.
    #[serde(default)]
    pub detail: Option<String>,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

/// Replay counts per verdict.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplaySummary {
    /// Replayed identically.
    pub ok: u64,
    /// Replayed identically up to the journal frontier.
    pub ok_frontier: u64,
    /// Diverged.
    pub diverged: u64,
    /// Skipped.
    pub skipped: u64,
    /// Errors.
    pub error: u64,
}

/// `POST /v1/modules/{sha}/replay`.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplayReport {
    /// Module replayed against.
    pub module_sha256: String,
    /// Verdicts.
    pub verdicts: Vec<ReplayVerdict>,
    /// Counts.
    pub summary: ReplaySummary,
    /// Other fields.
    #[serde(flatten)]
    pub extra: Extra,
}

// --- the typed client -------------------------------------------------------------------

/// Typed client: the [`Client`] calls with typed answers and [`ClientError`].
#[derive(Debug, Clone)]
pub struct TypedClient {
    raw: Client,
}

impl Client {
    /// The typed surface over this client.
    pub fn into_typed(self) -> TypedClient {
        TypedClient { raw: self }
    }
}

fn decode<T: DeserializeOwned>(
    r: Result<Response, CallError>,
    what: &str,
) -> Result<T, ClientError> {
    let r = r?;
    serde_json::from_value(r.body).map_err(|e| ClientError::decode(r.status, what, &e))
}

fn raw(r: Result<Response, CallError>) -> Result<Value, ClientError> {
    Ok(r?.body)
}

fn sha256_hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    Sha256::digest(b)
        .iter()
        .fold(String::with_capacity(64), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

impl TypedClient {
    /// A typed client for `server` (see [`Client::new`]).
    pub fn new(server: &str, timeout: Duration) -> Result<Self, ClientError> {
        Ok(Client::new(server, timeout)?.into_typed())
    }

    /// The raw client.
    pub fn raw(&self) -> &Client {
        &self.raw
    }

    // --- modules / workflows ------------------------------------------------------------

    /// Upload a Wasm module (its sha256 is computed here). Idempotent: the same bytes
    /// answer `created: false`.
    pub async fn upload_module(
        &self,
        wasm: Vec<u8>,
        name: Option<&str>,
        tenant: Option<&str>,
    ) -> Result<UploadOutcome, ClientError> {
        let sha = sha256_hex(&wasm);
        decode(
            self.raw.upload_module(&sha, wasm, name, tenant).await,
            "upload",
        )
    }

    /// Make `version` the active version of `workflow`.
    pub async fn activate(
        &self,
        workflow: &str,
        version: u64,
    ) -> Result<WorkflowInfo, ClientError> {
        decode(self.raw.activate(workflow, version).await, "activate")
    }

    /// Every workflow.
    pub async fn workflows(&self) -> Result<Vec<WorkflowInfo>, ClientError> {
        let l: WorkflowList = decode(self.raw.list_workflows().await, "workflows")?;
        Ok(l.workflows)
    }

    // --- runs ---------------------------------------------------------------------------

    /// Start a run.
    pub async fn start_run(&self, req: &StartRun) -> Result<StartedRun, ClientError> {
        let body = serde_json::to_value(req).map_err(|e| ClientError {
            status: None,
            code: "usage".to_owned(),
            message: format!("cannot encode the start request: {e}"),
            details: Value::Null,
        })?;
        decode(self.raw.start_run(&req.workflow, &body).await, "start")
    }

    /// One run.
    pub async fn get_run(&self, run_id: &str) -> Result<RunInfo, ClientError> {
        decode(self.raw.get_run(run_id).await, "run")
    }

    /// One page of runs.
    pub async fn list_runs(&self, filter: &RunFilter) -> Result<RunList, ClientError> {
        decode(self.raw.list_runs(&filter.query()).await, "run list")
    }

    /// One page of a run's history.
    pub async fn history(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
        limit: Option<u32>,
    ) -> Result<HistoryPage, ClientError> {
        decode(self.raw.history(run_id, from_seq, limit).await, "history")
    }

    /// The run's result, waiting up to `wait` (`None` = answer now) with as many long
    /// polls as needed. `completed: false` means the wait ended first.
    pub async fn result(
        &self,
        run_id: &str,
        wait: Option<Duration>,
    ) -> Result<RunResult, ClientError> {
        let wait = wait.unwrap_or(Duration::ZERO);
        let deadline = Instant::now() + wait;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let r: RunResult = decode(self.raw.result_once(run_id, left, false).await, "result")?;
            if r.completed || left.is_zero() {
                return Ok(r);
            }
        }
    }

    // --- lifecycle ----------------------------------------------------------------------

    /// Send signal `name` with `payload`; `request_id` deduplicates.
    pub async fn signal(
        &self,
        run_id: &str,
        name: &str,
        payload: Value,
        request_id: Option<&str>,
    ) -> Result<SignalOutcome, ClientError> {
        let mut body = json!({ "payload": payload });
        if let Some(id) = request_id {
            body["request_id"] = json!(id);
        }
        decode(self.raw.signal(run_id, name, &body).await, "signal")
    }

    /// Request a cancel.
    pub async fn cancel(
        &self,
        run_id: &str,
        reason: Option<&str>,
    ) -> Result<CancelOutcome, ClientError> {
        decode(
            self.raw.cancel(run_id, &json!({ "reason": reason })).await,
            "cancel",
        )
    }

    /// Terminate now.
    pub async fn terminate(
        &self,
        run_id: &str,
        reason: Option<&str>,
    ) -> Result<TerminateOutcome, ClientError> {
        decode(
            self.raw
                .terminate(run_id, &json!({ "reason": reason }))
                .await,
            "terminate",
        )
    }

    /// Retry a failed run.
    pub async fn retry(&self, run_id: &str) -> Result<RetryOutcome, ClientError> {
        decode(self.raw.retry(run_id, &json!({})).await, "retry")
    }

    /// Complete a token with `payload`.
    pub async fn complete_token(
        &self,
        token: &str,
        payload: Value,
    ) -> Result<TokenOutcome, ClientError> {
        decode(
            self.raw
                .complete_token(&json!({ "token": token, "payload": payload }))
                .await,
            "token completion",
        )
    }

    // --- Phase 10 tooling ---------------------------------------------------------------

    /// The dev clock. 409 `conflict` with reason `time_skipping_disabled` unless the
    /// server runs `ostro dev --ephemeral --time-skipping`.
    pub async fn dev_time(&self) -> Result<DevTime, ClientError> {
        decode(self.raw.dev_time().await, "dev time")
    }

    /// Skip the dev clock forward by `by`.
    pub async fn dev_skip_by(&self, by: Duration) -> Result<DevSkip, ClientError> {
        let body = json!({ "by": format!("{}ms", by.as_millis()) });
        decode(self.raw.dev_skip(&body).await, "dev skip")
    }

    /// Skip the dev clock forward to `to_ms` (ms since the epoch).
    pub async fn dev_skip_to(&self, to_ms: i64) -> Result<DevSkip, ClientError> {
        decode(
            self.raw.dev_skip(&json!({ "to_ms": to_ms })).await,
            "dev skip",
        )
    }

    /// Replay stored journals against module `sha` on the server.
    pub async fn replay(
        &self,
        sha: &str,
        select: &ReplaySelect,
        allow_missing: bool,
    ) -> Result<ReplayReport, ClientError> {
        let mut body = match select {
            ReplaySelect::Runs(runs) => json!({ "runs": runs }),
            ReplaySelect::Sample {
                n,
                workflow,
                status,
            } => {
                let mut b = json!({ "sample": n });
                if let Some(w) = workflow {
                    b["workflow"] = json!(w);
                }
                if let Some(s) = status {
                    b["status"] = json!(s);
                }
                b
            }
        };
        if allow_missing {
            body["allow_missing"] = json!(true);
        }
        decode(self.raw.replay_module(sha, &body).await, "replay")
    }

    // --- Phase 11 operations (raw JSON answers) -----------------------------------------

    /// Re-run from a cut (`{from, version, idempotency_key, dry_run}`).
    pub async fn rerun(&self, run_id: &str, body: &Value) -> Result<Value, ClientError> {
        raw(self.raw.rerun(run_id, body).await)
    }

    /// The cuts a run can be re-run from.
    pub async fn rerun_cuts(&self, run_id: &str) -> Result<Value, ClientError> {
        raw(self.raw.rerun_cuts(run_id).await)
    }

    /// One page of the DLQ (`workflow`, `queue`, `reason`, `tags`, `include_dismissed`,
    /// `limit`, `page_token`).
    pub async fn dlq(&self, query: &[(&str, String)]) -> Result<Value, ClientError> {
        raw(self.raw.list_dlq(query).await)
    }

    /// Requeue a DLQ entry (`{mode, dry_run}`).
    pub async fn requeue_dlq(&self, run_id: &str, body: &Value) -> Result<Value, ClientError> {
        raw(self.raw.requeue_dlq(run_id, body).await)
    }

    /// Dismiss a DLQ entry (`{reason}`).
    pub async fn dismiss_dlq(&self, run_id: &str, body: &Value) -> Result<Value, ClientError> {
        raw(self.raw.dismiss_dlq(run_id, body).await)
    }

    /// Create a bulk job (`{action, filter, rate_per_sec, max_in_flight, dry_run}`).
    pub async fn create_bulk(&self, body: &Value) -> Result<Value, ClientError> {
        raw(self.raw.create_bulk(body).await)
    }

    /// Every bulk job.
    pub async fn bulk_jobs(&self) -> Result<Value, ClientError> {
        raw(self.raw.list_bulk().await)
    }

    /// One bulk job.
    pub async fn bulk_job(&self, job_id: &str) -> Result<Value, ClientError> {
        raw(self.raw.get_bulk(job_id).await)
    }

    /// Cancel a bulk job.
    pub async fn cancel_bulk(&self, job_id: &str) -> Result<Value, ClientError> {
        raw(self.raw.cancel_bulk(job_id).await)
    }

    /// Effective limits.
    pub async fn limits(&self) -> Result<Value, ClientError> {
        raw(self.raw.limits().await)
    }

    /// One page of modules.
    pub async fn modules(
        &self,
        limit: Option<u32>,
        page_token: Option<&str>,
    ) -> Result<Value, ClientError> {
        let mut q = Vec::new();
        if let Some(l) = limit {
            q.push(("limit", l.to_string()));
        }
        if let Some(p) = page_token {
            q.push(("page_token", p.to_owned()));
        }
        raw(self
            .raw
            .call(Method::GET, &["v1", "modules"], &q, None)
            .await)
    }

    /// Every queue.
    pub async fn queues(&self) -> Result<Value, ClientError> {
        raw(self
            .raw
            .call(Method::GET, &["v1", "queues"], &[], None)
            .await)
    }

    /// Every partition.
    pub async fn partitions(&self) -> Result<Value, ClientError> {
        raw(self
            .raw
            .call(Method::GET, &["v1", "partitions"], &[], None)
            .await)
    }

    /// The failure policy of a workflow or queue.
    pub async fn failure_policy(
        &self,
        target: PolicyTarget,
        name: &str,
    ) -> Result<Value, ClientError> {
        raw(self
            .raw
            .call(
                Method::GET,
                &["v1", target.segment(), name, "failure-policy"],
                &[],
                None,
            )
            .await)
    }

    /// Replace the failure policy of a workflow or queue; `version_tag` makes the write
    /// conditional.
    pub async fn set_failure_policy(
        &self,
        target: PolicyTarget,
        name: &str,
        policy: &Value,
        version_tag: Option<&str>,
    ) -> Result<Value, ClientError> {
        let q: Vec<(&str, String)> = version_tag
            .map(|t| vec![("version_tag", t.to_owned())])
            .unwrap_or_default();
        raw(self
            .raw
            .call(
                Method::PUT,
                &["v1", target.segment(), name, "failure-policy"],
                &q,
                Some(policy),
            )
            .await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn start_body_omits_defaults() {
        let mut s = StartRun::new("w", json!({"a": 1}));
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            json!({"input": {"a": 1}})
        );
        s.conflict = Some("reject".into());
        s.tags = vec!["k=v".into()];
        s.extra.insert("priority".into(), json!(3));
        assert_eq!(
            serde_json::to_value(&s).unwrap(),
            json!({"input": {"a": 1}, "conflict_policy": "reject", "tags": ["k=v"], "priority": 3})
        );
    }

    #[test]
    fn unknown_fields_are_kept() {
        let r: RunResult = serde_json::from_value(json!({
            "run_id": "r", "completed": true, "status": "completed", "result": 7, "new_field": 1
        }))
        .unwrap();
        assert_eq!(r.output, Some(json!(7)));
        assert_eq!(r.extra["new_field"], 1);
    }

    #[test]
    fn filter_query() {
        let f = RunFilter {
            workflow: Some("w".into()),
            tags: vec!["a".into(), "b".into()],
            limit: Some(5),
            ..RunFilter::default()
        };
        assert_eq!(
            f.query(),
            vec![
                ("workflow", "w".to_owned()),
                ("tag", "a".to_owned()),
                ("tag", "b".to_owned()),
                ("limit", "5".to_owned())
            ]
        );
    }

    #[test]
    fn sha_is_lowercase_hex() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }
}
