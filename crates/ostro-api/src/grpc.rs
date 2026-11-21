//! gRPC adapter: `ostro.api.v1` services over [`ControlPlane`].
//!
//! [`GrpcModules`], [`GrpcWorkflows`], [`GrpcRuns`], [`GrpcTokens`], [`GrpcSecrets`],
//! [`GrpcConnectors`], [`GrpcQueues`] and [`GrpcWorkers`] are thin translators, like the
//! REST handlers: proto request -> domain request -> `ControlPlane` -> proto response.
//! Errors go through [`to_status`], which takes the code from the shared
//! [`ApiError::grpc_code`] table. Every proto <-> domain conversion lives in this file.

use std::sync::Arc;

use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{
    connectors_server, modules_server, queues_server, runs_server, secrets_server, tokens_server,
    workers_server, workflows_server,
};
use tonic::{Request, Response, Status};

use crate::domain::{self, ControlPlane};
use crate::error::ApiError;
use crate::phase7;

/// `ApiError` -> `tonic::Status`: code from the shared table, the message, and the
/// structured details (if any) as JSON bytes in the status details.
pub fn to_status(e: ApiError) -> Status {
    let code = e.grpc_code();
    match &e.details {
        Some(details) => {
            let bytes = serde_json::to_vec(details).unwrap_or_default();
            Status::with_details(code, e.message, bytes.into())
        }
        None => Status::new(code, e.message),
    }
}

/// Decode the JSON details a [`to_status`] status carries (client-side helper).
pub fn status_details(status: &Status) -> Option<serde_json::Value> {
    let d = status.details();
    if d.is_empty() {
        None
    } else {
        serde_json::from_slice(d).ok()
    }
}

/// tonic rejects messages over a server's decode limit with `OUT_OF_RANGE`; the shared
/// table says an over-limit body is `RESOURCE_EXHAUSTED` (REST 413). No [`ApiError`]
/// maps to `OUT_OF_RANGE`, so rewriting it on trailers-only gRPC responses is exact.
pub(crate) async fn map_size_limit_status(
    mut res: axum::response::Response,
) -> axum::response::Response {
    let out_of_range = (tonic::Code::OutOfRange as i32).to_string();
    let headers = res.headers_mut();
    if headers
        .get("grpc-status")
        .is_some_and(|v| v.as_bytes() == out_of_range.as_bytes())
    {
        let code = (crate::error::ErrorCode::PayloadTooLarge.grpc_code() as i32).to_string();
        if let Ok(v) = axum::http::HeaderValue::from_str(&code) {
            headers.insert("grpc-status", v);
        }
    }
    res
}

// ---------------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------------

/// Proto `RunStatus` (i32) -> domain; `UNSPECIFIED` and unknown values are invalid.
fn run_status_from_proto(field: &str, v: i32) -> Result<domain::RunStatus, ApiError> {
    use domain::RunStatus as S;
    match pb::RunStatus::try_from(v) {
        Ok(pb::RunStatus::Scheduled) => Ok(S::Scheduled),
        Ok(pb::RunStatus::Ready) => Ok(S::Ready),
        Ok(pb::RunStatus::Running) => Ok(S::Running),
        Ok(pb::RunStatus::Completed) => Ok(S::Completed),
        Ok(pb::RunStatus::Failed) => Ok(S::Failed),
        Ok(pb::RunStatus::Cancelled) => Ok(S::Cancelled),
        Ok(pb::RunStatus::Terminated) => Ok(S::Terminated),
        Ok(pb::RunStatus::TimedOut) => Ok(S::TimedOut),
        Ok(pb::RunStatus::ContinuedAsNew) => Ok(S::ContinuedAsNew),
        Ok(pb::RunStatus::Unspecified) | Err(_) => Err(ApiError::invalid_argument(format!(
            "{field}: invalid run status {v}"
        ))),
    }
}

impl From<domain::RunStatus> for pb::RunStatus {
    fn from(s: domain::RunStatus) -> Self {
        use domain::RunStatus as S;
        match s {
            S::Scheduled => pb::RunStatus::Scheduled,
            S::Ready => pb::RunStatus::Ready,
            S::Running => pb::RunStatus::Running,
            S::Completed => pb::RunStatus::Completed,
            S::Failed => pb::RunStatus::Failed,
            S::Cancelled => pb::RunStatus::Cancelled,
            S::Terminated => pb::RunStatus::Terminated,
            S::TimedOut => pb::RunStatus::TimedOut,
            S::ContinuedAsNew => pb::RunStatus::ContinuedAsNew,
        }
    }
}

fn conflict_policy_from_proto(v: i32) -> Result<domain::ConflictPolicy, ApiError> {
    match pb::ConflictPolicy::try_from(v) {
        Ok(pb::ConflictPolicy::ReturnExisting) => Ok(domain::ConflictPolicy::ReturnExisting),
        Ok(pb::ConflictPolicy::Reject) => Ok(domain::ConflictPolicy::Reject),
        Err(_) => Err(ApiError::invalid_argument(format!(
            "conflict_policy: invalid value {v}"
        ))),
    }
}

impl From<domain::WorkflowRef> for pb::WorkflowRef {
    fn from(w: domain::WorkflowRef) -> Self {
        pb::WorkflowRef {
            name: w.name,
            version: w.version,
        }
    }
}

impl From<domain::ModuleInfo> for pb::ModuleInfo {
    fn from(m: domain::ModuleInfo) -> Self {
        pb::ModuleInfo {
            sha256: m.sha256,
            size_bytes: m.size_bytes,
            language: m.language,
            workflows: m.workflows.into_iter().map(Into::into).collect(),
            uploaded_at_ms: m.uploaded_at_ms,
        }
    }
}

impl From<domain::WorkflowInfo> for pb::WorkflowInfo {
    fn from(w: domain::WorkflowInfo) -> Self {
        pb::WorkflowInfo {
            name: w.name,
            active_version: w.active_version,
            versions: w
                .versions
                .into_iter()
                .map(|v| pb::WorkflowVersionInfo {
                    version: v.version,
                    module_sha256: v.module_sha256,
                    default_queue: v.default_queue,
                    run_timeout_ms: v.run_timeout_ms,
                })
                .collect(),
            record_version: w.record_version,
        }
    }
}

impl From<domain::RunInfo> for pb::RunInfo {
    fn from(r: domain::RunInfo) -> Self {
        pb::RunInfo {
            run_id: r.run_id,
            workflow: r.workflow,
            version: r.version,
            module_sha256: r.module_sha256,
            queue: r.queue,
            status: pb::RunStatus::from(r.status) as i32,
            attempt: r.attempt,
            tags: r.tags,
            tenant: r.tenant,
            traceparent: r.traceparent,
            priority: r.priority,
            created_at_ms: r.created_at_ms,
            completed_at_ms: r.completed_at_ms,
            published_status_json: r.published_status_json,
            result_json: r.result_json,
            failure: r.failure,
            concurrency_key: r.concurrency_key,
            attempt_failures: r.attempt_failures,
            retention_ms: r.retention_ms,
            schedule_to_start_ms: r.schedule_to_start_ms,
        }
    }
}

impl From<domain::HistoryPage> for pb::RunHistoryResponse {
    fn from(p: domain::HistoryPage) -> Self {
        pb::RunHistoryResponse {
            events: p
                .events
                .into_iter()
                .map(|e| pb::HistoryEvent {
                    seq: e.seq,
                    kind: e.kind,
                    ts_ms: e.ts_ms,
                    summary: e.summary,
                    details_json: e.details_json,
                })
                .collect(),
            next_from_seq: p.next_from_seq,
        }
    }
}

impl From<domain::RunPage> for pb::ListRunsResponse {
    fn from(p: domain::RunPage) -> Self {
        pb::ListRunsResponse {
            runs: p
                .runs
                .into_iter()
                .map(|r| pb::RunSummary {
                    run_id: r.run_id,
                    workflow: r.workflow,
                    version: r.version,
                    status: pb::RunStatus::from(r.status) as i32,
                    queue: r.queue,
                    created_at_ms: r.created_at_ms,
                    tags: r.tags,
                })
                .collect(),
            next_page_token: p.next_page_token,
        }
    }
}

impl TryFrom<pb::StartRunRequest> for domain::StartRunRequest {
    type Error = ApiError;

    /// Empty `input_json` means JSON `null` (the REST default); anything else must be
    /// valid UTF-8 JSON and is passed through unchanged.
    fn try_from(r: pb::StartRunRequest) -> Result<Self, ApiError> {
        let conflict_policy = conflict_policy_from_proto(r.conflict_policy)?;
        let input_json = if r.input_json.is_empty() {
            b"null".to_vec()
        } else {
            serde_json::from_slice::<serde::de::IgnoredAny>(&r.input_json)
                .map_err(|e| ApiError::invalid_argument(format!("input_json: {e}")))?;
            r.input_json
        };
        Ok(domain::StartRunRequest {
            workflow: r.workflow,
            input_json,
            version: r.version,
            queue: r.queue,
            delay_ms: r.delay_ms,
            priority: r.priority,
            tags: r.tags,
            traceparent: r.traceparent,
            tenant: r.tenant,
            idempotency_key: r.idempotency_key,
            conflict_policy,
            concurrency_key: r.concurrency_key,
            schedule_to_start_ms: r.schedule_to_start_timeout_ms,
            retention_ms: r.retention_ms,
            dry_run: r.dry_run,
        })
    }
}

impl TryFrom<pb::ListRunsRequest> for domain::ListRunsRequest {
    type Error = ApiError;

    fn try_from(r: pb::ListRunsRequest) -> Result<Self, ApiError> {
        let status = r
            .status
            .map(|s| run_status_from_proto("status", s))
            .transpose()?;
        Ok(domain::ListRunsRequest {
            workflow: r.workflow,
            status,
            queue: r.queue,
            tag: r.tag,
            created_after_ms: r.created_after_ms,
            created_before_ms: r.created_before_ms,
            limit: domain::effective_limit(r.limit),
            page_token: r.page_token,
        })
    }
}

impl From<pb::RunHistoryRequest> for domain::HistoryRequest {
    fn from(r: pb::RunHistoryRequest) -> Self {
        domain::HistoryRequest {
            run_id: r.run_id,
            from_seq: r.from_seq,
            limit: domain::effective_limit(r.limit),
        }
    }
}

/// Optional JSON payload bytes -> value: empty = JSON null (like `input_json`).
fn payload_from_proto(field: &str, bytes: &[u8]) -> Result<serde_json::Value, ApiError> {
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(bytes).map_err(|e| ApiError::invalid_argument(format!("{field}: {e}")))
}

fn json_bytes(v: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(v).unwrap_or_default()
}

impl TryFrom<pb::SignalRunRequest> for domain::SignalRequest {
    type Error = ApiError;

    fn try_from(r: pb::SignalRunRequest) -> Result<Self, ApiError> {
        Ok(domain::SignalRequest {
            payload: payload_from_proto("payload_json", &r.payload_json)?,
            run_id: r.run_id,
            name: r.name,
            request_id: r.request_id,
            dry_run: r.dry_run,
        })
    }
}

impl TryFrom<pb::CompleteTokenRequest> for domain::CompleteTokenRequest {
    type Error = ApiError;

    fn try_from(r: pb::CompleteTokenRequest) -> Result<Self, ApiError> {
        Ok(domain::CompleteTokenRequest {
            payload: payload_from_proto("payload_json", &r.payload_json)?,
            token: r.token,
            dry_run: r.dry_run,
        })
    }
}

impl From<domain::RetryMode> for pb::RetryMode {
    fn from(m: domain::RetryMode) -> Self {
        match m {
            domain::RetryMode::Resumed => pb::RetryMode::Resumed,
            domain::RetryMode::Restarted => pb::RetryMode::Restarted,
        }
    }
}

impl From<domain::AwaitResultResponse> for pb::AwaitResultResponse {
    fn from(r: domain::AwaitResultResponse) -> Self {
        pb::AwaitResultResponse {
            run_id: r.run_id,
            status: pb::RunStatus::from(r.status) as i32,
            completed: r.completed,
            result_json: r.result.as_ref().map(json_bytes),
            failure_json: r.failure.as_ref().map(json_bytes),
        }
    }
}

impl From<pb::RunPolicy> for domain::RunPolicy {
    fn from(p: pb::RunPolicy) -> Self {
        domain::RunPolicy {
            retention_ms: p.retention_ms,
            schedule_to_start_ms: p.schedule_to_start_ms,
        }
    }
}

impl From<domain::RunPolicy> for pb::RunPolicy {
    fn from(p: domain::RunPolicy) -> Self {
        pb::RunPolicy {
            retention_ms: p.retention_ms,
            schedule_to_start_ms: p.schedule_to_start_ms,
        }
    }
}

/// The `traceparent` gRPC metadata value (`None` when absent; non-ASCII is invalid).
fn traceparent_metadata(md: &tonic::metadata::MetadataMap) -> Result<Option<String>, ApiError> {
    md.get(crate::phase8::TRACEPARENT_HEADER)
        .map(|v| {
            v.to_str()
                .map(|s| s.trim().to_owned())
                .map_err(|_| crate::phase8::invalid_traceparent("metadata is not ASCII"))
        })
        .transpose()
}

// ---------------------------------------------------------------------------------
// Services
// ---------------------------------------------------------------------------------

/// `ostro.api.v1.Modules` over a [`ControlPlane`].
///
/// Enforces `max_module_bytes` on the module bytes itself (ResourceExhausted, like
/// REST's 413), so the limit is exact even though tonic's decode limit must leave
/// headroom for the rest of the message.
#[derive(Clone)]
pub struct GrpcModules {
    cp: Arc<dyn ControlPlane>,
    max_module_bytes: usize,
}

impl GrpcModules {
    /// Modules service over `cp` accepting modules up to `max_module_bytes`.
    pub fn new(cp: Arc<dyn ControlPlane>, max_module_bytes: usize) -> Self {
        GrpcModules {
            cp,
            max_module_bytes,
        }
    }
}

/// `ostro.api.v1.Workflows` over a [`ControlPlane`].
#[derive(Clone)]
pub struct GrpcWorkflows(pub Arc<dyn ControlPlane>);

/// `ostro.api.v1.Runs` over a [`ControlPlane`].
#[derive(Clone)]
pub struct GrpcRuns(pub Arc<dyn ControlPlane>);

/// `ostro.api.v1.Tokens` (await-token completion) over a [`ControlPlane`].
#[derive(Clone)]
pub struct GrpcTokens(pub Arc<dyn ControlPlane>);

#[tonic::async_trait]
impl modules_server::Modules for GrpcModules {
    async fn upload(
        &self,
        request: Request<pb::UploadModuleRequest>,
    ) -> Result<Response<pb::UploadModuleResponse>, Status> {
        let r = request.into_inner();
        if r.wasm.len() > self.max_module_bytes {
            return Err(to_status(ApiError::payload_too_large(format!(
                "module is {} bytes, api.max_module_bytes is {}",
                r.wasm.len(),
                self.max_module_bytes
            ))));
        }
        let res = self
            .cp
            .upload_module(domain::UploadModule {
                sha256: r.sha256,
                wasm: r.wasm.into(),
                language: r.language,
                tenant: r.tenant,
                name: r.name,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::UploadModuleResponse {
            module: Some(res.module.into()),
            created: res.created,
        }))
    }

    async fn get(
        &self,
        request: Request<pb::GetModuleRequest>,
    ) -> Result<Response<pb::ModuleInfo>, Status> {
        let m = self
            .cp
            .get_module(request.into_inner().sha256)
            .await
            .map_err(to_status)?;
        Ok(Response::new(m.into()))
    }
}

#[tonic::async_trait]
impl workflows_server::Workflows for GrpcWorkflows {
    async fn list(
        &self,
        _request: Request<pb::ListWorkflowsRequest>,
    ) -> Result<Response<pb::ListWorkflowsResponse>, Status> {
        let workflows = self.0.list_workflows().await.map_err(to_status)?;
        Ok(Response::new(pb::ListWorkflowsResponse {
            workflows: workflows.into_iter().map(Into::into).collect(),
        }))
    }

    async fn activate(
        &self,
        request: Request<pb::ActivateWorkflowRequest>,
    ) -> Result<Response<pb::WorkflowInfo>, Status> {
        let r = request.into_inner();
        let wf = self
            .0
            .activate_workflow(domain::ActivateWorkflow {
                name: r.name,
                version: r.version,
                expected_version: r.expected_version,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(wf.into()))
    }

    async fn set_policy(
        &self,
        request: Request<pb::SetWorkflowPolicyRequest>,
    ) -> Result<Response<pb::RunPolicy>, Status> {
        let r = request.into_inner();
        let p = self
            .0
            .set_workflow_policy(r.name, r.policy.unwrap_or_default().into())
            .await
            .map_err(to_status)?;
        Ok(Response::new(p.into()))
    }

    async fn get_policy(
        &self,
        request: Request<pb::GetWorkflowPolicyRequest>,
    ) -> Result<Response<pb::RunPolicy>, Status> {
        let p = self
            .0
            .get_workflow_policy(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(p.into()))
    }

    async fn set_queue_policy(
        &self,
        request: Request<pb::SetQueuePolicyRequest>,
    ) -> Result<Response<pb::RunPolicy>, Status> {
        let r = request.into_inner();
        let p = self
            .0
            .set_queue_policy(r.name, r.policy.unwrap_or_default().into())
            .await
            .map_err(to_status)?;
        Ok(Response::new(p.into()))
    }

    async fn get_queue_policy(
        &self,
        request: Request<pb::GetQueuePolicyRequest>,
    ) -> Result<Response<pb::RunPolicy>, Status> {
        let p = self
            .0
            .get_queue_policy(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(p.into()))
    }

    // Phase 7 (07-11).

    async fn pause_workflow(
        &self,
        request: Request<pb::PauseWorkflowRequest>,
    ) -> Result<Response<pb::WorkflowPauseState>, Status> {
        let st = self
            .0
            .pause_workflow(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(workflow_pause_state(st)))
    }

    async fn resume_workflow(
        &self,
        request: Request<pb::ResumeWorkflowRequest>,
    ) -> Result<Response<pb::WorkflowPauseState>, Status> {
        let st = self
            .0
            .resume_workflow(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(workflow_pause_state(st)))
    }
}

#[tonic::async_trait]
impl runs_server::Runs for GrpcRuns {
    async fn start(
        &self,
        request: Request<pb::StartRunRequest>,
    ) -> Result<Response<pb::StartRunResponse>, Status> {
        let header = traceparent_metadata(request.metadata()).map_err(to_status)?;
        let mut req = domain::StartRunRequest::try_from(request.into_inner()).map_err(to_status)?;
        // Body wins; else the `traceparent` metadata (08-10). Validated either way.
        req.traceparent =
            crate::phase8::resolve_traceparent(req.traceparent.take(), header.as_deref())
                .map_err(to_status)?;
        let res = self.0.start_run(req).await.map_err(to_status)?;
        Ok(Response::new(pb::StartRunResponse {
            run_id: res.run_id,
            created: res.created,
            dry_run: res.dry_run,
        }))
    }

    async fn get(
        &self,
        request: Request<pb::GetRunRequest>,
    ) -> Result<Response<pb::RunInfo>, Status> {
        let run = self
            .0
            .get_run(request.into_inner().run_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(run.into()))
    }

    async fn history(
        &self,
        request: Request<pb::RunHistoryRequest>,
    ) -> Result<Response<pb::RunHistoryResponse>, Status> {
        let page = self
            .0
            .run_history(request.into_inner().into())
            .await
            .map_err(to_status)?;
        Ok(Response::new(page.into()))
    }

    async fn list(
        &self,
        request: Request<pb::ListRunsRequest>,
    ) -> Result<Response<pb::ListRunsResponse>, Status> {
        let req = domain::ListRunsRequest::try_from(request.into_inner()).map_err(to_status)?;
        let page = self.0.list_runs(req).await.map_err(to_status)?;
        Ok(Response::new(page.into()))
    }

    async fn signal(
        &self,
        request: Request<pb::SignalRunRequest>,
    ) -> Result<Response<pb::SignalRunResponse>, Status> {
        let req = domain::SignalRequest::try_from(request.into_inner()).map_err(to_status)?;
        let r = self.0.signal_run(req).await.map_err(to_status)?;
        Ok(Response::new(pb::SignalRunResponse {
            accepted: r.accepted,
            duplicate: r.duplicate,
            dry_run: r.dry_run,
        }))
    }

    async fn cancel(
        &self,
        request: Request<pb::CancelRunRequest>,
    ) -> Result<Response<pb::CancelRunResponse>, Status> {
        let r = request.into_inner();
        let res = self
            .0
            .cancel_run(domain::CancelRequest {
                run_id: r.run_id,
                reason: r.reason,
                dry_run: r.dry_run,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::CancelRunResponse {
            already_requested: res.already_requested,
            dry_run: res.dry_run,
        }))
    }

    async fn terminate(
        &self,
        request: Request<pb::TerminateRunRequest>,
    ) -> Result<Response<pb::TerminateRunResponse>, Status> {
        let r = request.into_inner();
        let res = self
            .0
            .terminate_run(domain::TerminateRequest {
                run_id: r.run_id,
                reason: r.reason,
                dry_run: r.dry_run,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::TerminateRunResponse {
            status: pb::RunStatus::from(res.status) as i32,
            dry_run: res.dry_run,
        }))
    }

    async fn retry(
        &self,
        request: Request<pb::RetryRunRequest>,
    ) -> Result<Response<pb::RetryRunResponse>, Status> {
        let r = request.into_inner();
        let res = self
            .0
            .retry_run(domain::RetryRequest {
                run_id: r.run_id,
                dry_run: r.dry_run,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::RetryRunResponse {
            mode: pb::RetryMode::from(res.mode) as i32,
            run_id: res.run_id,
            dry_run: res.dry_run,
        }))
    }

    async fn await_result(
        &self,
        request: Request<pb::AwaitResultRequest>,
    ) -> Result<Response<pb::AwaitResultResponse>, Status> {
        let r = request.into_inner();
        let res = self
            .0
            .await_result(domain::AwaitResultRequest {
                run_id: r.run_id,
                wait_ms: domain::effective_wait_ms(r.wait_ms),
                follow_continuations: r.follow_continuations,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(res.into()))
    }

    // Phase 7 (07-11).

    async fn pause_run(
        &self,
        request: Request<pb::PauseRunRequest>,
    ) -> Result<Response<pb::RunPauseState>, Status> {
        let r = request.into_inner();
        let st = self
            .0
            .pause_run(r.run_id, r.detail)
            .await
            .map_err(to_status)?;
        Ok(Response::new(st.into()))
    }

    async fn resume_run(
        &self,
        request: Request<pb::ResumeRunRequest>,
    ) -> Result<Response<pb::RunPauseState>, Status> {
        let st = self
            .0
            .resume_run(request.into_inner().run_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(st.into()))
    }
}

#[tonic::async_trait]
impl tokens_server::Tokens for GrpcTokens {
    async fn complete(
        &self,
        request: Request<pb::CompleteTokenRequest>,
    ) -> Result<Response<pb::CompleteTokenResponse>, Status> {
        let req =
            domain::CompleteTokenRequest::try_from(request.into_inner()).map_err(to_status)?;
        let r = self.0.complete_token(req).await.map_err(to_status)?;
        Ok(Response::new(pb::CompleteTokenResponse {
            run_id: r.run_id,
            accepted: r.accepted,
            dry_run: r.dry_run,
        }))
    }
}

// ---------------------------------------------------------------------------------
// Phase 6 bridge administration (06-08): ostro.api.v1.{Secrets, Connectors}
// ---------------------------------------------------------------------------------

impl From<domain::SecretInfo> for pb::SecretInfo {
    fn from(s: domain::SecretInfo) -> Self {
        pb::SecretInfo {
            name: s.name,
            key_id: s.key_id,
            tenant: s.tenant,
        }
    }
}

impl From<domain::GrantInfo> for pb::GrantInfo {
    fn from(g: domain::GrantInfo) -> Self {
        pb::GrantInfo {
            id: g.id,
            module: g.module,
            secret: g.secret,
            origins: g.origins,
            body: g.body,
        }
    }
}

impl From<pb::GrantInfo> for domain::GrantInfo {
    fn from(g: pb::GrantInfo) -> Self {
        domain::GrantInfo {
            id: g.id,
            module: g.module,
            secret: g.secret,
            origins: g.origins,
            body: g.body,
        }
    }
}

impl From<domain::ConnectionInfo> for pb::ConnectionInfo {
    fn from(c: domain::ConnectionInfo) -> Self {
        pb::ConnectionInfo {
            name: c.name,
            kind: c.kind,
            config_json: json_bytes(&c.config),
        }
    }
}

impl TryFrom<pb::ConnectionInfo> for domain::ConnectionInfo {
    type Error = ApiError;

    /// `config_json` must be JSON (empty = `null`, which validation then rejects).
    fn try_from(c: pb::ConnectionInfo) -> Result<Self, ApiError> {
        Ok(domain::ConnectionInfo {
            config: payload_from_proto("config_json", &c.config_json)?,
            name: c.name,
            kind: c.kind,
        })
    }
}

impl From<domain::EgressInfo> for pb::EgressInfo {
    fn from(e: domain::EgressInfo) -> Self {
        pb::EgressInfo {
            module: e.module,
            allow: e.allow,
        }
    }
}

impl From<pb::EgressInfo> for domain::EgressInfo {
    fn from(e: pb::EgressInfo) -> Self {
        domain::EgressInfo {
            module: e.module,
            allow: e.allow,
        }
    }
}

fn required<T>(field: &str, v: Option<T>) -> Result<T, Status> {
    v.ok_or_else(|| {
        to_status(
            ApiError::invalid_argument(format!("{field} is required"))
                .with_details(serde_json::json!({ "field": field })),
        )
    })
}

/// `ostro.api.v1.Secrets` (secrets and grants) over a [`ControlPlane`]. Secret values
/// are accepted on `PutSecret` only and never returned.
#[derive(Clone)]
pub struct GrpcSecrets(pub Arc<dyn ControlPlane>);

/// `ostro.api.v1.Connectors` (connections and egress policies) over a [`ControlPlane`].
#[derive(Clone)]
pub struct GrpcConnectors(pub Arc<dyn ControlPlane>);

#[tonic::async_trait]
impl secrets_server::Secrets for GrpcSecrets {
    async fn put_secret(
        &self,
        request: Request<pb::PutSecretRequest>,
    ) -> Result<Response<pb::SecretInfo>, Status> {
        let r = request.into_inner();
        let info = self
            .0
            .put_secret(domain::PutSecret {
                name: r.name,
                value: r.value,
                tenant: r.tenant,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(info.into()))
    }

    async fn list_secrets(
        &self,
        request: Request<pb::ListSecretsRequest>,
    ) -> Result<Response<pb::ListSecretsResponse>, Status> {
        let secrets = self
            .0
            .list_secrets(request.into_inner().tenant)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::ListSecretsResponse {
            secrets: secrets.into_iter().map(Into::into).collect(),
        }))
    }

    async fn delete_secret(
        &self,
        request: Request<pb::DeleteSecretRequest>,
    ) -> Result<Response<pb::DeleteSecretResponse>, Status> {
        let r = request.into_inner();
        self.0
            .delete_secret(r.name, r.tenant)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::DeleteSecretResponse {}))
    }

    async fn put_grant(
        &self,
        request: Request<pb::PutGrantRequest>,
    ) -> Result<Response<pb::GrantInfo>, Status> {
        let g = required("grant", request.into_inner().grant)?;
        let g = self.0.put_grant(g.into()).await.map_err(to_status)?;
        Ok(Response::new(g.into()))
    }

    async fn list_grants(
        &self,
        request: Request<pb::ListGrantsRequest>,
    ) -> Result<Response<pb::ListGrantsResponse>, Status> {
        let grants = self
            .0
            .list_grants(request.into_inner().module)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::ListGrantsResponse {
            grants: grants.into_iter().map(Into::into).collect(),
        }))
    }

    async fn delete_grant(
        &self,
        request: Request<pb::DeleteGrantRequest>,
    ) -> Result<Response<pb::DeleteGrantResponse>, Status> {
        self.0
            .delete_grant(request.into_inner().id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::DeleteGrantResponse {}))
    }
}

#[tonic::async_trait]
impl connectors_server::Connectors for GrpcConnectors {
    async fn put_connection(
        &self,
        request: Request<pb::PutConnectionRequest>,
    ) -> Result<Response<pb::ConnectionInfo>, Status> {
        let c = required("connection", request.into_inner().connection)?;
        let c = domain::ConnectionInfo::try_from(c).map_err(to_status)?;
        let c = self.0.put_connection(c).await.map_err(to_status)?;
        Ok(Response::new(c.into()))
    }

    async fn get_connection(
        &self,
        request: Request<pb::GetConnectionRequest>,
    ) -> Result<Response<pb::ConnectionInfo>, Status> {
        let c = self
            .0
            .get_connection(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(c.into()))
    }

    async fn list_connections(
        &self,
        _request: Request<pb::ListConnectionsRequest>,
    ) -> Result<Response<pb::ListConnectionsResponse>, Status> {
        let connections = self.0.list_connections().await.map_err(to_status)?;
        Ok(Response::new(pb::ListConnectionsResponse {
            connections: connections.into_iter().map(Into::into).collect(),
        }))
    }

    async fn delete_connection(
        &self,
        request: Request<pb::DeleteConnectionRequest>,
    ) -> Result<Response<pb::DeleteConnectionResponse>, Status> {
        self.0
            .delete_connection(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::DeleteConnectionResponse {}))
    }

    async fn put_egress(
        &self,
        request: Request<pb::PutEgressRequest>,
    ) -> Result<Response<pb::EgressInfo>, Status> {
        let e = required("egress", request.into_inner().egress)?;
        let e = self.0.put_egress(e.into()).await.map_err(to_status)?;
        Ok(Response::new(e.into()))
    }

    async fn get_egress(
        &self,
        request: Request<pb::GetEgressRequest>,
    ) -> Result<Response<pb::EgressInfo>, Status> {
        let e = self
            .0
            .get_egress(request.into_inner().module)
            .await
            .map_err(to_status)?;
        Ok(Response::new(e.into()))
    }

    async fn list_egress(
        &self,
        _request: Request<pb::ListEgressRequest>,
    ) -> Result<Response<pb::ListEgressResponse>, Status> {
        let policies = self.0.list_egress().await.map_err(to_status)?;
        Ok(Response::new(pb::ListEgressResponse {
            policies: policies.into_iter().map(Into::into).collect(),
        }))
    }

    async fn delete_egress(
        &self,
        request: Request<pb::DeleteEgressRequest>,
    ) -> Result<Response<pb::DeleteEgressResponse>, Status> {
        self.0
            .delete_egress(request.into_inner().module)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::DeleteEgressResponse {}))
    }
}

// ---------------------------------------------------------------------------------
// Phase 7 (07-11): ostro.api.v1.{Queues, Workers}, run and workflow pause/resume
// ---------------------------------------------------------------------------------

/// Proto `Placement` -> `Option<Placement>` (`UNSPECIFIED` = default); unknown values
/// are invalid.
fn placement_from_proto(v: i32) -> Result<Option<phase7::Placement>, ApiError> {
    match pb::Placement::try_from(v) {
        Ok(pb::Placement::Unspecified) => Ok(None),
        Ok(pb::Placement::Embedded) => Ok(Some(phase7::Placement::Embedded)),
        Ok(pb::Placement::Workers) => Ok(Some(phase7::Placement::Workers)),
        Ok(pb::Placement::Auto) => Ok(Some(phase7::Placement::Auto)),
        Err(_) => Err(ApiError::invalid_argument(format!(
            "placement: unknown value {v}"
        ))),
    }
}

fn placement_to_proto(p: Option<phase7::Placement>) -> i32 {
    (match p {
        None => pb::Placement::Unspecified,
        Some(phase7::Placement::Embedded) => pb::Placement::Embedded,
        Some(phase7::Placement::Workers) => pb::Placement::Workers,
        Some(phase7::Placement::Auto) => pb::Placement::Auto,
    }) as i32
}

fn exhaust_from_proto(v: i32) -> Result<Option<phase7::ExhaustAction>, ApiError> {
    match pb::ExhaustAction::try_from(v) {
        Ok(pb::ExhaustAction::Unspecified) => Ok(None),
        Ok(pb::ExhaustAction::Fail) => Ok(Some(phase7::ExhaustAction::Fail)),
        Ok(pb::ExhaustAction::Pause) => Ok(Some(phase7::ExhaustAction::Pause)),
        Err(_) => Err(ApiError::invalid_argument(format!(
            "on_retries_exhausted: unknown value {v}"
        ))),
    }
}

fn exhaust_to_proto(a: Option<phase7::ExhaustAction>) -> i32 {
    (match a {
        None => pb::ExhaustAction::Unspecified,
        Some(phase7::ExhaustAction::Fail) => pb::ExhaustAction::Fail,
        Some(phase7::ExhaustAction::Pause) => pb::ExhaustAction::Pause,
    }) as i32
}

impl From<pb::RateSpec> for phase7::RateSpecDto {
    fn from(r: pb::RateSpec) -> Self {
        phase7::RateSpecDto {
            limit: r.limit,
            period_ms: r.period_ms,
            burst: r.burst,
        }
    }
}

impl From<phase7::RateSpecDto> for pb::RateSpec {
    fn from(r: phase7::RateSpecDto) -> Self {
        pb::RateSpec {
            limit: r.limit,
            period_ms: r.period_ms,
            burst: r.burst,
        }
    }
}

/// A proto percentage (`uint32`) -> `u8`; values above 255 are invalid here, values
/// 101..=255 are rejected by [`phase7::QueueFlow::validate`] like on REST.
fn pct(field: &str, v: u32) -> Result<u8, ApiError> {
    u8::try_from(v)
        .map_err(|_| ApiError::invalid_argument(format!("{field} = {v} is out of range (max 100)")))
}

impl TryFrom<pb::SpillPolicy> for phase7::SpillPolicyDto {
    type Error = ApiError;

    fn try_from(s: pb::SpillPolicy) -> Result<Self, ApiError> {
        Ok(phase7::SpillPolicyDto {
            cpu_pct: pct("spill.cpu_pct", s.cpu_pct)?,
            slots_pct: pct("spill.slots_pct", s.slots_pct)?,
            mem_pct: pct("spill.mem_pct", s.mem_pct)?,
            lag_ms: s.lag_ms,
            sustain_ms: s.sustain_ms,
            cooldown_ms: s.cooldown_ms,
            hysteresis_pct: pct("spill.hysteresis_pct", s.hysteresis_pct)?,
        })
    }
}

impl From<phase7::SpillPolicyDto> for pb::SpillPolicy {
    fn from(s: phase7::SpillPolicyDto) -> Self {
        pb::SpillPolicy {
            cpu_pct: s.cpu_pct.into(),
            slots_pct: s.slots_pct.into(),
            mem_pct: s.mem_pct.into(),
            lag_ms: s.lag_ms,
            sustain_ms: s.sustain_ms,
            cooldown_ms: s.cooldown_ms,
            hysteresis_pct: s.hysteresis_pct.into(),
        }
    }
}

impl TryFrom<pb::QueueFlow> for phase7::QueueFlow {
    type Error = ApiError;

    fn try_from(f: pb::QueueFlow) -> Result<Self, ApiError> {
        Ok(phase7::QueueFlow {
            placement: placement_from_proto(f.placement)?,
            selector: f.selector,
            spill: f.spill.map(TryInto::try_into).transpose()?,
            concurrency: f.concurrency,
            weight: f.weight,
            key_concurrency: f.key_concurrency,
            throttle: f.throttle.map(Into::into),
            key_throttle: f.key_throttle.map(Into::into),
            rate_limit: f.rate_limit.map(Into::into),
            key_rate_limit: f.key_rate_limit.map(Into::into),
            on_retries_exhausted: exhaust_from_proto(f.on_retries_exhausted)?,
            paused: f.paused,
        })
    }
}

impl From<phase7::QueueFlow> for pb::QueueFlow {
    fn from(f: phase7::QueueFlow) -> Self {
        pb::QueueFlow {
            placement: placement_to_proto(f.placement),
            selector: f.selector,
            spill: f.spill.map(Into::into),
            concurrency: f.concurrency,
            weight: f.weight,
            key_concurrency: f.key_concurrency,
            throttle: f.throttle.map(Into::into),
            key_throttle: f.key_throttle.map(Into::into),
            rate_limit: f.rate_limit.map(Into::into),
            key_rate_limit: f.key_rate_limit.map(Into::into),
            on_retries_exhausted: exhaust_to_proto(f.on_retries_exhausted),
            paused: f.paused,
        }
    }
}

impl From<phase7::QueueFlowInfo> for pb::QueueFlowInfo {
    fn from(i: phase7::QueueFlowInfo) -> Self {
        pb::QueueFlowInfo {
            queue: i.queue,
            version: i.version,
            flow: Some(i.flow.into()),
        }
    }
}

impl From<phase7::DemandSnapshot> for pb::QueueDemand {
    fn from(d: phase7::DemandSnapshot) -> Self {
        pb::QueueDemand {
            queue: d.queue,
            demand: d.demand,
            backlog: d.backlog,
            running_on_workers: d.running_on_workers,
            placement: placement_to_proto(Some(d.placement)),
            spilling: d.spilling,
            workers: d.workers,
            worker_slots_free: d.worker_slots_free,
        }
    }
}

impl From<phase7::RunPauseState> for pb::RunPauseState {
    fn from(s: phase7::RunPauseState) -> Self {
        pb::RunPauseState {
            run_id: s.run_id,
            paused: s.paused,
            changed: s.changed,
        }
    }
}

fn workflow_pause_state(s: phase7::PauseState) -> pb::WorkflowPauseState {
    pb::WorkflowPauseState {
        workflow: s.name,
        paused: s.paused,
        changed: s.changed,
    }
}

fn queue_pause_state(s: phase7::PauseState) -> pb::QueuePauseState {
    pb::QueuePauseState {
        queue: s.name,
        paused: s.paused,
        changed: s.changed,
    }
}

impl From<phase7::WorkerInfo> for pb::WorkerInfo {
    fn from(w: phase7::WorkerInfo) -> Self {
        pb::WorkerInfo {
            worker_id: w.worker_id,
            labels: w.labels.into_iter().collect(),
            slots: w.slots,
            free_slots: w.free_slots,
            leases: w.leases,
            connected_at_ms: w.connected_at_ms,
            node_id: w.node_id,
            draining: w.draining,
        }
    }
}

impl From<phase7::WorkerTokenInfo> for pb::WorkerTokenInfo {
    fn from(t: phase7::WorkerTokenInfo) -> Self {
        pb::WorkerTokenInfo {
            token_id: t.token_id,
            created_at_ms: t.created_at_ms,
            expires_at_ms: t.expires_at_ms,
        }
    }
}

impl From<pb::JoinWorkerRequest> for phase7::JoinRequest {
    fn from(r: pb::JoinWorkerRequest) -> Self {
        phase7::JoinRequest {
            token: r.token,
            csr_pem: r.csr_pem,
            worker_id: r.worker_id,
            labels: r.labels.into_iter().collect(),
        }
    }
}

impl From<phase7::JoinResponse> for pb::JoinWorkerResponse {
    fn from(r: phase7::JoinResponse) -> Self {
        pb::JoinWorkerResponse {
            client_cert_pem: r.client_cert_pem,
            ca_cert_pem: r.ca_cert_pem,
            endpoints: r.endpoints,
            renew_after_ms: r.renew_after_ms,
            cluster_id: r.cluster_id,
        }
    }
}

/// `ostro.api.v1.Queues` (flow, pause, demand) over a [`ControlPlane`].
#[derive(Clone)]
pub struct GrpcQueues(pub Arc<dyn ControlPlane>);

/// `ostro.api.v1.Workers` (worker admin, join tokens, join) over a [`ControlPlane`].
#[derive(Clone)]
pub struct GrpcWorkers(pub Arc<dyn ControlPlane>);

#[tonic::async_trait]
impl queues_server::Queues for GrpcQueues {
    async fn get_queue_flow(
        &self,
        request: Request<pb::GetQueueFlowRequest>,
    ) -> Result<Response<pb::QueueFlowInfo>, Status> {
        let info = self
            .0
            .get_queue_flow(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(info.into()))
    }

    async fn put_queue_flow(
        &self,
        request: Request<pb::PutQueueFlowRequest>,
    ) -> Result<Response<pb::QueueFlowInfo>, Status> {
        let r = request.into_inner();
        let flow = phase7::QueueFlow::try_from(r.flow.unwrap_or_default()).map_err(to_status)?;
        let info = self
            .0
            .put_queue_flow(r.name, flow, r.expected_version)
            .await
            .map_err(to_status)?;
        Ok(Response::new(info.into()))
    }

    async fn pause_queue(
        &self,
        request: Request<pb::PauseQueueRequest>,
    ) -> Result<Response<pb::QueuePauseState>, Status> {
        let st = self
            .0
            .pause_queue(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(queue_pause_state(st)))
    }

    async fn resume_queue(
        &self,
        request: Request<pb::ResumeQueueRequest>,
    ) -> Result<Response<pb::QueuePauseState>, Status> {
        let st = self
            .0
            .resume_queue(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(queue_pause_state(st)))
    }

    async fn get_queue_demand(
        &self,
        request: Request<pb::GetQueueDemandRequest>,
    ) -> Result<Response<pb::QueueDemand>, Status> {
        let d = self
            .0
            .queue_demand(request.into_inner().name)
            .await
            .map_err(to_status)?;
        Ok(Response::new(d.into()))
    }
}

#[tonic::async_trait]
impl workers_server::Workers for GrpcWorkers {
    async fn list_workers(
        &self,
        _request: Request<pb::ListWorkersRequest>,
    ) -> Result<Response<pb::ListWorkersResponse>, Status> {
        let workers = self.0.list_workers().await.map_err(to_status)?;
        Ok(Response::new(pb::ListWorkersResponse {
            workers: workers.into_iter().map(Into::into).collect(),
        }))
    }

    async fn drain_worker(
        &self,
        request: Request<pb::DrainWorkerRequest>,
    ) -> Result<Response<pb::DrainWorkerResponse>, Status> {
        let r = self
            .0
            .drain_worker(request.into_inner().worker_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::DrainWorkerResponse {
            worker_id: r.worker_id,
            already_draining: r.already_draining,
        }))
    }

    async fn create_worker_token(
        &self,
        request: Request<pb::CreateWorkerTokenRequest>,
    ) -> Result<Response<pb::WorkerTokenCreated>, Status> {
        let c = self
            .0
            .create_worker_token(phase7::CreateWorkerTokenRequest {
                ttl_ms: request.into_inner().ttl_ms,
            })
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::WorkerTokenCreated {
            token_id: c.token_id,
            token: c.token,
            join_command: c.join_command,
            expires_at_ms: c.expires_at_ms,
        }))
    }

    async fn list_worker_tokens(
        &self,
        _request: Request<pb::ListWorkerTokensRequest>,
    ) -> Result<Response<pb::ListWorkerTokensResponse>, Status> {
        let tokens = self.0.list_worker_tokens().await.map_err(to_status)?;
        Ok(Response::new(pb::ListWorkerTokensResponse {
            tokens: tokens.into_iter().map(Into::into).collect(),
        }))
    }

    async fn revoke_worker_token(
        &self,
        request: Request<pb::RevokeWorkerTokenRequest>,
    ) -> Result<Response<pb::RevokeWorkerTokenResponse>, Status> {
        self.0
            .revoke_worker_token(request.into_inner().token_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(pb::RevokeWorkerTokenResponse {}))
    }

    async fn join_worker(
        &self,
        request: Request<pb::JoinWorkerRequest>,
    ) -> Result<Response<pb::JoinWorkerResponse>, Status> {
        let r = self
            .0
            .join_worker(request.into_inner().into())
            .await
            .map_err(to_status)?;
        Ok(Response::new(r.into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn to_status_carries_code_message_details() {
        let s = to_status(
            ApiError::conflict("dup").with_details(serde_json::json!({ "existing_run_id": "r" })),
        );
        assert_eq!(s.code(), tonic::Code::AlreadyExists);
        assert_eq!(s.message(), "dup");
        assert_eq!(
            status_details(&s),
            Some(serde_json::json!({ "existing_run_id": "r" }))
        );
        let s = to_status(ApiError::not_found("x"));
        assert_eq!(s.code(), tonic::Code::NotFound);
        assert_eq!(status_details(&s), None);
        for c in ErrorCode::ALL {
            assert_eq!(to_status(ApiError::new(c, "m")).code(), c.grpc_code());
        }
    }

    #[test]
    fn start_request_validation() {
        let ok = pb::StartRunRequest {
            workflow: "w".into(),
            ..Default::default()
        };
        let d = domain::StartRunRequest::try_from(ok.clone()).expect("ok");
        assert_eq!(d.input_json, b"null");
        assert_eq!(d.version, None);
        assert_eq!(d.conflict_policy, domain::ConflictPolicy::ReturnExisting);

        let bad_json = pb::StartRunRequest {
            input_json: b"{nope".to_vec(),
            ..ok.clone()
        };
        let e = domain::StartRunRequest::try_from(bad_json).expect_err("bad json");
        assert_eq!(e.code, ErrorCode::InvalidArgument);

        let bad_policy = pb::StartRunRequest {
            conflict_policy: 9,
            ..ok
        };
        let e = domain::StartRunRequest::try_from(bad_policy).expect_err("bad enum");
        assert_eq!(e.code, ErrorCode::InvalidArgument);
    }

    #[test]
    fn lifecycle_conversions() {
        let sig = pb::SignalRunRequest {
            run_id: "r".into(),
            name: "go".into(),
            ..Default::default()
        };
        let d = domain::SignalRequest::try_from(sig.clone()).expect("ok");
        assert_eq!(d.payload, serde_json::Value::Null);
        let bad = pb::SignalRunRequest {
            payload_json: b"{nope".to_vec(),
            ..sig
        };
        assert_eq!(
            domain::SignalRequest::try_from(bad).unwrap_err().code,
            ErrorCode::InvalidArgument
        );
        let tok = pb::CompleteTokenRequest {
            token: "ot1_x".into(),
            payload_json: br#"{"a":1}"#.to_vec(),
            dry_run: true,
        };
        let d = domain::CompleteTokenRequest::try_from(tok).expect("ok");
        assert_eq!(d.payload, serde_json::json!({ "a": 1 }));
        assert!(d.dry_run);
        let p = domain::RunPolicy {
            retention_ms: Some(0),
            schedule_to_start_ms: None,
        };
        assert_eq!(domain::RunPolicy::from(pb::RunPolicy::from(p)), p);
        let out = pb::AwaitResultResponse::from(domain::AwaitResultResponse {
            run_id: "r".into(),
            status: domain::RunStatus::Completed,
            completed: true,
            result: Some(serde_json::Value::Null),
            failure: None,
        });
        assert_eq!(out.result_json.as_deref(), Some(&b"null"[..]));
        assert_eq!(out.failure_json, None);
        assert_eq!(
            pb::RetryMode::from(domain::RetryMode::Resumed),
            pb::RetryMode::Resumed
        );
    }

    #[test]
    fn list_request_status_validation() {
        for bad in [pb::RunStatus::Unspecified as i32, 42] {
            let r = pb::ListRunsRequest {
                status: Some(bad),
                ..Default::default()
            };
            let e = domain::ListRunsRequest::try_from(r).expect_err("invalid status");
            assert_eq!(e.code, ErrorCode::InvalidArgument);
        }
        let r = pb::ListRunsRequest {
            status: Some(pb::RunStatus::Running as i32),
            limit: 0,
            ..Default::default()
        };
        let d = domain::ListRunsRequest::try_from(r).expect("ok");
        assert_eq!(d.status, Some(domain::RunStatus::Running));
        assert_eq!(d.limit, domain::DEFAULT_PAGE_LIMIT);
        for st in domain::RunStatus::ALL {
            assert_eq!(
                run_status_from_proto("s", pb::RunStatus::from(st) as i32).expect("round trip"),
                st
            );
        }
    }
}
