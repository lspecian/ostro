//! gRPC adapter: `ostro.api.v1` services over [`ControlPlane`].
//!
//! [`GrpcModules`], [`GrpcWorkflows`] and [`GrpcRuns`] are thin translators, like the
//! REST handlers: proto request -> domain request -> `ControlPlane` -> proto response.
//! Errors go through [`to_status`], which takes the code from the shared
//! [`ApiError::grpc_code`] table. Every proto <-> domain conversion lives in this file.

use std::sync::Arc;

use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{modules_server, runs_server, workflows_server};
use tonic::{Request, Response, Status};

use crate::domain::{self, ControlPlane};
use crate::error::ApiError;

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
}

#[tonic::async_trait]
impl runs_server::Runs for GrpcRuns {
    async fn start(
        &self,
        request: Request<pb::StartRunRequest>,
    ) -> Result<Response<pb::StartRunResponse>, Status> {
        let req = domain::StartRunRequest::try_from(request.into_inner()).map_err(to_status)?;
        let res = self.0.start_run(req).await.map_err(to_status)?;
        Ok(Response::new(pb::StartRunResponse {
            run_id: res.run_id,
            created: res.created,
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
