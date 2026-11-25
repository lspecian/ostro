//! The production `ostro.api.v1` gRPC client (13-03; D-P13-14): [`GrpcControlPlane`]
//! implements [`ControlPlane`] and every extension trait over a tonic [`Channel`], so a
//! whole control-plane call can be forwarded to another node losslessly. 13-12's
//! `PeerControlPlane` wraps one per peer over the mTLS cluster listener.
//!
//! - **Request metadata.** Every call copies [`RequestMeta::current`] into the outgoing
//!   headers (`ostro-hop`, `ostro-partition`, `ostro-partitions`, `ostro-principal`,
//!   `ostro-read-after`, [`crate::request_meta::to_metadata`]) as it is: the caller sets
//!   `hop = 1` in the scope it forwards from (13-12). A deadline in the scope becomes the
//!   request timeout (`grpc-timeout` = the remaining budget); a budget already spent fails
//!   with `unavailable` before anything is sent. Long-poll calls (`await_result`) clamp
//!   their wait to the remaining budget.
//! - **Consistency.** A response's `ostro-consistency` header is published into the
//!   caller's out-slot ([`crate::request_meta::set_consistency`]).
//! - **Errors.** [`status_to_error`] is the exact inverse of [`crate::grpc::to_status`]:
//!   code (with the `RESOURCE_EXHAUSTED` + `retry_after_ms` rule for `rate_limited`),
//!   message and JSON details (e.g. `not_leader {partition, leader, leader_addr, epoch}`)
//!   come back unchanged. Transport-level `DEADLINE_EXCEEDED` / `CANCELLED`, which the
//!   server never emits, are `unavailable`.
//! - **No retries.** The client sends each call once; 13-12 owns the retry policy.
//! - **REST-only methods.** `presigned_object`, `dev_time`, `dev_skip`, `replay_module`,
//!   `HookIngress::deliver` and `AdminApi::backup` have no gRPC twin (D-P10-18, the
//!   presign signature and the backup stream are node-local); the client answers
//!   `unimplemented` with `details.method` for them, explicitly.
//!
//! Every trait method has an explicit body here (no trait default is relied on);
//! `every_trait_method_is_overridden` pins that against the trait sources.

use std::future::Future;
use std::time::{Duration, Instant};

use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{
    cluster_client::ClusterClient, connectors_client::ConnectorsClient,
    inventory_client::InventoryClient, module_lifecycle_client::ModuleLifecycleClient,
    modules_client::ModulesClient, queues_client::QueuesClient, run_logs_client::RunLogsClient,
    run_ops_client::RunOpsClient, runs_client::RunsClient, schedules_client::SchedulesClient,
    secrets_client::SecretsClient, tokens_client::TokensClient, webhooks_client::WebhooksClient,
    workers_client::WorkersClient, workflows_client::WorkflowsClient,
};
use tonic::transport::Channel;

use crate::domain::{self, ControlPlane};
use crate::error::{ApiError, ErrorCode};
use crate::grpc::run_status_from_proto;
use crate::phase7;
use crate::phase8::{
    AdminApi, BackupStream, HookIngress, HookRequest, HookResponse, LogLevelDto, RunLogLine,
    RunLogsApi, RunLogsRequest, RunLogsResponse, ScheduleApi, ScheduleDto, WebhookApi, WebhookDto,
};
use crate::phase11::{self, InventoryApi, OpsApi};
use crate::phase13::{self, ClusterApi, ModuleLifecycleApi};
use crate::phase13_adapters::init_to_proto;
use crate::request_meta::{self, HEADER_CONSISTENCY, RequestMeta};

/// Default cap on encoded / decoded messages (above the 64 MiB default module limit plus
/// headroom); see [`GrpcControlPlane::with_max_message_bytes`].
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 80 * 1024 * 1024;

/// A `ControlPlane` (and every extension trait) over an `ostro.api.v1` gRPC channel.
#[derive(Clone, Debug)]
pub struct GrpcControlPlane {
    channel: Channel,
    max_message_bytes: usize,
}

/// Whether tonic built `s` on this side from a failure below the gRPC layer (the
/// connection reset or closed mid-call, an I/O error): such a status carries the error it
/// came from as its source. A status decoded from a server's trailers has no source.
fn is_client_transport_failure(s: &tonic::Status) -> bool {
    let mut cur: Option<&(dyn std::error::Error + 'static)> = std::error::Error::source(s);
    while let Some(e) = cur {
        if e.is::<tonic::transport::Error>() || e.is::<std::io::Error>() {
            return true;
        }
        cur = e.source();
    }
    false
}

/// `tonic::Status` -> [`ApiError`]: the exact inverse of [`crate::grpc::to_status`].
///
/// Transport failures are `unavailable` (reason-less, the routed caller's retry rule):
/// `DEADLINE_EXCEEDED` / `CANCELLED`, and the `UNKNOWN "transport error"` (or an h2
/// `INTERNAL`) that tonic synthesises on this side when the peer's connection dies
/// mid-call. A server never sends `UNKNOWN` ([`ErrorCode::grpc_code`] has no such row), and
/// a server's `INTERNAL` has no source error, so both keep the table.
pub fn status_to_error(s: &tonic::Status) -> ApiError {
    let details = crate::grpc::status_details(s);
    let code = match s.code() {
        tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => ErrorCode::Unavailable,
        tonic::Code::Unknown | tonic::Code::Internal if is_client_transport_failure(s) => {
            ErrorCode::Unavailable
        }
        c => ErrorCode::from_grpc_code_and_details(c, details.as_ref()),
    };
    ApiError {
        code,
        message: s.message().to_owned(),
        details,
    }
}

fn rest_only(method: &str) -> ApiError {
    ApiError::new(
        ErrorCode::Unimplemented,
        format!("{method} has no gRPC twin (REST-only); call it on the serving node"),
    )
    .with_details(serde_json::json!({ "method": method }))
}

fn deadline_spent() -> ApiError {
    ApiError::unavailable("the request deadline expired before the call was forwarded")
}

fn bad_reply(what: &str) -> ApiError {
    ApiError::internal(format!("malformed gRPC reply: {what}"))
}

fn json_bytes(v: &serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(v).unwrap_or_default()
}

fn json_from(field: &str, bytes: &[u8]) -> Result<serde_json::Value, ApiError> {
    if bytes.is_empty() {
        return Ok(serde_json::Value::Null);
    }
    serde_json::from_slice(bytes).map_err(|e| bad_reply(&format!("{field}: {e}")))
}

/// The remaining budget of the current scope (`None` = no deadline).
fn remaining(meta: Option<&RequestMeta>) -> Result<Option<Duration>, ApiError> {
    match meta.and_then(|m| m.deadline) {
        None => Ok(None),
        Some(d) => {
            // clock-exempt: request deadlines are wall-time budgets of the caller.
            let left = d.saturating_duration_since(Instant::now());
            if left.is_zero() {
                Err(deadline_spent())
            } else {
                Ok(Some(left))
            }
        }
    }
}

/// Build the outgoing request: metadata headers and the remaining deadline.
fn request<T>(msg: T) -> Result<tonic::Request<T>, ApiError> {
    let meta = RequestMeta::current();
    let mut r = tonic::Request::new(msg);
    if let Some(m) = &meta {
        request_meta::to_metadata(m, r.metadata_mut());
    }
    if let Some(left) = remaining(meta.as_ref())? {
        r.set_timeout(left);
    }
    Ok(r)
}

/// Await a call: publish `ostro-consistency`, map errors.
async fn finish<T>(
    fut: impl Future<Output = Result<tonic::Response<T>, tonic::Status>>,
) -> Result<T, ApiError> {
    match fut.await {
        Ok(resp) => {
            if let Some(t) = resp
                .metadata()
                .get(HEADER_CONSISTENCY)
                .and_then(|v| v.to_str().ok())
            {
                request_meta::set_consistency(t.to_owned());
            }
            Ok(resp.into_inner())
        }
        Err(s) => Err(status_to_error(&s)),
    }
}

/// A sized client of one service over the shared channel.
macro_rules! client {
    ($self:ident, $c:ident) => {
        $c::new($self.channel.clone())
            .max_decoding_message_size($self.max_message_bytes)
            .max_encoding_message_size($self.max_message_bytes)
    };
}

/// One unary call: `call!(self, Client, method, request_message)`.
macro_rules! call {
    ($self:ident, $c:ident, $m:ident, $msg:expr) => {{
        let req = request($msg)?;
        let mut c = client!($self, $c);
        finish(c.$m(req)).await
    }};
}

impl GrpcControlPlane {
    /// A client over `channel`.
    pub fn new(channel: Channel) -> Self {
        Self {
            channel,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        }
    }

    /// The underlying channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Cap encoded and decoded messages at `n` bytes (forwarders set it to the node's
    /// `api.max_module_bytes` plus headroom when that exceeds the default).
    pub fn with_max_message_bytes(mut self, n: usize) -> Self {
        self.max_message_bytes = n;
        self
    }
}

// ---------------------------------------------------------------------------------
// Reply conversions (proto -> domain) the server side never needed
// ---------------------------------------------------------------------------------

fn status(field: &str, v: i32) -> Result<domain::RunStatus, ApiError> {
    run_status_from_proto(field, v).map_err(|e| bad_reply(&e.message))
}

fn module_info(m: pb::ModuleInfo) -> domain::ModuleInfo {
    domain::ModuleInfo {
        sha256: m.sha256,
        size_bytes: m.size_bytes,
        language: m.language,
        workflows: m
            .workflows
            .into_iter()
            .map(|w| domain::WorkflowRef {
                name: w.name,
                version: w.version,
            })
            .collect(),
        uploaded_at_ms: m.uploaded_at_ms,
    }
}

fn workflow_info(w: pb::WorkflowInfo) -> domain::WorkflowInfo {
    domain::WorkflowInfo {
        name: w.name,
        active_version: w.active_version,
        versions: w
            .versions
            .into_iter()
            .map(|v| domain::WorkflowVersionInfo {
                version: v.version,
                module_sha256: v.module_sha256,
                default_queue: v.default_queue,
                run_timeout_ms: v.run_timeout_ms,
            })
            .collect(),
        record_version: w.record_version,
    }
}

fn run_info(r: pb::RunInfo) -> Result<domain::RunInfo, ApiError> {
    Ok(domain::RunInfo {
        status: status("status", r.status)?,
        run_id: r.run_id,
        workflow: r.workflow,
        version: r.version,
        module_sha256: r.module_sha256,
        queue: r.queue,
        attempt: r.attempt,
        tags: r.tags,
        tenant: r.tenant,
        traceparent: r.traceparent,
        priority: r.priority,
        concurrency_key: r.concurrency_key,
        created_at_ms: r.created_at_ms,
        completed_at_ms: r.completed_at_ms,
        published_status_json: r.published_status_json,
        result_json: r.result_json,
        failure: r.failure,
        attempt_failures: r.attempt_failures,
        retention_ms: r.retention_ms,
        schedule_to_start_ms: r.schedule_to_start_ms,
        pause: r.pause.map(|p| phase7::RunPauseMark {
            reason: p.reason,
            detail: p.detail,
            at_ms: p.at_ms,
        }),
    })
}

fn conflict_policy(p: domain::ConflictPolicy) -> i32 {
    (match p {
        domain::ConflictPolicy::ReturnExisting => pb::ConflictPolicy::ReturnExisting,
        domain::ConflictPolicy::Reject => pb::ConflictPolicy::Reject,
    }) as i32
}

fn list_order(o: domain::ListOrder) -> i32 {
    (match o {
        domain::ListOrder::Oldest => pb::ListOrder::Oldest,
        domain::ListOrder::Newest => pb::ListOrder::Newest,
    }) as i32
}

fn retry_mode(v: i32) -> Result<domain::RetryMode, ApiError> {
    match pb::RetryMode::try_from(v) {
        Ok(pb::RetryMode::Resumed) => Ok(domain::RetryMode::Resumed),
        Ok(pb::RetryMode::Restarted) => Ok(domain::RetryMode::Restarted),
        _ => Err(bad_reply(&format!("retry mode {v}"))),
    }
}

fn placement(v: i32) -> Result<phase7::Placement, ApiError> {
    match pb::Placement::try_from(v) {
        Ok(pb::Placement::Embedded) => Ok(phase7::Placement::Embedded),
        Ok(pb::Placement::Workers) => Ok(phase7::Placement::Workers),
        Ok(pb::Placement::Auto) => Ok(phase7::Placement::Auto),
        _ => Err(bad_reply(&format!("placement {v}"))),
    }
}

fn queue_flow_info(i: pb::QueueFlowInfo) -> Result<phase7::QueueFlowInfo, ApiError> {
    Ok(phase7::QueueFlowInfo {
        queue: i.queue,
        version: i.version,
        flow: phase7::QueueFlow::try_from(i.flow.unwrap_or_default())
            .map_err(|e| bad_reply(&e.message))?,
    })
}

fn level_to_proto(l: LogLevelDto) -> i32 {
    (match l {
        LogLevelDto::Trace => pb::LogLevel::Trace,
        LogLevelDto::Debug => pb::LogLevel::Debug,
        LogLevelDto::Info => pb::LogLevel::Info,
        LogLevelDto::Warn => pb::LogLevel::Warn,
        LogLevelDto::Error => pb::LogLevel::Error,
    }) as i32
}

fn level_from_proto(v: i32) -> Result<LogLevelDto, ApiError> {
    match pb::LogLevel::try_from(v) {
        Ok(pb::LogLevel::Trace) => Ok(LogLevelDto::Trace),
        Ok(pb::LogLevel::Debug) => Ok(LogLevelDto::Debug),
        Ok(pb::LogLevel::Info) => Ok(LogLevelDto::Info),
        Ok(pb::LogLevel::Warn) => Ok(LogLevelDto::Warn),
        Ok(pb::LogLevel::Error) => Ok(LogLevelDto::Error),
        _ => Err(bad_reply(&format!("log level {v}"))),
    }
}

fn schedule(s: pb::Schedule) -> Result<ScheduleDto, ApiError> {
    ScheduleDto::try_from(s).map_err(|e| bad_reply(&e.message))
}

fn webhook(w: pb::Webhook) -> Result<WebhookDto, ApiError> {
    WebhookDto::try_from(w).map_err(|e| bad_reply(&e.message))
}

// ---------------------------------------------------------------------------------
// ControlPlane
// ---------------------------------------------------------------------------------

#[async_trait::async_trait]
impl ControlPlane for GrpcControlPlane {
    async fn upload_module(
        &self,
        req: domain::UploadModule,
    ) -> Result<domain::UploadModuleResult, ApiError> {
        let r = call!(
            self,
            ModulesClient,
            upload,
            pb::UploadModuleRequest {
                sha256: req.sha256,
                wasm: req.wasm.to_vec(),
                language: req.language,
                tenant: req.tenant,
                name: req.name,
            }
        )?;
        Ok(domain::UploadModuleResult {
            module: module_info(r.module.ok_or_else(|| bad_reply("module missing"))?),
            created: r.created,
        })
    }

    async fn get_module(&self, sha256: String) -> Result<domain::ModuleInfo, ApiError> {
        let m = call!(self, ModulesClient, get, pb::GetModuleRequest { sha256 })?;
        Ok(module_info(m))
    }

    async fn list_workflows(&self) -> Result<Vec<domain::WorkflowInfo>, ApiError> {
        let r = call!(self, WorkflowsClient, list, pb::ListWorkflowsRequest {})?;
        Ok(r.workflows.into_iter().map(workflow_info).collect())
    }

    async fn activate_workflow(
        &self,
        req: domain::ActivateWorkflow,
    ) -> Result<domain::WorkflowInfo, ApiError> {
        let w = call!(
            self,
            WorkflowsClient,
            activate,
            pb::ActivateWorkflowRequest {
                name: req.name,
                version: req.version,
                expected_version: req.expected_version,
            }
        )?;
        Ok(workflow_info(w))
    }

    async fn start_run(
        &self,
        req: domain::StartRunRequest,
    ) -> Result<domain::StartRunResult, ApiError> {
        let r = call!(
            self,
            RunsClient,
            start,
            pb::StartRunRequest {
                workflow: req.workflow,
                input_json: req.input_json,
                version: req.version,
                queue: req.queue,
                delay_ms: req.delay_ms,
                priority: req.priority,
                tags: req.tags,
                traceparent: req.traceparent,
                tenant: req.tenant,
                idempotency_key: req.idempotency_key,
                conflict_policy: conflict_policy(req.conflict_policy),
                concurrency_key: req.concurrency_key,
                schedule_to_start_timeout_ms: req.schedule_to_start_ms,
                retention_ms: req.retention_ms,
                dry_run: req.dry_run,
            }
        )?;
        Ok(domain::StartRunResult {
            run_id: r.run_id,
            created: r.created,
            dry_run: r.dry_run,
        })
    }

    async fn get_run(&self, run_id: String) -> Result<domain::RunInfo, ApiError> {
        run_info(call!(self, RunsClient, get, pb::GetRunRequest { run_id })?)
    }

    async fn run_history(
        &self,
        req: domain::HistoryRequest,
    ) -> Result<domain::HistoryPage, ApiError> {
        let p = call!(
            self,
            RunsClient,
            history,
            pb::RunHistoryRequest {
                run_id: req.run_id,
                from_seq: req.from_seq,
                limit: req.limit,
            }
        )?;
        Ok(domain::HistoryPage {
            events: p
                .events
                .into_iter()
                .map(|e| domain::HistoryEvent {
                    seq: e.seq,
                    kind: e.kind,
                    ts_ms: e.ts_ms,
                    summary: e.summary,
                    details_json: e.details_json,
                    attempt: e.attempt,
                })
                .collect(),
            next_from_seq: p.next_from_seq,
        })
    }

    async fn list_runs(&self, req: domain::ListRunsRequest) -> Result<domain::RunPage, ApiError> {
        let p = call!(
            self,
            RunsClient,
            list,
            pb::ListRunsRequest {
                workflow: req.workflow,
                status: req.status.map(|s| pb::RunStatus::from(s) as i32),
                queue: req.queue,
                tag: req.tag,
                created_after_ms: req.created_after_ms,
                created_before_ms: req.created_before_ms,
                limit: req.limit,
                page_token: req.page_token,
                tags: req.tags,
                order: list_order(req.order),
                dlq: req.dlq,
            }
        )?;
        Ok(domain::RunPage {
            runs: p
                .runs
                .into_iter()
                .map(|r| {
                    Ok(domain::RunSummary {
                        status: status("runs.status", r.status)?,
                        run_id: r.run_id,
                        workflow: r.workflow,
                        version: r.version,
                        queue: r.queue,
                        created_at_ms: r.created_at_ms,
                        tags: r.tags,
                    })
                })
                .collect::<Result<_, ApiError>>()?,
            next_page_token: p.next_page_token,
        })
    }

    /// Always `true`: the channel connects lazily; readiness is the serving node's concern
    /// (a call to an unready node answers `unavailable`).
    fn ready(&self) -> bool {
        true
    }

    async fn signal_run(
        &self,
        r: domain::SignalRequest,
    ) -> Result<domain::SignalResponse, ApiError> {
        let s = call!(
            self,
            RunsClient,
            signal,
            pb::SignalRunRequest {
                payload_json: json_bytes(&r.payload),
                run_id: r.run_id,
                name: r.name,
                request_id: r.request_id,
                dry_run: r.dry_run,
            }
        )?;
        Ok(domain::SignalResponse {
            accepted: s.accepted,
            duplicate: s.duplicate,
            dry_run: s.dry_run,
        })
    }

    async fn cancel_run(
        &self,
        r: domain::CancelRequest,
    ) -> Result<domain::CancelResponse, ApiError> {
        let c = call!(
            self,
            RunsClient,
            cancel,
            pb::CancelRunRequest {
                run_id: r.run_id,
                reason: r.reason,
                dry_run: r.dry_run,
            }
        )?;
        Ok(domain::CancelResponse {
            already_requested: c.already_requested,
            dry_run: c.dry_run,
        })
    }

    async fn terminate_run(
        &self,
        r: domain::TerminateRequest,
    ) -> Result<domain::TerminateResponse, ApiError> {
        let t = call!(
            self,
            RunsClient,
            terminate,
            pb::TerminateRunRequest {
                run_id: r.run_id,
                reason: r.reason,
                dry_run: r.dry_run,
            }
        )?;
        Ok(domain::TerminateResponse {
            status: status("status", t.status)?,
            dry_run: t.dry_run,
        })
    }

    async fn retry_run(&self, r: domain::RetryRequest) -> Result<domain::RetryResponse, ApiError> {
        let t = call!(
            self,
            RunsClient,
            retry,
            pb::RetryRunRequest {
                run_id: r.run_id,
                dry_run: r.dry_run,
            }
        )?;
        Ok(domain::RetryResponse {
            mode: retry_mode(t.mode)?,
            run_id: t.run_id,
            dry_run: t.dry_run,
        })
    }

    /// The wait is clamped to the remaining deadline (long-poll rule).
    async fn await_result(
        &self,
        r: domain::AwaitResultRequest,
    ) -> Result<domain::AwaitResultResponse, ApiError> {
        let wait_ms = match remaining(RequestMeta::current().as_ref())? {
            Some(left) => r
                .wait_ms
                .min(u64::try_from(left.as_millis()).unwrap_or(u64::MAX)),
            None => r.wait_ms,
        };
        let a = call!(
            self,
            RunsClient,
            await_result,
            pb::AwaitResultRequest {
                run_id: r.run_id,
                wait_ms,
                follow_continuations: r.follow_continuations,
            }
        )?;
        Ok(domain::AwaitResultResponse {
            status: status("status", a.status)?,
            run_id: a.run_id,
            completed: a.completed,
            result: a
                .result_json
                .as_deref()
                .map(|b| json_from("result_json", b))
                .transpose()?,
            failure: a
                .failure_json
                .as_deref()
                .map(|b| json_from("failure_json", b))
                .transpose()?,
        })
    }

    async fn complete_token(
        &self,
        r: domain::CompleteTokenRequest,
    ) -> Result<domain::CompleteTokenResponse, ApiError> {
        let c = call!(
            self,
            TokensClient,
            complete,
            pb::CompleteTokenRequest {
                payload_json: json_bytes(&r.payload),
                token: r.token,
                dry_run: r.dry_run,
            }
        )?;
        Ok(domain::CompleteTokenResponse {
            run_id: c.run_id,
            accepted: c.accepted,
            dry_run: c.dry_run,
        })
    }

    async fn set_workflow_policy(
        &self,
        workflow: String,
        p: domain::RunPolicy,
    ) -> Result<domain::RunPolicy, ApiError> {
        let p = call!(
            self,
            WorkflowsClient,
            set_policy,
            pb::SetWorkflowPolicyRequest {
                name: workflow,
                policy: Some(p.into()),
            }
        )?;
        Ok(p.into())
    }

    async fn get_workflow_policy(&self, workflow: String) -> Result<domain::RunPolicy, ApiError> {
        let p = call!(
            self,
            WorkflowsClient,
            get_policy,
            pb::GetWorkflowPolicyRequest { name: workflow }
        )?;
        Ok(p.into())
    }

    async fn set_queue_policy(
        &self,
        queue: String,
        p: domain::RunPolicy,
    ) -> Result<domain::RunPolicy, ApiError> {
        let p = call!(
            self,
            WorkflowsClient,
            set_queue_policy,
            pb::SetQueuePolicyRequest {
                name: queue,
                policy: Some(p.into()),
            }
        )?;
        Ok(p.into())
    }

    async fn get_queue_policy(&self, queue: String) -> Result<domain::RunPolicy, ApiError> {
        let p = call!(
            self,
            WorkflowsClient,
            get_queue_policy,
            pb::GetQueuePolicyRequest { name: queue }
        )?;
        Ok(p.into())
    }

    async fn put_secret(&self, req: domain::PutSecret) -> Result<domain::SecretInfo, ApiError> {
        let s = call!(
            self,
            SecretsClient,
            put_secret,
            pb::PutSecretRequest {
                name: req.name,
                value: req.value,
                tenant: req.tenant,
            }
        )?;
        Ok(domain::SecretInfo {
            name: s.name,
            key_id: s.key_id,
            tenant: s.tenant,
        })
    }

    async fn list_secrets(
        &self,
        tenant: Option<String>,
    ) -> Result<Vec<domain::SecretInfo>, ApiError> {
        let r = call!(
            self,
            SecretsClient,
            list_secrets,
            pb::ListSecretsRequest { tenant }
        )?;
        Ok(r.secrets
            .into_iter()
            .map(|s| domain::SecretInfo {
                name: s.name,
                key_id: s.key_id,
                tenant: s.tenant,
            })
            .collect())
    }

    async fn delete_secret(&self, name: String, tenant: Option<String>) -> Result<(), ApiError> {
        call!(
            self,
            SecretsClient,
            delete_secret,
            pb::DeleteSecretRequest { name, tenant }
        )?;
        Ok(())
    }

    async fn put_grant(&self, g: domain::GrantInfo) -> Result<domain::GrantInfo, ApiError> {
        let g = call!(
            self,
            SecretsClient,
            put_grant,
            pb::PutGrantRequest {
                grant: Some(g.into()),
            }
        )?;
        Ok(g.into())
    }

    async fn list_grants(
        &self,
        module: Option<String>,
    ) -> Result<Vec<domain::GrantInfo>, ApiError> {
        let r = call!(
            self,
            SecretsClient,
            list_grants,
            pb::ListGrantsRequest { module }
        )?;
        Ok(r.grants.into_iter().map(Into::into).collect())
    }

    async fn delete_grant(&self, id: String) -> Result<(), ApiError> {
        call!(
            self,
            SecretsClient,
            delete_grant,
            pb::DeleteGrantRequest { id }
        )?;
        Ok(())
    }

    async fn put_connection(
        &self,
        c: domain::ConnectionInfo,
    ) -> Result<domain::ConnectionInfo, ApiError> {
        let c = call!(
            self,
            ConnectorsClient,
            put_connection,
            pb::PutConnectionRequest {
                connection: Some(c.into()),
            }
        )?;
        domain::ConnectionInfo::try_from(c).map_err(|e| bad_reply(&e.message))
    }

    async fn get_connection(&self, name: String) -> Result<domain::ConnectionInfo, ApiError> {
        let c = call!(
            self,
            ConnectorsClient,
            get_connection,
            pb::GetConnectionRequest { name }
        )?;
        domain::ConnectionInfo::try_from(c).map_err(|e| bad_reply(&e.message))
    }

    async fn list_connections(&self) -> Result<Vec<domain::ConnectionInfo>, ApiError> {
        let r = call!(
            self,
            ConnectorsClient,
            list_connections,
            pb::ListConnectionsRequest {}
        )?;
        r.connections
            .into_iter()
            .map(|c| domain::ConnectionInfo::try_from(c).map_err(|e| bad_reply(&e.message)))
            .collect()
    }

    async fn delete_connection(&self, name: String) -> Result<(), ApiError> {
        call!(
            self,
            ConnectorsClient,
            delete_connection,
            pb::DeleteConnectionRequest { name }
        )?;
        Ok(())
    }

    async fn put_egress(&self, e: domain::EgressInfo) -> Result<domain::EgressInfo, ApiError> {
        let e = call!(
            self,
            ConnectorsClient,
            put_egress,
            pb::PutEgressRequest {
                egress: Some(e.into()),
            }
        )?;
        Ok(e.into())
    }

    async fn get_egress(&self, module: String) -> Result<domain::EgressInfo, ApiError> {
        let e = call!(
            self,
            ConnectorsClient,
            get_egress,
            pb::GetEgressRequest { module }
        )?;
        Ok(e.into())
    }

    async fn list_egress(&self) -> Result<Vec<domain::EgressInfo>, ApiError> {
        let r = call!(
            self,
            ConnectorsClient,
            list_egress,
            pb::ListEgressRequest {}
        )?;
        Ok(r.policies.into_iter().map(Into::into).collect())
    }

    async fn delete_egress(&self, module: String) -> Result<(), ApiError> {
        call!(
            self,
            ConnectorsClient,
            delete_egress,
            pb::DeleteEgressRequest { module }
        )?;
        Ok(())
    }

    /// REST-only (the presign signature is checked by the serving node's local route).
    async fn presigned_object(
        &self,
        req: domain::PresignedObjectRequest,
    ) -> Result<domain::PresignedObjectResponse, ApiError> {
        let _ = req;
        Err(rest_only("presigned_object"))
    }

    async fn get_queue_flow(&self, queue: String) -> Result<phase7::QueueFlowInfo, ApiError> {
        let i = call!(
            self,
            QueuesClient,
            get_queue_flow,
            pb::GetQueueFlowRequest { name: queue }
        )?;
        queue_flow_info(i)
    }

    async fn put_queue_flow(
        &self,
        queue: String,
        flow: phase7::QueueFlow,
        expected_version: Option<u64>,
    ) -> Result<phase7::QueueFlowInfo, ApiError> {
        let i = call!(
            self,
            QueuesClient,
            put_queue_flow,
            pb::PutQueueFlowRequest {
                name: queue,
                flow: Some(flow.into()),
                expected_version,
            }
        )?;
        queue_flow_info(i)
    }

    async fn pause_queue(&self, queue: String) -> Result<phase7::PauseState, ApiError> {
        let s = call!(
            self,
            QueuesClient,
            pause_queue,
            pb::PauseQueueRequest { name: queue }
        )?;
        Ok(phase7::PauseState {
            name: s.queue,
            paused: s.paused,
            changed: s.changed,
        })
    }

    async fn resume_queue(&self, queue: String) -> Result<phase7::PauseState, ApiError> {
        let s = call!(
            self,
            QueuesClient,
            resume_queue,
            pb::ResumeQueueRequest { name: queue }
        )?;
        Ok(phase7::PauseState {
            name: s.queue,
            paused: s.paused,
            changed: s.changed,
        })
    }

    async fn pause_workflow(&self, workflow: String) -> Result<phase7::PauseState, ApiError> {
        let s = call!(
            self,
            WorkflowsClient,
            pause_workflow,
            pb::PauseWorkflowRequest { name: workflow }
        )?;
        Ok(phase7::PauseState {
            name: s.workflow,
            paused: s.paused,
            changed: s.changed,
        })
    }

    async fn resume_workflow(&self, workflow: String) -> Result<phase7::PauseState, ApiError> {
        let s = call!(
            self,
            WorkflowsClient,
            resume_workflow,
            pb::ResumeWorkflowRequest { name: workflow }
        )?;
        Ok(phase7::PauseState {
            name: s.workflow,
            paused: s.paused,
            changed: s.changed,
        })
    }

    async fn pause_run(
        &self,
        run_id: String,
        detail: Option<String>,
    ) -> Result<phase7::RunPauseState, ApiError> {
        let s = call!(
            self,
            RunsClient,
            pause_run,
            pb::PauseRunRequest { run_id, detail }
        )?;
        Ok(phase7::RunPauseState {
            run_id: s.run_id,
            paused: s.paused,
            changed: s.changed,
        })
    }

    async fn resume_run(&self, run_id: String) -> Result<phase7::RunPauseState, ApiError> {
        let s = call!(
            self,
            RunsClient,
            resume_run,
            pb::ResumeRunRequest { run_id }
        )?;
        Ok(phase7::RunPauseState {
            run_id: s.run_id,
            paused: s.paused,
            changed: s.changed,
        })
    }

    async fn queue_demand(&self, queue: String) -> Result<phase7::DemandSnapshot, ApiError> {
        let d = call!(
            self,
            QueuesClient,
            get_queue_demand,
            pb::GetQueueDemandRequest { name: queue }
        )?;
        Ok(phase7::DemandSnapshot {
            placement: placement(d.placement)?,
            queue: d.queue,
            demand: d.demand,
            backlog: d.backlog,
            running_on_workers: d.running_on_workers,
            spilling: d.spilling,
            workers: d.workers,
            worker_slots_free: d.worker_slots_free,
        })
    }

    async fn list_workers(&self) -> Result<Vec<phase7::WorkerInfo>, ApiError> {
        let r = call!(self, WorkersClient, list_workers, pb::ListWorkersRequest {})?;
        Ok(r.workers
            .into_iter()
            .map(|w| phase7::WorkerInfo {
                worker_id: w.worker_id,
                labels: w.labels.into_iter().collect(),
                slots: w.slots,
                free_slots: w.free_slots,
                leases: w.leases,
                connected_at_ms: w.connected_at_ms,
                node_id: w.node_id,
                draining: w.draining,
            })
            .collect())
    }

    async fn drain_worker(&self, worker_id: String) -> Result<phase7::DrainResult, ApiError> {
        let d = call!(
            self,
            WorkersClient,
            drain_worker,
            pb::DrainWorkerRequest { worker_id }
        )?;
        Ok(phase7::DrainResult {
            worker_id: d.worker_id,
            already_draining: d.already_draining,
        })
    }

    async fn create_worker_token(
        &self,
        req: phase7::CreateWorkerTokenRequest,
    ) -> Result<phase7::WorkerTokenCreated, ApiError> {
        let t = call!(
            self,
            WorkersClient,
            create_worker_token,
            pb::CreateWorkerTokenRequest { ttl_ms: req.ttl_ms }
        )?;
        Ok(phase7::WorkerTokenCreated {
            token_id: t.token_id,
            token: t.token,
            join_command: t.join_command,
            expires_at_ms: t.expires_at_ms,
        })
    }

    async fn list_worker_tokens(&self) -> Result<Vec<phase7::WorkerTokenInfo>, ApiError> {
        let r = call!(
            self,
            WorkersClient,
            list_worker_tokens,
            pb::ListWorkerTokensRequest {}
        )?;
        Ok(r.tokens
            .into_iter()
            .map(|t| phase7::WorkerTokenInfo {
                token_id: t.token_id,
                created_at_ms: t.created_at_ms,
                expires_at_ms: t.expires_at_ms,
            })
            .collect())
    }

    async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError> {
        call!(
            self,
            WorkersClient,
            revoke_worker_token,
            pb::RevokeWorkerTokenRequest { token_id }
        )?;
        Ok(())
    }

    async fn join_worker(
        &self,
        req: phase7::JoinRequest,
    ) -> Result<phase7::JoinResponse, ApiError> {
        let j = call!(
            self,
            WorkersClient,
            join_worker,
            pb::JoinWorkerRequest {
                token: req.token,
                csr_pem: req.csr_pem,
                worker_id: req.worker_id,
                labels: req.labels.into_iter().collect(),
            }
        )?;
        Ok(phase7::JoinResponse {
            client_cert_pem: j.client_cert_pem,
            ca_cert_pem: j.ca_cert_pem,
            endpoints: j.endpoints,
            renew_after_ms: j.renew_after_ms,
            cluster_id: j.cluster_id,
        })
    }

    /// REST-only tooling (D-P10-18).
    async fn dev_time(&self) -> Result<domain::DevTimeInfo, ApiError> {
        Err(rest_only("dev_time"))
    }

    /// REST-only tooling (D-P10-18).
    async fn dev_skip(&self, s: domain::DevSkip) -> Result<domain::DevSkipResult, ApiError> {
        let _ = s;
        Err(rest_only("dev_skip"))
    }

    /// REST-only tooling (D-P10-18).
    async fn replay_module(
        &self,
        r: domain::ReplayRequest,
    ) -> Result<domain::ReplayResult, ApiError> {
        let _ = r;
        Err(rest_only("replay_module"))
    }
}

// ---------------------------------------------------------------------------------
// Phase 8 slots
// ---------------------------------------------------------------------------------

#[async_trait::async_trait]
impl ScheduleApi for GrpcControlPlane {
    async fn list_schedules(&self) -> Result<Vec<ScheduleDto>, ApiError> {
        let r = call!(self, SchedulesClient, list, pb::ListSchedulesRequest {})?;
        r.schedules.into_iter().map(schedule).collect()
    }

    async fn get_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        schedule(call!(
            self,
            SchedulesClient,
            get,
            pb::GetScheduleRequest { name }
        )?)
    }

    async fn create_schedule(&self, s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        schedule(call!(
            self,
            SchedulesClient,
            create,
            pb::CreateScheduleRequest {
                schedule: Some(s.into()),
            }
        )?)
    }

    async fn update_schedule(&self, s: ScheduleDto) -> Result<ScheduleDto, ApiError> {
        schedule(call!(
            self,
            SchedulesClient,
            update,
            pb::UpdateScheduleRequest {
                schedule: Some(s.into()),
            }
        )?)
    }

    async fn delete_schedule(
        &self,
        name: String,
        version_tag: Option<u64>,
    ) -> Result<(), ApiError> {
        call!(
            self,
            SchedulesClient,
            delete,
            pb::DeleteScheduleRequest { name, version_tag }
        )?;
        Ok(())
    }

    async fn pause_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        schedule(call!(
            self,
            SchedulesClient,
            pause,
            pb::PauseScheduleRequest { name }
        )?)
    }

    async fn resume_schedule(&self, name: String) -> Result<ScheduleDto, ApiError> {
        schedule(call!(
            self,
            SchedulesClient,
            resume,
            pb::ResumeScheduleRequest { name }
        )?)
    }

    async fn trigger_schedule(
        &self,
        name: String,
        request_id: Option<String>,
    ) -> Result<String, ApiError> {
        let r = call!(
            self,
            SchedulesClient,
            trigger,
            pb::TriggerScheduleRequest { name, request_id }
        )?;
        Ok(r.run_id)
    }
}

#[async_trait::async_trait]
impl WebhookApi for GrpcControlPlane {
    async fn list_webhooks(&self) -> Result<Vec<WebhookDto>, ApiError> {
        let r = call!(self, WebhooksClient, list, pb::ListWebhooksRequest {})?;
        r.webhooks.into_iter().map(webhook).collect()
    }

    async fn get_webhook(&self, name: String) -> Result<WebhookDto, ApiError> {
        webhook(call!(
            self,
            WebhooksClient,
            get,
            pb::GetWebhookRequest { name }
        )?)
    }

    async fn create_webhook(&self, w: WebhookDto) -> Result<WebhookDto, ApiError> {
        webhook(call!(
            self,
            WebhooksClient,
            create,
            pb::CreateWebhookRequest {
                webhook: Some(w.into()),
            }
        )?)
    }

    async fn update_webhook(&self, w: WebhookDto) -> Result<WebhookDto, ApiError> {
        webhook(call!(
            self,
            WebhooksClient,
            update,
            pb::UpdateWebhookRequest {
                webhook: Some(w.into()),
            }
        )?)
    }

    async fn delete_webhook(&self, name: String, version_tag: Option<u64>) -> Result<(), ApiError> {
        call!(
            self,
            WebhooksClient,
            delete,
            pb::DeleteWebhookRequest { name, version_tag }
        )?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl HookIngress for GrpcControlPlane {
    /// REST-only (`POST /hooks/{name}`; the raw body and headers are verified locally).
    async fn deliver(&self, req: HookRequest) -> Result<HookResponse, ApiError> {
        let _ = req;
        Err(rest_only("deliver"))
    }
}

#[async_trait::async_trait]
impl RunLogsApi for GrpcControlPlane {
    async fn run_logs(&self, req: RunLogsRequest) -> Result<RunLogsResponse, ApiError> {
        let r = call!(
            self,
            RunLogsClient,
            get_run_logs,
            pb::GetRunLogsRequest {
                run_id: req.run_id,
                limit: req.limit,
                level: req.level.map(level_to_proto),
            }
        )?;
        Ok(RunLogsResponse {
            run_id: r.run_id,
            lines: r
                .lines
                .into_iter()
                .map(|l| {
                    Ok(RunLogLine {
                        level: level_from_proto(l.level)?,
                        fields: json_from("fields_json", &l.fields_json)?,
                        ts_ms: l.ts_ms,
                        message: l.message,
                        attempt: l.attempt,
                    })
                })
                .collect::<Result<_, ApiError>>()?,
            dropped: r.dropped,
        })
    }
}

#[async_trait::async_trait]
impl AdminApi for GrpcControlPlane {
    /// REST-only (`GET /v1/admin/backup` streams the serving node's own data).
    async fn backup(&self) -> Result<BackupStream, ApiError> {
        Err(rest_only("backup"))
    }
}

// ---------------------------------------------------------------------------------
// Phase 11 slots
// ---------------------------------------------------------------------------------

fn reply<T, U: TryInto<T, Error = ApiError>>(u: U) -> Result<T, ApiError> {
    u.try_into().map_err(|e| bad_reply(&e.message))
}

#[async_trait::async_trait]
impl OpsApi for GrpcControlPlane {
    async fn rerun_run(
        &self,
        req: phase11::RerunRequest,
    ) -> Result<phase11::RerunResponse, ApiError> {
        Ok(call!(self, RunOpsClient, rerun, pb::RerunRequest::from(req))?.into())
    }

    async fn analyze_rerun(&self, run_id: String) -> Result<phase11::RerunCutsView, ApiError> {
        Ok(call!(
            self,
            RunOpsClient,
            rerun_cuts,
            pb::RerunCutsRequest { run_id }
        )?
        .into())
    }

    async fn list_dlq(&self, req: phase11::DlqListRequest) -> Result<phase11::DlqPage, ApiError> {
        reply(call!(
            self,
            RunOpsClient,
            list_dlq,
            pb::ListDlqRequest {
                workflow: req.workflow,
                queue: req.queue,
                reason: req.reason,
                tags: req.tags,
                include_dismissed: req.include_dismissed,
                limit: req.limit,
                page_token: req.page_token,
            }
        )?)
    }

    async fn requeue_dlq(
        &self,
        req: phase11::DlqRequeueRequest,
    ) -> Result<phase11::DlqRequeueResponse, ApiError> {
        Ok(call!(
            self,
            RunOpsClient,
            requeue_dlq,
            pb::RequeueDlqRequest {
                run_id: req.run_id,
                mode: Some(req.mode.into()),
                dry_run: req.dry_run,
            }
        )?
        .into())
    }

    async fn dismiss_dlq(
        &self,
        req: phase11::DlqDismissRequest,
    ) -> Result<phase11::DlqEntry, ApiError> {
        reply(call!(
            self,
            RunOpsClient,
            dismiss_dlq,
            pb::DismissDlqRequest {
                run_id: req.run_id,
                reason: req.reason,
            }
        )?)
    }

    async fn create_bulk(
        &self,
        req: phase11::BulkCreateRequest,
    ) -> Result<phase11::BulkCreateResponse, ApiError> {
        Ok(call!(
            self,
            RunOpsClient,
            create_bulk,
            pb::CreateBulkRequest::from(req)
        )?
        .into())
    }

    async fn list_bulk(&self) -> Result<Vec<phase11::BulkJobView>, ApiError> {
        let r = call!(self, RunOpsClient, list_bulk, pb::ListBulkRequest {})?;
        Ok(r.jobs.into_iter().map(Into::into).collect())
    }

    async fn get_bulk(&self, job_id: String) -> Result<phase11::BulkJobView, ApiError> {
        Ok(call!(self, RunOpsClient, get_bulk, pb::GetBulkRequest { job_id })?.into())
    }

    async fn cancel_bulk(&self, job_id: String) -> Result<phase11::BulkJobView, ApiError> {
        Ok(call!(
            self,
            RunOpsClient,
            cancel_bulk,
            pb::CancelBulkRequest { job_id }
        )?
        .into())
    }

    async fn limits(&self) -> Result<phase11::LimitsView, ApiError> {
        Ok(call!(self, RunOpsClient, get_limits, pb::GetLimitsRequest {})?.into())
    }

    async fn get_failure_policy(
        &self,
        scope: phase11::PolicyScope,
    ) -> Result<phase11::FailurePolicy, ApiError> {
        use pb::get_failure_policy_request::Scope;
        let scope = match scope {
            phase11::PolicyScope::Workflow(n) => Scope::Workflow(n),
            phase11::PolicyScope::Queue(n) => Scope::Queue(n),
        };
        Ok(call!(
            self,
            RunOpsClient,
            get_failure_policy,
            pb::GetFailurePolicyRequest { scope: Some(scope) }
        )?
        .into())
    }

    async fn set_failure_policy(
        &self,
        scope: phase11::PolicyScope,
        p: phase11::FailurePolicy,
    ) -> Result<phase11::FailurePolicy, ApiError> {
        use pb::set_failure_policy_request::Scope;
        let scope = match scope {
            phase11::PolicyScope::Workflow(n) => Scope::Workflow(n),
            phase11::PolicyScope::Queue(n) => Scope::Queue(n),
        };
        Ok(call!(
            self,
            RunOpsClient,
            set_failure_policy,
            pb::SetFailurePolicyRequest {
                policy: Some(p.into()),
                scope: Some(scope),
            }
        )?
        .into())
    }
}

#[async_trait::async_trait]
impl InventoryApi for GrpcControlPlane {
    async fn list_modules(
        &self,
        req: phase11::ListModulesRequest,
    ) -> Result<phase11::ModulePage, ApiError> {
        Ok(call!(
            self,
            InventoryClient,
            list_modules,
            pb::ListModulesRequest {
                limit: req.limit,
                page_token: req.page_token,
            }
        )?
        .into())
    }

    async fn list_queues(&self) -> Result<Vec<phase11::QueueView>, ApiError> {
        let r = call!(self, InventoryClient, list_queues, pb::ListQueuesRequest {})?;
        r.queues.into_iter().map(reply).collect()
    }

    async fn list_partitions(&self) -> Result<Vec<phase11::PartitionView>, ApiError> {
        let r = call!(
            self,
            InventoryClient,
            list_partitions,
            pb::ListPartitionsRequest {}
        )?;
        r.partitions.into_iter().map(reply).collect()
    }
}

// ---------------------------------------------------------------------------------
// Phase 13 slots
// ---------------------------------------------------------------------------------

#[async_trait::async_trait]
impl ClusterApi for GrpcControlPlane {
    async fn init(&self, r: phase13::InitRequest) -> Result<phase13::ClusterStatus, ApiError> {
        reply::<phase13::ClusterStatus, _>(call!(self, ClusterClient, init, init_to_proto(r))?)
    }

    async fn join(&self, r: phase13::JoinRequest) -> Result<phase13::JoinResponse, ApiError> {
        reply(call!(
            self,
            ClusterClient,
            join,
            pb::JoinClusterRequest::from(r)
        )?)
    }

    async fn status(&self) -> Result<phase13::ClusterStatus, ApiError> {
        reply::<phase13::ClusterStatus, _>(call!(
            self,
            ClusterClient,
            status,
            pb::ClusterStatusRequest {}
        )?)
    }

    async fn create_node_token(
        &self,
        r: phase13::NodeTokenRequest,
    ) -> Result<phase13::NodeTokenResponse, ApiError> {
        Ok(call!(
            self,
            ClusterClient,
            create_node_token,
            pb::CreateNodeTokenRequest {
                ttl_secs: r.ttl_secs,
            }
        )?
        .into())
    }

    async fn remove(&self, id: u64, force: bool) -> Result<phase13::ClusterStatus, ApiError> {
        reply::<phase13::ClusterStatus, _>(call!(
            self,
            ClusterClient,
            remove,
            pb::RemoveMemberRequest { node_id: id, force }
        )?)
    }

    async fn drain(&self, id: u64) -> Result<phase13::ClusterStatus, ApiError> {
        reply::<phase13::ClusterStatus, _>(call!(
            self,
            ClusterClient,
            drain,
            pb::DrainMemberRequest { node_id: id }
        )?)
    }

    async fn reassign(
        &self,
        r: phase13::ReassignRequest,
    ) -> Result<phase13::ClusterStatus, ApiError> {
        reply::<phase13::ClusterStatus, _>(call!(
            self,
            ClusterClient,
            reassign,
            pb::ReassignPartitionRequest::from(r)
        )?)
    }

    async fn rebalance(&self) -> Result<phase13::ClusterStatus, ApiError> {
        reply::<phase13::ClusterStatus, _>(call!(
            self,
            ClusterClient,
            rebalance,
            pb::RebalanceRequest {}
        )?)
    }

    async fn upgrade_status(&self) -> Result<phase13::UpgradeStatus, ApiError> {
        Ok(call!(
            self,
            ClusterClient,
            upgrade_status,
            pb::UpgradeStatusRequest {}
        )?
        .into())
    }

    async fn upgrade_finalize(&self) -> Result<phase13::UpgradeStatus, ApiError> {
        Ok(call!(
            self,
            ClusterClient,
            upgrade_finalize,
            pb::UpgradeFinalizeRequest {}
        )?
        .into())
    }

    async fn upgrade_hold(&self) -> Result<phase13::UpgradeStatus, ApiError> {
        Ok(call!(self, ClusterClient, upgrade_hold, pb::UpgradeHoldRequest {})?.into())
    }

    async fn upgrade_release(&self) -> Result<phase13::UpgradeStatus, ApiError> {
        Ok(call!(
            self,
            ClusterClient,
            upgrade_release,
            pb::UpgradeReleaseRequest {}
        )?
        .into())
    }
}

#[async_trait::async_trait]
impl ModuleLifecycleApi for GrpcControlPlane {
    async fn retire_module(&self, sha: String) -> Result<phase13::RetireModuleResponse, ApiError> {
        Ok(call!(
            self,
            ModuleLifecycleClient,
            retire_module,
            pb::RetireModuleRequest { sha256: sha }
        )?
        .into())
    }

    async fn run_module_gc(&self) -> Result<phase13::GcRunReport, ApiError> {
        Ok(call!(
            self,
            ModuleLifecycleClient,
            run_module_gc,
            pb::RunModuleGcRequest {}
        )?
        .into())
    }

    async fn migrate_runs(
        &self,
        r: phase13::MigrateRunsRequest,
    ) -> Result<phase13::MigrateRunsReport, ApiError> {
        Ok(call!(
            self,
            ModuleLifecycleClient,
            migrate_runs,
            pb::MigrateRunsRequest::from(r)
        )?
        .into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// The `fn` names declared in the body of `pub trait <name>` in `src`.
    fn trait_fns(src: &str, name: &str) -> BTreeSet<String> {
        block_fns(src, &format!("pub trait {name}:"))
    }

    /// The `fn` names in the brace block that follows the first line containing `head`.
    fn block_fns(src: &str, head: &str) -> BTreeSet<String> {
        let start = src
            .find(head)
            .unwrap_or_else(|| panic!("{head:?} not found"));
        let body_start = start + src[start..].find('{').expect("block opens");
        let mut depth = 0usize;
        let mut end = body_start;
        for (i, c) in src[body_start..].char_indices() {
            match c {
                '{' => depth += 1,
                '}' => {
                    depth -= 1;
                    if depth == 0 {
                        end = body_start + i;
                        break;
                    }
                }
                _ => {}
            }
        }
        let mut out = BTreeSet::new();
        // Only top-level items of the block (one indentation level).
        for line in src[body_start..end].lines() {
            let item = line
                .strip_prefix("    ")
                .filter(|l| !l.starts_with(' '))
                .unwrap_or("");
            let rest = item
                .strip_prefix("async fn ")
                .or_else(|| item.strip_prefix("fn "));
            if let Some(rest) = rest {
                let name: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                    .collect();
                out.insert(name);
            }
        }
        out
    }

    #[test]
    fn every_trait_method_is_overridden() {
        let me = include_str!("grpc_client.rs");
        let sources = [
            ("ControlPlane", include_str!("domain.rs")),
            ("ScheduleApi", include_str!("phase8.rs")),
            ("WebhookApi", include_str!("phase8.rs")),
            ("HookIngress", include_str!("phase8.rs")),
            ("RunLogsApi", include_str!("phase8.rs")),
            ("AdminApi", include_str!("phase8.rs")),
            ("OpsApi", include_str!("phase11.rs")),
            ("InventoryApi", include_str!("phase11.rs")),
            ("ClusterApi", include_str!("phase13.rs")),
            ("ModuleLifecycleApi", include_str!("phase13.rs")),
        ];
        let mut total = 0;
        for (name, src) in sources {
            let declared = trait_fns(src, name);
            assert!(!declared.is_empty(), "{name}: no methods parsed");
            let implemented = block_fns(me, &format!("impl {name} for GrpcControlPlane"));
            assert_eq!(
                declared, implemented,
                "{name}: GrpcControlPlane must override exactly the trait's methods"
            );
            total += declared.len();
        }
        // ControlPlane 52 + Phase 8 16 + Phase 11 15 + Phase 13 15 (at 13-03).
        assert!(total >= 98, "{total}");
    }

    #[test]
    fn status_to_error_inverts_to_status() {
        use crate::grpc::to_status;
        let cases = [
            ApiError::invalid_argument("bad").with_details(serde_json::json!({"field": "x"})),
            ApiError::not_found("nope"),
            ApiError::conflict("dup").with_details(serde_json::json!({"reason": "run_terminal"})),
            ApiError::precondition_failed("pre"),
            ApiError::payload_too_large("big"),
            ApiError::unavailable("not leader").with_details(serde_json::json!({
                "reason": "not_leader", "partition": 3, "leader": 2,
                "leader_addr": "10.0.0.2:7071", "epoch": 9
            })),
            ApiError::internal("boom"),
            ApiError::unimplemented("x"),
            ApiError::rate_limited("slow down", 1500),
        ];
        for e in cases {
            assert_eq!(status_to_error(&to_status(e.clone())), e);
        }
        // `rate_limited` is told apart from `payload_too_large` (both RESOURCE_EXHAUSTED) by
        // its `retry_after_ms` detail, which `ApiError::rate_limited` always sets (above).
        for c in ErrorCode::ALL
            .into_iter()
            .filter(|c| *c != ErrorCode::RateLimited)
        {
            let e = ApiError::new(c, "m");
            assert_eq!(status_to_error(&to_status(e.clone())).code, c, "{c:?}");
        }
        let e = status_to_error(&tonic::Status::deadline_exceeded("late"));
        assert_eq!(e.code, ErrorCode::Unavailable);
    }

    #[tokio::test]
    async fn spent_deadline_fails_before_sending() {
        let meta = RequestMeta {
            deadline: Some(Instant::now()),
            ..RequestMeta::default()
        };
        let r = request_meta::scope(meta, async { request(()) }).await;
        assert_eq!(r.expect_err("spent").code, ErrorCode::Unavailable);
        let meta = RequestMeta {
            hop: 1,
            deadline: Some(Instant::now() + Duration::from_secs(30)),
            ..RequestMeta::default()
        };
        let r = request_meta::scope(meta, async { request(()) })
            .await
            .expect("request");
        assert_eq!(
            r.metadata()
                .get(request_meta::HEADER_HOP)
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
    }
}
