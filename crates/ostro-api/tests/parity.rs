//! REST vs gRPC parity on one port (API-01), against `FakeControlPlane`.
//!
//! One scenario, generic over [`Client`], runs twice against two fresh fakes served by
//! `router()` on 127.0.0.1:0: once over HTTP/1.1 JSON (reqwest), once over gRPC (tonic
//! h2c) to the same kind of listener. Each step is normalised to a transport-neutral
//! JSON value (errors to their [`ErrorCode`] plus details) and the two transcripts must
//! be equal.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use ostro_api::domain::RunStatus;
use ostro_api::fake::{FakeControlPlane, fake_module, sha256_hex};
use ostro_api::grpc::status_details;
use ostro_api::rest::friendly_ms;
use ostro_api::{ApiConfig, ControlPlane, ErrorCode};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{
    modules_client::ModulesClient, runs_client::RunsClient, tokens_client::TokensClient,
    workflows_client::WorkflowsClient,
};
use serde_json::{Value, json};
use tonic::transport::Channel;

/// Normalised outcome of one step: a domain-shaped JSON value, or the error code and
/// structured details.
type Out = Result<Value, (ErrorCode, Option<Value>)>;

const MIB: usize = 1024 * 1024;

fn cfg() -> ApiConfig {
    ApiConfig {
        max_module_bytes: 8 * MIB,
        max_request_bytes: MIB,
    }
}

struct Server {
    addr: SocketAddr,
    cp: Arc<FakeControlPlane>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(cfg: ApiConfig) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let cp = Arc::new(FakeControlPlane::new());
    let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
    let app = ostro_api::router(dyn_cp, cfg);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    Server { addr, cp, task }
}

// ---------------------------------------------------------------------------------
// Normalisation (shared by both clients)
// ---------------------------------------------------------------------------------

/// Embedded JSON from JSON text, as REST renders it.
fn embed(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

fn status_name(v: i32) -> String {
    let st = match pb::RunStatus::try_from(v).expect("known status") {
        pb::RunStatus::Scheduled => RunStatus::Scheduled,
        pb::RunStatus::Ready => RunStatus::Ready,
        pb::RunStatus::Running => RunStatus::Running,
        pb::RunStatus::Completed => RunStatus::Completed,
        pb::RunStatus::Failed => RunStatus::Failed,
        pb::RunStatus::Cancelled => RunStatus::Cancelled,
        pb::RunStatus::Terminated => RunStatus::Terminated,
        pb::RunStatus::TimedOut => RunStatus::TimedOut,
        pb::RunStatus::ContinuedAsNew => RunStatus::ContinuedAsNew,
        pb::RunStatus::Unspecified => panic!("unspecified status on the wire"),
    };
    st.as_str().to_owned()
}

fn status_proto(s: &str) -> i32 {
    let st = RunStatus::parse(s).expect("status name");
    let p = match st {
        RunStatus::Scheduled => pb::RunStatus::Scheduled,
        RunStatus::Ready => pb::RunStatus::Ready,
        RunStatus::Running => pb::RunStatus::Running,
        RunStatus::Completed => pb::RunStatus::Completed,
        RunStatus::Failed => pb::RunStatus::Failed,
        RunStatus::Cancelled => pb::RunStatus::Cancelled,
        RunStatus::Terminated => pb::RunStatus::Terminated,
        RunStatus::TimedOut => pb::RunStatus::TimedOut,
        RunStatus::ContinuedAsNew => pb::RunStatus::ContinuedAsNew,
    };
    p as i32
}

fn norm_module(m: &pb::ModuleInfo) -> Value {
    json!({
        "sha256": m.sha256,
        "size_bytes": m.size_bytes,
        "language": m.language,
        "workflows": m.workflows.iter().map(|w| json!([w.name, w.version])).collect::<Vec<_>>(),
        "uploaded_at_ms": m.uploaded_at_ms,
    })
}

fn norm_module_rest(m: &Value) -> Value {
    json!({
        "sha256": m["sha256"],
        "size_bytes": m["size_bytes"],
        "language": m["language"],
        "workflows": m["workflows"].as_array().expect("workflows").iter()
            .map(|w| json!([w["name"], w["version"]])).collect::<Vec<_>>(),
        "uploaded_at_ms": m["uploaded_at_ms"],
    })
}

fn norm_workflow(w: &pb::WorkflowInfo) -> Value {
    json!({
        "name": w.name,
        "active_version": w.active_version,
        "record_version": w.record_version,
        "versions": w.versions.iter().map(|v| json!({
            "version": v.version,
            "module_sha256": v.module_sha256,
            "default_queue": v.default_queue,
            "run_timeout_ms": v.run_timeout_ms,
        })).collect::<Vec<_>>(),
    })
}

fn norm_workflow_rest(w: &Value) -> Value {
    json!({
        "name": w["name"],
        "active_version": w["active_version"],
        "record_version": w["record_version"],
        "versions": w["versions"].as_array().expect("versions").iter().map(|v| json!({
            "version": v["version"],
            "module_sha256": v["module_sha256"],
            "default_queue": v["default_queue"],
            "run_timeout_ms": v["run_timeout_ms"],
        })).collect::<Vec<_>>(),
    })
}

fn norm_run(r: &pb::RunInfo) -> Value {
    json!({
        "run_id": r.run_id,
        "workflow": r.workflow,
        "version": r.version,
        "module_sha256": r.module_sha256,
        "queue": r.queue,
        "status": status_name(r.status),
        "attempt": r.attempt,
        "tags": r.tags,
        "tenant": r.tenant,
        "traceparent": r.traceparent,
        "priority": r.priority,
        "concurrency_key": r.concurrency_key,
        "created_at_ms": r.created_at_ms,
        "completed_at_ms": r.completed_at_ms,
        "published_status": r.published_status_json.as_deref().map(embed),
        "result": r.result_json.as_deref().map(embed),
        "failure": r.failure,
        "attempt_failures": r.attempt_failures,
        "retention_ms": r.retention_ms,
        "schedule_to_start_ms": r.schedule_to_start_ms,
    })
}

fn norm_run_rest(r: &Value) -> Value {
    let mut out = serde_json::Map::new();
    for k in [
        "run_id",
        "workflow",
        "version",
        "module_sha256",
        "queue",
        "status",
        "attempt",
        "tags",
        "tenant",
        "traceparent",
        "priority",
        "concurrency_key",
        "created_at_ms",
        "completed_at_ms",
        "published_status",
        "result",
        "failure",
        "attempt_failures",
        "retention_ms",
        "schedule_to_start_ms",
    ] {
        out.insert(k.to_owned(), r[k].clone());
    }
    Value::Object(out)
}

fn norm_summary(r: &pb::RunSummary) -> Value {
    json!({
        "run_id": r.run_id,
        "workflow": r.workflow,
        "version": r.version,
        "status": status_name(r.status),
        "queue": r.queue,
        "created_at_ms": r.created_at_ms,
        "tags": r.tags,
    })
}

fn norm_summary_rest(r: &Value) -> Value {
    json!({
        "run_id": r["run_id"],
        "workflow": r["workflow"],
        "version": r["version"],
        "status": r["status"],
        "queue": r["queue"],
        "created_at_ms": r["created_at_ms"],
        "tags": r["tags"],
    })
}

// ---------------------------------------------------------------------------------
// The two clients
// ---------------------------------------------------------------------------------

/// Start-run arguments shared by both transports.
#[derive(Clone, Debug, Default)]
struct Start {
    workflow: String,
    /// Workflow input.
    input: Value,
    /// Send syntactically invalid JSON instead of `input`.
    raw_invalid_input: bool,
    version: Option<u32>,
    queue: Option<String>,
    delay_ms: Option<u64>,
    tags: Vec<String>,
    tenant: Option<String>,
    idempotency_key: Option<String>,
    reject: bool,
    /// 05-13 start fields.
    retention_ms: Option<u64>,
    schedule_to_start_ms: Option<u64>,
    dry_run: bool,
}

/// Run listing arguments.
#[derive(Clone, Debug, Default)]
struct List {
    workflow: Option<String>,
    status: Option<&'static str>,
    tag: Option<String>,
    limit: u32,
    page_token: Option<String>,
}

/// Run policy shared by both transports (ms; REST sends friendly strings).
#[derive(Clone, Copy, Debug, Default)]
struct Policy {
    retention_ms: Option<u64>,
    schedule_to_start_ms: Option<u64>,
}

fn norm_policy(retention_ms: Option<u64>, schedule_to_start_ms: Option<u64>) -> Value {
    json!({ "retention_ms": retention_ms, "schedule_to_start_ms": schedule_to_start_ms })
}

fn norm_result(
    run_id: &str,
    status: String,
    completed: bool,
    result: Option<Value>,
    failure: Option<Value>,
) -> Value {
    json!({
        "run_id": run_id, "status": status, "completed": completed,
        "has_result": result.is_some(), "result": result,
        "has_failure": failure.is_some(), "failure": failure,
    })
}

trait Client {
    async fn upload(&mut self, sha: &str, wasm: Bytes) -> Out;
    /// Upload with the D23 tenant and the deployed module name (05-24).
    async fn upload_named(&mut self, sha: &str, wasm: Bytes, tenant: &str, name: &str) -> Out;
    async fn get_module(&mut self, sha: &str) -> Out;
    async fn list_workflows(&mut self) -> Out;
    async fn activate(&mut self, name: &str, version: u32, expected: Option<u64>) -> Out;
    async fn start(&mut self, s: Start) -> Out;
    async fn get_run(&mut self, run_id: &str) -> Out;
    async fn history(&mut self, run_id: &str, from_seq: u64, limit: u32) -> Out;
    async fn list_runs(&mut self, l: List) -> Out;
    async fn signal(
        &mut self,
        run_id: &str,
        name: &str,
        payload: Value,
        request_id: Option<&str>,
        dry_run: bool,
    ) -> Out;
    async fn cancel(&mut self, run_id: &str, reason: Option<&str>, dry_run: bool) -> Out;
    async fn terminate(&mut self, run_id: &str, reason: Option<&str>, dry_run: bool) -> Out;
    async fn retry(&mut self, run_id: &str, dry_run: bool) -> Out;
    async fn result(&mut self, run_id: &str, wait_ms: u64, follow: bool) -> Out;
    async fn complete_token(&mut self, token: &str, payload: Value, dry_run: bool) -> Out;
    async fn set_workflow_policy(&mut self, name: &str, p: Policy) -> Out;
    async fn get_workflow_policy(&mut self, name: &str) -> Out;
    async fn set_queue_policy(&mut self, name: &str, p: Policy) -> Out;
    async fn get_queue_policy(&mut self, name: &str) -> Out;
}

struct RestClient {
    base: String,
    http: reqwest::Client,
}

impl RestClient {
    fn new(addr: SocketAddr) -> Self {
        RestClient {
            base: format!("http://{addr}"),
            http: reqwest::Client::builder()
                .http1_only()
                .build()
                .expect("client"),
        }
    }

    async fn send(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<Value, (ErrorCode, Option<Value>)> {
        let resp = req.send().await.expect("http request");
        let status = resp.status();
        let text = resp.text().await.expect("body");
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::String(text));
        if status.is_success() {
            Ok(v)
        } else {
            let code = ErrorCode::from_http_status(
                axum::http::StatusCode::from_u16(status.as_u16()).expect("status"),
            );
            assert_eq!(v["error"]["code"], code.as_str(), "envelope: {v}");
            let details = v["error"].get("details").cloned();
            Err((code, details))
        }
    }
}

impl Client for RestClient {
    async fn upload(&mut self, sha: &str, wasm: Bytes) -> Out {
        let req = self
            .http
            .put(format!("{}/v1/modules/{sha}?language=rust", self.base))
            .header("content-type", "application/wasm")
            .body(wasm);
        let v = self.send(req).await?;
        Ok(json!({ "created": v["created"], "module": norm_module_rest(&v["module"]) }))
    }

    async fn upload_named(&mut self, sha: &str, wasm: Bytes, tenant: &str, name: &str) -> Out {
        let req = self
            .http
            .put(format!(
                "{}/v1/modules/{sha}?language=rust&tenant={tenant}&name={name}",
                self.base
            ))
            .header("content-type", "application/wasm")
            .body(wasm);
        let v = self.send(req).await?;
        Ok(json!({ "created": v["created"], "module": norm_module_rest(&v["module"]) }))
    }

    async fn get_module(&mut self, sha: &str) -> Out {
        let v = self
            .send(self.http.get(format!("{}/v1/modules/{sha}", self.base)))
            .await?;
        Ok(norm_module_rest(&v))
    }

    async fn list_workflows(&mut self) -> Out {
        let v = self
            .send(self.http.get(format!("{}/v1/workflows", self.base)))
            .await?;
        Ok(Value::Array(
            v["workflows"]
                .as_array()
                .expect("workflows")
                .iter()
                .map(norm_workflow_rest)
                .collect(),
        ))
    }

    async fn activate(&mut self, name: &str, version: u32, expected: Option<u64>) -> Out {
        let body = json!({ "version": version, "expected_version": expected });
        let v = self
            .send(
                self.http
                    .put(format!("{}/v1/workflows/{name}/active", self.base))
                    .json(&body),
            )
            .await?;
        Ok(norm_workflow_rest(&v))
    }

    async fn start(&mut self, s: Start) -> Out {
        let url = format!("{}/v1/workflows/{}/runs", self.base, s.workflow);
        let req = if s.raw_invalid_input {
            self.http
                .post(url)
                .header("content-type", "application/json")
                .body(r#"{"input": {nope}"#)
        } else {
            let body = json!({
                "input": s.input,
                "version": s.version,
                "queue": s.queue,
                "delay_ms": s.delay_ms,
                "tags": s.tags,
                "tenant": s.tenant,
                "idempotency_key": s.idempotency_key,
                "conflict_policy": if s.reject { "reject" } else { "return_existing" },
                "retention": s.retention_ms.map(friendly_ms),
                "schedule_to_start_timeout": s.schedule_to_start_ms.map(friendly_ms),
                "dry_run": s.dry_run,
            });
            self.http.post(url).json(&body)
        };
        let v = self.send(req).await?;
        Ok(json!({
            "run_id": v["run_id"],
            "created": v["created"],
            "dry_run": v.get("dry_run").cloned().unwrap_or(json!(false)),
        }))
    }

    async fn get_run(&mut self, run_id: &str) -> Out {
        let v = self
            .send(self.http.get(format!("{}/v1/runs/{run_id}", self.base)))
            .await?;
        Ok(norm_run_rest(&v))
    }

    async fn history(&mut self, run_id: &str, from_seq: u64, limit: u32) -> Out {
        let v = self
            .send(self.http.get(format!(
                "{}/v1/runs/{run_id}/history?from_seq={from_seq}&limit={limit}",
                self.base
            )))
            .await?;
        let events: Vec<Value> = v["events"]
            .as_array()
            .expect("events")
            .iter()
            .map(|e| {
                json!({
                    "seq": e["seq"], "kind": e["kind"], "ts_ms": e["ts_ms"],
                    "summary": e["summary"], "details": e["details"],
                })
            })
            .collect();
        Ok(json!({ "events": events, "next_from_seq": v["next_from_seq"] }))
    }

    async fn list_runs(&mut self, l: List) -> Out {
        let mut q: Vec<(&str, String)> = vec![("limit", l.limit.to_string())];
        if let Some(w) = l.workflow {
            q.push(("workflow", w));
        }
        if let Some(s) = l.status {
            q.push(("status", s.to_owned()));
        }
        if let Some(t) = l.tag {
            q.push(("tag", t));
        }
        if let Some(t) = l.page_token {
            q.push(("page_token", t));
        }
        // Test values are URL-safe; reqwest's `query` feature is not enabled.
        let qs: Vec<String> = q.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let v = self
            .send(
                self.http
                    .get(format!("{}/v1/runs?{}", self.base, qs.join("&"))),
            )
            .await?;
        let runs: Vec<Value> = v["runs"]
            .as_array()
            .expect("runs")
            .iter()
            .map(norm_summary_rest)
            .collect();
        Ok(json!({ "runs": runs, "next_page_token": v["next_page_token"] }))
    }

    async fn signal(
        &mut self,
        run_id: &str,
        name: &str,
        payload: Value,
        request_id: Option<&str>,
        dry_run: bool,
    ) -> Out {
        let body = json!({ "payload": payload, "request_id": request_id, "dry_run": dry_run });
        let v = self
            .send(
                self.http
                    .post(format!("{}/v1/runs/{run_id}/signals/{name}", self.base))
                    .json(&body),
            )
            .await?;
        Ok(
            json!({ "accepted": v["accepted"], "duplicate": v["duplicate"], "dry_run": v["dry_run"] }),
        )
    }

    async fn cancel(&mut self, run_id: &str, reason: Option<&str>, dry_run: bool) -> Out {
        let body = json!({ "reason": reason, "dry_run": dry_run });
        let v = self
            .send(
                self.http
                    .post(format!("{}/v1/runs/{run_id}/cancel", self.base))
                    .json(&body),
            )
            .await?;
        Ok(json!({ "already_requested": v["already_requested"], "dry_run": v["dry_run"] }))
    }

    async fn terminate(&mut self, run_id: &str, reason: Option<&str>, dry_run: bool) -> Out {
        let body = json!({ "reason": reason, "dry_run": dry_run });
        let v = self
            .send(
                self.http
                    .post(format!("{}/v1/runs/{run_id}/terminate", self.base))
                    .json(&body),
            )
            .await?;
        Ok(json!({ "status": v["status"], "dry_run": v["dry_run"] }))
    }

    async fn retry(&mut self, run_id: &str, dry_run: bool) -> Out {
        let v = self
            .send(
                self.http
                    .post(format!("{}/v1/runs/{run_id}/retry", self.base))
                    .json(&json!({ "dry_run": dry_run })),
            )
            .await?;
        Ok(json!({ "mode": v["mode"], "run_id": v["run_id"], "dry_run": v["dry_run"] }))
    }

    async fn result(&mut self, run_id: &str, wait_ms: u64, follow: bool) -> Out {
        // friendly_ms output ("1s 500ms") needs URL-encoding of the space.
        let wait = friendly_ms(wait_ms).replace(' ', "%20");
        let v = self
            .send(self.http.get(format!(
                "{}/v1/runs/{run_id}/result?wait={wait}&follow_continuations={follow}",
                self.base
            )))
            .await?;
        Ok(norm_result(
            v["run_id"].as_str().expect("run_id"),
            v["status"].as_str().expect("status").to_owned(),
            v["completed"].as_bool().expect("completed"),
            v.get("result").cloned(),
            v.get("failure").cloned(),
        ))
    }

    async fn complete_token(&mut self, token: &str, payload: Value, dry_run: bool) -> Out {
        let body = json!({ "token": token, "payload": payload, "dry_run": dry_run });
        let v = self
            .send(
                self.http
                    .post(format!("{}/v1/tokens/complete", self.base))
                    .json(&body),
            )
            .await?;
        Ok(json!({ "run_id": v["run_id"], "accepted": v["accepted"], "dry_run": v["dry_run"] }))
    }

    async fn set_workflow_policy(&mut self, name: &str, p: Policy) -> Out {
        let url = format!("{}/v1/workflows/{name}/policy", self.base);
        self.put_policy(url, p).await
    }

    async fn get_workflow_policy(&mut self, name: &str) -> Out {
        let url = format!("{}/v1/workflows/{name}/policy", self.base);
        self.get_policy(url).await
    }

    async fn set_queue_policy(&mut self, name: &str, p: Policy) -> Out {
        let url = format!("{}/v1/queues/{name}/policy", self.base);
        self.put_policy(url, p).await
    }

    async fn get_queue_policy(&mut self, name: &str) -> Out {
        let url = format!("{}/v1/queues/{name}/policy", self.base);
        self.get_policy(url).await
    }
}

impl RestClient {
    fn policy_out(v: &Value) -> Value {
        // The friendly rendering must agree with the ms fields.
        for (s, ms) in [
            ("retention", "retention_ms"),
            ("schedule_to_start_timeout", "schedule_to_start_ms"),
        ] {
            assert_eq!(
                v[s].as_str().map(str::to_owned),
                v[ms].as_u64().map(friendly_ms),
                "{v}"
            );
        }
        norm_policy(
            v["retention_ms"].as_u64(),
            v["schedule_to_start_ms"].as_u64(),
        )
    }

    async fn put_policy(&self, url: String, p: Policy) -> Out {
        let body = json!({
            "retention": p.retention_ms.map(friendly_ms),
            "schedule_to_start_timeout": p.schedule_to_start_ms.map(friendly_ms),
        });
        let v = self.send(self.http.put(url).json(&body)).await?;
        Ok(Self::policy_out(&v))
    }

    async fn get_policy(&self, url: String) -> Out {
        let v = self.send(self.http.get(url)).await?;
        Ok(Self::policy_out(&v))
    }
}

struct GrpcClient {
    modules: ModulesClient<Channel>,
    workflows: WorkflowsClient<Channel>,
    runs: RunsClient<Channel>,
    tokens: TokensClient<Channel>,
}

impl GrpcClient {
    async fn connect(addr: SocketAddr) -> Self {
        let channel = Channel::from_shared(format!("http://{addr}"))
            .expect("uri")
            .connect()
            .await
            .expect("h2c connect");
        GrpcClient {
            modules: ModulesClient::new(channel.clone()),
            workflows: WorkflowsClient::new(channel.clone()),
            runs: RunsClient::new(channel.clone()),
            tokens: TokensClient::new(channel),
        }
    }
}

fn grpc_err(s: tonic::Status) -> (ErrorCode, Option<Value>) {
    (ErrorCode::from_grpc_code(s.code()), status_details(&s))
}

impl Client for GrpcClient {
    async fn upload(&mut self, sha: &str, wasm: Bytes) -> Out {
        let r = self
            .modules
            .upload(pb::UploadModuleRequest {
                sha256: sha.to_owned(),
                wasm: wasm.to_vec(),
                language: Some("rust".to_owned()),
                tenant: None,
                name: None,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({
            "created": r.created,
            "module": norm_module(r.module.as_ref().expect("module")),
        }))
    }

    async fn upload_named(&mut self, sha: &str, wasm: Bytes, tenant: &str, name: &str) -> Out {
        let r = self
            .modules
            .upload(pb::UploadModuleRequest {
                sha256: sha.to_owned(),
                wasm: wasm.to_vec(),
                language: Some("rust".to_owned()),
                tenant: Some(tenant.to_owned()),
                name: Some(name.to_owned()),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({
            "created": r.created,
            "module": norm_module(r.module.as_ref().expect("module")),
        }))
    }

    async fn get_module(&mut self, sha: &str) -> Out {
        let m = self
            .modules
            .get(pb::GetModuleRequest {
                sha256: sha.to_owned(),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_module(&m))
    }

    async fn list_workflows(&mut self) -> Out {
        let r = self
            .workflows
            .list(pb::ListWorkflowsRequest {})
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(Value::Array(
            r.workflows.iter().map(norm_workflow).collect(),
        ))
    }

    async fn activate(&mut self, name: &str, version: u32, expected: Option<u64>) -> Out {
        let w = self
            .workflows
            .activate(pb::ActivateWorkflowRequest {
                name: name.to_owned(),
                version,
                expected_version: expected,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_workflow(&w))
    }

    async fn start(&mut self, s: Start) -> Out {
        let input_json = if s.raw_invalid_input {
            b"{nope".to_vec()
        } else {
            serde_json::to_vec(&s.input).expect("input")
        };
        let conflict_policy = if s.reject {
            pb::ConflictPolicy::Reject
        } else {
            pb::ConflictPolicy::ReturnExisting
        };
        let r = self
            .runs
            .start(pb::StartRunRequest {
                workflow: s.workflow,
                input_json,
                version: s.version,
                queue: s.queue,
                delay_ms: s.delay_ms,
                priority: None,
                tags: s.tags,
                traceparent: None,
                tenant: s.tenant,
                idempotency_key: s.idempotency_key,
                conflict_policy: conflict_policy as i32,
                concurrency_key: None,
                schedule_to_start_timeout_ms: s.schedule_to_start_ms,
                retention_ms: s.retention_ms,
                dry_run: s.dry_run,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({ "run_id": r.run_id, "created": r.created, "dry_run": r.dry_run }))
    }

    async fn get_run(&mut self, run_id: &str) -> Out {
        let r = self
            .runs
            .get(pb::GetRunRequest {
                run_id: run_id.to_owned(),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_run(&r))
    }

    async fn history(&mut self, run_id: &str, from_seq: u64, limit: u32) -> Out {
        let r = self
            .runs
            .history(pb::RunHistoryRequest {
                run_id: run_id.to_owned(),
                from_seq,
                limit,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        let events: Vec<Value> = r
            .events
            .iter()
            .map(|e| {
                json!({
                    "seq": e.seq, "kind": e.kind, "ts_ms": e.ts_ms,
                    "summary": e.summary, "details": embed(&e.details_json),
                })
            })
            .collect();
        Ok(json!({ "events": events, "next_from_seq": r.next_from_seq }))
    }

    async fn list_runs(&mut self, l: List) -> Out {
        let r = self
            .runs
            .list(pb::ListRunsRequest {
                workflow: l.workflow,
                status: l.status.map(status_proto),
                queue: None,
                tag: l.tag,
                created_after_ms: None,
                created_before_ms: None,
                limit: l.limit,
                page_token: l.page_token,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        let runs: Vec<Value> = r.runs.iter().map(norm_summary).collect();
        Ok(json!({ "runs": runs, "next_page_token": r.next_page_token }))
    }

    async fn signal(
        &mut self,
        run_id: &str,
        name: &str,
        payload: Value,
        request_id: Option<&str>,
        dry_run: bool,
    ) -> Out {
        let r = self
            .runs
            .signal(pb::SignalRunRequest {
                run_id: run_id.to_owned(),
                name: name.to_owned(),
                payload_json: serde_json::to_vec(&payload).expect("payload"),
                request_id: request_id.map(str::to_owned),
                dry_run,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({ "accepted": r.accepted, "duplicate": r.duplicate, "dry_run": r.dry_run }))
    }

    async fn cancel(&mut self, run_id: &str, reason: Option<&str>, dry_run: bool) -> Out {
        let r = self
            .runs
            .cancel(pb::CancelRunRequest {
                run_id: run_id.to_owned(),
                reason: reason.map(str::to_owned),
                dry_run,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({ "already_requested": r.already_requested, "dry_run": r.dry_run }))
    }

    async fn terminate(&mut self, run_id: &str, reason: Option<&str>, dry_run: bool) -> Out {
        let r = self
            .runs
            .terminate(pb::TerminateRunRequest {
                run_id: run_id.to_owned(),
                reason: reason.map(str::to_owned),
                dry_run,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({ "status": status_name(r.status), "dry_run": r.dry_run }))
    }

    async fn retry(&mut self, run_id: &str, dry_run: bool) -> Out {
        let r = self
            .runs
            .retry(pb::RetryRunRequest {
                run_id: run_id.to_owned(),
                dry_run,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        let mode = match pb::RetryMode::try_from(r.mode).expect("mode") {
            pb::RetryMode::Resumed => "resumed",
            pb::RetryMode::Restarted => "restarted",
            pb::RetryMode::Unspecified => panic!("unspecified retry mode on the wire"),
        };
        Ok(json!({ "mode": mode, "run_id": r.run_id, "dry_run": r.dry_run }))
    }

    async fn result(&mut self, run_id: &str, wait_ms: u64, follow: bool) -> Out {
        let r = self
            .runs
            .await_result(pb::AwaitResultRequest {
                run_id: run_id.to_owned(),
                wait_ms,
                follow_continuations: follow,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_result(
            &r.run_id,
            status_name(r.status),
            r.completed,
            r.result_json.as_deref().map(embed),
            r.failure_json.as_deref().map(embed),
        ))
    }

    async fn complete_token(&mut self, token: &str, payload: Value, dry_run: bool) -> Out {
        let r = self
            .tokens
            .complete(pb::CompleteTokenRequest {
                token: token.to_owned(),
                payload_json: serde_json::to_vec(&payload).expect("payload"),
                dry_run,
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({ "run_id": r.run_id, "accepted": r.accepted, "dry_run": r.dry_run }))
    }

    async fn set_workflow_policy(&mut self, name: &str, p: Policy) -> Out {
        let r = self
            .workflows
            .set_policy(pb::SetWorkflowPolicyRequest {
                name: name.to_owned(),
                policy: Some(pb::RunPolicy {
                    retention_ms: p.retention_ms,
                    schedule_to_start_ms: p.schedule_to_start_ms,
                }),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_policy(r.retention_ms, r.schedule_to_start_ms))
    }

    async fn get_workflow_policy(&mut self, name: &str) -> Out {
        let r = self
            .workflows
            .get_policy(pb::GetWorkflowPolicyRequest {
                name: name.to_owned(),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_policy(r.retention_ms, r.schedule_to_start_ms))
    }

    async fn set_queue_policy(&mut self, name: &str, p: Policy) -> Out {
        let r = self
            .workflows
            .set_queue_policy(pb::SetQueuePolicyRequest {
                name: name.to_owned(),
                policy: Some(pb::RunPolicy {
                    retention_ms: p.retention_ms,
                    schedule_to_start_ms: p.schedule_to_start_ms,
                }),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_policy(r.retention_ms, r.schedule_to_start_ms))
    }

    async fn get_queue_policy(&mut self, name: &str) -> Out {
        let r = self
            .workflows
            .get_queue_policy(pb::GetQueuePolicyRequest {
                name: name.to_owned(),
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(norm_policy(r.retention_ms, r.schedule_to_start_ms))
    }
}

// ---------------------------------------------------------------------------------
// Scenario
// ---------------------------------------------------------------------------------

fn ok_field(out: &Out, step: &str, path: &[&str]) -> Value {
    let mut v = out
        .as_ref()
        .unwrap_or_else(|e| panic!("{step}: {e:?}"))
        .clone();
    for p in path {
        v = v[*p].clone();
    }
    v
}

/// The scenario; returns the labelled transcript.
async fn scenario<C: Client>(c: &mut C, fake: &FakeControlPlane) -> Vec<(String, Out)> {
    let mut t: Vec<(String, Out)> = Vec::new();
    // Record a step whose outcome the scenario does not need.
    macro_rules! rec {
        ($label:expr, $e:expr) => {{
            let out: Out = $e;
            t.push(($label.to_owned(), out));
        }};
    }
    // Record a step and hand its outcome back.
    macro_rules! step {
        ($label:expr, $e:expr) => {{
            let out = $e;
            t.push(($label.to_owned(), out.clone()));
            out
        }};
    }

    let m1 = fake_module(&[("greet", 1)]);
    let m2 = fake_module(&[("greet", 2), ("other", 1)]);
    let sha1 = sha256_hex(&m1);
    let sha2 = sha256_hex(&m2);

    // Modules.
    rec!("upload m1", c.upload(&sha1, m1.clone()).await);
    rec!("re-upload m1", c.upload(&sha1, m1.clone()).await);
    rec!(
        "upload sha mismatch",
        c.upload(&sha256_hex(b"nope"), m1.clone()).await
    );
    rec!("upload m2", c.upload(&sha2, m2.clone()).await);
    let clash = fake_module(&[("greet", 1), ("x", 1)]);
    rec!(
        "upload conflicting version",
        c.upload(&sha256_hex(&clash), clash.clone()).await
    );
    rec!("get m1", c.get_module(&sha1).await);
    // D23 / Phase 14 F4: tenant and deployed module name reach the control plane
    // identically over both transports.
    rec!(
        "upload m1 as tenant acme, name mod-a",
        c.upload_named(&sha1, m1.clone(), "acme", "mod-a").await
    );
    rec!(
        "upload naming of m1",
        Ok(json!(
            fake.upload_naming(&sha1)
                .into_iter()
                .map(|(t, n)| json!([t, n]))
                .collect::<Vec<_>>()
        ))
    );
    rec!("get unknown module", c.get_module(&"0".repeat(64)).await);

    // Workflows.
    let wfs = step!("list workflows", c.list_workflows().await);
    let greet_rv = wfs
        .as_ref()
        .expect("list workflows")
        .as_array()
        .expect("array")
        .iter()
        .find(|w| w["name"] == "greet")
        .expect("greet")["record_version"]
        .as_u64()
        .expect("record_version");
    rec!(
        "activate stale expected",
        c.activate("greet", 2, Some(greet_rv + 7)).await
    );
    rec!(
        "activate greet@2",
        c.activate("greet", 2, Some(greet_rv)).await
    );
    rec!(
        "activate unknown workflow",
        c.activate("nope", 1, None).await
    );
    rec!(
        "activate unknown version",
        c.activate("greet", 9, None).await
    );

    // Runs.
    let s1 = Start {
        workflow: "greet".into(),
        input: json!({ "n": 1 }),
        idempotency_key: Some("k1".into()),
        tags: vec!["a".into()],
        ..Default::default()
    };
    let r1 = step!("start k1", c.start(s1.clone()).await);
    let run1 = ok_field(&r1, "start k1", &["run_id"]);
    let run1 = run1.as_str().expect("run id").to_owned();
    rec!("idempotent start k1", c.start(s1.clone()).await);
    rec!(
        "conflicting start k1 (reject)",
        c.start(Start {
            reject: true,
            ..s1.clone()
        })
        .await
    );
    rec!(
        "dry-run start k1 (existing)",
        c.start(Start {
            dry_run: true,
            ..s1.clone()
        })
        .await
    );
    rec!(
        "dry-run start k1 (reject)",
        c.start(Start {
            dry_run: true,
            reject: true,
            ..s1.clone()
        })
        .await
    );
    rec!(
        "dry-run start new key",
        c.start(Start {
            dry_run: true,
            idempotency_key: Some("k-dry".into()),
            ..s1.clone()
        })
        .await
    );
    rec!(
        "dry-run start unknown workflow",
        c.start(Start {
            workflow: "nope".into(),
            dry_run: true,
            ..Default::default()
        })
        .await
    );
    rec!(
        "start same key other tenant",
        c.start(Start {
            tenant: Some("t2".into()),
            ..s1.clone()
        })
        .await
    );
    rec!(
        "start delayed pinned v1",
        c.start(Start {
            workflow: "greet".into(),
            input: json!("hello"),
            version: Some(1),
            queue: Some("slow".into()),
            delay_ms: Some(5000),
            tags: vec!["a".into(), "b".into()],
            ..Default::default()
        })
        .await
    );
    rec!(
        "start other",
        c.start(Start {
            workflow: "other".into(),
            input: json!([1, 2, 3]),
            ..Default::default()
        })
        .await
    );
    rec!(
        "start unknown workflow",
        c.start(Start {
            workflow: "nope".into(),
            ..Default::default()
        })
        .await
    );
    rec!(
        "start unknown version",
        c.start(Start {
            workflow: "greet".into(),
            version: Some(9),
            ..Default::default()
        })
        .await
    );
    rec!(
        "start invalid input json",
        c.start(Start {
            workflow: "greet".into(),
            raw_invalid_input: true,
            ..Default::default()
        })
        .await
    );

    // Move run 1 forward through the fake (identical on both sides).
    fake.set_run_outcome(
        &run1,
        RunStatus::Running,
        None,
        None,
        Some(br#"{"p":1}"#.to_vec()),
    )
    .expect("running");
    fake.set_run_outcome(
        &run1,
        RunStatus::Completed,
        Some(br#"{"ok":true}"#.to_vec()),
        None,
        None,
    )
    .expect("completed");

    rec!("get run1", c.get_run(&run1).await);
    rec!("get unknown run", c.get_run("run-999999").await);
    let h1 = step!("history run1 page 1", c.history(&run1, 0, 2).await);
    let next = ok_field(&h1, "history page 1", &["next_from_seq"])
        .as_u64()
        .expect("next_from_seq");
    rec!("history run1 page 2", c.history(&run1, next, 2).await);
    rec!("history unknown run", c.history("run-999999", 0, 0).await);

    let p1 = step!(
        "list runs page 1",
        c.list_runs(List {
            limit: 2,
            ..Default::default()
        })
        .await
    );
    let token = ok_field(&p1, "list page 1", &["next_page_token"])
        .as_str()
        .expect("token")
        .to_owned();
    rec!(
        "list runs page 2",
        c.list_runs(List {
            limit: 2,
            page_token: Some(token),
            ..Default::default()
        })
        .await
    );
    rec!(
        "list completed",
        c.list_runs(List {
            status: Some("completed"),
            ..Default::default()
        })
        .await
    );
    rec!(
        "list greet tag b",
        c.list_runs(List {
            workflow: Some("greet".into()),
            tag: Some("b".into()),
            ..Default::default()
        })
        .await
    );
    rec!(
        "list bad page token",
        c.list_runs(List {
            page_token: Some("garbage".into()),
            ..Default::default()
        })
        .await
    );

    // Size limits: module bytes over max_module_bytes -> 413 / RESOURCE_EXHAUSTED,
    // both just over the limit and far over it (tonic's own decode limit).
    for (label, len) in [
        (
            "upload just over max_module_bytes",
            cfg().max_module_bytes + 1,
        ),
        (
            "upload far over max_module_bytes",
            cfg().max_module_bytes * 2,
        ),
    ] {
        let big = Bytes::from(vec![0u8; len]);
        let sha = sha256_hex(&big);
        rec!(label, c.upload(&sha, big).await);
    }
    // A start request over max_request_bytes.
    rec!(
        "start over max_request_bytes",
        c.start(Start {
            workflow: "greet".into(),
            input: Value::String("x".repeat(cfg().max_request_bytes + 1)),
            ..Default::default()
        })
        .await
    );
    // ---- Phase 5 lifecycle (API-05/06/07) and policies ----------------------------
    let lr = step!(
        "start lifecycle run",
        c.start(Start {
            workflow: "greet".into(),
            ..Default::default()
        })
        .await
    );
    let lrun = ok_field(&lr, "start lifecycle run", &["run_id"])
        .as_str()
        .expect("run id")
        .to_owned();
    rec!(
        "signal dry_run",
        c.signal(&lrun, "go", json!({"a": 1}), Some("r1"), true)
            .await
    );
    rec!(
        "signal r1",
        c.signal(&lrun, "go", json!({"a": 1}), Some("r1"), false)
            .await
    );
    rec!(
        "signal r1 duplicate",
        c.signal(&lrun, "go", json!(null), Some("r1"), false).await
    );
    rec!(
        "signal no request id",
        c.signal(&lrun, "other", json!([1]), None, false).await
    );
    rec!(
        "signal unknown run",
        c.signal("run-999999", "go", json!(null), None, false).await
    );
    rec!(
        "signal name too long",
        c.signal(&lrun, &"n".repeat(257), json!(null), None, false)
            .await
    );
    rec!(
        "signal request id too long",
        c.signal(&lrun, "go", json!(null), Some(&"x".repeat(65)), false)
            .await
    );
    rec!("cancel dry_run", c.cancel(&lrun, Some("why"), true).await);
    rec!("cancel", c.cancel(&lrun, Some("why"), false).await);
    rec!("cancel again", c.cancel(&lrun, None, false).await);
    rec!(
        "cancel unknown run",
        c.cancel("run-999999", None, false).await
    );
    rec!(
        "cancel reason too long",
        c.cancel(&lrun, Some(&"r".repeat(1025)), false).await
    );

    let token = fake.mint_token(&lrun, 5);
    rec!(
        "token dry_run",
        c.complete_token(&token, json!({"ok": 1}), true).await
    );
    rec!(
        "token complete",
        c.complete_token(&token, json!({"ok": 1}), false).await
    );
    rec!(
        "token complete again",
        c.complete_token(&token, json!(null), false).await
    );
    rec!(
        "token garbage",
        c.complete_token("ot1_nope", json!(null), false).await
    );
    rec!(
        "token no prefix",
        c.complete_token("hello", json!(null), false).await
    );

    rec!("result not terminal", c.result(&lrun, 0, false).await);
    rec!("result short wait", c.result(&lrun, 20, false).await);
    rec!("result unknown run", c.result("run-999999", 0, false).await);

    rec!("terminate dry_run", c.terminate(&lrun, None, true).await);
    rec!("terminate", c.terminate(&lrun, Some("ops"), false).await);
    rec!("terminate again", c.terminate(&lrun, None, false).await);
    rec!(
        "terminate unknown run",
        c.terminate("run-999999", None, false).await
    );
    rec!(
        "signal terminal run",
        c.signal(&lrun, "go", json!(null), None, false).await
    );
    rec!("cancel terminal run", c.cancel(&lrun, None, false).await);
    let token6 = fake.mint_token(&lrun, 6);
    rec!(
        "token on terminal run",
        c.complete_token(&token6, json!(null), false).await
    );
    rec!("result terminated", c.result(&lrun, 1500, false).await);

    rec!("retry dry_run (resume)", c.retry(&lrun, true).await);
    rec!("retry (resume)", c.retry(&lrun, false).await);
    rec!("retry live run", c.retry(&lrun, false).await);
    fake.set_run_outcome(&lrun, RunStatus::Failed, None, Some("boom".into()), None)
        .expect("failed");
    rec!("result failed", c.result(&lrun, 0, false).await);
    let rr = step!("retry (restart)", c.retry(&lrun, false).await);
    let restarted = ok_field(&rr, "retry (restart)", &["run_id"])
        .as_str()
        .expect("run id")
        .to_owned();
    rec!("get restarted run", c.get_run(&restarted).await);
    fake.set_run_outcome(
        &restarted,
        RunStatus::Completed,
        Some(b"null".to_vec()),
        None,
        None,
    )
    .expect("completed");
    rec!(
        "result completed with null",
        c.result(&restarted, 0, false).await
    );
    rec!("retry completed run", c.retry(&restarted, false).await);
    rec!("retry unknown run", c.retry("run-999999", false).await);

    let cr = step!(
        "start continuing run",
        c.start(Start {
            workflow: "greet".into(),
            ..Default::default()
        })
        .await
    );
    let crun = ok_field(&cr, "start continuing run", &["run_id"])
        .as_str()
        .expect("run id")
        .to_owned();
    let succ = fake.continue_as_new(&crun).expect("continue");
    fake.set_run_outcome(
        &succ,
        RunStatus::Completed,
        Some(br#"{"n":2}"#.to_vec()),
        None,
        None,
    )
    .expect("completed");
    rec!(
        "result continued (no follow)",
        c.result(&crun, 0, false).await
    );
    rec!("result continued (follow)", c.result(&crun, 0, true).await);
    rec!("retry continued run", c.retry(&crun, false).await);

    rec!(
        "get greet policy (unset)",
        c.get_workflow_policy("greet").await
    );
    rec!(
        "set greet policy",
        c.set_workflow_policy(
            "greet",
            Policy {
                retention_ms: Some(168 * 3_600_000),
                schedule_to_start_ms: Some(30_000),
            },
        )
        .await
    );
    rec!("get greet policy", c.get_workflow_policy("greet").await);
    rec!(
        "set unknown workflow policy",
        c.set_workflow_policy("nope", Policy::default()).await
    );
    rec!(
        "get unknown workflow policy",
        c.get_workflow_policy("nope").await
    );
    rec!(
        "set queue policy",
        c.set_queue_policy(
            "batch",
            Policy {
                retention_ms: None,
                schedule_to_start_ms: Some(90_500),
            },
        )
        .await
    );
    rec!("get queue policy", c.get_queue_policy("batch").await);
    rec!("get unset queue policy", c.get_queue_policy("other").await);
    rec!(
        "clear queue policy",
        c.set_queue_policy("batch", Policy::default()).await
    );

    // 05-13 start fields: request overrides and policy resolution, visible on GET.
    let pol = step!(
        "start with policy (greet policy applies)",
        c.start(Start {
            workflow: "greet".into(),
            ..Default::default()
        })
        .await
    );
    let pol = ok_field(&pol, "start with policy", &["run_id"]);
    let pol = pol.as_str().expect("run id").to_owned();
    rec!("get policy run", c.get_run(&pol).await);
    let ovr = step!(
        "start with overrides",
        c.start(Start {
            workflow: "greet".into(),
            retention_ms: Some(2 * 3_600_000),
            schedule_to_start_ms: Some(1_500),
            ..Default::default()
        })
        .await
    );
    let ovr = ok_field(&ovr, "start with overrides", &["run_id"]);
    let ovr = ovr.as_str().expect("run id").to_owned();
    rec!("get override run", c.get_run(&ovr).await);

    // Lifecycle size limits mirror max_request_bytes.
    let big = Value::String("x".repeat(cfg().max_request_bytes + 1));
    rec!(
        "signal over max_request_bytes",
        c.signal(&crun, "go", big.clone(), None, false).await
    );
    rec!(
        "token over max_request_bytes",
        c.complete_token(&token6, big, false).await
    );
    t
}

#[tokio::test]
async fn rest_and_grpc_transcripts_are_equal() {
    let rest_srv = serve(cfg()).await;
    let mut rest = RestClient::new(rest_srv.addr);
    let rest_t = scenario(&mut rest, &rest_srv.cp).await;

    let grpc_srv = serve(cfg()).await;
    let mut grpc = GrpcClient::connect(grpc_srv.addr).await;
    let grpc_t = scenario(&mut grpc, &grpc_srv.cp).await;

    assert_eq!(rest_t.len(), grpc_t.len());
    for ((label, r), (_, g)) in rest_t.iter().zip(&grpc_t) {
        assert_eq!(r, g, "step {label:?} differs\nREST: {r:#?}\ngRPC: {g:#?}");
    }

    // Spot-check the transcript actually exercised what it claims.
    let get = |label: &str| {
        rest_t
            .iter()
            .find(|(l, _)| l == label)
            .unwrap_or_else(|| panic!("no step {label}"))
            .1
            .clone()
    };
    assert_eq!(ok_field(&get("upload m1"), "", &["created"]), true);
    assert_eq!(ok_field(&get("re-upload m1"), "", &["created"]), false);
    assert_eq!(
        get("upload sha mismatch").unwrap_err().0,
        ErrorCode::InvalidArgument
    );
    assert_eq!(
        get("upload conflicting version").unwrap_err().0,
        ErrorCode::Conflict
    );
    assert_eq!(
        get("get unknown module").unwrap_err().0,
        ErrorCode::NotFound
    );
    assert_eq!(
        get("activate stale expected").unwrap_err().0,
        ErrorCode::PreconditionFailed
    );
    assert_eq!(ok_field(&get("start k1"), "", &["run_id"]), "run-000001");
    assert_eq!(
        get("idempotent start k1").unwrap(),
        json!({ "run_id": "run-000001", "created": false, "dry_run": false })
    );
    assert_eq!(
        get("dry-run start k1 (existing)").unwrap(),
        json!({ "run_id": "run-000001", "created": false, "dry_run": true })
    );
    assert_eq!(
        get("dry-run start k1 (reject)").unwrap_err().0,
        ErrorCode::Conflict
    );
    assert_eq!(
        get("dry-run start new key").unwrap(),
        json!({ "run_id": "", "created": true, "dry_run": true })
    );
    assert_eq!(
        get("dry-run start unknown workflow").unwrap_err().0,
        ErrorCode::NotFound
    );
    let pol = get("get policy run").unwrap();
    assert_eq!(pol["retention_ms"], json!(168 * 3_600_000u64));
    assert_eq!(pol["schedule_to_start_ms"], json!(30_000));
    assert_eq!(pol["attempt_failures"], json!(0));
    let ovr = get("get override run").unwrap();
    assert_eq!(ovr["retention_ms"], json!(2 * 3_600_000u64));
    assert_eq!(ovr["schedule_to_start_ms"], json!(1_500));
    assert_eq!(
        get("conflicting start k1 (reject)").unwrap_err(),
        (
            ErrorCode::Conflict,
            Some(json!({ "existing_run_id": "run-000001" }))
        )
    );
    assert_eq!(
        get("start invalid input json").unwrap_err().0,
        ErrorCode::InvalidArgument
    );
    assert_eq!(ok_field(&get("get run1"), "", &["status"]), "completed");
    assert_eq!(
        ok_field(&get("get run1"), "", &["result"]),
        json!({ "ok": true })
    );
    assert_eq!(
        ok_field(&get("history run1 page 1"), "", &["events"])
            .as_array()
            .map(Vec::len),
        Some(2)
    );
    assert!(ok_field(&get("list runs page 1"), "", &["next_page_token"]).is_string());
    for label in [
        "upload just over max_module_bytes",
        "upload far over max_module_bytes",
        "start over max_request_bytes",
        "signal over max_request_bytes",
        "token over max_request_bytes",
    ] {
        assert_eq!(
            get(label).unwrap_err().0,
            ErrorCode::PayloadTooLarge,
            "{label}"
        );
    }

    // Phase 5 lifecycle spot checks.
    let reason = |label: &str| {
        let (code, details) = get(label).unwrap_err();
        (
            code,
            details.and_then(|d| d["reason"].as_str().map(str::to_owned)),
        )
    };
    let conflict = |r: &str| (ErrorCode::Conflict, Some(r.to_owned()));
    assert_eq!(
        get("signal r1 duplicate").unwrap(),
        json!({ "accepted": true, "duplicate": true, "dry_run": false })
    );
    assert_eq!(
        get("signal unknown run").unwrap_err().0,
        ErrorCode::NotFound
    );
    assert_eq!(
        get("signal name too long").unwrap_err().0,
        ErrorCode::InvalidArgument
    );
    assert_eq!(
        ok_field(&get("cancel again"), "", &["already_requested"]),
        true
    );
    assert_eq!(
        reason("token complete again"),
        conflict("token_already_completed")
    );
    assert_eq!(
        reason("token garbage"),
        (ErrorCode::InvalidArgument, Some("token_invalid".to_owned()))
    );
    assert_eq!(
        ok_field(&get("result short wait"), "", &["completed"]),
        false
    );
    assert_eq!(ok_field(&get("terminate"), "", &["status"]), "terminated");
    for label in [
        "terminate again",
        "signal terminal run",
        "cancel terminal run",
        "token on terminal run",
    ] {
        assert_eq!(reason(label), conflict("run_terminal"), "{label}");
    }
    assert_eq!(ok_field(&get("retry (resume)"), "", &["mode"]), "resumed");
    assert_eq!(reason("retry live run"), conflict("run_not_terminal"));
    assert_eq!(
        ok_field(&get("retry (restart)"), "", &["mode"]),
        "restarted"
    );
    assert_eq!(reason("retry completed run"), conflict("run_completed"));
    assert_eq!(reason("retry continued run"), conflict("run_completed"));
    let null_result = get("result completed with null").unwrap();
    assert_eq!(
        (
            null_result["has_result"].clone(),
            null_result["result"].clone()
        ),
        (json!(true), Value::Null)
    );
    let follow = get("result continued (follow)").unwrap();
    assert_eq!(follow["result"], json!({ "n": 2 }));
    assert_ne!(
        follow["run_id"],
        get("result continued (no follow)").unwrap()["run_id"]
    );
    assert_eq!(
        get("get greet policy").unwrap(),
        json!({ "retention_ms": 604_800_000u64, "schedule_to_start_ms": 30_000 })
    );
    assert_eq!(
        get("get unknown workflow policy").unwrap_err().0,
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn http2_prior_knowledge_rest_on_the_same_port() {
    let srv = serve(cfg()).await;
    let h2 = reqwest::Client::builder()
        .http2_prior_knowledge()
        .build()
        .expect("client");
    let wasm = fake_module(&[("greet", 1)]);
    let resp = h2
        .put(format!(
            "http://{}/v1/modules/{}",
            srv.addr,
            sha256_hex(&wasm)
        ))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("h2 PUT");
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);
    let resp = h2
        .get(format!("http://{}/v1/workflows", srv.addr))
        .send()
        .await
        .expect("h2 GET");
    assert_eq!(resp.version(), reqwest::Version::HTTP_2);
    let v: Value = resp.json().await.expect("json");
    assert_eq!(v["workflows"][0]["name"], "greet");

    // gRPC on the very same port and connection type.
    let mut grpc = GrpcClient::connect(srv.addr).await;
    let r = grpc.list_workflows().await.expect("grpc list");
    assert_eq!(r[0]["name"], "greet");
}

#[tokio::test]
async fn grpc_upload_above_tonic_default_reaches_control_plane() {
    // Default config: 64 MiB modules. 6 MiB is above tonic's 4 MiB default decode limit.
    let srv = serve(ApiConfig::default()).await;
    let mut grpc = GrpcClient::connect(srv.addr).await;
    let big = Bytes::from(vec![7u8; 6 * MIB]);
    // Claim a different digest: the fake rejects with a sha mismatch, proving the
    // message was decoded and handed to the ControlPlane rather than size-rejected.
    let (code, details) = grpc
        .upload(&sha256_hex(b"other"), big.clone())
        .await
        .expect_err("sha mismatch");
    assert_eq!(code, ErrorCode::InvalidArgument);
    assert_eq!(details.expect("details")["actual_sha256"], sha256_hex(&big));
    // And with the right digest it is stored.
    let ok = grpc
        .upload(&sha256_hex(&big), big.clone())
        .await
        .expect("upload");
    assert_eq!(ok["module"]["size_bytes"], 6 * MIB);
}
