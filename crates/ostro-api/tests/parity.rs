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

/// REST-only tooling routes (D-P10-18, 10-05): they have no gRPC twin by design, so the
/// parity scenario never exercises them. Exactly these three (method, OpenAPI path)
/// pairs; [`rest_only_exclusions_are_exactly_the_phase10_tooling_routes`] pins the list.
const REST_ONLY: &[(&str, &str)] = &[
    // Dev clock (time skipping, D-P10-6): `ostro dev --ephemeral --time-skipping` only.
    ("get", "/v1/dev/time"),
    ("post", "/v1/dev/time/skip"),
    // Server-side replay (D-P10-7): `ostro replay`.
    ("post", "/v1/modules/{sha}/replay"),
];

fn cfg() -> ApiConfig {
    ApiConfig {
        max_module_bytes: 8 * MIB,
        max_request_bytes: MIB,
        ..Default::default()
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
        "pause": r.pause.as_ref().map(|p| json!({
            "reason": p.reason, "detail": p.detail, "at_ms": p.at_ms,
        })),
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
        "pause",
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
                ..Default::default()
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

// ---------------------------------------------------------------------------------
// Phase 8 (08-10): Schedules, Webhooks and RunLogs parity against `FakePhase8`.
// Hook ingress and the backup are REST-only (no gRPC counterpart).
// ---------------------------------------------------------------------------------

mod phase8 {
    use super::*;
    use ostro_api::fake::FakePhase8;
    use ostro_api::phase8::{LogLevelDto, RunLogLine, RunLogsResponse, ScheduleDto, WebhookDto};
    use ostro_proto::api::v1::{
        run_logs_client::RunLogsClient, schedules_client::SchedulesClient,
        webhooks_client::WebhooksClient,
    };

    struct P8Server {
        addr: SocketAddr,
        cp: Arc<FakeControlPlane>,
        p8: Arc<FakePhase8>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for P8Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn serve_p8() -> P8Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let cp = Arc::new(FakeControlPlane::new());
        let p8 = FakePhase8::new(cp.clone());
        let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
        let app = ostro_api::router_with(dyn_cp, cfg(), p8.extensions());
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        P8Server { addr, cp, p8, task }
    }

    /// Canonical JSON of a schedule / webhook / log page (both transports go through
    /// the DTO).
    fn canon_schedule(v: ScheduleDto) -> Value {
        serde_json::to_value(v).expect("json")
    }

    fn canon_webhook(v: WebhookDto) -> Value {
        serde_json::to_value(v).expect("json")
    }

    trait P8Client {
        async fn create_schedule(&mut self, s: Value) -> Out;
        async fn get_schedule(&mut self, name: &str) -> Out;
        async fn list_schedules(&mut self) -> Out;
        async fn update_schedule(&mut self, s: Value) -> Out;
        async fn delete_schedule(&mut self, name: &str, tag: Option<u64>) -> Out;
        async fn pause(&mut self, name: &str) -> Out;
        async fn resume(&mut self, name: &str) -> Out;
        async fn trigger(&mut self, name: &str, request_id: Option<&str>) -> Out;
        async fn create_webhook(&mut self, w: Value) -> Out;
        async fn get_webhook(&mut self, name: &str) -> Out;
        async fn list_webhooks(&mut self) -> Out;
        async fn update_webhook(&mut self, w: Value) -> Out;
        async fn delete_webhook(&mut self, name: &str, tag: Option<u64>) -> Out;
        async fn run_logs(&mut self, run_id: &str, limit: u32, level: Option<&str>) -> Out;
    }

    impl P8Client for RestClient {
        async fn create_schedule(&mut self, s: Value) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/schedules", self.base))
                        .json(&s),
                )
                .await?;
            Ok(canon_schedule(serde_json::from_value(v).expect("dto")))
        }

        async fn get_schedule(&mut self, name: &str) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/schedules/{name}", self.base)))
                .await?;
            Ok(canon_schedule(serde_json::from_value(v).expect("dto")))
        }

        async fn list_schedules(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/schedules", self.base)))
                .await?;
            Ok(Value::Array(
                v["schedules"]
                    .as_array()
                    .expect("schedules")
                    .iter()
                    .map(|s| canon_schedule(serde_json::from_value(s.clone()).expect("dto")))
                    .collect(),
            ))
        }

        async fn update_schedule(&mut self, s: Value) -> Out {
            let name = s["name"].as_str().expect("name").to_owned();
            let v = self
                .send(
                    self.http
                        .put(format!("{}/v1/schedules/{name}", self.base))
                        .json(&s),
                )
                .await?;
            Ok(canon_schedule(serde_json::from_value(v).expect("dto")))
        }

        async fn delete_schedule(&mut self, name: &str, tag: Option<u64>) -> Out {
            let q = tag.map(|t| format!("?version_tag={t}")).unwrap_or_default();
            self.send(
                self.http
                    .delete(format!("{}/v1/schedules/{name}{q}", self.base)),
            )
            .await
        }

        async fn pause(&mut self, name: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/schedules/{name}/pause", self.base)),
                )
                .await?;
            Ok(canon_schedule(serde_json::from_value(v).expect("dto")))
        }

        async fn resume(&mut self, name: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/schedules/{name}/resume", self.base)),
                )
                .await?;
            Ok(canon_schedule(serde_json::from_value(v).expect("dto")))
        }

        async fn trigger(&mut self, name: &str, request_id: Option<&str>) -> Out {
            self.send(
                self.http
                    .post(format!("{}/v1/schedules/{name}/trigger", self.base))
                    .json(&json!({ "request_id": request_id })),
            )
            .await
        }

        async fn create_webhook(&mut self, w: Value) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/webhooks", self.base))
                        .json(&w),
                )
                .await?;
            Ok(canon_webhook(serde_json::from_value(v).expect("dto")))
        }

        async fn get_webhook(&mut self, name: &str) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/webhooks/{name}", self.base)))
                .await?;
            Ok(canon_webhook(serde_json::from_value(v).expect("dto")))
        }

        async fn list_webhooks(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/webhooks", self.base)))
                .await?;
            Ok(Value::Array(
                v["webhooks"]
                    .as_array()
                    .expect("webhooks")
                    .iter()
                    .map(|w| canon_webhook(serde_json::from_value(w.clone()).expect("dto")))
                    .collect(),
            ))
        }

        async fn update_webhook(&mut self, w: Value) -> Out {
            let name = w["name"].as_str().expect("name").to_owned();
            let v = self
                .send(
                    self.http
                        .put(format!("{}/v1/webhooks/{name}", self.base))
                        .json(&w),
                )
                .await?;
            Ok(canon_webhook(serde_json::from_value(v).expect("dto")))
        }

        async fn delete_webhook(&mut self, name: &str, tag: Option<u64>) -> Out {
            let q = tag.map(|t| format!("?version_tag={t}")).unwrap_or_default();
            self.send(
                self.http
                    .delete(format!("{}/v1/webhooks/{name}{q}", self.base)),
            )
            .await
        }

        async fn run_logs(&mut self, run_id: &str, limit: u32, level: Option<&str>) -> Out {
            let mut url = format!("{}/v1/runs/{run_id}/logs?limit={limit}", self.base);
            if let Some(l) = level {
                url.push_str(&format!("&level={l}"));
            }
            let v = self.send(self.http.get(url)).await?;
            let page: RunLogsResponse = serde_json::from_value(v).expect("dto");
            Ok(serde_json::to_value(page).expect("json"))
        }
    }

    struct GrpcP8 {
        schedules: SchedulesClient<Channel>,
        webhooks: WebhooksClient<Channel>,
        logs: RunLogsClient<Channel>,
    }

    impl GrpcP8 {
        async fn connect(addr: SocketAddr) -> Self {
            let ch = Channel::from_shared(format!("http://{addr}"))
                .expect("uri")
                .connect()
                .await
                .expect("h2c connect");
            GrpcP8 {
                schedules: SchedulesClient::new(ch.clone()),
                webhooks: WebhooksClient::new(ch.clone()),
                logs: RunLogsClient::new(ch),
            }
        }
    }

    fn pb_schedule(v: Value) -> pb::Schedule {
        let dto: ScheduleDto = serde_json::from_value(v).expect("dto");
        dto.into()
    }

    fn pb_webhook(v: Value) -> pb::Webhook {
        let dto: WebhookDto = serde_json::from_value(v).expect("dto");
        dto.into()
    }

    fn from_pb_schedule(s: pb::Schedule) -> Value {
        canon_schedule(ScheduleDto::try_from(s).expect("valid schedule"))
    }

    fn from_pb_webhook(w: pb::Webhook) -> Value {
        canon_webhook(WebhookDto::try_from(w).expect("valid webhook"))
    }

    fn level_proto(l: &str) -> i32 {
        match l {
            "trace" => pb::LogLevel::Trace as i32,
            "debug" => pb::LogLevel::Debug as i32,
            "info" => pb::LogLevel::Info as i32,
            "warn" => pb::LogLevel::Warn as i32,
            "error" => pb::LogLevel::Error as i32,
            other => panic!("level {other}"),
        }
    }

    fn level_name(v: i32) -> &'static str {
        match pb::LogLevel::try_from(v).expect("level") {
            pb::LogLevel::Trace => "trace",
            pb::LogLevel::Debug => "debug",
            pb::LogLevel::Info => "info",
            pb::LogLevel::Warn => "warn",
            pb::LogLevel::Error => "error",
            pb::LogLevel::Unspecified => panic!("unspecified level on the wire"),
        }
    }

    impl P8Client for GrpcP8 {
        async fn create_schedule(&mut self, s: Value) -> Out {
            let r = self
                .schedules
                .create(pb::CreateScheduleRequest {
                    schedule: Some(pb_schedule(s)),
                })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_schedule(r.into_inner()))
        }

        async fn get_schedule(&mut self, name: &str) -> Out {
            let r = self
                .schedules
                .get(pb::GetScheduleRequest { name: name.into() })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_schedule(r.into_inner()))
        }

        async fn list_schedules(&mut self) -> Out {
            let r = self
                .schedules
                .list(pb::ListSchedulesRequest {})
                .await
                .map_err(grpc_err)?;
            Ok(Value::Array(
                r.into_inner()
                    .schedules
                    .into_iter()
                    .map(from_pb_schedule)
                    .collect(),
            ))
        }

        async fn update_schedule(&mut self, s: Value) -> Out {
            let r = self
                .schedules
                .update(pb::UpdateScheduleRequest {
                    schedule: Some(pb_schedule(s)),
                })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_schedule(r.into_inner()))
        }

        async fn delete_schedule(&mut self, name: &str, tag: Option<u64>) -> Out {
            let r = self
                .schedules
                .delete(pb::DeleteScheduleRequest {
                    name: name.into(),
                    version_tag: tag,
                })
                .await
                .map_err(grpc_err)?
                .into_inner();
            Ok(json!({ "name": r.name, "deleted": r.deleted }))
        }

        async fn pause(&mut self, name: &str) -> Out {
            let r = self
                .schedules
                .pause(pb::PauseScheduleRequest { name: name.into() })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_schedule(r.into_inner()))
        }

        async fn resume(&mut self, name: &str) -> Out {
            let r = self
                .schedules
                .resume(pb::ResumeScheduleRequest { name: name.into() })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_schedule(r.into_inner()))
        }

        async fn trigger(&mut self, name: &str, request_id: Option<&str>) -> Out {
            let r = self
                .schedules
                .trigger(pb::TriggerScheduleRequest {
                    name: name.into(),
                    request_id: request_id.map(str::to_owned),
                })
                .await
                .map_err(grpc_err)?;
            Ok(json!({ "run_id": r.into_inner().run_id }))
        }

        async fn create_webhook(&mut self, w: Value) -> Out {
            let r = self
                .webhooks
                .create(pb::CreateWebhookRequest {
                    webhook: Some(pb_webhook(w)),
                })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_webhook(r.into_inner()))
        }

        async fn get_webhook(&mut self, name: &str) -> Out {
            let r = self
                .webhooks
                .get(pb::GetWebhookRequest { name: name.into() })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_webhook(r.into_inner()))
        }

        async fn list_webhooks(&mut self) -> Out {
            let r = self
                .webhooks
                .list(pb::ListWebhooksRequest {})
                .await
                .map_err(grpc_err)?;
            Ok(Value::Array(
                r.into_inner()
                    .webhooks
                    .into_iter()
                    .map(from_pb_webhook)
                    .collect(),
            ))
        }

        async fn update_webhook(&mut self, w: Value) -> Out {
            let r = self
                .webhooks
                .update(pb::UpdateWebhookRequest {
                    webhook: Some(pb_webhook(w)),
                })
                .await
                .map_err(grpc_err)?;
            Ok(from_pb_webhook(r.into_inner()))
        }

        async fn delete_webhook(&mut self, name: &str, tag: Option<u64>) -> Out {
            let r = self
                .webhooks
                .delete(pb::DeleteWebhookRequest {
                    name: name.into(),
                    version_tag: tag,
                })
                .await
                .map_err(grpc_err)?
                .into_inner();
            Ok(json!({ "name": r.name, "deleted": r.deleted }))
        }

        async fn run_logs(&mut self, run_id: &str, limit: u32, level: Option<&str>) -> Out {
            let r = self
                .logs
                .get_run_logs(pb::GetRunLogsRequest {
                    run_id: run_id.into(),
                    limit,
                    level: level.map(level_proto),
                })
                .await
                .map_err(grpc_err)?
                .into_inner();
            Ok(json!({
                "run_id": r.run_id,
                "lines": r.lines.iter().map(|l| json!({
                    "ts_ms": l.ts_ms,
                    "level": level_name(l.level),
                    "message": l.message,
                    "fields": embed(&l.fields_json),
                    "attempt": l.attempt,
                })).collect::<Vec<_>>(),
                "dropped": r.dropped,
            }))
        }
    }

    async fn scenario<C: P8Client>(c: &mut C, srv: &P8Server) -> Vec<(String, Out)> {
        let mut t: Vec<(String, Out)> = Vec::new();
        macro_rules! step {
            ($label:expr, $e:expr) => {{
                let out = $e;
                t.push(($label.to_owned(), out));
            }};
        }
        let wasm = fake_module(&[("greet", 1)]);
        srv.cp
            .upload_module(ostro_api::domain::UploadModule {
                signatures: Vec::new(),
                sha256: sha256_hex(&wasm),
                wasm,
                language: None,
                tenant: None,
                name: None,
            })
            .await
            .expect("upload");

        // Schedules.
        let sched = json!({
            "name": "nightly", "workflow": "greet", "cron": "0 0 * * *",
            "timezone": "Europe/Lisbon", "input": {"n": 1}, "overlap": "buffer_one",
            "catchup": "all", "catchup_window_ms": 3_600_000u64,
            "start_at_ms": 1_000u64, "end_at_ms": 2_000_000_000_000u64,
        });
        step!("create schedule", c.create_schedule(sched.clone()).await);
        step!(
            "create duplicate schedule",
            c.create_schedule(sched.clone()).await
        );
        step!(
            "create bad name",
            c.create_schedule(json!({"name": "a b", "workflow": "greet", "cron": "x"}))
                .await
        );
        step!(
            "create no cron",
            c.create_schedule(json!({"name": "x", "workflow": "greet"}))
                .await
        );
        step!(
            "create no workflow",
            c.create_schedule(json!({"name": "x", "cron": "* * * * *"}))
                .await
        );
        step!(
            "create second",
            c.create_schedule(
                json!({"name": "hourly", "workflow": "greet", "cron": "0 * * * *", "paused": true})
            )
            .await
        );
        step!("get schedule", c.get_schedule("nightly").await);
        step!("get unknown schedule", c.get_schedule("nope").await);
        step!("get bad schedule name", c.get_schedule("a b").await);
        step!("list schedules", c.list_schedules().await);
        let mut upd = sched.clone();
        upd["overlap"] = json!("terminate_other");
        upd["version_tag"] = json!(1);
        step!("update schedule", c.update_schedule(upd.clone()).await);
        step!("update stale tag", c.update_schedule(upd.clone()).await);
        let mut missing = upd.clone();
        missing["name"] = json!("ghost");
        missing["version_tag"] = Value::Null;
        step!("update unknown", c.update_schedule(missing).await);
        step!("pause", c.pause("nightly").await);
        step!("pause again", c.pause("nightly").await);
        step!("resume", c.resume("nightly").await);
        step!("pause unknown", c.pause("nope").await);
        step!("trigger", c.trigger("nightly", Some("r1")).await);
        step!(
            "trigger same request",
            c.trigger("nightly", Some("r1")).await
        );
        step!("trigger no request id", c.trigger("nightly", None).await);
        step!(
            "trigger long request id",
            c.trigger("nightly", Some(&"x".repeat(65))).await
        );
        step!("trigger unknown", c.trigger("nope", None).await);
        step!("delete stale", c.delete_schedule("hourly", Some(9)).await);
        step!("delete", c.delete_schedule("hourly", Some(1)).await);
        step!("delete again", c.delete_schedule("hourly", None).await);
        step!("list after delete", c.list_schedules().await);

        // Webhooks.
        let gh = json!({
            "name": "gh",
            "action": {"kind": "signal", "workflow": "greet", "name": "push",
                       "run_by": {"idempotency_pointer": "/order/id"}},
            "verifier": {"kind": "github", "secret_ref": "gh-secret"},
            "idempotency": "header:X-GitHub-Delivery",
        });
        step!("create webhook", c.create_webhook(gh.clone()).await);
        step!(
            "create duplicate webhook",
            c.create_webhook(gh.clone()).await
        );
        step!(
            "create hmac webhook",
            c.create_webhook(json!({
                "name": "hmac",
                "action": {"kind": "start", "workflow": "greet", "version": 1, "input": "/data"},
                "verifier": {"kind": "hmac", "secret_ref": "s", "tolerance_s": 60, "header": "X-Sig",
                             "prefix": "sha256=", "algorithm": "sha256", "encoding": "hex",
                             "timestamp_header": "X-Ts", "template": "{ts}.{body}"},
            }))
            .await
        );
        step!(
            "create token webhook",
            c.create_webhook(json!({
                "name": "tok",
                "action": {"kind": "complete_token", "token_pointer": "/token"},
            }))
            .await
        );
        step!(
            "create webhook without secret",
            c.create_webhook(json!({
                "name": "bad", "action": {"kind": "complete_token", "token_pointer": "/t"},
                "verifier": {"kind": "slack"},
            }))
            .await
        );
        step!(
            "create webhook bad idempotency",
            c.create_webhook(json!({
                "name": "bad", "action": {"kind": "complete_token", "token_pointer": "/t"},
                "idempotency": "cookie:x",
            }))
            .await
        );
        step!("get webhook", c.get_webhook("gh").await);
        step!("get unknown webhook", c.get_webhook("nope").await);
        step!("list webhooks", c.list_webhooks().await);
        let mut gh2 = gh.clone();
        gh2["idempotency"] = json!("none");
        gh2["version_tag"] = json!(5);
        step!("update webhook stale", c.update_webhook(gh2.clone()).await);
        gh2["version_tag"] = json!(1);
        step!("update webhook", c.update_webhook(gh2).await);
        step!(
            "delete webhook stale",
            c.delete_webhook("tok", Some(3)).await
        );
        step!("delete webhook", c.delete_webhook("tok", None).await);
        step!(
            "delete unknown webhook",
            c.delete_webhook("tok", None).await
        );

        // Run logs.
        let run_id = srv
            .cp
            .start_run(ostro_api::domain::StartRunRequest {
                workflow: "greet".into(),
                input_json: b"null".to_vec(),
                ..Default::default()
            })
            .await
            .expect("start")
            .run_id;
        for (i, level) in [
            LogLevelDto::Trace,
            LogLevelDto::Info,
            LogLevelDto::Warn,
            LogLevelDto::Error,
        ]
        .into_iter()
        .enumerate()
        {
            srv.p8.push_log(
                &run_id,
                RunLogLine {
                    ts_ms: 1_000 + i as u64,
                    level,
                    message: format!("line {i}"),
                    fields: json!({"i": i}),
                    attempt: 1,
                },
            );
        }
        step!("logs all", c.run_logs(&run_id, 0, None).await);
        step!("logs limit 2", c.run_logs(&run_id, 2, None).await);
        step!("logs warn+", c.run_logs(&run_id, 0, Some("warn")).await);
        step!("logs unknown run", c.run_logs("run-999999", 0, None).await);
        t
    }

    #[tokio::test]
    async fn phase8_rest_and_grpc_transcripts_are_equal() {
        let rest_srv = serve_p8().await;
        let mut rest = RestClient::new(rest_srv.addr);
        let rest_t = scenario(&mut rest, &rest_srv).await;

        let grpc_srv = serve_p8().await;
        let mut grpc = GrpcP8::connect(grpc_srv.addr).await;
        let grpc_t = scenario(&mut grpc, &grpc_srv).await;

        assert_eq!(rest_t.len(), grpc_t.len());
        for ((label, r), (_, g)) in rest_t.iter().zip(&grpc_t) {
            assert_eq!(r, g, "step {label:?} differs\nREST: {r:#?}\ngRPC: {g:#?}");
        }

        let get = |label: &str| {
            rest_t
                .iter()
                .find(|(l, _)| l == label)
                .unwrap_or_else(|| panic!("no step {label}"))
                .1
                .clone()
        };
        let err = |label: &str| get(label).unwrap_err();
        let created = get("create schedule").unwrap();
        assert_eq!(created["version_tag"], 1);
        assert_eq!(created["timezone"], "Europe/Lisbon");
        assert_eq!(created["overlap"], "buffer_one");
        assert_eq!(created["catchup"], "all");
        assert_eq!(created["input"], json!({"n": 1}));
        assert_eq!(err("create duplicate schedule").0, ErrorCode::Conflict);
        for l in [
            "create bad name",
            "create no cron",
            "create no workflow",
            "get bad schedule name",
            "trigger long request id",
        ] {
            assert_eq!(err(l).0, ErrorCode::InvalidArgument, "{l}");
        }
        for l in [
            "get unknown schedule",
            "update unknown",
            "pause unknown",
            "trigger unknown",
            "delete again",
            "get unknown webhook",
            "delete unknown webhook",
            "logs unknown run",
        ] {
            assert_eq!(err(l).0, ErrorCode::NotFound, "{l}");
        }
        for l in [
            "update stale tag",
            "delete stale",
            "update webhook stale",
            "delete webhook stale",
        ] {
            let (code, details) = err(l);
            assert_eq!(code, ErrorCode::Conflict, "{l}");
            assert_eq!(
                details.expect("details")["reason"],
                "version_mismatch",
                "{l}"
            );
        }
        assert_eq!(get("update schedule").unwrap()["version_tag"], 2);
        assert_eq!(get("pause").unwrap()["paused"], true);
        assert_eq!(
            get("pause again").unwrap()["version_tag"],
            3,
            "idempotent pause"
        );
        assert_eq!(
            get("trigger").unwrap()["run_id"],
            get("trigger same request").unwrap()["run_id"]
        );
        assert_eq!(
            get("list after delete").unwrap().as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            err("create webhook without secret").0,
            ErrorCode::InvalidArgument
        );
        assert_eq!(
            err("create webhook bad idempotency").0,
            ErrorCode::InvalidArgument
        );
        assert_eq!(get("update webhook").unwrap()["idempotency"], "none");
        assert_eq!(
            get("logs all").unwrap()["lines"].as_array().map(Vec::len),
            Some(4)
        );
        assert_eq!(
            get("logs limit 2").unwrap()["lines"][0]["message"],
            "line 2"
        );
        assert_eq!(
            get("logs warn+").unwrap()["lines"].as_array().map(Vec::len),
            Some(2)
        );
    }
}

// ---------------------------------------------------------------------------------
// Phase 6 (06-08): Secrets and Connectors parity (secrets, grants, connections, egress).
// The presign route is REST-only (no gRPC counterpart).
// ---------------------------------------------------------------------------------

mod bridge {
    use super::*;
    use ostro_proto::api::v1::{
        connectors_client::ConnectorsClient, secrets_client::SecretsClient,
    };

    fn secret_out(name: &Value, key_id: &Value, tenant: &Value) -> Value {
        json!({ "name": name, "key_id": key_id, "tenant": tenant })
    }

    fn grant_out(
        id: &Value,
        module: &Value,
        secret: &Value,
        origins: &Value,
        body: &Value,
    ) -> Value {
        json!({ "id": id, "module": module, "secret": secret, "origins": origins, "body": body })
    }

    fn pb_secret(s: &pb::SecretInfo) -> Value {
        secret_out(&json!(s.name), &json!(s.key_id), &json!(s.tenant))
    }

    fn pb_grant(g: &pb::GrantInfo) -> Value {
        grant_out(
            &json!(g.id),
            &json!(g.module),
            &json!(g.secret),
            &json!(g.origins),
            &json!(g.body),
        )
    }

    fn pb_connection(c: &pb::ConnectionInfo) -> Value {
        json!({ "name": c.name, "kind": c.kind, "config": embed(&c.config_json) })
    }

    fn pb_egress(e: &pb::EgressInfo) -> Value {
        json!({ "module": e.module, "allow": e.allow })
    }

    fn rest_secret(v: &Value) -> Value {
        secret_out(&v["name"], &v["key_id"], &v["tenant"])
    }

    fn rest_grant(v: &Value) -> Value {
        grant_out(
            &v["id"],
            &v["module"],
            &v["secret"],
            &v["origins"],
            &v["body"],
        )
    }

    fn rest_connection(v: &Value) -> Value {
        json!({ "name": v["name"], "kind": v["kind"], "config": v["config"] })
    }

    fn rest_egress(v: &Value) -> Value {
        json!({ "module": v["module"], "allow": v["allow"] })
    }

    fn rest_list(v: &Value, key: &str, f: fn(&Value) -> Value) -> Value {
        Value::Array(v[key].as_array().expect(key).iter().map(f).collect())
    }

    /// Percent-encode a path segment (names may contain `/` and `:`).
    fn enc(s: &str) -> String {
        s.bytes()
            .map(|b| match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (b as char).to_string()
                }
                _ => format!("%{b:02X}"),
            })
            .collect()
    }

    trait BridgeClient {
        async fn put_secret(&mut self, name: &str, value: &[u8], tenant: Option<&str>) -> Out;
        async fn list_secrets(&mut self, tenant: Option<&str>) -> Out;
        async fn delete_secret(&mut self, name: &str, tenant: Option<&str>) -> Out;
        async fn put_grant(&mut self, g: Value) -> Out;
        async fn list_grants(&mut self, module: Option<&str>) -> Out;
        async fn delete_grant(&mut self, id: &str) -> Out;
        async fn put_connection(&mut self, name: &str, kind: &str, config: Value) -> Out;
        async fn get_connection(&mut self, name: &str) -> Out;
        async fn list_connections(&mut self) -> Out;
        async fn delete_connection(&mut self, name: &str) -> Out;
        async fn put_egress(&mut self, module: &str, allow: &[&str]) -> Out;
        async fn get_egress(&mut self, module: &str) -> Out;
        async fn list_egress(&mut self) -> Out;
        async fn delete_egress(&mut self, module: &str) -> Out;
    }

    fn tenant_q(tenant: Option<&str>) -> String {
        tenant
            .map(|t| format!("?tenant={}", enc(t)))
            .unwrap_or_default()
    }

    impl BridgeClient for RestClient {
        async fn put_secret(&mut self, name: &str, value: &[u8], tenant: Option<&str>) -> Out {
            // UTF-8 values travel as `value` (the scenario uses only text).
            let text = std::str::from_utf8(value).expect("utf8 scenario value");
            let url = format!("{}/v1/secrets/{}{}", self.base, enc(name), tenant_q(tenant));
            let v = self
                .send(self.http.put(url).json(&json!({ "value": text })))
                .await?;
            Ok(rest_secret(&v))
        }

        async fn list_secrets(&mut self, tenant: Option<&str>) -> Out {
            let url = format!("{}/v1/secrets{}", self.base, tenant_q(tenant));
            let v = self.send(self.http.get(url)).await?;
            Ok(rest_list(&v, "secrets", rest_secret))
        }

        async fn delete_secret(&mut self, name: &str, tenant: Option<&str>) -> Out {
            let url = format!("{}/v1/secrets/{}{}", self.base, enc(name), tenant_q(tenant));
            self.send(self.http.delete(url)).await?;
            Ok(json!({}))
        }

        async fn put_grant(&mut self, g: Value) -> Out {
            let v = self
                .send(self.http.put(format!("{}/v1/grants", self.base)).json(&g))
                .await?;
            Ok(rest_grant(&v))
        }

        async fn list_grants(&mut self, module: Option<&str>) -> Out {
            let q = module
                .map(|m| format!("?module={}", enc(m)))
                .unwrap_or_default();
            let v = self
                .send(self.http.get(format!("{}/v1/grants{q}", self.base)))
                .await?;
            Ok(rest_list(&v, "grants", rest_grant))
        }

        async fn delete_grant(&mut self, id: &str) -> Out {
            self.send(
                self.http
                    .delete(format!("{}/v1/grants/{}", self.base, enc(id))),
            )
            .await?;
            Ok(json!({}))
        }

        async fn put_connection(&mut self, name: &str, kind: &str, config: Value) -> Out {
            let v = self
                .send(
                    self.http
                        .put(format!("{}/v1/connections/{}", self.base, enc(name)))
                        .json(&json!({ "kind": kind, "config": config })),
                )
                .await?;
            Ok(rest_connection(&v))
        }

        async fn get_connection(&mut self, name: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .get(format!("{}/v1/connections/{}", self.base, enc(name))),
                )
                .await?;
            Ok(rest_connection(&v))
        }

        async fn list_connections(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/connections", self.base)))
                .await?;
            Ok(rest_list(&v, "connections", rest_connection))
        }

        async fn delete_connection(&mut self, name: &str) -> Out {
            self.send(
                self.http
                    .delete(format!("{}/v1/connections/{}", self.base, enc(name))),
            )
            .await?;
            Ok(json!({}))
        }

        async fn put_egress(&mut self, module: &str, allow: &[&str]) -> Out {
            let v = self
                .send(
                    self.http
                        .put(format!("{}/v1/egress/{}", self.base, enc(module)))
                        .json(&json!({ "allow": allow })),
                )
                .await?;
            Ok(rest_egress(&v))
        }

        async fn get_egress(&mut self, module: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .get(format!("{}/v1/egress/{}", self.base, enc(module))),
                )
                .await?;
            Ok(rest_egress(&v))
        }

        async fn list_egress(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/egress", self.base)))
                .await?;
            Ok(rest_list(&v, "policies", rest_egress))
        }

        async fn delete_egress(&mut self, module: &str) -> Out {
            self.send(
                self.http
                    .delete(format!("{}/v1/egress/{}", self.base, enc(module))),
            )
            .await?;
            Ok(json!({}))
        }
    }

    struct BridgeGrpc {
        secrets: SecretsClient<Channel>,
        connectors: ConnectorsClient<Channel>,
    }

    impl BridgeGrpc {
        async fn connect(addr: SocketAddr) -> Self {
            let ch = Channel::from_shared(format!("http://{addr}"))
                .expect("uri")
                .connect()
                .await
                .expect("h2c connect");
            BridgeGrpc {
                secrets: SecretsClient::new(ch.clone()),
                connectors: ConnectorsClient::new(ch),
            }
        }
    }

    fn str_vec(v: &Value) -> Vec<String> {
        v.as_array()
            .map(|a| {
                a.iter()
                    .map(|x| x.as_str().expect("string").to_owned())
                    .collect()
            })
            .unwrap_or_default()
    }

    impl BridgeClient for BridgeGrpc {
        async fn put_secret(&mut self, name: &str, value: &[u8], tenant: Option<&str>) -> Out {
            let r = self
                .secrets
                .put_secret(pb::PutSecretRequest {
                    name: name.into(),
                    value: value.to_vec(),
                    tenant: tenant.map(Into::into),
                })
                .await
                .map_err(grpc_err)?;
            Ok(pb_secret(&r.into_inner()))
        }

        async fn list_secrets(&mut self, tenant: Option<&str>) -> Out {
            let r = self
                .secrets
                .list_secrets(pb::ListSecretsRequest {
                    tenant: tenant.map(Into::into),
                })
                .await
                .map_err(grpc_err)?;
            Ok(Value::Array(
                r.into_inner().secrets.iter().map(pb_secret).collect(),
            ))
        }

        async fn delete_secret(&mut self, name: &str, tenant: Option<&str>) -> Out {
            self.secrets
                .delete_secret(pb::DeleteSecretRequest {
                    name: name.into(),
                    tenant: tenant.map(Into::into),
                })
                .await
                .map_err(grpc_err)?;
            Ok(json!({}))
        }

        async fn put_grant(&mut self, g: Value) -> Out {
            let r = self
                .secrets
                .put_grant(pb::PutGrantRequest {
                    grant: Some(pb::GrantInfo {
                        id: g["id"].as_str().unwrap_or_default().into(),
                        module: g["module"].as_str().unwrap_or_default().into(),
                        secret: g["secret"].as_str().unwrap_or_default().into(),
                        origins: str_vec(&g["origins"]),
                        body: g["body"].as_bool().unwrap_or(false),
                    }),
                })
                .await
                .map_err(grpc_err)?;
            Ok(pb_grant(&r.into_inner()))
        }

        async fn list_grants(&mut self, module: Option<&str>) -> Out {
            let r = self
                .secrets
                .list_grants(pb::ListGrantsRequest {
                    module: module.map(Into::into),
                })
                .await
                .map_err(grpc_err)?;
            Ok(Value::Array(
                r.into_inner().grants.iter().map(pb_grant).collect(),
            ))
        }

        async fn delete_grant(&mut self, id: &str) -> Out {
            self.secrets
                .delete_grant(pb::DeleteGrantRequest { id: id.into() })
                .await
                .map_err(grpc_err)?;
            Ok(json!({}))
        }

        async fn put_connection(&mut self, name: &str, kind: &str, config: Value) -> Out {
            let r = self
                .connectors
                .put_connection(pb::PutConnectionRequest {
                    connection: Some(pb::ConnectionInfo {
                        name: name.into(),
                        kind: kind.into(),
                        config_json: serde_json::to_vec(&config).expect("json"),
                    }),
                })
                .await
                .map_err(grpc_err)?;
            Ok(pb_connection(&r.into_inner()))
        }

        async fn get_connection(&mut self, name: &str) -> Out {
            let r = self
                .connectors
                .get_connection(pb::GetConnectionRequest { name: name.into() })
                .await
                .map_err(grpc_err)?;
            Ok(pb_connection(&r.into_inner()))
        }

        async fn list_connections(&mut self) -> Out {
            let r = self
                .connectors
                .list_connections(pb::ListConnectionsRequest {})
                .await
                .map_err(grpc_err)?;
            Ok(Value::Array(
                r.into_inner()
                    .connections
                    .iter()
                    .map(pb_connection)
                    .collect(),
            ))
        }

        async fn delete_connection(&mut self, name: &str) -> Out {
            self.connectors
                .delete_connection(pb::DeleteConnectionRequest { name: name.into() })
                .await
                .map_err(grpc_err)?;
            Ok(json!({}))
        }

        async fn put_egress(&mut self, module: &str, allow: &[&str]) -> Out {
            let r = self
                .connectors
                .put_egress(pb::PutEgressRequest {
                    egress: Some(pb::EgressInfo {
                        module: module.into(),
                        allow: allow.iter().map(|s| (*s).to_owned()).collect(),
                    }),
                })
                .await
                .map_err(grpc_err)?;
            Ok(pb_egress(&r.into_inner()))
        }

        async fn get_egress(&mut self, module: &str) -> Out {
            let r = self
                .connectors
                .get_egress(pb::GetEgressRequest {
                    module: module.into(),
                })
                .await
                .map_err(grpc_err)?;
            Ok(pb_egress(&r.into_inner()))
        }

        async fn list_egress(&mut self) -> Out {
            let r = self
                .connectors
                .list_egress(pb::ListEgressRequest {})
                .await
                .map_err(grpc_err)?;
            Ok(Value::Array(
                r.into_inner().policies.iter().map(pb_egress).collect(),
            ))
        }

        async fn delete_egress(&mut self, module: &str) -> Out {
            self.connectors
                .delete_egress(pb::DeleteEgressRequest {
                    module: module.into(),
                })
                .await
                .map_err(grpc_err)?;
            Ok(json!({}))
        }
    }

    async fn scenario<C: BridgeClient>(c: &mut C) -> Vec<(String, Out)> {
        let mut t: Vec<(String, Out)> = Vec::new();
        macro_rules! step {
            ($label:expr, $e:expr) => {
                t.push(($label.to_owned(), $e.await));
            };
        }
        step!(
            "put secret",
            c.put_secret("stripe/live", b"sk_live_0123456789", None)
        );
        step!(
            "put tenant secret",
            c.put_secret("db/pass", b"hunter2-hunter2", Some("acme"))
        );
        step!(
            "replace secret",
            c.put_secret("stripe/live", b"sk_live_9876543210", None)
        );
        step!("put short secret", c.put_secret("k", b"short", None));
        step!(
            "put reserved secret",
            c.put_secret("__ostro/token-key", b"0123456789", None)
        );
        step!(
            "put bad secret name",
            c.put_secret("Bad Name", b"0123456789", None)
        );
        step!("list secrets", c.list_secrets(None));
        step!("list tenant secrets", c.list_secrets(Some("acme")));
        step!("delete secret", c.delete_secret("db/pass", Some("acme")));
        step!(
            "delete secret again",
            c.delete_secret("db/pass", Some("acme"))
        );
        step!(
            "put grant",
            c.put_grant(json!({
                "module": "acme/billing", "secret": "stripe/live",
                "origins": ["https://api.stripe.com"], "body": true,
            }))
        );
        step!(
            "put grant with id",
            c.put_grant(json!({
                "id": "secret:acme/web:stripe/live", "module": "acme/web",
                "secret": "stripe/live", "origins": ["https://*.example.com:8443"],
            }))
        );
        step!(
            "put grant wrong id",
            c.put_grant(json!({
                "id": "secret:x:y", "module": "acme/web",
                "secret": "stripe/live", "origins": ["https://x.com"],
            }))
        );
        step!(
            "put grant bare star",
            c.put_grant(json!({ "module": "acme/web", "secret": "k", "origins": ["*"] }))
        );
        step!("list grants", c.list_grants(None));
        step!("list grants by module", c.list_grants(Some("acme/web")));
        step!(
            "delete grant",
            c.delete_grant("secret:acme/web:stripe/live")
        );
        step!(
            "delete grant again",
            c.delete_grant("secret:acme/web:stripe/live")
        );
        step!(
            "put connection",
            c.put_connection(
                "orders-db",
                "sql-postgres",
                json!({ "v": 1, "host": "db", "password": "{{secret:db/pass}}" })
            )
        );
        step!(
            "put http-target connection",
            c.put_connection("https://api.example.com", "http-target", json!({ "v": 1 }))
        );
        step!(
            "put connection bad kind",
            c.put_connection("x", "redis", json!({ "v": 1 }))
        );
        step!(
            "put connection bad version",
            c.put_connection("x", "object", json!({ "v": 2 }))
        );
        step!(
            "get connection",
            c.get_connection("https://api.example.com")
        );
        step!("get unknown connection", c.get_connection("nope"));
        step!("list connections", c.list_connections());
        step!("delete connection", c.delete_connection("orders-db"));
        step!("delete connection again", c.delete_connection("orders-db"));
        step!(
            "put egress",
            c.put_egress(
                "acme/billing",
                &["https://api.stripe.com", "connection:orders-db"]
            )
        );
        step!("put egress bad entry", c.put_egress("acme/billing", &["*"]));
        step!("put egress bad module", c.put_egress("a:b", &[]));
        step!("get egress", c.get_egress("acme/billing"));
        step!("get unknown egress", c.get_egress("acme/none"));
        step!("list egress", c.list_egress());
        step!("delete egress", c.delete_egress("acme/billing"));
        step!("delete egress again", c.delete_egress("acme/billing"));
        step!("list egress empty", c.list_egress());
        t
    }

    #[tokio::test]
    async fn bridge_rest_and_grpc_transcripts_are_equal() {
        let rest_srv = serve(cfg()).await;
        let grpc_srv = serve(cfg()).await;
        let rest = scenario(&mut RestClient::new(rest_srv.addr)).await;
        let grpc = scenario(&mut BridgeGrpc::connect(grpc_srv.addr).await).await;
        assert_eq!(rest.len(), grpc.len());
        for ((label, r), (_, g)) in rest.iter().zip(grpc.iter()) {
            assert_eq!(r, g, "step {label:?} differs between REST and gRPC");
        }
        // Both fakes stored the same (replaced) value; the API never returned it.
        for srv in [&rest_srv, &grpc_srv] {
            assert_eq!(
                srv.cp.secret_value("stripe/live", None).as_deref(),
                Some(&b"sk_live_9876543210"[..])
            );
        }

        let get = |l: &str| rest.iter().find(|(x, _)| x == l).expect(l).1.clone();
        let err = |l: &str| get(l).expect_err(l);
        for l in [
            "put short secret",
            "put reserved secret",
            "put bad secret name",
            "put grant wrong id",
            "put grant bare star",
            "put connection bad kind",
            "put connection bad version",
            "put egress bad entry",
            "put egress bad module",
        ] {
            assert_eq!(err(l).0, ErrorCode::InvalidArgument, "{l}");
        }
        assert_eq!(
            err("put short secret").1.expect("details")["reason"],
            "secret_too_short"
        );
        for l in [
            "delete secret again",
            "delete grant again",
            "get unknown connection",
            "delete connection again",
            "get unknown egress",
            "delete egress again",
        ] {
            assert_eq!(err(l).0, ErrorCode::NotFound, "{l}");
        }
        assert_eq!(
            get("put grant").unwrap()["id"],
            "secret:acme/billing:stripe/live"
        );
        assert_eq!(
            get("list secrets").unwrap().as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            get("list grants").unwrap().as_array().map(Vec::len),
            Some(2)
        );
        assert_eq!(
            get("list grants by module")
                .unwrap()
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(
            get("get connection").unwrap()["name"],
            "https://api.example.com"
        );
        assert_eq!(get("list egress empty").unwrap(), json!([]));
    }
}

// ---------------------------------------------------------------------------------
// Phase 7 (07-11): queue flow, pause/resume, demand, workers, join tokens, join
// ---------------------------------------------------------------------------------

mod phase7 {
    use super::*;
    use ostro_api::phase7::{self as p7, Placement, WorkerInfo};
    use ostro_proto::api::v1::{
        queues_client::QueuesClient, runs_client::RunsClient, workers_client::WorkersClient,
        workflows_client::WorkflowsClient,
    };

    const CSR: &str =
        "-----BEGIN CERTIFICATE REQUEST-----\nZmFrZQ==\n-----END CERTIFICATE REQUEST-----\n";

    /// Errors keyed by code and details; RateLimited disambiguated by its details.
    fn grpc_err7(s: tonic::Status) -> (ErrorCode, Option<Value>) {
        let details = status_details(&s);
        (
            ErrorCode::from_grpc_code_and_details(s.code(), details.as_ref()),
            details,
        )
    }

    /// Normalise a flow (either transport) through the domain type.
    fn flow_info(v: p7::QueueFlowInfo) -> Value {
        serde_json::to_value(v).expect("flow json")
    }

    fn placement_name(v: i32) -> Value {
        match pb::Placement::try_from(v).expect("placement") {
            pb::Placement::Unspecified => Value::Null,
            pb::Placement::Embedded => json!("embedded"),
            pb::Placement::Workers => json!("workers"),
            pb::Placement::Auto => json!("auto"),
        }
    }

    fn exhaust_name(v: i32) -> Option<p7::ExhaustAction> {
        match pb::ExhaustAction::try_from(v).expect("exhaust") {
            pb::ExhaustAction::Unspecified => None,
            pb::ExhaustAction::Fail => Some(p7::ExhaustAction::Fail),
            pb::ExhaustAction::Pause => Some(p7::ExhaustAction::Pause),
        }
    }

    fn rate(r: Option<pb::RateSpec>) -> Option<p7::RateSpecDto> {
        r.map(|r| p7::RateSpecDto {
            limit: r.limit,
            period_ms: r.period_ms,
            burst: r.burst,
        })
    }

    fn pb_flow_info(i: pb::QueueFlowInfo) -> Value {
        let f = i.flow.unwrap_or_default();
        let placement = match placement_name(f.placement) {
            Value::Null => None,
            v => Some(serde_json::from_value(v).expect("placement")),
        };
        flow_info(p7::QueueFlowInfo {
            queue: i.queue,
            version: i.version,
            flow: p7::QueueFlow {
                placement,
                selector: f.selector,
                spill: f.spill.map(|s| p7::SpillPolicyDto {
                    cpu_pct: s.cpu_pct as u8,
                    slots_pct: s.slots_pct as u8,
                    mem_pct: s.mem_pct as u8,
                    lag_ms: s.lag_ms,
                    sustain_ms: s.sustain_ms,
                    cooldown_ms: s.cooldown_ms,
                    hysteresis_pct: s.hysteresis_pct as u8,
                }),
                concurrency: f.concurrency,
                weight: f.weight,
                key_concurrency: f.key_concurrency,
                throttle: rate(f.throttle),
                key_throttle: rate(f.key_throttle),
                rate_limit: rate(f.rate_limit),
                key_rate_limit: rate(f.key_rate_limit),
                on_retries_exhausted: exhaust_name(f.on_retries_exhausted),
                paused: f.paused,
            },
        })
    }

    fn pause(name: &str, paused: bool, changed: bool) -> Value {
        json!({ "name": name, "paused": paused, "changed": changed })
    }

    fn pb_worker(w: &pb::WorkerInfo) -> Value {
        let labels: std::collections::BTreeMap<_, _> = w.labels.clone().into_iter().collect();
        json!({
            "worker_id": w.worker_id, "labels": labels, "slots": w.slots,
            "free_slots": w.free_slots, "leases": w.leases,
            "connected_at_ms": w.connected_at_ms, "node_id": w.node_id,
            "draining": w.draining,
        })
    }

    fn rest_worker(v: &Value) -> Value {
        json!({
            "worker_id": v["worker_id"], "labels": v["labels"], "slots": v["slots"],
            "free_slots": v["free_slots"], "leases": v["leases"],
            "connected_at_ms": v["connected_at_ms"], "node_id": v["node_id"],
            "draining": v["draining"],
        })
    }

    fn token_info(id: &Value, created: &Value, expires: &Value) -> Value {
        json!({ "token_id": id, "created_at_ms": created, "expires_at_ms": expires })
    }

    #[derive(Clone, Default)]
    struct Flow {
        body: Value,
        expected: Option<u64>,
    }

    trait P7Client {
        async fn get_flow(&mut self, q: &str) -> Out;
        async fn put_flow(&mut self, q: &str, f: Flow) -> Out;
        async fn pause_queue(&mut self, q: &str) -> Out;
        async fn resume_queue(&mut self, q: &str) -> Out;
        async fn demand(&mut self, q: &str) -> Out;
        async fn pause_workflow(&mut self, w: &str) -> Out;
        async fn resume_workflow(&mut self, w: &str) -> Out;
        async fn pause_run(&mut self, r: &str, detail: Option<&str>) -> Out;
        async fn resume_run(&mut self, r: &str) -> Out;
        async fn list_workers(&mut self) -> Out;
        async fn drain(&mut self, w: &str) -> Out;
        async fn create_token(&mut self, ttl_ms: Option<u64>) -> Out;
        async fn list_tokens(&mut self) -> Out;
        async fn revoke(&mut self, id: &str) -> Out;
        async fn join(&mut self, token: &str, worker_id: &str, csr: &str) -> Out;
        async fn start_on(&mut self, queue: &str) -> Out;
    }

    impl P7Client for RestClient {
        async fn get_flow(&mut self, q: &str) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/queues/{q}/flow", self.base)))
                .await?;
            Ok(flow_info(serde_json::from_value(v).expect("flow")))
        }

        async fn put_flow(&mut self, q: &str, f: Flow) -> Out {
            let mut body = f.body.clone();
            if let Some(e) = f.expected {
                body["expected_version"] = json!(e);
            }
            let v = self
                .send(
                    self.http
                        .put(format!("{}/v1/queues/{q}/flow", self.base))
                        .json(&body),
                )
                .await?;
            Ok(flow_info(serde_json::from_value(v).expect("flow")))
        }

        async fn pause_queue(&mut self, q: &str) -> Out {
            self.send(self.http.post(format!("{}/v1/queues/{q}/pause", self.base)))
                .await
        }

        async fn resume_queue(&mut self, q: &str) -> Out {
            self.send(
                self.http
                    .post(format!("{}/v1/queues/{q}/resume", self.base)),
            )
            .await
        }

        async fn demand(&mut self, q: &str) -> Out {
            self.send(self.http.get(format!("{}/v1/queues/{q}/demand", self.base)))
                .await
        }

        async fn pause_workflow(&mut self, w: &str) -> Out {
            self.send(
                self.http
                    .post(format!("{}/v1/workflows/{w}/pause", self.base)),
            )
            .await
        }

        async fn resume_workflow(&mut self, w: &str) -> Out {
            self.send(
                self.http
                    .post(format!("{}/v1/workflows/{w}/resume", self.base)),
            )
            .await
        }

        async fn pause_run(&mut self, r: &str, detail: Option<&str>) -> Out {
            let req = self.http.post(format!("{}/v1/runs/{r}/pause", self.base));
            let req = match detail {
                Some(d) => req.json(&json!({ "detail": d })),
                None => req,
            };
            let v = self.send(req).await?;
            Ok(json!({ "name": v["run_id"], "paused": v["paused"], "changed": v["changed"] }))
        }

        async fn resume_run(&mut self, r: &str) -> Out {
            let v = self
                .send(self.http.post(format!("{}/v1/runs/{r}/resume", self.base)))
                .await?;
            Ok(json!({ "name": v["run_id"], "paused": v["paused"], "changed": v["changed"] }))
        }

        async fn list_workers(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/workers", self.base)))
                .await?;
            Ok(Value::Array(
                v["workers"]
                    .as_array()
                    .expect("workers")
                    .iter()
                    .map(rest_worker)
                    .collect(),
            ))
        }

        async fn drain(&mut self, w: &str) -> Out {
            self.send(
                self.http
                    .post(format!("{}/v1/workers/{w}/drain", self.base)),
            )
            .await
        }

        async fn create_token(&mut self, ttl_ms: Option<u64>) -> Out {
            let req = self.http.post(format!("{}/v1/workers/tokens", self.base));
            let req = match ttl_ms {
                Some(t) => req.json(&json!({ "ttl_ms": t })),
                None => req,
            };
            self.send(req).await
        }

        async fn list_tokens(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/workers/tokens", self.base)))
                .await?;
            Ok(Value::Array(
                v["tokens"]
                    .as_array()
                    .expect("tokens")
                    .iter()
                    .map(|t| token_info(&t["token_id"], &t["created_at_ms"], &t["expires_at_ms"]))
                    .collect(),
            ))
        }

        async fn revoke(&mut self, id: &str) -> Out {
            self.send(
                self.http
                    .delete(format!("{}/v1/workers/tokens/{id}", self.base)),
            )
            .await?;
            Ok(json!({}))
        }

        async fn join(&mut self, token: &str, worker_id: &str, csr: &str) -> Out {
            self.send(
                self.http
                    .post(format!("{}/v1/workers/join", self.base))
                    .json(&json!({
                        "token": token, "csr_pem": csr, "worker_id": worker_id,
                        "labels": { "zone": "a" },
                    })),
            )
            .await
        }

        async fn start_on(&mut self, queue: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/workflows/greet/runs", self.base))
                        .json(&json!({ "input": {}, "queue": queue })),
                )
                .await?;
            Ok(json!({ "run_id": v["run_id"] }))
        }
    }

    struct P7Grpc {
        queues: QueuesClient<Channel>,
        workers: WorkersClient<Channel>,
        runs: RunsClient<Channel>,
        workflows: WorkflowsClient<Channel>,
    }

    impl P7Grpc {
        async fn connect(addr: SocketAddr) -> Self {
            let ch = Channel::from_shared(format!("http://{addr}"))
                .expect("uri")
                .connect()
                .await
                .expect("h2c connect");
            P7Grpc {
                queues: QueuesClient::new(ch.clone()),
                workers: WorkersClient::new(ch.clone()),
                runs: RunsClient::new(ch.clone()),
                workflows: WorkflowsClient::new(ch),
            }
        }
    }

    /// The REST JSON body as the proto flow (the scenario writes REST JSON).
    fn pb_flow(body: &Value) -> pb::QueueFlow {
        // Unknown enum names map to an out-of-range proto value: both transports reject.
        let placement = match body.get("placement").and_then(Value::as_str) {
            None => pb::Placement::Unspecified as i32,
            Some(s) => Placement::parse(s).map_or(99, |p| match p {
                Placement::Embedded => pb::Placement::Embedded as i32,
                Placement::Workers => pb::Placement::Workers as i32,
                Placement::Auto => pb::Placement::Auto as i32,
            }),
        };
        let exhaust = match body.get("on_retries_exhausted").and_then(Value::as_str) {
            None => pb::ExhaustAction::Unspecified as i32,
            Some("fail") => pb::ExhaustAction::Fail as i32,
            Some("pause") => pb::ExhaustAction::Pause as i32,
            Some(_) => 99,
        };
        let rate = |k: &str| {
            body.get(k).map(|r| pb::RateSpec {
                limit: r["limit"].as_u64().unwrap_or(0) as u32,
                period_ms: r["period_ms"].as_u64().unwrap_or(0),
                burst: r.get("burst").and_then(Value::as_u64).unwrap_or(0) as u32,
            })
        };
        let u32_of = |k: &str| body.get(k).and_then(Value::as_u64).map(|v| v as u32);
        pb::QueueFlow {
            placement,
            selector: body
                .get("selector")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .map(|s| s.as_str().expect("selector").to_owned())
                        .collect()
                })
                .unwrap_or_default(),
            spill: body.get("spill").map(|s| pb::SpillPolicy {
                cpu_pct: s["cpu_pct"].as_u64().unwrap_or(0) as u32,
                slots_pct: s["slots_pct"].as_u64().unwrap_or(0) as u32,
                mem_pct: s["mem_pct"].as_u64().unwrap_or(0) as u32,
                lag_ms: s["lag_ms"].as_u64().unwrap_or(0),
                sustain_ms: s["sustain_ms"].as_u64().unwrap_or(0),
                cooldown_ms: s["cooldown_ms"].as_u64().unwrap_or(0),
                hysteresis_pct: s["hysteresis_pct"].as_u64().unwrap_or(0) as u32,
            }),
            concurrency: u32_of("concurrency"),
            weight: u32_of("weight"),
            key_concurrency: u32_of("key_concurrency"),
            throttle: rate("throttle"),
            key_throttle: rate("key_throttle"),
            rate_limit: rate("rate_limit"),
            key_rate_limit: rate("key_rate_limit"),
            on_retries_exhausted: exhaust,
            paused: body.get("paused").and_then(Value::as_bool).unwrap_or(false),
        }
    }

    fn pb_queue_pause(s: pb::QueuePauseState) -> Value {
        pause(&s.queue, s.paused, s.changed)
    }

    fn pb_wf_pause(s: pb::WorkflowPauseState) -> Value {
        pause(&s.workflow, s.paused, s.changed)
    }

    fn pb_run_pause(s: pb::RunPauseState) -> Value {
        pause(&s.run_id, s.paused, s.changed)
    }

    impl P7Client for P7Grpc {
        async fn get_flow(&mut self, q: &str) -> Out {
            let r = self
                .queues
                .get_queue_flow(pb::GetQueueFlowRequest { name: q.into() })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_flow_info(r.into_inner()))
        }

        async fn put_flow(&mut self, q: &str, f: Flow) -> Out {
            let r = self
                .queues
                .put_queue_flow(pb::PutQueueFlowRequest {
                    name: q.into(),
                    flow: Some(pb_flow(&f.body)),
                    expected_version: f.expected,
                })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_flow_info(r.into_inner()))
        }

        async fn pause_queue(&mut self, q: &str) -> Out {
            let r = self
                .queues
                .pause_queue(pb::PauseQueueRequest { name: q.into() })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_queue_pause(r.into_inner()))
        }

        async fn resume_queue(&mut self, q: &str) -> Out {
            let r = self
                .queues
                .resume_queue(pb::ResumeQueueRequest { name: q.into() })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_queue_pause(r.into_inner()))
        }

        async fn demand(&mut self, q: &str) -> Out {
            let d = self
                .queues
                .get_queue_demand(pb::GetQueueDemandRequest { name: q.into() })
                .await
                .map_err(grpc_err7)?
                .into_inner();
            Ok(json!({
                "queue": d.queue, "demand": d.demand, "backlog": d.backlog,
                "running_on_workers": d.running_on_workers,
                "placement": placement_name(d.placement), "spilling": d.spilling,
                "workers": d.workers, "worker_slots_free": d.worker_slots_free,
            }))
        }

        async fn pause_workflow(&mut self, w: &str) -> Out {
            let r = self
                .workflows
                .pause_workflow(pb::PauseWorkflowRequest { name: w.into() })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_wf_pause(r.into_inner()))
        }

        async fn resume_workflow(&mut self, w: &str) -> Out {
            let r = self
                .workflows
                .resume_workflow(pb::ResumeWorkflowRequest { name: w.into() })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_wf_pause(r.into_inner()))
        }

        async fn pause_run(&mut self, r: &str, detail: Option<&str>) -> Out {
            let s = self
                .runs
                .pause_run(pb::PauseRunRequest {
                    run_id: r.into(),
                    detail: detail.map(str::to_owned),
                })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_run_pause(s.into_inner()))
        }

        async fn resume_run(&mut self, r: &str) -> Out {
            let s = self
                .runs
                .resume_run(pb::ResumeRunRequest { run_id: r.into() })
                .await
                .map_err(grpc_err7)?;
            Ok(pb_run_pause(s.into_inner()))
        }

        async fn list_workers(&mut self) -> Out {
            let r = self
                .workers
                .list_workers(pb::ListWorkersRequest {})
                .await
                .map_err(grpc_err7)?;
            Ok(Value::Array(
                r.into_inner().workers.iter().map(pb_worker).collect(),
            ))
        }

        async fn drain(&mut self, w: &str) -> Out {
            let r = self
                .workers
                .drain_worker(pb::DrainWorkerRequest {
                    worker_id: w.into(),
                })
                .await
                .map_err(grpc_err7)?
                .into_inner();
            Ok(json!({ "worker_id": r.worker_id, "already_draining": r.already_draining }))
        }

        async fn create_token(&mut self, ttl_ms: Option<u64>) -> Out {
            let c = self
                .workers
                .create_worker_token(pb::CreateWorkerTokenRequest { ttl_ms })
                .await
                .map_err(grpc_err7)?
                .into_inner();
            Ok(json!({
                "token_id": c.token_id, "token": c.token, "join_command": c.join_command,
                "expires_at_ms": c.expires_at_ms,
            }))
        }

        async fn list_tokens(&mut self) -> Out {
            let r = self
                .workers
                .list_worker_tokens(pb::ListWorkerTokensRequest {})
                .await
                .map_err(grpc_err7)?;
            Ok(Value::Array(
                r.into_inner()
                    .tokens
                    .iter()
                    .map(|t| {
                        token_info(
                            &json!(t.token_id),
                            &json!(t.created_at_ms),
                            &json!(t.expires_at_ms),
                        )
                    })
                    .collect(),
            ))
        }

        async fn revoke(&mut self, id: &str) -> Out {
            self.workers
                .revoke_worker_token(pb::RevokeWorkerTokenRequest {
                    token_id: id.into(),
                })
                .await
                .map_err(grpc_err7)?;
            Ok(json!({}))
        }

        async fn join(&mut self, token: &str, worker_id: &str, csr: &str) -> Out {
            let r = self
                .workers
                .join_worker(pb::JoinWorkerRequest {
                    token: token.into(),
                    csr_pem: csr.into(),
                    worker_id: worker_id.into(),
                    labels: [("zone".to_owned(), "a".to_owned())].into(),
                })
                .await
                .map_err(grpc_err7)?
                .into_inner();
            Ok(json!({
                "client_cert_pem": r.client_cert_pem, "ca_cert_pem": r.ca_cert_pem,
                "endpoints": r.endpoints, "renew_after_ms": r.renew_after_ms,
                "cluster_id": r.cluster_id,
            }))
        }

        async fn start_on(&mut self, queue: &str) -> Out {
            let r = self
                .runs
                .start(pb::StartRunRequest {
                    workflow: "greet".into(),
                    input_json: b"{}".to_vec(),
                    queue: Some(queue.into()),
                    ..Default::default()
                })
                .await
                .map_err(grpc_err7)?;
            Ok(json!({ "run_id": r.into_inner().run_id }))
        }
    }

    async fn setup(cp: &FakeControlPlane) {
        let wasm = fake_module(&[("greet", 1)]);
        cp.upload_module(ostro_api::domain::UploadModule {
            signatures: Vec::new(),
            sha256: sha256_hex(&wasm),
            wasm,
            language: None,
            tenant: None,
            name: None,
        })
        .await
        .expect("upload");
        for (id, free, labels) in [("w-a", 2u32, vec![("gpu", "true")]), ("w-b", 1, vec![])] {
            cp.add_worker(WorkerInfo {
                worker_id: id.into(),
                labels: labels
                    .into_iter()
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
                    .collect(),
                slots: 4,
                free_slots: free,
                leases: 4 - free,
                connected_at_ms: 1_700_000_000_000,
                node_id: 1,
                draining: false,
            });
        }
    }

    async fn scenario<C: P7Client>(c: &mut C) -> Vec<(String, Out)> {
        let mut t: Vec<(String, Out)> = Vec::new();
        macro_rules! keep {
            ($label:expr, $e:expr) => {{
                let out = $e.await;
                t.push(($label.to_owned(), out.clone()));
                out
            }};
        }
        macro_rules! step {
            ($label:expr, $e:expr) => {{
                let out = $e.await;
                t.push(($label.to_owned(), out));
            }};
        }
        let full = json!({
            "placement": "workers",
            "selector": ["gpu=true", "zone in (eu-1,eu-2)"],
            "spill": { "cpu_pct": 70, "slots_pct": 90, "mem_pct": 85, "lag_ms": 5000,
                "sustain_ms": 10000, "cooldown_ms": 30000, "hysteresis_pct": 85 },
            "concurrency": 8, "weight": 2, "key_concurrency": 1,
            "throttle": { "limit": 10, "period_ms": 1000, "burst": 5 },
            "key_throttle": { "limit": 1, "period_ms": 500 },
            "rate_limit": { "limit": 100, "period_ms": 60000 },
            "key_rate_limit": { "limit": 5, "period_ms": 60000, "burst": 1 },
            "on_retries_exhausted": "pause",
        });
        step!("get flow (never stored)", c.get_flow("gpu"));
        step!(
            "put flow",
            c.put_flow(
                "gpu",
                Flow {
                    body: full.clone(),
                    expected: Some(0)
                }
            )
        );
        step!("get flow", c.get_flow("gpu"));
        step!(
            "put flow stale version",
            c.put_flow(
                "gpu",
                Flow {
                    body: full.clone(),
                    expected: Some(0)
                }
            )
        );
        for (label, body) in [
            ("put flow bad placement", json!({ "placement": "gpu" })),
            (
                "put flow bad exhaust",
                json!({ "on_retries_exhausted": "retry" }),
            ),
            (
                "put flow period 0",
                json!({ "throttle": { "limit": 1, "period_ms": 0 } }),
            ),
            (
                "put flow bad selector",
                json!({ "selector": ["zone in eu-1"] }),
            ),
            (
                "put flow too many selectors",
                json!({ "selector": vec!["a"; 33] }),
            ),
            (
                "put flow spill pct",
                json!({ "spill": { "cpu_pct": 101, "slots_pct": 0, "mem_pct": 0,
                    "lag_ms": 0, "sustain_ms": 0, "cooldown_ms": 0, "hysteresis_pct": 0 } }),
            ),
        ] {
            step!(
                label,
                c.put_flow(
                    "gpu",
                    Flow {
                        body,
                        expected: None
                    }
                )
            );
        }
        step!(
            "put flow unconditional",
            c.put_flow(
                "gpu",
                Flow {
                    body: json!({ "placement": "workers" }),
                    expected: None
                }
            )
        );
        step!("pause queue", c.pause_queue("gpu"));
        step!("pause queue again", c.pause_queue("gpu"));
        step!("get flow paused", c.get_flow("gpu"));
        step!("demand paused", c.demand("gpu"));
        step!("resume queue", c.resume_queue("gpu"));
        step!("resume queue again", c.resume_queue("gpu"));
        let run = keep!("start on gpu", c.start_on("gpu"));
        step!("start on gpu 2", c.start_on("gpu"));
        step!("demand", c.demand("gpu"));
        step!("demand embedded queue", c.demand("other"));
        step!("pause workflow", c.pause_workflow("greet"));
        step!("pause workflow again", c.pause_workflow("greet"));
        step!("pause unknown workflow", c.pause_workflow("nope"));
        step!("resume workflow", c.resume_workflow("greet"));
        let run_id = run.expect("run")["run_id"]
            .as_str()
            .expect("run id")
            .to_owned();
        step!("pause run", c.pause_run(&run_id, Some("maintenance")));
        step!("pause run again", c.pause_run(&run_id, None));
        step!("demand with paused run", c.demand("gpu"));
        step!("resume run", c.resume_run(&run_id));
        step!("resume run again", c.resume_run(&run_id));
        step!("pause unknown run", c.pause_run("run-999999", None));
        let long = "x".repeat(1025);
        step!("pause run long detail", c.pause_run(&run_id, Some(&long)));
        step!("list workers", c.list_workers());
        step!("drain worker", c.drain("w-a"));
        step!("drain worker again", c.drain("w-a"));
        step!("drain unknown worker", c.drain("w-x"));
        step!("list workers drained", c.list_workers());
        let created = keep!("create token", c.create_token(Some(3_600_000)));
        step!("create token ttl 0", c.create_token(Some(0)));
        step!("create token default ttl", c.create_token(None));
        step!("list tokens", c.list_tokens());
        let created = created.expect("token");
        let token = created["token"].as_str().expect("token").to_owned();
        let token_id = created["token_id"].as_str().expect("id").to_owned();
        step!("join", c.join(&token, "w-c", CSR));
        step!(
            "join bad secret",
            c.join(&format!("{token_id}.nope"), "w-c", CSR)
        );
        step!("join bad csr", c.join(&token, "w-c", "not a csr"));
        step!("join bad worker id", c.join(&token, "w c", CSR));
        step!("revoke token", c.revoke(&token_id));
        step!("revoke token again", c.revoke(&token_id));
        step!("join revoked", c.join(&token, "w-c", CSR));
        step!(
            "rate limit flow",
            c.put_flow(
                "rl",
                Flow {
                    body: json!({ "rate_limit": { "limit": 1, "period_ms": 3_600_000 } }),
                    expected: None
                }
            )
        );
        step!("start on rl", c.start_on("rl"));
        step!("start on rl limited", c.start_on("rl"));
        t
    }

    #[tokio::test]
    async fn phase7_rest_and_grpc_transcripts_are_equal() {
        let rest_srv = serve(cfg()).await;
        let grpc_srv = serve(cfg()).await;
        setup(&rest_srv.cp).await;
        setup(&grpc_srv.cp).await;
        let rest = scenario(&mut RestClient::new(rest_srv.addr)).await;
        let grpc = scenario(&mut P7Grpc::connect(grpc_srv.addr).await).await;
        assert_eq!(rest.len(), grpc.len());
        for ((label, r), (_, g)) in rest.iter().zip(grpc.iter()) {
            assert_eq!(r, g, "step {label:?} differs between REST and gRPC");
        }

        let get = |l: &str| rest.iter().find(|(x, _)| x == l).expect(l).1.clone();
        let err = |l: &str| get(l).expect_err(l);
        for l in [
            "put flow bad placement",
            "put flow bad exhaust",
            "put flow period 0",
            "put flow bad selector",
            "put flow too many selectors",
            "put flow spill pct",
            "pause run long detail",
            "create token ttl 0",
            "join bad secret",
            "join bad csr",
            "join bad worker id",
            "join revoked",
        ] {
            assert_eq!(err(l).0, ErrorCode::InvalidArgument, "{l}");
        }
        for l in [
            "pause unknown workflow",
            "pause unknown run",
            "drain unknown worker",
            "revoke token again",
        ] {
            assert_eq!(err(l).0, ErrorCode::NotFound, "{l}");
        }
        assert_eq!(
            err("put flow stale version").0,
            ErrorCode::PreconditionFailed
        );
        let limited = err("start on rl limited");
        assert_eq!(limited.0, ErrorCode::RateLimited);
        assert!(
            limited.1.expect("details")["retry_after_ms"]
                .as_u64()
                .is_some()
        );
        assert_eq!(
            err("join revoked").1.expect("details")["reason"],
            "join_token_invalid"
        );
        assert_eq!(get("put flow").unwrap()["version"], 1);
        assert_eq!(get("get flow").unwrap(), get("put flow").unwrap());
        assert_eq!(get("get flow paused").unwrap()["paused"], true);
        assert_eq!(get("pause queue again").unwrap()["changed"], false);
        assert_eq!(get("demand").unwrap()["demand"], 2);
        assert_eq!(get("demand paused").unwrap()["demand"], 0);
        assert_eq!(get("demand with paused run").unwrap()["backlog"], 1);
        assert_eq!(
            get("demand embedded queue").unwrap()["placement"],
            "embedded"
        );
        assert_eq!(get("list workers drained").unwrap()[0]["draining"], true);
        assert_eq!(
            get("list tokens").unwrap().as_array().map(Vec::len),
            Some(2)
        );
        // The token secret is in the create answer only.
        let secret = get("create token").unwrap()["token"]
            .as_str()
            .unwrap()
            .to_owned();
        assert!(!get("list tokens").unwrap().to_string().contains(&secret));
    }
}

// ---------------------------------------------------------------------------------
// Phase 11 (11-09): RunOps and Inventory parity against `FakeOps` / `FakeInventory`,
// the new ListRuns fields, the history attempt and the API caps on Runs / Tokens
// (D-P11-31). An absent slot is REST 404 vs gRPC UNIMPLEMENTED (service not mounted,
// the Phase 8 convention); that pair is checked separately.
// ---------------------------------------------------------------------------------

mod phase11 {
    use super::*;
    use ostro_api::fake::{FAKE_MAX_BULK_JOBS, FakeInventory, FakeOps};
    use ostro_api::phase11::{
        BulkCreateRequest, BulkCreateResponse, BulkJobView, DlqEntry, DlqPage, DlqRequeueResponse,
        FailurePolicy, LimitsView, ModulePage, ModuleSummary, PartitionView, QueueView, RerunCut,
        RerunCutsView, RerunRequest, RerunResponse, rerun_cut_unsafe,
    };
    use ostro_api::rest::RunStatusDto;
    use ostro_api::{ApiExtensions, phase11 as p11};
    use ostro_proto::api::v1::{inventory_client::InventoryClient, run_ops_client::RunOpsClient};

    const CAP: usize = 1024;

    struct P11Server {
        addr: SocketAddr,
        cp: Arc<FakeControlPlane>,
        ops: Arc<FakeOps>,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for P11Server {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn p11_cfg() -> ApiConfig {
        ApiConfig {
            max_payload_bytes: CAP,
            ..cfg()
        }
    }

    async fn serve_p11(with_slots: bool) -> P11Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let cp = Arc::new(FakeControlPlane::new());
        let ops = FakeOps::new();
        let inv = FakeInventory::new();
        let mut ext = ApiExtensions::default();
        if with_slots {
            ops.install(&mut ext);
            inv.install(&mut ext);
        }
        seed(&ops, &inv);
        let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
        let app = ostro_api::router_with(dyn_cp, p11_cfg(), ext);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        P11Server {
            addr,
            cp,
            ops,
            task,
        }
    }

    fn seed(ops: &FakeOps, inv: &FakeInventory) {
        for i in 1..=5u64 {
            ops.push_dlq(DlqEntry {
                run_id: format!("run-dlq-{i}"),
                workflow: "greet".into(),
                version: 1,
                queue: "default".into(),
                status: if i == 3 {
                    RunStatusDto::TimedOut
                } else {
                    RunStatusDto::Failed
                },
                reason: format!("boom {i}"),
                failed_at_ms: 1_000 * i,
                tags: vec![format!("t{}", i % 2)],
                resumable: i == 1,
                hook_run_id: (i == 2).then(|| "run-hook".to_owned()),
                dismissed_at_ms: None,
                dismiss_reason: None,
                requeued_to: None,
                requeued_at_ms: None,
            });
        }
        ops.set_cuts(RerunCutsView {
            run_id: "run-src".into(),
            cuts: vec![
                RerunCut {
                    seq: 1,
                    step: None,
                    occurrence: 1,
                    safe: true,
                    reason: None,
                    nearest_safe_seq: None,
                },
                RerunCut {
                    seq: 6,
                    step: Some("charge".into()),
                    occurrence: 2,
                    safe: false,
                    reason: Some("open await".into()),
                    nearest_safe_seq: Some(1),
                },
            ],
        });
        ops.set_bulk_matches((1..=25).map(|i| format!("run-b{i:02}")).collect());
        inv.set_modules(
            (0..3)
                .map(|i| ModuleSummary {
                    sha256: format!("{i:064x}"),
                    size_bytes: 10 + i,
                    uploaded_at_ms: (i > 0).then_some(5),
                    workflows: vec!["greet".into()],
                })
                .collect(),
        );
        inv.set_queues(vec![
            QueueView {
                name: "default".into(),
                paused: false,
                ready: 2,
                running: 1,
                demand: Some(3),
                flow: Some(json!({ "max_concurrency": 4 })),
            },
            QueueView {
                name: "slow".into(),
                paused: true,
                ready: 0,
                running: 0,
                demand: None,
                flow: None,
            },
        ]);
        inv.set_partitions(vec![PartitionView {
            id: 0,
            role: "leader".into(),
            leader: Some(1),
            epoch: Some(3),
            applied_offset: Some(42),
            log_end_offset: None,
            ready: Some(2),
            running: Some(1),
            healthy: true,
            detail: None,
        }]);
    }

    fn canon<T: serde::Serialize>(v: T) -> Value {
        serde_json::to_value(v).expect("json")
    }

    fn rest_as<T: serde::de::DeserializeOwned + serde::Serialize>(v: Value) -> Value {
        canon(serde_json::from_value::<T>(v).expect("dto"))
    }

    /// `?k=v&...` with RFC 3986 percent-encoding (reqwest's `query` feature is off).
    fn qs(pairs: &[(String, String)]) -> String {
        fn enc(s: &str) -> String {
            s.bytes()
                .map(|b| {
                    if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
                        (b as char).to_string()
                    } else {
                        format!("%{b:02X}")
                    }
                })
                .collect()
        }
        if pairs.is_empty() {
            return String::new();
        }
        let q: Vec<String> = pairs
            .iter()
            .map(|(k, v)| format!("{}={}", enc(k), enc(v)))
            .collect();
        format!("?{}", q.join("&"))
    }

    fn grpc_err11(s: tonic::Status) -> (ErrorCode, Option<Value>) {
        let d = status_details(&s);
        (
            ErrorCode::from_grpc_code_and_details(s.code(), d.as_ref()),
            d,
        )
    }

    trait P11Client {
        async fn start(&mut self, input: Value, tags: Vec<&str>) -> Out;
        async fn list(
            &mut self,
            tag: Option<&str>,
            tags: Vec<&str>,
            newest: bool,
            dlq: bool,
        ) -> Out;
        async fn history(&mut self, run_id: &str) -> Out;
        async fn signal(&mut self, run_id: &str, payload: Value) -> Out;
        async fn complete_token(&mut self, token: &str, payload: Value) -> Out;
        async fn rerun(&mut self, r: Value) -> Out;
        async fn rerun_cuts(&mut self, run_id: &str) -> Out;
        async fn list_dlq(&mut self, q: Value) -> Out;
        async fn requeue(&mut self, run_id: &str, body: Value) -> Out;
        async fn dismiss(&mut self, run_id: &str, reason: &str) -> Out;
        async fn create_bulk(&mut self, r: Value) -> Out;
        async fn list_bulk(&mut self) -> Out;
        async fn get_bulk(&mut self, id: &str) -> Out;
        async fn cancel_bulk(&mut self, id: &str) -> Out;
        async fn limits(&mut self) -> Out;
        async fn get_policy(&mut self, queue: bool, name: &str) -> Out;
        async fn set_policy(&mut self, queue: bool, name: &str, p: Value) -> Out;
        async fn modules(&mut self, limit: u32, token: Option<&str>) -> Out;
        async fn queues(&mut self) -> Out;
        async fn partitions(&mut self) -> Out;
    }

    impl P11Client for RestClient {
        async fn start(&mut self, input: Value, tags: Vec<&str>) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/workflows/greet/runs", self.base))
                        .json(&json!({ "input": input, "tags": tags })),
                )
                .await?;
            Ok(json!({ "run_id": v["run_id"], "created": v["created"] }))
        }

        async fn list(
            &mut self,
            tag: Option<&str>,
            tags: Vec<&str>,
            newest: bool,
            dlq: bool,
        ) -> Out {
            let mut q: Vec<(String, String)> = Vec::new();
            if let Some(t) = tag {
                q.push(("tag".into(), t.into()));
            }
            for t in tags {
                q.push(("tags".into(), t.into()));
            }
            if newest {
                q.push(("order".into(), "newest".into()));
            }
            if dlq {
                q.push(("dlq".into(), "true".into()));
            }
            let v = self
                .send(self.http.get(format!("{}/v1/runs{}", self.base, qs(&q))))
                .await?;
            let ids: Vec<Value> = v["runs"]
                .as_array()
                .expect("runs")
                .iter()
                .map(|r| r["run_id"].clone())
                .collect();
            Ok(json!(ids))
        }

        async fn history(&mut self, run_id: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .get(format!("{}/v1/runs/{run_id}/history", self.base)),
                )
                .await?;
            Ok(json!(
                v["events"]
                    .as_array()
                    .expect("events")
                    .iter()
                    .map(|e| json!({ "seq": e["seq"], "attempt": e.get("attempt") }))
                    .collect::<Vec<_>>()
            ))
        }

        async fn signal(&mut self, run_id: &str, payload: Value) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/runs/{run_id}/signals/go", self.base))
                        .json(&json!({ "payload": payload })),
                )
                .await?;
            Ok(json!({ "accepted": v["accepted"], "duplicate": v["duplicate"] }))
        }

        async fn complete_token(&mut self, token: &str, payload: Value) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/tokens/complete", self.base))
                        .json(&json!({ "token": token, "payload": payload })),
                )
                .await?;
            Ok(json!({ "run_id": v["run_id"], "accepted": v["accepted"] }))
        }

        async fn rerun(&mut self, r: Value) -> Out {
            let run = r["run_id"].as_str().expect("run").to_owned();
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/runs/{run}/rerun", self.base))
                        .json(&r),
                )
                .await?;
            Ok(rest_as::<RerunResponse>(v))
        }

        async fn rerun_cuts(&mut self, run_id: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .get(format!("{}/v1/runs/{run_id}/rerun-cuts", self.base)),
                )
                .await?;
            Ok(rest_as::<RerunCutsView>(v))
        }

        async fn list_dlq(&mut self, q: Value) -> Out {
            let mut pairs: Vec<(String, String)> = Vec::new();
            for (k, v) in q.as_object().expect("query") {
                match v {
                    Value::Array(a) => {
                        for t in a {
                            pairs.push((k.clone(), t.as_str().expect("tag").to_owned()));
                        }
                    }
                    Value::String(s) => pairs.push((k.clone(), s.clone())),
                    other => pairs.push((k.clone(), other.to_string())),
                }
            }
            let v = self
                .send(self.http.get(format!("{}/v1/dlq{}", self.base, qs(&pairs))))
                .await?;
            Ok(rest_as::<DlqPage>(v))
        }

        async fn requeue(&mut self, run_id: &str, body: Value) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/dlq/{run_id}/requeue", self.base))
                        .json(&body),
                )
                .await?;
            Ok(rest_as::<DlqRequeueResponse>(v))
        }

        async fn dismiss(&mut self, run_id: &str, reason: &str) -> Out {
            let v = self
                .send(
                    self.http
                        .post(format!("{}/v1/dlq/{run_id}/dismiss", self.base))
                        .json(&json!({ "reason": reason })),
                )
                .await?;
            Ok(rest_as::<DlqEntry>(v))
        }

        async fn create_bulk(&mut self, r: Value) -> Out {
            let v = self
                .send(self.http.post(format!("{}/v1/bulk", self.base)).json(&r))
                .await?;
            Ok(rest_as::<BulkCreateResponse>(v))
        }

        async fn list_bulk(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/bulk", self.base)))
                .await?;
            Ok(rest_as::<Vec<BulkJobView>>(v["jobs"].clone()))
        }

        async fn get_bulk(&mut self, id: &str) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/bulk/{id}", self.base)))
                .await?;
            Ok(rest_as::<BulkJobView>(v))
        }

        async fn cancel_bulk(&mut self, id: &str) -> Out {
            let v = self
                .send(self.http.post(format!("{}/v1/bulk/{id}/cancel", self.base)))
                .await?;
            Ok(rest_as::<BulkJobView>(v))
        }

        async fn limits(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/limits", self.base)))
                .await?;
            Ok(rest_as::<LimitsView>(v))
        }

        async fn get_policy(&mut self, queue: bool, name: &str) -> Out {
            let kind = if queue { "queues" } else { "workflows" };
            let v = self
                .send(
                    self.http
                        .get(format!("{}/v1/{kind}/{name}/failure-policy", self.base)),
                )
                .await?;
            Ok(rest_as::<FailurePolicy>(v))
        }

        async fn set_policy(&mut self, queue: bool, name: &str, p: Value) -> Out {
            let kind = if queue { "queues" } else { "workflows" };
            let v = self
                .send(
                    self.http
                        .put(format!("{}/v1/{kind}/{name}/failure-policy", self.base))
                        .json(&p),
                )
                .await?;
            Ok(rest_as::<FailurePolicy>(v))
        }

        async fn modules(&mut self, limit: u32, token: Option<&str>) -> Out {
            let mut q = vec![("limit".to_owned(), limit.to_string())];
            if let Some(t) = token {
                q.push(("page_token".into(), t.into()));
            }
            let v = self
                .send(self.http.get(format!("{}/v1/modules{}", self.base, qs(&q))))
                .await?;
            Ok(rest_as::<ModulePage>(v))
        }

        async fn queues(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/queues", self.base)))
                .await?;
            Ok(rest_as::<Vec<QueueView>>(v["queues"].clone()))
        }

        async fn partitions(&mut self) -> Out {
            let v = self
                .send(self.http.get(format!("{}/v1/partitions", self.base)))
                .await?;
            Ok(rest_as::<Vec<PartitionView>>(v["partitions"].clone()))
        }
    }

    struct GrpcP11 {
        runs: RunsClient<Channel>,
        tokens: TokensClient<Channel>,
        ops: RunOpsClient<Channel>,
        inv: InventoryClient<Channel>,
    }

    impl GrpcP11 {
        async fn connect(addr: SocketAddr) -> Self {
            let ch = Channel::from_shared(format!("http://{addr}"))
                .expect("uri")
                .connect()
                .await
                .expect("connect");
            GrpcP11 {
                runs: RunsClient::new(ch.clone()),
                tokens: TokensClient::new(ch.clone()),
                ops: RunOpsClient::new(ch.clone()),
                inv: InventoryClient::new(ch),
            }
        }
    }

    impl P11Client for GrpcP11 {
        async fn start(&mut self, input: Value, tags: Vec<&str>) -> Out {
            let r = self
                .runs
                .start(pb::StartRunRequest {
                    workflow: "greet".into(),
                    input_json: serde_json::to_vec(&input).expect("json"),
                    tags: tags.into_iter().map(str::to_owned).collect(),
                    ..Default::default()
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(json!({ "run_id": r.run_id, "created": r.created }))
        }

        async fn list(
            &mut self,
            tag: Option<&str>,
            tags: Vec<&str>,
            newest: bool,
            dlq: bool,
        ) -> Out {
            let r = self
                .runs
                .list(pb::ListRunsRequest {
                    tag: tag.map(str::to_owned),
                    tags: tags.into_iter().map(str::to_owned).collect(),
                    order: if newest {
                        pb::ListOrder::Newest as i32
                    } else {
                        pb::ListOrder::Unspecified as i32
                    },
                    dlq,
                    ..Default::default()
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(json!(
                r.runs.iter().map(|r| r.run_id.clone()).collect::<Vec<_>>()
            ))
        }

        async fn history(&mut self, run_id: &str) -> Out {
            let r = self
                .runs
                .history(pb::RunHistoryRequest {
                    run_id: run_id.into(),
                    from_seq: 0,
                    limit: 0,
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(json!(
                r.events
                    .iter()
                    .map(|e| json!({ "seq": e.seq, "attempt": e.attempt }))
                    .collect::<Vec<_>>()
            ))
        }

        async fn signal(&mut self, run_id: &str, payload: Value) -> Out {
            let r = self
                .runs
                .signal(pb::SignalRunRequest {
                    run_id: run_id.into(),
                    name: "go".into(),
                    payload_json: serde_json::to_vec(&payload).expect("json"),
                    ..Default::default()
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(json!({ "accepted": r.accepted, "duplicate": r.duplicate }))
        }

        async fn complete_token(&mut self, token: &str, payload: Value) -> Out {
            let r = self
                .tokens
                .complete(pb::CompleteTokenRequest {
                    token: token.into(),
                    payload_json: serde_json::to_vec(&payload).expect("json"),
                    dry_run: false,
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(json!({ "run_id": r.run_id, "accepted": r.accepted }))
        }

        async fn rerun(&mut self, r: Value) -> Out {
            let req: RerunRequest = serde_json::from_value(r).expect("dto");
            let res = self
                .ops
                .rerun(pb::RerunRequest::from(req))
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(RerunResponse::from(res)))
        }

        async fn rerun_cuts(&mut self, run_id: &str) -> Out {
            let res = self
                .ops
                .rerun_cuts(pb::RerunCutsRequest {
                    run_id: run_id.into(),
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(RerunCutsView::from(res)))
        }

        async fn list_dlq(&mut self, q: Value) -> Out {
            let s = |k: &str| q.get(k).and_then(Value::as_str).map(str::to_owned);
            let res = self
                .ops
                .list_dlq(pb::ListDlqRequest {
                    workflow: s("workflow"),
                    queue: s("queue"),
                    reason: s("reason"),
                    tags: q
                        .get("tags")
                        .and_then(Value::as_array)
                        .map(|a| {
                            a.iter()
                                .map(|t| t.as_str().expect("tag").to_owned())
                                .collect()
                        })
                        .unwrap_or_default(),
                    include_dismissed: q
                        .get("include_dismissed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    limit: q.get("limit").and_then(Value::as_u64).unwrap_or(0) as u32,
                    page_token: s("page_token"),
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(DlqPage::try_from(res).expect("page")))
        }

        async fn requeue(&mut self, run_id: &str, body: Value) -> Out {
            let b: p11::RequeueBody = serde_json::from_value(body).expect("body");
            let res = self
                .ops
                .requeue_dlq(pb::RequeueDlqRequest {
                    run_id: run_id.into(),
                    mode: Some(b.mode.into()),
                    dry_run: b.dry_run,
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(DlqRequeueResponse::from(res)))
        }

        async fn dismiss(&mut self, run_id: &str, reason: &str) -> Out {
            let res = self
                .ops
                .dismiss_dlq(pb::DismissDlqRequest {
                    run_id: run_id.into(),
                    reason: reason.into(),
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(DlqEntry::try_from(res).expect("entry")))
        }

        async fn create_bulk(&mut self, r: Value) -> Out {
            let req: BulkCreateRequest = serde_json::from_value(r).expect("dto");
            let res = self
                .ops
                .create_bulk(pb::CreateBulkRequest::from(req))
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(BulkCreateResponse::from(res)))
        }

        async fn list_bulk(&mut self) -> Out {
            let res = self
                .ops
                .list_bulk(pb::ListBulkRequest {})
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(
                res.jobs
                    .into_iter()
                    .map(BulkJobView::from)
                    .collect::<Vec<_>>(),
            ))
        }

        async fn get_bulk(&mut self, id: &str) -> Out {
            let res = self
                .ops
                .get_bulk(pb::GetBulkRequest { job_id: id.into() })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(BulkJobView::from(res)))
        }

        async fn cancel_bulk(&mut self, id: &str) -> Out {
            let res = self
                .ops
                .cancel_bulk(pb::CancelBulkRequest { job_id: id.into() })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(BulkJobView::from(res)))
        }

        async fn limits(&mut self) -> Out {
            let res = self
                .ops
                .get_limits(pb::GetLimitsRequest {})
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(LimitsView::from(res)))
        }

        async fn get_policy(&mut self, queue: bool, name: &str) -> Out {
            use pb::get_failure_policy_request::Scope;
            let scope = if queue {
                Scope::Queue(name.into())
            } else {
                Scope::Workflow(name.into())
            };
            let res = self
                .ops
                .get_failure_policy(pb::GetFailurePolicyRequest { scope: Some(scope) })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(FailurePolicy::from(res)))
        }

        async fn set_policy(&mut self, queue: bool, name: &str, p: Value) -> Out {
            use pb::set_failure_policy_request::Scope;
            let scope = if queue {
                Scope::Queue(name.into())
            } else {
                Scope::Workflow(name.into())
            };
            let p: FailurePolicy = serde_json::from_value(p).expect("dto");
            let res = self
                .ops
                .set_failure_policy(pb::SetFailurePolicyRequest {
                    scope: Some(scope),
                    policy: Some(p.into()),
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(FailurePolicy::from(res)))
        }

        async fn modules(&mut self, limit: u32, token: Option<&str>) -> Out {
            let res = self
                .inv
                .list_modules(pb::ListModulesRequest {
                    limit,
                    page_token: token.map(str::to_owned),
                })
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(ModulePage::from(res)))
        }

        async fn queues(&mut self) -> Out {
            let res = self
                .inv
                .list_queues(pb::ListQueuesRequest {})
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(
                res.queues
                    .into_iter()
                    .map(|q| QueueView::try_from(q).expect("queue"))
                    .collect::<Vec<_>>(),
            ))
        }

        async fn partitions(&mut self) -> Out {
            let res = self
                .inv
                .list_partitions(pb::ListPartitionsRequest {})
                .await
                .map_err(grpc_err11)?
                .into_inner();
            Ok(canon(
                res.partitions
                    .into_iter()
                    .map(|p| PartitionView::try_from(p).expect("partition"))
                    .collect::<Vec<_>>(),
            ))
        }
    }

    async fn scenario11<C: P11Client>(c: &mut C, srv: &P11Server) -> Vec<(String, Out)> {
        let mut t: Vec<(String, Out)> = Vec::new();
        macro_rules! rec {
            ($label:expr, $e:expr) => {
                t.push(($label.to_owned(), $e));
            };
        }
        let wasm = fake_module(&[("greet", 1)]);
        let dyn_cp: &dyn ControlPlane = srv.cp.as_ref();
        dyn_cp
            .upload_module(ostro_api::domain::UploadModule {
                signatures: Vec::new(),
                sha256: sha256_hex(&wasm),
                wasm,
                language: None,
                tenant: None,
                name: None,
            })
            .await
            .expect("upload");

        // Core Runs / Tokens with the Phase 11 fields and caps.
        rec!("start ab", c.start(json!(1), vec!["a", "b"]).await);
        rec!("start a", c.start(json!(2), vec!["a"]).await);
        rec!("start b", c.start(json!(3), vec!["b"]).await);
        let first = ok_field(&t[0].1, "start ab", &["run_id"]);
        let first = first.as_str().expect("id").to_owned();
        rec!(
            "list tags a,b",
            c.list(None, vec!["a", "b"], false, false).await
        );
        rec!(
            "list tag a newest",
            c.list(Some("a"), vec![], true, false).await
        );
        rec!(
            "list tag a + tags b",
            c.list(Some("a"), vec!["b"], false, false).await
        );
        rec!("list dlq", c.list(None, vec![], false, true).await);
        let nine: Vec<&str> = vec!["1", "2", "3", "4", "5", "6", "7", "8", "9"];
        rec!("list nine tags", c.list(None, nine, false, false).await);
        srv.cp
            .set_history_attempt(&first, 1, Some((2u64 << 32) | 1));
        rec!("history attempt", c.history(&first).await);
        let big = json!({ "s": "x".repeat(CAP) });
        rec!("start over payload cap", c.start(big.clone(), vec![]).await);
        rec!(
            "start system tag",
            c.start(json!(null), vec!["__ostro/x"]).await
        );
        rec!(
            "signal over payload cap",
            c.signal(&first, big.clone()).await
        );
        rec!(
            "signal at cap",
            c.signal(&first, json!("y".repeat(CAP - 2))).await
        );
        let token = srv.cp.mint_token(&first, 1);
        rec!(
            "token over payload cap",
            c.complete_token(&token, big.clone()).await
        );
        rec!("token ok", c.complete_token(&token, json!(1)).await);

        // RunOps.
        rec!(
            "rerun dry",
            c.rerun(json!({ "run_id": "run-src", "from": { "seq": 4 }, "dry_run": true }))
                .await
        );
        let rr = json!({ "run_id": "run-src", "from": { "step": { "name": "charge", "occurrence": 2 } }, "version": { "version": 3 }, "idempotency_key": "k" });
        rec!("rerun", c.rerun(rr.clone()).await);
        rec!("rerun again", c.rerun(rr).await);
        rec!(
            "rerun empty step name",
            c.rerun(json!({ "run_id": "run-src", "from": { "step": { "name": "" } } }))
                .await
        );
        srv.ops.fail(
            "rerun_run",
            Some(rerun_cut_unsafe(6, "open await", Some(1))),
        );
        rec!(
            "rerun unsafe",
            c.rerun(json!({ "run_id": "run-src", "from": { "seq": 6 } }))
                .await
        );
        srv.ops.fail("rerun_run", None);
        rec!("rerun cuts", c.rerun_cuts("run-src").await);
        rec!("rerun cuts unknown", c.rerun_cuts("run-nope").await);

        rec!("dlq page 1", c.list_dlq(json!({ "limit": 2 })).await);
        rec!(
            "dlq page 2",
            c.list_dlq(json!({ "limit": 2, "page_token": "run-dlq-2" }))
                .await
        );
        rec!("dlq tag t1", c.list_dlq(json!({ "tags": ["t1"] })).await);
        rec!(
            "dlq nine tags",
            c.list_dlq(json!({ "tags": ["1", "2", "3", "4", "5", "6", "7", "8", "9"] }))
                .await
        );
        rec!("dismiss 4", c.dismiss("run-dlq-4", "known").await);
        rec!("dismiss 4 again", c.dismiss("run-dlq-4", "again").await);
        rec!(
            "dismiss long reason",
            c.dismiss("run-dlq-5", &"r".repeat(1025)).await
        );
        rec!(
            "dlq include dismissed",
            c.list_dlq(json!({ "include_dismissed": true, "workflow": "greet" }))
                .await
        );
        rec!(
            "requeue auto resumable",
            c.requeue("run-dlq-1", json!({})).await
        );
        rec!(
            "requeue restart dry",
            c.requeue("run-dlq-2", json!({ "mode": "restart", "dry_run": true }))
                .await
        );
        rec!(
            "requeue rerun",
            c.requeue(
                "run-dlq-3",
                json!({ "mode": { "rerun": { "from": { "seq": 2 } } } })
            )
            .await
        );
        rec!("requeue non-member", c.requeue("run-nope", json!({})).await);

        rec!(
            "bulk dry",
            c.create_bulk(json!({ "action": { "signal": { "name": "go", "payload": { "n": 1 } } }, "filter": { "status": "failed", "tags": ["x"], "dlq": true }, "dry_run": true }))
                .await
        );
        rec!(
            "bulk create",
            c.create_bulk(json!({ "action": { "rerun": { "from": { "seq": 3 }, "version": "source" } }, "rate_per_sec": 7 }))
                .await
        );
        let job = ok_field(t.last().map(|x| &x.1).expect("step"), "bulk", &["job_id"]);
        let job = job.as_str().expect("job").to_owned();
        srv.ops.set_bulk_progress(&job, 5, 1);
        rec!("bulk get", c.get_bulk(&job).await);
        rec!("bulk list", c.list_bulk().await);
        rec!("bulk cancel", c.cancel_bulk(&job).await);
        rec!("bulk get unknown", c.get_bulk("blk_nope").await);
        rec!(
            "bulk bad rate",
            c.create_bulk(json!({ "action": "cancel", "rate_per_sec": 0 }))
                .await
        );
        for i in 0..FAKE_MAX_BULK_JOBS {
            rec!(
                format!("bulk fill {i}"),
                c.create_bulk(json!({ "action": { "requeue": { "mode": "auto" } } }))
                    .await
            );
        }
        rec!(
            "bulk too many jobs",
            c.create_bulk(json!({ "action": "resume" })).await
        );

        rec!("limits", c.limits().await);
        rec!("policy get empty", c.get_policy(false, "greet").await);
        rec!(
            "policy bad hook name",
            c.set_policy(false, "greet", json!({ "on_failure": { "workflow": "" } }))
                .await
        );
        rec!(
            "policy dlq on workflow",
            c.set_policy(
                false,
                "greet",
                json!({ "dlq": { "enabled": true, "hook_catchup_ms": 1 } })
            )
            .await
        );
        rec!(
            "policy set",
            c.set_policy(false, "greet", json!({ "on_failure": { "workflow": "acme/alert", "queue": "ops", "include_input": true } }))
                .await
        );
        rec!(
            "policy stale",
            c.set_policy(
                false,
                "greet",
                json!({ "on_failure": { "workflow": "alert" }, "version_tag": 9 })
            )
            .await
        );
        rec!(
            "queue policy set",
            c.set_policy(
                true,
                "default",
                json!({ "dlq": { "enabled": false, "hook_catchup_ms": 5 } })
            )
            .await
        );
        rec!("queue policy get", c.get_policy(true, "default").await);
        rec!("bad scope name", c.get_policy(true, "a b").await);

        // Inventory.
        rec!("modules page 1", c.modules(2, None).await);
        let tok = ok_field(
            t.last().map(|x| &x.1).expect("step"),
            "modules",
            &["next_page_token"],
        );
        rec!("modules page 2", c.modules(2, tok.as_str()).await);
        rec!("queues", c.queues().await);
        rec!("partitions", c.partitions().await);
        t
    }

    #[tokio::test]
    async fn phase11_rest_and_grpc_transcripts_are_equal() {
        let rest_srv = serve_p11(true).await;
        let mut rest = RestClient::new(rest_srv.addr);
        let rest_t = scenario11(&mut rest, &rest_srv).await;

        let grpc_srv = serve_p11(true).await;
        let mut grpc = GrpcP11::connect(grpc_srv.addr).await;
        let grpc_t = scenario11(&mut grpc, &grpc_srv).await;

        assert_eq!(rest_t.len(), grpc_t.len());
        for ((label, r), (_, g)) in rest_t.iter().zip(&grpc_t) {
            assert_eq!(r, g, "step {label:?} differs\nREST: {r:#?}\ngRPC: {g:#?}");
        }

        let get = |label: &str| {
            rest_t
                .iter()
                .find(|(l, _)| l == label)
                .unwrap_or_else(|| panic!("no step {label}"))
                .1
                .clone()
        };
        let err = |label: &str| get(label).expect_err(label);
        // Search fields reached the control plane on both transports.
        assert_eq!(
            get("list tags a,b").expect("ok").as_array().map(Vec::len),
            Some(1)
        );
        assert_eq!(
            get("list tag a newest")
                .expect("ok")
                .as_array()
                .map(Vec::len),
            Some(2)
        );
        let (code, d) = err("list nine tags");
        assert_eq!(code, ErrorCode::InvalidArgument);
        assert_eq!(d.expect("details")["limit"], "search_tags");
        assert_eq!(
            get("history attempt").expect("ok")[0]["attempt"],
            (2u64 << 32) | 1
        );
        // Caps: 413 <-> RESOURCE_EXHAUSTED with identical details; 400 <-> INVALID_ARGUMENT.
        for label in [
            "start over payload cap",
            "signal over payload cap",
            "token over payload cap",
        ] {
            let (code, d) = err(label);
            assert_eq!(code, ErrorCode::PayloadTooLarge, "{label}");
            let d = d.expect("details");
            assert_eq!(d["reason"], "limit_exceeded", "{label}");
            assert_eq!(d["limit"], "payload_bytes", "{label}");
            assert_eq!(d["max"], CAP, "{label}");
        }
        assert_eq!(err("start system tag").0, ErrorCode::InvalidArgument);
        get("signal at cap").expect("exactly at the cap");
        // Ops error mapping.
        let (code, d) = err("rerun unsafe");
        assert_eq!(code, ErrorCode::Conflict);
        assert_eq!(d.expect("details")["reason"], "rerun_cut_unsafe");
        assert_eq!(err("rerun cuts unknown").0, ErrorCode::NotFound);
        assert_eq!(get("rerun again").expect("ok")["existing"], true);
        assert_eq!(
            err("dismiss 4 again").1.expect("d")["reason"],
            "dlq_not_member"
        );
        assert_eq!(
            err("dismiss long reason").1.expect("d")["limit"],
            "reason_bytes"
        );
        let (code, d) = err("bulk too many jobs");
        assert_eq!(code, ErrorCode::RateLimited);
        assert_eq!(d.expect("details")["reason"], "bulk_too_many_jobs");
        assert_eq!(
            err("policy stale").1.expect("d")["reason"],
            "version_mismatch"
        );
        assert_eq!(get("bulk cancel").expect("ok")["state"], "cancelled");
        assert_eq!(get("requeue rerun").expect("ok")["mode"], "rerun");
        assert_eq!(get("requeue auto resumable").expect("ok")["mode"], "resume");
        assert_eq!(
            get("modules page 2").expect("ok")["modules"]
                .as_array()
                .map(Vec::len),
            Some(1)
        );
        assert_eq!(get("queues").expect("ok")[0]["flow"]["max_concurrency"], 4);
    }

    #[tokio::test]
    async fn absent_slots_are_rest_404_and_grpc_unimplemented() {
        let srv = serve_p11(false).await;
        let http = reqwest::Client::new();
        for p in [
            "/v1/limits",
            "/v1/dlq",
            "/v1/bulk",
            "/v1/modules",
            "/v1/partitions",
        ] {
            let r = http
                .get(format!("http://{}{p}", srv.addr))
                .send()
                .await
                .expect("get");
            assert_eq!(r.status().as_u16(), 404, "{p}");
        }
        let mut g = GrpcP11::connect(srv.addr).await;
        let e = g
            .ops
            .get_limits(pb::GetLimitsRequest {})
            .await
            .expect_err("not mounted");
        assert_eq!(e.code(), tonic::Code::Unimplemented);
        let e = g
            .inv
            .list_queues(pb::ListQueuesRequest {})
            .await
            .expect_err("not mounted");
        assert_eq!(e.code(), tonic::Code::Unimplemented);
        // The core services still answer.
        g.runs
            .list(pb::ListRunsRequest::default())
            .await
            .expect("Runs is mounted");
    }
}

/// The REST-only exclusion list (D-P10-18) names exactly the Phase 10 tooling routes,
/// each documented in OpenAPI under the `dev` / `tooling` tags and described as
/// REST-only, and no gRPC service of the API package is named after them.
#[test]
fn rest_only_exclusions_are_exactly_the_phase10_tooling_routes() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let mut tagged = Vec::new();
    for (path, item) in doc["paths"].as_object().expect("paths") {
        for (method, op) in item.as_object().expect("path item") {
            let tags = op["tags"].as_array().cloned().unwrap_or_default();
            if tags.iter().any(|t| t == "dev" || t == "tooling") {
                tagged.push((method.clone(), path.clone()));
            }
        }
    }
    tagged.sort();
    let mut expected: Vec<(String, String)> = REST_ONLY
        .iter()
        .map(|(m, p)| ((*m).to_owned(), (*p).to_owned()))
        .collect();
    expected.sort();
    assert_eq!(tagged, expected, "REST-only routes drifted from REST_ONLY");
    for (method, path) in REST_ONLY {
        let desc = doc["paths"][*path][*method]["description"]
            .as_str()
            .unwrap_or_default();
        assert!(desc.contains("REST-only"), "{method} {path}: {desc}");
    }
}
