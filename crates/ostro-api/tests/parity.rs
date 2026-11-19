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
use ostro_api::{ApiConfig, ControlPlane, ErrorCode};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::{
    modules_client::ModulesClient, runs_client::RunsClient, workflows_client::WorkflowsClient,
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

trait Client {
    async fn upload(&mut self, sha: &str, wasm: Bytes) -> Out;
    async fn get_module(&mut self, sha: &str) -> Out;
    async fn list_workflows(&mut self) -> Out;
    async fn activate(&mut self, name: &str, version: u32, expected: Option<u64>) -> Out;
    async fn start(&mut self, s: Start) -> Out;
    async fn get_run(&mut self, run_id: &str) -> Out;
    async fn history(&mut self, run_id: &str, from_seq: u64, limit: u32) -> Out;
    async fn list_runs(&mut self, l: List) -> Out;
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
            });
            self.http.post(url).json(&body)
        };
        let v = self.send(req).await?;
        Ok(json!({ "run_id": v["run_id"], "created": v["created"] }))
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
}

struct GrpcClient {
    modules: ModulesClient<Channel>,
    workflows: WorkflowsClient<Channel>,
    runs: RunsClient<Channel>,
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
            runs: RunsClient::new(channel),
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
            })
            .await
            .map_err(grpc_err)?
            .into_inner();
        Ok(json!({ "run_id": r.run_id, "created": r.created }))
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
        json!({ "run_id": "run-000001", "created": false })
    );
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
    ] {
        assert_eq!(
            get(label).unwrap_err().0,
            ErrorCode::PayloadTooLarge,
            "{label}"
        );
    }
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
