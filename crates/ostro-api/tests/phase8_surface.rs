//! Phase 8 REST surface (08-10) against `FakePhase8` over a real listener: routes,
//! status codes, validation, CAS, the raw-body hook route and its own body limit, run
//! logs, the backup download, absent slots (404 / gRPC UNIMPLEMENTED), the OpenAPI
//! paths, and inbound `traceparent` (REST header and gRPC metadata).

use std::sync::Arc;

use ostro_api::fake::{FAKE_BACKUP, FakeControlPlane, FakePhase8, fake_module, sha256_hex};
use ostro_api::phase8::{LogLevelDto, RunLogLine};
use ostro_api::{ApiConfig, ApiExtensions, ControlPlane, ErrorCode};
use ostro_proto::api::v1 as pb;
use reqwest::StatusCode;
use serde_json::{Value, json};

const TP: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
const TP2: &str = "00-11111111111111111111111111111111-2222222222222222-00";

struct Server {
    addr: std::net::SocketAddr,
    cp: Arc<FakeControlPlane>,
    p8: Arc<FakePhase8>,
    http: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(with_ext: bool, hook_body_limit: usize) -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let cp = Arc::new(FakeControlPlane::new());
        let p8 = FakePhase8::new(cp.clone());
        let ext = if with_ext {
            ApiExtensions {
                hook_body_limit,
                ..p8.extensions()
            }
        } else {
            ApiExtensions::default()
        };
        let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
        let app = ostro_api::router_with(dyn_cp, ApiConfig::default(), ext);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let srv = Server {
            addr,
            cp,
            p8,
            http: reqwest::Client::builder()
                .http1_only()
                .build()
                .expect("client"),
            task,
        };
        let wasm = fake_module(&[("greet", 1)]);
        let r = srv
            .http
            .put(srv.url(&format!("/v1/modules/{}", sha256_hex(&wasm))))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("upload");
        assert_eq!(r.status(), StatusCode::CREATED);
        srv
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn send(&self, req: reqwest::RequestBuilder) -> (StatusCode, Value) {
        let r = req.send().await.expect("request");
        let st = r.status();
        let text = r.text().await.expect("body");
        (
            st,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.http.get(self.url(path))).await
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.http.post(self.url(path)).json(&body)).await
    }

    async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        self.send(self.http.put(self.url(path)).json(&body)).await
    }

    async fn delete(&self, path: &str) -> (StatusCode, Value) {
        self.send(self.http.delete(self.url(path))).await
    }

    async fn grpc_channel(&self) -> tonic::transport::Channel {
        tonic::transport::Channel::from_shared(format!("http://{}", self.addr))
            .expect("uri")
            .connect()
            .await
            .expect("connect")
    }
}

fn code(v: &Value) -> &str {
    v["error"]["code"].as_str().unwrap_or("<none>")
}

fn reason(v: &Value) -> &str {
    v["error"]["details"]["reason"].as_str().unwrap_or("<none>")
}

fn schedule(name: &str) -> Value {
    json!({ "name": name, "workflow": "greet", "cron": "*/5 * * * *", "input": {"n": 1} })
}

#[tokio::test]
async fn schedules_crud_cas_pause_trigger() {
    let s = Server::start(true, 1024).await;
    let (st, v) = s.post("/v1/schedules", schedule("nightly")).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["version_tag"], 1);
    assert_eq!(v["overlap"], "skip");
    assert_eq!(v["catchup"], "skip");
    assert_eq!(v["timezone"], "");

    let (st, v) = s.post("/v1/schedules", schedule("nightly")).await;
    assert_eq!((st, code(&v)), (StatusCode::CONFLICT, "conflict"));
    for bad in [
        json!({ "name": "a/b", "workflow": "greet", "cron": "* * * * *" }),
        json!({ "name": "x", "workflow": "", "cron": "* * * * *" }),
        json!({ "name": "x", "workflow": "greet", "cron": " " }),
        json!({ "name": "x", "workflow": "greet", "cron": "* * * * *", "overlap": "sometimes" }),
        json!({ "name": "x", "workflow": "greet", "cron": "* * * * *", "bogus": 1 }),
        json!({ "name": "x", "workflow": "greet", "cron": "* * * * *", "start_at_ms": 9, "end_at_ms": 1 }),
    ] {
        let (st, v) = s.post("/v1/schedules", bad.clone()).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad} -> {v}");
    }

    let (st, v) = s.get("/v1/schedules").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["schedules"].as_array().map(Vec::len), Some(1));
    let (st, _) = s.get("/v1/schedules/missing").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // PUT: path name wins, body name must match; CAS through body or If-Match.
    let mut upd = schedule("");
    upd["overlap"] = json!("buffer_one");
    upd["version_tag"] = json!(1);
    let (st, v) = s.put("/v1/schedules/nightly", upd.clone()).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        (v["name"].clone(), v["version_tag"].clone()),
        (json!("nightly"), json!(2))
    );
    assert_eq!(v["overlap"], "buffer_one");
    let (st, v) = s.put("/v1/schedules/nightly", upd.clone()).await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "version_mismatch"));
    assert_eq!(v["error"]["details"]["current_version_tag"], 2);
    let mut other = schedule("other");
    other["version_tag"] = Value::Null;
    let (st, _) = s.put("/v1/schedules/nightly", other).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "body name mismatch");
    let (st, v) = s
        .send(
            s.http
                .put(s.url("/v1/schedules/nightly"))
                .header("if-match", "\"2\"")
                .json(&schedule("nightly")),
        )
        .await;
    assert_eq!((st, v["version_tag"].clone()), (StatusCode::OK, json!(3)));
    let (st, _) = s
        .send(
            s.http
                .put(s.url("/v1/schedules/nightly"))
                .header("if-match", "3")
                .json(&json!({"name": "nightly", "workflow": "greet", "cron": "* * * * *", "version_tag": 4})),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "If-Match and body disagree");

    let (st, v) = s.post("/v1/schedules/nightly/pause", json!({})).await;
    assert_eq!((st, v["paused"].clone()), (StatusCode::OK, json!(true)));
    let (st, v) = s.post("/v1/schedules/nightly/resume", json!({})).await;
    assert_eq!((st, v["paused"].clone()), (StatusCode::OK, json!(false)));

    // Trigger: empty body allowed; request_id dedupes.
    let r = s
        .http
        .post(s.url("/v1/schedules/nightly/trigger"))
        .send()
        .await
        .expect("trigger");
    assert_eq!(r.status(), StatusCode::OK);
    let v: Value = r.json().await.expect("json");
    let run1 = v["run_id"].as_str().expect("run_id").to_owned();
    let run = s.cp.get_run(run1.clone()).await.expect("run");
    assert_eq!(run.workflow, "greet");
    assert_eq!(run.tags, vec!["ostro.schedule".to_owned()]);
    let (_, a) = s
        .post("/v1/schedules/nightly/trigger", json!({"request_id": "r1"}))
        .await;
    let (_, b) = s
        .post("/v1/schedules/nightly/trigger", json!({"request_id": "r1"}))
        .await;
    assert_eq!(a["run_id"], b["run_id"]);
    assert_ne!(a["run_id"], json!(run1));
    let (st, _) = s
        .post(
            "/v1/schedules/nightly/trigger",
            json!({"request_id": "x".repeat(65)}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // DELETE with a stale ?version_tag, then a good one.
    let (st, v) = s.delete("/v1/schedules/nightly?version_tag=1").await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "version_mismatch"));
    let (st, v) = s.delete("/v1/schedules/nightly").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v, json!({"name": "nightly", "deleted": true}));
    let (st, _) = s.delete("/v1/schedules/nightly").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

fn webhook(name: &str, verifier: &str) -> Value {
    json!({
        "name": name,
        "action": {"kind": "start", "workflow": "greet"},
        "verifier": {"kind": verifier, "secret_ref": if verifier == "none" { "" } else { "sec" }},
    })
}

#[tokio::test]
async fn webhooks_crud_and_never_return_secret_values() {
    let s = Server::start(true, 1024).await;
    let (st, v) = s.post("/v1/webhooks", webhook("gh", "github")).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(
        v["action"],
        json!({"kind": "start", "workflow": "greet", "version": null, "input": "body"})
    );
    assert_eq!(v["verifier"]["secret_ref"], "sec");
    assert_eq!(v["version_tag"], 1);
    let (st, v) = s
        .post(
            "/v1/webhooks",
            json!({"name": "x", "action": {"kind": "start", "workflow": "greet"}, "verifier": {"kind": "stripe"}}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "secret_ref required: {v}");
    let (st, _) = s
        .post(
            "/v1/webhooks",
            json!({"name": "x", "action": {"kind": "signal", "workflow": "greet", "name": "go", "run_by": {"run_id_pointer": "run"}}}),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "pointer must start with /");
    let (st, v) = s.get("/v1/webhooks").await;
    assert_eq!(
        (st, v["webhooks"].as_array().map(Vec::len)),
        (StatusCode::OK, Some(1))
    );
    let mut upd = webhook("gh", "github");
    upd["idempotency"] = json!("header:X-GitHub-Delivery");
    upd["version_tag"] = json!(7);
    let (st, v) = s.put("/v1/webhooks/gh", upd.clone()).await;
    assert_eq!((st, reason(&v)), (StatusCode::CONFLICT, "version_mismatch"));
    upd["version_tag"] = json!(1);
    let (st, v) = s.put("/v1/webhooks/gh", upd).await;
    assert_eq!((st, v["version_tag"].clone()), (StatusCode::OK, json!(2)));
    let (st, v) = s.get("/v1/webhooks/gh").await;
    assert_eq!(
        (st, v["idempotency"].clone()),
        (StatusCode::OK, json!("header:X-GitHub-Delivery"))
    );
    let (st, _) = s.delete("/v1/webhooks/gh").await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = s.get("/v1/webhooks/gh").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn hook_ingress_raw_body_limit_and_text_responses() {
    let s = Server::start(true, 1024).await;
    s.post("/v1/webhooks", webhook("open", "none")).await;
    s.post("/v1/webhooks", webhook("signed", "stripe")).await;

    // Raw bytes reach the ingress unchanged (not re-serialised).
    let raw = "{ \"order\" :  42 }";
    let r = s
        .http
        .post(s.url("/hooks/open"))
        .header("content-type", "application/json")
        .body(raw)
        .send()
        .await
        .expect("hook");
    assert_eq!(r.status(), StatusCode::ACCEPTED);
    let v: Value = r.json().await.expect("json");
    let run =
        s.cp.get_run(v["run_id"].as_str().expect("run_id").to_owned())
            .await
            .expect("run");
    assert_eq!(run.workflow, "greet");
    assert_eq!(s.p8.deliveries()[0].1.as_ref(), raw.as_bytes());

    // A body that is not JSON still reaches the ingress (no JSON extractor).
    let r = s
        .http
        .post(s.url("/hooks/signed"))
        .body("t=1,v1=zz not json")
        .send()
        .await
        .expect("hook");
    assert_eq!(r.status(), StatusCode::UNAUTHORIZED);

    // Slack-style challenge echoed as plain text.
    let r = s
        .http
        .post(s.url("/hooks/open"))
        .body(r#"{"type":"url_verification","challenge":"abc123"}"#)
        .send()
        .await
        .expect("hook");
    assert_eq!(r.status(), StatusCode::OK);
    assert!(
        r.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    assert_eq!(r.text().await.unwrap(), "abc123");

    // The hook route has its own limit (1024 here), below api.max_request_bytes.
    let r = s
        .http
        .post(s.url("/hooks/open"))
        .body(vec![b' '; 2048])
        .send()
        .await
        .expect("hook");
    assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let v: Value = r.json().await.expect("json");
    assert_eq!(code(&v), "payload_too_large");

    let (st, _) = s.post("/hooks/missing", json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    let (st, _) = s.post("/hooks/bad%20name", json!({})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
}

fn line(ts_ms: u64, level: LogLevelDto, message: &str) -> RunLogLine {
    RunLogLine {
        ts_ms,
        level,
        message: message.into(),
        fields: json!({"k": ts_ms}),
        attempt: 1,
    }
}

#[tokio::test]
async fn run_logs_limit_level_and_unknown_run() {
    let s = Server::start(true, 1024).await;
    let (_, v) = s.post("/v1/workflows/greet/runs", json!({})).await;
    let run = v["run_id"].as_str().expect("run_id").to_owned();
    s.p8.push_log(&run, line(1, LogLevelDto::Debug, "a"));
    s.p8.push_log(&run, line(2, LogLevelDto::Info, "b"));
    s.p8.push_log(&run, line(3, LogLevelDto::Error, "c"));
    s.p8.push_log(&run, line(4, LogLevelDto::Warn, "d"));

    let (st, v) = s.get(&format!("/v1/runs/{run}/logs")).await;
    assert_eq!(st, StatusCode::OK);
    let msgs: Vec<_> = v["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["message"].clone())
        .collect();
    assert_eq!(msgs, vec![json!("a"), json!("b"), json!("c"), json!("d")]);
    assert_eq!(
        v["lines"][0],
        json!({"ts_ms": 1, "level": "debug", "message": "a", "fields": {"k": 1}, "attempt": 1})
    );
    assert_eq!(v["dropped"], 0);

    let (_, v) = s
        .get(&format!("/v1/runs/{run}/logs?level=warn&limit=1"))
        .await;
    let msgs: Vec<_> = v["lines"]
        .as_array()
        .unwrap()
        .iter()
        .map(|l| l["message"].clone())
        .collect();
    assert_eq!(msgs, vec![json!("d")]);
    let (st, _) = s.get(&format!("/v1/runs/{run}/logs?level=loud")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, _) = s.get("/v1/runs/nope/logs").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn admin_backup_streams_tar() {
    let s = Server::start(true, 1024).await;
    let r = s
        .http
        .get(s.url("/v1/admin/backup"))
        .send()
        .await
        .expect("backup");
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(r.headers()["content-type"], "application/x-tar");
    assert_eq!(
        r.headers()["content-disposition"],
        "attachment; filename=\"ostro-backup.tar\""
    );
    assert_eq!(r.bytes().await.unwrap().as_ref(), FAKE_BACKUP);
}

#[tokio::test]
async fn absent_slots_are_404_and_grpc_unimplemented() {
    let s = Server::start(false, 1024).await;
    for (method, path) in [
        ("GET", "/v1/schedules"),
        ("POST", "/v1/schedules"),
        ("GET", "/v1/schedules/x"),
        ("PUT", "/v1/schedules/x"),
        ("DELETE", "/v1/schedules/x"),
        ("POST", "/v1/schedules/x/pause"),
        ("POST", "/v1/schedules/x/resume"),
        ("POST", "/v1/schedules/x/trigger"),
        ("GET", "/v1/webhooks"),
        ("POST", "/v1/webhooks"),
        ("GET", "/v1/webhooks/x"),
        ("PUT", "/v1/webhooks/x"),
        ("DELETE", "/v1/webhooks/x"),
        ("POST", "/hooks/x"),
        ("GET", "/v1/runs/r/logs"),
        ("GET", "/v1/admin/backup"),
    ] {
        let req = s
            .http
            .request(method.parse().unwrap(), s.url(path))
            .header("content-type", "application/json")
            .body("{}");
        let (st, v) = s.send(req).await;
        assert_eq!(st, StatusCode::NOT_FOUND, "{method} {path}: {v}");
        assert_eq!(code(&v), "not_found", "{method} {path}");
    }
    let ch = s.grpc_channel().await;
    let e = pb::schedules_client::SchedulesClient::new(ch.clone())
        .list(pb::ListSchedulesRequest {})
        .await
        .expect_err("absent");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = pb::webhooks_client::WebhooksClient::new(ch.clone())
        .list(pb::ListWebhooksRequest {})
        .await
        .expect_err("absent");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    let e = pb::run_logs_client::RunLogsClient::new(ch)
        .get_run_logs(pb::GetRunLogsRequest::default())
        .await
        .expect_err("absent");
    assert_eq!(e.code(), tonic::Code::Unimplemented);
}

#[test]
fn openapi_lists_every_phase8_route() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let paths = doc["paths"].as_object().expect("paths");
    for (path, method) in [
        ("/v1/schedules", "get"),
        ("/v1/schedules", "post"),
        ("/v1/schedules/{name}", "get"),
        ("/v1/schedules/{name}", "put"),
        ("/v1/schedules/{name}", "delete"),
        ("/v1/schedules/{name}/pause", "post"),
        ("/v1/schedules/{name}/resume", "post"),
        ("/v1/schedules/{name}/trigger", "post"),
        ("/v1/webhooks", "get"),
        ("/v1/webhooks", "post"),
        ("/v1/webhooks/{name}", "get"),
        ("/v1/webhooks/{name}", "put"),
        ("/v1/webhooks/{name}", "delete"),
        ("/hooks/{name}", "post"),
        ("/v1/runs/{run_id}/logs", "get"),
        ("/v1/admin/backup", "get"),
    ] {
        assert!(
            paths.get(path).and_then(|p| p.get(method)).is_some(),
            "{method} {path} missing from OpenAPI"
        );
    }
    let start = &paths["/v1/workflows/{name}/runs"]["post"]["parameters"];
    assert!(
        start
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["name"] == "traceparent" && p["in"] == "header")
    );
    let tags: Vec<_> = doc["tags"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_owned())
        .collect();
    for t in ["schedules", "webhooks", "admin"] {
        assert!(tags.contains(&t.to_owned()), "tag {t}");
    }
}

// ---------------------------------------------------------------------------------
// Inbound traceparent
// ---------------------------------------------------------------------------------

async fn rest_start(
    s: &Server,
    header: Option<&str>,
    body_tp: Option<&str>,
) -> (StatusCode, Value) {
    let mut body = json!({});
    if let Some(t) = body_tp {
        body["traceparent"] = json!(t);
    }
    let mut req = s.http.post(s.url("/v1/workflows/greet/runs")).json(&body);
    if let Some(h) = header {
        req = req.header("traceparent", h);
    }
    s.send(req).await
}

async fn stored_tp(s: &Server, run_id: &Value) -> Option<String> {
    s.cp.get_run(run_id.as_str().expect("run_id").to_owned())
        .await
        .expect("run")
        .traceparent
}

#[tokio::test]
async fn rest_traceparent_header_body_both_invalid() {
    let s = Server::start(false, 1024).await;
    let (st, v) = rest_start(&s, Some(TP), None).await;
    assert_eq!(st, StatusCode::CREATED, "header only: {v}");
    assert_eq!(stored_tp(&s, &v["run_id"]).await.as_deref(), Some(TP));
    let (st, v) = rest_start(&s, None, Some(TP)).await;
    assert_eq!(st, StatusCode::CREATED, "body only: {v}");
    assert_eq!(stored_tp(&s, &v["run_id"]).await.as_deref(), Some(TP));
    let (st, v) = rest_start(&s, Some(TP2), Some(TP)).await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(
        stored_tp(&s, &v["run_id"]).await.as_deref(),
        Some(TP),
        "body wins"
    );
    let (st, v) = rest_start(&s, Some(TP2), Some("")).await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(
        stored_tp(&s, &v["run_id"]).await.as_deref(),
        Some(TP2),
        "empty body field"
    );
    let (st, v) = rest_start(&s, None, None).await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(stored_tp(&s, &v["run_id"]).await, None);
    for (h, b) in [
        (Some("00-xyz"), None),
        (
            None,
            Some("00-00000000000000000000000000000000-b7ad6b7169203331-01"),
        ),
        (
            Some("01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"),
            Some(""),
        ),
    ] {
        let (st, v) = rest_start(&s, h, b).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{h:?} {b:?}: {v}");
        assert_eq!(reason(&v), "invalid_traceparent");
    }
}

#[tokio::test]
async fn grpc_traceparent_metadata_body_both_invalid() {
    let s = Server::start(false, 1024).await;
    let mut runs = pb::runs_client::RunsClient::new(s.grpc_channel().await);
    async fn start(
        runs: &mut pb::runs_client::RunsClient<tonic::transport::Channel>,
        md: Option<&str>,
        body: Option<&str>,
    ) -> Result<String, tonic::Status> {
        let mut req = tonic::Request::new(pb::StartRunRequest {
            workflow: "greet".into(),
            traceparent: body.map(str::to_owned),
            ..Default::default()
        });
        if let Some(m) = md {
            req.metadata_mut()
                .insert("traceparent", m.parse().expect("ascii"));
        }
        runs.start(req).await.map(|r| r.into_inner().run_id)
    }
    let id = start(&mut runs, Some(TP), None)
        .await
        .expect("metadata only");
    assert_eq!(stored_tp(&s, &json!(id)).await.as_deref(), Some(TP));
    let id = start(&mut runs, None, Some(TP)).await.expect("body only");
    assert_eq!(stored_tp(&s, &json!(id)).await.as_deref(), Some(TP));
    let id = start(&mut runs, Some(TP2), Some(TP)).await.expect("both");
    assert_eq!(
        stored_tp(&s, &json!(id)).await.as_deref(),
        Some(TP),
        "body wins"
    );
    for (m, b) in [(Some("00-xyz"), None), (None, Some("garbage"))] {
        let e = start(&mut runs, m, b).await.expect_err("invalid");
        assert_eq!(e.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            ostro_api::grpc::status_details(&e).expect("details")["reason"],
            "invalid_traceparent"
        );
        assert_eq!(
            ErrorCode::from_grpc_code(e.code()),
            ErrorCode::InvalidArgument
        );
    }
}
