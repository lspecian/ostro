//! REST adapter behaviour against `FakeControlPlane`, over a real HTTP listener.

use std::sync::Arc;

use ostro_api::domain::RunStatus;
use ostro_api::fake::{FAKE_EPOCH_MS, FAKE_TICK_MS, FakeControlPlane, fake_module, sha256_hex};
use ostro_api::{ApiConfig, ControlPlane};
use reqwest::StatusCode;
use serde_json::{Value, json};

struct Server {
    base: String,
    cp: Arc<FakeControlPlane>,
    http: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(cfg: ApiConfig) -> Server {
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
        Server {
            base: format!("http://{addr}"),
            cp,
            http: reqwest::Client::builder()
                .http1_only()
                .build()
                .expect("client"),
            task,
        }
    }

    async fn default() -> Server {
        Server::start(ApiConfig::default()).await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn upload(&self, wasm: bytes::Bytes) -> (StatusCode, Value) {
        let sha = sha256_hex(&wasm);
        self.upload_as(&sha, wasm).await
    }

    async fn upload_as(&self, sha: &str, wasm: bytes::Bytes) -> (StatusCode, Value) {
        let resp = self
            .http
            .put(self.url(&format!("/v1/modules/{sha}?language=rust")))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        decode(resp).await
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        decode(self.http.get(self.url(path)).send().await.expect("GET")).await
    }

    async fn post_json(&self, path: &str, body: Value) -> (StatusCode, Value) {
        decode(
            self.http
                .post(self.url(path))
                .json(&body)
                .send()
                .await
                .expect("POST"),
        )
        .await
    }

    async fn start_run(&self, wf: &str, body: Value) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/workflows/{wf}/runs"), body)
            .await
    }
}

async fn decode(resp: reqwest::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let text = resp.text().await.expect("body");
    let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, v)
}

fn assert_error(v: &Value, code: &str) {
    assert_eq!(v["error"]["code"], code, "body: {v}");
    assert!(v["error"]["message"].is_string(), "body: {v}");
}

#[tokio::test]
async fn upload_created_then_existing_then_mismatch() {
    let s = Server::default().await;
    let wasm = fake_module(&[("greet", 1)]);
    let (st, v) = s.upload(wasm.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v["created"], true);
    assert_eq!(v["module"]["sha256"], sha256_hex(&wasm));
    assert_eq!(v["module"]["language"], "rust");
    assert_eq!(
        v["module"]["workflows"],
        json!([{"name": "greet", "version": 1}])
    );
    assert_eq!(v["module"]["uploaded_at"], "2023-11-14T22:13:21Z");

    let (st, v) = s.upload(wasm.clone()).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["created"], false);

    let (st, v) = s.upload_as(&sha256_hex(b"nope"), wasm.clone()).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_error(&v, "invalid_argument");

    let (st, v) = s.get(&format!("/v1/modules/{}", sha256_hex(&wasm))).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["size_bytes"], wasm.len());
    let (st, v) = s.get(&format!("/v1/modules/{}", "0".repeat(64))).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");

    // Same workflow version from another module -> 409.
    let (st, v) = s.upload(fake_module(&[("greet", 1), ("x", 1)])).await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_error(&v, "conflict");
}

#[tokio::test]
async fn large_module_under_limit_passes_extractor() {
    // Pitfall 7: the axum 2 MiB default must not apply to the upload route.
    let s = Server::default().await;
    let big = bytes::Bytes::from(vec![7u8; 20 * 1024 * 1024]);
    let (st, v) = s.upload_as(&sha256_hex(b"different"), big).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_error(&v, "invalid_argument");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap()
            .contains("sha256 mismatch")
    );
}

#[tokio::test]
async fn module_over_limit_is_413() {
    let s = Server::start(ApiConfig {
        max_module_bytes: 1024 * 1024,
        max_request_bytes: 4 * 1024 * 1024,
    })
    .await;
    let big = bytes::Bytes::from(vec![1u8; 2 * 1024 * 1024]);
    let (st, v) = s.upload(big).await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_error(&v, "payload_too_large");
}

#[tokio::test]
async fn request_body_limit_applies_to_other_routes() {
    let s = Server::start(ApiConfig {
        max_module_bytes: 64 * 1024 * 1024,
        max_request_bytes: 64 * 1024,
    })
    .await;
    s.upload(fake_module(&[("greet", 1)])).await;
    let (st, v) = s
        .start_run("greet", json!({ "input": "x".repeat(128 * 1024) }))
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_error(&v, "payload_too_large");
    // A module bigger than max_request_bytes still uploads.
    let (st, _) = s
        .upload(fake_module(&[("y".repeat(100 * 1024).as_str(), 1)]))
        .await;
    assert_eq!(st, StatusCode::CREATED);
}

#[tokio::test]
async fn start_run_created_existing_and_conflict() {
    let s = Server::default().await;
    s.upload(fake_module(&[("greet", 1)])).await;
    let body = json!({
        "input": { "name": "ada", "n": [1, 2] },
        "idempotency_key": "k1",
        "tags": ["t1"],
        "priority": 3,
    });
    let (st, v) = s.start_run("greet", body.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    assert_eq!(v, json!({ "run_id": "run-000001", "created": true }));

    let (st, v) = s.start_run("greet", body.clone()).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v, json!({ "run_id": "run-000001", "created": false }));

    let mut reject = body.clone();
    reject["conflict_policy"] = json!("reject");
    let (st, v) = s.start_run("greet", reject).await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_error(&v, "conflict");
    assert_eq!(v["error"]["details"]["existing_run_id"], "run-000001");

    // Errors: unknown workflow, unknown field, bad policy, malformed JSON.
    let (st, v) = s.start_run("nope", json!({})).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
    let (st, v) = s.start_run("greet", json!({ "inptu": 1 })).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_error(&v, "invalid_argument");
    let (st, _) = s
        .start_run("greet", json!({ "conflict_policy": "maybe" }))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let resp = s
        .http
        .post(s.url("/v1/workflows/greet/runs"))
        .header("content-type", "application/json")
        .body("{not json")
        .send()
        .await
        .unwrap();
    let (st, v) = decode(resp).await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_error(&v, "invalid_argument");

    // Input defaults to null.
    let (st, v) = s.start_run("greet", json!({})).await;
    assert_eq!(st, StatusCode::CREATED);
    assert_eq!(v["run_id"], "run-000002");
}

#[tokio::test]
async fn get_run_renders_times_and_embedded_json() {
    let s = Server::default().await;
    s.upload(fake_module(&[("greet", 1)])).await;
    let (_, v) = s
        .start_run(
            "greet",
            json!({ "input": 1, "tags": ["a"], "tenant": "acme", "queue": "q1" }),
        )
        .await;
    let run_id = v["run_id"].as_str().unwrap().to_owned();

    let (st, v) = s.get(&format!("/v1/runs/{run_id}")).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["status"], "ready");
    assert_eq!(v["queue"], "q1");
    assert_eq!(v["tenant"], "acme");
    assert_eq!(v["created_at_ms"], FAKE_EPOCH_MS + 2 * FAKE_TICK_MS);
    assert_eq!(v["created_at"], "2023-11-14T22:13:22Z");
    assert_eq!(v["completed_at"], Value::Null);
    assert_eq!(v["result"], Value::Null);

    s.cp.set_run_outcome(
        &run_id,
        RunStatus::Completed,
        Some(br#"{"greeting":"hi"}"#.to_vec()),
        None,
        Some(br#"{"step":3}"#.to_vec()),
    )
    .unwrap();
    let (_, v) = s.get(&format!("/v1/runs/{run_id}")).await;
    assert_eq!(v["status"], "completed");
    assert_eq!(v["result"], json!({ "greeting": "hi" }));
    assert_eq!(v["published_status"], json!({ "step": 3 }));
    assert_eq!(v["completed_at"], "2023-11-14T22:13:23Z");
    assert_eq!(v["completed_at_ms"], FAKE_EPOCH_MS + 3 * FAKE_TICK_MS);

    let (st, v) = s.get("/v1/runs/run-999999").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
}

#[tokio::test]
async fn history_paging() {
    let s = Server::default().await;
    s.upload(fake_module(&[("greet", 1)])).await;
    let (_, v) = s.start_run("greet", json!({ "input": {"a": 1} })).await;
    let run_id = v["run_id"].as_str().unwrap().to_owned();
    s.cp.set_run_outcome(&run_id, RunStatus::Running, None, None, None)
        .unwrap();
    s.cp.set_run_outcome(&run_id, RunStatus::Failed, None, Some("boom".into()), None)
        .unwrap();

    let (st, v) = s
        .get(&format!("/v1/runs/{run_id}/history?from_seq=1&limit=2"))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["events"].as_array().unwrap().len(), 2);
    assert_eq!(v["events"][0]["kind"], "RunStarted");
    assert_eq!(v["events"][0]["details"], json!({ "input": { "a": 1 } }));
    assert_eq!(v["events"][0]["ts"], "2023-11-14T22:13:22Z");
    assert_eq!(v["next_from_seq"], 3);
    let (_, v) = s
        .get(&format!("/v1/runs/{run_id}/history?from_seq=3"))
        .await;
    assert_eq!(v["events"].as_array().unwrap().len(), 1);
    assert_eq!(v["next_from_seq"], Value::Null);

    let (st, v) = s.get("/v1/runs/run-999999/history").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
    let (st, v) = s
        .get(&format!("/v1/runs/{run_id}/history?limit=lots"))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_error(&v, "invalid_argument");
}

#[tokio::test]
async fn list_runs_filters_and_paging() {
    let s = Server::default().await;
    s.upload(fake_module(&[("greet", 1), ("other", 1)])).await;
    for i in 0..5 {
        let tags = if i % 2 == 0 {
            json!(["even"])
        } else {
            json!([])
        };
        s.start_run("greet", json!({ "tags": tags })).await;
    }
    s.start_run("other", json!({})).await;

    // Paging.
    let mut token: Option<String> = None;
    let mut ids = Vec::new();
    loop {
        let path = match &token {
            Some(t) => format!("/v1/runs?limit=2&page_token={t}"),
            None => "/v1/runs?limit=2".to_owned(),
        };
        let (st, v) = s.get(&path).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        for r in v["runs"].as_array().unwrap() {
            ids.push(r["run_id"].as_str().unwrap().to_owned());
        }
        match v["next_page_token"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
    }
    assert_eq!(ids.len(), 6);

    let (_, v) = s.get("/v1/runs?workflow=greet&tag=even&status=ready").await;
    assert_eq!(v["runs"].as_array().unwrap().len(), 3);
    assert_eq!(v["runs"][0]["created_at"], "2023-11-14T22:13:22Z");
    let (_, v) = s.get("/v1/runs?workflow=other").await;
    assert_eq!(v["runs"].as_array().unwrap().len(), 1);

    // created_after / created_before (RFC 3339): runs at ticks 2..=7.
    let (st, v) = s
        .get("/v1/runs?created_after=2023-11-14T22:13:24Z&created_before=2023-11-14T22:13:26Z")
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let got: Vec<_> = v["runs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["run_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(got, ["run-000003", "run-000004"]);

    // limit is capped at 1000 (the fake sees the capped value; nothing breaks).
    let (st, v) = s.get("/v1/runs?limit=100000").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(v["runs"].as_array().unwrap().len(), 6);

    let (st, v) = s.get("/v1/runs?created_after=yesterday").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_error(&v, "invalid_argument");
    let (st, _) = s.get("/v1/runs?status=sleeping").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    let (st, v) = s.get("/v1/runs?page_token=garbage").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_error(&v, "invalid_argument");
}

#[tokio::test]
async fn workflows_list_and_activate() {
    let s = Server::default().await;
    s.upload(fake_module(&[("greet", 1)])).await;
    s.upload(fake_module(&[("greet", 2)])).await;
    let (st, v) = s.get("/v1/workflows").await;
    assert_eq!(st, StatusCode::OK);
    let wf = &v["workflows"][0];
    assert_eq!(wf["name"], "greet");
    assert_eq!(wf["active_version"], 1);
    assert_eq!(wf["versions"].as_array().unwrap().len(), 2);
    let rv = wf["record_version"].as_u64().unwrap();

    let put = |body: Value, if_match: Option<String>| {
        let mut req = s.http.put(s.url("/v1/workflows/greet/active")).json(&body);
        if let Some(m) = if_match {
            req = req.header("if-match", m);
        }
        async move { decode(req.send().await.unwrap()).await }
    };

    // Stale expected_version (body) -> 412.
    let (st, v) = put(json!({ "version": 2, "expected_version": rv + 5 }), None).await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED, "{v}");
    assert_error(&v, "precondition_failed");
    // Stale If-Match -> 412.
    let (st, _) = put(json!({ "version": 2 }), Some(format!("\"{}\"", rv + 5))).await;
    assert_eq!(st, StatusCode::PRECONDITION_FAILED);
    // Disagreeing body/header -> 400.
    let (st, _) = put(
        json!({ "version": 2, "expected_version": rv }),
        Some(format!("\"{}\"", rv + 1)),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    // Matching If-Match -> 200.
    let (st, v) = put(json!({ "version": 2 }), Some(format!("W/\"{rv}\""))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["active_version"], 2);
    assert_eq!(v["record_version"], rv + 1);
    // Unknown version -> 404.
    let (st, v) = put(json!({ "version": 9 }), None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);
    assert_error(&v, "not_found");
}

#[tokio::test]
async fn health_ready_and_ui() {
    let s = Server::default().await;
    let resp = s.http.get(s.url("/healthz")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let resp = s.http.get(s.url("/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    s.cp.set_ready(false);
    let resp = s.http.get(s.url("/readyz")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    s.cp.set_ready(false);
    let resp = s.http.get(s.url("/healthz")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = s.http.get(s.url("/")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers()["content-type"].to_str().unwrap().to_owned();
    assert!(ct.starts_with("text/html"), "{ct}");
    let html = resp.text().await.unwrap();
    assert!(html.contains(env!("CARGO_PKG_VERSION")));
    assert!(html.contains("href=\"/openapi.json\""));
}

#[tokio::test]
async fn openapi_json_served_and_matches_openapi_fn() {
    let s = Server::default().await;
    let resp = s.http.get(s.url("/openapi.json")).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("application/json")
    );
    let served: Value = resp.json().await.unwrap();
    let local = serde_json::to_value(ostro_api::openapi()).unwrap();
    assert_eq!(served, local);
    assert_eq!(served["info"]["title"], "Ostro API");
}
