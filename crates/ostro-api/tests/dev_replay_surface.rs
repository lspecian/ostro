//! Phase 10 (10-05) REST contract of the REST-only tooling endpoints (D-P10-18) against
//! `FakeControlPlane`, over a real HTTP listener: `GET /v1/dev/time`,
//! `POST /v1/dev/time/skip` and `POST /v1/modules/{sha}/replay`.

use std::sync::Arc;

use ostro_api::domain::DevTimeMode;
use ostro_api::fake::{FAKE_EPOCH_MS, FakeControlPlane, fake_module, sha256_hex};
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
    async fn start() -> Server {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let cp = Arc::new(FakeControlPlane::new());
        let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
        let app = ostro_api::router(dyn_cp, ApiConfig::default());
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

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        decode(self.http.get(self.url(path)).send().await.expect("GET")).await
    }

    async fn post(&self, path: &str, body: Value) -> (StatusCode, Value) {
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

    /// Upload a fake module; returns its sha.
    async fn upload(&self, wasm: bytes::Bytes) -> String {
        let sha = sha256_hex(&wasm);
        let resp = self
            .http
            .put(self.url(&format!("/v1/modules/{sha}")))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(resp.status().is_success(), "{}", resp.status());
        sha
    }

    async fn start_run(&self, wf: &str) -> String {
        let (st, v) = self
            .post(&format!("/v1/workflows/{wf}/runs"), json!({}))
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
        v["run_id"].as_str().expect("run_id").to_owned()
    }
}

async fn decode(resp: reqwest::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let text = resp.text().await.expect("body");
    let v = serde_json::from_str(&text).unwrap_or(Value::String(text));
    (status, v)
}

fn assert_error(v: &Value, code: &str, reason: Option<&str>) {
    assert_eq!(v["error"]["code"], code, "body: {v}");
    assert!(v["error"]["message"].is_string(), "body: {v}");
    match reason {
        Some(r) => assert_eq!(v["error"]["details"]["reason"], r, "body: {v}"),
        None => assert!(v["error"]["details"]["reason"].is_null(), "body: {v}"),
    }
}

#[tokio::test]
async fn dev_time_and_skip() {
    let s = Server::start().await;
    let (st, v) = s.get("/v1/dev/time").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        v,
        json!({"now_ms": FAKE_EPOCH_MS, "offset_ms": 0, "mode": "auto", "idle": true,
               "next_due_ms": null})
    );

    let (st, v) = s.post("/v1/dev/time/skip", json!({"by": "72h"})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["skipped_ms"], 259_200_000u64);
    assert_eq!(v["offset_ms"], 259_200_000u64);
    assert_eq!(v["now_ms"], FAKE_EPOCH_MS + 259_200_000);
    let (_, v) = s.get("/v1/dev/time").await;
    assert_eq!(v["offset_ms"], 259_200_000u64, "{v}");
    assert_eq!(v["now_ms"], FAKE_EPOCH_MS + 259_200_000);

    // A target in the past skips nothing: time never goes back.
    let (st, v) = s
        .post("/v1/dev/time/skip", json!({"to_ms": FAKE_EPOCH_MS}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["skipped_ms"], 0);
    assert_eq!(v["offset_ms"], 259_200_000u64);

    // An absolute target ahead skips exactly the gap.
    let target = FAKE_EPOCH_MS + 259_200_000 + 1_000;
    let (st, v) = s.post("/v1/dev/time/skip", json!({"to_ms": target})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["skipped_ms"], 1_000);
    assert_eq!(v["now_ms"], target);
}

#[tokio::test]
async fn dev_skip_validation() {
    let s = Server::start().await;
    for body in [
        json!({}),
        json!({"by": "1h", "to_ms": 1}),
        json!({"by": "soon"}),
        json!({"extra": 1}),
    ] {
        let (st, v) = s.post("/v1/dev/time/skip", body.clone()).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{body}: {v}");
        assert_error(&v, "invalid_argument", None);
    }
    // An empty body is "neither".
    let resp = s
        .http
        .post(s.url("/v1/dev/time/skip"))
        .send()
        .await
        .expect("POST");
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let (st, v) = s.post("/v1/dev/time/skip", json!({"by": "7d"})).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let msg = v["error"]["message"].as_str().expect("message");
    assert!(msg.contains("write it as 168h"), "{msg}");
    let (_, v) = s.get("/v1/dev/time").await;
    assert_eq!(v["offset_ms"], 0, "rejected skips moved time: {v}");
}

#[tokio::test]
async fn dev_routes_answer_409_when_off() {
    let s = Server::start().await;
    s.cp.set_dev_mode(DevTimeMode::Off);
    let (st, v) = s.get("/v1/dev/time").await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_error(&v, "conflict", Some("time_skipping_disabled"));
    let (st, v) = s.post("/v1/dev/time/skip", json!({"by": "1h"})).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_error(&v, "conflict", Some("time_skipping_disabled"));

    s.cp.set_dev_mode(DevTimeMode::Manual);
    let (st, v) = s.get("/v1/dev/time").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "manual");
}

#[tokio::test]
async fn replay_known_runs_and_sample() {
    let s = Server::start().await;
    let sha = s.upload(fake_module(&[("greet", 1), ("bye", 1)])).await;
    let r1 = s.start_run("greet").await;
    let r2 = s.start_run("greet").await;
    let r3 = s.start_run("bye").await;
    let path = format!("/v1/modules/{sha}/replay");

    let (st, v) = s.post(&path, json!({"runs": [r1]})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["module_sha256"], sha);
    assert_eq!(
        v["verdicts"],
        json!([{"run_id": r1, "workflow": "greet", "verdict": "ok", "appended": 0}])
    );
    assert_eq!(
        v["summary"],
        json!({"ok": 1, "ok_frontier": 0, "diverged": 0, "skipped": 0, "error": 0})
    );

    // Newest first per workflow; one run each.
    let (st, v) = s
        .post(&path, json!({"sample": 1, "allow_missing": true}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let ids: Vec<&str> = v["verdicts"]
        .as_array()
        .expect("verdicts")
        .iter()
        .map(|x| x["run_id"].as_str().expect("id"))
        .collect();
    assert_eq!(ids, [r3.as_str(), r2.as_str()], "{v}");
    assert_eq!(v["summary"]["ok"], 2);

    let (st, v) = s
        .post(&path, json!({"sample": 10, "workflow": "greet"}))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["summary"]["ok"], 2, "{v}");

    // A run of a workflow the module does not export is skipped.
    let other = s.upload(fake_module(&[("other", 1)])).await;
    let (st, v) = s
        .post(
            &format!("/v1/modules/{other}/replay"),
            json!({"runs": [r1]}),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["verdicts"][0]["verdict"], "skipped", "{v}");
    assert!(v["verdicts"][0]["detail"].is_string(), "{v}");
    assert_eq!(v["summary"]["skipped"], 1);
}

#[tokio::test]
async fn replay_errors() {
    let s = Server::start().await;
    let sha = s.upload(fake_module(&[("greet", 1)])).await;
    let run = s.start_run("greet").await;
    let path = format!("/v1/modules/{sha}/replay");

    // Unknown module / run -> 404.
    let (st, v) = s
        .post(
            &format!("/v1/modules/{}/replay", "0".repeat(64)),
            json!({"runs": [run]}),
        )
        .await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");
    assert_error(&v, "not_found", None);
    let (st, v) = s.post(&path, json!({"runs": ["run-999999"]})).await;
    assert_eq!(st, StatusCode::NOT_FOUND, "{v}");

    // Malformed sha -> 400.
    for bad in ["xyz", &"A".repeat(64)] {
        let (st, v) = s
            .post(&format!("/v1/modules/{bad}/replay"), json!({"runs": [run]}))
            .await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{bad}: {v}");
        assert_error(&v, "invalid_argument", None);
    }

    for n in [1001u64, 5_000_000_000] {
        let (st, v) = s.post(&path, json!({"sample": n})).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
        assert_error(&v, "invalid_argument", Some("sample_too_large"));
        assert_eq!(v["error"]["details"]["limit"], 1000);
    }

    for body in [
        json!({"runs": [run], "sample": 1}),
        json!({}),
        json!({"allow_missing": true}),
        json!({"sample": 0}),
        json!({"runs": []}),
        json!({"runs": ["bad id"]}),
        json!({"runs": [run], "workflow": "greet"}),
        json!({"sample": 1, "status": "nope"}),
        json!({"sample": 1, "unknown": true}),
    ] {
        let (st, v) = s.post(&path, body.clone()).await;
        assert_eq!(st, StatusCode::BAD_REQUEST, "{body}: {v}");
        assert_error(&v, "invalid_argument", None);
    }
}

#[test]
fn routes_are_documented_rest_only() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    for (path, method, tag) in [
        ("/v1/dev/time", "get", "dev"),
        ("/v1/dev/time/skip", "post", "dev"),
        ("/v1/modules/{sha}/replay", "post", "tooling"),
    ] {
        let op = &doc["paths"][path][method];
        assert!(op.is_object(), "{method} {path} missing from OpenAPI");
        assert_eq!(op["tags"], json!([tag]), "{method} {path}");
        let desc = op["description"].as_str().unwrap_or_default();
        assert!(
            desc.contains("REST-only tooling endpoint (no gRPC)"),
            "{desc}"
        );
        if tag == "dev" {
            assert!(
                desc.contains("only with `ostro dev --ephemeral --time-skipping`"),
                "{desc}"
            );
        }
    }
    let tags: Vec<&str> = doc["tags"]
        .as_array()
        .expect("tags")
        .iter()
        .filter_map(|t| t["name"].as_str())
        .collect();
    assert!(
        tags.contains(&"dev") && tags.contains(&"tooling"),
        "{tags:?}"
    );
}
