//! Phase 5 lifecycle REST surface (API-05/06/07) against `FakeControlPlane`: status
//! codes, error mapping (`details.reason`), size limits, `dry_run` passthrough and
//! long-poll `wait` parsing, over a real listener. gRPC Tokens is checked on the same
//! port.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_api::domain::RunStatus;
use ostro_api::fake::{FakeControlPlane, fake_module, sha256_hex};
use ostro_api::{ApiConfig, ControlPlane, ErrorCode};
use reqwest::StatusCode;
use serde_json::{Value, json};

const MAX_REQUEST: usize = 64 * 1024;

struct Server {
    addr: std::net::SocketAddr,
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
        let app = ostro_api::router(
            dyn_cp,
            ApiConfig {
                max_module_bytes: 1024 * 1024,
                max_request_bytes: MAX_REQUEST,
                ..Default::default()
            },
        );
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let srv = Server {
            addr,
            cp,
            http: reqwest::Client::builder()
                .http1_only()
                .build()
                .expect("client"),
            task,
        };
        let wasm = fake_module(&[("greet", 1)]);
        let (st, _) = decode(
            srv.http
                .put(srv.url(&format!("/v1/modules/{}", sha256_hex(&wasm))))
                .header("content-type", "application/wasm")
                .body(wasm)
                .send()
                .await
                .expect("upload"),
        )
        .await;
        assert_eq!(st, StatusCode::CREATED);
        srv
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    async fn start_run(&self) -> String {
        let (st, v) = self.post("/v1/workflows/greet/runs", json!({})).await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
        v["run_id"].as_str().expect("run_id").to_owned()
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

    async fn post_empty(&self, path: &str) -> (StatusCode, Value) {
        decode(self.http.post(self.url(path)).send().await.expect("POST")).await
    }

    async fn put(&self, path: &str, body: Value) -> (StatusCode, Value) {
        decode(
            self.http
                .put(self.url(path))
                .json(&body)
                .send()
                .await
                .expect("PUT"),
        )
        .await
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        decode(self.http.get(self.url(path)).send().await.expect("GET")).await
    }
}

async fn decode(resp: reqwest::Response) -> (StatusCode, Value) {
    let st = resp.status();
    let text = resp.text().await.expect("body");
    (
        st,
        serde_json::from_str(&text).unwrap_or(Value::String(text)),
    )
}

fn assert_error(got: &(StatusCode, Value), status: StatusCode, code: &str, reason: Option<&str>) {
    assert_eq!(got.0, status, "{}", got.1);
    assert_eq!(got.1["error"]["code"], code, "{}", got.1);
    match reason {
        Some(r) => assert_eq!(got.1["error"]["details"]["reason"], r, "{}", got.1),
        None => assert!(
            got.1["error"]["details"].get("reason").is_none(),
            "{}",
            got.1
        ),
    }
}

#[tokio::test]
async fn signal_status_codes_and_dry_run() {
    let s = Server::start().await;
    let run = s.start_run().await;
    let path = format!("/v1/runs/{run}/signals/approve");

    let t0 = s.cp.clock_ticks();
    let dry = s
        .post(&path, json!({ "payload": {"ok": 1}, "dry_run": true }))
        .await;
    assert_eq!(dry.0, StatusCode::OK);
    assert_eq!(
        dry.1,
        json!({ "accepted": true, "duplicate": false, "dry_run": true })
    );
    assert_eq!(s.cp.clock_ticks(), t0, "dry_run must not mutate");
    assert!(s.cp.signals(&run).is_empty());

    let ok = s
        .post(&path, json!({ "payload": {"ok": 1}, "request_id": "r1" }))
        .await;
    assert_eq!(
        ok,
        (
            StatusCode::OK,
            json!({ "accepted": true, "duplicate": false, "dry_run": false })
        )
    );
    let dup = s.post(&path, json!({ "request_id": "r1" })).await;
    assert_eq!(dup.1["duplicate"], true);
    assert_eq!(
        s.cp.signals(&run),
        vec![("approve".to_owned(), json!({ "ok": 1 }))]
    );

    assert_error(
        &s.post("/v1/runs/run-999999/signals/x", json!({})).await,
        StatusCode::NOT_FOUND,
        "not_found",
        None,
    );
    let long = "n".repeat(257);
    assert_error(
        &s.post(&format!("/v1/runs/{run}/signals/{long}"), json!({}))
            .await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
    assert_error(
        &s.post(&path, json!({ "request_id": "x".repeat(65) })).await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
    assert_error(
        &s.post(&path, json!({ "bogus": 1 })).await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
    // Body over max_request_bytes.
    assert_error(
        &s.post(&path, json!({ "payload": "x".repeat(MAX_REQUEST + 1) }))
            .await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        None,
    );

    s.cp.set_run_outcome(&run, RunStatus::Completed, None, None, None)
        .expect("complete");
    let e = s.post(&path, json!({})).await;
    assert_error(&e, StatusCode::CONFLICT, "conflict", Some("run_terminal"));
    assert_eq!(e.1["error"]["details"]["status"], "completed");
}

#[tokio::test]
async fn cancel_and_terminate() {
    let s = Server::start().await;
    let run = s.start_run().await;
    let cancel = format!("/v1/runs/{run}/cancel");
    let terminate = format!("/v1/runs/{run}/terminate");

    let t0 = s.cp.clock_ticks();
    let dry = s.post(&cancel, json!({ "dry_run": true })).await;
    assert_eq!(
        dry,
        (
            StatusCode::OK,
            json!({ "already_requested": false, "dry_run": true })
        )
    );
    assert_eq!(s.cp.clock_ticks(), t0);
    // Empty body = no reason, real request.
    let first = s.post_empty(&cancel).await;
    assert_eq!(
        first,
        (
            StatusCode::OK,
            json!({ "already_requested": false, "dry_run": false })
        )
    );
    let again = s.post(&cancel, json!({ "reason": "again" })).await;
    assert_eq!(again.1["already_requested"], true);
    assert_eq!(s.cp.cancel_requested(&run), Some(None));
    assert_error(
        &s.post(&cancel, json!({ "reason": "r".repeat(1025) })).await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
    assert_error(
        &s.post_empty("/v1/runs/run-999999/cancel").await,
        StatusCode::NOT_FOUND,
        "not_found",
        None,
    );

    let t1 = s.cp.clock_ticks();
    let dry = s.post(&terminate, json!({ "dry_run": true })).await;
    assert_eq!(
        dry,
        (
            StatusCode::OK,
            json!({ "status": "terminated", "dry_run": true })
        )
    );
    assert_eq!(s.cp.clock_ticks(), t1);
    assert_eq!(s.get(&format!("/v1/runs/{run}")).await.1["status"], "ready");
    let done = s.post(&terminate, json!({ "reason": "ops" })).await;
    assert_eq!(
        done,
        (
            StatusCode::OK,
            json!({ "status": "terminated", "dry_run": false })
        )
    );
    assert_eq!(
        s.get(&format!("/v1/runs/{run}")).await.1["status"],
        "terminated"
    );
    assert_error(
        &s.post_empty(&terminate).await,
        StatusCode::CONFLICT,
        "conflict",
        Some("run_terminal"),
    );
    assert_error(
        &s.post_empty(&cancel).await,
        StatusCode::CONFLICT,
        "conflict",
        Some("run_terminal"),
    );
}

#[tokio::test]
async fn retry_modes() {
    let s = Server::start().await;
    let run = s.start_run().await;
    let retry = format!("/v1/runs/{run}/retry");
    assert_error(
        &s.post_empty(&retry).await,
        StatusCode::CONFLICT,
        "conflict",
        Some("run_not_terminal"),
    );
    s.post_empty(&format!("/v1/runs/{run}/terminate")).await;

    let t0 = s.cp.clock_ticks();
    let dry = s.post(&retry, json!({ "dry_run": true })).await;
    assert_eq!(
        dry,
        (
            StatusCode::OK,
            json!({ "mode": "resumed", "run_id": run, "dry_run": true })
        )
    );
    assert_eq!(s.cp.clock_ticks(), t0);
    let resumed = s.post_empty(&retry).await;
    assert_eq!(
        resumed.1,
        json!({ "mode": "resumed", "run_id": run, "dry_run": false })
    );
    assert_eq!(s.get(&format!("/v1/runs/{run}")).await.1["status"], "ready");

    s.cp.set_run_outcome(&run, RunStatus::Failed, None, Some("boom".into()), None)
        .expect("fail");
    let restarted = s.post_empty(&retry).await;
    assert_eq!(restarted.0, StatusCode::OK);
    assert_eq!(restarted.1["mode"], "restarted");
    let new_id = restarted.1["run_id"].as_str().expect("id").to_owned();
    assert_ne!(new_id, run);
    assert_eq!(
        s.get(&format!("/v1/runs/{new_id}")).await.1["status"],
        "ready"
    );

    s.cp.set_run_outcome(&new_id, RunStatus::Completed, None, None, None)
        .expect("complete");
    assert_error(
        &s.post_empty(&format!("/v1/runs/{new_id}/retry")).await,
        StatusCode::CONFLICT,
        "conflict",
        Some("run_completed"),
    );
    assert_error(
        &s.post(&retry, json!({ "nope": true })).await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
}

#[tokio::test]
async fn result_long_poll_and_wait_parsing() {
    let s = Arc::new(Server::start().await);
    let run = s.start_run().await;
    let path = format!("/v1/runs/{run}/result");

    let now = s.get(&path).await;
    assert_eq!(
        now,
        (
            StatusCode::OK,
            json!({ "run_id": run, "status": "ready", "completed": false })
        )
    );

    // Not terminal within the wait: 200, completed = false, after about the wait.
    let t = Instant::now();
    let out = s.get(&format!("{path}?wait=150ms")).await;
    assert_eq!(out.0, StatusCode::OK);
    assert_eq!(out.1["completed"], false);
    assert!(
        t.elapsed() >= Duration::from_millis(140),
        "{:?}",
        t.elapsed()
    );

    for bad in ["7d", "abc", "-1s", "30"] {
        assert_error(
            &s.get(&format!("{path}?wait={bad}")).await,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            None,
        );
    }
    assert_error(
        &s.get(&format!("{path}?follow_continuations=maybe")).await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
    assert_error(
        &s.get("/v1/runs/run-999999/result").await,
        StatusCode::NOT_FOUND,
        "not_found",
        None,
    );

    // Completes while a 30s wait is pending: answered promptly with the result.
    let s2 = s.clone();
    let run2 = run.clone();
    let waker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        s2.cp
            .set_run_outcome(
                &run2,
                RunStatus::Completed,
                Some(br#"{"sum":3}"#.to_vec()),
                None,
                None,
            )
            .expect("complete");
    });
    let t = Instant::now();
    let done = s.get(&format!("{path}?wait=30s")).await;
    waker.await.expect("waker");
    assert!(t.elapsed() < Duration::from_secs(10), "{:?}", t.elapsed());
    assert_eq!(
        done,
        (
            StatusCode::OK,
            json!({ "run_id": run, "status": "completed", "completed": true, "result": {"sum": 3} })
        )
    );

    // A wait above the cap is accepted (capped, not rejected).
    let capped = s.get(&format!("{path}?wait=168h")).await;
    assert_eq!(capped.1["completed"], true);

    // follow_continuations walks the chain to the last run.
    let a = s.start_run().await;
    let b = s.cp.continue_as_new(&a).expect("can");
    s.cp.set_run_outcome(&b, RunStatus::Failed, None, Some("bad".into()), None)
        .expect("fail");
    let own = s.get(&format!("/v1/runs/{a}/result")).await;
    assert_eq!(own.1["status"], "continued_as_new");
    let last = s
        .get(&format!("/v1/runs/{a}/result?follow_continuations=true"))
        .await;
    assert_eq!(
        last.1,
        json!({ "run_id": b, "status": "failed", "completed": true, "failure": "bad" })
    );
}

#[tokio::test]
async fn token_completion() {
    let s = Server::start().await;
    let run = s.start_run().await;
    let token = s.cp.mint_token(&run, 3);

    let t0 = s.cp.clock_ticks();
    let dry = s
        .post(
            "/v1/tokens/complete",
            json!({ "token": token, "payload": 1, "dry_run": true }),
        )
        .await;
    assert_eq!(
        dry,
        (
            StatusCode::OK,
            json!({ "run_id": run, "accepted": true, "dry_run": true })
        )
    );
    assert_eq!(s.cp.clock_ticks(), t0);
    assert_eq!(s.cp.token_completed(&run, 3), Some(false));

    let ok = s
        .post(
            "/v1/tokens/complete",
            json!({ "token": token, "payload": {"approved": true} }),
        )
        .await;
    assert_eq!(
        ok,
        (
            StatusCode::OK,
            json!({ "run_id": run, "accepted": true, "dry_run": false })
        )
    );
    assert_error(
        &s.post("/v1/tokens/complete", json!({ "token": token }))
            .await,
        StatusCode::CONFLICT,
        "conflict",
        Some("token_already_completed"),
    );
    let bad = s
        .post("/v1/tokens/complete", json!({ "token": "ot1_garbage" }))
        .await;
    assert_error(
        &bad,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        Some("token_invalid"),
    );
    assert!(!bad.1.to_string().contains("ot1_garbage"), "token echoed");
    // Token is required in the body.
    assert_error(
        &s.post("/v1/tokens/complete", json!({ "payload": 1 })).await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );
    assert_error(
        &s.post(
            "/v1/tokens/complete",
            json!({ "token": token, "payload": "x".repeat(MAX_REQUEST + 1) }),
        )
        .await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
        None,
    );
    // There is no URL form of token completion: a token in the path or the query
    // string completes nothing (unknown paths reach the gRPC fallback; the query form
    // has no body and is a 400).
    let t2 = s.cp.mint_token(&run, 8);
    for path in [
        format!("/v1/tokens/{t2}"),
        format!("/v1/tokens/complete?token={t2}"),
    ] {
        let (st, body) = decode(s.http.post(s.url(&path)).send().await.expect("POST")).await;
        assert!(body.get("accepted").is_none(), "{path}: {st} {body}");
    }
    assert_eq!(s.cp.token_completed(&run, 8), Some(false));
}

#[tokio::test]
async fn policies() {
    let s = Server::start().await;
    let empty = json!({
        "retention": null, "retention_ms": null,
        "schedule_to_start_timeout": null, "schedule_to_start_ms": null,
    });
    assert_eq!(
        s.get("/v1/workflows/greet/policy").await,
        (StatusCode::OK, empty.clone())
    );
    let set = s
        .put(
            "/v1/workflows/greet/policy",
            json!({ "retention": "168h", "schedule_to_start_timeout": "30s" }),
        )
        .await;
    let want = json!({
        "retention": "168h", "retention_ms": 604_800_000u64,
        "schedule_to_start_timeout": "30s", "schedule_to_start_ms": 30_000,
    });
    assert_eq!(set, (StatusCode::OK, want.clone()));
    assert_eq!(
        s.get("/v1/workflows/greet/policy").await,
        (StatusCode::OK, want)
    );
    assert_error(
        &s.put("/v1/workflows/nope/policy", json!({})).await,
        StatusCode::NOT_FOUND,
        "not_found",
        None,
    );
    assert_error(
        &s.get("/v1/workflows/nope/policy").await,
        StatusCode::NOT_FOUND,
        "not_found",
        None,
    );
    let e = s
        .put("/v1/workflows/greet/policy", json!({ "retention": "7d" }))
        .await;
    assert_error(&e, StatusCode::BAD_REQUEST, "invalid_argument", None);
    assert_eq!(e.1["error"]["details"]["field"], "retention");
    assert_error(
        &s.put("/v1/workflows/greet/policy", json!({ "ttl": "1h" }))
            .await,
        StatusCode::BAD_REQUEST,
        "invalid_argument",
        None,
    );

    let q = s
        .put(
            "/v1/queues/batch/policy",
            json!({ "schedule_to_start_timeout": "1m 30s" }),
        )
        .await;
    assert_eq!(q.0, StatusCode::OK);
    assert_eq!(q.1["schedule_to_start_ms"], 90_000);
    assert_eq!(q.1["schedule_to_start_timeout"], "1m 30s");
    assert_eq!(q.1["retention"], Value::Null);
    assert_eq!(s.get("/v1/queues/batch/policy").await.1, q.1);
    // PUT replaces the whole policy.
    assert_eq!(
        s.put("/v1/queues/batch/policy", json!({})).await,
        (StatusCode::OK, empty.clone())
    );
    assert_eq!(
        s.get("/v1/queues/other/policy").await,
        (StatusCode::OK, empty)
    );
}

#[tokio::test]
async fn grpc_tokens_service_is_mounted() {
    use ostro_proto::api::v1 as pb;
    use ostro_proto::api::v1::tokens_client::TokensClient;

    let s = Server::start().await;
    let run = s.start_run().await;
    let token = s.cp.mint_token(&run, 1);
    let mut client = TokensClient::connect(format!("http://{}", s.addr))
        .await
        .expect("connect");
    let r = client
        .complete(pb::CompleteTokenRequest {
            token: token.clone(),
            payload_json: b"true".to_vec(),
            dry_run: false,
        })
        .await
        .expect("complete")
        .into_inner();
    assert_eq!((r.run_id.as_str(), r.accepted), (run.as_str(), true));
    let e = client
        .complete(pb::CompleteTokenRequest {
            token,
            payload_json: Vec::new(),
            dry_run: false,
        })
        .await
        .expect_err("second completion");
    assert_eq!(
        ErrorCode::from_grpc_code(e.code()),
        ErrorCode::Conflict,
        "{e:?}"
    );
    // Decode limit = max_request_bytes, answered RESOURCE_EXHAUSTED.
    let e = client
        .complete(pb::CompleteTokenRequest {
            token: "ot1_x".into(),
            payload_json: vec![b' '; MAX_REQUEST + 1],
            dry_run: false,
        })
        .await
        .expect_err("over the limit");
    assert_eq!(e.code(), tonic::Code::ResourceExhausted);
}

#[tokio::test]
async fn unimplemented_control_plane_answers_501() {
    // A control plane that only has the Phase 4 methods (ostro-server before 05-13).
    struct Phase4(FakeControlPlane);

    #[async_trait::async_trait]
    impl ControlPlane for Phase4 {
        async fn upload_module(
            &self,
            r: ostro_api::domain::UploadModule,
        ) -> Result<ostro_api::domain::UploadModuleResult, ostro_api::ApiError> {
            self.0.upload_module(r).await
        }
        async fn get_module(
            &self,
            s: String,
        ) -> Result<ostro_api::domain::ModuleInfo, ostro_api::ApiError> {
            self.0.get_module(s).await
        }
        async fn list_workflows(
            &self,
        ) -> Result<Vec<ostro_api::domain::WorkflowInfo>, ostro_api::ApiError> {
            self.0.list_workflows().await
        }
        async fn activate_workflow(
            &self,
            r: ostro_api::domain::ActivateWorkflow,
        ) -> Result<ostro_api::domain::WorkflowInfo, ostro_api::ApiError> {
            self.0.activate_workflow(r).await
        }
        async fn start_run(
            &self,
            r: ostro_api::domain::StartRunRequest,
        ) -> Result<ostro_api::domain::StartRunResult, ostro_api::ApiError> {
            self.0.start_run(r).await
        }
        async fn get_run(
            &self,
            id: String,
        ) -> Result<ostro_api::domain::RunInfo, ostro_api::ApiError> {
            self.0.get_run(id).await
        }
        async fn run_history(
            &self,
            r: ostro_api::domain::HistoryRequest,
        ) -> Result<ostro_api::domain::HistoryPage, ostro_api::ApiError> {
            self.0.run_history(r).await
        }
        async fn list_runs(
            &self,
            r: ostro_api::domain::ListRunsRequest,
        ) -> Result<ostro_api::domain::RunPage, ostro_api::ApiError> {
            self.0.list_runs(r).await
        }
        fn ready(&self) -> bool {
            true
        }
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let cp: Arc<dyn ControlPlane> = Arc::new(Phase4(FakeControlPlane::new()));
    let app = ostro_api::router(cp, ApiConfig::default());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    let http = reqwest::Client::new();
    let resp = http
        .post(format!("http://{addr}/v1/runs/r/cancel"))
        .send()
        .await
        .expect("POST");
    let got = decode(resp).await;
    assert_error(&got, StatusCode::NOT_IMPLEMENTED, "unimplemented", None);
    assert_eq!(got.1["error"]["details"]["method"], "cancel_run");
    let got = decode(
        http.get(format!("http://{addr}/v1/queues/q/policy"))
            .send()
            .await
            .expect("GET"),
    )
    .await;
    assert_eq!(got.0, StatusCode::NOT_IMPLEMENTED);
    task.abort();
}
