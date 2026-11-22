//! Phase 11 REST surface (11-09) against `FakeOps` / `FakeInventory` over a real
//! listener: every route, 404 while a slot is absent, the search parameters of
//! `GET /v1/runs`, the history `attempt`, the API-side payload / tag caps with their
//! uniform details, failure policies, and the `/` redirect.

use std::sync::Arc;

use ostro_api::domain::ListOrder;
use ostro_api::fake::{
    FAKE_MAX_BULK_JOBS, FakeControlPlane, FakeInventory, FakeOps, fake_module, sha256_hex,
};
use ostro_api::phase11::{
    DlqEntry, ModuleSummary, PartitionView, QueueView, RerunCut, RerunCutsView, rerun_cut_unsafe,
};
use ostro_api::rest::RunStatusDto;
use ostro_api::{ApiConfig, ApiExtensions, ControlPlane};
use reqwest::StatusCode;
use serde_json::{Value, json};

struct Server {
    addr: std::net::SocketAddr,
    cp: Arc<FakeControlPlane>,
    ops: Arc<FakeOps>,
    inv: Arc<FakeInventory>,
    http: reqwest::Client,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Server {
    async fn start(with_slots: bool, cfg: ApiConfig) -> Server {
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
        let dyn_cp: Arc<dyn ControlPlane> = cp.clone();
        let app = ostro_api::router_with(dyn_cp, cfg, ext);
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let srv = Server {
            addr,
            cp,
            ops,
            inv,
            http: reqwest::Client::builder()
                .http1_only()
                .redirect(reqwest::redirect::Policy::none())
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

    async fn with_slots() -> Server {
        Server::start(true, ApiConfig::default()).await
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

    async fn start_run(&self, body: Value) -> (StatusCode, Value) {
        self.post("/v1/workflows/greet/runs", body).await
    }
}

fn code(v: &Value) -> &str {
    v["error"]["code"].as_str().unwrap_or("<none>")
}

fn details(v: &Value) -> &Value {
    &v["error"]["details"]
}

fn dlq_entry(i: usize) -> DlqEntry {
    DlqEntry {
        run_id: format!("run-{i:06}"),
        workflow: if i.is_multiple_of(2) { "even" } else { "odd" }.to_owned(),
        version: 1,
        queue: "default".to_owned(),
        status: RunStatusDto::Failed,
        reason: format!("boom {i}"),
        failed_at_ms: 1_000 + i as u64,
        tags: vec![format!("t{}", i % 3)],
        resumable: i.is_multiple_of(5),
        hook_run_id: None,
        dismissed_at_ms: None,
        dismiss_reason: None,
        requeued_to: None,
        requeued_at_ms: None,
    }
}

// ---------------------------------------------------------------------------------
// Absent slots
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn every_phase11_route_is_404_when_its_slot_is_absent() {
    let s = Server::start(false, ApiConfig::default()).await;
    let get_paths = [
        "/v1/runs/run-1/rerun-cuts",
        "/v1/dlq",
        "/v1/bulk",
        "/v1/bulk/blk_1",
        "/v1/limits",
        "/v1/workflows/greet/failure-policy",
        "/v1/queues/default/failure-policy",
        "/v1/modules",
        "/v1/queues",
        "/v1/partitions",
    ];
    for p in get_paths {
        let (st, v) = s.get(p).await;
        assert_eq!(
            (st, code(&v)),
            (StatusCode::NOT_FOUND, "not_found"),
            "GET {p}: {v}"
        );
    }
    for (p, body) in [
        ("/v1/runs/run-1/rerun", json!({ "from": { "seq": 3 } })),
        ("/v1/dlq/run-1/requeue", json!({})),
        ("/v1/dlq/run-1/dismiss", json!({ "reason": "x" })),
        ("/v1/bulk", json!({ "action": "cancel" })),
        ("/v1/bulk/blk_1/cancel", json!({})),
    ] {
        let (st, v) = s.post(p, body).await;
        assert_eq!(
            (st, code(&v)),
            (StatusCode::NOT_FOUND, "not_found"),
            "POST {p}: {v}"
        );
    }
    for p in [
        "/v1/workflows/greet/failure-policy",
        "/v1/queues/default/failure-policy",
    ] {
        let (st, v) = s.put(p, json!({})).await;
        assert_eq!(
            (st, code(&v)),
            (StatusCode::NOT_FOUND, "not_found"),
            "PUT {p}: {v}"
        );
    }
    // The existing module route is unaffected.
    let wasm = fake_module(&[("greet", 1)]);
    let (st, v) = s.get(&format!("/v1/modules/{}", sha256_hex(&wasm))).await;
    assert_eq!(st, StatusCode::OK, "{v}");
}

#[tokio::test]
async fn ops_and_inventory_slots_are_independent() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let cp: Arc<dyn ControlPlane> = Arc::new(FakeControlPlane::new());
    let inv = FakeInventory::new();
    let mut ext = ApiExtensions::default();
    inv.install(&mut ext);
    let app = ostro_api::router_with(cp, ApiConfig::default(), ext);
    let task = tokio::spawn(async move { axum::serve(listener, app).await.expect("serve") });
    let http = reqwest::Client::new();
    let st = |p: &str| {
        let http = http.clone();
        let url = format!("http://{addr}{p}");
        async move { http.get(url).send().await.expect("get").status() }
    };
    assert_eq!(st("/v1/queues").await, StatusCode::OK);
    assert_eq!(st("/v1/partitions").await, StatusCode::OK);
    assert_eq!(st("/v1/limits").await, StatusCode::NOT_FOUND);
    assert_eq!(st("/v1/dlq").await, StatusCode::NOT_FOUND);
    task.abort();
}

// ---------------------------------------------------------------------------------
// Rerun
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn rerun_dry_run_real_idempotent_and_unsafe_cut() {
    let s = Server::with_slots().await;
    let (st, v) = s
        .post(
            "/v1/runs/run-000001/rerun",
            json!({ "from": { "seq": 5 }, "dry_run": true }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(v.get("new_run_id").is_none(), "{v}");
    assert_eq!(v["cut_seq"], 5);
    assert_eq!(v["source_run_id"], "run-000001");

    let body = json!({ "from": { "step": { "name": "charge" } }, "version": "source", "idempotency_key": "k1" });
    let (st, v) = s.post("/v1/runs/run-000001/rerun", body.clone()).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let new_id = v["new_run_id"].as_str().expect("new run").to_owned();
    assert_eq!(v["existing"], false);
    let (st, v) = s.post("/v1/runs/run-000001/rerun", body).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["existing"], true);
    assert_eq!(v["new_run_id"], new_id.as_str());
    let reqs = s.ops.reruns();
    assert_eq!(reqs.len(), 3);
    assert_eq!(reqs[1].run_id, "run-000001");

    // Validation before the trait: occurrence is 1-based.
    let (st, v) = s
        .post(
            "/v1/runs/run-000001/rerun",
            json!({ "from": { "step": { "name": "x", "occurrence": 0 } } }),
        )
        .await;
    assert_eq!(
        (st, code(&v)),
        (StatusCode::BAD_REQUEST, "invalid_argument")
    );

    s.ops.fail(
        "rerun_run",
        Some(rerun_cut_unsafe(7, "inside a step", Some(4))),
    );
    let (st, v) = s
        .post("/v1/runs/run-000001/rerun", json!({ "from": { "seq": 7 } }))
        .await;
    assert_eq!((st, code(&v)), (StatusCode::CONFLICT, "conflict"), "{v}");
    assert_eq!(details(&v)["reason"], "rerun_cut_unsafe");
    assert_eq!(details(&v)["seq"], 7);
    assert_eq!(details(&v)["nearest_safe_seq"], 4);
}

#[tokio::test]
async fn rerun_cuts_view() {
    let s = Server::with_slots().await;
    s.ops.set_cuts(RerunCutsView {
        run_id: "run-000009".into(),
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
                seq: 4,
                step: Some("charge".into()),
                occurrence: 1,
                safe: false,
                reason: Some("open await".into()),
                nearest_safe_seq: Some(1),
            },
        ],
    });
    let (st, v) = s.get("/v1/runs/run-000009/rerun-cuts").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["cuts"].as_array().expect("cuts").len(), 2);
    assert_eq!(v["cuts"][1]["safe"], false);
    assert_eq!(v["cuts"][1]["nearest_safe_seq"], 1);
    let (st, _) = s.get("/v1/runs/run-unknown/rerun-cuts").await;
    assert_eq!(st, StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------------
// DLQ
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn dlq_paginates_dismisses_and_requeues() {
    let s = Server::with_slots().await;
    for i in 1..=120 {
        s.ops.push_dlq(dlq_entry(i));
    }
    let mut seen = Vec::new();
    let mut token: Option<String> = None;
    let mut pages = 0;
    loop {
        let q = token
            .as_deref()
            .map(|t| format!("&page_token={t}"))
            .unwrap_or_default();
        let (st, v) = s.get(&format!("/v1/dlq?limit=50{q}")).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        pages += 1;
        for e in v["entries"].as_array().expect("entries") {
            seen.push(e["run_id"].as_str().expect("id").to_owned());
        }
        match v["next_page_token"].as_str() {
            Some(t) => token = Some(t.to_owned()),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(seen.len(), 120);

    // Filters: workflow, repeatable tags.
    let (_, v) = s.get("/v1/dlq?workflow=even&limit=1000").await;
    assert_eq!(v["entries"].as_array().expect("entries").len(), 60);
    let (_, v) = s.get("/v1/dlq?tags=t0&tags=t1&limit=1000").await;
    assert_eq!(v["entries"].as_array().expect("entries").len(), 0);
    let (_, v) = s.get("/v1/dlq?tags=t0&limit=1000").await;
    assert_eq!(v["entries"].as_array().expect("entries").len(), 40);
    let nine: String = (0..9).map(|i| format!("tags=x{i}&")).collect();
    let (st, v) = s.get(&format!("/v1/dlq?{nine}")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(details(&v)["limit"], "search_tags");

    // Dismiss hides the entry unless include_dismissed.
    let (st, v) = s
        .post(
            "/v1/dlq/run-000002/dismiss",
            json!({ "reason": "known flake" }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["dismiss_reason"], "known flake");
    assert!(v["dismissed_at_ms"].as_u64().is_some());
    let (_, v) = s.get("/v1/dlq?limit=1000").await;
    assert_eq!(v["entries"].as_array().expect("entries").len(), 119);
    let (_, v) = s.get("/v1/dlq?limit=1000&include_dismissed=true").await;
    assert_eq!(v["entries"].as_array().expect("entries").len(), 120);
    let (st, v) = s
        .post("/v1/dlq/run-000002/dismiss", json!({ "reason": "again" }))
        .await;
    assert_eq!((st, code(&v)), (StatusCode::CONFLICT, "conflict"));
    assert_eq!(details(&v)["reason"], "dlq_not_member");
    let (st, v) = s
        .post(
            "/v1/dlq/run-000003/dismiss",
            json!({ "reason": "x".repeat(1025) }),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(details(&v)["limit"], "reason_bytes");

    // Requeue: empty body = auto; run 5 is resumable, run 7 is not.
    let r = s
        .send(s.http.post(s.url("/v1/dlq/run-000005/requeue")))
        .await;
    assert_eq!(r.0, StatusCode::OK, "{}", r.1);
    assert_eq!(r.1["mode"], "resume");
    let (st, v) = s
        .post(
            "/v1/dlq/run-000007/requeue",
            json!({ "mode": "auto", "dry_run": true }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "restart");
    assert!(v.get("new_run_id").is_none());
    let (st, v) = s
        .post(
            "/v1/dlq/run-000007/requeue",
            json!({ "mode": { "rerun": { "from": { "seq": 2 } } } }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["mode"], "rerun");
    assert!(v["new_run_id"].as_str().is_some());
    let e = s.ops.dlq_entry("run-000007").expect("entry");
    assert_eq!(e.requeued_to.as_deref(), v["new_run_id"].as_str());
}

// ---------------------------------------------------------------------------------
// Bulk
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn bulk_create_progress_cancel_dry_run_and_job_cap() {
    let s = Server::with_slots().await;
    s.ops
        .set_bulk_matches((1..=30).map(|i| format!("run-{i:06}")).collect());
    let (st, v) = s
        .post(
            "/v1/bulk",
            json!({ "action": { "terminate": { "reason": "cleanup" } }, "filter": { "workflow": "greet", "tags": ["a"] }, "dry_run": true }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(v.get("job_id").is_none());
    assert_eq!(v["matched_estimate"], 30);
    assert_eq!(v["sample"].as_array().expect("sample").len(), 20);

    let (st, v) = s
        .post(
            "/v1/bulk",
            json!({ "action": "cancel", "rate_per_sec": 50 }),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let job = v["job_id"].as_str().expect("job").to_owned();
    assert!(job.starts_with("blk_"), "{job}");
    s.ops.set_bulk_progress(&job, 10, 2);
    let (st, v) = s.get(&format!("/v1/bulk/{job}")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(
        (v["done"].as_u64(), v["skipped"].as_u64()),
        (Some(10), Some(2))
    );
    assert_eq!(v["state"], "running");
    assert_eq!(v["action"], "cancel");
    assert_eq!(v["rate_per_sec"], 50);
    let (st, v) = s.post(&format!("/v1/bulk/{job}/cancel"), json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["state"], "cancelled");
    let (_, v) = s.get("/v1/bulk").await;
    assert_eq!(v["jobs"].as_array().expect("jobs").len(), 1);
    let (st, _) = s.get("/v1/bulk/blk_nope").await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // Validation.
    let (st, v) = s
        .post("/v1/bulk", json!({ "action": "cancel", "rate_per_sec": 0 }))
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let tags: Vec<String> = (0..9).map(|i| format!("t{i}")).collect();
    let (st, v) = s
        .post(
            "/v1/bulk",
            json!({ "action": "cancel", "filter": { "tags": tags } }),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    assert_eq!(details(&v)["limit"], "search_tags");

    // Job cap: Phase 7's rate-limited code with retry_after_ms.
    for _ in 0..FAKE_MAX_BULK_JOBS {
        let (st, v) = s.post("/v1/bulk", json!({ "action": "pause" })).await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
    }
    let (st, v) = s.post("/v1/bulk", json!({ "action": "pause" })).await;
    assert_eq!(
        (st, code(&v)),
        (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        "{v}"
    );
    assert_eq!(details(&v)["reason"], "bulk_too_many_jobs");
    assert!(details(&v)["retry_after_ms"].as_u64().is_some());
}

// ---------------------------------------------------------------------------------
// Limits, inventory
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn limits_and_inventory() {
    let s = Server::with_slots().await;
    let (st, v) = s.get("/v1/limits").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let names: Vec<&str> = v["limits"]
        .as_array()
        .expect("limits")
        .iter()
        .map(|l| l["name"].as_str().expect("name"))
        .collect();
    assert!(names.contains(&"max_payload_bytes"), "{names:?}");

    s.inv.set_modules(
        (0..5)
            .map(|i| ModuleSummary {
                sha256: format!("{i:064x}"),
                size_bytes: 100 + i,
                uploaded_at_ms: Some(1),
                workflows: vec!["greet".into()],
            })
            .collect(),
    );
    let (st, v) = s.get("/v1/modules?limit=2").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["modules"].as_array().expect("modules").len(), 2);
    let t = v["next_page_token"].as_str().expect("token").to_owned();
    let (_, v) = s.get(&format!("/v1/modules?limit=10&page_token={t}")).await;
    assert_eq!(v["modules"].as_array().expect("modules").len(), 3);
    assert!(v.get("next_page_token").is_none());

    s.inv.set_queues(vec![QueueView {
        name: "default".into(),
        paused: false,
        ready: 3,
        running: 1,
        demand: Some(4),
        flow: None,
    }]);
    let (_, v) = s.get("/v1/queues").await;
    assert_eq!(v["queues"][0]["name"], "default");
    assert_eq!(v["queues"][0]["ready"], 3);
    s.inv.set_partitions(vec![PartitionView {
        id: 0,
        role: "leader".into(),
        leader: Some(1),
        epoch: Some(2),
        applied_offset: Some(10),
        log_end_offset: None,
        ready: None,
        running: None,
        healthy: true,
        detail: None,
    }]);
    let (_, v) = s.get("/v1/partitions").await;
    assert_eq!(v["partitions"][0]["role"], "leader");
    assert!(v["partitions"][0].get("log_end_offset").is_none());
}

// ---------------------------------------------------------------------------------
// Failure policy (RunPolicy untouched)
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn failure_policy_validation_round_trip_and_cas() {
    let s = Server::with_slots().await;
    let (st, v) = s.get("/v1/workflows/greet/failure-policy").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v, json!({}));

    for bad in [
        json!({ "on_failure": { "workflow": "" } }),
        json!({ "on_failure": { "workflow": "_x" } }),
        json!({ "on_failure": { "workflow": "a b" } }),
        json!({ "on_failure": { "workflow": "ok", "queue": "q/x/y" } }),
        json!({ "dlq": { "enabled": false, "hook_catchup_ms": 1 } }),
    ] {
        let (st, v) = s
            .put("/v1/workflows/greet/failure-policy", bad.clone())
            .await;
        assert_eq!(
            (st, code(&v)),
            (StatusCode::BAD_REQUEST, "invalid_argument"),
            "{bad}: {v}"
        );
    }
    let p = json!({ "on_failure": { "workflow": "acme/alert", "include_input": true } });
    let (st, v) = s.put("/v1/workflows/greet/failure-policy", p).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["version_tag"], 1);
    let (_, got) = s.get("/v1/workflows/greet/failure-policy").await;
    assert_eq!(got, v);
    // Stale tag (query) -> version_mismatch; If-Match must agree with the body.
    let (st, v) = s
        .put(
            "/v1/workflows/greet/failure-policy?version_tag=7",
            json!({ "on_failure": { "workflow": "alert" } }),
        )
        .await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(details(&v)["reason"], "version_mismatch");
    let (st, v) = s
        .send(
            s.http
                .put(s.url("/v1/queues/default/failure-policy"))
                .header("if-match", "\"3\"")
                .json(&json!({ "version_tag": 4 })),
        )
        .await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{v}");
    let (st, v) = s
        .put(
            "/v1/queues/default/failure-policy",
            json!({ "dlq": { "enabled": false, "hook_catchup_ms": 60000 } }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert_eq!(v["dlq"]["enabled"], false);

    // RunPolicy routes behave as before (no failure-policy fields leak in).
    let (st, before) = s.get("/v1/workflows/greet/policy").await;
    assert_eq!(st, StatusCode::OK, "{before}");
    let (st, v) = s
        .put("/v1/workflows/greet/policy", json!({ "retention": "1h" }))
        .await;
    assert_eq!(st, StatusCode::OK, "{v}");
    assert!(v.get("on_failure").is_none(), "{v}");
    let (st, v) = s
        .put(
            "/v1/workflows/greet/policy",
            json!({ "on_failure": { "workflow": "x" } }),
        )
        .await;
    assert_eq!(
        st,
        StatusCode::BAD_REQUEST,
        "RunPolicy rejects unknown fields: {v}"
    );
}

// ---------------------------------------------------------------------------------
// Search parameters and history attempt
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn list_runs_search_parameters_reach_the_control_plane() {
    let s = Server::with_slots().await;
    let (st, v) = s.get("/v1/runs?tags=a&tags=b&order=newest&dlq=true").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let req = s.cp.last_list_runs().expect("recorded");
    assert_eq!(req.tags, vec!["a".to_owned(), "b".to_owned()]);
    assert_eq!(req.tag.as_deref(), Some("a"));
    assert_eq!(req.order, ListOrder::Newest);
    assert!(req.dlq);

    let (st, _) = s.get("/v1/runs?tag=a").await;
    assert_eq!(st, StatusCode::OK);
    let req = s.cp.last_list_runs().expect("recorded");
    assert_eq!(req.tags, vec!["a".to_owned()]);
    assert_eq!(req.order, ListOrder::Oldest);
    assert!(!req.dlq);

    let (_, _) = s.get("/v1/runs?tag=a&tags=a&tags=c").await;
    assert_eq!(
        s.cp.last_list_runs().expect("recorded").tags,
        vec!["a".to_owned(), "c".to_owned()]
    );

    let nine: String = (0..9).map(|i| format!("tags=t{i}&")).collect();
    let (st, v) = s.get(&format!("/v1/runs?{nine}")).await;
    assert_eq!(
        (st, code(&v)),
        (StatusCode::BAD_REQUEST, "invalid_argument"),
        "{v}"
    );
    assert_eq!(details(&v)["limit"], "search_tags");
    assert_eq!(details(&v)["max"], 8);
    assert_eq!(details(&v)["actual"], 9);
    let (st, _) = s.get("/v1/runs?order=sideways").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // The fake honours the fields: AND over tags, newest first.
    for tags in [vec!["a", "b"], vec!["a"], vec!["b"]] {
        let (st, v) = s.start_run(json!({ "tags": tags })).await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
    }
    let (_, v) = s.get("/v1/runs?tags=a&tags=b").await;
    assert_eq!(v["runs"].as_array().expect("runs").len(), 1);
    let (_, v) = s.get("/v1/runs?tags=a&order=newest").await;
    let ids: Vec<&str> = v["runs"]
        .as_array()
        .expect("runs")
        .iter()
        .map(|r| r["run_id"].as_str().expect("id"))
        .collect();
    assert_eq!(ids.len(), 2);
    assert!(ids[0] > ids[1], "{ids:?}");
}

#[tokio::test]
async fn history_attempt_is_omitted_when_absent() {
    let s = Server::with_slots().await;
    let (st, v) = s.start_run(json!({})).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let run = v["run_id"].as_str().expect("run").to_owned();
    let (_, v) = s.get(&format!("/v1/runs/{run}/history")).await;
    assert!(v["events"][0].get("attempt").is_none(), "{v}");
    s.cp.set_history_attempt(&run, 1, Some((3u64 << 32) | 2));
    let (_, v) = s.get(&format!("/v1/runs/{run}/history")).await;
    assert_eq!(v["events"][0]["attempt"], (3u64 << 32) | 2);
}

// ---------------------------------------------------------------------------------
// API caps (D-P11-13)
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn payload_cap_and_system_tags_answer_uniform_details() {
    let cfg = ApiConfig {
        max_payload_bytes: 1024,
        ..ApiConfig::default()
    };
    let s = Server::start(true, cfg).await;
    let big = "x".repeat(1100);
    let input = json!({ "s": big });
    let n = serde_json::to_vec(&input).expect("json").len() as u64;
    let (st, v) = s.start_run(json!({ "input": input })).await;
    assert_eq!(
        (st, code(&v)),
        (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
        "{v}"
    );
    assert_eq!(
        *details(&v),
        json!({ "reason": "limit_exceeded", "limit": "payload_bytes", "max": 1024, "actual": n })
    );
    // Exactly at the cap is fine.
    let at = json!("y".repeat(1022));
    assert_eq!(serde_json::to_vec(&at).expect("json").len(), 1024);
    let (st, v) = s.start_run(json!({ "input": at })).await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let run = v["run_id"].as_str().expect("run").to_owned();

    let (st, v) = s
        .post(
            &format!("/v1/runs/{run}/signals/go"),
            json!({ "payload": { "s": big } }),
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(details(&v)["limit"], "payload_bytes");
    assert_eq!(details(&v)["actual"], n);
    assert!(s.cp.signals(&run).is_empty());

    let token = s.cp.mint_token(&run, 1);
    let (st, v) = s
        .post(
            "/v1/tokens/complete",
            json!({ "token": token, "payload": { "s": big } }),
        )
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(details(&v)["limit"], "payload_bytes");
    assert_eq!(s.cp.token_completed(&run, 1), Some(false));

    let (st, v) = s.start_run(json!({ "tags": ["ok", "__ostro/x"] })).await;
    assert_eq!(
        (st, code(&v)),
        (StatusCode::BAD_REQUEST, "invalid_argument"),
        "{v}"
    );
    let (st, v) = s
        .start_run(json!({ "tags": ["_ostro/x", "ostro.schedule"] }))
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
}

#[tokio::test]
async fn default_payload_cap_is_four_mib() {
    let s = Server::start(
        true,
        ApiConfig {
            max_request_bytes: 8 * 1024 * 1024,
            ..ApiConfig::default()
        },
    )
    .await;
    let (st, v) = s
        .start_run(json!({ "input": "z".repeat(4 * 1024 * 1024) }))
        .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE, "{v}");
    assert_eq!(details(&v)["max"], 4 * 1024 * 1024);
}

// ---------------------------------------------------------------------------------
// GET /
// ---------------------------------------------------------------------------------

#[tokio::test]
async fn root_redirects_to_the_ui_when_configured() {
    let s = Server::start(
        true,
        ApiConfig {
            root_redirect: Some("/ui/".into()),
            ..ApiConfig::default()
        },
    )
    .await;
    let r = s.http.get(s.url("/")).send().await.expect("get");
    assert_eq!(r.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        r.headers().get("location").and_then(|v| v.to_str().ok()),
        Some("/ui/")
    );

    let s = Server::with_slots().await;
    let r = s.http.get(s.url("/")).send().await.expect("get");
    assert_eq!(r.status(), StatusCode::OK);
    assert!(r.text().await.expect("body").contains("/openapi.json"));
}

#[test]
fn openapi_lists_every_phase11_route() {
    let doc = serde_json::to_value(ostro_api::openapi()).expect("serialize");
    let paths = doc["paths"].as_object().expect("paths");
    for (path, method) in [
        ("/v1/runs/{run_id}/rerun", "post"),
        ("/v1/runs/{run_id}/rerun-cuts", "get"),
        ("/v1/dlq", "get"),
        ("/v1/dlq/{run_id}/requeue", "post"),
        ("/v1/dlq/{run_id}/dismiss", "post"),
        ("/v1/bulk", "get"),
        ("/v1/bulk", "post"),
        ("/v1/bulk/{job_id}", "get"),
        ("/v1/bulk/{job_id}/cancel", "post"),
        ("/v1/limits", "get"),
        ("/v1/workflows/{name}/failure-policy", "get"),
        ("/v1/workflows/{name}/failure-policy", "put"),
        ("/v1/queues/{name}/failure-policy", "get"),
        ("/v1/queues/{name}/failure-policy", "put"),
        ("/v1/modules", "get"),
        ("/v1/queues", "get"),
        ("/v1/partitions", "get"),
    ] {
        assert!(
            paths.get(path).and_then(|p| p.get(method)).is_some(),
            "{method} {path} missing from OpenAPI"
        );
    }
    let list = &paths["/v1/runs"]["get"]["parameters"];
    for p in ["tags", "order", "dlq", "tag"] {
        assert!(
            list.as_array()
                .expect("params")
                .iter()
                .any(|x| x["name"] == p),
            "GET /v1/runs lacks {p}"
        );
    }
    assert!(paths["/"]["get"]["responses"].get("303").is_some());
}
