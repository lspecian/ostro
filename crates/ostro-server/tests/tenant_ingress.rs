//! 14-20 Task 2: tenant ingress quotas on a real node (DISP-06, D-P14-14/15), in token mode
//! with one tenant-admin token per tenant (ingress is decided after tenant scoping).
//!
//! - `event_rate = 10/1s burst 10`: 30 concurrent REST starts → ~10 accepted, the rest 429
//!   `rate_limited` with `Retry-After` and `details.{reason: tenant_event_rate,
//!   retry_after_ms}`; gRPC answers `RESOURCE_EXHAUSTED` with the same details; an accepted
//!   idempotency key re-submitted is never 429; after `retry_after_ms` a start succeeds;
//!   tenant b's budget is its own.
//! - Signals, await-token completions, webhook deliveries (`/hooks/a/x`) and Kafka-triggered
//!   starts (`__kafka/` keys) spend the same budget; a Kafka dedupe hit is free.
//! - Suspended tenant: start (REST + gRPC), signal, token completion, webhook and Kafka
//!   starts → 403 `tenant_suspended`; a schedule fires no runs (rejections counted in
//!   `ostro_tenant_ingress_rejected_total{reason="suspended",kind="cron"}`, one per due fire,
//!   no spin) and fires again after the resume; a run accepted before the suspension is held,
//!   not failed, and completes after the resume.
//! - Schedule trigger-now spends the same budget (429 `tenant_event_rate`, counted with
//!   `kind="cron"`); a repeated `request_id` is free (p14-fixups-3).

mod common;

use std::time::Duration;

use ostro_meta::ext::{ClusterVersion, put_cluster_version};
use ostro_meta::{
    ApiToken, ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, RateSpec,
    Tenant,
};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_server::config::AuthMode;
use ostro_server::p14::tokens;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::{Instant, sleep};
use tonic::metadata::MetadataValue;

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};

const LIMIT: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct Api {
    http: reqwest::Client,
    base: String,
    bearer: String,
}

/// `(status, Retry-After header, body)`.
type Resp = (StatusCode, Option<String>, Value);

impl Api {
    async fn call(&self, m: reqwest::Method, path: &str, body: Option<Value>) -> Resp {
        let mut rb = self
            .http
            .request(m, format!("{}{path}", self.base))
            .bearer_auth(&self.bearer);
        if let Some(b) = body {
            rb = rb.json(&b);
        }
        let r = rb.send().await.expect("request");
        let st = r.status();
        let ra = r
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let text = r.text().await.expect("body");
        let v = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or(Value::String(text))
        };
        (st, ra, v)
    }

    async fn get(&self, path: &str) -> (StatusCode, Value) {
        let (st, _, v) = self.call(reqwest::Method::GET, path, None).await;
        (st, v)
    }

    async fn post(&self, path: &str, body: Value) -> Resp {
        self.call(reqwest::Method::POST, path, Some(body)).await
    }

    async fn start(&self, workflow: &str, body: Value) -> Resp {
        self.post(&format!("/v1/workflows/{workflow}/runs"), body)
            .await
    }

    async fn start_ok(&self, workflow: &str, body: Value) -> String {
        let (st, _, v) = self.start(workflow, body).await;
        assert_eq!(st, StatusCode::CREATED, "start {workflow}: {v}");
        v["run_id"].as_str().expect("run_id").to_owned()
    }

    async fn signal(&self, run: &str) -> Resp {
        self.post(
            &format!("/v1/runs/{run}/signals/x"),
            json!({"payload": "hi"}),
        )
        .await
    }

    async fn token(&self, token: &str) -> Resp {
        self.post(
            "/v1/tokens/complete",
            json!({"token": token, "payload": "done"}),
        )
        .await
    }

    async fn deploy(&self, guest: &str) {
        let wasm = guest_wasm(guest);
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let hex: String = sha.iter().map(|x| format!("{x:02x}")).collect();
        let r = self
            .http
            .put(format!("{}/v1/modules/{hex}", self.base))
            .bearer_auth(&self.bearer)
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    }

    async fn published_token(&self, run: &str) -> String {
        let deadline = Instant::now() + LIMIT;
        loop {
            let (_, v) = self.get(&format!("/v1/runs/{run}")).await;
            if let Some(t) = v["published_status"]["token"].as_str() {
                return t.to_owned();
            }
            assert!(Instant::now() < deadline, "{run} published no token: {v}");
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn status(&self, run: &str) -> String {
        let (st, v) = self.get(&format!("/v1/runs/{run}")).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        v["status"].as_str().expect("status").to_owned()
    }

    async fn wait_status(&self, run: &str, want: &str) {
        let deadline = Instant::now() + LIMIT;
        loop {
            let s = self.status(run).await;
            if s == want {
                return;
            }
            assert!(Instant::now() < deadline, "{run} is {s}, want {want}");
            sleep(Duration::from_millis(50)).await;
        }
    }

    async fn run_count(&self, filter: &str) -> usize {
        let (st, v) = self.get(&format!("/v1/runs?limit=1000&{filter}")).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        v["runs"].as_array().expect("runs").len()
    }

    async fn grpc(&self, addr: std::net::SocketAddr) -> GrpcRuns {
        let ch = tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .expect("uri")
            .connect()
            .await
            .expect("grpc connect");
        let auth: MetadataValue<_> = format!("Bearer {}", self.bearer).parse().expect("header");
        RunsClient::with_interceptor(
            ch,
            Box::new(move |mut req: tonic::Request<()>| {
                req.metadata_mut().insert("authorization", auth.clone());
                Ok(req)
            }) as Interceptor,
        )
    }
}

type Interceptor =
    Box<dyn FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Send>;
type GrpcRuns = RunsClient<
    tonic::service::interceptor::InterceptedService<tonic::transport::Channel, Interceptor>,
>;

fn grpc_start(workflow: &str, input: Value, key: Option<&str>) -> pb::StartRunRequest {
    pb::StartRunRequest {
        workflow: workflow.into(),
        input_json: input.to_string().into_bytes(),
        delay_ms: Some(3_600_000),
        idempotency_key: key.map(str::to_owned),
        ..Default::default()
    }
}

async fn propose(meta: &MetaHandle, key: MetaKey, record: MetaRecord) {
    let cur = meta.view().read(|s| s.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version: cur,
            record,
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn put_tenant(meta: &MetaHandle, t: Tenant) {
    propose(meta, MetaKey::Tenant(t.id.clone()), MetaRecord::Tenant(t)).await;
}

fn tenant(id: &str, event_rate: Option<RateSpec>, suspended: bool) -> Tenant {
    Tenant {
        id: id.into(),
        created_at_ms: 1,
        event_rate,
        suspended,
        ..Tenant::default()
    }
}

fn rate(limit: u32, period_ms: u64, burst: u32) -> Option<RateSpec> {
    Some(RateSpec {
        limit,
        period_ms,
        burst,
    })
}

async fn put_token(meta: &MetaHandle, tenant: Option<&str>, scopes: &[&str]) -> String {
    let (id, secret, bearer) = tokens::generate_os().expect("generate");
    let rec = ApiToken {
        id: id.as_str().to_owned(),
        hash: tokens::hash(&secret),
        scopes: scopes.iter().map(|s| (*s).to_owned()).collect(),
        tenant: tenant.map(str::to_owned),
        created_at_ms: 1,
        expires_at_ms: None,
        ext: Vec::new(),
    };
    propose(meta, MetaKey::Token(rec.id.clone()), MetaRecord::Token(rec)).await;
    bearer.expose().to_owned()
}

struct Fx {
    s: TestServer,
    a: Api,
    b: Api,
}

async fn fixture() -> Fx {
    let dir = tmpfs_dir();
    let mut cfg = server_config(dir.path(), &ServerOpts::default());
    cfg.auth.mode = AuthMode::Token;
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(LIMIT).await;
    let meta = s.h().meta();
    let mut info: ClusterInfo = meta
        .view()
        .read(|st| st.cluster().cloned())
        .expect("cluster")
        .value;
    info.ext = put_cluster_version(
        &info.ext,
        &ClusterVersion {
            finalized: 2,
            hold: None,
            finalized_at_ms: 1,
        },
    )
    .expect("ext 98");
    propose(meta, MetaKey::Cluster, MetaRecord::Cluster(info)).await;
    for t in ["a", "b"] {
        put_tenant(meta, tenant(t, None, false)).await;
    }
    let base = s.url("").trim_end_matches('/').to_owned();
    let api = |bearer: String| Api {
        http: reqwest::Client::new(),
        base: base.clone(),
        bearer,
    };
    Fx {
        a: api(put_token(meta, Some("a"), &["admin"]).await),
        b: api(put_token(meta, Some("b"), &["admin"]).await),
        s,
    }
}

/// The sum of `ostro_tenant_ingress_rejected_total` samples with `reason` and `kind`.
async fn rejected(s: &TestServer, reason: &str, kind: &str) -> u64 {
    let body = reqwest::get(s.url("/metrics"))
        .await
        .expect("GET /metrics")
        .text()
        .await
        .expect("metrics body");
    let r = format!("reason=\"{reason}\"");
    let k = format!("kind=\"{kind}\"");
    body.lines()
        .filter(|l| l.starts_with("ostro_tenant_ingress_rejected_total{"))
        .filter(|l| l.contains(&r) && l.contains(&k))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .map(|v| v as u64)
        .sum()
}

fn assert_rate_limited(r: &Resp) -> u64 {
    let (st, ra, v) = r;
    assert_eq!(*st, StatusCode::TOO_MANY_REQUESTS, "{v}");
    assert_eq!(v["error"]["code"], "rate_limited", "{v}");
    assert_eq!(v["error"]["details"]["reason"], "tenant_event_rate", "{v}");
    let ms = v["error"]["details"]["retry_after_ms"]
        .as_u64()
        .expect("retry_after_ms");
    assert!(ms >= 1, "{v}");
    let secs: u64 = ra
        .as_deref()
        .expect("Retry-After")
        .parse()
        .expect("seconds");
    assert_eq!(secs, ms.div_ceil(1000), "Retry-After is ceil seconds");
    ms
}

fn assert_suspended(r: &Resp) {
    let (st, _, v) = r;
    assert_eq!(*st, StatusCode::FORBIDDEN, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "tenant_suspended", "{v}");
}

/// `n` parked starts of `api`, concurrently.
async fn concurrent(api: &Api, n: usize) -> Vec<Resp> {
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..n {
        let api = api.clone();
        set.spawn(async move { api.start("signals", parked()).await });
    }
    set.join_all().await
}

fn parked() -> Value {
    // Delayed by an hour: accepted runs stay scheduled (no execution load).
    json!({"input": {"mode": "long_wait"}, "delay_ms": 3_600_000})
}

fn keyed(key: &str) -> Value {
    json!({"input": {"mode": "long_wait"}, "delay_ms": 3_600_000, "idempotency_key": key})
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn event_rate_limits_rest_and_grpc_per_tenant() {
    let Fx { s, a, b } = fixture().await;
    a.deploy("signals").await;
    b.deploy("signals").await;
    let meta = s.h().meta();
    for t in ["a", "b"] {
        put_tenant(meta, tenant(t, rate(10, 1_000, 10), false)).await;
    }
    // The first start of the burst carries an idempotency key.
    let kept = a.start_ok("signals", keyed("keep-1")).await;
    // 29 more, concurrently.
    let t0 = Instant::now();
    let rs = concurrent(&a, 29).await;
    let elapsed = t0.elapsed();
    let accepted = 1 + rs.iter().filter(|r| r.0 == StatusCode::CREATED).count();
    let limited: Vec<&Resp> = rs
        .iter()
        .filter(|r| r.0 == StatusCode::TOO_MANY_REQUESTS)
        .collect();
    assert_eq!(accepted + limited.len(), 30, "only 201 or 429");
    let refill = usize::try_from(elapsed.as_millis() / 100).unwrap_or(usize::MAX);
    assert!(
        (10..=11 + refill).contains(&accepted),
        "accepted {accepted} of 30 in {elapsed:?}"
    );
    let mut last_wait = 0;
    for r in &limited {
        last_wait = assert_rate_limited(r);
        assert!(last_wait <= 1_000, "retry_after_ms {last_wait}");
    }
    eprintln!(
        "REST burst: {accepted} accepted / {} rejected of 30 in {elapsed:?}",
        limited.len()
    );
    // gRPC: RESOURCE_EXHAUSTED with the same details, on the same budget.
    let mut runs = a.grpc(s.addr()).await;
    let mut grpc_ok = 0;
    let e = loop {
        match runs
            .start(grpc_start("signals", json!({"mode": "long_wait"}), None))
            .await
        {
            Ok(_) => {
                grpc_ok += 1;
                assert!(grpc_ok <= 5, "gRPC starts are not limited");
            }
            Err(e) => break e,
        }
    };
    assert_eq!(e.code(), tonic::Code::ResourceExhausted, "{e:?}");
    let details = ostro_api::grpc::status_details(&e).expect("details");
    assert_eq!(details["reason"], "tenant_event_rate", "{details}");
    let wait = details["retry_after_ms"].as_u64().expect("retry_after_ms");
    assert!((1..=1_000).contains(&wait), "{details}");
    assert_eq!(
        ostro_api::ErrorCode::from_grpc_code_and_details(e.code(), Some(&details)),
        ostro_api::ErrorCode::RateLimited
    );
    // An accepted key re-submitted over budget is never 429 (dedupe hit, not charged).
    for _ in 0..5 {
        let (st, _, v) = a.start("signals", keyed("keep-1")).await;
        assert!(st.is_success(), "dedupe hit: {st} {v}");
        assert_eq!(v["run_id"], kept.as_str());
        assert_eq!(v["created"], false);
    }
    last_wait = last_wait.max(wait);
    // Tenant b's budget is its own.
    let rs = concurrent(&b, 10).await;
    assert!(
        rs.iter().all(|r| r.0 == StatusCode::CREATED),
        "b: {:?}",
        rs.iter().map(|r| r.0).collect::<Vec<_>>()
    );
    // After retry_after_ms a start succeeds.
    sleep(Duration::from_millis(last_wait)).await;
    a.start_ok("signals", parked()).await;
    assert!(rejected(&s, "event_rate", "start").await >= limited.len() as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signals_tokens_webhooks_and_kafka_share_the_budget() {
    let Fx { s, a, .. } = fixture().await;
    a.deploy("signals").await;
    a.deploy("tokens").await;
    let (st, _, v) = a
        .post(
            "/v1/webhooks",
            json!({"name": "x", "action": {"kind": "start", "workflow": "signals", "input": "body"}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    // Before any limit: a run waiting for three signals, two token runs, one Kafka-keyed
    // start.
    let sig = a
        .start_ok("signals", json!({"input": {"mode": "fifo"}}))
        .await;
    let t1 = a.start_ok("tokens", json!({"input": {}})).await;
    let t2 = a.start_ok("tokens", json!({"input": {}})).await;
    let tok1 = a.published_token(&t1).await;
    let tok2 = a.published_token(&t2).await;
    let kafka0 = a.start_ok("signals", keyed("__kafka/k1/t/0/0")).await;
    // 2 events per minute: the budget does not refill during the test.
    put_tenant(s.h().meta(), tenant("a", rate(2, 60_000, 2), false)).await;
    let (st, _, v) = a.signal(&sig).await;
    assert_eq!(st, StatusCode::OK, "signal 1: {v}");
    let (st, _, v) = a.token(&tok1).await;
    assert_eq!(st, StatusCode::OK, "token 1: {v}");
    // Budget spent: token, signal, webhook and Kafka starts are refused.
    assert_rate_limited(&a.token(&tok2).await);
    assert_rate_limited(&a.signal(&sig).await);
    let hook = reqwest::Client::new()
        .post(s.url("/hooks/a/x"))
        .header("content-type", "application/json")
        .body(br#"{"mode":"long_wait"}"#.to_vec())
        .send()
        .await
        .expect("POST hook");
    let st = hook.status();
    let ra = hook
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let body: Value = hook.json().await.expect("hook json");
    assert_rate_limited(&(st, ra, body));
    assert_rate_limited(&a.start("signals", keyed("__kafka/k1/t/0/1")).await);
    // A Kafka dedupe hit (the driver retrying a record whose start landed) is free.
    let (st, _, v) = a.start("signals", keyed("__kafka/k1/t/0/0")).await;
    assert!(st.is_success(), "kafka dedupe hit: {st} {v}");
    assert_eq!(v["run_id"], kafka0.as_str());
    // The refused events were not applied.
    a.wait_status(&t1, "completed").await;
    assert_ne!(
        a.status(&t2).await,
        "completed",
        "the refused completion was not applied"
    );
    for (kind, n) in [("token", 1), ("signal", 1), ("webhook", 1), ("kafka", 1)] {
        let got = rejected(&s, "event_rate", kind).await;
        assert!(got >= n, "{kind}: {got}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schedule_trigger_now_spends_the_budget() {
    let Fx { s, a, b } = fixture().await;
    a.deploy("signals").await;
    b.deploy("signals").await;
    // Paused (no cron fires; fires are not charged anyway), every manual fire starts.
    for api in [&a, &b] {
        let (st, _, v) = api
            .post(
                "/v1/schedules",
                json!({"name": "now", "workflow": "signals", "cron": "0 0 * * * *",
                       "input": {"mode": "long_wait"}, "paused": true, "overlap": "allow_all"}),
            )
            .await;
        assert_eq!(st, StatusCode::CREATED, "{v}");
    }
    // 2 events per minute: the budget does not refill during the test.
    put_tenant(s.h().meta(), tenant("a", rate(2, 60_000, 2), false)).await;
    let trigger = |api: &Api, request_id: &'static str| {
        let api = api.clone();
        async move {
            api.post(
                "/v1/schedules/now/trigger",
                json!({"request_id": request_id}),
            )
            .await
        }
    };
    let (st, _, v) = trigger(&a, "r-1").await;
    assert_eq!(st, StatusCode::OK, "trigger 1: {v}");
    let run1 = v["run_id"].as_str().expect("run_id").to_owned();
    let (st, _, v) = trigger(&a, "r-2").await;
    assert_eq!(st, StatusCode::OK, "trigger 2: {v}");
    // Budget spent: trigger-now and a plain start are refused, nothing is started.
    let before = a.run_count("workflow=signals").await;
    assert_rate_limited(&trigger(&a, "r-3").await);
    assert_rate_limited(&a.start("signals", parked()).await);
    assert_eq!(a.run_count("workflow=signals").await, before);
    // A repeated request_id (its dedupe row exists) is free: the same run.
    let (st, _, v) = trigger(&a, "r-1").await;
    assert_eq!(st, StatusCode::OK, "repeated request_id: {v}");
    assert_eq!(v["run_id"], run1.as_str());
    // Tenant b's trigger-now has its own (unlimited) budget.
    for id in ["r-1", "r-2", "r-3"] {
        let (st, _, v) = trigger(&b, id).await;
        assert_eq!(st, StatusCode::OK, "b {id}: {v}");
    }
    let got = rejected(&s, "event_rate", "cron").await;
    assert!(got >= 1, "cron event_rate rejections: {got}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suspended_tenant_ingress_is_refused_and_schedules_resume() {
    let Fx { s, a, b } = fixture().await;
    a.deploy("signals").await;
    a.deploy("tokens").await;
    a.deploy("p11-hook").await;
    b.deploy("signals").await;
    let meta = s.h().meta();
    let (st, _, v) = a
        .post(
            "/v1/webhooks",
            json!({"name": "x", "action": {"kind": "start", "workflow": "signals", "input": "body"}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let sig = a
        .start_ok("signals", json!({"input": {"mode": "long_wait"}}))
        .await;
    let t1 = a.start_ok("tokens", json!({"input": {}})).await;
    let tok = a.published_token(&t1).await;
    let (st, _, v) = a
        .post(
            "/v1/schedules",
            json!({"name": "tick", "workflow": "w", "cron": "* * * * * *", "input": {}}),
        )
        .await;
    assert_eq!(st, StatusCode::CREATED, "{v}");
    let deadline = Instant::now() + LIMIT;
    while a.run_count("workflow=w").await == 0 {
        assert!(Instant::now() < deadline, "the schedule never fired");
        sleep(Duration::from_millis(100)).await;
    }
    // A run accepted before the suspension; it becomes ready while a is suspended.
    let held = a
        .start_ok(
            "signals",
            json!({"input": {"mode": "timeout", "timeout_ms": 50}, "delay_ms": 3_000}),
        )
        .await;

    put_tenant(meta, tenant("a", None, true)).await;
    let suspended_at = Instant::now();
    // Every ingress kind: 403 tenant_suspended.
    assert_suspended(&a.start("signals", parked()).await);
    assert_suspended(&a.signal(&sig).await);
    assert_suspended(&a.token(&tok).await);
    assert_suspended(&a.start("signals", keyed("__kafka/k1/t/0/9")).await);
    let hook = reqwest::Client::new()
        .post(s.url("/hooks/a/x"))
        .header("content-type", "application/json")
        .body(br#"{"mode":"long_wait"}"#.to_vec())
        .send()
        .await
        .expect("POST hook");
    let st = hook.status();
    let body: Value = hook.json().await.expect("hook json");
    assert_suspended(&(st, None, body));
    let (st, _, v) = a.post("/v1/schedules/tick/trigger", json!({})).await;
    assert_suspended(&(st, None, v));
    let mut runs = a.grpc(s.addr()).await;
    let e = runs
        .start(grpc_start("signals", json!({"mode": "long_wait"}), None))
        .await
        .expect_err("gRPC start while suspended");
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e:?}");
    let details = ostro_api::grpc::status_details(&e).expect("details");
    assert_eq!(details["reason"], "tenant_suspended", "{details}");
    // Tenant b is unaffected.
    b.start_ok("signals", parked()).await;

    // The schedule creates no runs while suspended (a fire in flight may still land).
    sleep(Duration::from_millis(1_500)).await;
    let before = a.run_count("workflow=w").await;
    let cron0 = rejected(&s, "suspended", "cron").await;
    let window = Instant::now();
    sleep(Duration::from_millis(4_000)).await;
    let after = a.run_count("workflow=w").await;
    let cron1 = rejected(&s, "suspended", "cron").await;
    let secs = window.elapsed().as_secs() + 2;
    assert_eq!(before, after, "a suspended tenant's schedule fired");
    assert!(
        cron1 > cron0 && cron1 - cron0 <= secs,
        "cron rejections {cron0} -> {cron1} over ~{secs} s (one per due fire, no spin)"
    );
    eprintln!(
        "suspended: {} cron fires refused in {:?}; runs before {before}, after {after}",
        cron1 - cron0,
        window.elapsed()
    );
    // The run accepted before the suspension is held, not failed.
    assert!(suspended_at.elapsed() > Duration::from_secs(3));
    let st = a.status(&held).await;
    assert!(st == "scheduled" || st == "ready", "held run is {st}");

    // Resume: everything flows again.
    put_tenant(meta, tenant("a", None, false)).await;
    a.wait_status(&held, "completed").await;
    let deadline = Instant::now() + LIMIT;
    while a.run_count("workflow=w").await <= after {
        assert!(
            Instant::now() < deadline,
            "the schedule did not fire after resume"
        );
        sleep(Duration::from_millis(100)).await;
    }
    let (st, _, v) = a.signal(&sig).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    a.wait_status(&sig, "completed").await;
    let (st, _, v) = a.token(&tok).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    a.wait_status(&t1, "completed").await;
    a.start_ok("signals", parked()).await;
    let (st, _, v) = a.post("/v1/schedules/tick/pause", json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    for kind in ["start", "signal", "token", "webhook", "kafka"] {
        assert!(rejected(&s, "suspended", kind).await >= 1, "{kind}");
    }
}
