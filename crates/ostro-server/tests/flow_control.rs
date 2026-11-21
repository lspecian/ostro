//! Node flow control end to end (plan 07-08, DISP-03/04/05): the in-process server with
//! `flow::NodeFlow` installed in every partition dispatcher.
//!
//! - queue concurrency and key concurrency cap leased activations node-wide (sampled from
//!   `NodeFlow::running` / `key_running` and the dispatcher's lease gauge);
//! - a throttle spaces first activations and never rejects; re-activations are not
//!   throttled;
//! - queue and workflow pauses hold entries and a resume CAS wakes the dispatchers through
//!   `changed()` (there is no polling interval);
//! - weights 3:1 show in grant order;
//! - the StartRun rate limit (`rate_limit` tests) answers 429 / RESOURCE_EXHAUSTED with
//!   Retry-After, charges nothing for an idempotent re-submit, and isolates keys.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, QueuePolicy, RateSpec, WorkflowPolicy};
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(60);

async fn server_with(tweak: impl FnOnce(&mut ostro_server::config::ServerConfig)) -> TestServer {
    let s = TestServer::start_with(ServerOpts::default(), tweak).await;
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

async fn server() -> TestServer {
    server_with(|_| {}).await
}

async fn deploy(s: &TestServer, guest: &str) {
    let wasm = guest_wasm(guest);
    let sha: String = Sha256::digest(&wasm)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
}

/// Create or overwrite a meta record (CAS on the current version).
async fn put(s: &TestServer, record: MetaRecord) {
    let meta = s.h().meta();
    let key = record.key();
    let expected_version = meta.view().read(|st| st.get(&key).map(|v| v.version));
    let r = meta
        .propose(MetaCommand::Put {
            key,
            expected_version,
            record,
        })
        .await
        .expect("meta put");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

async fn queue_policy(s: &TestServer, p: QueuePolicy) {
    put(s, MetaRecord::QueuePolicy(p)).await;
}

/// POST a start; returns (status, Retry-After header, body).
async fn start_raw(
    s: &TestServer,
    workflow: &str,
    body: Value,
) -> (StatusCode, Option<String>, Value) {
    let r = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    let st = r.status();
    let ra = r
        .headers()
        .get("retry-after")
        .map(|v| v.to_str().expect("ascii").to_owned());
    (st, ra, r.json().await.expect("json body"))
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> String {
    let (st, _, b) = start_raw(s, workflow, body).await;
    assert_eq!(st, StatusCode::CREATED, "{b}");
    b["run_id"].as_str().expect("run_id").to_owned()
}

async fn get(s: &TestServer, run_id: &str) -> Value {
    let r = s
        .http
        .get(s.url(&format!("/v1/runs/{run_id}")))
        .send()
        .await
        .expect("GET run");
    assert_eq!(r.status(), StatusCode::OK);
    r.json().await.expect("json")
}

async fn wait_completed(s: &TestServer, run_id: &str, limit: Duration) -> Value {
    let t0 = Instant::now();
    loop {
        let b = get(s, run_id).await;
        match b["status"].as_str() {
            Some("completed") => return b,
            Some("failed" | "cancelled" | "terminated" | "timed_out") => {
                panic!("run {run_id} ended badly: {b}")
            }
            _ => {}
        }
        assert!(t0.elapsed() < limit, "run {run_id} not completed: {b}");
        sleep(Duration::from_millis(20)).await;
    }
}

fn granted(s: &TestServer, run_id: &str) -> bool {
    let run: RunId = run_id.parse().expect("run id");
    s.run_record(&run).current_attempt.is_some()
}

/// `ended_ms` of a completed `cpu-burn` run.
fn ended_ms(b: &Value) -> u64 {
    b["result"]["ended_ms"].as_u64().expect("ended_ms")
}

/// Sample `f` every 2 ms until stopped; returns the maximum seen.
fn sampler(
    f: impl Fn() -> u32 + Send + 'static,
) -> (Arc<AtomicBool>, tokio::task::JoinHandle<u32>) {
    let stop = Arc::new(AtomicBool::new(false));
    let st = Arc::clone(&stop);
    let h = tokio::spawn(async move {
        let mut max = 0;
        while !st.load(Ordering::Acquire) {
            max = max.max(f());
            sleep(Duration::from_millis(2)).await;
        }
        max
    });
    (stop, h)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_concurrency_caps_leased_activations() {
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "cq".into(),
            concurrency: Some(2),
            ..QueuePolicy::default()
        },
    )
    .await;
    let flow = Arc::clone(&s.h().p7().flow);
    let f = Arc::clone(&flow);
    let (stop, h) = sampler(move || f.running("cq"));
    // The dispatcher's own lease gauge (every lease of the node; only `cq` runs exist).
    let stats = Arc::clone(s.h().dispatcher_stats());
    let (stop2, h2) =
        sampler(move || u32::try_from(stats.leases.load(Ordering::Relaxed)).unwrap_or(u32::MAX));
    let mut ids = Vec::new();
    for _ in 0..10 {
        ids.push(start(&s, "cpu-burn", json!({"input": {"ms": 150}, "queue": "cq"})).await);
    }
    for id in &ids {
        wait_completed(&s, id, LIMIT).await;
    }
    stop.store(true, Ordering::Release);
    stop2.store(true, Ordering::Release);
    let max = h.await.expect("sampler");
    let leases = h2.await.expect("sampler");
    assert_eq!(max, 2, "sampled queue concurrency (flow counters)");
    assert!(leases <= 2, "sampled dispatcher leases {leases} > 2");
    assert_eq!(flow.running("cq"), 0, "every charge released");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn key_concurrency_isolates_keys() {
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "kq".into(),
            key_concurrency: Some(1),
            ..QueuePolicy::default()
        },
    )
    .await;
    let flow = Arc::clone(&s.h().p7().flow);
    let per_key_max = Arc::new(AtomicU32::new(0));
    let (f, pk) = (Arc::clone(&flow), Arc::clone(&per_key_max));
    let (stop, h) = sampler(move || {
        let ks = ["a", "b", "c"].map(|k| f.key_running("kq", k));
        pk.fetch_max(ks.into_iter().max().unwrap_or(0), Ordering::AcqRel);
        f.running("kq")
    });
    let mut ids: Vec<(String, &str)> = Vec::new();
    for _ in 0..3 {
        for k in ["a", "b", "c"] {
            let body = json!({"input": {"ms": 150}, "queue": "kq", "concurrency_key": k});
            ids.push((start(&s, "cpu-burn", body).await, k));
        }
    }
    for (id, _) in &ids {
        wait_completed(&s, id, LIMIT).await;
    }
    stop.store(true, Ordering::Release);
    let total_max = h.await.expect("sampler");
    assert_eq!(
        per_key_max.load(Ordering::Acquire),
        1,
        "per-key concurrency"
    );
    assert!(total_max >= 2, "different keys never ran in parallel");
    assert!(
        total_max <= 3,
        "more than one activation per key: {total_max}"
    );
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_spaces_first_activations_and_never_rejects() {
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "tq".into(),
            throttle: Some(RateSpec {
                limit: 5,
                period_ms: 1_000,
                burst: 1,
            }),
            ..QueuePolicy::default()
        },
    )
    .await;
    let mut ids = Vec::new();
    for _ in 0..6 {
        // Every start is accepted: a throttle queues, it never rejects.
        ids.push(start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": "tq"})).await);
    }
    // `ended_ms` is the journaled time right after a ~1 ms step: the activation's start
    // plus a few ms (`started_ms` is the run's first journal time, its creation).
    let mut starts = Vec::new();
    for id in &ids {
        starts.push(ended_ms(&wait_completed(&s, id, LIMIT).await));
    }
    starts.sort_unstable();
    let gaps: Vec<u64> = starts.windows(2).map(|w| w[1] - w[0]).collect();
    // GCRA spaces grants exactly 200 ms apart; the activation start adds jitter.
    assert!(
        starts[5] - starts[0] >= 900,
        "6 first activations at 5/s span < ~1 s: {starts:?}"
    );
    assert!(gaps.iter().all(|g| *g >= 100), "gaps {gaps:?}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reactivations_are_not_throttled() {
    // hot_wait 50 ms: the sleep evicts the activation, the timer re-activates it.
    let mut s = server_with(|c| c.engine.hot_wait = Duration::from_millis(50).into()).await;
    deploy(&s, "timeouts").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "rq".into(),
            throttle: Some(RateSpec {
                limit: 1,
                period_ms: 600_000,
                burst: 1,
            }),
            ..QueuePolicy::default()
        },
    )
    .await;
    let a = start(
        &s,
        "timeouts",
        json!({"input": {"mode": "sleep_ms", "n": 300}, "queue": "rq"}),
    )
    .await;
    // The re-activation after the sleep is not a first activation: it is not throttled
    // although the queue allows one first activation per 10 minutes.
    let t0 = Instant::now();
    wait_completed(&s, &a, Duration::from_secs(30)).await;
    assert!(t0.elapsed() < Duration::from_secs(30));
    let run: RunId = a.parse().expect("run id");
    assert!(
        s.jobs_of(&run).len() >= 2,
        "the sleep did not evict: {:?}",
        s.jobs_of(&run)
    );
    // A second first activation is held back by the throttle.
    let b = start(
        &s,
        "timeouts",
        json!({"input": {"mode": "sleep_ms", "n": 1}, "queue": "rq"}),
    )
    .await;
    sleep(Duration::from_millis(800)).await;
    assert!(
        !granted(&s, &b),
        "the throttle let a second first activation through"
    );
    s.shutdown().await;
}

async fn assert_held_then_resumed(s: &TestServer, ids: &[String], resume: MetaRecord) {
    sleep(Duration::from_millis(800)).await;
    for id in ids {
        assert!(!granted(s, id), "paused run {id} was granted");
        assert_eq!(get(s, id).await["status"], "ready", "run {id}");
    }
    put(s, resume).await;
    for id in ids {
        wait_completed(s, id, LIMIT).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_queue_holds_entries_and_resume_wakes_dispatchers() {
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    let paused = |paused| QueuePolicy {
        queue: "pq".into(),
        paused,
        ..QueuePolicy::default()
    };
    queue_policy(&s, paused(true)).await;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": "pq"})).await);
    }
    // Another queue is unaffected.
    let other = start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": "free"})).await;
    wait_completed(&s, &other, LIMIT).await;
    assert_held_then_resumed(&s, &ids, MetaRecord::QueuePolicy(paused(false))).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_workflow_holds_entries_and_resume_wakes_dispatchers() {
    let mut s = server().await;
    deploy(&s, "cpu-burn").await;
    deploy(&s, "smoke").await;
    let paused = |paused| WorkflowPolicy {
        workflow: "cpu-burn".into(),
        paused,
        ..WorkflowPolicy::default()
    };
    put(&s, MetaRecord::WorkflowPolicy(paused(true))).await;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(start(&s, "cpu-burn", json!({"input": {"ms": 1}})).await);
    }
    // Another workflow is unaffected. It runs on its own queue: the 07-05 dispatcher treats
    // `Admit::Paused` as a queue-level skip, so a paused workflow's entry at the head of a
    // shared queue holds that queue back for the round (follow-up in 07-08-SUMMARY).
    let other = start(&s, "smoke", json!({"input": {"x": 1}, "queue": "free"})).await;
    wait_completed(&s, &other, LIMIT).await;
    assert_held_then_resumed(&s, &ids, MetaRecord::WorkflowPolicy(paused(false))).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn weights_three_to_one_in_grant_order() {
    // One partition, one executor slot: grants are strictly sequential, so job completion
    // order is the dispatcher's selection order.
    let mut s = TestServer::start_with(
        ServerOpts {
            partitions: 1,
            ..ServerOpts::default()
        },
        |c| c.executor.slots = 1,
    )
    .await;
    s.wait_ready(Duration::from_secs(10)).await;
    deploy(&s, "cpu-burn").await;
    let hold = |paused| WorkflowPolicy {
        workflow: "cpu-burn".into(),
        paused,
        ..WorkflowPolicy::default()
    };
    put(&s, MetaRecord::WorkflowPolicy(hold(true))).await;
    for (q, w) in [("wa", 3), ("wb", 1)] {
        queue_policy(
            &s,
            QueuePolicy {
                queue: q.into(),
                weight: Some(w),
                ..QueuePolicy::default()
            },
        )
        .await;
    }
    let mut queue_of: BTreeMap<RunId, &str> = BTreeMap::new();
    let mut ids = Vec::new();
    for _ in 0..40 {
        for q in ["wa", "wb"] {
            let id = start(&s, "cpu-burn", json!({"input": {"ms": 1}, "queue": q})).await;
            queue_of.insert(id.parse().expect("run id"), q);
            ids.push(id);
        }
    }
    // One policy change releases both queues at once.
    put(&s, MetaRecord::WorkflowPolicy(hold(false))).await;
    for id in &ids {
        wait_completed(&s, id, LIMIT).await;
    }
    let order: Vec<&str> = s
        .hooks
        .jobs()
        .iter()
        .filter_map(|j| queue_of.get(&j.run).copied())
        .collect();
    let first: usize = order.iter().take(40).filter(|q| **q == "wa").count();
    assert!(
        (26..=34).contains(&first),
        "weight 3:1 expected ~30 of the first 40 grants for `wa`, got {first}: {order:?}"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// StartRun rate limit (D-W21)
// ---------------------------------------------------------------------------------

fn rate(limit: u32, burst: u32) -> Option<RateSpec> {
    // One minute periods: the test never waits for a refill, so a slow machine cannot let
    // an extra start conform.
    Some(RateSpec {
        limit,
        period_ms: 60_000,
        burst,
    })
}

async fn run_count(s: &TestServer, queue: &str) -> usize {
    let r = s
        .http
        .get(s.url(&format!("/v1/runs?limit=1000&queue={queue}")))
        .send()
        .await
        .expect("list");
    assert_eq!(r.status(), StatusCode::OK);
    let b: Value = r.json().await.expect("json");
    b["runs"].as_array().expect("runs").len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limit_rejects_excess_starts_with_429() {
    let mut s = server().await;
    deploy(&s, "smoke").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "lq".into(),
            rate_limit: rate(3, 3),
            ..QueuePolicy::default()
        },
    )
    .await;
    for _ in 0..3 {
        start(&s, "smoke", json!({"input": {}, "queue": "lq"})).await;
    }
    let (st, ra, b) = start_raw(&s, "smoke", json!({"input": {}, "queue": "lq"})).await;
    assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{b}");
    assert_eq!(b["error"]["code"], "rate_limited", "{b}");
    let retry_after: u64 = ra.expect("Retry-After").parse().expect("seconds");
    assert!(retry_after >= 1, "Retry-After {retry_after}");
    assert!(
        b["error"]["details"]["retry_after_ms"]
            .as_u64()
            .expect("retry_after_ms")
            > 0,
        "{b}"
    );
    assert_eq!(
        run_count(&s, "lq").await,
        3,
        "the rejected start created a run"
    );
    // Other queues are not limited.
    start(&s, "smoke", json!({"input": {}, "queue": "free"})).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limit_does_not_charge_idempotent_resubmits() {
    let mut s = server().await;
    deploy(&s, "smoke").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "iq".into(),
            rate_limit: rate(1, 1),
            ..QueuePolicy::default()
        },
    )
    .await;
    let body = json!({"input": {}, "queue": "iq", "idempotency_key": "k1"});
    let id = start(&s, "smoke", body.clone()).await;
    for _ in 0..3 {
        let (st, _, b) = start_raw(&s, "smoke", body.clone()).await;
        assert_eq!(st, StatusCode::OK, "idempotent re-submit: {b}");
        assert_eq!(b["run_id"], id.as_str());
        assert_eq!(b["created"], false);
    }
    let (st, _, b) = start_raw(
        &s,
        "smoke",
        json!({"input": {}, "queue": "iq", "idempotency_key": "k2"}),
    )
    .await;
    assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{b}");
    assert_eq!(run_count(&s, "iq").await, 1);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rate_limit_per_key_isolates_keys_and_maps_to_grpc() {
    let mut s = server().await;
    deploy(&s, "smoke").await;
    queue_policy(
        &s,
        QueuePolicy {
            queue: "gq".into(),
            key_rate_limit: rate(1, 1),
            ..QueuePolicy::default()
        },
    )
    .await;
    let mut runs = RunsClient::new(s.channel().await);
    let req = |key: &str| pb::StartRunRequest {
        workflow: "smoke".into(),
        input_json: b"{}".to_vec(),
        queue: Some("gq".into()),
        concurrency_key: Some(key.into()),
        ..Default::default()
    };
    runs.start(req("a")).await.expect("key a");
    let e = runs.start(req("a")).await.expect_err("key a again");
    assert_eq!(e.code(), tonic::Code::ResourceExhausted);
    let details = ostro_api::grpc::status_details(&e).expect("details");
    assert!(
        details["retry_after_ms"].as_u64().expect("retry_after_ms") > 0,
        "{details}"
    );
    assert_eq!(
        ostro_api::ErrorCode::from_grpc_code_and_details(e.code(), Some(&details)),
        ostro_api::ErrorCode::RateLimited
    );
    runs.start(req("b")).await.expect("key b is independent");
    // Unkeyed starts are not key-limited.
    for _ in 0..3 {
        start(&s, "smoke", json!({"input": {}, "queue": "gq"})).await;
    }
    // REST maps the same limit to 429.
    let (st, _, b) = start_raw(
        &s,
        "smoke",
        json!({"input": {}, "queue": "gq", "concurrency_key": "b"}),
    )
    .await;
    assert_eq!(st, StatusCode::TOO_MANY_REQUESTS, "{b}");
    s.shutdown().await;
}
