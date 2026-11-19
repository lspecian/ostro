//! Phase 4 SDK end to end against the full in-process server (plan 04-20): the real
//! `ostro_server::server::start_with_hooks` (meta, partitions, dispatcher, engine
//! executor, REST + gRPC on one real port). Used where the binary cannot help: config
//! knobs (hot wait, lease TTL), executor fault injection (lease stall / zombie), direct
//! journal access for a fresh-runtime replay, and meta CAS proposals.
//!
//! Journal assertions go through the history API (what users see), except the zombie
//! append and the native-determinism replay, which need the partition handle.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_engine::testkit::replay_journal;
use ostro_meta::command::{MetaCommand, MetaKey, MetaRecord, MetaResult};
use ostro_meta::records::ActiveVersion;
use ostro_proto::api::v1 as pb;
use ostro_proto::api::v1::runs_client::RunsClient;
use ostro_server::config::FriendlyDuration;
use ostro_server::sink::PartitionSink;
use ostro_types::RunId;
use ostro_types::journal::{
    CommandHash, CommandMeta, Event, EventBody, RecordedResult, StepCompleted,
};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::time::sleep;

const LIMIT: Duration = Duration::from_secs(30);

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn ready(s: TestServer) -> TestServer {
    s.wait_ready(Duration::from_secs(10)).await;
    s
}

async fn deploy(s: &TestServer, guest: &str) -> String {
    let wasm = guest_wasm(guest);
    let sha = sha_hex(&wasm);
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{sha}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
    sha
}

async fn get_json(s: &TestServer, path: &str) -> (StatusCode, Value) {
    let r = s.http.get(s.url(path)).send().await.expect("GET");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn start(s: &TestServer, workflow: &str, body: Value) -> String {
    let r = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&body)
        .send()
        .await
        .expect("POST run");
    let st = r.status();
    let b: Value = r.json().await.expect("json body");
    assert_eq!(st, StatusCode::CREATED, "{b}");
    b["run_id"].as_str().expect("run_id").to_owned()
}

async fn wait_terminal(s: &TestServer, id: &str) -> Value {
    let t0 = Instant::now();
    loop {
        let (st, b) = get_json(s, &format!("/v1/runs/{id}")).await;
        assert_eq!(st, StatusCode::OK, "{b}");
        if matches!(
            b["status"].as_str(),
            Some("completed" | "failed" | "cancelled")
        ) {
            return b;
        }
        assert!(t0.elapsed() < LIMIT, "run {id} not terminal: {b}");
        sleep(Duration::from_millis(20)).await;
    }
}

/// Every history event over REST.
async fn history(s: &TestServer, id: &str) -> Vec<Value> {
    let (st, p) = get_json(s, &format!("/v1/runs/{id}/history?limit=1000")).await;
    assert_eq!(st, StatusCode::OK, "{p}");
    assert_eq!(p["next_from_seq"], Value::Null);
    p["events"].as_array().expect("events").clone()
}

fn of_kind<'a>(h: &'a [Value], kind: &str) -> Vec<&'a Value> {
    h.iter().filter(|e| e["kind"] == kind).collect()
}

fn saf_of<'a>(h: &'a [Value], step: &str) -> Vec<&'a Value> {
    h.iter()
        .filter(|e| e["kind"] == "step_attempt_failed" && e["details"]["name"] == step)
        .collect()
}

fn completions_of<'a>(h: &'a [Value], step: &str) -> Vec<&'a Value> {
    h.iter()
        .filter(|e| e["kind"] == "step_completed" && e["details"]["name"] == step)
        .collect()
}

fn run_id(id: &str) -> RunId {
    id.parse().expect("run id")
}

/// The GET `failure` field is a JSON string: parse it.
fn failure(run: &Value) -> Value {
    serde_json::from_str(run["failure"].as_str().expect("failure string")).expect("failure json")
}

// ---------------------------------------------------------------------------------
// WF-01: typed JSON through REST and gRPC
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_json_rest_and_grpc() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-basic").await;
    let doc = json!({
        "s": "ünïcødé \"quoted\"",
        "n": -42,
        "f": 1.5,
        "big": 9_007_199_254_740_991_u64,
        "list": [1, null, true, {"nested": ["x"]}],
        "empty": {}
    });

    // sdk-echo returns the input value unchanged; sdk-basic maps a typed struct.
    let echo = start(&s, "sdk-echo", json!({ "input": doc })).await;
    let basic = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "rest", "n": 100}}),
    )
    .await;
    let re = wait_terminal(&s, &echo).await;
    let rb = wait_terminal(&s, &basic).await;
    assert_eq!(re["status"], "completed", "{re}");
    assert_eq!(re["result"], doc);
    assert_eq!(
        rb["result"],
        json!({"greeting": "hello, rest", "sum": 5050, "steps": 2})
    );

    let mut runs = RunsClient::new(s.channel().await);
    let mut ids = Vec::new();
    for (wf, input) in [
        ("sdk-echo", doc.clone()),
        ("sdk-basic", json!({"name": "grpc", "n": 7})),
    ] {
        let r = runs
            .start(pb::StartRunRequest {
                workflow: wf.into(),
                input_json: serde_json::to_vec(&input).expect("json"),
                ..Default::default()
            })
            .await
            .expect("grpc start")
            .into_inner();
        ids.push(r.run_id);
    }
    let mut results = Vec::new();
    for id in &ids {
        wait_terminal(&s, id).await;
        let info = runs
            .get(pb::GetRunRequest { run_id: id.clone() })
            .await
            .expect("grpc get")
            .into_inner();
        assert_eq!(info.status, pb::RunStatus::Completed as i32, "{info:?}");
        let v: Value = serde_json::from_slice(info.result_json.as_deref().expect("result"))
            .expect("result json");
        results.push(v);
    }
    assert_eq!(results[0], doc);
    assert_eq!(
        results[1],
        json!({"greeting": "hello, grpc", "sum": 28, "steps": 2})
    );

    // Input that does not match the typed struct fails the run, non-retryable.
    let bad = start(&s, "sdk-basic", json!({"input": {"name": 5}})).await;
    let rbad = wait_terminal(&s, &bad).await;
    assert_eq!(rbad["status"], "failed", "{rbad}");
    let f = failure(&rbad);
    assert!(f.to_string().contains("invalid_json"), "{f}");
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// WF-02: step retries through the server
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_exhaust() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-retry").await;
    let id = start(&s, "sdk-retry", json!({"input": {"mode": "exhaust"}})).await;
    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "failed", "{run}");
    let f = failure(&run);
    assert!(
        f["message"]
            .as_str()
            .is_some_and(|m| m.contains("transient")),
        "step error surfaced: {f}"
    );

    let h = history(&s, &id).await;
    let saf = saf_of(&h, "flaky");
    assert_eq!(saf.len(), 3, "{h:?}");
    // retry(max_attempts 4, initial 50ms, max 200ms, backoff 2.0): 50, 100, 200 ms.
    for (i, (e, gap)) in saf.iter().zip([50u64, 100, 200]).enumerate() {
        let d = &e["details"];
        assert_eq!(d["attempt"], i as u64 + 1, "{e}");
        assert_eq!(d["backoff"], "in_activation", "{e}");
        assert_eq!(d["failure"]["kind"], "transient", "{e}");
        let at = d["retry_at_ms"].as_u64().expect("retry_at_ms");
        let ts = e["ts_ms"].as_u64().expect("ts_ms");
        assert_eq!(at - ts, gap, "attempt {} backoff: {e}", i + 1);
    }
    // Each retry ran no earlier than its journaled retry_at.
    for w in saf.windows(2) {
        let at = w[0]["details"]["retry_at_ms"].as_u64().expect("at");
        assert!(w[1]["ts_ms"].as_u64().expect("ts") >= at, "{w:?}");
    }
    let done = completions_of(&h, "flaky");
    assert_eq!(done.len(), 1, "{h:?}");
    assert_eq!(done[0]["details"]["outcome"]["error"]["kind"], "transient");
    assert_eq!(completions_of(&h, "before").len(), 1);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_recover_hot() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-retry").await;
    let id = start(
        &s,
        "sdk-retry",
        json!({"input": {"mode": "recover_hot", "fail_times": 2}}),
    )
    .await;
    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(
        run["result"],
        json!({"mode": "recover_hot", "before": 1, "attempts": 3})
    );
    let h = history(&s, &id).await;
    let saf = saf_of(&h, "recover");
    assert_eq!(saf.len(), 2, "{h:?}");
    assert!(
        saf.iter()
            .all(|e| e["details"]["backoff"] == "in_activation")
    );
    let done = completions_of(&h, "recover");
    assert_eq!(done.len(), 1);
    assert_eq!(done[0]["details"]["outcome"]["ok"], 3);
    // In-activation retries: one grant, one activation (no sleeps in this mode).
    assert_eq!(run["attempt"], 1, "{run}");
    let jobs = s.jobs_of(&run_id(&id));
    assert_eq!(jobs.len(), 1, "{jobs:?}");
    assert_eq!(jobs[0].token.n, 1);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_non_retryable() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-retry").await;
    let id = start(&s, "sdk-retry", json!({"input": {"mode": "non_retryable"}})).await;
    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "failed", "{run}");
    let h = history(&s, &id).await;
    assert!(of_kind(&h, "step_attempt_failed").is_empty(), "{h:?}");
    let done = completions_of(&h, "fatal");
    assert_eq!(done.len(), 1);
    assert_eq!(
        done[0]["details"]["outcome"]["error"]["kind"], "fatal",
        "{h:?}"
    );
    assert!(failure(&run).to_string().contains("fatal"), "{run}");
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retry_restart_between_attempts() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-retry").await;
    let id = start(&s, "sdk-retry", json!({"input": {"mode": "exhaust_slow"}})).await;
    // Attempt 1 fails and is requeued for 3 s; restart while it waits.
    let t0 = Instant::now();
    loop {
        let h = history(&s, &id).await;
        if !saf_of(&h, "flaky_slow").is_empty() {
            break;
        }
        assert!(t0.elapsed() < LIMIT, "no attempt failure: {h:?}");
        sleep(Duration::from_millis(20)).await;
    }
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;

    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "failed", "{run}");
    let h = history(&s, &id).await;
    // max_attempts 3: exactly 2 requeued failures + the final StepCompleted(Err).
    let saf = saf_of(&h, "flaky_slow");
    assert_eq!(saf.len(), 2, "{h:?}");
    for (i, e) in saf.iter().enumerate() {
        assert_eq!(e["details"]["attempt"], i as u64 + 1);
        assert_eq!(e["details"]["backoff"], "requeue");
    }
    let done = completions_of(&h, "flaky_slow");
    assert_eq!(done.len(), 1, "3 attempts in total: {h:?}");
    assert!(done[0]["details"]["outcome"]["error"].is_object());
    assert_eq!(
        completions_of(&h, "before").len(),
        1,
        "before replayed: {h:?}"
    );
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Replay after eviction (durable sleep longer than hot_wait)
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_after_eviction() {
    // sdk-basic's longest awaited sleep is 20 ms; a 1 ms hot-wait window evicts the run
    // on its sleeps and every resume replays the journal.
    let mut s = ready(
        TestServer::start_with(ServerOpts::default(), |c| {
            c.engine.hot_wait = FriendlyDuration::from_millis(1);
        })
        .await,
    )
    .await;
    deploy(&s, "sdk-basic").await;
    let id = start(&s, "sdk-basic", json!({"input": {"name": "evict", "n": 9}})).await;
    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(
        run["result"],
        json!({"greeting": "hello, evict", "sum": 45, "steps": 2})
    );
    let jobs = s.jobs_of(&run_id(&id));
    assert!(jobs.len() >= 2, "evicted and resumed: {jobs:?}");
    assert!(run["attempt"].as_u64().is_some_and(|n| n >= 2), "{run}");
    let h = history(&s, &id).await;
    assert_eq!(completions_of(&h, "greet").len(), 1, "{h:?}");
    assert_eq!(completions_of(&h, "add").len(), 1, "{h:?}");
    assert_eq!(of_kind(&h, "run_started").len(), 1);
    assert_eq!(of_kind(&h, "run_completed").len(), 1);
    assert!(!of_kind(&h, "timer_fired").is_empty());
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// WF-07: native SystemTime / HashMap / rand are deterministic across restart + replay
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_determinism() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-native").await;
    let id = start(&s, "sdk-native", json!({"input": null})).await;
    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "completed", "{run}");
    let rid = run_id(&id);
    let stored = s.result_bytes(&rid).await.expect("stored result");
    let out: Value = serde_json::from_slice(&stored).expect("result json");
    assert_eq!(out, run["result"]);
    assert_eq!(
        out["outside"]["map_order"].as_array().map(Vec::len),
        Some(50)
    );
    assert_eq!(
        out["inside"]["map_order"].as_array().map(Vec::len),
        Some(50)
    );

    // Forced restart, then re-execute the whole recorded journal on a fresh Runtime
    // (fresh engine, empty caches, different token key): identical output bytes.
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    let journal: Vec<Event> = s.journal(&rid).await;
    assert!(journal.len() >= 3, "{journal:?}");
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            token_key: [0x5a; 32],
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime");
    let p = rt
        .prepare_cached(&guest_wasm("sdk-native"))
        .await
        .expect("prepare");
    let rref = RunRef {
        partition: rid.partition().0,
        run_id: rid.to_string(),
    };
    let (outcome, appended) = replay_journal(&rt, &p, rref, journal).await;
    assert_eq!(appended, 0, "replay appends nothing");
    let ActivationOutcome::Completed { output } = outcome else {
        panic!("replay outcome {outcome:?}");
    };
    assert_eq!(
        output.data.as_slice(),
        stored.as_slice(),
        "byte-identical output"
    );

    // The server still serves the same result after the restart.
    let (_, again) = get_json(&s, &format!("/v1/runs/{id}")).await;
    assert_eq!(again["result"], out);
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// DISP-07: stalled executor -> re-dispatch, zombie append fenced
// ---------------------------------------------------------------------------------

/// What a zombie would flush: a step named `zombie` at `seq`.
fn zombie_event(seq: u64) -> Event {
    Event {
        seq,
        ts_nanos: 1_700_000_000_000_000_000,
        body: EventBody::StepCompleted(StepCompleted {
            meta: CommandMeta {
                hash_ver: 1,
                hash: CommandHash([0xee; 16]),
            },
            name: "zombie".into(),
            outcome: RecordedResult {
                codec_ver: 1,
                bytes: Vec::new(),
            },
            fuel_used: 0,
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_zombie() {
    let mut s = ready(
        TestServer::start_with(ServerOpts::default(), |c| {
            c.executor.lease_ttl = FriendlyDuration::from_millis(500);
            c.executor.heartbeat = Some(FriendlyDuration::from_millis(125));
            // One slot: attempt 2 runs only because the fenced zombie holds no slot.
            c.executor.slots = 1;
        })
        .await,
    )
    .await;
    deploy(&s, "sdk-basic").await;
    // Attempt 1 stalls: no heartbeat, every engine append blocked.
    let gate = Arc::new(Semaphore::new(0));
    s.hooks.only_attempt.store(1, Ordering::Release);
    s.hooks.suppress_heartbeat.store(true, Ordering::Release);
    *s.hooks.pause_before_append.lock().expect("lock") = Some(Arc::clone(&gate));

    let id = start(
        &s,
        "sdk-basic",
        json!({"input": {"name": "zombie", "n": 4}}),
    )
    .await;
    let rid = run_id(&id);
    let run = wait_terminal(&s, &id).await;
    assert_eq!(run["status"], "completed", "{run}");
    assert_eq!(
        run["result"],
        json!({"greeting": "hello, zombie", "sum": 10, "steps": 2})
    );
    let jobs = s.jobs_of(&rid);
    let zombie = jobs
        .iter()
        .find(|j| j.token.n == 1)
        .unwrap_or_else(|| panic!("attempt 1 recorded: {jobs:?}"));
    assert!(zombie.revoked || zombie.self_fenced, "{zombie:?}");
    assert!(
        matches!(zombie.activation, Some(ActivationOutcome::Evicted { .. })),
        "{zombie:?}"
    );
    assert!(
        jobs.iter().any(|j| j.token.n >= 2),
        "re-dispatched: {jobs:?}"
    );
    assert!(run["attempt"].as_u64().is_some_and(|n| n >= 2), "{run}");

    // The zombie's late append with its old token is fenced.
    let old = zombie.token;
    let zsink = PartitionSink::new(s.handle_of(&rid), rid, old, None, None);
    let end = zsink.next_seq();
    let rref = RunRef {
        partition: rid.partition().0,
        run_id: rid.to_string(),
    };
    let r = zsink
        .append(&rref, old.n, end, vec![zombie_event(end)])
        .await;
    assert_eq!(r, Err(AppendError::Fenced));
    // Release the stalled gate: nothing more is written for attempt 1.
    gate.add_permits(1_000);
    sleep(Duration::from_millis(200)).await;

    let h = history(&s, &id).await;
    assert!(
        h.iter().all(|e| e["details"]["name"] != "zombie"),
        "no zombie events: {h:?}"
    );
    assert_eq!(completions_of(&h, "greet").len(), 1, "{h:?}");
    assert_eq!(completions_of(&h, "add").len(), 1, "{h:?}");
    assert_eq!(of_kind(&h, "run_started").len(), 1);
    assert_eq!(
        of_kind(&h, "run_completed").len(),
        1,
        "completed exactly once"
    );
    assert_eq!(h.last().expect("events")["kind"], "run_completed");
    s.shutdown().await;
}

// ---------------------------------------------------------------------------------
// META-03: stale CAS through the server's meta handle
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn meta_stale_cas_rejected() {
    let mut s = ready(TestServer::start(ServerOpts::default()).await).await;
    deploy(&s, "sdk-basic").await;
    deploy(&s, "sdk-basic-v2").await;
    let (_, before) = get_json(&s, "/v1/workflows").await;
    let current = s
        .h()
        .meta()
        .view()
        .read(|st| st.active("sdk-basic").map(|v| v.version))
        .expect("active record");

    for stale in [current.wrapping_sub(1), current + 1] {
        let res = s
            .h()
            .meta()
            .propose(MetaCommand::Put {
                key: MetaKey::Active("sdk-basic".into()),
                expected_version: Some(stale),
                record: MetaRecord::Active(ActiveVersion {
                    name: "sdk-basic".into(),
                    version: 2,
                }),
            })
            .await
            .expect("propose");
        assert_eq!(res, MetaResult::StaleVersion { current }, "stale {stale}");
    }
    // A create of an existing key conflicts too.
    let res = s
        .h()
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Active("sdk-basic".into()),
            expected_version: None,
            record: MetaRecord::Active(ActiveVersion {
                name: "sdk-basic".into(),
                version: 2,
            }),
        })
        .await
        .expect("propose");
    assert!(matches!(res, MetaResult::Conflict { .. }), "{res:?}");
    let (_, after) = get_json(&s, "/v1/workflows").await;
    assert_eq!(after, before, "nothing changed");

    // The REST activation CAS (expected_version) is the same check: stale -> 412.
    let r = s
        .http
        .put(s.url("/v1/workflows/sdk-basic/active"))
        .json(&json!({"version": 2, "expected_version": current + 7}))
        .send()
        .await
        .expect("PUT active");
    assert_eq!(r.status(), StatusCode::PRECONDITION_FAILED);
    let (_, after) = get_json(&s, "/v1/workflows").await;
    assert_eq!(after, before);
    s.shutdown().await;
}
