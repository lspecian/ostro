//! OBS-01 run traces end to end (plan 08-11): `workflow.run` -> `activation` -> `step`
//! spans reach the OTLP collector in the caller's trace (or the derived one), are never
//! re-emitted for replayed steps or after a restart, and the outbound `traceparent`
//! helper round-trips through `TraceContextPropagator`.
//!
//! One test function: the server installs the process-global subscriber and tracer
//! provider.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use common::{ServerOpts, TestServer, guest_wasm};
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use ostro_engine::telemetry::{
    KIND_EFFECT, KIND_RUN, KIND_STEP, effect_span_context, span_id, trace_id_for,
    traceparent_headers,
};
use ostro_server::config::FriendlyDuration;
use ostro_testkit::otlp::{Collector, SpanView};
use ostro_types::RunId;
use ostro_types::journal::EventBody;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::time::sleep;

const WAIT: Duration = Duration::from_secs(30);
const ENGINE_SCOPE: &str = "ostro-engine";
const CALLER_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const CALLER_SPAN: &str = "00f067aa0ba902b7";

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

async fn deploy(s: &TestServer, guest: &str) {
    let wasm = guest_wasm(guest);
    let sha: String = hex(&Sha256::digest(&wasm));
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

async fn start(s: &TestServer, workflow: &str, input: Value, traceparent: Option<&str>) -> String {
    let mut req = s
        .http
        .post(s.url(&format!("/v1/workflows/{workflow}/runs")))
        .json(&json!({ "input": input }));
    if let Some(tp) = traceparent {
        req = req.header("traceparent", tp);
    }
    let r = req.send().await.expect("POST run");
    let st = r.status();
    let body: Value = r.json().await.expect("json");
    assert_eq!(st, StatusCode::CREATED, "{body}");
    body["run_id"].as_str().expect("run_id").to_owned()
}

async fn wait_completed(s: &TestServer, run_id: &str) -> Value {
    let t0 = Instant::now();
    loop {
        let r = s
            .http
            .get(s.url(&format!("/v1/runs/{run_id}")))
            .send()
            .await
            .expect("GET run");
        let body: Value = r.json().await.expect("json");
        match body["status"].as_str() {
            Some("completed") => return body,
            Some("failed" | "cancelled") => panic!("run {run_id} ended: {body}"),
            _ => {}
        }
        assert!(t0.elapsed() < WAIT, "run {run_id} not completed: {body}");
        sleep(Duration::from_millis(20)).await;
    }
}

/// Engine / run-span spans of `run` (the node-internal `tracing` spans have another scope).
fn spans_of(c: &Collector, run: &str) -> Vec<SpanView> {
    c.spans()
        .into_iter()
        .filter(|s| {
            s.scope == ENGINE_SCOPE
                && s.attributes.get("ostro.run_id").map(String::as_str) == Some(run)
        })
        .collect()
}

fn run_spans(c: &Collector, run: &str) -> usize {
    spans_of(c, run)
        .iter()
        .filter(|s| s.name == "workflow.run")
        .count()
}

async fn step_seqs(s: &TestServer, run: &str) -> Vec<u64> {
    let id: RunId = run.parse().expect("run id");
    s.journal(&id)
        .await
        .iter()
        .filter(|e| matches!(e.body, EventBody::StepCompleted(_)))
        .map(|e| e.seq)
        .collect()
}

/// Checks the run -> activation -> step tree of `run` in trace `trace`; returns the
/// activation span count.
async fn check_tree(
    s: &TestServer,
    c: &Collector,
    run: &str,
    trace: &[u8],
    caller: Option<&[u8]>,
) -> usize {
    let spans = spans_of(c, run);
    for sp in &spans {
        assert_eq!(sp.trace_id, trace, "{} in the run's trace", sp.name);
    }
    let roots: Vec<&SpanView> = spans.iter().filter(|s| s.name == "workflow.run").collect();
    assert_eq!(roots.len(), 1, "one workflow.run: {spans:#?}");
    let root = roots[0];
    assert_eq!(root.span_id, span_id(run, KIND_RUN, 0));
    assert_eq!(
        root.parent_span_id,
        caller.unwrap_or(&[]),
        "run span parent"
    );
    assert_eq!(
        root.attributes.get("ostro.status").map(String::as_str),
        Some("completed")
    );
    let acts: Vec<&SpanView> = spans.iter().filter(|s| s.name == "activation").collect();
    assert!(!acts.is_empty(), "{spans:#?}");
    for a in &acts {
        assert_eq!(
            a.parent_span_id, root.span_id,
            "activation's parent is the run"
        );
    }
    let mut ids = acts.iter().map(|a| a.span_id.clone()).collect::<Vec<_>>();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), acts.len(), "activation ids are distinct");
    let steps: Vec<&SpanView> = spans
        .iter()
        .filter(|s| s.name != "activation" && s.name != "workflow.run")
        .collect();
    let seqs = step_seqs(s, run).await;
    assert!(!seqs.is_empty());
    assert_eq!(
        steps.len(),
        seqs.len(),
        "exactly one span per (run, seq): {steps:#?}"
    );
    let mut by_seq: BTreeMap<u64, &SpanView> = BTreeMap::new();
    for st in &steps {
        let seq: u64 = st.attributes["ostro.seq"].parse().expect("seq");
        assert!(
            by_seq.insert(seq, st).is_none(),
            "duplicate step span {seq}"
        );
        assert_eq!(st.span_id, span_id(run, KIND_STEP, seq));
        assert!(
            acts.iter().any(|a| a.span_id == st.parent_span_id),
            "step's parent is an activation"
        );
    }
    assert_eq!(by_seq.keys().copied().collect::<Vec<_>>(), seqs);
    acts.len()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_trace_tree_no_reemission_restart_and_propagation() {
    let (addr, collector) = Collector::start().await;
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.telemetry.otlp_endpoint = Some(format!("http://{addr}"));
        c.telemetry.service_name = "ostro-trace-test".into();
        // Every durable sleep evicts: the sleeper re-activates and replays its steps.
        c.engine.hot_wait = FriendlyDuration::from_millis(1);
    })
    .await;
    s.wait_ready(Duration::from_secs(5)).await;
    deploy(&s, "trivial-steps").await;
    deploy(&s, "sdk-basic").await;

    // (a) caller traceparent, (b) none, (c) a sleeper evicted and re-activated.
    let tp = format!("00-{CALLER_TRACE}-{CALLER_SPAN}-01");
    let a = start(&s, "trivial-steps", json!({"n": 3}), Some(&tp)).await;
    let b = start(&s, "trivial-steps", json!({"n": 2}), None).await;
    let c = start(&s, "sdk-basic", json!({"name": "t", "n": 3}), None).await;
    let rec_a = wait_completed(&s, &a).await;
    assert_eq!(rec_a["traceparent"], tp.as_str(), "header stored");
    wait_completed(&s, &b).await;
    wait_completed(&s, &c).await;

    s.h().telemetry().force_flush().await;
    for run in [&a, &b, &c] {
        collector
            .wait_for(|col| run_spans(col, run) == 1, WAIT)
            .await;
    }
    // The run span is the last one emitted per run; flush once more for stragglers.
    s.h().telemetry().force_flush().await;

    let caller_trace: Vec<u8> = (0..16)
        .map(|i| u8::from_str_radix(&CALLER_TRACE[2 * i..2 * i + 2], 16).expect("hex"))
        .collect();
    let caller_span: Vec<u8> = (0..8)
        .map(|i| u8::from_str_radix(&CALLER_SPAN[2 * i..2 * i + 2], 16).expect("hex"))
        .collect();
    let acts_a = check_tree(&s, &collector, &a, &caller_trace, Some(&caller_span)).await;
    assert_eq!(acts_a, 1, "trivial-steps completes in one activation");
    check_tree(&s, &collector, &b, &trace_id_for(&b, None), None).await;
    let acts_c = check_tree(&s, &collector, &c, &trace_id_for(&c, None), None).await;
    assert!(
        acts_c >= 2,
        "the sleeper was evicted and re-activated ({acts_c} activations)"
    );

    // (d) restart on the same data dir: no second workflow.run for a completed run.
    s.restart().await;
    s.wait_ready(Duration::from_secs(5)).await;
    let d = start(&s, "trivial-steps", json!({"n": 1}), None).await;
    wait_completed(&s, &d).await;
    s.h().telemetry().force_flush().await;
    collector
        .wait_for(|col| run_spans(col, &d) == 1, WAIT)
        .await;
    for run in [&a, &b, &c] {
        assert_eq!(
            run_spans(&collector, run),
            1,
            "{run} re-emitted after restart"
        );
        let steps = spans_of(&collector, run)
            .into_iter()
            .filter(|s| s.name != "activation" && s.name != "workflow.run")
            .count();
        assert_eq!(
            steps,
            step_seqs(&s, run).await.len(),
            "{run} steps re-emitted"
        );
    }

    // (e) outbound propagation: the effect span context's headers round-trip.
    let cx = effect_span_context(&a, Some(&tp), 7);
    let headers: HashMap<String, String> = traceparent_headers(&cx).into_iter().collect();
    let got = TraceContextPropagator::new().extract(&headers);
    let sc = got.span().span_context().clone();
    assert!(sc.is_valid() && sc.is_remote(), "{headers:?}");
    assert_eq!(hex(&sc.trace_id().to_bytes()), CALLER_TRACE);
    assert_eq!(sc.span_id().to_bytes(), span_id(&a, KIND_EFFECT, 7));
    assert!(sc.is_sampled());
    assert_eq!(
        headers.get("traceparent").map(String::as_str),
        Some(format!("00-{CALLER_TRACE}-{}-01", hex(&span_id(&a, KIND_EFFECT, 7))).as_str())
    );

    s.shutdown().await;
}
