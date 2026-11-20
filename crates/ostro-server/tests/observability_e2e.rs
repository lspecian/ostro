//! ROADMAP Phase 8 SC5 end to end (plan 08-21; OBS-01/02/03) on one server with the OTLP
//! collector and `/metrics`:
//!
//! - one `log-spammer` run started under the caller's `traceparent` yields
//!   `workflow.run` -> `activation` -> step spans in the caller's trace, the run span
//!   parented to the caller's span (it is evicted by durable sleeps, so it has several
//!   activations and replays);
//! - guest logs are bounded per run (the default 50/s, burst 500 bucket drops lines; the
//!   drops are on `/metrics`) and the collector holds **exactly one LogRecord per accepted
//!   line**, in the run's trace (W4: no bridged copy);
//! - `/metrics` carries the SC5 families: queue, lag, step, replay, fuel, memory and DLQ
//!   (a second run is dead-lettered by a nondeterministic replay);
//! - after a restart nothing is re-emitted: no second run, activation or step span and no
//!   extra LogRecord, while a fresh run after the restart is exported normally.
//!
//! One test function: the server installs the process-global subscriber and tracer
//! provider.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::phase8::Phase8Server;
use ostro_engine::telemetry::{KIND_RUN, KIND_STEP, span_id};
use ostro_partition::store::values::RunStatus;
use ostro_server::config::FriendlyDuration;
use ostro_testkit::otlp::{Collector, LogView, SpanView};
use ostro_types::RunId;
use ostro_types::journal::EventBody;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::sleep;

const WAIT: Duration = Duration::from_secs(60);
const ENGINE_SCOPE: &str = "ostro-engine";
const GUEST: &str = "log-spammer";
const CALLER_TRACE: &str = "0af7651916cd43dd8448eb211c80319c";
const CALLER_SPAN: &str = "b7ad6b7169203331";

/// The SC5 metric families (one series of each must be on `/metrics`).
const SC5_FAMILIES: &[(&str, &str)] = &[
    ("queue", "ostro_queue_ready"),
    ("queue", "ostro_queue_lag_seconds_bucket"),
    ("lag", "ostro_partition_apply_lag"),
    ("step", "ostro_steps_total"),
    ("step", "ostro_step_duration_seconds_bucket"),
    ("replay", "ostro_replay_events_total"),
    ("replay", "ostro_replay_duration_seconds_bucket"),
    ("fuel", "ostro_fuel_consumed_total"),
    ("memory", "ostro_instance_memory_bytes"),
    ("memory", "ostro_wasm_memory_budget_bytes"),
    ("dlq", "ostro_dlq_total"),
    ("guest logs", "ostro_guest_log_dropped_total"),
];

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

async fn scrape(s: &Phase8Server) -> String {
    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(r.status(), StatusCode::OK);
    r.text().await.expect("text")
}

fn has_family(body: &str, f: &str) -> bool {
    body.lines().any(|l| {
        l.strip_prefix(f)
            .is_some_and(|rest| rest.starts_with('{') || rest.starts_with(' '))
    })
}

/// Sum of the `name{..label..} value` samples.
fn metric(body: &str, name: &str, label: &str) -> f64 {
    body.lines()
        .filter(|l| l.starts_with(&format!("{name}{{")) && l.contains(label))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

async fn start(
    s: &Phase8Server,
    workflow: &str,
    input: Value,
    traceparent: Option<&str>,
) -> String {
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

fn spans_of(c: &Collector, run: &str) -> Vec<SpanView> {
    c.spans()
        .into_iter()
        .filter(|s| {
            s.scope == ENGINE_SCOPE
                && s.attributes.get("ostro.run_id").map(String::as_str) == Some(run)
        })
        .collect()
}

fn named(spans: &[SpanView], name: &str) -> usize {
    spans.iter().filter(|s| s.name == name).count()
}

fn steps(spans: &[SpanView]) -> Vec<&SpanView> {
    spans
        .iter()
        .filter(|s| {
            s.name != "activation" && s.name != "workflow.run" && !s.name.starts_with("effect:")
        })
        .collect()
}

fn logs_of(c: &Collector, tag: &str) -> Vec<LogView> {
    let prefix = format!("{tag} line ");
    c.logs()
        .into_iter()
        .filter(|l| l.body.starts_with(&prefix))
        .collect()
}

async fn step_seqs(s: &Phase8Server, run: &str) -> Vec<u64> {
    let id: RunId = run.parse().expect("run id");
    s.journal(&id)
        .await
        .iter()
        .filter(|e| matches!(e.body, EventBody::StepCompleted(_)))
        .map(|e| e.seq)
        .collect()
}

/// Span / log counts of one run, for the "nothing re-emitted" check.
#[derive(Debug, PartialEq, Eq)]
struct Counts {
    run: usize,
    activations: usize,
    steps: usize,
    logs: usize,
}

fn counts(c: &Collector, run: &str, tag: &str) -> Counts {
    let sp = spans_of(c, run);
    Counts {
        run: named(&sp, "workflow.run"),
        activations: named(&sp, "activation"),
        steps: steps(&sp).len(),
        logs: logs_of(c, tag).len(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sc5_traces_metrics_guest_logs_and_no_reemission() {
    let mut s = Phase8Server::builder()
        .with_telemetry_collector()
        .with_config(|c| {
            c.telemetry.service_name = "ostro-sc5".into();
            // Durable sleeps evict: the run re-activates and replays its journal.
            c.engine.hot_wait = FriendlyDuration::from_millis(50);
        })
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest(GUEST, None).await;
    let c = s.collector().clone();
    let before = scrape(&s).await;

    // ---- the SC5 run: 3 chunks of 400 lines, a step and an evicting sleep between them.
    let tp = format!("00-{CALLER_TRACE}-{CALLER_SPAN}-01");
    let t0 = Instant::now();
    let run = start(
        &s,
        GUEST,
        json!({"lines": 1_200, "burst": 400, "step_between": true, "sleep_ms": 200,
               "tag": "sc5", "fields": {"suite": "sc5"}}),
        Some(&tp),
    )
    .await;
    let rec = s.wait_run_status(&run, "completed", WAIT).await;
    let elapsed = t0.elapsed();
    assert_eq!(rec["traceparent"], tp.as_str(), "caller traceparent stored");

    // ---- a dead-lettered run (DLQ family): patch-v1 journals step A and a sleep; its
    // replay on patch-v3 diverges (nondeterminism, as in tests/metrics_endpoint.rs).
    s.deploy_guest("patch-v1", None).await;
    let v3 = s.deploy_guest("patch-v3", Some("beta")).await;
    let bad = s.start_rest("patch-demo", json!({"input": null})).await;
    s.wait_record(&bad, WAIT, |r| {
        r.next_seq > 0 && r.status == RunStatus::Scheduled
    })
    .await;
    s.hooks
        .module_override
        .set(Some(Arc::new(move |r: &RunId| (*r == bad).then_some(v3))));
    let bad_rec = s.wait_record(&bad, WAIT, |r| r.status.is_terminal()).await;
    s.hooks.module_override.set(None);
    assert_eq!(
        bad_rec.status,
        RunStatus::Failed,
        "the v3 replay must diverge"
    );

    // ---- guest logs: bounded per run, every accepted line exported exactly once.
    let st = s
        .h()
        .services()
        .guest_log_sink()
        .run_stats(&run)
        .expect("tracked");
    let accepted = st.accepted;
    assert!(
        st.dropped_run_rate > 0,
        "the run bucket must drop lines: {st:?}"
    );
    assert_eq!(
        st.accepted + st.dropped_run_rate + st.dropped_node_rate + st.dropped_run_cap,
        1_200,
        "{st:?}"
    );
    let bound = 500 + (50.0 * elapsed.as_secs_f64()).ceil() as u64 + 10;
    assert!(
        accepted <= bound,
        "burst 500 + 50/s over {elapsed:?}: accepted {accepted} > {bound}"
    );

    s.h().telemetry().force_flush().await;
    c.wait_for(
        |c| {
            logs_of(c, "sc5").len() as u64 >= accepted
                && named(&spans_of(c, &run), "workflow.run") == 1
                && named(&spans_of(c, &bad.to_string()), "workflow.run") == 1
        },
        WAIT,
    )
    .await;
    s.h().telemetry().force_flush().await;
    let trace = unhex(CALLER_TRACE);
    let logs = logs_of(&c, "sc5");
    assert_eq!(
        logs.len() as u64,
        accepted,
        "exactly one OTLP LogRecord per accepted guest line"
    );
    let mut seqs: Vec<u32> = logs
        .iter()
        .map(|l| l.attributes["seq"].parse().expect("seq"))
        .collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs.len() as u64, accepted, "no duplicated line");
    for l in &logs {
        assert_eq!(l.scope, "ostro.guest");
        assert_eq!(l.attributes.get("run.id"), Some(&run));
        assert_eq!(l.attributes.get("suite").map(String::as_str), Some("sc5"));
        assert_eq!(l.trace_id, trace, "guest log in the caller's trace");
    }

    // ---- span tree: workflow.run -> activation -> step, in the caller's trace.
    let spans = spans_of(&c, &run);
    for sp in &spans {
        assert_eq!(sp.trace_id, trace, "{} in the caller's trace", sp.name);
    }
    let root: Vec<&SpanView> = spans.iter().filter(|s| s.name == "workflow.run").collect();
    assert_eq!(root.len(), 1);
    let root = root[0];
    assert_eq!(root.span_id, span_id(&run, KIND_RUN, 0));
    assert_eq!(
        root.parent_span_id,
        unhex(CALLER_SPAN),
        "run span under the caller"
    );
    let acts: Vec<&SpanView> = spans.iter().filter(|s| s.name == "activation").collect();
    assert!(
        acts.len() >= 3,
        "evicted twice: >= 3 activations, got {}",
        acts.len()
    );
    assert!(acts.iter().all(|a| a.parent_span_id == root.span_id));
    let step_spans = steps(&spans);
    let journal_steps = step_seqs(&s, &run).await;
    assert_eq!(journal_steps.len(), 2, "mark(1), mark(2)");
    assert_eq!(step_spans.len(), journal_steps.len(), "one span per step");
    for st in &step_spans {
        let seq: u64 = st.attributes["ostro.seq"].parse().expect("seq");
        assert!(journal_steps.contains(&seq));
        assert_eq!(st.span_id, span_id(&run, KIND_STEP, seq));
        assert!(
            acts.iter().any(|a| a.span_id == st.parent_span_id),
            "step parent is an activation"
        );
    }
    // Guest log records point at the live activation spans.
    assert!(
        logs.iter()
            .all(|l| acts.iter().any(|a| a.span_id == l.span_id)),
        "every LogRecord carries one of the run's activation span ids"
    );
    println!(
        "SC5 spans: workflow.run=1 activation={} step={}; guest lines accepted={} dropped={} \
         LogRecords={}",
        acts.len(),
        step_spans.len(),
        accepted,
        st.dropped_run_rate,
        logs.len()
    );

    // ---- /metrics: the SC5 families (the poller and run consumers publish async).
    let t1 = Instant::now();
    let body = loop {
        let body = scrape(&s).await;
        let missing: Vec<&str> = SC5_FAMILIES
            .iter()
            .filter(|(_, f)| !has_family(&body, f))
            .map(|(_, f)| *f)
            .collect();
        if missing.is_empty() {
            break body;
        }
        assert!(t1.elapsed() < WAIT, "missing {missing:?}:\n{body}");
        sleep(Duration::from_millis(100)).await;
    };
    assert!(
        metric(&body, "ostro_dlq_total", "reason=\"nondeterminism\"") >= 1.0,
        "{body}"
    );
    let drops = metric(
        &body,
        "ostro_guest_log_dropped_total",
        "reason=\"run_rate\"",
    ) - metric(
        &before,
        "ostro_guest_log_dropped_total",
        "reason=\"run_rate\"",
    );
    assert!(
        drops >= st.dropped_run_rate as f64,
        "run_rate drops on /metrics: {drops} < {}",
        st.dropped_run_rate
    );
    assert!(
        metric(&body, "ostro_steps_total", "workflow=\"log-spammer\"") >= 2.0,
        "{body}"
    );
    assert!(!body.contains(&run), "a run id leaked into the scrape");

    // ---- restart: nothing is re-emitted.
    let pre = counts(&c, &run, "sc5");
    let pre_bad = named(&spans_of(&c, &bad.to_string()), "workflow.run");
    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    let fresh = start(&s, GUEST, json!({"lines": 3, "tag": "fresh"}), None).await;
    s.wait_run_status(&fresh, "completed", WAIT).await;
    s.h().telemetry().force_flush().await;
    c.wait_for(
        |c| named(&spans_of(c, &fresh), "workflow.run") == 1 && logs_of(c, "fresh").len() == 3,
        WAIT,
    )
    .await;
    s.h().telemetry().force_flush().await;
    assert_eq!(counts(&c, &run, "sc5"), pre, "re-emitted after restart");
    assert_eq!(
        named(&spans_of(&c, &bad.to_string()), "workflow.run"),
        pre_bad,
        "dead-lettered run span re-emitted"
    );
    println!("restart: {pre:?} unchanged; fresh run exported");

    s.shutdown().await;
}
