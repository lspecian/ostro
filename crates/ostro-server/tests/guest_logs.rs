//! OBS-03 guest logs end to end (plan 08-17, P8-D16), with the `log-spammer` guest:
//!
//! - (a) per-run lifetime cap: 20,000 lines requested -> exactly 10,000 accepted, the rest
//!   dropped as `run_cap` and counted in `/metrics`;
//! - (b) burst control: within the first second at most 500 + epsilon lines are accepted;
//! - (c) `GET /v1/runs/{id}/logs?limit=100` returns the last 100 accepted lines in order;
//! - (d) a restart mid-run: the replayed part emits nothing (the collector's count for
//!   the lines before the resume point does not grow) and the per-activation bound
//!   (1000 lines) still holds;
//! - (e) `LogFormat::Json`: the node log line parses as JSON with the guest fields;
//! - (f) the OTLP LogRecord carries `run.id`, `workflow`, `attempt`, the guest fields and
//!   the run's trace id (08-11) with the activation's span id;
//! - (g) exactly one OTLP LogRecord per accepted line (the appender bridge excludes
//!   `ostro::guest`, W4).
//!
//! Only `guest_logs_otlp_restart_and_exactly_once` starts an OTLP collector (it installs
//! the process-global subscriber); the others use servers without OTLP export.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::phase8::Phase8Server;
use ostro_engine::sink::GuestLogRecord;
use ostro_engine::telemetry::trace_id_for;
use ostro_server::config::{FriendlyDuration, GuestLogsSection, LogFormat};
use ostro_server::services::guest_logs::{GuestLogSink, Verdict};
use ostro_testkit::otlp::{Collector, LogView};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::sleep;
use tracing_subscriber::layer::SubscriberExt as _;

const WAIT: Duration = Duration::from_secs(60);
const GUEST: &str = "log-spammer";

fn unlimited(g: &mut GuestLogsSection) {
    g.rate_per_sec = 1_000_000;
    g.burst = 1_000_000;
    g.node_rate_per_sec = 10_000_000;
}

fn evicting(c: &mut ostro_server::config::ServerConfig) {
    // Every durable sleep evicts: each chunk runs in its own activation.
    c.engine.hot_wait = FriendlyDuration::from_millis(1);
}

async fn metrics_text(s: &Phase8Server) -> String {
    let r = s
        .http
        .get(s.url("/metrics"))
        .send()
        .await
        .expect("GET /metrics");
    assert_eq!(r.status(), StatusCode::OK);
    r.text().await.expect("text")
}

/// Sum of `name{..label..} value` samples.
fn metric(text: &str, name: &str, label: &str) -> f64 {
    text.lines()
        .filter(|l| l.starts_with(&format!("{name}{{")) && l.contains(label))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

async fn spam(s: &Phase8Server, input: Value) -> String {
    s.start_rest(GUEST, json!({ "input": input }))
        .await
        .to_string()
}

fn sink(s: &Phase8Server) -> Arc<GuestLogSink> {
    s.h().services().guest_log_sink()
}

fn logs_of(c: &Collector, tag: &str) -> Vec<LogView> {
    let prefix = format!("{tag} line ");
    c.logs()
        .into_iter()
        .filter(|l| l.body.starts_with(&prefix))
        .collect()
}

fn seq(l: &LogView) -> u32 {
    l.attributes["seq"].parse().expect("seq attribute")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guest_logs_cap_ring_endpoint_and_metrics() {
    let mut s = Phase8Server::builder()
        .with_guest_logs(unlimited)
        .with_config(evicting)
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest(GUEST, None).await;
    let before = metrics_text(&s).await;

    // (a) 20,000 lines in 20 activations of 1,000 (the per-activation bound) each.
    let run = spam(
        &s,
        json!({"lines": 20_000, "burst": 1_000, "sleep_ms": 50, "tag": "cap", "level": "debug",
               "fields": {"suite": "a"}}),
    )
    .await;
    s.wait_run_status(&run, "completed", WAIT).await;
    let st = sink(&s).run_stats(&run).expect("tracked");
    assert_eq!(st.accepted, 10_000, "lifetime line cap: {st:?}");
    assert_eq!(st.dropped_run_cap, 10_000, "{st:?}");
    assert_eq!(st.dropped_run_rate + st.dropped_node_rate, 0, "{st:?}");

    let after = metrics_text(&s).await;
    let d = |name: &str, label: &str| metric(&after, name, label) - metric(&before, name, label);
    assert!(
        d("ostro_guest_log_dropped_total", "reason=\"run_cap\"") >= 10_000.0,
        "run_cap drops in /metrics:\n{after}"
    );
    assert!(d("ostro_guest_log_lines_total", "level=\"debug\"") >= 10_000.0);

    // (c) the last 100 accepted lines (seq 9900..9999), oldest first.
    let (code, v) = s.get_json(&format!("/v1/runs/{run}/logs?limit=100")).await;
    assert_eq!(code, StatusCode::OK, "{v}");
    let lines = v["lines"].as_array().expect("lines");
    assert_eq!(lines.len(), 100);
    for (i, l) in lines.iter().enumerate() {
        let n = 9_900 + i;
        assert_eq!(l["message"], format!("cap line {n}"));
        assert_eq!(l["fields"]["seq"], n.to_string());
        assert_eq!(l["fields"]["suite"], "a");
        assert_eq!(l["level"], "debug");
    }
    assert_eq!(v["dropped"], 10_000);
    // limit is capped at 1000 (the ring size); a level filter above debug is empty.
    let (_, v) = s.get_json(&format!("/v1/runs/{run}/logs?limit=1000")).await;
    assert_eq!(v["lines"].as_array().map(Vec::len), Some(1_000));
    let (_, v) = s.get_json(&format!("/v1/runs/{run}/logs?level=info")).await;
    assert_eq!(v["lines"], json!([]));
    // Unknown run: best effort, empty, not 404.
    let (code, v) = s.get_json("/v1/runs/0000-unknown/logs?limit=10").await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(v["lines"], json!([]));

    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guest_logs_burst_control() {
    // Default limits: 50 lines/s, burst 500.
    let mut s = Phase8Server::builder().start().await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest(GUEST, None).await;
    let t0 = Instant::now();
    let run = spam(
        &s,
        json!({"lines": 2_000, "tag": "burst", "level": "debug"}),
    )
    .await;
    s.wait_run_status(&run, "completed", WAIT).await;
    let elapsed = t0.elapsed();
    let st = sink(&s).run_stats(&run).expect("tracked");
    eprintln!("burst: {st:?} (run took {elapsed:?})");
    // 1,000 lines reach the sink (activation bound); the bucket admits its burst plus the
    // refill during the emission (well under a second).
    let eps = 50;
    assert!(
        (500..=500 + eps).contains(&st.accepted),
        "accepted {} in {elapsed:?}: {st:?}",
        st.accepted
    );
    assert_eq!(st.accepted + st.dropped_run_rate, 1_000, "{st:?}");
    let (_, v) = s.get_json(&format!("/v1/runs/{run}/logs?limit=1000")).await;
    assert_eq!(
        v["lines"].as_array().map(Vec::len),
        Some(st.accepted as usize)
    );
    assert_eq!(v["dropped"], st.dropped_run_rate);
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guest_logs_otlp_restart_and_exactly_once() {
    let mut s = Phase8Server::builder()
        .with_telemetry_collector()
        .with_guest_logs(unlimited)
        .with_config(evicting)
        .start()
        .await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.deploy_guest(GUEST, None).await;
    let c = s.collector().clone();

    // (f) + (g): 50 lines, all accepted.
    let a = spam(
        &s,
        json!({"lines": 50, "tag": "otlp-a", "fields": {"order": "42", "region": "eu"}}),
    )
    .await;
    s.wait_run_status(&a, "completed", WAIT).await;
    s.h().telemetry().force_flush().await;
    c.wait_for(|c| logs_of(c, "otlp-a").len() >= 50, WAIT).await;
    s.h().telemetry().force_flush().await;
    let accepted = sink(&s).run_stats(&a).expect("tracked").accepted;
    assert_eq!(accepted, 50);
    let got = logs_of(&c, "otlp-a");
    assert_eq!(
        got.len() as u64,
        accepted,
        "exactly one OTLP LogRecord per accepted line (no bridged copy): {got:#?}"
    );
    let trace = trace_id_for(&a, None).to_vec();
    let mut seqs: Vec<u32> = got.iter().map(seq).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (0..50).collect::<Vec<_>>());
    for l in &got {
        assert_eq!(l.scope, "ostro.guest");
        assert_eq!(l.severity_text, "INFO");
        assert_eq!(l.attributes.get("run.id"), Some(&a));
        assert_eq!(
            l.attributes.get("workflow").map(String::as_str),
            Some(GUEST)
        );
        assert!(l.attributes.contains_key("attempt"));
        assert_eq!(l.attributes.get("order").map(String::as_str), Some("42"));
        assert_eq!(l.attributes.get("region").map(String::as_str), Some("eu"));
        assert_eq!(l.attributes.get("chunk").map(String::as_str), Some("0"));
        assert_eq!(l.trace_id, trace, "the run's trace id");
        assert_eq!(l.span_id.len(), 8, "activation span id");
        assert!(l.span_id.iter().any(|b| *b != 0));
        assert_eq!(
            l.resource.get("service.name").map(String::as_str),
            Some("ostro")
        );
    }
    let with_run = c
        .logs()
        .into_iter()
        .filter(|l| l.attributes.get("run.id") == Some(&a))
        .count();
    assert_eq!(with_run, 50);

    // (d) restart mid-run: chunk 0 (seq 0..1499) in activation 1, a 5 s durable sleep,
    // chunk 1 (seq 1500..2999) after the restart.
    let b = spam(
        &s,
        json!({"lines": 3_000, "burst": 1_500, "sleep_ms": 5_000, "tag": "otlp-b",
               "level": "debug"}),
    )
    .await;
    let t0 = Instant::now();
    while sink(&s).run_stats(&b).map(|x| x.accepted) != Some(1_000) {
        assert!(t0.elapsed() < WAIT, "chunk 0 not emitted");
        sleep(Duration::from_millis(10)).await;
    }
    s.h().telemetry().force_flush().await;
    c.wait_for(|c| logs_of(c, "otlp-b").len() >= 1_000, WAIT)
        .await;
    let pre: Vec<LogView> = logs_of(&c, "otlp-b");
    assert_eq!(pre.len(), 1_000, "activation bound: 1000 of chunk 0's 1500");
    assert!(pre.iter().all(|l| seq(l) < 1_500));
    assert!(pre.iter().all(|l| l.severity_text == "DEBUG"));
    let (_, v) = s.get_json(&format!("/v1/runs/{b}")).await;
    assert_ne!(
        v["status"], "completed",
        "restart must land inside the sleep: {v}"
    );

    s.restart().await;
    s.wait_ready(Duration::from_secs(10)).await;
    s.wait_run_status(&b, "completed", WAIT).await;
    // The new node's sink saw only the live chunk 1 (the replayed chunk 0 emitted
    // nothing), bounded per activation.
    let st = sink(&s).run_stats(&b).expect("tracked after restart");
    assert_eq!(st.accepted, 1_000, "{st:?}");
    s.h().telemetry().force_flush().await;
    c.wait_for(
        |c| {
            logs_of(c, "otlp-b")
                .iter()
                .filter(|l| seq(l) >= 1_500)
                .count()
                >= 1_000
        },
        WAIT,
    )
    .await;
    s.h().telemetry().force_flush().await;
    let all = logs_of(&c, "otlp-b");
    let before_resume = all.iter().filter(|l| seq(l) < 1_500).count();
    let after_resume = all.iter().filter(|l| seq(l) >= 1_500).count();
    assert_eq!(before_resume, 1_000, "replay re-emitted lines");
    assert_eq!(after_resume, 1_000, "activation bound after restart");
    assert!(all.iter().all(|l| l.attributes.get("run.id") == Some(&b)));
    // Logs are never journaled: the journal has no trace of the lines.
    let id = b.parse().expect("run id");
    let journal = s.journal(&id).await;
    assert!(journal.len() < 20, "journal: {} events", journal.len());

    s.shutdown().await;
}

/// A `MakeWriter` that captures everything.
#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().expect("lock").extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[test]
fn guest_logs_json_node_line() {
    let cap = Capture::default();
    let subscriber = tracing_subscriber::registry().with(ostro_server::logging::fmt_layer(
        LogFormat::Json,
        cap.clone(),
    ));
    let s = GuestLogSink::new(&GuestLogsSection::default());
    tracing::subscriber::with_default(subscriber, || {
        let v = s.emit(GuestLogRecord {
            run_id: "run-json".into(),
            workflow: "wf".into(),
            attempt: 3,
            level: "warn",
            message: "payment failed".into(),
            fields: vec![
                ("order".into(), "42".into()),
                ("reason".into(), "card \"declined\"".into()),
            ],
            ts_ms: 1,
            trace_id: None,
            span_id: None,
        });
        assert_eq!(v, Verdict::Accepted);
    });
    let out = String::from_utf8(cap.0.lock().expect("lock").clone()).expect("utf8");
    let line = out
        .lines()
        .find(|l| l.contains("payment failed"))
        .unwrap_or_else(|| panic!("no guest line in {out}"));
    let v: Value = serde_json::from_str(line).expect("node log line is JSON");
    assert_eq!(v["target"], "ostro::guest");
    assert_eq!(v["level"], "WARN");
    let f = &v["fields"];
    assert_eq!(f["message"], "payment failed");
    assert_eq!(f["run_id"], "run-json");
    assert_eq!(f["workflow"], "wf");
    assert_eq!(f["attempt"], 3);
    // Guest keys are dynamic, so they travel as one JSON object string.
    let guest: Value =
        serde_json::from_str(f["guest.fields"].as_str().expect("guest.fields string"))
            .expect("guest.fields is a JSON object");
    assert_eq!(guest, json!({"order": "42", "reason": "card \"declined\""}));
}
