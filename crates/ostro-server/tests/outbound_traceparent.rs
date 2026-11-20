//! OBS-01 outbound propagation (plan 08-21, B2 / P8-D24): every outbound HTTP effect of
//! the fx-http guest carries a W3C `traceparent` whose trace id is the run's trace id and
//! whose parent is the deterministic effect span `span_id(run, "effect", op)`. It is added
//! by `TraceEffectHooks` to the **live** request copy only (the journaled
//! `EffectScheduled` intent is unchanged), the `effect:http` span reaches the OTLP
//! collector under the run's `workflow.run` span, a re-driven effect (server crashed while
//! the request was in flight) sends the very same header, a guest-supplied `traceparent`
//! wins, and a server without OTLP traces sends none.
//!
//! One test function: the server installs the process-global subscriber and tracer
//! provider.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{Response, StatusCode};
use common::fx::{configure, deploy_fx, wait_terminal};
use common::{BridgeTestOpts, TestServer, start_server_with_bridge, tmpfs_dir};
use ostro_engine::telemetry::{KIND_EFFECT, KIND_RUN, span_id, trace_id_for};
use ostro_testkit::otlp::{Collector, SpanView};
use ostro_types::RunId;
use ostro_types::journal::{EffectRequest, EventBody, encode_event};
use serde_json::{Value, json};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep};

const RUN_LIMIT: Duration = Duration::from_secs(60);
const WAIT: Duration = Duration::from_secs(30);
const ENGINE_SCOPE: &str = "ostro-engine";
const CALLER_TRACE: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const CALLER_SPAN: &str = "00f067aa0ba902b7";

// ---------------------------------------------------------------------------------
// Recording mock HTTP server
// ---------------------------------------------------------------------------------

#[derive(Default)]
struct MockState {
    /// `(path, traceparent header values)` per request, in arrival order.
    seen: Mutex<Vec<(String, Vec<String>)>>,
    /// Requests to `/hold` so far: the first one hangs (the server is crashed meanwhile).
    holds: AtomicUsize,
}

struct Mock {
    addr: SocketAddr,
    state: Arc<MockState>,
    task: JoinHandle<()>,
}

impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Mock {
    async fn start() -> Mock {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock");
        let addr = l.local_addr().expect("addr");
        let state = Arc::new(MockState::default());
        let app = Router::new().fallback(handle).with_state(state.clone());
        let task = tokio::spawn(async move {
            let _ = axum::serve(l, app).await;
        });
        Mock { addr, state, task }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn seen(&self, path: &str) -> Vec<Vec<String>> {
        self.state
            .seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(p, _)| p == path)
            .map(|(_, tp)| tp.clone())
            .collect()
    }

    async fn wait_seen(&self, path: &str, n: usize) {
        let deadline = Instant::now() + WAIT;
        while self.seen(path).len() < n {
            assert!(
                Instant::now() < deadline,
                "mock: {path} seen {} < {n}",
                self.seen(path).len()
            );
            sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn handle(State(st): State<Arc<MockState>>, req: Request) -> Response<Body> {
    let path = req.uri().path().to_owned();
    let tps: Vec<String> = req
        .headers()
        .get_all("traceparent")
        .iter()
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .collect();
    st.seen.lock().unwrap().push((path.clone(), tps));
    if path == "/hold" && st.holds.fetch_add(1, Ordering::SeqCst) == 0 {
        // Long enough for the test to crash the server while this request is in flight.
        sleep(Duration::from_secs(120)).await;
    }
    Response::builder()
        .status(StatusCode::OK)
        .body(Body::from("ok"))
        .expect("response")
}

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).expect("hex"))
        .collect()
}

fn get(url: String, headers: Value) -> Value {
    json!({ "method": "GET", "url": url, "headers": headers })
}

/// Start fx-http with `requests`, optionally under the caller's `traceparent`.
async fn start(s: &TestServer, requests: Vec<Value>, traceparent: Option<&str>) -> RunId {
    let mut req = s
        .http
        .post(s.url("/v1/workflows/fx-http/runs"))
        .json(&json!({ "input": { "requests": requests, "parallel": false } }));
    if let Some(tp) = traceparent {
        req = req.header("traceparent", tp);
    }
    let r = req.send().await.expect("POST run");
    let st = r.status();
    let body: Value = r.json().await.expect("json");
    assert_eq!(st, reqwest::StatusCode::CREATED, "{body}");
    body["run_id"]
        .as_str()
        .expect("run_id")
        .parse()
        .expect("run id")
}

async fn completed(s: &TestServer, run: &RunId) {
    let info = wait_terminal(s, run, RUN_LIMIT).await;
    let st = serde_json::to_value(info.status).expect("status");
    assert_eq!(st, "completed", "run {run}: {info:?}");
}

/// The ops (journal seqs) of the run's `EffectScheduled` events, the journaled request
/// header names per op, and every encoded `EffectScheduled` event's bytes.
async fn scheduled(s: &TestServer, run: &RunId) -> (Vec<u64>, Vec<Vec<String>>, Vec<u8>) {
    let mut ops = Vec::new();
    let mut names = Vec::new();
    let mut bytes = Vec::new();
    for ev in s.journal(run).await {
        if let EventBody::EffectScheduled(e) = &ev.body {
            ops.push(ev.seq);
            if let EffectRequest::Http(h) = &e.request {
                names.push(h.headers.iter().map(|h| h.name.clone()).collect());
            }
            bytes.extend_from_slice(&encode_event(&ev).1);
        }
    }
    (ops, names, bytes)
}

fn effect_spans(c: &Collector, run: &RunId) -> Vec<SpanView> {
    let run = run.to_string();
    c.spans()
        .into_iter()
        .filter(|s| {
            s.scope == ENGINE_SCOPE
                && s.name.starts_with("effect:")
                && s.attributes.get("ostro.run_id") == Some(&run)
        })
        .collect()
}

fn expected_tp(trace: &[u8], run: &RunId, op: u64) -> String {
    format!(
        "00-{}-{}-01",
        hex(trace),
        hex(&span_id(&run.to_string(), KIND_EFFECT, op))
    )
}

fn assert_no_traceparent_in(bytes: &[u8], what: &str) {
    let lower = String::from_utf8_lossy(bytes).to_ascii_lowercase();
    assert!(
        !lower.contains("traceparent"),
        "{what} carries a traceparent"
    );
    assert!(!bytes.is_empty(), "{what} is empty");
}

/// Check the `effect:http` span of `(run, op)`: one span, deterministic id, in `trace`,
/// parented under the run's `workflow.run` span id, outcome ok.
fn check_effect_span(c: &Collector, run: &RunId, op: u64, trace: &[u8]) {
    let rs = run.to_string();
    let spans: Vec<SpanView> = effect_spans(c, run)
        .into_iter()
        .filter(|s| s.attributes.get("ostro.op") == Some(&op.to_string()))
        .collect();
    assert_eq!(spans.len(), 1, "one effect span for op {op}: {spans:#?}");
    let sp = &spans[0];
    assert_eq!(sp.name, "effect:http");
    assert_eq!(sp.span_id, span_id(&rs, KIND_EFFECT, op));
    assert_eq!(sp.trace_id, trace, "effect span in the run's trace");
    assert_eq!(
        sp.parent_span_id,
        span_id(&rs, KIND_RUN, 0),
        "effect span parent = workflow.run"
    );
    assert_eq!(
        sp.attributes.get("ostro.outcome").map(String::as_str),
        Some("ok")
    );
}

// ---------------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outbound_traceparent_live_copy_effect_span_and_redrive() {
    let mock = Mock::start().await;
    let (addr, collector) = Collector::start().await;
    let mut s = start_server_with_bridge(
        tmpfs_dir(),
        BridgeTestOpts {
            extra_toml: format!(
                "[telemetry]\notlp_endpoint = \"http://{addr}\"\nservice_name = \"ostro-outbound-test\"\n"
            ),
            ..BridgeTestOpts::default()
        },
    )
    .await;
    let module = deploy_fx(&s, "fx-http").await;
    configure(&s)
        .egress(&module, &[&mock.base_url()])
        .apply()
        .await;

    // (a) caller traceparent: the mock gets the run trace + the effect span as parent.
    let tp = format!("00-{CALLER_TRACE}-{CALLER_SPAN}-01");
    let caller_trace = unhex(CALLER_TRACE);
    let a = start(
        &s,
        vec![
            get(format!("{}/rec", mock.base_url()), json!([["x-req", "1"]])),
            get(format!("{}/rec", mock.base_url()), json!([])),
        ],
        Some(&tp),
    )
    .await;
    completed(&s, &a).await;
    let (ops_a, names_a, bytes_a) = scheduled(&s, &a).await;
    assert_eq!(ops_a.len(), 2, "two effects");
    let got = mock.seen("/rec");
    assert_eq!(got.len(), 2, "{got:?}");
    for (i, op) in ops_a.iter().enumerate() {
        assert_eq!(
            got[i],
            vec![expected_tp(&caller_trace, &a, *op)],
            "request {i}: trace id = caller trace, parent = effect span of op {op}"
        );
    }
    // The journaled intents are the guest's requests: no traceparent anywhere.
    assert_eq!(
        names_a,
        vec![vec!["x-req".to_owned()], Vec::<String>::new()]
    );
    assert_no_traceparent_in(&bytes_a, "run a's EffectScheduled bytes");

    // (b) no caller traceparent: the trace id is derived from the run id.
    let b = start(
        &s,
        vec![get(format!("{}/derived", mock.base_url()), json!([]))],
        None,
    )
    .await;
    completed(&s, &b).await;
    let (ops_b, _, _) = scheduled(&s, &b).await;
    let derived = trace_id_for(&b.to_string(), None);
    assert_eq!(
        mock.seen("/derived"),
        vec![vec![expected_tp(&derived, &b, ops_b[0])]]
    );

    // (c) a guest-supplied traceparent wins (sent verbatim, exactly once).
    let mine = "00-11111111111111111111111111111111-2222222222222222-01";
    let c = start(
        &s,
        vec![get(
            format!("{}/own", mock.base_url()),
            json!([["traceparent", mine]]),
        )],
        Some(&tp),
    )
    .await;
    completed(&s, &c).await;
    let (ops_c, _, _) = scheduled(&s, &c).await;
    assert_eq!(mock.seen("/own"), vec![vec![mine.to_owned()]]);

    // Effect spans: one per effect, deterministic ids, under workflow.run.
    s.h().telemetry().force_flush().await;
    let want = ops_a.len() + ops_b.len() + ops_c.len();
    collector
        .wait_for(
            |col| {
                effect_spans(col, &a).len()
                    + effect_spans(col, &b).len()
                    + effect_spans(col, &c).len()
                    == want
            },
            WAIT,
        )
        .await;
    for op in &ops_a {
        check_effect_span(&collector, &a, *op, &caller_trace);
    }
    check_effect_span(&collector, &b, ops_b[0], &derived);
    check_effect_span(&collector, &c, ops_c[0], &caller_trace);
    println!(
        "effect spans: a={} b={} c={}",
        effect_spans(&collector, &a).len(),
        effect_spans(&collector, &b).len(),
        effect_spans(&collector, &c).len()
    );

    // (d) re-drive: crash while `/hold` is in flight; after the restart the effect is
    // re-sent with the same traceparent and completes; one effect span.
    let d = start(
        &s,
        vec![get(format!("{}/hold", mock.base_url()), json!([]))],
        Some(&tp),
    )
    .await;
    mock.wait_seen("/hold", 1).await;
    s.crash().await;
    s.restart().await;
    s.wait_ready(WAIT).await;
    mock.wait_seen("/hold", 2).await;
    completed(&s, &d).await;
    let (ops_d, _, bytes_d) = scheduled(&s, &d).await;
    assert_eq!(ops_d.len(), 1, "one intent, re-driven (not re-scheduled)");
    let holds = mock.seen("/hold");
    let want_d = vec![expected_tp(&caller_trace, &d, ops_d[0])];
    assert!(holds.len() >= 2, "{holds:?}");
    for h in &holds {
        assert_eq!(h, &want_d, "every send of the re-driven effect: {holds:?}");
    }
    assert_no_traceparent_in(&bytes_d, "run d's EffectScheduled bytes");
    s.h().telemetry().force_flush().await;
    collector
        .wait_for(|col| !effect_spans(col, &d).is_empty(), WAIT)
        .await;
    check_effect_span(&collector, &d, ops_d[0], &caller_trace);
    println!(
        "re-drive: {} sends of /hold, identical traceparent {}",
        holds.len(),
        want_d[0]
    );
    s.shutdown().await;

    // (e) telemetry off: the no-op hooks send the journaled request unchanged.
    let off = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let module = deploy_fx(&off, "fx-http").await;
    configure(&off)
        .egress(&module, &[&mock.base_url()])
        .apply()
        .await;
    let e = start(
        &off,
        vec![get(
            format!("{}/off", mock.base_url()),
            json!([["x-req", "1"]]),
        )],
        Some(&tp),
    )
    .await;
    completed(&off, &e).await;
    let (_, names_e, bytes_e) = scheduled(&off, &e).await;
    assert_eq!(mock.seen("/off"), vec![Vec::<String>::new()]);
    assert_eq!(names_e, vec![vec!["x-req".to_owned()]]);
    assert_no_traceparent_in(&bytes_e, "run e's EffectScheduled bytes");
    let mut off = off;
    off.shutdown().await;

    let by_path: HashMap<String, usize> =
        mock.state
            .seen
            .lock()
            .unwrap()
            .iter()
            .fold(HashMap::new(), |mut m, (p, _)| {
                *m.entry(p.clone()).or_default() += 1;
                m
            });
    println!("BRIDGE_TRACEPARENT_LEG: wired; mock requests by path: {by_path:?}");
}
