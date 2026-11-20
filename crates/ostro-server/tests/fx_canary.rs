//! ROADMAP Phase 6 SC3 end to end (plan 06-19): a canary secret used as
//! `{{secret:canary-key}}` by the fx-canary guest never shows up anywhere it must not.
//!
//! Every test installs [`CaptureLayer`] as the process-wide subscriber first (TRACE, no
//! filter, `RUST_LOG` ignored), so everything the server under test logs is scanned. The
//! mock provider legitimately receives the canary; it runs through [`spawn_quiet`] (own OS
//! thread, own runtime, thread-local `NoSubscriber`), so it never feeds the capture (W6).
//!
//! Scanned for every needle of the canary ([`Canary::needles`]): every REST response body
//! the test received, the gRPC run/history/list responses, the guest output (observed
//! response, env, args, random), the decoded journal (REST history and the raw encoded
//! events), the captured tracing output, every registered sink, and after a clean shutdown
//! every file of the data dir (WAL, state, meta, blobs) and of the object-store root.
//!
//! The OTLP leg is closed at the Phase 8 gate (D-FX6): Phase 8 registers its mock collector
//! with `canary::register_sink`, and [`Report::sinks`] scans it here.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Response, StatusCode};
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use common::fx::{deploy_fx, seg, wait_terminal};
use common::{BridgeTestOpts, TestServer, start_server_with_bridge, tmpfs_dir};
use ostro_proto::api::v1 as pb;
use ostro_testkit::canary::{Canary, CaptureLayer, Quiet, Report, spawn_quiet};
use ostro_types::RunId;
use ostro_types::journal::{EventBody, Failure, OpOutput, decode_result, encode_event};
use serde_json::{Value, json};
use tokio::task::JoinHandle;

const RUN_LIMIT: Duration = Duration::from_secs(60);
const OTHER_ORIGIN: &str = "https://elsewhere.test";

// ---------------------------------------------------------------------------------
// Recording mock provider (runs under spawn_quiet)
// ---------------------------------------------------------------------------------

#[derive(Default)]
struct MockState {
    total: AtomicUsize,
    by_path: Mutex<HashMap<String, usize>>,
    /// Every `X-Api-Key` value received, verbatim.
    api_keys: Mutex<Vec<Vec<u8>>>,
    /// Every request body received.
    bodies: Mutex<Vec<Vec<u8>>>,
}

/// `/echo` (MockProvider shape), `/enc?as=<form>` (returns the received `X-Api-Key`
/// value encoded, in the body and in `x-echo`), `/redirect?to=<url>` (302).
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

    fn total_requests(&self) -> usize {
        self.state.total.load(Ordering::SeqCst)
    }

    fn requests_to(&self, path: &str) -> usize {
        self.state
            .by_path
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or(0)
    }

    fn api_keys(&self) -> Vec<Vec<u8>> {
        self.state.api_keys.lock().unwrap().clone()
    }

    fn bodies(&self) -> Vec<Vec<u8>> {
        self.state.bodies.lock().unwrap().clone()
    }
}

fn query(q: Option<&str>, key: &str) -> Option<String> {
    q?.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| pct_decode(v))
    })
}

fn pct_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && i + 2 < b.len()
            && let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex(v: &[u8], upper: bool) -> String {
    v.iter()
        .map(|b| {
            if upper {
                format!("{b:02X}")
            } else {
                format!("{b:02x}")
            }
        })
        .collect()
}

/// The provider-side encodings `/enc` can return.
const FORMS: &[&str] = &[
    "raw", "b64", "b64url", "b64off1", "b64off2", "hex", "HEX", "pct", "json",
];

fn encode_as(form: &str, v: &[u8]) -> Vec<u8> {
    match form {
        "raw" => v.to_vec(),
        "b64" => STANDARD.encode(v).into_bytes(),
        "b64url" => URL_SAFE_NO_PAD.encode(v).into_bytes(),
        // The value inside a larger base64 blob, at byte offsets 1 and 2.
        "b64off1" => STANDARD
            .encode([b"<".as_slice(), v, b">"].concat())
            .into_bytes(),
        "b64off2" => STANDARD
            .encode([b"<<".as_slice(), v, b">"].concat())
            .into_bytes(),
        "hex" => hex(v, false).into_bytes(),
        "HEX" => hex(v, true).into_bytes(),
        "pct" => v
            .iter()
            .map(|b| format!("%{b:02X}"))
            .collect::<String>()
            .into_bytes(),
        "json" => serde_json::to_vec(&String::from_utf8_lossy(v)).expect("json"),
        _ => b"unknown form".to_vec(),
    }
}

async fn handle(State(st): State<Arc<MockState>>, req: Request) -> Response<Body> {
    let (parts, body) = req.into_parts();
    let body: Bytes = axum::body::to_bytes(body, 1 << 20)
        .await
        .unwrap_or_default();
    let path = parts.uri.path().to_owned();
    st.total.fetch_add(1, Ordering::SeqCst);
    *st.by_path.lock().unwrap().entry(path.clone()).or_default() += 1;
    let key = parts
        .headers
        .get("x-api-key")
        .map(|v| v.as_bytes().to_vec());
    if let Some(k) = &key {
        st.api_keys.lock().unwrap().push(k.clone());
    }
    st.bodies.lock().unwrap().push(body.to_vec());
    let q = parts.uri.query();
    match path.as_str() {
        "/echo" => {
            let headers: Vec<(String, String)> = parts
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_owned(),
                        String::from_utf8_lossy(v.as_bytes()).into_owned(),
                    )
                })
                .collect();
            let doc = json!({
                "method": parts.method.as_str(),
                "path": path,
                "headers": headers,
                "body_b64": STANDARD.encode(&body),
            });
            Response::builder()
                .status(200)
                .header("content-type", "application/json")
                .body(Body::from(doc.to_string()))
                .expect("response")
        }
        "/enc" => {
            let form = query(q, "as").unwrap_or_default();
            let enc = encode_as(&form, key.as_deref().unwrap_or_default());
            let mut b = Response::builder()
                .status(200)
                .header("content-type", "text/plain");
            if let Ok(h) = HeaderValue::from_bytes(&enc) {
                b = b.header("x-echo", h);
            }
            let mut doc = format!("form={form} value=").into_bytes();
            doc.extend_from_slice(&enc);
            doc.extend_from_slice(b" end");
            b.body(Body::from(doc)).expect("response")
        }
        "/redirect" => Response::builder()
            .status(302)
            .header("location", query(q, "to").unwrap_or_default())
            .body(Body::from("redirect"))
            .expect("response"),
        _ => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("not found"))
            .expect("response"),
    }
}

fn start_mock() -> Quiet<Mock> {
    spawn_quiet(Mock::start())
}

// ---------------------------------------------------------------------------------
// API recorder: every REST response body the test receives is kept for the scan
// ---------------------------------------------------------------------------------

#[derive(Default)]
struct Api {
    bodies: Vec<(String, Vec<u8>)>,
}

impl Api {
    async fn send(
        &mut self,
        s: &TestServer,
        method: reqwest::Method,
        path: &str,
        body: Option<Value>,
    ) -> (reqwest::StatusCode, Vec<u8>) {
        let mut req = s.http.request(method.clone(), s.url(path));
        if let Some(b) = &body {
            req = req.json(b);
        }
        let r = req.send().await.unwrap_or_else(|e| panic!("{path}: {e}"));
        let st = r.status();
        let bytes = r.bytes().await.expect("body").to_vec();
        self.bodies
            .push((format!("rest {method} {path}"), bytes.clone()));
        (st, bytes)
    }

    /// `PUT` expecting 200 (error bodies never echo a secret value).
    async fn put_ok(&mut self, s: &TestServer, path: &str, body: Value) {
        let (st, b) = self.send(s, reqwest::Method::PUT, path, Some(body)).await;
        assert_eq!(
            st,
            reqwest::StatusCode::OK,
            "PUT {path}: {}",
            String::from_utf8_lossy(&b)
        );
    }

    async fn get(&mut self, s: &TestServer, path: &str) -> Vec<u8> {
        let (st, b) = self.send(s, reqwest::Method::GET, path, None).await;
        assert_eq!(st, reqwest::StatusCode::OK, "GET {path}");
        b
    }

    async fn secret(&mut self, s: &TestServer, name: &str, value: &str) {
        self.put_ok(
            s,
            &format!("/v1/secrets/{}", seg(name)),
            json!({ "value": value }),
        )
        .await;
    }

    async fn grant(
        &mut self,
        s: &TestServer,
        module: &str,
        secret: &str,
        origin: &str,
        body: bool,
    ) {
        self.put_ok(
            s,
            "/v1/grants",
            json!({ "module": module, "secret": secret, "origins": [origin], "body": body }),
        )
        .await;
    }

    async fn egress(&mut self, s: &TestServer, module: &str, allow: &[&str]) {
        self.put_ok(
            s,
            &format!("/v1/egress/{}", seg(module)),
            json!({ "allow": allow }),
        )
        .await;
    }

    /// Start `workflow` (already deployed) and wait until it is terminal and completed.
    async fn run(&mut self, s: &TestServer, workflow: &str, input: Value) -> (RunId, Value) {
        let (st, b) = self
            .send(
                s,
                reqwest::Method::POST,
                &format!("/v1/workflows/{workflow}/runs"),
                Some(json!({ "input": input })),
            )
            .await;
        assert_eq!(st, reqwest::StatusCode::CREATED, "start {workflow}");
        let v: Value = serde_json::from_slice(&b).expect("json");
        let run: RunId = v["run_id"].as_str().expect("run_id").parse().expect("id");
        let info = wait_terminal(s, &run, RUN_LIMIT).await;
        assert_eq!(
            serde_json::to_value(info.status).expect("status"),
            "completed",
            "{workflow}: {info:?}"
        );
        let out = s.result_bytes(&run).await.expect("result");
        (run, serde_json::from_slice(&out).expect("output json"))
    }
}

// ---------------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------------

struct Fixture {
    s: TestServer,
    mock: Quiet<Mock>,
    canary: Canary,
    api: Api,
    canary_mod: String,
    object_root: tempfile::TempDir,
    /// Guest outputs, scanned too.
    outputs: Vec<(String, Vec<u8>)>,
    runs: Vec<RunId>,
}

/// Server + mock + canary configured over REST: secret `canary-key` (granted to fx-canary
/// for the mock origin, header only), secret `other-key` (granted only to
/// `https://elsewhere.test`), egress fx-canary → mock, an object connection `files` rooted
/// in a temp dir with egress fx-object → `connection:files`.
async fn fixture() -> Fixture {
    CaptureLayer::install_global();
    let canary = Canary::new();
    CaptureLayer::watch(&canary);
    let other = Canary::new();
    let mock = start_mock();
    let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
    let object_root = tempfile::tempdir().expect("object root");
    let canary_mod = deploy_fx(&s, "fx-canary").await;
    let object_mod = deploy_fx(&s, "fx-object").await;
    let mut api = Api::default();
    api.secret(&s, "canary-key", canary.expose_str()).await;
    api.secret(&s, "other-key", other.expose_str()).await;
    api.grant(&s, &canary_mod, "canary-key", &mock.base_url(), false)
        .await;
    api.grant(&s, &canary_mod, "other-key", OTHER_ORIGIN, false)
        .await;
    api.egress(&s, &canary_mod, &[&mock.base_url()]).await;
    api.put_ok(
        &s,
        "/v1/connections/files",
        json!({ "kind": "object", "config": {
            "v": 1, "backend": "local", "root": object_root.path().display().to_string()
        }}),
    )
    .await;
    api.egress(&s, &object_mod, &["connection:files"]).await;
    Fixture {
        s,
        mock,
        canary,
        api,
        canary_mod,
        object_root,
        outputs: Vec::new(),
        runs: Vec::new(),
    }
}

impl Fixture {
    /// Run fx-canary; returns the guest output (kept for the scan).
    async fn canary_run(&mut self, input: Value) -> Value {
        let (run, out) = self.api.run(&self.s, "fx-canary", input).await;
        self.outputs
            .push((format!("guest output {run}"), out.to_string().into_bytes()));
        self.runs.push(run);
        out
    }

    fn echo_url(&self, path: &str) -> String {
        format!("{}{path}", self.mock.base_url())
    }

    /// Everything, then a clean shutdown and the data dir + object root. Asserts clean.
    async fn scan_everything(mut self) -> String {
        let s = &self.s;
        let mut report_items: Vec<(String, Vec<u8>)> = Vec::new();

        // REST: runs, history, listings.
        let mut paths = vec![
            "/v1/runs?limit=1000".to_owned(),
            "/v1/secrets".into(),
            "/v1/grants".into(),
            "/v1/connections".into(),
            "/v1/egress".into(),
            "/v1/connections/files".into(),
            "/openapi.json".into(),
            "/metrics".into(),
        ];
        for run in &self.runs {
            paths.push(format!("/v1/runs/{run}"));
            paths.push(format!("/v1/runs/{run}/history?from_seq=0&limit=1000"));
        }
        for p in &paths {
            self.api.get(s, p).await;
        }

        // gRPC.
        let ch = s.channel().await;
        let mut runs = pb::runs_client::RunsClient::new(ch.clone());
        let mut sec = pb::secrets_client::SecretsClient::new(ch.clone());
        let mut con = pb::connectors_client::ConnectorsClient::new(ch);
        let mut grpc = Vec::new();
        for run in &self.runs {
            let info = runs
                .get(pb::GetRunRequest {
                    run_id: run.to_string(),
                })
                .await
                .expect("grpc get")
                .into_inner();
            grpc.extend_from_slice(format!("{info:?}").as_bytes());
            grpc.extend_from_slice(info.result_json.as_deref().unwrap_or_default());
            let h = runs
                .history(pb::RunHistoryRequest {
                    run_id: run.to_string(),
                    from_seq: 0,
                    limit: 0,
                })
                .await
                .expect("grpc history")
                .into_inner();
            for e in &h.events {
                grpc.extend_from_slice(e.summary.as_bytes());
                grpc.extend_from_slice(&e.details_json);
            }
        }
        let l = runs
            .list(pb::ListRunsRequest::default())
            .await
            .expect("grpc list runs")
            .into_inner();
        grpc.extend_from_slice(format!("{l:?}").as_bytes());
        let l = sec
            .list_secrets(pb::ListSecretsRequest { tenant: None })
            .await
            .expect("grpc list secrets")
            .into_inner();
        grpc.extend_from_slice(format!("{l:?}").as_bytes());
        let l = sec
            .list_grants(pb::ListGrantsRequest { module: None })
            .await
            .expect("grpc list grants")
            .into_inner();
        grpc.extend_from_slice(format!("{l:?}").as_bytes());
        let l = con
            .list_connections(pb::ListConnectionsRequest {})
            .await
            .expect("grpc list connections")
            .into_inner();
        grpc.extend_from_slice(format!("{l:?}").as_bytes());
        for p in &l.connections {
            grpc.extend_from_slice(&p.config_json);
        }
        let l = con
            .list_egress(pb::ListEgressRequest {})
            .await
            .expect("grpc list egress")
            .into_inner();
        grpc.extend_from_slice(format!("{l:?}").as_bytes());
        report_items.push(("grpc responses".into(), grpc));

        // The journal: raw encoded events, their Debug form, and every effect outcome.
        let mut raw = Vec::new();
        let mut dbg = Vec::new();
        let mut outcomes = Vec::new();
        let mut events = 0usize;
        for run in &self.runs {
            for ev in s.journal(run).await {
                events += 1;
                raw.extend_from_slice(&encode_event(&ev).1);
                dbg.extend_from_slice(format!("{ev:?}").as_bytes());
                if let EventBody::EffectCompleted(c) = &ev.body
                    && let Ok(Ok(out)) = decode_result::<Result<OpOutput, Failure>>(&c.outcome)
                {
                    match out {
                        OpOutput::Http(h) => {
                            outcomes.extend_from_slice(&h.body);
                            for hd in &h.headers {
                                outcomes.extend_from_slice(hd.value.as_bytes());
                            }
                        }
                        OpOutput::Object(b) => outcomes.extend_from_slice(&b),
                        _ => {}
                    }
                }
            }
            if let Some(r) = s.result_bytes(run).await {
                outcomes.extend_from_slice(&r);
            }
        }
        assert!(events > 0, "the journal was read");
        report_items.push(("journal raw events".into(), raw));
        report_items.push(("journal debug".into(), dbg));
        report_items.push(("journal outcomes + results".into(), outcomes));

        // Control: the capture is live (the activation span carries each run id).
        let captured = CaptureLayer::captured();
        for run in &self.runs {
            let id = run.to_string();
            assert!(
                captured.windows(id.len()).any(|w| w == id.as_bytes()),
                "the server's tracing output for run {run} was captured (non-vacuous scan)"
            );
        }

        // Clean shutdown, then every file.
        let data_dir = s.cfg.node.data_dir.clone();
        drop(runs);
        drop(sec);
        drop(con);
        self.s.shutdown().await;

        let mut report = Report::new(&self.canary);
        for (label, bytes) in self.api.bodies.iter().chain(&self.outputs) {
            report.bytes(label, bytes);
        }
        for (label, bytes) in &report_items {
            report.bytes(label, bytes);
        }
        report.tracing();
        report.sinks();
        assert_dir_has_files(&data_dir);
        report.tree("data dir", &data_dir);
        report.tree("object root", self.object_root.path());
        let summary = report.summary();
        println!("SC3 scan:\n{summary}");
        println!("capture: {:?}", CaptureLayer::stats());
        report.assert_clean();
        summary
    }
}

fn assert_dir_has_files(dir: &Path) {
    let mut stack = vec![dir.to_path_buf()];
    let mut n = 0;
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).expect("read_dir").flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                n += 1;
            }
        }
    }
    assert!(n > 0, "{} has files", dir.display());
}

fn b64d(v: &Value) -> Vec<u8> {
    STANDARD
        .decode(v.as_str().expect("base64 string"))
        .expect("base64")
}

/// The guest output fields env / args / random hold no needle.
fn assert_guest_blind(c: &Canary, out: &Value) {
    let mut r = Report::new(c);
    r.bytes("guest env", out["env"].to_string().as_bytes());
    r.bytes("guest args", out["args"].to_string().as_bytes());
    r.bytes("guest random", out["random"].to_string().as_bytes());
    r.assert_clean();
    assert_eq!(out["random"].as_str().map(str::len), Some(32), "{out}");
}

// ---------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------

/// Case 1 (+5): the leak scan over every location, non-vacuous.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn leak_scan() {
    let mut f = fixture().await;
    let out = f
        .canary_run(json!({
            "echo_url": f.echo_url("/echo"),
            "secret": "canary-key",
            "header": "X-Api-Key",
        }))
        .await;
    let observed = &out["observed"];
    assert_eq!(observed["status"], 200, "{out}");
    let body = b64d(&observed["body_b64"]);
    let echoed: Value = serde_json::from_slice(&body).expect("echo json");
    let key = echoed["headers"]
        .as_array()
        .expect("headers")
        .iter()
        .find(|kv| kv[0] == "x-api-key")
        .and_then(|kv| kv[1].as_str())
        .expect("x-api-key echoed");
    assert_eq!(key, "[REDACTED]", "the guest sees the echo scrubbed");
    // Non-vacuous: the provider did receive the real value.
    assert_eq!(f.mock.api_keys(), vec![f.canary.expose().to_vec()]);
    assert_guest_blind(&f.canary, &out);

    // One object put/get of non-secret data, so the object root has content.
    let (run, obj) = f
        .api
        .run(
            &f.s,
            "fx-object",
            json!({ "connection": "files", "ops": [
                { "op": "put", "key": "probe/a.txt", "body_b64": STANDARD.encode(b"probe data") },
                { "op": "get", "key": "probe/a.txt" },
            ]}),
        )
        .await;
    assert_eq!(
        b64d(&obj["results"][1]["bytes_b64"]),
        b"probe data",
        "{obj}"
    );
    f.outputs
        .push((format!("guest output {run}"), obj.to_string().into_bytes()));
    f.runs.push(run);
    assert_dir_has_files(f.object_root.path());

    f.scan_everything().await;
}

/// Case 2: a grant bound to another origin is refused before anything is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grant_origin_refused() {
    let mut f = fixture().await;
    let before = f.mock.total_requests();
    let out = f
        .canary_run(json!({ "echo_url": f.echo_url("/echo"), "secret": "other-key" }))
        .await;
    assert_eq!(
        out["observed"]["error"]["kind"], "secret_not_granted",
        "{out}"
    );
    assert_eq!(f.mock.total_requests(), before, "nothing was sent");
    f.scan_everything().await;
}

/// Case 3: a templated request does not follow a redirect; the 302 comes back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_redirect() {
    let mut f = fixture().await;
    let target = f.echo_url("/echo");
    let url = f.echo_url(&format!("/redirect?to={}", seg(&target)));
    let out = f
        .canary_run(json!({ "echo_url": url, "secret": "canary-key" }))
        .await;
    assert_eq!(out["observed"]["status"], 302, "{out}");
    let loc = out["observed"]["headers"]
        .as_array()
        .expect("headers")
        .iter()
        .find(|kv| {
            kv[0]
                .as_str()
                .is_some_and(|k| k.eq_ignore_ascii_case("location"))
        })
        .and_then(|kv| kv[1].as_str())
        .expect("location");
    assert_eq!(loc, target);
    assert_eq!(f.mock.requests_to("/redirect"), 1);
    assert_eq!(
        f.mock.requests_to("/echo"),
        0,
        "the redirect was not followed"
    );
    f.scan_everything().await;
}

/// Case 4: the provider echoes the value (header and body template, body grant) back
/// encoded every way; the guest output and journal hold none of the needles.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn echo_scrubbed_encodings() {
    let mut f = fixture().await;
    let module = f.canary_mod.clone();
    let origin = f.mock.base_url();
    f.api
        .grant(&f.s, &module, "canary-key", &origin, true)
        .await;
    for form in FORMS {
        let out = f
            .canary_run(json!({
                "echo_url": f.echo_url(&format!("/enc?as={form}")),
                "secret": "canary-key",
                "in_body": true,
            }))
            .await;
        let observed = &out["observed"];
        assert_eq!(observed["status"], 200, "{form}: {out}");
        let body = b64d(&observed["body_b64"]);
        assert!(
            body.windows(10).any(|w| w == b"[REDACTED]"),
            "{form}: scrubbed marker in {}",
            String::from_utf8_lossy(&body)
        );
        let mut r = Report::new(&f.canary);
        r.bytes(
            &format!("guest output ({form})"),
            out.to_string().as_bytes(),
        );
        r.bytes(&format!("guest body ({form})"), &body);
        r.assert_clean();
        assert_guest_blind(&f.canary, &out);
    }
    // Non-vacuous: every request carried the value in the header and in the body.
    let keys = f.mock.api_keys();
    assert_eq!(keys.len(), FORMS.len());
    assert!(keys.iter().all(|k| k == f.canary.expose()));
    let bodies = f.mock.bodies();
    let expected = serde_json::to_vec(&json!({ "secret": f.canary.expose_str() })).unwrap();
    assert!(
        bodies.iter().all(|b| b == &expected),
        "the body template was expanded under the body grant"
    );
    f.scan_everything().await;
}
