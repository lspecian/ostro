//! ROADMAP Phase 6 SC1 with real crashes (plan 06-17): the failpoint build of the `ostro`
//! binary (`cargo test -p ostro --features failpoints --test fx_redrive`) runs the fx-http
//! guest against a loopback mock provider and is killed with SIGKILL mid-flight.
//!
//! - **same_key_after_crash**: the provider charged, the completion was held at
//!   `bridge::effect::before_complete_submit` (`return(hang)`), kill -9; the restarted
//!   server re-drives with the same `Idempotency-Key` and the provider charges once;
//! - **window_unknown**: past the destination's `idempotency_window` a POST is not re-sent:
//!   the effect completes `effect_outcome_unknown` and the provider saw one request;
//! - **idempotent_resend**: past the window a GET is re-sent anyway (RFC 9110 idempotent);
//! - **suspended_inflight_wakes**: a run evicted while its 6 s `/pay` is in flight is woken
//!   after kill -9 (LeaderAnnounced) and completes with one charge.
//!
//! The mock is test-local (raw tokio TCP, HTTP/1.1, `Connection: close`) because the `ostro`
//! crate has no dev-dependency on ostro-testkit. Its `/pay` charge completes in a detached
//! task, so a provider whose client vanished in the crash still finishes the request (the
//! same model as 06-15's `detached_proxy`).
//!
//! Each test prints its timings (`--nocapture`).
#![cfg(feature = "failpoints")]

mod common;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{SERIAL, Spawned, command, guest_wasm, http, sha_hex};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Generous limit for a run reaching a terminal state.
const RUN_LIMIT: Duration = Duration::from_secs(60);

/// The completion failpoint that holds a finished effect before it is journaled.
const HOLD_COMPLETION: &str = "bridge::effect::before_complete_submit=return(hang)";

// ---------------------------------------------------------------------------------
// Test-local mock provider
// ---------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    key: Option<String>,
}

#[derive(Clone)]
enum Pay {
    InProgress,
    Done(String),
}

#[derive(Default)]
struct MockState {
    seen: Vec<Seen>,
    charges: HashMap<String, usize>,
    pay: HashMap<String, Pay>,
    charge_seq: usize,
}

/// `POST /pay[?delay_ms=N]` (key dedupe: first request charges, sleeps, stores; a done key
/// replays with `idempotent-replayed: true`; an in-progress key gets 409) and
/// `GET /status/{code}`.
#[derive(Clone)]
struct Mock {
    addr: SocketAddr,
    state: Arc<Mutex<MockState>>,
}

impl Mock {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("mock addr");
        let state = Arc::new(Mutex::new(MockState::default()));
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            while let Ok((sock, _)) = listener.accept().await {
                tokio::spawn(serve(sock, Arc::clone(&st)));
            }
        });
        Self { addr, state }
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, MockState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Requests carrying `Idempotency-Key: key`.
    fn requests(&self, key: &str) -> usize {
        self.lock()
            .seen
            .iter()
            .filter(|s| s.key.as_deref() == Some(key))
            .count()
    }

    /// Requests to `path` (without the query).
    fn requests_to(&self, path: &str) -> usize {
        self.lock().seen.iter().filter(|s| s.path == path).count()
    }

    fn charges(&self, key: &str) -> usize {
        self.lock().charges.get(key).copied().unwrap_or(0)
    }

    fn total_charges(&self) -> usize {
        self.lock().charges.values().sum()
    }

    /// Every request seen so far.
    fn seen(&self) -> Vec<Seen> {
        self.lock().seen.clone()
    }
}

/// One request per connection: parse, answer, close.
async fn serve(mut sock: TcpStream, st: Arc<Mutex<MockState>>) {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
        match sock.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or("").split(' ');
    let method = first.next().unwrap_or("").to_owned();
    let target = first.next().unwrap_or("/").to_owned();
    let mut key = None;
    let mut len = 0usize;
    for l in lines {
        if let Some((k, v)) = l.split_once(':') {
            let (k, v) = (k.trim().to_ascii_lowercase(), v.trim());
            if k == "idempotency-key" {
                key = Some(v.to_owned());
            } else if k == "content-length" {
                len = v.parse().unwrap_or(0);
            }
        }
    }
    // Drain the body (the mock ignores its content).
    let mut have = buf.len() - head_end - 4;
    while have < len {
        match sock.read(&mut chunk).await {
            Ok(0) | Err(_) => return,
            Ok(n) => have += n,
        }
    }
    let (path, query) = match target.split_once('?') {
        Some((p, q)) => (p.to_owned(), q.to_owned()),
        None => (target.clone(), String::new()),
    };
    let param = |name: &str| {
        query.split('&').find_map(|kv| {
            kv.split_once('=')
                .filter(|(k, _)| *k == name)
                .and_then(|(_, v)| v.parse::<u64>().ok())
        })
    };
    st.lock()
        .unwrap_or_else(|e| e.into_inner())
        .seen
        .push(Seen {
            method: method.clone(),
            path: path.clone(),
            key: key.clone(),
        });

    let (status, extra, body) = if method == "POST" && path == "/pay" {
        let Some(key) = key else {
            respond(&mut sock, 400, "", r#"{"error":"missing_idempotency_key"}"#).await;
            return;
        };
        let delay = Duration::from_millis(param("delay_ms").unwrap_or(0));
        let prior = {
            let mut g = st.lock().unwrap_or_else(|e| e.into_inner());
            match g.pay.get(&key).cloned() {
                Some(p) => Some(p),
                None => {
                    g.pay.insert(key.clone(), Pay::InProgress);
                    *g.charges.entry(key.clone()).or_default() += 1;
                    g.charge_seq += 1;
                    None
                }
            }
        };
        match prior {
            Some(Pay::Done(body)) => (200, "idempotent-replayed: true\r\n", body),
            Some(Pay::InProgress) => (409, "", r#"{"error":"in_progress"}"#.to_owned()),
            None => {
                // The charge finishes even if this connection is gone (crash).
                let (tx, rx) = tokio::sync::oneshot::channel();
                let st2 = Arc::clone(&st);
                let k2 = key.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let body = {
                        let mut g = st2.lock().unwrap_or_else(|e| e.into_inner());
                        let body =
                            json!({ "charge_id": g.charge_seq.to_string(), "key": k2 }).to_string();
                        g.pay.insert(k2, Pay::Done(body.clone()));
                        body
                    };
                    let _ = tx.send(body);
                });
                match rx.await {
                    Ok(body) => (200, "", body),
                    Err(_) => return,
                }
            }
        }
    } else if method == "GET" && path.starts_with("/status/") {
        let code: u16 = path["/status/".len()..].parse().unwrap_or(500);
        (code, "", format!("status {code}"))
    } else {
        (404, "", "not found".to_owned())
    };
    respond(&mut sock, status, extra, &body).await;
}

async fn respond(sock: &mut TcpStream, status: u16, extra: &str, body: &str) {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        _ => "Status",
    };
    let msg = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n{extra}\r\n{body}",
        body.len()
    );
    let _ = sock.write_all(msg.as_bytes()).await;
    let _ = sock.shutdown().await;
}

// ---------------------------------------------------------------------------------
// Spawned server
// ---------------------------------------------------------------------------------

/// A data dir + config for `ostro server`, restartable across kill -9.
struct Node {
    work: tempfile::TempDir,
    srv: Option<Spawned>,
    addr: SocketAddr,
    http: reqwest::Client,
}

impl Node {
    fn config_path(work: &Path) -> PathBuf {
        work.join("ostro.toml")
    }

    /// Spawn `ostro server` on the node's data dir (FAILPOINTS when given) and wait for
    /// `/readyz`. Returns the time to ready.
    async fn boot(&mut self, failpoints: Option<&str>) -> Duration {
        let mut cmd = command();
        cmd.current_dir(self.work.path())
            .arg("server")
            .arg("--config")
            .arg(Self::config_path(self.work.path()))
            .arg("--data-dir")
            .arg(self.work.path().join("data"))
            .arg("--listen")
            .arg("127.0.0.1:0");
        match failpoints {
            Some(fp) => cmd.env("FAILPOINTS", fp),
            None => cmd.env_remove("FAILPOINTS"),
        };
        let srv = Spawned::spawn(cmd);
        let ready = srv.ready_line(common::T);
        self.addr = ready.addr;
        let took = srv.wait_readyz(&self.http, ready.addr).await;
        self.srv = Some(srv);
        took
    }

    async fn start(failpoints: Option<&str>) -> Self {
        let work = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            Self::config_path(work.path()),
            "[node]\npartitions = 4\n\n[egress]\nallow_private = [\"127.0.0.0/8\"]\n",
        )
        .expect("write config");
        let mut n = Self {
            work,
            srv: None,
            addr: SocketAddr::from(([127, 0, 0, 1], 0)),
            http: http(),
        };
        let took = n.boot(failpoints).await;
        println!("boot: ready in {took:?}");
        n
    }

    /// SIGKILL (no graceful shutdown) and wait for the process to be gone.
    fn kill9(&mut self) {
        let mut srv = self.srv.take().expect("server running");
        srv.signal("KILL");
        let st = srv.wait_exit(common::T);
        assert!(!st.success(), "killed server exited cleanly: {st:?}");
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    fn stderr(&self) -> String {
        self.srv.as_ref().map(Spawned::stderr).unwrap_or_default()
    }

    async fn put_json(&self, path: &str, body: Value) {
        let r = self
            .http
            .put(self.url(path))
            .json(&body)
            .send()
            .await
            .unwrap_or_else(|e| panic!("PUT {path}: {e}"));
        let st = r.status();
        let text = r.text().await.unwrap_or_default();
        assert_eq!(st, StatusCode::OK, "PUT {path}: {text}");
    }

    /// Deploy fx-http as module `fx-http` and allow it to reach `origin`.
    async fn deploy_fx_http(&self, origin: &str) {
        let wasm = guest_wasm("fx-http");
        let r = self
            .http
            .put(self.url(&format!("/v1/modules/{}?name=fx-http", sha_hex(&wasm))))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy fx-http: {}", r.status());
        self.put_json("/v1/egress/fx-http", json!({ "allow": [origin] }))
            .await;
    }

    /// An `http-target` connection for `origin` (its name is the origin).
    async fn http_target(&self, origin: &str, config: Value) {
        self.put_json(
            &format!("/v1/connections/{}", seg(origin)),
            json!({ "kind": "http-target", "config": config }),
        )
        .await;
    }

    async fn start_run(&self, requests: Vec<Value>) -> String {
        let r = self
            .http
            .post(self.url("/v1/workflows/fx-http/runs"))
            .json(&json!({ "input": { "requests": requests, "parallel": false } }))
            .send()
            .await
            .expect("POST run");
        let st = r.status();
        let b: Value = r.json().await.expect("json");
        assert_eq!(st, StatusCode::CREATED, "{b}");
        b["run_id"].as_str().expect("run_id").to_owned()
    }

    async fn get(&self, path: &str) -> Value {
        let r = self
            .http
            .get(self.url(path))
            .send()
            .await
            .unwrap_or_else(|e| panic!("GET {path}: {e}"));
        assert_eq!(r.status(), StatusCode::OK, "GET {path}");
        r.json().await.expect("json")
    }

    /// Poll until the run is terminal; returns the run body.
    async fn wait_terminal(&self, run: &str) -> Value {
        let deadline = Instant::now() + RUN_LIMIT;
        loop {
            let b = self.get(&format!("/v1/runs/{run}")).await;
            if b["status"]
                .as_str()
                .is_some_and(|s| matches!(s, "completed" | "failed" | "cancelled" | "terminated"))
            {
                return b;
            }
            assert!(
                Instant::now() < deadline,
                "run {run} not terminal after {RUN_LIMIT:?}: {b}\nstderr:\n{}",
                self.stderr()
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// The op (journal seq) of the run's first `effect_scheduled`.
    async fn first_effect_op(&self, run: &str) -> u64 {
        let deadline = Instant::now() + common::T;
        loop {
            let h = self
                .get(&format!("/v1/runs/{run}/history?from_seq=0&limit=100"))
                .await;
            if let Some(op) = h["events"].as_array().and_then(|evs| {
                evs.iter()
                    .find(|e| e["kind"] == "effect_scheduled")
                    .and_then(|e| e["seq"].as_u64())
            }) {
                return op;
            }
            assert!(Instant::now() < deadline, "no effect_scheduled: {h}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Percent-encode one path segment.
fn seg(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The connector's derived key (06-10 contract, pinned there by vectors):
/// `"ostro-" + hex(sha256("ostro/idem/v1" || run_id || op_le)[..16])`.
fn derive_key(run_id: &str, op: u64) -> String {
    let mut h = Sha256::new();
    h.update(b"ostro/idem/v1");
    h.update(run_id.as_bytes());
    h.update(op.to_le_bytes());
    let d: [u8; 32] = h.finalize().into();
    let hex: String = d[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("ostro-{hex}")
}

async fn wait_for(what: &str, limit: Duration, f: impl Fn() -> bool) {
    let deadline = Instant::now() + limit;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn post(url: String) -> Value {
    json!({
        "method": "POST",
        "url": url,
        "headers": [["content-type", "application/json"]],
        "body_b64": "e30=",
    })
}

/// The first fx-http result item of a completed run body.
fn first_item(run: &Value) -> Value {
    assert_eq!(run["status"], "completed", "{run}");
    run["result"]["results"][0].clone()
}

// ---------------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------------

/// kill -9 after the charge, before the completion is journaled: one re-drive, same key,
/// one charge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_key_after_crash() {
    let _g = SERIAL.lock().await;
    let mock = Mock::start().await;
    let mut n = Node::start(Some(HOLD_COMPLETION)).await;
    n.deploy_fx_http(&mock.base_url()).await;

    let t0 = Instant::now();
    let run = n
        .start_run(vec![post(format!("{}/pay", mock.base_url()))])
        .await;
    let op = n.first_effect_op(&run).await;
    let key = derive_key(&run, op);
    wait_for("the charge", common::T, || mock.charges(&key) == 1).await;
    println!("charged after {:?}", t0.elapsed());
    tokio::time::sleep(Duration::from_millis(200)).await;
    n.kill9();
    println!("killed after {:?}", t0.elapsed());

    let took = n.boot(None).await;
    println!("restart: ready in {took:?}");
    let b = n.wait_terminal(&run).await;
    println!("terminal after {:?}", t0.elapsed());
    let item = first_item(&b);
    assert_eq!(item["status"], 200, "{b}");

    assert_eq!(mock.requests(&key), 2, "re-driven once: {:?}", mock.seen());
    assert_eq!(mock.requests_to("/pay"), 2, "no request under another key");
    assert_eq!(mock.charges(&key), 1, "exactly one charge");
    assert_eq!(mock.total_charges(), 1);
}

/// Past the window (2 s, margin 0) a POST re-drive is not sent: `effect_outcome_unknown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn window_unknown() {
    let _g = SERIAL.lock().await;
    let mock = Mock::start().await;
    let mut n = Node::start(Some(HOLD_COMPLETION)).await;
    n.deploy_fx_http(&mock.base_url()).await;
    n.http_target(
        &mock.base_url(),
        json!({ "v": 1, "idempotency_window": "2s", "safety_margin": "0s" }),
    )
    .await;

    let t0 = Instant::now();
    let run = n
        .start_run(vec![post(format!("{}/pay", mock.base_url()))])
        .await;
    let op = n.first_effect_op(&run).await;
    let key = derive_key(&run, op);
    wait_for("the charge", common::T, || mock.charges(&key) == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    n.kill9();
    println!("killed after {:?}", t0.elapsed());
    tokio::time::sleep(Duration::from_secs(3)).await;

    let took = n.boot(None).await;
    println!("restart: ready in {took:?}");
    let b = n.wait_terminal(&run).await;
    println!("terminal after {:?}", t0.elapsed());
    let item = first_item(&b);
    assert_eq!(item["error"]["kind"], "effect_outcome_unknown", "{b}");
    assert_eq!(item["error"]["retryable"], false, "{b}");
    assert_eq!(mock.requests(&key), 1, "not re-sent: {:?}", mock.seen());
    assert_eq!(mock.charges(&key), 1);
}

/// Past the same window a GET (idempotent) is re-sent and completes 200.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idempotent_resend() {
    let _g = SERIAL.lock().await;
    let mock = Mock::start().await;
    let mut n = Node::start(Some(HOLD_COMPLETION)).await;
    n.deploy_fx_http(&mock.base_url()).await;
    n.http_target(
        &mock.base_url(),
        json!({ "v": 1, "idempotency_window": "2s", "safety_margin": "0s" }),
    )
    .await;

    let t0 = Instant::now();
    let run = n
        .start_run(vec![json!({
            "method": "GET",
            "url": format!("{}/status/200", mock.base_url()),
        })])
        .await;
    let op = n.first_effect_op(&run).await;
    let key = derive_key(&run, op);
    wait_for("the request", common::T, || mock.requests(&key) == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    n.kill9();
    println!("killed after {:?}", t0.elapsed());
    tokio::time::sleep(Duration::from_secs(3)).await;

    let took = n.boot(None).await;
    println!("restart: ready in {took:?}");
    let b = n.wait_terminal(&run).await;
    println!("terminal after {:?}", t0.elapsed());
    assert_eq!(first_item(&b)["status"], 200, "{b}");
    assert_eq!(mock.requests_to("/status/200"), 2, "{:?}", mock.seen());
    assert_eq!(mock.requests(&key), 2, "re-sent with the same key");
    assert!(
        mock.seen().iter().all(|s| s.method == "GET"),
        "{:?}",
        mock.seen()
    );
}

/// A run evicted while its `/pay?delay_ms=6000` is in flight (longer than the 2 s hot
/// wait) is woken after kill -9 and completes with one charge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suspended_inflight_wakes() {
    let _g = SERIAL.lock().await;
    let mock = Mock::start().await;
    let mut n = Node::start(None).await;
    n.deploy_fx_http(&mock.base_url()).await;

    let mut pay = post(format!("{}/pay?delay_ms=6000", mock.base_url()));
    // The re-drive may meet the in-progress 409 while the first request still sleeps.
    pay["retry"] = json!({
        "max_attempts": 40,
        "initial_interval_ms": 250,
        "max_interval_ms": 1000,
        "backoff_coefficient": 2.0,
    });
    let t0 = Instant::now();
    let run = n.start_run(vec![pay]).await;
    let op = n.first_effect_op(&run).await;
    let key = derive_key(&run, op);
    wait_for("the /pay request", common::T, || mock.requests(&key) == 1).await;
    let sent = Instant::now();
    // Past the 2 s hot wait (the activation evicts), well before the 6 s response.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(mock.charges(&key), 1);
    n.kill9();
    let killed = sent.elapsed();
    println!("killed {killed:?} after the provider saw the request");
    assert!(
        killed < Duration::from_millis(5500),
        "killed too late: the provider may have answered"
    );

    let took = n.boot(None).await;
    println!("restart: ready in {took:?}");
    let b = n.wait_terminal(&run).await;
    println!("terminal after {:?}", t0.elapsed());
    assert_eq!(first_item(&b)["status"], 200, "{b}");
    assert!(mock.requests(&key) >= 2, "re-driven: {:?}", mock.seen());
    assert_eq!(
        mock.requests_to("/pay"),
        mock.requests(&key),
        "every /pay request carried the same key"
    );
    assert_eq!(mock.charges(&key), 1, "exactly one charge");
}
