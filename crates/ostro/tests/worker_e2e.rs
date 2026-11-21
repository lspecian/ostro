//! ROADMAP Phase 7 success criterion 1 through the real binaries (plan 07-17; WORK-01,
//! WORK-02, WORK-06, SEC-01), on the 07-16 spawned harness (`common/p7.rs`).
//!
//! - `join`: `ostro worker --join` with the right `--ca-hash` joins over TLS and runs 5
//!   `sdk-basic` runs of a `workers` queue; a wrong `--ca-hash` is refused in the TLS
//!   handshake, before the join request (and its token) is sent; revoking the worker's
//!   join token closes its session within 2 s and a restart with that token is refused.
//! - `paging`: `[workers] max_frame_bytes = 64 KiB`, a 2,000-step `long-journal` run with
//!   a 3 MiB step result and a 3 MiB signal input completes on the worker with the result
//!   of the same run on an embedded queue.
//! - `module_cache`: the first run compiles the module into the worker's `--cache-dir`
//!   (AOT artefact created); a restarted worker with the same cache dir loads it (same
//!   artefact file, never rewritten); a tampered artefact is rejected and recompiled.
//! - `secret_never_on_worker`: `pay-flow`'s `Authorization: Bearer {{secret:pay_token}}`
//!   reaches the origin with the canary secret, expanded on the leader, while the worker's
//!   TRACE stdout/stderr and its cache dir contain no encoding of the canary.
//! - `listener`: the client listener's TLS modes through the binary (see the module).
//!
//! The CLI client cannot reach an `https://` server yet (07-15 follow-up), so tokens,
//! revocation, secrets, grants and egress go over REST, like the 07-16 harness.
#![cfg(unix)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p7.rs"]
mod p7;

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use common::{events_of, sha_hex};
use p7::{P7_T, P7Cluster, P7Opts, WorkerOpts};
use reqwest::StatusCode;
use serde_json::{Value, json};

/// Spawned servers are heavy: one cluster at a time per test process.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const MIB: u64 = 1 << 20;

/// Server logs at info: `remote activate run=..` and `worker joined` are info lines.
fn log_opts() -> P7Opts {
    P7Opts {
        env: vec![("RUST_LOG".into(), "info".into())],
        ..P7Opts::default()
    }
}

// ---------------------------------------------------------------------------------
// Local process helper (p7's P7Proc has a private constructor): a spawned `ostro`
// child with line-drained stdout/stderr, killed on drop.
// ---------------------------------------------------------------------------------

struct Proc {
    child: Child,
    stdout: Arc<Mutex<Vec<String>>>,
    stderr: Arc<Mutex<Vec<String>>>,
}

fn drain(r: impl std::io::Read + Send + 'static, into: Arc<Mutex<Vec<String>>>) {
    std::thread::spawn(move || {
        for line in BufReader::new(r).lines() {
            let Ok(line) = line else { break };
            into.lock().unwrap_or_else(|e| e.into_inner()).push(line);
        }
    });
}

impl Proc {
    fn spawn(mut cmd: Command) -> Self {
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn ostro");
        let stdout = Arc::new(Mutex::new(Vec::new()));
        let stderr = Arc::new(Mutex::new(Vec::new()));
        drain(child.stdout.take().expect("stdout"), Arc::clone(&stdout));
        drain(child.stderr.take().expect("stderr"), Arc::clone(&stderr));
        Self {
            child,
            stdout,
            stderr,
        }
    }

    fn pid(&self) -> u32 {
        self.child.id()
    }

    fn stdout(&self) -> Vec<String> {
        self.stdout
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn stderr(&self) -> Vec<String> {
        self.stderr
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn output(&self) -> String {
        let mut s = self.stdout().join("\n");
        s.push_str("\n--- stderr ---\n");
        s.push_str(&self.stderr().join("\n"));
        s
    }

    fn try_exit(&mut self) -> Option<ExitStatus> {
        self.child.try_wait().expect("try_wait")
    }

    fn wait_exit(&mut self, limit: Duration) -> ExitStatus {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(st) = self.try_exit() {
                // Let the drain threads collect the last lines.
                std::thread::sleep(Duration::from_millis(100));
                return st;
            }
            assert!(
                Instant::now() < deadline,
                "process {} did not exit within {limit:?}:\n{}",
                self.pid(),
                self.output()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// Poll until a stdout line satisfies `f` (panics if the process exits first).
    fn wait_stdout<T>(&mut self, limit: Duration, f: impl Fn(&str) -> Option<T>) -> T {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(v) = self.stdout().iter().find_map(|l| f(l)) {
                return v;
            }
            if let Some(st) = self.try_exit() {
                std::thread::sleep(Duration::from_millis(100));
                if let Some(v) = self.stdout().iter().find_map(|l| f(l)) {
                    return v;
                }
                panic!(
                    "exited ({st:?}) before the expected line:\n{}",
                    self.output()
                );
            }
            assert!(
                Instant::now() < deadline,
                "no expected line within {limit:?}:\n{}",
                self.output()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn signal(&self, sig: &str) {
        let st = Command::new("kill")
            .arg(format!("-{sig}"))
            .arg(self.pid().to_string())
            .status()
            .expect("run kill");
        assert!(st.success(), "kill -{sig} {} failed", self.pid());
    }

    /// SIGTERM and wait for the clean (drained) exit.
    fn stop(&mut self) {
        self.signal("TERM");
        let st = self.wait_exit(P7_T);
        assert!(
            st.success(),
            "exit after SIGTERM {st:?}:\n{}",
            self.output()
        );
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        if let Ok(None) = self.child.try_wait() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// `ostro worker` against the cluster with an explicit pin, token and cache dir (the
/// harness's `spawn_worker` always uses the cluster pin, its own token and a fresh cache).
fn worker_cmd(
    c: &P7Cluster,
    id: &str,
    ca_hash: &str,
    token: &str,
    cache: &Path,
    rust_log: Option<&str>,
) -> Command {
    let mut cmd = common::command();
    cmd.current_dir(c.dir.path())
        .arg("worker")
        .arg("--join")
        .arg(c.join_url())
        .arg("--ca-hash")
        .arg(ca_hash)
        .arg("--slots")
        .arg("2")
        .arg("--cache-dir")
        .arg(cache)
        .arg("--worker-id")
        .arg(id)
        .env("OSTRO_WORKER_TOKEN", token)
        .env("OSTRO_WORKER_RECONNECT_MAX_MS", "500");
    if let Some(l) = rust_log {
        cmd.env("RUST_LOG", l);
    }
    cmd
}

/// Spawn a worker and wait for its ready line.
fn spawn_ready(cmd: Command, id: &str) -> Proc {
    let mut p = Proc::spawn(cmd);
    let (got, _, _) = tokio::task::block_in_place(|| p.wait_stdout(P7_T, p7::worker_ready));
    assert_eq!(got, id);
    p
}

/// `wkr_<id>` of a `wkr_<id>.<secret>` token.
fn token_id(token: &str) -> &str {
    token.split('.').next().expect("token id")
}

fn worker_ids(ws: &[Value]) -> Vec<String> {
    ws.iter()
        .filter_map(|w| w["worker_id"].as_str().map(str::to_owned))
        .collect()
}

/// The run went to a worker: the leader logs every remote activation.
fn assert_remote(c: &P7Cluster, run: &str) {
    let out = c.server().output();
    assert!(
        out.contains(&format!("remote activate run={run}")),
        "run {run} was not activated on a worker:\n{out}"
    );
}

fn assert_not_remote(c: &P7Cluster, run: &str) {
    let out = c.server().output();
    assert!(
        !out.contains(&format!("remote activate run={run}")),
        "embedded run {run} was activated on a worker"
    );
}

async fn sdk_basic(c: &P7Cluster, name: &str, queue: &str) -> String {
    let id = c
        .api
        .start(
            "sdk-basic",
            json!({"input": {"name": name, "n": 3}, "queue": queue}),
        )
        .await;
    let r = c.api.wait_terminal(&id, P7_T).await;
    assert_eq!(r["status"], "completed", "{r}");
    id
}

// ---------------------------------------------------------------------------------
// join: pinning, runs, revocation
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(log_opts()).await;
    c.api.deploy("sdk-basic").await;
    c.api.set_flow("wq", json!({"placement": "workers"})).await;

    // The joined worker gets its own token, so revoking it touches nothing else.
    let (tok, pin) = c.create_token(None).await;
    assert_eq!(pin, c.ca_hash);
    let cache = tempfile::tempdir_in(c.dir.path()).expect("cache");
    let mut w = spawn_ready(
        worker_cmd(&c, "join-w1", &c.ca_hash, &tok, cache.path(), None),
        "join-w1",
    );
    let joined = t0.elapsed();
    let ws = c.wait_workers(1).await;
    assert_eq!(worker_ids(&ws), ["join-w1"]);
    let server_out = c.server().output();
    assert!(
        server_out.contains("worker joined") && server_out.contains("join-w1"),
        "the server logged no join for join-w1:\n{server_out}"
    );

    let t_runs = Instant::now();
    for i in 0..5 {
        let id = sdk_basic(&c, &format!("j{i}"), "wq").await;
        assert_remote(&c, &id);
    }
    let runs = t_runs.elapsed();

    // A wrong pin: the worker's TLS verifier refuses the client listener's chain, so the
    // join POST (which carries the token) is never sent. The token itself is valid: a
    // request that reached the server would have joined and logged `worker joined ..
    // worker_id=wrong-pin`.
    let wrong = format!("sha256:{}", "ab".repeat(32));
    assert_ne!(wrong, c.ca_hash);
    let wcache = tempfile::tempdir_in(c.dir.path()).expect("cache");
    let mut bad = Proc::spawn(worker_cmd(
        &c,
        "wrong-pin",
        &wrong,
        &c.token,
        wcache.path(),
        None,
    ));
    let st = tokio::task::block_in_place(|| bad.wait_exit(P7_T));
    assert!(!st.success(), "wrong --ca-hash joined:\n{}", bad.output());
    let out = bad.output();
    assert!(
        out.contains("--ca-hash"),
        "no pinning error in the worker output:\n{out}"
    );
    assert!(
        !bad.stdout().iter().any(|l| p7::worker_ready(l).is_some()),
        "{out}"
    );
    assert!(!out.contains(&c.token), "the token leaked into the output");
    let server_out = c.server().output();
    assert!(
        !server_out.contains("wrong-pin"),
        "the server saw a join attempt from the mis-pinned worker:\n{server_out}"
    );
    assert_eq!(worker_ids(&c.api.workers().await), ["join-w1"]);

    // Revoke the joined worker's token: its session closes within 2 s.
    let (st, v) = c
        .api
        .send(
            reqwest::Method::DELETE,
            &format!("/v1/workers/tokens/{}", token_id(&tok)),
            None,
        )
        .await;
    assert_eq!(st, StatusCode::NO_CONTENT, "revoke: {v}");
    let t_revoke = Instant::now();
    loop {
        if !worker_ids(&c.api.workers().await).contains(&"join-w1".to_owned()) {
            break;
        }
        assert!(
            t_revoke.elapsed() < Duration::from_secs(2),
            "join-w1 still connected 2 s after its token was revoked"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let closed = t_revoke.elapsed();
    let server_out = c.server().output();
    assert!(
        server_out.contains("worker join token revoked"),
        "{server_out}"
    );
    let worker_exit = w.try_exit();
    drop(w);

    // A restart with the revoked token is refused at the join.
    let rcache = tempfile::tempdir_in(c.dir.path()).expect("cache");
    let mut again = Proc::spawn(worker_cmd(
        &c,
        "join-w1",
        &c.ca_hash,
        &tok,
        rcache.path(),
        None,
    ));
    let st = tokio::task::block_in_place(|| again.wait_exit(P7_T));
    assert!(!st.success(), "a revoked token joined:\n{}", again.output());
    let out = again.output();
    assert!(
        out.contains("join refused") && out.contains("revoked"),
        "no refusal in the worker output:\n{out}"
    );
    assert!(!out.contains(&tok), "the token leaked into the output");
    assert!(worker_ids(&c.api.workers().await).is_empty());
    eprintln!(
        "worker_e2e::join: worker ready {joined:?}, 5 runs {runs:?}, session closed {closed:?} \
         after revoke (worker exit status then: {worker_exit:?}), total {:?}",
        t0.elapsed()
    );
}

// ---------------------------------------------------------------------------------
// paging: 64 KiB frames, 2,000 steps, 3 MiB result + 3 MiB input
// ---------------------------------------------------------------------------------

async fn long_run(c: &P7Cluster, queue: &str, big_input: &Value) -> (String, Value) {
    let id = c
        .api
        .start(
            "long-journal",
            json!({
                "input": {"steps": 2000, "result_bytes": 3 * MIB, "wait_input": true},
                "queue": queue
            }),
        )
        .await;
    let (st, v) = c
        .api
        .post(
            &format!("/v1/runs/{id}/signals/input"),
            json!({ "payload": big_input }),
        )
        .await;
    assert_eq!(st, StatusCode::OK, "signal: {v}");
    let r = c.api.wait_terminal(&id, Duration::from_secs(300)).await;
    assert_eq!(r["status"], "completed", "{queue}: {r}");
    (id, r)
}

/// `(kind, step name)` per history event: the run's shape without times or ids.
fn shape(h: &[Value]) -> Vec<(String, String)> {
    h.iter()
        .map(|e| {
            (
                e["kind"].as_str().unwrap_or("").to_owned(),
                e["details"]["name"].as_str().unwrap_or("").to_owned(),
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paging() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(P7Opts {
        max_frame_bytes: Some(64 * 1024),
        ..log_opts()
    })
    .await;
    let _w = c.spawn_worker(WorkerOpts::new("page-w1", 2));
    c.wait_workers(1).await;
    c.api.deploy("long-journal").await;
    c.api.set_flow("wq", json!({"placement": "workers"})).await;
    // A 3 MiB JSON string (the signal input).
    let big_input = Value::String("y".repeat(3 * MIB as usize));

    let t_emb = Instant::now();
    let (emb, re) = long_run(&c, "default", &big_input).await;
    let emb_t = t_emb.elapsed();
    let t_rem = Instant::now();
    let (rem, rr) = long_run(&c, "wq", &big_input).await;
    let rem_t = t_rem.elapsed();
    assert_not_remote(&c, &emb);
    assert_remote(&c, &rem);

    let out = &rr["result"];
    assert_eq!(out["steps"], 2000, "{out}");
    assert_eq!(out["sum"], 1999 * 2000 / 2, "{out}");
    assert_eq!(out["big_len"], 3 * MIB, "{out}");
    assert_eq!(out["input_len"], 3 * MIB + 2, "{out}");
    let sha = |v: &Value| sha_hex(&serde_json::to_vec(v).expect("json"));
    assert_eq!(
        sha(&rr["result"]),
        sha(&re["result"]),
        "worker result == embedded result"
    );
    let (he, hr) = (c.api.history(&emb).await, c.api.history(&rem).await);
    assert_eq!(events_of(&hr, "step_completed").len(), 2001);
    assert_eq!(
        shape(&he),
        shape(&hr),
        "same event shape embedded and remote"
    );
    eprintln!(
        "worker_e2e::paging: embedded {emb_t:?}, worker {rem_t:?}, {} events, total {:?}",
        hr.len(),
        t0.elapsed()
    );
}

// ---------------------------------------------------------------------------------
// module_cache: fetch by sha256, AOT cache across restarts, tamper -> recompile
// ---------------------------------------------------------------------------------

/// The AOT artefacts (`<cache>/aot/*.cwasm`).
fn artefacts(cache: &Path) -> Vec<PathBuf> {
    let Ok(rd) = std::fs::read_dir(cache.join("aot")) else {
        return Vec::new();
    };
    let mut v: Vec<PathBuf> = rd
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "cwasm"))
        .collect();
    v.sort();
    v
}

/// (inode, mtime, bytes) of a file: a compile persists through tmp + rename, so a
/// rewritten artefact always has a new inode.
fn identity(p: &Path) -> (u64, std::time::SystemTime, Vec<u8>) {
    use std::os::unix::fs::MetadataExt;
    let m = std::fs::metadata(p).expect("metadata");
    (
        m.ino(),
        m.modified().expect("mtime"),
        std::fs::read(p).expect("read"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn module_cache() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let c = P7Cluster::start(log_opts()).await;
    c.api.deploy("sdk-basic").await;
    c.api.set_flow("wq", json!({"placement": "workers"})).await;
    let cache = tempfile::tempdir_in(c.dir.path()).expect("cache");
    let tok = c.token.clone();
    let start = |id: &str| {
        spawn_ready(
            worker_cmd(&c, id, &c.ca_hash, &tok, cache.path(), Some("info")),
            id,
        )
    };

    // 1. Cold cache: the module is fetched by sha256 and compiled; the artefact appears.
    assert!(artefacts(cache.path()).is_empty());
    let mut w = start("cache-w1");
    let t1 = Instant::now();
    let r1 = sdk_basic(&c, "cold", "wq").await;
    let cold = t1.elapsed();
    assert_remote(&c, &r1);
    let arts = artefacts(cache.path());
    assert_eq!(arts.len(), 1, "one compiled artefact: {arts:?}");
    let art = arts[0].clone();
    let (ino1, mtime1, bytes1) = identity(&art);
    w.stop();
    assert!(
        !w.output().contains("AOT artefact"),
        "cold start warned about the cache:\n{}",
        w.output()
    );

    // 2. Restart, same cache dir: the artefact is loaded, not recompiled.
    let mut w = start("cache-w1");
    let t2 = Instant::now();
    let r2 = sdk_basic(&c, "warm", "wq").await;
    let warm = t2.elapsed();
    assert_remote(&c, &r2);
    assert_eq!(artefacts(cache.path()), vec![art.clone()]);
    let (ino2, mtime2, bytes2) = identity(&art);
    assert_eq!(
        (ino2, mtime2),
        (ino1, mtime1),
        "the artefact was rewritten (recompiled) on a warm start"
    );
    assert_eq!(bytes2, bytes1);
    w.stop();
    assert!(!w.output().contains("AOT artefact"), "{}", w.output());

    // 3. Tamper one byte: the next start rejects the artefact and recompiles.
    let mut tampered = bytes1.clone();
    let mid = tampered.len() / 2;
    tampered[mid] ^= 0x5a;
    std::fs::write(&art, &tampered).expect("tamper");
    let (ino_t, _, _) = identity(&art);
    let mut w = start("cache-w1");
    let t3 = Instant::now();
    let r3 = sdk_basic(&c, "tampered", "wq").await;
    let retamper = t3.elapsed();
    assert_remote(&c, &r3);
    let out = w.output();
    assert!(
        out.contains("AOT artefact failed authentication; deleted"),
        "no tamper rejection logged:\n{out}"
    );
    assert_eq!(artefacts(cache.path()), vec![art.clone()], "recompiled");
    let (ino3, _, bytes3) = identity(&art);
    assert_ne!(
        ino3, ino_t,
        "the artefact was not rewritten after the tamper"
    );
    assert_ne!(bytes3, tampered);
    w.stop();
    eprintln!(
        "worker_e2e::module_cache: cold run {cold:?}, warm run {warm:?}, after tamper \
         {retamper:?}, artefact {} bytes, total {:?}",
        bytes1.len(),
        t0.elapsed()
    );
}

// ---------------------------------------------------------------------------------
// secret_never_on_worker
// ---------------------------------------------------------------------------------

/// A loopback HTTP/1.1 origin recording each request head (the 06-10 `MockProvider`
/// does not record headers); answers 200 `{"charge_id":"1"}` and closes.
struct Origin {
    addr: std::net::SocketAddr,
    heads: Arc<Mutex<Vec<String>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Origin {
    async fn start() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let l = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind origin");
        let addr = l.local_addr().expect("addr");
        let heads = Arc::new(Mutex::new(Vec::new()));
        let rec = Arc::clone(&heads);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = l.accept().await else {
                    return;
                };
                let rec = Arc::clone(&rec);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    let head_end = loop {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + len {
                        match s.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    rec.lock().unwrap_or_else(|e| e.into_inner()).push(head);
                    let body = br#"{"charge_id":"1"}"#;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = s.write_all(resp.as_bytes()).await;
                    let _ = s.write_all(body).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        Self { addr, heads, task }
    }

    fn base(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn heads(&self) -> Vec<String> {
        self.heads.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Drop for Origin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// `PUT /v1/modules/{sha}?name=<name>`: a named module, for grants and egress.
async fn deploy_named(c: &P7Cluster, guest: &str, name: &str) {
    let wasm = common::guest_wasm(guest);
    let sha = sha_hex(&wasm);
    let r = c
        .api
        .http
        .put(c.api.url(&format!("/v1/modules/{sha}?name={name}")))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
}

async fn put_ok(c: &P7Cluster, path: &str, body: Value) {
    let (st, v) = c.api.put(path, body).await;
    assert!(st.is_success(), "PUT {path}: {st} {v}");
}

/// The worker's `RUST_LOG`: TRACE everywhere (ostro, tonic, h2, hyper, rustls, reqwest,
/// wasmtime's runtime) except Cranelift's compiler passes, which only ever see the
/// module's code and alone log over 1 GB per compile at TRACE (measured: 6.5 M lines).
const WORKER_TRACE: &str = "trace,cranelift_codegen=info,cranelift_frontend=info,\
                            wasmtime_internal_cranelift=info,regalloc2=info";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn secret_never_on_worker() {
    let _serial = SERIAL.lock().await;
    let t0 = Instant::now();
    let keys = tempfile::tempdir().expect("keys");
    let sealing = keys.path().join("sealing.key");
    std::fs::write(&sealing, [0x5au8; 32]).expect("sealing key");
    let c = P7Cluster::start(P7Opts {
        // `[egress] allow_private` for the loopback origin.
        mock: true,
        extra_toml: format!("[secrets]\nsealing_key_file = {sealing:?}\n\n"),
        ..log_opts()
    })
    .await;
    let canary = ostro_testkit::canary::Canary::new();
    let origin = Origin::start().await;
    let base = origin.base();

    deploy_named(&c, "pay-flow", "pay-flow").await;
    put_ok(
        &c,
        "/v1/secrets/pay_token",
        json!({ "value": canary.expose_str() }),
    )
    .await;
    put_ok(
        &c,
        "/v1/grants",
        json!({ "module": "pay-flow", "secret": "pay_token", "origins": [base], "body": false }),
    )
    .await;
    put_ok(&c, "/v1/egress/pay-flow", json!({ "allow": [base] })).await;
    c.api.set_flow("wq", json!({"placement": "workers"})).await;
    let w = c.spawn_worker(WorkerOpts {
        rust_log: Some(WORKER_TRACE.into()),
        ..WorkerOpts::new("secret-w1", 2)
    });
    c.wait_workers(1).await;

    let id = c
        .api
        .start("pay-flow", json!({"input": {"url": base}, "queue": "wq"}))
        .await;
    let r = c.api.wait_terminal(&id, P7_T).await;
    assert_eq!(r["status"], "completed", "{r}");
    assert_eq!(r["result"]["status"], 200, "{r}");
    assert_remote(&c, &id);

    // The origin received the expanded secret.
    let heads = origin.heads();
    assert_eq!(heads.len(), 1, "one /pay request: {}", heads.len());
    let auth: Vec<&str> = heads[0]
        .lines()
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.eq_ignore_ascii_case("authorization").then_some(v.trim())
        })
        .collect();
    assert!(
        auth == [format!("Bearer {}", canary.expose_str()).as_str()],
        "the origin did not get `Authorization: Bearer <canary>` ({} authorization headers)",
        auth.len()
    );
    assert!(heads[0].starts_with("POST /pay "), "request line");

    // The worker logged at TRACE (so the scan covers its most verbose output) and no
    // line, on either stream, carries the canary in any encoding.
    let (out, err) = (w.stdout(), w.stderr());
    assert!(
        out.iter().chain(err.iter()).any(|l| l.contains("TRACE")),
        "the worker did not log at TRACE"
    );
    let mut hits = canary.scan_bytes("worker stdout", out.join("\n").as_bytes());
    hits.extend(canary.scan_bytes("worker stderr", err.join("\n").as_bytes()));
    hits.extend(canary.scan_tree(w.cache.path()));
    assert!(
        hits.is_empty(),
        "canary on the worker: {:?}",
        hits.iter()
            .map(|h| format!("{} {} @{}", h.label, h.encoding, h.offset))
            .collect::<Vec<_>>()
    );
    eprintln!(
        "worker_e2e::secret_never_on_worker: {} worker lines scanned ({} bytes), total {:?}",
        out.len() + err.len(),
        out.iter()
            .chain(err.iter())
            .map(|l| l.len() + 1)
            .sum::<usize>(),
        t0.elapsed()
    );
    drop(w);
}
