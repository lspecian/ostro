//! 07-15: the Phase 7 CLI of the real `ostro` binary.
//!
//! - `verbs_*`: `ostro worker token`, `ostro queue`, `ostro run pause|resume` and
//!   `ostro workflow pause|resume` run as spawned processes against an in-process API
//!   server: `ostro_api::router` over [`P7Fake`], a small in-memory control plane
//!   implementing the Phase 7 methods with the 07-11 semantics (the `fake` feature of
//!   ostro-api is not enabled for this crate), served by ostro-server's client listener
//!   in plaintext mode. Each test asserts stdout, stderr and exit codes, and what the
//!   server received.
//! - `worker_*`: `ostro worker` argument and configuration handling, token redaction,
//!   and the SIGTERM path, through the spawned binary.
//! - `ready_line_*`: the TLS-aware server ready line (loopback `http://` unchanged, a
//!   non-loopback dev listener `https://` + fingerprint, D-W8) and the server-profile
//!   `auto` rule.

mod common;

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::*;
use ostro_api::domain::{
    ActivateWorkflow, HistoryPage, HistoryRequest, ListRunsRequest, ModuleInfo, RunInfo, RunPage,
    RunStatus, StartRunRequest, StartRunResult, UploadModule, UploadModuleResult, WorkflowInfo,
};
use ostro_api::phase7::{
    CreateWorkerTokenRequest, DemandSnapshot, PauseState, Placement, QueueFlow, QueueFlowInfo,
    RunPauseState, WorkerTokenCreated, WorkerTokenInfo,
};
use ostro_api::{ApiConfig, ApiError, ControlPlane};
use serde_json::Value;

// ---------------------------------------------------------------------------------
// In-memory Phase 7 control plane
// ---------------------------------------------------------------------------------

/// Secret part of every token the fake issues (must only ever appear in `create`).
const TOKEN_SECRET: &str = "0123456789abcdefSECRETpart000042";

#[derive(Default)]
struct State {
    flows: BTreeMap<String, QueueFlowInfo>,
    /// `expected_version` of every PUT, in order.
    put_versions: Vec<Option<u64>>,
    workflows_paused: BTreeMap<String, bool>,
    runs_paused: BTreeMap<String, bool>,
    run_details: Vec<Option<String>>,
    tokens: BTreeMap<String, WorkerTokenInfo>,
    next_token: u32,
}

#[derive(Default)]
struct P7Fake {
    st: Mutex<State>,
}

impl P7Fake {
    fn st(&self) -> std::sync::MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn flow(st: &State, q: &str) -> QueueFlowInfo {
        st.flows.get(q).cloned().unwrap_or_else(|| QueueFlowInfo {
            queue: q.to_owned(),
            version: 0,
            flow: QueueFlow::default(),
        })
    }

    fn set_queue_paused(&self, q: String, paused: bool) -> PauseState {
        let mut st = self.st();
        let mut f = Self::flow(&st, &q);
        let changed = f.flow.paused != paused;
        if changed {
            f.flow.paused = paused;
            f.version += 1;
            st.flows.insert(q.clone(), f);
        }
        PauseState {
            name: q,
            paused,
            changed,
        }
    }

    fn set_workflow_paused(&self, w: String, paused: bool) -> Result<PauseState, ApiError> {
        if w == "missing" {
            return Err(ApiError::not_found(format!("workflow {w} not found")));
        }
        let mut st = self.st();
        let prev = st
            .workflows_paused
            .insert(w.clone(), paused)
            .unwrap_or(false);
        Ok(PauseState {
            name: w,
            paused,
            changed: prev != paused,
        })
    }

    fn set_run_paused(&self, run: String, paused: bool) -> Result<RunPauseState, ApiError> {
        match run.as_str() {
            "done" => return Err(ApiError::run_terminal(&run, RunStatus::Completed)),
            "nope" => return Err(ApiError::not_found(format!("run {run} not found"))),
            _ => {}
        }
        let mut st = self.st();
        let prev = st.runs_paused.insert(run.clone(), paused).unwrap_or(false);
        Ok(RunPauseState {
            run_id: run,
            paused,
            changed: prev != paused,
        })
    }
}

fn unimpl<T>(m: &str) -> Result<T, ApiError> {
    Err(ApiError::unimplemented(m))
}

#[tonic::async_trait]
impl ControlPlane for P7Fake {
    async fn upload_module(&self, _: UploadModule) -> Result<UploadModuleResult, ApiError> {
        unimpl("upload_module")
    }
    async fn get_module(&self, _: String) -> Result<ModuleInfo, ApiError> {
        unimpl("get_module")
    }
    async fn list_workflows(&self) -> Result<Vec<WorkflowInfo>, ApiError> {
        Ok(Vec::new())
    }
    async fn activate_workflow(&self, _: ActivateWorkflow) -> Result<WorkflowInfo, ApiError> {
        unimpl("activate_workflow")
    }
    /// Every start is over the queue's rate limit (D-W21).
    async fn start_run(&self, _: StartRunRequest) -> Result<StartRunResult, ApiError> {
        Err(ApiError::rate_limited(
            "queue default is rate limited",
            1_500,
        ))
    }
    async fn get_run(&self, id: String) -> Result<RunInfo, ApiError> {
        Err(ApiError::not_found(format!("run {id} not found")))
    }
    async fn run_history(&self, _: HistoryRequest) -> Result<HistoryPage, ApiError> {
        unimpl("run_history")
    }
    async fn list_runs(&self, _: ListRunsRequest) -> Result<RunPage, ApiError> {
        unimpl("list_runs")
    }
    fn ready(&self) -> bool {
        true
    }

    async fn get_queue_flow(&self, queue: String) -> Result<QueueFlowInfo, ApiError> {
        Ok(Self::flow(&self.st(), &queue))
    }
    async fn put_queue_flow(
        &self,
        queue: String,
        flow: QueueFlow,
        expected_version: Option<u64>,
    ) -> Result<QueueFlowInfo, ApiError> {
        flow.validate()?;
        let mut st = self.st();
        st.put_versions.push(expected_version);
        let cur = Self::flow(&st, &queue);
        if let Some(v) = expected_version
            && v != cur.version
        {
            return Err(ApiError::precondition_failed(format!(
                "queue {queue}: version {v} is stale (current {})",
                cur.version
            )));
        }
        let next = QueueFlowInfo {
            queue: queue.clone(),
            version: cur.version + 1,
            flow,
        };
        st.flows.insert(queue, next.clone());
        Ok(next)
    }
    async fn pause_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        Ok(self.set_queue_paused(queue, true))
    }
    async fn resume_queue(&self, queue: String) -> Result<PauseState, ApiError> {
        Ok(self.set_queue_paused(queue, false))
    }
    async fn pause_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        self.set_workflow_paused(workflow, true)
    }
    async fn resume_workflow(&self, workflow: String) -> Result<PauseState, ApiError> {
        self.set_workflow_paused(workflow, false)
    }
    async fn pause_run(
        &self,
        run_id: String,
        detail: Option<String>,
    ) -> Result<RunPauseState, ApiError> {
        self.st().run_details.push(detail);
        self.set_run_paused(run_id, true)
    }
    async fn resume_run(&self, run_id: String) -> Result<RunPauseState, ApiError> {
        self.set_run_paused(run_id, false)
    }
    async fn queue_demand(&self, queue: String) -> Result<DemandSnapshot, ApiError> {
        Ok(DemandSnapshot {
            queue,
            demand: 7,
            backlog: 5,
            running_on_workers: 2,
            placement: Placement::Workers,
            spilling: false,
            workers: 1,
            worker_slots_free: 3,
        })
    }
    async fn create_worker_token(
        &self,
        req: CreateWorkerTokenRequest,
    ) -> Result<WorkerTokenCreated, ApiError> {
        req.validate()?;
        let mut st = self.st();
        st.next_token += 1;
        let token_id = format!("wkr_{:06}", st.next_token);
        let created_at_ms = 1_700_000_000_000;
        let expires_at_ms = created_at_ms + req.ttl_ms.unwrap_or(86_400_000);
        st.tokens.insert(
            token_id.clone(),
            WorkerTokenInfo {
                token_id: token_id.clone(),
                created_at_ms,
                expires_at_ms: Some(expires_at_ms),
            },
        );
        let token = format!("{token_id}.{TOKEN_SECRET}");
        Ok(WorkerTokenCreated {
            join_command: format!(
                "ostro worker --join https://h:7700 --token {token} --ca-hash sha256:{}",
                "ab".repeat(32)
            ),
            token_id,
            token,
            expires_at_ms,
        })
    }
    async fn list_worker_tokens(&self) -> Result<Vec<WorkerTokenInfo>, ApiError> {
        Ok(self.st().tokens.values().cloned().collect())
    }
    async fn revoke_worker_token(&self, token_id: String) -> Result<(), ApiError> {
        match self.st().tokens.remove(&token_id) {
            Some(_) => Ok(()),
            None => Err(ApiError::not_found(format!(
                "worker token {token_id} not found"
            ))),
        }
    }
}

/// The fake served on 127.0.0.1:0 (plaintext client listener); stops on drop.
struct FakeServer {
    addr: SocketAddr,
    fake: Arc<P7Fake>,
    /// Keeps the server's runtime alive.
    _rt: tokio::runtime::Runtime,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Drop for FakeServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeServer {
    fn start() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let fake = Arc::new(P7Fake::default());
        let cp: Arc<dyn ControlPlane> = fake.clone();
        let (addr, task) = rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind");
            let addr = listener.local_addr().expect("addr");
            let app = ostro_api::router(cp, ApiConfig::default());
            let task = tokio::spawn(ostro_server::tls::listener::serve(
                Arc::new(ostro_server::tls::TlsState::off()),
                listener,
                app,
                Default::default(),
            ));
            (addr, task)
        });
        Self {
            addr,
            fake,
            _rt: rt,
            task,
        }
    }

    /// `ostro <args>` against this server: (exit code, stdout, stderr).
    fn cli(&self, args: &[&str]) -> (i32, String, String) {
        run_cli(self.addr, args)
    }

    /// Like [`Self::cli`], asserting exit 0.
    fn ok(&self, args: &[&str]) -> String {
        let (code, out, err) = self.cli(args);
        assert_eq!(
            code, 0,
            "{args:?}: exit {code}\nstdout: {out}\nstderr: {err}"
        );
        out
    }

    fn json(&self, args: &[&str]) -> Value {
        let (code, body, err) = cli_json(self.addr, args);
        assert_eq!(code, 0, "{args:?} --json: exit {code}: {err}");
        body
    }
}

// ---------------------------------------------------------------------------------
// Admin verbs
// ---------------------------------------------------------------------------------

#[test]
fn verbs_worker_token_create_list_revoke() {
    let s = FakeServer::start();

    let out = s.ok(&["worker", "token", "create", "--ttl", "1h"]);
    assert!(out.contains("token id:   wkr_000001"), "{out}");
    assert!(out.contains("expires at: 2023-11-14T"), "{out}");
    assert!(
        out.contains(&format!(
            "ostro worker --join https://h:7700 --token wkr_000001.{TOKEN_SECRET}"
        )),
        "create prints the join command once: {out}"
    );
    let t = s.fake.st().tokens["wkr_000001"].clone();
    assert_eq!(t.expires_at_ms, Some(t.created_at_ms + 3_600_000));

    let created = s.json(&["worker", "token", "create"]);
    assert_eq!(created["token_id"], "wkr_000002");
    assert_eq!(created["token"], format!("wkr_000002.{TOKEN_SECRET}"));

    // list / ls: ids and times, never the secret.
    let out = s.ok(&["worker", "token", "list"]);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(lines.len(), 3, "{out}");
    assert!(lines[0].starts_with("TOKEN ID"), "{out}");
    assert!(
        lines[1].starts_with("wkr_000001  2023-11-14T22:13:20Z"),
        "{out}"
    );
    let listed = s.json(&["worker", "token", "ls"]);
    assert_eq!(listed["tokens"].as_array().map(Vec::len), Some(2));
    for text in [out, listed.to_string()] {
        assert!(
            !text.contains(TOKEN_SECRET),
            "list leaked the secret: {text}"
        );
    }

    assert_eq!(
        s.ok(&["worker", "token", "revoke", "wkr_000001"]),
        "revoked wkr_000001\n"
    );
    let (code, out, err) = s.cli(&["worker", "token", "revoke", "wkr_000001"]);
    assert_eq!(code, 1, "{out}{err}");
    assert!(
        err.contains("worker token wkr_000001 not found (HTTP 404 not_found)"),
        "{err}"
    );
    assert!(!err.contains(TOKEN_SECRET) && !out.contains(TOKEN_SECRET));
}

#[test]
fn verbs_queue_flow_get_set_file() {
    let s = FakeServer::start();
    let out = s.ok(&["queue", "flow", "q1"]);
    assert_eq!(
        out,
        "# queue q1 (version 0)\npaused = false\nselector = []\n"
    );

    // read-modify-PUT with the current version (0).
    let out = s.ok(&[
        "queue",
        "flow",
        "q1",
        "--set",
        "placement=workers",
        "--set",
        "selector=zone in (a,b)",
        "--set",
        "concurrency=8",
        "--set",
        "throttle.limit=5",
        "--set",
        "throttle.period_ms=1000",
    ]);
    assert!(out.starts_with("# queue q1 (version 1)\n"), "{out}");
    assert!(out.contains("placement = \"workers\"\n"), "{out}");
    assert!(
        out.contains("\n[throttle]\nburst = 0\nlimit = 5\nperiod_ms = 1000\n"),
        "{out}"
    );
    assert_eq!(s.fake.st().put_versions, vec![Some(0)]);

    let flow = s.json(&["queue", "flow", "q1"]);
    assert_eq!(flow["version"], 1);
    assert_eq!(flow["selector"], serde_json::json!(["zone in (a,b)"]));
    assert_eq!(flow["concurrency"], 8);

    // --set k=null clears a field; the PUT carries the current version (1).
    let flow = s.json(&["queue", "flow", "q1", "--set", "concurrency=null"]);
    assert_eq!(flow["version"], 2);
    assert!(flow.get("concurrency").is_none(), "{flow}");
    assert_eq!(flow["placement"], "workers");

    // --file: the human output is the file format.
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("flow.toml");
    let text = s.ok(&["queue", "flow", "q1"]).replace(
        "placement = \"workers\"",
        "placement = \"auto\"\nweight = 3",
    );
    std::fs::write(&path, text).expect("write flow file");
    let p = path.to_str().expect("utf-8 path");
    let flow = s.json(&["queue", "flow", "q1", "--file", p, "--set", "weight=4"]);
    assert_eq!(flow["version"], 3);
    assert_eq!(flow["placement"], "auto");
    assert_eq!(flow["weight"], 4);
    assert_eq!(flow["throttle"]["limit"], 5);
    assert_eq!(s.fake.st().put_versions, vec![Some(0), Some(1), Some(2)]);

    // Server-side validation: 400 invalid_argument, exit 1, nothing stored.
    let (code, _, err) = s.cli(&["queue", "flow", "q1", "--set", "spill.cpu_pct=150"]);
    assert_eq!(code, 1, "{err}");
    assert!(err.contains("(HTTP 400 invalid_argument)"), "{err}");
    assert_eq!(s.json(&["queue", "flow", "q1"])["version"], 3);

    // Local usage errors: exit 2.
    let (code, _, err) = s.cli(&["queue", "flow", "q1", "--set", "novalue"]);
    assert_eq!(code, 2, "{err}");
    let missing = dir.path().join("missing.toml");
    let (code, _, err) = s.cli(&[
        "queue",
        "flow",
        "q1",
        "--file",
        missing.to_str().expect("utf-8"),
    ]);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("cannot read"), "{err}");
}

#[test]
fn verbs_queue_pause_resume_demand() {
    let s = FakeServer::start();
    assert_eq!(s.ok(&["queue", "pause", "q1"]), "queue q1 paused\n");
    assert_eq!(s.ok(&["queue", "pause", "q1"]), "queue q1 already paused\n");
    assert_eq!(s.json(&["queue", "flow", "q1"])["paused"], true);
    let st = s.json(&["queue", "resume", "q1"]);
    assert_eq!(
        st,
        serde_json::json!({"name": "q1", "paused": false, "changed": true})
    );
    assert_eq!(
        s.ok(&["queue", "resume", "q1"]),
        "queue q1 already resumed\n"
    );

    let out = s.ok(&["queue", "demand", "q1"]);
    let d: Value = serde_json::from_str(&out).expect("demand is JSON");
    assert_eq!(
        d,
        serde_json::json!({
            "queue": "q1", "demand": 7, "backlog": 5, "running_on_workers": 2,
            "placement": "workers", "spilling": false, "workers": 1, "worker_slots_free": 3
        })
    );
}

#[test]
fn verbs_run_and_workflow_pause_resume() {
    let s = FakeServer::start();
    assert_eq!(
        s.ok(&["run", "pause", "r1", "--detail", "investigating"]),
        "run r1 paused\n"
    );
    assert_eq!(s.ok(&["run", "pause", "r1"]), "run r1 already paused\n");
    assert_eq!(
        s.fake.st().run_details,
        vec![Some("investigating".to_owned()), None]
    );
    let st = s.json(&["run", "resume", "r1"]);
    assert_eq!(
        st,
        serde_json::json!({"run_id": "r1", "paused": false, "changed": true})
    );

    let (code, _, err) = s.cli(&["run", "pause", "done"]);
    assert_eq!(code, 1);
    assert!(
        err.contains("HTTP 409 conflict, reason run_terminal"),
        "{err}"
    );
    let (code, body, _) = cli_json(s.addr, &["run", "resume", "nope"]);
    assert_eq!(code, 1);
    assert_eq!(body["error"]["code"], "not_found");

    assert_eq!(
        s.ok(&["workflow", "pause", "orders"]),
        "workflow orders paused\n"
    );
    assert_eq!(
        s.ok(&["workflow", "resume", "orders"]),
        "workflow orders resumed\n"
    );
    assert_eq!(
        s.ok(&["workflow", "resume", "orders"]),
        "workflow orders already resumed\n"
    );
    let (code, _, err) = s.cli(&["workflow", "pause", "missing"]);
    assert_eq!(code, 1);
    assert!(err.contains("workflow missing not found"), "{err}");
}

#[test]
fn verbs_rate_limited_start_shows_retry_after() {
    let s = FakeServer::start();
    let (code, out, err) = s.cli(&["run", "w", "--input", "1"]);
    assert_eq!(code, 1, "{out}{err}");
    assert!(err.contains("(HTTP 429 rate_limited)"), "{err}");
    assert!(
        err.contains("retry after 2 s (Retry-After), 1500 ms"),
        "{err}"
    );
}

#[test]
fn verbs_usage_errors_are_exit_2() {
    // clap rejects these before any request (nothing listens on the address).
    let addr = free_addr();
    for args in [
        &["run", "pause"][..],
        &["run", "resume"][..],
        &["workflow", "pause"][..],
        &["workflow", "stop", "w"][..],
        &["queue", "flow"][..],
        &["queue", "drain", "q"][..],
        &["worker", "token", "revoke"][..],
        &["worker", "token", "create", "--ttl", "1d"][..],
    ] {
        let (code, _, err) = run_cli(addr, args);
        assert_eq!(code, 2, "{args:?}: {err}");
    }
    let mut cmd = command();
    cmd.arg("--help");
    let (st, out, _) = run_to_end(cmd);
    assert!(st.success());
    for verb in ["worker", "queue", "workflow", "run"] {
        assert!(
            out.lines()
                .any(|l| l.split_whitespace().next() == Some(verb)),
            "--help lacks {verb}: {out}"
        );
    }
}

// ---------------------------------------------------------------------------------
// ostro worker
// ---------------------------------------------------------------------------------

const WORKER_SECRET: &str = "wkr_abc123.WORKER_SECRET_VALUE_42";

#[test]
fn worker_help_lists_every_flag() {
    let mut cmd = command();
    cmd.args(["worker", "--help"]);
    let (st, out, err) = run_to_end(cmd);
    assert!(st.success(), "{err}");
    for flag in [
        "--join",
        "--token",
        "--token-file",
        "--ca-hash",
        "--labels",
        "--slots",
        "--cache-dir",
        "--memory",
        "--tls-cert",
        "--tls-key",
        "--tls-ca",
        "--endpoint",
        "--worker-id",
        "--config",
        "--print-config",
    ] {
        assert!(out.contains(flag), "worker --help lacks {flag}: {out}");
    }
    assert!(
        out.contains("token"),
        "worker --help lacks the token subcommand: {out}"
    );
}

#[test]
fn worker_missing_join_or_token_is_exit_2() {
    for (args, needle) in [
        (&["worker"][..], "worker.join is required"),
        (
            &["worker", "--join", "https://h:7700"][..],
            "worker.token is required",
        ),
        (
            &["worker", "--join", "https://h:7700", "--token", "nope"][..],
            "wkr_<id>.<secret>",
        ),
        (
            &["worker", "--tls-cert", "c.pem", "--join", "https://h:7700"][..],
            "must be set together",
        ),
        (
            &["worker", "--config", "/nonexistent/worker.toml"][..],
            "config file not found",
        ),
    ] {
        let mut cmd = command();
        cmd.args(args);
        let (st, out, err) = run_to_end(cmd);
        assert_eq!(st.code(), Some(2), "{args:?}: {out}{err}");
        assert!(err.contains(needle), "{args:?}: {err}");
    }
    // A bad flag value is a clap usage error too.
    let mut cmd = command();
    cmd.args(["worker", "--memory", "lots"]);
    assert_eq!(run_to_end(cmd).0.code(), Some(2));
}

#[test]
fn worker_print_config_redacts_the_token() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = dir.path().join("worker.toml");
    std::fs::write(
        &file,
        "[worker]\njoin = \"https://from-file:7700\"\nslots = 2\n",
    )
    .expect("write config");
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, format!("{WORKER_SECRET}\n")).expect("write token");

    // file < env < flags; the token from each source is redacted.
    let cases: [(&[&str], Option<&str>); 3] = [
        (&["--token", WORKER_SECRET], None),
        (&["--token-file", token_file.to_str().expect("utf-8")], None),
        (&[], Some(WORKER_SECRET)),
    ];
    for (extra, env_token) in cases {
        let mut cmd = command();
        cmd.args(["worker", "--config", file.to_str().expect("utf-8")])
            .args(["--labels", "zone=a,gpu=yes", "--memory", "512MiB"])
            .args(["--ca-hash", &format!("sha256:{}", "0f".repeat(32))])
            .args(extra)
            .arg("--print-config")
            .env("OSTRO_WORKER_SLOTS", "3");
        if let Some(t) = env_token {
            cmd.env("OSTRO_WORKER_TOKEN", t);
        }
        let (st, out, err) = run_to_end(cmd);
        assert!(st.success(), "{extra:?}: {err}");
        assert!(out.starts_with("[worker]\n"), "{out}");
        assert!(out.contains("token = \"<redacted>\"\n"), "{extra:?}: {out}");
        assert!(out.contains("join = \"https://from-file:7700\"\n"), "{out}");
        assert!(out.contains("slots = 3\n"), "env overrides the file: {out}");
        assert!(out.contains("memory_bytes = 536870912\n"), "{out}");
        assert!(
            out.contains("labels = { \"gpu\" = \"yes\", \"zone\" = \"a\" }\n"),
            "{out}"
        );
        assert!(!out.contains("WORKER_SECRET_VALUE") && !err.contains("WORKER_SECRET_VALUE"));
    }
}

#[test]
fn worker_join_failure_exits_1_without_leaking_the_token() {
    // Nothing listens on this loopback port: the join fails (fatal at first start).
    let addr = free_addr();
    let mut cmd = command();
    cmd.args(["worker", "--join", &format!("http://{addr}")])
        .env("OSTRO_WORKER_TOKEN", WORKER_SECRET)
        .env("RUST_LOG", "debug");
    let (st, out, err) = run_to_end(cmd);
    assert_eq!(st.code(), Some(1), "{out}{err}");
    assert!(err.contains("join failed"), "{err}");
    assert!(
        !out.contains("WORKER_SECRET_VALUE") && !err.contains("WORKER_SECRET_VALUE"),
        "token leaked:\n{out}\n{err}"
    );
}

#[test]
fn worker_sigterm_while_joining_exits_0() {
    // A listener that never answers: the join request hangs until the signal.
    let blackhole = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = blackhole.local_addr().expect("addr");
    for sig in ["TERM", "INT"] {
        let mut cmd = command();
        cmd.args(["worker", "--join", &format!("http://{addr}")])
            .args(["--worker-id", "w-sig"])
            .env("OSTRO_WORKER_TOKEN", WORKER_SECRET);
        let mut w = Spawned::spawn(cmd);
        // Let it reach the join request.
        std::thread::sleep(Duration::from_millis(500));
        w.signal(sig);
        let st = w.wait_exit(ostro_worker::drain::DRAIN_BOUND + Duration::from_secs(10));
        assert!(st.success(), "SIG{sig}: {st:?}; stderr: {}", w.stderr());
        let all = format!("{}{}", w.stdout_lines().join("\n"), w.stderr());
        assert!(!all.contains("WORKER_SECRET_VALUE"), "{all}");
        assert!(
            !all.contains("ostro worker ready:"),
            "no session, no ready line: {all}"
        );
    }
    drop(blackhole);
}

// ---------------------------------------------------------------------------------
// Ready lines
// ---------------------------------------------------------------------------------

/// `NAME listening on https://ADDR (sha256 fingerprint HEX) (data: ...) ready in N ms`.
fn parse_tls_ready(line: &str) -> Option<(SocketAddr, String)> {
    let (_, rest) = line.split_once(" listening on https://")?;
    let (addr, rest) = rest.split_once(" (sha256 fingerprint ")?;
    let (hex, rest) = rest.split_once(") (data: ")?;
    rest.split_once(") ready in ")?;
    Some((addr.parse().ok()?, hex.to_owned()))
}

fn wait_line(s: &Spawned, pred: impl Fn(&str) -> bool) -> String {
    let deadline = std::time::Instant::now() + T;
    loop {
        if let Some(l) = s.stdout_lines().into_iter().find(|l| pred(l)) {
            return l;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no matching stdout line within {T:?}; stdout: {:?}; stderr: {}",
            s.stdout_lines(),
            s.stderr()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[tokio::test]
async fn ready_line_loopback_stays_http() {
    let _g = SERIAL.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = Spawned::spawn(dev(dir.path(), &["--ephemeral", "--listen", "127.0.0.1:0"]));
    let r = s.ready_line(T);
    assert!(r.addr.ip().is_loopback());
    let line = wait_line(&s, |l| l.contains(" listening on "));
    assert!(line.contains(" listening on http://127.0.0.1:"), "{line}");
    assert!(!line.contains("fingerprint"), "{line}");
    s.stop_clean("TERM");
}

#[tokio::test]
async fn ready_line_non_loopback_dev_is_https_with_fingerprint() {
    let _g = SERIAL.lock().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let mut s = Spawned::spawn(dev(dir.path(), &["--ephemeral", "--listen", "0.0.0.0:0"]));
    let line = wait_line(&s, |l| l.contains(" listening on "));
    let (addr, hex) =
        parse_tls_ready(&line).unwrap_or_else(|| panic!("not a TLS ready line: {line}"));
    assert!(addr.ip().is_unspecified(), "{line}");
    assert_eq!(hex.len(), 64, "{line}");
    assert!(
        hex.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()),
        "{line}"
    );
    // The listener really serves TLS (self-signed, so verification is off here).
    let client = reqwest::Client::builder()
        .no_proxy()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(10))
        .build()
        .expect("client");
    let url = format!("https://127.0.0.1:{}/readyz", addr.port());
    let deadline = std::time::Instant::now() + T;
    loop {
        match client.get(&url).send().await {
            Ok(r) if r.status() == reqwest::StatusCode::OK => break,
            other => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "{url}: {other:?}; stderr: {}",
                    s.stderr()
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    s.stop_clean("TERM");
}

#[test]
fn ready_line_server_profile_auto_rule_ignores_auto_sealing_key() {
    // 07-06 follow-up: `secrets.auto_sealing_key = true` must not turn the server
    // profile's non-loopback `auto` listener into a self-signed one.
    let dir = tempfile::tempdir().expect("tempdir");
    let data = dir.path().join("data");
    let mut cmd = server(
        dir.path(),
        &[
            "--listen",
            "0.0.0.0:0",
            "--data-dir",
            data.to_str().expect("utf-8"),
        ],
    );
    cmd.env("OSTRO_SECRETS__AUTO_SEALING_KEY", "true");
    let (st, out, err) = run_to_end(cmd);
    assert_eq!(st.code(), Some(1), "{out}{err}");
    assert!(
        err.contains(ostro_server::tls::AUTO_NON_LOOPBACK_ERROR),
        "{err}"
    );
    assert!(!out.contains("listening on"), "{out}");
}
