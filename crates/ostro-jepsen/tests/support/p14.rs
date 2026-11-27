//! 14-22 shared code of `jepsen_smoke` and `jepsen_gate` (included with `#[path]`): the
//! 1.0 configuration of the harness cluster (token auth with a bootstrap admin bearer, the
//! audit journal on), the all-classes nemesis menu, the operator (audited admin actions
//! through random nodes while the faults run), the post-run audit verification and the
//! report helpers.
//!
//! **Auth.** Harness nodes start with `[auth] mode = "none"` (14-03); here every node gets
//! `OSTRO_AUTH__MODE=token` and `OSTRO_BOOTSTRAP_TOKEN=<bearer>` through
//! `RunSpec::proc_options.env`, so the cluster bootstraps one system-admin token with a
//! bearer the test knows. The bearer is `OSTRO_JEPSEN_TOKEN` when set (it must then be a
//! valid `ostro_otk_…` bearer), else a fresh one; it is the harness token
//! (`RunSpec::auth`): deploy, workload, observation and the 14-10 test endpoints carry it.
//! The operator creates tenant `_` tokens with `trigger` + `read` and starts runs with them.
//!
//! **Audit.** `[audit] enabled = true` (`OSTRO_AUDIT__ENABLED`). After the run (cluster
//! stopped with `kill -9`, directory kept by `RunSpec::keep_dirs`) every node's
//! `<data>/audit` is opened the way a restart opens it (torn tail repaired), verified with
//! `audit::verify::verify_dir`, and scanned: every acknowledged operator action is in the
//! nodes' journals exactly once, an unacknowledged one at most once, and every cancel the
//! workload got acknowledged has an `ok` `run.cancel` record.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use ostro_jepsen::checker::Verdict;
use ostro_jepsen::history::{ClientOp, OpKind, Outcome, read_jsonl};
use ostro_jepsen::nemesis::clock::ClockNemesis;
use ostro_jepsen::nemesis::disk::DiskNemesis;
use ostro_jepsen::nemesis::partition;
use ostro_jepsen::report::RunReport;
use ostro_jepsen::run::{LocalBoxFuture, Nemesis, NemesisKind, RunSpec, TOKEN_ENV};
use ostro_jepsen::workload::{WORKFLOW, pause, plan_input};
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness};
use ostro_repl_conformance::scenarios::process_p13::{HEAL_T, wait_healthy};
use ostro_server::audit::journal::{AUDIT_RECOVERED, Record};
use ostro_server::audit::segment::{Journal, list_segments};
use ostro_server::audit::verify::verify_dir;
use ostro_types::audit::AuditOutcome;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

/// Bound on one operator request.
const REQ_T: Duration = Duration::from_secs(10);
/// Gap between two operator rounds (a token creation and a cancel each).
pub const OPERATOR_EVERY: Duration = Duration::from_secs(30);
/// Report note prefix of a kept cluster directory (`run.rs`).
const KEPT: &str = "cluster directory kept: ";
/// Name of the operator's starter slot (pinned at the start of the fault phase).
pub const OPERATOR_SLOT: &str = "operator:start";

/// The bootstrap admin bearer: `OSTRO_JEPSEN_TOKEN`, else a fresh `ostro_otk_…` bearer.
pub fn bootstrap_bearer() -> String {
    match std::env::var(TOKEN_ENV)
        .ok()
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
    {
        Some(t) => t,
        None => {
            let (_, _, b) = ostro_server::p14::tokens::generate_os().expect("OS random source");
            b.expose().to_owned()
        }
    }
}

/// Fault class of a nemesis name (`partition`, `kill`, `clock`, `disk`, `restart`; `None`
/// for the operator slot).
pub fn class_of(name: &str) -> Option<&str> {
    let c = name.split(':').next().unwrap_or(name);
    (c != "operator").then_some(c)
}

/// The all-classes menu, weighted so each class is picked equally often: four partition
/// modes, four kill targets, three clock faults (bump ≤ ±60 s, strobe, drift ±5%), the
/// disk stall (100..=2,000 ms on `wal::fsync` / `state::commit` / `state::persist`).
pub fn menu(token: &str) -> Vec<NemesisKind> {
    let mut v = Vec::new();
    for _ in 0..3 {
        v.extend(NemesisKind::partitions());
        v.extend(NemesisKind::kills());
    }
    for _ in 0..4 {
        v.extend(ClockNemesis::menu(Some(token.to_owned())));
    }
    let disk = NemesisKind::Custom(Arc::new(DiskNemesis::new(Some(token.to_owned()))));
    v.extend(std::iter::repeat_n(disk, 12));
    v
}

/// The 1.0 run spec: `RunSpec::from_env` with the menu, faults every 5..=15 s, token auth
/// with the bootstrap bearer, the audit journal on, the operator pinned at the start of the
/// fault phase, the cluster directory kept for the audit verification. `seed` = a fixed
/// seed (the gate's loop) instead of `OSTRO_JEPSEN_SEED` / `default_seed`.
pub fn spec(default_seed: u64, default_secs: u64, seed: Option<u64>) -> (RunSpec, Arc<Operator>) {
    let token = bootstrap_bearer();
    let mut spec = RunSpec::from_env(Some(default_seed), default_secs, menu(&token));
    if let Some(s) = seed {
        spec.seed = s;
        spec.out_dir = ostro_jepsen::report::seed_dir(s);
    }
    spec.auth = Some(token.clone());
    spec.interval = (Duration::from_secs(5), Duration::from_secs(15));
    spec.keep_dirs = true;
    spec.proc_options.env.extend([
        ("OSTRO_AUTH__MODE".to_owned(), "token".to_owned()),
        ("OSTRO_BOOTSTRAP_TOKEN".to_owned(), token.clone()),
        ("OSTRO_AUDIT__ENABLED".to_owned(), "true".to_owned()),
    ]);
    let op = Arc::new(Operator::new(spec.seed, token, spec.duration));
    spec.pinned.push((
        Duration::ZERO,
        Duration::ZERO,
        NemesisKind::Custom(Arc::clone(&op) as Arc<dyn Nemesis>),
    ));
    (spec, op)
}

// ---------------------------------------------------------------------------------
// Operator
// ---------------------------------------------------------------------------------

/// One audited action the operator issued.
#[derive(Clone, Debug)]
pub struct OpAction {
    /// Audit action name (`token.create`, `run.cancel`).
    pub action: &'static str,
    /// Audit target (token name, run id).
    pub target: String,
    /// The node it was sent to.
    pub node: u64,
    /// 2xx.
    pub acked: bool,
    /// `http <status>` or the transport error.
    pub answer: String,
}

/// What the operator did.
#[derive(Debug, Default)]
pub struct OperatorLog {
    /// Audited actions in issue order.
    pub actions: Vec<OpAction>,
    /// Tenant-token starts by answer (`http 201`, `http 401`, `error`, ...).
    pub tenant_starts: BTreeMap<String, u64>,
}

/// Audited operator actions (a tenant `_` token creation, a tenant-token start and an admin
/// cancel of that run, every [`OPERATOR_EVERY`]) through random nodes, from the first fault
/// slot until the fault phase ends. Its runs use the key prefix `op<seed>-` (the lost
/// checker sees their keys in the logs and checks only the workload's acknowledged runs).
pub struct Operator {
    seed: u64,
    token: String,
    span: Duration,
    log: Arc<Mutex<OperatorLog>>,
    stop: Arc<AtomicBool>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Operator {
    fn new(seed: u64, token: String, span: Duration) -> Self {
        Self {
            seed,
            token,
            span,
            log: Arc::new(Mutex::new(OperatorLog::default())),
            stop: Arc::new(AtomicBool::new(false)),
            task: Mutex::new(None),
        }
    }

    /// Stop the rounds and wait for the current one; the log.
    pub async fn finish(&self) -> OperatorLog {
        self.stop.store(true, Ordering::SeqCst);
        let t = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(t) = t {
            let _ = t.await;
        }
        std::mem::take(&mut *self.log.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl Nemesis for Operator {
    fn name(&self) -> String {
        OPERATOR_SLOT.to_owned()
    }

    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        _pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let mut g = self.task.lock().unwrap_or_else(PoisonError::into_inner);
            if g.is_none() {
                let apis: Vec<(u64, ApiClient)> =
                    c.node_ids().into_iter().map(|n| (n, c.client(n))).collect();
                // clock-exempt: harness timing
                let deadline = Instant::now() + self.span;
                *g = Some(tokio::spawn(operator_loop(
                    self.seed,
                    self.token.clone(),
                    apis,
                    Arc::clone(&self.log),
                    Arc::clone(&self.stop),
                    deadline,
                )));
            }
            Ok(format!(
                "operator rounds every {} s",
                OPERATOR_EVERY.as_secs()
            ))
        })
    }

    fn heal<'a>(&'a self, _c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async { Ok("operator running".to_owned()) })
    }
}

enum Answer {
    Status(u16, Value),
    Error(String),
}

impl Answer {
    fn label(&self) -> String {
        match self {
            Answer::Status(st, _) => format!("http {st}"),
            Answer::Error(e) => format!("error: {e}"),
        }
    }

    fn ok(&self) -> Option<&Value> {
        match self {
            Answer::Status(st, v) if (200..300).contains(st) => Some(v),
            _ => None,
        }
    }
}

async fn post(api: &ApiClient, bearer: &str, path: &str, body: &Value) -> Answer {
    let r = api
        .http
        .post(format!("{}{path}", api.base_url))
        .bearer_auth(bearer)
        .json(body)
        .timeout(REQ_T)
        .send()
        .await;
    match r {
        Ok(r) => {
            let st = r.status().as_u16();
            let v = r
                .bytes()
                .await
                .ok()
                .and_then(|b| serde_json::from_slice(&b).ok())
                .unwrap_or(Value::Null);
            Answer::Status(st, v)
        }
        Err(e) => Answer::Error(e.to_string()),
    }
}

async fn operator_loop(
    seed: u64,
    admin: String,
    apis: Vec<(u64, ApiClient)>,
    log: Arc<Mutex<OperatorLog>>,
    stop: Arc<AtomicBool>,
    deadline: Instant,
) {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x6f70_6572_6174_6f72);
    let mut tenant: Option<String> = None;
    let mut i = 0u64;
    let record = |a: OpAction| {
        log.lock()
            .unwrap_or_else(PoisonError::into_inner)
            .actions
            .push(a)
    };
    // clock-exempt: harness timing
    while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
        // clock-exempt: harness timing
        let round = Instant::now();
        i += 1;
        // 1. A tenant `_` token (trigger + read) through a random node.
        let (n, api) = &apis[rng.random_range(0..apis.len())];
        let name = format!("jepsen-{seed}-{i}");
        let body = json!({"name": name, "scopes": ["trigger", "read"], "tenant": "_",
            "ttl_ms": 3_600_000u64, "description": "14-22 operator"});
        let a = post(api, &admin, "/v1/auth/tokens", &body).await;
        if let Some(b) = a.ok().and_then(|v| v["bearer"].as_str()) {
            tenant = Some(b.to_owned());
        }
        record(OpAction {
            action: "token.create",
            target: name,
            node: *n,
            acked: a.ok().is_some(),
            answer: a.label(),
        });
        // 2. A run started with the tenant token (admin fallback), retried briefly.
        let key = format!("op{seed}-{i}");
        let body = json!({"input": plan_input(seed, &key), "idempotency_key": key});
        let path = format!("/v1/workflows/{WORKFLOW}/runs");
        let mut run = None;
        // clock-exempt: harness timing
        let t0 = Instant::now();
        while run.is_none()
            && t0.elapsed() < Duration::from_secs(20)
            && !stop.load(Ordering::SeqCst)
        {
            let (_, api) = &apis[rng.random_range(0..apis.len())];
            let (bearer, by_tenant) = match &tenant {
                Some(t) => (t.as_str(), true),
                None => (admin.as_str(), false),
            };
            let a = post(api, bearer, &path, &body).await;
            if by_tenant {
                *log.lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .tenant_starts
                    .entry(a.label().split(':').next().unwrap_or("").to_owned())
                    .or_default() += 1;
            }
            run = a.ok().and_then(|v| v["run_id"].as_str()).map(str::to_owned);
            if run.is_none() {
                pause(Duration::from_millis(500)).await;
            }
        }
        // 3. Its cancel with the admin token through a random node (issued once).
        if let Some(run) = run {
            let (n, api) = &apis[rng.random_range(0..apis.len())];
            let a = post(api, &admin, &format!("/v1/runs/{run}/cancel"), &json!({})).await;
            record(OpAction {
                action: "run.cancel",
                target: run,
                node: *n,
                acked: a.ok().is_some(),
                answer: a.label(),
            });
        }
        if let Some(rest) = OPERATOR_EVERY.checked_sub(round.elapsed()) {
            // clock-exempt: harness timing
            let until = (Instant::now() + rest).min(deadline);
            // clock-exempt: harness timing
            while !stop.load(Ordering::SeqCst) && Instant::now() < until {
                pause(Duration::from_millis(250)).await;
            }
        }
    }
}

// ---------------------------------------------------------------------------------
// Extra gate nemeses
// ---------------------------------------------------------------------------------

/// Quorum loss by partition: every node cut from both others (no majority anywhere) for
/// the hold; the checkers expect unavailability, not loss.
pub struct QuorumLossPartition;

impl Nemesis for QuorumLossPartition {
    fn name(&self) -> String {
        "partition:quorum_loss".to_owned()
    }

    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        _pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let nodes = c.node_ids();
            for n in &nodes {
                c.proxy().isolate(*n);
            }
            Ok(format!("every node of {nodes:?} isolated"))
        })
    }

    fn heal<'a>(&'a self, c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            partition::heal(c);
            Ok("healed".to_owned())
        })
    }
}

/// A rolling restart: each node in turn stopped with SIGTERM (controlled shutdown),
/// restarted and the cluster waited healthy before the next.
pub struct RollingRestart;

impl Nemesis for RollingRestart {
    fn name(&self) -> String {
        "restart:rolling".to_owned()
    }

    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        _pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let mut out = Vec::new();
            for n in c.node_ids() {
                if c.pid(n).is_none() {
                    return Err(format!("node {n} is down before its restart"));
                }
                let clean = c
                    .stop_gracefully(n)
                    .await
                    .map_err(|e| format!("stop node {n}: {e}"))?;
                c.restart(n)
                    .await
                    .map_err(|e| format!("restart node {n}: {e}\n{}", c.log_tail(n, 30)))?;
                let h = wait_healthy(c, &[], HEAL_T, "rolling restart")
                    .await
                    .map_err(|f| format!("node {n}: {}: {}", f.oracle, f.detail))?;
                out.push(format!(
                    "node {n} ({}; healthy after {} ms)",
                    if clean {
                        "clean exit"
                    } else {
                        "SIGKILL fallback"
                    },
                    h.as_millis()
                ));
            }
            Ok(out.join(", "))
        })
    }

    fn heal<'a>(&'a self, _c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async { Ok("restarted".to_owned()) })
    }
}

// ---------------------------------------------------------------------------------
// After the run
// ---------------------------------------------------------------------------------

/// The cluster directory the run kept (`RunSpec::keep_dirs`).
pub fn kept_dir(report: &RunReport) -> Option<PathBuf> {
    report
        .notes
        .iter()
        .find_map(|n| n.strip_prefix(KEPT))
        .map(PathBuf::from)
}

/// Verify every node's audit journal under `root` and the operator's / workload's audited
/// actions (module docs). Counts: `records`, `audit_recovered`, `torn_bytes_repaired`,
/// `operator_acked`, `operator_unacked`, `workload_cancels`, `actions:<name>`.
pub fn check_audit(root: &Path, nodes: &[u64], op: &OperatorLog, ops: &[ClientOp]) -> Verdict {
    let mut v = Verdict::new("audit");
    let mut all: Vec<Record> = Vec::new();
    for n in nodes {
        let dir = root.join(format!("node{n}")).join("audit");
        if !dir.is_dir() {
            v.violation(format!("node {n}: no audit journal at {}", dir.display()));
            continue;
        }
        // Open it the way a restart does: a torn tail from the final kill -9 is repaired.
        match Journal::open(&dir, *n, 0, u64::MAX) {
            Ok(j) => v.add("torn_bytes_repaired", j.recovery().truncated_bytes),
            Err(e) => {
                v.violation(format!("node {n}: journal open: {e}"));
                continue;
            }
        }
        match verify_dir(&dir) {
            Ok(r) => {
                v.add("segments", r.segments);
                v.note(format!(
                    "node {n}: {} records in {} segments verified (seq {}..={})",
                    r.records, r.segments, r.first_seq, r.last_seq
                ));
            }
            Err(e) => v.violation(format!("node {n}: verify: seq {}: {}", e.seq, e.reason)),
        }
        let segs = match list_segments(&dir) {
            Ok(s) => s,
            Err(e) => {
                v.violation(format!("node {n}: list segments: {e}"));
                continue;
            }
        };
        for (_, path) in segs {
            let bytes = match std::fs::read(&path) {
                Ok(b) => b,
                Err(e) => {
                    v.violation(format!("{}: {e}", path.display()));
                    continue;
                }
            };
            for line in bytes.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
                match Record::parse_line(line) {
                    Ok(r) => all.push(r),
                    Err(e) => v.violation(format!("{}: {e}", path.display())),
                }
            }
        }
    }
    v.add("records", all.len() as u64);
    let mut by: BTreeMap<(&str, &str), Vec<&Record>> = BTreeMap::new();
    for r in &all {
        if r.action == AUDIT_RECOVERED {
            v.add("audit_recovered", 1);
        }
        v.add(&format!("actions:{}", r.action), 1);
        by.entry((r.action.as_str(), r.target.as_str()))
            .or_default()
            .push(r);
    }
    for a in &op.actions {
        let recs = by
            .get(&(a.action, a.target.as_str()))
            .map_or(&[][..], |r| r);
        let oks = recs
            .iter()
            .filter(|r| r.outcome == AuditOutcome::Ok)
            .count();
        if a.acked {
            v.add("operator_acked", 1);
            if recs.len() != 1 || oks != 1 {
                v.violation(format!(
                    "acknowledged {} {} (node {}, {}): {} records ({} ok), expected exactly one ok",
                    a.action,
                    a.target,
                    a.node,
                    a.answer,
                    recs.len(),
                    oks
                ));
            }
        } else {
            v.add("operator_unacked", 1);
            if recs.len() > 1 {
                v.violation(format!(
                    "unacknowledged {} {} (node {}, {}): {} records, expected at most one",
                    a.action,
                    a.target,
                    a.node,
                    a.answer,
                    recs.len()
                ));
            }
        }
    }
    let cancels: std::collections::BTreeSet<&str> = ops
        .iter()
        .filter(|o| o.kind == OpKind::Cancel && o.outcome == Outcome::Ok)
        .filter_map(|o| o.run_id.as_deref())
        .collect();
    for run in cancels {
        v.add("workload_cancels", 1);
        let ok = by
            .get(&("run.cancel", run))
            .is_some_and(|r| r.iter().any(|r| r.outcome == AuditOutcome::Ok));
        if !ok {
            v.violation(format!(
                "acknowledged workload cancel of {run} has no ok run.cancel record"
            ));
        }
    }
    v
}

/// Faults executed by class (the operator slot excluded).
pub fn faults_by_class(report: &RunReport) -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    for (k, n) in &report.faults {
        if let Some(c) = class_of(k) {
            *m.entry(c.to_owned()).or_default() += n;
        }
    }
    m
}

/// The longest gap (ms) between two consecutive acknowledged writes (start / signal /
/// cancel) of the run's history, with its start (ms on the history clock).
pub fn max_unavailability(dir: &Path) -> (u64, u64) {
    let ops: Vec<ClientOp> = read_jsonl(&dir.join("history.jsonl")).unwrap_or_default();
    let mut t: Vec<u64> = ops
        .iter()
        .filter(|o| o.outcome == Outcome::Ok && o.kind != OpKind::Result)
        .map(|o| o.complete_ms)
        .collect();
    t.sort_unstable();
    t.windows(2)
        .map(|w| (w[1] - w[0], w[0]))
        .max()
        .unwrap_or((0, 0))
}

/// Shared post-run steps: operator log, audit verification over the kept directory (then
/// removed when everything passed), the run's ops. Returns the audit verdict.
pub async fn after_run(report: &RunReport, op: &Operator, dir: &Path) -> (Verdict, OperatorLog) {
    let log = op.finish().await;
    let ops: Vec<ClientOp> = read_jsonl(&dir.join("history.jsonl")).unwrap_or_default();
    let Some(root) = kept_dir(report) else {
        let mut v = Verdict::new("audit");
        v.violation("the run kept no cluster directory (keep_dirs)");
        return (v, log);
    };
    let v = check_audit(&root, &[1, 2, 3], &log, &ops);
    if report.passed && v.ok() {
        let _ = std::fs::remove_dir_all(&root);
    } else {
        println!("cluster directory kept for inspection: {}", root.display());
    }
    (v, log)
}

/// Assert every verdict of a run plus the audit verdict, non-vacuously.
pub fn assert_run(report: &RunReport, audit: &Verdict, log: &OperatorLog) {
    assert!(report.passed, "checkers failed:\n{}", report.text());
    assert!(
        audit.ok(),
        "audit verification failed:\n{}\n{}",
        audit.line(),
        audit.violations.join("\n")
    );
    let lost = report.verdict("lost").expect("lost verdict");
    let fencing = report.verdict("fencing").expect("fencing verdict");
    let duplicate = report.verdict("duplicate").expect("duplicate verdict");
    assert!(
        lost.count("runs_checked") > 0,
        "no acknowledged run checked"
    );
    assert!(lost.count("completed") > 0, "no run completed");
    assert!(
        fencing.count("tokens_checked") > 0,
        "no consistency token checked"
    );
    assert!(fencing.count("commands_reapplied") > 0, "no log re-applied");
    assert!(
        duplicate.count("records_compared") > 0,
        "no replica record compared"
    );
    assert!(
        audit.count("operator_acked") > 0,
        "no operator action acknowledged: {:?}",
        log.actions
    );
    assert!(
        !log.tenant_starts.contains_key("http 403"),
        "a tenant trigger token was refused with 403: {:?}",
        log.tenant_starts
    );
    assert!(
        log.tenant_starts
            .iter()
            .any(|(k, n)| k.starts_with("http 2") && *n > 0),
        "no run started with a tenant token: {:?}",
        log.tenant_starts
    );
}

/// The audit / operator lines of a run report.
pub fn audit_lines(audit: &Verdict, log: &OperatorLog) -> String {
    let mut s = format!("{}\n", audit.line());
    for x in audit.violations.iter().take(20) {
        s.push_str(&format!("  violation: {x}\n"));
    }
    for x in &audit.notes {
        s.push_str(&format!("  {x}\n"));
    }
    let acked = log.actions.iter().filter(|a| a.acked).count();
    s.push_str(&format!(
        "operator: {} actions ({} acknowledged), tenant-token starts {:?}\n",
        log.actions.len(),
        acked,
        log.tenant_starts
    ));
    s
}
