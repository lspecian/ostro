//! 14-10 Task 2 (D-P14-19/20, QA-05): clock-skew and disk-stall faults on the real 3-node
//! tier-C cluster (`#[ignore]`, release, the failpoint binary: its nodes serve the
//! failpoints-only test endpoints):
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_JEPSEN_SECS=120 cargo test --release -p ostro-jepsen --locked --test nemesis_faults \
//!   -- --ignored --nocapture --test-threads=1
//! ```
//!
//! - (a) `clock_skew_within_bounds`: bumps (≤ ±60 s), strobes and drift (±5%) on partition
//!   leaders, the crash-saga workload, plus read-your-writes sessions measured by the Phase
//!   13 RYW checker (13-08 oracle, `p13_ryw`) through random nodes without consistency
//!   tokens: 0 RYW violations; the lost / duplicate / fencing checkers pass.
//! - (b) `disk_stalls`: `wal::fsync` / `state::commit` / `state::persist` stalls of
//!   100..=2,000 ms on partition leaders: no lost acknowledged run, every checker passes, no
//!   node crashes (the nemesis heal requires every stalled node to still run), and the write
//!   latency inside the stall windows is above the latency outside them.
//! - (c) `negative_control_drift_above_lease_margin`: the D-P14-20 negative control (30%
//!   slow monotonic drift, above `lease_margin` 0.2) cannot reach the D-P13-15 lease: 14-01
//!   recorded that the lease is measured on `Instant::now()` and the raft driver's `Instant`
//!   anchor, never on the injected clock the SkewClock wraps. The test asserts that this is
//!   still so in the source and reports the control as inapplicable (14-24 documents that
//!   lease safety under real clock drift is not exercised by the harness, and that the skew
//!   moves the node's injected clock only: OS timers such as tokio sleeps and TCP keepalive
//!   are not skewed). If the lease ever reads an injectable clock, this test fails and the
//!   control must be run for real (`ClockFault::NegativeControl`, expected ≥ 1 RYW
//!   violation, labelled "assumption violated (expected)").
//!
//! Seeds: `OSTRO_JEPSEN_SEED` overrides the fixed defaults; `OSTRO_JEPSEN_SECS` the fault
//! phase (default 120 s). Reports in `target/jepsen/<seed>/`.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use ostro_jepsen::history::{ClientOp, NemesisEvent, OpKind, Outcome, read_jsonl};
use ostro_jepsen::nemesis::clock::{ClockFault, ClockNemesis, NEGATIVE_CONTROL_RATE};
use ostro_jepsen::nemesis::disk::DiskNemesis;
use ostro_jepsen::report::RunReport;
use ostro_jepsen::run::{LocalBoxFuture, Nemesis, NemesisKind, RunSpec, run};
use ostro_jepsen::workload::{WORKFLOW, plan_input};
use ostro_repl_conformance::harness::process::{ProcCluster, SKIP_NO_BIN, failpoint_bin};
use ostro_repl_conformance::harness::{ApiClient, ClusterHarness};
use ostro_repl_conformance::oracle::p13_ryw::{Effect, Observation, RywChecker, Session};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

/// Default seed of (a).
const SEED_CLOCK: u64 = 1410;
/// Default seed of (b).
const SEED_DISK: u64 = 1411;
/// RYW client sessions of (a).
const SESSIONS: u64 = 4;
/// Bound on one probe request.
const REQ_T: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------------
// Read-your-writes sessions (a)
// ---------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct ProbeStats {
    writes_acked: AtomicU64,
    reads_recorded: AtomicU64,
    unavailable: AtomicU64,
}

/// RYW sessions started at the first fault and stopped after the last heal (or when the
/// fault phase ends): each session starts a crash-saga run (own key prefix) through a random
/// node, reads it back through random nodes, signals it with a request id and re-sends the
/// signal through another node (the answer's `duplicate` shows whether the signal is
/// visible). Only 2xx / 404 answers are observations; anything else is availability.
struct RywProbe {
    seed: u64,
    checker: RywChecker,
    stats: Arc<ProbeStats>,
    stop: Arc<AtomicBool>,
    started: Mutex<Option<Vec<tokio::task::JoinHandle<()>>>>,
    /// The fault phase left when the first fault is injected.
    span: Duration,
}

impl RywProbe {
    fn new(seed: u64, span: Duration) -> Self {
        Self {
            seed,
            checker: RywChecker::new(),
            stats: Arc::new(ProbeStats::default()),
            stop: Arc::new(AtomicBool::new(false)),
            started: Mutex::new(None),
            span,
        }
    }

    fn ensure_started(&self, c: &ProcCluster) {
        let mut g = self.started.lock().unwrap_or_else(PoisonError::into_inner);
        if g.is_some() {
            return;
        }
        let apis: Vec<ApiClient> = c.node_ids().into_iter().map(|n| c.client(n)).collect();
        // clock-exempt: harness timing
        let deadline = Instant::now() + self.span;
        let tasks = (0..SESSIONS)
            .map(|s| {
                let sess = self.checker.session(format!("s{s}"));
                tokio::spawn(session(
                    self.seed,
                    s,
                    apis.clone(),
                    sess,
                    Arc::clone(&self.stats),
                    Arc::clone(&self.stop),
                    deadline,
                ))
            })
            .collect();
        *g = Some(tasks);
    }

    async fn finish(&self) {
        self.stop.store(true, Ordering::SeqCst);
        let tasks = self
            .started
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .unwrap_or_default();
        for t in tasks {
            let _ = t.await;
        }
    }
}

struct Resp {
    st: u16,
    body: Value,
}

async fn call(api: &ApiClient, method: &str, path: &str, body: Option<Value>) -> Option<Resp> {
    let url = format!("{}{path}", api.base_url);
    let mut rb = match method {
        "GET" => api.http.get(url),
        _ => api.http.post(url),
    }
    .timeout(REQ_T);
    if let Some(b) = body {
        rb = rb.json(&b);
    }
    let r = rb.send().await.ok()?;
    let st = r.status().as_u16();
    let b = r.bytes().await.ok()?;
    Some(Resp {
        st,
        body: serde_json::from_slice(&b).unwrap_or(Value::Null),
    })
}

async fn session(
    seed: u64,
    s: u64,
    apis: Vec<ApiClient>,
    sess: Session,
    stats: Arc<ProbeStats>,
    stop: Arc<AtomicBool>,
    deadline: Instant,
) {
    let mut rng = StdRng::seed_from_u64(seed ^ 0x7279_7721 ^ s.rotate_left(17));
    let pick = |rng: &mut StdRng| apis[rng.random_range(0..apis.len())].clone();
    let mut n = 0u64;
    // clock-exempt: harness timing
    while !stop.load(Ordering::SeqCst) && Instant::now() < deadline {
        n += 1;
        let key = format!("ryw-{seed}-{s}-{n}");
        let body = json!({ "input": plan_input(seed, &key), "idempotency_key": key });
        let path = format!("/v1/workflows/{WORKFLOW}/runs");
        let run_id = match call(&pick(&mut rng), "POST", &path, Some(body)).await {
            Some(r) if (200..300).contains(&r.st) => r.body["run_id"].as_str().map(str::to_owned),
            _ => None,
        };
        let Some(run_id) = run_id else {
            stats.unavailable.fetch_add(1, Ordering::Relaxed);
            ostro_jepsen::workload::pause(Duration::from_millis(100)).await;
            continue;
        };
        sess.ack(Effect::RunExists {
            run_id: run_id.clone(),
        });
        stats.writes_acked.fetch_add(1, Ordering::Relaxed);
        for _ in 0..2 {
            let ticket = sess.begin_read();
            match call(&pick(&mut rng), "GET", &format!("/v1/runs/{run_id}"), None).await {
                Some(r) if r.st == 200 || r.st == 404 => {
                    sess.observe_issued(
                        ticket,
                        Observation::run_from_json(&run_id, r.st == 200, &r.body, Vec::new()),
                    );
                    stats.reads_recorded.fetch_add(1, Ordering::Relaxed);
                }
                _ => {
                    stats.unavailable.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        let rid = format!("{key}-sig");
        let sig_path = format!("/v1/runs/{run_id}/signals/ryw");
        let sig_body = json!({ "payload": { "rid": rid }, "request_id": rid });
        match call(&pick(&mut rng), "POST", &sig_path, Some(sig_body.clone())).await {
            Some(r) if (200..300).contains(&r.st) => {
                sess.ack(Effect::SignalVisible {
                    run_id: run_id.clone(),
                    request_id: rid.clone(),
                });
                stats.writes_acked.fetch_add(1, Ordering::Relaxed);
                let ticket = sess.begin_read();
                match call(&pick(&mut rng), "POST", &sig_path, Some(sig_body)).await {
                    Some(r) if (200..300).contains(&r.st) => {
                        let seen = if r.body["duplicate"] == true {
                            vec![rid.clone()]
                        } else {
                            Vec::new()
                        };
                        sess.observe_issued(
                            ticket,
                            Observation::Run {
                                run_id: run_id.clone(),
                                found: true,
                                status: None,
                                signals: seen,
                            },
                        );
                        stats.reads_recorded.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        stats.unavailable.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            _ => {
                stats.unavailable.fetch_add(1, Ordering::Relaxed);
            }
        }
        ostro_jepsen::workload::pause(Duration::from_millis(50)).await;
    }
}

/// A nemesis that starts the RYW sessions at its first injection and stops them after the
/// schedule's last heal.
struct Armed {
    inner: Arc<dyn Nemesis>,
    probe: Arc<RywProbe>,
    heals_left: Arc<AtomicUsize>,
}

impl Nemesis for Armed {
    fn name(&self) -> String {
        self.inner.name()
    }

    fn inject<'a>(
        &'a self,
        c: &'a mut ProcCluster,
        pick: u64,
    ) -> LocalBoxFuture<'a, Result<String, String>> {
        self.probe.ensure_started(c);
        self.inner.inject(c, pick)
    }

    fn heal<'a>(&'a self, c: &'a mut ProcCluster) -> LocalBoxFuture<'a, Result<String, String>> {
        Box::pin(async move {
            let r = self.inner.heal(c).await;
            if self.heals_left.fetch_sub(1, Ordering::SeqCst) <= 1 {
                self.probe.stop.store(true, Ordering::SeqCst);
            }
            r
        })
    }
}

// ---------------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------------

fn assert_non_vacuous(report: &RunReport) {
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
        report.faults.values().sum::<u64>() > 0,
        "no fault executed: {:?}",
        report.faults
    );
}

fn mean_ms(v: &[u64]) -> f64 {
    if v.is_empty() {
        return 0.0;
    }
    v.iter().sum::<u64>() as f64 / v.len() as f64
}

// ---------------------------------------------------------------------------------
// (a) clock skew within bounds
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier-C Jepsen run (needs the failpoint ostro binary); run explicitly"]
async fn clock_skew_within_bounds() {
    assert!(failpoint_bin().is_some(), "{SKIP_NO_BIN}");
    let token = std::env::var("OSTRO_JEPSEN_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    // The schedule only depends on the menu's names: build it once to size the probe.
    let plain = RunSpec::from_env(Some(SEED_CLOCK), 120, ClockNemesis::menu(token.clone()));
    let schedule = plain.schedule();
    let first_at = schedule.slots.first().map_or(0, |s| s.at_ms);
    let probe = Arc::new(RywProbe::new(
        plain.seed,
        plain
            .duration
            .saturating_sub(Duration::from_millis(first_at)),
    ));
    let heals_left = Arc::new(AtomicUsize::new(schedule.slots.len()));
    let menu: Vec<NemesisKind> = ClockFault::IN_BOUNDS
        .into_iter()
        .map(|f| {
            NemesisKind::Custom(Arc::new(Armed {
                inner: Arc::new(ClockNemesis::new(f, token.clone())),
                probe: Arc::clone(&probe),
                heals_left: Arc::clone(&heals_left),
            }))
        })
        .collect();
    let mut spec = plain.clone();
    spec.nemeses = menu;
    assert_eq!(
        spec.schedule(),
        schedule,
        "the armed menu keeps the schedule"
    );
    let report = run(spec).await.expect("jepsen run");
    probe.finish().await;
    println!("{}", report.text());

    let ryw = probe.checker.check();
    let (w, r, u) = (
        probe.stats.writes_acked.load(Ordering::Relaxed),
        probe.stats.reads_recorded.load(Ordering::Relaxed),
        probe.stats.unavailable.load(Ordering::Relaxed),
    );
    println!(
        "ryw (p13_ryw, {SESSIONS} sessions, no tokens): {} violations; writes acked {w}, reads recorded {r}, unavailable {u}",
        ryw.violations.len()
    );
    for v in ryw.violations.iter().take(20) {
        println!("  ryw violation: {v}");
    }
    assert!(report.passed, "checkers failed:\n{}", report.text());
    assert_non_vacuous(&report);
    assert!(
        w > 0 && r > 0,
        "the RYW sessions recorded nothing (writes {w}, reads {r})"
    );
    assert!(
        ryw.violations.is_empty(),
        "read-your-writes violated under in-bounds clock skew: {:?}",
        ryw.violations
    );
    for f in ClockFault::IN_BOUNDS {
        println!(
            "faults {}: {}",
            f.name(),
            report.faults.get(f.name()).copied().unwrap_or(0)
        );
    }
}

// ---------------------------------------------------------------------------------
// (b) disk stalls
// ---------------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "tier-C Jepsen run (needs the failpoint ostro binary); run explicitly"]
async fn disk_stalls() {
    assert!(failpoint_bin().is_some(), "{SKIP_NO_BIN}");
    let token = std::env::var("OSTRO_JEPSEN_TOKEN")
        .ok()
        .filter(|t| !t.is_empty());
    let spec = RunSpec::from_env(
        Some(SEED_DISK),
        120,
        vec![NemesisKind::Custom(Arc::new(DiskNemesis::new(token)))],
    );
    let dir = spec.out_dir.clone();
    let report = run(spec).await.expect("jepsen run");
    println!("{}", report.text());
    assert!(report.passed, "checkers failed:\n{}", report.text());
    assert_non_vacuous(&report);

    let ops: Vec<ClientOp> = read_jsonl(&dir.join("history.jsonl")).expect("history.jsonl");
    let events: Vec<NemesisEvent> = read_jsonl(&dir.join("nemesis.jsonl")).expect("nemesis.jsonl");
    let windows: Vec<(u64, u64)> = events
        .iter()
        .filter(|e| e.kind == "disk:stall")
        .filter(|e| {
            e.detail
                .as_deref()
                .is_none_or(|d| !d.starts_with("skipped"))
        })
        .map(|e| (e.start_ms, e.end_ms))
        .collect();
    assert!(!windows.is_empty(), "no stall injected");
    let heal = events
        .iter()
        .find(|e| e.kind == "heal")
        .and_then(|e| e.detail.clone())
        .unwrap_or_default();
    assert!(
        heal.starts_with("restarted []"),
        "a node was down at the final heal (crash under a disk stall?): {heal}"
    );
    let (mut inside, mut outside) = (Vec::new(), Vec::new());
    for o in ops
        .iter()
        .filter(|o| o.outcome == Outcome::Ok && matches!(o.kind, OpKind::Start | OpKind::Signal))
    {
        let lat = o.complete_ms.saturating_sub(o.invoke_ms);
        if windows
            .iter()
            .any(|(s, e)| o.invoke_ms < *e && o.complete_ms > *s)
        {
            inside.push(lat);
        } else {
            outside.push(lat);
        }
    }
    let mut by_site: BTreeMap<String, u64> = BTreeMap::new();
    for e in events.iter().filter(|e| e.kind == "disk:stall") {
        let site = e
            .target
            .split(": ")
            .nth(1)
            .and_then(|t| t.split('=').next())
            .unwrap_or("?")
            .to_owned();
        *by_site.entry(site).or_default() += 1;
    }
    let (mi, mo) = (mean_ms(&inside), mean_ms(&outside));
    println!(
        "write latency: inside {} stall windows mean {mi:.1} ms over {} ops, outside mean {mo:.1} ms over {} ops; stalls by site {by_site:?}",
        windows.len(),
        inside.len(),
        outside.len()
    );
    assert!(
        !inside.is_empty() && !outside.is_empty(),
        "latency sample empty"
    );
    assert!(
        mi > mo,
        "the stalls did not raise the write latency ({mi:.1} vs {mo:.1} ms)"
    );
}

// ---------------------------------------------------------------------------------
// (c) negative control
// ---------------------------------------------------------------------------------

/// The lease clock source, read from the server source (14-01 check 6).
fn lease_reads_instant_now() -> Result<(), String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../ostro-server/src/route/consistency.rs");
    let src = std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let need = [
        "pub fn lease_holds(anchor: Option<Instant>, now: Instant, limit: Duration) -> bool",
        "fn meta_lease_anchor(&self) -> Option<Instant>;",
        "fn now(&self) -> Instant;",
    ];
    for n in need {
        if !src.contains(n) {
            return Err(format!("{} no longer contains `{n}`", path.display()));
        }
    }
    let imp = src
        .split("impl ReadEnv for NodeReadEnv")
        .nth(1)
        .ok_or("NodeReadEnv's ReadEnv impl not found")?;
    let now_fn = imp
        .split("fn now(&self) -> Instant {")
        .nth(1)
        .and_then(|r| r.split("\n    }").next())
        .ok_or("NodeReadEnv::now not found")?;
    if !now_fn.contains("Instant::now()") || now_fn.contains("clock") && now_fn.contains(".now_ms")
    {
        return Err(format!(
            "NodeReadEnv::now no longer returns Instant::now(): {now_fn}"
        ));
    }
    Ok(())
}

#[test]
#[ignore = "part of the 14-10 nemesis run (reports the negative control); run explicitly"]
fn negative_control_drift_above_lease_margin() {
    match lease_reads_instant_now() {
        Ok(()) => println!(
            "negative control ({:+.0}% monotonic drift > lease_margin 0.2): INAPPLICABLE - the \
             D-P13-15 lease compares std::time::Instant values (NodeReadEnv::now = \
             Instant::now(), the raft driver's lease anchor), never the injected Clock that \
             SkewClock wraps (14-01-SUMMARY, lease clock source); lease safety under real \
             clock drift is not exercised by the harness (14-24 threat model). SkewClock \
             moves the node's injected clock only; OS timers (tokio sleeps, TCP keepalive) \
             are not skewed.",
            NEGATIVE_CONTROL_RATE * 100.0
        ),
        Err(e) => panic!(
            "the lease clock source changed ({e}): if the D-P13-15 lease now reads an \
             injectable clock, run the negative control for real (ClockFault::NegativeControl \
             on an isolated leader; it must report >= 1 RYW violation, labelled 'assumption \
             violated (expected)')"
        ),
    }
}
