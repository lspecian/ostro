//! 12-17 (CLUS-04, SC4, D-P12-10/24): leader failover p99 on real processes, plus the
//! metadata election tick tuning sweep. `#[ignore]`; run with
//!
//! ```text
//! OSTRO_FAILOVER_ITERS=200 OSTRO_FAILOVER_ASSERT=1 cargo test --release -p ostro-repl-conformance \
//!   --test failover_p99 -- --ignored --nocapture --test-threads=1
//! OSTRO_FAILOVER_SWEEP=1 cargo test --release -p ostro-repl-conformance --test failover_p99 \
//!   -- --ignored --nocapture --test-threads=1 sweep
//! ```
//!
//! Three `ostro server` processes (failpoint build), RF 3, min_insync 2, the shipped
//! `[replication]` defaults (`ReplicationSection::default()`, both profiles), a steady workload
//! of short runs over every partition. Each iteration picks a target: in every third
//! iteration the metadata leader (the controller), otherwise the leader of a seeded-random
//! partition; and a fault kind: in a third of the iterations a chaos-proxy blackhole of every
//! link to and from the target (no FIN, no RST: only `session_timeout` detects it), otherwise
//! `kill -9` (the proxy passes the RST through, W2). Just before the fault a durable-sleep run
//! (`timeouts` `sleep_ms` 300) is acknowledged on every partition the target leads.
//!
//! Per partition the target led, from the fault: (a) the new leader's `ElectLeader`
//! (proposal time `ElectionInfo.elected_at_ms` from the committed meta log), (b) its
//! `LeaderAnnounced` committed (the new leader's reported HW passes the LA offset measured in
//! its WAL), (c) the run started before the fault completes, (d) a new start on that
//! partition is acknowledged. A client polls every 50 ms and follows the 503 leader hints.
//! The iteration's failover time is the worst partition's `max(c, d)`. Per-step breakdown:
//! detection (fault → `ElectLeader` proposed, minus the metadata re-election for controller
//! faults), metadata re-election (a survivor reports a new meta leader), `ElectLeader` apply
//! (proposed → the new leader's log leads), LA commit, apply catch-up + dispatcher re-grant
//! (LA committed → c/d). The detection path comes from the controller's log: a suspect report
//! confirmed (`liveness: suspect confirmed dead`), new-controller seeding (`controller active`
//! after a controller fault) or the session-timeout sample. Then the target is healed or
//! restarted and every ISR is full again before the next iteration.
//!
//! Writes `target/failover-p99/report.txt`. `OSTRO_FAILOVER_ITERS` (default 200),
//! `OSTRO_FAILOVER_ASSERT=1` asserts p99 < 10 s. Every iteration's pre-fault runs must
//! complete (runs resume automatically, W2), asserted always.
//!
//! The sweep (`OSTRO_FAILOVER_SWEEP=1`): `meta_election_tick` ∈ {10, 6, 4} × `session_timeout`
//! ∈ {3 s, 2 s}, 60 iterations each (or `OSTRO_FAILOVER_ITERS` when smaller) plus a 10-minute
//! loaded steady state counting metadata term changes (a value > 1 of `OSTRO_FAILOVER_SWEEP`
//! is the steady-state length in seconds, for smoke runs). Writes
//! `target/failover-p99/sweep.txt` with the pick: the most conservative setting whose p99 is
//! under 8 s with zero spurious term changes.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ostro_meta::MetaCommand;
use ostro_repl_conformance::harness::ClusterHarness;
use ostro_repl_conformance::harness::process::{ProcCluster, ProcOptions};
use ostro_repl_conformance::harness::{ClusterSpec, HarnessError};
use ostro_repl_conformance::scenarios::process::skip_reason;
use ostro_repl_conformance::scenarios::process::tools::{
    HintClient, Start, best_dump, deploy, key_for, live, meta_leader, meta_view, published_all,
    scan,
};
use ostro_server::config::{FriendlyDuration, ReplicationSection};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

/// The `timeouts` guest's workflow (durable sleep / busy steps).
const WF: &str = "timeouts";
/// Client poll period.
const POLL: Duration = Duration::from_millis(50);
/// Bound on one measured partition reaching (c) and (d).
const FAILOVER_LIMIT: Duration = Duration::from_secs(60);
/// Bound on the cluster getting healthy again between iterations.
const HEAL_LIMIT: Duration = Duration::from_secs(90);
/// Durable sleep of the pre-fault run.
const INFLIGHT_SLEEP_MS: u64 = 300;

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

fn now_unix_ms() -> u64 {
    // clock-exempt: measurement timestamps (compared with the nodes' own wall clocks)
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default()
}

fn report_dir() -> PathBuf {
    let d = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/failover-p99");
    std::fs::create_dir_all(&d).expect("report dir");
    d
}

/// The timings a run uses: the shipped defaults, optionally with a swept tick / session.
fn timings(tick: Option<u32>, session_ms: Option<u64>) -> ReplicationSection {
    let mut r = ReplicationSection::default();
    if let Some(t) = tick {
        r.meta_election_tick = t;
    }
    if let Some(s) = session_ms {
        r.session_timeout = FriendlyDuration::from_millis(s);
    }
    r
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Kill,
    Blackhole,
}

#[derive(Clone, Debug, Default)]
struct PartSample {
    partition: u16,
    old_epoch: u32,
    new_leader: u64,
    new_epoch: u32,
    /// All in ms from the fault.
    elect_proposed: Option<u64>,
    leads: Option<u64>,
    la_committed: Option<u64>,
    inflight_done: Option<u64>,
    new_ack: Option<u64>,
}

impl PartSample {
    fn failover(&self) -> Option<u64> {
        Some(self.inflight_done?.max(self.new_ack?))
    }
}

#[derive(Clone, Debug)]
struct Iteration {
    index: usize,
    target: u64,
    controller: bool,
    kind: Kind,
    meta_reelected: Option<u64>,
    parts: Vec<PartSample>,
    detection_path: &'static str,
    /// Worst partition's failover (ms); for a target that led nothing, the metadata
    /// re-election (controller) or 0.
    failover_ms: Option<u64>,
    resumed: bool,
}

/// One monitor sample of a survivor.
#[derive(Clone, Debug)]
struct Sample {
    t: u64,
    node: u64,
    meta: Option<(u64, u64)>,
    rows: Vec<Value>,
}

async fn healthy(c: &ProcCluster) -> bool {
    let all = published_all(c);
    if all
        .iter()
        .any(|a| a.leader == 0 || a.isr.len() != a.replicas.len())
    {
        return false;
    }
    meta_leader(c).await.is_some()
}

async fn wait_healthy(c: &ProcCluster) -> Result<(), String> {
    let t0 = Instant::now();
    while t0.elapsed() < HEAL_LIMIT {
        if healthy(c).await {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let all: Vec<String> = published_all(c)
        .iter()
        .map(|a| format!("p{} leader {} isr {:?}", a.partition, a.leader, a.isr))
        .collect();
    Err(format!(
        "cluster not healthy within {HEAL_LIMIT:?}: {all:?}"
    ))
}

/// A key routing [`WF`] to partition `p` (keyed routing), unique per `(salt, i)`.
fn routed_key(salt: &str, p: u16, partitions: u16, i: &mut u64) -> String {
    let count = ostro_types::PartitionCount::new(partitions).expect("partitions > 0");
    loop {
        *i += 1;
        let key = format!("wl-{salt}-{i}");
        if ostro_types::routing::partition_for_key(WF, key.as_bytes(), count).0 == p {
            return key;
        }
    }
}

/// Steady workload: a short `busy_steps` run per partition every 200 ms through clients
/// that follow leader hints (errors ignored; counts kept).
fn spawn_workload(c: &ProcCluster, stop: Arc<AtomicBool>) -> tokio::task::JoinHandle<(u64, u64)> {
    let partitions = c.spec().partitions;
    let clients: Vec<HintClient> = (0..partitions).map(|_| HintClient::new(c, 1)).collect();
    let salt = format!("{}-{}-{}", c.cluster_id(), c.r(), now_unix_ms());
    tokio::spawn(async move {
        let (mut ok, mut err) = (0u64, 0u64);
        let mut i = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let mut set = tokio::task::JoinSet::new();
            for (p, cl) in clients.iter().enumerate() {
                let key = routed_key(&salt, p as u16, partitions, &mut i);
                let cl = cl.clone();
                set.spawn(async move {
                    cl.start(
                        WF,
                        json!({"mode": "busy_steps", "n": 2}),
                        &key,
                        Duration::from_secs(2),
                    )
                    .await
                });
            }
            while let Some(r) = set.join_next().await {
                match r {
                    Ok(Start::Acked(_)) => ok += 1,
                    Ok(other) => {
                        err += 1;
                        if err <= 5 || err.is_multiple_of(500) {
                            println!(
                                "workload start not acknowledged ({err}): {}",
                                other.describe()
                            );
                        }
                    }
                    Err(_) => err += 1,
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        (ok, err)
    })
}

/// Poll (every [`POLL`], concurrent attempts, same idempotency key) until a start is acked.
async fn poll_ack(cl: HintClient, key: String, t0: Instant) -> Option<u64> {
    let mut set = tokio::task::JoinSet::new();
    let input = json!({"mode": "busy_steps", "n": 1});
    while t0.elapsed() < FAILOVER_LIMIT {
        if set.len() < 40 {
            let (cl, key, input) = (cl.clone(), key.clone(), input.clone());
            set.spawn(async move {
                matches!(
                    cl.start(WF, input, &key, Duration::from_secs(2)).await,
                    Start::Acked(_)
                )
            });
        }
        let tick = tokio::time::sleep(POLL);
        tokio::pin!(tick);
        loop {
            tokio::select! {
                () = &mut tick => break,
                Some(r) = set.join_next() => {
                    if matches!(r, Ok(true)) {
                        return Some(t0.elapsed().as_millis() as u64);
                    }
                }
            }
        }
    }
    None
}

/// Poll `GET /v1/runs/{id}` until `completed`.
async fn poll_completed(cl: HintClient, run_id: String, t0: Instant) -> Option<u64> {
    let mut set = tokio::task::JoinSet::new();
    let path = format!("/v1/runs/{run_id}");
    while t0.elapsed() < FAILOVER_LIMIT {
        if set.len() < 40 {
            let (cl, path) = (cl.clone(), path.clone());
            set.spawn(async move {
                matches!(cl.get(&path, Duration::from_secs(2)).await,
                    Some((200, v)) if v["status"] == "completed")
            });
        }
        let tick = tokio::time::sleep(POLL);
        tokio::pin!(tick);
        loop {
            tokio::select! {
                () = &mut tick => break,
                Some(r) = set.join_next() => {
                    if matches!(r, Ok(true)) {
                        return Some(t0.elapsed().as_millis() as u64);
                    }
                }
            }
        }
    }
    None
}

/// Monitor the survivors' `GET /v1/admin/replication` every [`POLL`] until `stop`.
fn spawn_monitor(
    c: &ProcCluster,
    survivors: Vec<u64>,
    t0: Instant,
    stop: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<Vec<Sample>> {
    let apis: Vec<(u64, ostro_repl_conformance::harness::ApiClient)> =
        survivors.iter().map(|n| (*n, c.client(*n))).collect();
    tokio::spawn(async move {
        let mut out = Vec::new();
        while !stop.load(Ordering::Relaxed) {
            for (n, api) in &apis {
                let Ok((_, v)) = api.get_json("/v1/admin/replication").await else {
                    continue;
                };
                let meta = v["meta"]["leader"].as_u64().zip(v["meta"]["term"].as_u64());
                let rows = v["partitions"].as_array().cloned().unwrap_or_default();
                out.push(Sample {
                    t: t0.elapsed().as_millis() as u64,
                    node: *n,
                    meta,
                    rows,
                });
            }
            tokio::time::sleep(POLL).await;
        }
        out
    })
}

fn log_len(c: &ProcCluster, n: u64) -> u64 {
    std::fs::metadata(c.log_path(n))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn log_since(c: &ProcCluster, n: u64, from: u64) -> String {
    let text = std::fs::read(c.log_path(n)).unwrap_or_default();
    let from = usize::try_from(from).unwrap_or(0).min(text.len());
    String::from_utf8_lossy(&text[from..]).into_owned()
}

/// One failover iteration.
async fn iteration(
    c: &mut ProcCluster,
    index: usize,
    rng: &mut StdRng,
) -> Result<Iteration, HarnessError> {
    wait_healthy(c).await.map_err(HarnessError::Other)?;
    let meta = meta_leader(c)
        .await
        .ok_or_else(|| HarnessError::Other("no meta leader".into()))?;
    let controller = index.is_multiple_of(3);
    let kind = if (index / 3) % 3 == 1 {
        Kind::Blackhole
    } else {
        Kind::Kill
    };
    let assignments = published_all(c);
    let target = if controller {
        meta
    } else {
        let led: Vec<u64> = assignments
            .iter()
            .map(|a| a.leader)
            .filter(|l| *l != meta)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        if led.is_empty() {
            // Every partition is led by the controller: take a random partition's leader.
            assignments[rng.random_range(0..assignments.len())].leader
        } else {
            let p = rng.random_range(0..assignments.len());
            let l = assignments[p].leader;
            if l != meta {
                l
            } else {
                led[rng.random_range(0..led.len())]
            }
        }
    };
    let led: Vec<(u16, u32)> = assignments
        .iter()
        .filter(|a| a.leader == target)
        .map(|a| (a.partition, a.epoch))
        .collect();
    let survivors: Vec<u64> = c.node_ids().into_iter().filter(|n| *n != target).collect();
    let old_term = meta_view(c, survivors[0]).await.map_or(0, |(_, t)| t);

    // Pre-fault in-flight runs on every partition the target leads (acknowledged).
    let mut inflight = BTreeMap::new();
    for (p, _) in &led {
        let cl = HintClient::new(c, target);
        let key = key_for(c, WF, *p, "inflight");
        let t = Instant::now();
        loop {
            match cl
                .start(
                    WF,
                    json!({"mode": "sleep_ms", "n": INFLIGHT_SLEEP_MS}),
                    &key,
                    Duration::from_secs(5),
                )
                .await
            {
                Start::Acked(a) => {
                    inflight.insert(*p, a.run_id);
                    break;
                }
                other if t.elapsed() > Duration::from_secs(30) => {
                    return Err(HarnessError::Other(format!(
                        "pre-fault run on partition {p} not acknowledged: {}",
                        other.describe()
                    )));
                }
                _ => tokio::time::sleep(POLL).await,
            }
        }
    }

    let log_marks: BTreeMap<u64, u64> = c.node_ids().iter().map(|n| (*n, log_len(c, *n))).collect();
    // The fault.
    let t0 = Instant::now();
    let t0_unix = now_unix_ms();
    match kind {
        Kind::Kill => c.kill(target).await,
        Kind::Blackhole => {
            for n in &survivors {
                c.proxy().blackhole(target, *n);
                c.proxy().blackhole(*n, target);
            }
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let monitor = spawn_monitor(c, survivors.clone(), t0, Arc::clone(&stop));
    let mut tasks = Vec::new();
    for (p, _) in &led {
        let first = survivors[0];
        let ack = tokio::spawn(poll_ack(
            HintClient::new(c, first),
            key_for(c, WF, *p, "after"),
            t0,
        ));
        let done = tokio::spawn(poll_completed(
            HintClient::new(c, first),
            inflight[p].clone(),
            t0,
        ));
        tasks.push((*p, ack, done));
    }
    let mut parts = Vec::new();
    for (p, ack, done) in tasks {
        let new_ack = ack.await.ok().flatten();
        let inflight_done = done.await.ok().flatten();
        let old_epoch = led.iter().find(|(q, _)| *q == p).map_or(0, |(_, e)| *e);
        parts.push(PartSample {
            partition: p,
            old_epoch,
            new_ack,
            inflight_done,
            ..PartSample::default()
        });
    }
    // Meta re-election: keep monitoring briefly when the target led nothing.
    if led.is_empty() {
        let t = Instant::now();
        while t.elapsed() < FAILOVER_LIMIT {
            if let Some(m) = meta_leader(c).await
                && m != target
            {
                break;
            }
            tokio::time::sleep(POLL).await;
        }
    }
    stop.store(true, Ordering::Relaxed);
    let samples = monitor.await.unwrap_or_default();

    // Meta re-election: first survivor sample with a new leader at a higher term.
    let meta_reelected = if controller {
        samples
            .iter()
            .find(|s| {
                s.meta
                    .is_some_and(|(l, t)| l != 0 && l != target && t > old_term)
            })
            .map(|s| s.t)
    } else {
        None
    };
    // Per-partition steps from the committed meta log and the monitor samples.
    let dump = best_dump(c);
    for ps in &mut parts {
        if let Some((epoch, leader, at)) = dump.entries.iter().find_map(|e| match &e.command {
            MetaCommand::ElectLeader {
                partition,
                epoch,
                leader,
                info,
                ..
            } if *partition == ps.partition && *epoch > ps.old_epoch => {
                Some((*epoch, *leader, info.elected_at_ms))
            }
            _ => None,
        }) {
            ps.new_epoch = epoch;
            ps.new_leader = leader;
            ps.elect_proposed = Some(at.saturating_sub(t0_unix));
        }
        let rows = |s: &Sample| {
            s.rows
                .iter()
                .find(|r| r["partition"].as_u64() == Some(u64::from(ps.partition)))
                .cloned()
        };
        ps.leads = samples
            .iter()
            .filter(|s| s.node == ps.new_leader)
            .find(|s| {
                rows(s).is_some_and(|r| {
                    r["role"]
                        .as_str()
                        .is_some_and(|x| x.to_ascii_lowercase().contains("leader"))
                        && r["epoch"].as_u64() == Some(u64::from(ps.new_epoch))
                })
            })
            .map(|s| s.t);
        // LA offset: the first record of the new epoch in the new leader's WAL.
        if ps.new_leader != 0
            && let Ok(sc) = scan(c, ps.new_leader, ps.partition)
            && let Some(la) = sc.records.iter().find(|r| r.epoch.0 == ps.new_epoch)
        {
            let la_base = la.base.0;
            ps.la_committed = samples
                .iter()
                .filter(|s| s.node == ps.new_leader)
                .find(|s| {
                    rows(s).is_some_and(|r| {
                        r["epoch"].as_u64() == Some(u64::from(ps.new_epoch))
                            && r["hw"].as_u64().is_some_and(|hw| hw > la_base)
                    })
                })
                .map(|s| s.t);
        }
    }
    // Detection path from the controller's log (the meta leader after the fault).
    let new_meta = meta_leader(c).await.unwrap_or(0);
    let ctl_log = log_marks
        .get(&new_meta)
        .map(|m| log_since(c, new_meta, *m))
        .unwrap_or_default();
    let detection_path = if controller && ctl_log.contains("controller active (meta leader)") {
        "new-controller seeding"
    } else if ctl_log.contains(&format!("liveness: suspect confirmed dead node={target}")) {
        "suspect report confirmed"
    } else {
        "session-timeout sample"
    };
    let resumed = parts.iter().all(|p| p.inflight_done.is_some());
    let failover_ms = if parts.is_empty() {
        Some(meta_reelected.unwrap_or(0))
    } else {
        parts
            .iter()
            .map(PartSample::failover)
            .collect::<Option<Vec<u64>>>()
            .and_then(|v| v.into_iter().max())
    };

    // Recover: heal or restart, then full ISRs again (checked at the next iteration start).
    match kind {
        Kind::Kill => c.restart(target).await?,
        Kind::Blackhole => c.proxy().heal_node(target),
    }
    Ok(Iteration {
        index,
        target,
        controller,
        kind,
        meta_reelected,
        parts,
        detection_path,
        failover_ms,
        resumed,
    })
}

fn pct(v: &[u64], q: f64) -> u64 {
    if v.is_empty() {
        return 0;
    }
    let mut s = v.to_vec();
    s.sort_unstable();
    let idx = ((q * s.len() as f64).ceil() as usize).clamp(1, s.len()) - 1;
    s[idx]
}

fn stats_line(name: &str, v: &[u64]) -> String {
    format!(
        "{name:<34} n={:<4} p50={:>6} p99={:>6} max={:>6} ms",
        v.len(),
        pct(v, 0.50),
        pct(v, 0.99),
        v.iter().max().copied().unwrap_or(0)
    )
}

/// The worst partition's per-step durations of an iteration.
fn steps(it: &Iteration) -> Option<[u64; 5]> {
    let worst = it
        .parts
        .iter()
        .max_by_key(|p| p.failover().unwrap_or(u64::MAX))?;
    let meta = it.meta_reelected.unwrap_or(0);
    let proposed = worst.elect_proposed?;
    let leads = worst.leads.unwrap_or(proposed).max(proposed);
    let la = worst.la_committed.unwrap_or(leads).max(leads);
    let end = worst.failover()?;
    Some([
        proposed.saturating_sub(meta),
        meta,
        leads - proposed,
        la - leads,
        end.saturating_sub(la),
    ])
}

fn report(its: &[Iteration], cfg: &ReplicationSection, extra: &str) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "failover p99 (12-17): {} iterations, 3 processes, RF 3, min_insync 2",
        its.len()
    );
    let _ = writeln!(
        out,
        "config: session_timeout {:?}, meta_tick_interval {:?}, meta_election_tick {}, meta_heartbeat_tick {}, replica_lag_max {:?}, fetch_max_wait {:?}",
        cfg.session_timeout.get(),
        cfg.meta_tick_interval.get(),
        cfg.meta_election_tick,
        cfg.meta_heartbeat_tick,
        cfg.replica_lag_max.get(),
        cfg.fetch_max_wait.get()
    );
    let _ = writeln!(out, "{extra}");
    let all: Vec<u64> = its.iter().filter_map(|i| i.failover_ms).collect();
    let _ = writeln!(out, "{}", stats_line("overall", &all));
    let sel = |f: &dyn Fn(&Iteration) -> bool| -> Vec<u64> {
        its.iter()
            .filter(|i| f(i))
            .filter_map(|i| i.failover_ms)
            .collect()
    };
    let _ = writeln!(
        out,
        "{}",
        stats_line("kill -9", &sel(&|i| i.kind == Kind::Kill))
    );
    let _ = writeln!(
        out,
        "{}",
        stats_line("blackhole (no RST)", &sel(&|i| i.kind == Kind::Blackhole))
    );
    let _ = writeln!(
        out,
        "{}",
        stats_line("controller target", &sel(&|i| i.controller))
    );
    let _ = writeln!(
        out,
        "{}",
        stats_line("non-controller target", &sel(&|i| !i.controller))
    );
    for (kind, ctl) in [
        (Kind::Kill, false),
        (Kind::Kill, true),
        (Kind::Blackhole, false),
        (Kind::Blackhole, true),
    ] {
        let name = format!(
            "{} / {}",
            if kind == Kind::Kill {
                "kill"
            } else {
                "blackhole"
            },
            if ctl { "controller" } else { "non-controller" }
        );
        let _ = writeln!(
            out,
            "{}",
            stats_line(&name, &sel(&|i| i.kind == kind && i.controller == ctl))
        );
    }
    let names = [
        "step: detection",
        "step: meta re-election (ctl)",
        "step: ElectLeader apply",
        "step: LA commit",
        "step: catch-up + re-grant",
    ];
    let st: Vec<[u64; 5]> = its.iter().filter_map(steps).collect();
    for (k, name) in names.iter().enumerate() {
        let v: Vec<u64> = if k == 1 {
            its.iter()
                .filter(|i| i.controller)
                .filter_map(steps)
                .map(|s| s[k])
                .collect()
        } else {
            st.iter().map(|s| s[k]).collect()
        };
        let _ = writeln!(out, "{}", stats_line(name, &v));
    }
    let mut paths: BTreeMap<&str, usize> = BTreeMap::new();
    for i in its {
        *paths.entry(i.detection_path).or_default() += 1;
    }
    let _ = writeln!(out, "detection paths: {paths:?}");
    let led0 = its.iter().filter(|i| i.parts.is_empty()).count();
    let _ = writeln!(
        out,
        "controller iterations: {}, blackhole iterations: {}, targets that led no partition: {led0}, not resumed: {}",
        its.iter().filter(|i| i.controller).count(),
        its.iter().filter(|i| i.kind == Kind::Blackhole).count(),
        its.iter().filter(|i| !i.resumed).count()
    );
    let _ = writeln!(
        out,
        "\nper iteration: idx kind target ctl path failover_ms [p: proposed/leads/la/inflight/ack ms]"
    );
    for i in its {
        let parts: Vec<String> = i
            .parts
            .iter()
            .map(|p| {
                format!(
                    "p{}:{:?}/{:?}/{:?}/{:?}/{:?}",
                    p.partition,
                    p.elect_proposed,
                    p.leads,
                    p.la_committed,
                    p.inflight_done,
                    p.new_ack
                )
            })
            .collect();
        let _ = writeln!(
            out,
            "{:>3} {:<9} n{} {} {:<24} {:>6?} meta {:?} {}",
            i.index,
            format!("{:?}", i.kind),
            i.target,
            if i.controller { "ctl" } else { "   " },
            i.detection_path,
            i.failover_ms,
            i.meta_reelected,
            parts.join(" ")
        );
    }
    out
}

/// Run `iters` iterations on a fresh cluster with `cfg`; returns the iterations.
async fn measure(cfg: &ReplicationSection, iters: usize, seed: u64) -> (Vec<Iteration>, String) {
    let spec = ClusterSpec {
        seed,
        ..ClusterSpec::default()
    };
    let opts = ProcOptions {
        replication: Some(cfg.clone()),
        ..ProcOptions::default()
    };
    let mut c = ProcCluster::start_with(spec, opts)
        .await
        .expect("start cluster");
    wait_healthy(&c).await.expect("formed");
    deploy(&c, WF).await.expect("deploy timeouts guest");
    let stop = Arc::new(AtomicBool::new(false));
    let wl = spawn_workload(&c, Arc::clone(&stop));
    let mut rng = StdRng::seed_from_u64(seed);
    let mut its = Vec::new();
    for i in 0..iters {
        match iteration(&mut c, i, &mut rng).await {
            Ok(it) => {
                println!(
                    "iteration {i}: {:?} n{} ctl={} {:?} failover {:?} ms ({})",
                    it.kind,
                    it.target,
                    it.controller,
                    it.parts.iter().map(|p| p.partition).collect::<Vec<_>>(),
                    it.failover_ms,
                    it.detection_path
                );
                its.push(it);
            }
            Err(e) => {
                for n in c.node_ids() {
                    println!("--- node {n} log tail ---\n{}", c.log_tail(n, 30));
                }
                panic!("iteration {i}: harness: {e}");
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    let (ok, err) = wl.await.unwrap_or_default();
    c.stop().await;
    (
        its,
        format!("steady workload: {ok} starts acknowledged, {err} not (during faults)"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C measurement: OSTRO_FAILOVER_ITERS iterations (default 200, ~40 min)"]
async fn failover_p99() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let iters = env_u64("OSTRO_FAILOVER_ITERS").unwrap_or(200) as usize;
    let cfg = timings(None, None);
    let (its, extra) = measure(&cfg, iters, 1717).await;
    let text = report(&its, &cfg, &extra);
    let path = report_dir().join("report.txt");
    std::fs::write(&path, &text).expect("write report");
    println!("{text}\nreport: {}", path.display());
    let not_resumed: Vec<usize> = its.iter().filter(|i| !i.resumed).map(|i| i.index).collect();
    assert!(
        not_resumed.is_empty(),
        "pre-fault runs did not complete (runs must resume automatically) in iterations {not_resumed:?}"
    );
    let all: Vec<u64> = its.iter().filter_map(|i| i.failover_ms).collect();
    assert_eq!(all.len(), its.len(), "every iteration measured a failover");
    if std::env::var("OSTRO_FAILOVER_ASSERT").is_ok_and(|v| v == "1") {
        let p99 = pct(&all, 0.99);
        assert!(p99 < 10_000, "failover p99 {p99} ms >= 10 s");
    }
}

/// Count metadata term changes on a loaded, fault-free cluster for `secs`.
async fn steady_state(cfg: &ReplicationSection, secs: u64, seed: u64) -> (u64, u64) {
    let spec = ClusterSpec {
        seed,
        ..ClusterSpec::default()
    };
    let opts = ProcOptions {
        replication: Some(cfg.clone()),
        ..ProcOptions::default()
    };
    let c = ProcCluster::start_with(spec, opts)
        .await
        .expect("start cluster");
    wait_healthy(&c).await.expect("formed");
    deploy(&c, WF).await.expect("deploy");
    let stop = Arc::new(AtomicBool::new(false));
    let wl = spawn_workload(&c, Arc::clone(&stop));
    let t0 = Instant::now();
    let mut first: Option<u64> = None;
    let mut max_term = 0u64;
    while t0.elapsed() < Duration::from_secs(secs) {
        for n in live(&c) {
            if let Some((_, t)) = meta_view(&c, n).await {
                max_term = max_term.max(t);
                first.get_or_insert(t);
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    stop.store(true, Ordering::Relaxed);
    let (ok, _) = wl.await.unwrap_or_default();
    c.stop().await;
    (max_term.saturating_sub(first.unwrap_or(max_term)), ok)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C tuning sweep (OSTRO_FAILOVER_SWEEP=1): 6 settings x (60 iterations + 10 min steady state)"]
async fn sweep() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let Some(sweep) = env_u64("OSTRO_FAILOVER_SWEEP").filter(|v| *v > 0) else {
        println!("SKIP: set OSTRO_FAILOVER_SWEEP=1 to run the tuning sweep");
        return;
    };
    let steady_secs = if sweep == 1 { 600 } else { sweep };
    let iters = env_u64("OSTRO_FAILOVER_ITERS").map_or(60, |v| v.min(60)) as usize;
    let mut table = String::new();
    let _ = writeln!(
        table,
        "meta election tick tuning sweep (12-17): {iters} failover iterations + {steady_secs} s loaded steady state per setting"
    );
    let _ = writeln!(
        table,
        "{:<6} {:<8} {:>7} {:>7} {:>7} {:>13} {:>12}",
        "tick", "session", "p50_ms", "p99_ms", "max_ms", "term_changes", "steady_acks"
    );
    let mut rows = Vec::new();
    for (i, (tick, session_ms)) in [
        (10u32, 3000u64),
        (10, 2000),
        (6, 3000),
        (6, 2000),
        (4, 3000),
        (4, 2000),
    ]
    .into_iter()
    .enumerate()
    {
        let cfg = timings(Some(tick), Some(session_ms));
        let (its, _) = measure(&cfg, iters, 1800 + i as u64).await;
        let v: Vec<u64> = its.iter().filter_map(|i| i.failover_ms).collect();
        let (changes, acks) = steady_state(&cfg, steady_secs, 1900 + i as u64).await;
        let (p50, p99, max) = (
            pct(&v, 0.5),
            pct(&v, 0.99),
            v.iter().max().copied().unwrap_or(0),
        );
        let _ = writeln!(
            table,
            "{tick:<6} {:<8} {p50:>7} {p99:>7} {max:>7} {changes:>13} {acks:>12}",
            format!("{}s", session_ms / 1000)
        );
        println!(
            "sweep tick {tick} session {session_ms} ms: p50 {p50} p99 {p99} max {max}, term changes {changes}"
        );
        rows.push((tick, session_ms, p99, changes));
        let partial = report_dir().join("sweep.txt");
        std::fs::write(&partial, &table).expect("write sweep");
    }
    // Most conservative = largest timeouts first (the order above).
    let pick = rows.iter().find(|(_, _, p99, ch)| *p99 < 8_000 && *ch == 0);
    let _ = writeln!(
        table,
        "\npick (most conservative with p99 < 8 s and zero spurious term changes): {}",
        match pick {
            Some((t, s, p99, _)) => format!(
                "meta_election_tick {t}, session_timeout {}s (p99 {p99} ms)",
                s / 1000
            ),
            None => "none qualifies".into(),
        }
    );
    let path = report_dir().join("sweep.txt");
    std::fs::write(&path, &table).expect("write sweep");
    println!("{table}\nsweep: {}", path.display());
}
