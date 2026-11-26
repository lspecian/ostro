//! 13-19 Task 3 (CLUS-05/06, D-P13-26): the Phase 13 multi-process cluster crash campaign,
//! with membership and upgrade faults on top of the Phase 12 menu. `#[ignore]`:
//!
//! ```text
//! cargo build -p ostro --features failpoints --target-dir target/failpoints --locked
//! OSTRO_CAMPAIGN_FAULTS=500 cargo test --release -p ostro-repl-conformance --features failpoints \
//!   --locked --test cluster_campaign_p13 -- --ignored --nocapture --test-threads=1
//! ```
//!
//! Three static `ostro server` processes (RF 3, min_insync 2, every link through the chaos
//! proxy) start at `OSTRO_TEST_LEVEL_MAX=1` under the continuous mixed workload
//! (`process_p13::Load`). A seeded schedule injects `OSTRO_CAMPAIGN_FAULTS` faults (default
//! 50), one at a time, each followed by recovery to a healthy cluster (meta leader, every
//! partition led by a running node with a full ISR):
//!
//! - Phase 12: partition leader / follower / controller (meta leader) `kill -9` + restart
//!   after 0.3–1.5 s; a chaos-proxy partition between two static nodes held 0.5–2 s; a
//!   follower restarted with `wal::write=return`, then `kill -9`ed and restarted clean;
//! - Phase 13: **join** a node (`ostro cluster join`), **drain + remove** the newest joined
//!   node (it is `kill -9`ed for good once the meta leader's configuration no longer lists
//!   it, 60 s bound, else a harness failure), a **graceful handoff storm** (SIGTERM restarts of
//!   the busiest leader, twice), **meta leader transfer** (SIGTERM restart of the meta
//!   leader: its controlled shutdown hands the meta leadership off), and a **rolling
//!   restart**: every node SIGTERMed and restarted in turn, at an emulated level.
//!
//! The campaign runs in two phases. Phase A (the first fifth of the faults, at least 10)
//! keeps the cluster finalized at level 1 under an operator hold; its rolling restarts flip
//! the processes between `OSTRO_TEST_LEVEL_MAX=1` and this build's level (upgrade and
//! downgrade before finalization). At level 1 replica moves are gated (AlterReplicas), so
//! there a drain finds no replica on a joined node. Between the phases every node is
//! restarted at this build's level, the hold is released and the cluster finalizes at 2;
//! phase B runs the rest of the faults at level 2, where joins receive replicas through the
//! placement controller and drains move them away, and its rolling restarts stay at this
//! build's level. Joined nodes dial their peers directly (13-18 handover), so proxy
//! partitions use the static nodes; every other fault takes joined nodes too.
//!
//! After every batch of 10 faults: acknowledged ⇒ on disk on `min_insync` non-removed
//! nodes. At the end: every acknowledged run completes exactly once (0 lost acked runs, 0
//! duplicate journal entries, one run id per key), the Phase 12 final checks, the
//! membership oracles over the status samples taken throughout (`voters_odd`,
//! `joint_left_within(5 s)`, `removed_never_return` over every node's meta log) and the
//! upgrade oracles (`no_gated_before_finalize_from(1)`, `no_reject_continue`). Time box:
//! `OSTRO_CAMPAIGN_TIMEBOX_S` (default 4 h, the 12-17 rule): when it is reached no further
//! fault is injected, the oracles still run and the report carries a `HUMAN_NEEDED` line.
//! Report: `$OSTRO_P13_REPORT_DIR/cluster_campaign_p13/report.txt`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::time::Duration;

use ostro_repl_conformance::harness::p13::{NodeOpts, P13Harness};
use ostro_repl_conformance::harness::process::ProcCluster;
use ostro_repl_conformance::harness::{ClusterHarness, DataDirView};
use ostro_repl_conformance::oracle::disk::acked_on_disk_copies;
use ostro_repl_conformance::oracle::p13_membership::{
    StatusSample, joint_left_within, removed_never_return, voters_odd,
};
use ostro_repl_conformance::scenarios::process::tools::{self, live, meta_leader, published_all};
use ostro_repl_conformance::scenarios::process::{skip_reason, with_failpoint};
use ostro_repl_conformance::scenarios::process_p13::{
    Load, ScenarioFailure, WORKLOAD_GUESTS, busiest_leader, deploy_guests, final_checks, finalized,
    healthy, host_line, outcomes, post, start_static, status, upgrade_oracles, wait_finalized,
    write_report,
};
use ostro_types::PartitionCount;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::json;
use tokio::time::Instant;

const TEST: &str = "cluster_campaign_p13";
const BATCH: usize = 10;
const HEAL_LIMIT: Duration = Duration::from_secs(180);
const RUN_T: Duration = Duration::from_secs(120);
/// Bound on a removed node leaving the meta configuration (leader's view) before its kill.
const LEAVE_CONF_T: Duration = Duration::from_secs(60);
/// Joined nodes alive at once (a join beyond it drains + removes instead).
const MAX_JOINED: usize = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Fault {
    LeaderKill,
    FollowerKill,
    ControllerKill,
    ProxyPartition,
    WalFailKill,
    Join,
    DrainRemove,
    HandoffStorm,
    MetaTransfer,
    RollingRestart,
}

const KINDS: [Fault; 10] = [
    Fault::LeaderKill,
    Fault::FollowerKill,
    Fault::ControllerKill,
    Fault::ProxyPartition,
    Fault::WalFailKill,
    Fault::Join,
    Fault::DrainRemove,
    Fault::HandoffStorm,
    Fault::MetaTransfer,
    Fault::RollingRestart,
];

fn f(oracle: &'static str, detail: impl Into<String>) -> ScenarioFailure {
    ScenarioFailure {
        oracle,
        detail: detail.into(),
    }
}

struct Campaign {
    c: ProcCluster,
    rng: StdRng,
    t0: Instant,
    /// Nodes removed from the cluster (stopped for good).
    removed: BTreeSet<u64>,
    /// Joined nodes still members, oldest first.
    joined: Vec<u64>,
    /// Phase A: finalized 1 under a hold.
    phase_a: bool,
    samples: Vec<StatusSample>,
    load: Option<Load>,
}

impl Campaign {
    fn members(&self) -> Vec<u64> {
        self.c
            .node_ids()
            .into_iter()
            .filter(|n| !self.removed.contains(n))
            .collect()
    }

    fn removed_vec(&self) -> Vec<u64> {
        self.removed.iter().copied().collect()
    }

    fn refresh_load(&self, exclude: &[u64]) {
        if let Some(l) = &self.load {
            let mut ex = exclude.to_vec();
            ex.extend(&self.removed);
            l.refresh(&self.c, &ex);
        }
    }

    async fn sample(&mut self) {
        let Some(node) = meta_leader(&self.c)
            .await
            .or_else(|| live(&self.c).first().copied())
        else {
            return;
        };
        if let Some(v) = status(&self.c, node).await
            && let Ok(s) = StatusSample::from_json(self.t0.elapsed(), node, &v)
        {
            self.samples.push(s);
        }
    }

    /// Wait until healthy (removed nodes skipped), sampling the status meanwhile.
    async fn heal(&mut self, at: &str) -> Result<(), ScenarioFailure> {
        let t = Instant::now(); // clock-exempt: test timeout
        let skip = self.removed_vec();
        while t.elapsed() < HEAL_LIMIT {
            self.sample().await;
            if healthy(&self.c, &skip).await {
                self.refresh_load(&[]);
                return Ok(());
            }
            tools::pause(Duration::from_millis(150)).await;
        }
        let all: Vec<String> = published_all(&self.c)
            .iter()
            .map(|a| {
                format!(
                    "p{} leader {} replicas {:?} isr {:?}",
                    a.partition, a.leader, a.replicas, a.isr
                )
            })
            .collect();
        for n in self.members() {
            println!("--- node {n} log tail ---\n{}", self.c.log_tail(n, 40));
        }
        Err(f(
            "availability",
            format!(
                "cluster not healthy within {HEAL_LIMIT:?} after {at}: {all:?}; live {:?}",
                live(&self.c)
            ),
        ))
    }

    /// Wait (bounded by [`LEAVE_CONF_T`]) until the meta leader's status lists `id` in
    /// neither the voters, the learners nor the outgoing voters; sampling meanwhile.
    async fn wait_out_of_conf(&mut self, id: u64) -> Result<(), ScenarioFailure> {
        let t = Instant::now(); // clock-exempt: test timeout
        let mut last = None;
        while t.elapsed() < LEAVE_CONF_T {
            if let Some(node) = meta_leader(&self.c).await
                && node != id
                && let Some(v) = status(&self.c, node).await
                && let Ok(s) = StatusSample::from_json(self.t0.elapsed(), node, &v)
            {
                let st = &s.status;
                let out = !st.voters.contains(&id)
                    && !st.learners.contains(&id)
                    && !st.voters_outgoing.contains(&id);
                last = Some((
                    node,
                    st.voters.clone(),
                    st.learners.clone(),
                    st.voters_outgoing.clone(),
                ));
                self.samples.push(s);
                if out {
                    return Ok(());
                }
            }
            tools::pause(Duration::from_millis(50)).await;
        }
        Err(f(
            "harness",
            format!(
                "removed node {id} still in the meta configuration {LEAVE_CONF_T:?} after its tombstone (last leader status (node, voters, learners, outgoing): {last:?})"
            ),
        ))
    }

    async fn hold(&mut self, lo: u64, hi: u64) {
        tools::pause(Duration::from_millis(self.rng.random_range(lo..=hi))).await;
    }

    fn pick(&mut self, pool: &[u64]) -> u64 {
        pool[self.rng.random_range(0..pool.len())]
    }

    /// A member other than `avoid`, preferring `prefer`.
    fn other_than(&mut self, prefer: &[u64], avoid: u64) -> u64 {
        let members = self.members();
        let pool: Vec<u64> = prefer
            .iter()
            .copied()
            .filter(|n| *n != avoid && members.contains(n))
            .collect();
        if !pool.is_empty() {
            return self.pick(&pool);
        }
        let rest: Vec<u64> = members.into_iter().filter(|n| *n != avoid).collect();
        self.pick(&rest)
    }

    async fn kill_restart(&mut self, t: u64) -> Result<(), ScenarioFailure> {
        self.refresh_load(&[t]);
        self.c.kill(t).await;
        self.hold(300, 1500).await;
        self.c
            .restart(t)
            .await
            .map_err(|e| f("harness", format!("restart node {t}: {e}")))
    }

    async fn graceful_restart(
        &mut self,
        t: u64,
        level: Option<u32>,
    ) -> Result<(), ScenarioFailure> {
        self.refresh_load(&[t]);
        self.c
            .upgrade(t, level)
            .await
            .map_err(|e| f("harness", format!("SIGTERM restart of node {t}: {e}")))
    }

    async fn inject(&mut self, kind: Fault) -> Result<(Fault, String), ScenarioFailure> {
        let meta = meta_leader(&self.c).await.unwrap_or(0);
        let parts = published_all(&self.c);
        let a = parts[self.rng.random_range(0..parts.len())].clone();
        let kind = match kind {
            Fault::Join if self.joined.len() >= MAX_JOINED => Fault::DrainRemove,
            Fault::DrainRemove if self.joined.is_empty() => Fault::Join,
            k => k,
        };
        let what = match kind {
            Fault::LeaderKill => {
                let leaders: Vec<u64> = parts.iter().map(|a| a.leader).collect();
                let t = self.other_than(&leaders, meta);
                self.kill_restart(t).await?;
                format!("leader kill n{t}")
            }
            Fault::FollowerKill => {
                let followers: Vec<u64> = a
                    .replicas
                    .iter()
                    .copied()
                    .filter(|r| *r != a.leader)
                    .collect();
                let t = self.other_than(&followers, meta);
                self.kill_restart(t).await?;
                format!("follower kill n{t} (p{})", a.partition)
            }
            Fault::ControllerKill => {
                let t = if meta != 0 { meta } else { self.members()[0] };
                self.kill_restart(t).await?;
                format!("controller kill n{t}")
            }
            Fault::ProxyPartition => {
                let statics = [1u64, 2, 3];
                let x = self.pick(&statics);
                let others: Vec<u64> = statics.iter().copied().filter(|n| *n != x).collect();
                let y = self.pick(&others);
                self.c.partition(x, y);
                self.hold(500, 2000).await;
                self.c.proxy().heal_all();
                format!("proxy partition n{x}|n{y}")
            }
            Fault::WalFailKill => {
                let followers: Vec<u64> = a
                    .replicas
                    .iter()
                    .copied()
                    .filter(|r| *r != a.leader)
                    .collect();
                let t = self.other_than(&followers, meta);
                let base = self.c.failpoints(t).map(str::to_owned);
                self.c.set_failpoints(
                    t,
                    Some(&with_failpoint(base.as_deref(), "wal::write=return")),
                );
                self.refresh_load(&[t]);
                self.c
                    .restart(t)
                    .await
                    .map_err(|e| f("harness", format!("restart n{t} with a failing WAL: {e}")))?;
                self.hold(500, 1500).await;
                self.c.kill(t).await;
                self.c.set_failpoints(t, base.as_deref());
                self.c
                    .restart(t)
                    .await
                    .map_err(|e| f("harness", format!("restart n{t} clean: {e}")))?;
                format!("follower WAL failure + kill n{t} (p{})", a.partition)
            }
            Fault::Join => {
                let level = if self.phase_a && self.rng.random_bool(0.5) {
                    Some(1)
                } else {
                    None
                };
                let id = self
                    .c
                    .add_node(NodeOpts {
                        level_max: level,
                        advertise: true,
                    })
                    .await
                    .map_err(|e| f("membership", format!("join: {e}")))?;
                self.joined.push(id);
                format!("join n{id} (level {level:?})")
            }
            Fault::DrainRemove => {
                let id = self.joined.remove(0);
                self.c
                    .drain(id)
                    .await
                    .map_err(|e| f("membership", format!("drain n{id}: {e}")))?;
                self.refresh_load(&[id]);
                self.c
                    .remove(id, false)
                    .await
                    .map_err(|e| f("membership", format!("remove n{id}: {e}")))?;
                self.removed.insert(id);
                // `remove` returns once the tombstone is visible; the conf change that takes
                // the node out of the configuration follows on a later driver tick. Kill it
                // only after the leader's configuration no longer lists it, so its own
                // persisted ConfState is not frozen between the two.
                self.wait_out_of_conf(id).await?;
                let _ = self.c.kill9(id).await;
                format!("drain + remove n{id}")
            }
            Fault::HandoffStorm => {
                let mut moved = Vec::new();
                for _ in 0..2 {
                    let t = busiest_leader(&self.c).await.unwrap_or(1);
                    let level = self.c.level_max(t);
                    self.graceful_restart(t, level).await?;
                    self.heal("a handoff-storm restart").await?;
                    moved.push(t);
                }
                format!("handoff storm (SIGTERM restarts of {moved:?})")
            }
            Fault::MetaTransfer => {
                let t = if meta != 0 { meta } else { self.members()[0] };
                let level = self.c.level_max(t);
                self.graceful_restart(t, level).await?;
                format!("meta leader transfer (SIGTERM restart of n{t})")
            }
            Fault::RollingRestart => {
                let mut levels = Vec::new();
                for n in self.members() {
                    let level = if self.phase_a && self.rng.random_bool(0.5) {
                        Some(1)
                    } else {
                        None
                    };
                    self.graceful_restart(n, level).await?;
                    self.heal("a rolling-restart step").await?;
                    levels.push((n, level));
                }
                format!("rolling restart {levels:?}")
            }
        };
        Ok((kind, what))
    }

    /// Phase A → B: every member at this build's level, release, finalized 2.
    async fn finalize(&mut self, out: &mut String) -> Result<(), ScenarioFailure> {
        for n in self.members() {
            if self.c.level_max(n).is_some() {
                self.graceful_restart(n, None).await?;
                self.heal("the pre-finalize restart").await?;
            }
        }
        let via = live(&self.c)[0];
        match post(&self.c, via, "/v1/cluster/upgrade/release", &json!({})).await {
            Some((200, _)) => {}
            other => return Err(f("upgrade", format!("release: {other:?}"))),
        }
        let members = self.members();
        let d = wait_finalized(&self.c, &members, 2, Duration::from_secs(60)).await?;
        self.phase_a = false;
        let _ = writeln!(
            out,
            "phase A -> B: every member at this build's level, hold released, finalized 2 after {d:?}"
        );
        Ok(())
    }

    fn disk_check(
        &self,
        acks: &[ostro_repl_conformance::harness::inproc::Ack],
    ) -> Result<(), ScenarioFailure> {
        let views: Vec<DataDirView> = self
            .members()
            .iter()
            .map(|n| self.c.node_disk(*n))
            .collect();
        let count = PartitionCount::new(self.c.spec().partitions).expect("partitions > 0");
        acked_on_disk_copies(&views, acks, self.c.spec().min_insync, count).into_result()
    }
}

async fn campaign(
    cp: &mut Campaign,
    faults: usize,
    timebox: Duration,
    out: &mut String,
) -> Result<(), ScenarioFailure> {
    deploy_guests(&cp.c, &WORKLOAD_GUESTS).await?;
    let ids = cp.c.node_ids();
    let l = finalized(&cp.c, &ids).await;
    if l.values().any(|v| *v != 1) {
        return Err(f("finalized", format!("level-1 cluster: {l:?}")));
    }
    match post(&cp.c, 1, "/v1/cluster/upgrade/hold", &json!({})).await {
        Some((200, _)) => {}
        other => return Err(f("upgrade", format!("hold: {other:?}"))),
    }
    let load = Load::start(&cp.c, &[], false);
    load.wait_acked(10, RUN_T).await?;
    cp.load = Some(load);
    let phase_a_faults = (faults / 5).max(10).min(faults);
    let mut counts: BTreeMap<Fault, usize> = BTreeMap::new();
    let mut batch_checks = 0usize;
    let mut injected = 0usize;
    let mut human_needed = None;
    for i in 0..faults {
        if cp.t0.elapsed() > timebox {
            human_needed = Some(format!(
                "HUMAN_NEEDED: time box {timebox:?} reached after {injected} of {faults} faults"
            ));
            break;
        }
        if i == phase_a_faults && cp.phase_a {
            cp.finalize(out).await?;
        }
        let kind = KINDS[cp.rng.random_range(0..KINDS.len())];
        let (kind, what) = cp.inject(kind).await?;
        *counts.entry(kind).or_default() += 1;
        injected += 1;
        cp.heal(&what).await?;
        if (i + 1) % BATCH == 0 {
            let snapshot = cp.load.as_ref().map(Load::snapshot).unwrap_or_default();
            cp.disk_check(&snapshot).map_err(|mut e| {
                e.detail = format!("after fault {} ({what}): {}", i + 1, e.detail);
                e
            })?;
            batch_checks += 1;
            println!(
                "faults {}/{faults} ({}): last {what}; members {:?}; {:?} elapsed",
                i + 1,
                if cp.phase_a { "phase A" } else { "phase B" },
                cp.members(),
                cp.t0.elapsed()
            );
        }
    }
    if cp.phase_a {
        cp.finalize(out).await?;
    }
    let load = cp.load.take().expect("load");
    let res = load.finish(RUN_T).await?;
    cp.c.proxy().heal_all();
    cp.heal("the campaign").await?;
    let acks = res.acks();
    cp.disk_check(&acks)?;
    let _ = writeln!(
        out,
        "faults injected: {injected} of {faults} (phase A: {phase_a_faults}); by kind: {counts:?}"
    );
    let _ = writeln!(
        out,
        "batch checks: {batch_checks} (every {BATCH} faults); final disk check over {} acks on members {:?}: pass",
        acks.len(),
        cp.members()
    );
    let _ = writeln!(
        out,
        "removed nodes: {:?}; joined members at the end: {:?}",
        cp.removed, cp.joined
    );
    if let Some(h) = &human_needed {
        let _ = writeln!(out, "{h}");
    }
    let _ = writeln!(out, "{}", res.line());
    res.verdict()?;
    let oc = outcomes(&cp.c, &res.runs).await;
    let _ = writeln!(
        out,
        "lost acked runs: {}; duplicate journal entries: {}",
        oc.lost.len(),
        oc.duplicates.len()
    );
    let _ = writeln!(out, "outcomes: {}", oc.line());
    oc.verdict()?;
    let members = cp.members();
    let fc = final_checks(&cp.c, &members, &acks).await?;
    let _ = writeln!(out, "final checks: {fc}");
    let logs: BTreeMap<u64, _> =
        cp.c.node_ids()
            .into_iter()
            .filter_map(|n| tools::dump(&cp.c, n).map(|d| (n, d)))
            .collect();
    voters_odd(&cp.samples).into_result()?;
    joint_left_within(&cp.samples, Duration::from_secs(5)).into_result()?;
    removed_never_return(&logs, &cp.samples).into_result()?;
    let _ = writeln!(
        out,
        "membership oracles (voters_odd, joint_left_within 5 s, removed_never_return over {} meta logs): pass over {} status samples",
        logs.len(),
        cp.samples.len()
    );
    let uo = upgrade_oracles(&cp.c, &members).await?;
    let _ = writeln!(out, "upgrade oracles: {uo}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "tier C campaign: OSTRO_CAMPAIGN_FAULTS faults (default 50)"]
async fn cluster_campaign_with_membership_and_upgrade_faults() {
    if let Some(skip) = skip_reason() {
        println!("{skip}");
        return;
    }
    let faults: usize = std::env::var("OSTRO_CAMPAIGN_FAULTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);
    let timebox = Duration::from_secs(
        std::env::var("OSTRO_CAMPAIGN_TIMEBOX_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4 * 3600),
    );
    let seed = 1919u64;
    let t0 = Instant::now(); // clock-exempt: measurement
    let c = start_static(seed, Some(1))
        .await
        .expect("start level-1 cluster");
    let mut cp = Campaign {
        c,
        rng: StdRng::seed_from_u64(seed),
        t0,
        removed: BTreeSet::new(),
        joined: Vec::new(),
        phase_a: true,
        samples: Vec::new(),
        load: None,
    };
    let mut out = String::new();
    let _ = writeln!(
        out,
        "cluster_campaign_p13 (13-19, tier C): seed {seed}, OSTRO_CAMPAIGN_FAULTS={faults}, 3 static processes at level 1 (+ joins), RF 3, min_insync 2\n{}",
        host_line()
    );
    let r = campaign(&mut cp, faults, timebox, &mut out).await;
    let _ = writeln!(
        out,
        "result: {}; elapsed {:?}",
        match &r {
            Ok(()) => "PASS".to_owned(),
            Err(e) => format!("FAIL {e}"),
        },
        t0.elapsed()
    );
    write_report(TEST, &out);
    cp.c.stop().await;
    r.expect("cluster_campaign_p13");
}
