//! Phase 13 `integration_p13` scenarios (tier B, 13-18; D-P13-1g / D-P13-25): features that
//! were proven alone, run together under a continuous workload with the Phase 12 and Phase 13
//! oracles. 13-02 declared the module; 13-18 fills it.
//!
//! | scenario | what |
//! |----------|------|
//! | `drain_node_with_replicas` | 4 nodes RF 3: `drain(n)` on a node holding replicas and leading partitions → its replicas move (reassign), its leaders move (handoff); then `remove(n)` → tombstoned; zero failed runs |
//! | `remove_replica_holding_voter` | 5 nodes RF 3: `remove(n)` straight away (implies the drain) → 4 members, 3 voters + 1 learner, RF 3 restored without `n` |
//! | `ryw_through_handoffs_and_reassignment` | 4 nodes RF 3: the 13-13 session checker while graceful handoffs, explicit reassignments (incl. leader moves) and rebalance passes run → 0 violations |
//! | `start_during_gc_race` | (failpoints) a start that passed its `module_retired` check is held (`gc::start_hold`) while its module is retired and deleted by GC; released, its run fails `module_gone` (loud, never silent); the GC oracle flags nothing for runs live at retire time |
//! | `emulated_rolling_upgrade_under_load` | 3 nodes at level 1 + workload: each node in turn controlled shutdown (handoff) → restart at this build's level; finalized stays 1 until the last node, then 2; then a reassignment, a migrate (repin) and the level-2 upload gate succeed; upgrade oracles (`no_gated_before_finalize`, `no_reject_continue` over the replayed apply outcomes); every acknowledged run completes |
//! | `dead_voter_replaced_by_learner` | 3 static + 1 joined learner: a non-leader voter dies → the reconciler replaces it with the live caught-up learner (3 voters, the dead one out), membership oracles (the `member::mutant::skip_quorum_check` scenario: the mutant counts every voter live) |
//! | `explicit_joint_left_after_leader_kill` | 3 static + 1 joined learner: a joint change with an explicit transition (raft never auto-leaves it, as if the leader died before its auto-leave, 13-09's unit setup) and the meta leader killed → the new leader's driver leaves the joint configuration (the `member::mutant::no_leave_joint` scenario) |
//! | `reject_continue_never_applies_newer_commands` | the apply-outcome streams of every node's partitions (`harness::outcomes`) hold no reject-and-continue; a newer-format command (a known command carrying a field this build does not know, as a newer Ostro writes it) appended to a real log fail-stops the replay (the `upgrade::mutant::reject_and_continue` scenario) |
//!
//! Regression scenarios ([`regressions`], run by `tests/regressions_p13.rs`) pin the fixes
//! 13-18 made for the Phase 13 handover items; each names what found it.
//!
//! Tier-B limits (recorded in the 13-18 SUMMARY): `ostro_types::compat::level_max()` is
//! per process, so an in-process node cannot apply at an emulated level 1. The rolling
//! upgrade therefore emulates the level only where the server reads it per node
//! (`level_max_override`: NodeVersion, the gates, the finalizer), and the reject-and-continue
//! leg replays real WAL bytes plus one newer-format record through this build's apply. No
//! real level-2 guest exists, so the level-2 upload is the upload gate itself
//! (`control::modules::upload_gate_for`) on every node, as in 13-14's scenario.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use ostro_api::phase13::ClusterStatus;
use ostro_types::{PartitionCount, PartitionId};
use reqwest::StatusCode;
use serde_json::json;
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, append_acked, deploy, fail, meta_leader, pause, published, published_all, read_dump,
    wait_meta_leader,
};
use super::reassign::{
    Load, MOVE_T, acked_everywhere, no_ext, partition_with_outsider, pick_node, reassign_ok,
    reassign_oracles, running, wait_level2, wait_parts,
};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::outcomes::partition_outcomes;
use crate::harness::p13::P13Harness;
use crate::harness::{ClusterHarness, ClusterSpec};
use crate::oracle::disk::{LogScan, ScannedRecord, scan_partition};
use crate::oracle::p13_membership::{
    StatusSample, joint_left_within, removed_never_return, voters_odd,
};
use crate::oracle::p13_upgrade::{
    OutcomeRecord, no_gated_before_finalize_from, no_reject_continue,
};

const DEADLINE: Duration = Duration::from_secs(600);
/// Bound on one membership convergence.
const CONVERGE_T: Duration = Duration::from_secs(90);

fn spec(nodes: usize) -> ClusterSpec {
    ClusterSpec {
        nodes,
        rf: 3,
        min_insync: 2,
        seed: 1318,
        ..ClusterSpec::default()
    }
}

fn spec3() -> ClusterSpec {
    spec(3)
}

fn spec4() -> ClusterSpec {
    spec(4)
}

fn spec5() -> ClusterSpec {
    spec(5)
}

fn single() -> ClusterSpec {
    ClusterSpec {
        seed: 1318,
        ..ClusterSpec::single()
    }
}

type Run = for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>;

fn scenario(name: &'static str, spec: fn() -> ClusterSpec, run: Run) -> Scenario {
    Scenario {
        name,
        group: "integration_p13",
        tier: Tier::B,
        deadline: DEADLINE,
        spec,
        run,
    }
}

/// Every `integration_p13` scenario.
pub fn all() -> Vec<Scenario> {
    #[allow(unused_mut)]
    let mut v = vec![
        scenario("drain_node_with_replicas", spec4, drain_node_with_replicas),
        scenario(
            "remove_replica_holding_voter",
            spec5,
            remove_replica_holding_voter,
        ),
        scenario(
            "ryw_through_handoffs_and_reassignment",
            spec4,
            ryw_through_handoffs_and_reassignment,
        ),
        scenario(
            "emulated_rolling_upgrade_under_load",
            single,
            emulated_rolling_upgrade_under_load,
        ),
        scenario(
            "reject_continue_never_applies_newer_commands",
            spec3,
            reject_continue_never_applies_newer_commands,
        ),
        scenario(
            "dead_voter_replaced_by_learner",
            spec3,
            dead_voter_replaced_by_learner,
        ),
        scenario(
            "explicit_joint_left_after_leader_kill",
            spec3,
            explicit_joint_left_after_leader_kill,
        ),
    ];
    #[cfg(feature = "failpoints")]
    v.push(scenario(
        "start_during_gc_race",
        spec3,
        start_during_gc_race,
    ));
    v
}

/// The 13-18 regression scenarios (`tests/regressions_p13.rs`).
pub fn regressions() -> Vec<Scenario> {
    vec![
        scenario(
            "restart_of_retired_module_refused",
            spec3,
            restart_of_retired_module_refused,
        ),
        scenario(
            "restart_lands_on_a_led_partition",
            spec3,
            restart_lands_on_a_led_partition,
        ),
        scenario("gates_answer_409", single, gates_answer_409),
    ]
}

// ------------------------------------------------------------------------- helpers

fn sorted(v: &[u64]) -> Vec<u64> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v
}

/// A running node that is not the meta leader, holds replicas and leads at least one
/// partition (lowest id first).
fn busy_follower(c: &InProcCluster) -> Option<u64> {
    let leader = meta_leader(c);
    let parts = published_all(c);
    running(c).into_iter().find(|n| {
        Some(*n) != leader
            && parts.iter().any(|a| a.leader == *n)
            && parts
                .iter()
                .any(|a| a.replicas.contains(n) && a.leader != *n)
    })
}

/// Status samples from the meta leader (else the lowest running node not in `skip`).
struct Samples {
    t0: std::time::Instant,
    all: Vec<StatusSample>,
    skip: Vec<u64>,
}

impl Samples {
    fn new() -> Self {
        Self {
            t0: std::time::Instant::now(), // clock-exempt: scenario timing
            all: Vec::new(),
            skip: Vec::new(),
        }
    }

    async fn take(&mut self, c: &InProcCluster) -> Option<ClusterStatus> {
        let nodes: Vec<u64> = running(c)
            .into_iter()
            .filter(|n| !self.skip.contains(n))
            .collect();
        let node = nodes
            .iter()
            .copied()
            .find(|n| c.handle(*n).is_some_and(|h| h.meta().is_leader()))
            .or_else(|| nodes.first().copied())?;
        let v = c.cluster_status(node).await.ok()?;
        let s = StatusSample::from_json(self.t0.elapsed(), node, &v).ok()?;
        let st = s.status.clone();
        self.all.push(s);
        Some(st)
    }

    async fn until(
        &mut self,
        c: &InProcCluster,
        what: &str,
        f: impl Fn(&ClusterStatus) -> bool,
    ) -> Result<ClusterStatus, ScenarioFailure> {
        let end = Instant::now() + CONVERGE_T; // clock-exempt: scenario deadline
        let mut last = None;
        loop {
            if let Some(st) = self.take(c).await {
                if f(&st) {
                    return Ok(st);
                }
                last = Some(st);
            }
            if Instant::now() >= end {
                return Err(fail(
                    "membership",
                    format!(
                        "{what} not reached within {CONVERGE_T:?}; last: {:?}",
                        last.map(|s| (s.voters, s.learners, s.voters_outgoing, s.meta_leader))
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The membership oracles over these samples and every node's meta log.
    fn oracles(&self, c: &InProcCluster) -> ScenarioResult {
        let logs: BTreeMap<u64, _> = c
            .node_ids()
            .into_iter()
            .map(|n| (n, read_dump(c, n)))
            .collect();
        voters_odd(&self.all).into_result()?;
        joint_left_within(&self.all, Duration::from_secs(5)).into_result()?;
        removed_never_return(&logs, &self.all).into_result()
    }
}

/// Every verified partition log of every node.
fn scans_all(c: &InProcCluster) -> Vec<LogScan> {
    let mut scans = Vec::new();
    for node in c.node_ids() {
        let view = c.node_disk(node);
        for p in 0..c.spec().partitions {
            if let Ok(s) = scan_partition(&view, PartitionId(p)) {
                scans.push(s);
            }
        }
    }
    scans
}

/// The apply-outcome streams of `scans` (trimmed logs are skipped and counted). A real
/// log never fail-stops the replay: a halt is a failure.
fn outcome_streams(
    scans: &[LogScan],
    count: PartitionCount,
) -> Result<(Vec<OutcomeRecord>, usize), ScenarioFailure> {
    let mut all = Vec::new();
    let mut trimmed = 0;
    for s in scans {
        match partition_outcomes(s, count) {
            Ok(st) => {
                if let Some((off, why)) = st.halted {
                    return Err(fail(
                        "p13_upgrade",
                        format!(
                            "node {} partition {} offset {off}: replay fail-stopped: {why}",
                            s.node, s.partition.0
                        ),
                    ));
                }
                all.extend(st.records);
            }
            Err(_) => trimmed += 1,
        }
    }
    Ok((all, trimmed))
}

fn count_of(c: &InProcCluster) -> PartitionCount {
    PartitionCount::new(c.spec().partitions).expect("partitions > 0")
}

// ------------------------------------------------------------------------- drain + remove

fn drain_node_with_replicas(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let n = busy_follower(c).ok_or_else(|| {
            fail(
                "reassign",
                "no follower node holding and leading partitions",
            )
        })?;
        let led: Vec<u16> = published_all(c)
            .iter()
            .filter(|a| a.leader == n)
            .map(|a| a.partition)
            .collect();
        let load = Load::start(c);
        load.refresh(c, &[n]);
        load.wait_acked(3).await?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        c.drain(n)
            .await
            .map_err(|e| fail("reassign", format!("drain {n}: {e}")))?;
        wait_parts(
            c,
            "the drained node holds and leads nothing",
            MOVE_T,
            |parts| {
                parts.iter().all(|a| {
                    !a.replicas.contains(&n) && !a.isr.contains(&n) && a.leader != n && no_ext(a)
                })
            },
        )
        .await?;
        for a in published_all(c) {
            if a.replicas.len() != 3 || a.min_insync != 2 {
                return Err(fail("reassign", format!("after the drain: {a:?}")));
            }
        }
        println!(
            "drain_node_with_replicas: node {n} (led {led:?}) drained in {:?}",
            t0.elapsed()
        );
        let mut s = Samples::new();
        s.take(c).await;
        c.remove(n, false)
            .await
            .map_err(|e| fail("membership", format!("remove {n}: {e}")))?;
        s.skip.push(n);
        let st = s
            .until(c, "the drained node tombstoned", |st| {
                st.members
                    .iter()
                    .any(|m| m.node_id == n && m.state == "removed")
                    && !st.voters.contains(&n)
                    && st.voters_outgoing.is_empty()
            })
            .await?;
        println!(
            "drain_node_with_replicas: node {n} removed; voters {:?} learners {:?}",
            st.voters, st.learners
        );
        load.wait_acked(3).await?;
        let (cnt, gap) = load.finish(c).await?;
        println!("drain_node_with_replicas: {cnt} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await?;
        removed_never_return(
            &c.node_ids()
                .into_iter()
                .map(|x| (x, read_dump(c, x)))
                .collect(),
            &s.all,
        )
        .into_result()
    })
}

fn remove_replica_holding_voter(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        wait_level2(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        acked_everywhere(c, &mut acks).await?;
        let mut s = Samples::new();
        s.until(c, "5 voters", |st| {
            st.voters.len() == 5 && st.voters_outgoing.is_empty()
        })
        .await?;
        let n = busy_follower(c)
            .ok_or_else(|| fail("membership", "no follower node holding replicas"))?;
        let load = Load::start(c);
        load.refresh(c, &[n]);
        load.wait_acked(3).await?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        c.remove(n, false)
            .await
            .map_err(|e| fail("membership", format!("remove {n} (holding replicas): {e}")))?;
        s.skip.push(n);
        let st = s
            .until(c, "4 members: 3 voters + 1 learner", |st| {
                let active = st.members.iter().filter(|m| m.state == "active").count();
                st.voters.len() == 3
                    && st.learners.len() == 1
                    && st.voters_outgoing.is_empty()
                    && active == 4
                    && !st.voters.contains(&n)
                    && !st.learners.contains(&n)
            })
            .await?;
        wait_parts(
            c,
            "RF 3 restored without the removed node",
            MOVE_T,
            |parts| {
                parts.iter().all(|a| {
                    a.replicas.len() == 3
                        && a.isr.len() == 3
                        && !a.replicas.contains(&n)
                        && a.leader != n
                        && no_ext(a)
                })
            },
        )
        .await?;
        println!(
            "remove_replica_holding_voter: node {n} removed in {:?}; voters {:?} learners {:?}",
            t0.elapsed(),
            sorted(&st.voters),
            st.learners
        );
        load.wait_acked(3).await?;
        let (cnt, gap) = load.finish(c).await?;
        println!("remove_replica_holding_voter: {cnt} runs acknowledged, longest gap {gap:?}");
        acked_everywhere(c, &mut acks).await?;
        reassign_oracles(c, &acks).await?;
        s.oracles(c)
    })
}

// ------------------------------------------------------------------------- membership driver

/// 3 static voters + one joined learner, active and caught up; the samples taken.
async fn three_voters_and_a_learner(
    c: &mut InProcCluster,
    s: &mut Samples,
) -> Result<u64, ScenarioFailure> {
    use crate::harness::p13::NodeOpts;
    wait_meta_leader(c).await?;
    wait_level2(c).await?;
    s.until(c, "3 voters", |st| {
        st.voters.len() == 3 && st.voters_outgoing.is_empty()
    })
    .await?;
    let a = c
        .add_node(NodeOpts::default())
        .await
        .map_err(|e| fail("membership", format!("add_node: {e}")))?;
    s.until(c, "4 members: 3 voters + the active learner", |st| {
        st.voters.len() == 3
            && st.learners == [a]
            && st
                .members
                .iter()
                .any(|m| m.node_id == a && m.state == "active")
    })
    .await?;
    Ok(a)
}

fn dead_voter_replaced_by_learner(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let mut s = Samples::new();
        let a = three_voters_and_a_learner(c, &mut s).await?;
        let leader = wait_meta_leader(c).await?;
        let st = s
            .take(c)
            .await
            .ok_or_else(|| fail("membership", "no status"))?;
        let v = *st
            .voters
            .iter()
            .find(|v| **v != leader)
            .ok_or_else(|| fail("membership", format!("no follower voter: {:?}", st.voters)))?;
        let t0 = Instant::now(); // clock-exempt: scenario timing
        c.kill(v).await;
        s.skip.push(v);
        let st = s
            .until(c, "the dead voter replaced by the live learner", |st| {
                st.voters.len() == 3
                    && st.voters.contains(&a)
                    && !st.voters.contains(&v)
                    && st.voters_outgoing.is_empty()
            })
            .await?;
        println!(
            "dead_voter_replaced_by_learner: voter {v} killed, learner {a} promoted in {:?}; voters {:?}",
            t0.elapsed(),
            sorted(&st.voters)
        );
        s.oracles(c)
    })
}

fn explicit_joint_left_after_leader_kill(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        use ostro_cluster::membership::reconcile::Step;
        use ostro_cluster::membership::service::step_conf_change;
        let mut s = Samples::new();
        let a = three_voters_and_a_learner(c, &mut s).await?;
        let leader = wait_meta_leader(c).await?;
        let st = s
            .take(c)
            .await
            .ok_or_else(|| fail("membership", "no status"))?;
        let d = *st
            .voters
            .iter()
            .find(|v| **v != leader)
            .ok_or_else(|| fail("membership", format!("no follower voter: {:?}", st.voters)))?;
        // The joint change the reconciler would make (add the learner, demote `d`), with an
        // explicit transition: raft never auto-leaves it, as if the leader died between the
        // joint entry and its auto-leave (raft.rs: "it may never auto_leave if leader steps
        // down before enter joint is applied"). 13-09's unit test uses the same setup.
        let mut cc = step_conf_change(&Step::Joint {
            add_voters: vec![a],
            remove_voters: Vec::new(),
            demote: vec![d],
        })
        .ok_or_else(|| fail("harness", "no conf change for a joint step"))?;
        cc.transition = 2; // eraftpb ConfChangeTransition::Explicit
        let meta = c.handle(leader).expect("leader running").meta();
        meta.propose_conf_change(cc)
            .await
            .map_err(|e| fail("harness", format!("explicit joint change: {e}")))?;
        if meta.raft_detail().voters_outgoing.is_empty() {
            return Err(fail(
                "harness",
                "the explicit joint change left the joint state at once",
            ));
        }
        let t0 = Instant::now(); // clock-exempt: scenario timing
        c.kill(leader).await;
        s.skip.push(leader);
        let st = s
            .until(c, "the joint configuration left by the new leader", |st| {
                st.voters_outgoing.is_empty() && st.voters.contains(&a) && !st.voters.contains(&d)
            })
            .await?;
        println!(
            "explicit_joint_left_after_leader_kill: meta leader {leader} killed in joint; left {:?} later; voters {:?}",
            t0.elapsed(),
            sorted(&st.voters)
        );
        s.oracles(c)
    })
}

// ------------------------------------------------------------------------- RYW

fn ryw_through_handoffs_and_reassignment(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        use super::ryw::{
            Lb, clients, hand_off, load_cfg, no_unexpected, prepare, run_load, verdict,
        };
        use crate::oracle::p13_ryw::RywChecker;
        prepare(c).await?;
        wait_level2(c).await?;
        let checker = RywChecker::new();
        let lb: Lb = Arc::new(Mutex::new(c.node_ids()));
        let cfg = load_cfg(c, "hr", 1500, false);
        let load = tokio::spawn(run_load(clients(c), lb, cfg, checker.clone()));
        let (mut handoffs, mut reassigns, mut rebalances) = (0usize, 0usize, 0usize);
        let mut refused = Vec::new();
        let mut i = 0usize;
        while !load.is_finished() {
            i += 1;
            match i % 3 {
                1 => {
                    let p = u16::try_from(i % usize::from(c.spec().partitions)).unwrap_or(0);
                    let a = published(c, p);
                    if a.leader != 0 && a.isr.len() >= 2 && no_ext(&a) {
                        let to = *a.isr.iter().find(|n| **n != a.leader).expect("isr ≥ 2");
                        let caller = meta_leader(c).unwrap_or(a.leader);
                        match hand_off(c, caller, a.leader, p, to, a.epoch).await {
                            Ok(()) => handoffs += 1,
                            Err(e) => refused.push(format!("handoff p{p}: {e}")),
                        }
                    }
                }
                2 => {
                    // Move one partition onto the outsider, its leader leaving (LeaderMove).
                    if let Some((a, d)) = partition_with_outsider(c, &[])
                        && no_ext(&a)
                    {
                        let target: Vec<u64> = std::iter::once(d)
                            .chain(a.replicas.iter().copied().filter(|r| *r != a.leader))
                            .collect();
                        let via = pick_node(c, &[]).unwrap_or(1);
                        reassign_ok(c, via, a.partition, &target, false).await?;
                        let p = a.partition;
                        let tgt = target.clone();
                        wait_parts(c, "explicit move done", MOVE_T, move |parts| {
                            let x = &parts[usize::from(p)];
                            sorted(&x.replicas) == sorted(&tgt)
                                && tgt.contains(&x.leader)
                                && no_ext(x)
                        })
                        .await?;
                        reassigns += 1;
                    }
                }
                _ => {
                    let via = pick_node(c, &[]).unwrap_or(1);
                    match c
                        .client(via)
                        .post_json("/v1/cluster/rebalance", &json!({}))
                        .await
                    {
                        Ok((st, _)) if st.is_success() => rebalances += 1,
                        Ok((st, v)) => refused.push(format!("rebalance: {st} {v}")),
                        Err(e) => refused.push(format!("rebalance: {e}")),
                    }
                }
            }
            pause(Duration::from_millis(400)).await;
        }
        let mut rep = load.await.expect("harness: load task");
        println!(
            "ryw_through_handoffs_and_reassignment: {handoffs} handoffs, {reassigns} reassignments, {rebalances} rebalance passes, refused {refused:?}"
        );
        if handoffs == 0 || reassigns == 0 {
            return Err(fail(
                "handoff",
                format!("only {handoffs} handoffs and {reassigns} reassignments happened"),
            ));
        }
        no_unexpected(&rep)?;
        verdict("ryw_through_handoffs_and_reassignment", &checker, &mut rep)?;
        reassign_oracles(c, &[]).await
    })
}

// ------------------------------------------------------------------------- start vs GC

#[cfg(feature = "failpoints")]
fn start_during_gc_race(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    use std::sync::atomic::Ordering;

    use ostro_server::control::module_retire::{FP_START_HOLD, START_HOLDS};

    use super::module_gc::{
        OP_T, STEP, WF, activate, deploy_both, gc_round, get_run, hex, module_present, retire,
        send, wait_terminal,
    };
    use crate::oracle::p13_gc::{RetireEvent, RunResult, no_module_gone_for_live};
    Box::pin(async move {
        let (v1, _v2) = deploy_both(c).await;
        super::module_gc::barrier_all(c).await;
        // A start of v1 passes its retired check and is held before it submits.
        let before = START_HOLDS.load(Ordering::SeqCst);
        fail::cfg(FP_START_HOLD, "return").map_err(|e| fail("harness", e))?;
        let api = c.client(1);
        let start = tokio::spawn(async move {
            send(
                api.http
                    .post(format!("{}/v1/workflows/{WF}/runs", api.base_url))
                    .json(&json!({ "input": {}, "version": 1 })),
                "POST run",
            )
            .await
        });
        let t0 = Instant::now(); // clock-exempt: test timeout
        while START_HOLDS.load(Ordering::SeqCst) == before {
            if t0.elapsed() > OP_T || start.is_finished() {
                fail::remove(FP_START_HOLD);
                return Err(fail("harness", "the start never reached gc::start_hold"));
            }
            tokio::time::sleep(STEP).await;
        }
        // Meanwhile: v2 active, v1 retired (no v1 run is live), GC deletes v1.
        let r = async {
            let (st, v) = activate(c, 1, 2).await;
            if !st.is_success() {
                return Err(fail("p13_gc", format!("activate v2: {st} {v}")));
            }
            let (st, v) = retire(c, 1, &v1).await;
            if st != StatusCode::OK {
                return Err(fail("p13_gc", format!("retire v1: {st} {v}")));
            }
            let deadline = Instant::now() + OP_T;
            loop {
                let rep = gc_round(c, 1, true).await;
                if rep.deleted.contains(&hex(&v1)) {
                    break;
                }
                if Instant::now() >= deadline {
                    return Err(fail("p13_gc", format!("v1 never deleted: {rep:?}")));
                }
                tokio::time::sleep(STEP).await;
            }
            let present = module_present(c, &v1, 1).await;
            if !present.is_empty() {
                return Err(fail("p13_gc", format!("v1 still on nodes {present:?}")));
            }
            Ok(())
        }
        .await;
        fail::remove(FP_START_HOLD);
        r?;
        let ev = RetireEvent {
            module_sha: hex(&v1),
            non_terminal_runs: Vec::new(),
        };
        let (st, v) = start.await.map_err(|e| fail("harness", e.to_string()))?;
        if !st.is_success() {
            // A refusal is loud too, but the start had already passed its check: it must
            // submit, and the run must then fail `module_gone`.
            return Err(fail(
                "p13_gc",
                format!("the held start answered {st} {v} after v1's deletion"),
            ));
        }
        let run_id = v["run_id"].as_str().unwrap_or_default().to_owned();
        let end = wait_terminal(c, &run_id, OP_T)
            .await
            .ok_or_else(|| fail("p13_gc", format!("run {run_id} on deleted v1 never ended")))?;
        let full = get_run(c, &run_id).await;
        let failure = match &full["failure"] {
            serde_json::Value::Null => String::new(),
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        println!(
            "start_during_gc_race: run {run_id} ended {} ({failure})",
            end["status"]
        );
        if end["status"] != "failed" || !failure.contains("module_gone") {
            return Err(fail(
                "p13_gc",
                format!(
                    "a run pinned to a deleted module ended {end} without module_gone (silent)"
                ),
            ));
        }
        let mut res = RunResult::from_json(&full);
        if res.error.is_none() {
            res.error = Some(failure);
        }
        no_module_gone_for_live(&[ev], &[res]).into_result()
    })
}

// ------------------------------------------------------------------------- rolling upgrade

fn emulated_rolling_upgrade_under_load(_c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let mut c = super::version_gate::start_level1_cluster(3)
            .await
            .map_err(|e| fail("harness", format!("level-1 cluster: {e}")))?;
        let r = rolling_body(&mut c).await;
        c.stop().await;
        r
    })
}

async fn rolling_body(c: &mut InProcCluster) -> ScenarioResult {
    use super::migrate::{
        body, deploy_all, expect_ids, migrate_ok, pinned_everywhere, sleeping_runs, wait_level,
        wait_ready,
    };
    use super::version_gate::finalized;
    let (_v1, _v2, v3) = deploy_all(c).await;
    let parked = sleeping_runs(c, 0..c.spec().partitions.min(2), 600).await?;
    let load = Load::start(c);
    load.wait_acked(3).await?;
    let ids = c.node_ids();
    for (i, n) in ids.iter().enumerate() {
        load.refresh(c, &[*n]);
        let t0 = Instant::now(); // clock-exempt: scenario timing
        c.upgrade(*n, None)
            .await
            .map_err(|e| fail("harness", format!("upgrade {n}: {e}")))?;
        load.refresh(c, &[]);
        wait_ready(c).await?;
        let levels: Vec<(u64, Option<u32>)> = running(c)
            .into_iter()
            .map(|m| (m, finalized(c, m)))
            .collect();
        println!(
            "rolling upgrade: node {n} at level 2 after {:?}; finalized {levels:?}",
            t0.elapsed()
        );
        if i + 1 < ids.len() && levels.iter().any(|(_, l)| *l != Some(1)) {
            return Err(fail(
                "finalized",
                format!("finalized moved before the last node upgraded: {levels:?}"),
            ));
        }
        load.wait_acked(2).await?;
    }
    wait_level(c, 2).await?;
    // Gated features after finalization: a reassignment ...
    let a = published(c, 0);
    let to = *running(c)
        .iter()
        .find(|m| !a.replicas.contains(m))
        .ok_or_else(|| fail("reassign", format!("no node outside {:?}", a.replicas)))?;
    let mut target: Vec<u64> = a
        .replicas
        .iter()
        .copied()
        .filter(|r| *r != a.leader)
        .collect();
    target.insert(0, to);
    let via = pick_node(c, &[]).unwrap_or(1);
    reassign_ok(c, via, 0, &target, false).await?;
    let tgt = target.clone();
    wait_parts(c, "p0 moved after finalization", MOVE_T, move |parts| {
        sorted(&parts[0].replicas) == sorted(&tgt)
            && tgt.contains(&parts[0].leader)
            && no_ext(&parts[0])
    })
    .await?;
    // ... a migrate (RepinRun) ...
    let last = *ids.last().expect("nodes");
    let rep = migrate_ok(c, last, &body(1, 3, false)).await?;
    expect_ids(&rep, "repinned", &parked)?;
    for r in &parked {
        pinned_everywhere(c, r, v3).await?;
    }
    // ... and the level-2 upload gate (ext 101) on every node.
    for n in running(c) {
        let h = c.handle(n).expect("running");
        match ostro_server::control::modules::upload_gate_for(h.meta().view(), "0.1", 2) {
            Ok(ext) if !ext.is_empty() => {}
            other => {
                return Err(fail(
                    "upload_gate",
                    format!("node {n}: level-2 module after finalization: {other:?}"),
                ));
            }
        }
    }
    load.wait_acked(3).await?;
    let (cnt, gap) = load.finish(c).await?;
    println!("rolling upgrade: {cnt} runs acknowledged and completed, longest gap {gap:?}");
    // The upgrade oracles.
    let n = running(c)
        .first()
        .copied()
        .ok_or_else(|| fail("harness", "no running node"))?;
    let meta = c.meta_log(n).await;
    let scans = scans_all(c);
    let r = no_gated_before_finalize_from(1, &meta, &scans);
    if !r.is_ok() {
        return Err(fail("p13_upgrade", r.violations.join("; ")));
    }
    let (records, trimmed) = outcome_streams(&scans, count_of(c))?;
    println!(
        "rolling upgrade: {} apply outcomes replayed over {} logs ({trimmed} trimmed, skipped)",
        records.len(),
        scans.len()
    );
    if records.is_empty() {
        return Err(fail("p13_upgrade", "no apply outcome was replayed"));
    }
    no_reject_continue(&records).into_result()
}

// ------------------------------------------------------------------------- reject-and-continue

/// `cmd` re-encoded with one field this build does not know appended (field 1000, varint
/// 1): what a newer Ostro writes when it adds a field to a known command.
fn with_unknown_field(cmd: &Bytes) -> Bytes {
    let mut v = cmd.to_vec();
    v.extend_from_slice(&[0xC0, 0x3E, 0x01]);
    Bytes::from(v)
}

fn reject_continue_never_applies_newer_commands(
    c: &mut InProcCluster,
) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        deploy(c).await?;
        for p in 0..c.spec().partitions {
            append_acked(c, p, 2, ACK_T).await?;
        }
        let scans = scans_all(c);
        let (records, trimmed) = outcome_streams(&scans, count_of(c))?;
        no_reject_continue(&records).into_result()?;
        println!(
            "reject_continue: {} real apply outcomes, none reject-and-continue ({trimmed} trimmed logs)",
            records.len()
        );
        // A newer-format record appended to partition 0's log on its leader.
        let leader = published(c, 0).leader;
        let mut scan = scans
            .iter()
            .find(|s| s.node == leader && s.partition == PartitionId(0) && !s.is_trimmed())
            .cloned()
            .ok_or_else(|| fail("harness", "no untrimmed log of partition 0 on its leader"))?;
        let last = scan
            .records
            .last()
            .cloned()
            .ok_or_else(|| fail("harness", "partition 0's log is empty"))?;
        let newer = with_unknown_field(last.commands.last().expect("a command"));
        scan.records.push(ScannedRecord {
            base: last.end,
            end: last.end + 1,
            epoch: last.epoch,
            envelope: Bytes::new(),
            commands: vec![newer],
        });
        let stream = partition_outcomes(&scan, count_of(c)).map_err(|e| fail("harness", e))?;
        // The oracle first: a reject-and-continue apply skips the command and moves on.
        no_reject_continue(&stream.records).into_result()?;
        match &stream.halted {
            Some((off, why)) if *off == last.end.0 && why.contains("newer") => {
                println!("reject_continue: the newer-format command fail-stops at {off}: {why}");
                Ok(())
            }
            other => Err(fail(
                "p13_upgrade",
                format!(
                    "a newer-format command at offset {} did not fail-stop the apply: {other:?}",
                    last.end.0
                ),
            )),
        }
    })
}

// ------------------------------------------------------------------------- regressions

/// 13-18 fix (found by 13-15's `protects_retryable_terminal_runs` / the restart-path
/// finding): retry-restart and rerun of a run pinned to a retired module are refused with
/// 409 `module_retired`, as a start is.
fn restart_of_retired_module_refused(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    use super::module_gc::{
        OP_T, activate, deploy_both, reason, retire, send, start_on, wait_terminal,
    };
    Box::pin(async move {
        let (v1, _v2) = deploy_both(c).await;
        let failed = start_on(c, 1, Some(1), json!({ "fail_at": 3 })).await;
        let rec = wait_terminal(c, &failed, OP_T)
            .await
            .ok_or_else(|| fail("harness", format!("run {failed} did not end")))?;
        if rec["status"] != "failed" {
            return Err(fail("harness", format!("fail_at run ended {rec}")));
        }
        let (st, v) = activate(c, 1, 2).await;
        if !st.is_success() {
            return Err(fail("harness", format!("activate v2: {st} {v}")));
        }
        let (st, v) = retire(c, 1, &v1).await;
        if st != StatusCode::OK {
            return Err(fail("harness", format!("retire v1: {st} {v}")));
        }
        super::module_gc::barrier_all(c).await;
        let id: ostro_types::RunId = failed.parse().expect("run id");
        let leader = c.assignment(id.partition().0).await.leader;
        let cl = c.client(leader);
        let (st, v) = send(
            cl.http
                .post(format!("{}/v1/runs/{failed}/retry", cl.base_url))
                .json(&json!({})),
            "POST retry",
        )
        .await;
        if (st, reason(&v)) != (StatusCode::CONFLICT, "module_retired") {
            return Err(fail(
                "p13_gc",
                format!("retry (restart) of a run pinned to retired v1 answered {st} {v}"),
            ));
        }
        let (st, v) = send(
            cl.http
                .post(format!("{}/v1/runs/{failed}/rerun", cl.base_url))
                .json(&json!({ "run_id": failed, "from": { "seq": 1 }, "version": "source" })),
            "POST rerun",
        )
        .await;
        if (st, reason(&v)) != (StatusCode::CONFLICT, "module_retired") {
            return Err(fail(
                "p13_gc",
                format!("rerun (version source) on retired v1 answered {st} {v}"),
            ));
        }
        Ok(())
    })
}

/// 13-18 fix (found by 13-15's `protects_retryable_terminal_runs`, which needed up to 40
/// attempts; 13-12 / 13-17 handover): a restart's new run lands on a partition the serving
/// node leads, so the first attempt never answers 503 `not_leader`.
fn restart_lands_on_a_led_partition(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    use super::module_gc::{OP_T, deploy, send, start_on, wait_terminal};
    Box::pin(async move {
        deploy(c, 1, "p11-rerun").await;
        let failed = start_on(c, 1, Some(1), json!({ "fail_at": 3 })).await;
        let rec = wait_terminal(c, &failed, OP_T)
            .await
            .ok_or_else(|| fail("harness", format!("run {failed} did not end")))?;
        if rec["status"] != "failed" {
            return Err(fail("harness", format!("fail_at run ended {rec}")));
        }
        let id: ostro_types::RunId = failed.parse().expect("run id");
        let leader = c.assignment(id.partition().0).await.leader;
        let cl = c.client(leader);
        let mut placed = Vec::new();
        for i in 0..8 {
            let (path, body) = if i % 2 == 0 {
                (format!("/v1/runs/{failed}/retry"), json!({}))
            } else {
                (
                    format!("/v1/runs/{failed}/rerun"),
                    json!({ "run_id": failed, "from": { "seq": 1 }, "version": "source" }),
                )
            };
            let (st, v) = send(
                cl.http.post(format!("{}{path}", cl.base_url)).json(&body),
                "POST restart",
            )
            .await;
            if !st.is_success() {
                return Err(fail(
                    "routing",
                    format!("restart {i} ({path}) via the source's leader {leader}: {st} {v}"),
                ));
            }
            let new = v["run_id"]
                .as_str()
                .or_else(|| v["new_run_id"].as_str())
                .unwrap_or_default()
                .to_owned();
            let new_id: ostro_types::RunId = new
                .parse()
                .map_err(|e| fail("routing", format!("restart {i}: run id {new:?}: {e} ({v})")))?;
            let p = new_id.partition().0;
            let owner = published(c, p).leader;
            if owner != leader {
                return Err(fail(
                    "routing",
                    format!(
                        "restart {i}: new run {new} on partition {p}, led by {owner}, not {leader}"
                    ),
                ));
            }
            placed.push(p);
        }
        println!(
            "restart_lands_on_a_led_partition: new runs on partitions {placed:?} (all led by {leader})"
        );
        Ok(())
    })
}

/// 13-18 decision (found by 13-14 / 13-17 / 13-16: 409 vs 412): every gated refusal is 409
/// `feature_not_enabled` (retire, rebalance, reassign) on a cluster finalized at level 1.
fn gates_answer_409(_c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    use super::module_gc::{delete_module, deploy, reason};
    Box::pin(async move {
        let c = super::version_gate::start_level1_cluster(1)
            .await
            .map_err(|e| fail("harness", format!("level-1 node: {e}")))?;
        let r = async {
            let _v1 = deploy(&c, 1, "p11-rerun").await;
            let v2 = deploy(&c, 1, "p11-rerun-v2").await;
            let (st, v) = delete_module(&c, 1, &v2).await;
            if (st, reason(&v)) != (StatusCode::CONFLICT, "feature_not_enabled") {
                return Err(fail(
                    "p13_upgrade",
                    format!("retire before finalize: {st} {v}"),
                ));
            }
            for (path, body) in [
                ("/v1/cluster/rebalance", json!({})),
                (
                    "/v1/cluster/reassign",
                    json!({ "partition": 0, "replicas": [1], "cancel": false }),
                ),
            ] {
                let (st, v) = c
                    .client(1)
                    .post_json(path, &body)
                    .await
                    .map_err(|e| fail("harness", format!("{path}: {e}")))?;
                if (st, reason(&v)) != (StatusCode::CONFLICT, "feature_not_enabled") {
                    return Err(fail(
                        "p13_upgrade",
                        format!("{path} before finalize: {st} {v}"),
                    ));
                }
            }
            Ok(())
        }
        .await;
        c.stop().await;
        r
    })
}
