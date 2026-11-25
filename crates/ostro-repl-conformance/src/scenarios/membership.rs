//! Phase 13 `membership` scenarios (tier B, 13-09; CLUS-05, D-P13-8/9).
//!
//! Every scenario starts from a 3-node static cluster (RF 3), grows / shrinks the metadata
//! Raft through the real join flow (`P13Harness::add_node`) and the remove API, samples
//! `GET /v1/cluster/status` throughout, and ends with the membership oracles
//! (`removed_never_return` over every node's meta log, `joint_left_within(5 s)`,
//! `voters_odd`). The growth / shrink / id scenarios also run the Phase 12 oracles over a
//! light workload (`final_checks` before the first change; acknowledged appends after).
//!
//! | scenario | what |
//! |----------|------|
//! | `join_grows_to_five_voters` | 3 static + 2 joins → 5 voters (one joint change); a 6th join → 5 voters + 1 learner |
//! | `shrink_to_three` | 5 voters → remove the two joined nodes (no replicas) → 3 voters |
//! | `force_remove_dead_node` | a dead voter holding replicas is force-removed; a removal that would leave no live quorum is refused by the meta leader |
//! | `joiner_never_bootstraps` | a joined node cut off from every member exits after `join_timeout`, never serves, no second cluster id |
//! | `ids_never_reused` | remove node 4, join again → max + 1; 4 never returns |
//! | `meta_leader_removal_transfers_first` | removing the meta leader: leadership moves before it leaves the voters |

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use ostro_api::phase13::ClusterStatus;
use serde_json::json;

use super::placement::support::{
    append_acked, deploy, fail, final_checks_with, harness, read_dump, wait_meta_leader,
};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::{Ack, InProcCluster, p13_get, p13_post};
use crate::harness::p13::{NodeOpts, P13Harness};
use crate::harness::{ClusterHarness, ClusterSpec};
use crate::oracle::p13_membership::{
    StatusSample, joint_left_within, removed_never_return, voters_odd,
};

const DEADLINE: Duration = Duration::from_secs(420);
/// Bound on one membership convergence.
const CONVERGE_T: Duration = Duration::from_secs(90);
/// Acknowledgement bound of the light workload.
const ACK_T: Duration = Duration::from_secs(20);

fn spec3() -> ClusterSpec {
    ClusterSpec {
        seed: 1309,
        ..ClusterSpec::default()
    }
}

fn scenario(
    name: &'static str,
    run: for<'a> fn(&'a mut InProcCluster) -> BoxFuture<'a, ScenarioResult>,
) -> Scenario {
    Scenario {
        name,
        group: "membership",
        tier: Tier::B,
        deadline: DEADLINE,
        spec: spec3,
        run,
    }
}

/// Every `membership` scenario.
pub fn all() -> Vec<Scenario> {
    vec![
        scenario("join_grows_to_five_voters", join_grows_to_five_voters),
        scenario("shrink_to_three", shrink_to_three),
        scenario("force_remove_dead_node", force_remove_dead_node),
        scenario("joiner_never_bootstraps", joiner_never_bootstraps),
        scenario("ids_never_reused", ids_never_reused),
        scenario(
            "meta_leader_removal_transfers_first",
            meta_leader_removal_transfers_first,
        ),
    ]
}

/// Status samples taken during a scenario.
struct Samples {
    t0: Instant,
    all: Vec<StatusSample>,
    /// Nodes never sampled again (removed / killed).
    skip: Vec<u64>,
}

impl Samples {
    fn new() -> Self {
        Samples {
            t0: Instant::now(), // clock-exempt: scenario timing
            all: Vec::new(),
            skip: Vec::new(),
        }
    }

    /// One status sample from the meta leader (else the lowest running, non-skipped node).
    async fn take(&mut self, c: &InProcCluster) -> Option<ClusterStatus> {
        let running: Vec<u64> = c
            .node_ids()
            .into_iter()
            .filter(|n| c.handle(*n).is_some() && !self.skip.contains(n))
            .collect();
        let node = running
            .iter()
            .copied()
            .find(|n| c.handle(*n).is_some_and(|h| h.meta().is_leader()))
            .or_else(|| running.first().copied())?;
        let v = p13_get(&c.client(node), "/v1/cluster/status").await.ok()?;
        let s = StatusSample::from_json(self.t0.elapsed(), node, &v).ok()?;
        let st = s.status.clone();
        self.all.push(s);
        Some(st)
    }

    /// Sample until `f` holds (bounded by [`CONVERGE_T`]).
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
                        "{what} not reached within {CONVERGE_T:?}; last status: {:?}",
                        last.map(|s| (s.voters, s.learners, s.voters_outgoing, s.meta_leader))
                    ),
                ));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

fn state_of(st: &ClusterStatus, id: u64) -> Option<&str> {
    st.members
        .iter()
        .find(|m| m.node_id == id)
        .map(|m| m.state.as_str())
}

fn sorted(v: &[u64]) -> Vec<u64> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v
}

/// The membership oracles over the samples and every node's meta log.
fn membership_oracles(c: &InProcCluster, s: &Samples) -> ScenarioResult {
    let logs: BTreeMap<u64, _> = c
        .node_ids()
        .into_iter()
        .map(|n| (n, read_dump(c, n)))
        .collect();
    voters_odd(&s.all).into_result()?;
    joint_left_within(&s.all, Duration::from_secs(5)).into_result()?;
    removed_never_return(&logs, &s.all).into_result()
}

/// The Phase 12 oracles over the light workload, run **before** the first membership
/// change. After a learner joins, the meta leader compacts its log into the snapshot it
/// sends (13-05: a snapshot whose ConfState lacks the learner is re-taken), so the early
/// `ElectLeader` entries the LA-rule / election audits look up exist only in the snapshot
/// state; those oracles read log entries only (13-18 follow-up, reported by 13-09). After
/// the changes the scenarios keep measuring availability (acknowledged appends) and run the
/// membership oracles.
async fn phase12_checks(c: &InProcCluster, acks: &mut Vec<Ack>) -> ScenarioResult {
    workload(c, acks).await?;
    final_checks_with(c, false, acks).await
}

/// Print every node's meta snapshot index (evidence for the compaction note above).
fn report_snapshots(c: &InProcCluster) {
    let idx: Vec<(u64, u64)> = c
        .node_ids()
        .into_iter()
        .map(|n| (n, read_dump(c, n).snapshot_index))
        .collect();
    println!("meta snapshot index per node: {idx:?}");
}

/// Two acknowledged runs on every led partition.
async fn workload(c: &InProcCluster, acks: &mut Vec<Ack>) -> ScenarioResult {
    for p in 0..c.spec().partitions {
        acks.extend(append_acked(c, p, 2, ACK_T).await?);
    }
    Ok(())
}

async fn add(c: &mut InProcCluster) -> Result<u64, ScenarioFailure> {
    c.add_node(NodeOpts::default())
        .await
        .map_err(|e| fail("membership", format!("add_node: {e}")))
}

/// 3 static + 2 joins → 5 voters.
async fn grow_to_five(
    c: &mut InProcCluster,
    s: &mut Samples,
) -> Result<(u64, u64), ScenarioFailure> {
    let a = add(c).await?;
    s.until(c, "4 members: 3 voters + 1 learner", |st| {
        st.voters.len() == 3 && st.learners == [a] && state_of(st, a) == Some("active")
    })
    .await?;
    let b = add(c).await?;
    s.until(c, "5 voters", |st| {
        st.voters.len() == 5 && st.voters_outgoing.is_empty()
    })
    .await?;
    Ok((a, b))
}

fn join_grows_to_five_voters(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        phase12_checks(c, &mut acks).await?;
        let mut s = Samples::new();
        let (a, b) = grow_to_five(c, &mut s).await?;
        if a == b || a <= 3 || b <= 3 {
            return Err(fail("membership", format!("joined ids {a}, {b}")));
        }
        workload(c, &mut acks).await?;
        // A sixth member stays a learner (largest odd ≤ min(6, max_voters 5) = 5).
        let f = add(c).await?;
        let st = s
            .until(c, "5 voters + 1 learner", |st| {
                st.voters.len() == 5 && st.learners == [f] && state_of(st, f) == Some("active")
            })
            .await?;
        if st.voters.contains(&f) {
            return Err(fail(
                "membership",
                format!("node {f} became a voter: {st:?}"),
            ));
        }
        workload(c, &mut acks).await?;
        report_snapshots(c);
        membership_oracles(c, &s)
    })
}

fn shrink_to_three(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        phase12_checks(c, &mut acks).await?;
        let mut s = Samples::new();
        let (a, b) = grow_to_five(c, &mut s).await?;
        workload(c, &mut acks).await?;
        for id in [b, a] {
            c.remove(id, false)
                .await
                .map_err(|e| fail("membership", format!("remove {id}: {e}")))?;
            s.skip.push(id);
        }
        let st = s
            .until(c, "3 voters", |st| {
                st.voters.len() == 3
                    && st.voters_outgoing.is_empty()
                    && !st.learners.contains(&a)
                    && !st.learners.contains(&b)
            })
            .await?;
        if sorted(&st.voters) != [1, 2, 3] {
            return Err(fail("membership", format!("voters {:?}", st.voters)));
        }
        workload(c, &mut acks).await?;
        report_snapshots(c);
        membership_oracles(c, &s)
    })
}

fn force_remove_dead_node(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        let mut s = Samples::new();
        grow_to_five(c, &mut s).await?;
        // Kill a static voter that holds replicas (not the meta leader) and force-remove it.
        let leader = wait_meta_leader(c).await?;
        let dead = [3, 2, 1]
            .into_iter()
            .find(|n| *n != leader)
            .expect("a follower");
        c.kill(dead).await;
        s.skip.push(dead);
        c.remove(dead, true)
            .await
            .map_err(|e| fail("membership", format!("force remove {dead}: {e}")))?;
        s.until(c, "dead node out, 3 voters", |st| {
            state_of(st, dead) == Some("removed")
                && st.voters.len() == 3
                && st.voters_outgoing.is_empty()
                && !st.voters.contains(&dead)
        })
        .await?;
        // Removed while it still holds replicas (evacuation is 13-10's).
        let holds = (0..c.spec().partitions).any(|p| {
            super::placement::support::published(c, p)
                .replicas
                .contains(&dead)
        });
        if !holds {
            return Err(fail(
                "membership",
                format!("node {dead} no longer holds replicas; the scenario lost its point"),
            ));
        }
        // Kill a second voter: removing a third would leave no live quorum and is refused
        // by the meta leader (with --force too). Pick them from a settled configuration
        // (the same voters and leader in two samples a second apart, leader a voter).
        let mut prev: Option<(Vec<u64>, u64, Vec<u64>)> = None;
        let settled = loop {
            let st = s
                .until(c, "a settled 3-voter configuration", |st| {
                    st.voters.len() == 3
                        && st.voters_outgoing.is_empty()
                        && st.voters.contains(&st.meta_leader)
                })
                .await?;
            let cur = (sorted(&st.voters), st.meta_leader, sorted(&st.learners));
            if prev.as_ref() == Some(&cur) {
                break cur;
            }
            prev = Some(cur);
            tokio::time::sleep(Duration::from_secs(1)).await;
        };
        let (voters, leader, learners) = settled;
        // A live caught-up learner would replace a dead voter (13-06), so stop them first.
        for l in learners {
            c.kill(l).await;
            s.skip.push(l);
        }
        let others: Vec<u64> = voters.iter().copied().filter(|v| *v != leader).collect();
        let (second, third) = (others[0], others[1]);
        c.kill(second).await;
        s.skip.push(second);
        // Let the leader's liveness declare it dead.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let r = p13_post(
            &c.client(leader),
            &format!("/v1/cluster/members/{third}/remove"),
            &json!({ "force": true }),
        )
        .await;
        match r {
            Err(e) if crate::harness::inproc::is_http_error(&e) => {}
            other => {
                return Err(fail(
                    "membership",
                    format!("removing {third} with {second} dead was not refused: {other:?}"),
                ));
            }
        }
        membership_oracles(c, &s)
    })
}

fn joiner_never_bootstraps(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        let api = c.client(1);
        let token = harness("node token", crate::harness::inproc::node_token(&api).await);
        let data_dir = c.root().join("join-isolated");
        let prepared = harness(
            "join",
            ostro_server::p13::joiner::run_join(ostro_server::p13::JoinArgs {
                seed: api.base_url.clone(),
                token: token.token,
                ca_hash: token.ca_hash,
                data_dir: Some(data_dir.clone()),
                no_start: true,
                ..Default::default()
            })
            .await,
        );
        let id = prepared.node_id;
        // Cut the joined node off from every member, then boot it with a short join_timeout.
        c.net().faults().isolate(id);
        let mut cfg = ostro_server::config::ServerConfig::server_defaults();
        cfg.api.listen = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
        cfg.cluster.listen = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
        cfg.node.data_dir = data_dir.clone();
        cfg.node.partitions = c.spec().partitions;
        cfg.node.node_id = id;
        cfg.cluster.cluster_id = Some(prepared.cluster_id.clone());
        cfg.cluster.join_timeout = ostro_server::config::FriendlyDuration::from_secs(3);
        cfg.replication.session_timeout = ostro_server::config::FriendlyDuration::from_millis(600);
        cfg.replication.meta_election_tick = 6;
        let opts =
            ostro_server::ServerOptions::default().with_cluster_transport(c.net().transport(id));
        let started = tokio::time::timeout(
            Duration::from_secs(60),
            ostro_server::server::start_with_options(cfg, opts),
        )
        .await;
        match started {
            Ok(Err(e)) if e.to_string().contains("join_timeout") => {}
            Ok(Ok(h)) => {
                let _ = h.shutdown().await;
                return Err(fail("membership", "an isolated joiner started serving"));
            }
            Ok(Err(e)) => return Err(fail("membership", format!("unexpected start error: {e}"))),
            Err(_) => return Err(fail("membership", "the joiner did not give up within 60 s")),
        }
        c.net().faults().heal(id);
        // Never bootstrapped: no meta written, and the cluster has one cluster id.
        let joined_meta = data_dir.join("meta");
        let probe = harness(
            "probe",
            ostro_cluster::identity::probe_members(&joined_meta),
        );
        if !probe.init_clusters.is_empty() {
            return Err(fail(
                "membership",
                format!("the joiner proposed InitCluster: {:?}", probe.init_clusters),
            ));
        }
        for n in c.node_ids() {
            let cid = read_dump(c, n)
                .state
                .cluster()
                .map(|ci| ci.value.cluster_id.clone());
            if cid.as_deref() != Some(prepared.cluster_id.as_str()) {
                return Err(fail(
                    "membership",
                    format!(
                        "node {n} has cluster id {cid:?}, expected {}",
                        prepared.cluster_id
                    ),
                ));
            }
        }
        Ok(())
    })
}

fn ids_never_reused(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        deploy(c).await?;
        let mut acks = Vec::new();
        phase12_checks(c, &mut acks).await?;
        let mut s = Samples::new();
        let a = add(c).await?;
        s.until(c, "joined node active", |st| {
            state_of(st, a) == Some("active")
        })
        .await?;
        c.remove(a, false)
            .await
            .map_err(|e| fail("membership", format!("remove {a}: {e}")))?;
        s.skip.push(a);
        let b = add(c).await?;
        if b != a + 1 {
            return Err(fail(
                "membership",
                format!(
                    "after removing {a} the next join got {b}, expected {}",
                    a + 1
                ),
            ));
        }
        let st = s
            .until(c, "new node active", |st| state_of(st, b) == Some("active"))
            .await?;
        if state_of(&st, a) != Some("removed") || st.voters.contains(&a) || st.learners.contains(&a)
        {
            return Err(fail("membership", format!("node {a} came back: {st:?}")));
        }
        workload(c, &mut acks).await?;
        report_snapshots(c);
        membership_oracles(c, &s)
    })
}

fn meta_leader_removal_transfers_first(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        wait_meta_leader(c).await?;
        let mut s = Samples::new();
        let (a, _) = grow_to_five(c, &mut s).await?;
        // Make the joined node `a` (no replicas) the meta leader, then remove it.
        let leader = wait_meta_leader(c).await?;
        if leader != a {
            let h = c.handle(leader).expect("leader handle");
            if let Err(e) = h.meta().transfer_leader(a).await {
                return Err(fail("membership", format!("transfer to {a}: {e}")));
            }
        }
        s.until(c, "joined node leads meta", |st| st.meta_leader == a)
            .await?;
        c.remove(a, false)
            .await
            .map_err(|e| fail("membership", format!("remove leader {a}: {e}")))?;
        s.skip.push(a);
        s.until(c, "leader removed", |st| {
            state_of(st, a) == Some("removed")
                && !st.voters.contains(&a)
                && !st.voters_outgoing.contains(&a)
                && st.voters.len() == 3
        })
        .await?;
        // Leadership moved before the old leader left the voters: no sample shows `a`
        // outside the voters while it still leads, and every sample after its removal names
        // another leader.
        for smp in &s.all {
            let st = &smp.status;
            let voter = st.voters.contains(&a) || st.voters_outgoing.contains(&a);
            if !voter && st.meta_leader == a {
                return Err(fail(
                    "membership",
                    format!(
                        "node {a} led meta after leaving the voters (at {:?})",
                        smp.at
                    ),
                ));
            }
        }
        membership_oracles(c, &s)
    })
}
