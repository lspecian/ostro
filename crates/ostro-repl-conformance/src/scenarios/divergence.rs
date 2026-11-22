//! CLUS-01 core group `divergence` (12-06): a leader's unreplicated tail never becomes
//! committed. After a failover the old leader (and, with two quick leader changes, an
//! intermediate leader too, KIP-279) truncates its divergent tail when it rejoins: replicas
//! are byte-identical below the HW, none of the unacknowledged divergent commands is below
//! the HW on any node, and every acknowledged run is on disk.
//!
//! The divergent appends are kept from committing by blackholing the followers' Fetch to
//! the leader, and kept from shrinking the ISR (which would make the failover an offline
//! partition) by blackholing the leader's `Propose` to the meta leader, so the leader is
//! always picked among partitions the meta leader does not lead.

use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_meta::PartitionAssignment;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, SESSION, Start, append_acked, blackhole, deploy, fail,
    final_checks_with, join_starts, none_committed, pick, settle, spawn_starts, wait_assignment,
    wait_keys_on_disk, wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster};
use crate::harness::{ClusterHarness, ClusterSpec};

/// The `divergence` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, spec, run: Run| Scenario {
        name,
        group: "divergence",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec,
        run,
    };
    vec![
        s(
            "leader_dies_with_unreplicated_writes",
            rf3,
            leader_dies_with_unreplicated_writes,
        ),
        s(
            "isolated_leader_rejoin_truncates",
            rf3,
            isolated_leader_rejoin_truncates,
        ),
        s(
            "two_quick_leader_changes_truncate",
            five_nodes,
            two_quick_leader_changes_truncate,
        ),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 605,
        ..ClusterSpec::default()
    }
}

/// Two leaders of one partition die in a row while the metadata group keeps a quorum: five
/// voters, RF 3.
fn five_nodes() -> ClusterSpec {
    ClusterSpec {
        nodes: 5,
        seed: 615,
        ..ClusterSpec::default()
    }
}

/// Unacknowledged appends per divergent leader.
const K: usize = 3;
/// Bound on divergent appends reaching the leader's own disk.
const LAND_T: Duration = Duration::from_secs(5);
/// Bound on each divergent request (it must not be acknowledged).
const DIV_T: Duration = Duration::from_secs(10);

fn restart_or_panic(r: Result<(), crate::harness::HarnessError>, node: u64) {
    if let Err(e) = r {
        panic!("harness: restart node {node}: {e}");
    }
}

/// Keep the divergent tail of `leader` uncommitted: followers cannot fetch from it, and it
/// cannot get an ISR shrink committed.
fn hold_tail(c: &InProcCluster, a: &PartitionAssignment, meta: u64) {
    for f in a.replicas.iter().filter(|r| **r != a.leader) {
        blackhole(c, *f, a.leader, RpcKind::Fetch);
    }
    if a.leader != meta {
        blackhole(c, a.leader, meta, RpcKind::Propose);
    }
}

/// Split joined divergent requests: acknowledged ones join `acks` (they must then survive,
/// checked by the disk oracle); the keys of the others are returned (they must never be
/// committed).
fn split(keys: Vec<String>, results: Vec<Start>, acks: &mut Vec<Ack>) -> Vec<String> {
    let mut unacked = Vec::new();
    for (k, r) in keys.into_iter().zip(results) {
        match r {
            Start::Acked(a) => acks.push(a),
            _ => unacked.push(k),
        }
    }
    unacked
}

/// Wait for `p` to settle with every node of `members` in its ISR, then check no divergent
/// key is below the HW anywhere.
async fn truncated(
    c: &InProcCluster,
    p: u16,
    members: &[u64],
    divergent: &[String],
) -> Result<(), ScenarioFailure> {
    wait_assignment(
        c,
        p,
        "rejoined by the old leader(s)",
        LAG_MAX + CHANGE_T,
        |x| members.iter().all(|m| x.isr.contains(m)),
    )
    .await?;
    match settle(c, p).await? {
        Some((_, hw, _)) => none_committed(c, p, hw, divergent),
        None => Err(fail("availability", format!("partition {p} is offline"))),
    }
}

/// The leader appends K commands nobody can fetch, then dies; after the failover and its
/// restart the divergent tail is truncated.
fn leader_dies_with_unreplicated_writes(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, leader) = (a.partition, a.leader);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;
        hold_tail(c, &a, meta);
        let (keys, tasks) = spawn_starts(c, leader, p, K, "div", DIV_T);
        wait_keys_on_disk(c, leader, p, &keys, LAND_T).await?;
        c.kill(leader).await;
        wait_assignment(c, p, "failed over", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != leader && x.epoch > a.epoch
        })
        .await?;
        let divergent = split(keys, join_starts(tasks).await, &mut acks);
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        restart_or_panic(c.restart(leader).await, leader);
        truncated(c, p, &[leader], &divergent).await?;
        final_checks_with(c, false, &acks).await
    })
}

/// The leader is cut off from every node (its API still answers), appends whatever it
/// accepts before stepping down, is replaced, and truncates its tail when the links heal.
fn isolated_leader_rejoin_truncates(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, leader) = (a.partition, a.leader);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;
        c.isolate(leader);
        let (keys, tasks) = spawn_starts(c, leader, p, K, "iso", Duration::from_secs(5));
        wait_assignment(c, p, "failed over", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != leader && x.epoch > a.epoch
        })
        .await?;
        let divergent = split(keys, join_starts(tasks).await, &mut acks);
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        c.heal(leader);
        truncated(c, p, &[leader], &divergent).await?;
        final_checks_with(c, false, &acks).await
    })
}

/// KIP-279: leader A (epoch e1) writes an unreplicated tail and dies; the next leader X
/// (e2) writes its own unreplicated tail and dies; Y (e3) leads. Restarted, A must truncate
/// a tail from two epochs back and X one from the previous epoch.
fn two_quick_leader_changes_truncate(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, first) = (a.partition, a.leader);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;

        hold_tail(c, &a, meta);
        let (k1, t1) = spawn_starts(c, first, p, K, "kip1", DIV_T);
        wait_keys_on_disk(c, first, p, &k1, LAND_T).await?;
        c.kill(first).await;
        let x = wait_assignment(c, p, "failed over once", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != first && x.epoch > a.epoch
        })
        .await?;
        let second = x.leader;

        let meta = wait_meta_leader(c).await?;
        hold_tail(c, &x, meta);
        let (k2, t2) = spawn_starts(c, second, p, K, "kip2", DIV_T);
        wait_keys_on_disk(c, second, p, &k2, LAND_T).await?;
        c.kill(second).await;
        let y = wait_assignment(c, p, "failed over twice", SESSION + CHANGE_T, |y| {
            y.leader != 0 && y.leader != first && y.leader != second && y.epoch > x.epoch
        })
        .await?;

        restart_or_panic(c.restart(first).await, first);
        restart_or_panic(c.restart(second).await, second);
        let mut divergent = split(k1, join_starts(t1).await, &mut acks);
        divergent.extend(split(k2, join_starts(t2).await, &mut acks));
        if !a.replicas.contains(&y.leader) {
            return Err(fail(
                "placement",
                format!(
                    "partition {p}: leader {} is not a replica {:?}",
                    y.leader, a.replicas
                ),
            ));
        }
        acks.extend(append_acked(c, p, 3, CHANGE_T + ACK_T).await?);
        truncated(c, p, &[first, second], &divergent).await?;
        final_checks_with(c, false, &acks).await
    })
}
