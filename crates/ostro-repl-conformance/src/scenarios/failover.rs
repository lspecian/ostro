//! CLUS-01 core group `failover` (12-06, B4): leadership moves only inside the published
//! ISR. A dead leader is replaced by a live ISR member even when the first live replica in
//! `replicas` order is outside the ISR (so an unclean planner's proposal is caught by the
//! election audit even though apply rejects it); with the whole ISR dead the partition
//! goes offline (`leader 0`, ISR kept) instead of electing an out-of-sync replica, and the
//! last ISR member returns cleanly; a leader that goes away is replaced and rejoins as a
//! follower without the epoch going back.

use std::time::Duration;

use futures_util::future::BoxFuture;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, SESSION, append_acked, blackhole, clear, deploy, election_audit,
    elections, fail, final_checks_with, pause, pick, published, same_set, wait_assignment,
    wait_completed, wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::InProcCluster;
use crate::harness::{ClusterHarness, ClusterSpec};

/// The `failover` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "failover",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run,
    };
    vec![
        s("failover_elects_isr_member", failover_elects_isr_member),
        s("whole_isr_dead_goes_offline", whole_isr_dead_goes_offline),
        s("resigned_leader_moves", resigned_leader_moves),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 606,
        ..ClusterSpec::default()
    }
}

/// Runs must resume on the new leader within this bound.
const RESUME_T: Duration = Duration::from_secs(10);

fn restart_or_panic(r: Result<(), crate::harness::HarnessError>, node: u64) {
    if let Err(e) = r {
        panic!("harness: restart node {node}: {e}");
    }
}

/// B4: replicas `[L, F2, F3]`; F2's Fetch is blackholed until it is shrunk out (ISR
/// `[L, F3]`) while F2 stays live; L is killed. The election must pick F3: F2, the first
/// live replica in `replicas` order, is outside the ISR, so an unclean planner proposes it
/// and the election audit flags the proposal even when apply rejects it. Runs resume and
/// complete within 10 s.
fn failover_elects_isr_member(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(
            c,
            "led by its preferred replica, not the meta leader",
            |a| {
                a.leader != 0
                    && a.leader != meta
                    && a.replicas.len() == 3
                    && a.replicas[0] == a.leader
            },
        )?;
        let (p, leader) = (a.partition, a.leader);
        let (f2, f3) = (a.replicas[1], a.replicas[2]);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;
        blackhole(c, f2, leader, RpcKind::Fetch);
        let shrunk = wait_assignment(c, p, "shrunk to [leader, F3]", LAG_MAX + CHANGE_T, |x| {
            same_set(&x.isr, &[leader, f3])
        })
        .await?;
        if c.handle(f2).is_none() {
            panic!("harness: node {f2} must stay live");
        }
        c.kill(leader).await;
        let moved = wait_assignment(c, p, "failed over", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != leader && x.epoch > shrunk.epoch
        })
        .await;
        // The audit first: an unclean proposal is the expected symptom of a wrong planner.
        election_audit(c)?;
        let now = moved?;
        if now.leader != f3 {
            return Err(fail(
                "election",
                format!(
                    "partition {p}: elected node {} (epoch {}), the only live ISR member was {f3} (ISR was {:?})",
                    now.leader, now.epoch, shrunk.isr
                ),
            ));
        }
        // clock-exempt: test bound
        let t0 = tokio::time::Instant::now();
        let more = append_acked(c, p, 3, RESUME_T).await?;
        let left = RESUME_T
            .saturating_sub(t0.elapsed())
            .max(Duration::from_millis(100));
        wait_completed(c, &more, left).await?;
        acks.extend(more);
        restart_or_panic(c.restart(leader).await, leader);
        clear(c, f2, leader);
        final_checks_with(c, false, &acks).await
    })
}

/// The ISR shrinks to the leader alone, then the leader dies: the partition goes offline
/// (`leader 0`, ISR kept as `[L]`) and no proposal elects an out-of-sync replica; the
/// restarted leader is re-elected and nothing acknowledged is lost.
fn whole_isr_dead_goes_offline(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, leader) = (a.partition, a.leader);
        let acks = append_acked(c, p, 3, ACK_T).await?;
        for f in a.replicas.iter().filter(|r| **r != leader) {
            blackhole(c, *f, leader, RpcKind::Fetch);
        }
        let shrunk = wait_assignment(
            c,
            p,
            "shrunk to the leader alone",
            LAG_MAX + CHANGE_T,
            |x| same_set(&x.isr, &[leader]),
        )
        .await?;
        let before = elections(c, p).last().map_or(0, |e| e.0);
        c.kill(leader).await;
        let offline = wait_assignment(c, p, "offline", SESSION + CHANGE_T, |x| {
            x.leader == 0 && x.epoch > shrunk.epoch
        })
        .await;
        election_audit(c)?;
        let offline = offline?;
        if !same_set(&offline.isr, &[leader]) {
            return Err(fail(
                "election",
                format!(
                    "partition {p}: offline with ISR {:?}, expected the last member [{leader}] kept",
                    offline.isr
                ),
            ));
        }
        // Stay offline: nothing elects an out-of-sync replica later either.
        pause(SESSION * 4 + Duration::from_secs(1)).await;
        let unclean: Vec<_> = elections(c, p)
            .into_iter()
            .filter(|(i, _, l)| *i > before && *l != 0 && *l != leader)
            .collect();
        if !unclean.is_empty() {
            return Err(fail(
                "election",
                format!(
                    "partition {p}: with the whole ISR [{leader}] dead, proposals elected {unclean:?} (index, epoch, leader)"
                ),
            ));
        }
        let still = published(c, p);
        if still.leader != 0 {
            return Err(fail(
                "election",
                format!(
                    "partition {p}: leader {} elected while the whole ISR [{leader}] is dead",
                    still.leader
                ),
            ));
        }
        restart_or_panic(c.restart(leader).await, leader);
        wait_assignment(c, p, "re-led by the returning ISR member", CHANGE_T, |x| {
            x.leader == leader && x.epoch > offline.epoch
        })
        .await?;
        wait_completed(c, &acks, ACK_T).await?;
        final_checks_with(c, false, &acks).await
    })
}

/// The leader goes away (graceful stop): leadership moves to a member of its ISR at a
/// higher epoch, writes resume there, and the old leader returns as an in-sync follower
/// (if it ever leads again, only at a still higher epoch).
fn resigned_leader_moves(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, leader) = (a.partition, a.leader);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;
        let isr = published(c, p).isr;
        c.kill(leader).await;
        let moved = wait_assignment(c, p, "moved", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != leader && x.epoch > a.epoch
        })
        .await;
        election_audit(c)?;
        let now = moved?;
        if !isr.contains(&now.leader) {
            return Err(fail(
                "election",
                format!(
                    "partition {p}: moved to node {} outside the ISR {isr:?}",
                    now.leader
                ),
            ));
        }
        acks.extend(append_acked(c, p, 3, ACK_T).await?);
        restart_or_panic(c.restart(leader).await, leader);
        let back = wait_assignment(
            c,
            p,
            "rejoined by the old leader",
            LAG_MAX + CHANGE_T,
            |x| x.isr.contains(&leader),
        )
        .await?;
        if back.epoch < now.epoch || (back.leader == leader && back.epoch <= now.epoch) {
            return Err(fail(
                "election",
                format!(
                    "partition {p}: epoch went from {} (leader {}) to {} (leader {})",
                    now.epoch, now.leader, back.epoch, back.leader
                ),
            ));
        }
        final_checks_with(c, false, &acks).await
    })
}
