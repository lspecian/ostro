//! CLUS-01 core group `min_insync` (12-06): with fewer than `min_insync` replicas in the
//! ISR the partition stops acknowledging. New appends are rejected `503 Unavailable`,
//! nothing appended before the shrink is ever acknowledged, and the HW stays frozen at a
//! value held on at least `min_insync` disks. Writes resume once a follower rejoins.

use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_types::Offset;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, Start, append_acked, blackhole, clear, deploy, durable_end, fail,
    final_checks_with, join_starts, key_for, leader_hw, pause, pick, poll_async, same_set,
    spawn_starts, start_keyed, wait_assignment, wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::ClusterHarness;
use crate::harness::ClusterSpec;
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster};

/// The `min_insync` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "min_insync",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run,
    };
    vec![
        s("writes_stop_below_min_insync", writes_stop_below_min_insync),
        s(
            "writes_resume_when_follower_rejoins",
            writes_resume_when_follower_rejoins,
        ),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 607,
        ..ClusterSpec::default()
    }
}

/// How long the pre-shrink requests may wait for an (illegal) ack.
const PRE_T: Duration = Duration::from_secs(15);

/// The state after [`shrink_below_min`].
struct Below {
    p: u16,
    leader: u64,
    followers: Vec<u64>,
    acks: Vec<Ack>,
    pre: Vec<tokio::task::JoinHandle<Start>>,
    hw: Offset,
}

/// The `k`-th highest measured durable end of `p` over `nodes` (1-based).
fn kth_highest_end(
    c: &InProcCluster,
    p: u16,
    nodes: &[u64],
    k: usize,
) -> Result<Offset, ScenarioFailure> {
    let mut ends = Vec::new();
    for n in nodes {
        ends.push(durable_end(c, *n, p)?);
    }
    ends.sort_unstable_by(|a, b| b.cmp(a));
    Ok(ends
        .get(k.saturating_sub(1))
        .copied()
        .unwrap_or(Offset::ZERO))
}

/// Acknowledged baseline, then cut both followers' Fetch (meta traffic keeps flowing), fire
/// appends that can no longer reach `min_insync` replicas, wait for the ISR to shrink to
/// the leader alone, and see a new append rejected `503`.
async fn shrink_below_min(c: &InProcCluster) -> Result<Below, ScenarioFailure> {
    deploy(c).await?;
    let meta = wait_meta_leader(c).await?;
    let a = pick(c, "led by a node other than the meta leader", |a| {
        a.leader != 0 && a.leader != meta
    })?;
    let (p, leader) = (a.partition, a.leader);
    let followers: Vec<u64> = a
        .replicas
        .iter()
        .copied()
        .filter(|r| *r != leader)
        .collect();
    let acks = append_acked(c, p, 3, ACK_T).await?;
    for f in &followers {
        blackhole(c, *f, leader, RpcKind::Fetch);
    }
    let (_, pre) = spawn_starts(c, leader, p, 3, "pre", PRE_T);
    wait_assignment(
        c,
        p,
        "shrunk to the leader alone",
        LAG_MAX + CHANGE_T,
        |x| same_set(&x.isr, &[leader]),
    )
    .await?;
    let api = c.client(leader);
    let rejected = poll_async(Duration::from_secs(5), || {
        let (api, key) = (api.clone(), key_for(c, p, "below"));
        async move {
            match start_keyed(&api, &key, Duration::from_secs(2)).await {
                Start::Rejected(503, _) => Some(Ok(())),
                Start::Acked(a) => Some(Err(fail(
                    "min_insync",
                    format!(
                        "partition {p}: run {} acknowledged with ISR [{leader}] below min_insync",
                        a.run_id
                    ),
                ))),
                _ => None,
            }
        }
    })
    .await;
    match rejected {
        Some(r) => r?,
        None => {
            return Err(fail(
                "min_insync",
                format!(
                    "partition {p}: appends with ISR [{leader}] were not rejected 503 within 5 s"
                ),
            ));
        }
    }
    let hw = leader_hw(c, p).await?.1;
    Ok(Below {
        p,
        leader,
        followers,
        acks,
        pre,
        hw,
    })
}

/// Below `min_insync`: rejected appends, zero acks for the pre-shrink appends, HW frozen
/// and never above the `min_insync`-th highest measured durable end.
fn writes_stop_below_min_insync(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let b = shrink_below_min(c).await?;
        let min = c.spec().min_insync;
        let mut replicas = b.followers.clone();
        replicas.push(b.leader);
        // clock-exempt: test window
        let end = tokio::time::Instant::now() + Duration::from_secs(3);
        while tokio::time::Instant::now() < end {
            let before = kth_highest_end(c, b.p, &replicas, min)?;
            let hw = leader_hw(c, b.p).await?.1;
            let after = kth_highest_end(c, b.p, &replicas, min)?;
            if hw != b.hw {
                return Err(fail(
                    "min_insync",
                    format!(
                        "partition {}: HW moved {} -> {} with the ISR below min_insync",
                        b.p, b.hw.0, hw.0
                    ),
                ));
            }
            if before == after && hw > before {
                return Err(fail(
                    "hw",
                    format!(
                        "partition {}: HW {} is above the measured durable end {} held by {min} replicas",
                        b.p, hw.0, before.0
                    ),
                ));
            }
            pause(Duration::from_millis(200)).await;
        }
        for s in join_starts(b.pre).await {
            if let Start::Acked(a) = s {
                return Err(fail(
                    "min_insync",
                    format!(
                        "partition {}: run {} appended before the shrink was acknowledged with the ISR below min_insync",
                        b.p, a.run_id
                    ),
                ));
            }
        }
        for f in &b.followers {
            clear(c, *f, b.leader);
        }
        final_checks_with(c, false, &b.acks).await
    })
}

/// A follower rejoining the ISR brings it back to `min_insync`: appends are acknowledged
/// again (on >= `min_insync` disks, checked by the final disk oracle).
fn writes_resume_when_follower_rejoins(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let b = shrink_below_min(c).await?;
        let back = b.followers[0];
        clear(c, back, b.leader);
        wait_assignment(
            c,
            b.p,
            "rejoined by the healed follower",
            LAG_MAX + CHANGE_T,
            |x| x.isr.contains(&back),
        )
        .await?;
        let mut acks = b.acks.clone();
        acks.extend(append_acked(c, b.p, 3, Duration::from_secs(10)).await?);
        for s in join_starts(b.pre).await {
            if let Start::Acked(a) = s {
                acks.push(a);
            }
        }
        for f in &b.followers {
            clear(c, *f, b.leader);
        }
        final_checks_with(c, false, &acks).await
    })
}
