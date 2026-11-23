//! CLUS-01 core group `isr_honesty` (12-06): the ISR is derived from measured durable LEOs,
//! never assumed. A follower that never fetched is not in the ISR and `/admin` never lists
//! it while it lacks the HW; a lagging follower rejoins only once its on-disk durable end
//! reaches the HW; a slow follower is shrunk out, and while that shrink is still
//! uncommitted the leader keeps computing the HW over the maximal ISR (B4, re-check W4).

use std::collections::BTreeMap;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_types::Offset;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, Start, append_acked, blackhole, clear, delay, deploy, durable_end,
    fail, final_checks_with, hw_vs_durable, join_starts, leader_hw, pause, pick, poll, poll_async,
    published, spawn_starts, wait_assignment, wait_completed, wait_meta_leader, within,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster};
use crate::harness::{ClusterHarness, ClusterSpec};
use crate::oracle::status;

/// The `isr_honesty` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "isr_honesty",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run,
    };
    vec![
        s(
            "isr_empty_follower_not_in_isr",
            isr_empty_follower_not_in_isr,
        ),
        s("isr_lagging_follower_rejoins", isr_lagging_follower_rejoins),
        s("isr_slow_follower_shrinks", isr_slow_follower_shrinks),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 602,
        ..ClusterSpec::default()
    }
}

/// The lagging node of these scenarios.
const LAGGER: u64 = 3;

/// One measured honesty sample of partition `p`: the published ISR is read before and after
/// the leader's status, and only a sample where both agree is checked (`Ok(false)`
/// otherwise). Every node's durable end is measured **after** the status: durable ends only
/// grow, so a member whose end measured afterwards is still below the HW the status reported
/// was below it when the status was taken (a sound violation, whatever moved meanwhile). The
/// HW checked is the one the status itself reports.
///
/// 12-16: the sample used to be discarded whenever any node's durable end moved while it was
/// taken, which on a partition with runs still executing left no checkable sample at all
/// (the `isr_from_assignment` mutant then failed on "no stable status sample" instead of on
/// the never-fetching follower listed in the ISR).
async fn honest_sample(c: &InProcCluster, p: u16) -> Result<bool, ScenarioFailure> {
    let before = published(c, p);
    if before.leader == 0 {
        return Ok(false);
    }
    let admin = c.admin_status(before.leader).await;
    let after = published(c, p);
    if before != after {
        return Ok(false);
    }
    let mut leos = BTreeMap::new();
    for n in c.node_ids() {
        leos.insert(n, durable_end(c, n, p)?);
    }
    let hw = admin["partitions"]
        .as_array()
        .and_then(|ps| {
            ps.iter()
                .find(|e| e["partition"].as_u64() == Some(u64::from(p)))
        })
        .and_then(|e| e["hw"].as_u64())
        .map(Offset)
        .ok_or_else(|| {
            fail(
                "status",
                format!("partition {p}: leader {} reports no HW", before.leader),
            )
        })?;
    status::honest(&admin, p, &before.isr, &leos, hw).into_result()?;
    Ok(true)
}

/// Blackhole every Fetch `LAGGER` sends; partitions it follows; acknowledged writes to each.
async fn lag_and_write(c: &InProcCluster) -> Result<(Vec<u16>, Vec<Ack>), ScenarioFailure> {
    deploy(c).await?;
    for n in c.node_ids().into_iter().filter(|n| *n != LAGGER) {
        blackhole(c, LAGGER, n, RpcKind::Fetch);
    }
    let followed: Vec<u16> = (0..c.spec().partitions)
        .filter(|p| {
            let a = published(c, *p);
            a.leader != LAGGER && a.replicas.contains(&LAGGER)
        })
        .collect();
    if followed.is_empty() {
        return Err(fail(
            "placement",
            format!("node {LAGGER} follows no partition"),
        ));
    }
    let mut acks = Vec::new();
    for p in &followed {
        acks.extend(append_acked(c, *p, 3, ACK_T).await?);
    }
    Ok((followed, acks))
}

fn heal_lagger(c: &InProcCluster) {
    for n in c.node_ids().into_iter().filter(|n| *n != LAGGER) {
        clear(c, LAGGER, n);
    }
}

/// A follower blackholed from before any data never appears in the published ISR once the
/// HW passes it, and the status never lists a member without the HW on disk.
fn isr_empty_follower_not_in_isr(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (followed, acks) = lag_and_write(c).await?;
        let mut valid = BTreeMap::<u16, usize>::new();
        let window = LAG_MAX + Duration::from_secs(3);
        // clock-exempt: test window
        let end = tokio::time::Instant::now() + window;
        while tokio::time::Instant::now() < end {
            for p in &followed {
                if honest_sample(c, *p).await? {
                    *valid.entry(*p).or_default() += 1;
                }
            }
            pause(Duration::from_millis(100)).await;
        }
        for p in &followed {
            wait_assignment(
                c,
                *p,
                "free of the never-fetching follower",
                CHANGE_T,
                |a| !a.isr.contains(&LAGGER),
            )
            .await?;
            // The status agrees with the published ISR (and so never lists the lagger).
            let p = *p;
            let c: &InProcCluster = c;
            let agreed = poll_async(CHANGE_T, || async move {
                match honest_sample(c, p).await {
                    Ok(true) => Some(Ok(true)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .await
            .unwrap_or(Ok(false))?;
            if !agreed || valid.get(&p).copied().unwrap_or(0) == 0 {
                return Err(fail(
                    "status",
                    format!("partition {p}: no stable status sample to check within the window"),
                ));
            }
        }
        heal_lagger(c);
        final_checks_with(c, false, &acks).await
    })
}

/// A follower shrunk out for lagging is re-added only once its measured durable end has
/// reached the HW (writes stopped and runs completed, so the HW is static).
fn isr_lagging_follower_rejoins(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let (followed, acks) = lag_and_write(c).await?;
        for p in &followed {
            wait_assignment(
                c,
                *p,
                "free of the lagging follower",
                LAG_MAX + CHANGE_T,
                |a| !a.isr.contains(&LAGGER),
            )
            .await?;
        }
        wait_completed(c, &acks, ACK_T).await?;
        let mut hws = BTreeMap::new();
        for p in &followed {
            hws.insert(*p, leader_hw(c, *p).await?.1);
        }
        heal_lagger(c);
        for p in &followed {
            let hw = hws[p];
            let joined = poll(LAG_MAX + CHANGE_T, || {
                if published(c, *p).isr.contains(&LAGGER) {
                    Some(durable_end(c, LAGGER, *p))
                } else {
                    None
                }
            })
            .await;
            match joined {
                None => {
                    return Err(fail(
                        "isr",
                        format!(
                            "partition {p}: healed node {LAGGER} not re-added within {:?}",
                            LAG_MAX + CHANGE_T
                        ),
                    ));
                }
                Some(end) => {
                    let end = end?;
                    if end < hw {
                        return Err(fail(
                            "isr",
                            format!(
                                "partition {p}: node {LAGGER} re-added with measured durable end {} < HW {}",
                                end.0, hw.0
                            ),
                        ));
                    }
                }
            }
        }
        final_checks_with(c, false, &acks).await
    })
}

/// B4: a slow follower is shrunk out; while the shrink `AlterIsr` is held uncommitted (the
/// partition leader's `Propose` to the meta leader is blackholed) and appends keep moving
/// the leader's LEO, the leader's HW never exceeds the slow follower's measured durable
/// end. Healed, the follower is removed within lag + 2 s.
fn isr_slow_follower_shrinks(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != meta && a.leader != 0
        })?;
        let (p, leader) = (a.partition, a.leader);
        let slow = if leader != LAGGER && a.replicas.contains(&LAGGER) {
            LAGGER
        } else {
            *a.replicas
                .iter()
                .filter(|r| **r != leader)
                .max()
                .ok_or_else(|| fail("placement", format!("partition {p} has no follower")))?
        };
        blackhole(c, leader, meta, RpcKind::Propose);
        let slow_ms = u64::try_from(LAG_MAX.as_millis()).unwrap_or(2000) * 2;
        delay(c, slow, leader, RpcKind::Fetch, slow_ms);

        let mut tasks = Vec::new();
        let mut valid = 0usize;
        let window = LAG_MAX + Duration::from_secs(3);
        // clock-exempt: test window
        let end = tokio::time::Instant::now() + window;
        while tokio::time::Instant::now() < end {
            // Continuous appends: the leader's LEO and the fast follower's end keep moving.
            tasks.extend(spawn_starts(c, leader, p, 1, "slow", Duration::from_secs(20)).1);
            if let Some((hw, slow_end)) = hw_vs_durable(c, p, leader, slow).await? {
                valid += 1;
                if hw > slow_end {
                    return Err(fail(
                        "hw",
                        format!(
                            "partition {p}: leader {leader} HW {} above slow follower {slow}'s measured durable end {} while its shrink is uncommitted",
                            hw.0, slow_end.0
                        ),
                    ));
                }
            }
            pause(Duration::from_millis(100)).await;
        }
        if valid < 3 {
            return Err(fail(
                "hw",
                format!("partition {p}: only {valid} stable HW samples during the hold"),
            ));
        }
        clear(c, leader, meta);
        wait_assignment(
            c,
            p,
            "shrunk without the slow follower",
            LAG_MAX + Duration::from_secs(2),
            |a| !a.isr.contains(&slow),
        )
        .await?;
        clear(c, slow, leader);
        let mut acks: Vec<Ack> = Vec::new();
        if let Some(done) = within(Duration::from_secs(30), join_starts(tasks)).await {
            acks.extend(done.into_iter().filter_map(|s| match s {
                Start::Acked(a) => Some(a),
                _ => None,
            }));
        }
        final_checks_with(c, false, &acks).await
    })
}
