//! CLUS-01 core group `hw` (12-06): the high watermark is the minimum durable LEO over the
//! ISR (never above a member's measured on-disk end), and on a healthy RF=3 partition it
//! advances within a few fetch rounds, so commits do not wait out `fetch_max_wait`
//! (research Pitfall 1).

use std::time::Duration;

use futures_util::future::BoxFuture;

use super::placement::support::{
    ACK_T, Start, append_acked, clear, delay, deploy, fail, final_checks_with, hw_vs_durable,
    join_starts, pause, pick, published, spawn_starts, within,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::ClusterSpec;
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster};

/// The `hw` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "hw",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run,
    };
    vec![
        s("hw_is_min_over_isr", hw_is_min_over_isr),
        s("hw_rf3_commits_within_rounds", hw_rf3_commits_within_rounds),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 603,
        ..ClusterSpec::default()
    }
}

/// The throttled follower.
const SLOW: u64 = 3;
/// Its per-Fetch delay: well below `replica_lag_max`, so it stays in the ISR.
const THROTTLE_MS: u64 = 800;
/// Bound on one commit on a healthy partition (Pitfall 1).
const COMMIT_T: Duration = Duration::from_secs(5);

/// While the throttled follower is in the published ISR, the leader's HW never exceeds its
/// measured durable end (stable samples only).
fn hw_is_min_over_isr(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let a = pick(c, "followed by the throttled node", |a| {
            a.leader != 0 && a.leader != SLOW && a.replicas.contains(&SLOW)
        })?;
        let (p, leader) = (a.partition, a.leader);
        delay(c, SLOW, leader, RpcKind::Fetch, THROTTLE_MS);
        let mut tasks = Vec::new();
        let mut valid = 0usize;
        // clock-exempt: test window
        let end = tokio::time::Instant::now() + Duration::from_secs(6);
        while tokio::time::Instant::now() < end {
            tasks.extend(spawn_starts(c, leader, p, 1, "hwmin", Duration::from_secs(20)).1);
            let in_isr = published(c, p).isr.contains(&SLOW);
            let sample = hw_vs_durable(c, p, leader, SLOW).await?;
            if in_isr
                && published(c, p).isr.contains(&SLOW)
                && let Some((hw, slow_end)) = sample
            {
                valid += 1;
                if hw > slow_end {
                    return Err(fail(
                        "hw",
                        format!(
                            "partition {p}: leader {leader} HW {} above ISR member {SLOW}'s measured durable end {}",
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
                format!("partition {p}: only {valid} stable samples with node {SLOW} in the ISR"),
            ));
        }
        clear(c, SLOW, leader);
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

/// Pitfall 1: on a healthy RF=3 cluster every sequential append on every partition is
/// acknowledged (HW passed it) within [`COMMIT_T`].
fn hw_rf3_commits_within_rounds(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let mut acks = Vec::new();
        // Warm-up: the module blob reaches every leader before the timed commits.
        for p in 0..c.spec().partitions {
            acks.extend(append_acked(c, p, 1, ACK_T).await?);
        }
        for p in 0..c.spec().partitions {
            for _ in 0..5 {
                acks.extend(append_acked(c, p, 1, COMMIT_T).await?);
            }
        }
        final_checks_with(c, false, &acks).await
    })
}
