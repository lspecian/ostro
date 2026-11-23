//! CLUS-03 group `zombie` (12-07): an isolated leader cannot commit. Cut off from every
//! node (its API still answers), it never acknowledges a write; it demotes itself within
//! `session_timeout` + 1 s of losing meta contact and then answers writes `503 Unavailable`
//! with a leader hint instead of hanging; after the failover, whatever it (or its executors)
//! appended under the old epoch is fenced: when the links heal it rejoins as a follower and
//! none of it is below the HW on any node.

use std::time::Duration;

use futures_util::future::BoxFuture;
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, SESSION, Start, append_acked, deploy, fail, final_checks_with,
    join_starts, key_for, none_committed, pick, settle, spawn_starts, start_keyed, wait_assignment,
    wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::{ClusterHarness, ClusterSpec};

/// The `zombie` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    vec![Scenario {
        name: "isolated_leader_cannot_commit",
        group: "zombie",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run: isolated_leader_cannot_commit as Run,
    }]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 751,
        ..ClusterSpec::default()
    }
}

/// Requests fired at the zombie right after the isolation.
const K: usize = 3;
/// Bound on self-demotion after the isolation.
const DEMOTE_T: Duration = Duration::from_millis(SESSION.as_millis() as u64 + 1000);
/// Per-probe request bound (a zombie that still thinks it leads holds the request).
const PROBE_T: Duration = Duration::from_millis(300);

/// The zombie leader: isolated, it acknowledges nothing, demotes itself in time and answers
/// 503 with a leader hint; its old-epoch tail never commits.
fn isolated_leader_cannot_commit(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, zombie) = (a.partition, a.leader);
        let mut acks = append_acked(c, p, 3, ACK_T).await?;

        // clock-exempt: test measurement
        let t0 = Instant::now();
        c.isolate(zombie);
        let (mut keys, tasks) = spawn_starts(c, zombie, p, K, "zombie", Duration::from_secs(8));

        let api = c.client(zombie);
        let mut demoted = None;
        let mut last = String::from("no probe answered");
        while Instant::now() <= t0 + DEMOTE_T {
            let sent = Instant::now();
            let key = key_for(c, p, "zprobe");
            let r = start_keyed(&api, &key, PROBE_T).await;
            keys.push(key);
            match r {
                Start::Rejected(503, body) => {
                    demoted = Some((sent.duration_since(t0), body));
                    break;
                }
                Start::Acked(ack) => {
                    return Err(fail(
                        "zombie",
                        format!(
                            "isolated leader {zombie} acknowledged run {} on partition {p}",
                            ack.run_id
                        ),
                    ));
                }
                other => last = other.describe(),
            }
        }
        let Some((after, body)) = demoted else {
            return Err(fail(
                "zombie",
                format!(
                    "isolated leader {zombie} of partition {p} did not answer 503 within {DEMOTE_T:?} of losing meta contact: {last}"
                ),
            ));
        };
        if !body.to_ascii_lowercase().contains("leader") {
            return Err(fail(
                "zombie",
                format!(
                    "isolated node {zombie} answered 503 after {after:?} without a leader hint: {body}"
                ),
            ));
        }

        wait_assignment(c, p, "failed over", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != zombie && x.epoch > a.epoch
        })
        .await?;
        for (k, r) in keys.iter().zip(join_starts(tasks).await) {
            if let Start::Acked(ack) = r {
                return Err(fail(
                    "zombie",
                    format!(
                        "isolated leader {zombie} acknowledged run {} (key {k}) on partition {p}",
                        ack.run_id
                    ),
                ));
            }
        }
        acks.extend(append_acked(c, p, 3, ACK_T).await?);

        c.heal(zombie);
        wait_assignment(
            c,
            p,
            "rejoined by the old leader",
            LAG_MAX + CHANGE_T,
            |x| x.isr.contains(&zombie),
        )
        .await?;
        match settle(c, p).await? {
            Some((_, hw, _)) => none_committed(c, p, hw, &keys)?,
            None => return Err(fail("availability", format!("partition {p} is offline"))),
        }
        final_checks_with(c, false, &acks).await
    })
}
