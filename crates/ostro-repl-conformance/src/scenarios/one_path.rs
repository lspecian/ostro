//! CLUS-04 group `one_path` (12-07, D-P12-15): RF=1 runs the same replicated log as RF=3.
//! Its HW rises only through the replica tracker (ISR = {self}), so the tracker's HW
//! publication counter (`hw_publishes` in `GET /v1/admin/replication`) is positive and
//! tracks the HW advances: each acknowledged sequential append needs at least one
//! publication (the HW was below it before), and each publication needs at least one new
//! durable record (measured by the verified WAL scan). An RF=1 shortcut that publishes the
//! HW straight from the WAL callback leaves the counter at zero.

use futures_util::future::BoxFuture;

use super::placement::support::{
    ACK_T, append_acked, deploy, fail, final_checks_with, scan, settle,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::{ClusterHarness, ClusterSpec};

/// The `one_path` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    vec![Scenario {
        name: "one_path_rf1_hw_through_tracker",
        group: "one_path",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: single,
        run: one_path_rf1_hw_through_tracker as Run,
    }]
}

fn single() -> ClusterSpec {
    ClusterSpec {
        seed: 741,
        ..ClusterSpec::single()
    }
}

/// Sequential acknowledged appends per partition.
const K: u64 = 5;

/// `hw_publishes` of partition `p` on `node` (a missing route or field is a status failure).
async fn hw_publishes(c: &InProcCluster, node: u64, p: u16) -> Result<u64, ScenarioFailure> {
    let v = c.admin_status(node).await;
    v["partitions"]
        .as_array()
        .and_then(|ps| {
            ps.iter()
                .find(|e| e["partition"].as_u64() == Some(u64::from(p)))
        })
        .and_then(|e| e["hw_publishes"].as_u64())
        .ok_or_else(|| {
            fail(
                "status",
                format!(
                    "node {node} partition {p}: /v1/admin/replication reports no hw_publishes (got {v})"
                ),
            )
        })
}

/// D-P12-15: per partition, `K <= Δhw_publishes <= Δrecords` across K sequential acked
/// appends, `hw_publishes > 0`, and the reported HW settles at the measured durable end.
fn one_path_rf1_hw_through_tracker(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let node = 1u64;
        let mut acks = Vec::new();
        for p in 0..c.spec().partitions {
            let before = hw_publishes(c, node, p).await?;
            let rec_before = scan(c, node, p)?.records.len() as u64;
            for _ in 0..K {
                acks.extend(append_acked(c, p, 1, ACK_T).await?);
            }
            let after = hw_publishes(c, node, p).await?;
            let rec_after = scan(c, node, p)?.records.len() as u64;
            let (published, appended) = (after.saturating_sub(before), rec_after - rec_before);
            if after == 0 || published < K {
                return Err(fail(
                    "one_path",
                    format!(
                        "partition {p}: {K} sequential acknowledged appends, but the tracker published the HW {published} time(s) (hw_publishes {before} -> {after}): the RF=1 HW bypasses the replica tracker"
                    ),
                ));
            }
            if published > appended {
                return Err(fail(
                    "one_path",
                    format!(
                        "partition {p}: hw_publishes rose by {published} while only {appended} durable record(s) were appended"
                    ),
                ));
            }
        }
        for p in 0..c.spec().partitions {
            if settle(c, p).await?.is_none() {
                return Err(fail("availability", format!("partition {p} is offline")));
            }
        }
        final_checks_with(c, false, &acks).await
    })
}
