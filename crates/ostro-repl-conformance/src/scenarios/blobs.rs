//! CLUS-01 group `blobs` (12-07, research F11 / Pitfall 5): a follower holds every blob a
//! replicated command references before its durable LEO passes the command. A 1 MiB input
//! (stored as a blob, above the 256 KiB inline limit) is acknowledged, the leader is killed
//! at once, and the new leader serves the run: it completes, and its result (the `smoke`
//! workflow echoes its input) is the input, byte for byte.

use std::time::Duration;

use futures_util::future::BoxFuture;
use serde_json::{Value, json};

use super::placement::support::{
    CHANGE_T, SESSION, deploy, fail, final_checks_with, key_for, pick, poll_async, published,
    wait_assignment, wait_completed, wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::inproc::{InProcCluster, TEST_WORKFLOW};
use crate::harness::{ClusterHarness, ClusterSpec};

/// The `blobs` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    vec![Scenario {
        name: "blob_before_commit_survives_failover",
        group: "blobs",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run: blob_before_commit_survives_failover as Run,
    }]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 731,
        ..ClusterSpec::default()
    }
}

/// Size of the blob input.
const BLOB_BYTES: usize = 1024 * 1024;
/// Bound on the acknowledgement of the big start.
const ACK_BIG_T: Duration = Duration::from_secs(30);

/// F11: acknowledged 1 MiB input, leader killed immediately, the new leader serves it.
fn blob_before_commit_survives_failover(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, leader) = (a.partition, a.leader);
        let key = key_for(c, p, "blob");
        let payload: String = (0..BLOB_BYTES)
            .map(|i| char::from(b'a' + (i % 26) as u8))
            .collect();
        let input = json!({ "k": key, "pad": payload });
        let ack = match tokio::time::timeout(
            ACK_BIG_T,
            c.client(leader)
                .start_run(TEST_WORKFLOW, input.clone(), Some(&key)),
        )
        .await
        {
            Ok(Ok(ack)) => ack,
            Ok(Err(e)) => {
                return Err(fail(
                    "availability",
                    format!("partition {p}: the 1 MiB start was not acknowledged: {e}"),
                ));
            }
            Err(_) => {
                return Err(fail(
                    "availability",
                    format!(
                        "partition {p}: the 1 MiB start was not acknowledged within {ACK_BIG_T:?}"
                    ),
                ));
            }
        };
        c.kill(leader).await;
        let next = wait_assignment(c, p, "failed over", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != leader && x.epoch > a.epoch
        })
        .await?;
        let acks = vec![ack.clone()];
        wait_completed(c, &acks, CHANGE_T).await?;
        let cr: &InProcCluster = c;
        let served = poll_async(CHANGE_T, || {
            let (ack, c) = (&ack, cr);
            async move {
                let l = published(c, p).leader;
                if l == 0 || c.handle(l).is_none() {
                    return None;
                }
                let (st, v) = c
                    .client(l)
                    .get_json(&format!("/v1/runs/{}/result", ack.run_id))
                    .await
                    .ok()?;
                st.is_success().then_some(v)
            }
        })
        .await;
        let Some(result) = served else {
            return Err(fail(
                "blobs",
                format!(
                    "partition {p}: new leader {} never served the result of run {}",
                    next.leader, ack.run_id
                ),
            ));
        };
        if !echoes(&result, &input) {
            return Err(fail(
                "blobs",
                format!(
                    "partition {p}: new leader {} serves run {} with a result that is not its 1 MiB input",
                    next.leader, ack.run_id
                ),
            ));
        }
        final_checks_with(c, false, &acks).await
    })
}

/// Whether the result document carries `input` (as the whole body or under a field such as
/// `output` / `result`).
fn echoes(result: &Value, input: &Value) -> bool {
    result == input
        || ["output", "result", "value", "payload"]
            .iter()
            .any(|k| &result[*k] == input)
}
