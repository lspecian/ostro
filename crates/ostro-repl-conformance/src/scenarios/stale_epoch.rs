//! CLUS-01 core group `stale_epoch` (12-06): the leader fences fetches by leader epoch
//! (KIP-320). A raw `Replication.Fetch` sent over the cluster transport as a real follower
//! (`channel_as(follower, leader)`, so the leader sees that node's identity) carrying an
//! older epoch is answered `FENCED_LEADER_EPOCH`; one carrying a newer epoch than the
//! leader knows is answered `UNKNOWN_LEADER_EPOCH`.

use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_proto::cluster::v1::replication_client::ReplicationClient;
use ostro_proto::cluster::v1::{FetchError, FetchRequest};

use super::placement::support::{
    CHANGE_T, durable_end, fail, final_checks, pick, wait_assignment, wait_meta_leader, within,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioResult, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::{ClusterHarness, ClusterSpec};

/// The `stale_epoch` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "stale_epoch",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: rf3,
        run,
    };
    vec![
        s(
            "fetch_with_stale_epoch_fenced",
            fetch_with_stale_epoch_fenced,
        ),
        s(
            "fetch_with_future_epoch_unknown",
            fetch_with_future_epoch_unknown,
        ),
    ]
}

fn rf3() -> ClusterSpec {
    ClusterSpec {
        seed: 604,
        ..ClusterSpec::default()
    }
}

/// Bound on one raw fetch answer (`max_wait_ms` is 0: no long poll).
const FETCH_T: Duration = Duration::from_secs(5);

/// Send one raw Fetch from `from` (as that node) to `to` for `p` at `leader_epoch`, and
/// require the answer `want`.
async fn fetch_expect(
    c: &InProcCluster,
    from: u64,
    to: u64,
    p: u16,
    leader_epoch: u32,
    want: FetchError,
) -> ScenarioResult {
    let end = durable_end(c, from, p)?;
    let req = FetchRequest {
        partition: u32::from(p),
        replica_id: from,
        leader_epoch,
        fetch_offset: end.0,
        last_fetched_epoch: leader_epoch,
        durable_leo: end.0,
        max_bytes: 1 << 20,
        max_wait_ms: 0,
        min_bytes: 0,
    };
    let mut client = ReplicationClient::new(c.channel_as(from, to));
    let at = format!("partition {p}: Fetch from node {from} to node {to} at epoch {leader_epoch}");
    match within(FETCH_T, client.fetch(req)).await {
        None => Err(fail("fence", format!("{at}: no answer within {FETCH_T:?}"))),
        Some(Err(status)) => Err(fail("fence", format!("{at}: RPC failed: {status}"))),
        Some(Ok(resp)) => {
            let resp = resp.into_inner();
            if resp.error() == want {
                if want != FetchError::None && !resp.envelopes.is_empty() {
                    return Err(fail(
                        "fence",
                        format!(
                            "{at}: answered {want:?} but returned {} envelopes",
                            resp.envelopes.len()
                        ),
                    ));
                }
                Ok(())
            } else {
                Err(fail(
                    "fence",
                    format!(
                        "{at}: answered {:?} (leader epoch {}), expected {want:?}",
                        resp.error(),
                        resp.leader_epoch
                    ),
                ))
            }
        }
    }
}

/// After a failover, a fetch carrying the old leader epoch is fenced by the new leader.
fn fetch_with_stale_epoch_fenced(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, old_leader, old_epoch) = (a.partition, a.leader, a.epoch);
        c.kill(old_leader).await;
        let now = wait_assignment(c, p, "failed over", CHANGE_T, |x| {
            x.leader != 0 && x.leader != old_leader && x.epoch > old_epoch
        })
        .await?;
        let follower = *now
            .replicas
            .iter()
            .find(|r| **r != now.leader && **r != old_leader)
            .ok_or_else(|| fail("placement", format!("partition {p} has no live follower")))?;
        fetch_expect(
            c,
            follower,
            now.leader,
            p,
            old_epoch,
            FetchError::FencedLeaderEpoch,
        )
        .await?;
        // The current epoch is still served.
        fetch_expect(c, follower, now.leader, p, now.epoch, FetchError::None).await?;
        let restarted = c.restart(old_leader).await;
        if let Err(e) = restarted {
            panic!("harness: restart node {old_leader}: {e}");
        }
        final_checks(c, true).await
    })
}

/// A fetch carrying an epoch newer than the leader's is answered `UNKNOWN_LEADER_EPOCH`.
fn fetch_with_future_epoch_unknown(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        let a = pick(c, "with a leader", |a| a.leader != 0)?;
        let p = a.partition;
        let follower = *a
            .replicas
            .iter()
            .find(|r| **r != a.leader)
            .ok_or_else(|| fail("placement", format!("partition {p} has no follower")))?;
        fetch_expect(
            c,
            follower,
            a.leader,
            p,
            a.epoch + 5,
            FetchError::UnknownLeaderEpoch,
        )
        .await?;
        fetch_expect(c, follower, a.leader, p, a.epoch, FetchError::None).await?;
        final_checks(c, true).await
    })
}
