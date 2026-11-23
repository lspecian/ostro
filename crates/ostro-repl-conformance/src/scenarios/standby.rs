//! CLUS-01 group `standby` (12-07, CLUS-04): followers are hot standbys. They apply the
//! committed log as it commits, so within a second of the HW settling every replica sits at
//! the same `applied_offset` with the same state (`state_hash`, the logical digest, every
//! table, and the derived `x_*` keyspaces when both sides have them ready). Followers never
//! dispatch: only the leader's dispatcher grants work.
//!
//! This module also hosts [`derived`], the measured read of the derived `x_run_index`
//! keyspace (not part of OSNP and so not covered by the `standby` oracle's table digests),
//! shared with the `bootstrap` group.

use std::sync::atomic::Ordering;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_types::{PartitionCount, PartitionId};
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, append_acked, deploy, fail, final_checks_with, pause, published, settle, wait_completed,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::InProcCluster;
use crate::harness::{ClusterHarness, ClusterSpec, DataDirView};
use crate::oracle::standby;

/// The `standby` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, run: Run| Scenario {
        name,
        group: "standby",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec: one_partition,
        run,
    };
    vec![s("followers_are_hot_standbys", followers_are_hot_standbys)]
}

/// One partition on three nodes: one leader, two followers (the dispatcher counters are
/// per node, so a node that leads nothing must show zero grants).
fn one_partition() -> ClusterSpec {
    ClusterSpec {
        partitions: 1,
        seed: 701,
        ..ClusterSpec::default()
    }
}

/// How far behind the HW a hot standby may apply.
pub(crate) const TRACK_T: Duration = Duration::from_secs(1);

/// Wait until every node in `nodes` holds partition `p` at one applied offset with equal
/// state (standby oracle plus the derived keyspaces), within `limit`. Returns the last
/// violation on timeout.
pub(crate) async fn wait_equal_state(
    c: &InProcCluster,
    p: u16,
    nodes: &[u64],
    limit: Duration,
) -> ScenarioResult {
    let count = PartitionCount::new(c.spec().partitions).expect("partitions > 0");
    let views: Vec<DataDirView> = nodes.iter().map(|n| c.node_disk(*n)).collect();
    // clock-exempt: test deadline
    let end = Instant::now() + limit;
    loop {
        let report = standby::equal_state(&views, PartitionId(p), count);
        let last = if report.is_ok() {
            match derived::compare(&views, PartitionId(p), count) {
                Ok(()) => return Ok(()),
                Err(e) => e,
            }
        } else {
            match report.into_result() {
                Err(e) => e,
                Ok(()) => unreachable!("report not ok"),
            }
        };
        if Instant::now() >= end {
            return Err(ScenarioFailure {
                oracle: last.oracle,
                detail: format!("after {limit:?}: {}", last.detail),
            });
        }
        pause(Duration::from_millis(50)).await;
    }
}

/// Acknowledged runs complete; once the HW settles, the followers reach the leader's
/// applied offset with identical state within [`TRACK_T`]; the followers' dispatchers
/// granted nothing while the leader's granted every run.
fn followers_are_hot_standbys(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let p = 0u16;
        let acks = append_acked(c, p, 12, ACK_T).await?;
        wait_completed(c, &acks, ACK_T).await?;
        let Some((leader, _hw, members)) = settle(c, p).await? else {
            return Err(fail("availability", format!("partition {p} is offline")));
        };
        let a = published(c, p);
        if members.len() != a.replicas.len() {
            return Err(fail(
                "standby",
                format!(
                    "partition {p}: live ISR {members:?} is not every replica {:?}",
                    a.replicas
                ),
            ));
        }
        wait_equal_state(c, p, &members, TRACK_T).await?;
        for n in c.node_ids() {
            let granted = c
                .handle(n)
                .map(|h| h.dispatcher_stats().dispatched.load(Ordering::Relaxed))
                .unwrap_or(0);
            if n != leader && granted != 0 {
                return Err(fail(
                    "standby",
                    format!(
                        "follower {n} of partition {p} dispatched {granted} job(s): only the leader {leader} grants"
                    ),
                ));
            }
            if n == leader && granted == 0 {
                return Err(fail(
                    "standby",
                    format!(
                        "leader {leader} of partition {p} dispatched nothing for {} completed runs",
                        acks.len()
                    ),
                ));
            }
        }
        final_checks_with(c, false, &acks).await
    })
}

/// Measured read of the derived `x_run_index` keyspace (12-07). Each node's state store is
/// copied and opened on the copy, as the `standby` oracle does; the keyspace is compared
/// only between nodes where it is `Ready` (a rebuilding index is not yet derived state).
pub(crate) mod derived {
    use ostro_partition::store::derived::IndexState;
    use ostro_partition::store::{PartitionStore, StoreConfig, state_dir};
    use ostro_types::{PartitionCount, PartitionId};
    use sha2::{Digest, Sha256};

    use crate::harness::DataDirView;
    use crate::oracle::copy_dir;
    use crate::scenarios::ScenarioFailure;

    /// `(node, row count, sha256 over the rows)` when the node's index is ready.
    pub(crate) fn measure(
        view: &DataDirView,
        p: PartitionId,
        count: PartitionCount,
    ) -> Result<Option<(usize, [u8; 32])>, String> {
        let src = state_dir(&view.root, p);
        if !src.is_dir() {
            return Err(format!("{} does not exist", src.display()));
        }
        let tmp = tempfile::tempdir().map_err(|e| format!("tempdir: {e}"))?;
        copy_dir(&src, &state_dir(tmp.path(), p))
            .map_err(|e| format!("copy {}: {e}", src.display()))?;
        let (store, _) = PartitionStore::open(tmp.path(), p, count, &StoreConfig::default())
            .map_err(|e| format!("open copy of {}: {e}", src.display()))?;
        let reader = store.reader();
        if reader.run_index_state() != IndexState::Ready {
            return Ok(None);
        }
        let rows = reader
            .run_index_rows(&[], None, usize::MAX)
            .map_err(|e| format!("read x_run_index: {e}"))?;
        let mut h = Sha256::new();
        for (k, v) in &rows {
            h.update((k.len() as u64).to_le_bytes());
            h.update(k);
            h.update((v.len() as u64).to_le_bytes());
            h.update(v);
        }
        Ok(Some((rows.len(), h.finalize().into())))
    }

    /// Every ready node's `x_run_index` equals the first ready node's.
    pub(crate) fn compare(
        views: &[DataDirView],
        p: PartitionId,
        count: PartitionCount,
    ) -> Result<(), ScenarioFailure> {
        let mut first: Option<(u64, usize, [u8; 32])> = None;
        for v in views {
            let m = measure(v, p, count).map_err(|e| ScenarioFailure {
                oracle: "standby",
                detail: format!("node {} partition {}: x_run_index: {e}", v.node, p.0),
            })?;
            let Some((rows, sha)) = m else { continue };
            match first {
                None => first = Some((v.node, rows, sha)),
                Some((n0, r0, s0)) if s0 != sha => {
                    return Err(ScenarioFailure {
                        oracle: "standby",
                        detail: format!(
                            "partition {}: derived x_run_index differs between node {n0} ({r0} rows) and node {} ({rows} rows)",
                            p.0, v.node
                        ),
                    });
                }
                Some(_) => {}
            }
        }
        Ok(())
    }
}
