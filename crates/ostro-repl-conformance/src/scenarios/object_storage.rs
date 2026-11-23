//! CLUS-01 group `object_storage` (12-07, `spec.objstore = true`): the bottomless bucket
//! holds the committed log and nothing more.
//!
//! - `remote_chain_equals_committed_prefix`: after load and a failover, every partition's
//!   chunk chain exists and equals the committed prefix of its leader's log, byte for byte
//!   (`objstore::chain_equals_committed`).
//! - `remote_chain_never_above_hw` (B4, no failover, catches `upload_above_hw`): both
//!   followers' `Fetch` to one leader is blackholed (and the leader's `Propose` to the meta
//!   leader, so the ISR cannot shrink to {leader} and let the HW move), so the HW stalls
//!   while the leader keeps appending (measured: leader log end above both followers'). After
//!   more than two `seal_interval`s, before healing, no chunk object of any partition ends
//!   above that partition's HW bound (`objstore::nothing_above`). The bound is the minimum
//!   measured durable end over the partition's live ISR, which the HW can never exceed, so a
//!   correct uploader never trips it and one that reads past the HW does.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_types::Offset;

use super::placement::support::{
    ACK_T, CHANGE_T, SESSION, Start, append_acked, blackhole, clear, deploy, durable_end, fail,
    final_checks_with, join_starts, pause, pick, poll_async, published, scan, settle, spawn_starts,
    wait_assignment, wait_keys_on_disk, wait_meta_leader,
};
use super::placement::{LONG_DEADLINE, Run};
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::fault::RpcKind;
use crate::harness::inproc::{Ack, InProcCluster};
use crate::harness::{ClusterHarness, ClusterSpec};
use crate::oracle::objstore;

/// The `object_storage` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, spec, run: Run| Scenario {
        name,
        group: "object_storage",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec,
        run,
    };
    vec![
        s(
            "remote_chain_equals_committed_prefix",
            spec_chain,
            remote_chain_equals_committed_prefix,
        ),
        s(
            "remote_chain_never_above_hw",
            spec_above,
            remote_chain_never_above_hw,
        ),
    ]
}

fn spec_chain() -> ClusterSpec {
    ClusterSpec {
        objstore: true,
        seed: 761,
        ..ClusterSpec::default()
    }
}

fn spec_above() -> ClusterSpec {
    ClusterSpec {
        objstore: true,
        seed: 762,
        ..ClusterSpec::default()
    }
}

/// Unacknowledged appends held above the stalled HW.
const HELD: usize = 6;
/// Bound on those appends reaching the leader's disk.
const LAND_T: Duration = Duration::from_secs(5);

fn bucket(c: &InProcCluster) -> PathBuf {
    c.bucket()
        .unwrap_or_else(|| panic!("harness: spec.objstore cluster has no bucket"))
}

/// `[backup] seal_interval` of the node config.
fn seal_interval(c: &InProcCluster, node: u64) -> Duration {
    c.config(node).backup.seal_interval.get()
}

/// Acknowledged runs on every led partition.
async fn load(c: &InProcCluster, per_partition: usize) -> Result<Vec<Ack>, ScenarioFailure> {
    let mut acks = Vec::new();
    for p in 0..c.spec().partitions {
        if published(c, p).leader != 0 {
            acks.extend(append_acked(c, p, per_partition, ACK_T).await?);
        }
    }
    Ok(acks)
}

/// Wait until every partition has a chain head in the bucket.
async fn wait_heads(c: &InProcCluster, limit: Duration) -> ScenarioResult {
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFs::new(bucket(c)).unwrap_or_else(|e| panic!("harness: open bucket: {e}")));
    for p in 0..c.spec().partitions {
        let store = &store;
        let found = poll_async(limit, || async move {
            matches!(objstore::find_head(store, p).await, Ok(Some(_))).then_some(())
        })
        .await;
        if found.is_none() {
            let why = match objstore::find_head(store, p).await {
                Ok(_) => "nothing uploaded".to_owned(),
                Err(e) => e,
            };
            return Err(fail(
                "objstore",
                format!("partition {p}: no chunk chain head within {limit:?}: {why}"),
            ));
        }
    }
    Ok(())
}

/// Load, a failover, more load; then every partition's chain equals its leader's committed
/// prefix.
fn remote_chain_equals_committed_prefix(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let mut acks = load(c, 4).await?;
        let meta = wait_meta_leader(c).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        c.kill(a.leader).await;
        wait_assignment(c, a.partition, "failed over", SESSION + CHANGE_T, |x| {
            x.leader != 0 && x.leader != a.leader && x.epoch > a.epoch
        })
        .await?;
        acks.extend(load(c, 4).await?);
        let seal = seal_interval(c, meta);
        wait_heads(c, seal * 10 + CHANGE_T).await?;
        // Let the uploader seal what the last load committed.
        pause(seal * 2).await;
        for p in 0..c.spec().partitions {
            let Some((leader, hw, _)) = settle(c, p).await? else {
                return Err(fail("availability", format!("partition {p} is offline")));
            };
            let committed = scan(c, leader, p)?;
            objstore::chain_equals_committed(&bucket(c), p, &committed.records, hw)
                .await
                .into_result()?;
        }
        final_checks_with(c, false, &acks).await
    })
}

/// The HW bound of `p`: the minimum measured durable end over its live published ISR.
fn hw_bound(c: &InProcCluster, p: u16) -> Result<Offset, ScenarioFailure> {
    let a = published(c, p);
    let mut bound: Option<Offset> = None;
    for n in a.isr.iter().filter(|n| c.handle(**n).is_some()) {
        let e = durable_end(c, *n, p)?;
        bound = Some(bound.map_or(e, |b| b.min(e)));
    }
    Ok(bound.unwrap_or(Offset::ZERO))
}

/// B4: with the HW stalled below the leader's log end for more than two seal intervals, no
/// chunk ends above the HW.
fn remote_chain_never_above_hw(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let meta = wait_meta_leader(c).await?;
        let mut acks = load(c, 3).await?;
        let a = pick(c, "led by a node other than the meta leader", |a| {
            a.leader != 0 && a.leader != meta
        })?;
        let (p, leader) = (a.partition, a.leader);
        for q in 0..c.spec().partitions {
            if settle(c, q).await?.is_none() {
                return Err(fail("availability", format!("partition {q} is offline")));
            }
        }
        let followers: Vec<u64> = a
            .replicas
            .iter()
            .copied()
            .filter(|r| *r != leader)
            .collect();
        for f in &followers {
            blackhole(c, *f, leader, RpcKind::Fetch);
        }
        blackhole(c, leader, meta, RpcKind::Propose);
        let (keys, tasks) = spawn_starts(c, leader, p, HELD, "above", Duration::from_secs(30));
        wait_keys_on_disk(c, leader, p, &keys, LAND_T).await?;
        let stalled = hw_bound(c, p)?;
        let leo = durable_end(c, leader, p)?;
        if leo <= stalled {
            return Err(fail(
                "objstore",
                format!(
                    "partition {p}: leader {leader} log end {} is not above the stalled HW bound {} (followers {followers:?} still replicate)",
                    leo.0, stalled.0
                ),
            ));
        }
        let seal = seal_interval(c, leader);
        pause(seal * 2 + Duration::from_millis(500)).await;
        for q in 0..c.spec().partitions {
            let bound = if q == p { stalled } else { hw_bound(c, q)? };
            objstore::nothing_above(&bucket(c), q, bound)
                .await
                .into_result()?;
        }
        for f in &followers {
            clear(c, *f, leader);
        }
        clear(c, leader, meta);
        for s in join_starts(tasks).await {
            if let Start::Acked(ack) = s {
                acks.push(ack);
            }
        }
        final_checks_with(c, false, &acks).await
    })
}
