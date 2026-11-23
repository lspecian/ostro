//! CLUS-01 group `bootstrap` (12-07, CLUS-04, research Pattern 6): a replica whose log
//! cannot be served by fetching (empty, or behind the leader's `log_start` after the leader
//! snapshotted and trimmed) is answered `SNAPSHOT_REQUIRED`, installs the leader's OSNP
//! snapshot, fetches from its applied offset and ends a hot standby with identical state
//! (tables, digest, derived keyspaces). A `FetchSnapshot` stream cut mid-way resumes.
//!
//! Every verdict is measured: the leader's trimmed `log_start` and the rejoined node's log
//! start are read from the verified WAL scans (a node that installed has its log anchored at
//! the snapshot offset, so its first record starts above what it held, or above 0 when it
//! came back empty), the ISR from the committed meta log, the state through the `standby`
//! oracle. The node's own snapshot and retention services are driven through its
//! [`ostro_partition::Node`] (`snapshot_now`, `retention_pass`) so the trim does not wait
//! for the hourly cadence. The `install::` failpoint kill loop is 12-12's unit test.

use std::path::Path;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_meta::PartitionAssignment;
use ostro_partition::leader::snapshot::snapshot_dir;
use ostro_types::{Offset, PartitionId};
use tokio::time::Instant;

use super::placement::support::{
    ACK_T, CHANGE_T, LAG_MAX, append_acked, deploy, fail, final_checks_with, harness, pause, poll,
    published_all, scan, settle, wait_assignment, wait_completed,
};
use super::placement::{LONG_DEADLINE, Run};
use super::standby::wait_equal_state;
use super::{Scenario, ScenarioFailure, ScenarioResult, Tier};
use crate::harness::inproc::{Ack, InProcCluster};
use crate::harness::{ClusterHarness, ClusterSpec, HarnessError};

/// The `bootstrap` scenarios.
pub fn scenarios() -> Vec<Scenario> {
    let s = |name, spec, run: Run| Scenario {
        name,
        group: "bootstrap",
        tier: Tier::B,
        deadline: LONG_DEADLINE,
        spec,
        run,
    };
    vec![
        s(
            "empty_node_bootstraps_from_snapshot",
            spec_empty,
            empty_node_bootstraps_from_snapshot,
        ),
        s(
            "node_behind_log_start_reinstalls",
            spec_behind,
            node_behind_log_start_reinstalls,
        ),
        s(
            "install_stream_cut_resumes",
            spec_cut,
            install_stream_cut_resumes,
        ),
    ]
}

fn spec_empty() -> ClusterSpec {
    ClusterSpec {
        seed: 711,
        wal_rotation_bytes: Some(WAL_ROTATION),
        ..ClusterSpec::default()
    }
}

fn spec_behind() -> ClusterSpec {
    ClusterSpec {
        seed: 712,
        wal_rotation_bytes: Some(WAL_ROTATION),
        ..ClusterSpec::default()
    }
}

fn spec_cut() -> ClusterSpec {
    ClusterSpec {
        seed: 713,
        wal_rotation_bytes: Some(WAL_ROTATION),
        ..ClusterSpec::default()
    }
}

/// WAL segment rotation of the bootstrap clusters: retention deletes whole segments only,
/// and with the 250 MB default a test-sized log would never trim (12-16).
const WAL_ROTATION: u64 = 2 * 1024;

/// The node that is stopped, emptied or left behind, and brought back.
const LATE: u64 = 3;
/// Bound on the late node installing, catching up and rejoining the ISR.
const CATCHUP_T: Duration = Duration::from_secs(60);
/// Bound on observing a `FetchSnapshot` stream being staged.
const STAGE_T: Duration = Duration::from_secs(20);

fn restart_or_panic(r: Result<(), HarnessError>, node: u64) {
    if let Err(e) = r {
        panic!("harness: restart node {node}: {e}");
    }
}

/// Measured log start of `node`'s partition `p`: the base of its first verified record
/// (the durable end, or 0, for an empty log).
fn log_start(c: &InProcCluster, node: u64, p: u16) -> Result<Offset, ScenarioFailure> {
    let s = scan(c, node, p)?;
    Ok(s.records
        .first()
        .map(|r| r.base)
        .or_else(|| s.durable_end())
        .unwrap_or(Offset::ZERO))
}

/// Delete the replicated partition data of a stopped node (WAL, state, snapshots, blobs),
/// keeping its identity (`node.toml`) and its metadata Raft log.
fn wipe_partition_data(root: &Path) {
    for d in ["wal", "state", "snapshots", "blobs"] {
        let dir = root.join(d);
        if dir.exists() {
            harness(
                &format!("remove {}", dir.display()),
                std::fs::remove_dir_all(&dir),
            );
        }
    }
}

/// Partitions the late node replicates.
fn late_partitions(c: &InProcCluster) -> Vec<PartitionAssignment> {
    published_all(c)
        .into_iter()
        .filter(|a| a.replicas.contains(&LATE))
        .collect()
}

/// Acknowledged runs on every partition, waited to completion (so the snapshots carry
/// terminal runs and the derived run index).
async fn load(c: &InProcCluster, per_partition: usize) -> Result<Vec<Ack>, ScenarioFailure> {
    let mut acks = Vec::new();
    for p in 0..c.spec().partitions {
        acks.extend(append_acked(c, p, per_partition, ACK_T).await?);
    }
    wait_completed(c, &acks, ACK_T).await?;
    Ok(acks)
}

/// Snapshot and trim every partition on every running node (both roles run the replica
/// services). Then every partition the late node replicates must have a leader whose
/// measured `log_start` is above `above[p]`.
async fn trim_above(
    c: &InProcCluster,
    parts: &[PartitionAssignment],
    above: impl Fn(u16) -> Offset,
) -> ScenarioResult {
    for n in c.node_ids() {
        let Some(h) = c.handle(n) else { continue };
        for p in 0..c.spec().partitions {
            let pid = PartitionId(p);
            if let Err(e) = h.node().snapshot_now(pid).await {
                return Err(fail(
                    "bootstrap",
                    format!("node {n} partition {p}: snapshot: {e}"),
                ));
            }
            if let Err(e) = h.node().retention_pass(pid).await {
                return Err(fail(
                    "bootstrap",
                    format!("node {n} partition {p}: retention pass: {e}"),
                ));
            }
        }
    }
    for a in parts {
        let p = a.partition;
        // The current leader (12-16: `parts` may predate the late node's kill, and a
        // partition it led has failed over since).
        let leader = wait_assignment(c, p, "led by a running node", CHANGE_T, |x| {
            x.leader != 0 && x.leader != LATE
        })
        .await?
        .leader;
        let start = log_start(c, leader, p)?;
        if start <= above(p) {
            return Err(fail(
                "bootstrap",
                format!(
                    "partition {p}: leader {leader} log starts at {} after snapshot + retention, not above {}: no snapshot install would be needed",
                    start.0,
                    above(p).0
                ),
            ));
        }
    }
    Ok(())
}

/// After the late node restarted: it rejoins the ISR of every partition it replicates, its
/// log is anchored above `held[p]` (it installed instead of fetching what the leader no
/// longer has), and after more acknowledged appends every ISR member holds identical state.
async fn rejoined_by_install(
    c: &InProcCluster,
    parts: &[PartitionAssignment],
    held: impl Fn(u16) -> Offset,
    acks: &mut Vec<Ack>,
) -> ScenarioResult {
    for a in parts {
        let p = a.partition;
        wait_assignment(c, p, "rejoined by the bootstrapped node", CATCHUP_T, |x| {
            x.isr.contains(&LATE)
        })
        .await?;
        acks.extend(append_acked(c, p, 2, ACK_T).await?);
    }
    wait_completed(c, acks, ACK_T).await?;
    for a in parts {
        let p = a.partition;
        let Some((_, _, members)) = settle(c, p).await? else {
            return Err(fail("availability", format!("partition {p} is offline")));
        };
        let start = log_start(c, LATE, p)?;
        if start <= held(p) {
            return Err(fail(
                "bootstrap",
                format!(
                    "partition {p}: node {LATE} log starts at {}, not above the {} it held: it did not install the leader's snapshot",
                    start.0,
                    held(p).0
                ),
            ));
        }
        wait_equal_state(c, p, &members, Duration::from_secs(5)).await?;
    }
    Ok(())
}

/// Node 3 comes back with no partition data after every leader trimmed below its snapshot.
fn empty_node_bootstraps_from_snapshot(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let mut acks = load(c, 6).await?;
        c.kill(LATE).await;
        wipe_partition_data(&c.node_disk(LATE).root);
        acks.extend(load(c, 4).await?);
        let parts = late_partitions(c);
        trim_above(c, &parts, |_| Offset::ZERO).await?;
        restart_or_panic(c.restart(LATE).await, LATE);
        rejoined_by_install(c, &parts, |_| Offset::ZERO, &mut acks).await?;
        final_checks_with(c, false, &acks).await
    })
}

/// Node 3 stops with a log prefix; the leaders move their `log_start` past its end; on
/// restart it must reinstall rather than fetch.
fn node_behind_log_start_reinstalls(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let mut acks = load(c, 4).await?;
        let parts = late_partitions(c);
        let mut held = std::collections::BTreeMap::new();
        for a in &parts {
            let p = a.partition;
            let Some((_, hw, _)) = settle(c, p).await? else {
                return Err(fail("availability", format!("partition {p} is offline")));
            };
            held.insert(p, hw);
        }
        c.kill(LATE).await;
        for a in &parts {
            let end = scan(c, LATE, a.partition)?
                .durable_end()
                .unwrap_or(Offset::ZERO);
            held.insert(a.partition, end.max(held[&a.partition]));
        }
        acks.extend(load(c, 6).await?);
        let held_at = |p: u16| held.get(&p).copied().unwrap_or(Offset::ZERO);
        trim_above(c, &parts, held_at).await?;
        restart_or_panic(c.restart(LATE).await, LATE);
        rejoined_by_install(c, &parts, held_at, &mut acks).await?;
        final_checks_with(c, false, &acks).await
    })
}

/// The partition (and its leader) whose snapshot stream to node 3 is being staged: an
/// `.incoming-*` file with bytes in node 3's snapshot dir.
fn staging(c: &InProcCluster, parts: &[PartitionAssignment]) -> Option<(u16, u64)> {
    let root = c.node_disk(LATE).root;
    parts.iter().find_map(|a| {
        let dir = snapshot_dir(&root, PartitionId(a.partition));
        let staged = std::fs::read_dir(&dir).ok()?.flatten().any(|e| {
            e.file_name().to_string_lossy().starts_with(".incoming-")
                && e.metadata().is_ok_and(|m| m.len() > 0)
        });
        staged.then_some((a.partition, a.leader))
    })
}

/// Like the empty-node case, but the links between node 3 and the leader it is installing
/// from are cut while the snapshot is being staged; the install resumes and node 3 still
/// ends identical.
fn install_stream_cut_resumes(c: &mut InProcCluster) -> BoxFuture<'_, ScenarioResult> {
    Box::pin(async move {
        deploy(c).await?;
        let mut acks = load(c, 20).await?;
        c.kill(LATE).await;
        wipe_partition_data(&c.node_disk(LATE).root);
        acks.extend(load(c, 10).await?);
        let parts = late_partitions(c);
        trim_above(c, &parts, |_| Offset::ZERO).await?;
        restart_or_panic(c.restart(LATE).await, LATE);
        // clock-exempt: test deadline
        let end = Instant::now() + STAGE_T;
        let mut cut = None;
        while Instant::now() < end {
            if let Some(found) = staging(c, &parts) {
                cut = Some(found);
                break;
            }
            // clock-exempt: tight poll for the staging file
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let Some((p, from)) = cut else {
            let caught_up = poll(Duration::from_millis(1), || {
                parts
                    .iter()
                    .all(|a| log_start(c, LATE, a.partition).is_ok_and(|s| s > Offset::ZERO))
                    .then_some(())
            })
            .await;
            if caught_up.is_some() {
                panic!(
                    "harness: every install finished before a staged snapshot stream was observed; the cut could not land mid-stream"
                );
            }
            return Err(fail(
                "bootstrap",
                format!("node {LATE} never started staging a snapshot within {STAGE_T:?}"),
            ));
        };
        c.partition(LATE, from);
        pause(LAG_MAX / 4).await;
        c.heal(LATE);
        tracing::info!(partition = p, leader = from, "cut node 3's snapshot stream");
        rejoined_by_install(c, &parts, |_| Offset::ZERO, &mut acks).await?;
        wait_assignment(c, p, "led and replicated after the cut", CHANGE_T, |x| {
            x.leader != 0 && x.isr.contains(&LATE)
        })
        .await?;
        final_checks_with(c, false, &acks).await
    })
}
