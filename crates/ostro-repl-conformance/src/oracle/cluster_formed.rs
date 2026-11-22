//! Oracle `cluster_formed` (12-03, D-P12-19): the measured precondition of every scenario.
//!
//! A cluster is formed when, on every node:
//! - the metadata Raft `ConfState` voters (read offline from the node's meta log store)
//!   equal the configured node set `1..=spec.nodes`;
//! - the node's committed meta state has `spec.partitions` partitions, each with
//!   `spec.rf` replicas, an epoch >= 1 and a leader that is one of its replicas;
//! - that leader's verified WAL for the partition holds the `LeaderAnnounced { epoch,
//!   leader }` that opens the epoch (measured on the leader's own data dir).
//!
//! [`check`] polls until all of that holds or the formation deadline (default
//! [`FORMATION_DEADLINE`]) passes, and then reports what is still missing. On the
//! unintegrated build (12-03 .. 12-13) a multi-node spec fails here with "meta voters [n] !=
//! configured [1, 2, 3]": the uniform expected red reason.

use std::collections::BTreeMap;
use std::time::Duration;

use ostro_meta::PartitionAssignment;
use ostro_types::command::decode_command;
use ostro_types::{Cmd, PartitionId};

use super::Report;
use super::disk::scan_partition;
use crate::harness::inproc::{InProcCluster, read_meta_log};
use crate::harness::{ClusterHarness, ClusterSpec, DataDirView};

/// Default formation deadline.
pub const FORMATION_DEADLINE: Duration = Duration::from_secs(30);

/// What one node measured.
#[derive(Clone, Debug)]
pub struct NodeMeasure {
    /// Node id.
    pub node: u64,
    /// Meta `ConfState` voters.
    pub voters: Vec<u64>,
    /// Partition records of the node's committed meta state.
    pub partitions: Vec<PartitionAssignment>,
}

/// Measure every node of `views` (meta log read offline).
pub fn measure(views: &[DataDirView]) -> Result<Vec<NodeMeasure>, String> {
    views
        .iter()
        .map(|v| {
            let dump = read_meta_log(&v.root.join("meta"))
                .map_err(|e| format!("node {}: meta log: {e}", v.node))?;
            Ok(NodeMeasure {
                node: v.node,
                voters: dump.voters,
                partitions: dump.state.partitions().map(|a| a.value.clone()).collect(),
            })
        })
        .collect()
}

/// Whether `leader`'s WAL of `p` opens `epoch` with `LeaderAnnounced { epoch, leader }`.
fn announced(view: &DataDirView, p: u16, epoch: u32, leader: u64) -> Result<bool, String> {
    let scan = scan_partition(view, PartitionId(p))?;
    Ok(scan
        .records
        .iter()
        .find(|r| r.epoch.0 == epoch)
        .and_then(|r| r.commands.first())
        .and_then(|c| decode_command(c).ok())
        .is_some_and(|c| {
            matches!(c.cmd, Some(Cmd::LeaderAnnounced(la)) if la.epoch == epoch && la.node_id == leader)
        }))
}

/// Evaluate the formation conditions once over measured inputs.
pub fn evaluate(spec: &ClusterSpec, views: &[DataDirView], measured: &[NodeMeasure]) -> Report {
    let mut report = Report::new("cluster_formed");
    let expected: Vec<u64> = (1..=spec.nodes as u64).collect();
    let by_node: BTreeMap<u64, &DataDirView> = views.iter().map(|v| (v.node, v)).collect();
    for m in measured {
        let mut voters = m.voters.clone();
        voters.sort_unstable();
        if voters != expected {
            report.violation(format!(
                "node {}: meta voters {voters:?} != configured {expected:?}",
                m.node
            ));
        }
        if m.partitions.len() != usize::from(spec.partitions) {
            report.violation(format!(
                "node {}: meta has {} partitions, expected {}",
                m.node,
                m.partitions.len(),
                spec.partitions
            ));
        }
        for a in &m.partitions {
            let at = format!("node {} partition {}", m.node, a.partition);
            if a.replicas.len() != spec.rf {
                report.violation(format!(
                    "{at}: {} replica(s) {:?}, expected rf {}",
                    a.replicas.len(),
                    a.replicas,
                    spec.rf
                ));
            }
            if a.epoch < 1 {
                report.violation(format!("{at}: epoch {} < 1", a.epoch));
                continue;
            }
            if a.leader == 0 || !a.replicas.contains(&a.leader) {
                report.violation(format!(
                    "{at}: leader {} is not one of the replicas {:?}",
                    a.leader, a.replicas
                ));
                continue;
            }
            match by_node.get(&a.leader) {
                None => {
                    report.violation(format!("{at}: leader {} is not a cluster node", a.leader))
                }
                Some(v) => match announced(v, a.partition, a.epoch, a.leader) {
                    Ok(true) => {}
                    Ok(false) => report.violation(format!(
                        "{at}: leader {} has no LeaderAnnounced for epoch {} on disk",
                        a.leader, a.epoch
                    )),
                    Err(e) => report.violation(format!("{at}: leader scan failed: {e}")),
                },
            }
        }
    }
    report
}

/// [`check_within`] with [`FORMATION_DEADLINE`].
pub async fn check(cluster: &InProcCluster, spec: &ClusterSpec) -> Report {
    check_within(cluster, spec, FORMATION_DEADLINE).await
}

/// Poll the formation conditions until they hold or `deadline` passes.
pub async fn check_within(
    cluster: &InProcCluster,
    spec: &ClusterSpec,
    deadline: Duration,
) -> Report {
    let views: Vec<DataDirView> = cluster
        .node_ids()
        .into_iter()
        .map(|n| cluster.node_disk(n))
        .collect();
    // clock-exempt: test deadline
    let end = tokio::time::Instant::now() + deadline;
    loop {
        let report = match measure(&views) {
            Ok(m) => evaluate(spec, &views, &m),
            Err(e) => {
                let mut r = Report::new("cluster_formed");
                r.violation(e);
                r
            }
        };
        if report.is_ok() || tokio::time::Instant::now() >= end {
            return report;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
