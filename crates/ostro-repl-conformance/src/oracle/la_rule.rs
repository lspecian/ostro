//! Oracle `la_rule` (12-03): every epoch in a partition's log starts with
//! `LeaderAnnounced { epoch: e, node_id }`, and `node_id` is the leader the committed
//! metadata log gave epoch `e` of that partition.
//!
//! Measured from an offline verified WAL scan and the committed meta log (`InitCluster`
//! gives epoch 0, accepted `SetPartitionLeader` / `ElectLeader` give later epochs; an
//! election to leader 0, i.e. offline, names nobody). A log that starts above offset 0 may
//! begin mid-epoch; its first epoch is checked only when it does start with an announcement.

use std::collections::BTreeMap;

use ostro_meta::{MetaCommand, MetaRecord, MetaResult};
use ostro_types::command::decode_command;
use ostro_types::{Cmd, PartitionId};

use super::Report;
use super::disk::{LogScan, scan_partition};
use crate::harness::{DataDirView, MetaLogEntry};

/// The leader the committed meta log assigned to each epoch of partition `p`.
pub fn meta_leaders(p: PartitionId, meta_log: &[MetaLogEntry]) -> BTreeMap<u32, u64> {
    let mut out = BTreeMap::new();
    for e in meta_log {
        if !matches!(e.applied_result, MetaResult::Ok { .. }) {
            continue;
        }
        match &e.command {
            MetaCommand::InitCluster {
                node_id,
                partitions,
                ..
            } if p.0 < *partitions => {
                out.entry(0).or_insert(*node_id);
            }
            MetaCommand::SetPartitionLeader {
                partition,
                epoch,
                leader,
                ..
            }
            | MetaCommand::ElectLeader {
                partition,
                epoch,
                leader,
                ..
            } if *partition == p.0 => {
                out.insert(*epoch, *leader);
            }
            // 12-14: the bootstrap placement (epoch 0 -> 1) is a `Put(Partition)` CAS.
            MetaCommand::Put {
                record: MetaRecord::Partition(a),
                ..
            } if a.partition == p.0 && a.epoch > 0 => {
                out.insert(a.epoch, a.leader);
            }
            _ => {}
        }
    }
    out
}

/// Scan `view`'s partition `p` and check the LA rule against `meta_log`.
pub fn check(view: &DataDirView, p: PartitionId, meta_log: &[MetaLogEntry]) -> Report {
    match scan_partition(view, p) {
        Ok(scan) => check_scan(&scan, meta_log),
        Err(e) => {
            let mut r = Report::new("la_rule");
            r.violation(format!(
                "node {} partition {}: scan failed: {e}",
                view.node, p.0
            ));
            r
        }
    }
}

/// [`check`] over an existing scan.
pub fn check_scan(scan: &LogScan, meta_log: &[MetaLogEntry]) -> Report {
    let mut report = Report::new("la_rule");
    let p = scan.partition;
    let leaders = meta_leaders(p, meta_log);
    let mut last_epoch = None;
    for r in &scan.records {
        if last_epoch == Some(r.epoch) {
            continue;
        }
        // A log starting above 0 (snapshot install) may begin mid-epoch: its first epoch's
        // announcement lies below the log start and cannot be measured here.
        let mid_epoch_start = last_epoch.is_none() && r.base.0 > 0;
        last_epoch = Some(r.epoch);
        let e = r.epoch.0;
        let at = format!(
            "node {} partition {} epoch {e} (offset {})",
            scan.node, p.0, r.base.0
        );
        let first = r.commands.first().map(|c| decode_command(c));
        let la = match first {
            Some(Ok(cmd)) => match cmd.cmd {
                Some(Cmd::LeaderAnnounced(la)) => la,
                _ if mid_epoch_start => continue,
                other => {
                    let kind = other.map_or("empty".to_owned(), |c| format!("{c:?}"));
                    let kind: String = kind.chars().take(40).collect();
                    report.violation(format!(
                        "{at}: first command is not LeaderAnnounced ({kind}…)"
                    ));
                    continue;
                }
            },
            Some(Err(err)) => {
                report.violation(format!("{at}: first command undecodable: {err}"));
                continue;
            }
            None => continue,
        };
        if la.epoch != e {
            report.violation(format!("{at}: LeaderAnnounced carries epoch {}", la.epoch));
        }
        match leaders.get(&e) {
            Some(&leader) if leader == la.node_id => {}
            Some(&leader) => report.violation(format!(
                "{at}: LeaderAnnounced by node {} but meta's leader for epoch {e} is node {leader}",
                la.node_id
            )),
            None => report.violation(format!(
                "{at}: LeaderAnnounced by node {} but the committed meta log has no leader for epoch {e}",
                la.node_id
            )),
        }
    }
    report
}
