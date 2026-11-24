//! Membership oracles (13-08; scenarios in 13-09, fixes in 13-18).
//!
//! | oracle | inputs | checks |
//! |--------|--------|--------|
//! | [`removed_never_return`] | offline meta logs ([`MetaLogDump`] per node), status samples | a removed id never reappears: not in the persisted `ConfState` voters / learners, not in any ISR or as a leader after its tombstone, never re-written as a non-removed member, never back in a later status (voters, learners, outgoing, non-removed state) |
//! | [`joint_left_within`] | status samples | `voters_outgoing` is empty again within the limit (5 s in the scenarios) of entering a joint configuration |
//! | [`voters_odd`] | status samples | the voter count is odd in every non-joint sample |
//!
//! The removed set comes from the committed meta log (a `Put(Member)` whose ext tag 97 says
//! `MEMBER_REMOVED` and that applied `Ok`, or a tombstone already in the snapshot) and from
//! any status sample that showed the member `removed`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use ostro_api::phase13::ClusterStatus;
use ostro_meta::ext::{MEMBER_REMOVED, get_member_state};
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, MetaState};

use super::Report;
use crate::harness::HarnessError;
use crate::harness::inproc::MetaLogDump;

/// One `GET /v1/cluster/status` answer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatusSample {
    /// When it was taken (since the scenario started).
    pub at: Duration,
    /// The node that answered.
    pub node: u64,
    /// The decoded status.
    pub status: ClusterStatus,
}

impl StatusSample {
    /// Decode a status document taken at `at` from `node`.
    pub fn from_json(at: Duration, node: u64, v: &serde_json::Value) -> Result<Self, HarnessError> {
        let status = serde_json::from_value(v.clone())
            .map_err(|e| HarnessError::Other(format!("cluster status {v}: {e}")))?;
        Ok(Self { at, node, status })
    }
}

fn removed_in(state: &MetaState) -> BTreeSet<u64> {
    state
        .members()
        .filter(
            |m| matches!(get_member_state(&m.value.ext), Ok(Some(s)) if s.state == MEMBER_REMOVED),
        )
        .map(|m| m.value.node_id)
        .collect()
}

/// The ISR / leader / member writes of one meta command that would bring `id` back.
fn reappears(cmd: &MetaCommand, id: u64) -> Option<String> {
    let in_isr = |isr: &[u64], leader: u64| isr.contains(&id) || leader == id;
    match cmd {
        MetaCommand::SetPartitionLeader {
            partition,
            leader,
            isr,
            ..
        }
        | MetaCommand::ElectLeader {
            partition,
            leader,
            isr,
            ..
        } if in_isr(isr, *leader) => {
            Some(format!("partition {partition} leader/ISR {leader}/{isr:?}"))
        }
        MetaCommand::AlterIsr { partition, isr, .. } if isr.contains(&id) => {
            Some(format!("partition {partition} ISR {isr:?}"))
        }
        MetaCommand::AlterReplicas { partition, isr, .. } if isr.contains(&id) => {
            Some(format!("partition {partition} AlterReplicas ISR {isr:?}"))
        }
        MetaCommand::Put {
            record: MetaRecord::Partition(p),
            ..
        } if in_isr(&p.isr, p.leader) => Some(format!(
            "partition {} record leader/ISR {}/{:?}",
            p.partition, p.leader, p.isr
        )),
        MetaCommand::Put {
            record: MetaRecord::Member(m),
            ..
        } if m.node_id == id
            && !matches!(get_member_state(&m.ext), Ok(Some(s)) if s.state == MEMBER_REMOVED) =>
        {
            Some("member record re-written as not removed".to_owned())
        }
        _ => None,
    }
}

/// A removed id never reappears (see the module docs). `meta_logs` maps node → its
/// offline-decoded meta log; `statuses` are in the order they were taken.
pub fn removed_never_return(
    meta_logs: &BTreeMap<u64, MetaLogDump>,
    statuses: &[StatusSample],
) -> Report {
    let mut report = Report::new("p13_membership");
    for (node, dump) in meta_logs {
        // id -> index of its tombstone (0 = already in the snapshot).
        let mut removed: BTreeMap<u64, u64> = removed_in(&dump.snapshot_state)
            .into_iter()
            .map(|id| (id, 0))
            .collect();
        for e in &dump.entries {
            for (id, at) in &removed {
                if e.index > *at
                    && matches!(e.applied_result, MetaResult::Ok { .. })
                    && let Some(why) = reappears(&e.command, *id)
                {
                    report.violation(format!(
                        "node {node} meta index {}: removed node {id} (tombstone at {at}) back: {why}",
                        e.index
                    ));
                }
            }
            if let MetaCommand::Put {
                record: MetaRecord::Member(m),
                ..
            } = &e.command
                && matches!(e.applied_result, MetaResult::Ok { .. })
                && matches!(get_member_state(&m.ext), Ok(Some(s)) if s.state == MEMBER_REMOVED)
            {
                removed.entry(m.node_id).or_insert(e.index);
            }
        }
        for id in removed.keys() {
            if dump.voters.contains(id) || dump.learners.contains(id) {
                report.violation(format!(
                    "node {node}: removed node {id} in the persisted ConfState (voters {:?}, learners {:?})",
                    dump.voters, dump.learners
                ));
            }
            for p in dump.state.partitions() {
                if p.value.isr.contains(id) || p.value.leader == *id {
                    report.violation(format!(
                        "node {node}: removed node {id} in partition {} leader/ISR {}/{:?}",
                        p.value.partition, p.value.leader, p.value.isr
                    ));
                }
            }
        }
    }
    // Status series: once a sample showed the member removed, no later sample may list it
    // as a voter / learner / outgoing voter or in another state. (Samples cannot be ordered
    // against meta indexes; the meta-log checks above cover the tombstoned ids.)
    let mut seen_removed: BTreeMap<u64, Duration> = BTreeMap::new();
    for s in statuses {
        let st = &s.status;
        for (id, since) in &seen_removed {
            let back = st.voters.contains(id)
                || st.learners.contains(id)
                || st.voters_outgoing.contains(id)
                || st
                    .members
                    .iter()
                    .any(|m| m.node_id == *id && m.state != "removed");
            if back {
                report.violation(format!(
                    "status at {:?} (node {}): node {id} (removed by {since:?}) is back (voters {:?}, learners {:?}, outgoing {:?})",
                    s.at, s.node, st.voters, st.learners, st.voters_outgoing
                ));
            }
        }
        for m in &st.members {
            if m.state == "removed" {
                seen_removed.entry(m.node_id).or_insert(s.at);
            }
        }
    }
    report
}

/// `voters_outgoing` is empty again within `limit` of entering a joint configuration.
pub fn joint_left_within(statuses: &[StatusSample], limit: Duration) -> Report {
    let mut report = Report::new("p13_membership");
    // Per answering node: the start of the current joint streak and whether it was reported.
    let mut streak: BTreeMap<u64, (Duration, bool)> = BTreeMap::new();
    for s in statuses {
        if s.status.voters_outgoing.is_empty() {
            streak.remove(&s.node);
            continue;
        }
        let (start, reported) = streak.entry(s.node).or_insert((s.at, false));
        if !*reported && s.at.saturating_sub(*start) > limit {
            *reported = true;
            report.violation(format!(
                "node {}: joint configuration (outgoing {:?}) since {:?}, still joint at {:?} (limit {limit:?})",
                s.node, s.status.voters_outgoing, start, s.at
            ));
        }
    }
    report
}

/// The voter count is odd in every non-joint sample.
pub fn voters_odd(statuses: &[StatusSample]) -> Report {
    let mut report = Report::new("p13_membership");
    for s in statuses {
        let st = &s.status;
        if st.voters_outgoing.is_empty() && st.voters.len() % 2 == 0 {
            report.violation(format!(
                "status at {:?} (node {}): even voter set {:?}",
                s.at, s.node, st.voters
            ));
        }
    }
    report
}
