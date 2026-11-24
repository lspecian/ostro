//! Rolling-upgrade oracles (13-08; scenarios in 13-04 / 13-14 / 13-19, fixes in 13-18).
//!
//! | oracle | inputs | checks |
//! |--------|--------|--------|
//! | [`no_gated_before_finalize`] | committed meta log, partition WAL scans | no gated feature below the finalization index of its level: no `MetaCommand::AlterReplicas` (variant 8) and no record carrying ext tag 100 / 101 / 102 is committed before the `Put(Cluster)` that raised the finalized level (ext 98) to the feature's `introduced_at`; no partition `Cmd::RepinRun` (tag 43) on disk while the meta log never reached that level |
//! | [`no_gated_partition_commands`] | partition WAL scans taken before finalization | no `Cmd::RepinRun` at all |
//! | [`no_reject_continue`] | apply outcome streams | no partition or voter applied `Rejected(Invalid(Empty))` to non-empty command bytes (an older binary skipping a newer command and moving on) |
//!
//! Every gated entry counts whether apply accepted it or not: an older node replays the
//! committed bytes either way. Decoding uses this build's codecs (`ostro_meta`,
//! `ostro_types::command`).

use bytes::Bytes;
use ostro_meta::ext::{
    EXT_REASSIGNMENT, EXT_RETIRED, EXT_WIT_REQUIREMENT, ext_get, get_cluster_version,
};
use ostro_meta::{MetaCommand, MetaRecord, MetaResult, MetaState};
use ostro_partition::apply::{ApplyOutcome, InvalidCode, RejectReason};
use ostro_types::command::{Cmd, decode_command};
use ostro_types::compat::{Feature, LEVEL_MIN};

use super::Report;
use super::disk::LogScan;
use crate::harness::MetaLogEntry;

/// The `ext` bytes of any meta record.
pub fn record_ext(r: &MetaRecord) -> &[u8] {
    match r {
        MetaRecord::Cluster(x) => &x.ext,
        MetaRecord::Member(x) => &x.ext,
        MetaRecord::Partition(x) => &x.ext,
        MetaRecord::Module(x) => &x.ext,
        MetaRecord::Workflow(x) => &x.ext,
        MetaRecord::Active(x) => &x.ext,
        MetaRecord::Queue(x) => &x.ext,
        MetaRecord::Schedule(x) => &x.ext,
        MetaRecord::Token(x) => &x.ext,
        MetaRecord::Tenant(x) => &x.ext,
        MetaRecord::Secret(x) => &x.ext,
        MetaRecord::Grant(x) => &x.ext,
        MetaRecord::Connection(x) => &x.ext,
        MetaRecord::Egress(x) => &x.ext,
        MetaRecord::WorkflowPolicy(x) => &x.ext,
        MetaRecord::QueuePolicy(x) => &x.ext,
        MetaRecord::Webhook(x) => &x.ext,
    }
}

/// The finalized level of a meta state (ext 98 of the cluster record; absent = `LEVEL_MIN`).
pub fn finalized_level(state: &MetaState) -> u32 {
    state
        .cluster()
        .and_then(|c| get_cluster_version(&c.value.ext).ok().flatten())
        .map_or(LEVEL_MIN, |v| v.finalized)
}

/// The finalized level a `Put(Cluster)` writes, if `e` is one that applied `Ok`.
fn finalizes(e: &MetaLogEntry) -> Option<u32> {
    match (&e.command, &e.applied_result) {
        (
            MetaCommand::Put {
                record: MetaRecord::Cluster(c),
                ..
            },
            MetaResult::Ok { .. },
        ) => Some(
            get_cluster_version(&c.ext)
                .ok()
                .flatten()
                .map_or(LEVEL_MIN, |v| v.finalized),
        ),
        _ => None,
    }
}

/// The index of the first committed entry at which the finalized level reached `level`
/// (`Some(0)` when `initial` already does; `None` when it never did).
pub fn finalization_index(initial: u32, meta_log: &[MetaLogEntry], level: u32) -> Option<u64> {
    if initial >= level {
        return Some(0);
    }
    meta_log
        .iter()
        .find(|e| finalizes(e).is_some_and(|l| l >= level))
        .map(|e| e.index)
}

/// The gated features one meta command emits.
fn gated_in(cmd: &MetaCommand) -> Vec<Feature> {
    let mut out = Vec::new();
    match cmd {
        MetaCommand::AlterReplicas { .. } => out.push(Feature::AlterReplicas),
        MetaCommand::Put { record, .. } => {
            let ext = record_ext(record);
            for (tag, f) in [
                (EXT_REASSIGNMENT, Feature::ReassignmentExt),
                (EXT_WIT_REQUIREMENT, Feature::WitRequirementExt),
                (EXT_RETIRED, Feature::RetiredExt),
            ] {
                if ext_get(ext, tag).is_some() {
                    out.push(f);
                }
            }
        }
        _ => {}
    }
    out
}

/// [`no_gated_before_finalize_from`] from `LEVEL_MIN` (a meta log that starts at the
/// cluster's creation, or whose snapshot was taken before any finalization).
pub fn no_gated_before_finalize(meta_log: &[MetaLogEntry], partition_logs: &[LogScan]) -> Report {
    no_gated_before_finalize_from(LEVEL_MIN, meta_log, partition_logs)
}

/// No gated feature below the finalization index of its level (module docs). `initial` is
/// the finalized level the log continues from ([`finalized_level`] of the dump's snapshot
/// state).
pub fn no_gated_before_finalize_from(
    initial: u32,
    meta_log: &[MetaLogEntry],
    partition_logs: &[LogScan],
) -> Report {
    let mut report = Report::new("p13_upgrade");
    for e in meta_log {
        for f in gated_in(&e.command) {
            let level = f.introduced_at();
            match finalization_index(initial, meta_log, level) {
                Some(fin) if e.index > fin => {}
                fin => report.violation(format!(
                    "meta index {}: {} (level {level}) committed before finalization (finalized at {})",
                    e.index,
                    f.name(),
                    fin.map_or_else(|| "never".to_owned(), |i| format!("index {i}"))
                )),
            }
        }
    }
    if finalization_index(initial, meta_log, Feature::RepinRun.introduced_at()).is_none() {
        let r = no_gated_partition_commands(partition_logs);
        for v in r.violations {
            report.violation(format!("{v} (the meta log never finalized its level)"));
        }
    }
    report
}

/// No `Cmd::RepinRun` in any of `scans` (partition logs captured before finalization).
pub fn no_gated_partition_commands(scans: &[LogScan]) -> Report {
    let mut report = Report::new("p13_upgrade");
    for scan in scans {
        for (offset, bytes) in scan.commands() {
            if let Ok(cmd) = decode_command(bytes)
                && matches!(cmd.cmd, Some(Cmd::RepinRun(_)))
            {
                report.violation(format!(
                    "node {} partition {} offset {}: RepinRun (level {})",
                    scan.node,
                    scan.partition.0,
                    offset.0,
                    Feature::RepinRun.introduced_at()
                ));
            }
        }
    }
    report
}

/// One applied command of an outcome stream (a partition replica or a meta voter).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutcomeRecord {
    /// The node that applied it.
    pub node: u64,
    /// The partition (`None` = the metadata group).
    pub partition: Option<u16>,
    /// Log offset / Raft index.
    pub offset: u64,
    /// The command bytes as stored.
    pub command: Bytes,
    /// The apply verdict.
    pub outcome: ApplyOutcome,
}

/// No `Rejected(Invalid(Empty))` for non-empty command bytes (module docs).
pub fn no_reject_continue(outcome_streams: &[OutcomeRecord]) -> Report {
    let mut report = Report::new("p13_upgrade");
    for r in outcome_streams {
        if r.outcome == ApplyOutcome::Rejected(RejectReason::Invalid(InvalidCode::Empty))
            && !r.command.is_empty()
        {
            let group = r
                .partition
                .map_or_else(|| "meta".to_owned(), |p| format!("partition {p}"));
            report.violation(format!(
                "node {} {group} offset {}: Rejected(Invalid(Empty)) for {} command bytes (reject-and-continue)",
                r.node,
                r.offset,
                r.command.len()
            ));
        }
    }
    report
}
