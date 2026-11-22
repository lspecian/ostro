//! Oracle `election` (12-03, B1/B4): leadership changes stay inside the published ISR.
//!
//! The committed metadata log is replayed independently over a fresh `MetaState` (or the
//! snapshot state the log continues from), so every check sees the ISR that was published
//! **when the entry was proposed**. Flagged, whether apply accepted or rejected the entry
//! (what was proposed matters, not only what took effect):
//!
//! - (a) an `ElectLeader` whose leader is not in the then-published ISR (leader 0 = offline
//!   excepted), whose new ISR is not a subset of the then-published ISR, or (leader != 0)
//!   whose new ISR is empty; also an `ElectLeader` for a partition with no record;
//! - (b) any `Put(Partition)` other than the one bootstrap placement of a partition (epoch
//!   0 → 1);
//! - (c) any `SetPartitionLeader` after cluster bootstrap (the first bootstrap placement or
//!   `ElectLeader` in the log). An RF=1 log that never bootstraps a replicated cluster keeps
//!   using `SetPartitionLeader` and is not flagged.

use std::collections::BTreeSet;

use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaState};

use super::Report;
use crate::harness::MetaLogEntry;

fn subset(a: &[u64], b: &[u64]) -> bool {
    a.iter().all(|x| b.contains(x))
}

/// [`isr_only_from`] over a log that starts at index 1 (no snapshot).
pub fn isr_only(meta_log: &[MetaLogEntry]) -> Report {
    isr_only_from(MetaState::default(), meta_log)
}

/// Replay `meta_log` over `initial` (the snapshot state the log continues from) and flag
/// every violation (module docs).
pub fn isr_only_from(initial: MetaState, meta_log: &[MetaLogEntry]) -> Report {
    let mut report = Report::new("election");
    let mut state = initial;
    let mut placed: BTreeSet<u16> = BTreeSet::new();
    let mut bootstrapped = false;
    for e in meta_log {
        let at = format!("meta index {}", e.index);
        match &e.command {
            MetaCommand::ElectLeader {
                partition,
                epoch,
                leader,
                isr,
                ..
            } => {
                match state.partition(*partition).map(|v| v.value.clone()) {
                    None => report.violation(format!(
                        "{at}: ElectLeader for partition {partition} which has no record ({:?})",
                        e.applied_result
                    )),
                    Some(cur) => {
                        let verdict = format!("{:?}", e.applied_result);
                        if *leader != 0 && !cur.isr.contains(leader) {
                            report.violation(format!(
                                "{at}: ElectLeader partition {partition} epoch {epoch} elects node {leader} outside the published ISR {:?} (apply: {verdict})",
                                cur.isr
                            ));
                        }
                        if !subset(isr, &cur.isr) {
                            report.violation(format!(
                                "{at}: ElectLeader partition {partition} epoch {epoch} sets ISR {isr:?}, not a subset of the published ISR {:?} (apply: {verdict})",
                                cur.isr
                            ));
                        }
                        if *leader != 0 && isr.is_empty() {
                            report.violation(format!(
                                "{at}: ElectLeader partition {partition} epoch {epoch} elects node {leader} with an empty ISR (apply: {verdict})"
                            ));
                        }
                    }
                }
                bootstrapped = true;
            }
            MetaCommand::Put {
                key: MetaKey::Partition(p),
                record,
                ..
            } => {
                let cur_epoch = state.partition(*p).map(|v| v.value.epoch);
                let new_epoch = match record {
                    MetaRecord::Partition(a) => Some(a.epoch),
                    _ => None,
                };
                let bootstrap =
                    !placed.contains(p) && cur_epoch.unwrap_or(0) == 0 && new_epoch == Some(1);
                if bootstrap {
                    placed.insert(*p);
                    bootstrapped = true;
                } else {
                    report.violation(format!(
                        "{at}: Put(Partition {p}) epoch {cur_epoch:?} -> {new_epoch:?} is not the bootstrap placement (apply: {:?})",
                        e.applied_result
                    ));
                }
            }
            MetaCommand::SetPartitionLeader {
                partition, epoch, ..
            } if bootstrapped => {
                report.violation(format!(
                    "{at}: SetPartitionLeader partition {partition} epoch {epoch} after cluster bootstrap (apply: {:?})",
                    e.applied_result
                ));
            }
            _ => {}
        }
        // The verdict is recomputed here, independently of the recorded one.
        state.apply(e.index, e.command.clone());
    }
    report
}
