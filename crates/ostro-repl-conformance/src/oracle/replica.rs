//! Oracle `replica` (12-03): below the high watermark, every replica stores byte-identical
//! OSTR envelopes (replicas append the leader's stored bytes and never re-encode, F1).
//!
//! Measured from offline verified scans ([`super::disk::scan_partition`]). A replica whose
//! verified log ends (or stops on a bad record) below HW is reported as missing offsets.

use std::collections::BTreeMap;

use ostro_types::{Offset, PartitionId};

use super::Report;
use super::disk::{LogScan, scan_partition};
use crate::harness::DataDirView;

/// One node's record starting at an offset: `(node, end, envelope)`.
type Held<'a> = (u64, u64, &'a [u8]);

/// Scan partition `p` on every view and compare envelopes below `hw`.
pub fn identical_below_hw(views: &[DataDirView], p: PartitionId, hw: Offset) -> Report {
    let mut scans = Vec::new();
    let mut report = Report::new("replica");
    for v in views {
        match scan_partition(v, p) {
            Ok(s) => scans.push(s),
            Err(e) => report.violation(format!(
                "node {} partition {}: scan failed: {e}",
                v.node, p.0
            )),
        }
    }
    report
        .violations
        .extend(compare_below_hw(&scans, hw).violations);
    report
}

/// Compare already-scanned logs (all of one partition) below `hw`. The first scan is the
/// reference only for naming; every node is compared with every other through the record
/// starting at each offset.
pub fn compare_below_hw(scans: &[LogScan], hw: Offset) -> Report {
    let mut report = Report::new("replica");
    let mut at: BTreeMap<u64, Vec<Held<'_>>> = BTreeMap::new();
    // A log that starts above 0 (snapshot install, anchoring) owes nothing below its start:
    // presence is compared from the highest log start on.
    let floor = scans
        .iter()
        .filter_map(|s| s.records.first().map(|r| r.base.0))
        .max()
        .unwrap_or(0);
    for s in scans {
        let covered = s
            .records
            .iter()
            .filter(|r| r.base < hw)
            .last()
            .map(|r| r.end);
        if covered.is_none_or(|end| end < hw) && hw > Offset::ZERO {
            let reach = covered
                .or(s.records.first().map(|r| r.base))
                .unwrap_or(Offset::ZERO);
            report.violation(format!(
                "node {} partition {}: verified log ends at {} below HW {}{}",
                s.node,
                s.partition.0,
                reach.0,
                hw.0,
                s.stopped
                    .as_ref()
                    .map(|w| format!(" (scan stopped: {w})"))
                    .unwrap_or_default()
            ));
        }
        for r in s.records.iter().filter(|r| r.base < hw) {
            at.entry(r.base.0)
                .or_default()
                .push((s.node, r.end.0, &r.envelope[..]));
        }
    }
    for (offset, copies) in at {
        let Some(&(n0, end0, env0)) = copies.first() else {
            continue;
        };
        let p = scans.first().map_or(0, |s| s.partition.0);
        for &(n, end, env) in &copies[1..] {
            if end != end0 {
                report.violation(format!(
                    "partition {p} offset {offset}: node {n0} record ends at {end0}, node {n} at {end}"
                ));
            } else if env != env0 {
                let byte = env0
                    .iter()
                    .zip(env)
                    .position(|(a, b)| a != b)
                    .unwrap_or(env0.len().min(env.len()));
                report.violation(format!(
                    "partition {p} offset {offset}: envelope differs between node {n0} and node {n} (first differing byte {byte})"
                ));
            }
        }
        if copies.len() < scans.len() && offset >= floor {
            let have: Vec<u64> = copies.iter().map(|c| c.0).collect();
            report.violation(format!(
                "partition {p} offset {offset}: record held only by nodes {have:?}"
            ));
        }
    }
    report
}
