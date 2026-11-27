//! Checker `duplicate` (14-09): no journal entry exists twice, and replicas agree.
//!
//! Three parts, each in the same verdict:
//!
//! - **Replica identity** ([`check_replicas`]): below the settled HW every replica stores
//!   byte-identical records (the Phase 12 `replica` oracle's rule, F1: replicas append the
//!   leader's stored bytes and never re-encode), compared by `(end, length, FNV-64 digest)`
//!   per record base offset. A live ISR member must hold everything below HW from the
//!   highest log start on; a replica outside the ISR is compared where it holds records.
//! - **Log journal** ([`check_log_journals`]): over the `EventsAppended` commands the offline
//!   re-apply accepted, no `(run, seq)` is journaled twice. (Rejected retries of the same
//!   append stay in the log as no-ops and are not duplicates.)
//! - **API journal** ([`check_api_journals`]): every observed run's journal
//!   (`GET /v1/runs/{id}/history`) has contiguous seqs from 0, exactly one `run_started`
//!   (at seq 0), at most one terminal event, each step completed at most once and each signal
//!   request id journaled at most once.

use std::collections::{BTreeMap, BTreeSet};

use super::lost::{JournalEvent, RunObservation, TERMINAL_EVENTS};
use super::{LogExport, Op, PartitionLogs, Verdict};

/// All three parts (module docs).
pub fn check_duplicates(logs: &LogExport, runs: &BTreeMap<String, RunObservation>) -> Verdict {
    let mut v = Verdict::new("duplicate");
    v.absorb(check_replicas(logs));
    v.absorb(check_log_journals(logs));
    v.absorb(check_api_journals(runs));
    v
}

/// Replica identity below HW for every partition.
pub fn check_replicas(logs: &LogExport) -> Verdict {
    let mut v = Verdict::new("duplicate");
    for p in &logs.partitions {
        compare_partition(p, &mut v);
    }
    v
}

fn compare_partition(p: &PartitionLogs, v: &mut Verdict) {
    let hw = p.hw;
    let floor = p
        .replicas
        .iter()
        .filter_map(|r| r.start())
        .max()
        .unwrap_or(0);
    // base offset -> (node, end, len, digest)
    let mut at: BTreeMap<u64, Vec<(u64, u64, u64, u64)>> = BTreeMap::new();
    for r in &p.replicas {
        let covered = r.records.iter().rfind(|x| x.base < hw).map(|x| x.end);
        if r.in_isr && hw > 0 && covered.is_none_or(|end| end < hw) {
            v.violation(format!(
                "partition {} node {}: verified log ends at {} below HW {hw}{}",
                p.partition,
                r.node,
                covered.or(r.start()).unwrap_or(0),
                r.stopped
                    .as_ref()
                    .map(|w| format!(" (scan stopped: {w})"))
                    .unwrap_or_default()
            ));
        }
        for rec in r.records.iter().filter(|x| x.base < hw) {
            v.add("records_compared", 1);
            at.entry(rec.base)
                .or_default()
                .push((r.node, rec.end, rec.len, rec.digest));
        }
    }
    let isr: BTreeSet<u64> = p
        .replicas
        .iter()
        .filter(|r| r.in_isr)
        .map(|r| r.node)
        .collect();
    for (offset, copies) in at {
        let Some(&(n0, end0, len0, d0)) = copies.first() else {
            continue;
        };
        for &(n, end, len, d) in &copies[1..] {
            if end != end0 {
                v.violation(format!(
                    "partition {} offset {offset}: node {n0} record ends at {end0}, node {n} at {end} (divergent replicas)",
                    p.partition
                ));
            } else if len != len0 || d != d0 {
                v.violation(format!(
                    "partition {} offset {offset}: record differs between node {n0} and node {n} (divergent replicas)",
                    p.partition
                ));
            }
        }
        let have: BTreeSet<u64> = copies.iter().map(|c| c.0).collect();
        if offset >= floor && !isr.is_subset(&have) {
            v.violation(format!(
                "partition {} offset {offset}: record held only by nodes {have:?} (ISR {isr:?})",
                p.partition
            ));
        }
    }
}

/// No `(run, seq)` among the accepted `EventsAppended` of any re-applied replica log.
pub fn check_log_journals(logs: &LogExport) -> Verdict {
    let mut v = Verdict::new("duplicate");
    for p in &logs.partitions {
        let mut reapplied = 0;
        for r in &p.replicas {
            if r.reapply.is_none() {
                continue;
            }
            reapplied += 1;
            let mut seen: BTreeMap<(&str, u64), u64> = BTreeMap::new();
            for e in &r.entries {
                let Op::Events { run_id, seqs, .. } = &e.op else {
                    continue;
                };
                if !r.applied_at(e.offset).is_some_and(|a| a.accepted) {
                    continue;
                }
                for s in seqs {
                    v.add("journal_entries", 1);
                    if let Some(prev) = seen.insert((run_id.as_str(), *s), e.offset) {
                        v.violation(format!(
                            "partition {} node {}: run {run_id} seq {s} journaled twice (offsets {prev} and {})",
                            p.partition, r.node, e.offset
                        ));
                    }
                }
            }
        }
        if reapplied == 0 && !p.replicas.is_empty() {
            v.note(format!(
                "partition {}: no replica log was re-applied; log journal not checked",
                p.partition
            ));
        }
    }
    v
}

/// Journal shape of every observed run (module docs).
pub fn check_api_journals(runs: &BTreeMap<String, RunObservation>) -> Verdict {
    let mut v = Verdict::new("duplicate");
    for (run, obs) in runs {
        let Some(j) = &obs.journal else { continue };
        v.add("journals_checked", 1);
        journal_shape(run, j, &mut v);
    }
    v
}

fn journal_shape(run: &str, j: &[JournalEvent], v: &mut Verdict) {
    for (i, e) in j.iter().enumerate() {
        if e.seq != i as u64 {
            v.violation(format!(
                "run {run}: journal event #{i} has seq {} (contiguous seqs from 0 expected)",
                e.seq
            ));
            break;
        }
    }
    let mut seqs = BTreeSet::new();
    for e in j {
        if !seqs.insert(e.seq) {
            v.violation(format!("run {run}: seq {} journaled twice", e.seq));
        }
    }
    let started: Vec<u64> = j
        .iter()
        .filter(|e| e.kind == "run_started")
        .map(|e| e.seq)
        .collect();
    if started != [0] {
        v.violation(format!(
            "run {run}: run_started at seqs {started:?} (exactly one, at seq 0, expected)"
        ));
    }
    let terminals: Vec<String> = j
        .iter()
        .filter(|e| TERMINAL_EVENTS.contains(&e.kind.as_str()))
        .map(|e| format!("{}@{}", e.kind, e.seq))
        .collect();
    if terminals.len() > 1 {
        v.violation(format!(
            "run {run}: terminal events {terminals:?} (duplicate completion)"
        ));
    }
    let mut steps: BTreeMap<&str, u64> = BTreeMap::new();
    let mut rids: BTreeMap<&str, u64> = BTreeMap::new();
    for e in j {
        if e.kind == "step_completed"
            && let Some(name) = e.details["name"].as_str()
            && let Some(prev) = steps.insert(name, e.seq)
        {
            v.violation(format!(
                "run {run}: step {name:?} completed at seqs {prev} and {}",
                e.seq
            ));
        }
        if e.kind == "signal_payload"
            && let Some(rid) = e.details["payload"]["request_id"].as_str()
            && let Some(prev) = rids.insert(rid, e.seq)
        {
            v.violation(format!(
                "run {run}: signal request id {rid} journaled at seqs {prev} and {}",
                e.seq
            ));
        }
    }
}
