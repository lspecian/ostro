//! Oracle `disk` (12-03): acknowledged ⇒ on disk on at least `min_insync` nodes.
//!
//! [`scan_partition`] is the measured read every log oracle shares: an offline, read-only,
//! verified scan of one node's partition WAL (`<data>/wal/p/<pid>/wal_<pid>_<seg>.log`) with
//! the same per-record checks as `ostro_log::recovery` (frame CRC, envelope CRC32C,
//! partition, contiguous offsets, non-decreasing epochs, via `ostro_log::verify_record`). It
//! never opens the log, never cuts a tail and never asks the process: the scan stops at the
//! first record that does not verify, and that point is the node's measured durable end. On
//! a running node a record still being written stops the scan the same way (best effort).

use std::collections::BTreeSet;
use std::path::Path;

use bytes::Bytes;
use ostro_log::ostro_wal::WalRecord;
use ostro_log::ostro_wal::truncate::{RECORD_HEADER_LEN, declared_record_size};
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_log::verify_record;
use ostro_types::command::decode_command;
use ostro_types::{Cmd, Epoch, Offset, PartitionId, RunId};

use super::Report;
use crate::harness::DataDirView;
use crate::harness::inproc::Ack;

/// One verified WAL record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScannedRecord {
    /// First offset.
    pub base: Offset,
    /// Offset just past the last command.
    pub end: Offset,
    /// Leader epoch that wrote it.
    pub epoch: Epoch,
    /// The stored OSTR envelope, byte for byte.
    pub envelope: Bytes,
    /// Its commands, in offset order.
    pub commands: Vec<Bytes>,
}

/// One node's verified partition log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogScan {
    /// Node id.
    pub node: u64,
    /// Partition.
    pub partition: PartitionId,
    /// Verified records, in offset order.
    pub records: Vec<ScannedRecord>,
    /// Why the scan stopped before the end of the files (`None` = everything verified).
    pub stopped: Option<String>,
}

impl LogScan {
    /// End of the last verified record (the measured durable end); `None` when empty.
    pub fn durable_end(&self) -> Option<Offset> {
        self.records.last().map(|r| r.end)
    }

    /// Every `(offset, command)` pair.
    pub fn commands(&self) -> impl Iterator<Item = (Offset, &Bytes)> {
        self.records.iter().flat_map(|r| {
            r.commands
                .iter()
                .enumerate()
                .map(move |(i, c)| (r.base + i as u64, c))
        })
    }

    /// Base offset of the first verified record (`None` when empty). A value above 0 means
    /// retention has trimmed this copy below that offset.
    pub fn log_start(&self) -> Option<Offset> {
        self.records.first().map(|r| r.base)
    }

    /// Whether retention has trimmed this copy (its first verified record starts above 0).
    pub fn is_trimmed(&self) -> bool {
        self.log_start().is_some_and(|o| o.0 > 0)
    }

    /// Whether the log holds a `StartRun` for `run`.
    pub fn has_start_run(&self, run: &RunId) -> bool {
        self.commands().any(|(_, c)| {
            matches!(decode_command(c), Ok(cmd) if matches!(&cmd.cmd, Some(Cmd::StartRun(s)) if s.run_id == run.as_bytes()))
        })
    }
}

/// Offline verified scan of `view`'s partition `p` (see the module docs).
pub fn scan_partition(view: &DataDirView, p: PartitionId) -> Result<LogScan, String> {
    scan_wal_dir(view.node, &view.root.join("wal"), p)
}

/// [`scan_partition`] over an explicit WAL dir.
pub fn scan_wal_dir(node: u64, wal_dir: &Path, p: PartitionId) -> Result<LogScan, String> {
    let dir = segment_dir(wal_dir, p);
    let segs = list_segments(&dir, p).map_err(|e| format!("list {}: {e}", dir.display()))?;
    let mut out = LogScan {
        node,
        partition: p,
        records: Vec::new(),
        stopped: None,
    };
    let mut next: Option<Offset> = None;
    let mut last_epoch: Option<Epoch> = None;
    'segs: for (_, path) in segs {
        let buf = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("read {}: {e}", path.display())),
        };
        let mut pos = 0usize;
        while pos < buf.len() {
            let stop = |why: String| format!("{} byte {pos}: {why}", path.display());
            let rest = &buf[pos..];
            if rest.len() < RECORD_HEADER_LEN {
                out.stopped = Some(stop("incomplete record header".into()));
                break 'segs;
            }
            let Some(len) = declared_record_size(rest).filter(|l| *l <= rest.len()) else {
                out.stopped = Some(stop("incomplete or unparseable record".into()));
                break 'segs;
            };
            let rec = match WalRecord::from_bytes(&rest[..len]) {
                Ok(r) => r,
                Err(e) => {
                    out.stopped = Some(stop(format!("unparseable record: {e}")));
                    break 'segs;
                }
            };
            let expect = match next {
                Some(n) => n,
                None => match Offset::from_i64(rec.get_base_offset()) {
                    Some(o) => o,
                    None => {
                        out.stopped = Some(stop("negative base offset".into()));
                        break 'segs;
                    }
                },
            };
            let batch = match verify_record(&rec, expect, p) {
                Ok(b) => b,
                Err(c) => {
                    out.stopped = Some(stop(format!(
                        "record at {} fails verification: {c}",
                        expect.0
                    )));
                    break 'segs;
                }
            };
            if last_epoch.is_some_and(|e| batch.epoch < e) {
                out.stopped = Some(stop(format!("epoch regression to {}", batch.epoch.0)));
                break 'segs;
            }
            let WalRecord::V2 { canonical_data, .. } = &rec else {
                out.stopped = Some(stop("not a V2 record".into()));
                break 'segs;
            };
            last_epoch = Some(batch.epoch);
            next = Some(batch.end());
            out.records.push(ScannedRecord {
                base: batch.base,
                end: batch.end(),
                epoch: batch.epoch,
                envelope: Bytes::copy_from_slice(canonical_data),
                commands: batch.commands,
            });
            pos += len;
        }
    }
    Ok(out)
}

/// For every acknowledged run, at least `min_insync` nodes' verified WAL of its partition
/// holds its `StartRun` command (below that node's measured durable end, by construction of
/// the scan). Violations name the run id, partition and the nodes that have it.
pub fn acked_on_disk(views: &[DataDirView], acks: &[Ack], min_insync: usize) -> Report {
    acked_on_disk_with(views, acks, min_insync, |_, _, _| false)
}

/// [`acked_on_disk`] with the trimmed-log rule (user-approved 2026-11-23, recorded in
/// 12-DECISIONS): a node counts as holding an acknowledged run if its verified WAL holds the
/// run's `StartRun` (exactly as in [`acked_on_disk`]), or — only when that node's WAL has
/// been trimmed by retention ([`LogScan::is_trimmed`]) — if `in_state(node, partition, run)`
/// finds the run's record in that node's checkpointed state store.
///
/// Soundness: a state store only holds what was applied from the log, so a run present in
/// state but absent from a contiguous trimmed WAL was applied below the WAL's start, and
/// retention only deletes segments covered by a durable checkpoint and the oldest snapshot.
/// An untrimmed copy never consults `in_state`.
pub fn acked_on_disk_with(
    views: &[DataDirView],
    acks: &[Ack],
    min_insync: usize,
    in_state: impl Fn(u64, PartitionId, &RunId) -> bool,
) -> Report {
    let partitions: BTreeSet<u16> = acks.iter().map(|a| a.partition).collect();
    let mut scans = Vec::new();
    let mut scan_errors = Vec::new();
    for v in views {
        for p in &partitions {
            match scan_partition(v, PartitionId(*p)) {
                Ok(s) => scans.push(s),
                Err(e) => {
                    scan_errors.push(format!("node {} partition {p}: scan failed: {e}", v.node))
                }
            }
        }
    }
    let mut report = Report::new("disk");
    for e in scan_errors {
        report.violation(e);
    }
    report
        .violations
        .extend(acked_on_scans(&scans, acks, min_insync, in_state).violations);
    report
}

/// The core of [`acked_on_disk_with`] over already-scanned logs (pure; used by the oracle
/// self-tests to model trimmed copies exactly).
pub fn acked_on_scans(
    scans: &[LogScan],
    acks: &[Ack],
    min_insync: usize,
    in_state: impl Fn(u64, PartitionId, &RunId) -> bool,
) -> Report {
    let mut report = Report::new("disk");
    for a in acks {
        let run: RunId = match a.run_id.parse() {
            Ok(r) => r,
            Err(e) => {
                report.violation(format!("acked run id {} does not parse: {e}", a.run_id));
                continue;
            }
        };
        let holders: Vec<u64> = scans
            .iter()
            .filter(|s| s.partition.0 == a.partition)
            .filter(|s| {
                s.has_start_run(&run) || (s.is_trimmed() && in_state(s.node, s.partition, &run))
            })
            .map(|s| s.node)
            .collect();
        if holders.len() < min_insync {
            report.violation(format!(
                "acked run {} (partition {}) is on disk on {} node(s) {holders:?} < min_insync {min_insync}",
                a.run_id,
                a.partition,
                holders.len()
            ));
        }
    }
    report
}
