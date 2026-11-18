//! Partition log recovery: a full verified scan of every segment at open.
//!
//! Research "Pattern 3" step `LogRecovered`, run per partition before any append can create
//! a WAL queue (a new `GroupCommitWal` queue always opens a NEW segment, research W4):
//!
//! 1. list `<wal_dir>/p/<pid>/wal_<pid>_<seg>.log` sorted by segment id (empty files ignored);
//! 2. verify every record: WAL frame CRC, envelope CRC32C, partition, `base == prev_last + 1`,
//!    epoch non-decreasing, codec known ([`verify_record`]);
//! 3. classify the first failure (torn tail → cut; anything else → [`LogError::Corrupt`]);
//! 4. fsync every retained segment once (after a crash, readable is not durable), rebuild
//!    the epoch history, `leo = last + 1`, `log_start = first base`.
//!
//! Records are parsed with [`declared_record_size`] + [`WalRecord::from_bytes`] over the
//! whole file read into memory. This module never calls `WalManager::read_segment`, which
//! byte-skips over bad data (W3), and never relies on the vendored readers, which treat a
//! bad record as end-of-segment (W2).

use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use ostro_types::{Epoch, Offset, PartitionId};
use ostro_wal::WalRecord;
use ostro_wal::truncate::{RECORD_HEADER_LEN, declared_record_size};

use crate::error::LogError;
use crate::verify::{Corruption, VerifiedBatch, verify_record};

/// WAL topic every Ostro partition lives under (one `GroupCommitWal` per node, research W11).
pub const TOPIC: &str = "p";

/// Outcome of [`recover_partition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Every segment file found, sorted by segment id (empty files included; they are not
    /// deleted in Phase 2).
    pub segments: Vec<PathBuf>,
    /// First offset in the log (`Offset(0)` for an empty log).
    pub log_start: Offset,
    /// Log end offset: last offset + 1 (`log_start` for an empty log).
    pub leo: Offset,
    /// Epoch history: each epoch with the first offset it wrote, ascending.
    pub epochs: Vec<(Epoch, Offset)>,
    /// The torn tail that was cut, if any.
    pub torn: Option<TornCut>,
    /// Verified records retained.
    pub records: u64,
    /// Bytes of verified records retained.
    pub bytes: u64,
}

/// A torn tail physically removed by [`recover_partition`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TornCut {
    /// Segment that was cut.
    pub segment: PathBuf,
    /// New segment length (start of the first bad record).
    pub byte_pos: u64,
    /// Records after the bad one that still parsed and were discarded with it.
    pub discarded_records: u64,
    /// Why the first discarded record was bad.
    pub reason: Corruption,
}

/// `<wal_dir>/p/<pid>`.
pub fn segment_dir(wal_dir: &Path, pid: PartitionId) -> PathBuf {
    wal_dir.join(TOPIC).join(pid.0.to_string())
}

/// Segment files `wal_<pid>_<seg>.log` in `dir`, sorted by segment id. Includes empty files;
/// a missing directory is an empty list.
pub fn list_segments(dir: &Path, pid: PartitionId) -> io::Result<Vec<(u64, PathBuf)>> {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let prefix = format!("wal_{}_", pid.0);
    let mut out = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let Some(id) = name
            .to_str()
            .and_then(|n| n.strip_prefix(&prefix))
            .and_then(|n| n.strip_suffix(".log"))
            .and_then(|n| n.parse::<u64>().ok())
        else {
            continue;
        };
        out.push((id, entry.path()));
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

/// A record that failed to parse or verify.
#[derive(Debug)]
struct Bad {
    reason: Corruption,
    /// Offsets from the frame when it parsed, else (expected base, -1).
    first: i64,
    last: i64,
    /// Declared record size from the frame header, when readable.
    #[allow(dead_code)] // read by the torn-tail classifier
    declared: Option<usize>,
}

/// Running state of the scan.
#[derive(Default)]
struct Scan {
    log_start: Option<Offset>,
    next: Option<Offset>,
    last_epoch: Option<Epoch>,
    epochs: Vec<(Epoch, Offset)>,
    records: u64,
    bytes: u64,
}

impl Scan {
    fn accept(&mut self, b: &VerifiedBatch, len: usize) {
        if self.log_start.is_none() {
            self.log_start = Some(b.base);
        }
        if self.last_epoch != Some(b.epoch) {
            self.epochs.push((b.epoch, b.base));
            self.last_epoch = Some(b.epoch);
        }
        self.next = Some(b.end());
        self.records += 1;
        self.bytes += len as u64;
    }
}

/// Parse and verify the record starting at `buf[pos..]`.
fn parse_at(
    buf: &[u8],
    pos: usize,
    scan: &Scan,
    pid: PartitionId,
) -> Result<(usize, VerifiedBatch), Bad> {
    let expected = scan.next.map_or(-1, Offset::as_i64);
    let bad = |reason, declared| Bad {
        reason,
        first: expected,
        last: -1,
        declared,
    };
    let rest = &buf[pos..];
    if rest.len() < RECORD_HEADER_LEN {
        return Err(bad(Corruption::Incomplete, None));
    }
    let Some(len) = declared_record_size(rest) else {
        return Err(bad(Corruption::Unparseable, None));
    };
    if len > rest.len() {
        return Err(bad(Corruption::Incomplete, Some(len)));
    }
    let raw = &rest[..len];
    let rec = WalRecord::from_bytes(raw).map_err(|_| bad(Corruption::Unparseable, Some(len)))?;
    let located = |reason| Bad {
        reason,
        first: rec.get_base_offset(),
        last: rec.get_last_offset(),
        declared: Some(len),
    };
    // The first record of the log sets the start; every later one must follow on.
    let expect = match scan.next {
        Some(n) => n,
        None => Offset::from_i64(rec.get_base_offset()).ok_or_else(|| {
            located(Corruption::Misplaced {
                partition: rec.get_topic_partition().map_or(-1, |(_, p)| p),
                base: rec.get_base_offset(),
            })
        })?,
    };
    let batch = verify_record(&rec, expect, pid).map_err(located)?;
    // The frame's own length field must cover exactly the parsed fields.
    if RECORD_HEADER_LEN + rec.calculate_length() as usize != len {
        return Err(located(Corruption::Unparseable));
    }
    if let Some(prev) = scan.last_epoch
        && batch.epoch < prev
    {
        return Err(located(Corruption::EpochRegression {
            prev: prev.0,
            found: batch.epoch.0,
        }));
    }
    Ok((len, batch))
}

fn corrupt(pid: PartitionId, segment: &Path, pos: usize, bad: Bad) -> LogError {
    LogError::Corrupt {
        partition: pid.0,
        segment: segment.to_path_buf(),
        byte_pos: pos as u64,
        first: bad.first,
        last: bad.last,
        reason: bad.reason,
    }
}

/// Recover partition `pid` under `wal_dir`: verify every record of every segment and rebuild
/// the log end, start and epoch history.
///
/// `floor` is the durable floor: the state store's `applied_offset` (`Offset(0)` when the
/// state store is missing). Every offset below it was applied, hence fsynced in the WAL.
///
/// Blocking (reads whole segments, fsyncs); async callers use `spawn_blocking`.
pub fn recover_partition(
    wal_dir: &Path,
    pid: PartitionId,
    floor: Offset,
) -> Result<RecoveryReport, LogError> {
    let _ = floor;
    let dir = segment_dir(wal_dir, pid);
    let segs = list_segments(&dir, pid)?;
    let mut scan = Scan::default();
    let mut retained = Vec::new();

    for (_, path) in &segs {
        let buf = fs::read(path)?;
        if buf.is_empty() {
            continue;
        }
        retained.push(path.clone());
        let mut pos = 0usize;
        while pos < buf.len() {
            match parse_at(&buf, pos, &scan, pid) {
                Ok((len, batch)) => {
                    scan.accept(&batch, len);
                    pos += len;
                }
                Err(bad) => return Err(corrupt(pid, path, pos, bad)),
            }
        }
    }

    // After a crash, readable is not durable: make every retained segment durable once
    // before anything (state apply, acks) relies on it.
    for path in &retained {
        File::open(path)?.sync_all()?;
    }

    let log_start = scan.log_start.unwrap_or(Offset::ZERO);
    Ok(RecoveryReport {
        segments: segs.into_iter().map(|(_, p)| p).collect(),
        log_start,
        leo: scan.next.unwrap_or(log_start),
        epochs: scan.epochs,
        torn: None,
        records: scan.records,
        bytes: scan.bytes,
    })
}
