//! Partition log recovery: a full verified scan of every segment at open.
//!
//! Research "Pattern 3" step `LogRecovered`, run per partition before any append can create
//! a WAL queue (a new `GroupCommitWal` queue always opens a NEW segment, research W4):
//!
//! 1. list `<wal_dir>/p/<pid>/wal_<pid>_<seg>.log` sorted by segment id (empty files ignored);
//! 2. verify every record: WAL frame CRC, envelope CRC32C, partition, `base == prev_last + 1`,
//!    epoch non-decreasing, codec known ([`verify_record`]);
//! 3. classify the first failure (torn tail → cut; anything else → [`LogError::Corrupt`]);
//! 4. make every retained segment durable (after a crash, readable is not durable) by
//!    fsyncing the last non-empty one (earlier segments are already durable, see
//!    [`recover_partition`]), rebuild the epoch history, `leo = last + 1`,
//!    `log_start = first base`; a log with no records is anchored at the floor
//!    (`log_start = leo = floor`, Phase 12 F3).
//!
//! Records are parsed with [`declared_record_size`] + [`WalRecord::from_bytes`] over the
//! whole file read into memory. This module never calls the vendored `WalManager` segment
//! reader, which byte-skips over bad data (W3), and never relies on the vendored readers,
//! which treat a bad record as end-of-segment (W2).

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use ostro_types::{Epoch, Offset, PartitionId, envelope};
use ostro_wal::WalRecord;
use ostro_wal::truncate::{RECORD_HEADER_LEN, declared_record_size};
use tracing::warn;

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
    /// First offset in the log. A log with no records is **anchored at the floor**
    /// (`log_start = leo = floor`, Phase 12 F3; `Offset(0)` when the floor is 0 or is the
    /// "state unreadable" sentinel above `i64::MAX`).
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

/// Sector size of the zero-sector rule (etcd `isTornEntry`).
const SECTOR: usize = 512;

/// Whether the bad record at `start` shows the signature of an unflushed extent: its byte
/// range `[start, min(start + declared, EOF))` contains an all-zero 512-byte chunk aligned
/// to 512 in file coordinates (etcd `isTornEntry`), or — when the frame header itself is
/// unreadable — everything from `start` to EOF is zero.
fn has_zero_sector(buf: &[u8], start: usize, declared: Option<usize>) -> bool {
    let end = declared.map_or(buf.len(), |l| start.saturating_add(l).min(buf.len()));
    let mut s = start.div_ceil(SECTOR) * SECTOR;
    while s + SECTOR <= end {
        if buf[s..s + SECTOR].iter().all(|&b| b == 0) {
            return true;
        }
        s += SECTOR;
    }
    declared.is_none() && start < buf.len() && buf[start..].iter().all(|&b| b == 0)
}

/// Records after the bad one that still frame and pass the WAL frame CRC. They are
/// discarded with the cut; the count is only reported.
fn count_parseable_after(buf: &[u8], start: usize, declared: Option<usize>) -> u64 {
    let Some(len) = declared else { return 0 };
    let mut pos = start.saturating_add(len);
    let mut n = 0;
    while pos < buf.len() {
        let rest = &buf[pos..];
        let Some(len) = declared_record_size(rest).filter(|&l| l <= rest.len()) else {
            break;
        };
        match WalRecord::from_bytes(&rest[..len]) {
            Ok(r) if r.calculate_checksum() == r.get_crc32() => n += 1,
            _ => break,
        }
        pos += len;
    }
    n
}

/// Physically cut `segment` at `len`: `set_len`, fsync the file, fsync its directory.
fn cut(segment: &Path, dir: &Path, len: usize) -> io::Result<()> {
    let f = OpenOptions::new().write(true).open(segment)?;
    f.set_len(len as u64)?;
    f.sync_all()?;
    File::open(dir)?.sync_all()
}

/// Where a log with no records starts (empty-log anchoring, Phase 12 F3): at `floor`, the
/// state's `applied_offset`, so the next append is assigned exactly the offset the state
/// machine applies next. A floor above `i64::MAX` (the vendored WAL's offset range) is the
/// "state unreadable" sentinel (`Offset(u64::MAX)`, see `ostro-partition` `Node::open`):
/// nothing is known, so the log anchors at 0 as before.
fn anchor(floor: Offset) -> Offset {
    if i64::try_from(floor.0).is_ok() {
        floor
    } else {
        Offset::ZERO
    }
}

/// Bytes from a record's start through the end of its envelope's `base` field: the 12-byte
/// WAL frame header, topic length + topic (`"p"`), partition, payload length, then the
/// envelope header up to and including `base` (envelope bytes 16..24). The WAL V2 frame
/// header itself carries no offset (the frame's base/last sit in the trailer, after the
/// payload); the envelope header is the first place a torn record names its base.
const BASE_PREFIX_LEN: usize = RECORD_HEADER_LEN + 2 + TOPIC.len() + 4 + 4 + 24;

/// The base offset a (possibly torn) record at `rest[0..]` names in its envelope header,
/// if the prefix through that field is present and well formed: valid WAL magic/version/
/// length, topic `"p"`, partition `pid`, envelope magic and partition. No CRC can be
/// checked on a torn record; these structural checks are what D-P12-4 relies on.
fn parsed_base(rest: &[u8], pid: PartitionId) -> Option<Offset> {
    if rest.len() < BASE_PREFIX_LEN || declared_record_size(rest).is_none() {
        return None;
    }
    let u16_at = |o: usize| u16::from_le_bytes([rest[o], rest[o + 1]]);
    let u32_at = |o: usize| u32::from_le_bytes(rest[o..o + 4].try_into().expect("4 bytes"));
    let tl = usize::from(u16_at(RECORD_HEADER_LEN));
    let topic_at = RECORD_HEADER_LEN + 2;
    if tl != TOPIC.len() || &rest[topic_at..topic_at + tl] != TOPIC.as_bytes() {
        return None;
    }
    let part_at = topic_at + tl;
    if i32::from_le_bytes(rest[part_at..part_at + 4].try_into().expect("4 bytes")) != pid.as_i32() {
        return None;
    }
    let env = part_at + 8;
    if u32_at(env) != envelope::MAGIC || u16_at(env + 8) != pid.0 {
        return None;
    }
    let base = u64::from_le_bytes(rest[env + 16..env + 24].try_into().expect("8 bytes"));
    Some(Offset(base))
}

/// D-P12-4 (parsed-base rule): may the first bad record of an anchored log (`floor > 0`, no
/// verified record before it) count as above the floor? Yes iff its envelope header names
/// `base == floor`, or fewer than [`BASE_PREFIX_LEN`] bytes remain to EOF (too short to name
/// any base). A record naming another base, or a long but unparseable prefix, stays corrupt.
fn anchored_first_record_is_above_floor(rest: &[u8], pid: PartitionId, floor: Offset) -> bool {
    rest.len() < BASE_PREFIX_LEN || parsed_base(rest, pid) == Some(floor)
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

/// Recover partition `pid` under `wal_dir`: verify every record of every segment, cut a torn
/// tail, and rebuild the log end, start and epoch history.
///
/// `floor` is the durable floor: the state store's `applied_offset` (`Offset(0)` when the
/// state store is missing and is being rebuilt). The state machine only applies offsets
/// below HW, and at RF=1 HW only advances after the WAL fsync, so every offset below the
/// floor was fsynced in the WAL.
///
/// # Torn tail vs corruption (research Pattern 4, etcd `isTornEntry` + Chronik `log_store`)
///
/// The first bad record is **torn**, and the segment is cut at its start, only if all hold:
/// 1. it is in the **last non-empty segment** (sealed segments were fsynced before rotation,
///    so damage there is real);
/// 2. it lies entirely above the floor: it starts at or after the byte end of every record
///    holding an offset below `floor`. Because the log is contiguous this is
///    `expected base >= floor`; it also protects the bad record itself when it holds offsets
///    below the floor (research Pitfall 6);
/// 3. it is incomplete at EOF, or its byte range contains an all-zero 512-byte sector
///    aligned in file coordinates (unflushed extents read back as zeros on ext4/xfs), or its
///    frame header is unreadable and everything up to EOF is zero.
///
/// Everything else fails with [`LogError::Corrupt`]; nothing is skipped.
///
/// # Empty-log anchoring (Phase 12 F3)
///
/// When no record survives (no segments, only empty segments, or nothing verified), the log
/// is anchored at the floor: `log_start = leo = floor`, `epochs` empty. The caller seeds the
/// epoch history from its cursor ([`PartitionLog::seed_epochs`](crate::PartitionLog::seed_epochs)).
/// Phase 8 restore of an idle partition and Phase 12 follower snapshot install both leave an
/// empty log under a state with `applied_offset > 0`; without the anchor that state would be
/// "ahead of the log". A non-empty log is unaffected: its start and end come from its
/// records, and a floor above its end stays the caller's "state ahead of log" error.
///
/// # Torn first record of an anchored log (D-P12-4, parsed-base rule)
///
/// With `floor > 0` and no verified record before it, the first bad record is torn (cut, as
/// at floor 0, leaving the log anchored empty at the floor) iff rules 1 and 3 above hold and
/// **either** its header names base offset `== floor` **or** fewer bytes than that header
/// prefix remain to EOF. The WAL V2 frame header carries no offset (base/last are in the
/// trailer after the payload), so the base is read from the envelope header right after the
/// topic, partition and payload length: the first [`BASE_PREFIX_LEN`] (47) bytes of the
/// record. A complete prefix naming any other base, or one that does not parse, stays
/// [`LogError::Corrupt`] (if it held offsets below the floor it was fsynced and the damage
/// is real). Install (`reset_to`) fsyncs the emptied partition directory, so after a crash
/// the log is either the old one or empty, and the first append after an anchor is the only
/// way to reach this shape (power loss before its first fsync). The "state unreadable"
/// sentinel floor never qualifies.
///
/// Records after a cut are discarded even when they parse: they were in the same un-fsynced
/// window and cannot have been acknowledged, because an fsync covering them would have made
/// the bad record's bytes durable too. Their count is logged at `warn`.
///
/// `kill -9` never produces a torn write (the page cache survives the process; at most a
/// short final write is left). Torn and zero-filled tails come only from power loss or
/// kernel crashes, which is why the tests inject them offline.
///
/// The cut (`set_len` + fsync file + fsync dir) happens here, before any `GroupCommitWal`
/// queue for the partition exists: a new queue opens a NEW segment (research W4), after
/// which the torn bytes would sit in a sealed segment and fail every later open.
///
/// Blocking (reads whole segments, fsyncs); async callers use `spawn_blocking`.
pub fn recover_partition(
    wal_dir: &Path,
    pid: PartitionId,
    floor: Offset,
) -> Result<RecoveryReport, LogError> {
    let dir = segment_dir(wal_dir, pid);
    let segs = list_segments(&dir, pid)?;
    let mut lens = Vec::with_capacity(segs.len());
    for (_, path) in &segs {
        lens.push(fs::metadata(path)?.len());
    }
    let last_non_empty = lens.iter().rposition(|&l| l > 0);
    let mut scan = Scan::default();
    let mut retained = Vec::new();
    let mut torn = None;

    'segments: for (i, (_, path)) in segs.iter().enumerate() {
        if lens[i] == 0 {
            continue;
        }
        let buf = fs::read(path)?;
        if buf.is_empty() {
            continue;
        }
        retained.push(path.clone());
        let mut pos = 0usize;
        while pos < buf.len() {
            let bad = match parse_at(&buf, pos, &scan, pid) {
                Ok((len, batch)) => {
                    scan.accept(&batch, len);
                    pos += len;
                    continue;
                }
                Err(bad) => bad,
            };
            let in_last_segment = Some(i) == last_non_empty;
            // An anchored log's first record (D-P12-4): no verified record precedes it, so
            // its offsets are known only from its own header. The "state unreadable"
            // sentinel floor never qualifies (nothing is cut for an unknown state).
            let anchored_first =
                scan.next.is_none() && floor > Offset::ZERO && anchor(floor) == floor;
            let above_floor = if anchored_first {
                anchored_first_record_is_above_floor(&buf[pos..], pid, floor)
            } else {
                scan.next.unwrap_or(Offset::ZERO) >= floor
            };
            let torn_shape =
                bad.reason == Corruption::Incomplete || has_zero_sector(&buf, pos, bad.declared);
            if !(in_last_segment && above_floor && torn_shape) {
                return Err(corrupt(pid, path, pos, bad));
            }
            let discarded = count_parseable_after(&buf, pos, bad.declared);
            warn!(
                partition = pid.0,
                segment = %path.display(),
                byte_pos = pos,
                discarded_records = discarded,
                reason = %bad.reason,
                "cutting torn tail"
            );
            cut(path, &dir, pos)?;
            torn = Some(TornCut {
                segment: path.clone(),
                byte_pos: pos as u64,
                discarded_records: discarded,
                reason: bad.reason,
            });
            break 'segments;
        }
    }

    // After a crash, readable is not durable: every retained segment must be durable before
    // anything (state apply, acks) relies on it. Only the LAST non-empty segment can hold
    // bytes that were never fsynced (plan 02-16; fsyncing all of them made reopen grow with
    // the segment count, about 1 s per partition after 500 restarts):
    // - a segment the WAL rotated away from was fsynced before the rotation (seal and
    //   truncation paths in the vendored `GroupCommitWal` call `sync_all` first);
    // - a segment that was the tail when a process died was the last non-empty segment at
    //   the next recovery, which fsynced it here before `LogNode::open` created the WAL (a
    //   new WAL queue opens a NEW segment, so nothing newer can exist before that fsync).
    //   A crash during recovery leaves the same layout, so the next recovery repeats it.
    // By induction every non-tail segment is durable; a torn cut already fsynced the tail.
    if let Some(path) = retained.last() {
        File::open(path)?.sync_all()?;
    }

    // Empty-log anchoring (Phase 12 F3, plan 05-21): a log with no records starts at the
    // floor. Records are never invented; this only fixes where the next append lands.
    let log_start = scan.log_start.unwrap_or_else(|| anchor(floor));
    Ok(RecoveryReport {
        segments: segs.into_iter().map(|(_, p)| p).collect(),
        log_start,
        leo: scan.next.unwrap_or(log_start),
        epochs: scan.epochs,
        torn,
        records: scan.records,
        bytes: scan.bytes,
    })
}
