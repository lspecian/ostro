//! Audit segment files, torn-tail recovery and retention (D-P14-11, 14-04).
//!
//! The journal directory holds `audit-<first seq, 20 digits>.jsonl` files, one record per
//! line ([`crate::audit::journal`]). Every segment starts with a `segment_open` record and
//! every process start opens a new segment, so an incarnation's records never share a file
//! with an earlier incarnation's torn tail.
//!
//! **Recovery** ([`Journal::open`]): the newest segment is scanned; the longest prefix of
//! newline-terminated, canonical, self-hashing and chain-linked records is kept and the rest
//! (a torn write, appended garbage, a zero-filled tail) is truncated. A segment with no
//! valid record is deleted and the previous one is scanned. A *valid* record after an
//! invalid region is not a torn tail but a modified journal: open refuses it
//! ([`io::ErrorKind::InvalidData`]) instead of destroying the evidence. The writer then
//! appends `audit_recovered{truncated_bytes, last_good_seq}` after the new `segment_open`.
//!
//! **Retention** ([`Journal::retention_plan`]): whole segments, oldest first and never the
//! active one, whose records are all older than `retention` (a segment's newest record is no
//! newer than the next segment's `segment_open`) or that push the journal over `max_bytes`.
//! The writer appends and syncs `retention_trim{first_kept_seq, first_kept_hash}` before it
//! deletes them, so verification can start from that anchor.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use ostro_types::audit::AuditEvent;

use super::chain::{self, Hash};
use super::journal::{self, Coalesced, Record, Stamp};

/// Segment file name prefix.
pub const PREFIX: &str = "audit-";
/// Segment file name suffix.
pub const SUFFIX: &str = ".jsonl";

/// `audit-<seq 20 digits>.jsonl`.
pub fn segment_name(first_seq: u64) -> String {
    format!("{PREFIX}{first_seq:020}{SUFFIX}")
}

/// The first seq of a segment file name (exactly 20 digits).
pub fn parse_segment_name(name: &str) -> Option<u64> {
    let digits = name.strip_prefix(PREFIX)?.strip_suffix(SUFFIX)?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The segment files of `dir`, oldest first (other files are ignored).
pub fn list_segments(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    for e in fs::read_dir(dir)? {
        let e = e?;
        if let Some(seq) = e.file_name().to_str().and_then(parse_segment_name)
            && e.file_type()?.is_file()
        {
            out.push((seq, e.path()));
        }
    }
    out.sort_by_key(|(s, _)| *s);
    Ok(out)
}

/// fsync a directory (after creating, renaming or deleting entries).
pub fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// What we keep in memory about a segment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegInfo {
    /// Seq of its `segment_open`.
    pub first_seq: u64,
    pub path: PathBuf,
    /// Bytes on disk (plus buffered bytes for the active segment).
    pub bytes: u64,
    /// `ts_ms` of its first record.
    pub first_ts_ms: u64,
    /// Hash of its first record (a retention anchor).
    pub first_hash: Hash,
}

/// What open found and repaired.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    /// Bytes removed (truncated tails and deleted empty/torn segments).
    pub truncated_bytes: u64,
    /// The last record kept (0 = none).
    pub last_good_seq: u64,
}

fn corrupt(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// The valid prefix of one segment's bytes.
struct Scan {
    valid_len: usize,
    first: Option<Record>,
    last: Option<Record>,
}

fn scan(bytes: &[u8], path: &Path) -> io::Result<Scan> {
    let mut pos = 0usize;
    let mut first: Option<Record> = None;
    let mut last: Option<Record> = None;
    while let Some(off) = bytes[pos..].iter().position(|&b| b == b'\n') {
        let line = &bytes[pos..pos + off];
        let ok = match Record::parse_line(line) {
            Ok(r) => {
                let linked = match &last {
                    None => r.action == journal::SEGMENT_OPEN,
                    Some(p) => r.seq == p.seq + 1 && r.prev_hash == p.hash,
                };
                if linked {
                    if first.is_none() {
                        first = Some(r.clone());
                    }
                    last = Some(r);
                }
                linked
            }
            Err(_) => false,
        };
        if !ok {
            break;
        }
        pos += off + 1;
    }
    // Anything after the valid prefix must be a torn tail: no complete, self-consistent
    // record may follow it.
    let mut rest = pos;
    let mut skipped_first = false;
    while let Some(off) = bytes[rest..].iter().position(|&b| b == b'\n') {
        let line = &bytes[rest..rest + off];
        if skipped_first && Record::parse_line(line).is_ok() {
            return Err(corrupt(format!(
                "{}: a valid audit record follows an invalid one at byte {pos}: the journal \
                 was modified, not torn; run `ostro audit verify` and move the directory \
                 aside to start a new journal",
                path.display()
            )));
        }
        skipped_first = true;
        rest += off + 1;
    }
    Ok(Scan {
        valid_len: pos,
        first,
        last,
    })
}

/// Read the first line of a segment (index of the older segments).
fn first_record(path: &Path) -> io::Result<Record> {
    let mut r = BufReader::new(File::open(path)?);
    let mut line = Vec::new();
    r.read_until(b'\n', &mut line)?;
    if line.pop() != Some(b'\n') {
        return Err(corrupt(format!(
            "{}: no complete first record",
            path.display()
        )));
    }
    let rec = Record::parse_line(&line)
        .map_err(|e| corrupt(format!("{}: first record: {e}", path.display())))?;
    if rec.action != journal::SEGMENT_OPEN {
        return Err(corrupt(format!(
            "{}: first record is {:?}, not segment_open",
            path.display(),
            rec.action
        )));
    }
    Ok(rec)
}

/// The open journal (owned by the writer thread).
pub struct Journal {
    dir: PathBuf,
    node: u64,
    incarnation: u64,
    segment_bytes: u64,
    segs: Vec<SegInfo>,
    active: Option<File>,
    buf: Vec<u8>,
    next_seq: u64,
    last_hash: Hash,
    recovery: Recovery,
    syncs: u64,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("dir", &self.dir)
            .field("segments", &self.segs.len())
            .field("next_seq", &self.next_seq)
            .finish_non_exhaustive()
    }
}

impl Journal {
    /// Open `dir` (created if missing), repair a torn tail and index the segments. No new
    /// segment is written until [`Journal::start_incarnation`].
    pub fn open(dir: &Path, node: u64, incarnation: u64, segment_bytes: u64) -> io::Result<Self> {
        let fresh = !dir.is_dir();
        fs::create_dir_all(dir)?;
        if fresh && let Some(parent) = dir.parent() {
            sync_dir(parent)?;
        }
        let mut recovery = Recovery::default();
        let mut files = list_segments(dir)?;
        let mut tail: Option<Record> = None;
        while let Some((seq, path)) = files.last().cloned() {
            let bytes = fs::read(&path)?;
            let s = scan(&bytes, &path)?;
            if let (Some(first), Some(_)) = (&s.first, &s.last)
                && first.seq != seq
            {
                return Err(corrupt(format!(
                    "{}: starts at seq {}, not {seq}",
                    path.display(),
                    first.seq
                )));
            }
            let cut = (bytes.len() - s.valid_len) as u64;
            if s.last.is_none() {
                // Nothing valid (a torn segment_open or an empty file): drop the file.
                fs::remove_file(&path)?;
                sync_dir(dir)?;
                recovery.truncated_bytes += cut;
                files.pop();
                continue;
            }
            if cut > 0 {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(s.valid_len as u64)?;
                f.sync_all()?;
                recovery.truncated_bytes += cut;
            }
            tail = s.last;
            break;
        }
        let mut segs = Vec::with_capacity(files.len());
        for (seq, path) in files {
            let first = first_record(&path)?;
            if first.seq != seq {
                return Err(corrupt(format!(
                    "{}: starts at seq {}, not {seq}",
                    path.display(),
                    first.seq
                )));
            }
            let first_hash = first
                .hash_bytes()
                .ok_or_else(|| corrupt(format!("{}: bad hash", path.display())))?;
            let bytes = fs::metadata(&path)?.len();
            segs.push(SegInfo {
                first_seq: seq,
                path,
                bytes,
                first_ts_ms: first.ts_ms,
                first_hash,
            });
        }
        let (next_seq, last_hash) = match &tail {
            Some(r) => (
                r.seq + 1,
                r.hash_bytes()
                    .ok_or_else(|| corrupt("bad tail hash".to_owned()))?,
            ),
            None => (1, chain::genesis(node, incarnation)),
        };
        recovery.last_good_seq = tail.as_ref().map_or(0, |r| r.seq);
        Ok(Journal {
            dir: dir.to_path_buf(),
            node,
            incarnation,
            segment_bytes: segment_bytes.max(1),
            segs,
            active: None,
            buf: Vec::new(),
            next_seq,
            last_hash,
            recovery,
            syncs: 0,
        })
    }

    /// What open repaired.
    pub fn recovery(&self) -> &Recovery {
        &self.recovery
    }

    /// The journal directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// `(seq, hash)` of the last record (seq 0 = empty journal; hash = genesis then).
    pub fn head(&self) -> (u64, Hash) {
        (self.next_seq - 1, self.last_hash)
    }

    /// The segments, oldest first (the active one last once written).
    pub fn segments(&self) -> &[SegInfo] {
        &self.segs
    }

    /// Open this incarnation's first segment and record the recovery, if any. Returns the
    /// records written (not yet synced).
    pub fn start_incarnation(&mut self, ts_ms: u64) -> io::Result<Vec<Record>> {
        let mut out = vec![self.rotate(ts_ms)?];
        if self.recovery.truncated_bytes > 0 {
            let mut d = BTreeMap::new();
            d.insert(
                journal::D_TRUNCATED_BYTES.to_owned(),
                self.recovery.truncated_bytes.to_string(),
            );
            d.insert(
                journal::D_LAST_GOOD_SEQ.to_owned(),
                self.recovery.last_good_seq.to_string(),
            );
            out.push(self.write(Record::control(journal::AUDIT_RECOVERED, d), None, ts_ms)?);
        }
        Ok(out)
    }

    /// Append a record (rotating first when the active segment reached `segment_bytes`).
    /// Returns every record written: a `segment_open` from the rotation, then `ev`'s.
    pub fn append(
        &mut self,
        ev: AuditEvent,
        coalesced: Option<Coalesced>,
        ts_ms: u64,
    ) -> io::Result<Vec<Record>> {
        let mut out = Vec::with_capacity(1);
        let full = self
            .segs
            .last()
            .is_none_or(|s| s.bytes >= self.segment_bytes);
        if self.active.is_none() || full {
            out.push(self.rotate(ts_ms)?);
        }
        out.push(self.write(ev, coalesced, ts_ms)?);
        Ok(out)
    }

    fn write(
        &mut self,
        ev: AuditEvent,
        coalesced: Option<Coalesced>,
        ts_ms: u64,
    ) -> io::Result<Record> {
        let stamp = Stamp {
            seq: self.next_seq,
            ts_ms,
            node: self.node,
            incarnation: self.incarnation,
        };
        let prev = if skip_chain_mutant() {
            chain::link(&self.last_hash, b"audit::mutant::skip_chain")
        } else {
            self.last_hash
        };
        let rec = Record::seal(ev, stamp, coalesced, &prev);
        let line = rec.to_line();
        self.buf.extend_from_slice(&line);
        if let Some(s) = self.segs.last_mut() {
            s.bytes += line.len() as u64;
        }
        self.next_seq += 1;
        self.last_hash = rec
            .hash_bytes()
            .ok_or_else(|| io::Error::other("sealed record has no hash"))?;
        Ok(rec)
    }

    /// Close the active segment (synced) and start a new one with `segment_open`.
    fn rotate(&mut self, ts_ms: u64) -> io::Result<Record> {
        if self.active.is_some() {
            self.sync()?;
        }
        let seq = self.next_seq;
        let path = self.dir.join(segment_name(seq));
        let f = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        sync_dir(&self.dir)?;
        self.active = Some(f);
        self.segs.push(SegInfo {
            first_seq: seq,
            path,
            bytes: 0,
            first_ts_ms: ts_ms,
            first_hash: [0; 32],
        });
        let mut d = BTreeMap::new();
        d.insert(
            journal::D_PREV_SEGMENT_LAST_HASH.to_owned(),
            chain::to_hex(&self.last_hash),
        );
        let rec = self.write(Record::control(journal::SEGMENT_OPEN, d), None, ts_ms)?;
        if let Some(s) = self.segs.last_mut() {
            s.first_hash = rec.hash_bytes().unwrap_or([0; 32]);
        }
        Ok(rec)
    }

    /// Write buffered records and fsync the active segment (the group commit).
    pub fn sync(&mut self) -> io::Result<()> {
        let Some(f) = self.active.as_mut() else {
            return Ok(());
        };
        if !self.buf.is_empty() {
            f.write_all(&self.buf)?;
            self.buf.clear();
        }
        before_fsync()?;
        f.sync_data()?;
        self.syncs += 1;
        Ok(())
    }

    /// Successful segment fsyncs so far (group commits, rotations, trims).
    pub fn syncs(&self) -> u64 {
        self.syncs
    }

    /// How many of the oldest segments retention deletes now (never the active one).
    pub fn retention_plan(&self, now_ms: u64, retention_ms: u64, max_bytes: u64) -> usize {
        let n = self.segs.len();
        if n < 2 {
            return 0;
        }
        let cutoff = now_ms.saturating_sub(retention_ms);
        let mut by_age = 0;
        while by_age < n - 1 && self.segs[by_age + 1].first_ts_ms < cutoff {
            by_age += 1;
        }
        let mut total: u64 = self.segs.iter().map(|s| s.bytes).sum();
        let mut by_size = 0;
        while by_size < n - 1 && total > max_bytes {
            total -= self.segs[by_size].bytes;
            by_size += 1;
        }
        by_age.max(by_size)
    }

    /// Delete the `n` oldest segments: appends and syncs `retention_trim` first. Returns the
    /// records written (already synced).
    pub fn trim(&mut self, n: usize, ts_ms: u64) -> io::Result<Vec<Record>> {
        let n = n.min(self.segs.len().saturating_sub(1));
        if n == 0 {
            return Ok(Vec::new());
        }
        let kept = self.segs[n].clone();
        let mut d = BTreeMap::new();
        d.insert(
            journal::D_FIRST_KEPT_SEQ.to_owned(),
            kept.first_seq.to_string(),
        );
        d.insert(
            journal::D_FIRST_KEPT_HASH.to_owned(),
            chain::to_hex(&kept.first_hash),
        );
        let out = self.append(Record::control(journal::RETENTION_TRIM, d), None, ts_ms)?;
        self.sync()?;
        for s in self.segs.drain(..n) {
            match fs::remove_file(&s.path) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        sync_dir(&self.dir)?;
        Ok(out)
    }
}

/// Read a whole segment (verification helper).
pub fn read_segment(path: &Path) -> io::Result<Vec<u8>> {
    let mut v = Vec::new();
    File::open(path)?.read_to_end(&mut v)?;
    Ok(v)
}

#[cfg(feature = "failpoints")]
fn skip_chain_mutant() -> bool {
    (|| {
        fail::fail_point!("audit::mutant::skip_chain", |_| true);
        false
    })()
}

#[cfg(not(feature = "failpoints"))]
fn skip_chain_mutant() -> bool {
    false
}

fn before_fsync() -> io::Result<()> {
    #[cfg(feature = "failpoints")]
    fail::fail_point!("audit::before_fsync", |_| Err(io::Error::other(
        "injected fsync failure (audit::before_fsync)"
    )));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_twenty_digits() {
        assert_eq!(segment_name(42), "audit-00000000000000000042.jsonl");
        assert_eq!(parse_segment_name(&segment_name(u64::MAX)), Some(u64::MAX));
        assert_eq!(parse_segment_name("audit-42.jsonl"), None);
        assert_eq!(parse_segment_name("audit-0000000000000000004x.jsonl"), None);
        assert_eq!(parse_segment_name("notes.txt"), None);
    }
}
