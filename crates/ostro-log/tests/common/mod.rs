//! Shared helpers for the recovery tests: write real segments through the
//! vendored `GroupCommitWal`, then corrupt them offline.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use bytes::Bytes;
use ostro_log::ostro_wal::{GroupCommitConfig, GroupCommitWal, WalRecord};
use ostro_types::{Epoch, Offset, PartitionId, envelope};

/// Topic every Ostro partition lives under in the shared WAL (research W11).
pub const TOPIC: &str = "p";

/// One written batch: where it starts in the log and on disk.
#[derive(Debug, Clone)]
pub struct Written {
    /// Base offset of the batch.
    pub base: Offset,
    /// Number of commands.
    pub count: u64,
    /// Byte position of the record start inside `segment`.
    pub byte_pos: u64,
    /// Encoded record length in bytes.
    pub len: u64,
    /// Segment file holding the record.
    pub segment: PathBuf,
}

impl Written {
    /// Byte position just past the record.
    pub fn end_pos(&self) -> u64 {
        self.byte_pos + self.len
    }
    /// Offset just past the batch.
    pub fn end(&self) -> Offset {
        self.base + self.count
    }
}

/// Deterministic command bytes for `(offset)`.
pub fn cmd(off: u64) -> Bytes {
    Bytes::from(format!("cmd-{off:08}-{}", "x".repeat((off % 37) as usize)))
}

/// Build a V2 WAL record whose canonical data is a real envelope.
pub fn make_record(pid: PartitionId, epoch: u32, base: Offset, n: u64) -> WalRecord {
    let cmds: Vec<Bytes> = (0..n).map(|i| cmd(base.0 + i)).collect();
    let env = envelope::encode(pid, Epoch(epoch), base, &cmds);
    WalRecord::new_v2(
        TOPIC.to_string(),
        pid.as_i32(),
        env,
        base.as_i64(),
        (base + (n - 1)).as_i64(),
        i32::try_from(n).unwrap(),
    )
}

/// Write `batches` (`(epoch, n_cmds)`) starting at `start` through one fresh
/// `GroupCommitWal` instance. Every instance opens a new segment (W4), so one
/// call = one segment.
pub async fn write_batches(
    wal_dir: &Path,
    pid: PartitionId,
    start: Offset,
    batches: &[(u32, u64)],
) -> Vec<Written> {
    let wal = GroupCommitWal::new(wal_dir.to_path_buf(), GroupCommitConfig::default());
    let mut out = Vec::new();
    let mut base = start;
    let mut pos = 0u64;
    for &(epoch, n) in batches {
        let rec = make_record(pid, epoch, base, n);
        let len = rec.to_bytes().unwrap().len() as u64;
        wal.append(TOPIC.to_string(), pid.as_i32(), rec, 1)
            .await
            .unwrap();
        out.push(Written {
            base,
            count: n,
            byte_pos: pos,
            len,
            segment: PathBuf::new(),
        });
        pos += len;
        base = base + n;
    }
    wal.shutdown().await;
    drop(wal);
    // Locate the segment just written: the highest-numbered file.
    let segs =
        ostro_log::recovery::list_segments(&ostro_log::recovery::segment_dir(wal_dir, pid), pid)
            .unwrap();
    let seg = segs.last().expect("segment written").1.clone();
    assert_eq!(
        std::fs::metadata(&seg).unwrap().len(),
        pos,
        "segment length"
    );
    for w in &mut out {
        w.segment = seg.clone();
    }
    out
}

/// Write one segment per entry of `segments`, contiguous offsets from 0.
pub async fn write_log(
    wal_dir: &Path,
    pid: PartitionId,
    segments: &[&[(u32, u64)]],
) -> Vec<Written> {
    let mut all = Vec::new();
    let mut next = Offset(0);
    for seg in segments {
        let w = write_batches(wal_dir, pid, next, seg).await;
        next = w.last().unwrap().end();
        all.extend(w);
    }
    all
}

/// Replace `len` bytes at `pos` of `path` with `bytes` (same length).
pub fn overwrite(path: &Path, pos: u64, bytes: &[u8]) {
    let mut data = std::fs::read(path).unwrap();
    let p = usize::try_from(pos).unwrap();
    data[p..p + bytes.len()].copy_from_slice(bytes);
    std::fs::write(path, data).unwrap();
}

/// Remove `[pos, pos+len)` from the file (splice).
pub fn splice_out(path: &Path, pos: u64, len: u64) {
    let mut data = std::fs::read(path).unwrap();
    let p = usize::try_from(pos).unwrap();
    let l = usize::try_from(len).unwrap();
    data.drain(p..p + l);
    std::fs::write(path, data).unwrap();
}

/// Byte offset of `canonical_data` inside a V2 record with topic "p":
/// header 12 + topic_len 2 + topic 1 + partition 4 + data_len 4.
pub const CANONICAL_OFF: u64 = 23;
