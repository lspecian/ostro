//! WAL import (plan 08-18, restore): write a partition log from raw OSTR envelope batches
//! ([`RawBatch`], as stored in bottomless chunks), stopping exactly at a cut.
//!
//! Every envelope becomes one standard WAL V2 record ([`WalRecord::new_v2`] +
//! [`WalRecord::to_bytes`], the vendored writer's frame, unchanged), byte-identical to what
//! [`PartitionLog`](crate::PartitionLog) appends for the same batch, so [`recover_partition`] and
//! [`LogNode::open`](crate::LogNode::open) read an imported log like any other:
//! `log_start` = first base, `LEO` = `upto`, the epoch history rebuilt from the batch epochs.
//!
//! - [`ImportMode::Create`] writes `wal/p/<id>/wal_<id>_0.log` and refuses an existing log.
//!   Zero batches is allowed and leaves an empty segment (an idle partition): the next open
//!   anchors it at the state's applied offset (`log_start = LEO = applied`, Phase 12 F3) and
//!   seeds the epoch history from the cursor.
//! - [`ImportMode::Append`] reopens the imported log, requires its LEO to equal the first
//!   batch's base (any base when it holds no record yet), and appends up to `upto`.
//!
//! Create to `x` then Append to `y` produces the same bytes as Create to `y`.
//!
//! Each envelope is fully verified (CRC32C, partition, contiguity, epoch order) before it is
//! written; `upto` must fall on an envelope (batch) boundary. The segment file and its
//! directory are fsynced before returning. Blocking; async callers use `spawn_blocking`.

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};

use ostro_types::{Epoch, Offset, PartitionId, envelope};
use ostro_wal::WalRecord;

use crate::error::LogError;
use crate::recovery::{TOPIC, list_segments, recover_partition, segment_dir};
use crate::replicated::RawBatch;

/// How [`write_partition_log`] treats an existing log.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum ImportMode {
    /// A fresh log; an existing segment is an error.
    Create,
    /// Continue the imported log at its LEO.
    Append,
}

/// What [`write_partition_log`] wrote.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ImportReport {
    /// The segment written.
    pub segment: PathBuf,
    /// First offset of the log (`None` while it holds no record).
    pub log_start: Option<Offset>,
    /// Log end after the import (`None` while it holds no record).
    pub leo: Option<Offset>,
    /// Records (envelopes) written by this call.
    pub records: u64,
    /// Bytes written by this call.
    pub bytes: u64,
}

fn bad(msg: impl Into<String>) -> LogError {
    LogError::Wal(format!("import: {}", msg.into()))
}

/// Write partition `partition`'s log under `wal_root` (the node's `wal/` directory) from
/// `batches`, stopping exactly at `upto` (exclusive). See the module docs.
pub fn write_partition_log(
    wal_root: &Path,
    partition: PartitionId,
    batches: impl IntoIterator<Item = RawBatch>,
    upto: Offset,
    mode: ImportMode,
) -> Result<ImportReport, LogError> {
    let dir = segment_dir(wal_root, partition);
    let existing = list_segments(&dir, partition)?;
    // Where the log stands: (segment, first, next offset, last epoch).
    let (segment, mut first, mut next, mut last_epoch) = match mode {
        ImportMode::Create => {
            if !existing.is_empty() {
                return Err(bad(format!(
                    "partition {}: a log already exists in {}",
                    partition.0,
                    dir.display()
                )));
            }
            (
                dir.join(format!("wal_{}_0.log", partition.0)),
                None,
                None,
                None,
            )
        }
        ImportMode::Append => {
            let Some((_, last)) = existing.last() else {
                return Err(bad(format!(
                    "partition {}: no imported log to append to in {}",
                    partition.0,
                    dir.display()
                )));
            };
            // The imported log is a single segment; append to the newest one holding data
            // (or the only, empty one).
            let seg = existing
                .iter()
                .rev()
                .find(|(_, p)| fs::metadata(p).is_ok_and(|m| m.len() > 0))
                .map_or_else(|| last.clone(), |(_, p)| p.clone());
            let rep = recover_partition(wal_root, partition, Offset::ZERO)?;
            if rep.records == 0 {
                (seg, None, None, None)
            } else {
                (
                    seg,
                    Some(rep.log_start),
                    Some(rep.leo),
                    rep.epochs.last().map(|(e, _)| *e),
                )
            }
        }
    };

    let mut buf: Vec<u8> = Vec::new();
    let mut records = 0u64;
    'outer: for b in batches {
        for env in &b.envelopes {
            let view = envelope::verify(env).map_err(|e| {
                bad(format!(
                    "partition {}: envelope near offset {}: {e}",
                    partition.0, b.base
                ))
            })?;
            let h = &view.header;
            if h.partition != partition {
                return Err(bad(format!(
                    "envelope of partition {} in the log of partition {}",
                    h.partition.0, partition.0
                )));
            }
            if h.epoch.0 != b.epoch {
                return Err(bad(format!(
                    "partition {}: envelope at {} has epoch {} in a batch of epoch {}",
                    partition.0, h.base, h.epoch.0, b.epoch
                )));
            }
            let end = Offset(h.base.0 + u64::from(h.cmd_count));
            if let Some(n) = next
                && h.base != n
            {
                return Err(bad(format!(
                    "partition {}: gap or overlap: envelope at {} but the log ends at {}",
                    partition.0, h.base, n
                )));
            }
            if h.base >= upto {
                break 'outer;
            }
            if end > upto {
                return Err(bad(format!(
                    "partition {}: cut {} falls inside the batch [{}, {})",
                    partition.0, upto, h.base, end
                )));
            }
            if last_epoch.is_some_and(|e: Epoch| h.epoch < e) {
                return Err(bad(format!(
                    "partition {}: epoch regresses at {}",
                    partition.0, h.base
                )));
            }
            let count = i32::try_from(h.cmd_count).map_err(|_| bad("too many commands"))?;
            let last = h.base.0 + u64::from(h.cmd_count) - 1;
            let rec = WalRecord::new_v2(
                TOPIC.to_string(),
                partition.as_i32(),
                env.to_vec(),
                h.base.as_i64(),
                Offset(last).as_i64(),
                count,
            );
            buf.extend_from_slice(&rec.to_bytes().map_err(|e| bad(e.to_string()))?);
            records += 1;
            first.get_or_insert(h.base);
            next = Some(end);
            last_epoch = Some(h.epoch);
        }
    }
    if let Some(n) = next
        && n != upto
    {
        return Err(bad(format!(
            "partition {}: batches end at {n}, before the cut {upto}",
            partition.0
        )));
    }

    let created = !dir.exists();
    fs::create_dir_all(&dir)?;
    if created {
        // Make the new directories durable in their parents.
        let mut d = dir.as_path();
        while let Some(parent) = d.parent() {
            File::open(parent)?.sync_all()?;
            if parent == wal_root || !parent.starts_with(wal_root) {
                break;
            }
            d = parent;
        }
    }
    let mut f = match mode {
        ImportMode::Create => OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&segment)?,
        ImportMode::Append => OpenOptions::new().append(true).open(&segment)?,
    };
    f.write_all(&buf)?;
    f.sync_all()?;
    drop(f);
    File::open(&dir)?.sync_all()?;
    Ok(ImportReport {
        segment,
        log_start: first,
        leo: next,
        records,
        bytes: buf.len() as u64,
    })
}
