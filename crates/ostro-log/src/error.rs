//! Errors returned by the partition log.

use std::path::PathBuf;

use ostro_types::{Epoch, Offset};

use crate::verify::Corruption;

/// Partition log failure.
#[derive(Debug, thiserror::Error)]
pub enum LogError {
    /// A record failed verification outside a torn tail. The open fails; nothing is skipped.
    /// `first`/`last` are the record's offsets when its frame parsed, otherwise the expected
    /// base and `-1`.
    #[error(
        "partition {partition} corrupt in {} at byte {byte_pos} (offsets {first}..={last}): {reason}",
        segment.display()
    )]
    Corrupt {
        /// Partition id.
        partition: u16,
        /// Segment file holding the bad record.
        segment: PathBuf,
        /// Byte position of the bad record's start inside `segment`.
        byte_pos: u64,
        /// First offset of the bad record (or the expected base when unparseable).
        first: i64,
        /// Last offset of the bad record (`-1` when unknown).
        last: i64,
        /// What failed.
        reason: Corruption,
    },
    /// The append carried an epoch older than the current one.
    #[error("stale epoch: current {current:?}")]
    StaleEpoch {
        /// Current epoch.
        current: Epoch,
    },
    /// The partition is failed (fail-stop) and refuses work until reopened.
    #[error("partition failed: {0}")]
    Failed(String),
    /// Filesystem error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Error from the vendored WAL.
    #[error("wal: {0}")]
    Wal(String),
    /// The log is not open for business yet.
    #[error("not ready")]
    NotReady,
    /// Offset outside `[log_start, leo)`.
    #[error("offset {0:?} out of range")]
    OutOfRange(Offset),
    /// The log was truncated underneath the caller.
    #[error("truncated")]
    Truncated,
    /// Phase 12 (D-P12-5): the published ISR is smaller than `min_insync`; appends are
    /// refused and the HW is frozen.
    #[error("not enough replicas: isr {isr} < min_insync {min}")]
    NotEnoughReplicas {
        /// Published ISR size.
        isr: usize,
        /// Required minimum.
        min: usize,
    },
    /// Phase 12: this replica does not lead the partition (`leader` when known).
    #[error("not leader (leader: {leader:?})")]
    NotLeader {
        /// The current leader, when known.
        leader: Option<u64>,
    },
}
