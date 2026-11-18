//! Verified read of one WAL record (frame CRC + envelope).
//!
//! The vendored WAL does not protect V2 records: `WalRecord::verify_checksum` is a no-op
//! for V2 (research W1) and its readers stop silently or byte-skip at bad bytes (W2/W3).
//! Every Ostro read goes through [`verify_record`], which checks the frame CRC itself,
//! then the envelope CRC32C, then that the header and the WAL trailer agree.

use bytes::Bytes;
use ostro_types::envelope::{self, EnvelopeError};
use ostro_types::{Epoch, Offset, PartitionId};
use ostro_wal::WalRecord;

/// Why a record failed verification.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Corruption {
    /// The WAL frame CRC32 does not match the record bytes.
    #[error("wal frame crc mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    FrameCrc {
        /// CRC stored in the frame.
        stored: u32,
        /// CRC computed over the parsed fields.
        computed: u32,
    },
    /// The record is not a V2 record.
    #[error("not a V2 wal record")]
    NotV2,
    /// The record belongs to another partition.
    #[error("record for partition {partition} (base {base}) in the wrong partition")]
    Misplaced {
        /// Partition named by the record.
        partition: i32,
        /// Base offset named by the record.
        base: i64,
    },
    /// The envelope failed verification.
    #[error("envelope: {0}")]
    Envelope(EnvelopeError),
    /// The envelope header and the WAL trailer disagree (base, count or last offset).
    #[error("envelope header and wal trailer disagree")]
    TrailerMismatch,
    /// The record does not start at the expected offset.
    #[error("offset gap: expected base {expected}, found {found}")]
    Gap {
        /// Expected base (previous last + 1).
        expected: u64,
        /// Base found.
        found: u64,
    },
    /// The epoch went backwards.
    #[error("epoch regression: {prev} then {found}")]
    EpochRegression {
        /// Previous batch's epoch.
        prev: u32,
        /// This batch's epoch.
        found: u32,
    },
    /// The frame header is bad or the record does not parse.
    #[error("unparseable wal record")]
    Unparseable,
    /// The record extends past the end of the file.
    #[error("incomplete record at end of file")]
    Incomplete,
}

/// A record that passed [`verify_record`].
#[derive(Debug, Clone)]
pub struct VerifiedBatch {
    /// Leader epoch that wrote the batch.
    pub epoch: Epoch,
    /// Offset of the first command.
    pub base: Offset,
    /// Commands in offset order (never empty).
    pub commands: Vec<Bytes>,
}

impl VerifiedBatch {
    /// Offset of the last command.
    pub fn last(&self) -> Offset {
        self.base + (self.commands.len() as u64 - 1)
    }

    /// Offset just past the batch (`last + 1`).
    pub fn end(&self) -> Offset {
        self.base + self.commands.len() as u64
    }

    /// Commands paired with their offsets.
    pub fn commands_with_offsets(&self) -> impl Iterator<Item = (Offset, &Bytes)> {
        let base = self.base;
        self.commands
            .iter()
            .enumerate()
            .map(move |(i, c)| (base + i as u64, c))
    }
}

/// Verify one WAL record that must hold partition `pid`'s batch starting at `expect_base`.
///
/// Order: frame CRC (W1) → V2 → frame partition → frame base (gap) → envelope CRC32C and
/// body → envelope partition → envelope header vs WAL trailer.
pub fn verify_record(
    rec: &WalRecord,
    expect_base: Offset,
    pid: PartitionId,
) -> Result<VerifiedBatch, Corruption> {
    let stored = rec.get_crc32();
    let computed = rec.calculate_checksum();
    if stored != computed {
        return Err(Corruption::FrameCrc { stored, computed });
    }
    let WalRecord::V2 {
        canonical_data,
        base_offset,
        last_offset,
        record_count,
        partition,
        ..
    } = rec
    else {
        return Err(Corruption::NotV2);
    };
    if *partition != pid.as_i32() {
        return Err(Corruption::Misplaced {
            partition: *partition,
            base: *base_offset,
        });
    }
    if *base_offset != expect_base.as_i64() {
        return match Offset::from_i64(*base_offset) {
            Some(found) => Err(Corruption::Gap {
                expected: expect_base.0,
                found: found.0,
            }),
            None => Err(Corruption::Misplaced {
                partition: *partition,
                base: *base_offset,
            }),
        };
    }
    let env = envelope::verify(canonical_data).map_err(Corruption::Envelope)?;
    if env.header.partition != pid {
        return Err(Corruption::Misplaced {
            partition: i32::from(env.header.partition.0),
            base: *base_offset,
        });
    }
    let count_matches = i32::try_from(env.header.cmd_count).is_ok_and(|c| c == *record_count);
    let last_matches = i64::from(*record_count)
        .checked_sub(1)
        .and_then(|n| base_offset.checked_add(n))
        == Some(*last_offset);
    if env.header.base != expect_base || !count_matches || !last_matches {
        return Err(Corruption::TrailerMismatch);
    }
    Ok(VerifiedBatch {
        epoch: env.header.epoch,
        base: env.header.base,
        commands: env.commands,
    })
}
