//! Partition log envelope (STOR-01): a 32-byte little-endian header plus length-prefixed commands.
//!
//! Layout (frozen):
//!
//! | off | field     | type | value                                   |
//! |-----|-----------|------|-----------------------------------------|
//! | 0   | magic     | u32  | `b"OSTR"`                               |
//! | 4   | codec_ver | u16  | [`CODEC_VERSION`] (1)                   |
//! | 6   | flags     | u16  | 0                                       |
//! | 8   | partition | u16  |                                         |
//! | 10  | reserved  | u16  | 0                                       |
//! | 12  | epoch     | u32  |                                         |
//! | 16  | base_off  | u64  | offset of the first command             |
//! | 24  | cmd_count | u32  | ≥ 1                                     |
//! | 28  | crc32c    | u32  | `crc32c(header[0..28] ‖ body)`          |
//!
//! Body = `cmd_count × (len u32 LE ‖ bytes)`. The vendored WAL's V2 record checksum is a no-op,
//! so this CRC is the only integrity check on command bytes.

use bytes::Bytes;

use crate::CODEC_VERSION;
use crate::ids::{Epoch, Offset, PartitionId};

/// `b"OSTR"` read as a little-endian u32.
pub const MAGIC: u32 = u32::from_le_bytes(*b"OSTR");
/// Header length in bytes.
pub const HEADER_LEN: usize = 32;
/// Default upper bound for one encoded envelope (4 MiB).
pub const DEFAULT_MAX_ENVELOPE_BYTES: usize = 4 << 20;

const CRC_OFF: usize = 28;

/// Envelope verification failures.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EnvelopeError {
    /// Fewer than [`HEADER_LEN`] bytes.
    #[error("envelope shorter than the {HEADER_LEN}-byte header")]
    TooShort,
    /// Magic is not `OSTR`.
    #[error("bad envelope magic {0:#010x}")]
    BadMagic(u32),
    /// Unsupported codec version.
    #[error("unknown envelope codec version {0}")]
    UnknownCodec(u16),
    /// Flags must be zero in codec v1.
    #[error("non-zero envelope flags {0:#06x}")]
    NonZeroFlags(u16),
    /// Reserved field must be zero.
    #[error("non-zero reserved field {0:#06x}")]
    NonZeroReserved(u16),
    /// CRC32C over header[0..28] ‖ body does not match.
    #[error("envelope crc mismatch: stored {stored:#010x}, computed {computed:#010x}")]
    CrcMismatch {
        /// CRC stored in the header.
        stored: u32,
        /// CRC computed over the bytes.
        computed: u32,
    },
    /// A length prefix or command overruns the body.
    #[error("envelope body malformed")]
    BodyMalformed,
    /// The body ended cleanly after fewer commands than the header claims.
    #[error("envelope command count mismatch: header {header}, parsed {parsed}")]
    CountMismatch {
        /// Count in the header.
        header: u32,
        /// Commands actually parsed.
        parsed: u32,
    },
    /// Bytes left over after `cmd_count` commands.
    #[error("{0} trailing bytes after envelope commands")]
    TrailingBytes(usize),
    /// `cmd_count == 0`.
    #[error("empty envelope")]
    Empty,
}

/// Decoded header fields.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct EnvelopeHeader {
    /// Codec version (currently always 1).
    pub codec_ver: u16,
    /// Owning partition.
    pub partition: PartitionId,
    /// Leader epoch that wrote the envelope.
    pub epoch: Epoch,
    /// Offset of the first command.
    pub base: Offset,
    /// Number of commands (≥ 1).
    pub cmd_count: u32,
}

/// A verified envelope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvelopeView {
    /// Header fields.
    pub header: EnvelopeHeader,
    /// Command bytes in offset order.
    pub commands: Vec<Bytes>,
}

impl EnvelopeView {
    /// Offset of the last command (`base + cmd_count - 1`).
    pub fn last(&self) -> Offset {
        self.header.base + (u64::from(self.header.cmd_count) - 1)
    }

    /// Commands paired with their offsets.
    pub fn commands_with_offsets(&self) -> impl Iterator<Item = (Offset, &Bytes)> {
        let base = self.header.base;
        self.commands
            .iter()
            .enumerate()
            .map(move |(i, c)| (base + i as u64, c))
    }
}

/// Encodes commands into an envelope. `cmds` must be non-empty (debug-asserted; an empty envelope
/// fails [`verify`] with [`EnvelopeError::Empty`]).
///
/// # Panics
/// If there are more than `u32::MAX` commands or a command exceeds `u32::MAX` bytes.
pub fn encode(p: PartitionId, epoch: Epoch, base: Offset, cmds: &[Bytes]) -> Vec<u8> {
    debug_assert!(!cmds.is_empty(), "envelope must carry at least one command");
    let body: usize = cmds.iter().map(|c| 4 + c.len()).sum();
    let count = u32::try_from(cmds.len()).expect("too many commands for one envelope");
    let mut b = Vec::with_capacity(HEADER_LEN + body);
    b.extend_from_slice(&MAGIC.to_le_bytes());
    b.extend_from_slice(&CODEC_VERSION.to_le_bytes());
    b.extend_from_slice(&0u16.to_le_bytes()); // flags
    b.extend_from_slice(&p.0.to_le_bytes());
    b.extend_from_slice(&0u16.to_le_bytes()); // reserved
    b.extend_from_slice(&epoch.0.to_le_bytes());
    b.extend_from_slice(&base.0.to_le_bytes());
    b.extend_from_slice(&count.to_le_bytes());
    b.extend_from_slice(&[0u8; 4]); // crc placeholder
    for c in cmds {
        let len = u32::try_from(c.len()).expect("command exceeds u32::MAX bytes");
        b.extend_from_slice(&len.to_le_bytes());
        b.extend_from_slice(c);
    }
    let crc = compute_crc(&b);
    b[CRC_OFF..HEADER_LEN].copy_from_slice(&crc.to_le_bytes());
    b
}

fn compute_crc(b: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&b[..CRC_OFF]), &b[HEADER_LEN..])
}

fn u16_at(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes([b[off], b[off + 1]])
}

fn u32_at(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().expect("4 bytes"))
}

fn u64_at(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().expect("8 bytes"))
}

/// Checks length, magic, codec version, flags, reserved and CRC; returns the header without
/// parsing the body.
pub fn peek_header(bytes: &[u8]) -> Result<EnvelopeHeader, EnvelopeError> {
    if bytes.len() < HEADER_LEN {
        return Err(EnvelopeError::TooShort);
    }
    let magic = u32_at(bytes, 0);
    if magic != MAGIC {
        return Err(EnvelopeError::BadMagic(magic));
    }
    let codec_ver = u16_at(bytes, 4);
    if codec_ver != CODEC_VERSION {
        return Err(EnvelopeError::UnknownCodec(codec_ver));
    }
    let flags = u16_at(bytes, 6);
    if flags != 0 {
        return Err(EnvelopeError::NonZeroFlags(flags));
    }
    let reserved = u16_at(bytes, 10);
    if reserved != 0 {
        return Err(EnvelopeError::NonZeroReserved(reserved));
    }
    let stored = u32_at(bytes, CRC_OFF);
    let computed = compute_crc(bytes);
    if stored != computed {
        return Err(EnvelopeError::CrcMismatch { stored, computed });
    }
    Ok(EnvelopeHeader {
        codec_ver,
        partition: PartitionId(u16_at(bytes, 8)),
        epoch: Epoch(u32_at(bytes, 12)),
        base: Offset(u64_at(bytes, 16)),
        cmd_count: u32_at(bytes, 24),
    })
}

/// Fully verifies an envelope: [`peek_header`] checks, then exactly `cmd_count` length-prefixed
/// commands with no trailing bytes.
pub fn verify(bytes: &[u8]) -> Result<EnvelopeView, EnvelopeError> {
    let header = peek_header(bytes)?;
    if header.cmd_count == 0 {
        return Err(EnvelopeError::Empty);
    }
    let mut rest = &bytes[HEADER_LEN..];
    // Every command takes at least 4 bytes, which bounds the allocation for hostile counts.
    let mut commands = Vec::with_capacity((header.cmd_count as usize).min(rest.len() / 4));
    for parsed in 0..header.cmd_count {
        if rest.is_empty() {
            return Err(EnvelopeError::CountMismatch {
                header: header.cmd_count,
                parsed,
            });
        }
        if rest.len() < 4 {
            return Err(EnvelopeError::BodyMalformed);
        }
        let len = u32_at(rest, 0) as usize;
        let after = &rest[4..];
        if after.len() < len {
            return Err(EnvelopeError::BodyMalformed);
        }
        commands.push(Bytes::copy_from_slice(&after[..len]));
        rest = &after[len..];
    }
    if !rest.is_empty() {
        return Err(EnvelopeError::TrailingBytes(rest.len()));
    }
    Ok(EnvelopeView { header, commands })
}
