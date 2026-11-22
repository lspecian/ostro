//! Partition ids, partition counts, log offsets, leader epochs, attempt tokens and run ids.
//!
//! `RunId` is 18 bytes: `partition u16 BE ‖ uuid (16)`, so the owning partition is a pure byte
//! read and routing an existing run never needs a lookup (STOR-09). Its string form is lowercase
//! Crockford base32 without padding (29 characters).

use std::fmt;
use std::num::NonZeroU16;
use std::ops::{Add, Sub};
use std::str::FromStr;

/// A partition index in `0..P`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId(pub u16);

impl PartitionId {
    /// The partition as the `i32` used by the vendored WAL.
    pub fn as_i32(self) -> i32 {
        i32::from(self.0)
    }

    /// The partition as a vector index.
    pub fn index(self) -> usize {
        usize::from(self.0)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// The fixed, non-zero partition count `P` of a cluster.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct PartitionCount(NonZeroU16);

impl PartitionCount {
    /// Default `P` for a single node.
    pub const SINGLE_NODE_DEFAULT: u16 = 16;
    /// Default `P` for a cluster.
    pub const CLUSTER_DEFAULT: u16 = 64;

    /// `None` when `n == 0`.
    pub fn new(n: u16) -> Option<Self> {
        NonZeroU16::new(n).map(Self)
    }

    /// The count.
    pub fn get(self) -> u16 {
        self.0.get()
    }

    /// Every partition id, `0..P`, in order.
    pub fn ids(self) -> impl Iterator<Item = PartitionId> {
        (0..self.get()).map(PartitionId)
    }

    /// Whether `p < P`.
    pub fn contains(self, p: PartitionId) -> bool {
        p.0 < self.get()
    }
}

/// A partition log offset (one per command).
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Offset(pub u64);

impl Offset {
    /// Offset 0.
    pub const ZERO: Offset = Offset(0);

    /// The following offset.
    pub fn next(self) -> Offset {
        Offset(self.0 + 1)
    }

    /// The offset as the `i64` used by the vendored WAL.
    ///
    /// # Panics
    /// If the offset exceeds `i64::MAX` (unreachable in practice).
    pub fn as_i64(self) -> i64 {
        i64::try_from(self.0).expect("offset exceeds i64::MAX")
    }

    /// `None` for negative values.
    pub fn from_i64(v: i64) -> Option<Offset> {
        u64::try_from(v).ok().map(Offset)
    }
}

impl Add<u64> for Offset {
    type Output = Offset;
    fn add(self, rhs: u64) -> Offset {
        Offset(self.0 + rhs)
    }
}

impl Sub for Offset {
    type Output = u64;
    fn sub(self, rhs: Offset) -> u64 {
        self.0 - rhs.0
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A partition leader epoch.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Epoch(pub u32);

impl fmt::Display for Epoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A fencing token for one run attempt. Ordered lexicographically by `(epoch, n)`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AttemptToken {
    /// The epoch that granted the attempt (compared first).
    pub epoch: Epoch,
    /// Attempt counter within the epoch.
    pub n: u32,
}

/// Errors parsing ids.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// Wrong number of bytes for a `RunId`.
    #[error("run id must be 18 bytes, got {0}")]
    BadLength(usize),
    /// Wrong number of characters for a `RunId` string.
    #[error("run id string must be {RUN_ID_STR_LEN} characters, got {0}")]
    BadStringLength(usize),
    /// A character outside the Crockford base32 alphabet.
    #[error("invalid base32 character {0:?}")]
    BadChar(char),
    /// Non-zero padding bits in the final character.
    #[error("non-zero padding bits in run id string")]
    NonCanonical,
}

const RUN_ID_STR_LEN: usize = (RunId::LEN * 8).div_ceil(5);

/// Crockford base32 alphabet (lowercase).
const ALPHABET: &[u8; 32] = b"0123456789abcdefghjkmnpqrstvwxyz";

/// A run id: `partition u16 BE ‖ uuid (16)`.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunId([u8; 18]);

impl RunId {
    /// Length in bytes.
    pub const LEN: usize = 18;

    /// Builds a run id for partition `p` from `uuid`.
    pub fn new(p: PartitionId, uuid: uuid::Uuid) -> Self {
        let mut b = [0u8; 18];
        b[..2].copy_from_slice(&p.0.to_be_bytes());
        b[2..].copy_from_slice(uuid.as_bytes());
        Self(b)
    }

    /// A fresh UUIDv7 run id. Called at submit time only, never during apply.
    pub fn new_v7(p: PartitionId) -> Self {
        Self::new(p, uuid::Uuid::now_v7())
    }

    /// Parses the 18-byte form.
    pub fn from_bytes(b: &[u8]) -> Result<Self, IdError> {
        <[u8; 18]>::try_from(b)
            .map(Self)
            .map_err(|_| IdError::BadLength(b.len()))
    }

    /// The 18-byte form.
    pub fn as_bytes(&self) -> &[u8; 18] {
        &self.0
    }

    /// The owning partition (a pure byte read).
    pub fn partition(&self) -> PartitionId {
        PartitionId(u16::from_be_bytes([self.0[0], self.0[1]]))
    }

    /// The embedded uuid.
    pub fn uuid(&self) -> uuid::Uuid {
        let mut u = [0u8; 16];
        u.copy_from_slice(&self.0[2..]);
        uuid::Uuid::from_bytes(u)
    }
}

/// Domain separator of the child run id digest (D-P9-7).
const CHILD_ID_DOMAIN: &[u8] = b"ostro/child/v1";

/// The uuid part of the deterministic child run id of `(parent, op)` (D-P9-7, plan 09-08):
/// `d = sha256("ostro/child/v1" ‖ parent (18 bytes) ‖ op u64 BE)`, uuid = `d[..16]` with the
/// RFC 9562 version nibble 8 and variant `10`. The server's `children::child_run_id` adds the
/// partition (`u16::from_be_bytes(d[16..18]) % P`); apply matches a continue-as-new
/// successor's completion to the parent's children row by this uuid (D-P9-27), with no
/// partition count needed. Pure.
pub fn child_run_uuid(parent: &RunId, op: u64) -> uuid::Uuid {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(CHILD_ID_DOMAIN);
    h.update(parent.as_bytes());
    h.update(op.to_be_bytes());
    let d = h.finalize();
    let mut u = [0u8; 16];
    u.copy_from_slice(&d[..16]);
    u[6] = (u[6] & 0x0f) | 0x80;
    u[8] = (u[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(u)
}

fn decode_char(c: u8) -> Option<u8> {
    let c = c.to_ascii_lowercase();
    let c = match c {
        b'i' | b'l' => b'1',
        b'o' => b'0',
        other => other,
    };
    ALPHABET.iter().position(|&a| a == c).map(|i| i as u8)
}

impl fmt::Display for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = [0u8; RUN_ID_STR_LEN];
        let mut acc: u32 = 0;
        let mut bits = 0u32;
        let mut i = 0;
        for &byte in &self.0 {
            acc = (acc << 8) | u32::from(byte);
            bits += 8;
            while bits >= 5 {
                bits -= 5;
                out[i] = ALPHABET[((acc >> bits) & 31) as usize];
                i += 1;
            }
        }
        if bits > 0 {
            out[i] = ALPHABET[((acc << (5 - bits)) & 31) as usize];
            i += 1;
        }
        debug_assert_eq!(i, RUN_ID_STR_LEN);
        // ALPHABET is ASCII.
        f.write_str(std::str::from_utf8(&out).expect("ascii"))
    }
}

impl fmt::Debug for RunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RunId({self})")
    }
}

impl FromStr for RunId {
    type Err = IdError;

    fn from_str(s: &str) -> Result<Self, IdError> {
        let raw = s.as_bytes();
        if raw.len() != RUN_ID_STR_LEN {
            return Err(IdError::BadStringLength(s.chars().count()));
        }
        let mut out = [0u8; 18];
        let mut acc: u32 = 0;
        let mut bits = 0u32;
        let mut i = 0;
        for (idx, &c) in raw.iter().enumerate() {
            let v = decode_char(c).ok_or_else(|| {
                IdError::BadChar(
                    s[idx..]
                        .chars()
                        .next()
                        .unwrap_or(char::REPLACEMENT_CHARACTER),
                )
            })?;
            acc = (acc << 5) | u32::from(v);
            bits += 5;
            if bits >= 8 {
                bits -= 8;
                if i >= out.len() {
                    return Err(IdError::BadLength(i + 1));
                }
                out[i] = (acc >> bits) as u8;
                i += 1;
            }
            acc &= (1 << bits) - 1;
        }
        if i != RunId::LEN {
            return Err(IdError::BadLength(i));
        }
        if acc != 0 {
            return Err(IdError::NonCanonical);
        }
        Ok(RunId(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_len_is_29() {
        assert_eq!(RUN_ID_STR_LEN, 29);
    }

    /// Golden vectors equal to 09-05's `child_run_id` uuid part (parent = partition 3 ‖
    /// 16 × 0x11): op 7 → `0071t7sbx8jw72bzhfyq2k5jaj6zw` at P 16.
    #[test]
    fn child_run_uuid_golden() {
        let mut b = [0x11u8; 18];
        b[..2].copy_from_slice(&3u16.to_be_bytes());
        let parent = RunId::from_bytes(&b).expect("parent");
        for (op, s) in [
            (7u64, "0071t7sbx8jw72bzhfyq2k5jaj6zw"),
            (8, "002yhpcepx40309vk1cdaptr0q16j"),
            (7, "0001t7sbx8jw72bzhfyq2k5jaj6zw"),
        ] {
            let want: RunId = s.parse().expect("golden");
            let u = child_run_uuid(&parent, op);
            assert_eq!(u, want.uuid(), "op {op}");
            assert_eq!(u.as_bytes()[6] >> 4, 8);
            assert_eq!(u.as_bytes()[8] >> 6, 0b10);
        }
    }
}
