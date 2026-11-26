//! The audit hash chain (D-P14-11, 14-04).
//!
//! Every record carries `prev_hash` (the previous record's `hash`) and
//! `hash = hex(sha256(prev_hash_bytes ‖ canonical))`, where `canonical` is the record's
//! canonical JSON without `hash` ([`crate::audit::journal::Record::canonical`]) and
//! `prev_hash_bytes` are the 32 raw bytes behind the previous hex hash. The very first
//! record of a node's journal chains from the genesis hash
//! `sha256("ostro-audit-genesis" ‖ node_be64 ‖ incarnation_be64)` of the node id and the
//! incarnation that wrote it.

use sha2::{Digest, Sha256};

/// A raw chain hash.
pub type Hash = [u8; 32];

/// The genesis domain separator.
pub const GENESIS_TAG: &[u8] = b"ostro-audit-genesis";

/// The `prev_hash` of a node's first record.
pub fn genesis(node: u64, incarnation: u64) -> Hash {
    let mut h = Sha256::new();
    h.update(GENESIS_TAG);
    h.update(node.to_be_bytes());
    h.update(incarnation.to_be_bytes());
    h.finalize().into()
}

/// `sha256(prev ‖ canonical)`.
pub fn link(prev: &Hash, canonical: &[u8]) -> Hash {
    let mut h = Sha256::new();
    h.update(prev);
    h.update(canonical);
    h.finalize().into()
}

/// Lowercase hex of a hash (64 characters).
pub fn to_hex(h: &Hash) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in h {
        s.push(char::from(DIGITS[usize::from(b >> 4)]));
        s.push(char::from(DIGITS[usize::from(b & 0x0f)]));
    }
    s
}

/// Parse 64 lowercase hex characters (uppercase is rejected: the encoding is canonical).
pub fn from_hex(s: &str) -> Option<Hash> {
    let b = s.as_bytes();
    if b.len() != 64 {
        return None;
    }
    let nib = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = (nib(b[2 * i])? << 4) | nib(b[2 * i + 1])?;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_round_trips_and_is_canonical() {
        let h = link(&genesis(1, 2), b"x");
        let s = to_hex(&h);
        assert_eq!(s.len(), 64);
        assert_eq!(from_hex(&s), Some(h));
        assert_eq!(from_hex(&s.to_uppercase()), None);
        assert_eq!(from_hex(&s[..63]), None);
    }

    #[test]
    fn genesis_depends_on_node_and_incarnation() {
        assert_ne!(genesis(1, 2), genesis(2, 1));
        assert_ne!(genesis(1, 2), genesis(1, 3));
        assert_eq!(genesis(7, 9), genesis(7, 9));
    }
}
