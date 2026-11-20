//! OSEG v1: uploader-sealed, hash-chained chunks of raw OSTR envelopes (P8-D3, plan 08-13).
//!
//! A chunk holds the stored envelopes of one partition's offsets `[base, end)`, all written
//! under one leader epoch, byte for byte (the envelope format is frozen). The uploader closes
//! ("seals") a chunk at a tick and never appends to it afterwards.
//!
//! ```text
//! header (64 B, little endian):
//!   magic "OSEG" | ver u16 = 1 | partition u16 | epoch u32 | base u64 | end u64 | count u32
//!   | prev_chunk_sha256 [32]
//! body:    (u32 LE len ‖ OSTR envelope) × count
//! trailer: sha256(header ‖ body)
//! ```
//!
//! The chunk's identity ([`chunk_sha`]) is the sha256 of the whole object (header, body and
//! trailer). Each chunk's `prev_chunk_sha256` is the identity of the previous chunk of the
//! same partition chain; the first chunk of a chain has `[0; 32]`. Manifests name the head of
//! each chain (`last_chunk_key` + `last_chunk_sha256`); [`walk_back`] follows the links back
//! to the chain start, so forked chunks under `seg/` that are not on the chain are ignored.
//!
//! Remote key: `p/<pppp>/seg/<epoch 10d>-<base 20d>-<end 20d>.oseg` ([`chunk_key`]).

use std::sync::Arc;

use ostro_objstore::ObjectStore;
use ostro_types::envelope;
use sha2::{Digest as _, Sha256};

/// `"OSEG"`.
pub const MAGIC: [u8; 4] = *b"OSEG";
/// Chunk format version.
pub const VERSION: u16 = 1;
/// Header length.
pub const HEADER_LEN: usize = 64;
/// Trailer length (sha256).
pub const TRAILER_LEN: usize = 32;
/// The uploader's chunk size cap (P8-D3: at most 8 MiB per chunk).
pub const MAX_CHUNK_BYTES: usize = 8 << 20;

/// Chunk decoding / chain verification failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ChunkError {
    /// The object is not a well-formed OSEG v1 chunk.
    #[error("malformed chunk: {0}")]
    Malformed(String),
    /// The trailer does not match `sha256(header ‖ body)`.
    #[error("chunk trailer sha256 mismatch")]
    TrailerMismatch,
    /// An envelope failed verification (CRC32C, structure) or does not fit the header.
    #[error("chunk envelope {index}: {reason}")]
    Envelope {
        /// Index of the envelope in the chunk.
        index: usize,
        /// What is wrong.
        reason: String,
    },
    /// The chain is broken (prev link, contiguity, epochs, start).
    #[error("chunk chain: {0}")]
    Chain(String),
    /// A chunk named by the chain is missing from the store.
    #[error("chunk {0} missing")]
    Missing(String),
    /// Object store failure.
    #[error("object store: {0}")]
    Store(String),
}

/// Decoded header fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChunkHeader {
    /// Partition.
    pub partition: u16,
    /// Leader epoch of every envelope in the chunk.
    pub epoch: u32,
    /// First offset.
    pub base: u64,
    /// Exclusive end offset.
    pub end: u64,
    /// Number of envelopes.
    pub count: u32,
    /// Identity of the previous chunk of the chain (`[0; 32]` for the first).
    pub prev_chunk_sha256: [u8; 32],
}

/// A decoded, verified chunk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Chunk {
    /// Header.
    pub header: ChunkHeader,
    /// The OSTR envelopes, in offset order (byte-identical to the log's stored envelopes).
    pub envelopes: Vec<Vec<u8>>,
    /// Identity: sha256 of the whole object.
    pub sha256: [u8; 32],
}

/// `sha256` of a whole chunk object (its identity, used by `prev_chunk_sha256` and manifests).
pub fn chunk_sha(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// `p/<pppp>/seg/` of partition `p`.
pub fn seg_prefix(partition: u16) -> String {
    format!("p/{partition:04}/seg/")
}

/// `p/<pppp>/seg/<epoch 10d>-<base 20d>-<end 20d>.oseg`.
pub fn chunk_key(partition: u16, epoch: u32, base: u64, end: u64) -> String {
    format!(
        "{}{epoch:010}-{base:020}-{end:020}.oseg",
        seg_prefix(partition)
    )
}

/// Parse a chunk key back into `(partition, epoch, base, end)`.
pub fn parse_chunk_key(key: &str) -> Option<(u16, u32, u64, u64)> {
    let rest = key.strip_prefix("p/")?;
    let (p, rest) = rest.split_once('/')?;
    let name = rest.strip_prefix("seg/")?.strip_suffix(".oseg")?;
    if p.len() != 4 || !p.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut it = name.split('-');
    let (e, b, en) = (it.next()?, it.next()?, it.next()?);
    if it.next().is_some() || e.len() != 10 || b.len() != 20 || en.len() != 20 {
        return None;
    }
    let digits = |s: &str| s.bytes().all(|c| c.is_ascii_digit());
    if !(digits(e) && digits(b) && digits(en)) {
        return None;
    }
    Some((
        p.parse().ok()?,
        e.parse().ok()?,
        b.parse().ok()?,
        en.parse().ok()?,
    ))
}

/// Encode a chunk from stored envelopes. `envelopes` must be non-empty, contiguous, of
/// `partition` and written under `epoch`; `[base, end)` is derived from their headers.
pub fn encode(
    partition: u16,
    epoch: u32,
    prev_chunk_sha256: [u8; 32],
    envelopes: &[&[u8]],
) -> Result<(ChunkHeader, Vec<u8>), ChunkError> {
    let first = envelopes
        .first()
        .ok_or_else(|| ChunkError::Malformed("a chunk needs at least one envelope".into()))?;
    let base = envelope::peek_header(first)
        .map_err(|e| ChunkError::Envelope {
            index: 0,
            reason: e.to_string(),
        })?
        .base
        .0;
    let end = check_envelopes(partition, epoch, base, envelopes.iter().copied())?;
    let count = u32::try_from(envelopes.len())
        .map_err(|_| ChunkError::Malformed("too many envelopes".into()))?;
    let header = ChunkHeader {
        partition,
        epoch,
        base,
        end,
        count,
        prev_chunk_sha256,
    };
    let body: usize = envelopes.iter().map(|e| 4 + e.len()).sum();
    let mut out = Vec::with_capacity(HEADER_LEN + body + TRAILER_LEN);
    write_header(&mut out, &header);
    for e in envelopes {
        let len = u32::try_from(e.len())
            .map_err(|_| ChunkError::Malformed("envelope exceeds u32::MAX bytes".into()))?;
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(e);
    }
    let trailer: [u8; 32] = Sha256::digest(&out).into();
    out.extend_from_slice(&trailer);
    Ok((header, out))
}

fn write_header(out: &mut Vec<u8>, h: &ChunkHeader) {
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&h.partition.to_le_bytes());
    out.extend_from_slice(&h.epoch.to_le_bytes());
    out.extend_from_slice(&h.base.to_le_bytes());
    out.extend_from_slice(&h.end.to_le_bytes());
    out.extend_from_slice(&h.count.to_le_bytes());
    out.extend_from_slice(&h.prev_chunk_sha256);
}

/// Verify every envelope (CRC32C and structure through the ostro-types decoder), its
/// partition and epoch, and contiguity from `base`. Returns the exclusive end offset.
fn check_envelopes<'a>(
    partition: u16,
    epoch: u32,
    base: u64,
    envelopes: impl Iterator<Item = &'a [u8]>,
) -> Result<u64, ChunkError> {
    let mut next = base;
    for (index, e) in envelopes.enumerate() {
        let bad = |reason: String| ChunkError::Envelope { index, reason };
        let view = envelope::verify(e).map_err(|err| bad(err.to_string()))?;
        let h = view.header;
        if h.partition.0 != partition {
            return Err(bad(format!("partition {} != {partition}", h.partition.0)));
        }
        if h.epoch.0 != epoch {
            return Err(bad(format!("epoch {} != {epoch}", h.epoch.0)));
        }
        if h.base.0 != next {
            return Err(bad(format!("base {} != expected {next}", h.base.0)));
        }
        next = next
            .checked_add(u64::from(h.cmd_count))
            .ok_or_else(|| bad("offset overflow".into()))?;
    }
    Ok(next)
}

/// Read only the header (length, magic and version checked; nothing else verified).
pub fn peek_header(bytes: &[u8]) -> Result<ChunkHeader, ChunkError> {
    if bytes.len() < HEADER_LEN + TRAILER_LEN {
        return Err(ChunkError::Malformed(format!(
            "{} bytes is shorter than header + trailer",
            bytes.len()
        )));
    }
    if bytes[0..4] != MAGIC {
        return Err(ChunkError::Malformed("bad magic".into()));
    }
    let u16_at = |o: usize| u16::from_le_bytes([bytes[o], bytes[o + 1]]);
    let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap_or([0; 4]));
    let u64_at = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().unwrap_or([0; 8]));
    let ver = u16_at(4);
    if ver != VERSION {
        return Err(ChunkError::Malformed(format!("unknown version {ver}")));
    }
    let mut prev = [0u8; 32];
    prev.copy_from_slice(&bytes[32..64]);
    Ok(ChunkHeader {
        partition: u16_at(6),
        epoch: u32_at(8),
        base: u64_at(12),
        end: u64_at(20),
        count: u32_at(28),
        prev_chunk_sha256: prev,
    })
}

/// Fully decode and verify one chunk: header, trailer, every envelope (CRC32C via the
/// ostro-types decoder, partition, epoch, contiguity) and `[base, end)`.
pub fn decode(bytes: &[u8]) -> Result<Chunk, ChunkError> {
    let header = peek_header(bytes)?;
    let split = bytes.len() - TRAILER_LEN;
    let trailer: [u8; 32] = Sha256::digest(&bytes[..split]).into();
    if trailer[..] != bytes[split..] {
        return Err(ChunkError::TrailerMismatch);
    }
    if header.count == 0 || header.end <= header.base {
        return Err(ChunkError::Malformed(format!(
            "empty range [{}, {}) / count {}",
            header.base, header.end, header.count
        )));
    }
    let mut rest = &bytes[HEADER_LEN..split];
    let mut envelopes = Vec::with_capacity((header.count as usize).min(rest.len() / 4));
    for i in 0..header.count {
        if rest.len() < 4 {
            return Err(ChunkError::Malformed(format!(
                "body ends before envelope {i}"
            )));
        }
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap_or([0; 4])) as usize;
        rest = &rest[4..];
        if rest.len() < len {
            return Err(ChunkError::Malformed(format!(
                "envelope {i} overruns the body"
            )));
        }
        envelopes.push(rest[..len].to_vec());
        rest = &rest[len..];
    }
    if !rest.is_empty() {
        return Err(ChunkError::Malformed(format!(
            "{} trailing body bytes",
            rest.len()
        )));
    }
    let end = check_envelopes(
        header.partition,
        header.epoch,
        header.base,
        envelopes.iter().map(Vec::as_slice),
    )?;
    if end != header.end {
        return Err(ChunkError::Malformed(format!(
            "envelopes end at {end}, header says {}",
            header.end
        )));
    }
    Ok(Chunk {
        header,
        envelopes,
        sha256: chunk_sha(bytes),
    })
}

/// Where a chain (or a verified suffix of one) starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ChainStart {
    /// The first chunk's `base`.
    pub base: u64,
    /// The first chunk's `prev_chunk_sha256`: `[0; 32]` for a chain that starts at its
    /// beginning, else the identity of the (deleted or untrusted) chunk before it.
    pub prev_sha: [u8; 32],
}

/// Verify a chain of decoded chunks: the first starts at `start` (base and prev link), each
/// next chunk links to the previous one's identity, `base == prev.end`, the epochs never
/// decrease and all chunks belong to one partition. (Each chunk's trailer and envelopes were
/// verified by [`decode`].)
pub fn verify_chain(start: ChainStart, chunks: &[Chunk]) -> Result<(), ChunkError> {
    let Some(first) = chunks.first() else {
        return Ok(());
    };
    if first.header.base != start.base {
        return Err(ChunkError::Chain(format!(
            "first chunk base {} != chain start {}",
            first.header.base, start.base
        )));
    }
    if first.header.prev_chunk_sha256 != start.prev_sha {
        return Err(ChunkError::Chain("first chunk prev link mismatch".into()));
    }
    for w in chunks.windows(2) {
        let (a, b) = (&w[0], &w[1]);
        if b.header.partition != a.header.partition {
            return Err(ChunkError::Chain("partition changes inside a chain".into()));
        }
        if b.header.prev_chunk_sha256 != a.sha256 {
            return Err(ChunkError::Chain(format!(
                "chunk at {} does not link to the chunk at {}",
                b.header.base, a.header.base
            )));
        }
        if b.header.base != a.header.end {
            return Err(ChunkError::Chain(format!(
                "gap or overlap: {} != previous end {}",
                b.header.base, a.header.end
            )));
        }
        if b.header.epoch < a.header.epoch {
            return Err(ChunkError::Chain(format!(
                "epoch decreases: {} after {}",
                b.header.epoch, a.header.epoch
            )));
        }
    }
    Ok(())
}

/// One chunk of a chain found by [`walk_back`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkRef {
    /// Remote key.
    pub key: String,
    /// The decoded, verified chunk.
    pub chunk: Chunk,
}

/// Walk a chain back from its head (a manifest's `last_chunk_key` / `last_chunk_sha256`):
/// GET each chunk, check its identity, and find its predecessor among the `seg/` keys ending
/// at its `base` whose identity equals its `prev_chunk_sha256` (the sha decides, so forked
/// chunks with the same base but another epoch or end are ignored). Stops at the first chunk
/// with `base <= stop` (a manifest's `chain_start`, or the referenced snapshot offset when
/// restoring an older generation). Returns the chunks in forward order, chain-verified.
pub async fn walk_back(
    store: &Arc<dyn ObjectStore>,
    last_chunk_key: &str,
    last_chunk_sha256: [u8; 32],
    stop: u64,
) -> Result<Vec<ChunkRef>, ChunkError> {
    let store_err = |e: ostro_objstore::Error| ChunkError::Store(e.to_string());
    let (partition, ..) = parse_chunk_key(last_chunk_key)
        .ok_or_else(|| ChunkError::Chain(format!("bad chunk key {last_chunk_key}")))?;
    let mut listing: Option<Vec<String>> = None;
    let mut out: Vec<ChunkRef> = Vec::new();
    let mut key = last_chunk_key.to_owned();
    let mut want = last_chunk_sha256;
    loop {
        let (bytes, _) = store
            .get(&key)
            .await
            .map_err(store_err)?
            .ok_or_else(|| ChunkError::Missing(key.clone()))?;
        if chunk_sha(&bytes) != want {
            return Err(ChunkError::Chain(format!("{key}: sha256 mismatch")));
        }
        let chunk = decode(&bytes)?;
        let (base, prev) = (chunk.header.base, chunk.header.prev_chunk_sha256);
        out.push(ChunkRef {
            key: key.clone(),
            chunk,
        });
        if base <= stop || prev == [0; 32] {
            break;
        }
        if listing.is_none() {
            let l = store
                .list(&seg_prefix(partition), None)
                .await
                .map_err(store_err)?;
            listing = Some(l.into_iter().map(|m| m.key).collect());
        }
        let mut found = None;
        for k in listing.iter().flatten() {
            let Some((p, _, _, end)) = parse_chunk_key(k) else {
                continue;
            };
            if p != partition || end != base {
                continue;
            }
            // Candidates share the end offset; the identity decides.
            if let Some((b, _)) = store.get(k).await.map_err(store_err)?
                && chunk_sha(&b) == prev
            {
                found = Some(k.clone());
                break;
            }
        }
        key = found.ok_or_else(|| {
            ChunkError::Missing(format!(
                "predecessor of the chunk at {base} (partition {partition})"
            ))
        })?;
        want = prev;
    }
    out.reverse();
    if let Some(first) = out.first() {
        let start = ChainStart {
            base: first.chunk.header.base,
            prev_sha: first.chunk.header.prev_chunk_sha256,
        };
        let chunks: Vec<Chunk> = out.iter().map(|c| c.chunk.clone()).collect();
        verify_chain(start, &chunks)?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_types::{Epoch, Offset, PartitionId};

    fn env(p: u16, epoch: u32, base: u64, n: usize) -> Vec<u8> {
        let cmds: Vec<_> = (0..n)
            .map(|i| format!("cmd-{base}-{i}").into_bytes().into())
            .collect();
        envelope::encode(PartitionId(p), Epoch(epoch), Offset(base), &cmds)
    }

    fn mk(p: u16, epoch: u32, base: u64, prev: [u8; 32]) -> (Vec<u8>, Chunk) {
        let e1 = env(p, epoch, base, 2);
        let e2 = env(p, epoch, base + 2, 3);
        let (_, bytes) = encode(p, epoch, prev, &[&e1, &e2]).expect("encode");
        let c = decode(&bytes).expect("decode");
        (bytes, c)
    }

    #[test]
    fn round_trip_and_keys() {
        let (bytes, c) = mk(3, 7, 10, [0; 32]);
        assert_eq!(c.header.base, 10);
        assert_eq!(c.header.end, 15);
        assert_eq!(c.header.count, 2);
        assert_eq!(c.sha256, chunk_sha(&bytes));
        assert_eq!(&bytes[..4], b"OSEG");
        let k = chunk_key(3, 7, 10, 15);
        assert_eq!(
            k,
            "p/0003/seg/0000000007-00000000000000000010-00000000000000000015.oseg"
        );
        assert_eq!(parse_chunk_key(&k), Some((3, 7, 10, 15)));
        assert_eq!(parse_chunk_key("p/0003/snap/x.osnap"), None);
        assert_eq!(parse_chunk_key("p/0003/seg/7-10-15.oseg"), None);
    }

    #[test]
    fn encode_rejects_mixed_epochs_and_gaps() {
        let a = env(0, 1, 0, 2);
        let b = env(0, 2, 2, 2);
        assert!(matches!(
            encode(0, 1, [0; 32], &[&a, &b]),
            Err(ChunkError::Envelope { index: 1, .. })
        ));
        let c = env(0, 1, 5, 1);
        assert!(encode(0, 1, [0; 32], &[&a, &c]).is_err());
        assert!(encode(1, 1, [0; 32], &[&a]).is_err());
        assert!(encode(0, 1, [0; 32], &[]).is_err());
    }

    #[test]
    fn corruption_is_detected() {
        let (bytes, _) = mk(0, 1, 0, [0; 32]);
        // Header (magic).
        let mut b = bytes.clone();
        b[0] ^= 1;
        assert!(matches!(decode(&b), Err(ChunkError::Malformed(_))));
        // Header (base): trailer catches it.
        let mut b = bytes.clone();
        b[12] ^= 1;
        assert_eq!(decode(&b), Err(ChunkError::TrailerMismatch));
        // Body byte.
        let mut b = bytes.clone();
        b[HEADER_LEN + 10] ^= 0x40;
        assert_eq!(decode(&b), Err(ChunkError::TrailerMismatch));
        // Trailer byte.
        let mut b = bytes.clone();
        let n = b.len();
        b[n - 1] ^= 1;
        assert_eq!(decode(&b), Err(ChunkError::TrailerMismatch));
        // Body corrupted with a recomputed trailer: the envelope CRC32C catches it.
        let mut b = bytes.clone();
        b[HEADER_LEN + 4 + 40] ^= 0x40;
        let split = b.len() - TRAILER_LEN;
        let t: [u8; 32] = Sha256::digest(&b[..split]).into();
        b[split..].copy_from_slice(&t);
        assert!(matches!(
            decode(&b),
            Err(ChunkError::Envelope { index: 0, .. })
        ));
        // Truncated.
        assert!(decode(&bytes[..40]).is_err());
    }

    #[test]
    fn chain_verification() {
        let (_, a) = mk(0, 1, 0, [0; 32]);
        let (_, b) = mk(0, 1, 5, a.sha256);
        let (_, c) = mk(0, 2, 10, b.sha256);
        let zero = ChainStart {
            base: 0,
            prev_sha: [0; 32],
        };
        verify_chain(zero, &[a.clone(), b.clone(), c.clone()]).expect("valid chain");
        // Wrong start (base).
        assert!(
            verify_chain(
                ChainStart {
                    base: 5,
                    prev_sha: [0; 32]
                },
                &[a.clone(), b.clone()]
            )
            .is_err()
        );
        // Wrong start (prev link of a suffix).
        assert!(
            verify_chain(
                ChainStart {
                    base: 5,
                    prev_sha: [9; 32]
                },
                &[b.clone(), c.clone()]
            )
            .is_err()
        );
        verify_chain(
            ChainStart {
                base: 5,
                prev_sha: a.sha256,
            },
            &[b.clone(), c.clone()],
        )
        .expect("suffix");
        // Broken link: c does not follow a.
        assert!(verify_chain(zero, &[a.clone(), c.clone()]).is_err());
        // Link right but gap.
        let (_, gap) = mk(0, 1, 6, a.sha256);
        assert!(verify_chain(zero, &[a.clone(), gap]).is_err());
        // Epoch decrease.
        let (_, c2) = mk(0, 2, 5, a.sha256);
        let (_, d) = mk(0, 1, 10, c2.sha256);
        assert!(verify_chain(zero, &[a, c2, d]).is_err());
    }
}
