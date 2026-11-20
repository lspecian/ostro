//! Bottomless manifest v1 (P8-D3, plan 08-13): `manifest/current` and `manifest/gen/<gen 20d>`.
//!
//! Encoding: `postcard(Manifest) ‖ sha256(postcard bytes)`. Each manifest names the previous
//! one (`prev_manifest_sha256`, the sha256 of its whole encoded object), so the generations
//! form a hash chain. `manifest/current` is replaced with `If-Match` on the previous etag
//! (fencing, P8-D4); an immutable `manifest/gen/<gen>` copy is written on every round that
//! changed a referenced snapshot (point-in-time anchors).
//!
//! [`Manifest::validate`] enforces the ordering rules restore relies on (B3/B4), and
//! [`decode`] refuses a manifest that fails them; 08-18 restore calls the same functions.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Manifest format version.
pub const FORMAT: u32 = 1;
/// `manifest/current`.
pub const CURRENT_KEY: &str = "manifest/current";
/// `manifest/gen/`.
pub const GEN_PREFIX: &str = "manifest/gen/";

/// `manifest/gen/<gen 20d>`.
pub fn gen_key(generation: u64) -> String {
    format!("{GEN_PREFIX}{generation:020}")
}

/// Parse a `manifest/gen/<gen 20d>` key.
pub fn parse_gen_key(key: &str) -> Option<u64> {
    let g = key.strip_prefix(GEN_PREFIX)?;
    (g.len() == 20 && g.bytes().all(|b| b.is_ascii_digit()))
        .then(|| g.parse().ok())
        .flatten()
}

/// One partition's entry of the cut.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionCut {
    /// Partition id.
    pub p: u16,
    /// The partition's offset in the consistent cut (exclusive): restore truncates here.
    pub cut_offset: u64,
    /// Base of the first chunk restore needs (the retained start of the chain).
    pub chain_start: u64,
    /// Head of the chunk chain (`None` when `cut_offset == chain_start`).
    pub last_chunk_key: Option<String>,
    /// Identity (sha256 of the whole object) of the head chunk.
    pub last_chunk_sha256: Option<[u8; 32]>,
    /// Referenced snapshot (`p/<pppp>/snap/<applied 20d>-<sha256>.osnap`); `None` only for a
    /// chain that starts at offset 0.
    pub snapshot_key: Option<String>,
    /// sha256 of the snapshot file.
    pub snapshot_sha256: Option<[u8; 32]>,
    /// The snapshot's `applied_offset`.
    pub snapshot_offset: Option<u64>,
    /// Offset at which restore checks the rolling state hash.
    pub verify_offset: u64,
    /// Expected `state_hash` at `verify_offset`.
    pub verify_state_hash: [u8; 32],
}

/// Manifest v1.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// [`FORMAT`].
    pub format: u32,
    /// Cluster id (meta `ClusterInfo`).
    pub cluster_id: String,
    /// Node id.
    pub node_id: u64,
    /// The writer's `node.toml` incarnation.
    pub incarnation: [u8; 16],
    /// Generation, +1 per manifest.
    pub generation: u64,
    /// sha256 of the previous encoded manifest (`[0; 32]` for generation 1).
    pub prev_manifest_sha256: [u8; 32],
    /// When the cut was read (injected clock, ms since the epoch).
    pub cut_at_ms: u64,
    /// Per partition, ascending by `p`.
    pub cut: Vec<PartitionCut>,
    /// `meta/<index 20d>.msnap`, captured after the cut.
    pub meta_snapshot_key: Option<String>,
    /// sha256 of the meta snapshot object.
    pub meta_snapshot_sha256: Option<[u8; 32]>,
    /// Writer version.
    pub ostro_version: String,
}

/// Manifest failures.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ManifestError {
    /// Not a manifest object (short, sha trailer mismatch, postcard error).
    #[error("malformed manifest: {0}")]
    Malformed(String),
    /// Unknown `format`.
    #[error("unsupported manifest format {0}")]
    Format(u32),
    /// The ordering rules do not hold.
    #[error("invalid manifest: partition {p}: {reason}")]
    Invalid {
        /// Partition.
        p: u16,
        /// Which rule failed.
        reason: String,
    },
}

impl Manifest {
    /// Enforce, per partition (B3/B4):
    /// - `snapshot_offset <= verify_offset <= cut_offset`;
    /// - `chain_start <= snapshot_offset`, or `chain_start == 0` when there is no snapshot;
    /// - the snapshot fields are all present or all absent;
    /// - `last_chunk_*` present (and the head chunk key ends at `cut_offset`) exactly when
    ///   `cut_offset > chain_start`;
    /// - partitions strictly ascending.
    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.format != FORMAT {
            return Err(ManifestError::Format(self.format));
        }
        let mut prev: Option<u16> = None;
        for c in &self.cut {
            let bad = |reason: String| ManifestError::Invalid { p: c.p, reason };
            if prev.is_some_and(|q| q >= c.p) {
                return Err(bad("partitions not strictly ascending".into()));
            }
            prev = Some(c.p);
            let snap_fields = [
                c.snapshot_key.is_some(),
                c.snapshot_sha256.is_some(),
                c.snapshot_offset.is_some(),
            ];
            if snap_fields.iter().any(|&b| b) && !snap_fields.iter().all(|&b| b) {
                return Err(bad("snapshot fields partially set".into()));
            }
            match c.snapshot_offset {
                Some(s) => {
                    if c.chain_start > s {
                        return Err(bad(format!(
                            "chain_start {} > snapshot_offset {s}",
                            c.chain_start
                        )));
                    }
                    if s > c.verify_offset {
                        return Err(bad(format!(
                            "snapshot_offset {s} > verify_offset {}",
                            c.verify_offset
                        )));
                    }
                }
                None => {
                    if c.chain_start != 0 {
                        return Err(bad(format!(
                            "chain starts at {} without a snapshot",
                            c.chain_start
                        )));
                    }
                }
            }
            if c.verify_offset > c.cut_offset {
                return Err(bad(format!(
                    "verify_offset {} > cut_offset {}",
                    c.verify_offset, c.cut_offset
                )));
            }
            if c.chain_start > c.cut_offset {
                return Err(bad(format!(
                    "chain_start {} > cut_offset {}",
                    c.chain_start, c.cut_offset
                )));
            }
            let has_head = c.last_chunk_key.is_some() || c.last_chunk_sha256.is_some();
            if c.cut_offset > c.chain_start {
                let (Some(k), Some(_)) = (&c.last_chunk_key, &c.last_chunk_sha256) else {
                    return Err(bad("chunks needed but no head chunk named".into()));
                };
                match crate::chunk::parse_chunk_key(k) {
                    Some((p, _, _, end)) if p == c.p && end == c.cut_offset => {}
                    _ => {
                        return Err(bad(format!(
                            "head chunk {k} does not end at cut_offset {}",
                            c.cut_offset
                        )));
                    }
                }
            } else if has_head {
                return Err(bad("head chunk named for an empty chain".into()));
            }
        }
        Ok(())
    }

    /// The entry of partition `p`.
    pub fn partition(&self, p: u16) -> Option<&PartitionCut> {
        self.cut.iter().find(|c| c.p == p)
    }
}

/// `postcard(m) ‖ sha256`. Does not validate (callers validate before writing).
pub fn encode(m: &Manifest) -> Vec<u8> {
    let mut out = postcard::to_allocvec(m).unwrap_or_default();
    let sha: [u8; 32] = Sha256::digest(&out).into();
    out.extend_from_slice(&sha);
    out
}

/// sha256 of an encoded manifest object (the next manifest's `prev_manifest_sha256`).
pub fn object_sha(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Decode, check the sha trailer and the format, and [`Manifest::validate`].
pub fn decode(bytes: &[u8]) -> Result<Manifest, ManifestError> {
    if bytes.len() < 32 {
        return Err(ManifestError::Malformed(
            "shorter than the sha trailer".into(),
        ));
    }
    let (body, trailer) = bytes.split_at(bytes.len() - 32);
    let sha: [u8; 32] = Sha256::digest(body).into();
    if sha[..] != *trailer {
        return Err(ManifestError::Malformed("sha256 trailer mismatch".into()));
    }
    let m: Manifest =
        postcard::from_bytes(body).map_err(|e| ManifestError::Malformed(e.to_string()))?;
    m.validate()?;
    Ok(m)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn part(p: u16) -> PartitionCut {
        PartitionCut {
            p,
            cut_offset: 100,
            chain_start: 40,
            last_chunk_key: Some(crate::chunk::chunk_key(p, 2, 80, 100)),
            last_chunk_sha256: Some([1; 32]),
            snapshot_key: Some(format!("p/{p:04}/snap/x.osnap")),
            snapshot_sha256: Some([2; 32]),
            snapshot_offset: Some(50),
            verify_offset: 90,
            verify_state_hash: [3; 32],
        }
    }

    fn manifest() -> Manifest {
        Manifest {
            format: FORMAT,
            cluster_id: "c".into(),
            node_id: 1,
            incarnation: [7; 16],
            generation: 3,
            prev_manifest_sha256: [0; 32],
            cut_at_ms: 1234,
            cut: vec![part(0), part(1)],
            meta_snapshot_key: None,
            meta_snapshot_sha256: None,
            ostro_version: "test".into(),
        }
    }

    #[test]
    fn round_trip() {
        let m = manifest();
        m.validate().expect("valid");
        let b = encode(&m);
        assert_eq!(decode(&b).expect("decode"), m);
        let mut bad = b.clone();
        bad[3] ^= 1;
        assert!(matches!(decode(&bad), Err(ManifestError::Malformed(_))));
        assert_eq!(gen_key(5), "manifest/gen/00000000000000000005");
        assert_eq!(parse_gen_key(&gen_key(5)), Some(5));
        assert_eq!(parse_gen_key("manifest/gen/5"), None);
    }

    fn invalid(f: impl FnOnce(&mut PartitionCut)) -> bool {
        let mut m = manifest();
        f(&mut m.cut[1]);
        let r = m.validate();
        // decode refuses what validate refuses.
        let d = decode(&encode(&m));
        assert_eq!(r.is_err(), d.is_err());
        r.is_err()
    }

    #[test]
    fn ordering_rules() {
        assert!(invalid(|c| c.snapshot_offset = Some(95))); // snapshot > verify
        assert!(invalid(|c| c.verify_offset = 101)); // verify > cut
        assert!(invalid(|c| c.chain_start = 60)); // chain_start > snapshot
        assert!(invalid(|c| c.snapshot_key = None)); // partial snapshot fields
        assert!(invalid(|c| {
            c.snapshot_key = None;
            c.snapshot_sha256 = None;
            c.snapshot_offset = None;
        })); // no snapshot but chain_start 40
        assert!(!invalid(|c| {
            c.snapshot_key = None;
            c.snapshot_sha256 = None;
            c.snapshot_offset = None;
            c.chain_start = 0;
        }));
        assert!(invalid(|c| c.last_chunk_key = None));
        assert!(invalid(
            |c| c.last_chunk_key = Some(crate::chunk::chunk_key(1, 2, 80, 99))
        ));
        assert!(invalid(
            |c| c.last_chunk_key = Some(crate::chunk::chunk_key(0, 2, 80, 100))
        ));
        // Empty chain: cut == chain_start, no head allowed.
        assert!(invalid(|c| {
            c.cut_offset = 40;
            c.verify_offset = 40;
            c.snapshot_offset = Some(40);
        }));
        assert!(!invalid(|c| {
            c.cut_offset = 40;
            c.verify_offset = 40;
            c.snapshot_offset = Some(40);
            c.last_chunk_key = None;
            c.last_chunk_sha256 = None;
        }));
        let mut m = manifest();
        m.cut[1].p = 0;
        assert!(m.validate().is_err());
        let mut m = manifest();
        m.format = 2;
        assert_eq!(m.validate(), Err(ManifestError::Format(2)));
    }
}
