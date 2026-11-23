//! Oracle `objstore` (12-03): the bottomless bucket holds the committed log and nothing more.
//!
//! - [`chain_equals_committed`]: walk the partition's chunk chain back from its head and
//!   compare every reachable envelope with the committed log (records below HW) byte for
//!   byte; a chunk beyond HW or an envelope the log does not hold is reported.
//! - [`nothing_above`]: no chunk object of the partition, reachable or not, ends above HW.
//!
//! The head is `p/<pppp>/head` (cluster mode, 12-15) when present, else the Phase 8
//! manifest (`manifest/current`, single-member mode). The cluster head's encoding is owned
//! by 12-15 (`ostro_backup::cluster::encode_head`): JSON `{"epoch", "last_chunk_key",
//! "last_chunk_sha256", "chain_start", ...}`. [`decode_head`] reads the chain pointer (sha as a
//! hex string or a byte array; `chain_start` absent = 0). A head with `last_chunk_key: null`
//! is an empty chain at its `chain_start` (a takeover re-chained from a snapshot, which is the
//! chain base): nothing to compare.

use std::path::Path;
use std::sync::Arc;

use ostro_backup::chunk::{parse_chunk_key, seg_prefix, walk_back};
use ostro_backup::manifest;
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_types::Offset;
use ostro_types::envelope::peek_header;
use serde_json::Value;

use super::Report;
use super::disk::ScannedRecord;

/// Where a partition's chain starts walking back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainHead {
    /// Key of the newest chunk.
    pub last_chunk_key: String,
    /// Its identity.
    pub last_chunk_sha256: [u8; 32],
    /// Stop walking at the first chunk with `base <= stop` (manifest or head `chain_start`).
    pub stop: u64,
}

/// `p/<pppp>/head`.
pub fn head_key(p: u16) -> String {
    format!("p/{p:04}/head")
}

fn sha_from(v: &Value) -> Option<[u8; 32]> {
    match v {
        Value::String(s) if s.len() == 64 => {
            let mut out = [0u8; 32];
            for (i, b) in out.iter_mut().enumerate() {
                *b = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
            }
            Some(out)
        }
        Value::Array(a) if a.len() == 32 => {
            let mut out = [0u8; 32];
            for (o, x) in out.iter_mut().zip(a) {
                *o = u8::try_from(x.as_u64()?).ok()?;
            }
            Some(out)
        }
        _ => None,
    }
}

/// Decode a cluster head object (see the module docs): `None` = unrecognised, `Some(None)` =
/// an empty chain.
pub fn decode_head(bytes: &[u8]) -> Option<Option<ChainHead>> {
    let v: Value = serde_json::from_slice(bytes).ok()?;
    let stop = match &v["chain_start"] {
        Value::Null => 0,
        c => c.as_u64()?,
    };
    if v["last_chunk_key"].is_null() && v.get("epoch").is_some() {
        return Some(None);
    }
    Some(Some(ChainHead {
        last_chunk_key: v["last_chunk_key"].as_str()?.to_owned(),
        last_chunk_sha256: sha_from(&v["last_chunk_sha256"])?,
        stop,
    }))
}

fn open(bucket: &Path) -> Result<Arc<dyn ObjectStore>, String> {
    Ok(Arc::new(LocalFs::new(bucket).map_err(|e| {
        format!("open bucket {}: {e}", bucket.display())
    })?))
}

/// The head of partition `p`'s chain (`None` = nothing uploaded for it).
pub async fn find_head(store: &Arc<dyn ObjectStore>, p: u16) -> Result<Option<ChainHead>, String> {
    if let Some((b, _)) = store.get(&head_key(p)).await.map_err(|e| e.to_string())? {
        return decode_head(&b).ok_or_else(|| format!("{}: unrecognised head object", head_key(p)));
    }
    let Some((b, _)) = store
        .get(manifest::CURRENT_KEY)
        .await
        .map_err(|e| e.to_string())?
    else {
        return Ok(None);
    };
    let m = manifest::decode(&b).map_err(|e| format!("{}: {e}", manifest::CURRENT_KEY))?;
    Ok(m.partition(p).and_then(|c| {
        Some(ChainHead {
            last_chunk_key: c.last_chunk_key.clone()?,
            last_chunk_sha256: c.last_chunk_sha256?,
            stop: c.chain_start,
        })
    }))
}

/// The reachable chain of partition `p` equals the committed log prefix (records below
/// `hw`) byte for byte.
pub async fn chain_equals_committed(
    bucket: &Path,
    p: u16,
    committed: &[ScannedRecord],
    hw: Offset,
) -> Report {
    let mut report = Report::new("objstore");
    let store = match open(bucket) {
        Ok(s) => s,
        Err(e) => {
            report.violation(e);
            return report;
        }
    };
    let head = match find_head(&store, p).await {
        Ok(Some(h)) => h,
        Ok(None) => return report,
        Err(e) => {
            report.violation(format!("partition {p}: {e}"));
            return report;
        }
    };
    let chain = match walk_back(
        &store,
        &head.last_chunk_key,
        head.last_chunk_sha256,
        head.stop,
    )
    .await
    {
        Ok(c) => c,
        Err(e) => {
            report.violation(format!(
                "partition {p}: chain from {} does not verify: {e}",
                head.last_chunk_key
            ));
            return report;
        }
    };
    let committed: std::collections::BTreeMap<u64, &ScannedRecord> = committed
        .iter()
        .filter(|r| r.end <= hw)
        .map(|r| (r.base.0, r))
        .collect();
    for c in &chain {
        if c.chunk.header.end > hw.0 {
            report.violation(format!(
                "partition {p}: chunk {} ends at {} beyond HW {}",
                c.key, c.chunk.header.end, hw.0
            ));
        }
        for env in &c.chunk.envelopes {
            let base = match peek_header(env) {
                Ok(h) => h.base.0,
                Err(e) => {
                    report.violation(format!("partition {p}: chunk {}: bad envelope: {e}", c.key));
                    continue;
                }
            };
            match committed.get(&base) {
                Some(r) if r.envelope[..] == env[..] => {}
                Some(_) => report.violation(format!(
                    "partition {p}: chunk {} envelope at offset {base} differs from the committed log",
                    c.key
                )),
                None => report.violation(format!(
                    "partition {p}: chunk {} holds offset {base}, which is not in the committed log below HW {}",
                    c.key, hw.0
                )),
            }
        }
    }
    report
}

/// No chunk object of partition `p` (reachable or not) ends above `hw`.
pub async fn nothing_above(bucket: &Path, p: u16, hw: Offset) -> Report {
    let mut report = Report::new("objstore");
    let store = match open(bucket) {
        Ok(s) => s,
        Err(e) => {
            report.violation(e);
            return report;
        }
    };
    let keys = match store.list(&seg_prefix(p), None).await {
        Ok(k) => k,
        Err(e) => {
            report.violation(format!("partition {p}: list {}: {e}", seg_prefix(p)));
            return report;
        }
    };
    for m in keys {
        match parse_chunk_key(&m.key) {
            Some((kp, epoch, _, end)) if kp == p && end > hw.0 => report.violation(format!(
                "partition {p}: chunk {} (epoch {epoch}) ends at {end} above HW {}",
                m.key, hw.0
            )),
            Some(_) => {}
            None => report.violation(format!("partition {p}: unparseable chunk key {}", m.key)),
        }
    }
    report
}
