//! The metadata head pointer of a multi-member cluster bucket (plan 14-29, D-P14-45).
//!
//! In a multi-member cluster no `manifest/current` is written (D-P12-3), so nothing named the
//! newest metadata snapshot. The metadata leader now uploads its snapshot under the Phase 8
//! key scheme ([`crate::uploader::meta_key`], `ostro_meta::encode_snapshot`, no format
//! change) and then moves one small JSON object, `meta/head`:
//!
//! ```json
//! {"format":1,"term":4,"index":118,"key":"meta/00000000000000000118.msnap",
//!  "sha256":"<64 hex>","at_ms":1764201600000,"members":3,"partitions":8}
//! ```
//!
//! `meta/head` is replaced with `If-Match` on the etag the writer last saw (`If-None-Match: *`
//! when it saw none) and is fenced by the metadata Raft term:
//!
//! - a writer never replaces a head with a **higher term**, nor one with a higher (or equal)
//!   index at the **same** term;
//! - on a failed precondition [`put`] re-reads the head: a higher term means another leader
//!   took over ([`MetaHeadPut::Fenced`], `ostro_backup_meta_fenced = 1`); an equal-term head at
//!   an equal or higher index is newer than ours ([`MetaHeadPut::Superseded`]); otherwise the
//!   write is retried once with the fresh etag ([`MetaHeadPut::Conflict`] if that fails too).
//!
//! Without conditional PUT (the documented, weaker fallback, as for partition heads) the head
//! is re-read and fence-checked right before an unconditional PUT (a race between the read
//! and the PUT can still let a lower term win until the next round of the higher one).

use std::sync::Arc;

use ostro_objstore::{Conditional, ObjectStore};
use serde::{Deserialize, Serialize};

use crate::uploader::UploadError;

/// The object key of the metadata head pointer.
pub const META_HEAD_KEY: &str = "meta/head";

/// `meta/head` format version.
pub const META_HEAD_FORMAT: u32 = 1;

/// `ostro_backup_meta_fenced`: 1 once a higher-term metadata leader fenced this writer's
/// `meta/head` moves (reset to 0 when it writes again at a higher term).
pub const METRIC_META_FENCED: &str = "ostro_backup_meta_fenced";

/// The metadata head pointer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaHead {
    /// Metadata Raft term of the writer (the fence).
    pub term: u64,
    /// Applied metadata index of the snapshot.
    pub index: u64,
    /// Object key of the snapshot (`meta/<index 20d>.msnap`).
    pub key: String,
    /// sha256 of the snapshot object.
    pub sha256: [u8; 32],
    /// When the head was written (injected clock, ms), after the round's partition uploads.
    pub at_ms: u64,
    /// Metadata member count the writer saw.
    pub members: u64,
    /// Partition count of the cluster.
    pub partitions: u16,
}

#[derive(Serialize, Deserialize)]
struct Wire {
    format: u32,
    term: u64,
    index: u64,
    key: String,
    sha256: String,
    at_ms: u64,
    members: u64,
    partitions: u16,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(s.get(2 * i..2 * i + 2)?, 16).ok()?;
    }
    Some(out)
}

/// Encode a head as JSON.
pub fn encode(h: &MetaHead) -> Vec<u8> {
    let w = Wire {
        format: META_HEAD_FORMAT,
        term: h.term,
        index: h.index,
        key: h.key.clone(),
        sha256: hex(&h.sha256),
        at_ms: h.at_ms,
        members: h.members,
        partitions: h.partitions,
    };
    serde_json::to_vec(&w).unwrap_or_default()
}

/// Decode a `meta/head` object.
pub fn decode(bytes: &[u8]) -> Result<MetaHead, String> {
    let w: Wire = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if w.format != META_HEAD_FORMAT {
        return Err(format!("unsupported meta/head format {}", w.format));
    }
    let sha256 = unhex32(&w.sha256).ok_or_else(|| "bad sha256".to_string())?;
    if !w.key.starts_with("meta/") {
        return Err(format!("meta/head names a non-meta key {:?}", w.key));
    }
    Ok(MetaHead {
        term: w.term,
        index: w.index,
        key: w.key,
        sha256,
        at_ms: w.at_ms,
        members: w.members,
        partitions: w.partitions,
    })
}

/// Read `meta/head` with its etag.
pub async fn get(
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<(MetaHead, Option<String>)>, UploadError> {
    let Some((bytes, meta)) = store.get(META_HEAD_KEY).await? else {
        return Ok(None);
    };
    let head = decode(&bytes).map_err(|e| UploadError::Io(format!("{META_HEAD_KEY}: {e}")))?;
    let etag = match meta.e_tag {
        Some(e) => Some(e),
        None => store.head(META_HEAD_KEY).await?.and_then(|m| m.e_tag),
    };
    Ok(Some((head, etag)))
}

/// Outcome of [`put`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetaHeadPut {
    /// Written; the new etag.
    Written(Option<String>),
    /// A head with a higher term exists: this writer must not move `meta/head` at its term.
    Fenced(MetaHead),
    /// A head at the same term with an equal or higher index exists (nothing to do); its etag.
    Superseded(MetaHead, Option<String>),
    /// The retry with the fresh etag failed as well; the current head (retry next round).
    Conflict(Option<(MetaHead, Option<String>)>),
}

/// Whether `new` may replace `cur` (term fence, then index within a term).
pub fn may_replace(cur: &MetaHead, new: &MetaHead) -> bool {
    cur.term < new.term || (cur.term == new.term && cur.index < new.index)
}

fn refuse(cur: MetaHead, etag: Option<String>, new: &MetaHead) -> MetaHeadPut {
    if cur.term > new.term {
        metrics::gauge!(METRIC_META_FENCED).set(1.0);
        MetaHeadPut::Fenced(cur)
    } else {
        MetaHeadPut::Superseded(cur, etag)
    }
}

async fn put_once(
    store: &Arc<dyn ObjectStore>,
    body: &[u8],
    etag: Option<&str>,
) -> Result<Option<Option<String>>, UploadError> {
    let put = match etag {
        Some(e) => {
            store
                .put_if_match(META_HEAD_KEY, body.to_vec().into(), e)
                .await?
        }
        None => {
            store
                .put_if_absent(META_HEAD_KEY, body.to_vec().into())
                .await?
        }
    };
    match put {
        Conditional::Ok(o) => {
            let etag = match o.e_tag {
                Some(e) => Some(e),
                None => store.head(META_HEAD_KEY).await?.and_then(|m| m.e_tag),
            };
            Ok(Some(etag))
        }
        Conditional::PreconditionFailed => Ok(None),
    }
}

/// Replace `meta/head` with `new`, conditioned on `seen_etag` (`None` = must be absent).
/// `seen_etag` must name a head the caller wrote itself or checked with [`may_replace`]
/// (the etag stands in for the term check). See the module docs for the fence.
pub async fn put(
    store: &Arc<dyn ObjectStore>,
    new: &MetaHead,
    seen_etag: Option<&str>,
) -> Result<MetaHeadPut, UploadError> {
    let body = encode(new);
    if !store.supports_conditional_put() {
        // The re-read + fence check stands in for the precondition (and its one retry).
        if let Some((h, e)) = get(store).await?
            && !may_replace(&h, new)
        {
            return Ok(refuse(h, e, new));
        }
        let out = store.put(META_HEAD_KEY, body.into()).await?;
        metrics::gauge!(METRIC_META_FENCED).set(0.0);
        return Ok(MetaHeadPut::Written(out.e_tag));
    }
    if let Some(etag) = put_once(store, &body, seen_etag).await? {
        metrics::gauge!(METRIC_META_FENCED).set(0.0);
        return Ok(MetaHeadPut::Written(etag));
    }
    // Precondition failed: re-read, apply the fence, retry once with the fresh etag.
    let cur = get(store).await?;
    let fresh = match cur {
        Some((h, e)) => {
            if !may_replace(&h, new) {
                return Ok(refuse(h, e, new));
            }
            e
        }
        None => None,
    };
    if let Some(etag) = put_once(store, &body, fresh.as_deref()).await? {
        metrics::gauge!(METRIC_META_FENCED).set(0.0);
        return Ok(MetaHeadPut::Written(etag));
    }
    let cur = get(store).await?;
    if let Some((h, e)) = &cur
        && !may_replace(h, new)
    {
        return Ok(refuse(h.clone(), e.clone(), new));
    }
    Ok(MetaHeadPut::Conflict(cur))
}

/// Parse `meta/<index 20d>.msnap` into the index.
pub fn parse_meta_key(key: &str) -> Option<u64> {
    let n = key.strip_prefix("meta/")?.strip_suffix(".msnap")?;
    if n.len() != 20 || !n.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    n.parse().ok()
}

/// The meta snapshot keys retention deletes: everything older than the newest `keep`
/// snapshots (by index) and not newer than `head_index`, except `protect` (the snapshot a
/// Phase 8 `manifest/current` still names).
pub fn retention_victims(
    keys: &[String],
    keep: usize,
    head_index: u64,
    protect: Option<&str>,
) -> Vec<String> {
    let mut idx: Vec<(u64, &String)> = keys
        .iter()
        .filter_map(|k| parse_meta_key(k).map(|i| (i, k)))
        .filter(|(i, _)| *i <= head_index)
        .collect();
    idx.sort_unstable_by_key(|(i, _)| std::cmp::Reverse(*i));
    idx.into_iter()
        .skip(keep.max(1))
        .filter(|(_, k)| Some(k.as_str()) != protect)
        .map(|(_, k)| k.clone())
        .collect()
}
