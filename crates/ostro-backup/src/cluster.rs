//! Bottomless storage in cluster mode (Phase 12, plan 12-15, D-P12-20, D-P12-3).
//!
//! Phase 8's uploader is single-writer by construction: one node, one `LOCK`, one
//! `manifest/current`. With replicated partitions, the writer of a partition's chain is that
//! partition's **current leader**. Each partition gets its own pointer object:
//!
//! ```text
//! p/<pppp>/head = {"format":1, "epoch":e, "last_chunk_key":k|null, "last_chunk_sha256":hex|null,
//!                  "chain_start":s, "end":x, "snapshot_key":..., "snapshot_offset":...,
//!                  "snapshot_sha256":..., "snapshot_state_hash":..., "node_id":n, "members":m}
//! ```
//!
//! `epoch` is the leader epoch of the writer. The head is replaced only with `If-Match` on the
//! etag the writer last saw (`If-None-Match: *` when absent). When the precondition fails, the
//! writer re-reads the head. If `head.epoch > e`, the writer is fenced for that partition
//! (`ostro_repl_upload_fenced{partition} = 1`). Otherwise it reconciles and retries.
//! A writer also refuses to replace a head whose epoch is above its own. So a stale leader can
//! never move the head, whatever etag it holds.
//!
//! [`reconcile_on_takeover`] runs when a node starts leading a partition, before its first
//! upload. It walks the chain back from the head ([`chunk::walk_back`]) and checks every
//! reachable envelope against the local log. An envelope is divergent when it ends above
//! `end_offset_for_epoch(chunk.epoch)` (the HW for the latest local epoch) or its bytes differ
//! from the local committed log. A straddling chunk is re-cut to its consistent prefix (a new
//! key, because `end` differs) and linked where the original was. Then the head moves to the
//! repaired tail, and the unreachable chunks are deleted **after** the head PUT returned. If
//! nothing diverges, nothing is written.
//!
//! Gap (W4): followers trim by their own snapshot floor, so a new leader's `log_start` can lie
//! above the remote tail. In that case, and when the head's chain cannot be walked back, the
//! leader re-chains: it uploads its newest local snapshot `S` with `log_start <= S <= HW`
//! (Phase 8 key scheme), moves the head to an empty chain starting at `S` that references the
//! snapshot, and deletes the old chain afterwards. The first chunk after `S` links to nothing
//! (the 08-13 chain start rule).
//!
//! The head is written in single-member mode too. `LOCK` and `manifest/current` are written
//! only while the cluster has exactly one member ([`crate::fence::acquire_if_single_member`],
//! the uploader's members watch). Restore refuses a bucket that has heads but no manifest
//! ([`crate::restore::RestoreError::ClusterBucketUnsupported`]).

use std::collections::VecDeque;
use std::sync::Arc;

use ostro_log::{EpochHistory, ReplicatedLog};
use ostro_objstore::{Conditional, ObjectStore};
use ostro_partition::leader::snapshot::LocalSnapshot;
use ostro_types::{Epoch, Offset};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::chunk::{self, ChunkRef};
use crate::manifest;
use crate::uploader::{UploadError, upload_snapshot_file};

/// `ostro_repl_upload_fenced{partition}`: 1 while a higher-epoch writer owns the partition's
/// head (reserved by 12-01).
pub const METRIC_UPLOAD_FENCED: &str = "ostro_repl_upload_fenced";

/// Head object format version.
pub const HEAD_FORMAT: u32 = 1;

/// Attempts of one reconcile against concurrent head moves by lower-or-equal epochs.
const RECONCILE_ATTEMPTS: usize = 3;

/// `p/<pppp>/head`.
pub fn head_key(partition: u16) -> String {
    format!("p/{partition:04}/head")
}

/// The snapshot a head references (its chain base when `offset == chain_start`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HeadSnapshot {
    /// `p/<pppp>/snap/...` key.
    pub key: String,
    /// Applied offset of the snapshot.
    pub offset: u64,
    /// sha256 of the object.
    pub sha256: [u8; 32],
    /// The snapshot's state hash, when known.
    pub state_hash: Option<[u8; 32]>,
}

/// A partition's chain pointer in cluster mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    /// Leader epoch of the writer.
    pub epoch: u32,
    /// Newest chunk (`None` = empty chain at `chain_start`).
    pub last_chunk_key: Option<String>,
    /// Its identity.
    pub last_chunk_sha256: Option<[u8; 32]>,
    /// Where the chain starts (walk-back stop).
    pub chain_start: u64,
    /// Where the chain ends (the last chunk's end, or `chain_start`).
    pub end: u64,
    /// The referenced snapshot.
    pub snapshot: Option<HeadSnapshot>,
    /// Writer node.
    pub node_id: u64,
    /// Cluster member count the writer saw.
    pub members: u64,
}

#[derive(Serialize, Deserialize)]
struct HeadWire {
    format: u32,
    epoch: u32,
    last_chunk_key: Option<String>,
    last_chunk_sha256: Option<String>,
    chain_start: u64,
    end: u64,
    #[serde(default)]
    snapshot_key: Option<String>,
    #[serde(default)]
    snapshot_offset: Option<u64>,
    #[serde(default)]
    snapshot_sha256: Option<String>,
    #[serde(default)]
    snapshot_state_hash: Option<String>,
    #[serde(default)]
    node_id: u64,
    #[serde(default)]
    members: u64,
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

/// Encode a head as JSON (the shape in the module docs).
pub fn encode_head(h: &Head) -> Vec<u8> {
    let w = HeadWire {
        format: HEAD_FORMAT,
        epoch: h.epoch,
        last_chunk_key: h.last_chunk_key.clone(),
        last_chunk_sha256: h.last_chunk_sha256.map(|s| hex(&s)),
        chain_start: h.chain_start,
        end: h.end,
        snapshot_key: h.snapshot.as_ref().map(|s| s.key.clone()),
        snapshot_offset: h.snapshot.as_ref().map(|s| s.offset),
        snapshot_sha256: h.snapshot.as_ref().map(|s| hex(&s.sha256)),
        snapshot_state_hash: h
            .snapshot
            .as_ref()
            .and_then(|s| s.state_hash.map(|x| hex(&x))),
        node_id: h.node_id,
        members: h.members,
    };
    serde_json::to_vec(&w).unwrap_or_default()
}

/// Decode a head object.
pub fn decode_head(bytes: &[u8]) -> Result<Head, String> {
    let w: HeadWire = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if w.format != HEAD_FORMAT {
        return Err(format!("unsupported head format {}", w.format));
    }
    let sha = |s: &Option<String>, what: &str| -> Result<Option<[u8; 32]>, String> {
        s.as_deref()
            .map(|x| unhex32(x).ok_or_else(|| format!("bad {what}")))
            .transpose()
    };
    let last_chunk_sha256 = sha(&w.last_chunk_sha256, "last_chunk_sha256")?;
    if w.last_chunk_key.is_some() != last_chunk_sha256.is_some() {
        return Err("last_chunk_key and last_chunk_sha256 must both be set or both null".into());
    }
    let snapshot = match (&w.snapshot_key, w.snapshot_offset, &w.snapshot_sha256) {
        (Some(k), Some(o), Some(_)) => Some(HeadSnapshot {
            key: k.clone(),
            offset: o,
            sha256: sha(&w.snapshot_sha256, "snapshot_sha256")?.unwrap_or([0; 32]),
            state_hash: sha(&w.snapshot_state_hash, "snapshot_state_hash")?,
        }),
        (None, None, None) => None,
        _ => return Err("partial snapshot reference".into()),
    };
    Ok(Head {
        epoch: w.epoch,
        last_chunk_key: w.last_chunk_key,
        last_chunk_sha256,
        chain_start: w.chain_start,
        end: w.end,
        snapshot,
        node_id: w.node_id,
        members: w.members,
    })
}

fn bad_head(p: u16, e: impl std::fmt::Display) -> UploadError {
    UploadError::Io(format!("{}: unreadable head: {e}", head_key(p)))
}

/// Read `p/<pppp>/head` with its etag.
pub async fn read_head(
    store: &Arc<dyn ObjectStore>,
    partition: u16,
) -> Result<Option<(Head, Option<String>)>, UploadError> {
    let key = head_key(partition);
    let Some((bytes, meta)) = store.get(&key).await? else {
        return Ok(None);
    };
    let head = decode_head(&bytes).map_err(|e| bad_head(partition, e))?;
    let etag = match meta.e_tag {
        Some(e) => Some(e),
        None => store.head(&key).await?.and_then(|m| m.e_tag),
    };
    Ok(Some((head, etag)))
}

/// Outcome of [`put_head`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HeadPut {
    /// Written; the new etag.
    Written(Option<String>),
    /// The precondition failed (or the current head has a higher epoch); the current head.
    Conflict(Option<(Head, Option<String>)>),
}

/// Replace `p/<pppp>/head` with `head`, conditioned on `etag` (`None` = must be absent).
///
/// Without conditional PUT (documented, weaker fallback) the current head is re-read and the
/// write happens only if its etag matches and its epoch is not above `head.epoch`.
pub async fn put_head(
    store: &Arc<dyn ObjectStore>,
    partition: u16,
    head: &Head,
    etag: Option<&str>,
) -> Result<HeadPut, UploadError> {
    let key = head_key(partition);
    let body = encode_head(head);
    if !store.supports_conditional_put() {
        let cur = read_head(store, partition).await?;
        let matches = match (&cur, etag) {
            (None, None) => true,
            (Some((h, e)), Some(want)) => e.as_deref() == Some(want) && h.epoch <= head.epoch,
            _ => false,
        };
        if !matches {
            return Ok(HeadPut::Conflict(cur));
        }
        let out = store.put(&key, body.into()).await?;
        return Ok(HeadPut::Written(out.e_tag));
    }
    let put = match etag {
        Some(e) => store.put_if_match(&key, body.into(), e).await?,
        None => store.put_if_absent(&key, body.into()).await?,
    };
    match put {
        Conditional::Ok(o) => {
            let etag = match o.e_tag {
                Some(e) => Some(e),
                None => store.head(&key).await?.and_then(|m| m.e_tag),
            };
            Ok(HeadPut::Written(etag))
        }
        Conditional::PreconditionFailed => {
            Ok(HeadPut::Conflict(read_head(store, partition).await?))
        }
    }
}

/// One chunk of a reachable chain.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChainLink {
    /// Object key.
    pub key: String,
    /// First offset.
    pub base: u64,
    /// Exclusive end.
    pub end: u64,
}

/// What a takeover needs to know (see [`reconcile_on_takeover`]).
pub struct Takeover<'a, L> {
    /// The bucket.
    pub store: &'a Arc<dyn ObjectStore>,
    /// The partition.
    pub partition: u16,
    /// The leader epoch this node leads at.
    pub epoch: u32,
    /// This node.
    pub node_id: u64,
    /// Cluster member count (recorded in the head).
    pub members: u64,
    /// The partition's local log (the new leader's committed log is the truth).
    pub log: &'a L,
    /// Local snapshots of the partition, ascending (re-chain candidates).
    pub snapshots: &'a [LocalSnapshot],
    /// Test hook: stop after the repair objects are written, before the head PUT (a crash).
    #[doc(hidden)]
    pub stop_before_head: bool,
}

/// The partition's remote state after a takeover.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// The head (`None` = nothing uploaded for this partition yet).
    pub head: Option<Head>,
    /// Its etag (`None` with `head: Some` = the head was derived from `manifest/current` and no
    /// head object exists yet).
    pub etag: Option<String>,
    /// The reachable chain, oldest first.
    pub chain: Vec<ChainLink>,
    /// The first divergent offset, when a divergent tail was repaired.
    pub repaired_at: Option<u64>,
    /// The re-cut chunk's key, when a straddling chunk was re-cut.
    pub recut: Option<String>,
    /// The new chain start, when the chain was restarted from a snapshot (gap or unusable
    /// chain).
    pub rechained_at: Option<u64>,
    /// Objects deleted after the new head became durable.
    pub deleted: Vec<String>,
}

impl Reconciled {
    /// Whether the takeover wrote anything (a head move).
    pub fn wrote(&self) -> bool {
        self.repaired_at.is_some() || self.rechained_at.is_some()
    }
}

/// Outcome of [`reconcile_on_takeover`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TakeoverOutcome {
    /// The remote chain equals a prefix of the local committed log.
    Ready(Box<Reconciled>),
    /// A writer with a higher epoch owns the head: do not upload this partition.
    Fenced {
        /// The head's epoch.
        head_epoch: u32,
    },
    /// A re-chain is needed but no local snapshot lies in `[log_start, HW]`.
    NoSnapshot {
        /// The local log start.
        log_start: u64,
        /// The consistent remote tail end.
        tail_end: u64,
    },
    /// The test hook stopped the takeover before the head PUT.
    Interrupted,
}

/// The local log, read envelope by envelope from a position on.
struct LocalCursor<'a, L> {
    log: &'a L,
    buf: VecDeque<(u64, Vec<u8>)>,
}

impl<L: ReplicatedLog> LocalCursor<'_, L> {
    /// The local envelope starting at `base` (`None` = the log has no batch starting there
    /// below HW).
    async fn at(&mut self, base: u64) -> Result<Option<Vec<u8>>, UploadError> {
        while self.buf.front().is_some_and(|(b, _)| *b < base) {
            self.buf.pop_front();
        }
        if self.buf.is_empty() {
            let batches = self
                .log
                .read_envelopes(Offset(base), chunk::MAX_CHUNK_BYTES)
                .await
                .map_err(|e| UploadError::Log(e.to_string()))?;
            for rb in batches {
                for env in rb.envelopes {
                    let (b, _) = chunk::envelope_span(&env)?;
                    self.buf.push_back((b, env.to_vec()));
                }
            }
        }
        Ok(match self.buf.front() {
            Some((b, e)) if *b == base => Some(e.clone()),
            _ => None,
        })
    }
}

/// Where data written under `epoch` must end in the local log: the HW for the latest local
/// epoch, `end_offset_for_epoch(epoch)` (capped at HW) for an earlier known one, HW for an
/// epoch older than the retained history (unknowable), and 0 for an epoch the local log never
/// had (another leader's data).
fn valid_end(hist: &EpochHistory, epoch: u32, hw: u64) -> u64 {
    if hist.latest() == Some(Epoch(epoch)) {
        return hw;
    }
    if hist.0.iter().any(|(e, _)| e.0 == epoch) {
        return hist
            .end_offset_for_epoch(Epoch(epoch))
            .map_or(hw, |o| o.0.min(hw));
    }
    match hist.0.first() {
        Some((oldest, _)) if epoch < oldest.0 => hw,
        _ => 0,
    }
}

/// The head of a bucket without a head object: the `manifest/current` entry of `p` (a Phase 8
/// bucket), at epoch 0.
async fn head_from_manifest(
    store: &Arc<dyn ObjectStore>,
    partition: u16,
) -> Result<Option<Head>, UploadError> {
    let Some((bytes, _)) = store.get(manifest::CURRENT_KEY).await? else {
        return Ok(None);
    };
    let m = manifest::decode(&bytes)?;
    Ok(m.partition(partition).map(|e| Head {
        epoch: 0,
        last_chunk_key: e.last_chunk_key.clone(),
        last_chunk_sha256: e.last_chunk_sha256,
        chain_start: e.chain_start,
        end: e.cut_offset,
        snapshot: match (&e.snapshot_key, e.snapshot_offset, e.snapshot_sha256) {
            (Some(k), Some(o), Some(s)) => Some(HeadSnapshot {
                key: k.clone(),
                offset: o,
                sha256: s,
                state_hash: (e.verify_offset == o).then_some(e.verify_state_hash),
            }),
            _ => None,
        },
        node_id: m.node_id,
        members: 1,
    }))
}

fn links(chain: &[ChunkRef]) -> Vec<ChainLink> {
    chain
        .iter()
        .map(|c| ChainLink {
            key: c.key.clone(),
            base: c.chunk.header.base,
            end: c.chunk.header.end,
        })
        .collect()
}

/// Reconcile partition `t.partition`'s remote chain with the local log when this node starts
/// leading it at `t.epoch` (see the module docs). Runs before the first upload of the term.
pub async fn reconcile_on_takeover<L: ReplicatedLog>(
    t: Takeover<'_, L>,
) -> Result<TakeoverOutcome, UploadError> {
    for _ in 0..RECONCILE_ATTEMPTS {
        if let Some(out) = reconcile_once(&t).await? {
            return Ok(out);
        }
    }
    Err(UploadError::Io(format!(
        "{}: head moved {RECONCILE_ATTEMPTS} times during the takeover",
        head_key(t.partition)
    )))
}

/// One attempt; `None` = the head moved under us (lower or equal epoch), retry.
async fn reconcile_once<L: ReplicatedLog>(
    t: &Takeover<'_, L>,
) -> Result<Option<TakeoverOutcome>, UploadError> {
    let p = t.partition;
    let store = t.store;
    let (head, etag) = match read_head(store, p).await? {
        Some((h, e)) => (Some(h), e),
        None => (head_from_manifest(store, p).await?, None),
    };
    let Some(head) = head else {
        return Ok(Some(TakeoverOutcome::Ready(Box::default())));
    };
    if head.epoch > t.epoch {
        return Ok(Some(TakeoverOutcome::Fenced {
            head_epoch: head.epoch,
        }));
    }

    // Walk back from the head.
    let (chain, walkable) = match (&head.last_chunk_key, head.last_chunk_sha256) {
        (Some(k), Some(s)) => match chunk::walk_back(store, k, s, head.chain_start).await {
            Ok(c)
                if c.first()
                    .is_some_and(|f| f.chunk.header.base <= head.chain_start) =>
            {
                (c, true)
            }
            Ok(_) => {
                warn!(
                    partition = p,
                    "remote chain does not reach its chain start; re-chaining"
                );
                (Vec::new(), false)
            }
            Err(e) => {
                warn!(partition = p, error = %e, "remote chain unusable; re-chaining");
                (Vec::new(), false)
            }
        },
        _ => (Vec::new(), true),
    };

    // First divergent envelope (oldest first).
    let log_start = t.log.log_start().0;
    let hw = t.log.high_watermark().borrow().0;
    let hist = t.log.epochs();
    let mut cursor = LocalCursor {
        log: t.log,
        buf: VecDeque::new(),
    };
    let mut cut: Option<(usize, u64)> = None;
    'chain: for (i, c) in chain.iter().enumerate() {
        let ve = valid_end(&hist, c.chunk.header.epoch, hw);
        for env in &c.chunk.envelopes {
            let (eb, ee) = chunk::envelope_span(env)?;
            if ee > ve {
                cut = Some((i, eb));
                break 'chain;
            }
            if eb < log_start {
                continue;
            }
            match cursor.at(eb).await? {
                Some(local) if local == *env => {}
                _ => {
                    cut = Some((i, eb));
                    break 'chain;
                }
            }
        }
    }

    let mut tail = links(&chain);
    let mut recut = None;
    let mut doomed: Vec<String> = Vec::new();
    if let Some((i, d)) = cut {
        doomed.extend(tail.drain(i..).map(|l| l.key));
        if let Some((h, bytes)) = chunk::recut_prefix(&chain[i].chunk, d)? {
            let key = chunk::chunk_key(p, h.epoch, h.base, h.end);
            recut = Some((key, chunk::chunk_sha(&bytes), h, bytes));
        }
    }
    let tail_end = match (&recut, tail.last()) {
        (Some((_, _, h, _)), _) => h.end,
        (None, Some(l)) => l.end,
        (None, None) => head.chain_start,
    };

    if !walkable || tail_end < log_start || tail_end > hw {
        return rechain(t, &head, etag, &chain, walkable, tail_end).await;
    }
    let Some((_, at)) = cut else {
        // Consistent: nothing to write.
        return Ok(Some(TakeoverOutcome::Ready(Box::new(Reconciled {
            chain: tail,
            head: Some(head),
            etag,
            ..Reconciled::default()
        }))));
    };

    // Repair: the consistent prefix of a straddling chunk becomes a new chunk.
    let (last_key, last_sha) = match recut {
        Some((key, sha, h, bytes)) => {
            crate::fence::put_immutable(store, &key, bytes, sha).await?;
            tail.push(ChainLink {
                key: key.clone(),
                base: h.base,
                end: h.end,
            });
            (Some(key), Some(sha))
        }
        None => match tail.last() {
            Some(_) => {
                let prev = &chain[tail.len() - 1];
                (Some(prev.key.clone()), Some(prev.chunk.sha256))
            }
            None => (None, None),
        },
    };
    if t.stop_before_head {
        return Ok(Some(TakeoverOutcome::Interrupted));
    }
    let recut_key = last_key
        .clone()
        .filter(|k| !chain.iter().any(|c| c.key == *k));
    let new = Head {
        epoch: t.epoch,
        last_chunk_key: last_key,
        last_chunk_sha256: last_sha,
        chain_start: head.chain_start,
        end: tail_end,
        snapshot: head.snapshot.clone().filter(|s| s.offset <= tail_end),
        node_id: t.node_id,
        members: t.members,
    };
    let Some(etag) = commit_head(t, &new, etag.as_deref()).await? else {
        return Ok(None);
    };
    let etag = match etag {
        Ok(e) => e,
        Err(fenced) => return Ok(Some(fenced)),
    };
    info!(
        partition = p,
        epoch = t.epoch,
        divergent_from = at,
        tail_end,
        dropped = doomed.len(),
        "bottomless takeover repaired a divergent remote tail"
    );
    let deleted = delete_after_head(store, p, doomed).await;
    Ok(Some(TakeoverOutcome::Ready(Box::new(Reconciled {
        head: Some(new),
        etag,
        chain: tail,
        repaired_at: Some(at),
        recut: recut_key,
        rechained_at: None,
        deleted,
    }))))
}

/// PUT the head. `None` = moved by an equal-or-lower epoch (retry); `Some(Err(Fenced))` = a
/// higher epoch owns it; `Some(Ok(etag))` = written.
async fn commit_head<L>(
    t: &Takeover<'_, L>,
    new: &Head,
    etag: Option<&str>,
) -> Result<Option<Result<Option<String>, TakeoverOutcome>>, UploadError> {
    match put_head(t.store, t.partition, new, etag).await? {
        HeadPut::Written(e) => Ok(Some(Ok(e))),
        HeadPut::Conflict(Some((cur, _))) if cur.epoch > t.epoch => {
            Ok(Some(Err(TakeoverOutcome::Fenced {
                head_epoch: cur.epoch,
            })))
        }
        HeadPut::Conflict(_) => Ok(None),
    }
}

/// Restart the chain at a local snapshot (W4 gap, or an unusable chain).
async fn rechain<L: ReplicatedLog>(
    t: &Takeover<'_, L>,
    head: &Head,
    etag: Option<String>,
    chain: &[ChunkRef],
    walkable: bool,
    tail_end: u64,
) -> Result<Option<TakeoverOutcome>, UploadError> {
    let p = t.partition;
    let store = t.store;
    let log_start = t.log.log_start().0;
    let hw = t.log.high_watermark().borrow().0;
    let mut snap = None;
    for ls in t.snapshots.iter().rev() {
        let o = ls.applied_offset.0;
        if o < log_start || o > hw {
            continue;
        }
        match upload_snapshot_file(store, p, &ls.path).await {
            Ok(Some((s, _))) => {
                snap = Some(s);
                break;
            }
            Ok(None) => {}
            Err(UploadError::Io(e)) => {
                warn!(partition = p, error = %e, "local snapshot vanished before upload");
            }
            Err(e) => return Err(e),
        }
    }
    let start = match &snap {
        Some(s) => s.offset,
        None if log_start == 0 => 0,
        None => {
            return Ok(Some(TakeoverOutcome::NoSnapshot {
                log_start,
                tail_end,
            }));
        }
    };
    if t.stop_before_head {
        return Ok(Some(TakeoverOutcome::Interrupted));
    }
    let new = Head {
        epoch: t.epoch,
        last_chunk_key: None,
        last_chunk_sha256: None,
        chain_start: start,
        end: start,
        snapshot: snap.map(|s| HeadSnapshot {
            key: s.key,
            offset: s.offset,
            sha256: s.sha,
            state_hash: s.state_hash,
        }),
        node_id: t.node_id,
        members: t.members,
    };
    let Some(etag) = commit_head(t, &new, etag.as_deref()).await? else {
        return Ok(None);
    };
    let etag = match etag {
        Ok(e) => e,
        Err(fenced) => return Ok(Some(fenced)),
    };
    info!(
        partition = p,
        epoch = t.epoch,
        chain_start = start,
        log_start,
        old_tail_end = tail_end,
        old_chain_start = head.chain_start,
        "bottomless takeover re-chained from a snapshot"
    );
    let mut doomed: Vec<String> = chain.iter().map(|c| c.key.clone()).collect();
    if !walkable {
        match store.list(&chunk::seg_prefix(p), None).await {
            Ok(l) => doomed.extend(l.into_iter().map(|m| m.key)),
            Err(e) => warn!(partition = p, error = %e, "re-chain: seg/ listing failed"),
        }
        doomed.sort();
        doomed.dedup();
    }
    let deleted = delete_after_head(store, p, doomed).await;
    Ok(Some(TakeoverOutcome::Ready(Box::new(Reconciled {
        head: Some(new),
        etag,
        chain: Vec::new(),
        repaired_at: None,
        recut: None,
        rechained_at: Some(start),
        deleted,
    }))))
}

/// Delete objects made unreachable by a head move (called only after the head PUT returned).
/// A failure is logged; remote retention removes leftovers later.
async fn delete_after_head(
    store: &Arc<dyn ObjectStore>,
    partition: u16,
    doomed: Vec<String>,
) -> Vec<String> {
    if doomed.is_empty() {
        return doomed;
    }
    match store.delete_many(&doomed).await {
        Ok(()) => doomed,
        Err(e) => {
            warn!(partition, error = %e, "deleting unreachable chunks failed; retention retries");
            Vec::new()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_round_trip() {
        let h = Head {
            epoch: 3,
            last_chunk_key: Some(chunk::chunk_key(1, 3, 0, 5)),
            last_chunk_sha256: Some([7; 32]),
            chain_start: 0,
            end: 5,
            snapshot: Some(HeadSnapshot {
                key: "p/0001/snap/x".into(),
                offset: 0,
                sha256: [1; 32],
                state_hash: None,
            }),
            node_id: 2,
            members: 3,
        };
        let b = encode_head(&h);
        assert_eq!(decode_head(&b).expect("decode"), h);
        let v: serde_json::Value = serde_json::from_slice(&b).expect("json");
        assert_eq!(v["epoch"], 3);
        assert_eq!(v["last_chunk_sha256"], hex(&[7; 32]));
        let empty = Head {
            last_chunk_key: None,
            last_chunk_sha256: None,
            snapshot: None,
            ..h
        };
        assert_eq!(decode_head(&encode_head(&empty)).expect("decode"), empty);
        assert!(decode_head(br#"{"format":2,"epoch":1,"last_chunk_key":null,"last_chunk_sha256":null,"chain_start":0,"end":0}"#).is_err());
        assert_eq!(head_key(7), "p/0007/head");
    }

    #[test]
    fn valid_end_rules() {
        let h = EpochHistory(vec![
            (Epoch(1), Offset(0)),
            (Epoch(2), Offset(5)),
            (Epoch(4), Offset(7)),
        ]);
        assert_eq!(valid_end(&h, 4, 9), 9, "latest epoch: HW");
        assert_eq!(valid_end(&h, 2, 9), 7, "earlier epoch: its end");
        assert_eq!(valid_end(&h, 1, 9), 5);
        assert_eq!(valid_end(&h, 3, 9), 0, "never in the local log");
        assert_eq!(valid_end(&h, 5, 9), 0, "newer than the local log");
        let trimmed = EpochHistory(vec![(Epoch(4), Offset(7))]);
        assert_eq!(
            valid_end(&trimmed, 2, 9),
            9,
            "older than the retained history"
        );
        assert_eq!(valid_end(&EpochHistory::default(), 1, 0), 0);
    }
}
