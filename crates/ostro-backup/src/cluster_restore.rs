//! Whole-cluster restore from a multi-member bucket (plan 14-30, D-P14-45, BACK-05).
//!
//! A multi-member cluster's bucket has one chain per partition (`p/<pppp>/head`, 12-15), the
//! metadata leader's snapshots and `meta/head` (14-29), and no `manifest/current`. `ostro
//! restore` detects such a bucket inside [`crate::restore::restore`] and calls
//! [`restore_cluster`], which rebuilds **one seed** data directory; the operator then grows
//! the cluster back with `ostro cluster join` (docs/cluster-restore.md).
//!
//! 1. **Partitions.** Per partition: read the head, walk the chain back to `chain_start` and
//!    verify it ([`chunk::walk_back`]); the restorable starts are offset 0 (when the chain
//!    starts there) and every remote snapshot inside `[chain_start, end]`; `floor_p` is the
//!    lowest. Import `[floor_p, end_p)` into a scan directory and replay it in Inspect mode with
//!    a [`CursorRecorders`] observer (14-29): the cross-partition cursors at every offset.
//! 2. **Cut.** The step functions are sampled at envelope boundaries (a restore can only stop
//!    between envelopes) and [`cluster_cut::search`] finds the greatest Pitfall-1-consistent
//!    cut `c_p` in `[floor_p, end_p]`; no such cut is [`RestoreError::NoConsistentCut`].
//!    Values are constant between boundaries, so rounding a result down to its boundary keeps
//!    every cursor and the cut stays consistent.
//! 3. **Restore at the cut.** Per partition the newest restorable start `S <= c_p` (snapshot,
//!    sha verified, its recorded state hash compared after the install; or 0), the envelopes
//!    `[S, c_p)` into a fresh WAL, Inspect replay: the state at the cut. The cursors must equal
//!    the scan's at `c_p` (two independent replays agree), and the Phase 8 Pitfall 1 check runs
//!    on the final states (it always passes; a failure is a bug and fails the restore).
//! 4. **Metadata.** Candidates are `meta/head` (sha verified) and then the older kept meta
//!    snapshots, newest first. The first one whose workflow versions and modules cover every
//!    non-terminal run at the cut (workflow version present, module record present and the
//!    module's `blobs/<sha>` in the bucket) is chosen; none is [`RestoreError::Meta`].
//! 5. **Rewrite** (existing records and ext tags only, D-P14-51; applied through
//!    [`MetaState::apply`] at the snapshot index): the seed member (`--node-id`, else partition
//!    0's head writer) Active (ext 97) with its addresses cleared (the server re-upserts them at
//!    start); every other member `Removed` (ext 97, ids never reused); every partition
//!    `replicas = isr = [seed]`, `leader = 0`, `isr_version + 1`, `min_insync = 1`, no
//!    in-flight reassignment (ext 100 dropped), and `epoch = max(snapshot assignment epoch,
//!    head epoch, highest epoch in the partition's chain and restored snapshot)`, so the
//!    controller's `ElectLeader` at `epoch + 1` is above every restored epoch even when an older
//!    covering metadata snapshot was chosen. `ClusterInfo` (cluster id, ReplicationPolicy ext
//!    99, ClusterVersion ext 98) and every other record are kept. The result is bootstrapped as
//!    a single-voter metadata group.
//! 6. `node.toml` as Phase 8 (new incarnation, `restored_from`) plus the kept `cluster_id`;
//!    the cluster CA stored in metadata (`__ostro/cluster-ca`, when the cluster had one) is
//!    written to `tls/ca.{crt,key}` so the seed and the nodes that join it keep the CA; blobs
//!    sha verified.
//!
//! `--at` is refused for cluster buckets (no common time base in 1.0).
//!
//! Mutant (feature `failpoints`): `repl::mutant::restore_skip_fixpoint` restores every
//! partition at its raw head end (no cut search); the final Pitfall 1 check must catch it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use ostro_log::{ImportMode, RawBatch, write_partition_log};
use ostro_meta::ext::{
    EXT_REASSIGNMENT, MEMBER_ACTIVE, MEMBER_REMOVED, MemberState, get_member_state,
    put_member_state,
};
use ostro_meta::{
    MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, MetaState, PartitionAssignment,
    ext_remove,
};
use ostro_objstore::ObjectStore;
use ostro_partition::node::{hex16, new_incarnation};
use ostro_partition::replay_observer::CursorRecorders;
use ostro_partition::store::snapshot::install_snapshot;
use ostro_partition::store::values::{RunRecord, decode};
use ostro_partition::{NodeConfig, StartMode, StoreConfig, Table};
use ostro_types::{Offset, PartitionCount, PartitionId, envelope};
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::chunk::{self, Chunk};
use crate::cluster::{self, Head};
use crate::cluster_cut::{self, CursorSteps, Cut, CutError, Lowering, StepFn};
use crate::meta_head::{self, MetaHead};
use crate::restore::{
    Final, PartitionReport, RestoreDurations, RestoreError, RestoreOptions, RestoreReport,
    check_consistency, hex, ms, open_inspect, read_final, restore_blobs, toml_str, write_durably,
};
use crate::uploader::{parse_snap_key, snap_prefix};

/// Mutant: restore every partition at its raw head end, skipping the cut search
/// (`repl::mutant::*` naming of ostro-log `mutants.rs`; local to this file).
const RESTORE_SKIP_FIXPOINT: &str = "repl::mutant::restore_skip_fixpoint";

fn skip_fixpoint() -> bool {
    #[cfg(feature = "failpoints")]
    fail::fail_point!(RESTORE_SKIP_FIXPOINT, |_| true);
    let _ = RESTORE_SKIP_FIXPOINT;
    false
}

/// The scan directory (inside the target data dir, removed before the final restore).
const SCAN_DIR: &str = ".cluster-restore-scan";

/// One partition in [`ClusterRestoreReport`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClusterPartitionReport {
    /// Partition id.
    pub p: u16,
    /// The head's `end` (what the bucket holds).
    pub head_end: u64,
    /// The head's writer epoch.
    pub head_epoch: u32,
    /// Lowest restorable cut (oldest reachable snapshot or chain start 0).
    pub floor: u64,
    /// The restored cut.
    pub cut: u64,
    /// Offsets above the cut that were dropped (`head_end - cut`, the partition's RPO).
    pub dropped: u64,
    /// Why the cut was lowered below the head (last lowering), if it was.
    pub lowered_reason: Option<String>,
    /// The assignment epoch written to the seed's metadata.
    pub epoch: u32,
}

/// The cluster part of [`RestoreReport`] (absent for a Phase 8 single-member restore).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ClusterRestoreReport {
    /// Metadata raft index of the snapshot restored.
    pub meta_index: u64,
    /// Its metadata term (`meta/head`'s; `None` for an older kept snapshot).
    pub meta_term: Option<u64>,
    /// Whether `meta/head` was chosen (false: an older covering snapshot).
    pub meta_from_head: bool,
    /// The seed node id.
    pub seed: u64,
    /// Members tombstoned `Removed` by the rewrite.
    pub removed_members: Vec<u64>,
    /// Non-terminal runs at the cut (their module pins were checked).
    pub open_runs: u64,
    /// The cluster CA stored in metadata was written to `<data_dir>/tls/ca.{crt,key}`.
    pub ca_kept: bool,
    /// Per partition.
    pub partitions: Vec<ClusterPartitionReport>,
}

/// What [`detect`] found.
#[derive(Debug)]
pub(crate) struct ClusterBucket {
    meta_head: Option<MetaHead>,
    heads: Vec<String>,
}

/// Whether the bucket is a multi-member cluster bucket: a `meta/head` newer than any
/// `manifest/current`, or per-partition heads without a usable `manifest/current`.
pub(crate) async fn detect(
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<ClusterBucket>, RestoreError> {
    let src = |e: ostro_objstore::Error| RestoreError::Source(e.to_string());
    let current = match store.get(crate::manifest::CURRENT_KEY).await.map_err(src)? {
        Some((b, _)) => crate::manifest::decode(&b).ok(),
        None => None,
    };
    let head = meta_head::get(store)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .map(|(h, _)| h);
    if let Some(h) = &head
        && current.as_ref().is_none_or(|m| m.cut_at_ms < h.at_ms)
    {
        let heads = head_keys(store).await?;
        return Ok(Some(ClusterBucket {
            meta_head: head,
            heads,
        }));
    }
    if current.is_some() {
        return Ok(None);
    }
    let heads = head_keys(store).await?;
    if heads.is_empty() {
        return Ok(None);
    }
    Ok(Some(ClusterBucket {
        meta_head: head,
        heads,
    }))
}

async fn head_keys(store: &Arc<dyn ObjectStore>) -> Result<Vec<String>, RestoreError> {
    Ok(store
        .list("p/", None)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .into_iter()
        .map(|m| m.key)
        .filter(|k| {
            k.strip_prefix("p/")
                .and_then(|r| r.split_once('/'))
                .is_some_and(|(p, rest)| p.len() == 4 && rest == "head")
        })
        .collect())
}

/// A metadata snapshot candidate.
struct MetaCandidate {
    index: u64,
    key: String,
    /// `meta/head`'s sha and term (the head candidate only).
    head: Option<([u8; 32], u64)>,
}

async fn meta_candidates(
    store: &Arc<dyn ObjectStore>,
    head: Option<&MetaHead>,
) -> Result<Vec<MetaCandidate>, RestoreError> {
    let mut out = Vec::new();
    if let Some(h) = head {
        out.push(MetaCandidate {
            index: h.index,
            key: h.key.clone(),
            head: Some((h.sha256, h.term)),
        });
    }
    let mut older: Vec<(u64, String)> = store
        .list("meta/", None)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .into_iter()
        .filter_map(|m| meta_head::parse_meta_key(&m.key).map(|i| (i, m.key)))
        .filter(|(i, _)| head.is_none_or(|h| *i < h.index))
        .collect();
    older.sort_unstable_by_key(|(i, _)| std::cmp::Reverse(*i));
    out.extend(older.into_iter().map(|(index, key)| MetaCandidate {
        index,
        key,
        head: None,
    }));
    Ok(out)
}

async fn load_meta(
    store: &Arc<dyn ObjectStore>,
    c: &MetaCandidate,
) -> Result<MetaState, RestoreError> {
    let (b, _) = store
        .get(&c.key)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .ok_or_else(|| RestoreError::Meta(format!("{} is missing", c.key)))?;
    if let Some((sha, _)) = c.head
        && chunk::chunk_sha(&b) != sha
    {
        return Err(RestoreError::Meta(format!(
            "{}: sha256 does not match meta/head",
            c.key
        )));
    }
    ostro_meta::decode_snapshot(&b).map_err(|e| RestoreError::Meta(format!("{}: {e}", c.key)))
}

/// A restorable start of a partition: offset 0 (empty) or a remote snapshot.
#[derive(Clone, Debug)]
struct Start {
    offset: u64,
    /// `(key, sha256, recorded state hash)`; `None` = the empty start at 0.
    snapshot: Option<(String, [u8; 32], Option<[u8; 32]>)>,
}

/// One partition's bucket view.
struct Part {
    p: u16,
    head: Head,
    chunks: Vec<Chunk>,
    /// Ascending by offset.
    starts: Vec<Start>,
    floor: u64,
    /// Envelope boundaries in `[floor, end]`, ascending.
    boundaries: Vec<u64>,
    /// Highest envelope epoch of the chain.
    max_epoch: u32,
}

async fn read_part(store: Arc<dyn ObjectStore>, p: u16) -> Result<Part, RestoreError> {
    let chain_err = |reason: String| RestoreError::Chain {
        partition: p,
        reason,
    };
    let head = cluster::read_head(&store, p)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .map(|(h, _)| h)
        .ok_or_else(|| {
            RestoreError::BadManifest(format!(
                "partition {p} has no {} object",
                cluster::head_key(p)
            ))
        })?;
    let mut chunks = Vec::new();
    if let (Some(k), Some(sha)) = (&head.last_chunk_key, head.last_chunk_sha256) {
        let refs = chunk::walk_back(&store, k, sha, head.chain_start)
            .await
            .map_err(|e| chain_err(e.to_string()))?;
        chunks = refs.into_iter().map(|r| r.chunk).collect::<Vec<_>>();
        let first = chunks.first().map_or(head.chain_start, |c| c.header.base);
        let last = chunks.last().map_or(head.chain_start, |c| c.header.end);
        if first > head.chain_start {
            return Err(chain_err(format!(
                "the chain starts at {first} above its chain_start {}",
                head.chain_start
            )));
        }
        if last != head.end {
            return Err(chain_err(format!(
                "the chain ends at {last}, the head says {}",
                head.end
            )));
        }
    } else if head.end != head.chain_start {
        return Err(chain_err(format!(
            "no chunks but the head ends at {} above chain_start {}",
            head.end, head.chain_start
        )));
    }

    // Restorable starts.
    let mut starts: BTreeMap<u64, Start> = BTreeMap::new();
    if head.chain_start == 0 {
        starts.insert(
            0,
            Start {
                offset: 0,
                snapshot: None,
            },
        );
    }
    let listed = store
        .list(&snap_prefix(p), None)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?;
    for m in listed {
        if let Some((q, off, sha)) = parse_snap_key(&m.key)
            && q == p
            && off >= head.chain_start
            && off <= head.end
        {
            starts.entry(off).or_insert(Start {
                offset: off,
                snapshot: Some((m.key, sha, None)),
            });
        }
    }
    if let Some(s) = &head.snapshot
        && s.offset >= head.chain_start
        && s.offset <= head.end
    {
        starts.insert(
            s.offset,
            Start {
                offset: s.offset,
                snapshot: Some((s.key.clone(), s.sha256, s.state_hash)),
            },
        );
    }
    let starts: Vec<Start> = starts.into_values().collect();
    let floor = starts.first().map(|s| s.offset).ok_or_else(|| {
        chain_err(format!(
            "no restorable start: the chain starts at {} and no remote snapshot lies in [{}, {}]",
            head.chain_start, head.chain_start, head.end
        ))
    })?;

    let mut boundaries = BTreeSet::from([floor]);
    let mut max_epoch = 0u32;
    for c in &chunks {
        max_epoch = max_epoch.max(c.header.epoch);
        for env in &c.envelopes {
            let h = envelope::peek_header(env).map_err(|e| chain_err(e.to_string()))?;
            max_epoch = max_epoch.max(h.epoch.0);
            let end = h.base.0 + u64::from(h.cmd_count);
            if end > floor {
                boundaries.insert(end);
            }
        }
    }
    // A snapshot start must be an envelope boundary of the chain.
    for s in &starts {
        if !boundaries.contains(&s.offset) {
            return Err(chain_err(format!(
                "snapshot offset {} is not an envelope boundary of the chain",
                s.offset
            )));
        }
    }
    Ok(Part {
        p,
        head,
        chunks,
        starts,
        floor,
        boundaries: boundaries.into_iter().collect(),
        max_epoch,
    })
}

/// The chain's envelopes in `[from, upto)` as raw batches (one per epoch run).
fn batches(part: &Part, from: u64, upto: u64) -> Result<Vec<RawBatch>, RestoreError> {
    let mut out: Vec<RawBatch> = Vec::new();
    for c in &part.chunks {
        for env in &c.envelopes {
            let h = envelope::peek_header(env).map_err(|e| RestoreError::Chain {
                partition: part.p,
                reason: e.to_string(),
            })?;
            let (base, end) = (h.base.0, h.base.0 + u64::from(h.cmd_count));
            if end <= from || base >= upto {
                continue;
            }
            if base < from || end > upto {
                return Err(RestoreError::Chain {
                    partition: part.p,
                    reason: format!("[{from}, {upto}) cuts the batch [{base}, {end})"),
                });
            }
            match out.last_mut() {
                Some(b) if b.epoch == h.epoch.0 && b.end.0 == base => {
                    b.envelopes.push(env.clone().into());
                    b.end = Offset(end);
                }
                _ => out.push(RawBatch {
                    base: Offset(base),
                    end: Offset(end),
                    epoch: h.epoch.0,
                    envelopes: vec![env.clone().into()],
                }),
            }
        }
    }
    Ok(out)
}

/// Install `start` (if a snapshot) and import `[start, upto)` for one partition.
/// Returns the installed snapshot's latest epoch (0 for the empty start).
async fn materialise(
    store: &Arc<dyn ObjectStore>,
    dir: &Path,
    count: PartitionCount,
    part: &Part,
    start: &Start,
    upto: u64,
) -> Result<u32, RestoreError> {
    let p = part.p;
    let pid = PartitionId(p);
    let mut latest_epoch = 0;
    if let Some((key, sha, recorded)) = &start.snapshot {
        let (bytes, _) = store
            .get(key)
            .await
            .map_err(|e| RestoreError::Source(e.to_string()))?
            .ok_or_else(|| RestoreError::Snapshot {
                partition: p,
                reason: format!("{key} is missing"),
            })?;
        if chunk::chunk_sha(&bytes) != *sha {
            return Err(RestoreError::SnapshotSha {
                partition: p,
                key: key.clone(),
            });
        }
        let d = dir.to_path_buf();
        let info = tokio::task::spawn_blocking(move || {
            install_snapshot(
                &d,
                pid,
                count,
                &StoreConfig::default(),
                std::io::Cursor::new(bytes),
            )
        })
        .await
        .map_err(|e| RestoreError::Io(e.to_string()))?
        .map_err(|e| RestoreError::Snapshot {
            partition: p,
            reason: e.to_string(),
        })?;
        if info.applied_offset != start.offset {
            return Err(RestoreError::Snapshot {
                partition: p,
                reason: format!(
                    "{key}: applied_offset {} != its key's offset {}",
                    info.applied_offset, start.offset
                ),
            });
        }
        if let Some(h) = recorded
            && *h != info.state_hash
        {
            return Err(RestoreError::HashMismatch {
                partition: p,
                offset: start.offset,
                expected: hex(h),
                found: hex(&info.state_hash),
            });
        }
        latest_epoch = info.latest_epoch;
    }
    let wal = dir.join("wal");
    let b = batches(part, start.offset, upto)?;
    tokio::task::spawn_blocking(move || {
        write_partition_log(&wal, pid, b, Offset(upto), ImportMode::Create)
    })
    .await
    .map_err(|e| RestoreError::Io(e.to_string()))?
    .map_err(|e| RestoreError::Import {
        partition: p,
        reason: e.to_string(),
    })?;
    Ok(latest_epoch)
}

fn write_node_toml(
    dir: &Path,
    partitions: u16,
    inc: &[u8; 16],
    restored_from: &str,
    cluster_id: Option<&str>,
) -> Result<(), RestoreError> {
    let mut s = format!(
        "format_version = 1\npartitions = {partitions}\nincarnation = \"{}\"\nrestored_from = {}\n",
        hex16(inc),
        toml_str(restored_from)
    );
    if let Some(id) = cluster_id {
        s.push_str(&format!("cluster_id = {}\n", toml_str(id)));
    }
    write_durably(dir, "node.toml", s.as_bytes())
}

/// The step functions sampled at `boundaries` only (the value at each boundary is exact).
fn at_boundaries(steps: &CursorSteps, boundaries: &[u64]) -> CursorSteps {
    let sample = |m: &BTreeMap<u16, StepFn>, default: u64| -> BTreeMap<u16, StepFn> {
        m.iter()
            .map(|(q, f)| {
                let mut g = StepFn::default();
                for &b in boundaries {
                    g.push_change(b, f.at(b, default), default);
                }
                (*q, g)
            })
            .filter(|(_, g)| !g.0.is_empty())
            .collect()
    };
    CursorSteps {
        floor: steps.floor,
        end: steps.end,
        next: sample(&steps.next, cluster_cut::NEXT_DEFAULT),
        truncated: sample(&steps.truncated, cluster_cut::ZERO_DEFAULT),
        seen: sample(&steps.seen, cluster_cut::ZERO_DEFAULT),
    }
}

/// The greatest boundary `<= c`.
fn round_down(boundaries: &[u64], c: u64) -> u64 {
    let i = boundaries.partition_point(|b| *b <= c);
    if i == 0 { c } else { boundaries[i - 1] }
}

fn lowering_text(l: Lowering) -> String {
    match l {
        Lowering::Unsent { from } => {
            format!("saw a message partition {from} had not sent at its cut")
        }
        Lowering::Untruncated { to } => {
            format!("had truncated its outbox to partition {to} beyond what {to} saw at its cut")
        }
    }
}

/// A module pin of a non-terminal run at the cut.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Pin {
    workflow: String,
    version: u32,
    module: Option<[u8; 32]>,
    run: String,
}

fn open_runs(node: &ostro_partition::Node, p: u16) -> Result<Vec<Pin>, RestoreError> {
    let h = node
        .handle(PartitionId(p))
        .ok_or_else(|| RestoreError::Replay {
            partition: p,
            reason: "not running".into(),
        })?;
    let rows = h
        .reader()
        .prefix(Table::Runs, b"", usize::MAX)
        .map_err(|e| RestoreError::Replay {
            partition: p,
            reason: e.to_string(),
        })?;
    let mut out = Vec::new();
    for (k, v) in rows {
        let r: RunRecord = decode(&v).map_err(|e| RestoreError::Replay {
            partition: p,
            reason: format!("run record: {e}"),
        })?;
        if r.status.is_terminal() {
            continue;
        }
        out.push(Pin {
            workflow: r.workflow,
            version: r.workflow_version,
            module: r.module_sha,
            run: hex(&k),
        });
    }
    Ok(out)
}

/// The first pin `state` does not cover (workflow version, module record, module blob).
fn uncovered<'a>(state: &MetaState, pins: &'a [Pin], blobs: &BTreeSet<String>) -> Vec<&'a Pin> {
    pins.iter()
        .filter(|pin| {
            let Some(wf) = state.workflow(&pin.workflow, pin.version) else {
                return true;
            };
            let sha = pin.module.unwrap_or(wf.value.module_sha256);
            state.module(&sha).is_none() || !blobs.contains(&hex(&sha))
        })
        .collect()
}

fn apply_ok(state: &mut MetaState, index: u64, cmd: MetaCommand) -> Result<(), RestoreError> {
    let what = format!("{cmd:?}");
    match state.apply(index, cmd) {
        MetaResult::Ok { .. } => Ok(()),
        other => Err(RestoreError::Meta(format!(
            "metadata rewrite: {what} -> {other:?}"
        ))),
    }
}

/// The rewrite of step 5 (module docs). Returns the removed member ids.
fn rewrite_meta(
    state: &mut MetaState,
    index: u64,
    seed: u64,
    epochs: &BTreeMap<u16, u32>,
    now_ms: u64,
) -> Result<Vec<u64>, RestoreError> {
    let ext_err = |e: ostro_meta::ExtError| RestoreError::Meta(format!("metadata rewrite: {e}"));
    let members: Vec<_> = state.members().cloned().collect();
    let mut removed = Vec::new();
    for m in &members {
        if m.value.node_id == seed {
            continue;
        }
        if matches!(get_member_state(&m.value.ext), Ok(Some(s)) if s.state == MEMBER_REMOVED) {
            continue;
        }
        let mut rec = m.value.clone();
        rec.ext = put_member_state(
            &rec.ext,
            &MemberState {
                state: MEMBER_REMOVED,
                changed_at_ms: now_ms,
            },
        )
        .map_err(ext_err)?;
        apply_ok(
            state,
            index,
            MetaCommand::Put {
                key: MetaKey::Member(rec.node_id),
                expected_version: Some(m.version),
                record: MetaRecord::Member(rec),
            },
        )?;
        removed.push(m.value.node_id);
    }
    let active = MemberState {
        state: MEMBER_ACTIVE,
        changed_at_ms: now_ms,
    };
    match state.member(seed).cloned() {
        Some(cur) => {
            if matches!(get_member_state(&cur.value.ext), Ok(Some(s)) if s.state == MEMBER_REMOVED)
            {
                return Err(RestoreError::Meta(format!(
                    "node {seed} is a removed member of the source cluster (ids are never \
                     reused): pass another --node-id"
                )));
            }
            let mut rec = cur.value.clone();
            rec.addr = String::new();
            rec.cluster_addr = None;
            rec.repl_addr = None;
            rec.ext = put_member_state(&rec.ext, &active).map_err(ext_err)?;
            apply_ok(
                state,
                index,
                MetaCommand::Put {
                    key: MetaKey::Member(seed),
                    expected_version: Some(cur.version),
                    record: MetaRecord::Member(rec),
                },
            )?;
        }
        None => {
            let rec = ostro_meta::Member {
                node_id: seed,
                addr: String::new(),
                joined_at_ms: now_ms,
                cluster_addr: None,
                repl_addr: None,
                ext: put_member_state(&[], &active).map_err(ext_err)?,
            };
            apply_ok(
                state,
                index,
                MetaCommand::Put {
                    key: MetaKey::Member(seed),
                    expected_version: None,
                    record: MetaRecord::Member(rec),
                },
            )?;
        }
    }
    for (&p, &epoch) in epochs {
        let cur = state.partition(p).cloned();
        let (isr_version, ext) = match &cur {
            Some(c) => {
                apply_ok(
                    state,
                    index,
                    MetaCommand::Delete {
                        key: MetaKey::Partition(p),
                        expected_version: c.version,
                    },
                )?;
                let ext = ext_remove(&c.value.ext, EXT_REASSIGNMENT).map_err(ext_err)?;
                (c.value.isr_version.saturating_add(1), ext)
            }
            None => (1, Vec::new()),
        };
        apply_ok(
            state,
            index,
            MetaCommand::Put {
                key: MetaKey::Partition(p),
                expected_version: None,
                record: MetaRecord::Partition(PartitionAssignment {
                    partition: p,
                    leader: 0,
                    epoch,
                    isr: vec![seed],
                    replicas: vec![seed],
                    isr_version,
                    min_insync: 1,
                    ext,
                }),
            },
        )?;
    }
    Ok(removed)
}

/// The cluster CA secret 13-09 stores in metadata (ostro-server `p13::join`).
const CLUSTER_CA_SECRET: &str = "__ostro/cluster-ca";

/// Write the metadata's cluster CA (`__ostro/cluster-ca`: certificate PEM then key PEM) to
/// `<data_dir>/tls/ca.{crt,key}`, so the seed keeps the cluster's CA: its own certificate
/// and the certificates it issues to joiners chain to the same CA (a fresh CA would not match
/// the stored one). Returns whether a CA was written. A sealed (aes256gcm, p14-fixups-2) CA
/// opens with `open` (`RestoreOptions::open_secret`).
fn write_stored_ca(
    state: &MetaState,
    data_dir: &Path,
    open: Option<&crate::restore::SecretOpener>,
) -> Result<bool, RestoreError> {
    let Some(secret) = state.secrets().find(|s| s.value.name == CLUSTER_CA_SECRET) else {
        return Ok(false);
    };
    let opened;
    let bytes = if secret.value.key_id == ostro_meta::TOKEN_KEY_PLAINTEXT_ID {
        secret.value.ciphertext.as_slice()
    } else {
        let open = open.ok_or_else(|| {
            RestoreError::Meta(format!(
                "{CLUSTER_CA_SECRET} is sealed ({}); set OSTRO_SEALING_KEY to the cluster's \
                 sealing key to restore it",
                secret.value.key_id
            ))
        })?;
        opened = (open.0)(&secret.value)
            .map_err(|e| RestoreError::Meta(format!("{CLUSTER_CA_SECRET}: {e}")))?;
        opened.as_slice()
    };
    let text = std::str::from_utf8(bytes)
        .map_err(|_| RestoreError::Meta(format!("{CLUSTER_CA_SECRET} is not PEM")))?;
    let end = "-----END CERTIFICATE-----";
    let at = text
        .find(end)
        .map(|i| i + end.len())
        .ok_or_else(|| RestoreError::Meta(format!("{CLUSTER_CA_SECRET} holds no certificate")))?;
    let (cert, key) = text.split_at(at);
    let key = key.trim();
    if !key.contains("PRIVATE KEY") {
        return Err(RestoreError::Meta(format!(
            "{CLUSTER_CA_SECRET} holds no key"
        )));
    }
    let tls = data_dir.join("tls");
    fs::create_dir_all(&tls)?;
    {
        // The key is created 0600 (never readable by others, not even briefly).
        use std::io::Write as _;
        let mut o = fs::OpenOptions::new();
        o.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut o, 0o600);
        let mut f = o.open(tls.join("ca.key"))?;
        f.write_all(format!("{key}\n").as_bytes())?;
        f.sync_all()?;
    }
    write_durably(&tls, "ca.crt", format!("{}\n", cert.trim()).as_bytes())?;
    Ok(true)
}

async fn shutdown_node(node: ostro_partition::Node, what: &str) -> Result<(), RestoreError> {
    node.shutdown().await.map_err(|e| RestoreError::Replay {
        partition: 0,
        reason: format!("shutdown after {what}: {e}"),
    })
}

/// Restore a multi-member cluster bucket into `opts.data_dir` as a seed (module docs).
/// Called by [`crate::restore::restore`] after the empty-dir check, with `--at` refused.
pub async fn restore_cluster(
    store: Arc<dyn ObjectStore>,
    opts: RestoreOptions,
    described: String,
    now_ms: u64,
) -> Result<RestoreReport, RestoreError> {
    let bucket = detect(&store).await?.ok_or_else(|| {
        RestoreError::BadManifest(
            "not a cluster bucket (no meta/head and no partition heads)".into(),
        )
    })?;
    restore_detected(store, opts, described, now_ms, bucket).await
}

pub(crate) async fn restore_detected(
    store: Arc<dyn ObjectStore>,
    opts: RestoreOptions,
    described: String,
    now_ms: u64,
    bucket: ClusterBucket,
) -> Result<RestoreReport, RestoreError> {
    let t0 = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let data_dir = opts.data_dir.clone();
    let candidates = meta_candidates(&store, bucket.meta_head.as_ref()).await?;
    if candidates.is_empty() {
        return Err(RestoreError::ClusterBucketUnsupported(format!(
            "no metadata snapshot in the bucket; the metadata leader uploads one in 1.0+ ({} \
             per-partition head object(s), e.g. {})",
            bucket.heads.len(),
            bucket.heads.first().map_or("-", String::as_str)
        )));
    }
    // Partition count: meta/head, else the newest candidate's ClusterInfo.
    let p_count = match &bucket.meta_head {
        Some(h) if h.partitions > 0 => h.partitions,
        _ => load_meta(&store, &candidates[0])
            .await?
            .cluster()
            .map(|c| c.value.partitions)
            .ok_or_else(|| RestoreError::Meta("the metadata snapshot has no ClusterInfo".into()))?,
    };
    let count = PartitionCount::new(p_count)
        .ok_or_else(|| RestoreError::BadManifest("0 partitions".into()))?;
    let parallelism = opts.parallelism.max(1);

    // 1. Partitions: heads, chains, starts.
    let sem = Arc::new(Semaphore::new(parallelism));
    let mut set = JoinSet::new();
    for p in 0..p_count {
        let (store, sem) = (Arc::clone(&store), Arc::clone(&sem));
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            read_part(store, p).await
        });
    }
    let mut parts: BTreeMap<u16, Part> = BTreeMap::new();
    let mut failed = None;
    while let Some(r) = set.join_next().await {
        match r
            .map_err(|e| RestoreError::Io(e.to_string()))
            .and_then(|x| x)
        {
            Ok(part) => {
                parts.insert(part.p, part);
            }
            Err(e) => {
                failed.get_or_insert(e);
            }
        }
    }
    if let Some(e) = failed {
        return Err(e);
    }
    let manifest_ms = ms(t0.elapsed());
    let seed = opts
        .node_id
        .or_else(|| parts.get(&0).map(|p| p.head.node_id).filter(|n| *n != 0))
        .ok_or_else(|| RestoreError::BadManifest("no seed node id: pass --node-id".into()))?;

    // Scan: [floor, end) of every partition, observed.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    fs::create_dir_all(&data_dir)?;
    let scan = data_dir.join(SCAN_DIR);
    fs::create_dir_all(&scan)?;
    let inc = new_incarnation();
    write_node_toml(&scan, p_count, &inc, &described, None)?;
    for part in parts.values() {
        materialise(&store, &scan, count, part, &part.starts[0], part.head.end).await?;
    }
    let rec = CursorRecorders::new();
    let mut cfg = NodeConfig::new(scan.clone(), count);
    cfg.node_id = seed;
    cfg.mode = StartMode::Inspect;
    cfg.replay_observer = Some(rec.factory());
    let node = open_inspect_cfg(cfg).await?;
    let mut steps: BTreeMap<u16, CursorSteps> = BTreeMap::new();
    let mut scan_err = None;
    for part in parts.values() {
        let applied = node.applied_state(PartitionId(part.p)).map(|(o, _)| o.0);
        if applied != Some(part.head.end) {
            scan_err = Some(RestoreError::Replay {
                partition: part.p,
                reason: format!(
                    "scan replayed to {applied:?}, the head ends at {}",
                    part.head.end
                ),
            });
            break;
        }
        let samples = rec.samples(PartitionId(part.p));
        let s = if samples.is_empty() {
            CursorSteps {
                floor: part.floor,
                end: part.head.end,
                ..CursorSteps::default()
            }
        } else {
            cluster_cut::steps_from_replay(&samples, part.floor, part.head.end)
        };
        steps.insert(part.p, s);
    }
    shutdown_node(node, "the scan").await?;
    fs::remove_dir_all(&scan)?;
    if let Some(e) = scan_err {
        return Err(e);
    }

    // 2. Cut.
    let sampled: BTreeMap<u16, CursorSteps> = steps
        .iter()
        .map(|(p, s)| (*p, at_boundaries(s, &parts[p].boundaries)))
        .collect();
    let cut = if skip_fixpoint() {
        Cut {
            offsets: parts.iter().map(|(p, x)| (*p, x.head.end)).collect(),
            lowered: Vec::new(),
        }
    } else {
        let mut c = cluster_cut::search(&sampled).map_err(|e| match e {
            CutError::NoConsistentCut { partitions } => {
                RestoreError::NoConsistentCut { partitions }
            }
        })?;
        for (p, o) in c.offsets.iter_mut() {
            *o = round_down(&parts[p].boundaries, *o);
        }
        c
    };
    let partitions_ms = ms(t.elapsed());

    // 3. Restore at the cut into the data dir.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    write_node_toml(&data_dir, p_count, &inc, &described, None)?;
    let mut chosen: BTreeMap<u16, (Start, u32)> = BTreeMap::new();
    for part in parts.values() {
        let c = cut.offsets[&part.p];
        let start = part
            .starts
            .iter()
            .rev()
            .find(|s| s.offset <= c)
            .cloned()
            .ok_or_else(|| RestoreError::NoConsistentCut {
                partitions: vec![part.p],
            })?;
        let latest = materialise(&store, &data_dir, count, part, &start, c).await?;
        chosen.insert(part.p, (start, latest));
    }
    #[cfg(feature = "failpoints")]
    fail::fail_point!("restore::mid_partition", |_| Err(RestoreError::Injected(
        "restore::mid_partition"
    )));
    let node = open_inspect(&data_dir, count, seed).await?;
    let mut finals: BTreeMap<u16, Final> = BTreeMap::new();
    let mut pins: Vec<Pin> = Vec::new();
    let mut failure = None;
    for part in parts.values() {
        let c = cut.offsets[&part.p];
        match read_final(&node, part.p).await {
            Ok(f) if f.applied == c => {
                finals.insert(part.p, f);
            }
            Ok(f) => {
                failure = Some(RestoreError::Replay {
                    partition: part.p,
                    reason: format!("replayed to {}, the cut is {c}", f.applied),
                });
                break;
            }
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
        match open_runs(&node, part.p) {
            Ok(mut v) => pins.append(&mut v),
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    shutdown_node(node, "the final replay").await?;
    if let Some(e) = failure {
        return Err(e);
    }
    // Two independent replays (scan from the floor, final from S) agree at the cut.
    for (p, f) in &finals {
        let (s, c) = (&steps[p], cut.offsets[p]);
        let same = f.next.iter().all(|(q, v)| s.next_at(*q, c) == *v)
            && f.truncated.iter().all(|(q, v)| s.truncated_at(*q, c) == *v)
            && f.seen.iter().all(|(q, v)| s.seen_at(*q, c) == *v);
        if !same {
            return Err(RestoreError::Replay {
                partition: *p,
                reason: format!(
                    "the cursors at the cut {c} differ between the scan and the final replay"
                ),
            });
        }
    }
    check_consistency(&finals)?;
    let replay_ms = ms(t.elapsed());

    // 4. Metadata: the newest covering candidate.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let blob_keys: BTreeSet<String> = store
        .list("blobs/", None)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .into_iter()
        .filter_map(|m| m.key.strip_prefix("blobs/").map(str::to_owned))
        .collect();
    pins.sort();
    let mut picked = None;
    let mut first_gap: Option<(usize, String)> = None;
    for c in &candidates {
        let state = load_meta(&store, c).await?;
        let gaps = uncovered(&state, &pins, &blob_keys);
        if gaps.is_empty() {
            picked = Some((c, state));
            break;
        }
        if first_gap.is_none() {
            let g = gaps[0];
            first_gap = Some((
                gaps.len(),
                format!(
                    "run {} on {} v{} (module {})",
                    g.run,
                    g.workflow,
                    g.version,
                    g.module
                        .map_or_else(|| "from the workflow version".into(), |m| hex(&m))
                ),
            ));
        }
        tracing::warn!(key = %c.key, uncovered = gaps.len(), "metadata snapshot does not cover the open runs; trying an older one");
    }
    let Some((cand, mut state)) = picked else {
        let (n, first) = first_gap.unwrap_or_default();
        return Err(RestoreError::Meta(format!(
            "no metadata snapshot covers the module pins of {n} non-terminal runs (first: {first})"
        )));
    };
    let cluster_id = state
        .cluster()
        .map(|c| c.value.cluster_id.clone())
        .ok_or_else(|| RestoreError::Meta(format!("{}: no ClusterInfo", cand.key)))?;
    if let Some(c) = state.cluster()
        && c.value.partitions != p_count
    {
        return Err(RestoreError::Meta(format!(
            "{} names {} partitions, the bucket has {p_count}",
            cand.key, c.value.partitions
        )));
    }
    let mut epochs = BTreeMap::new();
    for part in parts.values() {
        let meta_epoch = state.partition(part.p).map_or(0, |a| a.value.epoch);
        let snap_epoch = chosen.get(&part.p).map_or(0, |(_, e)| *e);
        let e = meta_epoch
            .max(part.head.epoch)
            .max(part.max_epoch)
            .max(snap_epoch);
        epochs.insert(part.p, e);
    }
    let removed = rewrite_meta(&mut state, cand.index, seed, &epochs, now_ms)?;
    let ca_kept = write_stored_ca(&state, &data_dir, opts.open_secret.as_ref())?;
    let bytes = ostro_meta::encode_snapshot(&state);
    let h = MetaHandle::bootstrap_from_snapshot(&data_dir.join("meta"), seed, cand.index, &bytes)
        .await
        .map_err(|e| RestoreError::Meta(e.to_string()))?;
    h.stop().await;
    let meta_ms = ms(t.elapsed());
    #[cfg(feature = "failpoints")]
    fail::fail_point!("restore::after_meta", |_| Err(RestoreError::Injected(
        "restore::after_meta"
    )));

    // 5. Blobs.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let blobs = restore_blobs(&store, &data_dir, parallelism).await?;
    let blobs_ms = ms(t.elapsed());

    // 6. node.toml with the kept cluster id.
    write_node_toml(&data_dir, p_count, &inc, &described, Some(&cluster_id))?;
    fs::File::open(&data_dir)?.sync_all()?;

    let mut reports = Vec::new();
    let mut cparts = Vec::new();
    for part in parts.values() {
        let c = cut.offsets[&part.p];
        let (start, _) = &chosen[&part.p];
        let f = &finals[&part.p];
        let envelopes: u64 = batches(part, start.offset, c)?
            .iter()
            .map(|b| b.envelopes.len() as u64)
            .sum();
        reports.push(PartitionReport {
            p: part.p,
            snapshot_offset: start.snapshot.as_ref().map(|_| start.offset),
            chain_start: part.head.chain_start,
            verify_offset: start.offset,
            verify_state_hash: start
                .snapshot
                .as_ref()
                .and_then(|(_, _, h)| h.map(|x| hex(&x)))
                .unwrap_or_default(),
            cut_offset: c,
            digest: hex(&f.digest),
            chunks: part
                .chunks
                .iter()
                .filter(|k| k.header.end > start.offset && k.header.base < c)
                .count(),
            envelopes,
        });
        let lowered_reason = cut
            .lowered
            .iter()
            .rev()
            .find(|(q, ..)| *q == part.p)
            .map(|(_, _, _, l)| lowering_text(*l))
            .filter(|_| c < part.head.end);
        cparts.push(ClusterPartitionReport {
            p: part.p,
            head_end: part.head.end,
            head_epoch: part.head.epoch,
            floor: part.floor,
            cut: c,
            dropped: part.head.end - c,
            lowered_reason,
            epoch: epochs[&part.p],
        });
    }
    Ok(RestoreReport {
        data_dir: data_dir.display().to_string(),
        source: described,
        generation: 0,
        cluster_id,
        node_id: seed,
        source_incarnation: String::new(),
        incarnation: hex16(&inc),
        cut_at_ms: bucket.meta_head.as_ref().map_or(0, |h| h.at_ms),
        meta_index: Some(cand.index),
        blobs,
        takeover: opts.takeover,
        partitions: reports,
        durations: RestoreDurations {
            manifest_ms,
            meta_ms,
            blobs_ms,
            partitions_ms,
            replay_ms,
            total_ms: ms(t0.elapsed()),
        },
        cluster: Some(ClusterRestoreReport {
            meta_index: cand.index,
            meta_term: cand.head.map(|(_, t)| t),
            meta_from_head: cand.head.is_some(),
            seed,
            removed_members: removed,
            open_runs: pins.len() as u64,
            ca_kept,
            partitions: cparts,
        }),
    })
}

async fn open_inspect_cfg(cfg: NodeConfig) -> Result<ostro_partition::Node, RestoreError> {
    let node = ostro_partition::Node::open(cfg)
        .await
        .map_err(|e| RestoreError::Replay {
            partition: 0,
            reason: format!("node open: {e}"),
        })?;
    for s in node.slots() {
        if let ostro_partition::PartitionSlot::Failed { partition, reason } = s {
            let (partition, reason) = (partition.0, reason.clone());
            let _ = node.shutdown().await;
            return Err(RestoreError::Replay { partition, reason });
        }
    }
    Ok(node)
}

/// Where a scan directory would live (tests).
pub fn scan_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(SCAN_DIR)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ostro_meta::{MetaCommand, MetaKey, MetaRecord, SealedSecret};

    use super::*;
    use crate::restore::SecretOpener;

    const PEM: &str = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n\
                       -----BEGIN PRIVATE KEY-----\nBBBB\n-----END PRIVATE KEY-----\n";

    fn state_with(ciphertext: &[u8], key_id: &str) -> MetaState {
        let mut s = MetaState::default();
        s.apply(
            1,
            MetaCommand::Put {
                key: MetaKey::Secret(CLUSTER_CA_SECRET.to_owned()),
                expected_version: None,
                record: MetaRecord::Secret(SealedSecret {
                    name: CLUSTER_CA_SECRET.to_owned(),
                    ciphertext: ciphertext.to_vec(),
                    key_id: key_id.to_owned(),
                    ext: Vec::new(),
                }),
            },
        );
        s
    }

    /// p14-fixups-2: a sealed CA needs the opener; a plaintext-v0 one is written as before.
    #[test]
    fn stored_ca_plaintext_and_sealed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let plain = state_with(PEM.as_bytes(), ostro_meta::TOKEN_KEY_PLAINTEXT_ID);
        assert!(write_stored_ca(&plain, &dir.path().join("a"), None).expect("plaintext"));
        let key = fs::read_to_string(dir.path().join("a/tls/ca.key")).expect("key");
        assert!(key.contains("BBBB"), "{key}");

        let sealed = state_with(b"\x01opaque", "aes256gcm:0011aabb");
        let e = write_stored_ca(&sealed, &dir.path().join("b"), None).expect_err("no opener");
        let msg = e.to_string();
        assert!(
            msg.contains("sealed") && msg.contains("OSTRO_SEALING_KEY"),
            "{msg}"
        );
        assert!(!dir.path().join("b/tls/ca.key").exists());

        let open = SecretOpener(Arc::new(|s: &SealedSecret| {
            assert_eq!(s.ciphertext, b"\x01opaque");
            Ok(PEM.as_bytes().to_vec())
        }));
        assert!(write_stored_ca(&sealed, &dir.path().join("c"), Some(&open)).expect("opened"));
        let crt = fs::read_to_string(dir.path().join("c/tls/ca.crt")).expect("crt");
        assert!(crt.contains("AAAA"), "{crt}");

        let bad = SecretOpener(Arc::new(|_: &SealedSecret| Err("wrong key".to_owned())));
        let e = write_stored_ca(&sealed, &dir.path().join("d"), Some(&bad)).expect_err("bad");
        assert!(e.to_string().contains("wrong key"), "{e}");
    }
}
