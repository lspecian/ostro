//! `ostro restore` (P8-D6, research Pattern 6, plan 08-18): rebuild a node's data directory
//! from a bottomless prefix (`s3://…`, `file://…`) or a backup file (tar), verified end to
//! end.
//!
//! 1. Refuse a non-empty data directory (a crashed restore leaves one: delete it and retry).
//! 2. Load the manifest: `manifest/current`, or with `--at` a generation (`manifest/gen/<g>`)
//!    or the newest one cut at or before a time. The object's sha256 trailer, format and
//!    [`Manifest::validate`] (`snapshot_offset <= verify_offset <= cut_offset`, chain start
//!    rule) are checked before anything is written ([`RestoreError::BadManifest`]).
//! 3. `node.toml`: `format_version = 1`, `partitions = P`, a **new** incarnation, and the
//!    additive `restored_from` key.
//! 4. Meta: `MetaHandle::bootstrap_from_snapshot` from `meta_snapshot_key` (sha verified):
//!    the native raft install, identical `MetaState`.
//! 5. Blobs: every remote `blobs/<sha>` (sha verified) into the local blob store.
//! 6. Per partition, in parallel: download the snapshot (sha verified) and
//!    `install_snapshot` it; walk the chunk chain **backwards from the manifest head**
//!    ([`chunk::walk_back`], never by listing `seg/`, so forked chunks are ignored) down to
//!    the snapshot offset (or `chain_start`), then [`chunk::verify_chain`] forward.
//! 7. Two-step verified replay, in files: (a) import the envelopes `[S, verify_offset)` into
//!    a fresh WAL ([`write_partition_log`], `Create`); (b) open the node in Inspect mode (it
//!    replays exactly to `verify_offset`) and compare `state_hash` with the manifest
//!    ([`RestoreError::HashMismatch`] names the partition); (c) append `[verify_offset, cut)`;
//!    (d) Inspect again: the state at the cut.
//! 8. Cross-partition consistency (Pitfall 1): for every pair A != B,
//!    `outbox_truncated[A->B] <= inbound_seen_B[A] < outbox_next_seq[A->B]`.
//! 9. [`RestoreReport`]: cut time, per-partition offsets, hashes and digests, durations.
//!
//! Restore never runs during a normal server start: a node without a usable state says
//! "snapshot required: run 'ostro restore'" (08-09) and stops.
//!
//! **Takeover.** The restored node has a new incarnation, so the source prefix's `LOCK`
//! names someone else: the restored server uploads to the same prefix only when started with
//! `[backup] takeover = true` (`--takeover`, P8-D4 fence); otherwise configure a new prefix.
//!
//! Failpoints (feature `failpoints`): `restore::after_meta`, `restore::mid_partition`.

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_blob::BlobStore;
use ostro_log::{ImportMode, RawBatch, write_partition_log};
use ostro_meta::MetaHandle;
use ostro_objstore::{LocalFs, ObjectStore, StoreOptions};
use ostro_partition::node::{hex16, new_incarnation};
use ostro_partition::store::snapshot::install_snapshot;
use ostro_partition::{Node, NodeConfig, PartitionSlot, StartMode, StoreConfig};
use ostro_types::{Offset, PartitionCount, PartitionId, envelope};
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::archive::{Staging, unpack};
use crate::chunk::{self, ChainStart, Chunk};
use crate::manifest::{self, Manifest, ManifestError, PartitionCut};

/// Where to restore from.
#[derive(Clone)]
pub enum RestoreSource {
    /// `s3://bucket/prefix` or `file:///abs/path`, opened with these transport options.
    Url {
        /// The URL.
        url: String,
        /// Transport options (credentials, endpoint, …).
        options: StoreOptions,
    },
    /// A backup file (tar of the LocalFs layout).
    Tar(PathBuf),
    /// An already open store (tests, embedding).
    Store(Arc<dyn ObjectStore>),
}

impl std::fmt::Debug for RestoreSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RestoreSource::Url { url, .. } => f.debug_tuple("Url").field(url).finish(),
            RestoreSource::Tar(p) => f.debug_tuple("Tar").field(p).finish(),
            RestoreSource::Store(_) => f.write_str("Store(..)"),
        }
    }
}

impl RestoreSource {
    /// How the source is recorded in `node.toml` `restored_from` (no credentials).
    fn describe(&self) -> String {
        match self {
            RestoreSource::Url { url, .. } => url.clone(),
            RestoreSource::Tar(p) => p.display().to_string(),
            RestoreSource::Store(_) => "store".to_owned(),
        }
    }
}

/// Which restore point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At {
    /// A manifest generation.
    Generation(u64),
    /// The newest manifest cut at or before this time (ms since the epoch).
    TimeMs(u64),
}

/// Restore settings.
#[derive(Clone, Debug)]
pub struct RestoreOptions {
    /// The data directory to create (must be absent or empty).
    pub data_dir: PathBuf,
    /// Restore point (`None` = `manifest/current`).
    pub at: Option<At>,
    /// The operator intends the restored node to take over the source prefix (reported;
    /// the server needs `[backup] takeover = true` to act on it).
    pub takeover: bool,
    /// Partitions restored concurrently (at least 1).
    pub parallelism: usize,
    /// The metadata voter id (`[node] node_id` of the server that will run on the data
    /// directory); `None` = the manifest's node id.
    pub node_id: Option<u64>,
}

impl RestoreOptions {
    /// Defaults: current manifest, no takeover, parallelism 4, the manifest's node id.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            at: None,
            takeover: false,
            parallelism: 4,
            node_id: None,
        }
    }
}

/// Restore failures. Every partition-specific failure names the partition.
#[derive(Debug, thiserror::Error)]
pub enum RestoreError {
    /// The data directory is not empty.
    #[error(
        "data directory {0} is not empty: restore only writes into an empty directory (if an \
         earlier restore was interrupted, delete the directory and retry)"
    )]
    NotEmpty(PathBuf),
    /// The source could not be opened or read.
    #[error("restore source: {0}")]
    Source(String),
    /// No manifest matches.
    #[error("no restore point: {0}")]
    NoManifest(String),
    /// The manifest is malformed or violates the ordering rules.
    #[error("bad manifest: {0}")]
    BadManifest(String),
    /// Metadata bootstrap failed.
    #[error("metadata: {0}")]
    Meta(String),
    /// A blob failed.
    #[error("blob {key}: {reason}")]
    Blob {
        /// Object key.
        key: String,
        /// Why.
        reason: String,
    },
    /// A snapshot object does not match its manifest sha256.
    #[error("partition {partition}: snapshot {key} sha256 mismatch")]
    SnapshotSha {
        /// Partition.
        partition: u16,
        /// Object key.
        key: String,
    },
    /// A snapshot failed to install.
    #[error("partition {partition}: snapshot: {reason}")]
    Snapshot {
        /// Partition.
        partition: u16,
        /// Why.
        reason: String,
    },
    /// The chunk chain is broken (missing, tampered, gap).
    #[error("partition {partition}: chunk chain: {reason}")]
    Chain {
        /// Partition.
        partition: u16,
        /// Why.
        reason: String,
    },
    /// The WAL import failed.
    #[error("partition {partition}: log import: {reason}")]
    Import {
        /// Partition.
        partition: u16,
        /// Why.
        reason: String,
    },
    /// The replayed state hash differs from the manifest's.
    #[error(
        "partition {partition}: state hash at offset {offset} is {found}, the manifest says \
         {expected}"
    )]
    HashMismatch {
        /// Partition.
        partition: u16,
        /// The verify offset.
        offset: u64,
        /// Manifest value (hex).
        expected: String,
        /// Replayed value (hex).
        found: String,
    },
    /// The replay did not reach the expected offset, or a partition failed to open.
    #[error("partition {partition}: replay: {reason}")]
    Replay {
        /// Partition.
        partition: u16,
        /// Why.
        reason: String,
    },
    /// The cross-partition consistency check failed.
    #[error("cross-partition consistency: {0}")]
    Inconsistent(String),
    /// Filesystem failure.
    #[error("restore io: {0}")]
    Io(String),
    /// Injected by a failpoint.
    #[error("injected failure at {0}")]
    Injected(&'static str),
}

impl From<std::io::Error> for RestoreError {
    fn from(e: std::io::Error) -> Self {
        RestoreError::Io(e.to_string())
    }
}

/// One partition in the [`RestoreReport`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PartitionReport {
    /// Partition id.
    pub p: u16,
    /// The installed snapshot's offset (`None`: the chain starts at 0).
    pub snapshot_offset: Option<u64>,
    /// The manifest's chain start.
    pub chain_start: u64,
    /// Where the state hash was checked.
    pub verify_offset: u64,
    /// The checked state hash (hex).
    pub verify_state_hash: String,
    /// The restored log end = applied offset.
    pub cut_offset: u64,
    /// Logical digest of the restored state at the cut (hex).
    pub digest: String,
    /// Chunks downloaded.
    pub chunks: usize,
    /// Envelopes imported.
    pub envelopes: u64,
}

/// Phase durations in milliseconds.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct RestoreDurations {
    /// Source open + manifest load.
    pub manifest_ms: u64,
    /// Meta bootstrap.
    pub meta_ms: u64,
    /// Blob download.
    pub blobs_ms: u64,
    /// Snapshots + chains + first import.
    pub partitions_ms: u64,
    /// Both Inspect replays + tail import + checks.
    pub replay_ms: u64,
    /// Everything.
    pub total_ms: u64,
}

/// What a restore did (`ostro restore --json` prints it).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RestoreReport {
    /// The data directory written.
    pub data_dir: String,
    /// Source (URL or file; never credentials).
    pub source: String,
    /// Manifest generation restored.
    pub generation: u64,
    /// Cluster id of the source.
    pub cluster_id: String,
    /// Source node id (the meta voter id used).
    pub node_id: u64,
    /// The source's incarnation (hex).
    pub source_incarnation: String,
    /// The new incarnation written to `node.toml` (hex).
    pub incarnation: String,
    /// When the cut was taken (ms since the epoch).
    pub cut_at_ms: u64,
    /// The metadata raft index restored (`None`: no meta snapshot in the manifest).
    pub meta_index: Option<u64>,
    /// Blobs downloaded.
    pub blobs: usize,
    /// Takeover requested.
    pub takeover: bool,
    /// Per partition.
    pub partitions: Vec<PartitionReport>,
    /// Durations.
    pub durations: RestoreDurations,
}

pub(crate) fn hex(b: &[u8]) -> String {
    use std::fmt::Write as _;
    b.iter()
        .fold(String::with_capacity(b.len() * 2), |mut s, x| {
            let _ = write!(s, "{x:02x}");
            s
        })
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// Whether `dir` is absent or an empty directory.
fn is_empty_dir(dir: &Path) -> Result<bool, RestoreError> {
    match fs::read_dir(dir) {
        Ok(mut it) => Ok(it.next().is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}

/// Load the requested manifest (sha trailer, format and `validate` checked by
/// [`manifest::decode`]).
async fn load_manifest(
    store: &Arc<dyn ObjectStore>,
    at: Option<At>,
) -> Result<Manifest, RestoreError> {
    let src = |e: ostro_objstore::Error| RestoreError::Source(e.to_string());
    let decode = |key: &str, bytes: &[u8]| {
        manifest::decode(bytes).map_err(|e| match e {
            ManifestError::Invalid { .. } | ManifestError::Malformed(_) => {
                RestoreError::BadManifest(format!("{key}: {e}"))
            }
            ManifestError::Format(_) => RestoreError::BadManifest(format!("{key}: {e}")),
        })
    };
    let current = match store.get(manifest::CURRENT_KEY).await.map_err(src)? {
        Some((b, _)) => Some(decode(manifest::CURRENT_KEY, &b)?),
        None => None,
    };
    let Some(at) = at else {
        return current.ok_or_else(|| {
            RestoreError::NoManifest(format!("{} is missing", manifest::CURRENT_KEY))
        });
    };
    if let (At::Generation(g), Some(c)) = (at, &current)
        && c.generation == g
    {
        return Ok(c.clone());
    }
    if let At::Generation(g) = at {
        let key = manifest::gen_key(g);
        return match store.get(&key).await.map_err(src)? {
            Some((b, _)) => decode(&key, &b),
            None => Err(RestoreError::NoManifest(format!("{key} is missing"))),
        };
    }
    let At::TimeMs(t) = at else {
        return Err(RestoreError::NoManifest("unreachable".into()));
    };
    if let Some(c) = &current
        && c.cut_at_ms <= t
    {
        return Ok(c.clone());
    }
    let mut gens: Vec<u64> = store
        .list("manifest/gen/", None)
        .await
        .map_err(src)?
        .iter()
        .filter_map(|m| manifest::parse_gen_key(&m.key))
        .collect();
    gens.sort_unstable();
    for g in gens.into_iter().rev() {
        let key = manifest::gen_key(g);
        if let Some((b, _)) = store.get(&key).await.map_err(src)? {
            let m = decode(&key, &b)?;
            if m.cut_at_ms <= t {
                return Ok(m);
            }
        }
    }
    Err(RestoreError::NoManifest(format!(
        "no manifest was cut at or before {t} ms"
    )))
}

/// The partition count, checked against the manifest: every partition 0..P has an entry.
fn partition_count(m: &Manifest, meta_partitions: Option<u16>) -> Result<u16, RestoreError> {
    let from_cut = m.cut.iter().map(|c| c.p + 1).max().unwrap_or(0);
    let p = meta_partitions.unwrap_or(from_cut);
    if p == 0 {
        return Err(RestoreError::BadManifest("no partitions".into()));
    }
    for q in 0..p {
        if m.partition(q).is_none() {
            return Err(RestoreError::BadManifest(format!(
                "partition {q} has no entry in the cut (of {p} partitions)"
            )));
        }
    }
    if from_cut > p {
        return Err(RestoreError::BadManifest(format!(
            "the cut names partition {} but the cluster has {p}",
            from_cut - 1
        )));
    }
    Ok(p)
}

fn write_durably(dir: &Path, name: &str, body: &[u8]) -> Result<(), RestoreError> {
    let tmp = dir.join(format!("{name}.tmp"));
    let mut f = File::create(&tmp)?;
    f.write_all(body)?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, dir.join(name))?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

/// TOML basic-string escaping for the `restored_from` value.
fn toml_str(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Everything restore keeps per partition between the phases.
struct PartWork {
    cut: PartitionCut,
    /// Envelopes from the restore start S on, grouped per chunk.
    batches: Vec<RawBatch>,
    chunks: usize,
}

/// Steps 6 + 7a for one partition.
async fn prepare_partition(
    store: Arc<dyn ObjectStore>,
    data_dir: PathBuf,
    count: PartitionCount,
    e: PartitionCut,
) -> Result<PartWork, RestoreError> {
    let p = e.p;
    let pid = PartitionId(p);
    let chain_err = |reason: String| RestoreError::Chain {
        partition: p,
        reason,
    };
    // Snapshot.
    if let (Some(key), Some(sha), Some(off)) =
        (&e.snapshot_key, e.snapshot_sha256, e.snapshot_offset)
    {
        let (bytes, _) = store
            .get(key)
            .await
            .map_err(|err| RestoreError::Source(err.to_string()))?
            .ok_or_else(|| RestoreError::Snapshot {
                partition: p,
                reason: format!("{key} is missing"),
            })?;
        if chunk::chunk_sha(&bytes) != sha {
            return Err(RestoreError::SnapshotSha {
                partition: p,
                key: key.clone(),
            });
        }
        let d = data_dir.clone();
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
        .map_err(|err| RestoreError::Io(err.to_string()))?
        .map_err(|err| RestoreError::Snapshot {
            partition: p,
            reason: err.to_string(),
        })?;
        if info.applied_offset != off {
            return Err(RestoreError::Snapshot {
                partition: p,
                reason: format!(
                    "snapshot applied_offset {} != manifest snapshot_offset {off}",
                    info.applied_offset
                ),
            });
        }
    }
    let start = e.snapshot_offset.unwrap_or(0);

    // Chain, walked back from the manifest head (forks ignored), then verified forward.
    let mut chunks: Vec<Chunk> = Vec::new();
    if let (Some(head), Some(sha)) = (&e.last_chunk_key, e.last_chunk_sha256) {
        let stop = e.snapshot_offset.unwrap_or(e.chain_start);
        let refs = chunk::walk_back(&store, head, sha, stop)
            .await
            .map_err(|err| chain_err(err.to_string()))?;
        chunks = refs.into_iter().map(|r| r.chunk).collect();
        let first = chunks
            .first()
            .ok_or_else(|| chain_err("empty chain".into()))?;
        let cs = ChainStart {
            base: first.header.base,
            prev_sha: first.header.prev_chunk_sha256,
        };
        chunk::verify_chain(cs, &chunks).map_err(|err| chain_err(err.to_string()))?;
        if first.header.base > start {
            return Err(chain_err(format!(
                "the chain starts at {} above the restore start {start}",
                first.header.base
            )));
        }
        let end = chunks.last().map_or(0, |c| c.header.end);
        if end != e.cut_offset {
            return Err(chain_err(format!(
                "the chain ends at {end}, the cut is {}",
                e.cut_offset
            )));
        }
    } else if e.cut_offset != start {
        return Err(chain_err(format!(
            "no chunks but the cut {} is above the restore start {start}",
            e.cut_offset
        )));
    }

    // Envelopes [S, cut), batch boundaries checked.
    let mut batches = Vec::with_capacity(chunks.len());
    let mut boundary_at_verify = e.verify_offset == start;
    for c in &chunks {
        let mut envs = Vec::new();
        let mut base = None;
        for env in &c.envelopes {
            let h = envelope::peek_header(env).map_err(|err| chain_err(err.to_string()))?;
            let end = h.base.0 + u64::from(h.cmd_count);
            if end <= start {
                continue;
            }
            if h.base.0 < start {
                return Err(chain_err(format!(
                    "the restore start {start} falls inside the batch [{}, {end})",
                    h.base
                )));
            }
            if end == e.verify_offset {
                boundary_at_verify = true;
            }
            base.get_or_insert(h.base);
            envs.push(env.clone().into());
        }
        if let Some(b) = base {
            batches.push(RawBatch {
                base: b,
                end: Offset(c.header.end),
                epoch: c.header.epoch,
                envelopes: envs,
            });
        }
    }
    if !boundary_at_verify {
        return Err(chain_err(format!(
            "verify_offset {} is not a batch boundary",
            e.verify_offset
        )));
    }

    // 7a. The log up to verify_offset.
    let wal = data_dir.join("wal");
    let first = batches.clone();
    let verify = Offset(e.verify_offset);
    tokio::task::spawn_blocking(move || {
        write_partition_log(&wal, pid, first, verify, ImportMode::Create)
    })
    .await
    .map_err(|err| RestoreError::Io(err.to_string()))?
    .map_err(|err| RestoreError::Import {
        partition: p,
        reason: err.to_string(),
    })?;
    Ok(PartWork {
        cut: e,
        batches,
        chunks: chunks.len(),
    })
}

/// Open the data directory in Inspect mode (replay to the log end, no append).
async fn open_inspect(
    data_dir: &Path,
    count: PartitionCount,
    node_id: u64,
) -> Result<Node, RestoreError> {
    let mut cfg = NodeConfig::new(data_dir.to_path_buf(), count);
    cfg.node_id = node_id;
    cfg.mode = StartMode::Inspect;
    let node = Node::open(cfg).await.map_err(|e| RestoreError::Replay {
        partition: 0,
        reason: format!("node open: {e}"),
    })?;
    for s in node.slots() {
        if let PartitionSlot::Failed { partition, reason } = s {
            let (partition, reason) = (partition.0, reason.clone());
            let _ = node.shutdown().await;
            return Err(RestoreError::Replay { partition, reason });
        }
    }
    Ok(node)
}

/// Per partition after the final replay.
struct Final {
    applied: u64,
    digest: [u8; 32],
    next: BTreeMap<u16, u64>,
    truncated: BTreeMap<u16, u64>,
    seen: BTreeMap<u16, u64>,
}

async fn read_final(node: &Node, p: u16) -> Result<Final, RestoreError> {
    let h = node
        .handle(PartitionId(p))
        .ok_or_else(|| RestoreError::Replay {
            partition: p,
            reason: "not running".into(),
        })?;
    let store = Arc::clone(h.store());
    tokio::task::spawn_blocking(move || {
        let meta = store.read_meta().map_err(|e| e.to_string())?;
        let digest = store.digest().map_err(|e| e.to_string())?;
        Ok::<_, String>(Final {
            applied: meta.applied.0,
            digest,
            next: meta.cursor.outbox_next_seq.clone(),
            truncated: meta.cursor.outbox_truncated.clone(),
            seen: meta.inbound_seen.clone(),
        })
    })
    .await
    .map_err(|e| RestoreError::Io(e.to_string()))?
    .map_err(|reason| RestoreError::Replay {
        partition: p,
        reason,
    })
}

/// Pitfall 1: `outbox_truncated[A->B] <= inbound_seen_B[A] < outbox_next_seq[A->B]`.
fn check_consistency(f: &BTreeMap<u16, Final>) -> Result<(), RestoreError> {
    for (&a, fa) in f {
        for (&b, fb) in f {
            if a == b {
                continue;
            }
            let next = fa.next.get(&b).copied().unwrap_or(1);
            let truncated = fa.truncated.get(&b).copied().unwrap_or(0);
            let seen = fb.seen.get(&a).copied().unwrap_or(0);
            if seen >= next {
                return Err(RestoreError::Inconsistent(format!(
                    "partition {b} saw seq {seen} from partition {a}, whose next seq is {next}"
                )));
            }
            if truncated > seen {
                return Err(RestoreError::Inconsistent(format!(
                    "partition {a} truncated its outbox to {b} up to {truncated}, but {b} only \
                     saw {seen}"
                )));
            }
        }
    }
    Ok(())
}

/// Restore `source` into `opts.data_dir` (see the module docs).
pub async fn restore(
    source: RestoreSource,
    opts: RestoreOptions,
) -> Result<RestoreReport, RestoreError> {
    let t0 = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let data_dir = opts.data_dir.clone();
    if !is_empty_dir(&data_dir)? {
        return Err(RestoreError::NotEmpty(data_dir));
    }
    let described = source.describe();
    // A tar is unpacked into a staging dir and restored through the LocalFs path.
    let mut _staging = None;
    let store: Arc<dyn ObjectStore> = match source {
        RestoreSource::Store(s) => s,
        RestoreSource::Url { url, options } => ostro_objstore::open_url(&url, &options)
            .map_err(|e| RestoreError::Source(format!("{url}: {e}")))?,
        RestoreSource::Tar(path) => {
            let staging = Staging::new("restore")?;
            let dir = staging.0.clone();
            let p = path.clone();
            let n = tokio::task::spawn_blocking(move || {
                let f = File::open(&p)?;
                unpack(std::io::BufReader::new(f), &dir)
            })
            .await
            .map_err(|e| RestoreError::Io(e.to_string()))?
            .map_err(|e| RestoreError::Source(format!("{}: {e}", path.display())))?;
            tracing::info!(files = n, file = %path.display(), "backup file unpacked");
            let s = LocalFs::new(&staging.0)
                .map_err(|e| RestoreError::Source(format!("staging store: {e}")))?;
            _staging = Some(staging);
            Arc::new(s)
        }
    };

    // 2. Manifest (validated before anything is written).
    let m = load_manifest(&store, opts.at).await?;
    let meta_bytes = match (&m.meta_snapshot_key, m.meta_snapshot_sha256) {
        (Some(k), Some(sha)) => {
            let (b, _) = store
                .get(k)
                .await
                .map_err(|e| RestoreError::Source(e.to_string()))?
                .ok_or_else(|| RestoreError::Meta(format!("{k} is missing")))?;
            if chunk::chunk_sha(&b) != sha {
                return Err(RestoreError::Meta(format!("{k}: sha256 mismatch")));
            }
            let index: u64 = k
                .strip_prefix("meta/")
                .and_then(|s| s.strip_suffix(".msnap"))
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| RestoreError::BadManifest(format!("bad meta snapshot key {k}")))?;
            let state = ostro_meta::decode_snapshot(&b)
                .map_err(|e| RestoreError::Meta(format!("{k}: {e}")))?;
            Some((index, b, state.cluster().map(|c| c.value.partitions)))
        }
        (None, None) => None,
        _ => {
            return Err(RestoreError::BadManifest(
                "meta snapshot fields partially set".into(),
            ));
        }
    };
    let p = partition_count(&m, meta_bytes.as_ref().and_then(|(_, _, p)| *p))?;
    let count =
        PartitionCount::new(p).ok_or_else(|| RestoreError::BadManifest("0 partitions".into()))?;
    let node_id = opts.node_id.unwrap_or(m.node_id);
    let manifest_ms = ms(t0.elapsed());

    // 3. node.toml with a new incarnation.
    fs::create_dir_all(&data_dir)?;
    if let Some(parent) = data_dir.parent().filter(|p| !p.as_os_str().is_empty()) {
        File::open(parent)?.sync_all()?;
    }
    let inc = new_incarnation();
    let node_toml = format!(
        "format_version = 1\npartitions = {p}\nincarnation = \"{}\"\nrestored_from = {}\n",
        hex16(&inc),
        toml_str(&described)
    );
    write_durably(&data_dir, "node.toml", node_toml.as_bytes())?;

    // 4. Meta.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let meta_index = match meta_bytes {
        Some((index, bytes, _)) => {
            let h =
                MetaHandle::bootstrap_from_snapshot(&data_dir.join("meta"), node_id, index, &bytes)
                    .await
                    .map_err(|e| RestoreError::Meta(e.to_string()))?;
            h.stop().await;
            Some(index)
        }
        None => {
            tracing::warn!(
                "the manifest has no meta snapshot: the restored node starts with empty metadata"
            );
            None
        }
    };
    let meta_ms = ms(t.elapsed());
    #[cfg(feature = "failpoints")]
    fail::fail_point!("restore::after_meta", |_| Err(RestoreError::Injected(
        "restore::after_meta"
    )));

    // 5. Blobs.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let parallelism = opts.parallelism.max(1);
    let blobs = restore_blobs(&store, &data_dir, parallelism).await?;
    let blobs_ms = ms(t.elapsed());

    // 6./7a. Partitions in parallel.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let sem = Arc::new(Semaphore::new(parallelism));
    let mut set = JoinSet::new();
    for q in 0..p {
        let e = m
            .partition(q)
            .cloned()
            .ok_or_else(|| RestoreError::BadManifest(format!("partition {q} has no entry")))?;
        let (store, dir, sem) = (Arc::clone(&store), data_dir.clone(), Arc::clone(&sem));
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            prepare_partition(store, dir, count, e).await
        });
    }
    let mut work: BTreeMap<u16, PartWork> = BTreeMap::new();
    while let Some(r) = set.join_next().await {
        let w = r.map_err(|e| RestoreError::Io(e.to_string()))??;
        work.insert(w.cut.p, w);
        #[cfg(feature = "failpoints")]
        if work.len() == 1 {
            fail::fail_point!("restore::mid_partition", |_| Err(RestoreError::Injected(
                "restore::mid_partition"
            )));
        }
    }
    let partitions_ms = ms(t.elapsed());

    // 7b. Inspect replay to verify_offset; compare the state hash.
    let t = Instant::now(); // clock-exempt: RestoreReport.durations (measurement only)
    let node = open_inspect(&data_dir, count, node_id).await?;
    let mut mismatch = None;
    for w in work.values() {
        let e = &w.cut;
        match node.applied_state(PartitionId(e.p)) {
            Some((off, hash)) if off.0 == e.verify_offset && hash == e.verify_state_hash => {}
            Some((off, hash)) if off.0 == e.verify_offset => {
                mismatch = Some(RestoreError::HashMismatch {
                    partition: e.p,
                    offset: e.verify_offset,
                    expected: hex(&e.verify_state_hash),
                    found: hex(&hash),
                });
                break;
            }
            other => {
                mismatch = Some(RestoreError::Replay {
                    partition: e.p,
                    reason: format!(
                        "replayed to {:?}, expected verify_offset {}",
                        other.map(|(o, _)| o.0),
                        e.verify_offset
                    ),
                });
                break;
            }
        }
    }
    node.shutdown().await.map_err(|e| RestoreError::Replay {
        partition: 0,
        reason: format!("shutdown after verify: {e}"),
    })?;
    if let Some(err) = mismatch {
        return Err(err);
    }

    // 7c. Append [verify_offset, cut).
    for w in work.values() {
        let e = &w.cut;
        if e.cut_offset == e.verify_offset {
            continue;
        }
        let pid = PartitionId(e.p);
        let wal = data_dir.join("wal");
        let tail: Vec<RawBatch> = w
            .batches
            .iter()
            .filter_map(|b| {
                let envs: Vec<_> = b
                    .envelopes
                    .iter()
                    .filter(|env| {
                        envelope::peek_header(env).is_ok_and(|h| h.base.0 >= e.verify_offset)
                    })
                    .cloned()
                    .collect();
                let base = envelope::peek_header(envs.first()?).ok()?.base;
                Some(RawBatch {
                    base,
                    end: b.end,
                    epoch: b.epoch,
                    envelopes: envs,
                })
            })
            .collect();
        let cut = Offset(e.cut_offset);
        tokio::task::spawn_blocking(move || {
            write_partition_log(&wal, pid, tail, cut, ImportMode::Append)
        })
        .await
        .map_err(|err| RestoreError::Io(err.to_string()))?
        .map_err(|err| RestoreError::Import {
            partition: e.p,
            reason: err.to_string(),
        })?;
    }

    // 7d. Inspect again: the state at the cut. 8. Consistency.
    let node = open_inspect(&data_dir, count, node_id).await?;
    let mut finals = BTreeMap::new();
    let mut failure = None;
    for w in work.values() {
        match read_final(&node, w.cut.p).await {
            Ok(f) if f.applied == w.cut.cut_offset => {
                finals.insert(w.cut.p, f);
            }
            Ok(f) => {
                failure = Some(RestoreError::Replay {
                    partition: w.cut.p,
                    reason: format!("replayed to {}, the cut is {}", f.applied, w.cut.cut_offset),
                });
                break;
            }
            Err(e) => {
                failure = Some(e);
                break;
            }
        }
    }
    node.shutdown().await.map_err(|e| RestoreError::Replay {
        partition: 0,
        reason: format!("shutdown after replay: {e}"),
    })?;
    if let Some(e) = failure {
        return Err(e);
    }
    check_consistency(&finals)?;
    File::open(&data_dir)?.sync_all()?;
    let replay_ms = ms(t.elapsed());

    let partitions = work
        .values()
        .map(|w| {
            let e = &w.cut;
            PartitionReport {
                p: e.p,
                snapshot_offset: e.snapshot_offset,
                chain_start: e.chain_start,
                verify_offset: e.verify_offset,
                verify_state_hash: hex(&e.verify_state_hash),
                cut_offset: e.cut_offset,
                digest: finals.get(&e.p).map(|f| hex(&f.digest)).unwrap_or_default(),
                chunks: w.chunks,
                envelopes: w.batches.iter().map(|b| b.envelopes.len() as u64).sum(),
            }
        })
        .collect();
    Ok(RestoreReport {
        data_dir: data_dir.display().to_string(),
        source: described,
        generation: m.generation,
        cluster_id: m.cluster_id.clone(),
        node_id,
        source_incarnation: hex(&m.incarnation),
        incarnation: hex16(&inc),
        cut_at_ms: m.cut_at_ms,
        meta_index,
        blobs,
        takeover: opts.takeover,
        partitions,
        durations: RestoreDurations {
            manifest_ms,
            meta_ms,
            blobs_ms,
            partitions_ms,
            replay_ms,
            total_ms: ms(t0.elapsed()),
        },
    })
}

/// Step 5: every remote blob, sha verified, into `<data_dir>/blobs`.
async fn restore_blobs(
    store: &Arc<dyn ObjectStore>,
    data_dir: &Path,
    parallelism: usize,
) -> Result<usize, RestoreError> {
    let keys: Vec<String> = store
        .list("blobs/", None)
        .await
        .map_err(|e| RestoreError::Source(e.to_string()))?
        .into_iter()
        .map(|m| m.key)
        .collect();
    if keys.is_empty() {
        return Ok(0);
    }
    let dir = data_dir.to_path_buf();
    let blobs = Arc::new(
        tokio::task::spawn_blocking(move || BlobStore::open(&dir))
            .await
            .map_err(|e| RestoreError::Io(e.to_string()))?
            .map_err(|e| RestoreError::Io(format!("blob store: {e}")))?,
    );
    let sem = Arc::new(Semaphore::new(parallelism));
    let mut set = JoinSet::new();
    for key in keys {
        let (store, blobs, sem) = (Arc::clone(store), Arc::clone(&blobs), Arc::clone(&sem));
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            let fail = |reason: String| RestoreError::Blob {
                key: key.clone(),
                reason,
            };
            let want = key
                .strip_prefix("blobs/")
                .filter(|h| h.len() == 64)
                .ok_or_else(|| fail("not a blob key".into()))?
                .to_owned();
            let (bytes, _) = store
                .get(&key)
                .await
                .map_err(|e| fail(e.to_string()))?
                .ok_or_else(|| fail("vanished".into()))?;
            let have = chunk::chunk_sha(&bytes);
            if hex(&have) != want {
                return Err(fail(format!("content sha256 is {}", hex(&have))));
            }
            tokio::task::spawn_blocking(move || blobs.put(&bytes))
                .await
                .map_err(|e| fail(e.to_string()))?
                .map_err(|e| fail(e.to_string()))?;
            Ok(())
        });
    }
    let mut n = 0;
    while let Some(r) = set.join_next().await {
        r.map_err(|e| RestoreError::Io(e.to_string()))??;
        n += 1;
    }
    Ok(n)
}
