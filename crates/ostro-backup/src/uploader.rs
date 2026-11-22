//! The node-level bottomless uploader (P8-D3, research Pattern 5, plan 08-13).
//!
//! [`Uploader::round`] does one tick against any [`ObjectStore`] (the server's bottomless
//! store, or the temp LocalFs store of an online backup, 08-18):
//!
//! 1. read every partition's atomic `(applied_offset, state_hash)` pair, **then** the
//!    consistent HW cut (`Node::log_cut`) and the cut time from the injected clock. Apply only
//!    advances below HW and the cut is read afterwards, so `applied_p <= cut_p` (W2);
//! 2. blobs: every blob put before the cut (put observer + a startup reconcile against
//!    `LIST blobs/`), with [`put_immutable`];
//! 3. per partition, the chain start rule (B4): on a fresh prefix, or when the uploaded end
//!    fell below `log_start`, the chain restarts at the newest uploaded snapshot `S` with
//!    `log_start <= S <= cut` (at 0 only when `log_start == 0` and no such snapshot exists);
//!    otherwise the partition is skipped (`failures_total{kind="no_snapshot"}`), a snapshot is
//!    requested, and no manifest is written this round;
//! 4. chunks `[uploaded_end, cut)`: OSEG v1, split at epoch boundaries, at most 8 MiB;
//! 5. snapshots (B3): every new local snapshot is uploaded as soon as it appears, but the
//!    manifest references only the newest uploaded snapshot with `offset <= cut`; later ones
//!    wait until a later cut covers them;
//! 6. the meta snapshot, if the meta applied index moved (captured after step 1);
//! 7. `verify_offset/state_hash` = the step-1 pair, or the referenced snapshot's own pair when
//!    the applied offset is below it;
//! 8. [`Manifest::validate`], [`check_before_manifest`], failpoint `upload::before_manifest`,
//!    `manifest/current` via `If-Match` (via `If-None-Match` on a fresh prefix), plus an
//!    immutable `manifest/gen/<gen>` on rounds that changed a referenced snapshot.
//!
//! A failed precondition stops the uploader for good and sets `ostro_backup_fenced = 1`.
//! After a durable manifest the upload floor of each partition is raised to
//! `min(referenced snapshot offset (or chain start), uploaded chunk end)` through
//! `Node::floor_handles`, and remote retention deletes snapshots beyond the newest
//! `remote_snapshots_keep`, chunks not needed by them or not on the chain (forks), and gen
//! manifests that reference deleted objects.
//!
//! Steps 3-5 run for all partitions concurrently (at most
//! [`UploaderConfig::upload_concurrency`] at a time, default `min(P, 8)`), each on its own
//! partition state, against the single cut read in step 1; the meta snapshot PUT overlaps
//! them, and blob PUTs (step 2, still before any chunk) are bounded the same way. The
//! manifest is built only after every partition finished, so it still names one consistent
//! cut. A partition failure fails the whole round (no manifest; the session is re-read).
//!
//! [`Uploader::run`] starts rounds on a fixed cadence of [`UploaderConfig::round_interval`]
//! (`seal_interval / 2`), measured from round start, not from the end of the previous
//! round; a round that overruns its slot is followed immediately by the next one. With
//! upload latency `U`, the data-loss window after a crash (newest lost ack − `cut_at_ms` of
//! the newest durable manifest) is at most `seal_interval / 2 + U`, which stays within
//! `seal_interval` (P8-D23, SC3/BACK-02) while `U <= seal_interval / 2`. Idle partitions
//! upload nothing, an idle round writes no manifest, and failures back off without ever
//! blocking apply.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ostro_blob::BlobStore;
use ostro_log::ReplicatedLog as _;
use ostro_meta::MetaView;
use ostro_objstore::{Conditional, ObjectStore};
use ostro_partition::{Clock, Node};
use ostro_types::{Offset, PartitionId};
use sha2::{Digest as _, Sha256};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tracing::{debug, error, info, warn};

use crate::chunk::{self, ChunkError, MAX_CHUNK_BYTES};
use crate::config::BackupConfig;
use crate::fence::{self, FenceError, Ident, Immutable, put_immutable};
use crate::manifest::{self, Manifest, ManifestError, PartitionCut};

/// `ostro_backup_cut_age_seconds`: now − `cut_at` of the last durable manifest.
pub const METRIC_CUT_AGE: &str = "ostro_backup_cut_age_seconds";
/// `ostro_backup_upload_seconds`: round duration (upload latency, W8).
pub const METRIC_UPLOAD_SECONDS: &str = "ostro_backup_upload_seconds";
/// `ostro_backup_upload_bytes_total{kind}`.
pub const METRIC_UPLOAD_BYTES: &str = "ostro_backup_upload_bytes_total";
/// `ostro_backup_failures_total{kind}`.
pub const METRIC_FAILURES: &str = "ostro_backup_failures_total";
/// `ostro_backup_fenced`: 1 once another writer fenced this uploader.
pub const METRIC_FENCED: &str = "ostro_backup_fenced";
/// `ostro_backup_upload_floor_lag{partition}`: applied − upload floor (W13).
pub const METRIC_FLOOR_LAG: &str = "ostro_backup_upload_floor_lag";

/// Longest back-off between failed rounds.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Shortest round period.
const MIN_ROUND_INTERVAL: Duration = Duration::from_millis(10);
/// Partitions uploaded concurrently within a round when
/// [`UploaderConfig::upload_concurrency`] is 0 (auto: `min(P, 8)`).
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 8;

/// Uploader failures.
#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    /// Another writer owns the prefix; the uploader stopped.
    #[error("fenced: {0}")]
    Fenced(String),
    /// Object store failure (retried next tick).
    #[error("object store: {0}")]
    Store(#[from] ostro_objstore::Error),
    /// Log read failure.
    #[error("log: {0}")]
    Log(String),
    /// Chunk encoding / chain problem.
    #[error("chunk: {0}")]
    Chunk(#[from] ChunkError),
    /// The manifest failed validation (a bug) or the remote one is unreadable.
    #[error("manifest: {0}")]
    Manifest(#[from] ManifestError),
    /// Local file failure (snapshot or blob).
    #[error("io: {0}")]
    Io(String),
    /// Fault injection.
    #[error("injected failure at {0}")]
    Injected(&'static str),
}

impl From<FenceError> for UploadError {
    fn from(e: FenceError) -> Self {
        match e {
            FenceError::Fenced(m) => Self::Fenced(m),
            FenceError::BadLock(m) => Self::Fenced(format!("unreadable LOCK: {m}")),
            FenceError::Store(e) => Self::Store(e),
        }
    }
}

impl UploadError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Fenced(_) => "fenced",
            Self::Store(_) => "store",
            Self::Log(_) => "log",
            Self::Chunk(_) => "chunk",
            Self::Manifest(_) => "manifest",
            Self::Io(_) => "io",
            Self::Injected(_) => "injected",
        }
    }
}

/// Uploader settings.
#[derive(Clone, Debug)]
pub struct UploaderConfig {
    /// RPO target (P8-D23): the bound on the end-to-end data-loss window. Rounds start
    /// every [`Self::round_interval`] = `seal_interval / 2`.
    pub seal_interval: Duration,
    /// Partitions (and blobs) uploaded concurrently within a round; 0 = auto (`min(P, 8)`).
    pub upload_concurrency: usize,
    /// Remote snapshots kept per partition.
    pub remote_snapshots_keep: usize,
    /// Cluster id (manifest + LOCK).
    pub cluster_id: String,
    /// Node id (manifest + LOCK).
    pub node_id: u64,
    /// Chunk size cap.
    pub max_chunk_bytes: usize,
    /// Raise the node's upload floors after durable manifests (off for one-off backups).
    pub publish_floor: bool,
    /// Run remote retention.
    pub retention: bool,
}

impl UploaderConfig {
    /// From `[backup]`.
    pub fn from_backup(cfg: &BackupConfig, cluster_id: impl Into<String>, node_id: u64) -> Self {
        Self {
            seal_interval: cfg.seal_interval,
            upload_concurrency: cfg.upload_concurrency,
            remote_snapshots_keep: cfg.remote_snapshots_keep.max(1),
            cluster_id: cluster_id.into(),
            node_id,
            max_chunk_bytes: MAX_CHUNK_BYTES,
            publish_floor: true,
            retention: true,
        }
    }

    /// Round cadence: `seal_interval / 2` (at least 10 ms), so that cadence + upload
    /// latency fits in `seal_interval` while uploads take at most half of it (P8-D23).
    pub fn round_interval(&self) -> Duration {
        (self.seal_interval / 2).max(MIN_ROUND_INTERVAL)
    }

    /// Concurrent partition uploads for `partitions` partitions (at least 1).
    pub fn concurrency(&self, partitions: u16) -> usize {
        let n = match self.upload_concurrency {
            0 => DEFAULT_UPLOAD_CONCURRENCY,
            n => n,
        };
        n.min(usize::from(partitions)).max(1)
    }
}

/// What one round did.
#[derive(Clone, Debug, Default)]
pub struct RoundReport {
    /// Every partition's barrier was not open: nothing was uploaded.
    pub barrier_closed: bool,
    /// The cut read in step 1.
    pub cut: Vec<Offset>,
    /// When the cut was read (injected clock).
    pub cut_at_ms: u64,
    /// When the manifest became durable (injected clock), if one was written.
    pub manifest_at_ms: Option<u64>,
    /// Round duration (upload latency, W8).
    pub upload: Duration,
    /// The manifest written this round.
    pub manifest: Option<Manifest>,
    /// Chunks / snapshots / blobs written (or found identical).
    pub chunks: usize,
    /// Chunk bytes.
    pub chunk_bytes: u64,
    /// Snapshot objects.
    pub snapshots: usize,
    /// Blob objects.
    pub blobs: usize,
    /// A meta snapshot was uploaded.
    pub meta_snapshot: bool,
    /// Partitions skipped by the chain start rule (no usable snapshot).
    pub skipped: Vec<u16>,
    /// Objects deleted by remote retention.
    pub deleted: usize,
}

/// A remote snapshot object.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SnapRef {
    offset: u64,
    key: String,
    sha: [u8; 32],
    /// Known for snapshots uploaded by this session (or referenced with verify == offset).
    state_hash: Option<[u8; 32]>,
}

/// One chunk of the in-memory chain index.
#[derive(Clone, Debug)]
struct ChunkMeta {
    key: String,
    end: u64,
}

#[derive(Clone, Debug, Default)]
struct PartState {
    /// A chain exists (remote or started this session).
    has_chain: bool,
    chain_start: u64,
    uploaded_end: u64,
    last_key: Option<String>,
    last_sha: Option<[u8; 32]>,
    /// Chunks from `chain_start` on, in order.
    chain: Vec<ChunkMeta>,
    /// Remote snapshots by offset (referenced, pending or older).
    snaps: BTreeMap<u64, SnapRef>,
    referenced: Option<u64>,
    /// Entry of the last durable manifest.
    last_entry: Option<PartitionCut>,
}

/// Gen manifest references, for retention.
#[derive(Clone, Debug, Default)]
struct GenRefs {
    objects: Vec<String>,
}

struct Session {
    store: Arc<dyn ObjectStore>,
    etag: Option<String>,
    generation: u64,
    prev_manifest_sha: [u8; 32],
    parts: Vec<PartState>,
    meta_index: Option<u64>,
    meta_key: Option<String>,
    meta_sha: Option<[u8; 32]>,
    remote_blobs: HashSet<[u8; 32]>,
    pending_blobs: BTreeSet<[u8; 32]>,
    blobs_reconciled: bool,
    gens: Option<BTreeMap<u64, Option<GenRefs>>>,
    /// Retention runs on the first manifest of a session and after snapshot changes.
    retention_due: bool,
    cut_at_ms: Option<u64>,
}

/// After-cut test hook: the uploader sends a resume sender and waits for it.
type Hook = mpsc::UnboundedSender<oneshot::Sender<()>>;

/// The node-level uploader.
pub struct Uploader {
    node: Arc<Node>,
    meta: Option<MetaView>,
    clock: Arc<dyn Clock>,
    cfg: UploaderConfig,
    ident: Ident,
    blobs: Option<Arc<BlobStore>>,
    blob_events: Mutex<Option<broadcast::Receiver<[u8; 32]>>>,
    session: tokio::sync::Mutex<Option<Session>>,
    fenced: AtomicBool,
    hook: Mutex<Option<Hook>>,
    /// Upload from a node opened in Inspect mode (offline backup, 08-18): the closed
    /// readiness barriers do not stop the round.
    inspect: bool,
    /// Phase 12 cluster mode (12-02 contract, consumed by 12-15); `None` = Phase 8
    /// behaviour.
    cluster: Option<UploaderClusterDeps>,
}

/// Phase 12 (D-P12-20): what the uploader needs in cluster mode. Passed by the server
/// (12-14) through [`Uploader::with_cluster`]; consumed by 12-15.
#[derive(Clone)]
pub struct UploaderClusterDeps {
    /// The partitions this node leads (only a partition's leader uploads its chunks).
    pub roles: Arc<dyn ostro_partition::roles::PartitionRoles>,
    /// Meta member count (`LOCK`/`manifest/current` only while it is 1, D-P12-3).
    pub members: watch::Receiver<usize>,
}

impl std::fmt::Debug for UploaderClusterDeps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploaderClusterDeps")
            .field("members", &*self.members.borrow())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for Uploader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Uploader")
            .field("cfg", &self.cfg)
            .field("fenced", &self.fenced.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
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

/// `p/<pppp>/snap/`.
pub fn snap_prefix(partition: u16) -> String {
    format!("p/{partition:04}/snap/")
}

/// `p/<pppp>/snap/<applied 20d>-<sha256>.osnap`.
pub fn snap_key(partition: u16, applied: u64, sha: &[u8; 32]) -> String {
    format!("{}{applied:020}-{}.osnap", snap_prefix(partition), hex(sha))
}

/// Parse a snapshot key into `(partition, applied, sha)`.
pub fn parse_snap_key(key: &str) -> Option<(u16, u64, [u8; 32])> {
    let rest = key.strip_prefix("p/")?;
    let (p, rest) = rest.split_once('/')?;
    let name = rest.strip_prefix("snap/")?.strip_suffix(".osnap")?;
    let (off, sha) = name.split_once('-')?;
    if p.len() != 4 || off.len() != 20 || !off.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((p.parse().ok()?, off.parse().ok()?, unhex32(sha)?))
}

/// `meta/<index 20d>.msnap`.
pub fn meta_key(index: u64) -> String {
    format!("meta/{index:020}.msnap")
}

/// `blobs/<sha256 hex>`.
pub fn blob_key(sha: &[u8; 32]) -> String {
    format!("blobs/{}", hex(sha))
}

fn ptr(s: &Arc<dyn ObjectStore>) -> *const () {
    Arc::as_ptr(s).cast::<()>()
}

impl Uploader {
    /// An uploader over `node`. `meta` (the metadata view) feeds the meta snapshot; `None`
    /// uploads none. Subscribes to blob puts immediately, so no put after this call is
    /// missed.
    pub fn new(
        node: Arc<Node>,
        meta: Option<MetaView>,
        clock: Arc<dyn Clock>,
        cfg: UploaderConfig,
    ) -> Self {
        let blobs = (0..node.log_cut().len()).find_map(|p| {
            node.handle(PartitionId(p as u16))
                .map(|h| Arc::clone(h.blobs()))
        });
        let blob_events = blobs.as_ref().map(|b| b.subscribe_puts());
        let ident = Ident::new(cfg.cluster_id.clone(), cfg.node_id, node.incarnation());
        Self {
            node,
            meta,
            clock,
            cfg,
            ident,
            blobs,
            blob_events: Mutex::new(blob_events),
            session: tokio::sync::Mutex::new(None),
            fenced: AtomicBool::new(false),
            hook: Mutex::new(None),
            inspect: false,
            cluster: None,
        }
    }

    /// Phase 12 cluster mode (D-P12-20): per-partition leader uploads. Without it the
    /// uploader keeps its Phase 8 behaviour.
    pub fn with_cluster(mut self, deps: UploaderClusterDeps) -> Self {
        self.cluster = Some(deps);
        self
    }

    /// The cluster-mode deps, when set.
    pub fn cluster(&self) -> Option<&UploaderClusterDeps> {
        self.cluster.as_ref()
    }

    /// Allow rounds over a node opened in Inspect mode (offline backup, 08-18): its
    /// barriers stay closed and nothing appends, so the cut is the recovered log end.
    pub fn for_inspect_node(mut self) -> Self {
        self.inspect = true;
        self
    }

    /// The LOCK identity this uploader writes under.
    pub fn ident(&self) -> &Ident {
        &self.ident
    }

    /// Settings.
    pub fn config(&self) -> &UploaderConfig {
        &self.cfg
    }

    /// Whether a precondition failure stopped this uploader.
    pub fn is_fenced(&self) -> bool {
        self.fenced.load(Ordering::Acquire)
    }

    /// Test hook (B3 test): after the cut is read, the round sends a resume sender on `tx`
    /// and waits until it fires (or is dropped).
    #[doc(hidden)]
    pub fn set_after_cut_hook(&self, tx: Option<mpsc::UnboundedSender<oneshot::Sender<()>>>) {
        *self.hook.lock().unwrap_or_else(PoisonError::into_inner) = tx;
    }

    fn set_fenced(&self, why: &str) {
        self.fenced.store(true, Ordering::Release);
        metrics::gauge!(METRIC_FENCED).set(1.0);
        error!(
            reason = why,
            "bottomless uploader fenced: another writer owns the prefix; uploads stopped"
        );
    }

    fn partitions(&self) -> u16 {
        self.node.log_cut().len() as u16
    }

    /// Run rounds on a fixed cadence of [`UploaderConfig::round_interval`] until `shutdown`
    /// turns true: each round starts one period after the previous round **started** (an
    /// overrunning round is followed at once by the next). Stops for good when fenced.
    /// Failures back off (doubling, up to 30 s) and never block apply.
    pub async fn run(
        self: Arc<Self>,
        store: Arc<dyn ObjectStore>,
        mut shutdown: watch::Receiver<bool>,
    ) {
        let period = self.cfg.round_interval();
        let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period); // clock-exempt: tokio interval cadence
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut backoff = Duration::ZERO;
        loop {
            tokio::select! {
                _ = shutdown.wait_for(|v| *v) => break,
                _ = ticker.tick() => {}
            }
            if *shutdown.borrow() {
                break;
            }
            match self.round(&store).await {
                Ok(_) => backoff = Duration::ZERO,
                Err(UploadError::Fenced(_)) => return,
                Err(e) => {
                    backoff = if backoff.is_zero() {
                        period
                    } else {
                        (backoff * 2).min(MAX_BACKOFF.max(period))
                    };
                    warn!(error = %e, retry_in = ?backoff, "bottomless upload round failed");
                    tokio::select! {
                        _ = shutdown.wait_for(|v| *v) => break,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    ticker.reset_immediately();
                }
            }
        }
        info!("bottomless uploader stopped");
    }

    /// One upload round (see the module docs). Callable with any store; switching stores
    /// starts a new session (state re-read from that store's `manifest/current`).
    pub async fn round(&self, store: &Arc<dyn ObjectStore>) -> Result<RoundReport, UploadError> {
        if self.is_fenced() {
            return Err(UploadError::Fenced("uploader already fenced".into()));
        }
        let r = self.round_inner(store).await;
        if let Err(e) = &r {
            metrics::counter!(METRIC_FAILURES, "kind" => e.kind()).increment(1);
            if let UploadError::Fenced(why) = e {
                self.set_fenced(why);
            }
            // The in-memory state may be ahead of the last durable manifest: the next round
            // re-reads it from the store (re-puts of identical objects are idempotent).
            *self.session.lock().await = None;
        }
        r
    }

    async fn round_inner(&self, store: &Arc<dyn ObjectStore>) -> Result<RoundReport, UploadError> {
        let mut report = RoundReport::default();
        let count = self.partitions();
        // Leader services only: every started partition's barrier must be open.
        let mut any = false;
        for p in 0..count {
            let pid = PartitionId(p);
            if self.node.handle(pid).is_some() {
                any = true;
                if !self.inspect && !self.node.barrier(pid).is_some_and(|b| b.is_open()) {
                    report.barrier_closed = true;
                    return Ok(report);
                }
            }
        }
        if !any {
            report.barrier_closed = true;
            return Ok(report);
        }

        let mut guard = self.session.lock().await;
        if guard.as_ref().is_none_or(|s| ptr(&s.store) != ptr(store)) {
            *guard = Some(self.open_session(store, count).await?);
        }
        let Some(sess) = guard.as_mut() else {
            return Err(UploadError::Io("no session".into()));
        };

        // 1. Applied pairs first, then the cut (W2).
        let pairs: Vec<Option<(Offset, [u8; 32])>> = (0..count)
            .map(|p| self.node.applied_state(PartitionId(p)))
            .collect();
        let cut = self.node.log_cut();
        let cut_at_ms = self.clock.now_ms();
        let started = std::time::Instant::now(); // clock-exempt: upload latency metric (W8)
        report.cut.clone_from(&cut);
        report.cut_at_ms = cut_at_ms;
        self.run_hook().await;

        // 2. Blobs put before the cut.
        report.blobs = self.upload_blobs(sess).await?;

        // 6. Meta snapshot, captured after the cut; its PUT overlaps the partition uploads.
        let meta_snap = self.meta.as_ref().and_then(|view| {
            let (index, bytes) = view.read(|s| (view.applied(), ostro_meta::encode_snapshot(s)));
            (sess.meta_index != Some(index)).then_some((index, bytes))
        });
        let meta_put = {
            let store = Arc::clone(&sess.store);
            async move {
                let Some((index, bytes)) = meta_snap else {
                    return Ok(None);
                };
                let sha: [u8; 32] = Sha256::digest(&bytes).into();
                let key = meta_key(index);
                let len = bytes.len() as u64;
                put_immutable(&store, &key, bytes, sha).await?;
                metrics::counter!(METRIC_UPLOAD_BYTES, "kind" => "meta").increment(len);
                Ok::<_, UploadError>(Some((index, key, sha)))
            }
        };

        // 3.-5. Partitions, concurrently (bounded), all against the step-1 cut.
        let ctx = PartCtx {
            node: Arc::clone(&self.node),
            store: Arc::clone(&sess.store),
            max_chunk_bytes: self.cfg.max_chunk_bytes,
        };
        let parts = std::mem::take(&mut sess.parts);
        let (parts_res, meta_res) = tokio::join!(
            upload_partitions(&ctx, parts, &cut, self.cfg.concurrency(count)),
            meta_put
        );
        let (parts, outcomes) = parts_res?;
        sess.parts = parts;
        let mut changed = false;
        let mut snapshot_changed = false;
        let mut incomplete = false;
        for o in &outcomes {
            report.snapshots += o.snapshots;
            report.chunks += o.chunks;
            report.chunk_bytes += o.chunk_bytes;
            changed |= o.changed;
            snapshot_changed |= o.snapshot_changed;
            if let Some(p) = o.skipped {
                report.skipped.push(p);
                incomplete = true;
            }
        }
        report.skipped.sort_unstable();
        if let Some((index, key, sha)) = meta_res? {
            sess.meta_index = Some(index);
            sess.meta_key = Some(key);
            sess.meta_sha = Some(sha);
            report.meta_snapshot = true;
            changed = true;
        }

        if incomplete {
            report.upload = started.elapsed();
            return Ok(report);
        }
        if !changed && sess.etag.is_some() {
            report.upload = started.elapsed();
            self.publish_metrics(sess, &pairs);
            return Ok(report);
        }

        // Retention plan (applied only after the manifest is durable).
        let plan = if self.cfg.retention && (sess.retention_due || snapshot_changed) {
            Some(self.plan_retention(sess))
        } else {
            None
        };

        // 7. Entries.
        let mut cut_entries = Vec::with_capacity(count as usize);
        for p in 0..count {
            let part = &sess.parts[p as usize];
            if self.node.handle(PartitionId(p)).is_none()
                || part.uploaded_end > cut.get(p as usize).map_or(0, |o| o.0)
            {
                if let Some(e) = &part.last_entry {
                    cut_entries.push(e.clone());
                }
                continue;
            }
            if !part.has_chain {
                continue;
            }
            let chain_start = plan
                .as_ref()
                .and_then(|pl| pl.chain_start.get(&p).copied())
                .unwrap_or(part.chain_start);
            let snap = part.referenced.and_then(|o| part.snaps.get(&o));
            let (mut vo, mut vh) = pairs[p as usize].map_or((0, [0u8; 32]), |(o, h)| (o.0, h));
            if let Some(s) = snap
                && vo < s.offset
            {
                match s.state_hash {
                    Some(h) => (vo, vh) = (s.offset, h),
                    None => {
                        return Err(UploadError::Io(format!(
                            "partition {p}: applied {vo} below snapshot {} with unknown state hash",
                            s.offset
                        )));
                    }
                }
            }
            let has_head = part.uploaded_end > chain_start;
            cut_entries.push(PartitionCut {
                p,
                cut_offset: part.uploaded_end,
                chain_start,
                last_chunk_key: if has_head {
                    part.last_key.clone()
                } else {
                    None
                },
                last_chunk_sha256: if has_head { part.last_sha } else { None },
                snapshot_key: snap.map(|s| s.key.clone()),
                snapshot_sha256: snap.map(|s| s.sha),
                snapshot_offset: snap.map(|s| s.offset),
                verify_offset: vo,
                verify_state_hash: vh,
            });
        }
        let m = Manifest {
            format: manifest::FORMAT,
            cluster_id: self.cfg.cluster_id.clone(),
            node_id: self.cfg.node_id,
            incarnation: self.node.incarnation(),
            generation: sess.generation + 1,
            prev_manifest_sha256: sess.prev_manifest_sha,
            cut_at_ms,
            cut: cut_entries,
            meta_snapshot_key: sess.meta_key.clone(),
            meta_snapshot_sha256: sess.meta_sha,
            ostro_version: env!("CARGO_PKG_VERSION").to_owned(),
        };

        // 8. Validate, fence, PUT.
        m.validate()?;
        fence::check_before_manifest(&sess.store, &self.ident).await?;
        #[cfg(feature = "failpoints")]
        fail::fail_point!("upload::before_manifest", |_| Err(UploadError::Injected(
            "upload::before_manifest"
        )));
        let bytes = manifest::encode(&m);
        let sha = manifest::object_sha(&bytes);
        let len = bytes.len() as u64;
        let put = match &sess.etag {
            Some(e) => {
                sess.store
                    .put_if_match(manifest::CURRENT_KEY, bytes.clone().into(), e)
                    .await?
            }
            None => {
                sess.store
                    .put_if_absent(manifest::CURRENT_KEY, bytes.clone().into())
                    .await?
            }
        };
        let etag = match put {
            Conditional::Ok(o) => o.e_tag,
            Conditional::PreconditionFailed => {
                return Err(UploadError::Fenced(
                    "manifest/current changed under us (precondition failed)".into(),
                ));
            }
        };
        let etag = match etag {
            Some(e) => e,
            None => sess
                .store
                .head(manifest::CURRENT_KEY)
                .await?
                .and_then(|m| m.e_tag)
                .ok_or_else(|| {
                    UploadError::Io("store reports no etag for manifest/current".into())
                })?,
        };
        metrics::counter!(METRIC_UPLOAD_BYTES, "kind" => "manifest").increment(len);
        let manifest_at_ms = self.clock.now_ms();
        if snapshot_changed || sess.generation == 0 {
            put_immutable(&sess.store, &manifest::gen_key(m.generation), bytes, sha).await?;
            if let Some(g) = sess.gens.as_mut() {
                g.insert(m.generation, Some(gen_refs(&m)));
            }
        }
        sess.etag = Some(etag);
        sess.generation = m.generation;
        sess.prev_manifest_sha = sha;
        sess.cut_at_ms = Some(cut_at_ms);
        for e in &m.cut {
            if let Some(part) = sess.parts.get_mut(e.p as usize) {
                part.chain_start = e.chain_start;
                part.last_entry = Some(e.clone());
            }
        }

        // Upload floors (only raised), after the durable manifest.
        if self.cfg.publish_floor {
            for e in &m.cut {
                let floor = e.snapshot_offset.unwrap_or(e.chain_start).min(e.cut_offset);
                if let Some(fh) = self.node.floor_handles(PartitionId(e.p)) {
                    fh.upload.send_if_modified(|v| {
                        if v.is_none_or(|c| c.0 < floor) {
                            *v = Some(Offset(floor));
                            true
                        } else {
                            false
                        }
                    });
                }
            }
        }

        // Remote retention.
        if let Some(plan) = plan {
            report.deleted = self.apply_retention(sess, plan).await;
            sess.retention_due = false;
        }

        report.upload = started.elapsed();
        report.manifest_at_ms = Some(manifest_at_ms);
        metrics::histogram!(METRIC_UPLOAD_SECONDS).record(report.upload.as_secs_f64());
        self.publish_metrics(sess, &pairs);
        debug!(
            generation = m.generation,
            chunks = report.chunks,
            "bottomless manifest written"
        );
        report.manifest = Some(m);
        Ok(report)
    }

    fn publish_metrics(&self, sess: &Session, pairs: &[Option<(Offset, [u8; 32])>]) {
        if let Some(at) = sess.cut_at_ms {
            let age = self.clock.now_ms().saturating_sub(at) as f64 / 1000.0;
            metrics::gauge!(METRIC_CUT_AGE).set(age);
        }
        for (p, pair) in pairs.iter().enumerate() {
            let Some(fh) = self.node.floor_handles(PartitionId(p as u16)) else {
                continue;
            };
            let floor = fh.upload.borrow().map_or(0, |o| o.0);
            let applied = pair.map_or(0, |(o, _)| o.0);
            metrics::gauge!(METRIC_FLOOR_LAG, "partition" => p.to_string())
                .set(applied.saturating_sub(floor) as f64);
        }
    }

    async fn run_hook(&self) {
        let hook = self
            .hook
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        if let Some(tx) = hook {
            let (resume, wait) = oneshot::channel();
            if tx.send(resume).is_ok() {
                let _ = wait.await;
            }
        }
    }

    // ------------------------------------------------------------------ session

    async fn open_session(
        &self,
        store: &Arc<dyn ObjectStore>,
        count: u16,
    ) -> Result<Session, UploadError> {
        let mut sess = Session {
            store: Arc::clone(store),
            etag: None,
            generation: 0,
            prev_manifest_sha: [0; 32],
            parts: vec![PartState::default(); count as usize],
            meta_index: None,
            meta_key: None,
            meta_sha: None,
            remote_blobs: HashSet::new(),
            pending_blobs: BTreeSet::new(),
            blobs_reconciled: false,
            gens: None,
            retention_due: true,
            cut_at_ms: None,
        };
        for p in 0..count {
            for m in store.list(&snap_prefix(p), None).await? {
                if let Some((pp, off, sha)) = parse_snap_key(&m.key)
                    && pp == p
                {
                    sess.parts[p as usize].snaps.insert(
                        off,
                        SnapRef {
                            offset: off,
                            key: m.key,
                            sha,
                            state_hash: None,
                        },
                    );
                }
            }
        }
        let Some((bytes, meta)) = store.get(manifest::CURRENT_KEY).await? else {
            return Ok(sess);
        };
        let m = manifest::decode(&bytes)?;
        sess.etag = meta.e_tag.or(store
            .head(manifest::CURRENT_KEY)
            .await?
            .and_then(|h| h.e_tag));
        sess.generation = m.generation;
        sess.prev_manifest_sha = manifest::object_sha(&bytes);
        sess.cut_at_ms = Some(m.cut_at_ms);
        sess.meta_key.clone_from(&m.meta_snapshot_key);
        sess.meta_sha = m.meta_snapshot_sha256;
        sess.meta_index = m.meta_snapshot_key.as_deref().and_then(|k| {
            k.strip_prefix("meta/")?
                .strip_suffix(".msnap")?
                .parse()
                .ok()
        });
        for e in &m.cut {
            let Some(part) = sess.parts.get_mut(e.p as usize) else {
                continue;
            };
            part.last_entry = Some(e.clone());
            part.referenced = e.snapshot_offset;
            if let (Some(o), Some(k), Some(sha)) =
                (e.snapshot_offset, &e.snapshot_key, e.snapshot_sha256)
            {
                let hash = (e.verify_offset == o).then_some(e.verify_state_hash);
                let s = part.snaps.entry(o).or_insert(SnapRef {
                    offset: o,
                    key: k.clone(),
                    sha,
                    state_hash: None,
                });
                if s.state_hash.is_none() {
                    s.state_hash = hash;
                }
            }
            match self.load_chain(store, e).await {
                Ok(chain) => {
                    part.has_chain = true;
                    part.chain_start = e.chain_start;
                    part.uploaded_end = e.cut_offset;
                    part.last_key.clone_from(&e.last_chunk_key);
                    part.last_sha = e.last_chunk_sha256;
                    part.chain = chain;
                }
                Err(err) => {
                    warn!(partition = e.p, error = %err, "remote chain unusable; restarting it from a snapshot");
                    part.has_chain = false;
                }
            }
        }
        info!(
            generation = m.generation,
            "bottomless session resumed from manifest/current"
        );
        Ok(sess)
    }

    /// The chain index of a manifest entry, from a `seg/` listing (GETs only to break ties
    /// between forked candidates).
    async fn load_chain(
        &self,
        store: &Arc<dyn ObjectStore>,
        e: &PartitionCut,
    ) -> Result<Vec<ChunkMeta>, UploadError> {
        let (Some(head), Some(head_sha)) = (&e.last_chunk_key, e.last_chunk_sha256) else {
            return Ok(Vec::new());
        };
        let listing: Vec<(String, u64, u64)> = store
            .list(&chunk::seg_prefix(e.p), None)
            .await?
            .into_iter()
            .filter_map(|m| {
                let (p, _, b, en) = chunk::parse_chunk_key(&m.key)?;
                (p == e.p).then_some((m.key, b, en))
            })
            .collect();
        let mut out = Vec::new();
        let (mut key, mut sha) = (head.clone(), head_sha);
        loop {
            let (_, base, end) = listing
                .iter()
                .find(|(k, ..)| *k == key)
                .cloned()
                .ok_or_else(|| ChunkError::Missing(key.clone()))?;
            out.push(ChunkMeta {
                key: key.clone(),
                end,
            });
            if base <= e.chain_start {
                break;
            }
            let cands: Vec<&(String, u64, u64)> =
                listing.iter().filter(|(_, _, en)| *en == base).collect();
            let prev = if cands.len() == 1 {
                // Still confirm the link when cheap? One candidate: take it; restore verifies.
                let (bytes, _) = store
                    .get(&key)
                    .await?
                    .ok_or_else(|| ChunkError::Missing(key.clone()))?;
                if chunk::chunk_sha(&bytes) != sha {
                    return Err(ChunkError::Chain(format!("{key}: sha256 mismatch")).into());
                }
                let h = chunk::peek_header(&bytes)?;
                (cands[0].0.clone(), h.prev_chunk_sha256)
            } else {
                let (bytes, _) = store
                    .get(&key)
                    .await?
                    .ok_or_else(|| ChunkError::Missing(key.clone()))?;
                let h = chunk::peek_header(&bytes)?;
                let mut found = None;
                for (k, ..) in cands {
                    if let Some((b, _)) = store.get(k).await?
                        && chunk::chunk_sha(&b) == h.prev_chunk_sha256
                    {
                        found = Some(k.clone());
                        break;
                    }
                }
                (
                    found.ok_or_else(|| ChunkError::Missing(format!("predecessor of {key}")))?,
                    h.prev_chunk_sha256,
                )
            };
            key = prev.0;
            sha = prev.1;
        }
        out.reverse();
        Ok(out)
    }

    // ------------------------------------------------------------------ blobs

    async fn upload_blobs(&self, sess: &mut Session) -> Result<usize, UploadError> {
        let Some(blobs) = &self.blobs else {
            return Ok(0);
        };
        let mut lagged = false;
        {
            let mut rx = self
                .blob_events
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if let Some(rx) = rx.as_mut() {
                loop {
                    match rx.try_recv() {
                        Ok(sha) => {
                            sess.pending_blobs.insert(sha);
                        }
                        Err(broadcast::error::TryRecvError::Lagged(_)) => lagged = true,
                        Err(_) => break,
                    }
                }
            }
        }
        let dir = blobs_dir(blobs);
        if lagged || !sess.blobs_reconciled {
            let remote: HashSet<[u8; 32]> = sess
                .store
                .list("blobs/", None)
                .await?
                .into_iter()
                .filter_map(|m| unhex32(m.key.strip_prefix("blobs/")?))
                .collect();
            for sha in list_local_blobs(&dir) {
                if !remote.contains(&sha) {
                    sess.pending_blobs.insert(sha);
                }
            }
            sess.remote_blobs = remote;
            sess.blobs_reconciled = true;
        }
        // PUTs bounded like the partition uploads; all finish before any chunk is written.
        let sem = Arc::new(tokio::sync::Semaphore::new(
            self.cfg.concurrency(self.partitions()),
        ));
        let mut set = tokio::task::JoinSet::new();
        let pending: Vec<[u8; 32]> = sess.pending_blobs.iter().copied().collect();
        for sha in pending {
            if sess.remote_blobs.contains(&sha) {
                sess.pending_blobs.remove(&sha);
                continue;
            }
            let (store, sem) = (Arc::clone(&sess.store), Arc::clone(&sem));
            let h = hex(&sha);
            let path = dir.join(&h[..2]).join(&h);
            set.spawn(async move {
                let _permit = sem.acquire_owned().await;
                let bytes = match tokio::fs::read(&path).await {
                    Ok(b) => b,
                    Err(e) => {
                        // Collected locally (GC) before upload: nothing references it any more.
                        warn!(blob = %h, error = %e, "blob vanished before upload");
                        return (sha, Ok(false));
                    }
                };
                let len = bytes.len() as u64;
                let r = put_immutable(&store, &blob_key(&sha), bytes, sha).await;
                if r.is_ok() {
                    metrics::counter!(METRIC_UPLOAD_BYTES, "kind" => "blob").increment(len);
                }
                (sha, r.map(|_| true).map_err(UploadError::from))
            });
        }
        let mut n = 0;
        while let Some(joined) = set.join_next().await {
            let (sha, r) = joined.map_err(|e| UploadError::Io(format!("blob upload task: {e}")))?;
            if r? {
                sess.remote_blobs.insert(sha);
                n += 1;
            }
            sess.pending_blobs.remove(&sha);
        }
        Ok(n)
    }

    // ------------------------------------------------------------------ retention

    fn plan_retention(&self, sess: &Session) -> RetentionPlan {
        let keep = self.cfg.remote_snapshots_keep.max(1);
        let mut plan = RetentionPlan::default();
        for (p, part) in sess.parts.iter().enumerate() {
            let p = p as u16;
            if !part.has_chain {
                continue;
            }
            let Some(r) = part.referenced else {
                continue;
            };
            if r < part.chain_start {
                continue;
            }
            let kept: Vec<u64> = part
                .snaps
                .range(part.chain_start..=r)
                .rev()
                .take(keep)
                .map(|(o, _)| *o)
                .collect();
            let Some(&oldest) = kept.last() else {
                continue;
            };
            for (o, s) in &part.snaps {
                if *o < oldest {
                    plan.delete_snaps.push((p, *o, s.key.clone()));
                }
            }
            // Chunks still needed: those ending after the oldest kept snapshot.
            let first_kept = part.chain.iter().position(|c| c.end > oldest);
            let start = match first_kept {
                Some(0) => part.chain_start,
                Some(i) => part.chain[i - 1].end,
                None => part.uploaded_end,
            };
            let keep_keys: HashSet<String> = match first_kept {
                Some(i) => part.chain[i..].iter().map(|c| c.key.clone()).collect(),
                None => HashSet::new(),
            };
            plan.chain_start.insert(p, start.max(part.chain_start));
            plan.keep_chunks
                .insert(p, (keep_keys, first_kept.unwrap_or(part.chain.len())));
        }
        plan
    }

    /// Delete what the plan dropped (after the manifest naming the new chain starts is
    /// durable). Failures are logged and retried by the next retention pass.
    async fn apply_retention(&self, sess: &mut Session, plan: RetentionPlan) -> usize {
        let store = Arc::clone(&sess.store);
        let mut doomed: Vec<String> = Vec::new();
        for (p, (keep, drop_n)) in &plan.keep_chunks {
            match store.list(&chunk::seg_prefix(*p), None).await {
                Ok(list) => doomed.extend(
                    list.into_iter()
                        .map(|m| m.key)
                        .filter(|k| !keep.contains(k)),
                ),
                Err(e) => warn!(partition = p, error = %e, "retention: seg/ listing failed"),
            }
            if let Some(part) = sess.parts.get_mut(*p as usize) {
                part.chain.drain(..(*drop_n).min(part.chain.len()));
            }
        }
        for (p, o, key) in &plan.delete_snaps {
            doomed.push(key.clone());
            if let Some(part) = sess.parts.get_mut(*p as usize) {
                part.snaps.remove(o);
            }
        }
        // Gen manifests that reference a deleted object.
        if sess.gens.is_none() {
            match store.list(manifest::GEN_PREFIX, None).await {
                Ok(list) => {
                    sess.gens = Some(
                        list.iter()
                            .filter_map(|m| manifest::parse_gen_key(&m.key))
                            .map(|g| (g, None))
                            .collect(),
                    );
                }
                Err(e) => warn!(error = %e, "retention: gen listing failed"),
            }
        }
        let doomed_set: HashSet<&str> = doomed.iter().map(String::as_str).collect();
        let mut drop_gens = Vec::new();
        if let Some(gens) = sess.gens.as_mut() {
            for (g, refs) in gens.iter_mut() {
                if *g == sess.generation {
                    continue;
                }
                if refs.is_none() {
                    *refs = match store.get(&manifest::gen_key(*g)).await {
                        // Undecodable: nothing can restore from it.
                        Ok(Some((b, _))) => Some(
                            manifest::decode(&b)
                                .map(|m| gen_refs(&m))
                                .unwrap_or_default(),
                        ),
                        // Already gone.
                        Ok(None) => Some(GenRefs::default()),
                        // Transient: decide on a later pass.
                        Err(e) => {
                            warn!(generation = *g, error = %e, "retention: gen read failed");
                            None
                        }
                    };
                }
                let dead = refs.as_ref().is_some_and(|r| {
                    r.objects.is_empty()
                        || r.objects.iter().any(|o| doomed_set.contains(o.as_str()))
                });
                if dead {
                    drop_gens.push(*g);
                }
            }
            for g in &drop_gens {
                gens.remove(g);
            }
        }
        let mut all = doomed;
        all.extend(drop_gens.iter().map(|g| manifest::gen_key(*g)));
        if all.is_empty() {
            return 0;
        }
        match store.delete_many(&all).await {
            Ok(()) => {
                debug!(deleted = all.len(), "remote retention");
                all.len()
            }
            Err(e) => {
                warn!(error = %e, "remote retention delete failed; retried later");
                sess.retention_due = true;
                0
            }
        }
    }
}

/// What one partition's upload did (steps 3-5).
#[derive(Debug, Default)]
struct PartOutcome {
    snapshots: usize,
    chunks: usize,
    chunk_bytes: u64,
    changed: bool,
    snapshot_changed: bool,
    /// Skipped by the chain start rule (no usable snapshot): no manifest this round.
    skipped: Option<u16>,
}

/// What a partition upload task needs (owned, so tasks run on any worker thread).
#[derive(Clone)]
struct PartCtx {
    node: Arc<Node>,
    store: Arc<dyn ObjectStore>,
    max_chunk_bytes: usize,
}

/// Steps 3-5 for every started partition, at most `concurrency` at a time, each against
/// `cut[p]`. Returns the partition states in partition order with the outcomes. On the first
/// failure the remaining tasks are aborted and the error is returned (the caller drops the
/// session, so the partial states are discarded).
async fn upload_partitions(
    ctx: &PartCtx,
    parts: Vec<PartState>,
    cut: &[Offset],
    concurrency: usize,
) -> Result<(Vec<PartState>, Vec<PartOutcome>), UploadError> {
    let sem = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
    let mut slots: Vec<Option<PartState>> = Vec::with_capacity(parts.len());
    let mut set = tokio::task::JoinSet::new();
    for (p, part) in parts.into_iter().enumerate() {
        let pid = PartitionId(p as u16);
        if ctx.node.handle(pid).is_none() {
            slots.push(Some(part));
            continue;
        }
        slots.push(None);
        let cut_p = cut.get(p).map_or(0, |o| o.0);
        let (ctx, sem) = (ctx.clone(), Arc::clone(&sem));
        set.spawn(async move {
            let _permit = sem.acquire_owned().await;
            let mut part = part;
            let r = ctx.upload_partition(pid, &mut part, cut_p).await;
            (p, part, r)
        });
    }
    let mut outcomes = Vec::with_capacity(set.len());
    while let Some(joined) = set.join_next().await {
        let (p, part, r) =
            joined.map_err(|e| UploadError::Io(format!("partition upload task: {e}")))?;
        outcomes.push(r?);
        slots[p] = Some(part);
    }
    let parts = slots
        .into_iter()
        .enumerate()
        .map(|(p, s)| s.ok_or_else(|| UploadError::Io(format!("partition {p}: state lost"))))
        .collect::<Result<Vec<_>, _>>()?;
    Ok((parts, outcomes))
}

impl PartCtx {
    /// Steps 3-5 of one partition.
    async fn upload_partition(
        &self,
        pid: PartitionId,
        part: &mut PartState,
        cut_p: u64,
    ) -> Result<PartOutcome, UploadError> {
        let mut out = PartOutcome::default();
        let p = pid.0;
        let Some(h) = self.node.handle(pid) else {
            return Ok(out);
        };
        let log_start = h.log().log_start().0;

        // 5a. Upload every new local snapshot now (held back from the manifest below).
        for ls in self.node.local_snapshots(pid).await {
            if part.snaps.contains_key(&ls.applied_offset.0) {
                continue;
            }
            match self.upload_snapshot(p, &ls.path).await {
                Ok(Some((s, bytes))) => {
                    out.snapshots += 1;
                    metrics::counter!(METRIC_UPLOAD_BYTES, "kind" => "snapshot").increment(bytes);
                    part.snaps.insert(s.offset, s);
                }
                Ok(None) => {}
                Err(UploadError::Io(e)) => {
                    // Deleted by local retention before we got to it.
                    debug!(partition = p, error = %e, "local snapshot vanished before upload");
                }
                Err(e) => return Err(e),
            }
        }

        // Remote ahead of the local cut (e.g. right after a restore): keep the entry.
        if part.has_chain && part.uploaded_end > cut_p {
            return Ok(out);
        }

        // 3. Chain start (B4).
        if !part.has_chain || part.uploaded_end < log_start {
            let start = if log_start <= cut_p {
                part.snaps
                    .range(log_start..=cut_p)
                    .next_back()
                    .map(|(o, _)| *o)
            } else {
                None
            };
            match start {
                Some(o) => {
                    info!(
                        partition = p,
                        chain_start = o,
                        log_start,
                        "bottomless chain starts at a snapshot"
                    );
                    *part = PartState {
                        has_chain: true,
                        chain_start: o,
                        uploaded_end: o,
                        referenced: Some(o),
                        snaps: std::mem::take(&mut part.snaps),
                        last_entry: part.last_entry.take(),
                        ..PartState::default()
                    };
                    out.changed = true;
                    out.snapshot_changed = true;
                }
                None if log_start == 0 => {
                    *part = PartState {
                        has_chain: true,
                        snaps: std::mem::take(&mut part.snaps),
                        last_entry: part.last_entry.take(),
                        ..PartState::default()
                    };
                    out.changed = true;
                }
                None => {
                    warn!(
                        partition = p,
                        log_start, "no snapshot to start the bottomless chain; requesting one"
                    );
                    metrics::counter!(METRIC_FAILURES, "kind" => "no_snapshot").increment(1);
                    out.skipped = Some(p);
                    let node = Arc::clone(&self.node);
                    tokio::spawn(async move {
                        if let Err(e) = node.snapshot_now(pid).await {
                            debug!(partition = pid.0, error = %e, "requested snapshot failed");
                        }
                    });
                    return Ok(out);
                }
            }
        }

        // 4. Chunks [uploaded_end, cut).
        let (n, bytes) = self.upload_chunks(pid, part, cut_p).await?;
        if n > 0 {
            out.changed = true;
            out.chunks += n;
            out.chunk_bytes += bytes;
            metrics::counter!(METRIC_UPLOAD_BYTES, "kind" => "chunk").increment(bytes);
        }

        // 5b. Reference the newest uploaded snapshot covered by this cut.
        let newest = part
            .snaps
            .range(part.chain_start..=part.uploaded_end)
            .next_back()
            .map(|(o, _)| *o);
        if let Some(o) = newest
            && part.referenced.is_none_or(|r| r < o)
        {
            part.referenced = Some(o);
            out.changed = true;
            out.snapshot_changed = true;
        }
        Ok(out)
    }

    // ------------------------------------------------------------------ snapshots

    /// Upload one local snapshot. `Ok(None)` if it is not a usable snapshot file.
    async fn upload_snapshot(
        &self,
        p: u16,
        path: &std::path::Path,
    ) -> Result<Option<(SnapRef, u64)>, UploadError> {
        let bytes = tokio::fs::read(path)
            .await
            .map_err(|e| UploadError::Io(format!("{}: {e}", path.display())))?;
        let checked = {
            let b = bytes.clone();
            tokio::task::spawn_blocking(move || {
                ostro_partition::store::snapshot::verify_snapshot(std::io::Cursor::new(b))
            })
            .await
            .map_err(|e| UploadError::Io(e.to_string()))?
        };
        let info = match checked {
            Ok(i) => i,
            Err(e) => {
                warn!(partition = p, path = %path.display(), error = %e, "local snapshot failed verification; not uploaded");
                return Ok(None);
            }
        };
        let sha: [u8; 32] = Sha256::digest(&bytes).into();
        let key = snap_key(p, info.applied_offset, &sha);
        let len = bytes.len() as u64;
        put_immutable(&self.store, &key, bytes, sha).await?;
        Ok(Some((
            SnapRef {
                offset: info.applied_offset,
                key,
                sha,
                state_hash: Some(info.state_hash),
            },
            len,
        )))
    }

    // ------------------------------------------------------------------ chunks

    async fn upload_chunks(
        &self,
        pid: PartitionId,
        part: &mut PartState,
        cut_p: u64,
    ) -> Result<(usize, u64), UploadError> {
        let (mut n, mut total) = (0usize, 0u64);
        while part.uploaded_end < cut_p {
            let batches = self
                .node
                .read_envelopes(pid, Offset(part.uploaded_end), self.max_chunk_bytes)
                .await
                .map_err(|e| UploadError::Log(e.to_string()))?;
            let before = part.uploaded_end;
            'batches: for b in batches {
                // Envelopes below the cut, grouped into chunks of at most max_chunk_bytes.
                let mut group: Vec<&[u8]> = Vec::new();
                let mut group_bytes = 0usize;
                for env in &b.envelopes {
                    let h = ostro_types::envelope::peek_header(env)
                        .map_err(|e| UploadError::Log(format!("stored envelope: {e}")))?;
                    let end = h.base.0 + u64::from(h.cmd_count);
                    if end > cut_p {
                        break;
                    }
                    if !group.is_empty() && group_bytes + env.len() > self.max_chunk_bytes {
                        let (c, t) = self.put_chunk(pid, part, b.epoch, &group).await?;
                        n += c;
                        total += t;
                        group.clear();
                        group_bytes = 0;
                    }
                    group.push(env);
                    group_bytes += env.len();
                }
                if !group.is_empty() {
                    let (c, t) = self.put_chunk(pid, part, b.epoch, &group).await?;
                    n += c;
                    total += t;
                }
                if part.uploaded_end >= cut_p {
                    break 'batches;
                }
            }
            if part.uploaded_end == before {
                return Err(UploadError::Log(format!(
                    "partition {}: no whole batch in [{before}, {cut_p})",
                    pid.0
                )));
            }
        }
        Ok((n, total))
    }

    async fn put_chunk(
        &self,
        pid: PartitionId,
        part: &mut PartState,
        epoch: u32,
        envelopes: &[&[u8]],
    ) -> Result<(usize, u64), UploadError> {
        let prev = part.last_sha.unwrap_or([0; 32]);
        let (h, bytes) = chunk::encode(pid.0, epoch, prev, envelopes)?;
        if h.base != part.uploaded_end {
            return Err(ChunkError::Chain(format!(
                "partition {}: chunk base {} != uploaded end {}",
                pid.0, h.base, part.uploaded_end
            ))
            .into());
        }
        let key = chunk::chunk_key(pid.0, epoch, h.base, h.end);
        let sha = chunk::chunk_sha(&bytes);
        let len = bytes.len() as u64;
        let outcome = put_immutable(&self.store, &key, bytes, sha).await?;
        if outcome == Immutable::AlreadyPresent {
            debug!(key, "chunk already present (idempotent retry)");
        }
        part.uploaded_end = h.end;
        part.last_key = Some(key.clone());
        part.last_sha = Some(sha);
        part.chain.push(ChunkMeta { key, end: h.end });
        Ok((1, len))
    }
}

#[derive(Debug, Default)]
struct RetentionPlan {
    /// New chain start per partition.
    chain_start: BTreeMap<u16, u64>,
    /// Chunk keys kept per partition, and how many chain entries are dropped from the front.
    keep_chunks: BTreeMap<u16, (HashSet<String>, usize)>,
    delete_snaps: Vec<(u16, u64, String)>,
}

fn gen_refs(m: &Manifest) -> GenRefs {
    let mut objects = Vec::new();
    for c in &m.cut {
        objects.extend(c.snapshot_key.clone());
        objects.extend(c.last_chunk_key.clone());
    }
    if objects.is_empty() {
        // Keep a gen without references alive (nothing it needs can be deleted).
        objects.push(manifest::gen_key(m.generation));
    }
    GenRefs { objects }
}

fn blobs_dir(b: &BlobStore) -> PathBuf {
    let probe = b.path_of(&ostro_blob::BlobRef {
        sha256: [0; 32],
        len: 0,
    });
    probe
        .parent()
        .and_then(|p| p.parent())
        .map(PathBuf::from)
        .unwrap_or_default()
}

fn list_local_blobs(dir: &std::path::Path) -> Vec<[u8; 32]> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for sub in rd.flatten() {
        if !sub.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Ok(files) = std::fs::read_dir(sub.path()) else {
            continue;
        };
        for f in files.flatten() {
            if let Some(sha) = f.file_name().to_str().and_then(unhex32) {
                out.push(sha);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        let sha = [0xab; 32];
        let k = snap_key(2, 42, &sha);
        assert!(k.starts_with("p/0002/snap/00000000000000000042-abab"));
        assert_eq!(parse_snap_key(&k), Some((2, 42, sha)));
        assert_eq!(parse_snap_key("p/0002/snap/42-ab.osnap"), None);
        assert_eq!(meta_key(7), "meta/00000000000000000007.msnap");
        assert_eq!(blob_key(&[1; 32]).len(), "blobs/".len() + 64);
        assert_eq!(unhex32(&hex(&sha)), Some(sha));
    }
}
