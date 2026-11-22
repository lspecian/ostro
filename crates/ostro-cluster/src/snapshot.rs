//! Leader side of `Replication.FetchSnapshot` (12-12, research Pattern 6, CLUS-04).
//!
//! A follower that is empty or behind the leader's `log_start` can only catch up through a
//! snapshot. [`stream_snapshot`] serves Phase 8's **one** snapshot format (OSNP v1, P8-D1),
//! byte for byte, as a resumable stream of [`SnapshotFrame`]s:
//!
//! - frame `0` is the 64-byte OSNP header;
//! - frame `i >= 1` is the `i`-th stored OSNP frame exactly as on disk
//!   (`u32 LE len | kind | payload | u32 LE crc32c`), checked against its CRC before it is
//!   sent;
//! - `last = true` on the trailer frame.
//!
//! Concatenating the frames in index order therefore reproduces the snapshot file. A request
//! with `resume_frame = k` gets frames `k..` of the **same** file (see pins below).
//!
//! **Which snapshot.** The newest local snapshot (`snapshots/pNNNN/<applied>.osnap`) whose
//! `applied_offset >= log_start` and whose header passes Phase 8's `quick_check`, so the
//! follower can continue from the log at `applied_offset`. When there is none, one is
//! produced now ([`Exporter`]): through the partition's `SnapshotService::take` (a regular,
//! durable local snapshot) or, without a snapshot service, as an ephemeral export of the
//! store that lives only as long as the pin. Exports are single-flight per partition:
//! concurrent requesters wait for and share the one export.
//!
//! **Pins.** The streamed file is held open (an open file descriptor keeps an unlinked inode
//! readable), so local-snapshot retention deleting it mid-stream does not cut the stream.
//! Each requesting replica keeps its pin between requests for [`SnapshotServeConfig::pin_ttl`]
//! (measured on the injected [`Clock`]), so a resume (`resume_frame > 0`) continues the same
//! file even after retention removed its name. A resume without a pin is refused with
//! `FAILED_PRECONDITION` (the follower restarts at frame 0): frames of two different
//! snapshots are never spliced.
//!
//! **Authorization** is the one of `Fetch`: the calling node (`authz::require_node`) must be
//! `replica_id` and one of the partition's assigned replicas, else `PERMISSION_DENIED`.

use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ostro_log::ReplicatedLog;
use ostro_partition::Clock;
use ostro_partition::PartitionStore;
use ostro_partition::leader::snapshot::{
    LocalSnapshot, SNAPSHOT_EXT, SnapshotService, TMP_PREFIX, quick_check, snapshot_dir,
};
use ostro_partition::roles::{PartitionAssignmentView, PartitionLogHandle};
use ostro_partition::store::snapshot::{
    HEADER_LEN, MAX_FRAME_LEN, RawFrame, SnapshotError, SnapshotHeader, kind,
};
use ostro_proto::cluster::v1::{FetchSnapshotRequest, SnapshotFrame};
use ostro_types::{Offset, PartitionCount, PartitionId};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use crate::service::BoxStream;

/// The gRPC message limit for `FetchSnapshot` on both sides: the largest OSNP frame
/// (`MAX_FRAME_LEN` payload + 9 bytes of framing) plus 64 KiB for the message itself.
pub const SNAPSHOT_MESSAGE_LIMIT: usize = MAX_FRAME_LEN as usize + 9 + 64 * 1024;

/// Default [`SnapshotServeConfig::pin_ttl`].
pub const DEFAULT_PIN_TTL: Duration = Duration::from_secs(600);
/// Default [`SnapshotServeConfig::export_reuse`].
pub const DEFAULT_EXPORT_REUSE: Duration = Duration::from_secs(60);
/// Default [`SnapshotServeConfig::channel_frames`].
pub const DEFAULT_CHANNEL_FRAMES: usize = 4;

/// Name prefix of an ephemeral on-demand export (a `.tmp-*` file: removed by Phase 8's
/// startup scan if a crash leaves it behind; unlinked as soon as it is open otherwise).
const SERVE_TMP_PREFIX: &str = "serve-";

/// Leader-side snapshot serving tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotServeConfig {
    /// How long a replica's pin survives without a request (resume window).
    pub pin_ttl: Duration,
    /// How long an ephemeral on-demand export is reused by later requesters (while it
    /// still covers `log_start`).
    pub export_reuse: Duration,
    /// Frames buffered between the file reader and the gRPC stream.
    pub channel_frames: usize,
}

impl Default for SnapshotServeConfig {
    fn default() -> Self {
        Self {
            pin_ttl: DEFAULT_PIN_TTL,
            export_reuse: DEFAULT_EXPORT_REUSE,
            channel_frames: DEFAULT_CHANNEL_FRAMES,
        }
    }
}

/// How a snapshot is produced when no local one covers `log_start`.
#[derive(Clone)]
pub enum Exporter {
    /// The partition's local snapshot service: `take()` writes a regular durable local
    /// snapshot (floor, retention and upload as for any other).
    Service(Arc<SnapshotService>),
    /// No snapshot service: export the store into an ephemeral file that lives only while
    /// it is pinned.
    Store(Arc<PartitionStore>),
    /// Never export (only existing local snapshots are served).
    None,
}

impl fmt::Debug for Exporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Exporter::Service(_) => "Exporter::Service",
            Exporter::Store(_) => "Exporter::Store",
            Exporter::None => "Exporter::None",
        })
    }
}

/// Counters of one partition's snapshot serving.
#[derive(Debug, Default)]
pub struct SnapshotServeStats {
    /// Snapshots produced on demand.
    pub exports: AtomicU64,
    /// Streams started (fresh and resumed).
    pub streams: AtomicU64,
    /// Streams that resumed at `resume_frame > 0`.
    pub resumes: AtomicU64,
    /// Frames sent.
    pub frames: AtomicU64,
}

/// An open (pinned) snapshot file.
#[derive(Debug)]
pub struct PinnedSnapshot {
    /// Its header.
    pub header: SnapshotHeader,
    /// Where it was opened (may no longer exist: the open descriptor keeps it readable).
    pub path: PathBuf,
    /// File length.
    pub len: u64,
    /// Ephemeral on-demand export (never a local snapshot).
    pub ephemeral: bool,
    file: File,
    /// Clock time it was opened.
    opened_at_ms: u64,
}

impl PinnedSnapshot {
    /// `applied_offset` of the snapshot.
    pub fn applied(&self) -> Offset {
        Offset(self.header.applied_offset)
    }
}

struct Pin {
    snap: Arc<PinnedSnapshot>,
    last_used_ms: u64,
}

/// Everything the leader needs to serve snapshots of one partition.
pub struct SnapshotCtx {
    partition: PartitionId,
    count: PartitionCount,
    dir: PathBuf,
    log: Arc<PartitionLogHandle>,
    assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    exporter: Exporter,
    clock: Arc<dyn Clock>,
    cfg: SnapshotServeConfig,
    pins: Mutex<HashMap<u64, Pin>>,
    export: tokio::sync::Mutex<Option<Arc<PinnedSnapshot>>>,
    stats: Arc<SnapshotServeStats>,
}

impl fmt::Debug for SnapshotCtx {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnapshotCtx")
            .field("partition", &self.partition)
            .field("dir", &self.dir)
            .field("exporter", &self.exporter)
            .finish_non_exhaustive()
    }
}

impl SnapshotCtx {
    /// The snapshot context of `log`'s partition (of `count`) on the node whose data dir is
    /// `data_dir` (local snapshots in `data_dir/snapshots/pNNNN`). `assignments` is the
    /// meta-published assignment watch; `clock` the node's injected clock.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log: Arc<PartitionLogHandle>,
        count: PartitionCount,
        data_dir: &Path,
        assignments: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
        exporter: Exporter,
        clock: Arc<dyn Clock>,
        cfg: SnapshotServeConfig,
    ) -> Self {
        let partition = log.partition();
        Self {
            partition,
            count,
            dir: snapshot_dir(data_dir, partition),
            log,
            assignments,
            exporter,
            clock,
            cfg,
            pins: Mutex::new(HashMap::new()),
            export: tokio::sync::Mutex::new(None),
            stats: Arc::new(SnapshotServeStats::default()),
        }
    }

    /// The partition.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// The counters.
    pub fn stats(&self) -> Arc<SnapshotServeStats> {
        Arc::clone(&self.stats)
    }

    /// The snapshot currently pinned for `replica` (tests, status).
    pub fn pinned(&self, replica: u64) -> Option<Arc<PinnedSnapshot>> {
        self.pins_lock().get(&replica).map(|p| Arc::clone(&p.snap))
    }

    fn pins_lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Pin>> {
        self.pins.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn is_replica(&self, peer: u64) -> bool {
        self.assignments
            .borrow()
            .iter()
            .find(|a| a.partition == self.partition)
            .is_some_and(|a| a.replicas.contains(&peer))
    }

    /// Drop pins unused for longer than `pin_ttl`.
    fn prune_pins(&self, now: u64) {
        let ttl = u64::try_from(self.cfg.pin_ttl.as_millis()).unwrap_or(u64::MAX);
        self.pins_lock()
            .retain(|_, p| now.saturating_sub(p.last_used_ms) <= ttl);
    }

    /// The newest usable local snapshot (`applied >= start`, quick check passes), opened.
    async fn newest_local(&self, start: Offset) -> Option<Arc<PinnedSnapshot>> {
        let dir = self.dir.clone();
        let (p, count) = (self.partition, self.count);
        let now = self.clock.now_ms();
        tokio::task::spawn_blocking(move || {
            let snaps = list_local(&dir).ok()?;
            for s in snaps.into_iter().rev() {
                if s.applied_offset < start {
                    break;
                }
                match open_local(&s, p, count, now) {
                    Ok(pinned) => return Some(Arc::new(pinned)),
                    Err(e) => {
                        debug!(partition = p.0, file = %s.path.display(), error = %e,
                            "local snapshot not servable; trying an older one");
                    }
                }
            }
            None
        })
        .await
        .ok()
        .flatten()
    }

    /// A snapshot covering `log_start`: the newest local one, else an export (single-flight).
    async fn select(&self) -> Result<Arc<PinnedSnapshot>, Status> {
        let start = self.log.log_start();
        if let Some(s) = self.newest_local(start).await {
            return Ok(s);
        }
        let mut cached = self.export.lock().await;
        // A concurrent requester may have produced one while we waited.
        let now = self.clock.now_ms();
        let reuse = u64::try_from(self.cfg.export_reuse.as_millis()).unwrap_or(u64::MAX);
        if let Some(c) = cached.as_ref()
            && c.applied() >= self.log.log_start()
            && now.saturating_sub(c.opened_at_ms) <= reuse
        {
            return Ok(Arc::clone(c));
        }
        if let Some(s) = self.newest_local(self.log.log_start()).await {
            return Ok(s);
        }
        let produced = match &self.exporter {
            Exporter::None => {
                return Err(Status::unavailable(format!(
                    "partition {}: no local snapshot covers log start {}",
                    self.partition.0,
                    self.log.log_start()
                )));
            }
            Exporter::Service(svc) => {
                let taken = svc
                    .take()
                    .await
                    .map_err(|e| Status::internal(format!("snapshot export: {e}")))?;
                self.stats.exports.fetch_add(1, Ordering::Relaxed);
                match taken {
                    Some(t) => {
                        let s = LocalSnapshot {
                            applied_offset: t.applied_offset,
                            path: t.path,
                        };
                        let (p, count) = (self.partition, self.count);
                        tokio::task::spawn_blocking(move || open_local(&s, p, count, now))
                            .await
                            .map_err(|e| Status::internal(format!("snapshot open task: {e}")))?
                            .map(Arc::new)
                            .map_err(|e| Status::internal(format!("snapshot open: {e}")))?
                    }
                    None => self
                        .newest_local(self.log.log_start())
                        .await
                        .ok_or_else(|| Status::unavailable("no snapshot covers log start"))?,
                }
            }
            Exporter::Store(store) => {
                let store = Arc::clone(store);
                let dir = self.dir.clone();
                let n = self.stats.exports.fetch_add(1, Ordering::Relaxed);
                let s = tokio::task::spawn_blocking(move || export_ephemeral(&store, &dir, n, now))
                    .await
                    .map_err(|e| Status::internal(format!("snapshot export task: {e}")))?
                    .map_err(|e| Status::internal(format!("snapshot export: {e}")))?;
                let s = Arc::new(s);
                *cached = Some(Arc::clone(&s));
                s
            }
        };
        if produced.applied() < self.log.log_start() {
            return Err(Status::unavailable(format!(
                "partition {}: exported snapshot at {} is below log start {}",
                self.partition.0,
                produced.applied(),
                self.log.log_start()
            )));
        }
        info!(
            partition = self.partition.0,
            applied = %produced.applied(),
            bytes = produced.len,
            ephemeral = produced.ephemeral,
            "snapshot produced on demand for a follower"
        );
        Ok(produced)
    }
}

/// Local snapshot files of `dir`, ascending by `applied_offset`. Unlike Phase 8's
/// `scan_dir` this never deletes anything (a `.tmp-*` file may be a snapshot being written
/// right now).
fn list_local(dir: &Path) -> io::Result<Vec<LocalSnapshot>> {
    let mut out = Vec::new();
    let rd = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(out),
        Err(e) => return Err(e),
    };
    let ext = format!(".{SNAPSHOT_EXT}");
    for e in rd {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(stem) = name.strip_suffix(&ext) else {
            continue;
        };
        if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if let Ok(off) = stem.parse::<u64>() {
            out.push(LocalSnapshot {
                applied_offset: Offset(off),
                path: e.path(),
            });
        }
    }
    out.sort_by_key(|s| s.applied_offset);
    Ok(out)
}

fn open_local(
    s: &LocalSnapshot,
    p: PartitionId,
    count: PartitionCount,
    now: u64,
) -> Result<PinnedSnapshot, SnapshotError> {
    // Open first: from here retention may unlink the name, the descriptor stays readable.
    let file = File::open(&s.path)?;
    let header = quick_check(s, p, count)?;
    let len = file.metadata()?.len();
    Ok(PinnedSnapshot {
        header,
        path: s.path.clone(),
        len,
        ephemeral: false,
        file,
        opened_at_ms: now,
    })
}

/// Export `store` into `dir/.tmp-serve-<n>`, open it and unlink the name: the file lives
/// as long as the returned pin.
fn export_ephemeral(
    store: &PartitionStore,
    dir: &Path,
    n: u64,
    now: u64,
) -> Result<PinnedSnapshot, SnapshotError> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{TMP_PREFIX}{SERVE_TMP_PREFIX}{n:020}"));
    let res = (|| {
        let f = File::create(&path)?;
        let mut w = io::BufWriter::with_capacity(1 << 20, f);
        store.export_snapshot(&mut w, now)?;
        let f = w
            .into_inner()
            .map_err(|e| SnapshotError::Io(e.into_error()))?;
        drop(f);
        let file = File::open(&path)?;
        let len = file.metadata()?.len();
        let mut hb = [0u8; HEADER_LEN];
        file.read_exact_at(&mut hb, 0)?;
        let header = SnapshotHeader::decode(&hb)?;
        Ok(PinnedSnapshot {
            header,
            path: path.clone(),
            len,
            ephemeral: true,
            file,
            opened_at_ms: now,
        })
    })();
    let _ = std::fs::remove_file(&path);
    res
}

/// `Replication.FetchSnapshot` behind `authz::require_node` (what 12-14's service calls).
pub async fn serve_snapshot(
    ctx: &SnapshotCtx,
    req: Request<FetchSnapshotRequest>,
) -> Result<Response<BoxStream<SnapshotFrame>>, Status> {
    let peer = crate::authz::require_node(&req)?;
    stream_snapshot(ctx, peer, req.into_inner())
        .await
        .map(Response::new)
}

/// Serve `req` from the calling node `peer` (see the module docs).
pub async fn stream_snapshot(
    ctx: &SnapshotCtx,
    peer: u64,
    req: FetchSnapshotRequest,
) -> Result<BoxStream<SnapshotFrame>, Status> {
    if req.partition != u32::from(ctx.partition.0) {
        return Err(Status::not_found(format!(
            "partition {} is not served here",
            req.partition
        )));
    }
    if peer != req.replica_id || !ctx.is_replica(peer) {
        return Err(Status::permission_denied(format!(
            "node {peer} is not a replica of partition {} (replica_id {})",
            ctx.partition.0, req.replica_id
        )));
    }
    let now = ctx.clock.now_ms();
    ctx.prune_pins(now);
    let snap = if req.resume_frame == 0 {
        let s = ctx.select().await?;
        ctx.pins_lock().insert(
            peer,
            Pin {
                snap: Arc::clone(&s),
                last_used_ms: now,
            },
        );
        s
    } else {
        let mut pins = ctx.pins_lock();
        let Some(pin) = pins.get_mut(&peer) else {
            return Err(Status::failed_precondition(
                "no pinned snapshot to resume: restart at frame 0",
            ));
        };
        pin.last_used_ms = now;
        ctx.stats.resumes.fetch_add(1, Ordering::Relaxed);
        Arc::clone(&pin.snap)
    };
    ctx.stats.streams.fetch_add(1, Ordering::Relaxed);
    debug!(
        partition = ctx.partition.0,
        replica = peer,
        applied = %snap.applied(),
        resume_frame = req.resume_frame,
        "streaming snapshot"
    );
    let (tx, rx) = mpsc::channel(ctx.cfg.channel_frames.max(1));
    let stats = Arc::clone(&ctx.stats);
    let partition = ctx.partition;
    let resume = req.resume_frame;
    tokio::task::spawn_blocking(move || {
        if let Err(e) = pump(&snap, resume, &tx, &stats) {
            warn!(partition = partition.0, error = %e, "snapshot stream failed");
            let _ = tx.blocking_send(Err(e));
        }
    });
    Ok(Box::pin(ReceiverStream::new(rx)))
}

/// Read the pinned file frame by frame and send frames `resume..` (blocking).
fn pump(
    snap: &PinnedSnapshot,
    resume: u64,
    tx: &mpsc::Sender<Result<SnapshotFrame, Status>>,
    stats: &SnapshotServeStats,
) -> Result<(), Status> {
    let io_err = |e: io::Error| Status::internal(format!("snapshot read: {e}"));
    let corrupt = |what: String| Status::data_loss(format!("local snapshot corrupt: {what}"));
    let send = |f: SnapshotFrame| -> bool {
        stats.frames.fetch_add(1, Ordering::Relaxed);
        tx.blocking_send(Ok(f)).is_ok()
    };
    let len = snap.len;
    if (len as usize) < HEADER_LEN {
        return Err(corrupt("shorter than the header".into()));
    }
    if resume == 0 {
        let mut hb = vec![0u8; HEADER_LEN];
        snap.file.read_exact_at(&mut hb, 0).map_err(io_err)?;
        if !send(SnapshotFrame {
            index: 0,
            bytes: hb,
            last: false,
        }) {
            return Ok(());
        }
    }
    let mut pos = HEADER_LEN as u64;
    let mut index = 1u64;
    loop {
        if pos + 5 > len {
            return Err(corrupt(format!("truncated before frame {index}")));
        }
        let mut lk = [0u8; 5];
        snap.file.read_exact_at(&mut lk, pos).map_err(io_err)?;
        let flen = u32::from_le_bytes([lk[0], lk[1], lk[2], lk[3]]);
        let fkind = lk[4];
        if flen > MAX_FRAME_LEN {
            return Err(corrupt(format!("frame {index} length {flen}")));
        }
        let total = 9 + u64::from(flen);
        if pos + total > len {
            return Err(corrupt(format!("truncated in frame {index}")));
        }
        let last = fkind == kind::TRAILER;
        if index >= resume {
            let mut buf = vec![0u8; total as usize];
            snap.file.read_exact_at(&mut buf, pos).map_err(io_err)?;
            if !frame_crc_ok(&buf) {
                return Err(corrupt(format!("CRC mismatch in frame {index}")));
            }
            if !send(SnapshotFrame {
                index,
                bytes: buf,
                last,
            }) {
                return Ok(());
            }
        }
        if last {
            if pos + total != len {
                return Err(corrupt("bytes after the trailer".into()));
            }
            return Ok(());
        }
        pos += total;
        index += 1;
    }
}

/// Whether `b` is one well-formed stored OSNP frame (`len | kind | payload | crc`) with a
/// valid CRC. Re-encodes it through Phase 8's [`RawFrame::encode`] (the only CRC
/// definition) and compares.
pub fn frame_crc_ok(b: &[u8]) -> bool {
    if b.len() < 9 {
        return false;
    }
    let len = u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize;
    if b.len() != 9 + len {
        return false;
    }
    let raw = RawFrame {
        kind: b[4],
        payload: b[5..5 + len].to_vec(),
    };
    raw.encode() == b
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_crc_check_round_trips() {
        let f = RawFrame {
            kind: 2,
            payload: b"hello".to_vec(),
        };
        let mut b = f.encode();
        assert!(frame_crc_ok(&b));
        let n = b.len();
        b[n - 1] ^= 1;
        assert!(!frame_crc_ok(&b));
        assert!(!frame_crc_ok(&b[..4]));
    }

    #[test]
    fn local_listing_ignores_tmp_and_sorts() {
        let d = tempfile::tempdir().unwrap();
        for n in ["00000000000000000009.osnap", "00000000000000000003.osnap"] {
            std::fs::write(d.path().join(n), b"x").unwrap();
        }
        std::fs::write(d.path().join(".tmp-00000000000000000011"), b"x").unwrap();
        std::fs::write(d.path().join("00000000000000000003.blobrefs"), b"x").unwrap();
        let v = list_local(d.path()).unwrap();
        assert_eq!(
            v.iter().map(|s| s.applied_offset.0).collect::<Vec<_>>(),
            vec![3, 9]
        );
        assert!(d.path().join(".tmp-00000000000000000011").exists());
        assert!(list_local(&d.path().join("missing")).unwrap().is_empty());
    }
}
