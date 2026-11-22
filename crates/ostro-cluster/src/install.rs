//! Follower snapshot install (12-12, research Pattern 6, D-P12-8, CLUS-04).
//!
//! [`install_from_leader`] bootstraps a follower that is empty or behind the leader's
//! `log_start` from the leader's OSNP v1 snapshot (Phase 8's one format, installed with
//! Phase 8's `install_snapshot`, unchanged). The order is fixed and never reordered:
//!
//! 1. **Stage.** Stream `Replication.FetchSnapshot` into
//!    `snapshots/pNNNN/.incoming-<applied 20d>`. Every frame is checked as it arrives
//!    (index, header CRC for frame 0, frame CRC for the others, `last` only on the trailer);
//!    a broken stream resumes at the next frame (`resume_frame`, the leader keeps the file
//!    pinned), at most [`InstallConfig::max_resumes`] times. Stale `.incoming-*` files of
//!    earlier attempts are deleted first.
//! 2. **Verify** before touching anything: the staged file is fsynced, re-read and fully
//!    verified by Phase 8's `verify_snapshot` (header CRC, every frame CRC, structure, row
//!    order, trailer digest and counts); the sha256 of the re-read file must equal the sha256
//!    of the received bytes; `partition` / `partition_count` must be this partition's. The
//!    epoch history `S` is anchored with comes from the snapshot's cursor row
//!    (`CursorMeta.epoch_history`).
//! 3. **Quiesce** (`PartitionControl::quiesce_for_install`): apply machine and services
//!    stopped, store closed, fetcher idle (it is the caller).
//! 4. **Reset the log first:** `reset_to(S, history)` deletes every segment, fsyncs the
//!    directory and anchors the log at `S`. Failpoint `install::after_log_reset`.
//! 5. **Install the state:** failpoint `install::before_rename`, then Phase 8
//!    `install_snapshot` (staged dir, digest + applied check, atomic rename, `state/` fsync),
//!    then failpoint `install::after_rename`.
//! 6. **Keep the snapshot:** the verified file becomes the local snapshot
//!    `snapshots/pNNNN/<S 20d>.osnap` (with its `.blobrefs` sidecar), so the reopened
//!    partition's snapshot service counts it.
//! 7. **Reopen** (`InstallGuard::reopen`): the partition reopens on the new state; the log is
//!    already anchored at `S` with the snapshot's epochs. The reopen closure must run the
//!    derived-keyspace rebuild hook (`PartitionStore::start_run_index_rebuild`, see
//!    `ostro_partition::roles::InstallGuard::new`): `x_*` keyspaces are not in OSNP.
//!
//! The fetcher then fetches from `S` with `last_fetched_epoch = header.latest_epoch` (the
//! newest epoch of the seeded history), as returned in [`InstallOutcome::Installed`].
//!
//! **Crash safety.** A crash at any step reopens (Phase 12 F3 anchoring: an empty log opens
//! at `log_start = LEO = applied`) into either the old state (crash before the rename: the
//! log is empty and anchored at the old applied offset) or the new one (after the rename:
//! anchored at `S`); never "state ahead of log". The next fetch then re-requests the
//! snapshot or continues from `S`.
//!
//! **In-process failures.** Steps 3-7 run in a spawned task, so cancelling the caller (the
//! fetcher being stopped) never abandons a quiesced partition half way. When steps 4 or 5
//! fail without a crash, an interrupted Phase 8 install is finished or rolled back
//! (`recover_install`), the log is reset again to the live state's applied offset and epoch
//! history, and the partition is reopened: the same outcome as a crash at that point.

use std::fs::File;
use std::io::{self, BufReader, Read, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt as _;
use ostro_log::{EpochHistory, ReplicatedLog};
use ostro_partition::PartitionStore;
use ostro_partition::leader::snapshot::{
    BLOBREFS_EXT, TMP_PREFIX, snapshot_dir, snapshot_file_name,
};
use ostro_partition::roles::{InstallGuard, PartitionControl, PartitionLogHandle};
use ostro_partition::store::Table;
use ostro_partition::store::meta::K_CURSOR;
use ostro_partition::store::snapshot::{
    Frame, HEADER_LEN, OsnpReader, SnapshotError, SnapshotHeader, SnapshotInfo, install_snapshot,
    kind, recover_install, verify_snapshot,
};
use ostro_partition::store::values::{self, CursorMeta};
use ostro_proto::cluster::v1::FetchSnapshotRequest;
use ostro_proto::cluster::v1::replication_client::ReplicationClient;
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};
use sha2::{Digest as _, Sha256};
use tracing::{info, warn};

use crate::snapshot::{SNAPSHOT_MESSAGE_LIMIT, frame_crc_ok};
use crate::transport::ClusterTransport;

/// Prefix of a snapshot being received (`.incoming-<applied 20d>`).
pub const INCOMING_PREFIX: &str = ".incoming-";

/// Default [`InstallConfig::frame_timeout`].
pub const DEFAULT_FRAME_TIMEOUT: Duration = Duration::from_secs(30);
/// Default [`InstallConfig::max_resumes`].
pub const DEFAULT_MAX_RESUMES: u32 = 5;

macro_rules! install_fail_point {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name, |_| Err(InstallError::Injected($name)));
    };
}

/// Install tuning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstallConfig {
    /// Longest wait for the next frame (and for the stream to open).
    pub frame_timeout: Duration,
    /// Resumes (`resume_frame > 0`) after a broken stream before the attempt fails.
    pub max_resumes: u32,
}

impl Default for InstallConfig {
    fn default() -> Self {
        Self {
            frame_timeout: DEFAULT_FRAME_TIMEOUT,
            max_resumes: DEFAULT_MAX_RESUMES,
        }
    }
}

/// Everything an install needs: this node's id and data dir, the partition count, the
/// cluster transport to reach the leader and the partition lifecycle to quiesce/reopen.
///
/// `InstallCtx::default()` is an unconfigured context (fetcher tests that never install):
/// every install fails with [`InstallError::NotConfigured`].
#[derive(Default)]
pub struct InstallCtx {
    inner: Option<Configured>,
    cfg: InstallConfig,
}

#[derive(Clone)]
struct Configured {
    node_id: u64,
    data_dir: PathBuf,
    count: PartitionCount,
    transport: Arc<dyn ClusterTransport>,
    control: Arc<dyn PartitionControl>,
}

impl std::fmt::Debug for InstallCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("InstallCtx");
        if let Some(c) = &self.inner {
            d.field("node_id", &c.node_id)
                .field("data_dir", &c.data_dir)
                .field("count", &c.count.get());
        } else {
            d.field("configured", &false);
        }
        d.field("cfg", &self.cfg).finish_non_exhaustive()
    }
}

impl InstallCtx {
    /// The install context of node `node_id` (data dir `data_dir`, `count` partitions).
    pub fn new(
        node_id: u64,
        data_dir: PathBuf,
        count: PartitionCount,
        transport: Arc<dyn ClusterTransport>,
        control: Arc<dyn PartitionControl>,
    ) -> Self {
        Self {
            inner: Some(Configured {
                node_id,
                data_dir,
                count,
                transport,
                control,
            }),
            cfg: InstallConfig::default(),
        }
    }

    /// Replace the tuning.
    pub fn with_config(mut self, cfg: InstallConfig) -> Self {
        self.cfg = cfg;
        self
    }

    /// The tuning.
    pub fn config(&self) -> &InstallConfig {
        &self.cfg
    }
}

/// What an install did.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum InstallOutcome {
    /// The snapshot is installed: the log is anchored at `applied` and fetching resumes
    /// there with `last_fetched_epoch = latest_epoch`.
    Installed {
        /// The snapshot's `applied_offset` `S` (the new log start).
        applied: Offset,
        /// The snapshot header's `latest_epoch`.
        latest_epoch: Epoch,
        /// The snapshot's logical digest.
        logical_digest: [u8; 32],
        /// Snapshot size in bytes.
        bytes: u64,
    },
}

/// Install failure.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InstallError {
    /// The context has no transport/control (`InstallCtx::default()`).
    #[error("snapshot install is not configured on this node")]
    NotConfigured,
    /// Reaching the leader or the stream failed.
    #[error("snapshot stream from node {leader}: {reason}")]
    Stream {
        /// The leader.
        leader: u64,
        /// What failed.
        reason: String,
    },
    /// The received snapshot is broken (CRC, structure, digest, sha256, framing).
    #[error("snapshot rejected: {0}")]
    Corrupt(String),
    /// The snapshot is of another partition / partition count.
    #[error("snapshot mismatch: {0}")]
    Mismatch(String),
    /// Local I/O.
    #[error("install I/O: {0}")]
    Io(#[from] io::Error),
    /// Quiescing the partition failed (nothing was changed).
    #[error("quiesce partition: {0}")]
    Quiesce(String),
    /// Resetting the log failed.
    #[error("log reset: {0}")]
    Log(String),
    /// Phase 8 `install_snapshot` failed.
    #[error("state install: {0}")]
    State(String),
    /// Reopening the partition failed.
    #[error("reopen partition: {0}")]
    Reopen(String),
    /// The install task died (panic) before finishing.
    #[error("install task aborted: {0}")]
    Aborted(String),
    /// Injected failure (failpoints).
    #[error("injected failure at {0}")]
    Injected(&'static str),
}

fn corrupt(e: impl std::fmt::Display) -> InstallError {
    InstallError::Corrupt(e.to_string())
}

/// A verified snapshot file in the partition's snapshot dir.
#[derive(Debug)]
struct Staged {
    path: PathBuf,
    header: SnapshotHeader,
    info: SnapshotInfo,
    history: EpochHistory,
}

/// Fetch the leader's snapshot for `p` and install it (see the module docs).
pub async fn install_from_leader(
    ctx: &InstallCtx,
    p: PartitionId,
    leader: u64,
) -> Result<InstallOutcome, InstallError> {
    let Some(c) = ctx.inner.clone() else {
        return Err(InstallError::NotConfigured);
    };
    let cfg = ctx.cfg;
    info!(partition = p.0, leader, "snapshot install: fetching");
    let staged = stage(&c, cfg, p, leader).await?;
    info!(
        partition = p.0,
        leader,
        applied = staged.header.applied_offset,
        latest_epoch = staged.header.latest_epoch,
        bytes = staged.info.bytes,
        "snapshot install: staged and verified"
    );
    // Steps 3-7 must not be abandoned half way by a cancelled caller.
    let task = tokio::spawn(apply_staged(c, p, staged));
    match task.await {
        Ok(r) => r,
        Err(e) => Err(InstallError::Aborted(e.to_string())),
    }
}

/// Steps 3-7.
async fn apply_staged(
    c: Configured,
    p: PartitionId,
    staged: Staged,
) -> Result<InstallOutcome, InstallError> {
    let s = Offset(staged.header.applied_offset);
    let guard = c
        .control
        .quiesce_for_install(p)
        .await
        .map_err(|e| InstallError::Quiesce(e.to_string()))?;

    let target = Target {
        log: Arc::clone(&guard.log),
        state_root: guard.state_root.clone(),
        store_cfg: guard.store_cfg.clone(),
    };
    match reset_and_install(&target, &c, p, &staged).await {
        Ok(()) => {}
        Err(e) => {
            warn!(partition = p.0, error = %e,
                "snapshot install failed; re-anchoring the log at the live state");
            let (root, cfg, count) = (guard.state_root.clone(), guard.store_cfg.clone(), c.count);
            let live =
                tokio::task::spawn_blocking(move || live_position(&root, p, count, &cfg)).await;
            match live {
                Ok(Ok((applied, history))) => {
                    if let Err(e2) = target.log.reset_to(applied, history).await {
                        warn!(partition = p.0, error = %e2,
                            "log re-anchor after a failed install failed");
                    }
                }
                Ok(Err(e2)) => warn!(partition = p.0, error = %e2,
                    "live state unreadable after a failed install"),
                Err(e2) => warn!(partition = p.0, error = %e2, "live-state task failed"),
            }
            reopen_after_failure(guard, p).await;
            return Err(e);
        }
    }

    // From here state and log agree at `S`: a failure keeps them and reopens.
    if let Err(e) = after_rename() {
        reopen_after_failure(guard, p).await;
        return Err(e);
    }

    // 6. keep the verified file as a local snapshot.
    let dir = snapshot_dir(&c.data_dir, p);
    let (from, refs) = (staged.path.clone(), staged.info.blob_refs.clone());
    let kept = tokio::task::spawn_blocking(move || keep_local(&dir, &from, s, &refs)).await;
    match kept {
        Ok(Ok(())) => {}
        Ok(Err(e)) => warn!(partition = p.0, error = %e,
            "installed snapshot not kept as a local snapshot"),
        Err(e) => warn!(partition = p.0, error = %e, "keep-snapshot task failed"),
    }

    // 7. reopen on the new state (the closure runs the x_* rebuild hook).
    guard
        .reopen()
        .await
        .map_err(|e| InstallError::Reopen(e.to_string()))?;
    info!(
        partition = p.0,
        applied = %s,
        latest_epoch = staged.header.latest_epoch,
        "snapshot installed; fetching from the snapshot offset"
    );
    Ok(InstallOutcome::Installed {
        applied: s,
        latest_epoch: Epoch(staged.header.latest_epoch),
        logical_digest: staged.info.logical_digest,
        bytes: staged.info.bytes,
    })
}

/// What the quiesced partition exposes to steps 4-5 (cloned out of the guard: the guard
/// itself is not `Sync`).
struct Target {
    log: Arc<PartitionLogHandle>,
    state_root: PathBuf,
    store_cfg: ostro_partition::StoreConfig,
}

/// Steps 4-5: reset the log to `S`, then install the state.
async fn reset_and_install(
    guard: &Target,
    c: &Configured,
    p: PartitionId,
    staged: &Staged,
) -> Result<(), InstallError> {
    let s = Offset(staged.header.applied_offset);
    guard
        .log
        .reset_to(s, staged.history.clone())
        .await
        .map_err(|e| InstallError::Log(e.to_string()))?;
    install_fail_point!("install::after_log_reset");

    // Phase 8's install_snapshot has no pre-rename hook callable from here (its own
    // failpoints are `snapshot::install_*`): the point is immediately before the call.
    install_fail_point!("install::before_rename");
    let (root, cfg, count, path) = (
        guard.state_root.clone(),
        guard.store_cfg.clone(),
        c.count,
        staged.path.clone(),
    );
    let info = tokio::task::spawn_blocking(move || -> Result<SnapshotInfo, SnapshotError> {
        let f = BufReader::with_capacity(1 << 20, File::open(&path)?);
        install_snapshot(&root, p, count, &cfg, f)
    })
    .await
    .map_err(|e| InstallError::Aborted(e.to_string()))?
    .map_err(|e| InstallError::State(e.to_string()))?;
    if info.logical_digest != staged.info.logical_digest {
        return Err(InstallError::State(
            "installed digest differs from the verified one".into(),
        ));
    }
    Ok(())
}

/// Failpoint `install::after_rename` (the new state is in place, log anchored at `S`).
fn after_rename() -> Result<(), InstallError> {
    install_fail_point!("install::after_rename");
    Ok(())
}

async fn reopen_after_failure(guard: InstallGuard, p: PartitionId) {
    if let Err(e) = guard.reopen().await {
        warn!(partition = p.0, error = %e, "reopen after a failed install failed");
    }
}

/// After an in-process failure of steps 4-5: finish or roll back an interrupted Phase 8
/// install (`recover_install`: the old state directory may be renamed aside), then return
/// `(applied, epoch history)` of the live state, `(0, [])` without one. The log is anchored
/// there, which is exactly what a crash at that point reopens into.
fn live_position(
    root: &Path,
    p: PartitionId,
    count: PartitionCount,
    cfg: &ostro_partition::StoreConfig,
) -> Result<(Offset, EpochHistory), InstallError> {
    recover_install(root, p).map_err(|e| InstallError::State(e.to_string()))?;
    if !PartitionStore::exists(root, p) {
        return Ok((Offset::ZERO, EpochHistory::default()));
    }
    let (store, meta) = PartitionStore::open(root, p, count, cfg)
        .map_err(|e| InstallError::State(format!("read the live state: {e}")))?;
    drop(store);
    Ok((meta.applied, history_of(&meta.cursor)))
}

fn history_of(cursor: &CursorMeta) -> EpochHistory {
    EpochHistory(
        cursor
            .epoch_history
            .iter()
            .map(|&(e, o)| (Epoch(e), Offset(o)))
            .collect(),
    )
}

/// Step 6 (blocking): `from` becomes `dir/<s>.osnap` with its `.blobrefs` sidecar.
fn keep_local(dir: &Path, from: &Path, s: Offset, refs: &[[u8; 32]]) -> io::Result<()> {
    let fin = dir.join(snapshot_file_name(s));
    if fin.exists() {
        std::fs::remove_file(from)?;
        return fsync_dir(dir);
    }
    let side = fin.with_extension(BLOBREFS_EXT);
    let side_tmp = dir.join(format!("{TMP_PREFIX}{:020}.{BLOBREFS_EXT}", s.0));
    {
        let n = u32::try_from(refs.len()).map_err(|_| io::Error::other("too many blob refs"))?;
        let mut f = io::BufWriter::new(File::create(&side_tmp)?);
        f.write_all(&n.to_le_bytes())?;
        for r in refs {
            f.write_all(r)?;
        }
        f.into_inner().map_err(|e| e.into_error())?.sync_all()?;
    }
    std::fs::rename(&side_tmp, &side)?;
    std::fs::rename(from, &fin)?;
    fsync_dir(dir)
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Remove every `.incoming-*` file of `dir` (earlier, abandoned attempts).
fn remove_stale_incoming(dir: &Path) -> io::Result<usize> {
    let mut n = 0;
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        if e.file_name().to_string_lossy().starts_with(INCOMING_PREFIX) {
            std::fs::remove_file(e.path())?;
            n += 1;
        }
    }
    if n > 0 {
        fsync_dir(dir)?;
    }
    Ok(n)
}

/// The receiving side of one snapshot: the staged file and what was checked so far.
struct Receiver {
    dir: PathBuf,
    p: PartitionId,
    count: PartitionCount,
    file: Option<(File, PathBuf)>,
    header: Option<SnapshotHeader>,
    next: u64,
    done: bool,
    sha: Sha256,
    bytes: u64,
}

impl Receiver {
    fn accept(&mut self, index: u64, bytes: &[u8], last: bool) -> Result<(), InstallError> {
        if self.done {
            return Err(corrupt(format!("frame {index} after the trailer")));
        }
        if index != self.next {
            return Err(corrupt(format!(
                "frame {index} out of order (expected {})",
                self.next
            )));
        }
        if index == 0 {
            let hb: &[u8; HEADER_LEN] = bytes
                .try_into()
                .map_err(|_| corrupt(format!("header frame of {} bytes", bytes.len())))?;
            let h = SnapshotHeader::decode(hb).map_err(corrupt)?;
            if h.partition != self.p.0 || h.partition_count != self.count.get() {
                return Err(InstallError::Mismatch(format!(
                    "snapshot of partition {}/{}, this is partition {}/{}",
                    h.partition,
                    h.partition_count,
                    self.p.0,
                    self.count.get()
                )));
            }
            if last {
                return Err(corrupt("header frame marked last"));
            }
            let path = self
                .dir
                .join(format!("{INCOMING_PREFIX}{:020}", h.applied_offset));
            let f = File::create(&path)?;
            self.file = Some((f, path));
            self.header = Some(h);
        } else {
            if !frame_crc_ok(bytes) {
                return Err(corrupt(format!("CRC mismatch in frame {index}")));
            }
            let is_trailer = bytes[4] == kind::TRAILER;
            if is_trailer != last {
                return Err(corrupt(format!(
                    "frame {index}: trailer {is_trailer}, last {last}"
                )));
            }
            self.done = last;
        }
        let (f, _) = self
            .file
            .as_mut()
            .ok_or_else(|| corrupt("frames before the header"))?;
        f.write_all(bytes)?;
        self.sha.update(bytes);
        self.bytes += bytes.len() as u64;
        self.next += 1;
        Ok(())
    }
}

/// Steps 1-2: receive (with resumes) and verify.
async fn stage(
    c: &Configured,
    cfg: InstallConfig,
    p: PartitionId,
    leader: u64,
) -> Result<Staged, InstallError> {
    let dir = snapshot_dir(&c.data_dir, p);
    std::fs::create_dir_all(&dir)?;
    let stale = remove_stale_incoming(&dir)?;
    if stale > 0 {
        info!(
            partition = p.0,
            files = stale,
            "removed stale incoming snapshot files"
        );
    }
    let mut rx = Receiver {
        dir: dir.clone(),
        p,
        count: c.count,
        file: None,
        header: None,
        next: 0,
        done: false,
        sha: Sha256::new(),
        bytes: 0,
    };
    let res = receive(c, cfg, p, leader, &mut rx).await;
    let path = rx.file.as_ref().map(|(_, path)| path.clone());
    let res = match res {
        Ok(()) => finish(rx).await,
        Err(e) => Err(e),
    };
    if res.is_err()
        && let Some(path) = path
    {
        let _ = std::fs::remove_file(path);
    }
    res
}

async fn receive(
    c: &Configured,
    cfg: InstallConfig,
    p: PartitionId,
    leader: u64,
    rx: &mut Receiver,
) -> Result<(), InstallError> {
    let stream_err = |reason: String| InstallError::Stream { leader, reason };
    let mut resumes = 0u32;
    loop {
        let channel = c
            .transport
            .channel(leader)
            .map_err(|e| stream_err(e.to_string()))?;
        let mut client = ReplicationClient::new(channel)
            .max_decoding_message_size(SNAPSHOT_MESSAGE_LIMIT)
            .max_encoding_message_size(SNAPSHOT_MESSAGE_LIMIT);
        let req = FetchSnapshotRequest {
            partition: u32::from(p.0),
            replica_id: c.node_id,
            resume_frame: rx.next,
        };
        let broken = match tokio::time::timeout(cfg.frame_timeout, client.fetch_snapshot(req)).await
        {
            Err(_) => "timed out opening the stream".to_string(),
            Ok(Err(s)) if rx.next > 0 && s.code() == tonic::Code::FailedPrecondition => {
                // The leader lost the pin: restart from scratch in the next attempt.
                return Err(stream_err(format!("resume refused: {}", s.message())));
            }
            Ok(Err(s)) => {
                if rx.next == 0 {
                    return Err(stream_err(format!("{}: {}", s.code(), s.message())));
                }
                format!("{}: {}", s.code(), s.message())
            }
            Ok(Ok(resp)) => {
                let mut stream = resp.into_inner();
                loop {
                    match tokio::time::timeout(cfg.frame_timeout, stream.next()).await {
                        Err(_) => break "timed out waiting for a frame".to_string(),
                        Ok(None) => {
                            break "stream ended before the trailer".to_string();
                        }
                        Ok(Some(Err(s))) => {
                            if matches!(
                                s.code(),
                                tonic::Code::DataLoss
                                    | tonic::Code::PermissionDenied
                                    | tonic::Code::NotFound
                            ) {
                                return Err(stream_err(format!("{}: {}", s.code(), s.message())));
                            }
                            break format!("{}: {}", s.code(), s.message());
                        }
                        Ok(Some(Ok(f))) => {
                            rx.accept(f.index, &f.bytes, f.last)?;
                            if rx.done {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        };
        if rx.next == 0 || resumes >= cfg.max_resumes {
            return Err(stream_err(broken));
        }
        resumes += 1;
        info!(partition = p.0, leader, resume_frame = rx.next, error = %broken,
            "snapshot stream broken; resuming");
    }
}

/// Sha256-hashing reader.
struct HashRead<R> {
    r: R,
    h: Sha256,
}

impl<R: Read> Read for HashRead<R> {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = self.r.read(b)?;
        self.h.update(&b[..n]);
        Ok(n)
    }
}

/// Step 2 (after the trailer arrived): fsync, re-read + verify, sha256, cursor history.
async fn finish(rx: Receiver) -> Result<Staged, InstallError> {
    let Receiver {
        dir,
        p,
        count,
        file,
        header,
        sha,
        bytes,
        ..
    } = rx;
    let (f, path) = file.ok_or_else(|| corrupt("no frames"))?;
    let header = header.ok_or_else(|| corrupt("no header"))?;
    let received: [u8; 32] = sha.finalize().into();
    tokio::task::spawn_blocking(move || -> Result<Staged, InstallError> {
        f.sync_all()?;
        drop(f);
        fsync_dir(&dir)?;
        let mut hr = HashRead {
            r: BufReader::with_capacity(1 << 20, File::open(&path)?),
            h: Sha256::new(),
        };
        let info = verify_snapshot(&mut hr).map_err(corrupt)?;
        // Drain anything after the trailer (verify_snapshot already refuses it).
        io::copy(&mut hr, &mut io::sink())?;
        let reread: [u8; 32] = hr.h.finalize().into();
        if reread != received {
            return Err(corrupt(
                "sha256 of the staged file differs from the received bytes",
            ));
        }
        if info.bytes != bytes {
            return Err(corrupt(format!(
                "snapshot size {} != received {bytes}",
                info.bytes
            )));
        }
        if info.partition != p.0 || info.partition_count != count.get() {
            return Err(InstallError::Mismatch(format!(
                "snapshot of partition {}/{}",
                info.partition, info.partition_count
            )));
        }
        let cursor = read_cursor(&path)?;
        if cursor.epoch != header.latest_epoch {
            return Err(corrupt(format!(
                "cursor epoch {} != header latest_epoch {}",
                cursor.epoch, header.latest_epoch
            )));
        }
        Ok(Staged {
            path,
            header,
            info,
            history: history_of(&cursor),
        })
    })
    .await
    .map_err(|e| InstallError::Aborted(e.to_string()))?
}

/// The cursor row of the snapshot's `meta` table (`CursorMeta`, with `epoch_history`).
fn read_cursor(path: &Path) -> Result<CursorMeta, InstallError> {
    let mut r =
        OsnpReader::new(BufReader::with_capacity(1 << 20, File::open(path)?)).map_err(corrupt)?;
    let mut in_meta = false;
    while let Some(f) = r.next_frame().map_err(corrupt)? {
        match f {
            Frame::TableBegin(name) => in_meta = name == Table::Meta.name(),
            Frame::TableEnd(_) => {
                if in_meta {
                    break;
                }
            }
            Frame::Rows(rows) if in_meta => {
                if let Some((_, v)) = rows.iter().find(|(k, _)| k.as_slice() == K_CURSOR) {
                    return values::decode::<CursorMeta>(v).map_err(corrupt);
                }
            }
            _ => {}
        }
    }
    Err(corrupt("no cursor row in the snapshot's meta table"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn default_ctx_is_not_configured() {
        let e = install_from_leader(&InstallCtx::default(), PartitionId(0), 1)
            .await
            .unwrap_err();
        assert!(matches!(e, InstallError::NotConfigured), "{e}");
        assert!(format!("{:?}", InstallCtx::default()).contains("configured"));
    }

    #[test]
    fn stale_incoming_files_are_removed() {
        let d = tempfile::tempdir().unwrap();
        std::fs::write(d.path().join(".incoming-00000000000000000004"), b"x").unwrap();
        std::fs::write(d.path().join("00000000000000000004.osnap"), b"x").unwrap();
        assert_eq!(remove_stale_incoming(d.path()).unwrap(), 1);
        assert!(d.path().join("00000000000000000004.osnap").exists());
        assert_eq!(remove_stale_incoming(d.path()).unwrap(), 0);
    }
}
