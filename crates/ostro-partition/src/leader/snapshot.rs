//! Local snapshot service (P8-D1/P8-D2, plan 08-09).
//!
//! One [`SnapshotService`] per partition writes OSNP v1 snapshots
//! (`<data>/snapshots/pNNNN/<applied 20d>.osnap`, plus a `.blobrefs` sidecar), keeps
//! [`SnapshotConfig::keep`] of them and publishes the **local trim floor**: the
//! `applied_offset` of the oldest retained snapshot. Retention
//! ([`retention`](super::retention)) never trims at or above that floor, so
//! `log_start <= applied_offset(oldest retained snapshot)` holds at all times (SC4), and every
//! retained snapshot plus the log rebuilds the state.
//!
//! Writing one snapshot:
//! 1. export on a blocking thread, one partition at a time node-wide (a semaphore: a full scan
//!    evicts the block cache, research Pitfall 6), into `snapshots/pNNNN/.tmp-<off>` while
//!    hashing it (sha256 of the whole file, for the remote key of 08-13);
//! 2. `sync_all`; write the `.blobrefs` sidecar (tmp + fsync + rename);
//! 3. fail point `snapshot::before_rename`; rename to `<applied 20d>.osnap`;
//! 4. fail point `snapshot::before_dir_fsync`; fsync the directory;
//! 5. **only then** publish the new local floor (the oldest of the newest `keep` snapshots);
//! 6. delete the snapshots beyond `keep` (oldest first) and fsync the directory;
//! 7. broadcast [`SnapshotTaken`] and record `ostro_snapshot_duration_seconds` /
//!    `ostro_snapshot_bytes{partition}`.
//!
//! A crash anywhere leaves either a `.tmp-*` file (removed by the next startup scan) or a
//! complete renamed snapshot; the floor never moves past what is durable.
//!
//! Cadence (the ticker, spawned by [`spawn`] when [`SnapshotConfig::enabled`]): every
//! `min(interval, 10 s)` the service snapshots when something was applied since the last
//! snapshot, at least `min_interval` after it, and either `interval` elapsed or the envelope
//! bytes appended since then reach `bytes_threshold`. All times come from the injected
//! [`Clock`](crate::clock::Clock).
//!
//! `.blobrefs` sidecar layout: the `BlobRefs` frame payload (`u32 LE n`, then `n` sorted,
//! deduplicated 32-byte sha256 digests).

use std::io::{self, Read as _, Seek as _, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ostro_log::ReplicatedLog;
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_types::{Offset, PartitionCount, PartitionId};
use sha2::{Digest as _, Sha256};
use tokio::sync::{Mutex, Semaphore, broadcast, watch};
use tracing::{info, warn};

use super::LeaderCtx;
use crate::clock::Clock;
use crate::store::PartitionStore;
use crate::store::snapshot::{
    HEADER_LEN, SnapshotError, SnapshotHeader, SnapshotInfo, TRAILER_FRAME_LEN, kind,
};

/// Local snapshot cadence and retention.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotConfig {
    /// Snapshots (and the trim they allow) are taken at all.
    pub enabled: bool,
    /// Take a snapshot at least this often while the partition changes (default 1 h).
    pub interval: Duration,
    /// Never snapshot more often than this (default 60 s).
    pub min_interval: Duration,
    /// Take a snapshot early once this many envelope bytes were appended since the last one
    /// (default 256 MiB).
    pub bytes_threshold: u64,
    /// Local snapshots retained per partition (default 2).
    pub keep: usize,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(3600),
            min_interval: Duration::from_secs(60),
            bytes_threshold: 256 * 1024 * 1024,
            keep: 2,
        }
    }
}

/// Longest ticker period.
const MAX_TICK: Duration = Duration::from_secs(10);
/// Shortest ticker period (tiny test intervals).
const MIN_TICK: Duration = Duration::from_millis(10);
/// Snapshot file extension.
pub const SNAPSHOT_EXT: &str = "osnap";
/// Blob-ref sidecar extension.
pub const BLOBREFS_EXT: &str = "blobrefs";
/// Prefix of in-progress files (removed by the startup scan).
pub const TMP_PREFIX: &str = ".tmp-";
/// Broadcast capacity of [`SnapshotTaken`] events.
pub const SNAPSHOT_EVENTS_CAPACITY: usize = 256;

/// `<data>/snapshots/pNNNN`.
pub fn snapshot_dir(data_dir: &Path, p: PartitionId) -> PathBuf {
    data_dir.join("snapshots").join(format!("p{:04}", p.0))
}

/// `<applied 20d>.osnap`.
pub fn snapshot_file_name(applied: Offset) -> String {
    format!("{:020}.{SNAPSHOT_EXT}", applied.0)
}

/// One snapshot file on disk (by name; see [`quick_check`] / `verify_snapshot` for content).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalSnapshot {
    pub applied_offset: Offset,
    pub path: PathBuf,
}

/// A snapshot was written durably and the floor was published.
#[derive(Clone, Debug)]
pub struct SnapshotTaken {
    pub partition: PartitionId,
    pub applied_offset: Offset,
    /// The final `.osnap` path (may be deleted later by retention, `keep`).
    pub path: PathBuf,
    /// sha256 of the whole file.
    pub sha256: [u8; 32],
    pub info: SnapshotInfo,
}

fn parse_name(name: &str) -> Option<Offset> {
    let stem = name.strip_suffix(&format!(".{SNAPSHOT_EXT}"))?;
    if stem.len() != 20 || !stem.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok().map(Offset)
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

/// Scan `dir` (created if missing): remove `.tmp-*` leftovers and `.blobrefs` sidecars without
/// a snapshot, and return the snapshots sorted by `applied_offset` (ascending).
pub fn scan_dir(dir: &Path) -> io::Result<Vec<LocalSnapshot>> {
    std::fs::create_dir_all(dir)?;
    let mut snaps = Vec::new();
    let mut sidecars = Vec::new();
    let mut removed = false;
    for e in std::fs::read_dir(dir)? {
        let e = e?;
        let name = e.file_name().to_string_lossy().into_owned();
        if name.starts_with(TMP_PREFIX) {
            info!(file = %e.path().display(), "removing an interrupted snapshot file");
            std::fs::remove_file(e.path())?;
            removed = true;
        } else if let Some(off) = parse_name(&name) {
            snaps.push(LocalSnapshot {
                applied_offset: off,
                path: e.path(),
            });
        } else if let Some(stem) = name.strip_suffix(&format!(".{BLOBREFS_EXT}")) {
            sidecars.push((stem.to_string(), e.path()));
        }
    }
    for (stem, path) in sidecars {
        if !dir.join(format!("{stem}.{SNAPSHOT_EXT}")).exists() {
            std::fs::remove_file(path)?;
            removed = true;
        }
    }
    if removed {
        fsync_dir(dir)?;
    }
    snaps.sort_by_key(|s| s.applied_offset);
    Ok(snaps)
}

/// Cheap validation: the header decodes (CRC, magic, version), names partition `p` of `count`
/// and the file's `applied_offset`, and the file ends with a CRC-valid trailer frame. The body
/// is **not** read (install verifies everything).
pub fn quick_check(
    s: &LocalSnapshot,
    p: PartitionId,
    count: PartitionCount,
) -> Result<SnapshotHeader, SnapshotError> {
    let mut f = std::fs::File::open(&s.path)?;
    let len = f.metadata()?.len();
    let min = (HEADER_LEN + TRAILER_FRAME_LEN) as u64;
    if len < min {
        return Err(SnapshotError::Truncated);
    }
    let mut hb = [0u8; HEADER_LEN];
    f.read_exact(&mut hb)?;
    let h = SnapshotHeader::decode(&hb)?;
    if h.partition != p.0 || h.partition_count != count.get() {
        return Err(SnapshotError::Mismatch(format!(
            "snapshot of partition {} of {}",
            h.partition, h.partition_count
        )));
    }
    if h.applied_offset != s.applied_offset.0 {
        return Err(SnapshotError::Mismatch(format!(
            "header applied_offset {} != file name {}",
            h.applied_offset, s.applied_offset.0
        )));
    }
    let mut tb = vec![0u8; TRAILER_FRAME_LEN];
    f.seek(SeekFrom::Start(len - TRAILER_FRAME_LEN as u64))?;
    f.read_exact(&mut tb)?;
    let body = &tb[4..TRAILER_FRAME_LEN - 4];
    let crc = u32::from_le_bytes(
        tb[TRAILER_FRAME_LEN - 4..]
            .try_into()
            .map_err(|_| SnapshotError::Truncated)?,
    );
    if body[0] != kind::TRAILER || crc32c::crc32c(body) != crc {
        return Err(SnapshotError::Malformed("bad trailer frame".into()));
    }
    Ok(h)
}

/// Node-wide shared parts: the one-export-at-a-time semaphore and the event channel.
#[derive(Debug)]
pub struct SnapshotShared {
    permits: Semaphore,
    events: broadcast::Sender<SnapshotTaken>,
}

impl SnapshotShared {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            permits: Semaphore::new(1),
            events: broadcast::channel(SNAPSHOT_EVENTS_CAPACITY).0,
        })
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SnapshotTaken> {
        self.events.subscribe()
    }
}

#[derive(Debug)]
struct SvcState {
    /// Snapshots on disk, ascending.
    retained: Vec<LocalSnapshot>,
    /// `applied_offset` of the newest snapshot (`0` = none).
    last_offset: Offset,
    /// Clock time of the newest snapshot (or of the service start).
    last_at_ms: u64,
}

/// The snapshot service of one partition.
pub struct SnapshotService {
    partition: PartitionId,
    data_dir: PathBuf,
    dir: PathBuf,
    store: Arc<PartitionStore>,
    cfg: SnapshotConfig,
    clock: Arc<dyn Clock>,
    shared: Arc<SnapshotShared>,
    local_floor: watch::Sender<Offset>,
    state: Mutex<SvcState>,
}

impl std::fmt::Debug for SnapshotService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotService")
            .field("partition", &self.partition)
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

impl SnapshotService {
    /// The service of partition `p` over the snapshots `retained` found by [`scan_dir`].
    ///
    /// The local floor starts at the `applied_offset` of the oldest snapshot on disk (the most
    /// conservative choice), or `0` (no trimming, the pre-Phase 8 behaviour) when there is none
    /// or `cfg.enabled` is false. The newest snapshot's header gives the cadence base time.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        partition: PartitionId,
        count: PartitionCount,
        data_dir: &Path,
        store: Arc<PartitionStore>,
        cfg: SnapshotConfig,
        clock: Arc<dyn Clock>,
        shared: Arc<SnapshotShared>,
        retained: Vec<LocalSnapshot>,
    ) -> Arc<Self> {
        let floor = if cfg.enabled {
            retained.first().map_or(Offset(0), |s| s.applied_offset)
        } else {
            Offset(0)
        };
        let now = clock.now_ms();
        let (last_offset, last_at_ms) = match retained.last() {
            Some(s) => match quick_check(s, partition, count) {
                Ok(h) => (s.applied_offset, h.created_at_ms.min(now)),
                Err(e) => {
                    warn!(partition = partition.0, file = %s.path.display(), error = %e,
                        "newest local snapshot fails the quick check; install will fall back");
                    (s.applied_offset, now)
                }
            },
            None => (Offset(0), now),
        };
        info!(
            partition = partition.0,
            snapshots = retained.len(),
            local_floor = %floor,
            "snapshot service ready"
        );
        Arc::new(Self {
            partition,
            data_dir: data_dir.to_path_buf(),
            dir: snapshot_dir(data_dir, partition),
            store,
            cfg,
            clock,
            shared,
            local_floor: watch::channel(floor).0,
            state: Mutex::new(SvcState {
                retained,
                last_offset,
                last_at_ms,
            }),
        })
    }

    pub fn config(&self) -> &SnapshotConfig {
        &self.cfg
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The local trim floor (`applied_offset` of the oldest retained snapshot).
    pub fn local_floor(&self) -> watch::Receiver<Offset> {
        self.local_floor.subscribe()
    }

    /// Snapshots on disk, ascending.
    pub async fn retained(&self) -> Vec<LocalSnapshot> {
        self.state.lock().await.retained.clone()
    }

    /// Take a snapshot now. `Ok(None)` when nothing was applied since the newest snapshot.
    pub async fn take(&self) -> Result<Option<SnapshotTaken>, SnapshotError> {
        let mut st = self.state.lock().await;
        let _permit = self
            .shared
            .permits
            .acquire()
            .await
            .map_err(|_| SnapshotError::Malformed("snapshot semaphore closed".into()))?;
        #[allow(clippy::disallowed_methods, clippy::disallowed_types)]
        let started = std::time::Instant::now(); // clock-exempt: metric duration only
        let created_at_ms = self.clock.now_ms();
        let store = Arc::clone(&self.store);
        let dir = self.dir.clone();
        let newest = st.last_offset;
        let have_newest = !st.retained.is_empty();
        let written = tokio::task::spawn_blocking(move || {
            write_snapshot(&store, &dir, created_at_ms, newest, have_newest)
        })
        .await
        .map_err(|e| SnapshotError::Io(io::Error::other(format!("snapshot task: {e}"))))??;
        let Some((info, sha256, path)) = written else {
            return Ok(None);
        };
        let applied = Offset(info.applied_offset);

        // 5. publish the floor: the oldest of the newest `keep` snapshots.
        st.retained.retain(|s| s.applied_offset != applied);
        st.retained.push(LocalSnapshot {
            applied_offset: applied,
            path: path.clone(),
        });
        st.retained.sort_by_key(|s| s.applied_offset);
        let keep = self.cfg.keep.max(1);
        let cut = st.retained.len().saturating_sub(keep);
        let floor = st.retained[cut].applied_offset;
        self.local_floor.send_if_modified(|f| {
            if floor > *f {
                *f = floor;
                true
            } else {
                false
            }
        });
        // 6. only now delete the snapshots beyond `keep`.
        let old: Vec<LocalSnapshot> = st.retained.drain(..cut).collect();
        if !old.is_empty() {
            let dir = self.dir.clone();
            let r = tokio::task::spawn_blocking(move || -> io::Result<()> {
                for s in &old {
                    remove_if_exists(&s.path)?;
                    remove_if_exists(&s.path.with_extension(BLOBREFS_EXT))?;
                }
                fsync_dir(&dir)
            })
            .await;
            match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => warn!(partition = self.partition.0, error = %e,
                    "deleting old snapshots failed (the floor stays safe)"),
                Err(e) => warn!(partition = self.partition.0, error = %e,
                    "snapshot delete task failed"),
            }
        }
        st.last_offset = applied;
        st.last_at_ms = created_at_ms;
        drop(st);

        let secs = started.elapsed().as_secs_f64();
        let label = self.partition.0.to_string();
        metrics::histogram!("ostro_snapshot_duration_seconds", "partition" => label.clone())
            .record(secs);
        metrics::gauge!("ostro_snapshot_bytes", "partition" => label).set(info.bytes as f64);
        info!(
            partition = self.partition.0,
            applied = %applied,
            bytes = info.bytes,
            rows = info.total_rows,
            secs,
            local_floor = %floor,
            "snapshot taken"
        );
        let ev = SnapshotTaken {
            partition: self.partition,
            applied_offset: applied,
            path,
            sha256,
            info,
        };
        let _ = self.shared.events.send(ev.clone());
        Ok(Some(ev))
    }

    /// Estimated envelope bytes in `[last_offset, applied)`: the partition's WAL bytes per
    /// offset over `[log_start, LEO)` times the offset distance (segment sizes, no reads).
    fn estimate_bytes(&self, last: Offset, applied: Offset, start: Offset, leo: Offset) -> u64 {
        let span = leo.0.saturating_sub(start.0);
        if span == 0 || applied <= last {
            return 0;
        }
        let dir = segment_dir(&self.data_dir.join("wal"), self.partition);
        let total: u64 = list_segments(&dir, self.partition)
            .map(|v| {
                v.iter()
                    .filter_map(|(_, p)| std::fs::metadata(p).ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0);
        let delta = applied.0 - last.0.max(start.0).min(applied.0);
        u64::try_from(u128::from(total) * u128::from(delta) / u128::from(span)).unwrap_or(u64::MAX)
    }

    /// One cadence decision; takes a snapshot when due.
    async fn tick(&self, ctx: &LeaderCtx) -> Result<(), SnapshotError> {
        let applied = *ctx.handle.machine().applied().borrow();
        let (last, last_at) = {
            let st = self.state.lock().await;
            (st.last_offset, st.last_at_ms)
        };
        if applied <= last {
            return Ok(());
        }
        let now = self.clock.now_ms();
        let since = Duration::from_millis(now.saturating_sub(last_at));
        if since < self.cfg.min_interval {
            return Ok(());
        }
        let log = ctx.handle.log();
        let due = since >= self.cfg.interval
            || self.estimate_bytes(last, applied, log.log_start(), log.log_end())
                >= self.cfg.bytes_threshold;
        if due {
            self.take().await?;
        }
        Ok(())
    }
}

fn remove_if_exists(p: &Path) -> io::Result<()> {
    match std::fs::remove_file(p) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}

/// Writes `w` through and hashes it.
struct HashWriter<W> {
    w: W,
    h: Sha256,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let n = self.w.write(b)?;
        self.h.update(&b[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}

macro_rules! snap_fail_point {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name, |_| Err(SnapshotError::Injected($name)));
    };
}

/// Steps 1-4 (blocking). `None` when the export's `applied_offset` is not newer than the
/// newest snapshot on disk.
fn write_snapshot(
    store: &PartitionStore,
    dir: &Path,
    created_at_ms: u64,
    newest: Offset,
    have_newest: bool,
) -> Result<Option<(SnapshotInfo, [u8; 32], PathBuf)>, SnapshotError> {
    std::fs::create_dir_all(dir)?;
    let hint = store.read_meta().map_or(0, |m| m.applied.0);
    let tmp = dir.join(format!("{TMP_PREFIX}{hint:020}"));
    let file = std::fs::File::create(&tmp)?;
    let mut w = HashWriter {
        w: io::BufWriter::with_capacity(1 << 20, file),
        h: Sha256::new(),
    };
    let r = store.export_snapshot(&mut w, created_at_ms);
    let info = match r {
        Ok(i) => i,
        Err(e) => {
            drop(w);
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    };
    let HashWriter { w, h } = w;
    let file = w
        .into_inner()
        .map_err(|e| SnapshotError::Io(e.into_error()))?;
    let sha256: [u8; 32] = h.finalize().into();
    let applied = Offset(info.applied_offset);
    if have_newest && applied <= newest {
        drop(file);
        std::fs::remove_file(&tmp)?;
        return Ok(None);
    }
    file.sync_all()?;
    drop(file);

    // 2. the blob-ref sidecar (the BlobRefs frame payload).
    let name = snapshot_file_name(applied);
    let fin = dir.join(&name);
    let side = fin.with_extension(BLOBREFS_EXT);
    let side_tmp = dir.join(format!("{TMP_PREFIX}{:020}.{BLOBREFS_EXT}", applied.0));
    {
        let n = u32::try_from(info.blob_refs.len())
            .map_err(|_| SnapshotError::Malformed("too many blob refs".into()))?;
        let mut f = io::BufWriter::new(std::fs::File::create(&side_tmp)?);
        f.write_all(&n.to_le_bytes())?;
        for r in &info.blob_refs {
            f.write_all(r)?;
        }
        let f = f
            .into_inner()
            .map_err(|e| SnapshotError::Io(e.into_error()))?;
        f.sync_all()?;
    }
    std::fs::rename(&side_tmp, &side)?;

    // 3./4. rename, then make it durable.
    snap_fail_point!("snapshot::before_rename");
    std::fs::rename(&tmp, &fin)?;
    snap_fail_point!("snapshot::before_dir_fsync");
    fsync_dir(dir)?;
    Ok(Some((info, sha256, fin)))
}

/// Start the snapshot ticker of `ctx.partition` (only called when `svc.config().enabled`).
/// Runs until `ctx.shutdown` turns true; stops on an I/O error (logged; the floor stays where
/// it was, so trimming stays safe).
pub fn spawn(ctx: LeaderCtx, svc: Arc<SnapshotService>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut shutdown = ctx.shutdown.clone();
        let p = ctx.partition.0;
        let period = svc.cfg.interval.min(MAX_TICK).max(MIN_TICK);
        loop {
            tokio::select! {
                _ = shutdown.wait_for(|v| *v) => break,
                () = tokio::time::sleep(period) => {}
            }
            if *shutdown.borrow() {
                break;
            }
            if let Err(e) = svc.tick(&ctx).await {
                if ctx.handle.machine().check_healthy().is_err() {
                    warn!(partition = p, error = %e, "snapshot failed; partition failing, snapshot service stops");
                    return;
                }
                warn!(partition = p, error = %e, "snapshot failed; retrying next tick");
            }
        }
        info!(partition = p, "snapshot service stopped");
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_round_trip() {
        assert_eq!(snapshot_file_name(Offset(42)), "00000000000000000042.osnap");
        assert_eq!(parse_name("00000000000000000042.osnap"), Some(Offset(42)));
        assert_eq!(parse_name("42.osnap"), None);
        assert_eq!(parse_name("00000000000000000042.blobrefs"), None);
        assert_eq!(parse_name(".tmp-00000000000000000042"), None);
    }
}
