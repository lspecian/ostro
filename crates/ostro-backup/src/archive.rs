//! Backup files (P8-D7, plan 08-18): a backup file is a plain tar of the bottomless LocalFs
//! layout (`manifest/`, `p/<pppp>/seg/`, `p/<pppp>/snap/`, `meta/`, `blobs/`, `LOCK`), so
//! `ostro restore --from f.tar` unpacks it and restores through the same LocalFs code path
//! as `--from file://…`.
//!
//! - [`pack_dir`] / [`unpack`]: tar of a directory tree. Regular files only, relative paths
//!   only (no absolute path, no `..`, no links); deterministic headers (sorted entries,
//!   mode 0644, mtime 0). `.ostro-tmp/` (LocalFs in-flight writes) is never packed.
//! - [`backup_node_to_tar`]: one upload round of a running node into a temp LocalFs store
//!   (fresh fence identity, `publish_floor = false`, `retention = false`), then the tar.
//!   The online backup (`GET /v1/admin/backup`) calls it after forcing a snapshot per
//!   partition.
//! - [`backup_offline`]: the same round over a stopped data directory opened in Inspect mode
//!   (no append, barriers closed; the meta group is started on a temporary copy, so the data
//!   directory is never written).

use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ostro_meta::{MetaHandle, MetaView};
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_partition::leader::snapshot::scan_dir;
use ostro_partition::{Clock, Node, NodeConfig, StartMode};
use ostro_types::{PartitionCount, PartitionId};
use sha2::{Digest as _, Sha256};

use crate::chunk::MAX_CHUNK_BYTES;
use crate::fence::{self, Ident, put_immutable};
use crate::manifest::Manifest;
use crate::uploader::{RoundReport, Uploader, UploaderConfig, snap_key};

/// LocalFs' reserved in-flight directory.
const LOCALFS_TMP: &str = ".ostro-tmp";

/// Backup failures.
#[derive(Debug, thiserror::Error)]
pub enum BackupError {
    /// Filesystem / tar failure.
    #[error("backup io: {0}")]
    Io(#[from] io::Error),
    /// The upload round failed or wrote no manifest.
    #[error("backup round: {0}")]
    Round(String),
    /// The data directory could not be opened.
    #[error("backup: {0}")]
    Open(String),
}

/// What a backup wrote.
#[derive(Clone, Debug)]
pub struct BackupReport {
    /// The manifest inside the tar.
    pub manifest: Manifest,
    /// Tar entries.
    pub entries: u64,
    /// Tar bytes.
    pub bytes: u64,
    /// The upload round.
    pub round: RoundReport,
}

/// Every regular file under `root`, relative, sorted (`.ostro-tmp` skipped).
fn walk(root: &Path) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![PathBuf::new()];
    while let Some(rel) = stack.pop() {
        for e in fs::read_dir(root.join(&rel))? {
            let e = e?;
            let name = rel.join(e.file_name());
            if rel.as_os_str().is_empty() && e.file_name() == LOCALFS_TMP {
                continue;
            }
            let t = e.file_type()?;
            if t.is_dir() {
                stack.push(name);
            } else if t.is_file() {
                out.push(name);
            }
        }
    }
    out.sort();
    Ok(out)
}

/// Write a tar of every regular file under `dir` to `w`. Returns the number of entries.
pub fn pack_dir(dir: &Path, w: impl Write) -> io::Result<u64> {
    let mut b = tar::Builder::new(w);
    let files = walk(dir)?;
    for rel in &files {
        let mut f = File::open(dir.join(rel))?;
        let len = f.metadata()?.len();
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_size(len);
        h.set_mode(0o644);
        h.set_mtime(0);
        b.append_data(&mut h, rel, &mut f)?;
    }
    b.into_inner()?.flush()?;
    Ok(files.len() as u64)
}

fn invalid(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

/// Unpack a tar written by [`pack_dir`] into `dir` (created if missing). Refuses absolute
/// paths, `..` / `.` components and anything but regular files (directory entries are
/// accepted and created). Returns the number of files.
pub fn unpack(r: impl Read, dir: &Path) -> io::Result<u64> {
    fs::create_dir_all(dir)?;
    let mut a = tar::Archive::new(r);
    let mut n = 0u64;
    for e in a.entries()? {
        let mut e = e?;
        let path = e.path()?.into_owned();
        if path.as_os_str().is_empty()
            || !path.components().all(|c| matches!(c, Component::Normal(_)))
        {
            return Err(invalid(format!(
                "backup file entry {}: absolute paths and '..' components are not allowed",
                path.display()
            )));
        }
        match e.header().entry_type() {
            tar::EntryType::Regular | tar::EntryType::GNUSparse => {}
            tar::EntryType::Directory => {
                fs::create_dir_all(dir.join(&path))?;
                continue;
            }
            other => {
                return Err(invalid(format!(
                    "backup file entry {}: {other:?} entries are not allowed (regular files only)",
                    path.display()
                )));
            }
        }
        let dest = dir.join(&path);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&dest)?;
        io::copy(&mut e, &mut f)?;
        n += 1;
    }
    Ok(n)
}

/// A unique staging directory under the system temp dir (removed on drop).
pub(crate) struct Staging(pub(crate) PathBuf);

impl Staging {
    pub(crate) fn new(tag: &str) -> io::Result<Self> {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut rnd = [0u8; 8];
        rnd.copy_from_slice(&ostro_partition::node::new_incarnation()[..8]);
        let dir = std::env::temp_dir().join(format!(
            "ostro-{tag}-{}-{n}-{}",
            std::process::id(),
            u64::from_le_bytes(rnd)
        ));
        fs::create_dir_all(&dir)?;
        Ok(Self(dir))
    }
}

impl Drop for Staging {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Uploader settings of a one-off backup round.
fn one_off_cfg(cluster_id: &str, node_id: u64) -> UploaderConfig {
    UploaderConfig {
        seal_interval: Duration::from_secs(1),
        remote_snapshots_keep: 1,
        cluster_id: cluster_id.to_owned(),
        node_id,
        max_chunk_bytes: MAX_CHUNK_BYTES,
        publish_floor: false,
        retention: false,
    }
}

/// One upload round of `node` into a temp LocalFs store, then a tar of it at `out`
/// (written to `out` + `.tmp`, fsynced, renamed). `inspect`: the node was opened in Inspect
/// mode (its newest local snapshots are staged first, because Inspect runs no snapshot
/// service). Fails when the round writes no manifest.
pub async fn backup_node_to_tar(
    node: Arc<Node>,
    meta: Option<MetaView>,
    clock: Arc<dyn Clock>,
    node_id: u64,
    inspect: bool,
    out: &Path,
) -> Result<BackupReport, BackupError> {
    let staging = Staging::new("backup")?;
    let store: Arc<dyn ObjectStore> = Arc::new(
        LocalFs::new(&staging.0).map_err(|e| BackupError::Round(format!("staging store: {e}")))?,
    );
    let cluster_id = meta
        .as_ref()
        .and_then(|v| v.read(|s| s.cluster().map(|c| c.value.cluster_id.clone())))
        .unwrap_or_else(|| "ostro".to_owned());
    if inspect {
        stage_local_snapshots(&node, &store).await?;
    }
    let mut up = Uploader::new(
        Arc::clone(&node),
        meta,
        clock,
        one_off_cfg(&cluster_id, node_id),
    );
    if inspect {
        up = up.for_inspect_node();
    }
    // A fresh fence identity (the node's incarnation) owns the staging prefix.
    let ident = Ident::new(cluster_id.clone(), node_id, node.incarnation());
    fence::acquire(&store, &ident, false)
        .await
        .map_err(|e| BackupError::Round(e.to_string()))?;
    let round = up
        .round(&store)
        .await
        .map_err(|e| BackupError::Round(e.to_string()))?;
    let Some(manifest) = round.manifest.clone() else {
        return Err(BackupError::Round(format!(
            "the upload round wrote no manifest (barrier closed: {}, partitions without a \
             usable snapshot: {:?})",
            round.barrier_closed, round.skipped
        )));
    };
    let dir = staging.0.clone();
    let out = out.to_path_buf();
    let (entries, bytes) = tokio::task::spawn_blocking(move || -> io::Result<(u64, u64)> {
        let tmp = tmp_path(&out);
        let f = File::create(&tmp)?;
        let mut w = io::BufWriter::new(f);
        let entries = pack_dir(&dir, &mut w)?;
        let f = w.into_inner().map_err(io::IntoInnerError::into_error)?;
        f.sync_all()?;
        let bytes = f.metadata()?.len();
        drop(f);
        fs::rename(&tmp, &out)?;
        if let Some(parent) = out.parent().filter(|p| !p.as_os_str().is_empty()) {
            File::open(parent)?.sync_all()?;
        }
        Ok((entries, bytes))
    })
    .await
    .map_err(|e| BackupError::Io(io::Error::other(e)))??;
    drop(staging);
    Ok(BackupReport {
        manifest,
        entries,
        bytes,
        round,
    })
}

fn tmp_path(out: &Path) -> PathBuf {
    let mut name = out.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    out.with_file_name(name)
}

/// Inspect mode runs no snapshot service, so `Node::local_snapshots` is empty: put each
/// partition's newest verified local snapshot into the store under its `snap/` key, where
/// the uploader's session finds it (chain start rule, B4).
async fn stage_local_snapshots(
    node: &Arc<Node>,
    store: &Arc<dyn ObjectStore>,
) -> Result<(), BackupError> {
    for p in 0..node.log_cut().len() {
        let pid = PartitionId(p as u16);
        if node.handle(pid).is_none() {
            continue;
        }
        let dir = node.snapshot_dir(pid);
        let found = tokio::task::spawn_blocking(move || -> io::Result<Option<Vec<u8>>> {
            let mut snaps = if dir.exists() {
                scan_dir(&dir)?
            } else {
                Vec::new()
            };
            snaps.sort_by_key(|s| s.applied_offset);
            for s in snaps.iter().rev() {
                let bytes = fs::read(&s.path)?;
                if ostro_partition::store::snapshot::verify_snapshot(io::Cursor::new(&bytes))
                    .is_ok()
                {
                    return Ok(Some(bytes));
                }
            }
            Ok(None)
        })
        .await
        .map_err(|e| BackupError::Io(io::Error::other(e)))??;
        let Some(bytes) = found else { continue };
        let info = ostro_partition::store::snapshot::verify_snapshot(io::Cursor::new(&bytes))
            .map_err(|e| BackupError::Round(format!("partition {p}: local snapshot: {e}")))?;
        let sha: [u8; 32] = Sha256::digest(&bytes).into();
        put_immutable(
            store,
            &snap_key(p as u16, info.applied_offset, &sha),
            bytes,
            sha,
        )
        .await
        .map_err(|e| BackupError::Round(e.to_string()))?;
    }
    Ok(())
}

/// The partition count recorded in `<data_dir>/node.toml`.
pub fn node_file_partitions(data_dir: &Path) -> Result<u16, BackupError> {
    let path = data_dir.join("node.toml");
    let s = fs::read_to_string(&path)
        .map_err(|e| BackupError::Open(format!("{}: {e}", path.display())))?;
    s.lines()
        .filter_map(|l| l.split_once('='))
        .find(|(k, _)| k.trim() == "partitions")
        .and_then(|(_, v)| v.trim().parse().ok())
        .ok_or_else(|| BackupError::Open(format!("{}: no partitions key", path.display())))
}

/// Copy a directory tree (regular files only).
fn copy_tree(from: &Path, to: &Path) -> io::Result<()> {
    for rel in walk(from)? {
        let dest = to.join(&rel);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(from.join(&rel), dest)?;
    }
    Ok(())
}

/// Offline backup of the stopped data directory `data_dir` to the tar file `out`: the node
/// is opened in Inspect mode (catch-up only: no append, no services), the metadata group is
/// started on a temporary copy of `meta/` (as voter `node_id`, the server's `[node]
/// node_id`), and one upload round goes into a temp LocalFs store, as for the online
/// backup. Refuses a data directory whose `LOCK` is held (a running server).
pub async fn backup_offline(
    data_dir: &Path,
    node_id: u64,
    clock: Arc<dyn Clock>,
    out: &Path,
) -> Result<BackupReport, BackupError> {
    let lock_path = data_dir.join("LOCK");
    let lock = match File::options().read(true).write(true).open(&lock_path) {
        Ok(f) => Some(f),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(BackupError::Open(format!("{}: {e}", lock_path.display()))),
    };
    if let Some(f) = &lock {
        match f.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => {
                return Err(BackupError::Open(format!(
                    "{} is in use by a running server (stop it, or use the online backup)",
                    data_dir.display()
                )));
            }
            Err(fs::TryLockError::Error(e)) => return Err(BackupError::Io(e)),
        }
    }
    let partitions = node_file_partitions(data_dir)?;
    let count = PartitionCount::new(partitions)
        .ok_or_else(|| BackupError::Open("node.toml: partitions = 0".into()))?;

    // Meta on a copy: starting the group appends a term entry, the data dir stays untouched.
    let meta_copy = Staging::new("backup-meta")?;
    let meta = if data_dir.join("meta").is_dir() {
        let (from, to) = (data_dir.join("meta"), meta_copy.0.clone());
        tokio::task::spawn_blocking(move || copy_tree(&from, &to))
            .await
            .map_err(|e| BackupError::Io(io::Error::other(e)))??;
        Some(
            MetaHandle::start(&meta_copy.0, node_id)
                .await
                .map_err(|e| BackupError::Open(format!("metadata: {e}")))?,
        )
    } else {
        None
    };

    let mut cfg = NodeConfig::new(data_dir.to_path_buf(), count);
    cfg.node_id = node_id;
    cfg.mode = StartMode::Inspect;
    cfg.clock = Arc::clone(&clock);
    let node = match Node::open(cfg).await {
        Ok(n) => Arc::new(n),
        Err(e) => {
            if let Some(m) = meta {
                m.stop().await;
            }
            return Err(BackupError::Open(format!("node: {e}")));
        }
    };
    let failed: Vec<String> = node
        .slots()
        .iter()
        .filter_map(|s| match s {
            ostro_partition::PartitionSlot::Failed { partition, reason } => {
                Some(format!("partition {}: {reason}", partition.0))
            }
            ostro_partition::PartitionSlot::Ready(_) => None,
        })
        .collect();
    let r = if failed.is_empty() {
        backup_node_to_tar(
            Arc::clone(&node),
            meta.as_ref().map(|m| m.view().clone()),
            clock,
            node_id,
            true,
            out,
        )
        .await
    } else {
        Err(BackupError::Open(failed.join("; ")))
    };
    if let Ok(n) = Arc::try_unwrap(node)
        && let Err(e) = n.shutdown().await
    {
        tracing::warn!(error = %e, "offline backup: node shutdown");
    }
    if let Some(m) = meta {
        m.stop().await;
    }
    drop(lock);
    r
}
