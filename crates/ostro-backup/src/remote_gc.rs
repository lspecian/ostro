//! Remote delayed-deletion blob GC (P8-D8, research Pattern 7, plan 08-19).
//!
//! Local blob GC deletes unreferenced blobs right away. Remote copies (`blobs/<hex>`) may
//! still be needed by an older restore point, so they are deleted only once no retained
//! restore point can reference them:
//!
//! 1. after each local sweep, the server records every deleted sha with the deletion time in
//!    the [`Ledger`] (`<data>/backup/gc-ledger`, append-only lines `"<sha hex> <ms>\n"`,
//!    fsynced). Without bottomless the ledger is never written;
//! 2. the **window** is the age of the oldest retained restore point:
//!    `now - cut_at_ms` of the oldest manifest still in the prefix (the oldest
//!    `manifest/gen/<g>`, or `manifest/current` when there is no gen manifest), see
//!    [`retention_window`];
//! 3. [`Ledger::run`] deletes remote `blobs/<hex>` for every entry with
//!    `deleted_at + window < now` (so every retained restore point was cut after the
//!    local deletion, when nothing referenced the blob any more) **unless the blob is
//!    present locally again** (it was re-put: re-referenced, and the uploader has or will
//!    upload it). Those entries are dropped without a remote delete. Then the ledger is
//!    compacted (tmp + fsync + rename + dir fsync).
//!
//! Only the newest deletion time of a sha counts (a blob deleted, re-put and deleted again
//! waits for the second deletion's window).
//!
//! # The re-put race (remote side)
//!
//! A put of `X` racing the remote delete must not leave `X` missing remotely while local
//! state references it. The uploader caches which blobs it already uploaded, so it would
//! not upload `X` again by itself. [`ReputGuard`] closes that hole:
//! - before deleting remote `X`, [`Ledger::run`] notes `X` in the guard;
//! - after the delete it re-checks the local store and re-uploads `X` if it reappeared;
//! - the server's `blob_gc` service watches `BlobStore::subscribe_puts` from start-up and
//!   calls [`ReputGuard::on_put`] for every put, which re-uploads a noted sha.
//!
//! So a put before the re-check is caught by the re-check, and a put after it produces an
//! event the watcher handles. A lagged watcher re-uploads every noted sha that is local.
//! After a restart the uploader's first round reconciles against `LIST blobs/`, so the
//! guard needs no persistence.
//!
//! Crash safety: a crash between the local unlink and the ledger append leaks the remote
//! copy, which [`Ledger::adopt_orphans`] later records (remote blobs that are neither local
//! nor in the ledger). A torn last ledger line is dropped (and truncated) on open.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use ostro_blob::{BlobRef, BlobStore};
use ostro_objstore::ObjectStore;
use tracing::{debug, warn};

use crate::fence::{FenceError, put_immutable};
use crate::manifest::{self, ManifestError};
use crate::uploader::blob_key;

/// Ledger location relative to the data dir.
pub const LEDGER_PATH: &str = "backup/gc-ledger";

/// Remote GC failures (all retried on the next pass).
#[derive(Debug, thiserror::Error)]
pub enum RemoteGcError {
    #[error("ledger io: {0}")]
    Io(#[from] io::Error),
    #[error("object store: {0}")]
    Store(#[from] ostro_objstore::Error),
    #[error("manifest: {0}")]
    Manifest(#[from] ManifestError),
    #[error("re-upload: {0}")]
    Reupload(#[from] FenceError),
    #[error("local blob store: {0}")]
    Blob(#[from] ostro_blob::BlobError),
}

/// What one [`Ledger::run`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RunReport {
    /// Ledger entries past the window.
    pub due: usize,
    /// Remote blobs deleted.
    pub deleted: usize,
    /// Due entries dropped because the blob is local again (re-put; never deleted).
    pub reput: usize,
    /// Blobs re-uploaded because they reappeared locally during the delete.
    pub reuploaded: usize,
    /// Entries left in the ledger.
    pub remaining: usize,
}

/// The delayed-deletion ledger (see the module docs).
#[derive(Debug)]
pub struct Ledger {
    path: PathBuf,
    /// sha → newest local deletion time (ms).
    entries: BTreeMap<[u8; 32], u64>,
    guard: Arc<ReputGuard>,
}

fn hex(sha: &[u8; 32]) -> String {
    ostro_blob::hex(sha)
}

fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn parse_line(line: &str) -> Option<([u8; 32], u64)> {
    let (h, ms) = line.split_once(' ')?;
    Some((ostro_blob::gc::parse_hex32(h)?, ms.parse().ok()?))
}

impl Ledger {
    /// Open (or create) the ledger under `data_dir`. Drops a torn last line (no trailing
    /// newline, or unparsable) and truncates the file to the last complete line; removes a
    /// leftover compaction tmp file.
    pub fn open(data_dir: &Path) -> Result<Self, RemoteGcError> {
        let path = data_dir.join(LEDGER_PATH);
        let dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
            if let Some(parent) = dir.parent() {
                fsync_dir(parent)?;
            }
        }
        let tmp = tmp_path(&path);
        if tmp.exists() {
            fs::remove_file(&tmp)?;
            fsync_dir(&dir)?;
        }
        let mut entries = BTreeMap::new();
        if let Ok(bytes) = fs::read(&path) {
            let mut good = 0usize;
            let mut pos = 0usize;
            while let Some(nl) = bytes[pos..].iter().position(|b| *b == b'\n') {
                let line = &bytes[pos..pos + nl];
                match std::str::from_utf8(line).ok().and_then(parse_line) {
                    Some((sha, ms)) => {
                        let e = entries.entry(sha).or_insert(ms);
                        *e = (*e).max(ms);
                        pos += nl + 1;
                        good = pos;
                    }
                    None => break,
                }
            }
            if good < bytes.len() {
                warn!(
                    path = %path.display(),
                    dropped = bytes.len() - good,
                    "blob gc ledger: dropping a torn tail"
                );
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(good as u64)?;
                f.sync_all()?;
            }
        }
        Ok(Self {
            path,
            entries,
            guard: Arc::new(ReputGuard::default()),
        })
    }

    /// The ledger file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The re-put guard shared with the put watcher (see the module docs).
    pub fn guard(&self) -> Arc<ReputGuard> {
        Arc::clone(&self.guard)
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the ledger is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The recorded deletion time of `sha`.
    pub fn deleted_at(&self, sha: &[u8; 32]) -> Option<u64> {
        self.entries.get(sha).copied()
    }

    /// Append `(sha, now_ms)` for every sha and fsync.
    pub fn record(&mut self, shas: &[[u8; 32]], now_ms: u64) -> Result<(), RemoteGcError> {
        if shas.is_empty() {
            return Ok(());
        }
        let mut buf = String::with_capacity(shas.len() * 80);
        for sha in shas {
            buf.push_str(&hex(sha));
            buf.push(' ');
            buf.push_str(&now_ms.to_string());
            buf.push('\n');
        }
        let created = !self.path.exists();
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
        if created && let Some(dir) = self.path.parent() {
            fsync_dir(dir)?;
        }
        for sha in shas {
            let e = self.entries.entry(*sha).or_insert(now_ms);
            *e = (*e).max(now_ms);
        }
        Ok(())
    }

    /// Entries whose newest deletion is older than the window: `deleted_at + window < now`.
    pub fn due(&self, window: Duration, now_ms: u64) -> Vec<[u8; 32]> {
        let window_ms = u64::try_from(window.as_millis()).unwrap_or(u64::MAX);
        self.entries
            .iter()
            .filter(|(_, at)| at.saturating_add(window_ms) < now_ms)
            .map(|(sha, _)| *sha)
            .collect()
    }

    /// Record remote blobs that are neither local nor in the ledger (a crash between the
    /// local unlink and [`record`](Self::record), or blobs a restore did not download), with
    /// deletion time `now_ms`. Returns how many were adopted.
    pub async fn adopt_orphans(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        local: &BlobStore,
        now_ms: u64,
    ) -> Result<usize, RemoteGcError> {
        let orphans: Vec<[u8; 32]> = store
            .list("blobs/", None)
            .await?
            .into_iter()
            .filter_map(|m| ostro_blob::gc::parse_hex32(m.key.strip_prefix("blobs/")?))
            .filter(|sha| !self.entries.contains_key(sha) && !is_local(local, sha))
            .collect();
        self.record(&orphans, now_ms)?;
        Ok(orphans.len())
    }

    /// Delete the remote copies of due entries that are not local again, then compact.
    pub async fn run(
        &mut self,
        store: &Arc<dyn ObjectStore>,
        window: Duration,
        now_ms: u64,
        local: &BlobStore,
    ) -> Result<RunReport, RemoteGcError> {
        let due = self.due(window, now_ms);
        let mut report = RunReport {
            due: due.len(),
            ..RunReport::default()
        };
        if due.is_empty() {
            report.remaining = self.entries.len();
            return Ok(report);
        }
        let (back, gone): (Vec<[u8; 32]>, Vec<[u8; 32]>) =
            due.into_iter().partition(|sha| is_local(local, sha));
        report.reput = back.len();
        for sha in &gone {
            self.guard.note(*sha);
        }
        if !gone.is_empty() {
            let keys: Vec<String> = gone.iter().map(blob_key).collect();
            store.delete_many(&keys).await?;
        }
        report.deleted = gone.len();
        // A put that landed during the delete: upload it again (the watcher covers later ones).
        for sha in &gone {
            if is_local(local, sha) && self.guard.reupload(store, local, sha).await? {
                report.reuploaded += 1;
            }
        }
        for sha in back.iter().chain(&gone) {
            self.entries.remove(sha);
        }
        self.compact()?;
        report.remaining = self.entries.len();
        debug!(?report, "remote blob gc");
        Ok(report)
    }

    /// Rewrite the ledger with the current entries (tmp + fsync + rename + dir fsync).
    pub fn compact(&self) -> Result<(), RemoteGcError> {
        let tmp = tmp_path(&self.path);
        let mut buf = String::with_capacity(self.entries.len() * 80);
        for (sha, ms) in &self.entries {
            buf.push_str(&hex(sha));
            buf.push(' ');
            buf.push_str(&ms.to_string());
            buf.push('\n');
        }
        let mut f = File::create(&tmp)?;
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
        drop(f);
        fs::rename(&tmp, &self.path)?;
        if let Some(dir) = self.path.parent() {
            fsync_dir(dir)?;
        }
        Ok(())
    }
}

fn tmp_path(path: &Path) -> PathBuf {
    path.with_extension("tmp")
}

fn is_local(local: &BlobStore, sha: &[u8; 32]) -> bool {
    local.contains(&BlobRef {
        sha256: *sha,
        len: 0,
    })
}

/// Shas whose remote copy this process deleted; a later local put of one re-uploads it.
#[derive(Debug, Default)]
pub struct ReputGuard {
    deleted: Mutex<HashSet<[u8; 32]>>,
}

impl ReputGuard {
    /// Note `sha` before its remote copy is deleted.
    pub fn note(&self, sha: [u8; 32]) {
        self.deleted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(sha);
    }

    /// Whether `sha` was remotely deleted by this process.
    pub fn contains(&self, sha: &[u8; 32]) -> bool {
        self.deleted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .contains(sha)
    }

    /// A put event: re-upload `sha` if its remote copy was deleted. `Ok(true)` if uploaded.
    pub async fn on_put(
        &self,
        store: &Arc<dyn ObjectStore>,
        local: &BlobStore,
        sha: &[u8; 32],
    ) -> Result<bool, RemoteGcError> {
        if !self.contains(sha) {
            return Ok(false);
        }
        self.reupload(store, local, sha).await
    }

    /// The put watcher lagged: re-upload every noted sha that is local. Returns the count.
    pub async fn on_lagged(
        &self,
        store: &Arc<dyn ObjectStore>,
        local: &BlobStore,
    ) -> Result<usize, RemoteGcError> {
        let noted: Vec<[u8; 32]> = self
            .deleted
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .copied()
            .collect();
        let mut n = 0;
        for sha in noted {
            if self.reupload(store, local, &sha).await? {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Upload the local copy of `sha` (immutable put; identical bytes are a no-op).
    /// `Ok(false)` when the blob is not local (nothing references it).
    async fn reupload(
        &self,
        store: &Arc<dyn ObjectStore>,
        local: &BlobStore,
        sha: &[u8; 32],
    ) -> Result<bool, RemoteGcError> {
        let path = local.path_of(&BlobRef {
            sha256: *sha,
            len: 0,
        });
        let bytes = match tokio::fs::read(&path).await {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(e) => return Err(e.into()),
        };
        put_immutable(store, &blob_key(sha), bytes, *sha).await?;
        debug!(blob = %hex(sha), "re-uploaded a blob re-put after its remote deletion");
        Ok(true)
    }
}

/// The remote retention window: `now - cut_at_ms` of the oldest manifest still in the
/// prefix (oldest `manifest/gen/<g>`, else `manifest/current`). `None` when there is no
/// manifest yet (nothing is ever remotely deleted then).
pub async fn retention_window(
    store: &Arc<dyn ObjectStore>,
    now_ms: u64,
) -> Result<Option<Duration>, RemoteGcError> {
    let mut oldest: Option<u64> = None;
    let gens: Vec<u64> = store
        .list(manifest::GEN_PREFIX, None)
        .await?
        .iter()
        .filter_map(|m| manifest::parse_gen_key(&m.key))
        .collect();
    // Oldest gen that still decodes (a dead gen awaiting retention is ignored only if it
    // is gone; an undecodable one cannot be restored from).
    let mut sorted = gens;
    sorted.sort_unstable();
    for g in sorted {
        if let Some((b, _)) = store.get(&manifest::gen_key(g)).await? {
            match manifest::decode(&b) {
                Ok(m) => {
                    oldest = Some(m.cut_at_ms);
                    break;
                }
                Err(e) => warn!(generation = g, error = %e, "remote gc: gen manifest undecodable"),
            }
        }
    }
    if let Some((b, _)) = store.get(manifest::CURRENT_KEY).await? {
        let m = manifest::decode(&b)?;
        oldest = Some(oldest.map_or(m.cut_at_ms, |o| o.min(m.cut_at_ms)));
    }
    Ok(oldest.map(|cut| Duration::from_millis(now_ms.saturating_sub(cut))))
}
