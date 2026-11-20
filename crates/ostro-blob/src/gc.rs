//! Local blob garbage collection: the GC lock and the sweep (P8-D8, plan 08-19).
//!
//! The caller (the server's `blob_gc` service) builds the **mark** set: every blob
//! referenced by current partition state, by meta `Module` records and by retained local
//! snapshots. [`BlobStore::sweep`] then deletes every blob that is unmarked **and** whose
//! mtime is older than `older_than` (`gc_start - grace`).
//!
//! # Why a concurrent put never loses its blob
//!
//! - [`BlobStore::put`] holds the GC lock in **read** mode for its whole body (existence
//!   check, mtime touch or tmp write + rename). A put of an already present blob touches
//!   its mtime (`File::set_modified`), so a re-referenced old blob is young again.
//! - The sweep scans the directory without the lock (cheap, collects candidates), then
//!   takes the lock in **write** mode per batch, re-reads each candidate's mtime under the
//!   lock and unlinks it only if it is still old.
//! - So every put either completed before the batch's write lock (its touch or rename is
//!   visible to the re-stat, the blob is young and kept) or starts after the batch released
//!   the lock (the blob is missing, so the put writes it again). A put that returned `Ok`
//!   is therefore never followed by a missing blob, unless a *later* sweep with a newer
//!   `older_than` deletes it, which needs the blob to be unreferenced past the grace.
//!
//! The blob layout is frozen (`blobs/<hh>/<sha>`): GC only deletes files. Shard
//! directories stay. Files whose names are not a valid sha in the right shard are left
//! alone and reported once per sweep.
//!
//! Failpoint `blobgc::before_unlink` (P8 prefix) fires before each unlink.

use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::SystemTime;

use crate::{BLOBS_DIR, BlobError, BlobStore, fsync_dir};

/// Candidates unlinked per write-lock hold, so puts stall at most one batch.
pub const SWEEP_BATCH: usize = 1024;

/// The GC lock: puts take it shared, the sweep's unlink pass takes it exclusive.
#[derive(Debug, Default)]
pub struct GcLock(RwLock<()>);

impl GcLock {
    /// Shared mode (held by [`BlobStore::put`]). A poisoned lock (a panic in a sweep) is
    /// still usable: the lock protects no data, only ordering.
    pub fn read(&self) -> RwLockReadGuard<'_, ()> {
        self.0.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Exclusive mode (held by the sweep's unlink pass).
    pub fn write(&self) -> RwLockWriteGuard<'_, ()> {
        self.0.write().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What one sweep did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Blob files seen (valid sha names).
    pub scanned: u64,
    /// Blobs deleted.
    pub deleted: u64,
    /// Blobs kept because they are in the mark set.
    pub kept_marked: u64,
    /// Unmarked blobs kept because their mtime is not older than the cut-off.
    pub kept_young: u64,
    /// Bytes of the deleted blobs.
    pub bytes_freed: u64,
    /// Files in `blobs/` that are not valid blob names (left alone).
    pub unknown: u64,
    /// The deleted blobs (input to the remote deletion ledger).
    pub deleted_shas: Vec<[u8; 32]>,
}

/// Parse a 64-char lowercase hex sha256 name.
pub fn parse_hex32(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        let hi = nibble(b[2 * i])?;
        let lo = nibble(b[2 * i + 1])?;
        *o = (hi << 4) | lo;
    }
    Some(out)
}

fn nibble(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    }
}

struct Candidate {
    sha: [u8; 32],
    path: PathBuf,
}

impl BlobStore {
    /// The GC lock (shared by `put`, exclusive in [`sweep`](Self::sweep)).
    pub fn gc_lock(&self) -> &GcLock {
        &self.gc_lock
    }

    /// Every blob currently in the store (valid names only), unordered.
    pub fn list(&self) -> Result<Vec<[u8; 32]>, BlobError> {
        let mut out = Vec::new();
        let mut unknown = 0u64;
        walk(&self.root.join(BLOBS_DIR), &mut unknown, |sha, _| {
            out.push(sha);
        })?;
        Ok(out)
    }

    /// Delete every blob that is not in `mark` and whose mtime is older than `older_than`
    /// (see the module docs for the locking argument). Errors on individual files are
    /// logged and the file is kept; only a failure to read the top-level directory fails.
    pub fn sweep(
        &self,
        mark: &HashSet<[u8; 32]>,
        older_than: SystemTime,
    ) -> Result<SweepReport, BlobError> {
        let mut report = SweepReport::default();
        let mut candidates: Vec<Candidate> = Vec::new();
        let dir = self.root.join(BLOBS_DIR);

        // Scan without the lock: candidates are re-checked under it.
        let mut scanned = 0u64;
        let mut kept_marked = 0u64;
        let mut kept_young = 0u64;
        walk(&dir, &mut report.unknown, |sha, path| {
            scanned += 1;
            if mark.contains(&sha) {
                kept_marked += 1;
                return;
            }
            match mtime(path) {
                Ok(Some(m)) if m < older_than => candidates.push(Candidate {
                    sha,
                    path: path.to_path_buf(),
                }),
                Ok(Some(_)) => kept_young += 1,
                // Vanished (or unreadable): nothing to do.
                Ok(None) | Err(_) => {}
            }
        })?;
        report.scanned = scanned;
        report.kept_marked = kept_marked;
        report.kept_young = kept_young;

        for batch in candidates.chunks(SWEEP_BATCH) {
            let _w = self.gc_lock.write();
            let mut dirty: HashSet<PathBuf> = HashSet::new();
            for c in batch {
                // Re-read under the lock: a put that completed since the scan touched it.
                let (m, len) = match fs::metadata(&c.path) {
                    Ok(md) => (md.modified().ok(), md.len()),
                    Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                    Err(e) => {
                        tracing::warn!(path = %c.path.display(), error = %e, "blob gc: stat failed; kept");
                        continue;
                    }
                };
                if m.is_none_or(|m| m >= older_than) {
                    report.kept_young += 1;
                    continue;
                }
                #[cfg(feature = "failpoints")]
                fail::fail_point!("blobgc::before_unlink");
                match fs::remove_file(&c.path) {
                    Ok(()) => {
                        report.deleted += 1;
                        report.bytes_freed += len;
                        report.deleted_shas.push(c.sha);
                        if let Some(p) = c.path.parent() {
                            dirty.insert(p.to_path_buf());
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(e) => {
                        tracing::warn!(path = %c.path.display(), error = %e, "blob gc: unlink failed; kept");
                    }
                }
            }
            for d in dirty {
                if let Err(e) = fsync_dir(&d) {
                    tracing::warn!(dir = %d.display(), error = %e, "blob gc: shard fsync failed");
                }
            }
        }
        if report.unknown > 0 {
            tracing::warn!(
                unknown = report.unknown,
                dir = %dir.display(),
                "blob gc: left files that are not blob names alone"
            );
        }
        Ok(report)
    }
}

fn mtime(path: &Path) -> io::Result<Option<SystemTime>> {
    match fs::metadata(path) {
        Ok(md) => Ok(Some(md.modified()?)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Visit every valid blob file `blobs/<hh>/<sha>` (`hh` = the sha's first two hex chars);
/// count everything else in `unknown`.
fn walk(
    dir: &Path,
    unknown: &mut u64,
    mut f: impl FnMut([u8; 32], &Path),
) -> Result<(), BlobError> {
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e.into()),
    };
    for shard in rd {
        let shard = shard?;
        let name = shard.file_name();
        let shard_ok = shard.file_type().is_ok_and(|t| t.is_dir())
            && name
                .to_str()
                .is_some_and(|n| n.len() == 2 && n.bytes().all(|c| nibble(c).is_some()));
        if !shard_ok {
            *unknown += 1;
            continue;
        }
        let shard_name = name.to_string_lossy().into_owned();
        let files = match fs::read_dir(shard.path()) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(dir = %shard.path().display(), error = %e, "blob gc: shard unreadable");
                continue;
            }
        };
        for file in files {
            let Ok(file) = file else { continue };
            let fname = file.file_name();
            let sha = fname
                .to_str()
                .filter(|n| n.starts_with(shard_name.as_str()))
                .and_then(parse_hex32);
            match sha {
                Some(sha) if file.file_type().is_ok_and(|t| t.is_file()) => f(sha, &file.path()),
                _ => *unknown += 1,
            }
        }
    }
    Ok(())
}
