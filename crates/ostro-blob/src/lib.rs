//! ostro-blob: local content-addressed blob store for payloads over the
//! 256 KiB inline limit (STOR-14).
//!
//! Blobs are content-addressed and durable before the referencing append
//! (02-RESEARCH Pattern 11): the submitter calls [`BlobStore::put`], which
//! returns only after the bytes are fsynced, renamed into place and the
//! directory entry is fsynced. Only then may a command referencing the
//! [`BlobRef`] be appended to the partition log. Apply never does blob I/O.
//!
//! Layout under the data dir:
//! - `blobs/<hex[0:2]>/<hex>`: complete, immutable blobs.
//! - `tmp/<hex>.<pid>.<counter>`: in-flight writes, on the same filesystem so
//!   `rename` is atomic. [`BlobStore::open`] deletes leftovers.
//!
//! A visible blob is always complete because it only appears through a rename
//! after its tmp file was fsynced. Object-store backends and blob GC are Phase 8.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

/// Reference to a stored blob: its sha256 and length.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct BlobRef {
    pub sha256: [u8; 32],
    pub len: u64,
}

/// Blob store errors. Callers treat every error from `put` as a failed
/// submit, never as success.
#[derive(Debug, thiserror::Error)]
pub enum BlobError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("blob {0} not found")]
    NotFound(String),
    #[error("blob {hex} corrupt: expected sha256 {hex}, got {actual}")]
    ShaMismatch { hex: String, actual: String },
    #[error("blob {hex} length {actual} != {expected}")]
    LenMismatch {
        hex: String,
        expected: u64,
        actual: u64,
    },
}

/// Lowercase hex of a sha256 digest (64 chars).
pub fn hex(sha: &[u8; 32]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(64);
    for b in sha {
        s.push(DIGITS[(b >> 4) as usize] as char);
        s.push(DIGITS[(b & 0x0f) as usize] as char);
    }
    s
}

static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Local content-addressed blob store rooted at a data dir.
#[derive(Debug)]
pub struct BlobStore {
    root: PathBuf,
}

const BLOBS_DIR: &str = "blobs";
const TMP_DIR: &str = "tmp";

fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Create `dir` if missing; fsync its parent when it was created.
fn ensure_dir(dir: &Path) -> io::Result<()> {
    match fs::create_dir(dir) {
        Ok(()) => {
            if let Some(parent) = dir.parent() {
                fsync_dir(parent)?;
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e),
    }
}

impl BlobStore {
    /// Open (or create) the store under `data_dir`. Creates `blobs/` and
    /// `tmp/`, fsyncs created directories and deletes stale `tmp/*` files
    /// left by a crash between tmp write and rename.
    pub fn open(data_dir: &Path) -> Result<Self, BlobError> {
        fs::create_dir_all(data_dir)?;
        let root = data_dir.to_path_buf();
        ensure_dir(&root.join(BLOBS_DIR))?;
        let tmp = root.join(TMP_DIR);
        ensure_dir(&tmp)?;

        let mut removed = 0usize;
        for entry in fs::read_dir(&tmp)? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(&path)?;
            } else {
                fs::remove_file(&path)?;
            }
            removed += 1;
        }
        if removed > 0 {
            fsync_dir(&tmp)?;
            tracing::info!(removed, dir = %tmp.display(), "removed stale blob tmp files");
        }
        Ok(Self { root })
    }

    /// Final path of a blob: `<data>/blobs/<hex[0:2]>/<hex>`.
    pub fn path_of(&self, r: &BlobRef) -> PathBuf {
        let h = hex(&r.sha256);
        self.root.join(BLOBS_DIR).join(&h[..2]).join(h)
    }

    /// Whether the blob is visible (complete) in the store.
    pub fn contains(&self, r: &BlobRef) -> bool {
        self.path_of(r).is_file()
    }

    /// Durably store `bytes` and return its reference. Idempotent: if the
    /// blob is already visible it is not rewritten. Synchronous; async
    /// callers use `spawn_blocking`.
    pub fn put(&self, bytes: &[u8]) -> Result<BlobRef, BlobError> {
        let r = BlobRef {
            sha256: Sha256::digest(bytes).into(),
            len: bytes.len() as u64,
        };
        let final_path = self.path_of(&r);
        if final_path.is_file() {
            // Visible blobs are complete: they only appear via rename after fsync.
            return Ok(r);
        }

        let h = hex(&r.sha256);
        let n = TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp_path = self
            .root
            .join(TMP_DIR)
            .join(format!("{h}.{}.{n}", std::process::id()));

        let mut f = File::create(&tmp_path)?;
        f.write_all(bytes)?;
        #[cfg(feature = "failpoints")]
        fail::fail_point!("blob::fsync", |_| Err(BlobError::Io(
            io::Error::from_raw_os_error(5)
        )));
        f.sync_all()?;
        drop(f);

        #[cfg(feature = "failpoints")]
        fail::fail_point!("blob::before_rename", |_| Err(BlobError::Io(
            io::Error::other("failpoint")
        )));

        let shard = self.root.join(BLOBS_DIR).join(&h[..2]);
        ensure_dir(&shard)?;
        fs::rename(&tmp_path, &final_path)?;
        fsync_dir(&shard)?;
        Ok(r)
    }

    /// Read a blob, verifying its length and sha256.
    pub fn get(&self, r: &BlobRef) -> Result<Vec<u8>, BlobError> {
        let h = hex(&r.sha256);
        let bytes = match fs::read(self.path_of(r)) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(BlobError::NotFound(h)),
            Err(e) => return Err(e.into()),
        };
        let actual_len = bytes.len() as u64;
        if actual_len != r.len {
            return Err(BlobError::LenMismatch {
                hex: h,
                expected: r.len,
                actual: actual_len,
            });
        }
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != r.sha256 {
            return Err(BlobError::ShaMismatch {
                hex: h,
                actual: hex(&actual),
            });
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod dev_dep_anchors {
    use tempfile as _;
}
