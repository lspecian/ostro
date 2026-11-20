//! LocalFs backend (`file:///abs/path`).
//!
//! - Keys map to paths under the root (`a/b/c` -> `<root>/a/b/c`); the key grammar
//!   ([`crate::check_key`]) rejects `..`, absolute keys and NUL, and the first segment
//!   `.ostro-tmp` is reserved for in-flight writes.
//! - Every write is durable: the body goes to `<root>/.ostro-tmp/<unique>`, is `sync_all`ed,
//!   missing parent directories are created (each creation fsynced into its parent), the tmp
//!   file is renamed over the destination and the destination directory is fsynced.
//! - Writes and deletes are serialised by a lock shared by every `LocalFs` on the same root in
//!   this process, so `put_if_absent` (If-None-Match `*`) and `put_if_match` (If-Match etag)
//!   are atomic check-and-install operations. Other processes writing the same root are not
//!   coordinated.
//! - etag = lowercase hex SHA-256 of the content. `get`/`head` report it; `list` does not
//!   (it would hash every listed object), so `list` returns `e_tag: None`.
//! - Because keys are paths, a key cannot also be a "directory" of another key (`a` and
//!   `a/b` cannot coexist); such a write fails with [`Error::Io`].
//! - Phase 6 (06-12): `put_with` stores the content type in a sidecar `<key>.ostro-meta.json`
//!   (written tmp + fsync + rename like the object, under the same lock). Key segments
//!   ending in `.ostro-meta.json` are reserved, sidecars never appear in `list`, and
//!   every write/delete of the key drops a stale sidecar. LocalFs never presigns.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use bytes::Bytes;
use futures_core::stream::BoxStream;
use futures_util::StreamExt as _;
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;

use crate::{Conditional, Error, ObjectMeta, ObjectStore, PutOpts, PutOutcome, Result, check_key};

const TMP_DIR: &str = ".ostro-tmp";
/// Suffix of the Phase 6 content-type sidecar (`<key>.ostro-meta.json`).
const META_SUFFIX: &str = ".ostro-meta.json";

/// The sidecar path of the object at `dest`.
fn sidecar_of(dest: &Path) -> PathBuf {
    let mut name = dest.file_name().unwrap_or_default().to_os_string();
    name.push(META_SUFFIX);
    dest.with_file_name(name)
}

/// Remove a stale sidecar (absent is fine). Caller holds the lock.
fn drop_sidecar(dest: &Path) -> Result<()> {
    let side = sidecar_of(dest);
    match fs::remove_file(&side) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(Error::io(side.display(), e)),
    }
}

/// `{"v":1,"content_type":"<escaped>"}`.
fn encode_sidecar(content_type: &str) -> Result<Vec<u8>> {
    let mut out = String::from("{\"v\":1,\"content_type\":\"");
    for c in content_type.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if c.is_control() => {
                return Err(Error::Config(
                    "content type contains a control character".into(),
                ));
            }
            c => out.push(c),
        }
    }
    out.push_str("\"}");
    Ok(out.into_bytes())
}

/// Inverse of [`encode_sidecar`]; `None` for anything this store did not write.
fn decode_sidecar(raw: &[u8]) -> Option<String> {
    let s = std::str::from_utf8(raw).ok()?;
    let body = s
        .strip_prefix("{\"v\":1,\"content_type\":\"")?
        .strip_suffix("\"}")?;
    let mut out = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => out.push(chars.next()?),
            '"' => return None,
            c => out.push(c),
        }
    }
    Some(out)
}

/// Filesystem-backed object store.
#[derive(Clone)]
pub struct LocalFs {
    root: PathBuf,
    lock: Arc<Mutex<()>>,
    conditional: bool,
}

impl std::fmt::Debug for LocalFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalFs").field("root", &self.root).finish()
    }
}

/// One write lock per canonical root, shared by every `LocalFs` instance in the process.
fn root_lock(root: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>> = OnceLock::new();
    let mut map = LOCKS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    map.entry(root.to_path_buf()).or_default().clone()
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(DIGITS[usize::from(b >> 4)] as char);
        s.push(DIGITS[usize::from(b & 0xf)] as char);
    }
    s
}

fn sha256_hex(body: &[u8]) -> String {
    hex(&Sha256::digest(body))
}

fn fsync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| Error::io(dir.display(), e))
}

fn mtime(md: &fs::Metadata) -> jiff::Timestamp {
    md.modified()
        .ok()
        .and_then(|t| jiff::Timestamp::try_from(t).ok())
        .unwrap_or(jiff::Timestamp::UNIX_EPOCH)
}

async fn blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::io("spawn_blocking", io::Error::other(e)))?
}

impl LocalFs {
    /// Open (creating if needed) a store rooted at the absolute directory `root`.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        if !root.is_absolute() {
            return Err(Error::Config(format!(
                "LocalFs root must be absolute: {}",
                root.display()
            )));
        }
        fs::create_dir_all(root.join(TMP_DIR)).map_err(|e| Error::io(root.display(), e))?;
        let root = root
            .canonicalize()
            .map_err(|e| Error::io(root.display(), e))?;
        Ok(Self {
            lock: root_lock(&root),
            root,
            conditional: true,
        })
    }

    /// Report `supports_conditional_put() == false` (tests of the P8-D4 fallback). The
    /// conditional methods still work atomically.
    pub fn with_conditional_put(mut self, enabled: bool) -> Self {
        self.conditional = enabled;
        self
    }

    /// The root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The content type stored by [`ObjectStore::put_with`] (Phase 6, 06-12); `None` when the
    /// object was written without one (or is absent).
    pub async fn content_type(&self, key: &str) -> Result<Option<String>> {
        let side = sidecar_of(&self.path_of(key)?);
        blocking(move || match fs::read(&side) {
            Ok(raw) => Ok(decode_sidecar(&raw)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(Error::io(side.display(), e)),
        })
        .await
    }

    fn path_of(&self, key: &str) -> Result<PathBuf> {
        check_key(key)?;
        if key == TMP_DIR || key.starts_with(".ostro-tmp/") {
            return Err(Error::InvalidKey {
                key: key.to_owned(),
                reason: "'.ostro-tmp' is reserved by LocalFs",
            });
        }
        if key.split('/').any(|seg| seg.ends_with(META_SUFFIX)) {
            return Err(Error::InvalidKey {
                key: key.to_owned(),
                reason: "the '.ostro-meta.json' suffix is reserved by LocalFs",
            });
        }
        Ok(self.root.join(key))
    }

    fn tmp_path(&self) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        self.root
            .join(TMP_DIR)
            .join(format!("{}-{n}.tmp", std::process::id()))
    }

    /// Write `body` to a fresh tmp file and `sync_all` it.
    fn write_tmp(&self, body: &[u8]) -> Result<PathBuf> {
        let tmp = self.tmp_path();
        let res = (|| {
            let mut f = File::create(&tmp)?;
            f.write_all(body)?;
            f.sync_all()
        })();
        match res {
            Ok(()) => Ok(tmp),
            Err(e) => {
                let _ = fs::remove_file(&tmp);
                Err(Error::io(tmp.display(), e))
            }
        }
    }

    /// Create the missing ancestors of `dest` (each fsynced into its parent), rename `tmp` over
    /// `dest` and fsync `dest`'s directory. Caller holds the lock. Removes `tmp` on failure.
    fn install(&self, tmp: &Path, dest: &Path) -> Result<()> {
        let res = (|| {
            let parent = dest.parent().unwrap_or(&self.root);
            let mut missing = Vec::new();
            let mut p = parent;
            while !p.exists() && p.starts_with(&self.root) && p != self.root {
                missing.push(p);
                p = p.parent().unwrap_or(&self.root);
            }
            for d in missing.into_iter().rev() {
                match fs::create_dir(d) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(e) => return Err(Error::io(d.display(), e)),
                }
                fsync_dir(d.parent().unwrap_or(&self.root))?;
            }
            fs::rename(tmp, dest).map_err(|e| Error::io(dest.display(), e))?;
            fsync_dir(parent)
        })();
        if res.is_err() {
            let _ = fs::remove_file(tmp);
        }
        res
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Hex SHA-256 of the file at `path`; `None` if it does not exist (or is a directory).
    fn file_etag(path: &Path) -> Result<Option<String>> {
        let mut f = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(Error::io(path.display(), e)),
        };
        if f.metadata()
            .map_err(|e| Error::io(path.display(), e))?
            .is_dir()
        {
            return Ok(None);
        }
        let mut h = Sha256::new();
        let mut buf = vec![0u8; 1 << 16];
        loop {
            let n = f.read(&mut buf).map_err(|e| Error::io(path.display(), e))?;
            if n == 0 {
                break;
            }
            h.update(&buf[..n]);
        }
        Ok(Some(hex(&h.finalize())))
    }

    fn conditional_install(
        &self,
        key: &str,
        body: Bytes,
        expect: Option<String>,
    ) -> Result<Conditional<PutOutcome>> {
        let dest = self.path_of(key)?;
        let e_tag = sha256_hex(&body);
        let tmp = self.write_tmp(&body)?;
        let _g = self.guard();
        let holds = match &expect {
            None => !dest.exists(),
            Some(want) => Self::file_etag(&dest)?.as_deref() == Some(want.as_str()),
        };
        if !holds {
            let _ = fs::remove_file(&tmp);
            return Ok(Conditional::PreconditionFailed);
        }
        self.install(&tmp, &dest)?;
        drop_sidecar(&dest)?;
        Ok(Conditional::Ok(PutOutcome { e_tag: Some(e_tag) }))
    }

    fn walk(&self, dir: &Path, rel: &str, out: &mut Vec<(String, fs::Metadata)>) -> Result<()> {
        let rd = match fs::read_dir(dir) {
            Ok(rd) => rd,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotADirectory => return Ok(()),
            Err(e) => return Err(Error::io(dir.display(), e)),
        };
        for entry in rd {
            let entry = entry.map_err(|e| Error::io(dir.display(), e))?;
            let Ok(name) = entry.file_name().into_string() else {
                continue; // not a key this store could have written
            };
            if rel.is_empty() && name == TMP_DIR {
                continue;
            }
            if name.ends_with(META_SUFFIX) {
                continue; // Phase 6 content-type sidecar, not an object
            }
            let key = if rel.is_empty() {
                name
            } else {
                format!("{rel}/{name}")
            };
            let md = entry.metadata().map_err(|e| Error::io(key.as_str(), e))?;
            if md.is_dir() {
                self.walk(&entry.path(), &key, out)?;
            } else if md.is_file() {
                out.push((key, md));
            }
        }
        Ok(())
    }

    fn list_blocking(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<ObjectMeta>> {
        // Start at the deepest directory the prefix names; filter the rest by string prefix.
        let dir_rel = prefix.rsplit_once('/').map_or("", |(d, _)| d);
        if !dir_rel.is_empty() && check_key(dir_rel).is_err() {
            return Ok(Vec::new()); // no stored key can have this prefix
        }
        let mut found = Vec::new();
        self.walk(&self.root.join(dir_rel), dir_rel, &mut found)?;
        let mut out: Vec<ObjectMeta> = found
            .into_iter()
            .filter(|(k, _)| k.starts_with(prefix) && start_after.is_none_or(|s| k.as_str() > s))
            .map(|(key, md)| ObjectMeta {
                key,
                size: md.len(),
                last_modified: mtime(&md),
                e_tag: None,
                version: None,
            })
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    fn delete_blocking(&self, key: &str) -> Result<()> {
        let path = self.path_of(key)?;
        let _g = self.guard();
        drop_sidecar(&path)?;
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) if path.is_dir() => {
                let _ = e;
                return Ok(()); // a "directory" is not an object
            }
            Err(e) => return Err(Error::io(path.display(), e)),
        }
        let parent = path.parent().unwrap_or(&self.root);
        fsync_dir(parent)?;
        // Prune now-empty directories so `a` can later be stored after `a/b` is deleted.
        let mut p = parent;
        while p != self.root && p.starts_with(&self.root) {
            if fs::remove_dir(p).is_err() {
                break;
            }
            p = p.parent().unwrap_or(&self.root);
            fsync_dir(p)?;
        }
        Ok(())
    }
}

#[async_trait]
impl ObjectStore for LocalFs {
    async fn put(&self, key: &str, body: Bytes) -> Result<PutOutcome> {
        let this = self.clone();
        let key = key.to_owned();
        blocking(move || {
            let dest = this.path_of(&key)?;
            let e_tag = sha256_hex(&body);
            let tmp = this.write_tmp(&body)?;
            let _g = this.guard();
            this.install(&tmp, &dest)?;
            drop_sidecar(&dest)?;
            Ok(PutOutcome { e_tag: Some(e_tag) })
        })
        .await
    }

    async fn put_with(&self, key: &str, body: Bytes, opts: &PutOpts) -> Result<PutOutcome> {
        let this = self.clone();
        let key = key.to_owned();
        let side = opts
            .content_type
            .as_deref()
            .map(encode_sidecar)
            .transpose()?;
        blocking(move || {
            let dest = this.path_of(&key)?;
            let e_tag = sha256_hex(&body);
            let tmp = this.write_tmp(&body)?;
            let side_tmp = match &side {
                Some(raw) => match this.write_tmp(raw) {
                    Ok(t) => Some(t),
                    Err(e) => {
                        let _ = fs::remove_file(&tmp);
                        return Err(e);
                    }
                },
                None => None,
            };
            let _g = this.guard();
            if let Err(e) = this.install(&tmp, &dest) {
                if let Some(t) = &side_tmp {
                    let _ = fs::remove_file(t);
                }
                return Err(e);
            }
            match side_tmp {
                Some(t) => this.install(&t, &sidecar_of(&dest))?,
                None => drop_sidecar(&dest)?,
            }
            Ok(PutOutcome { e_tag: Some(e_tag) })
        })
        .await
    }

    async fn put_if_absent(&self, key: &str, body: Bytes) -> Result<Conditional<PutOutcome>> {
        let this = self.clone();
        let key = key.to_owned();
        blocking(move || this.conditional_install(&key, body, None)).await
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Bytes,
        e_tag: &str,
    ) -> Result<Conditional<PutOutcome>> {
        let this = self.clone();
        let key = key.to_owned();
        let want = e_tag.trim_matches('"').to_owned();
        blocking(move || this.conditional_install(&key, body, Some(want))).await
    }

    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        let path = self.path_of(key)?;
        let key = key.to_owned();
        blocking(move || {
            let mut f = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(Error::io(path.display(), e)),
            };
            let md = f.metadata().map_err(|e| Error::io(path.display(), e))?;
            if md.is_dir() {
                return Ok(None);
            }
            let mut buf = Vec::with_capacity(usize::try_from(md.len()).unwrap_or(0));
            f.read_to_end(&mut buf)
                .map_err(|e| Error::io(path.display(), e))?;
            let meta = ObjectMeta {
                key,
                size: buf.len() as u64,
                last_modified: mtime(&md),
                e_tag: Some(sha256_hex(&buf)),
                version: None,
            };
            Ok(Some((Bytes::from(buf), meta)))
        })
        .await
    }

    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let path = self.path_of(key)?;
        let key = key.to_owned();
        blocking(move || {
            let mut f = match File::open(&path) {
                Ok(f) => f,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(Error::NotFound { key });
                }
                Err(e) => return Err(Error::io(path.display(), e)),
            };
            let md = f.metadata().map_err(|e| Error::io(path.display(), e))?;
            if md.is_dir() {
                return Err(Error::NotFound { key });
            }
            let size = md.len();
            let invalid = |size| Error::InvalidRange {
                key: key.clone(),
                start: range.start,
                end: range.end,
                size,
            };
            if range.start > range.end {
                return Err(invalid(Some(size)));
            }
            if range.start == range.end {
                return Ok(Bytes::new());
            }
            if range.start >= size {
                return Err(invalid(Some(size)));
            }
            let end = range.end.min(size);
            let len = usize::try_from(end - range.start).map_err(|_| invalid(Some(size)))?;
            let mut buf = vec![0u8; len];
            f.seek(SeekFrom::Start(range.start))
                .and_then(|_| f.read_exact(&mut buf))
                .map_err(|e| Error::io(path.display(), e))?;
            Ok(Bytes::from(buf))
        })
        .await
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let path = self.path_of(key)?;
        let key = key.to_owned();
        blocking(move || {
            let md = match fs::metadata(&path) {
                Ok(md) if md.is_file() => md,
                Ok(_) => return Ok(None),
                Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(e) => return Err(Error::io(path.display(), e)),
            };
            let Some(e_tag) = Self::file_etag(&path)? else {
                return Ok(None);
            };
            Ok(Some(ObjectMeta {
                key,
                size: md.len(),
                last_modified: mtime(&md),
                e_tag: Some(e_tag),
                version: None,
            }))
        })
        .await
    }

    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<ObjectMeta>> {
        let this = self.clone();
        let prefix = prefix.to_owned();
        let start_after = start_after.map(str::to_owned);
        blocking(move || this.list_blocking(&prefix, start_after.as_deref())).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let this = self.clone();
        let key = key.to_owned();
        blocking(move || this.delete_blocking(&key)).await
    }

    async fn delete_many(&self, keys: &[String]) -> Result<()> {
        let this = self.clone();
        let keys = keys.to_vec();
        blocking(move || keys.iter().try_for_each(|k| this.delete_blocking(k))).await
    }

    async fn put_multipart(
        &self,
        key: &str,
        mut body: BoxStream<'static, Result<Bytes>>,
    ) -> Result<PutOutcome> {
        let dest = self.path_of(key)?;
        let tmp = self.tmp_path();
        let streamed: Result<String> = async {
            let io_err = |e| Error::io(tmp.display(), e);
            let mut f = tokio::fs::File::create(&tmp).await.map_err(io_err)?;
            let mut h = Sha256::new();
            while let Some(chunk) = body.next().await {
                let chunk = chunk?;
                h.update(&chunk);
                f.write_all(&chunk).await.map_err(io_err)?;
            }
            f.flush().await.map_err(io_err)?;
            f.sync_all().await.map_err(io_err)?;
            Ok(hex(&h.finalize()))
        }
        .await;
        let e_tag = match streamed {
            Ok(t) => t,
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp).await;
                return Err(e);
            }
        };
        let this = self.clone();
        blocking(move || {
            let _g = this.guard();
            this.install(&tmp, &dest)?;
            drop_sidecar(&dest)?;
            Ok(PutOutcome { e_tag: Some(e_tag) })
        })
        .await
    }

    fn supports_conditional_put(&self) -> bool {
        self.conditional
    }
}
