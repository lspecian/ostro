//! AOT artefact cache + bounded compile pool (ENG-07).
//!
//! L1 is an on-disk cache of `Engine::precompile_component` artefacts keyed by
//! `sha256(module)` plus a stable digest of the engine's compatibility hash, the
//! Wasmtime version, the Ostro version and the target triple ([`cache_key`]).
//!
//! `Component::deserialize` is `unsafe`: it trusts the bytes to be a valid artefact for
//! this engine. Every artefact on disk therefore carries an HMAC-SHA256 trailer keyed
//! with a node-local secret ([`load_or_create_key`]). Loads read the whole file into
//! memory, verify the trailer in constant time and only then deserialize *from those
//! in-memory bytes*. The file-based deserialize API is never used: it would re-open and
//! mmap the file after verification (TOCTOU).

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use wasmtime::component::Component;

use crate::error::EngineError;

type HmacSha256 = Hmac<Sha256>;

/// Length of the HMAC-SHA256 trailer appended to every artefact.
const TAG_LEN: usize = 32;
/// File name of the node-local HMAC key inside the cache directory.
const KEY_FILE: &str = "aot.key";
/// Subdirectory holding `<key>.cwasm` artefacts.
const AOT_DIR: &str = "aot";
/// Wasmtime version baked into the cache key (must match the exact workspace pin).
const WASMTIME_VERSION: &str = "wasmtime=50.0.2";

/// `std::hash::Hasher` adapter that feeds a SHA-256 digest.
///
/// Wasmtime only exposes its compatibility hash as `impl Hash`; feeding it into
/// std's default SipHash hasher would make the key depend on an algorithm that is unspecified across
/// Rust releases, so it is fed into SHA-256 instead. `finish` is never used.
struct Sha256Hasher(Sha256);

impl Hasher for Sha256Hasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    fn finish(&self) -> u64 {
        0
    }
}

fn target_env() -> &'static str {
    if cfg!(target_env = "gnu") {
        "gnu"
    } else if cfg!(target_env = "musl") {
        "musl"
    } else if cfg!(target_env = "msvc") {
        "msvc"
    } else {
        "none"
    }
}

/// Stable digest of everything that makes an artefact loadable by `engine`.
pub fn compat_digest(engine: &wasmtime::Engine) -> [u8; 32] {
    let mut h = Sha256Hasher(Sha256::new());
    engine.precompile_compatibility_hash().hash(&mut h);
    let mut d = h.0;
    for part in [
        WASMTIME_VERSION,
        concat!("ostro=", env!("CARGO_PKG_VERSION")),
        std::env::consts::ARCH,
        std::env::consts::OS,
        target_env(),
    ] {
        // Length-prefix each part so concatenations cannot collide.
        d.update((part.len() as u64).to_le_bytes());
        d.update(part.as_bytes());
    }
    d.finalize().into()
}

/// SHA-256 of a module's bytes.
pub fn module_sha(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xf) as usize] as char);
    }
    s
}

/// Cache key: `hex(sha) + "-" + first 16 hex chars of compat_digest(engine)`.
pub fn cache_key(engine: &wasmtime::Engine, sha: &[u8; 32]) -> String {
    let digest = hex(&compat_digest(engine));
    format!("{}-{}", hex(sha), &digest[..16])
}

/// 32 random bytes for a fresh key.
///
/// Unix reads `/dev/urandom`. The portable fallback hashes a `RandomState`-seeded hash
/// (OS-randomised per process), the wall clock and the pid with SHA-256; it is only used
/// where `/dev/urandom` is unavailable.
fn random_key() -> io::Result<[u8; 32]> {
    let mut key = [0u8; 32];
    #[cfg(unix)]
    {
        if std::fs::File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut key))
            .is_ok()
        {
            return Ok(key);
        }
    }
    use std::hash::BuildHasher;
    let mut d = Sha256::new();
    for i in 0..4u64 {
        let rs = std::collections::hash_map::RandomState::new();
        d.update(rs.hash_one(i).to_le_bytes());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    d.update(now.as_nanos().to_le_bytes());
    d.update(std::process::id().to_le_bytes());
    key.copy_from_slice(&d.finalize());
    Ok(key)
}

#[cfg(unix)]
fn fsync_dir(dir: &Path) -> io::Result<()> {
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(not(unix))]
fn fsync_dir(_dir: &Path) -> io::Result<()> {
    Ok(())
}

/// Load the node-local HMAC key from `dir/aot.key`, creating it (mode 0600 on unix,
/// 32 random bytes) when absent. The key never leaves the node.
pub fn load_or_create_key(dir: &Path) -> io::Result<[u8; 32]> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(KEY_FILE);
    let read_existing = |path: &Path| -> io::Result<[u8; 32]> {
        let bytes = std::fs::read(path)?;
        bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "{} has {} bytes, expected 32 (delete it to regenerate)",
                    path.display(),
                    bytes.len()
                ),
            )
        })
    };
    if path.exists() {
        return read_existing(&path);
    }
    let key = random_key()?;
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    match opts.open(&path) {
        Ok(mut f) => {
            f.write_all(&key)?;
            f.sync_all()?;
            fsync_dir(dir)?;
            Ok(key)
        }
        // Lost a creation race: use the winner's key.
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => read_existing(&path),
        Err(e) => Err(e),
    }
}

/// Cache counters (also exported as metrics where noted).
#[derive(Debug, Default)]
pub struct CacheStats {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub compiles: AtomicU64,
    /// `ostro_aot_cache_tamper_total`: HMAC mismatch or truncated artefact.
    pub tamper_total: AtomicU64,
    /// Authenticated artefact that this engine refused to deserialize.
    pub incompatible_total: AtomicU64,
}

/// Point-in-time copy of [`CacheStats`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStatsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub compiles: u64,
    pub tamper_total: u64,
    pub incompatible_total: u64,
}

impl CacheStats {
    pub fn snapshot(&self) -> CacheStatsSnapshot {
        CacheStatsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            compiles: self.compiles.load(Ordering::Relaxed),
            tamper_total: self.tamper_total.load(Ordering::Relaxed),
            incompatible_total: self.incompatible_total.load(Ordering::Relaxed),
        }
    }
}

/// L1: HMAC-authenticated on-disk artefact cache (`dir/aot/<key>.cwasm`).
#[derive(Debug, Clone)]
pub struct DiskCache {
    dir: PathBuf,
    key: [u8; 32],
    stats: Arc<CacheStats>,
}

impl DiskCache {
    /// Open (creating `dir/aot/`) a disk cache authenticated with `key`.
    pub fn new(dir: &Path, key: [u8; 32]) -> io::Result<Self> {
        let dir = dir.join(AOT_DIR);
        std::fs::create_dir_all(&dir)?;
        Ok(Self {
            dir,
            key,
            stats: Arc::new(CacheStats::default()),
        })
    }

    /// Path of the artefact for `key`.
    pub fn artefact_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{key}.cwasm"))
    }

    pub fn stats(&self) -> CacheStatsSnapshot {
        self.stats.snapshot()
    }

    fn mac(&self) -> HmacSha256 {
        <HmacSha256 as KeyInit>::new_from_slice(&self.key).expect("HMAC accepts any key length")
    }

    fn discard(&self, path: &Path) {
        if let Err(e) = std::fs::remove_file(path)
            && e.kind() != io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), error = %e, "failed to delete AOT artefact");
        }
    }

    /// Load and authenticate the artefact for `key`. Any failure (absent, truncated,
    /// tampered, incompatible) is a miss; bad artefacts are deleted and counted.
    pub fn get(&self, engine: &wasmtime::Engine, key: &str) -> Option<Component> {
        let path = self.artefact_path(key);
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                if e.kind() != io::ErrorKind::NotFound {
                    tracing::warn!(path = %path.display(), error = %e, "failed to read AOT artefact");
                }
                return None;
            }
        };
        let authentic = bytes.len() >= TAG_LEN && {
            let (body, tag) = bytes.split_at(bytes.len() - TAG_LEN);
            let mut mac = self.mac();
            mac.update(body);
            mac.verify_slice(tag).is_ok()
        };
        if !authentic {
            self.discard(&path);
            self.stats.tamper_total.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("ostro_aot_cache_tamper_total").increment(1);
            tracing::warn!(path = %path.display(), "AOT artefact failed authentication; deleted");
            return None;
        }
        let body = &bytes[..bytes.len() - TAG_LEN];
        // SAFETY: `body` is an in-memory copy (not re-read from disk) whose HMAC under the
        // node-local key verified above, so it was written by `put` on this node with the
        // output of `precompile_component` from an engine with the same compat digest
        // (the digest is part of `key`). Wasmtime additionally checks its own
        // compatibility header and returns an error (handled below) on mismatch.
        match unsafe { Component::deserialize(engine, body) } {
            Ok(c) => Some(c),
            Err(e) => {
                self.discard(&path);
                self.stats
                    .incompatible_total
                    .fetch_add(1, Ordering::Relaxed);
                tracing::warn!(path = %path.display(), error = %e, "incompatible AOT artefact; deleted");
                None
            }
        }
    }

    /// Store `cwasm ‖ hmac(cwasm)` for `key` atomically (tmp + fsync + rename + dir fsync).
    pub fn put(&self, key: &str, cwasm: &[u8]) -> io::Result<()> {
        let mut mac = self.mac();
        mac.update(cwasm);
        let tag = mac.finalize().into_bytes();
        static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
        let tmp = self.dir.join(format!(
            ".{key}.{}.{}.tmp",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let write = || -> io::Result<()> {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(cwasm)?;
            f.write_all(&tag)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, self.artefact_path(key))?;
            fsync_dir(&self.dir)
        };
        let res = write();
        if res.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        res
    }
}

type Flight = Arc<tokio::sync::OnceCell<Result<Component, String>>>;

/// AOT cache facade: L1 disk cache + bounded, single-flight compile pool.
///
/// Phase 4 deploy calls [`AotCache::component`] at upload time so modules are compiled at
/// deploy (ENG-07), and plan 03-11 builds the L2 `InstancePre` LRU on top of this.
pub struct AotCache {
    disk: DiskCache,
    engine: wasmtime::Engine,
    permits: Arc<tokio::sync::Semaphore>,
    inflight: Mutex<HashMap<String, Flight>>,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl AotCache {
    /// Open the cache in `dir` (creating `dir/aot/`) with at most `compile_permits`
    /// concurrent compiles (minimum 1).
    pub fn new(
        dir: &Path,
        key: [u8; 32],
        engine: wasmtime::Engine,
        compile_permits: usize,
    ) -> io::Result<Self> {
        Ok(Self {
            disk: DiskCache::new(dir, key)?,
            engine,
            permits: Arc::new(tokio::sync::Semaphore::new(compile_permits.max(1))),
            inflight: Mutex::new(HashMap::new()),
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
        })
    }

    /// The underlying disk cache.
    pub fn disk(&self) -> &DiskCache {
        &self.disk
    }

    pub fn stats(&self) -> CacheStatsSnapshot {
        self.disk.stats()
    }

    /// Highest number of compiles observed running at once (test hook).
    pub fn max_concurrent_compiles(&self) -> usize {
        self.max_active.load(Ordering::Relaxed)
    }

    /// The compiled component for the module `wasm` whose SHA-256 is `sha`.
    ///
    /// Concurrent calls for the same cache key share one flight: one disk lookup and, on
    /// a miss, one compile. Compiles run on the blocking pool behind the semaphore.
    pub async fn component(&self, sha: [u8; 32], wasm: &[u8]) -> Result<Component, EngineError> {
        let key = cache_key(&self.engine, &sha);
        let flight: Flight = {
            let mut map = self.inflight.lock().expect("inflight lock poisoned");
            map.entry(key.clone()).or_default().clone()
        };
        let res = flight
            .get_or_init(|| self.lookup_or_compile(key.clone(), wasm))
            .await
            .clone();
        {
            let mut map = self.inflight.lock().expect("inflight lock poisoned");
            if map.get(&key).is_some_and(|f| Arc::ptr_eq(f, &flight)) {
                map.remove(&key);
            }
        }
        res.map_err(EngineError::Cache)
    }

    async fn lookup_or_compile(&self, key: String, wasm: &[u8]) -> Result<Component, String> {
        let stats = self.disk.stats.clone();
        let (disk, engine, k) = (self.disk.clone(), self.engine.clone(), key.clone());
        let hit = tokio::task::spawn_blocking(move || disk.get(&engine, &k))
            .await
            .map_err(|e| format!("disk lookup task failed: {e}"))?;
        if let Some(c) = hit {
            stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(c);
        }
        stats.misses.fetch_add(1, Ordering::Relaxed);

        let _permit = self
            .permits
            .acquire()
            .await
            .map_err(|e| format!("compile pool closed: {e}"))?;
        let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(now, Ordering::SeqCst);
        let wasm: Arc<[u8]> = wasm.into();
        let (disk, engine) = (self.disk.clone(), self.engine.clone());
        let compiled = tokio::task::spawn_blocking(move || -> Result<Component, String> {
            let cwasm = engine
                .precompile_component(&wasm)
                .map_err(|e| format!("compile failed: {e:#}"))?;
            if let Err(e) = disk.put(&key, &cwasm) {
                // Not fatal: the run proceeds and the next load recompiles.
                tracing::warn!(key = %key, error = %e, "failed to persist AOT artefact");
            }
            // SAFETY: `cwasm` was just produced in memory by `precompile_component` on
            // this same engine and never left the process.
            unsafe { Component::deserialize(&engine, &cwasm) }
                .map_err(|e| format!("deserialize of fresh artefact failed: {e:#}"))
        })
        .await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        let component = compiled.map_err(|e| format!("compile task failed: {e}"))??;
        stats.compiles.fetch_add(1, Ordering::Relaxed);
        Ok(component)
    }
}
