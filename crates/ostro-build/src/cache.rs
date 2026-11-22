//! Content-addressed toolchain cache (D-P10-1): `$OSTRO_TOOLCHAIN_DIR`, else
//! `$XDG_CACHE_HOME/ostro/toolchains`, else `~/.cache` / `~/Library/Caches` (hand-written, no
//! dirs crate), the cache lock, the atomic install, entry listing and pruning.
//!
//! Layout:
//! - `<root>/.lock`: the exclusive file lock (held for a whole install or prune);
//! - `<root>/tools/<tool>-<version>-<sha8>/`: one verified artefact, complete once `.ok`
//!   (holding the full pin sha) exists; installs build it in `tools/.tmp-<pid>-<n>/` and
//!   rename it into place;
//! - `<root>/state/<name>/`: builder state (uv venvs, jco node_modules, RUSTUP_HOME /
//!   CARGO_HOME, zig shims), owned by 10-12 / 10-15, removed only by `prune --state`.
//!
//! Locking (B5): first a process-wide async mutex keyed by the canonical root, then the
//! exclusive `std::fs::File::lock` on `<root>/.lock`, taken on the blocking pool so the async
//! runtime never blocks.
//!
//! Owner: 10-07.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use crate::extract;
use crate::fetch::{self, FetchOptions};
use crate::toolchain::{Pin, ToolchainError, ToolchainSet};

/// Tmp dirs of a process whose liveness cannot be checked are stale after this long.
const STALE_TMP_AGE: Duration = Duration::from_secs(3600);

static ROOT_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Arc<tokio::sync::Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// The toolchain cache.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cache {
    /// The cache root.
    pub root: PathBuf,
}

/// The exclusive cache lock; released on drop.
#[derive(Debug)]
pub struct CacheLock {
    file: fs::File,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for CacheLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// One `tools/` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryInfo {
    /// Dir name (`<tool>-<version>-<sha8>`).
    pub name: String,
    /// Full path.
    pub path: PathBuf,
    /// The full sha recorded in `.ok`; `None` for an incomplete entry.
    pub sha256: Option<String>,
}

impl Cache {
    /// The cache root from the process environment (see [`Cache::resolve_with`]).
    pub fn resolve() -> Result<Cache, ToolchainError> {
        Self::resolve_with(|k| std::env::var(k).ok(), std::env::consts::OS)
    }

    /// The cache root: `OSTRO_TOOLCHAIN_DIR`, else `XDG_CACHE_HOME/ostro/toolchains` (when
    /// absolute), else `HOME/Library/Caches/ostro/toolchains` on macOS and
    /// `HOME/.cache/ostro/toolchains` elsewhere. Empty values count as unset.
    pub fn resolve_with(
        env: impl Fn(&str) -> Option<String>,
        os: &str,
    ) -> Result<Cache, ToolchainError> {
        let get = |k: &str| env(k).filter(|v| !v.is_empty());
        if let Some(d) = get("OSTRO_TOOLCHAIN_DIR") {
            return Ok(Cache::with_root(d));
        }
        if let Some(x) = get("XDG_CACHE_HOME").filter(|x| Path::new(x).is_absolute()) {
            return Ok(Cache::with_root(
                Path::new(&x).join("ostro").join("toolchains"),
            ));
        }
        let home = get("HOME").ok_or_else(|| {
            ToolchainError::Config(
                "cannot resolve the toolchain cache: set OSTRO_TOOLCHAIN_DIR or HOME".into(),
            )
        })?;
        let base = if os == "macos" {
            Path::new(&home).join("Library").join("Caches")
        } else {
            Path::new(&home).join(".cache")
        };
        Ok(Cache::with_root(base.join("ostro").join("toolchains")))
    }

    /// A cache at `root`.
    pub fn with_root(root: impl Into<PathBuf>) -> Cache {
        Cache { root: root.into() }
    }

    /// `<root>/tools`.
    pub fn tools_dir(&self) -> PathBuf {
        self.root.join("tools")
    }

    /// `<root>/state/<name>` (builder state; not created here).
    pub fn state_dir(&self, name: &str) -> PathBuf {
        self.root.join("state").join(name)
    }

    /// `<tool>-<version>-<sha8>`.
    pub fn entry_name(pin: &Pin) -> String {
        format!(
            "{}-{}-{}",
            pin.tool.name(),
            pin.version,
            &pin.sha256[..8.min(pin.sha256.len())]
        )
    }

    /// `<root>/tools/<tool>-<version>-<sha8>`.
    pub fn entry_dir(&self, pin: &Pin) -> PathBuf {
        self.tools_dir().join(Self::entry_name(pin))
    }

    /// The entry exists and its `.ok` holds the full pin sha.
    pub fn is_installed(&self, pin: &Pin) -> bool {
        fs::read_to_string(self.entry_dir(pin).join(".ok")).is_ok_and(|s| s.trim() == pin.sha256)
    }

    /// Take the cache lock (process mutex, then the file lock) and remove stale tmp dirs.
    pub async fn lock(&self) -> Result<CacheLock, ToolchainError> {
        fs::create_dir_all(self.tools_dir())
            .map_err(|e| ToolchainError::io(format!("create {}", self.tools_dir().display()), e))?;
        let canonical = fs::canonicalize(&self.root)
            .map_err(|e| ToolchainError::io(format!("canonicalize {}", self.root.display()), e))?;
        let mutex = ROOT_LOCKS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(canonical.clone())
            .or_default()
            .clone();
        let guard = mutex.lock_owned().await;
        let path = canonical.join(".lock");
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| ToolchainError::io(format!("open {}", path.display()), e))?;
        let file = tokio::task::spawn_blocking(move || file.lock().map(|()| file))
            .await
            .map_err(|e| ToolchainError::Config(format!("lock task failed: {e}")))?
            .map_err(|e| ToolchainError::io(format!("lock {}", path.display()), e))?;
        let lock = CacheLock {
            file,
            _guard: guard,
        };
        clean_stale_tmp(&self.tools_dir());
        Ok(lock)
    }
}

/// Remove `tools/.tmp-<pid>-<n>` dirs left by dead processes (or by this process: under the
/// lock no install of ours is running). Without `/proc`, dirs older than an hour are stale.
fn clean_stale_tmp(tools: &Path) {
    let Ok(rd) = fs::read_dir(tools) else { return };
    let me = std::process::id();
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        let Some(rest) = name.strip_prefix(".tmp-") else {
            continue;
        };
        let pid = rest.split('-').next().and_then(|p| p.parse::<u32>().ok());
        let stale = match pid {
            Some(p) if p == me => true,
            Some(p) => match pid_alive(p) {
                Some(alive) => !alive,
                None => older_than(&e.path(), STALE_TMP_AGE),
            },
            None => older_than(&e.path(), STALE_TMP_AGE),
        };
        if stale {
            let _ = fs::remove_dir_all(e.path());
        }
    }
}

/// `Some(alive)` where liveness is observable (`/proc`), else `None`.
fn pid_alive(pid: u32) -> Option<bool> {
    let proc = Path::new("/proc");
    if proc.join("self").exists() {
        Some(proc.join(pid.to_string()).exists())
    } else {
        None
    }
}

fn older_than(p: &Path, age: Duration) -> bool {
    fs::symlink_metadata(p)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|e| e > age)
}

/// Removes the tmp dir on every exit path (including a cancelled future).
struct TmpDir(PathBuf);

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Download, verify, extract and atomically install `pin` (see [`crate::toolchain::ensure`]).
pub(crate) async fn install(
    cache: &Cache,
    pin: &Pin,
    opts: &FetchOptions,
) -> Result<PathBuf, ToolchainError> {
    let final_dir = cache.entry_dir(pin);
    if cache.is_installed(pin) {
        return Ok(final_dir);
    }
    if opts.offline {
        return Err(ToolchainError::Offline {
            tool: format!("{} {}", pin.tool, pin.version),
        });
    }
    let _lock = cache.lock().await?;
    // a concurrent install may have won while we waited
    if cache.is_installed(pin) {
        return Ok(final_dir);
    }
    let tools = cache.tools_dir();
    let tmp = TmpDir(tools.join(format!(
        ".tmp-{}-{}",
        std::process::id(),
        TMP_SEQ.fetch_add(1, Ordering::Relaxed)
    )));
    fs::create_dir_all(&tmp.0)
        .map_err(|e| ToolchainError::io(format!("create {}", tmp.0.display()), e))?;
    let url = opts.effective_url(&pin.url)?;
    let download = tmp.0.join("download");
    fetch::download_verified(opts, &url, &pin.sha256, pin.max_bytes(), &download).await?;

    let entry = tmp.0.join("entry");
    fs::create_dir(&entry)
        .map_err(|e| ToolchainError::io(format!("create {}", entry.display()), e))?;
    {
        let (download, entry) = (download.clone(), entry.clone());
        let (kind, strip, file_name) = (pin.kind, pin.strip.clone(), pin.file_name().to_string());
        tokio::task::spawn_blocking(move || {
            extract::extract(&download, kind, &entry, strip.as_deref(), &file_name)
        })
        .await
        .map_err(|e| ToolchainError::Config(format!("extract task failed: {e}")))??;
    }
    let ok = entry.join(".ok");
    fs::write(&ok, format!("{}\n", pin.sha256))
        .map_err(|e| ToolchainError::io(format!("write {}", ok.display()), e))?;
    if fs::symlink_metadata(&final_dir).is_ok() {
        // an incomplete entry (no or wrong `.ok`) from an older install
        fs::remove_dir_all(&final_dir)
            .map_err(|e| ToolchainError::io(format!("remove {}", final_dir.display()), e))?;
    }
    fs::rename(&entry, &final_dir)
        .map_err(|e| ToolchainError::io(format!("rename into {}", final_dir.display()), e))?;
    Ok(final_dir)
}

/// The `tools/` entries (tmp dirs excluded), sorted by name.
pub fn entries(cache: &Cache) -> Vec<EntryInfo> {
    let Ok(rd) = fs::read_dir(cache.tools_dir()) else {
        return Vec::new();
    };
    let mut out: Vec<EntryInfo> = rd
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') {
                return None;
            }
            let path = e.path();
            let sha256 = fs::read_to_string(path.join(".ok"))
                .ok()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            Some(EntryInfo { name, path, sha256 })
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    out
}

/// Under the lock: remove `tools/` entries not in `keep` and, with `include_state`, every
/// `state/` dir. Returns the removed paths.
pub async fn prune(
    cache: &Cache,
    keep: &ToolchainSet,
    include_state: bool,
) -> Result<Vec<PathBuf>, ToolchainError> {
    let _lock = cache.lock().await?;
    let keep_names: Vec<String> = keep.pins.iter().map(Cache::entry_name).collect();
    let mut removed = Vec::new();
    for e in entries(cache) {
        if !keep_names.contains(&e.name) {
            fs::remove_dir_all(&e.path)
                .map_err(|err| ToolchainError::io(format!("remove {}", e.path.display()), err))?;
            removed.push(e.path);
        }
    }
    if include_state {
        let state = cache.root.join("state");
        if let Ok(rd) = fs::read_dir(&state) {
            let mut paths: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
            paths.sort();
            for p in paths {
                let r = if p.is_dir() && !p.is_symlink() {
                    fs::remove_dir_all(&p)
                } else {
                    fs::remove_file(&p)
                };
                r.map_err(|err| ToolchainError::io(format!("remove {}", p.display()), err))?;
                removed.push(p);
            }
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| {
            pairs
                .iter()
                .find(|(n, _)| *n == k)
                .map(|(_, v)| v.to_string())
        }
    }

    #[test]
    fn root_precedence() {
        let all = [
            ("OSTRO_TOOLCHAIN_DIR", "/o"),
            ("XDG_CACHE_HOME", "/x"),
            ("HOME", "/h"),
        ];
        assert_eq!(
            Cache::resolve_with(env(&all), "linux").unwrap().root,
            PathBuf::from("/o")
        );
        assert_eq!(
            Cache::resolve_with(env(&all[1..]), "linux").unwrap().root,
            PathBuf::from("/x/ostro/toolchains")
        );
        assert_eq!(
            Cache::resolve_with(env(&all[2..]), "linux").unwrap().root,
            PathBuf::from("/h/.cache/ostro/toolchains")
        );
        assert_eq!(
            Cache::resolve_with(env(&all[2..]), "macos").unwrap().root,
            PathBuf::from("/h/Library/Caches/ostro/toolchains")
        );
        // empty and relative values are ignored
        let odd = [
            ("OSTRO_TOOLCHAIN_DIR", ""),
            ("XDG_CACHE_HOME", "rel"),
            ("HOME", "/h"),
        ];
        assert_eq!(
            Cache::resolve_with(env(&odd), "linux").unwrap().root,
            PathBuf::from("/h/.cache/ostro/toolchains")
        );
        assert!(matches!(
            Cache::resolve_with(env(&[]), "linux"),
            Err(ToolchainError::Config(_))
        ));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn root_macos_default() {
        let c = Cache::resolve_with(env(&[("HOME", "/Users/u")]), std::env::consts::OS).unwrap();
        assert_eq!(
            c.root,
            PathBuf::from("/Users/u/Library/Caches/ostro/toolchains")
        );
    }
}
