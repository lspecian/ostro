//! Polyglot guest builds through `sdks/scripts/build-guest.sh`, cached by source hash.
//!
//! The cache key is sha256 over the mode, `build-guest.sh`, the toolchain lock, the bundle
//! config, the frozen WIT, and every file of the guest dir and of the mode's SDK dir (when
//! present), sorted by relative path (`node_modules`, `__pycache__` and `.venv` excluded),
//! plus, for Python, the componentize-py selection (`OSTRO_COMPONENTIZE_PY`: path, size and
//! mtime of the fork binary, 09-17), so the released wheel and the D-P9-3 fork never share
//! cache entries.
//! Components land in `$OSTRO_POLYGLOT_CACHE` or `<repo>/target/polyglot-guests`. A
//! process-wide mutex serialises builds (a guest build can take ~0.5 GB, Pitfall 10).

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};

use sha2::{Digest, Sha256};

pub use crate::repo_root;

/// Guest build mode (`build-guest.sh` first argument).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Mode {
    /// componentize-py 0.25.1, `--stub-wasi`.
    Py,
    /// jco 1.42.0, QuickJS backend (default TS backend, D-P9-1).
    Ts,
    /// jco 1.42.0 + componentize-js 0.23.0, StarlingMonkey `--disable all` (fallback).
    TsSm,
}

impl Mode {
    /// The script's mode argument.
    pub fn arg(self) -> &'static str {
        match self {
            Mode::Py => "py",
            Mode::Ts => "ts",
            Mode::TsSm => "ts-sm",
        }
    }

    /// SDK sources staged with the guest (may not exist yet).
    pub fn sdk_dir(self) -> PathBuf {
        match self {
            Mode::Py => repo_root().join("sdks/python/ostro"),
            Mode::Ts | Mode::TsSm => repo_root().join("sdks/ts"),
        }
    }
}

static BUILD_LOCK: Mutex<()> = Mutex::new(());
static SCRIPT_RUNS: AtomicU32 = AtomicU32::new(0);

/// How many times this process ran `build-guest.sh` (cache misses).
pub fn script_runs() -> u32 {
    SCRIPT_RUNS.load(Ordering::SeqCst)
}

/// Cache directory: `$OSTRO_POLYGLOT_CACHE` or `<repo>/target/polyglot-guests`.
pub fn cache_dir() -> PathBuf {
    match std::env::var_os("OSTRO_POLYGLOT_CACHE") {
        Some(d) if !d.is_empty() => PathBuf::from(d),
        _ => repo_root().join("target/polyglot-guests"),
    }
}

fn skip_dir(name: &str) -> bool {
    matches!(name, "node_modules" | "__pycache__" | ".venv")
}

fn collect(base: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> Result<(), String> {
    let rd = std::fs::read_dir(dir).map_err(|e| format!("read_dir {}: {e}", dir.display()))?;
    for entry in rd {
        let entry = entry.map_err(|e| format!("read_dir {}: {e}", dir.display()))?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        let ft = entry
            .file_type()
            .map_err(|e| format!("stat {}: {e}", path.display()))?;
        if ft.is_dir() {
            if !skip_dir(&name) {
                collect(base, &path, out)?;
            }
        } else if ft.is_file() && !name.ends_with(".pyc") {
            let rel = path
                .strip_prefix(base)
                .expect("under base")
                .to_string_lossy()
                .replace('\\', "/");
            out.push((rel, path));
        }
    }
    Ok(())
}

fn hash_file(h: &mut Sha256, label: &str, path: &Path) -> Result<(), String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    h.update(label.as_bytes());
    h.update([0u8]);
    h.update((bytes.len() as u64).to_le_bytes());
    h.update(&bytes);
    Ok(())
}

fn hash_tree(h: &mut Sha256, label: &str, dir: &Path) -> Result<(), String> {
    let mut files = Vec::new();
    collect(dir, dir, &mut files)?;
    files.sort();
    h.update(format!("tree:{label}:{}", files.len()).as_bytes());
    for (rel, path) in files {
        hash_file(h, &format!("{label}/{rel}"), &path)?;
    }
    Ok(())
}

/// Hex cache key for `(mode, guest_dir)`.
pub fn cache_key(mode: Mode, guest_dir: &Path) -> Result<String, String> {
    let root = repo_root();
    let mut h = Sha256::new();
    h.update(b"ostro-polyglot-guest/v1\0");
    h.update(mode.arg().as_bytes());
    h.update([0u8]);
    hash_file(
        &mut h,
        "build-guest.sh",
        &root.join("sdks/scripts/build-guest.sh"),
    )?;
    hash_file(
        &mut h,
        "package-lock.json",
        &root.join("sdks/scripts/toolchain/package-lock.json"),
    )?;
    hash_file(
        &mut h,
        "rolldown.ostro.mjs",
        &root.join("sdks/scripts/toolchain/rolldown.ostro.mjs"),
    )?;
    hash_file(
        &mut h,
        "runtime.wit",
        &root.join("wit/ostro-runtime-0.1.0/runtime.wit"),
    )?;
    if mode == Mode::Py {
        match std::env::var_os("OSTRO_COMPONENTIZE_PY").filter(|v| !v.is_empty()) {
            Some(bin) => {
                let meta = std::fs::metadata(&bin)
                    .map_err(|e| format!("OSTRO_COMPONENTIZE_PY {}: {e}", bin.to_string_lossy()))?;
                let mtime = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .map_or(0, |d| d.as_nanos());
                h.update(
                    format!(
                        "componentize-py:{}:{}:{mtime}\0",
                        bin.to_string_lossy(),
                        meta.len()
                    )
                    .as_bytes(),
                );
            }
            None => h.update(b"componentize-py:released-0.25.1\0"),
        }
    }
    hash_tree(&mut h, "guest", guest_dir)?;
    let sdk = mode.sdk_dir();
    if sdk.is_dir() {
        hash_tree(&mut h, "sdk", &sdk)?;
    } else {
        h.update(b"no-sdk");
    }
    Ok(h.finalize().iter().map(|b| format!("{b:02x}")).collect())
}

/// Builds (or reuses from the cache) the component for `guest_dir` in `mode` and returns
/// its path. Fails loudly when a toolchain is missing (the script exits 2).
pub fn build(mode: Mode, guest_dir: &Path) -> Result<PathBuf, String> {
    let _guard = BUILD_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let guest_dir = guest_dir
        .canonicalize()
        .map_err(|e| format!("guest dir {}: {e}", guest_dir.display()))?;
    let key = cache_key(mode, &guest_dir)?;
    let name = guest_dir
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "guest".into());
    let dir = cache_dir();
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
    let out = dir.join(format!("{name}-{}-{}.wasm", mode.arg(), &key[..16]));
    if out.is_file() {
        return Ok(out);
    }
    let tmp = dir.join(format!(".tmp-{}-{}.wasm", std::process::id(), &key[..16]));
    let script = repo_root().join("sdks/scripts/build-guest.sh");
    SCRIPT_RUNS.fetch_add(1, Ordering::SeqCst);
    let res = Command::new("bash")
        .arg(&script)
        .arg(mode.arg())
        .arg(&guest_dir)
        .arg(&tmp)
        .output()
        .map_err(|e| format!("spawn bash {}: {e}", script.display()))?;
    if !res.status.success() {
        let _ = std::fs::remove_file(&tmp);
        return Err(format!(
            "build-guest.sh {} {} failed ({}):\n--- stdout\n{}\n--- stderr\n{}",
            mode.arg(),
            guest_dir.display(),
            res.status,
            String::from_utf8_lossy(&res.stdout),
            String::from_utf8_lossy(&res.stderr)
        ));
    }
    std::fs::rename(&tmp, &out).map_err(|e| format!("rename {}: {e}", out.display()))?;
    Ok(out)
}
