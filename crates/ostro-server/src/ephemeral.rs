//! Where `node.ephemeral` (`ostro dev --ephemeral`, the test kits) puts its data directory.
//!
//! `/dev/shm` is preferred: it is tmpfs, so the ~5,000 `fsync`s of a cold start are nearly
//! free and the CLI-01 1 s startup budget holds. It is used only when it
//!
//! 1. exists (macOS and Windows have none),
//! 2. is writable (a probe file is created and removed), and
//! 3. has at least [`MIN_SHM_FREE_BYTES`] available (`statvfs`, `f_bavail * f_frsize`).
//!
//! Otherwise the directory goes in [`std::env::temp_dir`] (`TMPDIR` on Unix). The data
//! layout and code path are the same either way (D5: no in-memory store). The server logs
//! one line with the base it picked and why.
//!
//! # Why 512 MiB
//!
//! Docker gives a container a 64 MB `/dev/shm` by default. One Python module is ~19 MB as
//! wasm, and its prepared (AOT) artifact is a few times larger, so the first Python deploy
//! failed with ENOSPC (HTTP 503 "storing the module failed", found by 10-19). The largest
//! module the API accepts is `[api] max_module_bytes` = 64 MiB by default. A module of that
//! size plus its AOT artifact (2-3x the wasm) is ~256 MiB. 512 MiB leaves the same again for
//! the WAL, partition state stores, meta and a second module version. Any normal Linux host
//! passes: tmpfs `/dev/shm` defaults to half of RAM. A container with the default 64 MB (or
//! a `--shm-size` below 512m) uses the temp dir instead. Startup is then slower (fsyncs hit
//! the disk), but deploys no longer run out of space.
//!
//! The check runs once at startup. A tmpfs that fills up later can still return ENOSPC.
//! There is no override knob: RESERVATIONS §11 reserves no name for one. `TMPDIR` moves
//! the fallback directory.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use tempfile::TempDir;
use tracing::{info, warn};

/// The preferred parent of ephemeral data directories (tmpfs on Linux).
pub const SHM_DIR: &str = "/dev/shm";

/// The least free space on [`SHM_DIR`] for it to be used (512 MiB; see the module docs).
pub const MIN_SHM_FREE_BYTES: u64 = 512 * 1024 * 1024;

/// The prefix of every ephemeral data directory.
const PREFIX: &str = "ostro-";

/// What startup found out about the shm candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShmProbe {
    /// It is a directory.
    pub exists: bool,
    /// A probe file could be created in it.
    pub writable: bool,
    /// Bytes available to this process (`None` when `statvfs` failed).
    pub free_bytes: Option<u64>,
}

/// Why the shm candidate was not used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fallback {
    /// No such directory (macOS, Windows, minimal containers).
    Missing,
    /// It exists but a probe file could not be created.
    NotWritable,
    /// Its free space could not be read.
    FreeUnknown,
    /// Less than the minimum is free.
    TooSmall {
        /// Bytes available.
        free: u64,
        /// The required minimum.
        min: u64,
    },
}

impl fmt::Display for Fallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("it does not exist"),
            Self::NotWritable => f.write_str("it is not writable"),
            Self::FreeUnknown => f.write_str("its free space could not be read"),
            Self::TooSmall { free, min } => write!(
                f,
                "only {} MiB free, {} MiB needed",
                free / (1024 * 1024),
                min / (1024 * 1024)
            ),
        }
    }
}

/// The base directory chosen for the ephemeral data directory.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BaseChoice {
    /// The shm candidate.
    Shm,
    /// The OS temp dir, for the given reason.
    TempDir(Fallback),
}

/// Pure selection: the shm candidate when it exists, is writable and has at least
/// `min_free` bytes available; the temp dir otherwise.
pub fn choose(probe: ShmProbe, min_free: u64) -> BaseChoice {
    if !probe.exists {
        return BaseChoice::TempDir(Fallback::Missing);
    }
    if !probe.writable {
        return BaseChoice::TempDir(Fallback::NotWritable);
    }
    match probe.free_bytes {
        None => BaseChoice::TempDir(Fallback::FreeUnknown),
        Some(free) if free < min_free => BaseChoice::TempDir(Fallback::TooSmall {
            free,
            min: min_free,
        }),
        Some(_) => BaseChoice::Shm,
    }
}

/// Probe `dir`: existence, a create-and-remove write test, and `statvfs` free space.
pub fn probe(dir: &Path) -> ShmProbe {
    let exists = dir.is_dir();
    if !exists {
        return ShmProbe {
            exists,
            writable: false,
            free_bytes: None,
        };
    }
    let writable = tempfile::Builder::new()
        .prefix(".ostro-probe-")
        .tempfile_in(dir)
        .is_ok();
    ShmProbe {
        exists,
        writable,
        free_bytes: free_bytes(dir),
    }
}

/// Bytes available to an unprivileged process on the filesystem holding `dir`.
#[cfg(unix)]
fn free_bytes(dir: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: `c` is a valid NUL-terminated path that outlives the call; `statvfs` only
    // writes a `struct statvfs` through the pointer, which points at a zero-initialised,
    // properly aligned `MaybeUninit<statvfs>` owned by this frame. On success (0) every
    // field is written, and a zeroed `statvfs` is a valid value anyway.
    let st = unsafe {
        let mut st = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
        (libc::statvfs(c.as_ptr(), st.as_mut_ptr()) == 0).then(|| st.assume_init())
    }?;
    #[allow(clippy::useless_conversion)] // the field widths differ between platforms
    let (avail, frsize) = (u64::from(st.f_bavail), u64::from(st.f_frsize));
    Some(avail.saturating_mul(frsize))
}

#[cfg(not(unix))]
fn free_bytes(_dir: &Path) -> Option<u64> {
    None
}

/// Test hook: the shm candidate and threshold to use instead of
/// ([`SHM_DIR`], [`MIN_SHM_FREE_BYTES`]). Process-wide.
static OVERRIDE: Mutex<Option<(PathBuf, u64)>> = Mutex::new(None);

/// Tests only: make every later ephemeral start in this process probe `candidate` with
/// threshold `min_free` (`None` restores the defaults). Set it from a test binary of its
/// own: it affects every server the process starts.
#[doc(hidden)]
pub fn set_shm_candidate_for_tests(candidate: Option<(PathBuf, u64)>) {
    *OVERRIDE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = candidate;
}

fn candidate() -> (PathBuf, u64) {
    OVERRIDE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .unwrap_or_else(|| (PathBuf::from(SHM_DIR), MIN_SHM_FREE_BYTES))
}

/// Whether an ephemeral start in this process would put its directory on shm now.
/// Free space can change before the start; tests use this to decide what to assert.
pub fn would_use_shm() -> bool {
    let (dir, min) = candidate();
    choose(probe(&dir), min) == BaseChoice::Shm
}

/// Create the ephemeral data directory (`ostro-*` under the chosen base) and log one line
/// saying where and why.
pub fn create_dir() -> std::io::Result<TempDir> {
    let (shm, min) = candidate();
    let mut builder = tempfile::Builder::new();
    builder.prefix(PREFIX);
    let tmp = std::env::temp_dir();
    match choose(probe(&shm), min) {
        BaseChoice::Shm => match builder.tempdir_in(&shm) {
            Ok(dir) => {
                info!(
                    "ephemeral data dir on {} (tmpfs with at least {} MiB free)",
                    shm.display(),
                    min / (1024 * 1024)
                );
                return Ok(dir);
            }
            Err(e) => warn!(
                error = %e,
                "ephemeral data dir in {}: creating it on {} failed",
                tmp.display(),
                shm.display()
            ),
        },
        BaseChoice::TempDir(why) => info!(
            "ephemeral data dir in {} (not {}: {why})",
            tmp.display(),
            shm.display()
        ),
    }
    builder.tempdir()
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn p(exists: bool, writable: bool, free_bytes: Option<u64>) -> ShmProbe {
        ShmProbe {
            exists,
            writable,
            free_bytes,
        }
    }

    #[test]
    fn selection_table() {
        let min = MIN_SHM_FREE_BYTES;
        assert_eq!(choose(p(true, true, Some(min)), min), BaseChoice::Shm);
        assert_eq!(
            choose(p(true, true, Some(16 * 1024 * MIB)), min),
            BaseChoice::Shm
        );
        // Docker's default 64 MB shm.
        assert_eq!(
            choose(p(true, true, Some(64_000_000)), min),
            BaseChoice::TempDir(Fallback::TooSmall {
                free: 64_000_000,
                min
            })
        );
        assert_eq!(
            choose(p(true, true, Some(min - 1)), min),
            BaseChoice::TempDir(Fallback::TooSmall { free: min - 1, min })
        );
        assert_eq!(
            choose(p(true, true, None), min),
            BaseChoice::TempDir(Fallback::FreeUnknown)
        );
        // Not writable wins over free space; missing wins over everything.
        assert_eq!(
            choose(p(true, false, Some(u64::MAX)), min),
            BaseChoice::TempDir(Fallback::NotWritable)
        );
        assert_eq!(
            choose(p(false, true, Some(u64::MAX)), min),
            BaseChoice::TempDir(Fallback::Missing)
        );
        assert_eq!(
            choose(p(false, false, None), min),
            BaseChoice::TempDir(Fallback::Missing)
        );
    }

    #[test]
    fn reasons_read_as_one_line() {
        assert_eq!(
            Fallback::TooSmall {
                free: 64 * MIB,
                min: 512 * MIB
            }
            .to_string(),
            "only 64 MiB free, 512 MiB needed"
        );
        assert_eq!(Fallback::Missing.to_string(), "it does not exist");
    }

    #[test]
    fn probe_real_dirs() {
        let d = tempfile::tempdir().expect("tempdir");
        let got = probe(d.path());
        assert!(got.exists && got.writable, "{got:?}");
        if cfg!(unix) {
            assert!(got.free_bytes.is_some_and(|b| b > 0), "{got:?}");
        }
        // The probe file is gone.
        assert_eq!(std::fs::read_dir(d.path()).expect("ls").count(), 0);

        let missing = probe(&d.path().join("nope"));
        assert_eq!(missing, p(false, false, None));
    }
}
