//! The SDK sources embedded at build time (`build.rs` writes `$OUT_DIR/sdk_bundle.tar`),
//! their bundle id and materialisation into `target/ostro/sdk/` (D-P10-6).
//!
//! Owner: 10-08.
//!
//! The bundle layout is documented in `build.rs`: `rust/{ostro-sdk,ostro-sdk-macros,
//! ostro-client}`, `python/ostro`, `ts`, `toolchain` and `clients/{python,ts}`.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

/// The embedded SDK bundle (a deterministic tar archive written by `build.rs`).
pub const BUNDLE: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/sdk_bundle.tar"));

/// Alias of [`BUNDLE`] (the 10-02 contract name).
pub const SDK_BUNDLE: &[u8] = BUNDLE;

/// The name of the marker file [`materialize`] writes into its destination.
pub const BUNDLE_ID_FILE: &str = ".bundle-id";

/// The full sha256 (lowercase hex) of [`BUNDLE`].
pub fn bundle_sha256() -> String {
    hex(&Sha256::digest(BUNDLE))
}

/// The SDK bundle id: the first 16 hex digits of [`bundle_sha256`].
pub fn bundle_id() -> String {
    let mut s = bundle_sha256();
    s.truncate(16);
    s
}

/// The bundle's entry paths, in archive order (sorted).
pub fn entries() -> io::Result<Vec<String>> {
    let mut archive = tar::Archive::new(BUNDLE);
    let mut out = Vec::new();
    for e in archive.entries()? {
        let e = e?;
        out.push(e.path()?.to_string_lossy().into_owned());
    }
    Ok(out)
}

/// The bytes of one bundled file, `None` if the bundle has no such entry.
pub fn read_entry(path: &str) -> io::Result<Option<Vec<u8>>> {
    let mut archive = tar::Archive::new(BUNDLE);
    for e in archive.entries()? {
        let mut e = e?;
        if e.path()?.to_string_lossy() == path {
            let mut buf = Vec::with_capacity(e.size() as usize);
            io::Read::read_to_end(&mut e, &mut buf)?;
            return Ok(Some(buf));
        }
    }
    Ok(None)
}

/// Extract the bundle into `dest` (normally `<project>/target/ostro/sdk`).
///
/// Idempotent: when `dest/.bundle-id` already holds [`bundle_id`] nothing is touched.
/// Otherwise `dest` is replaced as a whole: the bundle is extracted into a sibling
/// temporary directory, `.bundle-id` is written last and the directory is renamed into
/// place. Extracted files get the current time as mtime (not the archive's 0), so cargo
/// rebuilds a path dependency whose sources changed with a new bundle.
pub fn materialize(dest: &Path) -> io::Result<()> {
    let id = bundle_id();
    if is_current(dest, &id) {
        return Ok(());
    }
    let parent = dest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    fs::create_dir_all(&parent)?;
    let leaf = dest
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "sdk".to_owned());
    let tmp = parent.join(format!(".{leaf}.tmp-{}", std::process::id()));
    if tmp.exists() {
        fs::remove_dir_all(&tmp)?;
    }
    fs::create_dir_all(&tmp)?;
    let result = (|| {
        let mut archive = tar::Archive::new(BUNDLE);
        archive.set_preserve_mtime(false);
        archive.set_preserve_permissions(true);
        for e in archive.entries()? {
            let mut e = e?;
            if !e.unpack_in(&tmp)? {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsafe path in the SDK bundle: {}", e.path()?.display()),
                ));
            }
        }
        fs::write(tmp.join(BUNDLE_ID_FILE), format!("{id}\n"))?;
        if dest.exists() {
            fs::remove_dir_all(dest)?;
        }
        fs::rename(&tmp, dest)
    })();
    if let Err(e) = result {
        let _ = fs::remove_dir_all(&tmp);
        // A concurrent materialisation of the same bundle may have won the rename.
        if is_current(dest, &id) {
            return Ok(());
        }
        return Err(e);
    }
    Ok(())
}

fn is_current(dest: &Path, id: &str) -> bool {
    fs::read_to_string(dest.join(BUNDLE_ID_FILE)).is_ok_and(|s| s.trim() == id)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
