//! Safe archive extraction (.tar.gz, .tar.xz, single binary, .whl verify-only): rejects
//! absolute paths, `..` entries, escaping symlinks (checked against the canonicalised on-disk
//! parent) and escaping hardlinks (resolved against the archive root).
//!
//! Only ever called on an artefact whose sha256 already matched its pin.
//!
//! Owner: 10-07.

use std::fs;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use crate::toolchain::{ArchiveKind, ToolchainError};

/// Install the verified artefact `archive` into the (existing, empty) dir `dest`.
///
/// - tarballs are extracted entry by entry (see the module docs), `strip` dropping a leading
///   directory;
/// - a binary is copied to `dest/<file_name>` with mode 0755;
/// - a wheel is copied to `dest/<file_name>` unchanged.
pub fn extract(
    archive: &Path,
    kind: ArchiveKind,
    dest: &Path,
    strip: Option<&str>,
    file_name: &str,
) -> Result<(), ToolchainError> {
    match kind {
        ArchiveKind::TarGz => {
            let f = open(archive)?;
            unpack_tar(
                flate2::read::GzDecoder::new(std::io::BufReader::new(f)),
                dest,
                strip,
            )
        }
        ArchiveKind::TarXz => {
            let f = open(archive)?;
            unpack_tar(
                lzma_rust2::XzReader::new(std::io::BufReader::new(f), true),
                dest,
                strip,
            )
        }
        ArchiveKind::Binary | ArchiveKind::Wheel => {
            let name = safe_file_name(file_name)?;
            let target = dest.join(name);
            fs::copy(archive, &target)
                .map_err(|e| ToolchainError::io(format!("copy to {}", target.display()), e))?;
            if kind == ArchiveKind::Binary {
                set_mode(&target, 0o755)?;
            }
            Ok(())
        }
    }
}

fn open(p: &Path) -> Result<fs::File, ToolchainError> {
    fs::File::open(p).map_err(|e| ToolchainError::io(format!("open {}", p.display()), e))
}

fn safe_file_name(name: &str) -> Result<&str, ToolchainError> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(unsafe_entry(name, "not a plain file name"));
    }
    Ok(name)
}

fn unsafe_entry(entry: impl Into<String>, reason: impl Into<String>) -> ToolchainError {
    ToolchainError::UnsafeArchive {
        entry: entry.into(),
        reason: reason.into(),
    }
}

/// The normal components of `p`; absolute paths and `..` are rejected.
fn relative_components(display: &str, p: &Path) -> Result<PathBuf, ToolchainError> {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::Normal(n) => out.push(n),
            Component::CurDir => {}
            Component::ParentDir => return Err(unsafe_entry(display, "`..` component")),
            Component::RootDir | Component::Prefix(_) => {
                return Err(unsafe_entry(display, "absolute path"));
            }
        }
    }
    Ok(out)
}

/// Lexical normalisation (`.` dropped, `..` pops); `None` when `..` climbs above the start.
fn normalize(p: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            Component::ParentDir => {
                if !out.pop() {
                    return None;
                }
            }
            Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    Some(out)
}

fn canonical(p: &Path) -> Result<PathBuf, ToolchainError> {
    fs::canonicalize(p).map_err(|e| ToolchainError::io(format!("canonicalize {}", p.display()), e))
}

fn unpack_tar<R: Read>(reader: R, dest: &Path, strip: Option<&str>) -> Result<(), ToolchainError> {
    let root = canonical(dest)?;
    let mut ar = tar::Archive::new(reader);
    ar.set_preserve_permissions(true);
    ar.set_preserve_mtime(true);
    ar.set_unpack_xattrs(false);
    ar.set_overwrite(true);
    let entries = ar
        .entries()
        .map_err(|e| ToolchainError::io("read archive", e))?;
    let strip_path = strip.map(Path::new);
    for entry in entries {
        let mut entry = entry.map_err(|e| ToolchainError::io("read archive entry", e))?;
        let raw = entry
            .path()
            .map_err(|e| ToolchainError::io("archive entry path", e))?
            .into_owned();
        let display = raw.to_string_lossy().into_owned();
        let mut rel = relative_components(&display, &raw)?;
        if let Some(prefix) = strip_path {
            rel = match rel.strip_prefix(prefix) {
                Ok(r) => r.to_path_buf(),
                Err(_) => {
                    return Err(unsafe_entry(
                        &display,
                        format!("outside the strip prefix {}", prefix.display()),
                    ));
                }
            };
        }
        let etype = entry.header().entry_type();
        if rel.as_os_str().is_empty() {
            // the archive root (or the stripped top dir) itself
            if etype.is_dir() {
                continue;
            }
            return Err(unsafe_entry(&display, "non-directory at the archive root"));
        }
        let target = root.join(&rel);
        let parent = target.parent().unwrap_or(&root).to_path_buf();
        fs::create_dir_all(&parent)
            .map_err(|e| ToolchainError::io(format!("create {}", parent.display()), e))?;
        let canon_parent = canonical(&parent)?;
        if !canon_parent.starts_with(&root) {
            return Err(unsafe_entry(
                &display,
                "parent directory resolves outside the install dir",
            ));
        }
        let target = canon_parent.join(rel.file_name().expect("non-empty relative path"));
        match etype {
            t if t.is_dir() => {
                if let Ok(m) = fs::symlink_metadata(&target)
                    && !m.is_dir()
                {
                    remove_existing(&target)?;
                }
                fs::create_dir_all(&target)
                    .map_err(|e| ToolchainError::io(format!("create {}", target.display()), e))?;
                if let Ok(mode) = entry.header().mode() {
                    set_mode(&target, (mode & 0o777) | 0o700)?;
                }
            }
            t if t.is_file() || t == tar::EntryType::Continuous => {
                remove_existing(&target)?;
                entry
                    .unpack(&target)
                    .map_err(|e| ToolchainError::io(format!("unpack {}", target.display()), e))?;
                // keep rwx bits only (no setuid/setgid/sticky from an archive)
                if let Ok(mode) = entry.header().mode() {
                    set_mode(&target, mode & 0o777)?;
                }
            }
            t if t.is_symlink() => {
                let link = link_target(&entry, &display)?;
                if link.is_absolute() {
                    return Err(unsafe_entry(&display, "absolute symlink target"));
                }
                if !normalize(&canon_parent.join(&link)).is_some_and(|r| r.starts_with(&root)) {
                    return Err(unsafe_entry(
                        &display,
                        "symlink target escapes the install dir",
                    ));
                }
                remove_existing(&target)?;
                make_symlink(&link, &target)?;
            }
            t if t.is_hard_link() => {
                let link = link_target(&entry, &display)?;
                let mut link_rel = relative_components(&display, &link).map_err(|_| {
                    unsafe_entry(&display, "hardlink target escapes the archive root")
                })?;
                if let Some(prefix) = strip_path {
                    link_rel = link_rel
                        .strip_prefix(prefix)
                        .map(Path::to_path_buf)
                        .map_err(|_| {
                            unsafe_entry(&display, "hardlink target outside the strip prefix")
                        })?;
                }
                let src = root.join(&link_rel);
                let canon_src = canonical(&src).map_err(|_| {
                    unsafe_entry(&display, "hardlink target does not exist in the archive")
                })?;
                if !canon_src.starts_with(&root) {
                    return Err(unsafe_entry(
                        &display,
                        "hardlink target escapes the archive root",
                    ));
                }
                remove_existing(&target)?;
                fs::hard_link(&canon_src, &target)
                    .map_err(|e| ToolchainError::io(format!("hardlink {}", target.display()), e))?;
            }
            tar::EntryType::XGlobalHeader | tar::EntryType::XHeader => {}
            other => {
                return Err(unsafe_entry(
                    &display,
                    format!("unsupported entry type {other:?}"),
                ));
            }
        }
    }
    Ok(())
}

fn link_target<R: Read>(
    entry: &tar::Entry<'_, R>,
    display: &str,
) -> Result<PathBuf, ToolchainError> {
    entry
        .link_name()
        .map_err(|e| ToolchainError::io("archive link name", e))?
        .map(|l| l.into_owned())
        .ok_or_else(|| unsafe_entry(display, "link without a target"))
}

/// Remove a non-directory at `p` (a symlink is removed, never followed).
fn remove_existing(p: &Path) -> Result<(), ToolchainError> {
    match fs::symlink_metadata(p) {
        Ok(m) if m.is_dir() => Err(unsafe_entry(
            p.to_string_lossy(),
            "entry replaces an existing directory",
        )),
        Ok(_) => {
            fs::remove_file(p).map_err(|e| ToolchainError::io(format!("remove {}", p.display()), e))
        }
        Err(_) => Ok(()),
    }
}

#[cfg(unix)]
fn make_symlink(link: &Path, at: &Path) -> Result<(), ToolchainError> {
    std::os::unix::fs::symlink(link, at)
        .map_err(|e| ToolchainError::io(format!("symlink {}", at.display()), e))
}

#[cfg(not(unix))]
fn make_symlink(_link: &Path, at: &Path) -> Result<(), ToolchainError> {
    Err(unsafe_entry(
        at.to_string_lossy(),
        "symlinks are unsupported on this platform",
    ))
}

#[cfg(unix)]
fn set_mode(p: &Path, mode: u32) -> Result<(), ToolchainError> {
    use std::os::unix::fs::PermissionsExt as _;
    fs::set_permissions(p, fs::Permissions::from_mode(mode))
        .map_err(|e| ToolchainError::io(format!("chmod {}", p.display()), e))
}

#[cfg(not(unix))]
fn set_mode(_p: &Path, _mode: u32) -> Result<(), ToolchainError> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_lexically() {
        assert_eq!(
            normalize(Path::new("/a/b/../c")),
            Some(PathBuf::from("/a/c"))
        );
        assert_eq!(normalize(Path::new("a/../..")), None);
        assert_eq!(normalize(Path::new("./a/./b")), Some(PathBuf::from("a/b")));
    }

    #[test]
    fn relative_components_reject() {
        assert!(relative_components("x", Path::new("/etc/passwd")).is_err());
        assert!(relative_components("x", Path::new("a/../b")).is_err());
        assert_eq!(
            relative_components("x", Path::new("./a/b")).unwrap(),
            PathBuf::from("a/b")
        );
    }
}
