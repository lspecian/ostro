//! Filesystem helpers for storage tests.

use std::path::{Path, PathBuf};

/// Temporary directory under `std::env::temp_dir()`.
///
/// Panics on Linux if the directory is under `/mnt/` (WSL2 9p), where fsync
/// semantics are meaningless.
pub fn tempdir() -> tempfile::TempDir {
    let dir = tempfile::Builder::new()
        .prefix("ostro-test-")
        .tempdir_in(std::env::temp_dir())
        .expect("create temp dir");
    #[cfg(target_os = "linux")]
    assert!(
        !dir.path().starts_with("/mnt/"),
        "tests must run on a Linux filesystem, not WSL2 9p (/mnt): fsync semantics are meaningless there (temp dir: {})",
        dir.path().display()
    );
    dir
}

/// Length of the file at `path` in bytes (panics if it cannot be stat'ed).
pub fn file_len(path: &Path) -> u64 {
    std::fs::metadata(path)
        .unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
        .len()
}

/// Files directly in `dir` whose names start with `prefix` and end with `suffix`, sorted by name.
pub fn list_files_sorted(dir: &Path, prefix: &str, suffix: &str) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        .map(|e| e.expect("dir entry"))
        .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(prefix) && n.ends_with(suffix))
        })
        .map(|e| e.path())
        .collect();
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tempdir_is_not_on_9p() {
        let d = tempdir();
        assert!(d.path().is_dir());
        assert!(!d.path().starts_with("/mnt/"));
    }

    #[test]
    fn file_len_and_sorted_listing() {
        let d = tempdir();
        for name in ["seg-0002.wal", "seg-0001.wal", "other.wal", "seg-0003.tmp"] {
            std::fs::write(d.path().join(name), b"abc").unwrap();
        }
        std::fs::create_dir(d.path().join("seg-0000.wal")).unwrap();
        assert_eq!(file_len(&d.path().join("other.wal")), 3);
        let got = list_files_sorted(d.path(), "seg-", ".wal");
        let names: Vec<_> = got
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_owned())
            .collect();
        assert_eq!(names, ["seg-0001.wal", "seg-0002.wal"]);
    }
}
