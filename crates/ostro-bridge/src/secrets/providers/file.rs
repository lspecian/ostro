//! The file provider: secret `NAME` is the content of `<file_dir>/NAME` (Docker secrets,
//! mounted Kubernetes secrets).
//!
//! - The file is read on every call, so rotated mounts take effect without a restart.
//! - Names are validated first (template grammar: no `..`, no empty or `.` segments, no
//!   `__ostro/`), and the canonical path must stay inside the canonical `file_dir`: a symlink
//!   that resolves outside is refused (`ProviderError::Invalid`).
//! - A single trailing `\n` is stripped (files written with `echo` end with one); any other
//!   trailing whitespace, including `\r\n`'s `\r`, is kept.
//! - A missing file is `None`; other I/O errors are `Unavailable` (path and error kind only).

use std::path::{Path, PathBuf};

use secrecy::SecretBox;

use super::{ProviderError, SecretProvider};
use crate::secrets::template;
use crate::secrets::value::SecretValue;

/// Reads one file per secret under a directory.
#[derive(Clone, Debug)]
pub struct FileProvider {
    dir: PathBuf,
}

impl FileProvider {
    /// A provider over `dir`.
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self { dir: dir.into() }
    }

    /// The directory.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

fn read(dir: &Path, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
    template::validate_name(name).map_err(|e| ProviderError::Invalid(e.to_string()))?;
    let unavailable = |what: &str, p: &Path, e: std::io::Error| {
        ProviderError::Unavailable(format!("{what} {}: {}", p.display(), e.kind()))
    };
    let root = match std::fs::canonicalize(dir) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(unavailable("file_dir", dir, e)),
    };
    let path = root.join(name);
    let real = match std::fs::canonicalize(&path) {
        Ok(r) => r,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(unavailable("secret file", &path, e)),
    };
    if !real.starts_with(&root) {
        return Err(ProviderError::Invalid(format!(
            "secret file for {name:?} resolves outside file_dir"
        )));
    }
    if !real.is_file() {
        return Err(ProviderError::Invalid(format!(
            "secret path for {name:?} is not a regular file"
        )));
    }
    let mut bytes = std::fs::read(&real).map_err(|e| unavailable("secret file", &path, e))?;
    if bytes.last() == Some(&b'\n') {
        bytes.pop();
    }
    Ok(Some(SecretValue::new(bytes).into_box()))
}

#[async_trait::async_trait]
impl SecretProvider for FileProvider {
    fn name(&self) -> &'static str {
        "file"
    }

    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        let dir = self.dir.clone();
        let name = name.to_owned();
        tokio::task::spawn_blocking(move || read(&dir, &name))
            .await
            .map_err(|e| ProviderError::Unavailable(format!("file provider task failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    #[tokio::test]
    async fn reads_strips_one_newline_and_rereads() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("stripe")).unwrap();
        std::fs::write(dir.path().join("stripe/key"), b"canary-file-value\n\n").unwrap();
        let p = FileProvider::new(dir.path());
        let v = p.get("stripe/key").await.unwrap().unwrap();
        assert_eq!(v.expose_secret(), b"canary-file-value\n");
        std::fs::write(dir.path().join("stripe/key"), b"rotated-value-123").unwrap();
        let v = p.get("stripe/key").await.unwrap().unwrap();
        assert_eq!(v.expose_secret(), b"rotated-value-123");
        assert!(p.get("missing").await.unwrap().is_none());
        assert!(
            FileProvider::new(dir.path().join("nope"))
                .get("k")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn refuses_traversal_dirs_and_reserved() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("sub")).unwrap();
        let p = FileProvider::new(dir.path());
        for bad in ["../etc/passwd", "a/../../x", "__ostro/token-key", "a//b"] {
            assert!(
                matches!(p.get(bad).await, Err(ProviderError::Invalid(_))),
                "{bad}"
            );
        }
        assert!(matches!(p.get("sub").await, Err(ProviderError::Invalid(_))));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlink_outside_dir_is_refused_inside_is_ok() {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("target"), b"outside-secret-value").unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::os::unix::fs::symlink(outside.path().join("target"), dir.path().join("escape"))
            .unwrap();
        std::fs::write(dir.path().join("real"), b"inside-secret-value").unwrap();
        std::os::unix::fs::symlink(dir.path().join("real"), dir.path().join("alias")).unwrap();
        let p = FileProvider::new(dir.path());
        let err = p.get("escape").await.unwrap_err();
        assert!(matches!(err, ProviderError::Invalid(_)));
        assert!(!err.to_string().contains("outside-secret"));
        assert_eq!(
            p.get("alias").await.unwrap().unwrap().expose_secret(),
            b"inside-secret-value"
        );
    }
}
