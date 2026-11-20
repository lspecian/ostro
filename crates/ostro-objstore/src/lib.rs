//! ostro-objstore: the shared object-store abstraction (D19, P8-D5).
//!
//! One object-safe [`ObjectStore`] trait with two backends:
//! - [`LocalFs`] (`file:///abs/path`): durable writes (tmp + `sync_all` + rename + dir fsync),
//!   conditional puts emulated atomically under a per-root lock, etag = hex SHA-256;
//! - [`S3Store`] (`s3://bucket/prefix`): any S3-compatible service through `rusty-s3` 0.12
//!   (sans-IO SigV4) and `reqwest` over rustls, with retries on 5xx/429/timeouts.
//!
//! Created empty by plan 08-01 (Phase 8 ran first); 08-03 defined and **froze** the trait
//! below, matching 06-02. Phase 6 (06-12) adds only `put_with`/`presign`, each with a default
//! impl, so neither phase breaks the other's implementors. No `object_store`, no chrono.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_core::stream::BoxStream;
use futures_util::StreamExt as _;

mod error;
mod local;
mod s3;
mod url;

pub use error::{Error, Result};
pub use local::LocalFs;
pub use s3::S3Store;
pub use url::open_url;

/// Metadata of a stored object.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectMeta {
    /// The key, relative to the store's root/prefix.
    pub key: String,
    /// Size in bytes.
    pub size: u64,
    /// Last modification time reported by the backend.
    pub last_modified: jiff::Timestamp,
    /// Opaque entity tag (S3: quoted as the server sent it; LocalFs: hex SHA-256). `None` when
    /// the backend does not report one for this call (LocalFs `list`).
    pub e_tag: Option<String>,
    /// Object version id, when the backend has versioning.
    pub version: Option<String>,
}

/// Result of a successful write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutOutcome {
    /// Entity tag of the written object, when the backend reported one.
    pub e_tag: Option<String>,
}

/// Outcome of a conditional write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Conditional<T> {
    /// The precondition held and the write happened.
    Ok(T),
    /// The precondition failed (S3 412, or 409 for a conflicting conditional write); nothing
    /// was written.
    PreconditionFailed,
}

impl<T> Conditional<T> {
    /// The value when the precondition held.
    pub fn ok(self) -> Option<T> {
        match self {
            Self::Ok(v) => Some(v),
            Self::PreconditionFailed => None,
        }
    }

    /// Whether the precondition failed.
    pub fn is_precondition_failed(&self) -> bool {
        matches!(self, Self::PreconditionFailed)
    }
}

/// The object-store contract (frozen by 08-03; Phase 6 adds only defaulted methods).
///
/// Keys are `/`-separated UTF-8 strings without leading `/`, empty segments, `.`/`..`
/// segments or NUL. Both backends reject keys outside this set.
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    /// Unconditional write (create or replace).
    async fn put(&self, key: &str, body: Bytes) -> Result<PutOutcome>;

    /// Create only if absent (`If-None-Match: *`).
    async fn put_if_absent(&self, key: &str, body: Bytes) -> Result<Conditional<PutOutcome>>;

    /// Replace only if the current entity tag equals `e_tag` (`If-Match`). A missing object
    /// is a failed precondition.
    async fn put_if_match(
        &self,
        key: &str,
        body: Bytes,
        e_tag: &str,
    ) -> Result<Conditional<PutOutcome>>;

    /// Whole-object read; `None` if absent.
    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>>;

    /// Byte-range read. `range.end` is clamped to the object size; `start == end` returns
    /// empty. A missing object is [`Error::NotFound`]; `start > end`, or `start >= size` on a
    /// non-empty range, is [`Error::InvalidRange`].
    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes>;

    /// Metadata only; `None` if absent.
    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>>;

    /// Every object whose key starts with `prefix` and is strictly greater than `start_after`,
    /// in lexicographic (byte) key order. Paginates internally.
    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<ObjectMeta>>;

    /// Delete; idempotent (deleting a missing key succeeds).
    async fn delete(&self, key: &str) -> Result<()>;

    /// Delete several keys; idempotent per key.
    async fn delete_many(&self, keys: &[String]) -> Result<()>;

    /// Streaming write. Object-safe (B1): the body is a boxed stream, never `impl Stream`.
    ///
    /// The default collects the stream and calls [`ObjectStore::put`]. S3 overrides it with
    /// CreateMultipartUpload/UploadPart/Complete for bodies of at least the multipart
    /// threshold (64 MiB by default; Abort on error); LocalFs streams to a tmp file.
    async fn put_multipart(
        &self,
        key: &str,
        mut body: BoxStream<'static, Result<Bytes>>,
    ) -> Result<PutOutcome> {
        let mut buf = BytesMut::new();
        while let Some(chunk) = body.next().await {
            buf.extend_from_slice(&chunk?);
        }
        self.put(key, buf.freeze()).await
    }

    /// Whether `put_if_absent`/`put_if_match` are enforced by the backend. `false` means
    /// callers use the LOCK re-read fallback (P8-D4).
    fn supports_conditional_put(&self) -> bool;
}

/// S3 URL addressing style.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum UrlStyle {
    /// `https://endpoint/bucket/key` (MinIO, R2 and most S3-compatible stores).
    #[default]
    Path,
    /// `https://bucket.endpoint/key` (AWS virtual-hosted style).
    Virtual,
}

/// Where TLS root certificates come from (P8-D5 `tls_roots` knob).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum TlsRoots {
    /// The platform verifier / system store (reqwest's rustls default).
    #[default]
    Platform,
    /// Bundled webpki roots. Not available in this build: the bundle is CDLA-licensed
    /// (D-FX2), so selecting it is a configuration error that points at `File`.
    Webpki,
    /// Only the roots in this PEM bundle.
    File(std::path::PathBuf),
}

/// S3 credentials. `Debug` redacts every field.
#[derive(Clone, PartialEq, Eq)]
pub struct Credentials {
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Optional session token (STS).
    pub session_token: Option<String>,
}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

/// Transport options for [`open_url`] / [`S3Store::new`]. The fields mirror the transport
/// fields of `ostro_backup::config::BackupConfig` (08-13 maps one onto the other); this crate
/// does not depend on ostro-backup. LocalFs ignores everything but `conditional_put`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoreOptions {
    /// Region (default `us-east-1`).
    pub region: Option<String>,
    /// Endpoint override (MinIO, R2, GCS interop). Default: `https://s3.<region>.amazonaws.com`.
    pub endpoint: Option<String>,
    /// Path- or virtual-hosted-style addressing.
    pub url_style: UrlStyle,
    /// Credentials; `None` sends unsigned requests (public buckets).
    pub credentials: Option<Credentials>,
    /// Allow a plain `http://` endpoint (tests, local MinIO). Required for `http://`.
    pub allow_http: bool,
    /// TLS root source.
    pub tls_roots: TlsRoots,
    /// Use the backend's conditional writes (default `true`). `false` makes
    /// [`ObjectStore::supports_conditional_put`] report `false`, forcing callers onto the
    /// LOCK re-read fallback (P8-D4) for providers without `If-Match` on PUT.
    pub conditional_put: bool,
    /// `put_multipart` bodies at least this large use S3 multipart upload (default 64 MiB).
    pub multipart_threshold: u64,
    /// Multipart part size (default 16 MiB; S3 requires at least 5 MiB except the last part).
    pub multipart_part_size: u64,
    /// Retries after the first attempt on 5xx/429/timeouts (default 5).
    pub max_retries: u32,
    /// Base delay of the exponential backoff (default 100 ms; full jitter, capped at 5 s).
    pub retry_base: std::time::Duration,
}

/// Default multipart threshold (64 MiB).
pub const DEFAULT_MULTIPART_THRESHOLD: u64 = 64 << 20;
/// Default multipart part size (16 MiB).
pub const DEFAULT_MULTIPART_PART_SIZE: u64 = 16 << 20;
/// S3's minimum part size for every part but the last (5 MiB).
pub const MIN_MULTIPART_PART_SIZE: u64 = 5 << 20;

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            region: None,
            endpoint: None,
            url_style: UrlStyle::Path,
            credentials: None,
            allow_http: false,
            tls_roots: TlsRoots::Platform,
            conditional_put: true,
            multipart_threshold: DEFAULT_MULTIPART_THRESHOLD,
            multipart_part_size: DEFAULT_MULTIPART_PART_SIZE,
            max_retries: 5,
            retry_base: std::time::Duration::from_millis(100),
        }
    }
}

/// Validate a key against the common key grammar (see [`ObjectStore`]).
pub(crate) fn check_key(key: &str) -> Result<()> {
    let bad = |reason| {
        Err(Error::InvalidKey {
            key: key.to_owned(),
            reason,
        })
    };
    if key.is_empty() {
        return bad("empty");
    }
    if key.contains('\0') {
        return bad("contains NUL");
    }
    if key.starts_with('/') {
        return bad("absolute (leading '/')");
    }
    for seg in key.split('/') {
        match seg {
            "" => return bad("empty path segment"),
            "." | ".." => return bad("'.' or '..' segment"),
            _ => {}
        }
    }
    Ok(())
}

/// Compile-time object-safety check (B1).
#[allow(dead_code)]
fn _assert_object_safe(_: Arc<dyn ObjectStore>) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_grammar() {
        for ok in [
            "a",
            "a/b",
            "p/0001/seg/00000001",
            "LOCK",
            "x.y",
            "..a",
            "a..b",
        ] {
            check_key(ok).unwrap_or_else(|e| panic!("{ok}: {e}"));
        }
        for bad in [
            "", "/a", "a//b", "a/", "../a", "a/../b", "a/./b", ".", "a\0b",
        ] {
            assert!(check_key(bad).is_err(), "{bad:?} accepted");
        }
    }

    #[test]
    fn credentials_debug_redacted() {
        let c = Credentials {
            access_key_id: "AKIACANARY".into(),
            secret_access_key: "canary-secret".into(),
            session_token: Some("canary-token".into()),
        };
        let s = format!(
            "{:?}",
            StoreOptions {
                credentials: Some(c),
                ..StoreOptions::default()
            }
        );
        for secret in ["AKIACANARY", "canary-secret", "canary-token"] {
            assert!(!s.contains(secret), "{secret} leaked: {s}");
        }
    }
}
