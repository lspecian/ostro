//! Bottomless backup configuration contract (plan 08-01). The `[backup]` section of the server
//! config (RESERVATIONS §11, P8-D22) is parsed into it by 08-04; 08-13 and 08-18 consume it.
//!
//! Backup is off unless [`BackupConfig::url`] is set. Credentials never appear in `Debug`
//! output (hand-written impl below).

use std::path::PathBuf;
use std::time::Duration;

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
    /// The platform verifier / system store.
    #[default]
    Platform,
    /// The bundled webpki roots (for `FROM scratch` images without `/etc/ssl/certs`).
    Webpki,
    /// A PEM bundle at this path.
    File(PathBuf),
}

/// Bottomless backup settings.
#[derive(Clone, PartialEq, Eq)]
pub struct BackupConfig {
    /// `s3://bucket/prefix` or `file:///path`; `None` = backup off.
    pub url: Option<String>,
    /// Upload tick: each tick takes a consistent cut and seals chunks (default 1 s, P8-D3).
    pub seal_interval: Duration,
    /// S3 region.
    pub region: Option<String>,
    /// S3 endpoint override (MinIO, R2, ...).
    pub endpoint: Option<String>,
    /// Path- or virtual-hosted-style addressing.
    pub url_style: UrlStyle,
    /// Access key id (secret material: redacted in `Debug`).
    pub access_key_id: Option<String>,
    /// Secret access key (redacted in `Debug`).
    pub secret_access_key: Option<String>,
    /// Session token (redacted in `Debug`).
    pub session_token: Option<String>,
    /// Allow a plain `http://` endpoint (tests, local MinIO).
    pub allow_http: bool,
    /// TLS root source.
    pub tls_roots: TlsRoots,
    /// Remote snapshots kept per partition (default 3, P8-D3).
    pub remote_snapshots_keep: usize,
    /// Take over a prefix whose `LOCK` names another incarnation (`--takeover`, P8-D4).
    pub takeover: bool,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            url: None,
            seal_interval: Duration::from_secs(1),
            region: None,
            endpoint: None,
            url_style: UrlStyle::Path,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            allow_http: false,
            tls_roots: TlsRoots::Platform,
            remote_snapshots_keep: 3,
            takeover: false,
        }
    }
}

impl BackupConfig {
    /// Whether bottomless backup is configured (a `url` is set).
    pub fn enabled(&self) -> bool {
        self.url.is_some()
    }
}

fn redact(v: &Option<String>) -> Option<&'static str> {
    v.as_ref().map(|_| "<redacted>")
}

impl std::fmt::Debug for BackupConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackupConfig")
            .field("url", &self.url)
            .field("seal_interval", &self.seal_interval)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("url_style", &self.url_style)
            .field("access_key_id", &redact(&self.access_key_id))
            .field("secret_access_key", &redact(&self.secret_access_key))
            .field("session_token", &redact(&self.session_token))
            .field("allow_http", &self.allow_http)
            .field("tls_roots", &self.tls_roots)
            .field("remote_snapshots_keep", &self.remote_snapshots_keep)
            .field("takeover", &self.takeover)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_p8_d22() {
        let c = BackupConfig::default();
        assert!(!c.enabled());
        assert_eq!(c.seal_interval, Duration::from_secs(1));
        assert_eq!(c.remote_snapshots_keep, 3);
        assert_eq!(c.url_style, UrlStyle::Path);
        assert_eq!(c.tls_roots, TlsRoots::Platform);
        assert!(!c.allow_http && !c.takeover);
    }

    #[test]
    fn debug_redacts_credentials() {
        let c = BackupConfig {
            url: Some("s3://bucket/prefix".into()),
            access_key_id: Some("AKIACANARY0000".into()),
            secret_access_key: Some("canary-secret-key".into()),
            session_token: Some("canary-session".into()),
            ..BackupConfig::default()
        };
        let s = format!("{c:?}");
        for secret in ["AKIACANARY0000", "canary-secret-key", "canary-session"] {
            assert!(!s.contains(secret), "{secret} leaked: {s}");
        }
        assert!(s.contains("<redacted>") && s.contains("s3://bucket/prefix"));
    }
}
