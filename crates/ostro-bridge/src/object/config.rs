//! The `object` connection config (v1).
//!
//! ```json
//! {"v":1, "backend":"s3", "endpoint":"https://s3.eu-west-1.amazonaws.com", "bucket":"files",
//!  "region":"eu-west-1", "url_style":"path", "allow_http":false, "prefix":"tenant-a",
//!  "credentials":{"access_key_id":"{{secret:s3-ak}}", "secret_access_key":"{{secret:s3-sk}}",
//!                 "session_token":"{{secret:s3-token}}"},
//!  "presign_expiry":"15m", "max_get_bytes":16777216, "max_put_bytes":67108864,
//!  "content_type":"application/octet-stream", "rate":"100/s", "burst":100}
//! {"v":1, "backend":"local", "root":"/var/lib/ostro/objects", "prefix":"a"}
//! ```
//!
//! - `credentials.secret_access_key` and `session_token` must contain `{{secret:NAME}}`
//!   placeholders (a literal would sit in plain text in the meta store); `access_key_id` may be
//!   literal or templated. They resolve through
//!   [`SecretResolver::resolve_operator`](crate::secrets::SecretResolver::resolve_operator)
//!   when the backend is built. This struct holds **templates only**, never values.
//! - `presign_expiry` defaults to `[connectors] presign_default_expiry` and must lie within
//!   1 s ..= 168 h (S3's limit; write hours, not days). `max_get_bytes` defaults to
//!   `[connectors] object_max_get_bytes`, `max_put_bytes` to 64 MiB.
//! - `content_type` is sent with every `put` (and signed into `presign-put` URLs, so the
//!   uploader must send it): `object-request` has no content-type field in WIT 0.1.0.
//! - `prefix` is prepended to every guest key (`<prefix>/<key>`).
//!
//! Unknown keys, a missing or unknown `"v"` and invalid values are `connection_unknown`, so a
//! typo can never fall back to a default silently.

use std::path::PathBuf;
use std::time::Duration;

use ostro_meta::records::Connection;
use ostro_types::journal::Failure;
use serde::Deserialize;

use crate::config::{ConnectorsConfig, parse_bytes, parse_duration};
use crate::kinds::CONNECTION_UNKNOWN;
use crate::outcome::non_retryable;
use crate::ratelimit::QuotaSpec;
use crate::secrets::template;

/// Connection kind of an object-store connection.
pub const OBJECT_KIND: &str = "object";
/// Default `max_put_bytes` (64 MiB).
pub const DEFAULT_MAX_PUT_BYTES: u64 = 64 << 20;

/// Credential **templates** of an S3 backend. `Debug` prints only which fields are set.
#[derive(Clone, PartialEq, Eq)]
pub struct CredentialTemplates {
    /// Access key id (literal or `{{secret:..}}`).
    pub access_key_id: String,
    /// Secret access key template (`{{secret:..}}`).
    pub secret_access_key: String,
    /// Session token template (`{{secret:..}}`).
    pub session_token: Option<String>,
}

impl std::fmt::Debug for CredentialTemplates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialTemplates")
            .field("access_key_id", &"<template>")
            .field("secret_access_key", &"<template>")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "<template>"),
            )
            .finish()
    }
}

/// The backend of an object connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendConfig {
    /// S3-compatible store (AWS, SeaweedFS, R2, Ceph; GCS via XML interop, unverified).
    S3 {
        /// Endpoint override (default `https://s3.<region>.amazonaws.com`).
        endpoint: Option<String>,
        /// Bucket name.
        bucket: String,
        /// Region (default `us-east-1`).
        region: Option<String>,
        /// Virtual-hosted-style addressing (default path style).
        virtual_host: bool,
        /// Permit a plain `http://` endpoint.
        allow_http: bool,
        /// Credential templates; `None` = unsigned requests (public bucket).
        credentials: Option<CredentialTemplates>,
    },
    /// Local filesystem under an absolute root (presign through the Ostro HMAC route).
    Local {
        /// Absolute root directory.
        root: PathBuf,
    },
}

/// A parsed object connection config.
#[derive(Clone, Debug, PartialEq)]
pub struct ObjectConnConfig {
    /// The backend.
    pub backend: BackendConfig,
    /// Key prefix without leading/trailing `/` (empty = none).
    pub prefix: String,
    /// Validity of presigned URLs.
    pub presign_expiry: Duration,
    /// Largest object `get` returns.
    pub max_get_bytes: u64,
    /// Largest body `put` accepts.
    pub max_put_bytes: u64,
    /// Content type sent with `put` and signed into `presign-put`.
    pub content_type: Option<String>,
    /// Rate-limit quota of the connection.
    pub quota: Option<QuotaSpec>,
}

impl ObjectConnConfig {
    /// The stored key of guest key `key` (`<prefix>/<key>`).
    pub fn full_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_owned()
        } else {
            format!("{}/{key}", self.prefix)
        }
    }
}

/// The connection config is invalid (`connection_unknown`).
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("connection {connection:?}: {reason}")]
pub struct ConfigError {
    /// Connection name.
    pub connection: String,
    /// What is wrong.
    pub reason: String,
}

impl ConfigError {
    /// `NonRetryable{connection_unknown}`.
    pub fn to_failure(&self) -> Failure {
        non_retryable(
            CONNECTION_UNKNOWN,
            self.to_string(),
            Some(serde_json::json!({ "connection": self.connection })),
        )
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCredentials {
    access_key_id: String,
    secret_access_key: String,
    session_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum SizeValue {
    Int(u64),
    Text(String),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Raw {
    v: u32,
    backend: String,
    endpoint: Option<String>,
    bucket: Option<String>,
    region: Option<String>,
    url_style: Option<String>,
    allow_http: Option<bool>,
    root: Option<PathBuf>,
    prefix: Option<String>,
    credentials: Option<RawCredentials>,
    presign_expiry: Option<String>,
    max_get_bytes: Option<SizeValue>,
    max_put_bytes: Option<SizeValue>,
    content_type: Option<String>,
    rate: Option<String>,
    burst: Option<u32>,
}

const MIN_EXPIRY: Duration = Duration::from_secs(1);
const MAX_EXPIRY: Duration = Duration::from_secs(7 * 24 * 3600);

impl ObjectConnConfig {
    /// Parses `conn` (which must have kind `object`) with the node defaults.
    pub fn parse(conn: &Connection, defaults: &ConnectorsConfig) -> Result<Self, ConfigError> {
        let err = |reason: String| ConfigError {
            connection: conn.name.clone(),
            reason,
        };
        if conn.kind != OBJECT_KIND {
            return Err(err(format!("kind {:?} is not {OBJECT_KIND:?}", conn.kind)));
        }
        let raw: Raw = serde_json::from_slice(&conn.config)
            .map_err(|e| err(format!("invalid object config: {e}")))?;
        if raw.v != 1 {
            return Err(err(format!("unsupported config version v={}", raw.v)));
        }
        let s3_only = |name: &str, set: bool| {
            if set {
                Err(err(format!("{name} is only valid for backend \"s3\"")))
            } else {
                Ok(())
            }
        };
        let backend = match raw.backend.as_str() {
            "s3" => {
                if raw.root.is_some() {
                    return Err(err("root is only valid for backend \"local\"".into()));
                }
                let bucket = raw
                    .bucket
                    .filter(|b| !b.is_empty())
                    .ok_or_else(|| err("backend \"s3\" needs a bucket".into()))?;
                let virtual_host = match raw.url_style.as_deref() {
                    None | Some("path") => false,
                    Some("virtual") => true,
                    Some(o) => return Err(err(format!("url_style {o:?} (path | virtual)"))),
                };
                let credentials = match raw.credentials {
                    None => None,
                    Some(c) => Some(check_credentials(c).map_err(err)?),
                };
                BackendConfig::S3 {
                    endpoint: raw.endpoint,
                    bucket,
                    region: raw.region,
                    virtual_host,
                    allow_http: raw.allow_http.unwrap_or(false),
                    credentials,
                }
            }
            "local" => {
                s3_only("endpoint", raw.endpoint.is_some())?;
                s3_only("bucket", raw.bucket.is_some())?;
                s3_only("region", raw.region.is_some())?;
                s3_only("url_style", raw.url_style.is_some())?;
                s3_only("allow_http", raw.allow_http.is_some())?;
                s3_only("credentials", raw.credentials.is_some())?;
                let root = raw
                    .root
                    .ok_or_else(|| err("backend \"local\" needs a root".into()))?;
                if !root.is_absolute() {
                    return Err(err(format!("root {} is not absolute", root.display())));
                }
                BackendConfig::Local { root }
            }
            o => return Err(err(format!("backend {o:?} (s3 | local)"))),
        };
        let prefix = raw.prefix.unwrap_or_default().trim_matches('/').to_owned();
        if prefix
            .split('/')
            .any(|seg| matches!(seg, "." | "..") || (seg.is_empty() && !prefix.is_empty()))
            || prefix.contains('\0')
        {
            return Err(err(format!("invalid prefix {prefix:?}")));
        }
        let presign_expiry = match raw.presign_expiry.as_deref() {
            None => defaults.presign_default_expiry,
            Some(s) => parse_duration(s).map_err(|e| err(format!("presign_expiry: {e}")))?,
        };
        if !(MIN_EXPIRY..=MAX_EXPIRY).contains(&presign_expiry) {
            return Err(err(format!(
                "presign_expiry {presign_expiry:?} outside 1s..=168h"
            )));
        }
        let size = |name: &str, v: Option<SizeValue>, default: u64| -> Result<u64, ConfigError> {
            let n = match v {
                None => default,
                Some(SizeValue::Int(n)) => n,
                Some(SizeValue::Text(s)) => {
                    parse_bytes(&s).map_err(|e| err(format!("{name}: {e}")))?
                }
            };
            if n == 0 {
                return Err(err(format!("{name} must be > 0")));
            }
            Ok(n)
        };
        let max_get_bytes = size(
            "max_get_bytes",
            raw.max_get_bytes,
            defaults.object_max_get_bytes,
        )?;
        let max_put_bytes = size("max_put_bytes", raw.max_put_bytes, DEFAULT_MAX_PUT_BYTES)?;
        if let Some(ct) = &raw.content_type
            && (ct.is_empty() || ct.chars().any(char::is_control) || !ct.is_ascii())
        {
            return Err(err(format!("invalid content_type {ct:?}")));
        }
        let quota = match (raw.rate.as_deref(), raw.burst) {
            (None, None) => None,
            (None, Some(_)) => return Err(err("burst needs rate".into())),
            (Some(r), b) => {
                Some(QuotaSpec::parse_with_burst(r, b).map_err(|e| err(format!("rate: {e}")))?)
            }
        };
        Ok(Self {
            backend,
            prefix,
            presign_expiry,
            max_get_bytes,
            max_put_bytes,
            content_type: raw.content_type,
            quota,
        })
    }
}

fn check_credentials(c: RawCredentials) -> Result<CredentialTemplates, String> {
    let templated = |name: &str, v: &str| -> Result<(), String> {
        let ph = template::parse(v).map_err(|e| format!("credentials.{name}: {e}"))?;
        if ph.is_empty() {
            return Err(format!(
                "credentials.{name} must be a {{{{secret:NAME}}}} template, not a literal"
            ));
        }
        Ok(())
    };
    template::parse(&c.access_key_id).map_err(|e| format!("credentials.access_key_id: {e}"))?;
    if c.access_key_id.is_empty() {
        return Err("credentials.access_key_id is empty".into());
    }
    templated("secret_access_key", &c.secret_access_key)?;
    if let Some(t) = &c.session_token {
        templated("session_token", t)?;
    }
    Ok(CredentialTemplates {
        access_key_id: c.access_key_id,
        secret_access_key: c.secret_access_key,
        session_token: c.session_token,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conn(cfg: serde_json::Value) -> Connection {
        Connection {
            name: "files".into(),
            kind: OBJECT_KIND.into(),
            config: cfg.to_string().into_bytes(),
            ext: vec![],
        }
    }

    fn parse(cfg: serde_json::Value) -> Result<ObjectConnConfig, ConfigError> {
        ObjectConnConfig::parse(&conn(cfg), &ConnectorsConfig::default())
    }

    #[test]
    fn s3_full_config() {
        let c = parse(serde_json::json!({
            "v": 1, "backend": "s3", "endpoint": "http://127.0.0.1:8333", "bucket": "b",
            "region": "us-east-1", "url_style": "virtual", "allow_http": true, "prefix": "/a/b/",
            "credentials": {"access_key_id": "AKIA", "secret_access_key": "{{secret:sk}}",
                            "session_token": "{{secret:tok}}"},
            "presign_expiry": "2h", "max_get_bytes": "1MiB", "max_put_bytes": 10,
            "content_type": "text/plain", "rate": "10/s", "burst": 5
        }))
        .unwrap();
        assert_eq!(c.prefix, "a/b");
        assert_eq!(c.full_key("k"), "a/b/k");
        assert_eq!(c.presign_expiry, Duration::from_secs(7200));
        assert_eq!(c.max_get_bytes, 1 << 20);
        assert_eq!(c.max_put_bytes, 10);
        assert!(c.quota.is_some());
        let BackendConfig::S3 {
            virtual_host,
            allow_http,
            credentials,
            ..
        } = &c.backend
        else {
            panic!("s3")
        };
        assert!(*virtual_host && *allow_http);
        let dbg = format!("{credentials:?}");
        assert!(!dbg.contains("secret:sk") && !dbg.contains("AKIA"), "{dbg}");
    }

    #[test]
    fn local_defaults() {
        let c = parse(serde_json::json!({"v": 1, "backend": "local", "root": "/tmp/x"})).unwrap();
        assert_eq!(
            c.backend,
            BackendConfig::Local {
                root: "/tmp/x".into()
            }
        );
        assert_eq!(c.prefix, "");
        assert_eq!(c.full_key("k"), "k");
        let d = ConnectorsConfig::default();
        assert_eq!(c.presign_expiry, d.presign_default_expiry);
        assert_eq!(c.max_get_bytes, d.object_max_get_bytes);
        assert_eq!(c.max_put_bytes, DEFAULT_MAX_PUT_BYTES);
    }

    #[test]
    fn rejects_bad_configs() {
        for bad in [
            serde_json::json!({"backend": "local", "root": "/x"}),
            serde_json::json!({"v": 2, "backend": "local", "root": "/x"}),
            serde_json::json!({"v": 1, "backend": "gcs", "bucket": "b"}),
            serde_json::json!({"v": 1, "backend": "local", "root": "rel"}),
            serde_json::json!({"v": 1, "backend": "local"}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "bucket": "b"}),
            serde_json::json!({"v": 1, "backend": "s3"}),
            serde_json::json!({"v": 1, "backend": "s3", "bucket": "b", "root": "/x"}),
            serde_json::json!({"v": 1, "backend": "s3", "bucket": "b", "url_style": "dns"}),
            serde_json::json!({"v": 1, "backend": "s3", "bucket": "b", "typo": 1}),
            serde_json::json!({"v": 1, "backend": "s3", "bucket": "b",
                "credentials": {"access_key_id": "a", "secret_access_key": "literal-secret"}}),
            serde_json::json!({"v": 1, "backend": "s3", "bucket": "b",
                "credentials": {"access_key_id": "a", "secret_access_key": "{{secret:s}}",
                                "session_token": "literal"}}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "presign_expiry": "8d"}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "presign_expiry": "200h"}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "prefix": "a/../b"}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "prefix": "a//b"}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "max_get_bytes": 0}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "burst": 3}),
            serde_json::json!({"v": 1, "backend": "local", "root": "/x", "content_type": "a\nb"}),
        ] {
            let e = parse(bad.clone()).expect_err(&bad.to_string());
            match e.to_failure() {
                Failure::NonRetryable(info) => assert_eq!(info.kind, CONNECTION_UNKNOWN),
                f => panic!("{f:?}"),
            }
        }
        let wrong_kind = Connection {
            kind: "sql-postgres".into(),
            ..conn(serde_json::json!({"v": 1, "backend": "local", "root": "/x"}))
        };
        assert!(ObjectConnConfig::parse(&wrong_kind, &ConnectorsConfig::default()).is_err());
    }
}
