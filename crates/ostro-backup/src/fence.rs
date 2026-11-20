//! Prefix fencing (P8-D4, plan 08-13): the `LOCK` object, immutable puts and the LOCK re-read
//! fallback.
//!
//! - [`acquire`] creates `LOCK` = `{cluster_id, node_id, incarnation}` with `If-None-Match: *`.
//!   An existing LOCK with the same identity is ours (restart); another identity is
//!   [`FenceError::Fenced`] unless `takeover` is set, which replaces it with `If-Match` on its
//!   etag.
//! - The manifest PUT uses `If-Match` on the previous etag (the uploader); a failed
//!   precondition means another writer owns the prefix.
//! - [`check_before_manifest`] is a no-op on stores with conditional PUT. Without it (the
//!   documented, weaker fallback) it re-reads LOCK and compares the identity before every
//!   manifest PUT.
//! - [`put_immutable`] writes `seg/`, `snap/`, `meta/`, `blobs/` and `manifest/gen/` objects
//!   with `put_if_absent`: an existing object with the same sha256 is success (an idempotent
//!   retry after a crash); different bytes under our key mean another writer, so
//!   [`FenceError::Fenced`] (W9).

use std::sync::Arc;

use ostro_objstore::{Conditional, ObjectStore, StoreOptions};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::config::BackupConfig;

/// `LOCK`.
pub const LOCK_KEY: &str = "LOCK";

/// Who owns a prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ident {
    /// Cluster id.
    pub cluster_id: String,
    /// Node id.
    pub node_id: u64,
    /// `node.toml` incarnation, 32 lowercase hex.
    pub incarnation: String,
}

impl Ident {
    /// Build from the raw incarnation bytes.
    pub fn new(cluster_id: impl Into<String>, node_id: u64, incarnation: [u8; 16]) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            node_id,
            incarnation: ostro_partition::node::hex16(&incarnation),
        }
    }

    fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_default()
    }
}

/// Fencing failures.
#[derive(Debug, thiserror::Error)]
pub enum FenceError {
    /// Another writer owns the prefix (or wrote different bytes under one of our keys).
    #[error("fenced: {0}")]
    Fenced(String),
    /// `LOCK` exists but cannot be parsed.
    #[error("unreadable LOCK object: {0}")]
    BadLock(String),
    /// Object store failure (retryable).
    #[error("object store: {0}")]
    Store(#[from] ostro_objstore::Error),
}

/// Outcome of [`put_immutable`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Immutable {
    /// We wrote it.
    Written,
    /// An identical object was already there.
    AlreadyPresent,
}

async fn read_lock(
    store: &Arc<dyn ObjectStore>,
) -> Result<Option<(Ident, Option<String>)>, FenceError> {
    match store.get(LOCK_KEY).await? {
        None => Ok(None),
        Some((bytes, meta)) => {
            let id: Ident =
                serde_json::from_slice(&bytes).map_err(|e| FenceError::BadLock(e.to_string()))?;
            Ok(Some((id, meta.e_tag)))
        }
    }
}

/// Acquire the prefix (see the module docs).
pub async fn acquire(
    store: &Arc<dyn ObjectStore>,
    ident: &Ident,
    takeover: bool,
) -> Result<(), FenceError> {
    if store
        .put_if_absent(LOCK_KEY, ident.encode().into())
        .await?
        .ok()
        .is_some()
    {
        tracing::info!(node_id = ident.node_id, incarnation = %ident.incarnation, "bottomless LOCK created");
        return Ok(());
    }
    let Some((current, etag)) = read_lock(store).await? else {
        // Deleted between the two calls: try once more from scratch.
        return match store.put_if_absent(LOCK_KEY, ident.encode().into()).await? {
            Conditional::Ok(_) => Ok(()),
            Conditional::PreconditionFailed => {
                Err(FenceError::Fenced("LOCK changed concurrently".into()))
            }
        };
    };
    if current == *ident {
        return Ok(());
    }
    if !takeover {
        return Err(FenceError::Fenced(format!(
            "prefix owned by cluster {} node {} incarnation {} (use --takeover)",
            current.cluster_id, current.node_id, current.incarnation
        )));
    }
    let written = match (&etag, store.supports_conditional_put()) {
        (Some(e), true) => store
            .put_if_match(LOCK_KEY, ident.encode().into(), e)
            .await?
            .ok()
            .is_some(),
        _ => {
            // Fallback: unconditional replace, then re-read.
            store.put(LOCK_KEY, ident.encode().into()).await?;
            true
        }
    };
    if !written {
        return Err(FenceError::Fenced("LOCK changed during takeover".into()));
    }
    tracing::warn!(
        previous_node = current.node_id,
        previous_incarnation = %current.incarnation,
        incarnation = %ident.incarnation,
        "bottomless prefix taken over"
    );
    check_lock(store, ident).await
}

async fn check_lock(store: &Arc<dyn ObjectStore>, ident: &Ident) -> Result<(), FenceError> {
    match read_lock(store).await? {
        Some((id, _)) if id == *ident => Ok(()),
        Some((id, _)) => Err(FenceError::Fenced(format!(
            "LOCK now names node {} incarnation {}",
            id.node_id, id.incarnation
        ))),
        None => Err(FenceError::Fenced("LOCK disappeared".into())),
    }
}

/// Before a manifest PUT: nothing with conditional PUT (the `If-Match` on the manifest
/// fences); otherwise re-read LOCK and compare (P8-D4 fallback, weaker: a writer can still
/// race between this read and the PUT).
pub async fn check_before_manifest(
    store: &Arc<dyn ObjectStore>,
    ident: &Ident,
) -> Result<(), FenceError> {
    if store.supports_conditional_put() {
        return Ok(());
    }
    check_lock(store, ident).await
}

/// Write an immutable object (see the module docs). `sha` is the sha256 of `bytes`.
pub async fn put_immutable(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    bytes: Vec<u8>,
    sha: [u8; 32],
) -> Result<Immutable, FenceError> {
    if store.supports_conditional_put() {
        if store.put_if_absent(key, bytes.into()).await?.ok().is_some() {
            return Ok(Immutable::Written);
        }
        return compare_existing(store, key, sha).await;
    }
    // Without conditional PUT: check first, then write (best effort, documented).
    if store.head(key).await?.is_some() {
        return compare_existing(store, key, sha).await;
    }
    store.put(key, bytes.into()).await?;
    Ok(Immutable::Written)
}

async fn compare_existing(
    store: &Arc<dyn ObjectStore>,
    key: &str,
    sha: [u8; 32],
) -> Result<Immutable, FenceError> {
    match store.get(key).await? {
        Some((b, _)) => {
            let have: [u8; 32] = Sha256::digest(&b).into();
            if have == sha {
                Ok(Immutable::AlreadyPresent)
            } else {
                Err(FenceError::Fenced(format!(
                    "{key} exists with different content (another writer)"
                )))
            }
        }
        None => Err(FenceError::Fenced(format!(
            "{key}: conditional put failed but the object is absent"
        ))),
    }
}

/// Map the `[backup]` transport fields onto [`StoreOptions`].
pub fn store_options(cfg: &BackupConfig) -> StoreOptions {
    let credentials = match (&cfg.access_key_id, &cfg.secret_access_key) {
        (Some(a), Some(s)) => Some(ostro_objstore::Credentials {
            access_key_id: a.clone(),
            secret_access_key: s.clone(),
            session_token: cfg.session_token.clone(),
        }),
        _ => None,
    };
    StoreOptions {
        region: cfg.region.clone(),
        endpoint: cfg.endpoint.clone(),
        url_style: match cfg.url_style {
            crate::config::UrlStyle::Path => ostro_objstore::UrlStyle::Path,
            crate::config::UrlStyle::Virtual => ostro_objstore::UrlStyle::Virtual,
        },
        credentials,
        allow_http: cfg.allow_http,
        tls_roots: match &cfg.tls_roots {
            crate::config::TlsRoots::Platform => ostro_objstore::TlsRoots::Platform,
            crate::config::TlsRoots::Webpki => ostro_objstore::TlsRoots::Webpki,
            crate::config::TlsRoots::File(p) => ostro_objstore::TlsRoots::File(p.clone()),
        },
        ..StoreOptions::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_options_mapping() {
        let cfg = BackupConfig {
            url: Some("s3://b/p".into()),
            region: Some("eu".into()),
            endpoint: Some("http://127.0.0.1:8333".into()),
            url_style: crate::config::UrlStyle::Virtual,
            access_key_id: Some("a".into()),
            secret_access_key: Some("s".into()),
            allow_http: true,
            ..BackupConfig::default()
        };
        let o = store_options(&cfg);
        assert_eq!(o.region.as_deref(), Some("eu"));
        assert_eq!(o.url_style, ostro_objstore::UrlStyle::Virtual);
        assert!(o.allow_http && o.conditional_put);
        let c = o.credentials.expect("credentials");
        assert_eq!(c.access_key_id, "a");
        assert!(
            store_options(&BackupConfig::default())
                .credentials
                .is_none()
        );
    }
}
