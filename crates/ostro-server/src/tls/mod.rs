//! TLS for the client listener and the cluster CA (Phase 7, SEC-01).
//!
//! | File | Owner | Purpose |
//! |---|---|---|
//! | [`ca`] | 07-06 | cluster CA (`<data_dir>/tls/ca.{crt,key}`), node cert, CSR signing |
//! | [`dev_cert`] | 07-06 | `self-signed` mode: a dev leaf issued by the cluster CA (D-W20) |
//! | [`resolver`] | 07-06 | one `ResolvesServerCert` over an `ArcSwap<CertifiedKey>` |
//! | [`listener`] | 07-06 | the client listener: plaintext or TLS (axum-server, ALPN h2 + http/1.1) |
//! | [`acme`] | 07-10 | `acme` mode (instant-acme) |
//!
//! Plan 07-02 fixes the signatures; [`init`] resolves every mode to [`TlsMode::Off`] until
//! 07-06, so the listener stays plaintext (today's behaviour).

pub mod acme;
pub mod ca;
pub mod dev_cert;
pub mod listener;
pub mod resolver;

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use crate::config::ServerConfig;

/// The effective client-listener TLS mode (after `auto` is resolved, D-W8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsMode {
    /// Plaintext HTTP/1.1 + h2c.
    Off,
    /// A dev leaf issued by the cluster CA.
    SelfSigned,
    /// PEM files (`[api.tls] cert_file` / `key_file`), hot-reloaded.
    Files,
    /// ACME (Let's Encrypt or another RFC 8555 directory).
    Acme,
}

/// TLS state of the node: the effective client-listener mode and its certificate.
#[derive(Debug)]
pub struct TlsState {
    mode: TlsMode,
    /// sha256 fingerprint (`sha256:<hex>`) of the served certificate, when TLS is on.
    fingerprint: Option<String>,
}

/// How clients reach the client listener (the ready line, 07-15).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientEndpoint {
    /// `http` or `https`.
    pub scheme: &'static str,
    pub addr: SocketAddr,
    /// `sha256:<hex>` of the served certificate (self-signed mode).
    pub fingerprint: Option<String>,
}

impl ClientEndpoint {
    /// `<scheme>://<addr>`.
    pub fn url(&self) -> String {
        format!("{}://{}", self.scheme, self.addr)
    }
}

/// TLS setup failure (the message names the key or path; never key material).
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("invalid TLS configuration: {0}")]
    Config(String),
    #[error("TLS file {path}: {source}")]
    Io {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
}

impl TlsState {
    /// Plaintext client listener.
    pub fn off() -> Self {
        Self {
            mode: TlsMode::Off,
            fingerprint: None,
        }
    }

    pub fn mode(&self) -> TlsMode {
        self.mode
    }

    /// The client endpoint for the bound `addr`.
    pub fn endpoint(&self, addr: SocketAddr) -> ClientEndpoint {
        ClientEndpoint {
            scheme: match self.mode {
                TlsMode::Off => "http",
                TlsMode::SelfSigned | TlsMode::Files | TlsMode::Acme => "https",
            },
            addr,
            fingerprint: self.fingerprint.clone(),
        }
    }
}

/// Resolve `[api.tls]` (and create the cluster CA under `<data_dir>/tls/`) for
/// `cluster_id` (W11/B3).
///
/// Phase 7 stub (07-02): always [`TlsMode::Off`]; 07-06 implements the modes.
pub fn init(
    cfg: &ServerConfig,
    data_dir: &Path,
    cluster_id: &str,
) -> Result<Arc<TlsState>, TlsError> {
    let _ = (cfg, data_dir, cluster_id);
    Ok(Arc::new(TlsState::off()))
}
