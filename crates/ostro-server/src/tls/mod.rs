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
//! [`init`] creates (or loads) the cluster CA and the node certificate under
//! `<data_dir>/tls/` on every start, whatever the client-listener mode: the cluster
//! listener (07-09) always needs them.

pub mod acme;
pub mod ca;
pub mod dev_cert;
pub mod listener;
pub mod resolver;

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::config::ServerConfig;

pub use ca::{CertifiedKeyPair, ClusterCa, NodeIdentity, San, WorkerIdentity};

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

/// TLS state of the node: the cluster CA, the node certificate, the effective
/// client-listener mode and its certificate.
#[derive(Debug)]
pub struct TlsState {
    mode: TlsMode,
    cluster_id: String,
    node_id: u64,
    /// `<data_dir>/tls`.
    dir: Option<PathBuf>,
    /// The cluster CA (`None` only for [`TlsState::off`]).
    ca: Option<Arc<ClusterCa>>,
    /// `<data_dir>/tls/node.{crt,key}`.
    node: Option<CertifiedKeyPair>,
    /// The `self-signed` mode dev cert (issued lazily).
    dev: OnceLock<CertifiedKeyPair>,
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
    /// A certificate, key or CSR is unusable (never contains key material).
    #[error("TLS certificate: {0}")]
    Cert(String),
}

impl TlsState {
    /// Plaintext client listener.
    pub fn off() -> Self {
        Self {
            mode: TlsMode::Off,
            cluster_id: String::new(),
            node_id: 0,
            dir: None,
            ca: None,
            node: None,
            dev: OnceLock::new(),
            fingerprint: None,
        }
    }

    pub fn mode(&self) -> TlsMode {
        self.mode
    }

    /// The cluster id the CA belongs to (empty for [`TlsState::off`]).
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// The node id of the node certificate.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// `<data_dir>/tls`.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// The cluster CA.
    pub fn ca(&self) -> Option<&Arc<ClusterCa>> {
        self.ca.as_ref()
    }

    /// The CA certificate (DER).
    pub fn ca_der(&self) -> Option<Vec<u8>> {
        self.ca.as_ref().map(|c| c.cert_der().to_vec())
    }

    /// The CA certificate (PEM).
    pub fn ca_pem(&self) -> Option<String> {
        self.ca.as_ref().map(|c| c.cert_pem())
    }

    /// `sha256:<hex of the CA DER>` (what workers pin with `--ca-hash`).
    pub fn ca_hash(&self) -> Option<String> {
        self.ca.as_ref().map(|c| c.ca_hash())
    }

    /// The node certificate (`ostro://<cluster_id>/node/<node_id>`), chain [leaf, CA].
    pub fn node_cert(&self) -> Option<&CertifiedKeyPair> {
        self.node.as_ref()
    }

    /// rustls server config for the cluster listener (07-09): the node certificate, a
    /// client certificate chaining to the cluster CA always required, ALPN h2.
    pub fn node_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError> {
        let (Some(ca), Some(node)) = (&self.ca, &self.node) else {
            return Err(TlsError::Config("no cluster CA (TLS state is off)".into()));
        };
        let provider = Arc::new(ostro_api::tls_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(ca.root_store()?),
            Arc::clone(&provider),
        )
        .build()
        .map_err(|e| TlsError::Cert(format!("client verifier: {e}")))?;
        let mut cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| TlsError::Config(format!("TLS versions: {e}")))?
            .with_client_cert_verifier(verifier)
            .with_single_cert(node.chain().to_vec(), node.key_der())
            .map_err(|e| TlsError::Cert(format!("node certificate: {e}")))?;
        cfg.alpn_protocols = vec![b"h2".to_vec()];
        Ok(Arc::new(cfg))
    }

    /// Sign a worker CSR with the cluster CA (see [`ClusterCa::sign_worker_csr`]).
    pub fn sign_worker_csr(
        &self,
        csr_der: &[u8],
        token_id: &str,
        worker_id: &str,
        ttl: Duration,
    ) -> Result<Vec<u8>, TlsError> {
        let ca = self
            .ca
            .as_ref()
            .ok_or_else(|| TlsError::Config("no cluster CA (TLS state is off)".into()))?;
        ca.sign_worker_csr(csr_der, token_id, worker_id, ttl)
    }

    /// The `self-signed` mode dev cert for a listener on `listen` (issued, or reused from
    /// `dev.{crt,key}`, on first call).
    pub fn dev_cert(&self, listen: SocketAddr) -> Result<&CertifiedKeyPair, TlsError> {
        if let Some(d) = self.dev.get() {
            return Ok(d);
        }
        let ca = self
            .ca
            .as_ref()
            .ok_or_else(|| TlsError::Config("no cluster CA (TLS state is off)".into()))?;
        let pair = dev_cert::load_or_issue(ca, &dev_cert::sans(listen, &[]))?;
        Ok(self.dev.get_or_init(|| pair))
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
/// Phase 7 (07-06 task 1): the CA and node certificate are created; the client listener
/// mode is resolved in task 2.
pub fn init(
    cfg: &ServerConfig,
    data_dir: &Path,
    cluster_id: &str,
) -> Result<Arc<TlsState>, TlsError> {
    let dir = data_dir.join("tls");
    let ca = ClusterCa::load_or_create(&dir, cluster_id)?;
    let node = ca.load_or_issue_node_cert(cfg.node.node_id, &cfg.cluster.advertise)?;
    Ok(Arc::new(TlsState {
        mode: TlsMode::Off,
        cluster_id: cluster_id.to_owned(),
        node_id: cfg.node.node_id,
        dir: Some(dir),
        ca: Some(Arc::new(ca)),
        node: Some(node),
        dev: OnceLock::new(),
        fingerprint: None,
    }))
}
