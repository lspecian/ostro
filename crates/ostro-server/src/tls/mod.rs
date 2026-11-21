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
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, SystemTime};

use tracing::{error, info, warn};

use crate::config::{AcmeChallenge, AcmeSection, ApiTlsMode, ServerConfig};

pub use ca::{CertifiedKeyPair, ClusterCa, NodeIdentity, San, WorkerIdentity};
pub use resolver::{ACME_TLS_ALPN, ReloadableResolver};

/// Which profile's `auto` rule applies (D-W8): a non-loopback `auto` listener is
/// self-signed in dev and refused in the server profile (unless `allow_self_signed`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsProfile {
    Dev,
    Server,
}

impl TlsProfile {
    /// The profile of `cfg`. `ServerConfig` carries no profile field (07-02); the dev
    /// profile is the one that auto-generates the sealing key
    /// ([`ServerConfig::dev_defaults`]), so that flag identifies it. Callers that know the
    /// profile use [`init_with_profile`].
    pub fn of(cfg: &ServerConfig) -> Self {
        if cfg.secrets.auto_sealing_key {
            TlsProfile::Dev
        } else {
            TlsProfile::Server
        }
    }
}

/// The error of a server-profile `auto` listener on a non-loopback address.
pub const AUTO_NON_LOOPBACK_ERROR: &str = "[api.tls] mode=auto on a non-loopback listener requires files or acme (or allow_self_signed = true)";

/// `files` mode: the PEM paths and the mtimes last loaded.
#[derive(Debug)]
struct FilesSource {
    cert: PathBuf,
    key: PathBuf,
    seen: Mutex<(Option<SystemTime>, Option<SystemTime>)>,
}

fn mtime(p: &Path) -> Option<SystemTime> {
    std::fs::metadata(p).and_then(|m| m.modified()).ok()
}

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
    /// The client listener's certificate (every TLS mode feeds it).
    resolver: Arc<ReloadableResolver>,
    /// ALPN of the client listener.
    alpn: Vec<Vec<u8>>,
    /// `files` mode source.
    files: Option<FilesSource>,
    /// `files` mode mtime poll period.
    reload_poll: Duration,
    /// `[api.tls.acme]` (07-10).
    acme: AcmeSection,
    /// The node data directory.
    data_dir: Option<PathBuf>,
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
            resolver: Arc::new(ReloadableResolver::new(None)),
            alpn: alpn_protocols(TlsMode::Off, AcmeChallenge::TlsAlpn01),
            files: None,
            reload_poll: Duration::from_secs(30),
            acme: AcmeSection::default(),
            data_dir: None,
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
            fingerprint: self.fingerprint(),
        }
    }

    /// `sha256:<hex>` of the served leaf (TLS modes with a certificate installed).
    pub fn fingerprint(&self) -> Option<String> {
        match self.mode {
            TlsMode::Off => None,
            _ => self.resolver.fingerprint(),
        }
    }

    /// The client listener's resolver (07-10 installs ACME certificates and TLS-ALPN-01
    /// challenge certificates through it).
    pub fn resolver(&self) -> &Arc<ReloadableResolver> {
        &self.resolver
    }

    /// The client listener's ALPN list.
    pub fn alpn(&self) -> &[Vec<u8>] {
        &self.alpn
    }

    /// `[api.tls.acme]` (07-10).
    pub fn acme_config(&self) -> &AcmeSection {
        &self.acme
    }

    /// The node data directory (`None` for [`TlsState::off`]).
    pub fn data_dir(&self) -> Option<&Path> {
        self.data_dir.as_deref()
    }

    /// `files` mode mtime poll period.
    pub fn reload_poll(&self) -> Duration {
        self.reload_poll
    }

    /// rustls server config of the client listener: the resolver, ALPN per mode,
    /// provider [`ostro_api::tls_provider`], no client auth.
    pub fn client_server_config(&self) -> Result<Arc<rustls::ServerConfig>, TlsError> {
        let mut cfg =
            rustls::ServerConfig::builder_with_provider(Arc::new(ostro_api::tls_provider()))
                .with_safe_default_protocol_versions()
                .map_err(|e| TlsError::Config(format!("TLS versions: {e}")))?
                .with_no_client_auth()
                .with_cert_resolver(Arc::clone(&self.resolver) as Arc<_>);
        cfg.alpn_protocols = self.alpn.clone();
        Ok(Arc::new(cfg))
    }

    /// `files` mode: re-read `cert_file` / `key_file` and swap them in. On error the
    /// served certificate is kept (and the error returned).
    pub fn reload_files(&self) -> Result<(), TlsError> {
        let Some(f) = &self.files else {
            return Err(TlsError::Config("[api.tls] mode is not files".into()));
        };
        let seen = (mtime(&f.cert), mtime(&f.key));
        let res = load_files(&f.cert, &f.key).map(|k| self.resolver.swap(k));
        if let Ok(mut s) = f.seen.lock() {
            *s = seen;
        }
        match &res {
            Ok(()) => info!(
                cert = %f.cert.display(),
                fingerprint = ?self.resolver.fingerprint(),
                "[api.tls] certificate reloaded"
            ),
            Err(e) => error!(
                cert = %f.cert.display(),
                error = %e,
                "[api.tls] certificate reload failed; keeping the served certificate"
            ),
        }
        res
    }

    /// `files` mode poll: reload when either file's mtime changed since the last load.
    /// Returns whether a reload was attempted.
    pub fn poll_files(&self) -> bool {
        let Some(f) = &self.files else {
            return false;
        };
        let now = (mtime(&f.cert), mtime(&f.key));
        let changed = f.seen.lock().map(|s| *s != now).unwrap_or(true);
        if changed {
            let _ = self.reload_files();
        }
        changed
    }
}

/// ALPN of the client listener: `h2`, `http/1.1`, plus `acme-tls/1` in `acme` mode with
/// the TLS-ALPN-01 challenge (W11: without it rustls rejects the validator's ClientHello
/// before the resolver runs).
pub fn alpn_protocols(mode: TlsMode, challenge: AcmeChallenge) -> Vec<Vec<u8>> {
    let mut v = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    if mode == TlsMode::Acme && challenge == AcmeChallenge::TlsAlpn01 {
        v.push(ACME_TLS_ALPN.to_vec());
    }
    v
}

/// Load a PEM chain + key into a rustls `CertifiedKey` (key/leaf match checked).
fn load_files(cert: &Path, key: &Path) -> Result<Arc<rustls::sign::CertifiedKey>, TlsError> {
    let c = std::fs::read(cert).map_err(|source| TlsError::Io {
        path: cert.to_path_buf(),
        source,
    })?;
    let k = std::fs::read(key).map_err(|source| TlsError::Io {
        path: key.to_path_buf(),
        source,
    })?;
    CertifiedKeyPair::from_pem(&c, &k, &cert.display().to_string())?.to_rustls()
}

/// Resolve `[api.tls] mode` for `listen` (D-W8).
pub fn effective_mode(cfg: &ServerConfig, profile: TlsProfile) -> Result<TlsMode, TlsError> {
    let t = &cfg.api.tls;
    Ok(match t.mode {
        ApiTlsMode::Off => TlsMode::Off,
        ApiTlsMode::SelfSigned => TlsMode::SelfSigned,
        ApiTlsMode::Files => TlsMode::Files,
        ApiTlsMode::Acme => TlsMode::Acme,
        ApiTlsMode::Auto => {
            if cfg.api.listen.ip().is_loopback() {
                TlsMode::Off
            } else {
                match profile {
                    TlsProfile::Dev => TlsMode::SelfSigned,
                    TlsProfile::Server if t.allow_self_signed => {
                        warn!(
                            listen = %cfg.api.listen,
                            "[api.tls] mode=auto on a non-loopback listener: serving a \
                             self-signed certificate (allow_self_signed = true)"
                        );
                        TlsMode::SelfSigned
                    }
                    TlsProfile::Server => {
                        return Err(TlsError::Config(AUTO_NON_LOOPBACK_ERROR.to_owned()));
                    }
                }
            }
        }
    })
}

/// Resolve `[api.tls]` (and create the cluster CA under `<data_dir>/tls/`) for
/// `cluster_id` (W11/B3), with the profile taken from [`TlsProfile::of`].
pub fn init(
    cfg: &ServerConfig,
    data_dir: &Path,
    cluster_id: &str,
) -> Result<Arc<TlsState>, TlsError> {
    init_with_profile(cfg, data_dir, cluster_id, TlsProfile::of(cfg))
}

/// [`init`] with an explicit profile: creates/loads the cluster CA and the node
/// certificate under `<data_dir>/tls/` (always: the cluster listener needs them), then
/// resolves the client-listener mode and installs its certificate (`self-signed`: the
/// CA-issued dev cert; `files`: the PEM files; `acme`: none until `acme::start`).
pub fn init_with_profile(
    cfg: &ServerConfig,
    data_dir: &Path,
    cluster_id: &str,
    profile: TlsProfile,
) -> Result<Arc<TlsState>, TlsError> {
    let mode = effective_mode(cfg, profile)?;
    let dir = data_dir.join("tls");
    let ca = Arc::new(ClusterCa::load_or_create(&dir, cluster_id)?);
    let node = ca.load_or_issue_node_cert(cfg.node.node_id, &cfg.cluster.advertise)?;
    let t = &cfg.api.tls;
    let mut files = None;
    let dev = OnceLock::new();
    let initial = match mode {
        TlsMode::Off | TlsMode::Acme => None,
        TlsMode::SelfSigned => {
            let pair = dev_cert::load_or_issue(&ca, &dev_cert::sans(cfg.api.listen, &[]))?;
            let key = pair.to_rustls()?;
            let _ = dev.set(pair);
            Some(key)
        }
        TlsMode::Files => {
            let (Some(cert), Some(key)) = (t.cert_file.clone(), t.key_file.clone()) else {
                return Err(TlsError::Config(
                    "[api.tls] mode = files needs cert_file and key_file".into(),
                ));
            };
            let seen = (mtime(&cert), mtime(&key));
            let k = load_files(&cert, &key)?;
            files = Some(FilesSource {
                cert,
                key,
                seen: Mutex::new(seen),
            });
            Some(k)
        }
    };
    Ok(Arc::new(TlsState {
        mode,
        cluster_id: cluster_id.to_owned(),
        node_id: cfg.node.node_id,
        dir: Some(dir),
        ca: Some(ca),
        node: Some(node),
        dev,
        resolver: Arc::new(ReloadableResolver::new(initial)),
        alpn: alpn_protocols(mode, t.acme.challenge),
        files,
        reload_poll: t.reload_poll.into(),
        acme: t.acme.clone(),
        data_dir: Some(data_dir.to_path_buf()),
    }))
}
