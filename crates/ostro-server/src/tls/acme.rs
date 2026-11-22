//! `acme` mode of the client listener over instant-acme 0.8.5 (SEC-01, D-W6, D-W23).
//!
//! Contract fixed by 07-06 (the listener calls it in `acme` mode, inside the runtime,
//! before serving): [`start`] validates `[api.tls.acme]`, installs a cached certificate
//! if one covers the configured domains, binds the HTTP-01 responder when that challenge
//! is configured, and returns the task that orders / renews certificates. The task
//! installs them with [`ReloadableResolver::swap`](super::ReloadableResolver::swap) and
//! TLS-ALPN-01 challenge certs with `set_challenge`, and stops on `shutdown`. The
//! listener serves while that task runs, so TLS-ALPN-01 validation reaches it.
//!
//! Flow of one issuance ([`AcmeIssuer`]):
//! 1. account: `account-<sha256(directory)[..16]>.json` in the cache dir (instant-acme
//!    `AccountCredentials`, 0600), else `Account::builder()` (platform roots) or
//!    `Account::builder_with_root(directory_ca_file)` + `create(NewAccount)`;
//! 2. `new_order(NewOrder::new(dns identifiers))`, then per pending authorization the
//!    configured challenge: TLS-ALPN-01 registers [`challenge_cert`] (critical
//!    `acmeIdentifier` = sha256 of the key authorization, RFC 8737) for the domain,
//!    HTTP-01 publishes the key authorization on the [`Http01Responder`]; `set_ready`;
//! 3. `poll_ready`, then `finalize_csr` with an rcgen CSR over a fresh ECDSA P-256 key,
//!    `poll_certificate`;
//! 4. swap the chain into the resolver, persist `cert.pem` + `key.pem` (0600, tmp +
//!    rename), clear the challenge material.
//!
//! Renewal ([`run_renewal`]): due at `not_after - lifetime/3` (2/3 of the lifetime),
//! computed from the injected clock (Phase 10 F5); failures retry with jittered
//! exponential backoff from 1 min to 1 h ([`backoff_delay`]) while the old certificate
//! stays served. No metric (no TLS metric prefix is reserved for Phase 7): renewals log at
//! info, failures at warn with the next attempt time. Keys are never logged (D-W3).

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use axum::Router;
use axum::extract::{Path as UrlPath, State};
use axum::http::StatusCode;
use axum::routing::get;
use instant_acme::{
    Account, AccountCredentials, AuthorizationStatus, ChallengeType, Identifier, NewAccount,
    NewOrder, OrderStatus, RetryPolicy,
};
use ostro_partition::{Clock, SystemClock}; // clock-exempt: fallback clock import
use rcgen::{CertificateParams, CustomExtension, DistinguishedName, KeyPair};
use rustls::sign::CertifiedKey;
use rustls_pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use super::ca::{CertifiedKeyPair, hex, write_atomic};
use super::{ReloadableResolver, TlsError, TlsState};
use crate::config::{AcmeChallenge, AcmeSection};

/// Cached certificate chain (PEM, leaf first).
pub const CERT_FILE: &str = "cert.pem";
/// Cached certificate key (PEM, PKCS#8).
pub const KEY_FILE: &str = "key.pem";
/// The HTTP-01 path prefix (RFC 8555 §8.3).
pub const HTTP01_PREFIX: &str = "/.well-known/acme-challenge/";
/// First retry delay after a failed issuance.
pub const BACKOFF_MIN: Duration = Duration::from_secs(60);
/// Longest retry delay.
pub const BACKOFF_MAX: Duration = Duration::from_secs(3600);
/// The renewal wait re-reads the clock at least this often (clock jumps, suspend).
pub const MAX_SLEEP: Duration = Duration::from_secs(3600);

fn cfg_err(msg: impl Into<String>) -> TlsError {
    TlsError::Config(msg.into())
}

fn acme_err(what: &str, e: impl std::fmt::Display) -> TlsError {
    TlsError::Cert(format!("ACME {what}: {e}"))
}

// ------------------------------------------------------------------------------------
// Clock seam
// ------------------------------------------------------------------------------------

static CLOCK: OnceLock<Arc<dyn Clock>> = OnceLock::new();

/// Inject the node clock used by [`start`] for renewal scheduling (Phase 10 F5). The
/// first call wins; returns false when a clock was already set.
pub fn set_clock(clock: Arc<dyn Clock>) -> bool {
    CLOCK.set(clock).is_ok()
}

/// The clock [`start`] uses: the injected one, else the partition [`SystemClock`]. // clock-exempt: doc ref
fn clock() -> Arc<dyn Clock> {
    CLOCK
        .get()
        .cloned()
        // clock-exempt: fallback injection root for library callers; `p7::init` always
        // injects the node clock with `set_clock` (07-22).
        .unwrap_or_else(|| Arc::new(SystemClock) as Arc<dyn Clock>) // clock-exempt: default production clock for the injected seam
}

// ------------------------------------------------------------------------------------
// Config
// ------------------------------------------------------------------------------------

/// The cache directory: `cache_dir`, else `<data_dir>/tls/acme`.
pub fn cache_dir(section: &AcmeSection, data_dir: Option<&Path>) -> Result<PathBuf, TlsError> {
    match (&section.cache_dir, data_dir) {
        (Some(d), _) => Ok(d.clone()),
        (None, Some(d)) => Ok(d.join("tls").join("acme")),
        (None, None) => Err(cfg_err(
            "[api.tls.acme] cache_dir is unset and there is no data directory",
        )),
    }
}

/// Validate `[api.tls.acme]`: at least one DNS name, an `https://` directory.
pub fn validate(section: &AcmeSection) -> Result<(), TlsError> {
    if section.domains.is_empty() {
        return Err(cfg_err(
            "[api.tls] mode = acme needs [api.tls.acme] domains",
        ));
    }
    for d in &section.domains {
        let ok = !d.is_empty()
            && d.len() <= 253
            && !d.ends_with('.')
            && d.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'*')
            && d.parse::<std::net::IpAddr>().is_err();
        if !ok {
            return Err(cfg_err(format!(
                "[api.tls.acme] domains: {d:?} is not a DNS name"
            )));
        }
    }
    if !section.directory.starts_with("https://") {
        return Err(cfg_err(format!(
            "[api.tls.acme] directory must be an https:// URL, got {:?}",
            section.directory
        )));
    }
    Ok(())
}

// ------------------------------------------------------------------------------------
// Certificates
// ------------------------------------------------------------------------------------

/// Validity of a certificate, in ms since the Unix epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Validity {
    pub not_before_ms: u64,
    pub not_after_ms: u64,
}

impl Validity {
    /// Of a leaf certificate (DER).
    pub fn of_der(der: &[u8]) -> Result<Self, TlsError> {
        let (_, cert) = x509_parser::parse_x509_certificate(der)
            .map_err(|e| TlsError::Cert(format!("certificate parse: {e}")))?;
        let ms = |t: i64| u64::try_from(t).unwrap_or(0).saturating_mul(1000);
        Ok(Self {
            not_before_ms: ms(cert.validity().not_before.timestamp()),
            not_after_ms: ms(cert.validity().not_after.timestamp()),
        })
    }

    /// `not_after - not_before`.
    pub fn lifetime_ms(&self) -> u64 {
        self.not_after_ms.saturating_sub(self.not_before_ms)
    }

    /// When renewal is due: 2/3 of the lifetime (`not_after - lifetime/3`).
    pub fn renew_at_ms(&self) -> u64 {
        self.not_after_ms.saturating_sub(self.lifetime_ms() / 3)
    }

    /// Still valid at `now_ms`.
    pub fn valid_at(&self, now_ms: u64) -> bool {
        self.not_before_ms <= now_ms.saturating_add(60_000) && now_ms < self.not_after_ms
    }
}

/// The TLS-ALPN-01 challenge certificate of `domain` (RFC 8737 §3): self-signed over a
/// fresh P-256 key, the single SAN `domain`, the critical `id-pe-acmeIdentifier`
/// extension holding `sha256(key_authorization)` as an OCTET STRING.
pub fn challenge_cert(domain: &str, key_auth_digest: &[u8]) -> Result<CertifiedKey, TlsError> {
    if key_auth_digest.len() != 32 {
        return Err(TlsError::Cert(format!(
            "acmeIdentifier digest must be 32 bytes, got {}",
            key_auth_digest.len()
        )));
    }
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| acme_err("challenge key", e))?;
    let mut params = CertificateParams::new(vec![domain.to_owned()])
        .map_err(|e| acme_err("challenge SAN", e))?;
    params.distinguished_name = DistinguishedName::new();
    params
        .custom_extensions
        .push(CustomExtension::new_acme_identifier(key_auth_digest));
    let cert = params
        .self_signed(&key)
        .map_err(|e| acme_err("challenge cert", e))?;
    // Not `CertifiedKey::from_der`: its key/leaf consistency check parses the leaf with
    // webpki, which rejects the (required) unknown critical extension.
    let signing = ostro_api::tls_provider()
        .key_provider
        .load_private_key(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            key.serialize_der(),
        )))
        .map_err(|e| acme_err("challenge key", e))?;
    Ok(CertifiedKey::new(vec![cert.der().clone()], signing))
}

/// A new certificate key and the CSR for `domains` (DER).
fn csr_for(domains: &[String]) -> Result<(KeyPair, Vec<u8>), TlsError> {
    let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| acme_err("certificate key", e))?;
    let mut params =
        CertificateParams::new(domains.to_vec()).map_err(|e| acme_err("CSR SAN", e))?;
    params.distinguished_name = DistinguishedName::new();
    let csr = params
        .serialize_request(&key)
        .map_err(|e| acme_err("CSR", e))?;
    Ok((key, csr.der().to_vec()))
}

/// The DNS SANs of a leaf (lowercase).
fn dns_sans(der: &[u8]) -> Result<Vec<String>, TlsError> {
    let (_, cert) = x509_parser::parse_x509_certificate(der)
        .map_err(|e| TlsError::Cert(format!("certificate parse: {e}")))?;
    let mut out = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for n in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::DNSName(d) = n {
                out.push(d.to_ascii_lowercase());
            }
        }
    }
    Ok(out)
}

// ------------------------------------------------------------------------------------
// Cache
// ------------------------------------------------------------------------------------

fn io_err(path: &Path, source: io::Error) -> TlsError {
    TlsError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn ensure_dir(dir: &Path) -> Result<(), TlsError> {
    std::fs::create_dir_all(dir).map_err(|e| io_err(dir, e))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| io_err(dir, e))?;
    }
    Ok(())
}

/// The account credentials file of `directory` in `cache_dir`.
pub fn account_file(cache_dir: &Path, directory: &str) -> PathBuf {
    let h = Sha256::digest(directory.as_bytes());
    cache_dir.join(format!("account-{}.json", &hex(&h)[..16]))
}

/// A cached certificate that covers every configured domain.
#[derive(Debug)]
pub struct Cached {
    pub key: Arc<CertifiedKey>,
    pub validity: Validity,
    pub fingerprint: String,
}

/// Load `cert.pem` + `key.pem` from `cache_dir` when present, usable (key matches) and
/// covering every domain of `domains`. A missing pair is `Ok(None)`; an unusable pair is
/// logged and ignored (a new certificate is ordered).
pub fn load_cached(cache_dir: &Path, domains: &[String]) -> Result<Option<Cached>, TlsError> {
    let (cp, kp) = (cache_dir.join(CERT_FILE), cache_dir.join(KEY_FILE));
    let (c, k) = match (std::fs::read(&cp), std::fs::read(&kp)) {
        (Ok(c), Ok(k)) => (c, k),
        (Err(e), _) | (_, Err(e)) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        (Err(e), _) => return Err(io_err(&cp, e)),
        (_, Err(e)) => return Err(io_err(&kp, e)),
    };
    let loaded = (|| {
        let pair = CertifiedKeyPair::from_pem(&c, &k, &cp.display().to_string())?;
        let key = pair.to_rustls()?;
        let validity = Validity::of_der(pair.leaf_der())?;
        let sans = dns_sans(pair.leaf_der())?;
        let covers = domains
            .iter()
            .all(|d| sans.iter().any(|s| s == &d.to_ascii_lowercase()));
        Ok::<_, TlsError>(covers.then(|| Cached {
            key,
            validity,
            fingerprint: pair.fingerprint(),
        }))
    })();
    match loaded {
        Ok(c) => Ok(c),
        Err(e) => {
            warn!(cache = %cache_dir.display(), error = %e, "[api.tls.acme] cached certificate unusable; ordering a new one");
            Ok(None)
        }
    }
}

/// Persist a chain + key into `cache_dir` (both 0600, tmp + rename).
pub fn store_cert(cache_dir: &Path, chain_pem: &str, key_pem: &str) -> Result<(), TlsError> {
    ensure_dir(cache_dir)?;
    write_atomic(&cache_dir.join(KEY_FILE), key_pem.as_bytes(), 0o600)?;
    write_atomic(&cache_dir.join(CERT_FILE), chain_pem.as_bytes(), 0o600)
}

// ------------------------------------------------------------------------------------
// HTTP-01 responder
// ------------------------------------------------------------------------------------

/// The HTTP-01 key authorizations by token, served on `http01_listen`.
#[derive(Clone, Default)]
pub struct Http01Responder {
    tokens: Arc<RwLock<HashMap<String, String>>>,
}

impl std::fmt::Debug for Http01Responder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let n = self.tokens.read().map(|t| t.len()).unwrap_or_default();
        f.debug_struct("Http01Responder")
            .field("tokens", &n)
            .finish()
    }
}

impl Http01Responder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Publish `key_authorization` under `token`.
    pub fn insert(&self, token: &str, key_authorization: &str) {
        let mut m = match self.tokens.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        m.insert(token.to_owned(), key_authorization.to_owned());
    }

    /// Stop serving `token`.
    pub fn remove(&self, token: &str) {
        let mut m = match self.tokens.write() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        m.remove(token);
    }

    /// The key authorization of `token`.
    pub fn get(&self, token: &str) -> Option<String> {
        let m = match self.tokens.read() {
            Ok(m) => m,
            Err(p) => p.into_inner(),
        };
        m.get(token).cloned()
    }

    /// `GET /.well-known/acme-challenge/{token}` → the key authorization, else 404.
    pub fn router(&self) -> Router {
        async fn answer(
            State(r): State<Http01Responder>,
            UrlPath(token): UrlPath<String>,
        ) -> Result<String, StatusCode> {
            r.get(&token).ok_or(StatusCode::NOT_FOUND)
        }
        Router::new()
            .route("/.well-known/acme-challenge/{token}", get(answer))
            .with_state(self.clone())
    }

    /// Bind `listen` now (a bind error is a startup error) and serve until `shutdown`.
    /// Must be called inside the runtime. Returns the bound address and the task.
    pub fn serve(
        &self,
        listen: SocketAddr,
        shutdown: CancellationToken,
    ) -> Result<(SocketAddr, JoinHandle<()>), TlsError> {
        let what = format!("[api.tls.acme] http01_listen {listen}");
        let std_l = std::net::TcpListener::bind(listen).map_err(|e| io_err(Path::new(&what), e))?;
        std_l
            .set_nonblocking(true)
            .map_err(|e| io_err(Path::new(&what), e))?;
        let l =
            tokio::net::TcpListener::from_std(std_l).map_err(|e| io_err(Path::new(&what), e))?;
        let addr = l.local_addr().map_err(|e| io_err(Path::new(&what), e))?;
        let app = self.router();
        let task = tokio::spawn(async move {
            if let Err(e) = axum::serve(l, app)
                .with_graceful_shutdown(shutdown.cancelled_owned())
                .await
            {
                warn!(%addr, error = %e, "[api.tls.acme] HTTP-01 responder stopped");
            }
        });
        info!(%addr, "[api.tls.acme] HTTP-01 responder listening");
        Ok((addr, task))
    }
}

// ------------------------------------------------------------------------------------
// Renewal scheduling
// ------------------------------------------------------------------------------------

/// Renewal timing: retry backoff bounds and the longest single wait before the clock is
/// re-read. [`RenewalPolicy::default`] is the production policy (1 min, 1 h, 1 h).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RenewalPolicy {
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    pub max_sleep: Duration,
}

impl Default for RenewalPolicy {
    fn default() -> Self {
        Self {
            backoff_min: BACKOFF_MIN,
            backoff_max: BACKOFF_MAX,
            max_sleep: MAX_SLEEP,
        }
    }
}

impl RenewalPolicy {
    /// The retry delay after `failures` consecutive failed issuances (`failures >= 1`):
    /// `backoff_min * 2^(failures-1)` jittered by a factor in `[0.75, 1.25)` (`jitter` in
    /// `[0, 1)`), clamped to `[backoff_min, backoff_max]`.
    pub fn backoff_delay(&self, failures: u32, jitter: f64) -> Duration {
        let exp = failures.saturating_sub(1).min(16);
        let base = self
            .backoff_min
            .saturating_mul(1u32 << exp)
            .min(self.backoff_max);
        let factor = 0.75 + 0.5 * jitter.clamp(0.0, 1.0);
        base.mul_f64(factor)
            .clamp(self.backoff_min, self.backoff_max)
    }
}

/// [`RenewalPolicy::backoff_delay`] of the production policy (1 min -> 1 h).
pub fn backoff_delay(failures: u32, jitter: f64) -> Duration {
    RenewalPolicy::default().backoff_delay(failures, jitter)
}

/// A uniform value in `[0, 1)` from the OS RNG (0.5 if it is unavailable).
pub fn random_unit() -> f64 {
    let mut b = [0u8; 8];
    match getrandom::fill(&mut b) {
        Ok(()) => (u64::from_le_bytes(b) >> 11) as f64 / (1u64 << 53) as f64,
        Err(_) => 0.5,
    }
}

/// One issuance: order, validate, finalize, install. Returns the new validity.
pub trait Obtain: Send {
    fn obtain(&mut self) -> impl Future<Output = Result<Validity, TlsError>> + Send;
}

/// The renewal loop: wait (on the injected clock) until `current` is due for renewal
/// (immediately when `None`), call `obtain`, and on failure retry after
/// [`RenewalPolicy::backoff_delay`]. Returns when `shutdown` fires.
pub async fn run_renewal<O: Obtain>(
    policy: RenewalPolicy,
    clock: Arc<dyn Clock>,
    mut current: Option<Validity>,
    shutdown: CancellationToken,
    mut issuer: O,
    mut jitter: impl FnMut() -> f64 + Send,
) {
    let mut failures = 0u32;
    loop {
        loop {
            let due = current.map(|v| v.renew_at_ms()).unwrap_or(0);
            let now = clock.now_ms();
            if now >= due {
                break;
            }
            let wait = Duration::from_millis(due - now).min(policy.max_sleep);
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
        let res = tokio::select! {
            _ = shutdown.cancelled() => return,
            r = issuer.obtain() => r,
        };
        match res {
            Ok(v) => {
                failures = 0;
                current = Some(v);
                info!(
                    not_after_ms = v.not_after_ms,
                    renew_at_ms = v.renew_at_ms(),
                    "[api.tls.acme] certificate installed"
                );
            }
            Err(e) => {
                failures = failures.saturating_add(1);
                let d = policy.backoff_delay(failures, jitter());
                let next_attempt_ms = clock
                    .now_ms()
                    .saturating_add(u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
                warn!(
                    error = %e,
                    failures,
                    retry_in_s = d.as_secs(),
                    next_attempt_ms,
                    "[api.tls.acme] certificate issuance failed; keeping the served certificate"
                );
                tokio::select! {
                    _ = shutdown.cancelled() => return,
                    _ = tokio::time::sleep(d) => {}
                }
            }
        }
    }
}

// ------------------------------------------------------------------------------------
// Issuance over instant-acme
// ------------------------------------------------------------------------------------

/// Make `rustls::ClientConfig::builder()` (used inside instant-acme's hyper-rustls client)
/// resolve to the node's provider.
fn ensure_provider() {
    let _ = rustls::crypto::CryptoProvider::install_default(ostro_api::tls_provider());
}

/// Timeouts of the ACME polls (order ready, certificate).
fn poll_policy() -> RetryPolicy {
    RetryPolicy::new()
        .initial_delay(Duration::from_millis(500))
        .backoff(1.5)
        .timeout(Duration::from_secs(120))
}

/// The real [`Obtain`]: instant-acme against `[api.tls.acme] directory`.
pub struct AcmeIssuer {
    resolver: Arc<ReloadableResolver>,
    section: AcmeSection,
    cache_dir: PathBuf,
    http01: Option<Http01Responder>,
    account: Option<Account>,
}

impl std::fmt::Debug for AcmeIssuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AcmeIssuer")
            .field("directory", &self.section.directory)
            .field("domains", &self.section.domains)
            .field("cache_dir", &self.cache_dir)
            .finish_non_exhaustive()
    }
}

impl AcmeIssuer {
    pub fn new(
        resolver: Arc<ReloadableResolver>,
        section: AcmeSection,
        cache_dir: PathBuf,
        http01: Option<Http01Responder>,
    ) -> Self {
        Self {
            resolver,
            section,
            cache_dir,
            http01,
            account: None,
        }
    }

    fn builder(&self) -> Result<instant_acme::AccountBuilder, TlsError> {
        ensure_provider();
        match &self.section.directory_ca_file {
            Some(ca) => Account::builder_with_root(ca)
                .map_err(|e| acme_err(&format!("directory_ca_file {}", ca.display()), e)),
            None => Account::builder().map_err(|e| acme_err("HTTP client", e)),
        }
    }

    /// Load the cached account of this directory, or create and cache one.
    async fn account(&mut self) -> Result<Account, TlsError> {
        if let Some(a) = &self.account {
            return Ok(a.clone());
        }
        ensure_dir(&self.cache_dir)?;
        let path = account_file(&self.cache_dir, &self.section.directory);
        let account = match std::fs::read(&path) {
            Ok(bytes) => {
                let creds: AccountCredentials = serde_json::from_slice(&bytes)
                    .map_err(|e| TlsError::Cert(format!("{}: {e}", path.display())))?;
                let a = self
                    .builder()?
                    .from_credentials(creds)
                    .await
                    .map_err(|e| acme_err("account restore", e))?;
                info!(account = a.id(), "[api.tls.acme] account loaded from cache");
                a
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let contact: Vec<String> = self
                    .section
                    .email
                    .iter()
                    .map(|m| format!("mailto:{m}"))
                    .collect();
                let contact: Vec<&str> = contact.iter().map(String::as_str).collect();
                let (a, creds) = self
                    .builder()?
                    .create(
                        &NewAccount {
                            contact: &contact,
                            terms_of_service_agreed: true,
                            only_return_existing: false,
                        },
                        self.section.directory.clone(),
                        None,
                    )
                    .await
                    .map_err(|e| acme_err("account create", e))?;
                let json = serde_json::to_vec(&creds)
                    .map_err(|e| TlsError::Cert(format!("account credentials: {e}")))?;
                write_atomic(&path, &json, 0o600)?;
                info!(account = a.id(), "[api.tls.acme] account created");
                a
            }
            Err(e) => return Err(io_err(&path, e)),
        };
        self.account = Some(account.clone());
        Ok(account)
    }

    /// Order, validate and download a chain; returns (chain PEM, key PEM).
    async fn issue(
        &self,
        account: &Account,
        tls_alpn: &mut Vec<String>,
        http_tokens: &mut Vec<String>,
    ) -> Result<(String, String), TlsError> {
        let ids: Vec<Identifier> = self
            .section
            .domains
            .iter()
            .map(|d| Identifier::Dns(d.clone()))
            .collect();
        let mut order = account
            .new_order(&NewOrder::new(&ids))
            .await
            .map_err(|e| acme_err("new order", e))?;
        let ty = match self.section.challenge {
            AcmeChallenge::TlsAlpn01 => ChallengeType::TlsAlpn01,
            AcmeChallenge::Http01 => ChallengeType::Http01,
        };
        let mut authzs = order.authorizations();
        while let Some(a) = authzs.next().await {
            let mut a = a.map_err(|e| acme_err("authorization", e))?;
            match a.status {
                AuthorizationStatus::Pending => {}
                AuthorizationStatus::Valid => continue,
                s => return Err(acme_err("authorization", format!("status {s:?}"))),
            }
            let mut ch = a
                .challenge(ty.clone())
                .ok_or_else(|| acme_err("authorization", format!("no {ty:?} challenge offered")))?;
            let domain = match ch.identifier().identifier {
                Identifier::Dns(d) => d.clone(),
                other => return Err(acme_err("authorization", format!("identifier {other:?}"))),
            };
            let ka = ch.key_authorization();
            match self.section.challenge {
                AcmeChallenge::TlsAlpn01 => {
                    let cert = challenge_cert(&domain, ka.digest().as_ref())?;
                    self.resolver.set_challenge(&domain, Some(cert));
                    tls_alpn.push(domain.clone());
                }
                AcmeChallenge::Http01 => {
                    let r = self
                        .http01
                        .as_ref()
                        .ok_or_else(|| cfg_err("HTTP-01 responder not running"))?;
                    r.insert(&ch.token, ka.as_str());
                    http_tokens.push(ch.token.clone());
                }
            }
            ch.set_ready()
                .await
                .map_err(|e| acme_err(&format!("challenge ready ({domain})"), e))?;
        }
        let status = order
            .poll_ready(&poll_policy())
            .await
            .map_err(|e| acme_err("order poll", e))?;
        if status != OrderStatus::Ready {
            let detail = order
                .state()
                .error
                .as_ref()
                .map(|p| p.to_string())
                .unwrap_or_default();
            return Err(acme_err("order", format!("status {status:?} {detail}")));
        }
        let (key, csr) = csr_for(&self.section.domains)?;
        order
            .finalize_csr(&csr)
            .await
            .map_err(|e| acme_err("finalize", e))?;
        let chain = order
            .poll_certificate(&poll_policy())
            .await
            .map_err(|e| acme_err("certificate download", e))?;
        Ok((chain, key.serialize_pem()))
    }
}

impl Obtain for AcmeIssuer {
    async fn obtain(&mut self) -> Result<Validity, TlsError> {
        let account = self.account().await?;
        let (mut tls_alpn, mut http_tokens) = (Vec::new(), Vec::new());
        let res = self.issue(&account, &mut tls_alpn, &mut http_tokens).await;
        for d in &tls_alpn {
            self.resolver.set_challenge(d, None);
        }
        if let Some(r) = &self.http01 {
            for t in &http_tokens {
                r.remove(t);
            }
        }
        let (chain_pem, key_pem) = res?;
        let pair =
            CertifiedKeyPair::from_pem(chain_pem.as_bytes(), key_pem.as_bytes(), "ACME chain")?;
        let validity = Validity::of_der(pair.leaf_der())?;
        self.resolver.swap(pair.to_rustls()?);
        info!(
            domains = ?self.section.domains,
            fingerprint = %pair.fingerprint(),
            not_after_ms = validity.not_after_ms,
            "[api.tls.acme] new certificate served"
        );
        if let Err(e) = store_cert(&self.cache_dir, &chain_pem, &key_pem) {
            warn!(cache = %self.cache_dir.display(), error = %e, "[api.tls.acme] could not cache the certificate");
        }
        Ok(validity)
    }
}

// ------------------------------------------------------------------------------------
// Entry point
// ------------------------------------------------------------------------------------

/// Start ACME for the client listener with the injected clock ([`set_clock`]).
pub fn start(
    state: Arc<TlsState>,
    shutdown: CancellationToken,
) -> Result<JoinHandle<()>, TlsError> {
    start_with_clock(state, shutdown, clock())
}

/// [`start`] with an explicit clock: validate, install a cached certificate (no network
/// when it is valid for more than 1/3 of its lifetime), bind the HTTP-01 responder when
/// configured, and spawn the renewal task.
pub fn start_with_clock(
    state: Arc<TlsState>,
    shutdown: CancellationToken,
    clock: Arc<dyn Clock>,
) -> Result<JoinHandle<()>, TlsError> {
    let section = state.acme_config().clone();
    validate(&section)?;
    let dir = cache_dir(&section, state.data_dir())?;
    ensure_dir(&dir)?;
    let now = clock.now_ms();
    let mut current = None;
    if let Some(c) = load_cached(&dir, &section.domains)?
        && c.validity.valid_at(now)
    {
        state.resolver().swap(Arc::clone(&c.key));
        info!(
            fingerprint = %c.fingerprint,
            not_after_ms = c.validity.not_after_ms,
            renew_due = now >= c.validity.renew_at_ms(),
            "[api.tls.acme] cached certificate installed"
        );
        current = Some(c.validity);
    }
    let mut aux = None;
    let http01 = match section.challenge {
        AcmeChallenge::Http01 => {
            let r = Http01Responder::new();
            let (_, task) = r.serve(section.http01_listen, shutdown.clone())?;
            aux = Some(task);
            Some(r)
        }
        AcmeChallenge::TlsAlpn01 => None,
    };
    let issuer = AcmeIssuer::new(Arc::clone(state.resolver()), section, dir, http01);
    Ok(tokio::spawn(async move {
        run_renewal(
            RenewalPolicy::default(),
            clock,
            current,
            shutdown,
            issuer,
            random_unit,
        )
        .await;
        if let Some(t) = aux {
            t.abort();
        }
    }))
}
