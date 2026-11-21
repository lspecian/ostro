//! Cluster join (D-W4, D-W20) and the worker identity.
//!
//! `POST <join>/v1/workers/join` with JSON `{token, csr_pem, worker_id, labels}` returns
//! `{client_cert_pem, ca_cert_pem, endpoints: ["host:port"], renew_after_ms, cluster_id}`;
//! errors carry the ostro-api error body (`{"error": {"code", "message"}}`).
//!
//! - The private key is an ECDSA P-256 key generated in memory ([`rcgen`]); it is never
//!   written to disk and never printed (`Identity`'s `Debug` redacts it).
//! - **Bootstrap TLS ([`PinnedCaVerifier`]):** with `--ca-hash` the server certificate is
//!   accepted only if a certificate in the presented chain has `sha256(DER) == --ca-hash`
//!   and the leaf verifies (webpki: signature chain, validity, server name) against that
//!   certificate as the only root. The handshake fails before any request byte (the token
//!   included) is sent. Without `--ca-hash` the platform verifier is used (`files` / `acme`
//!   client listeners). With `--ca-hash` against a `files` / `acme` listener (its chain
//!   does not carry the cluster CA), the pinned handshake fails before any request byte and
//!   the join is retried once with the platform verifier (D-W20: those modes are verified
//!   with platform roots; 07-22 fix, the pin is then checked on the response below).
//! - In every case, with `--ca-hash`, `sha256(DER(ca_cert))` from the response must equal
//!   the pin before the CA is trusted for the cluster listener.
//! - Plain `http://` is accepted only for loopback hosts (logged warning).
//! - Errors never contain the token.
//!
//! The external-certificate path (`--tls-cert/--tls-key/--tls-ca`, cert-manager, D-W25)
//! skips the join: [`load_external`].

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{info, warn};

use crate::config::WorkerConfig;

/// Join path on the client listener.
pub const JOIN_PATH: &str = "/v1/workers/join";

/// Join failures. Messages never contain the join token.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JoinError {
    #[error("join URL: {0}")]
    Url(String),
    #[error("refusing plain http:// join to non-loopback host {0} (use https:// with --ca-hash)")]
    InsecureHttp(String),
    #[error("--ca-hash: {0}")]
    BadPin(String),
    #[error("join request failed: {0}")]
    Http(String),
    #[error("join refused (HTTP {status}): {message}")]
    Refused { status: u16, message: String },
    #[error("the CA certificate in the join response does not match --ca-hash")]
    CaHashMismatch,
    #[error("invalid join response: {0}")]
    BadResponse(String),
    #[error("key / CSR generation: {0}")]
    Csr(String),
    #[error("certificate files: {0}")]
    Files(String),
    #[error("renewal: {0}")]
    Renew(String),
}

/// The join request body.
#[derive(Serialize)]
struct JoinRequest<'a> {
    token: &'a str,
    csr_pem: &'a str,
    worker_id: &'a str,
    labels: &'a BTreeMap<String, String>,
}

/// The join response body.
#[derive(Debug, Clone, Deserialize)]
pub struct JoinResponse {
    pub client_cert_pem: String,
    pub ca_cert_pem: String,
    pub endpoints: Vec<String>,
    #[serde(default)]
    pub renew_after_ms: u64,
    #[serde(default)]
    pub cluster_id: String,
}

/// The worker's TLS identity for the cluster listener.
#[derive(Clone)]
pub struct Identity {
    /// Client certificate (PEM).
    pub cert_pem: String,
    /// Private key (PKCS#8 PEM), in memory only.
    key_pem: String,
    /// Cluster CA (PEM): the only root trusted for the cluster listener.
    pub ca_pem: String,
    /// Cluster listener endpoints (`host:port`).
    pub endpoints: Vec<String>,
    /// When to renew (relative to issuance); `None` = never.
    pub renew_after: Option<Duration>,
    /// Cluster id from the join response (empty for external certificates).
    pub cluster_id: String,
    /// Join-issued (renewable with `Renew`, re-joinable); `false` for external certs.
    pub joined: bool,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("key", &"<redacted>")
            .field("endpoints", &self.endpoints)
            .field("renew_after", &self.renew_after)
            .field("cluster_id", &self.cluster_id)
            .field("joined", &self.joined)
            .finish_non_exhaustive()
    }
}

impl Identity {
    /// The tonic client identity (certificate + in-memory key).
    pub fn tonic_identity(&self) -> tonic::transport::Identity {
        tonic::transport::Identity::from_pem(&self.cert_pem, &self.key_pem)
    }

    /// The same identity with a renewed certificate and key.
    fn renewed(&self, cert_pem: String, key_pem: String, renew_after: Option<Duration>) -> Self {
        Self {
            cert_pem,
            key_pem,
            renew_after,
            ..self.clone()
        }
    }
}

/// A fresh in-memory ECDSA P-256 key and a PKCS#10 CSR (PEM) for it.
pub fn new_key_and_csr(worker_id: &str) -> Result<(String, String), JoinError> {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
        .map_err(|e| JoinError::Csr(e.to_string()))?;
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new())
        .map_err(|e| JoinError::Csr(e.to_string()))?;
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, worker_id);
    let csr = params
        .serialize_request(&key)
        .and_then(|c| c.pem())
        .map_err(|e| JoinError::Csr(e.to_string()))?;
    Ok((key.serialize_pem(), csr))
}

/// Parse `sha256:<64 hex>`.
pub fn parse_ca_hash(h: &str) -> Result<[u8; 32], JoinError> {
    let hex = h
        .strip_prefix("sha256:")
        .ok_or_else(|| JoinError::BadPin("expected sha256:<hex>".into()))?;
    if hex.len() != 64 {
        return Err(JoinError::BadPin("expected 64 hex digits".into()));
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16)
            .map_err(|_| JoinError::BadPin("not hex".into()))?;
    }
    Ok(out)
}

/// `sha256:<hex>` of a DER certificate (what `ostro worker token create` prints).
pub fn ca_hash_of(der: &[u8]) -> String {
    let d: [u8; 32] = Sha256::digest(der).into();
    let mut s = String::from("sha256:");
    for b in d {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// First certificate of a PEM bundle, as DER.
pub fn pem_cert_der(pem: &str) -> Result<Vec<u8>, String> {
    CertificateDer::from_pem_slice(pem.as_bytes())
        .map(|c| c.to_vec())
        .map_err(|e| format!("not a PEM certificate: {e}"))
}

/// Whether `host` (a URL host) is a loopback name or address.
pub fn is_loopback_host(url: &reqwest::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let bare = host.trim_start_matches('[').trim_end_matches(']');
    match bare.parse::<IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => bare.eq_ignore_ascii_case("localhost"),
    }
}

/// Renewal deadline from a certificate: 2/3 of its validity window (time-free: computed
/// from the certificate's own not-before / not-after).
pub fn renew_after_from_cert(cert_pem: &str) -> Option<Duration> {
    let der = pem_cert_der(cert_pem).ok()?;
    let (_, cert) = x509_parser::parse_x509_certificate(&der).ok()?;
    let v = cert.validity();
    let secs = v.not_after.timestamp() - v.not_before.timestamp();
    u64::try_from(secs)
        .ok()
        .map(|s| Duration::from_millis(s * 1000 * 2 / 3))
}

fn renew_after(ms: u64, cert_pem: &str) -> Option<Duration> {
    if ms > 0 {
        Some(Duration::from_millis(ms))
    } else {
        renew_after_from_cert(cert_pem)
    }
}

/// The handshake error of [`PinnedCaVerifier`] when no chain certificate matches the pin.
const PIN_NOT_IN_CHAIN: &str = "no certificate in the server chain matches --ca-hash";

/// Bootstrap verifier: trust the presented chain only through the certificate whose
/// sha256 equals the pin.
#[derive(Debug)]
pub struct PinnedCaVerifier {
    pin: [u8; 32],
    provider: Arc<CryptoProvider>,
}

impl PinnedCaVerifier {
    pub fn new(pin: [u8; 32]) -> Self {
        Self {
            pin,
            provider: Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        }
    }
}

impl ServerCertVerifier for PinnedCaVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let matches =
            |c: &CertificateDer<'_>| <[u8; 32]>::from(Sha256::digest(c.as_ref())) == self.pin;
        let Some(ca) = std::iter::once(end_entity)
            .chain(intermediates.iter())
            .find(|c| matches(c))
        else {
            return Err(rustls::Error::General(PIN_NOT_IN_CHAIN.into()));
        };
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.clone().into_owned())
            .map_err(|e| rustls::Error::General(format!("pinned CA: {e}")))?;
        let inner = WebPkiServerVerifier::builder_with_provider(
            Arc::new(roots),
            Arc::clone(&self.provider),
        )
        .build()
        .map_err(|e| rustls::Error::General(format!("pinned CA verifier: {e}")))?;
        inner.verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn http_client(https: bool, pin: Option<[u8; 32]>) -> Result<reqwest::Client, JoinError> {
    let mut b = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10));
    if let (true, Some(pin)) = (https, pin) {
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|e| JoinError::Http(format!("TLS config: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedCaVerifier::new(pin)))
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        b = b.tls_backend_preconfigured(tls);
    }
    b.build().map_err(|e| JoinError::Http(e.to_string()))
}

/// Remove every occurrence of the token from a message (defence in depth: the server
/// never echoes it).
fn scrub(msg: &str, token: &str) -> String {
    if token.is_empty() {
        msg.to_owned()
    } else {
        msg.replace(token, "<redacted>")
    }
}

fn error_message(body: &str) -> String {
    let parsed: Option<serde_json::Value> = serde_json::from_str(body).ok();
    let msg = parsed
        .as_ref()
        .and_then(|v| {
            v.pointer("/error/message")
                .or_else(|| v.get("message"))
                .and_then(|m| m.as_str())
                .map(str::to_owned)
        })
        .unwrap_or_else(|| body.to_owned());
    msg.chars().take(512).collect()
}

/// Join the cluster: in-memory key + CSR, `POST /v1/workers/join` with CA-hash pinning.
pub async fn join(cfg: &WorkerConfig, worker_id: &str) -> Result<Identity, JoinError> {
    let join = cfg
        .join
        .as_deref()
        .ok_or_else(|| JoinError::Url("worker.join is not set".into()))?;
    let token = cfg
        .token
        .as_ref()
        .ok_or_else(|| JoinError::Url("worker.token is not set".into()))?;
    let url = reqwest::Url::parse(join).map_err(|e| JoinError::Url(e.to_string()))?;
    let https = match url.scheme() {
        "https" => true,
        "http" => {
            if !is_loopback_host(&url) {
                return Err(JoinError::InsecureHttp(
                    url.host_str().unwrap_or("").to_owned(),
                ));
            }
            warn!(join = %url, "joining over plain http:// (loopback only; never in production)");
            false
        }
        other => return Err(JoinError::Url(format!("unsupported scheme {other:?}"))),
    };
    let pin = cfg.ca_hash.as_deref().map(parse_ca_hash).transpose()?;
    if https && pin.is_none() {
        warn!("joining without --ca-hash: the client listener is verified with platform roots");
    }
    let (key_pem, csr_pem) = new_key_and_csr(worker_id)?;
    let endpoint = format!("{}{JOIN_PATH}", join.trim_end_matches('/'));
    let body = JoinRequest {
        token: token.expose(),
        csr_pem: &csr_pem,
        worker_id,
        labels: &cfg.labels,
    };
    let send = |client: reqwest::Client| {
        let req = client.post(&endpoint).json(&body);
        async move {
            req.send().await.map_err(|e| {
                use std::error::Error as _;
                let mut msg = e.to_string();
                let mut src = e.source();
                while let Some(s) = src {
                    msg.push_str(": ");
                    msg.push_str(&s.to_string());
                    src = s.source();
                }
                JoinError::Http(scrub(&msg, token.expose()))
            })
        }
    };
    let resp = match send(http_client(https, pin)?).await {
        // The listener's chain has no certificate matching the pin: a `files` / `acme`
        // listener (D-W20). The handshake failed before the request (and its token) was
        // sent; verify it with the platform roots instead. The response's CA must still
        // match the pin (below).
        Err(JoinError::Http(msg)) if pin.is_some() && msg.contains(PIN_NOT_IN_CHAIN) => {
            info!(
                "the client listener does not present the cluster CA (files / acme mode): \
                 verifying it with platform roots; the CA in the join response must match \
                 --ca-hash"
            );
            send(http_client(https, None)?).await.map_err(|e| match e {
                JoinError::Http(m) => JoinError::Http(format!(
                    "{PIN_NOT_IN_CHAIN}, and the platform verifier refused the listener: {m}"
                )),
                other => other,
            })?
        }
        other => other?,
    };
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| JoinError::Http(scrub(&e.to_string(), token.expose())))?;
    if !status.is_success() {
        return Err(JoinError::Refused {
            status: status.as_u16(),
            message: scrub(&error_message(&text), token.expose()),
        });
    }
    let jr: JoinResponse =
        serde_json::from_str(&text).map_err(|e| JoinError::BadResponse(e.to_string()))?;
    let ca_der = pem_cert_der(&jr.ca_cert_pem).map_err(JoinError::BadResponse)?;
    if let Some(pin) = pin
        && <[u8; 32]>::from(Sha256::digest(&ca_der)) != pin
    {
        return Err(JoinError::CaHashMismatch);
    }
    pem_cert_der(&jr.client_cert_pem).map_err(JoinError::BadResponse)?;
    if jr.endpoints.is_empty() {
        return Err(JoinError::BadResponse("no cluster endpoints".into()));
    }
    info!(cluster = %jr.cluster_id, endpoints = ?jr.endpoints, "joined cluster");
    Ok(Identity {
        renew_after: renew_after(jr.renew_after_ms, &jr.client_cert_pem),
        cert_pem: jr.client_cert_pem,
        key_pem,
        ca_pem: jr.ca_cert_pem,
        endpoints: jr.endpoints,
        cluster_id: jr.cluster_id,
        joined: true,
    })
}

/// The cert-manager identity: `tls_cert` / `tls_key` / `tls_ca` files and `endpoints`.
pub fn load_external(cfg: &WorkerConfig) -> Result<Identity, JoinError> {
    let read = |p: &Option<std::path::PathBuf>, what: &str| -> Result<String, JoinError> {
        let p = p
            .as_ref()
            .ok_or_else(|| JoinError::Files(format!("worker.{what} is not set")))?;
        std::fs::read_to_string(p)
            .map_err(|e| JoinError::Files(format!("{what} {}: {e}", p.display())))
    };
    let cert_pem = read(&cfg.tls_cert, "tls_cert")?;
    let key_pem = read(&cfg.tls_key, "tls_key")?;
    let ca_pem = read(&cfg.tls_ca, "tls_ca")?;
    pem_cert_der(&cert_pem).map_err(|e| JoinError::Files(format!("tls_cert: {e}")))?;
    pem_cert_der(&ca_pem).map_err(|e| JoinError::Files(format!("tls_ca: {e}")))?;
    if !key_pem.contains("PRIVATE KEY") {
        return Err(JoinError::Files("tls_key: not a PEM private key".into()));
    }
    Ok(Identity {
        cert_pem,
        key_pem,
        ca_pem,
        endpoints: cfg.endpoints.clone(),
        renew_after: None,
        cluster_id: String::new(),
        joined: false,
    })
}

fn der_to_pem(der: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut b64 = String::with_capacity(der.len() * 4 / 3 + 4);
    for c in der.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for i in 0..4 {
            if i <= c.len() {
                b64.push(T[((n >> (18 - 6 * i)) & 63) as usize] as char);
            } else {
                b64.push('=');
            }
        }
    }
    let mut out = String::from("-----BEGIN CERTIFICATE-----\n");
    for line in b64.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str("-----END CERTIFICATE-----\n");
    out
}

/// Renew the client certificate over the authenticated channel (`Renew{csr}`); returns
/// the new identity (new key, new certificate, same CA and endpoints).
pub(crate) async fn renew(
    client: &mut ostro_worker_proto::v1::execute_client::ExecuteClient<tonic::transport::Channel>,
    current: &Identity,
    worker_id: &str,
) -> Result<Identity, JoinError> {
    let (key_pem, csr_pem) = new_key_and_csr(worker_id)?;
    let resp = client
        .renew(ostro_worker_proto::v1::RenewRequest {
            csr: csr_pem.into_bytes(),
        })
        .await
        .map_err(|s| JoinError::Renew(format!("{}: {}", s.code(), s.message())))?
        .into_inner();
    let cert_pem = if resp.client_cert.starts_with(b"-----") {
        String::from_utf8(resp.client_cert)
            .map_err(|_| JoinError::Renew("certificate is not UTF-8 PEM".into()))?
    } else {
        der_to_pem(&resp.client_cert)
    };
    pem_cert_der(&cert_pem).map_err(JoinError::Renew)?;
    let after = renew_after(resp.renew_after_ms, &cert_pem);
    Ok(current.renewed(cert_pem, key_pem, after))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pin_parse_roundtrip() {
        let h = ca_hash_of(b"abc");
        assert_eq!(
            parse_ca_hash(&h).unwrap(),
            <[u8; 32]>::from(Sha256::digest(b"abc"))
        );
        assert!(parse_ca_hash("sha1:00").is_err());
        assert!(parse_ca_hash("sha256:zz").is_err());
    }

    #[test]
    fn loopback_hosts() {
        for (u, l) in [
            ("http://127.0.0.1:1/", true),
            ("http://localhost:1/", true),
            ("http://[::1]:1/", true),
            ("http://10.0.0.1:1/", false),
            ("http://example.com/", false),
        ] {
            assert_eq!(is_loopback_host(&reqwest::Url::parse(u).unwrap()), l, "{u}");
        }
    }

    #[test]
    fn key_never_in_debug_and_der_pem_roundtrip() {
        let (key, csr) = new_key_and_csr("w1").unwrap();
        assert!(csr.contains("CERTIFICATE REQUEST"));
        let id = Identity {
            cert_pem: String::new(),
            key_pem: key.clone(),
            ca_pem: String::new(),
            endpoints: vec![],
            renew_after: None,
            cluster_id: String::new(),
            joined: true,
        };
        let dbg = format!("{id:?}");
        assert!(!dbg.contains(&key[30..60]), "{dbg}");

        let ck = rcgen::KeyPair::generate().unwrap();
        let cert = rcgen::CertificateParams::new(vec!["x".to_string()])
            .unwrap()
            .self_signed(&ck)
            .unwrap();
        assert_eq!(der_to_pem(cert.der()), cert.pem().replace("\r\n", "\n"));
        assert!(renew_after_from_cert(&cert.pem()).is_some());
    }

    #[test]
    fn scrubs_token() {
        assert_eq!(
            scrub("bad wkr_a.secret here", "wkr_a.secret"),
            "bad <redacted> here"
        );
    }
}
