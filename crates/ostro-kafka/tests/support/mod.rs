//! In-process fake Kafka broker for the transport tests (14-31).
//!
//! Tokio TCP (optionally TLS with a CA + server certificate generated at test time by
//! `rcgen`; no key material is committed), kafka-protocol `broker` encodings. Answers
//! ApiVersions (configurable ranges; an "old broker" mode that refuses v3 with the v0-form
//! `UNSUPPORTED_VERSION` answer), SaslHandshake/SaslAuthenticate (PLAIN and SCRAM-SHA-256/512
//! for one known user, server side written independently from the client with aws-lc-rs) and a
//! minimal Metadata. Knobs send oversize, negative, truncated or garbage frames, wrong
//! correlation ids, stay silent, or drop the socket.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aws_lc_rs::{digest, hmac, pbkdf2};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::MetadataResponseBroker;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, BrokerId, MetadataRequest, MetadataResponse,
    ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse, SaslHandshakeRequest,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{
    Decodable, Encodable, HeaderVersion, StrBytes, decode_request_header_from_buffer,
};
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, SanType,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;

/// The SASL user the fake broker knows.
pub const USER: &str = "ostro";
/// Its password.
pub const PASSWORD: &str = "fake-broker-pw-7f3a";

/// Apache Kafka 4.1 ranges for the client's APIs.
pub fn kafka_41_ranges() -> Vec<(i16, i16, i16)> {
    vec![
        (18, 0, 4),
        (3, 0, 13),
        (10, 0, 6),
        (2, 1, 10),
        (1, 4, 17),
        (9, 1, 9),
        (8, 2, 9),
        (17, 0, 1),
        (36, 0, 2),
    ]
}

/// How to misbehave when answering a given API.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Misbehave {
    /// Size prefix above 64 MiB (nothing follows).
    Oversize,
    /// Negative size prefix.
    NegativeSize,
    /// Size 100, correlation id, 10 bytes, then close.
    Truncated,
    /// Correct size and correlation id, random body.
    Garbage,
    /// Correct size and correlation id, body claiming a huge array.
    HugeArray,
    /// Never answer.
    Silent,
    /// Answer with another correlation id.
    WrongCorrelation,
    /// Close the socket instead of answering.
    Close,
}

/// Fake broker options.
#[derive(Clone, Debug)]
pub struct Opts {
    /// Advertised `(key, min, max)` ranges.
    pub api_versions: Vec<(i16, i16, i16)>,
    /// Refuse ApiVersions above v2 the way a pre-KIP-511 broker does.
    pub old_api_versions: bool,
    /// Require SASL with these mechanisms (names) before Metadata.
    pub sasl: Option<Vec<&'static str>>,
    /// Serve TLS.
    pub tls: bool,
    /// Misbehave on `(api key, how, times)`.
    pub misbehave: Option<(i16, Misbehave, usize)>,
    /// Corrupt the SCRAM server signature.
    pub bad_server_signature: bool,
}

impl Default for Opts {
    fn default() -> Self {
        Opts {
            api_versions: kafka_41_ranges(),
            old_api_versions: false,
            sasl: None,
            tls: false,
            misbehave: None,
            bad_server_signature: false,
        }
    }
}

/// What the broker saw.
#[derive(Debug, Default)]
pub struct Seen {
    /// Accepted connections.
    pub connections: AtomicUsize,
    /// `(api key, version)` of every request.
    pub requests: Mutex<Vec<(i16, i16)>>,
    misbehaved: AtomicUsize,
}

/// A running fake broker (stops when dropped).
pub struct FakeBroker {
    /// `127.0.0.1:<port>`.
    pub addr: SocketAddr,
    /// Observations.
    pub seen: Arc<Seen>,
    /// PEM path of the CA that signed the TLS certificate (TLS mode).
    pub ca_pem: Option<PathBuf>,
    /// PEM path of an unrelated CA (TLS mode).
    pub other_ca_pem: Option<PathBuf>,
    _dir: tempfile::TempDir,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl FakeBroker {
    /// The `host:port` string.
    pub fn bootstrap(&self) -> String {
        self.addr.to_string()
    }

    /// Requests seen for `key`.
    pub fn count(&self, key: i16) -> usize {
        self.seen
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == key)
            .count()
    }

    /// Versions seen for `key`.
    pub fn versions_of(&self, key: i16) -> Vec<i16> {
        self.seen
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == key)
            .map(|(_, v)| *v)
            .collect()
    }

    /// Start a broker.
    pub async fn start(opts: Opts) -> FakeBroker {
        let dir = tempfile::tempdir().unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (acceptor, ca_pem, other_ca_pem) = if opts.tls {
            let (ca, server_cfg) = tls_material();
            let ca_path = dir.path().join("ca.pem");
            std::fs::write(&ca_path, ca).unwrap();
            let (other, _) = tls_material();
            let other_path = dir.path().join("other-ca.pem");
            std::fs::write(&other_path, other).unwrap();
            (
                Some(tokio_rustls::TlsAcceptor::from(server_cfg)),
                Some(ca_path),
                Some(other_path),
            )
        } else {
            (None, None, None)
        };
        let seen = Arc::new(Seen::default());
        let opts = Arc::new(opts);
        let task = {
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                loop {
                    let Ok((sock, _)) = listener.accept().await else {
                        return;
                    };
                    seen.connections.fetch_add(1, Ordering::SeqCst);
                    let (seen, opts, acceptor) =
                        (Arc::clone(&seen), Arc::clone(&opts), acceptor.clone());
                    tokio::spawn(async move {
                        match acceptor {
                            None => serve(sock, addr, seen, opts).await,
                            Some(a) => {
                                if let Ok(tls) = a.accept(sock).await {
                                    serve(tls, addr, seen, opts).await;
                                }
                            }
                        }
                    });
                }
            })
        };
        FakeBroker {
            addr,
            seen,
            ca_pem,
            other_ca_pem,
            _dir: dir,
            task,
        }
    }
}

/// A fresh CA (PEM) and a rustls server config with a certificate for 127.0.0.1/localhost.
fn tls_material() -> (String, Arc<rustls::ServerConfig>) {
    let mut p = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "ostro-kafka fake broker CA");
    p.distinguished_name = dn;
    p.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    p.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let ca_cert = p.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(p, ca_key);

    let mut l = CertificateParams::default();
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, "localhost");
    l.distinguished_name = dn;
    l.subject_alt_names = vec![
        SanType::DnsName("localhost".to_owned().try_into().unwrap()),
        SanType::IpAddress([127, 0, 0, 1].into()),
    ];
    l.is_ca = IsCa::ExplicitNoCa;
    l.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    l.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    l.use_authority_key_identifier_extension = true;
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).unwrap();
    let leaf = l.signed_by(&key, &issuer).unwrap();

    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let cfg = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![leaf.der().clone()],
            rustls_pki_types::PrivateKeyDer::Pkcs8(key.serialize_der().into()),
        )
        .unwrap();
    (ca_cert.pem(), Arc::new(cfg))
}

fn frame<R: Encodable + HeaderVersion>(corr: i32, version: i16, resp: &R) -> BytesMut {
    let mut b = BytesMut::new();
    b.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(corr)
        .encode(&mut b, R::header_version(version))
        .unwrap();
    resp.encode(&mut b, version).unwrap();
    let len = (b.len() - 4) as i32;
    b[..4].copy_from_slice(&len.to_be_bytes());
    b
}

#[derive(Default)]
struct ConnState {
    mechanism: Option<String>,
    authenticated: bool,
    scram: Option<(String, String)>, // (client-first-bare, server-first)
}

async fn read_frame<S: AsyncRead + Unpin>(s: &mut S) -> Option<Bytes> {
    let n = s.read_i32().await.ok()?;
    if !(0..=(16 << 20)).contains(&n) {
        return None;
    }
    let mut b = vec![0u8; n as usize];
    s.read_exact(&mut b).await.ok()?;
    Some(Bytes::from(b))
}

async fn serve<S: AsyncRead + AsyncWrite + Unpin>(
    mut s: S,
    addr: SocketAddr,
    seen: Arc<Seen>,
    opts: Arc<Opts>,
) {
    let mut st = ConnState::default();
    while let Some(mut buf) = read_frame(&mut s).await {
        let Ok(header) = decode_request_header_from_buffer(&mut buf) else {
            return;
        };
        let (key, v, corr) = (
            header.request_api_key,
            header.request_api_version,
            header.correlation_id,
        );
        seen.requests.lock().unwrap().push((key, v));
        if let Some((mkey, how, times)) = opts.misbehave
            && mkey == key
            && seen.misbehaved.fetch_add(1, Ordering::SeqCst) < times
        {
            if !misbehave(&mut s, how, corr).await {
                return;
            }
            continue;
        }
        let out = match ApiKey::try_from(key) {
            Ok(ApiKey::ApiVersions) => {
                let _ = ApiVersionsRequest::decode(&mut buf, v);
                if opts.old_api_versions && v > 2 {
                    let r = ApiVersionsResponse::default()
                        .with_error_code(35)
                        .with_api_keys(vec![api_version(18, 0, 2)]);
                    frame(corr, 0, &r)
                } else {
                    let r = ApiVersionsResponse::default().with_api_keys(
                        opts.api_versions
                            .iter()
                            .map(|&(k, lo, hi)| api_version(k, lo, hi))
                            .collect(),
                    );
                    frame(corr, v, &r)
                }
            }
            Ok(ApiKey::SaslHandshake) => {
                let req = SaslHandshakeRequest::decode(&mut buf, v).unwrap();
                let enabled = opts.sasl.clone().unwrap_or_default();
                let mut r = SaslHandshakeResponse::default().with_mechanisms(
                    enabled
                        .iter()
                        .map(|m| StrBytes::from_static_str(m))
                        .collect(),
                );
                if enabled.contains(&req.mechanism.as_str()) {
                    st.mechanism = Some(req.mechanism.to_string());
                } else {
                    r = r.with_error_code(33);
                }
                frame(corr, v, &r)
            }
            Ok(ApiKey::SaslAuthenticate) => {
                let req = SaslAuthenticateRequest::decode(&mut buf, v).unwrap();
                let r = sasl_step(&mut st, &req.auth_bytes, opts.bad_server_signature);
                frame(corr, v, &r)
            }
            Ok(ApiKey::Metadata) => {
                if opts.sasl.is_some() && !st.authenticated {
                    return; // Kafka drops unauthenticated non-SASL requests
                }
                let _ = MetadataRequest::decode(&mut buf, v);
                let r = MetadataResponse::default()
                    .with_brokers(vec![
                        MetadataResponseBroker::default()
                            .with_node_id(BrokerId(1))
                            .with_host(StrBytes::from_static_str("127.0.0.1"))
                            .with_port(i32::from(addr.port())),
                    ])
                    .with_cluster_id(Some(StrBytes::from_static_str("fake-cluster")))
                    .with_controller_id(BrokerId(1));
                frame(corr, v, &r)
            }
            _ => return,
        };
        if s.write_all(&out).await.is_err() || s.flush().await.is_err() {
            return;
        }
    }
}

fn api_version(k: i16, lo: i16, hi: i16) -> ApiVersion {
    ApiVersion::default()
        .with_api_key(k)
        .with_min_version(lo)
        .with_max_version(hi)
}

/// Returns false when the connection should end.
async fn misbehave<S: AsyncWrite + Unpin>(s: &mut S, how: Misbehave, corr: i32) -> bool {
    let mut b = BytesMut::new();
    let keep = match how {
        Misbehave::Oversize => {
            b.put_i32((64 << 20) + 1);
            b.put_i32(corr);
            true
        }
        Misbehave::NegativeSize => {
            b.put_i32(-1);
            true
        }
        Misbehave::Truncated => {
            b.put_i32(100);
            b.put_i32(corr);
            b.put_slice(&[0u8; 10]);
            false
        }
        Misbehave::Garbage => {
            let mut x = 0x5eed_u64;
            let body: Vec<u8> = (0..64)
                .map(|_| {
                    x = x
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    (x >> 33) as u8
                })
                .collect();
            b.put_i32(4 + 1 + body.len() as i32);
            b.put_i32(corr);
            b.put_u8(0); // empty header tags (flexible header)
            b.put_slice(&body);
            true
        }
        Misbehave::HugeArray => {
            // flexible header, throttle, then a broker array claiming 2^28 entries
            b.put_i32(4 + 1 + 4 + 5);
            b.put_i32(corr);
            b.put_u8(0);
            b.put_i32(0);
            b.put_slice(&[0x80, 0x80, 0x80, 0x80, 0x01]);
            true
        }
        Misbehave::Silent => true,
        Misbehave::WrongCorrelation => {
            b.put_i32(5);
            b.put_i32(corr.wrapping_add(1000));
            b.put_u8(0);
            true
        }
        Misbehave::Close => false,
    };
    if !b.is_empty() && (s.write_all(&b).await.is_err() || s.flush().await.is_err()) {
        return false;
    }
    keep
}

fn auth_failed(msg: &str) -> SaslAuthenticateResponse {
    SaslAuthenticateResponse::default()
        .with_error_code(58)
        .with_error_message(Some(StrBytes::from_string(msg.to_owned())))
}

const SALT: &[u8] = b"fake-broker-salt";
const ITERS: u32 = 4096;

fn scram_algos(
    mech: &str,
) -> (
    hmac::Algorithm,
    &'static digest::Algorithm,
    pbkdf2::Algorithm,
    usize,
) {
    if mech == "SCRAM-SHA-512" {
        (
            hmac::HMAC_SHA512,
            &digest::SHA512,
            pbkdf2::PBKDF2_HMAC_SHA512,
            64,
        )
    } else {
        (
            hmac::HMAC_SHA256,
            &digest::SHA256,
            pbkdf2::PBKDF2_HMAC_SHA256,
            32,
        )
    }
}

fn sasl_step(st: &mut ConnState, token: &Bytes, bad_sig: bool) -> SaslAuthenticateResponse {
    let Some(mech) = st.mechanism.clone() else {
        return auth_failed("no handshake");
    };
    if mech == "PLAIN" {
        let parts: Vec<&[u8]> = token.split(|b| *b == 0).collect();
        if parts.len() == 3 && parts[1] == USER.as_bytes() && parts[2] == PASSWORD.as_bytes() {
            st.authenticated = true;
            return SaslAuthenticateResponse::default();
        }
        return auth_failed("Authentication failed: Invalid username or password");
    }
    let Ok(msg) = std::str::from_utf8(token) else {
        return auth_failed("not utf-8");
    };
    let (h, d, p, len) = scram_algos(&mech);
    match st.scram.take() {
        None => {
            let Some(bare) = msg.strip_prefix("n,,") else {
                return auth_failed("bad gs2 header");
            };
            let Some((user, cnonce)) = bare.strip_prefix("n=").and_then(|r| r.split_once(",r="))
            else {
                return auth_failed("bad client-first");
            };
            if user != USER {
                return auth_failed("unknown user");
            }
            let server_first =
                format!("r={cnonce}fakeServerNonce,s={},i={ITERS}", B64.encode(SALT));
            st.scram = Some((bare.to_owned(), server_first.clone()));
            SaslAuthenticateResponse::default().with_auth_bytes(Bytes::from(server_first))
        }
        Some((bare, server_first)) => {
            let Some((without, proof)) = msg.rsplit_once(",p=") else {
                return auth_failed("bad client-final");
            };
            let Ok(proof) = B64.decode(proof) else {
                return auth_failed("bad proof");
            };
            let mut salted = vec![0u8; len];
            pbkdf2::derive(
                p,
                std::num::NonZeroU32::new(ITERS).unwrap(),
                SALT,
                PASSWORD.as_bytes(),
                &mut salted,
            );
            let ck = hmac::sign(&hmac::Key::new(h, &salted), b"Client Key");
            let stored = digest::digest(d, ck.as_ref());
            let auth = format!("{bare},{server_first},{without}");
            let sig = hmac::sign(&hmac::Key::new(h, stored.as_ref()), auth.as_bytes());
            if proof.len() != len {
                return auth_failed(&format!(
                    "Authentication failed during authentication due to invalid credentials with SASL mechanism {mech}"
                ));
            }
            let recovered: Vec<u8> = proof.iter().zip(sig.as_ref()).map(|(a, b)| a ^ b).collect();
            if digest::digest(d, &recovered).as_ref() != stored.as_ref() {
                return auth_failed(&format!(
                    "Authentication failed during authentication due to invalid credentials with SASL mechanism {mech}"
                ));
            }
            let sk = hmac::sign(&hmac::Key::new(h, &salted), b"Server Key");
            let mut ssig = hmac::sign(&hmac::Key::new(h, sk.as_ref()), auth.as_bytes())
                .as_ref()
                .to_vec();
            if bad_sig {
                ssig[0] ^= 0xff;
            }
            st.authenticated = true;
            SaslAuthenticateResponse::default()
                .with_auth_bytes(Bytes::from(format!("v={}", B64.encode(ssig))))
        }
    }
}
