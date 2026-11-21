//! ACME mode of the client listener (plan 07-10, SEC-01, D-W6, D-W23).
//!
//! Fake-ACME tests always run and never touch the network beyond loopback: the
//! TLS-ALPN-01 challenge cert through the real listener, the HTTP-01 responder, the
//! renewal schedule and backoff on the injected clock, cache reuse with an unroutable
//! directory. The Pebble tests run only when `OSTRO_TEST_ACME_DIRECTORY` (plus
//! `OSTRO_TEST_ACME_CA`) is set; otherwise they print `SKIP` and pass. No public ACME
//! directory is ever contacted.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use ostro_partition::{Clock, ManualClock, SystemClock};
use ostro_server::config::{AcmeChallenge, ApiTlsMode, ServerConfig};
use ostro_server::tls::acme::{
    self, BACKOFF_MAX, BACKOFF_MIN, Http01Responder, Obtain, RenewalPolicy, Validity, backoff_delay,
};
use ostro_server::tls::ca::sha256_fingerprint;
use ostro_server::tls::{self, ACME_TLS_ALPN, ClusterCa, San, TlsError, TlsState};
use rustls::DigitallySignedStruct;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const CLUSTER: &str = "0190f5c2-7d3e-7000-8000-0000000000ab";
/// TEST-NET-1 (RFC 5737): never routed, so nothing can answer.
const UNROUTABLE_DIRECTORY: &str = "https://192.0.2.1:14000/dir";
const DAY_MS: u64 = 86_400_000;

// ------------------------------------------------------------------------------------
// helpers
// ------------------------------------------------------------------------------------

/// Accepts any server certificate and signature (the TLS-ALPN-01 challenge cert is
/// self-signed and carries a critical extension webpki does not know).
#[derive(Debug)]
struct AcceptAny(Arc<CryptoProvider>);

impl ServerCertVerifier for AcceptAny {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // rustls' helper parses the leaf with webpki, which rejects the challenge cert's
        // critical acmeIdentifier; the test only inspects the served certificate.
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        // rustls' helper parses the leaf with webpki, which rejects the challenge cert's
        // critical acmeIdentifier; the test only inspects the served certificate.
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Handshake to `addr` with SNI `sni` offering `alpn`; returns (leaf DER, ALPN).
async fn handshake(
    addr: SocketAddr,
    alpn: &[&[u8]],
    sni: &str,
) -> std::io::Result<(CertificateDer<'static>, Option<Vec<u8>>)> {
    let provider = Arc::new(ostro_api::tls_provider());
    let mut cc = rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AcceptAny(provider)))
        .with_no_client_auth();
    cc.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
    let conn = tokio_rustls::TlsConnector::from(Arc::new(cc));
    let tcp = TcpStream::connect(addr).await?;
    let s = conn
        .connect(ServerName::try_from(sni.to_owned()).expect("sni"), tcp)
        .await?;
    let (_, c) = s.get_ref();
    let leaf = c.peer_certificates().expect("peer certs")[0].clone();
    Ok((leaf, c.alpn_protocol().map(<[u8]>::to_vec)))
}

fn acme_cfg(domain: &str, directory: &str) -> ServerConfig {
    let mut c = ServerConfig::dev_defaults();
    c.api.listen = "127.0.0.1:0".parse().expect("listen");
    c.api.tls.mode = ApiTlsMode::Acme;
    c.api.tls.acme.domains = vec![domain.to_owned()];
    c.api.tls.acme.directory = directory.to_owned();
    c
}

/// A certificate for `domain` issued by a throwaway CA, stored as the ACME cache.
fn seed_cache(cache: &Path, domain: &str, lifetime: Duration) -> String {
    let pki = tempfile::tempdir().expect("pki");
    let ca = ClusterCa::load_or_create(pki.path(), CLUSTER).expect("ca");
    let leaf = ca
        .issue_leaf(
            domain,
            &[San::Dns(domain.into())],
            &[rcgen::ExtendedKeyUsagePurpose::ServerAuth],
            lifetime,
        )
        .expect("leaf");
    acme::store_cert(cache, &leaf.chain_pem(), &leaf.key_pem()).expect("store");
    leaf.fingerprint()
}

#[cfg(unix)]
fn mode_of(p: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).expect("meta").permissions().mode() & 0o777
}

fn app() -> Router {
    Router::new().route("/hello", get(|| async { "hello" }))
}

/// The acmeIdentifier extension (OID 1.3.6.1.5.5.7.1.31) of `der`: (critical, value).
fn acme_identifier(der: &[u8]) -> Option<(bool, Vec<u8>)> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).expect("parse");
    cert.extensions()
        .iter()
        .find(|e| e.oid.to_id_string() == "1.3.6.1.5.5.7.1.31")
        .map(|e| (e.critical, e.value.to_vec()))
}

fn dns_sans(der: &[u8]) -> Vec<String> {
    let (_, cert) = x509_parser::parse_x509_certificate(der).expect("parse");
    let mut out = Vec::new();
    if let Ok(Some(san)) = cert.subject_alternative_name() {
        for n in &san.value.general_names {
            if let x509_parser::extensions::GeneralName::DNSName(d) = n {
                out.push((*d).to_owned());
            }
        }
    }
    out
}

// ------------------------------------------------------------------------------------
// fake-ACME tests
// ------------------------------------------------------------------------------------

#[test]
fn validate_rejects_bad_config() {
    let ok = acme_cfg("ostro.example", UNROUTABLE_DIRECTORY).api.tls.acme;
    acme::validate(&ok).expect("valid");
    let mut s = ok.clone();
    s.domains.clear();
    assert!(
        acme::validate(&s)
            .unwrap_err()
            .to_string()
            .contains("domains")
    );
    let mut s = ok.clone();
    s.domains = vec!["10.0.0.1".into()];
    assert!(acme::validate(&s).is_err());
    let mut s = ok.clone();
    s.domains = vec!["bad domain".into()];
    assert!(acme::validate(&s).is_err());
    let mut s = ok;
    s.directory = "http://acme.example/dir".into();
    assert!(
        acme::validate(&s)
            .unwrap_err()
            .to_string()
            .contains("https://")
    );
}

#[test]
fn challenge_cert_carries_critical_acme_identifier() {
    let ka = "tok3n.thumbprint";
    let digest = Sha256::digest(ka.as_bytes());
    let ck = acme::challenge_cert("ostro.example", &digest).expect("challenge");
    let der = ck.cert[0].as_ref();
    let (critical, value) = acme_identifier(der).expect("acmeIdentifier present");
    assert!(critical, "acmeIdentifier must be critical (RFC 8737)");
    let mut want = vec![0x04, 0x20];
    want.extend_from_slice(&digest);
    assert_eq!(value, want, "OCTET STRING(sha256(key authorization))");
    assert_eq!(dns_sans(der), ["ostro.example"]);
    assert!(acme::challenge_cert("ostro.example", &[0u8; 31]).is_err());
}

/// Cached certificate through the real listener + `acme::start` (unroutable directory),
/// and the TLS-ALPN-01 path end to end: the listener advertises `acme-tls/1`, a
/// ClientHello offering only `acme-tls/1` gets the challenge cert, normal clients the
/// served one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_serves_cached_cert_and_tls_alpn_challenge() {
    let dir = tempfile::tempdir().expect("tmp");
    let cfg = acme_cfg("ostro.example", UNROUTABLE_DIRECTORY);
    let cached_fp = seed_cache(
        &dir.path().join("tls").join("acme"),
        "ostro.example",
        Duration::from_secs(90 * 86_400),
    );
    let st = tls::init(&cfg, dir.path(), CLUSTER).expect("init");
    assert_eq!(
        st.alpn().iter().map(|p| p.as_slice()).collect::<Vec<_>>(),
        [b"h2".as_slice(), b"http/1.1", ACME_TLS_ALPN]
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(tls::listener::serve(
        Arc::clone(&st),
        listener,
        app(),
        shutdown.clone(),
    ));
    // The cached cert is installed before serving (no directory contact needed).
    let mut leaf = None;
    for _ in 0..100 {
        if let Ok(r) = handshake(addr, &[b"h2", b"http/1.1"], "ostro.example").await {
            leaf = Some(r);
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let (leaf, alpn) = leaf.expect("serves the cached cert");
    assert_eq!(sha256_fingerprint(&leaf), cached_fp);
    assert_eq!(alpn.as_deref(), Some(b"h2".as_slice()));

    // acme-tls/1 only, no challenge registered: nothing.
    assert!(
        handshake(addr, &[ACME_TLS_ALPN], "ostro.example")
            .await
            .is_err()
    );
    let digest = Sha256::digest(b"tok.thumb");
    let ck = acme::challenge_cert("ostro.example", &digest).expect("challenge");
    let challenge_fp = sha256_fingerprint(&ck.cert[0]);
    st.resolver().set_challenge("ostro.example", Some(ck));
    let (leaf, alpn) = handshake(addr, &[ACME_TLS_ALPN], "ostro.example")
        .await
        .expect("validator handshake");
    assert_eq!(sha256_fingerprint(&leaf), challenge_fp);
    assert_eq!(alpn.as_deref(), Some(ACME_TLS_ALPN));
    let (critical, _) = acme_identifier(&leaf).expect("acmeIdentifier");
    assert!(critical);
    let (leaf, _) = handshake(addr, &[b"h2"], "ostro.example")
        .await
        .expect("normal");
    assert_eq!(sha256_fingerprint(&leaf), cached_fp);
    // No account was created: the directory was never contacted.
    let cache = dir.path().join("tls").join("acme");
    assert!(!acme::account_file(&cache, UNROUTABLE_DIRECTORY).exists());

    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .expect("stops")
        .expect("join")
        .expect("serve ok");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http01_responder_serves_token_and_404s_unknown() {
    let r = Http01Responder::new();
    let shutdown = CancellationToken::new();
    let (addr, task) = r
        .serve("127.0.0.1:0".parse().expect("addr"), shutdown.clone())
        .expect("bind");
    r.insert("tok-1", "tok-1.thumb");
    let http = reqwest::Client::new();
    let url = |t: &str| format!("http://{addr}/.well-known/acme-challenge/{t}");
    let rsp = http.get(url("tok-1")).send().await.expect("get");
    assert_eq!(rsp.status(), 200);
    assert_eq!(rsp.text().await.expect("body"), "tok-1.thumb");
    let rsp = http.get(url("unknown")).send().await.expect("get");
    assert_eq!(rsp.status(), 404);
    let rsp = http
        .get(format!("http://{addr}/other"))
        .send()
        .await
        .expect("get");
    assert_eq!(rsp.status(), 404);
    r.remove("tok-1");
    let rsp = http.get(url("tok-1")).send().await.expect("get");
    assert_eq!(rsp.status(), 404);
    // A second bind on the same address is a startup error.
    assert!(
        Http01Responder::new()
            .serve(addr, shutdown.clone())
            .is_err()
    );
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("stops")
        .expect("join");
}

#[test]
fn renewal_schedule_math() {
    let v = Validity {
        not_before_ms: 1_000,
        not_after_ms: 1_000 + 90 * DAY_MS,
    };
    assert_eq!(v.lifetime_ms(), 90 * DAY_MS);
    assert_eq!(v.renew_at_ms(), 1_000 + 60 * DAY_MS, "2/3 of the lifetime");
    let short = Validity {
        not_before_ms: 0,
        not_after_ms: 6 * DAY_MS,
    };
    assert_eq!(short.renew_at_ms(), 4 * DAY_MS);

    // Backoff: 1 min -> 1 h, jittered, always inside the bounds.
    assert_eq!(backoff_delay(1, 0.0), BACKOFF_MIN);
    assert_eq!(backoff_delay(2, 0.0), Duration::from_secs(90));
    assert_eq!(backoff_delay(2, 0.5), Duration::from_secs(120));
    assert_eq!(backoff_delay(3, 0.5), Duration::from_secs(240));
    assert_eq!(backoff_delay(100, 0.999_999), BACKOFF_MAX);
    for f in 1..64 {
        let mut prev_lo = Duration::ZERO;
        for j in [0.0, 0.25, 0.5, 0.75, 0.999_999] {
            let d = backoff_delay(f, j);
            assert!((BACKOFF_MIN..=BACKOFF_MAX).contains(&d), "{f} {j} {d:?}");
            assert!(d >= prev_lo);
            prev_lo = d;
        }
        assert!(backoff_delay(f + 1, 0.0) >= backoff_delay(f, 0.0));
    }
    assert!(backoff_delay(7, 0.0) >= Duration::from_secs(2700));
    for _ in 0..100 {
        let u = acme::random_unit();
        assert!((0.0..1.0).contains(&u));
    }
}

/// Scripted issuer: results in order, call times recorded (tokio virtual time).
struct Scripted {
    results: Arc<Mutex<Vec<Result<Validity, TlsError>>>>,
    calls: Arc<Mutex<Vec<tokio::time::Instant>>>,
    count: Arc<AtomicU32>,
}

impl Obtain for Scripted {
    async fn obtain(&mut self) -> Result<Validity, TlsError> {
        self.calls
            .lock()
            .expect("calls")
            .push(tokio::time::Instant::now());
        self.count.fetch_add(1, Ordering::SeqCst);
        let mut r = self.results.lock().expect("results");
        if r.is_empty() {
            Err(TlsError::Cert("script exhausted".into()))
        } else {
            r.remove(0)
        }
    }
}

/// The loop on the injected clock with a millisecond policy (real tokio time: the crate
/// has no tokio `test-util`, so `start_paused` is unavailable).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn renewal_loop_follows_injected_clock_and_backs_off() {
    let policy = RenewalPolicy {
        backoff_min: Duration::from_millis(40),
        backoff_max: Duration::from_millis(400),
        max_sleep: Duration::from_millis(5),
    };
    let t0 = 1_800_000_000_000u64;
    let clock = ManualClock::new(t0);
    let first = Validity {
        not_before_ms: t0,
        not_after_ms: t0 + 90 * DAY_MS,
    };
    let renewed = Validity {
        not_before_ms: t0 + 60 * DAY_MS,
        not_after_ms: t0 + 150 * DAY_MS,
    };
    let results = Arc::new(Mutex::new(vec![
        Err(TlsError::Cert("directory down".into())),
        Err(TlsError::Cert("still down".into())),
        Ok(renewed),
    ]));
    let calls = Arc::new(Mutex::new(Vec::new()));
    let count = Arc::new(AtomicU32::new(0));
    let issuer = Scripted {
        results: Arc::clone(&results),
        calls: Arc::clone(&calls),
        count: Arc::clone(&count),
    };
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(acme::run_renewal(
        policy,
        Arc::new(clock.clone()) as Arc<dyn Clock>,
        Some(first),
        shutdown.clone(),
        issuer,
        || 0.0,
    ));
    let wait_for = |n: u32| {
        let count = Arc::clone(&count);
        async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                while count.load(Ordering::SeqCst) < n {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("issuer called");
        }
    };
    // Elapsed tokio time alone never makes renewal due: only the injected clock does.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0);
    clock.set(t0 + 60 * DAY_MS - 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(count.load(Ordering::SeqCst), 0, "not due 1 ms early");
    clock.set(t0 + 60 * DAY_MS);
    // Due: attempt, fail, retry after 40 ms, fail, retry after 60 ms, succeed.
    wait_for(3).await;
    {
        let c = calls.lock().expect("calls");
        assert!(
            c[1] - c[0] >= Duration::from_millis(40),
            "{:?}",
            c[1] - c[0]
        );
        assert!(
            c[2] - c[1] >= Duration::from_millis(60),
            "{:?}",
            c[2] - c[1]
        );
    }
    // The renewed cert is next due at t0 + 120 d.
    clock.set(t0 + 120 * DAY_MS - 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(count.load(Ordering::SeqCst), 3);
    clock.set(t0 + 120 * DAY_MS);
    wait_for(4).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("stops on shutdown")
        .expect("join");
}

/// Cache reuse: a valid cached cert is installed without contacting the directory; a
/// cert in its last third is installed and renewal starts at once; an expired one, or one
/// that does not cover the domains, is not installed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_reuse_and_rejection() {
    let now = SystemClock.now_ms();
    let lifetime = Duration::from_secs(90 * 86_400);
    let cases: [(&str, &str, u64, bool); 4] = [
        ("fresh", "ostro.example", now, true),
        ("renewal due", "ostro.example", now + 80 * DAY_MS, true),
        ("expired", "ostro.example", now + 100 * DAY_MS, false),
        ("other domain", "other.example", now, false),
    ];
    for (name, cached_domain, clock_ms, installed) in cases {
        let dir = tempfile::tempdir().expect("tmp");
        let cache = dir.path().join("acme-cache");
        let mut cfg = acme_cfg("ostro.example", UNROUTABLE_DIRECTORY);
        cfg.api.tls.acme.cache_dir = Some(cache.clone());
        let fp = seed_cache(&cache, cached_domain, lifetime);
        #[cfg(unix)]
        {
            assert_eq!(mode_of(&cache.join(acme::CERT_FILE)), 0o600);
            assert_eq!(mode_of(&cache.join(acme::KEY_FILE)), 0o600);
        }
        let st: Arc<TlsState> = tls::init(&cfg, dir.path(), CLUSTER).expect("init");
        let shutdown = CancellationToken::new();
        let task = acme::start_with_clock(
            Arc::clone(&st),
            shutdown.clone(),
            Arc::new(ManualClock::new(clock_ms)),
        )
        .expect("start succeeds with an unroutable directory");
        if installed {
            assert_eq!(st.fingerprint().as_deref(), Some(fp.as_str()), "{name}");
        } else {
            assert_eq!(st.fingerprint(), None, "{name}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!task.is_finished(), "{name}: renewal task keeps running");
        if name == "fresh" {
            assert!(
                !acme::account_file(&cache, UNROUTABLE_DIRECTORY).exists(),
                "fresh cache: no account, the directory was not contacted"
            );
        }
        // The served cert stays while a renewal attempt is pending.
        if installed {
            assert_eq!(st.fingerprint().as_deref(), Some(fp.as_str()), "{name}");
        }
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .expect("stops on shutdown")
            .expect("join");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_rejects_invalid_config() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut cfg = acme_cfg("ostro.example", UNROUTABLE_DIRECTORY);
    cfg.api.tls.acme.domains.clear();
    let st = tls::init(&cfg, dir.path(), CLUSTER).expect("init");
    let e = acme::start(Arc::clone(&st), CancellationToken::new()).expect_err("no domains");
    assert!(e.to_string().contains("domains"), "{e}");

    // HTTP-01 binds http01_listen at start: a taken port is a startup error.
    let taken = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let mut cfg = acme_cfg("ostro.example", UNROUTABLE_DIRECTORY);
    cfg.api.tls.acme.challenge = AcmeChallenge::Http01;
    cfg.api.tls.acme.http01_listen = taken.local_addr().expect("addr");
    let st = tls::init(&cfg, dir.path(), CLUSTER).expect("init");
    assert!(acme::start(st, CancellationToken::new()).is_err());
}
