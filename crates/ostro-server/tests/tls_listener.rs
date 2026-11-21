//! Client-listener TLS modes on the one port (plan 07-06 task 2, SEC-01, D-W7/D-W8):
//! REST over HTTPS and gRPC over TLS (ALPN h2) on the same port, `files` hot reload,
//! `auto` resolution per profile, the ACME challenge hook of the resolver.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::routing::get;
use ostro_proto::health::v1::{
    CheckRequest, health_client::HealthClient, health_server::HealthServer,
};
use ostro_server::config::{AcmeChallenge, ApiTlsMode, ServerConfig};
use ostro_server::tls::ca::sha256_fingerprint;
use ostro_server::tls::{
    self, ACME_TLS_ALPN, CertifiedKeyPair, ClusterCa, ReloadableResolver, San, TlsMode, TlsProfile,
    TlsState,
};
use rustls_pki_types::{CertificateDer, ServerName};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const CLUSTER: &str = "0190f5c2-7d3e-7000-8000-0000000000aa";

fn app() -> Router {
    tonic::service::Routes::new(HealthServer::new(ostro_api::HealthSvc))
        .into_axum_router()
        .route("/hello", get(|| async { "hello" }))
}

fn cfg(listen: &str, mode: ApiTlsMode) -> ServerConfig {
    let mut c = ServerConfig::dev_defaults();
    c.api.listen = listen.parse().expect("listen");
    c.api.tls.mode = mode;
    c
}

struct Running {
    addr: SocketAddr,
    shutdown: CancellationToken,
    task: JoinHandle<std::io::Result<()>>,
}

impl Running {
    async fn stop(self) {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(10), self.task)
            .await
            .expect("graceful shutdown within 10 s")
            .expect("join")
            .expect("serve ok");
    }
}

async fn start(state: &Arc<TlsState>, bind: &str) -> Running {
    let listener = TcpListener::bind(bind).await.expect("bind");
    let mut addr = listener.local_addr().expect("addr");
    if addr.ip().is_unspecified() {
        addr.set_ip([127, 0, 0, 1].into());
    }
    let shutdown = CancellationToken::new();
    let task = tokio::spawn(tls::listener::serve(
        Arc::clone(state),
        listener,
        app(),
        shutdown.clone(),
    ));
    Running {
        addr,
        shutdown,
        task,
    }
}

fn https_client(ca_der: &[u8]) -> reqwest::Client {
    reqwest::Client::builder()
        .tls_certs_only([reqwest::Certificate::from_der(ca_der).expect("ca")])
        .build()
        .expect("client")
}

/// TLS handshake to `addr` as `localhost` offering `alpn`; returns (leaf DER, ALPN).
async fn handshake(
    addr: SocketAddr,
    roots: &[CertificateDer<'static>],
    alpn: &[&[u8]],
    sni: &str,
) -> std::io::Result<(CertificateDer<'static>, Option<Vec<u8>>)> {
    let mut store = rustls::RootCertStore::empty();
    for r in roots {
        store.add(r.clone()).expect("root");
    }
    let mut cc = rustls::ClientConfig::builder_with_provider(Arc::new(ostro_api::tls_provider()))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_root_certificates(store)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_signed_serves_rest_and_grpc_on_one_port() {
    let dir = tempfile::tempdir().expect("tmp");
    let st = tls::init(
        &cfg("127.0.0.1:0", ApiTlsMode::SelfSigned),
        dir.path(),
        CLUSTER,
    )
    .expect("init");
    assert_eq!(st.mode(), TlsMode::SelfSigned);
    let run = start(&st, "127.0.0.1:0").await;
    let ep = st.endpoint(run.addr);
    assert_eq!(ep.scheme, "https");
    let fp = ep.fingerprint.clone().expect("fingerprint");
    assert!(fp.starts_with("sha256:"));
    let ca_der = st.ca_der().expect("ca");

    // REST over HTTPS, the CA as the only root (D-W20: the dev cert chains to it).
    let http = https_client(&ca_der);
    let r = http
        .get(format!("https://localhost:{}/hello", run.addr.port()))
        .send()
        .await
        .expect("https GET");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    assert_eq!(r.text().await.expect("body"), "hello");

    // gRPC over TLS (ALPN h2) on the same port.
    let tls_cfg = tonic::transport::ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(
            st.ca_pem().expect("pem"),
        ))
        .domain_name("localhost");
    let channel =
        tonic::transport::Channel::from_shared(format!("https://localhost:{}", run.addr.port()))
            .expect("uri")
            .tls_config(tls_cfg)
            .expect("tls config")
            .connect()
            .await
            .expect("grpc connect");
    let status = HealthClient::new(channel.clone())
        .check(CheckRequest::default())
        .await
        .expect("grpc health over TLS")
        .into_inner()
        .status;
    assert_eq!(status, "SERVING");

    // The served leaf is the dev cert, chain [leaf, CA], ALPN negotiated per offer.
    let roots = [CertificateDer::from(ca_der.clone())];
    let (leaf, alpn) = handshake(run.addr, &roots, &[b"h2", b"http/1.1"], "localhost")
        .await
        .expect("handshake");
    assert_eq!(sha256_fingerprint(&leaf), fp);
    assert_eq!(alpn.as_deref(), Some(&b"h2"[..]));
    let (_, alpn) = handshake(run.addr, &roots, &[b"http/1.1"], "localhost")
        .await
        .expect("handshake h1");
    assert_eq!(alpn.as_deref(), Some(&b"http/1.1"[..]));
    // A client that does not trust the cluster CA is refused.
    let other = tempfile::tempdir().expect("tmp");
    let foreign = ClusterCa::load_or_create(other.path(), CLUSTER).expect("other ca");
    assert!(
        handshake(
            run.addr,
            &[foreign.cert_der().clone()],
            &[b"h2"],
            "localhost"
        )
        .await
        .is_err()
    );

    drop(channel);
    drop(http);
    run.stop().await;
}

#[test]
fn alpn_list_per_mode() {
    let h = |v: Vec<Vec<u8>>| {
        v.into_iter()
            .map(|p| String::from_utf8(p).expect("utf8"))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        h(tls::alpn_protocols(
            TlsMode::SelfSigned,
            AcmeChallenge::TlsAlpn01
        )),
        ["h2", "http/1.1"]
    );
    assert_eq!(
        h(tls::alpn_protocols(
            TlsMode::Files,
            AcmeChallenge::TlsAlpn01
        )),
        ["h2", "http/1.1"]
    );
    assert_eq!(
        h(tls::alpn_protocols(TlsMode::Acme, AcmeChallenge::TlsAlpn01)),
        ["h2", "http/1.1", "acme-tls/1"]
    );
    assert_eq!(
        h(tls::alpn_protocols(TlsMode::Acme, AcmeChallenge::Http01)),
        ["h2", "http/1.1"]
    );
    // The TlsState carries the per-mode list.
    let dir = tempfile::tempdir().expect("tmp");
    let mut c = cfg("127.0.0.1:0", ApiTlsMode::Acme);
    c.api.tls.acme.domains = vec!["ostro.example".into()];
    let st = tls::init(&c, dir.path(), CLUSTER).expect("init acme");
    assert_eq!(st.mode(), TlsMode::Acme);
    assert_eq!(h(st.alpn().to_vec()), ["h2", "http/1.1", "acme-tls/1"]);
    assert_eq!(
        st.fingerprint(),
        None,
        "no certificate before ACME installs one"
    );
}

fn write_pair(dir: &Path, pair: &CertifiedKeyPair) -> (std::path::PathBuf, std::path::PathBuf) {
    let c = dir.join("server.crt");
    let k = dir.join("server.key");
    std::fs::write(&c, pair.chain_pem()).expect("crt");
    std::fs::write(&k, pair.key_pem()).expect("key");
    (c, k)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_mode_hot_reloads_without_restart() {
    let dir = tempfile::tempdir().expect("tmp");
    let pki = tempfile::tempdir().expect("pki");
    let issuer = ClusterCa::load_or_create(&pki.path().join("ca"), CLUSTER).expect("issuer");
    let sans = [San::Dns("localhost".into())];
    let leaf = |cn: &str| {
        issuer
            .issue_leaf(
                cn,
                &sans,
                &[rcgen::ExtendedKeyUsagePurpose::ServerAuth],
                Duration::from_secs(3600),
            )
            .expect("leaf")
    };
    let a = leaf("cert A");
    let b = leaf("cert B");
    let (crt, key) = write_pair(pki.path(), &a);

    let mut c = cfg("127.0.0.1:0", ApiTlsMode::Files);
    c.api.tls.cert_file = Some(crt.clone());
    c.api.tls.key_file = Some(key.clone());
    let st = tls::init(&c, dir.path(), CLUSTER).expect("init files");
    assert_eq!(st.mode(), TlsMode::Files);
    let run = start(&st, "127.0.0.1:0").await;
    let roots = [issuer.cert_der().clone()];

    let (served, _) = handshake(run.addr, &roots, &[b"h2"], "localhost")
        .await
        .expect("handshake A");
    assert_eq!(sha256_fingerprint(&served), a.fingerprint());
    assert_eq!(st.endpoint(run.addr).fingerprint, Some(a.fingerprint()));

    // Overwrite with B and reload (what SIGHUP triggers): the next handshake sees B.
    write_pair(pki.path(), &b);
    st.reload_files().expect("reload B");
    let (served, _) = handshake(run.addr, &roots, &[b"h2"], "localhost")
        .await
        .expect("handshake B");
    assert_eq!(sha256_fingerprint(&served), b.fingerprint());
    // REST still works over the swapped certificate.
    let r = https_client(issuer.cert_der())
        .get(format!("https://localhost:{}/hello", run.addr.port()))
        .send()
        .await
        .expect("https after reload");
    assert_eq!(r.status(), reqwest::StatusCode::OK);

    // A corrupt replacement keeps B.
    std::fs::write(
        &crt,
        b"-----BEGIN CERTIFICATE-----\nnope\n-----END CERTIFICATE-----\n",
    )
    .expect("corrupt");
    let e = st.reload_files().expect_err("corrupt reload fails");
    assert!(!e.to_string().contains("PRIVATE KEY"), "{e}");
    let (served, _) = handshake(run.addr, &roots, &[b"h2"], "localhost")
        .await
        .expect("handshake after failed reload");
    assert_eq!(sha256_fingerprint(&served), b.fingerprint());
    // A key that does not match the cert keeps B too.
    std::fs::write(&crt, a.chain_pem()).expect("crt A");
    std::fs::write(&key, b.key_pem()).expect("key B");
    assert!(st.reload_files().is_err(), "mismatched key rejected");
    assert_eq!(st.fingerprint(), Some(b.fingerprint()));

    // The mtime poll picks up a fixed pair (A) without an explicit reload; an unchanged
    // pair does not reload again.
    tokio::time::sleep(Duration::from_millis(20)).await;
    write_pair(pki.path(), &a);
    assert!(st.poll_files(), "mtime change detected");
    assert!(!st.poll_files(), "no change, no reload");
    let (served, _) = handshake(run.addr, &roots, &[b"h2"], "localhost")
        .await
        .expect("handshake A again");
    assert_eq!(sha256_fingerprint(&served), a.fingerprint());

    run.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn auto_and_off_resolution() {
    let http = reqwest::Client::new();

    // auto on 127.0.0.1 → plaintext (the CLI-01 http:// ready line is unchanged).
    let dir = tempfile::tempdir().expect("tmp");
    let st = tls::init(&cfg("127.0.0.1:0", ApiTlsMode::Auto), dir.path(), CLUSTER)
        .expect("auto loopback");
    assert_eq!(st.mode(), TlsMode::Off);
    assert!(st.ca_hash().is_some(), "the CA exists in every mode");
    let run = start(&st, "127.0.0.1:0").await;
    let ep = st.endpoint(run.addr);
    assert_eq!((ep.scheme, ep.fingerprint.clone()), ("http", None));
    let r = http
        .get(format!("{}/hello", ep.url()))
        .send()
        .await
        .expect("plaintext GET");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    run.stop().await;

    // auto on a non-loopback bind in dev → self-signed TLS.
    let dir = tempfile::tempdir().expect("tmp");
    let st = tls::init(&cfg("0.0.0.0:0", ApiTlsMode::Auto), dir.path(), CLUSTER)
        .expect("auto dev non-loopback");
    assert_eq!(st.mode(), TlsMode::SelfSigned);
    let run = start(&st, "0.0.0.0:0").await;
    assert_eq!(st.endpoint(run.addr).scheme, "https");
    let r = https_client(&st.ca_der().expect("ca"))
        .get(format!("https://localhost:{}/hello", run.addr.port()))
        .send()
        .await
        .expect("https GET");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    run.stop().await;

    // auto in the server profile on a non-loopback listener → startup error ...
    let mut server = ServerConfig::server_defaults();
    server.api.listen = "0.0.0.0:0".parse().expect("listen");
    assert_eq!(TlsProfile::of(&server), TlsProfile::Server);
    let dir = tempfile::tempdir().expect("tmp");
    let e = tls::init(&server, dir.path(), CLUSTER).expect_err("server auto refused");
    assert_eq!(
        e.to_string(),
        format!(
            "invalid TLS configuration: {}",
            tls::AUTO_NON_LOOPBACK_ERROR
        )
    );
    // ... unless allow_self_signed.
    server.api.tls.allow_self_signed = true;
    let st = tls::init(&server, dir.path(), CLUSTER).expect("allow_self_signed");
    assert_eq!(st.mode(), TlsMode::SelfSigned);
    // The server profile on loopback stays plaintext.
    let mut lo = ServerConfig::server_defaults();
    lo.api.listen = "127.0.0.1:0".parse().expect("listen");
    let st = tls::init_with_profile(&lo, dir.path(), CLUSTER, TlsProfile::Server)
        .expect("server loopback");
    assert_eq!(st.mode(), TlsMode::Off);

    // off → plaintext even on a non-loopback bind.
    let dir = tempfile::tempdir().expect("tmp");
    let st = tls::init(&cfg("0.0.0.0:0", ApiTlsMode::Off), dir.path(), CLUSTER).expect("off");
    assert_eq!(st.mode(), TlsMode::Off);
    let run = start(&st, "0.0.0.0:0").await;
    let r = http
        .get(format!("http://{}/hello", run.addr))
        .send()
        .await
        .expect("plaintext GET off");
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    run.stop().await;

    // files without paths is a startup error.
    let dir = tempfile::tempdir().expect("tmp");
    assert!(tls::init(&cfg("127.0.0.1:0", ApiTlsMode::Files), dir.path(), CLUSTER).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acme_mode_delegates_to_acme_start() {
    let dir = tempfile::tempdir().expect("tmp");
    // No domains: acme::start (07-10) rejects the config before any directory contact.
    let c = cfg("127.0.0.1:0", ApiTlsMode::Acme);
    let st = tls::init(&c, dir.path(), CLUSTER).expect("init");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let e = tokio::time::timeout(
        Duration::from_secs(5),
        tls::listener::serve(st, listener, app(), CancellationToken::new()),
    )
    .await
    .expect("returns")
    .expect_err("acme::start rejects the config");
    assert!(e.to_string().contains("domains"), "{e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resolver_answers_acme_tls_alpn_with_the_challenge_cert() {
    let pki = tempfile::tempdir().expect("pki");
    let ca = ClusterCa::load_or_create(pki.path(), CLUSTER).expect("ca");
    let mk = |host: &str| {
        ca.issue_leaf(
            host,
            &[San::Dns(host.into())],
            &[rcgen::ExtendedKeyUsagePurpose::ServerAuth],
            Duration::from_secs(3600),
        )
        .expect("leaf")
    };
    let real = mk("ostro.example");
    let challenge = mk("ostro.example");
    let resolver = Arc::new(ReloadableResolver::new(Some(
        real.to_rustls().expect("real"),
    )));
    let mut sc = rustls::ServerConfig::builder_with_provider(Arc::new(ostro_api::tls_provider()))
        .with_safe_default_protocol_versions()
        .expect("versions")
        .with_no_client_auth()
        .with_cert_resolver(Arc::clone(&resolver) as Arc<_>);
    sc.alpn_protocols = tls::alpn_protocols(TlsMode::Acme, AcmeChallenge::TlsAlpn01);
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(sc));
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let server = tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                let _ = acceptor.accept(tcp).await;
            });
        }
    });
    let roots = [ca.cert_der().clone()];

    // No challenge registered: an acme-tls/1-only hello gets nothing (never the real cert).
    assert!(
        handshake(addr, &roots, &[ACME_TLS_ALPN], "ostro.example")
            .await
            .is_err()
    );
    let challenge_key = (*challenge.to_rustls().expect("challenge")).clone();
    resolver.set_challenge("Ostro.Example.", Some(challenge_key));
    let (leaf, alpn) = handshake(addr, &roots, &[ACME_TLS_ALPN], "ostro.example")
        .await
        .expect("validator handshake");
    assert_eq!(sha256_fingerprint(&leaf), challenge.fingerprint());
    assert_eq!(alpn.as_deref(), Some(ACME_TLS_ALPN));
    // Normal clients still get the real cert.
    let (leaf, _) = handshake(addr, &roots, &[b"h2", b"http/1.1"], "ostro.example")
        .await
        .expect("normal handshake");
    assert_eq!(sha256_fingerprint(&leaf), real.fingerprint());
    // swap() replaces the real cert; clearing the challenge stops answering validators.
    let next = mk("ostro.example");
    resolver.swap(next.to_rustls().expect("next"));
    let (leaf, _) = handshake(addr, &roots, &[b"h2"], "ostro.example")
        .await
        .expect("after swap");
    assert_eq!(sha256_fingerprint(&leaf), next.fingerprint());
    resolver.set_challenge("ostro.example", None);
    assert!(
        handshake(addr, &roots, &[ACME_TLS_ALPN], "ostro.example")
            .await
            .is_err()
    );
    server.abort();
}
