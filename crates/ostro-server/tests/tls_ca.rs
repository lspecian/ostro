//! Cluster CA, node / worker / dev certificate issuance (plan 07-06 task 1, SEC-01,
//! WORK-01, D-W4/D-W5/D-W20).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_server::tls::ca::sha256_fingerprint;
use ostro_server::tls::{ClusterCa, NodeIdentity, TlsError, WorkerIdentity, dev_cert};
use rcgen::{
    CertificateParams, KeyPair, PKCS_ECDSA_P256_SHA256, PKCS_ECDSA_P384_SHA384, PKCS_ED25519,
};
use rustls::client::danger::ServerCertVerifier;
use rustls::server::danger::ClientCertVerifier;
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};

const CLUSTER: &str = "0190f5c2-7d3e-7000-8000-000000000001";
const OTHER_CLUSTER: &str = "0190f5c2-7d3e-7000-8000-000000000002";

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(ostro_api::tls_provider())
}

fn csr(alg: &'static rcgen::SignatureAlgorithm) -> Vec<u8> {
    let key = KeyPair::generate_for(alg).expect("key");
    // The CSR asks for a subject and SANs the CA must ignore.
    let mut p = CertificateParams::new(vec!["evil.example".to_owned()]).expect("params");
    p.distinguished_name
        .push(rcgen::DnType::CommonName, "root of everything");
    p.serialize_request(&key).expect("csr").der().to_vec()
}

fn client_verifier(ca: &ClusterCa) -> Arc<dyn ClientCertVerifier> {
    rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(ca.root_store().expect("roots")),
        provider(),
    )
    .build()
    .expect("verifier")
}

fn verify_client(ca: &ClusterCa, chain: &[CertificateDer<'static>]) -> Result<(), rustls::Error> {
    client_verifier(ca)
        .verify_client_cert(&chain[0], &chain[1..], UnixTime::now())
        .map(|_| ())
}

#[cfg(unix)]
fn mode_of(p: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).expect("meta").permissions().mode() & 0o777
}

#[test]
fn ca_create_reload_hash_and_modes() {
    let dir = tempfile::tempdir().expect("tmp");
    let tls = dir.path().join("tls");
    let t0 = Instant::now();
    let ca = ClusterCa::load_or_create(&tls, CLUSTER).expect("create");
    let node = ca.load_or_issue_node_cert(1, &[]).expect("node");
    let took = t0.elapsed();
    println!("startup cost: CA + node cert on an empty dir = {took:?}");
    // D-W16: must stay well inside the CLI-01 < 1 s budget (debug build included).
    assert!(
        took < Duration::from_millis(500),
        "CA + node cert took {took:?}"
    );

    let hash = ca.ca_hash();
    assert!(
        hash.starts_with("sha256:") && hash.len() == 7 + 64,
        "{hash}"
    );
    assert_eq!(hash, sha256_fingerprint(ca.cert_der()));
    for f in ["ca.crt", "ca.key", "node.crt", "node.key"] {
        assert!(tls.join(f).is_file(), "{f} written");
    }
    #[cfg(unix)]
    {
        assert_eq!(mode_of(&tls.join("ca.key")), 0o600);
        assert_eq!(mode_of(&tls.join("node.key")), 0o600);
        assert_eq!(mode_of(&tls), 0o700);
    }
    // Debug never shows keys.
    let dbg = format!("{ca:?} {node:?}");
    assert!(
        !dbg.contains("PRIVATE KEY") && dbg.contains("[redacted]"),
        "{dbg}"
    );

    // Reload: same CA, same node cert (reused, not re-issued).
    let ca2 = ClusterCa::load_or_create(&tls, CLUSTER).expect("reload");
    assert_eq!(ca2.ca_hash(), hash);
    let node2 = ca2.load_or_issue_node_cert(1, &[]).expect("node reload");
    assert_eq!(node2.fingerprint(), node.fingerprint(), "node cert reused");
    assert_eq!(node2.chain().len(), 2, "chain = [leaf, CA]");
    assert_eq!(&node2.chain()[1], ca.cert_der());

    // Changed SANs (advertise) or node id → re-issued.
    let node3 = ca2
        .load_or_issue_node_cert(1, &["ostro.example:7701".to_owned()])
        .expect("node advertise");
    assert_ne!(node3.fingerprint(), node.fingerprint());
    let node4 = ca2
        .load_or_issue_node_cert(2, &["ostro.example:7701".to_owned()])
        .expect("node id 2");
    assert_ne!(node4.fingerprint(), node3.fingerprint());
    assert_eq!(
        NodeIdentity::from_der(node4.leaf_der())
            .expect("id")
            .node_id,
        2
    );
}

#[test]
fn corrupt_or_mismatched_ca_is_an_error_naming_the_file() {
    let dir = tempfile::tempdir().expect("tmp");
    let tls = dir.path().join("tls");
    ClusterCa::load_or_create(&tls, CLUSTER).expect("create");
    let good_key = std::fs::read(tls.join("ca.key")).expect("key");

    std::fs::write(tls.join("ca.key"), b"garbage").expect("corrupt");
    let e = ClusterCa::load_or_create(&tls, CLUSTER).expect_err("corrupt key");
    let msg = e.to_string();
    assert!(msg.contains("ca.key"), "{msg}");
    assert!(!msg.contains("garbage"), "{msg}");
    // Nothing was regenerated.
    assert_eq!(std::fs::read(tls.join("ca.key")).expect("key"), b"garbage");

    // A key of another CA → mismatch.
    let other = tempfile::tempdir().expect("tmp");
    let other_ca = ClusterCa::load_or_create(other.path(), CLUSTER).expect("other");
    std::fs::write(tls.join("ca.key"), other_ca.key_pem()).expect("swap key");
    let msg = ClusterCa::load_or_create(&tls, CLUSTER)
        .expect_err("mismatch")
        .to_string();
    assert!(msg.contains("do not match"), "{msg}");
    assert!(!msg.contains("PRIVATE KEY"), "{msg}");

    // Half-present pair.
    std::fs::write(tls.join("ca.key"), &good_key).expect("restore");
    std::fs::remove_file(tls.join("ca.crt")).expect("rm crt");
    let msg = ClusterCa::load_or_create(&tls, CLUSTER)
        .expect_err("half")
        .to_string();
    assert!(msg.contains("ca.crt") && msg.contains("missing"), "{msg}");
}

#[test]
fn ca_is_cluster_scoped_and_importable() {
    let a = tempfile::tempdir().expect("tmp");
    let ca_a = ClusterCa::load_or_create(a.path(), CLUSTER).expect("create a");
    assert_eq!(ca_a.cluster_id(), CLUSTER);

    // A CA of another cluster is refused.
    let msg = ClusterCa::load_or_create(a.path(), OTHER_CLUSTER)
        .expect_err("other cluster")
        .to_string();
    assert!(msg.contains("another cluster"), "{msg}");
    let msg = ClusterCa::import_pem(
        tempfile::tempdir().expect("tmp").path(),
        OTHER_CLUSTER,
        &ca_a.cert_pem(),
        &ca_a.key_pem(),
    )
    .expect_err("import into another cluster")
    .to_string();
    assert!(msg.contains("another cluster"), "{msg}");

    // A second node (own data dir, its own CA first) imports node A's CA.
    let b = tempfile::tempdir().expect("tmp");
    let own = ClusterCa::load_or_create(b.path(), CLUSTER).expect("b own CA");
    let stale = own
        .load_or_issue_node_cert(2, &[])
        .expect("stale node cert");
    let ca_b = ClusterCa::import_pem(b.path(), CLUSTER, &ca_a.cert_pem(), &ca_a.key_pem())
        .expect("import");
    assert_eq!(ca_b.ca_hash(), ca_a.ca_hash());
    let reloaded = ClusterCa::load_or_create(b.path(), CLUSTER).expect("reload imported");
    assert_eq!(reloaded.ca_hash(), ca_a.ca_hash());
    // The node cert of the old CA is re-issued by the imported one.
    let node_b = reloaded.load_or_issue_node_cert(2, &[]).expect("node b");
    assert_ne!(node_b.fingerprint(), stale.fingerprint());
    // A verifier built from node A's CA accepts node B's certificate.
    verify_client(&ca_a, node_b.chain()).expect("A trusts B's node cert");
    let id = NodeIdentity::from_der(node_b.leaf_der()).expect("node id");
    assert_eq!((id.cluster_id.as_str(), id.node_id), (CLUSTER, 2));
}

#[test]
fn node_identity_round_trip_and_disjoint_schemes() {
    let dir = tempfile::tempdir().expect("tmp");
    let ca = ClusterCa::load_or_create(dir.path(), CLUSTER).expect("ca");
    let node = ca
        .issue_node_cert(7, &["10.0.0.5".to_owned(), "node7.example".to_owned()])
        .expect("node 7");
    let id = NodeIdentity::from_der(node.leaf_der()).expect("node id");
    assert_eq!(id.cluster_id, CLUSTER);
    assert_eq!(id.node_id, 7);
    assert!(id.not_after > 0);
    assert!(
        WorkerIdentity::from_der(node.leaf_der()).is_err(),
        "a node cert is not a worker identity"
    );
    // Node certs are server + client certs.
    verify_client(&ca, node.chain()).expect("node cert as client cert");
    let server = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(ca.root_store().expect("roots")),
        provider(),
    )
    .build()
    .expect("server verifier");
    for name in ["localhost", "node7.example", "10.0.0.5", "127.0.0.1", "::1"] {
        server
            .verify_server_cert(
                &node.chain()[0],
                &node.chain()[1..],
                &ServerName::try_from(name).expect("name"),
                &[],
                UnixTime::now(),
            )
            .unwrap_or_else(|e| panic!("node cert valid for {name}: {e}"));
    }

    let wcert = ca
        .sign_worker_csr(
            &csr(&PKCS_ECDSA_P256_SHA256),
            "wkr_abc",
            "w-1",
            Duration::from_secs(3600),
        )
        .expect("worker");
    assert!(
        NodeIdentity::from_der(&wcert).is_err(),
        "a worker cert is not a node identity"
    );
}

#[test]
fn worker_csr_signing_round_trip_and_verification() {
    let dir = tempfile::tempdir().expect("tmp");
    let ca = ClusterCa::load_or_create(dir.path(), CLUSTER).expect("ca");
    for alg in [&PKCS_ECDSA_P256_SHA256, &PKCS_ED25519] {
        let der = ca
            .sign_worker_csr(
                &csr(alg),
                "wkr_tok1",
                "worker-a.1",
                Duration::from_secs(24 * 3600),
            )
            .expect("sign");
        let id = WorkerIdentity::from_der(&der).expect("identity");
        assert_eq!(id.cluster_id, CLUSTER);
        assert_eq!(id.token_id, "wkr_tok1");
        assert_eq!(id.worker_id, "worker-a.1");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("now")
            .as_secs() as i64;
        assert!(
            (id.not_after - (now + 24 * 3600)).abs() < 120,
            "ttl honoured: {} vs {}",
            id.not_after,
            now + 24 * 3600
        );
        // The CSR's SANs / subject are ignored; EKU clientAuth only; not a CA.
        let (_, cert) = x509_parser::parse_x509_certificate(&der).expect("parse");
        assert!(!cert.is_ca());
        let sans = cert
            .subject_alternative_name()
            .expect("san")
            .expect("san present");
        assert_eq!(sans.value.general_names.len(), 1);
        let eku = cert
            .extended_key_usage()
            .expect("eku")
            .expect("eku present");
        assert!(eku.value.client_auth && !eku.value.server_auth);
        assert!(!format!("{:?}", cert.subject()).contains("root of everything"));

        // rustls accepts it against this CA ...
        let chain = vec![CertificateDer::from(der.clone())];
        verify_client(&ca, &chain).expect("verifier accepts worker cert");
    }
    // ... and rejects one issued by a different CA.
    let other = tempfile::tempdir().expect("tmp");
    let other_ca = ClusterCa::load_or_create(other.path(), CLUSTER).expect("other");
    let foreign = other_ca
        .sign_worker_csr(
            &csr(&PKCS_ECDSA_P256_SHA256),
            "wkr_tok1",
            "w",
            Duration::from_secs(60),
        )
        .expect("foreign");
    assert!(verify_client(&ca, &[CertificateDer::from(foreign)]).is_err());
}

#[test]
fn bad_csrs_and_ids_are_rejected() {
    let dir = tempfile::tempdir().expect("tmp");
    let ca = ClusterCa::load_or_create(dir.path(), CLUSTER).expect("ca");
    let ttl = Duration::from_secs(60);

    // Forged: flip a bit in the signature (the last byte of the DER).
    let mut forged = csr(&PKCS_ECDSA_P256_SHA256);
    let last = forged.len() - 1;
    forged[last] ^= 0x01;
    let e = ca
        .sign_worker_csr(&forged, "wkr_a", "w", ttl)
        .expect_err("forged");
    assert!(matches!(e, TlsError::Cert(_)), "{e}");

    // Garbage.
    assert!(ca.sign_worker_csr(b"not a csr", "wkr_a", "w", ttl).is_err());

    // Non-P256 / Ed25519 key.
    let msg = ca
        .sign_worker_csr(&csr(&PKCS_ECDSA_P384_SHA384), "wkr_a", "w", ttl)
        .expect_err("p384")
        .to_string();
    assert!(msg.contains("P-256 or Ed25519"), "{msg}");

    // Bad ids.
    let good = csr(&PKCS_ECDSA_P256_SHA256);
    let long = "a".repeat(65);
    for (tok, wid) in [
        ("wkr_a", "w/1"),
        ("wkr_a", ""),
        ("wkr_a", long.as_str()),
        ("wkr a", "w"),
        ("wkr_a", "w:1"),
        ("", "w"),
    ] {
        assert!(
            ca.sign_worker_csr(&good, tok, wid, ttl).is_err(),
            "({tok:?}, {wid:?}) must be rejected"
        );
    }
    assert!(
        ca.sign_worker_csr(&good, "wkr_a", &"a".repeat(64), ttl)
            .is_ok()
    );
}

#[test]
fn dev_cert_is_issued_by_the_cluster_ca() {
    let dir = tempfile::tempdir().expect("tmp");
    let ca = ClusterCa::load_or_create(dir.path(), CLUSTER).expect("ca");
    let listen: SocketAddr = "192.0.2.10:7700".parse().expect("addr");
    let sans = dev_cert::sans(listen, &[]);
    let dev = dev_cert::load_or_issue(&ca, &sans).expect("dev");
    assert_eq!(dev.chain().len(), 2);
    assert_eq!(
        &dev.chain()[1],
        ca.cert_der(),
        "chain includes the CA (D-W20)"
    );
    assert_eq!(dev.fingerprint(), sha256_fingerprint(dev.leaf_der()));
    assert!(
        NodeIdentity::from_der(dev.leaf_der()).is_err(),
        "the dev cert carries no node identity"
    );
    let server = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(ca.root_store().expect("roots")),
        provider(),
    )
    .build()
    .expect("server verifier");
    for name in ["localhost", "127.0.0.1", "::1", "192.0.2.10"] {
        server
            .verify_server_cert(
                &dev.chain()[0],
                &dev.chain()[1..],
                &ServerName::try_from(name).expect("name"),
                &[],
                UnixTime::now(),
            )
            .unwrap_or_else(|e| panic!("dev cert valid for {name}: {e}"));
    }
    if let Some(h) = dev_cert::hostname() {
        server
            .verify_server_cert(
                &dev.chain()[0],
                &dev.chain()[1..],
                &ServerName::try_from(h.clone()).expect("hostname"),
                &[],
                UnixTime::now(),
            )
            .unwrap_or_else(|e| panic!("dev cert valid for hostname {h}: {e}"));
    }
    // Reused across restarts (stable fingerprint) while the SANs are unchanged.
    let again = dev_cert::load_or_issue(&ca, &sans).expect("dev again");
    assert_eq!(again.fingerprint(), dev.fingerprint());
    #[cfg(unix)]
    assert_eq!(mode_of(&dir.path().join("dev.key")), 0o600);
    // The dev cert key round-trips through rustls.
    dev.to_rustls().expect("rustls key");
}

#[test]
fn tls_init_creates_ca_and_node_cert() {
    let dir = tempfile::tempdir().expect("tmp");
    let mut cfg = ostro_server::config::ServerConfig::dev_defaults();
    cfg.node.node_id = 3;
    let st = ostro_server::tls::init(&cfg, dir.path(), CLUSTER).expect("init");
    let hash = st.ca_hash().expect("hash");
    let id = NodeIdentity::from_der(st.node_cert().expect("node").leaf_der()).expect("id");
    assert_eq!((id.cluster_id.as_str(), id.node_id), (CLUSTER, 3));
    st.node_server_config().expect("node server config");
    let w = st
        .sign_worker_csr(
            &csr(&PKCS_ECDSA_P256_SHA256),
            "wkr_x",
            "w",
            Duration::from_secs(60),
        )
        .expect("sign");
    assert_eq!(WorkerIdentity::from_der(&w).expect("id").token_id, "wkr_x");
    // Restart: same CA.
    let st2 = ostro_server::tls::init(&cfg, dir.path(), CLUSTER).expect("init 2");
    assert_eq!(st2.ca_hash().expect("hash"), hash);
}
