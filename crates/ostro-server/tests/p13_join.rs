//! 13-09 Task 1: the node join handshake (D-P13-9 / D-P13-10) on a single seed node over
//! real loopback mTLS: node join tokens, id allocation (max incl. tombstones + 1) and resume,
//! CSR signing with the CA from `__ostro/cluster-ca`, every refusal path, and a joiner that
//! never runs `InitCluster`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::time::Duration;

use ostro_api::phase13::{Advertise, JoinRequest, NodeVersionDto};
use ostro_cluster::membership::service::set_member_state;
use ostro_meta::ext::{MEMBER_REMOVED, get_member_state};
use ostro_meta::{ApiToken, MetaCommand, MetaKey, MetaRecord};
use ostro_server::ServerHandle;
use ostro_server::p13::BootMode;
use p12::{START_T, start, tmp};
use p13::*;
use serde_json::{Value, json};

fn csr() -> String {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    // The CSR asks for a foreign identity: the server must ignore it.
    p.subject_alt_names = vec![rcgen::SanType::URI(
        "ostro://evil/node/1".try_into().unwrap(),
    )];
    p.serialize_request(&key).unwrap().pem().unwrap()
}

fn version(lo: u32, hi: u32) -> NodeVersionDto {
    NodeVersionDto {
        ostro_version: "test".into(),
        level_min: lo,
        level_max: hi,
        wasmtime: "w".into(),
        event_codec_ver: 1,
        wit_lines: vec!["0.1".into()],
    }
}

fn request(token: &str, resume: Option<u64>) -> JoinRequest {
    JoinRequest {
        token: token.to_owned(),
        csr_pem: csr(),
        advertise: Advertise {
            api: "127.0.0.1:1".into(),
            cluster: "127.0.0.1:2".into(),
            repl: None,
        },
        node_version: version(
            ostro_types::compat::LEVEL_MIN,
            ostro_types::compat::level_max(),
        ),
        resume_node_id: resume,
    }
}

async fn raw_join(h: &ServerHandle, r: &JoinRequest) -> (u16, Value) {
    post(h, "/v1/cluster/join", serde_json::to_value(r).unwrap()).await
}

fn reason(v: &Value) -> Option<String> {
    let e = v.get("error").unwrap_or(v);
    e.pointer("/details/reason")
        .and_then(|r| r.as_str())
        .map(str::to_owned)
}

fn member_ids(h: &ServerHandle) -> Vec<u64> {
    h.meta()
        .view()
        .read(|s| s.members().map(|m| m.value.node_id).collect())
}

fn state_of(h: &ServerHandle, id: u64) -> Option<u8> {
    h.meta().view().read(|s| {
        s.member(id)
            .and_then(|m| get_member_state(&m.value.ext).ok().flatten())
            .map(|st| st.state)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joiner_never_inits_cluster() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let j = joiner(root.path(), 2);
    let prepared = join(&seed, &j).await.expect("join");
    assert_eq!(prepared.node_id, 2, "max(1) + 1");
    assert_eq!(prepared.cluster_id, CLUSTER);
    // Marker first, joined TLS and node.toml written.
    assert!(j.data_dir.join("cluster-seeds.toml").is_file());
    assert!(ostro_server::p13::tls_complete(&j.data_dir));
    let toml = p12::node_toml(&j.data_dir);
    assert!(
        toml.contains("node_id = 2") && toml.contains(CLUSTER),
        "{toml}"
    );
    // The CA is in meta now (ensure_cluster_ca), and the seed knows the joiner as Joining.
    assert_eq!(state_of(&seed, 2), Some(ostro_meta::ext::MEMBER_JOINING));

    // Boot the prepared dir: a Joiner that gets a working certificate (it talks mTLS to the
    // seed) and becomes a learner, then Active.
    let joined = tokio::time::timeout(START_T, ostro_server::Server::start(joined_cfg(&j, 2)))
        .await
        .expect("joined start timed out")
        .expect("joined node starts");
    assert_eq!(
        joined.p13().map(|c| c.cluster.boot_mode()),
        Some(BootMode::Joiner)
    );
    let st = status_until(&seed, Duration::from_secs(60), |st| {
        member(st, 2).is_some_and(|m| m["state"] == "active" && m["raft_role"] == "learner")
    })
    .await;
    assert_eq!(
        st["voters"],
        json!([1]),
        "2 members: one voter (odd), one learner"
    );
    assert_eq!(st["cluster_id"], CLUSTER);
    // The joiner's meta has the seed's cluster, and nobody proposed a second InitCluster.
    let cid = joined
        .meta()
        .view()
        .read(|s| s.cluster().map(|c| c.value.cluster_id.clone()));
    assert_eq!(cid.as_deref(), Some(CLUSTER));
    // The joined node serves the status too (its own view, behind the read barrier).
    let st2 = status_until(&joined, Duration::from_secs(30), |st| {
        member(st, 2).is_some()
    })
    .await;
    assert_eq!(st2["meta_leader"], 1);
    let seed_dir = root.path().join("node1");
    joined.shutdown().await.expect("joined shutdown");
    seed.shutdown().await.expect("seed shutdown");
    for (_, node, cid) in init_clusters(&seed_dir)
        .into_iter()
        .chain(init_clusters(&j.data_dir))
    {
        assert_eq!(node, 1, "only the bootstrapping seed proposes InitCluster");
        assert_eq!(cid, CLUSTER);
    }
    assert!(
        init_clusters(&j.data_dir).iter().all(|(_, n, _)| *n != 2),
        "the joiner never proposed InitCluster"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_refusal_path() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let t = token(&seed, None).await;
    assert!(t.id.starts_with("njt_") && t.token.starts_with(&format!("{}.", t.id)));
    assert!(t.ca_hash.starts_with("sha256:"));
    let before = member_ids(&seed);

    // Unknown token, wrong secret, malformed: one indistinguishable refusal.
    for bad in [
        "njt_zzzzzzzzzz.secret".to_owned(),
        format!("{}.wrong", t.id),
        "garbage".to_owned(),
        t.token.replace("njt_", "wkr_"),
    ] {
        let (st, v) = raw_join(&seed, &request(&bad, None)).await;
        assert_eq!(st, 400, "{bad}: {v}");
        assert_eq!(reason(&v).as_deref(), Some("join_token_invalid"), "{v}");
    }
    // Wrong scope: an njt_ record with the worker scope.
    let secret = "s3cr3t";
    let hash: [u8; 32] = <sha2::Sha256 as sha2::Digest>::digest(secret.as_bytes()).into();
    let res = seed
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Token("njt_scopeless".into()),
            expected_version: None,
            record: MetaRecord::Token(ApiToken {
                id: "njt_scopeless".into(),
                hash,
                scopes: vec!["worker:join".into()],
                tenant: None,
                created_at_ms: 0,
                expires_at_ms: None,
                ext: Vec::new(),
            }),
        })
        .await
        .unwrap();
    assert!(matches!(res, ostro_meta::MetaResult::Ok { .. }), "{res:?}");
    let (st, v) = raw_join(&seed, &request(&format!("njt_scopeless.{secret}"), None)).await;
    assert_eq!(
        (st, reason(&v).as_deref()),
        (400, Some("join_token_invalid"))
    );
    // Expired token (1 s lifetime).
    let short = token(&seed, Some(1)).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let (st, v) = raw_join(&seed, &request(&short.token, None)).await;
    assert_eq!(
        (st, reason(&v).as_deref()),
        (400, Some("join_token_invalid")),
        "{v}"
    );
    // Revoked (deleted) token.
    let revoked = token(&seed, None).await;
    let tok_version = seed
        .meta()
        .view()
        .read(|s| s.token(&revoked.id).map(|t| t.version))
        .unwrap();
    seed.meta()
        .propose(MetaCommand::Delete {
            key: MetaKey::Token(revoked.id.clone()),
            expected_version: tok_version,
        })
        .await
        .unwrap();
    let (st, v) = raw_join(&seed, &request(&revoked.token, None)).await;
    assert_eq!(
        (st, reason(&v).as_deref()),
        (400, Some("join_token_invalid"))
    );
    // Level range without the finalized level.
    let mut r = request(&t.token, None);
    r.node_version = version(90, 91);
    let (st, v) = raw_join(&seed, &r).await;
    assert_eq!(
        (st, reason(&v).as_deref()),
        (412, Some("level_mismatch")),
        "{v}"
    );
    // A CSR that is not a CSR.
    let mut r = request(&t.token, None);
    r.csr_pem = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n".into();
    let (st, v) = raw_join(&seed, &r).await;
    assert_eq!(st, 400, "{v}");
    // Resume of an unknown id and of an Active id (node 1).
    let (st, v) = raw_join(&seed, &request(&t.token, Some(77))).await;
    assert_eq!(st, 404, "{v}");
    let (st, v) = raw_join(&seed, &request(&t.token, Some(1))).await;
    assert_eq!(
        (st, reason(&v).as_deref()),
        (412, Some("member_active")),
        "{v}"
    );
    // Nothing was allocated by any refusal.
    assert_eq!(member_ids(&seed), before);

    // A wrong CA pin is refused by the joiner itself (no marker written).
    let j = joiner(root.path(), 9);
    let mut args = join_args(&seed, &j, &t);
    args.ca_hash = format!("sha256:{}", "00".repeat(32));
    let e = ostro_server::p13::joiner::run_join(args).await.unwrap_err();
    assert!(e.contains("--ca-hash"), "{e}");
    assert!(!j.data_dir.join("cluster-seeds.toml").exists());
    seed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ids_after_tombstones_and_resume() {
    let root = tmp();
    let seed = start(seed_cfg(root.path())).await.expect("seed");
    let t = token(&seed, None).await;
    // Allocation: 2, then the same request resumed for 2 allocates nothing.
    let (st, v) = raw_join(&seed, &request(&t.token, None)).await;
    assert!((200..300).contains(&st), "{st} {v}");
    assert_eq!(v["node_id"], 2);
    assert_eq!(v["cluster_id"], CLUSTER);
    assert_eq!(v["seeds"][0]["id"], 1);
    let ca_pem = v["ca_cert_pem"].as_str().unwrap().to_owned();
    let cert_pem = v["node_cert_pem"].as_str().unwrap().to_owned();
    // The certificate names ostro://<cluster>/node/2 (never the CSR's identity) and chains
    // to the pinned CA.
    let leaf = rustls_pki_types::pem::PemObject::pem_slice_iter(cert_pem.as_bytes())
        .next()
        .unwrap()
        .map(|d: rustls_pki_types::CertificateDer<'static>| d)
        .unwrap();
    let id = ostro_server::tls::ca::NodeIdentity::from_der(&leaf).expect("node identity");
    assert_eq!((id.cluster_id.as_str(), id.node_id), (CLUSTER, 2));
    let ca_der = rustls_pki_types::pem::PemObject::from_pem_slice(ca_pem.as_bytes())
        .map(|d: rustls_pki_types::CertificateDer<'static>| d)
        .unwrap();
    assert_eq!(
        ostro_server::tls::ca::sha256_fingerprint(&ca_der),
        t.ca_hash
    );
    let (st, v) = raw_join(&seed, &request(&t.token, Some(2))).await;
    assert!((200..300).contains(&st), "{st} {v}");
    assert_eq!(v["node_id"], 2, "resume re-signs for the same id");
    assert_eq!(member_ids(&seed), vec![1, 2]);
    // Tombstone 2: a resume is refused with member_removed, a new join gets 3 (never 2).
    set_member_state(seed.meta(), 2, MEMBER_REMOVED, 1)
        .await
        .unwrap();
    let (st, v) = raw_join(&seed, &request(&t.token, Some(2))).await;
    assert_eq!(
        (st, reason(&v).as_deref()),
        (412, Some("member_removed")),
        "{v}"
    );
    let (st, v) = raw_join(&seed, &request(&t.token, None)).await;
    assert!((200..300).contains(&st), "{st} {v}");
    assert_eq!(v["node_id"], 3, "max(all ids incl. tombstones) + 1");
    assert_eq!(state_of(&seed, 2), Some(MEMBER_REMOVED));
    seed.shutdown().await.unwrap();
}
