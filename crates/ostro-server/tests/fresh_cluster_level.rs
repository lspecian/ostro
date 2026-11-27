//! p14-fixups (user decision 2026-11-27): a brand-new cluster starts finalized at the newest
//! feature level of the binary that bootstraps it, not at `LEVEL_MIN`. Existing clusters keep
//! the Phase 13 upgrade rules (auto-finalize / hold / fences); the joiner fences still refuse
//! an older binary.
//!
//! Every node here runs with `[cluster] auto_finalize = false`, so the finalizer cannot be
//! what raised the level: only the fresh-cluster stamp at boot can.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::time::Duration;

use ostro_api::phase13::{Advertise, JoinRequest, NodeVersionDto};
use ostro_cluster::compat::{finalized_level, read_cluster_version};
use ostro_meta::ext::{EXT_CLUSTER_VERSION, ext_remove, get_api_token_meta};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p13::BootMode;
use ostro_server::p14::bootstrap::{ADMIN_TOKEN_FILE, BOOTSTRAP_NAME};
use ostro_server::{ServerHandle, ServerOptions};
use ostro_types::compat::{LEVEL, LEVEL_MIN};
use p12::{START_T, start, tmp};
use p13::*;
use serde_json::{Value, json};

fn no_auto(mut cfg: ServerConfig) -> ServerConfig {
    cfg.cluster.auto_finalize = false;
    cfg
}

async fn start_opts(cfg: ServerConfig, opts: ServerOptions) -> Result<ServerHandle, String> {
    tokio::time::timeout(START_T, ostro_server::server::start_with_options(cfg, opts))
        .await
        .expect("start timed out")
        .map_err(|e| e.to_string())
}

/// `(tag 98 present, finalized level)` in `h`'s metadata view.
fn level(h: &ServerHandle) -> (bool, u32) {
    h.meta().view().read(|s| {
        (
            read_cluster_version(s).expect("decodable tag 98").is_some(),
            finalized_level(s),
        )
    })
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

fn csr() -> String {
    let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    p.serialize_request(&key).unwrap().pem().unwrap()
}

/// Remove tag 98 from the cluster record: the metadata of a cluster created before the
/// fresh-cluster stamp existed (finalized = `LEVEL_MIN` by absence).
async fn strip_cluster_version(meta: &MetaHandle) {
    let cur = meta
        .view()
        .read(|s| s.cluster().cloned())
        .expect("cluster info");
    let mut info = cur.value;
    info.ext = ext_remove(&info.ext, EXT_CLUSTER_VERSION).expect("ext");
    let r = meta
        .propose(MetaCommand::Put {
            key: MetaKey::Cluster,
            expected_version: Some(cur.version),
            record: MetaRecord::Cluster(info),
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_cluster_starts_at_the_newest_level_with_level_2_data() {
    let root = tmp();
    let mut cfg = no_auto(seed_cfg(root.path()));
    // Token mode: the online bootstrap token is the first level-2 record (ext 112).
    cfg.auth.mode = AuthMode::Token;
    let h = start(cfg).await.expect("start");
    assert_eq!(
        h.p13().map(|c| c.cluster.boot_mode()),
        Some(BootMode::Fresh)
    );
    // Right after start, without the finalizer: tag 98 written at this build's level.
    assert_eq!(level(&h), (true, LEVEL));
    // Level-2 data immediately: the bootstrap token carries ext 112 (name, created_by).
    let metas: Vec<_> = h.meta().view().read(|s| {
        s.tokens()
            .map(|t| get_api_token_meta(&t.value.ext).expect("decodable ext 112"))
            .collect()
    });
    assert!(
        metas
            .iter()
            .any(|m| m.as_ref().is_some_and(|m| m.name == BOOTSTRAP_NAME)),
        "bootstrap token without ext 112: {metas:?}"
    );
    // And a token created now keeps its name (ext 112) through the API.
    let admin = std::fs::read_to_string(root.path().join("node1").join(ADMIN_TOKEN_FILE))
        .expect("admin.token")
        .trim()
        .to_owned();
    let http = reqwest::Client::new();
    let r = http
        .post(format!("{}/v1/auth/tokens", api(&h)))
        .bearer_auth(&admin)
        .json(&json!({"name": "ci", "scopes": ["read"]}))
        .send()
        .await
        .expect("create token");
    assert_eq!(r.status().as_u16(), 201);
    let v: Value = r.json().await.expect("json");
    assert_eq!(v["info"]["name"], "ci", "{v}");
    h.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_seed_refuses_older_joiners() {
    let root = tmp();
    let seed = start(no_auto(seed_cfg(root.path()))).await.expect("seed");
    assert_eq!(level(&seed), (true, LEVEL));
    let t = token(&seed, None).await;
    // Join handshake: a binary that cannot run the finalized level is refused.
    let r = JoinRequest {
        token: t.token.clone(),
        csr_pem: csr(),
        advertise: Advertise {
            api: "127.0.0.1:1".into(),
            cluster: "127.0.0.1:2".into(),
            repl: None,
        },
        node_version: version(LEVEL_MIN, LEVEL - 1),
        resume_node_id: None,
    };
    let (st, v) = post(&seed, "/v1/cluster/join", serde_json::to_value(&r).unwrap()).await;
    let reason = v
        .pointer("/error/details/reason")
        .or_else(|| v.pointer("/details/reason"))
        .and_then(Value::as_str);
    assert_eq!((st, reason), (412, Some("level_mismatch")), "{v}");

    // A node prepared by this build, then booted as an older binary: the boot fence refuses
    // it before any partition opens.
    let j = joiner(root.path(), 2);
    let prepared = join(&seed, &j).await.expect("join");
    let e = start_opts(
        no_auto(joined_cfg(&j, prepared.node_id)),
        ServerOptions::default().with_level_max_override(Some(LEVEL - 1)),
    )
    .await
    .expect_err("an older binary must not start on a cluster finalized at the newest level");
    assert!(e.contains("finalized by a newer Ostro"), "{e}");
    assert_eq!(level(&seed), (true, LEVEL));
    seed.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_cluster_restart_keeps_its_level() {
    let root = tmp();
    let cfg = no_auto(seed_cfg(root.path()));
    // A fresh node of an emulated older binary starts at that binary's newest level.
    let h = start_opts(
        cfg.clone(),
        ServerOptions::default().with_level_max_override(Some(LEVEL_MIN)),
    )
    .await
    .expect("start");
    assert_eq!(level(&h), (true, LEVEL_MIN));
    // Metadata as a pre-stamp cluster left it: no tag 98 (finalized by absence).
    strip_cluster_version(h.meta()).await;
    assert_eq!(level(&h), (false, LEVEL_MIN));
    h.shutdown().await.unwrap();

    // Restart as this build: an existing cluster is never stamped; with auto-finalize off it
    // stays at its level until an operator finalizes (the Phase 13 rules).
    let h = start(cfg.clone()).await.expect("restart");
    assert_eq!(
        h.p13().map(|c| c.cluster.boot_mode()),
        Some(BootMode::Existing)
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(level(&h), (false, LEVEL_MIN));
    h.shutdown().await.unwrap();
}
