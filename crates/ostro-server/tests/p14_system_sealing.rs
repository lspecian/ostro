//! p14-fixups-2: `__ostro/token-key` and `__ostro/cluster-ca` sealed at rest (aes256gcm with
//! the operator sealing key, `p14/reseal.rs`).
//!
//! - An await token issued before a restart completes after it, with the token key sealed in
//!   metadata; a token minted after the restart completes too.
//! - Three nodes over real loopback mTLS: a seed with the key, two joiners with the same key
//!   boot (they open the sealed token key and import the sealed CA for their Phase 7 TLS
//!   state). A fourth joiner without the key completes the handshake (only the CA
//!   certificate travels) but refuses to boot with a clear error.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use common::{ServerOpts, TestServer};
use ostro_meta::records::TOKEN_KEY_SECRET;
use ostro_meta::{MetaKey, MetaRecord, SealedSecret};
use ostro_partition::store::values::RunStatus;
use ostro_server::ServerHandle;
use ostro_server::p13::join::CLUSTER_CA_SECRET;
use serde_json::json;

const T: Duration = Duration::from_secs(30);
const KEY: [u8; 32] = [0x2a; 32];

fn write_key(dir: &Path) -> PathBuf {
    let p = dir.join("sealing-test.key");
    std::fs::write(&p, B64.encode(KEY)).expect("key file");
    p
}

fn secret(h: &ServerHandle, name: &str) -> Option<SealedSecret> {
    h.meta()
        .view()
        .read(|s| s.get(&MetaKey::Secret(name.to_owned())))
        .and_then(|v| match v.value {
            MetaRecord::Secret(s) => Some(s),
            _ => None,
        })
}

fn sealed(h: &ServerHandle, name: &str) -> bool {
    secret(h, name).is_some_and(|s| s.key_id.starts_with("aes256gcm:"))
}

async fn complete(s: &TestServer, token: &str, payload: serde_json::Value) {
    // The run may not have opened its wait yet: retry until accepted (bounded).
    let deadline = std::time::Instant::now() + T; // clock-exempt: test deadline
    loop {
        let r = s
            .http
            .post(s.url("/v1/tokens/complete"))
            .json(&json!({"token": token, "payload": payload}))
            .send()
            .await
            .expect("complete token");
        let st = r.status();
        let body: serde_json::Value = r.json().await.unwrap_or_default();
        if st.is_success() {
            assert_eq!(body["accepted"], true, "{body}");
            return;
        }
        assert_ne!(
            body["error"]["details"]["reason"], "token_invalid",
            "the token no longer verifies: {body}"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "token not accepted: {st} {body}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn await_token_round_trip_across_a_restart_with_a_sealed_token_key() {
    let mut s = TestServer::start_with(ServerOpts::default(), |c| {
        c.secrets.sealing_key_file = Some(write_key(&c.node.data_dir));
    })
    .await;
    s.wait_ready(T).await;
    let key = s.h().token_key();
    assert!(
        p12::eventually(T, || sealed(s.h(), TOKEN_KEY_SECRET)).await,
        "{TOKEN_KEY_SECRET} was not sealed"
    );
    assert_eq!(s.h().meta().view().read(|m| m.token_key()), None);

    s.deploy_guest("tokens", None).await;
    let before = s.start_rest("tokens", json!({"input": {}})).await;
    let token = s.published_token(&before.to_string(), T).await;

    s.restart().await;
    s.wait_ready(T).await;
    assert_eq!(s.h().token_key(), key, "the await-token key changed");
    assert!(sealed(s.h(), TOKEN_KEY_SECRET));

    // Issued before the restart, completed after it.
    complete(&s, &token, json!("before")).await;
    s.wait_status(&before, RunStatus::Completed, T).await;

    // Minted after the restart (with the key opened from the sealed record).
    let after = s.start_rest("tokens", json!({"input": {}})).await;
    let token2 = s.published_token(&after.to_string(), T).await;
    let parts = ostro_engine::token::parse(&token2).expect("parse");
    assert_eq!(
        ostro_engine::token::mint(parts.partition, &parts.run_id, parts.op, &key),
        token2,
        "minted with the cluster key"
    );
    complete(&s, &token2, json!("after")).await;
    s.wait_status(&after, RunStatus::Completed, T).await;
    s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_join_with_a_sealed_cluster_ca() {
    let root = p12::tmp();
    let kf = write_key(root.path());
    let mut sc = p13::seed_cfg(root.path());
    sc.secrets.sealing_key_file = Some(kf.clone());
    let seed = p12::start(sc).await.expect("seed");
    assert!(
        p12::eventually(T, || sealed(&seed, TOKEN_KEY_SECRET)).await,
        "{TOKEN_KEY_SECRET} was not sealed"
    );
    let seed_ca = std::fs::read_to_string(root.path().join("node1/tls/ca.crt")).expect("seed CA");

    let mut joined = Vec::new();
    for k in [2u64, 3] {
        let j = p13::joiner(root.path(), k);
        let prepared = p13::join(&seed, &j).await.expect("join");
        assert!(
            sealed(&seed, CLUSTER_CA_SECRET),
            "the cluster CA is stored sealed"
        );
        let mut c = p13::joined_cfg(&j, prepared.node_id);
        c.secrets.sealing_key_file = Some(kf.clone());
        let h = p12::start(c).await.expect("a joiner with the key boots");
        assert_eq!(h.token_key(), seed.token_key());
        // The sealed CA was opened and imported for the Phase 7 TLS state.
        assert!(
            j.data_dir.join("tls/ca.key").is_file(),
            "CA key not imported"
        );
        assert_eq!(
            std::fs::read_to_string(j.data_dir.join("tls/ca.crt")).expect("ca.crt"),
            seed_ca
        );
        joined.push((prepared.node_id, h));
    }
    let ids: Vec<u64> = joined.iter().map(|(id, _)| *id).collect();
    p13::status_until(&seed, Duration::from_secs(90), |st| {
        ids.iter()
            .all(|id| p13::member(st, *id).is_some_and(|m| m["state"] == "active"))
    })
    .await;

    // No key: the handshake works (only the CA certificate travels), the boot refuses.
    let j4 = p13::joiner(root.path(), 4);
    let prepared = p13::join(&seed, &j4)
        .await
        .expect("handshake without the key");
    let e = p12::start(p13::joined_cfg(&j4, prepared.node_id))
        .await
        .expect_err("a joiner without the key boots");
    let msg = e.to_string();
    assert!(
        msg.contains(TOKEN_KEY_SECRET)
            && msg.contains("same sealing key")
            && msg.contains("OSTRO_SEALING_KEY"),
        "{msg}"
    );

    for (_, h) in joined {
        h.shutdown().await.expect("joined shutdown");
    }
    seed.shutdown().await.expect("seed shutdown");
}
