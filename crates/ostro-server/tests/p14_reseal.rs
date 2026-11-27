//! 14-19 Task 2: system-secret sealing at startup (D-P14-30) and its scope.
//!
//! - With a sealing key (`[secrets] sealing_key_file`), the meta leader creates
//!   `__ostro/ui-session-key` sealed with aes256gcm (32 random bytes), audits
//!   `system.reseal`, and a restart changes nothing (idempotent).
//! - A `plaintext-v0` `__ostro/ui-session-key` is re-sealed after a restart with a key: same
//!   value, ext 117 `rotated_at_ms` stamped.
//! - p14-fixups-2: `__ostro/token-key` (and `__ostro/cluster-ca` once stored) are re-sealed
//!   from `plaintext-v0` with the operator key, audited `system.reseal`, and keep their
//!   values: the await-token key is unchanged across the restart.
//! - A node whose token key is sealed refuses to boot without the key, or with another key,
//!   naming the secret and the sealing key; with the right key it boots.
//! - No key: nothing changes; in token mode the WARN is reported.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_bridge::secrets::{SealedStore, SealingKeyRing};
use ostro_meta::ext::{ClusterVersion, get_secret_times, put_cluster_version};
use ostro_meta::records::{SealedSecret, TOKEN_KEY_PLAINTEXT_ID, TOKEN_KEY_SECRET};
use ostro_meta::{MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_server::ServerHandle;
use ostro_server::config::{AuthMode, ServerConfig};
use ostro_server::p13::join::CLUSTER_CA_SECRET;
use ostro_server::p14::reseal::{self, SecretOutcome, UI_SESSION_KEY_SECRET};
use ostro_server::p14::{self};
use p12::{eventually, single, start, tmp};

const T: Duration = Duration::from_secs(30);
const KEY: [u8; 32] = [42; 32];

fn key_file(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("sealing-test.key");
    std::fs::write(&p, B64.encode(KEY)).expect("key file");
    p
}

fn store() -> SealedStore {
    SealedStore::new(Arc::new(SealingKeyRing::from_bytes(KEY)))
}

fn secret(h: &ServerHandle, name: &str) -> Option<(u64, SealedSecret)> {
    h.meta()
        .view()
        .read(|s| s.get(&MetaKey::Secret(name.to_owned())))
        .and_then(|v| match v.value {
            MetaRecord::Secret(s) => Some((v.version, s)),
            _ => None,
        })
}

async fn ensure_level2(meta: &MetaHandle) {
    for _ in 0..50 {
        let cur = meta.view().read(|s| s.cluster().cloned()).expect("cluster");
        if ostro_meta::ext::get_cluster_version(&cur.value.ext)
            .ok()
            .flatten()
            .is_some_and(|c| c.finalized >= 2)
        {
            return;
        }
        let mut info = cur.value.clone();
        info.ext = put_cluster_version(
            &info.ext,
            &ClusterVersion {
                finalized: 2,
                hold: None,
                finalized_at_ms: 1,
            },
        )
        .expect("ext 98");
        let r = meta
            .propose(MetaCommand::Put {
                key: MetaKey::Cluster,
                expected_version: Some(cur.version),
                record: MetaRecord::Cluster(info),
            })
            .await
            .expect("propose");
        if matches!(r, MetaResult::Ok { .. }) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("could not finalize level 2");
}

fn audit_text(data: &Path) -> String {
    let mut out = String::new();
    if let Ok(rd) = std::fs::read_dir(data.join("audit")) {
        for e in rd.flatten() {
            out.push_str(&std::fs::read_to_string(e.path()).unwrap_or_default());
        }
    }
    out
}

fn other_key_file(dir: &Path) -> std::path::PathBuf {
    let p = dir.join("sealing-other.key");
    std::fs::write(&p, B64.encode([7u8; 32])).expect("key file");
    p
}

fn sealed(h: &ServerHandle, name: &str) -> bool {
    secret(h, name).is_some_and(|(_, s)| s.key_id.starts_with("aes256gcm:"))
}

/// Store this node's CA as `__ostro/cluster-ca` (a node join token request does it).
async fn store_ca(h: &ServerHandle) {
    let r = reqwest::Client::new()
        .post(format!("http://{}/v1/cluster/tokens", h.local_addr()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .expect("token request");
    assert!(r.status().is_success(), "node token: {}", r.status());
}

fn cfg(dir: &Path, key: Option<&Path>) -> ServerConfig {
    let mut c = single(dir, 2);
    c.secrets.sealing_key_file = key.map(Path::to_path_buf);
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ui_session_key_is_created_sealed_and_restart_is_idempotent() {
    let dir = tmp();
    let data = dir.path().join("data");
    let kf = key_file(dir.path());
    let h = start(cfg(&data, Some(&kf))).await.expect("start");
    assert!(
        eventually(T, || secret(&h, UI_SESSION_KEY_SECRET).is_some()).await,
        "{UI_SESSION_KEY_SECRET} was not created"
    );
    let (ver, rec) = secret(&h, UI_SESSION_KEY_SECRET).expect("ui key");
    assert!(rec.key_id.starts_with("aes256gcm:"), "{}", rec.key_id);
    let value = store().open(&rec).expect("opens with the key");
    assert_eq!(value.expose().len(), 32);
    // p14-fixups-2: InitCluster's plaintext token key is re-sealed with the same value.
    assert!(
        eventually(T, || sealed(&h, TOKEN_KEY_SECRET)).await,
        "{TOKEN_KEY_SECRET} was not sealed"
    );
    let token_key = h.token_key();
    let (_, tk) = secret(&h, TOKEN_KEY_SECRET).expect("token key");
    assert_eq!(store().open(&tk).expect("open").expose(), token_key);
    assert_eq!(
        h.meta().view().read(|s| s.token_key()),
        None,
        "raw bytes are no longer the key"
    );
    assert_eq!(
        h.meta()
            .view()
            .read(|s| reseal::token_key(s, Some(&store())))
            .expect("opens"),
        Some(token_key)
    );
    // A second run on the same node is a no-op.
    let ctx = p14::ctx_of(h.control()).expect("ctx").expect("attached");
    let r = reseal::run(&ctx).await;
    assert!(
        r.secrets.contains(&(
            UI_SESSION_KEY_SECRET.to_owned(),
            SecretOutcome::AlreadySealed
        )) && r
            .secrets
            .contains(&(TOKEN_KEY_SECRET.to_owned(), SecretOutcome::AlreadySealed)),
        "{r:?}"
    );
    // The node's CA is stored at startup, sealed (written sealed directly, or re-sealed).
    assert!(
        r.secrets
            .contains(&(CLUSTER_CA_SECRET.to_owned(), SecretOutcome::AlreadySealed)),
        "{r:?}"
    );
    drop(ctx);
    store_ca(&h).await;
    assert!(
        sealed(&h, CLUSTER_CA_SECRET),
        "the CA is stored in plaintext"
    );
    h.shutdown().await.expect("shutdown");
    assert!(
        audit_text(&data).contains("system.reseal"),
        "no system.reseal audit record"
    );

    // Restart: nothing changes.
    let h = start(cfg(&data, Some(&kf))).await.expect("restart");
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (ver2, rec2) = secret(&h, UI_SESSION_KEY_SECRET).expect("ui key");
    assert_eq!(ver2, ver, "the restart rewrote the sealed key");
    assert_eq!(rec2, rec);
    assert_eq!(h.token_key(), token_key, "the await-token key changed");
    h.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plaintext_ui_session_key_is_resealed_after_restart_with_a_key() {
    let dir = tmp();
    let data = dir.path().join("data");
    // No sealing key: nothing is created; open mode does not warn.
    let h = start(cfg(&data, None)).await.expect("start");
    store_ca(&h).await;
    let token_key = h.token_key();
    let (_, ca_plain) = secret(&h, CLUSTER_CA_SECRET).expect("CA stored");
    assert_eq!(
        ca_plain.key_id, TOKEN_KEY_PLAINTEXT_ID,
        "no key: plaintext CA"
    );
    let ctx = p14::ctx_of(h.control()).expect("ctx").expect("attached");
    let r = reseal::run(&ctx).await;
    assert!(r.no_key && !r.warned && r.secrets.is_empty(), "{r:?}");
    drop(ctx);
    assert!(secret(&h, UI_SESSION_KEY_SECRET).is_none());
    ensure_level2(h.meta()).await;
    // Plant a plaintext-v0 UI session key.
    let planted = vec![5u8; 32];
    let r = h
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Secret(UI_SESSION_KEY_SECRET.to_owned()),
            expected_version: None,
            record: MetaRecord::Secret(SealedSecret {
                name: UI_SESSION_KEY_SECRET.to_owned(),
                ciphertext: planted.clone(),
                key_id: TOKEN_KEY_PLAINTEXT_ID.to_owned(),
                ext: Vec::new(),
            }),
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    h.shutdown().await.expect("shutdown");

    // Restart with a key: re-sealed, same value, rotation time stamped.
    let kf = key_file(dir.path());
    let h = start(cfg(&data, Some(&kf))).await.expect("restart");
    assert!(
        eventually(T, || secret(&h, UI_SESSION_KEY_SECRET)
            .is_some_and(|(_, s)| s.key_id.starts_with("aes256gcm:")))
        .await,
        "the plaintext key was not re-sealed"
    );
    let (_, rec) = secret(&h, UI_SESSION_KEY_SECRET).expect("ui key");
    assert_eq!(
        store().open(&rec).expect("open").expose(),
        planted.as_slice()
    );
    let times = get_secret_times(&rec.ext)
        .expect("ext 117")
        .expect("SecretTimes");
    assert!(times.rotated_at_ms.is_some(), "{times:?}");
    // p14-fixups-2: the token key and the CA are re-sealed with their values.
    assert!(
        eventually(T, || sealed(&h, TOKEN_KEY_SECRET)
            && sealed(&h, CLUSTER_CA_SECRET))
        .await,
        "token key / CA not re-sealed"
    );
    assert_eq!(h.token_key(), token_key, "the await-token key changed");
    let (_, tk) = secret(&h, TOKEN_KEY_SECRET).expect("token key");
    assert_eq!(store().open(&tk).expect("open").expose(), token_key);
    let (_, ca) = secret(&h, CLUSTER_CA_SECRET).expect("CA");
    assert_eq!(
        store().open(&ca).expect("open").expose(),
        ca_plain.ciphertext.as_slice()
    );
    let ca_read = h
        .meta()
        .view()
        .read(|s| ostro_server::p13::join::stored_ca(s, Some(&store())))
        .expect("opens")
        .expect("CA");
    assert!(ca_read.key_pem.contains("PRIVATE KEY"));
    for s in [&tk, &ca] {
        let t = get_secret_times(&s.ext).expect("ext 117").expect("times");
        assert!(t.rotated_at_ms.is_some(), "{t:?}");
    }
    h.shutdown().await.expect("shutdown");
    let audit = audit_text(&data);
    for name in [TOKEN_KEY_SECRET, CLUSTER_CA_SECRET] {
        assert!(
            audit
                .lines()
                .any(|l| l.contains("system.reseal") && l.contains(name) && l.contains("resealed")),
            "no system.reseal record for {name}:\n{audit}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_token_key_needs_the_same_key_to_boot() {
    let dir = tmp();
    let data = dir.path().join("data");
    let kf = key_file(dir.path());
    let h = start(cfg(&data, Some(&kf))).await.expect("start");
    assert!(eventually(T, || sealed(&h, TOKEN_KEY_SECRET)).await);
    let token_key = h.token_key();
    h.shutdown().await.expect("shutdown");

    // No key: a clear refusal naming the secret and the sealing key settings.
    let e = start(cfg(&data, None))
        .await
        .expect_err("boots without the key");
    let msg = e.to_string();
    assert!(
        msg.contains(TOKEN_KEY_SECRET)
            && msg.contains("no sealing key")
            && msg.contains("OSTRO_SEALING_KEY")
            && msg.contains("sealing_key_file"),
        "{msg}"
    );
    // Another key: refused too.
    let other = other_key_file(dir.path());
    let e = start(cfg(&data, Some(&other)))
        .await
        .expect_err("boots with another key");
    let msg = e.to_string();
    assert!(
        msg.contains(TOKEN_KEY_SECRET) && msg.contains("cannot be opened"),
        "{msg}"
    );
    // The right key: boots with the same await-token key.
    let h = start(cfg(&data, Some(&kf))).await.expect("restart");
    assert_eq!(h.token_key(), token_key);
    h.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_key_in_token_mode_warns_and_changes_nothing() {
    let dir = tmp();
    let data = dir.path().join("data");
    let mut c = cfg(&data, None);
    c.auth.mode = AuthMode::Token;
    let h = start(c).await.expect("start");
    let ctx = p14::ctx_of(h.control()).expect("ctx").expect("attached");
    let r = reseal::run(&ctx).await;
    assert!(r.no_key && r.warned, "{r:?}");
    drop(ctx);
    assert!(secret(&h, UI_SESSION_KEY_SECRET).is_none());
    let (_, tk) = secret(&h, TOKEN_KEY_SECRET).expect("token key");
    assert_eq!(tk.key_id, TOKEN_KEY_PLAINTEXT_ID);
    h.shutdown().await.expect("shutdown");
}
