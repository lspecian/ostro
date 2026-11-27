//! 14-19 Task 2: system-secret sealing at startup (D-P14-30) and its scope.
//!
//! - With a sealing key (`[secrets] sealing_key_file`), the meta leader creates
//!   `__ostro/ui-session-key` sealed with aes256gcm (32 random bytes), audits
//!   `system.reseal`, and a restart changes nothing (idempotent).
//! - A `plaintext-v0` `__ostro/ui-session-key` is re-sealed after a restart with a key: same
//!   value, ext 117 `rotated_at_ms` stamped.
//! - `__ostro/token-key` and `__ostro/cluster-ca` stay `plaintext-v0` (deferred until their
//!   readers open sealed records, see `p14/reseal.rs`): the await-token key is unchanged
//!   across the restart, so tokens issued before it keep verifying.
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
    // Deferred secrets are untouched and still readable as before.
    let (_, tk) = secret(&h, TOKEN_KEY_SECRET).expect("token key");
    assert_eq!(tk.key_id, TOKEN_KEY_PLAINTEXT_ID);
    let token_key = h.meta().view().read(|s| s.token_key()).expect("token key");
    if let Some((_, ca)) = secret(&h, CLUSTER_CA_SECRET) {
        assert_eq!(ca.key_id, TOKEN_KEY_PLAINTEXT_ID);
    }
    // A second run on the same node is a no-op.
    let ctx = p14::ctx_of(h.control()).expect("ctx").expect("attached");
    let r = reseal::run(&ctx).await;
    assert_eq!(
        r.secrets,
        [(
            UI_SESSION_KEY_SECRET.to_owned(),
            SecretOutcome::AlreadySealed
        )]
    );
    drop(ctx);
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
    assert_eq!(
        h.meta().view().read(|s| s.token_key()),
        Some(token_key),
        "the await-token key changed"
    );
    h.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plaintext_ui_session_key_is_resealed_after_restart_with_a_key() {
    let dir = tmp();
    let data = dir.path().join("data");
    // No sealing key: nothing is created; open mode does not warn.
    let h = start(cfg(&data, None)).await.expect("start");
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
