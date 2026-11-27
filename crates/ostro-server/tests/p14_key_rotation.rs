//! p14-key-rotation: sealing-key rotation (old keys next to the current one, the explicit
//! reseal step, the status that says when an old key can go).
//!
//! - **Three-node rotation** (`three_node_rolling_key_rotation`): a seed and two joiners on
//!   key K1 (token key, cluster CA, UI session key and a user secret sealed with K1, an await
//!   token issued). Rolling restart to `sealing_key_file = K2`, `old_sealing_key_files = [K1]`:
//!   every node boots, the old await token completes, new ones work, a fourth node joins.
//!   `GET /v1/security/sealing` reports the K1 records; `POST /v1/security/sealing/reseal`
//!   moves them all to K2 (audited `system.reseal` with counts), a second pass changes
//!   nothing. Rolling restart with K1 removed: every node still boots and tokens still work;
//!   a node configured with K1 only now refuses to boot with an error naming the key.
//! - **Unknown key** (`record_sealed_with_an_unknown_key_is_reported_clearly`): a record
//!   sealed with a key no node holds is counted `unknown`, and the reseal reports it by name
//!   with the unknown key id instead of failing the pass.
//! - **Dev auto key** (`dev_auto_key_still_never_seals_the_cluster_wide_secrets`): with only
//!   the per-node dev key (token mode), the reseal step skips `__ostro/token-key` and
//!   `__ostro/ui-session-key`: they stay `plaintext-v0` (the p14-fixups-2 / 14-21 rule).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use ostro_bridge::secrets::{SealedStore, SealingKeyRing};
use ostro_meta::records::{TOKEN_KEY_PLAINTEXT_ID, TOKEN_KEY_SECRET};
use ostro_meta::{MetaCommand, MetaKey, MetaRecord, MetaResult, SealedSecret};
use ostro_server::ServerHandle;
use ostro_server::config::ServerConfig;
use ostro_server::p13::join::CLUSTER_CA_SECRET;
use ostro_server::p14::reseal::UI_SESSION_KEY_SECRET;
use serde_json::{Value, json};

const T: Duration = Duration::from_secs(60);
const K1: [u8; 32] = [0x11; 32];
const K2: [u8; 32] = [0x22; 32];
const SECRET_VALUE: &str = "sk_test_rotation_value_42";

fn key_file(dir: &Path, name: &str, key: [u8; 32]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, B64.encode(key)).expect("key file");
    p
}

fn key_id(key: [u8; 32]) -> String {
    SealingKeyRing::from_bytes(key).current_key_id()
}

fn store(key: [u8; 32]) -> SealedStore {
    SealedStore::new(Arc::new(SealingKeyRing::from_bytes(key)))
}

fn record(h: &ServerHandle, name: &str) -> Option<SealedSecret> {
    h.meta()
        .view()
        .read(|s| s.get(&MetaKey::Secret(name.to_owned())))
        .and_then(|v| match v.value {
            MetaRecord::Secret(s) => Some(s),
            _ => None,
        })
}

fn key_ids(h: &ServerHandle) -> Vec<(String, String)> {
    h.meta().view().read(|s| {
        s.secrets()
            .map(|v| (v.value.name.clone(), v.value.key_id.clone()))
            .collect()
    })
}

async fn send(
    h: &ServerHandle,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> (u16, Value) {
    let mut r = reqwest::Client::new().request(method, format!("{}{path}", p13::api(h)));
    if let Some(b) = body {
        r = r.json(&b);
    }
    let r = r.send().await.expect("request");
    let st = r.status().as_u16();
    (st, r.json().await.unwrap_or(Value::Null))
}

/// Retry `method path` on `h` until it answers 2xx (leadership may be moving after a restart).
async fn ok(h: &ServerHandle, method: reqwest::Method, path: &str, body: Option<Value>) -> Value {
    let end = Instant::now() + T; // clock-exempt: test deadline
    loop {
        let (st, v) = send(h, method.clone(), path, body.clone()).await;
        if (200..300).contains(&st) {
            return v;
        }
        assert!(Instant::now() < end, "{method} {path}: {st} {v}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn deploy_tokens(h: &ServerHandle) {
    use sha2::Digest as _;
    let wasm = p12::guest_wasm("tokens");
    let hex = p12::hex(&sha2::Sha256::digest(&wasm));
    let end = Instant::now() + T; // clock-exempt: test deadline
    loop {
        let r = reqwest::Client::new()
            .put(format!("{}/v1/modules/{hex}", p13::api(h)))
            .header("content-type", "application/wasm")
            .body(wasm.clone())
            .send()
            .await
            .expect("PUT module");
        if r.status().is_success() {
            return;
        }
        assert!(Instant::now() < end, "deploy tokens: {}", r.status());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Start a `tokens` run on `h` and return `(run id, its await token)`.
async fn token_run(h: &ServerHandle) -> (String, String) {
    let v = ok(
        h,
        reqwest::Method::POST,
        "/v1/workflows/tokens/runs",
        Some(json!({"input": {}})),
    )
    .await;
    let id = v["run_id"].as_str().expect("run_id").to_owned();
    let end = Instant::now() + T; // clock-exempt: test deadline
    loop {
        let (_, v) = send(h, reqwest::Method::GET, &format!("/v1/runs/{id}"), None).await;
        if let Some(t) = v["published_status"]["token"].as_str() {
            return (id, t.to_owned());
        }
        assert!(Instant::now() < end, "run {id} published no token: {v}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Complete `token` through `h` and wait for the run to complete.
async fn complete(h: &ServerHandle, run: &str, token: &str) {
    let end = Instant::now() + T; // clock-exempt: test deadline
    loop {
        let (st, v) = send(
            h,
            reqwest::Method::POST,
            "/v1/tokens/complete",
            Some(json!({"token": token, "payload": "rotated"})),
        )
        .await;
        if (200..300).contains(&st) {
            break;
        }
        assert_ne!(
            v["error"]["details"]["reason"], "token_invalid",
            "the await token no longer verifies: {v}"
        );
        assert!(Instant::now() < end, "token not accepted: {st} {v}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    loop {
        let (_, v) = send(h, reqwest::Method::GET, &format!("/v1/runs/{run}"), None).await;
        if v["status"] == "completed" {
            return;
        }
        assert!(Instant::now() < end, "run {run} did not complete: {v}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// A node join token through `h` (a metadata write from this node): retried while the
/// metadata group settles after a restart.
async fn join_token_works(h: &ServerHandle) {
    ok(
        h,
        reqwest::Method::POST,
        "/v1/cluster/tokens",
        Some(json!({})),
    )
    .await;
}

type Nodes = [(ServerConfig, Option<ServerHandle>)];

fn n(nodes: &Nodes, i: usize) -> &ServerHandle {
    nodes[i].1.as_ref().expect("running")
}

fn set_keys(c: &mut ServerConfig, current: &Path, old: &[&Path]) {
    c.secrets.sealing_key_file = Some(current.to_path_buf());
    c.secrets.old_sealing_key_files = old.iter().map(|p| p.to_path_buf()).collect();
}

/// Restart every node in turn with `tweak` applied to its configuration; each must boot with
/// the cluster's await-token key and write metadata before the next one goes down.
async fn rolling_restart(
    nodes: &mut Nodes,
    token_key: [u8; 32],
    tweak: impl Fn(&mut ServerConfig),
) {
    for (i, (cfg, slot)) in nodes.iter_mut().enumerate() {
        let h = slot.take().expect("running");
        h.shutdown().await.expect("shutdown");
        tweak(cfg);
        let h = p12::start(cfg.clone())
            .await
            .unwrap_or_else(|e| panic!("node {} did not boot after the key change: {e}", i + 1));
        assert_eq!(h.token_key(), token_key, "node {}: await-token key", i + 1);
        join_token_works(&h).await;
        *slot = Some(h);
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_node_rolling_key_rotation() {
    let root = p12::tmp();
    let k1 = key_file(root.path(), "k1.key", K1);
    let k2 = key_file(root.path(), "k2.key", K2);
    let (id1, id2) = (key_id(K1), key_id(K2));

    // ---- a three-node cluster on K1 ----
    let mut sc = p13::seed_cfg(root.path());
    set_keys(&mut sc, &k1, &[]);
    let seed = p12::start(sc.clone()).await.expect("seed");
    assert!(
        p12::eventually(T, || record(&seed, TOKEN_KEY_SECRET)
            .is_some_and(|r| r.key_id == id1)
            && record(&seed, UI_SESSION_KEY_SECRET).is_some())
        .await,
        "token key / UI session key not sealed with K1"
    );
    let token_key = seed.token_key();
    let mut nodes: Vec<(ServerConfig, Option<ServerHandle>)> = Vec::new();
    let mut ids = Vec::new();
    for k in [2u64, 3] {
        let j = p13::joiner(root.path(), k);
        let prepared = p13::join(&seed, &j).await.expect("join");
        let mut c = p13::joined_cfg(&j, prepared.node_id);
        set_keys(&mut c, &k1, &[]);
        let h = p12::start(c.clone()).await.expect("joiner boots");
        ids.push(prepared.node_id);
        nodes.push((c, Some(h)));
    }
    nodes.insert(0, (sc, Some(seed)));
    p13::status_until(n(&nodes, 0), Duration::from_secs(90), |st| {
        ids.iter()
            .all(|id| p13::member(st, *id).is_some_and(|m| m["state"] == "active"))
    })
    .await;
    assert_eq!(
        record(n(&nodes, 0), CLUSTER_CA_SECRET).map(|r| r.key_id),
        Some(id1.clone()),
        "the cluster CA is sealed with K1"
    );
    ok(
        n(&nodes, 1),
        reqwest::Method::PUT,
        "/v1/secrets/stripe/key",
        Some(json!({"value": SECRET_VALUE})),
    )
    .await;
    deploy_tokens(n(&nodes, 0)).await;
    let (run_before, token_before) = token_run(n(&nodes, 0)).await;

    // ---- rolling key change: K2 current, K1 old ----
    rolling_restart(&mut nodes, token_key, |c| set_keys(c, &k2, &[&k1])).await;

    // Await tokens: issued before the change, completed after it on another node; new ones.
    complete(n(&nodes, 2), &run_before, &token_before).await;
    let (run, token) = token_run(n(&nodes, 1)).await;
    complete(n(&nodes, 0), &run, &token).await;

    // Joins keep working: a fourth node with the same keys opens the K1-sealed records.
    let j4 = p13::joiner(root.path(), 4);
    let prepared = p13::join(n(&nodes, 1), &j4).await.expect("join node 4");
    let mut c4 = p13::joined_cfg(&j4, prepared.node_id);
    set_keys(&mut c4, &k2, &[&k1]);
    let h4 = p12::start(c4.clone()).await.expect("node 4 boots");
    assert_eq!(h4.token_key(), token_key);
    assert!(j4.data_dir.join("tls/ca.key").is_file(), "CA not imported");
    nodes.push((c4, Some(h4)));

    // ---- status: K1 records remain ----
    let st = ok(
        n(&nodes, 1),
        reqwest::Method::GET,
        "/v1/security/sealing",
        None,
    )
    .await;
    assert_eq!(st["current_key_id"], json!(id2), "{st}");
    assert_eq!(st["old_key_ids"], json!([id1]), "{st}");
    assert!(st["old"].as_u64().expect("old") >= 4, "{st}");
    assert_eq!(st["unknown"], 0, "{st}");
    assert_eq!(st["old_keys_removable"], false, "{st}");

    // ---- reseal (on another node than the meta leader, most likely) ----
    let r = ok(
        n(&nodes, 2),
        reqwest::Method::POST,
        "/v1/security/sealing/reseal",
        None,
    )
    .await;
    assert_eq!(r["key_id"], json!(id2), "{r}");
    assert_eq!(r["failed"], json!([]), "{r}");
    assert!(r["resealed"].as_u64().expect("resealed") >= 4, "{r}");
    assert_eq!(r["status"]["old"], 0, "{r}");
    assert_eq!(r["status"]["old_keys_removable"], true, "{r}");
    let records = r["records"].as_u64().expect("records");
    assert!(
        p12::eventually(T, || key_ids(n(&nodes, 0)).iter().all(|(_, k)| *k == id2)).await,
        "records still under K1: {:?}",
        key_ids(n(&nodes, 0))
    );
    // Same values, now under K2.
    let tk = record(n(&nodes, 0), TOKEN_KEY_SECRET).expect("token key");
    assert_eq!(store(K2).open(&tk).expect("open").expose(), token_key);
    let user = record(n(&nodes, 0), "_/stripe/key").expect("user secret");
    assert_eq!(
        store(K2).open(&user).expect("open").expose(),
        SECRET_VALUE.as_bytes()
    );
    // Idempotent.
    let again = ok(
        n(&nodes, 1),
        reqwest::Method::POST,
        "/v1/security/sealing/reseal",
        None,
    )
    .await;
    assert_eq!(again["resealed"], 0, "{again}");
    assert_eq!(again["already_current"], json!(records), "{again}");
    // Audited with the counts on the node that ran the pass.
    let audit = audit_text(&nodes[2].0.node.data_dir);
    assert!(
        audit.lines().any(|l| l.contains("\"system.reseal\"")
            && l.contains("\"records\"")
            && l.contains("\"resealed\"")),
        "no system.reseal record with counts:\n{audit}"
    );
    let st = ok(
        n(&nodes, 3),
        reqwest::Method::GET,
        "/v1/security/sealing",
        None,
    )
    .await;
    assert_eq!(st["old_keys_removable"], true, "{st}");

    // ---- remove K1 everywhere ----
    rolling_restart(&mut nodes, token_key, |c| set_keys(c, &k2, &[])).await;
    let (run, token) = token_run(n(&nodes, 3)).await;
    complete(n(&nodes, 1), &run, &token).await;
    let st = ok(
        n(&nodes, 0),
        reqwest::Method::GET,
        "/v1/security/sealing",
        None,
    )
    .await;
    assert_eq!(st["old_key_ids"], json!([]), "{st}");
    assert_eq!(
        (st["old"].clone(), st["unknown"].clone()),
        (json!(0), json!(0)),
        "{st}"
    );

    // A node left on K1 only now holds no key of the stored records: a clear refusal.
    let h = nodes[2].1.take().expect("running");
    h.shutdown().await.expect("shutdown");
    let mut stale = nodes[2].0.clone();
    set_keys(&mut stale, &k1, &[]);
    let e = p12::start(stale)
        .await
        .expect_err("boots with the retired key only");
    let msg = e.to_string();
    assert!(
        msg.contains(TOKEN_KEY_SECRET)
            && msg.contains(&id2)
            && msg.contains("unknown sealing key id")
            && msg.contains("old_sealing_key_files"),
        "{msg}"
    );
    let h = p12::start(nodes[2].0.clone())
        .await
        .expect("node 3 with K2 boots");
    nodes[2].1 = Some(h);

    for (_, h) in nodes.into_iter().rev() {
        if let Some(h) = h {
            h.shutdown().await.expect("shutdown");
        }
    }
}

fn single_cfg(data: &Path, key: Option<&Path>) -> ServerConfig {
    let mut c = p12::single(data, 2);
    c.secrets.sealing_key_file = key.map(Path::to_path_buf);
    c
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn record_sealed_with_an_unknown_key_is_reported_clearly() {
    let dir = p12::tmp();
    let k1 = key_file(dir.path(), "k1.key", K1);
    let h = p12::start(single_cfg(&dir.path().join("data"), Some(&k1)))
        .await
        .expect("start");
    assert!(
        p12::eventually(T, || record(&h, TOKEN_KEY_SECRET)
            .is_some_and(|r| r.key_id == key_id(K1)))
        .await
    );
    // A record sealed with a key nobody configured (as after losing a key).
    let lost = [0x77; 32];
    let rec = store(lost)
        .seal("_/orphan", b"orphan-value-123")
        .expect("seal");
    let r = h
        .meta()
        .propose(MetaCommand::Put {
            key: MetaKey::Secret("_/orphan".into()),
            expected_version: None,
            record: MetaRecord::Secret(rec),
        })
        .await
        .expect("propose");
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");

    let st = ok(&h, reqwest::Method::GET, "/v1/security/sealing", None).await;
    assert_eq!(st["unknown"], 1, "{st}");
    assert!(
        st["keys"]
            .as_array()
            .expect("keys")
            .iter()
            .any(|k| k["key_id"] == json!(key_id(lost)) && k["state"] == "unknown"),
        "{st}"
    );
    let r = ok(
        &h,
        reqwest::Method::POST,
        "/v1/security/sealing/reseal",
        None,
    )
    .await;
    let failed = r["failed"].as_array().expect("failed");
    assert_eq!(failed.len(), 1, "{r}");
    let f = failed[0].as_str().expect("text");
    assert!(
        f.starts_with("_/orphan: ")
            && f.contains(&key_id(lost))
            && f.contains("unknown sealing key id")
            && f.contains("OSTRO_SEALING_KEY_OLD"),
        "{f}"
    );
    // Untouched, and the rest of the pass went through.
    assert_eq!(record(&h, "_/orphan").expect("orphan").key_id, key_id(lost));
    assert_eq!(r["status"]["unknown"], 1, "{r}");
    assert_eq!(r["status"]["old"], 0, "{r}");
    h.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dev_auto_key_still_never_seals_the_cluster_wide_secrets() {
    let dir = p12::tmp();
    let data = dir.path().join("data");
    let mut c = single_cfg(&data, None);
    c.secrets.auto_sealing_key = true;
    c.auth.mode = ostro_server::config::AuthMode::Token;
    let h = p12::start(c).await.expect("start");
    assert!(
        data.join("sealing.key").is_file(),
        "the dev key was generated"
    );
    // 14-21: the UI session key exists (token mode), in plaintext.
    assert!(
        p12::eventually(T, || record(&h, UI_SESSION_KEY_SECRET).is_some()).await,
        "no UI session key"
    );
    let token = std::fs::read_to_string(data.join("admin.token")).expect("bootstrap token");
    let r = reqwest::Client::new()
        .post(format!("{}/v1/security/sealing/reseal", p13::api(&h)))
        .bearer_auth(token.trim())
        .send()
        .await
        .expect("reseal");
    assert!(r.status().is_success(), "reseal: {}", r.status());
    let r: Value = r.json().await.expect("json");
    assert_eq!(r["failed"], json!([]), "{r}");
    assert!(r["skipped"].as_u64().expect("skipped") >= 2, "{r}");
    for name in [TOKEN_KEY_SECRET, UI_SESSION_KEY_SECRET] {
        assert_eq!(
            record(&h, name).expect("record").key_id,
            TOKEN_KEY_PLAINTEXT_ID,
            "the dev auto key sealed the cluster-wide {name}"
        );
    }
    assert!(
        r["status"]["plaintext"].as_u64().expect("plaintext") >= 2,
        "{r}"
    );
    h.shutdown().await.expect("shutdown");
}
