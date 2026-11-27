//! 14-28 (D-P14-48, CLI-01): in-place module replacement under `ostro dev --ephemeral`
//! only, on the real in-process server with `guests/conf-rust` (module A) and
//! `guests/conf-rust-v2` (module B). Both export the same `(name, version)` pairs;
//! `rs_conf_patch` v1 takes a new branch in B (`a2`), `rs_conf_ndet` v1 reorders its steps
//! in B (an A journal replayed on B is non-determinism), `rs_conf_signals` (identical in
//! both) parks on two `go` signals.
//!
//! - **ephemeral dev** (constructor [`ephemeral_dev`]: `ServerConfig::dev_defaults()` with
//!   `node.ephemeral = true`, which is what `ostro dev --ephemeral` loads; the profile is
//!   derived from the configuration as `server.rs` does): uploading B replaced A's
//!   bindings (the active version stays 1, now bound to B); a parked run keeps A and
//!   completes on A after the signals; a new run runs B; a re-upload of B is the usual
//!   idempotent no-op; a run on A started right before a replacement completes on A
//!   without non-determinism; with `guests/patch-v{1,2}`: A, retired, is kept by module GC
//!   while a run needs it and deleted afterwards without touching B's binding;
//! - **persistent dev**, **server profile** and **server profile with `node.ephemeral`**
//!   (the test-kit configuration): 409 `conflict` naming A's sha, nothing stored, new runs
//!   still run A.
//! - the upload response of a replacement carries `replaced: true` and `previous_module`
//!   (REST and gRPC; p14-fixups-3).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};
use ostro_server::config::ServerConfig;
use ostro_types::RunId;
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const LIMIT: Duration = Duration::from_secs(120);

fn hex(sha: &[u8]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

/// Shared tweaks: a short hot wait (parked runs are evicted, so their wake-up replays the
/// journal on the pinned module) and a short GC grace.
fn tweak(cfg: &mut ServerConfig) {
    cfg.node.partitions = 4;
    cfg.engine.hot_wait = Duration::from_millis(50).into();
    cfg.modules.gc_grace = Duration::from_millis(300).into();
}

/// The server `ostro dev --ephemeral` starts: dev defaults + `node.ephemeral`.
async fn ephemeral_dev() -> TestServer {
    let dir = tmpfs_dir();
    let mut cfg = ServerConfig::dev_defaults();
    cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
    cfg.node.data_dir = dir.path().to_path_buf();
    cfg.node.ephemeral = true;
    tweak(&mut cfg);
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

/// `ostro dev` without `--ephemeral`: dev defaults on a persistent data dir.
async fn persistent_dev() -> TestServer {
    let dir = tmpfs_dir();
    let mut cfg = ServerConfig::dev_defaults();
    cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
    cfg.node.data_dir = dir.path().to_path_buf();
    tweak(&mut cfg);
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

/// The server profile, optionally with `node.ephemeral` (the test kits' configuration).
async fn server_profile(ephemeral: bool) -> TestServer {
    let dir = tmpfs_dir();
    let mut cfg = server_config(
        dir.path(),
        &ServerOpts {
            partitions: 4,
            ephemeral,
        },
    );
    tweak(&mut cfg);
    let s = TestServer::start_cfg(cfg, Some(dir)).await;
    s.wait_ready(Duration::from_secs(30)).await;
    s
}

async fn put_module(s: &TestServer, wasm: &[u8]) -> (StatusCode, Value) {
    put_module_in(s, wasm, None).await
}

async fn put_module_in(s: &TestServer, wasm: &[u8], tenant: Option<&str>) -> (StatusCode, Value) {
    let q = tenant.map(|t| format!("&tenant={t}")).unwrap_or_default();
    let r = s
        .http
        .put(s.url(&format!(
            "/v1/modules/{}?language=rust{q}",
            hex(&Sha256::digest(wasm))
        )))
        .header("content-type", "application/wasm")
        .body(wasm.to_vec())
        .send()
        .await
        .expect("PUT module");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

async fn start(s: &TestServer, workflow: &str) -> RunId {
    s.start_rest(workflow, json!({ "input": {} })).await
}

/// The run's module pin (`GET /v1/runs/{id}`).
async fn pin(s: &TestServer, run: &RunId) -> String {
    let (st, v) = s.get_json(&format!("/v1/runs/{run}")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v["module_sha256"]
        .as_str()
        .expect("module_sha256")
        .to_owned()
}

/// `true` while the run is not terminal (`GET /result` without a wait).
async fn live(s: &TestServer, run: &RunId) -> bool {
    let (st, v) = s.get_json(&format!("/v1/runs/{run}/result")).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v["completed"] == json!(false)
}

/// Wait for a terminal state; the result body (`status`, `result`, `failure`).
async fn outcome(s: &TestServer, run: &RunId) -> Value {
    let deadline = tokio::time::Instant::now() + LIMIT;
    loop {
        let (st, v) = s.get_json(&format!("/v1/runs/{run}/result?wait=30s")).await;
        assert_eq!(st, StatusCode::OK, "{v}");
        if v["completed"] == json!(true) {
            return v;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "run {run} not terminal: {v}"
        );
    }
}

async fn completed(s: &TestServer, run: &RunId) -> Value {
    let v = outcome(s, run).await;
    assert_eq!(v["status"], "completed", "run {run}: {v}");
    v["result"].clone()
}

/// Deliver both `go` signals `rs_conf_signals` waits for (each wake-up replays the
/// journal: the hot wait is 50 ms).
async fn signal_go(s: &TestServer, run: &RunId) {
    for _ in 0..2 {
        let (st, v) = s
            .post_json(
                &format!("/v1/runs/{run}/signals/go"),
                &json!({ "payload": "ok" }),
            )
            .await;
        assert_eq!(st, StatusCode::OK, "signal {run}: {v}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The module each `(name, version 1)` is bound to, and the active version.
async fn binding(s: &TestServer, name: &str) -> (String, Option<u64>) {
    let (st, v) = s.get_json("/v1/workflows").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let w = v["workflows"]
        .as_array()
        .expect("array")
        .iter()
        .find(|w| w["name"] == name)
        .unwrap_or_else(|| panic!("{name} not in {v}"))
        .clone();
    let ver = w["versions"]
        .as_array()
        .expect("versions")
        .iter()
        .find(|x| x["version"] == 1)
        .unwrap_or_else(|| panic!("{name}@1 not in {w}"))
        .clone();
    (
        ver["module_sha256"].as_str().expect("sha").to_owned(),
        w["active_version"].as_u64(),
    )
}

async fn module_status(s: &TestServer, sha: &str) -> StatusCode {
    s.get_json(&format!("/v1/modules/{sha}")).await.0
}

/// `DELETE /v1/modules/{sha}` (retire), waiting out the level-2 feature gate.
async fn retire(s: &TestServer, sha: &str) -> (StatusCode, Value) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let r = s
            .http
            .delete(s.url(&format!("/v1/modules/{sha}")))
            .send()
            .await
            .expect("DELETE module");
        let st = r.status();
        let b = r.bytes().await.expect("body");
        let v: Value = serde_json::from_slice(&b).unwrap_or(Value::Null);
        if v["error"]["details"]["reason"] != "feature_not_enabled"
            || tokio::time::Instant::now() >= deadline
        {
            return (st, v);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn gc(s: &TestServer) -> Value {
    let (st, v) = s.post_json("/v1/modules/gc", &json!({})).await;
    assert_eq!(st, StatusCode::OK, "{v}");
    v
}

fn listed(v: &Value, field: &str, sha: &str) -> bool {
    v[field]
        .as_array()
        .is_some_and(|a| a.iter().any(|x| x == sha))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_dev_replaced_module_serves_new_runs_old_runs_keep_theirs() {
    let (wasm_a, wasm_b) = (guest_wasm("conf-rust"), guest_wasm("conf-rust-v2"));
    let (a, b) = (hex(&Sha256::digest(&wasm_a)), hex(&Sha256::digest(&wasm_b)));
    let s = ephemeral_dev().await;

    let (st, v) = put_module(&s, &wasm_a).await;
    assert!(st.is_success(), "upload A: {st} {v}");
    assert_eq!(binding(&s, "rs_conf_patch").await, (a.clone(), Some(1)));

    // r1 parks on a signal, pinned to A
    let r1 = start(&s, "rs_conf_signals").await;
    assert_eq!(pin(&s, &r1).await, a);

    // B under the same (name, version) pairs: replaced in place
    let (st, v) = put_module(&s, &wasm_b).await;
    assert!(st.is_success(), "upload B: {st} {v}");
    assert_eq!(v["created"], true, "{v}");
    for wf in ["rs_conf_patch", "rs_conf_ndet", "rs_conf_signals"] {
        let (bound, active) = binding(&s, wf).await;
        assert_eq!(bound, b, "{wf} replaced: now bound to B");
        assert_eq!(active, Some(1), "{wf}: the active version is still 1");
    }
    assert!(live(&s, &r1).await, "r1 is still parked");

    // r2: a new run runs B (the patched branch only B has)
    let r2 = start(&s, "rs_conf_patch").await;
    assert_eq!(pin(&s, &r2).await, b);
    let out = completed(&s, &r2).await;
    assert_eq!(out, json!(["a2", "b"]), "r2 ran B");

    // r1 wakes up on A (each wake-up replays its journal on its pinned module)
    signal_go(&s, &r1).await;
    assert_eq!(completed(&s, &r1).await, json!({ "got": ["ok", "ok"] }));
    assert_eq!(pin(&s, &r1).await, a);

    // re-uploading B is the usual idempotent no-op
    let (st, v) = put_module(&s, &wasm_b).await;
    assert!(st.is_success(), "re-upload B: {st} {v}");
    assert_eq!(v["created"], false, "{v}");
    assert_eq!(binding(&s, "rs_conf_patch").await, (b.clone(), Some(1)));

    // A run on A started right before a replacement completes on A: no non-determinism.
    // A comes back (replaced again), r3 starts on it and sleeps 1 s; B (already stored,
    // so the replacement is a few metadata writes) replaces A while r3 sleeps.
    let (st, v) = put_module(&s, &wasm_a).await;
    assert!(st.is_success(), "upload A again: {st} {v}");
    assert_eq!(binding(&s, "rs_conf_ndet").await, (a.clone(), Some(1)));
    let r3 = start(&s, "rs_conf_ndet").await;
    let (st, v) = put_module(&s, &wasm_b).await;
    assert!(st.is_success(), "replace with B: {st} {v}");
    assert_eq!(binding(&s, "rs_conf_ndet").await, (b.clone(), Some(1)));
    assert!(live(&s, &r3).await, "r3 still sleeping when B replaced A");
    assert_ne!(
        outcome(&s, &r3).await["status"],
        "failed",
        "r3 must not replay on B"
    );
    let r3_out = completed(&s, &r3).await;
    assert_eq!(r3_out, json!(["a", "b"]), "r3 ran A's step order");
    assert_eq!(pin(&s, &r3).await, a);
    let r4 = start(&s, "rs_conf_ndet").await;
    assert_eq!(completed(&s, &r4).await, json!(["b", "a"]), "r4 ran B");
}

/// The upload response reports a replacement (p14-fixups-3): REST `replaced: true` +
/// `previous_module` (both omitted otherwise), gRPC `replaced` + `previous_module`; also
/// when the replacing module was already stored (`created: false`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_dev_upload_response_names_the_replaced_module() {
    use ostro_proto::api::v1 as pb;
    use ostro_proto::api::v1::modules_client::ModulesClient;

    let (wasm_a, wasm_b) = (guest_wasm("conf-rust"), guest_wasm("conf-rust-v2"));
    let (a, b) = (hex(&Sha256::digest(&wasm_a)), hex(&Sha256::digest(&wasm_b)));
    let s = ephemeral_dev().await;

    // A fresh binding replaces nothing: both fields omitted.
    let (st, v) = put_module(&s, &wasm_a).await;
    assert_eq!(st, StatusCode::CREATED, "upload A: {v}");
    assert!(v.get("replaced").is_none(), "{v}");
    assert!(v.get("previous_module").is_none(), "{v}");

    // B replaces A.
    let (st, v) = put_module(&s, &wasm_b).await;
    assert_eq!(st, StatusCode::CREATED, "upload B: {v}");
    assert_eq!(v["replaced"], true, "{v}");
    assert_eq!(v["previous_module"], a.as_str(), "{v}");

    // The idempotent re-upload of B replaces nothing.
    let (st, v) = put_module(&s, &wasm_b).await;
    assert_eq!(st, StatusCode::OK, "re-upload B: {v}");
    assert!(v.get("replaced").is_none(), "{v}");
    assert!(v.get("previous_module").is_none(), "{v}");

    // gRPC: A (already stored) comes back over B.
    let mut modules = ModulesClient::new(s.channel().await).max_encoding_message_size(64 << 20);
    let upload = |wasm: Vec<u8>, sha: String| pb::UploadModuleRequest {
        sha256: sha,
        wasm,
        language: Some("rust".into()),
        tenant: None,
        name: None,
        signatures: vec![],
    };
    let r = modules
        .upload(upload(wasm_a.clone(), a.clone()))
        .await
        .expect("gRPC upload A")
        .into_inner();
    assert!(!r.created, "A was already stored");
    assert!(r.replaced, "{r:?}");
    assert_eq!(r.previous_module.as_deref(), Some(b.as_str()), "{r:?}");
    assert_eq!(binding(&s, "rs_conf_patch").await, (a.clone(), Some(1)));
    let r = modules
        .upload(upload(wasm_a, a))
        .await
        .expect("gRPC re-upload A")
        .into_inner();
    assert!(!r.replaced && r.previous_module.is_none(), "{r:?}");
}

/// Ephemeral dev, `guests/patch-v1` (A) and `guests/patch-v2` (B), both `patch-demo` v1
/// only: after the replacement A has no version left, so it can be retired; module GC
/// keeps it while a run on A is live and deletes it afterwards, leaving B's binding alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ephemeral_dev_replaced_module_is_kept_by_gc_while_a_run_needs_it() {
    let (wasm_a, wasm_b) = (guest_wasm("patch-v1"), guest_wasm("patch-v2"));
    let (a, b) = (hex(&Sha256::digest(&wasm_a)), hex(&Sha256::digest(&wasm_b)));
    let s = ephemeral_dev().await;
    let (st, v) = put_module(&s, &wasm_a).await;
    assert!(st.is_success(), "upload A: {st} {v}");
    // B is compiled and stored up front (another tenant), so the replacement below is a
    // few metadata writes and fits in the run's 2 s sleep.
    let (st, v) = put_module_in(&s, &wasm_b, Some("stage")).await;
    assert!(st.is_success(), "stage B: {st} {v}");
    // the retire feature gate is open (B is active in `stage`: module_active, not the gate)
    let (st, v) = retire(&s, &b).await;
    assert_eq!(st, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["error"]["details"]["reason"], "module_active", "{v}");

    // r runs on A and sleeps 2 s; B replaces A meanwhile; A is retired
    let r = start(&s, "patch-demo").await;
    let (st, v) = put_module(&s, &wasm_b).await;
    assert!(st.is_success(), "replace with B: {st} {v}");
    assert_eq!(binding(&s, "patch-demo").await, (b.clone(), Some(1)));
    let (st, v) = retire(&s, &a).await;
    assert_eq!(st, StatusCode::OK, "retire A (no version left): {v}");
    tokio::time::sleep(Duration::from_millis(350)).await;
    let report = gc(&s).await;
    assert!(live(&s, &r).await, "r still sleeping after the GC round");
    assert!(listed(&report, "candidates", &a), "{report}");
    assert!(listed(&report, "referenced", &a), "r needs A: {report}");
    assert!(!listed(&report, "deleted", &a), "{report}");
    assert_eq!(module_status(&s, &a).await, StatusCode::OK);

    // r completes on A (the old path: no step C)
    assert_eq!(completed(&s, &r).await, json!(["A", "B"]));
    assert_eq!(pin(&s, &r).await, a);

    // now A is GC-eligible; B's binding is untouched by the deletion
    let deadline = tokio::time::Instant::now() + LIMIT;
    loop {
        let report = gc(&s).await;
        if listed(&report, "deleted", &a) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "A never deleted: {report}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(module_status(&s, &a).await, StatusCode::NOT_FOUND);
    assert_eq!(binding(&s, "patch-demo").await, (b.clone(), Some(1)));
    let r2 = start(&s, "patch-demo").await;
    assert_eq!(completed(&s, &r2).await, json!(["A", "C", "B"]), "r2 ran B");
    assert_eq!(pin(&s, &r2).await, b);
}

/// Every other profile keeps write-once bindings: 409, nothing stored, new runs run A.
async fn write_once(s: TestServer, what: &str) {
    let (wasm_a, wasm_b) = (guest_wasm("conf-rust"), guest_wasm("conf-rust-v2"));
    let (a, b) = (hex(&Sha256::digest(&wasm_a)), hex(&Sha256::digest(&wasm_b)));
    let (st, v) = put_module(&s, &wasm_a).await;
    assert!(st.is_success(), "{what}: upload A: {st} {v}");
    let r1 = start(&s, "rs_conf_signals").await;

    let (st, v) = put_module(&s, &wasm_b).await;
    assert_eq!(st, StatusCode::CONFLICT, "{what}: {v}");
    assert_eq!(v["error"]["code"], "conflict", "{what}: {v}");
    assert_eq!(
        v["error"]["details"]["existing_module_sha256"], a,
        "{what}: {v}"
    );
    assert!(
        v["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("is already registered by module")),
        "{what}: {v}"
    );
    assert_eq!(
        module_status(&s, &b).await,
        StatusCode::NOT_FOUND,
        "{what}: nothing stored"
    );
    for wf in ["rs_conf_patch", "rs_conf_ndet", "rs_conf_signals"] {
        assert_eq!(binding(&s, wf).await, (a.clone(), Some(1)), "{what}: {wf}");
    }
    let r2 = start(&s, "rs_conf_patch").await;
    assert_eq!(pin(&s, &r2).await, a, "{what}");
    assert_eq!(
        completed(&s, &r2).await,
        json!(["a", "b"]),
        "{what}: r2 ran A"
    );
    signal_go(&s, &r1).await;
    assert_eq!(completed(&s, &r1).await, json!({ "got": ["ok", "ok"] }));
    assert_eq!(pin(&s, &r1).await, a, "{what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_dev_keeps_write_once_versions() {
    write_once(persistent_dev().await, "persistent dev").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_profile_keeps_write_once_versions() {
    write_once(server_profile(false).await, "server profile").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_profile_with_node_ephemeral_keeps_write_once_versions() {
    write_once(
        server_profile(true).await,
        "server profile + node.ephemeral",
    )
    .await;
}
