//! The module upload gate (13-14; ENG-10, D-P13-17, P13-F1 rule 1).
//!
//! - `gate_refuses_above_finalized_and_writes_ext_101`: [`upload_gate`] over a real meta
//!   group. A level-2 selection is `409 feature_not_enabled` at finalized 1 and accepted with
//!   ext 101 at finalized 2; a baseline selection (`{0.1, 1}`) is accepted at both and never
//!   carries ext 101.
//! - `server_upload_selection_and_validation`: the real upload path. The 13-07 fixture
//!   guests: `p13-wit011-lite` (imports `@0.1.1`, only 0.1.0 functions) is a level-1 module,
//!   accepted on a node finalized at 1 and on one finalized at 2, with no ext 101;
//!   `p13-wit-mixed` (two WIT lines) and `p13-wit011` (a function this build does not link)
//!   are `400`; a re-upload is unchanged.
//!
//! This build links no function above level 1 (`FUNCS_0_1_ADDED` is empty outside the
//! engine's `wit-next-fixture` feature, and the server constructs its runtime with the
//! production linker), so a real level-2 module cannot pass validation here: the 409 path
//! is exercised through the gate with the selection a level-2 module produces.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::{ServerOpts, TestServer, guest_wasm, server_config, tmpfs_dir};
use ostro_api::ErrorCode;
use ostro_engine::host::linker_set::{WitLine, WitSelection};
use ostro_meta::ext::{ClusterVersion, get_wit_requirement, put_cluster_version};
use ostro_meta::{ClusterInfo, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult};
use ostro_server::control::modules::{REASON_FEATURE_NOT_ENABLED, upload_gate};
use reqwest::StatusCode;
use serde_json::Value;
use sha2::{Digest, Sha256};

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

async fn set_finalized(meta: &MetaHandle, finalized: u32) {
    let cur = meta
        .view()
        .read(|s| s.cluster().cloned())
        .expect("cluster info");
    let mut info: ClusterInfo = cur.value;
    info.ext = put_cluster_version(
        &info.ext,
        &ClusterVersion {
            finalized,
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
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
}

fn sel(line: WitLine, min_level: u32) -> WitSelection {
    WitSelection { line, min_level }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gate_refuses_above_finalized_and_writes_ext_101() {
    let dir = tmpfs_dir();
    let meta = MetaHandle::start(&dir.path().join("meta"), 1)
        .await
        .expect("meta start");
    meta.init_cluster("0190f5c2-7d3e-7000-8000-0000000000d1", 1, 1, 1, [7; 32])
        .await
        .expect("init");
    set_finalized(&meta, 1).await;
    let view = meta.view();

    // finalized 1: a level-2 module is refused, with the operator's next step.
    let e = upload_gate(view, &sel(WitLine::V0_1, 2)).expect_err("refused at finalized 1");
    assert_eq!(e.code, ErrorCode::Conflict);
    assert_eq!(e.code.http_status(), StatusCode::CONFLICT);
    assert!(
        e.message
            .starts_with("module needs cluster level 2; finalize the upgrade first"),
        "{}",
        e.message
    );
    let d = e.details.expect("details");
    assert_eq!(d["reason"], REASON_FEATURE_NOT_ENABLED);
    assert_eq!(d["needed"], 2);
    assert_eq!(d["finalized"], 1);
    assert_eq!(d["wit_line"], "0.1");
    // A 0.2-line module needs level 2 as well.
    assert!(upload_gate(view, &sel(WitLine::V0_2, 2)).is_err());
    // The baseline is accepted, without ext 101.
    assert_eq!(
        upload_gate(view, &sel(WitLine::V0_1, 1)).expect("level 1"),
        Vec::<u8>::new()
    );

    // finalized 2: accepted, and the record carries ext 101.
    set_finalized(&meta, 2).await;
    let ext = upload_gate(view, &sel(WitLine::V0_1, 2)).expect("accepted at finalized 2");
    let req = get_wit_requirement(&ext).expect("decode").expect("ext 101");
    assert_eq!((req.line.as_str(), req.min_level), ("0.1", 2));
    let ext = upload_gate(view, &sel(WitLine::V0_2, 2)).expect("0.2 at finalized 2");
    let req = get_wit_requirement(&ext).expect("decode").expect("ext 101");
    assert_eq!((req.line.as_str(), req.min_level), ("0.2", 2));
    assert_eq!(
        upload_gate(view, &sel(WitLine::V0_1, 1)).expect("level 1"),
        Vec::<u8>::new()
    );
    // Above every level: refused.
    let e = upload_gate(view, &sel(WitLine::V0_1, 3)).expect_err("level 3");
    assert!(
        e.message.contains("module needs cluster level 3"),
        "{}",
        e.message
    );
}

async fn put_module(s: &TestServer, body: &[u8]) -> (StatusCode, Value) {
    let r = s
        .http
        .put(s.url(&format!("/v1/modules/{}?language=rust", sha_hex(body))))
        .header("content-type", "application/wasm")
        .body(body.to_vec())
        .send()
        .await
        .expect("PUT module");
    let st = r.status();
    (st, r.json().await.expect("json body"))
}

/// The upload checks on one running node; returns the lite module's Module record ext.
async fn upload_checks(s: &TestServer) -> Vec<u8> {
    let lite = guest_wasm("p13-wit011-lite");
    let (st, body) = put_module(s, &lite).await;
    assert!(st.is_success(), "lite: {st} {body}");
    assert_eq!(body["created"], true, "{body}");
    let (st, body) = put_module(s, &lite).await;
    assert!(st.is_success(), "lite again: {st} {body}");
    assert_eq!(body["created"], false, "{body}");

    let (st, body) = put_module(s, &guest_wasm("p13-wit-mixed")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    let msg = body.to_string();
    assert!(msg.contains("0.1") && msg.contains("0.2"), "{msg}");

    let (st, body) = put_module(s, &guest_wasm("p13-wit011")).await;
    assert_eq!(st, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.to_string().contains("note-hint"), "{body}");

    let sha: [u8; 32] = Sha256::digest(&lite).into();
    s.h()
        .meta()
        .view()
        .read(|st| st.module(&sha).map(|m| m.value.ext.clone()))
        .expect("module record")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_upload_selection_and_validation() {
    // A node finalized at 2 (a fresh node at this build's level).
    let mut s = TestServer::start(ServerOpts::default()).await;
    s.wait_ready(Duration::from_secs(30)).await;
    // A brand-new node starts finalized at its level (p14-fixups); the wait stays as a bound.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while s
        .h()
        .meta()
        .view()
        .read(ostro_cluster::compat::finalized_level)
        < 2
    {
        assert!(
            std::time::Instant::now() < deadline,
            "not finalized at 2 within 30 s"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ext = upload_checks(&s).await;
    assert_eq!(
        get_wit_requirement(&ext).expect("decode"),
        None,
        "level 1: no ext 101"
    );
    s.shutdown().await;

    // A node finalized at 1 (an emulated level-1 binary).
    let dir = tmpfs_dir();
    let cfg = server_config(dir.path(), &ServerOpts::default());
    let h = tokio::time::timeout(
        common::T,
        ostro_server::server::start_with_options(
            cfg.clone(),
            ostro_server::ServerOptions::default().with_level_max_override(Some(1)),
        ),
    )
    .await
    .expect("start timed out")
    .expect("start");
    let mut s = TestServer {
        handle: Some(h),
        cfg,
        hooks: std::sync::Arc::default(),
        http: reqwest::Client::new(),
        dir: Some(dir),
    };
    s.wait_ready(Duration::from_secs(30)).await;
    assert_eq!(
        s.h()
            .meta()
            .view()
            .read(ostro_cluster::compat::finalized_level),
        1
    );
    let ext = upload_checks(&s).await;
    assert_eq!(get_wit_requirement(&ext).expect("decode"), None);
    s.shutdown().await;
}
