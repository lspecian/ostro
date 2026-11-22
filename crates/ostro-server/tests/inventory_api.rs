//! Inventory endpoints against the real server (plan 11-11, UI-03 / D-P11-19):
//! `GET /v1/modules` (newest first, paginated), `GET /v1/queues` (paused, ready / running
//! counts, demand, flow) and `GET /v1/partitions` (the fields the node exposes).
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p11.rs"]
mod p11;

use std::time::Duration;

use ostro_partition::store::values::RunStatus;
use p11::P11Server;
use reqwest::StatusCode;
use serde_json::{Value, json};

const LIMIT: Duration = Duration::from_secs(60);

fn hex(sha: &[u8; 32]) -> String {
    sha.iter().map(|b| format!("{b:02x}")).collect()
}

fn queue<'a>(v: &'a Value, name: &str) -> &'a Value {
    v["queues"]
        .as_array()
        .expect("queues")
        .iter()
        .find(|q| q["name"] == name)
        .unwrap_or_else(|| panic!("queue {name} in {v}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn modules_queues_and_partitions() {
    let mut s = P11Server::builder().partitions(4).start().await;
    s.wait_ready(Duration::from_secs(20)).await;

    // Modules: two uploads, newest first, one per page with limit=1.
    let basic = s.deploy_guest("sdk-basic", None).await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let native = s.deploy_guest("sdk-native", None).await;
    let (st, p1) = s.get_json("/v1/modules?limit=1").await;
    assert_eq!(st, StatusCode::OK, "{p1}");
    let m1 = &p1["modules"][0];
    assert_eq!(m1["sha256"], hex(&native), "{p1}");
    assert!(m1["size_bytes"].as_u64().is_some_and(|n| n > 0), "{p1}");
    assert!(m1["uploaded_at_ms"].as_u64().is_some(), "{p1}");
    let token = p1["next_page_token"].as_str().expect("token");
    let (st, p2) = s
        .get_json(&format!("/v1/modules?limit=1&page_token={token}"))
        .await;
    assert_eq!(st, StatusCode::OK, "{p2}");
    let m2 = &p2["modules"][0];
    assert_eq!(m2["sha256"], hex(&basic), "{p2}");
    assert_eq!(m2["workflows"], json!(["sdk-basic", "sdk-echo"]), "{p2}");
    assert!(m2["uploaded_at_ms"].as_u64() <= m1["uploaded_at_ms"].as_u64());
    assert!(p2.get("next_page_token").is_none(), "{p2}");
    let (st, all) = s.get_json("/v1/modules").await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(all["modules"].as_array().expect("modules").len(), 2);
    let (st, _) = s.get_json("/v1/modules?page_token=garbage").await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // Queues: completed runs on qa, one run held on the paused queue qp.
    let mut done = Vec::new();
    for i in 0..3 {
        done.push(
            s.start_rest("sdk-echo", json!({"input": {"i": i}, "queue": "qa"}))
                .await,
        );
    }
    for r in &done {
        s.wait_record(r, LIMIT, |r| r.status == RunStatus::Completed)
            .await;
    }
    let (st, v) = s.post_json("/v1/queues/qp/pause", &json!({})).await;
    assert!(st.is_success(), "{st} {v}");
    let held = s
        .start_rest("sdk-echo", json!({"input": {"held": true}, "queue": "qp"}))
        .await;
    s.wait_record(&held, LIMIT, |r| r.status == RunStatus::Ready)
        .await;

    let (st, v) = s.get_json("/v1/queues").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let names: Vec<&str> = v["queues"]
        .as_array()
        .expect("queues")
        .iter()
        .map(|q| q["name"].as_str().expect("name"))
        .collect();
    let mut sorted = names.clone();
    sorted.sort_unstable();
    assert_eq!(names, sorted, "sorted by name");
    assert!(names.contains(&"default"), "{v}");
    let qa = queue(&v, "qa");
    assert_eq!(qa["paused"], false, "{v}");
    assert_eq!(qa["ready"], 0, "{v}");
    assert_eq!(qa["running"], 0, "{v}");
    let qp = queue(&v, "qp");
    assert_eq!(qp["paused"], true, "{v}");
    assert_eq!(qp["ready"], 1, "{v}");
    assert_eq!(qp["running"], 0, "{v}");
    assert_eq!(
        qp["flow"]["paused"], true,
        "flow from the queue policy: {v}"
    );
    assert!(qp["demand"].as_u64().is_some(), "Phase 7 demand: {v}");

    // Partitions: one per slot, the node's fields.
    let (st, v) = s.get_json("/v1/partitions").await;
    assert_eq!(st, StatusCode::OK, "{v}");
    let parts = v["partitions"].as_array().expect("partitions");
    assert_eq!(parts.len(), 4, "{v}");
    let mut ready_total = 0;
    for (i, p) in parts.iter().enumerate() {
        assert_eq!(p["id"], i, "{p}");
        assert_eq!(p["role"], "leader", "{p}");
        assert_eq!(p["leader"], s.cfg.node.node_id, "{p}");
        assert_eq!(p["healthy"], true, "{p}");
        assert!(p.get("detail").is_none(), "{p}");
        assert!(p["epoch"].as_u64().is_some(), "{p}");
        let applied = p["applied_offset"].as_u64().expect("applied");
        let end = p["log_end_offset"].as_u64().expect("log end");
        assert!(applied <= end, "{p}");
        assert_eq!(p["running"], 0, "{p}");
        ready_total += p["ready"].as_u64().expect("ready");
    }
    assert_eq!(ready_total, 1, "the held run: {v}");
    let held_p = held.partition().0 as usize;
    assert_eq!(parts[held_p]["ready"], 1, "{v}");
    assert!(parts[held_p]["applied_offset"].as_u64() > Some(0), "{v}");

    s.shutdown().await;
}
