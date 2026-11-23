//! 12-14 Task 2: the API of a cluster node (D-P12-21/22/9): partition-scoped calls on a
//! partition the node does not lead answer 503 with leader details; module uploads reach
//! enough peers before the `Module` record exists; `GET /v1/admin/replication` reports the
//! published ISR and measured follower progress (a node that never ran is never in it); a
//! node that lost meta contact for `session_timeout` self-demotes and answers 503.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::time::Duration;

use ostro_partition::PartitionRole;
use ostro_server::ServerHandle;
use ostro_types::PartitionId;
use p12::*;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const P: u16 = 3;

fn url(h: &ServerHandle, path: &str) -> String {
    format!("http://{}{path}", h.local_addr())
}

async fn status(http: &reqwest::Client, h: &ServerHandle) -> Value {
    http.get(url(h, "/v1/admin/replication"))
        .send()
        .await
        .expect("GET status")
        .json()
        .await
        .expect("status JSON")
}

fn has_blob(h: &ServerHandle, sha: &[u8; 32]) -> bool {
    h.node()
        .handle(PartitionId(0))
        .is_some_and(|x| ostro_cluster::blob::has_blob(x.blobs(), sha))
}

fn led_by(h: &ServerHandle) -> Vec<u16> {
    (0..P)
        .filter(|p| {
            matches!(
                h.node().role(PartitionId(*p)).map(|r| *r.borrow()),
                Some(PartitionRole::Leader { .. })
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn not_leader_503_module_push_status_and_self_demotion() {
    let root = tmp();
    let cfgs = static_cluster(root.path(), 3, P, "api-3");
    // Node 3 is configured but never runs: it must never appear in a published ISR.
    let hs = start_all(&cfgs[..2]).await;
    let http = reqwest::Client::new();

    // Placement is round-robin over the voters: p0 -> 1, p1 -> 2 are led with the other live
    // node in the ISR; p2 -> 3 never gets a live ISR member and goes offline (leader 0, the
    // ISR keeps its last member; no unclean election).
    assert!(
        eventually(Duration::from_secs(40), || {
            let a = assignments(&hs[0]);
            a.len() == usize::from(P)
                && a.iter().all(|(p, e, l, _, isr)| match p {
                    0 => *e >= 1 && *l == 1 && isr.len() == 2,
                    1 => *e >= 1 && *l == 2 && isr.len() == 2,
                    _ => *l == 0 && *isr == vec![3],
                })
                && led_by(&hs[0]) == vec![0]
                && led_by(&hs[1]) == vec![1]
        })
        .await,
        "{:?}",
        assignments(&hs[0])
    );

    // Module upload on node 1: the blob is on both live nodes once the call returns.
    let wasm = guest_wasm("smoke");
    let sha: [u8; 32] = Sha256::digest(&wasm).into();
    let r = http
        .put(url(&hs[0], &format!("/v1/modules/{}", hex(&sha))))
        .header("content-type", "application/wasm")
        .body(wasm)
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "{}", r.status());
    assert!(has_blob(&hs[0], &sha) && has_blob(&hs[1], &sha));

    // Runs started on node 1 land on random partitions: led ones are accepted, the others
    // answer 503 not_leader naming node 2.
    let (mut ok, mut not_leader) = (0, 0);
    for i in 0..40 {
        let r = http
            .post(url(&hs[0], "/v1/workflows/smoke/runs"))
            .json(&json!({ "input": { "i": i } }))
            .send()
            .await
            .expect("POST run");
        let st = r.status();
        let body: Value = r.json().await.unwrap_or(Value::Null);
        if st.is_success() {
            ok += 1;
        } else {
            assert_eq!(st, reqwest::StatusCode::SERVICE_UNAVAILABLE, "{body}");
            let d = &body["error"]["details"];
            let d = if d.is_null() { &body["details"] } else { d };
            assert_eq!(d["reason"], "not_leader", "{body}");
            match d["partition"].as_u64() {
                Some(1) => {
                    assert_eq!(d["leader"], 2, "{body}");
                    assert!(
                        d["leader_addr"].as_str().is_some_and(|a| !a.is_empty()),
                        "{body}"
                    );
                }
                Some(2) => assert!(d["leader"].is_null(), "offline partition: {body}"),
                other => panic!("partition {other:?} is led here: {body}"),
            }
            assert!(body.to_string().contains("leader"), "{body}");
            not_leader += 1;
        }
        if ok > 0 && not_leader > 0 {
            break;
        }
    }
    assert!(ok > 0 && not_leader > 0, "ok {ok} not_leader {not_leader}");

    // 12-16: no server component pins a follower partition's state store. Node 1 follows
    // partition 1; quiescing it for a snapshot install must succeed at once (a cached
    // handle in the control plane, the bridge table or a service task used to hold the
    // store until `quiesce_timeout`), and after the reopen the API looks up the new handle.
    let t = std::time::Instant::now();
    let guard = tokio::time::timeout(
        Duration::from_secs(5),
        hs[0].node().control().quiesce_for_install(PartitionId(1)),
    )
    .await
    .expect("quiesce does not wait for the store's users")
    .expect("a follower's store is not pinned by the server");
    assert!(t.elapsed() < Duration::from_secs(5), "{:?}", t.elapsed());
    assert!(hs[0].node().handle(PartitionId(1)).is_none(), "quiesced");
    guard.reopen().await.expect("reopen");
    assert!(
        eventually(Duration::from_secs(10), || {
            hs[0].node().handle(PartitionId(1)).is_some()
                && matches!(
                    hs[0].node().role(PartitionId(1)).map(|r| *r.borrow()),
                    Some(PartitionRole::Follower { .. })
                )
        })
        .await,
        "partition 1 follows again after the reopen"
    );

    // The status endpoint: published ISR (never node 3) and measured follower progress.
    let doc = status(&http, &hs[0]).await;
    println!("{}", serde_json::to_string_pretty(&doc).unwrap());
    assert_eq!(doc["node_id"], 1);
    let mut voters: Vec<u64> = doc["meta"]["voters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap())
        .collect();
    voters.sort_unstable();
    assert_eq!(voters, vec![1, 2, 3]);
    for part in doc["partitions"].as_array().unwrap() {
        let isr: Vec<u64> = part["published_isr"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap())
            .collect();
        assert!(!isr.contains(&3), "{part}");
        assert_eq!(part["isr"], part["published_isr"]);
        if part["role"] == "leader" {
            assert!(part["hw_publishes"].as_u64().unwrap() > 0, "{part}");
            let followers = part["followers"].as_array().unwrap();
            // Node 3 never fetched: no progress is ever attributed to it.
            assert!(
                followers
                    .iter()
                    .filter(|f| f["replica"] == 3)
                    .all(|f| f["durable_leo"] == 0 && f["fetch_offset"] == 0),
                "never fetched, nothing measured: {part}"
            );
            assert!(followers.iter().any(|f| f["replica"] == 2), "{part}");
        }
    }

    // Self-demotion: node 2 stops, node 1 loses meta contact (no quorum) and steps down.
    let mut hs = hs;
    let h2 = hs.pop().unwrap();
    h2.shutdown().await.unwrap();
    assert!(
        eventually(Duration::from_secs(15), || led_by(&hs[0]).is_empty()).await,
        "still leading {:?}",
        led_by(&hs[0])
    );
    let r = http
        .post(url(&hs[0], "/v1/workflows/smoke/runs"))
        .json(&json!({ "input": {} }))
        .send()
        .await
        .expect("POST run");
    assert_eq!(r.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    stop_all(hs).await;
}
