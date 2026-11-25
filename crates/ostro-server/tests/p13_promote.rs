//! 13-16 Task 1: online promotion (`POST /v1/cluster/init`, D-P13-11) of a running single
//! server-profile node over real loopback mTLS.
//!
//! - A running node with runs in flight is promoted without a restart: same process, same
//!   partition leaders and epochs, own Member Active with the advertised cluster address,
//!   `__ostro/cluster-ca` stored, `ClusterVersion` and `ReplicationPolicy{3, 2, 5}` on the
//!   cluster record; the runs complete afterwards; a node joins through the real join flow.
//! - Re-init is idempotent (200 with the status, the policy unchanged).
//! - Refused in the dev profile (409 `not_supported_in_profile`) and without an advertised
//!   address (400).
//! - With `--features failpoints`: a crash right after the CA step (`member::init_after_ca`)
//!   leaves no policy; running init again completes it.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::time::Duration;

use ostro_meta::ext::{get_cluster_version, get_replication_policy};
use ostro_server::ServerHandle;
use ostro_server::p13::join::stored_ca;
use ostro_server::p13::promote::REASON_NOT_SUPPORTED_IN_PROFILE;
use p12::{assignments, eventually, guest_wasm, hex, start, stop_all, tmp};
use p13::*;
use serde_json::json;
use sha2::{Digest, Sha256};

const RUN_T: Duration = Duration::from_secs(60);

fn advertise(cfg: &ostro_server::config::ServerConfig) -> String {
    cfg.cluster.listen.to_string()
}

async fn put_module(h: &ServerHandle, wasm: &[u8]) {
    let sha = hex(&Sha256::digest(wasm));
    let r = reqwest::Client::new()
        .put(format!("{}/v1/modules/{sha}", api(h)))
        .body(wasm.to_vec())
        .send()
        .await
        .expect("PUT module");
    assert!(r.status().is_success(), "upload: {}", r.status());
    let r = reqwest::Client::new()
        .put(format!("{}/v1/workflows/signals/active", api(h)))
        .json(&json!({ "version": 1 }))
        .send()
        .await
        .expect("PUT active");
    assert!(r.status().is_success(), "activate: {}", r.status());
}

async fn start_wait(h: &ServerHandle, i: usize) -> String {
    let (st, v) = post(
        h,
        "/v1/workflows/signals/runs",
        json!({ "input": { "mode": "long_wait" }, "idempotency_key": format!("promote-{i}") }),
    )
    .await;
    assert!((200..300).contains(&st), "start: {st} {v}");
    v["run_id"].as_str().expect("run_id").to_owned()
}

async fn run_status(h: &ServerHandle, run: &str) -> String {
    let (_, v) = get(h, &format!("/v1/runs/{run}")).await;
    v["status"].as_str().unwrap_or_default().to_owned()
}

fn policy(h: &ServerHandle) -> Option<(u8, u8, u8)> {
    h.meta().view().read(|s| {
        s.cluster()
            .and_then(|c| get_replication_policy(&c.value.ext).ok().flatten())
            .map(|p| (p.rf, p.min_insync, p.max_voters))
    })
}

fn finalized(h: &ServerHandle) -> Option<u32> {
    h.meta().view().read(|s| {
        s.cluster()
            .and_then(|c| get_cluster_version(&c.value.ext).ok().flatten())
            .map(|v| v.finalized)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn init_promotes_a_running_single_node_without_restart() {
    let root = tmp();
    let cfg = seed_cfg(root.path());
    let adv = advertise(&cfg);
    let seed = start(cfg).await.expect("seed");
    assert!(
        eventually(Duration::from_secs(30), || assignments(&seed)
            .iter()
            .all(|r| r.2 == 1))
        .await,
        "every partition led by node 1"
    );
    put_module(&seed, &guest_wasm("signals")).await;
    let mut runs = Vec::new();
    for i in 0..4 {
        runs.push(start_wait(&seed, i).await);
    }
    for r in &runs {
        let s = run_status(&seed, r).await;
        assert!(
            !["completed", "failed", "cancelled", "terminated"].contains(&s.as_str()),
            "{r} waits for its signal: {s}"
        );
    }
    let before = assignments(&seed);
    assert_eq!(policy(&seed), None, "no policy before init");

    let t0 = std::time::Instant::now(); // clock-exempt: test timing
    let (st, v) = post(&seed, "/v1/cluster/init", json!({ "advertise": adv })).await;
    let took = t0.elapsed();
    assert_eq!(st, 200, "init: {v}");
    println!("cluster init answered in {took:?}");
    assert_eq!(
        v["policy"],
        json!({ "rf": 3, "min_insync": 2, "max_voters": 5 })
    );
    let me = member(&v, 1).expect("member 1");
    assert_eq!(me["state"], "active", "{v}");
    assert_eq!(me["cluster_addr"], adv.as_str(), "{v}");
    assert_eq!(me["version"]["level_max"], json!(2), "{v}");
    assert_eq!(v["cluster_id"], CLUSTER, "the resolved cluster id is kept");
    assert_eq!(v["voters"], json!([1]));
    assert_eq!(policy(&seed), Some((3, 2, 5)));
    assert_eq!(finalized(&seed), Some(2));
    assert!(seed.meta().view().read(stored_ca).is_some(), "CA stored");
    // No restart, no leadership change, no new epoch.
    assert_eq!(assignments(&seed), before, "partitions untouched by init");

    // The runs in flight complete.
    for r in &runs {
        let (st, v) = post(
            &seed,
            &format!("/v1/runs/{r}/signals/x"),
            json!({ "payload": "go" }),
        )
        .await;
        assert!((200..300).contains(&st), "signal: {st} {v}");
    }
    for r in &runs {
        let end = std::time::Instant::now() + RUN_T; // clock-exempt: test deadline
        loop {
            let s = run_status(&seed, r).await;
            if s == "completed" {
                break;
            }
            assert!(std::time::Instant::now() < end, "{r} ended {s}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Idempotent re-init: 200 with the status, the policy unchanged.
    let (st, v2) = post(&seed, "/v1/cluster/init", json!({ "rf": 5 })).await;
    assert_eq!(st, 200, "re-init: {v2}");
    assert_eq!(v2["policy"]["rf"], json!(3), "re-init changes nothing");

    // Joinable: a node joins through the real join flow and becomes Active.
    let j = joiner(root.path(), 2);
    let prepared = join(&seed, &j).await.expect("join");
    assert_eq!(prepared.cluster_id, CLUSTER);
    let node2 = start(joined_cfg(&j, prepared.node_id))
        .await
        .expect("joined node");
    status_until(&seed, Duration::from_secs(60), |st| {
        member(st, prepared.node_id).is_some_and(|m| m["state"] == "active")
    })
    .await;
    // Partitions stayed led by node 1 throughout (the balancer runs every 5 min by default).
    assert!(assignments(&seed).iter().all(|r| r.2 == 1));
    stop_all(vec![node2, seed]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_refused_in_dev_profile() {
    let root = tmp();
    let mut cfg = seed_cfg(root.path());
    // The dev profile is the one that auto-generates the sealing key (TlsProfile::of).
    cfg.secrets.auto_sealing_key = true;
    let adv = advertise(&cfg);
    let h = start(cfg).await.expect("dev node");
    let (st, v) = post(&h, "/v1/cluster/init", json!({ "advertise": adv })).await;
    assert_eq!(st, 409, "{v}");
    assert_eq!(
        v["error"]["details"]["reason"],
        REASON_NOT_SUPPORTED_IN_PROFILE
    );
    assert_eq!(policy(&h), None);
    stop_all(vec![h]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_needs_an_advertised_address_and_a_valid_policy() {
    let root = tmp();
    let cfg = seed_cfg(root.path());
    assert!(cfg.cluster.advertise.is_empty());
    let adv = advertise(&cfg);
    let h = start(cfg).await.expect("node");
    let (st, v) = post(&h, "/v1/cluster/init", json!({})).await;
    assert_eq!(st, 400, "{v}");
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("advertise"),
        "{v}"
    );
    let (st, v) = post(&h, "/v1/cluster/init", json!({ "advertise": adv, "rf": 2 })).await;
    assert_eq!(st, 400, "{v}");
    let (st, v) = post(
        &h,
        "/v1/cluster/init",
        json!({ "advertise": adv, "rf": 3, "min_insync": 4 }),
    )
    .await;
    assert_eq!(st, 400, "{v}");
    assert_eq!(policy(&h), None, "nothing written");
    let (st, v) = post(&h, "/v1/cluster/init", json!({ "advertise": adv, "rf": 1 })).await;
    assert_eq!(st, 200, "{v}");
    assert_eq!(policy(&h), Some((1, 1, 5)));
    stop_all(vec![h]).await;
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn init_crash_after_ca_then_rerun_completes() {
    use ostro_server::p13::promote::FP_INIT_AFTER_CA;
    let root = tmp();
    let cfg = seed_cfg(root.path());
    let adv = advertise(&cfg);
    let h = start(cfg).await.expect("node");
    fail::cfg(FP_INIT_AFTER_CA, "return").expect("failpoint");
    let (st, v) = post(&h, "/v1/cluster/init", json!({ "advertise": adv })).await;
    fail::remove(FP_INIT_AFTER_CA);
    assert_eq!(st, 503, "the failpoint stops init: {v}");
    // Steps 1-2 happened, the policy (written last) did not.
    assert!(h.meta().view().read(stored_ca).is_some(), "CA stored");
    let (_, s) = get(&h, "/v1/cluster/status").await;
    assert_eq!(member(&s, 1).expect("member")["state"], "active");
    assert_eq!(policy(&h), None, "not initialised yet");
    let (st, v) = post(&h, "/v1/cluster/init", json!({ "advertise": adv })).await;
    assert_eq!(st, 200, "re-run: {v}");
    assert_eq!(v["policy"]["rf"], json!(3));
    assert_eq!(policy(&h), Some((3, 2, 5)));
    assert_eq!(finalized(&h), Some(2));
    stop_all(vec![h]).await;
}
