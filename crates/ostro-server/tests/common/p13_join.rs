//! Phase 13 join test helpers (13-09): a seed node over real loopback mTLS, node join
//! tokens through the REST API, `ostro cluster join` (`run_join --no-start`) and booting the
//! prepared data dir. Included with `#[path]` next to `p12_cluster.rs`.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use ostro_api::phase13::NodeTokenResponse;
use ostro_server::ServerHandle;
use ostro_server::config::{FriendlyDuration, ServerConfig};
use ostro_server::p13::{JoinArgs, JoinPrepared};
use serde_json::{Value, json};

use super::p12::{free_port, single};

pub const CLUSTER: &str = "p13-join";
pub const PARTS: u16 = 2;

/// Short Phase 12 timings for every node of these tests.
pub fn timings(cfg: &mut ServerConfig) {
    let r = &mut cfg.replication;
    r.session_timeout = FriendlyDuration::from_millis(1500);
    r.replica_lag_max = FriendlyDuration::from_secs(3);
    r.meta_election_tick = 6;
    r.fetch_max_wait = FriendlyDuration::from_millis(100);
    cfg.cluster.join_timeout = FriendlyDuration::from_secs(30);
}

/// A single seed node (`node1`, own CA) on a fixed loopback cluster port.
pub fn seed_cfg(root: &Path) -> ServerConfig {
    let mut cfg = single(&root.join("node1"), PARTS);
    cfg.node.node_id = 1;
    cfg.cluster.cluster_id = Some(CLUSTER.to_owned());
    cfg.cluster.listen = SocketAddr::from(([127, 0, 0, 1], free_port()));
    timings(&mut cfg);
    cfg
}

pub fn api(h: &ServerHandle) -> String {
    format!("http://{}", h.local_addr())
}

pub async fn post(h: &ServerHandle, path: &str, body: Value) -> (u16, Value) {
    let r = reqwest::Client::new()
        .post(format!("{}{path}", api(h)))
        .json(&body)
        .send()
        .await
        .expect("request");
    let st = r.status().as_u16();
    (st, r.json().await.unwrap_or(Value::Null))
}

pub async fn get(h: &ServerHandle, path: &str) -> (u16, Value) {
    let r = reqwest::Client::new()
        .get(format!("{}{path}", api(h)))
        .send()
        .await
        .expect("request");
    let st = r.status().as_u16();
    (st, r.json().await.unwrap_or(Value::Null))
}

/// A node join token from `h`.
pub async fn token(h: &ServerHandle, ttl_secs: Option<u64>) -> NodeTokenResponse {
    let (st, v) = post(h, "/v1/cluster/tokens", json!({ "ttl_secs": ttl_secs })).await;
    assert!((200..300).contains(&st), "token: {st} {v}");
    serde_json::from_value(v).expect("token response")
}

/// A joining node's dir and addresses.
#[derive(Clone, Debug)]
pub struct Joiner {
    pub data_dir: PathBuf,
    pub api: SocketAddr,
    pub cluster: SocketAddr,
}

pub fn joiner(root: &Path, k: u64) -> Joiner {
    Joiner {
        data_dir: root.join(format!("join{k}")),
        api: SocketAddr::from(([127, 0, 0, 1], free_port())),
        cluster: SocketAddr::from(([127, 0, 0, 1], free_port())),
    }
}

pub fn join_args(seed: &ServerHandle, j: &Joiner, t: &NodeTokenResponse) -> JoinArgs {
    JoinArgs {
        seed: api(seed),
        token: t.token.clone(),
        ca_hash: t.ca_hash.clone(),
        data_dir: Some(j.data_dir.clone()),
        config: None,
        advertise_api: Some(j.api.to_string()),
        advertise_cluster: Some(j.cluster.to_string()),
        advertise_repl: None,
        no_start: true,
    }
}

/// `ostro cluster join --no-start` through `seed`.
pub async fn join(seed: &ServerHandle, j: &Joiner) -> Result<JoinPrepared, String> {
    let t = token(seed, None).await;
    ostro_server::p13::joiner::run_join(join_args(seed, j, &t)).await
}

/// The configuration a joined node boots with (no peers, no CA files).
pub fn joined_cfg(j: &Joiner, node_id: u64) -> ServerConfig {
    let mut cfg = single(&j.data_dir, PARTS);
    cfg.node.node_id = node_id;
    cfg.api.listen = j.api;
    cfg.cluster.listen = j.cluster;
    cfg.cluster.cluster_id = Some(CLUSTER.to_owned());
    timings(&mut cfg);
    cfg
}

/// The member `id` in a status document.
pub fn member(st: &Value, id: u64) -> Option<&Value> {
    st["members"]
        .as_array()?
        .iter()
        .find(|m| m["node_id"].as_u64() == Some(id))
}

/// Poll `GET /v1/cluster/status` on `h` until `f` holds (bounded).
pub async fn status_until(h: &ServerHandle, limit: Duration, f: impl Fn(&Value) -> bool) -> Value {
    let end = std::time::Instant::now() + limit; // clock-exempt: test deadline
    let mut last = Value::Null;
    loop {
        let (st, v) = get(h, "/v1/cluster/status").await;
        if st == 200 {
            if f(&v) {
                return v;
            }
            last = v;
        }
        if std::time::Instant::now() >= end {
            panic!("status condition not reached within {limit:?}; last status: {last}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The committed InitCluster entries `(index, node_id, cluster_id)` of `data_dir`'s meta.
pub fn init_clusters(data_dir: &Path) -> Vec<(u64, u64, String)> {
    ostro_cluster::identity::probe_members(&data_dir.join("meta"))
        .expect("probe")
        .init_clusters
}
