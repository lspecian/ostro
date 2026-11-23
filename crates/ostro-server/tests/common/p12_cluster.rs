//! Phase 12 server test helpers (12-14): single-node and static multi-node configurations
//! over real loopback mTLS (a shared test CA written to files, `[[cluster.peers]]` on
//! pre-reserved loopback ports). Included with `#[path]`, never through `common/mod.rs`.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use ostro_server::config::{FriendlyDuration, PeerConfig, ServerConfig};
use ostro_server::tls::ca::ClusterCa;
use ostro_server::{ServerError, ServerHandle};

/// Bound on one server start.
pub const START_T: Duration = Duration::from_secs(120);

/// A fresh test dir (tmpfs when available).
pub fn tmp() -> tempfile::TempDir {
    let shm = Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-p12-")
            .tempdir_in(shm)
    {
        return d;
    }
    tempfile::tempdir().expect("tempdir")
}

/// A single-node server configuration on loopback ephemeral ports.
pub fn single(dir: &Path, partitions: u16) -> ServerConfig {
    let mut cfg = ServerConfig::server_defaults();
    cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
    cfg.cluster.listen = "127.0.0.1:0".parse().expect("addr");
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.partitions = partitions;
    cfg
}

/// A loopback port nobody listens on right now (reserved by bind-and-drop).
pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Write a shared CA for `cluster_id` under `dir` and return `(ca.crt, ca.key)`.
pub fn shared_ca(dir: &Path, cluster_id: &str) -> (PathBuf, PathBuf) {
    let ca = ClusterCa::load_or_create(dir, cluster_id).expect("test CA");
    drop(ca);
    (dir.join("ca.crt"), dir.join("ca.key"))
}

/// The configurations of an `n`-node static cluster under `root` (`root/node<i>`), each with
/// the shared CA, `[[cluster.peers]]` on reserved loopback ports and short timings.
pub fn static_cluster(root: &Path, n: u64, partitions: u16, cluster_id: &str) -> Vec<ServerConfig> {
    let (crt, key) = shared_ca(&root.join("pki"), cluster_id);
    let ports: Vec<u16> = (0..n).map(|_| free_port()).collect();
    (1..=n)
        .map(|id| {
            let mut cfg = single(&root.join(format!("node{id}")), partitions);
            cfg.node.node_id = id;
            cfg.cluster.listen = SocketAddr::from(([127, 0, 0, 1], ports[(id - 1) as usize]));
            cfg.cluster.cluster_id = Some(cluster_id.to_owned());
            cfg.cluster.tls.ca_cert_file = Some(crt.clone());
            cfg.cluster.tls.ca_key_file = Some(key.clone());
            cfg.cluster.peers = (1..=n)
                .filter(|p| *p != id)
                .map(|p| PeerConfig {
                    id: p,
                    addr: format!("127.0.0.1:{}", ports[(p - 1) as usize]),
                    repl_addr: None,
                })
                .collect();
            let r = &mut cfg.replication;
            r.session_timeout = FriendlyDuration::from_millis(1500);
            r.replica_lag_max = FriendlyDuration::from_secs(3);
            r.meta_election_tick = 6;
            r.fetch_max_wait = FriendlyDuration::from_millis(100);
            cfg
        })
        .collect()
}

/// Start one server within [`START_T`].
pub async fn start(cfg: ServerConfig) -> Result<ServerHandle, ServerError> {
    tokio::time::timeout(START_T, ostro_server::Server::start(cfg))
        .await
        .expect("server start timed out")
}

/// Start every configuration concurrently (a static cluster forms only when its voters run).
pub async fn start_all(cfgs: &[ServerConfig]) -> Vec<ServerHandle> {
    let mut joins = Vec::new();
    for c in cfgs {
        joins.push(tokio::spawn(start(c.clone())));
    }
    let mut out = Vec::new();
    for j in joins {
        out.push(j.await.expect("join").expect("server start"));
    }
    out
}

/// Stop every server.
pub async fn stop_all(hs: Vec<ServerHandle>) {
    for h in hs {
        h.shutdown().await.expect("shutdown");
    }
}

/// Poll `f` every 50 ms until it holds or `limit` passes.
pub async fn eventually(limit: Duration, mut f: impl FnMut() -> bool) -> bool {
    let end = std::time::Instant::now() + limit; // clock-exempt: test deadline
    loop {
        if f() {
            return true;
        }
        if std::time::Instant::now() >= end {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// `node.toml` of `dir`.
pub fn node_toml(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("node.toml")).unwrap_or_default()
}

/// Every partition's `(epoch, leader, replicas, isr)` as `h`'s meta sees it.
/// `(partition, epoch, leader, replicas, isr)`.
pub type Row = (u16, u32, u64, Vec<u64>, Vec<u64>);

pub fn assignments(h: &ServerHandle) -> Vec<Row> {
    h.meta().view().read(|m| {
        m.partitions()
            .map(|a| {
                let v = &a.value;
                (
                    v.partition,
                    v.epoch,
                    v.leader,
                    v.replicas.clone(),
                    v.isr.clone(),
                )
            })
            .collect()
    })
}

/// The election reason (ext tag 80) of partition `p`'s record, if any.
pub fn election_reason(h: &ServerHandle, p: u16) -> Option<u8> {
    h.meta().view().read(|m| {
        m.partition(p)
            .and_then(|a| {
                ostro_meta::ext::get_election_info(&a.value.ext)
                    .ok()
                    .flatten()
            })
            .map(|i| i.reason)
    })
}

/// Build the `guests/` workspace once per process and return the named guest's `.wasm`.
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILT: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = std::process::Command::new(cargo)
            .args([
                "build",
                "--release",
                "--target",
                "wasm32-wasip2",
                "--manifest-path",
            ])
            .arg(root.join("guests/Cargo.toml"))
            .arg("--target-dir")
            .arg(&target_dir)
            .env_remove("RUSTFLAGS")
            .env_remove("CARGO_ENCODED_RUSTFLAGS")
            .env_remove("CARGO_BUILD_TARGET")
            .env_remove("CARGO_TARGET_DIR")
            .env_remove("RUSTC_WRAPPER")
            .output()
            .expect("spawn cargo for the guests build");
        assert!(
            out.status.success(),
            "guests build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    });
    let path = target_dir
        .join("wasm32-wasip2/release")
        .join(format!("{}.wasm", name.replace('-', "_")));
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Lowercase hex.
pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
