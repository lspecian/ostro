//! Tier C (12-09, D-P12-18): N real `ostro server` processes on loopback, connected through
//! a [`ChaosProxy`], with real `kill -9` and restarts from the retained data dirs. An
//! in-process "crash" keeps the page cache and the threads (research Pitfall 8); SC2 and the
//! failover p99 (12-17) need real process death.
//!
//! **Binary.** The failpoint build of `ostro` ([`failpoint_bin`]): `OSTRO_FAILPOINT_BIN`, else
//! `target/failpoints/debug/ostro` (`cargo build -p ostro --features failpoints --target-dir
//! target/failpoints`). The harness only invokes cargo itself when `OSTRO_PROC_BUILD=1`
//! ([`ensure_failpoint_bin`], serialized by a file lock), never during a plain `cargo test`.
//!
//! **Addresses.** Each cluster picks a random `r` in `2..=250` and puts node `i` on
//! `127.r.r.i` (Linux routes all of 127/8 on `lo`, no root needed), so parallel tests never
//! collide. On that IP the node runs its API on port 0 at its first start (the bound port
//! comes from the ready line and is then pinned in its config, so restarts keep the address)
//! and its cluster listener on [`CLUSTER_PORT`]. The proxy listener of the directed link
//! `from → to` is `127.r.r.<to>:<PROXY_PORT_BASE + from>`: it sits on the *target's* IP, so
//! the TLS name check of a client dialing it sees the address the target's certificate is
//! issued for, and A's view of B is a port of its own. `[[cluster.peers]] addr` of node A
//! for peer B is that proxy listener. Where `127.r.r.x` cannot be bound (macOS without
//! aliases), [`loopback_aliases_supported`] is false and [`ProcCluster::start`] fails with a
//! reason; tests skip with a printed `SKIP:` line.
//!
//! **Configuration.** The same node configuration as tier B (`[node] node_id`, partitions,
//! `[cluster] cluster_id`, peers, `[cluster.tls] ca_cert_file / ca_key_file` from
//! [`TestPki`] for multi-node specs, `[replication]` short timings, `rf` / `min_insync`, the
//! shared `[backup] url` with `spec.objstore`), rendered by `ServerConfig::to_toml_string`
//! into `<root>/node<i>.toml` and started as `ostro server -c <file>`. Inherited `OSTRO_*`
//! and `RUST_LOG` variables are stripped; `FAILPOINTS` comes from [`ProcOptions`] or
//! [`ProcCluster::set_failpoints`] (effective at the next (re)start).
//!
//! **Lifecycle.** `kill(n)` is `SIGKILL` + `waitpid` (the wait status must say signal 9);
//! `restart(n)` heals n's links and respawns from the same config and data dir. stdout and
//! stderr go to `<root>/node<i>.log` (appended across restarts). `stop()` kills every child
//! and removes the root; a drop while panicking keeps the root and prints its path.
//!
//! Since 12-14 the processes of a multi-node spec form one cluster (each node advertises its
//! `127.r.r.i`, which its certificate names); a starting node waits for a metadata leader.
//!
//! **Phase 13 (13-08, D-P13-25).** [`ProcCluster`] implements [`P13Harness`].
//! `start_single` is one node on `127.r.r.1` (no peers, `[cluster] advertise = [ip]`, the
//! test CA imported). `add_node` takes a node join token from the lowest running node and
//! spawns, on the next free `127.r.r.k`,
//! `ostro cluster join http://<seed api> --token <njt_…> --ca-hash sha256:<hex> --data
//! <root>/join<k> -c <root>/join<k>.toml [--advertise-api <ip:port> --advertise-cluster
//! <ip:17701>]`; the joiner starts the server after the handshake, its ready line gives the
//! API address and its `joined cluster <id> as node <n>` line the allocated id. Chaos-proxy
//! links `n → m` / `m → n` (the Phase 12 scheme `127.r.r.<to>:<PROXY_PORT_BASE + from>`) are
//! registered for the joined node with every existing node, so `isolate` / `heal` /
//! `partition` / `kill` take its id like any other. Restarts of a joined node run
//! `ostro server -c <its config>`. `upgrade(node, Some(k))` stops the process gracefully
//! (SIGTERM, then SIGKILL after [`STOP_T`]) and respawns it with `OSTRO_TEST_LEVEL_MAX=k`
//! (`None` removes the variable).
//!
//! **13-19 (tier-C Phase 13 tests).** [`ProcOptions::level_max`] starts every node of a
//! static (Phase 12, `[[cluster.peers]]` through the proxy) cluster at an emulated level, so
//! a rolling upgrade can run with chaos-proxy faults on every link (joined nodes dial their
//! peers directly: 13-18 handover). [`ProcCluster::stop_gracefully`] is the SIGTERM
//! (controlled shutdown) stop on its own; [`ProcOptions::bin`] / [`ProcCluster::set_bin`]
//! start nodes from another `ostro` binary (the previous release in `upgrade_from_prev`).

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{OnceLock, mpsc};
use std::time::{Duration, Instant};

use ostro_partition::roles::PartitionAssignmentView;
use ostro_server::config::{FriendlyDuration, PeerConfig, ReplicationSection, ServerConfig};
use ostro_types::{Epoch, PartitionId};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::Value;
use tempfile::TempDir;

use super::certs::TestPki;
use super::chaos_proxy::{ChaosProxy, Link};
use super::inproc::{
    MEMBERSHIP_T, free_addr, node_token, p13_get, p13_post, p13_post_membership, read_meta_log,
    remove_and_wait,
};
use super::p13::{NodeOpts, P13Harness};
use super::{ApiClient, ClusterHarness, ClusterSpec, DataDirView, HarnessError, MetaLogEntry};

/// Every node's cluster listener port (on its own `127.r.r.i`).
pub const CLUSTER_PORT: u16 = 17701;
/// Proxy listener of `from → to`: `127.r.r.<to>:<PROXY_PORT_BASE + from>`.
pub const PROXY_PORT_BASE: u16 = 17800;
/// Bound on one process start (ready line).
pub const PROC_START_T: Duration = Duration::from_secs(120);
/// The binary the harness looks for when `OSTRO_FAILPOINT_BIN` is unset (repo-relative).
pub const DEFAULT_BIN: &str = "target/failpoints/debug/ostro";
/// Bound on a graceful (SIGTERM) stop before [`ProcCluster::upgrade`] falls back to SIGKILL.
pub const STOP_T: Duration = Duration::from_secs(60);
/// The skip line of tests gated on the failpoint binary (W14).
pub const SKIP_NO_BIN: &str = "SKIP: failpoint ostro binary not built (cargo build -p ostro \
                               --features failpoints --target-dir target/failpoints)";

fn start_err(e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Start(e.to_string())
}

/// Repository root (`crates/ostro-repl-conformance/../..`).
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("repo root")
        .to_path_buf()
}

/// The failpoint `ostro` binary if it exists: `OSTRO_FAILPOINT_BIN`, else [`DEFAULT_BIN`].
/// Never builds anything.
pub fn failpoint_bin() -> Option<PathBuf> {
    match std::env::var_os("OSTRO_FAILPOINT_BIN") {
        Some(b) if !b.is_empty() => {
            let p = PathBuf::from(b);
            p.is_file().then_some(p)
        }
        _ => {
            let p = repo_root().join(DEFAULT_BIN);
            p.is_file().then_some(p)
        }
    }
}

/// [`failpoint_bin`], building it first when it is missing and `OSTRO_PROC_BUILD=1`
/// (`cargo build -p ostro --features failpoints --target-dir target/failpoints --locked`,
/// serialized across test processes by a lock file). Also checks once per process that
/// `build-info` reports `failpoints=on`.
pub fn ensure_failpoint_bin() -> Result<PathBuf, HarnessError> {
    static CHECKED: OnceLock<Result<PathBuf, String>> = OnceLock::new();
    CHECKED
        .get_or_init(|| {
            let bin = match failpoint_bin() {
                Some(b) => b,
                None if std::env::var("OSTRO_PROC_BUILD").as_deref() == Ok("1") => build_bin()?,
                None => return Err(SKIP_NO_BIN.trim_start_matches("SKIP: ").to_owned()),
            };
            let out = Command::new(&bin)
                .arg("build-info")
                .env_remove("FAILPOINTS")
                .output()
                .map_err(|e| format!("run {} build-info: {e}", bin.display()))?;
            let info = String::from_utf8_lossy(&out.stdout);
            if !info.contains("failpoints=on") {
                return Err(format!(
                    "{} is not a failpoint build: {info}",
                    bin.display()
                ));
            }
            Ok(bin)
        })
        .clone()
        .map_err(HarnessError::Start)
}

fn build_bin() -> Result<PathBuf, String> {
    let root = repo_root();
    let lock_dir = root.join("target");
    std::fs::create_dir_all(&lock_dir).map_err(|e| format!("mkdir target: {e}"))?;
    let lock = std::fs::File::create(lock_dir.join(".ostro-proc-build.lock"))
        .map_err(|e| format!("lock file: {e}"))?;
    lock.lock().map_err(|e| format!("lock: {e}"))?;
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let out = Command::new(cargo)
        .current_dir(&root)
        .args([
            "build",
            "-p",
            "ostro",
            "--features",
            "failpoints",
            "--target-dir",
            "target/failpoints",
            "--locked",
        ])
        .env_remove("CARGO_TARGET_DIR")
        .output()
        .map_err(|e| format!("spawn cargo build (failpoint binary): {e}"))?;
    drop(lock);
    if !out.status.success() {
        return Err(format!(
            "failpoint binary build failed:\n{}",
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    Ok(root.join(DEFAULT_BIN))
}

/// Whether this host can bind loopback aliases `127.r.r.x` (Linux: yes; macOS: only with
/// configured aliases).
pub fn loopback_aliases_supported() -> bool {
    std::net::TcpListener::bind(SocketAddr::from(([127, 3, 3, 3], 0))).is_ok()
}

/// Node `i`'s address in cluster `r`.
pub fn node_ip(r: u8, i: u64) -> Ipv4Addr {
    Ipv4Addr::new(127, r, r, u8::try_from(i).expect("node id <= 254"))
}

/// Per-cluster process options.
#[derive(Clone, Debug, Default)]
pub struct ProcOptions {
    /// `FAILPOINTS` for every node (overridden per node by `node_failpoints`).
    pub failpoints: Option<String>,
    /// Per-node `FAILPOINTS`.
    pub node_failpoints: BTreeMap<u64, String>,
    /// Extra environment for every process (`OSTRO_<SECTION>__<KEY>` overrides).
    pub env: Vec<(String, String)>,
    /// `[replication]` timings replacing the tier-C test timings (12-17: the failover
    /// measurement runs the shipped defaults); `rf` / `min_insync` still come from the spec.
    pub replication: Option<ReplicationSection>,
    /// Phase 13 (13-19): `OSTRO_TEST_LEVEL_MAX` of every node of a multi-node spec at its
    /// first start (a static cluster at an emulated level; `upgrade(n, None)` lifts it per
    /// node). The single-node profile takes its level from `start_single_with`.
    pub level_max: Option<u32>,
    /// Phase 13 (13-19): the `ostro` binary every node starts with instead of the failpoint
    /// build (`upgrade_from_prev`: the previous release); [`ProcCluster::set_bin`] changes
    /// it per node for the next (re)start.
    pub bin: Option<PathBuf>,
}

/// Outcome of [`ProcCluster::kill9`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillOutcome {
    /// `waitpid` reported death by SIGKILL.
    Killed,
    /// The process had already exited on its own.
    AlreadyExited,
    /// The node was not running.
    NotRunning,
}

/// Start / kill timings of the last operations (SUMMARY numbers, 12-17 budgets).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProcTimings {
    /// Spawn → ready line of the last (re)start.
    pub last_start: Duration,
    /// SIGKILL → reaped of the last kill.
    pub last_kill: Duration,
}

struct ProcNode {
    ip: Ipv4Addr,
    cfg: ServerConfig,
    config_path: PathBuf,
    log_path: PathBuf,
    failpoints: Option<String>,
    child: Option<Child>,
    /// Last API address (kept after a kill, so clients fail with "connection refused").
    api: SocketAddr,
    /// Phase 13: `OSTRO_TEST_LEVEL_MAX` of every (re)start (`None` = unset).
    level_max: Option<u32>,
    /// Phase 13: the arguments of the next spawn instead of `server -c <config>` (the
    /// `cluster join` command line of a joining node; taken by that spawn).
    next_args: Option<Vec<String>>,
    /// Phase 13 (13-19): the binary of every (re)start instead of the cluster's (`None` =
    /// the failpoint build).
    bin: Option<PathBuf>,
}

/// The tier-C cluster.
pub struct ProcCluster {
    spec: ClusterSpec,
    cluster_id: String,
    r: u8,
    bin: PathBuf,
    pki: TestPki,
    proxy: ChaosProxy,
    /// Phase 13: the proxy links of joined nodes (one proxy per join).
    join_proxies: Vec<ChaosProxy>,
    nodes: BTreeMap<u64, ProcNode>,
    env: Vec<(String, String)>,
    replication: Option<ReplicationSection>,
    http: reqwest::Client,
    timings: ProcTimings,
    root: Option<TempDir>,
}

impl std::fmt::Debug for ProcCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProcCluster")
            .field("spec", &self.spec)
            .field("cluster_id", &self.cluster_id)
            .field("r", &self.r)
            .field("root", &self.root())
            .finish_non_exhaustive()
    }
}

/// A fresh test directory under `OSTRO_PROC_ROOT` when set (e.g. a disk-backed dir for a
/// long campaign), else on `/dev/shm` when writable, else the OS temp dir.
fn test_root() -> Result<TempDir, HarnessError> {
    if let Some(dir) = std::env::var_os("OSTRO_PROC_ROOT") {
        let dir = PathBuf::from(dir);
        std::fs::create_dir_all(&dir)
            .map_err(|e| start_err(format!("create {}: {e}", dir.display())))?;
        return tempfile::Builder::new()
            .prefix("ostro-proc-")
            .tempdir_in(&dir)
            .map_err(|e| start_err(format!("tempdir in {}: {e}", dir.display())));
    }
    let shm = Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-proc-")
            .tempdir_in(shm)
    {
        return Ok(d);
    }
    tempfile::Builder::new()
        .prefix("ostro-proc-")
        .tempdir()
        .map_err(|e| start_err(format!("tempdir: {e}")))
}

/// `NAME listening on http://ADDR (data: ...) ready in N ms` -> ADDR (04-19 format).
pub fn parse_ready(line: &str) -> Option<SocketAddr> {
    let (_, rest) = line.split_once(" listening on http://")?;
    let (addr, rest) = rest.split_once(" (data: ")?;
    rest.contains(") ready in ").then_some(())?;
    addr.parse().ok()
}

/// `joined cluster <cluster id> as node <id> (data: …)` (the `ostro cluster join` line) ->
/// `(cluster id, node id)`.
pub fn parse_joined(line: &str) -> Option<(String, u64)> {
    let (_, rest) = line.split_once("joined cluster ")?;
    let (cluster, rest) = rest.split_once(" as node ")?;
    let (id, _) = rest.split_once(" (data: ")?;
    Some((cluster.to_owned(), id.trim().parse().ok()?))
}

fn kill_child(mut c: Child) {
    let _ = c.kill();
    let _ = c.wait();
}

/// The proxy links of an `n`-node cluster on `127.r.r.x`.
pub fn links_for(r: u8, n: u64) -> Vec<Link> {
    let mut links = Vec::new();
    for from in 1..=n {
        for to in 1..=n {
            if from != to {
                links.push(Link {
                    from,
                    to,
                    listen: SocketAddr::new(
                        IpAddr::V4(node_ip(r, to)),
                        PROXY_PORT_BASE + u16::try_from(from).expect("node id"),
                    ),
                    upstream: SocketAddr::new(IpAddr::V4(node_ip(r, to)), CLUSTER_PORT),
                });
            }
        }
    }
    links
}

/// Pick `r` and start the proxy: retried on a bind conflict (another test chose the same r).
async fn allocate(n: u64) -> Result<(u8, ChaosProxy), HarnessError> {
    if !loopback_aliases_supported() {
        return Err(start_err(
            "loopback aliases 127.r.r.x cannot be bound on this host (macOS: tier C needs \
             `ifconfig lo0 alias`); tier-C tests skip here",
        ));
    }
    // clock-exempt: allocation salt only, never used for logic
    let salt = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default();
    let mut rng = StdRng::seed_from_u64(salt ^ u64::from(std::process::id()).rotate_left(32));
    let mut last = String::new();
    for _ in 0..20 {
        let r: u8 = rng.random_range(2..=250);
        // The cluster listeners must be free on every node IP.
        let probes: Result<Vec<_>, _> = (1..=n)
            .map(|i| {
                std::net::TcpListener::bind(SocketAddr::new(
                    IpAddr::V4(node_ip(r, i)),
                    CLUSTER_PORT,
                ))
            })
            .collect();
        match probes {
            Err(e) => {
                last = format!("r={r}: {e}");
                continue;
            }
            Ok(p) => drop(p),
        }
        match ChaosProxy::start(links_for(r, n)).await {
            Ok(proxy) => return Ok((r, proxy)),
            Err(e) => last = format!("r={r}: {e}"),
        }
    }
    Err(start_err(format!("no free 127.r.r.x range: {last}")))
}

impl ProcCluster {
    /// Start a cluster with explicit process options.
    pub async fn start_with(spec: ClusterSpec, opts: ProcOptions) -> Result<Self, HarnessError> {
        Self::launch(spec, opts, None).await
    }

    /// Start one server-profile node (Phase 13 single-node profile) with explicit process
    /// options at an emulated level (`OSTRO_TEST_LEVEL_MAX`; `None` = unset).
    /// [`P13Harness::start_single`] is `start_single_with(spec, ProcOptions::default(),
    /// None)`.
    pub async fn start_single_with(
        spec: ClusterSpec,
        opts: ProcOptions,
        level_max: Option<u32>,
    ) -> Result<Self, HarnessError> {
        if spec.nodes != 1 {
            return Err(start_err(format!(
                "start_single needs spec.nodes == 1, got {}",
                spec.nodes
            )));
        }
        Self::launch(spec, opts, Some(level_max)).await
    }

    /// Start every node of `spec`; `single = Some(level)` is the Phase 13 single-node
    /// profile (the test CA imported) at that emulated level.
    async fn launch(
        spec: ClusterSpec,
        opts: ProcOptions,
        single: Option<Option<u32>>,
    ) -> Result<Self, HarnessError> {
        if spec.nodes == 0 || spec.nodes > 200 {
            return Err(start_err(format!(
                "nodes must be 1..=200, got {}",
                spec.nodes
            )));
        }
        let bin = ensure_failpoint_bin()?;
        let node_bin = opts.bin.clone();
        let static_level = opts.level_max;
        let n = spec.nodes as u64;
        let (r, proxy) = allocate(n).await?;
        let root = test_root()?;
        let cluster_id = format!("conf-{}", spec.seed);
        let pki = TestPki::new(&cluster_id)?;
        let pki_dir = root.path().join("pki");
        let ca = pki.write_ca(&pki_dir)?;
        if spec.objstore {
            std::fs::create_dir_all(root.path().join("bucket"))
                .map_err(|e| start_err(format!("bucket dir: {e}")))?;
        }
        let mut c = ProcCluster {
            spec,
            cluster_id,
            r,
            bin,
            pki,
            proxy,
            join_proxies: Vec::new(),
            nodes: BTreeMap::new(),
            env: opts.env,
            replication: opts.replication,
            http: reqwest::Client::new(),
            timings: ProcTimings::default(),
            root: Some(root),
        };
        for i in 1..=n {
            let ip = node_ip(r, i);
            // Node certificates for clients and later plans (the server issues its own from
            // the CA key): the URI SAN plus this node's IP.
            let cert = c.pki.node_cert(i, &[IpAddr::V4(ip)])?;
            for (ext, pem) in [("crt", &cert.cert_pem), ("key", &cert.key_pem)] {
                std::fs::write(pki_dir.join(format!("node{i}.{ext}")), pem)
                    .map_err(|e| start_err(format!("write node cert: {e}")))?;
            }
            let mut cfg = c.node_config(i, &ca);
            if single.is_some() {
                cfg.cluster.tls.ca_cert_file = Some(ca.0.clone());
                cfg.cluster.tls.ca_key_file = Some(ca.1.clone());
            }
            let config_path = c.root().join(format!("node{i}.toml"));
            std::fs::write(&config_path, cfg.to_toml_string())
                .map_err(|e| start_err(format!("write {}: {e}", config_path.display())))?;
            let failpoints = opts
                .node_failpoints
                .get(&i)
                .cloned()
                .or_else(|| opts.failpoints.clone());
            c.nodes.insert(
                i,
                ProcNode {
                    ip,
                    cfg,
                    log_path: c.root().join(format!("node{i}.log")),
                    config_path,
                    failpoints,
                    child: None,
                    api: SocketAddr::new(IpAddr::V4(ip), 0),
                    level_max: match single {
                        Some(l) => l,
                        None => static_level,
                    },
                    next_args: None,
                    bin: node_bin.clone(),
                },
            );
        }
        // Spawn every node, then wait for every ready line (they boot concurrently).
        let mut pending = Vec::new();
        for i in 1..=n {
            match c.spawn(i) {
                Ok(rx) => pending.push((i, rx)),
                Err(e) => return Err(e),
            }
        }
        let t0 = Instant::now();
        for (i, rx) in pending {
            let api = c.await_ready(i, rx).await?;
            c.set_api(i, api)?;
        }
        c.timings.last_start = t0.elapsed();
        Ok(c)
    }

    fn node_config(&self, id: u64, ca: &(PathBuf, PathBuf)) -> ServerConfig {
        let spec = &self.spec;
        let ip = IpAddr::V4(node_ip(self.r, id));
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = SocketAddr::new(ip, 0);
        cfg.cluster.listen = SocketAddr::new(ip, CLUSTER_PORT);
        // The node certificate's SANs are `[cluster] advertise` plus loopback (12-14): the
        // proxied peers dial this node's own 127.r.r.i, so it must be advertised.
        cfg.cluster.advertise = vec![ip.to_string()];
        cfg.node.data_dir = self.root().join(format!("node{id}"));
        cfg.node.partitions = spec.partitions;
        cfg.node.node_id = id;
        cfg.cluster.cluster_id = Some(self.cluster_id.clone());
        cfg.cluster.peers = (1..=spec.nodes as u64)
            .filter(|p| *p != id)
            .map(|p| PeerConfig {
                id: p,
                addr: self.proxy.addr(id, p).to_string(),
                repl_addr: None,
            })
            .collect();
        if spec.nodes > 1 {
            cfg.cluster.tls.ca_cert_file = Some(ca.0.clone());
            cfg.cluster.tls.ca_key_file = Some(ca.1.clone());
        }
        self.test_timings(&mut cfg);
        cfg
    }

    /// The `[replication]` timings (test or [`ProcOptions::replication`]), `rf` /
    /// `min_insync` and the shared bucket.
    fn test_timings(&self, cfg: &mut ServerConfig) {
        let spec = &self.spec;
        let r = &mut cfg.replication;
        if let Some(timings) = &self.replication {
            *r = timings.clone();
        } else {
            r.session_timeout = FriendlyDuration::from_millis(600);
            r.replica_lag_max = FriendlyDuration::from_secs(2);
            r.meta_election_tick = 6;
            r.fetch_max_wait = FriendlyDuration::from_millis(100);
        }
        r.rf = u8::try_from(spec.rf).ok();
        r.min_insync = u8::try_from(spec.min_insync).ok();
        if spec.objstore {
            cfg.backup.url = Some(format!("file://{}", self.root().join("bucket").display()));
        }
    }

    /// Spawn node `i`'s process; the receiver yields the API address of its ready line.
    fn spawn(&mut self, i: u64) -> Result<mpsc::Receiver<SocketAddr>, HarnessError> {
        let env = self.env.clone();
        let default_bin = self.bin.clone();
        let node = self.nodes.get_mut(&i).expect("node");
        let bin = node.bin.clone().unwrap_or(default_bin);
        if node.child.is_some() {
            return Err(start_err(format!("node {i} is already running")));
        }
        std::fs::create_dir_all(&node.cfg.node.data_dir)
            .map_err(|e| start_err(format!("mkdir data dir: {e}")))?;
        let open_log = || {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&node.log_path)
                .map_err(|e| start_err(format!("open {}: {e}", node.log_path.display())))
        };
        let mut stdout_log = open_log()?;
        let stderr_log = open_log()?;
        let _ = writeln!(stdout_log, "=== harness: spawn node {i} ===");
        let mut cmd = Command::new(&bin);
        for (k, _) in std::env::vars_os() {
            if k.to_string_lossy().starts_with("OSTRO_") {
                cmd.env_remove(&k);
            }
        }
        cmd.env_remove("RUST_LOG");
        for (k, v) in &env {
            cmd.env(k, v);
        }
        match &node.failpoints {
            Some(fp) => cmd.env("FAILPOINTS", fp),
            None => cmd.env_remove("FAILPOINTS"),
        };
        // Phase 13: level emulation of the failpoint binary (D-P13-7).
        if let Some(k) = node.level_max {
            cmd.env(ostro_types::compat::TEST_LEVEL_MAX_ENV, k.to_string());
        }
        match node.next_args.take() {
            Some(args) => cmd.args(args),
            None => cmd.arg("server").arg("-c").arg(&node.config_path),
        };
        cmd.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr_log));
        let mut child = cmd
            .spawn()
            .map_err(|e| start_err(format!("spawn {}: {e}", bin.display())))?;
        let stdout = child.stdout.take().expect("stdout");
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if let Some(a) = parse_ready(&line) {
                    let _ = tx.send(a);
                }
                let _ = writeln!(stdout_log, "{line}");
            }
        });
        node.child = Some(child);
        Ok(rx)
    }

    async fn await_ready(
        &mut self,
        i: u64,
        rx: mpsc::Receiver<SocketAddr>,
    ) -> Result<SocketAddr, HarnessError> {
        // clock-exempt: test timeout
        let deadline = Instant::now() + PROC_START_T;
        loop {
            if let Ok(a) = rx.try_recv() {
                return Ok(a);
            }
            let node = self.nodes.get_mut(&i).expect("node");
            let exited = node
                .child
                .as_mut()
                .map(|c| c.try_wait())
                .and_then(Result::ok)
                .flatten();
            if let Some(st) = exited {
                node.child = None;
                return Err(start_err(format!(
                    "node {i} exited before ready ({st}); log tail:\n{}",
                    self.log_tail(i, 30)
                )));
            }
            if Instant::now() > deadline {
                if let Some(c) = node.child.take() {
                    kill_child(c);
                }
                return Err(start_err(format!(
                    "node {i}: no ready line within {PROC_START_T:?}; log tail:\n{}",
                    self.log_tail(i, 30)
                )));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Record node `i`'s API address and pin its port in the node's config, so a restart
    /// binds the same address (12-17: clients and leader hints stay valid across restarts,
    /// like a server with a fixed port).
    fn set_api(&mut self, i: u64, api: SocketAddr) -> Result<(), HarnessError> {
        let node = self.nodes.get_mut(&i).expect("node");
        node.api = api;
        if node.cfg.api.listen != api {
            node.cfg.api.listen = api;
            std::fs::write(&node.config_path, node.cfg.to_toml_string())
                .map_err(|e| start_err(format!("write {}: {e}", node.config_path.display())))?;
        }
        Ok(())
    }

    fn node(&self, i: u64) -> &ProcNode {
        self.nodes
            .get(&i)
            .unwrap_or_else(|| panic!("no node {i} in this cluster"))
    }

    /// The spec the cluster was started with.
    pub fn spec(&self) -> &ClusterSpec {
        &self.spec
    }

    /// `conf-<seed>`.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// The test CA.
    pub fn pki(&self) -> &TestPki {
        &self.pki
    }

    /// The chaos proxy (finer rules than isolate / heal / partition: delay, blackhole).
    pub fn proxy(&self) -> &ChaosProxy {
        &self.proxy
    }

    /// This cluster's `r` (nodes on `127.r.r.i`).
    pub fn r(&self) -> u8 {
        self.r
    }

    /// Node `i`'s IP.
    pub fn ip(&self, i: u64) -> Ipv4Addr {
        self.node(i).ip
    }

    /// The configuration node `i` runs with.
    pub fn config(&self, i: u64) -> &ServerConfig {
        &self.node(i).cfg
    }

    /// Node `i`'s config file.
    pub fn config_path(&self, i: u64) -> &Path {
        &self.node(i).config_path
    }

    /// Node `i`'s log file (stdout + stderr of every incarnation).
    pub fn log_path(&self, i: u64) -> &Path {
        &self.node(i).log_path
    }

    /// The last `n` lines of node `i`'s log.
    pub fn log_tail(&self, i: u64, n: usize) -> String {
        let text = std::fs::read_to_string(self.log_path(i)).unwrap_or_default();
        let lines: Vec<&str> = text.lines().collect();
        lines[lines.len().saturating_sub(n)..].join("\n")
    }

    /// The pid of node `i` while it runs.
    pub fn pid(&self, i: u64) -> Option<u32> {
        self.node(i).child.as_ref().map(Child::id)
    }

    /// Whether node `i`'s process is alive.
    pub fn is_running(&mut self, i: u64) -> bool {
        let node = self.nodes.get_mut(&i).expect("node");
        match node.child.as_mut().map(|c| c.try_wait()) {
            Some(Ok(None)) => true,
            Some(_) => {
                node.child = None;
                false
            }
            None => false,
        }
    }

    /// The root dir (`node<i>/`, `node<i>.toml`, `node<i>.log`, `pki/`, `bucket/`).
    pub fn root(&self) -> &Path {
        self.root.as_ref().expect("root").path()
    }

    /// Timings of the last start / kill.
    pub fn timings(&self) -> ProcTimings {
        self.timings
    }

    /// Node `i`'s `FAILPOINTS` (the spec of its current / next start).
    pub fn failpoints(&self, i: u64) -> Option<&str> {
        self.node(i).failpoints.as_deref()
    }

    /// Set node `i`'s `FAILPOINTS` (`None` = unset); effective at its next (re)start.
    pub fn set_failpoints(&mut self, i: u64, spec: Option<&str>) {
        self.nodes.get_mut(&i).expect("node").failpoints = spec.map(str::to_owned);
    }

    /// `SIGKILL` + `waitpid`; `Killed` only when the wait status says signal 9.
    pub async fn kill9(&mut self, i: u64) -> Result<KillOutcome, HarnessError> {
        let Some(mut child) = self.nodes.get_mut(&i).expect("node").child.take() else {
            return Ok(KillOutcome::NotRunning);
        };
        if let Ok(Some(_)) = child.try_wait() {
            return Ok(KillOutcome::AlreadyExited);
        }
        let t0 = Instant::now();
        // Child::kill is SIGKILL on unix; only our own child's pid is signalled.
        let _ = child.kill();
        let st = tokio::task::spawn_blocking(move || child.wait())
            .await
            .map_err(|e| HarnessError::Other(format!("wait task: {e}")))?
            .map_err(|e| HarnessError::Other(format!("waitpid: {e}")))?;
        self.timings.last_kill = t0.elapsed();
        match st.signal() {
            Some(9) => Ok(KillOutcome::Killed),
            _ if st.code().is_some() => Ok(KillOutcome::AlreadyExited),
            other => Err(HarnessError::Other(format!(
                "node {i}: unexpected wait status {st} (signal {other:?})"
            ))),
        }
    }

    /// Restart several nodes at once: every process is spawned before any ready line is
    /// awaited (a node's start waits for a metadata leader, so two nodes that together form
    /// the quorum must boot concurrently). Heals each node's links like `restart`.
    pub async fn restart_many(&mut self, ids: &[u64]) -> Result<(), HarnessError> {
        for &n in ids {
            if self.pid(n).is_some() {
                self.kill9(n).await?;
            }
        }
        let t0 = Instant::now();
        let mut pending = Vec::new();
        for &n in ids {
            self.heal_links(n);
            pending.push((n, self.spawn(n)?));
        }
        for (n, rx) in pending {
            let api = self.await_ready(n, rx).await?;
            self.set_api(n, api)?;
        }
        self.timings.last_start = t0.elapsed();
        Ok(())
    }

    fn kill_all(&mut self) {
        for node in self.nodes.values_mut() {
            if let Some(c) = node.child.take() {
                kill_child(c);
            }
        }
    }

    /// Clear every proxy rule of `node`'s links (the Phase 12 links and the joined nodes').
    fn heal_links(&self, node: u64) {
        self.proxy.heal_node(node);
        for p in &self.join_proxies {
            p.heal_node(node);
        }
    }

    /// The proxy that carries `from → to` (the Phase 12 proxy or a joined node's).
    pub fn proxy_for(&self, from: u64, to: u64) -> Option<&ChaosProxy> {
        std::iter::once(&self.proxy)
            .chain(&self.join_proxies)
            .find(|p| p.links().iter().any(|l| l.from == from && l.to == to))
    }

    /// The emulated level node `i` (re)starts with (`OSTRO_TEST_LEVEL_MAX`; `None` = unset).
    pub fn level_max(&self, i: u64) -> Option<u32> {
        self.node(i).level_max
    }

    /// The lowest running node other than `except`.
    fn running_node(&self, except: Option<u64>) -> Result<u64, HarnessError> {
        self.nodes
            .iter()
            .find(|(id, n)| n.child.is_some() && Some(**id) != except)
            .map(|(id, _)| *id)
            .ok_or_else(|| HarnessError::Other("no running node to send the request to".into()))
    }

    /// The binary node `i` (re)starts with (`None` = the failpoint build); effective at its
    /// next (re)start (13-19 `upgrade_from_prev`).
    pub fn set_bin(&mut self, i: u64, bin: Option<PathBuf>) {
        self.nodes.get_mut(&i).expect("node").bin = bin;
    }

    /// Set node `i`'s emulated level (`OSTRO_TEST_LEVEL_MAX`; `None` = unset) without
    /// restarting it; effective at its next (re)start.
    pub fn set_level_max(&mut self, i: u64, level_max: Option<u32>) {
        self.nodes.get_mut(&i).expect("node").level_max = level_max;
    }

    /// SIGTERM node `i` and wait up to [`STOP_T`] for it to exit (SIGKILL after that): the
    /// controlled shutdown path (13-11 `before_shutdown`). Its data dir is kept; `restart`
    /// starts it again. Returns whether it exited on its own within [`STOP_T`] (`false` =
    /// SIGKILL fallback, or the node was not running).
    pub async fn stop_gracefully(&mut self, i: u64) -> Result<bool, HarnessError> {
        let Some(mut child) = self.nodes.get_mut(&i).expect("node").child.take() else {
            return Ok(false);
        };
        if let Ok(Some(_)) = child.try_wait() {
            return Ok(false);
        }
        let pid = libc::pid_t::try_from(child.id())
            .map_err(|e| HarnessError::Other(format!("pid: {e}")))?;
        // SAFETY: signals only our own, not yet reaped child (its pid cannot be reused
        // before the wait below).
        unsafe {
            libc::kill(pid, libc::SIGTERM);
        }
        // clock-exempt: test timeout
        let deadline = Instant::now() + STOP_T;
        loop {
            match child.try_wait() {
                Ok(Some(_)) => return Ok(true),
                Ok(None) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                _ => {
                    tracing::warn!(node = i, "graceful stop timed out; SIGKILL");
                    kill_child(child);
                    return Ok(false);
                }
            }
        }
    }

    /// The last `joined cluster <cid> as node <n>` line in node slot `key`'s log.
    fn joined_id(&self, key: u64) -> Option<u64> {
        let text = std::fs::read_to_string(self.log_path(key)).unwrap_or_default();
        text.lines().rev().find_map(parse_joined).map(|(_, id)| id)
    }

    /// Wait (bounded) until no partition lists `id` as a replica (offline meta logs).
    async fn wait_no_replica(&self, id: u64) -> Result<(), HarnessError> {
        // clock-exempt: test timeout
        let deadline = Instant::now() + MEMBERSHIP_T;
        loop {
            let mut holds = false;
            for p in 0..self.spec.partitions {
                if self.assignment(p).await.replicas.contains(&id) {
                    holds = true;
                    break;
                }
            }
            if !holds {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(HarnessError::Deadline(MEMBERSHIP_T));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Register the proxy links between joined node `id` (on `ip`) and every other node.
    async fn add_links(&mut self, id: u64, ip: Ipv4Addr) -> Result<(), HarnessError> {
        let port = |n: u64| -> Result<u16, HarnessError> {
            u16::try_from(n)
                .ok()
                .and_then(|n| PROXY_PORT_BASE.checked_add(n))
                .ok_or_else(|| start_err(format!("node id {n} has no proxy port")))
        };
        let mut links = Vec::new();
        for (m, node) in &self.nodes {
            if *m == id {
                continue;
            }
            links.push(Link {
                from: id,
                to: *m,
                listen: SocketAddr::new(IpAddr::V4(node.ip), port(id)?),
                upstream: node.cfg.cluster.listen,
            });
            links.push(Link {
                from: *m,
                to: id,
                listen: SocketAddr::new(IpAddr::V4(ip), port(*m)?),
                upstream: SocketAddr::new(IpAddr::V4(ip), CLUSTER_PORT),
            });
        }
        if links.is_empty() {
            return Ok(());
        }
        let proxy = ChaosProxy::start(links)
            .await
            .map_err(|e| start_err(format!("proxy links of joined node {id}: {e}")))?;
        self.join_proxies.push(proxy);
        Ok(())
    }

    /// Kill every node and hand back the root instead of deleting it.
    pub fn stop_keeping_dirs(mut self) -> TempDir {
        self.kill_all();
        self.root.take().expect("root")
    }
}

impl Drop for ProcCluster {
    fn drop(&mut self) {
        self.kill_all();
        if std::thread::panicking()
            && let Some(root) = self.root.take()
        {
            let kept = root.keep();
            eprintln!(
                "tier-C cluster kept for post-mortem (logs: node<i>.log): {}",
                kept.display()
            );
        }
    }
}

impl ClusterHarness for ProcCluster {
    async fn start(spec: ClusterSpec) -> Result<Self, HarnessError> {
        Self::start_with(spec, ProcOptions::default()).await
    }

    fn node_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    fn client(&self, node: u64) -> ApiClient {
        ApiClient {
            base_url: format!("http://{}", self.node(node).api),
            http: self.http.clone(),
        }
    }

    async fn assignment(&self, p: u16) -> PartitionAssignmentView {
        // Offline: the meta log with the highest commit among all nodes.
        let best = self
            .nodes
            .values()
            .filter_map(|n| read_meta_log(&n.cfg.node.data_dir.join("meta")).ok())
            .max_by_key(|d| d.commit);
        let rec = best.and_then(|d| d.state.partition(p).map(|v| v.value.clone()));
        match rec {
            Some(a) => PartitionAssignmentView {
                partition: PartitionId(a.partition),
                leader: a.leader,
                epoch: Epoch(a.epoch),
                replicas: a.replicas,
                isr: a.isr,
                isr_version: a.isr_version,
                min_insync: a.min_insync,
            },
            None => PartitionAssignmentView {
                partition: PartitionId(p),
                leader: 0,
                epoch: Epoch(0),
                replicas: Vec::new(),
                isr: Vec::new(),
                isr_version: 0,
                min_insync: 0,
            },
        }
    }

    fn isolate(&mut self, node: u64) {
        self.proxy.isolate(node);
        for p in &self.join_proxies {
            p.isolate(node);
        }
    }

    fn heal(&mut self, node: u64) {
        self.heal_links(node);
    }

    fn partition(&mut self, a: u64, b: u64) {
        self.proxy.partition(a, b);
        for p in &self.join_proxies {
            p.partition(a, b);
        }
    }

    async fn kill(&mut self, node: u64) {
        match self.kill9(node).await {
            Ok(_) => {}
            Err(e) => tracing::warn!(node, error = %e, "kill -9"),
        }
    }

    async fn restart(&mut self, node: u64) -> Result<(), HarnessError> {
        if self.pid(node).is_some() {
            self.kill9(node).await?;
        }
        // Heal first: since 12-14 a starting node waits for a metadata leader before its
        // ready line, so a node restarted behind cut links would never become ready.
        self.heal_links(node);
        let t0 = Instant::now();
        let rx = self.spawn(node)?;
        let api = self.await_ready(node, rx).await?;
        self.timings.last_start = t0.elapsed();
        self.set_api(node, api)?;
        Ok(())
    }

    fn node_disk(&self, node: u64) -> DataDirView {
        DataDirView {
            node,
            root: self.node(node).cfg.node.data_dir.clone(),
        }
    }

    fn bucket(&self) -> Option<PathBuf> {
        self.spec.objstore.then(|| self.root().join("bucket"))
    }

    async fn meta_log(&self, node: u64) -> Vec<MetaLogEntry> {
        // A read-only copy: the process may be writing while we read.
        let src = self.node(node).cfg.node.data_dir.join("meta");
        let copy = tempfile::Builder::new()
            .prefix(&format!("meta-copy-{node}-"))
            .tempdir_in(self.root())
            .unwrap_or_else(|e| panic!("node {node}: meta copy dir: {e}"));
        copy_dir(&src, copy.path())
            .unwrap_or_else(|e| panic!("node {node}: copy {}: {e}", src.display()));
        match read_meta_log(copy.path()) {
            Ok(d) => d.entries,
            // Never a silently empty log: an oracle over it would pass vacuously.
            Err(e) => panic!("node {node}: meta log unreadable: {e}"),
        }
    }

    async fn admin_status(&self, node: u64) -> Value {
        self.client(node)
            .get_json("/v1/admin/replication")
            .await
            .map(|(_, v)| v)
            .unwrap_or(Value::Null)
    }

    async fn stop(mut self) {
        self.kill_all();
        // `self.root` drops here: the dirs are removed.
    }
}

impl P13Harness for ProcCluster {
    async fn start_single(spec: ClusterSpec) -> Result<Self, HarnessError> {
        Self::start_single_with(spec, ProcOptions::default(), None).await
    }

    async fn cluster_init(&mut self, node: u64) -> Result<Value, HarnessError> {
        // `[cluster] advertise` stays the Phase 7 list of names (13-02 open issue,
        // orchestrator decision); the init advertises the node's cluster listener.
        let advertise = self.node(node).cfg.cluster.listen.to_string();
        p13_post(
            &self.client(node),
            "/v1/cluster/init",
            &serde_json::json!({ "advertise": advertise }),
        )
        .await
    }

    async fn add_node(&mut self, opts: NodeOpts) -> Result<u64, HarnessError> {
        let seed = self.running_node(None)?;
        let api = self.client(seed);
        let token = node_token(&api).await?;
        // The next free 127.r.r.k (k above every address in use).
        let k = self
            .nodes
            .values()
            .map(|n| u64::from(n.ip.octets()[3]))
            .max()
            .unwrap_or(0)
            + 1;
        if k > 254 {
            return Err(start_err("no free 127.r.r.k address left"));
        }
        let ip = node_ip(self.r, k);
        drop(
            std::net::TcpListener::bind(SocketAddr::new(IpAddr::V4(ip), CLUSTER_PORT))
                .map_err(|e| start_err(format!("{ip}:{CLUSTER_PORT} is taken: {e}")))?,
        );
        let api_addr = free_addr(IpAddr::V4(ip))?;
        let data_dir = self.root().join(format!("join{k}"));
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = api_addr;
        cfg.cluster.listen = SocketAddr::new(IpAddr::V4(ip), CLUSTER_PORT);
        cfg.cluster.advertise = vec![ip.to_string()];
        cfg.cluster.cluster_id = Some(self.cluster_id.clone());
        cfg.node.data_dir = data_dir.clone();
        cfg.node.partitions = self.spec.partitions;
        self.test_timings(&mut cfg);
        let config_path = self.root().join(format!("join{k}.toml"));
        std::fs::write(&config_path, cfg.to_toml_string())
            .map_err(|e| start_err(format!("write {}: {e}", config_path.display())))?;
        let mut args = vec![
            "cluster".to_owned(),
            "join".to_owned(),
            api.base_url.clone(),
            "--token".to_owned(),
            token.token,
            "--ca-hash".to_owned(),
            token.ca_hash,
            "--data".to_owned(),
            data_dir.display().to_string(),
            "-c".to_owned(),
            config_path.display().to_string(),
        ];
        if opts.advertise {
            args.extend([
                "--advertise-api".to_owned(),
                api_addr.to_string(),
                "--advertise-cluster".to_owned(),
                cfg.cluster.listen.to_string(),
            ]);
        }
        // Spawned under a provisional key (ids are allocated by the cluster), re-keyed by the
        // id of the `joined cluster … as node <n>` line.
        let key = u64::MAX - k;
        self.nodes.insert(
            key,
            ProcNode {
                ip,
                cfg,
                log_path: self.root().join(format!("join{k}.log")),
                config_path,
                failpoints: None,
                child: None,
                api: api_addr,
                level_max: opts.level_max,
                next_args: Some(args),
                bin: None,
            },
        );
        let started = async {
            let rx = self.spawn(key)?;
            let api = self.await_ready(key, rx).await?;
            let id = self.joined_id(key).ok_or_else(|| {
                start_err(format!(
                    "joined node on {ip}: no `joined cluster … as node <n>` line; log tail:\n{}",
                    self.log_tail(key, 30)
                ))
            })?;
            Ok::<_, HarnessError>((api, id))
        }
        .await;
        let (api_bound, id) = match started {
            Ok(v) => v,
            Err(e) => {
                if let Some(mut n) = self.nodes.remove(&key)
                    && let Some(c) = n.child.take()
                {
                    kill_child(c);
                }
                return Err(e);
            }
        };
        let mut node = self.nodes.remove(&key).expect("joined node");
        if self.nodes.contains_key(&id) {
            if let Some(c) = node.child.take() {
                kill_child(c);
            }
            return Err(HarnessError::Other(format!(
                "join allocated node id {id}, which this harness already runs"
            )));
        }
        node.cfg.node.node_id = id;
        self.nodes.insert(id, node);
        // Pins the API port and rewrites the config with the allocated id (restarts run
        // `ostro server -c <config>` on the prepared data dir).
        self.set_api(id, api_bound)?;
        std::fs::write(
            &self.node(id).config_path,
            self.node(id).cfg.to_toml_string(),
        )
        .map_err(|e| start_err(format!("write joined config: {e}")))?;
        self.add_links(id, ip).await?;
        Ok(id)
    }

    async fn remove(&mut self, id: u64, force: bool) -> Result<(), HarnessError> {
        let via = self.running_node(Some(id))?;
        remove_and_wait(&self.client(via), id, force).await
    }

    async fn drain(&mut self, id: u64) -> Result<(), HarnessError> {
        let via = self
            .running_node(Some(id))
            .or_else(|_| self.running_node(None))?;
        p13_post_membership(
            &self.client(via),
            &format!("/v1/cluster/members/{id}/drain"),
            &serde_json::json!({}),
        )
        .await?;
        self.wait_no_replica(id).await
    }

    async fn upgrade(&mut self, node: u64, level_max: Option<u32>) -> Result<(), HarnessError> {
        self.nodes.get_mut(&node).expect("node").level_max = level_max;
        // Graceful stop (the controlled shutdown may hand partitions off), then the same
        // config and data dir with the new environment. Proxy rules are left as they are.
        self.stop_gracefully(node).await?;
        let t0 = Instant::now();
        let rx = self.spawn(node)?;
        let api = self.await_ready(node, rx).await?;
        self.timings.last_start = t0.elapsed();
        self.set_api(node, api)
    }

    async fn cluster_status(&self, node: u64) -> Result<Value, HarnessError> {
        p13_get(&self.client(node), "/v1/cluster/status").await
    }
}

/// Recursive copy (files that vanish mid-copy are skipped: the node may be compacting).
fn copy_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for e in std::fs::read_dir(src)? {
        let e = e?;
        let to = dst.join(e.file_name());
        let ft = e.file_type()?;
        if ft.is_dir() {
            copy_dir(&e.path(), &to)?;
        } else if ft.is_file() {
            match std::fs::copy(e.path(), &to) {
                Ok(_) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(err),
            }
        }
    }
    Ok(())
}
