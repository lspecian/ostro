//! Tier B (12-03, D-P12-18): N in-process `ostro-server` instances in one test process,
//! each with its own data dir and API listener, connected through one [`MemNet`]
//! (in-memory tonic transport + seeded [`FaultLayer`](super::fault::FaultLayer)).
//!
//! Every node gets the cluster configuration a real deployment would have (`[node] node_id`,
//! `[cluster] cluster_id`, `[[cluster.peers]]` with synthetic `mem://<id>` addresses, the
//! test CA as `[cluster.tls] ca_cert_file` / `ca_key_file`, `[replication]` with short
//! timings and the spec's `rf` / `min_insync`) and its [`MemTransport`] through the server
//! hook `ServerOptions::cluster_transport`.
//!
//! Since 12-14 the server serves `MetaPeer` and `Replication` on that transport's
//! in-memory incoming, so the nodes form one cluster (static voters = every node).
//!
//! `kill(n)` is "isolate + graceful stop" (data dir kept), not a real crash (research
//! Pitfall 8); real `kill -9` is tier C (12-09). `restart(n)` starts the node again from
//! the same dirs and config with a fresh transport, its links healed first.
//!
//! Phase 13 (13-08, D-P13-25): [`InProcCluster`] implements [`P13Harness`]. `start_single`
//! is one server-profile node (no `[[cluster.peers]]`, `[cluster] advertise = ["mem://1"]`,
//! the test CA imported, cluster listener bound); `add_node` takes a node join token from the
//! lowest running node (`POST /v1/cluster/tokens`), runs the joiner library
//! (`ostro_server::p13::joiner::run_join`, `--no-start`) against that node's API and boots
//! the prepared data dir in process with `ServerOptions::level_max_override`; `upgrade`
//! gracefully stops a node and boots it again from the same data dir at the new override.
//! The in-memory transport routes by node id, so a joined node's advertised `mem://` address
//! is informational only.
//!
//! [`read_meta_log`] decodes a node's metadata Raft log offline (state, snapshot and
//! segment files of the vendored log store, read-only, CRC-checked) and recomputes every
//! apply verdict by replaying a fresh `MetaState`, so oracles see every committed proposal,
//! including the ones apply rejected.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ostro_meta::{MetaState, decode_command, decode_snapshot};
use ostro_partition::roles::PartitionAssignmentView;
use ostro_server::config::{FriendlyDuration, PeerConfig, ServerConfig};
use ostro_server::{ServerHandle, ServerOptions};
use ostro_types::{Epoch, PartitionCount, PartitionId, RunId};
use serde_json::{Value, json};
use tempfile::TempDir;

use super::certs::TestPki;
use super::fault::{MemNet, MemTransport};
use super::p13::{NodeOpts, P13Harness};
use super::{ApiClient, ClusterHarness, ClusterSpec, DataDirView, HarnessError, MetaLogEntry};

/// Bound on one server start (fresh fjall stores cost ~300 fsyncs each; generous under load).
pub const START_T: Duration = Duration::from_secs(120);
/// Bound on one graceful shutdown.
pub const STOP_T: Duration = Duration::from_secs(60);
/// The test module's workflow (the `smoke` guest returns its input).
pub const TEST_WORKFLOW: &str = "smoke";

/// One node slot.
struct NodeSlot {
    cfg: ServerConfig,
    handle: Option<ServerHandle>,
    /// Last API address (kept after a stop, so clients fail with "connection refused").
    api: SocketAddr,
    /// Phase 13: `ServerOptions::level_max_override` of every (re)start (`None` = the
    /// build's level).
    level_max: Option<u32>,
}

/// The tier-B cluster.
pub struct InProcCluster {
    spec: ClusterSpec,
    cluster_id: String,
    net: Arc<MemNet>,
    pki: TestPki,
    nodes: BTreeMap<u64, NodeSlot>,
    http: reqwest::Client,
    /// Owns every node dir, the CA files and the bucket; dropped last.
    root: TempDir,
}

impl std::fmt::Debug for InProcCluster {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InProcCluster")
            .field("spec", &self.spec)
            .field("cluster_id", &self.cluster_id)
            .field("root", &self.root.path())
            .finish_non_exhaustive()
    }
}

fn other(what: &str, e: impl std::fmt::Display) -> HarnessError {
    HarnessError::Other(format!("{what}: {e}"))
}

/// A fresh test directory on `/dev/shm` when writable, else the OS temp dir.
fn test_root() -> Result<TempDir, HarnessError> {
    let shm = Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-repl-")
            .tempdir_in(shm)
    {
        return Ok(d);
    }
    tempfile::Builder::new()
        .prefix("ostro-repl-")
        .tempdir()
        .map_err(|e| HarnessError::Start(format!("tempdir: {e}")))
}

async fn boot(
    cfg: ServerConfig,
    transport: Arc<MemTransport>,
    wal_rotation_bytes: Option<u64>,
    level_max: Option<u32>,
) -> Result<ServerHandle, HarnessError> {
    let node = cfg.node.node_id;
    let mut opts = ServerOptions::default()
        .with_cluster_transport(transport)
        .with_level_max_override(level_max);
    if let Some(b) = wal_rotation_bytes {
        opts = opts.with_wal_rotation_bytes(b);
    }
    // clock-exempt: test timeout
    match tokio::time::timeout(START_T, ostro_server::server::start_with_options(cfg, opts)).await {
        Ok(Ok(h)) => Ok(h),
        Ok(Err(e)) => Err(HarnessError::Start(format!("node {node}: {e}"))),
        Err(_) => Err(HarnessError::Start(format!(
            "node {node}: start timed out after {START_T:?}"
        ))),
    }
}

async fn shutdown(node: u64, h: ServerHandle) {
    // clock-exempt: test timeout
    match tokio::time::timeout(STOP_T, h.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(node, error = %e, "node shutdown failed"),
        Err(_) => tracing::warn!(node, "node shutdown timed out"),
    }
}

impl InProcCluster {
    /// The spec the cluster was started with.
    pub fn spec(&self) -> &ClusterSpec {
        &self.spec
    }

    /// `conf-<seed>`.
    pub fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    /// The in-memory network (fault rules: `net().faults()`).
    pub fn net(&self) -> &Arc<MemNet> {
        &self.net
    }

    /// The test CA.
    pub fn pki(&self) -> &TestPki {
        &self.pki
    }

    /// The running server of `node` (`None` while killed).
    pub fn handle(&self, node: u64) -> Option<&ServerHandle> {
        self.nodes.get(&node).and_then(|s| s.handle.as_ref())
    }

    /// The configuration `node` runs with.
    pub fn config(&self, node: u64) -> &ServerConfig {
        &self.slot(node).cfg
    }

    /// The configuration `node` (re)starts with, for editing before a restart (13-13:
    /// `[api] strict_reads`).
    pub fn config_mut(&mut self, node: u64) -> &mut ServerConfig {
        &mut self
            .nodes
            .get_mut(&node)
            .unwrap_or_else(|| panic!("no node {node} in this cluster"))
            .cfg
    }

    /// A channel that presents `PeerNode(from)` to node `to`, through the fault layer (raw
    /// `Replication` / `MetaPeer` RPCs from scenarios, e.g. a stale-epoch `Fetch`).
    pub fn channel_as(&self, from: u64, to: u64) -> tonic::transport::Channel {
        self.net.channel_as(from, to)
    }

    /// The node dirs root (`<root>/node<id>`, `<root>/pki`, `<root>/bucket`).
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// Stop every node and hand back the root dir instead of deleting it (lock-leak checks,
    /// post-mortem oracles).
    pub async fn stop_keeping_dirs(mut self) -> TempDir {
        self.stop_all().await;
        self.root
    }

    fn slot(&self, node: u64) -> &NodeSlot {
        self.nodes
            .get(&node)
            .unwrap_or_else(|| panic!("no node {node} in this cluster"))
    }

    async fn stop_all(&mut self) {
        let mut stops = Vec::new();
        for (id, slot) in &mut self.nodes {
            if let Some(h) = slot.handle.take() {
                stops.push(shutdown(*id, h));
            }
        }
        futures_util::future::join_all(stops).await;
    }

    fn node_config(&self, id: u64, ca: &(PathBuf, PathBuf)) -> ServerConfig {
        let spec = &self.spec;
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        cfg.cluster.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        cfg.node.data_dir = self.root.path().join(format!("node{id}"));
        cfg.node.partitions = spec.partitions;
        cfg.node.node_id = id;
        cfg.cluster.cluster_id = Some(self.cluster_id.clone());
        let ids = 1..=spec.nodes as u64;
        cfg.cluster.peers = ids
            .filter(|p| *p != id)
            .map(|p| PeerConfig {
                id: p,
                addr: format!("mem://{p}"),
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

    /// The `[replication]` test timings, `rf` / `min_insync` and the shared bucket.
    fn test_timings(&self, cfg: &mut ServerConfig) {
        let spec = &self.spec;
        let r = &mut cfg.replication;
        r.session_timeout = FriendlyDuration::from_millis(600);
        r.replica_lag_max = FriendlyDuration::from_secs(2);
        r.meta_election_tick = 6;
        r.fetch_max_wait = FriendlyDuration::from_millis(100);
        r.rf = u8::try_from(spec.rf).ok();
        r.min_insync = u8::try_from(spec.min_insync).ok();
        if spec.objstore {
            cfg.backup.url = Some(format!(
                "file://{}",
                self.root.path().join("bucket").display()
            ));
        }
    }

    /// Start one server-profile node at an emulated level (`level_max` = its
    /// `ServerOptions::level_max_override`; `None` = this build's level). [`P13Harness::
    /// start_single`] is `start_single_with(spec, None)`.
    pub async fn start_single_with(
        spec: ClusterSpec,
        level_max: Option<u32>,
    ) -> Result<Self, HarnessError> {
        if spec.nodes != 1 {
            return Err(HarnessError::Start(format!(
                "start_single needs spec.nodes == 1, got {}",
                spec.nodes
            )));
        }
        Self::launch(spec, Some(level_max)).await
    }

    /// The emulated level `node` (re)starts with (`None` = this build's level).
    pub fn level_max(&self, node: u64) -> Option<u32> {
        self.slot(node).level_max
    }

    /// The lowest running node other than `except` (the node the Phase 13 REST calls go to).
    fn running_node(&self, except: Option<u64>) -> Result<u64, HarnessError> {
        self.nodes
            .iter()
            .find(|(id, s)| s.handle.is_some() && Some(**id) != except)
            .map(|(id, _)| *id)
            .ok_or_else(|| HarnessError::Other("no running node to send the request to".into()))
    }

    /// The configuration of a node joined through the join flow: the prepared data dir and
    /// its id, no peers and no CA files (the joined certificate set lives in `<data>/tls`).
    fn joined_config(
        &self,
        id: u64,
        cluster_id: &str,
        data_dir: PathBuf,
        api: SocketAddr,
        advertise: bool,
    ) -> ServerConfig {
        let mut cfg = ServerConfig::server_defaults();
        cfg.api.listen = api;
        cfg.cluster.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        if advertise {
            cfg.cluster.advertise = vec![format!("mem://{id}")];
        }
        cfg.node.data_dir = data_dir;
        cfg.node.partitions = self.spec.partitions;
        cfg.node.node_id = id;
        cfg.cluster.cluster_id = Some(cluster_id.to_owned());
        self.test_timings(&mut cfg);
        cfg
    }

    /// Start every node of `spec`; `single = Some(level)` is the Phase 13 single-node
    /// profile (`[cluster] advertise` set, the test CA imported) at that emulated level.
    async fn launch(spec: ClusterSpec, single: Option<Option<u32>>) -> Result<Self, HarnessError> {
        if spec.nodes == 0 {
            return Err(HarnessError::Start(
                "a cluster needs at least one node".into(),
            ));
        }
        let root = test_root()?;
        let cluster_id = format!("conf-{}", spec.seed);
        let pki = TestPki::new(&cluster_id)?;
        let ca = pki.write_ca(&root.path().join("pki"))?;
        if spec.objstore {
            std::fs::create_dir_all(root.path().join("bucket"))
                .map_err(|e| HarnessError::Start(format!("bucket dir: {e}")))?;
        }
        let mut c = InProcCluster {
            net: MemNet::new(spec.seed),
            spec,
            cluster_id,
            pki,
            nodes: BTreeMap::new(),
            http: reqwest::Client::new(),
            root,
        };
        let ids: Vec<u64> = (1..=c.spec.nodes as u64).collect();
        let mut boots = Vec::new();
        for id in &ids {
            let mut cfg = c.node_config(*id, &ca);
            let level_max = single.flatten();
            if single.is_some() {
                cfg.cluster.advertise = vec![format!("mem://{id}")];
                cfg.cluster.tls.ca_cert_file = Some(ca.0.clone());
                cfg.cluster.tls.ca_key_file = Some(ca.1.clone());
            }
            boots.push(boot(
                cfg.clone(),
                c.net.transport(*id),
                c.spec.wal_rotation_bytes,
                level_max,
            ));
            c.nodes.insert(
                *id,
                NodeSlot {
                    cfg,
                    handle: None,
                    api: SocketAddr::from(([127, 0, 0, 1], 0)),
                    level_max,
                },
            );
        }
        let results = futures_util::future::join_all(boots).await;
        let mut first_err = None;
        for (id, r) in ids.iter().zip(results) {
            match r {
                Ok(h) => {
                    let slot = c.nodes.get_mut(id).expect("slot");
                    slot.api = h.local_addr();
                    slot.handle = Some(h);
                }
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if let Some(e) = first_err {
            c.stop_all().await;
            return Err(e);
        }
        Ok(c)
    }
}

impl ClusterHarness for InProcCluster {
    async fn start(spec: ClusterSpec) -> Result<Self, HarnessError> {
        Self::launch(spec, None).await
    }

    fn node_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    fn client(&self, node: u64) -> ApiClient {
        ApiClient {
            base_url: format!("http://{}", self.slot(node).api),
            http: self.http.clone(),
        }
    }

    async fn assignment(&self, p: u16) -> PartitionAssignmentView {
        // Read from the meta leader when one is running, else any running node.
        let running: Vec<&ServerHandle> = self
            .nodes
            .values()
            .filter_map(|s| s.handle.as_ref())
            .collect();
        let pick = running
            .iter()
            .find(|h| h.meta().is_leader())
            .or_else(|| running.first());
        let rec = pick.and_then(|h| {
            h.meta()
                .view()
                .read(|s| s.partition(p).map(|v| v.value.clone()))
        });
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
        self.net.faults().isolate(node);
    }

    fn heal(&mut self, node: u64) {
        self.net.faults().heal(node);
    }

    fn partition(&mut self, a: u64, b: u64) {
        self.net.faults().partition(a, b);
    }

    async fn kill(&mut self, node: u64) {
        self.net.faults().isolate(node);
        let h = self
            .nodes
            .get_mut(&node)
            .unwrap_or_else(|| panic!("no node {node}"))
            .handle
            .take();
        if let Some(h) = h {
            shutdown(node, h).await;
        }
    }

    async fn restart(&mut self, node: u64) -> Result<(), HarnessError> {
        if self.handle(node).is_some() {
            self.kill(node).await;
        }
        let cfg = self.slot(node).cfg.clone();
        let level_max = self.slot(node).level_max;
        // Heal first (12-14): a node's start waits for its metadata group (meta before
        // partitions), so it must reach the other voters while it boots.
        self.net.faults().heal(node);
        let h = boot(
            cfg,
            self.net.transport(node),
            self.spec.wal_rotation_bytes,
            level_max,
        )
        .await?;
        let slot = self.nodes.get_mut(&node).expect("slot");
        slot.api = h.local_addr();
        slot.handle = Some(h);
        Ok(())
    }

    fn node_disk(&self, node: u64) -> DataDirView {
        DataDirView {
            node,
            root: self.slot(node).cfg.node.data_dir.clone(),
        }
    }

    fn bucket(&self) -> Option<PathBuf> {
        self.spec.objstore.then(|| self.root.path().join("bucket"))
    }

    async fn meta_log(&self, node: u64) -> Vec<MetaLogEntry> {
        let dir = self.slot(node).cfg.node.data_dir.join("meta");
        match read_meta_log(&dir) {
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
        self.stop_all().await;
    }
}

// ------------------------------------------------------------------------------------
// Phase 13 lifecycle (13-08)
// ------------------------------------------------------------------------------------

/// Bound on the wait of [`P13Harness::remove`] / [`P13Harness::drain`].
pub const MEMBERSHIP_T: Duration = Duration::from_secs(120);

/// A non-2xx answer of a Phase 13 REST call: `"{METHOD} {path}: HTTP {status}: {body}"`.
/// Transport failures (connection refused, reset) never use this form, so a scenario can
/// tell "the server answered with an error" from "the call never reached a server".
pub fn http_error(method: &str, path: &str, st: reqwest::StatusCode, body: &Value) -> HarnessError {
    HarnessError::Other(format!("{method} {path}: HTTP {}: {body}", st.as_u16()))
}

/// Whether `e` is a [`http_error`] (the call reached a server).
pub fn is_http_error(e: &HarnessError) -> bool {
    matches!(e, HarnessError::Other(m) if m.contains(": HTTP "))
}

/// `POST path` with a JSON body; the 2xx body, else [`http_error`].
pub(crate) async fn p13_post(
    api: &ApiClient,
    path: &str,
    body: &Value,
) -> Result<Value, HarnessError> {
    let (st, v) = api.post_json(path, body).await?;
    if st.is_success() {
        Ok(v)
    } else {
        Err(http_error("POST", path, st, &v))
    }
}

/// `GET path`; the 2xx body, else [`http_error`].
pub(crate) async fn p13_get(api: &ApiClient, path: &str) -> Result<Value, HarnessError> {
    let (st, v) = api.get_json(path).await?;
    if st.is_success() {
        Ok(v)
    } else {
        Err(http_error("GET", path, st, &v))
    }
}

/// `POST /v1/cluster/tokens` (a node join token and the CA pin).
pub(crate) async fn node_token(
    api: &ApiClient,
) -> Result<ostro_api::phase13::NodeTokenResponse, HarnessError> {
    let v = p13_post(api, "/v1/cluster/tokens", &json!({})).await?;
    serde_json::from_value(v.clone()).map_err(|e| other(&format!("token response {v}"), e))
}

/// The `state` of member `id` in a `GET /v1/cluster/status` document.
pub fn member_state(status: &Value, id: u64) -> Option<String> {
    status["members"]
        .as_array()?
        .iter()
        .find(|m| m["node_id"].as_u64() == Some(id))
        .and_then(|m| m["state"].as_str())
        .map(str::to_owned)
}

/// `POST /v1/cluster/members/{id}/remove`, then poll the status on `api` until `id` is
/// `removed` (bounded by [`MEMBERSHIP_T`]).
pub(crate) async fn remove_and_wait(
    api: &ApiClient,
    id: u64,
    force: bool,
) -> Result<(), HarnessError> {
    let path = format!("/v1/cluster/members/{id}/remove");
    p13_post(api, &path, &json!({ "force": force })).await?;
    // clock-exempt: test timeout
    let deadline = tokio::time::Instant::now() + MEMBERSHIP_T;
    loop {
        if let Ok(st) = p13_get(api, "/v1/cluster/status").await
            && member_state(&st, id).as_deref() == Some("removed")
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(HarnessError::Deadline(MEMBERSHIP_T));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A free `ip:port` (bound and released), so a joining node can advertise its API address
/// before its server starts.
pub(crate) fn free_addr(ip: std::net::IpAddr) -> Result<SocketAddr, HarnessError> {
    let l = std::net::TcpListener::bind(SocketAddr::new(ip, 0)).map_err(|e| other("bind", e))?;
    l.local_addr().map_err(|e| other("local addr", e))
}

impl InProcCluster {
    /// Wait (bounded) until no partition lists `id` as a replica, from the meta view.
    async fn wait_no_replica(&self, id: u64) -> Result<(), HarnessError> {
        // clock-exempt: test timeout
        let deadline = tokio::time::Instant::now() + MEMBERSHIP_T;
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
            if tokio::time::Instant::now() >= deadline {
                return Err(HarnessError::Deadline(MEMBERSHIP_T));
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl P13Harness for InProcCluster {
    async fn start_single(spec: ClusterSpec) -> Result<Self, HarnessError> {
        Self::start_single_with(spec, None).await
    }

    async fn cluster_init(&mut self, node: u64) -> Result<Value, HarnessError> {
        // `[cluster] advertise` stays the Phase 7 list (13-02 open issue, orchestrator
        // decision): its first entry is the cluster address the init advertises.
        let advertise = self.slot(node).cfg.cluster.advertise.first().cloned();
        p13_post(
            &self.client(node),
            "/v1/cluster/init",
            &json!({ "advertise": advertise }),
        )
        .await
    }

    async fn add_node(&mut self, opts: NodeOpts) -> Result<u64, HarnessError> {
        let seed = self.running_node(None)?;
        let api = self.client(seed);
        let token = node_token(&api).await?;
        let slot = self.nodes.keys().max().copied().unwrap_or(0) + 1;
        let data_dir = self.root.path().join(format!("join{slot}"));
        let api_addr = free_addr(std::net::IpAddr::from([127, 0, 0, 1]))?;
        let args = ostro_server::p13::JoinArgs {
            seed: api.base_url.clone(),
            token: token.token,
            ca_hash: token.ca_hash,
            data_dir: Some(data_dir.clone()),
            config: None,
            advertise_api: opts.advertise.then(|| api_addr.to_string()),
            advertise_cluster: opts.advertise.then(|| format!("mem://{slot}")),
            advertise_repl: None,
            no_start: true,
        };
        // clock-exempt: test timeout
        let prepared = tokio::time::timeout(START_T, ostro_server::p13::joiner::run_join(args))
            .await
            .map_err(|_| HarnessError::Other(format!("join timed out after {START_T:?}")))?
            .map_err(|e| HarnessError::Other(format!("join: {e}")))?;
        let id = prepared.node_id;
        if self.nodes.contains_key(&id) {
            return Err(HarnessError::Other(format!(
                "join allocated node id {id}, which this harness already runs"
            )));
        }
        let cfg = self.joined_config(
            id,
            &prepared.cluster_id,
            prepared.data_dir,
            api_addr,
            opts.advertise,
        );
        let h = boot(
            cfg.clone(),
            self.net.transport(id),
            self.spec.wal_rotation_bytes,
            opts.level_max,
        )
        .await?;
        self.nodes.insert(
            id,
            NodeSlot {
                cfg,
                api: h.local_addr(),
                handle: Some(h),
                level_max: opts.level_max,
            },
        );
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
        p13_post(
            &self.client(via),
            &format!("/v1/cluster/members/{id}/drain"),
            &json!({}),
        )
        .await?;
        self.wait_no_replica(id).await
    }

    async fn upgrade(&mut self, node: u64, level_max: Option<u32>) -> Result<(), HarnessError> {
        let slot = self
            .nodes
            .get_mut(&node)
            .unwrap_or_else(|| panic!("no node {node}"));
        slot.level_max = level_max;
        // A graceful stop without isolating the node (the controlled shutdown may hand its
        // partitions off), then the same data dir and config at the new override. Fault
        // rules are left as they are.
        if let Some(h) = slot.handle.take() {
            shutdown(node, h).await;
        }
        let cfg = slot.cfg.clone();
        let h = boot(
            cfg,
            self.net.transport(node),
            self.spec.wal_rotation_bytes,
            level_max,
        )
        .await?;
        let slot = self.nodes.get_mut(&node).expect("slot");
        slot.api = h.local_addr();
        slot.handle = Some(h);
        Ok(())
    }

    async fn cluster_status(&self, node: u64) -> Result<Value, HarnessError> {
        p13_get(&self.client(node), "/v1/cluster/status").await
    }
}

// ------------------------------------------------------------------------------------
// REST client
// ------------------------------------------------------------------------------------

/// A run acknowledged by the API (201/200 with a run id).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Ack {
    /// The run id string.
    pub run_id: String,
    /// Its partition (embedded in the run id).
    pub partition: u16,
}

impl Ack {
    /// The parsed run id.
    pub fn id(&self) -> RunId {
        self.run_id.parse().expect("acked run ids parse")
    }
}

/// Repository root (`crates/ostro-repl-conformance/../..`).
fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Build the `guests/` workspace once per process (into `target/guests`, like the server
/// tests) and return the named guest's `.wasm`.
pub fn guest_wasm(name: &str) -> Result<Vec<u8>, HarnessError> {
    static BUILT: OnceLock<Result<(), String>> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    let built = BUILT.get_or_init(|| {
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
            .map_err(|e| format!("spawn cargo for the guests build: {e}"))?;
        if out.status.success() {
            Ok(())
        } else {
            Err(format!(
                "guests build failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            ))
        }
    });
    built.clone().map_err(HarnessError::Other)?;
    let path = target_dir
        .join("wasm32-wasip2/release")
        .join(format!("{}.wasm", name.replace('-', "_")));
    std::fs::read(&path).map_err(|e| other(&format!("read {}", path.display()), e))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Terminal run statuses of `GET /v1/runs/{id}`.
pub const TERMINAL: [&str; 5] = [
    "completed",
    "failed",
    "cancelled",
    "terminated",
    "timed_out",
];

impl ApiClient {
    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// `GET path`: `(status, body)` (`Null` for an empty or non-JSON body).
    pub async fn get_json(&self, path: &str) -> Result<(reqwest::StatusCode, Value), HarnessError> {
        let r = self
            .http
            .get(self.url(path))
            .send()
            .await
            .map_err(|e| other(&format!("GET {path}"), e))?;
        let st = r.status();
        let b = r.bytes().await.map_err(|e| other("body", e))?;
        Ok((st, serde_json::from_slice(&b).unwrap_or(Value::Null)))
    }

    /// `POST path` with a JSON body: `(status, body)` (`Null` for an empty or non-JSON
    /// body). `Err` only when the request did not get an HTTP answer.
    pub async fn post_json(
        &self,
        path: &str,
        body: &Value,
    ) -> Result<(reqwest::StatusCode, Value), HarnessError> {
        let r = self
            .http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .map_err(|e| other(&format!("POST {path}"), e))?;
        let st = r.status();
        let b = r.bytes().await.map_err(|e| other("body", e))?;
        Ok((st, serde_json::from_slice(&b).unwrap_or(Value::Null)))
    }

    /// Whether `/readyz` answers 200.
    pub async fn ready(&self) -> bool {
        matches!(self.get_json("/readyz").await, Ok((st, _)) if st.is_success())
    }

    /// Deploy the `smoke` guest (workflow [`TEST_WORKFLOW`]); returns its sha256.
    pub async fn deploy_test_module(&self) -> Result<[u8; 32], HarnessError> {
        use sha2::{Digest, Sha256};
        let wasm = guest_wasm("smoke")?;
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let r = self
            .http
            .put(self.url(&format!("/v1/modules/{}", hex(&sha))))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .map_err(|e| other("PUT module", e))?;
        if !r.status().is_success() {
            return Err(HarnessError::Other(format!(
                "deploy test module: {}",
                r.status()
            )));
        }
        Ok(sha)
    }

    /// `POST /v1/workflows/{workflow}/runs`; an acknowledged run (201 new / 200 existing).
    pub async fn start_run(
        &self,
        workflow: &str,
        input: Value,
        idempotency_key: Option<&str>,
    ) -> Result<Ack, HarnessError> {
        let mut body = json!({ "input": input });
        if let Some(k) = idempotency_key {
            body["idempotency_key"] = json!(k);
        }
        let r = self
            .http
            .post(self.url(&format!("/v1/workflows/{workflow}/runs")))
            .json(&body)
            .send()
            .await
            .map_err(|e| other("POST run", e))?;
        let st = r.status();
        let v: Value = r.json().await.unwrap_or(Value::Null);
        if !st.is_success() {
            return Err(HarnessError::Other(format!("start {workflow}: {st} {v}")));
        }
        let run_id = v["run_id"]
            .as_str()
            .ok_or_else(|| HarnessError::Other(format!("start {workflow}: no run_id in {v}")))?
            .to_owned();
        let id: RunId = run_id
            .parse()
            .map_err(|e| other(&format!("run id {run_id}"), e))?;
        Ok(Ack {
            run_id,
            partition: id.partition().0,
        })
    }

    /// Poll `GET /v1/runs/{id}` until a terminal status (returned) or `limit`.
    pub async fn wait_run(&self, run_id: &str, limit: Duration) -> Result<String, HarnessError> {
        // clock-exempt: test timeout
        let deadline = tokio::time::Instant::now() + limit;
        loop {
            if let Ok((st, v)) = self.get_json(&format!("/v1/runs/{run_id}")).await
                && st.is_success()
                && let Some(s) = v["status"].as_str()
                && TERMINAL.contains(&s)
            {
                return Ok(s.to_owned());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(HarnessError::Other(format!(
                    "run {run_id} not terminal within {limit:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The node's partition count (`GET /v1/partitions`).
    pub async fn partition_count(&self) -> Result<u16, HarnessError> {
        let (st, v) = self.get_json("/v1/partitions").await?;
        let n = v["partitions"]
            .as_array()
            .filter(|_| st.is_success())
            .ok_or_else(|| HarnessError::Other(format!("GET /v1/partitions: {st} {v}")))?
            .len();
        u16::try_from(n).map_err(|e| other("partition count", e))
    }

    /// Start `k` runs of [`TEST_WORKFLOW`] routed to partition `p` (keyed routing: each
    /// idempotency key is chosen so that `partition_for_key(workflow, key, P) == p`).
    pub async fn append_workload(&self, p: u16, k: usize) -> Result<Vec<Ack>, HarnessError> {
        let count = PartitionCount::new(self.partition_count().await?)
            .ok_or_else(|| HarnessError::Other("zero partitions".into()))?;
        if p >= count.get() {
            return Err(HarnessError::Other(format!(
                "partition {p} >= {}",
                count.get()
            )));
        }
        let nonce = uuid_like();
        let mut acks = Vec::with_capacity(k);
        let mut i = 0u64;
        while acks.len() < k {
            let key = format!("wl-{nonce}-{i}");
            i += 1;
            let target =
                ostro_types::routing::partition_for_key(TEST_WORKFLOW, key.as_bytes(), count);
            if target.0 != p {
                continue;
            }
            let ack = self
                .start_run(TEST_WORKFLOW, json!({ "k": key }), Some(&key))
                .await?;
            if ack.partition != p {
                return Err(HarnessError::Other(format!(
                    "keyed start for partition {p} landed on {}",
                    ack.partition
                )));
            }
            acks.push(ack);
        }
        Ok(acks)
    }
}

/// A process-unique token for workload keys (no RNG dependency on the call site).
fn uuid_like() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    // clock-exempt: uniqueness salt only, never used for logic
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!(
        "{t:x}-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

// ------------------------------------------------------------------------------------
// Offline metadata Raft log reader
// ------------------------------------------------------------------------------------

/// A node's metadata Raft log, decoded offline.
#[derive(Clone, Debug)]
pub struct MetaLogDump {
    /// `ConfState.voters` (the state file's, else the snapshot's).
    pub voters: Vec<u64>,
    /// `ConfState.learners`.
    pub learners: Vec<u64>,
    /// `HardState.commit`.
    pub commit: u64,
    /// Index of the stored snapshot (0 = none). Entries at or below it are compacted away.
    pub snapshot_index: u64,
    /// Every committed normal entry after the snapshot, with the recomputed verdict.
    pub entries: Vec<MetaLogEntry>,
    /// Committed entries whose payload did not decode as a `MetaCommand` (index list).
    pub undecodable: Vec<u64>,
    /// The snapshot's state (what `entries` continue from; `election::isr_only_from`).
    pub snapshot_state: MetaState,
    /// The state after replaying the snapshot and every committed entry.
    pub state: MetaState,
}

const STATE_MAGIC: &[u8; 4] = b"CRS1";
const SNAPSHOT_MAGIC: &[u8; 4] = b"CRN1";
const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;

/// CRC-32 (IEEE), as the vendored log store writes it (`crc32fast`).
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

/// One protobuf field value.
#[derive(Clone, Copy, Debug)]
enum Pb<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
    Fixed,
}

fn varint(b: &[u8], pos: &mut usize) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *b.get(*pos)?;
        *pos += 1;
        v |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

/// Decode a protobuf message into `(field number, value)` pairs (wire types 0, 1, 2, 5).
fn pb_fields(b: &[u8]) -> Option<Vec<(u64, Pb<'_>)>> {
    let mut pos = 0;
    let mut out = Vec::new();
    while pos < b.len() {
        let key = varint(b, &mut pos)?;
        let (field, wire) = (key >> 3, key & 7);
        let v = match wire {
            0 => Pb::Varint(varint(b, &mut pos)?),
            1 => {
                pos = pos.checked_add(8).filter(|p| *p <= b.len())?;
                Pb::Fixed
            }
            2 => {
                let len = usize::try_from(varint(b, &mut pos)?).ok()?;
                let end = pos.checked_add(len).filter(|e| *e <= b.len())?;
                let s = &b[pos..end];
                pos = end;
                Pb::Bytes(s)
            }
            5 => {
                pos = pos.checked_add(4).filter(|p| *p <= b.len())?;
                Pb::Fixed
            }
            _ => return None,
        };
        out.push((field, v));
    }
    Some(out)
}

/// A repeated uint64 field (packed or not).
fn pb_u64s(fields: &[(u64, Pb<'_>)], field: u64) -> Option<Vec<u64>> {
    let mut out = Vec::new();
    for (f, v) in fields {
        if *f != field {
            continue;
        }
        match v {
            Pb::Varint(x) => out.push(*x),
            Pb::Bytes(b) => {
                let mut pos = 0;
                while pos < b.len() {
                    out.push(varint(b, &mut pos)?);
                }
            }
            Pb::Fixed => return None,
        }
    }
    Some(out)
}

fn pb_u64(fields: &[(u64, Pb<'_>)], field: u64) -> u64 {
    fields
        .iter()
        .rev()
        .find_map(|(f, v)| match v {
            Pb::Varint(x) if *f == field => Some(*x),
            _ => None,
        })
        .unwrap_or(0)
}

fn pb_bytes<'a>(fields: &[(u64, Pb<'a>)], field: u64) -> &'a [u8] {
    fields
        .iter()
        .rev()
        .find_map(|(f, v)| match v {
            Pb::Bytes(b) if *f == field => Some(*b),
            _ => None,
        })
        .unwrap_or(&[])
}

/// Read a `magic | [len u64] | body | crc32` file; `None` when absent.
fn read_framed(
    path: &Path,
    magic: &[u8; 4],
    len_bytes: usize,
) -> Result<Option<Vec<u8>>, HarnessError> {
    let buf = match std::fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(other(&format!("read {}", path.display()), e)),
    };
    if buf.len() < 8 + len_bytes || &buf[..4] != magic {
        return Err(HarnessError::Other(format!(
            "{} is not a valid file",
            path.display()
        )));
    }
    let (content, crc) = buf.split_at(buf.len() - 4);
    let stored = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
    if crc32(content) != stored {
        return Err(HarnessError::Other(format!(
            "{} failed its checksum",
            path.display()
        )));
    }
    Ok(Some(content[4 + len_bytes..].to_vec()))
}

struct RawEntry {
    entry_type: u64,
    index: u64,
    data: Vec<u8>,
}

/// Records of one segment up to the first torn / bad one.
fn read_segment(path: &Path) -> Result<Vec<RawEntry>, HarnessError> {
    let buf = std::fs::read(path).map_err(|e| other(&format!("read {}", path.display()), e))?;
    let mut out = Vec::new();
    let mut pos = 0usize;
    while buf.len() - pos >= 8 {
        let len = u32::from_le_bytes([buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]]) as usize;
        let crc = u32::from_le_bytes([buf[pos + 4], buf[pos + 5], buf[pos + 6], buf[pos + 7]]);
        let start = pos + 8;
        if len > MAX_RECORD_BYTES || buf.len() - start < len {
            break;
        }
        let payload = &buf[start..start + len];
        if crc32(payload) != crc {
            break;
        }
        let f = pb_fields(payload)
            .ok_or_else(|| HarnessError::Other(format!("{}: undecodable entry", path.display())))?;
        out.push(RawEntry {
            entry_type: pb_u64(&f, 1),
            index: pb_u64(&f, 3),
            data: pb_bytes(&f, 4).to_vec(),
        });
        pos = start + len;
    }
    Ok(out)
}

/// Decode the metadata Raft log in `meta_dir` (the vendored log store's `state`,
/// `snapshot` and `log/<index>.seg` files) without opening or modifying it, and replay a
/// fresh `MetaState` over the snapshot and every committed entry.
pub fn read_meta_log(meta_dir: &Path) -> Result<MetaLogDump, HarnessError> {
    let (mut voters, mut learners, mut commit) = (Vec::new(), Vec::new(), 0);
    if let Some(body) = read_framed(&meta_dir.join("state"), STATE_MAGIC, 0)? {
        let bad = || HarnessError::Other(format!("{}: truncated state", meta_dir.display()));
        let take = |pos: &mut usize, n: usize| -> Result<&[u8], HarnessError> {
            let s = body.get(*pos..*pos + n).ok_or_else(bad)?;
            *pos += n;
            Ok(s)
        };
        let mut pos = 0;
        let len = |s: &[u8]| u32::from_le_bytes([s[0], s[1], s[2], s[3]]) as usize;
        let hs_len = len(take(&mut pos, 4)?);
        let hs = pb_fields(take(&mut pos, hs_len)?).ok_or_else(bad)?;
        commit = pb_u64(&hs, 3);
        let cs_len = len(take(&mut pos, 4)?);
        let cs = pb_fields(take(&mut pos, cs_len)?).ok_or_else(bad)?;
        voters = pb_u64s(&cs, 1).ok_or_else(bad)?;
        learners = pb_u64s(&cs, 2).ok_or_else(bad)?;
    }
    let mut state = MetaState::default();
    let mut snapshot_index = 0;
    if let Some(body) = read_framed(&meta_dir.join("snapshot"), SNAPSHOT_MAGIC, 8)? {
        let bad = || HarnessError::Other(format!("{}: bad snapshot", meta_dir.display()));
        let snap = pb_fields(&body).ok_or_else(bad)?;
        let meta = pb_fields(pb_bytes(&snap, 2)).ok_or_else(bad)?;
        snapshot_index = pb_u64(&meta, 2);
        if voters.is_empty() {
            let cs = pb_fields(pb_bytes(&meta, 1)).ok_or_else(bad)?;
            voters = pb_u64s(&cs, 1).ok_or_else(bad)?;
            learners = pb_u64s(&cs, 2).ok_or_else(bad)?;
        }
        let data = pb_bytes(&snap, 1);
        if snapshot_index > 0 && !data.is_empty() {
            state = decode_snapshot(data).map_err(|e| other("meta snapshot", e))?;
        }
    }
    let mut segs: Vec<(u64, PathBuf)> = Vec::new();
    match std::fs::read_dir(meta_dir.join("log")) {
        Ok(rd) => {
            for d in rd {
                let path = d.map_err(|e| other("read meta log dir", e))?.path();
                if let Some(first) = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .and_then(|n| n.strip_suffix(".seg"))
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    segs.push((first, path));
                }
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(other("read meta log dir", e)),
    }
    segs.sort();
    // Later records replace earlier ones at the same index (a conflicting suffix is cut on
    // disk first, so this only matters for a torn read racing an append).
    let mut by_index: BTreeMap<u64, RawEntry> = BTreeMap::new();
    for (_, path) in &segs {
        for e in read_segment(path)? {
            by_index.insert(e.index, e);
        }
    }
    let snapshot_state = state.clone();
    let mut entries = Vec::new();
    let mut undecodable = Vec::new();
    // A snapshot taken at the commit index (`create_snapshot` allows `index <= commit`)
    // leaves nothing to replay: the snapshot state is the committed state.
    let replay = if snapshot_index < commit {
        by_index.range(snapshot_index + 1..=commit)
    } else {
        by_index.range(0..0)
    };
    for (index, e) in replay {
        // EntryNormal only; an empty entry is a new leader's no-op.
        if e.entry_type != 0 || e.data.is_empty() {
            continue;
        }
        match decode_command(&e.data) {
            Ok(command) => {
                let applied_result = state.apply(*index, command.clone());
                entries.push(MetaLogEntry {
                    index: *index,
                    command,
                    applied_result,
                });
            }
            Err(_) => undecodable.push(*index),
        }
    }
    Ok(MetaLogDump {
        voters,
        learners,
        commit,
        snapshot_index,
        entries,
        undecodable,
        snapshot_state,
        state,
    })
}
