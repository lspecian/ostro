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
//! **Until 12-14 wires that hook, the server drops the transport**, so the nodes of a
//! multi-node spec start as independent single-node clusters (each its own meta voter, every
//! partition RF=1). That is what makes the RF>1 scenarios of 12-06/12-07 red with the
//! measured `cluster_formed` failure. Raw channels from [`InProcCluster::channel_as`] then
//! reach nobody (the node never takes its listener), and calls fail at their deadline.
//!
//! `kill(n)` is "isolate + graceful stop" (data dir kept), not a real crash (research
//! Pitfall 8); real `kill -9` is tier C (12-09). `restart(n)` starts the node again from
//! the same dirs and config with a fresh transport and heals its links.
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
) -> Result<ServerHandle, HarnessError> {
    let node = cfg.node.node_id;
    let opts = ServerOptions::default().with_cluster_transport(transport);
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
        cfg
    }
}

impl ClusterHarness for InProcCluster {
    async fn start(spec: ClusterSpec) -> Result<Self, HarnessError> {
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
            let cfg = c.node_config(*id, &ca);
            boots.push(boot(cfg.clone(), c.net.transport(*id)));
            c.nodes.insert(
                *id,
                NodeSlot {
                    cfg,
                    handle: None,
                    api: SocketAddr::from(([127, 0, 0, 1], 0)),
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
        let h = boot(cfg, self.net.transport(node)).await?;
        let slot = self.nodes.get_mut(&node).expect("slot");
        slot.api = h.local_addr();
        slot.handle = Some(h);
        self.net.faults().heal(node);
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
    let mut entries = Vec::new();
    let mut undecodable = Vec::new();
    for (index, e) in by_index.range(snapshot_index + 1..=commit) {
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
        state,
    })
}
