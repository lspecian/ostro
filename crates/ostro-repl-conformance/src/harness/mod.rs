//! The cluster harness API (12-02). Tier B ([`inproc`], 12-03) runs N in-process servers
//! over an in-memory transport with a seeded fault layer; tier C ([`process`], 12-09) runs
//! N `ostro` processes behind a TCP chaos proxy. Scenarios and oracles code against
//! [`ClusterHarness`] and the plain types here.

pub mod certs;
pub mod chaos_proxy;
pub mod fault;
pub mod inproc;
// Phase 13 (13-02): the P13Harness API (implemented by 13-08).
pub mod p13;
pub mod process;

use std::future::Future;
use std::path::PathBuf;

use ostro_meta::{MetaCommand, MetaResult};
use ostro_partition::roles::PartitionAssignmentView;

/// What cluster a scenario needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterSpec {
    /// Number of nodes (ids `1..=nodes`).
    pub nodes: usize,
    /// `[replication] rf` seed.
    pub rf: usize,
    /// `[replication] min_insync` seed.
    pub min_insync: usize,
    /// `[node] partitions` (default 4).
    pub partitions: u16,
    /// Share one LocalFs bucket as every node's `[backup] url`.
    pub objstore: bool,
    /// Seed of the fault layer and workloads.
    pub seed: u64,
    /// Rotate every node's WAL segments at this size (`None` = the 250 MB default), so
    /// retention can trim a small test log (bootstrap scenarios).
    pub wal_rotation_bytes: Option<u64>,
}

impl Default for ClusterSpec {
    fn default() -> Self {
        Self {
            nodes: 3,
            rf: 3,
            min_insync: 2,
            partitions: 4,
            objstore: false,
            seed: 0,
            wal_rotation_bytes: None,
        }
    }
}

impl ClusterSpec {
    /// A single RF=1 node.
    pub fn single() -> Self {
        Self {
            nodes: 1,
            rf: 1,
            min_insync: 1,
            ..Self::default()
        }
    }
}

/// A harness failure: never a valid red result and never evidence that a mutant bites (W11).
#[derive(Debug, thiserror::Error)]
pub enum HarnessError {
    /// The cluster could not be started / restarted.
    #[error("harness start: {0}")]
    Start(String),
    /// The scenario ran past its deadline.
    #[error("scenario deadline exceeded after {0:?}")]
    Deadline(std::time::Duration),
    /// The scenario body panicked.
    #[error("scenario panicked: {0}")]
    Panic(String),
    /// Any other harness-side failure (I/O, HTTP, decode).
    #[error("harness: {0}")]
    Other(String),
}

/// REST client of one node's API listener (methods added by 12-03: deploy, start/wait
/// runs, workloads routed to a partition).
#[derive(Clone, Debug)]
pub struct ApiClient {
    /// `http://<api addr>`.
    pub base_url: String,
    /// The HTTP client.
    pub http: reqwest::Client,
}

/// A node's data directory, readable offline (and best-effort while the node runs).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataDirView {
    /// The node id.
    pub node: u64,
    /// `[node] data_dir`.
    pub root: PathBuf,
}

/// One committed metadata Raft entry, including proposals apply rejected.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaLogEntry {
    /// Raft log index.
    pub index: u64,
    /// The decoded command.
    pub command: MetaCommand,
    /// The apply verdict (recomputed by replaying a fresh state machine).
    pub applied_result: MetaResult,
}

/// A running test cluster.
pub trait ClusterHarness {
    /// Start a cluster for `spec`.
    fn start(spec: ClusterSpec) -> impl Future<Output = Result<Self, HarnessError>>
    where
        Self: Sized;

    /// Every node id, ascending.
    fn node_ids(&self) -> Vec<u64>;

    /// REST client of `node`.
    fn client(&self, node: u64) -> ApiClient;

    /// The current meta assignment of partition `p`.
    fn assignment(&self, p: u16) -> impl Future<Output = PartitionAssignmentView>;

    /// Cut every link to and from `node`.
    fn isolate(&mut self, node: u64);

    /// Restore every link of `node`.
    fn heal(&mut self, node: u64);

    /// Cut the links between `a` and `b`.
    fn partition(&mut self, a: u64, b: u64);

    /// Stop `node` (tier B: isolate + stop; tier C: `kill -9`). Its data dir is kept.
    fn kill(&mut self, node: u64) -> impl Future<Output = ()>;

    /// Restart `node` from its data dir and config.
    fn restart(&mut self, node: u64) -> impl Future<Output = Result<(), HarnessError>>;

    /// `node`'s data directory.
    fn node_disk(&self, node: u64) -> DataDirView;

    /// The shared object-store bucket dir (`spec.objstore`).
    fn bucket(&self) -> Option<PathBuf>;

    /// Every committed meta entry on `node`.
    fn meta_log(&self, node: u64) -> impl Future<Output = Vec<MetaLogEntry>>;

    /// `GET /v1/admin/replication` on `node` (`Value::Null` before 12-14).
    fn admin_status(&self, node: u64) -> impl Future<Output = serde_json::Value>;

    /// Stop every node.
    fn stop(self) -> impl Future<Output = ()>;
}
