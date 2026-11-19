//! In-process harness (plan 04-16): a `Node` whose dispatcher runs the real
//! `EngineExecutor` over a shared engine `Runtime`, plus module install, run start and
//! journal/result inspection helpers. No API yet: runs are started with `StartRun` on
//! `partition_for_new_run`.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::{Path, PathBuf};
use std::process::Command as Process;
use std::sync::atomic::Ordering;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use bytes::Bytes;
use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_log::Rf1Log;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{
    ApplyOutcome, DispatcherConfig, DispatcherStats, Executor, Node, NodeConfig, PartitionHandle,
    query,
};
use ostro_server::executor::{EngineExecutor, ExecutorConfig, ExecutorHooks, JobRecord};
use ostro_server::journal_io::read_journal;
use ostro_types::command::{ConflictPolicy, StartRun};
use ostro_types::journal::{Event, EventKind};
use ostro_types::{Command, PartitionCount, PartitionId, PayloadRef, RunId, partition_for_new_run};
use tokio::sync::Semaphore;
use tokio::time::{Instant, sleep, timeout};

/// Generous default timeout for harness operations.
pub const T: Duration = Duration::from_secs(20);

/// Repository root (`crates/ostro-server/../..`).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Build the `guests/` workspace once per process and return the named guest's `.wasm`
/// (identical to ostro-engine's helper).
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Process::new(cargo)
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
            .expect("failed to spawn cargo for guests build");
        if !out.status.success() {
            panic!(
                "guests build failed:\n{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
    });
    let file = format!("{}.wasm", name.replace('-', "_"));
    let path = target_dir.join("wasm32-wasip2/release").join(&file);
    std::fs::read(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Harness knobs.
#[derive(Clone, Debug)]
pub struct Opts {
    pub partitions: u16,
    /// Engine hot-wait window.
    pub hot_wait: Duration,
    /// The dispatcher's lease TTL (the executor's view is `ExecutorConfig::lease_ttl`).
    pub dispatcher_lease_ttl: Duration,
    /// Node-wide executor slots.
    pub slots: usize,
    pub exec: ExecutorConfig,
}

impl Opts {
    /// P=4, hot_wait 2 s, lease 30 s (heartbeat 10 s), 3 attempts, 50 ms retry backoff.
    pub fn new() -> Self {
        let mut exec = ExecutorConfig::for_lease_ttl(Duration::from_secs(30));
        exec.retry_backoff = Duration::from_millis(50);
        Self {
            partitions: 4,
            hot_wait: Duration::from_secs(2),
            dispatcher_lease_ttl: Duration::from_secs(30),
            slots: 64,
            exec,
        }
    }

    pub fn hot_wait(mut self, d: Duration) -> Self {
        self.hot_wait = d;
        self
    }

    /// Same TTL for dispatcher and executor; heartbeat ttl/4, margin ttl/8.
    pub fn lease(mut self, ttl: Duration) -> Self {
        self.dispatcher_lease_ttl = ttl;
        self.exec.lease_ttl = ttl;
        self.exec.heartbeat = ttl / 4;
        self.exec.self_fence_margin = ttl / 8;
        self
    }
}

impl Default for Opts {
    fn default() -> Self {
        Self::new()
    }
}

/// Engine runtime with the given hot-wait window (system clock).
pub fn runtime(hot_wait: Duration) -> Arc<Runtime> {
    let cfg = EngineConfig {
        hot_wait,
        ..EngineConfig::default()
    };
    Arc::new(
        Runtime::new(
            cfg,
            RuntimeOptions {
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

/// A node + dispatcher + engine executor.
pub struct TestNode {
    pub dir: PathBuf,
    pub node: Node,
    pub runtime: Arc<Runtime>,
    pub hooks: Arc<ExecutorHooks>,
    pub stats: Arc<DispatcherStats>,
    pub opts: Opts,
}

impl TestNode {
    /// Open a node under `dir` with a fresh runtime.
    pub async fn open(dir: &Path, opts: Opts) -> Self {
        let rt = runtime(opts.hot_wait);
        Self::open_with(dir, opts, rt, Arc::new(ExecutorHooks::default())).await
    }

    /// Open (or reopen) a node under `dir` reusing `runtime` and `hooks`.
    pub async fn open_with(
        dir: &Path,
        opts: Opts,
        runtime: Arc<Runtime>,
        hooks: Arc<ExecutorHooks>,
    ) -> Self {
        let exec = EngineExecutor::new(Arc::clone(&runtime), opts.exec.clone())
            .with_hooks(Arc::clone(&hooks));
        let executor: Arc<dyn Executor> = Arc::new(exec);
        let stats = Arc::new(DispatcherStats::default());
        let mut cfg = NodeConfig::new(
            dir.to_path_buf(),
            PartitionCount::new(opts.partitions).expect("non-zero"),
        );
        cfg.leader.dispatcher = Some(DispatcherConfig {
            executor,
            lease_ttl: opts.dispatcher_lease_ttl,
            slots: Arc::new(Semaphore::new(opts.slots)),
            stats: Some(Arc::clone(&stats)),
        });
        let node = timeout(T, Node::open(cfg))
            .await
            .expect("node open timed out")
            .expect("node open");
        Self {
            dir: dir.to_path_buf(),
            node,
            runtime,
            hooks,
            stats,
            opts,
        }
    }

    /// Shut the node down (dispatcher revokes leases), keeping runtime + hooks for a reopen.
    pub async fn stop(self) -> (Arc<Runtime>, Arc<ExecutorHooks>, Opts) {
        let TestNode {
            node,
            runtime,
            hooks,
            opts,
            ..
        } = self;
        timeout(T, node.shutdown())
            .await
            .expect("shutdown timed out")
            .expect("shutdown");
        (runtime, hooks, opts)
    }

    pub fn count(&self) -> PartitionCount {
        PartitionCount::new(self.opts.partitions).expect("non-zero")
    }

    pub fn handle(&self, p: PartitionId) -> PartitionHandle<Rf1Log> {
        self.node.handle(p).expect("partition ready").clone()
    }

    pub fn handle_of(&self, run: &RunId) -> PartitionHandle<Rf1Log> {
        self.handle(run.partition())
    }

    /// Store `wasm` in the node's blob store and prepare it (deploy-time compile).
    pub async fn install_module(&self, wasm: &[u8]) -> [u8; 32] {
        let blob = self
            .handle(PartitionId(0))
            .blobs()
            .put(wasm)
            .expect("blob put");
        let p = self.runtime.prepare_cached(wasm).await.expect("prepare");
        assert_eq!(p.sha, blob.sha256);
        blob.sha256
    }

    /// Store `wasm` in the blob store only (the executor must load + prepare it).
    pub fn put_blob(&self, wasm: &[u8]) -> [u8; 32] {
        self.handle(PartitionId(0))
            .blobs()
            .put(wasm)
            .expect("blob put")
            .sha256
    }

    /// Start `workflow`@`version` with `input` pinned to `module_sha`.
    pub async fn start(
        &self,
        workflow: &str,
        version: u32,
        input: &[u8],
        module_sha: &[u8; 32],
    ) -> RunId {
        let uuid = RunId::new_v7(PartitionId(0)).uuid();
        let p = partition_for_new_run(&uuid, self.count());
        let run = RunId::new(p, uuid);
        let now = now_ms();
        let o = timeout(
            T,
            self.handle(p).submit_one(Command::start_run(StartRun {
                run_id: run.as_bytes().to_vec(),
                workflow: workflow.into(),
                workflow_version: version,
                queue: "default".into(),
                input: Some(PayloadRef::inline(Bytes::copy_from_slice(input))),
                conflict_policy: ConflictPolicy::ReturnExisting as i32,
                ready_at_ms: now,
                requested_at_ms: now,
                rng_seed: uuid.as_bytes().repeat(2),
                module_sha: module_sha.to_vec(),
                ..Default::default()
            })),
        )
        .await
        .expect("start timed out")
        .expect("start");
        assert_eq!(o, ApplyOutcome::Accepted);
        run
    }

    pub fn record(&self, run: &RunId) -> RunRecord {
        self.handle_of(run)
            .read_run(run)
            .expect("read run")
            .expect("run exists")
    }

    /// Wait until `run` is terminal.
    pub async fn wait_terminal(&self, run: &RunId, limit: Duration) -> RunRecord {
        self.wait_until(run, limit, |r| r.status.is_terminal())
            .await
    }

    /// Wait until `f(record)` holds.
    pub async fn wait_until(
        &self,
        run: &RunId,
        limit: Duration,
        f: impl Fn(&RunRecord) -> bool,
    ) -> RunRecord {
        let deadline = Instant::now() + limit;
        loop {
            let r = self.record(run);
            if f(&r) {
                return r;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting on run {run}: status {:?}, next_seq {}, attempt {:?}; jobs {:?}",
                r.status,
                r.next_seq,
                r.current_attempt,
                self.jobs_of(run)
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait until the journal holds an event of `kind`.
    pub async fn wait_event(&self, run: &RunId, kind: EventKind, limit: Duration) {
        let deadline = Instant::now() + limit;
        loop {
            if count(&self.journal(run).await, kind) > 0 {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {kind:?} on run {run}"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn journal(&self, run: &RunId) -> Vec<Event> {
        read_journal(&self.handle_of(run), run)
            .await
            .expect("read journal")
    }

    /// The stored run result bytes.
    pub async fn result(&self, run: &RunId) -> Option<Vec<u8>> {
        let h = self.handle_of(run);
        let p = query::run_payload(&h.reader(), run, query::PAYLOAD_RESULT).expect("payload")?;
        Some(h.fetch_payload(&p).await.expect("fetch result").to_vec())
    }

    pub fn jobs_of(&self, run: &RunId) -> Vec<JobRecord> {
        self.hooks
            .jobs()
            .iter()
            .filter(|j| j.run == *run)
            .cloned()
            .collect()
    }

    /// Wait until a job of `run` matching `f` was recorded.
    pub async fn wait_job(
        &self,
        run: &RunId,
        limit: Duration,
        f: impl Fn(&JobRecord) -> bool,
    ) -> JobRecord {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(j) = self.jobs_of(run).into_iter().find(|j| f(j)) {
                return j;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for a job of {run}; jobs {:?}",
                self.jobs_of(run)
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub fn activations(&self) -> u64 {
        self.hooks.activations.load(Ordering::Relaxed)
    }
}

pub fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("ms")
}

pub fn count(journal: &[Event], kind: EventKind) -> usize {
    journal.iter().filter(|e| e.body.kind() == kind).count()
}

pub fn rref(run: &RunId) -> RunRef {
    RunRef {
        partition: run.partition().0,
        run_id: run.to_string(),
    }
}

pub fn is_terminal_completed(r: &RunRecord) -> bool {
    r.status == RunStatus::Completed
}

pub fn evicted_external(a: &Option<ActivationOutcome>) -> bool {
    matches!(
        a,
        Some(ActivationOutcome::Evicted {
            reason: ostro_engine::error::EvictReason::External
        })
    )
}

// ---------------------------------------------------------------------------------
// Full-server harness (plan 04-17): the real `Server` on 127.0.0.1:0 over a tmpfs data
// dir, with REST (reqwest) and gRPC (tonic) clients on the one port.
// ---------------------------------------------------------------------------------

use ostro_server::config::ServerConfig;
use ostro_server::executor::ExecutorHooks as ServerHooks;
use ostro_server::{ServerError, ServerHandle};
use tempfile::TempDir;

/// A fresh test directory on `/dev/shm` when writable, else the OS temp dir.
pub fn tmpfs_dir() -> TempDir {
    let shm = Path::new("/dev/shm");
    if shm.is_dir()
        && let Ok(d) = tempfile::Builder::new()
            .prefix("ostro-test-")
            .tempdir_in(shm)
    {
        return d;
    }
    tempfile::Builder::new()
        .prefix("ostro-test-")
        .tempdir()
        .expect("tempdir")
}

/// Full-server knobs.
#[derive(Clone, Debug)]
pub struct ServerOpts {
    pub partitions: u16,
    pub ephemeral: bool,
}

impl Default for ServerOpts {
    /// P=4 (fast opens), persistent tmpfs dir.
    fn default() -> Self {
        Self {
            partitions: 4,
            ephemeral: false,
        }
    }
}

/// `server_defaults()` on 127.0.0.1:0 with the given data dir and options.
pub fn server_config(dir: &Path, opts: &ServerOpts) -> ServerConfig {
    let mut cfg = ServerConfig::server_defaults();
    cfg.api.listen = "127.0.0.1:0".parse().expect("addr");
    cfg.node.data_dir = dir.to_path_buf();
    cfg.node.partitions = opts.partitions;
    cfg.node.ephemeral = opts.ephemeral;
    cfg
}

/// A running in-process server plus clients. `restart()` keeps the data dir.
pub struct TestServer {
    pub handle: Option<ServerHandle>,
    pub cfg: ServerConfig,
    pub hooks: Arc<ServerHooks>,
    pub http: reqwest::Client,
    /// Owns the data dir across restarts (None in ephemeral mode).
    pub dir: Option<TempDir>,
}

impl TestServer {
    /// Start on a fresh tmpfs dir.
    pub async fn start(opts: ServerOpts) -> Self {
        let dir = tmpfs_dir();
        let cfg = server_config(dir.path(), &opts);
        Self::start_cfg(cfg, Some(dir)).await
    }

    /// Start with an explicit config (the data dir is owned by `dir` when given).
    pub async fn start_cfg(cfg: ServerConfig, dir: Option<TempDir>) -> Self {
        let hooks = Arc::new(ServerHooks::default());
        let handle = Self::boot(cfg.clone(), Arc::clone(&hooks))
            .await
            .expect("server start");
        Self {
            handle: Some(handle),
            cfg,
            hooks,
            http: reqwest::Client::new(),
            dir,
        }
    }

    pub async fn boot(
        cfg: ServerConfig,
        hooks: Arc<ServerHooks>,
    ) -> Result<ServerHandle, ServerError> {
        timeout(T, ostro_server::server::start_with_hooks(cfg, hooks))
            .await
            .expect("server start timed out")
    }

    pub fn h(&self) -> &ServerHandle {
        self.handle.as_ref().expect("server running")
    }

    pub fn addr(&self) -> std::net::SocketAddr {
        self.h().local_addr()
    }

    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr())
    }

    /// A gRPC channel to the one port.
    pub async fn channel(&self) -> tonic::transport::Channel {
        tonic::transport::Channel::from_shared(format!("http://{}", self.addr()))
            .expect("uri")
            .connect()
            .await
            .expect("grpc connect")
    }

    /// Poll `/readyz` until 200 or `limit`.
    pub async fn wait_ready(&self, limit: Duration) -> Duration {
        let start = Instant::now();
        loop {
            if let Ok(r) = self.http.get(self.url("/readyz")).send().await
                && r.status() == reqwest::StatusCode::OK
            {
                return start.elapsed();
            }
            assert!(start.elapsed() < limit, "server not ready within {limit:?}");
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Graceful shutdown, keeping the data dir.
    pub async fn shutdown(&mut self) {
        if let Some(h) = self.handle.take() {
            timeout(T, h.shutdown())
                .await
                .expect("shutdown timed out")
                .expect("shutdown");
        }
    }

    /// Shut down and start again on the same data dir (new port).
    pub async fn restart(&mut self) {
        self.shutdown().await;
        self.http = reqwest::Client::new();
        let handle = Self::boot(self.cfg.clone(), Arc::clone(&self.hooks))
            .await
            .expect("server restart");
        self.handle = Some(handle);
    }
}

impl TestServer {
    /// Start on a fresh tmpfs dir with `tweak` applied to the config (plan 04-20:
    /// engine hot_wait, executor lease/heartbeat/slots/backoff).
    pub async fn start_with(opts: ServerOpts, tweak: impl FnOnce(&mut ServerConfig)) -> Self {
        let dir = tmpfs_dir();
        let mut cfg = server_config(dir.path(), &opts);
        tweak(&mut cfg);
        Self::start_cfg(cfg, Some(dir)).await
    }

    /// The partition handle owning `run`.
    pub fn handle_of(&self, run: &RunId) -> PartitionHandle<Rf1Log> {
        self.h()
            .node()
            .handle(run.partition())
            .expect("partition ready")
            .clone()
    }

    /// The run's journal, read straight from the partition.
    pub async fn journal(&self, run: &RunId) -> Vec<Event> {
        read_journal(&self.handle_of(run), run)
            .await
            .expect("read journal")
    }

    /// The stored result payload bytes of a completed run.
    pub async fn result_bytes(&self, run: &RunId) -> Option<Vec<u8>> {
        let h = self.handle_of(run);
        let p = query::run_payload(&h.reader(), run, query::PAYLOAD_RESULT).expect("payload")?;
        Some(h.fetch_payload(&p).await.expect("fetch result").to_vec())
    }

    /// Finished-job records of `run` (executor hooks).
    pub fn jobs_of(&self, run: &RunId) -> Vec<JobRecord> {
        self.hooks
            .jobs()
            .iter()
            .filter(|j| j.run == *run)
            .cloned()
            .collect()
    }
}

// ---------------------------------------------------------------------------------
// Lifecycle helpers (plan 05-13): records, status waits, history and the token a run
// published through `set_status`.
// ---------------------------------------------------------------------------------

impl TestServer {
    /// The run's record, read straight from the partition.
    pub fn run_record(&self, run: &RunId) -> RunRecord {
        self.handle_of(run)
            .read_run(run)
            .expect("read run")
            .expect("run exists")
    }

    /// Poll the record until `f` holds.
    pub async fn wait_record(
        &self,
        run: &RunId,
        limit: Duration,
        f: impl Fn(&RunRecord) -> bool,
    ) -> RunRecord {
        let deadline = Instant::now() + limit;
        loop {
            let r = self.run_record(run);
            if f(&r) {
                return r;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting on run {run}: status {:?}, next_seq {}, open_waits {}",
                r.status,
                r.next_seq,
                r.open_waits
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Poll until the run's status is `status`.
    pub async fn wait_status(&self, run: &RunId, status: RunStatus, limit: Duration) -> RunRecord {
        self.wait_record(run, limit, |r| r.status == status).await
    }

    /// The kinds of the run's journal events, in seq order.
    pub async fn history_kinds(&self, run: &RunId) -> Vec<EventKind> {
        self.journal(run)
            .await
            .iter()
            .map(|e| e.body.kind())
            .collect()
    }

    /// Poll `GET /v1/runs/{id}` until its published status carries `"token"` (the `tokens`
    /// guest publishes it with `set_status`) and return it.
    pub async fn published_token(&self, run_id: &str, limit: Duration) -> String {
        let deadline = Instant::now() + limit;
        loop {
            let body: serde_json::Value = self
                .http
                .get(self.url(&format!("/v1/runs/{run_id}")))
                .send()
                .await
                .expect("GET run")
                .json()
                .await
                .expect("json");
            if let Some(t) = body["published_status"]["token"].as_str() {
                return t.to_owned();
            }
            assert!(
                Instant::now() < deadline,
                "run {run_id} published no token: {body}"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }
}

// ---------------------------------------------------------------------------------
// Semantics e2e helpers (plan 05-15): raw starts with fields the API does not expose
// (per-run `run_timeout_ms`), WF-04 probes (in-flight jobs, durable running/timer rows)
// and process RSS.
// ---------------------------------------------------------------------------------

impl TestServer {
    /// Submit a `StartRun` straight to the owning partition of a fresh run id (default
    /// tenant, queue `default`, return-existing), with `tweak` applied last. Used for the
    /// fields the API derives from workflow metadata only (`run_timeout_ms`): the run is
    /// then executed, timed out and driven through the API like any other.
    pub async fn start_raw(
        &self,
        workflow: &str,
        version: u32,
        input: &[u8],
        module_sha: &[u8; 32],
        tweak: impl FnOnce(&mut StartRun),
    ) -> RunId {
        let count = PartitionCount::new(self.cfg.node.partitions).expect("non-zero");
        let uuid = RunId::new_v7(PartitionId(0)).uuid();
        let run = RunId::new(partition_for_new_run(&uuid, count), uuid);
        let now = now_ms();
        let mut start = StartRun {
            run_id: run.as_bytes().to_vec(),
            workflow: workflow.into(),
            workflow_version: version,
            queue: "default".into(),
            input: Some(PayloadRef::inline(Bytes::copy_from_slice(input))),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            ready_at_ms: now,
            requested_at_ms: now,
            rng_seed: uuid.as_bytes().repeat(2),
            module_sha: module_sha.to_vec(),
            ..Default::default()
        };
        tweak(&mut start);
        let o = timeout(
            T,
            self.handle_of(&run).submit_one(Command::start_run(start)),
        )
        .await
        .expect("raw start timed out")
        .expect("raw start");
        assert_eq!(o, ApplyOutcome::Accepted);
        run
    }

    /// Every partition handle of the running server.
    pub fn partitions(&self) -> Vec<PartitionHandle<Rf1Log>> {
        (0..self.cfg.node.partitions)
            .map(|p| {
                self.h()
                    .node()
                    .handle(PartitionId(p))
                    .expect("partition ready")
                    .clone()
            })
            .collect()
    }

    /// Jobs handed to the executor and not finished yet (dispatcher leases in phase
    /// *Executing*): `dispatched + reactivated - finished job records`. Retries regranted
    /// after a failure are not counted, so this is exact only for runs without retries.
    pub fn in_flight_jobs(&self) -> i64 {
        let st = self.h().dispatcher_stats();
        let started =
            st.dispatched.load(Ordering::Relaxed) + st.reactivated.load(Ordering::Relaxed);
        i64::try_from(started).expect("count")
            - i64::try_from(self.hooks.jobs().len()).expect("count")
    }

    /// Rows of the durable `running` keyspace over all partitions (granted attempts).
    pub fn running_rows(&self) -> usize {
        self.partitions()
            .iter()
            .map(|h| query::running_runs(&h.reader()).expect("running").len())
            .sum()
    }

    /// `(total, due at or before due_ms)` durable timer rows over all partitions.
    pub fn timer_rows(&self, due_ms: u64) -> (usize, usize) {
        use ostro_partition::store::Table;
        use ostro_partition::store::keys::decode_timer_key;
        let mut total = 0;
        let mut near = 0;
        for h in self.partitions() {
            let rows = h
                .reader()
                .prefix(Table::Timers, &[], usize::MAX)
                .expect("timers");
            for (k, _) in rows {
                total += 1;
                if decode_timer_key(&k).is_some_and(|(due, _, _)| due <= due_ms) {
                    near += 1;
                }
            }
        }
        (total, near)
    }
}

/// Resident set size of this process in bytes (`VmRSS` of `/proc/self/status`); `None`
/// off Linux.
pub fn rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = s.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kb * 1024)
}

impl TestServer {
    /// `PUT /v1/modules/{sha}` (optionally `?tenant=`); returns the module sha.
    pub async fn deploy_guest(&self, guest: &str, tenant: Option<&str>) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let wasm = guest_wasm(guest);
        let sha: [u8; 32] = Sha256::digest(&wasm).into();
        let hex: String = sha.iter().map(|x| format!("{x:02x}")).collect();
        let q = tenant.map(|t| format!("?tenant={t}")).unwrap_or_default();
        let r = self
            .http
            .put(self.url(&format!("/v1/modules/{hex}{q}")))
            .header("content-type", "application/wasm")
            .body(wasm)
            .send()
            .await
            .expect("PUT module");
        assert!(r.status().is_success(), "deploy {guest}: {}", r.status());
        sha
    }

    /// `POST /v1/workflows/{workflow}/runs`: `(status, body)`.
    pub async fn post_start(
        &self,
        workflow: &str,
        body: &serde_json::Value,
    ) -> (reqwest::StatusCode, serde_json::Value) {
        let r = self
            .http
            .post(self.url(&format!("/v1/workflows/{workflow}/runs")))
            .json(body)
            .send()
            .await
            .expect("POST run");
        let st = r.status();
        (st, r.json().await.expect("json"))
    }

    /// REST start expecting a new run (201); returns its id.
    pub async fn start_rest(&self, workflow: &str, body: serde_json::Value) -> RunId {
        let (st, v) = self.post_start(workflow, &body).await;
        assert_eq!(st, reqwest::StatusCode::CREATED, "start {workflow}: {v}");
        v["run_id"]
            .as_str()
            .expect("run_id")
            .parse()
            .expect("run id")
    }

    /// `GET path`: `(status, body)` (`Null` for an empty body).
    pub async fn get_json(&self, path: &str) -> (reqwest::StatusCode, serde_json::Value) {
        let r = self.http.get(self.url(path)).send().await.expect("GET");
        let st = r.status();
        let b = r.bytes().await.expect("body");
        let v = if b.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&b).expect("json")
        };
        (st, v)
    }

    /// `POST path` with a JSON body: `(status, body)`.
    pub async fn post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> (reqwest::StatusCode, serde_json::Value) {
        let r = self
            .http
            .post(self.url(path))
            .json(body)
            .send()
            .await
            .expect("POST");
        let st = r.status();
        let b = r.bytes().await.expect("body");
        let v = if b.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&b).expect("json")
        };
        (st, v)
    }

    /// Every run id listed by `GET /v1/runs?{filter}` (all pages, 1000 per page).
    pub async fn list_all(&self, filter: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut path = format!("/v1/runs?limit=1000&{filter}");
            if let Some(t) = &token {
                path.push_str(&format!("&page_token={t}"));
            }
            let (st, v) = self.get_json(&path).await;
            assert_eq!(st, reqwest::StatusCode::OK, "list {path}: {v}");
            for r in v["runs"].as_array().expect("runs") {
                out.push(r["run_id"].as_str().expect("id").to_owned());
            }
            match v["next_page_token"].as_str() {
                Some(t) => token = Some(t.to_owned()),
                None => return out,
            }
        }
    }
}
