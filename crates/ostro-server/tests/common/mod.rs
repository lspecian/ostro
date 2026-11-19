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
            slots: Arc::new(Semaphore::new(64)),
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
