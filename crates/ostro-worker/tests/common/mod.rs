//! Shared helpers for the ostro-worker integration tests.
#![allow(dead_code)]

pub mod fake_server;

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ostro_types::journal::{Event, EventBody, Payload, RngSeed, RunStarted, TimerFired};
use ostro_worker::{Worker, WorkerConfig, WorkerError, WorkerEvent};
use ostro_worker_proto::{WireOutcome, v1};
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

pub use fake_server::{FakeConfig, FakeJoin, FakeServer, FakeState, TestCa};

/// Repository root (`crates/ostro-worker/../..`).
pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Build the `guests/` workspace once per process and return the named guest's `.wasm`
/// (same pattern as the engine/server test helpers).
pub fn guest_wasm(name: &str) -> Vec<u8> {
    static BUILT: OnceLock<()> = OnceLock::new();
    let root = repo_root();
    let target_dir = root.join("target/guests");
    BUILT.get_or_init(|| {
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let out = Command::new(cargo)
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
            .args(["-p", "long-journal", "-p", "pay-flow", "-p", "cpu-burn"])
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

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// An 18-byte run id on partition 0.
pub fn run_id(n: u8) -> Vec<u8> {
    let mut b = vec![0u8; 18];
    b[2] = 0x01;
    b[17] = n;
    b
}

/// `RunStarted` (seq 0) for `workflow` with JSON `input`.
pub fn run_started(workflow: &str, module_sha: &[u8; 32], input: serde_json::Value) -> Event {
    Event {
        seq: 0,
        ts_nanos: 1_700_000_000_000_000_000,
        body: EventBody::RunStarted(RunStarted {
            workflow: workflow.into(),
            workflow_version: 1,
            input: Payload {
                content_type: "application/json".into(),
                data: serde_json::to_vec(&input).unwrap(),
            },
            rng_seed: RngSeed([7; 32]),
            accepted_at_nanos: 1_700_000_000_000_000_000,
            deadline_nanos: None,
            module_hash: hex(module_sha),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        }),
    }
}

pub fn timer_fired(seq: u64, op: u64) -> Event {
    Event {
        seq,
        ts_nanos: 1_700_000_000_000_000_000 + seq,
        body: EventBody::TimerFired(TimerFired { op }),
    }
}

pub fn token(n: u32) -> v1::Token {
    v1::Token { epoch: 1, n }
}

pub fn run_ref(id: &[u8]) -> v1::RunRef {
    v1::RunRef {
        run_id: id.to_vec(),
        partition: 0,
    }
}

/// An `Activate` message.
pub fn activate(id: &[u8], t: u32, sha: &[u8; 32], journal_len: u64) -> v1::server_msg::Msg {
    v1::server_msg::Msg::Activate(v1::Activate {
        run: Some(run_ref(id)),
        token: Some(token(t)),
        module_sha: sha.to_vec(),
        journal_len,
        workflow: String::new(),
        workflow_version: 1,
        traceparent: String::new(),
        hot_wait_ms: 0,
    })
}

pub fn input(id: &[u8], t: u32, up_to: u64) -> v1::server_msg::Msg {
    v1::server_msg::Msg::Input(v1::Input {
        run: Some(run_ref(id)),
        token: Some(token(t)),
        up_to_seq: up_to,
    })
}

pub fn cancel(id: &[u8], t: u32) -> v1::server_msg::Msg {
    v1::server_msg::Msg::Cancel(v1::Cancel {
        run: Some(run_ref(id)),
        token: Some(token(t)),
        reason: 0,
    })
}

/// Decode a `Done`'s outcome.
pub fn outcome(d: &v1::Done) -> WireOutcome {
    WireOutcome::decode(&d.outcome).unwrap()
}

/// A worker config joining through `join` with the CA pin, caching under `cache`.
pub fn worker_cfg(join: &FakeJoin, ca: &TestCa, cache: &Path) -> WorkerConfig {
    WorkerConfig {
        join: Some(join.url()),
        token: Some(ostro_worker::JoinToken::new("wkr_test.s3cr3t-join-token")),
        ca_hash: Some(ca.hash()),
        labels: [("zone".to_string(), "a".to_string())]
            .into_iter()
            .collect(),
        slots: 2,
        cache_dir: cache.to_path_buf(),
        memory_bytes: 512 * 1024 * 1024,
        worker_id: Some("w1".into()),
        reconnect_max_ms: 200,
        rejoin_after_failures: 2,
        ..WorkerConfig::default()
    }
}

/// A running worker task.
pub struct Running {
    pub worker: Arc<Worker>,
    pub events: broadcast::Receiver<WorkerEvent>,
    pub shutdown: CancellationToken,
    pub handle: tokio::task::JoinHandle<Result<(), WorkerError>>,
}

pub fn spawn_worker(cfg: WorkerConfig) -> Running {
    let worker = Arc::new(Worker::new(cfg).unwrap());
    let events = worker.subscribe();
    let shutdown = CancellationToken::new();
    let (w, sd) = (Arc::clone(&worker), shutdown.clone());
    let handle = tokio::spawn(async move { w.run(sd).await });
    Running {
        worker,
        events,
        shutdown,
        handle,
    }
}

impl Running {
    /// Wait for an event matching `f` (panics after `timeout`).
    pub async fn wait_event(
        &mut self,
        timeout: Duration,
        what: &str,
        f: impl Fn(&WorkerEvent) -> bool,
    ) -> WorkerEvent {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(left, self.events.recv()).await {
                Ok(Ok(ev)) if f(&ev) => return ev,
                Ok(Ok(_)) => {}
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {}
                Ok(Err(e)) => panic!("event stream closed waiting for {what}: {e}"),
                Err(_) => panic!("timed out after {timeout:?} waiting for {what}"),
            }
        }
    }

    /// Cancel and await the worker.
    pub async fn stop(self) -> Result<(), WorkerError> {
        self.shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(20), self.handle)
            .await
            .expect("worker stops")
            .expect("worker task")
    }
}

/// Everything a session test needs: CA, fake server, join endpoint.
pub struct Harness {
    pub ca: Arc<TestCa>,
    pub fake: FakeServer,
    pub join: FakeJoin,
    pub dir: tempfile::TempDir,
}

impl Harness {
    pub async fn new(cfg: FakeConfig) -> Self {
        let ca = Arc::new(TestCa::new());
        let st = FakeState::new(Arc::clone(&ca), cfg);
        let fake = FakeServer::start(st).await;
        let join = FakeJoin::start(Arc::clone(&ca), true, vec![fake.endpoint()]);
        Self {
            ca,
            fake,
            join,
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn st(&self) -> &Arc<FakeState> {
        &self.fake.st
    }

    pub fn cfg(&self) -> WorkerConfig {
        worker_cfg(&self.join, &self.ca, &self.dir.path().join("cache"))
    }

    /// Upload `guest`, seed a run with `RunStarted`, return the module sha.
    pub fn seed(
        &self,
        guest: &str,
        workflow: &str,
        id: &[u8],
        input: serde_json::Value,
    ) -> [u8; 32] {
        let wasm = guest_wasm(guest);
        let sha = self.st().put_blob(wasm);
        self.st()
            .push_events(id, &[run_started(workflow, &sha, input)]);
        sha
    }
}
