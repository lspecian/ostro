//! ostro-worker: the remote worker runtime behind `ostro worker` (Phase 7, WORK-01/02/06).
//!
//! A worker joins a cluster (kubeadm-style token + CSR, D-W4, [`join`]), opens an mTLS
//! `Execute.Session` to a server node (D-W2, [`session`]) and runs activations on its local
//! engine ([`runner`]). The leader stays authoritative: fencing, derive, effects and
//! secrets never leave it (D-W3). Everything the engine writes goes back over the wire
//! ([`remote_sink::RemoteSink`]); effects are bridged back to the leader
//! ([`effect_proxy::EffectProxy`]); journals and blobs are pulled and verified
//! ([`journal_fetch`], [`blob_fetch`]).
//!
//! - **Self-fence:** losing the session stream cancels every in-flight activation at once
//!   and no further I/O is done for them (appends answer `Fenced` locally).
//! - **Reconnect / re-join:** jittered exponential backoff across the known endpoints;
//!   after `rejoin_after_failures` consecutive failures on every endpoint the worker joins
//!   again (fresh key, certificate and endpoints; B5).
//! - **Renewal:** at `renew_after` the worker sends `Renew{csr}`, opens a new channel and
//!   session with the new identity, then drains and closes the old one (Pitfall 10).
//! - **Drain:** on shutdown the worker sends `Drain`, stops crediting, evicts in-flight
//!   activations (`Done{Evicted}`) and exits within a bound ([`drain`]).
//!
//! Failpoints (`--features failpoints`, names fixed for 07-18/07-21):
//! `worker::before_append`, `worker::before_done`, `worker::before_start_effect`,
//! `worker::after_start_effect`. Actions: `return(error)` (default), `return(hang)`,
//! `return(wait:<ms>)`.

pub mod blob_fetch;
pub mod config;
pub mod drain;
pub mod effect_proxy;
pub mod join;
pub mod journal_fetch;
pub mod remote_sink;
pub mod runner;
pub mod session;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

pub use config::{JoinToken, WorkerConfig};
use ostro_engine::runtime::Runtime;
use ostro_worker_proto::v1;
use tokio::sync::{Notify, broadcast, mpsc};
use tokio_util::sync::CancellationToken;

/// `Hello.ostro_version`: this build's ostro version.
pub const OSTRO_VERSION: &str = env!("CARGO_PKG_VERSION");
/// `Hello.wasmtime_version`: the Wasmtime pinned in the workspace, the engine's own
/// constant (one copy, 07-22).
pub const WASMTIME_VERSION: &str = ostro_engine::cache::WASMTIME_VERSION;
/// `Hello.wit_packages`: the WIT worlds this engine implements.
pub const WIT_PACKAGES: &[&str] = &["ostro:runtime@0.1.0"];
/// tonic encode/decode limit on both sides (D-W2).
pub const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Worker failures.
#[derive(Debug, thiserror::Error)]
pub enum WorkerError {
    /// Invalid configuration (the message names the key; never a secret value).
    #[error("invalid worker configuration: {0}")]
    Config(String),
    /// Joining the cluster failed (never includes the token).
    #[error("join failed: {0}")]
    Join(#[from] join::JoinError),
    /// The server refused this worker's versions (`GoAway` before `Welcome`); not retried.
    #[error("version mismatch: the server refused this worker: {0}")]
    VersionMismatch(String),
    /// Building the local engine failed.
    #[error("engine: {0}")]
    Engine(String),
}

/// Observable worker lifecycle events (tests, the binary's logs, 07-18/07-21 chaos).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerEvent {
    /// A join (or re-join) succeeded.
    Joined {
        endpoints: Vec<String>,
        cluster_id: String,
    },
    /// A session reached `Welcome`.
    Connected {
        endpoint: String,
        session_id: Vec<u8>,
    },
    /// Opening or keeping a session failed (the worker reconnects).
    Disconnected { endpoint: String, error: String },
    /// Stream loss: `cancelled` in-flight activations were cancelled at once.
    SelfFenced { cancelled: usize },
    /// The worker re-joined after `rejoin_after_failures` failures on every endpoint.
    Rejoined,
    /// The client certificate was renewed and the session moved to a new channel.
    Renewed,
    /// An `Activate` started.
    ActivationStarted { run_id: String, attempt: u32 },
    /// An activation could not start (module or journal fetch, verification).
    ActivationFailed { run_id: String, error: String },
    /// An activation ended; `outcome` is the `Debug` of the engine outcome.
    ActivationEnded {
        run_id: String,
        attempt: u32,
        outcome: String,
    },
    /// The worker drained and is exiting.
    Drained,
}

/// Failpoint action (`worker_fail!`), mirroring `ostro-bridge`'s convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailAction {
    /// Pend forever.
    Hang,
    /// Sleep this many ms, then continue.
    Wait(u64),
    /// Fail the operation at this site.
    Error,
}

impl FailAction {
    /// Parse a failpoint argument: `hang`, `wait:<ms>`, anything else = `Error`.
    pub fn from_arg(arg: Option<&str>) -> Self {
        match arg {
            Some("hang") => Self::Hang,
            Some(a) if a.starts_with("wait:") => {
                a[5..].parse().map(Self::Wait).unwrap_or(Self::Error)
            }
            _ => Self::Error,
        }
    }

    /// Apply `Hang` / `Wait`; returns `true` when the site must fail (`Error`).
    pub async fn apply(self) -> bool {
        match self {
            Self::Hang => std::future::pending().await,
            Self::Wait(ms) => {
                tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
                false
            }
            Self::Error => true,
        }
    }
}

/// The configured action of failpoint `name` (always `None` without `failpoints`).
#[doc(hidden)]
pub fn fail_eval(name: &str) -> Option<FailAction> {
    #[cfg(feature = "failpoints")]
    {
        fail::eval(name, |arg| FailAction::from_arg(arg.as_deref()))
    }
    #[cfg(not(feature = "failpoints"))]
    {
        let _ = name;
        None
    }
}

/// `worker_fail!("worker::name")` -> `bool`: `true` when the site must fail. Hang / wait
/// actions are applied here. Compiled to `false` without the `failpoints` feature (P-13).
#[macro_export]
macro_rules! worker_fail {
    ($name:literal) => {{
        #[cfg(feature = "failpoints")]
        let r = match $crate::fail_eval($name) {
            Some(a) => a.apply().await,
            None => false,
        };
        #[cfg(not(feature = "failpoints"))]
        let r = false;
        r
    }};
}

/// The session the activations currently talk through (swapped on renewal / reconnect).
#[derive(Clone)]
pub(crate) struct Link {
    pub(crate) client: v1::execute_client::ExecuteClient<tonic::transport::Channel>,
    pub(crate) out: mpsc::UnboundedSender<v1::WorkerMsg>,
    pub(crate) session_id: Vec<u8>,
    pub(crate) max_frame: usize,
}

/// One in-flight activation.
pub(crate) struct ActiveRun {
    pub(crate) run: v1::RunRef,
    pub(crate) token: v1::Token,
    pub(crate) cancel: CancellationToken,
    pub(crate) inputs: mpsc::UnboundedSender<u64>,
    /// Distinguishes a replaced entry for the same run.
    pub(crate) generation: u64,
}

/// State shared by the session loop and every activation.
pub(crate) struct Shared {
    pub(crate) cfg: WorkerConfig,
    pub(crate) worker_id: String,
    pub(crate) events: broadcast::Sender<WorkerEvent>,
    runtime: Mutex<Option<(v1::EngineLimits, Arc<Runtime>)>>,
    pub(crate) active: Mutex<HashMap<Vec<u8>, ActiveRun>>,
    link: Mutex<Option<Link>>,
    /// Bumped whenever the number of free slots changes.
    pub(crate) slots_changed: Notify,
    pub(crate) draining: std::sync::atomic::AtomicBool,
    generation: std::sync::atomic::AtomicU64,
    /// Await-token minting key (local: see the 07-07 SUMMARY open item).
    token_key: [u8; 32],
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl Shared {
    pub(crate) fn emit(&self, ev: WorkerEvent) {
        let _ = self.events.send(ev);
    }

    pub(crate) fn link(&self) -> Option<Link> {
        lock(&self.link).clone()
    }

    pub(crate) fn set_link(&self, l: Option<Link>) {
        *lock(&self.link) = l;
    }

    pub(crate) fn active(&self) -> MutexGuard<'_, HashMap<Vec<u8>, ActiveRun>> {
        lock(&self.active)
    }

    pub(crate) fn next_generation(&self) -> u64 {
        self.generation
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn free_slots(&self) -> u32 {
        let used = u32::try_from(self.active().len()).unwrap_or(u32::MAX);
        self.cfg.slots.saturating_sub(used)
    }

    pub(crate) fn runtime(&self) -> Option<Arc<Runtime>> {
        lock(&self.runtime).as_ref().map(|(_, r)| Arc::clone(r))
    }

    /// The runtime for the server's `EngineLimits` (Pitfall 5: never local defaults);
    /// rebuilt only when the limits change. The AOT disk cache under `cache_dir` survives.
    pub(crate) fn runtime_for(
        &self,
        limits: &v1::EngineLimits,
    ) -> Result<Arc<Runtime>, WorkerError> {
        let mut g = lock(&self.runtime);
        if let Some((l, r)) = g.as_ref()
            && l == limits
        {
            return Ok(Arc::clone(r));
        }
        let cfg = runner::engine_config(limits, &self.cfg);
        std::fs::create_dir_all(&self.cfg.cache_dir).map_err(|e| {
            WorkerError::Engine(format!("cache dir {}: {e}", self.cfg.cache_dir.display()))
        })?;
        let opts = ostro_engine::runtime::RuntimeOptions {
            // The engine's live-event clock, exactly as the server's runtime (RuntimeOptions
            // default); journaled timestamps, not worker scheduling logic.
            clock: Arc::new(ostro_engine::env::SystemClock),
            token_key: self.token_key,
            cache_dir: Some(self.cfg.cache_dir.clone()),
            instance_pre_capacity: ostro_engine::runtime::DEFAULT_INSTANCE_PRE_CAPACITY,
        };
        let rt = Arc::new(Runtime::new(cfg, opts).map_err(|e| WorkerError::Engine(e.to_string()))?);
        *g = Some((*limits, Arc::clone(&rt)));
        Ok(rt)
    }

    /// Cancel every in-flight activation (self-fence / drain); returns how many.
    pub(crate) fn cancel_all(&self) -> usize {
        let g = self.active();
        for a in g.values() {
            a.cancel.cancel();
        }
        g.len()
    }
}

/// A worker: build with [`Worker::new`], observe with [`Worker::subscribe`], run with
/// [`Worker::run`].
pub struct Worker {
    shared: Arc<Shared>,
}

impl std::fmt::Debug for Worker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Worker")
            .field("worker_id", &self.shared.worker_id)
            .finish_non_exhaustive()
    }
}

/// A random-enough u64 without a rand dependency (std's per-instance random SipHash keys).
pub(crate) fn random_u64() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(0x6f73_7472_6f2d_776b);
    h.finish()
}

impl Worker {
    /// Validate `cfg` and build the worker (no I/O yet).
    pub fn new(cfg: WorkerConfig) -> Result<Self, WorkerError> {
        cfg.validate()?;
        let worker_id = cfg
            .worker_id
            .clone()
            .unwrap_or_else(|| format!("w-{:016x}", random_u64()));
        let mut token_key = [0u8; 32];
        for chunk in token_key.chunks_mut(8) {
            chunk.copy_from_slice(&random_u64().to_le_bytes());
        }
        let (events, _) = broadcast::channel(1024);
        Ok(Self {
            shared: Arc::new(Shared {
                cfg,
                worker_id,
                events,
                runtime: Mutex::new(None),
                active: Mutex::new(HashMap::new()),
                link: Mutex::new(None),
                slots_changed: Notify::new(),
                draining: std::sync::atomic::AtomicBool::new(false),
                generation: std::sync::atomic::AtomicU64::new(1),
                token_key,
            }),
        })
    }

    /// This worker's id (configured or generated).
    pub fn worker_id(&self) -> &str {
        &self.shared.worker_id
    }

    /// Subscribe to lifecycle events.
    pub fn subscribe(&self) -> broadcast::Receiver<WorkerEvent> {
        self.shared.events.subscribe()
    }

    /// The engine runtime (built at the first `Welcome`), e.g. for AOT cache counters.
    pub fn runtime(&self) -> Option<Arc<Runtime>> {
        self.shared.runtime()
    }

    /// Number of in-flight activations.
    pub fn active_activations(&self) -> usize {
        self.shared.active().len()
    }

    /// Run until `shutdown` fires (then drain) or a fatal error (version mismatch, join
    /// refused, invalid configuration).
    pub async fn run(&self, shutdown: CancellationToken) -> Result<(), WorkerError> {
        session::run_loop(Arc::clone(&self.shared), shutdown).await
    }
}

/// Run the worker until `shutdown` fires.
pub async fn run(cfg: WorkerConfig, shutdown: CancellationToken) -> Result<(), WorkerError> {
    Worker::new(cfg)?.run(shutdown).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fail_action_parse() {
        assert_eq!(FailAction::from_arg(Some("hang")), FailAction::Hang);
        assert_eq!(FailAction::from_arg(Some("wait:5")), FailAction::Wait(5));
        assert_eq!(FailAction::from_arg(Some("wait:x")), FailAction::Error);
        assert_eq!(FailAction::from_arg(None), FailAction::Error);
    }

    #[cfg(not(feature = "failpoints"))]
    #[tokio::test]
    async fn failpoints_compiled_out() {
        assert!(!worker_fail!("worker::before_append"));
    }

    #[test]
    fn random_differs() {
        assert_ne!(random_u64(), random_u64());
    }
}
