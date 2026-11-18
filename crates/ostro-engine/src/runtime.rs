//! Engine runtime facade (plan 03-08): one [`Runtime`] per node owns the Wasmtime
//! engine, the production linker, the clock used to stamp live events and the
//! await-token key.
//!
//! [`Runtime::prepare_cached`] is the production entry point (plan 03-11): an L2
//! `InstancePre` LRU keyed by module SHA-256 in front of the authenticated L1 AOT disk
//! cache ([`AotCache`], compile at deploy, ENG-07); every component loaded from L1 or
//! compiled fresh is validated against the production linker before it is
//! pre-instantiated. [`Runtime::prepare`] is the uncached path (tests/tools).
//! [`Runtime::activate`] runs one activation through [`crate::activation`].

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use ostro_types::journal::{Event, EventBody, Payload, RngSeed, RunStarted};
use sha2::{Digest, Sha256};
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

use crate::cache::{AotCache, CacheStatsSnapshot, load_or_create_key, module_sha};

use crate::config::EngineConfig;
use crate::engine::{OstroEngine, configure_store};
use crate::env::{Clock, SystemClock};
use crate::error::EngineError;
use crate::host::bindings::WorkflowPre;
use crate::host::state::{HostInit, HostState, HotWait, build_linker};
use crate::limits::ParkedRegistry;
use crate::sink::{Activate, ActivationOutcome, ActivationSink, RunRef};
use crate::validate::{
    ValidatedComponent, reject_core_module, validate_component, validate_loaded,
};

/// Default L2 `InstancePre` cache capacity (prepared modules kept per node).
pub const DEFAULT_INSTANCE_PRE_CAPACITY: usize = 256;

/// Node-level runtime options.
pub struct RuntimeOptions {
    /// Clock used only to stamp new (live) journal events.
    pub clock: Arc<dyn Clock>,
    /// Key for minting await tokens.
    pub token_key: [u8; 32],
    /// L1 AOT disk cache directory (artefacts + node-local HMAC key); `None` compiles
    /// in memory on every L2 miss.
    pub cache_dir: Option<PathBuf>,
    /// L2 `InstancePre` LRU capacity (default [`DEFAULT_INSTANCE_PRE_CAPACITY`]).
    pub instance_pre_capacity: usize,
}

impl std::fmt::Debug for RuntimeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeOptions").finish_non_exhaustive()
    }
}

impl Default for RuntimeOptions {
    /// [`SystemClock`], a process-local token key (sha256 over time + pid), no AOT
    /// disk cache and the default L2 capacity.
    ///
    /// The process-local key only suits single-process tests: tokens minted with it
    /// do not verify after a restart. Phase 4 passes the cluster key.
    fn default() -> Self {
        let mut h = Sha256::new();
        h.update(b"ostro/token-key");
        h.update(SystemClock.now_unix_nanos().to_le_bytes());
        h.update(std::process::id().to_le_bytes());
        Self {
            clock: Arc::new(SystemClock),
            token_key: h.finalize().into(),
            cache_dir: None,
            instance_pre_capacity: DEFAULT_INSTANCE_PRE_CAPACITY,
        }
    }
}

/// Engine + production linker + clock + token key.
pub struct Runtime {
    engine: OstroEngine,
    linker: Linker<HostState>,
    parked: Arc<ParkedRegistry>,
    aot: Option<AotCache>,
    l2: Mutex<InstancePreLru>,
    pub(crate) opts: RuntimeOptions,
}

/// L2 cache: prepared modules by SHA-256, evicting the least recently used.
#[derive(Default)]
struct InstancePreLru {
    tick: u64,
    map: HashMap<[u8; 32], (Arc<Prepared>, u64)>,
}

impl InstancePreLru {
    fn get(&mut self, sha: &[u8; 32]) -> Option<Arc<Prepared>> {
        self.tick += 1;
        let tick = self.tick;
        self.map.get_mut(sha).map(|(p, t)| {
            *t = tick;
            p.clone()
        })
    }

    /// Insert unless present (a concurrent miss may have won); returns the cached entry.
    fn insert(&mut self, p: Arc<Prepared>, capacity: usize) -> Arc<Prepared> {
        self.tick += 1;
        let tick = self.tick;
        let entry = self.map.entry(p.sha).or_insert((p, tick));
        entry.1 = tick;
        let out = entry.0.clone();
        while self.map.len() > capacity.max(1) {
            let Some(oldest) = self
                .map
                .iter()
                .min_by_key(|(_, (_, t))| *t)
                .map(|(k, _)| *k)
            else {
                break;
            };
            self.map.remove(&oldest);
        }
        out
    }
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runtime")
            .field("engine", &self.engine)
            .finish_non_exhaustive()
    }
}

/// A validated, pre-instantiated workflow component.
pub struct Prepared {
    /// SHA-256 of the component bytes.
    pub sha: [u8; 32],
    /// Lowercase hex of [`Self::sha`] (reported in `run-info.module-hash`).
    pub module_hash_hex: String,
    pub(crate) pre: WorkflowPre<HostState>,
}

impl std::fmt::Debug for Prepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("module_hash_hex", &self.module_hash_hex)
            .finish_non_exhaustive()
    }
}

/// One entry of the component's `describe()` (WIT `workflow-meta`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowMeta {
    pub name: String,
    pub version: u32,
    pub default_queue: Option<String>,
    /// Run timeout declared by the workflow (WIT-06).
    pub run_timeout_ms: Option<u64>,
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

impl Runtime {
    /// Build the engine (pool, epoch ticker, memory budget) and the production linker.
    pub fn new(cfg: EngineConfig, opts: RuntimeOptions) -> Result<Self, EngineError> {
        let engine = OstroEngine::new(cfg)?;
        let linker = build_linker(engine.engine())?;
        let parked = ParkedRegistry::new(engine.budget().clone());
        let aot = match &opts.cache_dir {
            Some(dir) => {
                let cache_err = |e: std::io::Error| {
                    EngineError::Cache(format!("AOT cache at {}: {e}", dir.display()))
                };
                let key = load_or_create_key(dir).map_err(cache_err)?;
                Some(
                    AotCache::new(
                        dir,
                        key,
                        engine.engine().clone(),
                        engine.config().compile_permits,
                    )
                    .map_err(cache_err)?,
                )
            }
            None => None,
        };
        Ok(Self {
            engine,
            linker,
            parked,
            aot,
            l2: Mutex::new(InstancePreLru::default()),
            opts,
        })
    }

    fn l2(&self) -> std::sync::MutexGuard<'_, InstancePreLru> {
        self.l2.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Prepare `wasm` through the L2 `InstancePre` LRU and the L1 AOT cache.
    ///
    /// L2 hit: the cached [`Prepared`] (same `Arc`). Miss: the component comes from the
    /// AOT cache (authenticated disk artefact, or compiled once behind the compile
    /// permits and persisted) or, without a cache dir, from an in-memory compile; it is
    /// then validated against the production linker (on every load, hits included)
    /// and pre-instantiated. Phase 4 calls this at deploy (ENG-07) and at activation.
    pub async fn prepare_cached(&self, wasm: &[u8]) -> Result<Arc<Prepared>, EngineError> {
        let sha = module_sha(wasm);
        if let Some(p) = self.l2().get(&sha) {
            return Ok(p);
        }
        reject_core_module(wasm)?;
        let component = match &self.aot {
            Some(aot) => aot.component(sha, wasm).await?,
            None => Component::from_binary(self.engine.engine(), wasm).map_err(|e| {
                EngineError::Validation(vec![format!("not a valid Wasm component: {e:#}")])
            })?,
        };
        let validated = validate_loaded(self.engine.engine(), &self.linker, &component)?;
        let prepared = Arc::new(self.pre_instantiate(sha, &validated)?);
        Ok(self.l2().insert(prepared, self.opts.instance_pre_capacity))
    }

    /// AOT cache counters; `None` without a cache dir.
    pub fn cache_stats(&self) -> Option<CacheStatsSnapshot> {
        self.aot.as_ref().map(AotCache::stats)
    }

    /// Number of prepared modules in the L2 cache.
    pub fn instance_pre_len(&self) -> usize {
        self.l2().map.len()
    }

    fn pre_instantiate(
        &self,
        sha: [u8; 32],
        validated: &ValidatedComponent,
    ) -> Result<Prepared, EngineError> {
        let pre = WorkflowPre::new(self.linker.instantiate_pre(&validated.component)?)?;
        Ok(Prepared {
            sha,
            module_hash_hex: hex(&sha),
            pre,
        })
    }

    /// The [`HostInit`] of one activation, exactly as the driver builds it.
    pub(crate) fn host_init(
        &self,
        p: &Prepared,
        run: RunRef,
        attempt: u32,
        journal: Vec<Event>,
        sink: Option<Arc<dyn ActivationSink>>,
        hot: Option<HotWait>,
    ) -> HostInit {
        let cfg = self.engine.config();
        HostInit {
            run,
            attempt,
            module_hash_hex: p.module_hash_hex.clone(),
            journal,
            clock: self.opts.clock.clone(),
            limiter: self.engine.new_limiter(),
            wall_deadline: Instant::now() + cfg.activation_timeout,
            token_key: self.opts.token_key,
            continue_as_new_events: cfg.continue_as_new_events,
            sink,
            hot,
        }
    }

    /// Warm-instantiation probe (NFR-2.3 measurements): build the store exactly as an
    /// activation does over `journal` and instantiate `p`; no guest code runs and the
    /// store is dropped.
    pub async fn instantiate_probe(
        &self,
        p: &Prepared,
        run: RunRef,
        journal: Vec<Event>,
    ) -> Result<(), EngineError> {
        let state = HostState::new(self.host_init(p, run, 1, journal, None, None))
            .map_err(|e| EngineError::Wasmtime(wasmtime::format_err!("probe state: {e}")))?;
        let mut store = Store::new(self.engine.engine(), state);
        configure_store(&mut store, self.engine.config().activation_fuel)?;
        p.pre.instantiate_async(&mut store).await?;
        Ok(())
    }

    /// Registry of hot-waiting activations (LRU eviction under node budget pressure).
    pub fn parked(&self) -> &Arc<ParkedRegistry> {
        &self.parked
    }

    pub fn engine(&self) -> &OstroEngine {
        &self.engine
    }

    pub fn linker(&self) -> &Linker<HostState> {
        &self.linker
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.opts.clock
    }

    /// Validate `wasm` against the production linker and pre-instantiate it.
    pub fn prepare(&self, wasm: &[u8]) -> Result<Prepared, EngineError> {
        let sha: [u8; 32] = Sha256::digest(wasm).into();
        let validated = validate_component(self.engine.engine(), &self.linker, wasm)?;
        self.pre_instantiate(sha, &validated)
    }

    /// Call the component's `describe()` in a throwaway store (synthetic journal with a
    /// zero seed; nothing is journaled).
    pub async fn describe(&self, p: &Prepared) -> Result<Vec<WorkflowMeta>, EngineError> {
        let cfg = self.engine.config();
        let now = self.opts.clock.now_unix_nanos();
        let started = Event {
            seq: 0,
            ts_nanos: now,
            body: EventBody::RunStarted(RunStarted {
                workflow: String::new(),
                workflow_version: 0,
                input: Payload {
                    content_type: String::new(),
                    data: Vec::new(),
                },
                rng_seed: RngSeed([0; 32]),
                accepted_at_nanos: now,
                deadline_nanos: None,
                module_hash: p.module_hash_hex.clone(),
                tenant: None,
                parent_run_id: None,
                continued_from: None,
            }),
        };
        let state = HostState::new(HostInit {
            run: RunRef {
                partition: 0,
                run_id: "describe".to_string(),
            },
            attempt: 0,
            module_hash_hex: p.module_hash_hex.clone(),
            journal: vec![started],
            clock: self.opts.clock.clone(),
            limiter: self.engine.new_limiter(),
            wall_deadline: Instant::now() + cfg.activation_timeout,
            token_key: self.opts.token_key,
            continue_as_new_events: cfg.continue_as_new_events,
            sink: None,
            hot: None,
        })
        .map_err(|e| EngineError::Wasmtime(wasmtime::format_err!("describe state: {e:?}")))?;
        let mut store = Store::new(self.engine.engine(), state);
        configure_store(&mut store, cfg.activation_fuel)?;
        let wf = p.pre.instantiate_async(&mut store).await?;
        let metas = wf
            .ostro_runtime_workflows()
            .call_describe(&mut store)
            .await?;
        Ok(metas
            .into_iter()
            .map(|m| WorkflowMeta {
                name: m.name,
                version: m.version,
                default_queue: m.default_queue,
                run_timeout_ms: m.run_timeout_ms,
            })
            .collect())
    }

    /// Run one activation; reports the outcome through `sink.done` (exactly once) and
    /// returns it.
    pub async fn activate(
        &self,
        p: &Prepared,
        act: Activate,
        sink: Arc<dyn ActivationSink>,
    ) -> ActivationOutcome {
        crate::activation::run_activation(self, p, act, sink).await
    }
}
