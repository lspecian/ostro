//! Engine runtime facade (plan 03-08): one [`Runtime`] per node owns the Wasmtime
//! engine, the production linker, the clock used to stamp live events and the
//! await-token key.
//!
//! [`Runtime::prepare_cached`] is the production entry point (plan 03-11): an L2
//! `InstancePre` LRU keyed by module SHA-256 in front of the authenticated L1 AOT disk
//! cache ([`AotCache`], compile at deploy, ENG-07); every component loaded from L1 or
//! compiled fresh is validated against the production linker before it is
//! pre-instantiated. [`Runtime::prepare`] is the uncached path (tests/tools).
//!
//! WIT lines (13-07, ENG-10): the runtime holds a [`LinkerSet`]; a module is validated
//! against its line's linker and both cache levels key on the line (the L1 AOT key via
//! [`AotCache::component_for`], the L2 map by `(line, sha)`). Only the `0.1` line has an
//! activation driver; the test-only `0.2` fixture line validates but is not executable.
//! [`Runtime::activate`] runs one activation through [`crate::activation`];
//! [`Runtime::live_activations`] counts the activations running right now (WF-04: a
//! sleeping run holds no activation, so the gauge is 0 once every run has evicted).
//!
//! Core-instance admission (plan 09-07, ENG-2): every instantiation (activation,
//! `describe`, warm probe) first acquires the module's core-instance weight from the
//! node's [`CoreBudget`] (sized to the pool's `total_core_instances`; unbounded with the
//! on-demand allocator). The weight is the static count from validation
//! ([`crate::validate::count_core_instances`]) raised, once per prepared module, by a
//! calibration instantiation that counts what Wasmtime really creates (its fused adapter
//! modules are invisible statically: QuickJS has 11 static entries but needs 13, S9). `activate` holds the permit around the whole activation, hot-wait
//! parking included, and evicts with `BudgetPressure` (re-activated by the executor) when
//! the budget stays exhausted until the activation deadline.
//!
//! Every pooled resource (plan eng2-mem): the same admission also charges the module's
//! component-instance slot (1), linear memories and tables against the pool's
//! `total_component_instances`, `total_memories` and `total_tables`
//! ([`PoolBudget`], acquired in one fixed order with one deadline). Memories and tables
//! are exact and static: [`Component::resources_required`] walks every core module the
//! compiled component instantiates (Wasmtime's adapter modules included). Without this
//! a QuickJS instance (3 memories, 3 tables) passed the core budget and failed
//! instantiation with "instance pool exhausted ... memory stripe" once more than
//! `total_instances / 3` were live.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_types::journal::{Event, EventBody, Payload, RngSeed, RunStarted};
use sha2::{Digest, Sha256};
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

use crate::cache::{AotCache, CacheStatsSnapshot, load_or_create_key, module_sha};

use crate::config::EngineConfig;
use crate::core_budget::{CoreBudget, PoolBudget, PoolPermit, PoolWeights};
use crate::engine::{OstroEngine, PoolMode, configure_store};
use crate::env::{Clock, SystemClock};
use crate::error::{EngineError, EvictReason};
use crate::host::bindings::WorkflowPre;
use crate::host::linker_set::{LinkerSet, WitLine, WitSelection, line_from_bytes};
use crate::host::state::{HostInit, HostState, HotWait};
use crate::limits::ParkedRegistry;
use crate::sink::{Activate, ActivationOutcome, ActivationSink, RunRef};
use crate::validate::{ValidatedComponent, reject_core_module, validate_loaded};

/// Default L2 `InstancePre` cache capacity (prepared modules kept per node).
pub const DEFAULT_INSTANCE_PRE_CAPACITY: usize = 256;

/// How long `describe` / `instantiate_probe` wait for core-instance slots (09-07).
pub const BUDGET_PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Extra core instances (over the static count) a calibration may find; also the margin
/// of the conservative weight used when a calibration cannot run (09-07).
pub const CALIBRATION_MARGIN: u32 = 16;

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

/// Engine + production linkers (one per WIT line) + clock + token key.
pub struct Runtime {
    engine: OstroEngine,
    linkers: LinkerSet,
    parked: Arc<ParkedRegistry>,
    aot: Option<AotCache>,
    l2: Mutex<InstancePreLru>,
    /// Activations currently inside [`Runtime::activate`] (see [`LiveGuard`]).
    live: AtomicU64,
    /// Weighted admission over every pooled resource (plans 09-07, eng2-mem).
    pool_budget: PoolBudget,
    pub(crate) opts: RuntimeOptions,
}

/// Counts one running activation in [`Runtime::live_activations`] for its lifetime.
/// Dropped on every exit path of the activation future: return (any outcome), panic
/// unwind, or the future itself being dropped (caller cancellation).
struct LiveGuard<'a>(&'a AtomicU64);

impl<'a> LiveGuard<'a> {
    fn enter(live: &'a AtomicU64) -> Self {
        live.fetch_add(1, Ordering::Relaxed);
        Self(live)
    }
}

impl Drop for LiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// L2 key: the module's WIT line and SHA-256 (13-07).
type L2Key = (WitLine, [u8; 32]);

/// L2 cache: prepared modules by WIT line and SHA-256, evicting the least recently used.
#[derive(Default)]
struct InstancePreLru {
    tick: u64,
    map: HashMap<L2Key, (Arc<Prepared>, u64)>,
}

impl InstancePreLru {
    fn get(&mut self, key: &L2Key) -> Option<Arc<Prepared>> {
        self.tick += 1;
        let tick = self.tick;
        self.map.get_mut(key).map(|(p, t)| {
            *t = tick;
            p.clone()
        })
    }

    /// Insert unless present (a concurrent miss may have won); returns the cached entry.
    fn insert(&mut self, p: Arc<Prepared>, capacity: usize) -> Arc<Prepared> {
        self.tick += 1;
        let tick = self.tick;
        let entry = self.map.entry((p.wit.line, p.sha)).or_insert((p, tick));
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
    /// Static core-instance count from validation (a lower bound of the admission
    /// weight, see [`Runtime::core_weight`]) (09-07).
    pub core_instances: u32,
    /// Linear memories one instantiation defines (exact, [`Component::resources_required`];
    /// its weight in the memories budget) (eng2-mem).
    pub memories: u32,
    /// Tables one instantiation defines (exact; its weight in the tables budget).
    pub tables: u32,
    /// Largest initial linear memory any of its core modules declares, in bytes (the
    /// smallest `max_memory_size` it can start with).
    pub min_memory_bytes: u64,
    /// The module's WIT line and minimum feature level (13-07).
    pub wit: WitSelection,
    /// Calibrated weight, measured once (09-07).
    weight: tokio::sync::OnceCell<u32>,
    component: Component,
    pub(crate) pre: WorkflowPre<HostState>,
}

impl std::fmt::Debug for Prepared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Prepared")
            .field("module_hash_hex", &self.module_hash_hex)
            .field("core_instances", &self.core_instances)
            .field("memories", &self.memories)
            .field("tables", &self.tables)
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
    /// Build the engine (pool, epoch ticker, memory budget) and the production linkers
    /// ([`LinkerSet::new`]).
    pub fn new(cfg: EngineConfig, opts: RuntimeOptions) -> Result<Self, EngineError> {
        Self::with_linkers(cfg, opts, LinkerSet::new)
    }

    /// [`Runtime::new`] with the linker set built by `linkers` (13-07: tests use
    /// `LinkerSet::with_fixture_0_1_1` under `wit-next-fixture`). The set must link `0.1`.
    pub fn with_linkers(
        cfg: EngineConfig,
        opts: RuntimeOptions,
        linkers: impl FnOnce(&wasmtime::Engine) -> wasmtime::Result<LinkerSet>,
    ) -> Result<Self, EngineError> {
        let engine = OstroEngine::new(cfg)?;
        let linkers = linkers(engine.engine())?;
        if linkers.linker(WitLine::V0_1).is_none() {
            return Err(EngineError::Wasmtime(wasmtime::format_err!(
                "the runtime's linker set has no 0.1 linker"
            )));
        }
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
        let pool_budget = PoolBudget::new(match engine.pool_mode() {
            PoolMode::Pooling => {
                let c = engine.config();
                PoolWeights {
                    instances: c.pool.total_instances,
                    core_instances: c.effective_total_core_instances(),
                    memories: c.effective_total_memories(),
                    tables: c.effective_total_tables(),
                }
            }
            PoolMode::OnDemand { .. } => PoolWeights::UNBOUNDED,
        });
        Ok(Self {
            engine,
            linkers,
            parked,
            aot,
            l2: Mutex::new(InstancePreLru::default()),
            live: AtomicU64::new(0),
            pool_budget,
            opts,
        })
    }

    /// The node's core-instance admission budget (plan 09-07): the core-instance part of
    /// [`Self::pool_budget`].
    pub fn core_budget(&self) -> &CoreBudget {
        self.pool_budget
            .budget(crate::core_budget::PoolResource::CoreInstances)
    }

    /// The node's admission budget over every pooled resource (plan eng2-mem).
    pub fn pool_budget(&self) -> &PoolBudget {
        &self.pool_budget
    }

    /// Core-instance capacity a module must fit in (`None`: on-demand, unbounded).
    fn max_core_instances(&self) -> Option<u32> {
        match self.engine.pool_mode() {
            PoolMode::Pooling => Some(self.core_budget().capacity()),
            PoolMode::OnDemand { .. } => None,
        }
    }

    /// Permit for one short-lived instantiation (`describe`, warm probe).
    async fn probe_permit(&self, p: &Prepared) -> Result<PoolPermit, EngineError> {
        let deadline = Instant::now() + BUDGET_PROBE_TIMEOUT;
        self.admit(p, deadline)
            .await
            .map_err(|e| EngineError::Wasmtime(wasmtime::format_err!("{e}")))
    }

    /// Acquire `p`'s admission weights ([`Self::admission_weights`]) until `deadline`.
    async fn admit(
        &self,
        p: &Prepared,
        deadline: Instant,
    ) -> Result<PoolPermit, crate::core_budget::PoolBudgetError> {
        let w = self.weights_by(p, deadline).await;
        self.pool_budget.acquire(w, deadline).await
    }

    /// Pool slots one instantiation of `p` takes, per resource: 1 component instance, the
    /// calibrated core-instance weight ([`Self::core_weight`]) and its exact memories and
    /// tables (plan eng2-mem).
    pub async fn admission_weights(&self, p: &Prepared) -> PoolWeights {
        self.weights_by(p, Instant::now() + BUDGET_PROBE_TIMEOUT)
            .await
    }

    async fn weights_by(&self, p: &Prepared, deadline: Instant) -> PoolWeights {
        PoolWeights {
            instances: 1,
            core_instances: self.core_weight_by(p, deadline).await,
            memories: p.memories,
            tables: p.tables,
        }
    }

    /// Admission weight of `p`: the core instances one instantiation really creates.
    ///
    /// With the pooling allocator this is calibrated once per prepared module (see the
    /// module doc); until a calibration succeeds (it needs `static + margin` free slots
    /// and a successful instantiation) the conservative `static + CALIBRATION_MARGIN` is
    /// used. Capped at the budget capacity (a module above it fails at instantiation with
    /// Wasmtime's pool error, as before). On-demand: the static count (no pool limit).
    pub async fn core_weight(&self, p: &Prepared) -> u32 {
        self.core_weight_by(p, Instant::now() + BUDGET_PROBE_TIMEOUT)
            .await
    }

    async fn core_weight_by(&self, p: &Prepared, deadline: Instant) -> u32 {
        let cap = self.core_budget().capacity();
        if self.max_core_instances().is_none() {
            return p.core_instances.min(cap);
        }
        if let Some(w) = p.weight.get() {
            return (*w).min(cap);
        }
        let fallback = p.core_instances.saturating_add(CALIBRATION_MARGIN).min(cap);
        let calibrated = p
            .weight
            .get_or_try_init(|| async {
                // The calibration instantiates from the same pool: it takes the module's
                // full admission weights, with the conservative core-instance count.
                let w = PoolWeights {
                    instances: 1,
                    core_instances: fallback,
                    memories: p.memories,
                    tables: p.tables,
                };
                let _permit = self
                    .pool_budget
                    .acquire(w, deadline)
                    .await
                    .map_err(|_| ())?;
                self.calibrate(p, fallback).await.ok_or(())
            })
            .await;
        calibrated.map_or(fallback, |w| (*w).min(cap))
    }

    /// Instantiate `p` once in a throwaway store whose instance limit starts at the static
    /// count and grows until instantiation succeeds; returns that limit (the number of core
    /// instances Wasmtime created). `None` on any other error or past `max`.
    async fn calibrate(&self, p: &Prepared, max: u32) -> Option<u32> {
        use wasmtime::{StoreLimits, StoreLimitsBuilder};
        let engine = self.engine.engine();
        let mut linker: Linker<StoreLimits> = Linker::new(engine);
        linker.define_unknown_imports_as_traps(&p.component).ok()?;
        for limit in p.core_instances.max(1)..=max {
            let limits = StoreLimitsBuilder::new().instances(limit as usize).build();
            let mut store = Store::new(engine, limits);
            store.limiter(|l| l);
            store.set_fuel(u64::MAX).ok()?;
            store.set_epoch_deadline(u64::MAX);
            match linker.instantiate_async(&mut store, &p.component).await {
                Ok(_) => {
                    metrics::histogram!("ostro_module_core_instances").record(f64::from(limit));
                    return Some(limit);
                }
                Err(e) if format!("{e:#}").contains("instance count too high") => continue,
                Err(e) => {
                    tracing::debug!(
                        target: "ostro::engine",
                        module = %p.module_hash_hex,
                        error = %format!("{e:#}"),
                        "core-instance calibration failed; using the conservative weight"
                    );
                    return None;
                }
            }
        }
        None
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
        // A sha has exactly one line and only 0.1 is executable: no parse on the hit path.
        if let Some(p) = self.l2().get(&(WitLine::V0_1, sha)) {
            return Ok(p);
        }
        reject_core_module(wasm)?;
        let line = Self::key_line(wasm);
        let component = match &self.aot {
            Some(aot) => aot.component_for(sha, wasm, line).await?,
            None => Component::from_binary(self.engine.engine(), wasm).map_err(|e| {
                EngineError::Validation(vec![format!("not a valid Wasm component: {e:#}")])
            })?,
        };
        let validated = self.validate_for_line(line, &component, wasm)?;
        let prepared = Arc::new(self.pre_instantiate(sha, &validated)?);
        Ok(self.l2().insert(prepared, self.opts.instance_pre_capacity))
    }

    /// The prepared module with SHA-256 `sha` if it is in the L2 cache (bumps its LRU
    /// recency). Never reads, hashes or compiles module bytes and never touches the L1
    /// AOT cache: the executor's hot path for already-deployed modules.
    pub fn prepared(&self, sha: &[u8; 32]) -> Option<Arc<Prepared>> {
        self.l2().get(&(WitLine::V0_1, *sha))
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
        // Memories and tables are exact: every core module the compiled component
        // instantiates, adapter modules included. `None` only for a component importing
        // core modules, which the allow-list rules out; fall back to the per-instance share
        // the pool is sized with (1 memory, `tables_per_instance` tables).
        let cfg = self.engine.config();
        let (memories, tables, min_memory_bytes) = match validated.component.resources_required() {
            Some(r) => (
                r.num_memories,
                r.num_tables,
                r.max_initial_memory_size
                    .map_or(0, |pages| pages.saturating_mul(65_536)),
            ),
            None => (1, cfg.class.tables_per_instance(), 0),
        };
        if matches!(self.engine.pool_mode(), PoolMode::Pooling) {
            let mut violations = Vec::new();
            let (max_mem, max_tab) = (cfg.effective_total_memories(), cfg.effective_total_tables());
            if memories > max_mem {
                violations.push(format!(
                    "component needs {memories} linear memories per instance, more than the \
                     node's pool holds ({max_mem}, engine.pool_total_instances): it could \
                     never be instantiated"
                ));
            }
            if tables > max_tab {
                violations.push(format!(
                    "component needs {tables} tables per instance, more than the node's pool \
                     holds ({max_tab}, engine.pool_total_instances × 2): it could never be \
                     instantiated"
                ));
            }
            if !violations.is_empty() {
                return Err(EngineError::Validation(violations));
            }
        }
        if validated.selection.line != WitLine::V0_1 {
            return Err(EngineError::Validation(vec![format!(
                "WIT line {} links on this host but has no activation driver (only 0.1 runs)",
                validated.selection.line
            )]));
        }
        let pre = WorkflowPre::new(self.linker().instantiate_pre(&validated.component)?)?;
        Ok(Prepared {
            sha,
            module_hash_hex: hex(&sha),
            wit: validated.selection.clone(),
            core_instances: validated.core_instances,
            memories,
            tables,
            min_memory_bytes,
            weight: tokio::sync::OnceCell::new(),
            component: validated.component.clone(),
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
        let _permit = self.probe_permit(p).await?;
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

    /// The `0.1` line's linker (the production linker every executable module uses).
    pub fn linker(&self) -> &Linker<HostState> {
        self.linkers
            .linker(WitLine::V0_1)
            .expect("checked at construction: the linker set links 0.1")
    }

    /// Every WIT line's linker (13-07).
    pub fn linkers(&self) -> &LinkerSet {
        &self.linkers
    }

    /// The line a module's cache entries are keyed under, read from its import names before
    /// compiling ([`line_from_bytes`]). A module whose line cannot be read is keyed under
    /// `0.1`; validation then reports why it is rejected (with every other violation).
    fn key_line(wasm: &[u8]) -> WitLine {
        line_from_bytes(wasm).unwrap_or(WitLine::V0_1)
    }

    /// Validate `component` against `line`'s linker; the selection must agree with `line`.
    fn validate_for_line(
        &self,
        line: WitLine,
        component: &Component,
        wasm: &[u8],
    ) -> Result<ValidatedComponent, EngineError> {
        let linker = self.linkers.linker(line).unwrap_or(self.linker());
        let validated = validate_loaded(
            self.engine.engine(),
            linker,
            component,
            wasm,
            self.max_core_instances(),
        )?;
        if validated.selection.line != line {
            return Err(EngineError::Validation(vec![format!(
                "WIT line mismatch: imports name line {line}, the component type selects {}",
                validated.selection.line
            )]));
        }
        Ok(validated)
    }

    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.opts.clock
    }

    /// Validate `wasm` against the production linker and pre-instantiate it.
    pub fn prepare(&self, wasm: &[u8]) -> Result<Prepared, EngineError> {
        let sha: [u8; 32] = Sha256::digest(wasm).into();
        reject_core_module(wasm)?;
        let component = Component::from_binary(self.engine.engine(), wasm).map_err(|e| {
            EngineError::Validation(vec![format!("not a valid Wasm component: {e:#}")])
        })?;
        let validated = self.validate_for_line(Self::key_line(wasm), &component, wasm)?;
        self.pre_instantiate(sha, &validated)
    }

    /// Call the component's `describe()` in a throwaway store (synthetic journal with a
    /// zero seed; nothing is journaled).
    pub async fn describe(&self, p: &Prepared) -> Result<Vec<WorkflowMeta>, EngineError> {
        let _permit = self.probe_permit(p).await?;
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
    /// returns it. Counted in [`live_activations`](Self::live_activations) from entry
    /// until the activation ends (or its future is dropped).
    pub async fn activate(
        &self,
        p: &Prepared,
        act: Activate,
        sink: Arc<dyn ActivationSink>,
    ) -> ActivationOutcome {
        let _live = LiveGuard::enter(&self.live);
        self.publish_gauges();
        // 09-07 (ENG-2) / eng2-mem: admit by core-instance, memory, table and instance
        // weight; the permit is held across the whole activation (instantiate, run,
        // hot-wait parking) and released on return.
        let deadline = Instant::now() + self.engine.config().activation_timeout;
        let permit = tokio::select! {
            r = self.admit(p, deadline) => r.ok(),
            () = act.cancel.cancelled() => None,
        };
        let Some(_permit) = permit else {
            let reason = if act.cancel.is_cancelled() {
                EvictReason::External
            } else {
                EvictReason::BudgetPressure
            };
            metrics::counter!("ostro_core_budget_evictions_total").increment(1);
            let outcome = ActivationOutcome::Evicted { reason };
            sink.done(&act.run, act.attempt, outcome.clone()).await;
            self.publish_gauges();
            return outcome;
        };
        let outcome = crate::activation::run_activation(self, p, act, sink).await;
        self.publish_gauges();
        outcome
    }

    /// Publish the node-wide instance gauges (OBS-02, plan 08-16):
    /// `ostro_instance_memory_bytes` (linear memory charged by live instances, parked ones
    /// included), `ostro_wasm_memory_budget_bytes` (the soft node budget) and
    /// `ostro_parked_instances`. Called at activation start and end; the memory reads are
    /// atomics, the parked count takes the registry's own (per-park) lock once.
    pub fn publish_gauges(&self) {
        let budget = self.parked.budget();
        metrics::gauge!("ostro_instance_memory_bytes").set(budget.used() as f64);
        metrics::gauge!("ostro_wasm_memory_budget_bytes").set(budget.limit() as f64);
        metrics::gauge!("ostro_parked_instances").set(self.parked.parked_count() as f64);
    }

    /// Number of activations running right now on this runtime, hot-parked ones
    /// included (WF-04 gauge). Evicted runs hold no activation: once every run is
    /// sleeping (evicted, Suspended) this is 0.
    pub fn live_activations(&self) -> u64 {
        self.live.load(Ordering::Relaxed)
    }
}
