//! Engine runtime facade (plan 03-08): one [`Runtime`] per node owns the Wasmtime
//! engine, the production linker, the clock used to stamp live events and the
//! await-token key.
//!
//! [`Runtime::prepare`] validates a component and builds its `InstancePre` (the
//! uncached entry point; 03-11 adds the AOT cache + InstancePre LRU path).
//! [`Runtime::activate`] runs one activation through [`crate::activation`].

use std::sync::Arc;
use std::time::Instant;

use ostro_types::journal::{Event, EventBody, Payload, RngSeed, RunStarted};
use sha2::{Digest, Sha256};
use wasmtime::Store;
use wasmtime::component::Linker;

use crate::config::EngineConfig;
use crate::engine::{OstroEngine, configure_store};
use crate::env::{Clock, SystemClock};
use crate::error::EngineError;
use crate::host::bindings::WorkflowPre;
use crate::host::state::{HostInit, HostState, build_linker};
use crate::sink::{Activate, ActivationOutcome, ActivationSink, RunRef};
use crate::validate::validate_component;

/// Node-level runtime options.
pub struct RuntimeOptions {
    /// Clock used only to stamp new (live) journal events.
    pub clock: Arc<dyn Clock>,
    /// Key for minting await tokens.
    pub token_key: [u8; 32],
}

impl std::fmt::Debug for RuntimeOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeOptions").finish_non_exhaustive()
    }
}

impl Default for RuntimeOptions {
    /// [`SystemClock`] and a process-local token key (sha256 over time + pid).
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
        }
    }
}

/// Engine + production linker + clock + token key.
pub struct Runtime {
    engine: OstroEngine,
    linker: Linker<HostState>,
    pub(crate) opts: RuntimeOptions,
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
        Ok(Self {
            engine,
            linker,
            opts,
        })
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
        let pre = WorkflowPre::new(self.linker.instantiate_pre(&validated.component)?)?;
        Ok(Prepared {
            sha,
            module_hash_hex: hex(&sha),
            pre,
        })
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
