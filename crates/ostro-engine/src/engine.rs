//! Shared Wasmtime engine, epoch ticker and pooling setup (ENG-01, ENG-05, ENG-08).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use wasmtime::{PoolConcurrencyLimitError, PoolingAllocatorMetrics, Store, UpdateDeadline};

use crate::config::{EngineConfig, build_on_demand_config, build_wasmtime_config};
use crate::error::{EngineError, StepTimedOut};
use crate::limits::{HasLimits, NodeMemoryBudget, OstroLimiter};

/// Per-slot address space the pooling allocator reserves: 4 GiB memory reservation
/// plus a 32 MiB guard region (Wasmtime 50 defaults).
const SLOT_RESERVATION_BYTES: u128 = (4 << 30) + (32 << 20);
const GIB: u128 = 1 << 30;

/// Which instance allocator the engine ended up with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolMode {
    Pooling,
    /// The pooling reservation failed (or pooling is disabled) and the on-demand
    /// allocator is in use; `reason` explains why.
    OnDemand {
        reason: String,
    },
}

/// Observable state of the epoch ticker (tests and diagnostics).
#[derive(Debug, Clone)]
pub struct TickerProbe {
    ticks: Arc<AtomicU64>,
    exited: Arc<AtomicBool>,
}

impl TickerProbe {
    /// Number of `increment_epoch` calls made so far.
    pub fn ticks(&self) -> u64 {
        self.ticks.load(Ordering::Acquire)
    }

    /// Whether the ticker thread has exited.
    pub fn exited(&self) -> bool {
        self.exited.load(Ordering::Acquire)
    }
}

struct TickerHandle {
    stop: Arc<AtomicBool>,
    probe: TickerProbe,
    thread: Option<JoinHandle<()>>,
}

impl TickerHandle {
    fn spawn(engine: &wasmtime::Engine, tick: Duration) -> Self {
        let weak = engine.weak();
        let stop = Arc::new(AtomicBool::new(false));
        let probe = TickerProbe {
            ticks: Arc::new(AtomicU64::new(0)),
            exited: Arc::new(AtomicBool::new(false)),
        };
        let (thread_stop, thread_probe) = (stop.clone(), probe.clone());
        let thread = std::thread::Builder::new()
            .name("ostro-epoch-ticker".into())
            .spawn(move || {
                let mut next = Instant::now() + tick;
                loop {
                    // park_timeout so Drop can wake us immediately via unpark.
                    let now = Instant::now();
                    if now < next {
                        std::thread::park_timeout(next - now);
                    }
                    if thread_stop.load(Ordering::Acquire) {
                        break;
                    }
                    if Instant::now() < next {
                        continue; // spurious wake-up
                    }
                    let Some(engine) = weak.upgrade() else { break };
                    engine.increment_epoch();
                    drop(engine);
                    thread_probe.ticks.fetch_add(1, Ordering::AcqRel);
                    next += tick;
                    let now = Instant::now();
                    if next < now {
                        next = now + tick; // fell behind (suspended host); don't burst
                    }
                }
                thread_probe.exited.store(true, Ordering::Release);
            })
            .expect("spawn epoch ticker thread");
        Self {
            stop,
            probe,
            thread: Some(thread),
        }
    }
}

impl Drop for TickerHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            let _ = thread.join();
        }
    }
}

/// The node's Wasmtime engine plus its allocator mode, memory budget and epoch ticker.
pub struct OstroEngine {
    engine: wasmtime::Engine,
    config: EngineConfig,
    mode: PoolMode,
    budget: Arc<NodeMemoryBudget>,
    ticker: TickerHandle,
}

impl std::fmt::Debug for OstroEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OstroEngine")
            .field("config", &self.config)
            .field("mode", &self.mode)
            .field("budget", &self.budget)
            .finish_non_exhaustive()
    }
}

/// Wrap a pooling-allocator construction failure with the config keys to change.
fn pool_reservation_message(total_instances: u32, err: &wasmtime::Error) -> String {
    let reserve_gib = u128::from(total_instances) * SLOT_RESERVATION_BYTES / GIB;
    format!(
        "pooling allocator could not reserve ~{reserve_gib} GiB of virtual address space \
         (engine.pool.total_instances={total_instances} × (4 GiB reservation + 32 MiB guard)): \
         {err:#}. Lower engine.pool.total_instances, or check `ulimit -v`, \
         vm.overcommit_memory and vm.max_map_count"
    )
}

impl OstroEngine {
    /// Build the engine. With pooling enabled, a reservation failure is wrapped as
    /// [`EngineError::PoolReservation`]; with `engine.pool.fallback = on-demand` (the
    /// default) a warning is logged and the on-demand allocator is used instead.
    pub fn new(config: EngineConfig) -> Result<Self, EngineError> {
        let (engine, mode) = if config.pool.enabled {
            match wasmtime::Engine::new(&build_wasmtime_config(&config)) {
                Ok(engine) => (engine, PoolMode::Pooling),
                Err(err) => {
                    let message = pool_reservation_message(config.pool.total_instances, &err);
                    match config.pool.fallback {
                        crate::config::Fallback::Fail => {
                            return Err(EngineError::PoolReservation { message });
                        }
                        crate::config::Fallback::OnDemand => {
                            tracing::warn!(
                                reason = %message,
                                "falling back to the on-demand instance allocator"
                            );
                            let engine = wasmtime::Engine::new(&build_on_demand_config(&config))?;
                            (engine, PoolMode::OnDemand { reason: message })
                        }
                    }
                }
            }
        } else {
            let engine = wasmtime::Engine::new(&build_on_demand_config(&config))?;
            (
                engine,
                PoolMode::OnDemand {
                    reason: "engine.pool.enabled=false".to_string(),
                },
            )
        };
        let budget = NodeMemoryBudget::new(config.node_memory_budget);
        let ticker = TickerHandle::spawn(&engine, config.epoch_tick);
        Ok(Self {
            engine,
            config,
            mode,
            budget,
            ticker,
        })
    }

    pub fn engine(&self) -> &wasmtime::Engine {
        &self.engine
    }

    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn pool_mode(&self) -> &PoolMode {
        &self.mode
    }

    pub fn budget(&self) -> &Arc<NodeMemoryBudget> {
        &self.budget
    }

    /// Epoch ticker state (tick count, exited flag).
    pub fn ticker_probe(&self) -> TickerProbe {
        self.ticker.probe.clone()
    }

    /// A store limiter charging this engine's node memory budget.
    pub fn new_limiter(&self) -> OstroLimiter {
        OstroLimiter::new(
            usize::try_from(self.config.effective_max_memory()).unwrap_or(usize::MAX),
            self.config.max_table_elements,
            self.budget.clone(),
        )
    }

    /// Pooling allocator metrics; `None` with the on-demand allocator.
    pub fn pooling_metrics(&self) -> Option<PoolingAllocatorMetrics> {
        self.engine.pooling_allocator_metrics()
    }

    /// Decommit idle warm pool slots; returns the number of slots released.
    pub fn release_idle_pool_memory(&self) -> usize {
        self.engine.release_idle_pool_memory()
    }

    /// Render an instantiation error, naming `engine.pool.total_instances` when the
    /// pool's concurrency limit was hit.
    pub fn explain_instantiate_error(&self, e: &wasmtime::Error) -> String {
        let text = format!("{e:#}");
        let exhausted = e.downcast_ref::<PoolConcurrencyLimitError>().is_some()
            || text.contains("maximum concurrent limit");
        if exhausted {
            format!(
                "instance pool exhausted (engine.pool.total_instances={}): {text}",
                self.config.pool.total_instances
            )
        } else {
            text
        }
    }
}

/// Install the limiter, fuel and the epoch deadline callback on a store.
///
/// The callback yields to the async executor every epoch tick (time slicing on
/// fibers) and interrupts the guest (`Trap::Interrupt`) once the store's wall
/// deadline has passed. When the open live step's `timeout-ms` deadline
/// ([`HasLimits::step_deadline`]) passes first, it traps with [`StepTimedOut`] instead,
/// so the driver fails that step rather than the attempt (WF-02, plan 04-10).
///
/// The effective deadline is `min(wall deadline, step deadline)`; a step deadline is
/// checked at epoch granularity (`engine.epoch_tick`). Wasmtime 50.0.2 hands the
/// callback a `StoreHookState<T>` whose `data()` is the store data, so the per-step
/// deadline lives in `HostState` and is moved by `begin-step` / `complete`.
pub fn configure_store<T: HasLimits + Send + 'static>(
    store: &mut Store<T>,
    fuel: u64,
) -> wasmtime::Result<()> {
    store.limiter(|t| t.limiter());
    store.set_fuel(fuel)?;
    store.set_epoch_deadline(1);
    store.epoch_deadline_callback(|st| {
        let now = Instant::now();
        let wall = st.data().wall_deadline();
        if let Some((seq, at)) = st.data().step_deadline()
            && now >= at
            && at <= wall
        {
            return Err(wasmtime::Error::new(StepTimedOut { seq }));
        }
        if now >= wall {
            Ok(UpdateDeadline::Interrupt)
        } else {
            Ok(UpdateDeadline::Yield(1))
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_message_names_keys() {
        let msg = pool_reservation_message(40_000, &wasmtime::format_err!("mmap failed"));
        assert!(msg.contains("~161250 GiB"), "{msg}");
        assert!(msg.contains("engine.pool.total_instances=40000"));
        assert!(msg.contains("`ulimit -v`"));
        assert!(msg.contains("vm.overcommit_memory"));
        assert!(msg.contains("mmap failed"));
    }
}
