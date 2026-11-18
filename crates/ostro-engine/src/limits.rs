//! Per-store resource limits (ENG-05): memory and table caps that trap on breach,
//! plus a soft node-wide memory budget shared by every live instance, and the
//! [`ParkedRegistry`] that reacts to budget pressure by evicting the least recently
//! parked hot-waiting activation (ENG-09).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::error::MemoryExceeded;

/// Soft memory budget shared by all instances on a node.
///
/// Grows are never refused because of the budget (that would make node load visible
/// to the guest); instead, crossing the limit signals [`NodeMemoryBudget::pressure`]
/// so the runtime can evict parked activations.
#[derive(Debug)]
pub struct NodeMemoryBudget {
    limit: u64,
    used: AtomicU64,
    pressure: Notify,
    released: Notify,
}

impl NodeMemoryBudget {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
            pressure: Notify::new(),
            released: Notify::new(),
        })
    }

    /// Charge `bytes`; notifies [`Self::pressure`] when the budget is exceeded.
    pub fn charge(&self, bytes: u64) {
        let now = self.used.fetch_add(bytes, Ordering::AcqRel) + bytes;
        if now > self.limit {
            // Single consumer (the runtime's eviction loop): `notify_one` stores a
            // permit if it is not currently waiting.
            self.pressure.notify_one();
        }
    }

    /// Release `bytes` previously charged.
    pub fn release(&self, bytes: u64) {
        let _ = self
            .used
            .try_update(Ordering::AcqRel, Ordering::Acquire, |u| {
                Some(u.saturating_sub(bytes))
            });
        self.released.notify_waiters();
    }

    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    pub fn is_over(&self) -> bool {
        self.used() > self.limit
    }

    /// Notified whenever a charge leaves the budget over its limit.
    pub fn pressure(&self) -> &Notify {
        &self.pressure
    }

    /// Woken (all current waiters) whenever bytes are released.
    pub fn released(&self) -> &Notify {
        &self.released
    }
}

/// How long the eviction loop waits for an evicted activation to drop its store
/// (releasing its memory) before re-checking the budget.
const EVICT_SETTLE: Duration = Duration::from_millis(250);

#[derive(Debug, Default)]
struct ParkedInner {
    tick: u64,
    next_id: u64,
    /// id -> (parked-at tick, eviction token).
    parked: HashMap<u64, (u64, CancellationToken)>,
}

/// Registry of hot-waiting (parked) activations, evicted least-recently-parked first
/// while the [`NodeMemoryBudget`] is over its limit.
///
/// Only parked activations are registered, so budget pressure can never interrupt a
/// running activation (it is evicted, if at all, only once it parks). An activation
/// that parks while the budget is over is registered like any other and wakes the
/// loop, so older parked activations are evicted before it. The eviction
/// loop is one tokio task listening on [`NodeMemoryBudget::pressure`]; it is started
/// on construction when a runtime is current, and (re)started lazily by
/// [`ParkedRegistry::park`] otherwise (e.g. after the runtime it ran on was dropped).
#[derive(Debug)]
pub struct ParkedRegistry {
    inner: Mutex<ParkedInner>,
    budget: Arc<NodeMemoryBudget>,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    me: Weak<ParkedRegistry>,
}

impl ParkedRegistry {
    pub fn new(budget: Arc<NodeMemoryBudget>) -> Arc<Self> {
        let reg = Arc::new_cyclic(|me| Self {
            inner: Mutex::new(ParkedInner::default()),
            budget,
            task: Mutex::new(None),
            me: me.clone(),
        });
        if tokio::runtime::Handle::try_current().is_ok() {
            reg.ensure_task();
        }
        reg
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ParkedInner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Start the eviction loop unless it is running. Requires a current tokio runtime.
    fn ensure_task(&self) {
        let mut task = self.task.lock().unwrap_or_else(|p| p.into_inner());
        if task.as_ref().is_some_and(|t| !t.is_finished()) {
            return;
        }
        let me = self.me.clone();
        let budget = self.budget.clone();
        *task = Some(tokio::spawn(async move {
            loop {
                budget.pressure().notified().await;
                let Some(reg) = me.upgrade() else { return };
                reg.relieve().await;
            }
        }));
    }

    /// Evict least-recently-parked activations while the budget is over its limit.
    async fn relieve(&self) {
        while self.budget.is_over() {
            let Some(token) = self.pop_oldest() else {
                return;
            };
            // Arm before cancelling so a release racing the cancel is not missed.
            let released = self.budget.released().notified();
            tokio::pin!(released);
            released.as_mut().enable();
            token.cancel();
            metrics::counter!("ostro_activation_budget_evictions_total").increment(1);
            tracing::debug!(
                used = self.budget.used(),
                limit = self.budget.limit(),
                "budget pressure: evicting least recently parked activation"
            );
            // Let the evicted activation drop its store before deciding again.
            let _ = tokio::time::timeout(EVICT_SETTLE, released).await;
        }
    }

    fn pop_oldest(&self) -> Option<CancellationToken> {
        let mut inner = self.lock();
        let id = inner
            .parked
            .iter()
            .min_by_key(|(_, (tick, _))| *tick)
            .map(|(id, _)| *id)?;
        inner.parked.remove(&id).map(|(_, t)| t)
    }

    /// Register a parked activation; it stays registered until the guard drops.
    /// Must be called from within a tokio runtime.
    pub fn park(&self) -> ParkGuard {
        self.ensure_task();
        let token = CancellationToken::new();
        let id = {
            let mut inner = self.lock();
            inner.tick += 1;
            inner.next_id += 1;
            let (id, tick) = (inner.next_id, inner.tick);
            inner.parked.insert(id, (tick, token.clone()));
            id
        };
        // Parking while over budget: let the LRU decide who goes (older parked
        // activations first, this one only if nothing older is left).
        if self.budget.is_over() {
            self.budget.pressure().notify_one();
        }
        ParkGuard {
            reg: self.me.clone(),
            id,
            token,
        }
    }

    /// Number of currently parked activations.
    pub fn parked_count(&self) -> usize {
        self.lock().parked.len()
    }

    pub fn budget(&self) -> &Arc<NodeMemoryBudget> {
        &self.budget
    }
}

/// Registration of one parked activation; dropping it unregisters.
#[derive(Debug)]
pub struct ParkGuard {
    reg: Weak<ParkedRegistry>,
    id: u64,
    token: CancellationToken,
}

impl ParkGuard {
    /// Cancelled when budget pressure evicts this activation.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }
}

impl Drop for ParkGuard {
    fn drop(&mut self) {
        if let Some(reg) = self.reg.upgrade() {
            reg.lock().parked.remove(&self.id);
        }
    }
}

/// Which limit a store breached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Breach {
    Memory,
    Table,
}

/// Wasmtime resource limiter for one store.
///
/// Every refusal is an `Err` (a trap), never `Ok(false)`: a `-1` from `memory.grow`
/// would let the guest observe and branch on node conditions (nondeterminism).
#[derive(Debug)]
pub struct OstroLimiter {
    max_memory: usize,
    max_table_elements: usize,
    budget: Arc<NodeMemoryBudget>,
    charged: u64,
    /// The limit breached by this store, if any (read by the activation to classify
    /// the trap).
    pub breach: Option<Breach>,
}

impl OstroLimiter {
    pub fn new(
        max_memory: usize,
        max_table_elements: usize,
        budget: Arc<NodeMemoryBudget>,
    ) -> Self {
        Self {
            max_memory,
            max_table_elements,
            budget,
            charged: 0,
            breach: None,
        }
    }

    /// Bytes this store currently has charged to the node budget.
    pub fn charged(&self) -> u64 {
        self.charged
    }

    pub fn max_memory(&self) -> usize {
        self.max_memory
    }

    pub fn max_table_elements(&self) -> usize {
        self.max_table_elements
    }
}

impl wasmtime::ResourceLimiter for OstroLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let limit = maximum.map_or(self.max_memory, |m| m.min(self.max_memory));
        if desired > limit {
            self.breach = Some(Breach::Memory);
            return Err(wasmtime::Error::new(MemoryExceeded {
                requested: desired,
                limit,
            }));
        }
        let delta = desired.saturating_sub(current) as u64;
        self.budget.charge(delta);
        self.charged += delta;
        Ok(true)
    }

    fn memory_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.breach.get_or_insert(Breach::Memory);
        Err(error.context("memory grow failed"))
    }

    fn table_growing(
        &mut self,
        _current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let limit = maximum.map_or(self.max_table_elements, |m| m.min(self.max_table_elements));
        if desired > limit {
            self.breach = Some(Breach::Table);
            return Err(wasmtime::format_err!(
                "table limit exceeded: requested {desired} elements, limit {limit}"
            ));
        }
        Ok(true)
    }

    fn table_grow_failed(&mut self, error: wasmtime::Error) -> wasmtime::Result<()> {
        self.breach.get_or_insert(Breach::Table);
        Err(error.context("table grow failed"))
    }
}

impl Drop for OstroLimiter {
    fn drop(&mut self) {
        self.budget.release(self.charged);
    }
}

/// Store data that carries an [`OstroLimiter`] and a wall-clock deadline
/// (implemented by `HostState`, plan 03-07; used by `engine::configure_store`).
pub trait HasLimits {
    fn limiter(&mut self) -> &mut OstroLimiter;
    fn wall_deadline(&self) -> Instant;
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasmtime::ResourceLimiter;

    const PAGE: usize = 65_536;

    fn limiter(budget: &Arc<NodeMemoryBudget>) -> OstroLimiter {
        OstroLimiter::new(4 * PAGE, 100, budget.clone())
    }

    #[test]
    fn memory_breach_traps_and_records() {
        let budget = NodeMemoryBudget::new(u64::MAX);
        let mut l = limiter(&budget);
        let err = l
            .memory_growing(PAGE, 5 * PAGE, None)
            .expect_err("over max must trap, never Ok(false)");
        let me = err
            .downcast_ref::<MemoryExceeded>()
            .expect("MemoryExceeded payload");
        assert_eq!(me.requested, 5 * PAGE);
        assert_eq!(me.limit, 4 * PAGE);
        assert_eq!(l.breach, Some(Breach::Memory));
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn grow_failed_traps() {
        let budget = NodeMemoryBudget::new(u64::MAX);
        let mut l = limiter(&budget);
        assert!(l.memory_grow_failed(wasmtime::format_err!("os")).is_err());
        assert_eq!(l.breach, Some(Breach::Memory));
        let mut l = limiter(&budget);
        assert!(l.table_grow_failed(wasmtime::format_err!("x")).is_err());
        assert_eq!(l.breach, Some(Breach::Table));
    }

    #[test]
    fn table_breach_traps_and_records() {
        let budget = NodeMemoryBudget::new(u64::MAX);
        let mut l = limiter(&budget);
        assert!(l.table_growing(10, 101, None).is_err());
        assert_eq!(l.breach, Some(Breach::Table));
        let mut l = limiter(&budget);
        assert!(l.table_growing(10, 100, None).expect("within limit"));
        assert_eq!(l.breach, None);
    }

    #[test]
    fn within_limits_charges_and_drop_releases() {
        let budget = NodeMemoryBudget::new(u64::MAX);
        budget.charge(7); // someone else's charge
        {
            let mut l = limiter(&budget);
            assert!(l.memory_growing(0, PAGE, None).expect("ok"));
            assert!(l.memory_growing(PAGE, 3 * PAGE, None).expect("ok"));
            assert_eq!(budget.used(), 7 + 3 * PAGE as u64);
            assert_eq!(l.charged(), 3 * PAGE as u64);
            assert_eq!(l.breach, None);
        }
        assert_eq!(budget.used(), 7);
    }

    #[test]
    fn soft_budget_allows_grow_and_signals_pressure() {
        let budget = NodeMemoryBudget::new(PAGE as u64);
        let mut l = limiter(&budget);
        assert!(
            l.memory_growing(0, 2 * PAGE, None)
                .expect("soft budget allows")
        );
        assert!(budget.is_over());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                budget.pressure().notified(),
            )
            .await
            .expect("pressure notified");
        });
        drop(l);
        assert!(!budget.is_over());
        assert_eq!(budget.used(), 0);
    }

    #[tokio::test]
    async fn parked_registry_evicts_least_recently_parked_first() {
        let budget = NodeMemoryBudget::new(100);
        let reg = ParkedRegistry::new(budget.clone());
        let a = reg.park();
        let b = reg.park();
        let c = reg.park();
        assert_eq!(reg.parked_count(), 3);
        // A running activation pushes the budget over; the oldest parked is evicted.
        budget.charge(150);
        tokio::time::timeout(Duration::from_secs(2), a.token().cancelled())
            .await
            .expect("oldest parked evicted");
        // Its release brings the budget back under: nobody else is evicted.
        budget.release(100);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!b.token().is_cancelled());
        assert!(!c.token().is_cancelled());
        assert_eq!(reg.parked_count(), 2);
        drop(b);
        assert_eq!(reg.parked_count(), 1);
        drop(c);
        drop(a);
        assert_eq!(reg.parked_count(), 0);
    }

    #[test]
    fn parked_registry_starts_lazily_without_runtime() {
        let budget = NodeMemoryBudget::new(10);
        let reg = ParkedRegistry::new(budget.clone());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        rt.block_on(async {
            let g = reg.park();
            budget.charge(20);
            tokio::time::timeout(Duration::from_secs(2), g.token().cancelled())
                .await
                .expect("evicted");
        });
    }
}
