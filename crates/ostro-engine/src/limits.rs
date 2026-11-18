//! Per-store resource limits (ENG-05): memory and table caps that trap on breach,
//! plus a soft node-wide memory budget shared by every live instance.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::Notify;

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
}

impl NodeMemoryBudget {
    pub fn new(limit: u64) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: AtomicU64::new(0),
            pressure: Notify::new(),
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
}
