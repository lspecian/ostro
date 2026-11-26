//! Phase 14 (14-06, D-P14-14/15, DISP-06): `TenantFlow`, the tenant level above the queue
//! level of the node flow control.
//!
//! Node-wide per-tenant state, shared by every partition dispatcher of the node (through
//! [`NodeFlow`](super::NodeFlow)), enforced on this node's leadership share of each quota
//! ([`TenantQuotaView::share`]):
//!
//! - **Suspended** → `Blocked{until: None}`: the dispatcher holds the tenant's ready
//!   entries (never failed) until a `changed()` bump (the resume's meta apply).
//! - **`max_concurrency`** counts leased activations (D-W13 unit): `running <
//!   share(max_concurrency)` or `Blocked{None}` until a release.
//! - **`memory_bytes`** reserves the instance memory cap per leased activation: admitted
//!   while `reserved + mem_limit <= share(memory_bytes)`, and always when the tenant holds
//!   nothing (a quota below one instance delays, it never blocks forever).
//! - **`fuel_per_s` / `fuel_burst`**: a token bucket with **debt** (milli-fuel precision,
//!   refilled from the injected clock's milliseconds). [`TenantFlow::charge_fuel`] takes
//!   the consumed fuel after each activation and may drive the balance negative; a tenant
//!   whose balance is not positive is `Blocked{until: refill time}`. The bucket starts
//!   full; the burst defaults to one second of refill.
//!
//! A granted admission carries a [`TenantPermit`]: the concurrency and memory charge is
//! taken at admission and is **tentative** until the dispatcher's placement grants
//! capacity ([`TenantPermit::commit`], done by the node flow's `FlowPermit::commit`); a
//! dropped uncommitted permit rolls it back, a dropped committed permit releases it at
//! activation end. Unknown tenants (no `Tenant` record) and nodes without a quota view are
//! unlimited (counted for the metrics only).
//!
//! Metrics (W9): `ostro_tenant_running`, `ostro_tenant_memory_reserved_bytes`,
//! `ostro_tenant_fuel_balance` (gauges) and `ostro_tenant_blocked_total{reason}` (`reason`
//! = `suspended`, `concurrency`, `memory`, `fuel`). The `tenant` label exists only with
//! `[tenants] metrics_tenant_labels = true` (default false: `/metrics` is unauthenticated
//! and tenant ids would leak) and is capped at `metrics_label_limit` distinct values (the
//! rest share `other`); without it the gauges are node totals.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use crate::p14::quota_view::TenantQuotaView;

/// The label value of tenants beyond `metrics_label_limit`.
const OTHER: &str = "other";

/// A token bucket with debt, in milli-fuel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Bucket {
    milli: i128,
    at_ms: u64,
}

impl Bucket {
    fn full(burst: u64, now_ms: u64) -> Self {
        Self {
            milli: i128::from(burst) * 1000,
            at_ms: now_ms,
        }
    }

    /// Refill at `rate` fuel/s up to `burst` (a clock that went back refills nothing).
    fn refill(&mut self, rate: u64, burst: u64, now_ms: u64) {
        let elapsed = now_ms.saturating_sub(self.at_ms);
        let cap = i128::from(burst) * 1000;
        if self.milli < cap {
            self.milli = (self.milli + i128::from(elapsed) * i128::from(rate)).min(cap);
        }
        self.at_ms = self.at_ms.max(now_ms);
    }

    /// The first instant (clock ms) the balance is positive again.
    fn positive_at(&self, rate: u64) -> u64 {
        if self.milli > 0 || rate == 0 {
            return self.at_ms;
        }
        // Need `1 - milli` milli-fuel at `rate` milli-fuel per ms.
        let need = 1 - self.milli;
        let rate = i128::from(rate);
        let ms = (need + rate - 1) / rate;
        self.at_ms
            .saturating_add(u64::try_from(ms).unwrap_or(u64::MAX))
    }

    fn fuel(&self) -> i128 {
        self.milli.div_euclid(1000)
    }
}

#[derive(Debug, Default)]
struct TenantState {
    running: u32,
    memory: u64,
    bucket: Option<Bucket>,
}

/// Result of [`TenantFlow::admit`].
#[derive(Debug)]
pub enum Admit {
    /// Admitted; the charge is tentative until [`TenantPermit::commit`].
    Granted(TenantPermit),
    /// Not now: retry at `until` (clock ms; a fuel refill) or on the next `changed()`
    /// bump (`None`). `reason` is the metric label.
    Blocked {
        until: Option<u64>,
        reason: &'static str,
    },
}

#[derive(Debug, Default)]
struct Inner {
    view: OnceLock<Arc<TenantQuotaView>>,
    state: Mutex<HashMap<String, TenantState>>,
    /// Tenants that own a metric label (at most `metrics_label_limit`).
    labelled: Mutex<HashSet<String>>,
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, HashMap<String, TenantState>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Undo a charge.
    fn uncharge(&self, tenant: &str, memory: u64) {
        let mut st = self.state();
        if let Some(t) = st.get_mut(tenant) {
            t.running = t.running.saturating_sub(1);
            t.memory = t.memory.saturating_sub(memory);
        }
        self.publish(&st, tenant);
    }

    /// The label value of `tenant` (`None` = labels off).
    fn label(&self, tenant: &str) -> Option<String> {
        let view = self.view.get()?;
        if !view.metrics_tenant_labels() {
            return None;
        }
        let mut l = self.labelled.lock().unwrap_or_else(PoisonError::into_inner);
        if l.contains(tenant) {
            return Some(tenant.to_owned());
        }
        if l.len() < usize::try_from(view.metrics_label_limit()).unwrap_or(usize::MAX) {
            l.insert(tenant.to_owned());
            return Some(tenant.to_owned());
        }
        Some(OTHER.to_owned())
    }

    /// Publish the gauges after a change of `tenant`'s state.
    fn publish(&self, st: &HashMap<String, TenantState>, tenant: &str) {
        let sum = |keep: &dyn Fn(&str) -> bool| {
            st.iter()
                .filter(|(t, _)| keep(t))
                .fold((0u64, 0u64, 0i128), |(r, m, f), (_, s)| {
                    (
                        r + u64::from(s.running),
                        m.saturating_add(s.memory),
                        f + s.bucket.map_or(0, |b| b.fuel()),
                    )
                })
        };
        let (label, (running, memory, fuel)) = match self.label(tenant) {
            None => (None, sum(&|_| true)),
            Some(l) if l == OTHER => {
                let labelled = self.labelled.lock().unwrap_or_else(PoisonError::into_inner);
                (Some(l), sum(&|t| !labelled.contains(t)))
            }
            Some(l) => (Some(l), sum(&|t| t == tenant)),
        };
        #[allow(clippy::cast_precision_loss)]
        let (running, memory, fuel) = (running as f64, memory as f64, fuel as f64);
        match label {
            None => {
                metrics::gauge!("ostro_tenant_running").set(running);
                metrics::gauge!("ostro_tenant_memory_reserved_bytes").set(memory);
                metrics::gauge!("ostro_tenant_fuel_balance").set(fuel);
            }
            Some(l) => {
                metrics::gauge!("ostro_tenant_running", "tenant" => l.clone()).set(running);
                metrics::gauge!("ostro_tenant_memory_reserved_bytes", "tenant" => l.clone())
                    .set(memory);
                metrics::gauge!("ostro_tenant_fuel_balance", "tenant" => l).set(fuel);
            }
        }
    }

    fn blocked(&self, tenant: &str, reason: &'static str) {
        match self.label(tenant) {
            None => {
                metrics::counter!("ostro_tenant_blocked_total", "reason" => reason).increment(1)
            }
            Some(l) => metrics::counter!(
                "ostro_tenant_blocked_total", "reason" => reason, "tenant" => l
            )
            .increment(1),
        }
    }
}

/// A tentative (until [`commit`](Self::commit)) or committed per-tenant charge. Dropping
/// it gives the concurrency slot and the memory reservation back.
#[must_use = "a dropped TenantPermit gives its charge back"]
pub struct TenantPermit {
    inner: Option<Arc<Inner>>,
    tenant: String,
    memory: u64,
    capacity_limited: bool,
    committed: bool,
}

impl TenantPermit {
    /// A permit that charges nothing (no quota view).
    pub fn none() -> Self {
        Self {
            inner: None,
            tenant: String::new(),
            memory: 0,
            capacity_limited: false,
            committed: false,
        }
    }

    /// Make the charge final (placement granted capacity).
    pub fn commit(&mut self) {
        self.committed = true;
    }

    pub fn is_committed(&self) -> bool {
        self.committed
    }

    /// Whether the tenant has a concurrency or memory limit (its release may unblock it:
    /// the caller bumps `changed()`).
    pub fn capacity_limited(&self) -> bool {
        self.capacity_limited
    }

    /// The tenant charged.
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

impl Drop for TenantPermit {
    fn drop(&mut self) {
        // Rollback (uncommitted) and release (committed) undo the same charge.
        if let Some(inner) = self.inner.take() {
            inner.uncharge(&self.tenant, self.memory);
        }
    }
}

impl std::fmt::Debug for TenantPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantPermit")
            .field("tenant", &self.tenant)
            .field("memory", &self.memory)
            .field("committed", &self.committed)
            .finish_non_exhaustive()
    }
}

/// Per-tenant dispatch admission (module docs).
#[derive(Clone, Debug, Default)]
pub struct TenantFlow {
    inner: Arc<Inner>,
}

impl TenantFlow {
    /// A tenant flow without a quota view (admits everything until
    /// [`install`](Self::install)).
    pub fn new() -> Self {
        Self::default()
    }

    /// Enforce `view`'s quotas (the first call wins).
    pub fn install(&self, view: Arc<TenantQuotaView>) {
        let _ = self.inner.view.set(view);
    }

    /// The quota view, once installed.
    pub fn view(&self) -> Option<&Arc<TenantQuotaView>> {
        self.inner.view.get()
    }

    /// DRR quantum of `tenant` (1 without a view).
    pub fn weight(&self, tenant: &str) -> u32 {
        self.view().map_or(1, |v| v.weight(tenant))
    }

    /// Instance memory reserved per activation (0 without a view).
    pub fn instance_memory(&self) -> u64 {
        self.view().map_or(0, |v| v.instance_memory())
    }

    /// May one more activation of `tenant` reserving `mem_limit` bytes start at `now_ms`
    /// (injected clock)? Never blocks.
    pub fn admit(&self, tenant: &str, mem_limit: u64, now_ms: u64) -> Admit {
        let Some(view) = self.inner.view.get() else {
            return Admit::Granted(TenantPermit::none());
        };
        let q = view.quotas(tenant);
        let mut st = self.inner.state();
        let s = st.entry(tenant.to_owned()).or_default();
        let mut reserve = 0;
        let mut capacity_limited = false;
        if let Some(q) = q.as_deref() {
            let block = if q.suspended {
                Some((None, "suspended"))
            } else if q
                .max_concurrency
                .is_some_and(|mc| u64::from(s.running) >= view.share(u64::from(mc)))
            {
                Some((None, "concurrency"))
            } else if q.memory_bytes.is_some_and(|mb| {
                s.memory > 0 && s.memory.saturating_add(mem_limit) > view.share(mb)
            }) {
                Some((None, "memory"))
            } else if let Some(rate) = q.fuel_rate() {
                let (rate, burst) = (view.share(rate), view.share(q.fuel_burst.unwrap_or(rate)));
                let b = s.bucket.get_or_insert_with(|| Bucket::full(burst, now_ms));
                b.refill(rate, burst, now_ms);
                (b.milli <= 0).then(|| (Some(b.positive_at(rate).max(now_ms + 1)), "fuel"))
            } else {
                None
            };
            if let Some((until, reason)) = block {
                drop(st);
                self.inner.blocked(tenant, reason);
                return Admit::Blocked { until, reason };
            }
            if q.memory_bytes.is_some() {
                reserve = mem_limit;
            }
            capacity_limited = q.capacity_limited();
        }
        s.running = s.running.saturating_add(1);
        s.memory = s.memory.saturating_add(reserve);
        self.inner.publish(&st, tenant);
        Admit::Granted(TenantPermit {
            inner: Some(Arc::clone(&self.inner)),
            tenant: tenant.to_owned(),
            memory: reserve,
            capacity_limited,
            committed: false,
        })
    }

    /// Charge `consumed` fuel to `tenant` at `now_ms` after an activation ended: refill,
    /// then subtract (the balance may go negative: debt). No-op without a fuel quota.
    pub fn charge_fuel(&self, tenant: &str, consumed: u64, now_ms: u64) {
        let Some(view) = self.inner.view.get() else {
            return;
        };
        let Some(q) = view.quotas(tenant) else {
            return;
        };
        let Some(rate) = q.fuel_rate() else {
            return;
        };
        let (rate, burst) = (view.share(rate), view.share(q.fuel_burst.unwrap_or(rate)));
        let mut st = self.inner.state();
        let s = st.entry(tenant.to_owned()).or_default();
        let b = s.bucket.get_or_insert_with(|| Bucket::full(burst, now_ms));
        b.refill(rate, burst, now_ms);
        b.milli -= i128::from(consumed) * 1000;
        self.inner.publish(&st, tenant);
    }

    /// Leased activations of `tenant` holding a charge now.
    pub fn running(&self, tenant: &str) -> u32 {
        self.inner.state().get(tenant).map_or(0, |s| s.running)
    }

    /// Instance memory reserved by `tenant` now, bytes.
    pub fn memory_reserved(&self, tenant: &str) -> u64 {
        self.inner.state().get(tenant).map_or(0, |s| s.memory)
    }

    /// `tenant`'s fuel balance (negative = debt) as of its last refill; `None` without a
    /// fuel bucket.
    pub fn fuel_balance(&self, tenant: &str) -> Option<i128> {
        self.inner
            .state()
            .get(tenant)
            .and_then(|s| s.bucket.map(|b| b.fuel()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::p14::quota_view::Quotas;

    const MIB: u64 = 1 << 20;

    fn flow(tenants: &[(&str, Quotas)]) -> (TenantFlow, Arc<TenantQuotaView>) {
        let view = Arc::new(TenantQuotaView::default());
        let m: HashMap<String, Quotas> = tenants
            .iter()
            .map(|(t, q)| ((*t).to_owned(), q.clone()))
            .collect();
        view.set_tenants(m);
        let f = TenantFlow::new();
        f.install(Arc::clone(&view));
        (f, view)
    }

    fn granted(a: Admit) -> TenantPermit {
        match a {
            Admit::Granted(p) => p,
            Admit::Blocked { until, reason } => panic!("blocked: {reason} until {until:?}"),
        }
    }

    fn blocked(a: Admit) -> (Option<u64>, &'static str) {
        match a {
            Admit::Granted(p) => panic!("granted {p:?}"),
            Admit::Blocked { until, reason } => (until, reason),
        }
    }

    #[test]
    fn no_view_and_unknown_tenants_are_unlimited() {
        let f = TenantFlow::new();
        let p = granted(f.admit("acme", MIB, 0));
        assert_eq!(f.running("acme"), 0, "no view: nothing counted");
        drop(p);
        let (f, _) = flow(&[]);
        let ps: Vec<_> = (0..100).map(|_| granted(f.admit("acme", MIB, 0))).collect();
        assert_eq!(f.running("acme"), 100, "counted for metrics");
        assert_eq!(
            f.memory_reserved("acme"),
            0,
            "no memory quota: nothing reserved"
        );
        drop(ps);
        assert_eq!(f.running("acme"), 0);
    }

    #[test]
    fn concurrency_on_the_leadership_share() {
        let (f, view) = flow(&[(
            "a",
            Quotas {
                max_concurrency: Some(4),
                ..Quotas::default()
            },
        )]);
        let mut held: Vec<_> = (0..4).map(|_| granted(f.admit("a", 0, 0))).collect();
        assert_eq!(blocked(f.admit("a", 0, 0)), (None, "concurrency"));
        // Other tenants are unaffected.
        drop(granted(f.admit("b", 0, 0)));
        // Leading one partition of P: share = max(1, ceil(4 / P)).
        held.clear();
        view.set_led(1);
        let share = view.share(4);
        assert_eq!(share, 4u64.div_ceil(u64::from(view.partitions())).max(1));
        let held: Vec<_> = (0..share).map(|_| granted(f.admit("a", 0, 0))).collect();
        assert!(held.iter().all(TenantPermit::capacity_limited));
        assert_eq!(blocked(f.admit("a", 0, 0)).1, "concurrency");
        drop(held);
        // No leadership: nothing.
        view.set_led(0);
        assert_eq!(blocked(f.admit("a", 0, 0)).1, "concurrency");
    }

    #[test]
    fn uncommitted_permit_rolls_back_committed_releases() {
        let (f, _) = flow(&[(
            "a",
            Quotas {
                max_concurrency: Some(1),
                memory_bytes: Some(10 * MIB),
                ..Quotas::default()
            },
        )]);
        let p = granted(f.admit("a", MIB, 0));
        assert_eq!((f.running("a"), f.memory_reserved("a")), (1, MIB));
        assert!(!p.is_committed());
        drop(p); // placement blocked: rollback
        assert_eq!((f.running("a"), f.memory_reserved("a")), (0, 0));
        let mut p = granted(f.admit("a", MIB, 0));
        p.commit();
        assert_eq!(blocked(f.admit("a", MIB, 0)).1, "concurrency");
        drop(p); // activation end: release
        assert_eq!((f.running("a"), f.memory_reserved("a")), (0, 0));
        drop(granted(f.admit("a", MIB, 0)));
    }

    #[test]
    fn memory_reserves_the_instance_limit_and_never_blocks_an_idle_tenant() {
        let (f, _) = flow(&[(
            "a",
            Quotas {
                memory_bytes: Some(300 * MIB),
                ..Quotas::default()
            },
        )]);
        let p = granted(f.admit("a", 256 * MIB, 0));
        assert_eq!(blocked(f.admit("a", 256 * MIB, 0)), (None, "memory"));
        drop(p);
        // Below one instance: still one at a time (delay, never a permanent block).
        let (f, _) = flow(&[(
            "s",
            Quotas {
                memory_bytes: Some(MIB),
                ..Quotas::default()
            },
        )]);
        let p = granted(f.admit("s", 256 * MIB, 0));
        assert_eq!(blocked(f.admit("s", 256 * MIB, 0)).1, "memory");
        drop(p);
        drop(granted(f.admit("s", 256 * MIB, 0)));
    }

    #[test]
    fn fuel_debt_blocks_until_refilled() {
        let (f, _) = flow(&[(
            "a",
            Quotas {
                fuel_per_s: Some(1_000),
                fuel_burst: Some(2_000),
                ..Quotas::default()
            },
        )]);
        // Full bucket: admitted; charge 5,000 at t=0 -> balance 2,000 - 5,000 = -3,000.
        drop(granted(f.admit("a", 0, 0)));
        f.charge_fuel("a", 5_000, 0);
        assert_eq!(f.fuel_balance("a"), Some(-3_000));
        // Positive again after 3,001 ms (1 fuel/ms).
        assert_eq!(blocked(f.admit("a", 0, 1_000)), (Some(3_001), "fuel"));
        assert_eq!(blocked(f.admit("a", 0, 3_000)), (Some(3_001), "fuel"));
        drop(granted(f.admit("a", 0, 3_001)));
        // Refill caps at the burst.
        f.charge_fuel("a", 0, 1_000_000);
        assert_eq!(f.fuel_balance("a"), Some(2_000));
    }

    #[test]
    fn fuel_burst_defaults_to_one_second_and_a_clock_going_back_refills_nothing() {
        let (f, _) = flow(&[(
            "a",
            Quotas {
                fuel_per_s: Some(500),
                ..Quotas::default()
            },
        )]);
        f.charge_fuel("a", 0, 10_000);
        assert_eq!(f.fuel_balance("a"), Some(500));
        f.charge_fuel("a", 1_000, 10_000);
        assert_eq!(f.fuel_balance("a"), Some(-500));
        f.charge_fuel("a", 0, 5_000); // back in time
        assert_eq!(f.fuel_balance("a"), Some(-500));
        f.charge_fuel("a", 0, 11_000); // +500
        assert_eq!(f.fuel_balance("a"), Some(0));
        assert_eq!(blocked(f.admit("a", 0, 11_000)), (Some(11_001), "fuel"));
    }

    #[test]
    fn suspended_holds_with_no_deadline() {
        let (f, view) = flow(&[(
            "a",
            Quotas {
                suspended: true,
                ..Quotas::default()
            },
        )]);
        assert_eq!(blocked(f.admit("a", 0, 0)), (None, "suspended"));
        let mut m = HashMap::new();
        m.insert("a".to_owned(), Quotas::default());
        view.set_tenants(m);
        drop(granted(f.admit("a", 0, 0)));
    }

    #[test]
    fn weights_come_from_the_view() {
        let (f, _) = flow(&[(
            "a",
            Quotas {
                weight: Some(4),
                ..Quotas::default()
            },
        )]);
        assert_eq!(f.weight("a"), 4);
        assert_eq!(f.weight("b"), 1);
        assert_eq!(TenantFlow::new().weight("a"), 1);
    }
}
