//! Phase 14 (14-06, D-P14-14/15): `TenantQuotaView`, the tenant quotas this node applies.
//!
//! - **Records.** Every meta `Tenant` record's dispatch quotas (`weight`, `max_concurrency`,
//!   `memory_bytes`, `fuel_per_s`, `fuel_burst`, `suspended`) are cached in one immutable
//!   snapshot (`HashMap<tenant, Arc<Quotas>>` behind an [`ArcSwap`]) that [`start`]'s task
//!   swaps on every meta apply that changed one; the dispatch hot path is one atomic load
//!   plus a map lookup. A tenant without a record (the default tenant `""` included) is
//!   unlimited with weight 1. `event_rate` is enforced at ingress (14-20), not here.
//! - **Leadership share (D-P14-15).** `share(x) = max(1, ceil(x × led / P))` while the node
//!   leads `led > 0` of the `P = [node] partitions` partitions, `0` when it leads none (no
//!   dispatch happens without leadership). `led` comes from `PartitionRoles::led()` and is
//!   recomputed on every change. Until [`start`] has read the roles the node assumes it
//!   leads every partition (exact on a single node).
//! - **Changes.** Every swap (records or `led`) bumps [`TenantQuotaView::subscribe`]; the
//!   node flow forwards it to the dispatchers' `changed()`, so tenants blocked on a quota
//!   or a suspension are re-evaluated without polling.
//! - **Instance memory.** `memory_bytes` reserves the per-instance linear-memory cap
//!   (`EngineConfig::effective_max_memory`, from the `[engine]` class and pool settings)
//!   per leased activation ([`TenantQuotaView::instance_memory`]).

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ostro_meta::{MetaView, Tenant};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::P14Ctx;
use crate::config::ServerConfig;

/// One tenant's dispatch quotas (the `Tenant` record fields this node enforces).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Quotas {
    /// DRR quantum (`None` = 1).
    pub weight: Option<u32>,
    /// Leased activations (D-W13 unit).
    pub max_concurrency: Option<u32>,
    /// Instance memory reserved by leased activations, bytes.
    pub memory_bytes: Option<u64>,
    /// Fuel refill per second.
    pub fuel_per_s: Option<u64>,
    /// Fuel bucket size (`None` = one second of `fuel_per_s`).
    pub fuel_burst: Option<u64>,
    /// Ready entries are held, never failed.
    pub suspended: bool,
}

impl From<&Tenant> for Quotas {
    fn from(t: &Tenant) -> Self {
        Self {
            weight: t.weight,
            max_concurrency: t.max_concurrency,
            memory_bytes: t.memory_bytes,
            fuel_per_s: t.fuel_per_s,
            fuel_burst: t.fuel_burst,
            suspended: t.suspended,
        }
    }
}

impl Quotas {
    /// Whether a concurrency or memory limit applies (a release may unblock the tenant).
    pub fn capacity_limited(&self) -> bool {
        self.max_concurrency.is_some() || self.memory_bytes.is_some()
    }

    /// The fuel refill rate, when a fuel quota applies (`0` is treated as unset: a bucket
    /// that never refills would hold the tenant forever, and quotas only delay).
    pub fn fuel_rate(&self) -> Option<u64> {
        self.fuel_per_s.filter(|r| *r > 0)
    }
}

/// `share(x)` over `led` of `partitions` partitions (D-P14-15).
pub fn share_of(x: u64, led: u32, partitions: u32) -> u64 {
    if led == 0 {
        return 0;
    }
    let p = u128::from(partitions.max(1));
    let led = u128::from(led).min(p);
    let v = (u128::from(x) * led).div_ceil(p);
    u64::try_from(v).unwrap_or(u64::MAX).max(1)
}

/// One immutable generation.
#[derive(Debug, Default)]
struct Snapshot {
    tenants: HashMap<String, Arc<Quotas>>,
    led: u32,
}

/// Tenant quotas as this node applies them (module docs).
pub struct TenantQuotaView {
    snap: ArcSwap<Snapshot>,
    partitions: u32,
    instance_memory: u64,
    metrics_tenant_labels: bool,
    metrics_label_limit: u32,
    changed: watch::Sender<u64>,
}

impl std::fmt::Debug for TenantQuotaView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.snap.load();
        f.debug_struct("TenantQuotaView")
            .field("tenants", &s.tenants.len())
            .field("led", &s.led)
            .field("partitions", &self.partitions)
            .finish_non_exhaustive()
    }
}

impl Default for TenantQuotaView {
    fn default() -> Self {
        Self::new(&ServerConfig::default())
    }
}

impl TenantQuotaView {
    /// An empty view (every tenant unlimited) over `cfg`'s partition count, instance
    /// memory cap and `[tenants]` metric settings.
    pub fn new(cfg: &ServerConfig) -> Self {
        let partitions = u32::from(cfg.node.partitions.max(1));
        Self {
            snap: ArcSwap::from_pointee(Snapshot {
                tenants: HashMap::new(),
                led: partitions,
            }),
            partitions,
            instance_memory: cfg.engine.to_engine_config().effective_max_memory(),
            metrics_tenant_labels: cfg.tenants.metrics_tenant_labels,
            metrics_label_limit: cfg.tenants.metrics_label_limit,
            changed: watch::Sender::new(0),
        }
    }

    /// The quotas of `tenant` (`None` = no record: unlimited, weight 1).
    pub fn quotas(&self, tenant: &str) -> Option<Arc<Quotas>> {
        self.snap.load().tenants.get(tenant).cloned()
    }

    /// DRR quantum of `tenant` (`Tenant.weight`, default 1; the scheduler clamps it).
    pub fn weight(&self, tenant: &str) -> u32 {
        self.quotas(tenant).and_then(|q| q.weight).unwrap_or(1)
    }

    /// This node's share of a cluster-wide quota `x` (D-P14-15).
    pub fn share(&self, x: u64) -> u64 {
        share_of(x, self.snap.load().led, self.partitions)
    }

    /// Partitions this node leads (as last read from the roles).
    pub fn led(&self) -> u32 {
        self.snap.load().led
    }

    /// The cluster's partition count `P`.
    pub fn partitions(&self) -> u32 {
        self.partitions
    }

    /// Instance memory reserved per leased activation, bytes.
    pub fn instance_memory(&self) -> u64 {
        self.instance_memory
    }

    /// `[tenants] metrics_tenant_labels`.
    pub fn metrics_tenant_labels(&self) -> bool {
        self.metrics_tenant_labels
    }

    /// `[tenants] metrics_label_limit`.
    pub fn metrics_label_limit(&self) -> u32 {
        self.metrics_label_limit
    }

    /// Bumped on every change of the records or of `led`.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// Replace the records (`true` when any tenant's quotas changed).
    pub fn set_tenants(&self, tenants: HashMap<String, Quotas>) -> bool {
        let cur = self.snap.load();
        let same = cur.tenants.len() == tenants.len()
            && tenants
                .iter()
                .all(|(k, v)| cur.tenants.get(k).is_some_and(|c| **c == *v));
        if same {
            return false;
        }
        let tenants = tenants.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
        self.snap.store(Arc::new(Snapshot {
            tenants,
            led: cur.led,
        }));
        self.bump();
        true
    }

    /// Set the number of led partitions (`true` when it changed).
    pub fn set_led(&self, led: u32) -> bool {
        let cur = self.snap.load();
        if cur.led == led {
            return false;
        }
        self.snap.store(Arc::new(Snapshot {
            tenants: cur.tenants.clone(),
            led,
        }));
        self.bump();
        true
    }

    /// Re-read every `Tenant` record from `meta` (`true` when any changed).
    pub fn reload(&self, meta: &MetaView) -> bool {
        let tenants: HashMap<String, Quotas> = meta.read(|s| {
            s.tenants()
                .map(|v| (v.value.id.clone(), Quotas::from(&v.value)))
                .collect()
        });
        self.set_tenants(tenants)
    }
}

fn led_count(
    rx: &watch::Receiver<Arc<Vec<(ostro_types::PartitionId, ostro_types::Epoch)>>>,
) -> u32 {
    u32::try_from(rx.borrow().len()).unwrap_or(u32::MAX)
}

/// Watch meta `Tenant` records and the node's led partitions (module docs). Stops on
/// `stop`.
pub fn start(ctx: &P14Ctx, stop: CancellationToken) -> Option<JoinHandle<()>> {
    let view = Arc::clone(&ctx.quotas);
    let meta = ctx.meta.view().clone();
    view.reload(&meta);
    let mut led_rx = ctx.roles().map(|r| r.led());
    if let Some(rx) = &led_rx {
        view.set_led(led_count(rx));
    }
    let rt = tokio::runtime::Handle::try_current().ok()?;
    let mut sub = meta.subscribe();
    Some(rt.spawn(async move {
        loop {
            tokio::select! {
                _ = stop.cancelled() => break,
                r = sub.changed() => {
                    if r.is_err() {
                        break;
                    }
                    if view.reload(&meta) {
                        debug!("tenant quotas changed");
                    }
                }
                r = async {
                    match led_rx.as_mut() {
                        Some(rx) => rx.changed().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match (r, led_rx.as_ref()) {
                        (Ok(()), Some(rx)) => {
                            view.set_led(led_count(rx));
                        }
                        _ => led_rx = None,
                    }
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn share_follows_leadership() {
        // Single node: exact.
        assert_eq!(share_of(10, 4, 4), 10);
        // Half the partitions: ceil(x/2), at least 1.
        assert_eq!(share_of(10, 2, 4), 5);
        assert_eq!(share_of(3, 2, 4), 2);
        assert_eq!(share_of(1, 1, 8), 1);
        assert_eq!(share_of(0, 1, 8), 1, "max(1, ...)");
        // No leadership: nothing.
        assert_eq!(share_of(10, 0, 4), 0);
        // No overflow.
        assert_eq!(share_of(u64::MAX, 4, 4), u64::MAX);
        assert_eq!(share_of(u64::MAX, 9, 4), u64::MAX, "led clamps to P");
    }

    #[test]
    fn unknown_tenant_is_unlimited_weight_one_and_changes_bump() {
        let v = TenantQuotaView::default();
        let mut rx = v.subscribe();
        assert!(v.quotas("acme").is_none());
        assert_eq!(v.weight("acme"), 1);
        assert_eq!(v.led(), v.partitions(), "assumes it leads everything");
        assert_eq!(v.share(7), 7);
        let mut m = HashMap::new();
        m.insert(
            "acme".to_owned(),
            Quotas {
                weight: Some(4),
                max_concurrency: Some(3),
                ..Quotas::default()
            },
        );
        assert!(v.set_tenants(m.clone()));
        assert!(rx.has_changed().expect("open"));
        rx.borrow_and_update();
        assert!(!v.set_tenants(m), "same records: no bump");
        assert!(!rx.has_changed().expect("open"));
        assert_eq!(v.weight("acme"), 4);
        assert!(v.set_led(1));
        assert!(rx.has_changed().expect("open"));
        assert_eq!(v.share(8), 8u64.div_ceil(u64::from(v.partitions())));
        assert!(v.set_led(0));
        assert_eq!(v.share(8), 0);
    }

    #[tokio::test]
    async fn reload_reads_meta_tenant_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = ostro_meta::MetaHandle::start(dir.path(), 1)
            .await
            .expect("meta");
        let rec = ostro_meta::MetaRecord::Tenant(Tenant {
            id: "acme".into(),
            weight: Some(2),
            suspended: true,
            ..Tenant::default()
        });
        let r = meta
            .propose(ostro_meta::MetaCommand::Put {
                key: rec.key(),
                expected_version: None,
                record: rec,
            })
            .await
            .expect("propose");
        assert!(matches!(r, ostro_meta::MetaResult::Ok { .. }), "{r:?}");
        let v = TenantQuotaView::default();
        assert!(v.reload(meta.view()));
        let q = v.quotas("acme").expect("record");
        assert!(q.suspended);
        assert_eq!(q.weight, Some(2));
        assert!(!v.reload(meta.view()), "unchanged");
    }
}
