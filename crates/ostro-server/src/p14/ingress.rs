//! Phase 14 (14-20, DISP-06, D-P14-14/15): ingress admission. Suspended tenants and
//! per-tenant `event_rate` budgets, checked on the proposing node before anything is
//! written or proposed.
//!
//! # Where it runs
//!
//! [`admit`] is the `p14::admit_ingress` hook: `control/runs.rs::start_run` (after the dedupe
//! read, before the Phase 7 queue rate limit) and `control/lifecycle.rs::{signal_run,
//! complete_token}` (after the dry-run return). Those are the leader-side control plane
//! (routed requests reach the partition's leader first), and the tenant they pass is the
//! scoped one (the start's request tenant after `tenant_scope`, the run record's tenant for
//! signals and tokens), so a tenant cannot spend another tenant's budget. Webhook deliveries
//! (`__hook/` keys), schedule trigger-now (`__cron/`) and Kafka-triggered starts
//! (`__kafka/`, 14-32) go through the same start path and arrive with their
//! [`TriggerKind`]; they are charged exactly like an API start (the Kafka driver pauses its
//! partition on 429/403 and retries the record; it never skips). Schedule **fires** bypass
//! the API (the cron driver builds its `StartRun` in `control/schedules.rs`): they reach
//! [`cron_fire_gate`] instead, which refuses fires of a suspended tenant.
//!
//! # Rules, in order
//!
//! 1. No tenant record (the default tenant included): admitted, nothing charged.
//! 2. `suspended`: 403 `permission_denied`, `details.reason = "tenant_suspended"`, for every
//!    kind (dedupe hits included). Runs accepted before the suspension are held by the
//!    dispatcher (14-06), never failed.
//! 3. `Start { dedupe_hit: true }`: admitted without a charge (D-W21: a start whose
//!    idempotency key already names a run returns that run or 409; retries and resumes of
//!    existing runs never pass here).
//! 4. No `event_rate`: admitted.
//! 5. `event_rate`: the Phase 7 hand-rolled GCRA ([`crate::flow::limits::Gcra`],
//!    nanoseconds from the injected clock) over `share(limit)` events per `period_ms` with
//!    tolerance `burst` (D-P14-15: `share(x) = max(1, ceil(x × led / P))`). Non-conforming:
//!    429 `rate_limited` with `details.reason = "tenant_event_rate"` and
//!    `details.retry_after_ms` (`Retry-After` = ceil seconds, gRPC `RESOURCE_EXHAUSTED`),
//!    the Phase 7 encoding verbatim. Nothing is proposed.
//!
//! Every refusal counts `ostro_tenant_ingress_rejected_total{reason="suspended"|
//! "event_rate", kind="start"|"webhook"|"kafka"|"cron"|"signal"|"token"}` (no tenant label).
//!
//! # State
//!
//! One GCRA TAT per tenant, per server (keyed by the context's incarnation, dropped when
//! the Phase 14 services stop). The table is rebuilt lazily when the node's led partition
//! count changes (every tenant's TAT starts again at `now`); a changed spec keeps the TAT
//! and applies the new interval. Cells whose TAT is in the past carry no state and are
//! pruned when the table grows past [`PRUNE_AT`]. The race between the dedupe read and the
//! charge is the Phase 7 one (benign: two concurrent first starts of one key may both be
//! charged).

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use ostro_api::error::{RETRY_AFTER_MS, reason};
use ostro_api::{ApiError, ErrorCode};
use ostro_meta::{MetaView, RateSpec};

use super::{IngressKind, P14Ctx, TriggerKind};
use crate::flow::limits::{Gcra, ms_to_ns, ns_to_ms_ceil};

/// The refusal counter.
pub const REJECTED_METRIC: &str = "ostro_tenant_ingress_rejected_total";

/// Table size above which idle cells (TAT in the past) are pruned.
pub const PRUNE_AT: usize = 1024;

/// The `kind` label of an ingress event.
pub fn kind_label(kind: IngressKind) -> &'static str {
    match kind {
        IngressKind::Start { trigger, .. } => match trigger {
            None => "start",
            Some(TriggerKind::Webhook) => "webhook",
            Some(TriggerKind::Kafka) => "kafka",
            Some(TriggerKind::Cron) => "cron",
        },
        IngressKind::Signal => "signal",
        IngressKind::TokenComplete => "token",
    }
}

fn rejected(reason: &'static str, kind: &'static str) {
    metrics::counter!(REJECTED_METRIC, "reason" => reason, "kind" => kind).increment(1);
}

/// 403 `tenant_suspended`.
pub fn suspended_error(tenant: &str) -> ApiError {
    ApiError::permission_denied(
        reason::TENANT_SUSPENDED,
        format!("tenant {tenant:?} is suspended"),
    )
}

/// 429 `rate_limited`, `details.reason = "tenant_event_rate"`, `details.retry_after_ms`.
pub fn event_rate_error(tenant: &str, retry_after_ms: u64) -> ApiError {
    ApiError::new(
        ErrorCode::RateLimited,
        format!("tenant {tenant:?} event rate exceeded"),
    )
    .with_details(serde_json::json!({
        RETRY_AFTER_MS: retry_after_ms,
        "reason": reason::TENANT_EVENT_RATE,
    }))
}

// ---------------------------------------------------------------------------------
// GCRA table
// ---------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct Cell {
    gcra: Gcra,
    tat: u64,
}

#[derive(Debug, Default)]
struct Table {
    /// The led partition count the cells were charged under.
    led: Option<u32>,
    cells: HashMap<String, Cell>,
}

/// Per-tenant event-rate GCRA cells (module docs).
#[derive(Debug, Default)]
pub struct EventRates {
    table: Mutex<Table>,
}

impl EventRates {
    /// Charge one event of `tenant` at `now_ms` against `spec` scaled to `share_limit`
    /// events per period, while the node leads `led` partitions. `Err(retry_after_ms)`
    /// (at least 1) when it does not conform; nothing is charged then.
    pub fn charge(
        &self,
        tenant: &str,
        spec: &RateSpec,
        share_limit: u64,
        led: u32,
        now_ms: u64,
    ) -> Result<(), u64> {
        let scaled = RateSpec {
            limit: u32::try_from(share_limit.max(1)).unwrap_or(u32::MAX),
            period_ms: spec.period_ms,
            burst: spec.burst.max(1),
        };
        let Some(gcra) = Gcra::from_spec(&scaled) else {
            return Ok(());
        };
        let now_ns = ms_to_ns(now_ms);
        let mut t = self.table.lock().unwrap_or_else(PoisonError::into_inner);
        if t.led != Some(led) {
            // Leadership moved: rebuild lazily, every TAT starts again at now.
            t.cells.clear();
            t.led = Some(led);
        }
        if t.cells.len() >= PRUNE_AT && !t.cells.contains_key(tenant) {
            t.cells.retain(|_, c| c.tat > now_ns);
        }
        let cell = t
            .cells
            .entry(tenant.to_owned())
            .or_insert(Cell { gcra, tat: now_ns });
        cell.gcra = gcra;
        match gcra.conforms(cell.tat, now_ns) {
            Ok(()) => {
                cell.tat = gcra.advance(cell.tat, now_ns);
                Ok(())
            }
            Err(at) => Err(ns_to_ms_ceil(at.saturating_sub(now_ns)).max(1)),
        }
    }

    /// Tracked cells.
    pub fn len(&self) -> usize {
        self.table
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .cells
            .len()
    }

    /// Whether no cell is tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The tables of the live servers of this process, by context incarnation.
static TABLES: LazyLock<Mutex<HashMap<u64, Arc<EventRates>>>> = LazyLock::new(Default::default);

/// The event-rate table of `ctx` (created on first use; removed when its Phase 14 services
/// stop, [`P14Ctx::shutdown`]).
pub fn rates_of(ctx: &P14Ctx) -> Arc<EventRates> {
    let mut m = TABLES.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(r) = m.get(&ctx.incarnation) {
        return Arc::clone(r);
    }
    let r = Arc::new(EventRates::default());
    m.insert(ctx.incarnation, Arc::clone(&r));
    drop(m);
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        let stop = ctx.shutdown.clone();
        let key = ctx.incarnation;
        rt.spawn(async move {
            stop.cancelled().await;
            TABLES
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&key);
        });
    }
    r
}

// ---------------------------------------------------------------------------------
// Hooks
// ---------------------------------------------------------------------------------

/// What admission needs from a tenant record: `(suspended, event_rate)`.
fn record(meta: &MetaView, tenant: &str) -> Option<(bool, Option<RateSpec>)> {
    meta.read(|s| {
        s.tenant(tenant)
            .map(|v| (v.value.suspended, v.value.event_rate.clone()))
    })
}

/// `p14::admit_ingress` (module docs).
pub async fn admit(ctx: &P14Ctx, kind: IngressKind, tenant: Option<&str>) -> Result<(), ApiError> {
    let t = tenant.unwrap_or_default();
    let Some((suspended, rate)) = record(ctx.meta.view(), t) else {
        return Ok(());
    };
    if suspended {
        rejected("suspended", kind_label(kind));
        return Err(suspended_error(t));
    }
    if let IngressKind::Start {
        dedupe_hit: true, ..
    } = kind
    {
        return Ok(());
    }
    let Some(spec) = rate else {
        return Ok(());
    };
    let q = &ctx.quotas;
    let share = q.share(u64::from(spec.limit.max(1)));
    rates_of(ctx)
        .charge(t, &spec, share, q.led(), ctx.clock.now_ms())
        .map_err(|retry_after_ms| {
            rejected("event_rate", kind_label(kind));
            event_rate_error(t, retry_after_ms)
        })
}

/// The suspension check of a schedule fire (`control/schedules.rs::build_schedule_start`,
/// before anything is written): a fire of a suspended tenant is refused (counted with
/// `kind="cron"`); the cron driver drops that fire (its bounded `Skip` path) and fires
/// again on the schedule's next due time after the resume. Fires are not charged against
/// `event_rate` (D-P14-14 lists start, signal, token completion, webhook and Kafka
/// deliveries).
pub fn cron_fire_gate(meta: &MetaView, tenant: Option<&str>) -> Result<(), ApiError> {
    let t = tenant.unwrap_or_default();
    match record(meta, t) {
        Some((true, _)) => {
            rejected("suspended", "cron");
            Err(suspended_error(t))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ostro_meta::{MetaCommand, MetaHandle, MetaRecord, MetaResult, Tenant};
    use ostro_partition::{Clock, ManualClock};

    use super::*;
    use crate::config::ServerConfig;
    use crate::p14::{P14Init, build_ctx};

    fn spec(limit: u32, period_ms: u64, burst: u32) -> RateSpec {
        RateSpec {
            limit,
            period_ms,
            burst,
        }
    }

    #[test]
    fn burst_then_steady_rate() {
        let r = EventRates::default();
        let s = spec(10, 1_000, 10);
        // A burst of 10 at t=0, the 11th waits one interval (100 ms).
        for i in 0..10 {
            assert_eq!(r.charge("a", &s, 10, 4, 0), Ok(()), "burst {i}");
        }
        assert_eq!(r.charge("a", &s, 10, 4, 0), Err(100));
        // Steady: one every 100 ms.
        for k in 1..=20u64 {
            assert_eq!(r.charge("a", &s, 10, 4, k * 100), Ok(()), "steady {k}");
            assert!(r.charge("a", &s, 10, 4, k * 100).is_err(), "only one per T");
        }
    }

    #[test]
    fn retry_after_is_the_earliest_conforming_instant() {
        let r = EventRates::default();
        let s = spec(2, 1_000, 1);
        assert_eq!(r.charge("a", &s, 2, 1, 1_000), Ok(()));
        // T = 500 ms, burst 1: the next may go at 1500.
        assert_eq!(r.charge("a", &s, 2, 1, 1_001), Err(499));
        assert_eq!(r.charge("a", &s, 2, 1, 1_499), Err(1));
        assert_eq!(r.charge("a", &s, 2, 1, 1_500), Ok(()));
        // A refusal charges nothing: waiting exactly retry_after succeeds.
        let Err(wait) = r.charge("a", &s, 2, 1, 1_700) else {
            panic!("must refuse");
        };
        assert_eq!(wait, 300);
        assert_eq!(r.charge("a", &s, 2, 1, 1_700 + wait), Ok(()));
        // Sub-millisecond intervals round up to 1 ms.
        let fast = spec(5_000, 1_000, 1);
        assert_eq!(r.charge("f", &fast, 5_000, 1, 0), Ok(()));
        assert_eq!(r.charge("f", &fast, 5_000, 1, 0), Err(1));
    }

    #[test]
    fn tenants_are_independent() {
        let r = EventRates::default();
        let s = spec(1, 1_000, 1);
        assert_eq!(r.charge("a", &s, 1, 1, 0), Ok(()));
        assert!(r.charge("a", &s, 1, 1, 0).is_err());
        assert_eq!(r.charge("b", &s, 1, 1, 0), Ok(()));
    }

    #[test]
    fn leadership_change_rebuilds_and_share_scales() {
        let r = EventRates::default();
        let s = spec(10, 1_000, 1);
        // Leading every partition: 10/s, T = 100 ms.
        assert_eq!(r.charge("a", &s, 10, 4, 0), Ok(()));
        assert_eq!(r.charge("a", &s, 10, 4, 1), Err(99));
        // Half the partitions: share 5/s, T = 200 ms; the table is rebuilt (TAT = now).
        assert_eq!(r.charge("a", &s, 5, 2, 1), Ok(()));
        assert_eq!(r.charge("a", &s, 5, 2, 2), Err(199));
        // Over one second at half share: 5 events; at the full share: 10.
        let r = EventRates::default();
        let ok = (0..1_000u64)
            .filter(|ms| r.charge("a", &s, 5, 2, *ms).is_ok())
            .count();
        assert_eq!(ok, 5);
        let r = EventRates::default();
        let ok = (0..1_000u64)
            .filter(|ms| r.charge("a", &s, 10, 4, *ms).is_ok())
            .count();
        assert_eq!(ok, 10);
    }

    #[test]
    fn idle_cells_are_pruned() {
        let r = EventRates::default();
        let s = spec(1, 1_000, 1);
        for i in 0..PRUNE_AT {
            r.charge(&format!("t{i}"), &s, 1, 1, 0).expect("first");
        }
        assert_eq!(r.len(), PRUNE_AT);
        // Every TAT is 1000 ms: at 5000 they are idle and pruned on the next new tenant.
        r.charge("new", &s, 1, 1, 5_000).expect("new");
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn labels_and_errors() {
        let st = |trigger| IngressKind::Start {
            dedupe_hit: false,
            trigger,
        };
        assert_eq!(kind_label(st(None)), "start");
        assert_eq!(kind_label(st(Some(TriggerKind::Webhook))), "webhook");
        assert_eq!(kind_label(st(Some(TriggerKind::Kafka))), "kafka");
        assert_eq!(kind_label(st(Some(TriggerKind::Cron))), "cron");
        assert_eq!(kind_label(IngressKind::Signal), "signal");
        assert_eq!(kind_label(IngressKind::TokenComplete), "token");
        let e = event_rate_error("a", 1_500);
        assert_eq!(e.code, ErrorCode::RateLimited);
        assert_eq!(e.retry_after_ms(), Some(1_500));
        assert_eq!(e.reason(), Some(reason::TENANT_EVENT_RATE));
        let e = suspended_error("a");
        assert_eq!(e.code, ErrorCode::PermissionDenied);
        assert_eq!(e.reason(), Some(reason::TENANT_SUSPENDED));
    }

    async fn put_tenant(meta: &MetaHandle, t: Tenant) {
        let record = MetaRecord::Tenant(t);
        let key = record.key();
        let expected_version = meta.view().read(|s| s.get(&key).map(|v| v.version));
        let r = meta
            .propose(MetaCommand::Put {
                key,
                expected_version,
                record,
            })
            .await
            .expect("propose");
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }

    struct Fx {
        _dir: tempfile::TempDir,
        ctx: Arc<P14Ctx>,
        clock: ManualClock,
    }

    async fn fx() -> Fx {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = Arc::new(MetaHandle::start(dir.path(), 1).await.expect("meta"));
        let clock = ManualClock::new(10_000);
        let dyn_clock: Arc<dyn Clock> = Arc::new(clock.clone());
        let ctx = build_ctx(P14Init {
            cfg: Arc::new(ServerConfig::default()),
            meta,
            clock: dyn_clock,
            node_id: 1,
            data_dir: dir.path().to_path_buf(),
            level_max: ostro_types::compat::level_max(),
        })
        .expect("build_ctx");
        Fx {
            _dir: dir,
            ctx,
            clock,
        }
    }

    fn start(dedupe_hit: bool, trigger: Option<TriggerKind>) -> IngressKind {
        IngressKind::Start {
            dedupe_hit,
            trigger,
        }
    }

    #[tokio::test]
    async fn admit_charges_on_the_injected_clock_and_skips_dedupe_hits() {
        let f = fx().await;
        put_tenant(
            &f.ctx.meta,
            Tenant {
                id: "a".into(),
                created_at_ms: 1,
                event_rate: Some(spec(2, 1_000, 2)),
                ..Tenant::default()
            },
        )
        .await;
        // Unknown tenants and the default tenant are unlimited.
        for _ in 0..10 {
            admit(&f.ctx, start(false, None), Some("zz"))
                .await
                .expect("no record");
            admit(&f.ctx, IngressKind::Signal, None)
                .await
                .expect("default tenant");
        }
        // Start, signal and token share one budget of 2 (burst 2).
        admit(&f.ctx, start(false, None), Some("a"))
            .await
            .expect("1");
        admit(&f.ctx, IngressKind::Signal, Some("a"))
            .await
            .expect("2");
        let e = admit(&f.ctx, IngressKind::TokenComplete, Some("a"))
            .await
            .expect_err("over budget");
        assert_eq!(e.code, ErrorCode::RateLimited);
        assert_eq!(e.reason(), Some(reason::TENANT_EVENT_RATE));
        assert_eq!(e.retry_after_ms(), Some(500));
        // Webhook and Kafka deliveries are charged like any start.
        for trig in [TriggerKind::Webhook, TriggerKind::Kafka] {
            let e = admit(&f.ctx, start(false, Some(trig)), Some("a"))
                .await
                .expect_err("charged");
            assert_eq!(e.reason(), Some(reason::TENANT_EVENT_RATE));
        }
        // A dedupe hit is never charged, whatever the trigger.
        for trig in [None, Some(TriggerKind::Kafka), Some(TriggerKind::Webhook)] {
            admit(&f.ctx, start(true, trig), Some("a"))
                .await
                .expect("dedupe hit is free");
        }
        // The injected clock refills.
        f.clock.advance(500);
        admit(&f.ctx, start(false, Some(TriggerKind::Kafka)), Some("a"))
            .await
            .expect("refilled");
        assert!(admit(&f.ctx, start(false, None), Some("a")).await.is_err());
    }

    #[tokio::test]
    async fn admit_follows_the_leadership_share() {
        let f = fx().await;
        put_tenant(
            &f.ctx.meta,
            Tenant {
                id: "a".into(),
                created_at_ms: 1,
                event_rate: Some(spec(10, 1_000, 1)),
                ..Tenant::default()
            },
        )
        .await;
        let p = f.ctx.quotas.partitions();
        assert!(p >= 2 && p % 2 == 0, "default partition count {p}");
        async fn count(f: &Fx) -> usize {
            let mut ok = 0;
            for _ in 0..1_000 {
                if admit(&f.ctx, start(false, None), Some("a")).await.is_ok() {
                    ok += 1;
                }
                f.clock.advance(1);
            }
            ok
        }
        assert_eq!(count(&f).await, 10, "every partition led: the full rate");
        f.ctx.quotas.set_led(p / 2);
        assert_eq!(count(&f).await, 5, "half the partitions: half the rate");
    }

    #[tokio::test]
    async fn suspended_takes_precedence_and_cron_fires_are_refused() {
        let f = fx().await;
        put_tenant(
            &f.ctx.meta,
            Tenant {
                id: "a".into(),
                created_at_ms: 1,
                event_rate: Some(spec(1, 1_000, 1)),
                suspended: true,
                ..Tenant::default()
            },
        )
        .await;
        for kind in [
            start(false, None),
            start(true, None),
            start(false, Some(TriggerKind::Kafka)),
            start(false, Some(TriggerKind::Webhook)),
            start(false, Some(TriggerKind::Cron)),
            IngressKind::Signal,
            IngressKind::TokenComplete,
        ] {
            let e = admit(&f.ctx, kind, Some("a")).await.expect_err("suspended");
            assert_eq!(e.code, ErrorCode::PermissionDenied, "{kind:?}");
            assert_eq!(e.reason(), Some(reason::TENANT_SUSPENDED), "{kind:?}");
        }
        assert!(
            rates_of(&f.ctx).is_empty(),
            "a suspended tenant is not charged"
        );
        let e = cron_fire_gate(f.ctx.meta.view(), Some("a")).expect_err("fire refused");
        assert_eq!(e.reason(), Some(reason::TENANT_SUSPENDED));
        cron_fire_gate(f.ctx.meta.view(), Some("b")).expect("no record");
        cron_fire_gate(f.ctx.meta.view(), None).expect("default tenant");
        // Resume: admitted again, now charged.
        put_tenant(
            &f.ctx.meta,
            Tenant {
                id: "a".into(),
                created_at_ms: 1,
                event_rate: Some(spec(1, 1_000, 1)),
                suspended: false,
                ..Tenant::default()
            },
        )
        .await;
        cron_fire_gate(f.ctx.meta.view(), Some("a")).expect("resumed");
        admit(&f.ctx, start(false, None), Some("a"))
            .await
            .expect("resumed");
        assert_eq!(rates_of(&f.ctx).len(), 1);
    }

    #[tokio::test]
    async fn the_table_is_dropped_when_the_services_stop() {
        let f = fx().await;
        let a = rates_of(&f.ctx);
        assert!(Arc::ptr_eq(&a, &rates_of(&f.ctx)));
        f.ctx.shutdown.cancel();
        for _ in 0..100 {
            if !TABLES
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .contains_key(&f.ctx.incarnation)
            {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("the table outlived the services");
    }
}
