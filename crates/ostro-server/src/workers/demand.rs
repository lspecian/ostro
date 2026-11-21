//! Worker demand (plan 07-16, D-W12, WORK-05).
//!
//! `demand(q) = running_on_workers(q) + admissible_backlog_for_workers(q)`, published every
//! 1 s as `ostro_queue_worker_demand{queue}` (Phase 7 owns the value; 08-16 only describes
//! it in the dispatcher) and served by `GET /v1/queues/{q}/demand` (07-14 maps
//! [`snapshot`]).
//!
//! # Formula as implemented
//!
//! Inputs, read once per call:
//! - [`QueueGauges`](ostro_partition::QueueGauges) summed over every partition (07-05):
//!   `ready_unblocked` (ready entries flow control would admit now, including the ones
//!   placement holds back for lack of capacity, excluding paused queues, held paused-workflow
//!   entries and parked keys), `running_embedded`, `running_on_workers`;
//! - the queue's `QueuePolicy` (placement over `[workers] default_placement`, selector,
//!   `concurrency`, `paused`);
//! - [`SpillHandle::spilling`](super::spill::SpillHandle::spilling) (07-13);
//! - the worker registry (07-09): sessions whose labels match the queue selector;
//! - the free embedded slots (`[executor] slots` semaphore).
//!
//! ```text
//! headroom   = 0                                         if the queue is paused
//!            = concurrency - (running_embedded + running_on_workers)   if a limit is set
//!            = unbounded                                 otherwise
//! workers:   backlog = ready_unblocked
//!            admissible = min(backlog, headroom)
//! auto:      spilling:  backlog = ready_unblocked
//!                       admissible = max(0, min(backlog, headroom) - embedded_free)
//!            otherwise: backlog = 0, admissible = 0
//! embedded:  backlog = 0, admissible = 0, demand = 0
//! demand     = running_on_workers + admissible           (workers / auto)
//! ```
//!
//! On an `auto` queue embedded slots are filled first (W6), so the free embedded slots are
//! subtracted from the admissible backlog. A workers queue with no connected worker reports
//! its admissible backlog (scale from zero); runs executing on workers keep the demand
//! above zero (no scale-in under load).
//!
//! # Series
//!
//! [`start`] sets, every second on the injected clock (F5):
//! - `ostro_queue_worker_demand{queue}` and `ostro_queue_worker_backlog{queue}` for every
//!   queue with dispatcher gauges (a queue that loses its gauges gets one last zero row);
//! - `ostro_workers_connected` and `ostro_worker_slots_free` (node totals; the registry
//!   also sets them on every change).
//!
//! Labels are queue names only (never run ids). No other module sets the demand value.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::Duration;

use ostro_partition::{Clock, QueueGauge};
use serde::Serialize;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::PlacementSetting;
use crate::p7::{P7Ctx, P7Shared};
use crate::workers::selector::QueuePlacement;

/// How often [`start`] publishes the series (D-W12).
pub const TICK: Duration = Duration::from_secs(1);

/// `GET /v1/queues/{q}/demand` body.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DemandSnapshot {
    pub queue: String,
    pub demand: u64,
    pub backlog: u64,
    pub running_on_workers: u64,
    /// `embedded` | `workers` | `auto`.
    pub placement: String,
    pub spilling: bool,
    pub workers: u64,
    pub worker_slots_free: u64,
}

/// Everything [`compute`] needs about one queue.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DemandInputs {
    pub placement: PlacementSetting,
    pub spilling: bool,
    /// Per-queue gauge summed over the partitions.
    pub gauge: QueueGauge,
    /// `QueuePolicy.concurrency` (node-wide leased activations; `None` = unbounded).
    pub concurrency: Option<u32>,
    /// `QueuePolicy.paused`.
    pub paused: bool,
    /// Free embedded executor slots.
    pub embedded_free: u64,
    /// Connected workers matching the queue selector.
    pub workers: u64,
    /// Their free credit.
    pub worker_slots_free: u64,
}

/// The placement's wire name.
pub fn placement_name(p: PlacementSetting) -> &'static str {
    match p {
        PlacementSetting::Embedded => "embedded",
        PlacementSetting::Workers => "workers",
        PlacementSetting::Auto => "auto",
    }
}

/// The D-W12 formula (pure; see the module docs).
pub fn compute(queue: &str, i: &DemandInputs) -> DemandSnapshot {
    let g = &i.gauge;
    let headroom = if i.paused {
        0
    } else {
        match i.concurrency {
            Some(c) => u64::from(c).saturating_sub(g.running_embedded + g.running_on_workers),
            None => u64::MAX,
        }
    };
    let (backlog, admissible, running) = match i.placement {
        PlacementSetting::Embedded => (0, 0, 0),
        PlacementSetting::Workers => {
            let backlog = g.ready_unblocked;
            (backlog, backlog.min(headroom), g.running_on_workers)
        }
        PlacementSetting::Auto if i.spilling => {
            let backlog = g.ready_unblocked;
            let admissible = backlog.min(headroom).saturating_sub(i.embedded_free);
            (backlog, admissible, g.running_on_workers)
        }
        // Not spilling: embedded only, but runs still on workers (spill just ended) count.
        PlacementSetting::Auto => (0, 0, g.running_on_workers),
    };
    DemandSnapshot {
        queue: queue.to_owned(),
        demand: running.saturating_add(admissible),
        backlog,
        running_on_workers: g.running_on_workers,
        placement: placement_name(i.placement).to_owned(),
        spilling: i.spilling,
        workers: i.workers,
        worker_slots_free: i.worker_slots_free,
    }
}

// ---------------------------------------------------------------------------------
// Default placement per server
// ---------------------------------------------------------------------------------

/// `[workers] default_placement` per started server. [`snapshot`]'s fixed signature (07-02)
/// only gets the [`P7Shared`], which carries no config, so [`start`] records the default
/// here, keyed by the shared state's address (a live `Weak` guards against address reuse).
static DEFAULTS: Mutex<Vec<(Weak<P7Shared>, PlacementSetting)>> = Mutex::new(Vec::new());

fn set_default(shared: &Arc<P7Shared>, p: PlacementSetting) {
    let mut d = DEFAULTS.lock().unwrap_or_else(PoisonError::into_inner);
    d.retain(|(w, _)| w.strong_count() > 0 && !std::ptr::eq(w.as_ptr(), Arc::as_ptr(shared)));
    d.push((Arc::downgrade(shared), p));
}

fn default_of(shared: &P7Shared) -> PlacementSetting {
    let d = DEFAULTS.lock().unwrap_or_else(PoisonError::into_inner);
    d.iter()
        .find(|(w, _)| w.strong_count() > 0 && std::ptr::eq(w.as_ptr(), shared))
        .map_or(PlacementSetting::Embedded, |(_, p)| *p)
}

// ---------------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------------

/// The registry-wide view one tick (or one snapshot) reads once.
struct Workers {
    sessions: Vec<crate::workers::registry::WorkerInfo>,
}

impl Workers {
    fn read(shared: &P7Shared) -> Self {
        Self {
            sessions: shared.registry.sessions(),
        }
    }

    /// (distinct matching worker ids, their free credit).
    fn matching(&self, qp: &QueuePlacement) -> (u64, u64) {
        let mut ids = BTreeSet::new();
        let mut free = 0u64;
        for s in &self.sessions {
            if qp.matches(&s.labels) {
                ids.insert(s.worker_id.as_str());
                free += u64::from(s.free);
            }
        }
        (ids.len() as u64, free)
    }
}

fn inputs(
    shared: &P7Shared,
    queue: &str,
    gauge: QueueGauge,
    default: PlacementSetting,
    workers: &Workers,
) -> DemandInputs {
    let (qp, concurrency, paused) = shared.flow.meta().read(|s| {
        let p = s.queue_policy(queue).map(|v| &v.value);
        (
            QueuePlacement::from_policy(p, default),
            p.and_then(|p| p.concurrency),
            p.is_some_and(|p| p.paused),
        )
    });
    let (n, free) = workers.matching(&qp);
    DemandInputs {
        placement: qp.placement,
        spilling: shared.spill.spilling(queue),
        gauge,
        concurrency,
        paused,
        embedded_free: shared.embedded_slots.available_permits() as u64,
        workers: n,
        worker_slots_free: free,
    }
}

/// The current demand of `queue`.
pub fn snapshot(shared: &P7Shared, queue: &str) -> DemandSnapshot {
    let gauge = shared.gauges.snapshot().remove(queue).unwrap_or_default();
    let workers = Workers::read(shared);
    let i = inputs(shared, queue, gauge, default_of(shared), &workers);
    compute(queue, &i)
}

/// Every queue's demand from one gauge snapshot (queues with dispatcher gauges plus
/// `extra`).
pub fn snapshot_all(
    shared: &P7Shared,
    extra: impl IntoIterator<Item = String>,
) -> BTreeMap<String, DemandSnapshot> {
    let mut gauges = shared.gauges.snapshot();
    for q in extra {
        gauges.entry(q).or_default();
    }
    let workers = Workers::read(shared);
    let default = default_of(shared);
    gauges
        .into_iter()
        .map(|(q, g)| {
            let d = compute(&q, &inputs(shared, &q, g, default, &workers));
            (q, d)
        })
        .collect()
}

// ---------------------------------------------------------------------------------
// Gauge loop
// ---------------------------------------------------------------------------------

/// Publish one tick; `published` holds the queues that have rows (a queue that vanished
/// gets one zero row and is forgotten).
pub fn publish(shared: &P7Shared, published: &mut BTreeSet<String>) {
    let all = snapshot_all(shared, std::iter::empty());
    for q in published.iter() {
        if !all.contains_key(q) {
            metrics::gauge!("ostro_queue_worker_demand", "queue" => q.clone()).set(0.0);
            metrics::gauge!("ostro_queue_worker_backlog", "queue" => q.clone()).set(0.0);
        }
    }
    published.clear();
    for (q, d) in all {
        metrics::gauge!("ostro_queue_worker_demand", "queue" => q.clone()).set(d.demand as f64);
        metrics::gauge!("ostro_queue_worker_backlog", "queue" => q.clone()).set(d.backlog as f64);
        published.insert(q);
    }
    metrics::gauge!("ostro_workers_connected").set(shared.registry.connected() as f64);
    metrics::gauge!("ostro_worker_slots_free").set(shared.registry.slots_free() as f64);
}

fn describe() {
    metrics::describe_gauge!(
        "ostro_queue_worker_backlog",
        "Ready runs of the queue admissible for workers now (D-W12 backlog)"
    );
    metrics::describe_gauge!("ostro_workers_connected", "Connected worker sessions");
    metrics::describe_gauge!("ostro_worker_slots_free", "Assignable worker credit");
}

/// Run [`publish`] every `period` (deadline from `clock`, tokio only sleeps) until
/// `shutdown`.
pub async fn run(
    shared: Arc<P7Shared>,
    clock: Arc<dyn Clock>,
    shutdown: CancellationToken,
    period: Duration,
) {
    let period_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX).max(1);
    let mut published = BTreeSet::new();
    let mut next_ms = clock.now_ms();
    loop {
        publish(&shared, &mut published);
        next_ms = next_ms.saturating_add(period_ms);
        let now = clock.now_ms();
        if next_ms < now {
            // Fell behind (or the clock jumped): skip the missed ticks.
            next_ms = now;
        }
        // A manual clock that does not move still ticks once per period of real time.
        let wait = Duration::from_millis((next_ms - now).clamp(1, period_ms));
        tokio::select! {
            biased;
            () = shutdown.cancelled() => break,
            () = tokio::time::sleep(wait) => {}
        }
    }
}

/// Start the demand ticker (every [`TICK`]). Always runs: an embedded-only node publishes
/// zeros, which is what an autoscaler should read.
pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>> {
    set_default(&ctx.shared, ctx.config.workers.default_placement);
    describe();
    Some(tokio::spawn(run(
        Arc::clone(&ctx.shared),
        Arc::clone(&ctx.clock),
        ctx.shutdown.clone(),
        TICK,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn g(ready_unblocked: u64, embedded: u64, workers: u64) -> QueueGauge {
        QueueGauge {
            ready: ready_unblocked,
            ready_unblocked,
            oldest_ready_at_ms: None,
            running_embedded: embedded,
            running_on_workers: workers,
        }
    }

    fn workers_q(gauge: QueueGauge) -> DemandInputs {
        DemandInputs {
            placement: PlacementSetting::Workers,
            gauge,
            ..DemandInputs::default()
        }
    }

    #[test]
    fn workers_queue_scales_from_zero() {
        let d = compute("q", &workers_q(g(10, 0, 0)));
        assert_eq!(
            (d.demand, d.backlog, d.placement.as_str()),
            (10, 10, "workers")
        );
    }

    #[test]
    fn concurrency_caps_the_backlog() {
        let mut i = workers_q(g(10, 0, 0));
        i.concurrency = Some(4);
        assert_eq!(compute("q", &i).demand, 4);
        // 2 running on a worker, 8 ready: 2 + min(8, 4 - 2).
        i.gauge = g(8, 0, 2);
        assert_eq!(compute("q", &i).demand, 4);
        // Over the limit (policy lowered): running only.
        i.gauge = g(8, 1, 5);
        assert_eq!(compute("q", &i).demand, 5);
    }

    #[test]
    fn paused_queue_reports_running_only() {
        let mut i = workers_q(g(8, 0, 3));
        i.paused = true;
        let d = compute("q", &i);
        assert_eq!((d.demand, d.backlog), (3, 8));
    }

    #[test]
    fn auto_counts_backlog_only_while_spilling() {
        let mut i = DemandInputs {
            placement: PlacementSetting::Auto,
            gauge: g(10, 2, 1),
            embedded_free: 3,
            ..DemandInputs::default()
        };
        let d = compute("q", &i);
        assert_eq!((d.demand, d.backlog), (1, 0));
        i.spilling = true;
        let d = compute("q", &i);
        // 1 running + (min(10, inf) - 3 embedded free).
        assert_eq!((d.demand, d.backlog), (8, 10));
        i.embedded_free = 20;
        assert_eq!(compute("q", &i).demand, 1);
    }

    #[test]
    fn embedded_is_zero() {
        let i = DemandInputs {
            placement: PlacementSetting::Embedded,
            gauge: g(10, 4, 2),
            spilling: true,
            ..DemandInputs::default()
        };
        let d = compute("q", &i);
        assert_eq!((d.demand, d.backlog, d.running_on_workers), (0, 0, 2));
    }
}
