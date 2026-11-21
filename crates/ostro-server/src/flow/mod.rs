//! Node flow control (Phase 7, DISP-03/04/05; implemented in 07-08).
//!
//! | File | Owner | Purpose |
//! |---|---|---|
//! | [`policy_view`] | 07-08 | `QueuePolicy` / `WorkflowPolicy` limits read from meta |
//! | [`limits`] | 07-08 | node-wide concurrency counters + hand-rolled GCRA (D-W24) |
//! | [`rate_limit`] | 07-08 | StartRun admission (HTTP 429 `rate_limited`, D-W21) |
//!
//! [`NodeFlow`] is the node's `FlowControl` (one per node, shared by every partition's
//! dispatcher, installed by [`install`]):
//!
//! - `admit(entry, now_ms)`: workflow or queue paused → `Paused`; queue `concurrency`
//!   reached → `QueueBlocked{None}`; key `key_concurrency` reached (key = the entry's
//!   `concurrency_key`; empty = unkeyed, never key-limited) → `KeyBlocked{None}`; for a
//!   first activation only, queue `throttle` / key `key_throttle` not conforming →
//!   `QueueBlocked` / `KeyBlocked` with `until_ms` = the earliest conforming instant
//!   (throttles queue the excess, they never reject); else `Yes(permit)`.
//! - The permit charges the node-wide counters tentatively ([`limits`]); the dispatcher
//!   commits it after `Placement::reserve` granted capacity, and an uncommitted drop rolls
//!   the charge back. Dropping a committed permit (lease end) releases the counters.
//! - Concurrency counts **leased activations** (D-W13): suspended or sleeping runs hold no
//!   lease and count nothing. The counters are exact on one node and per node in a cluster
//!   (cluster-wide quotas are Phase 13).
//! - `queue_weight(q)` = `QueuePolicy.weight` or `[flow] default_queue_weight`, at least 1.
//! - `changed()` is bumped when a limited permit is released or rolled back and when a
//!   meta apply changes a cached policy (pause/resume of a queue or workflow, a limit
//!   edit), so dispatchers re-evaluate blocked and paused entries without polling.
//! - Auto-pause (OPS-01): [`NodeFlow`] is the executor's
//!   [`ExhaustPolicy`](crate::executor::ExhaustPolicy): `on_retries_exhausted` of the
//!   workflow policy wins over the queue's; `1` (pause) pauses, `0`/unset fails.
//!
//! Metrics: `ostro_flow_admit_total{queue,result}` (`result` = `admitted`,
//! `queue_blocked`, `key_blocked`, `paused`) and `ostro_flow_throttled_total{queue}` (a
//! throttle held back a first activation).

pub mod limits;
pub mod policy_view;
pub mod rate_limit;

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use ostro_meta::MetaView;
use ostro_partition::{
    Admit, Clock, DispatcherConfig, EntryView, FlowControl, FlowPermit, QueueGauges,
};
use tokio::sync::watch;
use tracing::warn;

use crate::config::FlowSection;
use crate::p7::P7Ctx;
use limits::{AdmitReq, Block, Charge, Counters, Gcra, KeyBounds, RateCells, ns_to_ms_ceil};
use policy_view::PolicyView;

/// State shared by the flow and its permits.
struct Inner {
    counters: Mutex<Counters>,
    rates: Mutex<RateCells>,
    changed: watch::Sender<u64>,
    clock: OnceLock<Arc<dyn Clock>>,
    /// Latest `now_ms` seen by an admission (rollback time when no clock is installed).
    last_now_ms: AtomicU64,
}

impl Inner {
    fn counters(&self) -> MutexGuard<'_, Counters> {
        self.counters.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn now_ms(&self) -> u64 {
        match self.clock.get() {
            Some(c) => c.now_ms(),
            None => self.last_now_ms.load(Ordering::Acquire),
        }
    }

    fn bump(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}

/// The node's flow control.
pub struct NodeFlow {
    meta: MetaView,
    gauges: Arc<QueueGauges>,
    policies: PolicyView,
    inner: Arc<Inner>,
    default_weight: AtomicU32,
    watcher: AtomicBool,
}

impl NodeFlow {
    /// Flow control over the meta policies, publishing into `gauges` (`[flow]` defaults
    /// until [`configure`](Self::configure)).
    pub fn new(meta: MetaView, gauges: Arc<QueueGauges>) -> Self {
        Self::with_config(meta, gauges, &FlowSection::default())
    }

    /// [`new`](Self::new) with an explicit `[flow]` section.
    pub fn with_config(meta: MetaView, gauges: Arc<QueueGauges>, cfg: &FlowSection) -> Self {
        let bounds = bounds(cfg);
        Self {
            policies: PolicyView::new(meta.clone()),
            meta,
            gauges,
            inner: Arc::new(Inner {
                counters: Mutex::new(Counters::new(bounds)),
                rates: Mutex::new(RateCells::new(bounds)),
                changed: watch::Sender::new(0),
                clock: OnceLock::new(),
                last_now_ms: AtomicU64::new(0),
            }),
            default_weight: AtomicU32::new(cfg.default_queue_weight.max(1)),
            watcher: AtomicBool::new(false),
        }
    }

    /// Apply the `[flow]` section and the injected clock (called by [`install`]).
    pub fn configure(&self, cfg: &FlowSection, clock: Arc<dyn Clock>) {
        let b = bounds(cfg);
        self.inner.counters().set_bounds(b);
        self.rates().set_bounds(b);
        self.default_weight
            .store(cfg.default_queue_weight.max(1), Ordering::Release);
        let _ = self.inner.clock.set(clock);
    }

    /// The meta view the policies are read from.
    pub fn meta(&self) -> &MetaView {
        &self.meta
    }

    /// The node's queue gauges.
    pub fn gauges(&self) -> &Arc<QueueGauges> {
        &self.gauges
    }

    /// Activations of `queue` holding a flow charge now (tests, diagnostics).
    pub fn running(&self, queue: &str) -> u32 {
        self.inner.counters().running(queue)
    }

    /// Activations of `(queue, key)` holding a flow charge now.
    pub fn key_running(&self, queue: &str, key: &str) -> u32 {
        self.inner.counters().key_running(queue, key)
    }

    /// Re-read the cached policies if meta moved; bump `changed()` when one changed.
    pub fn refresh_policies(&self) {
        if self.policies.refresh() {
            self.inner.bump();
        }
    }

    fn rates(&self) -> MutexGuard<'_, RateCells> {
        self.inner
            .rates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Whether a start into `(queue, key)` is subject to a rate limit (cheap; lets the
    /// start path skip the dedupe read when nothing would be charged).
    pub fn rate_limited(&self, queue: &str, key: &str) -> bool {
        self.refresh_policies();
        self.policies.queue(queue).is_some_and(|p| {
            p.rate_limit.as_ref().and_then(Gcra::from_spec).is_some()
                || (!key.is_empty()
                    && p.key_rate_limit
                        .as_ref()
                        .and_then(Gcra::from_spec)
                        .is_some())
        })
    }

    /// Charge one start into `(queue, key)` at `now_ms` against the queue's `rate_limit`
    /// and `key_rate_limit` (both or neither). `Err(retry_after_ms)` when it does not
    /// conform.
    pub fn charge_start(&self, queue: &str, key: &str, now_ms: u64) -> Result<(), u64> {
        self.refresh_policies();
        let Some(p) = self.policies.queue(queue) else {
            return Ok(());
        };
        let rate = p.rate_limit.as_ref().and_then(Gcra::from_spec);
        let key_rate = p.key_rate_limit.as_ref().and_then(Gcra::from_spec);
        self.rates()
            .admit(queue, key, rate, key_rate, now_ms)
            .map_err(|until_ns| ns_to_ms_ceil(until_ns).saturating_sub(now_ms).max(1))
    }

    /// Start the meta watcher once: every apply re-reads the cached policies and bumps
    /// `changed()` on a change. Stops on `shutdown`.
    fn spawn_watcher(self: &Arc<Self>, shutdown: tokio_util::sync::CancellationToken) {
        if self.watcher.swap(true, Ordering::AcqRel) {
            return;
        }
        let Ok(rt) = tokio::runtime::Handle::try_current() else {
            warn!("flow: no tokio runtime; policy changes are seen on the next admission only");
            self.watcher.store(false, Ordering::Release);
            return;
        };
        let me = Arc::clone(self);
        let mut sub = self.meta.subscribe();
        rt.spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    r = sub.changed() => {
                        if r.is_err() {
                            break;
                        }
                        me.refresh_policies();
                    }
                }
            }
        });
    }

    fn permit(&self, charge: Charge, limited: bool) -> FlowPermit {
        let (rb_inner, rel_inner) = (Arc::clone(&self.inner), Arc::clone(&self.inner));
        let rb_charge = charge.clone();
        FlowPermit::new(move || {
            let now = rb_inner.now_ms();
            rb_inner.counters().rollback(&rb_charge, now);
            rb_inner.bump();
        })
        .with_release(move || {
            let now = rel_inner.now_ms();
            rel_inner.counters().release(&charge, now);
            if limited {
                rel_inner.bump();
            }
        })
    }
}

fn bounds(cfg: &FlowSection) -> KeyBounds {
    KeyBounds {
        max_tracked_keys: usize::try_from(cfg.max_tracked_keys)
            .unwrap_or(usize::MAX)
            .max(1),
        idle_evict_ms: u64::try_from(cfg.idle_key_evict.get().as_millis()).unwrap_or(u64::MAX),
    }
}

fn count(queue: &str, result: &'static str) {
    metrics::counter!("ostro_flow_admit_total", "queue" => queue.to_owned(), "result" => result)
        .increment(1);
}

impl std::fmt::Debug for NodeFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFlow")
            .field("policies", &self.policies)
            .finish_non_exhaustive()
    }
}

impl FlowControl for NodeFlow {
    fn admit(&self, e: &EntryView<'_>, now_ms: u64) -> Admit {
        self.inner.last_now_ms.fetch_max(now_ms, Ordering::AcqRel);
        self.refresh_policies();
        if self.policies.workflow(e.workflow).is_some_and(|w| w.paused) {
            count(e.queue, "paused");
            return Admit::Paused;
        }
        let p = self.policies.queue(e.queue);
        if p.as_ref().is_some_and(|p| p.paused) {
            count(e.queue, "paused");
            return Admit::Paused;
        }
        let p = p.as_deref();
        let req = AdmitReq {
            queue: e.queue,
            key: e.concurrency_key,
            concurrency: p.and_then(|p| p.concurrency),
            key_concurrency: p.and_then(|p| p.key_concurrency),
            throttle: p
                .and_then(|p| p.throttle.as_ref())
                .and_then(Gcra::from_spec),
            key_throttle: p
                .and_then(|p| p.key_throttle.as_ref())
                .and_then(Gcra::from_spec),
            first_activation: e.first_activation,
        };
        let limited =
            req.concurrency.is_some() || (!req.key.is_empty() && req.key_concurrency.is_some());
        let r = self.inner.counters().try_admit(&req, now_ms);
        match r {
            Ok(charge) => {
                count(e.queue, "admitted");
                Admit::Yes(self.permit(charge, limited))
            }
            Err(Block::QueueConcurrency) => {
                count(e.queue, "queue_blocked");
                Admit::QueueBlocked { until_ms: None }
            }
            Err(Block::KeyConcurrency) => {
                count(e.queue, "key_blocked");
                Admit::KeyBlocked { until_ms: None }
            }
            Err(Block::QueueThrottle(at)) => {
                count(e.queue, "queue_blocked");
                metrics::counter!("ostro_flow_throttled_total", "queue" => e.queue.to_owned())
                    .increment(1);
                Admit::QueueBlocked {
                    until_ms: Some(ns_to_ms_ceil(at)),
                }
            }
            Err(Block::KeyThrottle(at)) => {
                count(e.queue, "key_blocked");
                metrics::counter!("ostro_flow_throttled_total", "queue" => e.queue.to_owned())
                    .increment(1);
                Admit::KeyBlocked {
                    until_ms: Some(ns_to_ms_ceil(at)),
                }
            }
        }
    }

    fn queue_weight(&self, queue: &str) -> u32 {
        self.refresh_policies();
        self.policies
            .queue(queue)
            .and_then(|p| p.weight)
            .unwrap_or_else(|| self.default_weight.load(Ordering::Acquire))
            .max(1)
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.inner.changed.subscribe()
    }
}

/// Install the node's flow control into the dispatcher configuration: the `[flow]` section
/// and the injected clock, the meta watcher, `cfg.flow` and `cfg.gauges`.
pub fn install(ctx: &P7Ctx, cfg: &mut DispatcherConfig) {
    let flow = &ctx.shared.flow;
    flow.configure(&ctx.config.flow, Arc::clone(&ctx.clock));
    flow.spawn_watcher(ctx.shutdown.clone());
    cfg.flow = Some(Arc::clone(flow) as Arc<dyn FlowControl>);
    cfg.gauges = Some(Arc::clone(&ctx.shared.gauges));
}

#[cfg(test)]
mod tests {
    use super::*;
    use ostro_meta::{MetaCommand, MetaHandle, MetaRecord, QueuePolicy, RateSpec, WorkflowPolicy};

    async fn meta() -> (tempfile::TempDir, MetaHandle) {
        let dir = tempfile::tempdir().expect("tempdir");
        let meta = MetaHandle::start(dir.path(), 1).await.expect("meta");
        (dir, meta)
    }

    /// Create or overwrite (CAS on the current version).
    async fn put(meta: &MetaHandle, record: MetaRecord) {
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
        assert!(matches!(r, ostro_meta::MetaResult::Ok { .. }), "{r:?}");
    }

    fn entry<'a>(q: &'a str, wf: &'a str, key: &'a str, first: bool) -> EntryView<'a> {
        EntryView {
            queue: q,
            workflow: wf,
            tenant: None,
            concurrency_key: key,
            priority: 0,
            first_activation: first,
        }
    }

    fn yes(a: Admit) -> FlowPermit {
        match a {
            Admit::Yes(p) => p,
            other => panic!("expected Yes, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn concurrency_pause_weight_and_changed() {
        let (_d, meta) = meta().await;
        let flow = NodeFlow::new(meta.view().clone(), Arc::new(QueueGauges::new()));
        let mut changed = flow.changed();
        // No policy: everything admitted, weight = default.
        let p0 = yes(flow.admit(&entry("q", "w", "", true), 1));
        drop(p0);
        assert_eq!(flow.queue_weight("q"), 1);
        put(
            &meta,
            MetaRecord::QueuePolicy(QueuePolicy {
                queue: "q".into(),
                concurrency: Some(1),
                weight: Some(3),
                ..QueuePolicy::default()
            }),
        )
        .await;
        changed.mark_unchanged();
        let mut p = yes(flow.admit(&entry("q", "w", "", true), 1));
        assert!(
            changed.has_changed().expect("open"),
            "policy edit bumps changed()"
        );
        assert_eq!(flow.queue_weight("q"), 3);
        assert!(matches!(
            flow.admit(&entry("q", "w", "", true), 1),
            Admit::QueueBlocked { until_ms: None }
        ));
        p.commit();
        changed.mark_unchanged();
        drop(p);
        assert!(
            changed.has_changed().expect("open"),
            "release bumps changed()"
        );
        assert_eq!(flow.running("q"), 0);
        // Workflow pause wins for every queue.
        put(
            &meta,
            MetaRecord::WorkflowPolicy(WorkflowPolicy {
                workflow: "w".into(),
                paused: true,
                ..WorkflowPolicy::default()
            }),
        )
        .await;
        assert!(matches!(
            flow.admit(&entry("q", "w", "", true), 2),
            Admit::Paused
        ));
        assert!(matches!(
            flow.admit(&entry("other", "w", "", true), 2),
            Admit::Paused
        ));
        drop(yes(flow.admit(&entry("other", "w2", "", true), 2)));
        meta.stop().await;
    }

    #[tokio::test]
    async fn throttle_until_and_rollback_through_permits() {
        let (_d, meta) = meta().await;
        put(
            &meta,
            MetaRecord::QueuePolicy(QueuePolicy {
                queue: "t".into(),
                throttle: Some(RateSpec {
                    limit: 5,
                    period_ms: 1_000,
                    burst: 1,
                }),
                key_throttle: Some(RateSpec {
                    limit: 1,
                    period_ms: 10_000,
                    burst: 1,
                }),
                ..QueuePolicy::default()
            }),
        )
        .await;
        let flow = NodeFlow::new(meta.view().clone(), Arc::new(QueueGauges::new()));
        // Uncommitted permit: rolled back, the next first activation conforms at once.
        drop(yes(flow.admit(&entry("t", "w", "", true), 1_000)));
        let mut p = yes(flow.admit(&entry("t", "w", "", true), 1_000));
        p.commit();
        match flow.admit(&entry("t", "w", "", true), 1_000) {
            Admit::QueueBlocked { until_ms } => assert_eq!(until_ms, Some(1_200)),
            other => panic!("expected QueueBlocked, got {other:?}"),
        }
        // A re-activation is never throttled.
        drop(yes(flow.admit(&entry("t", "w", "", false), 1_000)));
        // Key throttle: other keys still go; the same key waits 10 s.
        let mut k = yes(flow.admit(&entry("t", "w", "a", true), 1_200));
        k.commit();
        match flow.admit(&entry("t", "w", "a", true), 1_400) {
            Admit::KeyBlocked { until_ms } => assert_eq!(until_ms, Some(11_200)),
            other => panic!("expected KeyBlocked, got {other:?}"),
        }
        drop(yes(flow.admit(&entry("t", "w", "b", true), 1_400)));
        drop(p);
        meta.stop().await;
    }

    #[tokio::test]
    async fn watcher_bumps_changed_on_a_policy_edit() {
        let (_d, meta) = meta().await;
        let flow = Arc::new(NodeFlow::new(
            meta.view().clone(),
            Arc::new(QueueGauges::new()),
        ));
        let stop = tokio_util::sync::CancellationToken::new();
        flow.spawn_watcher(stop.clone());
        let qp = |paused| {
            MetaRecord::QueuePolicy(QueuePolicy {
                queue: "pq".into(),
                paused,
                ..QueuePolicy::default()
            })
        };
        put(&meta, qp(true)).await;
        assert!(matches!(
            flow.admit(&entry("pq", "w", "", true), 1),
            Admit::Paused
        ));
        let mut changed = flow.changed();
        changed.mark_unchanged();
        put(&meta, qp(false)).await;
        tokio::time::timeout(std::time::Duration::from_secs(5), changed.changed())
            .await
            .expect("watcher bumped changed()")
            .expect("open");
        drop(yes(flow.admit(&entry("pq", "w", "", true), 2)));
        stop.cancel();
        meta.stop().await;
    }
}
