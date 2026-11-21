//! `NodePlacement` (plan 07-12, WORK-04, B6, D-W9): the node's [`Placement`].
//!
//! One instance per node, shared by every partition dispatcher. Per queue (placement and
//! selector from [`crate::workers::selector`], default `[workers] default_placement`):
//!
//! | placement | reservation |
//! |---|---|
//! | `embedded` | an embedded slot (`try_acquire_owned` on the `[executor] slots` semaphore, the same one as `DispatcherConfig::slots`), else `Blocked` |
//! | `workers` | a selector-matching worker credit ([`WorkerRegistry::pick`]), else `Blocked` |
//! | `auto`, not spilling | an embedded slot, else `Blocked` |
//! | `auto`, spilling | an embedded slot if one is free (fill embedded first), else a matching worker credit (overflow), else `Blocked` (B6) |
//!
//! Every overflow **attempt** (spilling, embedded full, a worker wanted, whether or not a
//! credit was found) is reported with [`SpillHandle::note_overflow`] at the injected
//! clock's now: 07-13's exit rule counts attempts, not successes.
//!
//! [`Placement::changed`] is one merged watch bumped by every embedded-slot release (the
//! [`EmbeddedSlot`] drop guard), every [`WorkerRegistry::changed`] bump (credit, sessions,
//! attach, detach) and every [`SpillHandle::changed`] transition. With a configured
//! placement it is the dispatcher's only capacity wake-up (07-05 hand-off).
//!
//! A worker reservation holds a [`WorkerSlot`]: the [`CreditGuard`] until the remote runner
//! attaches the job (`take_credit`), plus the queue's compiled placement so a
//! re-activation under the same lease can pick a fresh credit.
//!
//! Metrics: `ostro_placement_reservations_total{queue,kind}` (kind `embedded` | `worker`),
//! `ostro_placement_blocked_total{queue}`.

use std::sync::{Arc, Mutex, PoisonError};

use ostro_partition::leader::placement::{EntryView, Reservation, ReservationKind, Reserve};
use ostro_partition::{Clock, DispatcherConfig, Placement};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tracing::warn;

use crate::config::PlacementSetting;
use crate::p7::P7Ctx;
use crate::workers::registry::{CreditGuard, WorkerRegistry};
use crate::workers::remote_executor::{PlacementExecutor, RemoteRunner};
use crate::workers::selector::{PlacementPolicies, PolicySource, QueuePlacement};
use crate::workers::spill::SpillHandle;

/// Whether a queue currently spills (07-13's [`SpillHandle`] in production).
pub trait SpillSource: Send + Sync + 'static {
    fn spilling(&self, queue: &str) -> bool;
    fn note_overflow(&self, queue: &str, now_ms: u64);
    /// Bumped on every spill transition (`None` = never changes).
    fn changed(&self) -> Option<watch::Receiver<u64>> {
        None
    }
}

impl SpillSource for SpillHandle {
    fn spilling(&self, queue: &str) -> bool {
        SpillHandle::spilling(self, queue)
    }

    fn note_overflow(&self, queue: &str, now_ms: u64) {
        SpillHandle::note_overflow(self, queue, now_ms);
    }

    fn changed(&self) -> Option<watch::Receiver<u64>> {
        Some(SpillHandle::changed(self))
    }
}

/// The guard of an embedded reservation: the slot permit; dropping it returns the slot
/// and then bumps the placement's `changed()`.
pub struct EmbeddedSlot {
    permit: Option<OwnedSemaphorePermit>,
    changed: Arc<watch::Sender<u64>>,
}

impl Drop for EmbeddedSlot {
    fn drop(&mut self) {
        // Release first, then notify: a woken dispatcher must find the slot free.
        drop(self.permit.take());
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}

impl std::fmt::Debug for EmbeddedSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedSlot").finish_non_exhaustive()
    }
}

/// The guard of a worker reservation (see the module docs).
pub struct WorkerSlot {
    credit: Mutex<Option<CreditGuard>>,
    worker_id: String,
    queue: Arc<QueuePlacement>,
}

impl WorkerSlot {
    /// Take the credit for an attach; `None` once taken (a re-activation picks again).
    pub fn take_credit(&self) -> Option<CreditGuard> {
        self.credit
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    /// The worker the reservation was made on.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// The queue's compiled placement (selector) at reservation time.
    pub fn queue(&self) -> &Arc<QueuePlacement> {
        &self.queue
    }
}

impl std::fmt::Debug for WorkerSlot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerSlot")
            .field("worker_id", &self.worker_id)
            .finish_non_exhaustive()
    }
}

/// The node's placement (module docs).
pub struct NodePlacement {
    slots: Arc<Semaphore>,
    registry: Arc<WorkerRegistry>,
    spill: Arc<dyn SpillSource>,
    policies: Arc<dyn PolicySource>,
    clock: Arc<dyn Clock>,
    changed: Arc<watch::Sender<u64>>,
}

impl std::fmt::Debug for NodePlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodePlacement")
            .field("embedded_free", &self.slots.available_permits())
            .field("worker_slots_free", &self.registry.slots_free())
            .finish_non_exhaustive()
    }
}

impl NodePlacement {
    pub fn new(
        slots: Arc<Semaphore>,
        registry: Arc<WorkerRegistry>,
        spill: Arc<dyn SpillSource>,
        policies: Arc<dyn PolicySource>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            slots,
            registry,
            spill,
            policies,
            clock,
            changed: Arc::new(watch::Sender::new(0)),
        }
    }

    /// Bump `changed()`.
    pub fn notify(&self) {
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }

    /// Forward every [`WorkerRegistry::changed`] bump and every spill transition into
    /// `changed()` until `stop`.
    pub fn forward_registry(
        self: &Arc<Self>,
        stop: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let mut credit = self.registry.changed();
        let mut spill = self.spill.changed();
        let changed = Arc::clone(&self.changed);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => return,
                    r = credit.changed() => {
                        if r.is_err() {
                            return;
                        }
                    }
                    r = async { spill.as_mut()?.changed().await.ok() }, if spill.is_some() => {
                        if r.is_none() {
                            spill = None;
                            continue;
                        }
                    }
                }
                changed.send_modify(|v| *v = v.wrapping_add(1));
            }
        })
    }

    fn embedded(&self) -> Option<Reservation> {
        let permit = Arc::clone(&self.slots).try_acquire_owned().ok()?;
        Some(Reservation::new(
            ReservationKind::Embedded,
            EmbeddedSlot {
                permit: Some(permit),
                changed: Arc::clone(&self.changed),
            },
        ))
    }

    fn worker(&self, queue: &Arc<QueuePlacement>) -> Option<Reservation> {
        let credit = self.registry.pick(|w| queue.matches(w.labels))?;
        Some(Reservation::new(
            ReservationKind::Worker,
            WorkerSlot {
                worker_id: credit.worker_id().to_owned(),
                credit: Mutex::new(Some(credit)),
                queue: Arc::clone(queue),
            },
        ))
    }

    fn decide(&self, e: &EntryView<'_>) -> Option<Reservation> {
        let qp = self.policies.queue(e.queue);
        match qp.placement {
            PlacementSetting::Embedded => self.embedded(),
            PlacementSetting::Workers => self.worker(&qp),
            PlacementSetting::Auto => {
                if !self.spill.spilling(e.queue) {
                    return self.embedded();
                }
                if let Some(r) = self.embedded() {
                    return Some(r);
                }
                // Overflow attempt (counted whether or not a credit is found).
                self.spill.note_overflow(e.queue, self.clock.now_ms());
                self.worker(&qp)
            }
        }
    }
}

impl Placement for NodePlacement {
    fn reserve(&self, e: &EntryView<'_>) -> Reserve {
        match self.decide(e) {
            Some(r) => {
                let kind = match r.kind() {
                    ReservationKind::Embedded => "embedded",
                    ReservationKind::Worker => "worker",
                };
                metrics::counter!(
                    "ostro_placement_reservations_total",
                    "queue" => e.queue.to_owned(),
                    "kind" => kind
                )
                .increment(1);
                Reserve::Granted(r)
            }
            None => {
                metrics::counter!("ostro_placement_blocked_total", "queue" => e.queue.to_owned())
                    .increment(1);
                Reserve::Blocked
            }
        }
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

/// Install the node's placement into the dispatcher configuration: `cfg.placement =
/// NodePlacement` over `cfg.slots` (the embedded slot semaphore, Pitfall 1) and
/// `cfg.executor = PlacementExecutor` wrapping the embedded executor (unchanged) with the
/// [`RemoteRunner`] for worker reservations.
pub fn install(ctx: &P7Ctx, cfg: &mut DispatcherConfig) {
    let policies = Arc::new(PlacementPolicies::new(
        ctx.meta.clone(),
        ctx.config.workers.default_placement,
    ));
    let placement = Arc::new(NodePlacement::new(
        Arc::clone(&cfg.slots),
        Arc::clone(&ctx.shared.registry),
        Arc::clone(&ctx.shared.spill) as Arc<dyn SpillSource>,
        policies,
        Arc::clone(&ctx.clock),
    ));
    match tokio::runtime::Handle::try_current() {
        Ok(_) => {
            // Stops with the server (P7Ctx::shutdown); not joined (it holds no resources).
            drop(placement.forward_registry(ctx.shutdown.clone()));
        }
        Err(_) => warn!("no tokio runtime: worker credit changes will not wake dispatchers"),
    }
    cfg.placement = Some(placement as Arc<dyn Placement>);
    let runner = Arc::new(RemoteRunner::new(ctx));
    cfg.executor = Arc::new(PlacementExecutor::new(Arc::clone(&cfg.executor), runner));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    use ostro_partition::ManualClock;
    use ostro_worker_proto::v1;

    use super::*;
    use crate::config::WorkersSection;
    use crate::workers::registry::PeerIdentity;

    struct FakeSpill {
        spilling: AtomicBool,
        overflows: AtomicU64,
        changed: watch::Sender<u64>,
    }

    impl Default for FakeSpill {
        fn default() -> Self {
            Self {
                spilling: AtomicBool::new(false),
                overflows: AtomicU64::new(0),
                changed: watch::Sender::new(0),
            }
        }
    }

    impl SpillSource for FakeSpill {
        fn spilling(&self, _queue: &str) -> bool {
            self.spilling.load(Ordering::SeqCst)
        }
        fn note_overflow(&self, _queue: &str, _now_ms: u64) {
            self.overflows.fetch_add(1, Ordering::SeqCst);
        }
        fn changed(&self) -> Option<watch::Receiver<u64>> {
            Some(self.changed.subscribe())
        }
    }

    struct FakePolicies(HashMap<String, Arc<QueuePlacement>>);

    impl PolicySource for FakePolicies {
        fn queue(&self, queue: &str) -> Arc<QueuePlacement> {
            self.0
                .get(queue)
                .cloned()
                .unwrap_or_else(|| Arc::new(QueuePlacement::new(PlacementSetting::Embedded)))
        }
    }

    struct Fx {
        p: NodePlacement,
        spill: Arc<FakeSpill>,
        registry: Arc<WorkerRegistry>,
        slots: Arc<Semaphore>,
        _txs: Vec<tokio::sync::mpsc::UnboundedReceiver<Result<v1::ServerMsg, tonic::Status>>>,
    }

    fn qp(placement: PlacementSetting, selector: &[&str]) -> Arc<QueuePlacement> {
        Arc::new(QueuePlacement {
            placement,
            selector: Some(crate::workers::selector::compile(selector).expect("selector")),
        })
    }

    fn fx(embedded: usize) -> Fx {
        let mut m = HashMap::new();
        m.insert("e".to_owned(), qp(PlacementSetting::Embedded, &[]));
        m.insert("w".to_owned(), qp(PlacementSetting::Workers, &[]));
        m.insert(
            "gpu".to_owned(),
            qp(PlacementSetting::Workers, &["gpu=true"]),
        );
        m.insert("a".to_owned(), qp(PlacementSetting::Auto, &[]));
        let registry = Arc::new(WorkerRegistry::new(&WorkersSection::default()));
        let spill = Arc::new(FakeSpill::default());
        let slots = Arc::new(Semaphore::new(embedded));
        let p = NodePlacement::new(
            Arc::clone(&slots),
            Arc::clone(&registry),
            Arc::clone(&spill) as Arc<dyn SpillSource>,
            Arc::new(FakePolicies(m)),
            Arc::new(ManualClock::new(1_000)),
        );
        Fx {
            p,
            spill,
            registry,
            slots,
            _txs: Vec::new(),
        }
    }

    impl Fx {
        /// A worker session with `free` credit and `labels`.
        fn worker(&mut self, id: &str, free: u32, labels: &[(&str, &str)]) -> Vec<u8> {
            let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
            self._txs.push(rx);
            let hello = v1::Hello {
                worker_id: id.to_owned(),
                slots: free,
                labels: labels
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                    .collect(),
                ..v1::Hello::default()
            };
            let peer = PeerIdentity {
                cluster_id: "c".into(),
                token_id: Some("wkr_t".into()),
                worker_id: id.to_owned(),
            };
            let ctl = self
                .registry
                .open_session(peer, &hello, 1, tx)
                .expect("session");
            self.registry.credit(&ctl.id, free);
            ctl.id
        }
    }

    fn view(queue: &str) -> EntryView<'_> {
        EntryView {
            queue,
            workflow: "wf",
            tenant: None,
            concurrency_key: "",
            priority: 0,
            first_activation: true,
        }
    }

    fn kind(r: &Reserve) -> Option<ReservationKind> {
        match r {
            Reserve::Granted(r) => Some(r.kind()),
            Reserve::Blocked => None,
        }
    }

    #[test]
    fn embedded_queue_uses_slots_only() {
        let mut f = fx(1);
        f.worker("w1", 4, &[]);
        let r1 = f.p.reserve(&view("e"));
        assert_eq!(kind(&r1), Some(ReservationKind::Embedded));
        // Slot gone: blocked although workers have credit.
        assert_eq!(kind(&f.p.reserve(&view("e"))), None);
        drop(r1);
        assert_eq!(f.slots.available_permits(), 1);
        assert_eq!(
            kind(&f.p.reserve(&view("e"))),
            Some(ReservationKind::Embedded)
        );
    }

    #[test]
    fn workers_queue_uses_matching_credit_only() {
        let mut f = fx(4);
        // No worker: blocked although embedded slots are free.
        assert_eq!(kind(&f.p.reserve(&view("w"))), None);
        f.worker("w1", 1, &[("zone", "a")]);
        let r = f.p.reserve(&view("w"));
        assert_eq!(kind(&r), Some(ReservationKind::Worker));
        assert_eq!(f.slots.available_permits(), 4);
        // Credit used up.
        assert_eq!(kind(&f.p.reserve(&view("w"))), None);
        if let Reserve::Granted(r) = r {
            let slot = r.downcast_ref::<WorkerSlot>().expect("worker slot");
            assert_eq!(slot.worker_id(), "w1");
        }
        // Released with the reservation.
        assert_eq!(
            kind(&f.p.reserve(&view("w"))),
            Some(ReservationKind::Worker)
        );
    }

    #[test]
    fn selector_routes_to_the_labelled_worker() {
        let mut f = fx(0);
        f.worker("plain", 4, &[("zone", "a")]);
        assert_eq!(kind(&f.p.reserve(&view("gpu"))), None);
        f.worker("gpu1", 1, &[("gpu", "true")]);
        let r = f.p.reserve(&view("gpu"));
        match &r {
            Reserve::Granted(r) => assert_eq!(
                r.downcast_ref::<WorkerSlot>().expect("slot").worker_id(),
                "gpu1"
            ),
            Reserve::Blocked => panic!("gpu queue blocked with a gpu worker"),
        }
        // The only matching worker is busy; the plain worker never takes gpu work.
        assert_eq!(kind(&f.p.reserve(&view("gpu"))), None);
    }

    #[test]
    fn auto_not_spilling_is_embedded_only() {
        let mut f = fx(1);
        f.worker("w1", 4, &[]);
        let r1 = f.p.reserve(&view("a"));
        assert_eq!(kind(&r1), Some(ReservationKind::Embedded));
        assert_eq!(kind(&f.p.reserve(&view("a"))), None);
        assert_eq!(f.spill.overflows.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn auto_spilling_fills_embedded_first_then_overflows() {
        let mut f = fx(1);
        f.spill.spilling.store(true, Ordering::SeqCst);
        // Embedded slot free: embedded first (B6), no overflow attempt.
        let r1 = f.p.reserve(&view("a"));
        assert_eq!(kind(&r1), Some(ReservationKind::Embedded));
        assert_eq!(f.spill.overflows.load(Ordering::SeqCst), 0);
        // Embedded full, no worker: blocked, but the attempt is counted.
        assert_eq!(kind(&f.p.reserve(&view("a"))), None);
        assert_eq!(f.spill.overflows.load(Ordering::SeqCst), 1);
        // With a worker: overflow to its credit.
        f.worker("w1", 1, &[]);
        let r2 = f.p.reserve(&view("a"));
        assert_eq!(kind(&r2), Some(ReservationKind::Worker));
        assert_eq!(f.spill.overflows.load(Ordering::SeqCst), 2);
        // A freed embedded slot is preferred again.
        drop(r1);
        assert_eq!(
            kind(&f.p.reserve(&view("a"))),
            Some(ReservationKind::Embedded)
        );
        assert_eq!(f.spill.overflows.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn changed_bumps_on_release_credit_and_notify() {
        let f = fx(1);
        let Fx {
            p,
            registry,
            spill,
            mut _txs,
            ..
        } = f;
        let p = Arc::new(p);
        let stop = tokio_util::sync::CancellationToken::new();
        let fwd = p.forward_registry(stop.clone());
        let mut rx = p.changed();
        async fn bumped(rx: &mut watch::Receiver<u64>) {
            tokio::time::timeout(std::time::Duration::from_secs(5), rx.changed())
                .await
                .expect("changed in time")
                .expect("open");
            rx.borrow_and_update();
        }
        rx.borrow_and_update();
        // Embedded release.
        let r = p.reserve(&view("e"));
        assert_eq!(kind(&r), Some(ReservationKind::Embedded));
        drop(r);
        bumped(&mut rx).await;
        // Worker credit appears (registry bump forwarded).
        let mut tmp = Fx {
            p: fx(0).p,
            spill: Arc::new(FakeSpill::default()),
            registry: Arc::clone(&registry),
            slots: Arc::new(Semaphore::new(0)),
            _txs: Vec::new(),
        };
        let sid = tmp.worker("w1", 1, &[]);
        bumped(&mut rx).await;
        // Credit taken and returned.
        let r = p.reserve(&view("w"));
        assert_eq!(kind(&r), Some(ReservationKind::Worker));
        bumped(&mut rx).await;
        drop(r);
        bumped(&mut rx).await;
        // Credit withdrawn by the worker.
        registry.credit(&sid, 0);
        bumped(&mut rx).await;
        // Spill transition (forwarded from the spill handle's watch).
        spill.spilling.store(true, Ordering::SeqCst);
        spill.changed.send_modify(|v| *v += 1);
        bumped(&mut rx).await;
        p.notify();
        bumped(&mut rx).await;
        stop.cancel();
        fwd.await.expect("forwarder");
        _txs.clear();
    }
}
