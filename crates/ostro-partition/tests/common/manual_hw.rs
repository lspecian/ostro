//! `ManualHwLog<L>`: a test double that forwards everything to an inner log but publishes
//! `min(inner HW, cap)` as its own high watermark, so tests can hold apply back.

use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use ostro_log::{
    AppendReceipt, EpochHistory, FollowerProgress, Isr, LogError, LogHealth, RawBatch, ReplicaSet,
    ReplicaStatus, ReplicatedLog, Role, TrimPermit, VerifiedBatch,
};
use ostro_types::{Epoch, Offset, PartitionId};
use tokio::sync::watch;

pub struct ManualHwLog<L: ReplicatedLog> {
    inner: Arc<L>,
    cap: watch::Sender<Offset>,
    hw: watch::Sender<Offset>,
}

impl<L: ReplicatedLog> ManualHwLog<L> {
    /// Wrap `inner` with an uncapped HW (cap = `Offset(u64::MAX)`). Spawns a forwarding task
    /// on the current runtime.
    pub fn new(inner: Arc<L>) -> Arc<Self> {
        let start = *inner.high_watermark().borrow();
        let me = Arc::new(Self {
            inner: Arc::clone(&inner),
            cap: watch::channel(Offset(u64::MAX)).0,
            hw: watch::channel(start).0,
        });
        let weak = Arc::downgrade(&me);
        let mut inner_rx = inner.high_watermark();
        let mut cap_rx = me.cap.subscribe();
        tokio::spawn(async move {
            loop {
                {
                    let Some(me) = weak.upgrade() else { break };
                    let v = (*inner_rx.borrow_and_update()).min(*cap_rx.borrow_and_update());
                    me.publish(v);
                }
                tokio::select! {
                    r = inner_rx.changed() => if r.is_err() { break },
                    r = cap_rx.changed() => if r.is_err() { break },
                }
            }
        });
        me
    }

    /// Set the cap and republish immediately.
    pub fn set_cap(&self, cap: Offset) {
        self.cap.send_replace(cap);
        let v = (*self.inner.high_watermark().borrow()).min(cap);
        self.publish(v);
    }

    pub fn inner(&self) -> &Arc<L> {
        &self.inner
    }

    fn publish(&self, v: Offset) {
        self.hw.send_if_modified(|h| {
            if *h != v {
                *h = v;
                true
            } else {
                false
            }
        });
    }
}

impl<L: ReplicatedLog> ReplicatedLog for ManualHwLog<L> {
    fn partition(&self) -> PartitionId {
        self.inner.partition()
    }

    fn append(
        &self,
        epoch: Epoch,
        cmds: Vec<Bytes>,
    ) -> impl Future<Output = Result<AppendReceipt, LogError>> + Send {
        self.inner.append(epoch, cmds)
    }

    fn wait_committed(&self, last: Offset) -> impl Future<Output = Result<(), LogError>> + Send {
        self.inner.wait_committed(last)
    }

    fn high_watermark(&self) -> watch::Receiver<Offset> {
        self.hw.subscribe()
    }

    fn log_end(&self) -> Offset {
        self.inner.log_end()
    }

    fn log_start(&self) -> Offset {
        self.inner.log_start()
    }

    fn read(
        &self,
        from: Offset,
        upto: Offset,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Vec<VerifiedBatch>, LogError>> + Send {
        let upto = upto.min(*self.hw.borrow());
        self.inner.read(from, upto, max_bytes)
    }

    fn epochs(&self) -> EpochHistory {
        self.inner.epochs()
    }

    fn isr(&self) -> Isr {
        self.inner.isr()
    }

    fn role(&self) -> watch::Receiver<Role> {
        self.inner.role()
    }

    fn health(&self) -> watch::Receiver<LogHealth> {
        self.inner.health()
    }

    fn truncate_to(&self, end: Offset) -> impl Future<Output = Result<Offset, LogError>> + Send {
        self.inner.truncate_to(end)
    }

    fn trim_prefix(
        &self,
        permit: TrimPermit,
    ) -> impl Future<Output = Result<Offset, LogError>> + Send {
        self.inner.trim_prefix(permit)
    }

    fn read_replica(
        &self,
        from: Offset,
        max_bytes: usize,
    ) -> impl Future<Output = Result<Vec<RawBatch>, LogError>> + Send {
        self.inner.read_replica(from, max_bytes)
    }

    fn append_replica(
        &self,
        batches: Vec<RawBatch>,
    ) -> impl Future<Output = Result<Offset, LogError>> + Send {
        self.inner.append_replica(batches)
    }

    fn durable_end(&self) -> Offset {
        self.inner.durable_end()
    }

    fn durable_end_watch(&self) -> watch::Receiver<Offset> {
        self.inner.durable_end_watch()
    }

    fn latest_epoch(&self) -> Option<Epoch> {
        self.inner.latest_epoch()
    }

    fn become_leader(&self, epoch: Epoch, set: ReplicaSet) -> Result<(), LogError> {
        self.inner.become_leader(epoch, set)
    }

    fn become_follower(&self, epoch: Epoch, leader: u64) -> Result<(), LogError> {
        self.inner.become_follower(epoch, leader)
    }

    fn set_follower_hw(&self, leader_hw: Offset) {
        self.inner.set_follower_hw(leader_hw)
    }

    fn record_progress(&self, p: FollowerProgress) -> Result<(), LogError> {
        self.inner.record_progress(p)
    }

    fn set_published_isr(&self, epoch: Epoch, set: ReplicaSet) -> Result<(), LogError> {
        self.inner.set_published_isr(epoch, set)
    }

    fn reset_to(
        &self,
        start: Offset,
        epochs: EpochHistory,
    ) -> impl Future<Output = Result<(), LogError>> + Send {
        self.inner.reset_to(start, epochs)
    }

    fn replica_status(&self) -> ReplicaStatus {
        self.inner.replica_status()
    }
}
