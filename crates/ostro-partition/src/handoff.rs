//! Phase 13: implemented in 13-11 (graceful leader handoff, D-P13-13).
//!
//! [`HandoffControl`] is the node-local control the cluster's `LocalHandoff` (ostro-cluster
//! `handoff.rs`) drives when partition `p` moves from this leader to another ISR member:
//!
//! 1. `prepare_handoff(p, grace)`: admission for `p` turns off (new submissions are refused
//!    with a retryable `leader_changed`, so the log end stops moving: write fence W11), and the
//!    call resolves once in-flight activations reached a flush/suspend point or `grace`
//!    elapsed (hot-waiting runs are evicted). From here until the guard is released,
//!    [`HandoffControl::in_handoff`] is true and partition reads are refused (W2).
//! 2. The caller waits for the target's durable LEO to equal ours and proposes
//!    `ElectLeader{epoch + 1, leader: target}`.
//! 3. [`HandoffGuard::complete`] (the role driver demotes this node) or
//!    [`HandoffGuard::abort`] (admission back on; the target fell out of the ISR, timeout).
//!
//! No run is drained, cancelled or failed: unfinished activations replay on the new leader.
//!
//! # Implementation (13-11)
//!
//! Every [`PartitionHandle`](crate::PartitionHandle) owns one [`HandoffCell`] (shared by its
//! clones). The cell holds a [`HandoffMark`]: the leader epoch the handoff started at and
//! whether appends are frozen. Every check compares the mark with the handle's **current**
//! epoch, so the fence lifts by itself when the role driver demotes this node (the handle's
//! epoch drops to 0) and never survives into a later term: no role-driver change is needed.
//!
//! - **Admission (dispatcher):** while [`HandoffCell::quiescing`] is true the dispatcher leases
//!   no new attempt (ready entries stay queued, timers still fire into the log), and a job
//!   that ends `Reactivate` (hot wait elapsed or evicted, budget pressure) gives its lease
//!   back and is re-queued instead of re-run, so it replays on the new leader (or here after
//!   an abort). The dispatcher publishes its lease count per partition
//!   ([`HandoffCell::leased`]).
//! - **Hot-wait eviction (executor):** the server's input bridge closes a job's hot-wait
//!   input stream when its partition starts quiescing; a parked activation (its intents are
//!   flushed before it parks) is then evicted at once and ends `Reactivate`.
//! - **Write fence (W11):** [`HandoffGuard::freeze_appends`] makes
//!   [`PartitionHandle::submit`](crate::PartitionHandle::submit) refuse new commands with the
//!   retryable [`PartitionError::LeaderChanged`] (API: 503 `leader_changed`); appends already
//!   handed to the log complete.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_types::{Epoch, PartitionId};
use tokio::sync::watch;

use crate::handle::PartitionError;

/// The handoff state of one partition handle: the epoch a handoff started at (`0` = none)
/// and whether appends are frozen.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandoffMark {
    /// Leader epoch of the handoff in progress (or completed, until demotion); 0 = none.
    pub epoch: u32,
    /// Appends are refused (write fence W11).
    pub frozen: bool,
}

/// Per-partition handoff state, owned by the partition handle (see the module docs).
#[derive(Debug)]
pub struct HandoffCell {
    mark: watch::Sender<HandoffMark>,
    leased: watch::Sender<usize>,
    evicted: AtomicU64,
}

impl Default for HandoffCell {
    fn default() -> Self {
        Self {
            mark: watch::channel(HandoffMark::default()).0,
            leased: watch::channel(0).0,
            evicted: AtomicU64::new(0),
        }
    }
}

impl HandoffCell {
    /// Start a handoff at leader epoch `epoch` (admission off). `false` when a handoff at
    /// this epoch is already in progress or `epoch` is 0 (not leading).
    pub fn begin(&self, epoch: Epoch) -> bool {
        if epoch.0 == 0 {
            return false;
        }
        self.mark.send_if_modified(|m| {
            if m.epoch == epoch.0 {
                return false;
            }
            *m = HandoffMark {
                epoch: epoch.0,
                frozen: false,
            };
            true
        })
    }

    /// Freeze appends of the handoff at `epoch` (no-op for another epoch).
    pub fn freeze(&self, epoch: Epoch) {
        self.mark.send_if_modified(|m| {
            if m.epoch != epoch.0 || m.frozen {
                return false;
            }
            m.frozen = true;
            true
        });
    }

    /// Abort the handoff at `epoch`: admission back on, appends accepted (no-op for another
    /// epoch, e.g. a newer handoff started meanwhile).
    pub fn clear(&self, epoch: Epoch) {
        self.mark.send_if_modified(|m| {
            if m.epoch != epoch.0 {
                return false;
            }
            *m = HandoffMark::default();
            true
        });
    }

    /// The current mark.
    pub fn mark(&self) -> HandoffMark {
        *self.mark.borrow()
    }

    /// Changes of the mark (the dispatcher and the executor's input bridge wake on it).
    pub fn subscribe(&self) -> watch::Receiver<HandoffMark> {
        self.mark.subscribe()
    }

    /// Whether a handoff is in progress for a handle at leader epoch `current`.
    pub fn quiescing(&self, current: Epoch) -> bool {
        Self::quiescing_at(&self.mark(), current)
    }

    /// [`quiescing`](Self::quiescing) for a mark read elsewhere.
    pub fn quiescing_at(m: &HandoffMark, current: Epoch) -> bool {
        m.epoch != 0 && m.epoch == current.0
    }

    /// Whether appends are frozen for a handle at leader epoch `current`.
    pub fn frozen(&self, current: Epoch) -> bool {
        let m = self.mark();
        m.frozen && Self::quiescing_at(&m, current)
    }

    /// Leases the partition's dispatcher holds right now (published every loop turn).
    pub fn leased(&self) -> usize {
        *self.leased.borrow()
    }

    /// Watch of [`leased`](Self::leased).
    pub fn leased_watch(&self) -> watch::Receiver<usize> {
        self.leased.subscribe()
    }

    /// Dispatcher: publish the lease count.
    pub fn set_leased(&self, n: usize) {
        self.leased.send_if_modified(|v| {
            if *v == n {
                return false;
            }
            *v = n;
            true
        });
    }

    /// Activations given back because the partition was quiescing (monotonic).
    pub fn evicted(&self) -> u64 {
        self.evicted.load(Ordering::Relaxed)
    }

    /// Dispatcher: one activation was given back while quiescing.
    pub fn note_evicted(&self) {
        self.evicted.fetch_add(1, Ordering::Relaxed);
    }
}

/// What [`HandoffControl::prepare_handoff`] reports in its guard.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandoffReport {
    /// Leader epoch the handoff started at.
    pub epoch: Epoch,
    /// Leased activations still running when `prepare_handoff` returned (grace elapsed);
    /// they finish, or are fenced after demotion and replay on the new leader.
    pub leased_left: usize,
    /// Activations given back (hot waits evicted, re-activations re-queued) during the wait.
    pub evicted: u64,
    /// The partition clock (leader services' injected clock) when the handoff started.
    pub started_at_ms: u64,
}

/// Node-local handoff control (implemented by `Node` in 13-11).
pub trait HandoffControl: Send + Sync + 'static {
    /// Turn admission for `p` off and wait up to `grace` for in-flight activations to reach a
    /// flush/suspend point. Fails if this node does not lead `p` or a handoff of `p` is
    /// already in progress.
    fn prepare_handoff(
        &self,
        p: PartitionId,
        grace: Duration,
    ) -> BoxFuture<'_, Result<HandoffGuard, PartitionError>>;

    /// Whether a handoff of `p` is in progress (from `prepare_handoff` until the guard is
    /// released). Partition reads are refused while it is set (D-P13-13 W2, D-P13-15).
    fn in_handoff(&self, p: PartitionId) -> bool;
}

/// How a [`HandoffGuard`] was released.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoffRelease {
    /// The handoff was abandoned: admission for the partition is back on.
    Aborted,
    /// Leadership moved; the role driver takes over (admission stays off until demotion).
    Completed,
}

type ReleaseFn = Box<dyn FnOnce(HandoffRelease) + Send>;

/// The admission token of one in-progress handoff. Dropping it without
/// [`complete`](Self::complete) aborts (admission back on), so an error path never leaves a
/// partition fenced.
pub struct HandoffGuard {
    partition: PartitionId,
    release: Option<ReleaseFn>,
    report: HandoffReport,
    freeze: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl std::fmt::Debug for HandoffGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandoffGuard")
            .field("partition", &self.partition)
            .field("released", &self.release.is_none())
            .field("report", &self.report)
            .finish()
    }
}

impl HandoffGuard {
    /// A guard for `partition`; `release` runs exactly once, on abort, complete or drop.
    pub fn new(
        partition: PartitionId,
        release: impl FnOnce(HandoffRelease) + Send + 'static,
    ) -> Self {
        Self {
            partition,
            release: Some(Box::new(release)),
            report: HandoffReport::default(),
            freeze: None,
        }
    }

    /// Attach the quiescing report.
    pub fn with_report(mut self, report: HandoffReport) -> Self {
        self.report = report;
        self
    }

    /// Attach the write fence [`freeze_appends`](Self::freeze_appends) runs.
    pub fn with_freeze(mut self, freeze: impl Fn() + Send + Sync + 'static) -> Self {
        self.freeze = Some(Arc::new(freeze));
        self
    }

    /// The partition being handed off.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// What the quiescing achieved.
    pub fn report(&self) -> HandoffReport {
        self.report
    }

    /// Write fence (W11): refuse new submissions to the partition with the retryable
    /// [`PartitionError::LeaderChanged`] so its log end stops moving. Lifted by
    /// [`abort`](Self::abort) (or drop); after [`complete`](Self::complete) it stays until
    /// demotion. Idempotent; a no-op on a guard without a fence.
    pub fn freeze_appends(&self) {
        if let Some(f) = &self.freeze {
            f();
        }
    }

    /// Abandon the handoff: admission back on.
    pub fn abort(mut self) {
        self.finish(HandoffRelease::Aborted);
    }

    /// Leadership moved to the target.
    pub fn complete(mut self) {
        self.finish(HandoffRelease::Completed);
    }

    fn finish(&mut self, how: HandoffRelease) {
        if let Some(f) = self.release.take() {
            f(how);
        }
    }
}

impl Drop for HandoffGuard {
    fn drop(&mut self) {
        self.finish(HandoffRelease::Aborted);
    }
}

/// 13-11: quiesce a partition this node leads (see the module docs).
impl HandoffControl for crate::Node {
    fn prepare_handoff(
        &self,
        p: PartitionId,
        grace: Duration,
    ) -> BoxFuture<'_, Result<HandoffGuard, PartitionError>> {
        let handle = self.handle(p);
        let role = self.role(p).map(|r| *r.borrow());
        let clock = self.leader_clock(p);
        Box::pin(async move {
            let handle = handle.ok_or_else(|| {
                PartitionError::Failed(format!("partition {} is not running here", p.0))
            })?;
            let epoch = match role {
                Some(crate::roles::PartitionRole::Leader { epoch }) => epoch,
                _ => return Err(PartitionError::NotLeader),
            };
            if handle.epoch() != epoch {
                return Err(PartitionError::NotLeader);
            }
            let started_at_ms = clock.map_or(0, |c| c.now_ms());
            quiesce(handle, epoch, grace, started_at_ms).await
        })
    }

    fn in_handoff(&self, p: PartitionId) -> bool {
        self.handle(p)
            .is_some_and(|h| h.handoff().quiescing(h.epoch()))
    }
}

/// Admission off for `handle` at `epoch`, then wait up to `grace` for its leases to reach 0.
/// The guard's release clears the mark on abort; on completion the mark stays (the handle's
/// epoch drops at demotion, which lifts it).
pub async fn quiesce(
    handle: crate::PartitionHandle<ostro_log::PartitionLog>,
    epoch: Epoch,
    grace: Duration,
    started_at_ms: u64,
) -> Result<HandoffGuard, PartitionError> {
    let p = handle.partition();
    let cell = handle.handoff_cell();
    if !cell.begin(epoch) {
        return Err(PartitionError::Failed(format!(
            "partition {}: a handoff at epoch {} is already in progress",
            p.0, epoch.0
        )));
    }
    let evicted_before = cell.evicted();
    // Guard first, so a cancelled caller (dropped future) aborts.
    let release_cell = Arc::clone(&cell);
    let guard = HandoffGuard::new(p, move |how| {
        if how == HandoffRelease::Aborted {
            release_cell.clear(epoch);
        }
    });
    let mut leased = cell.leased_watch();
    // The dispatcher publishes once per loop turn; the mark change wakes it.
    let _ = tokio::time::timeout(grace, leased.wait_for(|n| *n == 0)).await;
    let report = HandoffReport {
        epoch,
        leased_left: cell.leased(),
        evicted: cell.evicted().saturating_sub(evicted_before),
        started_at_ms,
    };
    let freeze_cell = Arc::clone(&cell);
    Ok(guard
        .with_report(report)
        .with_freeze(move || freeze_cell.freeze(epoch)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    fn recorder() -> (
        Arc<Mutex<Vec<HandoffRelease>>>,
        impl FnOnce(HandoffRelease) + Send,
    ) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let l = Arc::clone(&log);
        (log, move |r| l.lock().unwrap().push(r))
    }

    #[test]
    fn guard_releases_exactly_once() {
        let (log, f) = recorder();
        let g = HandoffGuard::new(PartitionId(3), f);
        assert_eq!(g.partition(), PartitionId(3));
        g.complete();
        assert_eq!(*log.lock().unwrap(), [HandoffRelease::Completed]);

        let (log, f) = recorder();
        HandoffGuard::new(PartitionId(1), f).abort();
        assert_eq!(*log.lock().unwrap(), [HandoffRelease::Aborted]);

        let (log, f) = recorder();
        drop(HandoffGuard::new(PartitionId(1), f));
        assert_eq!(
            *log.lock().unwrap(),
            [HandoffRelease::Aborted],
            "drop aborts"
        );
    }

    #[test]
    fn cell_marks_follow_the_term() {
        let c = HandoffCell::default();
        let e = Epoch(4);
        assert!(!c.quiescing(e));
        assert!(!c.begin(Epoch(0)), "not leading: no handoff");
        assert!(c.begin(e));
        assert!(!c.begin(e), "one handoff per term");
        assert!(c.quiescing(e) && !c.frozen(e));
        assert!(!c.quiescing(Epoch(0)), "demoted: lifted");
        assert!(!c.quiescing(Epoch(5)), "a later term: lifted");
        c.freeze(Epoch(3));
        assert!(!c.frozen(e), "freeze of another epoch is a no-op");
        c.freeze(e);
        assert!(c.frozen(e) && !c.frozen(Epoch(5)));
        c.clear(Epoch(3));
        assert!(c.frozen(e), "clear of another epoch is a no-op");
        c.clear(e);
        assert!(!c.quiescing(e) && !c.frozen(e));
        c.set_leased(2);
        assert_eq!(c.leased(), 2);
        c.note_evicted();
        assert_eq!(c.evicted(), 1);
    }

    #[test]
    fn guard_freeze_and_report() {
        let frozen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let f = Arc::clone(&frozen);
        let g = HandoffGuard::new(PartitionId(2), |_| {})
            .with_report(HandoffReport {
                epoch: Epoch(3),
                leased_left: 1,
                evicted: 2,
                started_at_ms: 9,
            })
            .with_freeze(move || {
                f.fetch_add(1, Ordering::SeqCst);
            });
        g.freeze_appends();
        assert_eq!(frozen.load(Ordering::SeqCst), 1);
        assert_eq!(g.report().leased_left, 1);
        assert_eq!(g.report().evicted, 2);
        HandoffGuard::new(PartitionId(2), |_| {}).freeze_appends(); // no fence: no-op
    }
}
