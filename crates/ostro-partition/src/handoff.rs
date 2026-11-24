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
//! `Node` implements the trait in 13-11; this file holds the fixed contract.

use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_types::PartitionId;

use crate::handle::PartitionError;

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
}

impl std::fmt::Debug for HandoffGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HandoffGuard")
            .field("partition", &self.partition)
            .field("released", &self.release.is_none())
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
        }
    }

    /// The partition being handed off.
    pub fn partition(&self) -> PartitionId {
        self.partition
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

/// Phase 13 stub (13-02 needs the impl so the server seam can build `LocalHandoff` over the
/// node; 13-11 replaces the body): no handoff is ever prepared and no partition is ever in
/// handoff, so every caller keeps the Phase 12 behaviour.
impl HandoffControl for crate::Node {
    fn prepare_handoff(
        &self,
        p: PartitionId,
        grace: Duration,
    ) -> BoxFuture<'_, Result<HandoffGuard, PartitionError>> {
        let _ = grace;
        Box::pin(async move {
            Err(PartitionError::Failed(format!(
                "partition {}: graceful handoff is implemented in 13-11",
                p.0
            )))
        })
    }

    fn in_handoff(&self, p: PartitionId) -> bool {
        let _ = p;
        false
    }
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
}
