//! Flow-control contract (Phase 7, plan 07-02): admission of ready entries (DISP-03/04/05)
//! and the per-queue gauges shared with placement, demand and Phase 8 metrics.
//!
//! - [`FlowControl::admit`] decides whether an entry may start now: per-queue and per-key
//!   concurrency, throttles (first activations only, D-W13) and pauses. Time is the
//!   injected clock's milliseconds (`now_ms`, D-W26); `until_ms` tells the dispatcher when
//!   to look again.
//! - A granted admission carries a [`FlowPermit`]. Its charge is **tentative** until the
//!   dispatcher's `Placement::reserve` returns `Granted` and it calls
//!   [`FlowPermit::commit`]; an uncommitted permit rolls the charge back on drop (W2).
//! - [`NoFlow`] admits everything with weight 1 (today's behaviour).
//! - [`QueueGauges`] holds per-(partition, queue) gauges written by each partition's
//!   dispatcher and summed per queue by [`QueueGauges::snapshot`] (BTreeMap only).
//!
//! The server implementation (`flow::NodeFlow`, ostro-server, 07-08) and the dispatcher
//! wiring (07-05) come later; the contract is fixed here.

use std::collections::BTreeMap;
use std::sync::Mutex;

use ostro_types::PartitionId;
use tokio::sync::watch;

use super::placement::EntryView;

/// Result of [`FlowControl::admit`].
#[derive(Debug)]
pub enum Admit {
    /// Admitted; commit the permit once placement granted capacity.
    Yes(FlowPermit),
    /// The queue is at a limit; retry at `until_ms` (clock ms) or on `changed()`.
    QueueBlocked { until_ms: Option<u64> },
    /// This entry's concurrency key is at a limit; other keys of the queue may run.
    KeyBlocked { until_ms: Option<u64> },
    /// The workflow or queue is paused.
    Paused,
}

type Hook = Box<dyn FnOnce() + Send + Sync>;

/// RAII flow charge. Dropped uncommitted: the tentative charge is rolled back. Dropped
/// committed: the optional release hook runs (e.g. a concurrency counter decremented when
/// the job ends).
#[must_use = "an uncommitted FlowPermit rolls back on drop"]
pub struct FlowPermit {
    rollback: Option<Hook>,
    release: Option<Hook>,
    committed: bool,
}

impl FlowPermit {
    /// A permit that charges nothing ([`NoFlow`]).
    pub fn none() -> Self {
        Self {
            rollback: None,
            release: None,
            committed: false,
        }
    }

    /// A tentative charge undone by `rollback` unless committed.
    pub fn new(rollback: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self {
            rollback: Some(Box::new(rollback)),
            release: None,
            committed: false,
        }
    }

    /// Run `release` when a committed permit is dropped.
    pub fn with_release(mut self, release: impl FnOnce() + Send + Sync + 'static) -> Self {
        self.release = Some(Box::new(release));
        self
    }

    /// Make the charge final (after `Placement::reserve` returned `Granted`).
    pub fn commit(&mut self) {
        self.committed = true;
        self.rollback = None;
    }

    pub fn is_committed(&self) -> bool {
        self.committed
    }
}

impl Drop for FlowPermit {
    fn drop(&mut self) {
        let hook = if self.committed {
            self.release.take()
        } else {
            self.rollback.take()
        };
        if let Some(f) = hook {
            f();
        }
    }
}

impl std::fmt::Debug for FlowPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowPermit")
            .field("committed", &self.committed)
            .finish_non_exhaustive()
    }
}

/// Admission control of the dispatcher (one per node, shared by every partition).
pub trait FlowControl: Send + Sync + 'static {
    /// May `e` start at `now_ms` (injected clock)? Never blocks.
    fn admit(&self, e: &EntryView<'_>, now_ms: u64) -> Admit;
    /// DRR quantum of `queue` (`QueuePolicy.weight`, default 1; always >= 1).
    fn queue_weight(&self, queue: &str) -> u32;
    /// Bumped whenever a blocked entry may have become admissible.
    fn changed(&self) -> watch::Receiver<u64>;
}

/// Admit everything, every queue weight 1 (the 04-14 behaviour).
#[derive(Debug)]
pub struct NoFlow {
    changed: watch::Sender<u64>,
}

impl NoFlow {
    pub fn new() -> Self {
        Self {
            changed: watch::Sender::new(0),
        }
    }
}

impl Default for NoFlow {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowControl for NoFlow {
    fn admit(&self, _e: &EntryView<'_>, _now_ms: u64) -> Admit {
        Admit::Yes(FlowPermit::none())
    }

    fn queue_weight(&self, _queue: &str) -> u32 {
        1
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}

/// One queue's gauges (per partition in [`QueueGauges::set`], summed in
/// [`QueueGauges::snapshot`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct QueueGauge {
    /// Ready entries.
    pub ready: u64,
    /// Ready entries flow control would admit now.
    pub ready_unblocked: u64,
    /// Oldest `ready_at_ms` among the ready entries (clock ms).
    pub oldest_ready_at_ms: Option<u64>,
    /// Leases running on this node's engine.
    pub running_embedded: u64,
    /// Leases running on remote workers.
    pub running_on_workers: u64,
}

impl QueueGauge {
    fn add(&mut self, o: &QueueGauge) {
        self.ready += o.ready;
        self.ready_unblocked += o.ready_unblocked;
        self.oldest_ready_at_ms = match (self.oldest_ready_at_ms, o.oldest_ready_at_ms) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
        self.running_embedded += o.running_embedded;
        self.running_on_workers += o.running_on_workers;
    }
}

/// Node-wide per-(partition, queue) gauges: each partition's dispatcher writes its own
/// rows; readers (placement/spill, demand, metrics) take a per-queue sum.
#[derive(Debug, Default)]
pub struct QueueGauges {
    rows: Mutex<BTreeMap<(u16, String), QueueGauge>>,
}

impl QueueGauges {
    pub fn new() -> Self {
        Self::default()
    }

    fn rows(&self) -> std::sync::MutexGuard<'_, BTreeMap<(u16, String), QueueGauge>> {
        self.rows
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Replace `partition`'s gauge of `queue`.
    pub fn set(&self, partition: PartitionId, queue: &str, gauge: QueueGauge) {
        self.rows().insert((partition.0, queue.to_owned()), gauge);
    }

    /// Drop every row of `partition` (it lost leadership or stopped).
    pub fn remove_partition(&self, partition: PartitionId) {
        self.rows().retain(|(p, _), _| *p != partition.0);
    }

    /// Per-queue sums over every partition.
    pub fn snapshot(&self) -> BTreeMap<String, QueueGauge> {
        let mut out: BTreeMap<String, QueueGauge> = BTreeMap::new();
        for ((_, q), g) in self.rows().iter() {
            out.entry(q.clone()).or_default().add(g);
        }
        out
    }
}
