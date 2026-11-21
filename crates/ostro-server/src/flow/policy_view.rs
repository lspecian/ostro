//! Per-queue and per-workflow flow limits read from the meta view (plan 07-08).
//!
//! `QueuePolicy.{concurrency, key_concurrency, throttle, key_throttle, rate_limit,
//! key_rate_limit, weight, paused, on_retries_exhausted}` and `WorkflowPolicy.{paused,
//! on_retries_exhausted}` are cached in two `BTreeMap`s behind an [`ArcSwap`], so the
//! dispatcher's hot path (`admit`, `queue_weight`) is one atomic load plus a map lookup.
//!
//! The meta state has point getters only (no iteration over policies), so the cache is
//! keyed by the names the node has asked about: a name enters on its first lookup
//! (`None` = no policy) and every meta apply re-reads the cached names
//! ([`PolicyView::refresh`]). `NodeFlow` refreshes before every decision when meta moved
//! (one atomic compare otherwise), and the watcher started by `flow::install` refreshes
//! on every apply.
//! A refresh that changes any cached policy returns `true`; the caller then bumps the
//! flow's `changed()` watch so blocked and paused entries are re-evaluated without
//! polling (a pause/resume CAS through 07-14's API lands here).
//!
//! The cache holds at most one entry per queue / workflow name that ever had a ready
//! entry or a start on this node; names are not evicted (their number is bounded by the
//! deployed workflows and configured queues, not by traffic).

use std::collections::BTreeMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use ostro_meta::{MetaView, QueuePolicy, RateSpec, WorkflowPolicy};

/// The flow-relevant part of a `QueuePolicy`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FlowPolicy {
    pub concurrency: Option<u32>,
    pub key_concurrency: Option<u32>,
    pub weight: Option<u32>,
    pub throttle: Option<RateSpec>,
    pub key_throttle: Option<RateSpec>,
    pub rate_limit: Option<RateSpec>,
    pub key_rate_limit: Option<RateSpec>,
    pub on_retries_exhausted: Option<u8>,
    pub paused: bool,
}

impl From<&QueuePolicy> for FlowPolicy {
    fn from(p: &QueuePolicy) -> Self {
        Self {
            concurrency: p.concurrency,
            key_concurrency: p.key_concurrency,
            weight: p.weight,
            throttle: p.throttle.clone(),
            key_throttle: p.key_throttle.clone(),
            rate_limit: p.rate_limit.clone(),
            key_rate_limit: p.key_rate_limit.clone(),
            on_retries_exhausted: p.on_retries_exhausted,
            paused: p.paused,
        }
    }
}

/// The flow-relevant part of a `WorkflowPolicy`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkflowFlow {
    pub paused: bool,
    pub on_retries_exhausted: Option<u8>,
}

impl From<&WorkflowPolicy> for WorkflowFlow {
    fn from(p: &WorkflowPolicy) -> Self {
        Self {
            paused: p.paused,
            on_retries_exhausted: p.on_retries_exhausted,
        }
    }
}

/// One immutable cache generation.
#[derive(Clone, Debug, Default)]
struct Snapshot {
    /// Meta applied index the cache reflects.
    applied: u64,
    queues: BTreeMap<String, Option<Arc<FlowPolicy>>>,
    workflows: BTreeMap<String, Option<Arc<WorkflowFlow>>>,
}

/// Cached flow policies over a [`MetaView`].
pub struct PolicyView {
    meta: MetaView,
    snap: ArcSwap<Snapshot>,
    /// Serialises writers (miss inserts and refreshes); readers never take it.
    write: std::sync::Mutex<()>,
}

impl std::fmt::Debug for PolicyView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = self.snap.load();
        f.debug_struct("PolicyView")
            .field("applied", &s.applied)
            .field("queues", &s.queues.len())
            .field("workflows", &s.workflows.len())
            .finish()
    }
}

impl PolicyView {
    pub fn new(meta: MetaView) -> Self {
        let applied = meta.applied();
        Self {
            meta,
            snap: ArcSwap::from_pointee(Snapshot {
                applied,
                ..Snapshot::default()
            }),
            write: std::sync::Mutex::new(()),
        }
    }

    /// The meta view the policies come from.
    pub fn meta(&self) -> &MetaView {
        &self.meta
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.write
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn read_queue(&self, q: &str) -> Option<Arc<FlowPolicy>> {
        self.meta.read(|s| {
            s.queue_policy(q)
                .map(|v| Arc::new(FlowPolicy::from(&v.value)))
        })
    }

    fn read_workflow(&self, w: &str) -> Option<Arc<WorkflowFlow>> {
        self.meta.read(|s| {
            s.workflow_policy(w)
                .map(|v| Arc::new(WorkflowFlow::from(&v.value)))
        })
    }

    /// Re-read every cached name if meta moved past the cache. `true` when any cached
    /// policy changed (bump `changed()`).
    pub fn refresh(&self) -> bool {
        let applied = self.meta.applied();
        if self.snap.load().applied >= applied {
            return false;
        }
        let _g = self.lock();
        let cur = self.snap.load_full();
        if cur.applied >= applied {
            return false;
        }
        let mut next = Snapshot {
            applied,
            queues: BTreeMap::new(),
            workflows: BTreeMap::new(),
        };
        let mut changed = false;
        for (q, old) in &cur.queues {
            let new = self.read_queue(q);
            changed |= old.as_deref() != new.as_deref();
            next.queues.insert(q.clone(), new);
        }
        for (w, old) in &cur.workflows {
            let new = self.read_workflow(w);
            changed |= old.as_deref() != new.as_deref();
            next.workflows.insert(w.clone(), new);
        }
        self.snap.store(Arc::new(next));
        changed
    }

    /// The queue's flow policy (`None` = no policy). Never refreshes: the caller
    /// (`NodeFlow`) runs [`refresh`](Self::refresh) first and bumps `changed()` on change.
    pub fn queue(&self, q: &str) -> Option<Arc<FlowPolicy>> {
        if let Some(p) = self.snap.load().queues.get(q) {
            return p.clone();
        }
        let _g = self.lock();
        let cur = self.snap.load_full();
        if let Some(p) = cur.queues.get(q) {
            return p.clone();
        }
        let p = self.read_queue(q);
        let mut next = (*cur).clone();
        next.queues.insert(q.to_owned(), p.clone());
        self.snap.store(Arc::new(next));
        p
    }

    /// The workflow's flow policy (`None` = no policy); like [`queue`](Self::queue).
    pub fn workflow(&self, w: &str) -> Option<Arc<WorkflowFlow>> {
        if let Some(p) = self.snap.load().workflows.get(w) {
            return p.clone();
        }
        let _g = self.lock();
        let cur = self.snap.load_full();
        if let Some(p) = cur.workflows.get(w) {
            return p.clone();
        }
        let p = self.read_workflow(w);
        let mut next = (*cur).clone();
        next.workflows.insert(w.to_owned(), p.clone());
        self.snap.store(Arc::new(next));
        p
    }
}
