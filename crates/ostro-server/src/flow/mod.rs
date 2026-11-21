//! Node flow control (Phase 7, DISP-03/04/05; implemented in 07-08).
//!
//! | File | Owner | Purpose |
//! |---|---|---|
//! | [`policy_view`] | 07-08 | `QueuePolicy` / `WorkflowPolicy` limits read from meta |
//! | [`limits`] | 07-08 | node-wide concurrency counters + hand-rolled GCRA (D-W24) |
//! | [`rate_limit`] | 07-08 | StartRun admission (HTTP 429 `rate_limited`, D-W21) |
//!
//! [`NodeFlow`] is the node's `FlowControl` (one per node, shared by every partition's
//! dispatcher). Plan 07-02 stub: it admits everything with weight 1 (`NoFlow` semantics)
//! and [`install`] leaves the dispatcher configuration unchanged.

pub mod limits;
pub mod policy_view;
pub mod rate_limit;

use std::sync::Arc;

use ostro_meta::MetaView;
use ostro_partition::{Admit, DispatcherConfig, EntryView, FlowControl, NoFlow, QueueGauges};
use tokio::sync::watch;

use crate::p7::P7Ctx;

/// The node's flow control.
pub struct NodeFlow {
    meta: MetaView,
    gauges: Arc<QueueGauges>,
    inner: NoFlow,
}

impl NodeFlow {
    /// Flow control over the meta policies, publishing into `gauges`.
    pub fn new(meta: MetaView, gauges: Arc<QueueGauges>) -> Self {
        Self {
            meta,
            gauges,
            inner: NoFlow::new(),
        }
    }

    /// The meta view the policies are read from.
    pub fn meta(&self) -> &MetaView {
        &self.meta
    }

    /// The node's queue gauges.
    pub fn gauges(&self) -> &Arc<QueueGauges> {
        &self.gauges
    }
}

impl std::fmt::Debug for NodeFlow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeFlow").finish_non_exhaustive()
    }
}

impl FlowControl for NodeFlow {
    fn admit(&self, e: &EntryView<'_>, now_ms: u64) -> Admit {
        self.inner.admit(e, now_ms)
    }

    fn queue_weight(&self, queue: &str) -> u32 {
        self.inner.queue_weight(queue)
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.inner.changed()
    }
}

/// Install the node's flow control into the dispatcher configuration.
///
/// Phase 7 stub (07-02): no-op (07-05 adds the `DispatcherConfig` field, 07-08 fills it).
pub fn install(ctx: &P7Ctx, cfg: &mut DispatcherConfig) {
    let _ = (ctx, cfg);
}
