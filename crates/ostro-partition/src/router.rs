//! Cross-partition delivery (shuffler -> target partition).
//!
//! Single node: [`InProcessRouter`] submits the `InboundBatch` to the target's local
//! [`PartitionHandle`]. Phase 13 adds a network-forwarding implementation of the same trait.

use std::future::Future;
use std::pin::Pin;
use std::sync::RwLock;

use ostro_log::PartitionLog;
use ostro_types::command::InboundBatch;
use ostro_types::{Command, PartitionCount, PartitionId};

use crate::apply::ApplyOutcome;
use crate::handle::{PartitionError, PartitionHandle};

pub type RouteFuture<'a> =
    Pin<Box<dyn Future<Output = Result<ApplyOutcome, PartitionError>> + Send + 'a>>;

/// Delivers an inbound batch to the leader of `target`.
pub trait PartitionRouter: Send + Sync + 'static {
    fn deliver(&self, target: PartitionId, batch: InboundBatch) -> RouteFuture<'_>;
}

/// Routes to local partition handles registered once each partition is ready.
#[derive(Debug)]
pub struct InProcessRouter {
    slots: RwLock<Vec<Option<PartitionHandle<PartitionLog>>>>,
}

impl InProcessRouter {
    /// An empty router for `count` partitions (every target `NotLeader` until registered).
    pub fn new(count: PartitionCount) -> Self {
        Self {
            slots: RwLock::new(vec![None; usize::from(count.get())]),
        }
    }

    /// Make `handle.partition()` routable.
    pub fn register(&self, handle: PartitionHandle<PartitionLog>) {
        let i = handle.partition().index();
        let mut s = self.slots.write().unwrap_or_else(|p| p.into_inner());
        if let Some(slot) = s.get_mut(i) {
            *slot = Some(handle);
        }
    }

    /// Drop every registered handle (node shutdown).
    pub fn clear(&self) {
        let mut s = self.slots.write().unwrap_or_else(|p| p.into_inner());
        s.iter_mut().for_each(|h| *h = None);
    }

    fn get(&self, p: PartitionId) -> Option<PartitionHandle<PartitionLog>> {
        let s = self.slots.read().unwrap_or_else(|p| p.into_inner());
        s.get(p.index()).and_then(Clone::clone)
    }
}

impl PartitionRouter for InProcessRouter {
    fn deliver(&self, target: PartitionId, batch: InboundBatch) -> RouteFuture<'_> {
        let handle = self.get(target);
        Box::pin(async move {
            let h = handle.ok_or(PartitionError::NotLeader)?;
            if h.machine().check_healthy().is_err() {
                return Err(PartitionError::NotLeader);
            }
            h.submit_one(Command::inbound_batch(batch)).await
        })
    }
}
