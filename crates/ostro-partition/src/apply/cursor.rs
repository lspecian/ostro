//! The apply cursor: next offset, rolling hash, fencing epoch, outbox
//! sequence counters and inbound dedupe watermarks.

use std::collections::{BTreeMap, BTreeSet};

use ostro_types::{Epoch, Offset, PartitionCount, PartitionId};

use crate::store::values::CursorMeta;
use crate::store::{MetaUpdate, StoreMeta};

#[derive(Clone, Debug)]
pub struct Cursor {
    pub partition: PartitionId,
    pub count: PartitionCount,
    /// Next offset to apply (persisted as `applied_offset`).
    pub next: Offset,
    /// Rolling hash over every applied command (see `apply::fold_hash`).
    pub hash: [u8; 32],
    pub meta: CursorMeta,
    /// Highest outbox seq applied per source partition.
    pub inbound_seen: BTreeMap<u16, u64>,
    inbound_changed: BTreeSet<u16>,
}

impl Cursor {
    pub fn from_meta(m: &StoreMeta) -> Self {
        Self {
            partition: m.partition,
            count: m.count,
            next: m.applied,
            hash: m.state_hash,
            meta: m.cursor.clone(),
            inbound_seen: m.inbound_seen.clone(),
            inbound_changed: BTreeSet::new(),
        }
    }

    pub fn epoch(&self) -> Epoch {
        Epoch(self.meta.epoch)
    }

    /// Records the highest seq applied from `src` (written with the next
    /// [`meta_update`](Self::meta_update)).
    #[allow(dead_code, reason = "consumed by InboundBatch apply (plan 02-10)")]
    pub(crate) fn set_inbound_seen(&mut self, src: u16, seq: u64) {
        self.inbound_seen.insert(src, seq);
        self.inbound_changed.insert(src);
    }

    /// Meta for the batch being committed. Drains the changed
    /// `inbound_seen` sources.
    pub fn meta_update(&mut self) -> MetaUpdate {
        let changed = std::mem::take(&mut self.inbound_changed);
        let inbound_seen_changed = changed
            .into_iter()
            .filter_map(|src| self.inbound_seen.get(&src).map(|&seq| (src, seq)))
            .collect();
        MetaUpdate {
            applied: self.next,
            state_hash: self.hash,
            cursor: self.meta.clone(),
            inbound_seen_changed,
        }
    }
}
