//! [`MetaLeadership`]: the metadata partition map as the epoch floor source (META-03).
//!
//! Every partition term begins at `next_term_epoch(floor, log latest)`, i.e.
//! `max(floor, log latest + 1)`. With the floor read from meta, a partition whose log was
//! lost (or restored from an older copy) still starts at or above the epoch the cluster
//! last recorded, so a stale leader epoch can never be re-used. After the term starts the
//! server records it with `SetPartitionLeader`, which meta rejects when it is lower than
//! the stored epoch.

use ostro_meta::MetaView;
use ostro_partition::LeadershipSource;
use ostro_types::{Epoch, PartitionId};

/// Reads partition epochs from the metadata state machine.
#[derive(Clone, Debug)]
pub struct MetaLeadership {
    view: MetaView,
}

impl MetaLeadership {
    pub fn new(view: MetaView) -> Self {
        Self { view }
    }
}

impl LeadershipSource for MetaLeadership {
    /// The epoch stored for `p` in meta (`Epoch(0)` when absent).
    fn epoch_floor(&self, p: PartitionId) -> Epoch {
        Epoch(
            self.view
                .read(|s| s.partition(p.0).map(|a| a.value.epoch))
                .unwrap_or(0),
        )
    }
}
