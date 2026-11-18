//! Where a leadership term's epoch comes from (STOR-10, research Pattern 7).
//!
//! Every term begins with `LeaderAnnounced{epoch}` where
//! `epoch = max(source floor, latest log epoch + 1)`, so a restart is always a new term
//! and fences every token granted before it. Phase 4 replaces [`LocalLeadership`] with the
//! metadata partition map (which supplies a floor from the cluster's view).

use ostro_types::{Epoch, PartitionId};

/// Supplies the lowest epoch a new term on partition `p` may use.
pub trait LeadershipSource: Send + Sync + 'static {
    fn epoch_floor(&self, p: PartitionId) -> Epoch;
}

/// Single node: no external floor (`Epoch(0)`); the log's epoch history decides.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalLeadership;

impl LeadershipSource for LocalLeadership {
    fn epoch_floor(&self, _p: PartitionId) -> Epoch {
        Epoch(0)
    }
}

/// `max(source_floor, log_latest + 1)`, at least 1.
pub fn next_term_epoch(source_floor: Epoch, log_latest: Option<Epoch>) -> Epoch {
    let after_log = log_latest.map_or(1, |e| e.0.saturating_add(1));
    Epoch(source_floor.0.max(after_log).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_term_epoch_rules() {
        assert_eq!(next_term_epoch(Epoch(0), None), Epoch(1));
        assert_eq!(next_term_epoch(Epoch(0), Some(Epoch(0))), Epoch(1));
        assert_eq!(next_term_epoch(Epoch(0), Some(Epoch(4))), Epoch(5));
        assert_eq!(next_term_epoch(Epoch(10), Some(Epoch(2))), Epoch(10));
        assert_eq!(next_term_epoch(Epoch(3), Some(Epoch(7))), Epoch(8));
        assert_eq!(LocalLeadership.epoch_floor(PartitionId(3)), Epoch(0));
    }
}
