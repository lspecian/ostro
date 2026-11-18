//! High-watermark computation shared by RF=1 and the Phase 12 cluster (D5: one code path).
//!
//! The HW is the exclusive end of the prefix every in-sync replica holds durably. At RF=1 the
//! only replica is the local node and its durable LEO comes from the WAL commit callback (after
//! fsync), so `compute_hw(&[self])` is that LEO. Phase 12 passes one entry per replica.

use ostro_types::Offset;

/// One replica's durable progress.
#[derive(Copy, Clone, Debug)]
pub struct ReplicaProgress {
    /// Node id.
    pub node: u64,
    /// Exclusive end of the replica's durable log.
    pub leo: Offset,
    /// Whether the replica is in the ISR.
    pub in_isr: bool,
}

/// Minimum `leo` over the in-ISR replicas; out-of-ISR replicas are ignored.
///
/// # Panics
/// If no replica is in the ISR: the leader is always in its own ISR, so an empty ISR is a bug
/// in the caller, and answering anything would publish an HW nobody holds.
pub fn compute_hw(progress: &[ReplicaProgress]) -> Offset {
    progress
        .iter()
        .filter(|p| p.in_isr)
        .map(|p| p.leo)
        .min()
        .expect("compute_hw: no replica in the ISR")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rp(node: u64, leo: u64, in_isr: bool) -> ReplicaProgress {
        ReplicaProgress {
            node,
            leo: Offset(leo),
            in_isr,
        }
    }

    #[test]
    fn single_replica_is_its_own_leo() {
        assert_eq!(compute_hw(&[rp(1, 42, true)]), Offset(42));
    }

    #[test]
    fn min_over_isr_ignoring_out_of_sync() {
        let p = [rp(1, 100, true), rp(2, 70, true), rp(3, 5, false)];
        assert_eq!(compute_hw(&p), Offset(70));
    }

    #[test]
    #[should_panic(expected = "no replica in the ISR")]
    fn empty_isr_panics() {
        compute_hw(&[rp(1, 10, false)]);
    }
}
