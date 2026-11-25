//! Quorum-safe member removal (13-09, D-P13-8).
//!
//! A voter may be removed only if the live voters keep a quorum of both the current
//! configuration and the configuration without it (the joint change needs both halves),
//! even with `--force`: `--force` only skips waiting for the dead node to hand off its
//! replicas (plan check W7); it never skips this check (Pitfall 5).

use std::collections::BTreeSet;

/// Why a removal is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RemoveError {
    /// The remaining live voters would not form a quorum.
    #[error("removing node {id} would leave no live quorum")]
    NoQuorum { id: u64 },
    /// The node is unknown or already removed.
    #[error("node {id} is not a member")]
    NotMember { id: u64 },
    /// Not available before 13-09 (kept for the 13-01 contract; never returned now).
    #[error("Phase 13: implemented in 13-09")]
    NotImplemented,
}

/// Majority of `n` voters.
pub fn quorum(n: usize) -> usize {
    n / 2 + 1
}

/// Whether `live` holds a quorum of `voters` (an empty set has none).
pub fn has_live_quorum(voters: &BTreeSet<u64>, live: &BTreeSet<u64>) -> bool {
    !voters.is_empty() && voters.intersection(live).count() >= quorum(voters.len())
}

/// Check that removing `id` from `voters` keeps a live quorum (`live` = live node ids). A
/// non-voter (learner, not in the configuration) can always be removed. `force` does not
/// relax the check (it only lets the caller skip waiting for a dead node's replicas).
pub fn check_remove(id: u64, voters: &[u64], live: &[u64], force: bool) -> Result<(), RemoveError> {
    let _ = force;
    let voters: BTreeSet<u64> = voters.iter().copied().collect();
    if !voters.contains(&id) {
        return Ok(());
    }
    let live: BTreeSet<u64> = live.iter().copied().collect();
    let mut after = voters.clone();
    after.remove(&id);
    if has_live_quorum(&voters, &live) && has_live_quorum(&after, &live) {
        Ok(())
    } else {
        Err(RemoveError::NoQuorum { id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_rules() {
        // A learner or unknown id: always.
        assert_eq!(check_remove(9, &[1, 2, 3], &[1], false), Ok(()));
        // Dead voter of three, two live: {1,2} keeps 2/2.
        assert_eq!(check_remove(3, &[1, 2, 3], &[1, 2], true), Ok(()));
        // Two dead of three: refused, with or without force.
        for force in [false, true] {
            assert_eq!(
                check_remove(3, &[1, 2, 3], &[1], force),
                Err(RemoveError::NoQuorum { id: 3 })
            );
        }
        // Five voters, two dead: removing a dead one leaves {1,2,3,4} with 3 live of 4.
        assert_eq!(check_remove(5, &[1, 2, 3, 4, 5], &[1, 2, 3], true), Ok(()));
        // Five voters, three dead: refused.
        assert!(check_remove(5, &[1, 2, 3, 4, 5], &[1, 2], true).is_err());
        // The last voter: never.
        assert!(check_remove(1, &[1], &[1], true).is_err());
    }
}
