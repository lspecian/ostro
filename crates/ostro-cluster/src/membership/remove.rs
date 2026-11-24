//! Phase 13: implemented in 13-09 (quorum-safe member removal; D-P13-8).
//!
//! A voter may be removed only if the remaining live voters form a quorum of the new
//! configuration, even with `--force` (`--force` skips only waiting on the dead node).
//! Fixed signature; the stub refuses every removal.

/// Why a removal is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RemoveError {
    /// The remaining live voters would not form a quorum.
    #[error("removing node {id} would leave no live quorum")]
    NoQuorum { id: u64 },
    /// The node is unknown or already removed.
    #[error("node {id} is not a member")]
    NotMember { id: u64 },
    /// Not available before 13-09.
    #[error("Phase 13: implemented in 13-09")]
    NotImplemented,
}

/// Check that removing `id` from `voters` keeps a live quorum (`live` = live node ids).
/// Stub: `NotImplemented`.
pub fn check_remove(id: u64, voters: &[u64], live: &[u64], force: bool) -> Result<(), RemoveError> {
    let _ = (id, voters, live, force);
    Err(RemoveError::NotImplemented)
}
