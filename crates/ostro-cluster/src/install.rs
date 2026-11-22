//! Phase 12: implemented in 12-12 (follower snapshot install, D-P12-8: stream to
//! `.incoming-*`, verify, quiesce, `reset_to`, `install_snapshot`, reopen).
//!
//! The reopen after an install must go through the same derived-keyspace rebuild hook as
//! a normal start (`PartitionStore::start_run_index_rebuild`), see
//! `ostro_partition::roles::InstallGuard::new`.

use ostro_types::PartitionId;

/// Everything an install needs (shape owned by 12-12).
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct InstallCtx {}

/// What an install did (shape owned by 12-12).
#[derive(Debug)]
#[non_exhaustive]
pub enum InstallOutcome {
    /// The snapshot is installed; fetch resumes from the new log start.
    Installed,
}

/// Install failure (shape owned by 12-12).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum InstallError {
    /// Not implemented yet.
    #[error("Phase 12: implemented in 12-12")]
    NotImplemented,
}

/// Fetch the leader's newest snapshot for `p` and install it.
pub async fn install_from_leader(
    ctx: &InstallCtx,
    p: PartitionId,
    leader: u64,
) -> Result<InstallOutcome, InstallError> {
    let _ = (ctx, p, leader);
    Err(InstallError::NotImplemented)
}
