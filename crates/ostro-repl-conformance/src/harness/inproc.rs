//! Phase 12: implemented in 12-03 (tier-B in-process cluster: N `ostro-server` instances
//! over the in-memory transport, implementing [`ClusterHarness`](super::ClusterHarness)).

/// The tier-B cluster (fields and the `ClusterHarness` impl land in 12-03).
#[derive(Debug)]
#[non_exhaustive]
pub struct InProcCluster {}
