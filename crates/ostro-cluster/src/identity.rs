//! Phase 12: implemented in 12-14 (cluster-id resolution and meta voter checks, D-P12-14 /
//! D-P12-16). All of it runs before any partition data is opened.

/// Where the bound cluster id comes from, in D-P12-14 source order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterIdSource {
    /// `[cluster] cluster_id`.
    Configured(String),
    /// `node.toml`.
    NodeToml(String),
    /// The existing CA certificate's subject CN (`ostro cluster <id>`).
    StoredCa(String),
    /// `ClusterInfo.cluster_id` of a non-blank persisted meta.
    Meta(String),
    /// Nothing present and meta is blank: generate a fresh id (single node only).
    GenerateFresh,
}

/// The voters and learners of a persisted meta Raft `ConfState`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConfStateView {
    /// Voter node ids.
    pub voters: Vec<u64>,
    /// Learner node ids.
    pub learners: Vec<u64>,
}

/// A fatal identity / bootstrap check failure (message names the remedy).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BootError {
    /// Not implemented yet.
    #[error("Phase 12: implemented in 12-14")]
    NotImplemented,
}

/// Resolve the cluster id: configured → node.toml → stored CA → persisted meta → fresh
/// (only when meta is blank). Two present sources that disagree are fatal.
pub fn resolve_cluster_id(
    configured: Option<&str>,
    node_toml: Option<&str>,
    stored_ca: Option<&str>,
    persisted_cluster_info: Option<&str>,
    meta_blank: bool,
) -> Result<ClusterIdSource, BootError> {
    let _ = (
        configured,
        node_toml,
        stored_ca,
        persisted_cluster_info,
        meta_blank,
    );
    Err(BootError::NotImplemented)
}

/// A blank meta over existing partition data is a joiner / wiped voter (fail-stop).
pub fn is_joiner(meta_blank: bool, partition_data_exists: bool) -> bool {
    meta_blank && partition_data_exists
}

/// A non-blank meta whose voters differ from `expected` (self ∪ configured peers) is fatal
/// ("grow via Phase 13"). `None` = blank meta.
pub fn check_meta_voters(
    persisted: Option<ConfStateView>,
    expected: &[u64],
) -> Result<(), BootError> {
    let _ = (persisted, expected);
    Err(BootError::NotImplemented)
}
