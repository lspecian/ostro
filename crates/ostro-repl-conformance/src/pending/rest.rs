//! Pending scenarios owned by 12-07: every RF>1 scenario of the standby, bootstrap, fetch
//! path, blobs, zombie and object-storage groups (they need the replicated cluster that
//! 12-14 wires; until then each fails the measured `cluster_formed` precondition), plus the
//! RF=1 `one_path_rf1_hw_through_tracker` (it needs the tracker's `hw_publishes` in
//! `GET /v1/admin/replication`, 12-14).

/// Scenario names not yet integrated (removed as their behaviour lands). 12-14 removed every
/// entry its first integrated red run saw pass; the rest is 12-16's work list.
pub const PENDING: &[&str] = &[
    // bootstrap
    "empty_node_bootstraps_from_snapshot",
    "node_behind_log_start_reinstalls",
    "install_stream_cut_resumes",
    // fetch_path
    "batching_on_user_partitions",
    // zombie
    "isolated_leader_cannot_commit",
];
