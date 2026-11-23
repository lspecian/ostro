//! Pending scenarios owned by 12-07: every RF>1 scenario of the standby, bootstrap, fetch
//! path, blobs, zombie and object-storage groups (they need the replicated cluster that
//! 12-14 wires; until then each fails the measured `cluster_formed` precondition), plus the
//! RF=1 `one_path_rf1_hw_through_tracker` (it needs the tracker's `hw_publishes` in
//! `GET /v1/admin/replication`, 12-14).

/// Scenario names not yet integrated (removed as their behaviour lands).
pub const PENDING: &[&str] = &[
    // standby
    "followers_are_hot_standbys",
    // bootstrap
    "empty_node_bootstraps_from_snapshot",
    "node_behind_log_start_reinstalls",
    "install_stream_cut_resumes",
    // fetch_path
    "slow_read_longer_than_max_wait_completes",
    "max_size_envelope_replicates",
    "batching_on_user_partitions",
    // blobs
    "blob_before_commit_survives_failover",
    // one_path
    "one_path_rf1_hw_through_tracker",
    // zombie
    "isolated_leader_cannot_commit",
    // object_storage
    "remote_chain_equals_committed_prefix",
    "remote_chain_never_above_hw",
];
