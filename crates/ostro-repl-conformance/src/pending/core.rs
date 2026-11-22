//! Pending scenarios owned by 12-06: every RF>1 core scenario (placement, ISR honesty, HW,
//! stale epoch, divergence, failover, min_insync). They need the replicated cluster that
//! 12-14 wires; until then each fails the measured `cluster_formed` precondition. The RF=1
//! `placement_rf1_single_node` is not pending: it passes on the current build.

/// Scenario names not yet integrated (removed as their behaviour lands).
pub const PENDING: &[&str] = &[
    // placement
    "placement_rf3_three_replicas",
    // isr_honesty
    "isr_empty_follower_not_in_isr",
    "isr_lagging_follower_rejoins",
    "isr_slow_follower_shrinks",
    // hw
    "hw_is_min_over_isr",
    "hw_rf3_commits_within_rounds",
    // stale_epoch
    "fetch_with_stale_epoch_fenced",
    "fetch_with_future_epoch_unknown",
];
