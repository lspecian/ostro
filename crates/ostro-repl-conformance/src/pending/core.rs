//! Pending scenarios owned by 12-06: every RF>1 core scenario (placement, ISR honesty, HW,
//! stale epoch, divergence, failover, min_insync). They need the replicated cluster that
//! 12-14 wires; until then each fails the measured `cluster_formed` precondition. The RF=1
//! `placement_rf1_single_node` is not pending: it passes on the current build.

/// Scenario names not yet integrated (removed as their behaviour lands). 12-14 removed every
/// entry its first integrated red run saw pass; the rest is 12-16's work list.
pub const PENDING: &[&str] = &[
    // isr_honesty
    "isr_slow_follower_shrinks",
    // divergence (leader_dies_with_unreplicated_writes passed the 12-14 red run but failed
    // the next suite run: kept pending until 12-16 makes it deterministic)
    "leader_dies_with_unreplicated_writes",
    "isolated_leader_rejoin_truncates",
    "two_quick_leader_changes_truncate",
];
