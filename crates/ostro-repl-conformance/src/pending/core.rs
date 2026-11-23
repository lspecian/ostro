//! Pending scenarios owned by 12-06: every RF>1 core scenario (placement, ISR honesty, HW,
//! stale epoch, divergence, failover, min_insync). They need the replicated cluster that
//! 12-14 wires; until then each fails the measured `cluster_formed` precondition. The RF=1
//! `placement_rf1_single_node` is not pending: it passes on the current build.

/// Scenario names not yet integrated (removed as their behaviour lands). 12-14 removed every
/// entry its first integrated red run saw pass; 12-16 the rest.
pub const PENDING: &[&str] = &[];
