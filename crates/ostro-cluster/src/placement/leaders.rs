//! Phase 13: implemented in 13-06 (pure preferred-leader balancing; D-P13-12).
//!
//! Fixed signature. Stub: no moves.

/// Partition → (replicas, current leader); returns at most `max_moves` `(partition, to)`
/// handoffs towards the preferred leader (first replica). Stub: empty.
pub fn preferred_leader_moves(
    partitions: &[(u16, Vec<u64>, u64)],
    live: &[u64],
    max_moves: usize,
) -> Vec<(u16, u64)> {
    let _ = (partitions, live, max_moves);
    Vec::new()
}

use std::collections::BTreeSet;

/// One partition as the leader balancer sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeaderObs {
    /// Partition.
    pub partition: u16,
    /// Replicas (the first is the preferred leader).
    pub replicas: Vec<u64>,
    /// Published ISR.
    pub isr: Vec<u64>,
    /// Current leader (0 = offline).
    pub leader: u64,
}

/// One preferred-leader handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaderMove {
    /// Partition.
    pub partition: u16,
    /// New leader.
    pub to: u64,
}

/// At most one preferred-leader move. Stub: `None`.
pub fn decide(parts: &[LeaderObs], live: &BTreeSet<u64>, members: usize) -> Option<LeaderMove> {
    let _ = (parts, live, members);
    None
}
