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
