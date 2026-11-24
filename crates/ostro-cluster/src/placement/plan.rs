//! Phase 13: implemented in 13-06 (pure target replica sets).
//!
//! Fixed signature. Stub: no targets (nothing moves).

use std::collections::BTreeMap;

/// Target replica set per partition for `members` (Active, non-Removed ids) at replication
/// factor `rf` (effective RF = min(rf, members)), starting from `current`. Stub: empty.
pub fn target_replicas(
    current: &BTreeMap<u16, Vec<u64>>,
    members: &[u64],
    rf: u8,
) -> BTreeMap<u16, Vec<u64>> {
    let _ = (current, members, rf);
    BTreeMap::new()
}
