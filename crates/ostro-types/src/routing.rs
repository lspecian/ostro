//! Frozen routing hash (STOR-09). Pinned by `tests/golden/routing.txt`.
//!
//! `xxh3_64` is stable across Rust versions and platforms (unlike `DefaultHasher`).

use xxhash_rust::xxh3::{Xxh3, xxh3_64};

use crate::ids::{PartitionCount, PartitionId};

/// Partition for a keyed submission: `xxh3_64(u32 LE len(workflow) ‖ workflow ‖ idem_key) % P`.
pub fn partition_for_key(workflow: &str, idem_key: &[u8], p: PartitionCount) -> PartitionId {
    let len = u32::try_from(workflow.len()).expect("workflow name exceeds u32::MAX bytes");
    let mut h = Xxh3::new();
    h.update(&len.to_le_bytes());
    h.update(workflow.as_bytes());
    h.update(idem_key);
    reduce(h.digest(), p)
}

/// Partition for an unkeyed submission: `xxh3_64(uuid bytes) % P`. Any partition would do because
/// the run id embeds it; hashing the uuid spreads load evenly.
pub fn partition_for_new_run(uuid: &uuid::Uuid, p: PartitionCount) -> PartitionId {
    reduce(xxh3_64(uuid.as_bytes()), p)
}

fn reduce(h: u64, p: PartitionCount) -> PartitionId {
    // Result < P <= u16::MAX, so the narrowing is lossless.
    PartitionId((h % u64::from(p.get())) as u16)
}
