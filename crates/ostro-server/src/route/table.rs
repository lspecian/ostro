//! Phase 13: implemented in 13-12 (route classes per method, the D-P13-14 table recorded in
//! 13-01-SUMMARY, and the partition of a call).
//!
//! Fixed signature: [`pinned_partition`], read by the start path (`control/runs.rs`).

use ostro_types::PartitionId;

/// The partition a forwarded unkeyed start is pinned to (`ostro-partition`, honoured only
/// from node identities; `RequestMeta::partition_pin`). Stub: never pinned.
pub fn pinned_partition() -> Option<PartitionId> {
    None
}
