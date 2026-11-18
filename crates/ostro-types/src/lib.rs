//! ostro-types: shared types for Ostro.
//!
//! The command envelope (log header, partition `Command` framing, fencing)
//! lands in Phase 2; `journal::Event` payload semantics land in Phase 3.

pub mod envelope;
pub mod ids;
pub mod journal;
pub mod payload;
pub mod routing;

pub use envelope::{EnvelopeError, EnvelopeHeader, EnvelopeView};
pub use ids::{AttemptToken, Epoch, IdError, Offset, PartitionCount, PartitionId, RunId};
pub use payload::{BlobRef, INLINE_PAYLOAD_LIMIT, PayloadRef};
pub use routing::{partition_for_key, partition_for_new_run};

/// Version of the on-disk / on-wire codec.
pub const CODEC_VERSION: u16 = 1;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codec_version_is_one() {
        assert_eq!(CODEC_VERSION, 1);
    }
}
