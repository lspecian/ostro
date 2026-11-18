//! ostro-types: shared types for Ostro.
//!
//! The command envelope (log header, partition `Command` framing, fencing)
//! lands in Phase 2; `journal::Event` payload semantics land in Phase 3.

pub mod journal;

// Phase 2 dependencies wired in plan 02-01 ahead of the envelope/command code
// (plan 02-03). The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
use bytes as _;
use crc32c as _;
use prost as _;
use thiserror as _;
use uuid as _;
use xxhash_rust as _;

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
