//! ostro-types: shared types for Ostro.
//!
//! The command envelope (log header, partition `Command` framing, fencing)
//! lands in Phase 2; `journal::Event` payload semantics land in Phase 3.

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
