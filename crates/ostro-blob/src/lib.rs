//! ostro-blob: placeholder. The content-addressed blob store lands in Phase 2.

// Dependencies wired in plan 02-01 ahead of their consuming plan.
// The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
#[cfg(feature = "failpoints")]
use fail as _;
use sha2 as _;
use thiserror as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use tempfile as _;
}
