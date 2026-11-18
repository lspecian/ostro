//! ostro-log: partition journal.
//!
//! Phase 2 builds the CRC-verified envelope on top of the vendored WAL
//! (`ostro-wal`, a trimmed Chronik Stream group-commit WAL; see VENDORED.md).
//! For now the crate only re-exports it.

pub use ostro_wal;

// Dependencies wired in plan 02-01 ahead of their consuming plans (02-03/02-04).
// The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
use bytes as _;
use metrics as _;
use ostro_types as _;
use thiserror as _;
use tokio as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use proptest as _;
    use tempfile as _;
}
