//! ostro-log: partition journal.
//!
//! Phase 2 builds the CRC-verified envelope on top of the vendored WAL
//! (`ostro-wal`, a trimmed Chronik Stream group-commit WAL; see VENDORED.md).
//! For now the crate only re-exports it.

pub use ostro_wal;
