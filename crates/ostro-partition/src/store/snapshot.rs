//! Phase 8: implemented in 08-02.
//!
//! Logical snapshot export/import in the OSNP v1 format (P8-D1; docs/snapshot-format.md,
//! created by 08-02): one fjall `Database::snapshot()` exported as CRC32C frames, installed
//! through a staged `state/.install-pNNNN` directory.
