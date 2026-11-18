//! ostro-partition: per-partition state store on fjall 3.1.13.
//!
//! fjall replaced redb after spike S3 (see docs/spikes/S3-redb-durability.md
//! and PROJECT.md Key Decisions). This module is a skeleton; plan 02-05
//! replaces it with the real store.

use std::path::Path;

// Dependencies wired in plan 02-01 ahead of their consuming plans (02-03..02-08).
// The anchors keep cargo's per-crate `unused_dependencies` lint quiet.
use blake3 as _;
use bytes as _;
#[cfg(feature = "failpoints")]
use fail as _;
use metrics as _;
use ostro_blob as _;
use ostro_log as _;
use ostro_types as _;
use postcard as _;
use serde as _;
use thiserror as _;
use tokio as _;
use tracing as _;

/// Open (or create) a fjall database at `path` with Ostro's smoke settings:
/// one background worker and the minimum 64 MiB journal.
pub fn open_smoke(path: &Path) -> fjall::Result<fjall::Database> {
    fjall::Database::builder(path)
        .worker_threads(1)
        .max_journaling_size(64 << 20)
        .open()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fjall::{KeyspaceCreateOptions, PersistMode};
    use proptest as _;
    use proptest_state_machine as _;

    #[test]
    fn batch_across_keyspaces_survives_reopen() -> Result<(), Box<dyn std::error::Error>> {
        let dir = tempfile::tempdir()?;
        {
            let db = open_smoke(dir.path())?;
            let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
            let runs = db.keyspace("runs", KeyspaceCreateOptions::default)?;
            let mut b = db.batch();
            b.insert(&meta, "applied_offset", 42u64.to_le_bytes());
            b.insert(&runs, "run-1", "started");
            b.commit()?;
            db.persist(PersistMode::SyncAll)?;
        }
        let db = open_smoke(dir.path())?;
        let meta = db.keyspace("meta", KeyspaceCreateOptions::default)?;
        let runs = db.keyspace("runs", KeyspaceCreateOptions::default)?;
        let off = meta.get("applied_offset")?.expect("applied_offset");
        assert_eq!(&*off, &42u64.to_le_bytes());
        let run = runs.get("run-1")?.expect("run-1");
        assert_eq!(&*run, b"started");
        Ok(())
    }
}
