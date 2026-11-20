//! ostro-backup: bottomless storage, backup files and restore (Phase 8, BACK-01..04).
//!
//! Every Phase 8 module is declared here once (plan 08-01, P8-D21) so later plans own disjoint
//! files and never edit this file:
//!
//! | module | owner | contents |
//! |--------|-------|----------|
//! | [`config`] | 08-01 | [`config::BackupConfig`] contract (parsed from `[backup]` by 08-04) |
//! | [`chunk`] | 08-13 | OSEG v1 uploader-sealed, hash-chained chunks (P8-D3) |
//! | [`manifest`] | 08-13 | `manifest/current` + `manifest/gen/<gen>`, `Manifest::validate` |
//! | [`fence`] | 08-13 | `LOCK` object + conditional manifest PUT (P8-D4) |
//! | [`uploader`] | 08-13 | node-level uploader driven by the consistent cut |
//! | [`restore`] | 08-18 | `ostro restore` (P8-D6) |
//! | [`archive`] | 08-18 | backup tar files (P8-D7) |
//! | [`remote_gc`] | 08-19 | remote delayed-deletion blob GC (P8-D8) |

pub mod archive;
pub mod chunk;
pub mod config;
pub mod fence;
pub mod manifest;
pub mod remote_gc;
pub mod restore;
pub mod uploader;

pub use config::{BackupConfig, TlsRoots, UrlStyle};

// Dependencies declared by plan 08-01 for the modules above (P8-D21), anchored so cargo's
// unused-dependency lint stays quiet until the owning plans consume them.
mod dep_anchors {
    #[cfg(feature = "failpoints")]
    use fail as _;
    use metrics as _;
    use ostro_blob as _;
    use ostro_log as _;
    use ostro_meta as _;
    use ostro_objstore as _;
    use ostro_partition as _;
    use ostro_types as _;
    use postcard as _;
    use serde as _;
    use serde_json as _;
    use sha2 as _;
    use tar as _;
    use thiserror as _;
    use tokio as _;
    use tracing as _;
}
