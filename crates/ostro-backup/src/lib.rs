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
//! | [`cluster`] | 12-15 | Phase 12 per-partition leader uploads, head fencing, takeover reconcile (D-P12-20) |

pub mod archive;
pub mod chunk;
pub mod cluster;
pub mod config;
pub mod fence;
pub mod manifest;
pub mod remote_gc;
pub mod restore;
pub mod uploader;

pub use config::{BackupConfig, TlsRoots, UrlStyle};

// Dependencies declared by plan 08-01 for modules owned by later plans (08-18 restore/archive,
// 08-19 remote GC), anchored so cargo's unused-dependency lint stays quiet until they land.
mod dep_anchors {
    use tar as _;
}
