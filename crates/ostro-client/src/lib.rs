//! ostro-client: the Rust REST client for an Ostro server and, behind the `testing`
//! feature, the spawned-server test kit.
//!
//! Skeleton (10-02). 10-09 moves the typed REST client here from `ostro-cli`; 10-17 fills
//! in [`testing`]. The crate is bundleable (10-08): its manifest uses explicit versions
//! only, so `ostro init` projects can depend on a copy outside this workspace.

/// Spawned-server test kit (10-17 replaces this stub).
#[cfg(feature = "testing")]
pub mod testing {}

// Plan 10-02: dependencies of 10-09 / 10-17, anchored until they are used.
use reqwest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use thiserror as _;
use tokio as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use axum as _;
    use tempfile as _;
}
