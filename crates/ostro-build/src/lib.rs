//! ostro-build: `ostro build` / `ostro init` (Phase 10).

// Plan 10-02: dependencies of 10-07 / 10-08 / 10-12 / 10-15, anchored until they are used.
use flate2 as _;
use lzma_rust2 as _;
use ostro_engine as _;
use reqwest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use tar as _;
use tempfile as _;
use thiserror as _;
use tokio as _;
use toml as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use axum as _;
    use insta as _;
}
