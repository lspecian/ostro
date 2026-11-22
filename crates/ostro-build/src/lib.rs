//! ostro-build: `ostro build` / `ostro init` (Phase 10, CLI-03/CLI-04).
//!
//! Module owners (each file was created as a stub by 10-02):
//! - [`cache`], [`fetch`], [`extract`], [`toolchain`]: 10-07 (pinned, sha256-verified
//!   toolchain downloads into a content-addressed cache);
//! - [`sdk_bundle`], [`project`], [`templates`]: 10-08 (embedded SDKs, `ostro.toml`,
//!   `ostro init`);
//! - [`pipeline`], [`stage`], [`manifest`], [`validate`], [`lang::rust`]: 10-12 (the build
//!   pipeline);
//! - [`lang::python`], [`lang::ts`], [`lang::docker`]: 10-15.

pub mod cache;
pub mod extract;
pub mod fetch;
pub mod lang;
pub mod manifest;
pub mod pipeline;
pub mod project;
pub mod sdk_bundle;
pub mod stage;
pub mod templates;
pub mod toolchain;
pub mod validate;

pub use pipeline::{BuildError, BuildOptions, BuildOutput, build};
pub use project::{Lang, ToolchainMode};
pub use toolchain::{Tool, ToolchainSet};

// Plan 10-02: dependencies of 10-07 / 10-08 / 10-12 / 10-15, anchored until they are used
// (thiserror is used by `BuildError`).
use flate2 as _;
use lzma_rust2 as _;
use ostro_engine as _;
use reqwest as _;
use serde as _;
use serde_json as _;
use sha2 as _;
use tar as _;
use tempfile as _;
use tokio as _;
use toml as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use axum as _;
    use futures_util as _;
    use insta as _;
}
