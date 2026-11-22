//! `ostro build` (CLI-04): build a workflow project into a Wasm component.
//!
//! Plan 10-03 fixes the flag set; the implementation is plan 10-12 (this file is its
//! own, `commands/mod.rs` is not edited again).

use std::path::PathBuf;

use clap::{Args, ValueEnum};

use crate::output::Outcome;

/// Where the build toolchain comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ToolchainMode {
    /// Hermetic toolchain if installed, else the system one.
    Auto,
    /// Only the pinned toolchain under the ostro toolchain dir.
    Hermetic,
    /// Only tools found on PATH.
    System,
    /// Inside the `ostro-build` container image.
    Docker,
}

/// `ostro build [DIR] [--toolchain <auto|hermetic|system|docker>] [--docker] [--offline]
/// [--json]`.
#[derive(Debug, Clone, Args)]
pub struct BuildArgs {
    /// Project directory [default: .].
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// Toolchain selection.
    #[arg(long, value_enum, default_value = "auto")]
    pub toolchain: ToolchainMode,
    /// Shorthand for `--toolchain docker`.
    #[arg(long, conflicts_with = "toolchain")]
    pub docker: bool,
    /// Never download anything (fail if a toolchain is missing).
    #[arg(long)]
    pub offline: bool,
    /// Print an `ostro.build.result` JSON object.
    #[arg(long)]
    pub json: bool,
    /// Internal: running inside the build container.
    #[arg(long, hide = true)]
    pub in_container: bool,
}

impl BuildArgs {
    /// The effective toolchain mode (`--docker` wins).
    pub fn mode(&self) -> ToolchainMode {
        if self.docker {
            ToolchainMode::Docker
        } else {
            self.toolchain
        }
    }
}

/// Run `ostro build` (stub until 10-12).
pub fn run(_a: &BuildArgs) -> Outcome {
    eprintln!("ostro: error: build is not yet implemented (10-12)");
    Outcome::Usage
}
