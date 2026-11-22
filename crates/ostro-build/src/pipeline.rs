//! The build pipeline (`ostro build`): load the project, stage, run the language step,
//! validate and describe, write `target/ostro/<name>.wasm`, `manifest.json` and
//! `inputs.sha256`; skip the step when the input hash is unchanged ("up to date").
//!
//! Owner: 10-12 (stub from 10-02: the contract types, [`IGNORE`] and a [`build`] that
//! returns [`BuildError::NotImplemented`]).

use std::path::PathBuf;

use crate::project::ToolchainMode;

/// Paths never staged, hashed or watched (shared with `ostro dev --watch`, D-P10-14 / W7).
/// Entries ending in `/` are directory names at any depth; the rest are file-name patterns
/// (`*` prefix = suffix match, `*` suffix = prefix match).
pub const IGNORE: &[&str] = &[
    "target/",
    "node_modules/",
    "__pycache__/",
    ".ostro/",
    ".git/",
    ".venv/",
    "venv/",
    ".tox/",
    ".mypy_cache/",
    "*~",
    ".#*",
    "*.swp",
    "4913",
];

/// Options of one `ostro build`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BuildOptions {
    /// Project directory (holds `ostro.toml` or a detectable project).
    pub dir: PathBuf,
    /// `--toolchain`; `None` = `[build] toolchain`, else [`ToolchainMode::Auto`].
    pub toolchain: Option<ToolchainMode>,
    /// `--docker` (Docker mode).
    pub docker: bool,
    /// `--offline` / `OSTRO_OFFLINE`: never download.
    pub offline: bool,
    /// `--json` output.
    pub json: bool,
}

/// Result of a successful build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildOutput {
    /// `target/ostro/<name>.wasm`.
    pub wasm_path: PathBuf,
    /// Lower-case hex sha256 of the component.
    pub sha256: String,
    /// Component size in bytes.
    pub bytes: u64,
    /// Exported workflows and their versions (from `Runtime::describe`).
    pub workflows: Vec<(String, u32)>,
    /// The input hash was unchanged and the language step did not run.
    pub up_to_date: bool,
    /// `target/ostro/manifest.json`.
    pub manifest_path: PathBuf,
}

/// Why a build failed.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum BuildError {
    /// The named plan has not landed yet.
    #[error("not yet implemented ({0})")]
    NotImplemented(&'static str),
}

/// Builds the project in `opts.dir`.
pub fn build(opts: BuildOptions) -> Result<BuildOutput, BuildError> {
    let _ = opts;
    Err(BuildError::NotImplemented("10-12"))
}
