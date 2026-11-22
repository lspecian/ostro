//! `ostro toolchain install|list|prune` and `ostro doctor` (CLI-08): the hermetic
//! build toolchains and an environment check.
//!
//! Plan 10-03 fixes the flag set; the implementation is plan 10-07 (this file is its
//! own, `commands/mod.rs` is not edited again).

use clap::{ArgGroup, Args, Subcommand, ValueEnum};

use crate::output::Outcome;

/// A toolchain language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ToolchainLang {
    Rust,
    Python,
    Ts,
}

/// `ostro toolchain <install|list|prune>`.
#[derive(Debug, Clone, Args)]
pub struct ToolchainArgs {
    #[command(subcommand)]
    pub command: ToolchainCommand,
}

/// The toolchain subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum ToolchainCommand {
    /// Install the pinned toolchain for one language or all of them.
    Install(InstallArgs),
    /// List installed toolchains.
    List(ListArgs),
    /// Remove toolchains that are no longer pinned.
    Prune(PruneArgs),
}

/// `ostro toolchain install [--lang L | --all] [--json]`.
#[derive(Debug, Clone, Args)]
#[command(group(ArgGroup::new("which").args(["lang", "all"]).multiple(false)))]
pub struct InstallArgs {
    /// Language whose toolchain to install.
    #[arg(long, value_enum)]
    pub lang: Option<ToolchainLang>,
    /// Install every language's toolchain.
    #[arg(long)]
    pub all: bool,
    /// Print an `ostro.toolchain.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// `ostro toolchain list [--json]`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Print an `ostro.toolchain.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// `ostro toolchain prune [--state] [--json]`.
#[derive(Debug, Clone, Args)]
pub struct PruneArgs {
    /// Also remove cached state (downloads, build caches).
    #[arg(long)]
    pub state: bool,
    /// Print an `ostro.toolchain.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// `ostro doctor [--json]`.
#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
    /// Print an `ostro.doctor.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// Run `ostro toolchain ...` (stub until 10-07).
pub fn run(_a: &ToolchainArgs) -> Outcome {
    eprintln!("ostro: error: toolchain is not yet implemented (10-07)");
    Outcome::Usage
}

/// Run `ostro doctor` (stub until 10-07).
pub fn doctor(_a: &DoctorArgs) -> Outcome {
    eprintln!("ostro: error: doctor is not yet implemented (10-07)");
    Outcome::Usage
}
