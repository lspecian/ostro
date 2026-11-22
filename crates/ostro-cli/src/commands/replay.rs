//! `ostro replay` (CLI-07, D-P10-7): replay recorded runs against a (new) module and
//! report divergence (exit 4).
//!
//! Selectors (exactly one): run ids (positional `RUN...` merged with `--run`),
//! `--sample N [--workflow W] [--status S]`, or `--corpus DIR` (offline, 05-12 entries).
//! `--module` is required unless `--corpus` is given.
//!
//! Plan 10-03 fixes the flag set; the implementation is plan 10-11 (this file is its
//! own, `commands/mod.rs` is not edited again). [`run`] is called outside any async
//! runtime, so it may use [`super::block_on`].

use std::path::PathBuf;

use clap::{ArgGroup, Args};

use super::{Conn, StatusArg};
use crate::output::Outcome;

/// `ostro replay [RUN...] [--run ID]... | --sample N [--workflow W] [--status S] |
/// --corpus DIR, [--module PATH] [--allow-missing]` plus the connection flags.
#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("selector")
        .args(["runs", "run", "sample", "corpus"])
        .required(true)
        .multiple(true)
))]
pub struct ReplayArgs {
    /// Run ids to replay (merged with `--run`).
    #[arg(value_name = "RUN", conflicts_with_all = ["sample", "corpus"])]
    pub runs: Vec<String>,
    /// A run id to replay (repeatable; merged with the positional ids).
    #[arg(long, value_name = "ID", conflicts_with_all = ["sample", "corpus"])]
    pub run: Vec<String>,
    /// Replay the newest N runs of each workflow the module describes.
    #[arg(long, value_name = "N", conflicts_with = "corpus")]
    pub sample: Option<u32>,
    /// With --sample: only this workflow.
    // `requires` alone is waived by clap when a conflicting selector is present, so the
    // other selectors are listed as conflicts too.
    #[arg(long, requires = "sample", conflicts_with_all = ["runs", "run", "corpus"])]
    pub workflow: Option<String>,
    /// With --sample: only runs in this status.
    #[arg(
        long,
        value_enum,
        requires = "sample",
        conflicts_with_all = ["runs", "run", "corpus"]
    )]
    pub status: Option<StatusArg>,
    /// Replay a 05-12 replay corpus directory offline (no server).
    #[arg(long, value_name = "DIR")]
    pub corpus: Option<PathBuf>,
    /// The Wasm component to replay with (required unless --corpus).
    #[arg(long, value_name = "PATH", required_unless_present = "corpus")]
    pub module: Option<PathBuf>,
    /// Do not count runs of workflows the module does not export as failures.
    #[arg(long)]
    pub allow_missing: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl ReplayArgs {
    /// The explicit run ids: positional ones first, then `--run`.
    pub fn run_ids(&self) -> Vec<String> {
        self.runs.iter().chain(&self.run).cloned().collect()
    }
}

/// Run `ostro replay` (stub until 10-11).
pub fn run(_a: &ReplayArgs) -> Outcome {
    eprintln!("ostro: error: replay is not yet implemented (10-11)");
    Outcome::Usage
}
