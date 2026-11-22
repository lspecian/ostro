//! `ostro init` (CLI-03): scaffold a workflow project from a template.
//!
//! Plan 10-03 fixes the flag set; the implementation is plan 10-08 (this file is its
//! own, `commands/mod.rs` is not edited again).

use std::path::PathBuf;

use clap::{Args, ValueEnum};

use crate::output::Outcome;

/// Project language.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum InitLang {
    Rust,
    Python,
    Ts,
}

/// `ostro init [DIR] --lang <rust|python|ts> [--name N] [--force] [--json]`.
#[derive(Debug, Clone, Args)]
pub struct InitArgs {
    /// Project directory [default: .].
    #[arg(value_name = "DIR")]
    pub dir: Option<PathBuf>,
    /// Project language.
    #[arg(long, value_enum)]
    pub lang: Option<InitLang>,
    /// Project (module) name [default: the directory name].
    #[arg(long)]
    pub name: Option<String>,
    /// Write into a non-empty directory, overwriting template files.
    #[arg(long)]
    pub force: bool,
    /// Print an `ostro.init.result` JSON object.
    #[arg(long)]
    pub json: bool,
}

/// Run `ostro init` (stub until 10-08).
pub fn run(_a: &InitArgs) -> Outcome {
    eprintln!("ostro: error: init is not yet implemented (10-08)");
    Outcome::Usage
}
