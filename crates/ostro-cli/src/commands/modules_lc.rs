//! `ostro modules {rm, gc}` (D-P13-20): retire a module, run one module GC round. Phase 13:
//! clap shape fixed by 13-02; 13-16 implements the verbs (until then they reach the 13-02
//! client stubs and report `unimplemented`).

use clap::{Args, Subcommand};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro modules ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum ModulesCommand {
    /// Retire a module (refused while one of its workflow versions is active).
    Rm(ModuleRmArgs),
    /// Run one module GC round now.
    Gc(ModuleGcArgs),
}

impl ModulesCommand {
    /// The connection flags of the subcommand.
    pub fn conn(&self) -> &Conn {
        match self {
            ModulesCommand::Rm(a) => &a.conn,
            ModulesCommand::Gc(a) => &a.conn,
        }
    }
}

/// `ostro modules rm <SHA>`.
#[derive(Debug, Clone, Args)]
pub struct ModuleRmArgs {
    /// Module sha256 (hex).
    pub sha: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro modules gc`.
#[derive(Debug, Clone, Args)]
pub struct ModuleGcArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// Run a `modules` verb (13-16 adds the human output).
pub async fn run(c: &Client, out: Out, cmd: &ModulesCommand) -> Result<Outcome, CallError> {
    let r = match cmd {
        ModulesCommand::Rm(a) => c.retire_module(&a.sha).await?,
        ModulesCommand::Gc(_) => c.run_module_gc().await?,
    };
    out.json(&r.body);
    Ok(Outcome::Ok)
}
