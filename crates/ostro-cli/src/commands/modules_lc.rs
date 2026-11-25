//! `ostro modules {rm, gc}` (D-P13-20): retire a module (`DELETE /v1/modules/{sha}`,
//! idempotent) and run one module GC round (`POST /v1/modules/gc`); 13-15's server side,
//! 13-02's clap shape, 13-16's verbs. Exit codes: 0, 1 API error (404 unknown module, 409
//! `module_active`, 412 `feature_not_enabled`), 2 usage.

use clap::{Args, Subcommand};
use ostro_client::cluster::{GcReport, RetiredModule};

use super::Conn;
use super::worker_tokens::ms_time;
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

/// Run a `modules` verb and return its stdout text.
pub async fn render(c: &Client, json: bool, cmd: &ModulesCommand) -> Result<String, CallError> {
    match cmd {
        ModulesCommand::Rm(a) => {
            let r = c.retire_module(&a.sha).await?;
            if json {
                return Ok(pretty(&r.body));
            }
            let Ok(m) = serde_json::from_value::<RetiredModule>(r.body.clone()) else {
                return Ok(pretty(&r.body));
            };
            let versions = if m.versions.is_empty() {
                "none".to_owned()
            } else {
                m.versions
                    .iter()
                    .map(|v| format!("{}@{}", v.name, v.version))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            Ok(format!(
                "retired module {} at {}\nworkflow versions retired: {versions}\nmodule GC deletes it once no run references it (`ostro modules gc`)",
                m.sha256,
                ms_time(&serde_json::json!(m.retired_at_ms)),
            ))
        }
        ModulesCommand::Gc(_) => {
            let r = c.run_module_gc().await?;
            if json {
                return Ok(pretty(&r.body));
            }
            let Ok(g) = serde_json::from_value::<GcReport>(r.body.clone()) else {
                return Ok(pretty(&r.body));
            };
            if g.postponed {
                return Ok(format!(
                    "module GC postponed: {}",
                    g.reason.as_deref().unwrap_or("no reason given")
                ));
            }
            let list = |v: &[String]| {
                if v.is_empty() {
                    "-".to_owned()
                } else {
                    v.join(", ")
                }
            };
            Ok(format!(
                "module GC: {} retired candidate(s)\ndeleted:    {}\nreferenced: {}",
                g.candidates.len(),
                list(&g.deleted),
                list(&g.referenced),
            ))
        }
    }
}

fn pretty(v: &serde_json::Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

/// Run a `modules` verb, printing its output.
pub async fn run(c: &Client, out: Out, cmd: &ModulesCommand) -> Result<Outcome, CallError> {
    let text = render(c, out.json, cmd).await?;
    println!("{text}");
    Ok(Outcome::Ok)
}
