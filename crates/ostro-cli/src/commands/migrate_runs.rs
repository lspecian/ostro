//! `ostro migrate-runs --workflow W --from V|sha --to V [--filter ...] [--limit N]
//! [--dry-run]` (D-P13-21). Phase 13: clap shape fixed by 13-02; 13-16 implements the verb
//! (until then it reaches the 13-02 client stub and reports `unimplemented`).

use clap::Args;
use serde_json::{Value, json};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro migrate-runs`.
#[derive(Debug, Clone, Args)]
pub struct MigrateRunsArgs {
    /// Workflow name.
    #[arg(long)]
    pub workflow: String,
    /// Source: a version number or a module sha256 (hex).
    #[arg(long)]
    pub from: String,
    /// Target version.
    #[arg(long)]
    pub to: u32,
    /// Run filter (`key=value`, repeatable; e.g. `status=running`, `tag=x`).
    #[arg(long)]
    pub filter: Vec<String>,
    /// At most this many runs.
    #[arg(long)]
    pub limit: Option<u32>,
    /// Replay-check only; repin nothing.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

/// The `from` field: a number is a version, anything else a module sha.
fn from_value(from: &str) -> Value {
    match from.parse::<u32>() {
        Ok(v) => json!({ "version": v }),
        Err(_) => json!({ "sha": from }),
    }
}

/// `ostro migrate-runs` (13-16 adds the human report).
pub async fn run(c: &Client, out: Out, a: &MigrateRunsArgs) -> Result<Outcome, CallError> {
    let r = c
        .migrate_runs(&json!({
            "workflow": a.workflow,
            "from": from_value(&a.from),
            "to_version": a.to,
            "filter": a.filter,
            "limit": a.limit,
            "dry_run": a.dry_run,
        }))
        .await?;
    out.json(&r.body);
    Ok(Outcome::Ok)
}
