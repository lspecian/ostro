//! `ostro tenant …` (Phase 14, 14-01b clap shape; **14-17** implements the verbs).

use clap::{Args, Subcommand};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro tenant` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum TenantCmd {
    /// List tenants.
    Ls(TenantLsArgs),
}

impl TenantCmd {
    /// The connection flags.
    pub fn conn(&self) -> &Conn {
        match self {
            TenantCmd::Ls(a) => &a.conn,
        }
    }
}

/// `ostro tenant ls`.
#[derive(Debug, Clone, Args)]
pub struct TenantLsArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// Run a tenant verb. Stub: not implemented until 14-17.
pub async fn run(_client: &Client, _out: Out, _cmd: &TenantCmd) -> Result<Outcome, CallError> {
    Err(super::lands_in("ostro tenant", "14-17"))
}
