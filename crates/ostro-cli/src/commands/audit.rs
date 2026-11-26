//! `ostro audit …` (Phase 14, 14-01b clap shape): `ls` is **14-18**'s; `ostro audit verify
//! --data-dir` is run by the `ostro` binary (offline, 14-04).

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro audit` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum AuditCmd {
    /// Query the cluster-wide audit log.
    Ls(AuditLsArgs),
    /// Verify a node's audit journal chain offline (run by the ostro binary).
    Verify(AuditVerifyArgs),
}

/// `ostro audit ls`.
#[derive(Debug, Clone, Args)]
pub struct AuditLsArgs {
    /// Only this tenant's records.
    #[arg(long)]
    pub tenant: Option<String>,
    /// Only this actor's records.
    #[arg(long)]
    pub actor: Option<String>,
    /// Only this action.
    #[arg(long)]
    pub action: Option<String>,
    /// Most records to return.
    #[arg(long)]
    pub limit: Option<u32>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro audit verify` (offline).
#[derive(Debug, Clone, Args)]
pub struct AuditVerifyArgs {
    /// The node's data directory.
    #[arg(long, value_name = "DIR")]
    pub data_dir: PathBuf,
    /// The server configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
}

impl AuditCmd {
    /// The connection flags; `None` for `verify` (run by the ostro binary).
    pub fn conn(&self) -> Option<&Conn> {
        match self {
            AuditCmd::Ls(a) => Some(&a.conn),
            AuditCmd::Verify(_) => None,
        }
    }
}

/// Run an audit verb. Stub: not implemented until 14-18.
pub async fn run(_client: &Client, _out: Out, _cmd: &AuditCmd) -> Result<Outcome, CallError> {
    Err(super::lands_in("ostro audit", "14-18"))
}
