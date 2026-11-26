//! `ostro auth …` (Phase 14, 14-01b clap shape): API tokens and whoami are **14-17**'s;
//! `ostro auth bootstrap --data-dir` is run by the `ostro` binary (offline, 14-03).

use std::path::PathBuf;

use clap::{Args, Subcommand};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro auth` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum AuthCmd {
    /// API tokens (create, ls, revoke, rotate).
    Token {
        #[command(subcommand)]
        command: TokenCmd,
    },
    /// Show the calling principal (token id, tenant, scopes).
    Whoami(WhoamiArgs),
    /// Create a system-admin token on a stopped single node (offline; run by the ostro
    /// binary).
    Bootstrap(BootstrapArgs),
}

/// `ostro auth token` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum TokenCmd {
    /// Create an API token (the bearer is printed once).
    Create(TokenCreateArgs),
    /// List API tokens (never a secret).
    Ls(TokenLsArgs),
    /// Revoke an API token.
    Revoke(TokenIdArgs),
    /// Rotate an API token (a new bearer; optionally revoke the old one).
    Rotate(TokenIdArgs),
}

/// `ostro auth token create`.
#[derive(Debug, Clone, Args)]
pub struct TokenCreateArgs {
    /// Token name.
    pub name: String,
    /// Scopes (repeatable or comma-separated): admin, deploy, operate, trigger, read.
    #[arg(long = "scope", value_delimiter = ',')]
    pub scopes: Vec<String>,
    /// Tenant of the token (unset: a system token).
    #[arg(long)]
    pub tenant: Option<String>,
    /// Lifetime (friendly duration); unset: `[auth] default_token_ttl`.
    #[arg(long, value_parser = super::parse_duration_text)]
    pub ttl: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth token ls`.
#[derive(Debug, Clone, Args)]
pub struct TokenLsArgs {
    /// Only this tenant's tokens.
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth token revoke|rotate <ID>`.
#[derive(Debug, Clone, Args)]
pub struct TokenIdArgs {
    /// Token id.
    pub id: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth whoami`.
#[derive(Debug, Clone, Args)]
pub struct WhoamiArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth bootstrap` (offline, run by the ostro binary).
#[derive(Debug, Clone, Args)]
pub struct BootstrapArgs {
    /// The stopped node's data directory.
    #[arg(long, value_name = "DIR")]
    pub data_dir: PathBuf,
    /// The server configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
    /// Token lifetime (friendly duration); unset: no expiry.
    #[arg(long, value_parser = super::parse_duration_text)]
    pub expires: Option<String>,
}

impl AuthCmd {
    /// The connection flags; `None` for `bootstrap` (run by the ostro binary).
    pub fn conn(&self) -> Option<&Conn> {
        match self {
            AuthCmd::Token { command } => Some(match command {
                TokenCmd::Create(a) => &a.conn,
                TokenCmd::Ls(a) => &a.conn,
                TokenCmd::Revoke(a) | TokenCmd::Rotate(a) => &a.conn,
            }),
            AuthCmd::Whoami(a) => Some(&a.conn),
            AuthCmd::Bootstrap(_) => None,
        }
    }
}

/// Run an auth verb. Stub: not implemented until 14-17.
pub async fn run(_client: &Client, _out: Out, _cmd: &AuthCmd) -> Result<Outcome, CallError> {
    Err(super::lands_in("ostro auth", "14-17"))
}
