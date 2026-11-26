//! `ostro security …` (Phase 14, 14-01b clap shape; **14-19** implements the verbs): the
//! module signing policy and trusted keys.

use clap::{Args, Subcommand};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro security` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum SecurityCmd {
    /// The module signing policy (get, set).
    Policy {
        #[command(subcommand)]
        command: PolicyCmd,
    },
    /// Trusted signing keys (add, rm).
    Trust {
        #[command(subcommand)]
        command: TrustCmd,
    },
}

/// `ostro security policy` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum PolicyCmd {
    /// Show the module policy.
    Get(SecurityArgs),
    /// Set the module policy.
    Set(PolicySetArgs),
}

/// `ostro security trust` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum TrustCmd {
    /// Trust a public key (minisign or cosign).
    Add(TrustAddArgs),
    /// Stop trusting a key.
    Rm(TrustRmArgs),
}

/// Connection-only arguments.
#[derive(Debug, Clone, Args)]
pub struct SecurityArgs {
    /// Tenant (unset: the cluster policy).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro security policy set`.
#[derive(Debug, Clone, Args)]
pub struct PolicySetArgs {
    /// Require a valid signature on every upload and activation.
    #[arg(long)]
    pub require_signed: Option<bool>,
    /// Tenant (unset: the cluster policy).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro security trust add`.
#[derive(Debug, Clone, Args)]
pub struct TrustAddArgs {
    /// The public key file (minisign `.pub` or cosign PEM).
    pub key: std::path::PathBuf,
    /// A comment stored with the key.
    #[arg(long)]
    pub comment: Option<String>,
    /// Tenant (unset: a cluster key).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro security trust rm`.
#[derive(Debug, Clone, Args)]
pub struct TrustRmArgs {
    /// Key id (hex).
    pub key_id: String,
    /// Tenant (unset: a cluster key).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

impl SecurityCmd {
    /// The connection flags.
    pub fn conn(&self) -> &Conn {
        match self {
            SecurityCmd::Policy { command } => match command {
                PolicyCmd::Get(a) => &a.conn,
                PolicyCmd::Set(a) => &a.conn,
            },
            SecurityCmd::Trust { command } => match command {
                TrustCmd::Add(a) => &a.conn,
                TrustCmd::Rm(a) => &a.conn,
            },
        }
    }
}

/// Run a security verb. Stub: not implemented until 14-19.
pub async fn run(_client: &Client, _out: Out, _cmd: &SecurityCmd) -> Result<Outcome, CallError> {
    Err(super::lands_in("ostro security", "14-19"))
}
