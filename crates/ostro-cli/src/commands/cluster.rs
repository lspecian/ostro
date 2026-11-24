//! `ostro cluster {init, join, remove, drain, status, token, reassign, rebalance, upgrade}`
//! (D-P13-23). Phase 13: the clap tree is fixed by 13-02; 13-16 implements the verbs (output
//! tables, `--json`, exit codes). Until then every REST verb reaches the 13-02 client stubs
//! and reports `unimplemented`.
//!
//! `ostro cluster join` prepares a blank data dir and starts the server, so the `ostro`
//! binary intercepts [`ClusterCmd::Join`] (it calls `ostro_server::p13::joiner::run_join`);
//! [`run`] refuses it as a usage error.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use serde_json::json;

use super::{Conn, parse_duration};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome};

/// `ostro cluster ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum ClusterCmd {
    /// Promote this running single node to a cluster (server profile only).
    Init(InitArgs),
    /// Join a blank node to a cluster through a seed, then start it.
    Join(JoinArgs),
    /// Remove a member (it must hold no replicas unless `--force` on a dead node).
    Remove(RemoveArgs),
    /// Drain a member: move its replicas and leaderships away.
    Drain(MemberArgs),
    /// Show members, voters, levels and reassignments.
    Status(StatusArgs),
    /// Node join tokens.
    Token {
        #[command(subcommand)]
        command: TokenCmd,
    },
    /// Reassign a partition's replicas (or cancel an in-flight reassignment).
    Reassign(ReassignArgs),
    /// Rebalance replicas and preferred leaders now.
    Rebalance(StatusArgs),
    /// Rolling upgrades: feature levels and finalization.
    Upgrade {
        #[command(subcommand)]
        command: UpgradeCmd,
    },
}

impl ClusterCmd {
    /// The connection flags of the subcommand; `None` for `join` (no server connection: the
    /// seed URL is positional and the binary runs it).
    pub fn conn(&self) -> Option<&Conn> {
        Some(match self {
            ClusterCmd::Init(a) => &a.conn,
            ClusterCmd::Join(_) => return None,
            ClusterCmd::Remove(a) => &a.conn,
            ClusterCmd::Drain(a) => &a.conn,
            ClusterCmd::Status(a) | ClusterCmd::Rebalance(a) => &a.conn,
            ClusterCmd::Token {
                command: TokenCmd::Create(a),
            } => &a.conn,
            ClusterCmd::Reassign(a) => &a.conn,
            ClusterCmd::Upgrade { command } => match command {
                UpgradeCmd::Status(a)
                | UpgradeCmd::Finalize(a)
                | UpgradeCmd::Hold(a)
                | UpgradeCmd::Release(a) => &a.conn,
            },
        })
    }
}

/// `ostro cluster init`.
#[derive(Debug, Clone, Args)]
pub struct InitArgs {
    /// Advertised API address (default: `[cluster] advertise` / the listener).
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_api: Option<String>,
    /// Advertised cluster listener address.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_cluster: Option<String>,
    /// Advertised replication listener address.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_repl: Option<String>,
    /// Replication factor policy (1, 3 or 5; default 3).
    #[arg(long)]
    pub rf: Option<u8>,
    /// `min_insync` policy (default 2 for rf 3).
    #[arg(long)]
    pub min_insync: Option<u8>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro cluster join <SEED> --token T --ca-hash sha256:H`.
#[derive(Debug, Clone, Args)]
pub struct JoinArgs {
    /// The seed node's API URL (`https://seed:7700`).
    pub seed: String,
    /// Node join token (`ostro cluster token create --node`).
    #[arg(long, env = "OSTRO_JOIN_TOKEN", hide_env_values = true)]
    pub token: String,
    /// The cluster CA pin (`sha256:<hex>`).
    #[arg(long)]
    pub ca_hash: String,
    /// Data directory to prepare [default: the server profile's].
    #[arg(long = "data", value_name = "DIR")]
    pub data_dir: Option<PathBuf>,
    /// Server configuration file to start with.
    #[arg(long, short, env = "OSTRO_CONFIG")]
    pub config: Option<PathBuf>,
    /// Advertised API address.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_api: Option<String>,
    /// Advertised cluster listener address.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_cluster: Option<String>,
    /// Advertised replication listener address.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_repl: Option<String>,
    /// Prepare the data dir only; do not start the server.
    #[arg(long)]
    pub no_start: bool,
}

/// `ostro cluster remove <ID> [--force]`.
#[derive(Debug, Clone, Args)]
pub struct RemoveArgs {
    /// Node id.
    pub id: u64,
    /// Do not wait for a dead node's replicas to move (the quorum check still applies).
    #[arg(long)]
    pub force: bool,
    #[command(flatten)]
    pub conn: Conn,
}

/// One member id.
#[derive(Debug, Clone, Args)]
pub struct MemberArgs {
    /// Node id.
    pub id: u64,
    #[command(flatten)]
    pub conn: Conn,
}

/// Connection flags only.
#[derive(Debug, Clone, Args)]
pub struct StatusArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro cluster token ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum TokenCmd {
    /// Create a node join token (printed once).
    Create(TokenCreateArgs),
}

/// `ostro cluster token create --node [--ttl D]`.
#[derive(Debug, Clone, Args)]
pub struct TokenCreateArgs {
    /// A node join token (the only kind here; worker tokens are `ostro worker token`).
    #[arg(long)]
    pub node: bool,
    /// Token lifetime (friendly duration) [default: the server's].
    #[arg(long, value_parser = parse_duration)]
    pub ttl: Option<std::time::Duration>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro cluster reassign --partition P (--to a,b,c | --cancel)`.
#[derive(Debug, Clone, Args)]
pub struct ReassignArgs {
    /// Partition.
    #[arg(long)]
    pub partition: u16,
    /// Target replica set (node ids, comma-separated).
    #[arg(long, value_delimiter = ',', required_unless_present = "cancel")]
    pub to: Vec<u64>,
    /// Cancel the in-flight reassignment (before its leader move).
    #[arg(long, conflicts_with = "to")]
    pub cancel: bool,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro cluster upgrade ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum UpgradeCmd {
    /// Feature levels of the cluster and every member.
    Status(StatusArgs),
    /// Finalize the highest level every member supports.
    Finalize(StatusArgs),
    /// Hold the current level (keep the downgrade window open).
    Hold(StatusArgs),
    /// Release a hold.
    Release(StatusArgs),
}

/// Run a `cluster` verb (13-16 adds the human output; until then the JSON body).
pub async fn run(c: &Client, out: Out, cmd: &ClusterCmd) -> Result<Outcome, CallError> {
    let r = match cmd {
        ClusterCmd::Join(_) => {
            return Err(CallError::Usage(
                "`ostro cluster join` is run by the ostro binary".into(),
            ));
        }
        ClusterCmd::Init(a) => {
            c.cluster_init(&json!({
                "advertise": a.advertise_cluster,
                "rf": a.rf,
                "min_insync": a.min_insync,
            }))
            .await?
        }
        ClusterCmd::Remove(a) => c.cluster_remove(a.id, &json!({ "force": a.force })).await?,
        ClusterCmd::Drain(a) => c.cluster_drain(a.id).await?,
        ClusterCmd::Status(_) => c.cluster_status().await?,
        ClusterCmd::Token {
            command: TokenCmd::Create(a),
        } => {
            c.create_node_token(&json!({ "ttl_secs": a.ttl.map(|d| d.as_secs()) }))
                .await?
        }
        ClusterCmd::Reassign(a) => {
            c.cluster_reassign(&json!({
                "partition": a.partition,
                "replicas": a.to,
                "cancel": a.cancel,
            }))
            .await?
        }
        ClusterCmd::Rebalance(_) => c.cluster_rebalance().await?,
        ClusterCmd::Upgrade { command } => match command {
            UpgradeCmd::Status(_) => c.upgrade_status().await?,
            UpgradeCmd::Finalize(_) => c.upgrade_finalize().await?,
            UpgradeCmd::Hold(_) => c.upgrade_hold().await?,
            UpgradeCmd::Release(_) => c.upgrade_release().await?,
        },
    };
    out.json(&r.body);
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;
    use crate::ClientCommand;

    #[derive(Debug, Parser)]
    struct T {
        #[command(subcommand)]
        cmd: ClientCommand,
    }

    fn parse(args: &[&str]) -> ClientCommand {
        T::try_parse_from(std::iter::once("ostro").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
            .cmd
    }

    #[test]
    fn cluster_verbs_parse() {
        let ClientCommand::Cluster {
            command: ClusterCmd::Join(j),
        } = parse(&[
            "cluster",
            "join",
            "https://seed:7700",
            "--token",
            "njt_x",
            "--ca-hash",
            "sha256:ab",
            "--data",
            "/d",
            "--no-start",
        ])
        else {
            panic!("join");
        };
        assert_eq!(j.seed, "https://seed:7700");
        assert_eq!(j.data_dir, Some(PathBuf::from("/d")));
        assert!(j.no_start);
        for args in [
            &["cluster", "init", "--rf", "3"][..],
            &["cluster", "remove", "4", "--force"],
            &["cluster", "drain", "4"],
            &["cluster", "status"],
            &["cluster", "token", "create", "--node", "--ttl", "1h"],
            &["cluster", "reassign", "--partition", "2", "--to", "1,2,3"],
            &["cluster", "reassign", "--partition", "2", "--cancel"],
            &["cluster", "rebalance"],
            &["cluster", "upgrade", "status"],
            &["cluster", "upgrade", "finalize"],
            &["cluster", "upgrade", "hold"],
            &["cluster", "upgrade", "release"],
        ] {
            let ClientCommand::Cluster { command } = parse(args) else {
                panic!("{args:?}");
            };
            assert!(command.conn().is_some(), "{args:?}");
        }
        assert!(
            T::try_parse_from(["ostro", "cluster", "reassign", "--partition", "2"]).is_err(),
            "--to or --cancel is required"
        );
    }

    #[test]
    fn migrate_and_modules_parse() {
        assert!(matches!(
            parse(&[
                "migrate-runs",
                "--workflow",
                "w",
                "--from",
                "1",
                "--to",
                "2",
                "--dry-run",
            ]),
            ClientCommand::MigrateRuns(_)
        ));
        assert!(matches!(
            parse(&["modules", "rm", "ab"]),
            ClientCommand::Modules { .. }
        ));
        assert!(matches!(
            parse(&["modules", "gc"]),
            ClientCommand::Modules { .. }
        ));
    }
}
