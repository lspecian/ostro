//! `ostro cluster {init, join, remove, drain, status, token, reassign, rebalance, upgrade}`
//! (D-P13-23). The clap tree is 13-02's; 13-16 implements the verbs over
//! `ostro_client::cluster`: `--json` prints the response body, human output prints the
//! status / upgrade tables ([`status_text`], [`upgrade_text`]); exit codes follow the
//! Phase 10 contract (0 ok, 1 API or transport error incl. 404 / 409 / 412 / 503, 2 usage).
//!
//! | verb | request | human output |
//! |------|---------|--------------|
//! | `init [--advertise-cluster A] [--rf N] [--min-insync N]` | `POST /v1/cluster/init` | headline + status |
//! | `join <SEED> --token T --ca-hash H [--data D] [--advertise-*] [--no-start]` | the ostro binary (13-09 joiner) | — |
//! | `remove <ID> [--force]` | `POST /v1/cluster/members/{id}/remove` | headline + status |
//! | `drain <ID>` | `POST /v1/cluster/members/{id}/drain` | headline + status |
//! | `status` | `GET /v1/cluster/status` | status |
//! | `token create --node [--ttl D]` | `POST /v1/cluster/tokens` | token id, expiry, join command (once) |
//! | `reassign --partition P (--to a,b,c \| --cancel)` | `POST /v1/cluster/reassign` | headline + status |
//! | `rebalance` | `POST /v1/cluster/rebalance` | headline + status |
//! | `upgrade {status,finalize,hold,release}` | `GET /v1/cluster/upgrade`, `POST …/{finalize,hold,release}` | levels table |
//!
//! `ostro cluster join` prepares a blank data dir and starts the server, so the `ostro`
//! binary intercepts [`ClusterCmd::Join`] (it calls `ostro_server::p13::joiner::run_join`);
//! [`run`] refuses it as a usage error.

use std::path::PathBuf;

use clap::{Args, Subcommand};
use ostro_client::cluster::{ClusterStatus, InitRequest, NodeToken, UpgradeStatus};
use serde_json::{Value, json};

use super::worker_tokens::ms_time;
use super::{Conn, parse_duration};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, table};

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
    /// Not accepted by `init` (a usage error): a promoted node's API address is its
    /// `[api] listen`.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_api: Option<String>,
    /// Advertised cluster listener address peers dial (default: the first `[cluster]
    /// advertise` entry, with the listener port when it names only a host).
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_cluster: Option<String>,
    /// Not accepted by `init` (a usage error): the replication listener is `[cluster]
    /// replication_listen`.
    #[arg(long, value_name = "HOST:PORT")]
    pub advertise_repl: Option<String>,
    /// Replication factor policy (1, 3 or 5; default 3).
    #[arg(long)]
    pub rf: Option<u8>,
    /// `min_insync` policy (1..=rf; default rf / 2 + 1, i.e. 2 for rf 3).
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

/// The connection-free part of `ostro cluster token create`: `--node` is required (worker
/// tokens are `ostro worker token create`).
fn token_body(a: &TokenCreateArgs) -> Result<Value, CallError> {
    if !a.node {
        return Err(CallError::Usage(
            "`ostro cluster token create` needs --node (worker join tokens: `ostro worker token create`)"
                .into(),
        ));
    }
    Ok(json!({ "ttl_secs": a.ttl.map(|d| d.as_secs()) }))
}

/// The init request of `ostro cluster init`: the advertised cluster address (the API and
/// replication addresses are the listener's own; 13-16 SUMMARY), rf and min_insync.
fn init_body(a: &InitArgs) -> Result<Value, CallError> {
    if a.advertise_api.is_some() || a.advertise_repl.is_some() {
        return Err(CallError::Usage(
            "`ostro cluster init` takes only --advertise-cluster: the API and replication \
             addresses of a promoted node are its listeners ([api] listen, [cluster] \
             replication_listen)"
                .into(),
        ));
    }
    let req = InitRequest {
        advertise: a.advertise_cluster.clone(),
        rf: a.rf,
        min_insync: a.min_insync,
    };
    serde_json::to_value(req).map_err(|e| CallError::Usage(format!("init request: {e}")))
}

/// Run one `cluster` verb and return what it prints on stdout (`--json`: the response body,
/// pretty-printed). Errors are returned for the shared `Out::error` mapping (exit 1 for API
/// and transport errors, 2 for usage).
pub async fn render(c: &Client, json: bool, cmd: &ClusterCmd) -> Result<String, CallError> {
    let (headline, r) = match cmd {
        ClusterCmd::Join(_) => {
            return Err(CallError::Usage(
                "`ostro cluster join` is run by the ostro binary".into(),
            ));
        }
        ClusterCmd::Init(a) => {
            let r = c.cluster_init(&init_body(a)?).await?;
            (Some("this node is a cluster now; add nodes with `ostro cluster token create --node` and `ostro cluster join`".to_owned()), r)
        }
        ClusterCmd::Remove(a) => {
            let r = c.cluster_remove(a.id, &json!({ "force": a.force })).await?;
            (Some(format!("node {} removed", a.id)), r)
        }
        ClusterCmd::Drain(a) => {
            let r = c.cluster_drain(a.id).await?;
            (
                Some(format!(
                    "node {} draining: its replicas and leaderships move away",
                    a.id
                )),
                r,
            )
        }
        ClusterCmd::Status(_) => (None, c.cluster_status().await?),
        ClusterCmd::Token {
            command: TokenCmd::Create(a),
        } => {
            let r = c.create_node_token(&token_body(a)?).await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(token_text(c, &r.body));
        }
        ClusterCmd::Reassign(a) => {
            let r = c
                .cluster_reassign(&json!({
                    "partition": a.partition,
                    "replicas": a.to,
                    "cancel": a.cancel,
                }))
                .await?;
            let line = if a.cancel {
                format!("partition {}: reassignment cancelled", a.partition)
            } else {
                format!(
                    "partition {}: reassignment to {:?} started",
                    a.partition, a.to
                )
            };
            (Some(line), r)
        }
        ClusterCmd::Rebalance(_) => (
            Some("rebalance requested".to_owned()),
            c.cluster_rebalance().await?,
        ),
        ClusterCmd::Upgrade { command } => {
            let (line, r) = match command {
                UpgradeCmd::Status(_) => (None, c.upgrade_status().await?),
                UpgradeCmd::Finalize(_) => (Some("finalized"), c.upgrade_finalize().await?),
                UpgradeCmd::Hold(_) => (Some("hold set"), c.upgrade_hold().await?),
                UpgradeCmd::Release(_) => (Some("hold released"), c.upgrade_release().await?),
            };
            if json {
                return Ok(pretty(&r.body));
            }
            let body = upgrade_text(&r.body);
            return Ok(match line {
                Some(l) => format!("{l}\n{body}"),
                None => body,
            });
        }
    };
    if json {
        return Ok(pretty(&r.body));
    }
    let body = status_text(&r.body);
    Ok(match headline {
        Some(h) => format!("{h}\n{body}"),
        None => body,
    })
}

/// Run a `cluster` verb, printing its output.
pub async fn run(c: &Client, out: Out, cmd: &ClusterCmd) -> Result<Outcome, CallError> {
    let text = render(c, out.json, cmd).await?;
    println!("{text}");
    Ok(Outcome::Ok)
}

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

fn yes(b: bool) -> String {
    if b { "yes" } else { "no" }.to_owned()
}

/// Human form of a `ClusterStatus` body.
pub fn status_text(v: &Value) -> String {
    let Ok(st) = serde_json::from_value::<ClusterStatus>(v.clone()) else {
        return pretty(v);
    };
    let mut out = format!(
        "cluster {}  meta leader {}  finalized level {}{}  member levels {}..{}",
        st.cluster_id,
        if st.meta_leader == 0 {
            "-".to_owned()
        } else {
            st.meta_leader.to_string()
        },
        st.finalized,
        st.hold
            .map(|h| format!(" (held at {h})"))
            .unwrap_or_default(),
        st.level_min,
        st.level_max,
    );
    out.push('\n');
    out.push_str(&match &st.policy {
        Some(p) => format!(
            "policy: rf {}, min_insync {}, max voters {}",
            p.rf, p.min_insync, p.max_voters
        ),
        None => "policy: none (a single node or a static cluster; `ostro cluster init` promotes a single node)".to_owned(),
    });
    let rows: Vec<Vec<String>> = st
        .members
        .iter()
        .map(|m| {
            vec![
                m.node_id.to_string(),
                m.state.clone(),
                m.raft_role.clone(),
                yes(m.live),
                m.version.as_ref().map_or("-".to_owned(), |v| {
                    format!("{}..{}", v.level_min, v.level_max)
                }),
                m.addr.clone(),
                m.cluster_addr.clone().unwrap_or_else(|| "-".to_owned()),
            ]
        })
        .collect();
    out.push_str("\n\n");
    out.push_str(&table(
        &["NODE", "STATE", "ROLE", "LIVE", "LEVELS", "API", "CLUSTER"],
        &rows,
    ));
    if !st.voters_outgoing.is_empty() {
        out.push_str(&format!(
            "\n\njoint configuration: voters {:?}, outgoing {:?}",
            st.voters, st.voters_outgoing
        ));
    }
    if !st.reassignments.is_empty() {
        let rows: Vec<Vec<String>> = st
            .reassignments
            .iter()
            .map(|r| {
                vec![
                    r.partition.to_string(),
                    ids(&r.target),
                    ids(&r.adding),
                    ids(&r.removing),
                ]
            })
            .collect();
        out.push_str("\n\nreassignments in flight:\n");
        out.push_str(&table(
            &["PARTITION", "TARGET", "ADDING", "REMOVING"],
            &rows,
        ));
    }
    out
}

fn ids(v: &[u64]) -> String {
    if v.is_empty() {
        return "-".to_owned();
    }
    v.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
}

/// Human form of an `UpgradeStatus` body.
pub fn upgrade_text(v: &Value) -> String {
    let Ok(u) = serde_json::from_value::<UpgradeStatus>(v.clone()) else {
        return pretty(v);
    };
    let mut out = format!(
        "finalized level {}  hold {}  highest common level {}  auto-finalize {}",
        u.finalized,
        u.hold.map_or("none".to_owned(), |h| h.to_string()),
        u.common_level_max,
        if u.auto_finalize { "on" } else { "off" },
    );
    if u.common_level_max > u.finalized && u.hold.is_none() {
        out.push_str(&format!(
            "\nlevel {} can be finalized (`ostro cluster upgrade finalize`)",
            u.common_level_max
        ));
    }
    let rows: Vec<Vec<String>> = u
        .members
        .iter()
        .map(|m| {
            vec![
                m.node_id.to_string(),
                format!("{}..{}", m.level_min, m.level_max),
                yes(m.live),
            ]
        })
        .collect();
    out.push_str("\n\n");
    out.push_str(&table(&["NODE", "LEVELS", "LIVE"], &rows));
    out
}

/// Human form of a created node token: the secret once, and the join command.
fn token_text(c: &Client, v: &Value) -> String {
    let Ok(t) = serde_json::from_value::<NodeToken>(v.clone()) else {
        return pretty(v);
    };
    format!(
        "token id:   {}\nexpires at: {}\njoin a blank node with (shown once; store the token as a secret):\n  ostro cluster join {} --token {} --ca-hash {}",
        t.id,
        ms_time(&json!(t.expires_at_ms)),
        c.base().as_str().trim_end_matches('/'),
        t.token,
        t.ca_hash,
    )
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
