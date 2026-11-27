//! `ostro audit …` (Phase 14): `ls` queries the cluster-wide audit log (14-18); `ostro audit
//! verify --data-dir` is run by the `ostro` binary (offline, 14-04's `verify_dir`; a broken
//! chain exits non-zero naming the first bad seq).
//!
//! `ostro audit ls [--since 1h] [--until T] [--action run.cancel] [--tenant t] [--actor a]
//! [--limit n] [--cursor c] [--json]`: one page, oldest first. `--since`/`--until` take a
//! friendly duration (`1h`: that long ago), Unix milliseconds or RFC 3339. The human view
//! prints a table and, when there is more, the `--cursor` for the next page; members that
//! did not answer are listed (the page is partial).

use std::path::PathBuf;

use clap::{Args, Subcommand};
use ostro_client::audit::AuditFilter;
use serde_json::Value;

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

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
    /// Only this tenant's records (`_` = records without a tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    /// Only this actor's records (token id, worker id, `run:<id>`).
    #[arg(long)]
    pub actor: Option<String>,
    /// Only this action (exact, or `prefix.*`).
    #[arg(long)]
    pub action: Option<String>,
    /// Records at or after: a duration ago (`1h`), Unix ms or RFC 3339.
    #[arg(long)]
    pub since: Option<String>,
    /// Records before: a duration ago, Unix ms or RFC 3339.
    #[arg(long)]
    pub until: Option<String>,
    /// Most records to return (server default 100, at most 1000).
    #[arg(long)]
    pub limit: Option<u32>,
    /// Cursor from the previous page.
    #[arg(long)]
    pub cursor: Option<String>,
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

/// A `--since`/`--until` value as sent: a friendly duration becomes `now_ms - d` (Unix
/// ms); Unix ms and RFC 3339 pass through for the server to parse.
pub fn time_arg(v: &str, now_ms: u64) -> String {
    let t = v.trim();
    if t.bytes().all(|b| b.is_ascii_digit()) {
        return t.to_owned();
    }
    match super::parse_duration(t) {
        Ok(d) => {
            let ms = u64::try_from(d.as_millis()).unwrap_or(u64::MAX);
            now_ms.saturating_sub(ms).to_string()
        }
        Err(_) => t.to_owned(),
    }
}

impl AuditLsArgs {
    /// The query filter (relative times resolved against `now_ms`).
    pub fn filter(&self, now_ms: u64) -> AuditFilter {
        AuditFilter {
            since: self.since.as_deref().map(|v| time_arg(v, now_ms)),
            until: self.until.as_deref().map(|v| time_arg(v, now_ms)),
            tenant: self.tenant.clone(),
            action: self.action.clone(),
            actor: self.actor.clone(),
            limit: self.limit,
            cursor: self.cursor.clone(),
        }
    }
}

/// The wall clock in Unix ms (relative `--since`).
fn now_ms() -> u64 {
    let ms = jiff::Timestamp::now().as_millisecond();
    u64::try_from(ms).unwrap_or(0)
}

/// Unix ms as RFC 3339 (UTC, millisecond precision), or `-`.
fn ts(v: &Value) -> String {
    v.as_i64()
        .and_then(|ms| jiff::Timestamp::from_millisecond(ms).ok())
        .map(|t| format!("{t:.3}"))
        .unwrap_or_else(|| "-".to_owned())
}

/// The human page (table, partial members, next cursor).
pub fn render_page(body: &Value) -> String {
    let records = body["records"].as_array().cloned().unwrap_or_default();
    let mut out = String::new();
    if records.is_empty() {
        out.push_str("no audit records\n");
    } else {
        let rows: Vec<Vec<String>> = records
            .iter()
            .map(|r| {
                let count = r["details"]["count"]
                    .as_str()
                    .map(|c| format!(" (x{c})"))
                    .unwrap_or_default();
                vec![
                    ts(&r["ts_ms"]),
                    s(&r["node"]),
                    format!("{}{count}", s(&r["action"])),
                    format!("{} ({})", s(&r["actor"]), s(&r["actor_kind"])),
                    s(&r["tenant"]),
                    s(&r["target"]),
                    s(&r["outcome"]),
                ]
            })
            .collect();
        out.push_str(&table(
            &[
                "TIME", "NODE", "ACTION", "ACTOR", "TENANT", "TARGET", "OUTCOME",
            ],
            &rows,
        ));
        out.push('\n');
    }
    if let Some(nodes) = body["partial_nodes"].as_array().filter(|n| !n.is_empty()) {
        let list: Vec<String> = nodes.iter().map(s).collect();
        out.push_str(&format!(
            "partial: nodes {} did not answer\n",
            list.join(", ")
        ));
    }
    if let Some(c) = body["cursor"].as_str() {
        out.push_str(&format!("more: --cursor {c}\n"));
    }
    out
}

/// The output of an audit verb: the page as pretty JSON with `json`, else the human view.
/// `verify` is intercepted by the ostro binary before this (usage error here).
pub async fn render(client: &Client, json: bool, cmd: &AuditCmd) -> Result<String, CallError> {
    match cmd {
        AuditCmd::Ls(a) => {
            let r = client.audit_query(&a.filter(now_ms())).await?;
            Ok(if json {
                let mut t =
                    serde_json::to_string_pretty(&r.body).unwrap_or_else(|_| r.body.to_string());
                t.push('\n');
                t
            } else {
                render_page(&r.body)
            })
        }
        AuditCmd::Verify(_) => Err(CallError::Usage(
            "`ostro audit verify` is run by the ostro binary".to_owned(),
        )),
    }
}

/// Run an audit verb.
pub async fn run(client: &Client, out: Out, cmd: &AuditCmd) -> Result<Outcome, CallError> {
    print!("{}", render(client, out.json, cmd).await?);
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_args_resolve_relative_durations_only() {
        let now = 10_000_000;
        assert_eq!(time_arg("1h", now), (now - 3_600_000).to_string());
        assert_eq!(time_arg("500ms", now), (now - 500).to_string());
        assert_eq!(time_arg("1700000000000", now), "1700000000000");
        assert_eq!(
            time_arg("2026-11-27T00:00:00Z", now),
            "2026-11-27T00:00:00Z"
        );
    }

    #[test]
    fn partial_pages_name_the_silent_members() {
        let body = serde_json::json!({
            "records": [],
            "cursor": "a1.1-4",
            "partial_nodes": [3],
        });
        assert_eq!(
            render_page(&body),
            "no audit records\npartial: nodes 3 did not answer\nmore: --cursor a1.1-4\n"
        );
    }
}
