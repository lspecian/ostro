//! `ostro migrate-runs --workflow W --from V|sha --to V [--filter ...] [--limit N]
//! [--dry-run]` (D-P13-21): `POST /v1/runs/migrate` (13-17's server side). 13-02's clap
//! shape, 13-16's verb: `--json` prints the report, human output the counts, the failed runs
//! and the runs to retry. Exit codes: 0, 4 when a run diverged or errored (as `ostro
//! replay`), 1 API error (404 unknown version, 409 `module_retired`, 412
//! `feature_not_enabled`), 2 usage.

use clap::Args;
use ostro_client::cluster::{MigrateRunsReport, MigrateRunsRequest, VersionOrSha};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, table};

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
    /// Run tag filter (`tag=x` or `x`, repeatable; every tag required).
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

/// The request of `ostro migrate-runs`: `--from` is a version when numeric, else a module
/// sha; `--filter` values are tags (`tag=x` or `x`, all required; the server matches the
/// comma-separated list).
pub fn request(a: &MigrateRunsArgs) -> Result<MigrateRunsRequest, CallError> {
    let mut tags = Vec::new();
    for f in &a.filter {
        let tag = match f.split_once('=') {
            Some(("tag", t)) => t,
            Some((k, _)) => {
                return Err(CallError::Usage(format!(
                    "--filter {f}: only tag filters are supported (`--filter tag=x`), not `{k}`"
                )));
            }
            None => f.as_str(),
        };
        if tag.is_empty() || tag.contains(',') {
            return Err(CallError::Usage(format!("--filter {f}: bad tag")));
        }
        tags.push(tag.to_owned());
    }
    Ok(MigrateRunsRequest {
        workflow: a.workflow.clone(),
        from: VersionOrSha::parse(&a.from),
        to_version: a.to,
        filter: (!tags.is_empty()).then(|| tags.join(",")),
        limit: a.limit,
        dry_run: a.dry_run,
    })
}

/// Run `ostro migrate-runs`; returns the stdout text and the outcome (exit 4, as `ostro
/// replay`, when a run diverged or errored; 0 otherwise).
pub async fn render(
    c: &Client,
    json: bool,
    a: &MigrateRunsArgs,
) -> Result<(String, Outcome), CallError> {
    let req = request(a)?;
    let body = serde_json::to_value(&req)
        .map_err(|e| CallError::Usage(format!("migrate-runs request: {e}")))?;
    let r = c.migrate_runs(&body).await?;
    let report: Option<MigrateRunsReport> = serde_json::from_value(r.body.clone()).ok();
    let outcome = if report.as_ref().is_some_and(|m| !m.failed.is_empty()) {
        Outcome::Diverged
    } else {
        Outcome::Ok
    };
    if json {
        return Ok((pretty(&r.body), outcome));
    }
    let Some(m) = report else {
        return Ok((pretty(&r.body), outcome));
    };
    let from = match &req.from {
        VersionOrSha::Version(v) => format!("version {v}"),
        VersionOrSha::Sha(s) => format!("module {s}"),
    };
    let mut out = format!(
        "migrate-runs {} {from} -> version {}{}\npassed {}, repinned {}, failed {}, busy {}, seq mismatch {}",
        req.workflow,
        req.to_version,
        if req.dry_run { " (dry run)" } else { "" },
        m.passed.len(),
        m.repinned.len(),
        m.failed.len(),
        m.busy.len(),
        m.seq_mismatch.len(),
    );
    if !m.failed.is_empty() {
        let rows: Vec<Vec<String>> = m
            .failed
            .iter()
            .map(|f| vec![f.run_id.clone(), f.seq.to_string(), f.detail.clone()])
            .collect();
        out.push_str("\n\n");
        out.push_str(&table(&["RUN", "SEQ", "DETAIL"], &rows));
    }
    if !m.busy.is_empty() {
        out.push_str(&format!(
            "\n\nbusy (leased, retry later): {}",
            m.busy.join(", ")
        ));
    }
    if !m.seq_mismatch.is_empty() {
        out.push_str(&format!(
            "\n\nprogressed since the check (run --dry-run again): {}",
            m.seq_mismatch.join(", ")
        ));
    }
    Ok((out, outcome))
}

fn pretty(v: &serde_json::Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

/// `ostro migrate-runs`, printing its output.
pub async fn run(c: &Client, out: Out, a: &MigrateRunsArgs) -> Result<Outcome, CallError> {
    let (text, outcome) = render(c, out.json, a).await?;
    println!("{text}");
    Ok(outcome)
}
