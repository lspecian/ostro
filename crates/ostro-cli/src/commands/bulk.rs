//! `ostro bulk ...` (11-14, OPS-05): rate-limited bulk jobs over a run selection.
//!
//! - `ostro bulk <cancel|terminate|retry|rerun|requeue|pause|resume|signal> [selection]
//!   [--rate N] [--max-in-flight N] [--dry-run]` creates a job (`POST /v1/bulk`); a dry
//!   run prints the estimated match count and a sample of matched runs.
//! - `ostro bulk ls`, `ostro bulk status <job> [--watch]`, `ostro bulk cancel <job>`.
//!
//! `bulk cancel` is both an action and the job-cancel verb: with a job id (and no
//! selection) it cancels that job; otherwise it creates a job cancelling the selected runs.
//! `bulk status --watch` exits 0 when the job ends `done` and 3 when it ends `cancelled`
//! or `failed` (the `--wait` convention).

use std::time::Duration;

use clap::{Args, Subcommand};
use serde_json::{Map, Value, json};

use super::dlq::{ModeArg, mode_value, ms_time};
use super::rerun::{CutArgs, parse_version};
use super::{Conn, StatusArg, parse_duration, parse_json_arg, report};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// Job states after which nothing changes.
const TERMINAL: [&str; 3] = ["done", "cancelled", "failed"];

/// `ostro bulk`.
#[derive(Debug, Clone, Subcommand)]
pub enum BulkCommand {
    /// Cancel the selected runs, or (`bulk cancel <JOB>`) cancel a bulk job.
    Cancel(BulkCancelArgs),
    /// Terminate the selected runs.
    Terminate(BulkTerminateArgs),
    /// Retry the selected failed runs (resume in place or restart as a new linked run).
    Retry(BulkPlainArgs),
    /// Rerun the selected runs from a cut.
    Rerun(BulkRerunArgs),
    /// Requeue the selected DLQ members.
    Requeue(BulkRequeueArgs),
    /// Pause the selected runs.
    Pause(BulkPlainArgs),
    /// Resume the selected paused runs.
    Resume(BulkPlainArgs),
    /// Send a signal to the selected runs.
    Signal(BulkSignalArgs),
    /// List bulk jobs.
    Ls(BulkLsArgs),
    /// Show (or `--watch`) a bulk job's progress.
    Status(BulkStatusArgs),
}

impl BulkCommand {
    /// The connection flags of the subcommand.
    pub fn conn(&self) -> &Conn {
        match self {
            BulkCommand::Cancel(a) => &a.conn,
            BulkCommand::Terminate(a) => &a.conn,
            BulkCommand::Retry(a) | BulkCommand::Pause(a) | BulkCommand::Resume(a) => &a.conn,
            BulkCommand::Rerun(a) => &a.conn,
            BulkCommand::Requeue(a) => &a.conn,
            BulkCommand::Signal(a) => &a.conn,
            BulkCommand::Ls(a) => &a.conn,
            BulkCommand::Status(a) => &a.conn,
        }
    }
}

/// Run selection and job options of a bulk action.
#[derive(Debug, Clone, Default, Args)]
#[group(id = "select", multiple = true)]
pub struct BulkSelect {
    /// Tag filter (repeatable; every tag must match).
    #[arg(long = "tag")]
    pub tags: Vec<String>,
    /// Status filter.
    #[arg(long, value_enum)]
    pub status: Option<StatusArg>,
    /// Workflow filter.
    #[arg(long)]
    pub workflow: Option<String>,
    /// Queue filter.
    #[arg(long)]
    pub queue: Option<String>,
    /// Only DLQ members.
    #[arg(long)]
    pub dlq: bool,
    /// Operations per second (server default `[bulk] default_rate`, capped at `max_rate`).
    #[arg(long, value_name = "N")]
    pub rate: Option<u32>,
    /// Concurrent operations (server default `[bulk] max_in_flight`).
    #[arg(long, value_name = "N")]
    pub max_in_flight: Option<u32>,
    /// Count and sample the matches only; create no job.
    #[arg(long)]
    pub dry_run: bool,
}

impl BulkSelect {
    /// The REST body for `action`.
    pub fn body(&self, action: Value) -> Value {
        let mut f = Map::new();
        if let Some(w) = &self.workflow {
            f.insert("workflow".into(), json!(w));
        }
        if let Some(st) = self.status {
            f.insert("status".into(), json!(st.as_str()));
        }
        if let Some(q) = &self.queue {
            f.insert("queue".into(), json!(q));
        }
        if !self.tags.is_empty() {
            f.insert("tags".into(), json!(self.tags));
        }
        if self.dlq {
            f.insert("dlq".into(), json!(true));
        }
        let mut b = Map::new();
        b.insert("action".into(), action);
        b.insert("filter".into(), Value::Object(f));
        if let Some(r) = self.rate {
            b.insert("rate_per_sec".into(), json!(r));
        }
        if let Some(m) = self.max_in_flight {
            b.insert("max_in_flight".into(), json!(m));
        }
        if self.dry_run {
            b.insert("dry_run".into(), json!(true));
        }
        Value::Object(b)
    }
}

/// `ostro bulk cancel [JOB]`.
#[derive(Debug, Clone, Args)]
pub struct BulkCancelArgs {
    /// Bulk job id (`blk_...`) to cancel; without it, cancel the selected runs.
    #[arg(conflicts_with = "select")]
    pub job: Option<String>,
    #[command(flatten)]
    pub select: BulkSelect,
    #[command(flatten)]
    pub conn: Conn,
}

/// A bulk action without parameters (`retry`, `pause`, `resume`).
#[derive(Debug, Clone, Args)]
pub struct BulkPlainArgs {
    #[command(flatten)]
    pub select: BulkSelect,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro bulk terminate`.
#[derive(Debug, Clone, Args)]
pub struct BulkTerminateArgs {
    /// Operator reason.
    #[arg(long)]
    pub reason: String,
    #[command(flatten)]
    pub select: BulkSelect,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro bulk rerun`.
#[derive(Debug, Clone, Args)]
#[command(group(clap::ArgGroup::new("cut").required(true).args(["from_step", "from_seq"])))]
pub struct BulkRerunArgs {
    #[command(flatten)]
    pub cut: CutArgs,
    /// Version to execute: `active` (server default), `source` or a number.
    #[arg(long, value_name = "active|source|N", value_parser = parse_version)]
    pub version: Option<Value>,
    #[command(flatten)]
    pub select: BulkSelect,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro bulk requeue`.
#[derive(Debug, Clone, Args)]
pub struct BulkRequeueArgs {
    /// How to requeue each member.
    #[arg(long, value_enum, default_value = "auto")]
    pub mode: ModeArg,
    #[command(flatten)]
    pub cut: CutArgs,
    #[command(flatten)]
    pub select: BulkSelect,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro bulk signal`.
#[derive(Debug, Clone, Args)]
pub struct BulkSignalArgs {
    /// Signal name.
    #[arg(long)]
    pub name: String,
    /// Payload: inline JSON or `@file` (default null).
    #[arg(long, value_parser = parse_json_arg)]
    pub payload: Option<Value>,
    #[command(flatten)]
    pub select: BulkSelect,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro bulk ls`.
#[derive(Debug, Clone, Args)]
pub struct BulkLsArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro bulk status`.
#[derive(Debug, Clone, Args)]
pub struct BulkStatusArgs {
    /// Bulk job id (`blk_...`).
    pub job: String,
    /// Poll until the job is done, cancelled or failed.
    #[arg(long)]
    pub watch: bool,
    /// Poll interval of `--watch`.
    #[arg(long, default_value = "1s", value_parser = parse_duration, requires = "watch")]
    pub interval: Duration,
    #[command(flatten)]
    pub conn: Conn,
}

/// The REST body of a create verb, or `None` for `ls` / `status` / `cancel <JOB>`.
pub fn create_body(cmd: &BulkCommand) -> Result<Option<Value>, CallError> {
    Ok(Some(match cmd {
        BulkCommand::Cancel(a) if a.job.is_some() => return Ok(None),
        BulkCommand::Cancel(a) => a.select.body(json!("cancel")),
        BulkCommand::Terminate(a) => a
            .select
            .body(json!({ "terminate": { "reason": a.reason } })),
        BulkCommand::Retry(a) => a.select.body(json!("retry")),
        BulkCommand::Pause(a) => a.select.body(json!("pause")),
        BulkCommand::Resume(a) => a.select.body(json!("resume")),
        BulkCommand::Rerun(a) => {
            let mut r = Map::new();
            r.insert("from".into(), a.cut.from().unwrap_or(Value::Null));
            if let Some(v) = &a.version {
                r.insert("version".into(), v.clone());
            }
            a.select.body(json!({ "rerun": r }))
        }
        BulkCommand::Requeue(a) => a
            .select
            .body(json!({ "requeue": { "mode": mode_value(a.mode, &a.cut)? } })),
        BulkCommand::Signal(a) => a.select.body(json!({ "signal": {
            "name": a.name,
            "payload": a.payload.clone().unwrap_or(Value::Null),
        }})),
        BulkCommand::Ls(_) | BulkCommand::Status(_) => return Ok(None),
    }))
}

fn action_name(action: &Value) -> String {
    match action {
        Value::String(s) => s.clone(),
        Value::Object(m) => m.keys().next().cloned().unwrap_or_default(),
        _ => "-".to_owned(),
    }
}

/// Dispatch `ostro bulk ...`.
pub async fn run(c: &Client, out: Out, cmd: &BulkCommand) -> Result<Outcome, CallError> {
    if let Some(body) = create_body(cmd)? {
        return create(c, out, &body).await;
    }
    match cmd {
        BulkCommand::Cancel(BulkCancelArgs { job: Some(job), .. }) => cancel_job(c, out, job).await,
        BulkCommand::Ls(_) => ls(c, out).await,
        BulkCommand::Status(a) => status(c, out, a).await,
        _ => Ok(Outcome::Ok),
    }
}

async fn create(c: &Client, out: Out, body: &Value) -> Result<Outcome, CallError> {
    let r = match c.create_bulk(body).await {
        Ok(r) => r,
        Err(e) => return Ok(report(out, &e)),
    };
    let b = &r.body;
    if out.json {
        out.json(b);
        return Ok(Outcome::Ok);
    }
    let action = action_name(&body["action"]);
    let est = s(&b["matched_estimate"]);
    match b["job_id"].as_str() {
        Some(job) => println!("started bulk job {job} ({action}, about {est} runs matched)"),
        None => {
            println!("dry run: bulk {action} would match about {est} runs (matched_estimate)");
            let sample: Vec<String> = b["sample"]
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or_default()
                .iter()
                .map(s)
                .collect();
            if !sample.is_empty() {
                println!("sample:");
                for id in sample {
                    println!("  {id}");
                }
            }
        }
    }
    Ok(Outcome::Ok)
}

async fn cancel_job(c: &Client, out: Out, job: &str) -> Result<Outcome, CallError> {
    let r = c.cancel_bulk(job).await?;
    if out.json {
        out.json(&r.body);
    } else {
        println!("bulk job {job}: {}", s(&r.body["state"]));
    }
    Ok(Outcome::Ok)
}

async fn ls(c: &Client, out: Out) -> Result<Outcome, CallError> {
    let r = c.list_bulk().await?;
    if out.json {
        out.json(&r.body);
        return Ok(Outcome::Ok);
    }
    let jobs = r.body["jobs"].as_array().cloned().unwrap_or_default();
    if jobs.is_empty() {
        println!("no bulk jobs");
        return Ok(Outcome::Ok);
    }
    let rows: Vec<Vec<String>> = jobs
        .iter()
        .map(|j| {
            vec![
                s(&j["job_id"]),
                s(&j["state"]),
                s(&j["action"]),
                s(&j["matched"]),
                s(&j["done"]),
                s(&j["skipped"]),
                s(&j["failed_count"]),
                s(&j["rate_per_sec"]),
                ms_time(&j["created_at_ms"]),
            ]
        })
        .collect();
    println!(
        "{}",
        table(
            &[
                "JOB", "STATE", "ACTION", "MATCHED", "DONE", "SKIPPED", "FAILED", "RATE", "CREATED"
            ],
            &rows
        )
    );
    Ok(Outcome::Ok)
}

/// One progress line of a job view.
fn progress_line(j: &Value) -> String {
    format!(
        "{} {}: {}, {}/{} done, {} skipped, {} failed",
        s(&j["job_id"]),
        s(&j["action"]),
        s(&j["state"]),
        s(&j["done"]),
        s(&j["matched"]),
        s(&j["skipped"]),
        s(&j["failed_count"]),
    )
}

fn print_job(j: &Value) {
    let eta = j["eta_ms"]
        .as_u64()
        .map(|m| format!("{m} ms"))
        .unwrap_or_else(|| "-".into());
    let lines = [
        ("job", s(&j["job_id"])),
        ("action", s(&j["action"])),
        ("state", s(&j["state"])),
        ("matched", s(&j["matched"])),
        ("done", s(&j["done"])),
        ("skipped", s(&j["skipped"])),
        ("failed", s(&j["failed_count"])),
        ("rate", format!("{}/s", s(&j["rate_per_sec"]))),
        ("created", ms_time(&j["created_at_ms"])),
        ("started", ms_time(&j["started_at_ms"])),
        ("finished", ms_time(&j["finished_at_ms"])),
        ("eta", eta),
    ];
    for (k, v) in lines {
        println!("{k:<9} {v}");
    }
    let failures = j["failed"].as_array().cloned().unwrap_or_default();
    if !failures.is_empty() {
        let rows: Vec<Vec<String>> = failures
            .iter()
            .map(|f| vec![s(&f["run_id"]), s(&f["reason"])])
            .collect();
        println!("{}", table(&["FAILED RUN", "REASON"], &rows));
    }
}

fn is_terminal(j: &Value) -> bool {
    j["state"].as_str().is_some_and(|st| TERMINAL.contains(&st))
}

async fn status(c: &Client, out: Out, a: &BulkStatusArgs) -> Result<Outcome, CallError> {
    let mut j = c.get_bulk(&a.job).await?.body;
    if a.watch {
        let mut last = String::new();
        while !is_terminal(&j) {
            if !out.json {
                let line = progress_line(&j);
                if line != last {
                    println!("{line}");
                    last = line;
                }
            }
            tokio::time::sleep(a.interval).await;
            j = c.get_bulk(&a.job).await?.body;
        }
    }
    if out.json {
        out.json(&j);
    } else {
        print_job(&j);
    }
    if a.watch && j["state"] != "done" {
        return Ok(Outcome::NotCompleted);
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    fn bulk(args: &[&str]) -> Result<BulkCommand, clap::Error> {
        let mut v = vec!["bulk"];
        v.extend_from_slice(args);
        parse(&v).map(|c| match c {
            ClientCommand::Bulk { command } => command,
            other => panic!("{other:?}"),
        })
    }

    fn body(args: &[&str]) -> Value {
        create_body(&bulk(args).unwrap()).unwrap().expect("create")
    }

    #[test]
    fn create_bodies() {
        assert_eq!(
            body(&[
                "cancel",
                "--tag",
                "t",
                "--tag",
                "u",
                "--status",
                "failed",
                "--workflow",
                "w",
                "--queue",
                "q",
                "--dlq",
                "--rate",
                "5",
                "--max-in-flight",
                "2",
                "--dry-run",
            ]),
            json!({
                "action": "cancel",
                "filter": {"workflow": "w", "status": "failed", "queue": "q",
                    "tags": ["t", "u"], "dlq": true},
                "rate_per_sec": 5,
                "max_in_flight": 2,
                "dry_run": true,
            })
        );
        assert_eq!(
            body(&["terminate", "--reason", "r", "--tag", "t"]),
            json!({"action": {"terminate": {"reason": "r"}}, "filter": {"tags": ["t"]}})
        );
        for a in ["retry", "pause", "resume"] {
            assert_eq!(body(&[a])["action"], a);
        }
        assert_eq!(
            body(&[
                "rerun",
                "--tag",
                "t",
                "--from-step",
                "s3",
                "--version",
                "active"
            ])["action"],
            json!({"rerun": {"from": {"step": {"name": "s3", "occurrence": 1}}, "version": "active"}})
        );
        assert_eq!(
            body(&["requeue", "--dlq", "--mode", "restart"])["action"],
            json!({"requeue": {"mode": "restart"}})
        );
        assert_eq!(
            body(&["signal", "--name", "go", "--payload", "{}"])["action"],
            json!({"signal": {"name": "go", "payload": {}}})
        );
        assert_eq!(
            body(&["signal", "--name", "go"])["action"],
            json!({"signal": {"name": "go", "payload": null}})
        );
    }

    #[test]
    fn job_verbs_and_usage_errors() {
        let cmd = bulk(&["cancel", "blk_1"]).unwrap();
        assert!(create_body(&cmd).unwrap().is_none());
        assert!(create_body(&bulk(&["ls"]).unwrap()).unwrap().is_none());
        let BulkCommand::Status(a) =
            bulk(&["status", "blk_1", "--watch", "--interval", "50ms"]).unwrap()
        else {
            panic!()
        };
        assert!(a.watch);
        assert_eq!(a.interval, Duration::from_millis(50));
        for args in [
            &["cancel", "blk_1", "--tag", "t"][..],
            &["cancel", "blk_1", "--dry-run"],
            &["rerun", "--tag", "t"],
            &["terminate", "--tag", "t"],
            &["signal", "--tag", "t"],
            &["status"],
            &["status", "blk_1", "--interval", "1s"],
            &["bogus"],
        ] {
            assert_eq!(bulk(args).unwrap_err().exit_code(), 2, "{args:?}");
        }
        let cmd = bulk(&["requeue", "--mode", "rerun"]).unwrap();
        assert!(matches!(create_body(&cmd), Err(CallError::Usage(_))));
    }
}
