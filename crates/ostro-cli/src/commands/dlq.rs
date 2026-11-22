//! `ostro dlq ls|requeue|dismiss` (11-14, OPS-04): the dead-letter view of failed and
//! timed-out runs.

use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};

use super::rerun::{CutArgs, refusal_details};
use super::{Conn, report};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// `ostro dlq`.
#[derive(Debug, Clone, Subcommand)]
pub enum DlqCommand {
    /// List DLQ entries (newest failures first).
    Ls(DlqLsArgs),
    /// Requeue an entry: resume in place, restart with the same input, or rerun from a cut.
    Requeue(RequeueArgs),
    /// Dismiss an entry (it leaves the default view; `dlq ls --all` still shows it).
    Dismiss(DismissArgs),
}

impl DlqCommand {
    /// The connection flags of the subcommand.
    pub fn conn(&self) -> &Conn {
        match self {
            DlqCommand::Ls(a) => &a.conn,
            DlqCommand::Requeue(a) => &a.conn,
            DlqCommand::Dismiss(a) => &a.conn,
        }
    }
}

/// `ostro dlq ls`.
#[derive(Debug, Clone, Args)]
pub struct DlqLsArgs {
    /// Workflow filter.
    #[arg(long)]
    pub workflow: Option<String>,
    /// Queue filter.
    #[arg(long)]
    pub queue: Option<String>,
    /// Failure reason prefix (e.g. `run_timeout`, `workflow_failed`).
    #[arg(long)]
    pub reason: Option<String>,
    /// Tag filter (repeatable; every tag must match).
    #[arg(long = "tag")]
    pub tags: Vec<String>,
    /// Include dismissed and requeued entries.
    #[arg(long)]
    pub all: bool,
    /// Page size (server default 100).
    #[arg(long)]
    pub limit: Option<u32>,
    /// Page token from a previous listing.
    #[arg(long)]
    pub page: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

impl DlqLsArgs {
    /// The REST query pairs (`tags` repeated).
    pub fn query(&self) -> Vec<(&'static str, String)> {
        let mut q = Vec::new();
        if let Some(w) = &self.workflow {
            q.push(("workflow", w.clone()));
        }
        if let Some(x) = &self.queue {
            q.push(("queue", x.clone()));
        }
        if let Some(r) = &self.reason {
            q.push(("reason", r.clone()));
        }
        for t in &self.tags {
            q.push(("tags", t.clone()));
        }
        if self.all {
            q.push(("include_dismissed", "true".to_owned()));
        }
        if let Some(l) = self.limit {
            q.push(("limit", l.to_string()));
        }
        if let Some(p) = &self.page {
            q.push(("page_token", p.clone()));
        }
        q
    }
}

/// Requeue mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ModeArg {
    /// Resume when the run is resumable, else restart (server default).
    Auto,
    /// Resume the run in place.
    Resume,
    /// Start a new run with the same input.
    Restart,
    /// Rerun from a cut (`--from-step` / `--from-seq`).
    Rerun,
}

/// The REST `mode` value; `rerun` needs a cut.
pub fn mode_value(mode: ModeArg, cut: &CutArgs) -> Result<Value, CallError> {
    let from = cut.from();
    match (mode, from) {
        (ModeArg::Rerun, Some(from)) => Ok(json!({ "rerun": { "from": from } })),
        (ModeArg::Rerun, None) => Err(CallError::Usage(
            "--mode rerun needs --from-step or --from-seq".to_owned(),
        )),
        (_, Some(_)) => Err(CallError::Usage(
            "--from-step / --from-seq apply only to --mode rerun".to_owned(),
        )),
        (ModeArg::Auto, None) => Ok(json!("auto")),
        (ModeArg::Resume, None) => Ok(json!("resume")),
        (ModeArg::Restart, None) => Ok(json!("restart")),
    }
}

/// `ostro dlq requeue`.
#[derive(Debug, Clone, Args)]
pub struct RequeueArgs {
    /// DLQ member (run id).
    pub run: String,
    /// How to requeue.
    #[arg(long, value_enum, default_value = "auto")]
    pub mode: ModeArg,
    #[command(flatten)]
    pub cut: CutArgs,
    /// Validate only: report the mode that would be used, requeue nothing.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl RequeueArgs {
    /// The REST body.
    pub fn body(&self) -> Result<Value, CallError> {
        let mut b = json!({ "mode": mode_value(self.mode, &self.cut)? });
        if self.dry_run {
            b["dry_run"] = json!(true);
        }
        Ok(b)
    }
}

/// `ostro dlq dismiss`.
#[derive(Debug, Clone, Args)]
pub struct DismissArgs {
    /// DLQ member (run id).
    pub run: String,
    /// Operator reason (at most 1024 bytes).
    #[arg(long)]
    pub reason: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// Milliseconds since the epoch as RFC 3339 (UTC, whole seconds, fixed width), or `-`.
pub fn ms_time(v: &Value) -> String {
    v.as_i64()
        .and_then(|ms| jiff::Timestamp::from_second(ms.div_euclid(1000)).ok())
        .map(|t| t.to_string())
        .unwrap_or_else(|| "-".to_owned())
}

/// The state column of a DLQ entry.
fn entry_state(e: &Value) -> String {
    if let Some(to) = e["requeued_to"].as_str() {
        if to == e["run_id"].as_str().unwrap_or_default() {
            "requeued (resumed)".to_owned()
        } else {
            format!("requeued -> {to}")
        }
    } else if !e["dismissed_at_ms"].is_null() {
        format!("dismissed: {}", s(&e["dismiss_reason"]))
    } else {
        "dlq".to_owned()
    }
}

/// Dispatch `ostro dlq ...`.
pub async fn run(c: &Client, out: Out, cmd: &DlqCommand) -> Result<Outcome, CallError> {
    match cmd {
        DlqCommand::Ls(a) => ls(c, out, a).await,
        DlqCommand::Requeue(a) => requeue(c, out, a).await,
        DlqCommand::Dismiss(a) => dismiss(c, out, a).await,
    }
}

async fn ls(c: &Client, out: Out, a: &DlqLsArgs) -> Result<Outcome, CallError> {
    let r = c.list_dlq(&a.query()).await?;
    if out.json {
        out.json(&r.body);
        return Ok(Outcome::Ok);
    }
    let entries = r.body["entries"].as_array().cloned().unwrap_or_default();
    if entries.is_empty() {
        println!("no DLQ entries");
    } else {
        let rows: Vec<Vec<String>> = entries
            .iter()
            .map(|e| {
                vec![
                    s(&e["run_id"]),
                    s(&e["workflow"]),
                    s(&e["version"]),
                    s(&e["status"]),
                    s(&e["queue"]),
                    s(&e["reason"]),
                    if e["resumable"].as_bool() == Some(true) {
                        "yes".to_owned()
                    } else {
                        "no".to_owned()
                    },
                    ms_time(&e["failed_at_ms"]),
                    entry_state(e),
                ]
            })
            .collect();
        println!(
            "{}",
            table(
                &[
                    "RUN ID",
                    "WORKFLOW",
                    "VERSION",
                    "STATUS",
                    "QUEUE",
                    "REASON",
                    "RESUMABLE",
                    "FAILED",
                    "STATE",
                ],
                &rows
            )
        );
    }
    if let Some(t) = r.body["next_page_token"].as_str() {
        println!("more: --page {t}");
    }
    Ok(Outcome::Ok)
}

async fn requeue(c: &Client, out: Out, a: &RequeueArgs) -> Result<Outcome, CallError> {
    let body = a.body()?;
    let r = match c.requeue_dlq(&a.run, &body).await {
        Ok(r) => r,
        Err(e) => {
            let o = report(out, &e);
            refusal_details(&e);
            return Ok(o);
        }
    };
    let b = &r.body;
    if out.json {
        out.json(b);
        return Ok(Outcome::Ok);
    }
    let mode = s(&b["mode"]);
    if a.dry_run {
        println!("dry run: {} would be requeued (mode {mode})", a.run);
    } else if let Some(new) = b["new_run_id"].as_str() {
        println!("requeued {} (mode {mode}): new run {new}", a.run);
    } else {
        println!("requeued {} (mode {mode}): resumed in place", a.run);
    }
    Ok(Outcome::Ok)
}

async fn dismiss(c: &Client, out: Out, a: &DismissArgs) -> Result<Outcome, CallError> {
    let r = c
        .dismiss_dlq(&a.run, &json!({ "reason": a.reason }))
        .await?;
    if out.json {
        out.json(&r.body);
    } else {
        println!("dismissed {} from the DLQ ({})", a.run, a.reason);
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    fn dlq(args: &[&str]) -> DlqCommand {
        let mut v = vec!["dlq"];
        v.extend_from_slice(args);
        match parse(&v).unwrap() {
            ClientCommand::Dlq { command } => command,
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn ls_query_repeats_tags() {
        let DlqCommand::Ls(a) = dlq(&[
            "ls",
            "--workflow",
            "w",
            "--queue",
            "q",
            "--reason",
            "run_timeout",
            "--tag",
            "a",
            "--tag",
            "b",
            "--all",
            "--limit",
            "5",
            "--page",
            "p",
        ]) else {
            panic!()
        };
        assert_eq!(
            a.query(),
            vec![
                ("workflow", "w".to_owned()),
                ("queue", "q".to_owned()),
                ("reason", "run_timeout".to_owned()),
                ("tags", "a".to_owned()),
                ("tags", "b".to_owned()),
                ("include_dismissed", "true".to_owned()),
                ("limit", "5".to_owned()),
                ("page_token", "p".to_owned()),
            ]
        );
    }

    #[test]
    fn requeue_bodies() {
        let DlqCommand::Requeue(a) = dlq(&["requeue", "r1"]) else {
            panic!()
        };
        assert_eq!(a.body().unwrap(), json!({"mode": "auto"}));
        let DlqCommand::Requeue(a) = dlq(&["requeue", "r1", "--mode", "restart", "--dry-run"])
        else {
            panic!()
        };
        assert_eq!(
            a.body().unwrap(),
            json!({"mode": "restart", "dry_run": true})
        );
        let DlqCommand::Requeue(a) =
            dlq(&["requeue", "r1", "--mode", "rerun", "--from-step", "s3"])
        else {
            panic!()
        };
        assert_eq!(
            a.body().unwrap(),
            json!({"mode": {"rerun": {"from": {"step": {"name": "s3", "occurrence": 1}}}}})
        );
        let DlqCommand::Requeue(a) = dlq(&["requeue", "r1", "--mode", "rerun"]) else {
            panic!()
        };
        assert!(matches!(a.body(), Err(CallError::Usage(_))));
        let DlqCommand::Requeue(a) = dlq(&["requeue", "r1", "--from-seq", "3"]) else {
            panic!()
        };
        assert!(matches!(a.body(), Err(CallError::Usage(_))));
    }

    #[test]
    fn dismiss_needs_a_reason() {
        assert_eq!(parse(&["dlq", "dismiss", "r1"]).unwrap_err().exit_code(), 2);
        let DlqCommand::Dismiss(a) = dlq(&["dismiss", "r1", "--reason", "flake"]) else {
            panic!()
        };
        assert_eq!(a.reason, "flake");
    }

    #[test]
    fn times_and_states() {
        assert_eq!(ms_time(&json!(0)), "1970-01-01T00:00:00Z");
        assert_eq!(ms_time(&json!(1_500)), "1970-01-01T00:00:01Z");
        assert_eq!(ms_time(&Value::Null), "-");
        assert_eq!(entry_state(&json!({"run_id": "r"})), "dlq");
        assert_eq!(
            entry_state(&json!({"run_id": "r", "dismissed_at_ms": 1, "dismiss_reason": "x"})),
            "dismissed: x"
        );
        assert_eq!(
            entry_state(&json!({"run_id": "r", "requeued_to": "r"})),
            "requeued (resumed)"
        );
        assert_eq!(
            entry_state(&json!({"run_id": "r", "requeued_to": "n"})),
            "requeued -> n"
        );
    }
}
