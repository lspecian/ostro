//! `ostro run`, `ls`, `inspect`, `history`.

use std::time::Duration;

use clap::{Args, ValueEnum};
use serde_json::{Map, Value, json};

use super::{Conn, StatusArg, parse_duration, parse_duration_text, parse_json_arg};
use crate::client::{CallError, Client, MAX_POLL};
use crate::output::{Out, Outcome, compact, s, table};

/// Idempotency-key collision policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ConflictArg {
    /// Fail with 409 and the existing run id.
    Reject,
    /// Return the existing run (default on the server).
    Return,
}

/// `ostro run`.
#[derive(Debug, Clone, Args)]
pub struct RunArgs {
    /// Workflow name (bare, or `tenant/name`).
    pub workflow: String,
    /// Input: inline JSON or `@file`.
    #[arg(long, value_parser = parse_json_arg)]
    pub input: Option<Value>,
    /// Explicit version (default: the active one).
    #[arg(long)]
    pub version: Option<u32>,
    /// Queue override.
    #[arg(long)]
    pub queue: Option<String>,
    /// Tenant (default: the default tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    /// Idempotency key.
    #[arg(long)]
    pub idempotency_key: Option<String>,
    /// What to do when the idempotency key was already used.
    #[arg(long, value_enum)]
    pub conflict: Option<ConflictArg>,
    /// Delay before the run becomes ready (e.g. 30s).
    #[arg(long, value_parser = parse_duration)]
    pub delay: Option<Duration>,
    /// Tag (repeatable), e.g. `--tag env=prod`.
    #[arg(long = "tag")]
    pub tags: Vec<String>,
    /// Schedule-to-start timeout (e.g. 30s; days as hours).
    #[arg(long, value_parser = parse_duration_text)]
    pub schedule_to_start: Option<String>,
    /// Retention of the terminal run (e.g. 168h; days as hours).
    #[arg(long, value_parser = parse_duration_text)]
    pub retention: Option<String>,
    /// Wait for the result (optionally at most DUR, e.g. `--wait=2m`).
    #[arg(long, value_name = "DUR", num_args = 0..=1, require_equals = true, value_parser = parse_duration)]
    pub wait: Option<Option<Duration>>,
    /// Validate only: the server runs the real checks and starts nothing.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl RunArgs {
    /// The REST start body.
    pub fn body(&self) -> Value {
        let mut b = Map::new();
        if let Some(i) = &self.input {
            b.insert("input".into(), i.clone());
        }
        if let Some(v) = self.version {
            b.insert("version".into(), json!(v));
        }
        if let Some(q) = &self.queue {
            b.insert("queue".into(), json!(q));
        }
        if let Some(t) = &self.tenant {
            b.insert("tenant".into(), json!(t));
        }
        if let Some(k) = &self.idempotency_key {
            b.insert("idempotency_key".into(), json!(k));
        }
        if let Some(c) = self.conflict {
            let p = match c {
                ConflictArg::Reject => "reject",
                ConflictArg::Return => "return_existing",
            };
            b.insert("conflict_policy".into(), json!(p));
        }
        if let Some(d) = self.delay {
            b.insert(
                "delay_ms".into(),
                json!(u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            );
        }
        if !self.tags.is_empty() {
            b.insert("tags".into(), json!(self.tags));
        }
        if let Some(s) = &self.schedule_to_start {
            b.insert("schedule_to_start_timeout".into(), json!(s));
        }
        if let Some(r) = &self.retention {
            b.insert("retention".into(), json!(r));
        }
        if self.dry_run {
            b.insert("dry_run".into(), json!(true));
        }
        Value::Object(b)
    }
}

/// `ostro run`.
pub async fn run(c: &Client, out: Out, a: &RunArgs) -> Result<Outcome, CallError> {
    let start = c.start_run(&a.workflow, &a.body()).await?;
    let b = &start.body;
    let run_id = b["run_id"].as_str().unwrap_or_default().to_owned();
    let created = b["created"].as_bool().unwrap_or(false);
    let dry_run = b["dry_run"].as_bool().unwrap_or(false);

    let Some(limit) = a.wait.filter(|_| !dry_run) else {
        if out.json {
            out.json(b);
        } else if dry_run {
            if created {
                println!("dry run: a new run of {} would be started", a.workflow);
            } else {
                println!("dry run: the existing run {run_id} would be returned");
            }
        } else if created {
            println!("started run {run_id} ({})", a.workflow);
        } else {
            println!("existing run {run_id} returned (idempotency key reused)");
        }
        return Ok(Outcome::Ok);
    };

    if !out.json {
        if created {
            println!("started run {run_id} ({}), waiting...", a.workflow);
        } else {
            println!("existing run {run_id} returned, waiting...");
        }
    }
    let result = wait_result(c, &run_id, limit, false).await?;
    if out.json {
        out.json(&json!({ "start": start.body, "result": result }));
    } else {
        print_result(&result);
    }
    Ok(completed_outcome(&result))
}

/// Long-poll `run_id` until it is terminal, or until `limit` (None = no limit) has
/// passed; returns the last result body. Each poll waits at most [`MAX_POLL`] (the
/// server's cap).
pub async fn wait_result(
    c: &Client,
    run_id: &str,
    limit: Option<Duration>,
    follow: bool,
) -> Result<Value, CallError> {
    let deadline = limit.map(|d| std::time::Instant::now() + d);
    loop {
        let wait = match deadline {
            Some(d) => d.saturating_duration_since(std::time::Instant::now()),
            None => MAX_POLL,
        };
        let r = c.result_once(run_id, wait, follow).await?;
        let done = r.body["completed"].as_bool().unwrap_or(false);
        let expired = deadline.is_some_and(|d| std::time::Instant::now() >= d);
        if done || expired {
            return Ok(r.body);
        }
    }
}

/// Human rendering of a result body.
pub fn print_result(b: &Value) {
    let run = s(&b["run_id"]);
    let status = s(&b["status"]);
    if b["completed"].as_bool() != Some(true) {
        println!("run {run} is {status} (not finished)");
    } else if !b["result"].is_null() || status == "completed" {
        println!("run {run} {status}: {}", compact(&b["result"]));
    } else if !b["failure"].is_null() {
        println!("run {run} {status}: {}", compact(&b["failure"]));
    } else {
        println!("run {run} {status}");
    }
}

/// 0 when the run completed, 3 otherwise (failed, cancelled, still running, ...).
pub fn completed_outcome(result: &Value) -> Outcome {
    if result["status"] == "completed" {
        Outcome::Ok
    } else {
        Outcome::NotCompleted
    }
}

/// `ostro ls`.
#[derive(Debug, Clone, Args)]
pub struct LsArgs {
    /// Workflow filter.
    #[arg(long)]
    pub workflow: Option<String>,
    /// Status filter.
    #[arg(long, value_enum)]
    pub status: Option<StatusArg>,
    /// Queue filter.
    #[arg(long)]
    pub queue: Option<String>,
    /// Tag filter.
    #[arg(long)]
    pub tag: Option<String>,
    /// Page size (server default 100, max 1000).
    #[arg(long)]
    pub limit: Option<u32>,
    /// Page token from a previous listing.
    #[arg(long)]
    pub page: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

impl LsArgs {
    /// The REST query pairs.
    pub fn query(&self) -> Vec<(&'static str, String)> {
        let mut q = Vec::new();
        if let Some(w) = &self.workflow {
            q.push(("workflow", w.clone()));
        }
        if let Some(st) = self.status {
            q.push(("status", st.as_str().to_owned()));
        }
        if let Some(x) = &self.queue {
            q.push(("queue", x.clone()));
        }
        if let Some(t) = &self.tag {
            q.push(("tag", t.clone()));
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

/// `ostro ls`.
pub async fn ls(c: &Client, out: Out, a: &LsArgs) -> Result<Outcome, CallError> {
    let r = c.list_runs(&a.query()).await?;
    if out.json {
        out.json(&r.body);
        return Ok(Outcome::Ok);
    }
    let runs = r.body["runs"].as_array().cloned().unwrap_or_default();
    if runs.is_empty() {
        println!("no runs");
    } else {
        let rows: Vec<Vec<String>> = runs
            .iter()
            .map(|r| {
                vec![
                    s(&r["run_id"]),
                    s(&r["workflow"]),
                    s(&r["version"]),
                    s(&r["status"]),
                    s(&r["queue"]),
                    s(&r["created_at"]),
                ]
            })
            .collect();
        println!(
            "{}",
            table(
                &[
                    "RUN ID", "WORKFLOW", "VERSION", "STATUS", "QUEUE", "CREATED"
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

/// `ostro inspect`.
#[derive(Debug, Clone, Args)]
pub struct InspectArgs {
    /// Run id.
    pub run: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro inspect`.
pub async fn inspect(c: &Client, out: Out, a: &InspectArgs) -> Result<Outcome, CallError> {
    let r = c.get_run(&a.run).await?;
    if out.json {
        out.json(&r.body);
        return Ok(Outcome::Ok);
    }
    let b = &r.body;
    let ms = |v: &Value| {
        v.as_u64()
            .map(|m| format!("{m} ms"))
            .unwrap_or_else(|| "-".into())
    };
    let tags = b["tags"]
        .as_array()
        .map(|t| t.iter().map(s).collect::<Vec<_>>().join(", "))
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "-".into());
    let lines = [
        ("run", s(&b["run_id"])),
        (
            "workflow",
            format!("{} v{}", s(&b["workflow"]), s(&b["version"])),
        ),
        ("status", s(&b["status"])),
        ("queue", s(&b["queue"])),
        ("tenant", s(&b["tenant"])),
        ("tags", tags),
        ("created", s(&b["created_at"])),
        ("completed", s(&b["completed_at"])),
        ("attempt", s(&b["attempt"])),
        ("retries", s(&b["attempt_failures"])),
        ("retention", ms(&b["retention_ms"])),
        ("schedule-to-start", ms(&b["schedule_to_start_ms"])),
        ("module", s(&b["module_sha256"])),
    ];
    for (k, v) in lines {
        println!("{k:<18} {v}");
    }
    if !b["published_status"].is_null() {
        println!(
            "{:<18} {}",
            "published status",
            compact(&b["published_status"])
        );
    }
    if !b["result"].is_null() {
        println!("{:<18} {}", "result", compact(&b["result"]));
    }
    if !b["failure"].is_null() {
        println!("{:<18} {}", "failure", s(&b["failure"]));
    }
    Ok(Outcome::Ok)
}

/// `ostro history`.
#[derive(Debug, Clone, Args)]
pub struct HistoryArgs {
    /// Run id.
    pub run: String,
    /// Page size (server default 100, max 1000).
    #[arg(long)]
    pub limit: Option<u32>,
    /// First seq to return.
    #[arg(long)]
    pub from_seq: Option<u64>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro history`.
pub async fn history(c: &Client, out: Out, a: &HistoryArgs) -> Result<Outcome, CallError> {
    let r = c.history(&a.run, a.from_seq, a.limit).await?;
    if out.json {
        out.json(&r.body);
        return Ok(Outcome::Ok);
    }
    let events = r.body["events"].as_array().cloned().unwrap_or_default();
    let rows: Vec<Vec<String>> = events
        .iter()
        .map(|e| vec![s(&e["seq"]), s(&e["ts"]), s(&e["kind"]), s(&e["summary"])])
        .collect();
    println!("{}", table(&["SEQ", "TIME", "KIND", "SUMMARY"], &rows));
    if let Some(n) = r.body["next_from_seq"].as_u64() {
        println!("more: --from-seq {n}");
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    #[test]
    fn run_body_maps_every_flag() {
        let ClientCommand::Run(a) = parse(&[
            "run",
            "acme/orders",
            "--input",
            r#"{"n":1}"#,
            "--version",
            "2",
            "--queue",
            "q",
            "--tenant",
            "acme",
            "--idempotency-key",
            "k1",
            "--conflict",
            "reject",
            "--delay",
            "1m 30s",
            "--tag",
            "env=prod",
            "--tag",
            "team=a",
            "--schedule-to-start",
            "30s",
            "--retention",
            "168h",
            "--dry-run",
        ])
        .unwrap() else {
            panic!("run")
        };
        assert_eq!(a.workflow, "acme/orders");
        assert_eq!(a.wait, None);
        assert_eq!(
            a.body(),
            json!({
                "input": {"n": 1},
                "version": 2,
                "queue": "q",
                "tenant": "acme",
                "idempotency_key": "k1",
                "conflict_policy": "reject",
                "delay_ms": 90_000,
                "tags": ["env=prod", "team=a"],
                "schedule_to_start_timeout": "30s",
                "retention": "168h",
                "dry_run": true,
            })
        );
    }

    #[test]
    fn run_minimal_body_and_wait_forms() {
        let ClientCommand::Run(a) = parse(&["run", "w"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.body(), json!({}));
        let ClientCommand::Run(a) = parse(&["run", "w", "--wait"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.wait, Some(None));
        let ClientCommand::Run(a) =
            parse(&["run", "w", "--wait=2m", "--conflict", "return"]).unwrap()
        else {
            panic!()
        };
        assert_eq!(a.wait, Some(Some(Duration::from_secs(120))));
        assert_eq!(a.body(), json!({"conflict_policy": "return_existing"}));
        // `--wait 5s` is not a wait value (require_equals): 5s is an extra positional.
        assert_eq!(
            parse(&["run", "w", "--wait", "5s"])
                .unwrap_err()
                .exit_code(),
            2
        );
        assert_eq!(
            parse(&["run", "w", "--conflict", "maybe"])
                .unwrap_err()
                .exit_code(),
            2
        );
        assert_eq!(
            parse(&["run", "w", "--input", "{"])
                .unwrap_err()
                .exit_code(),
            2
        );
    }

    #[test]
    fn ls_query() {
        let ClientCommand::Ls(a) = parse(&[
            "ls",
            "--workflow",
            "w",
            "--status",
            "timed_out",
            "--queue",
            "q",
            "--tag",
            "env=prod",
            "--limit",
            "5",
            "--page",
            "tok",
        ])
        .unwrap() else {
            panic!()
        };
        assert_eq!(
            a.query(),
            vec![
                ("workflow", "w".to_owned()),
                ("status", "timed_out".to_owned()),
                ("queue", "q".to_owned()),
                ("tag", "env=prod".to_owned()),
                ("limit", "5".to_owned()),
                ("page_token", "tok".to_owned()),
            ]
        );
        assert_eq!(
            parse(&["ls", "--status", "done"]).unwrap_err().exit_code(),
            2
        );
    }

    #[test]
    fn history_and_inspect_args() {
        let ClientCommand::History(a) =
            parse(&["history", "r1", "--limit", "10", "--from-seq", "3"]).unwrap()
        else {
            panic!()
        };
        assert_eq!(
            (a.run.as_str(), a.limit, a.from_seq),
            ("r1", Some(10), Some(3))
        );
        let ClientCommand::Inspect(a) = parse(&["inspect", "r1", "--json"]).unwrap() else {
            panic!()
        };
        assert!(a.conn.json);
    }

    #[test]
    fn completed_maps_to_exit_codes() {
        assert_eq!(
            completed_outcome(&json!({"status": "completed"})),
            Outcome::Ok
        );
        for st in ["failed", "cancelled", "terminated", "running", "timed_out"] {
            assert_eq!(
                completed_outcome(&json!({ "status": st })),
                Outcome::NotCompleted
            );
        }
    }
}
