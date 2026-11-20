//! `ostro schedules {ls,get,create,update,delete,pause,resume,trigger}` (08-12, TRIG-01).
//!
//! Thin REST client over the 08-10 surface:
//!
//! | Verb | Request |
//! |---|---|
//! | `ls` | `GET /v1/schedules` |
//! | `get <name>` | `GET /v1/schedules/{name}` |
//! | `create --name ...` | `POST /v1/schedules` |
//! | `update <name> --version-tag T ...` | `GET /v1/schedules/{name}`, then `PUT /v1/schedules/{name}` with the given fields overlaid and `version_tag = T` (stale = 409) |
//! | `delete <name> [--version-tag T]` | `DELETE /v1/schedules/{name}[?version_tag=T]` |
//! | `pause` / `resume <name>` | `POST /v1/schedules/{name}/pause` / `.../resume` |
//! | `trigger <name> [--request-id]` | `POST /v1/schedules/{name}/trigger` -> `{run_id}` |
//!
//! `--json` prints the REST body verbatim; human output is a table (`ls`) or `key: value`
//! lines. A server whose schedule controller is still a stub (HTTP 503 "not yet
//! implemented") or that has the API disabled is reported as "not implemented on this
//! server" (see [`unimplemented_hint`]).

use clap::{Args, Subcommand, ValueEnum};
use reqwest::Method;
use serde_json::{Map, Value, json};

use super::{Conn, parse_duration, parse_json_arg};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, compact, s, table};

/// `ostro schedules ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum SchedulesCommand {
    /// List schedules.
    Ls(ListArgs),
    /// Show one schedule.
    Get(NameArgs),
    /// Create a schedule.
    Create(CreateArgs),
    /// Update a schedule (compare-and-set on --version-tag).
    Update(UpdateArgs),
    /// Delete a schedule.
    Delete(DeleteArgs),
    /// Pause a schedule (it stops firing).
    Pause(NameArgs),
    /// Resume a paused schedule.
    Resume(NameArgs),
    /// Fire a schedule now; prints the started run id.
    Trigger(TriggerArgs),
}

impl SchedulesCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            SchedulesCommand::Ls(a) => &a.conn,
            SchedulesCommand::Get(a) | SchedulesCommand::Pause(a) | SchedulesCommand::Resume(a) => {
                &a.conn
            }
            SchedulesCommand::Create(a) => &a.conn,
            SchedulesCommand::Update(a) => &a.conn,
            SchedulesCommand::Delete(a) => &a.conn,
            SchedulesCommand::Trigger(a) => &a.conn,
        }
    }
}

/// A listing verb (connection flags only).
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// A verb on one named record.
#[derive(Debug, Clone, Args)]
pub struct NameArgs {
    /// Name.
    pub name: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// A delete verb.
#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Name.
    pub name: String,
    /// Expected record version (compare-and-set; unset = unconditional).
    #[arg(long)]
    pub version_tag: Option<u64>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro schedules trigger`.
#[derive(Debug, Clone, Args)]
pub struct TriggerArgs {
    /// Schedule name.
    pub name: String,
    /// Idempotency of the manual fire (1..=64 bytes): a repeat returns the same run.
    #[arg(long)]
    pub request_id: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// Overlap policy (what a fire does while the previous run is still open).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum OverlapArg {
    /// Skip the fire.
    Skip,
    /// Keep at most one buffered fire.
    BufferOne,
    /// Start every fire.
    AllowAll,
    /// Cancel the open run, then start.
    CancelOther,
    /// Terminate the open run, then start.
    TerminateOther,
}

impl OverlapArg {
    /// REST spelling.
    pub fn as_wire(self) -> &'static str {
        match self {
            OverlapArg::Skip => "skip",
            OverlapArg::BufferOne => "buffer_one",
            OverlapArg::AllowAll => "allow_all",
            OverlapArg::CancelOther => "cancel_other",
            OverlapArg::TerminateOther => "terminate_other",
        }
    }
}

/// Catch-up policy (which missed fires start after downtime).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum CatchupArg {
    /// Start none.
    Skip,
    /// Start the most recent one.
    One,
    /// Start every missed fire within the window (capped).
    All,
}

impl CatchupArg {
    /// REST spelling.
    pub fn as_wire(self) -> &'static str {
        match self {
            CatchupArg::Skip => "skip",
            CatchupArg::One => "one",
            CatchupArg::All => "all",
        }
    }
}

/// Optional schedule fields shared by `create` and `update`.
#[derive(Debug, Clone, Default, Args)]
pub struct ScheduleOpts {
    /// Workflow version (unset = the active version at fire time).
    #[arg(long)]
    pub version: Option<u32>,
    /// IANA time zone (unset on create = UTC).
    #[arg(long)]
    pub timezone: Option<String>,
    /// Run input as inline JSON (or `@file`).
    #[arg(long, value_parser = parse_json_arg, conflicts_with = "input_file")]
    pub input_json: Option<Value>,
    /// Run input from a JSON file.
    #[arg(long, value_parser = parse_json_file)]
    pub input_file: Option<Value>,
    /// Overlap policy.
    #[arg(long, value_enum)]
    pub overlap: Option<OverlapArg>,
    /// Catch-up policy.
    #[arg(long, value_enum)]
    pub catchup: Option<CatchupArg>,
    /// How far back missed fires are considered (friendly duration, e.g. 1h).
    #[arg(long, value_parser = parse_duration_ms)]
    pub catchup_window: Option<u64>,
    /// No fire before this time (RFC 3339, or ms since the Unix epoch).
    #[arg(long, value_parser = parse_time_ms)]
    pub start_at: Option<u64>,
    /// No fire after this time (RFC 3339, or ms since the Unix epoch).
    #[arg(long, value_parser = parse_time_ms)]
    pub end_at: Option<u64>,
}

impl ScheduleOpts {
    fn overlay(&self, b: &mut Map<String, Value>) {
        if let Some(v) = self.version {
            b.insert("version".into(), json!(v));
        }
        if let Some(tz) = &self.timezone {
            b.insert("timezone".into(), json!(tz));
        }
        if let Some(i) = self.input_json.as_ref().or(self.input_file.as_ref()) {
            b.insert("input".into(), i.clone());
        }
        if let Some(o) = self.overlap {
            b.insert("overlap".into(), json!(o.as_wire()));
        }
        if let Some(c) = self.catchup {
            b.insert("catchup".into(), json!(c.as_wire()));
        }
        if let Some(w) = self.catchup_window {
            b.insert("catchup_window_ms".into(), json!(w));
        }
        if let Some(t) = self.start_at {
            b.insert("start_at_ms".into(), json!(t));
        }
        if let Some(t) = self.end_at {
            b.insert("end_at_ms".into(), json!(t));
        }
    }
}

/// `ostro schedules create`.
#[derive(Debug, Clone, Args)]
pub struct CreateArgs {
    /// Schedule name (`[A-Za-z0-9_.-]{1,128}`).
    #[arg(long)]
    pub name: String,
    /// Workflow to start.
    #[arg(long)]
    pub workflow: String,
    /// Cron expression (e.g. "0 */5 * * * *" or "@hourly").
    #[arg(long)]
    pub cron: String,
    /// Create the schedule paused.
    #[arg(long)]
    pub paused: bool,
    #[command(flatten)]
    pub opts: ScheduleOpts,
    #[command(flatten)]
    pub conn: Conn,
}

impl CreateArgs {
    /// The REST body of `POST /v1/schedules`.
    pub fn body(&self) -> Value {
        let mut b = Map::new();
        b.insert("name".into(), json!(self.name));
        b.insert("workflow".into(), json!(self.workflow));
        b.insert("cron".into(), json!(self.cron));
        if self.paused {
            b.insert("paused".into(), json!(true));
        }
        self.opts.overlay(&mut b);
        Value::Object(b)
    }
}

/// `ostro schedules update`.
#[derive(Debug, Clone, Args)]
pub struct UpdateArgs {
    /// Schedule name.
    pub name: String,
    /// The record version the update applies to (from `get`); a stale tag is rejected
    /// with HTTP 409.
    #[arg(long)]
    pub version_tag: u64,
    /// New workflow.
    #[arg(long)]
    pub workflow: Option<String>,
    /// New cron expression.
    #[arg(long)]
    pub cron: Option<String>,
    #[command(flatten)]
    pub opts: ScheduleOpts,
    #[command(flatten)]
    pub conn: Conn,
}

impl UpdateArgs {
    /// The REST body of `PUT /v1/schedules/{name}`: `current` (from GET) with the given
    /// fields overlaid, output-only fields dropped and `version_tag` set to the flag.
    pub fn body(&self, current: &Value) -> Value {
        let mut b = current.as_object().cloned().unwrap_or_default();
        b.remove("next_fire_at_ms");
        b.remove("last_fire_at_ms");
        b.insert("name".into(), json!(self.name));
        if let Some(w) = &self.workflow {
            b.insert("workflow".into(), json!(w));
        }
        if let Some(c) = &self.cron {
            b.insert("cron".into(), json!(c));
        }
        self.opts.overlay(&mut b);
        b.insert("version_tag".into(), json!(self.version_tag));
        Value::Object(b)
    }
}

// ---------------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------------

/// A JSON file (`--input-file`).
pub fn parse_json_file(s: &str) -> Result<Value, String> {
    parse_json_arg(&format!("@{s}"))
}

/// A friendly duration in milliseconds.
pub fn parse_duration_ms(s: &str) -> Result<u64, String> {
    let d = parse_duration(s)?;
    u64::try_from(d.as_millis()).map_err(|_| format!("duration {s:?} is too large"))
}

/// An RFC 3339 timestamp or integer milliseconds since the Unix epoch.
pub fn parse_time_ms(s: &str) -> Result<u64, String> {
    let t = s.trim();
    if let Ok(ms) = t.parse::<u64>() {
        return Ok(ms);
    }
    let ts: jiff::Timestamp = t.parse().map_err(|e| {
        format!("invalid time {s:?} (expected RFC 3339, e.g. 2026-01-01T00:00:00Z, or ms): {e}")
    })?;
    u64::try_from(ts.as_millisecond()).map_err(|_| format!("time {s:?} is before 1970"))
}

// ---------------------------------------------------------------------------------
// Shared Phase 8 rendering helpers (also used by `webhooks`)
// ---------------------------------------------------------------------------------

/// Pretty JSON, as `--json` prints it.
pub(crate) fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

/// Milliseconds since the epoch as RFC 3339 (`-` when absent).
pub(crate) fn fmt_ms(v: &Value) -> String {
    match v.as_u64().and_then(|ms| i64::try_from(ms).ok()) {
        Some(ms) => match jiff::Timestamp::from_millisecond(ms) {
            Ok(ts) => ts.to_string(),
            Err(_) => ms.to_string(),
        },
        None => "-".to_owned(),
    }
}

/// `key: value` lines, keys padded to one width.
pub(crate) fn kv(rows: &[(&str, String)]) -> String {
    let w = rows.iter().map(|(k, _)| k.len()).max().unwrap_or(0) + 1;
    rows.iter()
        .map(|(k, v)| format!("{:<w$} {v}", format!("{k}:")))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Map "this server does not implement it" answers to one clear message: HTTP 503
/// `unavailable` from a controller stub ("not yet implemented (08-NN)") and HTTP 404
/// "the ... API is not enabled on this server". The status and code are kept (so the
/// exit code stays 1); only the message is rewritten. Every other error is returned
/// unchanged.
pub fn unimplemented_hint(what: &str, e: CallError) -> CallError {
    match e {
        CallError::Api { status, mut body } => {
            let msg = body["error"]["message"].as_str().unwrap_or("").to_owned();
            let stub = status == 503 && msg.contains("not yet implemented");
            let disabled = status == 404 && msg.contains("is not enabled on this server");
            if (stub || disabled)
                && let Some(err) = body.get_mut("error").and_then(Value::as_object_mut)
            {
                err.insert(
                    "message".into(),
                    json!(format!("{what} are not implemented on this server ({msg})")),
                );
            }
            CallError::Api { status, body }
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------------
// Verbs
// ---------------------------------------------------------------------------------

fn path<'a>(name: &'a str, tail: &[&'a str]) -> Vec<&'a str> {
    let mut v = vec!["v1", "schedules", name];
    v.extend_from_slice(tail);
    v
}

fn detail(b: &Value) -> String {
    let version = match b["version"].as_u64() {
        Some(v) => v.to_string(),
        None => "active".to_owned(),
    };
    let tz = match b["timezone"].as_str() {
        Some("") | None => "UTC".to_owned(),
        Some(t) => t.to_owned(),
    };
    let window = match b["catchup_window_ms"].as_u64() {
        Some(ms) if ms > 0 => format!("{ms} ms"),
        _ => "server default".to_owned(),
    };
    kv(&[
        ("name", s(&b["name"])),
        (
            "workflow",
            format!("{} (version {version})", s(&b["workflow"])),
        ),
        ("cron", s(&b["cron"])),
        ("timezone", tz),
        ("paused", s(&b["paused"])),
        ("overlap", s(&b["overlap"])),
        ("catchup", format!("{} (window {window})", s(&b["catchup"]))),
        ("start_at", fmt_ms(&b["start_at_ms"])),
        ("end_at", fmt_ms(&b["end_at_ms"])),
        ("next_fire", fmt_ms(&b["next_fire_at_ms"])),
        ("last_fire", fmt_ms(&b["last_fire_at_ms"])),
        ("input", compact(&b["input"])),
        ("version_tag", s(&b["version_tag"])),
    ])
}

fn listing(b: &Value) -> String {
    let rows: Vec<Vec<String>> = b["schedules"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|x| {
            vec![
                s(&x["name"]),
                s(&x["workflow"]),
                s(&x["cron"]),
                match x["timezone"].as_str() {
                    Some("") | None => "UTC".to_owned(),
                    Some(t) => t.to_owned(),
                },
                s(&x["paused"]),
                s(&x["overlap"]),
                fmt_ms(&x["next_fire_at_ms"]),
                s(&x["version_tag"]),
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no schedules".to_owned();
    }
    table(
        &[
            "NAME",
            "WORKFLOW",
            "CRON",
            "TIMEZONE",
            "PAUSED",
            "OVERLAP",
            "NEXT_FIRE",
            "TAG",
        ],
        &rows,
    )
}

/// Run one verb and return what it prints on stdout (`--json`: the REST body).
pub async fn render(c: &Client, json: bool, cmd: &SchedulesCommand) -> Result<String, CallError> {
    render_inner(c, json, cmd)
        .await
        .map_err(|e| unimplemented_hint("schedules", e))
}

async fn render_inner(c: &Client, json: bool, cmd: &SchedulesCommand) -> Result<String, CallError> {
    let (head, body) = match cmd {
        SchedulesCommand::Ls(_) => {
            let r = c.call(Method::GET, &["v1", "schedules"], &[], None).await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(listing(&r.body));
        }
        SchedulesCommand::Get(a) => {
            let r = c.call(Method::GET, &path(&a.name, &[]), &[], None).await?;
            (None, r.body)
        }
        SchedulesCommand::Create(a) => {
            let r = c
                .call(Method::POST, &["v1", "schedules"], &[], Some(&a.body()))
                .await?;
            (Some(format!("schedule {} created", a.name)), r.body)
        }
        SchedulesCommand::Update(a) => {
            let p = path(&a.name, &[]);
            let cur = c.call(Method::GET, &p, &[], None).await?;
            let r = c
                .call(Method::PUT, &p, &[], Some(&a.body(&cur.body)))
                .await?;
            (Some(format!("schedule {} updated", a.name)), r.body)
        }
        SchedulesCommand::Delete(a) => {
            let q: Vec<(&str, String)> = a
                .version_tag
                .map(|t| ("version_tag", t.to_string()))
                .into_iter()
                .collect();
            let r = c
                .call(Method::DELETE, &path(&a.name, &[]), &q, None)
                .await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(format!("schedule {} deleted", a.name));
        }
        SchedulesCommand::Pause(a) => {
            let r = c
                .call(
                    Method::POST,
                    &path(&a.name, &["pause"]),
                    &[],
                    Some(&json!({})),
                )
                .await?;
            (Some(format!("schedule {} paused", a.name)), r.body)
        }
        SchedulesCommand::Resume(a) => {
            let r = c
                .call(
                    Method::POST,
                    &path(&a.name, &["resume"]),
                    &[],
                    Some(&json!({})),
                )
                .await?;
            (Some(format!("schedule {} resumed", a.name)), r.body)
        }
        SchedulesCommand::Trigger(a) => {
            let body = match &a.request_id {
                Some(r) => json!({ "request_id": r }),
                None => json!({}),
            };
            let r = c
                .call(Method::POST, &path(&a.name, &["trigger"]), &[], Some(&body))
                .await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(format!(
                "schedule {} triggered: run {}",
                a.name,
                s(&r.body["run_id"])
            ));
        }
    };
    if json {
        return Ok(pretty(&body));
    }
    Ok(match head {
        Some(h) => format!("{h}\n{}", detail(&body)),
        None => detail(&body),
    })
}

/// Run one verb, printing its output.
pub async fn run(c: &Client, out: Out, cmd: &SchedulesCommand) -> Result<Outcome, CallError> {
    let text = render(c, out.json, cmd).await?;
    println!("{text}");
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    fn sched(args: &[&str]) -> Result<SchedulesCommand, clap::Error> {
        let mut v = vec!["schedules"];
        v.extend_from_slice(args);
        parse(&v).map(|c| match c {
            ClientCommand::Schedules { command } => command,
            other => panic!("not schedules: {other:?}"),
        })
    }

    #[test]
    fn create_body_maps_every_flag() {
        let SchedulesCommand::Create(a) = sched(&[
            "create",
            "--name",
            "nightly",
            "--workflow",
            "report",
            "--version",
            "3",
            "--cron",
            "0 0 * * *",
            "--timezone",
            "Europe/Lisbon",
            "--input-json",
            r#"{"k":1}"#,
            "--overlap",
            "buffer-one",
            "--catchup",
            "all",
            "--catchup-window",
            "1h",
            "--start-at",
            "2026-01-01T00:00:00Z",
            "--end-at",
            "1800000000000",
            "--paused",
        ])
        .unwrap() else {
            panic!("create")
        };
        assert_eq!(
            a.body(),
            json!({
                "name": "nightly", "workflow": "report", "version": 3, "cron": "0 0 * * *",
                "timezone": "Europe/Lisbon", "input": {"k": 1}, "overlap": "buffer_one",
                "catchup": "all", "catchup_window_ms": 3_600_000u64,
                "start_at_ms": 1_767_225_600_000u64, "end_at_ms": 1_800_000_000_000u64,
                "paused": true,
            })
        );
    }

    #[test]
    fn usage_errors() {
        // invalid --overlap value, missing --cron, both input flags, bad time
        for args in [
            &[
                "create",
                "--name",
                "a",
                "--workflow",
                "w",
                "--cron",
                "* * * * *",
                "--overlap",
                "never",
            ][..],
            &["create", "--name", "a", "--workflow", "w"][..],
            &[
                "create",
                "--name",
                "a",
                "--workflow",
                "w",
                "--cron",
                "x",
                "--input-json",
                "1",
                "--input-file",
                "f",
            ][..],
            &[
                "create",
                "--name",
                "a",
                "--workflow",
                "w",
                "--cron",
                "x",
                "--start-at",
                "tomorrow",
            ][..],
            &["update", "a"][..],
            &["trigger"][..],
        ] {
            let e = sched(args).unwrap_err();
            assert_eq!(e.exit_code(), 2, "{args:?}: {e}");
        }
    }

    #[test]
    fn update_body_overlays_and_drops_output_fields() {
        let SchedulesCommand::Update(a) = sched(&[
            "update",
            "n",
            "--version-tag",
            "4",
            "--cron",
            "@daily",
            "--overlap",
            "allow-all",
        ])
        .unwrap() else {
            panic!("update")
        };
        let cur = json!({"name": "n", "workflow": "w", "cron": "@hourly", "timezone": "",
            "input": null, "paused": true, "overlap": "skip", "catchup": "skip",
            "version_tag": 5, "next_fire_at_ms": 1, "last_fire_at_ms": 2});
        assert_eq!(
            a.body(&cur),
            json!({"name": "n", "workflow": "w", "cron": "@daily", "timezone": "",
                "input": null, "paused": true, "overlap": "allow_all", "catchup": "skip",
                "version_tag": 4})
        );
    }

    #[test]
    fn hint_rewrites_stub_and_disabled_only() {
        let stub = CallError::Api {
            status: 503,
            body: json!({"error": {"code": "unavailable", "message": "not yet implemented (08-14)"}}),
        };
        let m = unimplemented_hint("schedules", stub).message();
        assert!(m.contains("not implemented on this server"), "{m}");
        assert!(m.contains("HTTP 503"), "{m}");
        let other = CallError::Api {
            status: 503,
            body: json!({"error": {"code": "unavailable", "message": "no leader"}}),
        };
        assert_eq!(
            unimplemented_hint("schedules", other).message(),
            "no leader (HTTP 503 unavailable)"
        );
    }
}
