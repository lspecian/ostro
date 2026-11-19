//! CLI-06 client verbs: clap definitions, shared argument parsers and dispatch.
//!
//! The `ostro` binary embeds [`ClientCommand`] with `#[command(flatten)]`, next to
//! `dev` / `server` / `openapi` / `build-info`. Every verb carries the connection flags
//! ([`Conn`]: `--server`, `--json`, `--timeout`) itself, so they are accepted after the
//! verb and do not leak into `ostro dev`.

pub mod deploy;
pub mod runs;

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand, ValueEnum};
use serde_json::Value;

use crate::client::{CallError, Client, DEFAULT_SERVER};
use crate::output::{Out, Outcome};

/// Connection and output flags shared by every client verb.
#[derive(Debug, Clone, Args)]
pub struct Conn {
    /// Server URL.
    #[arg(long, env = "OSTRO_SERVER", default_value = DEFAULT_SERVER)]
    pub server: String,
    /// Print the REST response body as JSON.
    #[arg(long)]
    pub json: bool,
    /// Per-request timeout (friendly duration; a long-poll adds its wait).
    #[arg(long, default_value = "30s", value_parser = parse_duration)]
    pub timeout: Duration,
}

impl Conn {
    fn client(&self) -> Result<Client, CallError> {
        Client::new(&self.server, self.timeout)
    }

    fn out(&self) -> Out {
        Out { json: self.json }
    }
}

/// The client verbs.
#[derive(Debug, Clone, Subcommand)]
pub enum ClientCommand {
    /// Start a run of a workflow (optionally wait for its result).
    Run(runs::RunArgs),
    /// List runs.
    Ls(runs::LsArgs),
    /// Show one run.
    Inspect(runs::InspectArgs),
    /// Show a run's history.
    History(runs::HistoryArgs),
    /// Upload a Wasm component (no build) and optionally activate its workflows.
    Deploy(deploy::DeployArgs),
}

/// Run a verb to completion on a current-thread runtime.
pub fn run(cmd: ClientCommand) -> Outcome {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("ostro: error: cannot start the async runtime: {e}");
            return Outcome::Error;
        }
    };
    rt.block_on(dispatch(cmd))
}

/// Dispatch one verb.
pub async fn dispatch(cmd: ClientCommand) -> Outcome {
    let conn = match &cmd {
        ClientCommand::Run(a) => &a.conn,
        ClientCommand::Ls(a) => &a.conn,
        ClientCommand::Inspect(a) => &a.conn,
        ClientCommand::History(a) => &a.conn,
        ClientCommand::Deploy(a) => &a.conn,
    };
    let out = conn.out();
    let client = match conn.client() {
        Ok(c) => c,
        Err(e) => return out.error(&e),
    };
    let res = match &cmd {
        ClientCommand::Run(a) => runs::run(&client, out, a).await,
        ClientCommand::Ls(a) => runs::ls(&client, out, a).await,
        ClientCommand::Inspect(a) => runs::inspect(&client, out, a).await,
        ClientCommand::History(a) => runs::history(&client, out, a).await,
        ClientCommand::Deploy(a) => deploy::deploy(&client, out, a).await,
    };
    match res {
        Ok(o) => o,
        Err(e) => out.error(&e),
    }
}

// ---------------------------------------------------------------------------------
// Shared argument parsers (errors become clap usage errors, exit 2)
// ---------------------------------------------------------------------------------

static SPAN_PARSER: jiff::fmt::friendly::SpanParser = jiff::fmt::friendly::SpanParser::new();

/// A friendly duration (`30s`, `500ms`, `168h`, `1m 30s`), parsed with the same jiff
/// parser as the server. Days and weeks are rejected (jiff treats them as calendar units),
/// with a hint in hours: `7d` -> "write it as 168h".
pub fn parse_duration(s: &str) -> Result<Duration, String> {
    let t = s.trim();
    SPAN_PARSER
        .parse_unsigned_duration(t)
        .map_err(|e| match days_as_hours(t) {
            Some(hours) => format!(
                "invalid duration {s:?}: days and weeks are not accepted, write it as {hours}h"
            ),
            None => format!(
                "invalid duration {s:?} (expected e.g. 30s, 500ms, 168h; write days as hours): {e}"
            ),
        })
}

/// `7d` / `7 days` / `2w` -> the equivalent number of hours.
fn days_as_hours(s: &str) -> Option<u64> {
    let digits: String = s.chars().take_while(char::is_ascii_digit).collect();
    let n: u64 = digits.parse().ok()?;
    let unit = s[digits.len()..].trim();
    let per = match unit {
        "d" | "day" | "days" => 24,
        "w" | "week" | "weeks" => 168,
        _ => return None,
    };
    n.checked_mul(per)
}

/// A friendly duration that the server receives as text (validated locally first).
pub fn parse_duration_text(s: &str) -> Result<String, String> {
    parse_duration(s).map(|_| s.trim().to_owned())
}

/// A JSON value given inline (`'{"a":1}'`, `42`, `'"text"'`) or as `@path` (file
/// contents).
pub fn parse_json_arg(s: &str) -> Result<Value, String> {
    let (text, origin) = match s.strip_prefix('@') {
        Some(path) => {
            let p = PathBuf::from(path);
            let text = std::fs::read_to_string(&p)
                .map_err(|e| format!("cannot read {}: {e}", p.display()))?;
            (text, format!("file {}", p.display()))
        }
        None => (s.to_owned(), "argument".to_owned()),
    };
    serde_json::from_str(&text).map_err(|e| {
        format!("{origin} is not valid JSON ({e}); quote text as JSON, e.g. '\"hello\"'")
    })
}

/// Run status filter values (REST spelling).
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "snake_case")]
pub enum StatusArg {
    Scheduled,
    Ready,
    Running,
    Completed,
    Failed,
    Cancelled,
    Terminated,
    TimedOut,
    ContinuedAsNew,
}

impl StatusArg {
    /// The REST query value.
    pub fn as_str(self) -> &'static str {
        match self {
            StatusArg::Scheduled => "scheduled",
            StatusArg::Ready => "ready",
            StatusArg::Running => "running",
            StatusArg::Completed => "completed",
            StatusArg::Failed => "failed",
            StatusArg::Cancelled => "cancelled",
            StatusArg::Terminated => "terminated",
            StatusArg::TimedOut => "timed_out",
            StatusArg::ContinuedAsNew => "continued_as_new",
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use clap::Parser;

    /// The verbs under a test root command, as the `ostro` binary embeds them.
    #[derive(Debug, Parser)]
    #[command(name = "ostro")]
    pub struct TestCli {
        #[command(subcommand)]
        pub cmd: ClientCommand,
    }

    pub fn parse(args: &[&str]) -> Result<ClientCommand, clap::Error> {
        let mut v = vec!["ostro"];
        v.extend_from_slice(args);
        TestCli::try_parse_from(v).map(|c| c.cmd)
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(
            parse_duration("168h").unwrap(),
            Duration::from_secs(168 * 3600)
        );
        assert_eq!(parse_duration(" 1m 30s ").unwrap(), Duration::from_secs(90));
        let e = parse_duration("7d").unwrap_err();
        assert!(e.contains("168h"), "{e}");
        let e = parse_duration("2w").unwrap_err();
        assert!(e.contains("336h"), "{e}");
        let e = parse_duration("soon").unwrap_err();
        assert!(e.contains("write days as hours"), "{e}");
    }

    #[test]
    fn seven_days_is_a_usage_error_suggesting_168h() {
        let e = parse(&["run", "w", "--retention", "7d"]).unwrap_err();
        assert_eq!(e.exit_code(), 2);
        assert!(e.to_string().contains("168h"), "{e}");
        let e = parse(&["run", "w", "--schedule-to-start", "1d"]).unwrap_err();
        assert!(e.to_string().contains("24h"), "{e}");
    }

    #[test]
    fn json_args_inline_and_file() {
        assert_eq!(parse_json_arg("42").unwrap(), serde_json::json!(42));
        assert_eq!(
            parse_json_arg(r#"{"a":[1,2]}"#).unwrap(),
            serde_json::json!({"a": [1, 2]})
        );
        let e = parse_json_arg("hello").unwrap_err();
        assert!(e.contains("not valid JSON"), "{e}");

        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("in.json");
        std::fs::write(&p, r#"{"name": "file"}"#).unwrap();
        let arg = format!("@{}", p.display());
        assert_eq!(
            parse_json_arg(&arg).unwrap(),
            serde_json::json!({"name": "file"})
        );
        let e = parse_json_arg(&format!("@{}", dir.path().join("nope").display())).unwrap_err();
        assert!(e.contains("cannot read"), "{e}");
        std::fs::write(&p, "{").unwrap();
        assert!(parse_json_arg(&arg).unwrap_err().contains("not valid JSON"));
    }

    #[test]
    fn conn_defaults_and_flags() {
        // OSTRO_SERVER may be set in the developer's shell; only assert explicit flags.
        let ClientCommand::Inspect(a) = parse(&[
            "inspect",
            "r1",
            "--server",
            "http://h:9",
            "--json",
            "--timeout",
            "5s",
        ])
        .unwrap() else {
            panic!("inspect")
        };
        assert_eq!(a.conn.server, "http://h:9");
        assert!(a.conn.json);
        assert_eq!(a.conn.timeout, Duration::from_secs(5));
        let ClientCommand::Inspect(a) = parse(&["inspect", "r1"]).unwrap() else {
            panic!("inspect")
        };
        assert!(!a.conn.json);
        assert_eq!(a.conn.timeout, Duration::from_secs(30));
        assert_eq!(
            parse(&["inspect", "r1", "--timeout", "7d"])
                .unwrap_err()
                .exit_code(),
            2
        );
    }

    #[test]
    fn unknown_verbs_and_missing_args_are_usage_errors() {
        assert_eq!(parse(&["bogus"]).unwrap_err().exit_code(), 2);
        assert_eq!(parse(&["inspect"]).unwrap_err().exit_code(), 2);
    }
}
