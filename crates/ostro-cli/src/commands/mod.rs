//! CLI-06 client verbs: clap definitions, shared argument parsers and dispatch.
//!
//! The `ostro` binary embeds [`ClientCommand`] with `#[command(flatten)]`, next to
//! `dev` / `server` / `openapi` / `build-info`. Every verb carries the connection flags
//! ([`Conn`]: `--server`, `--json`, `--timeout`) itself, so they are accepted after the
//! verb and do not leak into `ostro dev`.
//!
//! [`backup`] holds the `ostro backup` / `ostro restore` arguments (08-18); the binary
//! embeds them as its own subcommands because restore and offline backup need the storage
//! crates.
//!
//! Phase 7 (07-15): [`queues`] (`ostro queue flow|pause|resume|demand`), [`pause`]
//! (`ostro run pause|resume`, `ostro workflow pause|resume`) and [`worker_tokens`]
//! (`ostro worker token create|list|revoke`, embedded under the binary's own `worker`
//! command and run through [`worker_tokens::run`]).

pub mod backup;
pub mod bridge;
pub mod deploy;
pub mod lifecycle;
pub mod pause;
pub mod queues;
pub mod runs;
pub mod schedules;
pub mod webhooks;
pub mod worker_tokens;
// Phase 11 (11-14): run operations.
pub mod bulk;
pub mod dlq;
pub mod limits;
pub mod rerun;
// Phase 10 (10-03): local verbs (stubs; one later plan owns each file).
pub mod build;
pub mod init;
pub mod replay;
pub mod toolchain;

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
    /// PEM CA certificate to trust for an https:// server instead of the platform roots
    /// (e.g. the cluster CA `<data_dir>/tls/ca.crt` behind a self-signed listener).
    #[arg(long, env = "OSTRO_CA_FILE", value_name = "PEM")]
    pub ca_file: Option<std::path::PathBuf>,
}

impl Conn {
    fn client(&self) -> Result<Client, CallError> {
        Client::with_ca(&self.server, self.timeout, self.ca_file.as_deref())
    }

    fn out(&self) -> Out {
        Out { json: self.json }
    }
}

/// The client verbs.
#[derive(Debug, Clone, Subcommand)]
pub enum ClientCommand {
    /// Start a run of a workflow (optionally wait for its result), or pause / resume a run.
    Run(pause::RunCommand),
    /// List runs.
    Ls(runs::LsArgs),
    /// Show one run.
    Inspect(runs::InspectArgs),
    /// Show a run's history.
    History(runs::HistoryArgs),
    /// Send a named signal to a run.
    Signal(lifecycle::SignalArgs),
    /// Request graceful cancellation of a run.
    Cancel(lifecycle::StopArgs),
    /// Hard-stop a run (no guest cleanup).
    Terminate(lifecycle::StopArgs),
    /// Retry a terminal run (resume in place or restart as a new linked run).
    Retry(lifecycle::RetryArgs),
    /// Show (or wait for) a run's result.
    Result(lifecycle::ResultArgs),
    /// Await tokens.
    Token {
        #[command(subcommand)]
        command: lifecycle::TokenCommand,
    },
    /// Upload a Wasm component (no build) and optionally activate its workflows.
    Deploy(deploy::DeployArgs),
    /// Manage cron schedules (ls, get, create, update, delete, pause, resume, trigger).
    Schedules {
        #[command(subcommand)]
        command: schedules::SchedulesCommand,
    },
    /// Manage inbound webhooks (ls, get, create, update, delete).
    Webhooks {
        #[command(subcommand)]
        command: webhooks::WebhooksCommand,
    },
    /// Manage sealed secrets (set, ls, rm); values are write-only and read from stdin,
    /// a file or an env var, never from the command line.
    Secret {
        #[command(subcommand)]
        command: bridge::SecretCommand,
    },
    /// Manage secret grants (add, ls, rm): which module may send which secret where.
    Grant {
        #[command(subcommand)]
        command: bridge::GrantCommand,
    },
    /// Manage connections (set, ls, get, rm): SQL databases, object stores, HTTP targets.
    Connection {
        #[command(subcommand)]
        command: bridge::ConnectionCommand,
    },
    /// Manage module egress policies (set, ls, get, rm).
    Egress {
        #[command(subcommand)]
        command: bridge::EgressCommand,
    },
    /// Queue flow control, placement, pause/resume and worker demand.
    Queue {
        #[command(subcommand)]
        command: queues::QueueCommand,
    },
    /// Pause or resume a workflow's runs.
    Workflow {
        #[command(subcommand)]
        command: pause::WorkflowCommand,
    },
    /// Rerun a terminal run from a cut (a new run; effects after the cut re-execute).
    Rerun(rerun::RerunArgs),
    /// Dead-letter queue of failed runs (ls, requeue, dismiss).
    Dlq {
        #[command(subcommand)]
        command: dlq::DlqCommand,
    },
    /// Show the published engine and API limits.
    Limits(limits::LimitsArgs),
    /// Rate-limited bulk jobs over a run selection (cancel, terminate, retry, rerun,
    /// requeue, pause, resume, signal; ls, status, cancel <JOB>).
    Bulk {
        #[command(subcommand)]
        command: bulk::BulkCommand,
    },
    /// Scaffold a workflow project (rust, python or ts).
    Init(init::InitArgs),
    /// Build a workflow project into a Wasm component.
    Build(build::BuildArgs),
    /// Manage the hermetic build toolchains (install, list, prune).
    Toolchain(toolchain::ToolchainArgs),
    /// Check the build environment and toolchains.
    Doctor(toolchain::DoctorArgs),
    /// Replay recorded runs against a module and report divergence (exit 4).
    Replay(replay::ReplayArgs),
}

/// Run a verb to completion on a current-thread runtime.
///
/// Phase 10 local verbs (`init`, `build`, `toolchain`, `doctor`, `replay`) run outside
/// any runtime (they may start their own with [`block_on`]).
pub fn run(cmd: ClientCommand) -> Outcome {
    match local(&cmd) {
        Some(run_local) => run_local(),
        None => block_on(dispatch(cmd)),
    }
}

/// The synchronous runner of a Phase 10 local verb, `None` for a REST verb.
fn local(cmd: &ClientCommand) -> Option<Box<dyn FnOnce() -> Outcome + Send>> {
    Some(match cmd {
        ClientCommand::Init(a) => {
            let a = a.clone();
            Box::new(move || init::run(&a))
        }
        ClientCommand::Build(a) => {
            let a = a.clone();
            Box::new(move || build::run(&a))
        }
        ClientCommand::Toolchain(a) => {
            let a = a.clone();
            Box::new(move || toolchain::run(&a))
        }
        ClientCommand::Doctor(a) => {
            let a = a.clone();
            Box::new(move || toolchain::doctor(&a))
        }
        ClientCommand::Replay(a) => {
            let a = a.clone();
            Box::new(move || replay::run(&a))
        }
        _ => return None,
    })
}

/// Drive `fut` on a fresh current-thread runtime (one per CLI invocation).
pub(crate) fn block_on(fut: impl std::future::Future<Output = Outcome>) -> Outcome {
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
    rt.block_on(fut)
}

/// Report a failed call ([`Out::error`]) and, for `rate_limited` (HTTP 429, D-W21), when
/// to retry (the `Retry-After` value, from `details.retry_after_ms`).
pub fn report(out: Out, e: &CallError) -> Outcome {
    let o = out.error(e);
    if let CallError::Api { body, .. } = e
        && body["error"]["code"] == "rate_limited"
        && let Some(ms) = body["error"]["details"]["retry_after_ms"].as_u64()
    {
        eprintln!(
            "ostro: rate limited: retry after {} s (Retry-After), {ms} ms",
            ms.div_ceil(1000)
        );
    }
    o
}

/// Dispatch one verb.
///
/// A Phase 10 local verb runs on the blocking pool (its runner may start its own
/// runtime).
pub async fn dispatch(cmd: ClientCommand) -> Outcome {
    if let Some(run_local) = local(&cmd) {
        return tokio::task::spawn_blocking(run_local)
            .await
            .unwrap_or(Outcome::Error);
    }
    let conn = match &cmd {
        ClientCommand::Run(a) => a.conn(),
        ClientCommand::Ls(a) => &a.conn,
        ClientCommand::Inspect(a) => &a.conn,
        ClientCommand::History(a) => &a.conn,
        ClientCommand::Signal(a) => &a.conn,
        ClientCommand::Cancel(a) | ClientCommand::Terminate(a) => &a.conn,
        ClientCommand::Retry(a) => &a.conn,
        ClientCommand::Result(a) => &a.conn,
        ClientCommand::Token {
            command: lifecycle::TokenCommand::Complete(a),
        } => &a.conn,
        ClientCommand::Deploy(a) => &a.conn,
        ClientCommand::Schedules { command } => command.conn(),
        ClientCommand::Webhooks { command } => command.conn(),
        ClientCommand::Secret { command } => command.conn(),
        ClientCommand::Grant { command } => command.conn(),
        ClientCommand::Connection { command } => command.conn(),
        ClientCommand::Egress { command } => command.conn(),
        ClientCommand::Queue { command } => command.conn(),
        ClientCommand::Workflow { command } => command.conn(),
        ClientCommand::Rerun(a) => &a.conn,
        ClientCommand::Dlq { command } => command.conn(),
        ClientCommand::Limits(a) => &a.conn,
        ClientCommand::Bulk { command } => command.conn(),
        ClientCommand::Init(_)
        | ClientCommand::Build(_)
        | ClientCommand::Toolchain(_)
        | ClientCommand::Doctor(_)
        | ClientCommand::Replay(_) => unreachable!("local verbs return above"),
    };
    let out = conn.out();
    let client = match conn.client() {
        Ok(c) => c,
        Err(e) => return report(out, &e),
    };
    let res = match &cmd {
        ClientCommand::Run(a) => pause::run(&client, out, a).await,
        ClientCommand::Ls(a) => runs::ls(&client, out, a).await,
        ClientCommand::Inspect(a) => runs::inspect(&client, out, a).await,
        ClientCommand::History(a) => runs::history(&client, out, a).await,
        ClientCommand::Signal(a) => lifecycle::signal(&client, out, a).await,
        ClientCommand::Cancel(a) => lifecycle::cancel(&client, out, a).await,
        ClientCommand::Terminate(a) => lifecycle::terminate(&client, out, a).await,
        ClientCommand::Retry(a) => lifecycle::retry(&client, out, a).await,
        ClientCommand::Result(a) => lifecycle::result(&client, out, a).await,
        ClientCommand::Token {
            command: lifecycle::TokenCommand::Complete(a),
        } => lifecycle::token_complete(&client, out, a).await,
        ClientCommand::Deploy(a) => deploy::deploy(&client, out, a).await,
        ClientCommand::Schedules { command } => schedules::run(&client, out, command).await,
        ClientCommand::Webhooks { command } => webhooks::run(&client, out, command).await,
        ClientCommand::Secret { command } => bridge::run_secret(&client, out, command).await,
        ClientCommand::Grant { command } => bridge::run_grant(&client, out, command).await,
        ClientCommand::Connection { command } => {
            bridge::run_connection(&client, out, command).await
        }
        ClientCommand::Egress { command } => bridge::run_egress(&client, out, command).await,
        ClientCommand::Queue { command } => queues::run(&client, out, command).await,
        ClientCommand::Workflow { command } => pause::workflow(&client, out, command).await,
        ClientCommand::Rerun(a) => rerun::rerun(&client, out, a).await,
        ClientCommand::Dlq { command } => dlq::run(&client, out, command).await,
        ClientCommand::Limits(a) => limits::limits(&client, out, a).await,
        ClientCommand::Bulk { command } => bulk::run(&client, out, command).await,
        ClientCommand::Init(_)
        | ClientCommand::Build(_)
        | ClientCommand::Toolchain(_)
        | ClientCommand::Doctor(_)
        | ClientCommand::Replay(_) => unreachable!("local verbs return above"),
    };
    match res {
        Ok(o) => o,
        Err(e) => report(out, &e),
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
        assert_eq!(parse(&["signal", "r1"]).unwrap_err().exit_code(), 2);
        assert_eq!(parse(&["token"]).unwrap_err().exit_code(), 2);
    }
}

/// Phase 10 (10-03): the local verbs' flag sets and selector rules.
#[cfg(test)]
mod p10_tests {
    use std::path::PathBuf;

    use super::tests::parse;
    use super::*;

    fn usage(args: &[&str]) {
        let e = parse(args).expect_err(&format!("{args:?} must not parse"));
        assert_eq!(e.exit_code(), 2, "{args:?}: {e}");
    }

    #[test]
    fn init_flags() {
        let ClientCommand::Init(a) = parse(&["init"]).unwrap() else {
            panic!("init")
        };
        assert!(a.dir.is_none() && a.lang.is_none() && a.name.is_none());
        assert!(!a.force && !a.json);
        let ClientCommand::Init(a) = parse(&[
            "init", "proj", "--lang", "python", "--name", "n", "--force", "--json",
        ])
        .unwrap() else {
            panic!("init")
        };
        assert_eq!(a.dir, Some(PathBuf::from("proj")));
        assert_eq!(a.lang, Some(init::InitLang::Python));
        assert_eq!(a.name.as_deref(), Some("n"));
        assert!(a.force && a.json);
        for l in ["rust", "ts"] {
            assert!(parse(&["init", "--lang", l]).is_ok(), "{l}");
        }
        usage(&["init", "--lang", "go"]);
        usage(&["init", "a", "b"]);
    }

    #[test]
    fn build_flags() {
        let ClientCommand::Build(a) = parse(&["build"]).unwrap() else {
            panic!("build")
        };
        assert_eq!(a.mode(), build::ToolchainMode::Auto);
        assert!(!a.offline && !a.json && !a.in_container && a.dir.is_none());
        let ClientCommand::Build(a) = parse(&[
            "build",
            "p",
            "--toolchain",
            "hermetic",
            "--offline",
            "--json",
            "--in-container",
        ])
        .unwrap() else {
            panic!("build")
        };
        assert_eq!(a.mode(), build::ToolchainMode::Hermetic);
        assert!(a.offline && a.json && a.in_container);
        assert_eq!(a.dir, Some(PathBuf::from("p")));
        let ClientCommand::Build(a) = parse(&["build", "--docker"]).unwrap() else {
            panic!("build")
        };
        assert_eq!(a.mode(), build::ToolchainMode::Docker);
        for t in ["auto", "system", "docker"] {
            assert!(parse(&["build", "--toolchain", t]).is_ok(), "{t}");
        }
        usage(&["build", "--toolchain", "nix"]);
        usage(&["build", "--docker", "--toolchain", "system"]);
    }

    #[test]
    fn toolchain_and_doctor_flags() {
        use toolchain::{ToolchainCommand as T, ToolchainLang};
        let ClientCommand::Toolchain(a) =
            parse(&["toolchain", "install", "--lang", "rust", "--json"]).unwrap()
        else {
            panic!("toolchain")
        };
        let T::Install(i) = a.command else {
            panic!("install")
        };
        assert_eq!(i.lang, Some(ToolchainLang::Rust));
        assert!(!i.all && i.json);
        let ClientCommand::Toolchain(a) = parse(&["toolchain", "install", "--all"]).unwrap() else {
            panic!("toolchain")
        };
        assert!(matches!(a.command, T::Install(ref i) if i.all && i.lang.is_none()));
        assert!(parse(&["toolchain", "install"]).is_ok());
        usage(&["toolchain", "install", "--lang", "ts", "--all"]);
        let ClientCommand::Toolchain(a) = parse(&["toolchain", "list", "--json"]).unwrap() else {
            panic!("toolchain")
        };
        assert!(matches!(a.command, T::List(ref l) if l.json));
        let ClientCommand::Toolchain(a) = parse(&["toolchain", "prune", "--state"]).unwrap() else {
            panic!("toolchain")
        };
        assert!(matches!(a.command, T::Prune(ref p) if p.state && !p.json));
        usage(&["toolchain"]);
        usage(&["toolchain", "bogus"]);

        let ClientCommand::Doctor(d) = parse(&["doctor", "--json"]).unwrap() else {
            panic!("doctor")
        };
        assert!(d.json);
        usage(&["doctor", "extra"]);
    }

    fn replay(args: &[&str]) -> replay::ReplayArgs {
        let mut v = vec!["replay"];
        v.extend_from_slice(args);
        match parse(&v) {
            Ok(ClientCommand::Replay(a)) => a,
            other => panic!("{v:?}: {other:?}"),
        }
    }

    #[test]
    fn replay_run_ids_merge_positional_and_flag() {
        let a = replay(&["r1", "r2", "--module", "m.wasm"]);
        assert_eq!(a.run_ids(), ["r1", "r2"]);
        assert_eq!(a.module, Some(PathBuf::from("m.wasm")));
        let a = replay(&["--run", "r3", "--run", "r4", "--module", "m.wasm"]);
        assert_eq!(a.run_ids(), ["r3", "r4"]);
        let a = replay(&["r1", "--run", "r2", "--module", "m.wasm", "--allow-missing"]);
        assert_eq!(a.run_ids(), ["r1", "r2"]);
        assert!(a.allow_missing && !a.conn.json);
        let a = replay(&[
            "r1",
            "--module",
            "m.wasm",
            "--json",
            "--server",
            "http://h:1",
        ]);
        assert!(a.conn.json);
        assert_eq!(a.conn.server, "http://h:1");
    }

    #[test]
    fn replay_sample_and_corpus() {
        let a = replay(&[
            "--sample",
            "5",
            "--workflow",
            "w",
            "--status",
            "completed",
            "--module",
            "m.wasm",
        ]);
        assert_eq!(a.sample, Some(5));
        assert_eq!(a.workflow.as_deref(), Some("w"));
        assert_eq!(a.status, Some(StatusArg::Completed));
        assert!(a.run_ids().is_empty());
        let a = replay(&["--corpus", "dir"]);
        assert_eq!(a.corpus, Some(PathBuf::from("dir")));
        assert!(a.module.is_none());
        let a = replay(&["--corpus", "dir", "--module", "new.wasm"]);
        assert_eq!(a.module, Some(PathBuf::from("new.wasm")));
    }

    #[test]
    fn replay_selector_rules() {
        // exactly one selector kind
        usage(&["replay", "r1", "--sample", "3", "--module", "x.wasm"]);
        usage(&[
            "replay", "--run", "r1", "--sample", "3", "--module", "x.wasm",
        ]);
        usage(&["replay", "r1", "--corpus", "d"]);
        usage(&["replay", "--run", "r1", "--corpus", "d"]);
        usage(&["replay", "--sample", "3", "--corpus", "d"]);
        // a selector is required
        usage(&["replay", "--module", "x.wasm"]);
        usage(&["replay"]);
        // --module required unless --corpus
        usage(&["replay", "r1"]);
        usage(&["replay", "--sample", "2"]);
        // --workflow / --status only with --sample
        usage(&["replay", "r1", "--workflow", "w", "--module", "x.wasm"]);
        usage(&["replay", "r1", "--status", "failed", "--module", "x.wasm"]);
        usage(&["replay", "--sample", "x", "--module", "x.wasm"]);
        usage(&["replay", "--corpus", "d", "--workflow", "w"]);
        usage(&["replay", "--workflow", "w", "--module", "x.wasm"]);
    }

    #[test]
    fn local_stubs_exit_2() {
        for args in [&["build"][..], &["replay", "r1", "--module", "m.wasm"]] {
            let cmd = parse(args).unwrap();
            assert_eq!(run(cmd.clone()), Outcome::Usage, "{args:?}");
            assert_eq!(block_on(dispatch(cmd)), Outcome::Usage, "{args:?}");
        }
    }
}
