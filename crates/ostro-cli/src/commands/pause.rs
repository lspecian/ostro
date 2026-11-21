//! `ostro run pause|resume` and `ostro workflow pause|resume` (07-15, WORK-05 / OPS-01).
//!
//! | Verb | Request |
//! |---|---|
//! | `run pause <id> [--detail T]` | `POST /v1/runs/{id}/pause` `{detail?}` -> `{run_id, paused, changed}` (404 unknown, 409 `run_terminal`) |
//! | `run resume <id>` | `POST /v1/runs/{id}/resume` |
//! | `workflow pause` / `resume <name>` | `POST /v1/workflows/{name}/pause` / `.../resume` -> `{name, paused, changed}` |
//!
//! `ostro run` keeps starting runs: [`RunCommand`] is `ostro run <WORKFLOW> [flags]`
//! ([`runs::RunArgs`], unchanged) **or** one of the `pause` / `resume` subcommands. A
//! first positional named `pause` or `resume` selects the subcommand; a workflow with one
//! of those names is started with `ostro run [FLAGS] -- pause`.

use std::ops::Deref;

use clap::{Args, FromArgMatches, Subcommand};
use reqwest::Method;
use serde_json::{Map, Value, json};

use super::{Conn, runs};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s};

/// `ostro run`: start a run, or pause / resume one.
///
/// Exactly one of `action` and `start` is present after a successful parse: the clap
/// glue below is hand-written because the derive cannot express "a required positional
/// unless a subcommand is given" for a flattened struct.
#[derive(Debug, Clone)]
pub struct RunCommand {
    pub action: Option<RunAction>,
    pub start: Option<runs::RunArgs>,
}

impl FromArgMatches for RunCommand {
    fn from_arg_matches(m: &clap::ArgMatches) -> Result<Self, clap::Error> {
        if m.subcommand_name().is_some() {
            Ok(Self {
                action: Some(RunAction::from_arg_matches(m)?),
                start: None,
            })
        } else {
            Ok(Self {
                action: None,
                start: Some(runs::RunArgs::from_arg_matches(m)?),
            })
        }
    }

    fn update_from_arg_matches(&mut self, m: &clap::ArgMatches) -> Result<(), clap::Error> {
        *self = Self::from_arg_matches(m)?;
        Ok(())
    }
}

impl Args for RunCommand {
    fn augment_args(cmd: clap::Command) -> clap::Command {
        RunAction::augment_subcommands(runs::RunArgs::augment_args(cmd))
            .subcommand_required(false)
            .args_conflicts_with_subcommands(true)
            .subcommand_negates_reqs(true)
    }

    fn augment_args_for_update(cmd: clap::Command) -> clap::Command {
        Self::augment_args(cmd)
    }
}

impl Deref for RunCommand {
    type Target = runs::RunArgs;

    /// The start arguments (`ostro run <WORKFLOW>`), so existing callers keep reading
    /// `a.workflow`, `a.body()`, ...
    ///
    /// # Panics
    /// On a `run pause|resume` invocation, which has no start arguments (clap never
    /// produces a `RunCommand` with neither).
    fn deref(&self) -> &runs::RunArgs {
        self.start
            .as_ref()
            .expect("`ostro run` without pause/resume carries the start arguments")
    }
}

impl RunCommand {
    /// The connection flags of the invocation.
    pub fn conn(&self) -> &Conn {
        match &self.action {
            None => &self.deref().conn,
            Some(RunAction::Pause(a)) => &a.conn,
            Some(RunAction::Resume(a)) => &a.conn,
        }
    }
}

/// `ostro run pause|resume`.
#[derive(Debug, Clone, Subcommand)]
pub enum RunAction {
    /// Pause a run: no further activations until it is resumed.
    Pause(RunPauseArgs),
    /// Resume a paused run.
    Resume(RunIdArgs),
}

/// `run pause`.
#[derive(Debug, Clone, Args)]
pub struct RunPauseArgs {
    /// Run id.
    pub run_id: String,
    /// Operator note stored with the pause (at most 1024 bytes).
    #[arg(long)]
    pub detail: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `run resume`.
#[derive(Debug, Clone, Args)]
pub struct RunIdArgs {
    /// Run id.
    pub run_id: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro workflow ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum WorkflowCommand {
    /// Pause a workflow: none of its runs is activated until it is resumed.
    Pause(WorkflowArgs),
    /// Resume a paused workflow.
    Resume(WorkflowArgs),
}

impl WorkflowCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            WorkflowCommand::Pause(a) | WorkflowCommand::Resume(a) => &a.conn,
        }
    }
}

/// A verb on one workflow.
#[derive(Debug, Clone, Args)]
pub struct WorkflowArgs {
    /// Workflow name (bare, or `tenant/name`).
    pub workflow: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// The `run pause` request body.
pub fn pause_body(a: &RunPauseArgs) -> Value {
    let mut b = Map::new();
    if let Some(d) = &a.detail {
        b.insert("detail".into(), json!(d));
    }
    Value::Object(b)
}

fn changed_line(body: &Value, what: &str, name: &str, verb: &str) -> String {
    if body["changed"].as_bool().unwrap_or(false) {
        format!("{what} {name} {verb}")
    } else {
        format!("{what} {name} already {verb}")
    }
}

/// `ostro run ...`: start (no subcommand) or pause / resume.
pub async fn run(c: &Client, out: Out, a: &RunCommand) -> Result<Outcome, CallError> {
    let (r, verb) = match &a.action {
        None => return runs::run(c, out, a).await,
        Some(RunAction::Pause(p)) => (
            c.call(
                Method::POST,
                &["v1", "runs", &p.run_id, "pause"],
                &[],
                Some(&pause_body(p)),
            )
            .await?,
            "paused",
        ),
        Some(RunAction::Resume(p)) => (
            c.call(
                Method::POST,
                &["v1", "runs", &p.run_id, "resume"],
                &[],
                None,
            )
            .await?,
            "resumed",
        ),
    };
    if out.json {
        out.json(&r.body);
    } else {
        println!(
            "{}",
            changed_line(&r.body, "run", &s(&r.body["run_id"]), verb)
        );
    }
    Ok(Outcome::Ok)
}

/// `ostro workflow ...`.
pub async fn workflow(c: &Client, out: Out, cmd: &WorkflowCommand) -> Result<Outcome, CallError> {
    let (a, action, verb) = match cmd {
        WorkflowCommand::Pause(a) => (a, "pause", "paused"),
        WorkflowCommand::Resume(a) => (a, "resume", "resumed"),
    };
    let r = c
        .call(
            Method::POST,
            &["v1", "workflows", &a.workflow, action],
            &[],
            None,
        )
        .await?;
    if out.json {
        out.json(&r.body);
    } else {
        println!(
            "{}",
            changed_line(&r.body, "workflow", &s(&r.body["name"]), verb)
        );
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    #[test]
    fn run_keeps_starting_runs_and_gains_pause_resume() {
        let ClientCommand::Run(a) = parse(&["run", "w", "--queue", "q"]).unwrap() else {
            panic!("run")
        };
        assert!(a.action.is_none());
        assert_eq!(a.workflow, "w");
        assert_eq!(a.body(), json!({"queue": "q"}));

        let ClientCommand::Run(a) =
            parse(&["run", "pause", "r1", "--detail", "investigating"]).unwrap()
        else {
            panic!("run pause")
        };
        let Some(RunAction::Pause(p)) = &a.action else {
            panic!("pause")
        };
        assert_eq!(p.run_id, "r1");
        assert_eq!(pause_body(p), json!({"detail": "investigating"}));

        let ClientCommand::Run(a) = parse(&["run", "resume", "r1", "--json"]).unwrap() else {
            panic!("run resume")
        };
        assert!(matches!(&a.action, Some(RunAction::Resume(r)) if r.run_id == "r1"));
        assert!(a.conn().json);

        // Missing ids and a missing workflow are usage errors.
        assert_eq!(parse(&["run", "pause"]).unwrap_err().exit_code(), 2);
        assert_eq!(parse(&["run"]).unwrap_err().exit_code(), 2);
    }

    #[test]
    fn workflow_pause_resume() {
        let ClientCommand::Workflow { command } = parse(&["workflow", "pause", "acme/w"]).unwrap()
        else {
            panic!("workflow")
        };
        assert!(matches!(&command, WorkflowCommand::Pause(a) if a.workflow == "acme/w"));
        assert_eq!(parse(&["workflow", "resume"]).unwrap_err().exit_code(), 2);
        assert_eq!(
            parse(&["workflow", "stop", "w"]).unwrap_err().exit_code(),
            2
        );
    }
}
