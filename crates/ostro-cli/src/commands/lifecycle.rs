//! `ostro signal`, `cancel`, `terminate`, `retry`, `result`, `token complete`.
//!
//! Every mutating verb takes `--dry-run`, sent as `"dry_run": true`: the server runs the
//! real validation and changes nothing. Bodies follow the 05-04 REST routes; cancel,
//! terminate and retry send only the fields given.

use std::time::Duration;

use clap::{Args, Subcommand};
use serde_json::{Map, Value, json};

use super::runs::{completed_outcome, print_result, wait_result};
use super::{Conn, parse_duration, parse_json_arg};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, token_prefix};

/// `ostro signal`.
#[derive(Debug, Clone, Args)]
pub struct SignalArgs {
    /// Run id.
    pub run: String,
    /// Signal name.
    pub name: String,
    /// Payload: inline JSON or `@file` (default null).
    #[arg(long, value_parser = parse_json_arg)]
    pub payload: Option<Value>,
    /// Dedupe id: a repeat is acknowledged as a duplicate and not delivered again.
    #[arg(long)]
    pub request_id: Option<String>,
    /// Validate only; nothing is delivered.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl SignalArgs {
    /// The REST body.
    pub fn body(&self) -> Value {
        let mut b = Map::new();
        if let Some(p) = &self.payload {
            b.insert("payload".into(), p.clone());
        }
        if let Some(r) = &self.request_id {
            b.insert("request_id".into(), json!(r));
        }
        if self.dry_run {
            b.insert("dry_run".into(), json!(true));
        }
        Value::Object(b)
    }
}

/// `ostro cancel` / `ostro terminate`.
#[derive(Debug, Clone, Args)]
pub struct StopArgs {
    /// Run id.
    pub run: String,
    /// Reason, recorded in history.
    #[arg(long)]
    pub reason: Option<String>,
    /// Validate only; nothing is changed.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl StopArgs {
    /// The REST body.
    pub fn body(&self) -> Value {
        let mut b = Map::new();
        if let Some(r) = &self.reason {
            b.insert("reason".into(), json!(r));
        }
        if self.dry_run {
            b.insert("dry_run".into(), json!(true));
        }
        Value::Object(b)
    }
}

/// `ostro retry`.
#[derive(Debug, Clone, Args)]
pub struct RetryArgs {
    /// Run id.
    pub run: String,
    /// Validate only; reports the mode the retry would use.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro result`.
#[derive(Debug, Clone, Args)]
pub struct ResultArgs {
    /// Run id.
    pub run: String,
    /// Wait until the run is finished (optionally at most DUR, e.g. `--wait=2m`).
    #[arg(long, value_name = "DUR", num_args = 0..=1, require_equals = true, value_parser = parse_duration)]
    pub wait: Option<Option<Duration>>,
    /// Follow continue-as-new successors to the last run of the chain.
    #[arg(long)]
    pub follow: bool,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro token ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum TokenCommand {
    /// Complete an await token.
    Complete(TokenCompleteArgs),
}

/// `ostro token complete`.
#[derive(Debug, Clone, Args)]
pub struct TokenCompleteArgs {
    /// The `ot1_...` token (sent only in the request body).
    pub token: String,
    /// Completion payload: inline JSON or `@file` (default null).
    #[arg(long, value_parser = parse_json_arg)]
    pub payload: Option<Value>,
    /// Validate only; nothing is completed.
    #[arg(long)]
    pub dry_run: bool,
    #[command(flatten)]
    pub conn: Conn,
}

impl TokenCompleteArgs {
    /// The REST body.
    pub fn body(&self) -> Value {
        let mut b = Map::new();
        b.insert("token".into(), json!(self.token));
        if let Some(p) = &self.payload {
            b.insert("payload".into(), p.clone());
        }
        if self.dry_run {
            b.insert("dry_run".into(), json!(true));
        }
        Value::Object(b)
    }
}

fn dry(b: &Value) -> &'static str {
    if b["dry_run"].as_bool() == Some(true) {
        "dry run: "
    } else {
        ""
    }
}

/// `ostro signal`.
pub async fn signal(c: &Client, out: Out, a: &SignalArgs) -> Result<Outcome, CallError> {
    let r = c.signal(&a.run, &a.name, &a.body()).await?;
    let b = &r.body;
    if out.json {
        out.json(b);
    } else if b["duplicate"].as_bool() == Some(true) {
        println!(
            "{}signal {} to run {}: duplicate request id, not delivered again",
            dry(b),
            a.name,
            a.run
        );
    } else if b["dry_run"].as_bool() == Some(true) {
        println!(
            "dry run: signal {} to run {} would be accepted",
            a.name, a.run
        );
    } else {
        println!("signal {} sent to run {}", a.name, a.run);
    }
    Ok(Outcome::Ok)
}

/// `ostro cancel`.
pub async fn cancel(c: &Client, out: Out, a: &StopArgs) -> Result<Outcome, CallError> {
    let r = c.cancel(&a.run, &a.body()).await?;
    let b = &r.body;
    if out.json {
        out.json(b);
    } else if b["already_requested"].as_bool() == Some(true) {
        println!(
            "{}cancellation of run {} was already requested",
            dry(b),
            a.run
        );
    } else if b["dry_run"].as_bool() == Some(true) {
        println!("dry run: cancellation of run {} would be requested", a.run);
    } else {
        println!("cancellation requested for run {}", a.run);
    }
    Ok(Outcome::Ok)
}

/// `ostro terminate`.
pub async fn terminate(c: &Client, out: Out, a: &StopArgs) -> Result<Outcome, CallError> {
    let r = c.terminate(&a.run, &a.body()).await?;
    let b = &r.body;
    if out.json {
        out.json(b);
    } else if b["dry_run"].as_bool() == Some(true) {
        println!("dry run: run {} would be terminated", a.run);
    } else {
        println!("run {} {}", a.run, s(&b["status"]));
    }
    Ok(Outcome::Ok)
}

/// `ostro retry`.
pub async fn retry(c: &Client, out: Out, a: &RetryArgs) -> Result<Outcome, CallError> {
    let body = if a.dry_run {
        json!({ "dry_run": true })
    } else {
        json!({})
    };
    let r = c.retry(&a.run, &body).await?;
    let b = &r.body;
    if out.json {
        out.json(b);
        return Ok(Outcome::Ok);
    }
    let mode = s(&b["mode"]);
    let id = s(&b["run_id"]);
    match (b["dry_run"].as_bool() == Some(true), mode.as_str()) {
        (true, "restarted") => println!("dry run: run {} would be restarted as a new run", a.run),
        (true, _) => println!("dry run: run {} would be {mode}", a.run),
        (false, "restarted") => println!("restarted: new run {id} (retry of {})", a.run),
        (false, _) => println!("{mode}: run {id}"),
    }
    Ok(Outcome::Ok)
}

/// `ostro result`.
pub async fn result(c: &Client, out: Out, a: &ResultArgs) -> Result<Outcome, CallError> {
    let (body, waited) = match a.wait {
        Some(limit) => (wait_result(c, &a.run, limit, a.follow).await?, true),
        None => (
            c.result_once(&a.run, Duration::ZERO, a.follow).await?.body,
            false,
        ),
    };
    if out.json {
        out.json(&body);
    } else {
        print_result(&body);
    }
    Ok(if waited {
        completed_outcome(&body)
    } else {
        Outcome::Ok
    })
}

/// `ostro token complete`. Human output shows only a short token prefix.
pub async fn token_complete(
    c: &Client,
    out: Out,
    a: &TokenCompleteArgs,
) -> Result<Outcome, CallError> {
    let r = c.complete_token(&a.body()).await?;
    let b = &r.body;
    if out.json {
        out.json(b);
    } else if b["dry_run"].as_bool() == Some(true) {
        println!(
            "dry run: token {} for run {} would be completed",
            token_prefix(&a.token),
            s(&b["run_id"])
        );
    } else {
        println!(
            "token {} completed for run {}",
            token_prefix(&a.token),
            s(&b["run_id"])
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
    fn signal_args() {
        let ClientCommand::Signal(a) = parse(&[
            "signal",
            "r1",
            "x",
            "--payload",
            "1",
            "--request-id",
            "rid",
            "--dry-run",
        ])
        .unwrap() else {
            panic!()
        };
        assert_eq!((a.run.as_str(), a.name.as_str()), ("r1", "x"));
        assert_eq!(
            a.body(),
            json!({"payload": 1, "request_id": "rid", "dry_run": true})
        );
        let ClientCommand::Signal(a) = parse(&["signal", "r1", "x"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.body(), json!({}));
        assert_eq!(
            parse(&["signal", "r1", "x", "--payload", "nope"])
                .unwrap_err()
                .exit_code(),
            2
        );
    }

    #[test]
    fn stop_and_retry_args() {
        for verb in ["cancel", "terminate"] {
            let cmd = parse(&[verb, "r1", "--reason", "bye", "--dry-run"]).unwrap();
            let a = match cmd {
                ClientCommand::Cancel(a) if verb == "cancel" => a,
                ClientCommand::Terminate(a) if verb == "terminate" => a,
                other => panic!("{other:?}"),
            };
            assert_eq!(a.body(), json!({"reason": "bye", "dry_run": true}));
            let a = match parse(&[verb, "r1"]).unwrap() {
                ClientCommand::Cancel(a) | ClientCommand::Terminate(a) => a,
                other => panic!("{other:?}"),
            };
            assert_eq!(a.body(), json!({}));
        }
        let ClientCommand::Retry(a) = parse(&["retry", "r1", "--dry-run"]).unwrap() else {
            panic!()
        };
        assert!(a.dry_run);
        assert_eq!(parse(&["retry"]).unwrap_err().exit_code(), 2);
    }

    #[test]
    fn result_args() {
        let ClientCommand::Result(a) = parse(&["result", "r1"]).unwrap() else {
            panic!()
        };
        assert_eq!((a.wait, a.follow), (None, false));
        let ClientCommand::Result(a) = parse(&["result", "r1", "--wait", "--follow"]).unwrap()
        else {
            panic!()
        };
        assert_eq!((a.wait, a.follow), (Some(None), true));
        let ClientCommand::Result(a) = parse(&["result", "r1", "--wait=90s"]).unwrap() else {
            panic!()
        };
        assert_eq!(a.wait, Some(Some(Duration::from_secs(90))));
        assert_eq!(
            parse(&["result", "r1", "--wait=7d"])
                .unwrap_err()
                .exit_code(),
            2
        );
    }

    #[test]
    fn token_complete_args() {
        let ClientCommand::Token {
            command: TokenCommand::Complete(a),
        } = parse(&[
            "token",
            "complete",
            "ot1_secret",
            "--payload",
            r#"{"ok":true}"#,
            "--dry-run",
        ])
        .unwrap()
        else {
            panic!()
        };
        assert_eq!(
            a.body(),
            json!({"token": "ot1_secret", "payload": {"ok": true}, "dry_run": true})
        );
        assert_eq!(parse(&["token", "complete"]).unwrap_err().exit_code(), 2);
    }
}
