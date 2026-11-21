//! `ostro worker token {create,list,revoke}` (07-15, WORK-01 / D-W4).
//!
//! | Verb | Request |
//! |---|---|
//! | `create [--ttl 24h]` | `POST /v1/workers/tokens` `{ttl_ms?}` -> 201 `{token_id, token, join_command, expires_at_ms}` |
//! | `list` | `GET /v1/workers/tokens` -> `{tokens: [{token_id, created_at_ms, expires_at_ms?}]}` |
//! | `revoke <id>` | `DELETE /v1/workers/tokens/{id}` -> 204 |
//!
//! The token secret is printed by `create` only, once (the server never shows it again);
//! `list` and `revoke` never see it. The `ostro` binary embeds these verbs under its
//! long-running `ostro worker` command, so they carry their own [`Conn`] and run through
//! [`run`], not through [`super::dispatch`].

use std::time::Duration;

use clap::{Args, Subcommand};
use reqwest::Method;
use serde_json::{Map, Value, json};

use super::{Conn, parse_duration};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// `ostro worker token ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum WorkerTokenCommand {
    /// Create a join token; prints the `ostro worker --join ...` command once.
    Create(CreateArgs),
    /// List join tokens (ids and expiry; never the secret).
    #[command(alias = "ls")]
    List(ListArgs),
    /// Revoke a join token: later joins and renewals with it are refused.
    #[command(alias = "rm")]
    Revoke(RevokeArgs),
}

impl WorkerTokenCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            WorkerTokenCommand::Create(a) => &a.conn,
            WorkerTokenCommand::List(a) => &a.conn,
            WorkerTokenCommand::Revoke(a) => &a.conn,
        }
    }
}

/// `create`.
#[derive(Debug, Clone, Args)]
pub struct CreateArgs {
    /// Token lifetime (friendly duration, e.g. 24h; default: the server's).
    #[arg(long, value_parser = parse_duration)]
    pub ttl: Option<Duration>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `list`.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `revoke`.
#[derive(Debug, Clone, Args)]
pub struct RevokeArgs {
    /// Token id (`wkr_...`, the part before the `.`).
    pub token_id: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// The `create` request body.
pub fn create_body(a: &CreateArgs) -> Value {
    let mut b = Map::new();
    if let Some(ttl) = a.ttl {
        b.insert(
            "ttl_ms".into(),
            json!(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)),
        );
    }
    Value::Object(b)
}

/// A wall-clock time in ms as RFC 3339 (UTC), or the raw value when out of range.
pub(crate) fn ms_time(v: &Value) -> String {
    match v.as_i64() {
        Some(ms) => jiff::Timestamp::from_millisecond(ms)
            .map(|t| t.to_string())
            .unwrap_or_else(|_| ms.to_string()),
        None => s(v),
    }
}

/// Run one `worker token` verb to completion (own current-thread runtime).
pub fn run(cmd: &WorkerTokenCommand) -> Outcome {
    super::block_on(async {
        let conn = cmd.conn();
        let out = conn.out();
        let client = match conn.client() {
            Ok(c) => c,
            Err(e) => return super::report(out, &e),
        };
        match execute(&client, out, cmd).await {
            Ok(o) => o,
            Err(e) => super::report(out, &e),
        }
    })
}

/// Execute one verb on `client`.
pub async fn execute(c: &Client, out: Out, cmd: &WorkerTokenCommand) -> Result<Outcome, CallError> {
    match cmd {
        WorkerTokenCommand::Create(a) => {
            let r = c
                .call(
                    Method::POST,
                    &["v1", "workers", "tokens"],
                    &[],
                    Some(&create_body(a)),
                )
                .await?;
            let b = &r.body;
            if out.json {
                out.json(b);
            } else {
                println!("token id:   {}", s(&b["token_id"]));
                println!("expires at: {}", ms_time(&b["expires_at_ms"]));
                println!("join command (shown once; store the token as a secret):");
                println!("  {}", s(&b["join_command"]));
            }
            Ok(Outcome::Ok)
        }
        WorkerTokenCommand::List(_) => {
            let r = c
                .call(Method::GET, &["v1", "workers", "tokens"], &[], None)
                .await?;
            if out.json {
                out.json(&r.body);
            } else {
                let rows: Vec<Vec<String>> = r.body["tokens"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .map(|t| {
                        vec![
                            s(&t["token_id"]),
                            ms_time(&t["created_at_ms"]),
                            ms_time(&t["expires_at_ms"]),
                        ]
                    })
                    .collect();
                println!("{}", table(&["TOKEN ID", "CREATED", "EXPIRES"], &rows));
            }
            Ok(Outcome::Ok)
        }
        WorkerTokenCommand::Revoke(a) => {
            c.call(
                Method::DELETE,
                &["v1", "workers", "tokens", &a.token_id],
                &[],
                None,
            )
            .await?;
            if out.json {
                out.json(&json!({"token_id": a.token_id, "revoked": true}));
            } else {
                println!("revoked {}", a.token_id);
            }
            Ok(Outcome::Ok)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct T {
        #[command(subcommand)]
        cmd: WorkerTokenCommand,
    }

    fn parse(args: &[&str]) -> Result<WorkerTokenCommand, clap::Error> {
        let mut v = vec!["token"];
        v.extend_from_slice(args);
        T::try_parse_from(v).map(|t| t.cmd)
    }

    #[test]
    fn create_body_carries_ttl_ms() {
        let WorkerTokenCommand::Create(a) = parse(&["create", "--ttl", "24h"]).unwrap() else {
            panic!("create")
        };
        assert_eq!(create_body(&a), json!({"ttl_ms": 86_400_000u64}));
        let WorkerTokenCommand::Create(a) = parse(&["create"]).unwrap() else {
            panic!("create")
        };
        assert_eq!(create_body(&a), json!({}));
        assert_eq!(
            parse(&["create", "--ttl", "1d"]).unwrap_err().exit_code(),
            2
        );
    }

    #[test]
    fn revoke_needs_an_id_and_aliases_work() {
        assert_eq!(parse(&["revoke"]).unwrap_err().exit_code(), 2);
        assert!(matches!(
            parse(&["rm", "wkr_1"]).unwrap(),
            WorkerTokenCommand::Revoke(_)
        ));
        assert!(matches!(
            parse(&["ls"]).unwrap(),
            WorkerTokenCommand::List(_)
        ));
    }

    #[test]
    fn times_render_as_rfc3339() {
        assert_eq!(ms_time(&json!(0)), "1970-01-01T00:00:00Z");
        assert_eq!(ms_time(&Value::Null), "-");
    }
}
