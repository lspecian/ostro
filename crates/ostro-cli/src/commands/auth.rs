//! `ostro auth …` (Phase 14): API tokens and whoami (14-17, D-P14-4) against a running server
//! with the bearer in `OSTRO_TOKEN`; `ostro auth bootstrap --data-dir` is run by the `ostro`
//! binary (offline, 14-03).
//!
//! | Verb | Request |
//! |---|---|
//! | `token create --name N --scope S… [--tenant t \| --system] [--ttl 720h \| --no-expiry] [--description D]` | `POST /v1/auth/tokens` |
//! | `token ls [--tenant t]` | `GET /v1/auth/tokens` |
//! | `token revoke <id>` | `DELETE /v1/auth/tokens/{id}` |
//! | `token rotate <id> [--keep-old] [--ttl 720h]` | `POST /v1/auth/tokens/{id}/rotate` (`revoke_old = !keep_old`) |
//! | `whoami` | `GET /v1/auth/whoami` |
//!
//! The bearer is printed by `create` and `rotate` only (the server never shows it again;
//! a hint on stderr says so); `ls`, `revoke` and `whoami` never see a secret. `--json` prints
//! the REST body (Phase 10 envelope).

use std::path::PathBuf;
use std::time::Duration;

use clap::{Args, Subcommand};
use serde_json::{Map, Value, json};

use super::worker_tokens::ms_time;
use super::{Conn, parse_duration};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// The stderr hint printed with a new bearer.
pub const STORE_HINT: &str = "ostro: store it now; it cannot be shown again";

/// `ostro auth` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum AuthCmd {
    /// API tokens (create, ls, revoke, rotate).
    Token {
        #[command(subcommand)]
        command: TokenCmd,
    },
    /// Show the calling principal (token id, tenant, scopes).
    Whoami(WhoamiArgs),
    /// Create a system-admin token on a stopped single node (offline; run by the ostro
    /// binary).
    Bootstrap(BootstrapArgs),
}

/// `ostro auth token` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum TokenCmd {
    /// Create an API token (the bearer is printed once).
    Create(TokenCreateArgs),
    /// List API tokens (never a secret).
    #[command(alias = "list")]
    Ls(TokenLsArgs),
    /// Revoke an API token (rejected on its next request).
    #[command(alias = "rm")]
    Revoke(TokenIdArgs),
    /// Rotate an API token: a new bearer with the same name, scopes and tenant; the old
    /// token is revoked unless --keep-old.
    Rotate(TokenRotateArgs),
}

/// `ostro auth token create`.
#[derive(Debug, Clone, Args)]
pub struct TokenCreateArgs {
    /// Token name.
    #[arg(long)]
    pub name: String,
    /// Scopes (repeatable or comma-separated): admin, deploy, operate, trigger, read.
    #[arg(long = "scope", value_delimiter = ',', required = true, value_parser = parse_scope)]
    pub scopes: Vec<String>,
    /// Tenant of the token (a tenant admin's own tenant when unset).
    #[arg(long, conflicts_with = "system")]
    pub tenant: Option<String>,
    /// A system token (no tenant; only admin or read).
    #[arg(long)]
    pub system: bool,
    /// Lifetime (friendly duration, e.g. 720h); unset: `[auth] default_token_ttl`.
    #[arg(long, value_parser = parse_duration, conflicts_with = "no_expiry")]
    pub ttl: Option<Duration>,
    /// No expiry (system admins only).
    #[arg(long)]
    pub no_expiry: bool,
    /// Free-form description.
    #[arg(long, default_value = "")]
    pub description: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth token ls`.
#[derive(Debug, Clone, Args)]
pub struct TokenLsArgs {
    /// Only this tenant's tokens.
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth token revoke <ID>`.
#[derive(Debug, Clone, Args)]
pub struct TokenIdArgs {
    /// Token id (`otk_...`).
    pub id: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth token rotate <ID>`.
#[derive(Debug, Clone, Args)]
pub struct TokenRotateArgs {
    /// Token id (`otk_...`).
    pub id: String,
    /// Keep the old token valid (default: revoke it once the new one exists).
    #[arg(long)]
    pub keep_old: bool,
    /// Lifetime of the new token (default: the old token's lifetime).
    #[arg(long, value_parser = parse_duration)]
    pub ttl: Option<Duration>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth whoami`.
#[derive(Debug, Clone, Args)]
pub struct WhoamiArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro auth bootstrap` (offline, run by the ostro binary).
#[derive(Debug, Clone, Args)]
pub struct BootstrapArgs {
    /// The stopped node's data directory.
    #[arg(long, value_name = "DIR")]
    pub data_dir: PathBuf,
    /// The server configuration file.
    #[arg(long, value_name = "FILE")]
    pub config: Option<PathBuf>,
    /// Token lifetime (friendly duration); unset: no expiry.
    #[arg(long, value_parser = super::parse_duration_text)]
    pub expires: Option<String>,
}

impl AuthCmd {
    /// The connection flags; `None` for `bootstrap` (run by the ostro binary).
    pub fn conn(&self) -> Option<&Conn> {
        match self {
            AuthCmd::Token { command } => Some(match command {
                TokenCmd::Create(a) => &a.conn,
                TokenCmd::Ls(a) => &a.conn,
                TokenCmd::Revoke(a) => &a.conn,
                TokenCmd::Rotate(a) => &a.conn,
            }),
            AuthCmd::Whoami(a) => Some(&a.conn),
            AuthCmd::Bootstrap(_) => None,
        }
    }
}

/// An API scope name (usage error otherwise).
pub fn parse_scope(s: &str) -> Result<String, String> {
    match s.trim() {
        v @ ("admin" | "deploy" | "operate" | "trigger" | "read") => Ok(v.to_owned()),
        other => Err(format!(
            "unknown scope {other:?} (expected admin, deploy, operate, trigger or read)"
        )),
    }
}

fn ms(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// The `create` body (`CreateTokenRequest`).
pub fn create_body(a: &TokenCreateArgs) -> Value {
    let mut m = Map::new();
    m.insert("name".into(), json!(a.name));
    m.insert("scopes".into(), json!(a.scopes));
    if !a.description.is_empty() {
        m.insert("description".into(), json!(a.description));
    }
    if let Some(t) = &a.tenant {
        m.insert("tenant".into(), json!(t));
    }
    if let Some(ttl) = a.ttl {
        m.insert("ttl_ms".into(), json!(ms(ttl)));
    }
    if a.no_expiry {
        m.insert("no_expiry".into(), json!(true));
    }
    Value::Object(m)
}

/// The `rotate` body (`RotateTokenRequest`).
pub fn rotate_body(a: &TokenRotateArgs) -> Value {
    let mut m = Map::new();
    m.insert("revoke_old".into(), json!(!a.keep_old));
    if let Some(ttl) = a.ttl {
        m.insert("ttl_ms".into(), json!(ms(ttl)));
    }
    Value::Object(m)
}

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

fn scopes_text(v: &Value) -> String {
    v.as_array()
        .map(|a| a.iter().map(s).collect::<Vec<_>>().join(","))
        .unwrap_or_else(|| "-".into())
}

fn tenant_text(v: &Value) -> String {
    if v.is_null() {
        "- (system)".into()
    } else {
        s(v)
    }
}

fn expires_text(v: &Value) -> String {
    if v.is_null() {
        "never".into()
    } else {
        ms_time(v)
    }
}

/// The human text of a created / rotated token (the only place a bearer is printed).
fn created_text(b: &Value, extra: &[(&str, String)]) -> String {
    let i = &b["info"];
    let mut rows: Vec<(&str, String)> = vec![
        ("token id", s(&i["id"])),
        ("name", s(&i["name"])),
        ("tenant", tenant_text(&i["tenant"])),
        ("scopes", scopes_text(&i["scopes"])),
        ("expires at", expires_text(&i["expires_at_ms"])),
    ];
    rows.extend(extra.iter().cloned());
    let mut out: Vec<String> = rows
        .iter()
        .map(|(k, v)| format!("{:<14} {v}", format!("{k}:")))
        .collect();
    out.push("bearer (shown once):".into());
    out.push(format!("  {}", s(&b["bearer"])));
    out.join("\n")
}

/// Run one verb; the stdout text and whether a bearer was printed (stderr hint).
pub async fn render(c: &Client, json: bool, cmd: &AuthCmd) -> Result<(String, bool), CallError> {
    match cmd {
        AuthCmd::Token { command } => match command {
            TokenCmd::Create(a) => {
                let r = c.create_api_token(&create_body(a)).await?;
                Ok((
                    if json {
                        pretty(&r.body)
                    } else {
                        created_text(&r.body, &[])
                    },
                    true,
                ))
            }
            TokenCmd::Ls(a) => {
                let r = c.list_api_tokens(a.tenant.as_deref()).await?;
                if json {
                    return Ok((pretty(&r.body), false));
                }
                let rows: Vec<Vec<String>> = r.body["tokens"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or_default()
                    .iter()
                    .map(|t| {
                        vec![
                            s(&t["id"]),
                            s(&t["name"]),
                            tenant_text(&t["tenant"]),
                            scopes_text(&t["scopes"]),
                            expires_text(&t["expires_at_ms"]),
                            s(&t["created_by"]),
                        ]
                    })
                    .collect();
                Ok((
                    table(
                        &["ID", "NAME", "TENANT", "SCOPES", "EXPIRES", "CREATED BY"],
                        &rows,
                    ),
                    false,
                ))
            }
            TokenCmd::Revoke(a) => {
                let r = c.revoke_api_token(&a.id).await?;
                Ok((
                    if json {
                        pretty(&r.body)
                    } else {
                        format!("revoked {}", a.id)
                    },
                    false,
                ))
            }
            TokenCmd::Rotate(a) => {
                let r = c.rotate_api_token(&a.id, &rotate_body(a)).await?;
                let old = if a.keep_old { "kept" } else { "revoked" };
                Ok((
                    if json {
                        pretty(&r.body)
                    } else {
                        created_text(
                            &r.body,
                            &[
                                ("rotated from", a.id.clone()),
                                ("old token", old.to_owned()),
                            ],
                        )
                    },
                    true,
                ))
            }
        },
        AuthCmd::Whoami(_) => {
            let r = c.whoami().await?;
            if json {
                return Ok((pretty(&r.body), false));
            }
            let b = &r.body;
            let rows = [
                ("token id", s(&b["token_id"])),
                ("kind", s(&b["kind"])),
                ("tenant", tenant_text(&b["tenant"])),
                ("scopes", scopes_text(&b["scopes"])),
            ];
            Ok((
                rows.iter()
                    .map(|(k, v)| format!("{:<10} {v}", format!("{k}:")))
                    .collect::<Vec<_>>()
                    .join("\n"),
                false,
            ))
        }
        AuthCmd::Bootstrap(_) => Err(CallError::Usage(
            "`ostro auth bootstrap` is run by the ostro binary".into(),
        )),
    }
}

/// Run an auth verb, printing its output (and the store-it-now hint with a new bearer).
pub async fn run(c: &Client, out: Out, cmd: &AuthCmd) -> Result<Outcome, CallError> {
    let (text, bearer) = render(c, out.json, cmd).await?;
    println!("{text}");
    if bearer {
        eprintln!("{STORE_HINT}");
    }
    Ok(Outcome::Ok)
}
