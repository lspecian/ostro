//! `ostro secret|grant|connection|egress` (06-16, D-FX9): the effects-bridge admin
//! verbs over the 06-08 REST surface.
//!
//! | Verb | Request |
//! |---|---|
//! | `secret set <name> [--tenant T] [--from-file P \| --from-env V]` | `PUT /v1/secrets/{name}[?tenant=T]` |
//! | `secret ls [--tenant T]` | `GET /v1/secrets[?tenant=T]` |
//! | `secret rm <name> [--tenant T]` | `DELETE /v1/secrets/{name}[?tenant=T]` |
//! | `grant add --module M --secret S --origin O... [--body]` | `PUT /v1/grants` |
//! | `grant ls [--module M]` | `GET /v1/grants[?module=M]` |
//! | `grant rm <id>` | `DELETE /v1/grants/{id}` |
//! | `connection set <name> --kind K --config-file P\|-` | `PUT /v1/connections/{name}` |
//! | `connection ls` / `get <name>` / `rm <name>` | `GET /v1/connections`, `GET` / `DELETE /v1/connections/{name}` |
//! | `egress set <module> --allow RULE...` | `PUT /v1/egress/{module}` |
//! | `egress ls` / `get <module>` / `rm <module>` | `GET /v1/egress`, `GET` / `DELETE /v1/egress/{module}` |
//!
//! **Secret values never travel through argv.** `secret set` reads the value from stdin
//! (to EOF; one trailing newline is dropped, so `echo "$V" | ostro secret set n` works),
//! from `--from-file` (same newline rule) or from the environment variable named by
//! `--from-env` (verbatim). A terminal on stdin is refused with a hint instead of waiting
//! for typed input. The value is sent as `value` when it is UTF-8 and as `value_b64`
//! otherwise; no verb ever prints it (the server never returns it either).
//!
//! `--json` prints the REST body verbatim (`null` for a delete's empty 204 body).

use std::io::{IsTerminal, Read};
use std::path::PathBuf;

use clap::{Args, Subcommand};
use reqwest::Method;
use serde_json::{Value, json};

use super::Conn;
use super::schedules::{kv, pretty};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, compact, s, table};

/// Connection kinds (`--kind`).
pub const CONNECTION_KINDS: [&str; 4] = ["sql-postgres", "sql-mysql", "object", "http-target"];

// ---------------------------------------------------------------------------------
// clap definitions
// ---------------------------------------------------------------------------------

/// `ostro secret ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum SecretCommand {
    /// Seal and store a secret. The value is read from stdin (default), --from-file or
    /// --from-env; never from the command line.
    Set(SecretSetArgs),
    /// List secret names and sealing key ids (values are never shown).
    Ls(SecretLsArgs),
    /// Delete a secret.
    Rm(SecretRmArgs),
}

/// `ostro secret set`.
#[derive(Debug, Clone, Args)]
pub struct SecretSetArgs {
    /// Secret name (`[a-z0-9][a-z0-9_.-/]*`, not under `__ostro/`).
    pub name: String,
    /// Tenant scope (unset = global / default tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    /// Read the value from this file (one trailing newline is dropped).
    #[arg(long, conflicts_with = "from_env")]
    pub from_file: Option<PathBuf>,
    /// Read the value from this environment variable (verbatim).
    #[arg(long)]
    pub from_env: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro secret ls`.
#[derive(Debug, Clone, Args)]
pub struct SecretLsArgs {
    /// Tenant scope (unset = global / default tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro secret rm`.
#[derive(Debug, Clone, Args)]
pub struct SecretRmArgs {
    /// Secret name.
    pub name: String,
    /// Tenant scope (unset = global / default tenant).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro grant ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum GrantCommand {
    /// Allow a module to send a secret to the given origins.
    Add(GrantAddArgs),
    /// List secret grants.
    Ls(GrantLsArgs),
    /// Delete a grant by id (`secret:<module>:<secret>`).
    Rm(IdArgs),
}

/// `ostro grant add`.
#[derive(Debug, Clone, Args)]
pub struct GrantAddArgs {
    /// Tenant-qualified deployed module name (`<tenant>/<name>`, or `<name>`).
    #[arg(long)]
    pub module: String,
    /// Secret name.
    #[arg(long)]
    pub secret: String,
    /// Origin pattern the secret may be sent to (`https://api.example.com`,
    /// `https://*.example.com:8443`); repeatable.
    #[arg(long = "origin", required = true)]
    pub origins: Vec<String>,
    /// Also allow `{{secret:..}}` placeholders in the request body.
    #[arg(long)]
    pub body: bool,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro grant ls`.
#[derive(Debug, Clone, Args)]
pub struct GrantLsArgs {
    /// Only this module's grants.
    #[arg(long)]
    pub module: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// A verb on one id / name.
#[derive(Debug, Clone, Args)]
pub struct IdArgs {
    /// Id or name.
    pub id: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// A listing verb.
#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro connection ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum ConnectionCommand {
    /// Create or replace a connection from a JSON config (`{"v": 1, ...}`).
    Set(ConnectionSetArgs),
    /// List connections.
    Ls(ListArgs),
    /// Show one connection (its config verbatim: templates, not secret values).
    Get(IdArgs),
    /// Delete a connection.
    Rm(IdArgs),
}

/// `ostro connection set`.
#[derive(Debug, Clone, Args)]
pub struct ConnectionSetArgs {
    /// Connection name (an http-target is named by its origin, `https://api.example.com`).
    pub name: String,
    /// Connection kind.
    #[arg(long, value_parser = CONNECTION_KINDS)]
    pub kind: String,
    /// JSON config file, or `-` for stdin.
    #[arg(long)]
    pub config_file: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro egress ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum EgressCommand {
    /// Set a module's egress allow list (replaces the previous one).
    Set(EgressSetArgs),
    /// List egress policies.
    Ls(ListArgs),
    /// Show one module's egress policy.
    Get(IdArgs),
    /// Delete a module's egress policy (the module then has no egress).
    Rm(IdArgs),
}

/// `ostro egress set`.
#[derive(Debug, Clone, Args)]
pub struct EgressSetArgs {
    /// Tenant-qualified deployed module name.
    pub module: String,
    /// Allowed origin pattern, IP/CIDR rule or `connection:<name>`; repeatable.
    #[arg(long = "allow", required = true)]
    pub allow: Vec<String>,
    #[command(flatten)]
    pub conn: Conn,
}

impl SecretCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            SecretCommand::Set(a) => &a.conn,
            SecretCommand::Ls(a) => &a.conn,
            SecretCommand::Rm(a) => &a.conn,
        }
    }
}

impl GrantCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            GrantCommand::Add(a) => &a.conn,
            GrantCommand::Ls(a) => &a.conn,
            GrantCommand::Rm(a) => &a.conn,
        }
    }
}

impl ConnectionCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            ConnectionCommand::Set(a) => &a.conn,
            ConnectionCommand::Ls(a) => &a.conn,
            ConnectionCommand::Get(a) | ConnectionCommand::Rm(a) => &a.conn,
        }
    }
}

impl EgressCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            EgressCommand::Set(a) => &a.conn,
            EgressCommand::Ls(a) => &a.conn,
            EgressCommand::Get(a) | EgressCommand::Rm(a) => &a.conn,
        }
    }
}

// ---------------------------------------------------------------------------------
// Secret value input
// ---------------------------------------------------------------------------------

/// Drop one trailing `\n` (or `\r\n`).
fn chomp(mut v: Vec<u8>) -> Vec<u8> {
    if v.last() == Some(&b'\n') {
        v.pop();
        if v.last() == Some(&b'\r') {
            v.pop();
        }
    }
    v
}

/// The secret value of `secret set`, from the chosen source. Errors never contain it.
pub fn read_secret_value(a: &SecretSetArgs) -> Result<Vec<u8>, CallError> {
    let v = if let Some(var) = &a.from_env {
        std::env::var_os(var)
            .ok_or_else(|| CallError::Usage(format!("environment variable {var} is not set")))?
            .into_encoded_bytes()
    } else if let Some(p) = &a.from_file {
        chomp(
            std::fs::read(p)
                .map_err(|e| CallError::Usage(format!("cannot read {}: {e}", p.display())))?,
        )
    } else {
        let mut stdin = std::io::stdin();
        if stdin.is_terminal() {
            return Err(CallError::Usage(
                "the secret value is read from stdin, which is a terminal: pipe it in \
                 (`printf %s \"$VALUE\" | ostro secret set NAME`) or use --from-file / \
                 --from-env (values are never accepted as arguments)"
                    .into(),
            ));
        }
        let mut buf = Vec::new();
        stdin
            .read_to_end(&mut buf)
            .map_err(|e| CallError::Usage(format!("cannot read stdin: {e}")))?;
        chomp(buf)
    };
    if v.is_empty() {
        return Err(CallError::Usage("the secret value is empty".into()));
    }
    Ok(v)
}

/// Standard base64 with padding (binary secret values travel as `value_b64`).
pub fn base64(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(char::from(A[((n >> (18 - 6 * i)) & 63) as usize]));
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// The `PUT /v1/secrets/{name}` body for `value`.
pub fn secret_body(value: Vec<u8>) -> Value {
    match String::from_utf8(value) {
        Ok(text) => json!({ "value": text }),
        Err(e) => json!({ "value_b64": base64(e.as_bytes()) }),
    }
}

// ---------------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------------

fn tenant_query(t: &Option<String>) -> Vec<(&'static str, String)> {
    t.iter().map(|t| ("tenant", t.clone())).collect()
}

fn list_of<'a>(b: &'a Value, key: &str) -> &'a [Value] {
    b[key].as_array().map(Vec::as_slice).unwrap_or_default()
}

fn secrets_table(b: &Value) -> String {
    let rows: Vec<Vec<String>> = list_of(b, "secrets")
        .iter()
        .map(|x| vec![s(&x["name"]), s(&x["key_id"]), s(&x["tenant"])])
        .collect();
    if rows.is_empty() {
        return "no secrets".to_owned();
    }
    table(&["NAME", "KEY_ID", "TENANT"], &rows)
}

fn origins(x: &Value) -> String {
    x["origins"]
        .as_array()
        .map(|a| a.iter().map(s).collect::<Vec<_>>().join(","))
        .unwrap_or_default()
}

fn grants_table(b: &Value) -> String {
    let rows: Vec<Vec<String>> = list_of(b, "grants")
        .iter()
        .map(|x| {
            vec![
                s(&x["id"]),
                s(&x["module"]),
                s(&x["secret"]),
                origins(x),
                s(&x["body"]),
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no grants".to_owned();
    }
    table(&["ID", "MODULE", "SECRET", "ORIGINS", "BODY"], &rows)
}

fn connections_table(b: &Value) -> String {
    let rows: Vec<Vec<String>> = list_of(b, "connections")
        .iter()
        .map(|x| vec![s(&x["name"]), s(&x["kind"]), compact(&x["config"])])
        .collect();
    if rows.is_empty() {
        return "no connections".to_owned();
    }
    table(&["NAME", "KIND", "CONFIG"], &rows)
}

fn allow(x: &Value) -> String {
    x["allow"]
        .as_array()
        .map(|a| a.iter().map(s).collect::<Vec<_>>().join(","))
        .unwrap_or_default()
}

fn egress_table(b: &Value) -> String {
    let rows: Vec<Vec<String>> = list_of(b, "policies")
        .iter()
        .map(|x| vec![s(&x["module"]), allow(x)])
        .collect();
    if rows.is_empty() {
        return "no egress policies".to_owned();
    }
    table(&["MODULE", "ALLOW"], &rows)
}

fn read_config(path: &str) -> Result<Value, CallError> {
    let text = if path == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .map_err(|e| CallError::Usage(format!("cannot read stdin: {e}")))?;
        buf
    } else {
        std::fs::read_to_string(path)
            .map_err(|e| CallError::Usage(format!("cannot read {path}: {e}")))?
    };
    serde_json::from_str(&text)
        .map_err(|e| CallError::Usage(format!("config {path} is not valid JSON: {e}")))
}

// ---------------------------------------------------------------------------------
// Verbs
// ---------------------------------------------------------------------------------

/// Run one `secret` verb and return what it prints on stdout.
pub async fn render_secret(
    c: &Client,
    json: bool,
    cmd: &SecretCommand,
) -> Result<String, CallError> {
    match cmd {
        SecretCommand::Set(a) => {
            let body = secret_body(read_secret_value(a)?);
            let r = c
                .call(
                    Method::PUT,
                    &["v1", "secrets", &a.name],
                    &tenant_query(&a.tenant),
                    Some(&body),
                )
                .await?;
            if json {
                return Ok(pretty(&r.body));
            }
            Ok(format!(
                "secret {} set (sealed with {})",
                a.name,
                s(&r.body["key_id"])
            ))
        }
        SecretCommand::Ls(a) => {
            let r = c
                .call(
                    Method::GET,
                    &["v1", "secrets"],
                    &tenant_query(&a.tenant),
                    None,
                )
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                secrets_table(&r.body)
            })
        }
        SecretCommand::Rm(a) => {
            let r = c
                .call(
                    Method::DELETE,
                    &["v1", "secrets", &a.name],
                    &tenant_query(&a.tenant),
                    None,
                )
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                format!("secret {} deleted", a.name)
            })
        }
    }
}

/// Run one `grant` verb and return what it prints on stdout.
pub async fn render_grant(c: &Client, json: bool, cmd: &GrantCommand) -> Result<String, CallError> {
    match cmd {
        GrantCommand::Add(a) => {
            let body = json!({
                "module": a.module,
                "secret": a.secret,
                "origins": a.origins,
                "body": a.body,
            });
            let r = c
                .call(Method::PUT, &["v1", "grants"], &[], Some(&body))
                .await?;
            if json {
                return Ok(pretty(&r.body));
            }
            Ok(format!(
                "grant {} stored\n{}",
                s(&r.body["id"]),
                kv(&[
                    ("module", s(&r.body["module"])),
                    ("secret", s(&r.body["secret"])),
                    ("origins", origins(&r.body)),
                    ("body", s(&r.body["body"])),
                ])
            ))
        }
        GrantCommand::Ls(a) => {
            let q: Vec<(&str, String)> = a.module.iter().map(|m| ("module", m.clone())).collect();
            let r = c.call(Method::GET, &["v1", "grants"], &q, None).await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                grants_table(&r.body)
            })
        }
        GrantCommand::Rm(a) => {
            let r = c
                .call(Method::DELETE, &["v1", "grants", &a.id], &[], None)
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                format!("grant {} deleted", a.id)
            })
        }
    }
}

/// Run one `connection` verb and return what it prints on stdout.
pub async fn render_connection(
    c: &Client,
    json: bool,
    cmd: &ConnectionCommand,
) -> Result<String, CallError> {
    let detail = |b: &Value| {
        kv(&[
            ("name", s(&b["name"])),
            ("kind", s(&b["kind"])),
            ("config", compact(&b["config"])),
        ])
    };
    match cmd {
        ConnectionCommand::Set(a) => {
            let body = json!({ "kind": a.kind, "config": read_config(&a.config_file)? });
            let r = c
                .call(
                    Method::PUT,
                    &["v1", "connections", &a.name],
                    &[],
                    Some(&body),
                )
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                format!("connection {} stored\n{}", a.name, detail(&r.body))
            })
        }
        ConnectionCommand::Ls(_) => {
            let r = c
                .call(Method::GET, &["v1", "connections"], &[], None)
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                connections_table(&r.body)
            })
        }
        ConnectionCommand::Get(a) => {
            let r = c
                .call(Method::GET, &["v1", "connections", &a.id], &[], None)
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                detail(&r.body)
            })
        }
        ConnectionCommand::Rm(a) => {
            let r = c
                .call(Method::DELETE, &["v1", "connections", &a.id], &[], None)
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                format!("connection {} deleted", a.id)
            })
        }
    }
}

/// Run one `egress` verb and return what it prints on stdout.
pub async fn render_egress(
    c: &Client,
    json: bool,
    cmd: &EgressCommand,
) -> Result<String, CallError> {
    let detail = |b: &Value| kv(&[("module", s(&b["module"])), ("allow", allow(b))]);
    match cmd {
        EgressCommand::Set(a) => {
            let body = json!({ "allow": a.allow });
            let r = c
                .call(Method::PUT, &["v1", "egress", &a.module], &[], Some(&body))
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                format!("egress policy of {} stored\n{}", a.module, detail(&r.body))
            })
        }
        EgressCommand::Ls(_) => {
            let r = c.call(Method::GET, &["v1", "egress"], &[], None).await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                egress_table(&r.body)
            })
        }
        EgressCommand::Get(a) => {
            let r = c
                .call(Method::GET, &["v1", "egress", &a.id], &[], None)
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                detail(&r.body)
            })
        }
        EgressCommand::Rm(a) => {
            let r = c
                .call(Method::DELETE, &["v1", "egress", &a.id], &[], None)
                .await?;
            Ok(if json {
                pretty(&r.body)
            } else {
                format!("egress policy of {} deleted", a.id)
            })
        }
    }
}

fn print(text: String) -> Outcome {
    println!("{text}");
    Outcome::Ok
}

/// `ostro secret ...`.
pub async fn run_secret(c: &Client, out: Out, cmd: &SecretCommand) -> Result<Outcome, CallError> {
    render_secret(c, out.json, cmd).await.map(print)
}

/// `ostro grant ...`.
pub async fn run_grant(c: &Client, out: Out, cmd: &GrantCommand) -> Result<Outcome, CallError> {
    render_grant(c, out.json, cmd).await.map(print)
}

/// `ostro connection ...`.
pub async fn run_connection(
    c: &Client,
    out: Out,
    cmd: &ConnectionCommand,
) -> Result<Outcome, CallError> {
    render_connection(c, out.json, cmd).await.map(print)
}

/// `ostro egress ...`.
pub async fn run_egress(c: &Client, out: Out, cmd: &EgressCommand) -> Result<Outcome, CallError> {
    render_egress(c, out.json, cmd).await.map(print)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    #[test]
    fn base64_matches_rfc4648_vectors() {
        for (i, o) in [
            ("", ""),
            ("f", "Zg=="),
            ("fo", "Zm8="),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg=="),
            ("fooba", "Zm9vYmE="),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64(i.as_bytes()), o, "{i}");
        }
        assert_eq!(base64(&[0xff, 0xfe, 0x00]), "//4A");
    }

    #[test]
    fn values_are_sent_as_text_or_base64() {
        assert_eq!(
            secret_body(b"sk_live_123".to_vec()),
            json!({"value": "sk_live_123"})
        );
        assert_eq!(
            secret_body(vec![0xff, 0x00, 0x01]),
            json!({"value_b64": "/wAB"})
        );
        assert_eq!(chomp(b"v\n".to_vec()), b"v");
        assert_eq!(chomp(b"v\r\n".to_vec()), b"v");
        assert_eq!(chomp(b"v\n\n".to_vec()), b"v\n");
        assert_eq!(chomp(b"v".to_vec()), b"v");
    }

    #[test]
    fn secret_set_never_takes_a_value_argument() {
        // A second positional (a value on argv) is a usage error.
        let e = parse(&["secret", "set", "stripe/live", "sk_live_123"]).unwrap_err();
        assert_eq!(e.exit_code(), 2);
        assert!(parse(&["secret", "set", "n", "--value", "x"]).is_err());
        let ClientCommand::Secret {
            command: SecretCommand::Set(a),
        } = parse(&["secret", "set", "n", "--from-env", "V", "--tenant", "acme"]).unwrap()
        else {
            panic!("secret set")
        };
        assert_eq!(a.from_env.as_deref(), Some("V"));
        assert_eq!(a.tenant.as_deref(), Some("acme"));
        assert!(parse(&["secret", "set", "n", "--from-env", "V", "--from-file", "f"]).is_err());
    }

    #[test]
    fn from_env_and_file_sources() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("v");
        std::fs::write(&p, "file-value-123\n").unwrap();
        let ClientCommand::Secret {
            command: SecretCommand::Set(a),
        } = parse(&["secret", "set", "n", "--from-file", p.to_str().unwrap()]).unwrap()
        else {
            panic!("secret set")
        };
        assert_eq!(read_secret_value(&a).unwrap(), b"file-value-123");
        let ClientCommand::Secret {
            command: SecretCommand::Set(a),
        } = parse(&[
            "secret",
            "set",
            "n",
            "--from-env",
            "OSTRO_TEST_UNSET_VAR_06_16",
        ])
        .unwrap()
        else {
            panic!("secret set")
        };
        let e = read_secret_value(&a).unwrap_err();
        assert!(e.message().contains("is not set"), "{}", e.message());
    }

    #[test]
    fn other_verbs_parse() {
        let ClientCommand::Grant {
            command: GrantCommand::Add(a),
        } = parse(&[
            "grant",
            "add",
            "--module",
            "acme/m",
            "--secret",
            "s",
            "--origin",
            "https://a.example",
            "--origin",
            "https://b.example",
            "--body",
        ])
        .unwrap()
        else {
            panic!("grant add")
        };
        assert_eq!(a.origins.len(), 2);
        assert!(a.body);
        assert!(parse(&["grant", "add", "--module", "m", "--secret", "s"]).is_err());
        assert!(
            parse(&[
                "connection",
                "set",
                "c",
                "--kind",
                "ftp",
                "--config-file",
                "-"
            ])
            .is_err()
        );
        assert!(
            parse(&[
                "connection",
                "set",
                "c",
                "--kind",
                "object",
                "--config-file",
                "-"
            ])
            .is_ok()
        );
        assert!(parse(&["egress", "set", "m"]).is_err());
        assert!(parse(&["egress", "set", "m", "--allow", "connection:db", "--json"]).is_ok());
        assert!(parse(&["egress", "rm", "m"]).is_ok());
        assert!(parse(&["connection", "ls"]).is_ok());
        assert!(parse(&["secret", "ls", "--tenant", "acme"]).is_ok());
    }
}
