//! `ostro security …` (Phase 14, 14-19; D-P14-10/14/15): the module signing policy and
//! trusted keys against a running server (admin bearer in `OSTRO_TOKEN`).
//!
//! | Verb | Request |
//! |---|---|
//! | `policy get [--tenant t]` | `GET /v1/security/modules/policy` (`--tenant`: show the cluster keys and that tenant's keys only) |
//! | `policy set --require-signed true\|false` | `PUT /v1/security/modules/policy` (cluster policy; keys are kept) |
//! | `trust add --minisign <KEY\|FILE> \| --cosign <FILE> [--comment c] [--tenant t]` | `POST /v1/security/modules/keys` (`key_id = auto`: the server derives it) |
//! | `trust rm <key-id> [--tenant t]` | `DELETE /v1/security/modules/keys/{key_id}` (`<t>:<key-id>` with `--tenant`) |
//!
//! `policy set --tenant t` sends `ModulePolicy.tenant` (p14-fixups-2): `--require-signed
//! true` tightens tenant `t`, `false` clears its tightening (a tenant never relaxes the
//! cluster policy; a tenant admin may only tighten its own tenant).
//! Signatures are attached at upload (`ostro deploy --signature`) or with
//! `POST /v1/modules/{sha}/signatures`; no `modules sign-attach` verb (minimal surface).
//! `--json` prints the REST body.

use std::path::{Path, PathBuf};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use clap::{ArgGroup, Args, Subcommand};
use ostro_client::security::{AUTO_KEY_ID, MAX_SIGNATURE_BYTES, MAX_SIGNATURES};
use serde_json::Value;

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// Largest key file read by `trust add`.
const MAX_KEY_FILE: u64 = 64 * 1024;

/// `ostro security` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum SecurityCmd {
    /// The module signing policy (get, set).
    Policy {
        #[command(subcommand)]
        command: PolicyCmd,
    },
    /// Trusted signing keys (add, rm).
    Trust {
        #[command(subcommand)]
        command: TrustCmd,
    },
}

/// `ostro security policy` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum PolicyCmd {
    /// Show the module policy.
    Get(SecurityArgs),
    /// Set the module policy.
    Set(PolicySetArgs),
}

/// `ostro security trust` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum TrustCmd {
    /// Trust a public key (minisign or cosign).
    Add(TrustAddArgs),
    /// Stop trusting a key.
    Rm(TrustRmArgs),
}

/// `ostro security policy get`.
#[derive(Debug, Clone, Args)]
pub struct SecurityArgs {
    /// Only show the cluster keys and this tenant's keys.
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro security policy set`.
#[derive(Debug, Clone, Args)]
pub struct PolicySetArgs {
    /// Require a valid signature on every upload and activation.
    #[arg(long, required = true)]
    pub require_signed: Option<bool>,
    /// Set this tenant's requirement instead of the cluster policy (`true` tightens the
    /// tenant, `false` clears its tightening).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro security trust add`.
#[derive(Debug, Clone, Args)]
#[command(group(ArgGroup::new("key").required(true).args(["minisign", "cosign"])))]
pub struct TrustAddArgs {
    /// A minisign public key (`RW…`) or a minisign `.pub` file.
    #[arg(long, value_name = "KEY|FILE")]
    pub minisign: Option<String>,
    /// A cosign public key PEM file (P-256).
    #[arg(long, value_name = "FILE")]
    pub cosign: Option<PathBuf>,
    /// A comment stored with the key.
    #[arg(long)]
    pub comment: Option<String>,
    /// Tenant (unset: a cluster key).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro security trust rm`.
#[derive(Debug, Clone, Args)]
pub struct TrustRmArgs {
    /// Key id (hex, as `policy get` shows it).
    pub key_id: String,
    /// The tenant whose key it is (unset: the cluster key, else the one tenant key).
    #[arg(long)]
    pub tenant: Option<String>,
    #[command(flatten)]
    pub conn: Conn,
}

impl SecurityCmd {
    /// The connection flags.
    pub fn conn(&self) -> &Conn {
        match self {
            SecurityCmd::Policy { command } => match command {
                PolicyCmd::Get(a) => &a.conn,
                PolicyCmd::Set(a) => &a.conn,
            },
            SecurityCmd::Trust { command } => match command {
                TrustCmd::Add(a) => &a.conn,
                TrustCmd::Rm(a) => &a.conn,
            },
        }
    }
}

// ---------------------------------------------------------------------------------
// Local checks
// ---------------------------------------------------------------------------------

fn read_small(path: &Path, limit: u64, what: &str) -> Result<Vec<u8>, CallError> {
    let meta = std::fs::metadata(path)
        .map_err(|e| CallError::Usage(format!("cannot read {what} {}: {e}", path.display())))?;
    if meta.len() > limit {
        return Err(CallError::Usage(format!(
            "{what} {} is {} bytes; at most {limit} bytes are accepted",
            path.display(),
            meta.len()
        )));
    }
    std::fs::read(path)
        .map_err(|e| CallError::Usage(format!("cannot read {what} {}: {e}", path.display())))
}

/// Read `--signature` files for an upload: at most 4, each non-empty and ≤ 8 KiB (refused
/// locally with a usage error, before any request).
pub fn read_signatures(paths: &[PathBuf]) -> Result<Vec<Vec<u8>>, CallError> {
    if paths.len() > MAX_SIGNATURES {
        return Err(CallError::Usage(format!(
            "{} signature files given; at most {MAX_SIGNATURES} are accepted",
            paths.len()
        )));
    }
    paths
        .iter()
        .map(|p| {
            let b = read_small(p, MAX_SIGNATURE_BYTES as u64, "signature file")?;
            if b.is_empty() {
                return Err(CallError::Usage(format!(
                    "signature file {} is empty",
                    p.display()
                )));
            }
            Ok(b)
        })
        .collect()
}

/// The key text of `trust add`: the minisign value itself unless it names a file.
pub fn key_text(a: &TrustAddArgs) -> Result<String, CallError> {
    let from_file = |p: &Path| {
        let b = read_small(p, MAX_KEY_FILE, "key file")?;
        String::from_utf8(b)
            .map_err(|_| CallError::Usage(format!("key file {} is not text", p.display())))
    };
    match (&a.minisign, &a.cosign) {
        (Some(m), None) => {
            let p = Path::new(m);
            if p.is_file() {
                from_file(p)
            } else {
                Ok(m.clone())
            }
        }
        (None, Some(c)) => from_file(c),
        _ => Err(CallError::Usage(
            "give exactly one of --minisign or --cosign".into(),
        )),
    }
}

/// The display key id of a minisign public key (`RW…` line or `.pub` file): the 8 key-id
/// bytes as a little-endian u64 in upper-case hex, as `minisign` prints it; `None` when the
/// text is not a minisign key (the server then answers 400).
pub fn minisign_key_id(text: &str) -> Option<String> {
    let line = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty() && !l.starts_with("untrusted comment:"))?;
    let raw = B64.decode(line).ok()?;
    if raw.len() != 42 || &raw[..2] != b"Ed" {
        return None;
    }
    Some(
        raw[2..10]
            .iter()
            .rev()
            .map(|b| format!("{b:02X}"))
            .collect(),
    )
}

// ---------------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------------

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_default()
}

fn keys_of<'a>(body: &'a Value, tenant: Option<&str>) -> Vec<&'a Value> {
    body["trusted_keys"]
        .as_array()
        .map(|ks| {
            ks.iter()
                .filter(|k| match tenant {
                    None => true,
                    Some(t) => k["tenant"].is_null() || k["tenant"].as_str() == Some(t),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn policy_text(body: &Value, tenant: Option<&str>) -> String {
    let rows: Vec<Vec<String>> = keys_of(body, tenant)
        .into_iter()
        .map(|k| {
            vec![
                s(&k["key_id"]),
                s(&k["alg"]),
                k["tenant"].as_str().unwrap_or("(cluster)").to_owned(),
                s(&k["comment"]),
            ]
        })
        .collect();
    let scope = body["tenant"]
        .as_str()
        .map(|t| format!(" (tenant {t})"))
        .unwrap_or_default();
    let mut out = format!(
        "require signed modules{scope}: {}\n",
        body["require_signed_modules"].as_bool().unwrap_or(false)
    );
    if rows.is_empty() {
        out.push_str("trusted keys: none");
    } else {
        out.push_str(&table(&["KEY ID", "ALG", "TENANT", "COMMENT"], &rows));
    }
    out
}

fn filtered(body: &Value, tenant: Option<&str>) -> Value {
    let mut b = body.clone();
    if tenant.is_some() {
        b["trusted_keys"] = Value::Array(keys_of(body, tenant).into_iter().cloned().collect());
    }
    b
}

/// Run one verb; the stdout text.
pub async fn render(c: &Client, json: bool, cmd: &SecurityCmd) -> Result<String, CallError> {
    let (body, head, tenant) = match cmd {
        SecurityCmd::Policy { command } => match command {
            PolicyCmd::Get(a) => (
                c.get_module_policy().await?.body,
                String::new(),
                a.tenant.as_deref(),
            ),
            PolicyCmd::Set(a) => {
                let on = a.require_signed.ok_or_else(|| {
                    CallError::Usage("--require-signed true|false is required".into())
                })?;
                let head = match &a.tenant {
                    Some(t) => format!("module policy of tenant {t} updated\n"),
                    None => "module policy updated\n".to_owned(),
                };
                (
                    c.set_module_policy(on, a.tenant.as_deref()).await?.body,
                    head,
                    a.tenant.as_deref(),
                )
            }
        },
        SecurityCmd::Trust { command } => match command {
            TrustCmd::Add(a) => {
                let key = key_text(a)?;
                // cosign ids are derived by the server from the canonical SPKI.
                let (alg, id) = if a.minisign.is_some() {
                    let id = minisign_key_id(&key).ok_or_else(|| {
                        CallError::Usage("--minisign: not a minisign public key".into())
                    })?;
                    ("minisign", id)
                } else {
                    ("cosign-p256", AUTO_KEY_ID.to_owned())
                };
                let r = c
                    .add_trusted_key(alg, &id, &key, a.comment.as_deref(), a.tenant.as_deref())
                    .await?;
                (r.body, "key trusted\n".to_owned(), a.tenant.as_deref())
            }
            TrustCmd::Rm(a) => {
                let id = match &a.tenant {
                    Some(t) => format!("{t}:{}", a.key_id),
                    None => a.key_id.clone(),
                };
                let r = c.remove_trusted_key(&id).await?;
                (
                    r.body,
                    format!("key {} removed\n", a.key_id),
                    a.tenant.as_deref(),
                )
            }
        },
    };
    if json {
        return Ok(pretty(&filtered(&body, tenant)));
    }
    Ok(format!("{head}{}", policy_text(&body, tenant)))
}

/// Run a security verb, printing its output.
pub async fn run(c: &Client, out: Out, cmd: &SecurityCmd) -> Result<Outcome, CallError> {
    let text = render(c, out.json, cmd).await?;
    println!("{text}");
    Ok(Outcome::Ok)
}
