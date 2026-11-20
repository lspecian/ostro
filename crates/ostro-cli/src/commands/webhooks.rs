//! `ostro webhooks {ls,get,create,update,delete}` (08-12, TRIG-02/TRIG-03).
//!
//! Thin REST client over the 08-10 surface:
//!
//! | Verb | Request |
//! |---|---|
//! | `ls` | `GET /v1/webhooks` |
//! | `get <name>` | `GET /v1/webhooks/{name}` |
//! | `create --name ...` | `POST /v1/webhooks` |
//! | `update <name> --version-tag T ...` | `GET /v1/webhooks/{name}`, then `PUT /v1/webhooks/{name}` with the given fields overlaid and `version_tag = T` (stale = 409) |
//! | `delete <name> [--version-tag T]` | `DELETE /v1/webhooks/{name}[?version_tag=T]` |
//!
//! Secrets: the CLI never takes or prints secret material. `--secret-ref` names a sealed
//! secret (`secret_ref` on the wire); the server never returns secret bytes, and every
//! response body is additionally scrubbed of any field whose name contains "secret"
//! other than `secret_ref` before it is printed ([`scrub_secrets`]).

use clap::{Args, Subcommand, ValueEnum};
use reqwest::Method;
use serde_json::{Map, Value, json};

use super::schedules::{DeleteArgs, ListArgs, NameArgs, kv, pretty, unimplemented_hint};
use super::{Conn, parse_duration};
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// `ostro webhooks ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum WebhooksCommand {
    /// List webhooks.
    Ls(ListArgs),
    /// Show one webhook.
    Get(NameArgs),
    /// Create a webhook (served at POST /hooks/{name}).
    Create(CreateArgs),
    /// Update a webhook (compare-and-set on --version-tag).
    Update(UpdateArgs),
    /// Delete a webhook.
    Delete(DeleteArgs),
}

impl WebhooksCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            WebhooksCommand::Ls(a) => &a.conn,
            WebhooksCommand::Get(a) => &a.conn,
            WebhooksCommand::Create(a) => &a.conn,
            WebhooksCommand::Update(a) => &a.conn,
            WebhooksCommand::Delete(a) => &a.conn,
        }
    }
}

/// What a verified delivery does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ActionArg {
    /// Start a run of --workflow.
    Start,
    /// Signal a run of --workflow.
    Signal,
    /// Complete the await token found at --token-pointer.
    CompleteToken,
}

impl ActionArg {
    /// REST spelling (`action.kind`).
    pub fn as_wire(self) -> &'static str {
        match self {
            ActionArg::Start => "start",
            ActionArg::Signal => "signal",
            ActionArg::CompleteToken => "complete_token",
        }
    }
}

/// Signature verifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum VerifierArg {
    /// No signature (unauthenticated; the server may refuse it).
    None,
    /// Stripe (`Stripe-Signature`).
    Stripe,
    /// GitHub (`X-Hub-Signature-256`).
    Github,
    /// Slack (`X-Slack-Signature`, v0).
    Slack,
    /// Standard Webhooks.
    Standard,
    /// Generic HMAC (see the --hmac-* options).
    Hmac,
}

impl VerifierArg {
    /// REST spelling (`verifier.kind`).
    pub fn as_wire(self) -> &'static str {
        match self {
            VerifierArg::None => "none",
            VerifierArg::Stripe => "stripe",
            VerifierArg::Github => "github",
            VerifierArg::Slack => "slack",
            VerifierArg::Standard => "standard",
            VerifierArg::Hmac => "hmac",
        }
    }
}

/// Generic HMAC algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HmacAlgorithmArg {
    /// HMAC-SHA256 (default).
    Sha256,
    /// HMAC-SHA1 (legacy senders only).
    LegacySha1,
}

/// Generic HMAC signature encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum HmacEncodingArg {
    /// Lowercase hex.
    Hex,
    /// Standard base64.
    Base64,
}

/// Action fields (`create` requires `--action`; `update` keeps the current action and
/// overlays what is given, or starts a fresh one when `--action` changes the kind).
#[derive(Debug, Clone, Default, Args)]
pub struct ActionOpts {
    /// Workflow (start, signal).
    #[arg(long)]
    pub workflow: Option<String>,
    /// Workflow version (start; unset = active).
    #[arg(long)]
    pub version: Option<u32>,
    /// Run input (start): `body` (the whole body) or a JSON pointer into it.
    #[arg(long, value_parser = parse_input_selector)]
    pub input: Option<String>,
    /// JSON pointer to the target run id in the body (signal).
    #[arg(long, value_parser = parse_pointer, conflicts_with = "idempotency_pointer")]
    pub run_id_pointer: Option<String>,
    /// JSON pointer to the target run's start idempotency key in the body (signal).
    #[arg(long, value_parser = parse_pointer)]
    pub idempotency_pointer: Option<String>,
    /// Signal name (signal).
    #[arg(long)]
    pub signal: Option<String>,
    /// JSON pointer to the await token in the body (complete-token).
    #[arg(long, value_parser = parse_pointer)]
    pub token_pointer: Option<String>,
}

/// Verifier fields.
#[derive(Debug, Clone, Default, Args)]
pub struct VerifierOpts {
    /// Name of the sealed secret holding the signing key (never the key itself).
    #[arg(long)]
    pub secret_ref: Option<String>,
    /// Replay window (friendly duration, whole seconds; unset = the verifier default).
    #[arg(long, value_parser = parse_tolerance_s)]
    pub tolerance: Option<u32>,
    /// Delivery idempotency: `header:<name>`, `json:/<pointer>` or `none`.
    #[arg(long, value_parser = parse_idempotency)]
    pub idempotency: Option<String>,
    /// Generic HMAC: signature header.
    #[arg(long)]
    pub hmac_header: Option<String>,
    /// Generic HMAC: signature prefix (e.g. `sha256=`).
    #[arg(long)]
    pub hmac_prefix: Option<String>,
    /// Generic HMAC: algorithm.
    #[arg(long, value_enum)]
    pub hmac_algorithm: Option<HmacAlgorithmArg>,
    /// Generic HMAC: signature encoding.
    #[arg(long, value_enum)]
    pub hmac_encoding: Option<HmacEncodingArg>,
    /// Generic HMAC: timestamp header.
    #[arg(long)]
    pub hmac_timestamp_header: Option<String>,
    /// Generic HMAC: signed template (`{body}` or `{ts}.{body}`).
    #[arg(long)]
    pub hmac_template: Option<String>,
}

impl VerifierOpts {
    fn hmac_fields(&self) -> Vec<(&'static str, Value)> {
        let mut v = Vec::new();
        if let Some(h) = &self.hmac_header {
            v.push(("header", json!(h)));
        }
        if let Some(p) = &self.hmac_prefix {
            v.push(("prefix", json!(p)));
        }
        if let Some(a) = self.hmac_algorithm {
            v.push((
                "algorithm",
                json!(match a {
                    HmacAlgorithmArg::Sha256 => "sha256",
                    HmacAlgorithmArg::LegacySha1 => "legacy_sha1",
                }),
            ));
        }
        if let Some(e) = self.hmac_encoding {
            v.push((
                "encoding",
                json!(match e {
                    HmacEncodingArg::Hex => "hex",
                    HmacEncodingArg::Base64 => "base64",
                }),
            ));
        }
        if let Some(t) = &self.hmac_timestamp_header {
            v.push(("timestamp_header", json!(t)));
        }
        if let Some(t) = &self.hmac_template {
            v.push(("template", json!(t)));
        }
        v
    }
}

const HMAC_KEYS: [&str; 6] = [
    "header",
    "prefix",
    "algorithm",
    "encoding",
    "timestamp_header",
    "template",
];

/// `ostro webhooks create`.
#[derive(Debug, Clone, Args)]
pub struct CreateArgs {
    /// Webhook name (`[A-Za-z0-9_.-]{1,128}`), the `{name}` of `POST /hooks/{name}`.
    #[arg(long)]
    pub name: String,
    /// What a verified delivery does.
    #[arg(long, value_enum)]
    pub action: ActionArg,
    /// Signature verifier.
    #[arg(long, value_enum)]
    pub verifier: VerifierArg,
    #[command(flatten)]
    pub action_opts: ActionOpts,
    #[command(flatten)]
    pub verifier_opts: VerifierOpts,
    #[command(flatten)]
    pub conn: Conn,
}

impl CreateArgs {
    /// The REST body of `POST /v1/webhooks`.
    pub fn body(&self) -> Result<Value, CallError> {
        let mut b = Map::new();
        b.insert("name".into(), json!(self.name));
        b.insert(
            "action".into(),
            build_action(&Value::Null, Some(self.action), &self.action_opts)?,
        );
        b.insert(
            "verifier".into(),
            build_verifier(&Value::Null, Some(self.verifier), &self.verifier_opts)?,
        );
        if let Some(i) = &self.verifier_opts.idempotency {
            b.insert("idempotency".into(), json!(i));
        }
        Ok(Value::Object(b))
    }
}

/// `ostro webhooks update`.
#[derive(Debug, Clone, Args)]
pub struct UpdateArgs {
    /// Webhook name.
    pub name: String,
    /// The record version the update applies to (from `get`); a stale tag is rejected
    /// with HTTP 409.
    #[arg(long)]
    pub version_tag: u64,
    /// Change the action kind (the action is rebuilt from the given fields).
    #[arg(long, value_enum)]
    pub action: Option<ActionArg>,
    /// Change the verifier kind (the verifier is rebuilt from the given fields).
    #[arg(long, value_enum)]
    pub verifier: Option<VerifierArg>,
    #[command(flatten)]
    pub action_opts: ActionOpts,
    #[command(flatten)]
    pub verifier_opts: VerifierOpts,
    #[command(flatten)]
    pub conn: Conn,
}

impl UpdateArgs {
    /// The REST body of `PUT /v1/webhooks/{name}`: `current` (from GET) with the given
    /// fields overlaid and `version_tag` set to the flag.
    pub fn body(&self, current: &Value) -> Result<Value, CallError> {
        let mut b = current.as_object().cloned().unwrap_or_default();
        b.insert("name".into(), json!(self.name));
        b.insert(
            "action".into(),
            build_action(&current["action"], self.action, &self.action_opts)?,
        );
        b.insert(
            "verifier".into(),
            build_verifier(&current["verifier"], self.verifier, &self.verifier_opts)?,
        );
        if let Some(i) = &self.verifier_opts.idempotency {
            b.insert("idempotency".into(), json!(i));
        }
        b.insert("version_tag".into(), json!(self.version_tag));
        Ok(Value::Object(b))
    }
}

fn usage(m: impl Into<String>) -> CallError {
    CallError::Usage(m.into())
}

/// The action object: `current` (or a fresh one when `kind` changes it) with `o`
/// overlaid; required fields of the kind are checked locally.
fn build_action(
    current: &Value,
    kind: Option<ActionArg>,
    o: &ActionOpts,
) -> Result<Value, CallError> {
    let cur_kind = current["kind"].as_str();
    let kind_wire = match (kind, cur_kind) {
        (Some(k), _) => k.as_wire(),
        (None, Some(k)) => k,
        (None, None) => return Err(usage("--action is required")),
    };
    let mut a = match (current.as_object(), cur_kind == Some(kind_wire)) {
        (Some(m), true) => m.clone(),
        _ => Map::new(),
    };
    a.insert("kind".into(), json!(kind_wire));
    let foreign = |flag: &str| {
        usage(format!(
            "{flag} does not apply to --action {}",
            kind_wire.replace('_', "-")
        ))
    };
    match kind_wire {
        "start" => {
            if o.run_id_pointer.is_some() || o.idempotency_pointer.is_some() {
                return Err(foreign("--run-id-pointer/--idempotency-pointer"));
            }
            if o.signal.is_some() {
                return Err(foreign("--signal"));
            }
            if o.token_pointer.is_some() {
                return Err(foreign("--token-pointer"));
            }
            if let Some(w) = &o.workflow {
                a.insert("workflow".into(), json!(w));
            }
            if let Some(v) = o.version {
                a.insert("version".into(), json!(v));
            }
            if let Some(i) = &o.input {
                a.insert("input".into(), json!(i));
            }
            a.entry("input").or_insert_with(|| json!("body"));
            if !a
                .get("workflow")
                .is_some_and(|w| w.as_str().is_some_and(|w| !w.is_empty()))
            {
                return Err(usage("--action start needs --workflow"));
            }
        }
        "signal" => {
            if o.version.is_some() {
                return Err(foreign("--version"));
            }
            if o.input.is_some() {
                return Err(foreign("--input"));
            }
            if o.token_pointer.is_some() {
                return Err(foreign("--token-pointer"));
            }
            if let Some(w) = &o.workflow {
                a.insert("workflow".into(), json!(w));
            }
            if let Some(p) = &o.run_id_pointer {
                a.insert("run_by".into(), json!({ "run_id_pointer": p }));
            }
            if let Some(p) = &o.idempotency_pointer {
                a.insert("run_by".into(), json!({ "idempotency_pointer": p }));
            }
            if let Some(n) = &o.signal {
                a.insert("name".into(), json!(n));
            }
            if !a.contains_key("workflow") {
                return Err(usage("--action signal needs --workflow"));
            }
            if !a.contains_key("run_by") {
                return Err(usage(
                    "--action signal needs --run-id-pointer or --idempotency-pointer",
                ));
            }
            if !a.contains_key("name") {
                return Err(usage("--action signal needs --signal <name>"));
            }
        }
        "complete_token" => {
            if o.workflow.is_some() || o.version.is_some() || o.input.is_some() {
                return Err(foreign("--workflow/--version/--input"));
            }
            if o.run_id_pointer.is_some() || o.idempotency_pointer.is_some() || o.signal.is_some() {
                return Err(foreign("--run-id-pointer/--idempotency-pointer/--signal"));
            }
            if let Some(p) = &o.token_pointer {
                a.insert("token_pointer".into(), json!(p));
            }
            if !a.contains_key("token_pointer") {
                return Err(usage("--action complete-token needs --token-pointer"));
            }
        }
        other => {
            return Err(usage(format!(
                "the current action kind {other:?} is unknown to this CLI; pass --action"
            )));
        }
    }
    Ok(Value::Object(a))
}

/// The verifier object: `current` (or a fresh one when `kind` changes it) with `o`
/// overlaid. `secret_ref` is required unless the kind is `none`; the `--hmac-*` options
/// only apply to `hmac`.
fn build_verifier(
    current: &Value,
    kind: Option<VerifierArg>,
    o: &VerifierOpts,
) -> Result<Value, CallError> {
    let cur_kind = current["kind"].as_str().unwrap_or("none");
    let kind_wire: &str = match kind {
        Some(k) => k.as_wire(),
        None => cur_kind,
    };
    let mut v = match (current.as_object(), cur_kind == kind_wire) {
        (Some(m), true) => m.clone(),
        _ => Map::new(),
    };
    v.insert("kind".into(), json!(kind_wire));
    if let Some(r) = &o.secret_ref {
        v.insert("secret_ref".into(), json!(r));
    }
    if let Some(t) = o.tolerance {
        v.insert("tolerance_s".into(), json!(t));
    }
    let hmac = o.hmac_fields();
    if kind_wire != "hmac" && !hmac.is_empty() {
        return Err(usage("the --hmac-* options apply to --verifier hmac only"));
    }
    for (k, val) in hmac {
        v.insert(k.into(), val);
    }
    if kind_wire != "hmac" {
        for k in HMAC_KEYS {
            v.remove(k);
        }
    }
    let has_ref = v
        .get("secret_ref")
        .and_then(Value::as_str)
        .is_some_and(|r| !r.is_empty());
    if kind_wire != "none" && !has_ref {
        return Err(usage(format!(
            "--verifier {kind_wire} needs --secret-ref <name of a sealed secret>"
        )));
    }
    Ok(Value::Object(v))
}

// ---------------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------------

/// A JSON pointer (must start with `/`).
pub fn parse_pointer(s: &str) -> Result<String, String> {
    if s.starts_with('/') {
        Ok(s.to_owned())
    } else {
        Err(format!(
            "invalid JSON pointer {s:?}: it must start with '/' (e.g. /data/id)"
        ))
    }
}

/// `body` or a JSON pointer.
pub fn parse_input_selector(s: &str) -> Result<String, String> {
    if s == "body" {
        Ok(s.to_owned())
    } else {
        parse_pointer(s).map_err(|e| format!("{e}, or `body` for the whole body"))
    }
}

/// `header:<name>`, `json:/<pointer>` or `none`.
pub fn parse_idempotency(s: &str) -> Result<String, String> {
    let ok = s == "none"
        || s.strip_prefix("header:").is_some_and(|h| !h.is_empty())
        || s.strip_prefix("json:").is_some_and(|p| p.starts_with('/'));
    if ok {
        Ok(s.to_owned())
    } else {
        Err(format!(
            "invalid idempotency {s:?}: expected header:<name>, json:/<pointer> or none"
        ))
    }
}

/// A friendly duration in whole seconds (at least 1 s).
pub fn parse_tolerance_s(s: &str) -> Result<u32, String> {
    let d = parse_duration(s)?;
    if d.subsec_nanos() != 0 || d.as_secs() == 0 {
        return Err(format!(
            "invalid tolerance {s:?}: use whole seconds of at least 1s (e.g. 300s, 5m)"
        ));
    }
    u32::try_from(d.as_secs()).map_err(|_| format!("tolerance {s:?} is too large"))
}

// ---------------------------------------------------------------------------------
// Output
// ---------------------------------------------------------------------------------

/// Remove every object field whose name contains "secret" (case-insensitive), except
/// `secret_ref`, recursively. Defence in depth: the 08-10 DTO carries no secret bytes,
/// and this keeps a misbehaving server from making the CLI print any.
pub fn scrub_secrets(v: &mut Value) {
    match v {
        Value::Object(m) => {
            m.retain(|k, _| k == "secret_ref" || !k.to_ascii_lowercase().contains("secret"));
            for x in m.values_mut() {
                scrub_secrets(x);
            }
        }
        Value::Array(a) => a.iter_mut().for_each(scrub_secrets),
        _ => {}
    }
}

fn target(a: &Value) -> String {
    match a["kind"].as_str() {
        Some("start") => match a["version"].as_u64() {
            Some(v) => format!("{}@{v}", s(&a["workflow"])),
            None => s(&a["workflow"]),
        },
        Some("signal") => format!("{} signal {}", s(&a["workflow"]), s(&a["name"])),
        Some("complete_token") => format!("token at {}", s(&a["token_pointer"])),
        _ => "-".to_owned(),
    }
}

fn run_by(a: &Value) -> String {
    let r = &a["run_by"];
    if let Some(p) = r["run_id_pointer"].as_str() {
        format!("run id at {p}")
    } else if let Some(p) = r["idempotency_pointer"].as_str() {
        format!("idempotency key at {p}")
    } else {
        "-".to_owned()
    }
}

fn detail(b: &Value) -> String {
    let a = &b["action"];
    let v = &b["verifier"];
    let mut rows = vec![
        ("name", s(&b["name"])),
        ("endpoint", format!("POST /hooks/{}", s(&b["name"]))),
        ("action", s(&a["kind"])),
        ("target", target(a)),
    ];
    match a["kind"].as_str() {
        Some("start") => rows.push(("input", s(&a["input"]))),
        Some("signal") => rows.push(("run_by", run_by(a))),
        _ => {}
    }
    rows.push(("verifier", s(&v["kind"])));
    rows.push(("secret_ref", s(&v["secret_ref"])));
    rows.push((
        "tolerance",
        match v["tolerance_s"].as_u64() {
            Some(t) => format!("{t}s"),
            None => "verifier default".to_owned(),
        },
    ));
    for k in HMAC_KEYS {
        if !v[k].is_null() {
            rows.push((k, s(&v[k])));
        }
    }
    rows.push((
        "idempotency",
        match b["idempotency"].as_str() {
            Some(i) => i.to_owned(),
            None => "verifier default".to_owned(),
        },
    ));
    rows.push(("version_tag", s(&b["version_tag"])));
    kv(&rows)
}

fn listing(b: &Value) -> String {
    let rows: Vec<Vec<String>> = b["webhooks"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|w| {
            vec![
                s(&w["name"]),
                s(&w["action"]["kind"]),
                target(&w["action"]),
                s(&w["verifier"]["kind"]),
                s(&w["verifier"]["secret_ref"]),
                match w["idempotency"].as_str() {
                    Some(i) => i.to_owned(),
                    None => "default".to_owned(),
                },
                s(&w["version_tag"]),
            ]
        })
        .collect();
    if rows.is_empty() {
        return "no webhooks".to_owned();
    }
    table(
        &[
            "NAME",
            "ACTION",
            "TARGET",
            "VERIFIER",
            "SECRET_REF",
            "IDEMPOTENCY",
            "TAG",
        ],
        &rows,
    )
}

/// Run one verb and return what it prints on stdout (`--json`: the REST body, scrubbed
/// of secret fields).
pub async fn render(c: &Client, json: bool, cmd: &WebhooksCommand) -> Result<String, CallError> {
    render_inner(c, json, cmd)
        .await
        .map_err(|e| unimplemented_hint("webhooks", e))
}

async fn render_inner(c: &Client, json: bool, cmd: &WebhooksCommand) -> Result<String, CallError> {
    let (head, mut body) = match cmd {
        WebhooksCommand::Ls(_) => {
            let mut r = c.call(Method::GET, &["v1", "webhooks"], &[], None).await?;
            scrub_secrets(&mut r.body);
            return Ok(if json {
                pretty(&r.body)
            } else {
                listing(&r.body)
            });
        }
        WebhooksCommand::Get(a) => {
            let r = c
                .call(Method::GET, &["v1", "webhooks", &a.name], &[], None)
                .await?;
            (None, r.body)
        }
        WebhooksCommand::Create(a) => {
            let body = a.body()?;
            let r = c
                .call(Method::POST, &["v1", "webhooks"], &[], Some(&body))
                .await?;
            (
                Some(format!(
                    "webhook {} created (deliveries: POST /hooks/{})",
                    a.name, a.name
                )),
                r.body,
            )
        }
        WebhooksCommand::Update(a) => {
            let p = ["v1", "webhooks", a.name.as_str()];
            let mut cur = c.call(Method::GET, &p, &[], None).await?;
            scrub_secrets(&mut cur.body);
            let body = a.body(&cur.body)?;
            let r = c.call(Method::PUT, &p, &[], Some(&body)).await?;
            (Some(format!("webhook {} updated", a.name)), r.body)
        }
        WebhooksCommand::Delete(a) => {
            let q: Vec<(&str, String)> = a
                .version_tag
                .map(|t| ("version_tag", t.to_string()))
                .into_iter()
                .collect();
            let r = c
                .call(Method::DELETE, &["v1", "webhooks", &a.name], &q, None)
                .await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(format!("webhook {} deleted", a.name));
        }
    };
    scrub_secrets(&mut body);
    if json {
        return Ok(pretty(&body));
    }
    Ok(match head {
        Some(h) => format!("{h}\n{}", detail(&body)),
        None => detail(&body),
    })
}

/// Run one verb, printing its output.
pub async fn run(c: &Client, out: Out, cmd: &WebhooksCommand) -> Result<Outcome, CallError> {
    let text = render(c, out.json, cmd).await?;
    println!("{text}");
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::super::ClientCommand;
    use super::super::tests::parse;
    use super::*;

    fn hooks(args: &[&str]) -> Result<WebhooksCommand, clap::Error> {
        let mut v = vec!["webhooks"];
        v.extend_from_slice(args);
        parse(&v).map(|c| match c {
            ClientCommand::Webhooks { command } => command,
            other => panic!("not webhooks: {other:?}"),
        })
    }

    fn create(args: &[&str]) -> Result<Value, CallError> {
        let mut v = vec!["create", "--name", "h"];
        v.extend_from_slice(args);
        let WebhooksCommand::Create(a) = hooks(&v).expect("parse") else {
            panic!("create")
        };
        a.body()
    }

    #[test]
    fn create_bodies() {
        assert_eq!(
            create(&[
                "--action",
                "start",
                "--workflow",
                "w",
                "--verifier",
                "stripe",
                "--secret-ref",
                "stripe-key",
                "--tolerance",
                "5m",
                "--idempotency",
                "header:Idempotency-Key",
            ])
            .unwrap(),
            json!({"name": "h",
                "action": {"kind": "start", "workflow": "w", "input": "body"},
                "verifier": {"kind": "stripe", "secret_ref": "stripe-key", "tolerance_s": 300},
                "idempotency": "header:Idempotency-Key"})
        );
        assert_eq!(
            create(&[
                "--action",
                "signal",
                "--workflow",
                "w",
                "--run-id-pointer",
                "/run",
                "--signal",
                "paid",
                "--verifier",
                "hmac",
                "--secret-ref",
                "k",
                "--hmac-header",
                "X-Sig",
                "--hmac-prefix",
                "sha256=",
                "--hmac-algorithm",
                "legacy-sha1",
                "--hmac-encoding",
                "base64",
                "--hmac-timestamp-header",
                "X-Ts",
                "--hmac-template",
                "{ts}.{body}",
            ])
            .unwrap(),
            json!({"name": "h",
                "action": {"kind": "signal", "workflow": "w",
                    "run_by": {"run_id_pointer": "/run"}, "name": "paid"},
                "verifier": {"kind": "hmac", "secret_ref": "k", "header": "X-Sig",
                    "prefix": "sha256=", "algorithm": "legacy_sha1", "encoding": "base64",
                    "timestamp_header": "X-Ts", "template": "{ts}.{body}"}})
        );
        assert_eq!(
            create(&[
                "--action",
                "complete-token",
                "--token-pointer",
                "/t",
                "--verifier",
                "none"
            ])
            .unwrap(),
            json!({"name": "h", "action": {"kind": "complete_token", "token_pointer": "/t"},
                "verifier": {"kind": "none"}})
        );
    }

    #[test]
    fn local_validation() {
        for (args, want) in [
            (
                &[
                    "--action",
                    "start",
                    "--verifier",
                    "github",
                    "--secret-ref",
                    "k",
                ][..],
                "--workflow",
            ),
            (
                &[
                    "--action",
                    "start",
                    "--workflow",
                    "w",
                    "--verifier",
                    "github",
                ][..],
                "--secret-ref",
            ),
            (
                &[
                    "--action",
                    "signal",
                    "--workflow",
                    "w",
                    "--signal",
                    "s",
                    "--verifier",
                    "none",
                ][..],
                "--run-id-pointer",
            ),
            (
                &["--action", "complete-token", "--verifier", "none"][..],
                "--token-pointer",
            ),
            (
                &[
                    "--action",
                    "start",
                    "--workflow",
                    "w",
                    "--token-pointer",
                    "/t",
                    "--verifier",
                    "none",
                ][..],
                "does not apply",
            ),
            (
                &[
                    "--action",
                    "start",
                    "--workflow",
                    "w",
                    "--verifier",
                    "slack",
                    "--secret-ref",
                    "k",
                    "--hmac-header",
                    "X",
                ][..],
                "hmac only",
            ),
        ] {
            let e = create(args).unwrap_err();
            assert!(matches!(e, CallError::Usage(_)), "{args:?}");
            assert!(e.message().contains(want), "{args:?}: {}", e.message());
        }
        // clap-level: bad enum values, pointers, idempotency, tolerance, no secret flag.
        for args in [
            &[
                "create",
                "--name",
                "h",
                "--action",
                "nope",
                "--verifier",
                "none",
            ][..],
            &[
                "create",
                "--name",
                "h",
                "--action",
                "start",
                "--verifier",
                "md5",
            ][..],
            &[
                "create",
                "--name",
                "h",
                "--action",
                "complete-token",
                "--token-pointer",
                "t",
                "--verifier",
                "none",
            ][..],
            &[
                "create",
                "--name",
                "h",
                "--action",
                "start",
                "--verifier",
                "none",
                "--idempotency",
                "cookie:x",
            ][..],
            &[
                "create",
                "--name",
                "h",
                "--action",
                "start",
                "--verifier",
                "none",
                "--tolerance",
                "500ms",
            ][..],
            &[
                "create",
                "--name",
                "h",
                "--action",
                "start",
                "--verifier",
                "stripe",
                "--secret",
                "whsec_x",
            ][..],
        ] {
            assert_eq!(hooks(args).unwrap_err().exit_code(), 2, "{args:?}");
        }
    }

    #[test]
    fn update_overlays_current() {
        let WebhooksCommand::Update(a) = hooks(&[
            "update",
            "h",
            "--version-tag",
            "2",
            "--secret-ref",
            "new-key",
            "--workflow",
            "w2",
        ])
        .unwrap() else {
            panic!("update")
        };
        let cur = json!({"name": "h", "action": {"kind": "start", "workflow": "w", "input": "/data"},
            "verifier": {"kind": "github", "secret_ref": "old", "tolerance_s": 60},
            "idempotency": null, "version_tag": 3});
        assert_eq!(
            a.body(&cur).unwrap(),
            json!({"name": "h", "action": {"kind": "start", "workflow": "w2", "input": "/data"},
                "verifier": {"kind": "github", "secret_ref": "new-key", "tolerance_s": 60},
                "idempotency": null, "version_tag": 2})
        );
        // Switching the verifier kind starts a fresh verifier (old options dropped).
        let WebhooksCommand::Update(a) =
            hooks(&["update", "h", "--version-tag", "3", "--verifier", "none"]).unwrap()
        else {
            panic!("update")
        };
        assert_eq!(a.body(&cur).unwrap()["verifier"], json!({"kind": "none"}));
    }

    #[test]
    fn scrub_removes_secret_fields_but_keeps_secret_ref() {
        let mut v = json!({"webhooks": [{"verifier": {"secret_ref": "k", "secret": "whsec_X",
            "Signing_Secret": "y"}, "client_secret_value": 1}]});
        scrub_secrets(&mut v);
        assert_eq!(v, json!({"webhooks": [{"verifier": {"secret_ref": "k"}}]}));
    }
}
