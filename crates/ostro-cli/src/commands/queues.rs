//! `ostro queue {flow,pause,resume,demand}` (07-15, OPS-01 / DISP-03 / WORK-05).
//!
//! | Verb | Request |
//! |---|---|
//! | `flow <q>` | `GET /v1/queues/{q}/flow` -> `{queue, version, <flow fields>}` |
//! | `flow <q> --set k=v ... / --file F` | `GET` (current fields and version), overlay, then `PUT /v1/queues/{q}/flow` `{<flow fields>, expected_version}` (stale = 412) |
//! | `pause` / `resume <q>` | `POST /v1/queues/{q}/pause` / `.../resume` -> `{name, paused, changed}` |
//! | `demand <q>` | `GET /v1/queues/{q}/demand` -> the D-W12 JSON |
//!
//! Human output of `flow` is a small TOML subset that `--file` reads back: one
//! `key = <value>` line per scalar or array field (values written as JSON, which is valid
//! TOML for strings, numbers, booleans and string arrays) and one `[table]` per object
//! field (`spill`, `throttle`, `rate_limit`, ...). `--file` also accepts a JSON object.
//!
//! `--set` keys may be dotted (`spill.cpu_pct=70`, `throttle.limit=5`). The value is
//! parsed as JSON when it is valid JSON (`5`, `true`, `null`, `["zone=a"]`) and taken as
//! a string otherwise (`placement=workers`); `null` removes the field (server default).
//! A plain string given for `selector` becomes a one-entry list.

use clap::{Args, Subcommand};
use reqwest::Method;
use serde_json::{Map, Value};

use super::Conn;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s};

/// `ostro queue ...`.
#[derive(Debug, Clone, Subcommand)]
pub enum QueueCommand {
    /// Show a queue's flow control and placement; `--set` / `--file` update it.
    Flow(FlowArgs),
    /// Pause a queue (nothing new is dispatched from it).
    Pause(QueueArgs),
    /// Resume a paused queue.
    Resume(QueueArgs),
    /// Show a queue's worker demand (what an autoscaler reads).
    Demand(QueueArgs),
}

impl QueueCommand {
    /// The connection flags of the verb.
    pub fn conn(&self) -> &Conn {
        match self {
            QueueCommand::Flow(a) => &a.conn,
            QueueCommand::Pause(a) | QueueCommand::Resume(a) | QueueCommand::Demand(a) => &a.conn,
        }
    }
}

/// A verb on one queue.
#[derive(Debug, Clone, Args)]
pub struct QueueArgs {
    /// Queue name.
    pub queue: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// `flow`.
#[derive(Debug, Clone, Args)]
pub struct FlowArgs {
    /// Queue name.
    pub queue: String,
    /// Set one field (repeatable): `key=value`, dotted keys for tables
    /// (`spill.cpu_pct=70`), `key=null` to clear.
    #[arg(long = "set", value_name = "KEY=VALUE", value_parser = parse_set)]
    pub set: Vec<(String, Value)>,
    /// Replace the flow fields with this file (TOML subset as printed by `flow`, or a
    /// JSON object); `--set` applies on top.
    #[arg(long, value_name = "FILE")]
    pub file: Option<std::path::PathBuf>,
    #[command(flatten)]
    pub conn: Conn,
}

/// `key=value` -> (key, JSON value or string).
pub fn parse_set(s: &str) -> Result<(String, Value), String> {
    let (k, v) = s
        .split_once('=')
        .ok_or_else(|| format!("--set {s:?}: expected key=value"))?;
    let k = k.trim();
    if k.is_empty() || k.split('.').any(str::is_empty) {
        return Err(format!("--set {s:?}: empty key"));
    }
    let v = v.trim();
    let value = serde_json::from_str::<Value>(v).unwrap_or_else(|_| Value::String(v.to_owned()));
    Ok((k.to_owned(), value))
}

/// Apply one `--set` to the flow object.
pub fn apply_set(flow: &mut Map<String, Value>, key: &str, value: Value) -> Result<(), String> {
    let value = match (key, value) {
        ("selector", Value::String(s)) => Value::Array(vec![Value::String(s)]),
        (_, v) => v,
    };
    let mut parts: Vec<&str> = key.split('.').collect();
    let last = parts.pop().unwrap_or(key);
    let mut obj = flow;
    let mut path = String::new();
    for p in parts {
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(p);
        let slot = obj
            .entry(p.to_owned())
            .or_insert_with(|| Value::Object(Map::new()));
        if slot.is_null() {
            *slot = Value::Object(Map::new());
        }
        obj = slot
            .as_object_mut()
            .ok_or_else(|| format!("--set {key}: {path} is not a table"))?;
    }
    if value.is_null() {
        obj.remove(last);
    } else {
        obj.insert(last.to_owned(), value);
    }
    Ok(())
}

/// The flow fields of a `GET .../flow` body (without `queue`, `version` and nulls).
pub fn flow_fields(body: &Value) -> Map<String, Value> {
    body.as_object()
        .map(|m| {
            m.iter()
                .filter(|(k, v)| k.as_str() != "queue" && k.as_str() != "version" && !v.is_null())
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        })
        .unwrap_or_default()
}

/// Render a flow body in the TOML subset described in the module docs.
pub fn render_flow(body: &Value) -> String {
    let mut out = format!(
        "# queue {} (version {})\n",
        s(&body["queue"]),
        s(&body["version"])
    );
    let fields = flow_fields(body);
    let mut tables = Vec::new();
    for (k, v) in &fields {
        match v {
            Value::Object(m) => tables.push((k, m)),
            v => out.push_str(&format!("{k} = {v}\n")),
        }
    }
    for (k, m) in tables {
        out.push_str(&format!("\n[{k}]\n"));
        for (ik, iv) in m.iter().filter(|(_, v)| !v.is_null()) {
            out.push_str(&format!("{ik} = {iv}\n"));
        }
    }
    out
}

/// Parse a `--file`: a JSON object, or the TOML subset `render_flow` writes.
pub fn parse_flow_file(text: &str) -> Result<Map<String, Value>, String> {
    if text.trim_start().starts_with('{') {
        return match serde_json::from_str::<Value>(text) {
            Ok(Value::Object(m)) => Ok(m),
            Ok(_) => Err("expected a JSON object".into()),
            Err(e) => Err(format!("invalid JSON: {e}")),
        };
    }
    let mut root = Map::new();
    let mut table: Option<String> = None;
    for (i, raw) in text.lines().enumerate() {
        let n = i + 1;
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            let name = name.trim();
            if name.is_empty() || name.contains('[') {
                return Err(format!("line {n}: invalid table header {line:?}"));
            }
            root.entry(name.to_owned())
                .or_insert_with(|| Value::Object(Map::new()));
            table = Some(name.to_owned());
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| format!("line {n}: expected key = value"))?;
        let k = k.trim().trim_matches('"');
        if k.is_empty() {
            return Err(format!("line {n}: empty key"));
        }
        let value: Value = serde_json::from_str(v.trim())
            .map_err(|e| format!("line {n}: value of {k} is not a supported literal ({e})"))?;
        let target = match &table {
            None => &mut root,
            Some(t) => root
                .get_mut(t)
                .and_then(Value::as_object_mut)
                .ok_or_else(|| format!("line {n}: {t} is not a table"))?,
        };
        target.insert(k.to_owned(), value);
    }
    Ok(root)
}

fn print_pause(out: Out, body: &Value, what: &str, verb: &str) {
    if out.json {
        out.json(body);
        return;
    }
    let name = s(&body["name"]);
    if body["changed"].as_bool().unwrap_or(false) {
        println!("{what} {name} {verb}");
    } else {
        println!("{what} {name} already {verb}");
    }
}

/// Execute one `queue` verb on `client`.
pub async fn run(c: &Client, out: Out, cmd: &QueueCommand) -> Result<Outcome, CallError> {
    match cmd {
        QueueCommand::Flow(a) => flow(c, out, a).await,
        QueueCommand::Pause(a) => {
            let r = c
                .call(
                    Method::POST,
                    &["v1", "queues", &a.queue, "pause"],
                    &[],
                    None,
                )
                .await?;
            print_pause(out, &r.body, "queue", "paused");
            Ok(Outcome::Ok)
        }
        QueueCommand::Resume(a) => {
            let r = c
                .call(
                    Method::POST,
                    &["v1", "queues", &a.queue, "resume"],
                    &[],
                    None,
                )
                .await?;
            print_pause(out, &r.body, "queue", "resumed");
            Ok(Outcome::Ok)
        }
        QueueCommand::Demand(a) => {
            let r = c
                .call(
                    Method::GET,
                    &["v1", "queues", &a.queue, "demand"],
                    &[],
                    None,
                )
                .await?;
            // Always JSON: D-W12 defines the demand answer as this document.
            out.json(&r.body);
            Ok(Outcome::Ok)
        }
    }
}

async fn flow(c: &Client, out: Out, a: &FlowArgs) -> Result<Outcome, CallError> {
    let path = ["v1", "queues", a.queue.as_str(), "flow"];
    let cur = c.call(Method::GET, &path, &[], None).await?;
    if a.set.is_empty() && a.file.is_none() {
        if out.json {
            out.json(&cur.body);
        } else {
            print!("{}", render_flow(&cur.body));
        }
        return Ok(Outcome::Ok);
    }
    let mut fields = match &a.file {
        Some(p) => {
            let text = std::fs::read_to_string(p)
                .map_err(|e| CallError::Usage(format!("cannot read {}: {e}", p.display())))?;
            parse_flow_file(&text)
                .map_err(|e| CallError::Usage(format!("--file {}: {e}", p.display())))?
        }
        None => flow_fields(&cur.body),
    };
    for (k, v) in &a.set {
        apply_set(&mut fields, k, v.clone()).map_err(CallError::Usage)?;
    }
    let version = cur.body["version"].as_u64().unwrap_or(0);
    fields.insert("expected_version".into(), Value::from(version));
    let r = c
        .call(Method::PUT, &path, &[], Some(&Value::Object(fields)))
        .await?;
    if out.json {
        out.json(&r.body);
    } else {
        print!("{}", render_flow(&r.body));
    }
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn set_values_and_dotted_keys() {
        let mut f = flow_fields(&json!({
            "queue": "q", "version": 3, "placement": "embedded", "selector": [],
            "concurrency": 4, "paused": false, "throttle": null
        }));
        for s in [
            "placement=workers",
            "selector=zone=a",
            "spill.cpu_pct=70",
            "throttle.limit=5",
            "throttle.period_ms=1000",
            "concurrency=null",
            "weight=2",
        ] {
            let (k, v) = parse_set(s).unwrap();
            apply_set(&mut f, &k, v).unwrap();
        }
        assert_eq!(
            Value::Object(f),
            json!({
                "placement": "workers", "selector": ["zone=a"], "paused": false,
                "spill": {"cpu_pct": 70}, "throttle": {"limit": 5, "period_ms": 1000},
                "weight": 2
            })
        );
        assert!(parse_set("novalue").is_err());
        assert!(parse_set("=1").is_err());
        assert!(parse_set("a..b=1").is_err());
        let mut f = Map::new();
        apply_set(&mut f, "weight", json!(1)).unwrap();
        assert!(apply_set(&mut f, "weight.x", json!(1)).is_err());
    }

    #[test]
    fn render_round_trips_through_the_file_parser() {
        let body = json!({
            "queue": "q", "version": 7, "placement": "auto", "selector": ["zone in (a,b)"],
            "weight": 3, "paused": true, "concurrency": null,
            "spill": {"cpu_pct": 70, "lag_ms": 5000},
            "rate_limit": {"limit": 10, "period_ms": 1000, "burst": 0}
        });
        let text = render_flow(&body);
        assert!(text.starts_with("# queue q (version 7)\n"), "{text}");
        assert!(text.contains("\n[spill]\ncpu_pct = 70\n"), "{text}");
        assert_eq!(parse_flow_file(&text).unwrap(), flow_fields(&body));
        // JSON files too.
        assert_eq!(
            parse_flow_file(r#"{"weight": 2}"#).unwrap(),
            flow_fields(&json!({"weight": 2}))
        );
        assert!(parse_flow_file("weight 2").unwrap_err().contains("line 1"));
        assert!(parse_flow_file("[]\n").is_err());
        assert!(parse_flow_file("placement = workers").is_err());
    }
}
