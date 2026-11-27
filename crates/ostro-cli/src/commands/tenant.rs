//! `ostro tenant …` (Phase 14, 14-17; D-P14-8): tenant lifecycle against a running server
//! (system admin bearer in `OSTRO_TOKEN`).
//!
//! | Verb | Request |
//! |---|---|
//! | `create <id> [quota flags]` | `POST /v1/tenants` |
//! | `ls` | `GET /v1/tenants` |
//! | `get <id>` | `GET /v1/tenants/{id}` |
//! | `set <id> [quota flags] [--clear <field>]…` | `PATCH /v1/tenants/{id}` (`null` clears) |
//! | `suspend <id>` / `resume <id>` | `POST /v1/tenants/{id}/{suspend,resume}` |
//! | `rm <id>` | `DELETE /v1/tenants/{id}` (409 `tenant_not_empty` unless suspended and empty) |
//!
//! Quota flags: `--weight N`, `--max-concurrency N`, `--fuel-per-s N`, `--fuel-burst N`,
//! `--memory 512MiB`, `--event-rate 100/1s[+burst]`; every value must be > 0 (checked
//! locally, usage error exit 2). `--json` prints the REST body (Phase 10 envelope).

use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Map, Value, json};

use super::Conn;
use super::worker_tokens::ms_time;
use crate::client::{CallError, Client};
use crate::output::{Out, Outcome, s, table};

/// `ostro tenant` subcommands.
#[derive(Debug, Clone, Subcommand)]
pub enum TenantCmd {
    /// Create a tenant with optional quotas (unset = unlimited).
    Create(TenantCreateArgs),
    /// List tenants.
    Ls(TenantLsArgs),
    /// Show one tenant.
    Get(TenantIdArgs),
    /// Change quotas / weight (`--clear <field>` removes a quota).
    Set(TenantSetArgs),
    /// Suspend a tenant (ingress refused, dispatch held; idempotent).
    Suspend(TenantIdArgs),
    /// Resume a suspended tenant (idempotent).
    Resume(TenantIdArgs),
    /// Delete a suspended, empty tenant.
    #[command(alias = "delete")]
    Rm(TenantIdArgs),
}

impl TenantCmd {
    /// The connection flags.
    pub fn conn(&self) -> &Conn {
        match self {
            TenantCmd::Create(a) => &a.conn,
            TenantCmd::Ls(a) => &a.conn,
            TenantCmd::Set(a) => &a.conn,
            TenantCmd::Get(a) | TenantCmd::Suspend(a) | TenantCmd::Resume(a) | TenantCmd::Rm(a) => {
                &a.conn
            }
        }
    }
}

/// Quota flags shared by `create` and `set`.
#[derive(Debug, Clone, Default, Args)]
pub struct QuotaFlags {
    /// Fair-share weight (the scheduler clamps it to 1..=10000).
    #[arg(long, value_parser = positive_u32)]
    pub weight: Option<u32>,
    /// Leased activations at once.
    #[arg(long, value_parser = positive_u32)]
    pub max_concurrency: Option<u32>,
    /// Fuel refill per second.
    #[arg(long, value_parser = positive_u64)]
    pub fuel_per_s: Option<u64>,
    /// Fuel bucket size.
    #[arg(long, value_parser = positive_u64)]
    pub fuel_burst: Option<u64>,
    /// Instance memory reserved at grant (bytes or 512MiB, 2GiB, 100MB, ...).
    #[arg(long, value_parser = parse_bytes)]
    pub memory: Option<u64>,
    /// Starts / signals / token completions / webhook deliveries: `<limit>/<period>` with an
    /// optional `+<burst>` (e.g. 100/1s, 10/1m+5).
    #[arg(long, value_parser = parse_rate)]
    pub event_rate: Option<Rate>,
}

/// `ostro tenant create`.
#[derive(Debug, Clone, Args)]
pub struct TenantCreateArgs {
    /// Tenant id (^[a-z0-9][a-z0-9-]{0,62}$; `_` is the default tenant and always exists).
    #[arg(value_parser = parse_tenant_id)]
    pub id: String,
    #[command(flatten)]
    pub quotas: QuotaFlags,
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro tenant ls`.
#[derive(Debug, Clone, Args)]
pub struct TenantLsArgs {
    #[command(flatten)]
    pub conn: Conn,
}

/// `ostro tenant get|suspend|resume|rm <id>`.
#[derive(Debug, Clone, Args)]
pub struct TenantIdArgs {
    /// Tenant id.
    #[arg(value_parser = parse_tenant_id)]
    pub id: String,
    #[command(flatten)]
    pub conn: Conn,
}

/// A quota field `set --clear` can remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum QuotaField {
    Weight,
    MaxConcurrency,
    FuelPerS,
    FuelBurst,
    Memory,
    EventRate,
}

impl QuotaField {
    /// The REST field name.
    pub fn key(self) -> &'static str {
        match self {
            QuotaField::Weight => "weight",
            QuotaField::MaxConcurrency => "max_concurrency",
            QuotaField::FuelPerS => "fuel_per_s",
            QuotaField::FuelBurst => "fuel_burst",
            QuotaField::Memory => "memory_bytes",
            QuotaField::EventRate => "event_rate",
        }
    }
}

/// `ostro tenant set`.
#[derive(Debug, Clone, Args)]
pub struct TenantSetArgs {
    /// Tenant id.
    #[arg(value_parser = parse_tenant_id)]
    pub id: String,
    #[command(flatten)]
    pub quotas: QuotaFlags,
    /// Remove a quota (unlimited) or reset the weight (repeatable).
    #[arg(long = "clear", value_enum)]
    pub clear: Vec<QuotaField>,
    #[command(flatten)]
    pub conn: Conn,
}

/// An event rate: `limit` per `period_ms`, plus `burst`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rate {
    pub limit: u32,
    pub period_ms: u64,
    pub burst: u32,
}

impl Rate {
    fn to_json(self) -> Value {
        json!({ "limit": self.limit, "period_ms": self.period_ms, "burst": self.burst })
    }
}

// ---------------------------------------------------------------------------------
// Parsers (errors are clap usage errors, exit 2)
// ---------------------------------------------------------------------------------

/// A tenant id (`_` refused with a hint).
pub fn parse_tenant_id(s: &str) -> Result<String, String> {
    if s == "_" {
        return Err("`_` is the default tenant: it always exists and cannot be managed".into());
    }
    let b = s.as_bytes();
    let ok = !b.is_empty()
        && b.len() <= 63
        && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
        && b[1..]
            .iter()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-');
    if ok {
        Ok(s.to_owned())
    } else {
        Err(format!(
            "{s:?} is not a tenant id (^[a-z0-9][a-z0-9-]{{0,62}}$)"
        ))
    }
}

fn positive_u32(s: &str) -> Result<u32, String> {
    match s.trim().parse::<u32>() {
        Ok(0) => Err("must be > 0 (omit it for no limit)".into()),
        Ok(v) => Ok(v),
        Err(e) => Err(format!("{s:?} is not a number: {e}")),
    }
}

fn positive_u64(s: &str) -> Result<u64, String> {
    match s.trim().parse::<u64>() {
        Ok(0) => Err("must be > 0 (omit it for no limit)".into()),
        Ok(v) => Ok(v),
        Err(e) => Err(format!("{s:?} is not a number: {e}")),
    }
}

/// Bytes with an optional unit: `B`, `KB`/`MB`/`GB`/`TB` (10^3), `KiB`/`MiB`/`GiB`/`TiB`
/// (2^10); must be > 0.
pub fn parse_bytes(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let split = t.find(|c: char| !c.is_ascii_digit()).unwrap_or(t.len());
    let (num, unit) = t.split_at(split);
    let n: u64 = num
        .parse()
        .map_err(|_| format!("invalid size {s:?} (expected e.g. 512MiB, 2GiB, 1048576)"))?;
    let mul: u64 = match unit.trim() {
        "" | "B" => 1,
        "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "KiB" => 1 << 10,
        "MiB" => 1 << 20,
        "GiB" => 1 << 30,
        "TiB" => 1 << 40,
        u => {
            return Err(format!(
                "invalid size unit {u:?} in {s:?} (use B, KB, MB, GB, TB, KiB, MiB, GiB, TiB)"
            ));
        }
    };
    let v = n
        .checked_mul(mul)
        .ok_or_else(|| format!("size {s:?} is too large"))?;
    if v == 0 {
        return Err("must be > 0 (omit it for no limit)".into());
    }
    Ok(v)
}

/// `<limit>/<period>[+<burst>]`, period a friendly duration (`1s`, `500ms`, `1m`).
pub fn parse_rate(s: &str) -> Result<Rate, String> {
    let t = s.trim();
    let (main, burst) = match t.split_once('+') {
        Some((m, b)) => (
            m,
            b.trim()
                .parse::<u32>()
                .map_err(|_| format!("invalid burst in {s:?}"))?,
        ),
        None => (t, 0),
    };
    let (limit, period) = main
        .split_once('/')
        .ok_or_else(|| format!("invalid rate {s:?} (expected <limit>/<period>, e.g. 100/1s)"))?;
    let limit: u32 = limit
        .trim()
        .parse()
        .map_err(|_| format!("invalid rate limit in {s:?}"))?;
    if limit == 0 {
        return Err("the rate limit must be > 0".into());
    }
    let period = super::parse_duration(period)?;
    let period_ms = u64::try_from(period.as_millis()).unwrap_or(u64::MAX);
    if period_ms == 0 {
        return Err("the rate period must be > 0 ms".into());
    }
    Ok(Rate {
        limit,
        period_ms,
        burst,
    })
}

// ---------------------------------------------------------------------------------
// Bodies
// ---------------------------------------------------------------------------------

fn quota_fields(q: &QuotaFlags, m: &mut Map<String, Value>) {
    if let Some(v) = q.weight {
        m.insert("weight".into(), json!(v));
    }
    if let Some(v) = q.max_concurrency {
        m.insert("max_concurrency".into(), json!(v));
    }
    if let Some(v) = q.fuel_per_s {
        m.insert("fuel_per_s".into(), json!(v));
    }
    if let Some(v) = q.fuel_burst {
        m.insert("fuel_burst".into(), json!(v));
    }
    if let Some(v) = q.memory {
        m.insert("memory_bytes".into(), json!(v));
    }
    if let Some(r) = q.event_rate {
        m.insert("event_rate".into(), r.to_json());
    }
}

/// The `create` body (`TenantSpec`).
pub fn create_body(a: &TenantCreateArgs) -> Value {
    let mut m = Map::new();
    m.insert("id".into(), json!(a.id));
    quota_fields(&a.quotas, &mut m);
    Value::Object(m)
}

/// The `set` body (`TenantPatch`: present keys change, `null` clears).
pub fn patch_body(a: &TenantSetArgs) -> Result<Value, CallError> {
    let mut m = Map::new();
    quota_fields(&a.quotas, &mut m);
    for f in &a.clear {
        if m.get(f.key()).is_some_and(|v| !v.is_null()) {
            return Err(CallError::Usage(format!(
                "--clear {} conflicts with a new value for the same field",
                f.key()
            )));
        }
        m.insert(f.key().into(), Value::Null);
    }
    if m.is_empty() {
        return Err(CallError::Usage(
            "nothing to change: give a quota flag or --clear <field>".into(),
        ));
    }
    Ok(Value::Object(m))
}

// ---------------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------------

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

fn rate_text(v: &Value) -> String {
    if v.is_null() {
        return "-".into();
    }
    let mut out = format!("{}/{}ms", s(&v["limit"]), s(&v["period_ms"]));
    if v["burst"].as_u64().is_some_and(|b| b > 0) {
        out.push_str(&format!("+{}", s(&v["burst"])));
    }
    out
}

fn tenant_lines(t: &Value) -> String {
    let rows = [
        ("id", s(&t["id"])),
        ("suspended", s(&t["suspended"])),
        ("weight", s(&t["weight"])),
        ("max concurrency", s(&t["max_concurrency"])),
        ("fuel per s", s(&t["fuel_per_s"])),
        ("fuel burst", s(&t["fuel_burst"])),
        ("memory bytes", s(&t["memory_bytes"])),
        ("event rate", rate_text(&t["event_rate"])),
        ("created", ms_time(&t["created_at_ms"])),
    ];
    rows.iter()
        .map(|(k, v)| format!("{:<16} {v}", format!("{k}:")))
        .collect::<Vec<_>>()
        .join("\n")
}

fn tenant_rows(body: &Value) -> Vec<Vec<String>> {
    body["tenants"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default()
        .iter()
        .map(|t| {
            vec![
                s(&t["id"]),
                if t["suspended"] == json!(true) {
                    "yes".into()
                } else {
                    "no".into()
                },
                s(&t["weight"]),
                s(&t["max_concurrency"]),
                s(&t["fuel_per_s"]),
                s(&t["memory_bytes"]),
                rate_text(&t["event_rate"]),
            ]
        })
        .collect()
}

/// Run one verb; the stdout text.
pub async fn render(c: &Client, json: bool, cmd: &TenantCmd) -> Result<String, CallError> {
    let (body, verb) = match cmd {
        TenantCmd::Create(a) => (c.create_tenant(&create_body(a)).await?.body, "created"),
        TenantCmd::Ls(_) => {
            let r = c.list_tenants().await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(table(
                &[
                    "ID",
                    "SUSPENDED",
                    "WEIGHT",
                    "MAX CONC",
                    "FUEL/S",
                    "MEMORY",
                    "EVENT RATE",
                ],
                &tenant_rows(&r.body),
            ));
        }
        TenantCmd::Get(a) => (c.get_tenant(&a.id).await?.body, ""),
        TenantCmd::Set(a) => (
            c.update_tenant(&a.id, &patch_body(a)?).await?.body,
            "updated",
        ),
        TenantCmd::Suspend(a) => (c.suspend_tenant(&a.id).await?.body, "suspended"),
        TenantCmd::Resume(a) => (c.resume_tenant(&a.id).await?.body, "resumed"),
        TenantCmd::Rm(a) => {
            let r = c.delete_tenant(&a.id).await?;
            if json {
                return Ok(pretty(&r.body));
            }
            return Ok(format!("deleted tenant {}", a.id));
        }
    };
    if json {
        return Ok(pretty(&body));
    }
    let lines = tenant_lines(&body);
    Ok(if verb.is_empty() {
        lines
    } else {
        format!("{verb} tenant {}\n{lines}", s(&body["id"]))
    })
}

/// Run a tenant verb, printing its output.
pub async fn run(c: &Client, out: Out, cmd: &TenantCmd) -> Result<Outcome, CallError> {
    let text = render(c, out.json, cmd).await?;
    println!("{text}");
    Ok(Outcome::Ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes() {
        assert_eq!(parse_bytes("512MiB"), Ok(512 << 20));
        assert_eq!(parse_bytes("2GiB"), Ok(2 << 30));
        assert_eq!(parse_bytes("100MB"), Ok(100_000_000));
        assert_eq!(parse_bytes("4096"), Ok(4096));
        for bad in [
            "0",
            "0MiB",
            "12XB",
            "MiB",
            "-1",
            "1.5GiB",
            "99999999999999TiB",
        ] {
            assert!(parse_bytes(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn rates() {
        assert_eq!(
            parse_rate("100/1s"),
            Ok(Rate {
                limit: 100,
                period_ms: 1000,
                burst: 0
            })
        );
        assert_eq!(
            parse_rate("10/1m+5"),
            Ok(Rate {
                limit: 10,
                period_ms: 60_000,
                burst: 5
            })
        );
        for bad in ["100", "0/1s", "10/0s", "x/1s", "10/1d", "10/1s+x"] {
            assert!(parse_rate(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn ids() {
        assert!(parse_tenant_id("acme").is_ok());
        assert!(parse_tenant_id("_").unwrap_err().contains("default tenant"));
        assert!(parse_tenant_id("Acme").is_err());
    }
}
