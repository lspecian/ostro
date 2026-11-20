//! Phase 6 test guest (plan 06-09): scripted SQL through the effects bridge.
//!
//! Input: `{"connection","statement","params":[..],"exactly_once":bool,"repeat":n}`
//! (`repeat` defaults to 1; each repetition is a separate effect, run sequentially).
//!
//! Output: `{"results":[{"columns","rows","rows_affected"} |
//!   {"error":{"kind","message","retryable"}}]}`.
//!
//! SQL values as JSON. Input params accept plain JSON (`null`, `true`, integer -> int8,
//! other number -> float8, string -> text) or a one-key tagged object. Output cells are
//! always tagged: `{"null":null}`, `{"boolean":b}`, `{"int8":n}`, `{"float8":f}`,
//! `{"numeric":"s"}`, `{"text":"s"}`, `{"bytea":"<base64>"}`, `{"json":"s"}`,
//! `{"timestamptz":{"seconds":n,"nanoseconds":n}}`.

use ostro_sdk::bindings::ostro::runtime::types::Datetime;
use ostro_sdk::prelude::*;
use ostro_sdk::{SqlResult, SqlValue};
use serde_json::Value;

/// `fx-sql` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub connection: String,
    pub statement: String,
    #[serde(default)]
    pub params: Vec<Value>,
    #[serde(default)]
    pub exactly_once: bool,
    #[serde(default = "one")]
    pub repeat: u32,
}

fn one() -> u32 {
    1
}

fn bad(msg: String) -> WorkflowError {
    WorkflowError::non_retryable("bad_input", msg)
}

fn to_sql(v: &Value) -> Result<SqlValue, WorkflowError> {
    Ok(match v {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Boolean(*b),
        Value::Number(n) => match n.as_i64() {
            Some(i) => SqlValue::Int8(i),
            None => SqlValue::Float8(n.as_f64().unwrap_or(f64::NAN)),
        },
        Value::String(s) => SqlValue::Text(s.clone()),
        Value::Object(m) if m.len() == 1 => {
            let (k, x) = m.iter().next().expect("one entry");
            let s = || {
                x.as_str()
                    .map(str::to_string)
                    .ok_or_else(|| bad(format!("{k} needs a string")))
            };
            match k.as_str() {
                "null" => SqlValue::Null,
                "boolean" => SqlValue::Boolean(x.as_bool().ok_or_else(|| bad("boolean".into()))?),
                "int8" => SqlValue::Int8(x.as_i64().ok_or_else(|| bad("int8".into()))?),
                "float8" => SqlValue::Float8(x.as_f64().ok_or_else(|| bad("float8".into()))?),
                "numeric" => SqlValue::Numeric(s()?),
                "text" => SqlValue::Text(s()?),
                "bytea" => SqlValue::Bytea(b64_decode(&s()?)?),
                "json" => SqlValue::Json(s()?),
                "timestamptz" => SqlValue::Timestamptz(Datetime {
                    seconds: x["seconds"]
                        .as_u64()
                        .ok_or_else(|| bad("timestamptz.seconds".into()))?,
                    nanoseconds: u32::try_from(x["nanoseconds"].as_u64().unwrap_or(0))
                        .map_err(|_| bad("timestamptz.nanoseconds".into()))?,
                }),
                other => return Err(bad(format!("unknown sql value tag {other}"))),
            }
        }
        other => return Err(bad(format!("unsupported sql param {other}"))),
    })
}

fn from_sql(v: SqlValue) -> Value {
    match v {
        SqlValue::Null => json!({ "null": null }),
        SqlValue::Boolean(b) => json!({ "boolean": b }),
        SqlValue::Int8(i) => json!({ "int8": i }),
        SqlValue::Float8(f) => json!({ "float8": f }),
        SqlValue::Numeric(s) => json!({ "numeric": s }),
        SqlValue::Text(s) => json!({ "text": s }),
        SqlValue::Bytea(b) => json!({ "bytea": b64_encode(&b) }),
        SqlValue::Json(s) => json!({ "json": s }),
        SqlValue::Timestamptz(d) => {
            json!({ "timestamptz": { "seconds": d.seconds, "nanoseconds": d.nanoseconds } })
        }
    }
}

fn result_json(r: Result<SqlResult, StepError>) -> Value {
    match r {
        Ok(res) => {
            let rows: Vec<Vec<Value>> = res
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(from_sql).collect())
                .collect();
            json!({ "columns": res.columns, "rows": rows, "rows_affected": res.rows_affected })
        }
        Err(e) => {
            json!({ "error": { "kind": e.kind, "message": e.message, "retryable": e.retryable } })
        }
    }
}

#[workflow(name = "fx-sql", version = 1)]
pub fn fx_sql(ctx: &Ctx, input: Input) -> Result<Value, WorkflowError> {
    let params = input
        .params
        .iter()
        .map(to_sql)
        .collect::<Result<Vec<_>, _>>()?;
    let mut sql = ctx.sql(&input.connection);
    if input.exactly_once {
        sql = sql.exactly_once();
    }
    let mut results = Vec::with_capacity(input.repeat as usize);
    for _ in 0..input.repeat {
        let r = sql
            .query(input.statement.as_str(), params.iter().cloned())
            .wait();
        if let Err(e) = &r
            && e.is_cancelled()
        {
            return Err(e.clone().into());
        }
        results.push(result_json(r));
    }
    Ok(json!({ "results": results }))
}

ostro_sdk::export!(FxSqlWorkflow);

// ---- local base64 (standard alphabet, padded) -------------------------------------------

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn b64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn b64_decode(s: &str) -> Result<Vec<u8>, WorkflowError> {
    let bad = || WorkflowError::non_retryable("bad_input", "invalid base64");
    let s = s.trim_end_matches('=');
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for c in s.bytes() {
        let v = B64.iter().position(|&x| x == c).ok_or_else(bad)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    Ok(out)
}
