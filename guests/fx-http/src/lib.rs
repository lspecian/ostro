//! Phase 6 test guest (plan 06-09): scripted outbound HTTP through the effects bridge.
//!
//! Input:
//! `{"requests":[{"method","url","headers":[[k,v]],"body_b64"?,"idempotency_key"?,
//!   "semantic_key"?,"timeout_ms"?,"retry"?,"redact":[..]}],"parallel":bool}`
//!
//! Output: `{"results":[{"status","headers":[[k,v]],"body_b64"} |
//!   {"error":{"kind","message","retryable"}}]}` in request order.
//!
//! - `semantic_key`: the override key is `ctx.idempotency_key(semantic_key)` (P-11) and it
//!   is also written into the JSON body as `"idempotency_key"` (merged into a JSON-object
//!   body, otherwise the body becomes `{"idempotency_key": key}`); it wins over
//!   `idempotency_key`.
//! - `retry`: `{"max_attempts","initial_interval_ms","max_interval_ms",
//!   "backoff_coefficient","non_retryable_kinds"}`.
//! - `parallel=true` issues every request before waiting on any (all in flight at once),
//!   then waits on them in order; otherwise each request is issued and awaited in turn.

use std::time::Duration;

use ostro_sdk::prelude::*;
use ostro_sdk::{HttpBuilder, HttpResponse, RetryPolicy};

/// Retry policy in the input script.
#[derive(Debug, Deserialize)]
pub struct RetryIn {
    pub max_attempts: u32,
    #[serde(default)]
    pub initial_interval_ms: u64,
    #[serde(default)]
    pub max_interval_ms: u64,
    #[serde(default = "one")]
    pub backoff_coefficient: f64,
    #[serde(default)]
    pub non_retryable_kinds: Vec<String>,
}

fn one() -> f64 {
    1.0
}

/// One scripted request.
#[derive(Debug, Deserialize)]
pub struct RequestIn {
    pub method: String,
    pub url: String,
    #[serde(default)]
    pub headers: Vec<(String, String)>,
    #[serde(default)]
    pub body_b64: Option<String>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub semantic_key: Option<String>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub retry: Option<RetryIn>,
    #[serde(default)]
    pub redact: Vec<String>,
}

/// `fx-http` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub requests: Vec<RequestIn>,
    #[serde(default)]
    pub parallel: bool,
}

fn build(ctx: &Ctx, r: RequestIn) -> Result<HttpBuilder, WorkflowError> {
    let mut b = ctx.http().request(r.method, r.url);
    for (k, v) in r.headers {
        b = b.header(k, v);
    }
    let body = r.body_b64.as_deref().map(b64_decode).transpose()?;
    if let Some(sk) = r.semantic_key {
        let key = ctx.idempotency_key(&sk);
        let mut obj = body
            .as_deref()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(b).ok())
            .and_then(|v| match v {
                serde_json::Value::Object(m) => Some(m),
                _ => None,
            })
            .unwrap_or_default();
        obj.insert(
            "idempotency_key".into(),
            serde_json::Value::String(key.clone()),
        );
        b = b.json(&obj)?.idempotency_key(key);
    } else {
        if let Some(body) = body {
            b = b.body(body);
        }
        if let Some(k) = r.idempotency_key {
            b = b.idempotency_key(k);
        }
    }
    if let Some(ms) = r.timeout_ms {
        b = b.timeout(Duration::from_millis(ms));
    }
    if let Some(p) = r.retry {
        b = b.retry(RetryPolicy {
            max_attempts: p.max_attempts,
            initial_interval: Duration::from_millis(p.initial_interval_ms),
            max_interval: Duration::from_millis(p.max_interval_ms),
            backoff_coefficient: p.backoff_coefficient,
            non_retryable_kinds: p.non_retryable_kinds,
        });
    }
    Ok(b.redact(r.redact))
}

fn result_json(r: Result<HttpResponse, StepError>) -> serde_json::Value {
    match r {
        Ok(resp) => {
            let headers: Vec<(String, String)> = resp
                .headers
                .into_iter()
                .map(|h| (h.name, h.value))
                .collect();
            json!({ "status": resp.status, "headers": headers, "body_b64": b64_encode(&resp.body) })
        }
        Err(e) => {
            json!({ "error": { "kind": e.kind, "message": e.message, "retryable": e.retryable } })
        }
    }
}

/// A cancelled wait ends the run (cancelled); every other outcome is reported.
fn not_cancelled<T>(r: Result<T, StepError>) -> Result<Result<T, StepError>, WorkflowError> {
    match r {
        Err(e) if e.is_cancelled() => Err(e.into()),
        r => Ok(r),
    }
}

#[workflow(name = "fx-http", version = 1)]
pub fn fx_http(ctx: &Ctx, input: Input) -> Result<serde_json::Value, WorkflowError> {
    let mut results = Vec::with_capacity(input.requests.len());
    if input.parallel {
        let mut ops = Vec::with_capacity(input.requests.len());
        for r in input.requests {
            ops.push(build(ctx, r)?.send());
        }
        for op in ops {
            results.push(result_json(not_cancelled(op.wait())?));
        }
    } else {
        for r in input.requests {
            results.push(result_json(not_cancelled(build(ctx, r)?.send().wait())?));
        }
    }
    Ok(json!({ "results": results }))
}

ostro_sdk::export!(FxHttpWorkflow);

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
