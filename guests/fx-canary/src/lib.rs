//! Phase 6 test guest (plan 06-09): sends a `{{secret:NAME}}`-templated request to an echo
//! endpoint and returns everything it observed, so a test can assert that the canary value
//! never reaches anything the guest can see. The guest only ever holds the template and
//! the secret *name*; it carries no secret value.
//!
//! Input: `{"echo_url","secret":"<name>","header":"X-Api-Key","in_body":bool,"redact"?:bool}`.
//! The header value is `{{secret:<name>}}`. With `in_body`, the body is
//! `{"secret":"{{secret:<name>}}"}` (W1: bodies are never expanded, so the echo shows the
//! literal template). With `redact` (default false) the header name is also redacted.
//!
//! Output: `{"observed":{"status","headers":[[k,v]],"body_b64"} | {"error":{..}},
//!   "env":[[k,v]],"args":[..],"random":"<32 hex>"}`. `env`/`args` are what the
//! deterministic WASI shim exposes, read after the response; `random` is 16 bytes from
//! `ctx.random_bytes`.

use ostro_sdk::prelude::*;
use serde_json::Value;

/// `fx-canary` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub echo_url: String,
    pub secret: String,
    #[serde(default = "default_header")]
    pub header: String,
    #[serde(default)]
    pub in_body: bool,
    #[serde(default)]
    pub redact: bool,
}

fn default_header() -> String {
    "X-Api-Key".into()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[workflow(name = "fx-canary", version = 1)]
pub fn fx_canary(ctx: &Ctx, input: Input) -> Result<Value, WorkflowError> {
    let template = format!("{{{{secret:{}}}}}", input.secret);
    let mut b = ctx
        .http()
        .post(input.echo_url.as_str())
        .secret_header(input.header.as_str(), template.as_str());
    if input.in_body {
        b = b.json(&json!({ "secret": template }))?;
    }
    if input.redact {
        b = b.redact([input.header.as_str()]);
    }
    let observed = match b.send().wait() {
        Ok(resp) => {
            let headers: Vec<(String, String)> = resp
                .headers
                .into_iter()
                .map(|h| (h.name, h.value))
                .collect();
            json!({ "status": resp.status, "headers": headers, "body_b64": b64_encode(&resp.body) })
        }
        Err(e) if e.is_cancelled() => return Err(e.into()),
        Err(e) => {
            json!({ "error": { "kind": e.kind, "message": e.message, "retryable": e.retryable } })
        }
    };
    // Vec, not a map: keep the shim's order (deterministic), never HashMap iteration.
    let env: Vec<(String, String)> = std::env::vars().collect();
    let args: Vec<String> = std::env::args().collect();
    let random = hex(&ctx.random_bytes(16));
    Ok(json!({ "observed": observed, "env": env, "args": args, "random": random }))
}

ostro_sdk::export!(FxCanaryWorkflow);

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
