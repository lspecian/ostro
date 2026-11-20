//! Phase 6 test guest (plan 06-09): scripted object-store ops through the effects bridge.
//!
//! Input: `{"connection","ops":[{"op","key","body_b64"?}]}`; ops run in order, one effect
//! each, and the op string is passed through verbatim (so unknown ops reach the host and
//! fail `unsupported_op`).
//!
//! Output: `{"results":[{"bytes_b64"} | {"error":{"kind","message","retryable"}}]}` where
//! `bytes_b64` is the raw `op-output.object` (W7: `get` -> object bytes, `put`/`head` ->
//! UTF-8 JSON, `delete` -> empty, `presign-*` -> UTF-8 URL).

use ostro_sdk::prelude::*;
use serde_json::Value;

/// One scripted op.
#[derive(Debug, Deserialize)]
pub struct OpIn {
    pub op: String,
    pub key: String,
    #[serde(default)]
    pub body_b64: Option<String>,
}

/// `fx-object` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub connection: String,
    pub ops: Vec<OpIn>,
}

#[workflow(name = "fx-object", version = 1)]
pub fn fx_object(ctx: &Ctx, input: Input) -> Result<Value, WorkflowError> {
    let client = ctx.object(&input.connection);
    let mut results = Vec::with_capacity(input.ops.len());
    for o in input.ops {
        let body = o.body_b64.as_deref().map(b64_decode).transpose()?;
        match client.raw(o.op, o.key, body).wait() {
            Ok(bytes) => results.push(json!({ "bytes_b64": b64_encode(&bytes) })),
            Err(e) if e.is_cancelled() => return Err(e.into()),
            Err(e) => results.push(
                json!({ "error": { "kind": e.kind, "message": e.message, "retryable": e.retryable } }),
            ),
        }
    }
    Ok(json!({ "results": results }))
}

ostro_sdk::export!(FxObjectWorkflow);

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
