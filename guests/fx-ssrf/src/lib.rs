//! Phase 6 test guest (plan 06-09): one GET per URL through the effects bridge, for the
//! SSRF / egress attack tables.
//!
//! Input: `{"urls":[..]}`; requests run sequentially, in order.
//!
//! Output: `{"results":[{"url","status"} | {"url","error":{"kind","message","retryable"}}]}`.

use ostro_sdk::prelude::*;
use serde_json::Value;

/// `fx-ssrf` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub urls: Vec<String>,
}

#[workflow(name = "fx-ssrf", version = 1)]
pub fn fx_ssrf(ctx: &Ctx, input: Input) -> Result<Value, WorkflowError> {
    let mut results = Vec::with_capacity(input.urls.len());
    for url in input.urls {
        match ctx.http().get(url.as_str()).send().wait() {
            Ok(resp) => results.push(json!({ "url": url, "status": resp.status })),
            Err(e) if e.is_cancelled() => return Err(e.into()),
            Err(e) => results.push(json!({
                "url": url,
                "error": { "kind": e.kind, "message": e.message, "retryable": e.retryable },
            })),
        }
    }
    Ok(json!({ "results": results }))
}

ostro_sdk::export!(FxSsrfWorkflow);
