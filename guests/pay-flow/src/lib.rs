//! Phase 7 test guest (plan 07-07, D-W15e): one payment effect, for fencing chaos (QA-03)
//! and the remote-effects secret canary (D-W3).
//!
//! Workflow `pay-flow` v1. Input `{"url": "http://host:port"}`. One HTTP effect
//! `POST {url}/pay` with
//! - `Idempotency-Key: <ctx.idempotency_key("pay")>` (run-derived, stable across replays,
//!   attempts and workers; also the host-level idempotency key),
//! - `Authorization: Bearer {{secret:PAY_TOKEN}}` (a template expanded on the leader only;
//!   the header is redacted in the journal),
//! - JSON body `{"idempotency_key": key, "amount": 100}`.
//!
//! Output: `{"status": u16, "idempotency_key": key}`.

use ostro_sdk::prelude::*;

/// `pay-flow` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub url: String,
}

/// `pay-flow` output.
#[derive(Debug, Serialize)]
pub struct Output {
    pub status: u16,
    pub idempotency_key: String,
}

#[workflow(name = "pay-flow", version = 1)]
pub fn pay_flow(ctx: &Ctx, input: Input) -> Result<Output, WorkflowError> {
    let key = ctx.idempotency_key("pay");
    let url = format!("{}/pay", input.url.trim_end_matches('/'));
    let resp = ctx
        .http()
        .post(url)
        .header("Idempotency-Key", key.as_str())
        .secret_header("Authorization", "Bearer {{secret:PAY_TOKEN}}")
        .redact(["Authorization"])
        .json(&json!({ "idempotency_key": key, "amount": 100 }))?
        .idempotency_key(key.as_str())
        .send()
        .wait()?;
    Ok(Output {
        status: resp.status,
        idempotency_key: key,
    })
}

ostro_sdk::export!(PayFlowWorkflow);
