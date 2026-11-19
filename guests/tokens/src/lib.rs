//! Await-token guest (plan 05-10, API-07; used by the 05-13/05-15 token API tests):
//! workflow `tokens` v1.
//!
//! Input `{"timeout_ms": u64 | null}` (absent = no timeout). Creates one await token,
//! publishes it with `set_status({"token": "<token>"})` so a client can read it from the
//! run's status, then waits on it:
//! - completed: returns the completion payload (any JSON value) unchanged;
//! - timed out: returns the string `"timeout"`;
//! - cancelled while waiting: the run ends Cancelled (D11, `Err(cancelled)` propagates).

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `tokens` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

#[workflow(name = "tokens", version = 1)]
pub fn tokens(ctx: &Ctx, input: Input) -> Result<serde_json::Value, WorkflowError> {
    let t =
        ctx.create_await_token::<serde_json::Value>(input.timeout_ms.map(Duration::from_millis));
    ctx.set_status(&json!({ "token": t.token() }))?;
    match t.op().wait() {
        Ok(v) => Ok(v),
        Err(e) if e.is_timeout() => Ok(json!("timeout")),
        Err(e) => Err(e.into()),
    }
}

ostro_sdk::export!(TokensWorkflow);
