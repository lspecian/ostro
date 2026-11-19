//! Signal guest (plan 05-10, WF-05; used by the 05-15 server signal tests): workflow
//! `signals` v1. Payloads are arbitrary JSON values.
//!
//! Input `{"mode": ..., "timeout_ms": u64, "sleep_ms": u64}`:
//! - `fifo`: waits for signal `"x"` three times (no timeout) and returns the three
//!   payloads in order, as a JSON array (FIFO: one signal satisfies one wait).
//! - `timeout`: waits for `"x"` with a `timeout_ms` timeout (default 200 ms); returns the
//!   payload, or the string `"timeout"`.
//! - `before`: sleeps `sleep_ms` (default 500 ms) first, then waits for `"x"` with no
//!   timeout and returns the payload. A signal sent during the sleep is buffered and
//!   matched by the later wait.
//! - `long_wait`: waits for `"x"` with no timeout and returns the payload (05-15
//!   resume-after-restart / long-wait tests; the wait always evicts).
//!
//! A cancel while waiting ends the run Cancelled (D11).

use std::time::Duration;

use ostro_sdk::prelude::*;

/// `signals` input.
#[derive(Debug, Deserialize)]
pub struct Input {
    pub mode: String,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub sleep_ms: Option<u64>,
}

type Value = serde_json::Value;

#[workflow(name = "signals", version = 1)]
pub fn signals(ctx: &Ctx, input: Input) -> Result<Value, WorkflowError> {
    match input.mode.as_str() {
        "fifo" => {
            let mut got = Vec::with_capacity(3);
            for _ in 0..3 {
                got.push(ctx.wait_signal::<Value>("x", None).wait()?);
            }
            Ok(Value::Array(got))
        }
        "timeout" => {
            let t = Duration::from_millis(input.timeout_ms.unwrap_or(200));
            match ctx.wait_signal::<Value>("x", Some(t)).wait() {
                Ok(v) => Ok(v),
                Err(e) if e.is_timeout() => Ok(json!("timeout")),
                Err(e) => Err(e.into()),
            }
        }
        "before" => {
            ctx.sleep(Duration::from_millis(input.sleep_ms.unwrap_or(500)))
                .wait()?;
            Ok(ctx.wait_signal::<Value>("x", None).wait()?)
        }
        "long_wait" => Ok(ctx.wait_signal::<Value>("x", None).wait()?),
        other => Err(WorkflowError::non_retryable(
            "bad_mode",
            format!("unknown mode {other:?}"),
        )),
    }
}

ostro_sdk::export!(SignalsWorkflow);
