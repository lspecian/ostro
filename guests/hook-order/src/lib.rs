//! Webhook order guest (plan 08-20, SC1): workflow `hook-order` v1.
//!
//! Input: a Stripe checkout session object (the `data/object` of a
//! `checkout.session.completed` event; at least `{"id": "cs_..."}`).
//!
//! 1. Records the session id in the run status (`{"session": "<id>", "state": "awaiting_payment"}`).
//! 2. Waits for signal `paid` (timeout 1 h). The signal payload is the whole verified
//!    webhook body (for example a `payment_intent.succeeded` event).
//! 3. Completes with `{"session": "<id>", "paid_at": <payload.created or null>,
//!    "payment": <payload.data.object.id or null>}`, or `{"session", "timeout": true}`
//!    when no payment arrives in time.
//!
//! A cancel while waiting ends the run Cancelled (D11).

use std::time::Duration;

use ostro_sdk::prelude::*;

type Value = serde_json::Value;

/// The part of the Stripe checkout session the workflow reads.
#[derive(Debug, Deserialize)]
pub struct Session {
    pub id: String,
}

const PAYMENT_TIMEOUT: Duration = Duration::from_secs(3600);

#[workflow(name = "hook-order", version = 1)]
pub fn hook_order(ctx: &Ctx, session: Session) -> Result<Value, WorkflowError> {
    ctx.set_status(&json!({ "session": session.id, "state": "awaiting_payment" }))?;
    match ctx
        .wait_signal::<Value>("paid", Some(PAYMENT_TIMEOUT))
        .wait()
    {
        Ok(event) => Ok(json!({
            "session": session.id,
            "paid_at": event.get("created").cloned().unwrap_or(Value::Null),
            "payment": event.pointer("/data/object/id").cloned().unwrap_or(Value::Null),
        })),
        Err(e) if e.is_timeout() => Ok(json!({ "session": session.id, "timeout": true })),
        Err(e) => Err(e.into()),
    }
}

ostro_sdk::export!(HookOrderWorkflow);
