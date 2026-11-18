//! Compiles and runs on the host: the macros expand to valid code, the generated
//! `Workflow` impls carry the declared metadata and `export!` accepts them. No WIT import is
//! called (on native targets wit-bindgen's import stubs are `unreachable!()`).

use ostro_sdk::prelude::*;
use ostro_sdk::registry::__describe;

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    pub id: String,
    pub amount: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub id: String,
    pub charged: u64,
}

#[step(
    retry(
        max_attempts = 5,
        initial = "200ms",
        max = "10s",
        backoff = 2.0,
        non_retryable = ["card_declined"]
    ),
    timeout = "5s",
    fuel = 1_000_000
)]
fn charge(order: &Order) -> Result<Receipt, StepError> {
    if order.amount == 0 {
        return Err(StepError::non_retryable("card_declined", "zero amount"));
    }
    Ok(Receipt {
        id: order.id.clone(),
        charged: order.amount,
    })
}

#[step(name = "notify-customer", retry(max_attempts = 2))]
pub fn notify(id: &str, attempt: u32) -> Result<(), StepError> {
    let _ = (id, attempt);
    Ok(())
}

#[step]
fn plain() -> Result<u64, StepError> {
    Ok(7)
}

/// Doc comments and attributes on the fn are preserved.
#[workflow(name = "order", version = 2, queue = "payments", run_timeout = "10m")]
pub fn order(ctx: &Ctx, input: Order) -> Result<Receipt, WorkflowError> {
    let r = charge(&input)?;
    notify(&r.id, 1)?;
    let _ = plain()?;
    let _ = ctx;
    Ok(r)
}

#[workflow(name = "echo", version = 1)]
fn echo_json(_ctx: &Ctx, input: serde_json::Value) -> Result<serde_json::Value, StepError> {
    // Any error type that converts into WorkflowError is accepted.
    Ok(input)
}

ostro_sdk::export!(OrderWorkflow, EchoJsonWorkflow);

fn assert_workflow<W: Workflow<Input = Order, Output = Receipt>>() {}

fn main() {
    assert_workflow::<OrderWorkflow>();
    assert_eq!(<OrderWorkflow as Workflow>::NAME, "order");
    assert_eq!(<OrderWorkflow as Workflow>::VERSION, 2);
    assert_eq!(<OrderWorkflow as Workflow>::DEFAULT_QUEUE, Some("payments"));
    assert_eq!(<OrderWorkflow as Workflow>::RUN_TIMEOUT_MS, Some(600_000));

    let m = __describe::<EchoJsonWorkflow>();
    assert_eq!((m.name.as_str(), m.version), ("echo", 1));
    assert_eq!((m.default_queue, m.run_timeout_ms), (None, None));

    // The original functions stay callable by name (not invoked: they would call imports).
    let _o: fn(&Ctx, Order) -> Result<Receipt, WorkflowError> = order;
    let _c: fn(&Order) -> Result<Receipt, StepError> = charge;
    let _n: fn(&str, u32) -> Result<(), StepError> = notify;
}
