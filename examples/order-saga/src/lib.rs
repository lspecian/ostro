//! # order-saga
//!
//! An example Ostro workflow (`order-saga` v1) written with the Rust SDK. It shows how the
//! durable building blocks fit together:
//!
//! 1. `reserve_inventory`: a durable step.
//! 2. `charge_payment`: a durable step with a retry policy (3 attempts, exponential
//!    backoff 200 ms -> 400 ms, capped at 2 s; `card_declined` is never retried).
//! 3. A durable timer: `ctx.sleep(settle_ms)` (default 1 s) lets the payment settle.
//! 4. A race: `select!` over the `approve` signal (a JSON `bool`, with a timeout of
//!    `approval_timeout_ms`, default 24 h) and the run's cancellation op.
//! 5. On approval: `create_shipment`, `notify_customer`, `complete_order`, and the run
//!    completes with `{"status": "completed", ...}`.
//!
//! If a step fails, the order is rejected (`approve` = `false`), the approval times out,
//! or the run is cancelled, the completed forward steps are compensated **in reverse
//! order inside `ctx.shielded`**: `refund_payment` if the payment was charged, then
//! `release_inventory` if the stock was reserved. Shielding matters for cancellation: an
//! unshielded wait after a cancel returns `Err(cancelled)` at once, while shielded code
//! runs to completion. The run then completes with `{"status": "compensated", "reason":
//! ...}`, or ends **Cancelled** when cancellation was requested.
//!
//! The steps are pure simulations (no I/O): ids are derived from the order id, so every
//! replay produces the same values. `fail_at` forces a failure for demos and tests:
//! `"charge_payment"` fails retryably on every attempt (the retry policy runs out after 3
//! attempts), any other step name fails that step non-retryably.
//!
//! ## Input
//!
//! ```json
//! {"order_id": "o-1", "amount": 4999, "sku": "BOOK-42", "qty": 1,
//!  "fail_at": null, "settle_ms": 1000, "approval_timeout_ms": 86400000}
//! ```
//!
//! `amount` is in cents. `fail_at`, `settle_ms` and `approval_timeout_ms` are optional.

use std::time::Duration;

use ostro_sdk::prelude::*;

/// Workflow input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// Order id (also the idempotency root for the simulated side effects).
    pub order_id: String,
    /// Amount in cents.
    pub amount: u64,
    /// Product SKU.
    pub sku: String,
    /// Quantity.
    pub qty: u32,
    /// Force a failure at this step (demo / tests).
    #[serde(default)]
    pub fail_at: Option<String>,
    /// Settlement delay before asking for approval (default 1000 ms).
    #[serde(default)]
    pub settle_ms: Option<u64>,
    /// How long to wait for the `approve` signal (default 24 h).
    #[serde(default)]
    pub approval_timeout_ms: Option<u64>,
}

/// Workflow output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResult {
    /// `"completed"` or `"compensated"`.
    pub status: String,
    /// The order id.
    pub order_id: String,
    /// Why the order was compensated (`None` when completed).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Shipment id (completed orders only).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shipment_id: Option<String>,
    /// Compensation steps that ran, in order.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub compensations: Vec<String>,
}

const DEFAULT_SETTLE_MS: u64 = 1_000;
const DEFAULT_APPROVAL_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1_000;

/// Fail `step` if the input asks for it.
fn forced(order: &Order, step: &str) -> Result<(), StepError> {
    if order.fail_at.as_deref() == Some(step) {
        return Err(StepError::non_retryable(
            "forced_failure",
            format!("{step} forced to fail"),
        ));
    }
    Ok(())
}

#[step]
fn reserve_inventory(order: &Order) -> Result<String, StepError> {
    forced(order, "reserve_inventory")?;
    Ok(format!(
        "res-{}-{}x{}",
        order.order_id, order.sku, order.qty
    ))
}

#[step(retry(
    max_attempts = 3,
    initial = "200ms",
    max = "2s",
    backoff = 2.0,
    non_retryable = ["card_declined"]
))]
fn charge_payment(order: &Order) -> Result<String, StepError> {
    if order.fail_at.as_deref() == Some("charge_payment") {
        // Transient: retried by the policy until the attempts run out.
        return Err(StepError::retryable(
            "gateway_unavailable",
            "payment gateway unavailable (simulated)",
        ));
    }
    if order.amount == 0 {
        return Err(StepError::non_retryable("card_declined", "zero amount"));
    }
    Ok(format!("pay-{}-{}", order.order_id, order.amount))
}

#[step]
fn create_shipment(order: &Order) -> Result<String, StepError> {
    forced(order, "create_shipment")?;
    Ok(format!("ship-{}", order.order_id))
}

#[step]
fn notify_customer(order: &Order, shipment_id: &str) -> Result<String, StepError> {
    forced(order, "notify_customer")?;
    Ok(format!("notified {} about {shipment_id}", order.order_id))
}

#[step]
fn complete_order(order: &Order) -> Result<String, StepError> {
    forced(order, "complete_order")?;
    Ok(format!("order {} completed", order.order_id))
}

#[step]
fn refund_payment(payment_id: &str) -> Result<String, StepError> {
    Ok(format!("refunded {payment_id}"))
}

#[step]
fn release_inventory(reservation_id: &str) -> Result<String, StepError> {
    Ok(format!("released {reservation_id}"))
}

/// What the forward path has done so far (drives the compensations).
#[derive(Debug, Default)]
struct Progress {
    reservation: Option<String>,
    payment: Option<String>,
}

/// Why the forward path stopped.
enum Abort {
    /// Compensate and complete with this reason.
    Compensate(String),
    /// Compensate and end the run Cancelled.
    Cancelled,
}

impl From<StepError> for Abort {
    fn from(e: StepError) -> Self {
        if e.is_cancelled() {
            Abort::Cancelled
        } else {
            Abort::Compensate(format!("{}: {}", e.kind, e.message))
        }
    }
}

/// Reverse compensations, shielded from cancellation so they always run to completion.
fn compensate(ctx: &Ctx, progress: &Progress) -> Result<Vec<String>, WorkflowError> {
    ctx.shielded(|| {
        let mut done = Vec::new();
        if let Some(payment) = &progress.payment {
            refund_payment(payment)?;
            done.push("refund_payment".to_string());
        }
        if let Some(reservation) = &progress.reservation {
            release_inventory(reservation)?;
            done.push("release_inventory".to_string());
        }
        Ok(done)
    })
}

/// The forward path; returns the shipment id.
fn forward(ctx: &Ctx, order: &Order, progress: &mut Progress) -> Result<String, Abort> {
    progress.reservation = Some(reserve_inventory(order)?);
    progress.payment = Some(charge_payment(order)?);

    let settle = order.settle_ms.unwrap_or(DEFAULT_SETTLE_MS);
    ctx.sleep(Duration::from_millis(settle)).wait()?;

    let timeout = order
        .approval_timeout_ms
        .unwrap_or(DEFAULT_APPROVAL_TIMEOUT_MS);
    let approval = ctx.wait_signal::<bool>("approve", Some(Duration::from_millis(timeout)));
    let cancellation = ctx.cancellation();
    match ostro_sdk::select!(approval, cancellation) {
        (0, c) => match approval.decode(c) {
            Ok(true) => {}
            Ok(false) => return Err(Abort::Compensate("rejected".into())),
            Err(e) if e.is_timeout() => {
                return Err(Abort::Compensate("approval_timeout".into()));
            }
            Err(e) => return Err(e.into()),
        },
        // The run was cancelled first: close the approval wait so a late signal is not
        // consumed by this run.
        _ => {
            ctx.cancel(&approval);
            return Err(Abort::Cancelled);
        }
    }

    let shipment = create_shipment(order)?;
    notify_customer(order, &shipment)?;
    complete_order(order)?;
    Ok(shipment)
}

/// The order saga.
#[workflow(name = "order-saga", version = 1)]
pub fn order_saga(ctx: &Ctx, order: Order) -> Result<OrderResult, WorkflowError> {
    let mut progress = Progress::default();
    match forward(ctx, &order, &mut progress) {
        Ok(shipment) => Ok(OrderResult {
            status: "completed".into(),
            order_id: order.order_id,
            reason: None,
            shipment_id: Some(shipment),
            compensations: Vec::new(),
        }),
        Err(Abort::Compensate(reason)) => {
            ctx.log_warn(&format!("order {} compensating: {reason}", order.order_id));
            let compensations = compensate(ctx, &progress)?;
            Ok(OrderResult {
                status: "compensated".into(),
                order_id: order.order_id,
                reason: Some(reason),
                shipment_id: None,
                compensations,
            })
        }
        Err(Abort::Cancelled) => {
            compensate(ctx, &progress)?;
            Err(StepError::cancelled().into())
        }
    }
}

ostro_sdk::export!(OrderSagaWorkflow);
