//! The saga helper (WF-10, D-P9-18): register a compensation after each completed step and
//! compensate in reverse order when a later step fails.
//!
//! ```ignore
//! let mut saga = Saga::new(ctx);
//! book_a()?;
//! saga.add("cancel_a", |_| { cancel_a()?; Ok(()) });
//! book_b()?;
//! saga.add("cancel_b", |_| { cancel_b()?; Ok(()) });
//! if let Err(e) = book_c() {
//!     return Err(saga.fail_with(e)); // runs cancel_b, then cancel_a
//! }
//! ```
//!
//! [`Saga::compensate`] runs the compensations **sequentially, last registered first
//! (LIFO)**, all inside [`Ctx::shielded`]. A workflow that was cancelled still compensates:
//! the waits inside a compensation do not return `Err(cancelled)`. A failing compensation
//! does not stop the others; every failure is reported. [`Saga::fail_with`] compensates and
//! returns the original error with the report in its `details`:
//!
//! ```json
//! {"compensated": ["cancel_b", "cancel_a"],
//!  "compensation_failures": [{"compensation": "cancel_x", "error": {"kind": "...", "message": "...", "retryable": false}}]}
//! ```
//!
//! The helper is SDK-only: compensations are ordinary steps and effects, so they are
//! journaled and replay like any other workflow code. Running compensations in parallel is
//! out of scope.

use serde::Serialize;

use crate::ctx::Ctx;
use crate::error::WorkflowError;

type Compensation<'a> = Box<dyn FnOnce(&Ctx) -> Result<(), WorkflowError> + 'a>;

/// Compensations registered while a workflow makes progress; see the [module docs](self).
#[must_use = "a saga does nothing unless compensate() or fail_with() is called"]
pub struct Saga<'a> {
    ctx: &'a Ctx,
    comps: Vec<(String, Compensation<'a>)>,
}

impl std::fmt::Debug for Saga<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Saga")
            .field(
                "compensations",
                &self
                    .comps
                    .iter()
                    .map(|(n, _)| n.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// One compensation that failed during [`Saga::compensate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompensationFailure {
    /// The compensation's name (as passed to [`Saga::add`]).
    pub compensation: String,
    /// Its error.
    pub error: WorkflowError,
}

/// What [`Saga::compensate`] did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SagaReport {
    /// Compensations that succeeded, in the order they ran (reverse registration order).
    pub compensated: Vec<String>,
    /// Compensations that failed, in the order they ran.
    pub failures: Vec<CompensationFailure>,
}

#[derive(Serialize)]
struct ErrorJson<'a> {
    kind: &'a str,
    message: &'a str,
    retryable: bool,
}

#[derive(Serialize)]
struct FailureJson<'a> {
    compensation: &'a str,
    error: ErrorJson<'a>,
}

impl SagaReport {
    /// True if every compensation succeeded.
    pub fn is_clean(&self) -> bool {
        self.failures.is_empty()
    }

    /// The report as JSON: `{"compensated": [..], "compensation_failures": [..]}`.
    pub fn to_json(&self) -> serde_json::Value {
        let failures: Vec<FailureJson<'_>> = self
            .failures
            .iter()
            .map(|f| FailureJson {
                compensation: &f.compensation,
                error: ErrorJson {
                    kind: &f.error.kind,
                    message: &f.error.message,
                    retryable: f.error.retryable,
                },
            })
            .collect();
        serde_json::json!({
            "compensated": self.compensated,
            "compensation_failures": failures,
        })
    }
}

impl<'a> Saga<'a> {
    /// An empty saga for this run.
    pub fn new(ctx: &'a Ctx) -> Self {
        Self {
            ctx,
            comps: Vec::new(),
        }
    }

    /// Register compensation `name`; it runs before every compensation registered earlier.
    pub fn add(
        &mut self,
        name: impl Into<String>,
        compensation: impl FnOnce(&Ctx) -> Result<(), WorkflowError> + 'a,
    ) {
        self.comps.push((name.into(), Box::new(compensation)));
    }

    /// Number of registered compensations.
    pub fn len(&self) -> usize {
        self.comps.len()
    }

    /// True if no compensation is registered.
    pub fn is_empty(&self) -> bool {
        self.comps.is_empty()
    }

    /// Run every compensation, last registered first, inside [`Ctx::shielded`]; a failure
    /// is recorded and the remaining compensations still run.
    pub fn compensate(self) -> SagaReport {
        let Self { ctx, comps } = self;
        ctx.shielded(|| {
            let mut report = SagaReport::default();
            for (name, f) in comps.into_iter().rev() {
                match f(ctx) {
                    Ok(()) => report.compensated.push(name),
                    Err(error) => report.failures.push(CompensationFailure {
                        compensation: name,
                        error,
                    }),
                }
            }
            report
        })
    }

    /// Compensate (see [`compensate`](Self::compensate)) and return `err` with the report
    /// merged into its `details` (`compensated`, `compensation_failures`; earlier details
    /// of `err` are kept under `"details"`). A continue-as-new request is returned
    /// unchanged after compensating.
    pub fn fail_with(self, err: impl Into<WorkflowError>) -> WorkflowError {
        let err = err.into();
        let report = self.compensate();
        if err.is_continue_as_new() {
            return err;
        }
        let mut details = report.to_json();
        if let (Some(prev), Some(obj)) = (err.details(), details.as_object_mut()) {
            obj.insert("details".to_string(), prev);
        }
        err.with_details(&details)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_json_shape() {
        let r = SagaReport {
            compensated: vec!["cancel_a".into()],
            failures: vec![CompensationFailure {
                compensation: "cancel_b".into(),
                error: WorkflowError::non_retryable("nope", "failed"),
            }],
        };
        assert!(!r.is_clean());
        assert_eq!(
            r.to_json(),
            serde_json::json!({
                "compensated": ["cancel_a"],
                "compensation_failures": [{
                    "compensation": "cancel_b",
                    "error": {"kind": "nope", "message": "failed", "retryable": false}
                }]
            })
        );
        assert!(SagaReport::default().is_clean());
    }

    #[test]
    fn compensates_lifo_shielded_past_failures() {
        use std::cell::RefCell;
        let ctx = Ctx::new();
        let order = RefCell::new(Vec::new());
        let mut saga = Saga::new(&ctx);
        for name in ["a", "b", "c"] {
            let order = &order;
            saga.add(format!("cancel_{name}"), move |_| {
                assert!(crate::ops::is_shielded(), "compensations run shielded");
                order.borrow_mut().push(name);
                if name == "b" {
                    Err(WorkflowError::non_retryable("cancel_b_failed", "b"))
                } else {
                    Ok(())
                }
            });
        }
        assert_eq!(saga.len(), 3);
        let err = saga
            .fail_with(WorkflowError::non_retryable("book_d_failed", "d").with_details(&"orig"));
        assert!(!crate::ops::is_shielded());
        assert_eq!(*order.borrow(), vec!["c", "b", "a"]);
        assert_eq!(err.kind, "book_d_failed");
        let d = err.details().expect("details");
        assert_eq!(
            d["compensated"],
            serde_json::json!(["cancel_c", "cancel_a"])
        );
        assert_eq!(d["compensation_failures"][0]["compensation"], "cancel_b");
        assert_eq!(
            d["compensation_failures"][0]["error"]["kind"],
            "cancel_b_failed"
        );
        assert_eq!(d["details"], "orig");

        let empty = Saga::new(&ctx);
        assert!(empty.is_empty());
        assert_eq!(empty.compensate(), SagaReport::default());
    }
}
