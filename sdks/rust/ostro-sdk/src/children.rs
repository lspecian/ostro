//! Child workflows (WF-06): [`ChildOptions`], [`ChildRun`] and the decoding behind
//! [`Ctx::start_child`](crate::Ctx::start_child).
//!
//! A child starts with `ctx.start_child::<Out, _>(workflow, &input, opts)`. The call journals
//! `ChildStarted` and returns an [`Op`](crate::Op) at once; nothing blocks until it is
//! waited on. The op resolves per the WIT `start-child` contract (PF-W7):
//!
//! | Child outcome | `op.wait()` |
//! |---------------|-------------|
//! | Completed | `Ok(output)`, JSON-decoded into `Out` (empty output = `null`) |
//! | Failed | `Err(<the child's error>)` (same kind, message, retryability) |
//! | Cancelled | `Err(StepError::cancelled())` |
//! | Timed out | `Err(StepError::timeout())` |
//! | Terminated | `Err` non-retryable, kind [`CHILD_TERMINATED`] |
//! | Detached ([`ChildOptions::detached`]) | `Ok` as soon as the start is accepted, with `{"run_id": "<id>"}`; decode it as [`ChildRun`] |
//!
//! [`Ctx::cancel`](crate::Ctx::cancel) on a child op completes it with `Err(cancelled)` and
//! requests a graceful cancel of the child and its descendants. Attached children get a
//! graceful cancel when the parent reaches any terminal status. A child that
//! continues-as-new is followed to the last run of the chain.

use std::time::Duration;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::bindings::ostro::runtime::effects::{ChildOptions as WitChildOptions, OpOutput};
use crate::ctx::duration_ms;
use crate::error::StepError;

/// Error kind of a child op whose child run was terminated (WIT-reserved, Phase 9).
pub const CHILD_TERMINATED: &str = "child_terminated";

/// Options for [`Ctx::start_child`](crate::Ctx::start_child). The default starts an attached
/// child on the workflow's default queue at the version the server resolves.
///
/// ```ignore
/// let opts = ChildOptions::new().queue("payments").version(2).run_timeout(Duration::from_secs(600));
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChildOptions {
    /// Task queue of the child run (`None` = the child workflow's default).
    pub queue: Option<String>,
    /// Pinned child workflow version (`None` = resolved by the server).
    pub version: Option<u32>,
    /// Detached children outlive the parent, are not cancelled with it, have no
    /// `parent_run_id`, and their op completes with a [`ChildRun`] once the start is
    /// accepted.
    pub detached: bool,
    /// Run timeout of the child.
    pub run_timeout: Option<Duration>,
}

impl ChildOptions {
    /// Default options: attached, default queue, server-resolved version, no run timeout.
    pub fn new() -> Self {
        Self::default()
    }

    /// Run the child on `queue`.
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Pin the child workflow version.
    pub fn version(mut self, version: u32) -> Self {
        self.version = Some(version);
        self
    }

    /// Start the child detached (see [`ChildOptions::detached`](Self#structfield.detached)).
    pub fn detached(mut self) -> Self {
        self.detached = true;
        self
    }

    /// Give the child a run timeout.
    pub fn run_timeout(mut self, d: Duration) -> Self {
        self.run_timeout = Some(d);
        self
    }

    pub(crate) fn to_wit(&self) -> WitChildOptions {
        WitChildOptions {
            queue: self.queue.clone(),
            version: self.version,
            detached: self.detached,
            run_timeout_ms: self.run_timeout.map(duration_ms),
        }
    }
}

/// What a detached child op resolves to: the id of the started child run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildRun {
    /// Run id of the child.
    pub run_id: String,
}

impl StepError {
    /// True if this is the error of a child op whose child run was terminated.
    pub fn is_child_terminated(&self) -> bool {
        !self.retryable && self.kind == CHILD_TERMINATED
    }
}

/// Child ops resolve to the JSON-decoded child output (empty output = `null`).
pub(crate) fn decode_child<T: DeserializeOwned>(out: OpOutput) -> Result<T, StepError> {
    match out {
        OpOutput::Child(p) => crate::step::decode_payload(&p),
        other => Err(StepError::non_retryable(
            "protocol",
            format!("expected child output, got {other:?}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bindings::ostro::runtime::types::{ErrorInfo, Failure};
    use crate::step::json_payload;

    #[test]
    fn options_builder_maps_to_wit() {
        let w = ChildOptions::new().to_wit();
        assert_eq!(
            (w.queue, w.version, w.detached, w.run_timeout_ms),
            (None, None, false, None)
        );
        let w = ChildOptions::new()
            .queue("q")
            .version(3)
            .detached()
            .run_timeout(Duration::from_secs(2))
            .to_wit();
        assert_eq!(
            (w.queue.as_deref(), w.version, w.detached, w.run_timeout_ms),
            (Some("q"), Some(3), true, Some(2000))
        );
    }

    #[test]
    fn child_output_decodes_json_and_empty_as_null() {
        let v: serde_json::Value =
            decode_child(OpOutput::Child(json_payload(br#"{"echo":1}"#.to_vec()))).unwrap();
        assert_eq!(v, serde_json::json!({"echo": 1}));
        let none: Option<u32> = decode_child(OpOutput::Child(json_payload(Vec::new()))).unwrap();
        assert_eq!(none, None);
        let run: ChildRun = decode_child(OpOutput::Child(json_payload(
            br#"{"run_id":"r1"}"#.to_vec(),
        )))
        .unwrap();
        assert_eq!(run.run_id, "r1");
        assert_eq!(
            decode_child::<u32>(OpOutput::Timer).unwrap_err().kind,
            "protocol"
        );
    }

    #[test]
    fn child_failures_map_to_step_errors() {
        let terminated = StepError::from(Failure::NonRetryable(ErrorInfo {
            kind: CHILD_TERMINATED.into(),
            message: "terminated".into(),
            details: None,
        }));
        assert!(terminated.is_child_terminated());
        assert!(StepError::from(Failure::Timeout).is_timeout());
        assert!(StepError::from(Failure::Cancelled).is_cancelled());
        assert!(!StepError::retryable(CHILD_TERMINATED, "x").is_child_terminated());
    }
}
