//! The connector contract (06-02, P-3): what the driver core (06-07) needs from a connector
//! (06-10 http, 06-11 sql, 06-12 object) and from the server seam (06-14).

use async_trait::async_trait;
use ostro_types::journal::{Failure, OpOutput, RecordedResult};
use ostro_types::live::{EffectKind, LiveEffect};

use crate::ratelimit::QuotaSpec;

/// Context of one effect execution.
#[derive(Clone, Debug)]
pub struct EffectCtx {
    /// Cluster id (part of derived idempotency keys and SQL tokens).
    pub cluster_id: String,
    /// Partition of the run.
    pub partition: u16,
    /// Run id (string form).
    pub run_id: String,
    /// Connector-level attempt number (1-based).
    pub attempt: u32,
    /// Tenant-qualified deployed module name `<tenant>/<name>`, taken from the run's
    /// `WorkflowVersion.module` (05-07, D23). Never derived from the sha-keyed `ModuleRecord`.
    pub module: String,
    /// Tenant of the run, if any.
    pub tenant: Option<String>,
    /// Cancelled when the effect is abandoned (run cancelled or terminal, leadership lost).
    pub cancel: tokio_util::sync::CancellationToken,
}

/// Result of the pre-send gate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GateDecision {
    /// Send the request.
    Proceed,
    /// The run is cancelled or terminal, or this node is not the leader: send nothing and
    /// submit no completion.
    Abandon,
}

/// Pre-send gate (Pitfall 7: no zombie sends after the lease is lost).
#[async_trait]
pub trait SendGate: Send + Sync {
    /// Whether effect `op` of `run_id` on `partition` may still be sent.
    async fn check(&self, partition: u16, run_id: &str, op: u64) -> GateDecision;
}

/// A completion could not be submitted.
#[derive(Debug, thiserror::Error)]
pub enum CompletionError {
    /// Transient (not leader yet, log busy): retry later.
    #[error("retry later: {0}")]
    Unavailable(String),
    /// The completion is no longer wanted (run gone, duplicate): drop it.
    #[error("dropped: {0}")]
    Dropped(String),
}

/// Where completions go: `Cmd::CompleteEffect` through the partition log (P-10, 06-14).
#[async_trait]
pub trait CompletionSink: Send + Sync {
    /// Submits the outcome of effect `op` of `run_id`. Attempt-independent; the first
    /// completion wins.
    async fn complete(
        &self,
        partition: u16,
        run_id: &str,
        op: u64,
        outcome: RecordedResult,
    ) -> Result<(), CompletionError>;
}

/// The rate-limit target of an effect.
#[derive(Clone, Debug, PartialEq)]
pub struct Target {
    /// The origin (`scheme://host[:port]`) or the connection name.
    pub key: String,
    /// The quota of the target; `None` = unlimited.
    pub quota: Option<QuotaSpec>,
}

/// One connector family (http, sql, object).
#[async_trait]
pub trait Connector: Send + Sync {
    /// The family this connector serves.
    fn kind(&self) -> EffectKind;

    /// Rate-limit target; resolved before the gate (pure, no I/O).
    fn target(&self, ctx: &EffectCtx, effect: &LiveEffect) -> Option<Target>;

    /// Executes the effect (the gate was already passed). Errors are guest-visible failures.
    ///
    /// `gate` is passed so the connector re-checks it immediately before secret resolution
    /// and before the send (Pitfall 7).
    async fn execute(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        gate: &dyn SendGate,
    ) -> Result<OpOutput, Failure>;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Always(GateDecision);

    #[async_trait]
    impl SendGate for Always {
        async fn check(&self, _: u16, _: &str, _: u64) -> GateDecision {
            self.0
        }
    }

    // The traits are object-safe (the driver stores `Arc<dyn Connector>` etc.).
    const _: fn(&dyn Connector, &dyn SendGate, &dyn CompletionSink) = |_, _, _| {};

    #[tokio::test]
    async fn gate_is_object_safe_and_callable() {
        let g: Box<dyn SendGate> = Box::new(Always(GateDecision::Abandon));
        assert_eq!(g.check(0, "r", 1).await, GateDecision::Abandon);
        let ctx = EffectCtx {
            cluster_id: "c".into(),
            partition: 0,
            run_id: "r".into(),
            attempt: 1,
            module: "_/m".into(),
            tenant: None,
            cancel: tokio_util::sync::CancellationToken::new(),
        };
        assert!(format!("{ctx:?}").contains("_/m"));
    }
}
