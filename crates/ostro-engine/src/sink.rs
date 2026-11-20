//! The `ActivationSink` seam between the engine and durable storage (docs/03).
//!
//! Phase 3 tests run against an in-memory fake (testkit); Phase 4 implements it on
//! the partition log.

use ostro_types::journal::{Continuation, Event, Failure, Payload};

use crate::effects_driver::EffectDriver;
use crate::error::{AttemptFailure, EvictReason};

/// Identifies a run on a partition.
///
/// Phase 4 maps `ostro_types::ids::RunId` into this; Phase 3 does not depend on Phase 2 ids.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RunRef {
    pub partition: u16,
    pub run_id: String,
}

/// Request to run one activation of a run attempt.
#[derive(Debug)]
pub struct Activate {
    pub run: RunRef,
    pub attempt: u32,
    /// SHA-256 of the component bytes (fetched via [`ActivationSink::fetch_blob`]).
    pub module_sha: [u8; 32],
    /// Committed journal; `journal[0]` is `RunStarted`.
    pub journal: Vec<Event>,
    /// Input events committed while this activation is parked (hot wait).
    pub inputs: Option<tokio::sync::mpsc::UnboundedReceiver<Event>>,
    /// External eviction.
    pub cancel: tokio_util::sync::CancellationToken,
}

/// How an activation ended.
#[derive(Debug, Clone, PartialEq)]
pub enum ActivationOutcome {
    Completed { output: Payload },
    Failed { failure: Failure },
    ContinuedAsNew { continuation: Continuation },
    Evicted { reason: EvictReason },
    AttemptFailed(AttemptFailure),
    Fenced,
}

/// Errors from the sink.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AppendError {
    #[error("fenced")]
    Fenced,
    #[error("seq conflict, expected {expected}")]
    Conflict { expected: u64 },
    #[error("unavailable: {0}")]
    Unavailable(String),
}

/// Durable storage as seen by the engine. Secrets are resolved by the effect driver, never here.
#[async_trait::async_trait]
pub trait ActivationSink: Send + Sync {
    /// Append `events` starting at `from_seq` for this attempt; returns the next seq.
    async fn append(
        &self,
        run: &RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError>;

    /// Fetch a content-addressed blob (component bytes).
    async fn fetch_blob(&self, sha: &[u8; 32]) -> Result<Vec<u8>, AppendError>;

    /// Report the activation outcome.
    async fn done(&self, run: &RunRef, attempt: u32, outcome: ActivationOutcome);

    /// The driver that performs effect I/O for activations on this sink (Phase 6, P-1).
    ///
    /// Fetched once per activation. The default `None` keeps the Phase 4/5 behaviour:
    /// effects are journaled and nothing is ever started (see [`crate::effects_driver`]).
    fn effect_driver(&self) -> Option<std::sync::Arc<dyn EffectDriver>> {
        None
    }
}
