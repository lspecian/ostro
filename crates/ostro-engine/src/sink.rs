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

    /// One live guest log line (08-17, OBS-03). Called only for lines emitted while
    /// **not** replaying and within the per-activation bound
    /// ([`LOG_LINE_LIMIT`](crate::host::state::LOG_LINE_LIMIT) /
    /// [`LOG_BYTE_LIMIT`](crate::host::state::LOG_BYTE_LIMIT)). Logs are never journaled.
    /// The default drops the line (the engine then logs it itself, see `host/log.rs`);
    /// the server forwards it to the node's guest log sink.
    fn guest_log(&self, _rec: GuestLogRecord) {}

    /// Whether [`Self::guest_log`] consumes lines. When `false` (the default) the engine
    /// writes live lines as `tracing` events itself (target `ostro::guest`), so engine
    /// tests and other implementors keep the pre-08-17 behaviour.
    fn handles_guest_logs(&self) -> bool {
        false
    }
}

/// One live guest log line, as handed from the engine to [`ActivationSink::guest_log`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GuestLogRecord {
    /// The run (string form of the run id).
    pub run_id: String,
    /// The workflow name (from the run's `RunStarted`).
    pub workflow: String,
    /// The attempt.
    pub attempt: u32,
    /// `trace` | `debug` | `info` | `warn` | `error`.
    pub level: &'static str,
    /// The message.
    pub message: String,
    /// Structured key/value fields, in guest order.
    pub fields: Vec<(String, String)>,
    /// Emission time (ms since the Unix epoch, the activation's live clock).
    pub ts_ms: u64,
    /// Trace id of the activation span (08-11), when one is active.
    pub trace_id: Option<[u8; 16]>,
    /// Span id of the activation span, when one is active.
    pub span_id: Option<[u8; 8]>,
}

impl GuestLogRecord {
    /// Bytes counted against log byte budgets: message plus every key and value.
    pub fn size(&self) -> usize {
        self.message.len()
            + self
                .fields
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }

    /// The fields as a JSON object string (`{}` when empty). Keys keep guest order; a
    /// repeated key appears repeatedly (JSON parsers keep the last one).
    pub fn fields_json(&self) -> String {
        let mut out = String::with_capacity(2 + self.size() + 6 * self.fields.len());
        out.push('{');
        for (i, (k, v)) in self.fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            json_string(&mut out, k);
            out.push(':');
            json_string(&mut out, v);
        }
        out.push('}');
        out
    }
}

/// Append `s` as a JSON string literal (RFC 8259 escaping).
fn json_string(out: &mut String, s: &str) {
    use std::fmt::Write as _;
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Guest log metric names (08-17; P8-D15 bounded labels, never `run_id`).
pub mod guest_log_metrics {
    /// Accepted guest lines, label `level`.
    pub const LINES: &str = "ostro_guest_log_lines_total";
    /// Dropped guest lines, label `reason`.
    pub const DROPPED: &str = "ostro_guest_log_dropped_total";
    /// Over the per-activation bound (engine).
    pub const REASON_ACTIVATION_BOUND: &str = "activation_bound";
    /// Over the node token bucket.
    pub const REASON_NODE_RATE: &str = "node_rate";
    /// Over the per-run token bucket.
    pub const REASON_RUN_RATE: &str = "run_rate";
    /// Over the per-run lifetime cap.
    pub const REASON_RUN_CAP: &str = "run_cap";

    /// Count one accepted line (the server's guest log sink calls this; it records
    /// through the `metrics` facade the engine already uses).
    pub fn line(level: &'static str) {
        metrics::counter!(LINES, "level" => level).increment(1);
    }

    /// Count one dropped line.
    pub fn dropped(reason: &'static str) {
        metrics::counter!(DROPPED, "reason" => reason).increment(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guest_log_record_fields_json_and_size() {
        let rec = GuestLogRecord {
            run_id: "r".into(),
            workflow: "w".into(),
            attempt: 1,
            level: "info",
            message: "hi".into(),
            fields: vec![
                ("a".into(), "1".into()),
                ("q\"\\".into(), "x\ny\u{7}".into()),
            ],
            ts_ms: 0,
            trace_id: None,
            span_id: None,
        };
        assert_eq!(rec.fields_json(), r#"{"a":"1","q\"\\":"x\ny\u0007"}"#);
        assert_eq!(rec.size(), 2 + 2 + 3 + 4);
    }
}
