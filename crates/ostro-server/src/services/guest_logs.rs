//! Phase 8: implemented in 08-17.
//!
//! The node-level guest log sink (P8-D16): per-run token bucket and lifetime caps, a node
//! bucket, a non-durable per-run ring buffer (`GET /v1/runs/{id}/logs`), the node log line
//! and an OTLP LogRecord per accepted guest line. The executor holds the sink from
//! construction (`EngineExecutor::with_guest_log_sink`), so 08-17 fills the internals
//! without changing the wiring. Until then [`GuestLogSink::emit`] is a no-op.

use crate::config::GuestLogsSection;

/// One guest log line as handed over by the executor.
#[derive(Clone, Copy, Debug)]
pub struct GuestLine<'a> {
    /// The run id (string form).
    pub run_id: &'a str,
    /// The workflow name.
    pub workflow: &'a str,
    /// The attempt (grant counter).
    pub attempt: u32,
    /// Guest log level (`trace` .. `error`).
    pub level: &'a str,
    /// The message.
    pub message: &'a str,
    /// Structured fields as a JSON object string (may be empty).
    pub fields_json: &'a str,
}

/// The guest log sink (stub until 08-17).
#[derive(Debug)]
pub struct GuestLogSink {
    cfg: GuestLogsSection,
}

impl GuestLogSink {
    pub fn new(cfg: &GuestLogsSection) -> Self {
        Self { cfg: cfg.clone() }
    }

    /// The `[guest_logs]` limits in force.
    pub fn config(&self) -> &GuestLogsSection {
        &self.cfg
    }

    /// Accept one guest line (rate limits, ring buffer, OTLP: 08-17). No-op for now.
    pub fn emit(&self, _line: &GuestLine<'_>) {}
}
