//! Host impl of `ostro:runtime/log` (OBS-03, P8-D16).
//!
//! Log lines are never journaled and are dropped while replaying. Live lines are bounded
//! per activation (1000 lines / 1 MiB; the excess is counted in
//! `ostro_guest_log_dropped_total{reason="activation_bound"}`), then handed to the
//! activation sink as a [`GuestLogRecord`] carrying the run, workflow, attempt, the
//! structured fields and the activation span's trace/span ids (08-11). The server's sink
//! applies the node/run rate limits and lifetime caps and emits the node log line and the
//! OTLP LogRecord.
//!
//! Without a sink that consumes guest logs ([`ActivationSink::handles_guest_logs`], e.g.
//! engine tests), the engine writes the line itself as a `tracing` event (target
//! `ostro::guest`) with the fields as one JSON string under `guest.fields`.
//!
//! [`ActivationSink::handles_guest_logs`]: crate::sink::ActivationSink::handles_guest_logs

use crate::host::bindings::ostro::runtime::log::{Host, Level};
use crate::host::state::{HostState, LogLine};
use crate::sink::{GuestLogRecord, guest_log_metrics};

fn level_name(l: Level) -> &'static str {
    match l {
        Level::Trace => "trace",
        Level::Debug => "debug",
        Level::Info => "info",
        Level::Warn => "warn",
        Level::Error => "error",
    }
}

/// The engine's own node-log line (no consuming sink).
fn fallback_event(level: Level, rec: &GuestLogRecord) {
    let fields = rec.fields_json();
    let run = rec.run_id.as_str();
    let workflow = rec.workflow.as_str();
    let attempt = rec.attempt;
    let message = rec.message.as_str();
    macro_rules! ev {
        ($lvl:ident) => {
            tracing::$lvl!(
                target: "ostro::guest",
                run_id = %run,
                workflow = %workflow,
                attempt,
                guest.fields = %fields,
                "{message}"
            )
        };
    }
    match level {
        Level::Trace => ev!(trace),
        Level::Debug => ev!(debug),
        Level::Info => ev!(info),
        Level::Warn => ev!(warn),
        Level::Error => ev!(error),
    }
}

impl Host for HostState {
    async fn emit(
        &mut self,
        level: Level,
        message: String,
        fields: Vec<(String, String)>,
    ) -> wasmtime::Result<()> {
        if self.replay.is_replaying() {
            return Ok(());
        }
        let kept = self.logs.push(LogLine {
            level: level_name(level),
            message: message.clone(),
            fields: fields.clone(),
        });
        if !kept {
            guest_log_metrics::dropped(guest_log_metrics::REASON_ACTIVATION_BOUND);
            return Ok(());
        }
        let (trace_id, span_id) = match self.activation_span_context() {
            Some(cx) if cx.is_valid() => (
                Some(cx.trace_id().to_bytes()),
                Some(cx.span_id().to_bytes()),
            ),
            _ => (None, None),
        };
        let rec = GuestLogRecord {
            run_id: self.run.run_id.clone(),
            workflow: self.replay.run().workflow.clone(),
            attempt: self.attempt,
            level: level_name(level),
            message,
            fields,
            ts_ms: self.replay.live_now_nanos() / 1_000_000,
            trace_id,
            span_id,
        };
        match &self.sink {
            Some(sink) if sink.handles_guest_logs() => sink.guest_log(rec),
            _ => fallback_event(level, &rec),
        }
        Ok(())
    }
}
