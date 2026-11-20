//! `workflow.run` span emission (OBS-01, P8-D14, plan 08-11).
//!
//! The service subscribes to every ready partition's **post-apply action stream**
//! (`MachineHandle::subscribe_actions`, the same broadcast the dispatcher, timer and
//! shuffler services consume). On `Action::RunCompleted` it reads the run record and emits
//! one `workflow.run` span through [`ostro_engine::telemetry::emit_run_span`]:
//!
//! - trace id: the run's stored traceparent, else `blake3("ostro/trace" ‖ run_id)[..16]`;
//! - span id: `span_id(run, "run", 0)` (the deterministic parent every activation span
//!   already referenced; OTLP backends accept the late parent);
//! - parent: the caller's span from the traceparent, when present;
//! - start = `created_at_ms`, end = `completed_at_ms` (both from the record, so the span is
//!   reproducible), status Ok for `Completed` / `ContinuedAsNew`, Error otherwise.
//!
//! **No re-emission:** actions are only published once the partition is past its startup
//! catch-up (02-12; the machine's `emit` flag is off while it replays the log), so a
//! restart never emits a second `workflow.run` for a run that completed before it. The
//! service starts only when OTLP traces are configured; otherwise it runs no task.
//!
//! Time: the only time read is the `ServiceCtx.clock` fallback for a record without a
//! `completed_at_ms` (not expected for a terminal run).

use std::sync::Arc;

use ostro_engine::telemetry::{RunTrace, emit_run_span, time_from_millis};
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{Action, Clock, PartitionSlot};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, warn};

use super::ServiceCtx;

/// Start the service: `None` unless OTLP traces are enabled (`[telemetry] otlp_endpoint`
/// set and `traces = true`) and at least one partition is ready.
pub fn spawn(ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    let t = &ctx.config.telemetry;
    if t.otlp_endpoint.is_none() || !t.traces {
        return None;
    }
    let handles: Vec<_> = ctx
        .node
        .slots()
        .iter()
        .filter_map(|s| match s {
            PartitionSlot::Ready(h) => Some(h.clone()),
            PartitionSlot::Failed { .. } => None,
        })
        .collect();
    if handles.is_empty() {
        return None;
    }
    let shutdown = ctx.shutdown.clone();
    let clock = Arc::clone(&ctx.clock);
    // Subscribe before returning, so no action published after `Services::start` is missed.
    let subs: Vec<_> = handles
        .into_iter()
        .map(|h| {
            let rx = h.machine().subscribe_actions();
            (h, rx)
        })
        .collect();
    Some(tokio::spawn(async move {
        let mut set = JoinSet::new();
        for (h, mut rx) in subs {
            let shutdown = shutdown.clone();
            let clock = Arc::clone(&clock);
            set.spawn(async move {
                loop {
                    let batch = tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => return,
                        r = rx.recv() => r,
                    };
                    let actions = match batch {
                        Ok(a) => a,
                        Err(RecvError::Lagged(n)) => {
                            warn!(
                                partition = h.partition().0,
                                skipped = n,
                                "run_spans lagged; some workflow.run spans are lost"
                            );
                            continue;
                        }
                        Err(RecvError::Closed) => return,
                    };
                    for a in actions.iter() {
                        let Action::RunCompleted { run, .. } = a else {
                            continue;
                        };
                        match h.read_run(run) {
                            Ok(Some(rec)) => emit(&run.to_string(), &rec, clock.as_ref()),
                            Ok(None) => debug!(%run, "completed run already gone; no run span"),
                            Err(e) => {
                                warn!(%run, error = %e, "run record read failed; no run span")
                            }
                        }
                    }
                }
            });
        }
        while set.join_next().await.is_some() {}
    }))
}

/// Stable status label of a terminal run.
fn status_label(s: RunStatus) -> &'static str {
    match s {
        RunStatus::Scheduled => "scheduled",
        RunStatus::Ready => "ready",
        RunStatus::Running => "running",
        RunStatus::Completed => "completed",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
        RunStatus::Terminated => "terminated",
        RunStatus::TimedOut => "timed_out",
        RunStatus::ContinuedAsNew => "continued_as_new",
    }
}

/// Emit the `workflow.run` span of a completed run (public for tests and tooling).
pub fn emit(run_id: &str, rec: &RunRecord, clock: &dyn Clock) {
    let trace = RunTrace::new(run_id, rec.traceparent.as_deref());
    let start = time_from_millis(rec.created_at_ms);
    let end = time_from_millis(rec.completed_at_ms.unwrap_or_else(|| clock.now_ms()));
    let ok = matches!(rec.status, RunStatus::Completed | RunStatus::ContinuedAsNew);
    emit_run_span(
        &trace,
        &rec.workflow,
        start,
        end,
        status_label(rec.status),
        ok,
    );
}
