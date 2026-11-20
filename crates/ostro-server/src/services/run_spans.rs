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
//! restart never emits a second `workflow.run` for a run that completed before it. Spans
//! are emitted only when OTLP traces are configured.
//!
//! # Run metrics (OBS-02, plan 08-16)
//!
//! The same consumer counts every completed run, with or without OTLP:
//!
//! - `ostro_runs_completed_total{workflow,status}`;
//! - `ostro_dlq_total{reason}` for a **dead-lettered** run: one the executor failed for an
//!   attempt-level or module-level reason (`nondeterminism`, `protocol_violation`,
//!   `fuel_exhausted`, `timeout`, `memory_exceeded`, `stack_overflow`, `guest_exit`,
//!   `trap`, `no_module`, `module_not_found`, `module_invalid`, `bad_run_record`), kept
//!   for `RetryRun`. The reason is the `reason_code` of the stored failure JSON; any other
//!   code is `other`, so the label set is bounded. A workflow's own `RunFailed` (code
//!   `workflow_failed`) is not dead-lettered. No DLQ row exists yet (Phase 11 adds the
//!   `dlq` keyspace rows), and the executor's `fail_run` is in `executor.rs` (another
//!   wave-4 plan's file); counting here, post-apply, also means a restart's log replay
//!   never counts a run twice.
//!
//! Time: the only time read is the `ServiceCtx.clock` fallback for a record without a
//! `completed_at_ms` (not expected for a terminal run).

use std::sync::Arc;

use ostro_engine::telemetry::{RunTrace, emit_run_span, time_from_millis};
use ostro_partition::query;
use ostro_partition::store::values::{RunRecord, RunStatus};
use ostro_partition::{Action, Clock, PartitionHandle, PartitionSlot};
use tokio::sync::broadcast::error::RecvError;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, warn};

use super::ServiceCtx;

/// Start the service: `None` when no partition is ready. Run metrics are always
/// recorded; `workflow.run` spans only when OTLP traces are enabled (`[telemetry]
/// otlp_endpoint` set and `traces = true`).
pub fn spawn(ctx: &ServiceCtx) -> Option<JoinHandle<()>> {
    let t = &ctx.config.telemetry;
    let spans = t.otlp_endpoint.is_some() && t.traces;
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
                        let Action::RunCompleted { run, status } = a else {
                            continue;
                        };
                        match h.read_run(run) {
                            Ok(Some(rec)) => {
                                count_completed(&h, run, &rec).await;
                                if spans {
                                    emit(&run.to_string(), &rec, clock.as_ref());
                                }
                            }
                            Ok(None) => {
                                metrics::counter!(
                                    "ostro_runs_completed_total",
                                    "workflow" => "",
                                    "status" => status_label(*status)
                                )
                                .increment(1);
                                debug!(%run, "completed run already gone; no run span");
                            }
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

/// `reason_code`s of executor failures (attempt-level and module-level) that dead-letter
/// a run (the `ostro_dlq_total{reason}` label set, plus `other`).
pub const DLQ_REASONS: &[&str] = &[
    "nondeterminism",
    "protocol_violation",
    "fuel_exhausted",
    "timeout",
    "memory_exceeded",
    "stack_overflow",
    "guest_exit",
    "trap",
    "no_module",
    "module_not_found",
    "module_invalid",
    "bad_run_record",
];

/// The `ostro_dlq_total` label of a failed run's stored failure JSON, `None` when the run
/// was not dead-lettered (a workflow's own failure, or no executor reason).
pub fn dlq_reason(resumable: bool, failure_json: &[u8]) -> Option<&'static str> {
    let v: serde_json::Value = serde_json::from_slice(failure_json).ok()?;
    let code = v.get("reason_code")?.as_str()?;
    if code == crate::derive::REASON_WORKFLOW_FAILED {
        return None;
    }
    match DLQ_REASONS.iter().find(|r| **r == code) {
        // `timeout` is also a workflow's own `Failure::Timeout`; only the executor's
        // attempt failure is resumable.
        Some(&"timeout") if !resumable => None,
        Some(r) => Some(r),
        None => Some("other"),
    }
}

/// Count one completed run (and a dead-lettered failure).
async fn count_completed(
    h: &PartitionHandle<ostro_log::Rf1Log>,
    run: &ostro_types::RunId,
    rec: &RunRecord,
) {
    metrics::counter!(
        "ostro_runs_completed_total",
        "workflow" => rec.workflow.clone(),
        "status" => status_label(rec.status)
    )
    .increment(1);
    // Executor completions carry no terminal reason (terminate / timeouts do).
    if rec.status != RunStatus::Failed || !rec.terminal_reason.is_empty() {
        return;
    }
    let payload = match query::run_payload(&h.reader(), run, query::PAYLOAD_RESULT) {
        Ok(Some(p)) => p,
        Ok(None) => return,
        Err(e) => {
            debug!(%run, error = %e, "failure payload read failed; DLQ not counted");
            return;
        }
    };
    match h.fetch_payload(&payload).await {
        Ok(bytes) => {
            if let Some(reason) = dlq_reason(rec.resumable, &bytes) {
                metrics::counter!("ostro_dlq_total", "reason" => reason).increment(1);
            }
        }
        Err(e) => debug!(%run, error = %e, "failure payload fetch failed; DLQ not counted"),
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn json(code: &str) -> Vec<u8> {
        crate::derive::failure_json(code, code, "m")
    }

    #[test]
    fn dlq_reason_is_bounded_and_skips_workflow_failures() {
        assert_eq!(
            dlq_reason(true, &json("nondeterminism")),
            Some("nondeterminism")
        );
        assert_eq!(
            dlq_reason(false, &json("module_not_found")),
            Some("module_not_found")
        );
        assert_eq!(dlq_reason(true, &json("timeout")), Some("timeout"));
        assert_eq!(dlq_reason(false, &json("timeout")), None);
        assert_eq!(
            dlq_reason(false, &json(crate::derive::REASON_WORKFLOW_FAILED)),
            None
        );
        assert_eq!(dlq_reason(true, &json("something-new")), Some("other"));
        assert_eq!(dlq_reason(true, b"not json"), None);
    }
}
