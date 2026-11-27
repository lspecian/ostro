//! Checker `lost` (14-09): no acknowledged run is lost, and every one completes exactly once
//! with the workload's expected result.
//!
//! For every `ok` start (a run id was acknowledged for an idempotency key):
//!
//! - after quiescence the run exists (`GET /v1/runs/{id}`) and, when logs are given, its
//!   `StartRun` was accepted by the offline re-apply of its partition's log;
//! - it is terminal, its journal holds exactly one terminal event, and its outcome is one the
//!   workload allows: `completed` with `result == expected(key)`, `cancelled` only when a
//!   cancel for it was sent (`ok` or `info`), `failed` only with the crash-saga
//!   retries-exhausted journal shape (05-19's documented outcome);
//! - every acknowledged signal's request id is journaled exactly once.
//!
//! For every idempotency key, `ok` or `info`: the acknowledgements never name two run ids and
//! the accepted `StartRun`s of the final logs never hold two run ids (an `info` start may or
//! may not exist, never twice). `fail` starts are never expected to exist.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{LogExport, Op, Verdict};
use crate::history::{ClientOp, OpKind, Outcome};

/// Terminal statuses of `GET /v1/runs/{id}`.
pub const TERMINAL: [&str; 6] = [
    "completed",
    "failed",
    "cancelled",
    "terminated",
    "timed_out",
    "continued_as_new",
];

/// Terminal journal event kinds (`GET /v1/runs/{id}/history`).
pub const TERMINAL_EVENTS: [&str; 4] = [
    "run_completed",
    "run_failed",
    "run_terminated",
    "run_continued_as_new",
];

/// Crash-saga step retry cap (`max_attempts = 10`, guests/crash-saga).
pub const STEP_MAX_ATTEMPTS: usize = 10;

/// One journal event as rendered by `GET /v1/runs/{id}/history`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct JournalEvent {
    /// Journal seq.
    pub seq: u64,
    /// Event kind (`run_started`, `step_completed`, ...).
    pub kind: String,
    /// The rendered details.
    #[serde(default)]
    pub details: Value,
}

impl JournalEvent {
    /// An event (self-tests).
    pub fn new(seq: u64, kind: &str, details: Value) -> Self {
        Self {
            seq,
            kind: kind.to_owned(),
            details,
        }
    }
}

/// What the harness observed about one run after quiescence.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RunObservation {
    /// `GET /v1/runs/{id}` answered 200.
    pub exists: bool,
    /// Its status.
    pub status: Option<String>,
    /// Its result (completed runs).
    pub result: Option<Value>,
    /// Its journal (`None` = unreadable).
    pub journal: Option<Vec<JournalEvent>>,
}

impl RunObservation {
    /// A terminal run with a result and a journal (self-tests).
    pub fn terminal(status: &str, result: Option<Value>, journal: Vec<JournalEvent>) -> Self {
        Self {
            exists: true,
            status: Some(status.to_owned()),
            result,
            journal: Some(journal),
        }
    }

    /// Terminal events in the journal (0 when unreadable).
    pub fn terminal_events(&self) -> usize {
        self.journal.as_ref().map_or(0, |j| {
            j.iter()
                .filter(|e| TERMINAL_EVENTS.contains(&e.kind.as_str()))
                .count()
        })
    }

    /// Whether the journal shows a step that exhausted its retries: its final
    /// `step_completed` is a `transient` error after `STEP_MAX_ATTEMPTS - 1` journaled attempt
    /// failures (crash-saga's documented `failed` outcome, 05-19).
    pub fn retries_exhausted(&self) -> bool {
        let Some(j) = &self.journal else {
            return false;
        };
        j.iter().any(|e| {
            e.kind == "step_completed"
                && e.details["outcome"]["error"]["kind"] == "transient"
                && j.iter()
                    .filter(|a| {
                        a.kind == "step_attempt_failed" && a.details["name"] == e.details["name"]
                    })
                    .count()
                    >= STEP_MAX_ATTEMPTS - 1
        })
    }

    /// Request ids embedded in journaled signal payloads (one element per payload).
    pub fn signal_request_ids(&self) -> Vec<String> {
        self.journal.as_ref().map_or_else(Vec::new, |j| {
            j.iter()
                .filter(|e| e.kind == "signal_payload")
                .filter_map(|e| e.details["payload"]["request_id"].as_str())
                .map(str::to_owned)
                .collect()
        })
    }
}

/// The inputs of [`check_lost`].
#[derive(Clone, Copy, Debug)]
pub struct LostInput<'a> {
    /// The client history.
    pub ops: &'a [ClientOp],
    /// Observations per run id.
    pub runs: &'a BTreeMap<String, RunObservation>,
    /// Expected `completed` result per idempotency key.
    pub expected: &'a BTreeMap<String, Value>,
    /// The final logs (`None` = the log-side checks are skipped, with a note).
    pub logs: Option<&'a LogExport>,
}

/// `(workflow, idempotency key) -> run ids`.
pub type RunsByKey = BTreeMap<(String, String), BTreeSet<String>>;

/// `(workflow, key) -> run ids` and the set of run ids of accepted `StartRun`s in `logs`
/// (accepted per the re-apply where it ran; every `StartRun` of a replica without one).
pub fn started_in_logs(logs: &LogExport) -> (RunsByKey, BTreeSet<String>) {
    let mut by_key = RunsByKey::new();
    let mut runs = BTreeSet::new();
    for p in &logs.partitions {
        for r in &p.replicas {
            for e in &r.entries {
                let Op::StartRun {
                    run_id,
                    workflow,
                    idem_key,
                } = &e.op
                else {
                    continue;
                };
                if r.reapply.is_some() && !r.applied_at(e.offset).is_some_and(|a| a.accepted) {
                    continue;
                }
                runs.insert(run_id.clone());
                if !idem_key.is_empty() {
                    by_key
                        .entry((workflow.clone(), idem_key.clone()))
                        .or_default()
                        .insert(run_id.clone());
                }
            }
        }
    }
    (by_key, runs)
}

/// The `lost` verdict (module docs).
pub fn check_lost(input: LostInput<'_>) -> Verdict {
    let mut v = Verdict::new("lost");
    // Keys -> acknowledged run ids; runs with a cancel attempt; acknowledged signals.
    let mut acked: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut ack_order: Vec<(&str, &str)> = Vec::new();
    let mut cancelled: BTreeSet<&str> = BTreeSet::new();
    let mut signals: BTreeMap<&str, BTreeSet<String>> = BTreeMap::new();
    for op in input.ops {
        match (op.kind, op.outcome) {
            (OpKind::Start, Outcome::Ok) => {
                v.add("acked_starts", 1);
                if let Some(r) = op.run_id.as_deref() {
                    if acked.entry(&op.idem_key).or_default().insert(r) {
                        ack_order.push((&op.idem_key, r));
                    }
                } else {
                    v.violation(format!(
                        "op {}: start of key {} acknowledged without a run id",
                        op.op_id, op.idem_key
                    ));
                }
            }
            (OpKind::Start, Outcome::Info) => v.add("info_starts", 1),
            (OpKind::Start, Outcome::Fail) => v.add("failed_starts", 1),
            (OpKind::Cancel, Outcome::Ok | Outcome::Info) => {
                if let Some(r) = op.run_id.as_deref() {
                    cancelled.insert(r);
                }
            }
            (OpKind::Signal, Outcome::Ok) => {
                if let (Some(r), Some(rid)) = (op.run_id.as_deref(), signal_request_id(op)) {
                    v.add("acked_signals", 1);
                    signals.entry(r).or_default().insert(rid);
                }
            }
            _ => {}
        }
    }
    for (key, runs) in &acked {
        if runs.len() > 1 {
            v.violation(format!(
                "idempotency key {key} acknowledged with {} run ids {runs:?}",
                runs.len()
            ));
        }
    }
    let log_side = input.logs.map(started_in_logs);
    if input.logs.is_none() {
        v.note(
            "no logs given: the log-side checks (accepted StartRun, one run per key) were skipped",
        );
    }
    if let Some((by_key, _)) = &log_side {
        v.add("keys_in_log", by_key.len() as u64);
        for ((wf, key), runs) in by_key {
            if runs.len() > 1 {
                v.violation(format!(
                    "idempotency key {key} (workflow {wf}) started {} runs in the logs: {runs:?}",
                    runs.len()
                ));
            }
        }
    }
    for (key, run) in ack_order {
        v.add("runs_checked", 1);
        if let Some((_, in_log)) = &log_side
            && !in_log.contains(run)
        {
            v.violation(format!(
                "acknowledged run {run} (key {key}): no accepted StartRun in any replica's log"
            ));
        }
        let Some(obs) = input.runs.get(run).filter(|o| o.exists) else {
            v.violation(format!(
                "acknowledged run {run} (key {key}) does not exist after quiescence (lost)"
            ));
            continue;
        };
        let status = obs.status.as_deref().unwrap_or("");
        if !TERMINAL.contains(&status) {
            v.violation(format!(
                "acknowledged run {run} (key {key}) is {status:?}, not terminal, after quiescence (lost)"
            ));
            continue;
        }
        if obs.journal.is_none() {
            v.violation(format!("run {run} (key {key}): journal unreadable"));
        } else if obs.terminal_events() != 1 {
            v.violation(format!(
                "run {run} (key {key}): {} terminal journal events (exactly one expected)",
                obs.terminal_events()
            ));
        }
        match status {
            "completed" => match input.expected.get(key) {
                Some(want) if obs.result.as_ref() == Some(want) => v.add("completed", 1),
                Some(want) => v.violation(format!(
                    "run {run} (key {key}) completed with result {} instead of {want}",
                    obs.result
                        .as_ref()
                        .map_or_else(|| "none".to_owned(), Value::to_string)
                )),
                None => v.violation(format!(
                    "run {run} (key {key}): the workload recorded no expected result"
                )),
            },
            "cancelled" if cancelled.contains(run) => v.add("cancelled", 1),
            "failed" if obs.retries_exhausted() => v.add("failed_retries_exhausted", 1),
            other => v.violation(format!(
                "run {run} (key {key}) ended {other} (allowed: completed with the expected result{})",
                if cancelled.contains(run) {
                    ", cancelled"
                } else {
                    ""
                }
            )),
        }
        if let Some(sent) = signals.get(run) {
            let journaled = obs.signal_request_ids();
            for rid in sent {
                let n = journaled.iter().filter(|x| *x == rid).count();
                if n != 1 {
                    v.violation(format!(
                        "run {run}: acknowledged signal request id {rid} journaled {n} times (exactly once expected)"
                    ));
                }
            }
        }
    }
    v
}

/// The request id of a signal op (`detail` = `<name>/<request id>[ <remark>]`).
pub fn signal_request_id(op: &ClientOp) -> Option<String> {
    let d = op.detail.as_deref()?;
    let (_, rest) = d.split_once('/')?;
    let rid = rest.split_whitespace().next()?;
    Some(rid.to_owned())
}
