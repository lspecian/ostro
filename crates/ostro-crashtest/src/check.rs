//! QA-01 checker (plan 05-19): "no lost accepted runs, no duplicate journal entries".
//!
//! Pure functions over three inputs, so the negative controls (`tests/check_controls.rs`)
//! can feed synthetic data without a server:
//!
//! - the [`Ledger`], built from the harness's fsynced [`Record`] log (intents and 2xx acks);
//! - a run's journal as rendered by `GET /v1/runs/{id}/history` ([`Event`]);
//! - the run row of `GET /v1/runs/{id}` ([`RunView`]).
//!
//! Checks:
//! - (a) [`check_exists`]: every acked run exists ([`Kind::LostAckedRun`]) and one
//!   idempotency key never maps to two runs ([`Kind::KeyConflict`]).
//! - (c) [`check_journal`]: contiguous seqs from 0; exactly one `run_started`, at seq 0 (D24);
//!   at most one terminal event, last apart from markers; at most one `step_completed` per
//!   step (crash-saga step names are unique per run); strictly increasing attempts per step;
//!   each op completed at most once (`timer_fired`, `wait_timed_out`, `signal_matched`, ...);
//!   each signal `request_id` (embedded in the payload, W9) journaled at most once, and every
//!   acked request_id exactly once ([`Kind::LostAckedSignal`]).
//! - (b) [`check_final`]: after the final settle the run is terminal, with the status the
//!   acked lifecycle calls allow and, when Completed, `result == crash_saga_expected(..)`.
//!
//! Never weaken a check to make a campaign pass: a violation is a durability bug.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

// ------------------------------------------------------------------------- records

/// Lifecycle stop call.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stop {
    Cancel,
    Terminate,
}

/// One line of the ack log. Intents are written before a request is sent, acks only after
/// a 2xx response arrived, rejections after a 409 `run_terminal`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "rec", rename_all = "snake_case")]
pub enum Record {
    StartIntent {
        key: String,
        input: Value,
    },
    StartAck {
        key: String,
        run_id: String,
        created: bool,
    },
    SignalIntent {
        run_id: String,
        name: String,
        request_id: String,
        payload: Value,
    },
    SignalAck {
        run_id: String,
        name: String,
        request_id: String,
        duplicate: bool,
    },
    SignalRejected {
        run_id: String,
        name: String,
        request_id: String,
    },
    StopIntent {
        run_id: String,
        stop: Stop,
    },
    StopAck {
        run_id: String,
        stop: Stop,
    },
    StopRejected {
        run_id: String,
        stop: Stop,
    },
}

/// What the harness knows about one idempotency key / run.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RunLedger {
    pub key: String,
    pub input: Value,
    pub run_id: Option<String>,
    /// Every run id a start ack returned for this key (more than one = KeyConflict).
    pub start_acks: BTreeSet<String>,
    /// name -> (request_id, payload) of the intended signal.
    pub signal_intents: BTreeMap<String, (String, Value)>,
    /// name -> request_id of the acked signal.
    pub signals_acked: BTreeMap<String, String>,
    pub signals_rejected: BTreeSet<String>,
    pub stop_intent: Option<Stop>,
    pub stop_acked: Option<Stop>,
    pub stop_rejected: bool,
}

impl RunLedger {
    /// Payload of the first journaled `name` signal (`Null` when none was journaled): the
    /// signal the guest's wait matched. A signal whose response was lost to a kill may still
    /// have been accepted, so this comes from the journal, not from the acks; every acked
    /// signal must be journaled ([`Kind::LostAckedSignal`], checked by [`check_journal`]).
    pub fn accepted_payload(&self, name: &str, events: &[Event]) -> Value {
        events
            .iter()
            .find(|e| {
                e.kind == "signal_payload"
                    && e.details.get("name").and_then(Value::as_str) == Some(name)
            })
            .and_then(|e| e.details.get("payload").cloned())
            .unwrap_or(Value::Null)
    }
}

/// All runs, keyed by idempotency key, plus `run_id -> key`.
#[derive(Clone, Debug, Default)]
pub struct Ledger {
    pub runs: BTreeMap<String, RunLedger>,
    pub by_run: HashMap<String, String>,
}

impl Ledger {
    pub fn from_records<'a>(recs: impl IntoIterator<Item = &'a Record>) -> Self {
        let mut l = Ledger::default();
        for r in recs {
            l.apply(r);
        }
        l
    }

    fn run_mut(&mut self, run_id: &str) -> Option<&mut RunLedger> {
        let key = self.by_run.get(run_id)?;
        self.runs.get_mut(key)
    }

    pub fn apply(&mut self, r: &Record) {
        match r {
            Record::StartIntent { key, input } => {
                let e = self.runs.entry(key.clone()).or_default();
                e.key = key.clone();
                e.input = input.clone();
            }
            Record::StartAck { key, run_id, .. } => {
                let e = self.runs.entry(key.clone()).or_default();
                e.key = key.clone();
                e.start_acks.insert(run_id.clone());
                e.run_id.get_or_insert_with(|| run_id.clone());
                self.by_run.insert(run_id.clone(), key.clone());
            }
            Record::SignalIntent {
                run_id,
                name,
                request_id,
                payload,
            } => {
                if let Some(e) = self.run_mut(run_id) {
                    e.signal_intents
                        .insert(name.clone(), (request_id.clone(), payload.clone()));
                }
            }
            Record::SignalAck {
                run_id,
                name,
                request_id,
                ..
            } => {
                if let Some(e) = self.run_mut(run_id) {
                    e.signals_acked.insert(name.clone(), request_id.clone());
                }
            }
            Record::SignalRejected { run_id, name, .. } => {
                if let Some(e) = self.run_mut(run_id) {
                    e.signals_rejected.insert(name.clone());
                }
            }
            Record::StopIntent { run_id, stop } => {
                if let Some(e) = self.run_mut(run_id) {
                    e.stop_intent = Some(*stop);
                }
            }
            Record::StopAck { run_id, stop } => {
                if let Some(e) = self.run_mut(run_id) {
                    e.stop_acked = Some(*stop);
                }
            }
            Record::StopRejected { run_id, .. } => {
                if let Some(e) = self.run_mut(run_id) {
                    e.stop_rejected = true;
                }
            }
        }
    }

    /// Acked runs (`run_id`, ledger entry).
    pub fn acked(&self) -> impl Iterator<Item = (&str, &RunLedger)> {
        self.runs
            .values()
            .filter_map(|r| r.run_id.as_deref().map(|id| (id, r)))
    }

    pub fn get(&self, run_id: &str) -> Option<&RunLedger> {
        self.by_run.get(run_id).and_then(|k| self.runs.get(k))
    }
}

// ------------------------------------------------------------------------ violations

/// Violation classes. `duplicate_journal_entries` in the report counts the `Duplicate*`
/// kinds; `lost_acked_runs` counts [`Kind::LostAckedRun`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    LostAckedRun,
    LostAckedSignal,
    KeyConflict,
    SeqGap,
    MissingRunStarted,
    DuplicateRunStarted,
    DuplicateTerminal,
    DuplicateStepCompleted,
    DuplicateOpCompletion,
    DuplicateSignal,
    TerminalNotLast,
    AttemptsNotIncreasing,
    NonTerminal,
    WrongStatus,
    WrongResult,
    CheckError,
}

impl Kind {
    pub fn is_duplicate(self) -> bool {
        matches!(
            self,
            Kind::DuplicateRunStarted
                | Kind::DuplicateTerminal
                | Kind::DuplicateStepCompleted
                | Kind::DuplicateOpCompletion
                | Kind::DuplicateSignal
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Violation {
    pub kind: Kind,
    pub run_id: String,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} run {}: {}", self.kind, self.run_id, self.detail)
    }
}

fn v(kind: Kind, run_id: &str, detail: impl Into<String>) -> Violation {
    Violation {
        kind,
        run_id: run_id.to_owned(),
        detail: detail.into(),
    }
}

// ------------------------------------------------------------------------------ (a)

/// (a) Every acked run is in `existing`; no key acked with two different run ids.
pub fn check_exists(ledger: &Ledger, existing: &BTreeSet<String>) -> Vec<Violation> {
    let mut out = Vec::new();
    for r in ledger.runs.values() {
        if r.start_acks.len() > 1 {
            out.push(v(
                Kind::KeyConflict,
                r.run_id.as_deref().unwrap_or(""),
                format!("key {} acked with run ids {:?}", r.key, r.start_acks),
            ));
        }
        for id in &r.start_acks {
            if !existing.contains(id) {
                out.push(v(
                    Kind::LostAckedRun,
                    id,
                    format!("key {} acked but the run does not exist", r.key),
                ));
            }
        }
    }
    out
}

// ------------------------------------------------------------------------------ (c)

/// One journal event as rendered by the history API.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: u64,
    pub kind: String,
    #[serde(default)]
    pub details: Value,
}

impl Event {
    pub fn new(seq: u64, kind: &str, details: Value) -> Self {
        Self {
            seq,
            kind: kind.to_owned(),
            details,
        }
    }
}

const TERMINAL: &[&str] = &[
    "run_completed",
    "run_failed",
    "run_terminated",
    "run_continued_as_new",
];
/// Events allowed after the terminal event.
const MARKERS: &[&str] = &["status_published", "patch_marker"];
/// Events that complete one op (`details.op`).
const OP_COMPLETIONS: &[&str] = &[
    "timer_fired",
    "wait_timed_out",
    "signal_received",
    "signal_matched",
    "effect_completed",
    "child_completed",
    "token_completed",
];

/// The `request_id` embedded in a signal payload (W9).
pub fn embedded_request_id(payload: &Value) -> Option<&str> {
    payload.get("request_id").and_then(Value::as_str)
}

/// (c) Journal invariants of one run. `ledger` (if known) supplies the acked signals,
/// each of which must be journaled exactly once.
pub fn check_journal(run_id: &str, events: &[Event], ledger: Option<&RunLedger>) -> Vec<Violation> {
    let mut out = Vec::new();
    for (i, e) in events.iter().enumerate() {
        if e.seq != i as u64 {
            out.push(v(
                Kind::SeqGap,
                run_id,
                format!("event #{i} has seq {} (expected {i})", e.seq),
            ));
            break;
        }
    }
    let started: Vec<u64> = events
        .iter()
        .filter(|e| e.kind == "run_started")
        .map(|e| e.seq)
        .collect();
    match started.as_slice() {
        [] => out.push(v(Kind::MissingRunStarted, run_id, "no run_started")),
        [0] => {}
        [s] => out.push(v(
            Kind::MissingRunStarted,
            run_id,
            format!("run_started at seq {s}, not 0"),
        )),
        many => out.push(v(
            Kind::DuplicateRunStarted,
            run_id,
            format!("run_started at seqs {many:?}"),
        )),
    }
    let terminals: Vec<&Event> = events
        .iter()
        .filter(|e| TERMINAL.contains(&e.kind.as_str()))
        .collect();
    if terminals.len() > 1 {
        out.push(v(
            Kind::DuplicateTerminal,
            run_id,
            format!(
                "terminal events {:?}",
                terminals
                    .iter()
                    .map(|e| format!("{}@{}", e.kind, e.seq))
                    .collect::<Vec<_>>()
            ),
        ));
    }
    if let Some(t) = terminals.first()
        && let Some(after) = events.iter().find(|e| {
            e.seq > t.seq
                && !MARKERS.contains(&e.kind.as_str())
                && !TERMINAL.contains(&e.kind.as_str())
        })
    {
        out.push(v(
            Kind::TerminalNotLast,
            run_id,
            format!("{}@{} after {}@{}", after.kind, after.seq, t.kind, t.seq),
        ));
    }
    let mut steps: HashMap<&str, u64> = HashMap::new();
    let mut attempts: HashMap<&str, u64> = HashMap::new();
    let mut ops: HashMap<u64, (&str, u64)> = HashMap::new();
    let mut cancelled_ops: HashMap<u64, u64> = HashMap::new();
    let mut rids: HashMap<String, u64> = HashMap::new();
    for e in events {
        let name = e.details.get("name").and_then(Value::as_str).unwrap_or("");
        match e.kind.as_str() {
            "step_completed" => {
                if let Some(prev) = steps.insert(name, e.seq) {
                    out.push(v(
                        Kind::DuplicateStepCompleted,
                        run_id,
                        format!("step {name:?} completed at seqs {prev} and {}", e.seq),
                    ));
                }
            }
            "step_attempt_failed" => {
                let a = e
                    .details
                    .get("attempt")
                    .and_then(Value::as_u64)
                    .unwrap_or(0);
                if let Some(prev) = attempts.insert(name, a)
                    && a <= prev
                {
                    out.push(v(
                        Kind::AttemptsNotIncreasing,
                        run_id,
                        format!(
                            "step {name:?} attempt {a} at seq {} after attempt {prev}",
                            e.seq
                        ),
                    ));
                }
            }
            "op_cancelled" => {
                if let Some(op) = e.details.get("op").and_then(Value::as_u64)
                    && let Some(prev) = cancelled_ops.insert(op, e.seq)
                {
                    out.push(v(
                        Kind::DuplicateOpCompletion,
                        run_id,
                        format!("op {op} cancelled at seqs {prev} and {}", e.seq),
                    ));
                }
            }
            "signal_payload" => {
                let p = e.details.get("payload").unwrap_or(&Value::Null);
                if let Some(rid) = embedded_request_id(p) {
                    let n = rids.entry(rid.to_owned()).or_insert(0);
                    *n += 1;
                    if *n == 2 {
                        out.push(v(
                            Kind::DuplicateSignal,
                            run_id,
                            format!(
                                "request_id {rid:?} journaled twice (second at seq {})",
                                e.seq
                            ),
                        ));
                    }
                }
            }
            k if OP_COMPLETIONS.contains(&k) => {
                if let Some(op) = e.details.get("op").and_then(Value::as_u64)
                    && let Some((pk, ps)) = ops.insert(op, (k, e.seq))
                {
                    out.push(v(
                        Kind::DuplicateOpCompletion,
                        run_id,
                        format!("op {op} completed by {pk}@{ps} and {k}@{}", e.seq),
                    ));
                }
            }
            _ => {}
        }
    }
    if let Some(l) = ledger {
        for (name, rid) in &l.signals_acked {
            if !rids.contains_key(rid) {
                out.push(v(
                    Kind::LostAckedSignal,
                    run_id,
                    format!("signal {name:?} request_id {rid:?} acked but not journaled"),
                ));
            }
        }
    }
    out
}

// ------------------------------------------------------------------------------ (b)

/// `GET /v1/runs/{id}`: the fields the final check uses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RunView {
    pub status: String,
    #[serde(default)]
    pub result: Option<Value>,
    #[serde(default)]
    pub failure: Option<String>,
}

pub fn is_terminal(status: &str) -> bool {
    matches!(
        status,
        "completed" | "failed" | "cancelled" | "terminated" | "timed_out" | "continued_as_new"
    )
}

/// Direct port of the documented crash-saga output function (guests/crash-saga/src/lib.rs):
/// `Ok(output)` of a Completed run that saw no cancel, given the payloads of the signals
/// matched to the `go` and `extra` waits (`Null` = timed out); `Err` = `bad_input` failure.
pub fn crash_saga_expected(input: &Value, go: &Value, extra: &Value) -> Result<Value, String> {
    let id = input.get("id").and_then(Value::as_str).unwrap_or("");
    let steps = input.get("steps").and_then(Value::as_u64).unwrap_or(0);
    if !(3..=8).contains(&steps) {
        return Err(format!("steps {steps} not in 3..=8"));
    }
    let pattern: Vec<char> = input
        .get("fail_pattern")
        .and_then(Value::as_str)
        .unwrap_or("")
        .chars()
        .collect();
    let mut fatal = None;
    for i in 0..steps as usize {
        match pattern.get(i).copied().unwrap_or('0') {
            '0'..='3' => {}
            'x' => {
                fatal.get_or_insert(i);
            }
            c => return Err(format!("fail_pattern[{i}] = {c:?}")),
        }
    }
    Ok(match fatal {
        Some(k) => json!({
            "id": id,
            "status": "compensated",
            "failed_at": k,
            "undone": (0..k).rev().collect::<Vec<_>>(),
        }),
        None => json!({
            "id": id,
            "status": "completed",
            "results": (0..steps).map(|i| format!("{id}-{i}")).collect::<Vec<_>>(),
            "go": go,
            "extra": extra,
        }),
    })
}

/// Crash-saga step retry cap (`max_attempts = 10`).
pub const STEP_MAX_ATTEMPTS: u64 = 10;

/// The journal ends a step with a retryable `transient` error after its last allowed
/// attempt: `STEP_MAX_ATTEMPTS - 1` journaled attempt failures, then the final
/// `step_completed` error. The guest's failure counter restarts with every activation, so
/// crashes and evictions in the middle of a step's retries can exhaust the cap; this is
/// crash-saga's documented `failed` outcome.
pub fn retries_exhausted(events: &[Event]) -> bool {
    events.iter().any(|e| {
        e.kind == "step_completed" && e.details["outcome"]["error"]["kind"] == "transient" && {
            let name = &e.details["name"];
            events
                .iter()
                .filter(|a| a.kind == "step_attempt_failed" && &a.details["name"] == name)
                .count() as u64
                >= STEP_MAX_ATTEMPTS - 1
        }
    })
}

/// (b) Final outcome of one acked run after the settle (`events` = its journal).
pub fn check_final(
    run_id: &str,
    l: &RunLedger,
    view: &RunView,
    events: &[Event],
) -> Vec<Violation> {
    let mut out = Vec::new();
    if !is_terminal(&view.status) {
        out.push(v(
            Kind::NonTerminal,
            run_id,
            format!("status {} after the final settle", view.status),
        ));
        return out;
    }
    let go = l.accepted_payload("go", events);
    let extra = l.accepted_payload("extra", events);
    for (name, got) in [("go", &go), ("extra", &extra)] {
        if !got.is_null() && l.signal_intents.get(name).map(|(_, p)| p) != Some(got) {
            out.push(v(
                Kind::WrongResult,
                run_id,
                format!(
                    "journaled {name} payload {got} was never sent (sent {:?})",
                    l.signal_intents.get(name)
                ),
            ));
        }
    }
    let expected = crash_saga_expected(&l.input, &go, &extra);
    let cancel_sent = l.stop_intent == Some(Stop::Cancel);
    let terminate_sent = l.stop_intent == Some(Stop::Terminate);
    let completed_ok = |out: &mut Vec<Violation>| match &expected {
        Ok(want) if view.result.as_ref() == Some(want) => {}
        Ok(want) => out.push(v(
            Kind::WrongResult,
            run_id,
            format!("result {:?}, expected {want}", view.result),
        )),
        Err(e) => out.push(v(
            Kind::WrongStatus,
            run_id,
            format!("completed, but the input is invalid ({e})"),
        )),
    };
    match (view.status.as_str(), l.stop_acked) {
        ("terminated", _) if terminate_sent => {}
        ("cancelled", _) if cancel_sent => {}
        ("completed", Some(Stop::Terminate)) => out.push(v(
            Kind::WrongStatus,
            run_id,
            "terminate acked but the run completed",
        )),
        ("completed", _) => completed_ok(&mut out),
        // Documented crash-saga outcome: a step exhausted its 10 attempts across crashes.
        ("failed", None)
            if !terminate_sent
                && view
                    .failure
                    .as_deref()
                    .is_some_and(|f| f.contains("transient"))
                && retries_exhausted(events) => {}
        ("failed", _) if expected.is_err() => {}
        (s, acked) => out.push(v(
            Kind::WrongStatus,
            run_id,
            format!(
                "status {s} (failure {:?}); stop intent {:?}, acked {acked:?}",
                view.failure, l.stop_intent
            ),
        )),
    }
    out
}

/// Aggregate counters of a violation list.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tally {
    pub lost_acked_runs: u64,
    pub lost_acked_signals: u64,
    pub duplicate_journal_entries: u64,
    pub non_terminal_runs: u64,
    pub wrong_results: u64,
    pub other_violations: u64,
}

impl Tally {
    pub fn add(&mut self, vs: &[Violation]) {
        for x in vs {
            match x.kind {
                Kind::LostAckedRun => self.lost_acked_runs += 1,
                Kind::LostAckedSignal => self.lost_acked_signals += 1,
                Kind::NonTerminal => self.non_terminal_runs += 1,
                Kind::WrongResult => self.wrong_results += 1,
                k if k.is_duplicate() => self.duplicate_journal_entries += 1,
                _ => self.other_violations += 1,
            }
        }
    }

    pub fn merge(&mut self, o: &Tally) {
        self.lost_acked_runs += o.lost_acked_runs;
        self.lost_acked_signals += o.lost_acked_signals;
        self.duplicate_journal_entries += o.duplicate_journal_entries;
        self.non_terminal_runs += o.non_terminal_runs;
        self.wrong_results += o.wrong_results;
        self.other_violations += o.other_violations;
    }

    pub fn total(&self) -> u64 {
        self.lost_acked_runs
            + self.lost_acked_signals
            + self.duplicate_journal_entries
            + self.non_terminal_runs
            + self.wrong_results
            + self.other_violations
    }
}
