//! Journal → normalised projection (D-P9-21, `conformance/spec/SCHEMA.md` vocabulary).
//!
//! A [`Projection`] has three parts:
//!
//! - **`facts`**: the ordered command facts, in journal (= scheduling) order. This is what
//!   a spec's `expect.projection` lists:
//!
//!   | Event | Fact |
//!   |-------|------|
//!   | `StepCompleted` | `step:<name>` |
//!   | `StepAttemptFailed` | `retry:<name>#<attempt>` |
//!   | `TimerStarted` | `timer#N` |
//!   | `SignalWaitStarted` | `signal:<name>#N` (N counts per signal name) |
//!   | `AwaitTokenCreated` | `token#N` |
//!   | `ChildStarted` | `child:<base workflow>#N` (N per base name; `rs_`/`py_`/`ts_` stripped) |
//!   | `OpCancelled` | `cancel-op#N` |
//!   | `EffectScheduled` | `effect:<http\|sql\|object>#N` (N per kind) |
//!   | `PatchMarker` | `patch:<change id>` |
//!   | `StatusPublished` | `status:<canonical json>` |
//!   | `RunCompleted` / `RunFailed` / `RunContinuedAsNew` / `RunTerminated` | `terminal:<completed\|failed\|cancelled\|continued_as_new\|terminated>` |
//!
//!   Dropped: `RunStarted`, `WaitResolved` (so Rust's re-issued `wait([op, CANCEL])` and
//!   the Python/TS remembered cancellation project equal), `SignalPayload` (the matching
//!   `SignalMatched` carries the payload), `ts_nanos`, command hashes and `fuel_used`.
//!
//! - **`values`**: recorded outcomes and arguments as canonical JSON, in order:
//!   `step:<name>=<json>` / `step:<name>!<failure>`, `child:<wf>#N<-<input json>`,
//!   `cancel-op#N-><target label>`, `effect:<kind>#N=<canonical args>`,
//!   `terminal=<output json>` / `terminal!<failure>` / `terminal->continue:<input json>`.
//!   Per-language value probes (C-NATIVE-01) skip them in cross-language comparison.
//!
//! - **`inputs`**: input events as a multiset keyed by the ordinal label of the op they
//!   complete (`fired:timer#1`, `signal:go#1=<json>`, `timeout:signal:never#1`,
//!   `token#1=<json>`, `child:leaf#1=<outcome>`, `effect:http#1=done`, `cancel`, ...).
//!
//! Op ids never appear: every op is renamed to its ordinal label.

use std::collections::{BTreeMap, HashMap};

use ostro_types::journal::{
    ChildCompleted, EffectCompleted, EffectRequest, Event, EventBody, Failure, OpId, OpOutput,
    Payload, decode_result,
};

use crate::canon;

/// Normalised projection of one journal (or of a continue-as-new chain).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Projection {
    /// Ordered command facts (the SCHEMA vocabulary).
    pub facts: Vec<String>,
    /// Recorded outcomes / arguments as canonical JSON, in order.
    pub values: Vec<String>,
    /// Input events as a multiset (label → count).
    pub inputs: BTreeMap<String, usize>,
}

impl Projection {
    /// Appends `other` (the next run of a continue-as-new chain).
    pub fn extend(&mut self, other: Projection) {
        self.facts.extend(other.facts);
        self.values.extend(other.values);
        for (k, n) in other.inputs {
            *self.inputs.entry(k).or_default() += n;
        }
    }
}

/// Strips the D-P9-23 language prefix (`rs_`, `py_`, `ts_`) from a workflow name.
pub fn base_workflow(name: &str) -> &str {
    for p in ["rs_", "py_", "ts_"] {
        if let Some(rest) = name.strip_prefix(p) {
            return rest;
        }
    }
    name
}

/// Canonical text of a JSON payload.
pub fn payload_json(p: &Payload) -> String {
    canon::canonical_bytes(&p.data)
}

/// Compact failure text: `non_retryable:<kind>`, `retryable:<kind>`, `timeout`, `cancelled`.
pub fn failure_text(f: &Failure) -> String {
    match f {
        Failure::Retryable(e) => format!("retryable:{}", e.kind),
        Failure::NonRetryable(e) => format!("non_retryable:{}", e.kind),
        Failure::Timeout => "timeout".to_string(),
        Failure::Cancelled => "cancelled".to_string(),
    }
}

/// Failure kind as compared by `expect.failure_kind`.
pub fn failure_kind(f: &Failure) -> String {
    match f {
        Failure::Retryable(e) | Failure::NonRetryable(e) => e.kind.clone(),
        Failure::Timeout => "timeout".to_string(),
        Failure::Cancelled => "cancelled".to_string(),
    }
}

/// Terminal status name of a terminal event, if `body` is one.
pub fn terminal_status(body: &EventBody) -> Option<&'static str> {
    match body {
        EventBody::RunCompleted(_) => Some("completed"),
        EventBody::RunFailed(f) if f.failure == Failure::Cancelled => Some("cancelled"),
        EventBody::RunFailed(_) => Some("failed"),
        EventBody::RunContinuedAsNew(_) => Some("continued_as_new"),
        EventBody::RunTerminated(_) => Some("terminated"),
        _ => None,
    }
}

fn effect_kind(r: &EffectRequest) -> &'static str {
    match r {
        EffectRequest::Http(_) => "http",
        EffectRequest::Sql(_) => "sql",
        EffectRequest::Object(_) => "object",
    }
}

/// Language-neutral effect arguments (bodies and header values are excluded: their byte
/// formatting differs per SDK).
fn effect_args(r: &EffectRequest) -> String {
    let v = match r {
        EffectRequest::Http(h) => serde_json::json!({
            "method": h.method,
            "url": h.url,
            "headers": h.headers.iter().map(|x| x.name.to_ascii_lowercase()).collect::<Vec<_>>(),
            "idempotency_key": h.idempotency_key,
        }),
        EffectRequest::Sql(s) => serde_json::json!({
            "connection": s.connection,
            "statement": s.statement,
            "params": s.params.len(),
            "exactly_once": s.exactly_once,
        }),
        EffectRequest::Object(o) => serde_json::json!({
            "connection": o.connection,
            "op": o.op,
            "key": o.key,
        }),
    };
    canon::canonical(&v)
}

fn result_text(r: &Result<Payload, Failure>) -> String {
    match r {
        Ok(p) => format!("={}", payload_json(p)),
        Err(f) => format!("!{}", failure_text(f)),
    }
}

fn op_output_text(r: &Result<OpOutput, Failure>) -> String {
    match r {
        Ok(OpOutput::Child(p) | OpOutput::Signal(p) | OpOutput::Token(p)) => {
            format!("={}", payload_json(p))
        }
        Ok(OpOutput::Timer) => "=timer".to_string(),
        Ok(OpOutput::CancelRequested) => "=cancel".to_string(),
        Ok(OpOutput::Http(h)) => format!("=http:{}", h.status),
        Ok(OpOutput::Sql(s)) => format!("=sql:{}", s.rows.len()),
        Ok(OpOutput::Object(b)) => format!("=object:{}", b.len()),
        Err(f) => format!("!{}", failure_text(f)),
    }
}

/// Per-kind ordinal counters and the op → label map.
#[derive(Default)]
struct Labels {
    counters: HashMap<String, u32>,
    ops: HashMap<OpId, String>,
}

impl Labels {
    fn next(&mut self, key: &str) -> u32 {
        let n = self.counters.entry(key.to_string()).or_default();
        *n += 1;
        *n
    }

    fn label(&self, op: OpId) -> String {
        self.ops
            .get(&op)
            .cloned()
            .unwrap_or_else(|| "op?".to_string())
    }
}

/// Projects one journal.
pub fn project(journal: &[Event]) -> Projection {
    let mut p = Projection::default();
    let mut l = Labels::default();
    // SignalPayload seq → payload, for SignalMatched.
    let mut payloads: HashMap<u64, &Payload> = HashMap::new();
    fn input(p: &mut Projection, key: String) {
        *p.inputs.entry(key).or_default() += 1;
    }
    for ev in journal {
        let op = ev.seq;
        match &ev.body {
            EventBody::RunStarted(_) | EventBody::WaitResolved(_) => {}
            EventBody::StepCompleted(s) => {
                p.facts.push(format!("step:{}", s.name));
                let r: Result<Payload, Failure> = decode_result(&s.outcome)
                    .unwrap_or_else(|_| Err(Failure::NonRetryable(undecodable())));
                p.values.push(format!("step:{}{}", s.name, result_text(&r)));
            }
            EventBody::StepAttemptFailed(s) => {
                p.facts.push(format!("retry:{}#{}", s.name, s.attempt));
            }
            EventBody::TimerStarted(_) => {
                let n = l.next("timer");
                let label = format!("timer#{n}");
                p.facts.push(label.clone());
                l.ops.insert(op, label);
            }
            EventBody::SignalWaitStarted(w) => {
                let n = l.next(&format!("signal:{}", w.name));
                let label = format!("signal:{}#{n}", w.name);
                p.facts.push(label.clone());
                l.ops.insert(op, label);
            }
            EventBody::AwaitTokenCreated(_) => {
                let n = l.next("token");
                let label = format!("token#{n}");
                p.facts.push(label.clone());
                l.ops.insert(op, label);
            }
            EventBody::ChildStarted(c) => {
                let base = base_workflow(&c.workflow);
                let n = l.next(&format!("child:{base}"));
                let label = format!("child:{base}#{n}");
                p.facts.push(label.clone());
                p.values
                    .push(format!("{label}<-{}", payload_json(&c.input)));
                l.ops.insert(op, label);
            }
            EventBody::OpCancelled(c) => {
                let n = l.next("cancel-op");
                let label = format!("cancel-op#{n}");
                p.facts.push(label.clone());
                p.values.push(format!("{label}->{}", l.label(c.op)));
                l.ops.insert(op, label);
            }
            EventBody::EffectScheduled(e) => {
                let kind = effect_kind(&e.request);
                let n = l.next(&format!("effect:{kind}"));
                let label = format!("effect:{kind}#{n}");
                p.facts.push(label.clone());
                p.values
                    .push(format!("{label}={}", effect_args(&e.request)));
                l.ops.insert(op, label);
            }
            EventBody::PatchMarker(m) => p.facts.push(format!("patch:{}", m.change_id)),
            EventBody::StatusPublished(s) => {
                let json = serde_json::from_str::<serde_json::Value>(&s.json).map_or_else(
                    |_| format!("<invalid:{}>", s.json),
                    |v| canon::canonical(&v),
                );
                p.facts.push(format!("status:{json}"));
            }
            EventBody::RunCompleted(c) => {
                p.facts.push("terminal:completed".to_string());
                p.values
                    .push(format!("terminal={}", payload_json(&c.output)));
            }
            EventBody::RunFailed(f) => {
                let status = terminal_status(&ev.body).unwrap_or("failed");
                p.facts.push(format!("terminal:{status}"));
                p.values
                    .push(format!("terminal!{}", failure_text(&f.failure)));
            }
            EventBody::RunContinuedAsNew(c) => {
                p.facts.push("terminal:continued_as_new".to_string());
                p.values.push(format!(
                    "terminal->continue:{}",
                    payload_json(&c.continuation.input)
                ));
            }
            EventBody::RunTerminated(_) => p.facts.push("terminal:terminated".to_string()),
            // Inputs: a multiset keyed by the ordinal label of the completed op.
            EventBody::TimerFired(t) => input(&mut p, format!("fired:{}", l.label(t.op))),
            EventBody::WaitTimedOut(t) => input(&mut p, format!("timeout:{}", l.label(t.op))),
            EventBody::SignalReceived(s) => input(
                &mut p,
                format!("{}={}", l.label(s.op), payload_json(&s.payload)),
            ),
            EventBody::SignalPayload(s) => {
                payloads.insert(op, &s.payload);
            }
            EventBody::SignalMatched(m) => {
                let data = payloads
                    .get(&m.payload_seq)
                    .map_or_else(|| "<missing payload>".to_string(), |p| payload_json(p));
                input(&mut p, format!("{}={data}", l.label(m.op)));
            }
            EventBody::TokenCompleted(t) => input(
                &mut p,
                format!("{}={}", l.label(t.op), payload_json(&t.payload)),
            ),
            EventBody::ChildCompleted(ChildCompleted { op, outcome }) => {
                let r: Result<OpOutput, Failure> = decode_result(outcome)
                    .unwrap_or_else(|_| Err(Failure::NonRetryable(undecodable())));
                input(&mut p, format!("{}{}", l.label(*op), op_output_text(&r)));
            }
            EventBody::EffectCompleted(EffectCompleted { op, outcome }) => {
                let r: Result<OpOutput, Failure> = decode_result(outcome)
                    .unwrap_or_else(|_| Err(Failure::NonRetryable(undecodable())));
                input(&mut p, format!("{}{}", l.label(*op), op_output_text(&r)));
            }
            EventBody::CancelRequested(_) => input(&mut p, "cancel".to_string()),
        }
    }
    p
}

/// Projects a continue-as-new chain (each run's journal in order).
pub fn project_chain(journals: &[Vec<Event>]) -> Projection {
    let mut p = Projection::default();
    for j in journals {
        p.extend(project(j));
    }
    p
}

fn undecodable() -> ostro_types::journal::ErrorInfo {
    ostro_types::journal::ErrorInfo {
        kind: "<undecodable outcome>".to_string(),
        message: String::new(),
        details: None,
    }
}
