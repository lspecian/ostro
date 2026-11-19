//! Journal events -> Phase 2 [`Derived`] storage intents (04-RESEARCH "Mapping journal
//! events to Derived intents"; normative except `StepAttemptFailed`, see below).
//!
//! # API shape (two phases, chosen in plan 04-15)
//!
//! [`intents`] is synchronous and pure: no I/O, no clock. The parts that need I/O are
//! resolved by the caller ([`crate::sink::PartitionSink`]) before calling it:
//!
//! 1. [`terminal_result_bytes`] returns the bytes of the run's result when the batch ends
//!    the run (`RunCompleted` output data, or the failure JSON of `RunFailed` /
//!    `RunContinuedAsNew`). The caller turns them into a [`PayloadRef`] with
//!    `PartitionHandle::prepare_payload`, which writes payloads over 256 KiB to the blob
//!    store **before** the append.
//! 2. `OpCancelled{op}` needs the event that started `op`. If it is not in the batch, the
//!    caller reads it from the journal and serves it through the `prior` lookup.
//! 3. [`intents`] maps the batch with the prepared result and the lookup.
//!
//! # Mapping
//!
//! | journal event | intent |
//! |---|---|
//! | `TimerStarted{fire_at_nanos}` at seq `s` | `ScheduleTimer{op: s, due_at_ms: ceil(fire_at / 1e6)}` |
//! | `OpCancelled{op}`, `op` a `TimerStarted` | `CancelTimer{op, due_at_ms}` (same rounding) |
//! | `OpCancelled{op}`, `op` anything else | none |
//! | `RunCompleted{output}` | `CompleteRun{Completed, result, completed_at_ms: ceil(ts / 1e6)}` |
//! | `RunFailed{failure}` | `CompleteRun{Failed, result: failure JSON}` |
//! | `RunContinuedAsNew` | `CompleteRun{Failed, result: unsupported JSON}` (Phase 5 implements it) |
//! | everything else | none |
//!
//! `StepAttemptFailed` maps to nothing: the executor (04-16) submits its own `EnqueueReady`
//! after `Evicted{StepBackoff}`. Effects, children and await tokens map to nothing here; the
//! sink adds the Phase 4 "unsupported" completions ([`crate::sink`]).
//!
//! Units: commands carry milliseconds, journal events nanoseconds; due and completion
//! times round **up** (a timer never fires before its journaled time).
//!
//! `CompleteRun` is always the last intent (apply rejects anything after it).

use ostro_types::PayloadRef;
use ostro_types::command::{
    CancelTimer, CompleteRun, Derived, ScheduleTimer, TerminalStatus, derived::Intent,
};
use ostro_types::journal::{Event, EventBody, Failure};

/// Result-payload `reason_code` of a run that ended with `RunFailed`.
pub const REASON_WORKFLOW_FAILED: &str = "workflow_failed";
/// `reason_code` of features that Phase 4 does not support (continue-as-new, await tokens).
pub const REASON_UNSUPPORTED: &str = "unsupported";

/// Errors of the mapping. Each one is a bug in the engine or the caller, never user input.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeriveError {
    /// More than one terminal event, or a terminal event that is not the last event.
    #[error("terminal event at seq {seq} is not the last event of the batch")]
    TerminalNotLast { seq: u64 },
    /// A terminal event, but the caller passed no prepared result payload.
    #[error("terminal event at seq {seq} without a prepared result payload")]
    MissingResult { seq: u64 },
    /// A prepared result payload, but the batch has no terminal event.
    #[error("prepared result payload but no terminal event in the batch")]
    UnexpectedResult,
    /// `OpCancelled{op}` where `op` is neither in the batch nor known to the lookup.
    #[error("OpCancelled at seq {seq} names unknown op {op}")]
    UnknownOp { seq: u64, op: u64 },
}

/// Nanoseconds -> milliseconds, rounded up.
pub fn ceil_ms(nanos: u64) -> u64 {
    nanos.div_ceil(1_000_000)
}

/// JSON body of a failure result: `{"reason_code", "kind", "message"}` (plus `detail` when
/// given). Used for `RunFailed`, unsupported features and (04-16) attempt failures.
pub fn failure_json(reason_code: &str, kind: &str, message: &str) -> Vec<u8> {
    serde_json::json!({
        "reason_code": reason_code,
        "kind": kind,
        "message": message,
    })
    .to_string()
    .into_bytes()
}

/// Failure JSON of a workflow's own `RunFailed{failure}`.
pub fn run_failed_json(f: &Failure) -> Vec<u8> {
    match f {
        Failure::Retryable(i) | Failure::NonRetryable(i) => {
            failure_json(REASON_WORKFLOW_FAILED, &i.kind, &i.message)
        }
        Failure::Timeout => failure_json("timeout", "timeout", "workflow failed with a timeout"),
        Failure::Cancelled => failure_json("cancelled", "cancelled", "workflow was cancelled"),
    }
}

/// Result JSON of an unsupported feature (`{"reason_code": "unsupported", "detail"}` plus
/// `kind`/`message`, so every failure result has the same three keys).
pub fn unsupported_json(detail: &str) -> Vec<u8> {
    serde_json::json!({
        "reason_code": REASON_UNSUPPORTED,
        "kind": REASON_UNSUPPORTED,
        "message": detail,
        "detail": detail,
    })
    .to_string()
    .into_bytes()
}

/// Detail text of `RunContinuedAsNew` in Phase 4.
pub const CONTINUE_AS_NEW_DETAIL: &str = "continue-as-new arrives in Phase 5";

/// `(status, result bytes)` when `ev` is a terminal event.
fn terminal(ev: &Event) -> Option<(TerminalStatus, Vec<u8>)> {
    match &ev.body {
        EventBody::RunCompleted(c) => Some((TerminalStatus::Completed, c.output.data.clone())),
        EventBody::RunFailed(f) => Some((TerminalStatus::Failed, run_failed_json(&f.failure))),
        EventBody::RunContinuedAsNew(_) => Some((
            TerminalStatus::Failed,
            unsupported_json(CONTINUE_AS_NEW_DETAIL),
        )),
        _ => None,
    }
}

/// Bytes of the run result when `events` contains a terminal event (phase 1 of the API).
///
/// `RunCompleted` stores the output's raw `data` (the SDK's JSON); its content type stays in
/// the journal event. Failures store [`run_failed_json`] / [`unsupported_json`].
pub fn terminal_result_bytes(events: &[Event]) -> Option<Vec<u8>> {
    events.iter().find_map(terminal).map(|(_, b)| b)
}

/// Map one appended batch to its intents (phase 3 of the API; see the module docs).
///
/// - `prior(op)`: the body of the journal event at seq `op` when it is not in `events`
///   (only consulted for `OpCancelled`).
/// - `result`: the prepared payload of [`terminal_result_bytes`]; required iff the batch
///   has a terminal event.
pub fn intents(
    events: &[Event],
    prior: &dyn Fn(u64) -> Option<EventBody>,
    result: Option<PayloadRef>,
) -> Result<Vec<Derived>, DeriveError> {
    let mut out = Vec::new();
    let mut result = result;
    let mut done = false;
    for (i, ev) in events.iter().enumerate() {
        if let Some((status, _)) = terminal(ev) {
            if done || i + 1 != events.len() {
                return Err(DeriveError::TerminalNotLast { seq: ev.seq });
            }
            let r = result
                .take()
                .ok_or(DeriveError::MissingResult { seq: ev.seq })?;
            out.push(Derived::new(Intent::CompleteRun(CompleteRun {
                status: status as i32,
                result: Some(r),
                completed_at_ms: ceil_ms(ev.ts_nanos),
                resumable: false,
                parent_event: None,
            })));
            done = true;
            continue;
        }
        match &ev.body {
            EventBody::TimerStarted(t) => {
                out.push(Derived::new(Intent::ScheduleTimer(ScheduleTimer {
                    op: ev.seq,
                    due_at_ms: ceil_ms(t.fire_at_nanos),
                })));
            }
            EventBody::OpCancelled(c) => {
                let started = events
                    .iter()
                    .find(|e| e.seq == c.op)
                    .map(|e| e.body.clone())
                    .or_else(|| prior(c.op))
                    .ok_or(DeriveError::UnknownOp {
                        seq: ev.seq,
                        op: c.op,
                    })?;
                if let EventBody::TimerStarted(t) = started {
                    out.push(Derived::new(Intent::CancelTimer(CancelTimer {
                        op: c.op,
                        due_at_ms: ceil_ms(t.fire_at_nanos),
                    })));
                }
            }
            // No intent: RunStarted (seq 0, written by the executor), StepCompleted,
            // StepAttemptFailed (the executor requeues itself, 04-16), PatchMarker,
            // StatusPublished, WaitResolved, EffectScheduled / ChildStarted /
            // AwaitTokenCreated (the sink adds the Phase 4 unsupported completion),
            // SignalWaitStarted (Phase 5), and inputs (never appended by the engine).
            _ => {}
        }
    }
    if result.is_some() {
        return Err(DeriveError::UnexpectedResult);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use ostro_types::journal::{
        AttemptBackoff, AwaitTokenCreated, ChildOptions, ChildStarted, CommandHash, CommandMeta,
        Continuation, EffectRequest, EffectScheduled, ErrorInfo, HttpRequest, OpCancelled,
        PatchMarker, Payload, RecordedResult, RunCompleted, RunContinuedAsNew, RunFailed,
        SignalWaitStarted, StatusPublished, StepAttemptFailed, StepCompleted, TimerStarted,
        WaitResolved,
    };

    use super::*;

    fn meta() -> CommandMeta {
        CommandMeta {
            hash_ver: 1,
            hash: CommandHash([0; 16]),
        }
    }

    fn ev(seq: u64, ts: u64, body: EventBody) -> Event {
        Event {
            seq,
            ts_nanos: ts,
            body,
        }
    }

    fn none(_: u64) -> Option<EventBody> {
        None
    }

    fn timer(fire_at: u64) -> EventBody {
        EventBody::TimerStarted(TimerStarted {
            meta: meta(),
            fire_at_nanos: fire_at,
        })
    }

    fn cancel(op: u64) -> EventBody {
        EventBody::OpCancelled(OpCancelled { meta: meta(), op })
    }

    fn payload(data: &[u8]) -> Payload {
        Payload {
            content_type: "application/json".into(),
            data: data.to_vec(),
        }
    }

    fn info(kind: &str, message: &str) -> ErrorInfo {
        ErrorInfo {
            kind: kind.into(),
            message: message.into(),
            details: None,
        }
    }

    fn step() -> EventBody {
        EventBody::StepCompleted(StepCompleted {
            meta: meta(),
            name: "s".into(),
            outcome: RecordedResult {
                codec_ver: 1,
                bytes: vec![],
            },
            fuel_used: 0,
        })
    }

    fn pref(b: &'static [u8]) -> PayloadRef {
        PayloadRef::inline(Bytes::from_static(b))
    }

    fn intent(d: &Derived) -> &Intent {
        d.intent.as_ref().expect("intent")
    }

    fn json(b: &[u8]) -> serde_json::Value {
        serde_json::from_slice(b).expect("json")
    }

    #[test]
    fn ceil_rounds_up() {
        assert_eq!(ceil_ms(0), 0);
        assert_eq!(ceil_ms(1), 1);
        assert_eq!(ceil_ms(1_000_000), 1);
        assert_eq!(ceil_ms(1_000_001), 2);
        assert_eq!(ceil_ms(u64::MAX), u64::MAX.div_ceil(1_000_000));
    }

    #[test]
    fn timer_started_schedules_timer_ceil_ms() {
        let d = intents(&[ev(5, 1, timer(1_500_000_001))], &none, None).unwrap();
        assert_eq!(
            d,
            vec![Derived::new(Intent::ScheduleTimer(ScheduleTimer {
                op: 5,
                due_at_ms: 1501
            }))]
        );
    }

    #[test]
    fn op_cancelled_timer_from_prior_journal() {
        let prior = |op: u64| (op == 5).then(|| timer(1_500_000_001));
        let d = intents(&[ev(9, 1, cancel(5))], &prior, None).unwrap();
        assert_eq!(
            d,
            vec![Derived::new(Intent::CancelTimer(CancelTimer {
                op: 5,
                due_at_ms: 1501
            }))]
        );
    }

    #[test]
    fn op_cancelled_timer_in_same_batch() {
        let d = intents(
            &[ev(3, 1, timer(2_000_000)), ev(4, 2, cancel(3))],
            &none,
            None,
        )
        .unwrap();
        assert_eq!(d.len(), 2);
        assert!(matches!(intent(&d[0]), Intent::ScheduleTimer(t) if t.op == 3 && t.due_at_ms == 2));
        assert!(matches!(intent(&d[1]), Intent::CancelTimer(t) if t.op == 3 && t.due_at_ms == 2));
    }

    #[test]
    fn op_cancelled_non_timer_is_no_intent() {
        let prior = |_: u64| {
            Some(EventBody::SignalWaitStarted(SignalWaitStarted {
                meta: meta(),
                name: "x".into(),
                timeout_ms: None,
            }))
        };
        assert!(
            intents(&[ev(9, 1, cancel(2))], &prior, None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn op_cancelled_unknown_op_is_error() {
        assert_eq!(
            intents(&[ev(9, 1, cancel(2))], &none, None),
            Err(DeriveError::UnknownOp { seq: 9, op: 2 })
        );
    }

    #[test]
    fn run_completed_is_last_complete_run() {
        let events = [
            ev(1, 10, step()),
            ev(
                2,
                11,
                EventBody::StatusPublished(StatusPublished {
                    meta: meta(),
                    json: "{}".into(),
                }),
            ),
            ev(
                3,
                2_000_000_001,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(),
                    output: payload(b"{\"ok\":1}"),
                }),
            ),
        ];
        assert_eq!(terminal_result_bytes(&events), Some(b"{\"ok\":1}".to_vec()));
        let d = intents(&events, &none, Some(pref(b"{\"ok\":1}"))).unwrap();
        assert_eq!(
            d,
            vec![Derived::new(Intent::CompleteRun(CompleteRun {
                status: TerminalStatus::Completed as i32,
                result: Some(pref(b"{\"ok\":1}")),
                completed_at_ms: 2001,
                resumable: false,
                parent_event: None,
            }))]
        );
    }

    #[test]
    fn complete_run_after_timer_is_last() {
        let events = [
            ev(1, 1, timer(5_000_000)),
            ev(
                2,
                3_000_000,
                EventBody::RunCompleted(RunCompleted {
                    meta: meta(),
                    output: payload(b"1"),
                }),
            ),
        ];
        let d = intents(&events, &none, Some(pref(b"1"))).unwrap();
        assert_eq!(d.len(), 2);
        assert!(matches!(intent(&d[0]), Intent::ScheduleTimer(_)));
        assert!(matches!(intent(&d[1]), Intent::CompleteRun(c) if c.completed_at_ms == 3));
    }

    #[test]
    fn run_failed_maps_to_failed_with_failure_json() {
        let events = [ev(
            4,
            7,
            EventBody::RunFailed(RunFailed {
                meta: meta(),
                failure: Failure::NonRetryable(info("bad_input", "nope")),
            }),
        )];
        let bytes = terminal_result_bytes(&events).unwrap();
        let v = json(&bytes);
        assert_eq!(v["reason_code"], "workflow_failed");
        assert_eq!(v["kind"], "bad_input");
        assert_eq!(v["message"], "nope");
        let d = intents(&events, &none, Some(PayloadRef::inline(bytes.into()))).unwrap();
        assert!(matches!(
            intent(&d[0]),
            Intent::CompleteRun(c) if c.status == TerminalStatus::Failed as i32 && c.completed_at_ms == 1
        ));
        // Timeout / Cancelled have their own reason codes.
        assert_eq!(
            json(&run_failed_json(&Failure::Timeout))["reason_code"],
            "timeout"
        );
        assert_eq!(
            json(&run_failed_json(&Failure::Cancelled))["reason_code"],
            "cancelled"
        );
    }

    #[test]
    fn continue_as_new_is_unsupported_failure() {
        let events = [ev(
            2,
            1,
            EventBody::RunContinuedAsNew(RunContinuedAsNew {
                meta: meta(),
                continuation: Continuation {
                    input: payload(b"{}"),
                    workflow: None,
                    version: None,
                },
            }),
        )];
        let v = json(&terminal_result_bytes(&events).unwrap());
        assert_eq!(v["reason_code"], "unsupported");
        assert_eq!(v["detail"], "continue-as-new arrives in Phase 5");
        let d = intents(&events, &none, Some(pref(b"x"))).unwrap();
        assert!(matches!(
            intent(&d[0]),
            Intent::CompleteRun(c) if c.status == TerminalStatus::Failed as i32
        ));
    }

    #[test]
    fn no_intent_events() {
        let bodies = vec![
            step(),
            EventBody::StepAttemptFailed(StepAttemptFailed {
                meta: meta(),
                name: "s".into(),
                attempt: 1,
                failure: Failure::Timeout,
                retry_at_nanos: 9_000_000_000,
                backoff: AttemptBackoff::Requeue,
                fuel_used: 0,
            }),
            EventBody::PatchMarker(PatchMarker {
                meta: meta(),
                change_id: "c".into(),
            }),
            EventBody::WaitResolved(WaitResolved {
                meta: meta(),
                completed: vec![1],
            }),
            EventBody::EffectScheduled(EffectScheduled {
                meta: meta(),
                request: EffectRequest::Http(HttpRequest {
                    method: "GET".into(),
                    url: "http://x".into(),
                    headers: vec![],
                    body: None,
                    timeout_ms: None,
                    idempotency_key: None,
                    redact: vec![],
                }),
            }),
            EventBody::ChildStarted(ChildStarted {
                meta: meta(),
                workflow: "w".into(),
                input: payload(b"{}"),
                options: ChildOptions {
                    queue: None,
                    version: None,
                    detached: false,
                    run_timeout_ms: None,
                },
            }),
            EventBody::SignalWaitStarted(SignalWaitStarted {
                meta: meta(),
                name: "x".into(),
                timeout_ms: Some(5),
            }),
            EventBody::AwaitTokenCreated(AwaitTokenCreated {
                meta: meta(),
                token: "t".into(),
                timeout_ms: None,
            }),
        ];
        let events: Vec<Event> = bodies
            .into_iter()
            .enumerate()
            .map(|(i, b)| ev(i as u64 + 1, 1, b))
            .collect();
        assert_eq!(terminal_result_bytes(&events), None);
        assert!(intents(&events, &none, None).unwrap().is_empty());
    }

    #[test]
    fn terminal_rules_are_enforced() {
        let done = || {
            EventBody::RunCompleted(RunCompleted {
                meta: meta(),
                output: payload(b"1"),
            })
        };
        // terminal not last
        assert_eq!(
            intents(
                &[ev(1, 1, done()), ev(2, 1, step())],
                &none,
                Some(pref(b"1"))
            ),
            Err(DeriveError::TerminalNotLast { seq: 1 })
        );
        // missing prepared result
        assert_eq!(
            intents(&[ev(1, 1, done())], &none, None),
            Err(DeriveError::MissingResult { seq: 1 })
        );
        // result without terminal
        assert_eq!(
            intents(&[ev(1, 1, step())], &none, Some(pref(b"1"))),
            Err(DeriveError::UnexpectedResult)
        );
    }
}
