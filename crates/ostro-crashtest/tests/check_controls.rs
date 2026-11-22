//! Negative controls for the QA-01 checker (plan 05-19), default suite, no server.
//!
//! A clean synthetic crash-saga history passes; each injected failure class (lost ack,
//! duplicate StepCompleted, two completions of one op, a duplicate request_id signal, a wrong
//! result, plus the D24 / seq / terminal / attempt invariants) produces its own specific
//! violation kind.

use std::collections::BTreeSet;

use ostro_crashtest::check::{
    Event, Kind, Ledger, Record, RunView, Stop, Tally, check_exists, check_final, check_journal,
    crash_saga_expected,
};
use ostro_crashtest::workload::{Plan, plan_input, request_id, signal_payload};
use serde_json::{Value, json};

const RUN: &str = "run-1";
const KEY: &str = "s00-1";

fn input() -> Value {
    json!({"id": KEY, "steps": 3, "fail_pattern": "01", "sleep_ms": 10,
           "signal_timeout_ms": 600000, "plan": {"signals": true}})
}

fn records() -> Vec<Record> {
    let mut r = vec![
        Record::StartIntent {
            key: KEY.into(),
            input: input(),
        },
        Record::StartAck {
            key: KEY.into(),
            run_id: RUN.into(),
            created: true,
        },
    ];
    for name in ["go", "extra"] {
        r.push(Record::SignalIntent {
            run_id: RUN.into(),
            name: name.into(),
            request_id: request_id(KEY, name),
            payload: signal_payload(KEY, name),
        });
        r.push(Record::SignalAck {
            run_id: RUN.into(),
            name: name.into(),
            request_id: request_id(KEY, name),
            duplicate: false,
        });
    }
    r
}

fn ledger() -> Ledger {
    Ledger::from_records(&records())
}

fn expected_output() -> Value {
    crash_saga_expected(
        &input(),
        &signal_payload(KEY, "go"),
        &signal_payload(KEY, "extra"),
    )
    .unwrap()
}

/// A plausible complete journal of the run (step1 retried once).
fn journal() -> Vec<Event> {
    let e = |kind: &str, d: Value| (kind.to_owned(), d);
    let evs = vec![
        e(
            "run_started",
            json!({"workflow": "crash-saga", "input": input()}),
        ),
        e(
            "signal_payload",
            json!({"name": "go", "payload": signal_payload(KEY, "go")}),
        ),
        e(
            "step_completed",
            json!({"name": "step0", "outcome": {"ok": "s00-1-0"}}),
        ),
        e(
            "step_attempt_failed",
            json!({"name": "step1", "attempt": 1}),
        ),
        e(
            "step_completed",
            json!({"name": "step1", "outcome": {"ok": "s00-1-1"}}),
        ),
        e(
            "step_completed",
            json!({"name": "step2", "outcome": {"ok": "s00-1-2"}}),
        ),
        e("timer_started", json!({"fire_at_ms": 1})), // seq 6
        e("timer_fired", json!({"op": 6})),
        e(
            "signal_wait_started",
            json!({"name": "go", "timeout_ms": 600000}),
        ), // seq 8
        e("signal_matched", json!({"op": 8, "payload_seq": 1})),
        e("timer_started", json!({"fire_at_ms": 2})), // 10
        e("timer_started", json!({"fire_at_ms": 3})), // 11
        e(
            "signal_wait_started",
            json!({"name": "extra", "timeout_ms": 600000}),
        ), // 12
        e(
            "signal_payload",
            json!({"name": "extra", "payload": signal_payload(KEY, "extra")}),
        ), // 13
        e("signal_matched", json!({"op": 12, "payload_seq": 13})),
        e("timer_fired", json!({"op": 10})),
        e("timer_fired", json!({"op": 11})),
        e("run_completed", json!({"output": expected_output()})),
        e("status_published", json!({"status": {}})),
    ];
    evs.into_iter()
        .enumerate()
        .map(|(i, (k, d))| Event::new(i as u64, &k, d))
        .collect()
}

fn completed_view() -> RunView {
    RunView {
        status: "completed".into(),
        result: Some(expected_output()),
        failure: None,
    }
}

fn kinds(vs: &[ostro_crashtest::check::Violation]) -> Vec<Kind> {
    vs.iter().map(|v| v.kind).collect()
}

fn renumber(mut evs: Vec<Event>) -> Vec<Event> {
    for (i, e) in evs.iter_mut().enumerate() {
        e.seq = i as u64;
    }
    evs
}

#[test]
fn clean_history_passes() {
    let l = ledger();
    let existing: BTreeSet<String> = [RUN.to_owned()].into();
    assert!(check_exists(&l, &existing).is_empty());
    let vs = check_journal(RUN, &journal(), l.get(RUN));
    assert!(vs.is_empty(), "{vs:?}");
    let vs = check_final(RUN, l.get(RUN).unwrap(), &completed_view(), &journal());
    assert!(vs.is_empty(), "{vs:?}");
}

#[test]
fn control_lost_ack() {
    let l = ledger();
    let vs = check_exists(&l, &BTreeSet::new());
    assert_eq!(kinds(&vs), [Kind::LostAckedRun]);
    let mut t = Tally::default();
    t.add(&vs);
    assert_eq!(t.lost_acked_runs, 1);
}

#[test]
fn control_key_conflict() {
    let mut recs = records();
    recs.push(Record::StartAck {
        key: KEY.into(),
        run_id: "run-2".into(),
        created: false,
    });
    let l = Ledger::from_records(&recs);
    let existing: BTreeSet<String> = [RUN.to_owned(), "run-2".to_owned()].into();
    assert_eq!(kinds(&check_exists(&l, &existing)), [Kind::KeyConflict]);
}

#[test]
fn control_duplicate_step_completed() {
    let mut j = journal();
    j.insert(
        5,
        Event::new(
            0,
            "step_completed",
            json!({"name": "step1", "outcome": {"ok": "s00-1-1"}}),
        ),
    );
    let vs = check_journal(RUN, &renumber(j), ledger().get(RUN));
    assert_eq!(kinds(&vs), [Kind::DuplicateStepCompleted], "{vs:?}");
    let mut t = Tally::default();
    t.add(&vs);
    assert_eq!(t.duplicate_journal_entries, 1);
}

#[test]
fn control_two_completions_for_one_op() {
    let mut j = journal();
    // The go wait (op 8) both matched and timed out.
    j.insert(10, Event::new(0, "wait_timed_out", json!({"op": 8})));
    let vs = check_journal(RUN, &renumber(j), ledger().get(RUN));
    assert_eq!(kinds(&vs), [Kind::DuplicateOpCompletion], "{vs:?}");
    // A timer fired twice.
    let mut j = journal();
    j.insert(8, Event::new(0, "timer_fired", json!({"op": 6})));
    let vs = check_journal(RUN, &renumber(j), ledger().get(RUN));
    assert_eq!(kinds(&vs), [Kind::DuplicateOpCompletion], "{vs:?}");
}

#[test]
fn control_duplicate_signal() {
    let mut j = journal();
    j.insert(
        2,
        Event::new(
            0,
            "signal_payload",
            json!({"name": "go", "payload": signal_payload(KEY, "go")}),
        ),
    );
    let vs = check_journal(RUN, &renumber(j), ledger().get(RUN));
    assert_eq!(kinds(&vs), [Kind::DuplicateSignal], "{vs:?}");
}

#[test]
fn control_lost_acked_signal() {
    let j: Vec<Event> = journal()
        .into_iter()
        .filter(|e| !(e.kind == "signal_payload" && e.details["name"] == "extra"))
        .collect();
    let vs = check_journal(RUN, &renumber(j), ledger().get(RUN));
    assert_eq!(kinds(&vs), [Kind::LostAckedSignal], "{vs:?}");
}

#[test]
fn control_wrong_result() {
    let l = ledger();
    let mut view = completed_view();
    view.result.as_mut().unwrap()["results"][1] = json!("s00-1-9");
    assert_eq!(
        kinds(&check_final(RUN, l.get(RUN).unwrap(), &view, &journal())),
        [Kind::WrongResult]
    );
    // A completed run whose go signal is missing from the result.
    let mut view = completed_view();
    view.result.as_mut().unwrap()["go"] = Value::Null;
    assert_eq!(
        kinds(&check_final(RUN, l.get(RUN).unwrap(), &view, &journal())),
        [Kind::WrongResult]
    );
}

#[test]
fn control_status_rules() {
    let mut recs = records();
    recs.push(Record::StopIntent {
        run_id: RUN.into(),
        stop: Stop::Terminate,
    });
    recs.push(Record::StopAck {
        run_id: RUN.into(),
        stop: Stop::Terminate,
    });
    let l = Ledger::from_records(&recs);
    let r = l.get(RUN).unwrap();
    assert_eq!(
        kinds(&check_final(RUN, r, &completed_view(), &journal())),
        [Kind::WrongStatus]
    );
    let term = RunView {
        status: "terminated".into(),
        result: None,
        failure: None,
    };
    assert!(check_final(RUN, r, &term, &journal()).is_empty());
    // Cancelled without any cancel sent is wrong; non-terminal after the settle is reported.
    let r = ledger();
    let r = r.get(RUN).unwrap();
    let cancelled = RunView {
        status: "cancelled".into(),
        result: None,
        failure: None,
    };
    assert_eq!(
        kinds(&check_final(RUN, r, &cancelled, &journal())),
        [Kind::WrongStatus]
    );
    let running = RunView {
        status: "running".into(),
        result: None,
        failure: None,
    };
    assert_eq!(
        kinds(&check_final(RUN, r, &running, &journal())),
        [Kind::NonTerminal]
    );
    // An acked cancel may lose the race against completion: completed with the right result.
    let mut recs = records();
    recs.push(Record::StopIntent {
        run_id: RUN.into(),
        stop: Stop::Cancel,
    });
    recs.push(Record::StopAck {
        run_id: RUN.into(),
        stop: Stop::Cancel,
    });
    let l = Ledger::from_records(&recs);
    assert!(check_final(RUN, l.get(RUN).unwrap(), &completed_view(), &journal()).is_empty());
    assert!(check_final(RUN, l.get(RUN).unwrap(), &cancelled, &journal()).is_empty());
}

#[test]
fn control_d24_seq_terminal_attempts() {
    let l = ledger();
    // Second RunStarted (recovery re-appended it).
    let mut j = journal();
    j.insert(1, Event::new(0, "run_started", json!({})));
    assert_eq!(
        kinds(&check_journal(RUN, &renumber(j), l.get(RUN))),
        [Kind::DuplicateRunStarted]
    );
    // Gap in seqs.
    let mut j = journal();
    j.remove(3);
    let vs = check_journal(RUN, &j, l.get(RUN));
    assert_eq!(kinds(&vs), [Kind::SeqGap], "{vs:?}");
    // Two terminal events.
    let mut j = journal();
    j.push(Event::new(0, "run_failed", json!({})));
    assert_eq!(
        kinds(&check_journal(RUN, &renumber(j), l.get(RUN))),
        [Kind::DuplicateTerminal]
    );
    // An input after the terminal event.
    let mut j = journal();
    j.push(Event::new(0, "cancel_requested", json!({})));
    assert_eq!(
        kinds(&check_journal(RUN, &renumber(j), l.get(RUN))),
        [Kind::TerminalNotLast]
    );
    // Attempts not strictly increasing.
    let mut j = journal();
    j.insert(
        4,
        Event::new(
            0,
            "step_attempt_failed",
            json!({"name": "step1", "attempt": 1}),
        ),
    );
    assert_eq!(
        kinds(&check_journal(RUN, &renumber(j), l.get(RUN))),
        [Kind::AttemptsNotIncreasing]
    );
}

#[test]
fn expected_function_matches_the_documented_cases() {
    let go = json!({"g": 1});
    let i = json!({"id": "a", "steps": 4, "fail_pattern": "3210"});
    assert_eq!(
        crash_saga_expected(&i, &go, &Value::Null).unwrap(),
        json!({"id": "a", "status": "completed", "results": ["a-0", "a-1", "a-2", "a-3"],
               "go": {"g": 1}, "extra": null})
    );
    let i = json!({"id": "b", "steps": 5, "fail_pattern": "01x0x"});
    assert_eq!(
        crash_saga_expected(&i, &go, &go).unwrap(),
        json!({"id": "b", "status": "compensated", "failed_at": 2, "undone": [1, 0]})
    );
    assert!(crash_saga_expected(&json!({"id": "c", "steps": 2}), &go, &go).is_err());
    assert!(
        crash_saga_expected(
            &json!({"id": "c", "steps": 3, "fail_pattern": "x0z"}),
            &go,
            &go
        )
        .is_err()
    );
    // Characters beyond `steps` are ignored.
    assert!(
        crash_saga_expected(
            &json!({"id": "c", "steps": 3, "fail_pattern": "000z"}),
            &go,
            &go
        )
        .is_ok()
    );
}

#[test]
fn workload_plans_are_deterministic_and_valid() {
    let mut stops = 0;
    for n in 0..2000 {
        let key = format!("s01-{n}");
        let i = plan_input(7, &key);
        assert_eq!(i, plan_input(7, &key));
        assert!(
            crash_saga_expected(&i, &Value::Null, &Value::Null).is_ok(),
            "{i}"
        );
        let p = Plan::of(&i);
        assert!(!p.b1 || p.signals);
        stops += u32::from(p.stop.is_some());
    }
    assert!((60..220).contains(&stops), "stops {stops}");
}

#[test]
fn signal_accepted_but_response_lost_is_not_a_wrong_result() {
    // Campaign case (dev run, shard 0): the go response was lost to a kill, the run then
    // completed with go, and the retry got 409 run_terminal. The result reflects the
    // journaled (accepted) signal.
    let recs: Vec<Record> = records()
        .into_iter()
        .filter(|r| !matches!(r, Record::SignalAck { name, .. } if name == "go"))
        .chain([Record::SignalRejected {
            run_id: RUN.into(),
            name: "go".into(),
            request_id: request_id(KEY, "go"),
        }])
        .collect();
    let l = Ledger::from_records(&recs);
    let vs = check_final(RUN, l.get(RUN).unwrap(), &completed_view(), &journal());
    assert!(vs.is_empty(), "{vs:?}");
    assert!(check_journal(RUN, &journal(), l.get(RUN)).is_empty());
}

#[test]
fn control_journaled_signal_never_sent() {
    let mut j = journal();
    j[1].details["payload"]["key"] = json!("forged");
    let vs = check_final(RUN, ledger().get(RUN).unwrap(), &completed_view(), &j);
    assert!(kinds(&vs).contains(&Kind::WrongResult), "{vs:?}");
}

#[test]
fn failed_is_accepted_only_after_retry_exhaustion() {
    let failed = RunView {
        status: "failed".into(),
        result: None,
        failure: Some(r#"{"kind":"transient","message":"step 1 simulated failure"}"#.into()),
    };
    let l = ledger();
    let r = l.get(RUN).unwrap();
    // A transient failure without exhausted retries is a wrong status.
    assert_eq!(
        kinds(&check_final(RUN, r, &failed, &journal())),
        [Kind::WrongStatus]
    );
    // Nine journaled attempt failures, then the final error: the documented outcome.
    let mut j = vec![Event::new(0, "run_started", json!({}))];
    for a in 1..=9 {
        j.push(Event::new(
            0,
            "step_attempt_failed",
            json!({"name": "step1", "attempt": a}),
        ));
    }
    j.push(Event::new(
        0,
        "step_completed",
        json!({"name": "step1",
        "outcome": {"error": {"kind": "transient", "type": "retryable"}}}),
    ));
    j.push(Event::new(0, "run_failed", json!({})));
    let j = renumber(j);
    assert!(check_journal(RUN, &j, None).is_empty());
    assert!(check_final(RUN, r, &failed, &j).is_empty());
}

/// A journal in which step1 exhausted its retries (nine attempt failures, then the final
/// transient error) and the run failed: crash-saga's documented `failed` outcome.
fn exhausted_journal() -> Vec<Event> {
    let mut j = vec![Event::new(0, "run_started", json!({}))];
    for a in 1..=9 {
        j.push(Event::new(
            0,
            "step_attempt_failed",
            json!({"name": "step1", "attempt": a}),
        ));
    }
    j.push(Event::new(
        0,
        "step_completed",
        json!({"name": "step1",
        "outcome": {"error": {"kind": "transient", "type": "retryable"}}}),
    ));
    j.push(Event::new(0, "run_failed", json!({})));
    renumber(j)
}

/// `records()` plus a terminate intent and the given outcome records.
fn terminate_ledger(outcome: &[Record]) -> Ledger {
    let mut recs = records();
    recs.push(Record::StopIntent {
        run_id: RUN.into(),
        stop: Stop::Terminate,
    });
    recs.extend(outcome.iter().cloned());
    Ledger::from_records(&recs)
}

/// D26 (shard 00, run 004038fnrafsrxswnmeq2kfv4qbn6): a terminate rejected with 409
/// `run_terminal` found the run already failed by retry exhaustion; it is not a terminate
/// that was sent for the status rule. Every other status rule is unchanged.
#[test]
fn control_d26_rejected_terminate() {
    let failed = RunView {
        status: "failed".into(),
        result: None,
        failure: Some(r#"{"kind":"transient","message":"step 1 simulated failure"}"#.into()),
    };
    let rejected = Record::StopRejected {
        run_id: RUN.into(),
        stop: Stop::Terminate,
    };
    let acked = Record::StopAck {
        run_id: RUN.into(),
        stop: Stop::Terminate,
    };
    let exhausted = exhausted_journal();
    assert!(check_journal(RUN, &exhausted, None).is_empty());

    // Positive: rejected terminate + failed by retry exhaustion -> no violation.
    let l = terminate_ledger(std::slice::from_ref(&rejected));
    let r = l.get(RUN).unwrap();
    assert!(check_final(RUN, r, &failed, &exhausted).is_empty());

    // Negative: rejected terminate + failed WITHOUT exhausted retries -> WrongStatus.
    assert_eq!(
        kinds(&check_final(RUN, r, &failed, &journal())),
        [Kind::WrongStatus]
    );

    // Negative: rejected terminate + completed with a wrong result -> WrongResult.
    let mut wrong = completed_view();
    wrong.result.as_mut().unwrap()["results"][1] = json!("s00-1-9");
    assert_eq!(
        kinds(&check_final(RUN, r, &wrong, &journal())),
        [Kind::WrongResult]
    );
    // ... and with the right result it passes (the terminate was refused).
    assert!(check_final(RUN, r, &completed_view(), &journal()).is_empty());

    // Negative: rejected terminate + non-terminal run -> NonTerminal.
    let running = RunView {
        status: "running".into(),
        result: None,
        failure: None,
    };
    assert_eq!(
        kinds(&check_final(RUN, r, &running, &journal())),
        [Kind::NonTerminal]
    );

    // Unchanged: a rejected retry after a first attempt whose 200 was lost may still have
    // terminated the run, so `terminated` stays accepted.
    let term = RunView {
        status: "terminated".into(),
        result: None,
        failure: None,
    };
    assert!(check_final(RUN, r, &term, &journal()).is_empty());

    // Negative: acked terminate + failed (even by retry exhaustion) -> WrongStatus.
    let l = terminate_ledger(std::slice::from_ref(&acked));
    assert_eq!(
        kinds(&check_final(RUN, l.get(RUN).unwrap(), &failed, &exhausted)),
        [Kind::WrongStatus]
    );

    // Negative: acked AND rejected (ack, then a rejected retry) + failed -> WrongStatus.
    let l = terminate_ledger(&[acked.clone(), rejected.clone()]);
    assert_eq!(
        kinds(&check_final(RUN, l.get(RUN).unwrap(), &failed, &exhausted)),
        [Kind::WrongStatus]
    );

    // Unchanged: an unacked terminate with no rejection + failed -> WrongStatus.
    let l = terminate_ledger(&[]);
    assert_eq!(
        kinds(&check_final(RUN, l.get(RUN).unwrap(), &failed, &exhausted)),
        [Kind::WrongStatus]
    );

    // Unchanged: a rejected cancel (no terminate intent) + failed by retry exhaustion passes.
    let mut recs = records();
    recs.push(Record::StopIntent {
        run_id: RUN.into(),
        stop: Stop::Cancel,
    });
    recs.push(Record::StopRejected {
        run_id: RUN.into(),
        stop: Stop::Cancel,
    });
    let l = Ledger::from_records(&recs);
    assert!(check_final(RUN, l.get(RUN).unwrap(), &failed, &exhausted).is_empty());
}

/// The journal of a run whose go wait (op 7) timed out before the go signal was journaled
/// (campaign-p8 shard 04: the harness's go POSTs reached the server only after the 600 s
/// wait timeout). The late go stays buffered; the guest saw `go = null`.
fn late_go_journal() -> Vec<Event> {
    let mut j = journal();
    let go = j.remove(1);
    j[6].details = json!({"op": 5}); // timer_fired of the timer_started now at seq 5
    assert_eq!(j[7].kind, "signal_wait_started");
    j[8] = Event::new(0, "wait_timed_out", json!({"op": 7}));
    j.insert(9, go);
    renumber(j)
}

fn go_null_view() -> RunView {
    let mut view = completed_view();
    view.result.as_mut().unwrap()["go"] = Value::Null;
    view
}

#[test]
fn late_signal_after_the_timeout_is_a_timeout_outcome() {
    let l = ledger();
    let r = l.get(RUN).unwrap();
    let j = late_go_journal();
    assert!(check_journal(RUN, &j, Some(r)).is_empty());
    // The timeout won the wait: go = null is the correct result.
    let vs = check_final(RUN, r, &go_null_view(), &j);
    assert!(vs.is_empty(), "{vs:?}");
    // A result claiming the late (never matched) go is wrong.
    assert_eq!(
        kinds(&check_final(RUN, r, &completed_view(), &j)),
        [Kind::WrongResult]
    );
    // A late payload the harness never sent is still flagged although no wait consumed it.
    let mut forged = j.clone();
    forged[9].details["payload"]["key"] = json!("forged");
    assert!(kinds(&check_final(RUN, r, &go_null_view(), &forged)).contains(&Kind::WrongResult));
}

#[test]
fn control_lost_wake() {
    // go journaled at seq 1, before the go wait (op 8) started, yet the wait timed out:
    // registration must have matched the buffered signal.
    let l = ledger();
    let r = l.get(RUN).unwrap();
    let mut j = journal();
    assert_eq!(j[9].kind, "signal_matched");
    j[9] = Event::new(9, "wait_timed_out", json!({"op": 8}));
    assert_eq!(
        kinds(&check_final(RUN, r, &go_null_view(), &j)),
        [Kind::LostWake]
    );
    assert_eq!(
        kinds(&check_final(RUN, r, &completed_view(), &j)),
        [Kind::LostWake, Kind::WrongResult]
    );
    // Same with the signal journaled after the wait started but before its timeout fired.
    let late_wait = late_go_journal();
    let mut j = late_wait.clone();
    let go = j.remove(9); // go payload (after wait_timed_out@8)
    j.insert(8, go); // now between signal_wait_started@7 and wait_timed_out
    let mut j = renumber(j);
    j[9].details = json!({"op": 7});
    assert_eq!(j[9].kind, "wait_timed_out");
    assert_eq!(
        kinds(&check_final(RUN, r, &go_null_view(), &j)),
        [Kind::LostWake]
    );
    // A signal_matched that points at no go payload is a broken journal.
    let mut j = journal();
    j[9].details = json!({"op": 8, "payload_seq": 2});
    assert!(kinds(&check_final(RUN, r, &completed_view(), &j)).contains(&Kind::WrongResult));
}
