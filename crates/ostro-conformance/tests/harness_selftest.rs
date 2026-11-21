//! Harness self-test (09-11 Task 1): canonical JSON, the normalised projection and the
//! comparators, proven on synthetic journals built in code (no guests, no engine).
//!
//! The comparator must accept language-neutral differences (op-id renumbering, ts,
//! command hashes, fuel, Rust's re-issued `wait([op, CANCEL])` vs the Python/TS
//! remembered cancellation, child-name prefixes, JSON formatting) and reject semantic ones
//! (renamed step, swapped command order, different outcome JSON, missing terminal).

use ostro_conformance::canon;
use ostro_conformance::compare::{self, Opts};
use ostro_conformance::langs::{self, Lang};
use ostro_conformance::projection::{self, Projection, project};
use ostro_types::journal::{
    AttemptBackoff, AwaitTokenCreated, CANCELLATION_OP, CancelRequested, ChildCompleted,
    ChildOptions, ChildStarted, CommandHash, CommandMeta, ErrorInfo, Event, EventBody, Failure,
    HASH_VER, OpCancelled, OpOutput, PatchMarker, Payload, RngSeed, RunCompleted, RunFailed,
    RunStarted, SignalMatched, SignalPayload, SignalWaitStarted, StatusPublished,
    StepAttemptFailed, StepCompleted, TimerFired, TimerStarted, TokenCompleted, WaitResolved,
    WaitTimedOut, encode_result,
};
use serde_json::json;

// ---------------------------------------------------------------------------
// Synthetic journal builder
// ---------------------------------------------------------------------------

/// Builds a journal; `ts`, hashes and fuel vary with `salt` to prove they are ignored.
struct J {
    events: Vec<Event>,
    salt: u8,
}

fn json_payload(text: &str) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: text.as_bytes().to_vec(),
    }
}

impl J {
    fn new(workflow: &str, salt: u8) -> Self {
        let mut j = Self {
            events: Vec::new(),
            salt,
        };
        j.push(EventBody::RunStarted(RunStarted {
            workflow: workflow.into(),
            workflow_version: 1,
            input: json_payload("null"),
            rng_seed: RngSeed([salt; 32]),
            accepted_at_nanos: 1,
            deadline_nanos: None,
            module_hash: format!("{salt:02x}"),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        }));
        j
    }

    fn meta(&self) -> CommandMeta {
        CommandMeta {
            hash_ver: HASH_VER,
            hash: CommandHash([self.salt.wrapping_add(self.events.len() as u8); 16]),
        }
    }

    /// Appends and returns the op id (= seq).
    fn push(&mut self, body: EventBody) -> u64 {
        let seq = self.events.len() as u64;
        self.events.push(Event {
            seq,
            ts_nanos: 1_000 * seq + u64::from(self.salt) * 7,
            body,
        });
        seq
    }

    fn step(&mut self, name: &str, out: &str) -> u64 {
        let meta = self.meta();
        let outcome: Result<Payload, Failure> = Ok(json_payload(out));
        self.push(EventBody::StepCompleted(StepCompleted {
            meta,
            name: name.into(),
            outcome: encode_result(&outcome),
            fuel_used: 1000 + u64::from(self.salt),
        }))
    }

    fn retry(&mut self, name: &str, attempt: u32) -> u64 {
        let meta = self.meta();
        self.push(EventBody::StepAttemptFailed(StepAttemptFailed {
            meta,
            name: name.into(),
            attempt,
            failure: Failure::Retryable(ErrorInfo {
                kind: "flaky".into(),
                message: format!("salt {}", self.salt),
                details: None,
            }),
            retry_at_nanos: 5,
            backoff: AttemptBackoff::Requeue,
            fuel_used: u64::from(self.salt),
        }))
    }

    fn timer(&mut self) -> u64 {
        let meta = self.meta();
        self.push(EventBody::TimerStarted(TimerStarted {
            meta,
            fire_at_nanos: 3_600,
        }))
    }

    fn fired(&mut self, op: u64) -> u64 {
        self.push(EventBody::TimerFired(TimerFired { op }))
    }

    fn signal_wait(&mut self, name: &str, timeout_ms: Option<u64>) -> u64 {
        let meta = self.meta();
        self.push(EventBody::SignalWaitStarted(SignalWaitStarted {
            meta,
            name: name.into(),
            timeout_ms,
        }))
    }

    fn signal(&mut self, name: &str, op: u64, payload: &str) {
        let seq = self.push(EventBody::SignalPayload(SignalPayload {
            name: name.into(),
            payload: json_payload(payload),
        }));
        self.push(EventBody::SignalMatched(SignalMatched {
            op,
            payload_seq: seq,
        }));
    }

    fn wait_resolved(&mut self, completed: Vec<u64>) -> u64 {
        let meta = self.meta();
        self.push(EventBody::WaitResolved(WaitResolved { meta, completed }))
    }

    fn cancel_op(&mut self, op: u64) -> u64 {
        let meta = self.meta();
        self.push(EventBody::OpCancelled(OpCancelled { meta, op }))
    }

    fn completed(&mut self, out: &str) -> u64 {
        let meta = self.meta();
        self.push(EventBody::RunCompleted(RunCompleted {
            meta,
            output: json_payload(out),
        }))
    }

    fn failed(&mut self, failure: Failure) -> u64 {
        let meta = self.meta();
        self.push(EventBody::RunFailed(RunFailed { meta, failure }))
    }
}

/// Reference journal: step a, step b (retried once), a 1 h sleep fired, a signal, a patch
/// marker and a status, then completion.
fn reference(salt: u8, wrap_waits: bool) -> Vec<Event> {
    let mut j = J::new("rs_conf_demo", salt);
    j.step("a", r#"{"v":1,"w":[1,2]}"#);
    j.retry("b", 1);
    j.step("b", r#""b""#);
    j.push(EventBody::PatchMarker(PatchMarker {
        meta: j.meta(),
        change_id: "x".into(),
    }));
    let t = j.timer();
    if wrap_waits {
        j.wait_resolved(vec![]);
    }
    j.fired(t);
    if wrap_waits {
        j.wait_resolved(vec![t]);
    }
    let s = j.signal_wait("go", None);
    j.signal("go", s, r#"{"n":1}"#);
    j.push(EventBody::StatusPublished(StatusPublished {
        meta: j.meta(),
        json: r#"{"stage":"done","n":2}"#.into(),
    }));
    j.completed(r#"{"a":{"v":1,"w":[1,2]},"b":"b"}"#);
    j.events
}

fn facts(j: &[Event]) -> Vec<String> {
    project(j).facts
}

// ---------------------------------------------------------------------------
// Canonical JSON
// ---------------------------------------------------------------------------

#[test]
fn canon_sorts_keys_and_unifies_integral_numbers() {
    let a: serde_json::Value = serde_json::from_str(r#"{"b":1,"a":[1.0,2]}"#).unwrap();
    let b: serde_json::Value = serde_json::from_str(r#"{"a":[1,2.0],"b":1}"#).unwrap();
    assert!(canon::equal(&a, &b));
    assert_eq!(canon::canonical(&a), r#"{"a":[1,2],"b":1}"#);
}

#[test]
fn canon_compares_non_integral_as_f64() {
    let sum = json!(0.1 + 0.2);
    let lit: serde_json::Value = serde_json::from_str("0.30000000000000004").unwrap();
    assert!(canon::equal(&sum, &lit));
    assert!(!canon::equal(&json!(0.3), &lit));
}

#[test]
fn canon_string_and_number_differ_and_nan_is_null() {
    assert!(!canon::equal(&json!("1"), &json!(1)));
    let nan = serde_json::Value::from(f64::NAN);
    assert_eq!(canon::canonical(&nan), "null");
    assert_eq!(canon::canonical(&json!([f64::INFINITY])), "[null]");
    assert!(!canon::canonical(&json!({"x": f64::NAN})).contains("NaN"));
}

#[test]
fn canon_large_integers_exact() {
    let a: serde_json::Value = serde_json::from_str("18446744073709551615").unwrap();
    let b: serde_json::Value = serde_json::from_str("18446744073709551614").unwrap();
    assert!(!canon::equal(&a, &b));
}

// ---------------------------------------------------------------------------
// Projection vocabulary
// ---------------------------------------------------------------------------

#[test]
fn projection_vocabulary() {
    let mut j = J::new("rs_conf_all", 1);
    j.step("a", "1");
    j.retry("flaky", 1);
    j.retry("flaky", 2);
    j.step("flaky", "3");
    let t = j.timer();
    j.fired(t);
    let s = j.signal_wait("go", Some(10_000));
    j.push(EventBody::WaitTimedOut(WaitTimedOut { op: s }));
    let tok = j.push(EventBody::AwaitTokenCreated(AwaitTokenCreated {
        meta: j.meta(),
        token: "tok-1".into(),
        timeout_ms: None,
    }));
    j.push(EventBody::TokenCompleted(TokenCompleted {
        op: tok,
        payload: json_payload(r#"{"ok": true}"#),
    }));
    let c = j.push(EventBody::ChildStarted(ChildStarted {
        meta: j.meta(),
        workflow: "py_leaf_echo".into(),
        input: json_payload(r#"{"x":1}"#),
        options: ChildOptions {
            queue: None,
            version: None,
            detached: false,
            run_timeout_ms: None,
        },
    }));
    let child_out: Result<OpOutput, Failure> = Ok(OpOutput::Child(json_payload("2")));
    j.push(EventBody::ChildCompleted(ChildCompleted {
        op: c,
        outcome: encode_result(&child_out),
    }));
    let t2 = j.timer();
    j.cancel_op(t2);
    j.push(EventBody::PatchMarker(PatchMarker {
        meta: j.meta(),
        change_id: "x".into(),
    }));
    j.push(EventBody::StatusPublished(StatusPublished {
        meta: j.meta(),
        json: r#"{"b": 2, "a": 1.0}"#.into(),
    }));
    j.push(EventBody::CancelRequested(CancelRequested {
        reason: "test".into(),
    }));
    j.failed(Failure::Cancelled);
    let p = project(&j.events);
    assert_eq!(
        p.facts,
        vec![
            "step:a",
            "retry:flaky#1",
            "retry:flaky#2",
            "step:flaky",
            "timer#1",
            "signal:go#1",
            "token#1",
            "child:leaf_echo#1",
            "timer#2",
            "cancel-op#1",
            "patch:x",
            r#"status:{"a":1,"b":2}"#,
            "terminal:cancelled",
        ]
    );
    assert_eq!(
        p.values,
        vec![
            "step:a=1",
            "step:flaky=3",
            r#"child:leaf_echo#1<-{"x":1}"#,
            "cancel-op#1->timer#2",
            "terminal!cancelled",
        ]
    );
    let inputs: Vec<(&str, usize)> = p.inputs.iter().map(|(k, n)| (k.as_str(), *n)).collect();
    assert_eq!(
        inputs,
        vec![
            ("cancel", 1),
            ("child:leaf_echo#1=2", 1),
            ("fired:timer#1", 1),
            ("timeout:signal:go#1", 1),
            (r#"token#1={"ok":true}"#, 1),
        ]
    );
}

#[test]
fn projection_drops_wait_resolved_ts_hash_fuel() {
    let a = project(&reference(1, false));
    let b = project(&reference(9, true));
    assert_eq!(
        a, b,
        "salted ts/hash/fuel and extra WaitResolved must not matter"
    );
    assert!(!a.facts.iter().any(|f| f.contains("wait")));
}

#[test]
fn projection_renames_op_ids_to_ordinals() {
    // Same commands, different op ids: extra WaitResolved events shift every seq.
    let mut j = J::new("rs_w", 2);
    j.wait_resolved(vec![]);
    j.wait_resolved(vec![]);
    let t = j.timer();
    j.wait_resolved(vec![]);
    j.fired(t);
    let mut k = J::new("rs_w", 3);
    let t2 = k.timer();
    k.fired(t2);
    assert_ne!(t, t2);
    assert_eq!(project(&j.events), project(&k.events));
    assert!(project(&j.events).inputs.contains_key("fired:timer#1"));
}

#[test]
fn rust_reissued_cancel_wait_equals_remembered_cancellation() {
    // Rust: every wait is wait([op, CANCEL]); after the cancel the SDK re-issues
    // wait([op, CANCEL]) and the host resolves it with the cancellation op.
    let mut rust = J::new("rs_conf_cancel_wait", 4);
    let t = rust.timer();
    rust.wait_resolved(vec![]);
    rust.push(EventBody::CancelRequested(CancelRequested {
        reason: "user".into(),
    }));
    rust.wait_resolved(vec![CANCELLATION_OP]);
    rust.wait_resolved(vec![CANCELLATION_OP]);
    rust.failed(Failure::Cancelled);
    // Python/TS: the SDK remembers the cancellation; one wait, resolved once.
    let mut py = J::new("py_conf_cancel_wait", 5);
    let t2 = py.timer();
    py.push(EventBody::CancelRequested(CancelRequested {
        reason: "user".into(),
    }));
    py.wait_resolved(vec![CANCELLATION_OP]);
    py.failed(Failure::Cancelled);
    assert_ne!(rust.events.len(), py.events.len());
    let _ = (t, t2);
    compare::projections(
        &project(&rust.events),
        &project(&py.events),
        Opts::default(),
    )
    .expect("Rust and Python cancellation shapes must project equal");
}

#[test]
fn child_names_lose_language_prefix() {
    assert_eq!(projection::base_workflow("rs_leaf_echo"), "leaf_echo");
    assert_eq!(projection::base_workflow("py_leaf_echo"), "leaf_echo");
    assert_eq!(projection::base_workflow("ts_leaf_echo"), "leaf_echo");
    assert_eq!(projection::base_workflow("leaf_echo"), "leaf_echo");
}

#[test]
fn json_formatting_differences_are_ignored() {
    let mut a = J::new("rs_x", 1);
    a.step("s", r#"{"b":1,"a":2.0}"#);
    a.completed(r#"{"ok":true,"n":1}"#);
    let mut b = J::new("py_x", 2);
    b.step("s", r#"{ "a": 2, "b": 1 }"#);
    b.completed(r#"{"n": 1.0, "ok": true}"#);
    compare::projections(&project(&a.events), &project(&b.events), Opts::default())
        .expect("formatting is not semantic");
}

#[test]
fn inputs_compare_as_multiset() {
    // Two timers fired in opposite orders: same facts, same multiset of inputs.
    let mut a = J::new("rs_g", 1);
    let t1 = a.timer();
    let t2 = a.timer();
    a.fired(t1);
    a.fired(t2);
    let mut b = J::new("ts_g", 2);
    let u1 = b.timer();
    let u2 = b.timer();
    b.fired(u2);
    b.fired(u1);
    compare::projections(&project(&a.events), &project(&b.events), Opts::default())
        .expect("input order is a multiset");
}

// ---------------------------------------------------------------------------
// Rejections
// ---------------------------------------------------------------------------

fn expect_diff(a: &[Event], b: &[Event], needle: &str) {
    let err = compare::projections(&project(a), &project(b), Opts::default())
        .expect_err("comparator must flag this difference");
    let text = err.to_string();
    assert!(text.contains(needle), "diff text lacks {needle:?}:\n{text}");
}

#[test]
fn flags_renamed_step() {
    let a = reference(1, false);
    let mut b = reference(1, false);
    if let EventBody::StepCompleted(s) = &mut b[1].body {
        s.name = "a2".into();
    }
    expect_diff(&a, &b, "step:a2");
}

#[test]
fn flags_swapped_command_order() {
    let mut a = J::new("rs_o", 1);
    a.step("a", "1");
    a.step("b", "2");
    a.completed("null");
    let mut b = J::new("rs_o", 1);
    b.step("b", "2");
    b.step("a", "1");
    b.completed("null");
    expect_diff(&a.events, &b.events, "first difference at index 0");
}

#[test]
fn flags_different_outcome_json() {
    let mut a = J::new("rs_v", 1);
    a.step("a", r#"{"v":1}"#);
    a.completed(r#"{"r":1}"#);
    let mut b = J::new("rs_v", 1);
    b.step("a", r#"{"v":1}"#);
    b.completed(r#"{"r":"1"}"#);
    expect_diff(&a.events, &b.events, r#"terminal={"r":"1"}"#);
    // A different step outcome is flagged too.
    let mut c = J::new("rs_v", 1);
    c.step("a", r#"{"v":2}"#);
    c.completed(r#"{"r":1}"#);
    expect_diff(&a.events, &c.events, r#"step:a={"v":2}"#);
}

#[test]
fn flags_missing_terminal() {
    let a = reference(1, false);
    let b = &a[..a.len() - 1];
    expect_diff(&a, b, "terminal:completed");
}

#[test]
fn flags_different_inputs() {
    let mut a = J::new("rs_s", 1);
    let s = a.signal_wait("go", None);
    a.signal("go", s, r#"{"v":1}"#);
    let mut b = J::new("rs_s", 1);
    let s2 = b.signal_wait("go", None);
    b.signal("go", s2, r#"{"v":2}"#);
    expect_diff(&a.events, &b.events, "inputs");
}

#[test]
fn values_can_be_skipped_for_value_probes() {
    let mut a = J::new("rs_n", 1);
    a.step("observe", "123");
    a.completed("123");
    let mut b = J::new("py_n", 1);
    b.step("observe", "456");
    b.completed("456");
    assert!(
        compare::projections(&project(&a.events), &project(&b.events), Opts::default()).is_err()
    );
    compare::projections(
        &project(&a.events),
        &project(&b.events),
        Opts { values: false },
    )
    .expect("C-NATIVE-01 values are per language");
}

#[test]
fn spec_fact_list_diff_is_readable() {
    let expected: Vec<String> = ["step:a", "step:b", "terminal:completed"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let actual = facts(&reference(1, false));
    let err = compare::facts(&expected, &actual).expect_err("differs");
    let text = err.to_string();
    assert!(text.contains("projection differs"), "{text}");
    assert!(text.contains("expected step:b"), "{text}");
    assert!(text.contains("!="), "{text}");
}

#[test]
fn outcome_comparator() {
    compare::outcomes("result", &json!({"a": 1.0}), &json!({"a": 1})).expect("equal");
    let e = compare::outcomes("result", &json!({"a": 1}), &json!({"a": 2})).expect_err("differ");
    assert!(e.to_string().contains("result differs"));
}

// ---------------------------------------------------------------------------
// Strict per-language check (byte identity) and the chain projection
// ---------------------------------------------------------------------------

#[test]
fn journals_identical_is_byte_exact() {
    let a = reference(1, false);
    compare::journals_identical(&a, &reference(1, false)).expect("same build, same bytes");
    // A ts difference is invisible to the projection but not to the strict check.
    let mut b = reference(1, false);
    b[3].ts_nanos += 1;
    assert_eq!(project(&a), project(&b));
    let err = compare::journals_identical(&a, &b).expect_err("ts differs");
    assert!(err.to_string().contains("index 3"), "{err}");
    // A different length is reported.
    assert!(compare::journals_identical(&a, &a[..a.len() - 1]).is_err());
}

#[test]
fn chain_projection_concatenates_runs() {
    let mut first = J::new("rs_conf_can", 1);
    first.push(EventBody::RunContinuedAsNew(
        ostro_types::journal::RunContinuedAsNew {
            meta: first.meta(),
            continuation: ostro_types::journal::Continuation {
                input: json_payload(r#"{"n":0}"#),
                workflow: None,
                version: None,
            },
        },
    ));
    let mut second = J::new("rs_conf_can", 2);
    second.completed(r#"{"generation":1}"#);
    let p = projection::project_chain(&[first.events, second.events]);
    assert_eq!(
        p.facts,
        vec!["terminal:continued_as_new", "terminal:completed"]
    );
    let mut q = Projection::default();
    q.extend(p.clone());
    assert_eq!(q, p);
}

// ---------------------------------------------------------------------------
// Languages
// ---------------------------------------------------------------------------

#[test]
fn language_selection_and_names() {
    assert_eq!(langs::parse_list(None, false).unwrap(), vec![Lang::Rust]);
    assert_eq!(
        langs::parse_list(Some(""), false).unwrap(),
        vec![Lang::Rust]
    );
    assert_eq!(
        langs::parse_list(Some("rust, py,ts,ts-sm,rust"), true).unwrap(),
        vec![Lang::Rust, Lang::Py, Lang::Ts, Lang::TsSm]
    );
    let e = langs::parse_list(Some("rust,py"), false).unwrap_err();
    assert!(e.contains("--features polyglot"), "{e}");
    assert!(langs::parse_list(Some("go"), true).is_err());
    assert_eq!(
        langs::workflow_name(Lang::Rust, "conf_steps"),
        "rs_conf_steps"
    );
    assert_eq!(
        langs::workflow_name(Lang::Py, "conf_steps"),
        "py_conf_steps"
    );
    assert_eq!(
        langs::workflow_name(Lang::Ts, "conf_steps"),
        "ts_conf_steps"
    );
    assert_eq!(
        langs::workflow_name(Lang::TsSm, "conf_steps"),
        "ts_conf_steps"
    );
}

#[test]
fn missing_polyglot_guest_names_its_plan() {
    for (lang, plan) in [
        (Lang::Py, "09-14"),
        (Lang::Ts, "09-15"),
        (Lang::TsSm, "09-15"),
    ] {
        if langs::guest_dir(lang, Some("v9-missing")).exists() {
            continue;
        }
        let e = langs::module_for(lang, Some("v9-missing")).unwrap_err();
        assert!(e.contains(plan) && e.contains("does not exist"), "{e}");
    }
}
