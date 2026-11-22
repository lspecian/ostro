//! First real-engine runs of SDK-built Python and TypeScript guests (plan 09-12 Task 3;
//! `--features polyglot`, needs uv 0.7.15 and Node 24).
//!
//! `conformance/fixtures/py-smoke` (componentize-py) and `conformance/fixtures/ts-smoke`
//! (jco QuickJS and StarlingMonkey) export the same workflows, built only with the SDKs.
//! Each workflow runs on `ostro_engine::runtime::Runtime` with the testkit `FakeSink` and
//! a frozen `FakeClock`; the testkit's `drive_to_completion` acts as the scheduler
//! (evict → commit scripted inputs → re-activate; timers fire on eviction, signals and
//! child completions come from script rules). For every workflow:
//!
//! - the outcome and the journal shape are asserted (step order, cancelled loser, timer
//!   due time, child start, saga compensation order and failure details, saga under run
//!   cancellation, effect records);
//! - a second run with the same seed and run id gives a byte-identical journal;
//! - replaying the final journal returns the same outcome and appends 0 events.
//!
//! The cross-language test checks that the host-drawn values (`random_f64`, `random_u64`,
//! `uuid7`, `idempotency_key`) and the effect requests are equal in all three builds.
#![cfg(feature = "polyglot")]

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use ostro_conformance::compare::journals_identical;
use ostro_conformance::polyglot::{Mode, build};
use ostro_conformance::repo_root;
use ostro_engine::config::{EngineConfig, LanguageClass};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, Rule, RunSpec, Script, drive_to_completion,
    replay_journal,
};
use ostro_types::journal::{
    EffectRequest, Event, EventBody, Failure, Header, HttpRequest, HttpResponse, ObjectRequest,
    OpOutput, Payload, SqlRequest, SqlResult, SqlValue,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

const RUN_ID: &str = "01890000-0000-7000-8000-000000000012";
const HOUR_NANOS: u64 = 3_600_000_000_000;

fn runtime(class: LanguageClass) -> Runtime {
    Runtime::new(
        EngineConfig {
            class,
            // No hot waits: every unsatisfied wait evicts and the testkit schedules.
            hot_wait: Duration::ZERO,
            ..EngineConfig::default()
        },
        RuntimeOptions {
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
            token_key: [12; 32],
            ..RuntimeOptions::default()
        },
    )
    .expect("runtime")
}

fn rt(mode: Mode) -> &'static Runtime {
    static PY: OnceLock<Runtime> = OnceLock::new();
    static TS: OnceLock<Runtime> = OnceLock::new();
    match mode {
        Mode::Py => PY.get_or_init(|| runtime(LanguageClass::Python)),
        Mode::Ts | Mode::TsSm => TS.get_or_init(|| runtime(LanguageClass::TypeScript)),
    }
}

fn fixture(mode: Mode) -> &'static str {
    match mode {
        Mode::Py => "py-smoke",
        Mode::Ts | Mode::TsSm => "ts-smoke",
    }
}

async fn prepared(mode: Mode) -> Prepared {
    let dir = repo_root().join("conformance/fixtures").join(fixture(mode));
    let t = Instant::now();
    let path = build(mode, &dir).unwrap_or_else(|e| panic!("build {}: {e}", mode.arg()));
    let wasm = std::fs::read(&path).expect("read component");
    let built = t.elapsed();
    let t = Instant::now();
    let p = rt(mode)
        .prepare(&wasm)
        .unwrap_or_else(|e| panic!("{}: deploy validation: {e}", mode.arg()));
    println!(
        "[{}] build/cache {:?}, prepare {:?}, {} bytes",
        mode.arg(),
        built,
        t.elapsed(),
        wasm.len()
    );
    p
}

fn json_payload(v: &Value) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: serde_json::to_vec(v).expect("json"),
    }
}

fn payload_json(p: &Payload) -> Value {
    if p.data.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&p.data).unwrap_or_else(|e| {
        panic!(
            "payload not JSON ({e}): {:?}",
            String::from_utf8_lossy(&p.data)
        )
    })
}

fn idempotency_key_for(run_id: &str, name: &str) -> String {
    let mut h = Sha256::new();
    h.update(b"ostro/idem/v1/name");
    h.update(run_id.as_bytes());
    h.update([0u8]);
    h.update(name.as_bytes());
    let d = h.finalize();
    let hex: String = d[..16].iter().map(|b| format!("{b:02x}")).collect();
    format!("ostro-{hex}")
}

fn spec(workflow: &str) -> RunSpec {
    let mut s = RunSpec::new(
        RunRef {
            partition: 1,
            run_id: RUN_ID.to_string(),
        },
        workflow,
        b"",
    );
    s.input = json_payload(&Value::Null);
    s
}

fn effect_output(req: &EffectRequest) -> Result<OpOutput, Failure> {
    Ok(match req {
        EffectRequest::Http(_) => OpOutput::Http(HttpResponse {
            status: 201,
            headers: vec![Header {
                name: "x-req".into(),
                value: "1".into(),
            }],
            body: br#"{"ok":true}"#.to_vec(),
        }),
        EffectRequest::Sql(_) => OpOutput::Sql(SqlResult {
            columns: vec!["n".into()],
            rows: vec![vec![SqlValue::Int8(2)]],
            rows_affected: 1,
        }),
        EffectRequest::Object(_) => OpOutput::Object(br#"{"e_tag":"\"e1\""}"#.to_vec()),
    })
}

fn script(workflow: &str) -> Script {
    match workflow {
        "smoke_race" => Script::new().fire_timers(false).rule(Rule::Signal {
            name: "go".into(),
            payload: json_payload(&json!({"n": 1})),
        }),
        "smoke_child" => Script::new().rule(Rule::CompleteChildren(Arc::new(|wf, input| {
            assert_eq!(wf, "leaf_echo");
            Ok(OpOutput::Child(json_payload(
                &json!({"echo": payload_json(input)}),
            )))
        }))),
        // The 1 h timer never fires; the run is cancelled after the first activation.
        "smoke_saga_cancel" => Script::new()
            .fire_timers(false)
            .rule(Rule::CancelAfterActivations(1)),
        "smoke_effects" => Script::new().rule(Rule::CompleteEffects(Arc::new(effect_output))),
        _ => Script::new(),
    }
}

/// Runs `workflow` twice (byte-identical journals) and replays the result (0 appended).
async fn run(mode: Mode, p: &Prepared, workflow: &str) -> Drive {
    let rt = rt(mode);
    let what = format!("{} {workflow}", mode.arg());
    let t = Instant::now();
    let d = drive_to_completion(rt, p, spec(workflow), script(workflow)).await;
    let first = t.elapsed();
    if let ActivationOutcome::Evicted { .. } | ActivationOutcome::AttemptFailed(_) = &d.outcome {
        panic!(
            "{what}: did not finish: {:?}\noutcomes {:?}\njournal {:#?}",
            d.outcome, d.outcomes, d.journal
        );
    }
    let again = drive_to_completion(rt, p, spec(workflow), script(workflow)).await;
    assert_eq!(again.outcome, d.outcome, "{what}: second run outcome");
    journals_identical(&d.journal, &again.journal)
        .unwrap_or_else(|diff| panic!("{what}: second run journal differs: {diff}"));
    let t = Instant::now();
    let (outcome, appended) = replay_journal(rt, p, spec(workflow).run, d.journal.clone()).await;
    let replay = t.elapsed();
    assert_eq!(
        outcome, d.outcome,
        "{what}: replay outcome must be identical"
    );
    assert_eq!(appended, 0, "{what}: replay must append nothing");
    println!(
        "[{}] {workflow}: {} activations, {} events, first run {first:?}, replay {replay:?}",
        mode.arg(),
        d.activations,
        d.journal.len()
    );
    d
}

fn completed(d: &Drive, what: &str) -> Value {
    match &d.outcome {
        ActivationOutcome::Completed { output } => payload_json(output),
        other => panic!(
            "{what}: expected Completed, got {other:?}\njournal {:#?}",
            d.journal
        ),
    }
}

fn step_names(journal: &[Event]) -> Vec<&str> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.as_str()),
            EventBody::StepAttemptFailed(s) => Some(s.name.as_str()),
            _ => None,
        })
        .collect()
}

fn effect_requests(journal: &[Event]) -> Vec<EffectRequest> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::EffectScheduled(s) => Some(s.request.clone()),
            _ => None,
        })
        .collect()
}

async fn smoke(mode: Mode) {
    let p = prepared(mode).await;
    let lang = mode.arg();
    let t = Instant::now();

    // Steps and gather/all: serialised in call order.
    let d = run(mode, &p, "smoke_steps").await;
    assert_eq!(completed(&d, lang), json!(["a", "b", "c", "d"]), "{lang}");
    assert_eq!(step_names(&d.journal), ["a", "b", "c", "d"], "{lang}");

    // Race with an explicit cancel of the losing timer.
    let d = run(mode, &p, "smoke_race").await;
    assert_eq!(
        completed(&d, lang),
        json!({"winner": "signal", "payload": {"n": 1}, "loser_cancelled": true}),
        "{lang}"
    );
    let timer_op = d
        .journal
        .iter()
        .find_map(|e| matches!(e.body, EventBody::TimerStarted(_)).then_some(e.seq))
        .expect("timer started");
    assert!(
        d.journal
            .iter()
            .any(|e| matches!(&e.body, EventBody::OpCancelled(c) if c.op == timer_op)),
        "{lang}: the losing timer must be cancelled: {:#?}",
        d.journal
    );

    // Durable sleep of 1 h of workflow time.
    let d = run(mode, &p, "smoke_sleep").await;
    assert_eq!(completed(&d, lang), json!({"slept": true}), "{lang}");
    let fire_at: Vec<u64> = d
        .journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::TimerStarted(t) => Some(t.fire_at_nanos),
            _ => None,
        })
        .collect();
    assert_eq!(fire_at, [DEFAULT_ACCEPTED_AT + HOUR_NANOS], "{lang}");

    // Child start journaled; the testkit feeds the ChildCompleted input.
    let d = run(mode, &p, "smoke_child").await;
    assert_eq!(
        completed(&d, lang),
        json!({"child": {"echo": {"x": 1}}}),
        "{lang}"
    );
    let child = d
        .journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::ChildStarted(c) => Some(c),
            _ => None,
        })
        .expect("child started");
    assert_eq!(child.workflow, "leaf_echo", "{lang}");
    assert_eq!(payload_json(&child.input), json!({"x": 1}), "{lang}");
    assert!(!child.options.detached, "{lang}");

    // Saga: compensations reversed, failure details attached.
    let d = run(mode, &p, "smoke_saga").await;
    assert_eq!(
        step_names(&d.journal),
        ["book_a", "book_b", "book_c", "cancel_b", "cancel_a"],
        "{lang}"
    );
    match &d.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(info),
        } => {
            assert_eq!(info.kind, "book_c_failed", "{lang}");
            assert_eq!(info.message, "no rooms left", "{lang}");
            let details = info.details.as_ref().expect("saga details");
            assert_eq!(
                payload_json(details),
                json!({"compensated": ["cancel_b", "cancel_a"], "compensation_failures": []}),
                "{lang}"
            );
        }
        other => panic!("{lang} smoke_saga: expected a non-retryable failure, got {other:?}"),
    }

    // Saga under run cancellation: compensations still run, the run ends cancelled.
    let d = run(mode, &p, "smoke_saga_cancel").await;
    assert_eq!(
        step_names(&d.journal),
        ["book_a", "book_b", "cancel_b", "cancel_a"],
        "{lang}"
    );
    assert!(
        d.journal
            .iter()
            .any(|e| matches!(e.body, EventBody::CancelRequested(_))),
        "{lang}"
    );
    assert_eq!(
        d.outcome,
        ActivationOutcome::Failed {
            failure: Failure::Cancelled
        },
        "{lang}"
    );

    // Randomness and time are replay-stable (checked by run()); the key is P-11.
    let d = run(mode, &p, "smoke_rand").await;
    let v = completed(&d, lang);
    assert_eq!(v["key"], json!(idempotency_key_for(RUN_ID, "x")), "{lang}");
    let f = v["f64"].as_f64().expect("f64");
    assert!((0.0..1.0).contains(&f), "{lang}: {f}");
    let native = v["native"].as_f64().expect("native");
    assert!((0.0..1.0).contains(&native), "{lang}: {native}");
    assert_eq!(
        v["time_ms"],
        json!(DEFAULT_ACCEPTED_AT / 1_000_000),
        "{lang}: native time is logical"
    );

    // print / console.log go to the log: no journal entry, no trap.
    let d = run(mode, &p, "smoke_print").await;
    assert_eq!(completed(&d, lang), json!({"printed": true}), "{lang}");
    assert_eq!(d.journal.len(), 2, "{lang}: {:#?}", d.journal);

    // Effect builders on the real host: records and decoded responses.
    let d = run(mode, &p, "smoke_effects").await;
    assert_eq!(
        completed(&d, lang),
        json!({"status": 201, "body": "{\"ok\":true}", "rows_affected": 1, "columns": ["n"], "e_tag": "\"e1\""}),
        "{lang}"
    );
    assert_eq!(
        effect_requests(&d.journal),
        expected_effects(),
        "{lang}: effect requests"
    );

    println!("[{lang}] all smoke workflows: {:?}", t.elapsed());
}

fn expected_effects() -> Vec<EffectRequest> {
    vec![
        EffectRequest::Http(HttpRequest {
            method: "POST".into(),
            url: "https://api.example.com/v1/charges".into(),
            headers: vec![
                // W3: the redacted header is journaled as "[redacted]".
                Header {
                    name: "Authorization".into(),
                    value: "[redacted]".into(),
                },
                Header {
                    name: "content-type".into(),
                    value: "application/json".into(),
                },
            ],
            body: Some(br#"{"amount":100}"#.to_vec()),
            timeout_ms: Some(10_000),
            idempotency_key: Some(idempotency_key_for(RUN_ID, "charge")),
            redact: vec!["Authorization".into()],
        }),
        EffectRequest::Sql(SqlRequest {
            connection: "db".into(),
            statement: "UPDATE c SET n = n + $1 RETURNING n".into(),
            params: vec![
                SqlValue::Int8(1),
                SqlValue::Text("t".into()),
                SqlValue::Null,
                SqlValue::Float8(1.5),
            ],
            exactly_once: true,
        }),
        EffectRequest::Object(ObjectRequest {
            connection: "files".into(),
            op: "put".into(),
            key: "k".into(),
            body: Some(b"body".to_vec()),
        }),
    ]
}

#[tokio::test]
async fn python_sdk_guest_runs_on_the_engine() {
    smoke(Mode::Py).await;
}

#[tokio::test]
async fn ts_quickjs_sdk_guest_runs_on_the_engine() {
    smoke(Mode::Ts).await;
}

#[tokio::test]
async fn ts_starlingmonkey_sdk_guest_runs_on_the_engine() {
    smoke(Mode::TsSm).await;
}

/// Host-drawn values are identical across the three builds for the same seed and run id.
#[tokio::test]
async fn host_values_agree_across_languages() {
    let mut values = Vec::new();
    for mode in [Mode::Py, Mode::Ts, Mode::TsSm] {
        let p = prepared(mode).await;
        let d = drive_to_completion(rt(mode), &p, spec("smoke_rand"), Script::new()).await;
        let v = completed(&d, mode.arg());
        values.push((
            mode.arg(),
            json!({"f64": v["f64"], "u64": v["u64"], "uuid": v["uuid"], "key": v["key"]}),
        ));
    }
    for (lang, v) in &values[1..] {
        assert_eq!(v, &values[0].1, "{lang} vs {}", values[0].0);
    }
}
