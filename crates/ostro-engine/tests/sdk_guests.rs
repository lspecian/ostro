//! Rust SDK guests on the engine (plan 04-06): every `guests/sdk-*` component (built
//! with plain cargo for wasm32-wasip2 against `sdks/rust/ostro-sdk`) passes the production
//! linker validation (`Runtime::prepare_cached`), `describe()` reports its stable
//! (name, version) list, and the workflows run to completion on the testkit with typed
//! JSON output. `sdk-native` (std time / HashMap / rand, WF-07) replays byte-identically.
//!
//! Retry behaviour of `sdk-retry` is exercised end to end by the server tests (04-07);
//! here it only has to validate, describe and run its non-retrying modes. Its
//! `long_backoff` (exactly one requeued failure under the D22 retry clock) and
//! `recover_hot` modes run on the engine testkit in `step_retry_clock.rs` (05-23).
//!
//! D11 (05-06): SDK waits now include the cancellation op (`wait([op, CANCELLATION],
//! any)`, and `join!` waits `any` in rounds), which changes every SDK wait's command hash
//! and can add the cancellation op to `WaitResolved.completed`. No assertion here depends
//! on the wait op list; the counts below (steps, timers) are unchanged. Cancellation
//! behaviour itself is proven in `sdk_cancellation.rs` with the `sdk-cancel` guest.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, RunSpec, Script, drive_to_completion, replay_journal,
};
use ostro_types::journal::{ErrorInfo, EventKind, Failure};

const PARTITION: u16 = 3;
const RUN_ID: &str = "run-sdk-1";

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                // Frozen clock: journals depend only on inputs, not activation count.
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [4; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

/// `guest` prepared once (production linker validation + L2 cache).
async fn guest(name: &str) -> Arc<Prepared> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<Prepared>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(p) = cache.lock().expect("lock").get(name) {
        return p.clone();
    }
    let p = rt()
        .prepare_cached(&common::guest_wasm(name))
        .await
        .unwrap_or_else(|e| panic!("prepare {name}: {e}"));
    cache
        .lock()
        .expect("lock")
        .insert(name.to_string(), p.clone());
    p
}

fn spec(workflow: &str, input: &str) -> RunSpec {
    RunSpec::new(
        RunRef {
            partition: PARTITION,
            run_id: RUN_ID.to_string(),
        },
        workflow,
        input.as_bytes(),
    )
}

fn output(d: &Drive) -> String {
    match &d.outcome {
        ActivationOutcome::Completed { output } => {
            String::from_utf8(output.data.clone()).expect("utf8 output")
        }
        other => panic!(
            "expected Completed, got {other:?} (journal: {:?})",
            d.journal
        ),
    }
}

fn count(d: &Drive, kind: EventKind) -> usize {
    d.journal.iter().filter(|e| e.body.kind() == kind).count()
}

/// Replaying the recorded journal yields the same outcome and appends nothing.
async fn assert_replays(p: &Prepared, d: &Drive) {
    let run = RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    };
    let (outcome, appended) = replay_journal(rt(), p, run, d.journal.clone()).await;
    assert_eq!(outcome, d.outcome, "replay outcome must be identical");
    assert_eq!(appended, 0, "replay must append nothing");
}

#[tokio::test]
async fn sdk_guests_validate_and_describe() {
    let expected: [(&str, &[(&str, u32)]); 5] = [
        ("sdk-basic", &[("sdk-basic", 1), ("sdk-echo", 1)]),
        ("sdk-basic-v2", &[("sdk-basic", 2)]),
        ("sdk-retry", &[("sdk-retry", 1)]),
        ("sdk-native", &[("sdk-native", 1)]),
        ("sdk-cancel", &[("sdk-cancel", 1)]),
    ];
    for (module, want) in expected {
        let p = guest(module).await;
        let metas = rt().describe(&p).await.expect("describe");
        let got: Vec<(&str, u32)> = metas.iter().map(|m| (m.name.as_str(), m.version)).collect();
        assert_eq!(got, want, "{module} describe()");
        for m in &metas {
            assert_eq!((m.default_queue.as_deref(), m.run_timeout_ms), (None, None));
        }
    }
}

#[tokio::test]
async fn sdk_basic_completes_with_typed_json() {
    let p = guest("sdk-basic").await;
    let d = drive_to_completion(
        rt(),
        &p,
        spec("sdk-basic", r#"{"name":"ostro","n":10}"#),
        Script::new(),
    )
    .await;
    assert_eq!(
        output(&d),
        r#"{"greeting":"hello, ostro","sum":55,"steps":2}"#
    );
    assert_eq!(d.latest_status.as_deref(), Some(r#"{"stage":"done"}"#));
    assert_eq!(count(&d, EventKind::StepCompleted), 2, "greet + add");
    // join! over two sleeps + select! over two more.
    assert_eq!(count(&d, EventKind::TimerStarted), 4);
    assert_replays(&p, &d).await;
}

#[tokio::test]
async fn sdk_echo_dispatches_by_name() {
    let p = guest("sdk-basic").await;
    let d = drive_to_completion(
        rt(),
        &p,
        spec("sdk-echo", r#"{"a":[1,2],"b":"x"}"#),
        Script::new(),
    )
    .await;
    assert_eq!(output(&d), r#"{"a":[1,2],"b":"x"}"#);
    assert_eq!(d.activations, 1);
}

#[tokio::test]
async fn sdk_basic_v2_reports_version() {
    let p = guest("sdk-basic-v2").await;
    let d = drive_to_completion(
        rt(),
        &p,
        spec("sdk-basic", r#"{"name":"v2","n":3}"#),
        Script::new(),
    )
    .await;
    assert_eq!(
        output(&d),
        r#"{"greeting":"hello, v2","sum":6,"steps":2,"version":2}"#
    );
}

#[tokio::test]
async fn sdk_basic_bad_input_is_non_retryable_invalid_json() {
    let p = guest("sdk-basic").await;
    let d = drive_to_completion(rt(), &p, spec("sdk-basic", r#"{"name":1}"#), Script::new()).await;
    match d.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(ErrorInfo { kind, .. }),
        } => assert_eq!(kind, "invalid_json"),
        other => panic!("expected invalid_json failure, got {other:?}"),
    }
}

#[tokio::test]
async fn sdk_retry_non_retryable_step_fails_run_after_one_attempt() {
    let p = guest("sdk-retry").await;
    let d = drive_to_completion(
        rt(),
        &p,
        spec("sdk-retry", r#"{"mode":"non_retryable"}"#),
        Script::new(),
    )
    .await;
    match &d.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(ErrorInfo { kind, .. }),
        } => assert_eq!(kind, "fatal"),
        other => panic!("expected fatal failure, got {other:?}"),
    }
    // `before` + `fatal`, one completion each.
    assert_eq!(count(&d, EventKind::StepCompleted), 2);
    assert_replays(&p, &d).await;

    let d = drive_to_completion(
        rt(),
        &p,
        spec("sdk-retry", r#"{"mode":"nope"}"#),
        Script::new(),
    )
    .await;
    match &d.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(ErrorInfo { kind, .. }),
        } => assert_eq!(kind, "bad_mode"),
        other => panic!("expected bad_mode failure, got {other:?}"),
    }
}

#[tokio::test]
async fn sdk_native_std_nondeterminism_replays_identically() {
    let p = guest("sdk-native").await;
    let d = drive_to_completion(rt(), &p, spec("sdk-native", ""), Script::new()).await;
    let out = output(&d);
    assert!(out.starts_with(r#"{"outside":{"time_nanos":"#), "{out}");
    assert!(out.contains(r#""inside":{"time_nanos":"#), "{out}");
    for k in ["\"k0\"", "\"k49\""] {
        assert_eq!(out.matches(k).count(), 2, "{k} in both maps: {out}");
    }
    assert_eq!(count(&d, EventKind::StepCompleted), 1);

    // Replay from the recorded journal: identical output bytes, nothing appended.
    assert_replays(&p, &d).await;

    // An independent run with the same RunStarted (seed, accepted_at) observes the same
    // values: std time / HashMap seeds / rand come only from the journaled seed.
    let again = drive_to_completion(rt(), &p, spec("sdk-native", ""), Script::new()).await;
    assert_eq!(output(&again), out);
}
