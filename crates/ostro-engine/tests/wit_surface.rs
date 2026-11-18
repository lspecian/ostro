//! WIT 0.1 behaviour (plan 03-12): every `ostro:runtime@0.1.0` import and both exports
//! run on the host through the activation driver + FakeSink and replay with zero
//! appends; graceful cancel with a journaled cleanup step, continue-as-new and its
//! suggestion, await tokens, set-status, failure kinds, run timeouts / deadlines and
//! exact effect bytes (WIT-01..06, ENG-04).
//!
//! These run on the evict-and-replay path (every unsatisfied wait evicts); plan 03-11
//! re-runs them as a regression gate once hot wait lands.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, Rule, RunSpec, Script, drive_to_completion,
    replay_journal,
};
use ostro_types::journal::{
    CANCELLATION_OP, Continuation, EffectRequest, Event, EventBody, EventKind, Failure, Header,
    HttpResponse, OpOutput, Payload, SqlResult, SqlValue, decode_result,
};

const PARTITION: u16 = 7;
const RUN_ID: &str = "run-wit-1";
const TOKEN_KEY: [u8; 32] = [9; 32];
const DEADLINE_NANOS: u64 = DEFAULT_ACCEPTED_AT + 30_000_000_000;
const BYTES: &str = "application/octet-stream";

fn runtime_with(cfg: EngineConfig, token_key: [u8; 32]) -> Runtime {
    Runtime::new(
        cfg,
        RuntimeOptions {
            // Frozen clock: live timestamps depend only on the journal, so outputs are
            // independent of how many activations a drive takes.
            clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
            token_key,
        },
    )
    .expect("runtime")
}

struct Shared {
    rt: Runtime,
    prepared: Mutex<HashMap<String, Arc<Prepared>>>,
}

fn shared() -> &'static Shared {
    static S: OnceLock<Shared> = OnceLock::new();
    S.get_or_init(|| Shared {
        rt: runtime_with(EngineConfig::default(), TOKEN_KEY),
        prepared: Mutex::new(HashMap::new()),
    })
}

fn rt() -> &'static Runtime {
    &shared().rt
}

/// wit-surface prepared on the shared runtime.
fn surface_guest() -> Arc<Prepared> {
    let s = shared();
    let mut map = s.prepared.lock().expect("lock");
    map.entry("wit-surface".to_string())
        .or_insert_with(|| Arc::new(prepare_on(&s.rt)))
        .clone()
}

fn prepare_on(rt: &Runtime) -> Prepared {
    rt.prepare(&common::guest_wasm("wit-surface"))
        .unwrap_or_else(|e| panic!("prepare wit-surface: {e}"))
}

fn run_ref() -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    }
}

fn spec(workflow: &str, input: &[u8]) -> RunSpec {
    let mut s = RunSpec::new(run_ref(), workflow, input);
    s.deadline_nanos = Some(DEADLINE_NANOS);
    s
}

fn text(s: &str) -> Payload {
    Payload {
        content_type: "text/plain".to_string(),
        data: s.as_bytes().to_vec(),
    }
}

fn kinds(journal: &[Event]) -> Vec<EventKind> {
    journal.iter().map(|e| e.body.kind()).collect()
}

fn output_text(o: &ActivationOutcome) -> String {
    match o {
        ActivationOutcome::Completed { output } => String::from_utf8(output.data.clone())
            .unwrap_or_else(|_| panic!("non-utf8 output: {o:?}")),
        other => panic!("expected Completed, got {other:?}"),
    }
}

/// Replay `d.journal` on `rt`/`p`: same outcome, zero appends.
async fn assert_replays(rt: &Runtime, p: &Prepared, d: &Drive) {
    let (outcome, appended) = replay_journal(rt, p, run_ref(), d.journal.clone()).await;
    assert_eq!(outcome, d.outcome, "replay outcome must be identical");
    assert_eq!(appended, 0, "replay must append nothing");
}

/// Seq of the nth (0-based) event of `kind`.
fn seq_of(journal: &[Event], kind: EventKind, nth: usize) -> u64 {
    journal
        .iter()
        .filter(|e| e.body.kind() == kind)
        .nth(nth)
        .unwrap_or_else(|| panic!("no {kind:?} #{nth} in journal"))
        .seq
}

/// `key=value` fields of the dump line starting with `prefix `.
fn line_fields(out: &str, prefix: &str) -> HashMap<String, String> {
    let line = out
        .lines()
        .find(|l| l.starts_with(&format!("{prefix} ")))
        .unwrap_or_else(|| panic!("no `{prefix}` line in:\n{out}"));
    line[prefix.len() + 1..]
        .split(' ')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn line_value<'a>(out: &'a str, prefix: &str) -> &'a str {
    out.lines()
        .find_map(|l| l.strip_prefix(&format!("{prefix} ")))
        .unwrap_or_else(|| panic!("no `{prefix}` line in:\n{out}"))
}

fn full_effects_rule() -> Rule {
    Rule::CompleteEffects(Arc::new(|req: &EffectRequest| match req {
        EffectRequest::Http(_) => Ok(OpOutput::Http(HttpResponse {
            status: 201,
            headers: vec![Header {
                name: "x-a".to_string(),
                value: "1".to_string(),
            }],
            body: b"ok".to_vec(),
        })),
        EffectRequest::Sql(_) => Ok(OpOutput::Sql(SqlResult {
            columns: vec!["c".to_string()],
            rows: vec![
                vec![SqlValue::Int8(1)],
                vec![SqlValue::Text("a".to_string())],
            ],
            rows_affected: 2,
        })),
        EffectRequest::Object(_) => Ok(OpOutput::Object(b"obj".to_vec())),
    }))
}

fn full_script() -> Script {
    Script::new()
        .rule(full_effects_rule())
        .rule(Rule::CompleteChildren(Arc::new(
            |_wf: &str, _in: &Payload| Ok(OpOutput::Child(text("child-out"))),
        )))
        .rule(Rule::Signal {
            name: "go".to_string(),
            payload: text("sig"),
        })
        .rule(Rule::CompleteTokens(text("tok")))
        .fire_timers(true)
}

// ---------------------------------------------------------------------------
// Task 1: WIT-01 / WIT-02 / WIT-03
// ---------------------------------------------------------------------------

#[tokio::test]
async fn wit_surface_every_import() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("surface", b""), full_script()).await;
    let out = output_text(&d.outcome);

    // context
    assert_eq!(
        line_value(&out, "info"),
        format!("{RUN_ID} surface v1"),
        "{out}"
    );
    let now = line_value(&out, "now");
    assert_eq!(
        now,
        format!("{}.0", DEFAULT_ACCEPTED_AT / 1_000_000_000),
        "logical now before any command = accepted_at"
    );
    assert!(!line_value(&out, "random-u64").is_empty());
    assert_eq!(line_value(&out, "random-bytes").len(), 16, "8 bytes hex");
    let uuid = line_value(&out, "uuid-v7");
    assert_eq!(uuid.len(), 36, "{uuid}");
    assert_eq!(&uuid[14..15], "7", "uuid version nibble: {uuid}");
    // The dump is produced by the final activation; everything before the last
    // recorded command is replayed there, so is-replaying is true iff an earlier
    // activation recorded the commands (evict-and-replay). Live, it is false.
    assert_eq!(
        line_value(&out, "is-replaying"),
        if d.activations > 1 { "true" } else { "false" },
        "activations = {}",
        d.activations
    );
    assert_eq!(line_value(&out, "patched"), "true");
    assert_eq!(line_value(&out, "step"), "Ok(8)", "step-out is 8 bytes");

    // effects: op ids are the seqs of their scheduling events.
    let ops = line_fields(&out, "ops");
    let j = &d.journal;
    let op = |k: &str| -> u64 { ops[k].split(':').next().unwrap().parse().unwrap() };
    assert_eq!(op("http"), seq_of(j, EventKind::EffectScheduled, 0));
    assert_eq!(op("sql"), seq_of(j, EventKind::EffectScheduled, 1));
    assert_eq!(op("object"), seq_of(j, EventKind::EffectScheduled, 2));
    assert_eq!(op("sleep"), seq_of(j, EventKind::TimerStarted, 0));
    assert_eq!(op("until"), seq_of(j, EventKind::TimerStarted, 1));
    assert_eq!(op("signal"), seq_of(j, EventKind::SignalWaitStarted, 0));
    assert_eq!(op("child"), seq_of(j, EventKind::ChildStarted, 0));
    assert_eq!(op("token"), seq_of(j, EventKind::AwaitTokenCreated, 0));
    assert!(ops["token"].contains(":ot1_"), "{}", ops["token"]);

    // Every journaled command kind is present once (journal / effects / context).
    for k in [
        EventKind::PatchMarker,
        EventKind::StatusPublished,
        EventKind::StepCompleted,
        EventKind::SignalWaitStarted,
        EventKind::ChildStarted,
        EventKind::AwaitTokenCreated,
        EventKind::OpCancelled,
        EventKind::RunCompleted,
    ] {
        assert_eq!(
            kinds(j).iter().filter(|x| **x == k).count(),
            1,
            "{k:?} in {:?}",
            kinds(j)
        );
    }
    assert_eq!(
        kinds(j)
            .iter()
            .filter(|x| **x == EventKind::WaitResolved)
            .count(),
        2
    );

    // Effect completions from the scripted leader reach the guest.
    let http = op("http");
    assert!(
        out.contains(&format!("op {http}: http 201 [x-a=1] body-sha256 ")),
        "{out}"
    );
    assert!(out.contains(&format!("op {}: sql cols [c] rows 2 affected 2", op("sql"))));
    assert!(out.contains(&format!("op {}: object 3 bytes", op("object"))));
    assert!(out.contains(&format!("op {}: timer", op("sleep"))), "{out}");
    assert_eq!(d.latest_status.as_deref(), Some("{\"surface\":1}"));

    assert_replays(rt(), &p, &d).await;
}

#[tokio::test]
async fn wit_surface_describe_and_exports() {
    let p = surface_guest();
    let metas = rt().describe(&p).await.expect("describe");
    let names: Vec<&str> = metas.iter().map(|m| m.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "surface",
            "echo-bytes",
            "cancel-cleanup",
            "can",
            "token",
            "status",
            "non-retryable",
            "step-fail",
            "nested-step",
            "info",
            "panic",
            "exit",
        ]
    );
    for m in &metas {
        assert_eq!(m.version, 1);
        let want = (m.name == "surface").then_some(30_000);
        assert_eq!(m.run_timeout_ms, want, "{} run-timeout-ms (WIT-06)", m.name);
    }
}

#[tokio::test]
async fn cancel_cleanup_runs_after_cancel() {
    let p = surface_guest();
    let script = Script::new()
        .fire_timers(false)
        .rule(Rule::CancelAfterActivations(1));
    let d = drive_to_completion(rt(), &p, spec("cancel-cleanup", b""), script).await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Failed {
            failure: Failure::Cancelled
        }
    );
    assert_eq!(d.activations, 2, "evicted at the wait, then cancelled");

    let j = &d.journal;
    let cancel_seq = seq_of(j, EventKind::CancelRequested, 0);
    let wait = j
        .iter()
        .find(|e| e.body.kind() == EventKind::WaitResolved)
        .expect("WaitResolved");
    assert!(
        wait.seq > cancel_seq,
        "wait resolves after the cancel input"
    );
    let EventBody::WaitResolved(w) = &wait.body else {
        unreachable!()
    };
    assert_eq!(w.completed, vec![CANCELLATION_OP]);
    assert_eq!(CANCELLATION_OP, u64::MAX);

    let tail: Vec<&EventBody> = j[wait.seq as usize..].iter().map(|e| &e.body).collect();
    assert_eq!(tail.len(), 3, "{:?}", kinds(j));
    assert!(matches!(tail[0], EventBody::WaitResolved(_)));
    match tail[1] {
        EventBody::StepCompleted(s) => {
            assert_eq!(s.name, "cleanup");
            let r: Result<Payload, Failure> = decode_result(&s.outcome).expect("decode");
            assert_eq!(r, Ok(text("cleaned")));
        }
        other => panic!("expected cleanup step, got {other:?}"),
    }
    assert!(matches!(
        tail[2],
        EventBody::RunFailed(f) if f.failure == Failure::Cancelled
    ));
    assert_eq!(
        kinds(j),
        [
            EventKind::RunStarted,
            EventKind::TimerStarted,
            EventKind::CancelRequested,
            EventKind::WaitResolved,
            EventKind::StepCompleted,
            EventKind::RunFailed,
        ]
    );

    assert_replays(rt(), &p, &d).await;
}

#[tokio::test]
async fn cancel_without_request_completes() {
    let p = surface_guest();
    let d = drive_to_completion(
        rt(),
        &p,
        spec("cancel-cleanup", b""),
        Script::new().fire_timers(true),
    )
    .await;
    assert_eq!(output_text(&d.outcome), "slept");
    assert!(!kinds(&d.journal).contains(&EventKind::CancelRequested));
    let timer = seq_of(&d.journal, EventKind::TimerStarted, 0);
    let wait = d
        .journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::WaitResolved(w) => Some(w.completed.clone()),
            _ => None,
        })
        .expect("WaitResolved");
    assert_eq!(wait, vec![timer], "only the 1 h timer resolves the wait");
    assert_replays(rt(), &p, &d).await;
}

#[test]
fn terminate_is_not_guest_visible() {
    // Hard terminate is a host/API operation only (WIT-02): the guest-visible world
    // declares no `terminate` function; graceful cancel is the cancellation op.
    let dir = common::repo_root().join("wit/ostro-runtime-0.1.0");
    let mut files = 0;
    for entry in std::fs::read_dir(&dir).expect("wit dir") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|e| e == "wit") {
            files += 1;
            let src = std::fs::read_to_string(&path).expect("read wit");
            for (n, line) in src.lines().enumerate() {
                let code = line.split("//").next().unwrap_or("");
                assert!(
                    !code.contains("terminate"),
                    "{}:{} declares terminate: {line}",
                    path.display(),
                    n + 1
                );
            }
            assert!(
                src.contains("cancellation: func"),
                "cancellation op present"
            );
        }
    }
    assert!(files > 0, "no .wit files in {}", dir.display());
}

#[tokio::test]
async fn continue_as_new_outcome() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("can", &0u32.to_le_bytes()), Script::new()).await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::ContinuedAsNew {
            continuation: Continuation {
                input: Payload {
                    content_type: BYTES.to_string(),
                    data: 1u32.to_le_bytes().to_vec(),
                },
                workflow: None,
                version: None,
            }
        }
    );
    assert_eq!(
        d.journal.last().map(|e| e.body.kind()),
        Some(EventKind::RunContinuedAsNew)
    );
    assert_eq!(d.activations, 1);
    assert_replays(rt(), &p, &d).await;

    let d3 = drive_to_completion(rt(), &p, spec("can", &3u32.to_le_bytes()), Script::new()).await;
    assert_eq!(output_text(&d3.outcome), "counter=3 suggested=false");
    assert_eq!(
        d3.journal.last().map(|e| e.body.kind()),
        Some(EventKind::RunCompleted)
    );
}

#[tokio::test]
async fn continue_as_new_suggested_threshold() {
    // `can` checks the suggestion before journaling anything, i.e. with a 1-event
    // journal (RunStarted): suggested iff journal events >= threshold.
    let low = runtime_with(
        EngineConfig {
            continue_as_new_events: 1,
            ..EngineConfig::default()
        },
        TOKEN_KEY,
    );
    let lp = prepare_on(&low);
    let d = drive_to_completion(&low, &lp, spec("can", &3u32.to_le_bytes()), Script::new()).await;
    assert_eq!(output_text(&d.outcome), "counter=3 suggested=true");
    assert_replays(&low, &lp, &d).await;

    let two = runtime_with(
        EngineConfig {
            continue_as_new_events: 2,
            ..EngineConfig::default()
        },
        TOKEN_KEY,
    );
    let tp = prepare_on(&two);
    let d = drive_to_completion(&two, &tp, spec("can", &3u32.to_le_bytes()), Script::new()).await;
    assert_eq!(output_text(&d.outcome), "counter=3 suggested=false");

    assert_eq!(EngineConfig::default().continue_as_new_events, 10_000);
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("can", &3u32.to_le_bytes()), Script::new()).await;
    assert_eq!(output_text(&d.outcome), "counter=3 suggested=false");
    assert_replays(rt(), &p, &d).await;
}
