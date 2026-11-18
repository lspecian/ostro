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
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::engine::configure_store;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::host::bindings::Workflow;
use ostro_engine::host::bindings::ostro::runtime::types as wit_types;
use ostro_engine::host::state::{HostInit, HostState};
use ostro_engine::replay::token;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{
    DEFAULT_ACCEPTED_AT, Drive, FakeClock, FakeSink, Rule, RunSpec, Script, drive_to_completion,
    replay_journal,
};
use ostro_types::journal::{
    CANCELLATION_OP, Continuation, EffectRequest, Event, EventBody, EventKind, Failure, Header,
    HttpResponse, OpOutput, Payload, SqlResult, SqlValue, TokenCompleted, decode_result,
};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use wasmtime::Store;
use wasmtime::component::Component;

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
    script_with(full_effects_rule())
}

/// Children, the `go` signal, tokens and timers all completed; effects by `effects`.
fn script_with(effects: Rule) -> Script {
    Script::new()
        .rule(effects)
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

// ---------------------------------------------------------------------------
// Task 2: WIT-04 / WIT-05 / WIT-06 + exact effect bytes (ENG-04)
// ---------------------------------------------------------------------------

fn token_event(journal: &[Event]) -> (u64, String) {
    journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::AwaitTokenCreated(t) => Some((e.seq, t.token.clone())),
            _ => None,
        })
        .expect("AwaitTokenCreated")
}

/// An external caller holding only the token string: everything it needs (partition,
/// run, op) comes out of the token; it never sees a run id from anywhere else.
fn complete_by_token(sink: &FakeSink, tok: &str, payload: Payload) {
    assert!(token::verify(tok, &TOKEN_KEY), "caller verifies the token");
    let parts = token::parse(tok).expect("parse token");
    let run = RunRef {
        partition: parts.partition,
        run_id: parts.run_id,
    };
    let journal = sink.journal(&run);
    assert!(!journal.is_empty(), "token routes to an existing run");
    let next = journal.len() as u64;
    let ts = journal.last().map_or(0, |e| e.ts_nanos);
    sink.commit_inputs(
        &run,
        vec![Event {
            seq: next,
            ts_nanos: ts,
            body: EventBody::TokenCompleted(TokenCompleted {
                op: parts.op,
                payload,
            }),
        }],
    );
}

async fn activate_once(rt: &Runtime, p: &Prepared, sink: &Arc<FakeSink>) -> ActivationOutcome {
    let run = run_ref();
    rt.activate(
        p,
        Activate {
            run: run.clone(),
            attempt: 1,
            module_sha: p.sha,
            journal: sink.journal(&run),
            inputs: None,
            cancel: CancellationToken::new(),
        },
        sink.clone(),
    )
    .await
}

#[tokio::test]
async fn await_token_roundtrip() {
    let p = surface_guest();

    // Scripted leader completing every token.
    let d = drive_to_completion(
        rt(),
        &p,
        spec("token", b""),
        Script::new().rule(Rule::CompleteTokens(text("hello"))),
    )
    .await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Completed {
            output: text("hello")
        }
    );
    let (seq, tok) = token_event(&d.journal);
    assert!(tok.starts_with("ot1_"), "{tok}");
    let parts = token::parse(&tok).expect("parse");
    assert_eq!(parts.partition, PARTITION);
    assert_eq!(parts.run_id, RUN_ID);
    assert_eq!(parts.op, seq, "token embeds the op = AwaitTokenCreated seq");
    assert!(token::verify(&tok, &TOKEN_KEY));
    assert!(!token::verify(&tok, &[8; 32]));
    assert!(token::parse("ot1_not-a-token").is_none());
    assert_replays(rt(), &p, &d).await;

    // A caller holding only the token completes the run.
    let sink = Arc::new(FakeSink::new());
    let s = spec("token", b"");
    sink.seed(run_ref(), vec![s.run_started_event(&p.module_hash_hex)], 1);
    assert_eq!(
        activate_once(rt(), &p, &sink).await,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    let (_, tok2) = token_event(&sink.journal(&run_ref()));
    assert_eq!(
        tok2, tok,
        "token is deterministic for (partition, run, op, key)"
    );
    complete_by_token(&sink, &tok2, text("hello"));
    assert_eq!(
        activate_once(rt(), &p, &sink).await,
        ActivationOutcome::Completed {
            output: text("hello")
        }
    );
}

#[tokio::test]
async fn await_token_replay_without_key() {
    let p = surface_guest();
    let d = drive_to_completion(
        rt(),
        &p,
        spec("token", b""),
        Script::new().rule(Rule::CompleteTokens(text("hello"))),
    )
    .await;
    let (_, tok) = token_event(&d.journal);

    // Different node key: replay returns the recorded token, never re-mints.
    let other = runtime_with(EngineConfig::default(), [1; 32]);
    let op = prepare_on(&other);
    let sink = Arc::new(FakeSink::new());
    sink.seed(run_ref(), d.journal.clone(), 1);
    let outcome = activate_once(&other, &op, &sink).await;
    assert_eq!(outcome, d.outcome);
    assert_eq!(sink.appended_events(&run_ref()), 0);
    let (_, replayed_tok) = token_event(&sink.journal(&run_ref()));
    assert_eq!(replayed_tok, tok);
    assert_replays(&other, &op, &d).await;

    // A fresh run on the other key mints a different token for the same
    // (partition, run, op), so the identical string above is the recorded one.
    let d2 = drive_to_completion(
        &other,
        &op,
        spec("token", b""),
        Script::new().rule(Rule::CompleteTokens(text("hello"))),
    )
    .await;
    let (_, tok_other) = token_event(&d2.journal);
    assert_ne!(tok_other, tok, "key changes the MAC");
    assert_eq!(
        token::parse(&tok_other).expect("parse"),
        token::parse(&tok).expect("parse")
    );
}

#[tokio::test]
async fn set_status_journaled_once() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("status", b"a"), Script::new()).await;
    assert_eq!(output_text(&d.outcome), "ok");
    assert_eq!(d.latest_status.as_deref(), Some("a!"));
    let statuses: Vec<&str> = d
        .journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StatusPublished(s) => Some(s.json.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(statuses, ["a", "a!"], "one event per set-status call");
    assert_replays(rt(), &p, &d).await;
}

/// Replay `journal` directly on a [`HostState`] (production linker) and return the
/// status the host would publish (`HostState::latest_status`).
async fn replay_status(journal: Vec<Event>) -> (Option<String>, usize) {
    let r = rt();
    let comp =
        Component::new(r.engine().engine(), common::guest_wasm("wit-surface")).expect("component");
    let (workflow, input) = match &journal[0].body {
        EventBody::RunStarted(rs) => (rs.workflow.clone(), rs.input.clone()),
        _ => panic!("journal[0] must be RunStarted"),
    };
    let state = HostState::new(HostInit {
        run: run_ref(),
        attempt: 1,
        module_hash_hex: surface_guest().module_hash_hex.clone(),
        journal,
        clock: r.clock().clone(),
        limiter: r.engine().new_limiter(),
        wall_deadline: Instant::now() + Duration::from_secs(60),
        token_key: [1; 32],
        continue_as_new_events: r.engine().config().continue_as_new_events,
        sink: None,
    })
    .expect("host state");
    let mut store = Store::new(r.engine().engine(), state);
    configure_store(&mut store, r.engine().config().activation_fuel).expect("store");
    let wf = Workflow::instantiate_async(&mut store, &comp, r.linker())
        .await
        .expect("instantiate");
    let out = wf
        .ostro_runtime_workflows()
        .call_run(
            &mut store,
            &workflow,
            &wit_types::Payload {
                content_type: input.content_type,
                data: input.data,
            },
        )
        .await
        .expect("replay must not trap");
    assert!(out.is_ok(), "{out:?}");
    let pending = store.data_mut().take_pending().len();
    (store.data().latest_status.clone(), pending)
}

#[tokio::test]
async fn set_status_changed_text_no_divergence() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("status", b"a"), Script::new()).await;
    let mut journal = d.journal.clone();
    let mut edits = ["x", "y"].into_iter();
    for e in &mut journal {
        if let EventBody::StatusPublished(s) = &mut e.body {
            s.json = edits.next().expect("two statuses").to_string();
        }
    }
    assert!(edits.next().is_none(), "both statuses edited");

    // Kind-only hash: the changed text never diverges.
    let (outcome, appended) = replay_journal(rt(), &p, run_ref(), journal.clone()).await;
    assert_eq!(outcome, ActivationOutcome::Completed { output: text("ok") });
    assert_eq!(appended, 0);

    // The status the host publishes comes from the journal, not the guest's text.
    let (status, pending) = replay_status(journal).await;
    assert_eq!(status.as_deref(), Some("y"));
    assert_eq!(pending, 0, "replay records nothing new");
}

#[tokio::test]
async fn non_retryable_failure() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("non-retryable", b""), Script::new()).await;
    match &d.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(e),
        } => {
            assert_eq!(e.kind, "bad-input");
            assert_eq!(e.message, "input rejected");
        }
        other => panic!("expected Failed(NonRetryable), got {other:?}"),
    }
    match d.journal.last().map(|e| &e.body) {
        Some(EventBody::RunFailed(f)) => {
            assert!(matches!(&f.failure, Failure::NonRetryable(e) if e.kind == "bad-input"));
        }
        other => panic!("journal must end RunFailed, got {other:?}"),
    }
    assert_replays(rt(), &p, &d).await;
}

#[tokio::test]
async fn step_failure_recorded() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("step-fail", b""), Script::new()).await;
    let step = d
        .journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.clone()),
            _ => None,
        })
        .expect("StepCompleted");
    assert_eq!(step.name, "flaky");
    let r: Result<Payload, Failure> = decode_result(&step.outcome).expect("decode");
    assert!(
        matches!(&r, Err(Failure::Retryable(e)) if e.kind == "flaky"),
        "{r:?}"
    );
    assert!(
        matches!(
            &d.outcome,
            ActivationOutcome::Failed { failure: Failure::Retryable(e) } if e.kind == "flaky"
        ),
        "{:?}",
        d.outcome
    );
    // Replay hands the recorded failure back to the guest (it never re-runs the body).
    assert_replays(rt(), &p, &d).await;
}

#[tokio::test]
async fn run_info_deadline() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("info", b""), Script::new()).await;
    let out = output_text(&d.outcome);
    let f = line_fields(&format!("info {out}"), "info");
    assert_eq!(f["run-id"], RUN_ID);
    assert_eq!(f["workflow"], "info");
    assert_eq!(f["version"], "1");
    assert_eq!(f["attempt"], "1");
    assert_eq!(f["module-hash"], p.module_hash_hex);
    assert_eq!(
        f["deadline"],
        format!(
            "{}.{}",
            DEADLINE_NANOS / 1_000_000_000,
            DEADLINE_NANOS % 1_000_000_000
        ),
        "deadline = accepted_at + 30 s"
    );
    assert_eq!(f["tenant"], "None");
    assert_eq!(f["continued-from"], "None");
    // The run is partition-qualified by its RunRef; the token carries both.
    assert_eq!(run_ref().partition, PARTITION);
    assert_replays(rt(), &p, &d).await;

    let mut no_deadline = spec("info", b"");
    no_deadline.deadline_nanos = None;
    let d = drive_to_completion(rt(), &p, no_deadline, Script::new()).await;
    assert!(output_text(&d.outcome).contains("deadline=none"));
}

fn sha_hex(b: &[u8]) -> String {
    Sha256::digest(b)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}

/// The guest's dump line for an http completion, computed from the recorded bytes.
fn expected_http_line(op: u64, r: &HttpResponse) -> String {
    let headers: Vec<String> = r
        .headers
        .iter()
        .map(|h| format!("{}={}", h.name, h.value))
        .collect();
    format!(
        "op {op}: http {} [{}] body-sha256 {}",
        r.status,
        headers.join(","),
        sha_hex(&r.body)
    )
}

#[tokio::test]
async fn effect_bytes_exact() {
    let p = surface_guest();
    let body: Vec<u8> = (0..4096u32)
        .map(|i| (0x80 | (i * 31) as u8) | 0x40)
        .collect();
    assert!(
        String::from_utf8(body.clone()).is_err(),
        "body must be non-UTF-8"
    );
    let resp = HttpResponse {
        status: 200,
        headers: vec![
            Header {
                name: "set-cookie".to_string(),
                value: "b=2".to_string(),
            },
            Header {
                name: "x-z".to_string(),
                value: "last-first".to_string(),
            },
            Header {
                name: "set-cookie".to_string(),
                value: "a=1".to_string(),
            },
        ],
        body,
    };
    let script = script_with(Rule::CompleteEffects(Arc::new(
        move |req: &EffectRequest| match req {
            EffectRequest::Http(_) => Ok(OpOutput::Http(resp.clone())),
            _ => Ok(OpOutput::Object(vec![0xff, 0x00, 0xfe])),
        },
    )));
    let d = drive_to_completion(rt(), &p, spec("surface", b""), script).await;
    let out = output_text(&d.outcome);

    let http_op = seq_of(&d.journal, EventKind::EffectScheduled, 0);
    let recorded = d
        .journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::EffectCompleted(c) if c.op == http_op => Some(c.outcome.clone()),
            _ => None,
        })
        .expect("EffectCompleted for http");
    let decoded: Result<OpOutput, Failure> = decode_result(&recorded).expect("decode");
    let Ok(OpOutput::Http(rec)) = decoded else {
        panic!("recorded http outcome: {decoded:?}")
    };
    assert_eq!(rec.body.len(), 4096);
    let want = expected_http_line(http_op, &rec);
    assert!(
        want.contains("[set-cookie=b=2,x-z=last-first,set-cookie=a=1]"),
        "{want}"
    );
    assert!(
        out.lines().any(|l| l == want),
        "guest dump must match the recorded bytes:\nwant {want}\nin:\n{out}"
    );
    assert_replays(rt(), &p, &d).await;
}

#[tokio::test]
async fn effect_unsupported_default() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("surface", b""), Script::new()).await;
    let out = output_text(&d.outcome);
    for nth in 0..3 {
        let op = seq_of(&d.journal, EventKind::EffectScheduled, nth);
        assert!(
            out.contains(&format!("op {op}: failed non-retryable:unsupported")),
            "effect op {op} must fail unsupported:\n{out}"
        );
    }
    // Children default to unsupported as well (recorded, not waited on by surface).
    let child = seq_of(&d.journal, EventKind::ChildStarted, 0);
    let child_out = d
        .journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::ChildCompleted(c) if c.op == child => Some(c.outcome.clone()),
            _ => None,
        })
        .expect("ChildCompleted");
    let decoded: Result<OpOutput, Failure> = decode_result(&child_out).expect("decode");
    assert!(
        matches!(&decoded, Err(Failure::NonRetryable(e)) if e.kind == "unsupported"),
        "{decoded:?}"
    );
    assert_replays(rt(), &p, &d).await;
}

#[tokio::test]
async fn nested_step_is_protocol_violation() {
    let p = surface_guest();
    let d = drive_to_completion(rt(), &p, spec("nested-step", b""), Script::new()).await;
    match &d.outcome {
        ActivationOutcome::AttemptFailed(f @ AttemptFailure::ProtocolViolation(msg)) => {
            assert!(msg.contains("inside an open step"), "{msg}");
            assert!(!f.retryable(), "protocol violations are not retryable");
        }
        other => panic!("expected AttemptFailed(ProtocolViolation), got {other:?}"),
    }
    assert!(
        !kinds(&d.journal).contains(&EventKind::TimerStarted),
        "the nested command is never journaled"
    );
}
