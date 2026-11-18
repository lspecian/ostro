//! Host adapter (plan 03-07): the WIT 0.1 imports over the replay core, driven directly
//! (no activation driver yet) through the production linker.
//!
//! Covers WIT-01..06 call paths and ENG-02/03/04 end to end: live -> evict -> resume ->
//! replay, seed-controlled native randomness, exact replayed bytes, the step guard and
//! one journaled event per command kind.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::engine::{OstroEngine, configure_store};
use ostro_engine::env::Clock;
use ostro_engine::error::{Evict, EvictReason, ProtocolViolation};
use ostro_engine::host::bindings::Workflow;
use ostro_engine::host::bindings::exports::ostro::runtime::workflows::RunResult;
use ostro_engine::host::bindings::ostro::runtime::types as wit_types;
use ostro_engine::host::state::{
    HostInit, HostState, RunReturn, build_linker, run_return_from_wit,
};
use ostro_engine::replay::token;
use ostro_engine::sink::RunRef;
use ostro_engine::validate::validate_component;
use ostro_types::journal::{
    CommandArgs, Event, EventBody, EventKind, Failure, Payload, RngSeed, RunStarted, TimerFired,
    command_hash, decode_result, encode_result,
};
use wasmtime::Store;
use wasmtime::component::{Component, Linker};

const ACCEPTED_AT: u64 = 1_700_000_000_000_000_000;
const PARTITION: u16 = 3;
const RUN_ID: &str = "run-0307";
const SEED_S: [u8; 32] = [0x5a; 32];
const SEED_S2: [u8; 32] = [0xa5; 32];

/// Deterministic test clock: starts at `accepted_at`, +1 ms per call.
struct TestClock(AtomicU64);

impl Clock for TestClock {
    fn now_unix_nanos(&self) -> u64 {
        self.0.fetch_add(1_000_000, Ordering::SeqCst)
    }
}

struct Harness {
    engine: OstroEngine,
    linker: Linker<HostState>,
    components: Mutex<HashMap<String, Component>>,
}

fn harness() -> &'static Harness {
    static H: OnceLock<Harness> = OnceLock::new();
    H.get_or_init(|| {
        let engine = OstroEngine::new(EngineConfig::default()).expect("engine");
        let linker = build_linker(engine.engine()).expect("linker");
        Harness {
            engine,
            linker,
            components: Mutex::new(HashMap::new()),
        }
    })
}

/// Validate `guest` against the production linker and return its component.
fn component(guest: &str) -> Component {
    let h = harness();
    let mut map = h.components.lock().expect("lock");
    map.entry(guest.to_string())
        .or_insert_with(|| {
            let wasm = common::guest_wasm(guest);
            validate_component(h.engine.engine(), &h.linker, &wasm)
                .unwrap_or_else(|e| panic!("validate {guest}: {e}"))
                .component
        })
        .clone()
}

fn run_started(workflow: &str, input: &[u8], seed: [u8; 32]) -> Event {
    Event {
        seq: 0,
        ts_nanos: ACCEPTED_AT,
        body: EventBody::RunStarted(RunStarted {
            workflow: workflow.to_string(),
            workflow_version: 1,
            input: Payload {
                content_type: "application/octet-stream".to_string(),
                data: input.to_vec(),
            },
            rng_seed: RngSeed(seed),
            accepted_at_nanos: ACCEPTED_AT,
            deadline_nanos: Some(ACCEPTED_AT + 600_000_000_000),
            module_hash: "00".repeat(32),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        }),
    }
}

type RunOut = wasmtime::Result<Result<RunResult, wit_types::Failure>>;

/// One activation: fresh store over `journal`, call `run` with the RunStarted input.
async fn activate(guest: &str, journal: Vec<Event>) -> (RunOut, Store<HostState>) {
    let h = harness();
    let comp = component(guest);
    let (workflow, input) = match &journal[0].body {
        EventBody::RunStarted(rs) => (rs.workflow.clone(), rs.input.clone()),
        _ => panic!("journal[0] must be RunStarted"),
    };
    let state = HostState::new(HostInit {
        run: RunRef {
            partition: PARTITION,
            run_id: RUN_ID.to_string(),
        },
        attempt: 1,
        module_hash_hex: "ab".repeat(32),
        journal,
        clock: Arc::new(TestClock(AtomicU64::new(ACCEPTED_AT))),
        limiter: h.engine.new_limiter(),
        wall_deadline: Instant::now() + Duration::from_secs(60),
        token_key: [7; 32],
        continue_as_new_events: h.engine.config().continue_as_new_events,
        sink: None,
        hot: None,
    })
    .expect("host state");
    let mut store = Store::new(h.engine.engine(), state);
    configure_store(&mut store, h.engine.config().activation_fuel).expect("store");
    let out = async {
        let wf = Workflow::instantiate_async(&mut store, &comp, &h.linker).await?;
        let input = wit_types::Payload {
            content_type: input.content_type,
            data: input.data,
        };
        wf.ostro_runtime_workflows()
            .call_run(&mut store, &workflow, &input)
            .await
    }
    .await;
    (out, store)
}

fn kinds(events: &[Event]) -> Vec<EventKind> {
    events.iter().map(|e| e.body.kind()).collect()
}

fn completed(out: RunOut) -> Payload {
    match run_return_from_wit(out.expect("run must not trap")) {
        Ok(RunReturn::Completed(p)) => p,
        other => panic!("expected completed, got {other:?}"),
    }
}

fn assert_evicted(out: &RunOut) {
    let err = out.as_ref().expect_err("expected an eviction trap");
    let ev = err
        .downcast_ref::<Evict>()
        .unwrap_or_else(|| panic!("not an Evict: {err:?}"));
    assert_eq!(ev.reason, EvictReason::WaitUnsatisfied);
}

/// Run det-basic through all three activations; returns (output, full journal).
async fn det_to_completion(seed: [u8; 32]) -> (Payload, Vec<Event>) {
    let mut journal = vec![run_started("det", b"", seed)];

    // Activation 1: live until the unsatisfied wait.
    let (out, mut store) = activate("det-basic", journal.clone()).await;
    assert_evicted(&out);
    let pending = store.data_mut().take_pending();
    assert_eq!(
        kinds(&pending),
        vec![EventKind::StepCompleted, EventKind::TimerStarted],
        "an unsatisfied wait appends nothing"
    );
    journal.extend(pending);
    let timer_seq = journal[2].seq;
    journal.push(Event {
        seq: journal.len() as u64,
        ts_nanos: ACCEPTED_AT + 5_000_000_000,
        body: EventBody::TimerFired(TimerFired { op: timer_seq }),
    });

    // Activation 2: replay s1/sleep, resolve the wait, finish live.
    let (out, mut store) = activate("det-basic", journal.clone()).await;
    let output = completed(out);
    let st = store.data_mut();
    assert_eq!(st.latest_status.as_deref(), Some("{\"phase\":\"done\"}"));
    let pending = st.take_pending();
    assert_eq!(
        kinds(&pending),
        vec![
            EventKind::WaitResolved,
            EventKind::StepCompleted,
            EventKind::StepCompleted,
            EventKind::StepCompleted,
            EventKind::StepCompleted,
            EventKind::PatchMarker,
            EventKind::StepCompleted,
            EventKind::StatusPublished,
        ]
    );
    journal.extend(pending);
    st.record_terminal(&Ok(RunReturn::Completed(output.clone())))
        .expect("terminal");
    let terminal = st.take_pending();
    assert_eq!(kinds(&terminal), vec![EventKind::RunCompleted]);
    journal.extend(terminal);
    (output, journal)
}

#[test]
fn host_validate_real_linker() {
    let h = harness();
    for guest in ["det-basic", "wit-surface", "smoke"] {
        let wasm = common::guest_wasm(guest);
        validate_component(h.engine.engine(), &h.linker, &wasm)
            .unwrap_or_else(|e| panic!("{guest} must validate against build_linker: {e}"));
    }
    let wasm = common::guest_wasm("wasi-forbidden");
    let err = validate_component(h.engine.engine(), &h.linker, &wasm)
        .expect_err("wasi-forbidden must be rejected");
    assert!(err.to_string().contains("wasi:filesystem"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn host_live_then_replay_det_basic() {
    let (output, journal) = det_to_completion(SEED_S).await;

    // Hashes and op ids of the first activation's events.
    let EventBody::StepCompleted(s1) = &journal[1].body else {
        panic!("seq 1 must be s1")
    };
    assert_eq!(s1.name, "s1");
    assert_eq!(
        s1.meta.hash,
        command_hash(1, &CommandArgs::Step { name: "s1" }).expect("hash")
    );
    assert!(
        s1.fuel_used > 0,
        "store-aware step.complete must account fuel"
    );
    let EventBody::TimerStarted(t) = &journal[2].body else {
        panic!("seq 2 must be the timer")
    };
    assert_eq!(
        t.meta.hash,
        command_hash(1, &CommandArgs::Sleep { duration_ms: 5_000 }).expect("hash")
    );
    // fire_at is computed from logical now (s1's ts) before the command.
    assert_eq!(t.fire_at_nanos, journal[1].ts_nanos + 5_000_000_000);
    // The wait resolved the timer op, whose id is its seq.
    let EventBody::WaitResolved(w) = &journal[4].body else {
        panic!("seq 4 must be wait-resolved")
    };
    assert_eq!(w.completed, vec![2]);
    for ev in &journal {
        if let EventBody::StepCompleted(sc) = &ev.body {
            assert_eq!(
                sc.meta.hash,
                command_hash(1, &CommandArgs::Step { name: &sc.name }).expect("hash")
            );
        }
    }

    // Activation 3: full replay, identical output, zero appends.
    let (out, mut store) = activate("det-basic", journal.clone()).await;
    let replayed = completed(out);
    assert_eq!(replayed, output, "replay must reproduce the output bytes");
    let st = store.data_mut();
    assert!(st.take_pending().is_empty());
    st.record_terminal(&Ok(RunReturn::Completed(replayed)))
        .expect("terminal matches");
    assert!(st.take_pending().is_empty(), "replay appends nothing");
    assert!(st.logs.lines.is_empty());
    assert_eq!(st.latest_status.as_deref(), Some("{\"phase\":\"done\"}"));

    // A recorded RunCompleted vs a failing return is a divergence.
    let (_, mut store) = activate("det-basic", journal).await;
    let err = store
        .data_mut()
        .record_terminal(&Err(Failure::Timeout))
        .expect_err("terminal kind mismatch must diverge");
    assert!(err.to_string().contains("nondeterminism"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn host_seed_controls_native_randomness() {
    let (a, _) = det_to_completion(SEED_S).await;
    let (b, _) = det_to_completion(SEED_S).await;
    assert_eq!(a, b, "same seed -> identical outputs");
    let (c, _) = det_to_completion(SEED_S2).await;
    assert_ne!(a, c, "different seed -> HashMap order / rand differ");
    // s1 (first length-prefixed record) carries now + map order + rand values.
    let s1 = |p: &Payload| {
        let n = u32::from_le_bytes(p.data[..4].try_into().expect("len")) as usize;
        p.data[4..4 + n].to_vec()
    };
    let (sa, sc) = (s1(&a), s1(&c));
    assert_eq!(
        sa[..8],
        sc[..8],
        "wall clock = logical time, seed-independent"
    );
    assert_eq!(
        u64::from_le_bytes(sa[..8].try_into().expect("secs")),
        ACCEPTED_AT / 1_000_000_000
    );
    assert_ne!(sa[8..], sc[8..]);
}

#[tokio::test(flavor = "multi_thread")]
async fn host_replay_exact_bytes() {
    let mut journal = vec![run_started("echo-bytes", b"", SEED_S)];
    let (out, mut store) = activate("wit-surface", journal.clone()).await;
    let live = completed(out);
    let pending = store.data_mut().take_pending();
    assert_eq!(kinds(&pending), vec![EventKind::StepCompleted]);
    journal.extend(pending);

    let EventBody::StepCompleted(sc) = &journal[1].body else {
        panic!("seq 1 must be the step")
    };
    let recorded: Result<Payload, Failure> = decode_result(&sc.outcome).expect("decode");
    let recorded = recorded.expect("ok outcome");
    assert_eq!(recorded.data.len(), 256);
    assert!(
        std::str::from_utf8(&recorded.data).is_err(),
        "random bytes are not UTF-8"
    );
    assert_eq!(
        encode_result(&Ok::<_, Failure>(recorded.clone())),
        sc.outcome,
        "recorded bytes re-encode identically"
    );
    assert_eq!(live.data, recorded.data);

    let (out, mut store) = activate("wit-surface", journal).await;
    let replayed = completed(out);
    assert_eq!(
        replayed.data, recorded.data,
        "byte-identical replay (ENG-04)"
    );
    assert_eq!(replayed.content_type, recorded.content_type);
    assert!(store.data_mut().take_pending().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn host_nested_step_protocol() {
    let (out, mut store) =
        activate("wit-surface", vec![run_started("nested-step", b"", SEED_S)]).await;
    let err = out.expect_err("nested journaling must trap");
    let pv = err
        .downcast_ref::<ProtocolViolation>()
        .unwrap_or_else(|| panic!("not a protocol violation: {err:?}"));
    assert!(pv.0.contains("open step"), "{pv}");
    assert!(store.data_mut().take_pending().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn host_surface_journals_every_command() {
    let (out, mut store) = activate("wit-surface", vec![run_started("surface", b"", SEED_S)]).await;
    assert_evicted(&out);
    let st = store.data_mut();
    let pending = st.take_pending();
    let count = |k: EventKind| pending.iter().filter(|e| e.body.kind() == k).count();
    assert_eq!(count(EventKind::PatchMarker), 1);
    assert_eq!(count(EventKind::StatusPublished), 1);
    assert_eq!(count(EventKind::StepCompleted), 1);
    assert_eq!(count(EventKind::EffectScheduled), 3);
    assert_eq!(count(EventKind::TimerStarted), 2);
    assert_eq!(count(EventKind::SignalWaitStarted), 1);
    assert_eq!(count(EventKind::ChildStarted), 1);
    assert_eq!(count(EventKind::AwaitTokenCreated), 1);
    assert_eq!(count(EventKind::OpCancelled), 1);
    assert_eq!(count(EventKind::WaitResolved), 0);
    assert_eq!(pending.len(), 12);

    for ev in &pending {
        match &ev.body {
            EventBody::AwaitTokenCreated(t) => {
                assert!(t.token.starts_with("ot1_"), "{}", t.token);
                let parts = token::parse(&t.token).expect("parse token");
                assert_eq!(parts.partition, PARTITION);
                assert_eq!(parts.run_id, RUN_ID);
                assert_eq!(parts.op, ev.seq);
                assert!(token::verify(&t.token, &[7; 32]));
                assert_eq!(t.timeout_ms, Some(60_000));
            }
            EventBody::EffectScheduled(e) => {
                if let ostro_types::journal::EffectRequest::Http(r) = &e.request {
                    // Header values (secret templates) are journaled verbatim.
                    assert_eq!(r.headers[0].value, "{{secret:tok}}");
                }
            }
            EventBody::OpCancelled(c) => {
                // cancel-op targets the sleep-until timer.
                let target = &pending.iter().find(|e| e.seq == c.op).expect("op").body;
                assert!(matches!(target, EventBody::TimerStarted(_)));
            }
            _ => {}
        }
    }
    assert_eq!(st.latest_status.as_deref(), Some("{\"surface\":1}"));
    assert_eq!(st.logs.lines.len(), 1);
    assert_eq!(st.logs.lines[0].message, "surface log line");
}

#[tokio::test(flavor = "multi_thread")]
async fn host_surface_resume_and_replay_effects() {
    use ostro_types::journal::{
        EffectCompleted, HttpResponse, OpOutput, SqlResult, SqlValue, TimerFired,
    };
    use sha2::{Digest, Sha256};

    let mut journal = vec![run_started("surface", b"", SEED_S)];
    let (out, mut store) = activate("wit-surface", journal.clone()).await;
    assert_evicted(&out);
    journal.extend(store.data_mut().take_pending());

    let seqs = |k: EventKind| -> Vec<u64> {
        journal
            .iter()
            .filter(|e| e.body.kind() == k)
            .map(|e| e.seq)
            .collect()
    };
    let effects = seqs(EventKind::EffectScheduled);
    let (http, sql, object) = (effects[0], effects[1], effects[2]);
    let sleep_op = seqs(EventKind::TimerStarted)[0];
    let body = vec![0xff, 0x00, 0x01];
    let http_out: Result<OpOutput, Failure> = Ok(OpOutput::Http(HttpResponse {
        status: 200,
        headers: vec![
            ostro_types::journal::Header {
                name: "b".into(),
                value: "2".into(),
            },
            ostro_types::journal::Header {
                name: "a".into(),
                value: "1".into(),
            },
        ],
        body: body.clone(),
    }));
    let sql_out: Result<OpOutput, Failure> = Ok(OpOutput::Sql(SqlResult {
        columns: vec!["x".into()],
        rows: vec![vec![SqlValue::Int8(1)]],
        rows_affected: 1,
    }));
    let object_out: Result<OpOutput, Failure> = Err(Failure::Timeout);
    // Completion order differs from the order the guest lists the ops in.
    let inputs = vec![
        EventBody::EffectCompleted(EffectCompleted {
            op: sql,
            outcome: encode_result(&sql_out),
        }),
        EventBody::EffectCompleted(EffectCompleted {
            op: http,
            outcome: encode_result(&http_out),
        }),
        EventBody::EffectCompleted(EffectCompleted {
            op: object,
            outcome: encode_result(&object_out),
        }),
        EventBody::TimerFired(TimerFired { op: sleep_op }),
    ];
    for body in inputs {
        journal.push(Event {
            seq: journal.len() as u64,
            ts_nanos: ACCEPTED_AT + 10_000_000_000,
            body,
        });
    }

    let (out, mut store) = activate("wit-surface", journal.clone()).await;
    let live = completed(out);
    let st = store.data_mut();
    let pending = st.take_pending();
    assert_eq!(
        kinds(&pending),
        vec![EventKind::WaitResolved, EventKind::WaitResolved]
    );
    let EventBody::WaitResolved(w) = &pending[0].body else {
        unreachable!()
    };
    assert_eq!(
        w.completed,
        vec![sql, http, object],
        "journal completion order"
    );
    let EventBody::WaitResolved(w) = &pending[1].body else {
        unreachable!()
    };
    assert_eq!(w.completed, vec![sleep_op]);
    let dump = String::from_utf8(live.data.clone()).expect("utf8 dump");
    let body_sha: String = Sha256::digest(&body)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert!(
        dump.contains(&format!(
            "op {http}: http 200 [b=2,a=1] body-sha256 {body_sha}"
        )),
        "{dump}"
    );
    assert!(
        dump.contains(&format!("op {object}: failed timeout")),
        "{dump}"
    );
    assert!(dump.contains(&format!("op {sleep_op}: timer")), "{dump}");
    // Activation 2 starts by replaying activation 1's commands.
    assert!(dump.contains("is-replaying true"), "{dump}");
    journal.extend(pending);
    st.record_terminal(&Ok(RunReturn::Completed(live.clone())))
        .expect("terminal");
    journal.extend(st.take_pending());

    // Full replay: identical dump (decoded from the same recorded bytes), no appends.
    let (out, mut store) = activate("wit-surface", journal).await;
    let replayed = completed(out);
    assert_eq!(
        replayed, live,
        "byte-identical replay of effect completions"
    );
    let st = store.data_mut();
    st.record_terminal(&Ok(RunReturn::Completed(replayed)))
        .expect("terminal");
    assert!(st.take_pending().is_empty());
    assert!(st.logs.lines.is_empty(), "logs are suppressed on replay");
}
