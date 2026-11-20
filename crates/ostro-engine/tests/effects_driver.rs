//! The effect I/O seam (plan 06-06, FX-01 / FX-03 / FX-06) against a recording fake
//! driver.
//!
//! - Redaction (W3/F2): values of headers named in `http-request.redact` are journaled as
//!   `"[redacted]"` while the [`LiveEffect`] carries the real value; the command hash is
//!   unchanged.
//! - FX-01: `start` only after the append carrying `EffectScheduled` returned `Ok`; a
//!   fenced or conflicting append starts nothing.
//! - FX-03 / F1: a replaying activation re-drives (`redrive = true`) every re-issued
//!   effect without a completion in the loaded journal, with the re-issued full request;
//!   completed or cancelled ops are never started.
//! - `cancel-op` on an effect calls `cancel` after the `OpCancelled` commit.
//! - Without a driver the journal is identical and nothing is started.
//!
//! The direct tests drive `HostState`'s import impls; the activation tests run the
//! `wit-surface` guest's `surface` workflow (http + sql + object effects, then
//! `wait(all)` on them) through the production activation driver.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_engine::config::EngineConfig;
use ostro_engine::engine::OstroEngine;
use ostro_engine::error::EvictReason;
use ostro_engine::host::bindings::ostro::runtime::effects::{
    Header, Host, HttpRequest, ObjectRequest, SqlRequest, SqlValue,
};
use ostro_engine::host::bindings::ostro::runtime::types::RetryPolicy;
use ostro_engine::host::state::{HostInit, HostState};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, ActivationSink, AppendError, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, Rule, RunSpec, Script};
use ostro_engine::{EffectDriver, LiveEffect};
use ostro_types::journal::{
    self as j, EffectCompleted, EffectRequest, Event, EventBody, EventKind, HASH_VER, OpOutput,
    command_args, command_hash, encode_result,
};
use ostro_types::live::EffectRetry;
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

const CANARY: &str = "canary-06-06-9d1e";
const PARTITION: u16 = 6;

/// One `start` call as seen by the driver.
#[derive(Debug, Clone)]
struct Started {
    run: RunRef,
    attempt: u32,
    effect: LiveEffect,
}

impl Started {
    fn headers(&self) -> Vec<(String, String)> {
        match &self.effect.request {
            EffectRequest::Http(r) => r
                .headers
                .iter()
                .map(|h| (h.name.clone(), h.value.clone()))
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// Records every `start` / `cancel` call.
#[derive(Default)]
struct RecordingDriver {
    started: Mutex<Vec<Started>>,
    cancelled: Mutex<Vec<(RunRef, u64)>>,
}

impl RecordingDriver {
    fn started(&self) -> Vec<Started> {
        self.started.lock().expect("lock").clone()
    }

    fn cancelled(&self) -> Vec<(RunRef, u64)> {
        self.cancelled.lock().expect("lock").clone()
    }

    fn ops(&self) -> Vec<(u64, bool)> {
        let mut v: Vec<_> = self
            .started()
            .iter()
            .map(|s| (s.effect.op, s.effect.redrive))
            .collect();
        v.sort_unstable();
        v
    }
}

impl EffectDriver for RecordingDriver {
    fn start(&self, run: &RunRef, attempt: u32, effect: LiveEffect) {
        self.started.lock().expect("lock").push(Started {
            run: run.clone(),
            attempt,
            effect,
        });
    }

    fn cancel(&self, run: &RunRef, op: u64) {
        self.cancelled.lock().expect("lock").push((run.clone(), op));
    }
}

fn run_ref(id: &str) -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: id.to_string(),
    }
}

// ---------------------------------------------------------------------------------------
// Direct HostState tests
// ---------------------------------------------------------------------------------------

fn engine() -> &'static OstroEngine {
    static E: std::sync::OnceLock<OstroEngine> = std::sync::OnceLock::new();
    E.get_or_init(|| OstroEngine::new(EngineConfig::default()).expect("engine"))
}

fn host(run: &RunRef, journal: Vec<Event>, sink: Arc<FakeSink>) -> HostState {
    let e = engine();
    HostState::new(HostInit {
        run: run.clone(),
        attempt: 1,
        module_hash_hex: "ab".repeat(32),
        journal,
        clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 1_000_000)),
        limiter: e.new_limiter(),
        wall_deadline: Instant::now() + Duration::from_secs(60),
        token_key: [7; 32],
        continue_as_new_events: e.config().continue_as_new_events,
        sink: Some(sink as Arc<dyn ActivationSink>),
        hot: None,
    })
    .expect("host state")
}

/// A sink (optionally with `driver`) seeded with `run`'s `RunStarted`.
fn seeded(run: &RunRef, driver: Option<Arc<RecordingDriver>>, script: &Script) -> Arc<FakeSink> {
    let mut sink = FakeSink::from_script(script);
    if let Some(d) = driver {
        sink = sink.with_effect_driver(d);
    }
    let started = RunSpec::new(run.clone(), "direct", b"").run_started_event(&"ab".repeat(32));
    sink.seed(run.clone(), vec![started], 1);
    Arc::new(sink)
}

fn retry() -> RetryPolicy {
    RetryPolicy {
        max_attempts: 4,
        initial_interval_ms: 250,
        max_interval_ms: 8_000,
        backoff_coefficient: 2.0,
        non_retryable_kinds: vec!["bad-request".into()],
    }
}

fn http_req(url: &str) -> HttpRequest {
    HttpRequest {
        method: "POST".into(),
        url: url.into(),
        headers: vec![
            Header {
                name: "Authorization".into(),
                value: format!("Bearer {CANARY}"),
            },
            Header {
                name: "X-Api-Key".into(),
                value: "{{secret:api_key}}".into(),
            },
            Header {
                name: "Accept".into(),
                value: "application/json".into(),
            },
        ],
        body: Some(b"{\"amount\":1}".to_vec()),
        timeout_ms: Some(5_000),
        idempotency_key: Some("idem-1".into()),
        retry: Some(retry()),
        // Case-insensitive match against "Authorization".
        redact: vec!["authorization".into()],
    }
}

fn sql_req() -> SqlRequest {
    SqlRequest {
        connection: "orders".into(),
        statement: "insert into t values ($1)".into(),
        params: vec![SqlValue::Text(CANARY.into()), SqlValue::Int8(7)],
        exactly_once: true,
    }
}

fn object_req() -> ObjectRequest {
    ObjectRequest {
        connection: "files".into(),
        op: "put".into(),
        key: "a/b".into(),
        body: Some(CANARY.as_bytes().to_vec()),
    }
}

fn headers_of(req: &EffectRequest) -> Vec<(String, String)> {
    match req {
        EffectRequest::Http(r) => r
            .headers
            .iter()
            .map(|h| (h.name.clone(), h.value.clone()))
            .collect(),
        _ => Vec::new(),
    }
}

fn real_headers() -> Vec<(String, String)> {
    vec![
        ("Authorization".into(), format!("Bearer {CANARY}")),
        ("X-Api-Key".into(), "{{secret:api_key}}".into()),
        ("Accept".into(), "application/json".into()),
    ]
}

fn journaled_headers() -> Vec<(String, String)> {
    vec![
        ("Authorization".into(), "[redacted]".into()),
        ("X-Api-Key".into(), "{{secret:api_key}}".into()),
        ("Accept".into(), "application/json".into()),
    ]
}

fn effect_retry() -> EffectRetry {
    EffectRetry {
        max_attempts: 4,
        initial_interval_ms: 250,
        max_interval_ms: 8_000,
        backoff_coefficient: 2.0,
        non_retryable_kinds: vec!["bad-request".into()],
    }
}

fn scheduled(journal: &[Event], op: u64) -> &j::EffectScheduled {
    match &journal[op as usize].body {
        EventBody::EffectScheduled(e) => e,
        other => panic!("seq {op} is {}", other.summary()),
    }
}

fn completed_event(seq: u64, op: u64) -> Event {
    Event {
        seq,
        ts_nanos: DEFAULT_ACCEPTED_AT + 1_000_000_000,
        body: EventBody::EffectCompleted(EffectCompleted {
            op,
            outcome: encode_result(&Ok::<OpOutput, j::Failure>(OpOutput::Object(
                b"done".to_vec(),
            ))),
        }),
    }
}

/// W3/F2: the journal holds `"[redacted]"`, the LiveEffect the real value; templates are
/// journaled verbatim; the retry policy reaches the driver; nothing starts before flush.
#[tokio::test]
async fn redacted_journal_copy_and_full_live_request() {
    let run = run_ref("redact");
    let driver = Arc::new(RecordingDriver::default());
    let sink = seeded(&run, Some(driver.clone()), &Script::new());
    let mut st = host(&run, sink.journal(&run), sink.clone());

    let op = st
        .http_fetch(http_req("https://api.example.com/charge"))
        .await
        .expect("fetch");
    assert_eq!(op, 1);
    assert!(driver.started().is_empty(), "no I/O before the commit");
    st.flush().await.expect("flush");

    let journal = sink.journal(&run);
    let ev = scheduled(&journal, op);
    assert_eq!(headers_of(&ev.request), journaled_headers());
    assert!(
        !format!("{journal:?}").contains(CANARY),
        "real value journaled"
    );

    let started = driver.started();
    assert_eq!(started.len(), 1);
    let s = &started[0];
    assert_eq!(s.run, run);
    assert_eq!(s.attempt, 1);
    assert_eq!(s.effect.op, op);
    assert!(!s.effect.redrive);
    assert_eq!(s.effect.scheduled_ts_nanos, journal[op as usize].ts_nanos);
    assert_eq!(s.headers(), real_headers());
    assert_eq!(s.effect.http_retry, Some(effect_retry()));
    assert!(
        !format!("{:?}", s.effect).contains(CANARY),
        "LiveEffect Debug leaks"
    );

    // The journaled hash is the hash of the live request and of the redacted copy.
    let EffectRequest::Http(live) = &s.effect.request else {
        panic!("not http");
    };
    let live_req = EffectRequest::Http(live.clone());
    let live_hash = command_hash(HASH_VER, &command_args(&live_req)).expect("hash");
    let journal_hash = command_hash(HASH_VER, &command_args(&ev.request)).expect("hash");
    assert_eq!(live_hash, journal_hash);
    assert_eq!(ev.meta.hash, live_hash);
}

/// Header values are not hashed: `[redacted]` and the real value hash the same.
#[test]
fn command_hash_ignores_redacted_values() {
    let mk = |v: &str| {
        EffectRequest::Http(j::HttpRequest {
            method: "GET".into(),
            url: "https://example.test/x".into(),
            headers: vec![j::Header {
                name: "Authorization".into(),
                value: v.into(),
            }],
            body: None,
            timeout_ms: None,
            idempotency_key: None,
            redact: vec!["authorization".into()],
        })
    };
    let real = command_hash(HASH_VER, &command_args(&mk(CANARY))).expect("hash");
    let redacted = command_hash(HASH_VER, &command_args(&mk("[redacted]"))).expect("hash");
    assert_eq!(real, redacted);
}

/// SQL and object requests reach the driver exactly as journaled (no redaction, no retry).
#[tokio::test]
async fn sql_and_object_requests_are_kept_as_is() {
    let run = run_ref("sql-object");
    let driver = Arc::new(RecordingDriver::default());
    let sink = seeded(&run, Some(driver.clone()), &Script::new());
    let mut st = host(&run, sink.journal(&run), sink.clone());
    let sql = st.sql_query(sql_req()).await.expect("sql");
    let obj = st.object_store(object_req()).await.expect("object");
    st.flush().await.expect("flush");

    let journal = sink.journal(&run);
    let started = driver.started();
    assert_eq!(driver.ops(), vec![(sql, false), (obj, false)]);
    for s in &started {
        assert_eq!(s.effect.request, scheduled(&journal, s.effect.op).request);
        assert_eq!(s.effect.http_retry, None);
        assert_eq!(
            s.effect.scheduled_ts_nanos,
            journal[s.effect.op as usize].ts_nanos
        );
    }
}

/// FX-01: a fenced or conflicting append starts nothing, and a later flush does not
/// start the dropped requests either.
#[tokio::test]
async fn failed_append_starts_nothing() {
    for (script, want) in [
        (
            Script::new().rule(Rule::FenceOnAppend(1)),
            AppendError::Fenced,
        ),
        (
            Script::new().rule(Rule::ConflictOnAppend(1)),
            AppendError::Conflict { expected: 1 },
        ),
    ] {
        let run = run_ref("fail");
        let driver = Arc::new(RecordingDriver::default());
        let sink = seeded(&run, Some(driver.clone()), &script);
        let mut st = host(&run, sink.journal(&run), sink.clone());
        st.http_fetch(http_req("https://api.example.com/a"))
            .await
            .expect("fetch");
        st.sql_query(sql_req()).await.expect("sql");
        assert_eq!(st.flush().await, Err(want));
        assert!(driver.started().is_empty(), "started after a failed append");
        assert_eq!(st.flush().await, Ok(()));
        assert!(driver.started().is_empty());
        assert_eq!(sink.journal(&run).len(), 1, "nothing committed");
    }
}

/// FX-03 / F1: activation 2 replays; uncompleted effects are re-driven with the re-issued
/// full request (real headers, retry) and the journaled timestamp; the completed one is
/// not; nothing is appended or started again by its flush.
#[tokio::test]
async fn replay_redrives_only_uncompleted_effects() {
    let run = run_ref("redrive");
    let d1 = Arc::new(RecordingDriver::default());
    let sink = seeded(&run, Some(d1.clone()), &Script::new());
    let mut st = host(&run, sink.journal(&run), sink.clone());
    let a = st
        .http_fetch(http_req("https://api.example.com/a"))
        .await
        .expect("a");
    let b = st
        .http_fetch(http_req("https://api.example.com/b"))
        .await
        .expect("b");
    let sql = st.sql_query(sql_req()).await.expect("sql");
    st.flush().await.expect("flush");
    drop(st);
    assert_eq!(d1.ops(), vec![(a, false), (b, false), (sql, false)]);

    // The leader completes `b` while the run is evicted.
    let len = sink.journal(&run).len() as u64;
    sink.commit_inputs(&run, vec![completed_event(len, b)]);
    let journal = sink.journal(&run);

    let d2 = Arc::new(RecordingDriver::default());
    let sink2 = Arc::new(FakeSink::new().with_effect_driver(d2.clone()));
    sink2.seed(run.clone(), journal.clone(), 1);
    let mut st = host(&run, journal.clone(), sink2.clone());
    assert!(st.is_replaying());
    assert_eq!(
        st.http_fetch(http_req("https://api.example.com/a"))
            .await
            .expect("a"),
        a
    );
    assert_eq!(d2.ops(), vec![(a, true)], "re-driven at the re-issued call");
    assert_eq!(
        st.http_fetch(http_req("https://api.example.com/b"))
            .await
            .expect("b"),
        b
    );
    assert_eq!(st.sql_query(sql_req()).await.expect("sql"), sql);
    st.flush().await.expect("flush");
    assert_eq!(d2.ops(), vec![(a, true), (sql, true)]);
    assert_eq!(sink2.appended_events(&run), 0);

    for s in d2.started() {
        assert_eq!(s.attempt, 1);
        assert_eq!(
            s.effect.scheduled_ts_nanos,
            journal[s.effect.op as usize].ts_nanos
        );
        if s.effect.op == a {
            assert_eq!(s.headers(), real_headers());
            assert_eq!(s.effect.http_retry, Some(effect_retry()));
            let EffectRequest::Http(r) = &s.effect.request else {
                panic!("not http");
            };
            assert_eq!(r.url, "https://api.example.com/a");
        }
    }
}

/// An op cancelled by the guest (`OpCancelled` in the loaded journal) is never re-driven.
#[tokio::test]
async fn replay_never_redrives_cancelled_effects() {
    let run = run_ref("cancelled");
    let d1 = Arc::new(RecordingDriver::default());
    let sink = seeded(&run, Some(d1.clone()), &Script::new());
    let mut st = host(&run, sink.journal(&run), sink.clone());
    let a = st
        .http_fetch(http_req("https://api.example.com/a"))
        .await
        .expect("a");
    st.flush().await.expect("flush");
    st.cancel_op(a).await.expect("cancel");
    st.flush().await.expect("flush");
    drop(st);
    let journal = sink.journal(&run);

    let d2 = Arc::new(RecordingDriver::default());
    let sink2 = Arc::new(FakeSink::new().with_effect_driver(d2.clone()));
    sink2.seed(run.clone(), journal.clone(), 1);
    let mut st = host(&run, journal, sink2.clone());
    st.http_fetch(http_req("https://api.example.com/a"))
        .await
        .expect("a");
    st.cancel_op(a).await.expect("cancel");
    st.flush().await.expect("flush");
    assert!(d2.started().is_empty());
    assert!(
        d2.cancelled().is_empty(),
        "replayed cancels are not re-sent"
    );
}

/// `cancel-op` on an in-flight effect calls `cancel` only after the `OpCancelled` commit;
/// an effect scheduled and cancelled in one batch is never started (nor cancelled).
#[tokio::test]
async fn cancel_op_cancels_after_commit() {
    let run = run_ref("cancel");
    let driver = Arc::new(RecordingDriver::default());
    let sink = seeded(&run, Some(driver.clone()), &Script::new());
    let mut st = host(&run, sink.journal(&run), sink.clone());
    let a = st
        .http_fetch(http_req("https://api.example.com/a"))
        .await
        .expect("a");
    st.flush().await.expect("flush");
    assert_eq!(driver.ops(), vec![(a, false)]);
    st.cancel_op(a).await.expect("cancel");
    assert!(driver.cancelled().is_empty(), "cancel before the commit");
    st.flush().await.expect("flush");
    assert_eq!(driver.cancelled(), vec![(run.clone(), a)]);

    let b = st
        .http_fetch(http_req("https://api.example.com/b"))
        .await
        .expect("b");
    st.cancel_op(b).await.expect("cancel");
    st.flush().await.expect("flush");
    assert_eq!(driver.ops(), vec![(a, false)], "b must never start");
    assert_eq!(driver.cancelled(), vec![(run.clone(), a)]);
}

/// Without a driver: same journal as with one, and nothing is ever started.
#[tokio::test]
async fn no_driver_identical_journal() {
    async fn journal_of(driver: Option<Arc<RecordingDriver>>) -> Vec<Event> {
        let run = run_ref("same");
        let sink = seeded(&run, driver, &Script::new());
        let mut st = host(&run, sink.journal(&run), sink.clone());
        let a = st
            .http_fetch(http_req("https://api.example.com/a"))
            .await
            .expect("a");
        st.sql_query(sql_req()).await.expect("sql");
        st.object_store(object_req()).await.expect("object");
        st.cancel_op(a).await.expect("cancel");
        st.flush().await.expect("flush");
        sink.journal(&run)
    }
    let driver = Arc::new(RecordingDriver::default());
    let with = journal_of(Some(driver.clone())).await;
    let without = journal_of(None).await;
    assert_eq!(with, without);
    assert_eq!(
        with.iter().map(|e| e.body.kind()).collect::<Vec<_>>(),
        vec![
            EventKind::RunStarted,
            EventKind::EffectScheduled,
            EventKind::EffectScheduled,
            EventKind::EffectScheduled,
            EventKind::OpCancelled,
        ]
    );
    assert_eq!(driver.ops(), vec![(2, false), (3, false)]);
}

// ---------------------------------------------------------------------------------------
// Activation tests (wit-surface `surface`: http + sql + object, then wait(all))
// ---------------------------------------------------------------------------------------

fn runtime(hot_wait: Duration) -> Arc<Runtime> {
    Arc::new(
        Runtime::new(
            EngineConfig {
                hot_wait,
                ..EngineConfig::default()
            },
            RuntimeOptions {
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [9; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime"),
    )
}

fn surface(rt: &Runtime) -> Arc<Prepared> {
    Arc::new(
        rt.prepare(&common::guest_wasm("wit-surface"))
            .expect("prepare wit-surface"),
    )
}

fn activate_req(run: &RunRef, journal: Vec<Event>) -> Activate {
    Activate {
        run: run.clone(),
        attempt: 1,
        module_sha: [0; 32],
        journal,
        inputs: None,
        cancel: CancellationToken::new(),
    }
}

fn seed_surface(p: &Prepared, run: &RunRef, sink: &FakeSink) -> Vec<Event> {
    let started = RunSpec::new(run.clone(), "surface", b"").run_started_event(&p.module_hash_hex);
    sink.seed(run.clone(), vec![started.clone()], 1);
    vec![started]
}

/// The effect ops of a journal (http, sql, object in issue order).
fn effect_ops(journal: &[Event]) -> Vec<u64> {
    journal
        .iter()
        .filter(|e| e.body.kind() == EventKind::EffectScheduled)
        .map(|e| e.seq)
        .collect()
}

/// A sink whose first `append` blocks until released.
struct GatedSink {
    inner: Arc<FakeSink>,
    entered: Arc<Notify>,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl ActivationSink for GatedSink {
    async fn append(
        &self,
        run: &RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        let gate = self.gate.lock().expect("lock").take();
        if let Some(rx) = gate {
            self.entered.notify_one();
            rx.await.expect("release");
        }
        self.inner.append(run, attempt, from_seq, events).await
    }

    async fn fetch_blob(&self, sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        self.inner.fetch_blob(sha).await
    }

    async fn done(&self, run: &RunRef, attempt: u32, outcome: ActivationOutcome) {
        self.inner.done(run, attempt, outcome).await;
    }

    fn effect_driver(&self) -> Option<Arc<dyn EffectDriver>> {
        self.inner.effect_driver()
    }
}

/// FX-01 end to end: while the append carrying the `EffectScheduled`s is pending the
/// driver has seen nothing; after it returns each effect is started exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn start_only_after_append_returns() {
    let rt = runtime(Duration::ZERO);
    let p = surface(&rt);
    let run = run_ref("gated");
    let driver = Arc::new(RecordingDriver::default());
    let inner = Arc::new(FakeSink::new().with_effect_driver(driver.clone()));
    let journal = seed_surface(&p, &run, &inner);
    let (tx, rx) = oneshot::channel();
    let entered = Arc::new(Notify::new());
    let sink = Arc::new(GatedSink {
        inner: inner.clone(),
        entered: entered.clone(),
        gate: Mutex::new(Some(rx)),
    });
    let task = {
        let (rt, p, act) = (rt.clone(), p.clone(), activate_req(&run, journal));
        tokio::spawn(async move { rt.activate(&p, act, sink).await })
    };
    tokio::time::timeout(Duration::from_secs(30), entered.notified())
        .await
        .expect("append reached");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        driver.started().is_empty(),
        "started before the append returned"
    );
    tx.send(()).expect("release");
    let outcome = task.await.expect("join");
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::WaitUnsatisfied
        }
    );
    let committed = inner.journal(&run);
    let ops = effect_ops(&committed);
    assert_eq!(ops.len(), 3);
    assert_eq!(
        driver.ops(),
        ops.iter().map(|&op| (op, false)).collect::<Vec<_>>()
    );
    // wit-surface redacts X-Token: journal "[redacted]", driver the real template.
    let http = driver
        .started()
        .into_iter()
        .find(|s| s.effect.op == ops[0])
        .expect("http start");
    assert_eq!(
        http.headers()[0],
        ("X-Token".into(), "{{secret:tok}}".into())
    );
    assert_eq!(
        headers_of(&scheduled(&committed, ops[0]).request)[0],
        ("X-Token".into(), "[redacted]".into())
    );
}

/// A fenced activation starts nothing.
#[tokio::test(flavor = "multi_thread")]
async fn fenced_activation_starts_nothing() {
    let rt = runtime(Duration::ZERO);
    let p = surface(&rt);
    let run = run_ref("fenced");
    let driver = Arc::new(RecordingDriver::default());
    let sink = Arc::new(
        FakeSink::from_script(&Script::new().rule(Rule::FenceOnAppend(1)))
            .with_effect_driver(driver.clone()),
    );
    let journal = seed_surface(&p, &run, &sink);
    let outcome = rt
        .activate(&p, activate_req(&run, journal), sink.clone())
        .await;
    assert_eq!(outcome, ActivationOutcome::Fenced);
    assert!(driver.started().is_empty());
}

/// Two activations: the first commits and evicts (live starts), the second replays the
/// committed journal plus the http completion and re-drives only sql and object.
#[tokio::test(flavor = "multi_thread")]
async fn evict_then_replay_redrives_open_effects() {
    let rt = runtime(Duration::ZERO);
    let p = surface(&rt);
    let run = run_ref("two-activations");
    let d1 = Arc::new(RecordingDriver::default());
    let sink = Arc::new(FakeSink::new().with_effect_driver(d1.clone()));
    let journal = seed_surface(&p, &run, &sink);
    let outcome = rt
        .activate(&p, activate_req(&run, journal), sink.clone())
        .await;
    assert!(
        matches!(outcome, ActivationOutcome::Evicted { .. }),
        "{outcome:?}"
    );
    let ops = effect_ops(&sink.journal(&run));
    assert_eq!(
        d1.ops(),
        ops.iter().map(|&op| (op, false)).collect::<Vec<_>>()
    );

    let len = sink.journal(&run).len() as u64;
    sink.commit_inputs(&run, vec![completed_event(len, ops[0])]);
    let journal = sink.journal(&run);
    let d2 = Arc::new(RecordingDriver::default());
    let sink2 = Arc::new(FakeSink::new().with_effect_driver(d2.clone()));
    sink2.seed(run.clone(), journal.clone(), 1);
    let outcome = rt
        .activate(&p, activate_req(&run, journal.clone()), sink2.clone())
        .await;
    assert!(
        matches!(outcome, ActivationOutcome::Evicted { .. }),
        "{outcome:?}"
    );
    assert_eq!(d2.ops(), vec![(ops[1], true), (ops[2], true)]);
    for s in d2.started() {
        assert_eq!(
            s.effect.scheduled_ts_nanos,
            journal[s.effect.op as usize].ts_nanos
        );
        assert_eq!(s.effect.request, scheduled(&journal, s.effect.op).request);
    }
    assert_eq!(
        sink2.appended_events(&run),
        0,
        "a faithful replay appends nothing"
    );
}

/// Hot wait: effect completions arriving through `inputs` resolve the wait in the same
/// activation, as before; each effect is started once, live.
///
/// Timers are not fired live: the fake sink would commit them concurrently with the
/// activation's own appends (a fake-leader race unrelated to the driver). The second wait
/// (cancellation or a 1 s sleep) therefore parks until `hot_wait` elapses.
#[tokio::test(flavor = "multi_thread")]
async fn hot_wait_resolves_from_inputs_with_driver() {
    let rt = runtime(Duration::from_secs(2));
    let p = surface(&rt);
    let run = run_ref("hot");
    let driver = Arc::new(RecordingDriver::default());
    let script = Script::new()
        .live(Duration::from_millis(10))
        .fire_timers(false)
        .rule(Rule::CompleteEffects(Arc::new(|_| {
            Ok(OpOutput::Object(b"ok".to_vec()))
        })));
    let sink = Arc::new(FakeSink::from_script(&script).with_effect_driver(driver.clone()));
    seed_surface(&p, &run, &sink);
    let (journal, rx) = sink.open_live(&run);
    let mut act = activate_req(&run, journal);
    act.inputs = Some(rx);
    let outcome = rt.activate(&p, act, sink.clone()).await;
    sink.close_live(&run);
    assert_eq!(
        outcome,
        ActivationOutcome::Evicted {
            reason: EvictReason::HotWaitElapsed
        }
    );
    let journal = sink.journal(&run);
    let ops = effect_ops(&journal);
    assert_eq!(ops.len(), 3);
    let resolved: Vec<&j::WaitResolved> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::WaitResolved(w) => Some(w),
            _ => None,
        })
        .collect();
    assert_eq!(resolved.len(), 1, "the effect wait resolved hot");
    let mut completed = resolved[0].completed.clone();
    completed.sort_unstable();
    assert_eq!(completed, ops);
    assert_eq!(
        driver.ops(),
        ops.iter().map(|&op| (op, false)).collect::<Vec<_>>()
    );
}
