//! Engine leg (D-P9-22): runs an engine-leg [`Spec`] against a language's conformance
//! module on `ostro-engine` (shared [`Runtime`] per language class, testkit [`FakeSink`],
//! a settable fake clock), with the harness acting as the partition leader / scheduler.
//!
//! **Scheduler.** After every activation:
//!
//! - `Completed` / `Failed` end the run; `ContinuedAsNew` starts the successor run (the
//!   chain is projected as one); `AttemptFailed(NonDeterminism)` ends the scenario with
//!   status `non_determinism`.
//! - `Evicted(StepBackoff{retry_at})`: the clock is advanced to `retry_at` and the run
//!   re-activated, consuming no script step. The engine runs with `hot_wait = 0`, so every
//!   step backoff is a requeue and the harness, not real time, moves the clock.
//! - `Evicted(WaitUnsatisfied)`, in this order: (1) buffered signals are matched to open
//!   waits (`SignalPayload` + `SignalMatched`, FIFO per name, as the leader does); (2) the
//!   next script step is delivered if its precondition holds (`token` needs an open
//!   token, `fire_timers` a pending timer); (3) otherwise the earliest pending timer fires
//!   at its due time (`TimerStarted`, or the timeout of a signal wait / await token, which
//!   completes as `WaitTimedOut`), advancing the clock; (4) otherwise the run is stuck,
//!   which is a failure.
//!
//! **Strict checks (per language).** Every scenario runs twice with the same seed and run
//! id and the encoded journals must be byte-identical; every journal of the chain is
//! replayed on the module the run ended on and must append 0 events.
//!
//! **Defaults.** Seed `42`, run id [`DEFAULT_RUN_ID`], partition 1. The 32-byte run seed is
//! the spec seed (u64 LE) in bytes 0..8 and the chain generation (u32 LE) in bytes 8..12.
//! Successor runs of a continue-as-new chain get run id `<run id>.<generation>`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::{Duration, Instant};

use ostro_engine::config::{EngineConfig, LanguageClass};
use ostro_engine::env::Clock;
use ostro_engine::error::{AttemptFailure, EvictReason};
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeSink, replay_journal};
use ostro_types::journal::{
    CancelRequested, Event, EventBody, OpId, Payload, RngSeed, RunStarted, SignalMatched,
    SignalPayload, TimerFired, TokenCompleted, WaitTimedOut,
};
use serde_json::Value;

use crate::canon;
use crate::compare;
use crate::langs::{self, Lang};
use crate::projection::{self, Projection};
use crate::spec::{ScriptStep, Spec};

/// Run id used when a spec does not fix one.
pub const DEFAULT_RUN_ID: &str = "01890000-0000-7000-8000-000000000001";
/// Seed used when a spec does not fix one.
pub const DEFAULT_SEED: u64 = 42;
/// Run start (`accepted_at`) of the first run of every scenario.
pub const T0: u64 = DEFAULT_ACCEPTED_AT;
/// Activation cap per run (a runaway scheduler is a failure, not a hang).
const MAX_ACTIVATIONS: u32 = 200;
/// Continue-as-new chain cap.
const MAX_CHAIN: u32 = 50;
const PARTITION: u16 = 1;

/// A failed scenario (or harness error) with a readable message.
#[derive(Debug, Clone)]
pub struct Failure {
    pub id: String,
    pub lang: Lang,
    pub message: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} [{}]: {}",
            self.id,
            langs::name(self.lang),
            self.message
        )
    }
}

impl std::error::Error for Failure {}

/// What a passing scenario produced.
#[derive(Debug, Clone)]
pub struct RunReport {
    pub id: String,
    pub lang: Lang,
    /// Terminal status of the last run (`completed`, `failed`, `cancelled`, ...,
    /// `non_determinism`).
    pub status: String,
    /// Output JSON of a completed run.
    pub result: Option<Value>,
    /// Failure kind of a failed / cancelled run.
    pub failure_kind: Option<String>,
    /// Normalised projection of the whole chain.
    pub projection: Projection,
    /// Every run's committed journal (first run first).
    pub journals: Vec<Vec<Event>>,
    /// Activations of the first execution.
    pub activations: u32,
    /// Wall time of the whole check (both executions, replays, extra runs).
    pub elapsed: Duration,
}

// ---------------------------------------------------------------------------
// Shared runtime, clock and module cache
// ---------------------------------------------------------------------------

/// Clock that only moves when the harness sets it (frozen per activation).
#[derive(Debug)]
struct ConfClock(AtomicU64);

impl ConfClock {
    fn set(&self, nanos: u64) {
        self.0.store(nanos, Ordering::SeqCst);
    }

    fn advance_to(&self, nanos: u64) {
        self.0.fetch_max(nanos, Ordering::SeqCst);
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

impl Clock for ConfClock {
    fn now_unix_nanos(&self) -> u64 {
        self.get()
    }
}

fn clock() -> &'static Arc<ConfClock> {
    static CLOCK: OnceLock<Arc<ConfClock>> = OnceLock::new();
    CLOCK.get_or_init(|| Arc::new(ConfClock(AtomicU64::new(T0))))
}

/// One scenario at a time per process: the runtimes share one settable clock.
fn run_lock() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

fn new_runtime(class: LanguageClass) -> Runtime {
    Runtime::new(
        EngineConfig {
            class,
            // Every step backoff becomes a requeue (StepBackoff) and no wait parks hot:
            // the harness owns time.
            hot_wait: Duration::ZERO,
            ..EngineConfig::default()
        },
        RuntimeOptions {
            clock: clock().clone(),
            token_key: [0x0c; 32],
            ..RuntimeOptions::default()
        },
    )
    .unwrap_or_else(|e| panic!("conformance runtime ({class:?}): {e}"))
}

/// The shared runtime of a language class (compiled modules are cached per runtime).
fn runtime(lang: Lang) -> &'static Runtime {
    static RUST: OnceLock<Runtime> = OnceLock::new();
    static PY: OnceLock<Runtime> = OnceLock::new();
    static TS: OnceLock<Runtime> = OnceLock::new();
    let class = langs::class(lang);
    let cell = match class {
        LanguageClass::Rust => &RUST,
        LanguageClass::Python => &PY,
        LanguageClass::TypeScript => &TS,
    };
    cell.get_or_init(|| new_runtime(class))
}

type ModuleKey = (Lang, Option<String>);

/// Prepared module for a language and variant, compiled once per process (Pitfall 10).
async fn prepared(lang: Lang, variant: Option<&str>) -> Result<Arc<Prepared>, String> {
    static CACHE: OnceLock<Mutex<HashMap<ModuleKey, Arc<Prepared>>>> = OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    let key = (lang, variant.map(str::to_owned));
    if let Some(p) = cache.lock().unwrap_or_else(|p| p.into_inner()).get(&key) {
        return Ok(p.clone());
    }
    let wasm = langs::module_for(lang, variant)?;
    let p = runtime(lang)
        .prepare_cached(&wasm)
        .await
        .map_err(|e| format!("prepare {} module {variant:?}: {e}", langs::name(lang)))?;
    cache
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(key, p.clone());
    Ok(p)
}

// ---------------------------------------------------------------------------
// One execution
// ---------------------------------------------------------------------------

/// Result of one execution of a scenario (no checks applied).
#[derive(Debug, Clone)]
pub struct Execution {
    pub status: String,
    pub result: Option<Value>,
    pub failure_kind: Option<String>,
    /// Non-determinism message, for status `non_determinism`.
    pub nondeterminism: Option<String>,
    pub journals: Vec<Vec<Event>>,
    /// Run ref of each run.
    runs: Vec<RunRef>,
    /// Module each run ended on (replay target).
    modules: Vec<Arc<Prepared>>,
    pub activations: u32,
}

fn json_payload(v: &Value) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: serde_json::to_vec(v).unwrap_or_else(|_| b"null".to_vec()),
    }
}

fn payload_value(p: &Payload) -> Value {
    if p.data.is_empty() {
        return Value::Null;
    }
    serde_json::from_slice(&p.data)
        .unwrap_or_else(|_| Value::String(format!("<non-json payload, {} bytes>", p.data.len())))
}

fn run_seed(seed: u64, generation: u32) -> RngSeed {
    let mut b = [0u8; 32];
    b[..8].copy_from_slice(&seed.to_le_bytes());
    b[8..12].copy_from_slice(&generation.to_le_bytes());
    RngSeed(b)
}

/// Ops already completed (by an input event) or cancelled by an `OpCancelled`.
fn closed_ops(journal: &[Event]) -> HashSet<OpId> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::OpCancelled(c) => Some(c.op),
            b => b.completes_op(),
        })
        .collect()
}

/// Pending timers and wait timeouts: `(due, op, is_wait_timeout)`, earliest first.
fn pending_timers(journal: &[Event]) -> Vec<(u64, OpId, bool)> {
    let closed = closed_ops(journal);
    let mut out: Vec<(u64, OpId, bool)> = journal
        .iter()
        .filter(|e| !closed.contains(&e.seq))
        .filter_map(|e| {
            let timeout = |ms: Option<u64>| {
                ms.map(|ms| {
                    (
                        e.ts_nanos.saturating_add(ms.saturating_mul(1_000_000)),
                        e.seq,
                        true,
                    )
                })
            };
            match &e.body {
                EventBody::TimerStarted(t) => Some((t.fire_at_nanos, e.seq, false)),
                EventBody::SignalWaitStarted(w) => timeout(w.timeout_ms),
                EventBody::AwaitTokenCreated(t) => timeout(t.timeout_ms),
                _ => None,
            }
        })
        .collect();
    out.sort_unstable();
    out
}

/// `SignalMatched` bodies pairing open signal waits (oldest op first) with unmatched
/// buffered `SignalPayload` events of the same name (oldest first).
fn signal_matches(journal: &[Event]) -> Vec<EventBody> {
    let closed = closed_ops(journal);
    let used: HashSet<u64> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::SignalMatched(m) => Some(m.payload_seq),
            _ => None,
        })
        .collect();
    let mut free: VecDeque<(u64, &str)> = journal
        .iter()
        .filter(|e| !used.contains(&e.seq))
        .filter_map(|e| match &e.body {
            EventBody::SignalPayload(p) => Some((e.seq, p.name.as_str())),
            _ => None,
        })
        .collect();
    let mut out = Vec::new();
    for e in journal {
        let EventBody::SignalWaitStarted(w) = &e.body else {
            continue;
        };
        if closed.contains(&e.seq) {
            continue;
        }
        if let Some(i) = free.iter().position(|(_, n)| *n == w.name) {
            let (payload_seq, _) = free.remove(i).unwrap_or_default();
            out.push(EventBody::SignalMatched(SignalMatched {
                op: e.seq,
                payload_seq,
            }));
        }
    }
    out
}

/// Commits input bodies at the end of the run's journal, stamped `max(clock, last ts)`.
fn commit(sink: &FakeSink, run: &RunRef, bodies: Vec<EventBody>, ts: Option<u64>) -> usize {
    let journal = sink.journal(run);
    let last_ts = journal.last().map_or(0, |e| e.ts_nanos);
    let ts = ts.unwrap_or_else(|| clock().get()).max(last_ts);
    let base = journal.len() as u64;
    let n = bodies.len();
    let events = bodies
        .into_iter()
        .enumerate()
        .map(|(i, body)| Event {
            seq: base + i as u64,
            ts_nanos: ts,
            body,
        })
        .collect();
    sink.commit_inputs(run, events);
    n
}

/// Fires the earliest pending timer; false if there is none.
fn fire_earliest(sink: &FakeSink, run: &RunRef) -> bool {
    let Some(&(due, op, is_wait)) = pending_timers(&sink.journal(run)).first() else {
        return false;
    };
    clock().advance_to(due);
    let body = if is_wait {
        EventBody::WaitTimedOut(WaitTimedOut { op })
    } else {
        EventBody::TimerFired(TimerFired { op })
    };
    commit(sink, run, vec![body], Some(due));
    true
}

fn status_of_failure(f: &ostro_types::journal::Failure) -> &'static str {
    if *f == ostro_types::journal::Failure::Cancelled {
        "cancelled"
    } else {
        "failed"
    }
}

fn run_started(
    workflow: &str,
    input: Payload,
    seed: RngSeed,
    accepted: u64,
    module_hash: &str,
    continued_from: Option<String>,
) -> Event {
    Event {
        seq: 0,
        ts_nanos: accepted,
        body: EventBody::RunStarted(RunStarted {
            workflow: workflow.to_string(),
            workflow_version: 1,
            input,
            rng_seed: seed,
            accepted_at_nanos: accepted,
            deadline_nanos: None,
            module_hash: module_hash.to_string(),
            tenant: None,
            parent_run_id: None,
            continued_from,
        }),
    }
}

/// Runs a spec once for `lang` with `seed` (no strict or expectation checks).
pub fn execute(spec: &Spec, lang: Lang, seed: u64) -> Result<Execution, String> {
    let _guard = run_lock();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    rt.block_on(execute_async(spec, lang, seed))
}

async fn execute_async(spec: &Spec, lang: Lang, seed: u64) -> Result<Execution, String> {
    let rt = runtime(lang);
    let clock = clock();
    clock.set(T0);
    let base_run_id = spec.run_id.clone().unwrap_or_else(|| DEFAULT_RUN_ID.into());
    let mut workflow = langs::workflow_name(lang, &spec.workflow);
    let mut input = json_payload(&spec.input);
    let mut module = prepared(lang, spec.variant.as_deref()).await?;
    let mut script: VecDeque<&ScriptStep> = VecDeque::new();
    let mut before_start: Vec<EventBody> = Vec::new();
    for step in &spec.script {
        match step {
            ScriptStep::Signal {
                name,
                payload,
                before_start: true,
            } => before_start.push(EventBody::SignalPayload(SignalPayload {
                name: name.clone(),
                payload: json_payload(payload),
            })),
            ScriptStep::Api { .. } | ScriptStep::Kill { .. } => {
                return Err(format!("script step {step:?} is not an engine-leg step"));
            }
            other => script.push_back(other),
        }
    }
    let mut exec = Execution {
        status: String::new(),
        result: None,
        failure_kind: None,
        nondeterminism: None,
        journals: Vec::new(),
        runs: Vec::new(),
        modules: Vec::new(),
        activations: 0,
    };
    let mut continued_from: Option<String> = None;
    for generation in 0..MAX_CHAIN {
        let run = RunRef {
            partition: PARTITION,
            run_id: if generation == 0 {
                base_run_id.clone()
            } else {
                format!("{base_run_id}.{generation}")
            },
        };
        let sink = Arc::new(FakeSink::new());
        sink.seed(
            run.clone(),
            vec![run_started(
                &workflow,
                input.clone(),
                run_seed(seed, generation),
                clock.get(),
                &module.module_hash_hex,
                continued_from.take(),
            )],
            1,
        );
        if generation == 0 && !before_start.is_empty() {
            commit(&sink, &run, std::mem::take(&mut before_start), None);
        }
        let mut activations = 0u32;
        let outcome = loop {
            activations += 1;
            exec.activations += 1;
            if activations > MAX_ACTIVATIONS {
                return Err(format!(
                    "run {} exceeded {MAX_ACTIVATIONS} activations",
                    run.run_id
                ));
            }
            let outcome = rt
                .activate(
                    &module,
                    Activate {
                        run: run.clone(),
                        attempt: 1,
                        module_sha: module.sha,
                        journal: sink.journal(&run),
                        inputs: None,
                        cancel: Default::default(),
                    },
                    sink.clone(),
                )
                .await;
            match outcome {
                ActivationOutcome::Evicted { reason } => match reason {
                    EvictReason::StepBackoff { retry_at_nanos } => {
                        clock.advance_to(retry_at_nanos);
                    }
                    EvictReason::WaitUnsatisfied | EvictReason::HotWaitElapsed => {
                        let matches = signal_matches(&sink.journal(&run));
                        if !matches.is_empty() {
                            commit(&sink, &run, matches, None);
                            continue;
                        }
                        if deliver_next(&mut script, &sink, &run, lang, &mut module).await? {
                            continue;
                        }
                        if fire_earliest(&sink, &run) {
                            continue;
                        }
                        let tail: Vec<String> = sink
                            .journal(&run)
                            .iter()
                            .map(|e| e.body.summary())
                            .collect();
                        return Err(format!(
                            "run {} is stuck: waiting with no deliverable script step and no \
                             pending timer; next step {:?}; journal {tail:?}",
                            run.run_id,
                            script.front()
                        ));
                    }
                    _ => {}
                },
                other => break other,
            }
        };
        exec.journals.push(sink.journal(&run));
        exec.runs.push(run.clone());
        exec.modules.push(module.clone());
        match outcome {
            ActivationOutcome::Completed { output } => {
                exec.status = "completed".into();
                exec.result = Some(payload_value(&output));
                return Ok(exec);
            }
            ActivationOutcome::Failed { failure } => {
                exec.status = status_of_failure(&failure).into();
                exec.failure_kind = Some(projection::failure_kind(&failure));
                return Ok(exec);
            }
            ActivationOutcome::ContinuedAsNew { continuation } => {
                if let Some(w) = continuation.workflow {
                    workflow = w;
                }
                input = continuation.input;
                continued_from = Some(run.run_id.clone());
            }
            ActivationOutcome::AttemptFailed(AttemptFailure::NonDeterminism(nd)) => {
                exec.status = "non_determinism".into();
                exec.nondeterminism = Some(nd.to_string());
                return Ok(exec);
            }
            other => {
                return Err(format!("run {} ended with {other:?}", run.run_id));
            }
        }
    }
    Err(format!(
        "continue-as-new chain longer than {MAX_CHAIN} runs"
    ))
}

/// Delivers the next script step if its precondition holds; true if it did.
async fn deliver_next(
    script: &mut VecDeque<&ScriptStep>,
    sink: &FakeSink,
    run: &RunRef,
    lang: Lang,
    module: &mut Arc<Prepared>,
) -> Result<bool, String> {
    let Some(step) = script.front().copied() else {
        return Ok(false);
    };
    let journal = sink.journal(run);
    let delivered = match step {
        ScriptStep::Signal { name, payload, .. } => {
            commit(
                sink,
                run,
                vec![EventBody::SignalPayload(SignalPayload {
                    name: name.clone(),
                    payload: json_payload(payload),
                })],
                None,
            );
            let matches = signal_matches(&sink.journal(run));
            commit(sink, run, matches, None);
            true
        }
        ScriptStep::Token { payload } => {
            let closed = closed_ops(&journal);
            let open = journal.iter().find(|e| {
                matches!(e.body, EventBody::AwaitTokenCreated(_)) && !closed.contains(&e.seq)
            });
            match open {
                Some(e) => {
                    let op = e.seq;
                    commit(
                        sink,
                        run,
                        vec![EventBody::TokenCompleted(TokenCompleted {
                            op,
                            payload: json_payload(payload),
                        })],
                        None,
                    );
                    true
                }
                None => false,
            }
        }
        ScriptStep::Cancel {} => {
            commit(
                sink,
                run,
                vec![EventBody::CancelRequested(CancelRequested {
                    reason: "conformance".into(),
                })],
                None,
            );
            true
        }
        ScriptStep::FireTimers {} => {
            let mut any = false;
            while fire_earliest(sink, run) {
                any = true;
            }
            any
        }
        ScriptStep::SwapModule { variant } => {
            *module = prepared(lang, Some(variant)).await?;
            true
        }
        ScriptStep::Api { .. } | ScriptStep::Kill { .. } => {
            return Err(format!("script step {step:?} is not an engine-leg step"));
        }
    };
    if delivered {
        script.pop_front();
    }
    Ok(delivered)
}

// ---------------------------------------------------------------------------
// Checks
// ---------------------------------------------------------------------------

/// Replays every journal of `exec` on the module its run ended on; each must append 0
/// events. Skipped for a non-determinism ending (that journal does not replay by design).
fn strict_replay(exec: &Execution, lang: Lang) -> Result<(), String> {
    if exec.status == "non_determinism" {
        return Ok(());
    }
    let _guard = run_lock();
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    let rt = runtime(lang);
    for ((journal, run), module) in exec.journals.iter().zip(&exec.runs).zip(&exec.modules) {
        clock().set(T0);
        let (outcome, appended) =
            tokio_rt.block_on(replay_journal(rt, module, run.clone(), journal.clone()));
        if appended != 0 {
            return Err(format!(
                "replay of run {} appended {appended} events (outcome {outcome:?})",
                run.run_id
            ));
        }
        if matches!(outcome, ActivationOutcome::AttemptFailed(_)) {
            return Err(format!("replay of run {} failed: {outcome:?}", run.run_id));
        }
    }
    Ok(())
}

fn spec_seed(spec: &Spec) -> Result<u64, String> {
    spec.seed.as_deref().map_or(Ok(DEFAULT_SEED), |s| {
        s.parse::<u64>()
            .map_err(|e| format!("seed {s:?} is not a decimal u64: {e}"))
    })
}

/// Runs a spec for `lang`: two executions (byte identity), strict replay, then the
/// expectations (status, canonical result, failure kind, projection, extras).
pub fn run_spec(spec: &Spec, lang: Lang) -> Result<RunReport, Failure> {
    let fail = |message: String| Failure {
        id: spec.id.clone(),
        lang,
        message,
    };
    let started = Instant::now();
    let seed = spec_seed(spec).map_err(fail)?;
    let first = execute(spec, lang, seed).map_err(fail)?;
    let projection = projection::project_chain(&first.journals);
    let context = || {
        format!(
            "\n  status {} result {} failure_kind {:?}\n  projection {:?}\n  values {:?}\n  inputs {:?}{}",
            first.status,
            first
                .result
                .as_ref()
                .map_or_else(|| "-".to_string(), canon::canonical),
            first.failure_kind,
            projection.facts,
            projection.values,
            projection.inputs,
            first
                .nondeterminism
                .as_ref()
                .map(|m| format!("\n  nondeterminism: {m}"))
                .unwrap_or_default()
        )
    };
    // Strict 1: same seed and run id twice -> byte-identical journals.
    let second = execute(spec, lang, seed).map_err(fail)?;
    if first.journals.len() != second.journals.len() {
        return Err(fail(format!(
            "second execution produced {} runs, first {}",
            second.journals.len(),
            first.journals.len()
        )));
    }
    for (a, b) in first.journals.iter().zip(&second.journals) {
        compare::journals_identical(a, b).map_err(|d| fail(d.to_string()))?;
    }
    // Strict 2: replay appends nothing.
    strict_replay(&first, lang).map_err(|e| fail(format!("{e}{}", context())))?;
    // Expectations.
    let exp = &spec.expect;
    if exp.status != first.status {
        return Err(fail(format!(
            "status: expected {}, actual {}{}",
            exp.status,
            first.status,
            context()
        )));
    }
    if let Some(want) = &exp.result {
        let got = first.result.clone().unwrap_or(Value::Null);
        compare::outcomes("result", want, &got).map_err(|d| fail(format!("{d}{}", context())))?;
    }
    if let Some(want) = &exp.failure_kind
        && first.failure_kind.as_deref() != Some(want.as_str())
    {
        return Err(fail(format!(
            "failure_kind: expected {want}, actual {:?}{}",
            first.failure_kind,
            context()
        )));
    }
    if let Some(want) = &exp.projection {
        compare::facts(want, &projection.facts).map_err(|d| fail(format!("{d}{}", context())))?;
    }
    for (key, value) in &exp.extra {
        match key.as_str() {
            "differs_across_seeds" => {
                if *value != Value::Bool(true) {
                    continue;
                }
                let other = execute(spec, lang, seed.wrapping_add(1)).map_err(fail)?;
                let (a, b) = (
                    first.result.as_ref().map(canon::canonical),
                    other.result.as_ref().map(canon::canonical),
                );
                if a.is_none() || a == b {
                    return Err(fail(format!(
                        "differs_across_seeds: seed {seed} and {} gave the same result {a:?}",
                        seed.wrapping_add(1)
                    )));
                }
            }
            other => {
                return Err(fail(format!(
                    "unknown expect key {other:?} (engine leg knows differs_across_seeds)"
                )));
            }
        }
    }
    Ok(RunReport {
        id: spec.id.clone(),
        lang,
        status: first.status,
        result: first.result,
        failure_kind: first.failure_kind,
        projection,
        journals: first.journals,
        activations: first.activations,
        elapsed: started.elapsed(),
    })
}

/// Cross-language check: `other` against the Rust reference report. Value probes whose
/// values are per language (C-NATIVE-01) compare facts and inputs only.
pub fn cross_compare(reference: &RunReport, other: &RunReport) -> Result<(), Failure> {
    let fail = |message: String| Failure {
        id: other.id.clone(),
        lang: other.lang,
        message,
    };
    let per_language_values = reference.id == "C-NATIVE-01";
    if reference.status != other.status {
        return Err(fail(format!(
            "status {} vs reference {}",
            other.status, reference.status
        )));
    }
    if !per_language_values {
        let null = Value::Null;
        compare::outcomes(
            "result vs reference",
            reference.result.as_ref().unwrap_or(&null),
            other.result.as_ref().unwrap_or(&null),
        )
        .map_err(|d| fail(d.to_string()))?;
    }
    compare::projections(
        &reference.projection,
        &other.projection,
        compare::Opts {
            values: !per_language_values,
        },
    )
    .map_err(|d| fail(d.to_string()))
}
