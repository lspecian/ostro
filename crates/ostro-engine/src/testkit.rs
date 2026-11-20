//! In-memory fake [`ActivationSink`] and drive helpers (feature `testkit`, plan 03-08).
//!
//! [`FakeSink`] is the test double for the Phase 2 partition leader: it keeps one
//! journal per run, enforces attempt fencing and contiguous seqs, and can be scripted
//! to fence or conflict on the nth append. [`drive_to_completion`] runs a workflow
//! through evict/replay cycles, delivering scripted inputs (timer fires, effect /
//! child / token completions, signals, cancellation) between activations, exactly as
//! the partition leader would commit them. [`replay_journal`] re-runs a recorded
//! journal and counts what it appends (0 for a faithful replay of a terminal journal).
//!
//! With [`Script::live`] the sink also delivers inputs *live* (hot wait, plan 03-11):
//! each activation gets an input channel, and every `TimerStarted` / `EffectScheduled`
//! the sink accepts schedules its scripted completion after the given delay. The
//! completion is committed to the journal (seq = journal length) and sent to the open
//! activation, or left in the journal for the next activation if none is open.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ostro_types::journal::{
    CancelRequested, ChildCompleted, EffectCompleted, EffectRequest, ErrorInfo, Event, EventBody,
    Failure, OpId, OpOutput, Payload, RngSeed, RunStarted, SignalReceived, TimerFired,
    TokenCompleted, encode_result,
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

use crate::effects_driver::EffectDriver;
use crate::env::Clock;
use crate::error::EvictReason;
use crate::runtime::{Prepared, Runtime};
use crate::sink::{Activate, ActivationOutcome, ActivationSink, AppendError, RunRef};

/// Default `accepted_at` of a [`RunSpec`] (2023-11-14T22:13:20Z).
pub const DEFAULT_ACCEPTED_AT: u64 = 1_700_000_000_000_000_000;

/// Deterministic [`Clock`]: starts at a given time and advances `step_nanos` per call
/// (default 1 ms), so live timestamps and therefore journals are byte-reproducible.
#[derive(Debug)]
pub struct FakeClock {
    now: AtomicU64,
    step: u64,
}

impl FakeClock {
    /// Starts at `start_nanos`, +1 ms per call.
    pub fn new(start_nanos: u64) -> Self {
        Self::with_step(start_nanos, 1_000_000)
    }

    /// Starts at `start_nanos`, +`step_nanos` per call (0 = frozen clock: live
    /// timestamps then depend only on the journal, not on how many activations ran).
    pub fn with_step(start_nanos: u64, step_nanos: u64) -> Self {
        Self {
            now: AtomicU64::new(start_nanos),
            step: step_nanos,
        }
    }
}

impl Clock for FakeClock {
    fn now_unix_nanos(&self) -> u64 {
        self.now.fetch_add(self.step, Ordering::SeqCst)
    }
}

/// What to start: the `RunStarted` fields of a test run.
#[derive(Debug, Clone)]
pub struct RunSpec {
    pub run: RunRef,
    pub workflow: String,
    pub input: Payload,
    pub seed: RngSeed,
    pub accepted_at_nanos: u64,
    pub deadline_nanos: Option<u64>,
}

impl RunSpec {
    /// A run of `workflow` with `input` bytes (`application/octet-stream`), seed
    /// `[0x5a; 32]`, accepted at [`DEFAULT_ACCEPTED_AT`], no deadline.
    pub fn new(run: RunRef, workflow: impl Into<String>, input: &[u8]) -> Self {
        Self {
            run,
            workflow: workflow.into(),
            input: Payload {
                content_type: "application/octet-stream".to_string(),
                data: input.to_vec(),
            },
            seed: RngSeed([0x5a; 32]),
            accepted_at_nanos: DEFAULT_ACCEPTED_AT,
            deadline_nanos: None,
        }
    }

    /// The run's `RunStarted` event (seq 0).
    pub fn run_started_event(&self, module_hash_hex: &str) -> Event {
        Event {
            seq: 0,
            ts_nanos: self.accepted_at_nanos,
            body: EventBody::RunStarted(RunStarted {
                workflow: self.workflow.clone(),
                workflow_version: 1,
                input: self.input.clone(),
                rng_seed: self.seed,
                accepted_at_nanos: self.accepted_at_nanos,
                deadline_nanos: self.deadline_nanos,
                module_hash: module_hash_hex.to_string(),
                tenant: None,
                parent_run_id: None,
                continued_from: None,
            }),
        }
    }
}

/// Completes an effect request.
pub type EffectFn = Arc<dyn Fn(&EffectRequest) -> Result<OpOutput, Failure> + Send + Sync>;
/// Completes a child run from `(workflow, input)`.
pub type ChildFn = Arc<dyn Fn(&str, &Payload) -> Result<OpOutput, Failure> + Send + Sync>;

/// One scripted behaviour of the fake partition leader.
#[derive(Clone)]
pub enum Rule {
    /// Complete every scheduled effect with this function.
    CompleteEffects(EffectFn),
    /// Deliver `payload` once to the first unanswered wait on signal `name`.
    Signal { name: String, payload: Payload },
    /// Complete every await token with this payload.
    CompleteTokens(Payload),
    /// Complete every child run with this function.
    CompleteChildren(ChildFn),
    /// Deliver `CancelRequested` after the nth activation (1-based).
    CancelAfterActivations(u32),
    /// Fence the nth append (1-based, counted across the sink).
    FenceOnAppend(u32),
    /// Report a seq conflict on the nth append (1-based, counted across the sink).
    ConflictOnAppend(u32),
}

impl std::fmt::Debug for Rule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CompleteEffects(_) => f.write_str("CompleteEffects(..)"),
            Self::Signal { name, .. } => write!(f, "Signal({name})"),
            Self::CompleteTokens(_) => f.write_str("CompleteTokens(..)"),
            Self::CompleteChildren(_) => f.write_str("CompleteChildren(..)"),
            Self::CancelAfterActivations(n) => write!(f, "CancelAfterActivations({n})"),
            Self::FenceOnAppend(n) => write!(f, "FenceOnAppend({n})"),
            Self::ConflictOnAppend(n) => write!(f, "ConflictOnAppend({n})"),
        }
    }
}

/// Scripted inputs for [`drive_to_completion`].
///
/// Defaults when no rule matches: started timers fire on eviction (if `fire_timers`),
/// effects and children complete with a non-retryable `unsupported` failure (the
/// effect bridge lands in Phase 6), signals and tokens stay pending.
#[derive(Debug, Clone)]
pub struct Script {
    pub rules: Vec<Rule>,
    /// Stop after this many activations (default 100).
    pub max_activations: u32,
    /// Fire every started timer on eviction (default true).
    pub fire_timers: bool,
    /// Live input delivery: completions of accepted timers/effects are committed and
    /// sent to the open activation after this delay (default `None` = off).
    pub live: Option<Duration>,
}

impl Default for Script {
    fn default() -> Self {
        Self {
            rules: Vec::new(),
            max_activations: 100,
            fire_timers: true,
            live: None,
        }
    }
}

impl Script {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rule(mut self, r: Rule) -> Self {
        self.rules.push(r);
        self
    }

    pub fn max_activations(mut self, n: u32) -> Self {
        self.max_activations = n;
        self
    }

    pub fn fire_timers(mut self, on: bool) -> Self {
        self.fire_timers = on;
        self
    }

    /// Deliver timer fires and effect completions live, `delay` after the sink accepts
    /// the scheduling event (needs a hot-wait-capable runtime to resume in-activation).
    pub fn live(mut self, delay: Duration) -> Self {
        self.live = Some(delay);
        self
    }

    fn effect_fn(&self) -> Option<&EffectFn> {
        self.rules.iter().find_map(|r| match r {
            Rule::CompleteEffects(f) => Some(f),
            _ => None,
        })
    }

    fn child_fn(&self) -> Option<&ChildFn> {
        self.rules.iter().find_map(|r| match r {
            Rule::CompleteChildren(f) => Some(f),
            _ => None,
        })
    }

    fn token_payload(&self) -> Option<&Payload> {
        self.rules.iter().find_map(|r| match r {
            Rule::CompleteTokens(p) => Some(p),
            _ => None,
        })
    }

    fn cancel_after(&self) -> Option<u32> {
        self.rules.iter().find_map(|r| match r {
            Rule::CancelAfterActivations(n) => Some(*n),
            _ => None,
        })
    }
}

fn unsupported() -> Failure {
    Failure::NonRetryable(ErrorInfo {
        kind: "unsupported".to_string(),
        message: "effect bridge lands in Phase 6".to_string(),
        details: None,
    })
}

/// Mutable state of a [`FakeSink`].
#[derive(Debug, Default)]
pub struct FakeState {
    pub journals: HashMap<RunRef, Vec<Event>>,
    pub current_attempt: HashMap<RunRef, u32>,
    /// Accepted appends: `(run, from_seq, event count)`.
    pub batches: Vec<(RunRef, u64, usize)>,
    /// Every `done` report, in order.
    pub done: Vec<(RunRef, u32, ActivationOutcome)>,
    /// Appends attempted (accepted or not).
    pub append_count: u32,
    fence_on: Vec<u32>,
    conflict_on: Vec<u32>,
    /// Live delivery script (when [`Script::live`] is set).
    live: Option<Script>,
    /// Input channel of the open activation per run (live delivery).
    senders: HashMap<RunRef, UnboundedSender<Event>>,
}

/// In-memory [`ActivationSink`]: the Phase 3 stand-in for the partition leader.
///
/// [`FakeSink::with_effect_driver`] attaches an [`EffectDriver`] (06-06); without one
/// `effect_driver()` is `None` and nothing is ever started (Phase 4/5 behaviour).
#[derive(Default)]
pub struct FakeSink {
    state: Arc<Mutex<FakeState>>,
    driver: Option<Arc<dyn EffectDriver>>,
}

impl std::fmt::Debug for FakeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeSink")
            .field("state", &self.state)
            .field("effect_driver", &self.driver.is_some())
            .finish()
    }
}

fn lock_state(state: &Mutex<FakeState>) -> MutexGuard<'_, FakeState> {
    state.lock().unwrap_or_else(|p| p.into_inner())
}

/// Commit the live completion of `op` (unless already answered) and send it to the
/// run's open activation.
fn commit_live(state: &Mutex<FakeState>, run: &RunRef, op: OpId) {
    let mut st = lock_state(state);
    let Some(script) = st.live.clone() else {
        return;
    };
    let Some(journal) = st.journals.get_mut(run) else {
        return;
    };
    if journal.iter().any(|e| e.body.completes_op() == Some(op)) {
        return;
    }
    let last_ts = journal.last().map_or(0, |e| e.ts_nanos);
    let (ts_nanos, body) = match journal.get(op as usize).map(|e| &e.body) {
        Some(EventBody::TimerStarted(t)) => (
            t.fire_at_nanos.max(last_ts),
            EventBody::TimerFired(TimerFired { op }),
        ),
        Some(EventBody::EffectScheduled(e)) => {
            let out = script
                .effect_fn()
                .map_or_else(|| Err(unsupported()), |f| f(&e.request));
            (
                last_ts,
                EventBody::EffectCompleted(EffectCompleted {
                    op,
                    outcome: encode_result(&out),
                }),
            )
        }
        _ => return,
    };
    let ev = Event {
        seq: journal.len() as u64,
        ts_nanos,
        body,
    };
    journal.push(ev.clone());
    if let Some(tx) = st.senders.get(run) {
        // A closed channel (activation ended) leaves the input for the next one.
        let _ = tx.send(ev);
    }
}

impl FakeSink {
    pub fn new() -> Self {
        Self::default()
    }

    /// A sink honouring the script's `FenceOnAppend` / `ConflictOnAppend` rules.
    pub fn from_script(script: &Script) -> Self {
        let sink = Self::new();
        {
            let mut st = sink.lock();
            for r in &script.rules {
                match r {
                    Rule::FenceOnAppend(n) => st.fence_on.push(*n),
                    Rule::ConflictOnAppend(n) => st.conflict_on.push(*n),
                    _ => {}
                }
            }
            if script.live.is_some() {
                st.live = Some(script.clone());
            }
        }
        sink
    }

    /// Attach an effect driver: `effect_driver()` returns it (06-06).
    pub fn with_effect_driver(mut self, driver: Arc<dyn EffectDriver>) -> Self {
        self.driver = Some(driver);
        self
    }

    fn lock(&self) -> MutexGuard<'_, FakeState> {
        lock_state(&self.state)
    }

    /// Open a live input channel for `run`'s next activation; returns the committed
    /// journal and the receiver, atomically (no input can fall between the two).
    pub fn open_live(&self, run: &RunRef) -> (Vec<Event>, UnboundedReceiver<Event>) {
        let (tx, rx) = unbounded_channel();
        let mut st = self.lock();
        st.senders.insert(run.clone(), tx);
        (st.journals.get(run).cloned().unwrap_or_default(), rx)
    }

    /// Close `run`'s live input channel (the activation ended).
    pub fn close_live(&self, run: &RunRef) {
        self.lock().senders.remove(run);
    }

    /// Compute and commit the scripted inputs for `run`'s open ops atomically (so a
    /// concurrent live completion can neither duplicate nor reorder them); returns how
    /// many were committed.
    fn commit_scripted(
        &self,
        run: &RunRef,
        script: &Script,
        activations: u32,
        signals_used: &mut [bool],
    ) -> usize {
        let mut st = self.lock();
        let j = st.journals.entry(run.clone()).or_default();
        let inputs = scripted_inputs(j, script, activations, signals_used);
        let n = inputs.len();
        j.extend(inputs);
        n
    }

    /// Install `journal` (committed) for `run`, owned by `attempt`.
    pub fn seed(&self, run: RunRef, journal: Vec<Event>, attempt: u32) {
        let mut st = self.lock();
        st.current_attempt.insert(run.clone(), attempt);
        st.journals.insert(run, journal);
    }

    /// Grant the run to a new attempt (older attempts are fenced).
    pub fn set_attempt(&self, run: &RunRef, attempt: u32) {
        self.lock().current_attempt.insert(run.clone(), attempt);
    }

    /// Commit input events (as the leader would); their seqs must continue the journal.
    pub fn commit_inputs(&self, run: &RunRef, events: Vec<Event>) {
        let mut st = self.lock();
        let j = st.journals.entry(run.clone()).or_default();
        for ev in events {
            assert_eq!(
                ev.seq,
                j.len() as u64,
                "input seq must continue the journal"
            );
            j.push(ev);
        }
    }

    /// The committed journal of `run`.
    pub fn journal(&self, run: &RunRef) -> Vec<Event> {
        self.lock().journals.get(run).cloned().unwrap_or_default()
    }

    /// Accepted appends: `(run, from_seq, event count)`.
    pub fn batches(&self) -> Vec<(RunRef, u64, usize)> {
        self.lock().batches.clone()
    }

    /// Every `done` report, in order.
    pub fn outcomes(&self) -> Vec<(RunRef, u32, ActivationOutcome)> {
        self.lock().done.clone()
    }

    /// Number of events accepted through `append` for `run`.
    pub fn appended_events(&self, run: &RunRef) -> usize {
        self.lock()
            .batches
            .iter()
            .filter(|(r, _, _)| r == run)
            .map(|(_, _, n)| n)
            .sum()
    }

    /// Appends attempted so far (accepted or not).
    pub fn append_count(&self) -> u32 {
        self.lock().append_count
    }
}

#[async_trait::async_trait]
impl ActivationSink for FakeSink {
    async fn append(
        &self,
        run: &RunRef,
        attempt: u32,
        from_seq: u64,
        events: Vec<Event>,
    ) -> Result<u64, AppendError> {
        let mut st = self.lock();
        st.append_count += 1;
        let n = st.append_count;
        if st.current_attempt.get(run).copied().unwrap_or(attempt) != attempt
            || st.fence_on.contains(&n)
        {
            return Err(AppendError::Fenced);
        }
        let len = st.journals.get(run).map_or(0, Vec::len) as u64;
        if st.conflict_on.contains(&n) || from_seq != len {
            return Err(AppendError::Conflict { expected: len });
        }
        if events
            .iter()
            .enumerate()
            .any(|(i, e)| e.seq != from_seq + i as u64)
        {
            return Err(AppendError::Unavailable("bad seqs".to_string()));
        }
        let count = events.len();
        if let Some(script) = &st.live {
            let delay = script.live.unwrap_or_default();
            for e in &events {
                let deliver = match &e.body {
                    EventBody::TimerStarted(_) => script.fire_timers,
                    EventBody::EffectScheduled(_) => true,
                    _ => false,
                };
                if deliver {
                    let (state, run, op) = (self.state.clone(), run.clone(), e.seq);
                    tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        commit_live(&state, &run, op);
                    });
                }
            }
        }
        st.journals.entry(run.clone()).or_default().extend(events);
        st.batches.push((run.clone(), from_seq, count));
        Ok(from_seq + count as u64)
    }

    async fn fetch_blob(&self, _sha: &[u8; 32]) -> Result<Vec<u8>, AppendError> {
        Err(AppendError::Unavailable(
            "FakeSink stores no blobs".to_string(),
        ))
    }

    async fn done(&self, run: &RunRef, attempt: u32, outcome: ActivationOutcome) {
        self.lock().done.push((run.clone(), attempt, outcome));
    }

    fn effect_driver(&self) -> Option<Arc<dyn EffectDriver>> {
        self.driver.clone()
    }
}

/// Result of [`drive_to_completion`].
#[derive(Debug, Clone)]
pub struct Drive {
    /// Outcome of the last activation.
    pub outcome: ActivationOutcome,
    /// Final committed journal.
    pub journal: Vec<Event>,
    pub activations: u32,
    /// Appends accepted by the sink.
    pub appended_batches: usize,
    /// JSON of the last `StatusPublished` in the journal (WIT-05).
    pub latest_status: Option<String>,
    /// Every `done` report, in order.
    pub outcomes: Vec<ActivationOutcome>,
}

/// Scripted input events for the ops left open in `journal`.
fn scripted_inputs(
    journal: &[Event],
    script: &Script,
    activations: u32,
    signals_used: &mut [bool],
) -> Vec<Event> {
    let answered: HashSet<OpId> = journal
        .iter()
        .filter_map(|e| e.body.completes_op())
        .collect();
    let cancel_seen = journal
        .iter()
        .any(|e| matches!(e.body, EventBody::CancelRequested(_)));
    let last_ts = journal.last().map_or(0, |e| e.ts_nanos);
    let mut bodies: Vec<(u64, EventBody)> = Vec::new();
    for ev in journal {
        let op = ev.seq;
        if answered.contains(&op) {
            continue;
        }
        match &ev.body {
            EventBody::TimerStarted(t) if script.fire_timers => {
                bodies.push((
                    t.fire_at_nanos.max(last_ts),
                    EventBody::TimerFired(TimerFired { op }),
                ));
            }
            EventBody::EffectScheduled(e) => {
                let out = script
                    .effect_fn()
                    .map_or_else(|| Err(unsupported()), |f| f(&e.request));
                bodies.push((
                    last_ts,
                    EventBody::EffectCompleted(EffectCompleted {
                        op,
                        outcome: encode_result(&out),
                    }),
                ));
            }
            EventBody::SignalWaitStarted(w) => {
                let hit = script.rules.iter().enumerate().find(|(i, r)| {
                    !signals_used[*i] && matches!(r, Rule::Signal { name, .. } if *name == w.name)
                });
                if let Some((i, Rule::Signal { payload, .. })) = hit {
                    signals_used[i] = true;
                    bodies.push((
                        last_ts,
                        EventBody::SignalReceived(SignalReceived {
                            op,
                            payload: payload.clone(),
                        }),
                    ));
                }
            }
            EventBody::AwaitTokenCreated(_) => {
                if let Some(p) = script.token_payload() {
                    bodies.push((
                        last_ts,
                        EventBody::TokenCompleted(TokenCompleted {
                            op,
                            payload: p.clone(),
                        }),
                    ));
                }
            }
            EventBody::ChildStarted(c) => {
                let out = script
                    .child_fn()
                    .map_or_else(|| Err(unsupported()), |f| f(&c.workflow, &c.input));
                bodies.push((
                    last_ts,
                    EventBody::ChildCompleted(ChildCompleted {
                        op,
                        outcome: encode_result(&out),
                    }),
                ));
            }
            _ => {}
        }
    }
    if !cancel_seen && script.cancel_after() == Some(activations) {
        bodies.push((
            last_ts,
            EventBody::CancelRequested(CancelRequested {
                reason: "scripted cancel".to_string(),
            }),
        ));
    }
    let base = journal.len() as u64;
    bodies
        .into_iter()
        .enumerate()
        .map(|(i, (ts_nanos, body))| Event {
            seq: base + i as u64,
            ts_nanos,
            body,
        })
        .collect()
}

fn latest_status(journal: &[Event]) -> Option<String> {
    journal.iter().rev().find_map(|e| match &e.body {
        EventBody::StatusPublished(s) => Some(s.json.clone()),
        _ => None,
    })
}

/// Drive a run to a terminal outcome through evict/replay cycles on a fresh
/// [`FakeSink`] (attempt 1).
///
/// After each `Evicted` activation the scripted inputs for the open ops are committed
/// and the run is re-activated over the full journal. Stops on any non-evicted
/// outcome, when a `WaitUnsatisfied` eviction gets no new inputs (stuck), or after
/// `script.max_activations`. Conflict / external evictions re-activate without inputs.
/// With [`Script::live`] each activation gets a live input channel (hot wait).
pub async fn drive_to_completion(
    rt: &Runtime,
    p: &Prepared,
    spec: RunSpec,
    script: Script,
) -> Drive {
    let sink = Arc::new(FakeSink::from_script(&script));
    let run = spec.run.clone();
    sink.seed(
        run.clone(),
        vec![spec.run_started_event(&p.module_hash_hex)],
        1,
    );
    let mut signals_used = vec![false; script.rules.len()];
    let mut activations = 0u32;
    let outcome = loop {
        activations += 1;
        let (journal, inputs) = if script.live.is_some() {
            let (journal, rx) = sink.open_live(&run);
            (journal, Some(rx))
        } else {
            (sink.journal(&run), None)
        };
        let start_len = journal.len();
        let outcome = rt
            .activate(
                p,
                Activate {
                    run: run.clone(),
                    attempt: 1,
                    module_sha: p.sha,
                    journal,
                    inputs,
                    cancel: CancellationToken::new(),
                },
                sink.clone(),
            )
            .await;
        sink.close_live(&run);
        let ActivationOutcome::Evicted { reason } = outcome else {
            break outcome;
        };
        if activations >= script.max_activations {
            break outcome;
        }
        let committed = sink.commit_scripted(&run, &script, activations, &mut signals_used);
        // Live completions committed since this activation started also count as news.
        let live_news = sink
            .journal(&run)
            .get(start_len..)
            .is_some_and(|tail| tail.iter().any(|e| e.body.completes_op().is_some()));
        if committed == 0 && !live_news && reason == EvictReason::WaitUnsatisfied {
            break outcome;
        }
    };
    let journal = sink.journal(&run);
    Drive {
        outcome,
        latest_status: latest_status(&journal),
        journal,
        activations,
        appended_batches: sink.batches().len(),
        outcomes: sink.outcomes().into_iter().map(|(_, _, o)| o).collect(),
    }
}

/// Run one activation over `journal` (attempt 1, fresh [`FakeSink`] pre-seeded with
/// it); returns the outcome and the number of events appended.
pub async fn replay_journal(
    rt: &Runtime,
    p: &Prepared,
    run: RunRef,
    journal: Vec<Event>,
) -> (ActivationOutcome, usize) {
    let sink = Arc::new(FakeSink::new());
    sink.seed(run.clone(), journal.clone(), 1);
    let outcome = rt
        .activate(
            p,
            Activate {
                run: run.clone(),
                attempt: 1,
                module_sha: p.sha,
                journal,
                inputs: None,
                cancel: CancellationToken::new(),
            },
            sink.clone(),
        )
        .await;
    (outcome, sink.appended_events(&run))
}
