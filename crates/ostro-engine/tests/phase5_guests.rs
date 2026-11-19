//! Phase 5 test guests on the engine (plan 05-10): `race-fuzz`, `signals`, `timeouts`,
//! `tokens`, `can-chain`, `crash-saga` and `trivial-steps` (all built with the Rust SDK
//! for wasm32-wasip2) validate, describe, and behave as their module docs specify.
//!
//! Inputs are committed by hand, in an explicit order, by a small evict/re-activate driver
//! (not the testkit's default scripting). Signals use the D21 shape apply produces from
//! Phase 5 on: a `SignalPayload{name, payload}` event followed (in the same batch, or in a
//! later one for a buffered signal) by `SignalMatched{op, payload_seq}`. The legacy
//! `SignalReceived` is never fed. Wait timeouts are `WaitTimedOut{op}` (D4). Every
//! recorded journal replays to the same outcome with zero appends.

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock};

use ostro_engine::config::EngineConfig;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeClock, FakeSink, RunSpec, replay_journal};
use ostro_types::journal::{
    CancelRequested, Event, EventBody, EventKind, Failure, OpId, Payload, SignalMatched,
    SignalPayload, TimerFired, TokenCompleted, WaitTimedOut,
};
use tokio_util::sync::CancellationToken;

const PARTITION: u16 = 6;
const RUN_ID: &str = "run-phase5-1";

fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        Runtime::new(
            EngineConfig::default(),
            RuntimeOptions {
                // Frozen clock: journals depend only on the inputs fed.
                clock: Arc::new(FakeClock::with_step(DEFAULT_ACCEPTED_AT, 0)),
                token_key: [7; 32],
                ..RuntimeOptions::default()
            },
        )
        .expect("runtime")
    })
}

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

fn run() -> RunRef {
    RunRef {
        partition: PARTITION,
        run_id: RUN_ID.to_string(),
    }
}

fn spec(workflow: &str, input: &str) -> RunSpec {
    RunSpec::new(run(), workflow, input.as_bytes())
}

fn json(data: &str) -> Payload {
    Payload {
        content_type: "application/json".to_string(),
        data: data.as_bytes().to_vec(),
    }
}

/// One activation over `journal`; returns the outcome and the full journal afterwards.
async fn activate(p: &Prepared, journal: Vec<Event>) -> (ActivationOutcome, Vec<Event>) {
    let sink = Arc::new(FakeSink::new());
    sink.seed(run(), journal.clone(), 1);
    let outcome = rt()
        .activate(
            p,
            Activate {
                run: run(),
                attempt: 1,
                module_sha: p.sha,
                journal,
                inputs: None,
                cancel: CancellationToken::new(),
            },
            sink.clone(),
        )
        .await;
    (outcome, sink.journal(&run()))
}

/// An input the driver commits.
#[derive(Debug, Clone)]
enum In {
    /// `TimerFired{op}`.
    Fire(OpId),
    /// `SignalPayload{name, payload}` + `SignalMatched{op, payload_seq}` (same batch).
    Signal {
        op: OpId,
        name: String,
        payload: String,
    },
    /// `SignalPayload{name, payload}` alone (buffered; nothing waits yet).
    Buffer { name: String, payload: String },
    /// `SignalMatched{op, payload_seq}` for an earlier buffered `SignalPayload`.
    Match { op: OpId, payload_seq: u64 },
    /// `WaitTimedOut{op}`.
    TimedOut(OpId),
    /// `TokenCompleted{op, payload}`.
    Token { op: OpId, payload: String },
    /// `CancelRequested`.
    Cancel,
}

/// Append `inputs` to `journal` as one committed batch, in order. Timestamps never go
/// backwards; a timer fire is stamped no earlier than its `fire_at`.
fn commit(journal: &mut Vec<Event>, inputs: Vec<In>) {
    let mut ts = journal.last().map_or(0, |e| e.ts_nanos);
    let fire_at: HashMap<OpId, u64> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::TimerStarted(t) => Some((e.seq, t.fire_at_nanos)),
            _ => None,
        })
        .collect();
    let push = |journal: &mut Vec<Event>, ts: u64, body: EventBody| {
        let seq = journal.len() as u64;
        journal.push(Event {
            seq,
            ts_nanos: ts,
            body,
        });
        seq
    };
    for input in inputs {
        match input {
            In::Fire(op) => {
                ts = ts.max(fire_at.get(&op).copied().unwrap_or(ts));
                push(journal, ts, EventBody::TimerFired(TimerFired { op }));
            }
            In::Signal { op, name, payload } => {
                let payload_seq = push(
                    journal,
                    ts,
                    EventBody::SignalPayload(SignalPayload {
                        name,
                        payload: json(&payload),
                    }),
                );
                push(
                    journal,
                    ts,
                    EventBody::SignalMatched(SignalMatched { op, payload_seq }),
                );
            }
            In::Buffer { name, payload } => {
                push(
                    journal,
                    ts,
                    EventBody::SignalPayload(SignalPayload {
                        name,
                        payload: json(&payload),
                    }),
                );
            }
            In::Match { op, payload_seq } => {
                push(
                    journal,
                    ts,
                    EventBody::SignalMatched(SignalMatched { op, payload_seq }),
                );
            }
            In::TimedOut(op) => {
                push(journal, ts, EventBody::WaitTimedOut(WaitTimedOut { op }));
            }
            In::Token { op, payload } => {
                push(
                    journal,
                    ts,
                    EventBody::TokenCompleted(TokenCompleted {
                        op,
                        payload: json(&payload),
                    }),
                );
            }
            In::Cancel => {
                push(
                    journal,
                    ts,
                    EventBody::CancelRequested(CancelRequested {
                        reason: "test cancel".to_string(),
                    }),
                );
            }
        }
    }
}

/// An op the guest started that nothing has completed yet (no completing input and no
/// guest `OpCancelled`).
#[derive(Debug, Clone, PartialEq, Eq)]
enum OpenOp {
    Timer(OpId),
    Signal {
        op: OpId,
        name: String,
        timeout: bool,
    },
    Token(OpId),
}

fn completed_ops(journal: &[Event]) -> HashSet<OpId> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::OpCancelled(c) => Some(c.op),
            b => b.completes_op(),
        })
        .collect()
}

fn open_ops(journal: &[Event]) -> Vec<OpenOp> {
    let done = completed_ops(journal);
    journal
        .iter()
        .filter(|e| !done.contains(&e.seq))
        .filter_map(|e| match &e.body {
            EventBody::TimerStarted(_) => Some(OpenOp::Timer(e.seq)),
            EventBody::SignalWaitStarted(w) => Some(OpenOp::Signal {
                op: e.seq,
                name: w.name.clone(),
                timeout: w.timeout_ms.is_some(),
            }),
            EventBody::AwaitTokenCreated(_) => Some(OpenOp::Token(e.seq)),
            _ => None,
        })
        .collect()
}

/// Result of [`drive`].
#[derive(Debug)]
struct Driven {
    outcome: ActivationOutcome,
    journal: Vec<Event>,
    activations: u32,
}

/// Activate, and after every eviction commit `feed(journal)` and re-activate over the full
/// journal. Stops on a non-evicted outcome, or when `feed` returns nothing.
async fn drive(p: &Prepared, spec: RunSpec, mut feed: impl FnMut(&[Event]) -> Vec<In>) -> Driven {
    let mut journal = vec![spec.run_started_event(&p.module_hash_hex)];
    let mut activations = 0;
    loop {
        activations += 1;
        assert!(activations <= 200, "too many activations: {journal:?}");
        let (outcome, j) = activate(p, journal).await;
        journal = j;
        if !matches!(outcome, ActivationOutcome::Evicted { .. }) {
            return Driven {
                outcome,
                journal,
                activations,
            };
        }
        let inputs = feed(&journal);
        if inputs.is_empty() {
            return Driven {
                outcome,
                journal,
                activations,
            };
        }
        commit(&mut journal, inputs);
    }
}

fn output(d: &Driven) -> String {
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

fn count(journal: &[Event], kind: EventKind) -> usize {
    journal.iter().filter(|e| e.body.kind() == kind).count()
}

fn step_names(journal: &[Event]) -> Vec<String> {
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::StepCompleted(s) => Some(s.name.clone()),
            _ => None,
        })
        .collect()
}

/// Replaying the journal gives the same outcome and appends nothing.
async fn assert_replays(p: &Prepared, d: &Driven) {
    let (again, appended) = replay_journal(rt(), p, run(), d.journal.clone()).await;
    assert_eq!(again, d.outcome, "replay outcome must be identical");
    assert_eq!(appended, 0, "replay must append nothing");
}

/// Fire every open timer (the testkit's default timer behaviour).
fn fire_all(journal: &[Event]) -> Vec<In> {
    open_ops(journal)
        .into_iter()
        .filter_map(|o| match o {
            OpenOp::Timer(op) => Some(In::Fire(op)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn phase5_guests_validate_and_describe() {
    for name in [
        "race-fuzz",
        "signals",
        "timeouts",
        "tokens",
        "can-chain",
        "crash-saga",
        "trivial-steps",
    ] {
        let p = guest(name).await;
        let metas = rt().describe(&p).await.expect("describe");
        let got: Vec<(&str, u32)> = metas.iter().map(|m| (m.name.as_str(), m.version)).collect();
        assert_eq!(got, vec![(name, 1)], "{name} describe()");
    }
}

// ---------------------------------------------------------------------------------------
// race-fuzz (WF-03)
// ---------------------------------------------------------------------------------------

/// splitmix64 (test side: input shapes and input arrival order).
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn race_input(seed: u64) -> String {
    let mut r = Rng(seed ^ 0xA5A5);
    let sleeps: Vec<String> = (0..2 + r.below(4))
        .map(|_| (10 + r.below(990)).to_string())
        .collect();
    let names = ["a", "b", "c"];
    let signals: Vec<String> = (0..r.below(4))
        .map(|_| format!("\"{}\"", names[r.below(names.len())]))
        .collect();
    format!(
        r#"{{"seed":{seed},"sleeps_ms":[{}],"signals":[{}],"nest":{}}}"#,
        sleeps.join(","),
        signals.join(","),
        seed.is_multiple_of(2)
    )
}

/// A random non-empty subset of the open ops, in random order: timers fire, signal waits
/// get a signal (FIFO: matched to the oldest open wait on that name) or time out.
fn race_feed(r: &mut Rng, journal: &[Event], sent: &mut u32) -> Vec<In> {
    let mut open = open_ops(journal);
    if open.is_empty() {
        return Vec::new();
    }
    let k = 1 + r.below(open.len());
    let mut chosen = Vec::new();
    for _ in 0..k {
        chosen.push(open.remove(r.below(open.len())));
    }
    // Oldest-first open signal waits per name (for FIFO matching).
    let mut waits: HashMap<String, Vec<OpId>> = HashMap::new();
    for o in open_ops(journal) {
        if let OpenOp::Signal { op, name, .. } = o {
            waits.entry(name).or_default().push(op);
        }
    }
    let mut out = Vec::new();
    let mut used: HashSet<OpId> = HashSet::new();
    for o in chosen {
        match o {
            OpenOp::Timer(op) => out.push(In::Fire(op)),
            OpenOp::Signal { op, name, timeout } => {
                if timeout && r.below(2) == 0 {
                    if used.insert(op) {
                        out.push(In::TimedOut(op));
                    }
                } else if let Some(&target) = waits[&name].iter().find(|w| !used.contains(*w)) {
                    used.insert(target);
                    *sent += 1;
                    out.push(In::Signal {
                        op: target,
                        name,
                        payload: format!(r#"{{"n":{sent}}}"#),
                    });
                }
            }
            OpenOp::Token(_) => {}
        }
    }
    out
}

/// `(label, op, outcome)` triples from race-fuzz's output `ops` array.
fn race_ops(out: &str) -> Vec<(String, OpId, String)> {
    let ops = &out[out.find(r#""ops":["#).expect("ops array")..];
    let mut res = Vec::new();
    for part in ops.split(r#"{"label":""#).skip(1) {
        let (label, rest) = part.split_once('"').expect("label");
        let rest = rest.strip_prefix(r#","op":"#).expect("op");
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        let rest = rest[digits.len()..]
            .strip_prefix(r#","outcome":""#)
            .expect("outcome");
        let mut outcome = String::new();
        let mut chars = rest.chars();
        while let Some(c) = chars.next() {
            match c {
                '\\' => outcome.push(chars.next().expect("escape")),
                '"' => break,
                c => outcome.push(c),
            }
        }
        res.push((label.to_string(), digits.parse().expect("op id"), outcome));
    }
    res
}

/// The outcome the first completion of `op` in journal order implies.
fn expected_outcome(journal: &[Event], op: OpId) -> String {
    for e in journal {
        let hit = match &e.body {
            EventBody::OpCancelled(c) => c.op == op,
            b => b.completes_op() == Some(op),
        };
        if !hit {
            continue;
        }
        return match &e.body {
            EventBody::TimerFired(_) => "ok".to_string(),
            EventBody::WaitTimedOut(_) => "timeout".to_string(),
            EventBody::OpCancelled(_) => "cancelled".to_string(),
            EventBody::SignalMatched(m) => match &journal[m.payload_seq as usize].body {
                EventBody::SignalPayload(sp) => {
                    format!("ok:{}", String::from_utf8_lossy(&sp.payload.data))
                }
                other => panic!("payload_seq {} is {other:?}", m.payload_seq),
            },
            other => panic!("unexpected completion {other:?}"),
        };
    }
    panic!("op {op} never completed: {journal:?}");
}

#[tokio::test]
async fn race_fuzz_replay_equals_live_for_20_seeds() {
    let p = guest("race-fuzz").await;
    let mut seen: HashSet<&'static str> = HashSet::new();
    for seed in 1..=20u64 {
        let input = race_input(seed);
        let mut r = Rng(seed.wrapping_mul(0x1234_5678_9ABC_DEF1));
        let mut sent = 0;
        let d = drive(&p, spec("race-fuzz", &input), |j| {
            race_feed(&mut r, j, &mut sent)
        })
        .await;
        let out = output(&d);
        // Every op the guest started is reported exactly once, with the outcome its first
        // completion in journal order implies (first completion wins; losers cancelled).
        let started: Vec<OpId> = d
            .journal
            .iter()
            .filter(|e| {
                matches!(
                    e.body,
                    EventBody::TimerStarted(_) | EventBody::SignalWaitStarted(_)
                )
            })
            .map(|e| e.seq)
            .collect();
        let ops = race_ops(&out);
        let mut reported: Vec<OpId> = ops.iter().map(|(_, op, _)| *op).collect();
        reported.sort_unstable();
        assert_eq!(reported, started, "seed {seed}: {out}");
        for (label, op, outcome) in &ops {
            assert_eq!(
                outcome,
                &expected_outcome(&d.journal, *op),
                "seed {seed} op {label}/{op}: {out}"
            );
        }
        for (_, _, outcome) in &ops {
            seen.insert(match outcome.as_str() {
                "ok" => "timer",
                "cancelled" => "cancelled",
                "timeout" => "timeout",
                o if o.starts_with("ok:") => "signal",
                _ => "other",
            });
        }
        for shape in ["\"wait(", "\"select(", "\"join(", "\"select_join("] {
            if out.contains(shape) {
                seen.insert(shape);
            }
        }
        assert_replays(&p, &d).await;

        // Same input, same arrival order: identical journal (the guest is deterministic).
        let mut r = Rng(seed.wrapping_mul(0x1234_5678_9ABC_DEF1));
        let mut sent = 0;
        let again = drive(&p, spec("race-fuzz", &input), |j| {
            race_feed(&mut r, j, &mut sent)
        })
        .await;
        assert_eq!(again.journal, d.journal, "seed {seed}: journal differs");
    }
    // The seeds cover every round shape and every outcome class.
    let mut seen: Vec<&str> = seen.into_iter().collect();
    seen.sort_unstable();
    assert_eq!(
        seen,
        [
            "\"join(",
            "\"select(",
            "\"select_join(",
            "\"wait(",
            "cancelled",
            "signal",
            "timeout",
            "timer"
        ]
    );
}

// ---------------------------------------------------------------------------------------
// signals (WF-05)
// ---------------------------------------------------------------------------------------

fn only_signal_wait(journal: &[Event]) -> OpId {
    let waits: Vec<OpId> = open_ops(journal)
        .into_iter()
        .filter_map(|o| match o {
            OpenOp::Signal { op, .. } => Some(op),
            _ => None,
        })
        .collect();
    assert_eq!(waits.len(), 1, "{journal:?}");
    waits[0]
}

#[tokio::test]
async fn signals_fifo_returns_payloads_in_order() {
    let p = guest("signals").await;
    // All three signals arrive before the second wait exists: the first is matched at
    // once, the other two are buffered and matched (oldest first) as the waits register.
    let mut payload_seqs: Vec<u64> = Vec::new();
    let mut round = 0;
    let d = drive(&p, spec("signals", r#"{"mode":"fifo"}"#), |j| {
        let op = only_signal_wait(j);
        round += 1;
        if round == 1 {
            let base = j.len() as u64;
            payload_seqs = vec![base + 2, base + 3];
            vec![
                In::Signal {
                    op,
                    name: "x".into(),
                    payload: r#""a""#.into(),
                },
                In::Buffer {
                    name: "x".into(),
                    payload: r#""b""#.into(),
                },
                In::Buffer {
                    name: "x".into(),
                    payload: r#""c""#.into(),
                },
            ]
        } else {
            vec![In::Match {
                op,
                payload_seq: payload_seqs.remove(0),
            }]
        }
    })
    .await;
    assert_eq!(output(&d), r#"["a","b","c"]"#);
    assert_eq!(count(&d.journal, EventKind::SignalWaitStarted), 3);
    assert_eq!(count(&d.journal, EventKind::SignalPayload), 3);
    assert_eq!(count(&d.journal, EventKind::SignalMatched), 3);
    assert_eq!(count(&d.journal, EventKind::SignalReceived), 0);
    assert_replays(&p, &d).await;
}

#[tokio::test]
async fn signals_before_buffers_during_sleep() {
    let p = guest("signals").await;
    let mut buffered = None;
    let d = drive(&p, spec("signals", r#"{"mode":"before"}"#), |j| {
        let open = open_ops(j);
        match open.as_slice() {
            // Sleeping: the signal arrives (buffered) before the timer fires.
            [OpenOp::Timer(t)] => {
                buffered = Some(j.len() as u64);
                vec![
                    In::Buffer {
                        name: "x".into(),
                        payload: r#"{"early":true}"#.into(),
                    },
                    In::Fire(*t),
                ]
            }
            // The wait registered after the sleep: matched from the buffer.
            [OpenOp::Signal { op, .. }] => vec![In::Match {
                op: *op,
                payload_seq: buffered.expect("buffered first"),
            }],
            other => panic!("unexpected open ops {other:?}"),
        }
    })
    .await;
    assert_eq!(output(&d), r#"{"early":true}"#);
    // The payload precedes the wait in the journal.
    let payload = d
        .journal
        .iter()
        .position(|e| e.body.kind() == EventKind::SignalPayload)
        .expect("payload");
    let wait = d
        .journal
        .iter()
        .position(|e| e.body.kind() == EventKind::SignalWaitStarted)
        .expect("wait");
    assert!(payload < wait, "{:?}", d.journal);
    assert_replays(&p, &d).await;
}

#[tokio::test]
async fn signals_timeout_mode() {
    let p = guest("signals").await;
    // Times out.
    let d = drive(&p, spec("signals", r#"{"mode":"timeout"}"#), |j| {
        vec![In::TimedOut(only_signal_wait(j))]
    })
    .await;
    assert_eq!(output(&d), r#""timeout""#);
    let timeout_ms = d.journal.iter().find_map(|e| match &e.body {
        EventBody::SignalWaitStarted(w) => Some(w.timeout_ms),
        _ => None,
    });
    assert_eq!(timeout_ms, Some(Some(200)));
    assert_replays(&p, &d).await;

    // Signal first; a stale timeout after the match changes nothing (first wins).
    let d = drive(&p, spec("signals", r#"{"mode":"timeout"}"#), |j| {
        let op = only_signal_wait(j);
        vec![
            In::Signal {
                op,
                name: "x".into(),
                payload: "42".into(),
            },
            In::TimedOut(op),
        ]
    })
    .await;
    assert_eq!(output(&d), "42");
    assert_replays(&p, &d).await;
}

#[tokio::test]
async fn signals_long_wait_has_no_timeout() {
    let p = guest("signals").await;
    let d = drive(&p, spec("signals", r#"{"mode":"long_wait"}"#), |j| {
        let op = only_signal_wait(j);
        vec![In::Signal {
            op,
            name: "x".into(),
            payload: r#""late""#.into(),
        }]
    })
    .await;
    assert_eq!(output(&d), r#""late""#);
    let timeout_ms = d.journal.iter().find_map(|e| match &e.body {
        EventBody::SignalWaitStarted(w) => Some(w.timeout_ms),
        _ => None,
    });
    assert_eq!(timeout_ms, Some(None));
    assert_replays(&p, &d).await;
}

// ---------------------------------------------------------------------------------------
// tokens (API-07)
// ---------------------------------------------------------------------------------------

fn token_op(journal: &[Event]) -> (OpId, String) {
    journal
        .iter()
        .find_map(|e| match &e.body {
            EventBody::AwaitTokenCreated(t) => Some((e.seq, t.token.clone())),
            _ => None,
        })
        .expect("token created")
}

fn latest_status(journal: &[Event]) -> Option<String> {
    journal.iter().rev().find_map(|e| match &e.body {
        EventBody::StatusPublished(s) => Some(s.json.clone()),
        _ => None,
    })
}

#[tokio::test]
async fn tokens_complete_and_time_out() {
    let p = guest("tokens").await;
    let d = drive(&p, spec("tokens", r#"{"timeout_ms":5000}"#), |j| {
        let (op, _) = token_op(j);
        vec![In::Token {
            op,
            payload: r#"{"approved":true}"#.into(),
        }]
    })
    .await;
    assert_eq!(output(&d), r#"{"approved":true}"#);
    // The token is published through the run status.
    let (_, token) = token_op(&d.journal);
    assert_eq!(
        latest_status(&d.journal),
        Some(format!(r#"{{"token":"{token}"}}"#))
    );
    assert_replays(&p, &d).await;

    let d = drive(&p, spec("tokens", r#"{"timeout_ms":5000}"#), |j| {
        vec![In::TimedOut(token_op(j).0)]
    })
    .await;
    assert_eq!(output(&d), r#""timeout""#);
    assert_replays(&p, &d).await;

    // Cancelled while waiting: the run ends Cancelled.
    let d = drive(&p, spec("tokens", "{}"), |j| {
        if count(j, EventKind::CancelRequested) == 0 {
            vec![In::Cancel]
        } else {
            Vec::new()
        }
    })
    .await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Failed {
            failure: Failure::Cancelled
        },
        "{:?}",
        d.journal
    );
    assert_replays(&p, &d).await;
}

// ---------------------------------------------------------------------------------------
// can-chain, timeouts, trivial-steps
// ---------------------------------------------------------------------------------------

#[tokio::test]
async fn can_chain_continues_until_max() {
    let p = guest("can-chain").await;
    let mut input = r#"{"n":0,"max":3}"#.to_string();
    let mut continued = 0;
    let last = loop {
        let d = drive(&p, spec("can-chain", &input), |_| Vec::new()).await;
        assert_replays(&p, &d).await;
        match &d.outcome {
            ActivationOutcome::ContinuedAsNew { continuation } => {
                assert_eq!(count(&d.journal, EventKind::RunContinuedAsNew), 1);
                assert_eq!(continuation.input.content_type, "application/json");
                assert_eq!(
                    (&continuation.workflow, continuation.version),
                    (&None, None)
                );
                input = String::from_utf8(continuation.input.data.clone()).expect("utf8");
                continued += 1;
                assert!(continued <= 3, "chain did not stop");
            }
            _ => break d,
        }
    };
    assert_eq!(continued, 3);
    assert_eq!(input, r#"{"n":3,"max":3}"#);
    assert_eq!(output(&last), "3");
}

#[tokio::test]
async fn timeouts_modes() {
    let p = guest("timeouts").await;
    let d = drive(
        &p,
        spec("timeouts", r#"{"mode":"sleep_ms","n":250}"#),
        fire_all,
    )
    .await;
    assert_eq!(output(&d), r#""slept""#);
    assert_replays(&p, &d).await;

    let d = drive(
        &p,
        spec("timeouts", r#"{"mode":"busy_steps","n":25}"#),
        |_| Vec::new(),
    )
    .await;
    assert_eq!(output(&d), r#""busy:25""#);
    assert_eq!(count(&d.journal, EventKind::StepCompleted), 25);
    assert_eq!(d.activations, 1);

    // sleep_forever keeps sleeping however many timers fire; only a cancel ends it here
    // (the run timeout is a partition-side hard stop, tested by 05-15).
    let mut fired = 0;
    let d = drive(&p, spec("timeouts", r#"{"mode":"sleep_forever"}"#), |j| {
        fired += 1;
        if fired <= 5 {
            fire_all(j)
        } else if count(j, EventKind::CancelRequested) == 0 {
            vec![In::Cancel]
        } else {
            Vec::new()
        }
    })
    .await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Failed {
            failure: Failure::Cancelled
        }
    );
    assert_eq!(count(&d.journal, EventKind::TimerStarted), 6);
    assert_replays(&p, &d).await;
}

#[tokio::test]
async fn trivial_steps_ten_in_one_activation() {
    let p = guest("trivial-steps").await;
    let d = drive(&p, spec("trivial-steps", r#"{"n":10}"#), |_| Vec::new()).await;
    assert_eq!(output(&d), "10");
    assert_eq!(d.activations, 1);
    assert_eq!(count(&d.journal, EventKind::StepCompleted), 10);
    let want: Vec<String> = (0..10).map(|i| format!("s{i}")).collect();
    assert_eq!(step_names(&d.journal), want);
    assert_replays(&p, &d).await;
}

// ---------------------------------------------------------------------------------------
// crash-saga (05-19 workload)
// ---------------------------------------------------------------------------------------

/// Timers fire; `go` gets `go_payload` (or times out if `None`); `extra` times out.
fn saga_feed(go_payload: Option<&'static str>) -> impl FnMut(&[Event]) -> Vec<In> {
    move |j| {
        open_ops(j)
            .into_iter()
            .filter_map(|o| match o {
                OpenOp::Timer(op) => Some(In::Fire(op)),
                OpenOp::Signal { op, name, .. } if name == "go" => Some(match go_payload {
                    Some(p) => In::Signal {
                        op,
                        name,
                        payload: p.into(),
                    },
                    None => In::TimedOut(op),
                }),
                OpenOp::Signal { op, .. } => Some(In::TimedOut(op)),
                OpenOp::Token(_) => None,
            })
            .collect()
    }
}

#[tokio::test]
async fn crash_saga_output_is_the_documented_function() {
    let p = guest("crash-saga").await;
    let input =
        r#"{"id":"c1","steps":4,"fail_pattern":"0120","sleep_ms":100,"signal_timeout_ms":1000}"#;
    let d = drive(
        &p,
        spec("crash-saga", input),
        saga_feed(Some(r#"{"ok":1}"#)),
    )
    .await;
    assert_eq!(
        output(&d),
        r#"{"extra":null,"go":{"ok":1},"id":"c1","results":["c1-0","c1-1","c1-2","c1-3"],"status":"completed"}"#
    );
    // Retry failures are journaled but do not change the output.
    assert_eq!(count(&d.journal, EventKind::StepAttemptFailed), 3);
    assert_eq!(step_names(&d.journal), ["step0", "step1", "step2", "step3"]);
    assert_replays(&p, &d).await;

    // go times out too.
    let d = drive(&p, spec("crash-saga", input), saga_feed(None)).await;
    assert_eq!(
        output(&d),
        r#"{"extra":null,"go":null,"id":"c1","results":["c1-0","c1-1","c1-2","c1-3"],"status":"completed"}"#
    );
    assert_replays(&p, &d).await;
}

#[tokio::test]
async fn crash_saga_compensates_in_reverse() {
    let p = guest("crash-saga").await;
    let input =
        r#"{"id":"c2","steps":5,"fail_pattern":"01x","sleep_ms":100,"signal_timeout_ms":1000}"#;
    let d = drive(&p, spec("crash-saga", input), saga_feed(None)).await;
    assert_eq!(
        output(&d),
        r#"{"failed_at":2,"id":"c2","status":"compensated","undone":[1,0]}"#
    );
    assert_eq!(
        step_names(&d.journal),
        ["step0", "step1", "step2", "undo1", "undo0"]
    );
    assert_replays(&p, &d).await;

    let bad = drive(
        &p,
        spec(
            "crash-saga",
            r#"{"id":"c3","steps":2,"sleep_ms":1,"signal_timeout_ms":1}"#,
        ),
        |_| Vec::new(),
    )
    .await;
    match &bad.outcome {
        ActivationOutcome::Failed {
            failure: Failure::NonRetryable(e),
        } => assert_eq!(e.kind, "bad_input"),
        other => panic!("expected bad_input, got {other:?}"),
    }
}

#[tokio::test]
async fn crash_saga_cancel_runs_shielded_cleanup() {
    let p = guest("crash-saga").await;
    let input =
        r#"{"id":"c4","steps":3,"fail_pattern":"","sleep_ms":5000,"signal_timeout_ms":1000}"#;
    // Cancel while the saga sleeps after its steps.
    let d = drive(&p, spec("crash-saga", input), |j| {
        if count(j, EventKind::CancelRequested) == 0 {
            vec![In::Cancel]
        } else {
            Vec::new()
        }
    })
    .await;
    assert_eq!(
        d.outcome,
        ActivationOutcome::Failed {
            failure: Failure::Cancelled
        },
        "{:?}",
        d.journal
    );
    assert_eq!(
        step_names(&d.journal),
        ["step0", "step1", "step2", "undo2", "undo1", "undo0"]
    );
    assert_replays(&p, &d).await;
}
