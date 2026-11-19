//! QA-02 replay-corpus recorder (plan 05-12).
//!
//! Runs a guest on the engine testkit with scripted inputs and writes a new corpus entry
//! `tests/replay-corpus/<entry>/{module.wasm, journal.bin, entry.toml}`. It refuses to
//! overwrite an existing entry: entries are never re-recorded (tests/replay-corpus/README.md).
//!
//! ```text
//! cargo run -p ostro-engine --example record_corpus -- <guest | path/to/x.wasm> <entry> \
//!     [--input TEXT] [--inputs FILE | --inputs '{...}'] [--workflow NAME] \
//!     [--partition N] [--run-id ID] [--seed-hex HEX] [--token-key-hex HEX] \
//!     [--deadline-nanos N] [--race-seed N] [--corpus DIR]
//! ```
//!
//! - `<guest>` is a `guests/` member, built in release for wasm32-wasip2 (`guest_wasm`, the
//!   same build the engine suites use); a path ending in `.wasm` is read as is.
//! - `--input` is the run input (bytes of the text, `application/octet-stream` like every
//!   testkit `RunSpec`). `--workflow` defaults to the guest name.
//! - Defaults: partition 7, run id `run-<entry>`, seed `[0x5a; 32]`, accepted at
//!   `DEFAULT_ACCEPTED_AT`, token key [`TEST_TOKEN_KEY`] (`[9; 32]`, the engine suites' test
//!   key; never a production key).
//! - The node clock is frozen at `accepted_at` (journals depend only on the inputs fed) and
//!   only moves to `retry_at` when an attempt is requeued (`StepBackoff`, D22).
//!
//! Inputs (`--inputs`) are a JSON object fed between activations, exactly as the partition
//! leader commits inputs from Phase 5 on (D21 signals, D4 timeouts, D3 cancel):
//!
//! ```json
//! { "pre":    [action, ...],            // committed before the first activation (B1 shape)
//!   "rounds": [[action, ...], ...],     // one batch per eviction, in order
//!   "auto":   [action, ...] }           // every later eviction (default: fire_timers,
//!                                       // match_buffered, effects, children)
//! ```
//!
//! Actions:
//! - `"fire_timers"`: `TimerFired` for every open timer, stamped no earlier than `fire_at`;
//! - `{"signal": {"name": N, "payload": JSON-TEXT, "buffer": true}}`: `SignalPayload` +
//!   `SignalMatched` for the oldest open wait on `N` (FIFO), else a buffered `SignalPayload`
//!   (`"buffer": false` delivers only when a wait is open);
//! - `"match_buffered"`: `SignalMatched` for each open signal wait from the oldest unmatched
//!   buffered `SignalPayload` of its name (late match, `payload_seq` points back);
//! - `{"timeout": N}`: `WaitTimedOut` for every open signal wait on `N` (`"*"`: every open
//!   signal and token wait);
//! - `{"token": JSON-TEXT}`: `TokenCompleted` for every open await token;
//! - `"cancel"` / `{"cancel": REASON}`: `CancelRequested` (once per run);
//! - `"effects"` / `"children"`: complete open effects / children with canned outputs (the
//!   wit-surface suite's responses).
//!
//! `--race-seed N` replaces the inputs with the race-fuzz feed of `phase5_guests.rs` (random
//! subsets of the open ops per eviction, seeded) and defaults `--input` to that suite's input
//! for seed N.
//!
//! Before writing, the recorder replays the journal on the same runtime and requires the same
//! outcome with zero appends.
//!
//! `record_corpus --dump <entry-dir>` prints an existing entry's events (read-only).

#![allow(clippy::print_stdout, clippy::print_stderr)]

#[path = "../tests/common/mod.rs"]
mod common;

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ostro_engine::config::EngineConfig;
use ostro_engine::env::Clock;
use ostro_engine::error::EvictReason;
use ostro_engine::runtime::{Prepared, Runtime, RuntimeOptions};
use ostro_engine::sink::{Activate, ActivationOutcome, RunRef};
use ostro_engine::testkit::{DEFAULT_ACCEPTED_AT, FakeSink, RunSpec, replay_journal};
use ostro_types::PayloadRef;
use ostro_types::command::JournalEvent;
use ostro_types::journal::{
    CancelRequested, ChildCompleted, EffectCompleted, EffectRequest, Event, EventBody, Failure,
    Header, HttpResponse, OpId, OpOutput, Payload, RngSeed, SignalMatched, SignalPayload,
    SqlResult, SqlValue, TimerFired, TokenCompleted, WaitTimedOut, encode_event, encode_result,
};
use prost::Message;
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

/// Await-token key of recorded entries: the engine suites' fixed test key. Not a secret.
const TEST_TOKEN_KEY: [u8; 32] = [9; 32];
const DEFAULT_PARTITION: u16 = 7;
const MAX_ACTIVATIONS: u32 = 500;

// ------------------------------------------------------------------------------- clock

/// Frozen clock the recorder moves explicitly (to `retry_at` on a requeued attempt).
#[derive(Debug)]
struct SetClock(AtomicU64);

impl Clock for SetClock {
    fn now_unix_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

// ------------------------------------------------------------------------------ inputs

/// One committed input.
#[derive(Debug, Clone)]
enum In {
    Fire(OpId),
    Signal {
        op: OpId,
        name: String,
        payload: String,
    },
    Buffer {
        name: String,
        payload: String,
    },
    Match {
        op: OpId,
        payload_seq: u64,
    },
    TimedOut(OpId),
    Token {
        op: OpId,
        payload: String,
    },
    Cancel(String),
    Effect {
        op: OpId,
        out: Result<OpOutput, Failure>,
    },
    Child {
        op: OpId,
        out: Result<OpOutput, Failure>,
    },
}

fn json(data: &str) -> Payload {
    Payload {
        content_type: "application/json".to_string(),
        data: data.as_bytes().to_vec(),
    }
}

fn text(s: &str) -> Payload {
    Payload {
        content_type: "text/plain".to_string(),
        data: s.as_bytes().to_vec(),
    }
}

/// Append `inputs` as one committed batch. Timestamps never go backwards; a timer fire is
/// stamped no earlier than its `fire_at` (same rule as the testkit and `phase5_guests.rs`).
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
            In::Cancel(reason) => {
                push(
                    journal,
                    ts,
                    EventBody::CancelRequested(CancelRequested { reason }),
                );
            }
            In::Effect { op, out } => {
                push(
                    journal,
                    ts,
                    EventBody::EffectCompleted(EffectCompleted {
                        op,
                        outcome: encode_result(&out),
                    }),
                );
            }
            In::Child { op, out } => {
                push(
                    journal,
                    ts,
                    EventBody::ChildCompleted(ChildCompleted {
                        op,
                        outcome: encode_result(&out),
                    }),
                );
            }
        }
    }
}

/// An op nothing has completed yet (no completing input and no guest `OpCancelled`).
#[derive(Debug, Clone)]
enum OpenOp {
    Timer(OpId),
    Signal {
        op: OpId,
        name: String,
        timeout: bool,
    },
    Token(OpId),
    Effect(OpId, EffectRequest),
    Child(OpId),
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
            EventBody::EffectScheduled(s) => Some(OpenOp::Effect(e.seq, s.request.clone())),
            EventBody::ChildStarted(_) => Some(OpenOp::Child(e.seq)),
            _ => None,
        })
        .collect()
}

/// Buffered `SignalPayload`s (`(seq, name)`, oldest first) no `SignalMatched` refers to yet.
fn buffered_payloads(journal: &[Event]) -> Vec<(u64, String)> {
    let matched: HashSet<u64> = journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::SignalMatched(m) => Some(m.payload_seq),
            _ => None,
        })
        .collect();
    journal
        .iter()
        .filter_map(|e| match &e.body {
            EventBody::SignalPayload(p) if !matched.contains(&e.seq) => {
                Some((e.seq, p.name.clone()))
            }
            _ => None,
        })
        .collect()
}

/// Canned effect outputs (the wit-surface suite's `full_effects_rule`).
fn effect_output(req: &EffectRequest) -> Result<OpOutput, Failure> {
    Ok(match req {
        EffectRequest::Http(_) => OpOutput::Http(HttpResponse {
            status: 201,
            headers: vec![Header {
                name: "x-a".to_string(),
                value: "1".to_string(),
            }],
            body: b"ok".to_vec(),
        }),
        EffectRequest::Sql(_) => OpOutput::Sql(SqlResult {
            columns: vec!["c".to_string()],
            rows: vec![
                vec![SqlValue::Int8(1)],
                vec![SqlValue::Text("a".to_string())],
            ],
            rows_affected: 2,
        }),
        EffectRequest::Object(_) => OpOutput::Object(b"obj".to_vec()),
    })
}

/// A scripted action (see the module docs).
#[derive(Debug, Clone)]
enum Action {
    FireTimers,
    Signal {
        name: String,
        payload: String,
        buffer: bool,
    },
    MatchBuffered,
    Timeout(String),
    Token(String),
    Cancel(String),
    Effects,
    Children,
}

fn parse_action(v: &Value) -> Result<Action, String> {
    if let Some(s) = v.as_str() {
        return match s {
            "fire_timers" => Ok(Action::FireTimers),
            "match_buffered" => Ok(Action::MatchBuffered),
            "effects" => Ok(Action::Effects),
            "children" => Ok(Action::Children),
            "cancel" => Ok(Action::Cancel("recorded cancel".to_string())),
            other => Err(format!("unknown action {other:?}")),
        };
    }
    let obj = v
        .as_object()
        .filter(|o| o.len() == 1)
        .ok_or_else(|| format!("an action is a string or a one-key object: {v}"))?;
    let (k, arg) = obj.iter().next().expect("one key");
    let str_arg = || {
        arg.as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("{k}: expected a string, got {arg}"))
    };
    match k.as_str() {
        "signal" => {
            let name = arg["name"]
                .as_str()
                .ok_or("signal.name must be a string")?
                .to_string();
            let payload = arg["payload"]
                .as_str()
                .ok_or("signal.payload must be a string (JSON text)")?
                .to_string();
            let buffer = arg.get("buffer").and_then(Value::as_bool).unwrap_or(true);
            Ok(Action::Signal {
                name,
                payload,
                buffer,
            })
        }
        "timeout" => Ok(Action::Timeout(str_arg()?)),
        "token" => Ok(Action::Token(str_arg()?)),
        "cancel" => Ok(Action::Cancel(str_arg()?)),
        other => Err(format!("unknown action {other:?}")),
    }
}

fn parse_actions(v: &Value) -> Result<Vec<Action>, String> {
    v.as_array()
        .ok_or_else(|| format!("expected an array of actions, got {v}"))?
        .iter()
        .map(parse_action)
        .collect()
}

/// The parsed `--inputs` script.
#[derive(Debug, Clone)]
struct Script {
    pre: Vec<Action>,
    rounds: Vec<Vec<Action>>,
    auto: Vec<Action>,
}

impl Script {
    fn parse(v: &Value) -> Result<Self, String> {
        let obj = v.as_object().ok_or("--inputs must be a JSON object")?;
        for k in obj.keys() {
            if !matches!(k.as_str(), "pre" | "rounds" | "auto") {
                return Err(format!("unknown --inputs key {k:?}"));
            }
        }
        let pre = obj.get("pre").map(parse_actions).transpose()?;
        let rounds = obj
            .get("rounds")
            .map(|r| {
                r.as_array()
                    .ok_or("rounds must be an array of arrays")?
                    .iter()
                    .map(parse_actions)
                    .collect::<Result<Vec<_>, String>>()
            })
            .transpose()?;
        let auto = obj.get("auto").map(parse_actions).transpose()?;
        Ok(Self {
            pre: pre.unwrap_or_default(),
            rounds: rounds.unwrap_or_default(),
            auto: auto.unwrap_or_else(default_auto),
        })
    }
}

impl Default for Script {
    fn default() -> Self {
        Self {
            pre: Vec::new(),
            rounds: Vec::new(),
            auto: default_auto(),
        }
    }
}

fn default_auto() -> Vec<Action> {
    vec![
        Action::FireTimers,
        Action::MatchBuffered,
        Action::Effects,
        Action::Children,
    ]
}

/// The inputs `actions` produce against `journal` (one batch).
fn apply_actions(journal: &[Event], actions: &[Action]) -> Vec<In> {
    let open = open_ops(journal);
    let mut out = Vec::new();
    // Ops answered earlier in this batch, and buffered payloads matched in it.
    let mut used: HashSet<OpId> = HashSet::new();
    let mut used_payloads: HashSet<u64> = HashSet::new();
    let mut buffered = buffered_payloads(journal);
    let cancel_seen = journal
        .iter()
        .any(|e| matches!(e.body, EventBody::CancelRequested(_)));
    let mut cancel_sent = false;
    // Seq the next committed input will get (buffered payloads committed in this batch can
    // be matched later in it).
    let mut next_seq = journal.len() as u64;
    let width = |input: &In| match input {
        In::Signal { .. } => 2,
        _ => 1,
    };
    for action in actions {
        match action {
            Action::FireTimers => {
                for o in &open {
                    if let OpenOp::Timer(op) = o
                        && used.insert(*op)
                    {
                        let i = In::Fire(*op);
                        next_seq += width(&i);
                        out.push(i);
                    }
                }
            }
            Action::Signal {
                name,
                payload,
                buffer,
            } => {
                let target = open.iter().find_map(|o| match o {
                    OpenOp::Signal { op, name: n, .. } if n == name && !used.contains(op) => {
                        Some(*op)
                    }
                    _ => None,
                });
                match target {
                    Some(op) => {
                        used.insert(op);
                        let i = In::Signal {
                            op,
                            name: name.clone(),
                            payload: payload.clone(),
                        };
                        next_seq += width(&i);
                        out.push(i);
                    }
                    None if *buffer => {
                        buffered.push((next_seq, name.clone()));
                        let i = In::Buffer {
                            name: name.clone(),
                            payload: payload.clone(),
                        };
                        next_seq += width(&i);
                        out.push(i);
                    }
                    None => {}
                }
            }
            Action::MatchBuffered => {
                for o in &open {
                    let OpenOp::Signal { op, name, .. } = o else {
                        continue;
                    };
                    if used.contains(op) {
                        continue;
                    }
                    if let Some(&(payload_seq, _)) = buffered
                        .iter()
                        .find(|(s, n)| n == name && !used_payloads.contains(s))
                    {
                        used.insert(*op);
                        used_payloads.insert(payload_seq);
                        let i = In::Match {
                            op: *op,
                            payload_seq,
                        };
                        next_seq += width(&i);
                        out.push(i);
                    }
                }
            }
            Action::Timeout(name) => {
                for o in &open {
                    let op = match o {
                        OpenOp::Signal { op, name: n, .. } if name == "*" || n == name => *op,
                        OpenOp::Token(op) if name == "*" => *op,
                        _ => continue,
                    };
                    if used.insert(op) {
                        let i = In::TimedOut(op);
                        next_seq += width(&i);
                        out.push(i);
                    }
                }
            }
            Action::Token(payload) => {
                for o in &open {
                    if let OpenOp::Token(op) = o
                        && used.insert(*op)
                    {
                        let i = In::Token {
                            op: *op,
                            payload: payload.clone(),
                        };
                        next_seq += width(&i);
                        out.push(i);
                    }
                }
            }
            Action::Cancel(reason) => {
                if !cancel_seen && !cancel_sent {
                    cancel_sent = true;
                    let i = In::Cancel(reason.clone());
                    next_seq += width(&i);
                    out.push(i);
                }
            }
            Action::Effects => {
                for o in &open {
                    if let OpenOp::Effect(op, req) = o
                        && used.insert(*op)
                    {
                        let i = In::Effect {
                            op: *op,
                            out: effect_output(req),
                        };
                        next_seq += width(&i);
                        out.push(i);
                    }
                }
            }
            Action::Children => {
                for o in &open {
                    if let OpenOp::Child(op) = o
                        && used.insert(*op)
                    {
                        let i = In::Child {
                            op: *op,
                            out: Ok(OpOutput::Child(text("child-out"))),
                        };
                        next_seq += width(&i);
                        out.push(i);
                    }
                }
            }
        }
    }
    out
}

// --------------------------------------------------------------------- race-fuzz feed

/// splitmix64 (identical to `phase5_guests.rs`).
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

/// race-fuzz input for `seed` (identical to `phase5_guests.rs::race_input`).
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

/// race-fuzz feed (identical to `phase5_guests.rs::race_feed`): a random non-empty subset of
/// the open ops in random order; timers fire, signal waits get a FIFO-matched signal or time
/// out.
fn race_feed(r: &mut Rng, journal: &[Event], sent: &mut u32) -> Vec<In> {
    let all: Vec<OpenOp> = open_ops(journal)
        .into_iter()
        .filter(|o| {
            matches!(
                o,
                OpenOp::Timer(_) | OpenOp::Signal { .. } | OpenOp::Token(_)
            )
        })
        .collect();
    let mut open = all.clone();
    if open.is_empty() {
        return Vec::new();
    }
    let k = 1 + r.below(open.len());
    let mut chosen = Vec::new();
    for _ in 0..k {
        chosen.push(open.remove(r.below(open.len())));
    }
    let mut waits: HashMap<String, Vec<OpId>> = HashMap::new();
    for o in &all {
        if let OpenOp::Signal { op, name, .. } = o {
            waits.entry(name.clone()).or_default().push(*op);
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
            _ => {}
        }
    }
    out
}

// --------------------------------------------------------------------------- driver

enum Feed {
    Script { script: Script, round: usize },
    Race { rng: Rng, sent: u32 },
}

impl Feed {
    fn next(&mut self, journal: &[Event]) -> Vec<In> {
        match self {
            Feed::Script { script, round } => {
                let actions = match script.rounds.get(*round) {
                    Some(a) => a.clone(),
                    None => script.auto.clone(),
                };
                *round += 1;
                apply_actions(journal, &actions)
            }
            Feed::Race { rng, sent } => race_feed(rng, journal, sent),
        }
    }
}

async fn activate(
    rt: &Runtime,
    p: &Prepared,
    run: &RunRef,
    journal: Vec<Event>,
) -> (ActivationOutcome, Vec<Event>) {
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
    (outcome, sink.journal(run))
}

/// Drive the run to a terminal outcome; returns `(outcome, journal, activations)`.
async fn drive(
    rt: &Runtime,
    clock: &SetClock,
    p: &Prepared,
    spec: &RunSpec,
    pre: Vec<In>,
    mut feed: Feed,
) -> Result<(ActivationOutcome, Vec<Event>, u32), String> {
    let mut journal = vec![spec.run_started_event(&p.module_hash_hex)];
    commit(&mut journal, pre);
    let mut activations = 0u32;
    loop {
        activations += 1;
        if activations > MAX_ACTIVATIONS {
            return Err(format!("more than {MAX_ACTIVATIONS} activations"));
        }
        let (outcome, j) = activate(rt, p, &spec.run, journal).await;
        journal = j;
        match &outcome {
            ActivationOutcome::Evicted {
                reason: EvictReason::StepBackoff { retry_at_nanos },
            } => {
                // Requeued attempt: the leader re-dispatches at retry_at (D22).
                clock.0.fetch_max(*retry_at_nanos, Ordering::SeqCst);
            }
            ActivationOutcome::Evicted { reason } => {
                let inputs = feed.next(&journal);
                if inputs.is_empty() {
                    return Err(format!(
                        "stuck: evicted ({reason:?}) and the inputs feed nothing; open ops {:?}",
                        open_ops(&journal)
                    ));
                }
                commit(&mut journal, inputs);
            }
            ActivationOutcome::Completed { .. }
            | ActivationOutcome::Failed { .. }
            | ActivationOutcome::ContinuedAsNew { .. } => {
                return Ok((outcome, journal, activations));
            }
            other => return Err(format!("activation ended {other:?}")),
        }
    }
}

// -------------------------------------------------------------------------- entry file

#[derive(Serialize)]
struct EntryToml {
    name: String,
    workflow: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_json: Option<String>,
    expected: String,
    partition: u16,
    run_id: String,
    token_key_hex: String,
    terminal_payload_sha256: String,
    journal_events: u64,
    module_sha256: String,
    journal_sha256: String,
    recorded_with: RecordedWith,
}

#[derive(Serialize)]
struct RecordedWith {
    ostro: String,
    wasmtime: String,
    target: String,
    date: String,
    tool: String,
    command: String,
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex32(s: &str) -> Result<[u8; 32], String> {
    if s.len() != 64 {
        return Err(format!("expected 64 hex chars, got {}", s.len()));
    }
    let mut out = [0u8; 32];
    for (i, o) in out.iter_mut().enumerate() {
        *o = u8::from_str_radix(&s[2 * i..2 * i + 2], 16).map_err(|e| format!("hex: {e}"))?;
    }
    Ok(out)
}

/// `(expected, terminal payload bytes)` of a terminal outcome (see tests/replay_corpus.rs).
fn terminal_of(outcome: &ActivationOutcome) -> Result<(&'static str, Vec<u8>), String> {
    Ok(match outcome {
        ActivationOutcome::Completed { output } => ("completed", output.data.clone()),
        ActivationOutcome::Failed {
            failure: f @ Failure::Cancelled,
        } => ("cancelled", encode_result(f).bytes),
        ActivationOutcome::Failed { failure } => ("failed", encode_result(failure).bytes),
        ActivationOutcome::ContinuedAsNew { continuation } => {
            ("continued_as_new", encode_result(continuation).bytes)
        }
        other => return Err(format!("not terminal: {other:?}")),
    })
}

fn encode_journal(journal: &[Event]) -> Vec<u8> {
    let mut out = Vec::new();
    for ev in journal {
        let (kind, bytes) = encode_event(ev);
        let msg = JournalEvent {
            seq: ev.seq,
            kind,
            payload: Some(PayloadRef::inline(Bytes::from(bytes))),
        }
        .encode_to_vec();
        out.extend_from_slice(&(msg.len() as u32).to_le_bytes());
        out.extend_from_slice(&msg);
    }
    out
}

/// `git describe --always --dirty` of the repository (informational).
fn git_describe() -> String {
    std::process::Command::new("git")
        .args(["describe", "--always", "--dirty", "--abbrev=12"])
        .current_dir(common::repo_root())
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

/// The workspace's exact wasmtime pin (`wasmtime = { version = "=X", ...}`).
fn wasmtime_pin() -> String {
    let text = std::fs::read_to_string(common::repo_root().join("Cargo.toml")).unwrap_or_default();
    text.lines()
        .find(|l| l.starts_with("wasmtime = "))
        .and_then(|l| l.split("version = \"=").nth(1))
        .and_then(|r| r.split('"').next())
        .unwrap_or("unknown")
        .to_string()
}

/// Target triple of this build (approximation from `std::env::consts` + cfg).
fn target_triple() -> String {
    let arch = std::env::consts::ARCH;
    match std::env::consts::OS {
        "linux" => {
            let env = if cfg!(target_env = "musl") {
                "musl"
            } else {
                "gnu"
            };
            format!("{arch}-unknown-linux-{env}")
        }
        "macos" => format!("{arch}-apple-darwin"),
        os => format!("{arch}-unknown-{os}"),
    }
}

/// Today's UTC date `YYYY-MM-DD` (civil-from-days; no date crate).
fn today() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let z = (secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

// ----------------------------------------------------------------------------- main

struct Args {
    guest: String,
    entry: String,
    input: Option<String>,
    inputs: Option<String>,
    workflow: Option<String>,
    partition: u16,
    run_id: Option<String>,
    seed: [u8; 32],
    token_key: [u8; 32],
    deadline_nanos: Option<u64>,
    race_seed: Option<u64>,
    corpus: PathBuf,
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut positional = Vec::new();
    let mut a = Args {
        guest: String::new(),
        entry: String::new(),
        input: None,
        inputs: None,
        workflow: None,
        partition: DEFAULT_PARTITION,
        run_id: None,
        seed: [0x5a; 32],
        token_key: TEST_TOKEN_KEY,
        deadline_nanos: None,
        race_seed: None,
        corpus: common::repo_root().join("tests/replay-corpus"),
    };
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or_else(|| format!("{arg} needs a value"));
        match arg.as_str() {
            "--input" => a.input = Some(val()?),
            "--inputs" => a.inputs = Some(val()?),
            "--workflow" => a.workflow = Some(val()?),
            "--partition" => a.partition = val()?.parse().map_err(|e| format!("{e}"))?,
            "--run-id" => a.run_id = Some(val()?),
            "--seed-hex" => a.seed = unhex32(&val()?)?,
            "--token-key-hex" => a.token_key = unhex32(&val()?)?,
            "--deadline-nanos" => {
                a.deadline_nanos = Some(val()?.parse().map_err(|e| format!("{e}"))?);
            }
            "--race-seed" => a.race_seed = Some(val()?.parse().map_err(|e| format!("{e}"))?),
            "--corpus" => a.corpus = PathBuf::from(val()?),
            s if s.starts_with("--") => return Err(format!("unknown flag {s}")),
            _ => positional.push(arg),
        }
    }
    let [guest, entry] = <[String; 2]>::try_from(positional)
        .map_err(|p| format!("expected <guest> <entry>, got {p:?}"))?;
    if entry.is_empty()
        || !entry
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(format!("entry name {entry:?} must match [a-z0-9-]+"));
    }
    a.guest = guest;
    a.entry = entry;
    Ok(a)
}

fn load_script(spec: &str) -> Result<Script, String> {
    let text = if spec.trim_start().starts_with('{') {
        spec.to_string()
    } else {
        std::fs::read_to_string(spec).map_err(|e| format!("{spec}: {e}"))?
    };
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("--inputs: {e}"))?;
    Script::parse(&v)
}

async fn record(a: Args) -> Result<PathBuf, String> {
    let dir = a.corpus.join(&a.entry);
    if dir.exists() {
        return Err(format!(
            "{} exists: entries are never re-recorded (add a new entry instead)",
            dir.display()
        ));
    }
    let (module, workflow) = if a.guest.ends_with(".wasm") {
        let bytes = std::fs::read(&a.guest).map_err(|e| format!("{}: {e}", a.guest))?;
        let wf = a
            .workflow
            .clone()
            .ok_or("--workflow is required with a .wasm path")?;
        (bytes, wf)
    } else {
        (
            common::guest_wasm(&a.guest),
            a.workflow.clone().unwrap_or_else(|| a.guest.clone()),
        )
    };

    let clock = Arc::new(SetClock(AtomicU64::new(DEFAULT_ACCEPTED_AT)));
    let rt = Runtime::new(
        EngineConfig::default(),
        RuntimeOptions {
            clock: clock.clone(),
            token_key: a.token_key,
            ..RuntimeOptions::default()
        },
    )
    .map_err(|e| format!("runtime: {e}"))?;
    let p = rt.prepare(&module).map_err(|e| format!("prepare: {e}"))?;
    let run = RunRef {
        partition: a.partition,
        run_id: a
            .run_id
            .clone()
            .unwrap_or_else(|| format!("run-{}", a.entry)),
    };
    let input = match (&a.input, a.race_seed) {
        (Some(i), _) => i.clone(),
        (None, Some(seed)) => race_input(seed),
        (None, None) => String::new(),
    };
    let mut spec = RunSpec::new(run.clone(), workflow.clone(), input.as_bytes());
    spec.seed = RngSeed(a.seed);
    spec.deadline_nanos = a.deadline_nanos;

    let (feed, pre) = match (a.race_seed, &a.inputs) {
        (Some(_), Some(_)) => return Err("--race-seed and --inputs are exclusive".into()),
        (Some(seed), None) => (
            Feed::Race {
                rng: Rng(seed.wrapping_mul(0x1234_5678_9ABC_DEF1)),
                sent: 0,
            },
            Vec::new(),
        ),
        (None, inputs) => {
            let script = inputs
                .as_deref()
                .map(load_script)
                .transpose()?
                .unwrap_or_default();
            let initial = vec![spec.run_started_event(&p.module_hash_hex)];
            let pre = apply_actions(&initial, &script.pre);
            (Feed::Script { script, round: 0 }, pre)
        }
    };
    let (outcome, journal, activations) = drive(&rt, &clock, &p, &spec, pre, feed).await?;
    let (expected, terminal) = terminal_of(&outcome)?;

    // Self-check: the journal replays to the same outcome with zero appends.
    let (again, appended) = replay_journal(&rt, &p, run.clone(), journal.clone()).await;
    if again != outcome || appended != 0 {
        return Err(format!(
            "recorded journal does not replay clean: {again:?}, {appended} appended"
        ));
    }

    let bin = encode_journal(&journal);
    let input_json = serde_json::from_str::<Value>(&input)
        .ok()
        .map(|_| input.clone());
    let command = std::env::args()
        .skip(1)
        .map(|a| {
            if a.chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_./=:".contains(c))
            {
                a
            } else {
                format!("'{}'", a.replace('\'', r"'\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    let meta = EntryToml {
        name: a.entry.clone(),
        workflow,
        input_json,
        expected: expected.to_string(),
        partition: run.partition,
        run_id: run.run_id.clone(),
        token_key_hex: hex(&a.token_key),
        terminal_payload_sha256: hex(&Sha256::digest(&terminal)),
        journal_events: journal.len() as u64,
        module_sha256: hex(&Sha256::digest(&module)),
        journal_sha256: hex(&Sha256::digest(&bin)),
        recorded_with: RecordedWith {
            ostro: git_describe(),
            wasmtime: wasmtime_pin(),
            target: target_triple(),
            date: today(),
            tool: "record_corpus".to_string(),
            command: format!("record_corpus {command}"),
        },
    };
    let body = toml::to_string(&meta).map_err(|e| format!("entry.toml: {e}"))?;
    write_entry(&dir, &module, &bin, &body)?;
    println!(
        "recorded {} ({expected}, {} events, {activations} activations, {} + {} bytes)",
        dir.display(),
        journal.len(),
        module.len(),
        bin.len()
    );
    Ok(dir)
}

/// Create `dir` (must not exist) and write the three files.
fn write_entry(dir: &Path, module: &[u8], bin: &[u8], toml_body: &str) -> Result<(), String> {
    if let Some(parent) = dir.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("{}: {e}", parent.display()))?;
    }
    // create_dir (not _all) fails if a concurrent recorder created the entry meanwhile.
    std::fs::create_dir(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
    let header = "# QA-02 replay-corpus entry. Never edit or re-record; see ../README.md.\n";
    std::fs::write(dir.join("module.wasm"), module).map_err(|e| format!("module.wasm: {e}"))?;
    std::fs::write(dir.join("journal.bin"), bin).map_err(|e| format!("journal.bin: {e}"))?;
    std::fs::write(dir.join("entry.toml"), format!("{header}{toml_body}"))
        .map_err(|e| format!("entry.toml: {e}"))?;
    Ok(())
}

/// `--dump <entry-dir>`: print an existing entry's events (seq, kind, ts, summary) and its
/// terminal payload. Read-only.
fn dump(dir: &Path) -> Result<(), String> {
    let bin = std::fs::read(dir.join("journal.bin")).map_err(|e| format!("journal.bin: {e}"))?;
    let mut pos = 0usize;
    while pos + 4 <= bin.len() {
        let len = u32::from_le_bytes(bin[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let msg = bin
            .get(pos + 4..pos + 4 + len)
            .ok_or_else(|| format!("truncated record at {pos}"))?;
        pos += 4 + len;
        let je = JournalEvent::decode(msg).map_err(|e| format!("record: {e}"))?;
        let bytes = match je.payload.and_then(|p| p.kind) {
            Some(ostro_types::payload::payload_ref::Kind::Inline(b)) => b,
            other => return Err(format!("seq {}: non-inline payload {other:?}", je.seq)),
        };
        let ev = ostro_types::journal::decode_event(je.seq, je.kind, &bytes)
            .map_err(|e| format!("seq {}: {e}", je.seq))?;
        let detail = match &ev.body {
            EventBody::SignalMatched(m) => format!(" (payload_seq {})", m.payload_seq),
            EventBody::StepAttemptFailed(f) => format!(
                " ({} attempt {} {:?} retry_at {})",
                f.name, f.attempt, f.backoff, f.retry_at_nanos
            ),
            EventBody::RunCompleted(c) => {
                format!(" -> {}", String::from_utf8_lossy(&c.output.data))
            }
            EventBody::RunFailed(f) => format!(" -> {:?}", f.failure),
            EventBody::RunContinuedAsNew(c) => format!(
                " -> {}",
                String::from_utf8_lossy(&c.continuation.input.data)
            ),
            _ => String::new(),
        };
        println!(
            "{:>4} {:<18} {} {}{detail}",
            ev.seq,
            format!("{:?}", ev.body.kind()),
            ev.ts_nanos,
            ev.body.summary()
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if let [flag, dir] = argv.as_slice()
        && flag == "--dump"
    {
        return match dump(Path::new(dir)) {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("record_corpus --dump: {e}");
                ExitCode::FAILURE
            }
        };
    }
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("record_corpus: {e}");
            eprintln!(
                "usage: record_corpus <guest|x.wasm> <entry> [--input TEXT] [--inputs FILE|JSON] \
                 [--workflow W] [--partition N] [--run-id ID] [--seed-hex HEX] \
                 [--token-key-hex HEX] [--deadline-nanos N] [--race-seed N] [--corpus DIR]"
            );
            return ExitCode::from(2);
        }
    };
    match record(args).await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("record_corpus: {e}");
            ExitCode::FAILURE
        }
    }
}
