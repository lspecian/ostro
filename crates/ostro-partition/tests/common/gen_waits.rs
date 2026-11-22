//! `rebuild_digest` generator hook for waits, signals, await tokens and graceful cancel
//! (plan **05-08**; hook stubbed by 05-02).
//!
//! `tests/rebuild_digest.rs` merges [`strategy`] into its command strategy and, for every
//! generated op, submits [`commands`] against the driver's runs (their `next_seq` is re-read
//! from live state first). The ops are mostly valid, with deliberate repeats and misses:
//!
//! - `Register`: an `EventsAppended` with one opaque event (the wait's own
//!   `SignalWaitStarted`/`AwaitTokenCreated`) and a `RegisterWait` for its seq, signal or token,
//!   optionally with a timeout (past due, so the live timer service fires it as
//!   `WaitTimedOut`, or far future), optionally repeated in the same batch (idempotency) and
//!   optionally with a `CancelWait` for an earlier op.
//! - `Signal`: `SignalRun` with or without a request id (a small id space gives `Duplicate`s),
//!   inline or a 1 MiB blob-backed event (never fetched: apply has no blob access). Runs whose
//!   journal is empty get B1 `Pending` slots.
//! - `Token`: `CompleteToken` for a recent op (token waits complete, repeats are `Duplicate`,
//!   signal waits and plain events are rejected), inline or blob event.
//! - `Cancel`: `RequestCancel` (repeats are `Duplicate`, terminal runs `RunTerminal`).
//! - `Fire`: `TimerFired` for a recent op at one of the generated due times (mostly a
//!   `Duplicate` because the live timer service fired it already).
//!
//! Journal events are opaque blobs, as in the base generator. Outcomes are not asserted: the
//! property is that rebuilding from the log reproduces the live digest.

use bytes::Bytes;
use ostro_types::command::{
    AttemptTokenMsg, CompleteToken, Derived, EventsAppended, JournalEvent, RequestCancel,
    SignalRun, TimerFired, WaitKind,
};
use ostro_types::{Command, PayloadRef};
use proptest::prelude::*;

use super::GenRun;

/// Signal names used by the generator (two, so FIFO per name and cross-name misses occur).
const NAMES: [&str; 2] = ["a", "b"];

/// Wait-timeout due times: past (fired by the live timer service) or never due in a test.
const DUES: [u64; 3] = [5, 17, u64::MAX / 4];

/// One generated wait/signal/token/cancel operation.
#[derive(Clone, Debug)]
pub enum WaitOp {
    Register {
        run: usize,
        token: bool,
        name: usize,
        /// Index into [`DUES`] (`None` = no timeout).
        due: Option<usize>,
        /// Repeat the same `RegisterWait` in the batch (must be a no-op).
        repeat: bool,
        /// `CancelWait` for the op `back` seqs below this one.
        cancel_back: Option<u64>,
    },
    Signal {
        run: usize,
        name: usize,
        request_id: Option<u8>,
        blob: bool,
    },
    Token {
        run: usize,
        back: u64,
        blob: bool,
    },
    Cancel {
        run: usize,
    },
    Fire {
        run: usize,
        back: u64,
        due: usize,
    },
}

/// Distance below `next_seq` of a `Token`/`Fire` target: mostly 1, the op of the latest
/// `Register` on that run.
fn back() -> impl Strategy<Value = u64> {
    prop_oneof![3 => Just(1u64), 1 => 2u64..4]
}

/// Strategy for one optional [`WaitOp`] (always `Some`).
pub fn strategy() -> BoxedStrategy<Option<WaitOp>> {
    let run = any::<usize>();
    prop_oneof![
        4 => (
            run,
            proptest::bool::weighted(0.3),
            0..NAMES.len(),
            proptest::option::weighted(0.5, 0..DUES.len()),
            proptest::bool::weighted(0.15),
            proptest::option::weighted(0.15, 1u64..4),
        )
            .prop_map(|(run, token, name, due, repeat, cancel_back)| WaitOp::Register {
                run,
                token,
                name,
                due,
                repeat,
                cancel_back,
            }),
        4 => (
            run,
            0..NAMES.len(),
            proptest::option::weighted(0.4, 0u8..3),
            proptest::bool::weighted(0.1),
        )
            .prop_map(|(run, name, request_id, blob)| WaitOp::Signal {
                run,
                name,
                request_id,
                blob,
            }),
        2 => (run, back(), proptest::bool::weighted(0.1))
            .prop_map(|(run, back, blob)| WaitOp::Token { run, back, blob }),
        1 => run.prop_map(|run| WaitOp::Cancel { run }),
        1 => (run, back(), 0..DUES.len()).prop_map(|(run, back, due)| WaitOp::Fire {
            run,
            back,
            due,
        }),
    ]
    .prop_map(Some)
    .boxed()
}

fn event(blob: bool, tag: &'static [u8], n: usize) -> PayloadRef {
    if blob {
        let mut sha = [0x5e; 32];
        sha[..8].copy_from_slice(&(n as u64).to_be_bytes());
        PayloadRef::blob(sha, 1 << 20)
    } else {
        PayloadRef::inline(Bytes::from_static(tag))
    }
}

/// `(partition, command)` pairs for `op` over the driver's runs; `n` is the op index (unique
/// per scenario).
pub fn commands(op: &WaitOp, runs: &[GenRun], n: usize) -> Vec<(u16, Command)> {
    let pick = |i: usize| (!runs.is_empty()).then(|| &runs[i % runs.len()]);
    // EventsAppended needs the current attempt token: prefer runs the driver granted.
    let granted: Vec<&GenRun> = runs.iter().filter(|r| r.token.is_some()).collect();
    let pick_granted = |i: usize| {
        if granted.is_empty() {
            pick(i)
        } else {
            Some(granted[i % granted.len()])
        }
    };
    let id = |r: &GenRun| r.id.as_bytes().to_vec();
    match op {
        WaitOp::Register {
            run,
            token,
            name,
            due,
            repeat,
            cancel_back,
        } => {
            let Some(r) = pick_granted(*run) else {
                return vec![];
            };
            let t = r.token.unwrap_or(ostro_types::AttemptToken {
                epoch: ostro_types::Epoch(1),
                n: 1,
            });
            let op = r.next_seq;
            let (kind, name) = if *token {
                (WaitKind::Token, "")
            } else {
                (WaitKind::Signal, NAMES[*name])
            };
            let due_ms = due.map_or(0, |d| DUES[d]);
            let mut derived = vec![Derived::register_wait(op, kind, name, due_ms, n as u64)];
            if *repeat {
                derived.push(Derived::register_wait(op, kind, name, due_ms, n as u64));
            }
            if let Some(back) = cancel_back
                && let Some(prev) = op.checked_sub(*back)
            {
                derived.push(Derived::cancel_wait(prev));
            }
            vec![(
                r.partition,
                Command::events_appended(EventsAppended {
                    run_id: id(r),
                    token: Some(AttemptTokenMsg {
                        epoch: t.epoch.0,
                        n: t.n,
                    }),
                    from_seq: op,
                    events: vec![JournalEvent {
                        seq: op,
                        kind: 16,
                        payload: Some(PayloadRef::inline(Bytes::from_static(b"w"))),
                        ..Default::default()
                    }],
                    derived,
                }),
            )]
        }
        WaitOp::Signal {
            run,
            name,
            request_id,
            blob,
        } => {
            let Some(r) = pick(*run) else { return vec![] };
            vec![(
                r.partition,
                Command::signal_run(SignalRun {
                    run_id: id(r),
                    name: NAMES[*name].into(),
                    event: Some(event(*blob, b"\x01sig", n)),
                    request_id: request_id
                        .map(|k| format!("req-{k}").into_bytes())
                        .unwrap_or_default(),
                    sent_at_ms: n as u64,
                }),
            )]
        }
        WaitOp::Token { run, back, blob } => {
            let Some(r) = pick_granted(*run) else {
                return vec![];
            };
            vec![(
                r.partition,
                Command::complete_token(CompleteToken {
                    run_id: id(r),
                    op: r.next_seq.saturating_sub(*back),
                    event: Some(event(*blob, b"\x01tok", n)),
                    completed_at_ms: n as u64,
                }),
            )]
        }
        WaitOp::Cancel { run } => {
            let Some(r) = pick(*run) else { return vec![] };
            vec![(
                r.partition,
                Command::request_cancel(RequestCancel {
                    run_id: id(r),
                    reason: format!("gen-{n}"),
                    requested_at_ms: n as u64,
                }),
            )]
        }
        WaitOp::Fire { run, back, due } => {
            let Some(r) = pick_granted(*run) else {
                return vec![];
            };
            let due_at_ms = DUES[*due];
            vec![(
                r.partition,
                Command::timer_fired(TimerFired {
                    run_id: id(r),
                    op: r.next_seq.saturating_sub(*back),
                    due_at_ms,
                    fired_at_ms: due_at_ms.saturating_add(1),
                }),
            )]
        }
    }
}
