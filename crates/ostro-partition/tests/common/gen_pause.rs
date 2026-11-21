//! `rebuild_digest` generator hook for run pause / resume (plan **07-04**; OPS-01).
//!
//! `tests/rebuild_digest.rs` merges [`strategy`] into its command strategy and, for every
//! generated op, submits [`commands`] against the driver's runs (whose `next_seq` is re-read
//! from live state first). Outcomes are not asserted: the property is that rebuilding from the
//! log reproduces the live digest, plus the pause invariants checked after the drive (no run
//! both paused and with a ready row, no terminal run paused).
//!
//! - [`PauseOp::Own`] starts a run of its own (id derived from the op index), optionally with
//!   a run timeout (past due, so the live timer service fires it while the run may be paused,
//!   or never due), then runs a short scripted sequence: grant, pause (operator or
//!   token-carrying auto-pause, possibly with a stale token), inputs (timer fire of the run
//!   timeout, signal, cancel request), an executor write that a paused run rejects, resume,
//!   terminate and **RetryRun** (B7: a run that went terminal while paused must be retryable).
//! - `Pause`, `Resume`, `Grant`, `Events` (plain / `EnqueueReady` / `Suspend`), `Signal`,
//!   `Cancel`, `Terminate` and `Retry` act on the driver's runs, terminal ones included
//!   (rejections are deterministic too).

use bytes::Bytes;
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, Derived, EnqueueReady, EventsAppended, JournalEvent,
    PAUSE_REASON_OPERATOR, PAUSE_REASON_RETRIES_EXHAUSTED, PauseRun, RUN_TIMEOUT_OP, RequestCancel,
    ResumeRun, RetryRun, SignalRun, TerminateRun, TimerFired, derived,
};
use ostro_types::{AttemptToken, Command, Epoch, PayloadRef, RunId};
use proptest::prelude::*;

use super::{GenRun, start_run};

/// A deadline that never comes due during a test.
const NEVER: u64 = u64::MAX / 8;

/// How a pause is issued.
#[derive(Clone, Copy, Debug)]
pub enum PauseKind {
    /// Operator pause (no token).
    Operator,
    /// Auto-pause with the run's current token (accepted when it is the current attempt).
    Auto,
    /// Auto-pause with a token that is not the current attempt (`Fenced`).
    StaleAuto,
}

/// One generated pause/resume operation.
#[derive(Clone, Debug)]
pub enum PauseOp {
    /// An own run on `p`: start (optional run timeout: 0 unset, small past due, or never),
    /// optionally grant, pause, poke it with inputs and an executor write, then optionally
    /// resume, terminate and retry.
    Own {
        p: u16,
        run_timeout: u64,
        grant: bool,
        pause: PauseKind,
        fire_timeout: bool,
        inputs: bool,
        write: bool,
        resume: bool,
        terminate: bool,
        retry: bool,
        pause_after_retry: bool,
    },
    Pause {
        i: usize,
        kind: PauseKind,
    },
    Resume {
        i: usize,
    },
    /// `AttemptGranted` with the next token (`RunPaused` while paused).
    Grant {
        i: usize,
    },
    /// `EventsAppended` at the live journal end: 0 plain, 1 `EnqueueReady`, 2 `Suspend`.
    Events {
        i: usize,
        intent: u8,
    },
    Signal {
        i: usize,
    },
    Cancel {
        i: usize,
    },
    Terminate {
        i: usize,
    },
    Retry {
        i: usize,
    },
}

fn pause_kind() -> impl Strategy<Value = PauseKind> {
    prop_oneof![
        3 => Just(PauseKind::Operator),
        2 => Just(PauseKind::Auto),
        1 => Just(PauseKind::StaleAuto),
    ]
}

fn deadline() -> impl Strategy<Value = u64> {
    prop_oneof![3 => Just(0u64), 2 => 1u64..50, 1 => Just(NEVER)]
}

/// Strategy for one optional [`PauseOp`].
pub fn strategy() -> BoxedStrategy<Option<PauseOp>> {
    let own = (
        0u16..2,
        deadline(),
        pause_kind(),
        proptest::collection::vec(any::<bool>(), 9),
    )
        .prop_map(|(p, run_timeout, pause, f)| PauseOp::Own {
            p,
            run_timeout,
            grant: f[0],
            pause,
            fire_timeout: f[1],
            inputs: f[2],
            write: f[3],
            resume: f[4],
            terminate: f[5],
            retry: f[6],
            pause_after_retry: f[7] && f[8],
        });
    let i = any::<usize>();
    let op = prop_oneof![
        3 => own,
        3 => (i, pause_kind()).prop_map(|(i, kind)| PauseOp::Pause { i, kind }),
        3 => i.prop_map(|i| PauseOp::Resume { i }),
        1 => i.prop_map(|i| PauseOp::Grant { i }),
        2 => (i, 0u8..3).prop_map(|(i, intent)| PauseOp::Events { i, intent }),
        1 => i.prop_map(|i| PauseOp::Signal { i }),
        1 => i.prop_map(|i| PauseOp::Cancel { i }),
        1 => i.prop_map(|i| PauseOp::Terminate { i }),
        1 => i.prop_map(|i| PauseOp::Retry { i }),
    ];
    proptest::option::weighted(0.8, op).boxed()
}

/// A run id on `p` derived from the op index (stable across replays of the same scenario).
fn own_id(p: u16, n: usize) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[2] = 0x07; // keeps clear of the driver's v7 ids and gen_lifecycle's 0x09
    b[10..18].copy_from_slice(&(n as u64).to_be_bytes());
    RunId::from_bytes(&b).expect("18 bytes")
}

fn msg(t: AttemptToken) -> Option<AttemptTokenMsg> {
    Some(AttemptTokenMsg {
        epoch: t.epoch.0,
        n: t.n,
    })
}

fn pause(id: &RunId, kind: PauseKind, token: Option<AttemptToken>, at: u64) -> Command {
    let (reason, token) = match kind {
        PauseKind::Operator => (PAUSE_REASON_OPERATOR, None),
        PauseKind::Auto => (PAUSE_REASON_RETRIES_EXHAUSTED, token.and_then(msg)),
        PauseKind::StaleAuto => {
            let t = token.unwrap_or(AttemptToken {
                epoch: Epoch(1),
                n: 0,
            });
            (
                PAUSE_REASON_RETRIES_EXHAUSTED,
                msg(AttemptToken {
                    epoch: t.epoch,
                    n: t.n.wrapping_add(3),
                }),
            )
        }
    };
    Command::pause_run(PauseRun {
        run_id: id.as_bytes().to_vec(),
        reason,
        detail: "gen".into(),
        token,
        requested_at_ms: at,
    })
}

fn resume(id: &RunId, at: u64) -> Command {
    Command::resume_run(ResumeRun {
        run_id: id.as_bytes().to_vec(),
        requested_at_ms: at,
    })
}

fn grant(id: &RunId, t: AttemptToken) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: msg(t),
        after_failure: false,
    })
}

fn signal(id: &RunId, at: u64) -> Command {
    Command::signal_run(SignalRun {
        run_id: id.as_bytes().to_vec(),
        name: "p".into(),
        event: Some(PayloadRef::inline(Bytes::from_static(b"sig"))),
        request_id: vec![],
        sent_at_ms: at,
    })
}

fn cancel(id: &RunId, at: u64) -> Command {
    Command::request_cancel(RequestCancel {
        run_id: id.as_bytes().to_vec(),
        reason: "gen".into(),
        requested_at_ms: at,
    })
}

fn terminate(id: &RunId, at: u64) -> Command {
    Command::terminate_run(TerminateRun {
        run_id: id.as_bytes().to_vec(),
        reason: "gen".into(),
        terminated_at_ms: at,
    })
}

fn retry(id: &RunId, at: u64) -> Command {
    Command::retry_run(RetryRun {
        run_id: id.as_bytes().to_vec(),
        requested_at_ms: at,
    })
}

fn events(id: &RunId, t: AttemptToken, from: u64, n: u64, intent: u8) -> Command {
    let d = match intent {
        1 => vec![Derived::new(derived::Intent::EnqueueReady(EnqueueReady {
            ready_at_ms: 400,
        }))],
        2 => vec![Derived::suspend()],
        _ => vec![],
    };
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: msg(t),
        from_seq: from,
        events: (from..from + n)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
            })
            .collect(),
        derived: d,
    })
}

/// The token after the driver's last one (`None` = the driver never granted the run).
fn next_token(r: &GenRun) -> Option<AttemptToken> {
    r.token.map(|t| AttemptToken {
        epoch: t.epoch,
        n: t.n.saturating_add(1),
    })
}

/// `(partition, command)` pairs for `op` over the driver's runs; `n` is the op index.
pub fn commands(op: &PauseOp, runs: &[GenRun], n: usize) -> Vec<(u16, Command)> {
    let pick = |i: usize| (!runs.is_empty()).then(|| &runs[i % runs.len()]);
    match op {
        PauseOp::Own {
            p,
            run_timeout,
            grant: g,
            pause: kind,
            fire_timeout,
            inputs,
            write,
            resume: res,
            terminate: term,
            retry: re,
            pause_after_retry,
        } => {
            let id = own_id(*p, n);
            let mut s = start_run(&id);
            s.run_timeout_ms = *run_timeout;
            let mut out = vec![(*p, Command::start_run(s))];
            // The driver's epoch is unknown here; epoch 1 is the first leader term (later
            // terms make these grants StaleEpoch, which is just as deterministic).
            let t = AttemptToken {
                epoch: Epoch(1),
                n: 1,
            };
            if *g {
                out.push((*p, grant(&id, t)));
                // Journal seq 0 so inputs are journaled (not B1-pending) and Suspend works.
                out.push((*p, events(&id, t, 0, 1, 0)));
            }
            out.push((*p, pause(&id, *kind, g.then_some(t), 200)));
            if *fire_timeout && *run_timeout > 0 {
                let due = 100u64.saturating_add(*run_timeout);
                out.push((
                    *p,
                    Command::timer_fired(TimerFired {
                        run_id: id.as_bytes().to_vec(),
                        op: RUN_TIMEOUT_OP,
                        due_at_ms: due,
                        fired_at_ms: due.saturating_add(1),
                    }),
                ));
            }
            if *inputs {
                out.push((*p, signal(&id, 210)));
                out.push((*p, cancel(&id, 220)));
            }
            if *write {
                // Rejected RunPaused while paused (Fenced/terminal otherwise).
                out.push((*p, events(&id, t, 1, 1, 1)));
                out.push((
                    *p,
                    grant(
                        &id,
                        AttemptToken {
                            epoch: Epoch(1),
                            n: 2,
                        },
                    ),
                ));
            }
            if *res {
                out.push((*p, resume(&id, 300)));
            }
            if *term {
                out.push((*p, terminate(&id, 310)));
            }
            if *re {
                out.push((*p, retry(&id, 320)));
                if *pause_after_retry {
                    out.push((*p, pause(&id, PauseKind::Operator, None, 330)));
                }
            }
            out
        }
        PauseOp::Pause { i, kind } => pick(*i)
            .map(|r| vec![(r.partition, pause(&r.id, *kind, r.token, 500))])
            .unwrap_or_default(),
        PauseOp::Resume { i } => pick(*i)
            .map(|r| vec![(r.partition, resume(&r.id, 600))])
            .unwrap_or_default(),
        PauseOp::Grant { i } => pick(*i)
            .and_then(|r| next_token(r).map(|t| vec![(r.partition, grant(&r.id, t))]))
            .unwrap_or_default(),
        PauseOp::Events { i, intent } => pick(*i)
            .and_then(|r| {
                let t = r.token?;
                let evs = u64::from(*intent != 2 || r.next_seq == 0);
                Some(vec![(
                    r.partition,
                    events(&r.id, t, r.next_seq, evs, *intent),
                )])
            })
            .unwrap_or_default(),
        PauseOp::Signal { i } => pick(*i)
            .map(|r| vec![(r.partition, signal(&r.id, 700))])
            .unwrap_or_default(),
        PauseOp::Cancel { i } => pick(*i)
            .map(|r| vec![(r.partition, cancel(&r.id, 710))])
            .unwrap_or_default(),
        PauseOp::Terminate { i } => pick(*i)
            .map(|r| vec![(r.partition, terminate(&r.id, 720))])
            .unwrap_or_default(),
        PauseOp::Retry { i } => pick(*i)
            .map(|r| vec![(r.partition, retry(&r.id, 730))])
            .unwrap_or_default(),
    }
}
