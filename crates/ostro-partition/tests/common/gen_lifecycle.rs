//! `rebuild_digest` generator hook for terminate, retry, run/schedule-to-start timeouts,
//! retention expiry and continue-as-new (plan **05-09**; stubbed by 05-02).
//!
//! `tests/rebuild_digest.rs` merges [`strategy`] into its command strategy and, for every
//! generated op, submits [`commands`] against the driver's runs (whose `next_seq` is
//! re-read from live state first).
//!
//! - [`LifecycleOp::StartTimed`] starts a run of its own (id derived from the op index) with
//!   a run timeout, schedule-to-start and/or retention. Deadlines are either already past
//!   (`requested_at_ms` is 100, so the live timer service fires them right away, racing the
//!   explicit `TimerFired` the op may also submit: one wins, the other is `Duplicate`) or
//!   never due. It may terminate and retry that run immediately.
//! - `Terminate`, `Retry`, `Complete` (resumable or not), `ContinueAsNew` and `Fire`
//!   (`RUN_TIMEOUT_OP` / `SCHEDULE_TO_START_OP` / `EXPIRE_OP`, `fired_at >= due`) act on the
//!   driver's runs, terminal ones included (rejections are deterministic too).
//! - Inbound `StartRun` batches come from `ContinueAsNew`: the successor start goes through
//!   the outbox and the shuffler, so `InboundBatch{StartRun}` is exercised without forging
//!   inbound seqs (a forged batch would race the shuffler's `inbound_seen` watermark).

use bytes::Bytes;
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, Derived, EXPIRE_OP, EventsAppended, JournalEvent, OutboxMsg,
    RUN_TIMEOUT_OP, RetryRun, SCHEDULE_TO_START_OP, SendMessage, TerminalStatus, TerminateRun,
    TimerFired, derived,
};
use ostro_types::{Command, PayloadRef, RunId};
use proptest::prelude::*;

use super::{GenRun, start_run};

/// A deadline that never comes due during a test.
const NEVER: u64 = u64::MAX / 8;

/// One generated lifecycle operation.
#[derive(Clone, Debug)]
pub enum LifecycleOp {
    /// Start an own run on `p` with optional timeouts/retention (0 = unset, else a small
    /// past-due value or [`NEVER`]), then optionally terminate it and retry it, then
    /// optionally fire one of its reserved timers explicitly.
    StartTimed {
        p: u16,
        run_timeout: u64,
        s2s: u64,
        retention: u64,
        key: Option<u8>,
        terminate: bool,
        retry: bool,
        fire: Option<u8>,
    },
    /// `TerminateRun` on driver run `i` (reason length `reason`).
    Terminate { i: usize, reason: u8 },
    /// `RetryRun` on driver run `i`.
    Retry { i: usize },
    /// `CompleteRun` on driver run `i` with the driver's token.
    Complete {
        i: usize,
        failed: bool,
        resumable: bool,
    },
    /// Continue-as-new of driver run `i`; the successor lives on the partition of run `j`.
    ContinueAsNew { i: usize, j: usize, events: u8 },
    /// `TimerFired` for a reserved op on driver run `i` (mostly no row: `Duplicate`).
    Fire {
        i: usize,
        which: u8,
        due: u64,
        late: u64,
    },
}

fn deadline() -> impl Strategy<Value = u64> {
    prop_oneof![3 => Just(0u64), 2 => 1u64..50, 1 => Just(NEVER)]
}

/// Strategy for one optional [`LifecycleOp`].
pub fn strategy() -> BoxedStrategy<Option<LifecycleOp>> {
    let op = prop_oneof![
        3 => (
            0u16..2,
            (deadline(), deadline(), deadline()),
            proptest::option::of(0u8..3),
            any::<bool>(),
            any::<bool>(),
            proptest::option::of(0u8..3),
        )
            .prop_map(|(p, (run_timeout, s2s, retention), key, terminate, retry, fire)| {
                LifecycleOp::StartTimed { p, run_timeout, s2s, retention, key, terminate, retry, fire }
            }),
        2 => (any::<usize>(), any::<u8>())
            .prop_map(|(i, reason)| LifecycleOp::Terminate { i, reason }),
        2 => any::<usize>().prop_map(|i| LifecycleOp::Retry { i }),
        2 => (any::<usize>(), any::<bool>(), any::<bool>())
            .prop_map(|(i, failed, resumable)| LifecycleOp::Complete { i, failed, resumable }),
        2 => (any::<usize>(), any::<usize>(), 0u8..3)
            .prop_map(|(i, j, events)| LifecycleOp::ContinueAsNew { i, j, events }),
        1 => (any::<usize>(), 0u8..3, 0u64..10_000, 0u64..5)
            .prop_map(|(i, which, due, late)| LifecycleOp::Fire { i, which, due, late }),
    ];
    proptest::option::weighted(0.8, op).boxed()
}

/// A run id on `p` derived from the op index (unique per scenario, stable across replays of
/// the same scenario).
fn own_id(p: u16, n: usize) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[2] = 0x09; // keeps clear of the driver's v7 ids
    b[10..18].copy_from_slice(&(n as u64).to_be_bytes());
    RunId::from_bytes(&b).expect("18 bytes")
}

fn reserved_op(which: u8) -> u64 {
    match which % 3 {
        0 => RUN_TIMEOUT_OP,
        1 => SCHEDULE_TO_START_OP,
        _ => EXPIRE_OP,
    }
}

fn fire(id: &RunId, op: u64, due: u64, late: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: id.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: due.saturating_add(late),
    })
}

fn terminate(id: &RunId, reason: usize, at: u64) -> Command {
    Command::terminate_run(TerminateRun {
        run_id: id.as_bytes().to_vec(),
        reason: "r".repeat(reason),
        terminated_at_ms: at,
    })
}

fn retry(id: &RunId, at: u64) -> Command {
    Command::retry_run(RetryRun {
        run_id: id.as_bytes().to_vec(),
        requested_at_ms: at,
    })
}

fn append(r: &GenRun, events: u64, derived: Vec<Derived>) -> Option<Command> {
    let t = r.token?;
    Some(Command::events_appended(EventsAppended {
        run_id: r.id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: r.next_seq,
        events: (r.next_seq..r.next_seq + events)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
            })
            .collect(),
        derived,
    }))
}

fn complete(status: TerminalStatus, resumable: bool) -> Derived {
    Derived::new(derived::Intent::CompleteRun(CompleteRun {
        status: status as i32,
        result: Some(PayloadRef::inline(Bytes::from_static(b"res"))),
        completed_at_ms: 900,
        resumable,
        parent_event: None,
    }))
}

/// `(partition, command)` pairs for `op` over the driver's runs; `n` is the op index.
pub fn commands(op: &LifecycleOp, runs: &[GenRun], n: usize) -> Vec<(u16, Command)> {
    let pick = |i: usize| (!runs.is_empty()).then(|| &runs[i % runs.len()]);
    match op {
        LifecycleOp::StartTimed {
            p,
            run_timeout,
            s2s,
            retention,
            key,
            terminate: term,
            retry: re,
            fire: f,
        } => {
            let id = own_id(*p, n);
            let mut s = start_run(&id);
            s.run_timeout_ms = *run_timeout;
            s.schedule_to_start_ms = *s2s;
            s.retention_ms = *retention;
            if let Some(k) = key {
                s.idempotency_key = format!("lk{k}").into_bytes();
            }
            let mut out = vec![(*p, Command::start_run(s))];
            if *term {
                out.push((*p, terminate(&id, 4, 150)));
            }
            if *re {
                out.push((*p, retry(&id, 160)));
            }
            if let Some(w) = f {
                let op = reserved_op(*w);
                let due = match op {
                    RUN_TIMEOUT_OP => 100u64.saturating_add(*run_timeout),
                    SCHEDULE_TO_START_OP => 100u64.saturating_add(*s2s),
                    _ => 150u64.saturating_add(*retention),
                };
                out.push((*p, fire(&id, op, due, 1)));
            }
            out
        }
        LifecycleOp::Terminate { i, reason } => pick(*i)
            .map(|r| vec![(r.partition, terminate(&r.id, usize::from(*reason) * 5, 200))])
            .unwrap_or_default(),
        LifecycleOp::Retry { i } => pick(*i)
            .map(|r| vec![(r.partition, retry(&r.id, 300))])
            .unwrap_or_default(),
        LifecycleOp::Complete {
            i,
            failed,
            resumable,
        } => pick(*i)
            .and_then(|r| {
                let status = if *failed {
                    TerminalStatus::Failed
                } else {
                    TerminalStatus::Completed
                };
                append(r, 1, vec![complete(status, *resumable)]).map(|c| vec![(r.partition, c)])
            })
            .unwrap_or_default(),
        LifecycleOp::ContinueAsNew { i, j, events } => pick(*i)
            .and_then(|r| {
                let target = pick(*j).map_or(r.partition, |t| t.partition);
                let succ = own_id(target, n);
                let msg = Derived::new(derived::Intent::SendMessage(SendMessage {
                    msg: Some(OutboxMsg::start_run(start_run(&succ))),
                }));
                append(
                    r,
                    u64::from(*events),
                    vec![msg, complete(TerminalStatus::ContinuedAsNew, false)],
                )
                .map(|c| vec![(r.partition, c)])
            })
            .unwrap_or_default(),
        LifecycleOp::Fire {
            i,
            which,
            due,
            late,
        } => pick(*i)
            .map(|r| vec![(r.partition, fire(&r.id, reserved_op(*which), *due, *late))])
            .unwrap_or_default(),
    }
}
