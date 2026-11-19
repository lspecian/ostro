//! Replay core tests (unit + proptest).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ostro_types::journal::{AttemptBackoff, Failure, StepAttemptFailed};
use ostro_types::journal::{
    AwaitTokenCreated, CANCELLATION_OP, CancelRequested, ChildCompleted, ChildOptions,
    ChildStarted, CommandHash, CommandKind, CommandMeta, EffectCompleted, EffectRequest,
    EffectScheduled, Event, EventBody, EventKind, HASH_VER, HttpRequest, ObjectRequest,
    OpCancelled, PatchMarker, Payload, RESULT_CODEC_VER, RecordedResult, RngSeed, RunCompleted,
    RunStarted, SignalWaitStarted, SqlRequest, StatusPublished, StepCompleted, TimerFired,
    TimerStarted, WaitMode, WaitResolved,
};
use proptest::prelude::*;

use super::*;
use crate::env::{Clock, RandomStream};

const ACCEPTED: u64 = 500;
const OPEN_STEP: &str = "journaled command inside an open step";

/// Clock that advances 1000 ns per reading.
struct TestClock(AtomicU64);

impl Clock for TestClock {
    fn now_unix_nanos(&self) -> u64 {
        self.0.fetch_add(1_000, Ordering::SeqCst)
    }
}

fn clock(start: u64) -> Arc<dyn Clock> {
    Arc::new(TestClock(AtomicU64::new(start)))
}

fn run_started() -> Event {
    Event {
        seq: 0,
        ts_nanos: ACCEPTED,
        body: EventBody::RunStarted(RunStarted {
            workflow: "wf".into(),
            workflow_version: 1,
            input: payload(),
            rng_seed: RngSeed([7; 32]),
            accepted_at_nanos: ACCEPTED,
            deadline_nanos: None,
            module_hash: "m".into(),
            tenant: None,
            parent_run_id: None,
            continued_from: None,
        }),
    }
}

fn payload() -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: b"1".to_vec(),
    }
}

fn rr() -> RecordedResult {
    RecordedResult {
        codec_ver: RESULT_CODEC_VER,
        bytes: vec![1],
    }
}

fn h(n: u8) -> CommandHash {
    CommandHash([n; 16])
}

fn meta(hash: CommandHash) -> CommandMeta {
    CommandMeta {
        hash_ver: HASH_VER,
        hash,
    }
}

/// A live event body for a command of `kind`.
fn body_for(kind: CommandKind, hash: CommandHash) -> EventBody {
    let meta = meta(hash);
    match kind {
        CommandKind::Step => EventBody::StepCompleted(StepCompleted {
            meta,
            name: "s".into(),
            outcome: rr(),
            fuel_used: 0,
        }),
        CommandKind::Http => EventBody::EffectScheduled(EffectScheduled {
            meta,
            request: EffectRequest::Http(HttpRequest {
                method: "GET".into(),
                url: "https://x".into(),
                headers: vec![],
                body: None,
                timeout_ms: None,
                idempotency_key: None,
                redact: vec![],
            }),
        }),
        CommandKind::Sql => EventBody::EffectScheduled(EffectScheduled {
            meta,
            request: EffectRequest::Sql(SqlRequest {
                connection: "db".into(),
                statement: "select 1".into(),
                params: vec![],
                exactly_once: false,
            }),
        }),
        CommandKind::Object => EventBody::EffectScheduled(EffectScheduled {
            meta,
            request: EffectRequest::Object(ObjectRequest {
                connection: "s3".into(),
                op: "get".into(),
                key: "k".into(),
                body: None,
            }),
        }),
        CommandKind::Sleep | CommandKind::SleepUntil => EventBody::TimerStarted(TimerStarted {
            meta,
            fire_at_nanos: 0,
        }),
        CommandKind::WaitSignal => EventBody::SignalWaitStarted(SignalWaitStarted {
            meta,
            name: "go".into(),
            timeout_ms: None,
        }),
        CommandKind::StartChild => EventBody::ChildStarted(ChildStarted {
            meta,
            workflow: "child".into(),
            input: payload(),
            options: ChildOptions {
                queue: None,
                version: None,
                detached: false,
                run_timeout_ms: None,
            },
        }),
        CommandKind::CreateAwaitToken => EventBody::AwaitTokenCreated(AwaitTokenCreated {
            meta,
            token: "ot1_x".into(),
            timeout_ms: None,
        }),
        CommandKind::CancelOp => EventBody::OpCancelled(OpCancelled { meta, op: 1 }),
        CommandKind::Wait => EventBody::WaitResolved(WaitResolved {
            meta,
            completed: vec![],
        }),
        CommandKind::Patch => EventBody::PatchMarker(PatchMarker {
            meta,
            change_id: "p".into(),
        }),
        CommandKind::SetStatus => EventBody::StatusPublished(StatusPublished {
            meta,
            json: "{}".into(),
        }),
        CommandKind::Terminal => EventBody::RunCompleted(RunCompleted {
            meta,
            output: payload(),
        }),
    }
}

/// Issue a command; returns `(seq, replayed)`.
fn cmd(
    st: &mut ReplayState,
    kind: CommandKind,
    hash: CommandHash,
) -> Result<(u64, bool), ReplayError> {
    let actual = move || format!("{kind:?}");
    match st.command(kind, hash, &actual, |_| body_for(kind, hash))? {
        CommandResult::Replayed(ev) => Ok((ev.seq, true)),
        CommandResult::Live { seq, .. } => Ok((seq, false)),
    }
}

fn fresh() -> ReplayState {
    ReplayState::new(vec![run_started()], clock(10_000)).unwrap()
}

fn input(seq: u64, body: EventBody) -> Event {
    Event {
        seq,
        ts_nanos: 9_000_000_000_000 + seq,
        body,
    }
}

fn timer_fired(seq: u64, op: u64) -> Event {
    input(seq, EventBody::TimerFired(TimerFired { op }))
}

/// Record `cmds` live; after command `j`, deliver `inputs_after[j]` input events.
fn record(cmds: &[(CommandKind, CommandHash)], inputs_after: &[usize]) -> Vec<Event> {
    let mut st = fresh();
    for (j, (kind, hash)) in cmds.iter().enumerate() {
        let (seq, replayed) = cmd(&mut st, *kind, *hash).unwrap();
        assert!(!replayed);
        st.take_pending();
        let n = inputs_after.get(j).copied().unwrap_or(0);
        let mut evs = Vec::new();
        for k in 0..n {
            evs.push(timer_fired(st.journal().len() as u64 + k as u64, seq));
        }
        st.deliver_inputs(evs).unwrap();
    }
    st.journal().to_vec()
}

/// Seqs of the guest-command events of a journal, in order.
fn command_seqs(journal: &[Event]) -> Vec<u64> {
    journal
        .iter()
        .filter(|e| e.body.is_guest_command())
        .map(|e| e.seq)
        .collect()
}

fn replay(journal: Vec<Event>) -> ReplayState {
    ReplayState::new(journal, clock(5_000_000_000_000_000)).unwrap()
}

fn nd(r: Result<impl std::fmt::Debug, ReplayError>) -> crate::error::NonDeterminism {
    match r {
        Err(ReplayError::NonDeterminism(nd)) => nd,
        other => panic!("expected nondeterminism, got {other:?}"),
    }
}

fn is_open_step<T: std::fmt::Debug>(r: Result<T, ReplayError>) -> bool {
    matches!(r, Err(ReplayError::Protocol(ref p)) if p.0 == OPEN_STEP)
}

const SAMPLE: [(CommandKind, u8); 5] = [
    (CommandKind::Http, 1),
    (CommandKind::Sleep, 2),
    (CommandKind::SetStatus, 3),
    (CommandKind::StartChild, 4),
    (CommandKind::Patch, 5),
];

fn sample() -> Vec<(CommandKind, CommandHash)> {
    SAMPLE.iter().map(|(k, n)| (*k, h(*n))).collect()
}

#[test]
fn new_validates_journal() {
    assert!(matches!(
        ReplayState::new(vec![], clock(0)),
        Err(ReplayError::BadJournal(_))
    ));
    assert!(matches!(
        ReplayState::new(vec![timer_fired(0, 1)], clock(0)),
        Err(ReplayError::BadJournal(_))
    ));
    assert!(matches!(
        ReplayState::new(vec![run_started(), timer_fired(2, 1)], clock(0)),
        Err(ReplayError::BadJournal(_))
    ));
    let st = fresh();
    assert_eq!(st.run().accepted_at_nanos, ACCEPTED);
    assert!(!st.is_replaying());
    assert_eq!(st.next_command_hash_ver(), None);
}

#[test]
fn cursor_live_appends() {
    let mut st = fresh();
    for (i, kind) in [
        CommandKind::Http,
        CommandKind::Sleep,
        CommandKind::SetStatus,
    ]
    .into_iter()
    .enumerate()
    {
        let (seq, replayed) = cmd(&mut st, kind, h(i as u8)).unwrap();
        assert_eq!(seq, i as u64 + 1);
        assert!(!replayed);
    }
    assert_eq!(st.pending_len(), 3);
    assert!(matches!(
        st.op_kind(1).map(|e| e.body.kind()),
        Some(EventKind::EffectScheduled)
    ));
    let pending = st.take_pending();
    assert_eq!(st.pending_len(), 0);
    let seqs: Vec<u64> = pending.iter().map(|e| e.seq).collect();
    let ts: Vec<u64> = pending.iter().map(|e| e.ts_nanos).collect();
    assert_eq!(seqs, [1, 2, 3]);
    assert_eq!(ts, [10_000, 11_000, 12_000]);
    assert_eq!(st.journal().len(), 4);
    assert!(!st.is_replaying());
}

#[test]
fn cursor_replay_no_appends() {
    let cmds = sample();
    let journal = record(&cmds, &[]);
    let seqs = command_seqs(&journal);
    let mut st = replay(journal);
    assert_eq!(st.next_command_hash_ver(), Some(HASH_VER));
    for (i, (kind, hash)) in cmds.iter().enumerate() {
        assert!(st.is_replaying());
        let (seq, replayed) = cmd(&mut st, *kind, *hash).unwrap();
        assert!(replayed);
        assert_eq!(seq, seqs[i]);
    }
    assert!(!st.is_replaying());
    assert_eq!(st.pending_len(), 0);
    st.finish().unwrap();
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn cursor_skips_inputs() {
    let cmds = sample();
    let journal = record(&cmds, &[2, 0, 3, 1, 0]);
    let seqs = command_seqs(&journal);
    assert_eq!(seqs, [1, 4, 5, 9, 11]);
    let mut st = replay(journal);
    for (i, (kind, hash)) in cmds.iter().enumerate() {
        assert_eq!(cmd(&mut st, *kind, *hash).unwrap(), (seqs[i], true));
    }
    st.finish().unwrap();
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn cursor_divergence_changed_hash() {
    let journal = record(
        &[(CommandKind::Http, h(1)), (CommandKind::Sleep, h(2))],
        &[1, 0],
    );
    let mut st = replay(journal);
    cmd(&mut st, CommandKind::Http, h(1)).unwrap();
    let e = nd(cmd(&mut st, CommandKind::Sleep, h(9)));
    assert_eq!(e.seq, 3);
    assert_eq!(e.expected, "sleep #02020202");
    assert_eq!(e.actual, "Sleep #09090909");
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn cursor_divergence_changed_kind() {
    let journal = record(
        &[(CommandKind::Http, h(1)), (CommandKind::Sleep, h(2))],
        &[],
    );
    let mut st = replay(journal);
    cmd(&mut st, CommandKind::Http, h(1)).unwrap();
    let e = nd(cmd(&mut st, CommandKind::Sql, h(2)));
    assert_eq!(e.seq, 2);
    assert_eq!(e.expected, "sleep #02020202");
    assert_eq!(e.actual, "Sql #02020202");
}

#[test]
fn finish_trailing_commands() {
    let cmds = sample();
    let journal = record(&cmds, &[1, 1, 1, 1, 1]);
    let seqs = command_seqs(&journal);
    let mut st = replay(journal);
    for (kind, hash) in &cmds[..2] {
        cmd(&mut st, *kind, *hash).unwrap();
    }
    let e = nd(st.finish());
    assert_eq!(e.seq, seqs[2]);
    assert_eq!(e.expected, "set-status #03030303");
    assert_eq!(e.actual, "run returned");

    // A trailing terminal event is compared through command(Terminal), not finish().
    let journal = record(
        &[(CommandKind::Http, h(1)), (CommandKind::Terminal, h(14))],
        &[1, 0],
    );
    let mut st = replay(journal);
    cmd(&mut st, CommandKind::Http, h(1)).unwrap();
    st.finish().unwrap();
    assert_eq!(
        cmd(&mut st, CommandKind::Terminal, h(14)).unwrap(),
        (3, true)
    );
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn time_rule() {
    // Record.
    let mut st = fresh();
    let mut rec = vec![st.logical_now_nanos()];
    cmd(&mut st, CommandKind::Http, h(1)).unwrap();
    rec.push(st.logical_now_nanos());
    st.take_pending();
    st.deliver_inputs(vec![timer_fired(2, 1)]).unwrap();
    rec.push(st.logical_now_nanos());
    cmd(&mut st, CommandKind::Sleep, h(2)).unwrap();
    rec.push(st.logical_now_nanos());
    // The delivered input does not move logical now, but the next live command is
    // stamped at least the input's ts (monotonic over all journaled events).
    assert_eq!(rec, [ACCEPTED, 10_000, 10_000, 9_000_000_000_002]);
    st.take_pending();
    let journal = st.journal().to_vec();

    // Replay with a different wall clock: identical values at identical points.
    let mut st = replay(journal);
    let mut rep = vec![st.logical_now_nanos()];
    cmd(&mut st, CommandKind::Http, h(1)).unwrap();
    rep.push(st.logical_now_nanos());
    rep.push(st.logical_now_nanos());
    cmd(&mut st, CommandKind::Sleep, h(2)).unwrap();
    rep.push(st.logical_now_nanos());
    assert_eq!(rep, rec);
    // Going live afterwards stamps from the clock.
    let (seq, live) = cmd(&mut st, CommandKind::SetStatus, h(3)).unwrap();
    assert!(!live);
    assert_eq!(st.logical_now_nanos(), st.journal()[seq as usize].ts_nanos);
}

/// Regression (03-09 observation): a frozen or lagging wall clock must not stamp live
/// events earlier than a committed input. Journal ts never decrease across live
/// appends, and logical now after the wait that consumed a timer is >= its fire ts.
#[test]
fn time_monotonic_over_inputs() {
    const FIRE: u64 = 9_000_000_000_000;
    // Frozen clock far behind the timer's fire time (e.g. a skewed node).
    struct Frozen;
    impl Clock for Frozen {
        fn now_unix_nanos(&self) -> u64 {
            1_000
        }
    }
    let check = |st: &ReplayState| {
        let ts: Vec<u64> = st.journal().iter().map(|e| e.ts_nanos).collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "ts decreased: {ts:?}");
    };

    let mut st = ReplayState::new(vec![run_started()], Arc::new(Frozen)).unwrap();
    let (op, _) = cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    st.take_pending();
    st.deliver_inputs(vec![Event {
        seq: 2,
        ts_nanos: FIRE,
        body: EventBody::TimerFired(TimerFired { op }),
    }])
    .unwrap();
    assert_eq!(
        st.wait(&[op], WaitMode::All, h(2)).unwrap(),
        WaitOutcome::Resolved(vec![op])
    );
    assert!(
        st.logical_now_nanos() >= FIRE,
        "now() after the timer < fire ts"
    );
    cmd(&mut st, CommandKind::SetStatus, h(3)).unwrap();
    assert_eq!(st.logical_now_nanos(), FIRE);
    check(&st);
    let journal = st.journal().to_vec();

    // Inputs already in the loaded journal count too (re-activation after eviction).
    let mut st = ReplayState::new(journal[..3].to_vec(), Arc::new(Frozen)).unwrap();
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    assert_eq!(st.logical_now_nanos(), journal[1].ts_nanos);
    st.wait(&[op], WaitMode::All, h(2)).unwrap();
    assert_eq!(st.logical_now_nanos(), FIRE);
    check(&st);

    // Replay returns the recorded values at the same points.
    let mut st = ReplayState::new(journal, Arc::new(Frozen)).unwrap();
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    st.wait(&[op], WaitMode::All, h(2)).unwrap();
    assert_eq!(st.logical_now_nanos(), FIRE);
    assert_eq!(st.pending_len(), 0);
}

/// a = Sleep (op 1), b = Http (op 2), seqs 3..=6 filler commands.
fn wait_fixture() -> ReplayState {
    let mut st = fresh();
    assert_eq!(cmd(&mut st, CommandKind::Sleep, h(1)).unwrap().0, 1);
    assert_eq!(cmd(&mut st, CommandKind::Http, h(2)).unwrap().0, 2);
    for n in 3..=6 {
        cmd(&mut st, CommandKind::SetStatus, h(n)).unwrap();
    }
    st.take_pending();
    st
}

#[test]
fn wait_live_resolution() {
    let mut st = wait_fixture();
    st.deliver_inputs(vec![input(
        7,
        EventBody::EffectCompleted(EffectCompleted {
            op: 2,
            outcome: rr(),
        }),
    )])
    .unwrap();
    assert_eq!(
        st.wait(&[1, 2], WaitMode::All, h(50)).unwrap(),
        WaitOutcome::Pending
    );
    assert_eq!(st.pending_len(), 0);
    assert_eq!(st.journal().len(), 8);
    st.deliver_inputs(vec![
        input(
            8,
            EventBody::ChildCompleted(ChildCompleted {
                op: 99,
                outcome: rr(),
            }),
        ),
        timer_fired(9, 1),
    ])
    .unwrap();
    assert_eq!(
        st.wait(&[1, 2], WaitMode::Any, h(50)).unwrap(),
        WaitOutcome::Resolved(vec![2, 1])
    );
    let p = st.take_pending();
    assert_eq!(p.len(), 1);
    assert_eq!(p[0].seq, 10);
    assert_eq!(
        p[0].body,
        EventBody::WaitResolved(WaitResolved {
            meta: meta(h(50)),
            completed: vec![2, 1],
        })
    );
    assert_eq!(st.completion_for(1).map(|e| e.seq), Some(9));
    assert_eq!(st.completion_for(2).map(|e| e.seq), Some(7));
    assert_eq!(st.completion_for(3), None);
    assert_eq!(
        st.op_kind(2).map(|e| e.body.kind()),
        Some(EventKind::EffectScheduled)
    );
}

#[test]
fn wait_all_resolves_when_all_complete() {
    let mut st = wait_fixture();
    st.deliver_inputs(vec![timer_fired(7, 1)]).unwrap();
    assert_eq!(
        st.wait(&[2, 1], WaitMode::All, h(50)).unwrap(),
        WaitOutcome::Pending
    );
    st.deliver_inputs(vec![input(
        8,
        EventBody::EffectCompleted(EffectCompleted {
            op: 2,
            outcome: rr(),
        }),
    )])
    .unwrap();
    assert_eq!(
        st.wait(&[2, 1], WaitMode::All, h(50)).unwrap(),
        WaitOutcome::Resolved(vec![1, 2])
    );
}

#[test]
fn wait_rejects_unknown_ops() {
    let mut st = wait_fixture();
    // op 3 is a set-status (not an op), op 77 does not exist.
    for op in [3, 77] {
        assert!(matches!(
            st.wait(&[op], WaitMode::Any, h(50)),
            Err(ReplayError::Protocol(_))
        ));
    }
}

#[test]
fn wait_replay() {
    let mut st = fresh();
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    cmd(&mut st, CommandKind::Http, h(2)).unwrap();
    st.take_pending();
    st.deliver_inputs(vec![input(
        3,
        EventBody::EffectCompleted(EffectCompleted {
            op: 2,
            outcome: rr(),
        }),
    )])
    .unwrap();
    assert_eq!(
        st.wait(&[1, 2], WaitMode::Any, h(50)).unwrap(),
        WaitOutcome::Resolved(vec![2])
    );
    st.take_pending();
    st.deliver_inputs(vec![timer_fired(5, 1)]).unwrap();
    let journal = st.journal().to_vec();

    let mut st = replay(journal.clone());
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    cmd(&mut st, CommandKind::Http, h(2)).unwrap();
    assert_eq!(
        st.wait(&[1, 2], WaitMode::Any, h(50)).unwrap(),
        WaitOutcome::Resolved(vec![2])
    );
    assert_eq!(st.pending_len(), 0);

    let mut st = replay(journal);
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    cmd(&mut st, CommandKind::Http, h(2)).unwrap();
    let e = nd(st.wait(&[1, 2], WaitMode::Any, h(51)));
    assert_eq!(e.seq, 4);
}

#[test]
fn wait_cancellation_op() {
    let mut st = fresh();
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    st.take_pending();
    assert_eq!(
        st.wait(&[CANCELLATION_OP, 1], WaitMode::Any, h(50))
            .unwrap(),
        WaitOutcome::Pending
    );
    st.deliver_inputs(vec![
        input(
            2,
            EventBody::CancelRequested(CancelRequested {
                reason: "user".into(),
            }),
        ),
        timer_fired(3, 1),
    ])
    .unwrap();
    assert_eq!(
        st.wait(&[1, CANCELLATION_OP], WaitMode::Any, h(50))
            .unwrap(),
        WaitOutcome::Resolved(vec![CANCELLATION_OP, 1])
    );
    assert_eq!(st.completion_for(CANCELLATION_OP).map(|e| e.seq), Some(2));
}

#[test]
fn deliver_inputs_rules() {
    let mut st = fresh();
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    assert!(matches!(
        st.deliver_inputs(vec![timer_fired(2, 1)]),
        Err(ReplayError::BadJournal(_))
    ));
    st.take_pending();
    assert!(matches!(
        st.deliver_inputs(vec![timer_fired(3, 1)]),
        Err(ReplayError::BadJournal(_))
    ));
    assert!(matches!(
        st.deliver_inputs(vec![Event {
            seq: 2,
            ts_nanos: 1,
            body: body_for(CommandKind::Http, h(2)),
        }]),
        Err(ReplayError::BadJournal(_))
    ));
    st.deliver_inputs(vec![timer_fired(2, 1), timer_fired(3, 1)])
        .unwrap();
    assert_eq!(st.journal().len(), 4);
    assert_eq!(cmd(&mut st, CommandKind::Http, h(2)).unwrap(), (4, false));
}

#[test]
fn patched_semantics() {
    // Live: true and a marker is appended.
    let mut st = fresh();
    assert!(st.patched(h(5), "v2").unwrap());
    let p = st.take_pending();
    assert_eq!(p.len(), 1);
    assert_eq!(
        p[0].body,
        EventBody::PatchMarker(PatchMarker {
            meta: meta(h(5)),
            change_id: "v2".into(),
        })
    );

    // Replay with the marker next: true, consumed.
    let journal = vec![
        run_started(),
        Event {
            seq: 1,
            ts_nanos: 1,
            body: p[0].body.clone(),
        },
        Event {
            seq: 2,
            ts_nanos: 2,
            body: body_for(CommandKind::Http, h(6)),
        },
    ];
    let mut st = replay(journal);
    assert!(st.patched(h(5), "v2").unwrap());
    assert_eq!(cmd(&mut st, CommandKind::Http, h(6)).unwrap(), (2, true));

    // Replay of a pre-patch journal: false, nothing consumed, no error.
    let journal = vec![
        run_started(),
        Event {
            seq: 1,
            ts_nanos: 1,
            body: body_for(CommandKind::Http, h(6)),
        },
    ];
    let mut st = replay(journal);
    assert!(!st.patched(h(5), "v2").unwrap());
    assert_eq!(cmd(&mut st, CommandKind::Http, h(6)).unwrap(), (1, true));
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn set_status_is_kind_only() {
    // The kind-only hash makes a different status payload replay cleanly; the recorded
    // event is returned.
    let journal = record(&[(CommandKind::SetStatus, h(13))], &[]);
    let mut st = replay(journal.clone());
    let actual = || "set-status".to_string();
    let r = st
        .command(CommandKind::SetStatus, h(13), &actual, |_| {
            EventBody::StatusPublished(StatusPublished {
                meta: meta(h(13)),
                json: "{\"other\":1}".into(),
            })
        })
        .unwrap();
    match r {
        CommandResult::Replayed(ev) => assert_eq!(ev, &journal[1]),
        CommandResult::Live { .. } => panic!("expected replay"),
    }
}

fn step_body(hash: CommandHash) -> EventBody {
    body_for(CommandKind::Step, hash)
}

#[test]
fn step_guard() {
    let mut st = fresh();
    let actual = || "step".to_string();
    let seq = match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Live { seq, .. } => seq,
        StepBegin::Replayed(_) => panic!("expected live"),
    };
    assert_eq!(seq, 1);
    assert_eq!(st.pending_len(), 0);
    assert!(is_open_step(cmd(&mut st, CommandKind::Http, h(2))));
    assert!(is_open_step(st.wait(
        &[CANCELLATION_OP],
        WaitMode::Any,
        h(3)
    )));
    assert!(is_open_step(st.patched(h(4), "p")));
    assert!(is_open_step(st.begin_step(h(5), &actual).map(|_| ())));
    assert!(matches!(
        st.complete_step(2, step_body(h(1))),
        Err(ReplayError::Protocol(_))
    ));
    st.complete_step(1, step_body(h(1))).unwrap();
    assert_eq!(st.pending_len(), 1);
    assert!(matches!(
        st.complete_step(1, step_body(h(1))),
        Err(ReplayError::Protocol(_))
    ));

    // Abandoned step releases its seq.
    let seq = match st.begin_step(h(6), &actual).unwrap() {
        StepBegin::Live { seq, .. } => seq,
        StepBegin::Replayed(_) => panic!("expected live"),
    };
    assert_eq!(seq, 2);
    st.abandon_step(seq);
    assert_eq!(cmd(&mut st, CommandKind::Http, h(2)).unwrap(), (2, false));
    let p = st.take_pending();
    assert_eq!(p.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(p[0].body.kind(), EventKind::StepCompleted);
    let journal = st.journal().to_vec();

    // Replay: begin_step returns the recorded completion and opens no guard.
    let mut st = replay(journal);
    match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Replayed(ev) => assert_eq!(ev.seq, 1),
        StepBegin::Live { .. } => panic!("expected replay"),
    }
    assert_eq!(cmd(&mut st, CommandKind::Http, h(2)).unwrap(), (2, true));
    // Step hash mismatch on replay is nondeterminism.
    let journal = st.journal().to_vec();
    let mut st = replay(journal);
    assert_eq!(nd(st.begin_step(h(9), &actual).map(|_| ())).seq, 1);
}

// ---------------------------------------------------------------------------
// Step retry attempt chains (WF-02, plan 04-04)
// ---------------------------------------------------------------------------

fn saf_body(hash: CommandHash, attempt: u32, retry_at_nanos: u64) -> EventBody {
    EventBody::StepAttemptFailed(StepAttemptFailed {
        meta: meta(hash),
        name: "s".into(),
        attempt,
        failure: Failure::Timeout,
        retry_at_nanos,
        backoff: AttemptBackoff::InActivation,
        fuel_used: 0,
    })
}

fn recorded(seq: u64, body: EventBody) -> Event {
    Event {
        seq,
        ts_nanos: ACCEPTED + seq * 10,
        body,
    }
}

#[test]
fn replay_consumes_attempt_chain_then_completion() {
    let journal = vec![
        run_started(),
        recorded(1, saf_body(h(1), 1, 111)),
        recorded(2, saf_body(h(1), 2, 222)),
        recorded(3, step_body(h(1))),
    ];
    let mut st = replay(journal);
    let actual = || "step".to_string();
    match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Replayed(ev) => {
            assert_eq!(ev.seq, 3);
            assert_eq!(ev.body.kind(), EventKind::StepCompleted);
        }
        StepBegin::Live { .. } => panic!("expected replay"),
    }
    assert!(!st.is_replaying());
    assert_eq!(st.logical_now_nanos(), ACCEPTED + 30);
    // No guard opened: the next command goes live at seq 4.
    assert_eq!(cmd(&mut st, CommandKind::Http, h(2)).unwrap(), (4, false));
}

#[test]
fn replay_chain_without_completion_goes_live_at_next_attempt() {
    let journal = vec![run_started(), recorded(1, saf_body(h(1), 1, 777))];
    let mut st = replay(journal);
    let actual = || "step".to_string();
    match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Live {
            seq,
            attempt,
            last_retry_at_nanos,
        } => {
            assert_eq!(seq, 2);
            assert_eq!(attempt, 2);
            assert_eq!(last_retry_at_nanos, Some(777));
        }
        StepBegin::Replayed(_) => panic!("expected live"),
    }
    // The step is open at seq 2.
    assert!(is_open_step(cmd(&mut st, CommandKind::Http, h(2))));
    st.complete_step(2, step_body(h(1))).unwrap();
    assert_eq!(st.take_pending().len(), 1);
}

#[test]
fn fresh_live_step_is_attempt_one() {
    let mut st = fresh();
    match st.begin_step(h(1), &|| "step".to_string()).unwrap() {
        StepBegin::Live {
            seq,
            attempt,
            last_retry_at_nanos,
        } => {
            assert_eq!((seq, attempt, last_retry_at_nanos), (1, 1, None));
        }
        StepBegin::Replayed(_) => panic!("expected live"),
    }
}

#[test]
fn attempt_chain_divergence() {
    let actual = || "step".to_string();
    // SAF recorded for a different step hash.
    let mut st = replay(vec![run_started(), recorded(1, saf_body(h(9), 1, 0))]);
    assert_eq!(nd(st.begin_step(h(1), &actual).map(|_| ())).seq, 1);
    // Chain followed by a different command.
    let mut st = replay(vec![
        run_started(),
        recorded(1, saf_body(h(1), 1, 0)),
        recorded(2, body_for(CommandKind::Http, h(2))),
    ]);
    assert_eq!(nd(st.begin_step(h(1), &actual).map(|_| ())).seq, 2);
    // A non-step command where the journal has a SAF.
    let mut st = replay(vec![run_started(), recorded(1, saf_body(h(1), 1, 0))]);
    assert_eq!(nd(cmd(&mut st, CommandKind::Http, h(1))).seq, 1);
    // Out-of-order attempt numbers are a bad journal.
    let mut st = replay(vec![run_started(), recorded(1, saf_body(h(1), 2, 0))]);
    assert!(matches!(
        st.begin_step(h(1), &actual),
        Err(ReplayError::BadJournal(_))
    ));
}

#[test]
fn record_attempt_failed_live() {
    let mut st = fresh();
    let actual = || "step".to_string();
    let seq = match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Live { seq, .. } => seq,
        StepBegin::Replayed(_) => panic!("expected live"),
    };
    let first = st.rng(RandomStream::CtxRandom).next_u64();
    let seed = st.run().rng_seed;
    let mut expect1 = RngStreams::new(&rng::step_seed(&seed, 1));
    assert_eq!(first, expect1.stream(RandomStream::CtxRandom).next_u64());

    // Guards: wrong seq, wrong body.
    assert!(matches!(
        st.record_attempt_failed(seq + 1, saf_body(h(1), 1, 5)),
        Err(ReplayError::Protocol(_))
    ));
    assert!(matches!(
        st.record_attempt_failed(seq, step_body(h(1))),
        Err(ReplayError::Protocol(_))
    ));

    let next = st.record_attempt_failed(seq, saf_body(h(1), 1, 5)).unwrap();
    assert_eq!(next, 2);
    // Step stays open at the new seq; the SAF is pending at the old one.
    assert!(is_open_step(cmd(&mut st, CommandKind::Http, h(2))));
    let pending = st.take_pending();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].seq, 1);
    assert_eq!(pending[0].body.kind(), EventKind::StepAttemptFailed);
    // Fresh step-local stream seeded from the NEW seq.
    let mut expect2 = RngStreams::new(&rng::step_seed(&seed, 2));
    assert_eq!(
        st.rng(RandomStream::CtxRandom).next_u64(),
        expect2.stream(RandomStream::CtxRandom).next_u64()
    );
    // complete_step at the old seq is rejected; at the new seq it closes the step.
    assert!(st.complete_step(1, step_body(h(1))).is_err());
    st.complete_step(2, step_body(h(1))).unwrap();
    // Run-level streams restored (untouched by either attempt).
    let mut run_level = RngStreams::new(&seed);
    assert_eq!(
        st.rng(RandomStream::CtxRandom).next_u64(),
        run_level.stream(RandomStream::CtxRandom).next_u64()
    );
    let journal = st.journal().to_vec();
    assert_eq!(command_seqs(&journal), [1, 2]);

    // Replay of the recorded journal returns the completion, consuming both.
    let mut st = replay(journal);
    match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Replayed(ev) => assert_eq!(ev.seq, 2),
        StepBegin::Live { .. } => panic!("expected replay"),
    }
    assert!(st.finish().is_ok());
}

#[test]
fn record_attempt_failed_requires_open_step() {
    let mut st = fresh();
    assert!(matches!(
        st.record_attempt_failed(1, saf_body(h(1), 1, 0)),
        Err(ReplayError::Protocol(_))
    ));
}

#[test]
fn continue_as_new_suggested_rule() {
    let st = fresh();
    assert!(st.continue_as_new_suggested(1));
    assert!(!st.continue_as_new_suggested(2));

    let cmds = sample();
    let inputs = [1, 0, 2, 0, 1];
    let threshold = 6;
    // Record, querying after each command and after its inputs.
    let mut st = fresh();
    let mut rec = vec![st.continue_as_new_suggested(threshold)];
    for (j, (kind, hash)) in cmds.iter().enumerate() {
        let (seq, _) = cmd(&mut st, *kind, *hash).unwrap();
        assert_eq!(
            st.continue_as_new_suggested(threshold),
            seq + 1 >= threshold
        );
        rec.push(st.continue_as_new_suggested(threshold));
        st.take_pending();
        let evs = (0..inputs[j])
            .map(|k| timer_fired(st.journal().len() as u64 + k as u64, seq))
            .collect();
        st.deliver_inputs(evs).unwrap();
    }
    let journal = st.journal().to_vec();
    let mut st = replay(journal);
    let mut rep = vec![st.continue_as_new_suggested(threshold)];
    for (kind, hash) in &cmds {
        cmd(&mut st, *kind, *hash).unwrap();
        rep.push(st.continue_as_new_suggested(threshold));
    }
    assert_eq!(rep, rec);
    assert!(rec.contains(&true) && rec.contains(&false));
}

#[test]
fn rng_streams_follow_run_seed() {
    let mut a = fresh();
    let mut b = replay(record(&sample(), &[]));
    let x = a.rng(RandomStream::CtxRandom).next_u64();
    let y = b.rng(RandomStream::CtxRandom).next_u64();
    assert_eq!(x, y);
    assert_ne!(a.rng(RandomStream::Uuid).next_u64(), x);
}

// ---------------------------------------------------------------------------
// Property: faithful replay appends nothing; single mutations are caught at the exact seq.
// ---------------------------------------------------------------------------

/// Command kinds of the mutation property. `Patch` is not among them: since D6
/// (05-03) a recorded `PatchMarker` the code does not query is skipped instead of
/// diverging, so markers have their own tests (`patched_*`) rather than the generic
/// "every mutation diverges at its seq" rule.
const KINDS: [CommandKind; 12] = [
    CommandKind::Step,
    CommandKind::Http,
    CommandKind::Sql,
    CommandKind::Object,
    CommandKind::Sleep,
    CommandKind::SleepUntil,
    CommandKind::WaitSignal,
    CommandKind::StartChild,
    CommandKind::CreateAwaitToken,
    CommandKind::CancelOp,
    CommandKind::Wait,
    CommandKind::SetStatus,
];

#[derive(Debug, Clone)]
enum Mutation {
    None,
    Swap(prop::sample::Index),
    Insert(prop::sample::Index, usize),
    Delete(prop::sample::Index),
    ChangeHash(prop::sample::Index),
}

#[derive(Debug, PartialEq)]
enum Expect {
    Clean,
    NonDeterminismAt(u64),
    /// Extra command after the end of the journal: goes live (a legitimate continuation).
    LiveExtra,
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        Just(Mutation::None),
        any::<prop::sample::Index>().prop_map(Mutation::Swap),
        (any::<prop::sample::Index>(), 0..KINDS.len()).prop_map(|(i, k)| Mutation::Insert(i, k)),
        any::<prop::sample::Index>().prop_map(Mutation::Delete),
        any::<prop::sample::Index>().prop_map(Mutation::ChangeHash),
    ]
}

/// Distinct hash per command index (`i < 40`), randomised in the remaining bytes.
fn hash_for(i: usize, salt: u8) -> CommandHash {
    let mut b = [salt; 16];
    b[0] = i as u8;
    CommandHash(b)
}

proptest! {
    #[test]
    fn replay_property(
        spec in prop::collection::vec((0..KINDS.len(), 0usize..3, any::<u8>()), 1..40),
        m in mutation(),
    ) {
        let cmds: Vec<(CommandKind, CommandHash)> = spec
            .iter()
            .enumerate()
            .map(|(i, (k, _, salt))| (KINDS[*k], hash_for(i, *salt)))
            .collect();
        let inputs: Vec<usize> = spec.iter().map(|(_, n, _)| *n).collect();
        let journal = record(&cmds, &inputs);
        let seqs = command_seqs(&journal);
        prop_assert_eq!(seqs.len(), cmds.len());
        let n = cmds.len();

        let mut mutated = cmds.clone();
        let expect = match m {
            Mutation::None => Expect::Clean,
            Mutation::Swap(_) if n < 2 => Expect::Clean,
            Mutation::Swap(i) => {
                let i = i.index(n - 1);
                mutated.swap(i, i + 1);
                Expect::NonDeterminismAt(seqs[i])
            }
            Mutation::Insert(i, k) => {
                let i = i.index(n + 1);
                mutated.insert(i, (KINDS[k], CommandHash([0xFF; 16])));
                if i < n { Expect::NonDeterminismAt(seqs[i]) } else { Expect::LiveExtra }
            }
            Mutation::Delete(i) => {
                // Deleting the last command is caught by finish(); any other by the
                // next command's mismatch. Both report the deleted command's seq.
                let i = i.index(n);
                mutated.remove(i);
                Expect::NonDeterminismAt(seqs[i])
            }
            Mutation::ChangeHash(i) => {
                let i = i.index(n);
                mutated[i].1 = CommandHash([0xEE; 16]);
                Expect::NonDeterminismAt(seqs[i])
            }
        };

        let mut st = replay(journal);
        let mut got = None;
        for (kind, hash) in &mutated {
            match cmd(&mut st, *kind, *hash) {
                Ok(_) => {}
                Err(ReplayError::NonDeterminism(nd)) => {
                    got = Some(nd.seq);
                    break;
                }
                Err(other) => prop_assert!(false, "unexpected error {other:?}"),
            }
        }
        if got.is_none() {
            match st.finish() {
                Ok(()) => {}
                Err(ReplayError::NonDeterminism(nd)) => got = Some(nd.seq),
                Err(other) => prop_assert!(false, "unexpected error {other:?}"),
            }
        }
        match expect {
            Expect::Clean => {
                prop_assert_eq!(got, None);
                prop_assert_eq!(st.pending_len(), 0);
                prop_assert!(!st.is_replaying());
            }
            Expect::NonDeterminismAt(seq) => prop_assert_eq!(got, Some(seq)),
            Expect::LiveExtra => {
                prop_assert_eq!(got, None);
                prop_assert_eq!(st.pending_len(), 1);
            }
        }
    }
}

/// Regression (03-11): randomness drawn inside a live step's closure (skipped on
/// replay) must not move the run-level streams. Before the fix, a value drawn outside
/// any step after a live step differed from the same draw after that step was
/// replayed, so hot-waited and evict-and-replay runs journaled different bytes, and a
/// command derived from such a value would diverge on replay.
#[test]
fn step_closure_randomness_does_not_leak() {
    let actual = || "s".to_owned();
    let seed = RngSeed([7; 32]);
    let run_level = |n: usize| {
        let mut s = ChaChaStream::new(&seed, RandomStream::CtxRandom);
        (0..n).map(|_| s.next_u64()).last().unwrap()
    };

    // Record: a live step draws ctx and uuid randomness; then a draw outside any step.
    let mut st = fresh();
    let seq = match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Live { seq, .. } => seq,
        StepBegin::Replayed(_) => panic!("live"),
    };
    let inside = st.rng(RandomStream::CtxRandom).next_u64();
    let _uuid = st.new_uuid_v7();
    st.complete_step(seq, body_for(CommandKind::Step, h(1)))
        .unwrap();
    let after_live = st.rng(RandomStream::CtxRandom).next_u64();
    st.take_pending();
    let journal = st.journal().to_vec();

    assert_eq!(
        after_live,
        run_level(1),
        "run-level stream untouched by the step"
    );
    let mut local = ChaChaStream::new(&rng::step_seed(&seed, seq), RandomStream::CtxRandom);
    assert_eq!(
        inside,
        local.next_u64(),
        "step-local stream = step_seed(seed, seq)"
    );
    assert_ne!(inside, after_live);

    // Replay: the closure is skipped; the same draw yields the same value.
    let mut st = replay(journal);
    assert!(matches!(
        st.begin_step(h(1), &actual).unwrap(),
        StepBegin::Replayed(_)
    ));
    assert_eq!(st.rng(RandomStream::CtxRandom).next_u64(), after_live);

    // An abandoned live step restores the run-level streams too.
    let mut st = fresh();
    let seq = match st.begin_step(h(1), &actual).unwrap() {
        StepBegin::Live { seq, .. } => seq,
        StepBegin::Replayed(_) => panic!("live"),
    };
    st.rng(RandomStream::CtxRandom).next_u64();
    st.abandon_step(seq);
    assert_eq!(st.rng(RandomStream::CtxRandom).next_u64(), run_level(1));
}

// ---------------------------------------------------------------------------
// Completion semantics (05-03): cancel-op, WaitTimedOut, D21 signals, index.
// ---------------------------------------------------------------------------

use ostro_types::journal::{
    OpId, OpOutput, RunTerminated, SignalMatched, SignalPayload, SignalReceived, TokenCompleted,
    WaitTimedOut,
};

use super::wait::completion_target;

fn sig_payload(data: &[u8]) -> Payload {
    Payload {
        content_type: "application/json".into(),
        data: data.to_vec(),
    }
}

/// Live `cancel-op(op)`; returns the seq of the `OpCancelled` (or the replayed one).
fn cancel(st: &mut ReplayState, op: OpId, hash: CommandHash) -> Result<u64, ReplayError> {
    let actual = move || format!("cancel-op {op}");
    match st.command(CommandKind::CancelOp, hash, &actual, |_| {
        EventBody::OpCancelled(OpCancelled {
            meta: meta(hash),
            op,
        })
    })? {
        CommandResult::Replayed(ev) => Ok(ev.seq),
        CommandResult::Live { seq, .. } => Ok(seq),
    }
}

fn outcome(st: &ReplayState, op: OpId) -> Result<OpOutput, Failure> {
    st.completion_outcome(op).expect("well-formed completion")
}

#[test]
fn cancelled_sleep_waited_all_is_err_cancelled() {
    let mut st = fresh();
    let (op, _) = cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    assert_eq!(cancel(&mut st, op, h(2)).unwrap(), 2);
    assert_eq!(
        st.wait(&[op], WaitMode::All, h(3)).unwrap(),
        WaitOutcome::Resolved(vec![op]),
        "a wait on a cancelled op no longer hangs"
    );
    assert_eq!(st.completion_seq(op), Some(2));
    assert_eq!(outcome(&st, op), Err(Failure::Cancelled));
    let journal = {
        st.take_pending();
        st.journal().to_vec()
    };

    // Replay: same result, nothing appended.
    let mut st = replay(journal);
    cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    cancel(&mut st, op, h(2)).unwrap();
    assert_eq!(
        st.wait(&[op], WaitMode::All, h(3)).unwrap(),
        WaitOutcome::Resolved(vec![op])
    );
    assert_eq!(outcome(&st, op), Err(Failure::Cancelled));
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn input_before_cancel_wins() {
    // TimerFired at seq 5, OpCancelled at seq 7: the op completes Ok(timer).
    let mut st = fresh();
    let (op, _) = cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    for n in 2..=4 {
        cmd(&mut st, CommandKind::SetStatus, h(n)).unwrap();
    }
    st.take_pending();
    st.deliver_inputs(vec![timer_fired(5, op)]).unwrap();
    cmd(&mut st, CommandKind::SetStatus, h(6)).unwrap();
    assert_eq!(cancel(&mut st, op, h(7)).unwrap(), 7);
    assert_eq!(st.completion_seq(op), Some(5));
    assert_eq!(
        st.wait(&[op], WaitMode::All, h(8)).unwrap(),
        WaitOutcome::Resolved(vec![op])
    );
    assert_eq!(outcome(&st, op), Ok(OpOutput::Timer));
}

#[test]
fn cancel_before_input_wins_and_later_input_is_ignored() {
    // OpCancelled at seq 5, TimerFired at seq 7: Err(cancelled).
    let mut st = fresh();
    let (op, _) = cmd(&mut st, CommandKind::Sleep, h(1)).unwrap();
    let (other, _) = cmd(&mut st, CommandKind::Sleep, h(2)).unwrap();
    for n in 3..=4 {
        cmd(&mut st, CommandKind::SetStatus, h(n)).unwrap();
    }
    assert_eq!(cancel(&mut st, op, h(5)).unwrap(), 5);
    st.take_pending();
    st.deliver_inputs(vec![timer_fired(6, other), timer_fired(7, op)])
        .unwrap();
    assert_eq!(st.completion_seq(op), Some(5), "the later input is ignored");
    // Order is completion seq: the cancel (5) before the other timer (6).
    assert_eq!(
        st.wait(&[other, op], WaitMode::All, h(8)).unwrap(),
        WaitOutcome::Resolved(vec![op, other])
    );
    assert_eq!(outcome(&st, op), Err(Failure::Cancelled));
    assert_eq!(outcome(&st, other), Ok(OpOutput::Timer));
}

#[test]
fn wait_timed_out_is_err_timeout_for_signal_and_token() {
    let mut st = fresh();
    let (sig, _) = cmd(&mut st, CommandKind::WaitSignal, h(1)).unwrap();
    let (tok, _) = cmd(&mut st, CommandKind::CreateAwaitToken, h(2)).unwrap();
    st.take_pending();
    st.deliver_inputs(vec![
        input(3, EventBody::WaitTimedOut(WaitTimedOut { op: tok })),
        input(4, EventBody::WaitTimedOut(WaitTimedOut { op: sig })),
    ])
    .unwrap();
    assert_eq!(
        st.wait(&[sig, tok], WaitMode::All, h(3)).unwrap(),
        WaitOutcome::Resolved(vec![tok, sig])
    );
    assert_eq!(outcome(&st, sig), Err(Failure::Timeout));
    assert_eq!(outcome(&st, tok), Err(Failure::Timeout));
}

#[test]
fn signal_matched_completes_with_referenced_payload() {
    let mut st = fresh();
    // A buffered signal: its payload is journaled long before the wait exists.
    st.deliver_inputs(vec![input(
        1,
        EventBody::SignalPayload(SignalPayload {
            name: "go".into(),
            payload: sig_payload(b"\"early\""),
        }),
    )])
    .unwrap();
    for n in 2..=20 {
        cmd(&mut st, CommandKind::SetStatus, h(n)).unwrap();
    }
    let (buffered, _) = cmd(&mut st, CommandKind::WaitSignal, h(21)).unwrap();
    let (direct, _) = cmd(&mut st, CommandKind::WaitSignal, h(22)).unwrap();
    st.take_pending();
    let base = st.journal().len() as u64;
    st.deliver_inputs(vec![
        input(
            base,
            EventBody::SignalMatched(SignalMatched {
                op: buffered,
                payload_seq: 1,
            }),
        ),
        input(
            base + 1,
            EventBody::SignalPayload(SignalPayload {
                name: "go".into(),
                payload: sig_payload(b"\"late\""),
            }),
        ),
        input(
            base + 2,
            EventBody::SignalMatched(SignalMatched {
                op: direct,
                payload_seq: base + 1,
            }),
        ),
    ])
    .unwrap();
    assert_eq!(
        st.wait(&[direct, buffered], WaitMode::All, h(23)).unwrap(),
        WaitOutcome::Resolved(vec![buffered, direct])
    );
    assert_eq!(
        outcome(&st, buffered),
        Ok(OpOutput::Signal(sig_payload(b"\"early\"")))
    );
    assert_eq!(
        outcome(&st, direct),
        Ok(OpOutput::Signal(sig_payload(b"\"late\"")))
    );
    // SignalPayload events never complete an op.
    assert_eq!(st.completion_seq(1), None);
    assert_eq!(st.completion_seq(base + 1), None);
}

#[test]
fn signal_matched_bad_payload_seq_is_bad_journal() {
    // Points at a non-SignalPayload event, past the journal, or at a later event.
    for payload_seq in [1, 3, 99] {
        let mut st = fresh();
        let (sig, _) = cmd(&mut st, CommandKind::WaitSignal, h(1)).unwrap();
        st.take_pending();
        st.deliver_inputs(vec![
            input(
                2,
                EventBody::SignalMatched(SignalMatched {
                    op: sig,
                    payload_seq,
                }),
            ),
            input(
                3,
                EventBody::SignalPayload(SignalPayload {
                    name: "go".into(),
                    payload: payload(),
                }),
            ),
        ])
        .unwrap();
        assert_eq!(
            st.wait(&[sig], WaitMode::Any, h(2)).unwrap(),
            WaitOutcome::Resolved(vec![sig])
        );
        let r = st.completion_outcome(sig);
        assert!(
            matches!(r, Err(ReplayError::BadJournal(_))),
            "payload_seq {payload_seq}: {r:?}"
        );
    }
}

/// W4: SignalPayload (96) and RunTerminated (71) are accepted as non-completing
/// inputs; SignalMatched (97) and WaitTimedOut (70) complete. Replay matches live.
#[test]
fn deliver_inputs_accepts_non_completing_kinds() {
    let mut st = fresh();
    let (sig, _) = cmd(&mut st, CommandKind::WaitSignal, h(1)).unwrap();
    st.take_pending();
    assert_eq!(
        st.wait(&[sig], WaitMode::Any, h(2)).unwrap(),
        WaitOutcome::Pending
    );
    st.deliver_inputs(vec![input(
        2,
        EventBody::SignalPayload(SignalPayload {
            name: "go".into(),
            payload: payload(),
        }),
    )])
    .unwrap();
    assert_eq!(
        st.wait(&[sig], WaitMode::Any, h(2)).unwrap(),
        WaitOutcome::Pending,
        "a signal payload alone completes nothing"
    );
    st.deliver_inputs(vec![input(
        3,
        EventBody::RunTerminated(RunTerminated {
            reason: "marker".into(),
        }),
    )])
    .unwrap();
    assert_eq!(
        st.wait(&[sig], WaitMode::Any, h(2)).unwrap(),
        WaitOutcome::Pending,
        "a run-terminated marker completes nothing"
    );
    st.deliver_inputs(vec![input(
        4,
        EventBody::SignalMatched(SignalMatched {
            op: sig,
            payload_seq: 2,
        }),
    )])
    .unwrap();
    assert_eq!(
        st.wait(&[sig], WaitMode::Any, h(2)).unwrap(),
        WaitOutcome::Resolved(vec![sig])
    );
    assert_eq!(st.completion_seq(sig), Some(4));
    let live = outcome(&st, sig);
    assert_eq!(live, Ok(OpOutput::Signal(payload())));
    st.take_pending();
    let journal = st.journal().to_vec();

    let mut st = replay(journal);
    cmd(&mut st, CommandKind::WaitSignal, h(1)).unwrap();
    assert_eq!(
        st.wait(&[sig], WaitMode::Any, h(2)).unwrap(),
        WaitOutcome::Resolved(vec![sig])
    );
    assert_eq!(outcome(&st, sig), live);
    assert_eq!(st.pending_len(), 0);

    // A guest command is still rejected as an input.
    let mut st = fresh();
    assert!(matches!(
        st.deliver_inputs(vec![input(
            1,
            EventBody::OpCancelled(OpCancelled {
                meta: meta(h(9)),
                op: 1,
            }),
        )]),
        Err(ReplayError::BadJournal(_))
    ));
}

#[test]
fn legacy_signal_received_and_token_completed_still_complete() {
    let mut st = fresh();
    let (sig, _) = cmd(&mut st, CommandKind::WaitSignal, h(1)).unwrap();
    let (tok, _) = cmd(&mut st, CommandKind::CreateAwaitToken, h(2)).unwrap();
    st.take_pending();
    st.deliver_inputs(vec![
        input(
            3,
            EventBody::SignalReceived(SignalReceived {
                op: sig,
                payload: sig_payload(b"1"),
            }),
        ),
        input(
            4,
            EventBody::TokenCompleted(TokenCompleted {
                op: tok,
                payload: sig_payload(b"2"),
            }),
        ),
    ])
    .unwrap();
    assert_eq!(
        st.wait(&[tok, sig], WaitMode::All, h(3)).unwrap(),
        WaitOutcome::Resolved(vec![sig, tok])
    );
    assert_eq!(outcome(&st, sig), Ok(OpOutput::Signal(sig_payload(b"1"))));
    assert_eq!(outcome(&st, tok), Ok(OpOutput::Token(sig_payload(b"2"))));
}

/// The completion index is filled on load, on delivery and on live appends, agrees
/// with a first-in-journal-order scan, and answers lookups without scanning: 100k
/// lookups over a 50k-event journal take milliseconds (a scan per lookup would be
/// billions of event visits).
#[test]
fn completion_index_is_a_lookup_not_a_scan() {
    const OPS: u64 = 25_000;
    let mut st = fresh();
    for i in 0..OPS {
        let hash = CommandHash((i as u128).to_le_bytes());
        cmd(&mut st, CommandKind::Sleep, hash).unwrap();
    }
    st.take_pending();
    // Fire in reverse order so most completions sit far from their op.
    let base = st.journal().len() as u64;
    let fires: Vec<Event> = (0..OPS).map(|k| timer_fired(base + k, OPS - k)).collect();
    st.deliver_inputs(fires).unwrap();
    assert_eq!(st.journal().len() as u64, 1 + 2 * OPS);
    let journal = st.journal().to_vec();

    let scan = |op: OpId| {
        journal
            .iter()
            .find(|e| completion_target(&e.body) == Some(op))
            .map(|e| e.seq)
    };
    for op in [1, 2, OPS / 2, OPS - 1, OPS, OPS + 1] {
        assert_eq!(st.completion_seq(op), scan(op), "op {op}");
    }

    // Loaded from scratch: same index.
    let loaded = replay(journal.clone());
    let start = std::time::Instant::now();
    let mut hits = 0u64;
    for k in 0..100_000u64 {
        let op = 1 + k % OPS;
        if loaded.completion_for(op).is_some() {
            hits += 1;
        }
        assert_eq!(loaded.completion_seq(op), st.completion_seq(op));
    }
    assert_eq!(hits, 100_000);
    assert!(
        start.elapsed() < std::time::Duration::from_secs(5),
        "100k lookups took {:?}",
        start.elapsed()
    );
}

// ---------------------------------------------------------------------------
// gather_race_prop (WF-03): random ops, input interleavings, All/Any/cancel.
// ---------------------------------------------------------------------------

/// Proptest case count: 256 by default, `OSTRO_PROP_CASES` overrides.
fn prop_cases() -> u32 {
    std::env::var("OSTRO_PROP_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256)
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum GOp {
    Sleep,
    Signal,
    Token,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Fate {
    /// Normal completion (timer fired, signal payload + match, token completed).
    Complete,
    /// Signal whose payload was buffered before the op existed; other kinds: Complete.
    Buffered,
    /// WaitTimedOut for signal/token; sleeps fire normally.
    TimedOut,
    /// No input ever arrives.
    Never,
}

#[derive(Debug, Clone)]
enum GStep {
    Wait(WaitMode, Vec<usize>),
    Cancel(usize),
}

/// One arrival group (apply journals a group in one batch).
#[derive(Debug, Clone)]
enum Arrival {
    Fired(usize),
    /// SignalPayload immediately followed by its SignalMatched.
    SignalNow(usize),
    /// SignalMatched referencing the buffered payload of op `.0`.
    SignalBuffered(usize),
    Token(usize),
    TimedOut(usize),
    /// A signal nobody waits for (never matched).
    Noise,
}

#[derive(Debug, Clone)]
struct GScenario {
    ops: Vec<(GOp, Fate)>,
    arrivals: Vec<Arrival>,
    steps: Vec<GStep>,
    chunks: Vec<usize>,
    evict: Vec<bool>,
}

#[derive(Debug, Clone, PartialEq)]
enum GTrace {
    Resolved(Vec<OpId>, Vec<Result<OpOutput, Failure>>),
    Cancelled(OpId),
    Stuck(usize),
}

fn g_scenario() -> impl Strategy<Value = GScenario> {
    let op = (0..3usize, 0..4usize).prop_map(|(k, f)| {
        (
            [GOp::Sleep, GOp::Signal, GOp::Token][k],
            [Fate::Complete, Fate::Buffered, Fate::TimedOut, Fate::Never][f],
        )
    });
    (prop::collection::vec(op, 1..=6), any::<u64>()).prop_flat_map(|(ops, _)| {
        let n = ops.len();
        let step = (0..4usize, any::<u8>()).prop_map(move |(t, mask)| {
            let mut idx: Vec<usize> = (0..n).filter(|i| mask & (1 << i) != 0).collect();
            if idx.is_empty() {
                idx.push(usize::from(mask) % n);
            }
            match t {
                0 => GStep::Cancel(usize::from(mask) % n),
                1 => GStep::Wait(WaitMode::All, idx),
                _ => GStep::Wait(WaitMode::Any, idx),
            }
        });
        (
            Just(ops),
            // Arrival order keys, stale extras (op index, key), noise keys.
            prop::collection::vec(any::<u32>(), n),
            prop::collection::vec((0..n, any::<u32>()), 0..3),
            prop::collection::vec(any::<u32>(), 0..2),
            prop::collection::vec(step, 1..10),
            prop::collection::vec(1..=3usize, 1..4),
            prop::collection::vec(any::<bool>(), 1..4),
        )
            .prop_map(|(ops, keys, extras, noise, steps, chunks, evict)| {
                let mut keyed: Vec<(u32, Arrival)> = Vec::new();
                for (i, (kind, fate)) in ops.iter().enumerate() {
                    let a = match (kind, fate) {
                        (_, Fate::Never) => continue,
                        (GOp::Sleep, _) => Arrival::Fired(i),
                        (GOp::Signal, Fate::Buffered) => Arrival::SignalBuffered(i),
                        (GOp::Signal | GOp::Token, Fate::TimedOut) => Arrival::TimedOut(i),
                        (GOp::Signal, _) => Arrival::SignalNow(i),
                        (GOp::Token, _) => Arrival::Token(i),
                    };
                    keyed.push((keys[i], a));
                }
                // Stale duplicates: a second completion for an op (ignored by
                // first-completion-wins, or the first one when the op had none).
                for (i, key) in extras {
                    let a = match ops[i].0 {
                        GOp::Sleep => Arrival::Fired(i),
                        GOp::Signal | GOp::Token => Arrival::TimedOut(i),
                    };
                    keyed.push((key, a));
                }
                for key in noise {
                    keyed.push((key, Arrival::Noise));
                }
                keyed.sort_by_key(|(k, _)| *k);
                GScenario {
                    ops,
                    arrivals: keyed.into_iter().map(|(_, a)| a).collect(),
                    steps,
                    chunks,
                    evict,
                }
            })
    })
}

fn g_kind(k: GOp) -> CommandKind {
    match k {
        GOp::Sleep => CommandKind::Sleep,
        GOp::Signal => CommandKind::WaitSignal,
        GOp::Token => CommandKind::CreateAwaitToken,
    }
}

/// Buffered signal payloads journaled right after `RunStarted` (seq 1..), one per
/// `Fate::Buffered` signal op, in op order.
fn g_buffered(sc: &GScenario) -> Vec<usize> {
    sc.ops
        .iter()
        .enumerate()
        .filter(|(_, (k, f))| *k == GOp::Signal && *f == Fate::Buffered)
        .map(|(i, _)| i)
        .collect()
}

/// Input events of `arrival` starting at `seq`.
fn g_events(sc: &GScenario, ids: &[OpId], arrival: &Arrival, seq: u64) -> Vec<Event> {
    let data = |i: usize| sig_payload(format!("{i}").as_bytes());
    match *arrival {
        Arrival::Fired(i) => vec![timer_fired(seq, ids[i])],
        Arrival::SignalNow(i) => vec![
            input(
                seq,
                EventBody::SignalPayload(SignalPayload {
                    name: "go".into(),
                    payload: data(i),
                }),
            ),
            input(
                seq + 1,
                EventBody::SignalMatched(SignalMatched {
                    op: ids[i],
                    payload_seq: seq,
                }),
            ),
        ],
        Arrival::SignalBuffered(i) => {
            let pos = g_buffered(sc)
                .iter()
                .position(|&b| b == i)
                .expect("buffered");
            vec![input(
                seq,
                EventBody::SignalMatched(SignalMatched {
                    op: ids[i],
                    payload_seq: 1 + pos as u64,
                }),
            )]
        }
        Arrival::Token(i) => vec![input(
            seq,
            EventBody::TokenCompleted(TokenCompleted {
                op: ids[i],
                payload: data(i),
            }),
        )],
        Arrival::TimedOut(i) => vec![input(
            seq,
            EventBody::WaitTimedOut(WaitTimedOut { op: ids[i] }),
        )],
        Arrival::Noise => vec![input(
            seq,
            EventBody::SignalPayload(SignalPayload {
                name: "nobody".into(),
                payload: payload(),
            }),
        )],
    }
}

/// Delivery state shared across live restarts.
struct GFeed {
    next: usize,
    chunk_i: usize,
    evict_i: usize,
}

/// Run the scenario's guest program on `st`. `feed = None` is a pure replay (an
/// unsatisfied wait is `Stuck`). Live: an unsatisfied wait delivers the next chunk of
/// arrivals and either retries in place (hot) or returns `Err(())` (evict: the caller
/// re-runs the program on a fresh state over the committed journal).
fn g_exec(
    sc: &GScenario,
    st: &mut ReplayState,
    mut feed: Option<&mut GFeed>,
) -> Result<Vec<GTrace>, ()> {
    let b = g_buffered(sc).len() as u64;
    let ids: Vec<OpId> = (0..sc.ops.len() as u64).map(|i| 1 + b + i).collect();
    for (i, (k, _)) in sc.ops.iter().enumerate() {
        let (seq, _) = cmd(st, g_kind(*k), hash_for(i, 0xA0)).unwrap();
        assert_eq!(seq, ids[i]);
        st.take_pending();
    }
    let mut trace = Vec::new();
    for (si, step) in sc.steps.iter().enumerate() {
        match step {
            GStep::Cancel(i) => {
                cancel(st, ids[*i], hash_for(si, 0xC0)).unwrap();
                trace.push(GTrace::Cancelled(ids[*i]));
            }
            GStep::Wait(mode, idx) => {
                let ops: Vec<OpId> = idx.iter().map(|&i| ids[i]).collect();
                loop {
                    match st.wait(&ops, *mode, hash_for(si, 0xB0)).unwrap() {
                        WaitOutcome::Resolved(list) => {
                            let outs = list
                                .iter()
                                .map(|&op| st.completion_outcome(op).unwrap())
                                .collect();
                            trace.push(GTrace::Resolved(list, outs));
                            break;
                        }
                        WaitOutcome::Pending => {
                            let Some(f) = feed.as_deref_mut() else {
                                trace.push(GTrace::Stuck(si));
                                return Ok(trace);
                            };
                            if f.next >= sc.arrivals.len() {
                                trace.push(GTrace::Stuck(si));
                                return Ok(trace);
                            }
                            st.take_pending();
                            let k = sc.chunks[f.chunk_i % sc.chunks.len()];
                            f.chunk_i += 1;
                            let mut evs = Vec::new();
                            for a in &sc.arrivals[f.next..(f.next + k).min(sc.arrivals.len())] {
                                let seq = st.journal().len() as u64 + evs.len() as u64;
                                evs.extend(g_events(sc, &ids, a, seq));
                            }
                            f.next = (f.next + k).min(sc.arrivals.len());
                            st.deliver_inputs(evs).unwrap();
                            let ev = sc.evict[f.evict_i % sc.evict.len()];
                            f.evict_i += 1;
                            if ev {
                                return Err(());
                            }
                        }
                    }
                }
            }
        }
    }
    st.finish().unwrap();
    Ok(trace)
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(prop_cases()))]

    /// WF-03: live (with random hot/evict re-activations) and a replay of the produced
    /// journal give identical `WaitResolved` lists and guest-visible results; every
    /// resolved list is in completion-seq order and every outcome comes from the first
    /// completion of its op in journal order.
    #[test]
    fn gather_race_prop(sc in g_scenario()) {
        // Buffered signal payloads arrive before any op exists.
        let mut st = fresh();
        let buffered: Vec<Event> = g_buffered(&sc)
            .iter()
            .enumerate()
            .map(|(k, &i)| input(
                1 + k as u64,
                EventBody::SignalPayload(SignalPayload {
                    name: "go".into(),
                    payload: sig_payload(format!("buffered-{i}").as_bytes()),
                }),
            ))
            .collect();
        st.deliver_inputs(buffered).unwrap();
        let mut journal = st.journal().to_vec();

        let mut feed = GFeed { next: 0, chunk_i: 0, evict_i: 0 };
        let mut restarts = 0;
        let (live_trace, live_journal) = loop {
            let mut st = replay(journal.clone());
            match g_exec(&sc, &mut st, Some(&mut feed)) {
                Ok(t) => {
                    st.take_pending();
                    break (t, st.journal().to_vec());
                }
                Err(()) => {
                    st.take_pending();
                    journal = st.journal().to_vec();
                    restarts += 1;
                    prop_assert!(restarts < 1_000);
                }
            }
        };

        // Replay from the produced journal: identical trace, nothing appended.
        let mut st = replay(live_journal.clone());
        let replay_trace = g_exec(&sc, &mut st, None).unwrap();
        prop_assert_eq!(&replay_trace, &live_trace);
        prop_assert_eq!(st.pending_len(), 0);

        // Ordering and first-completion-wins against a journal scan.
        let first = |op: OpId| live_journal
            .iter()
            .find(|e| completion_target(&e.body) == Some(op))
            .map(|e| e.seq);
        for t in &live_trace {
            if let GTrace::Resolved(list, outs) = t {
                let seqs: Vec<u64> = list.iter().map(|&op| first(op).unwrap()).collect();
                prop_assert!(seqs.windows(2).all(|w| w[0] < w[1]), "order {seqs:?}");
                for (&op, out) in list.iter().zip(outs) {
                    let ev = &live_journal[first(op).unwrap() as usize];
                    let expect_err = match &ev.body {
                        EventBody::OpCancelled(_) => Some(Failure::Cancelled),
                        EventBody::WaitTimedOut(_) => Some(Failure::Timeout),
                        _ => None,
                    };
                    match expect_err {
                        Some(f) => prop_assert_eq!(out, &Err(f)),
                        None => prop_assert!(out.is_ok(), "{out:?} from {:?}", ev.body),
                    }
                }
            }
        }
        for op in 1..live_journal.len() as u64 {
            prop_assert_eq!(st.completion_seq(op), first(op));
        }
    }
}

// ---------------------------------------------------------------------------
// patched memo + unqueried-marker skip (D6, 05-03).
// ---------------------------------------------------------------------------

fn marker(seq: u64, hash: CommandHash, id: &str) -> Event {
    recorded(
        seq,
        EventBody::PatchMarker(PatchMarker {
            meta: meta(hash),
            change_id: id.into(),
        }),
    )
}

#[test]
fn patched_is_memoised_per_change_id() {
    // Live: one marker per id, however often it is queried.
    let mut st = fresh();
    assert!(st.patched(h(5), "a").unwrap());
    assert!(st.patched(h(5), "a").unwrap());
    assert!(st.patched(h(6), "b").unwrap());
    assert!(st.patched(h(5), "a").unwrap());
    let p = st.take_pending();
    assert_eq!(p.len(), 2, "one marker per change-id: {p:?}");

    // Replay of a pre-patch journal: false, memoised, nothing consumed or appended.
    let journal = vec![
        run_started(),
        recorded(1, body_for(CommandKind::Http, h(7))),
    ];
    let mut st = replay(journal);
    assert!(!st.patched(h(5), "a").unwrap());
    assert!(!st.patched(h(5), "a").unwrap());
    assert_eq!(cmd(&mut st, CommandKind::Http, h(7)).unwrap(), (1, true));
    // Still the memo after the cursor moved on (no live marker appended).
    assert!(!st.patched(h(5), "a").unwrap());
    assert_eq!(st.pending_len(), 0);
    st.finish().unwrap();

    // Replay with the marker: true, then the memo (the marker is consumed once).
    let journal = vec![
        run_started(),
        marker(1, h(5), "a"),
        recorded(2, body_for(CommandKind::Http, h(7))),
    ];
    let mut st = replay(journal);
    assert!(st.patched(h(5), "a").unwrap());
    assert!(st.patched(h(5), "a").unwrap());
    assert_eq!(cmd(&mut st, CommandKind::Http, h(7)).unwrap(), (2, true));
    st.finish().unwrap();
    assert_eq!(st.pending_len(), 0);
}

#[test]
fn patched_inside_open_step_is_rejected_even_when_memoised() {
    let mut st = fresh();
    assert!(st.patched(h(5), "a").unwrap());
    let actual = || "s".to_owned();
    let StepBegin::Live { .. } = st.begin_step(h(1), &actual).unwrap() else {
        panic!("live step");
    };
    assert!(is_open_step(st.patched(h(5), "a")));
}

#[test]
fn unqueried_markers_are_skipped() {
    // The patched call was removed: the next command skips the marker.
    let journal = vec![
        run_started(),
        recorded(1, body_for(CommandKind::Http, h(7))),
        marker(2, h(5), "a"),
        recorded(3, body_for(CommandKind::Sleep, h(8))),
        marker(4, h(6), "b"),
        marker(5, h(5), "a"),
    ];
    let mut st = replay(journal.clone());
    assert_eq!(cmd(&mut st, CommandKind::Http, h(7)).unwrap(), (1, true));
    assert_eq!(cmd(&mut st, CommandKind::Sleep, h(8)).unwrap(), (3, true));
    // Trailing markers (incl. a duplicate from before the memo) are skipped by finish.
    st.finish().unwrap();
    assert_eq!(st.pending_len(), 0);

    // `patched(b)` with `a`'s marker first: `a` is no longer queried, so it is skipped.
    let journal = vec![
        run_started(),
        marker(1, h(5), "a"),
        marker(2, h(6), "b"),
        recorded(3, body_for(CommandKind::Http, h(7))),
    ];
    let mut st = replay(journal);
    assert!(st.patched(h(6), "b").unwrap());
    assert_eq!(cmd(&mut st, CommandKind::Http, h(7)).unwrap(), (3, true));
    st.finish().unwrap();

    // A later live command after skipped markers appends at the end of the journal.
    let journal = vec![run_started(), marker(1, h(5), "a")];
    let mut st = replay(journal);
    assert_eq!(cmd(&mut st, CommandKind::Http, h(7)).unwrap(), (2, false));

    // A real divergence behind a skipped marker is still caught at its seq.
    let journal = vec![
        run_started(),
        marker(1, h(5), "a"),
        recorded(2, body_for(CommandKind::Http, h(7))),
    ];
    let mut st = replay(journal);
    assert_eq!(nd(cmd(&mut st, CommandKind::Http, h(9))).seq, 2);
}

/// v1 (no patch) / v2 (patched + second query) / v3 (patch removed, new path always)
/// over the replay core, mirroring the patch-v* guests of `tests/patching.rs`.
#[test]
fn patched_v1_v2_v3_lifecycle() {
    let a = (CommandKind::Step, h(1));
    let c = (CommandKind::Step, h(3));
    let sleep = (CommandKind::Sleep, h(4));
    let b = (CommandKind::Step, h(2));
    let pid = h(9);
    let run = |st: &mut ReplayState, with_patch: bool, always_c: bool| {
        let mut steps = vec!['A'];
        cmd(st, a.0, a.1).unwrap();
        let take_c = if with_patch {
            let first = st.patched(pid, "add-c").unwrap();
            assert_eq!(st.patched(pid, "add-c").unwrap(), first, "memo");
            first
        } else {
            always_c
        };
        if take_c {
            cmd(st, c.0, c.1).unwrap();
            steps.push('C');
        }
        cmd(st, sleep.0, sleep.1).unwrap();
        cmd(st, b.0, b.1).unwrap();
        steps.push('B');
        st.finish().unwrap();
        steps
    };

    // v1 journal replayed by v2: old path, no marker, no divergence.
    let mut st = fresh();
    assert_eq!(run(&mut st, false, false), vec!['A', 'B']);
    st.take_pending();
    let v1 = st.journal().to_vec();
    let mut st = replay(v1.clone());
    assert_eq!(run(&mut st, true, false), vec!['A', 'B']);
    assert_eq!(st.pending_len(), 0);

    // Fresh v2: one marker, new path.
    let mut st = fresh();
    assert_eq!(run(&mut st, true, false), vec!['A', 'C', 'B']);
    let v2 = st.take_pending();
    let markers = v2
        .iter()
        .filter(|e| e.body.kind() == EventKind::PatchMarker)
        .count();
    assert_eq!(markers, 1);
    let v2 = st.journal().to_vec();

    // v2 by v2: nothing new. v2 by v3: the unqueried marker is skipped.
    let mut st = replay(v2.clone());
    assert_eq!(run(&mut st, true, false), vec!['A', 'C', 'B']);
    assert_eq!(st.pending_len(), 0);
    let mut st = replay(v2);
    assert_eq!(run(&mut st, false, true), vec!['A', 'C', 'B']);
    assert_eq!(st.pending_len(), 0);
}
