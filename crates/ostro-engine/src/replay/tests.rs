//! Replay core tests (unit + proptest).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ostro_types::journal::{
    AwaitTokenCreated, CANCELLATION_OP, CancelRequested, ChildCompleted, ChildOptions,
    ChildStarted, CommandHash, CommandKind, CommandMeta, EffectCompleted, EffectRequest,
    EffectScheduled, Event, EventBody, EventKind, HASH_VER, HttpRequest, ObjectRequest,
    OpCancelled, PatchMarker, Payload, RESULT_CODEC_VER, RecordedResult, RngSeed, RunCompleted,
    RunStarted, SignalWaitStarted, SqlRequest, StatusPublished, StepCompleted, TimerFired,
    TimerStarted, WaitMode, WaitResolved,
};
use ostro_types::journal::{AttemptBackoff, Failure, StepAttemptFailed};
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

const KINDS: [CommandKind; 13] = [
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
    CommandKind::Patch,
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
