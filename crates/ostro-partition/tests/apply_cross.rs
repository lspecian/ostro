//! Cross-partition and timer apply (plan 02-10; STOR-12, STOR-13): inbound
//! batches with `(src, seq)` dedupe, child start/completion, signals, cancel
//! cascades, cron starts, outbox truncation and idempotent `TimerFired`.

use bytes::Bytes;
use ostro_partition::apply::{
    Action, ApplyOutcome, Cursor, InvalidCode, RejectReason, START_DELAY_OP, apply_command,
};
use ostro_partition::store::keys::{
    children_key, dedupe_key, outbox_key, outbox_prefix, ready_key, run_key, timer_key,
};
use ostro_partition::store::values::{self, InboxMsg, RunRecord, RunStatus};
use ostro_partition::{PartitionStore, StoreConfig, Table};
use ostro_types::command::{
    AttemptGranted, AttemptTokenMsg, CancelRun, CompleteRun, ConflictPolicy, CronStart, Derived,
    EnqueueReady, EventsAppended, InboundBatch, InboundMsg, OutboxMsg, OutboxTruncated, ParentRef,
    SendMessage, Signal, StartRun, TerminalStatus, derived, encode_command, outbox_msg,
};
use ostro_types::command::{CancelTimer, ScheduleTimer, TimerFired};
use ostro_types::{Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use prost::Message;

// ---------------------------------------------------------------- helpers

type Rows = Vec<(Vec<u8>, Vec<u8>)>;

struct H {
    p: u16,
    dir: tempfile::TempDir,
    store: PartitionStore,
    cur: Cursor,
}

impl H {
    fn new(p: u16) -> Self {
        let dir = ostro_testkit::fs::tempdir();
        let (store, meta) = PartitionStore::open(
            dir.path(),
            PartitionId(p),
            PartitionCount::new(16).expect("non-zero"),
            &StoreConfig::default(),
        )
        .expect("open store");
        let cur = Cursor::from_meta(&meta);
        H { p, dir, store, cur }
    }

    /// Applies one command (epoch 0) in its own batch and commits. Returns
    /// the outcome, the actions and the keyspace reads.
    fn apply_full(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>, u64) {
        let bytes = encode_command(c);
        let mut b = self.store.batch();
        let mut actions = Vec::new();
        let off = self.cur.next;
        let before = b.reads();
        let out = apply_command(&mut b, &mut self.cur, off, Epoch(0), &bytes, &mut actions)
            .expect("apply");
        let reads = b.reads() - before;
        let mu = self.cur.meta_update();
        b.commit(&mu).expect("commit");
        (out, actions, reads)
    }

    fn apply(&mut self, c: &Command) -> ApplyOutcome {
        self.apply_full(c).0
    }

    fn apply_act(&mut self, c: &Command) -> (ApplyOutcome, Vec<Action>) {
        let (o, a, _) = self.apply_full(c);
        (o, a)
    }

    fn get(&self, t: Table, k: &[u8]) -> Option<Vec<u8>> {
        self.store.reader().get(t, k).expect("read")
    }

    fn rows(&self, t: Table, prefix: &[u8]) -> Rows {
        self.store
            .reader()
            .prefix(t, prefix, usize::MAX)
            .expect("prefix")
    }

    fn run(&self, id: &RunId) -> Option<RunRecord> {
        self.get(Table::Runs, &run_key(id))
            .map(|v| values::decode(&v).expect("decode run"))
    }

    fn inbox(&self, id: &RunId) -> Vec<InboxMsg> {
        self.rows(Table::Inbox, id.as_bytes())
            .into_iter()
            .map(|(_, v)| values::decode(&v).expect("decode inbox"))
            .collect()
    }

    /// Pending outbox rows for `target`, as inbound messages in seq order.
    fn outbox_msgs(&self, target: u16) -> Vec<InboundMsg> {
        self.rows(Table::Outbox, &outbox_prefix(target))
            .into_iter()
            .map(|(k, v)| InboundMsg {
                seq: u64::from_be_bytes(k[2..].try_into().expect("seq")),
                msg: Some(OutboxMsg::decode(&*v).expect("prost outbox")),
            })
            .collect()
    }

    /// Every table except `meta` and `inbound_seen` (which move with every
    /// command), as raw rows.
    fn data(&self) -> Vec<(Table, Rows)> {
        ostro_partition::TABLES
            .into_iter()
            .filter(|t| !matches!(t, Table::Meta | Table::InboundSeen))
            .map(|t| (t, self.rows(t, &[])))
            .collect()
    }
}

/// Shuffler stand-in: reads `src`'s outbox for `dst`, applies the inbound
/// batch on `dst`, then truncates `src`'s outbox. Returns the inbound
/// outcome.
fn deliver(src: &mut H, dst: &mut H) -> ApplyOutcome {
    let msgs = src.outbox_msgs(dst.p);
    let upto = msgs.last().map(|m| m.seq);
    let o = dst.apply(&inbound(src.p, msgs));
    if let Some(upto) = upto {
        assert_eq!(src.apply(&truncate(dst.p, upto)), ApplyOutcome::Accepted);
    }
    o
}

fn inbound(src: u16, msgs: Vec<InboundMsg>) -> Command {
    Command::inbound_batch(InboundBatch {
        src: u32::from(src),
        msgs,
    })
}

fn truncate(target: u16, upto: u64) -> Command {
    Command::outbox_truncated(OutboxTruncated {
        target: u32::from(target),
        upto_seq: upto,
    })
}

fn im(seq: u64, kind: outbox_msg::Kind) -> InboundMsg {
    InboundMsg {
        seq,
        msg: Some(OutboxMsg { kind: Some(kind) }),
    }
}

fn rid(p: u16, n: u8) -> RunId {
    let mut b = [0u8; 18];
    b[..2].copy_from_slice(&p.to_be_bytes());
    b[2] = n.wrapping_mul(7);
    b[17] = n;
    RunId::from_bytes(&b).expect("18 bytes")
}

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn start(id: &RunId) -> StartRun {
    StartRun {
        run_id: id.as_bytes().to_vec(),
        workflow: "wf".into(),
        workflow_version: 1,
        queue: "q".into(),
        input: inline(b"in"),
        idempotency_key: vec![],
        conflict_policy: ConflictPolicy::ReturnExisting as i32,
        concurrency_key: "ck".into(),
        priority: 5,
        ready_at_ms: 100,
        requested_at_ms: 100,
        parent: None,
        rng_seed: vec![7; 32],
    }
}

fn grant(id: &RunId, n: u32) -> Command {
    Command::attempt_granted(AttemptGranted {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 0, n }),
    })
}

fn events(id: &RunId, n: u32, derived: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: id.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: 0, n }),
        from_seq: 0,
        events: vec![],
        derived,
    })
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn send(kind: outbox_msg::Kind) -> Derived {
    intent(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg { kind: Some(kind) }),
    }))
}

fn signal_kind(target: &RunId, name: &str) -> outbox_msg::Kind {
    outbox_msg::Kind::Signal(Signal {
        target_run: target.as_bytes().to_vec(),
        name: name.into(),
        payload: inline(b"p"),
    })
}

fn cancel_kind(target: &RunId) -> outbox_msg::Kind {
    outbox_msg::Kind::CancelRun(CancelRun {
        target_run: target.as_bytes().to_vec(),
        reason: "stop".into(),
    })
}

fn child_start(parent: &RunId, child: &RunId, op: u64) -> Derived {
    let mut s = start(child);
    s.parent = Some(ParentRef {
        run_id: parent.as_bytes().to_vec(),
        op,
    });
    send(outbox_msg::Kind::StartChild(
        ostro_types::command::StartChild { start: Some(s) },
    ))
}

fn cron(schedule: &str, fire: u64, id: &RunId) -> outbox_msg::Kind {
    outbox_msg::Kind::CronStart(CronStart {
        schedule_id: schedule.into(),
        fire_time_ms: fire,
        start: Some(start(id)),
    })
}

fn rej(r: RejectReason) -> ApplyOutcome {
    ApplyOutcome::Rejected(r)
}

/// Run `n` started (ready at 100) and granted `{0,1}` on partition `p`.
fn running(p: u16, n: u8) -> (H, RunId) {
    let mut h = H::new(p);
    let r = rid(p, n);
    assert_eq!(
        h.apply(&Command::start_run(start(&r))),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.apply(&grant(&r, 1)), ApplyOutcome::Accepted);
    (h, r)
}

fn has_outbox_action(a: &[Action]) -> bool {
    a.iter().any(|x| matches!(x, Action::OutboxAppended { .. }))
}

// ---------------------------------------------------------------- inbound

#[test]
fn inbound_outbox_exactly_once() {
    let (mut src, r) = running(0, 1);
    let mut dst = H::new(5);
    let t = rid(5, 1);
    assert_eq!(
        dst.apply(&Command::start_run(start(&t))),
        ApplyOutcome::Accepted
    );
    for name in ["a", "b", "c"] {
        assert_eq!(
            src.apply(&events(&r, 1, vec![send(signal_kind(&t, name))])),
            ApplyOutcome::Accepted
        );
    }
    let batch = inbound(0, src.outbox_msgs(5));
    assert_eq!(dst.apply(&batch), ApplyOutcome::Accepted);
    assert_eq!(dst.inbox(&t).len(), 3);
    assert_eq!(dst.cur.inbound_seen.get(&0), Some(&3));
    let signal_names: Vec<_> = dst
        .inbox(&t)
        .into_iter()
        .map(|m| match m {
            InboxMsg::Signal { name, payload } => {
                assert_eq!(
                    PayloadRef::decode(&*payload).unwrap(),
                    inline(b"p").unwrap()
                );
                name
            }
            other => panic!("unexpected inbox entry {other:?}"),
        })
        .collect();
    assert_eq!(signal_names, vec!["a", "b", "c"]);
    assert_eq!(dst.run(&t).expect("run").inbox_next, 3);

    // Re-delivery of the same batch: duplicate, nothing written.
    let before = dst.data();
    assert_eq!(
        dst.apply(&batch),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert_eq!(dst.data(), before);

    // Overlapping re-send [2, 3, 4] after [1, 2, 3]: only 4 applies.
    assert_eq!(
        src.apply(&events(&r, 1, vec![send(signal_kind(&t, "d"))])),
        ApplyOutcome::Accepted
    );
    let msgs: Vec<_> = src.outbox_msgs(5).into_iter().skip(1).collect();
    assert_eq!(msgs.iter().map(|m| m.seq).collect::<Vec<_>>(), [2, 3, 4]);
    assert_eq!(dst.apply(&inbound(0, msgs)), ApplyOutcome::Accepted);
    assert_eq!(dst.inbox(&t).len(), 4);
    assert_eq!(dst.cur.inbound_seen.get(&0), Some(&4));
}

#[test]
fn inbound_gap() {
    let mut dst = H::new(5);
    let t = rid(5, 1);
    assert_eq!(
        dst.apply(&Command::start_run(start(&t))),
        ApplyOutcome::Accepted
    );
    assert_eq!(
        dst.apply(&inbound(0, vec![im(1, signal_kind(&t, "a"))])),
        ApplyOutcome::Accepted
    );
    let before = dst.data();
    assert_eq!(
        dst.apply(&inbound(0, vec![im(3, signal_kind(&t, "c"))])),
        rej(RejectReason::SeqGap {
            expected: 2,
            found: 3
        })
    );
    // A gap later in the batch rejects the whole batch (no partial writes).
    assert_eq!(
        dst.apply(&inbound(
            0,
            vec![im(2, signal_kind(&t, "b")), im(4, signal_kind(&t, "d"))]
        )),
        rej(RejectReason::SeqGap {
            expected: 3,
            found: 4
        })
    );
    assert_eq!(dst.data(), before);
    assert_eq!(dst.inbox(&t).len(), 1);
    assert_eq!(dst.cur.inbound_seen.get(&0), Some(&1));
    assert_eq!(
        dst.store.read_meta().unwrap().inbound_seen.get(&0),
        Some(&1)
    );
}

#[test]
fn inbound_validation() {
    let mut dst = H::new(5);
    let t = rid(5, 1);
    // Source outside the partition count.
    assert_eq!(
        dst.apply(&inbound(16, vec![im(1, signal_kind(&t, "a"))])),
        rej(RejectReason::Invalid(InvalidCode::BadSource))
    );
    // Message without a body.
    assert_eq!(
        dst.apply(&inbound(0, vec![InboundMsg { seq: 1, msg: None }])),
        rej(RejectReason::Invalid(InvalidCode::MissingField))
    );
    assert_eq!(
        dst.apply(&inbound(
            0,
            vec![InboundMsg {
                seq: 1,
                msg: Some(OutboxMsg { kind: None })
            }]
        )),
        rej(RejectReason::Invalid(InvalidCode::MissingField))
    );
    // Misrouted message (target lives in partition 9).
    assert_eq!(
        dst.apply(&inbound(0, vec![im(1, signal_kind(&rid(9, 1), "a"))])),
        rej(RejectReason::WrongPartition)
    );
    // Empty batch: nothing new.
    assert_eq!(
        dst.apply(&inbound(0, vec![])),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert!(dst.cur.inbound_seen.is_empty());
}

#[test]
fn inbound_signal_unknown_run() {
    let mut dst = H::new(5);
    let before = dst.data();
    assert_eq!(
        dst.apply(&inbound(0, vec![im(1, signal_kind(&rid(5, 9), "a"))])),
        ApplyOutcome::Accepted
    );
    assert_eq!(dst.cur.inbound_seen.get(&0), Some(&1));
    assert_eq!(dst.data(), before);
}

#[test]
fn inbound_signal_wakes_scheduled_run() {
    let mut dst = H::new(5);
    let t = rid(5, 1);
    let mut s = start(&t);
    s.ready_at_ms = 5000;
    assert_eq!(dst.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(dst.run(&t).unwrap().status, RunStatus::Scheduled);
    let (o, a) = dst.apply_act(&inbound(0, vec![im(1, signal_kind(&t, "a"))]));
    assert_eq!(o, ApplyOutcome::Accepted);
    let run = dst.run(&t).unwrap();
    assert_eq!(run.status, RunStatus::Ready);
    assert!(dst.get(Table::Ready, &ready_key("q", 5, 100, &t)).is_some());
    assert!(matches!(a.as_slice(), [Action::EnqueueReady { run, .. }] if *run == t));

    // Already Ready: a second signal adds an inbox entry but no ready row.
    let (o, a) = dst.apply_act(&inbound(0, vec![im(2, signal_kind(&t, "b"))]));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty());
    assert_eq!(dst.rows(Table::Ready, &[]).len(), 1);
    assert_eq!(dst.inbox(&t).len(), 2);
}

#[test]
fn inbound_child_roundtrip() {
    let (mut p0, parent) = running(0, 1);
    let mut p5 = H::new(5);
    let child = rid(5, 7);
    // The parent starts the child and yields (EnqueueReady).
    let yield_ready = intent(derived::Intent::EnqueueReady(EnqueueReady {
        ready_at_ms: 200,
    }));
    assert_eq!(
        p0.apply(&events(
            &parent,
            1,
            vec![child_start(&parent, &child, 3), yield_ready]
        )),
        ApplyOutcome::Accepted
    );
    assert_eq!(p0.run(&parent).unwrap().children, 1);

    let (o, a) = {
        let msgs = p0.outbox_msgs(5);
        p5.apply_act(&inbound(0, msgs))
    };
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(matches!(a.as_slice(), [Action::EnqueueReady { run, .. }] if *run == child));
    assert_eq!(p0.apply(&truncate(5, 1)), ApplyOutcome::Accepted);
    let c = p5.run(&child).expect("child started on p5");
    assert_eq!(c.parent, Some((*parent.as_bytes(), 3)));
    assert_eq!(c.status, RunStatus::Ready);

    // A re-sent StartChild with a fresh seq hits the runs row: consumed.
    assert_eq!(
        p5.apply(&inbound(
            0,
            vec![im(2, {
                let mut s = start(&child);
                s.parent = Some(ParentRef {
                    run_id: parent.as_bytes().to_vec(),
                    op: 3,
                });
                outbox_msg::Kind::StartChild(ostro_types::command::StartChild { start: Some(s) })
            })]
        )),
        ApplyOutcome::Accepted
    );
    assert_eq!(p5.cur.inbound_seen.get(&0), Some(&2));

    // The child completes and reports back.
    assert_eq!(p5.apply(&grant(&child, 1)), ApplyOutcome::Accepted);
    let done = intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: inline(b"out"),
        completed_at_ms: 9,
    }));
    assert_eq!(
        p5.apply(&events(&child, 1, vec![done])),
        ApplyOutcome::Accepted
    );
    assert_eq!(deliver(&mut p5, &mut p0), ApplyOutcome::Accepted);

    let pr = p0.run(&parent).unwrap();
    assert_eq!(pr.children, 0);
    assert!(
        p0.get(Table::Children, &children_key(&parent, &child))
            .is_none()
    );
    assert_eq!(pr.status, RunStatus::Ready);
    assert!(
        p0.get(Table::Ready, &ready_key("q", 5, 200, &parent))
            .is_some()
    );
    assert_eq!(
        p0.inbox(&parent),
        vec![InboxMsg::ChildCompleted {
            child: *child.as_bytes(),
            op: 3,
            status: RunStatus::Completed,
            result: inline(b"out").unwrap().encode_to_vec(),
        }]
    );
    assert!(p5.rows(Table::Outbox, &[]).is_empty(), "truncated");
}

#[test]
fn inbound_cancel_cascade() {
    let (mut p0, parent) = running(0, 1);
    let mut p5 = H::new(5);
    let mut p9 = H::new(9);
    let (c5, c9) = (rid(5, 1), rid(9, 1));
    assert_eq!(
        p0.apply(&events(
            &parent,
            1,
            vec![child_start(&parent, &c5, 1), child_start(&parent, &c9, 2)]
        )),
        ApplyOutcome::Accepted
    );
    assert_eq!(deliver(&mut p0, &mut p5), ApplyOutcome::Accepted);
    assert_eq!(deliver(&mut p0, &mut p9), ApplyOutcome::Accepted);
    assert!(p5.run(&c5).is_some() && p9.run(&c9).is_some());

    // Cancel request for the parent (from some partition 3).
    let (o, a) = p0.apply_act(&inbound(3, vec![im(1, cancel_kind(&parent))]));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.contains(&Action::CancelRequested { run: parent }));
    assert!(a.contains(&Action::OutboxAppended {
        target: PartitionId(5),
        seq: 2
    }));
    assert!(a.contains(&Action::OutboxAppended {
        target: PartitionId(9),
        seq: 2
    }));
    let pr = p0.run(&parent).unwrap();
    assert!(pr.cancel_requested);
    assert_eq!(
        p0.inbox(&parent),
        vec![InboxMsg::CancelRequested {
            reason: "stop".into()
        }]
    );
    let m5 = p0.outbox_msgs(5);
    assert_eq!(m5.len(), 1);
    assert_eq!(
        m5[0].msg.as_ref().unwrap().kind,
        Some(outbox_msg::Kind::CancelRun(CancelRun {
            target_run: c5.as_bytes().to_vec(),
            reason: "stop".into()
        }))
    );

    assert_eq!(deliver(&mut p0, &mut p5), ApplyOutcome::Accepted);
    assert_eq!(deliver(&mut p0, &mut p9), ApplyOutcome::Accepted);
    assert!(p5.run(&c5).unwrap().cancel_requested);
    assert!(p9.run(&c9).unwrap().cancel_requested);

    // A second cancel: consumed, no second cascade.
    let (o, a) = p0.apply_act(&inbound(3, vec![im(2, cancel_kind(&parent))]));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty(), "{a:?}");
    assert!(p0.rows(Table::Outbox, &[]).is_empty());
    assert_eq!(p0.inbox(&parent).len(), 1);
}

#[test]
fn inbound_cron_once() {
    let mut p5 = H::new(5);
    let (a, b, c) = (rid(5, 1), rid(5, 2), rid(5, 3));
    assert_eq!(
        p5.apply(&inbound(
            0,
            vec![
                im(1, cron("nightly", 1000, &a)),
                im(2, cron("nightly", 1000, &b))
            ]
        )),
        ApplyOutcome::Accepted
    );
    assert!(p5.run(&a).is_some());
    assert!(
        p5.run(&b).is_none(),
        "same (schedule, fire_time) starts once"
    );
    assert_eq!(
        p5.get(
            Table::Dedupe,
            &dedupe_key("__cron/nightly", &1000u64.to_be_bytes())
        ),
        Some(a.as_bytes().to_vec())
    );
    assert_eq!(
        p5.apply(&inbound(0, vec![im(3, cron("nightly", 2000, &c))])),
        ApplyOutcome::Accepted
    );
    assert!(p5.run(&c).is_some());
    assert_eq!(p5.rows(Table::Runs, &[]).len(), 2);
}

#[test]
fn inbound_seen_persists() {
    let mut dst = H::new(5);
    let t = rid(5, 1);
    assert_eq!(
        dst.apply(&inbound(
            2,
            vec![im(1, signal_kind(&t, "a")), im(2, signal_kind(&t, "b"))]
        )),
        ApplyOutcome::Accepted
    );
    let H {
        dir, store, cur, ..
    } = dst;
    drop(store);
    let (_store, meta) = PartitionStore::open(
        dir.path(),
        PartitionId(5),
        PartitionCount::new(16).unwrap(),
        &StoreConfig::default(),
    )
    .expect("reopen");
    let restored = Cursor::from_meta(&meta);
    assert_eq!(restored.inbound_seen.get(&2), Some(&2));
    assert_eq!(restored.inbound_seen, cur.inbound_seen);
}

// ---------------------------------------------------------------- outbox

#[test]
fn outbox_truncate() {
    let (mut h, r) = running(0, 1);
    let t = rid(5, 1);
    for name in ["a", "b", "c"] {
        assert_eq!(
            h.apply(&events(&r, 1, vec![send(signal_kind(&t, name))])),
            ApplyOutcome::Accepted
        );
    }
    let (o, a, reads) = h.apply_full(&truncate(5, 2));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty());
    assert_eq!(reads, 0, "truncation is blind");
    assert!(h.get(Table::Outbox, &outbox_key(5, 1)).is_none());
    assert!(h.get(Table::Outbox, &outbox_key(5, 2)).is_none());
    assert!(h.get(Table::Outbox, &outbox_key(5, 3)).is_some());
    assert_eq!(h.cur.meta.outbox_truncated.get(&5), Some(&2));

    let dup = ApplyOutcome::Duplicate { existing: None };
    assert_eq!(h.apply(&truncate(5, 2)), dup);
    assert_eq!(h.apply(&truncate(5, 1)), dup);
    let beyond = rej(RejectReason::Invalid(InvalidCode::TruncateBeyondEnd));
    assert_eq!(h.apply(&truncate(5, 4)), beyond);
    assert_eq!(h.apply(&truncate(7, 1)), beyond, "never emitted to 7");
    let wide = Command::outbox_truncated(OutboxTruncated {
        target: 70_000,
        upto_seq: 1,
    });
    assert_eq!(h.apply(&wide), beyond);
    let (o, _, reads) = h.apply_full(&truncate(5, 3));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(reads, 0);
    assert!(h.rows(Table::Outbox, &[]).is_empty());
    assert_eq!(
        h.store.read_meta().unwrap().cursor.outbox_truncated.get(&5),
        Some(&3)
    );
    // New emissions continue the seq after truncation.
    let (_, a) = h.apply_act(&events(&r, 1, vec![send(signal_kind(&t, "d"))]));
    assert!(has_outbox_action(&a));
    assert!(h.get(Table::Outbox, &outbox_key(5, 4)).is_some());
}

// ---------------------------------------------------------------- timers

fn fired(run: &RunId, op: u64, due: u64, at: u64) -> Command {
    Command::timer_fired(TimerFired {
        run_id: run.as_bytes().to_vec(),
        op,
        due_at_ms: due,
        fired_at_ms: at,
    })
}

fn schedule(op: u64, due: u64) -> Derived {
    intent(derived::Intent::ScheduleTimer(ScheduleTimer {
        op,
        due_at_ms: due,
    }))
}

#[test]
fn timer_fire_once() {
    let (mut h, r) = running(0, 1);
    assert_eq!(
        h.apply(&events(&r, 1, vec![schedule(7, 1000)])),
        ApplyOutcome::Accepted
    );
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_some());

    let (o, a, reads) = h.apply_full(&fired(&r, 7, 1000, 1003));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 2, "TimerFired reads: {reads}");
    assert!(a.is_empty(), "Running run: no new ready row ({a:?})");
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_none());
    assert_eq!(
        h.inbox(&r),
        vec![InboxMsg::TimerFired {
            op: 7,
            fired_at_ms: 1003
        }]
    );
    let run = h.run(&r).unwrap();
    assert_eq!(run.status, RunStatus::Running);
    assert_eq!(run.inbox_next, 1);

    // A new leader re-fires the same timer: Duplicate, state unchanged.
    let before = h.data();
    let (o, a, reads) = h.apply_full(&fired(&r, 7, 1000, 1500));
    assert_eq!(o, ApplyOutcome::Duplicate { existing: None });
    assert!(a.is_empty());
    assert_eq!(reads, 1);
    assert_eq!(h.data(), before);
}

#[test]
fn timer_cancelled() {
    let (mut h, r) = running(0, 1);
    assert_eq!(
        h.apply(&events(&r, 1, vec![schedule(7, 1000)])),
        ApplyOutcome::Accepted
    );
    let cancel = intent(derived::Intent::CancelTimer(CancelTimer {
        op: 7,
        due_at_ms: 1000,
    }));
    assert_eq!(
        h.apply(&events(&r, 1, vec![cancel])),
        ApplyOutcome::Accepted
    );
    let before = h.data();
    assert_eq!(
        h.apply(&fired(&r, 7, 1000, 1001)),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert_eq!(h.data(), before);
    assert!(h.inbox(&r).is_empty());
}

#[test]
fn timer_delayed_start() {
    let mut h = H::new(0);
    let r = rid(0, 1);
    let mut s = start(&r);
    s.ready_at_ms = 5000;
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(h.run(&r).unwrap().status, RunStatus::Scheduled);

    let (o, a, reads) = h.apply_full(&fired(&r, START_DELAY_OP, 5000, 5002));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(reads <= 2);
    assert_eq!(
        a,
        vec![Action::EnqueueReady {
            run: r,
            queue: "q".into(),
            priority: 5,
            ready_at_ms: 5000,
            concurrency_key: "ck".into()
        }]
    );
    let run = h.run(&r).unwrap();
    assert_eq!(run.status, RunStatus::Ready);
    assert!(h.get(Table::Ready, &ready_key("q", 5, 5000, &r)).is_some());
    assert!(
        h.get(Table::Timers, &timer_key(5000, &r, START_DELAY_OP))
            .is_none()
    );
    assert!(
        h.inbox(&r).is_empty(),
        "a delayed start is not an inbox event"
    );

    let before = h.data();
    assert_eq!(
        h.apply(&fired(&r, START_DELAY_OP, 5000, 5100)),
        ApplyOutcome::Duplicate { existing: None }
    );
    assert_eq!(h.data(), before);
}

#[test]
fn timer_delayed_start_after_early_wake() {
    // A signal woke the delayed run early; the start timer then only removes
    // its row (no second ready row).
    let mut h = H::new(5);
    let r = rid(5, 1);
    let mut s = start(&r);
    s.ready_at_ms = 5000;
    assert_eq!(h.apply(&Command::start_run(s)), ApplyOutcome::Accepted);
    assert_eq!(
        h.apply(&inbound(0, vec![im(1, signal_kind(&r, "a"))])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).unwrap().status, RunStatus::Ready);
    let (o, a) = h.apply_act(&fired(&r, START_DELAY_OP, 5000, 5000));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty());
    assert_eq!(h.rows(Table::Ready, &[]).len(), 1);
    assert!(h.rows(Table::Timers, &[]).is_empty());
}

#[test]
fn timer_terminal_run() {
    let (mut h, r) = running(0, 1);
    let done = intent(derived::Intent::CompleteRun(CompleteRun {
        status: TerminalStatus::Completed as i32,
        result: None,
        completed_at_ms: 9,
    }));
    assert_eq!(
        h.apply(&events(&r, 1, vec![schedule(7, 1000), done])),
        ApplyOutcome::Accepted
    );
    assert_eq!(h.run(&r).unwrap().status, RunStatus::Completed);
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_some());

    let (o, a) = h.apply_act(&fired(&r, 7, 1000, 1001));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert!(a.is_empty());
    assert!(h.get(Table::Timers, &timer_key(1000, &r, 7)).is_none());
    assert!(h.inbox(&r).is_empty());
    assert_eq!(h.run(&r).unwrap().status, RunStatus::Completed);
}

#[test]
fn timer_reads_and_validation() {
    let (mut h, r) = running(0, 1);
    // Unknown timer: exactly one point read.
    let (o, _, reads) = h.apply_full(&fired(&r, 3, 10, 11));
    assert_eq!(o, ApplyOutcome::Duplicate { existing: None });
    assert_eq!(reads, 1);
    assert_eq!(
        h.apply(&fired(&rid(4, 1), 3, 10, 11)),
        rej(RejectReason::WrongPartition)
    );
    let bad = Command::timer_fired(TimerFired {
        run_id: vec![1, 2, 3],
        op: 3,
        due_at_ms: 10,
        fired_at_ms: 11,
    });
    assert_eq!(
        h.apply(&bad),
        rej(RejectReason::Invalid(InvalidCode::BadRunId))
    );
}
