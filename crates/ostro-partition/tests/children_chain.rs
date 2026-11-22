//! Duplicate StartChild re-completion, the row guard and continue-as-new child chains (plan
//! 09-08; D-P9-24 B3, D-P9-27 W3).
//!
//! - A re-sent `StartChild` for a terminal child whose parent matches is answered with an
//!   event-less `ChildCompleted`; a live child or another parent drops it; a continue-as-new
//!   child forwards it to its successor.
//! - The parent accepts a `ChildCompleted` only with a matching `children` row (exact, or the
//!   original child of a chain via `child_run_uuid(parent, op)`), so nothing completes twice;
//!   a chain match sends `CancelRun{reason: "chain_done"}` to the original child.
//! - `chain_done` is forwarded hop by hop and clears `continued_to`; the expiry of a chain
//!   link with a parent is deferred (record kept, journal deleted) until `chain_done` or the
//!   `completed_at_ms + 2 × retention_ms` bound.

mod children_common;

use children_common::*;
use ostro_partition::Table;
use ostro_partition::apply::ApplyOutcome;
use ostro_partition::store::keys::children_key;
use ostro_partition::store::values::RunStatus;
use ostro_types::RunId;
use ostro_types::command::{
    ChildCompleted, EXPIRE_OP, RUN_TIMEOUT_OP, StartChild, StartRun, TerminalStatus, outbox_msg,
};

const ALL_TERMINAL: [TerminalStatus; 5] = [
    TerminalStatus::Completed,
    TerminalStatus::Failed,
    TerminalStatus::Cancelled,
    TerminalStatus::Terminated,
    TerminalStatus::TimedOut,
];

/// Delivers the child start of `(p, 1)` (child = [`child_id`]), grants it and ends it with
/// `status`. Returns the child id and its start.
fn ended_child(h: &mut H, p: &RunId, status: TerminalStatus) -> (RunId, StartRun) {
    let c = child_id(p, 1);
    let mut s = child_start(p, 1, &c, false);
    s.run_timeout_ms = 5_000;
    h.deliver_out(start_child_kind(s.clone()));
    h.ok(&grant(&c, 1));
    match status {
        TerminalStatus::Terminated => {
            h.ok(&terminate(&c, "ops", 500));
        }
        TerminalStatus::TimedOut => {
            h.ok(&fire(&c, RUN_TIMEOUT_OP, 100 + 5_000));
        }
        st => {
            h.ok(&append(&c, 0, 1, vec![complete(st)]));
        }
    }
    (c, s)
}

/// A child of `(p, 1)` that continued as new into `succ` (successor delivered, still live).
fn chain_link(h: &mut H, p: Option<&RunId>, succ: &RunId) -> RunId {
    let c = match p {
        Some(p) => child_id(p, 1),
        None => rid(40),
    };
    let s = match p {
        Some(p) => child_start(p, 1, &c, false),
        None => start(&c),
    };
    running_with(h, s);
    let before = h.outbox();
    h.ok(&append(&c, 0, 2, can_tail(succ)));
    for m in h.outbox_since(&before) {
        h.deliver_out(m);
    }
    let rec = h.run(&c);
    assert_eq!(rec.status, RunStatus::ContinuedAsNew);
    assert_eq!(rec.continued_to, Some(*succ));
    assert!(h.try_run(succ).is_some());
    c
}

fn child_completed(m: &outbox_msg::Kind) -> &ChildCompleted {
    match m {
        outbox_msg::Kind::ChildCompleted(c) => c,
        other => panic!("expected ChildCompleted, got {other:?}"),
    }
}

// ------------------------------------------------------------ duplicate StartChild (B3)

#[test]
fn duplicate_start_for_a_terminal_child_re_completes_event_less() {
    for st in ALL_TERMINAL {
        let mut h = H::new();
        let p = rid(1);
        let (c, s) = ended_child(&mut h, &p, st);
        let out = h.deliver_out(start_child_kind(s));
        assert_eq!(out.len(), 1, "{st:?}: {out:?}");
        let cc = child_completed(&out[0]);
        assert_eq!(cc.parent_run, p.as_bytes().to_vec());
        assert_eq!(cc.child_run, c.as_bytes().to_vec());
        assert_eq!(cc.op, 1);
        assert_eq!(cc.status, st as i32, "{st:?}");
        assert!(cc.result.is_none() && cc.event.is_none());
    }
}

#[test]
fn duplicate_start_for_a_live_child_is_a_drop() {
    let mut h = H::new();
    let p = rid(1);
    let c = child_id(&p, 1);
    let s = child_start(&p, 1, &c, false);
    h.deliver_out(start_child_kind(s.clone()));
    let rec = h.run(&c);
    assert!(h.deliver_out(start_child_kind(s)).is_empty());
    assert_eq!(h.run(&c), rec);
}

#[test]
fn duplicate_start_with_another_parent_or_detached_is_a_drop() {
    let mut h = H::new();
    let p = rid(1);
    let (c, s) = ended_child(&mut h, &p, TerminalStatus::Completed);
    let mut other_op = s.clone();
    other_op.parent.as_mut().expect("parent").op = 2;
    assert!(h.deliver_out(start_child_kind(other_op)).is_empty());
    let mut other_parent = s.clone();
    other_parent.parent.as_mut().expect("parent").run_id = rid(2).as_bytes().to_vec();
    assert!(h.deliver_out(start_child_kind(other_parent)).is_empty());
    assert!(
        h.deliver_out(start_child_kind(child_start(&p, 1, &c, true)))
            .is_empty()
    );
}

#[test]
fn chain_duplicate_start_is_forwarded_to_the_successor() {
    let mut h = H::new();
    let p = rid(1);
    let succ = rid(50);
    let c = chain_link(&mut h, Some(&p), &succ);
    let s = child_start(&p, 1, &c, false);
    let out = h.deliver_out(start_child_kind(s.clone()));
    assert_eq!(out.len(), 1, "{out:?}");
    let outbox_msg::Kind::StartChild(StartChild { start: Some(fwd) }) = &out[0] else {
        panic!("expected StartChild, got {out:?}");
    };
    assert_eq!(fwd.run_id, succ.as_bytes().to_vec());
    assert_eq!(fwd.parent, s.parent);
    // The successor is live: the forwarded duplicate is a drop.
    assert!(h.deliver_out(out[0].clone()).is_empty());
    // Once the successor ended, the forwarded duplicate re-completes from it.
    h.ok(&grant(&succ, 1));
    h.ok(&append(
        &succ,
        0,
        1,
        vec![complete(TerminalStatus::Completed)],
    ));
    let again = h.deliver_out(out[0].clone());
    assert_eq!(again.len(), 1, "{again:?}");
    let cc = child_completed(&again[0]);
    assert_eq!(cc.child_run, succ.as_bytes().to_vec());
    assert_eq!(cc.parent_run, p.as_bytes().to_vec());
    assert_eq!(cc.op, 1);
    assert_eq!(cc.status, TerminalStatus::Completed as i32);
}

// ------------------------------------------------------------------- row guard

#[test]
fn exact_row_completion_is_journaled_once() {
    let mut h = H::new();
    let kids = parent_with_children(&mut h, start(&rid(1)), &[1]);
    let p = rid(1);
    let c = kids[0];
    let out = h.deliver_out(completed_kind(&p, &c, 1, TerminalStatus::Completed, None));
    assert!(out.is_empty());
    assert_eq!(h.journal(&p).len(), 4);
    let rec = h.run(&p);
    assert_eq!(rec.children, 0);
    assert_eq!(rec.next_seq, 4);
    assert!(h.get(Table::Children, &children_key(&p, &c)).is_none());
    // A second completion of the same op (re-completion) finds no row: drop.
    let (o, _, _) = h.deliver(completed_kind(&p, &c, 1, TerminalStatus::Cancelled, None));
    assert_eq!(o, ApplyOutcome::Accepted);
    assert_eq!(h.journal(&p).len(), 4);
    assert_eq!(h.run(&p), rec);
}

#[test]
fn completion_without_any_matching_row_is_a_drop() {
    let mut h = H::new();
    let kids = parent_with_children(&mut h, start(&rid(1)), &[1]);
    let p = rid(1);
    let rec = h.run(&p);
    // Unknown child, op that is not the chain's.
    let out = h.deliver_out(completed_kind(
        &p,
        &rid(70),
        2,
        TerminalStatus::Completed,
        None,
    ));
    assert!(out.is_empty());
    assert_eq!(h.run(&p), rec);
    assert_eq!(h.journal(&p).len(), 3);
    assert!(
        h.get(Table::Children, &children_key(&p, &kids[0]))
            .is_some()
    );
}

#[test]
fn chain_successor_completion_matches_the_original_row_and_sends_chain_done() {
    let mut h = H::new();
    let kids = parent_with_children(&mut h, start(&rid(1)), &[1, 2]);
    let p = rid(1);
    let original = kids[0];
    let succ = rid(60);
    let out = h.deliver_out(completed_kind(
        &p,
        &succ,
        1,
        TerminalStatus::Completed,
        None,
    ));
    assert_eq!(cancels(&out), vec![(original, "chain_done".to_string())]);
    let rec = h.run(&p);
    assert_eq!(h.journal(&p).len(), 4, "journaled");
    assert_eq!(rec.children, 1, "no leaked row");
    assert!(
        h.get(Table::Children, &children_key(&p, &original))
            .is_none()
    );
    assert!(
        h.get(Table::Children, &children_key(&p, &kids[1]))
            .is_some()
    );
    // Re-completion from the successor: no row any more → drop, no second chain_done.
    let again = h.deliver_out(completed_kind(
        &p,
        &succ,
        1,
        TerminalStatus::Completed,
        None,
    ));
    assert!(again.is_empty());
    assert_eq!(h.run(&p), rec);
}

// ------------------------------------------------------------- chain_done / expiry

#[test]
fn chain_done_forwards_and_clears_continued_to() {
    let mut h = H::new();
    let p = rid(1);
    let succ = rid(50);
    let c = chain_link(&mut h, Some(&p), &succ);
    let out = h.deliver_out(cancel_kind(&c, "chain_done"));
    assert_eq!(cancels(&out), vec![(succ, "chain_done".to_string())]);
    let rec = h.run(&c);
    assert_eq!(rec.continued_to, None);
    assert!(!rec.cancel_requested);
    assert_eq!(h.journal(&c).len(), 2, "not deferred: journal kept");
    // Its normal expiry now deletes it fully.
    h.ok(&expire(&c, DONE_AT + RETENTION));
    assert!(h.try_run(&c).is_none());
    assert!(h.journal(&c).is_empty());
}

#[test]
fn chain_done_anywhere_else_is_a_drop() {
    let mut h = H::new();
    let p = rid(1);
    // A non-CAN terminal run.
    let (c, _) = ended_child(&mut h, &p, TerminalStatus::Completed);
    let rec = h.run(&c);
    assert!(h.deliver_out(cancel_kind(&c, "chain_done")).is_empty());
    assert_eq!(h.run(&c), rec);
    // A live run: never a cancel request.
    let live = running_with(&mut h, start(&rid(30)));
    let rec = h.run(&live);
    assert!(h.deliver_out(cancel_kind(&live, "chain_done")).is_empty());
    assert_eq!(h.run(&live), rec);
    // A link whose chain_done already passed.
    let succ = rid(51);
    let link = chain_link(&mut h, Some(&rid(2)), &succ);
    h.deliver_out(cancel_kind(&link, "chain_done"));
    assert!(h.deliver_out(cancel_kind(&link, "chain_done")).is_empty());
    // Unknown run.
    assert!(
        h.deliver_out(cancel_kind(&rid(99), "chain_done"))
            .is_empty()
    );
}

#[test]
fn chain_link_expiry_is_deferred_until_chain_done() {
    let mut h = H::new();
    let p = rid(1);
    let succ = rid(50);
    let c = chain_link(&mut h, Some(&p), &succ);
    let first = DONE_AT + RETENTION;
    let bound = DONE_AT + 2 * RETENTION;
    h.ok(&expire(&c, first));
    let rec = h.run(&c);
    assert_eq!(rec.status, RunStatus::ContinuedAsNew);
    assert_eq!(rec.next_seq, 0, "deferred mark");
    assert_eq!(rec.continued_to, Some(succ));
    assert!(h.journal(&c).is_empty(), "journal deleted");
    assert_eq!(h.timers(&c), vec![(bound, EXPIRE_OP)]);
    // Cancel propagation keeps working while kept.
    let out = h.deliver_out(cancel_kind(&c, "user"));
    assert_eq!(cancels(&out), vec![(succ, "user".to_string())]);
    // chain_done: forwarded, then the kept record deletes itself (bound timer cancelled).
    let out = h.deliver_out(cancel_kind(&c, "chain_done"));
    assert_eq!(cancels(&out), vec![(succ, "chain_done".to_string())]);
    assert!(h.try_run(&c).is_none());
    assert!(h.timers(&c).is_empty());
}

#[test]
fn chain_link_deferral_is_bounded() {
    let mut h = H::new();
    let p = rid(1);
    let succ = rid(50);
    let c = chain_link(&mut h, Some(&p), &succ);
    h.ok(&expire(&c, DONE_AT + RETENTION));
    assert!(h.try_run(&c).is_some());
    // A redelivered first fire (its timer row is gone) changes nothing.
    let rec = h.run(&c);
    h.apply(&expire(&c, DONE_AT + RETENTION));
    assert_eq!(h.run(&c), rec);
    h.ok(&expire(&c, DONE_AT + 2 * RETENTION));
    assert!(
        h.try_run(&c).is_none(),
        "deleted at the bound without chain_done"
    );
}

#[test]
fn chain_top_level_link_expires_as_before() {
    let mut h = H::new();
    let succ = rid(50);
    let c = chain_link(&mut h, None, &succ);
    h.ok(&expire(&c, DONE_AT + RETENTION));
    assert!(h.try_run(&c).is_none());
}

#[test]
fn chain_parent_close_mid_chain_reaches_the_live_successor() {
    let mut h = H::new();
    let kids = parent_with_children(&mut h, start(&rid(1)), &[1]);
    let p = rid(1);
    let succ = rid(50);
    let c = chain_link(&mut h, Some(&p), &succ);
    assert_eq!(c, kids[0]);
    // The parent completes: cascade to the original child, forwarded to the successor.
    let before = h.outbox();
    h.ok(&append(&p, 3, 1, vec![complete(TerminalStatus::Completed)]));
    let out = h.outbox_since(&before);
    assert_eq!(cancels(&out), vec![(c, "parent_closed".to_string())]);
    let fwd = h.deliver_out(out[0].clone());
    assert_eq!(cancels(&fwd), vec![(succ, "parent_closed".to_string())]);
    assert!(h.deliver_out(fwd[0].clone()).is_empty());
    assert!(h.run(&succ).cancel_requested);
    // The successor ends Cancelled; its completion reaches a terminal parent: drop.
    h.ok(&grant(&succ, 1));
    let before = h.outbox();
    h.ok(&append(
        &succ,
        0,
        1,
        vec![complete(TerminalStatus::Cancelled)],
    ));
    let done = h.outbox_since(&before);
    assert_eq!(done.len(), 1);
    let journal = h.journal(&p).len();
    assert!(h.deliver_out(done[0].clone()).is_empty());
    assert_eq!(h.journal(&p).len(), journal);
    // Without chain_done the kept predecessor goes at the bound.
    h.ok(&expire(&c, DONE_AT + RETENTION));
    assert!(h.try_run(&c).is_some());
    h.ok(&expire(&c, DONE_AT + 2 * RETENTION));
    assert!(h.try_run(&c).is_none());
}

#[test]
fn chain_end_to_end_journals_once_and_releases_the_predecessor() {
    let mut h = H::new();
    let kids = parent_with_children(&mut h, start(&rid(1)), &[1]);
    let p = rid(1);
    let succ = rid(50);
    let c = chain_link(&mut h, Some(&p), &succ);
    assert_eq!(c, kids[0]);
    h.ok(&grant(&succ, 1));
    let before = h.outbox();
    h.ok(&append(
        &succ,
        0,
        1,
        vec![complete(TerminalStatus::Completed)],
    ));
    let done = h.outbox_since(&before);
    assert_eq!(done.len(), 1);
    let chain_done = h.deliver_out(done[0].clone());
    assert_eq!(cancels(&chain_done), vec![(c, "chain_done".to_string())]);
    assert_eq!(h.journal(&p).len(), 4);
    assert_eq!(h.run(&p).children, 0);
    let fwd = h.deliver_out(chain_done[0].clone());
    assert_eq!(cancels(&fwd), vec![(succ, "chain_done".to_string())]);
    assert_eq!(h.run(&c).continued_to, None);
    // The final run (Completed) drops it.
    assert!(h.deliver_out(fwd[0].clone()).is_empty());
    assert!(!h.run(&succ).cancel_requested);
    h.ok(&expire(&c, DONE_AT + RETENTION));
    assert!(h.try_run(&c).is_none());
}

// ------------------------------------------------------------------ rebuild (STOR-06)

/// A children workload on a real two-partition node (the shuffler delivers every outbox
/// message, the timer service fires 1 ms expiries): attached + detached starts, a completion,
/// a continue-as-new chain (uuid match, chain_done forward, deferred expiry), a re-sent start
/// (re-completion), a terminated parent (cascade) resumed by RetryRun, and parent closes.
/// The live state equals the state rebuilt from the log (rebuild_digest property).
mod rebuild {
    use std::path::Path;
    use std::time::Duration;

    use bytes::Bytes;
    use ostro_log::{ReplicatedLog, Rf1Log};
    use ostro_partition::{Node, NodeConfig, PartitionHandle, PartitionSlot, StartMode};
    use ostro_types::command::{
        AttemptTokenMsg, CompleteRun, ConflictPolicy, Derived, EventsAppended, JournalEvent,
        OutboxMsg, ParentRef, RetryRun, SendMessage, StartChild, StartRun, TerminalStatus,
        TerminateRun, derived, outbox_msg,
    };
    use ostro_types::ids::child_run_uuid;
    use ostro_types::{AttemptToken, Command, PartitionCount, PartitionId, PayloadRef, RunId};
    use tokio::time::timeout;

    const PC: u16 = 2;
    const T: Duration = Duration::from_secs(20);

    async fn open(dir: &Path, mode: StartMode) -> Node {
        let mut nc = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(PC).expect("pc"));
        nc.mode = mode;
        nc.leader.shuffler.idle_poll = Duration::from_millis(20);
        timeout(Duration::from_secs(120), Node::open(nc))
            .await
            .expect("node open timed out")
            .expect("node open")
    }

    fn h(node: &Node, p: u16) -> PartitionHandle<Rf1Log> {
        node.handle(PartitionId(p)).expect("ready").clone()
    }

    fn start(id: &RunId, parent: Option<(&RunId, u64)>, retention: u64) -> StartRun {
        StartRun {
            run_id: id.as_bytes().to_vec(),
            workflow: "wf".into(),
            workflow_version: 1,
            queue: "q".into(),
            input: Some(PayloadRef::inline(Bytes::from_static(b"{}"))),
            conflict_policy: ConflictPolicy::ReturnExisting as i32,
            requested_at_ms: 100,
            ready_at_ms: 100,
            rng_seed: vec![7; 32],
            retention_ms: retention,
            parent: parent.map(|(p, op)| ParentRef {
                run_id: p.as_bytes().to_vec(),
                op,
            }),
            ..Default::default()
        }
    }

    fn send(kind: outbox_msg::Kind) -> Derived {
        Derived::new(derived::Intent::SendMessage(SendMessage {
            msg: Some(OutboxMsg { kind: Some(kind) }),
        }))
    }

    fn complete(status: TerminalStatus) -> Derived {
        Derived::new(derived::Intent::CompleteRun(CompleteRun {
            status: status as i32,
            result: None,
            completed_at_ms: 200,
            resumable: false,
            parent_event: None,
        }))
    }

    fn child_of(parent: &RunId, op: u64, p: u16) -> RunId {
        RunId::new(PartitionId(p), child_run_uuid(parent, op))
    }

    struct R {
        id: RunId,
        token: AttemptToken,
        next: u64,
    }

    async fn submit(node: &Node, p: u16, c: Command) {
        let kind = c.kind_name();
        let o = h(node, p).submit_one(c).await.expect("submit");
        assert_eq!(o, ostro_partition::ApplyOutcome::Accepted, "{kind}");
    }

    async fn wait_run(node: &Node, id: &RunId) {
        let t0 = tokio::time::Instant::now();
        while h(node, id.partition().0)
            .read_run(id)
            .expect("read")
            .is_none()
        {
            assert!(t0.elapsed() < T, "run {id} never arrived");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn run(node: &Node, s: Option<StartRun>, id: RunId) -> R {
        if let Some(s) = s {
            submit(node, id.partition().0, Command::start_run(s)).await;
        }
        wait_run(node, &id).await;
        let token = h(node, id.partition().0)
            .grant_attempt(id)
            .await
            .expect("grant");
        R { id, token, next: 0 }
    }

    async fn append(node: &Node, r: &mut R, n: u64, d: Vec<Derived>) {
        // Inputs (child completions) may have been journaled meanwhile.
        r.next = h(node, r.id.partition().0)
            .read_run(&r.id)
            .expect("read")
            .expect("record")
            .next_seq;
        let events = (r.next..r.next + n)
            .map(|seq| JournalEvent {
                seq,
                kind: 16,
                payload: Some(PayloadRef::inline(Bytes::from_static(b"e"))),
            })
            .collect();
        submit(
            node,
            r.id.partition().0,
            Command::events_appended(EventsAppended {
                run_id: r.id.as_bytes().to_vec(),
                token: Some(AttemptTokenMsg::from(r.token)),
                from_seq: r.next,
                events,
                derived: d,
            }),
        )
        .await;
        r.next += n;
    }

    fn outbox_rows(node: &Node) -> usize {
        node.slots()
            .iter()
            .filter_map(|s| match s {
                PartitionSlot::Ready(h) => Some(h),
                _ => None,
            })
            .map(|h| {
                h.reader()
                    .prefix(ostro_partition::Table::Outbox, &[], usize::MAX)
                    .expect("outbox")
                    .len()
            })
            .sum()
    }

    async fn settle(node: &Node) {
        let ends = || -> Vec<u64> { (0..PC).map(|p| h(node, p).log().log_end().0).collect() };
        let t0 = tokio::time::Instant::now();
        let mut last = ends();
        loop {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let now = ends();
            if now == last && outbox_rows(node) == 0 {
                return;
            }
            assert!(t0.elapsed() < T, "node never settled");
            last = now;
        }
    }

    fn states(node: &Node) -> Vec<([u8; 32], [u8; 32], u64)> {
        (0..PC)
            .map(|p| {
                let hd = h(node, p);
                let meta = hd.store().read_meta().expect("meta");
                (
                    hd.store().digest().expect("digest"),
                    meta.state_hash,
                    meta.applied.0,
                )
            })
            .collect()
    }

    fn copy_dir(src: &Path, dst: &Path) {
        std::fs::create_dir_all(dst).expect("mkdir");
        for e in std::fs::read_dir(src).expect("read_dir") {
            let e = e.expect("entry");
            let to = dst.join(e.file_name());
            if e.file_type().expect("file type").is_dir() {
                copy_dir(&e.path(), &to);
            } else {
                std::fs::copy(e.path(), &to).expect("copy");
            }
        }
    }

    async fn drive(dir: &Path) {
        let node = open(dir, StartMode::Leader).await;
        // Parent A (p0): attached c1 (p1), detached c2 (p0), attached c3 (p1, will CAN).
        let a_id = RunId::new_v7(PartitionId(0));
        let mut a = run(&node, Some(start(&a_id, None, 1)), a_id).await;
        let (c1, c2, c3) = (
            child_of(&a_id, 1, 1),
            child_of(&a_id, 2, 0),
            child_of(&a_id, 3, 1),
        );
        let sc = |c: &RunId, op: u64, attached: bool| {
            send(outbox_msg::Kind::StartChild(StartChild {
                start: Some(start(c, attached.then_some((&a_id, op)), 1)),
            }))
        };
        append(
            &node,
            &mut a,
            4,
            vec![
                sc(&c1, 1, true),
                Derived::child_journal(&c1),
                sc(&c2, 2, false),
                sc(&c3, 3, true),
                Derived::child_journal(&c3),
            ],
        )
        .await;
        // c1 completes normally.
        let mut r1 = run(&node, None, c1).await;
        append(&node, &mut r1, 1, vec![complete(TerminalStatus::Completed)]).await;
        // c3 continues as new into s3 (p0); s3 completes -> uuid match, chain_done.
        let s3 = RunId::new_v7(PartitionId(0));
        let mut r3 = run(&node, None, c3).await;
        append(
            &node,
            &mut r3,
            1,
            vec![
                send(outbox_msg::Kind::StartRun(start(&s3, None, 1))),
                complete(TerminalStatus::ContinuedAsNew),
            ],
        )
        .await;
        let mut rs3 = run(&node, None, s3).await;
        append(
            &node,
            &mut rs3,
            1,
            vec![complete(TerminalStatus::Completed)],
        )
        .await;
        settle(&node).await;
        // Both attached completions consumed their rows (c3's through the uuid match), and
        // chain_done released c3 (cleared, or deleted when its expiry was deferred).
        let ra = h(&node, 0).read_run(&a_id).expect("read").expect("a");
        assert_eq!(ra.children, 0);
        assert_eq!(ra.next_seq, 6, "two journaled completions");
        let rc3 = h(&node, 1).read_run(&c3).expect("read");
        assert!(
            rc3.as_ref().is_none_or(|r| r.continued_to.is_none()),
            "{rc3:?}"
        );
        // A re-sends c1's start (re-completion of a terminal child).
        append(
            &node,
            &mut a,
            0,
            vec![sc(&c1, 1, true), Derived::child_journal(&c1)],
        )
        .await;
        settle(&node).await;
        // Parent B (p1) with attached d1 (p0): terminated (cascade), resumed, completes.
        let b_id = RunId::new_v7(PartitionId(1));
        let mut b = run(&node, Some(start(&b_id, None, 4_102_444_800_000)), b_id).await;
        let d1 = child_of(&b_id, 1, 0);
        append(
            &node,
            &mut b,
            2,
            vec![
                send(outbox_msg::Kind::StartChild(StartChild {
                    start: Some(start(&d1, Some((&b_id, 1)), 1)),
                })),
                Derived::child_journal(&d1),
            ],
        )
        .await;
        settle(&node).await;
        submit(
            &node,
            1,
            Command::terminate_run(TerminateRun {
                run_id: b_id.as_bytes().to_vec(),
                reason: "ops".into(),
                terminated_at_ms: 300,
            }),
        )
        .await;
        settle(&node).await;
        let rd = h(&node, 0).read_run(&d1).expect("read").expect("d1");
        assert!(rd.cancel_requested, "cascade reached d1");
        assert_eq!(
            h(&node, 1)
                .read_run(&b_id)
                .expect("read")
                .expect("b")
                .children,
            0
        );
        submit(
            &node,
            1,
            Command::retry_run(RetryRun {
                run_id: b_id.as_bytes().to_vec(),
                requested_at_ms: 400,
            }),
        )
        .await;
        let mut d = run(&node, None, d1).await;
        append(&node, &mut d, 1, vec![complete(TerminalStatus::Cancelled)]).await;
        // A closes (no rows left) and B's resumed attempt re-sends d1's start, then closes.
        append(&node, &mut a, 1, vec![complete(TerminalStatus::Completed)]).await;
        let mut b2 = run(&node, None, b_id).await;
        b2.next = h(&node, 1)
            .read_run(&b_id)
            .expect("read")
            .expect("b")
            .next_seq;
        append(
            &node,
            &mut b2,
            0,
            vec![
                send(outbox_msg::Kind::StartChild(StartChild {
                    start: Some(start(&d1, Some((&b_id, 1)), 1)),
                })),
                Derived::child_journal(&d1),
            ],
        )
        .await;
        settle(&node).await;
        let next = h(&node, 1)
            .read_run(&b_id)
            .expect("read")
            .expect("b")
            .next_seq;
        b2.next = next;
        append(&node, &mut b2, 1, vec![complete(TerminalStatus::Completed)]).await;
        settle(&node).await;
        node.shutdown().await.expect("shutdown");
    }

    #[test]
    fn chain_children_workload_rebuilds_to_the_same_state() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let root = ostro_testkit::fs::tempdir();
            let live = root.path().join("live");
            drive(&live).await;
            let copy = root.path().join("rebuild");
            copy_dir(&live, &copy);
            std::fs::remove_dir_all(copy.join("state")).expect("delete state");
            let a = open(&live, StartMode::Inspect).await;
            let b = open(&copy, StartMode::Inspect).await;
            let (sa, sb) = (states(&a), states(&b));
            for (p, st) in (0..PC).zip(&sa) {
                let leo = h(&a, p).log().log_end().0;
                assert_eq!(st.2, leo, "p{p}: live applied == LEO");
            }
            assert_eq!(sa, sb, "live == rebuilt");
            a.shutdown().await.expect("shutdown");
            b.shutdown().await.expect("shutdown");
        });
    }
}
