//! Rebuild a partition's state from its log (plan 02-12; STOR-06): deleting `state/pNNNN`
//! and reopening replays the log into a fresh store with the same digest, rolling hash and
//! applied offset.

mod common;

use bytes::Bytes;
use common::{T, copy_dir, open_node, run_on, start_run};
use ostro_log::ReplicatedLog;
use ostro_partition::{ApplyOutcome, Node, RejectReason, StartMode, Table};
use ostro_types::command::{
    AttemptTokenMsg, CompleteRun, Derived, EventsAppended, JournalEvent, OutboxMsg, ScheduleTimer,
    SendMessage, Signal, TerminalStatus, derived, outbox_msg,
};
use ostro_types::{AttemptToken, Command, PartitionId, PayloadRef, RunId};
use tokio::time::timeout;

const P: u16 = 4;

fn inline(b: &'static [u8]) -> Option<PayloadRef> {
    Some(PayloadRef::inline(Bytes::from_static(b)))
}

fn intent(i: derived::Intent) -> Derived {
    Derived { intent: Some(i) }
}

fn events(run: &RunId, t: AttemptToken, from: u64, derived: Vec<Derived>) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: t.epoch.0,
            n: t.n,
        }),
        from_seq: from,
        events: vec![JournalEvent {
            seq: from,
            kind: 16,
            payload: inline(b"e"),
        }],
        derived,
    })
}

fn signal(target: &RunId) -> Derived {
    intent(derived::Intent::SendMessage(SendMessage {
        msg: Some(OutboxMsg {
            kind: Some(outbox_msg::Kind::Signal(Signal {
                target_run: target.as_bytes().to_vec(),
                name: "s".into(),
                payload: inline(b"p"),
            })),
        }),
    }))
}

async fn submit(node: &Node, p: u16, c: Command) -> ApplyOutcome {
    let h = node.handle(PartitionId(p)).expect("ready");
    timeout(T, h.submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

/// Starts (keyed and unkeyed), grants, events with timers / cross-partition signals /
/// completions, then a manual shuffle of every outbox through the router.
async fn workload(node: &Node) {
    let mut runs: Vec<Vec<RunId>> = vec![Vec::new(); usize::from(P)];
    for i in 0..200u32 {
        let p = (i % u32::from(P)) as u16;
        let run = run_on(p);
        let mut s = start_run(&run);
        if i % 3 == 0 {
            s.idempotency_key = format!("k{}", i / 6).into_bytes();
        }
        if i % 7 == 0 {
            s.ready_at_ms = 10_000 + u64::from(i);
        }
        let o = submit(node, p, Command::start_run(s)).await;
        assert!(
            matches!(o, ApplyOutcome::Accepted | ApplyOutcome::Duplicate { .. }),
            "{o:?}"
        );
        if o == ApplyOutcome::Accepted {
            runs[usize::from(p)].push(run);
        }
    }
    for p in 0..P {
        let h = node.handle(PartitionId(p)).expect("ready");
        let mine = runs[usize::from(p)].clone();
        let other = &runs[usize::from((p + 1) % P)];
        for (j, run) in mine.iter().enumerate().take(30) {
            let Ok(t) = h.grant_attempt(*run).await else {
                continue; // delayed (Scheduled) runs cannot be granted yet
            };
            let timer = intent(derived::Intent::ScheduleTimer(ScheduleTimer {
                op: 1,
                due_at_ms: 5_000 + j as u64,
            }));
            let o = submit(node, p, events(run, t, 0, vec![timer, signal(&other[j])])).await;
            assert_eq!(o, ApplyOutcome::Accepted);
            if j % 2 == 0 {
                let done = intent(derived::Intent::CompleteRun(CompleteRun {
                    status: TerminalStatus::Completed as i32,
                    result: inline(b"out"),
                    completed_at_ms: 9,
                }));
                // The timer above is already overdue (real clock), so the timer service may
                // have journaled its TimerFired input at seq 1 (plan 04-13): follow
                // SeqMismatch to the journal end.
                let mut from = 1;
                loop {
                    match submit(node, p, events(run, t, from, vec![done.clone()])).await {
                        ApplyOutcome::Accepted => break,
                        ApplyOutcome::Rejected(RejectReason::SeqMismatch { expected }) => {
                            assert!(expected > from, "seq went backwards");
                            from = expected;
                        }
                        o => panic!("complete: {o:?}"),
                    }
                }
            }
        }
    }
    // The live shuffler (02-13) delivers every outbox and truncates it.
    settle(node).await;
    for p in 0..P {
        let h = node.handle(PartitionId(p)).expect("ready");
        let inbox = h
            .reader()
            .prefix(Table::Inbox, &[], usize::MAX)
            .expect("inbox");
        assert!(
            !inbox.is_empty(),
            "partition {p} received no cross-partition signal"
        );
    }
}

/// Wait until every outbox is empty and no partition's log has grown for 300 ms (the
/// leader services, shuffler and timers, have nothing left to append).
async fn settle(node: &Node) {
    common::wait_drained(node, T).await;
    let ends = || -> Vec<u64> {
        (0..P)
            .map(|p| {
                node.handle(PartitionId(p))
                    .expect("ready")
                    .log()
                    .log_end()
                    .0
            })
            .collect()
    };
    let deadline = tokio::time::Instant::now() + T;
    let mut last = ends();
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let now = ends();
        if now == last && common::outbox_rows(node) == 0 {
            return;
        }
        assert!(tokio::time::Instant::now() < deadline, "node never settled");
        last = now;
    }
}

struct PartState {
    digest: [u8; 32],
    hash: [u8; 32],
    applied: u64,
}

fn state_of(node: &Node, p: u16) -> PartState {
    let h = node.handle(PartitionId(p)).expect("ready");
    let meta = h.store().read_meta().expect("meta");
    PartState {
        digest: h.store().digest().expect("digest"),
        hash: meta.state_hash,
        applied: meta.applied.0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deleted_state_rebuilds_identically() {
    let root = ostro_testkit::fs::tempdir();
    let a = root.path().join("a");
    let b = root.path().join("b");
    let node = open_node(&a, P, StartMode::Leader).await;
    workload(&node).await;
    node.shutdown().await.expect("shutdown");

    copy_dir(&a, &b);
    std::fs::remove_dir_all(b.join("state/p0000")).unwrap();
    std::fs::remove_dir_all(b.join("state/p0002")).unwrap();

    let na = open_node(&a, P, StartMode::Inspect).await;
    let nb = open_node(&b, P, StartMode::Inspect).await;
    for p in 0..P {
        let (sa, sb) = (state_of(&na, p), state_of(&nb, p));
        assert!(sa.applied > 1, "partition {p} saw no work");
        assert_eq!(sa.applied, sb.applied, "applied p{p}");
        assert_eq!(sa.hash, sb.hash, "rolling hash p{p}");
        assert_eq!(sa.digest, sb.digest, "digest p{p}");
        let log_end = na.handle(PartitionId(p)).unwrap().log().log_end().0;
        assert_eq!(sa.applied, log_end);
    }
    // the rebuilt partitions really replayed from 0
    let ev = nb.startup_events();
    for p in [0u16, 2] {
        assert!(ev.contains(&ostro_partition::StartupEvent::StateOpened { p, applied: 0 }));
    }
    na.shutdown().await.expect("shutdown");
    nb.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn all_state_deleted_full_replay_then_continue() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), P, StartMode::Leader).await;
    workload(&node).await;
    let before: Vec<PartState> = (0..P).map(|p| state_of(&node, p)).collect();
    node.shutdown().await.expect("shutdown");

    std::fs::remove_dir_all(d.path().join("state")).unwrap();
    let node = open_node(d.path(), P, StartMode::Leader).await;
    for p in 0..P {
        let ev = node.startup_events();
        assert!(ev.contains(&ostro_partition::StartupEvent::CaughtUp {
            p,
            applied: before[usize::from(p)].applied
        }));
        assert!(ev.iter().any(|e| matches!(
            e,
            ostro_partition::StartupEvent::TermStarted { p: q, epoch: 2, .. } if *q == p
        )));
        assert_eq!(
            submit(&node, p, common::start_cmd(&run_on(p))).await,
            ApplyOutcome::Accepted
        );
    }
    node.shutdown().await.expect("shutdown");
}
