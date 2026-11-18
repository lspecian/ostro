//! Term fencing across restarts (plan 02-12; STOR-10): every term begins with
//! `LeaderAnnounced{epoch}`, a restart is a new term, and tokens of an earlier term are
//! rejected after it.

mod common;

use std::sync::Arc;

use common::{T, open_node, run_on, start_cmd};
use ostro_log::ReplicatedLog;
use ostro_partition::{
    ApplyOutcome, LeadershipSource, Node, NodeConfig, PartitionError, RejectReason, StartMode,
    StartupEvent,
};
use ostro_types::command::{AttemptTokenMsg, Cmd, EventsAppended, JournalEvent, decode_command};
use ostro_types::{AttemptToken, Command, Epoch, PartitionCount, PartitionId, PayloadRef, RunId};
use tokio::time::timeout;

const P0: PartitionId = PartitionId(0);

fn term_of(node: &Node) -> (u32, u64) {
    node.startup_events()
        .iter()
        .find_map(|e| match e {
            StartupEvent::TermStarted {
                p: 0,
                epoch,
                offset,
            } => Some((*epoch, *offset)),
            _ => None,
        })
        .expect("TermStarted for p0")
}

fn events(run: &RunId, tok: (u32, u32), from: u64) -> Command {
    Command::events_appended(EventsAppended {
        run_id: run.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg {
            epoch: tok.0,
            n: tok.1,
        }),
        from_seq: from,
        events: vec![JournalEvent {
            seq: from,
            kind: 16,
            payload: Some(PayloadRef::inline(bytes::Bytes::from_static(b"e"))),
        }],
        derived: vec![],
    })
}

async fn submit(node: &Node, c: Command) -> ApplyOutcome {
    timeout(T, node.handle(P0).expect("ready").submit_one(c))
        .await
        .expect("submit timed out")
        .expect("submit")
}

fn rejected(o: &ApplyOutcome) -> bool {
    matches!(
        o,
        ApplyOutcome::Rejected(RejectReason::StaleEpoch { .. } | RejectReason::Fenced { .. })
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn every_term_starts_with_leader_announced() {
    let d = ostro_testkit::fs::tempdir();
    for expected in 1..=3u32 {
        let node = open_node(d.path(), 1, StartMode::Leader).await;
        assert_eq!(term_of(&node).0, expected);
        assert_eq!(node.handle(P0).unwrap().epoch(), Epoch(expected));
        assert_eq!(
            submit(&node, start_cmd(&run_on(0))).await,
            ApplyOutcome::Accepted
        );
        node.shutdown().await.expect("shutdown");
    }

    let node = open_node(d.path(), 1, StartMode::Inspect).await;
    let h = node.handle(P0).unwrap();
    let log = h.log();
    let hist = log.epochs();
    assert_eq!(
        hist.0.iter().map(|(e, _)| e.0).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    for (epoch, first) in &hist.0 {
        let b = log.read(*first, first.next(), 1 << 20).await.expect("read");
        let bytes = &b[0].commands[0];
        assert_eq!(b[0].base, *first);
        match decode_command(bytes).expect("decode").cmd {
            Some(Cmd::LeaderAnnounced(la)) => assert_eq!(la.epoch, epoch.0),
            other => panic!("first record of epoch {epoch:?} is {other:?}"),
        }
    }
    let meta = h.store().read_meta().expect("meta");
    let state_hist: Vec<(u32, u64)> = hist.0.iter().map(|(e, o)| (e.0, o.0)).collect();
    assert_eq!(meta.cursor.epoch_history, state_hist);
    assert_eq!(meta.cursor.epoch, 3);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn zombie_token_rejected_after_restart() {
    let d = ostro_testkit::fs::tempdir();
    let run = run_on(0);
    let node = open_node(d.path(), 1, StartMode::Leader).await;
    assert_eq!(submit(&node, start_cmd(&run)).await, ApplyOutcome::Accepted);
    let t1 = node.handle(P0).unwrap().grant_attempt(run).await.unwrap();
    assert_eq!(
        t1,
        AttemptToken {
            epoch: Epoch(1),
            n: 1
        }
    );
    node.shutdown().await.expect("shutdown");

    let node = open_node(d.path(), 1, StartMode::Leader).await;
    assert_eq!(term_of(&node).0, 2);
    let o = submit(&node, events(&run, (1, 1), 0)).await;
    assert!(rejected(&o), "zombie {{1,1}} must be rejected, got {o:?}");
    let t2 = node.handle(P0).unwrap().grant_attempt(run).await.unwrap();
    assert_eq!(
        t2,
        AttemptToken {
            epoch: Epoch(2),
            n: 1
        }
    );
    assert_eq!(
        submit(&node, events(&run, (2, 1), 0)).await,
        ApplyOutcome::Accepted
    );
    let o = submit(&node, events(&run, (1, 1), 1)).await;
    assert!(rejected(&o), "zombie {{1,1}} must stay rejected, got {o:?}");
    // a stale-epoch envelope (the old leader's handle) is refused by the log itself
    let h = node.handle(P0).unwrap();
    let stale = h.clone();
    stale.set_epoch(Epoch(1));
    let r = stale.submit_one(events(&run, (1, 1), 1)).await;
    assert!(
        matches!(
            r,
            Err(PartitionError::Log(ostro_log::LogError::StaleEpoch { .. }))
        ),
        "{r:?}"
    );
    h.set_epoch(Epoch(2));
    node.shutdown().await.expect("shutdown");
}

struct Floor(u32);

impl LeadershipSource for Floor {
    fn epoch_floor(&self, _p: PartitionId) -> Epoch {
        Epoch(self.0)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn leadership_floor_raises_epoch() {
    let d = ostro_testkit::fs::tempdir();
    for _ in 0..2 {
        let node = open_node(d.path(), 1, StartMode::Leader).await;
        node.shutdown().await.expect("shutdown");
    }
    let mut cfg = NodeConfig::new(d.path().to_path_buf(), PartitionCount::new(1).unwrap());
    cfg.leadership = Arc::new(Floor(10));
    let node = timeout(T, Node::open(cfg)).await.unwrap().unwrap();
    assert_eq!(node.handle(P0).unwrap().log().epochs().0.len(), 3);
    assert_eq!(term_of(&node), (10, 2));
    assert_eq!(
        submit(&node, start_cmd(&run_on(0))).await,
        ApplyOutcome::Accepted
    );
    node.shutdown().await.expect("shutdown");
    // the next local term continues above the floor's epoch
    let node = open_node(d.path(), 1, StartMode::Leader).await;
    assert_eq!(term_of(&node).0, 11);
    node.shutdown().await.expect("shutdown");
}
