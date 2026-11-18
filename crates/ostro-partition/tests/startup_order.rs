//! Node startup order, readiness barrier, fixed partition count, corrupt-partition isolation,
//! torn tail cut at open, state-ahead-of-log detection (plan 02-12; STOR-03, STOR-08, STOR-09).

mod common;

use common::{T, open_node, run_on, start_cmd};
use ostro_log::recovery::{list_segments, segment_dir};
use ostro_log::{LogError, ReplicatedLog};
use ostro_partition::{
    ApplyOutcome, Cursor, Node, NodeConfig, NodeError, PartitionError, PartitionRouter,
    PartitionSlot, PartitionStore, StartMode, StartupEvent, StoreConfig,
};
use ostro_testkit::corrupt;
use ostro_types::command::InboundBatch;
use ostro_types::{Offset, PartitionCount, PartitionId};
use tokio::time::timeout;

fn kind(e: &StartupEvent) -> u8 {
    match e {
        StartupEvent::NodeFileChecked => 0,
        StartupEvent::StateOpened { .. } => 1,
        StartupEvent::LogRecovered { .. } => 2,
        StartupEvent::Validated { .. } => 3,
        StartupEvent::CaughtUp { .. } => 4,
        StartupEvent::TermStarted { .. } => 5,
        StartupEvent::BarrierOpened { .. } => 6,
        StartupEvent::LeaderServicesStarted { .. } => 7,
        StartupEvent::PartitionFailed { .. } => 8,
    }
}

fn kinds_of(ev: &[StartupEvent], p: u16) -> Vec<u8> {
    ev.iter()
        .filter(|e| e.partition() == Some(p))
        .map(kind)
        .collect()
}

async fn submit_start(node: &Node, p: u16) -> ApplyOutcome {
    let h = node.handle(PartitionId(p)).expect("ready");
    timeout(T, h.submit_one(start_cmd(&run_on(p))))
        .await
        .expect("submit timed out")
        .expect("submit")
}

fn failed_reason(node: &Node, p: u16) -> String {
    match &node.slots()[usize::from(p)] {
        PartitionSlot::Failed { partition, reason } => {
            assert_eq!(*partition, PartitionId(p));
            reason.clone()
        }
        PartitionSlot::Ready(_) => panic!("partition {p} unexpectedly ready"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn fresh_node_startup_order() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 4, StartMode::Leader).await;
    let ev = node.startup_events();
    assert_eq!(ev[0], StartupEvent::NodeFileChecked);
    for p in 0..4u16 {
        assert_eq!(
            kinds_of(&ev, p),
            vec![1, 2, 3, 4, 5, 6, 7],
            "partition {p}: {ev:?}"
        );
        assert!(node.barrier(PartitionId(p)).expect("barrier").is_open());
        assert!(matches!(
            ev.iter()
                .find(|e| matches!(e, StartupEvent::TermStarted { p: q, .. } if *q == p)),
            Some(StartupEvent::TermStarted {
                epoch: 1,
                offset: 0,
                ..
            })
        ));
    }
    // services start only after every partition's barrier opened
    let last_barrier = ev.iter().rposition(|e| kind(e) == 6).expect("barrier");
    let first_service = ev.iter().position(|e| kind(e) == 7).expect("services");
    assert!(last_barrier < first_service, "{ev:?}");
    for p in 0..4u16 {
        assert_eq!(submit_start(&node, p).await, ApplyOutcome::Accepted);
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn inspect_mode_keeps_barrier_closed() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    assert_eq!(submit_start(&node, 1).await, ApplyOutcome::Accepted);
    node.shutdown().await.expect("shutdown");

    let node = open_node(d.path(), 2, StartMode::Inspect).await;
    let ev = node.startup_events();
    for p in 0..2u16 {
        assert_eq!(kinds_of(&ev, p), vec![1, 2, 3, 4], "partition {p}: {ev:?}");
        let b = node.barrier(PartitionId(p)).expect("barrier");
        assert!(matches!(b.mint(Offset(1)), Err(LogError::NotReady)));
        // caught up, nothing appended
        let h = node.handle(PartitionId(p)).expect("ready");
        assert_eq!(*h.machine().applied().borrow(), h.log().log_end());
    }
    assert_eq!(
        node.handle(PartitionId(1)).unwrap().log().log_end(),
        Offset(2)
    );
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn partition_count_is_fixed() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 4, StartMode::Leader).await;
    node.shutdown().await.expect("shutdown");
    let s = std::fs::read_to_string(d.path().join("node.toml")).unwrap();
    assert!(s.contains("partitions = 4"), "{s}");
    assert!(s.contains("format_version = 1"), "{s}");

    let cfg = NodeConfig::new(d.path().to_path_buf(), PartitionCount::new(16).unwrap());
    match Node::open(cfg).await {
        Err(NodeError::PartitionCountMismatch {
            configured: 16,
            stored: 4,
        }) => {}
        other => panic!("expected PartitionCountMismatch, got {other:?}"),
    }
    // the stores record the count too: a lost node.toml is still caught per partition
    std::fs::remove_file(d.path().join("node.toml")).unwrap();
    let cfg = NodeConfig::new(d.path().to_path_buf(), PartitionCount::new(16).unwrap());
    let node = Node::open(cfg).await.expect("open");
    for p in 0..4u16 {
        assert!(failed_reason(&node, p).contains("partition count"), "{p}");
    }
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupt_partition_fails_alone() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 4, StartMode::Leader).await;
    for _ in 0..20 {
        assert_eq!(submit_start(&node, 2).await, ApplyOutcome::Accepted);
    }
    node.shutdown().await.expect("shutdown");
    // second term: partition 2's first segment is sealed and no longer the last one
    let node = open_node(d.path(), 4, StartMode::Leader).await;
    node.shutdown().await.expect("shutdown");

    let segs = list_segments(
        &segment_dir(&d.path().join("wal"), PartitionId(2)),
        PartitionId(2),
    )
    .unwrap();
    let (_, victim) = segs
        .iter()
        .find(|(_, p)| std::fs::metadata(p).unwrap().len() > 0)
        .expect("non-empty segment")
        .clone();
    let len = std::fs::metadata(&victim).unwrap().len();
    corrupt::flip_bit(&victim, len / 2, 3).unwrap();

    let node = open_node(d.path(), 4, StartMode::Leader).await;
    let reason = failed_reason(&node, 2);
    let name = victim.file_name().unwrap().to_string_lossy().to_string();
    assert!(reason.contains(&name), "{reason} should name {name}");
    assert!(node.handle(PartitionId(2)).is_none());
    assert!(!node.barrier(PartitionId(2)).unwrap().is_open());
    assert!(
        node.startup_events()
            .iter()
            .any(|e| matches!(e, StartupEvent::PartitionFailed { p: 2, .. }))
    );
    for p in [0u16, 1, 3] {
        assert_eq!(submit_start(&node, p).await, ApplyOutcome::Accepted);
    }
    // routing to the failed partition is refused
    let r = node
        .router()
        .deliver(
            PartitionId(2),
            InboundBatch {
                src: 0,
                msgs: vec![],
            },
        )
        .await;
    assert!(matches!(r, Err(PartitionError::NotLeader)), "{r:?}");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn torn_tail_cut_at_open() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    for _ in 0..5 {
        assert_eq!(submit_start(&node, 0).await, ApplyOutcome::Accepted);
    }
    node.shutdown().await.expect("shutdown");
    let segs = list_segments(
        &segment_dir(&d.path().join("wal"), PartitionId(0)),
        PartitionId(0),
    )
    .unwrap();
    let (_, last) = segs
        .iter()
        .rev()
        .find(|(_, p)| std::fs::metadata(p).unwrap().len() > 0)
        .expect("non-empty segment")
        .clone();
    let before = std::fs::metadata(&last).unwrap().len();
    corrupt::append_garbage(&last, &[0x4f, 0x57, 0x02, 0x00, 0x10]).unwrap();

    let node = open_node(d.path(), 2, StartMode::Leader).await;
    let ev = node.startup_events();
    assert!(
        ev.contains(&StartupEvent::LogRecovered {
            p: 0,
            leo: 6,
            torn: true
        }),
        "{ev:?}"
    );
    assert!(ev.contains(&StartupEvent::LogRecovered {
        p: 1,
        leo: 1,
        torn: false
    }));
    assert_eq!(std::fs::metadata(&last).unwrap().len(), before);
    // the new term starts right after the cut, and submits continue contiguously
    assert!(ev.contains(&StartupEvent::TermStarted {
        p: 0,
        epoch: 2,
        offset: 6
    }));
    assert_eq!(submit_start(&node, 0).await, ApplyOutcome::Accepted);
    let h = node.handle(PartitionId(0)).unwrap();
    assert_eq!(h.log().log_end(), Offset(8));
    assert_eq!(*h.machine().applied().borrow(), Offset(8));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread")]
async fn state_ahead_of_log_fails_partition() {
    let d = ostro_testkit::fs::tempdir();
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    assert_eq!(submit_start(&node, 1).await, ApplyOutcome::Accepted);
    node.shutdown().await.expect("shutdown");
    {
        let pc = PartitionCount::new(2).unwrap();
        let (store, meta) =
            PartitionStore::open(d.path(), PartitionId(1), pc, &StoreConfig::default()).unwrap();
        let mut cur = Cursor::from_meta(&meta);
        cur.next = Offset(1_000);
        let mu = cur.meta_update();
        store.batch().commit(&mu).unwrap();
        store.checkpoint(Offset(1_000)).unwrap();
    }
    let node = open_node(d.path(), 2, StartMode::Leader).await;
    let reason = failed_reason(&node, 1);
    assert!(reason.contains("state ahead of log"), "{reason}");
    assert_eq!(submit_start(&node, 0).await, ApplyOutcome::Accepted);
    node.shutdown().await.expect("shutdown");
}
