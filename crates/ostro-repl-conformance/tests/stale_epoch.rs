//! CLUS-01 core group `stale_epoch` (12-06). Pending scenarios print `PENDING <name>` unless
//! `OSTRO_REPL_RED_RUN` is set (then they must fail with a measured `ScenarioFailure`).

use std::sync::Arc;

use bytes::Bytes;
use ostro_cluster::fetch::{FetchConfig, FetchCtx, handle_fetch};
use ostro_log::{ReplicaSet, ReplicatedLog};
use ostro_partition::SystemClock;
use ostro_partition::roles::{PartitionAssignmentView, open_partition_log_for_tests};
use ostro_proto::cluster::v1::FetchError;
use ostro_repl_conformance::oracle::disk::{LogScan, ScannedRecord};
use ostro_repl_conformance::scenarios::stale_epoch::raw_fetch_request;
use ostro_types::command::{StartRun, encode_command};
use ostro_types::{Command, Epoch, Offset, PartitionId};
use tokio::sync::watch;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_epoch() {
    ostro_repl_conformance::scenarios::runner::run_group_test("stale_epoch").await;
}

/// 12-18 regression (macOS CI run 45309300428, "answered SnapshotRequired, expected None"):
/// a new leader leads (role `Leader{2}`) before its `LeaderAnnounced{2}` is in its log. The
/// scenario's raw fetch must be what a real follower sends then: its last record's epoch
/// (1), which is served. The old request claimed `last_fetched_epoch = 2`, which the leader
/// cannot validate in that window (UNDEFINED end offset → `SNAPSHOT_REQUIRED`, KIP-101).
#[tokio::test]
async fn raw_fetch_is_served_before_the_new_leaders_announcement() {
    let p = PartitionId(0);
    let dir = tempfile::TempDir::new().expect("tempdir");
    let (node, log) = open_partition_log_for_tests(&dir.path().join("wal"), p, 1, Offset(0))
        .await
        .expect("open log");
    let set = ReplicaSet {
        replicas: vec![1, 2, 3],
        isr: vec![1],
        isr_version: 0,
        min_insync: 1,
    };
    log.become_leader(Epoch(1), set.clone())
        .expect("lead epoch 1");
    let cmds: Vec<Bytes> = (0..2u8)
        .map(|i| {
            encode_command(&Command::start_run(StartRun {
                run_id: vec![i; 16],
                workflow: "smoke".into(),
                ..Default::default()
            }))
        })
        .collect();
    let r = log.append(Epoch(1), cmds).await.expect("append");
    log.wait_committed(r.last).await.expect("committed");
    // Elected at epoch 2; LeaderAnnounced{2} not appended yet.
    log.become_leader(Epoch(2), set).expect("lead epoch 2");
    let assignment = PartitionAssignmentView {
        partition: p,
        leader: 1,
        epoch: Epoch(2),
        replicas: vec![1, 2, 3],
        isr: vec![1],
        isr_version: 0,
        min_insync: 1,
    };
    let (_tx, rx) = watch::channel(Arc::new(vec![assignment]));
    let ctx = FetchCtx::new(
        Arc::clone(&log),
        rx,
        Arc::new(SystemClock),
        FetchConfig::default(),
    );
    // Follower 2 holds both epoch-1 records.
    let follower = LogScan {
        node: 2,
        partition: p,
        records: vec![ScannedRecord {
            base: Offset(0),
            end: Offset(2),
            epoch: Epoch(1),
            envelope: Bytes::new(),
            commands: Vec::new(),
        }],
        stopped: None,
    };
    let req = raw_fetch_request(&follower, 2);
    assert_eq!((req.fetch_offset, req.last_fetched_epoch), (2, 1));
    let resp = handle_fetch(&ctx, 2, req).await.expect("fetch");
    assert_eq!(resp.error(), FetchError::None, "{resp:?}");
    // The old request's claim, in the same window.
    let claim = ostro_proto::cluster::v1::FetchRequest {
        last_fetched_epoch: 2,
        ..req
    };
    let resp = handle_fetch(&ctx, 2, claim).await.expect("fetch");
    assert_eq!(resp.error(), FetchError::SnapshotRequired);
    node.shutdown().await;
}
