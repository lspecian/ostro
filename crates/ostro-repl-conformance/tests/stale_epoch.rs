//! CLUS-01 core group `stale_epoch` (12-06). Pending scenarios print `PENDING <name>` unless
//! `OSTRO_REPL_RED_RUN` is set (then they must fail with a measured `ScenarioFailure`).

use std::sync::Arc;

use bytes::Bytes;
use ostro_cluster::fetch::{FetchConfig, FetchCtx, handle_fetch};
use ostro_log::replica::divergence::follower_truncation_point;
use ostro_log::{EpochHistory, ReplicaSet, ReplicatedLog};
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

/// p12-r4 regression (QA-01 10k campaign run 4, fault ~1,930): node 1 led p0 at epoch 696
/// just long enough for follower 3 to fetch its `LeaderAnnounced{696}`, then lost the
/// partition to node 2 at epoch 697. Node 2's history still ended at 695 (its own
/// `LeaderAnnounced{697}` was not in the log yet), so follower 3's fetch
/// (`last_fetched_epoch = 696`) got UNDEFINED → `SNAPSHOT_REQUIRED`: a whole-partition
/// install (snapshot 27 s, transfer, reopen) for a log that only needed one record dropped,
/// and the ISR stayed short past the campaign's 90 s budget. The leader of epoch 3 here
/// (history ending at 1, no announcement yet) must answer a follower holding a record of the
/// deposed epoch 2 with `diverging{1, LEO}`, and serve it after the truncation.
#[tokio::test]
async fn deposed_epoch_suffix_diverges_before_the_new_leaders_announcement() {
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
    // Epoch 2 was led elsewhere (deposed); this node leads epoch 3, not announced yet.
    log.become_leader(Epoch(3), set).expect("lead epoch 3");
    let assignment = PartitionAssignmentView {
        partition: p,
        leader: 1,
        epoch: Epoch(3),
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
    // Follower 3: both epoch-1 records plus one record of the deposed epoch 2.
    let follower = LogScan {
        node: 3,
        partition: p,
        records: vec![
            ScannedRecord {
                base: Offset(0),
                end: Offset(2),
                epoch: Epoch(1),
                envelope: Bytes::new(),
                commands: Vec::new(),
            },
            ScannedRecord {
                base: Offset(2),
                end: Offset(3),
                epoch: Epoch(2),
                envelope: Bytes::new(),
                commands: Vec::new(),
            },
        ],
        stopped: None,
    };
    let req = raw_fetch_request(&follower, 3);
    assert_eq!((req.fetch_offset, req.last_fetched_epoch), (3, 2));
    let resp = handle_fetch(&ctx, 3, req).await.expect("fetch");
    assert_eq!(resp.error(), FetchError::None, "{resp:?}");
    let div = resp
        .diverging
        .expect("a diverging answer, not a snapshot install");
    assert_eq!((div.epoch, div.end_offset), (1, 2), "{resp:?}");
    // The follower truncates where KIP-279 says and is then served from there.
    let own = EpochHistory(vec![(Epoch(1), Offset(0)), (Epoch(2), Offset(2))]);
    let point =
        follower_truncation_point(&own, Offset(3), Epoch(div.epoch), Offset(div.end_offset));
    assert_eq!(point, Offset(2));
    let again = ostro_proto::cluster::v1::FetchRequest {
        fetch_offset: point.0,
        last_fetched_epoch: 1,
        durable_leo: point.0,
        ..req
    };
    let resp = handle_fetch(&ctx, 3, again).await.expect("refetch");
    assert_eq!(resp.error(), FetchError::None, "{resp:?}");
    assert!(resp.diverging.is_none(), "{resp:?}");
    node.shutdown().await;
}
