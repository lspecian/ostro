//! STOR-05: apply never passes HW, the fjall checkpoint cadence (100 ms / 256 batches, on
//! demand, on shutdown) and restart replay from the state's own `applied_offset`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::time::Duration;

use common::manual_hw::ManualHwLog;
use common::{T, cfg, new_run, open_single, open_with, rebuild_digest, start_cmd};
use ostro_log::{LogNode, LogNodeConfig, ReplicatedLog};
use ostro_partition::{ApplyOutcome, MachineConfig};
use ostro_types::command::encode_command;
use ostro_types::{Epoch, Offset};
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_stops_exactly_at_hw() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_with(tmp.path(), cfg(), true, ManualHwLog::new).await;
    let log = tp.handle.log().clone();
    let a0 = tp.applied();
    log.set_cap(a0);

    let jobs: Vec<_> = (0..10)
        .map(|_| {
            let h = tp.handle.clone();
            tokio::spawn(async move { h.submit_one(start_cmd(&new_run())).await })
        })
        .collect();
    // All ten appended (and durable in the inner log) ...
    timeout(T, async {
        while *tp.rf1.high_watermark().borrow() < a0 + 10 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("appends never became durable");
    // ... but nothing applied or acknowledged while HW is held back.
    sleep(Duration::from_millis(300)).await;
    assert_eq!(tp.applied(), a0);
    assert_eq!(jobs.iter().filter(|j| j.is_finished()).count(), 0);

    log.set_cap(a0 + 5);
    let mut applied = tp.handle.machine().applied();
    timeout(T, applied.wait_for(|a| *a >= a0 + 5))
        .await
        .unwrap()
        .unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(tp.applied(), a0 + 5, "apply passed the HW cap");
    assert_eq!(jobs.iter().filter(|j| j.is_finished()).count(), 5);

    log.set_cap(Offset(u64::MAX));
    for j in jobs {
        let o = timeout(T, j).await.unwrap().unwrap().unwrap();
        assert_eq!(o, ApplyOutcome::Accepted);
    }
    assert_eq!(tp.applied(), a0 + 10);
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_every_n_batches() {
    let tmp = ostro_testkit::fs::tempdir();
    let c = MachineConfig {
        checkpoint_batches: 4,
        checkpoint_every: Duration::from_secs(3600),
        ..cfg()
    };
    let tp = open_single(tmp.path(), c).await;
    let mut last = tp.handle.machine().force_checkpoint().await.unwrap();
    assert_eq!(last, tp.applied());
    let mut durable = tp.handle.machine().durable_checkpoint();
    for i in 1..=8u32 {
        tp.handle.submit_one(start_cmd(&new_run())).await.unwrap();
        let a = tp.applied();
        if i % 4 == 0 {
            timeout(T, durable.wait_for(|d| *d == a))
                .await
                .expect("no checkpoint after the 4th batch")
                .unwrap();
            last = a;
        } else {
            assert_eq!(*durable.borrow(), last, "checkpoint before the 4th batch ({i})");
        }
    }
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn checkpoint_every_interval() {
    let tmp = ostro_testkit::fs::tempdir();
    let c = MachineConfig {
        checkpoint_batches: u32::MAX,
        checkpoint_every: Duration::from_millis(50),
        ..cfg()
    };
    let tp = open_single(tmp.path(), c).await;
    for _ in 0..3 {
        tp.handle.submit_one(start_cmd(&new_run())).await.unwrap();
    }
    let a = tp.applied();
    let mut durable = tp.handle.machine().durable_checkpoint();
    timeout(Duration::from_millis(500), durable.wait_for(|d| *d >= a))
        .await
        .expect("interval checkpoint did not catch up within 500 ms")
        .unwrap();
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_shutdown_checkpoints_and_reopen_has_nothing_to_replay() {
    let tmp = ostro_testkit::fs::tempdir();
    let c = MachineConfig {
        checkpoint_batches: u32::MAX,
        checkpoint_every: Duration::from_secs(3600),
        ..cfg()
    };
    let tp = open_single(tmp.path(), c.clone()).await;
    for _ in 0..5 {
        tp.handle.submit_one(start_cmd(&new_run())).await.unwrap();
    }
    let applied = tp.applied();
    let digest = tp.handle.store().digest().unwrap();
    let ck = tp.close().await.unwrap();
    assert_eq!(ck, applied, "final checkpoint != applied");

    let tp = open_with(tmp.path(), c, false, |l| l).await;
    let meta = tp.handle.store().read_meta().unwrap();
    assert_eq!(meta.applied, applied);
    assert_eq!(meta.checkpointed, applied);
    assert_eq!(tp.applied(), applied);
    assert_eq!(tp.rf1.log_end(), applied, "nothing to replay");
    assert_eq!(tp.handle.store().digest().unwrap(), digest);
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_replays_exactly_the_missing_suffix() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    for _ in 0..3 {
        tp.handle.submit_one(start_cmd(&new_run())).await.unwrap();
    }
    let epoch = tp.handle.epoch();
    let applied = tp.close().await.unwrap();

    // Append directly to the log while no machine runs.
    {
        let (node, mut logs) = LogNode::open(
            LogNodeConfig::new(tmp.path().join("wal"), common::pc(), 1),
            &[applied],
        )
        .await
        .unwrap();
        let log = logs.remove(0).unwrap();
        let cmds = (0..4)
            .map(|_| encode_command(&start_cmd(&new_run())))
            .collect();
        let r = log.append(epoch, cmds).await.unwrap();
        timeout(T, log.wait_committed(r.last))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(r.base, applied);
        node.shutdown().await;
    }

    let tp = open_with(tmp.path(), cfg(), false, |l| l).await;
    assert_eq!(tp.handle.epoch(), Epoch(epoch.0));
    assert_eq!(tp.applied(), applied + 4, "replayed exactly the missing suffix");
    let meta = tp.handle.store().read_meta().unwrap();
    assert_eq!(meta.applied, applied + 4);
    assert_eq!(
        tp.handle.store().digest().unwrap(),
        rebuild_digest(&*tp.rf1).await,
        "digest differs from a fresh rebuild"
    );
    tp.close().await.unwrap();
}
