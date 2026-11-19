//! STOR-11: `submit` resolves each command's outcome only after the batch containing it was
//! committed to the state store; rejections surface as `Rejected`, never `Ok`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use common::{cfg, new_run, open_single, start_cmd, start_run};
use ostro_log::LogError;
use ostro_partition::{ACK_RETAIN, ApplyOutcome, PartitionError, PendingAcks, RejectReason};
use ostro_types::command::{AttemptGranted, AttemptTokenMsg, ConflictPolicy};
use ostro_types::{AttemptToken, Command, Epoch, Offset};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn state_is_visible_at_ack_time() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let run = new_run();
    let o = tp.handle.submit(vec![start_cmd(&run)]).await.unwrap();
    assert_eq!(o, vec![ApplyOutcome::Accepted]);
    let rec = tp
        .handle
        .read_run(&run)
        .unwrap()
        .expect("run visible at ack time");
    assert_eq!(rec.workflow, "wf");
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn outcomes_in_order_with_rejections() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let e = tp.handle.epoch();
    let (r1, r2) = (new_run(), new_run());
    let mut s1 = start_run(&r1);
    s1.idempotency_key = b"k".to_vec();
    let mut s2 = start_run(&r2);
    s2.idempotency_key = b"k".to_vec();
    s2.conflict_policy = ConflictPolicy::Reject as i32;
    let grant = Command::attempt_granted(AttemptGranted {
        run_id: r1.as_bytes().to_vec(),
        token: Some(AttemptTokenMsg { epoch: e.0, n: 1 }),
        after_failure: false,
    });
    let out = tp
        .handle
        .submit(vec![Command::start_run(s1), Command::start_run(s2), grant])
        .await
        .unwrap();
    assert_eq!(
        out,
        vec![
            ApplyOutcome::Accepted,
            ApplyOutcome::Rejected(RejectReason::IdempotencyConflict { existing: r1 }),
            ApplyOutcome::Accepted,
        ]
    );
    tp.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grant_attempt_tokens_and_stale_epoch() {
    let tmp = ostro_testkit::fs::tempdir();
    let tp = open_single(tmp.path(), cfg()).await;
    let e = tp.handle.epoch();
    let run = new_run();
    tp.handle.submit_one(start_cmd(&run)).await.unwrap();
    assert_eq!(
        tp.handle.grant_attempt(run).await.unwrap(),
        AttemptToken { epoch: e, n: 1 }
    );
    assert_eq!(
        tp.handle.grant_attempt(run).await.unwrap(),
        AttemptToken { epoch: e, n: 2 }
    );
    // Unknown run: the rejection surfaces as PartitionError::Rejected.
    assert!(matches!(
        tp.handle.grant_attempt(new_run()).await,
        Err(PartitionError::Rejected(RejectReason::RunNotFound))
    ));

    let stale = tp.handle.clone();
    stale.set_epoch(Epoch(e.0 - 1));
    let r = stale.submit_one(start_cmd(&new_run())).await;
    assert!(
        matches!(r, Err(PartitionError::Log(LogError::StaleEpoch { .. }))),
        "{r:?}"
    );
    tp.close().await.unwrap();
}

#[test]
fn pending_acks_resolve_before_register() {
    let acks = PendingAcks::new();
    acks.resolve(&[(Offset(0), ApplyOutcome::Accepted)], Offset(1));
    let mut rx = acks.register(Offset(0), Offset(0));
    assert_eq!(rx.len(), 1);
    match rx.remove(0).try_recv() {
        Ok(Ok(ApplyOutcome::Accepted)) => {}
        other => panic!("expected an already-completed receiver, got {other:?}"),
    }
}

#[test]
fn pending_acks_evicted_offset_is_ack_lost() {
    let acks = PendingAcks::new();
    acks.resolve(&[(Offset(0), ApplyOutcome::Accepted)], Offset(1));
    acks.resolve(&[], Offset(ACK_RETAIN + 10));
    let mut rx = acks.register(Offset(0), Offset(0));
    match rx.remove(0).try_recv() {
        Ok(Err(PartitionError::AckLost(Offset(0)))) => {}
        other => panic!("expected AckLost, got {other:?}"),
    }
}

#[test]
fn pending_acks_fail_all() {
    let acks = PendingAcks::new();
    let rx = acks.register(Offset(100), Offset(102));
    assert_eq!(rx.len(), 3);
    acks.fail_all("boom");
    for mut r in rx {
        assert!(matches!(r.try_recv(), Ok(Err(PartitionError::Failed(_)))));
    }
    // Registering after the failure fails immediately.
    let mut rx = acks.register(Offset(200), Offset(200));
    assert!(matches!(
        rx.remove(0).try_recv(),
        Ok(Err(PartitionError::Failed(_)))
    ));
}
