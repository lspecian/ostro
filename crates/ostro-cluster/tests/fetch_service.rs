//! Leader `Fetch` handler (12-11 Task 1): validation order, divergence, progress, long-poll
//! vs read timeouts (RP-12), durable-capped reads (W3) and the decode limit, called directly
//! and through a tonic server over loopback mTLS.

mod support_fetch;
mod support_meta;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use ostro_cluster::fetch::{FetchConfig, FetchCtx, fetch_decode_limit, handle_fetch};
use ostro_log::{FollowerStatus, PartitionLog, ReplicaSet, ReplicatedLog};
use ostro_partition::SystemClock;
use ostro_proto::cluster::v1::replication_client::ReplicationClient;
use ostro_proto::cluster::v1::{FetchError, FetchRequest, FetchResponse};
use ostro_types::{Epoch, Offset};
use support_fetch::*;

fn set(replicas: &[u64], isr: &[u64]) -> ReplicaSet {
    ReplicaSet {
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version: 0,
        min_insync: 1,
    }
}

/// Leader node 1 of partition 0, replicas [1, 2, 3], ISR [1]: epoch 1 wrote offsets 0..3,
/// epoch 2 wrote 3..5 (history [(1, 0), (2, 3)]).
async fn leader() -> (LogFx, Assignments) {
    let fx = LogFx::open(1).await;
    fx.log
        .become_leader(Epoch(1), set(&[1, 2, 3], &[1]))
        .unwrap();
    append(&fx.log, Epoch(1), vec![cmd(0), cmd(1)]).await;
    append(&fx.log, Epoch(1), vec![cmd(2)]).await;
    fx.log
        .become_leader(Epoch(2), set(&[1, 2, 3], &[1]))
        .unwrap();
    append(&fx.log, Epoch(2), vec![cmd(3)]).await;
    append(&fx.log, Epoch(2), vec![cmd(4)]).await;
    let a = assignments(view(1, 2, &[1, 2, 3], &[1]));
    (fx, a)
}

fn ctx(fx: &LogFx, a: &Assignments, cfg: FetchConfig) -> FetchCtx {
    FetchCtx::new(
        Arc::clone(&fx.log),
        a.subscribe(),
        Arc::new(SystemClock),
        cfg,
    )
}

fn req(replica: u64, epoch: u32, offset: u64, last_epoch: u32, durable: u64) -> FetchRequest {
    FetchRequest {
        partition: 0,
        replica_id: replica,
        leader_epoch: epoch,
        fetch_offset: offset,
        last_fetched_epoch: last_epoch,
        durable_leo: durable,
        max_bytes: 1 << 20,
        max_wait_ms: 0,
        min_bytes: 1,
    }
}

fn err(r: &FetchResponse) -> FetchError {
    FetchError::try_from(r.error).unwrap()
}

fn row(log: &PartitionLog, replica: u64) -> FollowerStatus {
    log.replica_status()
        .followers
        .into_iter()
        .find(|f| f.replica == replica)
        .expect("follower row")
}

#[tokio::test]
async fn validation_order_and_every_error_code() {
    let (fx, a) = leader().await;
    let c = ctx(&fx, &a, FetchConfig::default());

    // 1. identity: a node outside the replicas, and a replica naming another replica_id.
    let r = handle_fetch(&c, 4, req(4, 2, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::NotReplica);
    let r = handle_fetch(&c, 2, req(3, 2, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::NotReplica);
    // identity comes before fencing: a foreign node with a stale epoch is NOT_REPLICA.
    let r = handle_fetch(&c, 4, req(4, 1, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::NotReplica);

    // 2. fencing.
    let r = handle_fetch(&c, 2, req(2, 1, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::FencedLeaderEpoch);
    assert_eq!(r.leader_epoch, 2);
    let r = handle_fetch(&c, 2, req(2, 3, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::UnknownLeaderEpoch);

    // 3. not leader: meta says epoch 3 and this log follows it.
    let other = LogFx::open(1).await;
    other.log.become_follower(Epoch(3), 2).unwrap();
    let oa = assignments(view(2, 3, &[1, 2, 3], &[2]));
    let oc = FetchCtx::new(
        Arc::clone(&other.log),
        oa.subscribe(),
        Arc::new(SystemClock),
        FetchConfig::default(),
    );
    let r = handle_fetch(&oc, 3, req(3, 3, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::NotLeader);
    // ... and a stale epoch there is still fenced first.
    let r = handle_fetch(&oc, 3, req(3, 2, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::FencedLeaderEpoch);

    // 4. divergence: a follower holding epoch 1 up to 4 diverges at (1, 3).
    let r = handle_fetch(&c, 2, req(2, 2, 4, 1, 4)).await.unwrap();
    assert_eq!(err(&r), FetchError::None);
    let d = r.diverging.expect("diverging");
    assert_eq!((d.epoch, d.end_offset), (1, 3));
    assert!(r.envelopes.is_empty());
    assert_eq!(r.high_watermark, 5);
    // ... snapshot required: records above log start with no epoch.
    let r = handle_fetch(&c, 2, req(2, 2, 2, 0, 2)).await.unwrap();
    assert_eq!(err(&r), FetchError::SnapshotRequired);
    // neither recorded progress.
    let f = row(&fx.log, 2);
    assert_eq!((f.fetch_offset, f.durable_leo), (Offset(0), Offset(0)));

    // 5-6. a valid fetch from the start gets data.
    let r = handle_fetch(&c, 2, req(2, 2, 0, 0, 0)).await.unwrap();
    assert_eq!(err(&r), FetchError::None);
    assert!(r.diverging.is_none());
    assert_eq!(r.envelopes.len(), 4, "all four batches fit");
    assert_eq!(r.leader_durable_end, 5);
    assert_eq!(r.log_start, 0);
    // verbatim: byte-identical to the stored envelopes.
    let stored = envelopes(&fx.log, Offset(0)).await;
    assert_eq!(
        r.envelopes,
        stored.iter().map(|b| b.to_vec()).collect::<Vec<_>>()
    );
    other.shutdown().await;
    fx.shutdown().await;
}

#[tokio::test]
async fn progress_is_capped_and_recorded_only_after_ok() {
    let (fx, a) = leader().await;
    let c = ctx(&fx, &a, FetchConfig::default());
    // Over-reported durable_leo (100 > fetch_offset 3) counts as 3. (fetch 3 with last
    // epoch 1 is the end of epoch 1: Ok.)
    let r = handle_fetch(&c, 2, req(2, 2, 3, 1, 100)).await.unwrap();
    assert_eq!(err(&r), FetchError::None);
    assert!(r.diverging.is_none());
    let f = row(&fx.log, 2);
    assert_eq!(f.fetch_offset, Offset(3));
    assert_eq!(f.durable_leo, Offset(3));
    // A diverging fetch afterwards records nothing.
    let r = handle_fetch(&c, 2, req(2, 2, 4, 1, 4)).await.unwrap();
    assert!(r.diverging.is_some());
    assert_eq!(row(&fx.log, 2).fetch_offset, Offset(3));
    fx.shutdown().await;
}

#[tokio::test]
async fn caught_up_follower_long_polls_and_a_write_returns_early() {
    let (fx, a) = leader().await;
    let cfg = FetchConfig {
        max_wait: Duration::from_millis(300),
        ..FetchConfig::default()
    };
    let c = Arc::new(ctx(&fx, &a, cfg));

    // Caught up: waits ~max_wait, returns empty; progress was recorded before the wait.
    let mut r0 = req(2, 2, 5, 2, 5);
    r0.max_wait_ms = 10_000; // capped by the config's 300 ms
    let started = Instant::now();
    let c2 = Arc::clone(&c);
    let h = tokio::spawn(async move { handle_fetch(&c2, 2, r0).await.unwrap() });
    let log = Arc::clone(&fx.log);
    wait_until("progress before the wait", Duration::from_secs(1), || {
        row(&log, 2).fetch_offset == Offset(5)
    })
    .await;
    assert!(!h.is_finished(), "still waiting");
    let r = h.await.unwrap();
    let took = started.elapsed();
    assert!(r.envelopes.is_empty());
    assert_eq!(err(&r), FetchError::None);
    assert!(took >= Duration::from_millis(280), "{took:?}");
    assert!(took < Duration::from_secs(3), "{took:?}");

    // A write during the wait returns early with it.
    let mut r1 = req(2, 2, 5, 2, 5);
    r1.max_wait_ms = 300;
    let c3 = Arc::clone(&c);
    let started = Instant::now();
    let h = tokio::spawn(async move { handle_fetch(&c3, 2, r1).await.unwrap() });
    tokio::time::sleep(Duration::from_millis(50)).await;
    append(&fx.log, Epoch(2), vec![cmd(5)]).await;
    let r = h.await.unwrap();
    assert_eq!(r.envelopes.len(), 1);
    assert!(
        started.elapsed() < Duration::from_millis(280),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(r.leader_durable_end, 6);
    fx.shutdown().await;
}

#[tokio::test]
async fn read_slower_than_max_wait_still_returns_data_rp12() {
    let (fx, a) = leader().await;
    let cfg = FetchConfig {
        max_wait: Duration::from_millis(100),
        read_timeout: Duration::from_secs(5),
        ..FetchConfig::default()
    };
    let c = ctx(&fx, &a, cfg).with_read_hook(Arc::new(|| {
        Box::pin(tokio::time::sleep(Duration::from_millis(500)))
    }));
    let mut r = req(2, 2, 0, 0, 0);
    r.max_wait_ms = 100;
    let started = Instant::now();
    let resp = handle_fetch(&c, 2, r).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(500));
    assert_eq!(
        resp.envelopes.len(),
        4,
        "a slow read is never cut short by max_wait"
    );

    // A read past fetch_read_timeout fails the fetch; it never answers "no data".
    let cfg = FetchConfig {
        max_wait: Duration::from_millis(100),
        read_timeout: Duration::from_millis(150),
        ..FetchConfig::default()
    };
    let c = ctx(&fx, &a, cfg).with_read_hook(Arc::new(|| {
        Box::pin(tokio::time::sleep(Duration::from_millis(600)))
    }));
    let e = handle_fetch(&c, 2, req(2, 2, 0, 0, 0)).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::DeadlineExceeded);
    fx.shutdown().await;
}

#[tokio::test]
async fn durable_end_is_snapshotted_before_the_read_w3() {
    let (fx, a) = leader().await;
    let log = Arc::clone(&fx.log);
    let appended = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let flag = Arc::clone(&appended);
    let c = ctx(&fx, &a, FetchConfig::default()).with_read_hook(Arc::new(move || {
        let log = Arc::clone(&log);
        let flag = Arc::clone(&flag);
        Box::pin(async move {
            if !flag.swap(true, std::sync::atomic::Ordering::SeqCst) {
                // Becomes durable between the snapshot and the read.
                append(&log, Epoch(2), vec![cmd(50)]).await;
            }
        })
    }));
    let r = handle_fetch(&c, 2, req(2, 2, 0, 0, 0)).await.unwrap();
    assert!(appended.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(fx.log.durable_end(), Offset(6));
    assert_eq!(r.leader_durable_end, 5, "the pre-read snapshot");
    assert_eq!(r.envelopes.len(), 4, "nothing beyond the snapshot");
    // The tracker's target is the snapshot: fetching at 5 is caught up, at 6 it is reached.
    let before = row(&fx.log, 2).last_caught_up_ms;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let r = handle_fetch(&c, 2, req(2, 2, 5, 2, 5)).await.unwrap();
    assert_eq!(r.envelopes.len(), 1);
    assert!(row(&fx.log, 2).last_caught_up_ms > before);
    fx.shutdown().await;
}

#[tokio::test]
async fn over_mtls_identity_and_max_size_envelope() {
    let max_env = ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES;
    let fx = LogFx::open(1).await;
    fx.log.become_leader(Epoch(1), set(&[1, 2], &[1])).unwrap();
    // One envelope of exactly max_envelope_bytes (32-byte header + 4-byte length prefix).
    let big = Bytes::from(vec![7u8; max_env - 32 - 4]);
    append(&fx.log, Epoch(1), vec![big]).await;
    let stored = envelopes(&fx.log, Offset(0)).await;
    assert_eq!(stored[0].len(), max_env);
    let a = assignments(view(1, 1, &[1, 2], &[1]));
    let cfg = FetchConfig {
        max_bytes: max_env,
        ..FetchConfig::default()
    };
    let limit = fetch_decode_limit(max_env, cfg.max_bytes);
    assert_eq!(limit, max_env + 64 * 1024);
    let c = Arc::new(FetchCtx::new(
        Arc::clone(&fx.log),
        a.subscribe(),
        Arc::new(SystemClock),
        cfg,
    ));
    let ca = test_ca();
    let srv = ReplServer::start(&ca, 1, Some(c), Arc::clone(&fx.blobs), limit).await;

    let client = |tls, limit: usize| {
        let t = support_meta::client_transport(&ca.cluster_id, tls, 1, srv.addr);
        let ch = ostro_cluster::transport::ClusterTransport::channel(&*t, 1).unwrap();
        ReplicationClient::new(ch)
            .max_decoding_message_size(limit)
            .max_encoding_message_size(limit)
    };

    // A replica gets the maximum-size envelope through the limit.
    let r = client(ca.node(2), limit)
        .fetch(req(2, 1, 0, 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(err(&r), FetchError::None);
    assert_eq!(r.envelopes.len(), 1);
    assert_eq!(r.envelopes[0], stored[0].to_vec());
    // Without the 64 KiB slack the same response does not decode.
    let e = client(ca.node(2), max_env)
        .fetch(req(2, 1, 0, 0, 0))
        .await
        .unwrap_err();
    assert_ne!(e.code(), tonic::Code::Ok);

    // A node outside the replica set: NOT_REPLICA. A node claiming another replica id: too.
    let r = client(ca.node(3), limit)
        .fetch(req(3, 1, 0, 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(err(&r), FetchError::NotReplica);
    let r = client(ca.node(3), limit)
        .fetch(req(2, 1, 0, 0, 0))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(err(&r), FetchError::NotReplica);
    // A worker certificate and a node of another cluster carry no node identity of this
    // cluster: refused before the handler (authz::require_node).
    let e = client(ca.worker("tok", "w1"), limit)
        .fetch(req(2, 1, 0, 0, 0))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);
    let e = client(ca.node_of("other-cluster", 2), limit)
        .fetch(req(2, 1, 0, 0, 0))
        .await
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied);

    srv.stop().await;
    fx.shutdown().await;
}

#[tokio::test]
async fn fetch_round_trip_rate() {
    // Measured for the SUMMARY: unary fetch round trips of one small batch over mTLS.
    let fx = LogFx::open(1).await;
    fx.log.become_leader(Epoch(1), set(&[1, 2], &[1])).unwrap();
    let n = 200u64;
    for i in 0..n {
        append(&fx.log, Epoch(1), vec![cmd(i)]).await;
    }
    let a = assignments(view(1, 1, &[1, 2], &[1]));
    let c = Arc::new(FetchCtx::new(
        Arc::clone(&fx.log),
        a.subscribe(),
        Arc::new(SystemClock),
        FetchConfig::default(),
    ));
    let ca = test_ca();
    let limit = fetch_decode_limit(ostro_types::envelope::DEFAULT_MAX_ENVELOPE_BYTES, 8 << 20);
    let srv = ReplServer::start(&ca, 1, Some(c), Arc::clone(&fx.blobs), limit).await;
    let t = support_meta::client_transport(&ca.cluster_id, ca.node(2), 1, srv.addr);
    let ch = ostro_cluster::transport::ClusterTransport::channel(&*t, 1).unwrap();
    let mut client = ReplicationClient::new(ch).max_decoding_message_size(limit);
    let started = Instant::now();
    let mut at = 0u64;
    let mut last_epoch = 0;
    while at < n {
        let mut r = req(2, 1, at, last_epoch, at);
        r.max_bytes = 1; // one batch per round trip
        let resp = client.fetch(r).await.unwrap().into_inner();
        assert_eq!(resp.envelopes.len(), 1);
        at += 1;
        last_epoch = 1;
    }
    let took = started.elapsed();
    eprintln!(
        "fetch round trips: {n} in {took:?} ({:.0}/s, {:?} each)",
        n as f64 / took.as_secs_f64(),
        took / u32::try_from(n).unwrap()
    );
    srv.stop().await;
    fx.shutdown().await;
}
