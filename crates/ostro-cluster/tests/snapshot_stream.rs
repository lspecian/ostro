//! `Replication.FetchSnapshot` (12-12 Task 1): the leader streams its newest usable OSNP v1
//! snapshot frame by frame, byte-identical to the file, resumable, pinned against
//! retention, with single-flight on-demand exports, behind the Fetch authorization.

mod support_install;
mod support_meta;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ostro_cluster::snapshot::{Exporter, SnapshotServeConfig, stream_snapshot};
use ostro_partition::leader::snapshot::{
    SnapshotConfig, SnapshotService, SnapshotShared, snapshot_dir,
};
use ostro_partition::store::snapshot::{HEADER_LEN, SnapshotHeader, verify_snapshot};
use ostro_partition::{Clock, ManualClock, SystemClock};
use ostro_proto::cluster::v1::FetchSnapshotRequest;
use support_install::*;

fn req(replica: u64, resume_frame: u64) -> FetchSnapshotRequest {
    FetchSnapshotRequest {
        partition: u32::from(P.0),
        replica_id: replica,
        resume_frame,
    }
}

fn check_frames(frames: &[ostro_proto::cluster::v1::SnapshotFrame], from: u64) {
    for (i, f) in frames.iter().enumerate() {
        assert_eq!(f.index, from + i as u64, "frame indices are consecutive");
        assert_eq!(f.last, i + 1 == frames.len(), "last only on the trailer");
    }
}

#[tokio::test]
async fn stream_equals_the_file_byte_for_byte() {
    let leader = LeaderFx::new(40, 64).await;
    let path = leader.write_local_snapshot();
    let file = std::fs::read(&path).unwrap();
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = Arc::new(leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    ));
    let ca = test_ca();
    let srv = ReplServer::start(
        &ca,
        TestReplication {
            fetch: None,
            snapshot: Arc::clone(&ctx),
            faults: Default::default(),
        },
    )
    .await;
    let mut client = snapshot_client(&ca, FOLLOWER, srv.addr);
    let frames = fetch_frames(&mut client, FOLLOWER, 0).await.unwrap();
    check_frames(&frames, 0);
    assert_eq!(
        frames[0].bytes.len(),
        HEADER_LEN,
        "frame 0 is the 64-byte header"
    );
    let h = SnapshotHeader::decode(frames[0].bytes.as_slice().try_into().unwrap()).unwrap();
    assert_eq!(h.applied_offset, leader.s.0);
    assert!(
        frames.len() > 20,
        "one frame per OSNP frame ({})",
        frames.len()
    );
    let bytes = concat(&frames);
    assert_eq!(bytes, file, "concatenated frames == the snapshot file");
    let info = verify_snapshot(bytes.as_slice()).unwrap();
    assert_eq!(info.applied_offset, leader.s.0);
    assert_eq!(
        ctx.stats().exports.load(Ordering::Relaxed),
        0,
        "no export needed"
    );
    srv.stop().await;
    leader.shutdown().await;
}

#[tokio::test]
async fn newest_snapshot_covering_log_start_is_chosen() {
    let leader = LeaderFx::new(10, 16).await;
    let newest = leader.write_local_snapshot();
    // An older snapshot (below log_start) and a corrupt "newer" one are skipped.
    let dir = snapshot_dir(leader.data_dir(), P);
    let old = dir.join("00000000000000000001.osnap");
    std::fs::write(&old, b"old snapshot below log start").unwrap();
    let bad = dir.join("00000000000000000999.osnap");
    std::fs::write(&bad, vec![0u8; 200]).unwrap();
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    );
    let frames = collect(
        stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 0))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(concat(&frames), std::fs::read(&newest).unwrap());
    leader.shutdown().await;
}

#[tokio::test]
async fn resume_from_frame_k_yields_the_identical_suffix() {
    let leader = LeaderFx::new(30, 32).await;
    leader.write_local_snapshot();
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = Arc::new(leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    ));
    let ca = test_ca();
    let srv = ReplServer::start(
        &ca,
        TestReplication {
            fetch: None,
            snapshot: Arc::clone(&ctx),
            faults: Default::default(),
        },
    )
    .await;
    let mut client = snapshot_client(&ca, FOLLOWER, srv.addr);
    let full = fetch_frames(&mut client, FOLLOWER, 0).await.unwrap();
    let n = full.len() as u64;
    for k in 1..n {
        let suffix = fetch_frames(&mut client, FOLLOWER, k).await.unwrap();
        check_frames(&suffix, k);
        assert_eq!(suffix, full[k as usize..].to_vec(), "resume at {k}");
    }
    assert_eq!(ctx.stats().resumes.load(Ordering::Relaxed), n - 1);
    srv.stop().await;
    leader.shutdown().await;
}

#[tokio::test]
async fn concurrent_requests_share_one_export() {
    let leader = LeaderFx::new(200, 256).await;
    let replicas = [LEADER, 2, 3, 4, 5];
    let a = assignments(&replicas);
    let ctx = Arc::new(leader.snapshot_ctx(
        a.subscribe(),
        Exporter::Store(Arc::clone(&leader.store)),
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    ));
    let mut tasks = Vec::new();
    for r in 2..=5u64 {
        let ctx = Arc::clone(&ctx);
        tasks.push(tokio::spawn(async move {
            collect(stream_snapshot(&ctx, r, req(r, 0)).await.unwrap())
                .await
                .unwrap()
        }));
    }
    let mut outs = Vec::new();
    for t in tasks {
        outs.push(concat(&t.await.unwrap()));
    }
    assert_eq!(ctx.stats().exports.load(Ordering::Relaxed), 1, "one export");
    for o in &outs {
        assert_eq!(o, &outs[0], "every requester gets the same snapshot");
    }
    let info = verify_snapshot(outs[0].as_slice()).unwrap();
    assert_eq!(info.applied_offset, leader.s.0);
    assert_eq!(info.logical_digest, leader.store.digest().unwrap());
    // The ephemeral export left no file behind.
    let dir = snapshot_dir(leader.data_dir(), P);
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(left.is_empty(), "ephemeral export unlinked: {left:?}");
    // A later requester reuses it.
    collect(stream_snapshot(&ctx, 2, req(2, 0)).await.unwrap())
        .await
        .unwrap();
    assert_eq!(ctx.stats().exports.load(Ordering::Relaxed), 1);
    leader.shutdown().await;
}

#[tokio::test]
async fn service_export_writes_a_durable_local_snapshot() {
    let leader = LeaderFx::new(20, 16).await;
    let svc = SnapshotService::new(
        P,
        count(),
        leader.data_dir(),
        Arc::clone(&leader.store),
        SnapshotConfig::default(),
        Arc::new(SystemClock),
        SnapshotShared::new(),
        Vec::new(),
    );
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = Arc::new(leader.snapshot_ctx(
        a.subscribe(),
        Exporter::Service(Arc::clone(&svc)),
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    ));
    let (x, y) = tokio::join!(
        async {
            collect(
                stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 0))
                    .await
                    .unwrap(),
            )
            .await
        },
        async {
            collect(
                stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 0))
                    .await
                    .unwrap(),
            )
            .await
        }
    );
    let (x, y) = (concat(&x.unwrap()), concat(&y.unwrap()));
    assert_eq!(x, y);
    assert_eq!(ctx.stats().exports.load(Ordering::Relaxed), 1, "one take()");
    let retained = svc.retained().await;
    assert_eq!(retained.len(), 1);
    assert_eq!(retained[0].applied_offset, leader.s);
    assert_eq!(std::fs::read(&retained[0].path).unwrap(), x);
    leader.shutdown().await;
}

#[tokio::test]
async fn non_replicas_are_refused() {
    let leader = LeaderFx::new(5, 16).await;
    leader.write_local_snapshot();
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = Arc::new(leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    ));
    let code = |r: Result<_, tonic::Status>| r.err().map(|s| s.code());
    // In-process: a non-replica, a replica claiming another id, another partition.
    assert_eq!(
        code(stream_snapshot(&ctx, 9, req(9, 0)).await),
        Some(tonic::Code::PermissionDenied)
    );
    assert_eq!(
        code(stream_snapshot(&ctx, FOLLOWER, req(9, 0)).await),
        Some(tonic::Code::PermissionDenied)
    );
    let mut other = req(FOLLOWER, 0);
    other.partition = 7;
    assert_eq!(
        code(stream_snapshot(&ctx, FOLLOWER, other).await),
        Some(tonic::Code::NotFound)
    );
    // Over mTLS: node 9 of this cluster is identified, then refused.
    let ca = test_ca();
    let srv = ReplServer::start(
        &ca,
        TestReplication {
            fetch: None,
            snapshot: Arc::clone(&ctx),
            faults: Default::default(),
        },
    )
    .await;
    let mut c9 = snapshot_client(&ca, 9, srv.addr);
    let e = fetch_frames(&mut c9, 9, 0).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::PermissionDenied, "{e}");
    let mut c2 = snapshot_client(&ca, FOLLOWER, srv.addr);
    assert!(!fetch_frames(&mut c2, FOLLOWER, 0).await.unwrap().is_empty());
    srv.stop().await;
    leader.shutdown().await;
}

#[tokio::test]
async fn a_snapshot_deleted_by_retention_keeps_streaming_and_resuming() {
    let leader = LeaderFx::new(300, 2048).await;
    let path = leader.write_local_snapshot();
    let file = std::fs::read(&path).unwrap();
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::new(SystemClock),
        SnapshotServeConfig {
            channel_frames: 1,
            ..SnapshotServeConfig::default()
        },
    );
    let mut s = stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 0))
        .await
        .unwrap();
    use futures_util::StreamExt as _;
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(s.next().await.unwrap().unwrap());
    }
    // Retention deletes the snapshot (as SnapshotService::take does beyond `keep`).
    std::fs::remove_file(&path).unwrap();
    assert!(!path.exists());
    while let Some(f) = s.next().await {
        got.push(f.unwrap());
    }
    assert_eq!(concat(&got), file, "the pinned file streamed to the end");
    // A resume of the same replica continues from the pin.
    let k = 5u64;
    let suffix = collect(
        stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, k))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(suffix, got[k as usize..].to_vec());
    assert!(ctx.pinned(FOLLOWER).is_some());
    // A fresh request finds nothing on disk and may not export.
    let e = stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 0))
        .await
        .err()
        .unwrap();
    assert_eq!(e.code(), tonic::Code::Unavailable, "{e}");
    leader.shutdown().await;
}

#[tokio::test]
async fn resume_without_a_pin_is_refused_and_pins_expire() {
    let leader = LeaderFx::new(5, 16).await;
    leader.write_local_snapshot();
    let a = assignments(&[LEADER, FOLLOWER]);
    let clock = Arc::new(ManualClock::new(1_000));
    let ctx = leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::clone(&clock) as Arc<dyn Clock>,
        SnapshotServeConfig {
            pin_ttl: Duration::from_secs(10),
            ..SnapshotServeConfig::default()
        },
    );
    let e = stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 3))
        .await
        .err()
        .unwrap();
    assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e}");
    collect(
        stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 0))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    clock.advance(9_000);
    collect(
        stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 3))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    clock.advance(10_001);
    let e = stream_snapshot(&ctx, FOLLOWER, req(FOLLOWER, 3))
        .await
        .err()
        .unwrap();
    assert_eq!(
        e.code(),
        tonic::Code::FailedPrecondition,
        "pin expired: {e}"
    );
    assert!(ctx.pinned(FOLLOWER).is_none());
    leader.shutdown().await;
}

/// Snapshot stream throughput over loopback mTLS (printed for the SUMMARY).
#[tokio::test]
async fn stream_throughput() {
    let leader = LeaderFx::new(4_000, 4096).await;
    let path = leader.write_local_snapshot();
    let size = std::fs::metadata(&path).unwrap().len();
    let a = assignments(&[LEADER, FOLLOWER]);
    let ctx = Arc::new(leader.snapshot_ctx(
        a.subscribe(),
        Exporter::None,
        Arc::new(SystemClock),
        SnapshotServeConfig::default(),
    ));
    let ca = test_ca();
    let srv = ReplServer::start(
        &ca,
        TestReplication {
            fetch: None,
            snapshot: Arc::clone(&ctx),
            faults: Default::default(),
        },
    )
    .await;
    let mut client = snapshot_client(&ca, FOLLOWER, srv.addr);
    for run in 0..3 {
        #[allow(clippy::disallowed_methods)] // wall clock: throughput measurement only
        let t = Instant::now();
        let frames = fetch_frames(&mut client, FOLLOWER, 0).await.unwrap();
        let secs = t.elapsed().as_secs_f64();
        assert_eq!(concat(&frames).len() as u64, size);
        println!(
            "snapshot stream run {run}: {size} bytes, {} frames in {secs:.3}s = {:.1} MiB/s",
            frames.len(),
            size as f64 / secs / (1 << 20) as f64
        );
    }
    srv.stop().await;
    leader.shutdown().await;
}
