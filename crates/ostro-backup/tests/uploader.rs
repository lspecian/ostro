//! Bottomless uploader (plan 08-13, BACK-01) on LocalFs: below-cut uploads, closed barrier,
//! keys and epoch split, blobs first, meta snapshot, upload floor vs trimming, remote
//! retention, per-tick cut lag (W8, with the P=16 timing report), the fixed round cadence
//! and parallel partition uploads behind one consistent cut (08-23), and the
//! `upload::before_manifest` failpoint.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{
    current_manifest, keys, local_store, node_cfg, open, shutdown, simulate_restore, start_runs,
    submit_ok, up_cfg, uploader,
};
use ostro_backup::chunk;
use ostro_backup::manifest;
use ostro_backup::uploader::Uploader;
use ostro_log::ReplicatedLog as _;
use ostro_partition::{StartMode, SystemClock};
use ostro_types::{Command, Offset, PartitionId};

const P0: PartitionId = PartitionId(0);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn uploads_only_below_the_cut_and_idle_rounds_write_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 2)).await;
    let up = uploader(&node, up_cfg());
    start_runs(&node, 0, 20).await;
    start_runs(&node, 1, 7).await;
    let r = up.round(&store).await.expect("round");
    let m = r.manifest.expect("manifest");
    assert_eq!(m.generation, 1);
    for e in &m.cut {
        let cut = r.cut[e.p as usize].0;
        assert!(e.cut_offset <= cut, "{e:?} beyond the cut {cut}");
        let hw = *node
            .handle(PartitionId(e.p))
            .expect("ready")
            .log()
            .high_watermark()
            .borrow();
        assert!(e.cut_offset <= hw.0);
        assert!(e.verify_offset <= e.cut_offset);
        assert_eq!(e.chain_start, 0);
        assert!(e.snapshot_key.is_none());
    }
    for k in keys(&store, "p/").await {
        let (p, _, _, end) = chunk::parse_chunk_key(&k).expect("chunk key");
        assert!(end <= r.cut[p as usize].0);
    }
    assert!(r.chunks >= 2);
    // Idle: nothing new, no manifest.
    let idle = up.round(&store).await.expect("idle round");
    assert_eq!(idle.chunks, 0);
    assert!(idle.manifest.is_none());
    assert_eq!(current_manifest(&store).await.expect("m").generation, 1);
    // One partition busy: only its chunks.
    start_runs(&node, 1, 3).await;
    let r2 = up.round(&store).await.expect("round 2");
    let m2 = r2.manifest.expect("manifest 2");
    assert_eq!(m2.generation, 2);
    assert_eq!(m2.prev_manifest_sha256, {
        let (b, _) = store
            .get(&manifest::gen_key(1))
            .await
            .expect("get")
            .expect("gen 1 written");
        manifest::object_sha(&b)
    });
    assert_eq!(r2.chunks, 1);
    assert_eq!(m2.cut[0], m.cut[0]);
    let restored = simulate_restore(&store, &m2, 2).await;
    for (rp, e) in restored.iter().zip(&m2.cut) {
        assert_eq!(rp.applied, e.cut_offset);
    }
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn closed_barrier_inspect_mode_uploads_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    {
        let node = open(node_cfg(dir.path(), 2)).await;
        start_runs(&node, 0, 5).await;
        shutdown(node).await;
    }
    let mut cfg = node_cfg(dir.path(), 2);
    cfg.mode = StartMode::Inspect;
    let node = open(cfg).await;
    let up = uploader(&node, up_cfg());
    let r = up.round(&store).await.expect("round");
    assert!(r.barrier_closed);
    assert!(keys(&store, "").await.is_empty(), "nothing uploaded");
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_split_at_epoch_boundaries() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    {
        let node = open(node_cfg(dir.path(), 1)).await;
        start_runs(&node, 0, 10).await;
        shutdown(node).await;
    }
    // Reopen: a new term (epoch bump), then more writes; one round covers both epochs.
    let node = open(node_cfg(dir.path(), 1)).await;
    start_runs(&node, 0, 10).await;
    let up = uploader(&node, up_cfg());
    let r = up.round(&store).await.expect("round");
    let m = r.manifest.expect("manifest");
    let mut parsed: Vec<(u32, u64, u64)> = keys(&store, "p/0000/seg/")
        .await
        .iter()
        .map(|k| {
            let (_, e, b, en) = chunk::parse_chunk_key(k).expect("key");
            (e, b, en)
        })
        .collect();
    parsed.sort_by_key(|x| x.1);
    let epochs: std::collections::BTreeSet<u32> = parsed.iter().map(|x| x.0).collect();
    assert!(epochs.len() >= 2, "chunks of two epochs: {parsed:?}");
    for w in parsed.windows(2) {
        assert_eq!(w[0].2, w[1].1, "contiguous");
        assert!(w[0].0 <= w[1].0, "epochs non-decreasing");
    }
    // Every chunk holds a single epoch (decode checks each envelope's epoch).
    for k in keys(&store, "p/0000/seg/").await {
        let (b, _) = store.get(&k).await.expect("get").expect("chunk");
        let c = chunk::decode(&b).expect("decode");
        let (_, e, base, end) = chunk::parse_chunk_key(&k).expect("key");
        assert_eq!(
            (c.header.epoch, c.header.base, c.header.end),
            (e, base, end)
        );
    }
    simulate_restore(&store, &m, 1).await;
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blobs_are_uploaded_before_chunks_and_meta_snapshot_is_named() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 1)).await;
    // A blob put before the uploader exists (startup reconcile) and one after (observer).
    let h = node.handle(P0).expect("ready");
    let early = h
        .prepare_payload(bytes::Bytes::from(vec![1u8; 300 * 1024]))
        .await
        .expect("blob");
    let (_view_sm, view) = ostro_meta::MetaSm::new();
    let up = Arc::new(Uploader::new(
        Arc::clone(&node),
        Some(view),
        Arc::new(SystemClock),
        up_cfg(),
    ));
    let late = h
        .prepare_payload(bytes::Bytes::from(vec![2u8; 300 * 1024]))
        .await
        .expect("blob");
    let mut cmds = Vec::new();
    for input in [early.clone(), late.clone()] {
        let run = common::run_on(0);
        let mut s = common::start_run(&run);
        s.input = Some(input);
        cmds.push(Command::start_run(s));
    }
    submit_ok(&node, P0, cmds).await;
    let r = up.round(&store).await.expect("round");
    assert_eq!(r.blobs, 2);
    assert!(r.meta_snapshot);
    let blob_keys = store.list("blobs/", None).await.expect("list");
    assert_eq!(blob_keys.len(), 2);
    let chunks = store.list("p/0000/seg/", None).await.expect("list");
    let first_chunk = chunks
        .iter()
        .map(|m| m.last_modified)
        .min()
        .expect("a chunk");
    for b in &blob_keys {
        assert!(
            b.last_modified <= first_chunk,
            "blob {} after a chunk",
            b.key
        );
    }
    let m = r.manifest.expect("manifest");
    let mk = m.meta_snapshot_key.clone().expect("meta snapshot named");
    let (mb, _) = store.get(&mk).await.expect("get").expect("meta snapshot");
    assert_eq!(m.meta_snapshot_sha256, Some(chunk::chunk_sha(&mb)));
    ostro_meta::decode_snapshot(&mb).expect("decodes as a meta snapshot");
    // The meta index did not move: no second meta upload.
    start_runs(&node, 0, 1).await;
    let r2 = up.round(&store).await.expect("round 2");
    assert!(!r2.meta_snapshot);
    assert_eq!(r2.blobs, 0);
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_floor_caps_trimming() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 1)).await;
    let up = uploader(&node, up_cfg());
    for _ in 0..3 {
        start_runs(&node, 0, 60).await;
        node.snapshot_now(P0).await.expect("snap").expect("new");
    }
    let log = Arc::clone(node.handle(P0).expect("ready").log());
    let floor = node.floor_handles(P0).expect("handles");
    assert_eq!(*floor.upload.borrow(), Some(Offset(0)));
    assert_eq!(node.retention_pass(P0).await.expect("pass"), Offset(0));
    // First round: the chain starts at the newest snapshot; the floor follows it.
    let r = up.round(&store).await.expect("round");
    let m = r.manifest.expect("manifest");
    let e = &m.cut[0];
    let s = e.snapshot_offset.expect("snapshot referenced");
    assert_eq!(*floor.upload.borrow(), Some(Offset(s.min(e.cut_offset))));
    let start = node.retention_pass(P0).await.expect("pass");
    assert!(
        start > Offset(0) && start.0 <= s,
        "log_start {start}, floor {s}"
    );
    assert_eq!(log.log_start(), start);
    // More data, a newer snapshot: the floor moves only after the manifest that references it.
    start_runs(&node, 0, 60).await;
    let snap = node.snapshot_now(P0).await.expect("snap").expect("new");
    assert_eq!(*floor.upload.borrow(), Some(Offset(s)));
    let r2 = up.round(&store).await.expect("round 2");
    let e2 = &r2.manifest.expect("manifest 2").cut[0];
    assert_eq!(e2.snapshot_offset, Some(snap.applied_offset.0));
    assert_eq!(*floor.upload.borrow(), Some(snap.applied_offset));
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_retention_keeps_k_snapshots_and_drops_forks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 1)).await;
    let mut cfg = up_cfg();
    cfg.remote_snapshots_keep = 2;
    let up = uploader(&node, cfg);
    start_runs(&node, 0, 30).await;
    up.round(&store).await.expect("round 0");
    // A forked chunk under seg/ (not on the chain).
    let env = ostro_types::envelope::encode(
        P0,
        ostro_types::Epoch(99),
        Offset(0),
        &[bytes::Bytes::from_static(b"fork")],
    );
    let (fh, fb) = chunk::encode(0, 99, [0; 32], &[&env]).expect("fork");
    let fork_key = chunk::chunk_key(0, 99, fh.base, fh.end);
    store.put(&fork_key, fb.into()).await.expect("put fork");
    let mut snaps = Vec::new();
    let mut last = None;
    for _ in 0..4 {
        start_runs(&node, 0, 30).await;
        up.round(&store).await.expect("round");
        snaps.push(
            node.snapshot_now(P0)
                .await
                .expect("snap")
                .expect("new")
                .applied_offset
                .0,
        );
        start_runs(&node, 0, 5).await;
        last = up.round(&store).await.expect("round").manifest;
    }
    let m = last.expect("manifest");
    let e = &m.cut[0];
    assert_eq!(e.snapshot_offset, snaps.last().copied());
    let remote_snaps: Vec<u64> = keys(&store, "p/0000/snap/")
        .await
        .iter()
        .map(|k| {
            ostro_backup::uploader::parse_snap_key(k)
                .expect("snap key")
                .1
        })
        .collect();
    assert_eq!(
        remote_snaps,
        snaps[snaps.len() - 2..].to_vec(),
        "K = 2 kept"
    );
    let oldest = snaps[snaps.len() - 2];
    assert!(
        e.chain_start <= oldest && e.chain_start > 0,
        "chain start {}",
        e.chain_start
    );
    let segs = keys(&store, "p/0000/seg/").await;
    assert!(!segs.contains(&fork_key), "fork deleted");
    for k in &segs {
        let (_, _, base, end) = chunk::parse_chunk_key(k).expect("key");
        assert!(
            end > oldest,
            "chunk [{base},{end}) is not needed by snapshot {oldest}"
        );
    }
    // Gen manifests that referenced deleted snapshots are gone; the rest still restore.
    for k in keys(&store, manifest::GEN_PREFIX).await {
        let (b, _) = store.get(&k).await.expect("get").expect("gen");
        let g = manifest::decode(&b).expect("gen decodes");
        if let Some(sk) = &g.cut[0].snapshot_key {
            assert!(
                store.head(sk).await.expect("head").is_some(),
                "{k} names {sk}"
            );
        }
    }
    simulate_restore(&store, &m, 1).await;
    drop(up);
    shutdown(node).await;
}

/// W8 at P=16 (all partitions busy): per tick, the time between consecutive cut reads minus
/// the previous round's duration stays within `seal_interval` + slack, and the cut lag at
/// manifest time minus the round duration stays within `seal_interval`. Prints the upload
/// latency separately (SUMMARY).
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn cut_lag_per_tick_at_p16() {
    const P: u16 = 16;
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), P)).await;
    let seal = Duration::from_millis(250);
    let mut cfg = up_cfg();
    cfg.seal_interval = seal;
    let up = uploader(&node, cfg);
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let node = Arc::clone(&node);
        let stop = Arc::clone(&stop);
        tokio::spawn(async move {
            let mut n = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut tasks = Vec::new();
                for p in 0..P {
                    let node = Arc::clone(&node);
                    tasks.push(tokio::spawn(async move {
                        start_runs(&node, p, 5).await;
                    }));
                }
                for t in tasks {
                    t.await.expect("writer");
                }
                n += 5 * u64::from(P);
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            n
        })
    };
    let slack = Duration::from_millis(500);
    let mut prev: Option<(u64, Duration)> = None;
    let mut uploads = Vec::new();
    let mut lags = Vec::new();
    for _ in 0..12 {
        tokio::time::sleep(seal).await;
        let r = up.round(&store).await.expect("round");
        if let Some((prev_cut, prev_upload)) = prev {
            let gap = Duration::from_millis(r.cut_at_ms - prev_cut);
            assert!(
                gap.saturating_sub(prev_upload) <= seal + slack,
                "cut gap {gap:?} (previous upload {prev_upload:?})"
            );
        }
        if let Some(at) = r.manifest_at_ms {
            let lag = Duration::from_millis(at - r.cut_at_ms);
            assert!(
                lag.saturating_sub(r.upload) <= seal,
                "cut lag {lag:?}, upload {:?}",
                r.upload
            );
            lags.push(lag);
        }
        uploads.push((r.upload, r.chunks, r.chunk_bytes));
        prev = Some((r.cut_at_ms, r.upload));
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let written = writer.await.expect("writer");
    let mut ups: Vec<Duration> = uploads.iter().map(|u| u.0).collect();
    ups.sort();
    let chunks: usize = uploads.iter().map(|u| u.1).sum();
    let bytes: u64 = uploads.iter().map(|u| u.2).sum();
    println!(
        "P=16 LocalFs, seal {seal:?}: {} rounds, {written} runs started, {chunks} chunks, {bytes} chunk bytes; \
         upload latency p50 {:?} max {:?}; cut lag at manifest max {:?}",
        ups.len(),
        ups[ups.len() / 2],
        ups[ups.len() - 1],
        lags.iter().max()
    );
    let m = current_manifest(&store).await.expect("manifest");
    assert_eq!(m.cut.len(), P as usize);
    drop(up);
    shutdown(node).await;
}

/// 08-23: with every partition busy, a round at any upload concurrency (1, auto = min(P, 8),
/// 16) writes one manifest whose entries all sit exactly at the single cut read in step 1,
/// and the last manifest restores (chains walk, state hashes match) for every partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn parallel_uploads_name_one_consistent_cut() {
    const P: u16 = 16;
    for conc in [1usize, 0, 16] {
        let dir = tempfile::tempdir().expect("tempdir");
        let remote = tempfile::tempdir().expect("remote");
        let store = local_store(remote.path(), true);
        let node = open(node_cfg(dir.path(), P)).await;
        let mut cfg = up_cfg();
        cfg.upload_concurrency = conc;
        assert_eq!(cfg.concurrency(P), if conc == 0 { 8 } else { conc });
        let up = uploader(&node, cfg);
        for gen_no in 1..=3u64 {
            for p in 0..P {
                start_runs(&node, p, 3 + usize::from(p % 4)).await;
            }
            let r = up.round(&store).await.expect("round");
            let m = r.manifest.expect("manifest");
            assert_eq!(m.generation, gen_no);
            assert_eq!(m.cut.len(), usize::from(P), "one entry per partition");
            assert_eq!(m.cut_at_ms, r.cut_at_ms);
            for e in &m.cut {
                assert_eq!(
                    e.cut_offset, r.cut[e.p as usize].0,
                    "concurrency {conc}: p{} not at the round's cut",
                    e.p
                );
            }
            assert!(r.chunks >= usize::from(P));
            if gen_no == 3 {
                simulate_restore(&store, &m, P).await;
            }
        }
        drop(up);
        shutdown(node).await;
    }
}

/// 08-23: `run` starts rounds on a fixed cadence of `seal_interval / 2` measured from round
/// start. Each round is held 150 ms after its cut (the after-cut hook); the gap between cut
/// reads stays near the 200 ms period instead of the old `seal_interval + round time`
/// (400 + 150 ms).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_starts_rounds_on_a_fixed_cadence() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 1)).await;
    let mut cfg = up_cfg();
    cfg.seal_interval = Duration::from_millis(400);
    assert_eq!(cfg.round_interval(), Duration::from_millis(200));
    let up = uploader(&node, cfg);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    up.set_after_cut_hook(Some(tx));
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let runner = tokio::spawn(Arc::clone(&up).run(Arc::clone(&store), stop_rx));
    let mut at = Vec::new();
    for _ in 0..8 {
        start_runs(&node, 0, 2).await;
        let resume: tokio::sync::oneshot::Sender<()> =
            tokio::time::timeout(Duration::from_secs(10), rx.recv())
                .await
                .expect("a round within 10 s")
                .expect("hook");
        at.push(std::time::Instant::now());
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = resume.send(());
    }
    up.set_after_cut_hook(None);
    drop(rx); // releases a round already waiting on the hook
    let _ = stop_tx.send(true);
    runner.await.expect("runner");
    let mut gaps: Vec<Duration> = at.windows(2).map(|w| w[1] - w[0]).collect();
    gaps.sort();
    let median = gaps[gaps.len() / 2];
    println!("round cadence 200 ms, 150 ms rounds: cut gaps {gaps:?}");
    assert!(
        median < Duration::from_millis(350),
        "median cut gap {median:?}: rounds are not on a fixed 200 ms cadence"
    );
    let m = current_manifest(&store).await.expect("manifest");
    assert!(m.generation >= 7, "generation {}", m.generation);
    drop(up);
    shutdown(node).await;
}

/// `upload::before_manifest`: a round that dies after uploading chunks leaves no manifest;
/// the retry re-puts identical chunks (AlreadyPresent, not fenced) and writes it.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failpoint_before_manifest_then_idempotent_retry() {
    use ostro_backup::uploader::UploadError;
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 2)).await;
    let up = uploader(&node, up_cfg());
    start_runs(&node, 0, 10).await;
    let scenario = fail::FailScenario::setup();
    fail::cfg("upload::before_manifest", "return").expect("cfg");
    match up.round(&store).await {
        Err(UploadError::Injected(n)) => assert_eq!(n, "upload::before_manifest"),
        other => panic!("expected the injected failure, got {other:?}"),
    }
    fail::remove("upload::before_manifest");
    assert!(current_manifest(&store).await.is_none());
    assert!(
        !keys(&store, "p/0000/seg/").await.is_empty(),
        "chunks were uploaded"
    );
    // A fresh uploader (as after a crash) re-uploads the same chunks idempotently.
    let up2 = uploader(&node, up_cfg());
    let r = up2.round(&store).await.expect("retry");
    assert!(r.manifest.is_some());
    assert!(!up2.is_fenced());
    scenario.teardown();
    drop((up, up2));
    shutdown(node).await;
}
