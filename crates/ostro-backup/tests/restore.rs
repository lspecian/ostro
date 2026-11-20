//! Restore pipeline (BACK-02/03, plan 08-18): verified restore from a LocalFs prefix and from
//! tar backup files (online + offline) reproduces the state at the cut, detects tampering,
//! refuses bad manifests and non-empty directories, and ignores forked chunks.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use common::{
    current_manifest, keys, local_store, node_cfg, open, run_on, shutdown, signals_cmd,
    simulate_restore, start_and_grant, start_runs, submit_ok, up_cfg, uploader,
};
use ostro_backup::archive::{backup_node_to_tar, backup_offline, pack_dir, unpack};
use ostro_backup::chunk;
use ostro_backup::manifest::{self, Manifest};
use ostro_backup::restore::{
    At, RestoreError, RestoreOptions, RestoreReport, RestoreSource, restore,
};
use ostro_backup::uploader::{Uploader, UploaderConfig};
use ostro_meta::MetaHandle;
use ostro_objstore::ObjectStore;
use ostro_partition::{Node, NodeConfig, PartitionSlot, StartMode, SystemClock};
use ostro_types::{Epoch, Offset, PartitionCount, PartitionId, envelope};

const P: u16 = 4;

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// Runs on every partition plus cross-partition signals from partition 0.
async fn workload(node: &Node, n: usize) {
    for p in 0..P {
        start_runs(node, p, n).await;
    }
    let sender = run_on(0);
    let t = start_and_grant(node, &sender).await;
    let mut targets = Vec::new();
    for p in 1..P {
        for r in start_runs(node, p, 2).await {
            targets.push((r, format!("sig-{p}")));
        }
    }
    submit_ok(
        node,
        PartitionId(0),
        vec![signals_cmd(&sender, t, &targets)],
    )
    .await;
}

/// Wait until every outbox message was delivered (the shuffler is asynchronous).
async fn settle(node: &Node) {
    for _ in 0..200 {
        let mut pending = false;
        for p in 0..P {
            let m = node
                .handle(PartitionId(p))
                .expect("ready")
                .store()
                .read_meta()
                .expect("meta");
            for q in 0..P {
                if q == p {
                    continue;
                }
                let next = m.cursor.outbox_next_seq.get(&q).copied().unwrap_or(1);
                let seen = node
                    .handle(PartitionId(q))
                    .expect("ready")
                    .store()
                    .read_meta()
                    .expect("meta")
                    .inbound_seen
                    .get(&p)
                    .copied()
                    .unwrap_or(0);
                if seen + 1 < next {
                    pending = true;
                }
            }
        }
        if !pending {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn opts(dir: &Path) -> RestoreOptions {
    let mut o = RestoreOptions::new(dir);
    o.node_id = Some(1);
    o
}

/// Restored digests must equal the reference restore (08-13 `simulate_restore`) of `m`.
async fn assert_matches(store: &Arc<dyn ObjectStore>, m: &Manifest, r: &RestoreReport) {
    let reference = simulate_restore(store, m, P).await;
    assert_eq!(r.partitions.len(), reference.len());
    for (got, want) in r.partitions.iter().zip(&reference) {
        assert_eq!(got.p, want.p);
        assert_eq!(got.cut_offset, want.applied, "partition {}", got.p);
        assert_eq!(got.digest, hex(&want.digest), "partition {} digest", got.p);
    }
    assert_eq!(r.generation, m.generation);
    assert_eq!(r.cut_at_ms, m.cut_at_ms);
}

/// The live digests of a stopped data directory (Inspect open).
async fn inspect_digests(dir: &Path) -> Vec<String> {
    let mut cfg = NodeConfig::new(dir.to_path_buf(), PartitionCount::new(P).unwrap());
    cfg.mode = StartMode::Inspect;
    let node = open(cfg).await;
    let mut out = Vec::new();
    for p in 0..P {
        let h = node.handle(PartitionId(p)).expect("ready");
        out.push(hex(&h.store().digest().expect("digest")));
    }
    shutdown(node).await;
    out
}

/// A source with meta, a 4-partition workload, snapshots and several upload rounds.
struct Fixture {
    _src: tempfile::TempDir,
    _remote: tempfile::TempDir,
    remote_path: std::path::PathBuf,
    store: Arc<dyn ObjectStore>,
    first: Manifest,
    last: Manifest,
}

async fn fixture() -> Fixture {
    let src = tempfile::tempdir().expect("src");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let meta = MetaHandle::start(&src.path().join("meta"), 1)
        .await
        .expect("meta");
    meta.init_cluster("restore-test", 1, P, 1, [3; 32])
        .await
        .expect("cluster");
    let node = open(node_cfg(src.path(), P)).await;
    workload(&node, 5).await;
    settle(&node).await;
    let up = Arc::new(Uploader::new(
        Arc::clone(&node),
        Some(meta.view().clone()),
        Arc::new(SystemClock),
        // Retention off: generation 1 (chains from 0) stays restorable for `--at`.
        UploaderConfig {
            retention: false,
            ..up_cfg()
        },
    ));
    let first = up
        .round(&store)
        .await
        .expect("round 1")
        .manifest
        .expect("manifest 1");
    // Snapshots on every partition, more work, more rounds.
    workload(&node, 4).await;
    settle(&node).await;
    for p in 0..P {
        node.snapshot_now(PartitionId(p)).await.expect("snapshot");
    }
    workload(&node, 3).await;
    settle(&node).await;
    up.round(&store).await.expect("round 2");
    workload(&node, 2).await;
    settle(&node).await;
    up.round(&store).await.expect("round 3");
    let last = current_manifest(&store).await.expect("current");
    assert!(last.generation > first.generation);
    drop(up);
    shutdown(node).await;
    meta.stop().await;
    Fixture {
        remote_path: remote.path().to_path_buf(),
        _src: src,
        _remote: remote,
        store,
        first,
        last,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_current_and_an_older_generation() {
    let f = fixture().await;
    assert!(f.last.cut.iter().all(|e| e.snapshot_offset.is_some()));

    // Current.
    let dst = tempfile::tempdir().expect("dst");
    let data = dst.path().join("data");
    let r = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(&data))
        .await
        .expect("restore current");
    assert_matches(&f.store, &f.last, &r).await;
    assert!(r.meta_index.is_some());
    assert_eq!(r.cluster_id, "test-cluster");
    assert!(r.partitions.iter().all(|p| p.snapshot_offset.is_some()));
    println!(
        "restore report: {}",
        serde_json::to_string(&r).expect("json")
    );
    // node.toml: new incarnation + restored_from.
    let toml = std::fs::read_to_string(data.join("node.toml")).expect("node.toml");
    assert!(
        toml.contains(&format!("incarnation = \"{}\"", r.incarnation)),
        "{toml}"
    );
    assert_ne!(r.incarnation, r.source_incarnation);
    assert!(toml.contains("restored_from = \"store\""), "{toml}");
    // The restored meta has the cluster record.
    let meta = MetaHandle::start(&data.join("meta"), 1)
        .await
        .expect("restored meta");
    let c = meta
        .view()
        .read(|s| s.cluster().map(|c| c.value.clone()))
        .expect("cluster");
    assert_eq!((c.cluster_id.as_str(), c.partitions), ("restore-test", P));
    meta.stop().await;
    // A Leader-mode open of the restored directory works and keeps the state.
    let node = open(NodeConfig::new(
        data.clone(),
        PartitionCount::new(P).unwrap(),
    ))
    .await;
    assert!(
        node.slots()
            .iter()
            .all(|s| matches!(s, PartitionSlot::Ready(_)))
    );
    shutdown(node).await;

    // --at the first generation (no snapshots referenced then: chains from 0).
    let dst2 = tempfile::tempdir().expect("dst2");
    let mut o = opts(dst2.path());
    o.at = Some(At::Generation(f.first.generation));
    let r = restore(RestoreSource::Store(Arc::clone(&f.store)), o)
        .await
        .expect("restore gen 1");
    assert_matches(&f.store, &f.first, &r).await;

    // --at a time: the newest manifest cut at or before it.
    let dst3 = tempfile::tempdir().expect("dst3");
    let mut o = opts(dst3.path());
    o.at = Some(At::TimeMs(f.first.cut_at_ms));
    let r = restore(RestoreSource::Store(Arc::clone(&f.store)), o)
        .await
        .expect("restore at time");
    assert!(r.cut_at_ms <= f.first.cut_at_ms);

    // A non-empty data directory is refused.
    let e = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(&data))
        .await
        .expect_err("non-empty");
    assert!(matches!(e, RestoreError::NotEmpty(_)), "{e}");
    assert!(
        e.to_string().contains("delete the directory and retry"),
        "{e}"
    );

    // Through a file:// URL: identical digests (same code path).
    let dst4 = tempfile::tempdir().expect("dst4");
    let url = format!("file://{}", f.remote_path.display());
    let r2 = restore(
        RestoreSource::Url {
            url: url.clone(),
            options: Default::default(),
        },
        opts(dst4.path()),
    )
    .await
    .expect("restore url");
    assert_matches(&f.store, &f.last, &r2).await;
    let toml = std::fs::read_to_string(dst4.path().join("node.toml")).expect("node.toml");
    assert!(
        toml.contains(&format!("restored_from = \"{url}\"")),
        "{toml}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tampering_and_bad_manifests_are_detected() {
    let f = fixture().await;
    // A forked chunk (same base, other epoch and end) planted under seg/ is ignored.
    let e0 = f.last.partition(1).expect("p1").clone();
    let head = e0.last_chunk_key.clone().expect("head");
    let (_, _, base, end) = chunk::parse_chunk_key(&head).expect("key");
    let env = envelope::encode(
        PartitionId(1),
        Epoch(99),
        Offset(base),
        &[Bytes::from_static(b"forked")],
    );
    let (_, bytes) = chunk::encode(1, 99, [0; 32], &[&env]).expect("encode fork");
    f.store
        .put(
            &chunk::chunk_key(1, 99, base, base + 1),
            bytes.clone().into(),
        )
        .await
        .expect("plant");
    f.store
        .put(&chunk::chunk_key(1, 98, base, end), bytes.into())
        .await
        .expect("plant 2");
    let dst = tempfile::tempdir().expect("dst");
    let r = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(dst.path()))
        .await
        .expect("restore with forks planted");
    assert_matches(&f.store, &f.last, &r).await;

    // A hand-built manifest violating the ordering rules: BadManifest, nothing written.
    let bad_remote = tempfile::tempdir().expect("bad");
    let bad = local_store(bad_remote.path(), true);
    let mut m = f.last.clone();
    m.cut[0].verify_offset = m.cut[0].cut_offset + 1;
    bad.put(manifest::CURRENT_KEY, manifest::encode(&m).into())
        .await
        .expect("put");
    let dst = tempfile::tempdir().expect("dst");
    let data = dst.path().join("data");
    let e = restore(RestoreSource::Store(Arc::clone(&bad)), opts(&data))
        .await
        .expect_err("bad manifest");
    assert!(
        matches!(&e, RestoreError::BadManifest(m) if m.contains("partition 0")),
        "{e}"
    );
    assert!(!data.exists(), "nothing written beyond the refusal");
    let mut m = f.last.clone();
    m.cut[2].snapshot_offset = Some(m.cut[2].cut_offset + 5);
    bad.put(manifest::CURRENT_KEY, manifest::encode(&m).into())
        .await
        .expect("put");
    let e = restore(RestoreSource::Store(bad), opts(&data))
        .await
        .expect_err("bad manifest 2");
    assert!(
        matches!(&e, RestoreError::BadManifest(m) if m.contains("partition 2")),
        "{e}"
    );
    assert!(!data.exists());

    // Tamper one byte of a chunk on partition 3: a chain error naming the partition.
    let e3 = f.last.partition(3).expect("p3").clone();
    let key = e3.last_chunk_key.clone().expect("p3 head");
    let path = f.remote_path.join(&key);
    let orig = std::fs::read(&path).expect("chunk");
    let mut t = orig.clone();
    let mid = t.len() / 2;
    t[mid] ^= 0x40;
    std::fs::write(&path, &t).expect("tamper");
    let dst = tempfile::tempdir().expect("dst");
    let e = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(dst.path()))
        .await
        .expect_err("tampered chunk");
    assert!(
        matches!(&e, RestoreError::Chain { partition: 3, .. }),
        "{e}"
    );
    std::fs::write(&path, &orig).expect("untamper");

    // Tamper a snapshot: sha error naming the partition.
    let e2 = f.last.partition(2).expect("p2").clone();
    let key = e2.snapshot_key.clone().expect("p2 snapshot");
    let path = f.remote_path.join(&key);
    let mut t = std::fs::read(&path).expect("snapshot");
    let mid = t.len() / 2;
    t[mid] ^= 0x01;
    std::fs::write(&path, &t).expect("tamper");
    let dst = tempfile::tempdir().expect("dst");
    let e = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(dst.path()))
        .await
        .expect_err("tampered snapshot");
    assert!(
        matches!(&e, RestoreError::SnapshotSha { partition: 2, .. }),
        "{e}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_partition_and_chain_starting_at_a_snapshot() {
    let src = tempfile::tempdir().expect("src");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(src.path(), P)).await;
    for p in 0..P {
        start_runs(&node, p, 20).await;
        node.snapshot_now(PartitionId(p)).await.expect("snap");
    }
    // Partitions 0..2 keep writing; partition 3 is idle since its snapshot.
    for p in 0..3 {
        start_runs(&node, p, 7).await;
    }
    let up = uploader(&node, up_cfg());
    let m = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("manifest");
    let idle = m.partition(3).expect("p3");
    assert_eq!(idle.snapshot_offset, Some(idle.cut_offset), "{idle:?}");
    assert_eq!(idle.verify_offset, idle.cut_offset);
    for p in 0..3 {
        let e = m.partition(p).expect("entry");
        assert!(
            e.chain_start > 0,
            "chain starts at the snapshot (case b): {e:?}"
        );
        assert_eq!(Some(e.chain_start), e.snapshot_offset);
    }
    drop(up);
    shutdown(node).await;

    let dst = tempfile::tempdir().expect("dst");
    let r = restore(RestoreSource::Store(Arc::clone(&store)), opts(dst.path()))
        .await
        .expect("restore");
    assert_matches(&store, &m, &r).await;
    assert_eq!(r.partitions[3].envelopes, 0, "idle partition: zero batches");
    assert!(r.meta_index.is_none());
    // The restored digests equal the stopped source's.
    assert_eq!(
        r.partitions
            .iter()
            .map(|p| p.digest.clone())
            .collect::<Vec<_>>(),
        inspect_digests(src.path()).await
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tar_round_trips_online_and_offline() {
    let src = tempfile::tempdir().expect("src");
    let out = tempfile::tempdir().expect("out");
    let meta = MetaHandle::start(&src.path().join("meta"), 1)
        .await
        .expect("meta");
    meta.init_cluster("tar-test", 1, P, 1, [5; 32])
        .await
        .expect("cluster");
    let node = open(node_cfg(src.path(), P)).await;
    workload(&node, 6).await;
    settle(&node).await;
    for p in 0..P {
        node.snapshot_now(PartitionId(p)).await.expect("snap");
    }
    workload(&node, 3).await;
    settle(&node).await;

    // Online: one round of the running node into a temp store, packed.
    let f_tar = out.path().join("f.tar");
    let rep = backup_node_to_tar(
        Arc::clone(&node),
        Some(meta.view().clone()),
        Arc::new(SystemClock),
        1,
        false,
        &f_tar,
    )
    .await
    .expect("online backup");
    assert!(rep.entries > 0 && rep.bytes > 0);
    assert_eq!(rep.manifest.cluster_id, "tar-test");
    shutdown(node).await;
    meta.stop().await;

    // The tar holds the LocalFs layout; restore from it and from its unpacked copy agree.
    let unpacked = tempfile::tempdir().expect("unpacked");
    let n = unpack(std::fs::File::open(&f_tar).expect("open"), unpacked.path()).expect("unpack");
    assert_eq!(n, rep.entries);
    let ustore = local_store(unpacked.path(), true);
    assert!(!keys(&ustore, "manifest/").await.is_empty());
    let d1 = tempfile::tempdir().expect("d1");
    let r1 = restore(RestoreSource::Tar(f_tar.clone()), opts(d1.path()))
        .await
        .expect("restore from tar");
    assert_matches(&ustore, &rep.manifest, &r1).await;
    let d2 = tempfile::tempdir().expect("d2");
    let r2 = restore(RestoreSource::Store(Arc::clone(&ustore)), opts(d2.path()))
        .await
        .expect("restore from the unpacked dir");
    let digests = |r: &RestoreReport| {
        r.partitions
            .iter()
            .map(|p| p.digest.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(digests(&r1), digests(&r2), "tar and LocalFs restores agree");

    // Offline: the stopped source directory, Inspect mode.
    let g_tar = out.path().join("g.tar");
    let before = inspect_digests(src.path()).await;
    let rep = backup_offline(src.path(), 1, Arc::new(SystemClock), &g_tar)
        .await
        .expect("offline backup");
    assert_eq!(rep.manifest.cluster_id, "tar-test");
    let d3 = tempfile::tempdir().expect("d3");
    let r3 = restore(RestoreSource::Tar(g_tar), opts(d3.path()))
        .await
        .expect("restore offline tar");
    assert_eq!(
        digests(&r3),
        before,
        "offline backup restores the stopped state"
    );
    assert_eq!(
        inspect_digests(src.path()).await,
        before,
        "the source is untouched"
    );
}

#[test]
fn tar_refuses_unsafe_entries() {
    // pack_dir + unpack round trip.
    let a = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(a.path().join("x/y")).unwrap();
    std::fs::write(a.path().join("x/y/z"), b"zz").unwrap();
    std::fs::write(a.path().join("top"), b"t").unwrap();
    std::fs::create_dir_all(a.path().join(".ostro-tmp")).unwrap();
    std::fs::write(a.path().join(".ostro-tmp/inflight"), b"no").unwrap();
    let mut buf = Vec::new();
    assert_eq!(pack_dir(a.path(), &mut buf).unwrap(), 2);
    let b = tempfile::tempdir().unwrap();
    assert_eq!(unpack(&buf[..], b.path()).unwrap(), 2);
    assert_eq!(std::fs::read(b.path().join("x/y/z")).unwrap(), b"zz");
    assert!(!b.path().join(".ostro-tmp").exists());

    // `..`, absolute paths and links are refused.
    for (name, kind) in [
        ("../evil", tar::EntryType::Regular),
        ("ok/../../evil", tar::EntryType::Regular),
        ("link", tar::EntryType::Symlink),
    ] {
        let mut raw = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut raw);
            let mut h = tar::Header::new_gnu();
            h.set_entry_type(kind);
            h.set_size(1);
            // set_path refuses `..`; write the name bytes directly.
            let gnu = h.as_gnu_mut().unwrap();
            gnu.name[..name.len()].copy_from_slice(name.as_bytes());
            h.set_cksum();
            builder.append(&h, &b"x"[..]).unwrap();
            builder.finish().unwrap();
        }
        let c = tempfile::tempdir().unwrap();
        let e = unpack(&raw[..], c.path()).unwrap_err();
        assert!(e.to_string().contains("not allowed"), "{name}: {e}");
    }
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupted_restore_leaves_a_directory_the_next_restore_refuses() {
    let f = fixture().await;
    for fp in ["restore::after_meta", "restore::mid_partition"] {
        let scenario = fail::FailScenario::setup();
        fail::cfg(fp, "return").expect("cfg");
        let dst = tempfile::tempdir().expect("dst");
        let e = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(dst.path()))
            .await
            .expect_err("injected");
        assert!(
            matches!(e, RestoreError::Injected(n) if n == fp),
            "{fp}: {e}"
        );
        scenario.teardown();
        let e = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(dst.path()))
            .await
            .expect_err("refused");
        assert!(matches!(e, RestoreError::NotEmpty(_)), "{fp}: {e}");
        assert!(e.to_string().contains("delete the directory and retry"));
        // Deleting and retrying works.
        std::fs::remove_dir_all(dst.path()).expect("delete");
        let r = restore(RestoreSource::Store(Arc::clone(&f.store)), opts(dst.path()))
            .await
            .expect("retry");
        assert_matches(&f.store, &f.last, &r).await;
    }
}

/// Restore timing at P=16 with 10k runs on LocalFs (reported in the 08-18 SUMMARY; run with
/// `--ignored --nocapture`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "timing report"]
async fn restore_timing_p16_10k_runs() {
    const P16: u16 = 16;
    let src = tempfile::tempdir().expect("src");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let mut cfg = node_cfg(src.path(), P16);
    cfg.wal.rotation_size_bytes = 64 * 1024 * 1024;
    let node = open(cfg).await;
    // 10k runs: 9k before the snapshots, 1k in the tail.
    for p in 0..P16 {
        for _ in 0..9 {
            start_runs(&node, p, 9000 / 16 / 9).await;
        }
        node.snapshot_now(PartitionId(p)).await.expect("snapshot");
    }
    for p in 0..P16 {
        start_runs(&node, p, 1000 / 16).await;
    }
    let up = uploader(&node, up_cfg());
    let t = std::time::Instant::now();
    let m = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("manifest");
    println!("upload round: {:?}", t.elapsed());
    drop(up);
    shutdown(node).await;
    let dst = tempfile::tempdir().expect("dst");
    let mut o = opts(dst.path());
    o.parallelism = 4;
    let r = restore(RestoreSource::Store(Arc::clone(&store)), o)
        .await
        .expect("restore");
    let runs: u64 = m.cut.iter().map(|e| e.cut_offset).sum();
    println!(
        "restore P=16, ~{runs} commands: {}",
        serde_json::to_string(&r.durations).expect("json")
    );
}
