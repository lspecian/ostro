//! Takeover reconcile (plan 12-15, CLUS-02 "divergent tails truncated, including in object
//! storage"; research Pattern 7): a new leader repairs a divergent remote tail before its first
//! upload. A whole divergent chunk is dropped and a straddling chunk is re-cut to its
//! consistent prefix. A crash before the head PUT leaves the old head valid, and a re-run
//! converges. A consistent chain is left alone. A gap below the new leader's `log_start` is
//! re-chained from a remote snapshot (W4). Restore refuses a cluster bucket (D-P12-3).
//!
//! The log-level cases use real `PartitionLog`s whose epoch histories are built batch by
//! batch, so the old and new leader share a byte-identical prefix and differ after it. The gap
//! case uses real `Node`s and the uploader.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use common::{keys, local_store, node_cfg, open, shutdown, start_runs, up_cfg};
use ostro_backup::chunk;
use ostro_backup::cluster::{self, Head, Takeover, TakeoverOutcome};
use ostro_backup::restore::{self, RestoreError, RestoreOptions, RestoreSource};
use ostro_backup::uploader::{Uploader, UploaderClusterDeps};
use ostro_log::{LogNode, PartitionLog, ReplicaSet, ReplicatedLog as _};
use ostro_objstore::ObjectStore;
use ostro_partition::roles::{PartitionRoles, open_partition_log_for_tests};
use ostro_partition::{Node, SystemClock};
use ostro_types::{Epoch, Offset, PartitionId};
use tokio::sync::watch;

const P0: PartitionId = PartitionId(0);

/// A log built as `[(epoch, [command per batch])]`: one append per command, each committed.
async fn build_log(
    dir: &Path,
    floor: u64,
    plan: &[(u32, &[&str])],
) -> (LogNode, Arc<PartitionLog>) {
    let (node, log) = open_partition_log_for_tests(dir, P0, 1, Offset(floor))
        .await
        .expect("open log");
    for (epoch, cmds) in plan {
        log.become_leader(Epoch(*epoch), ReplicaSet::solo(1))
            .expect("lead");
        for c in *cmds {
            let r = log
                .append(Epoch(*epoch), vec![Bytes::from(c.to_string())])
                .await
                .expect("append");
            log.wait_committed(r.last).await.expect("committed");
        }
    }
    (node, log)
}

fn hw(log: &PartitionLog) -> u64 {
    log.high_watermark().borrow().0
}

/// The stored envelopes of `log` in `[from, to)`.
async fn envelopes(log: &PartitionLog, from: u64, to: u64) -> (u32, Vec<Vec<u8>>) {
    let mut out = Vec::new();
    let mut epoch = None;
    let mut at = from;
    while at < to {
        let batches = log.read_envelopes(Offset(at), 1 << 20).await.expect("read");
        assert!(!batches.is_empty(), "log ends at {at} before {to}");
        for b in batches {
            for env in b.envelopes {
                let (_, end) = chunk::envelope_span(&env).expect("span");
                if end > to {
                    return (epoch.expect("epoch"), out);
                }
                epoch.get_or_insert(b.epoch);
                out.push(env.to_vec());
                at = end;
            }
        }
    }
    (epoch.expect("epoch"), out)
}

/// Upload `[base, end)` of `log` as one chunk linked to `prev` (what an old leader did).
async fn put_chunk(
    store: &Arc<dyn ObjectStore>,
    log: &PartitionLog,
    base: u64,
    end: u64,
    prev: [u8; 32],
) -> (String, [u8; 32]) {
    let (epoch, envs) = envelopes(log, base, end).await;
    let refs: Vec<&[u8]> = envs.iter().map(Vec::as_slice).collect();
    let (h, bytes) = chunk::encode(0, epoch, prev, &refs).expect("encode");
    assert_eq!((h.base, h.end), (base, end));
    let key = chunk::chunk_key(0, epoch, base, end);
    let sha = chunk::chunk_sha(&bytes);
    store.put(&key, bytes.into()).await.expect("put chunk");
    (key, sha)
}

/// The old leader's remote state: chunks over `cuts` of `log`, head at `epoch`.
async fn upload_chain(
    store: &Arc<dyn ObjectStore>,
    log: &PartitionLog,
    cuts: &[(u64, u64)],
    epoch: u32,
) -> Vec<(String, [u8; 32])> {
    let mut prev = [0u8; 32];
    let mut out = Vec::new();
    for (b, e) in cuts {
        let (k, s) = put_chunk(store, log, *b, *e, prev).await;
        prev = s;
        out.push((k, s));
    }
    let (k, s) = out.last().cloned().expect("a chunk");
    let head = Head {
        epoch,
        last_chunk_key: Some(k),
        last_chunk_sha256: Some(s),
        chain_start: cuts[0].0,
        end: cuts.last().expect("cut").1,
        snapshot: None,
        node_id: 1,
        members: 2,
    };
    assert!(matches!(
        cluster::put_head(store, 0, &head, None)
            .await
            .expect("head"),
        cluster::HeadPut::Written(_)
    ));
    out
}

fn takeover<'a>(
    store: &'a Arc<dyn ObjectStore>,
    log: &'a PartitionLog,
    epoch: u32,
) -> Takeover<'a, PartitionLog> {
    Takeover {
        store,
        partition: 0,
        epoch,
        node_id: 2,
        members: 2,
        log,
        snapshots: &[],
        stop_before_head: false,
    }
}

async fn head(store: &Arc<dyn ObjectStore>) -> Head {
    cluster::read_head(store, 0)
        .await
        .expect("read")
        .expect("head")
        .0
}

/// Minimal 12-03 objstore oracle: the chain reachable from the head equals `log`'s committed
/// log over the same range, byte for byte (chain ⊆ committed prefix). Returns the chain.
async fn assert_chain_is_committed(
    store: &Arc<dyn ObjectStore>,
    log: &PartitionLog,
) -> Vec<chunk::ChunkRef> {
    let h = head(store).await;
    let Some(k) = &h.last_chunk_key else {
        return Vec::new();
    };
    let chain = chunk::walk_back(store, k, h.last_chunk_sha256.expect("sha"), h.chain_start)
        .await
        .expect("chain verifies");
    let (b, e) = (
        chain[0].chunk.header.base,
        chain.last().expect("chunk").chunk.header.end,
    );
    assert_eq!(b, h.chain_start);
    assert_eq!(e, h.end);
    assert!(e <= hw(log), "chain end {e} above HW {}", hw(log));
    let remote: Vec<Vec<u8>> = chain
        .iter()
        .flat_map(|c| c.chunk.envelopes.iter().cloned())
        .collect();
    let (_, local) = envelopes(log, b, e).await;
    assert_eq!(remote, local, "remote chain = committed log [{b}, {e})");
    chain
}

const COMMON: &[&str] = &["a0", "a1", "a2", "a3", "a4"];
const E2_OLD: &[&str] = &["b5", "b6", "x7", "x8", "x9"];
const E2_NEW: &[&str] = &["b5", "b6"];
const E3_NEW: &[&str] = &["c7", "c8"];

/// Old leader: epoch 1 [0,5), epoch 2 [5,10). New leader: epoch 1 [0,5), epoch 2 [5,7) (the
/// same bytes), epoch 3 [7,9). Its history ends epoch 2 at 7.
async fn divergent_pair(
    dir: &Path,
) -> ((LogNode, Arc<PartitionLog>), (LogNode, Arc<PartitionLog>)) {
    let a = build_log(&dir.join("a"), 0, &[(1, COMMON), (2, E2_OLD)]).await;
    let b = build_log(&dir.join("b"), 0, &[(1, COMMON), (2, E2_NEW), (3, E3_NEW)]).await;
    assert_eq!(b.1.epochs().end_offset_for_epoch(Epoch(2)), Some(Offset(7)));
    (a, b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_whole_divergent_chunk_is_dropped_after_the_head_moves() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let ((_na, a), (_nb, b)) = divergent_pair(dir.path()).await;
    let old = upload_chain(&store, &a, &[(0, 5), (5, 7), (7, 10)], 2).await;

    let out = cluster::reconcile_on_takeover(takeover(&store, &b, 3))
        .await
        .expect("reconcile");
    let TakeoverOutcome::Ready(r) = out else {
        panic!("expected Ready, got {out:?}");
    };
    assert_eq!(r.repaired_at, Some(7), "first divergent offset");
    assert_eq!(r.recut, None, "the chunk at 7 is wholly divergent");
    assert_eq!(r.deleted, vec![old[2].0.clone()]);
    let h = head(&store).await;
    assert_eq!(h.epoch, 3);
    assert_eq!(h.last_chunk_key.as_deref(), Some(old[1].0.as_str()));
    assert_eq!(h.end, 7);
    assert!(!keys(&store, "p/0000/seg/").await.contains(&old[2].0));
    assert_eq!(assert_chain_is_committed(&store, &b).await.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_straddling_chunk_is_recut_to_its_consistent_prefix() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let ((_na, a), (_nb, b)) = divergent_pair(dir.path()).await;
    let old = upload_chain(&store, &a, &[(0, 5), (5, 10)], 2).await;

    let TakeoverOutcome::Ready(r) = cluster::reconcile_on_takeover(takeover(&store, &b, 3))
        .await
        .expect("reconcile")
    else {
        panic!("expected Ready");
    };
    let recut = chunk::chunk_key(0, 2, 5, 7);
    assert_eq!(r.repaired_at, Some(7));
    assert_eq!(
        r.recut.as_deref(),
        Some(recut.as_str()),
        "straddling chunk re-cut"
    );
    assert_eq!(r.deleted, vec![old[1].0.clone()]);
    let h = head(&store).await;
    assert_eq!(h.last_chunk_key.as_deref(), Some(recut.as_str()));
    assert_eq!((h.epoch, h.end, h.chain_start), (3, 7, 0));
    let chain = assert_chain_is_committed(&store, &b).await;
    assert_eq!(chain.len(), 2);
    assert_eq!(
        chain[1].chunk.header.prev_chunk_sha256, old[0].1,
        "the re-cut chunk links where the original did"
    );
    let segs = keys(&store, "p/0000/seg/").await;
    assert!(segs.contains(&recut) && !segs.contains(&old[1].0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_before_the_head_put_keeps_the_old_head_and_a_rerun_converges() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let ((_na, a), (_nb, b)) = divergent_pair(dir.path()).await;
    let old = upload_chain(&store, &a, &[(0, 5), (5, 10)], 2).await;
    let before = head(&store).await;

    let mut t = takeover(&store, &b, 3);
    t.stop_before_head = true;
    assert_eq!(
        cluster::reconcile_on_takeover(t).await.expect("reconcile"),
        TakeoverOutcome::Interrupted
    );
    // The re-cut chunk is there, the old head (and its whole chain) still valid.
    let recut = chunk::chunk_key(0, 2, 5, 7);
    let segs = keys(&store, "p/0000/seg/").await;
    assert!(segs.contains(&recut) && segs.contains(&old[1].0));
    assert_eq!(head(&store).await, before);
    let chain = chunk::walk_back(
        &store,
        before.last_chunk_key.as_deref().expect("key"),
        before.last_chunk_sha256.expect("sha"),
        0,
    )
    .await
    .expect("old chain still walks");
    assert_eq!(chain.len(), 2);

    // The re-run (the new leader restarting its takeover) converges.
    let TakeoverOutcome::Ready(r) = cluster::reconcile_on_takeover(takeover(&store, &b, 3))
        .await
        .expect("reconcile")
    else {
        panic!("expected Ready");
    };
    assert_eq!(r.recut.as_deref(), Some(recut.as_str()));
    assert_eq!(
        head(&store).await.last_chunk_key.as_deref(),
        Some(recut.as_str())
    );
    assert_chain_is_committed(&store, &b).await;
    assert!(!keys(&store, "p/0000/seg/").await.contains(&old[1].0));

    // And once more: nothing left to repair.
    let TakeoverOutcome::Ready(r) = cluster::reconcile_on_takeover(takeover(&store, &b, 3))
        .await
        .expect("reconcile")
    else {
        panic!("expected Ready");
    };
    assert!(!r.wrote());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_consistent_chain_is_not_rewritten() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let (_na, a) = build_log(&dir.path().join("a"), 0, &[(1, COMMON), (2, E2_NEW)]).await;
    // The new leader has everything the old one uploaded, and more.
    let (_nb, b) = build_log(
        &dir.path().join("b"),
        0,
        &[(1, COMMON), (2, E2_NEW), (3, E3_NEW)],
    )
    .await;
    upload_chain(&store, &a, &[(0, 3), (3, 5), (5, 7)], 2).await;
    let segs = keys(&store, "p/0000/seg/").await;
    let (head_bytes, etag) = store
        .get(&cluster::head_key(0))
        .await
        .expect("get")
        .map(|(b, m)| (b, m.e_tag))
        .expect("head");

    let TakeoverOutcome::Ready(r) = cluster::reconcile_on_takeover(takeover(&store, &b, 3))
        .await
        .expect("reconcile")
    else {
        panic!("expected Ready");
    };
    assert!(!r.wrote() && r.deleted.is_empty());
    assert_eq!(r.chain.len(), 3);
    assert_eq!(r.chain.last().expect("link").end, 7);
    let (now, now_etag) = store
        .get(&cluster::head_key(0))
        .await
        .expect("get")
        .map(|(b, m)| (b, m.e_tag))
        .expect("head");
    assert_eq!((now, now_etag), (head_bytes, etag), "head not written");
    assert_eq!(keys(&store, "p/0000/seg/").await, segs);
    assert_chain_is_committed(&store, &b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_epoch_bytes_that_differ_are_divergent_too() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    // Same epochs and offsets, other bytes from offset 2 (never possible under the ISR rules,
    // but the byte check must still catch it).
    let (_na, a) = build_log(&dir.path().join("a"), 0, &[(1, COMMON)]).await;
    let (_nb, b) = build_log(
        &dir.path().join("b"),
        0,
        &[(1, &["a0", "a1", "z2", "z3", "z4"])],
    )
    .await;
    let old = upload_chain(&store, &a, &[(0, 5)], 1).await;
    let TakeoverOutcome::Ready(r) = cluster::reconcile_on_takeover(takeover(&store, &b, 2))
        .await
        .expect("reconcile")
    else {
        panic!("expected Ready");
    };
    assert_eq!(r.repaired_at, Some(2));
    assert_eq!(
        r.recut.as_deref(),
        Some(chunk::chunk_key(0, 1, 0, 2).as_str())
    );
    assert_eq!(r.deleted, vec![old[0].0.clone()]);
    assert_eq!(assert_chain_is_committed(&store, &b).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_higher_epoch_head_fences_the_takeover() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let ((_na, a), (_nb, b)) = divergent_pair(dir.path()).await;
    upload_chain(&store, &a, &[(0, 5), (5, 10)], 5).await;
    let before = head(&store).await;
    let segs = keys(&store, "p/0000/seg/").await;
    assert_eq!(
        cluster::reconcile_on_takeover(takeover(&store, &b, 3))
            .await
            .expect("reconcile"),
        TakeoverOutcome::Fenced { head_epoch: 5 }
    );
    assert_eq!(head(&store).await, before);
    assert_eq!(keys(&store, "p/0000/seg/").await, segs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_without_a_local_snapshot_is_reported_not_papered_over() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let (_na, a) = build_log(&dir.path().join("a"), 0, &[(1, COMMON), (2, E2_OLD)]).await;
    // The new leader's log starts at 20 (trimmed by its own snapshot floor).
    let (_nb, b) = build_log(&dir.path().join("b"), 20, &[(3, E3_NEW)]).await;
    assert_eq!(b.log_start(), Offset(20));
    upload_chain(&store, &a, &[(0, 5), (5, 10)], 2).await;
    let before = head(&store).await;
    assert_eq!(
        cluster::reconcile_on_takeover(takeover(&store, &b, 3))
            .await
            .expect("reconcile"),
        TakeoverOutcome::NoSnapshot {
            log_start: 20,
            tail_end: 10
        }
    );
    assert_eq!(head(&store).await, before, "nothing written without a base");
}

// ---------------------------------------------------------------- W4 gap, end to end

type Led = Arc<Vec<(PartitionId, Epoch)>>;

struct FakeRoles(watch::Sender<Led>);

impl PartitionRoles for FakeRoles {
    fn led(&self) -> watch::Receiver<Led> {
        self.0.subscribe()
    }
}

fn cluster_uploader(node: &Arc<Node>, node_id: u64, led: &[(u16, u32)]) -> Arc<Uploader> {
    let (tx, _) = watch::channel(Arc::new(
        led.iter()
            .map(|(p, e)| (PartitionId(*p), Epoch(*e)))
            .collect(),
    ));
    // The receiver keeps the last value after the sender is dropped.
    let (_, members) = watch::channel(2usize);
    let mut cfg = up_cfg();
    cfg.node_id = node_id;
    Arc::new(
        Uploader::new(Arc::clone(node), None, Arc::new(SystemClock), cfg).with_cluster(
            UploaderClusterDeps {
                roles: Arc::new(FakeRoles(tx)) as Arc<dyn PartitionRoles>,
                members,
            },
        ),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_above_the_remote_tail_rechains_from_a_snapshot() {
    let dir_a = tempfile::tempdir().expect("tempdir");
    let dir_b = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);

    // A leads p0 at epoch 2 and uploads a short chain from 0.
    let a = open(node_cfg(dir_a.path(), 1)).await;
    start_runs(&a, 0, 10).await;
    let up_a = cluster_uploader(&a, 1, &[(0, 2)]);
    up_a.round(&store).await.expect("A round");
    let a_head = head(&store).await;
    let a_segs = keys(&store, "p/0000/seg/").await;
    assert_eq!((a_head.chain_start, a_head.epoch), (0, 2));

    // B (a follower while A led) trimmed its log by its own snapshot floor, far above A's
    // remote tail.
    let mut cfg = node_cfg(dir_b.path(), 1);
    cfg.node_id = 2;
    cfg.backup_enabled = false;
    let b = open(cfg).await;
    for _ in 0..3 {
        start_runs(&b, 0, 60).await;
        b.snapshot_now(P0).await.expect("snap").expect("new");
    }
    start_runs(&b, 0, 10).await;
    let log_start = b.retention_pass(P0).await.expect("pass");
    shutdown(b).await;
    assert!(
        log_start.0 > a_head.end,
        "B's log_start {log_start} above the remote tail {}",
        a_head.end
    );
    let mut cfg = node_cfg(dir_b.path(), 1);
    cfg.node_id = 2;
    let b = open(cfg).await;
    let newest = b
        .local_snapshots(P0)
        .await
        .last()
        .expect("snapshot")
        .applied_offset;

    // B takes p0 over at epoch 3: re-chain from its snapshot, then upload the tail.
    let up_b = cluster_uploader(&b, 2, &[(0, 3)]);
    up_b.round(&store).await.expect("B round");
    let h = head(&store).await;
    assert_eq!((h.epoch, h.node_id), (3, 2));
    assert_eq!(h.chain_start, newest.0, "new chain starts at B's snapshot");
    let snap = h.snapshot.as_ref().expect("head references the snapshot");
    assert_eq!(snap.offset, newest.0);
    assert!(
        store.get(&snap.key).await.expect("get").is_some(),
        "snapshot uploaded"
    );
    let segs = keys(&store, "p/0000/seg/").await;
    assert!(
        a_segs.iter().all(|k| !segs.contains(k)),
        "the old, unreachable chain was deleted after the head moved"
    );
    // The chain from the snapshot equals B's committed log [S, HW), and its first chunk links
    // to nothing (the 08-13 start rule).
    let key = h.last_chunk_key.as_deref().expect("tail uploaded");
    let chain = chunk::walk_back(
        &store,
        key,
        h.last_chunk_sha256.expect("sha"),
        h.chain_start,
    )
    .await
    .expect("walks");
    assert_eq!(chain[0].chunk.header.base, newest.0);
    assert_eq!(chain[0].chunk.header.prev_chunk_sha256, [0; 32]);
    let end = chain.last().expect("chunk").chunk.header.end;
    assert_eq!(end, b.log_cut()[0].0, "chain reaches B's HW");
    let remote_envs: Vec<Vec<u8>> = chain
        .iter()
        .flat_map(|c| c.chunk.envelopes.iter().cloned())
        .collect();
    let mut local = Vec::new();
    let mut at = newest.0;
    while at < end {
        for batch in b
            .read_envelopes(P0, Offset(at), 1 << 20)
            .await
            .expect("read")
        {
            for env in batch.envelopes {
                at = chunk::envelope_span(&env).expect("span").1;
                local.push(env.to_vec());
            }
        }
    }
    assert_eq!(
        remote_envs, local,
        "reachable chain from the snapshot = B's committed log"
    );

    drop((up_a, up_b));
    shutdown(a).await;
    shutdown(b).await;
}

// ---------------------------------------------------------------- restore refusal

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restore_refuses_a_multi_member_cluster_bucket() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let (_na, a) = build_log(&dir.path().join("a"), 0, &[(1, COMMON)]).await;
    upload_chain(&store, &a, &[(0, 5)], 1).await;
    let err = restore::restore(
        RestoreSource::Store(Arc::clone(&store)),
        RestoreOptions::new(dir.path().join("restored")),
    )
    .await
    .expect_err("cluster bucket refused");
    assert!(
        matches!(err, RestoreError::ClusterBucketUnsupported(_)),
        "got {err:?}"
    );
    assert!(
        err.to_string().contains(
            "no metadata snapshot in the bucket; the metadata leader uploads one in 1.0+"
        ),
        "{err}"
    );
    assert!(!dir.path().join("restored").join("state").exists());
}
