//! 14-29 Task 1 (D-P14-45): metadata snapshots and the term-fenced `meta/head` pointer of a
//! multi-member cluster bucket.
//!
//! Real `Node`s (one per fake member) share one LocalFs bucket; metadata leadership is a
//! `watch::Receiver<Option<u64>>` each test drives (`Uploader::with_meta_leader`), and each
//! member's metadata state is a `MetaSm` the test advances.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;

use common::{keys, local_store, node_cfg, open, shutdown, start_runs, up_cfg};
use ostro_backup::chunk;
use ostro_backup::meta_head::{self, META_HEAD_KEY, MetaHead, MetaHeadPut};
use ostro_backup::uploader::{Uploader, UploaderClusterDeps, meta_key};
use ostro_meta::{MetaCommand, MetaSm, MetaState, MetaView, StateMachine as _};
use ostro_objstore::ObjectStore;
use ostro_partition::roles::PartitionRoles;
use ostro_partition::{Node, SystemClock};
use ostro_types::{Epoch, PartitionId};
use tokio::sync::watch;

type Led = Arc<Vec<(PartitionId, Epoch)>>;

struct FakeRoles(watch::Sender<Led>);

impl PartitionRoles for FakeRoles {
    fn led(&self) -> watch::Receiver<Led> {
        self.0.subscribe()
    }
}

fn roles(led: &[(u16, u32)]) -> Arc<FakeRoles> {
    let (tx, _) = watch::channel(Arc::new(
        led.iter()
            .map(|(p, e)| (PartitionId(*p), Epoch(*e)))
            .collect(),
    ));
    Arc::new(FakeRoles(tx))
}

/// A member's metadata: a state machine the test advances, and its view.
struct Meta {
    sm: MetaSm,
    view: MetaView,
    state: MetaState,
}

impl Meta {
    fn new() -> Self {
        let (sm, view) = MetaSm::new();
        let mut m = Self {
            sm,
            view,
            state: MetaState::default(),
        };
        m.state.apply(
            1,
            MetaCommand::InitCluster {
                cluster_id: "test-cluster".into(),
                node_id: 1,
                partitions: 1,
                created_at_ms: 1,
                token_key: [7; 32],
            },
        );
        m.set(1);
        m
    }

    /// Move the applied index to `index` (same state: an applied no-op entry).
    fn set(&mut self, index: u64) {
        self.sm
            .restore(index, &ostro_meta::encode_snapshot(&self.state))
            .expect("restore");
    }
}

struct Member {
    up: Arc<Uploader>,
    leader: watch::Sender<Option<u64>>,
    _members: watch::Sender<usize>,
}

fn member(
    node: &Arc<Node>,
    node_id: u64,
    meta: &Meta,
    led: &[(u16, u32)],
    members: usize,
    term: Option<u64>,
    keep: usize,
) -> Member {
    let mut cfg = up_cfg();
    cfg.node_id = node_id;
    cfg.remote_snapshots_keep = keep;
    let (mtx, mrx) = watch::channel(members);
    let (ltx, lrx) = watch::channel(term);
    let up = Uploader::new(
        Arc::clone(node),
        Some(meta.view.clone()),
        Arc::new(SystemClock),
        cfg,
    )
    .with_cluster(UploaderClusterDeps {
        roles: roles(led) as Arc<dyn PartitionRoles>,
        members: mrx,
    })
    .with_meta_leader(lrx);
    Member {
        up: Arc::new(up),
        leader: ltx,
        _members: mtx,
    }
}

async fn head(store: &Arc<dyn ObjectStore>) -> Option<(MetaHead, Option<String>)> {
    meta_head::get(store).await.expect("get meta/head")
}

async fn snapshot_keys(store: &Arc<dyn ObjectStore>) -> Vec<String> {
    keys(store, "meta/")
        .await
        .into_iter()
        .filter(|k| meta_head::parse_meta_key(k).is_some())
        .collect()
}

fn mh(term: u64, index: u64) -> MetaHead {
    MetaHead {
        term,
        index,
        key: meta_key(index),
        sha256: [index as u8; 32],
        at_ms: 1000 + index,
        members: 3,
        partitions: 4,
    }
}

#[test]
fn codec_round_trips_and_names_the_format() {
    let h = mh(4, 118);
    let bytes = meta_head::encode(&h);
    let v: serde_json::Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(v["format"], 1);
    assert_eq!(v["term"], 4);
    assert_eq!(v["index"], 118);
    assert_eq!(v["key"], "meta/00000000000000000118.msnap");
    assert_eq!(v["sha256"].as_str().map(str::len), Some(64));
    assert_eq!(v["members"], 3);
    assert_eq!(v["partitions"], 4);
    assert_eq!(meta_head::decode(&bytes).expect("decode"), h);
    let bad = String::from_utf8(bytes)
        .expect("utf8")
        .replace("\"format\":1", "\"format\":2");
    assert!(meta_head::decode(bad.as_bytes()).is_err());
    assert_eq!(meta_head::parse_meta_key(&meta_key(42)), Some(42));
    assert_eq!(meta_head::parse_meta_key(META_HEAD_KEY), None);
}

#[test]
fn retention_victims_keep_the_newest_k_and_the_protected_key() {
    let ks: Vec<String> = (1..=6).map(meta_key).collect();
    let mut v = meta_head::retention_victims(&ks, 3, 6, None);
    v.sort();
    assert_eq!(v, vec![meta_key(1), meta_key(2), meta_key(3)]);
    let v = meta_head::retention_victims(&ks, 3, 6, Some(&meta_key(2)));
    assert!(!v.contains(&meta_key(2)));
    // Never above the head: a newer, unreferenced key of another writer is not counted.
    let v = meta_head::retention_victims(&ks, 3, 4, None);
    assert_eq!(v, vec![meta_key(1)]);
}

async fn fence_rules(conditional: bool) {
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), conditional);
    // Absent: If-None-Match.
    let MetaHeadPut::Written(e3) = meta_head::put(&store, &mh(3, 10), None).await.expect("put")
    else {
        panic!("first write");
    };
    // Same term, newer index, stale (absent) etag: re-read and retried once.
    assert!(matches!(
        meta_head::put(&store, &mh(3, 11), None).await.expect("put"),
        MetaHeadPut::Written(_)
    ));
    assert_eq!(head(&store).await.expect("head").0.index, 11);
    // Same term, older index: superseded, never moved back.
    assert!(matches!(
        meta_head::put(&store, &mh(3, 9), e3.as_deref())
            .await
            .expect("put"),
        MetaHeadPut::Superseded(..)
    ));
    // A higher term takes over.
    let (_, cur) = head(&store).await.expect("head");
    assert!(matches!(
        meta_head::put(&store, &mh(4, 5), cur.as_deref())
            .await
            .expect("put"),
        MetaHeadPut::Written(_)
    ));
    // The lower term is fenced, whatever its index (absent or stale etag).
    for etag in [None, e3.clone()] {
        assert!(matches!(
            meta_head::put(&store, &mh(3, 1000), etag.as_deref())
                .await
                .expect("put"),
            MetaHeadPut::Fenced(h) if h.term == 4
        ));
    }
    let h = head(&store).await.expect("head").0;
    assert_eq!((h.term, h.index), (4, 5));
}

#[tokio::test]
async fn put_is_fenced_by_term_with_conditional_put() {
    fence_rules(true).await;
}

#[tokio::test]
async fn put_is_fenced_by_term_without_conditional_put() {
    fence_rules(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_leaders_end_at_the_higher_term_and_the_lower_one_stops() {
    let (da, db) = (
        tempfile::tempdir().expect("a"),
        tempfile::tempdir().expect("b"),
    );
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let (na, nb) = (
        open(node_cfg(da.path(), 1)).await,
        open(node_cfg(db.path(), 1)).await,
    );
    let (mut ma, mut mb) = (Meta::new(), Meta::new());
    ma.set(10);
    mb.set(12);
    let a = member(&na, 1, &ma, &[], 3, Some(3), 3);
    let b = member(&nb, 2, &mb, &[], 3, Some(4), 3);

    let ra = a.up.round(&store).await.expect("a round");
    let h = ra.meta_head.expect("a moved meta/head");
    assert_eq!((h.term, h.index), (3, 10));
    let rb = b.up.round(&store).await.expect("b round");
    assert_eq!(rb.meta_head.map(|h| (h.term, h.index)), Some((4, 12)));
    // A's index moves on: it re-reads, sees term 4 and is fenced.
    ma.set(20);
    let ra = a.up.round(&store).await.expect("a round");
    assert!(ra.meta_fenced, "{ra:?}");
    assert!(ra.meta_head.is_none());
    assert_eq!(a.up.meta_fenced(), Some(3));
    // ...and stops moving it: later rounds at term 3 do nothing.
    for i in 21..24 {
        ma.set(i);
        let ra = a.up.round(&store).await.expect("a round");
        assert!(!ra.meta_snapshot && ra.meta_head.is_none() && !ra.meta_fenced);
    }
    let h = head(&store).await.expect("head").0;
    assert_eq!((h.term, h.index), (4, 12));
    // B keeps moving it.
    mb.set(30);
    let rb = b.up.round(&store).await.expect("b round");
    assert_eq!(rb.meta_head.map(|h| (h.term, h.index)), Some((4, 30)));
    assert!(
        !a.up.is_fenced() && !b.up.is_fenced(),
        "partition uploads unaffected"
    );
    drop((a, b));
    shutdown(na).await;
    shutdown(nb).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_rounds_of_two_terms_never_leave_the_lower_term() {
    for _ in 0..5 {
        let (da, db) = (
            tempfile::tempdir().expect("a"),
            tempfile::tempdir().expect("b"),
        );
        let remote = tempfile::tempdir().expect("remote");
        let store = local_store(remote.path(), true);
        let (na, nb) = (
            open(node_cfg(da.path(), 1)).await,
            open(node_cfg(db.path(), 1)).await,
        );
        let (mut ma, mut mb) = (Meta::new(), Meta::new());
        let a = member(&na, 1, &ma, &[], 3, Some(3), 3);
        let b = member(&nb, 2, &mb, &[], 3, Some(4), 3);
        for i in 2..8 {
            ma.set(i * 10);
            mb.set(i * 10 + 1);
            let (ra, rb) = tokio::join!(a.up.round(&store), b.up.round(&store));
            ra.expect("a");
            rb.expect("b");
        }
        let h = head(&store).await.expect("head").0;
        assert_eq!(h.term, 4, "{h:?}");
        assert_eq!(h.index, 71);
        drop((a, b));
        shutdown(na).await;
        shutdown(nb).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn idle_rounds_write_nothing_and_retention_keeps_k() {
    let d = tempfile::tempdir().expect("d");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(d.path(), 1)).await;
    let mut m = Meta::new();
    let a = member(&node, 1, &m, &[], 3, Some(2), 3);
    let r = a.up.round(&store).await.expect("round");
    assert!(r.meta_snapshot);
    let (h1, e1) = head(&store).await.expect("head");
    // Idle: nothing moved, nothing written.
    for _ in 0..3 {
        let r = a.up.round(&store).await.expect("idle round");
        assert!(!r.meta_snapshot && r.meta_head.is_none());
    }
    let (h2, e2) = head(&store).await.expect("head");
    assert_eq!((h1, e1), (h2, e2));
    assert_eq!(snapshot_keys(&store).await.len(), 1);
    // Six moves: exactly K = 3 snapshots stay, the newest ones, the head names the newest.
    for i in 2..=7 {
        m.set(i);
        let r = a.up.round(&store).await.expect("round");
        assert!(r.meta_snapshot);
    }
    let mut ks = snapshot_keys(&store).await;
    ks.sort();
    assert_eq!(ks, vec![meta_key(5), meta_key(6), meta_key(7)]);
    assert_eq!(head(&store).await.expect("head").0.key, meta_key(7));
    drop(a);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_decodes_to_the_leader_state_and_follows_the_partition_uploads() {
    let d = tempfile::tempdir().expect("d");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(d.path(), 1)).await;
    start_runs(&node, 0, 5).await;
    let mut m = Meta::new();
    m.set(9);
    // This member leads partition 0 too: meta comes after the partition head.
    let a = member(&node, 1, &m, &[(0, 1)], 3, Some(5), 3);
    let r = a.up.round(&store).await.expect("round");
    assert!(r.chunks > 0, "{r:?}");
    let h = r.meta_head.clone().expect("meta/head");
    assert!(h.at_ms >= r.cut_at_ms, "{h:?} vs cut {}", r.cut_at_ms);
    assert_eq!((h.term, h.index, h.members, h.partitions), (5, 9, 3, 1));
    let (stored, _) = head(&store).await.expect("head");
    assert_eq!(stored, h);
    let (bytes, _) = store.get(&h.key).await.expect("get").expect("snapshot");
    assert_eq!(chunk::chunk_sha(&bytes), h.sha256);
    let decoded = ostro_meta::decode_snapshot(&bytes).expect("decodes");
    assert_eq!(decoded, m.view.read(Clone::clone));
    assert!(decoded.cluster().is_some());
    // The partition head exists: per-partition chains are unchanged.
    assert!(
        store
            .get(&ostro_backup::cluster::head_key(0))
            .await
            .expect("get")
            .is_some()
    );
    drop(a);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leadership_flips_are_honoured_round_by_round() {
    let d = tempfile::tempdir().expect("d");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(d.path(), 1)).await;
    let mut m = Meta::new();
    m.set(3);
    let a = member(&node, 1, &m, &[], 3, Some(2), 3);
    assert_eq!(
        a.up.round(&store)
            .await
            .expect("r")
            .meta_head
            .map(|h| h.term),
        Some(2)
    );
    // Follower: the index moves but nothing is uploaded.
    a.leader.send_replace(None);
    m.set(4);
    let r = a.up.round(&store).await.expect("r");
    assert!(!r.meta_snapshot && r.meta_head.is_none());
    assert_eq!(head(&store).await.expect("head").0.index, 3);
    assert!(store.get(&meta_key(4)).await.expect("get").is_none());
    // Leader again at a higher term.
    a.leader.send_replace(Some(7));
    m.set(5);
    let h =
        a.up.round(&store)
            .await
            .expect("r")
            .meta_head
            .expect("moved");
    assert_eq!((h.term, h.index), (7, 5));
    drop(a);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_member_and_unset_leader_write_no_meta_head() {
    let d = tempfile::tempdir().expect("d");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(d.path(), 1)).await;
    start_runs(&node, 0, 3).await;
    let mut m = Meta::new();
    m.set(4);
    // Single member, leader watch set: the Phase 8 manifest names the meta snapshot.
    let a = member(&node, 1, &m, &[(0, 1)], 1, Some(1), 3);
    let r = a.up.round(&store).await.expect("round");
    assert!(r.manifest.is_some() && r.meta_snapshot);
    assert!(r.meta_head.is_none());
    assert!(
        head(&store).await.is_none(),
        "no meta/head in single-member mode"
    );
    drop(a);
    // Multi-member without with_meta_leader: no meta at all.
    let remote2 = tempfile::tempdir().expect("remote2");
    let store2 = local_store(remote2.path(), true);
    let (mtx, mrx) = watch::channel(3usize);
    let up = Uploader::new(
        Arc::clone(&node),
        Some(m.view.clone()),
        Arc::new(SystemClock),
        up_cfg(),
    )
    .with_cluster(UploaderClusterDeps {
        roles: roles(&[(0, 1)]) as Arc<dyn PartitionRoles>,
        members: mrx,
    });
    let r = up.round(&store2).await.expect("round");
    assert!(!r.meta_snapshot && r.meta_head.is_none());
    assert!(snapshot_keys(&store2).await.is_empty());
    assert!(head(&store2).await.is_none());
    drop((up, mtx));
    shutdown(node).await;
}
