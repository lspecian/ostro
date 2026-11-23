//! Cluster-mode uploads (plan 12-15, D-P12-20, D-P12-3): only a partition's current leader
//! uploads it. Each partition's `p/<pppp>/head` is fenced by leader epoch. A stale leader is
//! fenced for that partition only and can never move the head back. A single-member cluster
//! still writes a Phase 8 manifest that restores.
//!
//! Two "nodes" (real `Node`s with their own data directories) share one LocalFs bucket. The
//! leader set comes from a fake `PartitionRoles` that each test drives.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;

use std::sync::Arc;

use common::{
    current_manifest, keys, local_store, node_cfg, open, shutdown, simulate_restore, start_runs,
    up_cfg,
};
use ostro_backup::chunk;
use ostro_backup::cluster::{self, Head};
use ostro_backup::uploader::{Uploader, UploaderClusterDeps};
use ostro_objstore::ObjectStore;
use ostro_partition::roles::PartitionRoles;
use ostro_partition::{Node, SystemClock};
use ostro_types::{Epoch, Offset, PartitionId};
use tokio::sync::watch;

const P0: PartitionId = PartitionId(0);
const P1: PartitionId = PartitionId(1);

type Led = Arc<Vec<(PartitionId, Epoch)>>;

/// The leader set of one fake node.
struct FakeRoles(watch::Sender<Led>);

impl PartitionRoles for FakeRoles {
    fn led(&self) -> watch::Receiver<Led> {
        self.0.subscribe()
    }
}

fn roles(led: &[(u16, u32)]) -> Arc<FakeRoles> {
    let (tx, _) = watch::channel(Arc::new(Vec::new()));
    let r = Arc::new(FakeRoles(tx));
    set_led(&r, led);
    r
}

fn set_led(r: &FakeRoles, led: &[(u16, u32)]) {
    r.0.send_replace(Arc::new(
        led.iter()
            .map(|(p, e)| (PartitionId(*p), Epoch(*e)))
            .collect(),
    ));
}

fn cluster_uploader(
    node: &Arc<Node>,
    node_id: u64,
    roles: &Arc<FakeRoles>,
    members: watch::Receiver<usize>,
) -> Arc<Uploader> {
    let mut cfg = up_cfg();
    cfg.node_id = node_id;
    Arc::new(
        Uploader::new(Arc::clone(node), None, Arc::new(SystemClock), cfg).with_cluster(
            UploaderClusterDeps {
                roles: Arc::clone(roles) as Arc<dyn PartitionRoles>,
                members,
            },
        ),
    )
}

async fn head(store: &Arc<dyn ObjectStore>, p: PartitionId) -> Option<Head> {
    cluster::read_head(store, p.0)
        .await
        .expect("read head")
        .map(|(h, _)| h)
}

async fn head_bytes(store: &Arc<dyn ObjectStore>, p: PartitionId) -> Option<Vec<u8>> {
    store
        .get(&cluster::head_key(p.0))
        .await
        .expect("get head")
        .map(|(b, _)| b.to_vec())
}

fn hw(node: &Node, p: PartitionId) -> u64 {
    node.log_cut()[p.index()].0
}

/// The local committed envelopes of `p` in `[from, to)`.
async fn local_envelopes(node: &Node, p: PartitionId, from: u64, to: u64) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut at = from;
    while at < to {
        let batches = node
            .read_envelopes(p, Offset(at), 1 << 20)
            .await
            .expect("read envelopes");
        assert!(!batches.is_empty(), "local log ends at {at} before {to}");
        for b in batches {
            for env in b.envelopes {
                let (_, end) = chunk::envelope_span(&env).expect("span");
                if end > to {
                    return out;
                }
                out.push(env.to_vec());
                at = end;
            }
        }
    }
    out
}

/// The chain reachable from `p`'s head equals `node`'s committed log over the same range,
/// envelope for envelope, and ends at or below HW. Returns the chain end.
async fn assert_chain_is_committed(
    store: &Arc<dyn ObjectStore>,
    node: &Node,
    p: PartitionId,
) -> u64 {
    let h = head(store, p).await.expect("head");
    let Some(key) = &h.last_chunk_key else {
        return h.chain_start;
    };
    let chain = chunk::walk_back(store, key, h.last_chunk_sha256.expect("sha"), h.chain_start)
        .await
        .expect("chain walks back");
    let first = chain.first().expect("non-empty").chunk.header.base;
    let end = chain.last().expect("non-empty").chunk.header.end;
    assert_eq!(first, h.chain_start, "chain starts at its chain_start");
    assert_eq!(end, h.end, "head end = chain end");
    assert!(
        end <= hw(node, p),
        "chain end {end} above HW {}",
        hw(node, p)
    );
    let remote: Vec<Vec<u8>> = chain
        .iter()
        .flat_map(|c| c.chunk.envelopes.iter().cloned())
        .collect();
    assert_eq!(
        remote,
        local_envelopes(node, p, first, end).await,
        "remote chain of p{} = committed log [{first}, {end})",
        p.0
    );
    end
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_led_partitions_upload_and_each_gets_a_head() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 2)).await;
    start_runs(&node, 0, 10).await;
    start_runs(&node, 1, 10).await;
    let r = roles(&[(0, 2)]);
    let (_members_tx, members) = watch::channel(2usize);
    let up = cluster_uploader(&node, 1, &r, members);

    let rep = up.round(&store).await.expect("round");
    assert!(rep.manifest.is_none(), "multi-member: no manifest");
    assert!(current_manifest(&store).await.is_none());
    let h0 = head(&store, P0).await.expect("p0 head");
    assert_eq!((h0.epoch, h0.node_id, h0.members), (2, 1, 2));
    assert_eq!(
        assert_chain_is_committed(&store, &node, P0).await,
        hw(&node, P0)
    );
    assert!(head(&store, P1).await.is_none(), "p1 is not led: no head");
    assert!(
        keys(&store, "p/0001/seg/").await.is_empty(),
        "p1 not uploaded"
    );

    // An idle round writes nothing.
    let before = head_bytes(&store, P0).await;
    up.round(&store).await.expect("idle round");
    assert_eq!(head_bytes(&store, P0).await, before);

    // Leading p1 too: it uploads from the next round on.
    set_led(&r, &[(0, 2), (1, 2)]);
    start_runs(&node, 0, 5).await;
    up.round(&store).await.expect("round");
    assert_eq!(
        assert_chain_is_committed(&store, &node, P0).await,
        hw(&node, P0)
    );
    assert_eq!(
        assert_chain_is_committed(&store, &node, P1).await,
        hw(&node, P1)
    );
    assert_eq!(head(&store, P1).await.expect("p1 head").epoch, 2);

    // Losing p0: no more uploads of it.
    set_led(&r, &[(1, 2)]);
    start_runs(&node, 0, 5).await;
    let before = head_bytes(&store, P0).await;
    let segs = keys(&store, "p/0000/seg/").await;
    up.round(&store).await.expect("round");
    assert_eq!(head_bytes(&store, P0).await, before);
    assert_eq!(keys(&store, "p/0000/seg/").await, segs);
    assert!(up.partition_fenced(P0).is_none());
    drop(up);
    shutdown(node).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_takeover_fences_the_old_leader_for_that_partition_only() {
    let dir_a = tempfile::tempdir().expect("tempdir");
    let dir_b = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let (_members_tx, members) = watch::channel(2usize);

    // A leads p0 and p1 at epoch 2.
    let a = open(node_cfg(dir_a.path(), 2)).await;
    start_runs(&a, 0, 10).await;
    start_runs(&a, 1, 10).await;
    let ra = roles(&[(0, 2), (1, 2)]);
    let up_a = cluster_uploader(&a, 1, &ra, members.clone());
    up_a.round(&store).await.expect("A round");
    let a_segs = keys(&store, "p/0000/seg/").await;
    assert!(!a_segs.is_empty());
    assert_eq!(head(&store, P0).await.expect("head").epoch, 2);

    // B takes p0 over at epoch 3. Its log is not A's, so its takeover repairs the remote chain
    // down to the common prefix (none), then uploads its own log.
    let mut cfg_b = node_cfg(dir_b.path(), 2);
    cfg_b.node_id = 2;
    let b = open(cfg_b).await;
    start_runs(&b, 0, 15).await;
    let rb = roles(&[(0, 3)]);
    let up_b = cluster_uploader(&b, 2, &rb, members.clone());
    up_b.round(&store).await.expect("B round");
    let h = head(&store, P0).await.expect("head");
    assert_eq!((h.epoch, h.node_id), (3, 2));
    assert_eq!(assert_chain_is_committed(&store, &b, P0).await, hw(&b, P0));
    let now = keys(&store, "p/0000/seg/").await;
    assert!(
        a_segs.iter().all(|k| !now.contains(k)),
        "A's divergent chunks deleted after B's head moved"
    );

    // A keeps going: its p0 head update fails and it is fenced for p0 only.
    start_runs(&a, 0, 5).await;
    start_runs(&a, 1, 5).await;
    let p1_before = head(&store, P1).await.expect("p1 head");
    let b_head = head_bytes(&store, P0).await;
    up_a.round(&store).await.expect("A round after takeover");
    assert_eq!(
        up_a.partition_fenced(P0),
        Some(2),
        "A fenced for p0 at epoch 2"
    );
    assert_eq!(up_a.partition_fenced(P1), None);
    assert!(!up_a.is_fenced(), "the uploader itself keeps running");
    assert_eq!(head_bytes(&store, P0).await, b_head, "B's head untouched");
    let p1_after = head(&store, P1).await.expect("p1 head");
    assert!(p1_after.end > p1_before.end, "p1 keeps uploading");
    assert_eq!(assert_chain_is_committed(&store, &a, P1).await, hw(&a, P1));

    // A stale-epoch writer never moves the head: more rounds, and a fresh uploader at epoch 2.
    start_runs(&a, 0, 5).await;
    up_a.round(&store).await.expect("A round");
    assert_eq!(head_bytes(&store, P0).await, b_head);
    let up_a2 = cluster_uploader(&a, 1, &ra, members.clone());
    up_a2.round(&store).await.expect("fresh A uploader");
    assert_eq!(up_a2.partition_fenced(P0), Some(2));
    assert_eq!(
        head_bytes(&store, P0).await,
        b_head,
        "head never moves backwards"
    );
    // Its other partition is unaffected.
    assert_eq!(up_a2.partition_fenced(P1), None);

    // A leads p0 again at a higher epoch: the fence lifts and A's log wins.
    set_led(&ra, &[(0, 4), (1, 2)]);
    up_a.round(&store).await.expect("A leads p0 at 4");
    assert_eq!(up_a.partition_fenced(P0), None);
    let h = head(&store, P0).await.expect("head");
    assert_eq!((h.epoch, h.node_id), (4, 1));
    assert_eq!(assert_chain_is_committed(&store, &a, P0).await, hw(&a, P0));

    // And B, still believing it leads at 3, is now fenced for p0.
    start_runs(&b, 0, 5).await;
    up_b.round(&store).await.expect("B round");
    assert_eq!(up_b.partition_fenced(P0), Some(3));
    assert_eq!(head(&store, P0).await.expect("head").epoch, 4);

    drop((up_a, up_a2, up_b));
    shutdown(a).await;
    shutdown(b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_member_writes_manifest_and_heads_and_restores() {
    let dir = tempfile::tempdir().expect("tempdir");
    let remote = tempfile::tempdir().expect("remote");
    let store = local_store(remote.path(), true);
    let node = open(node_cfg(dir.path(), 2)).await;
    start_runs(&node, 0, 12).await;
    start_runs(&node, 1, 7).await;
    node.snapshot_now(P1).await.expect("snap").expect("new");
    start_runs(&node, 1, 3).await;
    let r = roles(&[(0, 1), (1, 1)]);
    let (members_tx, members) = watch::channel(1usize);
    let up = cluster_uploader(&node, 1, &r, members);

    let m = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("single member: manifest written");
    m.validate().expect("valid");
    for p in [P0, P1] {
        let h = head(&store, p).await.expect("head");
        let e = m.partition(p.0).expect("entry");
        assert_eq!(h.last_chunk_key, e.last_chunk_key, "head = manifest entry");
        assert_eq!(h.last_chunk_sha256, e.last_chunk_sha256);
        assert_eq!(h.chain_start, e.chain_start);
        assert_eq!(h.members, 1);
        assert_eq!(
            h.snapshot.as_ref().map(|s| s.offset),
            e.snapshot_offset,
            "same referenced snapshot"
        );
        assert_eq!(
            assert_chain_is_committed(&store, &node, p).await,
            e.cut_offset
        );
    }
    let restored = simulate_restore(&store, &m, 2).await;
    for rp in &restored {
        let live = node
            .handle(PartitionId(rp.p))
            .expect("ready")
            .store()
            .digest()
            .expect("digest");
        assert_eq!(rp.digest, live, "p{} restores to the live digest", rp.p);
    }

    // The cluster grows: heads keep moving, the manifest stops.
    members_tx.send_replace(2);
    start_runs(&node, 0, 4).await;
    let rep = up.round(&store).await.expect("round");
    assert!(rep.manifest.is_none());
    assert_eq!(
        current_manifest(&store).await.expect("manifest").generation,
        m.generation,
        "manifest/current untouched with two members"
    );
    assert_eq!(
        assert_chain_is_committed(&store, &node, P0).await,
        hw(&node, P0)
    );
    assert_eq!(head(&store, P0).await.expect("head").members, 2);

    // Back to one member: the manifest resumes from the heads' state.
    members_tx.send_replace(1);
    start_runs(&node, 0, 2).await;
    let m2 = up
        .round(&store)
        .await
        .expect("round")
        .manifest
        .expect("manifest again");
    assert_eq!(m2.generation, m.generation + 1);
    assert_eq!(
        m2.partition(0).expect("p0").last_chunk_key,
        head(&store, P0).await.expect("head").last_chunk_key
    );
    drop(up);
    shutdown(node).await;
}
