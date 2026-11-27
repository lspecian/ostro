//! 14-29 Task 1 (D-P14-45): a 3-node loopback server cluster with `[backup]` to one LocalFs
//! bucket writes `meta/head` from the metadata leader only, at its Raft term, naming a meta
//! snapshot that decodes; after a metadata leadership transfer the head moves to the new
//! leader's higher term.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::sync::Arc;
use std::time::Duration;

use ostro_backup::meta_head::{self, MetaHead};
use ostro_objstore::{LocalFs, ObjectStore};
use ostro_server::ServerHandle;
use ostro_server::config::FriendlyDuration;
use p12::*;

const P: u16 = 2;
const SEAL: Duration = Duration::from_millis(400);
const WAIT: Duration = Duration::from_secs(60);

fn leader(hs: &[ServerHandle]) -> Option<(usize, u64)> {
    hs.iter().enumerate().find_map(|(i, h)| {
        let s = h.meta().raft_status();
        s.is_leader.then_some((i, s.term))
    })
}

async fn read_head(store: &Arc<dyn ObjectStore>) -> Option<MetaHead> {
    meta_head::get(store)
        .await
        .ok()
        .flatten()
        .map(|(h, _)| h)
}

/// Poll until `meta/head` satisfies `f`; every head seen on the way must carry the term of
/// the metadata leader of that moment or an older one (never a follower's newer term).
async fn wait_head(
    store: &Arc<dyn ObjectStore>,
    hs: &[ServerHandle],
    what: &str,
    mut f: impl FnMut(&MetaHead) -> bool,
) -> MetaHead {
    let end = std::time::Instant::now() + WAIT; // clock-exempt: test deadline
    loop {
        if let Some(h) = read_head(store).await {
            if let Some((_, term)) = leader(hs) {
                assert!(h.term <= term, "head term {} above the leader's {term}", h.term);
            }
            if f(&h) {
                return h;
            }
        }
        assert!(
            std::time::Instant::now() < end,
            "{what}: meta/head = {:?}",
            read_head(store).await
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn meta_leader_writes_meta_head_and_a_transfer_moves_it_up() {
    let root = tmp();
    let bucket = root.path().join("bucket");
    std::fs::create_dir_all(&bucket).expect("bucket");
    let url = format!("file://{}", bucket.display());
    let mut cfgs = static_cluster(root.path(), 3, P, "meta-head-test");
    for c in &mut cfgs {
        c.backup.url = Some(url.clone());
        c.backup.seal_interval = FriendlyDuration(SEAL);
    }
    let hs = start_all(&cfgs).await;
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFs::new(&bucket).expect("localfs"));
    assert!(
        eventually(WAIT, || leader(&hs).is_some()).await,
        "no metadata leader"
    );

    // 1. The current leader's term reaches meta/head; its snapshot decodes.
    let h = wait_head(&store, &hs, "first head", |h| {
        leader(&hs).is_some_and(|(_, t)| h.term == t)
    })
    .await;
    let (li, term) = leader(&hs).expect("leader");
    assert_eq!(h.term, term);
    assert_eq!(h.partitions, P);
    assert_eq!(h.members, 3, "{h:?}");
    let (bytes, _) = store.get(&h.key).await.expect("get").expect("snapshot");
    let state = ostro_meta::decode_snapshot(&bytes).expect("meta snapshot decodes");
    assert_eq!(
        state.cluster().map(|c| c.value.cluster_id.clone()).as_deref(),
        Some("meta-head-test")
    );
    // Followers never write: no head carries a term no leader held, and the followers' own
    // view of the term is the leader's (they hold no newer one).
    for (i, x) in hs.iter().enumerate() {
        if i != li {
            let s = x.meta().raft_status();
            assert!(!s.is_leader);
            assert!(s.term <= term);
        }
    }
    // No Phase 8 manifest in a multi-member cluster.
    assert!(
        store
            .get(ostro_backup::manifest::CURRENT_KEY)
            .await
            .expect("get")
            .is_none()
    );

    // 2. Transfer metadata leadership: the head follows to the new, higher term.
    let to = hs
        .iter()
        .enumerate()
        .find(|(i, _)| *i != li)
        .map(|(_, x)| x.meta().node_id())
        .expect("other node");
    hs[li].meta().transfer_leader(to).await.expect("transfer");
    assert!(
        eventually(WAIT, || leader(&hs).is_some_and(|(_, t)| t > term)).await,
        "no new leader term"
    );
    let (ni, new_term) = leader(&hs).expect("leader");
    assert_eq!(hs[ni].meta().node_id(), to);
    let h2 = wait_head(&store, &hs, "head after transfer", |h| h.term == new_term).await;
    assert!(h2.term > h.term && h2.index > h.index, "{h:?} -> {h2:?}");
    let (bytes, _) = store.get(&h2.key).await.expect("get").expect("snapshot");
    ostro_meta::decode_snapshot(&bytes).expect("decodes");
    // The old leader is fenced from moving it back: the head stays at the new term.
    tokio::time::sleep(SEAL * 3).await;
    assert!(read_head(&store).await.expect("head").term >= new_term);
    stop_all(hs).await;
}
