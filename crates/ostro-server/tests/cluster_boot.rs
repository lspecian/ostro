//! 12-14 Task 1: the Phase 12 boot order (D-P12-13/14/15/16). Single nodes are meta-first
//! (bootstrap placement on the first boot, one fresh `ElectLeader` term per partition on every
//! later start, `LeaderAnnounced` at that epoch); identity and voter checks fail-stop before
//! meta opens; static 3-node clusters form over real loopback mTLS.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::time::Duration;

use ostro_log::ReplicatedLog as _;
use ostro_meta::ext::{ELECTION_REASON_BOOTSTRAP, ELECTION_REASON_STARTUP};
use ostro_partition::PartitionRole;
use ostro_types::{Epoch, PartitionId};
use p12::*;

const P: u16 = 3;

fn latest_epochs(h: &ostro_server::ServerHandle) -> Vec<Option<Epoch>> {
    (0..P)
        .map(|p| {
            h.node()
                .handle(PartitionId(p))
                .and_then(|x| x.log().latest_epoch())
        })
        .collect()
}

fn all_lead(h: &ostro_server::ServerHandle, epoch: u32) -> bool {
    (0..P).all(|p| {
        matches!(
            h.node().role(PartitionId(p)).map(|r| *r.borrow()),
            Some(PartitionRole::Leader { epoch: e }) if e == Epoch(epoch)
        )
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_meta_first_placement_then_fresh_terms() {
    let d = tmp();
    let cfg = single(d.path(), P);

    // First boot: placement epoch 0 -> 1, replicas [self], leads at 1.
    let h = start(cfg.clone()).await.expect("first boot");
    let toml = node_toml(d.path());
    assert!(toml.contains("node_id = 1"), "{toml}");
    assert!(
        toml.contains(&format!("cluster_id = \"{}\"", h.cluster_id())),
        "{toml}"
    );
    let stored = h
        .meta()
        .view()
        .read(|m| m.cluster().map(|c| c.value.cluster_id.clone()));
    assert_eq!(stored.as_deref(), Some(h.cluster_id()));
    for (p, epoch, leader, replicas, isr) in assignments(&h) {
        assert_eq!((epoch, leader), (1, 1), "p{p}");
        assert_eq!(replicas, vec![1]);
        assert_eq!(isr, vec![1]);
        assert_eq!(election_reason(&h, p), Some(ELECTION_REASON_BOOTSTRAP));
    }
    assert!(all_lead(&h, 1));
    assert_eq!(latest_epochs(&h), vec![Some(Epoch(1)); P as usize]);
    let first = h.timings();
    let cluster_id = h.cluster_id().to_owned();
    h.shutdown().await.expect("shutdown");

    // Every later start: one fresh ElectLeader term per partition, then LA at that epoch.
    for round in 2..=3u32 {
        let h = start(cfg.clone()).await.expect("restart");
        assert_eq!(h.cluster_id(), cluster_id);
        for (p, epoch, leader, _, isr) in assignments(&h) {
            assert_eq!((epoch, leader), (round, 1), "p{p} round {round}");
            assert_eq!(isr, vec![1]);
            assert_eq!(election_reason(&h, p), Some(ELECTION_REASON_STARTUP));
        }
        assert!(all_lead(&h, round));
        assert_eq!(latest_epochs(&h), vec![Some(Epoch(round)); P as usize]);
        let t = h.timings();
        println!(
            "round {round}: first boot {:?} / warm {:?} (meta {:?}, node_open {:?}, first_terms {:?})",
            first.total, t.total, t.meta_leader, t.node_open, t.first_terms
        );
        // The 04-19 warm-restart budget (bootstrap.rs / ostro binary suites) is 5 s.
        assert!(
            t.total < Duration::from_secs(5),
            "warm restart {:?}",
            t.total
        );
        h.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn other_node_id_on_existing_dir_is_fatal_before_meta() {
    let d = tmp();
    let cfg = single(d.path(), P);
    start(cfg.clone()).await.unwrap().shutdown().await.unwrap();
    let meta = d.path().join("meta");
    let before = std::fs::metadata(meta.join("state"))
        .unwrap()
        .modified()
        .unwrap();
    let mut other = cfg.clone();
    other.node.node_id = 7;
    let e = start(other).await.expect_err("node id mismatch");
    let msg = e.to_string();
    assert!(msg.contains("node_id") && msg.contains('7'), "{msg}");
    let after = std::fs::metadata(meta.join("state"))
        .unwrap()
        .modified()
        .unwrap();
    assert_eq!(before, after, "meta was not opened");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configured_cluster_id_conflicting_with_node_toml_is_fatal() {
    let d = tmp();
    let cfg = single(d.path(), P);
    let h = start(cfg.clone()).await.unwrap();
    let id = h.cluster_id().to_owned();
    h.shutdown().await.unwrap();
    let mut other = cfg.clone();
    other.cluster.cluster_id = Some("some-other-cluster".into());
    let msg = start(other).await.expect_err("conflict").to_string();
    assert!(msg.contains("conflict") && msg.contains(&id), "{msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wiped_meta_without_peers_fail_stops_meta_dir_missing() {
    let d = tmp();
    let cfg = single(d.path(), P);
    start(cfg.clone()).await.unwrap().shutdown().await.unwrap();
    std::fs::remove_dir_all(d.path().join("meta")).unwrap();
    let msg = start(cfg).await.expect_err("joiner").to_string();
    assert!(msg.contains("meta dir missing"), "{msg}");
    assert!(
        !d.path().join("meta").join("state").exists(),
        "no new meta written"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pre_phase12_dir_adopts_the_stored_cluster_id() {
    let d = tmp();
    let cfg = single(d.path(), P);
    let h = start(cfg.clone()).await.unwrap();
    let id = h.cluster_id().to_owned();
    h.shutdown().await.unwrap();
    // A pre-Phase-12 node.toml has no identity keys.
    let toml: String = node_toml(d.path())
        .lines()
        .filter(|l| !l.starts_with("node_id") && !l.starts_with("cluster_id"))
        .map(|l| format!("{l}\n"))
        .collect();
    std::fs::write(d.path().join("node.toml"), toml).unwrap();
    let h = start(cfg).await.expect("adopts");
    assert_eq!(
        h.cluster_id(),
        id,
        "the stored CA / ClusterInfo id, never a new one"
    );
    assert!(node_toml(d.path()).contains(&format!("cluster_id = \"{id}\"")));
    // A fresh term (epoch + 1), not a placement.
    for (p, epoch, _, _, _) in assignments(&h) {
        assert_eq!(epoch, 2, "p{p}");
        assert_eq!(election_reason(&h, p), Some(ELECTION_REASON_STARTUP));
    }
    h.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persisted_voters_differing_from_peers_is_fatal() {
    let root = tmp();
    let cfgs = static_cluster(root.path(), 3, P, "grow-test");
    // Node 1 first runs alone as a single node of the same cluster id.
    let mut solo = cfgs[0].clone();
    solo.cluster.peers.clear();
    start(solo).await.unwrap().shutdown().await.unwrap();
    let msg = start(cfgs[0].clone())
        .await
        .expect_err("voters changed")
        .to_string();
    assert!(msg.contains("grow via Phase 13"), "{msg}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_node_cluster_forms_and_replicates_meta_first() {
    let root = tmp();
    let cfgs = static_cluster(root.path(), 3, P, "boot-3");
    // A crash between bind_cluster_id and the meta bootstrap leaves node.toml with both
    // identity keys and no meta: not a joiner.
    std::fs::create_dir_all(&cfgs[0].node.data_dir).unwrap();
    ostro_partition::bind_node_id(&cfgs[0].node.data_dir, 1).unwrap();
    ostro_partition::bind_cluster_id(&cfgs[0].node.data_dir, "boot-3").unwrap();

    let hs = start_all(&cfgs).await;
    for h in &hs {
        assert_eq!(h.cluster_id(), "boot-3");
        let voters = h.meta().raft().status().voters;
        let mut v = voters.clone();
        v.sort_unstable();
        assert_eq!(v, vec![1, 2, 3]);
    }
    // Placement: every partition RF 3, epoch 1, one leader per partition.
    assert!(
        eventually(Duration::from_secs(30), || {
            assignments(&hs[0])
                .iter()
                .all(|(_, e, l, r, _)| *e == 1 && *l != 0 && r.len() == 3)
        })
        .await,
        "placement: {:?}",
        assignments(&hs[0])
    );
    // Followers join the ISR (measured, AlterIsr through the publisher).
    assert!(
        eventually(Duration::from_secs(30), || {
            assignments(&hs[0])
                .iter()
                .all(|(_, _, _, _, isr)| isr.len() >= 2)
        })
        .await,
        "ISR: {:?}",
        assignments(&hs[0])
    );
    // Every partition is led by exactly the node meta names, the others follow.
    assert!(
        eventually(Duration::from_secs(30), || {
            assignments(&hs[0]).iter().all(|(p, e, l, _, _)| {
                hs.iter().zip(1u64..).all(|(h, me)| {
                    let role = h.node().role(PartitionId(*p)).map(|r| *r.borrow());
                    if me == *l {
                        matches!(role, Some(PartitionRole::Leader { epoch }) if epoch == Epoch(*e))
                    } else {
                        matches!(role, Some(PartitionRole::Follower { epoch, leader }) if epoch == Epoch(*e) && leader == *l)
                    }
                })
            })
        })
        .await,
        "roles"
    );
    // The leaders' LeaderAnnounced records are replicated: every node's log holds epoch 1.
    for (h, id) in hs.iter().zip(1u64..) {
        assert!(
            eventually(Duration::from_secs(20), || latest_epochs(h)
                .iter()
                .all(|e| *e == Some(Epoch(1))))
            .await,
            "node {id} epochs {:?}",
            latest_epochs(h)
        );
    }
    stop_all(hs).await;

    // A wiped meta dir over intact partitions, peers configured: the wiped-voter remedy.
    std::fs::remove_dir_all(cfgs[1].node.data_dir.join("meta")).unwrap();
    let msg = start(cfgs[1].clone())
        .await
        .expect_err("wiped voter")
        .to_string();
    assert!(msg.contains("ostro cluster"), "{msg}");
    assert!(!cfgs[1].node.data_dir.join("meta").join("state").exists());
}
