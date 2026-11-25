//! p13-bugs2: boot regressions the 13-19 tier-C runs found, over real loopback mTLS.
//!
//! | test | fix | found by |
//! |------|-----|----------|
//! | `static_node_restarts_under_a_joined_meta_leader` | the TCP address book is seeded from the replayed metadata (and follows it) before the boot proposals; InitCluster / the own Member record retry while no leader is reachable (`cluster::boot_propose`) | 13-19 `cluster_campaign_p13` (SIGTERM restart of node 1 exited 1, "not the raft leader (no leader known)") |
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;
#[path = "common/p13_join.rs"]
mod p13;

use std::time::Duration;

use ostro_server::ServerHandle;
use p12::{START_T, start_all, static_cluster, stop_all, tmp};
use p13::*;

async fn start_joined(j: &Joiner, id: u64) -> ServerHandle {
    tokio::time::timeout(START_T, ostro_server::Server::start(joined_cfg(j, id)))
        .await
        .expect("joined start timed out")
        .expect("joined node starts")
}

/// Bug: a restarted static node's address book held only `[[cluster.peers]]` until
/// `p13::membership` started after boot. With a joined node as metadata leader, its
/// forwarded InitCluster had no address for the leader and kept failing until the proposal
/// deadline (30 s), then the process exited ("not the raft leader (no leader known)").
/// The restart must now complete well inside that deadline.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn static_node_restarts_under_a_joined_meta_leader() {
    let root = tmp();
    let mut cfgs = static_cluster(root.path(), 3, PARTS, CLUSTER);
    for c in &mut cfgs {
        timings(c);
    }
    let mut hs = start_all(&cfgs).await;
    let j4 = joiner(root.path(), 4);
    let j5 = joiner(root.path(), 5);
    let p4 = join(&hs[0], &j4).await.expect("join 4");
    let n4 = start_joined(&j4, p4.node_id).await;
    let p5 = join(&hs[0], &j5).await.expect("join 5");
    let n5 = start_joined(&j5, p5.node_id).await;
    status_until(&hs[0], Duration::from_secs(90), |st| {
        st["voters"].as_array().is_some_and(|v| v.len() == 5)
            && st["voters_outgoing"]
                .as_array()
                .is_some_and(|v| v.is_empty())
    })
    .await;

    // Metadata leadership on the joined node 4 (no static node's config names it).
    let to = p4.node_id;
    let end = std::time::Instant::now() + Duration::from_secs(30); // clock-exempt: test deadline
    loop {
        let leader = hs
            .iter()
            .map(|h| h.meta())
            .chain([n4.meta(), n5.meta()])
            .find(|m| m.is_leader());
        if n4.meta().is_leader() {
            break;
        }
        if let Some(m) = leader {
            let _ = m.transfer_leader(to).await;
        }
        assert!(
            std::time::Instant::now() < end,
            "meta leadership never reached node {to}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Controlled shutdown of static node 1 (not the meta leader), then restart it.
    let n1 = hs.remove(0);
    n1.shutdown().await.expect("node 1 shutdown");
    assert!(n4.meta().is_leader(), "node 4 still leads the metadata");
    let started = std::time::Instant::now(); // clock-exempt: test timing
    let again = tokio::time::timeout(
        Duration::from_secs(25),
        ostro_server::Server::start(cfgs[0].clone()),
    )
    .await
    .expect("node 1 restart hung past the 25 s bound (the 30 s proposal deadline path)")
    .expect("node 1 restarts under a joined metadata leader");
    println!("node 1 restart under leader {to}: {:?}", started.elapsed());
    // Its member record and the cluster are intact; it reaches the leader.
    status_until(&again, Duration::from_secs(30), |st| {
        st["meta_leader"] == to && member(st, 1).is_some_and(|m| m["state"] == "active")
    })
    .await;
    hs.insert(0, again);
    n5.shutdown().await.unwrap();
    n4.shutdown().await.unwrap();
    stop_all(hs).await;
}
