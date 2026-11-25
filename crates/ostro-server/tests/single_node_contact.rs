//! A single-voter node never self-demotes (bug hunt p13-offline).
//!
//! Self-demotion (12-14, `cluster.rs` module docs) masks a node's led partitions as
//! leaderless once its metadata contact (`leader_contact_age`) is older than
//! `[replication] session_timeout`. The contact is refreshed by the meta Raft loop, so any
//! pause of that loop longer than the timeout (a slow meta fsync, a whole-process stall, CFS
//! throttling) used to demote a single node from itself: every partition published leader 0
//! for a watchdog period and the 13-12 router answered `503 partition_offline` ("no
//! in-sync replica can lead it") although the only replica was fine; contact back then
//! bumped every epoch with a fresh term.
//!
//! The test makes that pause the normal case: `session_timeout` (10 ms) is far below the
//! idle gap of the meta Raft loop (its 100 ms tick), so the old watchdog demoted within a
//! few of its 20 ms periods. A single voter is its own metadata quorum and cannot lose
//! contact with it: the partitions must stay led at their first epoch, the published view
//! must never show leader 0 and routed calls must never answer `partition_offline`.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

#[path = "common/p12_cluster.rs"]
mod p12;

use std::time::{Duration, Instant};

use ostro_partition::PartitionRole;
use ostro_server::config::FriendlyDuration;
use ostro_types::{Epoch, PartitionId};
use p12::*;

const P: u16 = 3;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_voter_never_self_demotes_on_a_stale_contact() {
    let d = tmp();
    let mut cfg = single(d.path(), P);
    cfg.replication.session_timeout = FriendlyDuration::from_millis(10);
    let h = start(cfg).await.expect("boot");
    assert!(
        eventually(Duration::from_secs(30), || (0..P).all(|p| matches!(
            h.node().role(PartitionId(p)).map(|r| *r.borrow()),
            Some(PartitionRole::Leader { .. })
        )))
        .await,
        "partitions never led: {:?}",
        assignments(&h)
    );
    let first: Vec<u32> = assignments(&h).iter().map(|r| r.1).collect();
    let views = h.p13().expect("p13 context").cluster.assignment_watch();
    let http = reqwest::Client::new();
    let absent = ostro_types::RunId::new_v7(PartitionId(0));
    let url = format!("http://{}/v1/runs/{absent}", h.local_addr());

    // ~150 watchdog periods (20 ms), each with a contact age of up to one Raft tick (100 ms).
    let end = Instant::now() + Duration::from_secs(3); // clock-exempt: test deadline
    let mut samples = 0u32;
    while Instant::now() < end {
        for p in 0..P {
            let role = h.node().role(PartitionId(p)).map(|r| *r.borrow());
            assert!(
                matches!(role, Some(PartitionRole::Leader { epoch }) if epoch == Epoch(first[usize::from(p)])),
                "p{p} left leadership after {samples} samples: {role:?}, meta {:?}",
                assignments(&h)
            );
        }
        let masked: Vec<u16> = views
            .borrow()
            .iter()
            .filter(|v| v.leader == 0)
            .map(|v| v.partition.0)
            .collect();
        assert!(
            masked.is_empty(),
            "partitions {masked:?} published leaderless after {samples} samples"
        );
        // A routed partition read (an absent run on partition 0) is a 404, never a 503.
        let r = http.get(&url).send().await.expect("GET run");
        let st = r.status();
        let body = r.text().await.unwrap_or_default();
        assert!(
            !body.contains("partition_offline"),
            "{st} after {samples} samples: {body}"
        );
        samples += 1;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let now: Vec<u32> = assignments(&h).iter().map(|r| r.1).collect();
    assert_eq!(now, first, "fresh terms after a self-demotion");
    h.shutdown().await.expect("shutdown");
}
