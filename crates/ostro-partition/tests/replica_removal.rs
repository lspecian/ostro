//! 13-10 plan check W6: what the Phase 12 role lifecycle (12-13) does when the local node
//! leaves a partition's `replicas` (a reassignment Shrink, `AlterReplicas`), and when it is
//! added back later.
//!
//! Observed and asserted Phase 12 behaviour (no `roles.rs` change was needed):
//! - leaving `replicas` → `roles::decide` = Offline: the fetcher is stopped, the router
//!   keeps a leader hint to the leader, the partition is not routable, no term is requested;
//! - the local partition data is **retained inert** (log and store stay on disk, nothing is
//!   appended, nothing is served) for as long as the node is not a replica, including well
//!   past a session timeout; it is never deleted by the role lifecycle;
//! - re-added as a follower → `become_follower` + a new fetcher at the current epoch (the
//!   Phase 12 fetcher truncates a stale tail through the divergence path or installs a
//!   snapshot below `log_start`; tier B `reassign::readded_replica_truncates` checks that
//!   end to end).
//!
//! Fake assignment source + fake follower driver, real logs and stores.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p12_roles.rs"]
mod p12_roles;

use std::time::Duration;

use ostro_log::ReplicatedLog;
use ostro_partition::router::PartitionRouter;
use ostro_partition::{PartitionError, PartitionRole};
use ostro_types::command::InboundBatch;
use ostro_types::{Command, Epoch, PartitionId};
use p12_roles::{FakeAssignments, FakeFollower, FetcherEvent, open, roles_cfg, view, wait_role};
use tokio::time::sleep;

const P0: PartitionId = PartitionId(0);

async fn routable(node: &ostro_partition::Node, p: PartitionId) -> bool {
    let batch = InboundBatch {
        src: 0,
        msgs: Vec::new(),
    };
    !matches!(
        node.router().deliver(p, batch).await,
        Err(PartitionError::NotLeader)
    )
}

/// Every file under `dir` with its length (the partition's on-disk footprint).
fn footprint(dir: &std::path::Path) -> Vec<(std::path::PathBuf, u64)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            match e.metadata() {
                Ok(m) if m.is_dir() => stack.push(p),
                Ok(m) => out.push((p, m.len())),
                Err(_) => {}
            }
        }
    }
    out.sort();
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_follower_stops_fetching_and_keeps_data_inert() {
    let tmp = ostro_testkit::fs::tempdir();
    // Node 1 leads p0 at epoch 1 and appends some history, then node 2 takes over at
    // epoch 2 with replicas [2, 1]: node 1 follows.
    let asg = FakeAssignments::new(1, vec![view(0, 1, 1, &[1, 2], &[1], 1, 1)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    p12_roles::wait_leader(&node, 0, 1).await;
    let h = node.handle(P0).expect("ready");
    for _ in 0..3 {
        h.submit_one(Command::start_run(common::start_run(&common::run_on(0))))
            .await
            .expect("submit");
    }
    let leo_before = h.log().replica_status().durable_end;
    asg.set(view(0, 2, 2, &[2, 1], &[2], 2, 1));
    wait_role(&node, 0, "Follower{2, 2}", |r| {
        *r == PartitionRole::Follower {
            epoch: Epoch(2),
            leader: 2,
        }
    })
    .await;
    assert_eq!(
        fol.events(),
        vec![FetcherEvent::Start {
            p: 0,
            epoch: 2,
            leader: 2
        }]
    );

    // Shrink: replicas [2] (node 1 removed), same epoch, isr_version bumped.
    asg.set(view(0, 2, 2, &[2], &[2], 3, 1));
    wait_role(&node, 0, "Offline", |r| *r == PartitionRole::Offline).await;
    p12_roles::eventually("fetcher stopped", || {
        matches!(fol.events().last(), Some(FetcherEvent::Stop { p: 0, .. }))
    })
    .await;
    assert_eq!(node.router().leader_hint(P0), Some(2), "hint to the leader");
    assert!(
        !routable(&node, P0).await,
        "a removed replica is never served"
    );
    assert!(node.roles().led().borrow().is_empty());
    assert!(asg.term_requests().is_empty(), "no term request");

    // Past a session timeout the data is still there, unchanged (retained inert).
    let part_dir = tmp.path().join("partitions");
    let dir = if part_dir.exists() {
        part_dir
    } else {
        tmp.path().to_path_buf()
    };
    let before = footprint(&dir);
    assert!(!before.is_empty(), "partition data on disk");
    sleep(Duration::from_millis(1500)).await;
    let h = node
        .handle(P0)
        .expect("the handle stays open while Offline");
    assert_eq!(
        h.log().replica_status().durable_end,
        leo_before,
        "nothing appended"
    );
    let r = h
        .submit_one(Command::start_run(common::start_run(&common::run_on(0))))
        .await;
    assert!(r.is_err(), "a removed replica refuses submits: {r:?}");
    let after = footprint(&dir);
    let names = |v: &[(std::path::PathBuf, u64)]| -> Vec<std::path::PathBuf> {
        v.iter()
            .filter(|(p, _)| p.extension().is_some_and(|e| e == "wal" || e == "log"))
            .map(|(p, _)| p.clone())
            .collect()
    };
    assert_eq!(names(&before), names(&after), "log segments retained");
    assert!(!after.is_empty(), "data retained, never deleted");
    assert_eq!(
        fol.events()
            .iter()
            .filter(|e| matches!(e, FetcherEvent::Start { .. }))
            .count(),
        1,
        "no fetcher restarted while removed"
    );

    // Re-added (Expand of a later reassignment): follows the current leader again.
    asg.set(view(0, 2, 2, &[2, 1], &[2], 4, 1));
    wait_role(&node, 0, "Follower{2, 2} again", |r| {
        *r == PartitionRole::Follower {
            epoch: Epoch(2),
            leader: 2,
        }
    })
    .await;
    assert!(matches!(
        fol.events().last(),
        Some(FetcherEvent::Start {
            p: 0,
            epoch: 2,
            leader: 2
        })
    ));
    assert!(asg.term_requests().is_empty());
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn readded_at_a_later_epoch_follows_without_requesting_a_term() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![view(0, 2, 1, &[2, 1], &[2], 1, 1)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_role(&node, 0, "Follower{1, 2}", |r| {
        *r == PartitionRole::Follower {
            epoch: Epoch(1),
            leader: 2,
        }
    })
    .await;
    // Removed, then leadership moves twice while node 1 is out.
    asg.set(view(0, 2, 1, &[2, 3], &[2, 3], 2, 2));
    wait_role(&node, 0, "Offline", |r| *r == PartitionRole::Offline).await;
    asg.set(view(0, 3, 4, &[2, 3], &[3], 3, 1));
    sleep(Duration::from_millis(200)).await;
    assert_eq!(
        *node.role(P0).expect("role").borrow(),
        PartitionRole::Offline,
        "a leader change elsewhere does not wake a non-replica"
    );
    // Re-added at epoch 4 under leader 3.
    asg.set(view(0, 3, 4, &[3, 2, 1], &[3], 4, 1));
    wait_role(&node, 0, "Follower{4, 3}", |r| {
        *r == PartitionRole::Follower {
            epoch: Epoch(4),
            leader: 3,
        }
    })
    .await;
    assert!(
        asg.term_requests().is_empty(),
        "a follower never asks for a term"
    );
    node.shutdown().await.expect("shutdown");
}
