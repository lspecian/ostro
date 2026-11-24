//! Per-partition role lifecycle (12-13, research Pattern 5): roles follow meta only, a
//! takeover announces its term (`LeaderAnnounced` at the meta epoch, appended and applied)
//! before leader services start, a demotion fails pending acks `NotLeader` with a leader hint
//! and starts the fetcher (`become_follower`), epoch 0 is Offline, a restart never leads at
//! its old epoch (it requests a fresh term), AlterIsr keeps the leader, and a failed leader
//! resigns once. Fake assignment source + fake follower driver, real logs and stores.
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

mod common;
#[path = "common/p12_roles.rs"]
mod p12_roles;

use std::sync::Arc;
use std::time::Duration;

use ostro_log::ReplicatedLog;
use ostro_partition::router::PartitionRouter;
use ostro_partition::{PartitionError, PartitionRole, StartupEvent};
use ostro_types::command::InboundBatch;
use ostro_types::{Command, Epoch, PartitionId};
use p12_roles::{
    FakeAssignments, FakeFollower, FetcherEvent, ROLE_T, events_of, index_of, open, roles_cfg,
    solo, view, wait_leader, wait_role,
};
use tokio::time::{sleep, timeout};

const P0: PartitionId = PartitionId(0);

fn term_started(ev: &[StartupEvent]) -> Vec<u32> {
    ev.iter()
        .filter_map(|e| match e {
            StartupEvent::TermStarted { epoch, .. } => Some(*epoch),
            _ => None,
        })
        .collect()
}

/// `deliver` through the node's router: `NotLeader` unless this node leads.
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_at_meta_epoch_announces_before_services() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![solo(0, 1, 2)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_leader(&node, 0, 2).await;

    let ev = events_of(&node, 0);
    let first_role = ev
        .iter()
        .find_map(|e| match e {
            StartupEvent::RoleChanged { role, .. } => Some(*role),
            _ => None,
        })
        .expect("role events");
    assert_eq!(
        first_role,
        PartitionRole::Offline,
        "a partition opens Offline"
    );
    let la = index_of(&ev, |e| {
        matches!(e, StartupEvent::TermStarted { epoch: 2, .. })
    })
    .expect("TermStarted{2}");
    let svc = index_of(&ev, |e| {
        matches!(e, StartupEvent::LeaderServicesStarted { .. })
    })
    .expect("LeaderServicesStarted");
    let announcing = index_of(&ev, |e| {
        matches!(
            e,
            StartupEvent::RoleChanged {
                role: PartitionRole::Announcing { epoch: Epoch(2) },
                ..
            }
        )
    })
    .expect("Announcing{2}");
    assert!(
        announcing < la && la < svc,
        "Announcing < TermStarted < services: {ev:?}"
    );
    assert_eq!(term_started(&ev), vec![2], "one term, at the meta epoch");

    // LeaderAnnounced{2} is in the log and applied.
    let h = node.handle(P0).expect("ready");
    assert_eq!(h.log().latest_epoch(), Some(Epoch(2)));
    let meta = h.store().read_meta().expect("meta");
    assert_eq!(
        meta.cursor.epoch_history.last().map(|(e, _)| *e),
        Some(2),
        "LeaderAnnounced{{2}} applied"
    );
    assert_eq!(h.epoch(), Epoch(2));
    assert!(routable(&node, P0).await);
    assert_eq!(*node.roles().led().borrow(), Arc::new(vec![(P0, Epoch(2))]));
    assert!(asg.term_requests().is_empty());
    assert!(fol.events().is_empty(), "a leader never fetches");

    // Submits work at the meta epoch.
    let out = h
        .submit_one(Command::start_run(common::start_run(&common::run_on(0))))
        .await
        .expect("submit");
    assert_eq!(out, ostro_partition::ApplyOutcome::Accepted);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_to_follower_fails_acks_hints_and_starts_fetcher() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![view(0, 1, 1, &[1, 2], &[1], 1, 1)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_leader(&node, 0, 1).await;

    // Node 2 joins the ISR (AlterIsr): node 2 never fetches, so nothing commits now.
    asg.set(view(0, 1, 1, &[1, 2], &[1, 2], 2, 1));
    let h = node.handle(P0).expect("ready");
    p12_roles::eventually("ISR {1,2} adopted", || {
        h.log().replica_status().published_isr == vec![1, 2]
    })
    .await;
    let pending = {
        let h = h.clone();
        tokio::spawn(async move {
            h.submit_one(Command::start_run(common::start_run(&common::run_on(0))))
                .await
        })
    };
    sleep(Duration::from_millis(200)).await;
    assert!(!pending.is_finished(), "the submit waits for the HW");

    // Meta elects node 2 at epoch 2.
    asg.set(view(0, 2, 2, &[1, 2], &[2], 3, 1));
    let r = timeout(ROLE_T, pending)
        .await
        .expect("pending submit resolved")
        .expect("join");
    assert!(
        matches!(r, Err(PartitionError::NotLeader)),
        "pending ack fails NotLeader, got {r:?}"
    );
    wait_role(&node, 0, "Follower{2, 2}", |r| {
        *r == PartitionRole::Follower {
            epoch: Epoch(2),
            leader: 2,
        }
    })
    .await;
    assert_eq!(node.router().leader_hint(P0), Some(2));
    assert!(!routable(&node, P0).await, "a follower is not routable");
    assert!(node.roles().led().borrow().is_empty());
    assert_eq!(
        fol.events(),
        vec![FetcherEvent::Start {
            p: 0,
            epoch: 2,
            leader: 2
        }]
    );
    // New submits are refused (the log follows).
    let r = h
        .submit_one(Command::start_run(common::start_run(&common::run_on(0))))
        .await;
    assert!(r.is_err(), "a follower refuses submits: {r:?}");
    assert_eq!(
        h.log().replica_status().role,
        ostro_log::Role::Follower { epoch: Epoch(2) }
    );
    node.shutdown().await.expect("shutdown");
    assert!(
        matches!(fol.events().last(), Some(FetcherEvent::Stop { .. })),
        "shutdown stops the fetcher"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_to_leader_stops_fetcher_before_announcing() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![view(0, 2, 1, &[1, 2], &[2], 1, 1)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_role(&node, 0, "Follower{1, 2}", |r| {
        *r == PartitionRole::Follower {
            epoch: Epoch(1),
            leader: 2,
        }
    })
    .await;
    assert_eq!(node.router().leader_hint(P0), Some(2));

    asg.set(view(0, 1, 2, &[1, 2], &[1], 2, 1));
    wait_leader(&node, 0, 2).await;
    let ev = fol.events();
    assert_eq!(
        ev,
        vec![
            FetcherEvent::Start {
                p: 0,
                epoch: 1,
                leader: 2
            },
            FetcherEvent::Stop {
                p: 0,
                latest_epoch: None
            },
        ],
        "the fetcher stopped before LeaderAnnounced{{2}} was appended"
    );
    assert_eq!(term_started(&events_of(&node, 0)), vec![2]);
    assert_eq!(node.router().leader_hint(P0), None);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_enough_replicas_retries_until_isr_recovers() {
    let tmp = ostro_testkit::fs::tempdir();
    // min_insync 2 with ISR {1}: LeaderAnnounced is refused (NotEnoughReplicas).
    let asg = FakeAssignments::new(1, vec![view(0, 1, 1, &[1, 2], &[1], 1, 2)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_role(&node, 0, "Announcing{1}", |r| {
        *r == PartitionRole::Announcing { epoch: Epoch(1) }
    })
    .await;
    sleep(Duration::from_millis(300)).await;
    assert_eq!(
        *node.role(P0).unwrap().borrow(),
        PartitionRole::Announcing { epoch: Epoch(1) },
        "still taking over while the ISR is too small"
    );
    assert!(!routable(&node, P0).await, "unavailable while announcing");
    assert!(node.roles().led().borrow().is_empty());
    assert!(term_started(&events_of(&node, 0)).is_empty());

    // The ISR recovers (meta lowers min_insync for this test; same epoch, newer version).
    asg.set(view(0, 1, 1, &[1, 2], &[1], 2, 1));
    wait_leader(&node, 0, 1).await;
    assert_eq!(term_started(&events_of(&node, 0)), vec![1]);
    assert!(routable(&node, P0).await);
    assert!(asg.term_requests().is_empty());
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unplaced_or_unassigned_partitions_stay_offline() {
    let tmp = ostro_testkit::fs::tempdir();
    // p0: no assignment; p1: epoch 0, no leader; p2: epoch 0 naming this node.
    let asg = FakeAssignments::new(
        1,
        vec![
            view(1, 0, 0, &[1], &[], 0, 1),
            view(2, 1, 0, &[1], &[1], 0, 1),
        ],
    );
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 3, &asg, &fol)).await;
    sleep(Duration::from_millis(400)).await;
    for p in 0..3u16 {
        assert_eq!(
            *node.role(PartitionId(p)).unwrap().borrow(),
            PartitionRole::Offline,
            "p{p}"
        );
        let ev = events_of(&node, p);
        assert!(
            !ev.iter().any(|e| matches!(
                e,
                StartupEvent::LeaderServicesStarted { .. } | StartupEvent::TermStarted { .. }
            )),
            "p{p} runs no leader services: {ev:?}"
        );
        assert!(!routable(&node, PartitionId(p)).await);
        // The partition is open (replica services, barrier) even while Offline.
        assert!(node.handle(PartitionId(p)).is_some());
        assert!(node.barrier(PartitionId(p)).unwrap().is_open());
    }
    assert!(asg.term_requests().is_empty(), "no term request at epoch 0");
    assert!(fol.events().is_empty());
    // Placement (epoch 0 -> 1) makes p2 a leader.
    asg.set(solo(2, 1, 1));
    wait_leader(&node, 2, 1).await;
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_starts_follower_and_requests_a_fresh_term() {
    let tmp = ostro_testkit::fs::tempdir();
    let fol = FakeFollower::new();
    // Session 1: this node leads at epoch 1 (its own LeaderAnnounced{1} is the last term).
    {
        let asg = FakeAssignments::new(1, vec![solo(0, 1, 1)]);
        let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
        wait_leader(&node, 0, 1).await;
        node.shutdown().await.expect("shutdown");
    }
    // Session 2: meta now names node 2 at epoch 1: the restart follows, never leads first.
    {
        let asg = FakeAssignments::new(1, vec![view(0, 2, 1, &[1, 2], &[2], 2, 1)]);
        let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
        wait_role(&node, 0, "Follower{1, 2}", |r| {
            *r == PartitionRole::Follower {
                epoch: Epoch(1),
                leader: 2,
            }
        })
        .await;
        let roles: Vec<PartitionRole> = events_of(&node, 0)
            .into_iter()
            .filter_map(|e| match e {
                StartupEvent::RoleChanged { role, .. } => Some(role),
                _ => None,
            })
            .collect();
        assert_eq!(
            roles,
            vec![
                PartitionRole::Offline,
                PartitionRole::Follower {
                    epoch: Epoch(1),
                    leader: 2
                }
            ]
        );
        assert!(term_started(&events_of(&node, 0)).is_empty());
        node.shutdown().await.expect("shutdown");
    }
    // Session 3: meta still names this node at the old epoch 1 (meta behind the log): no
    // leadership at 1; a fresh term >= 2 is requested instead.
    {
        let asg = FakeAssignments::new(1, vec![solo(0, 1, 1)]);
        let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
        p12_roles::eventually("request_term(p0, 2)", || {
            asg.term_requests() == vec![(P0, Epoch(2))]
        })
        .await;
        sleep(Duration::from_millis(200)).await;
        assert_eq!(*node.role(P0).unwrap().borrow(), PartitionRole::Offline);
        assert!(term_started(&events_of(&node, 0)).is_empty());
        assert_eq!(asg.term_requests().len(), 1, "one request per assignment");
        // Meta grants the fresh term.
        asg.set(solo(0, 1, 2));
        wait_leader(&node, 0, 2).await;
        assert_eq!(term_started(&events_of(&node, 0)), vec![2]);
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_isr_keeps_the_leader_without_a_term_request() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![view(0, 1, 1, &[1, 2], &[1], 1, 1)]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_leader(&node, 0, 1).await;
    let h = node.handle(P0).expect("ready");

    asg.set(view(0, 1, 1, &[1, 2], &[1, 2], 2, 1));
    p12_roles::eventually("ISR {1,2} adopted", || {
        let s = h.log().replica_status();
        s.published_isr == vec![1, 2] && s.isr_version == 2
    })
    .await;
    asg.set(view(0, 1, 1, &[1, 2], &[1], 3, 1));
    p12_roles::eventually("ISR {1} adopted", || {
        h.log().replica_status().isr_version == 3
    })
    .await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        *node.role(P0).unwrap().borrow(),
        PartitionRole::Leader { epoch: Epoch(1) }
    );
    assert!(
        asg.term_requests().is_empty(),
        "AlterIsr never asks for a term"
    );
    assert_eq!(term_started(&events_of(&node, 0)), vec![1], "one term");
    node.shutdown().await.expect("shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_leader_steps_down_and_resigns_once() {
    let tmp = ostro_testkit::fs::tempdir();
    let asg = FakeAssignments::new(1, vec![solo(0, 1, 1), solo(1, 1, 1)]);
    let fol = FakeFollower::new();
    let cfg = roles_cfg(tmp.path(), 1, 2, &asg, &fol);
    let hooks = cfg.test_hooks.clone();
    let node = open(cfg).await;
    wait_leader(&node, 0, 1).await;
    wait_leader(&node, 1, 1).await;

    hooks.fail_partition(P0);
    wait_role(&node, 0, "Offline after failure", |r| {
        *r == PartitionRole::Offline
    })
    .await;
    p12_roles::eventually("resign(p0, 1)", || {
        asg.resignations() == vec![(P0, Epoch(1))]
    })
    .await;
    assert!(!routable(&node, P0).await);
    // Further assignment updates do not resurrect it nor resign again.
    asg.set(view(0, 1, 1, &[1], &[1], 2, 1));
    sleep(Duration::from_millis(200)).await;
    assert_eq!(*node.role(P0).unwrap().borrow(), PartitionRole::Offline);
    assert_eq!(asg.resignations().len(), 1, "exactly one resignation");
    assert!(asg.term_requests().is_empty());
    // The other partition (same node) is untouched: the hook is per node and partition.
    assert_eq!(
        *node.role(PartitionId(1)).unwrap().borrow(),
        PartitionRole::Leader { epoch: Epoch(1) }
    );
    assert_eq!(
        *node.roles().led().borrow(),
        Arc::new(vec![(PartitionId(1), Epoch(1))])
    );
    node.shutdown().await.expect("shutdown");
}

/// p12-wedge (10k campaign, seed 1741, fault ~641): a leader that self-demotes while
/// `Announcing` (its `LeaderAnnounced{e}` refused on `NotEnoughReplicas`, nothing appended
/// at `e`) must not wedge once metadata contact returns. Meta still names it leader at `e`;
/// the log's epoch cache is still below `e` (so `decide` says Lead), but the aborted term
/// left the log leading `e`, so `become_leader(e)` is `StaleEpoch`. The driver must ask
/// for a fresh term above `e` (never reuse `e`) and lead once meta grants it; meanwhile it
/// stays Offline and accepts no writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_demotion_while_announcing_requests_a_fresh_term() {
    let tmp = ostro_testkit::fs::tempdir();
    // ISR {1} < min_insync 2: the takeover at epoch 1 stays Announcing.
    let announcing = view(0, 1, 1, &[1, 2], &[1], 1, 2);
    let asg = FakeAssignments::new(1, vec![announcing.clone()]);
    let fol = FakeFollower::new();
    let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
    wait_role(&node, 0, "Announcing{1}", |r| {
        *r == PartitionRole::Announcing { epoch: Epoch(1) }
    })
    .await;
    let h = node.handle(P0).expect("ready");
    assert!(
        h.log().latest_epoch() < Some(Epoch(1)),
        "nothing appended at epoch 1 yet"
    );

    // Self-demotion: meta's view masked as leaderless (the cluster's views_of).
    asg.set(view(0, 0, 1, &[1, 2], &[1], 1, 2));
    wait_role(&node, 0, "Offline after self-demotion", |r| {
        *r == PartitionRole::Offline
    })
    .await;
    assert!(
        h.log().latest_epoch() < Some(Epoch(1)),
        "epoch 1 stayed empty"
    );

    // Contact back: meta still names this node leader at epoch 1, ISR {1}.
    asg.set(announcing);
    p12_roles::eventually("request_term(p0, 2)", || {
        asg.term_requests().contains(&(P0, Epoch(2)))
    })
    .await;
    assert!(
        asg.term_requests().iter().all(|(_, e)| *e >= Epoch(2)),
        "epoch 1 is never reused: {:?}",
        asg.term_requests()
    );
    assert_eq!(*node.role(P0).unwrap().borrow(), PartitionRole::Offline);
    assert!(
        !routable(&node, P0).await,
        "a demoted leader routes nothing"
    );
    let r = timeout(
        Duration::from_millis(800),
        h.submit_one(Command::start_run(common::start_run(&common::run_on(0)))),
    )
    .await
    .expect("a submit to the demoted leader is refused at once");
    assert!(r.is_err(), "a demoted leader accepts no writes: {r:?}");
    assert!(term_started(&events_of(&node, 0)).is_empty());

    // Meta grants the fresh term (and the ISR recovers): the partition leads again.
    asg.set(view(0, 1, 2, &[1, 2], &[1], 2, 1));
    wait_leader(&node, 0, 2).await;
    assert_eq!(term_started(&events_of(&node, 0)), vec![2]);
    assert!(routable(&node, P0).await);
    node.shutdown().await.expect("shutdown");
}

#[cfg(feature = "failpoints")]
mod failpoints {
    use super::*;

    /// 12-16 `isolated_leader_cannot_commit`: a demoted leader fails its pending submits and
    /// refuses new ones at once, even while its leader services take long to stop (they may
    /// wait out the dispatcher's grace for executor jobs).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn demotion_refuses_submits_before_the_services_stop() {
        let _scenario = fail::FailScenario::setup();
        let tmp = ostro_testkit::fs::tempdir();
        let asg = FakeAssignments::new(1, vec![view(0, 1, 1, &[1, 2], &[1], 1, 1)]);
        let fol = FakeFollower::new();
        let node = open(roles_cfg(tmp.path(), 1, 1, &asg, &fol)).await;
        wait_leader(&node, 0, 1).await;
        asg.set(view(0, 1, 1, &[1, 2], &[1, 2], 2, 1));
        let h = node.handle(P0).expect("ready");
        p12_roles::eventually("ISR {1,2} adopted", || {
            h.log().replica_status().published_isr == vec![1, 2]
        })
        .await;
        let pending = {
            let h = h.clone();
            tokio::spawn(async move {
                h.submit_one(Command::start_run(common::start_run(&common::run_on(0))))
                    .await
            })
        };
        sleep(Duration::from_millis(200)).await;
        assert!(!pending.is_finished(), "the submit waits for the HW");

        fail::cfg("repl::before_services_stop", "return(2000)").expect("cfg");
        // Self-demotion: meta's view masked as leaderless.
        asg.set(view(0, 0, 1, &[1, 2], &[1, 2], 2, 1));
        let r = timeout(Duration::from_millis(800), pending)
            .await
            .expect("pending submit fails before the services stopped")
            .expect("join");
        assert!(matches!(r, Err(PartitionError::NotLeader)), "{r:?}");
        let r = timeout(
            Duration::from_millis(800),
            h.submit_one(Command::start_run(common::start_run(&common::run_on(0)))),
        )
        .await
        .expect("a new submit is refused at once");
        assert!(r.is_err(), "{r:?}");
        assert!(node.roles().led().borrow().is_empty());
        fail::remove("repl::before_services_stop");
        wait_role(&node, 0, "Offline", |r| *r == PartitionRole::Offline).await;
        node.shutdown().await.expect("shutdown");
    }
}
