//! 12-10 Task 3: the partition controller on a real static metadata group (3 voters, or
//! one) with scripted liveness: bootstrap placement, fresh terms, ISR-only failover,
//! offline and return, resignation, and liveness seeding after a controller change.

mod support_controller;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ostro_cluster::bootstrap::placement;
use ostro_cluster::controller::ProposalRecord;
use ostro_cluster::controller::{self, ControllerConfig, FreshTerm};
use ostro_cluster::liveness::{Liveness, LivenessConfig, LivenessHandle};
use ostro_cluster::members;
use ostro_meta::ext::{
    ELECTION_REASON_FAILOVER, ELECTION_REASON_OFFLINE, ELECTION_REASON_RESIGN, ElectionInfo,
    get_election_info,
};
use ostro_meta::{MetaCommand, MetaResult};
use ostro_partition::SystemClock;
use support_controller::{FakeLiveness, Group, audit, partition_of};

const LONG: Duration = Duration::from_secs(20);

/// An applied `ElectLeader` record for partition `p`.
fn is_applied_elect(r: &ProposalRecord, p: u16) -> bool {
    matches!(r.cmd, MetaCommand::ElectLeader { .. })
        && matches!(r.result, Ok(MetaResult::Ok { .. }))
        && partition_of(&r.cmd) == p
}

fn cfg(rf: usize, min_insync: u8) -> ControllerConfig {
    ControllerConfig::new(rf, min_insync)
}

/// Place `partitions` on a fresh group with all nodes live; returns the liveness script.
async fn placed(g: &mut Group, partitions: u16, rf: usize) -> Arc<FakeLiveness> {
    g.init(partitions).await;
    let live = Arc::new(FakeLiveness::default());
    g.start_controllers(live.clone(), cfg(rf, 2));
    g.wait_for("placement", LONG, |a| a.iter().all(|a| a.epoch == 1))
        .await;
    live
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_three_node_cluster_places_every_partition_once() {
    let mut g = Group::new(3).await;
    let _live = placed(&mut g, 6, 3).await;
    let want = placement(&[1, 2, 3], 6, 3);
    for a in g.assignments().await {
        let r = &want[usize::from(a.partition)];
        assert_eq!(a.replicas, *r, "partition {}", a.partition);
        assert_eq!(a.replicas.len(), 3);
        assert_eq!(a.leader, r[0]);
        assert_eq!(a.isr, vec![r[0]]);
        assert_eq!((a.epoch, a.min_insync), (1, 2));
    }
    // Quiet afterwards: no re-placement, no election (every ISR member is live).
    tokio::time::sleep(Duration::from_millis(500)).await;
    let recs = g.records();
    audit(&recs);
    for p in 0..6 {
        let applied = g.applied_for(p);
        assert_eq!(applied.len(), 1, "partition {p}: {applied:?}");
        assert!(matches!(applied[0].cmd, MetaCommand::Put { .. }));
    }
    assert!(
        recs.iter()
            .all(|r| matches!(r.cmd, MetaCommand::Put { .. })),
        "{recs:?}"
    );

    // Member self-registration: written once, then only on change. The joiner is a node
    // with no member record yet: InitCluster already registered the founding node (the
    // meta leader at init, any of 1..=3) with joined_at_ms 1.
    let l = g.leader().await;
    let joiner = g
        .meta(l)
        .view()
        .read(|s| g.voters.iter().copied().find(|&n| s.member(n).is_none()))
        .expect("a voter without a member record");
    let me = ostro_meta::Member {
        node_id: joiner,
        addr: "127.0.0.1:7001".into(),
        joined_at_ms: 5,
        cluster_addr: Some("127.0.0.1:7002".into()),
        repl_addr: None,
        ext: Vec::new(),
    };
    assert!(
        members::upsert_self(g.meta(joiner), me.clone())
            .await
            .unwrap()
    );
    assert!(
        !members::upsert_self(g.meta(joiner), me.clone())
            .await
            .unwrap()
    );
    let moved = ostro_meta::Member {
        repl_addr: Some("127.0.0.1:7003".into()),
        joined_at_ms: 99,
        ..me.clone()
    };
    assert!(members::upsert_self(g.meta(l), moved).await.unwrap());
    let stored = g
        .meta(l)
        .view()
        .read(|s| s.member(joiner).cloned())
        .unwrap()
        .value;
    assert_eq!(stored.repl_addr.as_deref(), Some("127.0.0.1:7003"));
    assert_eq!(stored.joined_at_ms, 5, "joined_at is kept");
    // The founding member (InitCluster writes addr "") gets its address.
    let founder = ostro_meta::Member {
        node_id: l,
        addr: "10.0.0.1:7000".into(),
        ..Default::default()
    };
    assert!(members::upsert_self(g.meta(l), founder).await.unwrap());
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_term_is_a_noop_at_epoch_0_and_placement_survives_a_meta_leader_change() {
    let mut g = Group::new(3).await;
    g.init(8).await;
    let l = g.leader().await;
    let applied = g.meta(l).view().applied();
    for p in 0..8 {
        let r = controller::request_fresh_term(g.meta(l), l, p, 1, &SystemClock)
            .await
            .unwrap();
        assert_eq!(r, FreshTerm::Unplaced);
    }
    assert_eq!(
        g.meta(l).view().applied(),
        applied,
        "nothing proposed at epoch 0"
    );

    // Controllers start; the meta leader dies right after the first placement applies.
    let live = Arc::new(FakeLiveness::default());
    g.start_controllers(live.clone(), cfg(3, 2));
    let deadline = Instant::now() + LONG;
    while !g
        .records()
        .iter()
        .any(|r| matches!(r.result, Ok(MetaResult::Ok { .. })))
    {
        assert!(Instant::now() < deadline, "no placement");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    g.stop(l).await;
    let new = g.leader().await;
    assert_ne!(new, l);
    // Still nothing at epoch 0 for a fresh-term request, during the move as well.
    for a in g.assignments().await {
        if a.epoch == 0 {
            let r = controller::request_fresh_term(g.meta(new), new, a.partition, 1, &SystemClock)
                .await
                .unwrap();
            assert!(
                matches!(
                    r,
                    FreshTerm::Unplaced
                        | FreshTerm::NotLeader { .. }
                        | FreshTerm::AlreadyFresh { .. }
                ),
                "{r:?}"
            );
        }
    }
    let all = g
        .wait_for("placement after the leader change", LONG, |a| {
            a.iter().all(|a| a.epoch == 1)
        })
        .await;
    let want = placement(&[1, 2, 3], 8, 3);
    for a in &all {
        assert_eq!(a.replicas, want[usize::from(a.partition)]);
    }
    audit(&g.records());
    for p in 0..8 {
        let puts: Vec<_> = g
            .applied_for(p)
            .into_iter()
            .filter(|r| matches!(r.cmd, MetaCommand::Put { .. }))
            .collect();
        // The stopped controller's in-flight proposals were abandoned without a record
        // (one may still have applied), so the records give an upper bound; the CAS from
        // version 0 makes a second placement impossible, which the stored version shows.
        assert!(puts.len() <= 1, "partition {p} placed {} times", puts.len());
    }
    let l2 = g.leader().await;
    let versions: Vec<u64> = g
        .meta(l2)
        .view()
        .read(|s| s.partitions().map(|v| v.version).collect());
    assert_eq!(
        versions,
        vec![1; 8],
        "every partition written once at epoch 1"
    );
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_epoch_one_rf1_partitions_are_not_replaced() {
    let mut g = Group::new(3).await;
    g.init(4).await;
    let l = g.leader().await;
    // A pre-Phase-12 data dir: every partition led at epoch 1 with replicas [l].
    for p in 0..4 {
        let r = g
            .propose(MetaCommand::SetPartitionLeader {
                partition: p,
                epoch: 1,
                leader: l,
                isr: vec![l],
            })
            .await;
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }
    let live = Arc::new(FakeLiveness::default());
    g.start_controllers(live, cfg(3, 2));
    tokio::time::sleep(Duration::from_millis(800)).await;
    for a in g.assignments().await {
        assert_eq!((a.epoch, a.replicas.clone(), a.leader), (1, vec![l], l));
    }
    assert!(g.records().is_empty(), "{:?}", g.records());
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_places_self_and_takes_one_fresh_term_per_restart() {
    let mut g = Group::new(1).await;
    let live = placed(&mut g, 4, 3).await;
    for a in g.assignments().await {
        assert_eq!(
            (a.replicas.clone(), a.isr.clone(), a.leader, a.epoch),
            (vec![1], vec![1], 1, 1)
        );
        assert_eq!(a.min_insync, 1, "min_insync clamped to rf 1");
        // Fresh node: log empty, epoch 1 is already fresh.
        let r = controller::request_fresh_term(g.meta(1), 1, a.partition, 1, &SystemClock)
            .await
            .unwrap();
        assert_eq!(r, FreshTerm::AlreadyFresh { epoch: 1 });
    }
    for restart in 0..2u32 {
        g.stop(1).await;
        g.start_meta(1).await;
        g.start_controller(1, live.clone(), cfg(3, 2));
        let before = g.assignments().await;
        for a in &before {
            // The local log's latest epoch is the meta epoch it led at.
            let min = a.epoch + 1;
            let r = controller::request_fresh_term(g.meta(1), 1, a.partition, min, &SystemClock)
                .await
                .unwrap();
            assert_eq!(
                r,
                FreshTerm::Elected { epoch: a.epoch + 1 },
                "restart {restart}"
            );
            // A repeated call is idempotent.
            let r = controller::request_fresh_term(g.meta(1), 1, a.partition, min, &SystemClock)
                .await
                .unwrap();
            assert_eq!(r, FreshTerm::AlreadyFresh { epoch: a.epoch + 1 });
        }
        for (a, b) in before.iter().zip(g.assignments().await) {
            assert_eq!(b.epoch, a.epoch + 1);
            assert_eq!(b.isr_version, a.isr_version + 1, "exactly one ElectLeader");
            assert_eq!((b.leader, b.isr.clone()), (1, vec![1]));
        }
    }
    // A node that meta does not name leader, or that is outside the ISR, proposes nothing.
    let applied = g.meta(1).view().applied();
    let r = controller::request_fresh_term(g.meta(1), 9, 0, 10, &SystemClock)
        .await
        .unwrap();
    assert_eq!(r, FreshTerm::NotLeader { leader: 1 });
    let a = g.assignment(3).await;
    let r = g
        .propose(MetaCommand::SetPartitionLeader {
            partition: 3,
            epoch: a.epoch + 1,
            leader: 9,
            isr: vec![1],
        })
        .await;
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    let applied2 = g.meta(1).view().applied();
    assert!(applied2 > applied);
    let r = controller::request_fresh_term(g.meta(1), 9, 3, a.epoch + 5, &SystemClock)
        .await
        .unwrap();
    assert_eq!(r, FreshTerm::NotInIsr);
    assert_eq!(g.meta(1).view().applied(), applied2, "nothing proposed");
    // The controller itself never re-places or elects on the single node.
    audit(&g.records());
    assert!(
        g.records()
            .iter()
            .all(|r| matches!(r.cmd, MetaCommand::Put { .. })),
        "{:?}",
        g.records()
    );
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dead_leader_fails_over_to_the_first_live_isr_member_only() {
    let mut g = Group::new(3).await;
    let live = placed(&mut g, 6, 3).await;
    // Followers caught up: full ISR, except partition 0 where replicas[1] never joined.
    for a in g.assignments().await {
        let isr = if a.partition == 0 {
            vec![a.replicas[0], a.replicas[2]]
        } else {
            a.replicas.clone()
        };
        g.expand_isr(a.partition, isr).await;
    }
    let before = g.assignments().await;
    let victim = before[0].leader; // placement: partition 0 is led by node 1
    let t0 = Instant::now();
    live.kill(victim);
    let after = g
        .wait_for("failover", LONG, |a| {
            a.iter()
                .all(|a| a.leader != victim && !a.isr.contains(&victim))
        })
        .await;
    let took = t0.elapsed();
    eprintln!("liveness dead -> every partition re-led / shrunk: {took:?}");
    assert!(took < Duration::from_millis(500), "{took:?}");
    for (b, a) in before.iter().zip(&after) {
        let want_isr: Vec<u64> = b.isr.iter().copied().filter(|&n| n != victim).collect();
        assert_eq!(a.isr, want_isr, "partition {}", a.partition);
        if b.leader == victim {
            // First live ISR member in replicas order.
            let want = *b
                .replicas
                .iter()
                .find(|n| **n != victim && b.isr.contains(n))
                .unwrap();
            assert_eq!(a.leader, want, "partition {}", a.partition);
            assert_eq!(a.epoch, b.epoch + 1);
            let info = get_election_info(&a.ext).unwrap().unwrap();
            assert_eq!(
                (info.reason, info.prev_leader),
                (ELECTION_REASON_FAILOVER, victim)
            );
        } else {
            // A dead follower only shrinks the ISR (AlterIsr keeps the epoch).
            assert_eq!(
                (a.leader, a.epoch),
                (b.leader, b.epoch),
                "partition {}",
                a.partition
            );
        }
    }
    // Partition 0: replicas[1] is live and preferred but outside the ISR: never chosen.
    assert_eq!(after[0].leader, before[0].replicas[2]);
    assert_ne!(after[0].leader, before[0].replicas[1]);
    // The records trail the meta state the wait above observed.
    let failed_over: Vec<u16> = before
        .iter()
        .filter(|b| b.leader == victim)
        .map(|b| b.partition)
        .collect();
    let recs = g
        .wait_records("the failover ElectLeader records", LONG, |r| {
            failed_over
                .iter()
                .all(|&p| r.iter().any(|x| is_applied_elect(x, p)))
        })
        .await;
    audit(&recs);
    // Exactly one applied ElectLeader per failed-over partition (one controller acts).
    for b in &before {
        let elects = g
            .applied_for(b.partition)
            .into_iter()
            .filter(|r| matches!(r.cmd, MetaCommand::ElectLeader { .. }))
            .count();
        assert_eq!(
            elects,
            usize::from(b.leader == victim),
            "partition {}",
            b.partition
        );
    }
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_isr_dead_goes_offline_then_the_last_member_returns() {
    let mut g = Group::new(3).await;
    let live = placed(&mut g, 3, 3).await;
    // Partition 1 keeps ISR [leader] (placement default); its leader dies.
    let b = g.assignment(1).await;
    assert_eq!(b.isr, vec![b.leader]);
    live.kill(b.leader);
    let a = g
        .wait_for("offline", LONG, |a| a[1].leader == 0)
        .await
        .remove(1);
    assert_eq!(
        (a.epoch, a.isr.clone()),
        (b.epoch + 1, vec![b.leader]),
        "keeps the last member"
    );
    let info = get_election_info(&a.ext).unwrap().unwrap();
    assert_eq!(info.reason, ELECTION_REASON_OFFLINE);
    // Stays offline (no unclean election of a live non-ISR replica), proposed only once.
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(g.assignment(1).await.leader, 0);
    let offline = g
        .records()
        .into_iter()
        .filter(|r| {
            matches!(
                r.cmd,
                MetaCommand::ElectLeader {
                    partition: 1,
                    leader: 0,
                    ..
                }
            )
        })
        .count();
    assert_eq!(offline, 1);
    // The member returns: re-elected.
    live.revive(b.leader);
    let a = g
        .wait_for("re-election", LONG, |a| a[1].leader != 0)
        .await
        .remove(1);
    assert_eq!(
        (a.leader, a.epoch, a.isr.clone()),
        (b.leader, b.epoch + 2, vec![b.leader])
    );
    audit(&g.records());
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resign_elects_the_next_isr_member_and_a_sole_member_goes_offline() {
    let mut g = Group::new(3).await;
    let _live = placed(&mut g, 3, 3).await;
    let full = g.assignment(0).await;
    g.expand_isr(0, full.replicas.clone()).await;
    let b = g.assignment(0).await;
    let l = g.leader().await;
    let r = controller::resign(g.meta(l), b.leader, 0, b.epoch, &SystemClock)
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    let a = g
        .wait_for("election after resign", LONG, |a| {
            a[0].leader != 0 && a[0].epoch == b.epoch + 2
        })
        .await
        .remove(0);
    assert_eq!(a.leader, b.replicas[1], "next ISR member in replicas order");
    assert!(!a.isr.contains(&b.leader));
    let info = get_election_info(&a.ext).unwrap().unwrap();
    assert_eq!(
        (info.reason, info.prev_leader),
        (ELECTION_REASON_RESIGN, b.leader)
    );

    // Partition 2 has ISR [leader] only: resigning leaves it offline with that member.
    let s = g.assignment(2).await;
    let r = controller::resign(g.meta(l), s.leader, 2, s.epoch, &SystemClock)
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    tokio::time::sleep(Duration::from_millis(800)).await;
    let a = g.assignment(2).await;
    assert_eq!(
        (a.leader, a.epoch, a.isr.clone()),
        (0, s.epoch + 1, vec![s.leader])
    );
    audit(&g.records());
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_controller_seeds_suspicion_and_does_not_elect_on_its_first_sample() {
    let session_timeout = Duration::from_millis(1500);
    let lcfg = LivenessConfig::new(session_timeout);
    let mut g = Group::new(3).await;
    g.init(6).await;
    for id in g.running() {
        let live = LivenessHandle::new(Liveness::new(Arc::new(SystemClock), session_timeout));
        g.start_controller(id, Arc::new(live), cfg(3, 2));
    }
    g.wait_for("placement", LONG, |a| a.iter().all(|a| a.epoch == 1))
        .await;
    for a in g.assignments().await {
        g.expand_isr(a.partition, a.replicas.clone()).await;
    }
    // Real liveness: everyone stays live while all nodes run.
    tokio::time::sleep(session_timeout * 2).await;
    assert!(g.assignments().await.iter().all(|a| a.epoch == 1));

    // The meta leader (also a partition leader) dies; a new controller takes over.
    let old = g.leader().await;
    let led: Vec<u16> = g
        .assignments()
        .await
        .iter()
        .filter(|a| a.leader == old)
        .map(|a| a.partition)
        .collect();
    assert!(!led.is_empty());
    let stop_at = Instant::now();
    g.stop(old).await;
    let new = g.leader().await;
    let took_over = Instant::now();
    g.wait_for("failover by the new controller", LONG, |a| {
        a.iter().all(|a| a.leader != old)
    })
    .await;
    let elected = Instant::now();
    eprintln!(
        "meta leader {old} stopped; new controller {new} after {:?}; partitions re-led {:?} after takeover ({:?} after the stop)",
        took_over - stop_at,
        elected - took_over,
        elected - stop_at
    );
    // Seeding: a node absent from the first seed_window of samples, then confirmed.
    assert!(
        elected - took_over >= lcfg.seed_window - Duration::from_millis(20),
        "elected {:?} after takeover, before the seeding window {:?}",
        elected - took_over,
        lcfg.seed_window
    );
    // The records trail the meta state the wait above observed.
    let recs = g
        .wait_records("the takeover ElectLeader records", LONG, |r| {
            led.iter()
                .all(|&p| r.iter().any(|x| is_applied_elect(x, p)))
        })
        .await;
    audit(&recs);
    for p in led {
        let elects: Vec<_> = g
            .applied_for(p)
            .into_iter()
            .filter(|r| matches!(r.cmd, MetaCommand::ElectLeader { .. }))
            .collect();
        assert_eq!(elects.len(), 1, "partition {p}: {elects:?}");
        assert_eq!(elects[0].proposer, new);
    }
    g.stop_all().await;
}

/// `elect::before_propose` (RESERVATIONS §11): an injected failure before every proposal
/// leaves the partitions unplaced; once removed, the next tick places them.
#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn elect_before_propose_failpoint_blocks_proposals_until_removed() {
    let mut g = Group::new(1).await;
    g.init(2).await;
    fail::cfg("elect::before_propose", "return").unwrap();
    let live = Arc::new(FakeLiveness::default());
    g.start_controllers(live, cfg(1, 1));
    tokio::time::sleep(Duration::from_millis(400)).await;
    let blocked = g.assignments().await.iter().all(|a| a.epoch == 0);
    let failed = g
        .records()
        .iter()
        .filter(|r| {
            r.result
                .as_ref()
                .is_err_and(|e| e.contains("elect::before_propose"))
        })
        .count();
    fail::remove("elect::before_propose");
    assert!(blocked, "placed despite the failpoint");
    assert!(failed > 0, "{:?}", g.records());
    g.wait_for("placement after removal", LONG, |a| {
        a.iter().all(|a| a.epoch == 1)
    })
    .await;
    audit(&g.records());
    g.stop_all().await;
}

/// 12-16 `isolated_leader_rejoin_truncates`: a partition leader cut off from the metadata
/// group still sees itself leading at the old epoch with the old ISR. When it asks for a
/// fresh term on rejoining, the decision must come from the current view (another node was
/// elected meanwhile and the ISR shrank without it): no `ElectLeader` planned from the stale
/// ISR is ever proposed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_term_after_a_cut_decides_from_the_current_view() {
    let mut g = Group::new(3).await;
    let _live = placed(&mut g, 3, 3).await;
    for a in g.assignments().await {
        g.expand_isr(a.partition, a.replicas.clone()).await;
    }
    let ml = g.leader().await;
    let a = g
        .assignments()
        .await
        .into_iter()
        .find(|a| a.leader != ml)
        .expect("a partition led by a node other than the meta leader");
    let (p, x) = (a.partition, a.leader);
    g.cut(x);
    let y = *a.replicas.iter().find(|r| **r != x).unwrap();
    let isr: Vec<u64> = a.isr.iter().copied().filter(|r| *r != x).collect();
    let r = g
        .meta(ml)
        .propose(MetaCommand::ElectLeader {
            partition: p,
            epoch: a.epoch + 1,
            leader: y,
            isr: isr.clone(),
            expected_isr_version: a.isr_version,
            info: ElectionInfo {
                reason: ELECTION_REASON_FAILOVER,
                elected_at_ms: 1,
                prev_leader: x,
            },
        })
        .await
        .unwrap();
    assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    // The cut node's view is stale: it still leads at the old epoch.
    let seen = g
        .meta(x)
        .view()
        .read(|s| s.partition(p).map(|v| v.value.clone()));
    assert_eq!(seen.map(|v| (v.leader, v.epoch)), Some((x, a.epoch)));

    let mx = g.meta_arc(x);
    let req = tokio::spawn(async move {
        controller::request_fresh_term(&mx, x, p, a.epoch + 1, &SystemClock).await
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    g.heal(x);
    let r = tokio::time::timeout(LONG, req)
        .await
        .expect("fresh term request finishes after the heal")
        .unwrap()
        .unwrap();
    assert_eq!(r, FreshTerm::NotLeader { leader: y });
    let stale: Vec<_> = g
        .forwarded
        .lock()
        .unwrap()
        .iter()
        .filter(|(from, c)| *from == x && matches!(c, MetaCommand::ElectLeader { .. }))
        .cloned()
        .collect();
    assert!(stale.is_empty(), "stale ElectLeader proposed: {stale:?}");
    let now = g.assignment(p).await;
    assert_eq!((now.leader, now.epoch, now.isr), (y, a.epoch + 1, isr));
    audit(&g.records());
    g.stop_all().await;
}
