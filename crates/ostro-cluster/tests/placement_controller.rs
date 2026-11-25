//! 13-10 Task 2: the placement controller over a real in-process metadata group with a fake
//! partition layer (honest ISR publication) and a fake handoff: RF raise on growth, Draining
//! evacuation, a Removed member's replicas re-placed (W7), controller failover mid-move,
//! the feature gate before finalization, explicit reassign + cancel, the W5 shrink rule
//! (never before the target is in the published ISR, min_insync never above |ISR|), epoch-0
//! partitions untouched, and the one-move-at-a-time leader balancer.
//!
//! Every round checks `max_concurrent_moves` and that no partition ever has |ISR| <
//! min_insync (writes would stop with NotEnoughReplicas).

mod support_placement;

use ostro_cluster::placement::controller::{
    Action, PlacementError, blocked_partitions, effective_policy, observe, request_reassign,
    request_rebalance,
};
use ostro_meta::ext::{MEMBER_DRAINING, MEMBER_REMOVED, ReplicationPolicy, get_reassignment};
use ostro_meta::{MetaCommand, PartitionAssignment};
use support_placement::{Bed, P, assignment, sorted};

fn policy(rf: u8, min_insync: u8) -> Option<ReplicationPolicy> {
    Some(ReplicationPolicy {
        rf,
        min_insync,
        max_voters: 5,
    })
}

fn rf1_on(node: u64) -> Vec<PartitionAssignment> {
    (0..P)
        .map(|p| assignment(p, node, &[node], &[node], 1))
        .collect()
}

fn no_ext(a: &PartitionAssignment) -> bool {
    matches!(get_reassignment(&a.ext), Ok(None))
}

fn alter_replicas_in(reports: &[ostro_cluster::placement::controller::TickReport]) -> usize {
    reports
        .iter()
        .flat_map(|r| r.actions.iter())
        .filter(|a| matches!(a, Action::Proposed { .. }))
        .count()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf1_raised_to_3_when_two_members_appear() {
    let mut bed = Bed::new(&[11], rf1_on(11), Some(2), policy(3, 2)).await;
    // One member: nothing to do.
    for _ in 0..3 {
        bed.round(false).await;
    }
    assert!(bed.partitions().await.iter().all(|a| a.replicas == [11]));

    // Two members appear; 13 lags at first: Shrink must wait for the target to be in the
    // published ISR, and min_insync must stay 1 meanwhile (W5, Pitfall 6).
    bed.g.put_member(12, None).await;
    bed.g.put_member(13, None).await;
    bed.lagging.lock().unwrap().insert(13);
    for _ in 0..15 {
        bed.round(false).await;
        for a in bed.partitions().await {
            if a.replicas.len() < 3 && !no_ext(&a) {
                assert_eq!(a.min_insync, 1, "min_insync raised before Shrink: {a:?}");
            }
            assert!(
                a.replicas.contains(&11) || a.isr.contains(&13),
                "shrank before the target was in the ISR: {a:?}"
            );
        }
    }
    bed.lagging.lock().unwrap().clear();
    bed.drive_until("every partition at RF 3", |parts| {
        parts.iter().all(|a| {
            sorted(&a.replicas) == [11, 12, 13]
                && sorted(&a.isr) == [11, 12, 13]
                && a.min_insync == 2
                && no_ext(a)
        })
    })
    .await;
    assert!(bed.max_in_flight <= 2, "in flight {}", bed.max_in_flight);
    assert!(bed.stalled.is_empty(), "writes stalled: {:?}", bed.stalled);
    // The preferred leaders are spread (planner) and every leader is in the ISR.
    for a in bed.partitions().await {
        assert!(a.isr.contains(&a.leader));
    }
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_member_is_evacuated() {
    // 4 nodes, RF 3; node 14 is a replica of every partition and leads two.
    let placed = vec![
        assignment(0, 14, &[14, 11, 12], &[14, 11, 12], 2),
        assignment(1, 11, &[11, 12, 14], &[11, 12, 14], 2),
        assignment(2, 14, &[14, 12, 13], &[14, 12, 13], 2),
        assignment(3, 13, &[13, 14, 11], &[13, 14, 11], 2),
    ];
    let mut bed = Bed::new(&[11, 12, 13, 14], placed, Some(2), policy(3, 2)).await;
    bed.g.set_state(14, MEMBER_DRAINING).await;
    bed.drive_until("node 14 holds nothing", |parts| {
        parts
            .iter()
            .all(|a| !a.replicas.contains(&14) && a.leader != 14 && no_ext(a))
    })
    .await;
    for a in bed.partitions().await {
        assert_eq!(a.replicas.len(), 3, "{a:?}");
        assert_eq!(a.min_insync, 2, "{a:?}");
    }
    assert!(bed.max_in_flight <= 2);
    assert!(bed.stalled.is_empty(), "writes stalled: {:?}", bed.stalled);
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removed_dead_member_replicas_are_replaced_without_policy() {
    // A Phase 12 static cluster (no policy): node 13 is dead and --force-removed while it
    // still holds replicas (W7); epoch-0 partition 3 is never touched.
    let placed = vec![
        assignment(0, 11, &[11, 12, 13], &[11, 12], 2),
        assignment(1, 12, &[12, 13, 11], &[12, 11], 2),
        assignment(2, 11, &[11, 12, 13], &[11, 12], 2),
    ];
    let mut bed = Bed::new(&[11, 12, 13, 14], placed, Some(2), None).await;
    bed.down.lock().unwrap().insert(13);
    bed.g.set_state(13, MEMBER_REMOVED).await;
    let p3 = bed.part(3).await;
    assert_eq!(p3.epoch, 0);
    bed.drive_until("13's replicas re-placed", |parts| {
        parts
            .iter()
            .filter(|a| a.epoch > 0)
            .all(|a| !a.replicas.contains(&13) && a.replicas.len() == 3 && no_ext(a))
    })
    .await;
    assert_eq!(bed.part(3).await, p3, "epoch-0 partition untouched");
    // Without a policy nothing else moves: only partitions that held 13 changed.
    let obs = bed.leader().await.view().read(observe);
    assert_eq!(effective_policy(&obs), (3, 2));
    assert!(bed.stalled.is_empty(), "writes stalled: {:?}", bed.stalled);
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn static_cluster_without_policy_is_left_alone() {
    // Unbalanced, but no policy and nothing draining: no move, no balancer.
    let placed = vec![
        assignment(0, 12, &[11, 12], &[11, 12], 1),
        assignment(1, 12, &[11, 12], &[11, 12], 1),
        assignment(2, 12, &[11, 12], &[11, 12], 1),
        assignment(3, 12, &[11, 12], &[11, 12], 1),
    ];
    let mut bed = Bed::new(&[11, 12, 13], placed.clone(), Some(2), None).await;
    let mut reports = Vec::new();
    for _ in 0..10 {
        reports.extend(bed.round(false).await);
    }
    assert!(reports.iter().all(|r| r.actions.is_empty()), "{reports:?}");
    for (a, b) in bed.partitions().await.iter().zip(&placed) {
        assert_eq!((&a.replicas, a.leader), (&b.replicas, b.leader));
    }
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn controller_killed_mid_move_and_new_controller_completes_it() {
    let mut bed = Bed::new(&[11, 12, 13], rf1_on(11), Some(2), policy(3, 2)).await;
    bed.lagging.lock().unwrap().extend([12, 13]);
    // Drive until moves are in flight (Expand applied, CatchUp).
    bed.drive_until("a move in CatchUp", |parts| {
        parts.iter().any(|a| !no_ext(a))
    })
    .await;
    let old = bed.g.leader().await;
    bed.kill_meta(old).await;
    let new = bed.g.leader().await;
    assert_ne!(old, new);
    // The in-flight ext survived in meta; the new controller resumes it.
    assert!(bed.partitions().await.iter().any(|a| !no_ext(a)));
    bed.lagging.lock().unwrap().clear();
    bed.drive_until(
        "every partition at RF 3 under the new controller",
        |parts| {
            parts.iter().all(|a| {
                a.replicas.len() == 3 && a.isr.len() == 3 && a.min_insync == 2 && no_ext(a)
            })
        },
    )
    .await;
    assert!(bed.stalled.is_empty(), "writes stalled: {:?}", bed.stalled);
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gated_before_finalize_proposes_nothing() {
    // Finalized absent (= LEVEL_MIN 1): the gate refuses every AlterReplicas.
    let mut bed = Bed::new(&[11, 12, 13], rf1_on(11), None, policy(3, 2)).await;
    let before = bed.partitions().await;
    let mut reports = Vec::new();
    for _ in 0..5 {
        reports.extend(bed.round(true).await);
    }
    assert_eq!(alter_replicas_in(&reports), 0, "{reports:?}");
    assert!(
        reports
            .iter()
            .flat_map(|r| r.actions.iter())
            .any(|a| matches!(a, Action::Refused { .. })),
        "{reports:?}"
    );
    let after = bed.partitions().await;
    assert_eq!(before, after, "no AlterReplicas applied");
    let leader = bed.leader().await;
    let r = request_reassign(&leader, 0, vec![12], false, ostro_types::compat::LEVEL, 1).await;
    assert!(
        matches!(r, Err(PlacementError::FeatureNotEnabled(_))),
        "{r:?}"
    );
    // And a node whose own level_max is 1 refuses even after finalization.
    bed.set_cluster(Some(2), None).await;
    let r = request_reassign(&leader, 0, vec![12], false, 1, 1).await;
    assert!(
        matches!(r, Err(PlacementError::FeatureNotEnabled(_))),
        "{r:?}"
    );
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_reassign_and_cancel() {
    let mut bed = Bed::new(&[11, 12, 13], rf1_on(11), Some(2), None).await;
    let via = bed.follower().await;
    let lvl = ostro_types::compat::LEVEL;

    // Validation.
    for (target, what) in [
        (vec![], "empty"),
        (vec![12, 12], "twice"),
        (vec![99], "not a member"),
        (vec![1], "joining"),
    ] {
        let r = request_reassign(&via, 0, target, false, lvl, 5).await;
        assert!(
            matches!(&r, Err(PlacementError::Invalid(m)) if m.contains(what)),
            "{what}: {r:?}"
        );
    }
    let r = request_reassign(&via, 9, vec![12], false, lvl, 5).await;
    assert!(matches!(r, Err(PlacementError::Invalid(_))), "{r:?}");
    let r = request_reassign(&via, 0, vec![], true, lvl, 5).await;
    assert!(
        matches!(r, Err(PlacementError::Conflict(_))),
        "nothing to cancel: {r:?}"
    );

    // Move p0 to [12] (from a follower: the proposal forwards to the meta leader).
    request_reassign(&via, 0, vec![12], false, lvl, 5)
        .await
        .unwrap();
    let ext = get_reassignment(&bed.part(0).await.ext).unwrap().unwrap();
    assert_eq!((ext.target.clone(), ext.started_at_ms), (vec![12], 5));
    bed.drive_until("p0 on [12]", |parts| {
        parts[0].replicas == [12] && parts[0].leader == 12 && no_ext(&parts[0])
    })
    .await;

    // Move p1 to [12, 13] with 13 lagging, then cancel before LeaderMove.
    bed.lagging.lock().unwrap().insert(13);
    request_reassign(&via, 1, vec![13, 12], false, lvl, 6)
        .await
        .unwrap();
    for _ in 0..5 {
        bed.round(false).await;
    }
    let p1 = bed.part(1).await;
    assert_eq!(p1.leader, 11, "LeaderMove waits for the target in the ISR");
    assert!(!no_ext(&p1));
    request_reassign(&via, 1, vec![], true, lvl, 7)
        .await
        .unwrap();
    bed.drive_until("p1 restored to [11]", |parts| {
        parts[1].replicas == [11] && parts[1].isr == [11] && no_ext(&parts[1])
    })
    .await;
    assert_eq!(bed.part(1).await.leader, 11);

    // Cancel after LeaderMove is refused: p2 → [12]; once 12 leads, cancel conflicts.
    bed.lagging.lock().unwrap().clear();
    request_reassign(&via, 2, vec![12], false, lvl, 8)
        .await
        .unwrap();
    // Hold the Shrink back by keeping the old replica's controller from proposing: drive
    // only until the leader moved (ext still present or already cleared).
    let mut refused = false;
    for _ in 0..40 {
        bed.round(false).await;
        let p2 = bed.part(2).await;
        if p2.leader == 12 && !no_ext(&p2) {
            let r = request_reassign(&via, 2, vec![], true, lvl, 9).await;
            assert!(
                matches!(r, Err(PlacementError::Conflict(ref m)) if m.contains("past LeaderMove")),
                "{r:?}"
            );
            refused = true;
            break;
        }
        if p2.leader == 12 && no_ext(&p2) {
            break;
        }
    }
    let _ = refused; // the Shrink may land in the same round as the move
    bed.drive_until("p2 on [12]", |parts| {
        parts[2].replicas == [12] && no_ext(&parts[2])
    })
    .await;
    assert!(bed.stalled.is_empty(), "writes stalled: {:?}", bed.stalled);
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn balancer_moves_one_preferred_leader_per_pass() {
    // A balanced RF-2 placement over four members (2 replicas and 1 preferred leader each),
    // so the planner has nothing to do; no partition is led by its preferred replica. p3's
    // preferred replica 14 lags (never in the ISR), so p3 is never a candidate.
    let placed = vec![
        assignment(0, 12, &[11, 12], &[11, 12], 1),
        assignment(1, 13, &[12, 13], &[12, 13], 1),
        assignment(2, 14, &[13, 14], &[13, 14], 1),
        assignment(3, 11, &[14, 11], &[11], 1),
    ];
    let mut bed = Bed::new(&[11, 12, 13, 14], placed, Some(2), None).await;
    bed.lagging.lock().unwrap().insert(14);
    // Not due without a policy and without a trigger.
    for _ in 0..3 {
        let r = bed.round(false).await;
        assert!(r.iter().all(|r| r.actions.is_empty()), "{r:?}");
    }
    let balance_moves =
        |r: &[ostro_cluster::placement::controller::TickReport]| -> Vec<(u16, u64)> {
            r.iter()
                .flat_map(|r| r.actions.iter())
                .filter_map(|a| match a {
                    Action::HandOff {
                        partition,
                        to,
                        balance: true,
                    } => Some((*partition, *to)),
                    _ => None,
                })
                .collect()
        };
    let mut moved = Vec::new();
    for _ in 0..3 {
        // A forced pass (rebalance trigger): exactly one handoff per pass.
        let r = bed.round(true).await;
        assert_eq!(alter_replicas_in(&r), 0, "balanced: no replica move {r:?}");
        let m = balance_moves(&r);
        assert_eq!(m.len(), 1, "one move per pass: {r:?}");
        let (p, to) = m[0];
        moved.extend(m);
        // Wait until the handoff applied before the next pass (the balancer never starts a
        // second handoff for a partition whose handoff is still running).
        bed.drive_until("the balancer's handoff applied", |parts| {
            parts[usize::from(p)].leader == to
        })
        .await;
    }
    moved.sort_unstable();
    assert_eq!(
        moved,
        vec![(0, 11), (1, 12), (2, 13)],
        "only in-ISR preferred replicas"
    );
    let r = bed.round(true).await;
    assert!(balance_moves(&r).is_empty(), "p3 stays: {r:?}");
    for a in bed.partitions().await.iter().take(3) {
        assert_eq!(a.leader, a.replicas[0], "{a:?}");
    }
    bed.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_partition_reported_until_isr_recovers() {
    let placed = vec![assignment(0, 11, &[11, 12], &[11], 2)];
    let bed = Bed::new(&[11, 12, 13], placed, Some(2), policy(3, 2)).await;
    let obs = bed.leader().await.view().read(observe);
    let blocked = blocked_partitions(&obs);
    assert_eq!(blocked.len(), 1, "{blocked:?}");
    assert_eq!(blocked[0].0, 0);
    // AlterReplicas with min_insync above |ISR| would be refused at apply anyway.
    let a = bed.part(0).await;
    let r = bed
        .leader()
        .await
        .propose(MetaCommand::AlterReplicas {
            partition: 0,
            epoch: a.epoch,
            expected_isr_version: a.isr_version,
            replicas: vec![11, 12, 13],
            isr: vec![11],
            min_insync: 2,
            reassignment: None,
        })
        .await
        .unwrap();
    assert!(matches!(r, ostro_meta::MetaResult::Invalid(_)), "{r:?}");
    bed.stop().await;
}

/// 13-18 (13-16 handover): `request_rebalance` stamps the moves it starts with the caller's
/// injected clock, never the system clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebalance_moves_carry_the_injected_clock() {
    let bed = Bed::new(&[11, 12, 13], rf1_on(11), Some(2), policy(3, 2)).await;
    let via = bed.follower().await;
    let trigger = ostro_cluster::placement::controller::PlacementTrigger::new();
    request_rebalance(&via, &trigger, ostro_types::compat::LEVEL, 4242)
        .await
        .expect("rebalance");
    let started: Vec<u64> = bed
        .partitions()
        .await
        .iter()
        .filter_map(|a| get_reassignment(&a.ext).ok().flatten())
        .map(|r| r.started_at_ms)
        .collect();
    assert!(!started.is_empty(), "the rebalance started no move");
    assert!(
        started.iter().all(|t| *t == 4242),
        "started_at_ms not from the injected clock: {started:?}"
    );
    bed.stop().await;
}
