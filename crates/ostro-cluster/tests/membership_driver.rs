//! 13-09 Task 3: the membership driver (`MembershipService::tick`, D-P13-8) on in-process
//! metadata groups: learner → voter promotion, 4 members → 3 voters + 1 learner, a voter
//! replaced through one joint change, leave-joint after the leader died mid-replace,
//! removal of a node holding no replicas (5 → 3), the refusal when quorum would break and
//! the orphan joiner tombstone. Every applied configuration is checked to be odd (both
//! halves of a joint one).

mod support_membership;

use std::time::Duration;

use ostro_cluster::membership::service::TickOutcome;
use ostro_meta::ext::{MEMBER_ACTIVE, MEMBER_DRAINING, MEMBER_REMOVED};

use support_membership::*;

const T: Duration = Duration::from_secs(30);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn learner_promoted_to_voter() {
    let mut g = Group::new(&[1]).await;
    g.join(2).await;
    g.join(3).await;
    g.drive_until("1 -> 3 voters", T, |st| {
        sorted(&st.voters) == [1, 2, 3] && st.voters_outgoing.is_empty()
    })
    .await;
    assert_eq!(g.state_of(2), Some(MEMBER_ACTIVE));
    assert_eq!(g.state_of(3), Some(MEMBER_ACTIVE));
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_members_are_three_voters_and_a_learner() {
    let mut g = Group::new(&[1, 2, 3]).await;
    g.join(4).await;
    g.drive_until("4 active learner", T, |st| st.learners == [4])
        .await;
    g.drive_until("4 active", T, |_| g.state_of(4) == Some(MEMBER_ACTIVE))
        .await;
    // Stable: more ticks change nothing (an even voter set never appears).
    for _ in 0..20 {
        let did = g.tick().await;
        assert!(did.is_empty(), "{did:?}");
    }
    let st = g.leader_handle().await.raft_detail();
    assert_eq!(sorted(&st.voters), [1, 2, 3]);
    assert_eq!(st.learners, [4]);
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn voter_replaced_through_one_joint_change() {
    let mut g = Group::new(&[1, 2, 3]).await;
    g.join(4).await;
    g.drive_until("4 active", T, |st| st.learners == [4]).await;
    g.drive_until("4 active", T, |_| g.state_of(4) == Some(MEMBER_ACTIVE))
        .await;
    // Replace 3 (unless 3 leads: then replace 2, the leader is never demoted in place).
    let leader = g.leader().await;
    let victim = if leader == 3 { 2 } else { 3 };
    g.set_state(victim, MEMBER_DRAINING).await;
    let log = g
        .drive_until("victim out", T, |st| {
            !st.voters.contains(&victim)
                && !st.learners.contains(&victim)
                && st.voters_outgoing.is_empty()
        })
        .await;
    let st = g.leader_handle().await.raft_detail();
    assert_eq!(st.voters.len(), 3);
    assert!(st.voters.contains(&4));
    assert_eq!(g.state_of(victim), Some(MEMBER_REMOVED));
    assert!(
        log.iter().any(|(_, o)| matches!(o, TickOutcome::Step(ostro_cluster::membership::reconcile::Step::Joint { add_voters, .. }) if add_voters == &[4])),
        "one joint change added 4: {log:?}"
    );
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leave_joint_after_killing_the_leader_mid_replace() {
    assert!(
        leave_joint_after_leader_kill(T).await,
        "joint config left by the new leader"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn removal_of_a_node_without_replicas_shrinks_five_to_three() {
    let mut g = Group::new(&[1, 2, 3]).await;
    g.join(4).await;
    g.join(5).await;
    g.drive_until("3 -> 5 voters", T, |st| {
        st.voters.len() == 5 && st.voters_outgoing.is_empty()
    })
    .await;
    let leader = g.leader().await;
    let victim = if leader == 5 { 4 } else { 5 };
    g.set_state(victim, MEMBER_DRAINING).await;
    g.drive_until("5 -> 3 voters", T, |st| {
        st.voters.len() == 3
            && st.voters_outgoing.is_empty()
            && !st.voters.contains(&victim)
            && !st.learners.contains(&victim)
    })
    .await;
    assert_eq!(g.state_of(victim), Some(MEMBER_REMOVED));
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refused_when_quorum_would_break() {
    assert_eq!(quorum_refusal(30).await, [1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orphan_joiner_is_removed_then_tombstoned() {
    let g = Group::new(&[1, 2, 3]).await;
    // A Joining record whose node never starts.
    g.put_member(4, Some(ostro_meta::ext::MEMBER_JOINING)).await;
    g.drive_until("4 added as a learner", T, |st| st.learners == [4])
        .await;
    g.dead.lock().unwrap().insert(4);
    for _ in 0..10 {
        g.tick().await;
    }
    assert_ne!(
        g.state_of(4),
        Some(MEMBER_REMOVED),
        "not before the timeout"
    );
    g.clock.advance(3_600_000 + 1);
    g.drive_until("4 out of the conf", T, |st| !st.learners.contains(&4))
        .await;
    g.drive_until("4 tombstoned", T, |_| g.state_of(4) == Some(MEMBER_REMOVED))
        .await;
    g.stop_all().await;
}
