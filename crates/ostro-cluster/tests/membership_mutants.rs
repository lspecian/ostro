//! 13-09: the membership mutants (D-P13-25) bite: under `member::mutant::skip_quorum_check`
//! the driver removes a voter without a live quorum; under `member::mutant::no_leave_joint`
//! a joint configuration left by a dead leader is never left. Failpoints build only.
#![cfg(feature = "failpoints")]

mod support_membership;

use std::time::Duration;

use ostro_cluster::membership::service::{MUTANT_NO_LEAVE_JOINT, MUTANT_SKIP_QUORUM_CHECK};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn skip_quorum_check_removes_without_a_live_quorum() {
    fail::cfg(MUTANT_SKIP_QUORUM_CHECK, "return").unwrap();
    let voters = support_membership::quorum_refusal(30).await;
    fail::remove(MUTANT_SKIP_QUORUM_CHECK);
    assert_ne!(
        voters,
        [1, 2, 3],
        "the mutant must break the quorum refusal"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_leave_joint_stays_joint() {
    fail::cfg(MUTANT_NO_LEAVE_JOINT, "return").unwrap();
    let left = support_membership::leave_joint_after_leader_kill(Duration::from_secs(5)).await;
    fail::remove(MUTANT_NO_LEAVE_JOINT);
    assert!(!left, "the mutant must keep the joint configuration");
}
