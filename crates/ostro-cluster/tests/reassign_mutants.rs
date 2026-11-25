//! 13-10: the reassignment mutant (D-P13-25) bites. Under
//! `reassign::mutant::raise_min_insync_early` Expand sends `min_insync = policy` instead of
//! the current value; with a lagging new replica and one existing ISR member falling
//! behind, the partition ends with |ISR| < min_insync, so writes stop (NotEnoughReplicas).
//! The same run without the mutant keeps writes flowing. Failpoints build only; one test so
//! the global failpoint never leaks into the baseline.
#![cfg(feature = "failpoints")]

mod support_placement;

use ostro_cluster::placement::controller::MUTANT_RAISE_MIN_INSYNC_EARLY;
use ostro_meta::ext::{ReplicationPolicy, get_reassignment};
use support_placement::{Bed, P, assignment};

/// RF 2 → 3 (policy min_insync 2) with node 13 lagging; then node 12 falls behind too.
/// Returns the stalls observed (partitions with |ISR| < min_insync).
async fn lagging_expand() -> Vec<String> {
    let placed = (0..P)
        .map(|p| assignment(p, 11, &[11, 12], &[11, 12], 1))
        .collect();
    let policy = ReplicationPolicy {
        rf: 3,
        min_insync: 2,
        max_voters: 5,
    };
    let mut bed = Bed::new(&[11, 12, 13], placed, Some(2), Some(policy)).await;
    bed.lagging.lock().unwrap().insert(13);
    bed.drive_until("two Expands applied", |parts| {
        parts
            .iter()
            .filter(|a| matches!(get_reassignment(&a.ext), Ok(Some(_))))
            .count()
            == 2
    })
    .await;
    // An existing ISR member falls behind: the leader's ISR shrinks to [11].
    bed.lagging.lock().unwrap().insert(12);
    for _ in 0..5 {
        bed.round(false).await;
    }
    let stalled = bed.stalled.clone();
    bed.stop().await;
    stalled
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raise_min_insync_early_stops_writes() {
    let baseline = lagging_expand().await;
    assert!(
        baseline.is_empty(),
        "without the mutant writes keep flowing: {baseline:?}"
    );
    fail::cfg(MUTANT_RAISE_MIN_INSYNC_EARLY, "return").unwrap();
    let mutant = lagging_expand().await;
    fail::remove(MUTANT_RAISE_MIN_INSYNC_EARLY);
    assert!(
        !mutant.is_empty(),
        "the mutant must leave a partition with |ISR| < min_insync (NotEnoughReplicas)"
    );
}
