//! Unit cases for the pure replica algorithms (12-04): tracker (B5), KIP-497 watermark,
//! ISR expand/shrink, KIP-595 divergence with KIP-279 iteration, and the failover planner.

use std::collections::BTreeSet;

use ostro_log::EpochHistory;
use ostro_log::hw::ReplicaProgress;
use ostro_log::replica::divergence::{FetchCheck, check, check_leading, follower_truncation_point};
use ostro_log::replica::failover::{AssignmentIn, Plan, plan};
use ostro_log::replica::isr::{
    IsrState, PendingChange, maximal, next_change, should_expand, should_shrink,
};
use ostro_log::replica::tracker::ReplicaTracker;
use ostro_log::replica::watermark::{append_allowed, next_hw};
use ostro_types::{Epoch, Offset};
use proptest::prelude::*;

const LAG: u64 = 10_000;

fn o(v: u64) -> Offset {
    Offset(v)
}

fn hist(pairs: &[(u32, u64)]) -> EpochHistory {
    EpochHistory(pairs.iter().map(|&(e, s)| (Epoch(e), Offset(s))).collect())
}

fn progress(rows: &[(u64, u64)]) -> Vec<ReplicaProgress> {
    rows.iter()
        .map(|&(node, leo)| ReplicaProgress {
            node,
            leo: Offset(leo),
            in_isr: false,
        })
        .collect()
}

fn isr(published: &[u64], pending: Option<PendingChange>) -> IsrState {
    IsrState {
        published: published.to_vec(),
        version: 1,
        pending,
    }
}

// ---------------------------------------------------------------- tracker (B5)

#[test]
fn tracker_records_durable_leo_not_fetch_offset() {
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(120), 0);
    t.on_fetch(2, o(100), o(90), o(120), 5);
    assert_eq!(t.durable_leo(2), Some(o(90)));
}

#[test]
fn tracker_caps_reported_durable_leo_at_fetch_offset() {
    let mut t = ReplicaTracker::new(1, &[1, 2], o(120), 0);
    t.on_fetch(2, o(100), o(110), o(120), 5);
    assert_eq!(t.durable_leo(2), Some(o(100)));
}

#[test]
fn tracker_durable_leo_never_decreases_without_truncate() {
    let mut t = ReplicaTracker::new(1, &[1, 2], o(120), 0);
    t.on_fetch(2, o(100), o(90), o(120), 5);
    t.on_fetch(2, o(100), o(50), o(120), 6);
    assert_eq!(t.durable_leo(2), Some(o(90)));
    t.on_truncate(2);
    assert_eq!(t.durable_leo(2), Some(o(0)));
    t.on_fetch(2, o(60), o(60), o(120), 7);
    assert_eq!(t.durable_leo(2), Some(o(60)));
}

#[test]
fn tracker_leader_durable_end_counts_for_self() {
    let mut t = ReplicaTracker::new(1, &[1, 2], o(10), 0);
    assert_eq!(t.durable_leo(1), Some(o(10)));
    t.on_local_durable(o(25));
    assert_eq!(t.durable_leo(1), Some(o(25)));
    assert_eq!(t.durable_leo(9), None);
}

#[test]
fn tracker_caught_up_compares_with_previous_response_end_not_leo() {
    let mut t = ReplicaTracker::new(1, &[1, 2], o(100), 0);
    // First response: the leader sampled durable end 100 before reading.
    t.on_fetch(2, o(50), o(50), o(100), 1_000);
    // Appends land between the snapshot and the read: the target stays 100.
    t.on_local_durable(o(180));
    // The follower reaches 100 (what it could have had) => caught up, though LEO is 180.
    t.on_fetch(2, o(100), o(100), o(180), 2_000);
    assert_eq!(t.lag_ms(2, 2_000), Some(0));
    // It now has to reach 180 (the previous response's end); 150 is not enough.
    t.on_fetch(2, o(150), o(150), o(200), 3_000);
    assert_eq!(t.lag_ms(2, 3_000), Some(1_000));
}

/// 12-16: a replica that never fetched is reported out of sync, even inside its join grace.
#[test]
fn tracker_status_never_fetched_is_not_in_sync() {
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(0), 1_000);
    t.on_local_durable(o(5));
    t.on_fetch(2, o(5), o(5), o(5), 1_100);
    let rows = t.status(1_200, LAG);
    let r2 = rows.iter().find(|r| r.replica == 2).unwrap();
    let r3 = rows.iter().find(|r| r.replica == 3).unwrap();
    assert!(r2.in_sync);
    assert!(!r3.in_sync, "never fetched: {r3:?}");
    assert_eq!(r3.durable_leo, o(0));
}

#[test]
fn tracker_continuous_appends_keep_a_keeping_up_follower_in_sync() {
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(0), 0);
    let mut leader_end = 0u64;
    let mut prev_resp_2 = 0u64; // follower 2 reaches every previous response end
    let stuck_3 = 40u64; // follower 3 stops at 40
    for tick in 1..=200u64 {
        let now = tick * 100;
        leader_end += 10;
        t.on_local_durable(o(leader_end));
        let sample = leader_end; // sampled before the read
        t.on_fetch(2, o(prev_resp_2), o(prev_resp_2), o(sample), now);
        prev_resp_2 = sample;
        let f3 = stuck_3.min(leader_end);
        t.on_fetch(3, o(f3), o(f3), o(sample), now);
        assert!(t.lag_ms(2, now).unwrap() <= 100, "tick {tick}");
        // The follower never equals the moving LEO at fetch time...
        assert!(prev_resp_2 > 0);
    }
    let now = 200 * 100;
    let rows = t.status(now, LAG);
    let r2 = rows.iter().find(|r| r.replica == 2).unwrap();
    let r3 = rows.iter().find(|r| r.replica == 3).unwrap();
    assert!(r2.in_sync);
    assert!(
        !r3.in_sync,
        "a stuck follower loses caught-up after lag max"
    );
    assert!(t.lag_ms(3, now).unwrap() > LAG);
    assert!(
        rows.iter().all(|r| r.replica != 1),
        "status lists followers only"
    );
}

#[test]
fn tracker_progress_lists_every_replica() {
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(30), 0);
    t.on_fetch(3, o(20), o(20), o(30), 1);
    let mut p: Vec<(u64, u64)> = t.progress().iter().map(|p| (p.node, p.leo.0)).collect();
    p.sort();
    assert_eq!(p, vec![(1, 30), (2, 0), (3, 20)]);
}

#[test]
fn tracker_ignores_unknown_replicas() {
    let mut t = ReplicaTracker::new(1, &[1, 2], o(30), 0);
    t.on_fetch(7, o(20), o(20), o(30), 1);
    assert_eq!(t.durable_leo(7), None);
    assert_eq!(t.lag_ms(7, 1), None);
}

// ---------------------------------------------------------------- watermark (KIP-497)

#[test]
fn hw_is_min_durable_over_isr() {
    let p = progress(&[(1, 50), (2, 40), (3, 45)]);
    assert_eq!(next_hw(o(0), &p, &isr(&[1, 2, 3], None), 2), o(40));
}

#[test]
fn hw_never_decreases() {
    let p = progress(&[(1, 50), (2, 40), (3, 45)]);
    assert_eq!(next_hw(o(42), &p, &isr(&[1, 2, 3], None), 2), o(42));
}

#[test]
fn hw_frozen_below_min_insync() {
    let p = progress(&[(1, 50), (2, 40)]);
    assert_eq!(next_hw(o(7), &p, &isr(&[1], None), 2), o(7));
    assert!(!append_allowed(&isr(&[1], None), 2));
    assert!(append_allowed(&isr(&[1, 2], None), 2));
}

#[test]
fn hw_counts_pending_add() {
    let p = progress(&[(1, 50), (2, 40), (4, 30)]);
    let s = isr(&[1, 2], Some(PendingChange::Add(4)));
    assert_eq!(maximal(&s), vec![1, 2, 4]);
    assert_eq!(next_hw(o(0), &p, &s, 2), o(30));
}

#[test]
fn hw_pending_remove_still_counts() {
    let p = progress(&[(1, 50), (2, 40), (3, 10)]);
    let s = isr(&[1, 2, 3], Some(PendingChange::Remove(3)));
    assert_eq!(maximal(&s), vec![1, 2, 3]);
    assert_eq!(next_hw(o(0), &p, &s, 2), o(10));
}

#[test]
fn hw_rf1_is_self_durable_end() {
    let p = progress(&[(1, 77)]);
    assert_eq!(next_hw(o(0), &p, &isr(&[1], None), 1), o(77));
    assert!(append_allowed(&isr(&[1], None), 1));
}

#[test]
fn hw_member_without_progress_holds_hw() {
    let p = progress(&[(1, 77)]);
    assert_eq!(next_hw(o(5), &p, &isr(&[1, 2], None), 1), o(5));
}

proptest! {
    #[test]
    fn hw_bounded_and_monotone(
        prev in 0u64..100,
        leos in proptest::collection::vec(0u64..200, 1..5),
        min in 1usize..4,
    ) {
        let rows: Vec<(u64, u64)> = leos.iter().enumerate().map(|(i, l)| (i as u64 + 1, *l)).collect();
        let p = progress(&rows);
        let published: Vec<u64> = rows.iter().map(|r| r.0).collect();
        let s = isr(&published, None);
        let hw = next_hw(o(prev), &p, &s, min);
        prop_assert!(hw >= o(prev));
        if published.len() < min {
            prop_assert_eq!(hw, o(prev));
        } else if hw > o(prev) {
            for (_, l) in &rows { prop_assert!(hw <= o(*l)); }
        }
    }
}

// ---------------------------------------------------------------- ISR

fn caught_up_tracker(f_durable: u64, leader: u64, now: u64) -> ReplicaTracker {
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(leader), 0);
    t.on_fetch(2, o(f_durable), o(f_durable), o(f_durable), now);
    t
}

#[test]
fn expand_blocked_below_epoch_start_even_at_hw() {
    let t = caught_up_tracker(90, 120, 100);
    assert!(!should_expand(&t, 2, o(80), o(100), 100, LAG));
}

#[test]
fn expand_allowed_at_max_of_hw_and_epoch_start() {
    let t = caught_up_tracker(100, 120, 100);
    assert!(should_expand(&t, 2, o(80), o(100), 100, LAG));
    assert!(should_expand(&t, 2, o(100), o(50), 100, LAG));
    assert!(!should_expand(&t, 2, o(101), o(50), 100, LAG));
}

#[test]
fn expand_requires_caught_up_within_lag() {
    let t = caught_up_tracker(100, 120, 100);
    assert!(!should_expand(&t, 2, o(80), o(50), 100 + LAG + 1, LAG));
}

#[test]
fn expand_refuses_a_follower_that_never_fetched() {
    let t = ReplicaTracker::new(1, &[1, 2, 3], o(0), 0);
    assert!(!should_expand(&t, 2, o(0), o(0), 0, LAG));
}

#[test]
fn shrink_when_lag_exceeds_max_never_the_leader() {
    let t = caught_up_tracker(100, 120, 100);
    assert!(!should_shrink(&t, 2, 100 + LAG, LAG));
    assert!(should_shrink(&t, 2, 100 + LAG + 1, LAG));
    assert!(should_shrink(&t, 3, LAG + 1, LAG));
    assert!(!should_shrink(&t, 1, 10 * LAG, LAG));
}

#[test]
fn next_change_one_pending_at_a_time() {
    let t = caught_up_tracker(100, 120, 100);
    let s = isr(&[1, 3], Some(PendingChange::Remove(3)));
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(80), o(50), 100, LAG),
        None
    );
    let s = isr(&[1], None);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(80), o(50), 100, LAG),
        Some(PendingChange::Add(2))
    );
    let s = isr(&[1, 2, 3], None);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(80), o(50), LAG + 1, LAG),
        Some(PendingChange::Remove(3))
    );
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(80), o(50), 100, LAG),
        None
    );
}

#[test]
fn alter_isr_rejection_drops_pending_and_keeps_published() {
    let mut s = isr(&[1, 2, 3], None);
    assert!(s.propose(PendingChange::Remove(3)));
    assert!(
        !s.propose(PendingChange::Remove(2)),
        "one pending at a time"
    );
    s.reject();
    assert_eq!(s.pending, None);
    assert_eq!(s.published, vec![1, 2, 3]);
    assert_eq!(s.version, 1);
}

#[test]
fn alter_isr_commit_applies_pending() {
    let mut s = isr(&[1, 2, 3], None);
    assert!(s.propose(PendingChange::Remove(3)));
    s.commit(2);
    assert_eq!(s.published, vec![1, 2]);
    assert_eq!(s.version, 2);
    assert!(s.propose(PendingChange::Add(3)));
    s.commit(3);
    assert_eq!(s.published, vec![1, 2, 3]);
    assert_eq!(s.pending, None);
}

#[test]
fn rejected_shrink_never_lets_hw_pass_removed_replica() {
    // KIP-497: replica 3 lags at 10; the shrink is proposed then rejected.
    let p = progress(&[(1, 50), (2, 40), (3, 10)]);
    let mut s = isr(&[1, 2, 3], None);
    s.propose(PendingChange::Remove(3));
    assert_eq!(next_hw(o(0), &p, &s, 2), o(10));
    s.reject();
    assert_eq!(next_hw(o(0), &p, &s, 2), o(10));
}

// ---------------------------------------------------------------- divergence (KIP-595/279/101)

#[test]
fn fetch_below_log_start_needs_snapshot() {
    let h = hist(&[(3, 100)]);
    assert_eq!(
        check(&h, o(100), o(150), o(99), Some(Epoch(3))),
        FetchCheck::SnapshotRequired
    );
}

#[test]
fn fetch_at_log_start_anchored_follower_is_ok_after_trim() {
    // W5: the leader trimmed its history below 200; epoch 2 was in force at 200.
    let h = hist(&[(2, 200), (5, 260)]);
    assert_eq!(
        check(&h, o(200), o(300), o(200), Some(Epoch(2))),
        FetchCheck::Ok
    );
    assert_eq!(
        check(&h, o(200), o(300), o(200), Some(Epoch(1))),
        FetchCheck::Ok
    );
    assert_eq!(check(&h, o(200), o(300), o(200), None), FetchCheck::Ok);
}

#[test]
fn empty_follower_at_zero_is_ok() {
    let h = hist(&[(1, 0)]);
    assert_eq!(check(&h, o(0), o(10), o(0), None), FetchCheck::Ok);
}

#[test]
fn diverging_epoch_cases() {
    let h = hist(&[(1, 0), (3, 100)]);
    assert_eq!(
        check(&h, o(0), o(150), o(120), Some(Epoch(2))),
        FetchCheck::Diverging {
            epoch: Epoch(1),
            end: o(100)
        }
    );
    assert_eq!(
        check(&h, o(0), o(150), o(100), Some(Epoch(1))),
        FetchCheck::Ok
    );
    assert_eq!(
        check(&h, o(0), o(150), o(110), Some(Epoch(1))),
        FetchCheck::Diverging {
            epoch: Epoch(1),
            end: o(100)
        }
    );
    assert_eq!(
        check(&h, o(0), o(150), o(140), Some(Epoch(3))),
        FetchCheck::Ok
    );
    assert_eq!(
        check(&h, o(0), o(150), o(160), Some(Epoch(3))),
        FetchCheck::Diverging {
            epoch: Epoch(3),
            end: o(150)
        }
    );
}

#[test]
fn epoch_older_than_retained_history_needs_snapshot() {
    let h = hist(&[(4, 100), (6, 200)]);
    assert_eq!(
        check(&h, o(100), o(250), o(150), Some(Epoch(2))),
        FetchCheck::SnapshotRequired
    );
}

#[test]
fn leading_epoch_starts_at_leo_before_its_announcement() {
    // p12-r4: leader of epoch 697 whose history still ends at 695 (LeaderAnnounced{697} not
    // in the log yet); the follower holds one record of epoch 696 from a deposed leader.
    let h = hist(&[(1, 0), (695, 100)]);
    let lead = Epoch(697);
    assert_eq!(
        check(&h, o(0), o(180), o(182), Some(Epoch(696))),
        FetchCheck::SnapshotRequired,
        "the bare history cannot answer epoch 696"
    );
    assert_eq!(
        check_leading(&h, lead, o(0), o(180), o(182), Some(Epoch(696))),
        FetchCheck::Diverging {
            epoch: Epoch(695),
            end: o(180)
        }
    );
    // The follower truncates to where its epoch 696 began, then is served.
    let own = hist(&[(1, 0), (695, 100), (696, 181)]);
    let point = follower_truncation_point(&own, o(182), Epoch(695), o(180));
    assert_eq!(point, o(180));
    assert_eq!(
        check_leading(&h, lead, o(0), o(180), point, Some(Epoch(695))),
        FetchCheck::Ok
    );
    // A follower that is behind and below log_start still needs a snapshot.
    assert_eq!(
        check_leading(&h, lead, o(150), o(180), o(120), Some(Epoch(696))),
        FetchCheck::SnapshotRequired
    );
    // A claim of the leading epoch itself (no such record exists yet), or of a newer one.
    for e in [697, 698] {
        assert_eq!(
            check_leading(&h, lead, o(0), o(180), o(182), Some(Epoch(e))),
            FetchCheck::SnapshotRequired,
            "claim of epoch {e}"
        );
    }
    // Once the announcement is in the history, check_leading == check.
    let announced = hist(&[(1, 0), (695, 100), (697, 180)]);
    for (fetch, e) in [(182, 696), (181, 697), (150, 695), (190, 697)] {
        assert_eq!(
            check_leading(&announced, lead, o(0), o(181), o(fetch), Some(Epoch(e))),
            check(&announced, o(0), o(181), o(fetch), Some(Epoch(e))),
            "fetch {fetch} epoch {e}"
        );
    }
    // Answers check already gives are kept (older epochs, retained-history floor).
    assert_eq!(
        check_leading(
            &hist(&[(4, 100), (6, 200)]),
            Epoch(7),
            o(100),
            o(250),
            o(150),
            Some(Epoch(2))
        ),
        FetchCheck::SnapshotRequired
    );
}

#[test]
fn truncation_point_is_min_of_both_ends() {
    let own = hist(&[(1, 0), (2, 100)]);
    assert_eq!(
        follower_truncation_point(&own, o(150), Epoch(1), o(100)),
        o(100)
    );
    let own = hist(&[(1, 0), (2, 80)]);
    assert_eq!(
        follower_truncation_point(&own, o(150), Epoch(1), o(100)),
        o(80)
    );
}

/// Drive the follower against the leader until Ok; returns the follower's final LEO.
fn converge(
    leader: &EpochHistory,
    leader_leo: u64,
    own: &mut EpochHistory,
    own_leo: &mut u64,
) -> usize {
    for round in 0..64 {
        match check(leader, o(0), o(leader_leo), o(*own_leo), own.latest()) {
            FetchCheck::Ok => return round,
            FetchCheck::Diverging { epoch, end } => {
                let p = follower_truncation_point(own, o(*own_leo), epoch, end);
                assert!(p.0 <= *own_leo);
                *own_leo = p.0;
                own.0.retain(|(_, s)| s.0 < p.0);
            }
            FetchCheck::SnapshotRequired => panic!("unexpected snapshot"),
        }
    }
    panic!("no convergence");
}

#[test]
fn kip279_two_quick_leader_changes_iterate_to_common_prefix() {
    let leader = hist(&[(1, 0), (3, 90), (5, 95)]);
    let mut own = hist(&[(1, 0), (2, 80), (4, 100)]);
    let mut leo = 130;
    let rounds = converge(&leader, 140, &mut own, &mut leo);
    assert_eq!(leo, 80);
    assert_eq!(own, hist(&[(1, 0)]));
    assert!(rounds >= 2, "needed {rounds} rounds");
}

fn history_of(epochs: &[u32]) -> EpochHistory {
    let mut h: Vec<(Epoch, Offset)> = Vec::new();
    for (i, e) in epochs.iter().enumerate() {
        if h.last().map(|x| x.0) != Some(Epoch(*e)) {
            h.push((Epoch(*e), Offset(i as u64)));
        }
    }
    EpochHistory(h)
}

proptest! {
    /// Leader and follower share a non-empty prefix; their suffixes never share an epoch at
    /// the same offset (log matching). The loop converges exactly to the common prefix.
    #[test]
    fn divergence_loop_converges_to_common_prefix(
        prefix in proptest::collection::vec(0u32..3, 1..6),
        follower_runs in proptest::collection::vec((0u32..3, 1usize..4), 0..4),
        leader_runs in proptest::collection::vec((0u32..3, 1usize..4), 0..4),
        follower_continues in any::<bool>(),
    ) {
        // Prefix epochs: nondecreasing from 1.
        let mut p: Vec<u32> = Vec::new();
        let mut e = 1u32;
        for step in prefix { e += step; p.push(e); }
        let e0 = *p.last().unwrap();
        let mut f = p.clone();
        let mut l = p.clone();
        let mut fe = e0;
        if follower_continues { f.push(e0); }
        for (step, len) in follower_runs {
            fe += 2 * step + 1; // odd offsets from e0
            if fe % 2 == e0 % 2 { fe += 1; }
            for _ in 0..len { f.push(fe); }
        }
        let mut le = e0;
        for (step, len) in leader_runs {
            le += 2 * step + 2; // same parity as e0, never e0
            for _ in 0..len { l.push(le); }
        }
        // The leader's current epoch announcement: newer than anything the follower has.
        let top = le.max(fe) + 1;
        l.push(top);
        let lh = history_of(&l);
        let mut fh = history_of(&f);
        let mut leo = f.len() as u64;
        converge(&lh, l.len() as u64, &mut fh, &mut leo);
        prop_assert_eq!(leo as usize, p.len());
        prop_assert_eq!(fh, history_of(&p));
    }
}

// ---------------------------------------------------------------- failover planner

fn a(leader: u64, replicas: &[u64], isr: &[u64]) -> AssignmentIn {
    AssignmentIn {
        partition: 0,
        leader,
        epoch: 4,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version: 9,
    }
}

fn live(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

#[test]
fn elect_first_live_isr_member_with_isr_intersect_live() {
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 2, 3]), &live(&[2, 3]), None),
        Plan::Elect {
            leader: 2,
            isr: vec![2, 3]
        }
    );
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[2, 3]), &live(&[2, 3]), None),
        Plan::Elect {
            leader: 2,
            isr: vec![2, 3]
        }
    );
}

#[test]
fn live_replica_outside_isr_is_never_chosen() {
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 3]), &live(&[2, 3]), None),
        Plan::Elect {
            leader: 3,
            isr: vec![3]
        }
    );
}

#[test]
fn preferred_order_follows_replicas() {
    assert_eq!(
        plan(&a(1, &[1, 3, 2], &[1, 2, 3]), &live(&[2, 3]), None),
        Plan::Elect {
            leader: 3,
            isr: vec![2, 3]
        }
    );
}

#[test]
fn resigned_node_is_skipped() {
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 2, 3]), &live(&[1, 2, 3]), Some(1)),
        Plan::Elect {
            leader: 2,
            isr: vec![1, 2, 3]
        }
    );
    assert_eq!(
        plan(&a(0, &[1, 2, 3], &[1, 2, 3]), &live(&[1, 2, 3]), Some(2)),
        Plan::Elect {
            leader: 1,
            isr: vec![1, 2, 3]
        }
    );
}

#[test]
fn offline_keeps_last_isr_member() {
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 2]), &live(&[3]), None),
        Plan::Offline { keep: vec![1] }
    );
    // Already offline with one kept member: nothing more to do.
    assert_eq!(
        plan(&a(0, &[1, 2, 3], &[1]), &live(&[3]), None),
        Plan::Nothing
    );
    // The kept member returns: it is elected.
    assert_eq!(
        plan(&a(0, &[1, 2, 3], &[1]), &live(&[1, 3]), None),
        Plan::Elect {
            leader: 1,
            isr: vec![1]
        }
    );
}

#[test]
fn dead_follower_only_shrinks() {
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 2, 3]), &live(&[1, 2]), None),
        Plan::Shrink { isr: vec![1, 2] }
    );
}

#[test]
fn live_leader_full_isr_is_nothing() {
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 2, 3]), &live(&[1, 2, 3]), None),
        Plan::Nothing
    );
    // A dead replica outside the ISR is no ISR change.
    assert_eq!(
        plan(&a(1, &[1, 2, 3], &[1, 2]), &live(&[1, 2]), None),
        Plan::Nothing
    );
}

#[test]
fn unplaced_partition_is_left_to_bootstrap() {
    let mut x = a(0, &[1, 2, 3], &[]);
    x.epoch = 0;
    assert_eq!(plan(&x, &live(&[1, 2, 3]), None), Plan::Nothing);
}

proptest! {
    #[test]
    fn planner_never_adds_never_empty_never_outside_isr(
        replicas_mask in 1u8..16,
        isr_mask in 1u8..16,
        live_mask in 0u8..16,
        leader_pick in 0usize..5,
        resigned_pick in 0usize..6,
    ) {
        let ids = |m: u8| -> Vec<u64> { (0..4).filter(|i| m & (1 << i) != 0).map(|i| i + 1).collect() };
        let replicas = ids(replicas_mask);
        let isr: Vec<u64> = ids(isr_mask & replicas_mask);
        prop_assume!(!isr.is_empty());
        let leader = if leader_pick == 0 { 0 } else { *isr.get(leader_pick - 1).unwrap_or(&isr[0]) };
        let resigned = (resigned_pick > 0 && resigned_pick <= 4).then_some(resigned_pick as u64);
        let lv: BTreeSet<u64> = ids(live_mask).into_iter().collect();
        let asg = a(leader, &replicas, &isr);
        match plan(&asg, &lv, resigned) {
            Plan::Elect { leader: l, isr: new } => {
                prop_assert!(isr.contains(&l));
                prop_assert!(lv.contains(&l));
                prop_assert!(Some(l) != resigned);
                prop_assert!(new.contains(&l));
                prop_assert!(!new.is_empty());
                prop_assert!(new.iter().all(|n| isr.contains(n) && lv.contains(n)));
            }
            Plan::Shrink { isr: new } => {
                prop_assert!(!new.is_empty());
                prop_assert!(new.contains(&leader));
                prop_assert!(new.iter().all(|n| isr.contains(n)));
                prop_assert!(new.len() < isr.len());
            }
            Plan::Offline { keep } => {
                prop_assert_eq!(keep.len(), 1);
                prop_assert!(isr.contains(&keep[0]));
                prop_assert!(!isr.iter().any(|n| lv.contains(n) && Some(*n) != resigned));
            }
            Plan::Nothing => {}
        }
    }
}

// ---------------------------------------------------------------- 14-33: no re-add from stale progress

/// Leader 1, followers 2 and 3 both caught up at `t = 100` with durable LEO 100 (= HW).
fn three_caught_up() -> ReplicaTracker {
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(100), 0);
    t.on_fetch(2, o(100), o(100), o(100), 100);
    t.on_fetch(3, o(100), o(100), o(100), 100);
    t
}

#[test]
fn removed_replica_is_not_readded_from_stale_progress() {
    // A controller / self-shrink removal of 3 at version 2, while 3 still looks caught up.
    let mut t = three_caught_up();
    assert!(
        should_expand(&t, 3, o(100), o(0), 150, LAG),
        "caught up before"
    );
    t.on_isr_published(&[1, 2, 3], &[1, 2], 2);
    assert_eq!(t.removed_at_version(3), Some(2));
    assert_eq!(t.removed_at_version(2), None, "2 stays in the ISR");
    assert!(
        !should_expand(&t, 3, o(100), o(0), 150, LAG),
        "progress from before the removal must not re-add 3"
    );
    let s = IsrState::new(vec![1, 2], 2);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(100), o(0), 150, LAG),
        None,
        "no Add(3) from stale progress"
    );
    // The durable LEO is kept (a fact about 3's disk); the HW no longer counts it.
    assert_eq!(t.durable_leo(3), Some(o(100)));
}

#[test]
fn fresh_caught_up_fetch_after_the_removal_readds() {
    let mut t = three_caught_up();
    t.on_isr_published(&[1, 2, 3], &[1, 2], 2);
    // A fetch after the removal that is not caught up (leader end 120 > 110): still out.
    t.on_fetch(3, o(110), o(110), o(120), 200);
    assert_eq!(t.removed_at_version(3), Some(2));
    assert!(!should_expand(&t, 3, o(110), o(0), 200, LAG));
    // The next fetch reaches the end returned in the previous response: caught up again.
    t.on_fetch(3, o(120), o(120), o(130), 250);
    assert_eq!(t.removed_at_version(3), None);
    assert!(should_expand(&t, 3, o(120), o(0), 250, LAG));
    let s = IsrState::new(vec![1, 2], 2);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(120), o(0), 250, LAG),
        Some(PendingChange::Add(3)),
        "re-add proposed as before"
    );
}

#[test]
fn first_fetch_after_the_removal_must_reach_the_current_end() {
    // The previous-response target is cleared with the removal: a fetch reaching only the
    // end sampled before the removal is not a caught-up observation.
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(100), 0);
    t.on_fetch(3, o(90), o(90), o(100), 100); // target for the next fetch: 100
    t.on_isr_published(&[1, 2, 3], &[1, 2], 7);
    t.on_fetch(3, o(100), o(100), o(140), 150);
    assert_eq!(
        t.removed_at_version(3),
        Some(7),
        "100 < the current end 140"
    );
    t.on_fetch(3, o(140), o(140), o(160), 160);
    assert_eq!(
        t.removed_at_version(3),
        None,
        "reached the end of the previous response"
    );
}

#[test]
fn lag_shrink_and_its_readd_are_unchanged() {
    // 3 stops catching up; the leader's own lag shrink removes it ...
    let mut t = three_caught_up();
    t.on_fetch(2, o(200), o(200), o(200), 100 + LAG + 1);
    let s = IsrState::new(vec![1, 2, 3], 1);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(100), o(0), 100 + LAG + 1, LAG),
        Some(PendingChange::Remove(3))
    );
    t.on_isr_published(&[1, 2, 3], &[1, 2], 2);
    // ... and re-adds it after a caught-up fetch, as before 14-33.
    let now = 100 + LAG + 50;
    t.on_fetch(3, o(200), o(200), o(200), now);
    assert!(should_expand(&t, 3, o(200), o(0), now, LAG));
    let s = IsrState::new(vec![1, 2], 2);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(200), o(0), now, LAG),
        Some(PendingChange::Add(3))
    );
}

#[test]
fn readded_replica_drops_its_removal_mark() {
    let mut t = three_caught_up();
    t.on_isr_published(&[1, 2, 3], &[1, 2], 2);
    // Published again (by whoever) at version 3: the mark goes, the leader's own measured
    // lag decides from now on.
    t.on_isr_published(&[1, 2], &[1, 2, 3], 3);
    assert_eq!(t.removed_at_version(3), None);
    // Leader and unchanged members are never marked.
    t.on_isr_published(&[1, 2, 3], &[1, 3], 4);
    assert_eq!(t.removed_at_version(1), None);
    assert_eq!(t.removed_at_version(3), None);
    assert_eq!(t.removed_at_version(2), Some(4));
}

#[test]
fn pending_add_of_a_removed_replica_is_dropped_and_recomputed() {
    // The leader proposed Add(3) at version 1; meta publishes version 2 without 3 (another
    // proposer's change). Existing behaviour: the pending change stays until its CAS fails,
    // then is rejected; the next decision recomputes from the new published set.
    let mut t = ReplicaTracker::new(1, &[1, 2, 3], o(100), 0);
    t.on_fetch(2, o(100), o(100), o(100), 100);
    t.on_fetch(3, o(100), o(100), o(100), 100);
    let mut s = IsrState::new(vec![1, 2], 1);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(100), o(0), 100, LAG),
        Some(PendingChange::Add(3))
    );
    assert!(s.propose(PendingChange::Add(3)));
    // Version 2 published (3 was never in it; an Add CAS on version 1 is now stale).
    t.on_isr_published(&[1, 2], &[1, 2], 2);
    s.set_published(vec![1, 2], 2);
    assert_eq!(
        s.pending,
        Some(PendingChange::Add(3)),
        "pending until its CAS fails"
    );
    s.reject();
    assert_eq!(s.pending, None);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(100), o(0), 100, LAG),
        Some(PendingChange::Add(3)),
        "3 was not removed by version 2 (never a member): recomputed, still caught up"
    );
    // Now a version that *removes* 3 after it had joined: no Add from the old progress.
    s.set_published(vec![1, 2, 3], 3);
    t.on_isr_published(&[1, 2], &[1, 2, 3], 3);
    s.set_published(vec![1, 2], 4);
    t.on_isr_published(&[1, 2, 3], &[1, 2], 4);
    assert_eq!(
        next_change(&t, &s, &[1, 2, 3], o(100), o(0), 100, LAG),
        None
    );
}

/// The `readd_stale_progress` mutant (14-33) skips the reset: the removed replica is
/// re-added from its old progress. `fail` is process-global: one test only.
#[cfg(feature = "failpoints")]
#[test]
fn readd_stale_progress_mutant_bites() {
    let holds = || {
        let mut t = three_caught_up();
        t.on_isr_published(&[1, 2, 3], &[1, 2], 2);
        !should_expand(&t, 3, o(100), o(0), 150, LAG)
    };
    let scenario = fail::FailScenario::setup();
    assert!(holds(), "property fails without the mutant");
    fail::cfg(ostro_log::mutants::READD_STALE_PROGRESS, "return").expect("cfg");
    let with = holds();
    fail::remove(ostro_log::mutants::READD_STALE_PROGRESS);
    assert!(!with, "mutant did not bite");
    assert!(holds(), "property fails after removing the mutant");
    scenario.teardown();
}
