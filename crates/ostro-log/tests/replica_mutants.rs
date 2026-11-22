//! "Mutant bites" for the six `repl::mutant::*` failpoints placed in `ostro_log::replica`
//! (12-04, D-P12-19): each test checks the property holds, enables the mutant with
//! `fail::cfg(name, "return")`, shows the same property violated in a constructed case, and
//! removes it again.
//!
//! Requires `--features failpoints`; run single-threaded (`--test-threads=1`): the `fail`
//! registry is process-global (`FailScenario` also serialises the tests).
#![cfg(feature = "failpoints")]

use std::collections::BTreeSet;

use fail::FailScenario;
use ostro_log::hw::ReplicaProgress;
use ostro_log::mutants;
use ostro_log::replica::failover::{AssignmentIn, Plan, plan};
use ostro_log::replica::isr::{IsrState, PendingChange, maximal, should_expand};
use ostro_log::replica::tracker::ReplicaTracker;
use ostro_log::replica::watermark::{append_allowed, next_hw};
use ostro_types::Offset;

const LAG: u64 = 10_000;

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

/// Run `holds` with the mutant off (must be true), on (must be false), then off again.
fn bites(name: &str, holds: impl Fn() -> bool) {
    let scenario = FailScenario::setup();
    assert!(holds(), "{name}: property fails without the mutant");
    fail::cfg(name, "return").unwrap();
    let with = holds();
    fail::remove(name);
    assert!(!with, "{name}: mutant did not bite");
    assert!(holds(), "{name}: property fails after removing the mutant");
    scenario.teardown();
}

#[test]
fn isr_from_assignment_expands_an_empty_follower() {
    bites(mutants::ISR_FROM_ASSIGNMENT, || {
        let t = ReplicaTracker::new(1, &[1, 2, 3], Offset(100), 0);
        !should_expand(&t, 2, Offset(50), Offset(0), 0, LAG)
    });
}

#[test]
fn hw_leader_leo_ignores_followers() {
    bites(mutants::HW_LEADER_LEO, || {
        let p = progress(&[(1, 50), (2, 40), (3, 45)]);
        next_hw(Offset(0), &p, &isr(&[1, 2, 3], None), 2) == Offset(40)
    });
}

#[test]
fn count_nondurable_leo_counts_the_fetch_offset() {
    bites(mutants::COUNT_NONDURABLE_LEO, || {
        let mut t = ReplicaTracker::new(1, &[1, 2, 3], Offset(120), 0);
        t.on_fetch(2, Offset(100), Offset(90), Offset(120), 5);
        t.durable_leo(2) == Some(Offset(90))
    });
}

#[test]
fn shrink_before_commit_drops_a_pending_remove() {
    bites(mutants::SHRINK_BEFORE_COMMIT, || {
        let p = progress(&[(1, 50), (2, 40), (3, 10)]);
        let s = isr(&[1, 2, 3], Some(PendingChange::Remove(3)));
        maximal(&s).contains(&3) && next_hw(Offset(0), &p, &s, 2) == Offset(10)
    });
}

#[test]
fn unclean_election_elects_outside_the_isr() {
    bites(mutants::UNCLEAN_ELECTION, || {
        let a = AssignmentIn {
            partition: 0,
            leader: 1,
            epoch: 3,
            replicas: vec![1, 2, 3],
            isr: vec![1, 3],
            isr_version: 7,
        };
        let live: BTreeSet<u64> = [2].into_iter().collect();
        match plan(&a, &live, None) {
            Plan::Elect { leader, .. } => a.isr.contains(&leader),
            Plan::Offline { keep } => keep == vec![1],
            Plan::Shrink { .. } | Plan::Nothing => false,
        }
    });
}

#[test]
fn ignore_min_insync_advances_and_appends_below_min() {
    bites(mutants::IGNORE_MIN_INSYNC, || {
        let p = progress(&[(1, 50), (2, 40)]);
        let s = isr(&[1], None);
        next_hw(Offset(7), &p, &s, 2) == Offset(7) && !append_allowed(&s, 2)
    });
}
