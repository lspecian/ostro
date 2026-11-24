//! 13-06: the pure placement planner (`placement::plan::target_replicas`) and the
//! preferred-leader balancer (`placement::leaders`), D-P13-12.

use std::collections::{BTreeMap, BTreeSet};

use ostro_cluster::placement::leaders::{LeaderMove, LeaderObs, decide, preferred_leader_moves};
use ostro_cluster::placement::plan::target_replicas;
use proptest::prelude::*;

fn apply(
    current: &BTreeMap<u16, Vec<u64>>,
    moves: &BTreeMap<u16, Vec<u64>>,
) -> BTreeMap<u16, Vec<u64>> {
    let mut out = current.clone();
    for (p, t) in moves {
        out.insert(*p, t.clone());
    }
    out
}

/// Checks the planner's output invariants; returns the final assignment.
fn check_plan(
    current: &BTreeMap<u16, Vec<u64>>,
    members: &[u64],
    rf: u8,
) -> Result<BTreeMap<u16, Vec<u64>>, String> {
    let moves = target_replicas(current, members, rf);
    for (p, t) in &moves {
        if !current.contains_key(p) {
            return Err(format!("move for unknown partition {p}"));
        }
        if current[p] == *t {
            return Err(format!("no-op move for partition {p}"));
        }
    }
    let fin = apply(current, &moves);
    let ms: BTreeSet<u64> = members.iter().copied().collect();
    if ms.is_empty() {
        return if moves.is_empty() {
            Ok(fin)
        } else {
            Err("moves without members".into())
        };
    }
    let eff = usize::from(rf.max(1)).min(ms.len());
    let mut counts: BTreeMap<u64, usize> = ms.iter().map(|m| (*m, 0)).collect();
    let mut leads: BTreeMap<u64, usize> = ms.iter().map(|m| (*m, 0)).collect();
    for (p, r) in &fin {
        let set: BTreeSet<u64> = r.iter().copied().collect();
        if set.len() != r.len() {
            return Err(format!("partition {p} holds a node twice: {r:?}"));
        }
        if r.len() != eff {
            return Err(format!(
                "partition {p} has {} replicas, want {eff}",
                r.len()
            ));
        }
        if !set.is_subset(&ms) {
            return Err(format!(
                "partition {p} on a non-member: {r:?} (members {ms:?})"
            ));
        }
        for n in r {
            *counts.get_mut(n).unwrap() += 1;
        }
        *leads.get_mut(&r[0]).unwrap() += 1;
    }
    let spread = |c: &BTreeMap<u64, usize>| {
        c.values().max().copied().unwrap_or(0) - c.values().min().copied().unwrap_or(0)
    };
    if spread(&counts) > 1 {
        return Err(format!("replica counts differ by > 1: {counts:?}"));
    }
    if spread(&leads) > 1 {
        return Err(format!("preferred leaders differ by > 1: {leads:?}"));
    }
    Ok(fin)
}

// ---------------------------------------------------------------------------------------
// Placement unit cases
// ---------------------------------------------------------------------------------------

fn assignment(p: u16, f: impl Fn(u16) -> Vec<u64>) -> BTreeMap<u16, Vec<u64>> {
    (0..p).map(|i| (i, f(i))).collect()
}

#[test]
fn promoted_rf1_cluster_grows_to_rf3_balanced() {
    let cur = assignment(16, |_| vec![1]);
    let fin = check_plan(&cur, &[1, 2, 3], 3).unwrap();
    // node 1 keeps every replica (only adds), so no partition loses its data holder
    assert!(fin.values().all(|r| r.contains(&1)));
}

#[test]
fn single_node_rf3_stays_put() {
    let cur = assignment(16, |_| vec![1]);
    assert!(target_replicas(&cur, &[1], 3).is_empty());
}

#[test]
fn draining_node_is_emptied() {
    let cur = assignment(16, |i| match i % 3 {
        0 => vec![1, 2, 3],
        1 => vec![2, 3, 1],
        _ => vec![3, 1, 2],
    });
    let fin = check_plan(&cur, &[1, 2, 4], 3).unwrap();
    assert!(fin.values().all(|r| !r.contains(&3)));
}

#[test]
fn balanced_assignment_needs_no_moves() {
    let cur = assignment(16, |i| match i % 3 {
        0 => vec![1, 2, 3],
        1 => vec![2, 3, 1],
        _ => vec![3, 1, 2],
    });
    assert!(target_replicas(&cur, &[1, 2, 3], 3).is_empty());
}

#[test]
fn empty_partitions_are_placed() {
    let cur = assignment(4, |_| vec![]);
    check_plan(&cur, &[1, 2], 2).unwrap();
}

#[test]
fn no_members_no_moves() {
    let cur = assignment(4, |_| vec![1]);
    assert!(target_replicas(&cur, &[], 3).is_empty());
}

// ---------------------------------------------------------------------------------------
// Placement properties
// ---------------------------------------------------------------------------------------

fn scenario() -> impl Strategy<Value = (BTreeMap<u16, Vec<u64>>, Vec<u64>, u8)> {
    (
        1u64..=7,
        prop_oneof![Just(4u16), Just(16), Just(64)],
        1u8..=3,
    )
        .prop_flat_map(|(nodes, p, rf)| {
            let members =
                proptest::sample::subsequence((1..=nodes).collect::<Vec<_>>(), 1..=nodes as usize);
            let random = proptest::collection::vec(
                proptest::collection::vec(1..=nodes + 1, 0..=3),
                p as usize,
            )
            .prop_map(|v| {
                v.into_iter()
                    .enumerate()
                    .map(|(i, r)| (i as u16, r))
                    .collect()
            });
            let promoted = Just(assignment(p, |_| vec![1]));
            (
                prop_oneof![3 => random.boxed(), 1 => promoted.boxed()],
                members,
                Just(rf),
            )
        })
}

proptest! {
    #[test]
    fn placement_invariants((cur, members, rf) in scenario()) {
        let fin = match check_plan(&cur, &members, rf) {
            Ok(f) => f,
            Err(e) => { prop_assert!(false, "{e}\ncurrent {cur:?} members {members:?} rf {rf}"); unreachable!() }
        };
        // minimal: a plan from the planned (balanced) assignment is empty
        let again = target_replicas(&fin, &members, rf);
        prop_assert!(again.is_empty(), "not idempotent: {again:?}");
        // deterministic
        prop_assert_eq!(target_replicas(&cur, &members, rf), target_replicas(&cur, &members, rf));
    }

    #[test]
    fn node_counts_move_towards_balance((cur, members, rf) in scenario()) {
        // Greedy minimality: a node never ends above max(its valid start count, ceil(avg))
        // and never gains replicas while above floor(avg) (no churn through balanced nodes).
        let moves = target_replicas(&cur, &members, rf);
        let ms: BTreeSet<u64> = members.iter().copied().collect();
        let eff = usize::from(rf).min(ms.len());
        let total = cur.len() * eff;
        let ceil = total.div_ceil(ms.len());
        let mut before: BTreeMap<u64, usize> = ms.iter().map(|m| (*m, 0)).collect();
        let mut after = before.clone();
        for (p, r) in &cur {
            let mut seen = BTreeSet::new();
            for n in r.iter().filter(|n| ms.contains(n) && seen.insert(**n)) {
                *before.get_mut(n).unwrap() += 1;
            }
            for n in moves.get(p).unwrap_or(r) {
                *after.get_mut(n).unwrap() += 1;
            }
        }
        for m in &ms {
            prop_assert!(after[m] <= before[m].max(ceil), "node {m}: {} -> {} (ceil {ceil})", before[m], after[m]);
        }
    }
}

// ---------------------------------------------------------------------------------------
// Preferred-leader balancer
// ---------------------------------------------------------------------------------------

fn obs(p: u16, replicas: &[u64], isr: &[u64], leader: u64) -> LeaderObs {
    LeaderObs {
        partition: p,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        leader,
    }
}

fn live(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

#[test]
fn no_move_when_preferred_leaders_lead() {
    let parts = vec![obs(0, &[1, 2], &[1, 2], 1), obs(1, &[2, 1], &[1, 2], 2)];
    assert_eq!(decide(&parts, &live(&[1, 2]), 2), None);
}

#[test]
fn moves_only_to_in_isr_live_preferred() {
    let parts = vec![obs(0, &[2, 1], &[1], 1)];
    assert_eq!(
        decide(&parts, &live(&[1, 2]), 2),
        None,
        "preferred not in ISR"
    );
    let parts = vec![obs(0, &[2, 1], &[1, 2], 1)];
    assert_eq!(decide(&parts, &live(&[1]), 2), None, "preferred not live");
    assert_eq!(
        decide(&parts, &live(&[1, 2]), 2),
        Some(LeaderMove {
            partition: 0,
            to: 2
        })
    );
}

#[test]
fn most_loaded_leader_moves_first() {
    let parts = vec![
        obs(0, &[3, 2], &[2, 3], 2),
        obs(1, &[3, 1], &[1, 3], 1),
        obs(2, &[1, 2], &[1, 2], 1),
        obs(3, &[1, 3], &[1, 3], 1),
    ];
    assert_eq!(
        decide(&parts, &live(&[1, 2, 3]), 3),
        Some(LeaderMove {
            partition: 1,
            to: 3
        })
    );
}

#[test]
fn offline_partitions_are_not_moved() {
    let parts = vec![obs(0, &[2, 1], &[1, 2], 0)];
    assert_eq!(decide(&parts, &live(&[1, 2]), 2), None);
}

#[test]
fn preferred_leader_moves_respects_max_and_liveness() {
    let parts: Vec<(u16, Vec<u64>, u64)> = vec![
        (0, vec![2, 1], 1),
        (1, vec![3, 1], 1),
        (2, vec![1, 2], 1),
        (3, vec![4, 1], 1),
    ];
    let moves = preferred_leader_moves(&parts, &[1, 2, 3], 1);
    assert_eq!(moves.len(), 1);
    let moves = preferred_leader_moves(&parts, &[1, 2, 3], 10);
    assert_eq!(moves.len(), 2, "4 is not live: {moves:?}");
    assert!(moves.iter().all(|(p, to)| parts[*p as usize].1[0] == *to));
    assert!(preferred_leader_moves(&parts, &[1, 2, 3], 0).is_empty());
}

fn leader_scenario() -> impl Strategy<Value = (Vec<LeaderObs>, BTreeSet<u64>, usize)> {
    (2u64..=6, 1usize..=32).prop_flat_map(|(n, p)| {
        let part = (
            proptest::sample::subsequence((1..=n).collect::<Vec<_>>(), 1..=3.min(n as usize))
                .prop_shuffle(),
            any::<proptest::sample::Index>(),
            proptest::collection::vec(any::<bool>(), 3),
        );
        (
            proptest::collection::vec(part, p),
            proptest::sample::subsequence((1..=n).collect::<Vec<_>>(), 0..=n as usize),
            Just(n as usize),
        )
            .prop_map(|(parts, live, n)| {
                let parts = parts
                    .into_iter()
                    .enumerate()
                    .map(|(i, (replicas, li, in_isr))| {
                        let leader = replicas[li.index(replicas.len())];
                        let isr: Vec<u64> = replicas
                            .iter()
                            .zip(in_isr.iter().chain(std::iter::repeat(&true)))
                            .filter(|(r, b)| **r == leader || **b)
                            .map(|(r, _)| *r)
                            .collect();
                        LeaderObs {
                            partition: i as u16,
                            replicas,
                            isr,
                            leader,
                        }
                    })
                    .collect();
                (parts, live.into_iter().collect(), n)
            })
    })
}

proptest! {
    #[test]
    fn leader_balancer_invariants((parts, live, members) in leader_scenario()) {
        match decide(&parts, &live, members) {
            None => {
                for p in &parts {
                    let pref = p.replicas[0];
                    prop_assert!(
                        pref == p.leader || !p.isr.contains(&pref) || !live.contains(&pref),
                        "missed move for {p:?}"
                    );
                }
            }
            Some(LeaderMove { partition, to }) => {
                let p = parts.iter().find(|x| x.partition == partition).unwrap();
                prop_assert_eq!(to, p.replicas[0]);
                prop_assert!(p.isr.contains(&to) && live.contains(&to) && p.leader != to);
                prop_assert!(p.leader != 0);
            }
        }
    }
}
