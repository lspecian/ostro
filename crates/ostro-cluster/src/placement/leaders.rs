//! Phase 13 (13-06): pure preferred-leader balancing (D-P13-12).
//!
//! The preferred leader of a partition is its first replica (the planner balances first
//! replicas, `plan.rs`). [`decide`] returns at most one move: a partition whose live,
//! in-ISR preferred replica is not leading, choosing the partition whose current leader
//! leads the most partitions (a node above `ceil(P / members) + 1` is therefore always
//! relieved first). Offline partitions (leader 0) are never moved; the handoff itself
//! (D-P13-13) re-checks the ISR. No I/O, no clock.

use std::collections::{BTreeMap, BTreeSet};

/// One partition as the leader balancer sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LeaderObs {
    /// Partition.
    pub partition: u16,
    /// Replicas (the first is the preferred leader).
    pub replicas: Vec<u64>,
    /// Published ISR.
    pub isr: Vec<u64>,
    /// Current leader (0 = offline).
    pub leader: u64,
}

/// One preferred-leader handoff.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaderMove {
    /// Partition.
    pub partition: u16,
    /// New leader (the partition's preferred replica).
    pub to: u64,
}

/// At most one preferred-leader move over `parts` (`live` = live node ids, `members` =
/// number of Active members, used for the overload threshold `ceil(P / members) + 1`).
///
/// Candidates: leader ≠ 0, preferred ≠ leader, preferred live and in the published ISR.
/// Order: partitions led by an overloaded node first, then by the current leader's load
/// (descending), then by partition id. `None` when no candidate exists.
pub fn decide(parts: &[LeaderObs], live: &BTreeSet<u64>, members: usize) -> Option<LeaderMove> {
    let mut leads: BTreeMap<u64, usize> = BTreeMap::new();
    for p in parts.iter().filter(|p| p.leader != 0) {
        *leads.entry(p.leader).or_default() += 1;
    }
    let threshold = parts.len().div_ceil(members.max(1)) + 1;
    parts
        .iter()
        .filter_map(|p| {
            let pref = *p.replicas.first()?;
            let ok =
                p.leader != 0 && pref != p.leader && live.contains(&pref) && p.isr.contains(&pref);
            ok.then_some((p, pref))
        })
        .min_by_key(|(p, _)| {
            let load = leads.get(&p.leader).copied().unwrap_or(0);
            (load <= threshold, std::cmp::Reverse(load), p.partition)
        })
        .map(|(p, to)| LeaderMove {
            partition: p.partition,
            to,
        })
}

/// Partition → (replicas, current leader); returns at most `max_moves` `(partition, to)`
/// handoffs towards the preferred leader (first replica), each to a live preferred replica,
/// by repeated [`decide`] on the simulated result. The caller passes partitions whose
/// preferred replica is in the published ISR (or uses [`decide`] with the ISR); the handoff
/// refuses a target outside the ISR anyway (D-P13-13).
pub fn preferred_leader_moves(
    partitions: &[(u16, Vec<u64>, u64)],
    live: &[u64],
    max_moves: usize,
) -> Vec<(u16, u64)> {
    let live: BTreeSet<u64> = live.iter().copied().collect();
    let members = live.len();
    let mut parts: Vec<LeaderObs> = partitions
        .iter()
        .map(|(p, replicas, leader)| LeaderObs {
            partition: *p,
            replicas: replicas.clone(),
            isr: replicas.clone(),
            leader: *leader,
        })
        .collect();
    let mut out = Vec::new();
    while out.len() < max_moves {
        let Some(m) = decide(&parts, &live, members) else {
            break;
        };
        out.push((m.partition, m.to));
        if let Some(p) = parts.iter_mut().find(|p| p.partition == m.partition) {
            p.leader = m.to;
        }
    }
    out
}
