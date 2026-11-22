//! Failover planner (D-P12-7, B1/W1): a pure function of the meta assignment and the live
//! set. No unclean election: only a live **published ISR** member is ever elected, and the
//! elected ISR is `isr ∩ live` (an election only shrinks the ISR).

use std::collections::BTreeSet;

use crate::mutants;

/// The planner's view of one partition assignment (meta).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssignmentIn {
    /// Partition id.
    pub partition: u16,
    /// Current leader (`0` = offline).
    pub leader: u64,
    /// Current leader epoch (`0` = unplaced).
    pub epoch: u32,
    /// Assigned replicas in preference order.
    pub replicas: Vec<u64>,
    /// Published ISR.
    pub isr: Vec<u64>,
    /// Meta CAS token of `isr`.
    pub isr_version: u64,
}

/// What the controller should propose.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
    /// `ElectLeader{leader, isr}`: `leader ∈ isr`, `isr = old isr ∩ live`, non-empty.
    Elect {
        /// The new leader.
        leader: u64,
        /// The new ISR.
        isr: Vec<u64>,
    },
    /// `AlterIsr` removing dead followers (leader alive).
    Shrink {
        /// The new ISR (leader included).
        isr: Vec<u64>,
    },
    /// No live ISR member: `ElectLeader{leader: 0, isr: keep}` with one kept member.
    Offline {
        /// The single ISR member kept.
        keep: Vec<u64>,
    },
    /// No change.
    Nothing,
}

/// Plan the next step for `a` given the `live` nodes and an optional `resigned` node.
///
/// - epoch 0 (unplaced) or an empty ISR → `Nothing` (bootstrap placement owns it).
/// - leader live (and not `resigned`): dead ISR followers → `Shrink{isr ∩ live}`, else
///   `Nothing`.
/// - otherwise the first replica in `replicas` order that is in the ISR, live and not
///   `resigned` → `Elect{leader, isr ∩ live}`.
/// - no such member → `Offline{keep}`: the current leader when it is in the ISR (the last
///   member known to have accepted writes), otherwise the last ISR member. Already offline
///   with a single member → `Nothing`.
pub fn plan(a: &AssignmentIn, live: &BTreeSet<u64>, resigned: Option<u64>) -> Plan {
    if a.epoch == 0 || a.isr.is_empty() {
        return Plan::Nothing;
    }
    let live_isr: Vec<u64> = a.isr.iter().copied().filter(|n| live.contains(n)).collect();
    let leader_up = a.leader != 0 && live.contains(&a.leader) && resigned != Some(a.leader);
    if leader_up {
        if live_isr.len() < a.isr.len() && live_isr.contains(&a.leader) {
            return Plan::Shrink { isr: live_isr };
        }
        return Plan::Nothing;
    }
    let eligible = |n: &u64| -> bool {
        live.contains(n)
            && resigned != Some(*n)
            && (a.isr.contains(n) || super::mutant(mutants::UNCLEAN_ELECTION))
    };
    if let Some(leader) = a.replicas.iter().copied().find(eligible) {
        let mut isr = live_isr;
        if !isr.contains(&leader) {
            // Only reachable under the unclean-election mutant.
            isr.push(leader);
        }
        return Plan::Elect { leader, isr };
    }
    if a.leader == 0 && a.isr.len() == 1 {
        return Plan::Nothing;
    }
    let keep = if a.isr.contains(&a.leader) {
        a.leader
    } else {
        *a.isr.last().expect("non-empty ISR")
    };
    Plan::Offline { keep: vec![keep] }
}
