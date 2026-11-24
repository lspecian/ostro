//! Phase 13: implemented in 13-06 (pure reassignment state machine; D-P13-12).
//!
//! Fixed signatures. Stub: [`next_step`] answers [`ReassignStep::None`].

use ostro_meta::ext::Reassignment;

/// One partition as the reassignment machine sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionObs {
    /// Partition.
    pub partition: u16,
    /// Leader epoch (0 = unplaced: never touched).
    pub epoch: u32,
    /// Leader (0 = offline).
    pub leader: u64,
    /// Current replicas.
    pub replicas: Vec<u64>,
    /// Published ISR.
    pub isr: Vec<u64>,
    /// `isr_version` (the CAS token of AlterReplicas).
    pub isr_version: u64,
    /// `min_insync`.
    pub min_insync: u8,
    /// In-flight reassignment (ext 100).
    pub reassignment: Option<Reassignment>,
}

/// The next step of one partition's reassignment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReassignStep {
    /// `AlterReplicas` with `replicas = old ∪ target` and the reassignment ext.
    Expand {
        replicas: Vec<u64>,
        reassignment: Reassignment,
    },
    /// Wait for the adding replicas to join the ISR.
    CatchUp,
    /// Graceful handoff to `to` (leader ∉ target).
    LeaderMove { to: u64 },
    /// `AlterReplicas` with `replicas = target`, `isr ∩ target`, `min_insync`, ext cleared.
    Shrink {
        replicas: Vec<u64>,
        isr: Vec<u64>,
        min_insync: u8,
    },
    /// Nothing to do.
    None,
}

/// The next step for `p` towards `target` (policy `min_insync`). Stub: `None`.
pub fn next_step(p: &PartitionObs, target: &[u64], policy_min_insync: u8) -> ReassignStep {
    let _ = (p, target, policy_min_insync);
    ReassignStep::None
}
