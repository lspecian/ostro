//! Phase 13: implemented in 13-06 (pure learner-first reconciler; D-P13-8).
//!
//! Fixed signatures: [`next`] maps one observation of the meta group to the single next
//! conf-change step. Stub: always [`Step::None`].

/// One member as the reconciler sees it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberObs {
    /// Node id.
    pub id: u64,
    /// `MemberState` (`ostro_meta::ext::MEMBER_*`).
    pub state: u8,
    /// Live per the liveness sampler.
    pub live: bool,
    /// Raft progress `matched` index (0 when unknown).
    pub matched: u64,
    /// A snapshot to this node is pending.
    pub pending_snapshot: bool,
}

/// One observation of the meta group (taken on the meta leader).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileInput {
    /// This node (the meta leader).
    pub leader: u64,
    /// Incoming voters.
    pub voters: Vec<u64>,
    /// Outgoing voters (non-empty while joint).
    pub voters_outgoing: Vec<u64>,
    /// Learners.
    pub learners: Vec<u64>,
    /// Every member record (tombstones included).
    pub members: Vec<MemberObs>,
    /// Raft commit index.
    pub commit: u64,
    /// A conf change is proposed but not yet applied.
    pub pending_conf_change: bool,
    /// `[cluster] promote_lag` (entries).
    pub promote_lag: u64,
    /// `ReplicationPolicy.max_voters`.
    pub max_voters: u8,
}

/// The single next conf-change step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Propose an empty `ConfChangeV2` to leave a joint configuration.
    LeaveJoint,
    /// Add a learner.
    AddLearner(u64),
    /// One joint `ConfChangeV2` (transition Auto) adding/removing voters and demoting voters
    /// to learners.
    Joint {
        add_voters: Vec<u64>,
        remove_voters: Vec<u64>,
        demote: Vec<u64>,
    },
    /// Remove a learner.
    RemoveLearner(u64),
    /// Transfer meta leadership before this leader's own removal.
    TransferLeader(u64),
    /// Nothing to do.
    None,
}

/// The next step for `input`. Stub: [`Step::None`].
pub fn next(input: &ReconcileInput) -> Step {
    let _ = input;
    Step::None
}
