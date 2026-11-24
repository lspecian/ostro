//! Phase 13 (13-06): the pure learner-first membership reconciler (D-P13-8).
//!
//! [`next`] maps one observation of the meta group (taken on the meta leader) to the single
//! next conf-change step. No I/O, no clock: deterministic for a given input.
//!
//! Decision order:
//! 1. a conf change is pending → [`Step::None`] (one change at a time);
//! 2. `voters_outgoing` non-empty → [`Step::LeaveJoint`] (never trust raft-rs auto-leave,
//!    raft.rs:983);
//! 3. a conf member without a `Member` record → [`Step::None`] (records are written before
//!    any conf change, D-P13-9; an incomplete view is never acted on);
//! 4. the voter step: the desired voter set `N` is built from the current Active voters
//!    (the leader first, then live, most caught-up), then caught-up live Active learners, then dead
//!    Active voters, truncated to `n` = largest odd ≤ min(Active members that are live or
//!    already voters, `max_voters`) and to the largest odd ≤ the candidates available. If
//!    `N ≠ voters`: when the leader itself is not in `N` → [`Step::TransferLeader`] to the
//!    best caught-up live Active voter; otherwise one [`Step::Joint`] (transition Auto)
//!    adding `N \ voters`, removing Removed voters and demoting the rest — provided the
//!    current voters, the new config `N` and the remaining voters `voters \ remove` each
//!    keep a live quorum;
//! 5. a Removed learner → [`Step::RemoveLearner`];
//! 6. a Joining/Active member outside the conf → [`Step::AddLearner`].
//!
//! Both halves of every joint config are odd (growth 1→3→5 and shrink 5→3→1 are one joint
//! change, plan check W4). A dead voter keeps counting towards `n`, so a transient failure
//! never shrinks the group; it is replaced only by a caught-up learner.

use std::collections::{BTreeMap, BTreeSet};

use ostro_meta::ext::{MEMBER_ACTIVE, MEMBER_JOINING, MEMBER_REMOVED};

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

/// The next step for `input`.
pub fn next(input: &ReconcileInput) -> Step {
    if input.pending_conf_change {
        return Step::None;
    }
    if !input.voters_outgoing.is_empty() {
        return Step::LeaveJoint;
    }
    let members: BTreeMap<u64, &MemberObs> = input.members.iter().map(|m| (m.id, m)).collect();
    let voters: BTreeSet<u64> = input.voters.iter().copied().collect();
    let learners: BTreeSet<u64> = input.learners.iter().copied().collect();
    if voters
        .iter()
        .chain(&learners)
        .any(|id| !members.contains_key(id))
    {
        return Step::None;
    }
    let ctx = Ctx {
        input,
        members: &members,
    };
    if let Some(step) = voter_step(&ctx, &voters, &learners) {
        return step;
    }
    if let Some(id) = learners
        .iter()
        .copied()
        .find(|id| members[id].state == MEMBER_REMOVED)
    {
        return Step::RemoveLearner(id);
    }
    if let Some(m) = input
        .members
        .iter()
        .filter(|m| {
            (m.state == MEMBER_JOINING || m.state == MEMBER_ACTIVE)
                && !voters.contains(&m.id)
                && !learners.contains(&m.id)
        })
        .min_by_key(|m| m.id)
    {
        return Step::AddLearner(m.id);
    }
    Step::None
}

struct Ctx<'a> {
    input: &'a ReconcileInput,
    members: &'a BTreeMap<u64, &'a MemberObs>,
}

impl Ctx<'_> {
    fn state(&self, id: u64) -> u8 {
        self.members[&id].state
    }

    fn live(&self, id: u64) -> bool {
        self.members[&id].live
    }

    fn caught_up(&self, id: u64) -> bool {
        let m = self.members[&id];
        m.matched >= self.input.commit.saturating_sub(self.input.promote_lag)
    }

    fn ready_learner(&self, id: u64) -> bool {
        let m = self.members[&id];
        m.state == MEMBER_ACTIVE && m.live && !m.pending_snapshot && self.caught_up(id)
    }

    fn live_quorum(&self, set: &BTreeSet<u64>) -> bool {
        let live = set.iter().filter(|id| self.live(**id)).count();
        live > set.len() / 2
    }
}

fn largest_odd_at_most(n: usize) -> usize {
    if n == 0 {
        0
    } else if n.is_multiple_of(2) {
        n - 1
    } else {
        n
    }
}

/// The voter step (Joint or TransferLeader), or `None` when the voter set is settled or no
/// safe change exists right now.
fn voter_step(ctx: &Ctx<'_>, voters: &BTreeSet<u64>, learners: &BTreeSet<u64>) -> Option<Step> {
    let me = ctx.input.leader;
    let active = |id: &u64| ctx.state(*id) == MEMBER_ACTIVE;

    // Voters that may stay: Active ones, the leader first, then live, most caught-up, id.
    let mut stay: Vec<u64> = voters.iter().copied().filter(active).collect();
    stay.sort_by_key(|id| {
        (
            *id != me,
            !ctx.live(*id),
            std::cmp::Reverse(ctx.members[id].matched),
            *id,
        )
    });
    let (stay_live, stay_dead): (Vec<u64>, Vec<u64>) =
        stay.into_iter().partition(|id| ctx.live(*id));
    // Caught-up live Active learners, most caught-up first.
    let mut promote: Vec<u64> = learners
        .iter()
        .copied()
        .filter(|id| ctx.ready_learner(*id))
        .collect();
    promote.sort_by_key(|id| (std::cmp::Reverse(ctx.members[id].matched), *id));

    let eligible = ctx
        .input
        .members
        .iter()
        .filter(|m| m.state == MEMBER_ACTIVE && (m.live || voters.contains(&m.id)))
        .count();
    let max = usize::from(ctx.input.max_voters.max(1));
    let cands: Vec<u64> = stay_live
        .iter()
        .chain(&promote)
        .chain(&stay_dead)
        .copied()
        .collect();
    let n = largest_odd_at_most(eligible.min(max).min(cands.len()));
    if n == 0 {
        return None;
    }
    let new: BTreeSet<u64> = cands.into_iter().take(n).collect();
    if new == *voters {
        return None;
    }

    if !new.contains(&me) {
        // Never demote/remove the leader itself (raft-rs leaves that branch untested):
        // hand leadership to the best caught-up live Active voter, preferring one that stays.
        let to = voters
            .iter()
            .copied()
            .filter(|id| *id != me && active(id) && ctx.live(*id) && ctx.caught_up(*id))
            .min_by_key(|id| {
                (
                    !new.contains(id),
                    std::cmp::Reverse(ctx.members[id].matched),
                    *id,
                )
            });
        return to.map(Step::TransferLeader);
    }

    let remove: BTreeSet<u64> = voters
        .iter()
        .copied()
        .filter(|id| !new.contains(id) && ctx.state(*id) == MEMBER_REMOVED)
        .collect();
    let rest: BTreeSet<u64> = voters.difference(&remove).copied().collect();
    if !ctx.live_quorum(voters) || !ctx.live_quorum(&new) || !ctx.live_quorum(&rest) {
        return None;
    }
    Some(Step::Joint {
        add_voters: new.difference(voters).copied().collect(),
        remove_voters: remove.iter().copied().collect(),
        demote: voters
            .iter()
            .copied()
            .filter(|id| !new.contains(id) && !remove.contains(id))
            .collect(),
    })
}
