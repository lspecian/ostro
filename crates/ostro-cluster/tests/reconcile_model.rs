//! 13-06: the pure membership reconciler (`membership::reconcile::next`, D-P13-8).
//!
//! Unit tables for the plan's cases, then an action-sequence model (plain proptest; the
//! crate has no `proptest-state-machine` dev-dependency and `Cargo.*` is 13-01-only) that
//! drives a simulated meta group through joins, activations, drains, removals, liveness
//! flaps, lagging learners, lost proposals, meta leader changes (no raft auto-leave) and
//! checks every membership safety invariant on every emitted step and every applied config.

use std::collections::{BTreeMap, BTreeSet};

use ostro_cluster::membership::reconcile::{next, MemberObs, ReconcileInput, Step};
use ostro_meta::ext::{MEMBER_ACTIVE, MEMBER_DRAINING, MEMBER_JOINING, MEMBER_REMOVED};
use proptest::prelude::*;

const LAG: u64 = 64;
const COMMIT: u64 = 1_000;

fn m(id: u64, state: u8, live: bool, matched: u64) -> MemberObs {
    MemberObs {
        id,
        state,
        live,
        matched,
        pending_snapshot: false,
    }
}

fn active(id: u64) -> MemberObs {
    m(id, MEMBER_ACTIVE, true, COMMIT)
}

fn input(voters: &[u64], learners: &[u64], members: Vec<MemberObs>) -> ReconcileInput {
    ReconcileInput {
        leader: 1,
        voters: voters.to_vec(),
        voters_outgoing: vec![],
        learners: learners.to_vec(),
        members,
        commit: COMMIT,
        pending_conf_change: false,
        promote_lag: LAG,
        max_voters: 5,
    }
}

fn joint(add: &[u64], remove: &[u64], demote: &[u64]) -> Step {
    Step::Joint {
        add_voters: add.to_vec(),
        remove_voters: remove.to_vec(),
        demote: demote.to_vec(),
    }
}

// ---------------------------------------------------------------------------------------
// Unit tables (13-06 plan, reconciler cases)
// ---------------------------------------------------------------------------------------

#[test]
fn joint_config_is_left_explicitly() {
    let mut i = input(&[1, 2, 3], &[], vec![active(1), active(2), active(3)]);
    i.voters_outgoing = vec![3];
    assert_eq!(next(&i), Step::LeaveJoint);
    i.pending_conf_change = true;
    assert_eq!(next(&i), Step::None);
}

#[test]
fn pending_change_blocks_everything() {
    let mut i = input(&[1], &[], vec![active(1), m(4, MEMBER_JOINING, true, 0)]);
    i.pending_conf_change = true;
    assert_eq!(next(&i), Step::None);
}

#[test]
fn joining_member_becomes_learner() {
    let i = input(&[1], &[], vec![active(1), m(4, MEMBER_JOINING, true, 0)]);
    assert_eq!(next(&i), Step::AddLearner(4));
}

#[test]
fn growth_1_to_3_is_one_joint_change() {
    let i = input(&[1], &[2, 3], vec![active(1), active(2), active(3)]);
    assert_eq!(next(&i), joint(&[2, 3], &[], &[]));
}

#[test]
fn growth_waits_for_two_caught_up_learners() {
    let i = input(
        &[1],
        &[2, 3],
        vec![active(1), active(2), m(3, MEMBER_ACTIVE, true, COMMIT - LAG - 1)],
    );
    assert_eq!(next(&i), Step::None);
}

#[test]
fn two_members_keep_one_voter() {
    let i = input(&[1], &[2], vec![active(1), active(2)]);
    assert_eq!(next(&i), Step::None);
}

#[test]
fn growth_3_to_5() {
    let ms = (1..=5).map(active).collect();
    let i = input(&[1, 2, 3], &[4, 5], ms);
    assert_eq!(next(&i), joint(&[4, 5], &[], &[]));
}

#[test]
fn six_members_stay_at_five_voters() {
    let ms = (1..=6).map(active).collect();
    let i = input(&[1, 2, 3, 4, 5], &[6], ms);
    assert_eq!(next(&i), Step::None);
}

#[test]
fn max_voters_is_enforced() {
    let ms = (1..=7).map(active).collect();
    let i = input(&[1, 2, 3, 4, 5], &[6, 7], ms);
    assert_eq!(next(&i), Step::None);
    let mut i = input(&[1, 2, 3], &[4, 5], (1..=5).map(active).collect());
    i.max_voters = 3;
    assert_eq!(next(&i), Step::None);
    i.max_voters = 4;
    assert_eq!(next(&i), Step::None, "largest odd ≤ 4 is 3");
}

#[test]
fn removing_a_voter_shrinks_3_to_1() {
    let i = input(
        &[1, 2, 3],
        &[],
        vec![active(1), active(2), m(3, MEMBER_REMOVED, true, COMMIT)],
    );
    assert_eq!(next(&i), joint(&[], &[3], &[2]));
}

#[test]
fn removal_needs_a_live_quorum_of_the_remaining_voters() {
    let i = input(
        &[1, 2, 3],
        &[],
        vec![
            active(1),
            m(2, MEMBER_ACTIVE, false, COMMIT),
            m(3, MEMBER_REMOVED, true, COMMIT),
        ],
    );
    assert_eq!(next(&i), Step::None);
}

#[test]
fn removing_two_of_five_is_one_joint_change() {
    let mut ms: Vec<_> = (1..=3).map(active).collect();
    ms.push(m(4, MEMBER_REMOVED, true, COMMIT));
    ms.push(m(5, MEMBER_REMOVED, true, COMMIT));
    let i = input(&[1, 2, 3, 4, 5], &[], ms);
    assert_eq!(next(&i), joint(&[], &[4, 5], &[]));
}

#[test]
fn removing_self_transfers_leadership_first() {
    let i = input(
        &[1, 2, 3],
        &[],
        vec![
            m(1, MEMBER_REMOVED, true, COMMIT),
            m(2, MEMBER_ACTIVE, true, COMMIT - 10),
            active(3),
        ],
    );
    assert_eq!(next(&i), Step::TransferLeader(3), "best caught-up live voter");
    let i = input(
        &[1, 2, 3],
        &[],
        vec![
            m(1, MEMBER_REMOVED, true, COMMIT),
            active(2),
            m(3, MEMBER_ACTIVE, false, COMMIT),
        ],
    );
    assert_eq!(next(&i), Step::TransferLeader(2), "never to a dead voter");
}

#[test]
fn draining_leader_transfers_before_its_demotion() {
    let i = input(
        &[1, 2, 3],
        &[],
        vec![m(1, MEMBER_DRAINING, true, COMMIT), active(2), active(3)],
    );
    assert_eq!(next(&i), Step::TransferLeader(2));
}

#[test]
fn lagging_or_snapshotting_learners_are_not_promoted() {
    let mut snap = active(3);
    snap.pending_snapshot = true;
    let i = input(&[1], &[2, 3], vec![active(1), active(2), snap]);
    assert_eq!(next(&i), Step::None);
    let i = input(
        &[1],
        &[2, 3],
        vec![active(1), active(2), m(3, MEMBER_ACTIVE, false, COMMIT)],
    );
    assert_eq!(next(&i), Step::None, "dead learner");
    let i = input(
        &[1],
        &[2, 3],
        vec![active(1), active(2), m(3, MEMBER_JOINING, true, COMMIT)],
    );
    assert_eq!(next(&i), Step::None, "a Joining learner is not promoted");
}

#[test]
fn replace_is_one_auto_joint_change() {
    let i = input(
        &[1, 2, 3],
        &[4],
        vec![
            active(1),
            active(2),
            m(3, MEMBER_REMOVED, true, COMMIT),
            active(4),
        ],
    );
    assert_eq!(next(&i), joint(&[4], &[3], &[]));
}

#[test]
fn removed_learner_is_removed() {
    let i = input(
        &[1],
        &[2],
        vec![active(1), m(2, MEMBER_REMOVED, false, 0)],
    );
    assert_eq!(next(&i), Step::RemoveLearner(2));
}

#[test]
fn removed_member_is_never_re_added() {
    let i = input(&[1], &[], vec![active(1), m(2, MEMBER_REMOVED, true, 0)]);
    assert_eq!(next(&i), Step::None);
}

#[test]
fn conf_member_without_a_record_freezes_the_reconciler() {
    let i = input(&[1, 2, 3], &[], vec![active(1), active(2)]);
    assert_eq!(next(&i), Step::None);
}

#[test]
fn dead_voter_is_not_dropped_without_a_replacement() {
    let i = input(
        &[1, 2, 3],
        &[],
        vec![active(1), active(2), m(3, MEMBER_ACTIVE, false, 0)],
    );
    assert_eq!(next(&i), Step::None);
    let i = input(
        &[1, 2, 3],
        &[4],
        vec![
            active(1),
            active(2),
            m(3, MEMBER_ACTIVE, false, 0),
            active(4),
        ],
    );
    assert_eq!(next(&i), joint(&[4], &[], &[3]), "replaced by a caught-up learner");
}

// ---------------------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct Mem {
    state: u8,
    live: bool,
    matched: u64,
    snap: bool,
}

#[derive(Clone, Debug)]
struct Model {
    me: u64,
    members: BTreeMap<u64, Mem>,
    voters: BTreeSet<u64>,
    outgoing: BTreeSet<u64>,
    learners: BTreeSet<u64>,
    pending: Option<Step>,
    commit: u64,
    tombstones: BTreeSet<u64>,
    next_id: u64,
    max_voters: u8,
}

#[derive(Clone, Debug)]
enum Action {
    Join,
    Activate(usize),
    Drain(usize),
    Remove(usize),
    SetLive(usize, bool),
    CatchUp(usize),
    Lag(usize),
    Snapshot(usize, bool),
    Commit(u64),
    Tick,
    Apply,
    DropPending,
    AutoLeave,
    LeaderChange(usize),
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        2 => Just(Action::Join),
        2 => any::<usize>().prop_map(Action::Activate),
        1 => any::<usize>().prop_map(Action::Drain),
        1 => any::<usize>().prop_map(Action::Remove),
        2 => (any::<usize>(), any::<bool>()).prop_map(|(i, b)| Action::SetLive(i, b)),
        3 => any::<usize>().prop_map(Action::CatchUp),
        1 => any::<usize>().prop_map(Action::Lag),
        1 => (any::<usize>(), any::<bool>()).prop_map(|(i, b)| Action::Snapshot(i, b)),
        1 => (1u64..200).prop_map(Action::Commit),
        6 => Just(Action::Tick),
        4 => Just(Action::Apply),
        1 => Just(Action::DropPending),
        1 => Just(Action::AutoLeave),
        1 => any::<usize>().prop_map(Action::LeaderChange),
    ]
}

fn majority(n: usize) -> usize {
    n / 2 + 1
}

impl Model {
    fn new(max_voters: u8) -> Self {
        let mut members = BTreeMap::new();
        members.insert(
            1,
            Mem {
                state: MEMBER_ACTIVE,
                live: true,
                matched: 0,
                snap: false,
            },
        );
        Model {
            me: 1,
            members,
            voters: BTreeSet::from([1]),
            outgoing: BTreeSet::new(),
            learners: BTreeSet::new(),
            pending: None,
            commit: 0,
            tombstones: BTreeSet::new(),
            next_id: 2,
            max_voters,
        }
    }

    fn pick(&self, i: usize) -> u64 {
        let ids: Vec<u64> = self.members.keys().copied().collect();
        ids[i % ids.len()]
    }

    /// The driver-side precondition for Drain/Remove (13-09 `check_remove` shape): a voter
    /// may leave only while another live Active voter stays and the remaining voters keep a
    /// live quorum, so the last Active voter never leaves.
    fn may_leave(&self, id: u64) -> bool {
        if !self.voters.contains(&id) && !self.outgoing.contains(&id) {
            return true;
        }
        let rest: BTreeSet<u64> = self
            .voters
            .iter()
            .copied()
            .filter(|v| *v != id && !self.tombstones.contains(v))
            .collect();
        let other_active = rest
            .iter()
            .any(|v| self.members[v].state == MEMBER_ACTIVE && self.live(*v));
        other_active && self.live_count(&rest) >= majority(rest.len())
    }

    /// A leader outside the incoming voters (only after leaving a joint config it entered
    /// under another leader) is replaced by a live incoming voter.
    fn reelect_if_not_voter(&mut self) {
        if self.outgoing.is_empty() && !self.voters.contains(&self.me) {
            if let Some(v) = self.voters.iter().copied().find(|v| self.live(*v)) {
                self.me = v;
                self.pending = None;
            }
        }
    }

    fn live(&self, id: u64) -> bool {
        self.members.get(&id).is_some_and(|m| m.live)
    }

    fn input(&self) -> ReconcileInput {
        ReconcileInput {
            leader: self.me,
            voters: self.voters.iter().copied().collect(),
            voters_outgoing: self.outgoing.iter().copied().collect(),
            learners: self.learners.iter().copied().collect(),
            members: self
                .members
                .iter()
                .map(|(&id, x)| MemberObs {
                    id,
                    state: x.state,
                    live: x.live,
                    matched: if id == self.me { self.commit } else { x.matched },
                    pending_snapshot: x.snap,
                })
                .collect(),
            commit: self.commit,
            pending_conf_change: self.pending.is_some(),
            promote_lag: LAG,
            max_voters: self.max_voters,
        }
    }

    fn live_count(&self, set: &BTreeSet<u64>) -> usize {
        set.iter().filter(|id| self.live(**id)).count()
    }

    /// Checks an emitted step against the safety invariants; returns a failure message.
    fn check_step(&self, step: &Step) -> Result<(), String> {
        if self.pending.is_some() {
            return if *step == Step::None {
                Ok(())
            } else {
                Err(format!("{step:?} while a change is pending"))
            };
        }
        if !self.outgoing.is_empty() && *step != Step::LeaveJoint {
            return Err(format!("joint config not left: {step:?}"));
        }
        let max = usize::from(self.max_voters.max(1));
        match step {
            Step::None | Step::LeaveJoint => {}
            Step::AddLearner(id) => {
                if self.tombstones.contains(id) {
                    return Err(format!("tombstoned {id} re-added"));
                }
                if self.voters.contains(id) || self.learners.contains(id) {
                    return Err(format!("{id} already in the conf"));
                }
            }
            Step::RemoveLearner(id) => {
                if !self.learners.contains(id) {
                    return Err(format!("RemoveLearner({id}) of a non-learner"));
                }
            }
            Step::TransferLeader(to) => {
                if !self.voters.contains(to) || *to == self.me || !self.live(*to) {
                    return Err(format!("bad transfer target {to}"));
                }
            }
            Step::Joint {
                add_voters,
                remove_voters,
                demote,
            } => {
                let mut new = self.voters.clone();
                for id in remove_voters.iter().chain(demote) {
                    if !self.voters.contains(id) {
                        return Err(format!("{id} removed/demoted but not a voter"));
                    }
                    if *id == self.me {
                        return Err("the leader removes/demotes itself".into());
                    }
                    new.remove(id);
                }
                for id in add_voters {
                    let x = &self.members[id];
                    if !self.learners.contains(id)
                        || x.state != MEMBER_ACTIVE
                        || !x.live
                        || x.snap
                        || x.matched < self.commit.saturating_sub(LAG)
                    {
                        return Err(format!("promoted {id} not a ready learner: {x:?}"));
                    }
                    if self.tombstones.contains(id) {
                        return Err(format!("tombstoned {id} promoted"));
                    }
                    new.insert(*id);
                }
                if new == self.voters {
                    return Err("empty joint change".into());
                }
                if new.len() % 2 == 0 || self.voters.len() % 2 == 0 {
                    return Err(format!("even half: {:?} -> {new:?}", self.voters));
                }
                if new.len() > max {
                    return Err(format!("{} voters > max {max}", new.len()));
                }
                if self.live_count(&new) < majority(new.len()) {
                    return Err(format!("new config {new:?} has no live quorum"));
                }
                let mut rest = self.voters.clone();
                for id in remove_voters {
                    rest.remove(id);
                }
                if self.live_count(&rest) < majority(rest.len()) {
                    return Err(format!("remaining voters {rest:?} have no live quorum"));
                }
            }
        }
        Ok(())
    }

    fn check_config(&self) -> Result<(), String> {
        if self.voters.len() % 2 == 0 {
            return Err(format!("even incoming voters {:?}", self.voters));
        }
        if !self.outgoing.is_empty() && self.outgoing.len() % 2 == 0 {
            return Err(format!("even outgoing voters {:?}", self.outgoing));
        }
        if self.voters.len() > usize::from(self.max_voters.max(1)) && self.outgoing.is_empty() {
            // Only reachable if the initial config were too large; the model starts at 1.
            return Err(format!("{} voters > max", self.voters.len()));
        }
        if !self.voters.is_disjoint(&self.learners) {
            return Err("voter is also a learner".into());
        }
        Ok(())
    }

    fn apply(&mut self, step: Step) {
        match step {
            Step::None => {}
            Step::LeaveJoint => self.outgoing.clear(),
            Step::AddLearner(id) => {
                self.learners.insert(id);
            }
            Step::RemoveLearner(id) => {
                self.learners.remove(&id);
            }
            Step::TransferLeader(to) => {
                if self.voters.contains(&to) && self.live(to) {
                    self.me = to;
                }
            }
            Step::Joint {
                add_voters,
                remove_voters,
                demote,
            } => {
                self.outgoing = self.voters.clone();
                for id in remove_voters {
                    self.voters.remove(&id);
                }
                for id in demote {
                    self.voters.remove(&id);
                    self.learners.insert(id);
                }
                for id in add_voters {
                    self.learners.remove(&id);
                    self.voters.insert(id);
                }
            }
        }
    }

    fn act(&mut self, a: &Action) -> Result<(), String> {
        match *a {
            Action::Join => {
                let id = self.next_id;
                self.next_id += 1;
                self.members.insert(
                    id,
                    Mem {
                        state: MEMBER_JOINING,
                        live: true,
                        matched: 0,
                        snap: true,
                    },
                );
            }
            Action::Activate(i) => {
                let id = self.pick(i);
                let x = self.members.get_mut(&id).unwrap();
                if x.state == MEMBER_JOINING {
                    x.state = MEMBER_ACTIVE;
                }
            }
            Action::Drain(i) => {
                let id = self.pick(i);
                if self.may_leave(id) {
                    let x = self.members.get_mut(&id).unwrap();
                    if x.state == MEMBER_ACTIVE {
                        x.state = MEMBER_DRAINING;
                    }
                }
            }
            Action::Remove(i) => {
                let id = self.pick(i);
                if self.may_leave(id) {
                    self.members.get_mut(&id).unwrap().state = MEMBER_REMOVED;
                    self.tombstones.insert(id);
                }
            }
            Action::SetLive(i, live) => {
                let id = self.pick(i);
                if id != self.me {
                    self.members.get_mut(&id).unwrap().live = live;
                }
            }
            Action::CatchUp(i) => {
                let id = self.pick(i);
                let commit = self.commit;
                let x = self.members.get_mut(&id).unwrap();
                x.matched = commit;
                x.snap = false;
            }
            Action::Lag(i) => {
                let id = self.pick(i);
                if id != self.me {
                    self.members.get_mut(&id).unwrap().matched = 0;
                }
            }
            Action::Snapshot(i, s) => {
                let id = self.pick(i);
                if id != self.me {
                    self.members.get_mut(&id).unwrap().snap = s;
                }
            }
            Action::Commit(k) => self.commit += k,
            Action::Tick => {
                let step = next(&self.input());
                self.check_step(&step)?;
                if step != Step::None {
                    self.pending = Some(step);
                }
            }
            Action::Apply => {
                if let Some(step) = self.pending.take() {
                    self.apply(step);
                    self.check_config()?;
                    self.reelect_if_not_voter();
                }
            }
            Action::DropPending => self.pending = None,
            Action::AutoLeave => {
                if self.pending.is_none() {
                    self.outgoing.clear();
                    self.reelect_if_not_voter();
                }
            }
            Action::LeaderChange(i) => {
                let cands: Vec<u64> = self
                    .voters
                    .iter()
                    .copied()
                    .filter(|id| self.live(*id))
                    .collect();
                if !cands.is_empty() {
                    self.me = cands[i % cands.len()];
                    self.pending = None;
                }
            }
        }
        if self.pending.is_none() {
            self.check_config()?;
        }
        Ok(())
    }

    /// Heals the cluster and runs the reconciler to a fixpoint; returns the number of steps.
    fn settle(&mut self) -> Result<usize, String> {
        if let Some(step) = self.pending.take() {
            self.apply(step);
            self.check_config()?;
            self.reelect_if_not_voter();
        }
        for n in 0..64 {
            let commit = self.commit;
            for x in self.members.values_mut() {
                x.live = true;
                x.matched = commit;
                x.snap = false;
            }
            let step = next(&self.input());
            self.check_step(&step)?;
            if step == Step::None {
                return Ok(n);
            }
            self.apply(step);
            self.check_config()?;
            self.reelect_if_not_voter();
        }
        Err("reconciler did not reach a fixpoint in 64 steps".into())
    }

    fn check_settled(&self) -> Result<(), String> {
        if !self.outgoing.is_empty() {
            return Err("joint config never left".into());
        }
        let in_conf: BTreeSet<u64> = self.voters.union(&self.learners).copied().collect();
        if !in_conf.is_disjoint(&self.tombstones) {
            return Err(format!("tombstones still in the conf: {in_conf:?}"));
        }
        let active: BTreeSet<u64> = self
            .members
            .iter()
            .filter(|(_, x)| x.state == MEMBER_ACTIVE)
            .map(|(id, _)| *id)
            .collect();
        if active.is_empty() {
            return Ok(());
        }
        let cap = active.len().min(usize::from(self.max_voters.max(1)));
        let want = if cap % 2 == 0 { cap - 1 } else { cap };
        if self.voters.len() != want {
            return Err(format!(
                "settled at {} voters, want {want} ({:?}, active {active:?})",
                self.voters.len(),
                self.voters
            ));
        }
        if !self.voters.is_subset(&active) {
            return Err(format!("non-Active voter after settling: {:?}", self.voters));
        }
        for (id, x) in &self.members {
            let wanted = x.state == MEMBER_JOINING || x.state == MEMBER_ACTIVE;
            if wanted && !in_conf.contains(id) {
                return Err(format!("member {id} ({}) not in the conf", x.state));
            }
        }
        Ok(())
    }
}

proptest! {
    #[test]
    fn membership_model(
        max_voters in prop_oneof![Just(1u8), Just(3), Just(4), Just(5), Just(7)],
        actions in proptest::collection::vec(action(), 1..120),
    ) {
        let mut model = Model::new(max_voters);
        for a in &actions {
            if let Err(e) = model.act(a) {
                prop_assert!(false, "after {a:?}: {e}\nmodel: {model:?}");
            }
        }
        if let Err(e) = model.settle().and_then(|_| model.check_settled()) {
            prop_assert!(false, "settling: {e}\nmodel: {model:?}");
        }
    }
}
