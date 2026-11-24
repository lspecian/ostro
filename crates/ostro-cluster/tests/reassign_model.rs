//! 13-06: the pure reassignment state machine (`placement::reassign`, D-P13-12).
//!
//! Unit cases, then an action-sequence model: a simulated partition (replicas, ISR, leader,
//! epoch, min_insync, ext 100) under controller steps, honest ISR expansion, ISR shrinks,
//! failovers, failed handoffs, cancels and re-targets. Every emitted `AlterReplicas` is
//! validated with the D-P13-12 apply rules and the min_insync rules (plan check W5).

use std::collections::BTreeSet;

use ostro_cluster::placement::reassign::{
    cancel_target, next_step, next_step_at, shrink_min_insync, PartitionObs, ReassignStep,
};
use ostro_meta::ext::Reassignment;
use proptest::prelude::*;

fn part(replicas: &[u64], isr: &[u64], leader: u64, min_insync: u8) -> PartitionObs {
    PartitionObs {
        partition: 3,
        epoch: 1,
        leader,
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version: 1,
        min_insync,
        reassignment: None,
    }
}

/// The AlterReplicas a controller would propose for `step` (None for non-proposals).
fn alter(p: &PartitionObs, step: &ReassignStep) -> Option<(Vec<u64>, Vec<u64>, u8, Option<Reassignment>)> {
    match step {
        ReassignStep::Expand { replicas, reassignment } => Some((
            replicas.clone(),
            p.isr.clone(),
            p.min_insync,
            Some(reassignment.clone()),
        )),
        ReassignStep::Shrink { replicas, isr, min_insync } => {
            Some((replicas.clone(), isr.clone(), *min_insync, None))
        }
        _ => None,
    }
}

/// The D-P13-12 `AlterReplicas` apply validation.
fn valid_alter(p: &PartitionObs, replicas: &[u64], isr: &[u64], min_insync: u8) -> Result<(), String> {
    let rs: BTreeSet<u64> = replicas.iter().copied().collect();
    let is: BTreeSet<u64> = isr.iter().copied().collect();
    if p.epoch == 0 {
        return Err("AlterReplicas at epoch 0".into());
    }
    if rs.len() != replicas.len() || is.len() != isr.len() {
        return Err(format!("duplicates: {replicas:?} / {isr:?}"));
    }
    if p.leader != 0 && !rs.contains(&p.leader) {
        return Err(format!("leader {} not in replicas {replicas:?}", p.leader));
    }
    if !is.is_subset(&rs) {
        return Err(format!("isr {isr:?} ⊄ replicas {replicas:?}"));
    }
    if p.leader != 0 && !is.contains(&p.leader) {
        return Err(format!("leader {} not in isr {isr:?}", p.leader));
    }
    if min_insync < 1 || usize::from(min_insync) > isr.len() {
        return Err(format!("min_insync {min_insync} outside 1..=|isr| ({})", isr.len()));
    }
    Ok(())
}

fn apply(p: &mut PartitionObs, step: &ReassignStep) {
    if let Some((replicas, isr, min_insync, ext)) = alter(p, step) {
        p.replicas = replicas;
        p.isr = isr;
        p.min_insync = min_insync;
        p.reassignment = ext;
        p.isr_version += 1;
    }
    if let ReassignStep::LeaderMove { to } = step {
        p.leader = *to;
        p.epoch += 1;
    }
}

fn join_isr(p: &mut PartitionObs, id: u64) {
    if p.replicas.contains(&id) && !p.isr.contains(&id) {
        p.isr.push(id);
        p.isr_version += 1;
    }
}

/// Runs the machine with honest catch-up until it answers `None`; returns the steps taken.
fn drive(p: &mut PartitionObs, target: &[u64], policy: u8) -> Vec<ReassignStep> {
    let mut steps = vec![];
    for _ in 0..16 {
        let s = next_step(p, target, policy);
        if s == ReassignStep::None {
            return steps;
        }
        if s == ReassignStep::CatchUp {
            for id in p.replicas.clone() {
                join_isr(p, id);
            }
        }
        apply(p, &s);
        steps.push(s);
    }
    panic!("no fixpoint: {steps:?}");
}

// ---------------------------------------------------------------------------------------
// Unit cases
// ---------------------------------------------------------------------------------------

#[test]
fn epoch_zero_is_never_touched() {
    let mut p = part(&[], &[], 0, 1);
    p.epoch = 0;
    assert_eq!(next_step(&p, &[1, 2, 3], 2), ReassignStep::None);
}

#[test]
fn done_when_replicas_equal_target() {
    let p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    assert_eq!(next_step(&p, &[1, 2, 3], 2), ReassignStep::None);
}

#[test]
fn rf1_to_3_raises_min_insync_only_in_shrink() {
    let mut p = part(&[1], &[1], 1, 1);
    let s = next_step_at(&p, &[1, 2, 3], 2, 77);
    assert_eq!(
        s,
        ReassignStep::Expand {
            replicas: vec![1, 2, 3],
            reassignment: Reassignment {
                target: vec![1, 2, 3],
                adding: vec![2, 3],
                removing: vec![],
                started_at_ms: 77,
            },
        }
    );
    apply(&mut p, &s);
    assert_eq!(p.min_insync, 1);
    assert_eq!(next_step(&p, &[1, 2, 3], 2), ReassignStep::CatchUp);
    join_isr(&mut p, 2);
    assert_eq!(next_step(&p, &[1, 2, 3], 2), ReassignStep::CatchUp);
    join_isr(&mut p, 3);
    assert_eq!(
        next_step(&p, &[1, 2, 3], 2),
        ReassignStep::Shrink {
            replicas: vec![1, 2, 3],
            isr: vec![1, 2, 3],
            min_insync: 2,
        }
    );
}

#[test]
fn shrink_waits_for_target_in_isr() {
    let mut p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    let s = next_step(&p, &[1, 2, 4], 2);
    assert!(matches!(s, ReassignStep::Expand { .. }));
    apply(&mut p, &s);
    assert_eq!(p.replicas, vec![1, 2, 3, 4]);
    // 4 lags; even if 3 drops out of the ISR the machine waits (never lowers min_insync)
    assert_eq!(next_step(&p, &[1, 2, 4], 2), ReassignStep::CatchUp);
    p.isr.retain(|x| *x != 3);
    assert_eq!(next_step(&p, &[1, 2, 4], 2), ReassignStep::CatchUp);
    join_isr(&mut p, 4);
    assert_eq!(
        next_step(&p, &[1, 2, 4], 2),
        ReassignStep::Shrink {
            replicas: vec![1, 2, 4],
            isr: vec![1, 2, 4],
            min_insync: 2,
        }
    );
}

#[test]
fn rf3_to_1_with_min_insync_2() {
    let mut p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    let steps = drive(&mut p, &[1], 2);
    assert_eq!(steps.len(), 2, "{steps:?}");
    assert_eq!(p.replicas, vec![1]);
    assert_eq!(p.isr, vec![1]);
    assert_eq!(p.min_insync, 1);
    assert_eq!(p.reassignment, None);
}

#[test]
fn leader_moves_before_shrink_when_not_in_target() {
    let mut p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    let steps = drive(&mut p, &[2, 3, 4], 2);
    assert!(steps.contains(&ReassignStep::LeaderMove { to: 2 }), "{steps:?}");
    assert_eq!(p.replicas, vec![2, 3, 4]);
    assert_eq!(p.leader, 2);
}

#[test]
fn reorder_only_needs_no_leader_move() {
    let mut p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    let steps = drive(&mut p, &[2, 1, 3], 2);
    assert_eq!(steps.len(), 2, "{steps:?}");
    assert_eq!(p.replicas, vec![2, 1, 3]);
}

#[test]
fn cancel_before_leader_move_restores_old_replicas() {
    let mut p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    assert_eq!(cancel_target(&p), None, "nothing in flight");
    let s = next_step(&p, &[4, 5, 6], 2);
    apply(&mut p, &s);
    assert_eq!(p.replicas, vec![1, 2, 3, 4, 5, 6]);
    let old = cancel_target(&p).expect("cancel before LeaderMove");
    assert_eq!(old, vec![1, 2, 3]);
    drive(&mut p, &old, 2);
    assert_eq!(p.replicas, vec![1, 2, 3]);
    assert_eq!(p.leader, 1);
    assert_eq!(p.reassignment, None);
    assert_eq!(p.min_insync, 2);
}

#[test]
fn cancel_refused_after_leader_move() {
    let mut p = part(&[1, 2, 3], &[1, 2, 3], 1, 2);
    let s = next_step(&p, &[4, 5, 6], 2);
    apply(&mut p, &s);
    for id in [4, 5, 6] {
        join_isr(&mut p, id);
    }
    let s = next_step(&p, &[4, 5, 6], 2);
    assert_eq!(s, ReassignStep::LeaderMove { to: 4 });
    apply(&mut p, &s);
    assert_eq!(cancel_target(&p), None);
}

#[test]
fn expand_waits_while_isr_is_below_min_insync() {
    let p = part(&[1, 2, 3], &[1], 1, 2);
    assert_eq!(next_step(&p, &[1, 4, 5], 2), ReassignStep::None);
}

#[test]
fn shrink_min_insync_formula() {
    // min(max(current, min(policy, |target|)), |target|)
    assert_eq!(shrink_min_insync(1, 2, 3), 2);
    assert_eq!(shrink_min_insync(2, 2, 1), 1);
    assert_eq!(shrink_min_insync(3, 2, 3), 3);
    assert_eq!(shrink_min_insync(1, 3, 2), 2);
    assert_eq!(shrink_min_insync(0, 0, 3), 1);
}

// ---------------------------------------------------------------------------------------
// Model
// ---------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Action {
    Step,
    StepHandoffFails,
    IsrJoin(usize),
    IsrLeave(usize),
    Failover(usize),
    Offline,
    Cancel,
    Retarget(Vec<u64>),
}

fn target_strategy() -> impl Strategy<Value = Vec<u64>> {
    proptest::sample::subsequence((1u64..=6).collect::<Vec<_>>(), 1..=3).prop_shuffle()
}

fn action() -> impl Strategy<Value = Action> {
    prop_oneof![
        8 => Just(Action::Step),
        1 => Just(Action::StepHandoffFails),
        5 => any::<usize>().prop_map(Action::IsrJoin),
        2 => any::<usize>().prop_map(Action::IsrLeave),
        1 => any::<usize>().prop_map(Action::Failover),
        1 => Just(Action::Offline),
        1 => Just(Action::Cancel),
        1 => target_strategy().prop_map(Action::Retarget),
    ]
}

fn initial() -> impl Strategy<Value = PartitionObs> {
    (
        proptest::sample::subsequence((1u64..=5).collect::<Vec<_>>(), 1..=3).prop_shuffle(),
        proptest::collection::vec(any::<bool>(), 3),
        any::<proptest::sample::Index>(),
    )
        .prop_map(|(replicas, in_isr, mi)| {
            let leader = replicas[0];
            let isr: Vec<u64> = replicas
                .iter()
                .zip(in_isr)
                .filter(|(r, b)| **r == leader || *b)
                .map(|(r, _)| *r)
                .collect();
            let min_insync = 1 + mi.index(isr.len()) as u8;
            part(&replicas, &isr, leader, min_insync)
        })
}

struct Sim {
    p: PartitionObs,
    target: Vec<u64>,
    policy: u8,
}

impl Sim {
    fn check_state(&self) -> Result<(), String> {
        let p = &self.p;
        let rs: BTreeSet<u64> = p.replicas.iter().copied().collect();
        if !p.isr.iter().all(|x| rs.contains(x)) {
            return Err(format!("isr ⊄ replicas: {p:?}"));
        }
        if p.leader != 0 && !p.isr.contains(&p.leader) {
            return Err(format!("leader not in isr: {p:?}"));
        }
        Ok(())
    }

    fn step(&mut self, handoff_ok: bool) -> Result<(), String> {
        let p = &self.p;
        let s = next_step(p, &self.target, self.policy);
        let floor = self.policy.min(self.target.len() as u8).max(1);
        if let Some((replicas, isr, mi, _)) = alter(p, &s) {
            valid_alter(p, &replicas, &isr, mi).map_err(|e| format!("{s:?}: {e} (from {p:?})"))?;
            if mi > p.min_insync && !matches!(s, ReassignStep::Shrink { .. }) {
                return Err(format!("{s:?} raises min_insync"));
            }
            if mi < p.min_insync && mi < floor {
                return Err(format!("{s:?} lowers min_insync below min(policy, |target|) = {floor}"));
            }
        }
        match &s {
            ReassignStep::Expand { replicas, reassignment } => {
                if !p.replicas.iter().chain(&self.target).all(|x| replicas.contains(x)) {
                    return Err(format!("Expand {replicas:?} ⊉ old ∪ target"));
                }
                if reassignment.target != self.target {
                    return Err(format!("Expand ext target {:?} ≠ {:?}", reassignment.target, self.target));
                }
            }
            ReassignStep::Shrink { replicas, isr, min_insync } => {
                if *replicas != self.target {
                    return Err(format!("Shrink to {replicas:?}, target {:?}", self.target));
                }
                if !self.target.iter().all(|x| p.isr.contains(x)) {
                    return Err(format!("Shrink before target ⊆ ISR: {p:?}"));
                }
                if usize::from(*min_insync) > self.target.len() || usize::from(*min_insync) > isr.len() {
                    return Err(format!("Shrink min_insync {min_insync} > |target|/|isr|"));
                }
                if *min_insync != shrink_min_insync(p.min_insync, self.policy, self.target.len()) {
                    return Err("Shrink min_insync ≠ the W5 formula".into());
                }
                if p.leader != 0 && !self.target.contains(&p.leader) {
                    return Err("Shrink drops the leader".into());
                }
            }
            ReassignStep::LeaderMove { to } => {
                if !self.target.contains(to) || !p.isr.contains(to) || *to == p.leader || p.leader == 0 {
                    return Err(format!("LeaderMove to {to}: {p:?} target {:?}", self.target));
                }
            }
            ReassignStep::CatchUp => {
                if self.target.iter().all(|x| p.isr.contains(x)) && p.reassignment.is_some() {
                    return Err(format!("CatchUp with target ⊆ ISR: {p:?}"));
                }
            }
            ReassignStep::None => {}
        }
        if handoff_ok || !matches!(s, ReassignStep::LeaderMove { .. }) {
            apply(&mut self.p, &s);
        }
        self.check_state()
    }

    fn act(&mut self, a: &Action) -> Result<(), String> {
        match a {
            Action::Step => self.step(true)?,
            Action::StepHandoffFails => self.step(false)?,
            Action::IsrJoin(i) => {
                let r = self.p.replicas.clone();
                if !r.is_empty() {
                    join_isr(&mut self.p, r[i % r.len()]);
                }
            }
            Action::IsrLeave(i) => {
                let cands: Vec<u64> =
                    self.p.isr.iter().copied().filter(|x| *x != self.p.leader).collect();
                if !cands.is_empty() {
                    let x = cands[i % cands.len()];
                    self.p.isr.retain(|y| *y != x);
                    self.p.isr_version += 1;
                }
            }
            Action::Failover(i) => {
                let cands: Vec<u64> =
                    self.p.isr.iter().copied().filter(|x| *x != self.p.leader).collect();
                if !cands.is_empty() {
                    self.p.leader = cands[i % cands.len()];
                    self.p.epoch += 1;
                }
            }
            Action::Offline => {
                if self.p.isr.len() > 1 {
                    let old = self.p.leader;
                    self.p.isr.retain(|y| *y != old);
                    self.p.leader = 0;
                    // Phase 12 keeps min_insync, so |isr| < min_insync is reachable here.
                    self.p.epoch += 1;
                }
            }
            Action::Cancel => {
                if let Some(old) = cancel_target(&self.p) {
                    let orig: Vec<u64> = {
                        let ext = self.p.reassignment.as_ref().unwrap();
                        self.p.replicas.iter().copied().filter(|x| !ext.adding.contains(x)).collect()
                    };
                    if old != orig {
                        return Err(format!("cancel target {old:?} ≠ original {orig:?}"));
                    }
                    if self.p.leader != 0 && !old.contains(&self.p.leader) {
                        return Err("cancel allowed after the leader moved".into());
                    }
                    self.target = old;
                }
            }
            Action::Retarget(t) => self.target = t.clone(),
        }
        self.check_state()
    }

    /// Heals (every replica joins the ISR, a leader is elected, handoffs succeed) and drives
    /// the machine to completion.
    fn settle(&mut self) -> Result<(), String> {
        for _ in 0..16 {
            for id in self.p.replicas.clone() {
                join_isr(&mut self.p, id);
            }
            if self.p.leader == 0 {
                self.p.leader = self.p.isr[0];
                self.p.epoch += 1;
            }
            if next_step(&self.p, &self.target, self.policy) == ReassignStep::None {
                break;
            }
            self.step(true)?;
        }
        let p = &self.p;
        if p.replicas != self.target || p.reassignment.is_some() {
            return Err(format!("not settled at target {:?}: {p:?}", self.target));
        }
        if !self.target.contains(&p.leader) {
            return Err(format!("leader {} outside target", p.leader));
        }
        Ok(())
    }
}

proptest! {
    #[test]
    fn reassign_model(
        p in initial(),
        target in target_strategy(),
        policy in 1u8..=3,
        actions in proptest::collection::vec(action(), 0..60),
    ) {
        let mut sim = Sim { p, target, policy };
        for a in &actions {
            if let Err(e) = sim.act(a) {
                prop_assert!(false, "after {a:?}: {e}");
            }
        }
        let started = sim.p.replicas != sim.target || sim.p.reassignment.is_some();
        if let Err(e) = sim.settle() {
            prop_assert!(false, "{e}");
        }
        if started {
            let floor = policy.min(sim.target.len() as u8).max(1);
            prop_assert!(sim.p.min_insync >= floor, "ended below min(policy, |target|): {:?}", sim.p);
        }
        prop_assert!(usize::from(sim.p.min_insync) <= sim.target.len());
    }
}
