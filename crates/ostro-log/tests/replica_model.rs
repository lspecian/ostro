//! Model-based test of the pure replica algorithms (12-04): one partition, three replicas,
//! a meta store with CAS on the ISR version, a leader running the real tracker / ISR /
//! watermark / divergence code, and a controller running the real failover planner.
//!
//! A record is identified by `(offset, epoch)` (log matching), so a log is the per-offset
//! epoch vector. Transitions: Tick, Append, Fsync(r), FollowerFetch(r), Crash(r) (loses the
//! non-durable tail; a crashed leader loses its leader state), Restart(r) (a restarted
//! leader resigns, D-P12-7), IsrPropose / IsrCommit (meta CAS) / IsrReject, Elect (planner
//! over the live set), MinInsync(m).
//!
//! Invariants (checked after every step):
//! - HW monotone per leader; no HW advance while |published ISR| < min_insync;
//! - HW <= every maximal-ISR member's durable LEO;
//! - every published ISR member durably holds every offset below every HW ever published,
//!   so **any elected leader's log contains all of them** (checked again at each election);
//! - no elected leader outside the published ISR; ISR never empty; leader ∈ ISR.

use std::collections::BTreeSet;

use ostro_log::EpochHistory;
use ostro_log::replica::divergence::{FetchCheck, check, follower_truncation_point};
use ostro_log::replica::failover::{AssignmentIn, Plan, plan};
use ostro_log::replica::isr::{IsrState, maximal, next_change};
use ostro_log::replica::tracker::ReplicaTracker;
use ostro_log::replica::watermark::{append_allowed, next_hw};
use ostro_types::{Epoch, Offset};
use proptest::prelude::*;
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest, prop_state_machine};

const IDS: [u64; 3] = [1, 2, 3];
const LAG_MS: u64 = 30;

#[derive(Clone, Debug)]
struct Node {
    log: Vec<u32>,
    durable: usize,
    alive: bool,
    hw: usize,
}

#[derive(Clone, Debug)]
struct LeaderState {
    id: u64,
    epoch: u32,
    tracker: ReplicaTracker,
    isr: IsrState,
    hw: usize,
    epoch_start: usize,
}

#[derive(Clone, Debug)]
struct Meta {
    leader: u64,
    epoch: u32,
    isr: Vec<u64>,
    version: u64,
}

#[derive(Clone, Debug)]
struct Sim {
    nodes: Vec<Node>,
    meta: Meta,
    ls: Option<LeaderState>,
    min_insync: usize,
    now: u64,
    /// The records below the highest HW ever published (any leader).
    committed: Vec<u32>,
    elections: usize,
    snapshots: usize,
}

#[derive(Clone, Debug)]
enum Transition {
    Tick,
    Append,
    Fsync(u64),
    FollowerFetch(u64),
    Crash(u64),
    Restart(u64),
    IsrPropose,
    IsrCommit,
    IsrReject,
    Elect,
    MinInsync(usize),
}

fn history(log: &[u32]) -> EpochHistory {
    let mut h: Vec<(Epoch, Offset)> = Vec::new();
    for (i, e) in log.iter().enumerate() {
        if h.last().map(|x| x.0) != Some(Epoch(*e)) {
            h.push((Epoch(*e), Offset(i as u64)));
        }
    }
    EpochHistory(h)
}

fn ix(id: u64) -> usize {
    (id - 1) as usize
}

impl Sim {
    fn new() -> Self {
        let mut nodes = vec![
            Node {
                log: Vec::new(),
                durable: 0,
                alive: true,
                hw: 0,
            };
            3
        ];
        nodes[0].log.push(1); // epoch-1 leader announcement
        Sim {
            nodes,
            meta: Meta {
                leader: 1,
                epoch: 1,
                isr: IDS.to_vec(),
                version: 1,
            },
            ls: Some(LeaderState {
                id: 1,
                epoch: 1,
                tracker: ReplicaTracker::new(1, &IDS, Offset(0), 0),
                isr: IsrState::new(IDS.to_vec(), 1),
                hw: 0,
                epoch_start: 0,
            }),
            min_insync: 2,
            now: 0,
            committed: Vec::new(),
            elections: 0,
            snapshots: 0,
        }
    }

    fn recompute_hw(&mut self) {
        let Some(ls) = self.ls.as_mut() else { return };
        let prev = ls.hw;
        let new = next_hw(
            Offset(prev as u64),
            &ls.tracker.progress(),
            &ls.isr,
            self.min_insync,
        )
        .0 as usize;
        assert!(new >= prev, "HW went back on one leader: {prev} -> {new}");
        if ls.isr.published.len() < self.min_insync {
            assert_eq!(new, prev, "HW advanced below min_insync");
        }
        if new == prev {
            return;
        }
        for m in maximal(&ls.isr) {
            assert!(
                self.nodes[ix(m)].durable >= new,
                "HW {new} above maximal-ISR member {m} durable {}",
                self.nodes[ix(m)].durable
            );
        }
        let leader = &mut self.nodes[ix(ls.id)];
        assert!(leader.durable >= new);
        let k = self.committed.len().min(new);
        assert_eq!(
            leader.log[..k],
            self.committed[..k],
            "leader log disagrees below a published HW"
        );
        if new > self.committed.len() {
            self.committed = leader.log[..new].to_vec();
        }
        ls.hw = new;
        leader.hw = new;
    }

    fn step(&mut self, t: &Transition) {
        match *t {
            Transition::Tick => self.now += 10,
            Transition::Append => {
                if let Some(ls) = &self.ls
                    && append_allowed(&ls.isr, self.min_insync)
                {
                    self.nodes[ix(ls.id)].log.push(ls.epoch);
                }
            }
            Transition::Fsync(r) => {
                let n = &mut self.nodes[ix(r)];
                if !n.alive {
                    return;
                }
                n.durable = n.log.len();
                let d = n.durable;
                if let Some(ls) = self.ls.as_mut()
                    && ls.id == r
                {
                    ls.tracker.on_local_durable(Offset(d as u64));
                }
                self.recompute_hw();
            }
            Transition::FollowerFetch(r) => self.fetch(r),
            Transition::Crash(r) => {
                let n = &mut self.nodes[ix(r)];
                if !n.alive {
                    return;
                }
                n.alive = false;
                n.log.truncate(n.durable);
                n.hw = n.hw.min(n.durable);
                if self.ls.as_ref().is_some_and(|ls| ls.id == r) {
                    self.ls = None;
                }
            }
            Transition::Restart(r) => {
                let n = &mut self.nodes[ix(r)];
                if n.alive {
                    return;
                }
                n.alive = true;
                if self.meta.leader == r {
                    // ResignLeader apply (D-P12-7): epoch + 1, leader 0, isr minus r if > 1.
                    self.meta.epoch += 1;
                    self.meta.leader = 0;
                    if self.meta.isr.len() > 1 {
                        self.meta.isr.retain(|x| *x != r);
                    }
                    self.meta.version += 1;
                }
            }
            Transition::IsrPropose => {
                let Some(ls) = self.ls.as_mut() else { return };
                if let Some(c) = next_change(
                    &ls.tracker,
                    &ls.isr,
                    &IDS,
                    Offset(ls.hw as u64),
                    Offset(ls.epoch_start as u64),
                    self.now,
                    LAG_MS,
                ) {
                    assert!(ls.isr.propose(c));
                }
            }
            Transition::IsrCommit => {
                let Some(ls) = self.ls.as_mut() else { return };
                let Some(c) = ls.isr.pending else { return };
                let new = ls.isr.proposed(c);
                // AlterIsr apply: epoch == current, version match, isr ⊆ replicas, leader ∈ isr.
                let ok = self.meta.epoch == ls.epoch
                    && self.meta.leader == ls.id
                    && self.meta.version == ls.isr.version
                    && new.iter().all(|n| IDS.contains(n))
                    && new.contains(&ls.id);
                if ok {
                    self.meta.isr = new;
                    self.meta.version += 1;
                    ls.isr.commit(self.meta.version);
                } else {
                    ls.isr.reject();
                }
                self.recompute_hw();
            }
            Transition::IsrReject => {
                if let Some(ls) = self.ls.as_mut() {
                    ls.isr.reject();
                }
                self.recompute_hw();
            }
            Transition::Elect => self.elect(),
            Transition::MinInsync(m) => {
                self.min_insync = m;
                self.recompute_hw();
            }
        }
    }

    fn fetch(&mut self, r: u64) {
        let Some(ls) = self.ls.as_mut() else { return };
        if r == ls.id || !self.nodes[ix(r)].alive {
            return;
        }
        let (lead_log, lead_durable) = {
            let l = &self.nodes[ix(ls.id)];
            (l.log.clone(), l.durable)
        };
        let f = &mut self.nodes[ix(r)];
        let fetch = f.log.len();
        let verdict = check(
            &history(&lead_log),
            Offset(0),
            Offset(lead_log.len() as u64),
            Offset(fetch as u64),
            f.log.last().map(|e| Epoch(*e)),
        );
        match verdict {
            FetchCheck::Ok => {
                assert_eq!(f.log[..], lead_log[..fetch], "Ok on a non-prefix log");
                ls.tracker.on_fetch(
                    r,
                    Offset(fetch as u64),
                    Offset(f.durable as u64),
                    Offset(lead_durable as u64),
                    self.now,
                );
                if fetch < lead_durable {
                    f.log.extend_from_slice(&lead_log[fetch..lead_durable]);
                }
                f.hw = f.hw.max(ls.hw.min(f.durable));
                self.recompute_hw();
            }
            FetchCheck::Diverging { epoch, end } => {
                let p =
                    follower_truncation_point(&history(&f.log), Offset(fetch as u64), epoch, end).0
                        as usize;
                assert!(p < fetch, "truncation must make progress");
                f.log.truncate(p);
                f.durable = f.durable.min(p);
                f.hw = f.hw.min(p);
            }
            FetchCheck::SnapshotRequired => {
                // Only reachable here when the follower holds records of an epoch older than
                // anything the leader ever had (D-P12-6: UNDEFINED, never truncate-to-HW).
                // Install the leader's state at its HW: the follower then holds exactly the
                // leader's prefix below HW, durably.
                assert!(
                    f.log.first() < lead_log.first(),
                    "snapshot for a follower whose epoch the leader knows: {lead_log:?} vs {:?}",
                    f.log
                );
                f.log = lead_log[..ls.hw].to_vec();
                f.durable = ls.hw;
                f.hw = ls.hw;
                self.snapshots += 1;
            }
        }
    }

    fn elect(&mut self) {
        let live: BTreeSet<u64> = IDS
            .iter()
            .copied()
            .filter(|id| self.nodes[ix(*id)].alive)
            .collect();
        let a = AssignmentIn {
            partition: 0,
            leader: self.meta.leader,
            epoch: self.meta.epoch,
            replicas: IDS.to_vec(),
            isr: self.meta.isr.clone(),
            isr_version: self.meta.version,
        };
        match plan(&a, &live, None) {
            Plan::Elect { leader, isr } => {
                // ElectLeader apply: isr non-empty, ⊆ current ISR, leader ∈ isr.
                assert!(self.meta.isr.contains(&leader), "elected {leader} ∉ ISR");
                assert!(!isr.is_empty() && isr.contains(&leader));
                assert!(isr.iter().all(|n| self.meta.isr.contains(n)));
                assert!(self.ls.is_none(), "election while a leader serves");
                self.meta.epoch += 1;
                self.meta.leader = leader;
                self.meta.isr = isr.clone();
                self.meta.version += 1;
                self.elections += 1;
                let n = &mut self.nodes[ix(leader)];
                let k = self.committed.len();
                assert!(
                    n.log.len() >= k && n.log[..k] == self.committed[..],
                    "new leader {leader} lacks committed records: {:?} vs {:?}",
                    n.log,
                    self.committed
                );
                let epoch_start = n.log.len();
                n.log.push(self.meta.epoch);
                self.ls = Some(LeaderState {
                    id: leader,
                    epoch: self.meta.epoch,
                    tracker: ReplicaTracker::new(leader, &IDS, Offset(n.durable as u64), self.now),
                    isr: IsrState::new(isr, self.meta.version),
                    hw: n.hw,
                    epoch_start,
                });
                self.recompute_hw();
            }
            Plan::Shrink { isr } => {
                assert!(isr.contains(&self.meta.leader));
                self.meta.isr = isr.clone();
                self.meta.version += 1;
                if let Some(ls) = self.ls.as_mut() {
                    ls.isr.set_published(isr, self.meta.version);
                }
                self.recompute_hw();
            }
            Plan::Offline { keep } => {
                assert!(self.ls.is_none());
                assert!(keep.len() == 1 && self.meta.isr.contains(&keep[0]));
                self.meta.epoch += 1;
                self.meta.leader = 0;
                self.meta.isr = keep;
                self.meta.version += 1;
            }
            Plan::Nothing => {}
        }
    }

    fn check(&self) {
        assert!(!self.meta.isr.is_empty(), "empty ISR");
        if self.meta.leader != 0 {
            assert!(self.meta.isr.contains(&self.meta.leader), "leader ∉ ISR");
        }
        let k = self.committed.len();
        for m in &self.meta.isr {
            let n = &self.nodes[ix(*m)];
            assert!(
                n.durable >= k && n.log[..k] == self.committed[..],
                "ISR member {m} does not durably hold the committed prefix: {:?} (durable {}) vs {:?}",
                n.log,
                n.durable,
                self.committed
            );
        }
        if let Some(ls) = &self.ls {
            assert_eq!(ls.id, self.meta.leader);
            assert_eq!(ls.epoch, self.meta.epoch);
            assert_eq!(ls.isr.published, self.meta.isr);
            for m in maximal(&ls.isr) {
                assert!(self.nodes[ix(m)].durable >= ls.hw, "HW above member {m}");
            }
        }
    }
}

struct Model;

impl ReferenceStateMachine for Model {
    type State = ();
    type Transition = Transition;

    fn init_state() -> BoxedStrategy<()> {
        Just(()).boxed()
    }

    fn transitions(_: &()) -> BoxedStrategy<Transition> {
        let id = || prop_oneof![Just(1u64), Just(2u64), Just(3u64)];
        prop_oneof![
            3 => Just(Transition::Tick),
            4 => Just(Transition::Append),
            4 => id().prop_map(Transition::Fsync),
            6 => id().prop_map(Transition::FollowerFetch),
            1 => id().prop_map(Transition::Crash),
            1 => id().prop_map(Transition::Restart),
            2 => Just(Transition::IsrPropose),
            2 => Just(Transition::IsrCommit),
            1 => Just(Transition::IsrReject),
            2 => Just(Transition::Elect),
            1 => (1usize..=3).prop_map(Transition::MinInsync),
        ]
        .boxed()
    }

    fn apply(state: (), _: &Transition) {
        state
    }
}

impl StateMachineTest for Sim {
    type SystemUnderTest = Sim;
    type Reference = Model;

    fn init_test(_: &()) -> Sim {
        Sim::new()
    }

    fn apply(mut state: Sim, _: &(), t: Transition) -> Sim {
        state.step(&t);
        state
    }

    fn check_invariants(state: &Sim, _: &()) {
        state.check();
    }
}

prop_state_machine! {
    #[test]
    fn replica_model_election_safety(sequential 1..200 => Sim);
}

/// A scripted run reaches a published HW, loses the leader, elects a successor that holds
/// it, and keeps advancing: the model is not vacuous.
#[test]
fn scripted_failover_keeps_committed_prefix() {
    use Transition::*;
    let mut s = Sim::new();
    let script = [
        Fsync(1),
        FollowerFetch(2),
        FollowerFetch(3),
        Fsync(2),
        Fsync(3),
        FollowerFetch(2),
        FollowerFetch(3),
        Append,
        Append,
        Fsync(1),
        FollowerFetch(2),
        Fsync(2),
        FollowerFetch(2),
    ];
    for t in &script {
        s.step(t);
        s.check();
    }
    // Replica 3 sits at 1 and holds the HW there.
    assert_eq!(s.committed.len(), 1, "{s:?}");
    // Replica 3 lags; the leader shrinks it (HW jumps to 3), then crashes.
    for _ in 0..4 {
        s.step(&Tick);
    }
    s.step(&FollowerFetch(2));
    s.step(&IsrPropose);
    s.step(&IsrCommit);
    s.check();
    assert_eq!(s.meta.isr, vec![1, 2]);
    assert_eq!(s.committed.len(), 3);
    s.step(&Crash(1));
    s.step(&Elect);
    s.check();
    assert_eq!(s.meta.leader, 2);
    assert_eq!(s.meta.isr, vec![2]);
    assert_eq!(s.elections, 1);
    // min_insync 2 with ISR {2}: frozen until 3 rejoins.
    s.step(&Append);
    assert_eq!(s.nodes[1].log.len(), 4, "append refused below min_insync");
    s.step(&MinInsync(1));
    s.step(&Append);
    s.step(&Fsync(2));
    s.check();
    assert_eq!(s.committed.len(), 5);
    // Replica 3 catches up and is expanded back.
    for _ in 0..4 {
        s.step(&FollowerFetch(3));
        s.step(&Fsync(3));
    }
    s.step(&IsrPropose);
    s.step(&IsrCommit);
    s.check();
    assert_eq!(s.meta.isr, vec![2, 3]);
}
