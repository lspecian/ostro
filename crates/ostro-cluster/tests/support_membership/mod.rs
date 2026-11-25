//! In-process metadata groups for the 13-09 membership driver suites: the 13-05
//! `membership_handle` router pattern (raft messages stepped directly between handles), one
//! `MembershipService` per node ticked by the test, a test-controlled live set and a manual
//! clock.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_cluster::membership::remove::{RemoveError, check_remove};
use ostro_cluster::membership::service::{
    MembershipConfig, MembershipService, TickOutcome, conf_change, set_member_state,
};
use ostro_meta::ext::{MEMBER_ACTIVE, MEMBER_REMOVED};
use ostro_meta::ext::{MemberState, put_member_state};
use ostro_meta::handle::{ForwardFuture, MetaOptions, MetaStart, ProposeForwarder, TickConfig};
use ostro_meta::{
    Member, MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, ProposeError, RaftHandle,
};
use ostro_partition::{Clock as _, ManualClock};
use ostro_raft_driver::driver::ChannelOutbox;
use ostro_raft_driver::raft::eraftpb::Message;
use ostro_raft_driver::raft::eraftpb::{ConfChangeTransition, ConfChangeType};
use tempfile::TempDir;
use tokio::sync::mpsc;

pub const LONG: Duration = Duration::from_secs(30);
const T: Duration = LONG;

fn ticks() -> TickConfig {
    TickConfig {
        interval: Duration::from_millis(50),
        election_tick: 10,
        heartbeat_tick: 2,
    }
}

#[derive(Clone, Default)]
pub struct Net {
    handles: Arc<Mutex<HashMap<u64, RaftHandle>>>,
    /// Nodes whose traffic (both ways) is dropped.
    cut: Arc<Mutex<BTreeSet<u64>>>,
}

impl Net {
    fn get(&self, id: u64) -> Option<RaftHandle> {
        self.handles.lock().unwrap().get(&id).cloned()
    }
}

struct LocalForwarder(Net);

impl ProposeForwarder for LocalForwarder {
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_> {
        Box::pin(async move {
            match self.0.get(leader) {
                Some(h) => match h.propose(cmd).await {
                    Err(ProposeError::Stopped) => Err(ProposeError::OutcomeUnknown),
                    other => other,
                },
                None => Err(ProposeError::NotLeader { leader: None }),
            }
        })
    }
}

/// A metadata group with one membership driver per node.
pub struct Group {
    pub net: Net,
    pub initial: Vec<u64>,
    dirs: HashMap<u64, TempDir>,
    pub nodes: HashMap<u64, Arc<MetaHandle>>,
    drivers: HashMap<u64, Arc<MembershipService>>,
    /// Nodes the live source reports dead (traffic still flows unless also cut).
    pub dead: Arc<Mutex<BTreeSet<u64>>>,
    pub clock: Arc<ManualClock>,
    pub cfg: MembershipConfig,
}

impl Group {
    /// Start `voters` (a fresh group) and write a Phase 12 style Member record (no
    /// MemberState) for each.
    pub async fn new(voters: &[u64]) -> Group {
        let mut g = Group {
            net: Net::default(),
            initial: voters.to_vec(),
            dirs: HashMap::new(),
            nodes: HashMap::new(),
            drivers: HashMap::new(),
            dead: Arc::new(Mutex::new(BTreeSet::new())),
            clock: Arc::new(ManualClock::new(1_000_000)),
            cfg: MembershipConfig {
                promote_lag: 64,
                join_orphan_timeout: Duration::from_secs(3600),
                interval: Duration::from_millis(20),
            },
        };
        for &id in voters {
            g.start(id, false).await;
        }
        for h in g.nodes.values() {
            h.wait_ready().await.unwrap();
        }
        for &id in voters {
            g.put_member(id, None).await;
        }
        g
    }

    pub async fn start(&mut self, id: u64, joiner: bool) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Vec<Message>>();
        let dir = self
            .dirs
            .entry(id)
            .or_insert_with(|| TempDir::new().unwrap());
        let start = MetaStart {
            node_id: id,
            voters: self.initial.clone(),
            outbox: Box::new(ChannelOutbox(tx)),
            forwarder: Some(Arc::new(LocalForwarder(self.net.clone()))),
            tick: ticks(),
        };
        let opts = MetaOptions {
            sync: false,
            ..MetaOptions::default()
        };
        let h = if joiner {
            MetaHandle::start_joiner(dir.path(), start, opts).await
        } else {
            MetaHandle::start_with_options(dir.path(), start, opts).await
        }
        .expect("meta start");
        self.net
            .handles
            .lock()
            .unwrap()
            .insert(id, h.raft().clone());
        let net = self.net.clone();
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                for m in batch {
                    let cut = net.cut.lock().unwrap();
                    if cut.contains(&m.from) || cut.contains(&m.to) {
                        continue;
                    }
                    drop(cut);
                    if let Some(to) = net.get(m.to) {
                        to.step(m);
                    }
                }
            }
        });
        let h = Arc::new(h);
        let dead = Arc::clone(&self.dead);
        let svc = MembershipService::new(Arc::clone(&h), self.cfg.clone())
            .with_clock(self.clock.clone())
            .with_live_source(Arc::new(move |u: &BTreeSet<u64>| {
                let dead = dead.lock().unwrap();
                u.iter().copied().filter(|id| !dead.contains(id)).collect()
            }));
        self.drivers.insert(id, Arc::new(svc));
        self.nodes.insert(id, h);
    }

    /// Stop node `id` (gone from the network).
    pub async fn kill(&mut self, id: u64) {
        self.net.handles.lock().unwrap().remove(&id);
        self.drivers.remove(&id);
        if let Some(h) = self.nodes.remove(&id)
            && let Ok(h) = Arc::try_unwrap(h)
        {
            h.stop().await;
        }
        self.dead.lock().unwrap().insert(id);
    }

    pub async fn leader(&self) -> u64 {
        let deadline = Instant::now() + LONG;
        loop {
            let leaders: Vec<u64> = self
                .nodes
                .iter()
                .filter(|(_, h)| h.is_leader())
                .map(|(&i, _)| i)
                .collect();
            if let [l] = leaders[..] {
                return l;
            }
            assert!(Instant::now() < deadline, "no single leader: {leaders:?}");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn leader_handle(&self) -> Arc<MetaHandle> {
        let l = self.leader().await;
        Arc::clone(&self.nodes[&l])
    }

    /// Put (create or replace) Member `id` with an optional MemberState.
    pub async fn put_member(&self, id: u64, state: Option<u8>) {
        let leader = self.leader_handle().await;
        let ext = match state {
            Some(s) => put_member_state(
                &[],
                &MemberState {
                    state: s,
                    changed_at_ms: self.clock.now_ms(),
                },
            )
            .unwrap(),
            None => Vec::new(),
        };
        let cur = leader.view().read(|s| s.member(id).map(|m| m.version));
        let res = leader
            .propose(MetaCommand::Put {
                key: MetaKey::Member(id),
                expected_version: cur,
                record: MetaRecord::Member(Member {
                    node_id: id,
                    addr: format!("api-{id}"),
                    joined_at_ms: 1,
                    cluster_addr: Some(format!("mem://{id}")),
                    repl_addr: None,
                    ext,
                }),
            })
            .await
            .unwrap();
        assert!(matches!(res, MetaResult::Ok { .. }), "{res:?}");
    }

    /// A joining node: its Joining record (what `handle_join` writes), then its meta started
    /// as a joiner.
    pub async fn join(&mut self, id: u64) {
        self.put_member(id, Some(ostro_meta::ext::MEMBER_JOINING))
            .await;
        self.start(id, true).await;
    }

    pub async fn set_state(&self, id: u64, state: u8) {
        let leader = self.leader_handle().await;
        set_member_state(&leader, id, state, self.clock.now_ms())
            .await
            .unwrap();
    }

    /// One tick of every driver; the outcomes that did something.
    pub async fn tick(&self) -> Vec<(u64, TickOutcome)> {
        let mut out = Vec::new();
        let mut ids: Vec<u64> = self.drivers.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let o = self.drivers[&id].tick().await;
            if !matches!(
                o,
                TickOutcome::NotLeader | TickOutcome::Idle | TickOutcome::Static
            ) {
                out.push((id, o));
            }
        }
        out
    }

    /// Tick until `f` holds on the leader's status (bounded by `within`); every applied
    /// configuration seen on the way is checked by `check`.
    pub async fn drive_until(
        &self,
        what: &str,
        within: Duration,
        mut f: impl FnMut(&ostro_meta::RaftStatus) -> bool,
    ) -> Vec<(u64, TickOutcome)> {
        let deadline = Instant::now() + within;
        let mut log = Vec::new();
        loop {
            log.extend(self.tick().await);
            let st = self.leader_handle().await.raft_detail();
            assert_odd(&st);
            if f(&st) {
                return log;
            }
            assert!(
                Instant::now() < deadline,
                "timed out driving to {what}; status voters {:?} learners {:?} outgoing {:?}; steps {log:?}",
                st.voters,
                st.learners,
                st.voters_outgoing
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn state_of(&self, id: u64) -> Option<u8> {
        let h = self.nodes.values().next()?;
        h.view().read(|s| {
            s.member(id)
                .map(ostro_cluster::membership::service::member_state_of_versioned)
        })
    }

    pub async fn stop_all(self) {
        for (_, h) in self.nodes {
            if let Ok(h) = Arc::try_unwrap(h) {
                h.stop().await;
            }
        }
    }
}

/// Every non-joint voter set is odd; both halves of a joint configuration are odd.
pub fn assert_odd(st: &ostro_meta::RaftStatus) {
    if st.voters.is_empty() {
        return;
    }
    assert!(st.voters.len() % 2 == 1, "even voter set {:?}", st.voters);
    if !st.voters_outgoing.is_empty() {
        assert!(
            st.voters_outgoing.len() % 2 == 1,
            "even outgoing half {:?}",
            st.voters_outgoing
        );
    }
}

pub fn sorted(v: &[u64]) -> Vec<u64> {
    let mut v = v.to_vec();
    v.sort_unstable();
    v
}

/// Shared with the mutant suite: a joint configuration left behind by a dead leader is left
/// by the next leader's driver. Returns whether the joint state was left within `within`.
pub async fn leave_joint_after_leader_kill(within: Duration) -> bool {
    let mut g = Group::new(&[1, 2, 3]).await;
    g.join(4).await;
    g.drive_until("4 active", T, |st| st.learners == [4]).await;
    g.drive_until("4 active", T, |_| g.state_of(4) == Some(MEMBER_ACTIVE))
        .await;
    let leader = g.leader().await;
    let demote = [1, 2, 3].into_iter().find(|v| *v != leader).unwrap();
    // An explicit joint change (raft never auto-leaves it), as if the leader died between
    // the joint entry and its auto-leave.
    let mut cc = conf_change(&[
        (ConfChangeType::AddNode, 4),
        (ConfChangeType::AddLearnerNode, demote),
    ]);
    cc.set_transition(ConfChangeTransition::Explicit);
    g.nodes[&leader]
        .propose_conf_change(cc)
        .await
        .expect("joint");
    assert!(!g.nodes[&leader].raft_detail().voters_outgoing.is_empty());
    g.kill(leader).await;
    let deadline = std::time::Instant::now() + within;
    loop {
        g.tick().await;
        let left = g
            .nodes
            .values()
            .find(|h| h.is_leader())
            .map(|h| h.raft_detail())
            .is_some_and(|st| st.voters_outgoing.is_empty());
        if left {
            g.stop_all().await;
            return true;
        }
        if std::time::Instant::now() >= deadline {
            g.stop_all().await;
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Shared with the mutant suite: with 2 of 3 voters dead, a Removed voter is not removed.
/// Returns the voter set after `ticks` driver ticks.
pub async fn quorum_refusal(ticks: usize) -> Vec<u64> {
    let g = Group::new(&[1, 2, 3]).await;
    let leader = g.leader().await;
    let others: Vec<u64> = [1, 2, 3].into_iter().filter(|v| *v != leader).collect();
    g.set_state(leader, MEMBER_ACTIVE).await;
    g.set_state(others[1], MEMBER_REMOVED).await;
    g.dead.lock().unwrap().extend(others.iter().copied());
    assert_eq!(
        check_remove(others[1], &[1, 2, 3], &[leader], true),
        Err(RemoveError::NoQuorum { id: others[1] })
    );
    for _ in 0..ticks {
        g.tick().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let v = sorted(&g.leader_handle().await.raft_detail().voters);
    g.stop_all().await;
    v
}
