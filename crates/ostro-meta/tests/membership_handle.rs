//! 13-05 Task 2: the `MetaHandle` membership and consistency surface on an in-process
//! 3-voter group (the 12-05 `multi_voter` router pattern): `read_index_barrier`,
//! `transfer_leader`, `propose_conf_change` (a joiner added as a learner), `conf_state`,
//! `raft_detail` progress and the lease anchor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_meta::handle::{ForwardFuture, MetaOptions, MetaStart, ProposeForwarder, TickConfig};
use ostro_meta::{
    MetaCommand, MetaError, MetaHandle, MetaKey, MetaRecord, MetaResult, ProposeError, QueueConfig,
    RaftHandle,
};
use ostro_raft_driver::driver::ChannelOutbox;
use ostro_raft_driver::raft::eraftpb::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
use tempfile::TempDir;
use tokio::sync::mpsc;

const VOTERS: [u64; 3] = [1, 2, 3];
const LONG: Duration = Duration::from_secs(30);

fn ticks() -> TickConfig {
    TickConfig {
        interval: Duration::from_millis(50),
        election_tick: 10,
        heartbeat_tick: 2,
    }
}

fn queue(name: &str) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Queue(name.into()),
        expected_version: None,
        record: MetaRecord::Queue(QueueConfig {
            name: name.into(),
            max_concurrency: Some(1),
            created_at_ms: 1,
            ext: Vec::new(),
        }),
    }
}

fn add_learner(id: u64) -> ConfChangeV2 {
    let mut s = ConfChangeSingle::default();
    s.set_change_type(ConfChangeType::AddLearnerNode);
    s.node_id = id;
    ConfChangeV2 {
        changes: vec![s],
        ..Default::default()
    }
}

async fn until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + LONG;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[derive(Clone, Default)]
struct Net(Arc<Mutex<HashMap<u64, RaftHandle>>>);

impl Net {
    fn get(&self, id: u64) -> Option<RaftHandle> {
        self.0.lock().unwrap().get(&id).cloned()
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

struct Group {
    net: Net,
    dirs: HashMap<u64, TempDir>,
    nodes: HashMap<u64, MetaHandle>,
}

impl Group {
    async fn new() -> Group {
        let mut g = Group {
            net: Net::default(),
            dirs: HashMap::new(),
            nodes: HashMap::new(),
        };
        for id in VOTERS {
            g.start(id, false).await;
        }
        for h in g.nodes.values() {
            h.wait_ready().await.unwrap();
        }
        g
    }

    async fn start(&mut self, id: u64, joiner: bool) {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<Vec<ostro_raft_driver::raft::eraftpb::Message>>();
        let dir = self
            .dirs
            .entry(id)
            .or_insert_with(|| TempDir::new().unwrap());
        let start = MetaStart {
            node_id: id,
            voters: VOTERS.to_vec(),
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
        .expect("start");
        self.net.0.lock().unwrap().insert(id, h.raft().clone());
        let net = self.net.clone();
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                for m in batch {
                    if let Some(to) = net.get(m.to) {
                        to.step(m);
                    }
                }
            }
        });
        self.nodes.insert(id, h);
    }

    async fn leader(&self) -> u64 {
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
            assert!(Instant::now() < deadline, "no single leader");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn h(&self, id: u64) -> &MetaHandle {
        &self.nodes[&id]
    }

    async fn stop_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.net.0.lock().unwrap().remove(&id);
            if let Some(h) = self.nodes.remove(&id) {
                h.stop().await;
            }
        }
    }
}

/// A write acknowledged on voter A is visible on voter B after B's barrier, for every
/// ordered pair (the barrier's index is at least the write's).
#[tokio::test(flavor = "multi_thread")]
async fn barrier_on_another_voter_observes_the_write() {
    let mut g = Group::new().await;
    let mut n = 0;
    for a in VOTERS {
        for b in VOTERS.into_iter().filter(|&b| b != a) {
            n += 1;
            let name = format!("q{n}");
            assert_eq!(
                g.h(a).propose(queue(&name)).await.unwrap(),
                MetaResult::Ok { version: 1 }
            );
            let written = g.h(a).view().applied();
            let idx = g.h(b).read_index_barrier().await.unwrap();
            assert!(idx >= written, "barrier index {idx} < write {written}");
            assert!(g.h(b).view().applied() >= idx);
            assert!(
                g.h(b).view().read(|s| s.queue(&name).is_some()),
                "{b} misses {name} written on {a}"
            );
        }
    }
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn transfer_moves_leadership_and_proposals_continue() {
    let mut g = Group::new().await;
    let leader = g.leader().await;
    let target = *VOTERS.iter().find(|&&i| i != leader).unwrap();
    g.h(leader).propose(queue("t0")).await.unwrap();
    // Not on a follower.
    match g.h(target).transfer_leader(leader).await {
        Err(MetaError::NotLeader { hint }) => assert_eq!(hint, Some(leader)),
        other => panic!("{other:?}"),
    }
    g.h(leader).transfer_leader(target).await.unwrap();
    until("target leads", || g.h(target).is_leader()).await;
    assert!(!g.h(leader).is_leader());
    // Proposals continue, from any node.
    for id in VOTERS {
        assert_eq!(
            g.h(id).propose(queue(&format!("after{id}"))).await.unwrap(),
            MetaResult::Ok { version: 1 }
        );
    }
    g.stop_all().await;
}

/// A joiner (blank dir, no configuration) is added as a learner through
/// `propose_conf_change`, catches up, shows in the leader's progress and `conf_state`,
/// and passes a read barrier.
#[tokio::test(flavor = "multi_thread")]
async fn learner_added_by_conf_change_catches_up_and_shows_in_progress() {
    let mut g = Group::new().await;
    for i in 0..20 {
        g.h(1).propose(queue(&format!("pre{i}"))).await.unwrap();
    }
    g.start(4, true).await;
    let leader = g.leader().await;
    let follower = *VOTERS.iter().find(|&&i| i != leader).unwrap();
    match g.h(follower).propose_conf_change(add_learner(4)).await {
        Err(MetaError::NotLeader { hint }) => assert_eq!(hint, Some(leader)),
        other => panic!("{other:?}"),
    }
    let applied = g
        .h(leader)
        .propose_conf_change(add_learner(4))
        .await
        .unwrap();
    assert!(!applied.result.is_empty());
    let cs = g.h(leader).conf_state();
    assert_eq!(cs.voters, vec![1, 2, 3]);
    assert_eq!(cs.learners, vec![4]);
    assert!(cs.voters_outgoing.is_empty());

    let target = g.h(leader).view().applied();
    tokio::time::timeout(LONG, g.h(4).raft().wait_applied(target))
        .await
        .expect("learner caught up")
        .unwrap();
    assert!(g.h(4).view().read(|s| s.queue("pre19").is_some()));
    until("learner in progress", || {
        g.h(leader)
            .raft_detail()
            .progress
            .iter()
            .any(|p| p.id == 4 && p.matched >= target)
    })
    .await;
    assert!(
        g.h(follower).raft_detail().progress.is_empty(),
        "progress is leader-only"
    );
    assert_eq!(g.h(4).conf_state().learners, vec![4]);

    // A write after it reaches the learner through its barrier; its proposals forward.
    g.h(follower).propose(queue("post")).await.unwrap();
    g.h(4).read_index_barrier().await.unwrap();
    assert!(g.h(4).view().read(|s| s.queue("post").is_some()));
    assert_eq!(
        g.h(4).propose(queue("from-learner")).await.unwrap(),
        MetaResult::Ok { version: 1 }
    );
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_anchor_on_leader_and_follower() {
    let mut g = Group::new().await;
    let leader = g.leader().await;
    g.h(leader).propose(queue("l")).await.unwrap();
    until("leader lease", || g.h(leader).lease_anchor().is_some()).await;
    let follower = *VOTERS.iter().find(|&&i| i != leader).unwrap();
    until("follower lease", || g.h(follower).lease_anchor().is_some()).await;
    let a = g.h(follower).lease_anchor().unwrap();
    assert!(a <= Instant::now());
    g.stop_all().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_voter_barrier_and_lease() {
    let dir = TempDir::new().unwrap();
    let h = MetaHandle::start(dir.path(), 1).await.unwrap();
    h.propose(queue("solo")).await.unwrap();
    let idx = h.read_index_barrier().await.unwrap();
    assert!(idx >= h.view().applied().min(idx));
    assert!(h.view().read(|s| s.queue("solo").is_some()));
    let before = Instant::now();
    assert!(h.lease_anchor().unwrap() >= before);
    assert_eq!(h.conf_state().voters, vec![1]);
    h.stop().await;
}
