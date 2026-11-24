//! 12-05 Task 2: a static 3-voter metadata group (D-P12-2) over the driver's
//! `ChannelOutbox` and an in-test router, with proposal forwarding and read-your-writes.
//!
//! The router steps every outgoing message into the target's `RaftHandle`; the forwarder
//! runs a follower's proposal on the leader's `RaftHandle` (what `MetaPeer.Propose` does
//! over the network in ostro-cluster).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_meta::handle::{ForwardFuture, MetaStart, ProposeForwarder, TickConfig};
use ostro_meta::{
    MetaCommand, MetaHandle, MetaKey, MetaRecord, MetaResult, ProposeError, QueueConfig,
    RaftHandle, encode_snapshot,
};
use ostro_raft_driver::driver::ChannelOutbox;
use tempfile::TempDir;
use tokio::sync::mpsc;

const VOTERS: [u64; 3] = [1, 2, 3];

/// The `[replication]` meta defaults (100 ms tick, election 10, heartbeat 2): an election
/// timeout of 1-2 s.
fn ticks() -> TickConfig {
    TickConfig {
        interval: Duration::from_millis(100),
        election_tick: 10,
        heartbeat_tick: 2,
    }
}

#[derive(Clone, Default)]
struct Net(Arc<Mutex<HashMap<u64, RaftHandle>>>);

impl Net {
    fn get(&self, id: u64) -> Option<RaftHandle> {
        self.0.lock().unwrap().get(&id).cloned()
    }
}

/// Runs a proposal on the leader's raft handle; counts forwarded proposals.
struct LocalForwarder {
    net: Net,
    calls: Arc<AtomicUsize>,
}

impl ProposeForwarder for LocalForwarder {
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.net.get(leader) {
                Some(h) => h.propose(cmd).await,
                None => Err(ProposeError::NotLeader { leader: None }),
            }
        })
    }
}

struct Group {
    net: Net,
    dirs: HashMap<u64, TempDir>,
    nodes: HashMap<u64, MetaHandle>,
    calls: Arc<AtomicUsize>,
}

impl Group {
    async fn new() -> Group {
        let mut g = Group {
            net: Net::default(),
            dirs: VOTERS
                .iter()
                .map(|&i| (i, TempDir::new().unwrap()))
                .collect(),
            nodes: HashMap::new(),
            calls: Arc::new(AtomicUsize::new(0)),
        };
        for id in VOTERS {
            g.start(id).await;
        }
        g
    }

    async fn start(&mut self, id: u64) {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<Vec<ostro_raft_driver::raft::eraftpb::Message>>();
        let dir: &Path = self.dirs[&id].path();
        // No fsync: the 5 s failover bound below is a Raft timing claim (election timeout
        // 1-2 s plus a pre-vote and a vote round); on macOS CI a single multi-second
        // F_FULLFSYNC of a candidate's HardState or the new leader's first append left both
        // survivors at the same term with no leader past the bound (ci-fix7; reproduced
        // here with one injected 4.5 s `meta::fsync` sleep). Durability has its own suites.
        let h = MetaHandle::start_with_unsynced_log(
            dir,
            MetaStart {
                node_id: id,
                voters: VOTERS.to_vec(),
                outbox: Box::new(ChannelOutbox(tx)),
                forwarder: Some(Arc::new(LocalForwarder {
                    net: self.net.clone(),
                    calls: Arc::clone(&self.calls),
                })),
                tick: ticks(),
            },
        )
        .await
        .expect("start_with");
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

    async fn stop(&mut self, id: u64) {
        self.net.0.lock().unwrap().remove(&id);
        if let Some(h) = self.nodes.remove(&id) {
            h.stop().await;
        }
    }

    /// The single leader every running node agrees on.
    async fn leader(&self, within: Duration) -> u64 {
        let start = Instant::now();
        let deadline = start + within;
        // (elapsed, node, term, leader, is_leader) at every change, for a failure report
        let mut seen: HashMap<u64, (u64, Option<u64>, bool)> = HashMap::new();
        let mut trail: Vec<String> = Vec::new();
        loop {
            for (&i, h) in &self.nodes {
                let s = h.raft().status();
                let now = (s.term, s.leader_id, s.is_leader);
                if seen.insert(i, now) != Some(now) {
                    trail.push(format!(
                        "{:?}: node {i} term {} leader {:?} is_leader {}",
                        start.elapsed(),
                        s.term,
                        s.leader_id,
                        s.is_leader
                    ));
                }
            }
            let leaders: Vec<u64> = self
                .nodes
                .iter()
                .filter(|(_, h)| h.is_leader())
                .map(|(&i, _)| i)
                .collect();
            if let [l] = leaders[..]
                && self
                    .nodes
                    .values()
                    .all(|h| h.raft().status().leader_id == Some(l))
            {
                return l;
            }
            if Instant::now() >= deadline {
                // the raft view of every running node, for a failure on a slow runner
                let status: Vec<String> = self
                    .nodes
                    .iter()
                    .map(|(i, h)| {
                        let s = h.raft().status();
                        format!(
                            "node {i}: term {} leader {:?} is_leader {} commit {} applied {} failed {:?}",
                            s.term, s.leader_id, s.is_leader, s.commit, s.applied, s.failed
                        )
                    })
                    .collect();
                panic!("no agreed leader within {within:?}: {status:#?}\ntrail: {trail:#?}");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn follower(&self, leader: u64) -> u64 {
        *self.nodes.keys().find(|&&i| i != leader).unwrap()
    }

    fn snapshot(&self, id: u64) -> Vec<u8> {
        self.nodes[&id].view().read(encode_snapshot)
    }

    /// Wait until every node applied the highest index any node applied. The state
    /// machine view is updated before the driver publishes its status, so the target is
    /// taken from the views and awaited through the (lagging) status.
    async fn settle(&self) {
        let target = self
            .nodes
            .values()
            .map(|h| h.view().applied())
            .max()
            .unwrap();
        for h in self.nodes.values() {
            tokio::time::timeout(Duration::from_secs(30), h.raft().wait_applied(target))
                .await
                .expect("settle timeout")
                .expect("applied");
        }
    }
}

fn queue(name: &str, at: u64) -> MetaCommand {
    MetaCommand::Put {
        key: MetaKey::Queue(name.into()),
        expected_version: None,
        record: MetaRecord::Queue(QueueConfig {
            name: name.into(),
            max_concurrency: Some(4),
            created_at_ms: at,
            ext: Vec::new(),
        }),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_voters_elect_forward_fail_over_and_converge() {
    let mut g = Group::new().await;
    let t0 = Instant::now();
    let leader = g.leader(Duration::from_secs(30)).await;
    eprintln!("initial election: leader {leader} after {:?}", t0.elapsed());
    for h in g.nodes.values() {
        h.wait_ready().await.expect("ready");
    }

    // A follower's InitCluster is forwarded and visible locally afterwards.
    let f = g.follower(leader);
    let info = g.nodes[&f]
        .init_cluster("multi", 1, 4, 10, [5; 32])
        .await
        .expect("init via follower");
    assert_eq!(info.partitions, 4);
    assert!(
        g.calls.load(Ordering::SeqCst) >= 1,
        "proposal was forwarded"
    );
    let r = g.nodes[&f].propose(queue("q1", 11)).await.expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    assert!(
        g.nodes[&f].view().read(|s| s.queue("q1").is_some()),
        "read-your-writes on the follower"
    );
    // The leader applies locally (no forwarding).
    let before = g.calls.load(Ordering::SeqCst);
    let r = g.nodes[&leader]
        .propose(queue("q2", 12))
        .await
        .expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    assert_eq!(g.calls.load(Ordering::SeqCst), before);

    // Stop the leader: a new one is elected within 5 s and proposals continue.
    g.stop(leader).await;
    let t1 = Instant::now();
    let new_leader = g.leader(Duration::from_secs(5)).await;
    let failover = t1.elapsed();
    eprintln!("failover: leader {leader} -> {new_leader} after {failover:?}");
    assert_ne!(new_leader, leader);
    let other = g.follower(new_leader);
    let r = g.nodes[&other].propose(queue("q3", 13)).await.expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    assert!(g.nodes[&other].view().read(|s| s.queue("q3").is_some()));

    // Restart the old leader: it catches up to identical state.
    g.start(leader).await;
    g.nodes[&leader].wait_ready().await.expect("ready");
    let r = g.nodes[&new_leader]
        .propose(queue("q4", 14))
        .await
        .expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    g.settle().await;
    let snap = g.snapshot(new_leader);
    for id in VOTERS {
        assert_eq!(g.snapshot(id), snap, "node {id} state differs");
    }
    assert!(g.nodes[&leader].view().read(|s| s.queue("q4").is_some()));

    for id in VOTERS {
        g.stop(id).await;
    }
}

#[tokio::test]
async fn start_with_single_voter_matches_start() {
    let dir = TempDir::new().unwrap();
    let h = MetaHandle::start_with(
        dir.path(),
        MetaStart {
            node_id: 7,
            voters: vec![7],
            outbox: Box::new(ostro_meta::NullOutbox),
            forwarder: None,
            tick: TickConfig::default(),
        },
    )
    .await
    .expect("start_with");
    assert!(h.is_leader(), "a lone voter leads on return");
    let r = h.propose(queue("q", 1)).await.expect("put");
    assert_eq!(r, MetaResult::Ok { version: 1 });
    h.stop().await;
}

#[tokio::test]
async fn start_with_rejects_bad_voter_sets() {
    let dir = TempDir::new().unwrap();
    for (node_id, voters) in [(4, vec![1, 2, 3]), (1, vec![1, 1, 2]), (0, vec![0])] {
        let err = MetaHandle::start_with(
            dir.path(),
            MetaStart {
                node_id,
                voters: voters.clone(),
                outbox: Box::new(ostro_meta::NullOutbox),
                forwarder: None,
                tick: TickConfig::default(),
            },
        )
        .await
        .expect_err("rejected");
        assert!(
            matches!(err, ostro_meta::MetaError::Start(_)),
            "{voters:?}: {err}"
        );
    }
}
