//! Test support for the controller suite (12-10): a static metadata group of real
//! `MetaHandle`s routed in-process over the driver's `ChannelOutbox` (as 12-05's
//! `multi_voter` test), one `Controller` per node, a scripted liveness source, and a
//! collector of every proposal the controllers make.

#![allow(dead_code)]

use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ostro_cluster::controller::{Controller, ControllerConfig, ProposalRecord};
use ostro_cluster::liveness::{LiveFuture, LiveSource};
use ostro_meta::handle::{ForwardFuture, MetaStart, ProposeForwarder, TickConfig};
use ostro_meta::{
    MetaCommand, MetaHandle, MetaRecord, MetaResult, PartitionAssignment, ProposeError, RaftHandle,
};
use ostro_partition::SystemClock;
use ostro_raft_driver::driver::ChannelOutbox;
use tempfile::TempDir;
use tokio::sync::{Notify, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Fast metadata ticks for tests: 50 ms, election 10 (0.5-1 s), heartbeat 2.
pub fn ticks() -> TickConfig {
    TickConfig {
        interval: Duration::from_millis(50),
        election_tick: 10,
        heartbeat_tick: 2,
    }
}

/// A liveness source the test scripts: every node of the universe is live unless marked
/// dead. Counts controller takeovers.
#[derive(Default)]
pub struct FakeLiveness {
    dead: Mutex<BTreeSet<u64>>,
    pub became: AtomicUsize,
    wake: Notify,
}

impl FakeLiveness {
    pub fn kill(&self, node: u64) {
        self.dead.lock().unwrap().insert(node);
        self.wake.notify_waiters();
    }
    pub fn revive(&self, node: u64) {
        self.dead.lock().unwrap().remove(&node);
        self.wake.notify_waiters();
    }
}

impl LiveSource for FakeLiveness {
    fn on_became_controller(&self) {
        self.became.fetch_add(1, Ordering::SeqCst);
    }
    fn poll<'a>(
        &'a self,
        _raft: &'a RaftHandle,
        universe: &'a BTreeSet<u64>,
    ) -> LiveFuture<'a, BTreeSet<u64>> {
        let dead = self.dead.lock().unwrap().clone();
        Box::pin(async move { universe.difference(&dead).copied().collect() })
    }
    fn wake(&self) -> LiveFuture<'_, ()> {
        Box::pin(self.wake.notified())
    }
}

#[derive(Clone, Default)]
struct Net(Arc<Mutex<HashMap<u64, RaftHandle>>>);

impl Net {
    fn get(&self, id: u64) -> Option<RaftHandle> {
        self.0.lock().unwrap().get(&id).cloned()
    }
}

/// Nodes cut off from every other node (Raft messages and forwarded proposals dropped).
type Cut = Arc<Mutex<BTreeSet<u64>>>;

/// Commands forwarded to the leader, by sending node.
type Forwarded = Arc<Mutex<Vec<(u64, MetaCommand)>>>;

struct LocalForwarder {
    net: Net,
    from: u64,
    cut: Cut,
    forwarded: Forwarded,
}

impl ProposeForwarder for LocalForwarder {
    fn forward(&self, leader: u64, cmd: Vec<u8>) -> ForwardFuture<'_> {
        Box::pin(async move {
            {
                let cut = self.cut.lock().unwrap();
                if cut.contains(&self.from) || cut.contains(&leader) {
                    return Err(ProposeError::NotLeader { leader: None });
                }
            }
            if let Ok(c) = ostro_meta::decode_command(&cmd) {
                self.forwarded.lock().unwrap().push((self.from, c));
            }
            match self.net.get(leader) {
                Some(h) => h.propose(cmd).await,
                None => Err(ProposeError::NotLeader { leader: None }),
            }
        })
    }
}

struct Running {
    meta: Arc<MetaHandle>,
    controller: Option<(CancellationToken, JoinHandle<()>)>,
}

/// A static metadata group with one controller per node.
pub struct Group {
    pub voters: Vec<u64>,
    net: Net,
    dirs: HashMap<u64, TempDir>,
    nodes: HashMap<u64, Running>,
    pub records: Arc<Mutex<Vec<ProposalRecord>>>,
    cut: Cut,
    pub forwarded: Forwarded,
}

impl Group {
    /// Start `n` voters (ids 1..=n) and wait for a leader.
    pub async fn new(n: u64) -> Group {
        let voters: Vec<u64> = (1..=n).collect();
        let mut g = Group {
            net: Net::default(),
            dirs: voters
                .iter()
                .map(|&i| (i, TempDir::new().unwrap()))
                .collect(),
            nodes: HashMap::new(),
            records: Arc::new(Mutex::new(Vec::new())),
            cut: Cut::default(),
            forwarded: Forwarded::default(),
            voters,
        };
        for id in g.voters.clone() {
            g.start_meta(id).await;
        }
        g.leader().await;
        for r in g.nodes.values() {
            r.meta.wait_ready().await.expect("ready");
        }
        g
    }

    pub async fn start_meta(&mut self, id: u64) {
        let (tx, mut rx) =
            mpsc::unbounded_channel::<Vec<ostro_raft_driver::raft::eraftpb::Message>>();
        let dir: &Path = self.dirs[&id].path();
        let h = MetaHandle::start_with(
            dir,
            MetaStart {
                node_id: id,
                voters: self.voters.clone(),
                outbox: Box::new(ChannelOutbox(tx)),
                forwarder: Some(Arc::new(LocalForwarder {
                    net: self.net.clone(),
                    from: id,
                    cut: Arc::clone(&self.cut),
                    forwarded: Arc::clone(&self.forwarded),
                })),
                tick: ticks(),
            },
        )
        .await
        .expect("start_with");
        self.net.0.lock().unwrap().insert(id, h.raft().clone());
        let net = self.net.clone();
        let cut = Arc::clone(&self.cut);
        tokio::spawn(async move {
            while let Some(batch) = rx.recv().await {
                for m in batch {
                    let dropped = {
                        let cut = cut.lock().unwrap();
                        cut.contains(&m.from) || cut.contains(&m.to)
                    };
                    if dropped {
                        continue;
                    }
                    if let Some(to) = net.get(m.to) {
                        to.step(m);
                    }
                }
            }
        });
        self.nodes.insert(
            id,
            Running {
                meta: Arc::new(h),
                controller: None,
            },
        );
    }

    /// Start node `id`'s controller.
    pub fn start_controller(&mut self, id: u64, live: Arc<dyn LiveSource>, cfg: ControllerConfig) {
        let r = self.nodes.get_mut(&id).expect("running node");
        let c = Controller::new(Arc::clone(&r.meta), live, cfg, Arc::new(SystemClock));
        let mut events = c.subscribe();
        let records = Arc::clone(&self.records);
        tokio::spawn(async move {
            while let Ok(rec) = events.recv().await {
                records.lock().unwrap().push(rec);
            }
        });
        let token = CancellationToken::new();
        let join = tokio::spawn(c.run(token.clone()));
        r.controller = Some((token, join));
    }

    /// Start a controller on every running node, all sharing `live`.
    pub fn start_controllers(&mut self, live: Arc<dyn LiveSource>, cfg: ControllerConfig) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.start_controller(id, Arc::clone(&live), cfg);
        }
    }

    /// Stop node `id` (controller first, then meta).
    pub async fn stop(&mut self, id: u64) {
        self.net.0.lock().unwrap().remove(&id);
        let Some(mut r) = self.nodes.remove(&id) else {
            return;
        };
        if let Some((token, join)) = r.controller.take() {
            token.cancel();
            let _ = join.await;
        }
        let meta = Arc::try_unwrap(r.meta).expect("controller released the meta handle");
        meta.stop().await;
    }

    pub async fn stop_all(&mut self) {
        let ids: Vec<u64> = self.nodes.keys().copied().collect();
        for id in ids {
            self.stop(id).await;
        }
    }

    pub fn meta(&self, id: u64) -> &MetaHandle {
        &self.nodes[&id].meta
    }

    /// A shared handle to node `id`'s meta (drop it before [`Group::stop`]).
    pub fn meta_arc(&self, id: u64) -> Arc<MetaHandle> {
        Arc::clone(&self.nodes[&id].meta)
    }

    /// Cut node `id` off from every other node.
    pub fn cut(&self, id: u64) {
        self.cut.lock().unwrap().insert(id);
    }

    /// Restore node `id`'s links.
    pub fn heal(&self, id: u64) {
        self.cut.lock().unwrap().remove(&id);
    }

    pub fn running(&self) -> Vec<u64> {
        let mut v: Vec<u64> = self.nodes.keys().copied().collect();
        v.sort_unstable();
        v
    }

    /// The single leader every running node agrees on.
    pub async fn leader(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let leaders: Vec<u64> = self
                .nodes
                .iter()
                .filter(|(_, r)| r.meta.is_leader())
                .map(|(&i, _)| i)
                .collect();
            if let [l] = leaders[..]
                && self
                    .nodes
                    .values()
                    .all(|r| r.meta.raft().status().leader_id == Some(l))
            {
                return l;
            }
            assert!(Instant::now() < deadline, "no agreed leader");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Propose through the current leader.
    pub async fn propose(&self, cmd: MetaCommand) -> MetaResult {
        let l = self.leader().await;
        self.meta(l).propose(cmd).await.expect("propose")
    }

    /// `InitCluster` with `partitions` partitions (founding node = the current leader).
    pub async fn init(&self, partitions: u16) {
        let l = self.leader().await;
        self.meta(l)
            .init_cluster("c-12-10", l, partitions, 1, [7; 32])
            .await
            .expect("init_cluster");
    }

    /// Every assignment as the leader sees it.
    pub async fn assignments(&self) -> Vec<PartitionAssignment> {
        let l = self.leader().await;
        self.meta(l)
            .view()
            .read(|s| s.partitions().map(|v| v.value.clone()).collect())
    }

    pub async fn assignment(&self, p: u16) -> PartitionAssignment {
        self.assignments()
            .await
            .into_iter()
            .find(|a| a.partition == p)
            .expect("partition")
    }

    /// Wait until `f` holds on the leader's assignments.
    pub async fn wait_for(
        &self,
        what: &str,
        within: Duration,
        f: impl Fn(&[PartitionAssignment]) -> bool,
    ) -> Vec<PartitionAssignment> {
        let deadline = Instant::now() + within;
        loop {
            let a = self.assignments().await;
            if f(&a) {
                return a;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: {a:#?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Emulate followers catching up: the partition leader publishes `isr` (`AlterIsr`).
    pub async fn expand_isr(&self, p: u16, isr: Vec<u64>) {
        let a = self.assignment(p).await;
        let r = self
            .propose(MetaCommand::AlterIsr {
                partition: p,
                epoch: a.epoch,
                expected_isr_version: a.isr_version,
                isr,
            })
            .await;
        assert!(matches!(r, MetaResult::Ok { .. }), "{r:?}");
    }

    pub fn records(&self) -> Vec<ProposalRecord> {
        self.records.lock().unwrap().clone()
    }

    /// Applied (`Ok`) controller records for partition `p`.
    pub fn applied_for(&self, p: u16) -> Vec<ProposalRecord> {
        self.records()
            .into_iter()
            .filter(|r| matches!(r.result, Ok(MetaResult::Ok { .. })) && partition_of(&r.cmd) == p)
            .collect()
    }
}

pub fn partition_of(cmd: &MetaCommand) -> u16 {
    match cmd {
        MetaCommand::ElectLeader { partition, .. }
        | MetaCommand::AlterIsr { partition, .. }
        | MetaCommand::ResignLeader { partition, .. } => *partition,
        MetaCommand::Put {
            record: MetaRecord::Partition(p),
            ..
        } => p.partition,
        _ => u16::MAX,
    }
}

/// The election oracle over every proposed command (D-P12-7), applied or not: only
/// `ElectLeader` / `AlterIsr` / the epoch-0 bootstrap `Put`; an elected leader is always a
/// member of the ISR published when the command was planned, and an election only shrinks
/// the ISR.
pub fn audit(records: &[ProposalRecord]) {
    for r in records {
        match &r.cmd {
            MetaCommand::ElectLeader {
                leader, isr, epoch, ..
            } => {
                assert!(*epoch > r.before.epoch, "epoch must grow: {r:?}");
                assert!(!isr.is_empty(), "{r:?}");
                for m in isr {
                    assert!(
                        r.before.isr.contains(m),
                        "election added {m} to the ISR: {r:?}"
                    );
                }
                if *leader != 0 {
                    assert!(
                        r.before.isr.contains(leader),
                        "leader {leader} outside the published ISR: {r:?}"
                    );
                    assert!(isr.contains(leader), "{r:?}");
                }
            }
            MetaCommand::AlterIsr { isr, epoch, .. } => {
                assert_eq!(*epoch, r.before.epoch, "{r:?}");
                assert!(isr.contains(&r.before.leader), "{r:?}");
            }
            MetaCommand::Put {
                expected_version,
                record: MetaRecord::Partition(p),
                ..
            } => {
                assert_eq!(*expected_version, Some(0), "{r:?}");
                assert_eq!(
                    r.before.epoch, 0,
                    "re-placement of a placed partition: {r:?}"
                );
                assert_eq!(p.epoch, 1, "{r:?}");
            }
            other => panic!("controller proposed {other:?}"),
        }
    }
}
