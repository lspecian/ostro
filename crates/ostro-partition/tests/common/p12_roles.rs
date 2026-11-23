//! Phase 12 role-lifecycle test doubles (12-13): a fake meta [`AssignmentSource`] that
//! records term requests and resignations, and a fake [`FollowerDriver`] that records every
//! fetcher start/stop. Included with `#[path = "common/p12_roles.rs"] mod p12_roles;`.
#![allow(dead_code, clippy::disallowed_methods, clippy::disallowed_types)]

use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::future::BoxFuture;
use ostro_log::ReplicatedLog;
use ostro_partition::roles::{
    AssignmentSource, FollowerDriver, FollowerTask, PartitionAssignmentView, PartitionLogHandle,
};
use ostro_partition::{Node, NodeConfig, PartitionRole, StartupEvent};
use ostro_types::{Epoch, PartitionCount, PartitionId};
use tokio::sync::watch;
use tokio::time::timeout;

/// Generous bound for role transitions.
pub const ROLE_T: Duration = Duration::from_secs(20);
/// `Node::open` bound (fresh fjall stores are fsync-heavy under load).
pub const OPEN_T: Duration = Duration::from_secs(120);

/// One partition's assignment.
pub fn view(
    p: u16,
    leader: u64,
    epoch: u32,
    replicas: &[u64],
    isr: &[u64],
    isr_version: u64,
    min_insync: u8,
) -> PartitionAssignmentView {
    PartitionAssignmentView {
        partition: PartitionId(p),
        leader,
        epoch: Epoch(epoch),
        replicas: replicas.to_vec(),
        isr: isr.to_vec(),
        isr_version,
        min_insync,
    }
}

/// RF=1: this node leads `p` at `epoch` with replicas = ISR = `[node]`.
pub fn solo(p: u16, node: u64, epoch: u32) -> PartitionAssignmentView {
    view(p, node, epoch, &[node], &[node], 1, 1)
}

/// A fake meta assignment watch.
pub struct FakeAssignments {
    me: u64,
    tx: watch::Sender<Arc<Vec<PartitionAssignmentView>>>,
    pub term_requests: Mutex<Vec<(PartitionId, Epoch)>>,
    pub resignations: Mutex<Vec<(PartitionId, Epoch)>>,
}

impl FakeAssignments {
    pub fn new(me: u64, views: Vec<PartitionAssignmentView>) -> Arc<Self> {
        Arc::new(Self {
            me,
            tx: watch::channel(Arc::new(views)).0,
            term_requests: Mutex::new(Vec::new()),
            resignations: Mutex::new(Vec::new()),
        })
    }

    /// Replace (or add) the assignment of `v.partition`.
    pub fn set(&self, v: PartitionAssignmentView) {
        self.tx.send_modify(|all| {
            let mut n: Vec<_> = all
                .iter()
                .filter(|x| x.partition != v.partition)
                .cloned()
                .collect();
            n.push(v);
            n.sort_by_key(|x| x.partition.0);
            *all = Arc::new(n);
        });
    }

    pub fn term_requests(&self) -> Vec<(PartitionId, Epoch)> {
        self.term_requests.lock().unwrap().clone()
    }

    pub fn resignations(&self) -> Vec<(PartitionId, Epoch)> {
        self.resignations.lock().unwrap().clone()
    }
}

impl AssignmentSource for FakeAssignments {
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>> {
        self.tx.subscribe()
    }
    fn local_node(&self) -> u64 {
        self.me
    }
    fn request_term(&self, p: PartitionId, min_epoch: Epoch) -> BoxFuture<'_, ()> {
        self.term_requests.lock().unwrap().push((p, min_epoch));
        Box::pin(async {})
    }
    fn resign(&self, p: PartitionId, epoch: Epoch) -> BoxFuture<'_, ()> {
        self.resignations.lock().unwrap().push((p, epoch));
        Box::pin(async {})
    }
}

/// What the fake follower driver saw, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FetcherEvent {
    Start {
        p: u16,
        epoch: u32,
        leader: u64,
    },
    /// The fetcher stopped; `latest_epoch` is the log's latest epoch at that moment.
    Stop {
        p: u16,
        latest_epoch: Option<u32>,
    },
}

/// A follower driver whose fetchers do nothing but record their lifecycle.
#[derive(Default)]
pub struct FakeFollower {
    pub events: Arc<Mutex<Vec<FetcherEvent>>>,
}

impl FakeFollower {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
    pub fn events(&self) -> Vec<FetcherEvent> {
        self.events.lock().unwrap().clone()
    }
}

struct FakeTask {
    p: u16,
    log: Arc<PartitionLogHandle>,
    events: Arc<Mutex<Vec<FetcherEvent>>>,
}

impl FollowerTask for FakeTask {
    fn stop(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            self.events.lock().unwrap().push(FetcherEvent::Stop {
                p: self.p,
                latest_epoch: self.log.latest_epoch().map(|e| e.0),
            });
        })
    }
}

impl FollowerDriver for FakeFollower {
    fn start(
        &self,
        p: PartitionId,
        epoch: Epoch,
        leader: u64,
        log: Arc<PartitionLogHandle>,
    ) -> Box<dyn FollowerTask> {
        self.events.lock().unwrap().push(FetcherEvent::Start {
            p: p.0,
            epoch: epoch.0,
            leader,
        });
        Box::new(FakeTask {
            p: p.0,
            log,
            events: Arc::clone(&self.events),
        })
    }
}

/// A role-driven node config (`node_id`, `count` partitions) over the fakes.
pub fn roles_cfg(
    dir: &Path,
    node_id: u64,
    count: u16,
    asg: &Arc<FakeAssignments>,
    follower: &Arc<FakeFollower>,
) -> NodeConfig {
    let mut cfg = NodeConfig::new(
        dir.to_path_buf(),
        PartitionCount::new(count).expect("non-zero"),
    );
    cfg.node_id = node_id;
    cfg.assignments = Some(Arc::clone(asg) as Arc<dyn AssignmentSource>);
    cfg.follower_driver = Some(Arc::clone(follower) as Arc<dyn FollowerDriver>);
    cfg.roles.announce_backoff_min = Duration::from_millis(10);
    cfg.roles.announce_backoff_max = Duration::from_millis(50);
    cfg
}

pub async fn open(cfg: NodeConfig) -> Node {
    timeout(OPEN_T, Node::open(cfg))
        .await
        .expect("node open timed out")
        .expect("node open")
}

/// Wait until partition `p`'s role satisfies `f`; returns it.
pub async fn wait_role(
    node: &Node,
    p: u16,
    what: &str,
    f: impl Fn(&PartitionRole) -> bool,
) -> PartitionRole {
    let mut rx = node.role(PartitionId(p)).expect("partition exists");
    let last = node.role(PartitionId(p)).expect("partition exists");
    match timeout(ROLE_T, rx.wait_for(|r| f(r))).await {
        Ok(r) => *r.expect("role watch open"),
        Err(_) => panic!("timed out waiting for {what}; role {:?}", *last.borrow()),
    }
}

pub async fn wait_leader(node: &Node, p: u16, epoch: u32) {
    wait_role(node, p, &format!("Leader{{{epoch}}} on p{p}"), |r| {
        *r == PartitionRole::Leader {
            epoch: Epoch(epoch),
        }
    })
    .await;
}

/// Poll `f` every 5 ms until it holds.
pub async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + ROLE_T;
    while !f() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Partition `p`'s events, in order.
pub fn events_of(node: &Node, p: u16) -> Vec<StartupEvent> {
    node.startup_events()
        .into_iter()
        .filter(|e| e.partition() == Some(p))
        .collect()
}

pub fn index_of(ev: &[StartupEvent], f: impl Fn(&StartupEvent) -> bool) -> Option<usize> {
    ev.iter().position(f)
}
