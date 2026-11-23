//! Phase 12 role contracts (12-02): how a partition learns its role from meta and
//! hands follower work to the cluster layer.
//!
//! - [`PartitionLogHandle`]: the one concrete log type, named in exactly one alias line
//!   (12-08 renames the type by changing that line). Code outside `ostro-log` names the
//!   log only through this alias or the [`ReplicatedLog`](ostro_log::ReplicatedLog) trait.
//! - [`AssignmentSource`]: the meta-published assignment of every partition (12-10).
//! - [`FollowerDriver`] / [`FollowerTask`]: the fetcher loop for a followed partition (12-11).
//! - [`PartitionRoles`]: which partitions this node leads (bottomless uploader, 12-15).
//! - [`PartitionControl`] / [`InstallGuard`]: quiesce a partition for a snapshot install
//!   (12-12 / 12-13).
//!
//! The role lifecycle (12-13): [`PartitionRole`], the pure transition table [`decide`] and
//! one role driver per open partition (Leader at a meta-assigned epoch, Follower of a named
//! leader, or Offline), identical for RF=1 and RF=N. Snapshots and retention run in every
//! role ([`replica_services`](crate::replica_services)).
//!
//! Role transition table ([`decide`]; `latest` = the log's latest epoch):
//!
//! | assignment | current | decision |
//! |---|---|---|
//! | none, `epoch == 0`, `leader == 0`, or self not a replica | any | Offline |
//! | `leader == l != self` | any | Follow(e, l) (no-op when already) |
//! | `leader == self`, `e` | Leader/Announcing at `e` | keep leading, adopt ISR |
//! | `leader == self`, `e > latest` | anything else | Lead(e) (full step-down + step-up from an older term) |
//! | `leader == self`, `e <= latest` | anything else | stay, `request_term(latest + 1)` (B2) |
//! | partition failed | any | Offline, `resign(p, e)` once if it led at `e` (W3) |
//!
//! Become leader: stop the fetcher → `become_leader(e, replica set)` → wait applied ≥ local
//! HW → append `LeaderAnnounced{e}` (retried with backoff on `NotEnoughReplicas`, the
//! partition `Announcing` and unroutable meanwhile) → committed and applied → leader
//! services. Become follower: leader services stop → pending acks fail `NotLeader` with
//! `router.leader_hint(p)` = the new leader → `become_follower(e, leader)` → fetcher.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures_util::future::BoxFuture;
use ostro_blob::BlobStore;
use ostro_log::{LogError, LogNode, LogNodeConfig, ReadyBarrier, ReplicaSet, ReplicatedLog};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::apply::ApplyOutcome;
use crate::clock::Clock;
use crate::handle::PartitionError;
use crate::leader::snapshot::{self as snap, SnapshotConfig, SnapshotShared};
use crate::leader::{self, LeaderConfig, LeaderTasks};
use crate::leadership::LeadershipSource;
use crate::machine::{MachineConfig, PartitionHealth, wait_failed};
use crate::node::{
    CellState, PartCell, PartitionStart, RunningParts, ServicesParts, TermMode, build_running,
    term_offset, validate,
};
use crate::replica_services::{ReplicaTasks, spawn_replica_services};
use crate::router::{InProcessRouter, PartitionRouter};
use crate::startup::{StartupEvent, StartupLog};
use crate::store::{PartitionStore, StoreConfig, StoreMeta};

/// The concrete partition log handle `Node` holds. The only line that names the type.
pub type PartitionLogHandle = ostro_log::PartitionLog;

/// Test/fixture constructor next to the alias (W6): opens a one-node log for
/// `partition` (partition count `partition + 1`) under `wal_dir`, anchored at `floor`
/// when empty. Keep the returned [`LogNode`] alive while using the log and call
/// [`LogNode::shutdown`] at the end. Fixtures use this, never the type name. The log opens
/// unassigned (role `Offline`, HW = `floor`); call `become_leader` / `become_follower`.
#[doc(hidden)]
pub async fn open_partition_log_for_tests(
    wal_dir: &Path,
    partition: PartitionId,
    node_id: u64,
    floor: Offset,
) -> Result<(LogNode, Arc<PartitionLogHandle>), LogError> {
    let n = partition
        .0
        .checked_add(1)
        .and_then(PartitionCount::new)
        .ok_or_else(|| LogError::Failed(format!("partition {} out of range", partition.0)))?;
    let cfg = LogNodeConfig::new(wal_dir.to_path_buf(), n, node_id);
    let floors = vec![floor; usize::from(n.get())];
    let (node, mut logs) = LogNode::open(cfg, &floors).await?;
    let log = logs.swap_remove(usize::from(partition.0))?;
    Ok((node, log))
}

/// One partition's meta-published assignment, as the role lifecycle sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionAssignmentView {
    /// The partition.
    pub partition: PartitionId,
    /// The leader (0 = none / offline).
    pub leader: u64,
    /// Leader epoch (0 = unplaced / offline, D-P12-14).
    pub epoch: Epoch,
    /// Every assigned replica.
    pub replicas: Vec<u64>,
    /// The published ISR.
    pub isr: Vec<u64>,
    /// CAS token of `isr`.
    pub isr_version: u64,
    /// Minimum ISR size.
    pub min_insync: u8,
}

/// The meta-published assignments plus the two requests a partition makes of the
/// controller (12-10).
pub trait AssignmentSource: Send + Sync + 'static {
    /// Every partition's current assignment; changes on each meta apply that touches one.
    fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>>;

    /// This node's id.
    fn local_node(&self) -> u64;

    /// Ask for a fresh term for this node at an epoch `>= min_epoch` (12-10
    /// `request_fresh_term`; a no-op at epoch 0). Resolves once proposed.
    fn request_term(&self, p: PartitionId, min_epoch: Epoch) -> BoxFuture<'_, ()>;

    /// Step down as leader of `p` at `epoch` (`ResignLeader`; used when the log turns
    /// `LogHealth::Failed`, D-P12-9). Resolves once proposed.
    fn resign(&self, p: PartitionId, epoch: Epoch) -> BoxFuture<'_, ()>;
}

/// Starts the fetcher for a followed partition (12-11).
pub trait FollowerDriver: Send + Sync + 'static {
    /// Follow `leader` at `epoch`, appending into `log`.
    fn start(
        &self,
        p: PartitionId,
        epoch: Epoch,
        leader: u64,
        log: Arc<PartitionLogHandle>,
    ) -> Box<dyn FollowerTask>;
}

/// A running fetcher.
pub trait FollowerTask: Send {
    /// Stop fetching; resolves once the task no longer touches the log.
    fn stop(self: Box<Self>) -> BoxFuture<'static, ()>;
}

/// The partitions this node currently leads, with their epochs.
pub trait PartitionRoles: Send + Sync + 'static {
    /// `(partition, epoch)` of every led partition.
    fn led(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>>;
}

/// Partition lifecycle control for a snapshot install (D-P12-8).
pub trait PartitionControl: Send + Sync + 'static {
    /// Stop the partition's apply machine and services and close its state store, so the
    /// installer may reset the log and replace the state directory.
    fn quiesce_for_install(
        &self,
        p: PartitionId,
    ) -> BoxFuture<'_, Result<InstallGuard, PartitionError>>;
}

type ReopenFn = Box<dyn FnOnce() -> BoxFuture<'static, Result<(), PartitionError>> + Send>;

/// A quiesced partition: its store is closed, its log is idle. Dropping the guard
/// without [`reopen`](Self::reopen) leaves the partition stopped.
pub struct InstallGuard {
    /// The data dir root that holds `state/` (Phase 8 `install_snapshot`'s `data_dir`).
    pub state_root: PathBuf,
    /// The partition's log (for `reset_to`).
    pub log: Arc<PartitionLogHandle>,
    /// The store tuning to reopen with.
    pub store_cfg: StoreConfig,
    reopen: ReopenFn,
}

impl InstallGuard {
    /// Build a guard; `reopen` reopens the store and restarts the partition. It must
    /// also run the derived-keyspace rebuild hook the normal startup runs
    /// (`PartitionStore::start_run_index_rebuild`), since `x_*` keyspaces are outside the
    /// snapshot's digest scope.
    pub fn new(
        state_root: PathBuf,
        log: Arc<PartitionLogHandle>,
        store_cfg: StoreConfig,
        reopen: impl FnOnce() -> BoxFuture<'static, Result<(), PartitionError>> + Send + 'static,
    ) -> Self {
        Self {
            state_root,
            log,
            store_cfg,
            reopen: Box::new(reopen),
        }
    }

    /// Reopen the partition (after the install, or after an aborted one).
    pub fn reopen(self) -> BoxFuture<'static, Result<(), PartitionError>> {
        (self.reopen)()
    }
}

impl std::fmt::Debug for InstallGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstallGuard")
            .field("state_root", &self.state_root)
            .field(
                "partition",
                &ostro_log::ReplicatedLog::partition(&*self.log),
            )
            .finish_non_exhaustive()
    }
}

// ------------------------------------------------------------------------ role lifecycle
//
// 12-13 (research Pattern 5): one role driver per open partition. It is the only place that
// changes a partition's role, and it does so only from meta's assignment (`AssignmentSource`)
// and the partition's own health. The same code runs for RF=1 (replicas [self]) and RF=N.

/// A partition's role on this node (12-13).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionRole {
    /// Unplaced (epoch 0), not a replica, failed, waiting for a fresh term, or quiesced.
    Offline,
    /// Following `leader` at `epoch`: the fetcher appends, the apply machine applies up to
    /// the follower HW with actions off.
    Follower { epoch: Epoch, leader: u64 },
    /// Taking over at `epoch`: the log leads, `LeaderAnnounced{epoch}` is not applied yet.
    /// The partition is unavailable (not routable, no leader services).
    Announcing { epoch: Epoch },
    /// Leading at `epoch`: `LeaderAnnounced{epoch}` applied, leader services running.
    Leader { epoch: Epoch },
}

impl PartitionRole {
    /// The epoch this node leads or is taking over at.
    pub fn leading_epoch(&self) -> Option<Epoch> {
        match self {
            PartitionRole::Leader { epoch } | PartitionRole::Announcing { epoch } => Some(*epoch),
            _ => None,
        }
    }
}

/// What one assignment asks of this node for one partition ([`decide`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoleDecision {
    /// Unplaced (epoch 0 / leader 0), not a replica, or no assignment.
    Offline,
    /// Follow `leader` at `epoch`.
    Follow { epoch: Epoch, leader: u64 },
    /// Take over at `epoch` (`epoch > latest log epoch`).
    Lead { epoch: Epoch, set: ReplicaSet },
    /// Already leading (or taking over) at this epoch: adopt the published ISR only
    /// (AlterIsr / version updates; never a term request: our `LeaderAnnounced` is logged).
    KeepLeading { epoch: Epoch, set: ReplicaSet },
    /// Meta names this node leader at an epoch not above the log's latest (a restart, or meta
    /// behind the log, B2): stay Follower/Offline and ask for a term `>= min_epoch`.
    RequestTerm { min_epoch: Epoch },
}

/// The replica set an assignment publishes.
pub fn replica_set(view: &PartitionAssignmentView) -> ReplicaSet {
    ReplicaSet {
        replicas: view.replicas.clone(),
        isr: view.isr.clone(),
        isr_version: view.isr_version,
        min_insync: view.min_insync,
    }
}

/// The role transition table (pure). `current` is the partition's role, `latest` the log's
/// latest epoch. Epoch 0 is unplaced (B2); a node enters Leader only at a meta epoch above its
/// log's latest epoch; a node already leading at the assigned epoch stays leader (W1).
pub fn decide(
    current: PartitionRole,
    view: Option<&PartitionAssignmentView>,
    me: u64,
    latest: Option<Epoch>,
) -> RoleDecision {
    let Some(v) = view else {
        return RoleDecision::Offline;
    };
    if v.epoch.0 == 0 || v.leader == 0 || !v.replicas.contains(&me) {
        return RoleDecision::Offline;
    }
    if v.leader != me {
        return RoleDecision::Follow {
            epoch: v.epoch,
            leader: v.leader,
        };
    }
    if current.leading_epoch() == Some(v.epoch) {
        return RoleDecision::KeepLeading {
            epoch: v.epoch,
            set: replica_set(v),
        };
    }
    let latest = latest.map_or(0, |e| e.0);
    if v.epoch.0 > latest {
        RoleDecision::Lead {
            epoch: v.epoch,
            set: replica_set(v),
        }
    } else {
        RoleDecision::RequestTerm {
            min_epoch: Epoch(latest.saturating_add(1)),
        }
    }
}

/// Role driver tuning.
#[derive(Clone, Debug)]
pub struct RoleConfig {
    /// First retry delay of a `LeaderAnnounced` refused with `NotEnoughReplicas`.
    pub announce_backoff_min: Duration,
    /// Retry delay cap.
    pub announce_backoff_max: Duration,
    /// A fresh-term request still unanswered after this long is sent again.
    pub term_retry: Duration,
    /// How long a quiesce waits for every other user of the state store to let go.
    pub quiesce_timeout: Duration,
}

impl Default for RoleConfig {
    fn default() -> Self {
        Self {
            announce_backoff_min: Duration::from_millis(20),
            announce_backoff_max: Duration::from_secs(1),
            term_retry: Duration::from_secs(2),
            quiesce_timeout: Duration::from_secs(10),
        }
    }
}

/// Per-node test hooks (never process-global: each node has its own).
#[derive(Clone)]
pub struct NodeTestHooks {
    failed: Arc<watch::Sender<BTreeSet<u16>>>,
}

impl Default for NodeTestHooks {
    fn default() -> Self {
        Self {
            failed: Arc::new(watch::channel(BTreeSet::new()).0),
        }
    }
}

impl std::fmt::Debug for NodeTestHooks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeTestHooks")
            .field("failed", &*self.failed.borrow())
            .finish()
    }
}

impl NodeTestHooks {
    /// Mark partition `p` Failed on the node configured with these hooks (W3 tests): its role
    /// driver steps down to Offline and resigns once per led epoch.
    #[doc(hidden)]
    pub fn fail_partition(&self, p: PartitionId) {
        self.failed.send_modify(|s| {
            s.insert(p.0);
        });
    }
}

/// The partitions this node leads (after `LeaderAnnounced` applied), shared by the drivers.
pub(crate) struct LedSet {
    tx: watch::Sender<Arc<Vec<(PartitionId, Epoch)>>>,
}

impl Default for LedSet {
    fn default() -> Self {
        Self {
            tx: watch::channel(Arc::new(Vec::new())).0,
        }
    }
}

impl LedSet {
    pub(crate) fn subscribe(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>> {
        self.tx.subscribe()
    }

    pub(crate) fn insert(&self, p: PartitionId, e: Epoch) {
        self.tx.send_modify(|v| {
            let mut n: Vec<_> = v.iter().copied().filter(|(q, _)| *q != p).collect();
            n.push((p, e));
            n.sort_unstable_by_key(|(q, _)| q.0);
            *v = Arc::new(n);
        });
    }

    pub(crate) fn remove(&self, p: PartitionId) {
        self.tx.send_if_modified(|v| {
            if !v.iter().any(|(q, _)| *q == p) {
                return false;
            }
            *v = Arc::new(v.iter().copied().filter(|(q, _)| *q != p).collect());
            true
        });
    }
}

/// Everything the role drivers of one node share.
pub(crate) struct RoleEnv {
    pub(crate) node_id: u64,
    pub(crate) count: PartitionCount,
    pub(crate) data_dir: PathBuf,
    pub(crate) store_cfg: StoreConfig,
    pub(crate) machine_cfg: MachineConfig,
    pub(crate) snapshots_cfg: SnapshotConfig,
    pub(crate) snap_shared: Arc<SnapshotShared>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) leadership: Arc<dyn LeadershipSource>,
    pub(crate) blobs: Arc<BlobStore>,
    pub(crate) router: Arc<InProcessRouter>,
    pub(crate) services_router: Arc<dyn PartitionRouter>,
    pub(crate) leader_cfg: LeaderConfig,
    pub(crate) events: Arc<StartupLog>,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) led: Arc<LedSet>,
    pub(crate) source: Arc<dyn AssignmentSource>,
    pub(crate) follower: Option<Arc<dyn FollowerDriver>>,
    pub(crate) hooks: NodeTestHooks,
    pub(crate) cfg: RoleConfig,
}

/// What a quiesced partition hands the installer.
pub(crate) struct Quiesced {
    state_root: PathBuf,
    log: Arc<PartitionLogHandle>,
    store_cfg: StoreConfig,
}

/// Requests to a role driver.
pub(crate) enum DriverCmd {
    Quiesce(oneshot::Sender<Result<Quiesced, PartitionError>>),
    Reopen(oneshot::Sender<Result<(), PartitionError>>),
}

fn driver_gone() -> PartitionError {
    PartitionError::Failed("role driver stopped".into())
}

/// Quiesce through the driver behind `tx` and build the install guard (whose reopen goes
/// back through the same driver).
pub(crate) async fn quiesce_via(
    tx: mpsc::Sender<DriverCmd>,
) -> Result<InstallGuard, PartitionError> {
    let (rtx, rrx) = oneshot::channel();
    tx.send(DriverCmd::Quiesce(rtx))
        .await
        .map_err(|_| driver_gone())?;
    let q = rrx.await.map_err(|_| driver_gone())??;
    Ok(InstallGuard::new(
        q.state_root,
        q.log,
        q.store_cfg,
        move || {
            Box::pin(async move {
                let (rtx, rrx) = oneshot::channel();
                tx.send(DriverCmd::Reopen(rtx))
                    .await
                    .map_err(|_| driver_gone())?;
                rrx.await.map_err(|_| driver_gone())?
            })
        },
    ))
}

/// Spawn the role driver of the started partition in `cell`. Returns its command sender
/// (install control) and its task.
pub(crate) fn spawn_driver(
    env: Arc<RoleEnv>,
    cell: Arc<PartCell>,
    log: Arc<PartitionLogHandle>,
    barrier: ReadyBarrier,
) -> (mpsc::Sender<DriverCmd>, JoinHandle<()>) {
    let (tx, cmds) = mpsc::channel(4);
    let health = cell.running().map_or_else(
        || watch::channel(PartitionHealth::Starting).1,
        |r| r.handle.machine().health(),
    );
    let d = RoleDriver {
        p: cell.p,
        asg: env.source.subscribe(),
        fail: env.hooks.failed.subscribe(),
        shutdown: env.shutdown.clone(),
        env,
        cell,
        log,
        barrier,
        cmds,
        health,
        role: PartitionRole::Offline,
        leader: None,
        follower: None,
        replica: None,
        upload_fwd: None,
        failed: false,
        resigned: None,
        term: None,
        quiesced: false,
        asg_open: true,
        cmds_open: true,
    };
    (tx, tokio::spawn(d.run()))
}

/// An outstanding fresh-term request.
struct TermRequest {
    min_epoch: Epoch,
    /// The retry delay elapsed: the next reconcile sends it again.
    due: bool,
    timer: Pin<Box<tokio::time::Sleep>>,
}

enum Event {
    Shutdown,
    Assignment,
    Failed(String),
    Cmd(DriverCmd),
    Wake,
}

/// Outcome of waiting for a takeover step.
enum Waited<T> {
    Done(T),
    Shutdown,
    /// The assignment no longer asks this node to lead at the epoch.
    Changed,
    Failed(String),
}

/// Whether `reconcile` must decide again.
enum Flow {
    Done,
    Again,
}

struct RoleDriver {
    p: PartitionId,
    env: Arc<RoleEnv>,
    cell: Arc<PartCell>,
    log: Arc<PartitionLogHandle>,
    barrier: ReadyBarrier,
    cmds: mpsc::Receiver<DriverCmd>,
    asg: watch::Receiver<Arc<Vec<PartitionAssignmentView>>>,
    fail: watch::Receiver<BTreeSet<u16>>,
    shutdown: watch::Receiver<bool>,
    /// Health of the current apply machine.
    health: watch::Receiver<PartitionHealth>,
    role: PartitionRole,
    leader: Option<LeaderTasks>,
    follower: Option<Box<dyn FollowerTask>>,
    replica: Option<ReplicaTasks>,
    upload_fwd: Option<JoinHandle<()>>,
    failed: bool,
    /// The last epoch this node resigned (once per epoch, W3).
    resigned: Option<Epoch>,
    term: Option<TermRequest>,
    quiesced: bool,
    asg_open: bool,
    cmds_open: bool,
}

impl RoleDriver {
    fn me(&self) -> u64 {
        self.env.node_id
    }

    async fn run(mut self) {
        // The partition is open and caught up: the barrier opens and the replica services
        // start, whatever the role. The role starts Offline (a restart is never a leader
        // until meta says so at a fresh epoch).
        self.env.events.push(StartupEvent::RoleChanged {
            p: self.p.0,
            role: PartitionRole::Offline,
        });
        if let Some(run) = self.cell.running() {
            run.acks.refuse_not_leader();
        }
        self.start_replica();
        loop {
            if *self.shutdown.borrow() {
                break;
            }
            self.reconcile().await;
            match self.next_event().await {
                Event::Shutdown => break,
                Event::Assignment | Event::Wake => {}
                Event::Failed(reason) => self.on_failed(&reason).await,
                Event::Cmd(DriverCmd::Quiesce(reply)) => {
                    let r = self.quiesce().await;
                    let _ = reply.send(r);
                }
                Event::Cmd(DriverCmd::Reopen(reply)) => {
                    let r = self.reopen().await;
                    let _ = reply.send(r);
                }
            }
        }
        self.teardown().await;
    }

    async fn next_event(&mut self) -> Event {
        let p = self.p.0;
        let health_on = !self.quiesced && !self.failed;
        let term_on = self.term.as_ref().is_some_and(|t| !t.due);
        let RoleDriver {
            shutdown,
            asg,
            fail,
            health,
            cmds,
            term,
            asg_open,
            cmds_open,
            failed,
            ..
        } = self;
        tokio::select! {
            biased;
            _ = shutdown.wait_for(|v| *v) => Event::Shutdown,
            reason = wait_failed(health), if health_on => Event::Failed(reason),
            _ = fail.wait_for(|s| s.contains(&p)), if !*failed => {
                Event::Failed("partition marked failed (test hook)".into())
            }
            c = cmds.recv(), if *cmds_open => match c {
                Some(c) => Event::Cmd(c),
                None => {
                    *cmds_open = false;
                    Event::Wake
                }
            },
            r = asg.changed(), if *asg_open => {
                if r.is_err() {
                    *asg_open = false;
                }
                Event::Assignment
            }
            () = async {
                match term.as_mut() {
                    Some(t) => t.timer.as_mut().await,
                    None => std::future::pending().await,
                }
            }, if term_on => {
                if let Some(t) = term.as_mut() {
                    t.due = true;
                }
                Event::Wake
            }
        }
    }

    /// This partition's assignment (marks the current value seen).
    fn view(&mut self) -> Option<PartitionAssignmentView> {
        let p = self.p;
        self.asg
            .borrow_and_update()
            .iter()
            .find(|v| v.partition == p)
            .cloned()
    }

    fn decide_now(&mut self) -> (RoleDecision, Option<u64>) {
        let view = self.view();
        let me = self.me();
        let hint = view
            .as_ref()
            .filter(|v| v.epoch.0 > 0 && v.leader != 0 && v.leader != me)
            .map(|v| v.leader);
        let d = if self.failed {
            RoleDecision::Offline
        } else {
            decide(self.role, view.as_ref(), me, self.log.latest_epoch())
        };
        (d, hint)
    }

    /// Bring the role in line with the current assignment.
    async fn reconcile(&mut self) {
        if self.quiesced {
            // No role change while a snapshot install owns the partition; the reopen
            // reconciles.
            return;
        }
        loop {
            let (d, hint) = self.decide_now();
            if !matches!(d, RoleDecision::RequestTerm { .. }) {
                self.term = None;
            }
            let flow = match d {
                RoleDecision::Offline => {
                    self.go_offline(hint).await;
                    Flow::Done
                }
                RoleDecision::Follow { epoch, leader } => {
                    if self.role != (PartitionRole::Follower { epoch, leader }) {
                        self.go_follower(epoch, leader).await;
                    }
                    Flow::Done
                }
                RoleDecision::KeepLeading { epoch, set } => {
                    self.adopt_isr(epoch, set);
                    Flow::Done
                }
                RoleDecision::Lead { epoch, set } => self.go_leader(epoch, set).await,
                RoleDecision::RequestTerm { min_epoch } => {
                    if self.role.leading_epoch().is_some() {
                        self.step_down(hint).await;
                        self.set_role(PartitionRole::Offline);
                    }
                    self.request_term(min_epoch);
                    Flow::Done
                }
            };
            if let Flow::Done = flow {
                break;
            }
        }
    }

    fn set_role(&mut self, role: PartitionRole) {
        if self.role == role {
            return;
        }
        info!(partition = self.p.0, from = ?self.role, to = ?role, "partition role changed");
        self.role = role;
        self.cell.role.send_replace(role);
        self.env
            .events
            .push(StartupEvent::RoleChanged { p: self.p.0, role });
    }

    /// The ISR follows meta only (AlterIsr adoption while leading).
    fn adopt_isr(&self, epoch: Epoch, set: ReplicaSet) {
        if let Err(e) = self.log.set_published_isr(epoch, set) {
            warn!(partition = self.p.0, epoch = epoch.0, error = %e, "published ISR not adopted");
        }
    }

    fn request_term(&mut self, min_epoch: Epoch) {
        if let Some(t) = &self.term
            && t.min_epoch == min_epoch
            && !t.due
        {
            return;
        }
        self.term = Some(TermRequest {
            min_epoch,
            due: false,
            timer: Box::pin(tokio::time::sleep(self.env.cfg.term_retry)),
        });
        info!(
            partition = self.p.0,
            min_epoch = min_epoch.0,
            "meta names this node leader at an epoch not above the log's: requesting a fresh term"
        );
        let src = Arc::clone(&self.env.source);
        let p = self.p;
        tokio::spawn(async move { src.request_term(p, min_epoch).await });
    }

    async fn stop_follower(&mut self) {
        if let Some(f) = self.follower.take() {
            f.stop().await;
        }
    }

    /// Leave Leader/Announcing: the router stops routing here, pending and new submits fail
    /// `NotLeader` and the handle can no longer append (epoch 0) **before** the leader
    /// services stop: their shutdown may wait for executor jobs (up to the dispatcher's
    /// grace), and a leader that lost meta contact must answer clients at once instead of
    /// holding their requests until the services are gone (12-16,
    /// `isolated_leader_cannot_commit`).
    async fn step_down(&mut self, hint: Option<u64>) {
        let p = self.p;
        self.env.led.remove(p);
        self.env.router.unregister(p);
        self.env.router.set_leader_hint(p, hint);
        if let Some(run) = self.cell.running() {
            run.handle.machine().set_emit_actions(false);
            run.handle.set_epoch(Epoch(0));
            run.acks.refuse_not_leader();
        }
        self.stop_upload_fwd().await;
        #[cfg(feature = "failpoints")]
        if let Some(ms) = services_stop_delay() {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
        if let Some(t) = self.leader.take() {
            t.shutdown().await;
        }
    }

    async fn go_offline(&mut self, hint: Option<u64>) {
        if self.role.leading_epoch().is_some() {
            self.step_down(hint).await;
        }
        self.stop_follower().await;
        self.env.router.set_leader_hint(self.p, hint);
        self.set_role(PartitionRole::Offline);
    }

    async fn go_follower(&mut self, epoch: Epoch, leader: u64) {
        if self.role.leading_epoch().is_some() {
            self.step_down(Some(leader)).await;
        }
        self.stop_follower().await;
        self.env.router.set_leader_hint(self.p, Some(leader));
        if let Err(e) = self.log.become_follower(epoch, leader) {
            warn!(partition = self.p.0, epoch = epoch.0, leader, error = %e, "cannot follow");
            self.set_role(PartitionRole::Offline);
            return;
        }
        if let Some(d) = &self.env.follower {
            self.follower = Some(d.start(self.p, epoch, leader, Arc::clone(&self.log)));
        }
        self.set_role(PartitionRole::Follower { epoch, leader });
    }

    /// Take over at `epoch`: fetcher stopped → `become_leader` → applied ≥ local HW →
    /// `LeaderAnnounced{epoch}` appended (retried on `NotEnoughReplicas`), committed and
    /// applied → leader services.
    async fn go_leader(&mut self, epoch: Epoch, set: ReplicaSet) -> Flow {
        let p = self.p;
        if self.role.leading_epoch().is_some() {
            self.step_down(None).await;
        }
        self.stop_follower().await;
        let Some(run) = self.cell.running() else {
            return Flow::Done;
        };
        if let Err(e) = self.log.become_leader(epoch, set) {
            warn!(partition = p.0, epoch = epoch.0, error = %e, "cannot lead");
            self.set_role(PartitionRole::Offline);
            return Flow::Done;
        }
        self.env.router.set_leader_hint(p, None);
        self.set_role(PartitionRole::Announcing { epoch });

        // Everything up to the local HW is applied before the term starts.
        let hw = *self.log.high_watermark().borrow();
        let applied = *run.handle.machine().applied().borrow();
        if hw > applied {
            let m = run.handle.machine().clone();
            match self
                .wait_takeover(async move { m.wait_applied(Offset(hw.0 - 1)).await }, epoch)
                .await
            {
                Waited::Done(Ok(())) => {}
                Waited::Done(Err(e)) => {
                    let reason = format!("catch-up before the term: {e}");
                    self.on_failed(&reason).await;
                    return Flow::Again;
                }
                other => return self.interrupted(other).await,
            }
        }

        run.handle.set_epoch(epoch);
        run.acks.accept();
        let mut backoff = self.env.cfg.announce_backoff_min;
        loop {
            let la = Command::leader_announced(epoch.0, self.me(), self.env.clock.now_ms());
            let h = run.handle.clone();
            match self
                .wait_takeover(async move { h.submit_one(la).await }, epoch)
                .await
            {
                Waited::Done(Ok(ApplyOutcome::Accepted)) => break,
                Waited::Done(Err(PartitionError::Log(LogError::NotEnoughReplicas {
                    isr,
                    min,
                }))) => {
                    info!(
                        partition = p.0,
                        epoch = epoch.0,
                        isr,
                        min,
                        retry_ms = backoff.as_millis() as u64,
                        "LeaderAnnounced refused: not enough replicas; partition unavailable, retrying"
                    );
                    match self.wait_takeover(tokio::time::sleep(backoff), epoch).await {
                        Waited::Done(()) => {}
                        other => return self.interrupted(other).await,
                    }
                    backoff = (backoff * 2).min(self.env.cfg.announce_backoff_max);
                }
                Waited::Done(Ok(o)) => {
                    error!(partition = p.0, epoch = epoch.0, outcome = ?o, "LeaderAnnounced not accepted");
                    self.abandon_takeover().await;
                    return Flow::Done;
                }
                Waited::Done(Err(e)) => {
                    if run.handle.machine().check_healthy().is_err() {
                        self.on_failed(&format!("LeaderAnnounced: {e}")).await;
                        return Flow::Again;
                    }
                    warn!(partition = p.0, epoch = epoch.0, error = %e, "LeaderAnnounced failed");
                    self.abandon_takeover().await;
                    return Flow::Done;
                }
                other => return self.interrupted(other).await,
            }
        }

        // The term started: LeaderAnnounced{epoch} is committed and applied.
        let offset = term_offset(&self.log, epoch).map_or(0, |o| o.0);
        self.env.events.push(StartupEvent::TermStarted {
            p: p.0,
            epoch: epoch.0,
            offset,
        });
        run.handle.machine().set_emit_actions(true);
        self.env.router.register(run.handle.clone());
        self.start_upload_fwd();
        if let Some(ctx) = run.ctx.clone() {
            // A failover is semantically a restart: the dispatcher's rebuild re-grants every
            // running row under the new epoch.
            self.leader = Some(leader::spawn_all(ctx, &self.env.leader_cfg));
            self.env
                .events
                .push(StartupEvent::LeaderServicesStarted { p: p.0 });
        }
        self.env.led.insert(p, epoch);
        self.set_role(PartitionRole::Leader { epoch });
        Flow::Done
    }

    /// Wait for `fut` while still asked to lead at `epoch` (ISR updates of that epoch are
    /// adopted meanwhile).
    async fn wait_takeover<T>(
        &mut self,
        fut: impl std::future::Future<Output = T>,
        epoch: Epoch,
    ) -> Waited<T> {
        tokio::pin!(fut);
        let p = self.p.0;
        loop {
            let changed = {
                let RoleDriver {
                    shutdown,
                    asg,
                    fail,
                    health,
                    asg_open,
                    ..
                } = &mut *self;
                tokio::select! {
                    biased;
                    _ = shutdown.wait_for(|v| *v) => return Waited::Shutdown,
                    reason = wait_failed(health) => return Waited::Failed(reason),
                    _ = fail.wait_for(|s| s.contains(&p)) => {
                        return Waited::Failed("partition marked failed (test hook)".into());
                    }
                    r = asg.changed(), if *asg_open => {
                        if r.is_err() {
                            *asg_open = false;
                        }
                        true
                    }
                    v = &mut fut => return Waited::Done(v),
                }
            };
            if changed {
                match self.decide_now().0 {
                    RoleDecision::KeepLeading { epoch: e, set } if e == epoch => {
                        self.adopt_isr(e, set);
                    }
                    _ => return Waited::Changed,
                }
            }
        }
    }

    async fn interrupted<T>(&mut self, w: Waited<T>) -> Flow {
        match w {
            Waited::Done(_) | Waited::Shutdown => {
                self.abandon_takeover().await;
                Flow::Done
            }
            Waited::Changed => {
                self.abandon_takeover().await;
                Flow::Again
            }
            Waited::Failed(reason) => {
                self.on_failed(&reason).await;
                Flow::Again
            }
        }
    }

    async fn abandon_takeover(&mut self) {
        self.step_down(None).await;
        self.set_role(PartitionRole::Offline);
    }

    /// W3: a failed partition steps down and, if it led, resigns once per epoch.
    async fn on_failed(&mut self, reason: &str) {
        if self.failed {
            return;
        }
        self.failed = true;
        error!(partition = self.p.0, %reason, role = ?self.role, "partition failed; stepping down");
        if let Some(e) = self.role.leading_epoch() {
            self.step_down(None).await;
            if self.resigned != Some(e) {
                self.resigned = Some(e);
                let src = Arc::clone(&self.env.source);
                let p = self.p;
                tokio::spawn(async move { src.resign(p, e).await });
            }
        }
        self.stop_follower().await;
        self.set_role(PartitionRole::Offline);
    }

    /// Retention follows the uploader's floor only while leading.
    fn start_upload_fwd(&mut self) {
        if self.upload_fwd.is_some() {
            return;
        }
        let cell = Arc::clone(&self.cell);
        let mut src = cell.upload.subscribe();
        self.upload_fwd = Some(tokio::spawn(async move {
            loop {
                let v = *src.borrow_and_update();
                cell.effective_upload.send_replace(v);
                if src.changed().await.is_err() {
                    break;
                }
            }
        }));
    }

    async fn stop_upload_fwd(&mut self) {
        if let Some(t) = self.upload_fwd.take() {
            t.abort();
            let _ = t.await;
        }
        self.cell.effective_upload.send_replace(None);
    }

    /// Barrier + replica services of the running partition (every role).
    fn start_replica(&mut self) {
        if self.replica.is_some() {
            return;
        }
        let Some(ctx) = self.cell.running().and_then(|r| r.ctx) else {
            return;
        };
        if !self.barrier.is_open() {
            self.barrier.open();
            self.env
                .events
                .push(StartupEvent::BarrierOpened { p: self.p.0 });
        }
        self.replica = Some(spawn_replica_services(ctx, &self.env.leader_cfg.retention));
    }

    /// Stop the apply machine and services and close the state store (the fetcher keeps
    /// running: a snapshot install runs inside its round, 12-12).
    async fn quiesce(&mut self) -> Result<Quiesced, PartitionError> {
        let p = self.p;
        if self.quiesced {
            return Err(PartitionError::Failed(format!(
                "partition {} is already quiesced",
                p.0
            )));
        }
        if let Some(e) = self.role.leading_epoch() {
            return Err(PartitionError::Failed(format!(
                "partition {} leads at epoch {}: no snapshot install",
                p.0, e.0
            )));
        }
        if let Some(r) = self.replica.take() {
            r.shutdown().await;
        }
        let Some(run) = self.cell.take_running() else {
            return Err(PartitionError::Failed(format!(
                "partition {} is not running",
                p.0
            )));
        };
        let store = Arc::clone(run.handle.store());
        let machine = run.handle.machine().clone();
        drop(run);
        if let Err(e) = machine.shutdown().await {
            warn!(partition = p.0, error = %e, "apply machine shutdown before an install");
        }
        // Every other user of the store (cached handles) must let go: an open store must
        // never write into a directory the install swapped in.
        let deadline = tokio::time::sleep(self.env.cfg.quiesce_timeout);
        tokio::pin!(deadline);
        while Arc::strong_count(&store) > 1 {
            tokio::select! {
                () = &mut deadline => {
                    let users = Arc::strong_count(&store) - 1;
                    warn!(partition = p.0, users, "state store still in use; install refused, partition restarted");
                    let meta = store.read_meta();
                    let restarted = match meta {
                        Ok(meta) => self.start_partition(store, meta).await,
                        Err(e) => Err(format!("state store: {e}")),
                    };
                    if let Err(reason) = restarted {
                        self.fail_cell(reason).await;
                    }
                    return Err(PartitionError::Failed(format!(
                        "partition {}: state store still in use by {users} handle(s)",
                        p.0
                    )));
                }
                () = tokio::time::sleep(Duration::from_millis(5)) => {}
            }
        }
        drop(store);
        self.quiesced = true;
        info!(partition = p.0, "partition quiesced for a snapshot install");
        Ok(Quiesced {
            state_root: self.env.data_dir.clone(),
            log: Arc::clone(&self.log),
            store_cfg: self.env.store_cfg.clone(),
        })
    }

    /// Reopen after an install (or an aborted one): the normal open path (the log is
    /// already anchored and epoch-seeded by `reset_to`), the run-index rebuild, a fresh
    /// snapshot service, the replica services; the role is kept (a follower keeps its
    /// fetcher).
    async fn reopen(&mut self) -> Result<(), PartitionError> {
        let p = self.p;
        if !self.quiesced {
            return Err(PartitionError::Failed(format!(
                "partition {} is not quiesced",
                p.0
            )));
        }
        let (dir, count, sc) = (
            self.env.data_dir.clone(),
            self.env.count,
            self.env.store_cfg.clone(),
        );
        let opened =
            tokio::task::spawn_blocking(move || PartitionStore::open(&dir, p, count, &sc)).await;
        let r = match opened {
            Ok(Ok((store, meta))) => match validate(&meta, &self.log) {
                Ok(()) => self.start_partition(Arc::new(store), meta).await,
                Err(reason) => Err(reason),
            },
            Ok(Err(e)) => Err(format!("state store: {e}")),
            Err(e) => Err(format!("state store open task: {e}")),
        };
        self.quiesced = false;
        match r {
            Ok(()) => {
                info!(
                    partition = p.0,
                    "partition reopened after a snapshot install"
                );
                Ok(())
            }
            Err(reason) => {
                self.fail_cell(reason.clone()).await;
                Err(PartitionError::Failed(reason))
            }
        }
    }

    async fn fail_cell(&mut self, reason: String) {
        error!(partition = self.p.0, %reason, "partition failed");
        self.env.events.push(StartupEvent::PartitionFailed {
            p: self.p.0,
            reason: reason.clone(),
        });
        self.cell.set(CellState::Failed(reason.clone()));
        self.quiesced = false;
        self.on_failed(&reason).await;
    }

    /// Machine + handle over `store`, caught up, then the running parts and replica services.
    async fn start_partition(
        &mut self,
        store: Arc<PartitionStore>,
        meta: StoreMeta,
    ) -> Result<(), String> {
        let env = Arc::clone(&self.env);
        let start = PartitionStart {
            p: self.p,
            store,
            meta,
            rf1: Arc::clone(&self.log),
            blobs: Arc::clone(&env.blobs),
            machine: env.machine_cfg.clone(),
            term: TermMode::Roles,
            leadership: Arc::clone(&env.leadership),
            node_id: env.node_id,
            events: Arc::clone(&env.events),
        };
        let s = start.run().await?;
        s.acks.refuse_not_leader();
        let local = match snap::scan_dir(&snap::snapshot_dir(&env.data_dir, self.p)) {
            Ok(v) => v,
            Err(e) => {
                warn!(partition = self.p.0, error = %e, "local snapshot scan failed; none used");
                Vec::new()
            }
        };
        let running = build_running(
            RunningParts {
                count: env.count,
                data_dir: &env.data_dir,
                services: Some(ServicesParts {
                    snapshots: &env.snapshots_cfg,
                    snap_shared: &env.snap_shared,
                    clock: &env.clock,
                    router: &env.services_router,
                    barrier: &self.barrier,
                    shutdown: env.shutdown.clone(),
                    upload: self.cell.effective_upload.subscribe(),
                }),
            },
            s.handle,
            s.acks,
            local,
        );
        self.health = running.handle.machine().health();
        self.cell.set(CellState::Running(running));
        self.start_replica();
        Ok(())
    }

    async fn teardown(&mut self) {
        // Node shutdown: leader services stop; pending submits are not failed here (the
        // machine's graceful shutdown drains to the HW and resolves them).
        self.env.led.remove(self.p);
        self.env.router.unregister(self.p);
        if let Some(t) = self.leader.take() {
            t.shutdown().await;
        }
        self.stop_follower().await;
        if let Some(r) = self.replica.take() {
            r.shutdown().await;
        }
        self.stop_upload_fwd().await;
    }
}

/// Test hook: `roles::before_services_stop` = `return(<ms>)` holds a step-down for that long
/// before the leader services stop (a slow services shutdown, as an executor job in the
/// dispatcher's grace period makes it).
#[cfg(feature = "failpoints")]
fn services_stop_delay() -> Option<u64> {
    fail::fail_point!("roles::before_services_stop", |a| a
        .and_then(|s| s.parse().ok()));
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use ostro_log::ReplicatedLog;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[tokio::test]
    async fn fixture_log_opens_and_guard_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let (node, log) =
            open_partition_log_for_tests(&dir.path().join("wal"), PartitionId(2), 1, Offset(5))
                .await
                .unwrap();
        assert_eq!(log.partition(), PartitionId(2));
        assert_eq!(log.log_start(), Offset(5));
        // The fixture opens unassigned (role Offline); lead it the RF=1 way.
        log.become_leader(Epoch(1), ostro_log::ReplicaSet::solo(1))
            .unwrap();
        let r = log
            .append(Epoch(1), vec![Bytes::from_static(b"a")])
            .await
            .unwrap();
        assert_eq!(r.base, Offset(5));
        log.wait_committed(r.last).await.unwrap();

        let reopened = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&reopened);
        let guard = InstallGuard::new(
            dir.path().to_path_buf(),
            Arc::clone(&log),
            StoreConfig::default(),
            move || {
                Box::pin(async move {
                    flag.store(true, Ordering::SeqCst);
                    Ok(())
                })
            },
        );
        assert!(format!("{guard:?}").contains("InstallGuard"));
        guard.reopen().await.unwrap();
        assert!(reopened.load(Ordering::SeqCst));
        node.shutdown().await;
    }

    struct Fixed(watch::Sender<Arc<Vec<PartitionAssignmentView>>>);
    impl AssignmentSource for Fixed {
        fn subscribe(&self) -> watch::Receiver<Arc<Vec<PartitionAssignmentView>>> {
            self.0.subscribe()
        }
        fn local_node(&self) -> u64 {
            1
        }
        fn request_term(&self, _p: PartitionId, _min_epoch: Epoch) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
        fn resign(&self, _p: PartitionId, _epoch: Epoch) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    #[tokio::test]
    async fn assignment_source_is_object_safe() {
        let view = PartitionAssignmentView {
            partition: PartitionId(0),
            leader: 1,
            epoch: Epoch(1),
            replicas: vec![1],
            isr: vec![1],
            isr_version: 1,
            min_insync: 1,
        };
        let src: Arc<dyn AssignmentSource> =
            Arc::new(Fixed(watch::channel(Arc::new(vec![view.clone()])).0));
        assert_eq!(src.subscribe().borrow()[0], view);
        assert_eq!(src.local_node(), 1);
        src.request_term(PartitionId(0), Epoch(2)).await;
        src.resign(PartitionId(0), Epoch(1)).await;
        fn _object_safe(
            _: &dyn FollowerDriver,
            _: Box<dyn FollowerTask>,
            _: &dyn PartitionRoles,
            _: &dyn PartitionControl,
        ) {
        }
    }

    fn v(leader: u64, epoch: u32, replicas: &[u64]) -> PartitionAssignmentView {
        PartitionAssignmentView {
            partition: PartitionId(0),
            leader,
            epoch: Epoch(epoch),
            replicas: replicas.to_vec(),
            isr: replicas.to_vec(),
            isr_version: 1,
            min_insync: 1,
        }
    }

    #[test]
    fn transition_table() {
        use PartitionRole as R;
        use RoleDecision as D;
        let off = R::Offline;
        // Offline: no assignment, epoch 0 (even naming self), no leader, not a replica.
        assert_eq!(decide(off, None, 1, None), D::Offline);
        assert_eq!(decide(off, Some(&v(1, 0, &[1])), 1, None), D::Offline);
        assert_eq!(decide(off, Some(&v(0, 3, &[1])), 1, None), D::Offline);
        assert_eq!(decide(off, Some(&v(2, 3, &[2, 3])), 1, None), D::Offline);
        let leader = R::Leader { epoch: Epoch(3) };
        assert_eq!(
            decide(leader, Some(&v(1, 0, &[1])), 1, Some(Epoch(3))),
            D::Offline
        );
        // Follow a named leader.
        assert_eq!(
            decide(leader, Some(&v(2, 4, &[1, 2])), 1, Some(Epoch(3))),
            D::Follow {
                epoch: Epoch(4),
                leader: 2
            }
        );
        // Lead only above the log's latest epoch (B2).
        let set = replica_set(&v(1, 4, &[1, 2]));
        assert_eq!(
            decide(off, Some(&v(1, 4, &[1, 2])), 1, Some(Epoch(3))),
            D::Lead {
                epoch: Epoch(4),
                set: set.clone()
            }
        );
        assert_eq!(
            decide(off, Some(&v(1, 1, &[1])), 1, None),
            D::Lead {
                epoch: Epoch(1),
                set: replica_set(&v(1, 1, &[1]))
            }
        );
        assert_eq!(
            decide(off, Some(&v(1, 3, &[1])), 1, Some(Epoch(3))),
            D::RequestTerm {
                min_epoch: Epoch(4)
            }
        );
        let follower = R::Follower {
            epoch: Epoch(5),
            leader: 2,
        };
        assert_eq!(
            decide(follower, Some(&v(1, 2, &[1, 2])), 1, Some(Epoch(5))),
            D::RequestTerm {
                min_epoch: Epoch(6)
            }
        );
        // Already leading (or announcing) at the epoch: keep, adopt the ISR (W1).
        let mut alter = v(1, 3, &[1, 2]);
        alter.isr = vec![1];
        alter.isr_version = 7;
        assert_eq!(
            decide(leader, Some(&alter), 1, Some(Epoch(3))),
            D::KeepLeading {
                epoch: Epoch(3),
                set: replica_set(&alter)
            }
        );
        assert_eq!(
            decide(
                R::Announcing { epoch: Epoch(3) },
                Some(&alter),
                1,
                Some(Epoch(3))
            ),
            D::KeepLeading {
                epoch: Epoch(3),
                set: replica_set(&alter)
            }
        );
        // A newer term for the same leader: full step-down + step-up.
        assert_eq!(
            decide(leader, Some(&v(1, 5, &[1])), 1, Some(Epoch(3))),
            D::Lead {
                epoch: Epoch(5),
                set: replica_set(&v(1, 5, &[1]))
            }
        );
        assert_eq!(leader.leading_epoch(), Some(Epoch(3)));
        assert_eq!(follower.leading_epoch(), None);
    }

    #[test]
    fn led_set_and_hooks() {
        let led = LedSet::default();
        let rx = led.subscribe();
        led.insert(PartitionId(2), Epoch(1));
        led.insert(PartitionId(0), Epoch(4));
        led.insert(PartitionId(2), Epoch(3));
        assert_eq!(
            **rx.borrow(),
            vec![(PartitionId(0), Epoch(4)), (PartitionId(2), Epoch(3))]
        );
        led.remove(PartitionId(0));
        led.remove(PartitionId(9));
        assert_eq!(**rx.borrow(), vec![(PartitionId(2), Epoch(3))]);
        let hooks = NodeTestHooks::default();
        let other = NodeTestHooks::default();
        hooks.fail_partition(PartitionId(1));
        assert!(hooks.failed.borrow().contains(&1));
        assert!(other.failed.borrow().is_empty(), "hooks are per node");
        assert!(format!("{hooks:?}").contains("failed"));
    }
}
