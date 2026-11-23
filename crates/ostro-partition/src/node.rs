//! `Node`: opens the P partitions of one data directory in a fixed, tested order
//! (STOR-03, STOR-08, STOR-09, STOR-10; see [`startup`](crate::startup) for the sequence).
//!
//! Layout under `data_dir`:
//! - `node.toml`: `format_version = 1`, the fixed partition count (STOR-09) and the
//!   additive `incarnation` key (08-08, P8-D4: 128-bit random, 32 lowercase hex chars,
//!   written on first open, preserved afterwards; older readers ignore unknown keys);
//! - `state/pNNNN`: one fjall state store per partition (also records the count);
//! - `wal/`: the shared group-commit WAL of every partition log;
//! - `blobs/`: the content-addressed blob store shared by all partitions;
//! - `snapshots/pNNNN/`: local OSNP snapshots (08-09, [`snapshot`](crate::leader::snapshot)).
//!
//! Trimming (08-09, P8-D2): each Leader-mode partition runs a snapshot service whose local
//! floor, together with the bottomless upload floor ([`Node::floor_handles`]), bounds WAL
//! retention, so `log_start` never passes the oldest retained snapshot. A state that is
//! missing, unreadable or below `log_start` is rebuilt as newest usable snapshot + log tail.
//! Since 12-13 snapshots and retention are replica services
//! ([`replica_services`](crate::replica_services)): they run in every role.
//!
//! Roles (12-13, research Pattern 5): with [`NodeConfig::assignments`] set, every partition
//! has a role driven only by meta ([`roles`](crate::roles)): Leader at a meta-assigned epoch,
//! Follower of a named leader, or Offline. A partition opens unassigned (Offline) and never
//! picks its own cluster epoch. Without an assignment source the temporary legacy
//! self-leader path runs (Phase 12: legacy path, removed by 12-14).

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use futures_util::future::BoxFuture;
use ostro_blob::BlobStore;
use ostro_log::ostro_wal::GroupCommitConfig;
use ostro_log::{
    LogClock, LogError, LogNode, LogNodeConfig, PartitionLog, RawBatch, ReadyBarrier, ReplicaSet,
    ReplicatedLog,
};
use ostro_types::{Command, Epoch, Offset, PartitionCount, PartitionId};
use tokio::sync::{broadcast, mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::acks::PendingAcks;
use crate::apply::{ApplyOutcome, Cursor};
use crate::clock::{Clock, SystemClock}; // clock-exempt: default clock import
use crate::handle::{PartitionError, PartitionHandle};
use crate::leader::cron::{CronConfig, ScheduleSource};
use crate::leader::retention::{FloorWatches, RetentionConfig};
use crate::leader::snapshot::{
    self as snap, LocalSnapshot, SnapshotConfig, SnapshotService, SnapshotShared, SnapshotTaken,
};
use crate::leader::{self, LeaderConfig, LeaderCtx, LeaderTasks};
use crate::leadership::{LeadershipSource, LocalLeadership, next_term_epoch};
use crate::machine::{MachineConfig, MachineHandle, PartitionHealth};
use crate::replica_services::{ReplicaTasks, spawn_replica_services};
use crate::roles::{
    AssignmentSource, DriverCmd, FollowerDriver, InstallGuard, LedSet, NodeTestHooks,
    PartitionControl, PartitionRole, PartitionRoles, RoleConfig, RoleEnv,
};
use crate::router::{InProcessRouter, PartitionRouter};
use crate::startup::{StartupEvent, StartupLog};
use crate::store::meta::{K_APPLIED, K_STATE_HASH};
use crate::store::snapshot::{install_snapshot, recover_install};
use crate::store::{PartitionStore, StoreConfig, StoreMeta, Table};

/// `node.toml` format version.
const NODE_FILE_VERSION: u64 = 1;
const NODE_FILE: &str = "node.toml";

/// What the node does after recovery.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StartMode {
    /// Full startup: roles from meta (or the legacy self-leader term), readiness barrier,
    /// services.
    Leader,
    /// Recover and catch up only: no term, no barrier, no leader services, no appends
    /// (digest comparisons, offline inspection). The log leads locally at
    /// [`next_term_epoch`] with ISR {self} so its HW reaches the durable end and the whole
    /// tail is replayed; nothing is appended and no assignment source is consulted.
    Inspect,
}

/// Wraps the node's [`InProcessRouter`] before it is handed to the leader services (test
/// doubles: lossy/duplicating delivery, instrumentation).
pub type RouterWrapper =
    Arc<dyn Fn(Arc<InProcessRouter>) -> Arc<dyn PartitionRouter> + Send + Sync>;

/// Node configuration.
#[derive(Clone)]
pub struct NodeConfig {
    pub data_dir: PathBuf,
    pub partitions: PartitionCount,
    pub node_id: u64,
    pub store: StoreConfig,
    pub machine: MachineConfig,
    pub wal: GroupCommitConfig,
    pub leader: LeaderConfig,
    pub mode: StartMode,
    pub leadership: Arc<dyn LeadershipSource>,
    pub clock: Arc<dyn Clock>,
    /// Optional wrapper around the router used by the leader services (default: none, the
    /// services use the [`InProcessRouter`] directly).
    pub router_wrapper: Option<RouterWrapper>,
    /// Local snapshot service settings (Phase 8; consumed by 08-09).
    pub snapshots: SnapshotConfig,
    /// Cron driver settings (Phase 8; consumed by 08-14).
    pub cron: CronConfig,
    /// The schedule set for the cron driver (Phase 8, 08-14); `None` = no cron driver.
    pub schedule_source: Option<Arc<dyn ScheduleSource>>,
    /// Bottomless backup is on (B4): 08-09 then initialises the upload floor to
    /// `Some(log_start)` in `Node::open` before retention starts.
    pub backup_enabled: bool,
    /// Meta-published partition assignments (12-13). `Some` = every partition's role is
    /// driven by meta (RF=1 included: replicas `[self]`). `None` = the temporary legacy
    /// self-leader path (Phase 12: legacy path, removed by 12-14). Ignored in
    /// [`StartMode::Inspect`].
    pub assignments: Option<Arc<dyn AssignmentSource>>,
    /// Starts the fetcher of a followed partition (12-11 `FetcherDriver`); `None` = a
    /// follower does not fetch (tests, RF=1).
    pub follower_driver: Option<Arc<dyn FollowerDriver>>,
    /// Role driver tuning (12-13).
    pub roles: RoleConfig,
    /// Per-node test hooks (12-13: `fail_partition`); inert unless a test calls them.
    pub test_hooks: NodeTestHooks,
}

impl NodeConfig {
    /// Defaults: node id 1, default store/machine/WAL/leader configs, [`LocalLeadership`],
    /// [`SystemClock`], [`StartMode::Leader`], no assignment source. // clock-exempt: doc ref
    pub fn new(data_dir: PathBuf, partitions: PartitionCount) -> Self {
        Self {
            data_dir,
            partitions,
            node_id: 1,
            store: StoreConfig::default(),
            machine: MachineConfig::default(),
            wal: GroupCommitConfig::default(),
            leader: LeaderConfig::default(),
            mode: StartMode::Leader,
            leadership: Arc::new(LocalLeadership),
            clock: Arc::new(SystemClock), // clock-exempt: default production clock for the injected seam
            router_wrapper: None,
            snapshots: SnapshotConfig::default(),
            cron: CronConfig::default(),
            schedule_source: None,
            backup_enabled: false,
            assignments: None,
            follower_driver: None,
            roles: RoleConfig::default(),
            test_hooks: NodeTestHooks::default(),
        }
    }
}

/// One partition's state, as seen through [`Node::slots`].
#[derive(Clone, Debug)]
pub enum PartitionSlot {
    Ready(PartitionHandle<PartitionLog>),
    /// The partition failed to start (corrupt log, state ahead of log, ...), or is quiesced
    /// for a snapshot install. The other partitions are unaffected.
    Failed {
        partition: PartitionId,
        reason: String,
    },
}

/// Node-level open failure (a single partition's failure is a [`PartitionSlot::Failed`]).
#[derive(Debug, thiserror::Error)]
pub enum NodeError {
    #[error(
        "partition count mismatch: configured {configured}, data directory was created with {stored}"
    )]
    PartitionCountMismatch { configured: u16, stored: u16 },
    #[error("node file: {0}")]
    NodeFile(String),
    #[error("log: {0}")]
    Log(#[from] LogError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// The writable floor slots of one partition, for the bottomless uploader (08-13).
#[derive(Clone, Debug)]
pub struct FloorHandles {
    /// The upload floor; `None` = unbounded. With `NodeConfig::backup_enabled` it starts at
    /// `Some(log_start)` (set in `Node::open`, before any retention pass: B4), so nothing more
    /// is trimmed until the uploader publishes a real floor. Only ever raise it. With roles
    /// (12-13) retention honours it only while the partition leads.
    pub upload: watch::Sender<Option<Offset>>,
}

/// A started partition's live parts (replaced by a snapshot install's reopen).
#[derive(Clone)]
pub(crate) struct Running {
    pub(crate) handle: PartitionHandle<PartitionLog>,
    pub(crate) acks: Arc<PendingAcks>,
    pub(crate) snapshots: Option<Arc<SnapshotService>>,
    pub(crate) floors: FloorWatches,
    /// Leader mode only: the services' context (replica + leader services, forced passes).
    pub(crate) ctx: Option<LeaderCtx>,
}

pub(crate) enum CellState {
    Running(Running),
    Failed(String),
    /// Stopped for a snapshot install (store closed) until the guard reopens it.
    Quiesced,
}

/// One partition's slot in the node: its current parts plus the stable channels that outlive
/// a reopen.
pub(crate) struct PartCell {
    pub(crate) p: PartitionId,
    state: Mutex<CellState>,
    /// The uploader's floor (08-13 writes it through [`FloorHandles`]).
    pub(crate) upload: watch::Sender<Option<Offset>>,
    /// The upload floor retention sees (roles: follows `upload` only while leading).
    pub(crate) effective_upload: watch::Sender<Option<Offset>>,
    pub(crate) role: watch::Sender<PartitionRole>,
}

impl PartCell {
    fn new(p: PartitionId, state: CellState, upload_init: Option<Offset>) -> Self {
        Self {
            p,
            state: Mutex::new(state),
            upload: watch::channel(upload_init).0,
            effective_upload: watch::channel(None).0,
            role: watch::channel(PartitionRole::Offline).0,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, CellState> {
        self.state.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(crate) fn running(&self) -> Option<Running> {
        match &*self.lock() {
            CellState::Running(r) => Some(r.clone()),
            _ => None,
        }
    }

    /// Take the running parts out (the cell becomes `Quiesced`).
    pub(crate) fn take_running(&self) -> Option<Running> {
        let mut g = self.lock();
        match std::mem::replace(&mut *g, CellState::Quiesced) {
            CellState::Running(r) => Some(r),
            other => {
                *g = other;
                None
            }
        }
    }

    pub(crate) fn set(&self, s: CellState) {
        *self.lock() = s;
    }

    fn slot(&self) -> PartitionSlot {
        match &*self.lock() {
            CellState::Running(r) => PartitionSlot::Ready(r.handle.clone()),
            CellState::Failed(reason) => PartitionSlot::Failed {
                partition: self.p,
                reason: reason.clone(),
            },
            CellState::Quiesced => PartitionSlot::Failed {
                partition: self.p,
                reason: "quiesced for a snapshot install".into(),
            },
        }
    }

    fn is_failed(&self) -> bool {
        matches!(&*self.lock(), CellState::Failed(_))
    }
}

/// What a started partition needs to build its [`Running`] parts.
pub(crate) struct RunningParts<'a> {
    pub(crate) count: PartitionCount,
    pub(crate) data_dir: &'a Path,
    /// `None` = Inspect mode (no snapshot service, no services context).
    pub(crate) services: Option<ServicesParts<'a>>,
}

pub(crate) struct ServicesParts<'a> {
    pub(crate) snapshots: &'a SnapshotConfig,
    pub(crate) snap_shared: &'a Arc<SnapshotShared>,
    pub(crate) clock: &'a Arc<dyn Clock>,
    pub(crate) router: &'a Arc<dyn PartitionRouter>,
    pub(crate) barrier: &'a ReadyBarrier,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) upload: watch::Receiver<Option<Offset>>,
}

/// Snapshot service, floors and services context of a started partition.
pub(crate) fn build_running(
    parts: RunningParts<'_>,
    handle: PartitionHandle<PartitionLog>,
    acks: Arc<PendingAcks>,
    local: Vec<LocalSnapshot>,
) -> Running {
    let p = handle.partition();
    let Some(sv) = parts.services else {
        return Running {
            handle,
            acks,
            snapshots: None,
            floors: FloorWatches::fixed(Offset(0), None),
            ctx: None,
        };
    };
    let snapshots = SnapshotService::new(
        p,
        parts.count,
        parts.data_dir,
        Arc::clone(handle.store()),
        sv.snapshots.clone(),
        Arc::clone(sv.clock),
        Arc::clone(sv.snap_shared),
        local,
    );
    let floors = FloorWatches {
        local: snapshots.local_floor(),
        upload: sv.upload,
    };
    let ctx = LeaderCtx {
        partition: p,
        count: parts.count,
        handle: handle.clone(),
        router: Arc::clone(sv.router),
        barrier: sv.barrier.clone(),
        clock: Arc::clone(sv.clock),
        shutdown: sv.shutdown,
        floors: floors.clone(),
        snapshots: Some(Arc::clone(&snapshots)),
    };
    Running {
        handle,
        acks,
        snapshots: Some(snapshots),
        floors,
        ctx: Some(ctx),
    }
}

/// An open node: P partitions over one shared WAL.
pub struct Node {
    log: LogNode,
    cells: Vec<Arc<PartCell>>,
    barriers: Vec<ReadyBarrier>,
    router: Arc<InProcessRouter>,
    events: Arc<StartupLog>,
    /// Legacy path: leader and replica services of every partition.
    leaders: Vec<LeaderTasks>,
    replicas: Vec<ReplicaTasks>,
    /// Roles path: one role driver per started partition.
    drivers: Vec<JoinHandle<()>>,
    control: NodeControl,
    led: Arc<LedSet>,
    shutdown: watch::Sender<bool>,
    incarnation: [u8; 16],
    data_dir: PathBuf,
    retention_cfg: RetentionConfig,
    snap_shared: Arc<SnapshotShared>,
    installed: Vec<Option<Offset>>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("partitions", &self.log.partitions())
            .field("slots", &self.slots())
            .finish_non_exhaustive()
    }
}

/// Per-partition state between the startup steps.
type Stage = Result<(Arc<PartitionStore>, StoreMeta), String>;
/// A partition that recovered and validated: store, meta, log.
type Recovered = (Arc<PartitionStore>, StoreMeta, Arc<PartitionLog>);

impl Node {
    /// Open every partition in the order documented in [`startup`](crate::startup).
    pub async fn open(cfg: NodeConfig) -> Result<Node, NodeError> {
        let events = Arc::new(StartupLog::default());
        let count = cfg.partitions;
        std::fs::create_dir_all(&cfg.data_dir)?;

        // 0. the fixed partition count (and the node incarnation).
        let incarnation = check_node_file(&cfg.data_dir, count)?;
        events.push(StartupEvent::NodeFileChecked);

        let term = match (cfg.mode, &cfg.assignments) {
            (StartMode::Inspect, _) => TermMode::Inspect,
            (StartMode::Leader, None) => TermMode::Legacy,
            (StartMode::Leader, Some(_)) => TermMode::Roles,
        };

        // 0b. (08-09) finish or roll back an interrupted snapshot install, note which states
        //     are missing, and scan the local snapshots (removing interrupted `.tmp-*` files).
        let n = usize::from(count.get());
        let mut pre_fail: Vec<Option<String>> = vec![None; n];
        let mut missing = vec![false; n];
        let mut local: Vec<Vec<LocalSnapshot>> = vec![Vec::new(); n];
        for p in count.ids() {
            let i = p.index();
            if let Err(e) = recover_install(&cfg.data_dir, p) {
                pre_fail[i] = Some(format!("snapshot install recovery: {e}"));
            }
            missing[i] = !PartitionStore::exists(&cfg.data_dir, p);
            match snap::scan_dir(&snap::snapshot_dir(&cfg.data_dir, p)) {
                Ok(v) => local[i] = v,
                Err(e) => {
                    warn!(partition = p.0, error = %e, "local snapshot scan failed; none used");
                }
            }
        }

        // 1. state stores (the durable floors), in parallel.
        let mut opens = Vec::with_capacity(n);
        for p in count.ids() {
            let dir = cfg.data_dir.clone();
            let sc = cfg.store.clone();
            let failed = pre_fail[p.index()].take();
            opens.push(tokio::task::spawn_blocking(move || match failed {
                Some(reason) => Err(reason),
                None => PartitionStore::open(&dir, p, count, &sc).map_err(|e| format!("{e}")),
            }));
        }
        let mut stages: Vec<Stage> = Vec::with_capacity(opens.len());
        for (t, p) in opens.into_iter().zip(count.ids()) {
            let r = match t.await {
                Ok(Ok((store, meta))) => {
                    events.push(StartupEvent::StateOpened {
                        p: p.0,
                        applied: meta.applied.0,
                    });
                    Ok((Arc::new(store), meta))
                }
                Ok(Err(e)) => Err(format!("state store: {e}")),
                Err(e) => Err(format!("state store open task: {e}")),
            };
            stages.push(r);
        }
        let blobs = {
            let dir = cfg.data_dir.clone();
            let r = tokio::task::spawn_blocking(move || BlobStore::open(&dir))
                .await
                .map_err(|e| std::io::Error::other(format!("blob store open task: {e}")))?;
            Arc::new(r.map_err(|e| std::io::Error::other(format!("blob store: {e}")))?)
        };

        // 2. log recovery with those floors (torn tails cut before the WAL opens). A
        //    partition whose state could not be read gets floor = MAX: nothing counts as a
        //    torn tail, so its files are never cut on behalf of an unknown state. A log with
        //    no records is anchored at its floor (log_start = LEO = applied, Phase 12 F3);
        //    its epoch history is seeded from the cursor in step 4.
        let floors: Vec<Offset> = stages
            .iter()
            .map(|s| s.as_ref().map_or(Offset(u64::MAX), |(_, m)| m.applied))
            .collect();
        let mut lcfg = LogNodeConfig::new(cfg.data_dir.join("wal"), count, cfg.node_id);
        lcfg.wal = cfg.wal.clone();
        lcfg.clock = Arc::new(LogClockAdapter(Arc::clone(&cfg.clock)));
        let (log, logs) = LogNode::open(lcfg, &floors).await?;

        let mut ready: Vec<Option<Recovered>> = Vec::with_capacity(stages.len());
        let mut failed: Vec<Option<String>> = vec![None; stages.len()];
        let mut installed: Vec<Option<Offset>> = vec![None; stages.len()];
        for ((stage, lr), p) in stages.into_iter().zip(logs).zip(count.ids()) {
            let r = match (stage, lr) {
                (Err(reason), Err(_)) => Err(reason),
                (Ok(_), Err(e)) => Err(format!("log recovery: {e}")),
                (stage, Ok(rf1)) => {
                    let rep = rf1.recovery();
                    let log_event = StartupEvent::LogRecovered {
                        p: p.0,
                        leo: rep.leo.0,
                        torn: rep.torn.is_some(),
                    };
                    let readable = stage.is_ok();
                    if readable {
                        events.push(log_event.clone());
                    }
                    // 3a. (08-09) snapshot + tail when the state is missing, unreadable or
                    //     below log_start.
                    let settled =
                        settle_state(&cfg, p, stage, &rf1, missing[p.index()], &local[p.index()])
                            .await;
                    settled.and_then(|(store, meta, s)| {
                        if let Some(s) = s {
                            installed[p.index()] = Some(s);
                            if !readable {
                                events.push(log_event);
                            }
                        }
                        // 3b. validation.
                        validate(&meta, &rf1)?;
                        events.push(StartupEvent::Validated { p: p.0 });
                        Ok((store, meta, rf1))
                    })
                }
            };
            match r {
                Ok(v) => ready.push(Some(v)),
                Err(reason) => {
                    error!(partition = p.0, %reason, "partition failed to start");
                    events.push(StartupEvent::PartitionFailed {
                        p: p.0,
                        reason: reason.clone(),
                    });
                    failed[p.index()] = Some(reason);
                    ready.push(None);
                }
            }
        }

        // 4./5. catch-up (and, on the legacy path, the term start), partitions in parallel.
        let mut tasks = Vec::with_capacity(ready.len());
        for (r, p) in ready.into_iter().zip(count.ids()) {
            tasks.push(r.map(|(store, meta, rf1)| {
                let start = PartitionStart {
                    p,
                    store,
                    meta,
                    rf1,
                    blobs: Arc::clone(&blobs),
                    machine: cfg.machine.clone(),
                    term,
                    leadership: Arc::clone(&cfg.leadership),
                    clock: Arc::clone(&cfg.clock),
                    node_id: cfg.node_id,
                    events: Arc::clone(&events),
                };
                tokio::spawn(start.run())
            }));
        }
        let mut started: Vec<Result<Started, String>> = Vec::with_capacity(tasks.len());
        for (t, p) in tasks.into_iter().zip(count.ids()) {
            let r = match t {
                None => Err(failed[p.index()].take().unwrap_or_default()),
                Some(t) => match t.await {
                    Ok(Ok(s)) => Ok(s),
                    Ok(Err(reason)) => {
                        error!(partition = p.0, %reason, "partition failed to start");
                        events.push(StartupEvent::PartitionFailed {
                            p: p.0,
                            reason: reason.clone(),
                        });
                        Err(reason)
                    }
                    Err(e) => {
                        let reason = format!("startup task: {e}");
                        events.push(StartupEvent::PartitionFailed {
                            p: p.0,
                            reason: reason.clone(),
                        });
                        Err(reason)
                    }
                },
            };
            started.push(r);
        }

        let barriers: Vec<ReadyBarrier> = count.ids().map(|_| ReadyBarrier::new()).collect();
        let router = Arc::new(InProcessRouter::new(count));
        let (shutdown, _) = watch::channel(false);
        let services_router: Arc<dyn PartitionRouter> = match &cfg.router_wrapper {
            Some(wrap) => wrap(Arc::clone(&router)),
            None => Arc::clone(&router) as Arc<dyn PartitionRouter>,
        };
        // 08-14: the cron driver runs on every led partition when a schedule source is
        // configured (it drives only the schedules the partition owns).
        let mut leader_cfg = cfg.leader.clone();
        if leader_cfg.cron.is_none()
            && let Some(source) = &cfg.schedule_source
        {
            leader_cfg.cron = Some(leader::cron::CronService {
                config: cfg.cron.clone(),
                source: Arc::clone(source),
            });
        }
        let led: Arc<LedSet> = Arc::new(LedSet::default());

        // (08-09) floors and snapshot services, before any retention pass can run. The upload
        // floor starts at Some(log_start) with backup on (B4), None (unbounded) otherwise.
        let snap_shared = SnapshotShared::new();
        let mut cells: Vec<Arc<PartCell>> = Vec::with_capacity(started.len());
        let mut legacy_epochs: Vec<Option<Epoch>> = vec![None; started.len()];
        for (r, p) in started.into_iter().zip(count.ids()) {
            let cell = match r {
                Ok(s) => {
                    let upload_init = cfg.backup_enabled.then(|| s.handle.log().log_start());
                    let cell = PartCell::new(p, CellState::Quiesced, upload_init);
                    legacy_epochs[p.index()] = s.epoch;
                    let upload = match term {
                        TermMode::Roles => cell.effective_upload.subscribe(),
                        TermMode::Legacy | TermMode::Inspect => cell.upload.subscribe(),
                    };
                    let services = (term != TermMode::Inspect).then(|| ServicesParts {
                        snapshots: &cfg.snapshots,
                        snap_shared: &snap_shared,
                        clock: &cfg.clock,
                        router: &services_router,
                        barrier: &barriers[p.index()],
                        shutdown: shutdown.subscribe(),
                        upload,
                    });
                    let running = build_running(
                        RunningParts {
                            count,
                            data_dir: &cfg.data_dir,
                            services,
                        },
                        s.handle,
                        s.acks,
                        std::mem::take(&mut local[p.index()]),
                    );
                    cell.set(CellState::Running(running));
                    cell
                }
                Err(reason) => PartCell::new(p, CellState::Failed(reason), None),
            };
            cells.push(Arc::new(cell));
        }

        let mut leaders = Vec::new();
        let mut replicas = Vec::new();
        let mut drivers = Vec::new();
        let mut cmds: Vec<Option<mpsc::Sender<DriverCmd>>> = vec![None; cells.len()];
        match term {
            TermMode::Inspect => {}
            TermMode::Legacy => {
                // Phase 12: legacy path, removed by 12-14 (no assignment source: every
                // started partition leads at the term `PartitionStart` picked).
                // 6. barriers + routing, then services once every partition is routable.
                for cell in &cells {
                    if let Some(run) = cell.running() {
                        let p = cell.p;
                        barriers[p.index()].open();
                        events.push(StartupEvent::BarrierOpened { p: p.0 });
                        router.register(run.handle.clone());
                    }
                }
                for cell in &cells {
                    let Some(run) = cell.running() else {
                        continue;
                    };
                    let Some(ctx) = run.ctx.clone() else {
                        continue;
                    };
                    let p = cell.p;
                    replicas.push(spawn_replica_services(ctx.clone(), &cfg.leader.retention));
                    leaders.push(leader::spawn_all(ctx, &leader_cfg));
                    events.push(StartupEvent::LeaderServicesStarted { p: p.0 });
                    if let Some(e) = legacy_epochs[p.index()] {
                        cell.role.send_replace(PartitionRole::Leader { epoch: e });
                        led.insert(p, e);
                    }
                }
            }
            TermMode::Roles => {
                let Some(source) = cfg.assignments.clone() else {
                    unreachable!("TermMode::Roles has an assignment source");
                };
                let env = Arc::new(RoleEnv {
                    node_id: cfg.node_id,
                    count,
                    data_dir: cfg.data_dir.clone(),
                    store_cfg: cfg.store.clone(),
                    machine_cfg: cfg.machine.clone(),
                    snapshots_cfg: cfg.snapshots.clone(),
                    snap_shared: Arc::clone(&snap_shared),
                    clock: Arc::clone(&cfg.clock),
                    leadership: Arc::clone(&cfg.leadership),
                    blobs: Arc::clone(&blobs),
                    router: Arc::clone(&router),
                    services_router: Arc::clone(&services_router),
                    leader_cfg: leader_cfg.clone(),
                    events: Arc::clone(&events),
                    shutdown: shutdown.subscribe(),
                    led: Arc::clone(&led),
                    source,
                    follower: cfg.follower_driver.clone(),
                    hooks: cfg.test_hooks.clone(),
                    cfg: cfg.roles.clone(),
                });
                for cell in &cells {
                    let Some(run) = cell.running() else {
                        continue;
                    };
                    let p = cell.p;
                    let (tx, join) = crate::roles::spawn_driver(
                        Arc::clone(&env),
                        Arc::clone(cell),
                        Arc::clone(run.handle.log()),
                        barriers[p.index()].clone(),
                    );
                    cmds[p.index()] = Some(tx);
                    drivers.push(join);
                }
            }
        }

        Ok(Node {
            log,
            control: NodeControl {
                cmds: Arc::new(cmds),
            },
            cells,
            barriers,
            router,
            events,
            leaders,
            replicas,
            drivers,
            led,
            shutdown,
            incarnation,
            data_dir: cfg.data_dir.clone(),
            retention_cfg: cfg.leader.retention.clone(),
            snap_shared,
            installed,
        })
    }

    fn running(&self, p: PartitionId) -> Option<Running> {
        self.cells.get(p.index())?.running()
    }

    /// Partition `p`'s writable floor slots (the bottomless upload floor, 08-13). `None` for a
    /// partition that did not start.
    pub fn floor_handles(&self, p: PartitionId) -> Option<FloorHandles> {
        let c = self.cells.get(p.index())?;
        if c.is_failed() {
            return None;
        }
        Some(FloorHandles {
            upload: c.upload.clone(),
        })
    }

    /// `<data>/snapshots/pNNNN`.
    pub fn snapshot_dir(&self, p: PartitionId) -> PathBuf {
        snap::snapshot_dir(&self.data_dir, p)
    }

    /// Every snapshot written from now on, node-wide (after its floor was published).
    pub fn snapshot_events(&self) -> broadcast::Receiver<SnapshotTaken> {
        self.snap_shared.subscribe()
    }

    /// The local trim floor of `p` (`applied_offset` of its oldest retained snapshot; `0` =
    /// no trimming). `None` for a partition that is not running.
    pub fn local_floor(&self, p: PartitionId) -> Option<Offset> {
        let r = self.running(p)?;
        Some(*r.floors.local.borrow())
    }

    /// The snapshot `Node::open` installed for `p` (snapshot + tail rebuild), if any.
    pub fn snapshot_installed(&self, p: PartitionId) -> Option<Offset> {
        self.installed.get(p.index()).copied().flatten()
    }

    /// Partition `p`'s local snapshots, ascending (Leader mode; empty otherwise).
    pub async fn local_snapshots(&self, p: PartitionId) -> Vec<LocalSnapshot> {
        match self.running(p).and_then(|r| r.snapshots) {
            Some(s) => s.retained().await,
            None => Vec::new(),
        }
    }

    /// Take a snapshot of `p` now (Leader mode, any role), regardless of the cadence.
    /// `Ok(None)` when nothing was applied since the newest snapshot.
    pub async fn snapshot_now(&self, p: PartitionId) -> Result<Option<SnapshotTaken>, String> {
        let s = self
            .running(p)
            .and_then(|r| r.snapshots)
            .ok_or_else(|| format!("partition {} has no snapshot service", p.0))?;
        s.take().await.map_err(|e| e.to_string())
    }

    /// Run one retention pass of `p` now (Leader mode, any role). Returns the log start
    /// after it.
    pub async fn retention_pass(&self, p: PartitionId) -> Result<Offset, String> {
        let ctx = self
            .running(p)
            .and_then(|r| r.ctx)
            .ok_or_else(|| format!("partition {} runs no services", p.0))?;
        leader::retention::run_pass(&ctx, &self.retention_cfg)
            .await
            .map_err(|e| e.to_string())
    }

    /// This data directory's incarnation (`node.toml` `incarnation`, 08-08): generated on
    /// first open, stable across reopens; a restore (08-18) writes a new one. Used by the
    /// bottomless `LOCK` fence (P8-D4).
    pub fn incarnation(&self) -> [u8; 16] {
        self.incarnation
    }

    /// The consistent HW cut of every partition ([`LogNode::cut`]): causally closed, so a
    /// cut containing a record appended after observing another partition's HW `x` also
    /// contains `x`. A failed partition reports 0.
    pub fn log_cut(&self) -> Vec<Offset> {
        self.log.cut()
    }

    /// Partition `p`'s stored OSTR envelopes of whole batches in `[from, HW)`, up to about
    /// `max_bytes` ([`ReplicatedLog::read_envelopes`]). `LogError::Failed` for a partition
    /// that is not running.
    pub async fn read_envelopes(
        &self,
        p: PartitionId,
        from: Offset,
        max_bytes: usize,
    ) -> Result<Vec<RawBatch>, LogError> {
        let h = self
            .handle(p)
            .ok_or_else(|| LogError::Failed(format!("partition {} is not running", p.0)))?;
        h.log().read_envelopes(from, max_bytes).await
    }

    /// Partition `p`'s committed `(applied_offset, state_hash)`, read atomically: both meta
    /// rows come from ONE fjall snapshot, and every apply chunk commits them in one batch,
    /// so the pair always belongs to the same chunk (never a mix of two). `None` for a
    /// partition that is not running or whose meta rows cannot be read (logged).
    pub fn applied_state(&self, p: PartitionId) -> Option<(Offset, [u8; 32])> {
        let h = self.handle(p)?;
        // The meta keyspace holds a handful of fixed rows; one prefix scan = one snapshot.
        let rows = match h.reader().prefix(Table::Meta, b"", 64) {
            Ok(r) => r,
            Err(e) => {
                warn!(partition = p.0, error = %e, "applied_state: meta read failed");
                return None;
            }
        };
        let mut applied = None;
        let mut hash = None;
        for (k, v) in rows {
            if k == K_APPLIED {
                applied = <[u8; 8]>::try_from(v.as_slice())
                    .ok()
                    .map(|b| Offset(u64::from_be_bytes(b)));
            } else if k == K_STATE_HASH {
                hash = <[u8; 32]>::try_from(v.as_slice()).ok();
            }
        }
        match (applied, hash) {
            (Some(a), Some(h)) => Some((a, h)),
            _ => {
                warn!(
                    partition = p.0,
                    "applied_state: meta rows missing or malformed"
                );
                None
            }
        }
    }

    /// Partition `p`'s current handle, if it is running (a snapshot install's reopen
    /// replaces it: take a fresh one rather than caching it across installs).
    pub fn handle(&self, p: PartitionId) -> Option<PartitionHandle<PartitionLog>> {
        self.running(p).map(|r| r.handle)
    }

    /// Every partition's current slot.
    pub fn slots(&self) -> Vec<PartitionSlot> {
        self.cells.iter().map(|c| c.slot()).collect()
    }

    pub fn router(&self) -> Arc<InProcessRouter> {
        Arc::clone(&self.router)
    }

    /// Partition `p`'s readiness barrier (closed in Inspect mode and for failed slots).
    pub fn barrier(&self, p: PartitionId) -> Option<ReadyBarrier> {
        self.barriers.get(p.index()).cloned()
    }

    /// Partition `p`'s role (12-13). Legacy path: `Leader` at the picked term; Inspect:
    /// `Offline`.
    pub fn role(&self, p: PartitionId) -> Option<watch::Receiver<PartitionRole>> {
        Some(self.cells.get(p.index())?.role.subscribe())
    }

    /// The install control of this node's partitions (12-12's `InstallCtx` takes it). It
    /// does not keep the node alive.
    pub fn control(&self) -> Arc<dyn PartitionControl> {
        Arc::new(self.control.clone())
    }

    /// The partitions this node leads (12-15's uploader takes it).
    pub fn roles(&self) -> Arc<dyn PartitionRoles> {
        Arc::new(NodeRoles {
            led: Arc::clone(&self.led),
        })
    }

    /// Startup steps in order of occurrence.
    pub fn startup_events(&self) -> Vec<StartupEvent> {
        self.events.snapshot()
    }

    /// Stop role drivers and services, shut every machine down gracefully (final
    /// checkpoint), then flush and seal the WAL. Per-partition shutdown errors are logged (the
    /// WAL holds everything acknowledged; the next open replays from the state's
    /// `applied_offset`).
    pub async fn shutdown(self) -> Result<(), NodeError> {
        let Node {
            log,
            cells,
            router,
            leaders,
            replicas,
            drivers,
            shutdown,
            ..
        } = self;
        shutdown.send_replace(true);
        for d in drivers {
            let _ = d.await;
        }
        for t in leaders {
            t.shutdown().await;
        }
        for t in replicas {
            t.shutdown().await;
        }
        router.clear();
        for cell in cells {
            if let Some(run) = cell.take_running() {
                let p = cell.p;
                let machine = run.handle.machine().clone();
                drop(run);
                if let Err(e) = machine.shutdown().await {
                    warn!(partition = p.0, error = %e, "partition shutdown");
                }
            }
        }
        log.shutdown().await;
        Ok(())
    }
}

impl PartitionControl for Node {
    fn quiesce_for_install(
        &self,
        p: PartitionId,
    ) -> BoxFuture<'_, Result<InstallGuard, PartitionError>> {
        self.control.quiesce_for_install(p)
    }
}

impl PartitionRoles for Node {
    fn led(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>> {
        self.led.subscribe()
    }
}

/// [`PartitionControl`] over the role drivers (does not own the node).
#[derive(Clone)]
pub(crate) struct NodeControl {
    cmds: Arc<Vec<Option<mpsc::Sender<DriverCmd>>>>,
}

impl PartitionControl for NodeControl {
    fn quiesce_for_install(
        &self,
        p: PartitionId,
    ) -> BoxFuture<'_, Result<InstallGuard, PartitionError>> {
        let tx = self.cmds.get(p.index()).cloned().flatten();
        Box::pin(async move {
            let tx = tx.ok_or_else(|| {
                PartitionError::Failed(format!(
                    "partition {} has no role driver (not started, Inspect mode or the legacy path)",
                    p.0
                ))
            })?;
            crate::roles::quiesce_via(tx).await
        })
    }
}

/// [`PartitionRoles`] over the node's led set.
struct NodeRoles {
    led: Arc<LedSet>,
}

impl PartitionRoles for NodeRoles {
    fn led(&self) -> watch::Receiver<Arc<Vec<(PartitionId, Epoch)>>> {
        self.led.subscribe()
    }
}

/// Step 3: the state must lie inside the log.
///
/// A log with no records was anchored at the floor (`log_start = LEO = applied`, Phase 12
/// F3), so it always passes; "state ahead of log" is raised only for a non-empty log.
pub(crate) fn validate(meta: &StoreMeta, log: &PartitionLog) -> Result<(), String> {
    let leo = log.log_end();
    if meta.applied > leo {
        return Err(format!(
            "state ahead of log: rebuild the state store (applied {} > log end {})",
            meta.applied.0, leo.0
        ));
    }
    if meta.applied < log.log_start() {
        return Err(format!(
            "snapshot required: run 'ostro restore' (applied {} below log start {}, no usable local snapshot)",
            meta.applied.0,
            log.log_start().0
        ));
    }
    Ok(())
}

/// Step 3a (08-09): decide whether partition `p` needs a snapshot install and do it.
///
/// - unreadable state, or `applied < log_start`: install the newest usable local snapshot;
///   none usable → the original error / "snapshot required";
/// - missing state (the directory did not exist before open) with local snapshots: install
///   the newest usable one; none usable → keep the fresh store (full replay, `log_start = 0`
///   or validation fails);
/// - otherwise the state is used as is.
///
/// Usable = `log_start <= S <= LEO` and a successful staged, verified install; candidates are
/// tried newest first and a failure (logged) falls back to the next older one. Returns the
/// store, its meta and the installed snapshot's offset.
async fn settle_state(
    cfg: &NodeConfig,
    p: PartitionId,
    stage: Stage,
    rf1: &PartitionLog,
    missing: bool,
    local: &[LocalSnapshot],
) -> Result<(Arc<PartitionStore>, StoreMeta, Option<Offset>), String> {
    let start = rf1.log_start();
    let leo = rf1.log_end();
    let fallback: Result<(), String> = match &stage {
        Err(reason) => Err(reason.clone()),
        Ok((_, meta)) if meta.applied < start => Err(format!(
            "snapshot required: run 'ostro restore' (applied {} below log start {}, no usable local snapshot)",
            meta.applied.0, start.0
        )),
        Ok(_) if missing && !local.is_empty() => Ok(()),
        Ok(_) => {
            let (store, meta) = stage?;
            return Ok((store, meta, None));
        }
    };
    let candidates: Vec<LocalSnapshot> = local
        .iter()
        .rev()
        .filter(|s| s.applied_offset >= start && s.applied_offset <= leo)
        .cloned()
        .collect();
    if candidates.is_empty() {
        return match fallback {
            Ok(()) => {
                let (store, meta) = stage?;
                Ok((store, meta, None))
            }
            Err(e) => Err(e),
        };
    }
    // The store must be closed during the install.
    let had_state = stage.is_ok();
    drop(stage);
    let dir = cfg.data_dir.clone();
    let sc = cfg.store.clone();
    let count = cfg.partitions;
    let res = tokio::task::spawn_blocking(move || {
        let mut chosen = None;
        for s in candidates {
            let r = std::fs::File::open(&s.path)
                .map_err(crate::store::snapshot::SnapshotError::from)
                .and_then(|f| install_snapshot(&dir, p, count, &sc, std::io::BufReader::new(f)));
            match r {
                Ok(info) => {
                    info!(partition = p.0, snapshot = %s.path.display(),
                        applied = info.applied_offset, "local snapshot installed (snapshot + tail)");
                    chosen = Some(Offset(info.applied_offset));
                    break;
                }
                Err(e) => warn!(partition = p.0, snapshot = %s.path.display(), error = %e,
                    "local snapshot unusable; trying an older one"),
            }
        }
        let reopened = if chosen.is_some() || had_state {
            Some(PartitionStore::open(&dir, p, count, &sc))
        } else {
            None
        };
        (chosen, reopened)
    })
    .await
    .map_err(|e| format!("snapshot install task: {e}"))?;
    match res {
        (Some(s), Some(Ok((store, meta)))) => Ok((Arc::new(store), meta, Some(s))),
        (Some(_), Some(Err(e))) => Err(format!("state store after snapshot install: {e}")),
        (None, reopened) => match fallback {
            Err(e) => Err(e),
            Ok(()) => match reopened {
                Some(Ok((store, meta))) => Ok((Arc::new(store), meta, None)),
                Some(Err(e)) => Err(format!("state store: {e}")),
                None => Err("state store: not reopened".into()),
            },
        },
        (Some(_), None) => Err("state store: not reopened".into()),
    }
}

/// The node's injected [`Clock`] as the partition log's replica-tracker clock.
struct LogClockAdapter(Arc<dyn Clock>);

impl std::fmt::Debug for LogClockAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogClockAdapter").finish_non_exhaustive()
    }
}

impl LogClock for LogClockAdapter {
    fn now_ms(&self) -> u64 {
        self.0.now_ms()
    }
}

/// How a partition's log gets its role at open.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum TermMode {
    /// Phase 12: legacy path, removed by 12-14. No assignment source: lead at
    /// [`next_term_epoch`] with ISR {self} and append `LeaderAnnounced` (the Phase 4 term).
    Legacy,
    /// Offline tooling: lead locally at [`next_term_epoch`] (HW reaches the durable end, the
    /// whole tail is replayed), append nothing.
    Inspect,
    /// Meta-driven roles (12-13): the log stays unassigned (HW = applied, F4); the role
    /// driver leads or follows at meta's epoch afterwards.
    Roles,
}

/// A partition after steps 4/5.
pub(crate) struct Started {
    pub(crate) handle: PartitionHandle<PartitionLog>,
    pub(crate) acks: Arc<PendingAcks>,
    /// Legacy path: the epoch of the term started at open.
    pub(crate) epoch: Option<Epoch>,
}

/// Steps 4 and 5 of one partition (also the reopen after a snapshot install, `Roles`).
pub(crate) struct PartitionStart {
    pub(crate) p: PartitionId,
    pub(crate) store: Arc<PartitionStore>,
    pub(crate) meta: StoreMeta,
    pub(crate) rf1: Arc<PartitionLog>,
    pub(crate) blobs: Arc<BlobStore>,
    pub(crate) machine: MachineConfig,
    pub(crate) term: TermMode,
    pub(crate) leadership: Arc<dyn LeadershipSource>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) node_id: u64,
    pub(crate) events: Arc<StartupLog>,
}

impl PartitionStart {
    pub(crate) async fn run(self) -> Result<Started, String> {
        self.seed_anchored_epochs().await?;
        // Inspect and the legacy path pick a local term before catch-up (12-08): the log
        // opened with HW = applied (F4), and leading with ISR {self} is what raises the HW to
        // the durable LEO, so the replay below covers [applied, LEO). With roles the log stays
        // unassigned: the role driver leads (and waits for applied >= HW) or follows.
        let epoch = match self.term {
            TermMode::Roles => None,
            TermMode::Legacy | TermMode::Inspect => {
                let epoch = next_term_epoch(
                    self.leadership.epoch_floor(self.p),
                    self.rf1.epochs().latest(),
                );
                self.rf1
                    .become_leader(epoch, ReplicaSet::solo(self.node_id))
                    .map_err(|e| format!("lead at epoch {}: {e}", epoch.0))?;
                Some(epoch)
            }
        };
        let acks = Arc::new(PendingAcks::new());
        let machine = MachineHandle::spawn(
            Arc::clone(&self.store),
            Arc::clone(&self.rf1),
            Cursor::from_meta(&self.meta),
            self.machine.clone(),
            tokio::runtime::Handle::current(),
            Arc::clone(&acks),
        );
        // Replayed commands' actions were acted upon (or are recomputed) by the leader
        // services; never re-emit them. A follower never emits.
        machine.set_emit_actions(false);
        let handle = PartitionHandle::new(
            self.p,
            Arc::clone(&self.rf1),
            Arc::clone(&self.store),
            machine.clone(),
            Arc::clone(&acks),
            Arc::clone(&self.blobs),
        );
        match self.catch_up_and_announce(&handle, epoch).await {
            Ok(()) => Ok(Started {
                handle,
                acks,
                epoch: epoch.filter(|_| self.term == TermMode::Legacy),
            }),
            Err(reason) => {
                drop(handle);
                let _ = machine.shutdown().await;
                Err(reason)
            }
        }
    }

    /// Empty-log anchoring (Phase 12 F3, plan 05-21): a log with no records recovered at
    /// `log_start = LEO = applied_offset` and carries no epoch history, so seed it from the
    /// state's applied `LeaderAnnounced` history. The next term then picks an epoch above
    /// the cursor's (an unseeded log would pick epoch 1 and its `LeaderAnnounced` would be
    /// rejected as stale), and Phase 12 divergence queries see the history below the start.
    /// A log `reset_to` by a snapshot install is already seeded (no-op).
    async fn seed_anchored_epochs(&self) -> Result<(), String> {
        let empty = self.rf1.log_end() == self.rf1.log_start() && self.rf1.epochs().0.is_empty();
        if !empty || self.meta.cursor.epoch_history.is_empty() {
            return Ok(());
        }
        let history: Vec<(Epoch, Offset)> = self
            .meta
            .cursor
            .epoch_history
            .iter()
            .map(|&(e, o)| (Epoch(e), Offset(o)))
            .collect();
        self.rf1
            .seed_epochs(&history)
            .await
            .map_err(|e| format!("seed epoch history from the cursor: {e}"))?;
        info!(
            partition = self.p.0,
            anchor = %self.rf1.log_start(),
            epochs = history.len(),
            "empty log anchored at applied offset; epoch history seeded from the cursor"
        );
        Ok(())
    }

    async fn catch_up_and_announce(
        &self,
        handle: &PartitionHandle<PartitionLog>,
        epoch: Option<Epoch>,
    ) -> Result<(), String> {
        let p = self.p.0;
        let machine = handle.machine();

        // 4. replay [applied, HW).
        let hw = *self.rf1.high_watermark().borrow();
        if hw > self.meta.applied {
            machine
                .wait_applied(Offset(hw.0 - 1))
                .await
                .map_err(|e| format!("catch-up: {e}"))?;
        }
        let mut health = machine.health();
        let h = health
            .wait_for(|h| *h != PartitionHealth::Starting)
            .await
            .map(|h| h.clone())
            .map_err(|_| "catch-up: apply machine stopped".to_string())?;
        if let PartitionHealth::Failed { reason } = h {
            return Err(format!("catch-up: {reason}"));
        }
        self.events.push(StartupEvent::CaughtUp {
            p,
            applied: machine.applied().borrow().0,
        });
        if self.term == TermMode::Inspect {
            return Ok(());
        }
        // 11-05: a stale or missing x_run_index rebuilds in the background (never in Inspect;
        // also after a snapshot install's reopen, 12-12).
        self.store.start_run_index_rebuild();
        let Some(epoch) = epoch else {
            return Ok(());
        };

        // 5. (legacy path) a new term: LeaderAnnounced{epoch} is its first record (the log
        //    already leads at `epoch`, see `run`).
        handle.set_epoch(epoch);
        let la = Command::leader_announced(epoch.0, self.node_id, self.clock.now_ms());
        match handle.submit_one(la).await {
            Ok(ApplyOutcome::Accepted) => {}
            Ok(o) => return Err(format!("LeaderAnnounced{{epoch {}}}: {o:?}", epoch.0)),
            Err(e) => return Err(format!("LeaderAnnounced{{epoch {}}}: {e}", epoch.0)),
        }
        let offset = term_offset(&self.rf1, epoch)
            .ok_or_else(|| format!("epoch {} missing from the log history", epoch.0))?;
        self.events.push(StartupEvent::TermStarted {
            p,
            epoch: epoch.0,
            offset: offset.0,
        });
        machine.set_emit_actions(true);
        Ok(())
    }
}

/// The start offset of `epoch` when it is the log's latest epoch.
pub(crate) fn term_offset(log: &PartitionLog, epoch: Epoch) -> Option<Offset> {
    log.epochs()
        .0
        .last()
        .filter(|(e, _)| *e == epoch)
        .map(|(_, o)| *o)
}

/// Create `node.toml` or check its partition count against `count`; returns the node
/// incarnation. A file without `incarnation` (written before 08-08) gains one through an
/// append-only atomic rewrite (tmp + fsync + rename + dir fsync); the existing lines are
/// kept byte for byte.
fn check_node_file(dir: &Path, count: PartitionCount) -> Result<[u8; 16], NodeError> {
    let path = dir.join(NODE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(s) => {
            let f = parse_node_file(&s)?;
            if f.version != NODE_FILE_VERSION {
                return Err(NodeError::NodeFile(format!(
                    "{}: unsupported format_version {}",
                    path.display(),
                    f.version
                )));
            }
            if f.partitions != count.get() {
                return Err(NodeError::PartitionCountMismatch {
                    configured: count.get(),
                    stored: f.partitions,
                });
            }
            if let Some(inc) = f.incarnation {
                return Ok(inc);
            }
            let inc = new_incarnation();
            let mut body = s;
            if !body.is_empty() && !body.ends_with('\n') {
                body.push('\n');
            }
            body.push_str(&incarnation_line(&inc));
            write_durably(dir, NODE_FILE, body.as_bytes())?;
            info!(incarnation = %hex16(&inc), "node.toml: incarnation added");
            Ok(inc)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let inc = new_incarnation();
            let body = format!(
                "format_version = {NODE_FILE_VERSION}\npartitions = {}\n{}",
                count.get(),
                incarnation_line(&inc)
            );
            write_durably(dir, NODE_FILE, body.as_bytes())?;
            Ok(inc)
        }
        Err(e) => Err(e.into()),
    }
}

/// The `incarnation = "<32 hex>"` line of `node.toml`.
fn incarnation_line(inc: &[u8; 16]) -> String {
    format!("incarnation = \"{}\"\n", hex16(inc))
}

/// Lowercase hex of an incarnation (32 chars).
pub fn hex16(b: &[u8; 16]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(32);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

/// A fresh 128-bit random node incarnation (08-08; restore, 08-18, calls it too).
///
/// No new dependency: the bytes are BLAKE3 over 16 bytes of `/dev/urandom` (when readable),
/// two hashes keyed by std's OS-seeded `RandomState` (128 bits of OS randomness per process),
/// the pid and a process-wide counter. No clock read is involved.
pub fn new_incarnation() -> [u8; 16] {
    use std::hash::BuildHasher as _;
    use std::io::Read as _;
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    let mut h = blake3::Hasher::new();
    h.update(b"ostro node incarnation v1");
    let mut os = [0u8; 16];
    if std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut os))
        .is_ok()
    {
        h.update(&os);
    }
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let rs = std::collections::hash_map::RandomState::new();
    h.update(&rs.hash_one((0u8, n)).to_le_bytes());
    h.update(&rs.hash_one((1u8, n)).to_le_bytes());
    h.update(&std::process::id().to_le_bytes());
    h.update(&n.to_le_bytes());
    let mut out = [0u8; 16];
    out.copy_from_slice(&h.finalize().as_bytes()[..16]);
    out
}

/// The keys of `node.toml` this version reads.
#[derive(Debug, PartialEq, Eq)]
struct NodeFile {
    version: u64,
    partitions: u16,
    incarnation: Option<[u8; 16]>,
}

fn parse_incarnation(v: &str) -> Result<[u8; 16], NodeError> {
    let bad = || NodeError::NodeFile(format!("incarnation {v:?}: want 32 hex chars in quotes"));
    let inner = v
        .strip_prefix('"')
        .and_then(|x| x.strip_suffix('"'))
        .ok_or_else(bad)?;
    if inner.len() != 32 || !inner.is_ascii() {
        return Err(bad());
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&inner[2 * i..2 * i + 2], 16).map_err(|_| bad())?;
    }
    Ok(out)
}

/// The known keys from the `key = value` lines of `node.toml` (unknown keys are ignored).
fn parse_node_file(s: &str) -> Result<NodeFile, NodeError> {
    let mut version = None;
    let mut partitions = None;
    let mut incarnation = None;
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .ok_or_else(|| NodeError::NodeFile(format!("malformed line {line:?}")))?;
        let v = v.trim();
        match k.trim() {
            "format_version" => {
                version = Some(
                    v.parse::<u64>()
                        .map_err(|e| NodeError::NodeFile(format!("format_version {v:?}: {e}")))?,
                );
            }
            "partitions" => {
                partitions = Some(
                    v.parse::<u16>()
                        .map_err(|e| NodeError::NodeFile(format!("partitions {v:?}: {e}")))?,
                );
            }
            "incarnation" => incarnation = Some(parse_incarnation(v)?),
            _ => {}
        }
    }
    match (version, partitions) {
        (Some(version), Some(partitions)) => Ok(NodeFile {
            version,
            partitions,
            incarnation,
        }),
        _ => Err(NodeError::NodeFile(
            "missing format_version or partitions".into(),
        )),
    }
}

/// tmp + fsync + rename + dir fsync.
fn write_durably(dir: &Path, name: &str, body: &[u8]) -> std::io::Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(body)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    std::fs::File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_file_parse() {
        let vp = |s: &str| {
            let f = parse_node_file(s).unwrap();
            (f.version, f.partitions)
        };
        assert_eq!(vp("format_version = 1\npartitions = 16\n"), (1, 16));
        assert_eq!(
            vp("# c\n\npartitions=4\nformat_version=1\nother = x\n"),
            (1, 4)
        );
        let f = parse_node_file(
            "format_version = 1\npartitions = 2\nincarnation = \"000102030405060708090a0b0c0d0eff\"\n",
        )
        .unwrap();
        assert_eq!(
            f.incarnation,
            Some([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255])
        );
        assert_eq!(
            parse_node_file("format_version = 1\npartitions = 2\n")
                .unwrap()
                .incarnation,
            None
        );
        for bad in [
            "00",
            "\"00\"",
            "000102030405060708090a0b0c0d0eff",
            "\"zz0102030405060708090a0b0c0d0eff\"",
        ] {
            assert!(
                parse_node_file(&format!(
                    "format_version = 1\npartitions = 2\nincarnation = {bad}\n"
                ))
                .is_err(),
                "{bad}"
            );
        }
        assert_ne!(new_incarnation(), new_incarnation());
        assert_eq!(hex16(&[0xab; 16]), "ab".repeat(16));
        assert!(parse_node_file("partitions = 4\n").is_err());
        assert!(parse_node_file("format_version = 1\npartitions = x\n").is_err());
        assert!(parse_node_file("garbage\n").is_err());
    }
}
