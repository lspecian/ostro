//! `Node`: opens the P partitions of one data directory in a fixed, tested order
//! (STOR-03, STOR-08, STOR-09, STOR-10; see [`startup`](crate::startup) for the sequence).
//!
//! Layout under `data_dir`:
//! - `node.toml`: `format_version = 1` and the fixed partition count (STOR-09);
//! - `state/pNNNN`: one fjall state store per partition (also records the count);
//! - `wal/`: the shared group-commit WAL of every partition log;
//! - `blobs/`: the content-addressed blob store shared by all partitions.

use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ostro_blob::BlobStore;
use ostro_log::ostro_wal::GroupCommitConfig;
use ostro_log::{LogError, LogNode, LogNodeConfig, ReadyBarrier, ReplicatedLog, Rf1Log, Role};
use ostro_types::{Command, Offset, PartitionCount, PartitionId};
use tokio::sync::watch;
use tracing::{error, warn};

use crate::acks::PendingAcks;
use crate::apply::{ApplyOutcome, Cursor};
use crate::clock::{Clock, SystemClock};
use crate::handle::PartitionHandle;
use crate::leader::{self, LeaderConfig, LeaderCtx, LeaderTasks};
use crate::leadership::{LeadershipSource, LocalLeadership, next_term_epoch};
use crate::machine::{MachineConfig, MachineHandle, PartitionHealth};
use crate::router::{InProcessRouter, PartitionRouter};
use crate::startup::{StartupEvent, StartupLog};
use crate::store::{PartitionStore, StoreConfig, StoreMeta};

/// `node.toml` format version.
const NODE_FILE_VERSION: u64 = 1;
const NODE_FILE: &str = "node.toml";

/// What the node does after recovery.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum StartMode {
    /// Full startup: new term, readiness barrier, leader services.
    Leader,
    /// Recover and catch up only: no term, no barrier, no leader services, no appends
    /// (digest comparisons, offline inspection).
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
}

impl NodeConfig {
    /// Defaults: node id 1, default store/machine/WAL/leader configs, [`LocalLeadership`],
    /// [`SystemClock`], [`StartMode::Leader`].
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
            clock: Arc::new(SystemClock),
            router_wrapper: None,
        }
    }
}

/// One partition after startup.
#[derive(Debug)]
pub enum PartitionSlot {
    Ready(PartitionHandle<Rf1Log>),
    /// The partition failed to start (corrupt log, state ahead of log, ...). The other
    /// partitions are unaffected.
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

/// An open node: P partitions over one shared WAL.
pub struct Node {
    log: LogNode,
    slots: Vec<PartitionSlot>,
    barriers: Vec<ReadyBarrier>,
    router: Arc<InProcessRouter>,
    events: Arc<StartupLog>,
    leaders: Vec<LeaderTasks>,
    shutdown: watch::Sender<bool>,
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("partitions", &self.log.partitions())
            .field("slots", &self.slots)
            .finish_non_exhaustive()
    }
}

/// Per-partition state between the startup steps.
type Stage = Result<(Arc<PartitionStore>, StoreMeta), String>;
/// A partition that recovered and validated: store, meta, log.
type Recovered = (Arc<PartitionStore>, StoreMeta, Arc<Rf1Log>);

impl Node {
    /// Open every partition in the order documented in [`startup`](crate::startup).
    pub async fn open(cfg: NodeConfig) -> Result<Node, NodeError> {
        let events = Arc::new(StartupLog::default());
        let count = cfg.partitions;
        std::fs::create_dir_all(&cfg.data_dir)?;

        // 0. the fixed partition count.
        check_node_file(&cfg.data_dir, count)?;
        events.push(StartupEvent::NodeFileChecked);

        // 1. state stores (the durable floors), in parallel.
        let mut opens = Vec::with_capacity(usize::from(count.get()));
        for p in count.ids() {
            let dir = cfg.data_dir.clone();
            let sc = cfg.store.clone();
            opens.push(tokio::task::spawn_blocking(move || {
                PartitionStore::open(&dir, p, count, &sc)
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
        //    torn tail, so its files are never cut on behalf of an unknown state.
        let floors: Vec<Offset> = stages
            .iter()
            .map(|s| s.as_ref().map_or(Offset(u64::MAX), |(_, m)| m.applied))
            .collect();
        let mut lcfg = LogNodeConfig::new(cfg.data_dir.join("wal"), count, cfg.node_id);
        lcfg.wal = cfg.wal.clone();
        let (log, logs) = LogNode::open(lcfg, &floors).await?;

        let mut ready: Vec<Option<Recovered>> = Vec::with_capacity(stages.len());
        let mut failed: Vec<Option<String>> = vec![None; stages.len()];
        for ((stage, lr), p) in stages.into_iter().zip(logs).zip(count.ids()) {
            let r = stage.and_then(|(store, meta)| match lr {
                Ok(rf1) => {
                    let rep = rf1.recovery();
                    events.push(StartupEvent::LogRecovered {
                        p: p.0,
                        leo: rep.leo.0,
                        torn: rep.torn.is_some(),
                    });
                    // 3. validation.
                    validate(&meta, &rf1)?;
                    events.push(StartupEvent::Validated { p: p.0 });
                    Ok((store, meta, rf1))
                }
                Err(e) => Err(format!("log recovery: {e}")),
            });
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

        // 4./5. catch-up and term start, partitions in parallel.
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
                    mode: cfg.mode,
                    leadership: Arc::clone(&cfg.leadership),
                    clock: Arc::clone(&cfg.clock),
                    node_id: cfg.node_id,
                    events: Arc::clone(&events),
                };
                tokio::spawn(start.run())
            }));
        }
        let mut slots = Vec::with_capacity(tasks.len());
        for (t, p) in tasks.into_iter().zip(count.ids()) {
            let r = match t {
                None => Err(failed[p.index()].take().unwrap_or_default()),
                Some(t) => match t.await {
                    Ok(Ok(h)) => Ok(h),
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
            slots.push(match r {
                Ok(h) => PartitionSlot::Ready(h),
                Err(reason) => PartitionSlot::Failed {
                    partition: p,
                    reason,
                },
            });
        }

        let barriers: Vec<ReadyBarrier> = count.ids().map(|_| ReadyBarrier::new()).collect();
        let router = Arc::new(InProcessRouter::new(count));
        let (shutdown, _) = watch::channel(false);
        let mut leaders = Vec::new();

        if cfg.mode == StartMode::Leader {
            let services_router: Arc<dyn PartitionRouter> = match &cfg.router_wrapper {
                Some(wrap) => wrap(Arc::clone(&router)),
                None => Arc::clone(&router) as Arc<dyn PartitionRouter>,
            };
            // 6. barriers + routing, then services once every partition is routable.
            for slot in &slots {
                if let PartitionSlot::Ready(h) = slot {
                    let p = h.partition();
                    barriers[p.index()].open();
                    events.push(StartupEvent::BarrierOpened { p: p.0 });
                    router.register(h.clone());
                }
            }
            for slot in &slots {
                if let PartitionSlot::Ready(h) = slot {
                    let p = h.partition();
                    let ctx = LeaderCtx {
                        partition: p,
                        count,
                        handle: h.clone(),
                        router: Arc::clone(&services_router),
                        barrier: barriers[p.index()].clone(),
                        clock: Arc::clone(&cfg.clock),
                        shutdown: shutdown.subscribe(),
                    };
                    leaders.push(leader::spawn_all(ctx, &cfg.leader));
                    events.push(StartupEvent::LeaderServicesStarted { p: p.0 });
                }
            }
        }

        Ok(Node {
            log,
            slots,
            barriers,
            router,
            events,
            leaders,
            shutdown,
        })
    }

    /// Partition `p`'s handle, if it started.
    pub fn handle(&self, p: PartitionId) -> Option<&PartitionHandle<Rf1Log>> {
        match self.slots.get(p.index()) {
            Some(PartitionSlot::Ready(h)) => Some(h),
            _ => None,
        }
    }

    pub fn slots(&self) -> &[PartitionSlot] {
        &self.slots
    }

    pub fn router(&self) -> Arc<InProcessRouter> {
        Arc::clone(&self.router)
    }

    /// Partition `p`'s readiness barrier (closed in Inspect mode and for failed slots).
    pub fn barrier(&self, p: PartitionId) -> Option<ReadyBarrier> {
        self.barriers.get(p.index()).cloned()
    }

    /// Startup steps in order of occurrence.
    pub fn startup_events(&self) -> Vec<StartupEvent> {
        self.events.snapshot()
    }

    /// Stop leader services, shut every machine down gracefully (final checkpoint), then
    /// flush and seal the WAL. Per-partition shutdown errors are logged (the WAL holds
    /// everything acknowledged; the next open replays from the state's `applied_offset`).
    pub async fn shutdown(self) -> Result<(), NodeError> {
        let Node {
            log,
            slots,
            router,
            leaders,
            shutdown,
            ..
        } = self;
        shutdown.send_replace(true);
        for t in leaders {
            t.shutdown().await;
        }
        router.clear();
        for slot in slots {
            if let PartitionSlot::Ready(h) = slot {
                let p = h.partition();
                if let Err(e) = h.machine().clone().shutdown().await {
                    warn!(partition = p.0, error = %e, "partition shutdown");
                }
            }
        }
        log.shutdown().await;
        Ok(())
    }
}

/// Step 3: the state must lie inside the log.
fn validate(meta: &StoreMeta, log: &Rf1Log) -> Result<(), String> {
    let leo = log.log_end();
    if meta.applied > leo {
        return Err(format!(
            "state ahead of log: rebuild the state store (applied {} > log end {})",
            meta.applied.0, leo.0
        ));
    }
    if meta.applied < log.log_start() {
        return Err(format!(
            "snapshot required (Phase 8): applied {} below log start {}",
            meta.applied.0,
            log.log_start().0
        ));
    }
    Ok(())
}

/// Steps 4 and 5 of one partition.
struct PartitionStart {
    p: PartitionId,
    store: Arc<PartitionStore>,
    meta: StoreMeta,
    rf1: Arc<Rf1Log>,
    blobs: Arc<BlobStore>,
    machine: MachineConfig,
    mode: StartMode,
    leadership: Arc<dyn LeadershipSource>,
    clock: Arc<dyn Clock>,
    node_id: u64,
    events: Arc<StartupLog>,
}

impl PartitionStart {
    async fn run(self) -> Result<PartitionHandle<Rf1Log>, String> {
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
        // services; never re-emit them.
        machine.set_emit_actions(false);
        let handle = PartitionHandle::new(
            self.p,
            Arc::clone(&self.rf1),
            Arc::clone(&self.store),
            machine.clone(),
            acks,
            Arc::clone(&self.blobs),
        );
        match self.catch_up_and_announce(&handle).await {
            Ok(()) => Ok(handle),
            Err(reason) => {
                let _ = machine.shutdown().await;
                Err(reason)
            }
        }
    }

    async fn catch_up_and_announce(&self, handle: &PartitionHandle<Rf1Log>) -> Result<(), String> {
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
        if self.mode == StartMode::Inspect {
            return Ok(());
        }

        // 5. a new term: LeaderAnnounced{epoch} is its first record.
        let epoch = next_term_epoch(
            self.leadership.epoch_floor(self.p),
            self.rf1.epochs().latest(),
        );
        self.rf1.set_role(Role::Leader { epoch });
        handle.set_epoch(epoch);
        let la = Command::leader_announced(epoch.0, self.node_id, self.clock.now_ms());
        match handle.submit_one(la).await {
            Ok(ApplyOutcome::Accepted) => {}
            Ok(o) => return Err(format!("LeaderAnnounced{{epoch {}}}: {o:?}", epoch.0)),
            Err(e) => return Err(format!("LeaderAnnounced{{epoch {}}}: {e}", epoch.0)),
        }
        let offset = self
            .rf1
            .epochs()
            .0
            .last()
            .filter(|(e, _)| *e == epoch)
            .map(|(_, o)| o.0)
            .ok_or_else(|| format!("epoch {} missing from the log history", epoch.0))?;
        self.events.push(StartupEvent::TermStarted {
            p,
            epoch: epoch.0,
            offset,
        });
        machine.set_emit_actions(true);
        Ok(())
    }
}

/// Create `node.toml` or check its partition count against `count`.
fn check_node_file(dir: &Path, count: PartitionCount) -> Result<(), NodeError> {
    let path = dir.join(NODE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(s) => {
            let (version, stored) = parse_node_file(&s)?;
            if version != NODE_FILE_VERSION {
                return Err(NodeError::NodeFile(format!(
                    "{}: unsupported format_version {version}",
                    path.display()
                )));
            }
            if stored != count.get() {
                return Err(NodeError::PartitionCountMismatch {
                    configured: count.get(),
                    stored,
                });
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let body = format!(
                "format_version = {NODE_FILE_VERSION}\npartitions = {}\n",
                count.get()
            );
            write_durably(dir, NODE_FILE, body.as_bytes())?;
            Ok(())
        }
        Err(e) => Err(e.into()),
    }
}

/// `(format_version, partitions)` from the `key = value` lines of `node.toml`.
fn parse_node_file(s: &str) -> Result<(u64, u16), NodeError> {
    let mut version = None;
    let mut partitions = None;
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
            _ => {}
        }
    }
    match (version, partitions) {
        (Some(v), Some(p)) => Ok((v, p)),
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
        assert_eq!(
            parse_node_file("format_version = 1\npartitions = 16\n").unwrap(),
            (1, 16)
        );
        assert_eq!(
            parse_node_file("# c\n\npartitions=4\nformat_version=1\nother = x\n").unwrap(),
            (1, 4)
        );
        assert!(parse_node_file("partitions = 4\n").is_err());
        assert!(parse_node_file("format_version = 1\npartitions = x\n").is_err());
        assert!(parse_node_file("garbage\n").is_err());
    }
}
