//! Leader services of one partition: outbox shuffler, timer service and, when configured,
//! the dispatcher (plan 04-14) and the cron driver (`cron`, 08-14). The local snapshot
//! service (`snapshot`, 08-09) and log retention (`retention`) are **replica services**
//! since 12-13: [`spawn_replica_services`](crate::replica_services::spawn_replica_services)
//! runs them in every role; their floors reach retention through [`LeaderCtx::floors`].
//!
//! Phase 7 (07-02) adds the dispatcher contracts [`placement`] (`Placement`,
//! `SemaphorePlacement`) and [`flow`] (`FlowControl`, `NoFlow`, `QueueGauges`), plus the
//! `fair` selection stub (07-05).
//!
//! They are spawned by [`Node`](crate::node::Node) only after the partition's
//! [`ReadyBarrier`] is open (state caught up, term started) and after every partition is
//! registered in the router. Each service has a fixed `spawn(ctx, cfg) -> JoinHandle<()>`
//! signature and stops when `ctx.shutdown` turns true.

pub mod cron;
pub mod dispatcher;
pub mod fair;
pub mod flow;
pub mod placement;
pub mod retention;
pub mod shuffler;
pub mod snapshot;
pub mod timers;

use std::sync::Arc;

use ostro_log::{PartitionLog, ReadyBarrier};
use ostro_types::{PartitionCount, PartitionId};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::clock::Clock;
use crate::handle::PartitionHandle;
use crate::router::PartitionRouter;

/// Everything a leader service needs.
#[derive(Clone)]
pub struct LeaderCtx {
    pub partition: PartitionId,
    pub count: PartitionCount,
    pub handle: PartitionHandle<PartitionLog>,
    pub router: Arc<dyn PartitionRouter>,
    pub barrier: ReadyBarrier,
    pub clock: Arc<dyn Clock>,
    pub shutdown: watch::Receiver<bool>,
    /// Dynamic trim floors (local snapshot floor, bottomless upload floor; P8-D2).
    pub floors: retention::FloorWatches,
    /// The partition's snapshot service (08-09); `None` = no snapshots, local floor fixed.
    pub snapshots: Option<Arc<snapshot::SnapshotService>>,
}

impl std::fmt::Debug for LeaderCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderCtx")
            .field("partition", &self.partition)
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

/// Configuration of the leader services.
#[derive(Clone, Default)]
pub struct LeaderConfig {
    pub shuffler: shuffler::ShufflerConfig,
    pub timers: timers::TimerConfig,
    /// Retention tuning (a replica service since 12-13; kept here for compatibility).
    pub retention: retention::RetentionConfig,
    /// The dispatcher runs only when this is `Some` (Phase 2 suites run without it).
    pub dispatcher: Option<dispatcher::DispatcherConfig>,
    /// The cron driver runs only when this is `Some` (08-14). `Node::open` fills it from
    /// `NodeConfig::{cron, schedule_source}` when a schedule source is configured.
    pub cron: Option<cron::CronService>,
}

impl std::fmt::Debug for LeaderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderConfig")
            .field("shuffler", &self.shuffler)
            .field("timers", &self.timers)
            .field("retention", &self.retention)
            .field("dispatcher", &self.dispatcher)
            .field("cron", &self.cron)
            .finish()
    }
}

/// The running services of one partition.
#[derive(Debug)]
pub struct LeaderTasks {
    stop: Arc<watch::Sender<bool>>,
    tasks: Vec<JoinHandle<()>>,
}

impl LeaderTasks {
    /// Signal every service to stop and wait for them.
    pub async fn shutdown(self) {
        self.stop.send_replace(true);
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

/// A copy of `ctx` whose `shutdown` is a fresh per-service-set stop signal, plus the task
/// forwarding the node-wide shutdown (`ctx.shutdown`) into it.
pub(crate) fn child_ctx(ctx: LeaderCtx) -> (Arc<watch::Sender<bool>>, LeaderCtx, JoinHandle<()>) {
    let stop = Arc::new(watch::channel(false).0);
    let mut parent = ctx.shutdown.clone();
    let child = LeaderCtx {
        shutdown: stop.subscribe(),
        ..ctx
    };
    let fwd = {
        let stop = Arc::clone(&stop);
        let mut mine = stop.subscribe();
        tokio::spawn(async move {
            tokio::select! {
                _ = parent.wait_for(|v| *v) => { stop.send_replace(true); }
                _ = mine.wait_for(|v| *v) => {}
            }
        })
    };
    (stop, child, fwd)
}

/// Spawn shuffler, timers and (if configured) the dispatcher and the cron driver for
/// `ctx.partition`. They stop when either `ctx.shutdown` turns true or
/// [`LeaderTasks::shutdown`] is called. Snapshots and retention are replica services
/// (12-13), started separately and kept across role changes.
pub fn spawn_all(ctx: LeaderCtx, cfg: &LeaderConfig) -> LeaderTasks {
    let (stop, child, fwd) = child_ctx(ctx);
    let mut tasks = vec![
        shuffler::spawn(child.clone(), cfg.shuffler.clone()),
        timers::spawn(child.clone(), cfg.timers.clone()),
    ];
    if let Some(d) = &cfg.dispatcher {
        tasks.push(dispatcher::spawn(child.clone(), d.clone()));
    }
    if let Some(c) = &cfg.cron {
        tasks.push(cron::spawn(child, c.clone()));
    }
    tasks.push(fwd);
    LeaderTasks { stop, tasks }
}
