//! Leader services of one partition: outbox shuffler, timer service, log retention.
//!
//! They are spawned by [`Node`](crate::node::Node) only after the partition's
//! [`ReadyBarrier`] is open (state caught up, term started) and after every partition is
//! registered in the router. Each service has a fixed `spawn(ctx, cfg) -> JoinHandle<()>`
//! signature and stops when `ctx.shutdown` turns true.

pub mod retention;
pub mod shuffler;
pub mod timers;

use std::sync::Arc;

use ostro_log::{ReadyBarrier, Rf1Log};
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
    pub handle: PartitionHandle<Rf1Log>,
    pub router: Arc<dyn PartitionRouter>,
    pub barrier: ReadyBarrier,
    pub clock: Arc<dyn Clock>,
    pub shutdown: watch::Receiver<bool>,
}

impl std::fmt::Debug for LeaderCtx {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderCtx")
            .field("partition", &self.partition)
            .field("count", &self.count)
            .finish_non_exhaustive()
    }
}

/// Configuration of the three leader services.
#[derive(Clone, Debug, Default)]
pub struct LeaderConfig {
    pub shuffler: shuffler::ShufflerConfig,
    pub timers: timers::TimerConfig,
    pub retention: retention::RetentionConfig,
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

/// Spawn shuffler, timers and retention for `ctx.partition`. They stop when either
/// `ctx.shutdown` turns true or [`LeaderTasks::shutdown`] is called.
pub fn spawn_all(ctx: LeaderCtx, cfg: &LeaderConfig) -> LeaderTasks {
    let stop = Arc::new(watch::channel(false).0);
    let mut parent = ctx.shutdown.clone();
    let child = LeaderCtx {
        shutdown: stop.subscribe(),
        ..ctx
    };
    // Forward the node-wide shutdown to this partition's services.
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
    let tasks = vec![
        shuffler::spawn(child.clone(), cfg.shuffler.clone()),
        timers::spawn(child.clone(), cfg.timers.clone()),
        retention::spawn(child, cfg.retention.clone()),
        fwd,
    ];
    LeaderTasks { stop, tasks }
}
