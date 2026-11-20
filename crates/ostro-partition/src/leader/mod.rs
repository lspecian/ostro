//! Leader services of one partition: outbox shuffler, timer service, log retention and,
//! when configured, the dispatcher (plan 04-14). Phase 8 adds the local snapshot service
//! (`snapshot`, 08-09) and the cron driver (`cron`, 08-14); 08-01 declares both modules with
//! their config/trait contracts.
//!
//! They are spawned by [`Node`](crate::node::Node) only after the partition's
//! [`ReadyBarrier`] is open (state caught up, term started) and after every partition is
//! registered in the router. Each service has a fixed `spawn(ctx, cfg) -> JoinHandle<()>`
//! signature and stops when `ctx.shutdown` turns true.

pub mod cron;
pub mod dispatcher;
pub mod retention;
pub mod shuffler;
pub mod snapshot;
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

/// Configuration of the leader services.
#[derive(Clone, Default)]
pub struct LeaderConfig {
    pub shuffler: shuffler::ShufflerConfig,
    pub timers: timers::TimerConfig,
    pub retention: retention::RetentionConfig,
    /// The dispatcher runs only when this is `Some` (Phase 2 suites run without it).
    pub dispatcher: Option<dispatcher::DispatcherConfig>,
}

impl std::fmt::Debug for LeaderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderConfig")
            .field("shuffler", &self.shuffler)
            .field("timers", &self.timers)
            .field("retention", &self.retention)
            .field("dispatcher", &self.dispatcher)
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

/// Spawn shuffler, timers, retention and (if configured) the dispatcher for
/// `ctx.partition`. They stop when either
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
    let mut tasks = vec![
        shuffler::spawn(child.clone(), cfg.shuffler.clone()),
        timers::spawn(child.clone(), cfg.timers.clone()),
    ];
    if let Some(d) = &cfg.dispatcher {
        tasks.push(dispatcher::spawn(child.clone(), d.clone()));
    }
    tasks.push(retention::spawn(child, cfg.retention.clone()));
    tasks.push(fwd);
    LeaderTasks { stop, tasks }
}
