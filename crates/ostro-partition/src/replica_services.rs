//! Replica services (12-13, research Pitfall 12): the local snapshot service and log
//! retention of one partition, run in **every** role (leader, follower, offline) for as long
//! as the partition is open, not only under [`leader::spawn_all`](crate::leader::spawn_all).
//!
//! A follower is a hot standby: its apply machine keeps applying up to the follower HW, so it
//! keeps taking local snapshots and trims its WAL below them like a leader does. Retention's
//! floors are unchanged (`min(checkpoint, local snapshot floor, upload floor?, retention
//! floor?)`, see [`retention`](crate::leader::retention)), with one role-dependent input: the
//! bottomless uploader runs only on leaders, so on a follower (or offline replica) the upload
//! floor is **not** a constraint. The role driver feeds retention an *effective* upload floor
//! that follows the uploader's floor while the partition leads and is `None` (unbounded)
//! otherwise; a follower therefore trims by its snapshot floor only. A replica that is later
//! promoted resumes uploading from its snapshots (12-15).
//!
//! The services are spawned once the partition's state caught up (its
//! [`ReadyBarrier`](ostro_log::ReadyBarrier) is open, so retention may mint permits) and
//! stopped around a snapshot install ([`PartitionControl`](crate::roles::PartitionControl)).

use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::leader::retention::{self, RetentionConfig};
use crate::leader::{LeaderCtx, snapshot};

/// The running replica services of one partition.
#[derive(Debug)]
pub struct ReplicaTasks {
    stop: Arc<watch::Sender<bool>>,
    tasks: Vec<JoinHandle<()>>,
}

impl ReplicaTasks {
    /// Signal every service to stop and wait for them.
    pub async fn shutdown(self) {
        self.stop.send_replace(true);
        for t in self.tasks {
            let _ = t.await;
        }
    }
}

/// Spawn the snapshot ticker (when `ctx.snapshots` is set and enabled) and log retention for
/// `ctx.partition`, whatever its role. They stop when either `ctx.shutdown` turns true or
/// [`ReplicaTasks::shutdown`] is called.
pub fn spawn_replica_services(ctx: LeaderCtx, retention: &RetentionConfig) -> ReplicaTasks {
    let (stop, child, fwd) = crate::leader::child_ctx(ctx);
    let mut tasks = Vec::with_capacity(3);
    if let Some(s) = &child.snapshots
        && s.config().enabled
    {
        tasks.push(snapshot::spawn(child.clone(), Arc::clone(s)));
    }
    tasks.push(retention::spawn(child, retention.clone()));
    tasks.push(fwd);
    ReplicaTasks { stop, tasks }
}
