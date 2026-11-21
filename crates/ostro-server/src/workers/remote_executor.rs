//! Phase 7: implemented in 07-12.
//!
//! The remote runner: a dispatcher `Job` placed on a worker stays a server-side job; the
//! runner appends `RunStarted` on the server, sends `Activate`, serves `FetchJournal` /
//! `Append` / `StartEffect` through the run's `PartitionSink` (built by
//! `EngineExecutor::sink_for`, bridge included, 06-14 B1) and maps `Done` through the
//! shared outcome mapper. Stream loss is not an attempt failure (Pitfall 3).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use ostro_partition::leader::placement::ReservationKind;
use ostro_partition::{ExecOutcome, Executor, Job};
use tracing::warn;

use crate::p7::{P7Ctx, P7Shared};

/// Runs embedded reservations on the wrapped (engine) executor and worker reservations
/// on the [`RemoteRunner`].
pub struct PlacementExecutor {
    embedded: Arc<dyn Executor>,
    remote: Arc<RemoteRunner>,
}

impl PlacementExecutor {
    pub fn new(embedded: Arc<dyn Executor>, remote: Arc<RemoteRunner>) -> Self {
        Self { embedded, remote }
    }
}

impl Executor for PlacementExecutor {
    fn execute(&self, job: Job) -> Pin<Box<dyn Future<Output = ExecOutcome> + Send + 'static>> {
        match job.reservation_kind() {
            ReservationKind::Embedded => self.embedded.execute(job),
            ReservationKind::Worker => {
                let remote = Arc::clone(&self.remote);
                Box::pin(async move { remote.run(job).await })
            }
        }
    }
}

/// Proxies one leased job to a worker session (Task 2 of 07-12).
pub struct RemoteRunner {
    shared: Arc<P7Shared>,
}

impl RemoteRunner {
    pub fn new(ctx: &P7Ctx) -> Self {
        Self {
            shared: Arc::clone(&ctx.shared),
        }
    }

    /// Run `job` on its worker.
    pub async fn run(&self, job: Job) -> ExecOutcome {
        let _ = &self.shared;
        warn!(run = %job.run, "remote execution is not wired yet; abandoning the attempt");
        ExecOutcome::Abandoned
    }
}
