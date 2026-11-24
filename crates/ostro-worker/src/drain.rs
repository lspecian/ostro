//! Drain (SIGTERM in the binary; the shutdown `CancellationToken` here).
//!
//! 1. Stop taking work: `draining` is set (later `Activate`s answer `Done{Evicted}`) and
//!    `Credit{0}` is sent.
//! 2. `Drain` tells the server this session is going away.
//! 3. Every in-flight activation is evicted (engine cancel → `Done{Evicted{External}}`).
//!    Evicted, not fenced: the leases are still this worker's, so each engine's final flush
//!    of its unflushed events is appended (a fenced flush would report `Fenced`, which the
//!    server reads as "a newer lease owns the run" and does not re-grant; ci-fix7).
//! 4. The worker waits for the activations to finish (bounded by [`DRAIN_BOUND`]), closes
//!    its half of the stream and waits briefly for the server to close the other half, so
//!    the queued `Done`s are flushed.

use std::sync::atomic::Ordering;
use std::time::Duration;

use ostro_worker_proto::v1;
use tracing::{info, warn};

use crate::session::Session;
use crate::{Shared, WorkerEvent};

/// Upper bound for evicted activations to report `Done`.
pub const DRAIN_BOUND: Duration = Duration::from_secs(10);
/// Upper bound for the server to close the stream after ours closed.
const CLOSE_BOUND: Duration = Duration::from_secs(2);

/// Drain `s` (see the module docs); always returns.
pub(crate) async fn drain(shared: &Shared, s: Session) {
    shared.draining.store(true, Ordering::Release);
    s.send(v1::worker_msg::Msg::Credit(v1::Credit { free_slots: 0 }));
    s.send(v1::worker_msg::Msg::Drain(v1::Drain {}));
    let n = shared.evict_all();
    info!(evicted = n, "draining");
    let wait = async {
        loop {
            let notified = shared.slots_changed.notified();
            if shared.active().is_empty() {
                return;
            }
            notified.await;
        }
    };
    if tokio::time::timeout(DRAIN_BOUND, wait).await.is_err() {
        warn!(
            remaining = shared.active().len(),
            "drain bound reached with activations still running"
        );
    }
    shared.set_link(None);
    let Session { out, inbound, .. } = s;
    drop(out);
    let mut inbound = inbound;
    let _ = tokio::time::timeout(CLOSE_BOUND, async {
        while let Ok(Some(_)) = inbound.message().await {}
    })
    .await;
    shared.emit(WorkerEvent::Drained);
}
