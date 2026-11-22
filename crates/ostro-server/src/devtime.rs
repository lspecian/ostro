//! Dev time idle detector and automatic time skipping (SDK-06, D-P10-6, plan 10-13).
//!
//! [`DevTime`] exists only when time skipping is on (`ostro dev --ephemeral
//! --time-skipping`). It answers two questions from read-only snapshots:
//!
//! - **Is the node idle?** Every input must show no work:
//!   - per partition: `applied == log_end` (no committed or appended command waits for
//!     apply; `MachineHandle::applied` watch + `ReplicatedLog::log_end`);
//!   - per partition, from the store: no `ready` row due now (`query::ready_entries`)
//!     and no `outbox` row (the shuffler has delivered and truncated everything);
//!   - node-wide: no dispatcher lease and no grant in flight
//!     ([`DispatcherStats::idle_stats`]: activations, attempt-failure backoffs included);
//!   - no effect task in flight (`Bridge::inflight`, Phase 6);
//!   - no mutating API request in flight ([`count_api_writes`], the server's
//!     `ApiWriteCounter` layer).
//! - **What is due next?** The minimum over partitions of the earliest `timers` row
//!   excluding `EXPIRE_OP` ([`ostro_partition::leader::timers::earliest_due_ms`]) and the
//!   earliest future `ready_at` (a step backoff or a delayed start). Schedules (Phase 8
//!   cron) are **not** skip targets: they live in meta, and fire as the clock passes them
//!   under their catch-up policy.
//!
//! The store reads are fenced by the per-partition applied offsets: a snapshot is valid
//! only if the offsets read before and after the scan agree and equal the log end. Store
//! contents change only through apply, so a later check that sees the same offsets sees
//! the same store; the scan result is therefore cached per offsets, and a parked node that
//! is polled every 10 ms reads the store once per apply, not once per poll (the scans walk
//! the tables' tombstones, which grow with the run history until compaction).
//!
//! # Auto mode (D-P10-6 [USER REVIEW], W1)
//!
//! [`DevTime::spawn_auto`] polls every [`POLL`]. It skips the clock to `next_due` only
//! when the node is idle, at least one result long-poll waits
//! ([`RunWatchers::pending_waiters`](crate::control::watch::RunWatchers::pending_waiters)),
//! and both still hold after a [`QUIESCENCE`] re-check with an unchanged target and
//! unchanged offsets. The final check runs under the skip barrier's write guard
//! ([`DevClock::skip_guarded`]), so no dispatcher grant interleaves it, and reads only
//! atomics and `watch` values (snapshot rule W-A): the counters, the waiter count and the
//! offsets, which must equal the re-checked snapshot's. Manual mode never auto-skips.
//!
//! # Known limit
//!
//! The apply thread publishes `applied` just before it broadcasts the chunk's actions, and
//! the dispatcher learns new work from that broadcast. Idleness is read from the store, not
//! from the dispatcher's queue, so a just-applied ready row is visible at once; the
//! remaining window (a dispatcher entry with no store row, e.g. a stale entry after a
//! cancel) can at worst start a grant that waits on the barrier and then runs at the new
//! time.
//!
//! The executor-level attempt-failure retry (`[executor] retry_backoff`,
//! `ExecOutcome::Retry`) keeps its lease during the real-time backoff, so the node is busy
//! and is never skipped across it: the backoff stays deliberately real (D-W26: no skip
//! while a lease is active). Lease TTLs stay real time too (10-06), so a skip can never
//! expire a lease.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::Method;
use axum::middleware::Next;
use axum::response::Response;
use ostro_log::ReplicatedLog;
use ostro_partition::DispatcherStats;
use ostro_partition::clock::Clock as _;
use ostro_partition::query;
use ostro_partition::store::Table;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::control::ServerControlPlane;
use crate::devclock::DevClock;

/// Auto-skip polling cadence (real time).
pub const POLL: Duration = Duration::from_millis(10);
/// Quiescence: an idle snapshot must still hold this long later (real time).
pub const QUIESCENCE: Duration = Duration::from_millis(20);
/// Most `timers` rows read per partition to find the earliest non-retention timer.
const TIMER_SCAN_LIMIT: usize = 4_096;
/// Most `ready` rows read per partition (more means busy anyway).
const READY_SCAN_LIMIT: usize = 4_096;

/// Atomic activity counters (all non-blocking loads).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    /// Leases or grants in flight (dispatcher).
    pub busy_activations: u64,
    /// Mutating API requests in flight.
    pub api_writes: usize,
    /// Effect tasks in flight (bridge).
    pub effects: usize,
    /// Result long-polls waiting.
    pub waiters: usize,
}

impl Counters {
    fn quiet(&self) -> bool {
        self.busy_activations == 0 && self.api_writes == 0 && self.effects == 0
    }
}

/// One idle snapshot (see the module docs).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DevSnapshot {
    /// Every idle input shows no work.
    pub idle: bool,
    /// The skip target: earliest non-retention timer or future `ready_at` (clock ms).
    pub next_due_ms: Option<u64>,
    /// Result long-polls waiting.
    pub waiters: usize,
    /// Leases or grants in flight.
    pub busy_activations: u64,
    /// Mutating API requests in flight.
    pub api_writes: usize,
    /// Why the node is not idle (first reason found), for logs and tests.
    pub reason: Option<&'static str>,
    /// Applied offset per partition (the fence; empty when not caught up).
    pub applied: Vec<u64>,
}

/// The idle detector and auto-skip loop (module docs).
pub struct DevTime {
    clock: Arc<DevClock>,
    cp: Weak<ServerControlPlane>,
    stats: Arc<DispatcherStats>,
    api_writes: Arc<AtomicUsize>,
    /// The last store view and the applied offsets it was read at.
    view: Mutex<Option<(Vec<u64>, StoreView)>>,
}

/// The store-derived idle inputs over every partition (see [`DevTime::store_view`]).
#[derive(Clone, Copy, Debug, Default)]
struct StoreView {
    /// Earliest `ready_at` of any `ready` row (due now = work queued).
    min_ready_at: Option<u64>,
    /// Some `outbox` row is not delivered yet.
    outbox: bool,
    /// Earliest non-retention timer.
    earliest_timer: Option<u64>,
    /// A read failed (never cached).
    failed: bool,
}

impl std::fmt::Debug for DevTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevTime")
            .field("clock", &self.clock)
            .field("api_writes", &self.api_writes.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl DevTime {
    /// A detector over the control plane's partitions, watchers and bridge, the shared
    /// dispatcher stats and the API write counter. Attaches itself to `clock`.
    pub fn new(
        clock: Arc<DevClock>,
        cp: &Arc<ServerControlPlane>,
        stats: Arc<DispatcherStats>,
        api_writes: Arc<AtomicUsize>,
    ) -> Arc<Self> {
        let t = Arc::new(Self {
            clock: Arc::clone(&clock),
            cp: Arc::downgrade(cp),
            stats,
            api_writes,
            view: Mutex::new(None),
        });
        clock.attach_time(&t);
        t
    }

    /// The API write counter the server's layer moves.
    pub fn api_writes(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.api_writes)
    }

    /// The dev clock.
    pub fn clock(&self) -> &Arc<DevClock> {
        &self.clock
    }

    /// The atomic counters (non-blocking; safe under the skip barrier).
    pub fn counters(&self) -> Counters {
        let Some(cp) = self.cp.upgrade() else {
            return Counters::default();
        };
        Counters {
            busy_activations: self.stats.idle_stats().busy(),
            api_writes: self.api_writes.load(Ordering::SeqCst),
            effects: cp.bridge().bridge().inflight(),
            waiters: cp.watchers().pending_waiters(),
        }
    }

    /// Applied offset per partition when every partition has applied its whole log;
    /// `None` otherwise (a slot failed to start, or a command waits for apply). Watch
    /// borrows and atomic loads only.
    fn fence(cp: &ServerControlPlane) -> Option<Vec<u64>> {
        cp.partitions
            .iter()
            .map(|slot| {
                let h = slot.as_ref()?;
                let applied = *h.machine().applied().borrow();
                (applied == h.log().log_end()).then_some(applied.0)
            })
            .collect()
    }

    /// A full snapshot (counters, fence, store scans; see the module docs). The store
    /// reads make this unsuitable under the skip barrier: re-check with [`Self::still`].
    pub fn snapshot(&self) -> DevSnapshot {
        let Some(cp) = self.cp.upgrade() else {
            return DevSnapshot {
                reason: Some("server stopped"),
                ..DevSnapshot::default()
            };
        };
        let c = self.counters();
        let mut s = DevSnapshot {
            waiters: c.waiters,
            busy_activations: c.busy_activations,
            api_writes: c.api_writes,
            ..DevSnapshot::default()
        };
        let not_idle = |mut s: DevSnapshot, why: &'static str| {
            s.idle = false;
            s.reason = Some(why);
            s
        };
        if c.busy_activations > 0 {
            return not_idle(s, "activations in flight");
        }
        if c.api_writes > 0 {
            return not_idle(s, "API writes in flight");
        }
        if c.effects > 0 {
            return not_idle(s, "effects in flight");
        }
        let Some(before) = Self::fence(&cp) else {
            return not_idle(s, "apply behind the log");
        };
        let view = match self.store_view(&cp, &before) {
            Some(v) => v,
            None => return not_idle(s, "state changed during the snapshot"),
        };
        let now = self.clock.now_ms();
        let mut why: Option<&'static str> = None;
        if view.failed {
            why = Some("store read failed");
        } else if view.outbox {
            why = Some("outbox not delivered");
        } else if view.min_ready_at.is_some_and(|at| at <= now) {
            why = Some("ready work queued");
        }
        let next = [view.min_ready_at, view.earliest_timer]
            .into_iter()
            .flatten()
            .min();
        s.next_due_ms = next;
        // Counters again: work that started and ended during the scan moved the offsets;
        // work still running shows here.
        let c2 = self.counters();
        if !c2.quiet() {
            return not_idle(s, "activity during the snapshot");
        }
        s.waiters = c2.waiters;
        if let Some(why) = why {
            return not_idle(s, why);
        }
        s.applied = before;
        s.idle = true;
        s
    }

    /// The store inputs at the applied offsets `fence`: cached per fence (the store only
    /// changes through apply, so repeated polls of a parked node read nothing), else
    /// scanned. `None` when apply moved during the scan.
    fn store_view(&self, cp: &ServerControlPlane, fence: &[u64]) -> Option<StoreView> {
        let mut cache = self
            .view
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((f, v)) = cache.as_ref()
            && f.as_slice() == fence
        {
            return Some(*v);
        }
        let mut v = StoreView::default();
        let min = |a: Option<u64>, b: u64| Some(a.map_or(b, |a| a.min(b)));
        for h in cp.partitions.iter().flatten() {
            let reader = h.reader();
            match query::ready_entries(&reader, READY_SCAN_LIMIT) {
                Ok(rows) => {
                    for r in rows {
                        v.min_ready_at = min(v.min_ready_at, r.ready_at_ms);
                    }
                }
                Err(_) => v.failed = true,
            }
            match reader.prefix(Table::Outbox, &[], 1) {
                Ok(rows) => v.outbox |= !rows.is_empty(),
                Err(_) => v.failed = true,
            }
            match ostro_partition::leader::timers::earliest_due_ms(&reader, true, TIMER_SCAN_LIMIT)
            {
                Ok(Some(due)) => v.earliest_timer = min(v.earliest_timer, due),
                Ok(None) => {}
                Err(_) => v.failed = true,
            }
        }
        if Self::fence(cp).as_deref() != Some(fence) {
            return None;
        }
        if !v.failed {
            *cache = Some((fence.to_vec(), v));
        }
        Some(v)
    }

    /// The barrier re-check of an idle snapshot `s`: atomics and watches only (W-A). True
    /// when nothing ran since `s` (same offsets, all caught up), no activation, API write or
    /// effect is in flight, and (with `need_waiter`) a long-poll still waits.
    pub fn still(&self, s: &DevSnapshot, need_waiter: bool) -> bool {
        let Some(cp) = self.cp.upgrade() else {
            return false;
        };
        let c = self.counters();
        s.idle
            && c.quiet()
            && (!need_waiter || c.waiters >= 1)
            && Self::fence(&cp).as_ref() == Some(&s.applied)
    }

    /// One auto-skip attempt (module docs). Returns the ms skipped (0 = no skip).
    pub async fn auto_step(&self) -> u64 {
        // Cheap gate first: no waiter or work in flight means no skip (no store read).
        let c = self.counters();
        if c.waiters == 0 || !c.quiet() {
            return 0;
        }
        let s1 = self.snapshot();
        let Some(due) = s1.next_due_ms else {
            return 0;
        };
        if !(s1.idle && s1.waiters >= 1 && due > self.clock.now_ms()) {
            return 0;
        }
        tokio::time::sleep(QUIESCENCE).await; // clock-exempt: dev auto-skip quiescence (real time)
        let s2 = self.snapshot();
        if !(s2.idle && s2.waiters >= 1 && s2.next_due_ms == Some(due) && s2.applied == s1.applied)
        {
            return 0;
        }
        let skipped = self
            .clock
            .skip_guarded(|now| (due > now && self.still(&s2, true)).then_some(due))
            .await;
        if skipped > 0 {
            debug!(skipped_ms = skipped, due_ms = due, "dev clock auto-skipped");
        }
        skipped
    }

    /// Run [`Self::auto_step`] every [`POLL`] until `shutdown` (auto mode only; the server
    /// spawns it only then).
    pub fn spawn_auto(self: Arc<Self>, shutdown: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            info!("dev time skipping: auto (skips while idle with a result waiter)");
            let mut tick = tokio::time::interval(POLL); // clock-exempt: dev auto-skip polling cadence
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = tick.tick() => {}
                }
                if self.cp.strong_count() == 0 {
                    break;
                }
                tokio::select! {
                    () = shutdown.cancelled() => break,
                    _ = self.auto_step() => {}
                }
            }
        })
    }
}

/// gRPC methods that never write (prefix of the method name).
const GRPC_READS: &[&str] = &[
    "Get", "List", "Await", "Fetch", "Watch", "History", "Check", "Describe",
];

/// Whether a request is a mutating API call for the idle detector: REST POST/PUT/PATCH/
/// DELETE (gRPC: every method that is not a read by name), excluding `/v1/dev/*`.
pub fn is_api_write(method: &Method, path: &str, grpc: bool) -> bool {
    if path.starts_with("/v1/dev/") {
        return false;
    }
    if grpc {
        let name = path.rsplit('/').next().unwrap_or("");
        return !GRPC_READS.iter().any(|p| name.starts_with(p));
    }
    matches!(
        *method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    )
}

/// The `ApiWriteCounter` layer body (installed only with time skipping on): counts a
/// mutating request from before the handler until its response is produced.
pub async fn count_api_writes(
    State(counter): State<Arc<AtomicUsize>>,
    req: Request,
    next: Next,
) -> Response {
    let grpc = req
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/grpc"));
    if !is_api_write(req.method(), req.uri().path(), grpc) {
        return next.run(req).await;
    }
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    counter.fetch_add(1, Ordering::SeqCst);
    let _g = Guard(counter);
    next.run(req).await
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::routing::{get, post};

    use super::*;

    #[test]
    fn devtime_api_write_classification() {
        assert!(is_api_write(&Method::POST, "/v1/runs", false));
        assert!(is_api_write(&Method::DELETE, "/v1/secrets/x", false));
        assert!(is_api_write(&Method::PUT, "/v1/queues/q", false));
        assert!(!is_api_write(&Method::GET, "/v1/runs/x/result", false));
        assert!(!is_api_write(&Method::POST, "/v1/dev/time/skip", false));
        assert!(is_api_write(&Method::POST, "/ostro.v1.Runs/Start", true));
        assert!(is_api_write(&Method::POST, "/ostro.v1.Runs/Signal", true));
        assert!(!is_api_write(
            &Method::POST,
            "/ostro.v1.Runs/AwaitResult",
            true
        ));
        assert!(!is_api_write(&Method::POST, "/ostro.v1.Runs/Get", true));
        assert!(!is_api_write(&Method::POST, "/ostro.v1.Runs/History", true));
    }

    #[tokio::test]
    async fn devtime_api_write_counter_spans_the_handler() {
        let counter = Arc::new(AtomicUsize::new(0));
        let (seen_tx, seen_rx) = tokio::sync::oneshot::channel::<usize>();
        let seen_tx = Arc::new(std::sync::Mutex::new(Some(seen_tx)));
        let c2 = Arc::clone(&counter);
        let app = Router::new()
            .route(
                "/v1/runs",
                post(move || {
                    let c = Arc::clone(&c2);
                    let tx = Arc::clone(&seen_tx);
                    async move {
                        if let Some(tx) = tx.lock().expect("lock").take() {
                            let _ = tx.send(c.load(Ordering::SeqCst));
                        }
                        "ok"
                    }
                }),
            )
            .route("/v1/runs/x", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                Arc::clone(&counter),
                count_api_writes,
            ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        let http = reqwest::Client::new();
        let res = http
            .post(format!("http://{addr}/v1/runs"))
            .send()
            .await
            .expect("POST");
        assert!(res.status().is_success());
        assert_eq!(seen_rx.await.expect("handler ran"), 1, "counted in flight");
        assert_eq!(counter.load(Ordering::SeqCst), 0, "released after");
        let res = http
            .get(format!("http://{addr}/v1/runs/x"))
            .send()
            .await
            .expect("GET");
        assert!(res.status().is_success());
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        server.abort();
    }
}
