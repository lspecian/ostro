//! Timer service (STOR-13): a lazily loaded, bounded window of due timers, fired by appending
//! `TimerFired` commands.
//!
//! The durable source of truth is the `timers` table (`due ‖ run ‖ op`, scanned in due order).
//! The leader keeps at most `window_max` of those rows in memory, with the invariant
//!
//! > every timer row with `due < horizon` is in the window
//!
//! A refill scans `timers` from the start (so overdue timers, e.g. after a restart, are always
//! included) up to `now + window_span_ms`, at most `window_max` rows. When the scan is
//! truncated, `horizon` becomes the last row's due: rows sharing that due may be only partly
//! loaded, which the invariant allows (it only covers `due < horizon`). Committed
//! `ScheduleTimer`/`CancelTimer` actions keep the window current between refills; a lagged
//! action stream forces a refill.
//!
//! Firing is idempotent end to end: apply removes the timer row on the first `TimerFired` and
//! answers `Duplicate` to any later one, so a stale action re-inserting an already fired
//! timer (or a re-fire by a new leader) is harmless. `fired_at_ms` comes from the clock, which
//! is read only here, never in apply.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use ostro_types::command::TimerFired;
use ostro_types::{Command, RunId};
use tokio::sync::broadcast::error::{RecvError, TryRecvError};
use tracing::{debug, error, info, warn};

use super::LeaderCtx;
use crate::apply::{Action, ApplyOutcome};
use crate::store::Table;
use crate::store::keys::decode_timer_key;

/// Longest real-time sleep between checks (ManualClock tests rely on this poll).
const MAX_SLEEP: Duration = Duration::from_millis(50);
/// Retry delay after a non-fatal submit/refill error.
const RETRY: Duration = Duration::from_millis(50);

/// Counters for tests and introspection (all monotonic except `window_len`).
#[derive(Debug, Default)]
pub struct TimerStats {
    /// Current window size.
    pub window_len: AtomicUsize,
    /// Largest window size ever observed (after enforcing `window_max`).
    pub max_window_len: AtomicUsize,
    /// `TimerFired` commands applied as `Accepted`.
    pub fired: AtomicU64,
    /// `TimerFired` commands answered `Duplicate`.
    pub duplicates: AtomicU64,
    /// Refill scans performed.
    pub refills: AtomicU64,
}

/// Timer service tuning.
#[derive(Clone, Debug)]
pub struct TimerConfig {
    /// Most timers held in memory. Default 10 000.
    pub window_max: usize,
    /// How far ahead the window reaches (ms). Default 60 000.
    pub window_span_ms: u64,
    /// Most `TimerFired` commands per append. Default 256.
    pub fire_batch: usize,
    /// Optional introspection counters. Default `None`.
    pub stats: Option<Arc<TimerStats>>,
}

impl Default for TimerConfig {
    fn default() -> Self {
        Self {
            window_max: 10_000,
            window_span_ms: 60_000,
            fire_batch: 256,
            stats: None,
        }
    }
}

/// One timer: `(due, run, op)`; tuple order equals the `timers` key order.
type Entry = (u64, RunId, u64);

/// The in-memory window and its horizon.
struct Window {
    set: BTreeSet<Entry>,
    /// Every timer row with `due < horizon` is in `set`. 0 = nothing known (refill needed).
    horizon: u64,
    max: usize,
}

impl Window {
    fn new(max: usize) -> Self {
        Self {
            set: BTreeSet::new(),
            horizon: 0,
            max: max.max(1),
        }
    }

    /// Replace the window with a due-ordered scan result. `truncated` = the scan hit its
    /// limit, so rows beyond the last one may exist.
    fn reset(&mut self, rows: Vec<Entry>, truncated: bool, reach: u64) {
        self.horizon = match (truncated, rows.last()) {
            (true, Some(last)) => last.0,
            _ => reach,
        };
        self.set = rows.into_iter().collect();
    }

    fn insert(&mut self, e: Entry) {
        if e.0 >= self.horizon {
            return; // the next refill loads it
        }
        self.set.insert(e);
        while self.set.len() > self.max {
            if let Some(popped) = self.set.pop_last() {
                // Rows with due < popped.0 are all still loaded.
                self.horizon = popped.0;
            }
        }
    }

    fn remove(&mut self, e: &Entry) {
        self.set.remove(e);
    }

    fn earliest(&self) -> Option<u64> {
        self.set.first().map(|e| e.0)
    }

    /// Up to `n` entries with `due <= now`, earliest first.
    fn due(&self, now: u64, n: usize) -> Vec<Entry> {
        self.set
            .iter()
            .take_while(|e| e.0 <= now)
            .take(n)
            .copied()
            .collect()
    }
}

/// The earliest due time in the `timers` table (plan 10-13, the dev clock's skip target),
/// skipping `EXPIRE_OP` rows (run retention) when `exclude_expire` is set. Read-only.
///
/// This reads the table, not the service's window: the window only reaches
/// `now + window_span_ms`, so a multi-day sleep is never in it, while the table is the
/// source of truth (a fired timer's row goes away when its `TimerFired` applies). At most
/// `scan_limit` rows are read in due order; `Ok(None)` when no qualifying row is among them
/// (no timer, or only retention timers within the limit).
pub fn earliest_due_ms(
    reader: &crate::store::StoreReader,
    exclude_expire: bool,
    scan_limit: usize,
) -> Result<Option<u64>, crate::store::StoreError> {
    let rows = reader.prefix(Table::Timers, &[], scan_limit.max(1))?;
    Ok(rows.iter().find_map(|(k, _)| {
        let (due, _, op) = decode_timer_key(k)?;
        (!(exclude_expire && op == ostro_types::command::EXPIRE_OP)).then_some(due)
    }))
}

/// Start the timer service of `ctx.partition`. Runs until `ctx.shutdown` turns true.
pub fn spawn(ctx: LeaderCtx, cfg: TimerConfig) -> tokio::task::JoinHandle<()> {
    // Subscribe before the task starts so nothing committed after spawn is missed; anything
    // committed before is in the table and the first refill loads it.
    let actions = ctx.handle.machine().subscribe_actions();
    tokio::spawn(async move {
        let svc = TimerService::new(ctx, cfg);
        svc.run(actions).await;
    })
}

struct TimerService {
    ctx: LeaderCtx,
    cfg: TimerConfig,
    win: Window,
    stats: Arc<TimerStats>,
    m_window: metrics::Gauge,
    m_fired: metrics::Counter,
}

impl TimerService {
    fn new(ctx: LeaderCtx, cfg: TimerConfig) -> Self {
        let label = ctx.partition.0.to_string();
        Self {
            win: Window::new(cfg.window_max),
            stats: cfg.stats.clone().unwrap_or_default(),
            m_window: metrics::gauge!("ostro_timer_window_entries", "partition" => label.clone()),
            m_fired: metrics::counter!("ostro_timers_fired_total", "partition" => label),
            ctx,
            cfg,
        }
    }

    fn publish(&self) {
        let n = self.win.set.len();
        self.stats.window_len.store(n, Ordering::Relaxed);
        self.stats.max_window_len.fetch_max(n, Ordering::Relaxed);
        self.m_window.set(n as f64);
    }

    fn on_actions(&mut self, batch: &[Action]) {
        for a in batch {
            match a {
                Action::ScheduleTimer { due_at_ms, run, op } => {
                    self.win.insert((*due_at_ms, *run, *op));
                }
                Action::CancelTimer { due_at_ms, run, op } => {
                    self.win.remove(&(*due_at_ms, *run, *op));
                }
                _ => {}
            }
        }
    }

    /// Scan `timers` from the start to `now + span`, at most `window_max` rows.
    fn refill(&mut self, now: u64) -> Result<(), String> {
        let reach = now.saturating_add(self.cfg.window_span_ms);
        // Keys are `due BE ‖ run ‖ op`: the 8-byte due prefixes bound the scan to
        // `0 <= due < reach` (a full key with due == reach sorts after its own prefix).
        let from = 0u64.to_be_bytes();
        let to = reach.to_be_bytes();
        let rows = self
            .ctx
            .handle
            .reader()
            .range(Table::Timers, &from, &to, self.win.max)
            .map_err(|e| e.to_string())?;
        let truncated = rows.len() >= self.win.max;
        let mut entries = Vec::with_capacity(rows.len());
        for (k, _) in rows {
            match decode_timer_key(&k) {
                Some(e) => entries.push(e),
                None => warn!(
                    partition = self.ctx.partition.0,
                    "undecodable timer key skipped"
                ),
            }
        }
        self.win.reset(entries, truncated, reach);
        self.stats.refills.fetch_add(1, Ordering::Relaxed);
        debug!(
            partition = self.ctx.partition.0,
            loaded = self.win.set.len(),
            horizon = self.win.horizon,
            truncated,
            "timer window refilled"
        );
        Ok(())
    }

    fn needs_refill(&self, now: u64) -> bool {
        let half = self.cfg.window_span_ms / 2;
        // A refill can only load more if the window has room (rows below the horizon are
        // all loaded already).
        self.win.horizon == 0
            || (now.saturating_add(half) >= self.win.horizon && self.win.set.len() < self.win.max)
    }

    /// Submit one batch of `TimerFired`. `Err` = stop the service (partition failing).
    async fn fire(&mut self, due: Vec<Entry>, now: u64) -> Result<(), ()> {
        let cmds = due
            .iter()
            .map(|(d, run, op)| {
                Command::timer_fired(TimerFired {
                    run_id: run.as_bytes().to_vec(),
                    op: *op,
                    due_at_ms: *d,
                    fired_at_ms: now,
                })
            })
            .collect();
        let outcomes = match self.ctx.handle.submit(cmds).await {
            Ok(o) => o,
            Err(e) => {
                if self.ctx.handle.machine().check_healthy().is_err() {
                    error!(partition = self.ctx.partition.0, error = %e, "timer fire failed; partition failing, timer service stops");
                    return Err(());
                }
                warn!(partition = self.ctx.partition.0, error = %e, "timer fire submit failed; retrying");
                tokio::time::sleep(RETRY).await;
                return Ok(());
            }
        };
        for (e, o) in due.iter().zip(outcomes) {
            match o {
                ApplyOutcome::Accepted => {
                    self.stats.fired.fetch_add(1, Ordering::Relaxed);
                    self.m_fired.increment(1);
                }
                ApplyOutcome::Duplicate { .. } => {
                    self.stats.duplicates.fetch_add(1, Ordering::Relaxed);
                }
                ApplyOutcome::Rejected(r) => {
                    // Not expected (TimerFired needs no token). The row, if any, stays in
                    // the table and a later refill retries it.
                    warn!(partition = self.ctx.partition.0, due = e.0, op = e.2, reason = ?r, "TimerFired rejected");
                }
            }
            self.win.remove(e);
        }
        Ok(())
    }

    async fn run(mut self, mut actions: tokio::sync::broadcast::Receiver<Arc<Vec<Action>>>) {
        let mut shutdown = self.ctx.shutdown.clone();
        let clock = Arc::clone(&self.ctx.clock);
        info!(
            partition = self.ctx.partition.0,
            window_max = self.cfg.window_max,
            window_span_ms = self.cfg.window_span_ms,
            "timer service started"
        );
        loop {
            if *shutdown.borrow() {
                break;
            }
            // Drain pending actions.
            loop {
                match actions.try_recv() {
                    Ok(b) => self.on_actions(&b),
                    Err(TryRecvError::Lagged(n)) => {
                        debug!(
                            partition = self.ctx.partition.0,
                            skipped = n,
                            "timer actions lagged; refilling"
                        );
                        self.win.horizon = 0;
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Closed) => {
                        info!(
                            partition = self.ctx.partition.0,
                            "apply machine gone; timer service stops"
                        );
                        return;
                    }
                }
            }

            let now = clock.now_ms();
            if self.needs_refill(now)
                && let Err(e) = self.refill(now)
            {
                if self.ctx.handle.machine().check_healthy().is_err() {
                    error!(partition = self.ctx.partition.0, error = %e, "timer refill failed; partition failing, timer service stops");
                    return;
                }
                warn!(partition = self.ctx.partition.0, error = %e, "timer refill failed; retrying");
                self.win.horizon = 0;
                self.publish();
                tokio::time::sleep(RETRY).await;
                continue;
            }
            self.publish();

            let due = self.win.due(now, self.cfg.fire_batch.max(1));
            if !due.is_empty() {
                if self.fire(due, now).await.is_err() {
                    return;
                }
                self.publish();
                continue;
            }

            let delay = self
                .win
                .earliest()
                .map_or(MAX_SLEEP, |d| Duration::from_millis(d.saturating_sub(now)))
                .min(MAX_SLEEP);
            tokio::select! {
                _ = shutdown.changed() => {}
                r = actions.recv() => match r {
                    Ok(b) => self.on_actions(&b),
                    Err(RecvError::Lagged(_)) => self.win.horizon = 0,
                    Err(RecvError::Closed) => return,
                },
                () = tokio::time::sleep(delay) => {}
            }
        }
        info!(partition = self.ctx.partition.0, "timer service stopped");
    }
}

#[cfg(test)]
mod tests {
    use ostro_types::PartitionId;

    use super::*;

    fn run() -> RunId {
        RunId::new_v7(PartitionId(0))
    }

    #[test]
    fn insert_respects_horizon_and_bound() {
        let r = run();
        let mut w = Window::new(3);
        w.reset(vec![], false, 100);
        w.insert((100, r, 1)); // at horizon: not loaded
        assert!(w.set.is_empty());
        for d in [50, 10, 30, 20] {
            w.insert((d, r, d));
        }
        assert_eq!(w.set.len(), 3);
        assert_eq!(w.horizon, 50);
        assert_eq!(w.due(25, 10), vec![(10, r, 10), (20, r, 20)]);
        w.insert((60, r, 2)); // above the lowered horizon
        assert_eq!(w.set.len(), 3);
    }

    #[test]
    fn truncated_reset_sets_horizon_to_last_due() {
        let r = run();
        let mut w = Window::new(2);
        w.reset(vec![(5, r, 1), (5, r, 2)], true, 1_000);
        assert_eq!(w.horizon, 5);
        assert_eq!(w.set.len(), 2);
        w.reset(vec![(5, r, 3)], false, 1_000);
        assert_eq!(w.horizon, 1_000);
    }
}
