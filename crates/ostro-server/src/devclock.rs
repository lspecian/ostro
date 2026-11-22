//! The dev clock (SDK-06, D-P10-6, plan 10-10): time skipping for `ostro dev --ephemeral
//! --time-skipping`.
//!
//! [`DevClock`] is `base + offset_ms`: `base` is the clock the server would otherwise use
//! (the system clock, or a test's clock) and `offset_ms` only grows. It implements both
//! clock traits from that one source:
//!
//! - [`ostro_partition::clock::Clock`] (ms): the Phase 8 seam clock handed to
//!   `InitCluster`, `NodeConfig.clock` (timers, dispatcher, cron), the services and the
//!   Phase 8 controllers;
//! - [`ostro_engine::env::Clock`] (ns): `RuntimeOptions.clock`, which stamps journal
//!   events and the control plane's timestamps (`now_ms * 1_000_000`: the dev clock has
//!   ms resolution, so both traits always agree exactly; Pitfall 9).
//!
//! A skip notifies [`subscribe_changes`](ostro_partition::clock::Clock::subscribe_changes)
//! receivers (the dispatcher wakes and re-evaluates delayed `ready_at`s; the timer service
//! polls the clock at most every 50 ms anyway).
//!
//! **Skip barrier (W1).** [`DevClock::skip_guarded`] takes the barrier's write guard, runs
//! `check(now)` under it and skips to the returned target only if there is one; the
//! dispatcher holds the read guard while it grants an activation, so no grant interleaves a
//! skip. The guard is never held across anything that takes the barrier again.
//!
//! The state is process memory only (D-P10-17): no keyspace, meta or journal field. With
//! time skipping off no `DevClock` exists and the server runs exactly the old clock path.
//!
//! Metrics (only registered when a `DevClock` exists): `ostro_dev_time_offset_ms` (gauge,
//! set at construction and after every skip) and `ostro_dev_time_skips_total{mode}`
//! (counter, non-zero skips).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use ostro_api::domain::DevTimeMode;
use tokio::sync::{RwLock, watch};

/// `base + offset_ms` with an offset that only grows (module docs).
pub struct DevClock {
    base: Arc<dyn ostro_partition::clock::Clock>,
    offset_ms: AtomicU64,
    mode: DevTimeMode,
    tx: watch::Sender<u64>,
    barrier: Arc<RwLock<()>>,
    /// The idle detector (10-13), attached once by the server; weak (the server and the
    /// auto loop hold it).
    time: OnceLock<Weak<crate::devtime::DevTime>>,
    /// Test hook (10-13): called with `(now_ms, target_ms)` under the write guard at the
    /// skip instant, right before the offset moves.
    skip_hook: Mutex<Option<SkipHook>>,
}

/// See [`DevClock::set_skip_hook`].
pub type SkipHook = Arc<dyn Fn(u64, u64) + Send + Sync>;

impl std::fmt::Debug for DevClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevClock")
            .field("offset_ms", &self.offset_ms())
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

impl DevClock {
    /// A dev clock over `base` with a zero offset.
    pub fn new(base: Arc<dyn ostro_partition::clock::Clock>, mode: DevTimeMode) -> Arc<Self> {
        let now = base.now_ms();
        let c = Arc::new(Self {
            base,
            offset_ms: AtomicU64::new(0),
            mode,
            tx: watch::channel(now).0,
            barrier: Arc::new(RwLock::new(())),
            time: OnceLock::new(),
            skip_hook: Mutex::new(None),
        });
        metrics::gauge!("ostro_dev_time_offset_ms").set(0.0);
        c
    }

    /// Total skipped so far, ms.
    pub fn offset_ms(&self) -> u64 {
        self.offset_ms.load(Ordering::SeqCst)
    }

    /// The configured mode (`auto` or `manual`).
    pub fn mode(&self) -> DevTimeMode {
        self.mode
    }

    /// `(now_ms, offset_ms, mode)` from one offset read.
    pub fn info(&self) -> (u64, u64, DevTimeMode) {
        let off = self.offset_ms();
        (self.base.now_ms().saturating_add(off), off, self.mode)
    }

    /// Advance by `ms`; returns the ms skipped (`ms`, or 0 for `ms == 0`).
    pub fn skip_by(&self, ms: u64) -> u64 {
        if ms == 0 {
            return 0;
        }
        let prev = self
            .offset_ms
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |o| {
                Some(o.saturating_add(ms))
            })
            .unwrap_or_else(|o| o);
        let skipped = prev.saturating_add(ms) - prev;
        self.skipped(skipped);
        skipped
    }

    /// Advance to `target_ms`; returns the ms skipped, 0 when `target_ms` is not in the
    /// future (time never moves backwards).
    pub fn skip_to(&self, target_ms: u64) -> u64 {
        let mut skipped = 0;
        let _ = self
            .offset_ms
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |o| {
                let now = self.base.now_ms().saturating_add(o);
                if target_ms <= now {
                    skipped = 0;
                    None
                } else {
                    skipped = target_ms - now;
                    Some(o.saturating_add(skipped))
                }
            });
        self.skipped(skipped);
        skipped
    }

    /// Under the barrier's write guard (no grant in progress): run `check(now_ms)` and
    /// skip to its target, if any. Returns the ms skipped (0 when `check` returns `None` or
    /// a target that is not in the future).
    pub async fn skip_guarded<F: FnOnce(u64) -> Option<u64>>(&self, check: F) -> u64 {
        let _w = self.barrier.write().await;
        let now = ostro_partition::clock::Clock::now_ms(self);
        match check(now) {
            Some(target) => {
                if target > now {
                    let hook = self
                        .skip_hook
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clone();
                    if let Some(h) = hook {
                        h(now, target);
                    }
                }
                self.skip_to(target)
            }
            None => 0,
        }
    }

    /// Attach the idle detector (10-13; the first attachment wins).
    pub fn attach_time(&self, t: &Arc<crate::devtime::DevTime>) {
        let _ = self.time.set(Arc::downgrade(t));
    }

    /// The idle detector, when the server attached one and it is still alive.
    pub fn time(&self) -> Option<Arc<crate::devtime::DevTime>> {
        self.time.get().and_then(Weak::upgrade)
    }

    /// Test hook: `f(now_ms, target_ms)` runs for every guarded skip that moves the clock,
    /// under the barrier's write guard, right before the offset moves (`None` clears it).
    /// The race suite asserts zero leases and grants there (D-W26).
    #[doc(hidden)]
    pub fn set_skip_hook(&self, f: Option<SkipHook>) {
        *self
            .skip_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = f;
    }

    /// The skip barrier (the dispatcher's grant path takes read guards).
    pub fn barrier(&self) -> Arc<RwLock<()>> {
        Arc::clone(&self.barrier)
    }

    /// Notify subscribers and record metrics after a skip of `skipped` ms.
    fn skipped(&self, skipped: u64) {
        if skipped == 0 {
            return;
        }
        let now = ostro_partition::clock::Clock::now_ms(self);
        self.tx.send_replace(now);
        metrics::gauge!("ostro_dev_time_offset_ms").set(self.offset_ms() as f64);
        metrics::counter!("ostro_dev_time_skips_total", "mode" => self.mode.as_str()).increment(1);
    }
}

impl ostro_partition::clock::Clock for DevClock {
    fn now_ms(&self) -> u64 {
        self.base.now_ms().saturating_add(self.offset_ms())
    }

    fn subscribe_changes(&self) -> Option<watch::Receiver<u64>> {
        Some(self.tx.subscribe())
    }

    fn skip_barrier(&self) -> Option<Arc<RwLock<()>>> {
        Some(self.barrier())
    }
}

impl ostro_engine::env::Clock for DevClock {
    /// `now_ms * 1_000_000`: ms resolution, so the engine's journal timestamps and the
    /// partition clock agree exactly (no sub-ms part from the base clock).
    fn now_unix_nanos(&self) -> u64 {
        ostro_partition::clock::Clock::now_ms(self).saturating_mul(1_000_000)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use ostro_engine::env::Clock as _;
    use ostro_partition::clock::{Clock as _, ManualClock};

    use super::*;

    fn dev(start: u64) -> Arc<DevClock> {
        DevClock::new(Arc::new(ManualClock::new(start)), DevTimeMode::Manual)
    }

    #[test]
    fn devclock_skips_only_forward_and_both_traits_agree() {
        let c = dev(1_000);
        assert_eq!(c.now_ms(), 1_000);
        assert_eq!(c.now_unix_nanos(), 1_000 * 1_000_000);
        assert_eq!(c.skip_by(500), 500);
        assert_eq!(c.now_ms(), 1_500);
        assert_eq!(c.now_unix_nanos(), 1_500 * 1_000_000);
        assert_eq!(c.skip_to(1_200), 0);
        assert_eq!(c.now_ms(), 1_500);
        assert_eq!(c.skip_to(10_000), 8_500);
        assert_eq!(c.now_ms(), 10_000);
        assert_eq!(c.now_unix_nanos(), 10_000 * 1_000_000);
        assert_eq!(c.offset_ms(), 9_000);
        assert_eq!(c.info(), (10_000, 9_000, DevTimeMode::Manual));
        assert_eq!(c.skip_by(0), 0);
        assert_eq!(c.mode(), DevTimeMode::Manual);
    }

    #[test]
    fn devclock_follows_its_base() {
        let base = ManualClock::new(100);
        let c = DevClock::new(Arc::new(base.clone()), DevTimeMode::Auto);
        c.skip_by(50);
        base.advance(10);
        assert_eq!(c.now_ms(), 160);
        assert_eq!(c.mode(), DevTimeMode::Auto);
    }

    #[test]
    fn devclock_notifies_on_nonzero_skips_only() {
        let c = dev(1_000);
        let mut rx = c.subscribe_changes().expect("dev clock has subscribers");
        assert!(c.skip_barrier().is_some());
        c.skip_by(10);
        assert!(rx.has_changed().expect("open"));
        assert_eq!(*rx.borrow_and_update(), 1_010);
        c.skip_to(5);
        c.skip_by(0);
        assert!(!rx.has_changed().expect("open"), "a zero skip notified");
        c.skip_to(2_000);
        assert!(rx.has_changed().expect("open"));
        assert_eq!(*rx.borrow_and_update(), 2_000);
    }

    #[tokio::test]
    async fn devclock_skip_guarded_waits_for_readers_and_rechecks() {
        let c = dev(1_000);
        let read = c.barrier().read_owned().await;
        let c2 = Arc::clone(&c);
        let task = tokio::spawn(async move { c2.skip_guarded(|now| Some(now + 100)).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !task.is_finished(),
            "skip ran while a grant held the read guard"
        );
        assert_eq!(c.now_ms(), 1_000);
        drop(read);
        assert_eq!(task.await.expect("join"), 100);
        assert_eq!(c.now_ms(), 1_100);
        // A check that finds work: no skip.
        assert_eq!(c.skip_guarded(|_| None).await, 0);
        assert_eq!(c.now_ms(), 1_100);
    }

    #[tokio::test]
    async fn devclock_check_sees_a_grant_counted_under_the_read_guard() {
        // The dispatcher's protocol: read guard -> count the grant -> append -> drop the
        // guard (the count stays until the grant task ends). A skip that acquires the
        // write guard after the grant therefore sees the count.
        let c = dev(1_000);
        let granting = Arc::new(AtomicU64::new(0));
        let g = Arc::clone(&granting);
        let barrier = c.barrier();
        let grant = tokio::spawn(async move {
            let guard = barrier.read_owned().await;
            g.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await; // the append
            drop(guard);
        });
        tokio::time::sleep(Duration::from_millis(5)).await;
        let seen = Arc::clone(&granting);
        let skipped = c
            .skip_guarded(|now| (seen.load(Ordering::SeqCst) == 0).then_some(now + 1_000))
            .await;
        grant.await.expect("join");
        assert_eq!(skipped, 0, "skipped over an in-flight grant");
        assert_eq!(c.now_ms(), 1_000);
    }
}
