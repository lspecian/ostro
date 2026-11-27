//! Wall clock for leader services (timestamps on leader-produced commands).
//!
//! Apply never reads a clock (timestamps come from commands); only node startup and the
//! leader services do, through this trait so tests can drive time with [`ManualClock`].

use std::sync::Arc;

use tokio::sync::watch;

/// Milliseconds since the Unix epoch.
pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;

    /// Dev time skipping (Phase 10 D-P10-6): a receiver notified whenever the clock jumps
    /// forward, so leader services that turned a clock deadline into a real-time sleep
    /// re-evaluate it. Production clocks return `None` (the default).
    fn subscribe_changes(&self) -> Option<watch::Receiver<u64>> {
        None
    }

    /// Dev time skipping (Phase 10 D-P10-6, W1): the skip barrier. The dispatcher holds a
    /// **read** guard while it grants an activation; a skip takes the **write** guard, so
    /// no grant interleaves with a skip. Production clocks return `None` (the default).
    fn skip_barrier(&self) -> Option<Arc<tokio::sync::RwLock<()>>> {
        None
    }

    /// Failpoints builds only (14-10, D-P14-20): the skew controls when this clock is a
    /// [`SkewClock`]; every other clock returns `None` (the default).
    #[cfg(feature = "failpoints")]
    fn skew(&self) -> Option<SkewClock> {
        None
    }
}

/// The system wall clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    // The one production wall-clock read of the partition layer (leader side, never apply).
    #[allow(clippy::disallowed_methods)]
    fn now_ms(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
    }
}

/// A test clock that moves only when told to. Clones share the time.
#[derive(Clone, Debug)]
pub struct ManualClock {
    tx: Arc<watch::Sender<u64>>,
}

impl ManualClock {
    pub fn new(start_ms: u64) -> Self {
        Self {
            tx: Arc::new(watch::channel(start_ms).0),
        }
    }

    pub fn set(&self, ms: u64) {
        self.tx.send_replace(ms);
    }

    pub fn advance(&self, ms: u64) {
        self.tx.send_modify(|t| *t = t.saturating_add(ms));
    }

    /// Notified on every `set`/`advance`.
    pub fn changed(&self) -> watch::Receiver<u64> {
        self.tx.subscribe()
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        *self.tx.borrow()
    }
}

#[cfg(feature = "failpoints")]
pub use skew::{MAX_RATE_PPM, MonoSource, SkewClock, SkewSettings, SkewSnapshot};

/// Failpoints builds only (14-10, D-P14-20/34): a skewable clock over the injected one, for
/// the Jepsen clock nemesis (static musl binaries cannot use libfaketime).
///
/// - **Wall** = inner wall + `offset_ms` + strobe (a triangle wave of amplitude `strobe_ms`
///   and period `strobe_period_ms`, 0 at its origin) + the drift accumulated since the last
///   reset. Offset changes apply as given (bumps are the point).
/// - **Monotonic** = `anchor_skewed + (inner_mono - anchor_inner) * (1 + rate)`. Every rate
///   change **re-anchors** (`anchor_skewed` = the current skewed value, `anchor_inner` = now),
///   so it never jumps; **reset** sets the rate (and offset, strobe) to 0 and re-anchors,
///   keeping the accumulated monotonic offset (it never jumps back to the inner value).
///   Wall offset changes never touch it. The rate is bounded by [`MAX_RATE_PPM`] (> -100%),
///   so the skewed monotonic clock is non-decreasing across any sequence of changes.
///
/// With nothing set, [`Clock::now_ms`] returns the inner value unchanged (byte-identical).
/// It moves the node's injected clock only: kernel / OS timers (tokio sleeps, TCP
/// keepalive) and the D-P13-15 lease (`Instant` based, 14-01) are not skewed.
#[cfg(feature = "failpoints")]
mod skew {
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::Duration;

    use tokio::sync::watch;

    use super::Clock;

    /// Largest |rate| in parts per million (±50%).
    pub const MAX_RATE_PPM: i64 = 500_000;
    const PPM: i128 = 1_000_000;
    const NS_PER_MS: i128 = 1_000_000;

    /// The inner monotonic source in nanoseconds (non-decreasing).
    pub type MonoSource = Arc<dyn Fn() -> u64 + Send + Sync>;

    /// A change ([`SkewClock::apply`]): `reset` first, then every field present.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct SkewSettings {
        /// Back to no skew (keeps the accumulated monotonic offset).
        pub reset: bool,
        /// Wall offset (ms), set as given.
        pub offset_ms: Option<i64>,
        /// Strobe amplitude (ms); 0 = off.
        pub strobe_ms: Option<u64>,
        /// Strobe period (ms); must be > 0 while the strobe is on.
        pub strobe_period_ms: Option<u64>,
        /// Monotonic rate drift (ppm; 50_000 = 5% fast, -50_000 = 5% slow).
        pub mono_rate_ppm: Option<i64>,
    }

    /// The state and both clocks at one instant.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct SkewSnapshot {
        /// Wall offset (ms).
        pub offset_ms: i64,
        /// Strobe amplitude (ms).
        pub strobe_ms: u64,
        /// Strobe period (ms).
        pub strobe_period_ms: u64,
        /// Monotonic rate (ppm).
        pub mono_rate_ppm: i64,
        /// Skewed wall clock (ms since the Unix epoch).
        pub wall_ms: u64,
        /// Inner wall clock (ms since the Unix epoch).
        pub inner_wall_ms: u64,
        /// Skewed monotonic clock (ns).
        pub mono_ns: u64,
        /// Inner monotonic clock (ns).
        pub inner_mono_ns: u64,
    }

    #[derive(Debug, Default)]
    struct State {
        offset_ms: i64,
        strobe_ms: u64,
        strobe_period_ms: u64,
        strobe_origin_ns: u64,
        rate_ppm: i64,
        anchor_inner_ns: u64,
        anchor_skewed_ns: u64,
        /// `skewed - inner` monotonic (ns) at the last reset: the wall clock carries only the
        /// drift accumulated since then.
        wall_drift_zero_ns: i128,
    }

    impl State {
        fn mono(&self, inner_ns: u64) -> u64 {
            let d = i128::from(inner_ns.saturating_sub(self.anchor_inner_ns));
            let scaled = d * (PPM + i128::from(self.rate_ppm)) / PPM;
            let v = i128::from(self.anchor_skewed_ns) + scaled.max(0);
            u64::try_from(v).unwrap_or(u64::MAX)
        }

        fn reanchor(&mut self, inner_ns: u64) {
            let skewed = self.mono(inner_ns);
            self.anchor_skewed_ns = skewed;
            self.anchor_inner_ns = inner_ns;
        }

        fn strobe(&self, inner_ns: u64) -> i128 {
            if self.strobe_ms == 0 || self.strobe_period_ms == 0 {
                return 0;
            }
            let period = i128::from(self.strobe_period_ms) * NS_PER_MS;
            let x = i128::from(inner_ns.saturating_sub(self.strobe_origin_ns)) % period;
            let amp = i128::from(self.strobe_ms);
            // Triangle wave: 0 -> +amp (quarter) -> -amp (three quarters) -> 0.
            let q = period / 4;
            if q == 0 {
                return 0;
            }
            if x < q {
                amp * x / q
            } else if x < 3 * q {
                amp - amp * (x - q) / q
            } else {
                -amp + amp * (x - 3 * q) / q
            }
        }

        fn is_identity(&self, inner_ns: u64) -> bool {
            self.offset_ms == 0
                && self.strobe_ms == 0
                && self.rate_ppm == 0
                && i128::from(self.mono(inner_ns)) - i128::from(inner_ns) == self.wall_drift_zero_ns
        }

        fn wall(&self, inner_wall: u64, inner_ns: u64) -> u64 {
            if self.is_identity(inner_ns) {
                return inner_wall;
            }
            let drift_ns =
                i128::from(self.mono(inner_ns)) - i128::from(inner_ns) - self.wall_drift_zero_ns;
            let total = i128::from(inner_wall)
                + i128::from(self.offset_ms)
                + self.strobe(inner_ns)
                + drift_ns / NS_PER_MS;
            u64::try_from(total.max(0)).unwrap_or(u64::MAX)
        }
    }

    /// The skewable clock (module docs above). Clones share the state.
    #[derive(Clone)]
    pub struct SkewClock {
        inner: Arc<dyn Clock>,
        mono: MonoSource,
        state: Arc<Mutex<State>>,
    }

    impl std::fmt::Debug for SkewClock {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("SkewClock").finish_non_exhaustive()
        }
    }

    impl SkewClock {
        /// Over `inner`, with the process monotonic clock as the inner monotonic source.
        pub fn new(inner: Arc<dyn Clock>) -> Self {
            #[allow(clippy::disallowed_methods)]
            let origin = std::time::Instant::now(); // clock-exempt: skew clock wraps the real source
            let mono: MonoSource =
                Arc::new(move || u64::try_from(origin.elapsed().as_nanos()).unwrap_or(u64::MAX));
            Self::with_monotonic(inner, mono)
        }

        /// Over `inner` with an explicit inner monotonic source (tests).
        pub fn with_monotonic(inner: Arc<dyn Clock>, mono: MonoSource) -> Self {
            let now = mono();
            let state = State {
                anchor_inner_ns: now,
                anchor_skewed_ns: now,
                strobe_origin_ns: now,
                ..State::default()
            };
            Self {
                inner,
                mono,
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn lock(&self) -> std::sync::MutexGuard<'_, State> {
            self.state.lock().unwrap_or_else(PoisonError::into_inner)
        }

        /// Apply `s` atomically (module docs); an invalid change applies nothing.
        pub fn apply(&self, s: &SkewSettings) -> Result<SkewSnapshot, String> {
            if let Some(r) = s.mono_rate_ppm
                && !(-MAX_RATE_PPM..=MAX_RATE_PPM).contains(&r)
            {
                return Err(format!(
                    "mono_rate out of range: {r} ppm (allowed ±{MAX_RATE_PPM} ppm)"
                ));
            }
            let now = (self.mono)();
            let mut st = self.lock();
            let strobe_ms = s
                .strobe_ms
                .unwrap_or(if s.reset { 0 } else { st.strobe_ms });
            let period =
                s.strobe_period_ms
                    .unwrap_or(if s.reset { 0 } else { st.strobe_period_ms });
            if strobe_ms > 0 && period == 0 {
                return Err("strobe_period_ms must be > 0 while strobe_ms > 0".into());
            }
            if s.reset {
                st.reanchor(now);
                st.rate_ppm = 0;
                st.offset_ms = 0;
                st.strobe_ms = 0;
                st.strobe_period_ms = 0;
                st.wall_drift_zero_ns = i128::from(st.anchor_skewed_ns) - i128::from(now);
            }
            if let Some(o) = s.offset_ms {
                st.offset_ms = o;
            }
            if s.strobe_ms.is_some() || s.strobe_period_ms.is_some() {
                st.strobe_ms = strobe_ms;
                st.strobe_period_ms = period;
                st.strobe_origin_ns = now;
            }
            if let Some(r) = s.mono_rate_ppm
                && r != st.rate_ppm
            {
                st.reanchor(now);
                st.rate_ppm = r;
            }
            Ok(self.snapshot_locked(&st, now))
        }

        /// Shift the wall offset by `delta_ms`.
        pub fn bump_ms(&self, delta_ms: i64) -> SkewSnapshot {
            let now = (self.mono)();
            let mut st = self.lock();
            st.offset_ms = st.offset_ms.saturating_add(delta_ms);
            self.snapshot_locked(&st, now)
        }

        /// Set the monotonic rate (ppm, clamped to ±[`MAX_RATE_PPM`]); re-anchors.
        pub fn set_rate_ppm(&self, ppm: i64) -> SkewSnapshot {
            let now = (self.mono)();
            let mut st = self.lock();
            st.reanchor(now);
            st.rate_ppm = ppm.clamp(-MAX_RATE_PPM, MAX_RATE_PPM);
            self.snapshot_locked(&st, now)
        }

        /// No skew from now on (keeps the accumulated monotonic offset).
        pub fn reset(&self) -> SkewSnapshot {
            self.apply(&SkewSettings {
                reset: true,
                ..SkewSettings::default()
            })
            .unwrap_or_else(|_| self.snapshot())
        }

        /// The skewed monotonic clock (since the inner source's origin).
        pub fn monotonic(&self) -> Duration {
            let now = (self.mono)();
            Duration::from_nanos(self.lock().mono(now))
        }

        /// The current state and clocks.
        pub fn snapshot(&self) -> SkewSnapshot {
            let now = (self.mono)();
            let st = self.lock();
            self.snapshot_locked(&st, now)
        }

        fn snapshot_locked(&self, st: &State, now: u64) -> SkewSnapshot {
            let inner_wall = self.inner.now_ms();
            SkewSnapshot {
                offset_ms: st.offset_ms,
                strobe_ms: st.strobe_ms,
                strobe_period_ms: st.strobe_period_ms,
                mono_rate_ppm: st.rate_ppm,
                wall_ms: st.wall(inner_wall, now),
                inner_wall_ms: inner_wall,
                mono_ns: st.mono(now),
                inner_mono_ns: now,
            }
        }
    }

    impl Clock for SkewClock {
        fn now_ms(&self) -> u64 {
            let inner_wall = self.inner.now_ms();
            let now = (self.mono)();
            self.lock().wall(inner_wall, now)
        }

        fn subscribe_changes(&self) -> Option<watch::Receiver<u64>> {
            self.inner.subscribe_changes()
        }

        fn skip_barrier(&self) -> Option<Arc<tokio::sync::RwLock<()>>> {
            self.inner.skip_barrier()
        }

        fn skew(&self) -> Option<SkewClock> {
            Some(self.clone())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manual_clock_moves_only_when_told() {
        let c = ManualClock::new(5);
        let rx = c.changed();
        assert_eq!(c.now_ms(), 5);
        c.advance(10);
        assert_eq!(c.clone().now_ms(), 15);
        c.set(3);
        assert_eq!(*rx.borrow(), 3);
        assert!(SystemClock.now_ms() > 1_600_000_000_000);
    }

    #[test]
    fn production_and_manual_clocks_have_no_skip_hooks() {
        assert!(SystemClock.subscribe_changes().is_none());
        assert!(SystemClock.skip_barrier().is_none());
        let c = ManualClock::new(1);
        assert!(c.subscribe_changes().is_none());
        assert!(c.skip_barrier().is_none());
    }
}
