//! Wall clock for leader services (timestamps on leader-produced commands).
//!
//! Apply never reads a clock (timestamps come from commands); only node startup and the
//! leader services do, through this trait so tests can drive time with [`ManualClock`].

use std::sync::Arc;

use tokio::sync::watch;

/// Milliseconds since the Unix epoch.
pub trait Clock: Send + Sync + 'static {
    fn now_ms(&self) -> u64;
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
}
