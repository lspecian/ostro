//! Readiness barrier and trim permits (STOR-08, research Pattern 12 / Pitfall 8).
//!
//! Trimming the log prefix is the one irreversible log operation. Chronik once deleted live
//! WAL segments 2 ms after recovery because retention ran before the node was ready. Here
//! [`ReplicatedLog::trim_prefix`](crate::ReplicatedLog::trim_prefix) requires a [`TrimPermit`],
//! and the only way to get one is [`ReadyBarrier::mint`], which refuses until the node has
//! opened the barrier (recovery done, state caught up, checkpoint taken).

use ostro_types::Offset;
use tokio::sync::watch;

use crate::error::LogError;

/// Opened once the node is ready; mints [`TrimPermit`]s afterwards. Cheap to clone (shared).
#[derive(Clone, Debug)]
pub struct ReadyBarrier {
    tx: std::sync::Arc<watch::Sender<bool>>,
}

impl Default for ReadyBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl ReadyBarrier {
    /// A closed barrier.
    pub fn new() -> Self {
        Self {
            tx: std::sync::Arc::new(watch::channel(false).0),
        }
    }

    /// Open the barrier (idempotent; it never closes again).
    pub fn open(&self) {
        self.tx.send_replace(true);
    }

    /// Whether the barrier is open.
    pub fn is_open(&self) -> bool {
        *self.tx.borrow()
    }

    /// Resolve once the barrier is open.
    pub async fn wait_open(&self) {
        let mut rx = self.tx.subscribe();
        // The sender lives in `self`, so the channel cannot close while we wait.
        let _ = rx.wait_for(|open| *open).await;
    }

    /// Mint a permit to trim below `floor`. `Err(NotReady)` until [`open`](Self::open).
    pub fn mint(&self, floor: Offset) -> Result<TrimPermit, LogError> {
        if self.is_open() {
            Ok(TrimPermit { floor })
        } else {
            Err(LogError::NotReady)
        }
    }
}

/// Permission to delete whole log segments entirely below [`floor`](Self::floor).
///
/// There is no public constructor; only [`ReadyBarrier::mint`] creates one:
///
/// ```compile_fail
/// use ostro_log::TrimPermit;
/// use ostro_types::Offset;
/// let p = TrimPermit { floor: Offset(10) };
/// ```
#[derive(Debug)]
pub struct TrimPermit {
    floor: Offset,
}

impl TrimPermit {
    /// Offsets strictly below the floor may be deleted (whole segments only).
    pub fn floor(&self) -> Offset {
        self.floor
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mint_requires_open() {
        let b = ReadyBarrier::new();
        assert!(matches!(b.mint(Offset(5)), Err(LogError::NotReady)));
        let b2 = b.clone();
        b2.open();
        assert!(b.is_open());
        assert_eq!(b.mint(Offset(5)).unwrap().floor(), Offset(5));
    }

    #[tokio::test]
    async fn wait_open_resolves() {
        let b = ReadyBarrier::new();
        let b2 = b.clone();
        let h = tokio::spawn(async move { b2.wait_open().await });
        b.open();
        h.await.unwrap();
    }
}
