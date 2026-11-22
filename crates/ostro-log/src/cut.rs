//! `HwVector`: the consistent cut of every partition's high watermark (BACK-01, P8-D3, W7,
//! Phase 12 F12a).
//!
//! The WAL has per-partition queues and commit callbacks, so there is no global commit order
//! to snapshot. Instead every HW publication goes through **one function**,
//! [`HwVector::publish_hw`], which:
//!
//! 1. locks the vector, raises `v[p]` to `hw` (monotonic) and releases the lock;
//! 2. only then publishes `hw` on partition `p`'s HW watch (`send_if_modified`).
//!
//! Why the order matters: a task that observes `HW_A >= x` through the watch and then
//! appends to partition B does so strictly after `x` entered the vector. Any cut that
//! contains B's append (`cut[B] >= y`) was taken after B's HW was published, hence after the
//! task observed A, hence after `v[A] >= x`. So `cut[B] >= y` implies `cut[A] >= x`: the cut
//! is causally closed. Publishing first and updating after would open a window in which a
//! cut sees B's append but not the A record it depends on (`tests/hw_cut.rs` shows it).
//!
//! At every replication factor, RF=1 included, the partition log's replica core calls
//! `publish_hw` with the tracker-driven HW ([`PartitionLog`](crate::PartitionLog), 12-08):
//! the WAL commit callback reports the durable end to the tracker, `next_hw` computes the HW. The only other writer of the HW watch is [`HwVector::retract_hw`], used by
//! suffix truncation (the single path that lowers an HW).
//!
//! Cost: one uncontended `std::sync::Mutex` lock per publication (one per group commit and
//! partition) and one per cut.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use ostro_types::{Offset, PartitionId};
use tokio::sync::watch;

/// Per-partition HWs read atomically under one lock, plus the HW watches they feed.
pub struct HwVector {
    /// `v[p]` = the highest HW published for partition `p` (Offset 0 for a partition whose
    /// recovery failed: it has no watch and never publishes).
    cut: Mutex<Vec<Offset>>,
    watches: Box<[Option<Arc<watch::Sender<Offset>>>]>,
    /// Negative control only (`tests/hw_cut.rs`): publish on the watch first, update after.
    publish_first: AtomicBool,
    /// Negative control only: microseconds slept between the two steps, for partition
    /// `publish_first_delay_p` only (widens that partition's window).
    publish_first_delay_us: AtomicU64,
    publish_first_delay_p: AtomicU32,
}

impl std::fmt::Debug for HwVector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HwVector")
            .field("cut", &self.cut())
            .finish_non_exhaustive()
    }
}

impl HwVector {
    /// One slot per partition: `Some(hw)` creates partition `p`'s HW watch starting at `hw`
    /// (with `v[p] = hw`); `None` (recovery failed) leaves the partition without a watch and
    /// its cut entry at 0.
    pub fn new(initial: &[Option<Offset>]) -> Self {
        Self {
            cut: Mutex::new(initial.iter().map(|o| o.unwrap_or(Offset::ZERO)).collect()),
            watches: initial
                .iter()
                .map(|o| o.map(|hw| Arc::new(watch::channel(hw).0)))
                .collect(),
            publish_first: AtomicBool::new(false),
            publish_first_delay_us: AtomicU64::new(0),
            publish_first_delay_p: AtomicU32::new(u32::MAX),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Vec<Offset>> {
        self.cut.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn raise(&self, p: PartitionId, hw: Offset) {
        let mut v = self.lock();
        if let Some(slot) = v.get_mut(p.index())
            && hw > *slot
        {
            *slot = hw;
        }
    }

    /// The single HW publication point: raise `v[p]` to `hw`, release the lock, **then**
    /// publish `hw` on `p`'s HW watch. Monotonic: an `hw` at or below the current value is a
    /// no-op. A partition without a watch (failed recovery) is ignored.
    pub fn publish_hw(&self, p: PartitionId, hw: Offset) {
        let Some(Some(tx)) = self.watches.get(p.index()) else {
            return;
        };
        let publish_first = self.publish_first.load(Ordering::Relaxed);
        if !publish_first {
            // (1) the vector first: anything that can observe `hw` below is already in it.
            self.raise(p, hw);
        }
        // (2) only now the watch.
        tx.send_if_modified(|cur| {
            if hw > *cur {
                *cur = hw;
                true
            } else {
                false
            }
        });
        if publish_first {
            let us = self.publish_first_delay_us.load(Ordering::Relaxed);
            if us > 0 && self.publish_first_delay_p.load(Ordering::Relaxed) == u32::from(p.0) {
                std::thread::sleep(Duration::from_micros(us));
            }
            self.raise(p, hw);
        }
    }

    /// Lower partition `p`'s HW to `new` (suffix truncation below the HW; the caller has
    /// quiesced every in-flight commit). The watch is lowered before the vector, both under
    /// the vector lock, so `watch <= v[p]` holds throughout.
    pub(crate) fn retract_hw(&self, p: PartitionId, new: Offset) {
        let Some(Some(tx)) = self.watches.get(p.index()) else {
            return;
        };
        let mut v = self.lock();
        tx.send_if_modified(|cur| {
            if *cur > new {
                *cur = new;
                true
            } else {
                false
            }
        });
        if let Some(slot) = v.get_mut(p.index())
            && *slot > new
        {
            *slot = new;
        }
    }

    /// The consistent cut: every partition's published HW, cloned under one lock.
    pub fn cut(&self) -> Vec<Offset> {
        self.lock().clone()
    }

    /// Partition `p`'s HW watch (`None` for a partition whose recovery failed).
    pub fn subscribe(&self, p: PartitionId) -> Option<watch::Receiver<Offset>> {
        self.sender(p).map(|tx| tx.subscribe())
    }

    pub(crate) fn sender(&self, p: PartitionId) -> Option<Arc<watch::Sender<Offset>>> {
        self.watches.get(p.index()).and_then(Clone::clone)
    }

    /// **Negative control only** (`tests/hw_cut.rs`): when `Some((p, delay))`, every
    /// partition publishes on the watch first and updates the vector after (the order that
    /// breaks causal closure), and partition `p` sleeps `delay` between the two steps.
    /// `None` restores the correct order. Never call this outside tests.
    #[doc(hidden)]
    pub fn set_publish_first_for_test(&self, delay: Option<(PartitionId, Duration)>) {
        let (p, us) = delay.map_or((u32::MAX, 0), |(p, d)| {
            (
                u32::from(p.0),
                u64::try_from(d.as_micros()).unwrap_or(u64::MAX),
            )
        });
        self.publish_first_delay_p.store(p, Ordering::Relaxed);
        self.publish_first_delay_us.store(us, Ordering::Relaxed);
        self.publish_first.store(delay.is_some(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_is_monotonic_and_feeds_watch() {
        let v = HwVector::new(&[Some(Offset(3)), None, Some(Offset(0))]);
        assert_eq!(v.cut(), vec![Offset(3), Offset(0), Offset(0)]);
        let rx = v.subscribe(PartitionId(0)).unwrap();
        v.publish_hw(PartitionId(0), Offset(10));
        v.publish_hw(PartitionId(0), Offset(7));
        assert_eq!(*rx.borrow(), Offset(10));
        v.publish_hw(PartitionId(1), Offset(99)); // no watch: ignored
        v.publish_hw(PartitionId(2), Offset(4));
        assert_eq!(v.cut(), vec![Offset(10), Offset(0), Offset(4)]);
        assert!(v.subscribe(PartitionId(1)).is_none());
        v.retract_hw(PartitionId(0), Offset(5));
        assert_eq!(*rx.borrow(), Offset(5));
        assert_eq!(v.cut()[0], Offset(5));
    }
}
