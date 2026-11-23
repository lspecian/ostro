//! `PendingAcks`: the rendezvous between submitters and the apply thread (STOR-11, research
//! Pattern 8).
//!
//! A submitter learns its offsets from the append receipt and then registers; the apply
//! thread resolves outcomes after `ApplyBatch::commit`. Either side may arrive first (the
//! fsync and apply can finish before the submitter registers), so resolved outcomes that
//! nobody waits for are kept in `ready`. Retention is bounded: entries older than
//! `applied - ACK_RETAIN` are evicted, and registering an evicted (or already taken) offset
//! yields [`PartitionError::AckLost`]: outcome unknown, never `Ok`.

use std::collections::BTreeMap;
use std::sync::Mutex;

use ostro_types::Offset;
use tokio::sync::oneshot;

use crate::apply::ApplyOutcome;
use crate::handle::PartitionError;

/// Resolved-but-unclaimed outcomes are kept for this many offsets behind `applied`.
pub const ACK_RETAIN: u64 = 65_536;

/// Receiver of one command's outcome.
pub type AckReceiver = oneshot::Receiver<Result<ApplyOutcome, PartitionError>>;
type AckSender = oneshot::Sender<Result<ApplyOutcome, PartitionError>>;

#[derive(Default)]
struct Inner {
    waiters: BTreeMap<Offset, AckSender>,
    ready: BTreeMap<Offset, ApplyOutcome>,
    /// Next offset to apply (everything below was resolved).
    applied: Offset,
    failed: Option<String>,
    /// Not leading (12-13): registrations answer `NotLeader`.
    refusing: bool,
}

/// Two-sided ack rendezvous. The mutex is a `std::sync::Mutex` and is never held across an
/// `.await`.
#[derive(Default)]
pub struct PendingAcks {
    inner: Mutex<Inner>,
}

impl std::fmt::Debug for PendingAcks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingAcks").finish_non_exhaustive()
    }
}

impl PendingAcks {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        // A panic while holding the lock cannot leave the maps inconsistent in a way that
        // could produce a wrong Ok; recover the guard.
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// One receiver per offset in `[base, last]`, in order.
    pub fn register(&self, base: Offset, last: Offset) -> Vec<AckReceiver> {
        let mut g = self.lock();
        let mut out =
            Vec::with_capacity(usize::try_from(last.0.saturating_sub(base.0) + 1).unwrap_or(0));
        let mut off = base;
        while off <= last {
            let (tx, rx) = oneshot::channel();
            if let Some(reason) = &g.failed {
                let _ = tx.send(Err(PartitionError::Failed(reason.clone())));
            } else if g.refusing {
                let _ = tx.send(Err(PartitionError::NotLeader));
            } else if off < g.applied {
                let r = match g.ready.remove(&off) {
                    Some(o) => Ok(o),
                    None => Err(PartitionError::AckLost(off)),
                };
                let _ = tx.send(r);
            } else {
                g.waiters.insert(off, tx);
            }
            out.push(rx);
            off = off.next();
        }
        out
    }

    /// Called by the apply thread **after** the chunk committed; `applied` is the new
    /// `cursor.next`.
    pub fn resolve(&self, outcomes: &[(Offset, ApplyOutcome)], applied: Offset) {
        let mut g = self.lock();
        if g.failed.is_some() {
            return;
        }
        for (off, o) in outcomes {
            match g.waiters.remove(off) {
                Some(tx) => {
                    let _ = tx.send(Ok(o.clone()));
                }
                None => {
                    g.ready.insert(*off, o.clone());
                }
            }
        }
        if applied > g.applied {
            g.applied = applied;
        }
        let floor = Offset(g.applied.0.saturating_sub(ACK_RETAIN));
        g.ready = g.ready.split_off(&floor);
        // Waiters below `applied` that were not resolved (cannot happen while apply covers
        // every offset; defensive): outcome unknown.
        let applied = g.applied;
        let still = g.waiters.split_off(&applied);
        for (off, tx) in std::mem::replace(&mut g.waiters, still) {
            let _ = tx.send(Err(PartitionError::AckLost(off)));
        }
    }

    /// Demotion (12-13): fail every current waiter with [`PartitionError::NotLeader`] and
    /// answer every later registration the same way until [`accept`](Self::accept), without
    /// failing the partition (the leader hint is
    /// [`InProcessRouter::leader_hint`](crate::router::InProcessRouter::leader_hint)). A submit
    /// that appended before the demotion therefore never waits on an offset a new leader may
    /// fill with another command. Outcomes the apply thread resolves meanwhile are kept in
    /// `ready` like any unclaimed outcome.
    pub fn refuse_not_leader(&self) {
        let mut g = self.lock();
        g.refusing = true;
        for (_, tx) in std::mem::take(&mut g.waiters) {
            let _ = tx.send(Err(PartitionError::NotLeader));
        }
    }

    /// Takeover (12-13): registrations wait for their outcomes again.
    pub fn accept(&self) {
        self.lock().refusing = false;
    }

    /// Fail every current and future waiter (the partition is failed).
    pub fn fail_all(&self, reason: &str) {
        let mut g = self.lock();
        if g.failed.is_none() {
            g.failed = Some(reason.to_string());
        }
        g.ready.clear();
        for (_, tx) in std::mem::take(&mut g.waiters) {
            let _ = tx.send(Err(PartitionError::Failed(reason.to_string())));
        }
    }
}
