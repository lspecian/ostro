//! Placement contract (Phase 7, plan 07-02): where a ready run executes.
//!
//! The dispatcher asks [`Placement::reserve`] for capacity before it grants an attempt
//! (Pitfall 2: never grant before admission). [`SemaphorePlacement`] is the 04-14
//! behaviour, a node-wide slot semaphore shared by every partition; Phase 7's
//! `NodePlacement` (ostro-server `workers/placement.rs`, 07-12) reserves embedded slots or
//! worker credit. A [`Reservation`] is RAII: dropping it gives the capacity back and bumps
//! [`Placement::changed`] so blocked dispatchers retry.
//!
//! The dispatcher is not wired to this trait yet (07-05 does that); the contract is fixed
//! here so the seam and later waves compile against it.

use std::any::Any;
use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::query::ReadyEntryRow;

/// What placement and flow control see of a ready entry (borrowed from the ready row).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryView<'a> {
    pub queue: &'a str,
    pub workflow: &'a str,
    pub tenant: Option<&'a str>,
    pub concurrency_key: &'a str,
    pub priority: i32,
    /// The next activation is the run's first (no attempt was ever granted). Comes from
    /// F7: `ReadyEntryRow.first_activation` / `Action::EnqueueReady.first_activation`, set
    /// by apply's `enqueue_ready` (from `rec.current_attempt.is_none()`; see 07-01). The
    /// dispatcher never guesses it. Throttles apply to first activations only (D-W13).
    pub first_activation: bool,
}

impl<'a> EntryView<'a> {
    /// View of a ready-queue row.
    pub fn from_row(row: &'a ReadyEntryRow) -> Self {
        Self {
            queue: &row.queue,
            workflow: &row.workflow,
            tenant: row.tenant.as_deref(),
            concurrency_key: &row.concurrency_key,
            priority: row.priority,
            first_activation: row.first_activation,
        }
    }
}

/// Where a reservation executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ReservationKind {
    /// On this node's engine (an executor slot).
    Embedded,
    /// On a remote worker (session credit).
    Worker,
}

/// Capacity held for one job; released on drop. `inner` is the placement's own guard
/// (a semaphore permit, a worker credit) and travels with the job to the executor.
pub struct Reservation {
    kind: ReservationKind,
    inner: Box<dyn Any + Send + Sync>,
}

impl Reservation {
    /// Wrap a placement-specific guard.
    pub fn new<T: Any + Send + Sync>(kind: ReservationKind, inner: T) -> Self {
        Self {
            kind,
            inner: Box::new(inner),
        }
    }

    pub fn kind(&self) -> ReservationKind {
        self.kind
    }

    /// The guard, if it is a `T`.
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.inner.downcast_ref::<T>()
    }

    /// Take the guard out as a `T`; on a type mismatch the reservation is returned intact.
    pub fn into_inner<T: Any>(self) -> Result<Box<T>, Self> {
        let kind = self.kind;
        self.inner
            .downcast::<T>()
            .map_err(|inner| Self { kind, inner })
    }
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// Result of [`Placement::reserve`].
#[derive(Debug)]
pub enum Reserve {
    Granted(Reservation),
    /// No capacity now; retry after [`Placement::changed`] moves.
    Blocked,
}

/// Capacity source of the dispatcher (one per node, shared by every partition).
pub trait Placement: Send + Sync + 'static {
    /// Try to reserve capacity for `e` without waiting.
    fn reserve(&self, e: &EntryView<'_>) -> Reserve;
    /// Bumped whenever capacity may have become available (a release, a worker joining).
    fn changed(&self) -> watch::Receiver<u64>;
}

/// The guard inside a [`SemaphorePlacement`] reservation: the slot permit plus the change
/// notifier, bumped after the permit is returned.
pub struct EmbeddedPermit {
    permit: Option<OwnedSemaphorePermit>,
    changed: Arc<watch::Sender<u64>>,
}

impl Drop for EmbeddedPermit {
    fn drop(&mut self) {
        // Release first, then notify: a woken dispatcher must find the slot free.
        drop(self.permit.take());
        self.changed.send_modify(|v| *v = v.wrapping_add(1));
    }
}

impl std::fmt::Debug for EmbeddedPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedPermit").finish_non_exhaustive()
    }
}

/// The 04-14 behaviour: one node-wide slot semaphore; every reservation is `Embedded`.
#[derive(Clone)]
pub struct SemaphorePlacement {
    slots: Arc<Semaphore>,
    changed: Arc<watch::Sender<u64>>,
}

impl SemaphorePlacement {
    /// Placement over `slots` (the same semaphore as `DispatcherConfig::slots`).
    pub fn new(slots: Arc<Semaphore>) -> Self {
        Self {
            slots,
            changed: Arc::new(watch::Sender::new(0)),
        }
    }

    /// The underlying slot semaphore.
    pub fn slots(&self) -> &Arc<Semaphore> {
        &self.slots
    }
}

impl std::fmt::Debug for SemaphorePlacement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SemaphorePlacement")
            .field("available", &self.slots.available_permits())
            .finish()
    }
}

impl Placement for SemaphorePlacement {
    fn reserve(&self, _e: &EntryView<'_>) -> Reserve {
        match Arc::clone(&self.slots).try_acquire_owned() {
            Ok(permit) => Reserve::Granted(Reservation::new(
                ReservationKind::Embedded,
                EmbeddedPermit {
                    permit: Some(permit),
                    changed: Arc::clone(&self.changed),
                },
            )),
            Err(_) => Reserve::Blocked,
        }
    }

    fn changed(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
}
