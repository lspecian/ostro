//! Weighted core-instance admission (plan 09-07, ENG-2; D-P9-17).
//!
//! The pooling allocator has a fixed number of core-instance slots
//! (`total_core_instances`). A component instance takes as many as its module
//! instantiates (Rust 3, QuickJS 13, Python 38, S9), so counting component instances
//! alone lets a node run out of core slots and fail `instantiate` with "maximum
//! concurrent limit ... core instances". [`CoreBudget`] is a weighted semaphore sized to
//! the pool's core-instance total: every instantiation first acquires the module's
//! measured weight ([`crate::validate::count_core_instances`]) and holds the
//! [`CorePermit`] for as long as the instance lives. When the budget is exhausted the
//! caller waits (FIFO, so a heavy module is not starved by light ones) up to a deadline;
//! [`crate::runtime::Runtime::activate`] then evicts for retry instead of failing.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why [`CoreBudget::acquire`] gave no permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BudgetError {
    /// The deadline passed before enough slots were free.
    #[error("core-instance budget: no {weight} free slots before the deadline")]
    Timeout { weight: u32 },
    /// The weight exceeds the whole budget: it can never be admitted.
    #[error("core-instance budget: weight {weight} exceeds capacity {capacity}")]
    TooLarge { weight: u32, capacity: u32 },
}

/// Weighted semaphore over the node's core-instance slots.
#[derive(Debug)]
pub struct CoreBudget {
    sem: Arc<Semaphore>,
    capacity: u32,
}

/// Slots held by one live instance; released on drop.
#[derive(Debug)]
pub struct CorePermit {
    _permit: Option<OwnedSemaphorePermit>,
    weight: u32,
}

impl CorePermit {
    /// Slots this permit holds.
    pub fn weight(&self) -> u32 {
        self.weight
    }
}

impl CoreBudget {
    /// Capacity used without a pool (on-demand allocator): effectively unbounded.
    pub const UNBOUNDED: u32 = u32::MAX;

    /// A budget of `capacity` slots.
    pub fn new(capacity: u32) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(capacity as usize)),
            capacity,
        }
    }

    /// Total slots.
    pub fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Slots free right now.
    pub fn available(&self) -> u32 {
        u32::try_from(self.sem.available_permits()).unwrap_or(u32::MAX)
    }

    /// Slots held by live permits right now.
    pub fn in_use(&self) -> u32 {
        self.capacity.saturating_sub(self.available())
    }

    /// Acquire `weight` slots, waiting (FIFO) until `deadline`.
    ///
    /// A weight above the capacity fails at once with [`BudgetError::TooLarge`]; a zero
    /// weight succeeds at once.
    pub async fn acquire(&self, weight: u32, deadline: Instant) -> Result<CorePermit, BudgetError> {
        if weight > self.capacity {
            return Err(BudgetError::TooLarge {
                weight,
                capacity: self.capacity,
            });
        }
        if weight == 0 {
            return Ok(CorePermit {
                _permit: None,
                weight,
            });
        }
        let fut = self.sem.clone().acquire_many_owned(weight);
        match tokio::time::timeout_at(deadline.into(), fut).await {
            Ok(Ok(permit)) => Ok(CorePermit {
                _permit: Some(permit),
                weight,
            }),
            // The semaphore is never closed; treat it like a timeout regardless.
            Ok(Err(_)) | Err(_) => Err(BudgetError::Timeout { weight }),
        }
    }
}
