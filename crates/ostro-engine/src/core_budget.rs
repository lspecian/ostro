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
//!
//! Core instances are not the only pooled resource (plan eng2-mem): the pool also has a
//! fixed number of component-instance slots, linear memories (`total_memories =
//! total_instances`) and tables (`total_tables = 2 × total_instances`), and a QuickJS
//! instance needs 3 memories and 3 tables. [`PoolBudget`] therefore keeps one
//! [`CoreBudget`] per [`PoolResource`] and admits an instantiation only when its weight
//! fits in every one of them, acquiring them in a fixed order with one deadline.

use std::sync::Arc;
use std::time::Instant;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Why [`CoreBudget::acquire`] gave no permit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BudgetError {
    /// The deadline passed before enough slots were free.
    #[error("no {weight} free slots before the deadline")]
    Timeout { weight: u32 },
    /// The weight exceeds the whole budget: it can never be admitted.
    #[error("weight {weight} exceeds capacity {capacity}")]
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

/// A pooled resource that admission budgets (plan eng2-mem).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolResource {
    /// Component-instance slots (`total_component_instances`; also bounds the async fiber
    /// stacks, `total_stacks`, one per live store).
    Instances,
    /// Core-instance slots (`total_core_instances`).
    CoreInstances,
    /// Linear-memory slots (`total_memories`).
    Memories,
    /// Table slots (`total_tables`).
    Tables,
}

impl PoolResource {
    /// Acquisition order of [`PoolBudget::acquire`] (fixed, so waiters cannot deadlock).
    pub const ORDER: [PoolResource; 4] = [
        Self::Instances,
        Self::CoreInstances,
        Self::Memories,
        Self::Tables,
    ];

    /// Metric/log spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Instances => "instances",
            Self::CoreInstances => "core_instances",
            Self::Memories => "memories",
            Self::Tables => "tables",
        }
    }

    /// Human spelling used in error messages ("core-instance budget: ...").
    pub fn label(self) -> &'static str {
        match self {
            Self::Instances => "component-instance",
            Self::CoreInstances => "core-instance",
            Self::Memories => "memory",
            Self::Tables => "table",
        }
    }
}

/// Pool slots one instantiation takes, per resource (also used for capacities).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PoolWeights {
    pub instances: u32,
    pub core_instances: u32,
    pub memories: u32,
    pub tables: u32,
}

impl PoolWeights {
    /// Effectively unbounded capacities (on-demand allocator).
    pub const UNBOUNDED: PoolWeights = PoolWeights {
        instances: CoreBudget::UNBOUNDED,
        core_instances: CoreBudget::UNBOUNDED,
        memories: CoreBudget::UNBOUNDED,
        tables: CoreBudget::UNBOUNDED,
    };

    /// The value for `r`.
    pub fn get(&self, r: PoolResource) -> u32 {
        match r {
            PoolResource::Instances => self.instances,
            PoolResource::CoreInstances => self.core_instances,
            PoolResource::Memories => self.memories,
            PoolResource::Tables => self.tables,
        }
    }
}

/// Why [`PoolBudget::acquire`] gave no permit: which resource, and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{} budget: {source}", resource.label())]
pub struct PoolBudgetError {
    pub resource: PoolResource,
    pub source: BudgetError,
}

/// Admission over every pooled resource an instantiation takes (plan eng2-mem).
///
/// One [`CoreBudget`] per [`PoolResource`], each sized to the pooling allocator's total.
/// [`PoolBudget::acquire`] takes a module's weights from all of them in the fixed
/// [`PoolResource::ORDER`], with one deadline. Every waiter acquires in the same order,
/// so no cycle of waiters can form (a waiter only ever waits on a resource later in the
/// order than everything it already holds). On failure the partial permits are dropped.
#[derive(Debug)]
pub struct PoolBudget {
    instances: CoreBudget,
    core: CoreBudget,
    memories: CoreBudget,
    tables: CoreBudget,
}

/// Slots of every resource held by one live instance; released on drop.
#[derive(Debug)]
pub struct PoolPermit {
    _permits: Vec<CorePermit>,
    weights: PoolWeights,
}

impl PoolPermit {
    /// Slots this permit holds, per resource.
    pub fn weights(&self) -> PoolWeights {
        self.weights
    }
}

impl PoolBudget {
    /// Budgets with the given per-resource capacities.
    pub fn new(capacity: PoolWeights) -> Self {
        Self {
            instances: CoreBudget::new(capacity.instances),
            core: CoreBudget::new(capacity.core_instances),
            memories: CoreBudget::new(capacity.memories),
            tables: CoreBudget::new(capacity.tables),
        }
    }

    /// The budget of one resource.
    pub fn budget(&self, r: PoolResource) -> &CoreBudget {
        match r {
            PoolResource::Instances => &self.instances,
            PoolResource::CoreInstances => &self.core,
            PoolResource::Memories => &self.memories,
            PoolResource::Tables => &self.tables,
        }
    }

    /// Capacities, per resource.
    pub fn capacity(&self) -> PoolWeights {
        self.map(CoreBudget::capacity)
    }

    /// Slots held by live permits right now, per resource.
    pub fn in_use(&self) -> PoolWeights {
        self.map(CoreBudget::in_use)
    }

    fn map(&self, f: impl Fn(&CoreBudget) -> u32) -> PoolWeights {
        PoolWeights {
            instances: f(&self.instances),
            core_instances: f(&self.core),
            memories: f(&self.memories),
            tables: f(&self.tables),
        }
    }

    /// Acquire `w` from every resource, waiting (FIFO per resource) until `deadline`.
    ///
    /// A weight above its capacity fails at once with [`BudgetError::TooLarge`] before
    /// anything is acquired.
    pub async fn acquire(
        &self,
        w: PoolWeights,
        deadline: Instant,
    ) -> Result<PoolPermit, PoolBudgetError> {
        for r in PoolResource::ORDER {
            let (weight, capacity) = (w.get(r), self.budget(r).capacity());
            if weight > capacity {
                return Err(PoolBudgetError {
                    resource: r,
                    source: BudgetError::TooLarge { weight, capacity },
                });
            }
        }
        let mut permits = Vec::with_capacity(PoolResource::ORDER.len());
        for r in PoolResource::ORDER {
            let p = self
                .budget(r)
                .acquire(w.get(r), deadline)
                .await
                .map_err(|source| PoolBudgetError {
                    resource: r,
                    source,
                })?;
            permits.push(p);
        }
        Ok(PoolPermit {
            _permits: permits,
            weights: w,
        })
    }
}
