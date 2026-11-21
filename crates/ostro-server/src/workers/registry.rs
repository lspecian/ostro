//! Phase 7: implemented in 07-09.
//!
//! The worker registry: connected workers and their sessions (one per worker and node),
//! labels, free slots (absolute credit), memory, leases held.

use crate::config::WorkersSection;

/// Connected workers of this node.
#[derive(Debug)]
pub struct WorkerRegistry {
    max_frame_bytes: u64,
}

impl WorkerRegistry {
    /// An empty registry configured from `[workers]`.
    pub fn new(cfg: &WorkersSection) -> Self {
        Self {
            max_frame_bytes: cfg.max_frame_bytes,
        }
    }

    /// Connected workers (0 until 07-09).
    pub fn connected(&self) -> usize {
        0
    }

    /// Free worker slots over every session (0 until 07-09).
    pub fn slots_free(&self) -> u64 {
        0
    }

    /// `[workers] max_frame_bytes`.
    pub fn max_frame_bytes(&self) -> u64 {
        self.max_frame_bytes
    }
}
