//! Phase 7: implemented in 07-13.
//!
//! Spill hysteresis (D-W9): sampled every 1 s (`LoadSampler`: getrusage CPU, embedded slot
//! use, Wasm memory, queue lag); entry on any signal over its threshold for `sustain`,
//! exit after a full cool-down below `entry * hysteresis_pct`. Time comes from the injected
//! clock (D-W26).

use tokio::task::JoinHandle;

use crate::p7::P7Ctx;

/// Spill state shared by placement and demand.
#[derive(Debug, Default)]
pub struct SpillHandle {
    _private: (),
}

impl SpillHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `queue` currently spills to workers.
    ///
    /// Phase 7 stub (07-02): never.
    pub fn spilling(&self, queue: &str) -> bool {
        let _ = queue;
        false
    }

    /// Placement found the embedded slots full for `queue` at `now_ms` (injected clock, B6).
    ///
    /// Phase 7 stub (07-02): no-op.
    pub fn note_overflow(&self, queue: &str, now_ms: u64) {
        let _ = (queue, now_ms);
    }
}

/// Start the spill sampler; `None` when nothing runs.
///
/// Phase 7 stub (07-02): `None`.
pub fn start(ctx: &P7Ctx) -> Option<JoinHandle<()>> {
    let _ = ctx;
    None
}
