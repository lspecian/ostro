//! `rebuild_digest` generator hook for terminate, retry, run/schedule-to-start timeouts,
//! retention expiry and continue-as-new (owned by plan **05-09**; stubbed by 05-02).
//!
//! `tests/rebuild_digest.rs` merges [`strategy`] into its command strategy and, for every
//! generated op, submits [`commands`] against the driver's runs (whose `next_seq` is
//! re-read from live state first). 05-09 fills [`LifecycleOp`]; no edit of
//! `rebuild_digest.rs` is needed for that.

use ostro_types::Command;
use proptest::prelude::*;

use super::GenRun;

/// One generated lifecycle operation. Empty until 05-09.
#[derive(Clone, Debug)]
pub enum LifecycleOp {}

/// Strategy for one optional [`LifecycleOp`]. Stub (05-02): always `None` (no command).
pub fn strategy() -> BoxedStrategy<Option<LifecycleOp>> {
    Just(None).boxed()
}

/// `(partition, command)` pairs for `op` over the driver's runs; `n` is the op index (unique
/// per scenario, usable as an op id).
pub fn commands(op: &LifecycleOp, _runs: &[GenRun], _n: usize) -> Vec<(u16, Command)> {
    match *op {}
}
