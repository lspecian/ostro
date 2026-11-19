//! `rebuild_digest` generator hook for waits, signals, await tokens and graceful cancel
//! (owned by plan **05-08**; stubbed by 05-02).
//!
//! `tests/rebuild_digest.rs` merges [`strategy`] into its command strategy and, for every
//! generated op, submits [`commands`] against the driver's runs (whose `next_seq` is
//! re-read from live state first). 05-08 fills [`WaitOp`] with EventsAppended carrying
//! RegisterWait/CancelWait, SignalRun (with and without request_id, duplicates included),
//! CompleteToken (duplicates included), RequestCancel and TimerFired for wait ops. No edit
//! of `rebuild_digest.rs` is needed for that.

use ostro_types::Command;
use proptest::prelude::*;

use super::GenRun;

/// One generated wait/signal/token/cancel operation. Empty until 05-08.
#[derive(Clone, Debug)]
pub enum WaitOp {}

/// Strategy for one optional [`WaitOp`]. Stub (05-02): always `None` (no command).
pub fn strategy() -> BoxedStrategy<Option<WaitOp>> {
    Just(None).boxed()
}

/// `(partition, command)` pairs for `op` over the driver's runs; `n` is the op index (unique
/// per scenario, usable as an op id).
pub fn commands(op: &WaitOp, _runs: &[GenRun], _n: usize) -> Vec<(u16, Command)> {
    match *op {}
}
