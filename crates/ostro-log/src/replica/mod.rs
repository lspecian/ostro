//! Phase 12 replica logic (pure, clock-injected): follower progress tracking, the
//! published-ISR rules, HW computation, divergence answers and failover choice.
//!
//! Every function here is deterministic: no clocks (time enters as `now_ms: u64`), no I/O,
//! no async. Written from the Phase 12 spec (Kafka KIP-497 / KIP-595 / KIP-279 / KIP-101
//! semantics, D-P12-5/6/7); no upstream code is ported.
//!
//! The permanent `repl::mutant::*` failpoints placed here (D-P12-19) are evaluated through
//! [`mutant`], which is a constant `false` unless the crate's `failpoints` feature is on.

pub mod divergence;
pub mod failover;
pub mod isr;
pub mod tracker;
pub mod watermark;

/// Whether the mutant failpoint `name` is enabled (`fail::cfg(name, "return")`).
#[cfg(feature = "failpoints")]
#[inline]
pub(crate) fn mutant(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

/// Whether the mutant failpoint `name` is enabled: never, without the `failpoints` feature.
#[cfg(not(feature = "failpoints"))]
#[inline(always)]
pub(crate) fn mutant(_name: &str) -> bool {
    false
}
