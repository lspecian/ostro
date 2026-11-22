//! Phase 12 replica logic (pure, clock-injected): follower progress tracking, the
//! published-ISR rules, HW computation, divergence answers and failover choice.
//!
//! Module list fixed by 12-02; the bodies are implemented in 12-04.

pub mod divergence;
pub mod failover;
pub mod isr;
pub mod tracker;
pub mod watermark;
