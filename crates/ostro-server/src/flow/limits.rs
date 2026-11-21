//! Phase 7: implemented in 07-08.
//!
//! Node-wide concurrency counters per queue and per (queue, key), and the hand-rolled GCRA
//! (D-W24): one TAT per queue and per (queue, key), `T = period_ms / limit`,
//! `tau = T * (burst - 1)`; time from the injected clock (D-W26). Tentative charges commit
//! through `FlowPermit::commit`.
