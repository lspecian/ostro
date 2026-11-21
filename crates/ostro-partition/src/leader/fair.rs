//! Phase 7: implemented in 07-05.
//!
//! Fair selection of the next ready entry (D-W10): weighted DRR across queues (quantum =
//! `FlowControl::queue_weight`), strict priority bands within a queue, DRR across
//! concurrency keys within a band (quantum 1), FIFO per key by (ready_at, run). A
//! `KeyBlocked` key leaves the ring until a release or its `until_ms`. Pure (no I/O, no
//! clock reads), unit-tested on its own.
