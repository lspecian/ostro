//! Phase 7: implemented in 07-08.
//!
//! StartRun admission (D-W21): GCRA per queue and per (queue, key) before the proposal;
//! excess answers `ErrorCode::RateLimited` (HTTP 429 + `Retry-After`, gRPC
//! `RESOURCE_EXHAUSTED`, `retry_after_ms` detail). A start whose idempotency key already
//! has a dedupe row is not charged.
