//! Remote workers on the server side (Phase 7, WORK-01..06).
//!
//! | File | Owner | Purpose |
//! |---|---|---|
//! | [`registry`] | 07-09 | connected workers, sessions, credit, labels |
//! | [`service`] | 07-09 | the `ostro.worker.v1` Execute service on the cluster listener (mTLS) |
//! | [`join`] | 07-09 | `POST /v1/workers/join`, `Renew`: token check + CSR signing (D-W4) |
//! | [`remote_executor`] | 07-12 | the remote runner: one dispatcher `Job` proxied to a session |
//! | [`placement`] | 07-12 | `NodePlacement`: embedded slots first, overflow to workers (B6) |
//! | [`selector`] | 07-12 | workflow/queue label selectors |
//! | [`spill`] | 07-13 | spill hysteresis (D-W9) |
//! | [`demand`] | 07-16 | `ostro_queue_worker_demand` value + `GET /v1/queues/{q}/demand` (D-W12) |
//!
//! Plan 07-02 fixes the signatures; every hook is a stub that preserves today's behaviour.

pub mod demand;
pub mod join;
pub mod placement;
pub mod registry;
pub mod remote_executor;
pub mod selector;
pub mod service;
pub mod spill;
