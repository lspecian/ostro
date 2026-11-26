//! The node-local, hash-chained audit journal (D-P14-11), implemented by **14-04**; stubs
//! with fixed signatures until then (14-01b).
//!
//! - [`writer`]: `open` (the [`ostro_types::audit::AuditSink`] in `P14Ctx`), `start`, `flush`.
//! - [`journal`], [`segment`], [`chain`]: record format, segment files, the hash chain.
//! - [`verify`]: offline chain verification (`ostro audit verify --data-dir`).
//! - [`export`]: chain-head export and OTLP log export.

pub mod chain;
pub mod export;
pub mod journal;
pub mod segment;
pub mod verify;
pub mod writer;
