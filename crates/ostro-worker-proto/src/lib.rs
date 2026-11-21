//! ostro-worker-proto: the `ostro.worker.v1` leader <-> worker protocol (Phase 7, D-W2).
//!
//! - [`v1`]: the generated `Execute` service (`Session`, `FetchJournal`, `FetchBlob`,
//!   `Append`, `StartEffect`, `Renew`) and its messages (codegen via protox, no protoc).
//! - [`WireOutcome`]: the postcard mirror of `ostro_engine::sink::ActivationOutcome` carried
//!   in `Done.outcome` (D-W19). The engine is not edited; the conversion is exhaustive, so
//!   a new engine variant breaks this crate's build.
//!
//! Proto field numbers are owned by this crate and versioned by [`PROTO_VERSION`]; they
//! are not a RESERVATIONS.md space.

pub mod outcome;

pub use outcome::{WireAttemptFailure, WireEvictReason, WireOutcome, WireOutcomeError};

/// Version of the `ostro.worker.v1` protocol spoken by this build (`Hello.proto_version`).
pub const PROTO_VERSION: u32 = 1;

/// `ostro.worker.v1` generated types.
pub mod v1 {
    tonic::include_proto!("ostro.worker.v1");
}
