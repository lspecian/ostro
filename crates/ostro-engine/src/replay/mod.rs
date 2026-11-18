//! Replay core (plan 03-05): journal cursor, command matching and nondeterminism
//! detection (ENG-03/ENG-04), deterministic logical time, wait ordering, patch markers,
//! the step guard, continue-as-new suggestion, per-run randomness streams and await
//! tokens (WIT-02..05).
//!
//! Pure Rust: no Wasmtime types and no dependency on the journal codec or command hash
//! (plan 03-02). Callers pass precomputed `(CommandKind, CommandHash)` pairs and the
//! core treats recorded results as opaque events. The host adapter (plan 03-07) is a
//! thin layer over [`ReplayState`].

mod cursor;
pub mod rng;
pub mod token;
mod wait;

#[cfg(test)]
mod tests;

pub use cursor::{CommandResult, ReplayError, ReplayState, StepBegin};
pub use rng::{ChaChaStream, RngStreams, insecure_seed, uuid_v7};
pub use token::TokenParts;
pub use wait::WaitOutcome;
