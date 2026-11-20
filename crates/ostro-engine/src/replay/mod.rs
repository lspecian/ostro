//! Replay core (plan 03-05): journal cursor, command matching and nondeterminism
//! detection (ENG-03/ENG-04), deterministic logical time, wait ordering, patch markers,
//! the step guard, continue-as-new suggestion, per-run randomness streams and await
//! tokens (WIT-02..05).
//!
//! Pure Rust: no Wasmtime types and no dependency on the journal codec or command hash
//! (plan 03-02). Callers pass precomputed `(CommandKind, CommandHash)` pairs and the
//! core treats recorded results as opaque events. The host adapter (plan 03-07) is a
//! thin layer over [`ReplayState`].
//!
//! Logical time is derived from the journal only: the ts of the last guest command,
//! raised for a re-run step attempt to the failed attempt's `retry_at_nanos` (D22, the
//! retry logical clock; see `cursor`), and never moving backwards.

mod cursor;
pub mod rng;
pub mod token;
mod wait;

#[cfg(test)]
mod tests;

pub use cursor::{CommandResult, ReplayError, ReplayState, StepBegin};
pub use rng::{ChaChaStream, RngStreams, insecure_seed, uuid_v7};
pub use token::{TokenError, TokenParts};
pub use wait::WaitOutcome;

/// Replay metrics (OBS-02, plan 08-16). Recorded once per activation by the activation
/// driver; no labels (a run id is never a label).
pub(crate) mod metrics {
    /// `ostro_replay_events_total`: committed journal events an activation re-executed.
    pub(crate) const EVENTS: &str = "ostro_replay_events_total";
    /// `ostro_replay_duration_seconds`: activation start until replay ended (the first
    /// live command, or the end of the guest call when nothing live was journaled).
    pub(crate) const DURATION: &str = "ostro_replay_duration_seconds";

    /// Count `events` replayed events.
    pub(crate) fn events(events: u64) {
        ::metrics::counter!(EVENTS).increment(events);
    }

    /// Record one replay duration.
    pub(crate) fn duration(secs: f64) {
        ::metrics::histogram!(DURATION).record(secs);
    }
}
