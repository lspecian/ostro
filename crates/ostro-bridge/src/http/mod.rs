//! The HTTP connector (06-10; FX-01, FX-02, FX-03, FX-07).
//!
//! - [`key`]: the idempotency key (persistent contract, pinned by test vectors).
//! - [`window`]: the unknown-outcome window, applied at re-drive time only.
//! - [`retry`]: in-attempt retry classification and backoff.
//! - [`target`]: the per-destination `http-target` config.

pub mod key;
pub mod retry;
pub mod target;
pub mod window;

pub use key::{EffectiveKey, KeySource, derive_key, effective_key};
pub use retry::{Class, Schedule, Transport};
pub use target::{HTTP_TARGET_KIND, HttpTarget, ResendMode, TargetError};
pub use window::{Decision, decide};
