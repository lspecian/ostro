//! ostro-engine: deterministic Wasm execution engine.
//!
//! Every Phase 3 module is declared here once (plan 03-01) so later plans own
//! disjoint files and never edit this file.

pub mod activation;
pub mod cache;
pub mod config;
pub mod engine;
pub mod env;
pub mod error;
pub mod host;
pub mod limits;
pub mod replay;
pub mod runtime;
pub mod sink;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod validate;

// Dependencies wired in plan 03-01 for later Phase 3 plans (ENG-04 RNG derivation).
// Anchored here so cargo's unused-dependency lint stays quiet until the owning modules
// consume them; harmless afterwards. (hmac/metrics/tracing are used by `cache`.)
use chacha20 as _;
