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
pub mod retry;
pub mod runtime;
pub mod sink;
/// Span-id override contract (Phase 8, P8-D14; plan 08-01).
pub mod telemetry;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod validate;

/// Await-token API (05-03) for the server's token completion endpoint (05-13):
/// `parse` (routing fields, no MAC check) and `verify` (constant-time HMAC check under
/// the cluster token key). Token bodies are bearer credentials: keep them out of URLs
/// and logs.
pub mod token {
    pub use crate::replay::token::{
        TokenError, TokenParts, mint, parse_token as parse, verify_token as verify,
    };
}

// Dependencies wired in plan 03-01 for later Phase 3 plans (ENG-04 RNG derivation).
// Anchored here so cargo's unused-dependency lint stays quiet until the owning modules
// consume them; harmless afterwards. (hmac/metrics/tracing are used by `cache`.)
use chacha20 as _;
// Phase 8 (plan 08-01): the OTel API crate for 08-11's span contexts (P8-D13/D14).
use opentelemetry as _;
