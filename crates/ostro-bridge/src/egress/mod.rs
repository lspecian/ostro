//! SSRF-hardened egress (06-04, FX-08).
//!
//! The guarantees here come from *where* checks happen, not from string matching:
//! [`classify`] holds the IANA special-purpose tables and unwraps embedded IPv4 addresses.

pub mod classify;
pub mod config;

pub use classify::{effective_v4, is_blocked};
pub use config::EgressConfig;
