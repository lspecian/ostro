//! Phase 7: implemented in 07-06.
//!
//! One rustls `ResolvesServerCert` backed by `arc_swap::ArcSwap<CertifiedKey>` for every
//! mode (reloads and renewals swap without a restart); answers `acme-tls/1` ClientHellos
//! with the TLS-ALPN-01 challenge certificate (07-10).
