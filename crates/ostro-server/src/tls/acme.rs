//! Phase 7: implemented in 07-10.
//!
//! `acme` mode over instant-acme (D-W6): account, order, HTTP-01 / TLS-ALPN-01
//! challenge, finalize, cache under `[api.tls.acme] cache_dir`, renewal at 2/3 of the
//! lifetime scheduled from the injected clock.
