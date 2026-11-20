//! ostro-bridge: the Phase 6 effects bridge (FX-01..FX-10). It executes the host effects a
//! workflow schedules (`http-fetch`, `sql-execute`, `object-op`), resolves secrets for them
//! and completes each effect exactly once through the partition log.
//!
//! # Layers
//!
//! 1. **Driver core** ([`driver`], [`registry`], [`ratelimit`], [`outcome`], [`config`]): the
//!    `EffectDriver` the engine calls, the in-flight registry, per-target rate limits, outcome
//!    encoding and completion submission through a [`connector::CompletionSink`].
//! 2. **Connectors** ([`http`], [`sql`], [`object`]) implement [`connector::Connector`]. Every
//!    outbound HTTP request goes through [`egress`] (URL gate, address classification,
//!    resolver, the single client builder).
//! 3. **Secrets** ([`secrets`]): templates, grants, scrubbing and the provider chain
//!    ([`secrets::providers::SecretProvider`]).
//!
//! The contract files ([`kinds`], [`connector`], [`meta_view`], [`origin`], [`failpoints`]) are
//! written by 06-02; later Phase 6 plans fill only their own modules and never edit this file.
//!
//! # Dependency direction
//!
//! `ostro-bridge` depends on `ostro-types` (journal mirrors, [`ostro_types::live`]),
//! `ostro-meta` (sealed secrets, grants, connections, egress policies) and `ostro-objstore`.
//! It depends on **neither `ostro-engine` nor `ostro-partition`**: `ostro-server` wires the
//! bridge to the engine seam and the partition handle.
//!
//! A resolved secret value never reaches a log line, a `Debug` impl, the journal, an error
//! message or a snapshot.

pub mod config;
pub mod connector;
pub mod driver;
pub mod egress;
pub mod failpoints;
pub mod http;
pub mod kinds;
pub mod meta_view;
pub mod object;
pub mod origin;
pub mod outcome;
pub mod ratelimit;
pub mod registry;
pub mod secrets;
pub mod sql;

// Dependencies declared once by plan 06-02 for later Phase 6 plans (06-04 egress resolver,
// 06-07 rate limits/metrics, 06-11 sql, 06-12 objects, 06-13 vault/k8s). Anchored here so
// cargo's unused-dependency lint stays quiet until the owning modules consume them; harmless
// afterwards.
mod dep_anchors {
    use aho_corasick as _;
    use aws_lc_rs as _;
    use base64 as _;
    use bytes as _;
    use governor as _;
    use hickory_resolver as _;
    use hmac as _;
    use ipnet as _;
    use jiff as _;
    #[cfg(feature = "k8s")]
    use k8s_openapi as _;
    #[cfg(feature = "k8s")]
    use kube as _;
    use metrics as _;
    use ostro_objstore as _;
    use reqwest as _;
    use serde as _;
    use serde_json as _;
    use sha2 as _;
    use sqlx as _;
    use tracing as _;
    #[cfg(feature = "vault")]
    use vaultrs as _;
    use zeroize as _;
}
