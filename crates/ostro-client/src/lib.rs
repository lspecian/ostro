//! ostro-client: the Rust REST client for an Ostro server and, behind the `testing`
//! feature, the spawned-server test kit.
//!
//! - [`client`]: the raw client (moved here from `ostro-cli` in 10-09). Every method
//!   returns the JSON body as a [`serde_json::Value`] ([`Response`]) or a [`CallError`]
//!   that keeps the server's error body. The `ostro` CLI uses it through a re-export.
//! - [`typed`]: [`TypedClient`], typed requests and answers over the raw client, and
//!   [`typed::ROUTES`], every `(method, path)` the crate calls (checked against the
//!   server's OpenAPI document by `tests/contract.rs`).
//! - [`error`]: [`ClientError`] `{ status, code, message, details }`, built from the REST
//!   error body.
//! - `testing` (feature `testing`): `TestEnv`, a spawned `ostro dev --ephemeral
//!   --time-skipping` with deploy, run, signal, time-skip and history helpers (10-17).
//!
//! The crate is bundleable (10-08): its manifest uses explicit versions only, so
//! `ostro init` projects can depend on a copy outside this workspace. Phase 11 adds client
//! methods in their own module of this crate.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ostro_client::{StartRun, TypedClient};
//! use serde_json::json;
//!
//! # async fn demo() -> Result<(), ostro_client::ClientError> {
//! let client = TypedClient::new("http://127.0.0.1:7700", Duration::from_secs(10))?;
//! let started = client
//!     .start_run(&StartRun::new("hello", json!({ "name": "world" })))
//!     .await?;
//! client.signal(&started.run_id, "approve", json!(true), None).await?;
//! let result = client
//!     .result(&started.run_id, Some(Duration::from_secs(30)))
//!     .await?;
//! println!("{:?} {:?}", result.status, result.output);
//!
//! // The raw client answers with the JSON body (what `ostro --json` prints).
//! let run = client.raw().get_run(&started.run_id).await?;
//! println!("{}", run.body["status"]);
//! # Ok(())
//! # }
//! ```

pub mod client;
// Phase 13 (13-02 stubs; 13-16 fills): cluster, upgrade, migration, module lifecycle.
pub mod cluster;
pub mod error;
pub mod typed;
// Phase 14 (14-01b stubs; 14-17 tenants/api_tokens, 14-18 audit, 14-19 security).
pub mod api_tokens;
pub mod audit;
pub mod security;
pub mod tenants;

pub use client::{CallError, Client, Response};
pub use error::ClientError;
pub use typed::{StartRun, TypedClient};

#[cfg(feature = "testing")]
pub mod testing;

// Plan 10-02: dependencies of 10-09 / 10-17, anchored until they are used.
use tokio as _;
use tracing as _;

/// The environment variable holding the API bearer token (Phase 14, D-P14-5).
pub const TOKEN_ENV: &str = "OSTRO_TOKEN";

/// The bearer token from [`TOKEN_ENV`] (`None` when unset or blank). [`Client`] sends it as
/// `Authorization: Bearer <token>` on every request ([`authorization`]).
pub fn bearer_from_env() -> Option<String> {
    std::env::var(TOKEN_ENV)
        .ok()
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
}

/// The `Authorization` header value for `token` (`Bearer <token>`); `None` when the token
/// is not a valid header value.
pub fn authorization(token: &str) -> Option<reqwest::header::HeaderValue> {
    let mut v = reqwest::header::HeaderValue::from_str(&format!("Bearer {token}")).ok()?;
    v.set_sensitive(true);
    Some(v)
}

#[cfg(test)]
mod dev_dep_anchors {
    use axum as _;
}
