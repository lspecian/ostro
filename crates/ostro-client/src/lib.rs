//! ostro-client: the Rust REST client for an Ostro server and, behind the `testing`
//! feature, the spawned-server test kit.
//!
//! - [`client`]: the raw client (moved here from `ostro-cli` in 10-09). Every method
//!   returns the JSON body as a [`serde_json::Value`] ([`Response`]) or a [`CallError`]
//!   that keeps the server's error body. The `ostro` CLI uses it through a re-export.
//! - [`error`]: [`ClientError`] `{ status, code, message, details }`, built from the REST
//!   error body.
//!
//! The crate is bundleable (10-08): its manifest uses explicit versions only, so
//! `ostro init` projects can depend on a copy outside this workspace. Phase 11 adds client
//! methods in their own module of this crate.
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use ostro_client::Client;
//! use serde_json::json;
//!
//! # async fn demo() -> Result<(), ostro_client::ClientError> {
//! let client = Client::new("http://127.0.0.1:7700", Duration::from_secs(10))?;
//! let started = client
//!     .start_run("hello", &json!({ "input": { "name": "world" } }))
//!     .await?;
//! let run_id = started.body["run_id"].as_str().unwrap_or_default().to_owned();
//! let run = client.get_run(&run_id).await?;
//! println!("{}", run.body["status"]);
//! # Ok(())
//! # }
//! ```

pub mod client;
pub mod error;

pub use client::{CallError, Client, Response};
pub use error::ClientError;

/// Spawned-server test kit (10-17 replaces this stub).
#[cfg(feature = "testing")]
pub mod testing {}

// Plan 10-02: dependencies of 10-09 / 10-17, anchored until they are used.
use serde as _;
use sha2 as _;
use tokio as _;
use tracing as _;

#[cfg(test)]
mod dev_dep_anchors {
    use axum as _;
}
