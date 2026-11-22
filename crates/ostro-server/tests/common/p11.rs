//! Phase 11 test helpers (11-04): a [`P11Server`] builder (wrapping the Phase 8
//! [`Phase8Builder`]) with `[limits]`, `[ui]` and `[bulk]` overrides, and REST helpers for
//! rerun, DLQ, bulk jobs and run status. `tests/common/mod.rs` is never edited by Phase 11;
//! include this file next to it:
//!
//! ```ignore
//! mod common;
//! #[path = "common/p11.rs"]
//! mod p11;
//! ```
#![allow(dead_code)]

use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::time::Duration;

use ostro_server::config::{BulkSection, LimitsSection, ServerConfig, UiSection};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

use crate::common::TestServer;
use crate::common::phase8::{Phase8Builder, Phase8Server};

/// Builder of a [`P11Server`] (`P11Server::builder()`).
pub struct P11Builder {
    inner: Phase8Builder,
}

/// A [`TestServer`] started with Phase 11 overrides. Derefs to the [`TestServer`].
pub struct P11Server {
    pub server: Phase8Server,
}

impl Deref for P11Server {
    type Target = TestServer;
    fn deref(&self) -> &TestServer {
        &self.server.server
    }
}

impl DerefMut for P11Server {
    fn deref_mut(&mut self) -> &mut TestServer {
        &mut self.server.server
    }
}

impl P11Server {
    /// Defaults: P=4, persistent tmpfs data dir, 127.0.0.1:0 (so the UI is on).
    pub fn builder() -> P11Builder {
        P11Builder {
            inner: Phase8Server::builder(),
        }
    }

    /// The data directory.
    pub fn data_dir(&self) -> &Path {
        self.h().data_dir()
    }
}

impl P11Builder {
    /// Partition count.
    pub fn partitions(mut self, p: u16) -> Self {
        self.inner = self.inner.partitions(p);
        self
    }

    /// Edit `[limits]`.
    pub fn with_limits(self, f: impl FnOnce(&mut LimitsSection) + Send + 'static) -> Self {
        self.with_config(move |c| f(&mut c.limits))
    }

    /// Edit `[ui]`.
    pub fn with_ui(self, f: impl FnOnce(&mut UiSection) + Send + 'static) -> Self {
        self.with_config(move |c| f(&mut c.ui))
    }

    /// Edit `[bulk]`.
    pub fn with_bulk(self, f: impl FnOnce(&mut BulkSection) + Send + 'static) -> Self {
        self.with_config(move |c| f(&mut c.bulk))
    }

    /// Any other config edit (applied in call order, after the defaults).
    pub fn with_config(mut self, f: impl FnOnce(&mut ServerConfig) + Send + 'static) -> Self {
        self.inner = self.inner.with_config(f);
        self
    }

    /// Start the server.
    pub async fn start(self) -> P11Server {
        P11Server {
            server: self.inner.start().await,
        }
    }
}

// ---------------------------------------------------------------------------------
// REST helpers (routes from 11-09)
// ---------------------------------------------------------------------------------

/// On [`P11Server`] (not [`TestServer`]): `wait_status` shadows the record-level
/// `TestServer::wait_status`, which stays reachable through `server.server`.
impl P11Server {
    /// `POST /v1/runs/{run_id}/rerun` with `body` (`from`, `version`, `dry_run`, ...):
    /// `(status, body)`.
    pub async fn rerun(&self, run_id: &str, body: &Value) -> (StatusCode, Value) {
        self.post_json(&format!("/v1/runs/{run_id}/rerun"), body)
            .await
    }

    /// `GET /v1/dlq?{query}` (`query` may be empty): `(status, body)`.
    pub async fn dlq_list(&self, query: &str) -> (StatusCode, Value) {
        let path = if query.is_empty() {
            "/v1/dlq".to_owned()
        } else {
            format!("/v1/dlq?{query}")
        };
        self.get_json(&path).await
    }

    /// `POST /v1/bulk` with `{action, filter, ...}`: `(status, body)`.
    pub async fn bulk_create(&self, body: &Value) -> (StatusCode, Value) {
        self.post_json("/v1/bulk", body).await
    }

    /// Poll `GET /v1/bulk/{job_id}` until its `state` is terminal (`done`, `cancelled`,
    /// `failed`); returns the job body.
    pub async fn bulk_wait(&self, job_id: &str, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            let (st, v) = self.get_json(&format!("/v1/bulk/{job_id}")).await;
            if st == StatusCode::OK
                && matches!(v["state"].as_str(), Some("done" | "cancelled" | "failed"))
            {
                return v;
            }
            assert!(
                Instant::now() < deadline,
                "bulk job {job_id} not finished within {limit:?}: {st} {v}"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    /// Poll `GET /v1/runs/{run_id}` (REST) until its `status` is `status` (snake_case
    /// wire name); returns the run body.
    pub async fn wait_status(&self, run_id: &str, status: &str, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            let (st, v) = self.get_json(&format!("/v1/runs/{run_id}")).await;
            if st == StatusCode::OK && v["status"] == status {
                return v;
            }
            assert!(
                Instant::now() < deadline,
                "run {run_id} not {status} within {limit:?}: {st} {v}"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }
}

/// `{"seq": n}` (a `RerunFrom`).
pub fn from_seq(seq: u64) -> Value {
    json!({ "seq": seq })
}

/// `{"step": {"name": name, "occurrence": occurrence}}` (a `RerunFrom`).
pub fn from_step(name: &str, occurrence: u32) -> Value {
    json!({ "step": { "name": name, "occurrence": occurrence } })
}
