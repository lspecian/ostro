//! Phase 8 test helpers (08-10): a [`Phase8Server`] builder with Phase 8 config overrides
//! and an OTLP collector hookup, webhook signers (Stripe, GitHub, Slack; checked against
//! ostro-triggers' verifier in `tests/phase8_smoke.rs`), and REST helpers for schedules,
//! webhooks, hooks and run status. Later plans use these without editing
//! `tests/common/mod.rs`.

use std::ops::{Deref, DerefMut};
use std::path::Path;
use std::time::Duration;

use hmac::digest::{KeyInit, Mac};
use ostro_server::config::{FriendlyDuration, GuestLogsSection, ServerConfig, TriggersSection};
use ostro_testkit::otlp::Collector;
use serde_json::Value;
use tokio::time::{Instant, sleep};

use super::{ServerOpts, TestServer, server_config, tmpfs_dir};

type Tweak = Box<dyn FnOnce(&mut ServerConfig) + Send>;

/// Builder of a [`Phase8Server`] (`Phase8Server::builder()`).
pub struct Phase8Builder {
    opts: ServerOpts,
    collector: bool,
    tweaks: Vec<Tweak>,
}

/// A [`TestServer`] started with Phase 8 overrides, plus the OTLP collector when asked
/// for. Derefs to the [`TestServer`].
pub struct Phase8Server {
    pub server: TestServer,
    /// The in-process OTLP collector (`with_telemetry_collector`).
    pub collector: Option<Collector>,
}

impl Deref for Phase8Server {
    type Target = TestServer;
    fn deref(&self) -> &TestServer {
        &self.server
    }
}

impl DerefMut for Phase8Server {
    fn deref_mut(&mut self) -> &mut TestServer {
        &mut self.server
    }
}

impl Phase8Server {
    /// Defaults: P=4, persistent tmpfs data dir, no OTLP.
    pub fn builder() -> Phase8Builder {
        Phase8Builder {
            opts: ServerOpts::default(),
            collector: false,
            tweaks: Vec::new(),
        }
    }

    /// The collector (panics when the builder did not ask for one).
    pub fn collector(&self) -> &Collector {
        self.collector
            .as_ref()
            .expect("built with with_telemetry_collector()")
    }
}

impl Phase8Builder {
    /// Partition count.
    pub fn partitions(mut self, p: u16) -> Self {
        self.opts.partitions = p;
        self
    }

    /// Start an in-process OTLP collector (08-04 testkit) and point
    /// `[telemetry] otlp_endpoint` at it. A process installs one global subscriber:
    /// use this in one test per test binary.
    pub fn with_telemetry_collector(mut self) -> Self {
        self.collector = true;
        self
    }

    /// `[backup] url = "file://<dir>"`.
    pub fn with_backup_dir(self, dir: &Path) -> Self {
        let url = format!("file://{}", dir.display());
        self.with_config(move |c| c.backup.url = Some(url))
    }

    /// `[snapshots]`: `interval` (and `min_interval`) and the byte threshold.
    pub fn with_snapshot_thresholds(self, interval: Duration, bytes: u64) -> Self {
        self.with_config(move |c| {
            c.snapshots.enabled = true;
            c.snapshots.interval = FriendlyDuration(interval);
            c.snapshots.min_interval = FriendlyDuration(interval.min(c.snapshots.min_interval.0));
            c.snapshots.bytes_threshold = bytes;
        })
    }

    /// Edit `[guest_logs]`.
    pub fn with_guest_logs(self, f: impl FnOnce(&mut GuestLogsSection) + Send + 'static) -> Self {
        self.with_config(move |c| f(&mut c.guest_logs))
    }

    /// Edit `[triggers]`.
    pub fn with_triggers(self, f: impl FnOnce(&mut TriggersSection) + Send + 'static) -> Self {
        self.with_config(move |c| f(&mut c.triggers))
    }

    /// Any other config edit (applied in call order, after the defaults).
    pub fn with_config(mut self, f: impl FnOnce(&mut ServerConfig) + Send + 'static) -> Self {
        self.tweaks.push(Box::new(f));
        self
    }

    /// Start the server.
    pub async fn start(self) -> Phase8Server {
        let collector = if self.collector {
            Some(Collector::start().await)
        } else {
            None
        };
        let dir = tmpfs_dir();
        let mut cfg = server_config(dir.path(), &self.opts);
        if let Some((addr, _)) = &collector {
            cfg.telemetry.otlp_endpoint = Some(format!("http://{addr}"));
        }
        for t in self.tweaks {
            t(&mut cfg);
        }
        let server = TestServer::start_cfg(cfg, Some(dir)).await;
        Phase8Server {
            server,
            collector: collector.map(|(_, c)| c),
        }
    }
}

// ---------------------------------------------------------------------------------
// Webhook signers
// ---------------------------------------------------------------------------------

/// HMAC-SHA256 of `parts` (concatenated) under `key`.
pub fn hmac_sha256(key: &[u8], parts: &[&[u8]]) -> Vec<u8> {
    let mut m = hmac::Hmac::<sha2::Sha256>::new_from_slice(key).expect("any key length");
    for p in parts {
        m.update(p);
    }
    m.finalize().into_bytes().to_vec()
}

/// Lowercase hex.
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Stripe: `stripe-signature: t=<ts>,v1=<hex hmac("<ts>.<body>")>`.
pub fn sign_stripe(secret: &[u8], ts: i64, body: &[u8]) -> Vec<(String, String)> {
    let t = ts.to_string();
    let sig = hex(&hmac_sha256(secret, &[t.as_bytes(), b".", body]));
    vec![("stripe-signature".into(), format!("t={t},v1={sig}"))]
}

/// GitHub: `x-hub-signature-256: sha256=<hex hmac(body)>`.
pub fn sign_github(secret: &[u8], body: &[u8]) -> Vec<(String, String)> {
    let sig = hex(&hmac_sha256(secret, &[body]));
    vec![("x-hub-signature-256".into(), format!("sha256={sig}"))]
}

/// Slack: `x-slack-request-timestamp: <ts>`, `x-slack-signature: v0=<hex hmac("v0:<ts>:<body>")>`.
pub fn sign_slack(secret: &[u8], ts: i64, body: &[u8]) -> Vec<(String, String)> {
    let t = ts.to_string();
    let sig = hex(&hmac_sha256(secret, &[b"v0:", t.as_bytes(), b":", body]));
    vec![
        ("x-slack-request-timestamp".into(), t),
        ("x-slack-signature".into(), format!("v0={sig}")),
    ]
}

// ---------------------------------------------------------------------------------
// REST helpers
// ---------------------------------------------------------------------------------

impl TestServer {
    /// `PUT path` with a JSON body: `(status, body)`.
    pub async fn put_json(&self, path: &str, body: &Value) -> (reqwest::StatusCode, Value) {
        let r = self
            .http
            .put(self.url(path))
            .json(body)
            .send()
            .await
            .expect("PUT");
        decode(r).await
    }

    /// `DELETE path`: `(status, body)`.
    pub async fn delete_json(&self, path: &str) -> (reqwest::StatusCode, Value) {
        let r = self
            .http
            .delete(self.url(path))
            .send()
            .await
            .expect("DELETE");
        decode(r).await
    }

    /// `POST /v1/schedules`: `(status, body)`.
    pub async fn create_schedule(&self, schedule: &Value) -> (reqwest::StatusCode, Value) {
        self.post_json("/v1/schedules", schedule).await
    }

    /// `POST /v1/webhooks`: `(status, body)`.
    pub async fn create_webhook(&self, webhook: &Value) -> (reqwest::StatusCode, Value) {
        self.post_json("/v1/webhooks", webhook).await
    }

    /// `POST /hooks/{name}` with the raw `body` and `headers`: `(status, content type,
    /// body bytes)`.
    pub async fn deliver_hook(
        &self,
        name: &str,
        headers: &[(String, String)],
        body: &[u8],
    ) -> (reqwest::StatusCode, String, bytes::Bytes) {
        let mut req = self
            .http
            .post(self.url(&format!("/hooks/{name}")))
            .header("content-type", "application/json")
            .body(body.to_vec());
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        let r = req.send().await.expect("POST hook");
        let st = r.status();
        let ct = r
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_owned();
        (st, ct, r.bytes().await.expect("body"))
    }

    /// Poll `GET /v1/runs/{run_id}` until its `status` is `status` (snake_case wire name);
    /// returns the run body.
    pub async fn wait_run_status(&self, run_id: &str, status: &str, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            let (st, v) = self.get_json(&format!("/v1/runs/{run_id}")).await;
            if st == reqwest::StatusCode::OK && v["status"] == status {
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

async fn decode(r: reqwest::Response) -> (reqwest::StatusCode, Value) {
    let st = r.status();
    let b = r.bytes().await.expect("body");
    let v = if b.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&b)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&b).into()))
    };
    (st, v)
}
