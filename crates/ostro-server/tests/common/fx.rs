//! API-based Phase 6 helpers (plan 06-16) for the effects e2e plans (06-17..06-19).
//!
//! Unlike the 06-14 helpers in `common/mod.rs` (`put_egress`, `put_grant`,
//! `put_sealed_secret`, `put_connection`), which write meta records straight through the
//! server's meta handle, everything here goes through the public REST API, exactly as an
//! operator (or `ostro secret|grant|connection|egress`) would:
//!
//! ```ignore
//! let s = start_server_with_bridge(tmpfs_dir(), BridgeTestOpts::default()).await;
//! let module = deploy_fx(&s, "fx-http").await;               // module name = "fx-http"
//! configure(&s)
//!     .secret("stripe/live", CANARY)                          // PUT /v1/secrets/{name}
//!     .grant(&module, "stripe/live", &[&mock.base_url()], false) // PUT /v1/grants
//!     .connection("orders-db", "sql-postgres", json!({"v": 1, ..})) // PUT /v1/connections/{name}
//!     .egress(&module, &[&mock.base_url(), "connection:orders-db"]) // PUT /v1/egress/{module}
//!     .apply()
//!     .await;                                                  // panics on any non-200
//! let run = start_fx_run(&s, "fx-http", "fx-http-pay", json!({..})).await;
//! let info = wait_terminal(&s, &run, Duration::from_secs(30)).await; // RunInfo (RunDto)
//! let events = history(&s, &run).await;                             // every page
//! ```
//!
//! Egress policies and grants are keyed by the tenant-qualified deployed module name, so
//! a guest must be deployed with a name ([`deploy_fx`], `TestServer::deploy_named`)
//! before its policy matters; `deploy_guest` uploads without a name.
#![allow(dead_code)]

use std::time::Duration;

use ostro_api::rest::{HistoryEventDto, HistoryResponse, RunDto};
use ostro_types::RunId;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

use super::TestServer;

/// What [`wait_terminal`] returns: the REST `GET /v1/runs/{id}` body.
pub type RunInfo = RunDto;

/// One `PUT` of a [`Configure`] batch.
#[derive(Clone, Debug)]
enum Item {
    Secret {
        tenant: Option<String>,
        name: String,
        value: Vec<u8>,
    },
    Grant(Value),
    Connection {
        name: String,
        body: Value,
    },
    Egress {
        module: String,
        allow: Vec<String>,
    },
}

/// A batch of bridge resources applied over REST in insertion order ([`configure`]).
#[must_use = "nothing is sent until .apply().await"]
pub struct Configure<'a> {
    server: &'a TestServer,
    items: Vec<Item>,
}

/// Start a configuration batch for `server`.
pub fn configure(server: &TestServer) -> Configure<'_> {
    Configure {
        server,
        items: Vec::new(),
    }
}

/// Percent-encode one path segment (everything but unreserved characters).
pub fn seg(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

impl Configure<'_> {
    /// A global (default-tenant) secret; the value must be at least 8 bytes.
    pub fn secret(mut self, name: &str, value: impl AsRef<[u8]>) -> Self {
        self.items.push(Item::Secret {
            tenant: None,
            name: name.to_owned(),
            value: value.as_ref().to_vec(),
        });
        self
    }

    /// A secret of `tenant`.
    pub fn tenant_secret(mut self, tenant: &str, name: &str, value: impl AsRef<[u8]>) -> Self {
        self.items.push(Item::Secret {
            tenant: Some(tenant.to_owned()),
            name: name.to_owned(),
            value: value.as_ref().to_vec(),
        });
        self
    }

    /// Allow `module` to send `secret` to `origins` (origin patterns), in the body too
    /// when `body`.
    pub fn grant(mut self, module: &str, secret: &str, origins: &[&str], body: bool) -> Self {
        self.items.push(Item::Grant(json!({
            "module": module,
            "secret": secret,
            "origins": origins,
            "body": body,
        })));
        self
    }

    /// A connection (`kind`: `http-target`, `sql-postgres`, `sql-mysql`, `object`;
    /// `config`: its JSON config with `"v": 1`).
    pub fn connection(mut self, name: &str, kind: &str, config: Value) -> Self {
        self.items.push(Item::Connection {
            name: name.to_owned(),
            body: json!({ "kind": kind, "config": config }),
        });
        self
    }

    /// The egress allow list of `module` (origin patterns, IP rules,
    /// `connection:<name>`).
    pub fn egress(mut self, module: &str, allow: &[&str]) -> Self {
        self.items.push(Item::Egress {
            module: module.to_owned(),
            allow: allow.iter().map(|a| (*a).to_owned()).collect(),
        });
        self
    }

    /// Send every item (`PUT`), panicking with the server's answer on any non-200.
    /// Error texts never contain a secret value (the server never echoes one).
    pub async fn apply(self) {
        let s = self.server;
        for item in self.items {
            let (what, req) = match item {
                Item::Secret {
                    tenant,
                    name,
                    value,
                } => {
                    let mut url = s.url(&format!("/v1/secrets/{}", seg(&name)));
                    if let Some(t) = &tenant {
                        url.push_str(&format!("?tenant={}", seg(t)));
                    }
                    let body = match String::from_utf8(value) {
                        Ok(text) => json!({ "value": text }),
                        Err(e) => json!({ "value_b64": b64(e.as_bytes()) }),
                    };
                    (format!("secret {name}"), s.http.put(url).json(&body))
                }
                Item::Grant(body) => (
                    format!("grant {}:{}", body["module"], body["secret"]),
                    s.http.put(s.url("/v1/grants")).json(&body),
                ),
                Item::Connection { name, body } => (
                    format!("connection {name}"),
                    s.http
                        .put(s.url(&format!("/v1/connections/{}", seg(&name))))
                        .json(&body),
                ),
                Item::Egress { module, allow } => (
                    format!("egress {module}"),
                    s.http
                        .put(s.url(&format!("/v1/egress/{}", seg(&module))))
                        .json(&json!({ "allow": allow })),
                ),
            };
            let r = req.send().await.unwrap_or_else(|e| panic!("{what}: {e}"));
            let st = r.status();
            let text = r.text().await.unwrap_or_default();
            assert_eq!(st, reqwest::StatusCode::OK, "{what}: {text}");
        }
    }
}

/// Standard padded base64 (binary secret values travel as `value_b64`).
fn b64(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// Deploy test guest `guest` under the module name `guest` (default tenant) and return
/// that module name (what grants and egress policies are keyed by). Idempotent.
pub async fn deploy_fx(server: &TestServer, guest: &str) -> String {
    server.deploy_named(guest, guest, None).await
}

/// Deploy `guest` ([`deploy_fx`]) and start `workflow` with `input` over REST
/// (`POST /v1/workflows/{workflow}/runs`, expecting 201). Returns the run id.
pub async fn start_fx_run(server: &TestServer, guest: &str, workflow: &str, input: Value) -> RunId {
    deploy_fx(server, guest).await;
    server.start_rest(workflow, json!({ "input": input })).await
}

/// Whether a REST run status is terminal.
pub fn is_terminal(info: &RunInfo) -> bool {
    matches!(
        serde_json::to_value(info.status)
            .ok()
            .as_ref()
            .and_then(Value::as_str),
        Some(
            "completed" | "failed" | "cancelled" | "terminated" | "timed_out" | "continued_as_new"
        )
    )
}

/// Poll `GET /v1/runs/{id}` every 50 ms until the run is terminal; panics after
/// `limit` with the last body.
pub async fn wait_terminal(server: &TestServer, run: &RunId, limit: Duration) -> RunInfo {
    let deadline = Instant::now() + limit;
    loop {
        let (st, v) = server.get_json(&format!("/v1/runs/{run}")).await;
        assert_eq!(st, reqwest::StatusCode::OK, "GET run {run}: {v}");
        let info: RunInfo = serde_json::from_value(v.clone()).expect("RunDto");
        if is_terminal(&info) {
            return info;
        }
        assert!(
            Instant::now() < deadline,
            "run {run} not terminal after {limit:?}: {v}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// The run's whole decoded history over REST (`GET /v1/runs/{id}/history`, every page).
/// For the raw journal events use `TestServer::journal`.
pub async fn history(server: &TestServer, run: &RunId) -> Vec<HistoryEventDto> {
    let mut out = Vec::new();
    let mut from = 0u64;
    loop {
        let (st, v) = server
            .get_json(&format!(
                "/v1/runs/{run}/history?from_seq={from}&limit=1000"
            ))
            .await;
        assert_eq!(st, reqwest::StatusCode::OK, "history {run}: {v}");
        let page: HistoryResponse = serde_json::from_value(v).expect("HistoryResponse");
        out.extend(page.events);
        match page.next_from_seq {
            Some(n) if n > from => from = n,
            _ => return out,
        }
    }
}

/// The history events of kind `kind` (REST spelling, e.g. `"effect_completed"`).
pub fn events_of<'a>(events: &'a [HistoryEventDto], kind: &str) -> Vec<&'a HistoryEventDto> {
    events.iter().filter(|e| e.kind == kind).collect()
}
