//! Typed REST client for the Ostro server (CLI-06).
//!
//! Every method returns the response body as a [`serde_json::Value`], so `--json` can
//! print it verbatim. Non-2xx answers become [`CallError::Api`] with the server's error
//! body preserved (`{"error": {"code", "message", "details"}}`). Transport: HTTP/1.1 and
//! h2 via reqwest; `https://` servers (SEC-01, 07-22) are verified against the platform
//! roots, or only against `--ca-file` (for example the cluster CA `<data_dir>/tls/ca.crt`
//! that issues the self-signed listener certificate, D-W20).

use std::path::Path;
use std::time::Duration;

use reqwest::{Method, RequestBuilder, Url};
use serde_json::{Value, json};

/// The default server address (`ostro dev` listens here).
pub const DEFAULT_SERVER: &str = "http://127.0.0.1:7700";

/// The server caps one long-poll at 60 s; longer waits are a loop of polls.
pub const MAX_POLL: Duration = Duration::from_secs(60);

/// Why a call failed.
#[derive(Debug)]
pub enum CallError {
    /// Bad local input discovered after parsing (exit 2).
    Usage(String),
    /// The server could not be reached, or the request timed out (exit 1).
    Transport(String),
    /// The server answered with a non-2xx status (exit 1). `body` is the server's JSON
    /// error body, or `{"error": {"code": "http_<status>", "message": <text>}}` when the
    /// body was not JSON.
    Api {
        /// HTTP status.
        status: u16,
        /// Error body.
        body: Value,
    },
}

impl CallError {
    /// One human-readable line.
    pub fn message(&self) -> String {
        match self {
            CallError::Usage(m) | CallError::Transport(m) => m.clone(),
            CallError::Api { status, body } => {
                let err = &body["error"];
                let msg = err["message"].as_str().unwrap_or("request failed");
                let code = err["code"].as_str().unwrap_or("error");
                match err["details"]["reason"].as_str() {
                    Some(reason) => format!("{msg} (HTTP {status} {code}, reason {reason})"),
                    None => format!("{msg} (HTTP {status} {code})"),
                }
            }
        }
    }
}

/// A successful answer: the HTTP status and the JSON body.
#[derive(Debug, Clone)]
pub struct Response {
    /// HTTP status (200 or 201).
    pub status: u16,
    /// JSON body.
    pub body: Value,
}

/// REST client bound to one server.
#[derive(Debug, Clone)]
pub struct Client {
    base: Url,
    http: reqwest::Client,
    timeout: Duration,
    /// Phase 14 (14-01b): the bearer sent on every request (`OSTRO_TOKEN` by default).
    bearer: Option<reqwest::header::HeaderValue>,
}

/// Parse a `--server` value: `http(s)://host:port[/prefix]`, or `host:port` (http implied).
pub fn parse_server(s: &str) -> Result<Url, String> {
    let s = s.trim();
    let with_scheme = if s.contains("://") {
        s.to_owned()
    } else {
        format!("http://{s}")
    };
    let url = Url::parse(&with_scheme).map_err(|e| format!("invalid server URL {s:?}: {e}"))?;
    match url.scheme() {
        "http" | "https" => {}
        other => {
            return Err(format!(
                "invalid server URL {s:?}: unsupported scheme {other}"
            ));
        }
    }
    if url.cannot_be_a_base() || url.host_str().is_none() {
        return Err(format!("invalid server URL {s:?}: no host"));
    }
    Ok(url)
}

/// The PEM certificates in `path` (at least one).
pub fn ca_certs(path: &Path) -> Result<Vec<reqwest::Certificate>, String> {
    let pem = std::fs::read(path).map_err(|e| format!("--ca-file {}: {e}", path.display()))?;
    let certs = reqwest::Certificate::from_pem_bundle(&pem)
        .map_err(|e| format!("--ca-file {}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!(
            "--ca-file {}: no PEM certificate found",
            path.display()
        ));
    }
    Ok(certs)
}

impl Client {
    /// A client for `server` (see [`parse_server`]); `timeout` bounds every request (a
    /// long-poll gets its wait added on top).
    pub fn new(server: &str, timeout: Duration) -> Result<Self, CallError> {
        Self::with_ca(server, timeout, None)
    }

    /// [`Client::new`] trusting only the PEM CA certificate(s) in `ca_file` for `https://`
    /// (`None` = the platform roots).
    pub fn with_ca(
        server: &str,
        timeout: Duration,
        ca_file: Option<&Path>,
    ) -> Result<Self, CallError> {
        let base = parse_server(server).map_err(CallError::Usage)?;
        let mut builder = reqwest::Client::builder()
            .no_proxy()
            .user_agent(concat!("ostro-cli/", env!("CARGO_PKG_VERSION")));
        if let Some(path) = ca_file {
            builder = builder.tls_certs_only(ca_certs(path).map_err(CallError::Usage)?);
        }
        let http = builder
            .build()
            .map_err(|e| CallError::Transport(format!("cannot build the HTTP client: {e}")))?;
        Ok(Self {
            base,
            http,
            timeout,
            bearer: crate::bearer_from_env().and_then(|t| crate::authorization(&t)),
        })
    }

    /// Send `token` as `Authorization: Bearer <token>` (`None`: no header), replacing the
    /// `OSTRO_TOKEN` default.
    pub fn with_bearer(mut self, token: Option<&str>) -> Self {
        self.bearer = token.and_then(crate::authorization);
        self
    }

    /// The server base URL.
    pub fn base(&self) -> &Url {
        &self.base
    }

    /// `base` + percent-encoded path segments (a `/` inside a segment becomes `%2F`, so
    /// tenant-qualified names stay one segment) + query pairs.
    pub fn url(&self, segments: &[&str], query: &[(&str, String)]) -> Url {
        let mut url = self.base.clone();
        {
            // `parse_server` rejected cannot-be-a-base URLs.
            let mut p = url.path_segments_mut().expect("base URL");
            p.pop_if_empty();
            p.extend(segments);
        }
        url.set_query(None);
        if !query.is_empty() {
            let mut q = url.query_pairs_mut();
            for (k, v) in query {
                q.append_pair(k, v);
            }
        }
        url
    }

    pub(crate) fn request(&self, method: Method, url: Url) -> RequestBuilder {
        let r = self.http.request(method, url).timeout(self.timeout);
        match &self.bearer {
            Some(b) => r.header(reqwest::header::AUTHORIZATION, b.clone()),
            None => r,
        }
    }

    pub(crate) async fn send(&self, rb: RequestBuilder) -> Result<Response, CallError> {
        let resp = rb.send().await.map_err(|e| self.transport(e))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(|e| self.transport(e))?;
        let body = if text.trim().is_empty() {
            Value::Null
        } else {
            match serde_json::from_str::<Value>(&text) {
                Ok(v) => v,
                Err(_) if (200..300).contains(&status) => {
                    return Err(CallError::Transport(format!(
                        "the server at {} answered HTTP {status} with a non-JSON body",
                        self.base
                    )));
                }
                Err(_) => json!({ "error": {
                    "code": format!("http_{status}"),
                    "message": text.trim(),
                }}),
            }
        };
        if (200..300).contains(&status) {
            Ok(Response { status, body })
        } else {
            let body = if body["error"].is_object() {
                body
            } else {
                json!({ "error": { "code": format!("http_{status}"), "message": body.to_string() } })
            };
            Err(CallError::Api { status, body })
        }
    }

    fn transport(&self, e: reqwest::Error) -> CallError {
        let mut msg = if e.is_timeout() {
            format!("request to the Ostro server at {} timed out", self.base)
        } else if e.is_connect() {
            format!("cannot reach the Ostro server at {}", self.base)
        } else {
            format!("request to the Ostro server at {} failed", self.base)
        };
        // reqwest's Display hides the cause (connection refused, ...); append the chain.
        let mut src: Option<&dyn std::error::Error> = Some(&e);
        let mut last = String::new();
        while let Some(s) = src {
            let part = s.to_string();
            if part != last {
                msg.push_str(": ");
                msg.push_str(&part);
                last = part;
            }
            src = s.source();
        }
        CallError::Transport(msg)
    }

    pub(crate) async fn get(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
    ) -> Result<Response, CallError> {
        self.send(self.request(Method::GET, self.url(segments, query)))
            .await
    }

    pub(crate) async fn post(
        &self,
        segments: &[&str],
        body: &Value,
    ) -> Result<Response, CallError> {
        self.send(
            self.request(Method::POST, self.url(segments, &[]))
                .json(body),
        )
        .await
    }

    // --- modules / workflows ----------------------------------------------------------

    /// `PUT /v1/modules/{sha256}?name=&tenant=` with the raw Wasm bytes (201 created,
    /// 200 unchanged).
    pub async fn upload_module(
        &self,
        sha256: &str,
        wasm: Vec<u8>,
        name: Option<&str>,
        tenant: Option<&str>,
    ) -> Result<Response, CallError> {
        let mut q = Vec::new();
        if let Some(n) = name {
            q.push(("name", n.to_owned()));
        }
        if let Some(t) = tenant {
            q.push(("tenant", t.to_owned()));
        }
        let rb = self
            .request(Method::PUT, self.url(&["v1", "modules", sha256], &q))
            .header(reqwest::header::CONTENT_TYPE, "application/wasm")
            .body(wasm);
        self.send(rb).await
    }

    /// `GET /v1/workflows`.
    pub async fn list_workflows(&self) -> Result<Response, CallError> {
        self.get(&["v1", "workflows"], &[]).await
    }

    /// `PUT /v1/workflows/{name}/active {version}`.
    pub async fn activate(&self, name: &str, version: u64) -> Result<Response, CallError> {
        let rb = self
            .request(
                Method::PUT,
                self.url(&["v1", "workflows", name, "active"], &[]),
            )
            .json(&json!({ "version": version }));
        self.send(rb).await
    }

    // --- runs -------------------------------------------------------------------------

    /// `POST /v1/workflows/{workflow}/runs`.
    pub async fn start_run(&self, workflow: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "workflows", workflow, "runs"], body)
            .await
    }

    /// `GET /v1/runs?...`.
    pub async fn list_runs(&self, query: &[(&str, String)]) -> Result<Response, CallError> {
        self.get(&["v1", "runs"], query).await
    }

    /// `GET /v1/runs/{id}`.
    pub async fn get_run(&self, run_id: &str) -> Result<Response, CallError> {
        self.get(&["v1", "runs", run_id], &[]).await
    }

    /// `GET /v1/runs/{id}/history?from_seq&limit`.
    pub async fn history(
        &self,
        run_id: &str,
        from_seq: Option<u64>,
        limit: Option<u32>,
    ) -> Result<Response, CallError> {
        let mut q = Vec::new();
        if let Some(f) = from_seq {
            q.push(("from_seq", f.to_string()));
        }
        if let Some(l) = limit {
            q.push(("limit", l.to_string()));
        }
        self.get(&["v1", "runs", run_id, "history"], &q).await
    }

    // --- lifecycle --------------------------------------------------------------------

    /// `POST /v1/runs/{id}/signals/{name}` `{payload, request_id, dry_run}`.
    pub async fn signal(
        &self,
        run_id: &str,
        name: &str,
        body: &Value,
    ) -> Result<Response, CallError> {
        self.post(&["v1", "runs", run_id, "signals", name], body)
            .await
    }

    /// `POST /v1/runs/{id}/cancel` `{reason, dry_run}`.
    pub async fn cancel(&self, run_id: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "runs", run_id, "cancel"], body).await
    }

    /// `POST /v1/runs/{id}/terminate` `{reason, dry_run}`.
    pub async fn terminate(&self, run_id: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "runs", run_id, "terminate"], body).await
    }

    /// `POST /v1/runs/{id}/retry` `{dry_run}`.
    pub async fn retry(&self, run_id: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "runs", run_id, "retry"], body).await
    }

    /// One long-poll: `GET /v1/runs/{id}/result?wait=&follow_continuations=` (always 200
    /// for a known run; `completed: false` when the wait ended first). `wait` is capped
    /// at [`MAX_POLL`].
    pub async fn result_once(
        &self,
        run_id: &str,
        wait: Duration,
        follow: bool,
    ) -> Result<Response, CallError> {
        let wait = wait.min(MAX_POLL);
        let mut q = vec![("wait", format!("{}ms", wait.as_millis()))];
        if follow {
            q.push(("follow_continuations", "true".to_owned()));
        }
        let rb = self
            .http
            .get(self.url(&["v1", "runs", run_id, "result"], &q))
            .timeout(self.timeout + wait);
        self.send(rb).await
    }

    /// `POST /v1/tokens/complete` `{token, payload, dry_run}` (the token travels only in
    /// the body).
    pub async fn complete_token(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "tokens", "complete"], body).await
    }

    // --- generic (Phase 8 verbs, 08-12) -----------------------------------------------

    /// One JSON request: `method base/segments?query` with an optional JSON body. The
    /// Phase 8 verbs (`ostro schedules`, `ostro webhooks`) build their routes on this.
    pub async fn call(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, String)],
        body: Option<&Value>,
    ) -> Result<Response, CallError> {
        let mut rb = self.request(method, self.url(segments, query));
        if let Some(b) = body {
            rb = rb.json(b);
        }
        self.send(rb).await
    }
}

// --- Phase 11 (11-14) ---------------------------------------------------------------
// Run operations (OPS-02..05, OPS-07): rerun, DLQ, bulk jobs, limits. One contiguous
// block so Phase 10's client extraction (10-09) can move it verbatim.

impl Client {
    /// `POST /v1/runs/{id}/rerun` `{from, version, idempotency_key, dry_run}` (201 new
    /// run; 200 dry run or `existing`).
    pub async fn rerun(&self, run_id: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "runs", run_id, "rerun"], body).await
    }

    /// `GET /v1/runs/{id}/rerun-cuts`.
    pub async fn rerun_cuts(&self, run_id: &str) -> Result<Response, CallError> {
        self.get(&["v1", "runs", run_id, "rerun-cuts"], &[]).await
    }

    /// `GET /v1/dlq?workflow&queue&reason&tags*&include_dismissed&limit&page_token`.
    pub async fn list_dlq(&self, query: &[(&str, String)]) -> Result<Response, CallError> {
        self.get(&["v1", "dlq"], query).await
    }

    /// `POST /v1/dlq/{id}/requeue` `{mode, dry_run}`.
    pub async fn requeue_dlq(&self, run_id: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "dlq", run_id, "requeue"], body).await
    }

    /// `POST /v1/dlq/{id}/dismiss` `{reason}`.
    pub async fn dismiss_dlq(&self, run_id: &str, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "dlq", run_id, "dismiss"], body).await
    }

    /// `POST /v1/bulk` `{action, filter, rate_per_sec, max_in_flight, dry_run}` (201 job;
    /// 200 dry run).
    pub async fn create_bulk(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "bulk"], body).await
    }

    /// `GET /v1/bulk`.
    pub async fn list_bulk(&self) -> Result<Response, CallError> {
        self.get(&["v1", "bulk"], &[]).await
    }

    /// `GET /v1/bulk/{job_id}`.
    pub async fn get_bulk(&self, job_id: &str) -> Result<Response, CallError> {
        self.get(&["v1", "bulk", job_id], &[]).await
    }

    /// `POST /v1/bulk/{job_id}/cancel`.
    pub async fn cancel_bulk(&self, job_id: &str) -> Result<Response, CallError> {
        self.post(&["v1", "bulk", job_id, "cancel"], &json!({}))
            .await
    }

    /// `GET /v1/limits`.
    pub async fn limits(&self) -> Result<Response, CallError> {
        self.get(&["v1", "limits"], &[]).await
    }
}

// --- end Phase 11 (11-14) -----------------------------------------------------------

// --- Phase 10 (10-05) ---------------------------------------------------------------
// REST-only tooling (D-P10-18): the dev clock (SDK-06) and server-side replay (CLI-07).
// One contiguous block so the client extraction (10-09) can move it verbatim.

/// Extra time a replay request may take on top of the client timeout (the server
/// replays up to 1000 journals per request).
pub const REPLAY_EXTRA_TIMEOUT: Duration = Duration::from_secs(300);

impl Client {
    /// `GET /v1/dev/time` (409 `time_skipping_disabled` unless the server runs
    /// `ostro dev --ephemeral --time-skipping`).
    pub async fn dev_time(&self) -> Result<Response, CallError> {
        self.get(&["v1", "dev", "time"], &[]).await
    }

    /// `POST /v1/dev/time/skip` `{by}` or `{to_ms}`.
    pub async fn dev_skip(&self, body: &Value) -> Result<Response, CallError> {
        self.post(&["v1", "dev", "time", "skip"], body).await
    }

    /// `POST /v1/modules/{sha}/replay` `{runs | sample, workflow, status, allow_missing}`,
    /// with [`REPLAY_EXTRA_TIMEOUT`] on top of the client timeout.
    pub async fn replay_module(&self, sha: &str, body: &Value) -> Result<Response, CallError> {
        let rb = self
            .http
            .post(self.replay_url(sha))
            .timeout(self.timeout + REPLAY_EXTRA_TIMEOUT)
            .json(body);
        self.send(rb).await
    }

    /// The replay route of module `sha` (one path segment).
    fn replay_url(&self, sha: &str) -> Url {
        self.url(&["v1", "modules", sha, "replay"], &[])
    }
}

// --- end Phase 10 (10-05) -----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_urls() {
        assert_eq!(
            parse_server("127.0.0.1:7700").unwrap().as_str(),
            "http://127.0.0.1:7700/"
        );
        assert_eq!(
            parse_server(DEFAULT_SERVER).unwrap().as_str(),
            "http://127.0.0.1:7700/"
        );
        assert_eq!(
            parse_server("https://example.com:7700").unwrap().as_str(),
            "https://example.com:7700/"
        );
        assert!(parse_server("ftp://x").is_err());
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty.pem");
        std::fs::write(&empty, "").unwrap();
        let e = Client::with_ca("https://h:1", Duration::from_secs(1), Some(&empty)).unwrap_err();
        assert!(
            e.message().contains("no PEM certificate"),
            "{}",
            e.message()
        );
        let e = Client::with_ca(
            "https://h:1",
            Duration::from_secs(1),
            Some(&dir.path().join("missing.pem")),
        )
        .unwrap_err();
        assert!(e.message().contains("--ca-file"), "{}", e.message());
    }

    #[test]
    fn segments_are_encoded() {
        let c = Client::new("http://h:1/prefix/", Duration::from_secs(1)).unwrap();
        let u = c.url(
            &["v1", "workflows", "acme/w", "runs"],
            &[("tag", "k=v &x".to_owned())],
        );
        assert_eq!(
            u.as_str(),
            "http://h:1/prefix/v1/workflows/acme%2Fw/runs?tag=k%3Dv+%26x"
        );
        let u = c.url(&["v1", "runs"], &[]);
        assert_eq!(u.as_str(), "http://h:1/prefix/v1/runs");
    }

    #[test]
    fn phase10_urls() {
        let c = Client::new("http://h:1/prefix", Duration::from_secs(1)).unwrap();
        assert_eq!(
            c.url(&["v1", "dev", "time", "skip"], &[]).as_str(),
            "http://h:1/prefix/v1/dev/time/skip"
        );
        assert_eq!(
            c.replay_url(&"ab".repeat(32)).as_str(),
            format!("http://h:1/prefix/v1/modules/{}/replay", "ab".repeat(32))
        );
        // A hostile sha stays one segment.
        assert_eq!(
            c.replay_url("../x?y").as_str(),
            "http://h:1/prefix/v1/modules/..%2Fx%3Fy/replay"
        );
    }

    #[test]
    fn api_error_message() {
        let e = CallError::Api {
            status: 409,
            body: json!({"error": {"code": "conflict", "message": "run is terminal",
                "details": {"reason": "run_terminal", "status": "completed"}}}),
        };
        assert_eq!(
            e.message(),
            "run is terminal (HTTP 409 conflict, reason run_terminal)"
        );
    }
}
