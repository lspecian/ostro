//! Typed REST client for the Ostro server (CLI-06).
//!
//! Every method returns the response body as a [`serde_json::Value`], so `--json` can
//! print it verbatim. Non-2xx answers become [`CallError::Api`] with the server's error
//! body preserved (`{"error": {"code", "message", "details"}}`). Transport only:
//! plain HTTP/1.1 + h2c via reqwest, no TLS until SEC-01.

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
}

/// Parse a `--server` value: `http://host:port[/prefix]`, or `host:port` (http implied).
pub fn parse_server(s: &str) -> Result<Url, String> {
    let s = s.trim();
    let with_scheme = if s.contains("://") {
        s.to_owned()
    } else {
        format!("http://{s}")
    };
    let url = Url::parse(&with_scheme).map_err(|e| format!("invalid server URL {s:?}: {e}"))?;
    match url.scheme() {
        "http" => {}
        "https" => {
            return Err(format!(
                "invalid server URL {s:?}: https is not supported yet (TLS lands with SEC-01); \
                 use http://"
            ));
        }
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

impl Client {
    /// A client for `server` (see [`parse_server`]); `timeout` bounds every request (a
    /// long-poll gets its wait added on top).
    pub fn new(server: &str, timeout: Duration) -> Result<Self, CallError> {
        let base = parse_server(server).map_err(CallError::Usage)?;
        let http = reqwest::Client::builder()
            .no_proxy()
            .user_agent(concat!("ostro-cli/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| CallError::Transport(format!("cannot build the HTTP client: {e}")))?;
        Ok(Self {
            base,
            http,
            timeout,
        })
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

    fn request(&self, method: Method, url: Url) -> RequestBuilder {
        self.http.request(method, url).timeout(self.timeout)
    }

    async fn send(&self, rb: RequestBuilder) -> Result<Response, CallError> {
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

    async fn get(
        &self,
        segments: &[&str],
        query: &[(&str, String)],
    ) -> Result<Response, CallError> {
        self.send(self.request(Method::GET, self.url(segments, query)))
            .await
    }

    async fn post(&self, segments: &[&str], body: &Value) -> Result<Response, CallError> {
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
        let e = parse_server("https://example.com").unwrap_err();
        assert!(e.contains("https is not supported"), "{e}");
        assert!(parse_server("ftp://x").is_err());
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
