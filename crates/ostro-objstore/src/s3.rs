//! S3-compatible backend (`s3://bucket/prefix`) over `rusty_s3` 0.12 actions and reqwest.
//!
//! - Every action is presigned with a short expiry ([`SIGN_EXPIRY`], 60 s) and re-signed on
//!   every attempt, then sent with reqwest (rustls; TLS roots per [`TlsRoots`]).
//! - Conditional headers (`if-none-match: *`, `if-match: <etag>`) are inserted through
//!   `headers_mut()` **before signing**, so they are part of `X-Amz-SignedHeaders`, and the
//!   request carries the same value. A failed condition is 412 (409 for a concurrent
//!   conditional write; `404 NoSuchKey` for `if-match` on a missing object).
//! - Retries: exponential backoff with jitter on 5xx, 429, timeouts and connection errors, at
//!   most `max_retries` (5) retries; never on 412 or any other 4xx.
//! - `put_multipart` buffers up to `multipart_threshold` (64 MiB); a shorter body is one PUT,
//!   a longer one is CreateMultipartUpload / UploadPart (`multipart_part_size`) / Complete,
//!   with AbortMultipartUpload on any error.
//! - Phase 6 (06-12): `put_with` signs and sends `content-type`; `presign` returns a
//!   `rusty_s3` `S3Action::sign` URL (validity clamped to 1 s ..= 7 d) with an optional signed
//!   `content-type` (PUT) or `response-content-type` query (GET); [`S3Store::with_client`]
//!   accepts an operator-supplied reqwest client.

use std::time::Duration;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_core::stream::BoxStream;
use futures_util::StreamExt as _;
use reqwest::header::HeaderMap;
use reqwest::{Method, StatusCode};
use rusty_s3::actions::{
    CompleteMultipartUpload, CompleteMultipartUploadResponse, CreateMultipartUpload,
    DeleteObjectsResponse, ListObjectsV2, ObjectIdentifier,
};
use rusty_s3::{Bucket, S3Action as _};

use crate::{
    Conditional, Error, MIN_MULTIPART_PART_SIZE, ObjectMeta, ObjectStore, PresignMethod,
    PresignOpts, PutOpts, PutOutcome, Result, StoreOptions, TlsRoots, UrlStyle, check_key,
    clamp_presign_expiry,
};

/// Lifetime of each presigned request URL.
pub const SIGN_EXPIRY: Duration = Duration::from_secs(60);

const DEFAULT_REGION: &str = "us-east-1";
const LIST_PAGE: usize = 1000;
const DELETE_BATCH: usize = 1000;
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// A buffered HTTP response.
struct Resp {
    status: StatusCode,
    headers: HeaderMap,
    body: Bytes,
}

impl Resp {
    fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    }

    fn into_status_error(self, op: &'static str, key: &str) -> Error {
        let mut body = String::from_utf8_lossy(&self.body).into_owned();
        if body.len() > 512 {
            let mut cut = 512;
            while !body.is_char_boundary(cut) {
                cut -= 1;
            }
            body.truncate(cut);
            body.push_str("...");
        }
        Error::Status {
            op,
            key: key.to_owned(),
            status: self.status.as_u16(),
            body,
        }
    }

    fn meta(&self, key: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            key: key.to_owned(),
            size,
            last_modified: self
                .header("last-modified")
                .and_then(|s| {
                    jiff::fmt::rfc2822::DateTimeParser::new()
                        .parse_timestamp(s.as_bytes())
                        .ok()
                })
                .unwrap_or(jiff::Timestamp::UNIX_EPOCH),
            e_tag: self.header("etag"),
            version: self.header("x-amz-version-id"),
        }
    }
}

/// S3-compatible object store (AWS, SeaweedFS, R2, GCS interop, ...).
pub struct S3Store {
    bucket: Bucket,
    credentials: Option<rusty_s3::Credentials>,
    client: reqwest::Client,
    prefix: String,
    conditional: bool,
    multipart_threshold: u64,
    multipart_part_size: u64,
    max_retries: u32,
    retry_base: Duration,
}

impl std::fmt::Debug for S3Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Store")
            .field("bucket", &self.bucket.name())
            .field("base_url", &self.bucket.base_url().as_str())
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

fn backoff(base: Duration, attempt: u32) -> Duration {
    use std::hash::{BuildHasher as _, Hasher as _};
    let exp = base
        .saturating_mul(1u32 << attempt.min(16))
        .min(MAX_BACKOFF);
    // Equal jitter: half fixed, half random (std's RandomState is the entropy source).
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u32(attempt);
    let half = u64::try_from(exp.as_nanos() / 2).unwrap_or(u64::MAX);
    Duration::from_nanos(half + h.finish() % (half + 1))
}

fn is_retryable_status(s: StatusCode) -> bool {
    s.is_server_error() || s == StatusCode::TOO_MANY_REQUESTS
}

fn is_retryable_error(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request() || e.is_body()
}

/// Normalise a key prefix: `""` stays empty, anything else ends with exactly one `/`.
pub(crate) fn normalise_prefix(prefix: &str) -> Result<String> {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        return Ok(String::new());
    }
    check_key(p)?;
    Ok(format!("{p}/"))
}

impl S3Store {
    /// A store over `bucket`, with every key prefixed by `prefix` (normalised to end in `/`).
    pub fn new(bucket: &str, prefix: &str, opts: &StoreOptions) -> Result<Self> {
        let client = Self::build_client(opts)?;
        Self::with_client(bucket, prefix, opts, client)
    }

    /// Like [`S3Store::new`], but sends every request through `client` (Phase 6: the bridge's
    /// trusted operator client). `opts.tls_roots` is ignored (the client's own roots apply);
    /// `opts.allow_http` still gates a plain-`http` endpoint.
    pub fn with_client(
        bucket: &str,
        prefix: &str,
        opts: &StoreOptions,
        client: reqwest::Client,
    ) -> Result<Self> {
        let region = opts
            .region
            .clone()
            .unwrap_or_else(|| DEFAULT_REGION.to_owned());
        let endpoint = opts
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        let endpoint: ::url::Url = endpoint
            .parse()
            .map_err(|e| Error::Config(format!("bad S3 endpoint {endpoint:?}: {e}")))?;
        match endpoint.scheme() {
            "https" => {}
            "http" if opts.allow_http => {}
            "http" => {
                return Err(Error::Config(format!(
                    "S3 endpoint {endpoint} is plain http; set allow_http to permit it"
                )));
            }
            s => {
                return Err(Error::Config(format!(
                    "unsupported S3 endpoint scheme {s:?}"
                )));
            }
        }
        let style = match opts.url_style {
            UrlStyle::Path => rusty_s3::UrlStyle::Path,
            UrlStyle::Virtual => rusty_s3::UrlStyle::VirtualHost,
        };
        let bucket = Bucket::new(endpoint, style, bucket.to_owned(), region)
            .map_err(|e| Error::Config(format!("bad S3 bucket/endpoint: {e}")))?;
        let credentials = opts.credentials.as_ref().map(|c| match &c.session_token {
            Some(t) => rusty_s3::Credentials::new_with_token(
                c.access_key_id.clone(),
                c.secret_access_key.clone(),
                t.clone(),
            ),
            None => {
                rusty_s3::Credentials::new(c.access_key_id.clone(), c.secret_access_key.clone())
            }
        });

        Ok(Self {
            bucket,
            credentials,
            client,
            prefix: normalise_prefix(prefix)?,
            conditional: opts.conditional_put,
            multipart_threshold: opts.multipart_threshold.max(1),
            multipart_part_size: opts.multipart_part_size.max(MIN_MULTIPART_PART_SIZE),
            max_retries: opts.max_retries,
            retry_base: opts.retry_base,
        })
    }

    fn build_client(opts: &StoreOptions) -> Result<reqwest::Client> {
        let mut cb = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .read_timeout(Duration::from_secs(60))
            .https_only(!opts.allow_http);
        match &opts.tls_roots {
            TlsRoots::Platform => {}
            TlsRoots::Webpki => {
                return Err(Error::Config(
                    "tls_roots = \"webpki\" is not available in this build (the bundled roots are \
                     CDLA-licensed, D-FX2); use tls_roots = \"file:<pem bundle>\" or \"platform\""
                        .into(),
                ));
            }
            TlsRoots::File(path) => {
                let pem = std::fs::read(path).map_err(|e| Error::io(path.display(), e))?;
                let certs = reqwest::Certificate::from_pem_bundle(&pem).map_err(|e| {
                    Error::Config(format!("bad PEM bundle {}: {e}", path.display()))
                })?;
                cb = cb.tls_certs_only(certs);
            }
        }
        cb.build()
            .map_err(|e| Error::Config(format!("HTTP client: {e}")))
    }

    /// The key prefix every key is stored under (empty or ending in `/`).
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn full_key(&self, key: &str) -> Result<String> {
        check_key(key)?;
        Ok(format!("{}{key}", self.prefix))
    }

    fn creds(&self) -> Option<&rusty_s3::Credentials> {
        self.credentials.as_ref()
    }

    /// Send the request `build` produces (re-signed per attempt), retrying 5xx/429/timeouts.
    /// Returns the last response, whatever its status; transport errors after the last retry
    /// are [`Error::Transport`].
    async fn send<F>(&self, build: F) -> Result<Resp>
    where
        F: Fn() -> reqwest::RequestBuilder + Send + Sync,
    {
        let mut attempt = 0u32;
        loop {
            let res = async {
                let r = build().send().await?;
                let status = r.status();
                let headers = r.headers().clone();
                let body = r.bytes().await?;
                Ok::<_, reqwest::Error>(Resp {
                    status,
                    headers,
                    body,
                })
            }
            .await;
            let retry = match &res {
                Ok(r) => is_retryable_status(r.status),
                Err(e) => is_retryable_error(e),
            };
            if !retry || attempt >= self.max_retries {
                return res.map_err(Error::Transport);
            }
            let delay = backoff(self.retry_base, attempt);
            tracing::debug!(attempt, ?delay, "object store request retry");
            // clock-exempt: transport retry backoff (a sleep, not a time read feeding logic)
            tokio::time::sleep(delay).await;
            attempt += 1;
        }
    }

    async fn put_with_condition(
        &self,
        key: &str,
        body: Bytes,
        condition: Option<(&'static str, &str)>,
    ) -> Result<Conditional<PutOutcome>> {
        let full = self.full_key(key)?;
        let mut action = self.bucket.put_object(self.creds(), &full);
        if let Some((name, value)) = condition {
            // Before signing: the header becomes part of X-Amz-SignedHeaders.
            action.headers_mut().insert(name, value);
        }
        let resp = self
            .send(|| {
                let mut rb = self.client.put(action.sign(SIGN_EXPIRY)).body(body.clone());
                if let Some((name, value)) = condition {
                    rb = rb.header(name, value);
                }
                rb
            })
            .await?;
        match resp.status {
            s if s.is_success() => Ok(Conditional::Ok(PutOutcome {
                e_tag: resp.header("etag"),
            })),
            StatusCode::PRECONDITION_FAILED => Ok(Conditional::PreconditionFailed),
            // A concurrent conditional write to the same key (AWS ConditionalRequestConflict).
            StatusCode::CONFLICT if condition.is_some() => Ok(Conditional::PreconditionFailed),
            // `If-Match` on a missing object: AWS and SeaweedFS answer 404 NoSuchKey (others
            // 412). Either way the precondition failed (the trait contract); a 404 for any
            // other reason (NoSuchBucket, ...) stays an error.
            StatusCode::NOT_FOUND
                if matches!(condition, Some(("if-match", _)))
                    && resp.body.windows(9).any(|w| w == b"NoSuchKey") =>
            {
                Ok(Conditional::PreconditionFailed)
            }
            _ => Err(resp.into_status_error("put", key)),
        }
    }

    async fn upload_part(
        &self,
        full: &str,
        upload_id: &str,
        n: u16,
        part: Bytes,
    ) -> Result<String> {
        let action = self.bucket.upload_part(self.creds(), full, n, upload_id);
        let resp = self
            .send(|| self.client.put(action.sign(SIGN_EXPIRY)).body(part.clone()))
            .await?;
        if !resp.status.is_success() {
            return Err(resp.into_status_error("upload_part", full));
        }
        resp.header("etag").ok_or_else(|| Error::Protocol {
            op: "upload_part",
            detail: "no ETag header".into(),
        })
    }

    async fn multipart(
        &self,
        full: &str,
        upload_id: &str,
        mut buf: BytesMut,
        body: &mut BoxStream<'static, Result<Bytes>>,
    ) -> Result<PutOutcome> {
        let part_size = usize::try_from(self.multipart_part_size).unwrap_or(usize::MAX);
        let mut etags: Vec<String> = Vec::new();
        let next_part = |etags: &Vec<String>| {
            u16::try_from(etags.len() + 1).map_err(|_| Error::Protocol {
                op: "upload_part",
                detail: "more than 65535 parts; raise multipart_part_size".into(),
            })
        };
        loop {
            while buf.len() >= part_size {
                let part = buf.split_to(part_size).freeze();
                let n = next_part(&etags)?;
                etags.push(self.upload_part(full, upload_id, n, part).await?);
            }
            match body.next().await {
                Some(chunk) => buf.extend_from_slice(&chunk?),
                None => break,
            }
        }
        if !buf.is_empty() || etags.is_empty() {
            let n = next_part(&etags)?;
            etags.push(self.upload_part(full, upload_id, n, buf.freeze()).await?);
        }
        let resp = self
            .send(|| {
                let action: CompleteMultipartUpload<'_, _> = self.bucket.complete_multipart_upload(
                    self.creds(),
                    full,
                    upload_id,
                    etags.iter().map(String::as_str),
                );
                let url = action.sign(SIGN_EXPIRY);
                self.client.post(url).body(action.body())
            })
            .await?;
        if !resp.status.is_success() {
            return Err(resp.into_status_error("complete_multipart_upload", full));
        }
        // S3 can answer 200 with an <Error> body; the typed parse rejects it.
        let text = String::from_utf8_lossy(&resp.body);
        let done = CompleteMultipartUploadResponse::parse(&text).map_err(|e| Error::Protocol {
            op: "complete_multipart_upload",
            detail: format!("{e}: {}", text.chars().take(256).collect::<String>()),
        })?;
        Ok(PutOutcome {
            e_tag: Some(done.etag().to_owned()),
        })
    }
}

#[async_trait]
impl ObjectStore for S3Store {
    async fn put(&self, key: &str, body: Bytes) -> Result<PutOutcome> {
        match self.put_with_condition(key, body, None).await? {
            Conditional::Ok(o) => Ok(o),
            Conditional::PreconditionFailed => Err(Error::Protocol {
                op: "put",
                detail: "412 on an unconditional PUT".into(),
            }),
        }
    }

    async fn put_with(&self, key: &str, body: Bytes, opts: &PutOpts) -> Result<PutOutcome> {
        let Some(ct) = opts.content_type.as_deref() else {
            return self.put(key, body).await;
        };
        let full = self.full_key(key)?;
        let mut action = self.bucket.put_object(self.creds(), &full);
        // Before signing: content-type becomes a signed header the request must carry.
        action.headers_mut().insert("content-type", ct);
        let resp = self
            .send(|| {
                self.client
                    .put(action.sign(SIGN_EXPIRY))
                    .header("content-type", ct)
                    .body(body.clone())
            })
            .await?;
        if resp.status.is_success() {
            Ok(PutOutcome {
                e_tag: resp.header("etag"),
            })
        } else {
            Err(resp.into_status_error("put", key))
        }
    }

    async fn presign(
        &self,
        method: PresignMethod,
        key: &str,
        expires: Duration,
        opts: &PresignOpts,
    ) -> Result<Option<::url::Url>> {
        let full = self.full_key(key)?;
        let expires = clamp_presign_expiry(expires);
        let url = match method {
            PresignMethod::Get => {
                let mut action = self.bucket.get_object(self.creds(), &full);
                if let Some(rct) = opts.response_content_type.as_deref() {
                    action.query_mut().insert("response-content-type", rct);
                }
                action.sign(expires)
            }
            PresignMethod::Put => {
                let mut action = self.bucket.put_object(self.creds(), &full);
                if let Some(ct) = opts.content_type.as_deref() {
                    action.headers_mut().insert("content-type", ct);
                }
                action.sign(expires)
            }
        };
        Ok(Some(url))
    }

    async fn put_if_absent(&self, key: &str, body: Bytes) -> Result<Conditional<PutOutcome>> {
        self.put_with_condition(key, body, Some(("if-none-match", "*")))
            .await
    }

    async fn put_if_match(
        &self,
        key: &str,
        body: Bytes,
        e_tag: &str,
    ) -> Result<Conditional<PutOutcome>> {
        self.put_with_condition(key, body, Some(("if-match", e_tag)))
            .await
    }

    async fn get(&self, key: &str) -> Result<Option<(Bytes, ObjectMeta)>> {
        let full = self.full_key(key)?;
        let action = self.bucket.get_object(self.creds(), &full);
        let resp = self
            .send(|| self.client.get(action.sign(SIGN_EXPIRY)))
            .await?;
        match resp.status {
            StatusCode::OK => {
                let meta = resp.meta(key, resp.body.len() as u64);
                Ok(Some((resp.body, meta)))
            }
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(resp.into_status_error("get", key)),
        }
    }

    async fn get_range(&self, key: &str, range: std::ops::Range<u64>) -> Result<Bytes> {
        let invalid = |size| Error::InvalidRange {
            key: key.to_owned(),
            start: range.start,
            end: range.end,
            size,
        };
        if range.start > range.end {
            return Err(invalid(None));
        }
        if range.start == range.end {
            return match self.head(key).await? {
                Some(_) => Ok(Bytes::new()),
                None => Err(Error::NotFound {
                    key: key.to_owned(),
                }),
            };
        }
        let full = self.full_key(key)?;
        let action = self.bucket.get_object(self.creds(), &full);
        // Open-ended form for "to the end" (some servers reject last-byte values > i64::MAX).
        let header = if range.end > i64::MAX as u64 {
            format!("bytes={}-", range.start)
        } else {
            format!("bytes={}-{}", range.start, range.end - 1)
        };
        let resp = self
            .send(|| {
                self.client
                    .get(action.sign(SIGN_EXPIRY))
                    .header("range", header.as_str())
            })
            .await?;
        match resp.status {
            StatusCode::PARTIAL_CONTENT => Ok(resp.body),
            // A server that ignores Range sends the whole object: slice it.
            StatusCode::OK => {
                let len = resp.body.len() as u64;
                if range.start >= len {
                    return Err(invalid(Some(len)));
                }
                let (s, e) = (range.start as usize, range.end.min(len) as usize);
                Ok(resp.body.slice(s..e))
            }
            StatusCode::NOT_FOUND => Err(Error::NotFound {
                key: key.to_owned(),
            }),
            StatusCode::RANGE_NOT_SATISFIABLE => Err(invalid(None)),
            _ => Err(resp.into_status_error("get_range", key)),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        let full = self.full_key(key)?;
        let action = self.bucket.head_object(self.creds(), &full);
        let resp = self
            .send(|| self.client.head(action.sign(SIGN_EXPIRY)))
            .await?;
        match resp.status {
            StatusCode::OK => {
                let size = resp
                    .header("content-length")
                    .and_then(|v| v.parse().ok())
                    .ok_or_else(|| Error::Protocol {
                        op: "head",
                        detail: "no Content-Length".into(),
                    })?;
                Ok(Some(resp.meta(key, size)))
            }
            StatusCode::NOT_FOUND => Ok(None),
            _ => Err(resp.into_status_error("head", key)),
        }
    }

    async fn list(&self, prefix: &str, start_after: Option<&str>) -> Result<Vec<ObjectMeta>> {
        let full_prefix = format!("{}{prefix}", self.prefix);
        let full_start = start_after.map(|s| format!("{}{s}", self.prefix));
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut action = self.bucket.list_objects_v2(self.creds());
            if !full_prefix.is_empty() {
                action.with_prefix(full_prefix.as_str());
            }
            match (&token, &full_start) {
                (Some(t), _) => action.with_continuation_token(t.as_str()),
                (None, Some(s)) => action.with_start_after(s.as_str()),
                (None, None) => {}
            }
            action.with_max_keys(LIST_PAGE);
            let resp = self
                .send(|| self.client.get(action.sign(SIGN_EXPIRY)))
                .await?;
            if resp.status != StatusCode::OK {
                return Err(resp.into_status_error("list", prefix));
            }
            let text = String::from_utf8_lossy(&resp.body);
            let page = ListObjectsV2::parse_response(&text).map_err(|e| Error::Protocol {
                op: "list",
                detail: e.to_string(),
            })?;
            for c in page.contents {
                let Some(key) = c.key.strip_prefix(&self.prefix) else {
                    continue;
                };
                out.push(ObjectMeta {
                    key: key.to_owned(),
                    size: c.size.unwrap_or(0),
                    last_modified: c
                        .last_modified
                        .as_deref()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(jiff::Timestamp::UNIX_EPOCH),
                    e_tag: c.etag,
                    version: None,
                });
            }
            match page.next_continuation_token {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        // S3 lists in UTF-8 byte order already; sort defensively for other servers.
        out.sort_by(|a, b| a.key.cmp(&b.key));
        Ok(out)
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let full = self.full_key(key)?;
        let action = self.bucket.delete_object(self.creds(), &full);
        let resp = self
            .send(|| self.client.delete(action.sign(SIGN_EXPIRY)))
            .await?;
        if resp.status.is_success() || resp.status == StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(resp.into_status_error("delete", key))
        }
    }

    async fn delete_many(&self, keys: &[String]) -> Result<()> {
        for chunk in keys.chunks(DELETE_BATCH) {
            let objects = chunk
                .iter()
                .map(|k| self.full_key(k).map(ObjectIdentifier::new))
                .collect::<Result<Vec<_>>>()?;
            let resp = self
                .send(|| {
                    let mut action = self.bucket.delete_objects(self.creds(), objects.iter());
                    action.set_quiet(true);
                    let url = action.sign(SIGN_EXPIRY);
                    let (body, md5) = action.body_with_md5();
                    self.client
                        .request(Method::POST, url)
                        .header("content-md5", md5)
                        .body(body)
                })
                .await?;
            if !resp.status.is_success() {
                return Err(resp.into_status_error("delete_many", &chunk[0]));
            }
            let text = String::from_utf8_lossy(&resp.body);
            let parsed = DeleteObjectsResponse::parse(&text).map_err(|e| Error::Protocol {
                op: "delete_many",
                detail: e.to_string(),
            })?;
            if let Some(err) = parsed.errors.iter().find(|e| e.code != "NoSuchKey") {
                return Err(Error::Status {
                    op: "delete_many",
                    key: err.key.clone(),
                    status: 200,
                    body: format!("{}: {}", err.code, err.message),
                });
            }
        }
        Ok(())
    }

    async fn put_multipart(
        &self,
        key: &str,
        mut body: BoxStream<'static, Result<Bytes>>,
    ) -> Result<PutOutcome> {
        let full = self.full_key(key)?;
        let threshold = usize::try_from(self.multipart_threshold).unwrap_or(usize::MAX);
        let mut buf = BytesMut::new();
        while buf.len() < threshold {
            match body.next().await {
                Some(chunk) => buf.extend_from_slice(&chunk?),
                None => return self.put(key, buf.freeze()).await,
            }
        }

        let create = self.bucket.create_multipart_upload(self.creds(), &full);
        let resp = self
            .send(|| self.client.post(create.sign(SIGN_EXPIRY)))
            .await?;
        if !resp.status.is_success() {
            return Err(resp.into_status_error("create_multipart_upload", key));
        }
        let text = String::from_utf8_lossy(&resp.body);
        let created =
            CreateMultipartUpload::parse_response(&text).map_err(|e| Error::Protocol {
                op: "create_multipart_upload",
                detail: e.to_string(),
            })?;
        let upload_id = created.upload_id().to_owned();

        let res = self.multipart(&full, &upload_id, buf, &mut body).await;
        if res.is_err() {
            let abort = self
                .bucket
                .abort_multipart_upload(self.creds(), &full, &upload_id);
            match self
                .send(|| self.client.delete(abort.sign(SIGN_EXPIRY)))
                .await
            {
                Ok(r) if r.status.is_success() || r.status == StatusCode::NOT_FOUND => {}
                Ok(r) => tracing::warn!(status = r.status.as_u16(), key, "multipart abort failed"),
                Err(e) => tracing::warn!(error = %e, key, "multipart abort failed"),
            }
        }
        res
    }

    fn supports_conditional_put(&self) -> bool {
        self.conditional
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conditional_header_is_signed() {
        let bucket = Bucket::new(
            "http://127.0.0.1:9000".parse().unwrap(),
            rusty_s3::UrlStyle::Path,
            "b",
            "us-east-1",
        )
        .unwrap();
        let creds = rusty_s3::Credentials::new("k", "s");
        let mut action = bucket.put_object(Some(&creds), "x");
        action.headers_mut().insert("if-match", "\"abc\"");
        let url = action.sign(SIGN_EXPIRY);
        assert!(
            url.as_str().contains("X-Amz-SignedHeaders=host%3Bif-match"),
            "{url}"
        );
    }

    #[tokio::test]
    async fn presign_signs_content_type_and_clamps_expiry() {
        let o = StoreOptions {
            endpoint: Some("http://127.0.0.1:9000".into()),
            allow_http: true,
            credentials: Some(crate::Credentials {
                access_key_id: "k".into(),
                secret_access_key: "s".into(),
                session_token: None,
            }),
            ..StoreOptions::default()
        };
        let s = S3Store::new("b", "pre", &o).unwrap();
        let put = s
            .presign(
                PresignMethod::Put,
                "x",
                Duration::from_secs(30 * 24 * 3600),
                &PresignOpts {
                    content_type: Some("text/plain".into()),
                    response_content_type: None,
                },
            )
            .await
            .unwrap()
            .unwrap();
        let q = put.as_str();
        assert!(q.contains("/b/pre/x?"), "{q}");
        assert!(q.contains("X-Amz-SignedHeaders=content-type%3Bhost"), "{q}");
        assert!(q.contains("X-Amz-Expires=604800"), "{q}");
        let get = s
            .presign(
                PresignMethod::Get,
                "x",
                Duration::ZERO,
                &PresignOpts {
                    content_type: None,
                    response_content_type: Some("application/json".into()),
                },
            )
            .await
            .unwrap()
            .unwrap();
        let q = get.as_str();
        assert!(
            q.contains("X-Amz-Expires=1&") || q.ends_with("X-Amz-Expires=1"),
            "{q}"
        );
        assert!(
            q.contains("response-content-type=application%2Fjson"),
            "{q}"
        );
        assert!(
            s.presign(
                PresignMethod::Get,
                "../x",
                Duration::from_secs(5),
                &PresignOpts::default()
            )
            .await
            .is_err()
        );
    }

    #[test]
    fn backoff_is_bounded_and_grows() {
        let base = Duration::from_millis(100);
        for attempt in 0..10 {
            let d = backoff(base, attempt);
            let exp = base.saturating_mul(1 << attempt).min(MAX_BACKOFF);
            assert!(
                d >= exp / 2 && d <= exp,
                "attempt {attempt}: {d:?} vs {exp:?}"
            );
        }
    }

    #[test]
    fn http_endpoint_needs_allow_http_and_webpki_is_refused() {
        let mut o = StoreOptions {
            endpoint: Some("http://127.0.0.1:9000".into()),
            ..StoreOptions::default()
        };
        assert!(matches!(S3Store::new("b", "", &o), Err(Error::Config(_))));
        o.allow_http = true;
        let s = S3Store::new("b", "/x/y/", &o).unwrap();
        assert_eq!(s.prefix(), "x/y/");
        o.tls_roots = TlsRoots::Webpki;
        assert!(matches!(S3Store::new("b", "", &o), Err(Error::Config(_))));
    }

    #[test]
    fn prefix_normalisation() {
        assert_eq!(normalise_prefix("").unwrap(), "");
        assert_eq!(normalise_prefix("/").unwrap(), "");
        assert_eq!(normalise_prefix("a").unwrap(), "a/");
        assert_eq!(normalise_prefix("a/b/").unwrap(), "a/b/");
        assert!(normalise_prefix("a/../b").is_err());
    }
}
