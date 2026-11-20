//! The object-store connector (06-12, FX-05): `object-op` effects over the shared
//! `ostro-objstore` crate (D-FX1).
//!
//! - [`config`]: the `object` connection config (backend, credentials templates, caps).
//! - [`presign_local`]: Ostro-served HMAC presigned URLs for the local backend.
//!
//! # Trust model
//!
//! Object-store endpoints are **operator-configured and trusted**: backends use
//! [`Egress::operator_client`](crate::egress::Egress::operator_client) (no guest allow-list, no
//! private-range block, so an S3-compatible store on localhost works), never the guarded guest
//! client. A module may use connection `X` only if its egress policy lists `connection:X` (P-7),
//! else `egress_denied`; an unknown connection (or one of another kind) is
//! `connection_unknown`. Credentials are `{{secret:..}}` templates resolved through
//! [`SecretResolver::resolve_operator`] when the backend is built; values never reach a log,
//! `Debug` output or an error message (S3 error bodies are not copied into failures).
//!
//! Backends: S3-compatible (AWS S3, SeaweedFS, R2, Ceph). GCS should work as `backend = "s3"`
//! with `endpoint = "https://storage.googleapis.com"` and HMAC keys through the XML API's S3
//! interoperability, but that is **unverified**. Azure Blob is **deferred** (no S3 API; it
//! needs a native backend in `ostro-objstore`). Local filesystem roots presign through the
//! Ostro route `/v1/objects/{connection}/{key}` ([`ObjectConnector::serve_local`]).
//!
//! # Ops (W7)
//!
//! | op | output (`op-output.object`) |
//! |----|------------------------------|
//! | `get` | the object bytes; over `max_get_bytes` → `response_too_large` |
//! | `put` | JSON `{"e_tag"}`; body required; over `max_put_bytes` → `response_too_large` |
//! | `head` | JSON `{"size","last_modified" (RFC 3339),"e_tag","version"}` |
//! | `delete` | empty (idempotent) |
//! | `presign-get` / `presign-put` | the UTF-8 URL (validity = connection `presign_expiry`) |
//! | anything else | `non-retryable{unsupported_op}` |
//!
//! Other failure kinds (plain `ErrorInfo.kind` values of this connector, not W8 bridge
//! kinds): [`NOT_FOUND`] (get/head of an absent key), [`INVALID_REQUEST`] (bad key, missing
//! put body), [`OBJECT_ERROR`] (backend/transport failure; retryable for transport, I/O and
//! 5xx/429, non-retryable for other HTTP statuses).

pub mod config;
pub mod presign_local;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use ostro_meta::records::Connection;
use ostro_objstore::{
    Credentials, LocalFs, ObjectMeta, ObjectStore, PresignMethod, PresignOpts, PutOpts, S3Store,
    StoreOptions, UrlStyle,
};
use ostro_types::journal::{EffectRequest, Failure, OpOutput};
use ostro_types::live::{EffectKind, LiveEffect};

use crate::config::ConnectorsConfig;
use crate::connector::{Connector, EffectCtx, GateDecision, SendGate, Target};
use crate::egress::allows_connection;
use crate::kinds::{CONNECTION_UNKNOWN, EGRESS_DENIED, RESPONSE_TOO_LARGE, UNSUPPORTED_OP};
use crate::meta_view::MetaView;
use crate::outcome::{non_retryable, retryable};
use crate::secrets::{SecretResolver, SecretValue, template};

pub use config::{BackendConfig, ConfigError, OBJECT_KIND, ObjectConnConfig};
pub use presign_local::{MemoryPresignKeys, PresignKeyStore, VerifyError};

/// `ErrorInfo.kind` of a get/head of an absent key.
pub const NOT_FOUND: &str = "not_found";
/// `ErrorInfo.kind` of a malformed request (invalid key, missing put body).
pub const INVALID_REQUEST: &str = "invalid_request";
/// `ErrorInfo.kind` of a backend failure.
pub const OBJECT_ERROR: &str = "object_error";
/// Failpoint evaluated immediately before a `put` is sent.
pub const FP_BEFORE_PUT: &str = "bridge::object::before_put";

/// An object-op operation (W7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// `get`
    Get,
    /// `put`
    Put,
    /// `delete`
    Delete,
    /// `head`
    Head,
    /// `presign-get`
    PresignGet,
    /// `presign-put`
    PresignPut,
}

impl Op {
    /// Parses the WIT `object-request.op` string.
    pub fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "get" => Self::Get,
            "put" => Self::Put,
            "delete" => Self::Delete,
            "head" => Self::Head,
            "presign-get" => Self::PresignGet,
            "presign-put" => Self::PresignPut,
            _ => return None,
        })
    }
}

/// A built backend.
#[derive(Clone)]
struct Backend {
    store: Arc<dyn ObjectStore>,
    /// Set for the local backend (content type lookups, Ostro presign).
    local: Option<LocalFs>,
    cfg: Arc<ObjectConnConfig>,
}

struct Cached {
    config: Vec<u8>,
    built: Instant,
    backend: Backend,
}

/// Response of [`ObjectConnector::serve_local`]: `(status, body, content type)`.
pub type LocalResponse = (u16, Vec<u8>, Option<String>);

/// Why [`ObjectConnector::serve_local`] refused a request.
#[derive(Debug, thiserror::Error)]
pub enum ServeError {
    /// Bad, tampered or expired signature, or no presign key exists (403).
    #[error("forbidden: {0}")]
    Forbidden(VerifyError),
    /// Unknown connection, not a local backend, or absent object (404).
    #[error("not found")]
    NotFound,
    /// Method other than GET/PUT, invalid key, or missing body (400).
    #[error("bad request: {0}")]
    BadRequest(String),
    /// The body or object exceeds the connection's cap (413).
    #[error("payload too large")]
    TooLarge,
    /// The backend failed (500).
    #[error("object store error: {0}")]
    Backend(String),
}

impl ServeError {
    /// The HTTP status for this refusal.
    pub fn status(&self) -> u16 {
        match self {
            Self::Forbidden(_) => 403,
            Self::NotFound => 404,
            Self::BadRequest(_) => 400,
            Self::TooLarge => 413,
            Self::Backend(_) => 500,
        }
    }
}

/// The object connector.
pub struct ObjectConnector {
    meta: Arc<dyn MetaView>,
    resolver: SecretResolver,
    client: reqwest::Client,
    cfg: ConnectorsConfig,
    keys: Option<Arc<dyn PresignKeyStore>>,
    backends: Mutex<HashMap<String, Cached>>,
    presign_keys: Mutex<HashMap<String, SecretValue>>,
}

impl std::fmt::Debug for ObjectConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectConnector")
            .field("cfg", &self.cfg)
            .field("presign_key_store", &self.keys.is_some())
            .finish_non_exhaustive()
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// Maps an `ostro-objstore` error to a guest failure without copying response bodies (an S3
/// error body can echo the access key id).
fn store_failure(op: &str, key: &str, e: ostro_objstore::Error) -> Failure {
    use ostro_objstore::Error as E;
    let details = Some(serde_json::json!({ "op": op, "key": key }));
    match e {
        E::InvalidKey { reason, .. } => non_retryable(
            INVALID_REQUEST,
            format!("invalid object key {key:?}: {reason}"),
            details,
        ),
        E::NotFound { .. } => {
            non_retryable(NOT_FOUND, format!("object {key:?} not found"), details)
        }
        E::InvalidRange { .. } => non_retryable(INVALID_REQUEST, "invalid range", details),
        E::Config(m) => non_retryable(
            CONNECTION_UNKNOWN,
            format!("object store config: {m}"),
            details,
        ),
        E::Status { status, .. } if status >= 500 || status == 429 => retryable(
            OBJECT_ERROR,
            format!("object store {op} {key:?}: HTTP {status}"),
        ),
        E::Status { status, .. } => non_retryable(
            OBJECT_ERROR,
            format!("object store {op} {key:?}: HTTP {status}"),
            details,
        ),
        E::Io { .. } => retryable(
            OBJECT_ERROR,
            format!("object store {op} {key:?}: I/O error"),
        ),
        E::Transport(_) => retryable(
            OBJECT_ERROR,
            format!("object store {op} {key:?}: transport error"),
        ),
        other => retryable(OBJECT_ERROR, format!("object store {op} {key:?}: {other}")),
    }
}

fn too_large(what: &str, size: u64, cap: u64) -> Failure {
    non_retryable(
        RESPONSE_TOO_LARGE,
        format!("{what} of {size} bytes exceeds the cap of {cap} bytes"),
        Some(serde_json::json!({ "size": size, "cap": cap })),
    )
}

fn head_json(m: &ObjectMeta) -> Vec<u8> {
    serde_json::json!({
        "size": m.size,
        "last_modified": m.last_modified.to_string(),
        "e_tag": m.e_tag,
        "version": m.version,
    })
    .to_string()
    .into_bytes()
}

impl ObjectConnector {
    /// A connector over the meta view (connections, egress policies), the secret resolver
    /// (credentials; `resolve_reserved` for presign keys), the trusted operator client
    /// (`Egress::operator_client`) and the `[connectors]` section (`backend_ttl`,
    /// `public_base_url`, `presign_default_expiry`, `object_max_get_bytes`).
    pub fn new(
        meta: Arc<dyn MetaView>,
        resolver: SecretResolver,
        operator_client: reqwest::Client,
        cfg: ConnectorsConfig,
    ) -> Self {
        Self {
            meta,
            resolver,
            client: operator_client,
            cfg,
            keys: None,
            backends: Mutex::new(HashMap::new()),
            presign_keys: Mutex::new(HashMap::new()),
        }
    }

    /// Supplies the store that persists newly generated local presign keys (06-14 seals
    /// them; tests use [`MemoryPresignKeys`]). Without it, a local `presign-*` for a
    /// connection whose key does not exist yet fails.
    pub fn with_presign_key_store(mut self, store: Arc<dyn PresignKeyStore>) -> Self {
        self.keys = Some(store);
        self
    }

    /// The parsed config of object connection `name` (`connection_unknown` when absent, of
    /// another kind, or invalid).
    fn conn_config(&self, name: &str) -> Result<(Connection, ObjectConnConfig), Failure> {
        let conn = self
            .meta
            .connection(name)
            .filter(|c| c.kind == OBJECT_KIND)
            .ok_or_else(|| {
                non_retryable(
                    CONNECTION_UNKNOWN,
                    format!("no object connection named {name:?}"),
                    Some(serde_json::json!({ "connection": name })),
                )
            })?;
        let cfg = ObjectConnConfig::parse(&conn, &self.cfg).map_err(|e| e.to_failure())?;
        Ok((conn, cfg))
    }

    /// The backend of `name`, built on first use and rebuilt after `backend_ttl` or a config
    /// change.
    async fn backend(&self, name: &str) -> Result<Backend, Failure> {
        let (conn, cfg) = self.conn_config(name)?;
        {
            let cache = self.backends.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(c) = cache.get(name)
                && c.config == conn.config
                && c.built.elapsed() < self.cfg.backend_ttl
            {
                return Ok(c.backend.clone());
            }
        }
        let backend = self.build(name, cfg).await?;
        let mut cache = self.backends.lock().unwrap_or_else(|p| p.into_inner());
        cache.insert(
            name.to_owned(),
            Cached {
                config: conn.config,
                built: Instant::now(),
                backend: backend.clone(),
            },
        );
        Ok(backend)
    }

    async fn expand(&self, tpl: &str) -> Result<String, Failure> {
        let mut values = BTreeMap::new();
        for p in template::parse(tpl).map_err(|e| {
            non_retryable(
                CONNECTION_UNKNOWN,
                format!("credential template: {e}"),
                None,
            )
        })? {
            if let std::collections::btree_map::Entry::Vacant(slot) = values.entry(p.name) {
                let v = self.resolver.resolve_operator(slot.key()).await?;
                slot.insert(v);
            }
        }
        let bytes = template::expand(tpl, &values);
        String::from_utf8(bytes.to_vec())
            .map_err(|_| non_retryable(CONNECTION_UNKNOWN, "credential value is not UTF-8", None))
    }

    async fn build(&self, name: &str, cfg: ObjectConnConfig) -> Result<Backend, Failure> {
        let cfg = Arc::new(cfg);
        let config_failure = |e: ostro_objstore::Error| {
            non_retryable(
                CONNECTION_UNKNOWN,
                format!("connection {name:?}: {e}"),
                Some(serde_json::json!({ "connection": name })),
            )
        };
        match &cfg.backend {
            BackendConfig::Local { root } => {
                let root = root.clone();
                let fs = tokio::task::spawn_blocking(move || LocalFs::new(root))
                    .await
                    .map_err(|_| retryable(OBJECT_ERROR, "local backend setup panicked"))?
                    .map_err(config_failure)?;
                Ok(Backend {
                    store: Arc::new(fs.clone()),
                    local: Some(fs),
                    cfg,
                })
            }
            BackendConfig::S3 {
                endpoint,
                bucket,
                region,
                virtual_host,
                allow_http,
                credentials,
            } => {
                let credentials = match credentials {
                    None => None,
                    Some(t) => Some(Credentials {
                        access_key_id: self.expand(&t.access_key_id).await?,
                        secret_access_key: self.expand(&t.secret_access_key).await?,
                        session_token: match &t.session_token {
                            Some(s) => Some(self.expand(s).await?),
                            None => None,
                        },
                    }),
                };
                let opts = StoreOptions {
                    region: region.clone(),
                    endpoint: endpoint.clone(),
                    url_style: if *virtual_host {
                        UrlStyle::Virtual
                    } else {
                        UrlStyle::Path
                    },
                    credentials,
                    allow_http: *allow_http,
                    ..StoreOptions::default()
                };
                let s3 = S3Store::with_client(bucket, "", &opts, self.client.clone())
                    .map_err(config_failure)?;
                Ok(Backend {
                    store: Arc::new(s3),
                    local: None,
                    cfg,
                })
            }
        }
    }

    /// The presign key of `connection`: cache, then the sealed reserved secret; when absent
    /// and `create`, a fresh key stored through the [`PresignKeyStore`].
    async fn presign_key(
        &self,
        connection: &str,
        create: bool,
    ) -> Result<Option<SecretValue>, Failure> {
        if let Some(k) = self
            .presign_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(connection)
        {
            return Ok(Some(k.clone()));
        }
        let name = presign_local::key_name(connection);
        let key = match self.resolver.resolve_reserved(&name).await? {
            Some(k) => k,
            None if !create => return Ok(None),
            None => {
                let store = self.keys.as_ref().ok_or_else(|| {
                    non_retryable(
                        UNSUPPORTED_OP,
                        "local presign needs a presign key store (server not configured)",
                        None,
                    )
                })?;
                store.create(&name, presign_local::generate_key()?).await?
            }
        };
        self.presign_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(connection.to_owned(), key.clone());
        Ok(Some(key))
    }

    async fn presign_local_url(
        &self,
        connection: &str,
        key: &str,
        method: PresignMethod,
        expiry: Duration,
    ) -> Result<String, Failure> {
        let base = self.cfg.public_base_url.as_deref().ok_or_else(|| {
            non_retryable(
                UNSUPPORTED_OP,
                "local presign needs [connectors] public_base_url",
                None,
            )
        })?;
        let hmac_key = self
            .presign_key(connection, true)
            .await?
            .ok_or_else(|| retryable(OBJECT_ERROR, "presign key unavailable"))?;
        let exp = now_unix().saturating_add(expiry.as_secs().max(1));
        let bad_key = || {
            non_retryable(
                INVALID_REQUEST,
                format!("key {key:?} cannot be presigned"),
                None,
            )
        };
        let sig = presign_local::sign(hmac_key.expose(), method, connection, key, exp)
            .ok_or_else(bad_key)?;
        let url = presign_local::url(base, connection, key, exp, &sig).ok_or_else(|| {
            non_retryable(UNSUPPORTED_OP, "invalid [connectors] public_base_url", None)
        })?;
        Ok(url.to_string())
    }

    async fn gate(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        gate: &dyn SendGate,
    ) -> Result<(), Failure> {
        if gate.check(ctx.partition, &ctx.run_id, effect.op).await == GateDecision::Abandon {
            ctx.cancel.cancel();
            return Err(Failure::Cancelled);
        }
        Ok(())
    }

    /// Serves the local presign route `GET|PUT /v1/objects/{connection}/{key}?exp=&sig=`
    /// (wired into ostro-api by 06-16). Verifies the signature first (constant time), then
    /// the expiry, then reads or writes the local backend (applying the connection prefix,
    /// caps and content type). Returns `(status, body, content type)`.
    pub async fn serve_local(
        &self,
        connection: &str,
        key: &str,
        method: &str,
        exp: u64,
        sig: &str,
        body: Option<Bytes>,
    ) -> Result<LocalResponse, ServeError> {
        let method = match method {
            "GET" => PresignMethod::Get,
            "PUT" => PresignMethod::Put,
            m => return Err(ServeError::BadRequest(format!("method {m} not allowed"))),
        };
        let hmac_key = self
            .presign_key(connection, false)
            .await
            .map_err(|_| ServeError::Forbidden(VerifyError::BadSignature))?
            .ok_or(ServeError::Forbidden(VerifyError::BadSignature))?;
        presign_local::verify(
            hmac_key.expose(),
            method,
            connection,
            key,
            exp,
            sig,
            now_unix(),
        )
        .map_err(ServeError::Forbidden)?;
        let backend = self
            .backend(connection)
            .await
            .map_err(|_| ServeError::NotFound)?;
        let Some(fs) = backend.local.clone() else {
            return Err(ServeError::NotFound);
        };
        let full = backend.cfg.full_key(key);
        let map = |e: ostro_objstore::Error| match e {
            ostro_objstore::Error::InvalidKey { reason, .. } => {
                ServeError::BadRequest(format!("invalid key: {reason}"))
            }
            ostro_objstore::Error::NotFound { .. } => ServeError::NotFound,
            other => ServeError::Backend(other.to_string()),
        };
        match method {
            PresignMethod::Get => {
                let meta = fs
                    .head(&full)
                    .await
                    .map_err(map)?
                    .ok_or(ServeError::NotFound)?;
                if meta.size > backend.cfg.max_get_bytes {
                    return Err(ServeError::TooLarge);
                }
                let (bytes, _) = fs
                    .get(&full)
                    .await
                    .map_err(map)?
                    .ok_or(ServeError::NotFound)?;
                let ct = fs.content_type(&full).await.map_err(map)?;
                Ok((200, bytes.to_vec(), ct))
            }
            PresignMethod::Put => {
                let body = body.ok_or_else(|| ServeError::BadRequest("PUT needs a body".into()))?;
                if body.len() as u64 > backend.cfg.max_put_bytes {
                    return Err(ServeError::TooLarge);
                }
                let out = fs
                    .put_with(
                        &full,
                        body,
                        &PutOpts {
                            content_type: backend.cfg.content_type.clone(),
                        },
                    )
                    .await
                    .map_err(map)?;
                let json = serde_json::json!({ "e_tag": out.e_tag }).to_string();
                Ok((200, json.into_bytes(), Some("application/json".into())))
            }
        }
    }
}

#[async_trait]
impl Connector for ObjectConnector {
    fn kind(&self) -> EffectKind {
        EffectKind::Object
    }

    fn target(&self, _ctx: &EffectCtx, effect: &LiveEffect) -> Option<Target> {
        let EffectRequest::Object(req) = &effect.request else {
            return None;
        };
        let quota = self
            .meta
            .connection(&req.connection)
            .filter(|c| c.kind == OBJECT_KIND)
            .and_then(|c| ObjectConnConfig::parse(&c, &self.cfg).ok())
            .and_then(|c| c.quota);
        Some(Target {
            key: req.connection.clone(),
            quota,
        })
    }

    async fn execute(
        &self,
        ctx: &EffectCtx,
        effect: &LiveEffect,
        gate: &dyn SendGate,
    ) -> Result<OpOutput, Failure> {
        let EffectRequest::Object(req) = &effect.request else {
            return Err(non_retryable(
                UNSUPPORTED_OP,
                "the object connector only executes object-op effects",
                None,
            ));
        };
        let op = Op::parse(&req.op).ok_or_else(|| {
            non_retryable(
                UNSUPPORTED_OP,
                format!("unsupported object op {:?} (get | put | delete | head | presign-get | presign-put)", req.op),
                Some(serde_json::json!({ "op": req.op })),
            )
        })?;
        let name = req.connection.as_str();
        if !allows_connection(self.meta.egress_policy(&ctx.module).as_ref(), name) {
            return Err(non_retryable(
                EGRESS_DENIED,
                format!("egress denied: module may not use connection {name:?}"),
                Some(serde_json::json!({ "reason": "not_allowed", "connection": name })),
            ));
        }
        // Gate before credentials are resolved, and again right before the I/O.
        self.gate(ctx, effect, gate).await?;
        let backend = self.backend(name).await?;
        let cfg = backend.cfg.clone();
        let full = cfg.full_key(&req.key);
        let fail = |e| store_failure(&req.op, &req.key, e);
        self.gate(ctx, effect, gate).await?;
        let out = match op {
            Op::Get => {
                let meta = backend
                    .store
                    .head(&full)
                    .await
                    .map_err(fail)?
                    .ok_or_else(|| fail(ostro_objstore::Error::NotFound { key: full.clone() }))?;
                if meta.size > cfg.max_get_bytes {
                    return Err(too_large("object", meta.size, cfg.max_get_bytes));
                }
                let (bytes, _) = backend
                    .store
                    .get(&full)
                    .await
                    .map_err(fail)?
                    .ok_or_else(|| fail(ostro_objstore::Error::NotFound { key: full.clone() }))?;
                if bytes.len() as u64 > cfg.max_get_bytes {
                    return Err(too_large("object", bytes.len() as u64, cfg.max_get_bytes));
                }
                bytes.to_vec()
            }
            Op::Put => {
                let body = req
                    .body
                    .as_ref()
                    .ok_or_else(|| non_retryable(INVALID_REQUEST, "put requires a body", None))?;
                if body.len() as u64 > cfg.max_put_bytes {
                    return Err(too_large("put body", body.len() as u64, cfg.max_put_bytes));
                }
                if let Some(action) = crate::bridge_fail!(FP_BEFORE_PUT)
                    && crate::failpoints::apply(action).await.is_err()
                {
                    return Err(retryable(
                        OBJECT_ERROR,
                        "failpoint bridge::object::before_put",
                    ));
                }
                let out = backend
                    .store
                    .put_with(
                        &full,
                        Bytes::from(body.clone()),
                        &PutOpts {
                            content_type: cfg.content_type.clone(),
                        },
                    )
                    .await
                    .map_err(fail)?;
                serde_json::json!({ "e_tag": out.e_tag })
                    .to_string()
                    .into_bytes()
            }
            Op::Head => {
                let meta = backend
                    .store
                    .head(&full)
                    .await
                    .map_err(fail)?
                    .ok_or_else(|| fail(ostro_objstore::Error::NotFound { key: full.clone() }))?;
                head_json(&meta)
            }
            Op::Delete => {
                backend.store.delete(&full).await.map_err(fail)?;
                Vec::new()
            }
            Op::PresignGet | Op::PresignPut => {
                let method = if op == Op::PresignGet {
                    PresignMethod::Get
                } else {
                    PresignMethod::Put
                };
                let opts = PresignOpts {
                    content_type: (method == PresignMethod::Put)
                        .then(|| cfg.content_type.clone())
                        .flatten(),
                    response_content_type: None,
                };
                match backend
                    .store
                    .presign(method, &full, cfg.presign_expiry, &opts)
                    .await
                    .map_err(fail)?
                {
                    Some(url) => url.to_string().into_bytes(),
                    None if backend.local.is_some() => {
                        // Validate the key the way the backend would before signing it.
                        backend.store.head(&full).await.map_err(fail)?;
                        self.presign_local_url(name, &req.key, method, cfg.presign_expiry)
                            .await?
                            .into_bytes()
                    }
                    None => {
                        return Err(non_retryable(
                            UNSUPPORTED_OP,
                            "this backend cannot presign",
                            None,
                        ));
                    }
                }
            }
        };
        Ok(OpOutput::Object(out))
    }
}
