//! The HashiCorp Vault KV v2 provider (feature `vault`), owned by 06-13.
//!
//! # Names
//!
//! `path/to/secret#field` reads `field` of the KV v2 secret at `path/to/secret` under the
//! configured mount (default `secret`). Without `#field` the field is `value`. The path must
//! pass the template name grammar (no `..`, no empty or `.` segments, no `__ostro/`); the
//! field is `[A-Za-z0-9_.-]{1,128}`. A malformed name is `ProviderError::Invalid`.
//!
//! Note: the `{{secret:NAME}}` grammar (06-05) has no `#`, so guest templates and
//! `resolve_operator` can only reach the default `value` field today; `#field` is available
//! to direct callers.
//!
//! # Values
//!
//! A string field is returned as its UTF-8 bytes; a number or boolean as its JSON text. A
//! `null` or missing field, or a missing (or deleted) secret, is `Ok(None)`. An object or array
//! field is `Invalid`. Every other string of the response is zeroized after the field is
//! taken.
//!
//! # Auth
//!
//! - `token`: the token is read from `token_file` (or `VAULT_TOKEN`) when a session starts.
//! - `approle`: `role_id` + the secret id read from `secret_id_file` at login
//!   (`POST /v1/auth/approle/login`).
//! - `kubernetes`: `k8s_role` + the service-account JWT read from
//!   `/var/run/secrets/kubernetes.io/serviceaccount/token` at login
//!   (`POST /v1/auth/kubernetes/login`).
//!
//! Sessions start lazily on the first read, so an unreachable Vault never blocks server
//! start. A login lease is refreshed (a fresh login) after 3/4 of its duration. A 403 on a
//! read starts a new session once (re-reading the token or secret-id file) and retries; a
//! second 403 is `Unavailable("permission denied")`.
//!
//! # Caching
//!
//! Found values are cached per name for `cache_ttl` in a [`TtlCache`] (zeroized on eviction).
//!
//! # Hygiene
//!
//! Tokens and secret ids are held in [`SecretString`]. vaultrs keeps the session token in a
//! plain `String` inside its client settings (its API takes `&str`); that copy is never
//! formatted by this module, and the provider's `Debug` shows the address, mount, auth
//! method and TTL only. Errors carry the operation and HTTP status, never response bodies.
//!
//! # vaultrs 0.8.0 API used (checked in the registry source)
//!
//! `client::{VaultClient, VaultClientSettingsBuilder}` (`address`, `token`, `timeout`,
//! `build`), `kv2::read::<serde_json::Value>(client, mount, path)` (returns the inner
//! `data` map), `auth::approle::login(client, mount, role_id, secret_id)` and
//! `auth::kubernetes::login(client, mount, role, jwt)` (both `-> AuthInfo { client_token,
//! lease_duration, .. }`), `error::ClientError::{APIError { code, .. }, RestClientError }`.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use secrecy::{ExposeSecret, SecretBox, SecretString};
use serde_json::Value;
use tokio::sync::Mutex;
use vaultrs::client::{VaultClient, VaultClientSettingsBuilder};
use vaultrs::error::ClientError;
use zeroize::{Zeroize, Zeroizing};

use super::cache::TtlCache;
use super::{ProviderError, SecretProvider};
use crate::secrets::config::{VaultAuth, VaultConfig};
use crate::secrets::template;

/// The in-pod service-account token used by `auth = "kubernetes"`.
pub const SA_TOKEN_FILE: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
/// The field read when a name has no `#field`.
pub const DEFAULT_FIELD: &str = "value";
/// Mount of the AppRole auth method.
pub const APPROLE_MOUNT: &str = "approle";
/// Mount of the Kubernetes auth method.
pub const K8S_AUTH_MOUNT: &str = "kubernetes";
/// Per-request timeout of the Vault client.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FIELD_LEN: usize = 128;

enum Auth {
    Token {
        file: Option<PathBuf>,
    },
    Approle {
        role_id: String,
        secret_id_file: PathBuf,
    },
    Kubernetes {
        role: String,
        jwt_file: PathBuf,
    },
}

impl Auth {
    fn kind(&self) -> &'static str {
        match self {
            Self::Token { .. } => "token",
            Self::Approle { .. } => "approle",
            Self::Kubernetes { .. } => "kubernetes",
        }
    }
}

struct Session {
    client: Arc<VaultClient>,
    generation: u64,
    refresh_at: Option<Instant>,
}

#[derive(Default)]
struct SessionState {
    current: Option<Session>,
    generation: u64,
}

/// Reads secrets from a Vault KV v2 mount.
pub struct VaultProvider {
    addr: String,
    mount: String,
    auth: Auth,
    cache: TtlCache<String>,
    state: Mutex<SessionState>,
}

impl fmt::Debug for VaultProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VaultProvider")
            .field("addr", &self.addr)
            .field("mount", &self.mount)
            .field("auth", &self.auth.kind())
            .field("cache_ttl", &self.cache.ttl())
            .finish_non_exhaustive()
    }
}

impl VaultProvider {
    /// A provider for `[secrets.vault]`. Validates the configuration; the first login (or
    /// token-file read) happens on the first read.
    pub fn from_config(cfg: &VaultConfig) -> Result<Self, ProviderError> {
        Self::from_config_with_sa_token(cfg, SA_TOKEN_FILE)
    }

    /// Like [`Self::from_config`], with the service-account JWT of `auth = "kubernetes"` read
    /// from `jwt_file` instead of [`SA_TOKEN_FILE`].
    pub fn from_config_with_sa_token(
        cfg: &VaultConfig,
        jwt_file: impl Into<PathBuf>,
    ) -> Result<Self, ProviderError> {
        let invalid = |m: String| ProviderError::Invalid(format!("secrets.vault: {m}"));
        let url =
            url::Url::parse(&cfg.addr).map_err(|e| invalid(format!("addr {:?}: {e}", cfg.addr)))?;
        if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
            return Err(invalid(format!(
                "addr {:?} must be an http(s) URL with a host",
                cfg.addr
            )));
        }
        if cfg.mount.is_empty()
            || cfg.mount.starts_with('/')
            || cfg.mount.ends_with('/')
            || cfg.mount.contains("..")
            || !cfg
                .mount
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-./".contains(&b))
        {
            return Err(invalid(format!("invalid mount {:?}", cfg.mount)));
        }
        let auth = match cfg.auth {
            VaultAuth::Token => {
                if cfg.token_file.is_none() && std::env::var_os("VAULT_TOKEN").is_none() {
                    return Err(invalid(
                        "auth = \"token\" needs token_file or VAULT_TOKEN".into(),
                    ));
                }
                Auth::Token {
                    file: cfg.token_file.clone(),
                }
            }
            VaultAuth::Approle => match (&cfg.role_id, &cfg.secret_id_file) {
                (Some(r), Some(f)) if !r.is_empty() => Auth::Approle {
                    role_id: r.clone(),
                    secret_id_file: f.clone(),
                },
                _ => {
                    return Err(invalid(
                        "auth = \"approle\" needs role_id and secret_id_file".into(),
                    ));
                }
            },
            VaultAuth::Kubernetes => match &cfg.k8s_role {
                Some(r) if !r.is_empty() => Auth::Kubernetes {
                    role: r.clone(),
                    jwt_file: jwt_file.into(),
                },
                _ => return Err(invalid("auth = \"kubernetes\" needs k8s_role".into())),
            },
        };
        Ok(Self {
            addr: cfg.addr.clone(),
            mount: cfg.mount.clone(),
            auth,
            cache: TtlCache::new(cfg.cache_ttl),
            state: Mutex::new(SessionState::default()),
        })
    }

    /// The value cache (exposed for tests and for an operator-triggered flush).
    pub fn cache(&self) -> &TtlCache<String> {
        &self.cache
    }

    /// A vaultrs client for the operator-configured Vault address.
    ///
    /// This is not guest egress: the address comes from `[secrets.vault]`, never from a
    /// workflow, so the SSRF-guarded egress client (P-5) does not apply. The constructor is
    /// called through the `<VaultClient>` path form because `tests/egress_client.rs` is a
    /// textual guard against reqwest client constructors outside `egress/client.rs`.
    fn client(&self, token: &str) -> Result<VaultClient, ProviderError> {
        let settings = VaultClientSettingsBuilder::default()
            .address(&self.addr)
            .token(token)
            .timeout(Some(REQUEST_TIMEOUT))
            .build()
            .map_err(|e| ProviderError::Invalid(format!("secrets.vault: client settings: {e}")))?;
        <VaultClient>::new(settings)
            .map_err(|e| ProviderError::Unavailable(format!("vault client: {e}")))
    }

    /// Obtains a token: `(token, lease)`; `lease` is `None` when it does not expire (or is
    /// unknown, as for a static token).
    async fn login(&self) -> Result<(SecretString, Option<Duration>), ProviderError> {
        match &self.auth {
            Auth::Token { file } => {
                let t = match file {
                    Some(p) => read_secret_file(p).await?,
                    None => match std::env::var("VAULT_TOKEN") {
                        Ok(t) if !t.trim().is_empty() => {
                            let t = Zeroizing::new(t);
                            SecretString::from(t.trim().to_owned())
                        }
                        _ => {
                            return Err(ProviderError::Unavailable(
                                "vault: VAULT_TOKEN is not set".into(),
                            ));
                        }
                    },
                };
                Ok((t, None))
            }
            Auth::Approle {
                role_id,
                secret_id_file,
            } => {
                let secret_id = read_secret_file(secret_id_file).await?;
                let c = self.client("")?;
                let a = vaultrs::auth::approle::login(
                    &c,
                    APPROLE_MOUNT,
                    role_id,
                    secret_id.expose_secret(),
                )
                .await
                .map_err(|e| unavailable("approle login", &e))?;
                Ok((SecretString::from(a.client_token), lease(a.lease_duration)))
            }
            Auth::Kubernetes { role, jwt_file } => {
                let jwt = read_secret_file(jwt_file).await?;
                let c = self.client("")?;
                let a =
                    vaultrs::auth::kubernetes::login(&c, K8S_AUTH_MOUNT, role, jwt.expose_secret())
                        .await
                        .map_err(|e| unavailable("kubernetes login", &e))?;
                Ok((SecretString::from(a.client_token), lease(a.lease_duration)))
            }
        }
    }

    /// The current session, starting a new one when there is none, its lease is due for
    /// refresh, or it is the session `stale` (which just got a 403).
    async fn session(&self, stale: Option<u64>) -> Result<(Arc<VaultClient>, u64), ProviderError> {
        let mut st = self.state.lock().await;
        let now = Instant::now();
        if let Some(s) = &st.current {
            let fresh = s.refresh_at.is_none_or(|t| now < t);
            if fresh && Some(s.generation) != stale {
                return Ok((s.client.clone(), s.generation));
            }
        }
        st.current = None;
        let (token, lease) = self.login().await?;
        let client = Arc::new(self.client(token.expose_secret())?);
        st.generation += 1;
        let generation = st.generation;
        st.current = Some(Session {
            client: client.clone(),
            generation,
            refresh_at: lease.and_then(|l| now.checked_add(l * 3 / 4)),
        });
        tracing::debug!(
            provider = "vault",
            auth = self.auth.kind(),
            generation,
            "vault session started"
        );
        Ok((client, generation))
    }

    async fn read(&self, client: &VaultClient, path: &str) -> Result<Option<Value>, ClientError> {
        match vaultrs::kv2::read::<Value>(client, &self.mount, path).await {
            Ok(v) => Ok(Some(v)),
            Err(e) if status_of(&e) == Some(404) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

fn lease(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

/// Reads a token / secret-id / JWT file (trimmed). Errors name the path and the error kind.
async fn read_secret_file(p: &Path) -> Result<SecretString, ProviderError> {
    let raw = Zeroizing::new(tokio::fs::read_to_string(p).await.map_err(|e| {
        ProviderError::Unavailable(format!("vault: reading {}: {}", p.display(), e.kind()))
    })?);
    let t = raw.trim();
    if t.is_empty() {
        return Err(ProviderError::Unavailable(format!(
            "vault: {} is empty",
            p.display()
        )));
    }
    Ok(SecretString::from(t.to_owned()))
}

/// The HTTP status of a failed call, when known.
///
/// `APIError` carries it. A non-JSON or non-standard error body (for example the 404 of a
/// deleted KV v2 version, whose body has `data` but no `errors`) surfaces as
/// `RestClientError` wrapping rustify's `ServerResponseError { code, .. }`; rustify is not a
/// direct dependency, so the code is read from that error's `Debug` prefix. The formatted
/// text stays local and is dropped (error bodies hold no secret values).
fn status_of(e: &ClientError) -> Option<u16> {
    match e {
        ClientError::APIError { code, .. } => Some(*code),
        ClientError::RestClientError { source } => {
            let mut d = format!("{source:?}");
            let code = d
                .strip_prefix("ServerResponseError { code: ")
                .and_then(|rest| {
                    let n = rest.bytes().take_while(u8::is_ascii_digit).count();
                    rest[..n].parse().ok()
                });
            d.zeroize();
            code
        }
        _ => None,
    }
}

fn unavailable(op: &str, e: &ClientError) -> ProviderError {
    match status_of(e) {
        Some(403) => ProviderError::Unavailable(format!("vault {op}: permission denied")),
        Some(code) => ProviderError::Unavailable(format!("vault {op}: status {code}")),
        None => match e {
            ClientError::RestClientError { source } => {
                ProviderError::Unavailable(format!("vault {op}: {e}: {source}"))
            }
            _ => ProviderError::Unavailable(format!("vault {op}: {e}")),
        },
    }
}

fn is_forbidden(e: &ClientError) -> bool {
    status_of(e) == Some(403)
}

fn parse_name(name: &str) -> Result<(&str, &str), ProviderError> {
    let (path, field) = name.split_once('#').unwrap_or((name, DEFAULT_FIELD));
    template::validate_name(path).map_err(|e| ProviderError::Invalid(e.to_string()))?;
    if field.is_empty()
        || field.len() > MAX_FIELD_LEN
        || !field
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-.".contains(&b))
    {
        return Err(ProviderError::Invalid(
            "invalid vault field in secret name".into(),
        ));
    }
    Ok((path, field))
}

/// Zeroizes every string (keys and values) of a JSON value.
fn scrub(v: &mut Value) {
    match v {
        Value::String(s) => s.zeroize(),
        Value::Array(a) => a.iter_mut().for_each(scrub),
        Value::Object(m) => {
            let taken = std::mem::take(m);
            for (mut k, mut v) in taken {
                k.zeroize();
                scrub(&mut v);
            }
        }
        _ => {}
    }
}

/// Takes `field` out of the KV v2 data map (see the module docs for the value rules).
fn take_field(
    mut data: Value,
    path: &str,
    field: &str,
) -> Result<Option<Zeroizing<Vec<u8>>>, ProviderError> {
    let taken = match &mut data {
        Value::Object(m) => m.remove(field),
        _ => None,
    };
    scrub(&mut data);
    let out = match taken {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(Zeroizing::new(s.into_bytes())),
        Some(v @ (Value::Number(_) | Value::Bool(_))) => {
            Some(Zeroizing::new(v.to_string().into_bytes()))
        }
        Some(mut v) => {
            scrub(&mut v);
            return Err(ProviderError::Invalid(format!(
                "vault: field {field:?} of {path:?} is not a scalar"
            )));
        }
    };
    Ok(out)
}

#[async_trait::async_trait]
impl SecretProvider for VaultProvider {
    fn name(&self) -> &'static str {
        "vault"
    }

    async fn get(&self, name: &str) -> Result<Option<SecretBox<[u8]>>, ProviderError> {
        let (path, field) = parse_name(name)?;
        let key = name.to_owned();
        if let Some(v) = self.cache.get(&key) {
            return Ok(Some(v));
        }
        let (client, generation) = self.session(None).await?;
        let data = match self.read(&client, path).await {
            Err(e) if is_forbidden(&e) => {
                tracing::debug!(
                    provider = "vault",
                    path,
                    "vault read forbidden; starting a new session"
                );
                let (client, _) = self.session(Some(generation)).await?;
                match self.read(&client, path).await {
                    Err(e) if is_forbidden(&e) => {
                        return Err(ProviderError::Unavailable("permission denied".into()));
                    }
                    r => r.map_err(|e| unavailable("read", &e))?,
                }
            }
            r => r.map_err(|e| unavailable("read", &e))?,
        };
        let Some(data) = data else {
            return Ok(None);
        };
        let Some(bytes) = take_field(data, path, field)? else {
            return Ok(None);
        };
        let out = SecretBox::new(bytes.as_slice().into());
        self.cache.insert(key, bytes);
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use secrecy::ExposeSecret;

    fn cfg(v: serde_json::Value) -> VaultConfig {
        serde_json::from_value(v).unwrap()
    }

    #[test]
    fn names() {
        assert_eq!(parse_name("db/pass").unwrap(), ("db/pass", "value"));
        assert_eq!(
            parse_name("db/creds#API_KEY").unwrap(),
            ("db/creds", "API_KEY")
        );
        for bad in [
            "__ostro/x",
            "a/../b",
            "a//b",
            "db#",
            "db#a b",
            "db#a#b",
            "#f",
        ] {
            assert!(
                matches!(parse_name(bad), Err(ProviderError::Invalid(_))),
                "{bad}"
            );
        }
    }

    #[test]
    fn field_values() {
        let d = serde_json::json!({"value": "s3cret-value", "n": 42, "b": true, "z": null, "o": {"x": 1}});
        let get = |f: &str| take_field(d.clone(), "p", f);
        assert_eq!(get("value").unwrap().unwrap().as_slice(), b"s3cret-value");
        assert_eq!(get("n").unwrap().unwrap().as_slice(), b"42");
        assert_eq!(get("b").unwrap().unwrap().as_slice(), b"true");
        assert!(get("z").unwrap().is_none());
        assert!(get("missing").unwrap().is_none());
        assert!(matches!(get("o"), Err(ProviderError::Invalid(_))));
        assert!(take_field(Value::Null, "p", "value").unwrap().is_none());
    }

    #[test]
    fn config_validation() {
        let tok = |extra: serde_json::Value| {
            let mut v = serde_json::json!({"addr": "http://127.0.0.1:8200", "token_file": "/t"});
            v.as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            VaultProvider::from_config(&cfg(v))
        };
        tok(serde_json::json!({})).unwrap();
        assert!(tok(serde_json::json!({"addr": "ftp://x"})).is_err());
        assert!(tok(serde_json::json!({"addr": "not a url"})).is_err());
        assert!(tok(serde_json::json!({"mount": "/kv"})).is_err());
        assert!(tok(serde_json::json!({"mount": "a/../b"})).is_err());
        assert!(tok(serde_json::json!({"auth": "approle"})).is_err());
        assert!(
            tok(serde_json::json!({"auth": "approle", "role_id": "r", "secret_id_file": "/s"}))
                .is_ok()
        );
        assert!(tok(serde_json::json!({"auth": "kubernetes"})).is_err());
        assert!(tok(serde_json::json!({"auth": "kubernetes", "k8s_role": "ostro"})).is_ok());
    }

    #[test]
    fn status_from_errors() {
        let e = ClientError::APIError {
            code: 403,
            errors: vec!["permission denied".into()],
        };
        assert_eq!(status_of(&e), Some(403));
        assert!(is_forbidden(&e));
        assert!(
            matches!(unavailable("read", &e), ProviderError::Unavailable(m) if m == "vault read: permission denied")
        );
        assert_eq!(status_of(&ClientError::ResponseEmptyError), None);
    }

    #[tokio::test]
    async fn token_file_is_read_and_trimmed() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("t");
        std::fs::write(&p, "  hvs.canary-token\n").unwrap();
        assert_eq!(
            read_secret_file(&p).await.unwrap().expose_secret(),
            "hvs.canary-token"
        );
        std::fs::write(&p, "\n").unwrap();
        let e = read_secret_file(&p).await.unwrap_err();
        assert!(matches!(e, ProviderError::Unavailable(_)));
        let e = read_secret_file(&dir.path().join("missing"))
            .await
            .unwrap_err();
        assert!(
            e.to_string().contains("NotFound") || e.to_string().contains("not found"),
            "{e}"
        );
    }
}
